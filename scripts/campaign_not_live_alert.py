#!/usr/bin/env python3
import csv
import hashlib
import hmac
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.oauth2 import service_account

# Ensure repo root is importable when executed as a script in CI.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gmail_client import GmailInboxClient

ALERT_TYPE = "NOT_LIVE"


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def as_bool(value: str, default: bool = False) -> bool:
    raw = (value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def as_int(value: str, default: int) -> int:
    raw = (value or "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def split_csv(value: str) -> List[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def sanitize_id(value: str, field_name: str) -> str:
    clean = value.strip()
    if not clean:
        raise ValueError(f"{field_name} is required.")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if not set(clean).issubset(allowed):
        raise ValueError(f"{field_name} contains invalid characters: {clean}")
    return clean


def bq_project_id() -> str:
    return sanitize_id(env("BQ_PROJECT_ID", "sm-test-391201"), "BQ_PROJECT_ID")


def bq_dataset() -> str:
    return sanitize_id(env("BQ_DATASET", "supermetrics_data"), "BQ_DATASET")


def bq_view() -> str:
    return sanitize_id(env("BQ_VIEW", "master_overview"), "BQ_VIEW")


def build_bq_client() -> bigquery.Client:
    project_id = bq_project_id()
    sa_json = env("BQ_SERVICE_ACCOUNT_JSON")

    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=project_id or info.get("project_id"), credentials=creds)

    return bigquery.Client(project=project_id)


def should_run_now_nz() -> bool:
    now_nz = datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland"))
    return now_nz.weekday() < 5 and now_nz.hour == 6


def today_nz() -> date:
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland")).date()


def snoozes_table_fqn() -> str:
    return f"`{bq_project_id()}.{bq_dataset()}.snoozes`"


def snapshots_table_fqn() -> str:
    return f"`{bq_project_id()}.{bq_dataset()}.live_alert_snapshots`"


def snapshots_table_ref() -> str:
    return f"{bq_project_id()}.{bq_dataset()}.live_alert_snapshots"


def ensure_control_tables(client: bigquery.Client) -> None:
    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {snoozes_table_fqn()} (
  our_ref STRING NOT NULL,
  snooze_type STRING NOT NULL,
  snooze_status STRING NOT NULL,
  snooze_reason STRING,
  snooze_start_date DATE,
  snooze_end_date DATE,
  snoozed_by STRING,
  run_id STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP NOT NULL,
  unsnoozed_by STRING,
  unsnoozed_at TIMESTAMP,
  dismissed_by STRING,
  dismissed_at TIMESTAMP
)
"""
    ).result()

    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {snapshots_table_fqn()} (
  run_id STRING NOT NULL,
  run_date_nz DATE NOT NULL,
  run_timestamp_utc TIMESTAMP NOT NULL,
  alert_type STRING NOT NULL,
  our_ref STRING,
  job_number STRING,
  start_date DATE,
  end_date DATE,
  advertiser STRING,
  campaign STRING,
  location_text STRING,
  property_name STRING,
  booking_status STRING
)
"""
    ).result()


@dataclass
class AlertRow:
    our_ref: str
    job_number: str
    start_date: str
    end_date: str
    advertiser: str
    campaign: str
    location_text: str
    property_name: str
    booking_status: str


def fetch_alert_rows(client: bigquery.Client) -> List[AlertRow]:
    table_fqn = f"`{bq_project_id()}.{bq_dataset()}.{bq_view()}`"

    query = f"""
WITH typed AS (
  SELECT
    CAST(OURREF AS STRING) AS OUR_REF,
    CAST(JOBNUMBER AS STRING) AS JOB_NUMBER,
    COALESCE(
      SAFE_CAST(STARTDATE AS DATE),
      SAFE.PARSE_DATE('%Y-%m-%d', CAST(STARTDATE AS STRING)),
      SAFE.PARSE_DATE('%d/%m/%Y', CAST(STARTDATE AS STRING)),
      SAFE.PARSE_DATE('%m/%d/%Y', CAST(STARTDATE AS STRING))
    ) AS START_DATE,
    COALESCE(
      SAFE_CAST(ENDDATE AS DATE),
      SAFE.PARSE_DATE('%Y-%m-%d', CAST(ENDDATE AS STRING)),
      SAFE.PARSE_DATE('%d/%m/%Y', CAST(ENDDATE AS STRING)),
      SAFE.PARSE_DATE('%m/%d/%Y', CAST(ENDDATE AS STRING))
    ) AS END_DATE,
    CAST(ADVERTISERNAME AS STRING) AS ADVERTISER,
    CAST(CAMPAIGNNAME AS STRING) AS CAMPAIGN,
    CAST(LOCATIONTEXT AS STRING) AS LOCATION_TEXT,
    CAST(PROPERTYNAME AS STRING) AS PROPERTY_NAME,
    CAST(BOOKINGSTATUS AS STRING) AS BOOKING_STATUS,
    COALESCE(SAFE_CAST(IMPRESSIONS AS FLOAT64), 0) AS IMPRESSIONS
  FROM {table_fqn}
),
agg AS (
  SELECT
    OUR_REF,
    ANY_VALUE(JOB_NUMBER) AS JOB_NUMBER,
    MIN(START_DATE) AS START_DATE,
    MIN(END_DATE) AS END_DATE,
    ANY_VALUE(ADVERTISER) AS ADVERTISER,
    ANY_VALUE(CAMPAIGN) AS CAMPAIGN,
    ANY_VALUE(LOCATION_TEXT) AS LOCATION_TEXT,
    ANY_VALUE(PROPERTY_NAME) AS PROPERTY_NAME,
    ANY_VALUE(BOOKING_STATUS) AS BOOKING_STATUS,
    MAX(IMPRESSIONS) AS MAX_IMPRESSIONS
  FROM typed
  WHERE OUR_REF IS NOT NULL
    AND TRIM(OUR_REF) != ''
  GROUP BY OUR_REF
)
SELECT
  OUR_REF,
  JOB_NUMBER,
  START_DATE,
  END_DATE,
  ADVERTISER,
  CAMPAIGN,
  LOCATION_TEXT,
  PROPERTY_NAME,
  BOOKING_STATUS
FROM agg
WHERE START_DATE < CURRENT_DATE('Pacific/Auckland')
  AND END_DATE IS NOT NULL
  AND END_DATE >= CURRENT_DATE('Pacific/Auckland')
  AND LOWER(COALESCE(BOOKING_STATUS, '')) = 'booked'
  AND LOWER(COALESCE(PROPERTY_NAME, '')) LIKE '%programmatic%'
  AND LOWER(COALESCE(PROPERTY_NAME, '')) NOT LIKE '%adserving%'
  AND MAX_IMPRESSIONS = 0
ORDER BY START_DATE, OUR_REF
"""

    rows = client.query(query).result()
    alerts: List[AlertRow] = []
    for row in rows:
        alerts.append(
            AlertRow(
                our_ref=str(row["OUR_REF"] or "").strip(),
                job_number=str(row["JOB_NUMBER"] or "").strip(),
                start_date=str(row["START_DATE"] or "").strip(),
                end_date=str(row["END_DATE"] or "").strip(),
                advertiser=str(row["ADVERTISER"] or "").strip(),
                campaign=str(row["CAMPAIGN"] or "").strip(),
                location_text=str(row["LOCATION_TEXT"] or "").strip(),
                property_name=str(row["PROPERTY_NAME"] or "").strip(),
                booking_status=str(row["BOOKING_STATUS"] or "").strip(),
            )
        )
    return alerts


def fetch_latest_snooze_states(client: bigquery.Client, refs: List[str]) -> Dict[str, Dict[str, object]]:
    if not refs:
        return {}

    query = f"""
WITH latest AS (
  SELECT
    our_ref,
    snooze_status,
    snooze_end_date,
    snooze_reason,
    snoozed_by,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY our_ref, snooze_type ORDER BY updated_at DESC) AS rn
  FROM {snoozes_table_fqn()}
  WHERE snooze_type = @snooze_type
    AND our_ref IN UNNEST(@refs)
)
SELECT our_ref, snooze_status, snooze_end_date, snooze_reason, snoozed_by, updated_at
FROM latest
WHERE rn = 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snooze_type", "STRING", ALERT_TYPE),
            bigquery.ArrayQueryParameter("refs", "STRING", refs),
        ]
    )
    rows = client.query(query, job_config=job_config).result()

    out: Dict[str, Dict[str, object]] = {}
    for row in rows:
        out[str(row["our_ref"])] = {
            "snooze_status": str(row["snooze_status"] or ""),
            "snooze_end_date": row["snooze_end_date"],
            "snooze_reason": str(row["snooze_reason"] or ""),
            "snoozed_by": str(row["snoozed_by"] or ""),
            "updated_at": row["updated_at"],
        }
    return out


def filter_suppressed_rows(rows: List[AlertRow], states: Dict[str, Dict[str, object]]) -> List[AlertRow]:
    today = today_nz()
    filtered: List[AlertRow] = []
    for row in rows:
        state = states.get(row.our_ref)
        if not state:
            filtered.append(row)
            continue

        status = str(state.get("snooze_status") or "").upper()
        end_date = state.get("snooze_end_date")

        if status == "DISMISSED":
            continue
        if status == "ACTIVE" and end_date and end_date >= today:
            continue

        filtered.append(row)

    return filtered


def store_snapshot_rows(client: bigquery.Client, rows: List[AlertRow], run_id: str) -> None:
    if not rows:
        return

    now_utc = datetime.now(timezone.utc)
    run_date = today_nz().isoformat()
    payload = []
    for row in rows:
        payload.append(
            {
                "run_id": run_id,
                "run_date_nz": run_date,
                "run_timestamp_utc": now_utc.isoformat(),
                "alert_type": ALERT_TYPE,
                "our_ref": row.our_ref,
                "job_number": row.job_number,
                "start_date": row.start_date,
                "end_date": row.end_date,
                "advertiser": row.advertiser,
                "campaign": row.campaign,
                "location_text": row.location_text,
                "property_name": row.property_name,
                "booking_status": row.booking_status,
            }
        )

    errors = client.insert_rows_json(snapshots_table_ref(), payload)
    if errors:
        raise RuntimeError(f"Failed to insert live alert snapshot rows: {errors}")


def build_csv(rows: List[AlertRow]) -> bytes:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "OUR_REF",
            "JOB_NUMBER",
            "START_DATE",
            "END_DATE",
            "ADVERTISER",
            "CAMPAIGN",
            "LOCATIONTEXT",
            "PROPERTYNAME",
            "BOOKINGSTATUS",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.our_ref,
                row.job_number,
                row.start_date,
                row.end_date,
                row.advertiser,
                row.campaign,
                row.location_text,
                row.property_name,
                row.booking_status,
            ]
        )
    return buf.getvalue().encode("utf-8")


def build_signed_dashboard_link(base_url: str, user: str, run_id: str, ttl_days: int, secret: str) -> str:
    exp = int(time.time()) + max(ttl_days, 1) * 24 * 60 * 60
    payload = f"{user}|{run_id}|{exp}"
    sig = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    query = urlencode(
        {
            "mode": "live_alerts",
            "user": user,
            "run_id": run_id,
            "exp": str(exp),
            "sig": sig,
        }
    )
    joiner = "&" if "?" in base_url else "?"
    return f"{base_url}{joiner}{query}"


def build_email_body(link: str) -> str:
    return (
        "Please check the attached campaigns. They're either not live or have some issue with "
        "SuperMetrics (line item IDs not added on platform, account not ticked on SuperMetrics). "
        "Both kind of issues require action.\n\n"
        "NOTE: This list does not include bookings that are booked in any property that does not "
        "contain the word 'Programmatic' - so 'Acquire Fee', 'Other' or any custom Property names "
        "on AdTeamPro are not included. Please check these manually\n\n"
        f"Open dashboard: {link}"
    )


def send_digest(rows: List[AlertRow], run_id: str) -> str:
    recipients = split_csv(env("ALERT_EMAIL_TO", ""))
    if not recipients:
        raise RuntimeError("ALERT_EMAIL_TO is required.")

    dashboard_base_url = env("ALERT_DASHBOARD_BASE_URL", "")
    link_secret = env("LINK_SIGNING_SECRET", "")
    if not dashboard_base_url:
        raise RuntimeError("ALERT_DASHBOARD_BASE_URL is required for signed dashboard links.")
    if not link_secret:
        raise RuntimeError("LINK_SIGNING_SECRET is required for signed dashboard links.")

    link_ttl_days = as_int(env("ALERT_LINK_TTL_DAYS", "7"), 7)

    client = GmailInboxClient(
        client_id=env("GMAIL_CLIENT_ID"),
        client_secret=env("GMAIL_CLIENT_SECRET"),
        refresh_token=env("GMAIL_REFRESH_TOKEN"),
        user_id=env("GMAIL_USER", "me"),
    )

    subject = env("ALERT_EMAIL_SUBJECT", "")
    if not subject:
        subject = "ALERT: Campaigns not live"

    now_nz = datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland"))
    attachment_name = f"campaign_not_live_{now_nz.strftime('%Y%m%d')}.csv"

    sent_ids: List[str] = []
    for recipient in recipients:
        link = build_signed_dashboard_link(
            base_url=dashboard_base_url,
            user=recipient,
            run_id=run_id,
            ttl_days=link_ttl_days,
            secret=link_secret,
        )
        body = build_email_body(link)
        msg_id = client.send_email(
            to_email=recipient,
            subject=subject,
            body_text=body,
            attachments={attachment_name: build_csv(rows)},
        )
        sent_ids.append(msg_id)

    return ",".join(sent_ids)


def main() -> int:
    if not as_bool(env("ALERT_FORCE_RUN", ""), default=False) and not should_run_now_nz():
        print("Skip: current NZ time is outside 6:00 AM weekday window.")
        return 0

    bq_client = build_bq_client()
    ensure_control_tables(bq_client)

    rows = fetch_alert_rows(bq_client)
    states = fetch_latest_snooze_states(bq_client, [r.our_ref for r in rows])
    rows = filter_suppressed_rows(rows, states)

    if not rows:
        print("No campaigns matched alert criteria. No email sent.")
        return 0

    run_id = uuid.uuid4().hex
    store_snapshot_rows(bq_client, rows, run_id=run_id)

    message_ids = send_digest(rows, run_id=run_id)
    print(f"Alert emails sent. Run id: {run_id}; Gmail message ids: {message_ids}; rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
