#!/usr/bin/env python3
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

from google.cloud import bigquery
from google.oauth2 import service_account

# Ensure repo root is importable when executed as a script in CI.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gmail_client import GmailInboxClient


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def as_bool(value: str, default: bool = False) -> bool:
    raw = (value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


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


def build_bq_client() -> bigquery.Client:
    project_id = env("BQ_PROJECT_ID", "sm-test-391201")
    sa_json = env("BQ_SERVICE_ACCOUNT_JSON")

    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=project_id or info.get("project_id"), credentials=creds)

    return bigquery.Client(project=project_id)


def should_run_now_nz() -> bool:
    now_nz = datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland"))
    return now_nz.weekday() < 5 and now_nz.hour == 6


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
    project_id = sanitize_id(env("BQ_PROJECT_ID", "sm-test-391201"), "BQ_PROJECT_ID")
    dataset = sanitize_id(env("BQ_DATASET", "supermetrics_data"), "BQ_DATASET")
    view_name = sanitize_id(env("BQ_VIEW", "master_overview"), "BQ_VIEW")
    table_fqn = f"`{project_id}.{dataset}.{view_name}`"

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


def build_email_body(rows: List[AlertRow]) -> str:
    return (
        "Please check the attached campaigns. They're either not live or have some issue with "
        "SuperMetrics (line item IDs not added on platform, account not ticked on SuperMetrics). "
        "Both kind of issues require action.\n\n"
        "NOTE: This list does not include bookings that are booked in any property that does not "
        "contain the word 'Programmatic' - so 'Acquire Fee', 'Other' or any custom Property names "
        "on AdTeamPro are not included. Please check these manually"
    )


def send_digest(rows: List[AlertRow]) -> str:
    recipients = split_csv(env("ALERT_EMAIL_TO", ""))
    if not recipients:
        raise RuntimeError("ALERT_EMAIL_TO is required.")

    client = GmailInboxClient(
        client_id=env("GMAIL_CLIENT_ID"),
        client_secret=env("GMAIL_CLIENT_SECRET"),
        refresh_token=env("GMAIL_REFRESH_TOKEN"),
        user_id=env("GMAIL_USER", "me"),
    )

    now_nz = datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland"))
    subject = env("ALERT_EMAIL_SUBJECT", "")
    if not subject:
        subject = "ALERT: Campaigns not live"
    body = build_email_body(rows)
    attachment_name = f"campaign_not_live_{now_nz.strftime('%Y%m%d')}.csv"
    return client.send_email(
        to_email=", ".join(recipients),
        subject=subject,
        body_text=body,
        attachments={attachment_name: build_csv(rows)},
    )


def main() -> int:
    if not as_bool(env("ALERT_FORCE_RUN", ""), default=False) and not should_run_now_nz():
        print("Skip: current NZ time is outside 6:00 AM weekday window.")
        return 0

    bq_client = build_bq_client()
    rows = fetch_alert_rows(bq_client)

    if not rows:
        print("No campaigns matched alert criteria. No email sent.")
        return 0

    message_id = send_digest(rows)
    print(f"Alert email sent. Gmail message id: {message_id}; rows: {len(rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
