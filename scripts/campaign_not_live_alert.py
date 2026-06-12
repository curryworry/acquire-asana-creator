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

ALERT_TYPE_NOT_LIVE = "NOT_LIVE"
ALERT_TYPE_MISSING_OUR_REF = "MISSING_OUR_REF"
ALERT_TYPE_ENDED_BUT_IMPRESSIONS = "ENDED_BUT_IMPRESSIONS"
ALERT_TYPE_STOPPED_IMPRESSIONS = "STOPPED_IMPRESSIONS"


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


def bq_delivery_table() -> str:
    return sanitize_id(env("BQ_DELIVERY_TABLE", "BLEND_BLEND_5_1_2"), "BQ_DELIVERY_TABLE")


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


def make_alert_key(alert_type: str, dims: List[str]) -> str:
    canonical = "|".join([alert_type] + [str(x or "").strip().lower() for x in dims])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


def _nullable_date(value: str) -> str | None:
    clean = str(value or "").strip()
    return clean or None


def ensure_control_tables(client: bigquery.Client) -> None:
    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {snoozes_table_fqn()} (
  our_ref STRING NOT NULL,
  alert_key STRING,
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
  alert_key STRING,
  our_ref STRING,
  job_number STRING,
  start_date DATE,
  end_date DATE,
  advertiser STRING,
  campaign STRING,
  location_text STRING,
  property_name STRING,
  booking_status STRING,
  datasource STRING,
  account STRING,
  first_missing_date DATE,
  last_missing_date DATE,
  total_impressions FLOAT64,
  total_clicks FLOAT64,
  total_cost FLOAT64,
  row_count INT64
)
"""
    ).result()

    client.query(f"ALTER TABLE {snoozes_table_fqn()} ADD COLUMN IF NOT EXISTS alert_key STRING").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS alert_key STRING").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS datasource STRING").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS account STRING").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS first_missing_date DATE").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS last_missing_date DATE").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS total_impressions FLOAT64").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS total_clicks FLOAT64").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS total_cost FLOAT64").result()
    client.query(f"ALTER TABLE {snapshots_table_fqn()} ADD COLUMN IF NOT EXISTS row_count INT64").result()


@dataclass
class AlertRow:
    alert_type: str
    alert_key: str
    our_ref: str
    job_number: str
    start_date: str
    end_date: str
    advertiser: str
    campaign: str
    location_text: str
    property_name: str
    booking_status: str
    datasource: str = ""
    account: str = ""
    first_missing_date: str = ""
    last_missing_date: str = ""
    total_impressions: float = 0.0
    total_clicks: float = 0.0
    total_cost: float = 0.0
    row_count: int = 0


def fetch_not_live_rows(client: bigquery.Client) -> List[AlertRow]:
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
        our_ref = str(row["OUR_REF"] or "").strip()
        alerts.append(
            AlertRow(
                alert_type=ALERT_TYPE_NOT_LIVE,
                alert_key=make_alert_key(ALERT_TYPE_NOT_LIVE, [our_ref]),
                our_ref=our_ref,
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


def fetch_missing_our_ref_rows(client: bigquery.Client) -> List[AlertRow]:
    table_fqn = f"`{bq_project_id()}.{bq_dataset()}.{bq_delivery_table()}`"
    query = f"""
SELECT
  DATASOURCENAME,
  ACCOUNT,
  ADVERTISER_NAME,
  CAMPAIGN,
  JOB_NUMBER,
  MIN(DATE) AS FIRST_MISSING_DATE,
  MAX(DATE) AS LAST_MISSING_DATE,
  SUM(COALESCE(IMPRESSIONS, 0)) AS IMPRESSIONS,
  SUM(COALESCE(CLICKS, 0)) AS CLICKS,
  SUM(COALESCE(COST, 0)) AS COST,
  COUNT(*) AS ROW_COUNT
FROM {table_fqn}
WHERE TRIM(COALESCE(OUR_REF, '')) = ''
  AND LOWER(COALESCE(ADVERTISER_NAME, '')) NOT LIKE '%client card%'
GROUP BY DATASOURCENAME, ACCOUNT, ADVERTISER_NAME, CAMPAIGN, JOB_NUMBER
HAVING IMPRESSIONS > 0
ORDER BY COST DESC, IMPRESSIONS DESC
"""

    rows = client.query(query).result()
    alerts: List[AlertRow] = []
    for row in rows:
        datasource = str(row["DATASOURCENAME"] or "").strip()
        account = str(row["ACCOUNT"] or "").strip()
        advertiser = str(row["ADVERTISER_NAME"] or "").strip()
        campaign = str(row["CAMPAIGN"] or "").strip()
        job_number = str(row["JOB_NUMBER"] or "").strip()
        key_dims = [datasource, account, advertiser, campaign, job_number]

        alerts.append(
            AlertRow(
                alert_type=ALERT_TYPE_MISSING_OUR_REF,
                alert_key=make_alert_key(ALERT_TYPE_MISSING_OUR_REF, key_dims),
                our_ref="",
                job_number=job_number,
                start_date="",
                end_date="",
                advertiser=advertiser,
                campaign=campaign,
                location_text="",
                property_name="",
                booking_status="",
                datasource=datasource,
                account=account,
                first_missing_date=str(row["FIRST_MISSING_DATE"] or "").strip(),
                last_missing_date=str(row["LAST_MISSING_DATE"] or "").strip(),
                total_impressions=float(row["IMPRESSIONS"] or 0),
                total_clicks=float(row["CLICKS"] or 0),
                total_cost=float(row["COST"] or 0),
                row_count=int(row["ROW_COUNT"] or 0),
            )
        )
    return alerts


def fetch_ended_but_impressions_rows(client: bigquery.Client) -> List[AlertRow]:
    overview_table_fqn = f"`{bq_project_id()}.{bq_dataset()}.{bq_view()}`"
    delivery_table_fqn = f"`{bq_project_id()}.{bq_dataset()}.{bq_delivery_table()}`"
    query = f"""
WITH meta AS (
  SELECT
    TRIM(CAST(OURREF AS STRING)) AS OUR_REF,
    ANY_VALUE(CAST(JOBNUMBER AS STRING)) AS JOB_NUMBER,
    ANY_VALUE(CAST(ADVERTISERNAME AS STRING)) AS ADVERTISER,
    ANY_VALUE(CAST(CAMPAIGNNAME AS STRING)) AS CAMPAIGN,
    ANY_VALUE(CAST(LOCATIONTEXT AS STRING)) AS LOCATION_TEXT,
    ANY_VALUE(CAST(PROPERTYNAME AS STRING)) AS PROPERTY_NAME,
    ANY_VALUE(CAST(BOOKINGSTATUS AS STRING)) AS BOOKING_STATUS,
    MAX(SAFE_CAST(ENDDATE AS DATE)) AS END_DATE
  FROM {overview_table_fqn}
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
    AND LOWER(COALESCE(BOOKINGSTATUS, '')) = 'booked'
    AND LOWER(COALESCE(PROPERTYNAME, '')) LIKE '%programmatic%'
    AND LOWER(COALESCE(PROPERTYNAME, '')) NOT LIKE '%adserving%'
  GROUP BY 1
),
delivery AS (
  SELECT
    TRIM(CAST(OUR_REF AS STRING)) AS OUR_REF,
    SAFE_CAST(DATE AS DATE) AS DELIVERY_DATE,
    SUM(COALESCE(IMPRESSIONS, 0)) AS IMPRESSIONS,
    SUM(COALESCE(CLICKS, 0)) AS CLICKS,
    SUM(COALESCE(COST, 0)) AS COST,
    COUNT(*) AS ROW_COUNT
  FROM {delivery_table_fqn}
  WHERE OUR_REF IS NOT NULL
    AND TRIM(CAST(OUR_REF AS STRING)) != ''
  GROUP BY 1, 2
)
SELECT
  m.OUR_REF,
  m.JOB_NUMBER,
  m.ADVERTISER,
  m.CAMPAIGN,
  m.LOCATION_TEXT,
  m.PROPERTY_NAME,
  m.BOOKING_STATUS,
  m.END_DATE,
  MIN(d.DELIVERY_DATE) AS FIRST_IMPRESSIONS_AFTER_END_DATE,
  MAX(d.DELIVERY_DATE) AS LAST_IMPRESSIONS_AFTER_END_DATE,
  SUM(d.IMPRESSIONS) AS TOTAL_IMPRESSIONS_AFTER_END_DATE,
  SUM(d.CLICKS) AS TOTAL_CLICKS_AFTER_END_DATE,
  SUM(d.COST) AS TOTAL_COST_AFTER_END_DATE,
  SUM(d.ROW_COUNT) AS TOTAL_ROWS_AFTER_END_DATE
FROM meta m
JOIN delivery d ON d.OUR_REF = m.OUR_REF
WHERE m.END_DATE < CURRENT_DATE('Pacific/Auckland')
  AND d.DELIVERY_DATE > m.END_DATE
GROUP BY m.OUR_REF, m.JOB_NUMBER, m.ADVERTISER, m.CAMPAIGN, m.LOCATION_TEXT, m.PROPERTY_NAME, m.BOOKING_STATUS, m.END_DATE
HAVING SUM(d.IMPRESSIONS) > 10
ORDER BY TOTAL_IMPRESSIONS_AFTER_END_DATE DESC, m.OUR_REF
"""

    rows = client.query(query).result()
    alerts: List[AlertRow] = []
    for row in rows:
        our_ref = str(row["OUR_REF"] or "").strip()
        alerts.append(
            AlertRow(
                alert_type=ALERT_TYPE_ENDED_BUT_IMPRESSIONS,
                alert_key=make_alert_key(ALERT_TYPE_ENDED_BUT_IMPRESSIONS, [our_ref]),
                our_ref=our_ref,
                job_number=str(row["JOB_NUMBER"] or "").strip(),
                start_date="",
                end_date=str(row["END_DATE"] or "").strip(),
                advertiser=str(row["ADVERTISER"] or "").strip(),
                campaign=str(row["CAMPAIGN"] or "").strip(),
                location_text=str(row["LOCATION_TEXT"] or "").strip(),
                property_name=str(row["PROPERTY_NAME"] or "").strip(),
                booking_status=str(row["BOOKING_STATUS"] or "").strip(),
                first_missing_date=str(row["FIRST_IMPRESSIONS_AFTER_END_DATE"] or "").strip(),
                last_missing_date=str(row["LAST_IMPRESSIONS_AFTER_END_DATE"] or "").strip(),
                total_impressions=float(row["TOTAL_IMPRESSIONS_AFTER_END_DATE"] or 0),
                total_clicks=float(row["TOTAL_CLICKS_AFTER_END_DATE"] or 0),
                total_cost=float(row["TOTAL_COST_AFTER_END_DATE"] or 0),
                row_count=int(row["TOTAL_ROWS_AFTER_END_DATE"] or 0),
            )
        )
    return alerts


def fetch_stopped_impressions_rows(client: bigquery.Client) -> List[AlertRow]:
    overview_table_fqn = f"`{bq_project_id()}.{bq_dataset()}.{bq_view()}`"
    delivery_table_fqn = f"`{bq_project_id()}.{bq_dataset()}.{bq_delivery_table()}`"
    query = f"""
WITH latest AS (
  SELECT MAX(DATE) AS LATEST_DELIVERY_DATE
  FROM {delivery_table_fqn}
),
meta AS (
  SELECT
    TRIM(CAST(OURREF AS STRING)) AS OUR_REF,
    ANY_VALUE(CAST(JOBNUMBER AS STRING)) AS JOB_NUMBER,
    MIN(SAFE_CAST(STARTDATE AS DATE)) AS START_DATE,
    MAX(SAFE_CAST(ENDDATE AS DATE)) AS END_DATE,
    ANY_VALUE(CAST(ADVERTISERNAME AS STRING)) AS ADVERTISER,
    ANY_VALUE(CAST(CAMPAIGNNAME AS STRING)) AS CAMPAIGN,
    ANY_VALUE(CAST(LOCATIONTEXT AS STRING)) AS LOCATION_TEXT,
    ANY_VALUE(CAST(PROPERTYNAME AS STRING)) AS PROPERTY_NAME,
    ANY_VALUE(CAST(BOOKINGSTATUS AS STRING)) AS BOOKING_STATUS
  FROM {overview_table_fqn}
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
    AND LOWER(COALESCE(BOOKINGSTATUS, '')) = 'booked'
    AND LOWER(COALESCE(PROPERTYNAME, '')) LIKE '%programmatic%'
    AND LOWER(COALESCE(PROPERTYNAME, '')) NOT LIKE '%adserving%'
  GROUP BY 1
),
daily AS (
  SELECT
    TRIM(CAST(OUR_REF AS STRING)) AS OUR_REF,
    DATE,
    SUM(COALESCE(IMPRESSIONS, 0)) AS IMPRESSIONS,
    SUM(COALESCE(CLICKS, 0)) AS CLICKS,
    SUM(COALESCE(COST, 0)) AS COST,
    COUNT(*) AS ROW_COUNT
  FROM {delivery_table_fqn}
  WHERE OUR_REF IS NOT NULL
    AND TRIM(CAST(OUR_REF AS STRING)) != ''
  GROUP BY 1, 2
)
SELECT
  m.OUR_REF,
  m.JOB_NUMBER,
  m.START_DATE,
  m.END_DATE,
  m.ADVERTISER,
  m.CAMPAIGN,
  m.LOCATION_TEXT,
  m.PROPERTY_NAME,
  m.BOOKING_STATUS,
  l.LATEST_DELIVERY_DATE,
  SUM(
    CASE
      WHEN d.DATE BETWEEN DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 7 DAY)
        AND DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 1 DAY)
      THEN d.IMPRESSIONS
      ELSE 0
    END
  ) AS PRIOR_7D_IMPRESSIONS,
  SUM(
    CASE
      WHEN d.DATE = l.LATEST_DELIVERY_DATE
      THEN d.IMPRESSIONS
      ELSE 0
    END
  ) AS LATEST_DAY_IMPRESSIONS,
  SUM(
    CASE
      WHEN d.DATE = DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 1 DAY)
      THEN d.IMPRESSIONS
      ELSE 0
    END
  ) AS PRIOR_DAY_IMPRESSIONS,
  SUM(
    CASE
      WHEN d.DATE BETWEEN DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 7 DAY)
        AND DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 1 DAY)
      THEN d.CLICKS
      ELSE 0
    END
  ) AS PRIOR_7D_CLICKS,
  SUM(
    CASE
      WHEN d.DATE BETWEEN DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 7 DAY)
        AND DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 1 DAY)
      THEN d.COST
      ELSE 0
    END
  ) AS PRIOR_7D_COST,
  SUM(
    CASE
      WHEN d.DATE BETWEEN DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 7 DAY)
        AND DATE_SUB(l.LATEST_DELIVERY_DATE, INTERVAL 1 DAY)
      THEN d.ROW_COUNT
      ELSE 0
    END
  ) AS PRIOR_7D_ROWS
FROM meta m
CROSS JOIN latest l
LEFT JOIN daily d ON d.OUR_REF = m.OUR_REF
WHERE m.START_DATE < l.LATEST_DELIVERY_DATE
  AND m.END_DATE IS NOT NULL
  AND m.END_DATE >= l.LATEST_DELIVERY_DATE
GROUP BY
  m.OUR_REF,
  m.JOB_NUMBER,
  m.START_DATE,
  m.END_DATE,
  m.ADVERTISER,
  m.CAMPAIGN,
  m.LOCATION_TEXT,
  m.PROPERTY_NAME,
  m.BOOKING_STATUS,
  l.LATEST_DELIVERY_DATE
HAVING PRIOR_7D_IMPRESSIONS > 200
   AND LATEST_DAY_IMPRESSIONS = 0
   AND PRIOR_DAY_IMPRESSIONS = 0
ORDER BY PRIOR_7D_IMPRESSIONS DESC, m.OUR_REF
"""

    rows = client.query(query).result()
    alerts: List[AlertRow] = []
    for row in rows:
        our_ref = str(row["OUR_REF"] or "").strip()
        alerts.append(
            AlertRow(
                alert_type=ALERT_TYPE_STOPPED_IMPRESSIONS,
                alert_key=make_alert_key(ALERT_TYPE_STOPPED_IMPRESSIONS, [our_ref]),
                our_ref=our_ref,
                job_number=str(row["JOB_NUMBER"] or "").strip(),
                start_date=str(row["START_DATE"] or "").strip(),
                end_date=str(row["END_DATE"] or "").strip(),
                advertiser=str(row["ADVERTISER"] or "").strip(),
                campaign=str(row["CAMPAIGN"] or "").strip(),
                location_text=str(row["LOCATION_TEXT"] or "").strip(),
                property_name=str(row["PROPERTY_NAME"] or "").strip(),
                booking_status=str(row["BOOKING_STATUS"] or "").strip(),
                first_missing_date=str(row["LATEST_DELIVERY_DATE"] or "").strip(),
                last_missing_date=str(row["LATEST_DELIVERY_DATE"] or "").strip(),
                total_impressions=float(row["PRIOR_7D_IMPRESSIONS"] or 0),
                total_clicks=float(row["PRIOR_7D_CLICKS"] or 0),
                total_cost=float(row["PRIOR_7D_COST"] or 0),
                row_count=int(row["PRIOR_7D_ROWS"] or 0),
            )
        )
    return alerts


def fetch_latest_snooze_states(client: bigquery.Client, rows: List[AlertRow]) -> Dict[str, Dict[str, object]]:
    if not rows:
        return {}

    keys = sorted({r.alert_key for r in rows})
    legacy_not_live_refs = sorted(
        {r.our_ref for r in rows if r.alert_type == ALERT_TYPE_NOT_LIVE and str(r.our_ref).strip()}
    )
    types = sorted({r.alert_type for r in rows})

    query = f"""
WITH latest AS (
  SELECT
    COALESCE(alert_key, our_ref) AS resolved_alert_key,
    snooze_type,
    snooze_status,
    snooze_end_date,
    snooze_reason,
    snoozed_by,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(alert_key, our_ref), snooze_type ORDER BY updated_at DESC) AS rn
  FROM {snoozes_table_fqn()}
  WHERE snooze_type IN UNNEST(@types)
    AND (
      COALESCE(alert_key, our_ref) IN UNNEST(@keys)
      OR (snooze_type = @not_live_type AND our_ref IN UNNEST(@legacy_not_live_refs))
    )
)
SELECT resolved_alert_key, snooze_type, snooze_status, snooze_end_date, snooze_reason, snoozed_by, updated_at
FROM latest
WHERE rn = 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("types", "STRING", types),
            bigquery.ArrayQueryParameter("keys", "STRING", keys),
            bigquery.ScalarQueryParameter("not_live_type", "STRING", ALERT_TYPE_NOT_LIVE),
            bigquery.ArrayQueryParameter("legacy_not_live_refs", "STRING", legacy_not_live_refs),
        ]
    )
    rows = client.query(query, job_config=job_config).result()

    out: Dict[str, Dict[str, object]] = {}
    for row in rows:
        identity = f"{str(row['snooze_type'] or '').strip()}::{str(row['resolved_alert_key'] or '').strip()}"
        out[identity] = {
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
        state = states.get(f"{row.alert_type}::{row.alert_key}")
        if not state and row.alert_type == ALERT_TYPE_NOT_LIVE and row.our_ref:
            state = states.get(f"{row.alert_type}::{row.our_ref}")
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
                "alert_type": row.alert_type,
                "alert_key": row.alert_key,
                "our_ref": row.our_ref,
                "job_number": row.job_number,
                "start_date": _nullable_date(row.start_date),
                "end_date": _nullable_date(row.end_date),
                "advertiser": row.advertiser,
                "campaign": row.campaign,
                "location_text": row.location_text,
                "property_name": row.property_name,
                "booking_status": row.booking_status,
                "datasource": row.datasource,
                "account": row.account,
                "first_missing_date": _nullable_date(row.first_missing_date),
                "last_missing_date": _nullable_date(row.last_missing_date),
                "total_impressions": row.total_impressions,
                "total_clicks": row.total_clicks,
                "total_cost": row.total_cost,
                "row_count": row.row_count,
            }
        )

    errors = client.insert_rows_json(snapshots_table_ref(), payload)
    if errors:
        raise RuntimeError(f"Failed to insert live alert snapshot rows: {errors}")


def build_csv_all(rows: List[AlertRow]) -> bytes:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "ALERT_TYPE",
            "ALERT_KEY",
            "OUR_REF",
            "JOB_NUMBER",
            "START_DATE",
            "END_DATE",
            "ADVERTISER",
            "CAMPAIGN",
            "LOCATIONTEXT",
            "PROPERTYNAME",
            "BOOKINGSTATUS",
            "DATASOURCE",
            "ACCOUNT",
            "FIRST_MISSING_DATE",
            "LAST_MISSING_DATE",
            "TOTAL_IMPRESSIONS",
            "TOTAL_CLICKS",
            "TOTAL_COST",
            "ROW_COUNT",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.alert_type,
                row.alert_key,
                row.our_ref,
                row.job_number,
                row.start_date,
                row.end_date,
                row.advertiser,
                row.campaign,
                row.location_text,
                row.property_name,
                row.booking_status,
                row.datasource,
                row.account,
                row.first_missing_date,
                row.last_missing_date,
                row.total_impressions,
                row.total_clicks,
                row.total_cost,
                row.row_count,
            ]
        )
    return buf.getvalue().encode("utf-8")


def build_csv_not_live(rows: List[AlertRow]) -> bytes:
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


def build_csv_missing_our_ref(rows: List[AlertRow]) -> bytes:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "DATASOURCE",
            "ACCOUNT",
            "ADVERTISER",
            "CAMPAIGN",
            "JOB_NUMBER",
            "FIRST_MISSING_DATE",
            "LAST_MISSING_DATE",
            "TOTAL_IMPRESSIONS",
            "TOTAL_CLICKS",
            "TOTAL_COST",
            "ROW_COUNT",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.datasource,
                row.account,
                row.advertiser,
                row.campaign,
                row.job_number,
                row.first_missing_date,
                row.last_missing_date,
                row.total_impressions,
                row.total_clicks,
                row.total_cost,
                row.row_count,
            ]
        )
    return buf.getvalue().encode("utf-8")


def build_csv_ended_but_impressions(rows: List[AlertRow]) -> bytes:
    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(
        [
            "OUR_REF",
            "JOB_NUMBER",
            "ADVERTISER",
            "CAMPAIGN",
            "LOCATIONTEXT",
            "PROPERTYNAME",
            "BOOKINGSTATUS",
            "END_DATE",
            "FIRST_IMPRESSIONS_AFTER_END_DATE",
            "LAST_IMPRESSIONS_AFTER_END_DATE",
            "TOTAL_IMPRESSIONS_AFTER_END_DATE",
            "TOTAL_CLICKS_AFTER_END_DATE",
            "TOTAL_COST_AFTER_END_DATE",
            "TOTAL_ROWS_AFTER_END_DATE",
        ]
    )
    for row in rows:
        writer.writerow(
            [
                row.our_ref,
                row.job_number,
                row.advertiser,
                row.campaign,
                row.location_text,
                row.property_name,
                row.booking_status,
                row.end_date,
                row.first_missing_date,
                row.last_missing_date,
                row.total_impressions,
                row.total_clicks,
                row.total_cost,
                row.row_count,
            ]
        )
    return buf.getvalue().encode("utf-8")


def build_csv_stopped_impressions(rows: List[AlertRow]) -> bytes:
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
            "LATEST_DELIVERY_DATE",
            "PRIOR_7D_IMPRESSIONS",
            "PRIOR_7D_CLICKS",
            "PRIOR_7D_COST",
            "PRIOR_7D_ROWS",
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
                row.first_missing_date,
                row.total_impressions,
                row.total_clicks,
                row.total_cost,
                row.row_count,
            ]
        )
    return buf.getvalue().encode("utf-8")


def build_signed_dashboard_link(base_url: str, user: str, run_id: str, ttl_days: int, secret: str) -> str:
    normalized_base = base_url.strip()
    if not normalized_base.lower().startswith(("http://", "https://")):
        normalized_base = f"https://{normalized_base.lstrip('/')}"

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
    joiner = "&" if "?" in normalized_base else "?"
    return f"{normalized_base}{joiner}{query}"


def build_email_body(link: str, rows: List[AlertRow]) -> str:
    by_type: Dict[str, int] = {}
    for row in rows:
        by_type[row.alert_type] = by_type.get(row.alert_type, 0) + 1

    lines = ["Alert summary:"]
    for t in sorted(by_type.keys()):
        lines.append(f"- {t}: {by_type[t]}")

    lines.append("")
    lines.append("Please check attachments and dashboard for details.")
    lines.append("")
    lines.append(f"Open dashboard: {link}")
    return "\n".join(lines)


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
        subject = "ALERT: Campaign alert digest"

    now_nz = datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland"))
    all_name = f"campaign_alerts_all_{now_nz.strftime('%Y%m%d')}.csv"

    attachments = {all_name: build_csv_all(rows)}
    for alert_type in sorted({r.alert_type for r in rows}):
        type_rows = [r for r in rows if r.alert_type == alert_type]
        type_name = f"campaign_alerts_{alert_type.lower()}_{now_nz.strftime('%Y%m%d')}.csv"
        if alert_type == ALERT_TYPE_NOT_LIVE:
            attachments[type_name] = build_csv_not_live(type_rows)
        elif alert_type == ALERT_TYPE_MISSING_OUR_REF:
            attachments[type_name] = build_csv_missing_our_ref(type_rows)
        elif alert_type == ALERT_TYPE_ENDED_BUT_IMPRESSIONS:
            attachments[type_name] = build_csv_ended_but_impressions(type_rows)
        elif alert_type == ALERT_TYPE_STOPPED_IMPRESSIONS:
            attachments[type_name] = build_csv_stopped_impressions(type_rows)
        else:
            attachments[type_name] = build_csv_all(type_rows)

    sent_ids: List[str] = []
    for recipient in recipients:
        link = build_signed_dashboard_link(
            base_url=dashboard_base_url,
            user=recipient,
            run_id=run_id,
            ttl_days=link_ttl_days,
            secret=link_secret,
        )
        body = build_email_body(link, rows)
        msg_id = client.send_email(
            to_email=recipient,
            subject=subject,
            body_text=body,
            attachments=attachments,
        )
        sent_ids.append(msg_id)

    return ",".join(sent_ids)


def main() -> int:
    if not as_bool(env("ALERT_FORCE_RUN", ""), default=False) and not should_run_now_nz():
        print("Skip: current NZ time is outside 6:00 AM weekday window.")
        return 0

    bq_client = build_bq_client()
    ensure_control_tables(bq_client)

    rows = (
        fetch_not_live_rows(bq_client)
        + fetch_missing_our_ref_rows(bq_client)
        + fetch_ended_but_impressions_rows(bq_client)
        + fetch_stopped_impressions_rows(bq_client)
    )
    states = fetch_latest_snooze_states(bq_client, rows)
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
