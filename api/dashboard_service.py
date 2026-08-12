import json
import uuid
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account

from api.config import REPO_ROOT, get_secret


DEFAULT_BQ_PROJECT_ID = "sm-test-391201"
DEFAULT_BQ_DATASET = "supermetrics_data"
MARGIN_VIEW_NAME = "margin_dashboard"
ALERT_TYPE_NOT_LIVE = "NOT_LIVE"
ALERT_TYPE_STOPPED_IMPRESSIONS = "STOPPED_IMPRESSIONS"
ALERT_TYPE_MISSING_OUR_REF = "MISSING_OUR_REF"
ALERT_TYPE_ENDED_BUT_IMPRESSIONS = "ENDED_BUT_IMPRESSIONS"
MARGIN_SNOOZE_TYPE = "MARGIN_DASHBOARD"
PACING_TYPE_UNDER = "UNDERPACING"


class AlertConflictError(Exception):
    pass


def sanitize_id(value: str, field_name: str) -> str:
    clean = value.strip()
    if not clean:
        raise ValueError(f"{field_name} is required.")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if not set(clean).issubset(allowed):
        raise ValueError(f"{field_name} contains invalid characters: {clean}")
    return clean


def today_nz() -> date:
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland")).date()


@lru_cache(maxsize=1)
def bq_context() -> tuple[bigquery.Client, str, str]:
    project_id = sanitize_id(get_secret("BQ_PROJECT_ID", DEFAULT_BQ_PROJECT_ID), "BQ_PROJECT_ID")
    dataset = sanitize_id(get_secret("BQ_DATASET", DEFAULT_BQ_DATASET), "BQ_DATASET")
    sa_json = get_secret("BQ_SERVICE_ACCOUNT_JSON", "")

    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=project_id or info.get("project_id"), credentials=creds), project_id, dataset

    return bigquery.Client(project=project_id), project_id, dataset


def table_fqn(project_id: str, dataset: str, table: str) -> str:
    return f"`{project_id}.{dataset}.{table}`"


def margin_view_fqn(project_id: str, dataset: str) -> str:
    return table_fqn(project_id, dataset, MARGIN_VIEW_NAME)


def margin_view_sql(project_id: str, dataset: str) -> str:
    sql_path = REPO_ROOT / "sql" / "margin_dashboard_view.sql"
    template = sql_path.read_text(encoding="utf-8")
    source = f"{DEFAULT_BQ_PROJECT_ID}.{DEFAULT_BQ_DATASET}"
    target = f"{project_id}.{dataset}"
    return template.replace(source, target)


def ensure_control_tables(client: bigquery.Client, project_id: str, dataset: str) -> None:
    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {table_fqn(project_id, dataset, "snoozes")} (
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
CREATE TABLE IF NOT EXISTS {table_fqn(project_id, dataset, "live_alert_snapshots")} (
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
    client.query(f"ALTER TABLE {table_fqn(project_id, dataset, 'snoozes')} ADD COLUMN IF NOT EXISTS alert_key STRING").result()
    client.query(f"ALTER TABLE {table_fqn(project_id, dataset, 'live_alert_snapshots')} ADD COLUMN IF NOT EXISTS alert_key STRING").result()


def ensure_margin_view(client: bigquery.Client, project_id: str, dataset: str) -> None:
    client.query(margin_view_sql(project_id, dataset)).result()


def records_from_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{k: (v.isoformat() if hasattr(v, "isoformat") else v) for k, v in row.items()} for row in rows]


def alert_counts(df: pd.DataFrame) -> dict[str, int]:
    counts = {
        ALERT_TYPE_NOT_LIVE: 0,
        ALERT_TYPE_STOPPED_IMPRESSIONS: 0,
        ALERT_TYPE_MISSING_OUR_REF: 0,
        ALERT_TYPE_ENDED_BUT_IMPRESSIONS: 0,
    }
    if df.empty:
        return counts
    open_df = df[df["LIVE_ALERT_STATE"] == "OPEN"]
    grouped = open_df.groupby("ALERT_TYPE").size().to_dict()
    for key in counts:
        counts[key] = int(grouped.get(key, 0))
    return counts


def alert_state_counts(df: pd.DataFrame) -> tuple[int, int, int]:
    if df.empty:
        return 0, 0, 0
    return (
        int((df["LIVE_ALERT_STATE"] == "OPEN").sum()),
        int((df["LIVE_ALERT_STATE"] == "ACTIVE").sum()),
        int((df["LIVE_ALERT_STATE"] == "DISMISSED").sum()),
    )


def normalize_merged_our_ref(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "OUR_REF_x" in df.columns:
        left_ref = df["OUR_REF_x"].fillna("").astype(str)
        right_ref = df["OUR_REF_y"].fillna("").astype(str) if "OUR_REF_y" in df.columns else ""
        df["OUR_REF"] = left_ref.where(left_ref.str.strip() != "", right_ref)
        df = df.drop(columns=[col for col in ["OUR_REF_x", "OUR_REF_y"] if col in df.columns])
    return df


def alert_meta(
    project_id: str,
    dataset: str,
    counts: dict[str, int],
    page: int = 1,
    page_size: int = 100,
    total_rows: int = 0,
    total_pages: int = 1,
    open_count: int = 0,
    snoozed_count: int = 0,
    dismissed_count: int = 0,
    latest_run: str = "",
) -> dict[str, str]:
    return {
        "alert_api_version": "3",
        "project_id": project_id,
        "dataset": dataset,
        "latest_run": latest_run,
        "page": str(page),
        "page_size": str(page_size),
        "total_rows": str(total_rows),
        "total_pages": str(total_pages),
        "open_count": str(open_count),
        "snoozed_count": str(snoozed_count),
        "dismissed_count": str(dismissed_count),
        "count_not_live": str(counts[ALERT_TYPE_NOT_LIVE]),
        "count_stopped_impressions": str(counts[ALERT_TYPE_STOPPED_IMPRESSIONS]),
        "count_missing_our_ref": str(counts[ALERT_TYPE_MISSING_OUR_REF]),
        "count_ended_but_impressions": str(counts[ALERT_TYPE_ENDED_BUT_IMPRESSIONS]),
    }


def latest_snoozes(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alert_keys: list[str],
    alert_types: list[str],
    legacy_not_live_refs: list[str] | None = None,
) -> pd.DataFrame:
    if not alert_keys:
        return pd.DataFrame()
    query = f"""
WITH latest AS (
  SELECT
    COALESCE(alert_key, our_ref) AS alert_key,
    snooze_type,
    our_ref,
    snooze_status,
    snooze_reason,
    snooze_start_date,
    snooze_end_date,
    snoozed_by,
    dismissed_by,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(alert_key, our_ref), snooze_type ORDER BY updated_at DESC) AS rn
  FROM {table_fqn(project_id, dataset, "snoozes")}
  WHERE snooze_type IN UNNEST(@alert_types)
    AND (
      COALESCE(alert_key, our_ref) IN UNNEST(@alert_keys)
      OR (snooze_type = @not_live_type AND our_ref IN UNNEST(@legacy_not_live_refs))
    )
)
SELECT * FROM latest WHERE rn = 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ScalarQueryParameter("not_live_type", "STRING", ALERT_TYPE_NOT_LIVE),
            bigquery.ArrayQueryParameter("legacy_not_live_refs", "STRING", legacy_not_live_refs or []),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    return pd.DataFrame(
        [
            {
                "ALERT_KEY": str(r["alert_key"] or ""),
                "ALERT_TYPE": str(r["snooze_type"] or ""),
                "OUR_REF": str(r["our_ref"] or ""),
                "SNOOZE_STATUS": str(r["snooze_status"] or ""),
                "SNOOZE_REASON": str(r["snooze_reason"] or ""),
                "SNOOZE_START_DATE": str(r["snooze_start_date"] or ""),
                "SNOOZE_END_DATE": str(r["snooze_end_date"] or ""),
                "SNOOZED_BY": str(r["snoozed_by"] or ""),
                "DISMISSED_BY": str(r["dismissed_by"] or ""),
                "UPDATED_AT": str(r["updated_at"] or ""),
            }
            for r in rows
        ]
    )


def global_latest_snoozes(client: bigquery.Client, project_id: str, dataset: str) -> pd.DataFrame:
    query = f"""
WITH latest AS (
  SELECT
    COALESCE(alert_key, our_ref) AS alert_key,
    snooze_type,
    our_ref,
    snooze_status,
    snooze_reason,
    snooze_start_date,
    snooze_end_date,
    snoozed_by,
    dismissed_by,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY COALESCE(alert_key, our_ref), snooze_type ORDER BY updated_at DESC) AS rn
  FROM {table_fqn(project_id, dataset, "snoozes")}
),
snapshot_latest AS (
  SELECT
    alert_type,
    alert_key,
    advertiser,
    campaign,
    property_name,
    start_date,
    end_date,
    ROW_NUMBER() OVER (PARTITION BY alert_type, alert_key ORDER BY run_timestamp_utc DESC) AS rn
  FROM {table_fqn(project_id, dataset, "live_alert_snapshots")}
)
SELECT
  l.alert_key,
  l.snooze_type,
  l.our_ref,
  l.snooze_status,
  l.snooze_reason,
  l.snooze_start_date,
  l.snooze_end_date,
  l.snoozed_by,
  l.dismissed_by,
  l.updated_at,
  COALESCE(s.advertiser, '') AS advertiser,
  COALESCE(s.campaign, '') AS campaign,
  COALESCE(s.property_name, '') AS property_name,
  s.start_date,
  s.end_date
FROM latest l
LEFT JOIN snapshot_latest s
  ON s.alert_type = l.snooze_type
 AND s.alert_key = l.alert_key
 AND s.rn = 1
WHERE l.rn = 1
  AND (
    (
      UPPER(COALESCE(l.snooze_status, '')) = 'ACTIVE'
      AND (l.snooze_end_date IS NULL OR l.snooze_end_date >= CURRENT_DATE('Pacific/Auckland'))
    )
    OR UPPER(COALESCE(l.snooze_status, '')) = 'DISMISSED'
  )
ORDER BY l.updated_at DESC
"""
    rows = list(client.query(query).result())
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
                "RUN_ID": "",
                "RUN_DATE_NZ": "",
                "RUN_TS_UTC": str(r["updated_at"] or ""),
                "ALERT_TYPE": str(r["snooze_type"] or ""),
                "ALERT_KEY": str(r["alert_key"] or ""),
                "OUR_REF": str(r["our_ref"] or ""),
                "JOB_NUMBER": "",
                "START_DATE": str(r["start_date"] or ""),
                "END_DATE": str(r["end_date"] or ""),
                "ADVERTISER": str(r["advertiser"] or ""),
                "CAMPAIGN": str(r["campaign"] or ""),
                "LOCATIONTEXT": "",
                "PROPERTYNAME": str(r["property_name"] or ""),
                "BOOKINGSTATUS": "",
                "DATASOURCE": "",
                "ACCOUNT": "",
                "FIRST_MISSING_DATE": "",
                "LAST_MISSING_DATE": "",
                "TOTAL_IMPRESSIONS": 0.0,
                "TOTAL_CLICKS": 0.0,
                "TOTAL_COST": 0.0,
                "ROW_COUNT": 0,
                "SNOOZE_STATUS": str(r["snooze_status"] or ""),
                "SNOOZE_REASON": str(r["snooze_reason"] or ""),
                "SNOOZE_START_DATE": str(r["snooze_start_date"] or ""),
                "SNOOZE_END_DATE": str(r["snooze_end_date"] or ""),
                "SNOOZED_BY": str(r["snoozed_by"] or ""),
                "DISMISSED_BY": str(r["dismissed_by"] or ""),
                "UPDATED_AT": str(r["updated_at"] or ""),
                "LIVE_ALERT_STATE": "DISMISSED"
                if str(r["snooze_status"] or "").upper() == "DISMISSED"
                else "ACTIVE",
                "SOURCE_VIEW": "GLOBAL_SNOOZE",
            }
            for r in rows
        ]
    )


def margin_dashboard() -> dict[str, Any]:
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    ensure_margin_view(client, project_id, dataset)

    rows = list(
        client.query(
            f"""
SELECT
  our_ref, job_number, campaign_name, advertiser_name, property_name,
  location_text, account_manager_name, trafficker_name, campaign_lead,
  booking_status, budget, booked_nett_cost, start_date, end_date,
  latest_delivery_date, as_of_date, total_days, elapsed_days, pacing_ratio,
  actual_nett_spend, total_impressions, total_clicks, first_delivery_date,
  last_delivery_date, expected_gross_spend_to_date, margin_amount, margin_pct,
  spend_vs_budget_ratio
FROM {margin_view_fqn(project_id, dataset)}
ORDER BY margin_amount ASC, our_ref
"""
        ).result()
    )
    data = [
        {
            "OUR_REF": str(r["our_ref"] or ""),
            "JOB_NUMBER": str(r["job_number"] or ""),
            "CAMPAIGN_NAME": str(r["campaign_name"] or ""),
            "ADVERTISER_NAME": str(r["advertiser_name"] or ""),
            "PROPERTY_NAME": str(r["property_name"] or ""),
            "LOCATION_TEXT": str(r["location_text"] or ""),
            "ACCOUNT_MANAGER": str(r["account_manager_name"] or ""),
            "TRAFFICKER_NAME": str(r["trafficker_name"] or ""),
            "CAMPAIGN_LEAD": str(r["campaign_lead"] or ""),
            "BOOKING_STATUS": str(r["booking_status"] or ""),
            "BUDGET": float(r["budget"] or 0),
            "BOOKED_NETT_COST": float(r["booked_nett_cost"] or 0),
            "START_DATE": str(r["start_date"] or ""),
            "END_DATE": str(r["end_date"] or ""),
            "LATEST_DELIVERY_DATE": str(r["latest_delivery_date"] or ""),
            "AS_OF_DATE": str(r["as_of_date"] or ""),
            "TOTAL_DAYS": int(r["total_days"] or 0),
            "ELAPSED_DAYS": int(r["elapsed_days"] or 0),
            "PACING_RATIO": float(r["pacing_ratio"] or 0),
            "ACTUAL_NETT_SPEND": float(r["actual_nett_spend"] or 0),
            "TOTAL_IMPRESSIONS": float(r["total_impressions"] or 0),
            "TOTAL_CLICKS": float(r["total_clicks"] or 0),
            "FIRST_DELIVERY_DATE": str(r["first_delivery_date"] or ""),
            "LAST_DELIVERY_DATE": str(r["last_delivery_date"] or ""),
            "EXPECTED_GROSS_SPEND_TO_DATE": float(r["expected_gross_spend_to_date"] or 0),
            "MARGIN_AMOUNT": float(r["margin_amount"] or 0),
            "MARGIN_PCT": float(r["margin_pct"]) if r["margin_pct"] is not None else None,
            "SPEND_VS_BUDGET_RATIO": float(r["spend_vs_budget_ratio"]) if r["spend_vs_budget_ratio"] is not None else None,
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    if df.empty:
        return {"rows": [], "meta": {"project_id": project_id, "dataset": dataset}}

    snooze_df = latest_snoozes(
        client,
        project_id,
        dataset,
        alert_keys=sorted(df["OUR_REF"].dropna().astype(str).unique().tolist()),
        alert_types=[MARGIN_SNOOZE_TYPE],
    )
    if not snooze_df.empty:
        df = df.merge(
            snooze_df[["ALERT_KEY", "OUR_REF", "SNOOZE_STATUS", "SNOOZE_REASON", "SNOOZE_START_DATE", "SNOOZE_END_DATE", "SNOOZED_BY", "UPDATED_AT"]],
            on="OUR_REF",
            how="left",
        )
    else:
        for col in ["SNOOZE_STATUS", "SNOOZE_REASON", "SNOOZE_START_DATE", "SNOOZE_END_DATE", "SNOOZED_BY", "UPDATED_AT"]:
            df[col] = ""

    today = today_nz()
    states: list[str] = []
    for _, row in df.iterrows():
        status = str(row.get("SNOOZE_STATUS", "") or "").upper()
        end_date_raw = str(row.get("SNOOZE_END_DATE", "") or "").strip()
        end_date = pd.to_datetime(end_date_raw, errors="coerce")
        states.append("ACTIVE" if status == "ACTIVE" and (not end_date_raw or (not pd.isna(end_date) and end_date.date() >= today)) else "OPEN")
    df["MARGIN_SNOOZE_STATE"] = states
    df["STATE_VERSION"] = df["UPDATED_AT"].fillna("").astype(str)
    return {
        "rows": records_from_rows(df.fillna("").to_dict(orient="records")),
        "meta": {"project_id": project_id, "dataset": dataset, "view": MARGIN_VIEW_NAME},
    }


def pacing_dashboard() -> dict[str, Any]:
    client, project_id, dataset = bq_context()
    rows = list(
        client.query(
            f"""
WITH latest_delivery AS (
  SELECT MAX(DATE) AS latest_delivery_date
  FROM {table_fqn(project_id, dataset, "BLEND_BLEND_5_1_2")}
),
line_items AS (
  SELECT
    TRIM(CAST(OURREF AS STRING)) AS our_ref,
    CAST(JOBNUMBER AS STRING) AS job_number,
    MAX(NULLIF(TRIM(CAST(DATASOURCE AS STRING)), '')) AS datasource,
    MAX(NULLIF(TRIM(CAST(CAMPAIGNNAME AS STRING)), '')) AS campaign_name,
    MAX(NULLIF(TRIM(CAST(ADVERTISERNAME AS STRING)), '')) AS advertiser_name,
    MAX(NULLIF(TRIM(CAST(PROPERTYNAME AS STRING)), '')) AS property_name,
    MAX(NULLIF(TRIM(CAST(LOCATIONTEXT AS STRING)), '')) AS location_text,
    MAX(NULLIF(TRIM(CAST(ACCOUNTMANAGERNAME AS STRING)), '')) AS account_manager_name,
    MAX(NULLIF(TRIM(CAST(TRAFFICKERNAME AS STRING)), '')) AS trafficker_name,
    MAX(NULLIF(TRIM(CAST(CAMPAIGNLEAD AS STRING)), '')) AS campaign_lead,
    MAX(NULLIF(TRIM(CAST(BOOKINGSTATUS AS STRING)), '')) AS booking_status,
    MAX(COALESCE(SAFE_CAST(ACTUALPRICE AS FLOAT64), 0)) AS budget,
    MAX(COALESCE(SAFE_CAST(OURCOST AS FLOAT64), 0)) AS booked_nett_cost,
    COALESCE(
      MIN(SAFE_CAST(STARTDATE AS DATE)),
      MIN(SAFE.PARSE_DATE('%Y-%m-%d', CAST(STARTDATE AS STRING))),
      MIN(SAFE.PARSE_DATE('%d/%m/%Y', CAST(STARTDATE AS STRING))),
      MIN(SAFE.PARSE_DATE('%m/%d/%Y', CAST(STARTDATE AS STRING)))
    ) AS start_date,
    COALESCE(
      MAX(SAFE_CAST(ENDDATE AS DATE)),
      MAX(SAFE.PARSE_DATE('%Y-%m-%d', CAST(ENDDATE AS STRING))),
      MAX(SAFE.PARSE_DATE('%d/%m/%Y', CAST(ENDDATE AS STRING))),
      MAX(SAFE.PARSE_DATE('%m/%d/%Y', CAST(ENDDATE AS STRING)))
    ) AS end_date
  FROM {table_fqn(project_id, dataset, "master_overview")}
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
  GROUP BY 1, 2
),
our_ref_rollup AS (
  SELECT
    our_ref,
    STRING_AGG(DISTINCT datasource, ', ' ORDER BY datasource) AS datasources,
    STRING_AGG(DISTINCT NULLIF(TRIM(job_number), ''), ', ' ORDER BY NULLIF(TRIM(job_number), '')) AS job_numbers,
    STRING_AGG(DISTINCT campaign_name, ' | ' ORDER BY campaign_name) AS campaign_names,
    STRING_AGG(DISTINCT advertiser_name, ', ' ORDER BY advertiser_name) AS advertiser_names,
    STRING_AGG(DISTINCT property_name, ' | ' ORDER BY property_name) AS property_names,
    STRING_AGG(DISTINCT location_text, ' | ' ORDER BY location_text) AS location_texts,
    STRING_AGG(DISTINCT account_manager_name, ', ' ORDER BY account_manager_name) AS account_manager_names,
    STRING_AGG(DISTINCT trafficker_name, ', ' ORDER BY trafficker_name) AS trafficker_names,
    STRING_AGG(DISTINCT campaign_lead, ', ' ORDER BY campaign_lead) AS campaign_leads,
    STRING_AGG(DISTINCT booking_status, ', ' ORDER BY booking_status) AS booking_statuses,
    SUM(COALESCE(budget, 0)) AS budget,
    SUM(COALESCE(booked_nett_cost, 0)) AS booked_nett_cost,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date
  FROM line_items
  GROUP BY 1
),
delivery AS (
  SELECT
    TRIM(CAST(OUR_REF AS STRING)) AS our_ref,
    SUM(COALESCE(COST, 0)) AS actual_nett_spend,
    SUM(COALESCE(IMPRESSIONS, 0)) AS total_impressions,
    SUM(COALESCE(CLICKS, 0)) AS total_clicks,
    MIN(DATE) AS first_delivery_date,
    MAX(DATE) AS last_delivery_date
  FROM {table_fqn(project_id, dataset, "BLEND_BLEND_5_1_2")}
  WHERE OUR_REF IS NOT NULL
    AND TRIM(CAST(OUR_REF AS STRING)) != ''
  GROUP BY 1
),
base AS (
  SELECT
    l.our_ref,
    l.datasources,
    l.job_numbers,
    l.campaign_names,
    l.advertiser_names,
    l.property_names,
    l.location_texts,
    l.account_manager_names,
    l.trafficker_names,
    l.campaign_leads,
    l.booking_statuses,
    l.budget,
    l.booked_nett_cost,
    l.start_date,
    l.end_date,
    ld.latest_delivery_date,
    LEAST(ld.latest_delivery_date, l.end_date) AS as_of_date,
    DATE_DIFF(l.end_date, l.start_date, DAY) + 1 AS total_days,
    GREATEST(
      0,
      LEAST(
        DATE_DIFF(l.end_date, l.start_date, DAY) + 1,
        DATE_DIFF(LEAST(ld.latest_delivery_date, l.end_date), l.start_date, DAY) + 1
      )
    ) AS elapsed_days,
    COALESCE(d.actual_nett_spend, 0) AS actual_nett_spend,
    COALESCE(d.total_impressions, 0) AS total_impressions,
    COALESCE(d.total_clicks, 0) AS total_clicks,
    d.first_delivery_date,
    d.last_delivery_date
  FROM our_ref_rollup l
  -- Roll up at OUR_REF level after deduping repeated daily rows at JOB_NUMBER level.
  CROSS JOIN latest_delivery ld
  LEFT JOIN delivery d
    ON d.our_ref = l.our_ref
  WHERE l.start_date IS NOT NULL
    AND l.end_date IS NOT NULL
    AND l.end_date >= l.start_date
)
SELECT
  our_ref,
  datasources,
  job_numbers,
  campaign_names,
  advertiser_names,
  property_names,
  location_texts,
  account_manager_names,
  trafficker_names,
  campaign_leads,
  booking_statuses,
  budget,
  booked_nett_cost,
  start_date,
  end_date,
  latest_delivery_date,
  as_of_date,
  total_days,
  elapsed_days,
  SAFE_DIVIDE(elapsed_days, total_days) AS pacing_ratio,
  actual_nett_spend,
  total_impressions,
  total_clicks,
  first_delivery_date,
  last_delivery_date,
  SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days)) AS expected_spend_to_date,
  SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days)) - actual_nett_spend AS pacing_delta,
  SAFE_DIVIDE(actual_nett_spend, SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days))) AS spend_vs_expected_ratio,
  SAFE_DIVIDE(actual_nett_spend, budget) AS spend_vs_budget_ratio
FROM base
WHERE SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days)) > 0
  AND SAFE_DIVIDE(actual_nett_spend, SAFE_MULTIPLY(budget, SAFE_DIVIDE(elapsed_days, total_days))) <= 0.9
ORDER BY spend_vs_expected_ratio ASC, pacing_delta ASC, our_ref
"""
        ).result()
    )

    data: list[dict[str, Any]] = []
    for r in rows:
        actual_nett_spend = float(r["actual_nett_spend"] or 0)
        expected_spend_to_date = float(r["expected_spend_to_date"] or 0)
        time_progress_ratio = float(r["pacing_ratio"] or 0)
        spend_vs_expected_ratio = float(r["spend_vs_expected_ratio"]) if r["spend_vs_expected_ratio"] is not None else None
        pacing_delta = float(r["pacing_delta"] or 0)

        data.append(
            {
                "OUR_REF": str(r["our_ref"] or ""),
                "DATASOURCE": str(r["datasources"] or ""),
                "JOB_NUMBER": str(r["job_numbers"] or ""),
                "CAMPAIGN_NAME": str(r["campaign_names"] or ""),
                "ADVERTISER_NAME": str(r["advertiser_names"] or ""),
                "PROPERTY_NAME": str(r["property_names"] or ""),
                "LOCATION_TEXT": str(r["location_texts"] or ""),
                "ACCOUNT_MANAGER": str(r["account_manager_names"] or ""),
                "TRAFFICKER_NAME": str(r["trafficker_names"] or ""),
                "CAMPAIGN_LEAD": str(r["campaign_leads"] or ""),
                "BOOKING_STATUS": str(r["booking_statuses"] or ""),
                "BUDGET": float(r["budget"] or 0),
                "BOOKED_NETT_COST": float(r["booked_nett_cost"] or 0),
                "START_DATE": str(r["start_date"] or ""),
                "END_DATE": str(r["end_date"] or ""),
                "LATEST_DELIVERY_DATE": str(r["latest_delivery_date"] or ""),
                "AS_OF_DATE": str(r["as_of_date"] or ""),
                "TOTAL_DAYS": int(r["total_days"] or 0),
                "ELAPSED_DAYS": int(r["elapsed_days"] or 0),
                "TIME_PROGRESS_RATIO": time_progress_ratio,
                "ACTUAL_NETT_SPEND": actual_nett_spend,
                "EXPECTED_SPEND_TO_DATE": expected_spend_to_date,
                "PACING_DELTA": pacing_delta,
                "SPEND_VS_EXPECTED_RATIO": spend_vs_expected_ratio,
                "PACING_STATUS": "UNDER",
                "PACING_BUCKET": PACING_TYPE_UNDER,
                "TOTAL_IMPRESSIONS": float(r["total_impressions"] or 0),
                "TOTAL_CLICKS": float(r["total_clicks"] or 0),
                "FIRST_DELIVERY_DATE": str(r["first_delivery_date"] or ""),
                "LAST_DELIVERY_DATE": str(r["last_delivery_date"] or ""),
                "SPEND_VS_BUDGET_RATIO": float(r["spend_vs_budget_ratio"]) if r["spend_vs_budget_ratio"] is not None else None,
            }
        )

    return {
        "rows": records_from_rows(data),
        "meta": {
            "project_id": project_id,
            "dataset": dataset,
            "source_table": "master_overview",
            "count_underpacing": str(len(data)),
        },
    }


def alerts_dataset() -> tuple[pd.DataFrame, dict[str, str]]:
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    global_snooze_df = global_latest_snoozes(client, project_id, dataset)
    rows = list(
        client.query(
            f"""
WITH latest_run AS (
  SELECT run_id, run_timestamp_utc
  FROM {table_fqn(project_id, dataset, "live_alert_snapshots")}
  ORDER BY run_timestamp_utc DESC
  LIMIT 1
)
SELECT
  s.run_id, s.run_date_nz, s.run_timestamp_utc, s.alert_type, s.alert_key,
  s.our_ref, s.job_number, s.start_date, s.end_date, s.advertiser, s.campaign,
  s.location_text, s.property_name, s.booking_status, s.datasource, s.account,
  s.first_missing_date, s.last_missing_date, s.total_impressions, s.total_clicks,
  s.total_cost, s.row_count
FROM {table_fqn(project_id, dataset, "live_alert_snapshots")} s
INNER JOIN latest_run lr ON s.run_id = lr.run_id
ORDER BY s.run_timestamp_utc DESC, s.alert_type, s.our_ref
"""
        ).result()
    )
    data = [
        {
            "RUN_ID": str(r["run_id"] or ""),
            "RUN_DATE_NZ": str(r["run_date_nz"] or ""),
            "RUN_TS_UTC": str(r["run_timestamp_utc"] or ""),
            "ALERT_TYPE": str(r["alert_type"] or ""),
            "ALERT_KEY": str(r["alert_key"] or ""),
            "OUR_REF": str(r["our_ref"] or ""),
            "JOB_NUMBER": str(r["job_number"] or ""),
            "START_DATE": str(r["start_date"] or ""),
            "END_DATE": str(r["end_date"] or ""),
            "ADVERTISER": str(r["advertiser"] or ""),
            "CAMPAIGN": str(r["campaign"] or ""),
            "LOCATIONTEXT": str(r["location_text"] or ""),
            "PROPERTYNAME": str(r["property_name"] or ""),
            "BOOKINGSTATUS": str(r["booking_status"] or ""),
            "DATASOURCE": str(r["datasource"] or ""),
            "ACCOUNT": str(r["account"] or ""),
            "FIRST_MISSING_DATE": str(r["first_missing_date"] or ""),
            "LAST_MISSING_DATE": str(r["last_missing_date"] or ""),
            "TOTAL_IMPRESSIONS": float(r["total_impressions"] or 0),
            "TOTAL_CLICKS": float(r["total_clicks"] or 0),
            "TOTAL_COST": float(r["total_cost"] or 0),
            "ROW_COUNT": int(r["row_count"] or 0),
        }
        for r in rows
    ]
    df = pd.DataFrame(data)
    if df.empty:
        visible_df = global_snooze_df.copy()
        counts = alert_counts(visible_df) if not visible_df.empty else alert_counts(pd.DataFrame())
        open_count, snoozed_count, dismissed_count = alert_state_counts(visible_df)
        if not visible_df.empty:
            visible_df["STATE_VERSION"] = visible_df["UPDATED_AT"].fillna("").astype(str)
        return visible_df, alert_meta(
            project_id=project_id,
            dataset=dataset,
            counts=counts,
            total_rows=len(visible_df.index),
            open_count=open_count,
            snoozed_count=snoozed_count,
            dismissed_count=dismissed_count,
        )

    snooze_df = latest_snoozes(
        client,
        project_id,
        dataset,
        alert_keys=sorted(df["ALERT_KEY"].dropna().astype(str).unique().tolist()),
        alert_types=sorted(df["ALERT_TYPE"].dropna().astype(str).unique().tolist()),
        legacy_not_live_refs=sorted(df.loc[df["ALERT_TYPE"] == ALERT_TYPE_NOT_LIVE, "OUR_REF"].dropna().astype(str).unique().tolist()),
    )
    if not snooze_df.empty:
        df = df.merge(snooze_df, on=["ALERT_TYPE", "ALERT_KEY"], how="left")
        df = normalize_merged_our_ref(df)
    else:
        for col in ["SNOOZE_STATUS", "SNOOZE_REASON", "SNOOZE_START_DATE", "SNOOZE_END_DATE", "SNOOZED_BY", "DISMISSED_BY", "UPDATED_AT"]:
            df[col] = ""

    today = today_nz()
    states: list[str] = []
    for _, row in df.iterrows():
        status = str(row.get("SNOOZE_STATUS", "") or "").upper()
        end_date_raw = str(row.get("SNOOZE_END_DATE", "") or "").strip()
        end_date = pd.to_datetime(end_date_raw, errors="coerce")
        if status == "DISMISSED":
            states.append("DISMISSED")
        elif status == "ACTIVE" and (not end_date_raw or (not pd.isna(end_date) and end_date.date() >= today)):
            states.append("ACTIVE")
        else:
            states.append("OPEN")
    df["LIVE_ALERT_STATE"] = states
    df["SOURCE_VIEW"] = "LATEST_RUN"

    visible_df = df[df["LIVE_ALERT_STATE"] == "OPEN"].copy()
    if not global_snooze_df.empty:
        visible_df = pd.concat([visible_df, global_snooze_df], ignore_index=True, sort=False)
    visible_df["STATE_VERSION"] = visible_df["UPDATED_AT"].fillna("").astype(str)
    counts = alert_counts(visible_df)
    open_count, snoozed_count, dismissed_count = alert_state_counts(visible_df)
    return visible_df, alert_meta(
        project_id=project_id,
        dataset=dataset,
        counts=counts,
        total_rows=len(visible_df.index),
        open_count=open_count,
        snoozed_count=snoozed_count,
        dismissed_count=dismissed_count,
        latest_run=str(visible_df.iloc[0]["RUN_TS_UTC"]) if not visible_df.empty else "",
    )


def alerts_bootstrap() -> dict[str, Any]:
    visible_df, meta = alerts_dataset()
    return {"rows": records_from_rows(visible_df.fillna("").to_dict(orient="records")), "meta": meta}


def alerts_dashboard(
    alert_type: str | None = None,
    state: str = "OPEN",
    query_text: str = "",
    page: int = 1,
    page_size: int = 100,
) -> dict[str, Any]:
    visible_df, meta = alerts_dataset()
    scoped_df = visible_df.copy()
    if scoped_df.empty:
        return {
            "rows": [],
            "meta": alert_meta(
                project_id=meta["project_id"],
                dataset=meta["dataset"],
                counts={
                    ALERT_TYPE_NOT_LIVE: int(meta["count_not_live"]),
                    ALERT_TYPE_STOPPED_IMPRESSIONS: int(meta["count_stopped_impressions"]),
                    ALERT_TYPE_MISSING_OUR_REF: int(meta["count_missing_our_ref"]),
                    ALERT_TYPE_ENDED_BUT_IMPRESSIONS: int(meta["count_ended_but_impressions"]),
                },
                page=1,
                page_size=page_size,
                latest_run=meta["latest_run"],
            ),
        }
    if alert_type:
        scoped_df = scoped_df[scoped_df["ALERT_TYPE"] == alert_type].copy()

    scoped_open = int((scoped_df["LIVE_ALERT_STATE"] == "OPEN").sum()) if not scoped_df.empty else 0
    scoped_snoozed = int((scoped_df["LIVE_ALERT_STATE"] == "ACTIVE").sum()) if not scoped_df.empty else 0
    scoped_dismissed = int((scoped_df["LIVE_ALERT_STATE"] == "DISMISSED").sum()) if not scoped_df.empty else 0

    normalized_state = state.upper().strip() or "OPEN"
    if normalized_state == "SNOOZED":
        normalized_state = "ACTIVE"
    if normalized_state != "ALL":
        scoped_df = scoped_df[scoped_df["LIVE_ALERT_STATE"] == normalized_state].copy()

    if query_text.strip():
        q = query_text.strip().lower()
        search_cols = [
            "ALERT_TYPE",
            "ALERT_KEY",
            "OUR_REF",
            "JOB_NUMBER",
            "ADVERTISER",
            "CAMPAIGN",
            "SNOOZE_REASON",
            "SNOOZED_BY",
            "DISMISSED_BY",
        ]
        haystack = scoped_df[search_cols].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        scoped_df = scoped_df[haystack.str.contains(q, regex=False)].copy()

    page_size = max(1, min(int(page_size), 500))
    total_rows = int(len(scoped_df.index))
    total_pages = max(1, (total_rows + page_size - 1) // page_size)
    page = max(1, min(int(page), total_pages))
    start = (page - 1) * page_size
    end = start + page_size
    scoped_df = scoped_df.iloc[start:end].copy()
    return {
        "rows": records_from_rows(scoped_df.fillna("").to_dict(orient="records")),
        "meta": alert_meta(
            project_id=meta["project_id"],
            dataset=meta["dataset"],
            counts={
                ALERT_TYPE_NOT_LIVE: int(meta["count_not_live"]),
                ALERT_TYPE_STOPPED_IMPRESSIONS: int(meta["count_stopped_impressions"]),
                ALERT_TYPE_MISSING_OUR_REF: int(meta["count_missing_our_ref"]),
                ALERT_TYPE_ENDED_BUT_IMPRESSIONS: int(meta["count_ended_but_impressions"]),
            },
            page=page,
            page_size=page_size,
            total_rows=total_rows,
            total_pages=total_pages,
            open_count=scoped_open,
            snoozed_count=scoped_snoozed,
            dismissed_count=scoped_dismissed,
            latest_run=meta["latest_run"],
        ),
    }


def write_snooze_action(action: str, alerts: list[dict[str, str]], user: str, reason: str = "", end_date: str | None = None, run_id: str = "") -> None:
    if not alerts:
        return
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    run_id = run_id or f"react_dashboard:{uuid.uuid4().hex}"
    our_refs = [str(a.get("our_ref", "") or "") for a in alerts]
    alert_keys = [str(a.get("alert_key", "") or "") for a in alerts]
    alert_types = [str(a.get("alert_type", "") or "") for a in alerts]
    expected_versions = [str(a.get("state_version", a.get("expected_updated_at", "")) or "") for a in alerts]

    if action == "snooze":
        set_clause = """
  alert_key = S.alert_key,
  snooze_status = 'ACTIVE',
  snooze_reason = @reason,
  snooze_start_date = @start_date,
  snooze_end_date = @end_date,
  snoozed_by = @user,
  run_id = @run_id,
  updated_at = CURRENT_TIMESTAMP(),
  unsnoozed_by = NULL,
  unsnoozed_at = NULL,
  dismissed_by = NULL,
  dismissed_at = NULL
"""
        values_clause = """
    S.our_ref, S.alert_key, S.alert_type, 'ACTIVE', @reason, @start_date, @end_date,
    @user, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, NULL, NULL
"""
    elif action == "unsnooze":
        set_clause = """
  alert_key = S.alert_key,
  snooze_status = 'UNSNOOZED',
  snooze_reason = 'Manual unsnooze',
  run_id = @run_id,
  unsnoozed_by = @user,
  unsnoozed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
"""
        values_clause = """
    S.our_ref, S.alert_key, S.alert_type, 'UNSNOOZED', 'Manual unsnooze', NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @user, CURRENT_TIMESTAMP(), NULL, NULL
"""
    elif action == "dismiss":
        set_clause = """
  alert_key = S.alert_key,
  snooze_status = 'DISMISSED',
  snooze_reason = @reason,
  run_id = @run_id,
  dismissed_by = @user,
  dismissed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
"""
        values_clause = """
    S.our_ref, S.alert_key, S.alert_type, 'DISMISSED', @reason, NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, @user, CURRENT_TIMESTAMP()
"""
    else:
        raise ValueError(f"Unknown action: {action}")

    query = f"""
BEGIN TRANSACTION;

ASSERT (
  WITH source AS (
    SELECT
      refs[OFFSET(i)] AS our_ref,
      keys[OFFSET(i)] AS alert_key,
      types[OFFSET(i)] AS alert_type,
      expected_versions[OFFSET(i)] AS expected_version
    FROM (SELECT @our_refs AS refs, @alert_keys AS keys, @alert_types AS types, @expected_versions AS expected_versions),
    UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(refs) - 1)) AS i
  ),
  latest AS (
    SELECT
      COALESCE(alert_key, our_ref) AS alert_key,
      snooze_type,
      updated_at,
      ROW_NUMBER() OVER (PARTITION BY COALESCE(alert_key, our_ref), snooze_type ORDER BY updated_at DESC) AS rn
    FROM {table_fqn(project_id, dataset, "snoozes")}
    WHERE COALESCE(alert_key, our_ref) IN UNNEST(@alert_keys)
      AND snooze_type IN UNNEST(@alert_types)
  )
  SELECT COUNT(*) = 0
  FROM source S
  LEFT JOIN latest L
    ON L.alert_key = S.alert_key
   AND L.snooze_type = S.alert_type
   AND L.rn = 1
  WHERE COALESCE(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', L.updated_at), '') != S.expected_version
) AS 'Alert state changed before your action was saved. Refresh and try again.';

MERGE {table_fqn(project_id, dataset, "snoozes")} T
USING (
  SELECT
    refs[OFFSET(i)] AS our_ref,
    keys[OFFSET(i)] AS alert_key,
    types[OFFSET(i)] AS alert_type
  FROM (SELECT @our_refs AS refs, @alert_keys AS keys, @alert_types AS types),
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(refs) - 1)) AS i
) S
ON COALESCE(T.alert_key, T.our_ref) = S.alert_key AND T.snooze_type = S.alert_type
WHEN MATCHED THEN UPDATE SET
{set_clause}
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, alert_key, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
{values_clause}
  );

COMMIT TRANSACTION;
"""
    parsed_end_date = end_date or None
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("our_refs", "STRING", our_refs),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ArrayQueryParameter("expected_versions", "STRING", expected_versions),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("start_date", "DATE", today_nz().isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", parsed_end_date),
            bigquery.ScalarQueryParameter("user", "STRING", user),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except BadRequest as exc:
        if "Alert state changed" in str(exc):
            raise AlertConflictError("Alert state changed before your action was saved. Refresh and try again.") from exc
        raise
