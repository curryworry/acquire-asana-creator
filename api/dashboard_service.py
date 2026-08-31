import json
import hashlib
import uuid
from datetime import date, datetime, timezone
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from google.api_core.exceptions import BadRequest
from google.cloud import bigquery
from google.oauth2 import service_account

from asana_client import AsanaClient
from api.config import REPO_ROOT, get_secret


DEFAULT_BQ_PROJECT_ID = "sm-test-391201"
DEFAULT_BQ_DATASET = "supermetrics_data"
MARGIN_VIEW_NAME = "margin_dashboard"
ALERT_TYPE_NOT_LIVE = "NOT_LIVE"
ALERT_TYPE_STOPPED_IMPRESSIONS = "STOPPED_IMPRESSIONS"
ALERT_TYPE_MISSING_OUR_REF = "MISSING_OUR_REF"
ALERT_TYPE_ENDED_BUT_IMPRESSIONS = "ENDED_BUT_IMPRESSIONS"
LIVE_ALERT_TYPES = (
    ALERT_TYPE_NOT_LIVE,
    ALERT_TYPE_STOPPED_IMPRESSIONS,
    ALERT_TYPE_MISSING_OUR_REF,
    ALERT_TYPE_ENDED_BUT_IMPRESSIONS,
)
MARGIN_SNOOZE_TYPE = "MARGIN_DASHBOARD"
PACING_TYPE_UNDER = "UNDERPACING"
PACING_SNOOZE_TYPE = "PACING_UNDERPACING"
ASANA_COMMENT_ALERT_TYPES = (
    PACING_SNOOZE_TYPE,
    ALERT_TYPE_NOT_LIVE,
    ALERT_TYPE_STOPPED_IMPRESSIONS,
)
ALERT_LABELS = {
    PACING_SNOOZE_TYPE: "Underpacing",
    ALERT_TYPE_NOT_LIVE: "Not live",
    ALERT_TYPE_STOPPED_IMPRESSIONS: "Stopped impressions",
    ALERT_TYPE_MISSING_OUR_REF: "Missing OUR_REF",
    ALERT_TYPE_ENDED_BUT_IMPRESSIONS: "Ended but impressions",
}


class AlertConflictError(Exception):
    pass


class AsanaCommentFailure(Exception):
    def __init__(self, message: str, resolution: str = "") -> None:
        super().__init__(message)
        self.resolution = resolution


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


def normalized_snooze_end_date(end_date: str | None) -> str | None:
    current_date = today_nz()
    parsed_end_date = end_date or None
    if not parsed_end_date:
        return None
    try:
        end_date_value = date.fromisoformat(parsed_end_date)
    except ValueError:
        return current_date.isoformat()
    if end_date_value < current_date:
        return current_date.isoformat()
    return parsed_end_date


def make_alert_key(alert_type: str, dims: list[str]) -> str:
    canonical = "|".join([alert_type] + [str(x or "").strip().lower() for x in dims])
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


def split_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def asana_project_gids() -> list[str]:
    project_gids: list[str] = []
    for gid in [get_secret("ASANA_PROJECT_GID", ""), *split_csv(get_secret("ASANA_DEDUPE_PROJECT_GIDS", ""))]:
        if gid and gid not in project_gids:
            project_gids.append(gid)
    return project_gids


def format_nz_datetime(value: datetime | None = None) -> str:
    dt = (value or datetime.now(timezone.utc)).astimezone(ZoneInfo("Pacific/Auckland"))
    hour = dt.hour % 12 or 12
    suffix = "AM" if dt.hour < 12 else "PM"
    return f"{dt.day}/{dt.month}/{dt:%y} {hour}:{dt:%M}{suffix}"


def asana_comment_text(alert_type: str, our_ref: str, reason: str, end_date: str | None, requested_by: str) -> str:
    snooze_until = end_date or "Permanent"
    return "\n".join(
        [
            f"Snoozed in Acquire Ops by {requested_by}",
            "",
            f"Snoozed at: {format_nz_datetime()}",
            f"Alert: {ALERT_LABELS.get(alert_type, alert_type)}",
            f"OUR_REF: {our_ref}",
            f"Snooze until: {snooze_until}",
            f"Reason: {reason}",
        ]
    )


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
    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {table_fqn(project_id, dataset, "snooze_actions")} (
  action_id STRING NOT NULL,
  batch_id STRING NOT NULL,
  action_status STRING NOT NULL,
  action_type STRING NOT NULL,
  snooze_type STRING NOT NULL,
  alert_key STRING NOT NULL,
  our_ref STRING,
  reason STRING,
  snooze_end_date DATE,
  requested_by STRING NOT NULL,
  requested_at TIMESTAMP NOT NULL,
  processor_id STRING,
  attempt_count INT64,
  processed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
)
"""
    ).result()
    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {table_fqn(project_id, dataset, "asana_comment_actions")} (
  action_id STRING NOT NULL,
  snooze_action_id STRING NOT NULL,
  action_status STRING NOT NULL,
  alert_type STRING NOT NULL,
  alert_key STRING NOT NULL,
  our_ref STRING,
  job_number STRING,
  reason STRING,
  snooze_end_date DATE,
  requested_by STRING NOT NULL,
  requested_at TIMESTAMP NOT NULL,
  comment_text STRING NOT NULL,
  target_resolution STRING,
  asana_parent_gid STRING,
  asana_target_gid STRING,
  asana_target_type STRING,
  asana_story_gid STRING,
  processor_id STRING,
  attempt_count INT64,
  processed_at TIMESTAMP,
  error_message STRING,
  updated_at TIMESTAMP NOT NULL
)
"""
    ).result()
    client.query(f"ALTER TABLE {table_fqn(project_id, dataset, 'snoozes')} ADD COLUMN IF NOT EXISTS alert_key STRING").result()
    client.query(f"ALTER TABLE {table_fqn(project_id, dataset, 'live_alert_snapshots')} ADD COLUMN IF NOT EXISTS alert_key STRING").result()
    client.query(f"ALTER TABLE {table_fqn(project_id, dataset, 'snooze_actions')} ADD COLUMN IF NOT EXISTS processor_id STRING").result()
    for column_sql in [
        "target_resolution STRING",
        "asana_parent_gid STRING",
        "asana_target_gid STRING",
        "asana_target_type STRING",
        "asana_story_gid STRING",
        "processor_id STRING",
    ]:
        client.query(
            f"ALTER TABLE {table_fqn(project_id, dataset, 'asana_comment_actions')} ADD COLUMN IF NOT EXISTS {column_sql}"
        ).result()


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
    canonical_df = pd.DataFrame(
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
    pending_df = latest_pending_snooze_actions(client, project_id, dataset, alert_keys, alert_types)
    if pending_df.empty:
        return canonical_df
    if canonical_df.empty:
        return pending_df
    pending_keys = set(zip(pending_df["ALERT_TYPE"], pending_df["ALERT_KEY"]))
    canonical_df = canonical_df[
        ~canonical_df.apply(lambda row: (row["ALERT_TYPE"], row["ALERT_KEY"]) in pending_keys, axis=1)
    ]
    return pd.concat([canonical_df, pending_df], ignore_index=True, sort=False)


def latest_pending_snooze_actions(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alert_keys: list[str],
    alert_types: list[str],
) -> pd.DataFrame:
    if not alert_keys or not alert_types:
        return pd.DataFrame()
    query = f"""
WITH latest AS (
  SELECT
    action_id,
    action_type,
    snooze_type,
    alert_key,
    our_ref,
    reason,
    snooze_end_date,
    requested_by,
    requested_at,
    ROW_NUMBER() OVER (PARTITION BY snooze_type, alert_key ORDER BY requested_at DESC, action_id DESC) AS rn
  FROM {table_fqn(project_id, dataset, "snooze_actions")}
  WHERE action_status IN ('PENDING', 'PROCESSING')
    AND snooze_type IN UNNEST(@alert_types)
    AND alert_key IN UNNEST(@alert_keys)
)
SELECT * FROM latest WHERE rn = 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    data: list[dict[str, Any]] = []
    for r in rows:
        action = str(r["action_type"] or "").lower()
        if action == "snooze":
            status = "ACTIVE"
            reason = str(r["reason"] or "")
            snoozed_by = str(r["requested_by"] or "")
            dismissed_by = ""
        elif action == "dismiss":
            status = "DISMISSED"
            reason = str(r["reason"] or "")
            snoozed_by = ""
            dismissed_by = str(r["requested_by"] or "")
        else:
            status = "UNSNOOZED"
            reason = "Manual unsnooze"
            snoozed_by = ""
            dismissed_by = ""
        data.append(
            {
                "ALERT_KEY": str(r["alert_key"] or ""),
                "ALERT_TYPE": str(r["snooze_type"] or ""),
                "OUR_REF": str(r["our_ref"] or ""),
                "SNOOZE_STATUS": status,
                "SNOOZE_REASON": reason,
                "SNOOZE_START_DATE": today_nz().isoformat() if status == "ACTIVE" else "",
                "SNOOZE_END_DATE": str(r["snooze_end_date"] or ""),
                "SNOOZED_BY": snoozed_by or "Saving...",
                "DISMISSED_BY": dismissed_by or ("Saving..." if status == "DISMISSED" else ""),
                "UPDATED_AT": "Queued...",
                "STATE_VERSION": "",
            }
        )
    return pd.DataFrame(data)


def global_pending_snooze_actions(client: bigquery.Client, project_id: str, dataset: str) -> pd.DataFrame:
    query = f"""
WITH latest AS (
  SELECT
    action_id,
    action_type,
    snooze_type,
    alert_key,
    our_ref,
    reason,
    snooze_end_date,
    requested_by,
    requested_at,
    ROW_NUMBER() OVER (PARTITION BY snooze_type, alert_key ORDER BY requested_at DESC, action_id DESC) AS rn
  FROM {table_fqn(project_id, dataset, "snooze_actions")}
  WHERE action_status IN ('PENDING', 'PROCESSING')
    AND snooze_type IN UNNEST(@alert_types)
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
  l.*,
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
"""
    rows = list(
        client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("alert_types", "STRING", list(LIVE_ALERT_TYPES))]
            ),
        ).result()
    )
    data: list[dict[str, Any]] = []
    for r in rows:
        action = str(r["action_type"] or "").lower()
        if action == "snooze":
            status = "ACTIVE"
            live_state = "ACTIVE"
            reason = str(r["reason"] or "")
            snoozed_by = str(r["requested_by"] or "")
            dismissed_by = ""
        elif action == "dismiss":
            status = "DISMISSED"
            live_state = "DISMISSED"
            reason = str(r["reason"] or "")
            snoozed_by = ""
            dismissed_by = str(r["requested_by"] or "")
        else:
            status = "UNSNOOZED"
            live_state = "OPEN"
            reason = "Manual unsnooze"
            snoozed_by = ""
            dismissed_by = ""
        data.append(
            {
                "RUN_ID": "",
                "RUN_DATE_NZ": "",
                "RUN_TS_UTC": str(r["requested_at"] or ""),
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
                "SNOOZE_STATUS": status,
                "SNOOZE_REASON": reason,
                "SNOOZE_START_DATE": today_nz().isoformat() if status == "ACTIVE" else "",
                "SNOOZE_END_DATE": str(r["snooze_end_date"] or ""),
                "SNOOZED_BY": snoozed_by or ("Saving..." if status == "ACTIVE" else ""),
                "DISMISSED_BY": dismissed_by or ("Saving..." if status == "DISMISSED" else ""),
                "UPDATED_AT": "Queued...",
                "LIVE_ALERT_STATE": live_state,
                "SOURCE_VIEW": "GLOBAL_SNOOZE",
            }
        )
    return pd.DataFrame(data)


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
  AND l.snooze_type IN UNNEST(@alert_types)
  AND (
    (
      UPPER(COALESCE(l.snooze_status, '')) = 'ACTIVE'
      AND (l.snooze_end_date IS NULL OR l.snooze_end_date >= CURRENT_DATE('Pacific/Auckland'))
    )
    OR UPPER(COALESCE(l.snooze_status, '')) = 'DISMISSED'
  )
ORDER BY l.updated_at DESC
"""
    rows = list(
        client.query(
            query,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ArrayQueryParameter("alert_types", "STRING", list(LIVE_ALERT_TYPES))]
            ),
        ).result()
    )
    canonical_df = pd.DataFrame(
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
    pending_df = global_pending_snooze_actions(client, project_id, dataset)
    if not pending_df.empty:
        pending_keys = set(zip(pending_df["ALERT_TYPE"], pending_df["ALERT_KEY"]))
        if not canonical_df.empty:
            canonical_df = canonical_df[
                ~canonical_df.apply(lambda row: (row["ALERT_TYPE"], row["ALERT_KEY"]) in pending_keys, axis=1)
            ]
        pending_visible_df = pending_df[pending_df["LIVE_ALERT_STATE"].isin(["ACTIVE", "DISMISSED"])].copy()
        canonical_df = pd.concat([canonical_df, pending_visible_df], ignore_index=True, sort=False)
    return canonical_df


def margin_dashboard() -> dict[str, Any]:
    client, project_id, dataset = bq_context()
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
  FROM {table_fqn(project_id, dataset, "master_overview")}
  WHERE DATE IS NOT NULL
),
line_items AS (
  SELECT
    TRIM(CAST(OURREF AS STRING)) AS our_ref,
    CAST(JOBNUMBER AS STRING) AS job_number,
    MAX(NULLIF(TRIM(CAST(T_GOAL_TYPE_REPORTING_V2 AS STRING)), '')) AS goal_type,
    STRING_AGG(DISTINCT NULLIF(TRIM(CAST(DATASOURCE AS STRING)), ''), ', ' ORDER BY NULLIF(TRIM(CAST(DATASOURCE AS STRING)), '')) AS datasources,
    MAX(NULLIF(TRIM(CAST(CAMPAIGNNAME AS STRING)), '')) AS campaign_name,
    MAX(NULLIF(TRIM(CAST(ADVERTISERNAME AS STRING)), '')) AS advertiser_name,
    MAX(NULLIF(TRIM(CAST(PROPERTYNAME AS STRING)), '')) AS property_name,
    MAX(NULLIF(TRIM(CAST(LOCATIONTEXT AS STRING)), '')) AS location_text,
    MAX(NULLIF(TRIM(CAST(ACCOUNTMANAGERNAME AS STRING)), '')) AS account_manager_name,
    MAX(NULLIF(TRIM(CAST(TRAFFICKERNAME AS STRING)), '')) AS trafficker_name,
    MAX(NULLIF(TRIM(CAST(CAMPAIGNLEAD AS STRING)), '')) AS campaign_lead,
    MAX(NULLIF(TRIM(CAST(BOOKINGSTATUS AS STRING)), '')) AS booking_status,
    MAX(COALESCE(SAFE_CAST(NUMUNITS AS FLOAT64), 0)) AS goal_delivery,
    MAX(COALESCE(SAFE_CAST(ACTUALPRICE AS FLOAT64), 0)) AS budget,
    MAX(COALESCE(SAFE_CAST(OURCOST AS FLOAT64), 0)) AS booked_nett_cost,
    SUM(
      CASE UPPER(TRIM(CAST(T_GOAL_TYPE_REPORTING_V2 AS STRING)))
        WHEN 'IMPRESSIONS' THEN COALESCE(IMPRESSIONS, 0)
        WHEN 'CLICKS' THEN COALESCE(LINK_CLICKS, 0)
        WHEN 'VIEWS' THEN COALESCE(VIDEO_COMPLETIONS, 0)
        ELSE 0
      END
    ) AS actual_delivery,
    SUM(COALESCE(COST, 0)) AS actual_cost,
    SUM(
      CASE WHEN DATE = ld.latest_delivery_date THEN
        CASE UPPER(TRIM(CAST(T_GOAL_TYPE_REPORTING_V2 AS STRING)))
          WHEN 'IMPRESSIONS' THEN COALESCE(IMPRESSIONS, 0)
          WHEN 'CLICKS' THEN COALESCE(LINK_CLICKS, 0)
          WHEN 'VIEWS' THEN COALESCE(VIDEO_COMPLETIONS, 0)
          ELSE 0
        END
      ELSE 0 END
    ) AS current_daily_delivery,
    SUM(CASE WHEN DATE = ld.latest_delivery_date THEN COALESCE(COST, 0) ELSE 0 END) AS current_daily_cost,
    SUM(COALESCE(IMPRESSIONS, 0)) AS total_impressions,
    SUM(COALESCE(LINK_CLICKS, 0)) AS total_link_clicks,
    SUM(COALESCE(VIDEO_COMPLETIONS, 0)) AS total_video_completions,
    MIN(DATE) AS first_delivery_date,
    MAX(DATE) AS last_delivery_date,
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
  CROSS JOIN latest_delivery ld
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
    AND T_GOAL_TYPE_REPORTING_V2 IS NOT NULL
  GROUP BY 1, 2
),
our_ref_rollup AS (
  SELECT
    our_ref,
    STRING_AGG(DISTINCT goal_type, ', ' ORDER BY goal_type) AS goal_types,
    STRING_AGG(DISTINCT datasources, ', ' ORDER BY datasources) AS datasources,
    STRING_AGG(DISTINCT NULLIF(TRIM(job_number), ''), ', ' ORDER BY NULLIF(TRIM(job_number), '')) AS job_numbers,
    STRING_AGG(DISTINCT campaign_name, ' | ' ORDER BY campaign_name) AS campaign_names,
    STRING_AGG(DISTINCT advertiser_name, ', ' ORDER BY advertiser_name) AS advertiser_names,
    STRING_AGG(DISTINCT property_name, ' | ' ORDER BY property_name) AS property_names,
    STRING_AGG(DISTINCT location_text, ' | ' ORDER BY location_text) AS location_texts,
    STRING_AGG(DISTINCT account_manager_name, ', ' ORDER BY account_manager_name) AS account_manager_names,
    STRING_AGG(DISTINCT trafficker_name, ', ' ORDER BY trafficker_name) AS trafficker_names,
    STRING_AGG(DISTINCT campaign_lead, ', ' ORDER BY campaign_lead) AS campaign_leads,
    STRING_AGG(DISTINCT booking_status, ', ' ORDER BY booking_status) AS booking_statuses,
    SUM(COALESCE(goal_delivery, 0)) AS goal_delivery,
    SUM(COALESCE(actual_delivery, 0)) AS actual_delivery,
    SUM(COALESCE(actual_cost, 0)) AS actual_cost,
    SUM(COALESCE(current_daily_delivery, 0)) AS current_daily_delivery,
    SUM(COALESCE(current_daily_cost, 0)) AS current_daily_cost,
    SUM(COALESCE(total_impressions, 0)) AS total_impressions,
    SUM(COALESCE(total_link_clicks, 0)) AS total_link_clicks,
    SUM(COALESCE(total_video_completions, 0)) AS total_video_completions,
    SUM(COALESCE(budget, 0)) AS budget,
    SUM(COALESCE(booked_nett_cost, 0)) AS booked_nett_cost,
    MIN(start_date) AS start_date,
    MAX(end_date) AS end_date,
    MIN(first_delivery_date) AS first_delivery_date,
    MAX(last_delivery_date) AS last_delivery_date
  FROM line_items
  GROUP BY 1
),
base AS (
  SELECT
    l.our_ref,
    l.goal_types,
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
    l.goal_delivery,
    l.actual_delivery,
    l.actual_cost,
    l.current_daily_delivery,
    l.current_daily_cost,
    l.total_impressions,
    l.total_link_clicks,
    l.total_video_completions,
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
    GREATEST(1, DATE_DIFF(l.end_date, LEAST(ld.latest_delivery_date, l.end_date), DAY)) AS remaining_days,
    l.first_delivery_date,
    l.last_delivery_date
  FROM our_ref_rollup l
  -- Roll up at OUR_REF level after deduping repeated daily rows at JOB_NUMBER level.
  CROSS JOIN latest_delivery ld
  WHERE l.start_date IS NOT NULL
    AND l.end_date IS NOT NULL
    AND l.end_date >= l.start_date
    AND l.start_date <= ld.latest_delivery_date
    AND l.end_date >= ld.latest_delivery_date
    AND NULLIF(TRIM(COALESCE(l.datasources, '')), '') IS NOT NULL
)
SELECT
  our_ref,
  goal_types,
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
  remaining_days,
  SAFE_DIVIDE(elapsed_days, total_days) AS pacing_ratio,
  goal_delivery,
  SAFE_MULTIPLY(goal_delivery, SAFE_DIVIDE(elapsed_days, total_days)) AS expected_delivery_to_date,
  actual_delivery,
  actual_cost,
  current_daily_delivery,
  current_daily_cost,
  CASE
    WHEN UPPER(goal_types) = 'IMPRESSIONS' THEN 'CPM'
    WHEN UPPER(goal_types) = 'CLICKS' THEN 'CPC'
    WHEN UPPER(goal_types) = 'VIEWS' THEN 'CPV'
    ELSE 'CPU'
  END AS cost_unit,
  SAFE_DIVIDE(GREATEST(goal_delivery - actual_delivery, 0), remaining_days) AS required_daily_delivery,
  actual_delivery + SAFE_MULTIPLY(current_daily_delivery, remaining_days) AS projected_end_delivery,
  CASE
    WHEN UPPER(goal_types) = 'IMPRESSIONS' THEN SAFE_MULTIPLY(SAFE_DIVIDE(GREATEST(booked_nett_cost - actual_cost, 0), NULLIF(GREATEST(goal_delivery - actual_delivery, 0), 0)), 1000)
    ELSE SAFE_DIVIDE(GREATEST(booked_nett_cost - actual_cost, 0), NULLIF(GREATEST(goal_delivery - actual_delivery, 0), 0))
  END AS required_cost_per_unit,
  CASE
    WHEN UPPER(goal_types) = 'IMPRESSIONS' THEN SAFE_MULTIPLY(SAFE_DIVIDE(current_daily_cost, NULLIF(current_daily_delivery, 0)), 1000)
    ELSE SAFE_DIVIDE(current_daily_cost, NULLIF(current_daily_delivery, 0))
  END AS current_daily_cost_per_unit,
  actual_delivery - SAFE_MULTIPLY(goal_delivery, SAFE_DIVIDE(elapsed_days, total_days)) AS delivery_delta,
  SAFE_DIVIDE(actual_delivery, SAFE_MULTIPLY(goal_delivery, SAFE_DIVIDE(elapsed_days, total_days))) AS delivery_pacing_ratio,
  total_impressions,
  total_link_clicks,
  total_video_completions,
  first_delivery_date,
  last_delivery_date
FROM base
WHERE SAFE_MULTIPLY(goal_delivery, SAFE_DIVIDE(elapsed_days, total_days)) > 0
  AND SAFE_DIVIDE(elapsed_days, total_days) < 1
  AND SAFE_DIVIDE(actual_delivery, SAFE_MULTIPLY(goal_delivery, SAFE_DIVIDE(elapsed_days, total_days))) <= 0.9
ORDER BY delivery_pacing_ratio ASC, delivery_delta ASC, our_ref
"""
        ).result()
    )

    data: list[dict[str, Any]] = []
    for r in rows:
        goal_delivery = float(r["goal_delivery"] or 0)
        expected_delivery_to_date = float(r["expected_delivery_to_date"] or 0)
        actual_delivery = float(r["actual_delivery"] or 0)
        time_progress_ratio = float(r["pacing_ratio"] or 0)
        delivery_pacing_ratio = float(r["delivery_pacing_ratio"]) if r["delivery_pacing_ratio"] is not None else None
        delivery_delta = float(r["delivery_delta"] or 0)

        data.append(
            {
                "OUR_REF": str(r["our_ref"] or ""),
                "ALERT_KEY": make_alert_key(PACING_SNOOZE_TYPE, [str(r["our_ref"] or "")]),
                "GOAL_TYPE": str(r["goal_types"] or ""),
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
                "REMAINING_DAYS": int(r["remaining_days"] or 0),
                "TIME_PROGRESS_RATIO": time_progress_ratio,
                "GOAL_DELIVERY": goal_delivery,
                "EXPECTED_DELIVERY_TO_DATE": expected_delivery_to_date,
                "ACTUAL_DELIVERY": actual_delivery,
                "ACTUAL_COST": float(r["actual_cost"] or 0),
                "CURRENT_DAILY_DELIVERY": float(r["current_daily_delivery"] or 0),
                "CURRENT_DAILY_COST": float(r["current_daily_cost"] or 0),
                "COST_UNIT": str(r["cost_unit"] or "CPU"),
                "REQUIRED_DAILY_DELIVERY": float(r["required_daily_delivery"] or 0),
                "PROJECTED_END_DELIVERY": float(r["projected_end_delivery"] or 0),
                "REQUIRED_COST_PER_UNIT": float(r["required_cost_per_unit"]) if r["required_cost_per_unit"] is not None else None,
                "CURRENT_DAILY_COST_PER_UNIT": float(r["current_daily_cost_per_unit"]) if r["current_daily_cost_per_unit"] is not None else None,
                "DELIVERY_DELTA": delivery_delta,
                "DELIVERY_PACING_RATIO": delivery_pacing_ratio,
                "PACING_STATUS": "UNDER",
                "PACING_BUCKET": PACING_TYPE_UNDER,
                "TOTAL_IMPRESSIONS": float(r["total_impressions"] or 0),
                "TOTAL_LINK_CLICKS": float(r["total_link_clicks"] or 0),
                "TOTAL_VIDEO_COMPLETIONS": float(r["total_video_completions"] or 0),
                "FIRST_DELIVERY_DATE": str(r["first_delivery_date"] or ""),
                "LAST_DELIVERY_DATE": str(r["last_delivery_date"] or ""),
            }
        )

    if data:
        df = pd.DataFrame(data)
        snooze_df = latest_snoozes(
            client,
            project_id,
            dataset,
            alert_keys=sorted(df["ALERT_KEY"].dropna().astype(str).unique().tolist()),
            alert_types=[PACING_SNOOZE_TYPE],
        )
        if not snooze_df.empty:
            df = df.merge(
                snooze_df[["ALERT_KEY", "SNOOZE_STATUS", "SNOOZE_REASON", "SNOOZE_START_DATE", "SNOOZE_END_DATE", "SNOOZED_BY", "UPDATED_AT"]],
                on="ALERT_KEY",
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
            states.append("SNOOZED" if status == "ACTIVE" and (not end_date_raw or (not pd.isna(end_date) and end_date.date() >= today)) else "OPEN")
        df["PACING_SNOOZE_STATE"] = states
        df["STATE_VERSION"] = df["UPDATED_AT"].fillna("").astype(str)
        data = records_from_rows(df.fillna("").to_dict(orient="records"))
    else:
        data = []

    open_count = sum(1 for row in data if row.get("PACING_SNOOZE_STATE") == "OPEN")
    snoozed_count = sum(1 for row in data if row.get("PACING_SNOOZE_STATE") == "SNOOZED")

    return {
        "rows": data,
        "meta": {
            "project_id": project_id,
            "dataset": dataset,
            "source_table": "master_overview",
            "count_underpacing": str(len(data)),
            "open_count": str(open_count),
            "snoozed_count": str(snoozed_count),
        },
    }


def alerts_dataset() -> tuple[pd.DataFrame, dict[str, str]]:
    client, project_id, dataset = bq_context()
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


def enqueue_snooze_actions(
    action: str,
    alerts: list[dict[str, str]],
    user: str,
    reason: str = "",
    end_date: str | None = None,
    run_id: str = "",
) -> list[str]:
    if not alerts:
        return []
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    batch_id = run_id or f"react_dashboard:{uuid.uuid4().hex}"
    action_ids = [uuid.uuid4().hex for _ in alerts]
    alert_keys = [str(a.get("alert_key", a.get("our_ref", "")) or "") for a in alerts]
    our_refs = [str(a.get("our_ref", "") or "") for a in alerts]
    alert_types = [str(a.get("alert_type", "") or "") for a in alerts]
    parsed_end_date = normalized_snooze_end_date(end_date)
    query = f"""
INSERT INTO {table_fqn(project_id, dataset, "snooze_actions")} (
  action_id, batch_id, action_status, action_type, snooze_type, alert_key, our_ref,
  reason, snooze_end_date, requested_by, requested_at, processor_id, attempt_count,
  processed_at, error_message, updated_at
)
SELECT
  action_ids[OFFSET(i)] AS action_id,
  @batch_id AS batch_id,
  'PENDING' AS action_status,
  @action AS action_type,
  types[OFFSET(i)] AS snooze_type,
  keys[OFFSET(i)] AS alert_key,
  refs[OFFSET(i)] AS our_ref,
  @reason AS reason,
  @end_date AS snooze_end_date,
  @user AS requested_by,
  CURRENT_TIMESTAMP() AS requested_at,
  NULL AS processor_id,
  0 AS attempt_count,
  NULL AS processed_at,
  NULL AS error_message,
  CURRENT_TIMESTAMP() AS updated_at
FROM (
  SELECT @action_ids AS action_ids, @alert_keys AS keys, @our_refs AS refs, @alert_types AS types
),
UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(action_ids) - 1)) AS i
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("action_ids", "STRING", action_ids),
            bigquery.ScalarQueryParameter("batch_id", "STRING", batch_id),
            bigquery.ScalarQueryParameter("action", "STRING", action),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ArrayQueryParameter("our_refs", "STRING", our_refs),
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("end_date", "DATE", parsed_end_date),
            bigquery.ScalarQueryParameter("user", "STRING", user),
        ]
    )
    client.query(query, job_config=job_config).result()
    return action_ids


def first_value(value: str) -> str:
    for part in str(value or "").replace("|", ",").split(","):
        clean = part.strip()
        if clean:
            return clean
    return ""


def resolve_asana_job_number(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alert_type: str,
    alert_key: str,
    our_ref: str,
) -> str:
    snapshot_query = f"""
SELECT job_number
FROM {table_fqn(project_id, dataset, "live_alert_snapshots")}
WHERE alert_type = @alert_type
  AND COALESCE(alert_key, our_ref) = @alert_key
  AND NULLIF(TRIM(COALESCE(job_number, '')), '') IS NOT NULL
ORDER BY run_timestamp_utc DESC
LIMIT 1
"""
    snapshot_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("alert_type", "STRING", alert_type),
            bigquery.ScalarQueryParameter("alert_key", "STRING", alert_key),
        ]
    )
    snapshot_rows = list(client.query(snapshot_query, job_config=snapshot_config).result())
    if snapshot_rows:
        job_number = first_value(str(snapshot_rows[0]["job_number"] or ""))
        if job_number:
            return job_number

    if not our_ref:
        return ""
    master_query = f"""
SELECT TRIM(CAST(JOBNUMBER AS STRING)) AS job_number
FROM {table_fqn(project_id, dataset, "master_overview")}
WHERE TRIM(CAST(OURREF AS STRING)) = @our_ref
  AND NULLIF(TRIM(CAST(JOBNUMBER AS STRING)), '') IS NOT NULL
GROUP BY job_number
ORDER BY job_number
LIMIT 1
"""
    master_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref)]
    )
    master_rows = list(client.query(master_query, job_config=master_config).result())
    return first_value(str(master_rows[0]["job_number"] or "")) if master_rows else ""


def enqueue_asana_comment_action(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    snooze_action: Any,
) -> None:
    if str(snooze_action["action_type"] or "").lower() != "snooze":
        return
    alert_type = str(snooze_action["snooze_type"] or "")
    if alert_type not in ASANA_COMMENT_ALERT_TYPES:
        return

    alert_key = str(snooze_action["alert_key"] or "")
    our_ref = str(snooze_action["our_ref"] or "")
    reason = str(snooze_action["reason"] or "")
    end_date = str(snooze_action["snooze_end_date"] or "") or None
    requested_by = str(snooze_action["requested_by"] or "")
    job_number = resolve_asana_job_number(client, project_id, dataset, alert_type, alert_key, our_ref)
    action_status = "PENDING" if job_number else "FAILED"
    error_message = "" if job_number else "Missing JOB_NUMBER for Asana comment."
    comment_text = asana_comment_text(alert_type, our_ref, reason, end_date, requested_by)
    query = f"""
MERGE {table_fqn(project_id, dataset, "asana_comment_actions")} T
USING (
  SELECT
    @action_id AS action_id,
    @snooze_action_id AS snooze_action_id,
    @action_status AS action_status,
    @alert_type AS alert_type,
    @alert_key AS alert_key,
    @our_ref AS our_ref,
    @job_number AS job_number,
    @reason AS reason,
    @snooze_end_date AS snooze_end_date,
    @requested_by AS requested_by,
    @requested_at AS requested_at,
    @comment_text AS comment_text,
    @error_message AS error_message
) S
ON T.snooze_action_id = S.snooze_action_id
WHEN NOT MATCHED THEN
  INSERT (
    action_id, snooze_action_id, action_status, alert_type, alert_key, our_ref, job_number,
    reason, snooze_end_date, requested_by, requested_at, comment_text, target_resolution,
    asana_parent_gid, asana_target_gid, asana_target_type, asana_story_gid, processor_id,
    attempt_count, processed_at, error_message, updated_at
  )
  VALUES (
    S.action_id, S.snooze_action_id, S.action_status, S.alert_type, S.alert_key, S.our_ref, S.job_number,
    S.reason, S.snooze_end_date, S.requested_by, S.requested_at, S.comment_text, NULL,
    NULL, NULL, NULL, NULL, NULL, 0, NULL, S.error_message, CURRENT_TIMESTAMP()
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("action_id", "STRING", uuid.uuid4().hex),
            bigquery.ScalarQueryParameter("snooze_action_id", "STRING", str(snooze_action["action_id"] or "")),
            bigquery.ScalarQueryParameter("action_status", "STRING", action_status),
            bigquery.ScalarQueryParameter("alert_type", "STRING", alert_type),
            bigquery.ScalarQueryParameter("alert_key", "STRING", alert_key),
            bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref),
            bigquery.ScalarQueryParameter("job_number", "STRING", job_number),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("snooze_end_date", "DATE", end_date),
            bigquery.ScalarQueryParameter("requested_by", "STRING", requested_by),
            bigquery.ScalarQueryParameter("requested_at", "TIMESTAMP", snooze_action["requested_at"]),
            bigquery.ScalarQueryParameter("comment_text", "STRING", comment_text),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
        ]
    )
    client.query(query, job_config=job_config).result()


def process_snooze_action_queue(max_actions: int = 50) -> dict[str, int]:
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    applied = 0
    failed = 0
    for _ in range(max_actions):
        processor_id = uuid.uuid4().hex
        try:
            action_row = claim_next_snooze_action(client, project_id, dataset, processor_id)
        except BadRequest:
            break
        if action_row is None:
            break
        action_id = str(action_row["action_id"])
        try:
            write_snooze_action(
                str(action_row["action_type"]),
                [
                    {
                        "our_ref": str(action_row["our_ref"] or ""),
                        "alert_key": str(action_row["alert_key"] or ""),
                        "alert_type": str(action_row["snooze_type"] or ""),
                    }
                ],
                str(action_row["requested_by"] or ""),
                str(action_row["reason"] or ""),
                str(action_row["snooze_end_date"] or "") or None,
                str(action_row["batch_id"] or ""),
                enforce_version=False,
            )
        except Exception as exc:  # noqa: BLE001 - persist queue failures for inspection.
            failed += 1
            mark_snooze_action(client, project_id, dataset, action_id, "FAILED", str(exc)[:1000])
        else:
            try:
                enqueue_asana_comment_action(client, project_id, dataset, action_row)
            except Exception:
                pass
            applied += 1
            mark_snooze_action(client, project_id, dataset, action_id, "APPLIED", "")
    return {"applied": applied, "failed": failed}


def claim_next_snooze_action(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    processor_id: str,
) -> Any | None:
    client.query(
        f"""
UPDATE {table_fqn(project_id, dataset, "snooze_actions")}
SET action_status = 'PENDING', processor_id = NULL, updated_at = CURRENT_TIMESTAMP()
WHERE action_status = 'PROCESSING'
  AND updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
"""
    ).result()
    claim_query = f"""
UPDATE {table_fqn(project_id, dataset, "snooze_actions")}
SET
  action_status = 'PROCESSING',
  processor_id = @processor_id,
  attempt_count = COALESCE(attempt_count, 0) + 1,
  updated_at = CURRENT_TIMESTAMP()
WHERE action_id IN (
  SELECT action_id
  FROM {table_fqn(project_id, dataset, "snooze_actions")}
  WHERE action_status = 'PENDING'
    AND NOT EXISTS (
      SELECT 1
      FROM {table_fqn(project_id, dataset, "snooze_actions")}
      WHERE action_status = 'PROCESSING'
    )
  ORDER BY requested_at, action_id
  LIMIT 1
)
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("processor_id", "STRING", processor_id)]
    )
    client.query(claim_query, job_config=job_config).result()
    rows = list(
        client.query(
            f"""
SELECT *
FROM {table_fqn(project_id, dataset, "snooze_actions")}
WHERE action_status = 'PROCESSING'
  AND processor_id = @processor_id
ORDER BY requested_at, action_id
LIMIT 1
""",
            job_config=job_config,
        ).result()
    )
    return rows[0] if rows else None


def mark_snooze_action(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    action_id: str,
    status: str,
    error_message: str,
) -> None:
    query = f"""
UPDATE {table_fqn(project_id, dataset, "snooze_actions")}
SET
  action_status = @status,
  processed_at = CURRENT_TIMESTAMP(),
  error_message = @error_message,
  updated_at = CURRENT_TIMESTAMP()
WHERE action_id = @action_id
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("action_id", "STRING", action_id),
        ]
    )
    client.query(query, job_config=job_config).result()


def process_asana_comment_action_queue(max_actions: int = 20) -> dict[str, int]:
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    applied = 0
    failed = 0
    for _ in range(max_actions):
        processor_id = uuid.uuid4().hex
        try:
            action_row = claim_next_asana_comment_action(client, project_id, dataset, processor_id)
        except BadRequest:
            break
        if action_row is None:
            break
        action_id = str(action_row["action_id"])
        try:
            result = post_asana_snooze_comment(action_row)
        except AsanaCommentFailure as exc:
            failed += 1
            mark_asana_comment_action(
                client,
                project_id,
                dataset,
                action_id,
                "FAILED",
                str(exc)[:1000],
                exc.resolution,
            )
        except Exception as exc:  # noqa: BLE001 - Asana side effects must not affect snoozes.
            failed += 1
            mark_asana_comment_action(
                client,
                project_id,
                dataset,
                action_id,
                "FAILED",
                str(exc)[:1000],
                "",
            )
        else:
            applied += 1
            mark_asana_comment_action(
                client,
                project_id,
                dataset,
                action_id,
                "APPLIED",
                "",
                result["target_resolution"],
                result["asana_parent_gid"],
                result["asana_target_gid"],
                result["asana_target_type"],
                result["asana_story_gid"],
            )
    return {"applied": applied, "failed": failed}


def claim_next_asana_comment_action(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    processor_id: str,
) -> Any | None:
    client.query(
        f"""
UPDATE {table_fqn(project_id, dataset, "asana_comment_actions")}
SET action_status = 'PENDING', processor_id = NULL, updated_at = CURRENT_TIMESTAMP()
WHERE action_status = 'PROCESSING'
  AND updated_at < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
"""
    ).result()
    claim_query = f"""
UPDATE {table_fqn(project_id, dataset, "asana_comment_actions")}
SET
  action_status = 'PROCESSING',
  processor_id = @processor_id,
  attempt_count = COALESCE(attempt_count, 0) + 1,
  updated_at = CURRENT_TIMESTAMP()
WHERE action_id IN (
  SELECT action_id
  FROM {table_fqn(project_id, dataset, "asana_comment_actions")}
  WHERE action_status = 'PENDING'
    AND NOT EXISTS (
      SELECT 1
      FROM {table_fqn(project_id, dataset, "asana_comment_actions")}
      WHERE action_status = 'PROCESSING'
    )
  ORDER BY requested_at, action_id
  LIMIT 1
)
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("processor_id", "STRING", processor_id)]
    )
    client.query(claim_query, job_config=job_config).result()
    rows = list(
        client.query(
            f"""
SELECT *
FROM {table_fqn(project_id, dataset, "asana_comment_actions")}
WHERE action_status = 'PROCESSING'
  AND processor_id = @processor_id
ORDER BY requested_at, action_id
LIMIT 1
""",
            job_config=job_config,
        ).result()
    )
    return rows[0] if rows else None


def mark_asana_comment_action(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    action_id: str,
    status: str,
    error_message: str,
    target_resolution: str,
    asana_parent_gid: str = "",
    asana_target_gid: str = "",
    asana_target_type: str = "",
    asana_story_gid: str = "",
) -> None:
    query = f"""
UPDATE {table_fqn(project_id, dataset, "asana_comment_actions")}
SET
  action_status = @status,
  target_resolution = NULLIF(@target_resolution, ''),
  asana_parent_gid = NULLIF(@asana_parent_gid, ''),
  asana_target_gid = NULLIF(@asana_target_gid, ''),
  asana_target_type = NULLIF(@asana_target_type, ''),
  asana_story_gid = NULLIF(@asana_story_gid, ''),
  processed_at = CURRENT_TIMESTAMP(),
  error_message = @error_message,
  updated_at = CURRENT_TIMESTAMP()
WHERE action_id = @action_id
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("target_resolution", "STRING", target_resolution),
            bigquery.ScalarQueryParameter("asana_parent_gid", "STRING", asana_parent_gid),
            bigquery.ScalarQueryParameter("asana_target_gid", "STRING", asana_target_gid),
            bigquery.ScalarQueryParameter("asana_target_type", "STRING", asana_target_type),
            bigquery.ScalarQueryParameter("asana_story_gid", "STRING", asana_story_gid),
            bigquery.ScalarQueryParameter("error_message", "STRING", error_message),
            bigquery.ScalarQueryParameter("action_id", "STRING", action_id),
        ]
    )
    client.query(query, job_config=job_config).result()


def find_asana_parent_task(asana: AsanaClient, job_number: str) -> dict[str, str] | None:
    for project_gid in asana_project_gids():
        for task in asana.list_project_tasks(project_gid):
            if job_number and job_number in str(task.get("name", "")):
                return task
    return None


def post_asana_snooze_comment(action_row: Any) -> dict[str, str]:
    token = get_secret("ASANA_ACCESS_TOKEN", "")
    if not token:
        raise AsanaCommentFailure("Missing ASANA_ACCESS_TOKEN.", "CONFIG_MISSING")
    if not asana_project_gids():
        raise AsanaCommentFailure("Missing Asana project GIDs.", "CONFIG_MISSING")

    job_number = str(action_row["job_number"] or "").strip()
    our_ref = str(action_row["our_ref"] or "").strip()
    if not job_number:
        raise AsanaCommentFailure("Missing JOB_NUMBER for Asana comment.", "JOB_NUMBER_MISSING")

    asana = AsanaClient(access_token=token)
    parent = find_asana_parent_task(asana, job_number)
    if not parent:
        raise AsanaCommentFailure(f"Asana parent task not found for JOB_NUMBER {job_number}.", "PARENT_NOT_FOUND")

    parent_gid = str(parent.get("gid", ""))
    if not parent_gid:
        raise AsanaCommentFailure("Matched Asana parent task has no gid.", "PARENT_GID_MISSING")

    target_gid = parent_gid
    target_type = "parent"
    resolution = "PARENT_NO_SUBTASK"
    if our_ref:
        subtasks = asana.list_subtasks(parent_gid)
        matches = [task for task in subtasks if our_ref.lower() in str(task.get("name", "")).lower()]
        if len(matches) == 1:
            target_gid = str(matches[0].get("gid", ""))
            target_type = "subtask"
            resolution = "SUBTASK_EXACT"
        elif len(matches) > 1:
            resolution = "PARENT_AMBIGUOUS_SUBTASKS"

    story = asana.create_task_comment(target_gid, str(action_row["comment_text"] or ""))
    return {
        "target_resolution": resolution,
        "asana_parent_gid": parent_gid,
        "asana_target_gid": target_gid,
        "asana_target_type": target_type,
        "asana_story_gid": str(story.get("gid", "")),
    }


def write_snooze_action(
    action: str,
    alerts: list[dict[str, str]],
    user: str,
    reason: str = "",
    end_date: str | None = None,
    run_id: str = "",
    enforce_version: bool = True,
) -> None:
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
  WHERE @enforce_version
    AND COALESCE(FORMAT_TIMESTAMP('%Y-%m-%d %H:%M:%E6S%Ez', L.updated_at), '') != S.expected_version
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
    current_date = today_nz()
    parsed_end_date = normalized_snooze_end_date(end_date)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("our_refs", "STRING", our_refs),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ArrayQueryParameter("expected_versions", "STRING", expected_versions),
            bigquery.ScalarQueryParameter("enforce_version", "BOOL", enforce_version),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("start_date", "DATE", current_date.isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", parsed_end_date),
            bigquery.ScalarQueryParameter("user", "STRING", user),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    try:
        client.query(query, job_config=job_config).result()
    except BadRequest as exc:
        error_text = str(exc)
        if "Alert state changed" in error_text:
            raise AlertConflictError("Alert state changed before your action was saved. Refresh and try again.") from exc
        if "concurrent update" in error_text.lower():
            raise AlertConflictError("Snooze state changed while your action was saving. Refresh and try again.") from exc
        raise
