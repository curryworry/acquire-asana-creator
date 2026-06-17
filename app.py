import re
import subprocess
import os
import json
import hmac
import hashlib
import time
import sys
import uuid
import tomllib
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo
from collections.abc import Mapping

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

from asana_client import AsanaClient, AsanaError
from gmail_client import GmailAttachment, GmailError, GmailInboxClient
from trafficking_logic import (
    REQUIRED_TRAFFICKING_COLUMNS,
    build_candidate_rows,
    build_subtask_blueprints,
    build_subtask_rows,
    clean_dataframe,
    existing_subtask_matches,
    find_existing_parent_task,
    parent_due_from_blueprints,
    read_table_bytes,
)


st.set_page_config(page_title="Trafficking to Asana", page_icon="✅", layout="wide")

GID_RE = re.compile(r"^\d+$")
ALERT_TYPE_NOT_LIVE = "NOT_LIVE"
ALERT_TYPE_MISSING_OUR_REF = "MISSING_OUR_REF"
MARGIN_SNOOZE_TYPE = "MARGIN_DASHBOARD"
DEFAULT_BQ_PROJECT_ID = "sm-test-391201"
DEFAULT_BQ_DATASET = "supermetrics_data"
MARGIN_VIEW_NAME = "margin_dashboard"
APP_PAGES = {
    "home": "Home",
    "live_margin_dashboard": "Live Margin Dashboard",
    "margin_analysis_dashboard": "Margin Analysis Dashboard",
    "alerts_dashboard": "Alerts Dashboard",
}


def _get_secret(name: str, default: str = "") -> str:
    try:
        return str(st.secrets.get(name, default))
    except Exception:
        return default


def _set_query_params(params: Dict[str, str]) -> None:
    try:
        st.query_params.clear()
        for key, value in params.items():
            if value:
                st.query_params[key] = value
    except Exception:
        pass


def _get_auth_users() -> Dict[str, Dict[str, str]]:
    auth_block: Any = {}
    try:
        auth_block = st.secrets.get("auth", {})
    except Exception:
        auth_block = {}

    if not auth_block:
        try:
            secrets_path = Path(__file__).resolve().parent / ".streamlit" / "secrets.toml"
            if secrets_path.exists():
                parsed = tomllib.loads(secrets_path.read_text(encoding="utf-8"))
                auth_block = parsed.get("auth", {})
        except Exception:
            auth_block = {}

    if not isinstance(auth_block, Mapping):
        return {}

    users = auth_block.get("users", {})
    if not isinstance(users, Mapping):
        return {}

    out: Dict[str, Dict[str, str]] = {}
    for username, raw_value in users.items():
        if not isinstance(raw_value, Mapping):
            continue
        password = str(raw_value.get("password", "") or "").strip()
        if not password:
            continue
        out[str(username).strip()] = {
            "display_name": str(raw_value.get("display_name", username) or username).strip(),
            "password": password,
        }
    return out


def _verify_password(password: str, expected_password: str) -> bool:
    return hmac.compare_digest(password, expected_password)


def _is_auth_enabled() -> bool:
    return bool(_get_auth_users())


def _is_authenticated() -> bool:
    return bool(st.session_state.get("auth_user"))


def _get_authenticated_user() -> Dict[str, str]:
    user = st.session_state.get("auth_user")
    return user if isinstance(user, dict) else {}


def _get_actor_name(fallback: str = "") -> str:
    auth_user = _get_authenticated_user()
    if auth_user:
        return str(auth_user.get("username") or auth_user.get("display_name") or "").strip()
    return fallback.strip()


def _render_login_gate() -> bool:
    if not _is_auth_enabled():
        st.info("Auth is not configured. Add `[auth.users]` entries in `.streamlit/secrets.toml` to enable login.")
        return True

    if _is_authenticated():
        return True

    st.title("Dashboard Login")
    st.caption("Sign in to access the dashboards.")
    with st.form("login_form"):
        username = st.text_input("Username").strip()
        password = st.text_input("Password", type="password")
        submitted = st.form_submit_button("Sign In", type="primary")

    if submitted:
        users = _get_auth_users()
        user = users.get(username)
        if not user or not _verify_password(password, user["password"]):
            st.error("Invalid username or password.")
        else:
            st.session_state["auth_user"] = {
                "username": username,
                "display_name": user["display_name"],
            }
            st.rerun()
    return False


def _render_navigation() -> None:
    auth_user = _get_authenticated_user()
    if auth_user:
        st.sidebar.caption(f"Signed in as {auth_user.get('display_name', auth_user.get('username', ''))}")
        if st.sidebar.button("Home", use_container_width=True):
            _set_query_params({"page": "home"})
            st.rerun()
        if st.sidebar.button("Live Margin Dashboard", use_container_width=True):
            _set_query_params({"page": "live_margin_dashboard"})
            st.rerun()
        if st.sidebar.button("Margin Analysis Dashboard", use_container_width=True):
            _set_query_params({"page": "margin_analysis_dashboard"})
            st.rerun()
        if st.sidebar.button("Alerts Dashboard", use_container_width=True):
            _set_query_params({"page": "alerts_dashboard"})
            st.rerun()
        if st.sidebar.button("Sign Out", use_container_width=True):
            st.session_state.pop("auth_user", None)
            _set_query_params({"page": "home"})
            st.rerun()


def _render_home_page() -> bool:
    page = _get_qparam("page")
    mode = _get_qparam("mode")
    if mode == "live_alerts" or page in {"margin_dashboard", "live_margin_dashboard", "margin_analysis_dashboard", "alerts_dashboard"}:
        return False

    st.title("Dashboards")
    auth_user = _get_authenticated_user()
    if auth_user:
        st.caption(f"Welcome, {auth_user.get('display_name', auth_user.get('username', ''))}.")

    c1, c2, c3 = st.columns(3)
    with c1:
        st.subheader("Live Margin Dashboard")
        st.caption("Current live margin, pacing, and snooze workflow at `OUR REF` level.")
        if st.button("Open Live Margin Dashboard", type="primary", key="open_live_margin_dashboard"):
            _set_query_params({"page": "live_margin_dashboard"})
            st.rerun()
    with c2:
        st.subheader("Margin Analysis Dashboard")
        st.caption("Selected-period margin analysis using overlap logic and period-only spend.")
        if st.button("Open Margin Analysis Dashboard", key="open_margin_analysis_dashboard"):
            _set_query_params({"page": "margin_analysis_dashboard"})
            st.rerun()
    with c3:
        st.subheader("Alerts Dashboard")
        st.caption("Open the active alerts and snoozed workflow view.")
        if st.button("Open Alerts Dashboard", key="open_alerts_dashboard"):
            _set_query_params({"page": "alerts_dashboard"})
            st.rerun()

    return True


def _split_csv_secret(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _as_int_secret(name: str, default: int) -> int:
    raw = _get_secret(name, str(default)).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _get_qparam(name: str) -> str:
    try:
        value = st.query_params.get(name, "")
    except Exception:
        return ""
    if isinstance(value, list):
        return str(value[0]).strip() if value else ""
    return str(value).strip()


def _sanitize_id(value: str, field_name: str) -> str:
    clean = value.strip()
    if not clean:
        raise ValueError(f"{field_name} is required.")
    allowed = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    if not set(clean).issubset(allowed):
        raise ValueError(f"{field_name} contains invalid characters: {clean}")
    return clean


def _today_nz() -> date:
    return datetime.now(timezone.utc).astimezone(ZoneInfo("Pacific/Auckland")).date()


def _month_start_nz() -> date:
    return _today_nz().replace(day=1)


def _run_query_with_details(
    client: bigquery.Client,
    query: str,
    job_config: bigquery.QueryJobConfig,
    context: str,
) -> None:
    try:
        job = client.query(query, job_config=job_config)
        job.result()
    except Exception as exc:
        details = getattr(exc, "errors", None)
        if not details and "job" in locals():
            details = getattr(job, "errors", None)
        raise RuntimeError(f"{context} failed: {exc}; details={details}") from exc


def _build_bq_client_from_secrets() -> tuple[bigquery.Client, str, str]:
    project_id = _sanitize_id(_get_secret("BQ_PROJECT_ID", DEFAULT_BQ_PROJECT_ID), "BQ_PROJECT_ID")
    dataset = _sanitize_id(_get_secret("BQ_DATASET", DEFAULT_BQ_DATASET), "BQ_DATASET")
    sa_json = _get_secret("BQ_SERVICE_ACCOUNT_JSON", "")

    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=project_id or info.get("project_id"), credentials=creds), project_id, dataset

    return bigquery.Client(project=project_id), project_id, dataset


def _margin_view_fqn(project_id: str, dataset: str) -> str:
    return f"`{project_id}.{dataset}.{MARGIN_VIEW_NAME}`"


def _margin_view_sql(project_id: str, dataset: str) -> str:
    sql_path = Path(__file__).resolve().parent / "sql" / "margin_dashboard_view.sql"
    template = sql_path.read_text(encoding="utf-8")
    source = f"{DEFAULT_BQ_PROJECT_ID}.{DEFAULT_BQ_DATASET}"
    target = f"{project_id}.{dataset}"
    return template.replace(source, target)


def _create_or_replace_margin_view(client: bigquery.Client, project_id: str, dataset: str) -> None:
    client.query(_margin_view_sql(project_id, dataset)).result()


def _fetch_margin_dashboard_df(client: bigquery.Client, project_id: str, dataset: str) -> pd.DataFrame:
    query = f"""
SELECT
  our_ref,
  job_number,
  campaign_name,
  advertiser_name,
  property_name,
  location_text,
  account_manager_name,
  trafficker_name,
  campaign_lead,
  booking_status,
  budget,
  booked_nett_cost,
  start_date,
  end_date,
  latest_delivery_date,
  as_of_date,
  total_days,
  elapsed_days,
  pacing_ratio,
  actual_nett_spend,
  total_impressions,
  total_clicks,
  first_delivery_date,
  last_delivery_date,
  expected_gross_spend_to_date,
  margin_amount,
  margin_pct,
  spend_vs_budget_ratio
FROM {_margin_view_fqn(project_id, dataset)}
ORDER BY margin_amount ASC, our_ref
"""
    rows = list(client.query(query).result())
    if not rows:
        return pd.DataFrame()

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "OUR_REF": str(row["our_ref"] or ""),
                "JOB_NUMBER": str(row["job_number"] or ""),
                "CAMPAIGN_NAME": str(row["campaign_name"] or ""),
                "ADVERTISER_NAME": str(row["advertiser_name"] or ""),
                "PROPERTY_NAME": str(row["property_name"] or ""),
                "LOCATION_TEXT": str(row["location_text"] or ""),
                "ACCOUNT_MANAGER": str(row["account_manager_name"] or ""),
                "TRAFFICKER_NAME": str(row["trafficker_name"] or ""),
                "CAMPAIGN_LEAD": str(row["campaign_lead"] or ""),
                "BOOKING_STATUS": str(row["booking_status"] or ""),
                "BUDGET": float(row["budget"] or 0),
                "BOOKED_NETT_COST": float(row["booked_nett_cost"] or 0),
                "START_DATE": str(row["start_date"] or ""),
                "END_DATE": str(row["end_date"] or ""),
                "LATEST_DELIVERY_DATE": str(row["latest_delivery_date"] or ""),
                "AS_OF_DATE": str(row["as_of_date"] or ""),
                "TOTAL_DAYS": int(row["total_days"] or 0),
                "ELAPSED_DAYS": int(row["elapsed_days"] or 0),
                "PACING_RATIO": float(row["pacing_ratio"] or 0),
                "ACTUAL_NETT_SPEND": float(row["actual_nett_spend"] or 0),
                "TOTAL_IMPRESSIONS": float(row["total_impressions"] or 0),
                "TOTAL_CLICKS": float(row["total_clicks"] or 0),
                "FIRST_DELIVERY_DATE": str(row["first_delivery_date"] or ""),
                "LAST_DELIVERY_DATE": str(row["last_delivery_date"] or ""),
                "EXPECTED_GROSS_SPEND_TO_DATE": float(row["expected_gross_spend_to_date"] or 0),
                "MARGIN_AMOUNT": float(row["margin_amount"] or 0),
                "MARGIN_PCT": (float(row["margin_pct"]) if row["margin_pct"] is not None else None),
                "SPEND_VS_BUDGET_RATIO": (float(row["spend_vs_budget_ratio"]) if row["spend_vs_budget_ratio"] is not None else None),
            }
        )
    return pd.DataFrame(out)


def _fetch_period_margin_dashboard_df(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    period_start: date,
    period_end: date,
) -> pd.DataFrame:
    query = f"""
WITH line_items AS (
  SELECT
    TRIM(CAST(OURREF AS STRING)) AS our_ref,
    CAST(JOBNUMBER AS STRING) AS job_number,
    MAX(CAMPAIGNNAME) AS campaign_name,
    MAX(ADVERTISERNAME) AS advertiser_name,
    MAX(PROPERTYNAME) AS property_name,
    MAX(LOCATIONTEXT) AS location_text,
    MAX(ACCOUNTMANAGERNAME) AS account_manager_name,
    MAX(TRAFFICKERNAME) AS trafficker_name,
    MAX(CAMPAIGNLEAD) AS campaign_lead,
    MAX(BOOKINGSTATUS) AS booking_status,
    MAX(ACTUALPRICE) AS budget,
    MAX(OURCOST) AS booked_nett_cost,
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
  FROM `{project_id}.{dataset}.master_overview`
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
  GROUP BY 1, 2
),
delivery AS (
  SELECT
    TRIM(CAST(OUR_REF AS STRING)) AS our_ref,
    SUM(COALESCE(COST, 0)) AS actual_spend_in_period,
    SUM(COALESCE(IMPRESSIONS, 0)) AS total_impressions_in_period,
    SUM(COALESCE(CLICKS, 0)) AS total_clicks_in_period,
    MIN(DATE) AS first_delivery_date_in_period,
    MAX(DATE) AS last_delivery_date_in_period
  FROM `{project_id}.{dataset}.BLEND_BLEND_5_1_2`
  WHERE OUR_REF IS NOT NULL
    AND TRIM(CAST(OUR_REF AS STRING)) != ''
    AND DATE BETWEEN @period_start AND @period_end
  GROUP BY 1
),
base AS (
  SELECT
    l.our_ref,
    l.job_number,
    l.campaign_name,
    l.advertiser_name,
    l.property_name,
    l.location_text,
    l.account_manager_name,
    l.trafficker_name,
    l.campaign_lead,
    l.booking_status,
    l.budget,
    l.booked_nett_cost,
    l.start_date,
    l.end_date,
    @period_start AS period_start,
    @period_end AS period_end,
    GREATEST(l.start_date, @period_start) AS effective_start,
    LEAST(l.end_date, @period_end) AS effective_end,
    DATE_DIFF(l.end_date, l.start_date, DAY) + 1 AS total_days,
    DATE_DIFF(LEAST(l.end_date, @period_end), GREATEST(l.start_date, @period_start), DAY) + 1 AS days_in_period,
    COALESCE(d.actual_spend_in_period, 0) AS actual_spend_in_period,
    COALESCE(d.total_impressions_in_period, 0) AS total_impressions_in_period,
    COALESCE(d.total_clicks_in_period, 0) AS total_clicks_in_period,
    d.first_delivery_date_in_period,
    d.last_delivery_date_in_period
  FROM line_items l
  LEFT JOIN delivery d
    ON d.our_ref = l.our_ref
  WHERE l.start_date IS NOT NULL
    AND l.end_date IS NOT NULL
    AND l.end_date >= l.start_date
    AND l.start_date <= @period_end
    AND l.end_date >= @period_start
)
SELECT
  our_ref,
  job_number,
  campaign_name,
  advertiser_name,
  property_name,
  location_text,
  account_manager_name,
  trafficker_name,
  campaign_lead,
  booking_status,
  budget,
  booked_nett_cost,
  start_date,
  end_date,
  period_start,
  period_end,
  effective_start,
  effective_end,
  total_days,
  days_in_period,
  actual_spend_in_period,
  total_impressions_in_period,
  total_clicks_in_period,
  first_delivery_date_in_period,
  last_delivery_date_in_period,
  SAFE_MULTIPLY(budget, SAFE_DIVIDE(days_in_period, total_days)) AS prorated_revenue_in_period,
  SAFE_MULTIPLY(budget, SAFE_DIVIDE(days_in_period, total_days)) - actual_spend_in_period AS margin_in_period,
  CASE
    WHEN SAFE_MULTIPLY(budget, SAFE_DIVIDE(days_in_period, total_days)) > 0
    THEN 1 - SAFE_DIVIDE(actual_spend_in_period, SAFE_MULTIPLY(budget, SAFE_DIVIDE(days_in_period, total_days)))
    ELSE NULL
  END AS margin_pct_in_period
FROM base
ORDER BY margin_in_period ASC, our_ref
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("period_start", "DATE", period_start.isoformat()),
            bigquery.ScalarQueryParameter("period_end", "DATE", period_end.isoformat()),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return pd.DataFrame()

    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "OUR_REF": str(row["our_ref"] or ""),
                "JOB_NUMBER": str(row["job_number"] or ""),
                "CAMPAIGN_NAME": str(row["campaign_name"] or ""),
                "ADVERTISER_NAME": str(row["advertiser_name"] or ""),
                "PROPERTY_NAME": str(row["property_name"] or ""),
                "LOCATION_TEXT": str(row["location_text"] or ""),
                "ACCOUNT_MANAGER": str(row["account_manager_name"] or ""),
                "TRAFFICKER_NAME": str(row["trafficker_name"] or ""),
                "CAMPAIGN_LEAD": str(row["campaign_lead"] or ""),
                "BOOKING_STATUS": str(row["booking_status"] or ""),
                "BUDGET": float(row["budget"] or 0),
                "BOOKED_NETT_COST": float(row["booked_nett_cost"] or 0),
                "START_DATE": str(row["start_date"] or ""),
                "END_DATE": str(row["end_date"] or ""),
                "PERIOD_START": str(row["period_start"] or ""),
                "PERIOD_END": str(row["period_end"] or ""),
                "EFFECTIVE_START": str(row["effective_start"] or ""),
                "EFFECTIVE_END": str(row["effective_end"] or ""),
                "TOTAL_DAYS": int(row["total_days"] or 0),
                "DAYS_IN_PERIOD": int(row["days_in_period"] or 0),
                "ACTUAL_SPEND_IN_PERIOD": float(row["actual_spend_in_period"] or 0),
                "TOTAL_IMPRESSIONS_IN_PERIOD": float(row["total_impressions_in_period"] or 0),
                "TOTAL_CLICKS_IN_PERIOD": float(row["total_clicks_in_period"] or 0),
                "FIRST_DELIVERY_DATE_IN_PERIOD": str(row["first_delivery_date_in_period"] or ""),
                "LAST_DELIVERY_DATE_IN_PERIOD": str(row["last_delivery_date_in_period"] or ""),
                "PRORATED_REVENUE_IN_PERIOD": float(row["prorated_revenue_in_period"] or 0),
                "MARGIN_IN_PERIOD": float(row["margin_in_period"] or 0),
                "MARGIN_PCT_IN_PERIOD": (
                    float(row["margin_pct_in_period"]) if row["margin_pct_in_period"] is not None else None
                ),
            }
        )
    return pd.DataFrame(out)


def _snoozes_table_fqn(project_id: str, dataset: str) -> str:
    return f"`{project_id}.{dataset}.snoozes`"


def _snapshots_table_fqn(project_id: str, dataset: str) -> str:
    return f"`{project_id}.{dataset}.live_alert_snapshots`"


def _ensure_control_tables(client: bigquery.Client, project_id: str, dataset: str) -> None:
    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {_snoozes_table_fqn(project_id, dataset)} (
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
CREATE TABLE IF NOT EXISTS {_snapshots_table_fqn(project_id, dataset)} (
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
    client.query(f"ALTER TABLE {_snoozes_table_fqn(project_id, dataset)} ADD COLUMN IF NOT EXISTS alert_key STRING").result()
    client.query(f"ALTER TABLE {_snapshots_table_fqn(project_id, dataset)} ADD COLUMN IF NOT EXISTS alert_key STRING").result()


def _verify_live_alert_link(user: str, run_id: str, exp: str, sig: str, secret: str) -> bool:
    try:
        exp_int = int(exp)
    except ValueError:
        return False
    if exp_int < int(time.time()):
        return False
    payload = f"{user}|{run_id}|{exp_int}"
    expected = hmac.new(secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig)


def _fetch_snapshot_df(client: bigquery.Client, project_id: str, dataset: str, run_id: str) -> pd.DataFrame:
    query = f"""
SELECT
  run_id,
  run_date_nz,
  run_timestamp_utc,
  alert_type,
  alert_key,
  our_ref,
  job_number,
  start_date,
  end_date,
  advertiser,
  campaign,
  location_text,
  property_name,
  booking_status,
  datasource,
  account,
  first_missing_date,
  last_missing_date,
  total_impressions,
  total_clicks,
  total_cost,
  row_count
FROM {_snapshots_table_fqn(project_id, dataset)}
WHERE run_id = @run_id
ORDER BY start_date, our_ref
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return pd.DataFrame()
    data = []
    for r in rows:
        data.append(
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
        )
    return pd.DataFrame(data)


def _fetch_latest_snapshot_global_df(client: bigquery.Client, project_id: str, dataset: str) -> pd.DataFrame:
    query = f"""
WITH latest_run AS (
  SELECT
    run_id,
    run_timestamp_utc
  FROM {_snapshots_table_fqn(project_id, dataset)}
  ORDER BY run_timestamp_utc DESC
  LIMIT 1
),
latest AS (
  SELECT
    s.run_id,
    s.run_date_nz,
    s.run_timestamp_utc,
    s.alert_type,
    s.alert_key,
    s.our_ref,
    s.job_number,
    s.start_date,
    s.end_date,
    s.advertiser,
    s.campaign,
    s.location_text,
    s.property_name,
    s.booking_status,
    s.datasource,
    s.account,
    s.first_missing_date,
    s.last_missing_date,
    s.total_impressions,
    s.total_clicks,
    s.total_cost,
    s.row_count
  FROM {_snapshots_table_fqn(project_id, dataset)} s
  INNER JOIN latest_run lr
    ON s.run_id = lr.run_id
)
SELECT
  run_id,
  run_date_nz,
  run_timestamp_utc,
  alert_type,
  alert_key,
  our_ref,
  job_number,
  start_date,
  end_date,
  advertiser,
  campaign,
  location_text,
  property_name,
  booking_status,
  datasource,
  account,
  first_missing_date,
  last_missing_date,
  total_impressions,
  total_clicks,
  total_cost,
  row_count
FROM latest
ORDER BY run_timestamp_utc DESC, alert_type, our_ref
"""
    rows = list(client.query(query).result())
    if not rows:
        return pd.DataFrame()
    data = []
    for r in rows:
        data.append(
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
        )
    return pd.DataFrame(data)


def _fetch_latest_snooze_df(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alert_keys: List[str],
    alert_types: List[str],
    legacy_not_live_refs: List[str],
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
  FROM {_snoozes_table_fqn(project_id, dataset)}
  WHERE snooze_type IN UNNEST(@alert_types)
    AND (
      COALESCE(alert_key, our_ref) IN UNNEST(@alert_keys)
      OR (snooze_type = @not_live_type AND our_ref IN UNNEST(@legacy_not_live_refs))
    )
)
SELECT
  alert_key,
  snooze_type,
  our_ref,
  snooze_status,
  snooze_reason,
  snooze_start_date,
  snooze_end_date,
  snoozed_by,
  dismissed_by,
  updated_at
FROM latest
WHERE rn = 1
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ScalarQueryParameter("not_live_type", "STRING", ALERT_TYPE_NOT_LIVE),
            bigquery.ArrayQueryParameter("legacy_not_live_refs", "STRING", legacy_not_live_refs),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return pd.DataFrame()
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


def _fetch_global_latest_snooze_df(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
) -> pd.DataFrame:
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
  FROM {_snoozes_table_fqn(project_id, dataset)}
),
snapshot_latest AS (
  SELECT
    alert_type,
    alert_key,
    advertiser,
    campaign,
    ROW_NUMBER() OVER (PARTITION BY alert_type, alert_key ORDER BY run_timestamp_utc DESC) AS rn
  FROM {_snapshots_table_fqn(project_id, dataset)}
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
  COALESCE(s.campaign, '') AS campaign
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
                "ADVERTISER": str(r["advertiser"] or ""),
                "CAMPAIGN": str(r["campaign"] or ""),
            }
            for r in rows
        ]
    )


def _upsert_snooze_active(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alert_type: str,
    alert_key: str,
    our_ref: str,
    user: str,
    reason: str,
    end_date: date | None,
    run_id: str,
) -> None:
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (SELECT @our_ref AS our_ref, @alert_type AS snooze_type) S
ON COALESCE(T.alert_key, T.our_ref) = @alert_key AND T.snooze_type = S.snooze_type
WHEN MATCHED THEN UPDATE SET
  alert_key = @alert_key,
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
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, alert_key, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    @our_ref, @alert_key, @alert_type, 'ACTIVE', @reason, @start_date, @end_date,
    @user, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, NULL, NULL
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref),
            bigquery.ScalarQueryParameter("alert_type", "STRING", alert_type),
            bigquery.ScalarQueryParameter("alert_key", "STRING", alert_key),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("start_date", "DATE", _today_nz().isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat() if end_date else None),
            bigquery.ScalarQueryParameter("user", "STRING", user),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _run_query_with_details(client, query, job_config, context="batch snooze active")


def _upsert_snooze_active_batch(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alerts: List[Dict[str, str]],
    user: str,
    reason: str,
    end_date: date | None,
    run_id: str,
) -> None:
    if not alerts:
        return
    our_refs = [str(a.get("our_ref", "") or "") for a in alerts]
    alert_keys = [str(a.get("alert_key", "") or "") for a in alerts]
    alert_types = [str(a.get("alert_type", "") or "") for a in alerts]
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (
  SELECT
    refs[OFFSET(i)] AS our_ref,
    keys[OFFSET(i)] AS alert_key,
    types[OFFSET(i)] AS alert_type
  FROM (
    SELECT @our_refs AS refs, @alert_keys AS keys, @alert_types AS types
  ),
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(refs) - 1)) AS i
) S
ON COALESCE(T.alert_key, T.our_ref) = S.alert_key AND T.snooze_type = S.alert_type
WHEN MATCHED THEN UPDATE SET
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
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, alert_key, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    S.our_ref, S.alert_key, S.alert_type, 'ACTIVE', @reason, @start_date, @end_date,
    @user, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, NULL, NULL
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("our_refs", "STRING", our_refs),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("start_date", "DATE", _today_nz().isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat() if end_date else None),
            bigquery.ScalarQueryParameter("user", "STRING", user),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    _run_query_with_details(client, query, job_config, context="batch unsnooze")


def _upsert_unsnooze(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alert_type: str,
    alert_key: str,
    our_ref: str,
    user: str,
    run_id: str,
) -> None:
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (SELECT @our_ref AS our_ref, @alert_type AS snooze_type) S
ON COALESCE(T.alert_key, T.our_ref) = @alert_key AND T.snooze_type = S.snooze_type
WHEN MATCHED THEN UPDATE SET
  alert_key = @alert_key,
  snooze_status = 'UNSNOOZED',
  snooze_reason = 'Manual unsnooze',
  run_id = @run_id,
  unsnoozed_by = @user,
  unsnoozed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, alert_key, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    @our_ref, @alert_key, @alert_type, 'UNSNOOZED', 'Manual unsnooze', NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @user, CURRENT_TIMESTAMP(), NULL, NULL
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref),
            bigquery.ScalarQueryParameter("alert_type", "STRING", alert_type),
            bigquery.ScalarQueryParameter("alert_key", "STRING", alert_key),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("user", "STRING", user),
        ]
    )
    _run_query_with_details(client, query, job_config, context="batch dismiss")


def _upsert_unsnooze_batch(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alerts: List[Dict[str, str]],
    user: str,
    run_id: str,
) -> None:
    if not alerts:
        return
    our_refs = [str(a.get("our_ref", "") or "") for a in alerts]
    alert_keys = [str(a.get("alert_key", "") or "") for a in alerts]
    alert_types = [str(a.get("alert_type", "") or "") for a in alerts]
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (
  SELECT
    refs[OFFSET(i)] AS our_ref,
    keys[OFFSET(i)] AS alert_key,
    types[OFFSET(i)] AS alert_type
  FROM (
    SELECT @our_refs AS refs, @alert_keys AS keys, @alert_types AS types
  ),
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(refs) - 1)) AS i
) S
ON COALESCE(T.alert_key, T.our_ref) = S.alert_key AND T.snooze_type = S.alert_type
WHEN MATCHED THEN UPDATE SET
  alert_key = S.alert_key,
  snooze_status = 'UNSNOOZED',
  snooze_reason = 'Manual unsnooze',
  run_id = @run_id,
  unsnoozed_by = @user,
  unsnoozed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, alert_key, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    S.our_ref, S.alert_key, S.alert_type, 'UNSNOOZED', 'Manual unsnooze', NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @user, CURRENT_TIMESTAMP(), NULL, NULL
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("our_refs", "STRING", our_refs),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("user", "STRING", user),
        ]
    )
    client.query(query, job_config=job_config).result()


def _upsert_dismiss(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alert_type: str,
    alert_key: str,
    our_ref: str,
    user: str,
    reason: str,
    run_id: str,
) -> None:
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (SELECT @our_ref AS our_ref, @alert_type AS snooze_type) S
ON COALESCE(T.alert_key, T.our_ref) = @alert_key AND T.snooze_type = S.snooze_type
WHEN MATCHED THEN UPDATE SET
  alert_key = @alert_key,
  snooze_status = 'DISMISSED',
  snooze_reason = @reason,
  run_id = @run_id,
  dismissed_by = @user,
  dismissed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, alert_key, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    @our_ref, @alert_key, @alert_type, 'DISMISSED', @reason, NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, @user, CURRENT_TIMESTAMP()
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref),
            bigquery.ScalarQueryParameter("alert_type", "STRING", alert_type),
            bigquery.ScalarQueryParameter("alert_key", "STRING", alert_key),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("user", "STRING", user),
        ]
    )
    client.query(query, job_config=job_config).result()


def _upsert_dismiss_batch(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    alerts: List[Dict[str, str]],
    user: str,
    reason: str,
    run_id: str,
) -> None:
    if not alerts:
        return
    our_refs = [str(a.get("our_ref", "") or "") for a in alerts]
    alert_keys = [str(a.get("alert_key", "") or "") for a in alerts]
    alert_types = [str(a.get("alert_type", "") or "") for a in alerts]
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (
  SELECT
    refs[OFFSET(i)] AS our_ref,
    keys[OFFSET(i)] AS alert_key,
    types[OFFSET(i)] AS alert_type
  FROM (
    SELECT @our_refs AS refs, @alert_keys AS keys, @alert_types AS types
  ),
  UNNEST(GENERATE_ARRAY(0, ARRAY_LENGTH(refs) - 1)) AS i
) S
ON COALESCE(T.alert_key, T.our_ref) = S.alert_key AND T.snooze_type = S.alert_type
WHEN MATCHED THEN UPDATE SET
  alert_key = S.alert_key,
  snooze_status = 'DISMISSED',
  snooze_reason = @reason,
  run_id = @run_id,
  dismissed_by = @user,
  dismissed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, alert_key, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    S.our_ref, S.alert_key, S.alert_type, 'DISMISSED', @reason, NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, @user, CURRENT_TIMESTAMP()
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("our_refs", "STRING", our_refs),
            bigquery.ArrayQueryParameter("alert_keys", "STRING", alert_keys),
            bigquery.ArrayQueryParameter("alert_types", "STRING", alert_types),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("user", "STRING", user),
        ]
    )
    client.query(query, job_config=job_config).result()


def _render_margin_dashboard() -> bool:
    mode = _get_qparam("mode")
    page = _get_qparam("page")
    if mode != "margin_dashboard" and page not in {"margin_dashboard", "live_margin_dashboard", "margin_analysis_dashboard"}:
        return False

    is_period_mode = page == "margin_analysis_dashboard"
    if is_period_mode:
        st.title("Margin Analysis Dashboard")
        st.caption("Selected-period margin analysis at `OUR REF` level using full budget, prorated revenue, and period-only spend.")
    else:
        st.title("Live Margin Dashboard")
        st.caption("Margin at `OUR REF` level using budget from `ACTUALPRICE` and actual nett spend from delivery `COST`.")

    try:
        bq_client, project_id, dataset = _build_bq_client_from_secrets()
        _ensure_control_tables(bq_client, project_id, dataset)
    except Exception as exc:
        st.error(f"Could not initialize BigQuery: {exc}")
        return True

    view_sql = _margin_view_sql(project_id, dataset)
    try:
        _create_or_replace_margin_view(bq_client, project_id, dataset)
    except Exception as exc:
        st.error(f"Could not create or update margin view: {exc}")
        return True

    st.caption(f"View: `{project_id}.{dataset}.{MARGIN_VIEW_NAME}`")
    margin_user = _get_actor_name()
    today = _today_nz()
    default_period_start = _month_start_nz()
    default_period_end = today
    period_start = default_period_start
    period_end = default_period_end
    if is_period_mode:
        period_col1, period_col2 = st.columns(2)
        with period_col1:
            period_start = st.date_input(
                "Period Start",
                value=default_period_start,
                key="margin_period_start",
            )
        with period_col2:
            period_end = st.date_input(
                "Period End",
                value=default_period_end,
                key="margin_period_end",
            )
        if period_start > period_end:
            st.error("Period start must be on or before period end.")
            return True

    with st.expander("Formula"):
        if not is_period_mode:
            st.markdown(
                """
- `budget = ACTUALPRICE`
- `actual_nett_spend = SUM(COST)`
- `expected_gross_spend_to_date = budget * elapsed_days / total_days`
- `margin_amount = expected_gross_spend_to_date - actual_nett_spend`
- `margin_pct = 1 - actual_nett_spend / expected_gross_spend_to_date`
- `as_of_date = LEAST(latest_delivery_date, end_date)`
"""
            )
        else:
            st.markdown(
                """
- `campaign is included if start_date <= period_end AND end_date >= period_start`
- `full campaign budget = ACTUALPRICE`
- `effective_start = GREATEST(start_date, period_start)`
- `effective_end = LEAST(end_date, period_end)`
- `days_in_period = effective_end - effective_start + 1`
- `prorated_revenue_in_period = budget * days_in_period / total_days`
- `actual_spend_in_period = SUM(COST where DATE between period_start and period_end)`
- `margin_in_period = prorated_revenue_in_period - actual_spend_in_period`
- `margin_pct_in_period = 1 - actual_spend_in_period / prorated_revenue_in_period`
"""
            )

    with st.expander("Query Source"):
        if not is_period_mode:
            st.code(view_sql, language="sql")
        else:
            st.caption("Selected Period uses a parameterized BigQuery query, not the saved live margin view.")

    try:
        if not is_period_mode:
            margin_df = _fetch_margin_dashboard_df(bq_client, project_id, dataset)
        else:
            margin_df = _fetch_period_margin_dashboard_df(
                bq_client,
                project_id,
                dataset,
                period_start=period_start,
                period_end=period_end,
            )
    except Exception as exc:
        st.error(f"Could not query margin data: {exc}")
        return True

    if margin_df.empty:
        st.warning("Margin query returned no rows.")
        return True

    margin_refs = sorted(margin_df["OUR_REF"].dropna().astype(str).unique().tolist())
    margin_snooze_df = _fetch_latest_snooze_df(
        bq_client,
        project_id,
        dataset,
        alert_keys=margin_refs,
        alert_types=[MARGIN_SNOOZE_TYPE],
        legacy_not_live_refs=[],
    )
    if not margin_snooze_df.empty:
        margin_df = margin_df.merge(
            margin_snooze_df[
                [
                    "ALERT_KEY",
                    "OUR_REF",
                    "SNOOZE_STATUS",
                    "SNOOZE_REASON",
                    "SNOOZE_START_DATE",
                    "SNOOZE_END_DATE",
                    "SNOOZED_BY",
                    "UPDATED_AT",
                ]
            ],
            on="OUR_REF",
            how="left",
        )
    else:
        margin_df["SNOOZE_STATUS"] = ""
        margin_df["SNOOZE_REASON"] = ""
        margin_df["SNOOZE_START_DATE"] = ""
        margin_df["SNOOZE_END_DATE"] = ""
        margin_df["SNOOZED_BY"] = ""
        margin_df["UPDATED_AT"] = ""

    margin_states = []
    for _, row in margin_df.iterrows():
        status = str(row.get("SNOOZE_STATUS", "") or "").upper()
        end_date_raw = str(row.get("SNOOZE_END_DATE", "") or "").strip()
        end_date = pd.to_datetime(end_date_raw, errors="coerce")
        if status == "ACTIVE" and (not end_date_raw or (not pd.isna(end_date) and end_date.date() >= today)):
            margin_states.append("ACTIVE")
        else:
            margin_states.append("OPEN")
    margin_df["MARGIN_SNOOZE_STATE"] = margin_states

    if not is_period_mode:
        latest_delivery_date = margin_df["LATEST_DELIVERY_DATE"].replace("", pd.NA).dropna()
        as_of_dates = margin_df["AS_OF_DATE"].replace("", pd.NA).dropna()
        st.caption(
            f"Latest delivery date: {latest_delivery_date.max() if not latest_delivery_date.empty else 'N/A'} | "
            f"As-of date max: {as_of_dates.max() if not as_of_dates.empty else 'N/A'}"
        )
    else:
        st.caption(f"Selected period: {period_start.isoformat()} to {period_end.isoformat()} (inclusive)")

    filter_col1, filter_col2, filter_col3 = st.columns(3)
    with filter_col1:
        advertiser_options = ["ALL"] + sorted(margin_df["ADVERTISER_NAME"].dropna().astype(str).unique().tolist())
        advertiser_filter = st.selectbox("Advertiser", advertiser_options, index=0)
    with filter_col2:
        booking_options = ["ALL"] + sorted(margin_df["BOOKING_STATUS"].dropna().astype(str).unique().tolist())
        booking_filter = st.selectbox("Booking Status", booking_options, index=0)
    with filter_col3:
        search_term = st.text_input("Search OUR REF / Job / Campaign", "").strip().lower()

    filtered_df = margin_df.copy()
    if advertiser_filter != "ALL":
        filtered_df = filtered_df[filtered_df["ADVERTISER_NAME"] == advertiser_filter]
    if booking_filter != "ALL":
        filtered_df = filtered_df[filtered_df["BOOKING_STATUS"] == booking_filter]
    if search_term:
        search_mask = (
            filtered_df["OUR_REF"].str.lower().str.contains(search_term, na=False)
            | filtered_df["JOB_NUMBER"].str.lower().str.contains(search_term, na=False)
            | filtered_df["CAMPAIGN_NAME"].str.lower().str.contains(search_term, na=False)
        )
        filtered_df = filtered_df[search_mask]

    with st.expander("Column Filters"):
        candidate_columns = filtered_df.columns.tolist()
        selected_filter_columns = st.multiselect(
            "Choose columns to filter",
            options=candidate_columns,
            default=[],
            key="margin_dashboard_filter_columns",
        )

        for col_name in selected_filter_columns:
            series = filtered_df[col_name]
            st.markdown(f"**{col_name}**")

            if pd.api.types.is_numeric_dtype(series):
                numeric_series = pd.to_numeric(series, errors="coerce").dropna()
                if numeric_series.empty:
                    st.caption("No numeric values available for filtering.")
                    continue
                min_value = float(numeric_series.min())
                max_value = float(numeric_series.max())
                range_value = st.slider(
                    f"{col_name} range",
                    min_value=min_value,
                    max_value=max_value,
                    value=(min_value, max_value),
                    key=f"margin_filter_range_{col_name}",
                )
                filtered_df = filtered_df[
                    pd.to_numeric(filtered_df[col_name], errors="coerce").between(range_value[0], range_value[1], inclusive="both")
                ]
                continue

            non_null_values = sorted(series.dropna().astype(str).unique().tolist())
            if not non_null_values:
                st.caption("No values available for filtering.")
                continue

            if len(non_null_values) <= 50:
                selected_values = st.multiselect(
                    f"{col_name} values",
                    options=non_null_values,
                    default=[],
                    key=f"margin_filter_values_{col_name}",
                )
                if selected_values:
                    filtered_df = filtered_df[filtered_df[col_name].astype(str).isin(selected_values)]
            else:
                contains_value = st.text_input(
                    f"{col_name} contains",
                    "",
                    key=f"margin_filter_contains_{col_name}",
                ).strip().lower()
                if contains_value:
                    filtered_df = filtered_df[
                        filtered_df[col_name].astype(str).str.lower().str.contains(contains_value, na=False)
                    ]

    active_df = filtered_df[filtered_df["MARGIN_SNOOZE_STATE"] == "OPEN"].copy()
    snoozed_df = filtered_df[filtered_df["MARGIN_SNOOZE_STATE"] == "ACTIVE"].copy()

    metric1, metric2, metric3, metric4 = st.columns(4)
    metric1.metric("Active Rows", f"{len(active_df):,}")
    metric2.metric("Snoozed Rows", f"{len(snoozed_df):,}")
    metric3.metric("Active Budget", f"${active_df['BUDGET'].sum():,.0f}")
    if not is_period_mode:
        metric4.metric("Active Actual Nett Spend", f"${active_df['ACTUAL_NETT_SPEND'].sum():,.0f}")
        metric5, metric6, metric7 = st.columns(3)
        metric5.metric("Active Margin Amount", f"${active_df['MARGIN_AMOUNT'].sum():,.0f}")
        margin_pct_series = active_df["MARGIN_PCT"].dropna()
        metric6.metric("Active Average Margin %", f"{(margin_pct_series.mean() * 100):.1f}%" if not margin_pct_series.empty else "N/A")
        pacing_series = active_df["PACING_RATIO"].dropna()
        metric7.metric("Active Average Pace %", f"{(pacing_series.mean() * 100):.1f}%" if not pacing_series.empty else "N/A")
    else:
        metric4.metric("Active Spend In Period", f"${active_df['ACTUAL_SPEND_IN_PERIOD'].sum():,.0f}")
        metric5, metric6, metric7 = st.columns(3)
        metric5.metric("Active Prorated Revenue In Period", f"${active_df['PRORATED_REVENUE_IN_PERIOD'].sum():,.0f}")
        metric6.metric("Active Margin In Period", f"${active_df['MARGIN_IN_PERIOD'].sum():,.0f}")
        margin_pct_series = active_df["MARGIN_PCT_IN_PERIOD"].dropna()
        metric7.metric(
            "Active Average Margin % In Period",
            f"{(margin_pct_series.mean() * 100):.1f}%" if not margin_pct_series.empty else "N/A",
        )

    def _prepare_margin_display(df: pd.DataFrame) -> pd.DataFrame:
        display_df = df.copy()
        for col in [
            "PACING_RATIO",
            "MARGIN_PCT",
            "SPEND_VS_BUDGET_RATIO",
            "MARGIN_PCT_IN_PERIOD",
        ]:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(lambda v: None if pd.isna(v) else round(float(v) * 100, 2))
        if "SNOOZE_STATUS" in display_df.columns and "SNOOZE_END_DATE" in display_df.columns:
            active_permanent_mask = (
                display_df["SNOOZE_STATUS"].astype(str).str.upper().eq("ACTIVE")
                & display_df["SNOOZE_END_DATE"].astype(str).str.strip().eq("")
            )
            display_df.loc[active_permanent_mask, "SNOOZE_END_DATE"] = "Permanent"
        return display_df

    table_column_config = {
        "PACING_RATIO": st.column_config.NumberColumn("PACING_RATIO (%)", format="%.2f"),
        "MARGIN_PCT": st.column_config.NumberColumn("MARGIN_PCT (%)", format="%.2f"),
        "SPEND_VS_BUDGET_RATIO": st.column_config.NumberColumn("SPEND_VS_BUDGET_RATIO (%)", format="%.2f"),
        "MARGIN_PCT_IN_PERIOD": st.column_config.NumberColumn("MARGIN_PCT_IN_PERIOD (%)", format="%.2f"),
        "BUDGET": st.column_config.NumberColumn("BUDGET", format="$%.2f"),
        "BOOKED_NETT_COST": st.column_config.NumberColumn("BOOKED_NETT_COST", format="$%.2f"),
        "ACTUAL_NETT_SPEND": st.column_config.NumberColumn("ACTUAL_NETT_SPEND", format="$%.2f"),
        "EXPECTED_GROSS_SPEND_TO_DATE": st.column_config.NumberColumn("EXPECTED_GROSS_SPEND_TO_DATE", format="$%.2f"),
        "MARGIN_AMOUNT": st.column_config.NumberColumn("MARGIN_AMOUNT", format="$%.2f"),
        "ACTUAL_SPEND_IN_PERIOD": st.column_config.NumberColumn("ACTUAL_SPEND_IN_PERIOD", format="$%.2f"),
        "PRORATED_REVENUE_IN_PERIOD": st.column_config.NumberColumn("PRORATED_REVENUE_IN_PERIOD", format="$%.2f"),
        "MARGIN_IN_PERIOD": st.column_config.NumberColumn("MARGIN_IN_PERIOD", format="$%.2f"),
    }

    tab_active, tab_snoozed = st.tabs(["Active Rows", "Snoozed"])

    with tab_active:
        if active_df.empty:
            st.info("No active rows for the current filters.")
        else:
            active_view = _prepare_margin_display(active_df)
            active_view.insert(0, "_ROW_ID", active_df.index.astype(str))
            active_view.insert(0, "SELECT", False)
            edited_active = st.data_editor(
                active_view,
                use_container_width=True,
                hide_index=True,
                column_config={"SELECT": st.column_config.CheckboxColumn("Select"), **table_column_config},
                disabled=[c for c in active_view.columns if c not in {"SELECT"}],
                column_order=["SELECT"] + [c for c in active_view.columns if c not in {"SELECT", "_ROW_ID"}],
                key="margin_active_rows_editor",
            )
            selected_ids = edited_active.loc[edited_active["SELECT"] == True, "_ROW_ID"].astype(str).tolist()
            selected_rows = active_df.loc[active_df.index.astype(str).isin(selected_ids), ["OUR_REF"]]
            selected_margin_rows = [
                {
                    "alert_type": MARGIN_SNOOZE_TYPE,
                    "alert_key": str(r["OUR_REF"] or ""),
                    "our_ref": str(r["OUR_REF"] or ""),
                }
                for _, r in selected_rows.iterrows()
            ]
            st.caption(f"Selected row count: {len(selected_margin_rows)}")

            snooze_reason = st.text_area("Snooze reason", key="margin_snooze_reason_text")
            permanent_snooze = st.checkbox("Permanent snooze", key="margin_permanent_snooze_checkbox")
            snooze_end_date = None
            if not permanent_snooze:
                snooze_end_date = st.date_input(
                    "Snooze end date (NZ)",
                    value=today,
                    min_value=today,
                    key="margin_snooze_end_date_input",
                )
            if st.button("Snooze Selected Rows", type="primary"):
                if not selected_margin_rows:
                    st.error("Select at least one row.")
                elif not snooze_reason.strip():
                    st.error("Snooze reason is required.")
                else:
                    _upsert_snooze_active_batch(
                        bq_client,
                        project_id,
                        dataset,
                        alerts=selected_margin_rows,
                        user=margin_user,
                        reason=snooze_reason.strip(),
                        end_date=snooze_end_date,
                        run_id=f"margin_dashboard:{uuid.uuid4().hex}",
                    )
                    st.success(f"Snoozed {len(selected_margin_rows)} row(s).")
                    st.rerun()

            st.download_button(
                "Download Active Margin CSV",
                data=_prepare_margin_display(active_df).to_csv(index=False).encode("utf-8"),
                file_name="margin_dashboard_active.csv",
                mime="text/csv",
            )

    with tab_snoozed:
        if snoozed_df.empty:
            st.info("No snoozed rows for the current filters.")
        else:
            snoozed_view = _prepare_margin_display(snoozed_df)
            snoozed_view.insert(0, "_ROW_ID", snoozed_df.index.astype(str))
            snoozed_view.insert(0, "SELECT", False)
            edited_snoozed = st.data_editor(
                snoozed_view,
                use_container_width=True,
                hide_index=True,
                column_config={"SELECT": st.column_config.CheckboxColumn("Select"), **table_column_config},
                disabled=[c for c in snoozed_view.columns if c not in {"SELECT"}],
                column_order=["SELECT"] + [c for c in snoozed_view.columns if c not in {"SELECT", "_ROW_ID"}],
                key="margin_snoozed_rows_editor",
            )
            selected_ids = edited_snoozed.loc[edited_snoozed["SELECT"] == True, "_ROW_ID"].astype(str).tolist()
            selected_rows = snoozed_df.loc[snoozed_df.index.astype(str).isin(selected_ids), ["OUR_REF"]]
            selected_snoozed_rows = [
                {
                    "alert_type": MARGIN_SNOOZE_TYPE,
                    "alert_key": str(r["OUR_REF"] or ""),
                    "our_ref": str(r["OUR_REF"] or ""),
                }
                for _, r in selected_rows.iterrows()
            ]
            st.caption(f"Selected row count: {len(selected_snoozed_rows)}")

            extend_permanent_snooze = st.checkbox("Set as permanent snooze", key="margin_extend_permanent_snooze_checkbox")
            extend_end_date = None
            if not extend_permanent_snooze:
                extend_end_date = st.date_input(
                    "New snooze end date (NZ)",
                    value=today,
                    min_value=today,
                    key="margin_extend_end_date_input",
                )
            extend_reason = st.text_input("Extend reason (optional)", key="margin_extend_reason_input")
            c1, c2 = st.columns(2)
            with c1:
                if st.button("Unsnooze Selected Rows"):
                    if not selected_snoozed_rows:
                        st.error("Select at least one row.")
                    else:
                        _upsert_unsnooze_batch(
                            bq_client,
                            project_id,
                            dataset,
                            alerts=selected_snoozed_rows,
                            user=margin_user,
                            run_id=f"margin_dashboard:{uuid.uuid4().hex}",
                        )
                        st.success(f"Unsnoozed {len(selected_snoozed_rows)} row(s).")
                        st.rerun()
            with c2:
                if st.button("Extend Snooze"):
                    if not selected_snoozed_rows:
                        st.error("Select at least one row.")
                    else:
                        reason = extend_reason.strip() or "Snooze extended"
                        _upsert_snooze_active_batch(
                            bq_client,
                            project_id,
                            dataset,
                            alerts=selected_snoozed_rows,
                            user=margin_user,
                            reason=reason,
                            end_date=extend_end_date,
                            run_id=f"margin_dashboard:{uuid.uuid4().hex}",
                        )
                        st.success(f"Extended snooze for {len(selected_snoozed_rows)} row(s).")
                        st.rerun()

            st.download_button(
                "Download Snoozed Margin CSV",
                data=_prepare_margin_display(snoozed_df).to_csv(index=False).encode("utf-8"),
                file_name="margin_dashboard_snoozed.csv",
                mime="text/csv",
            )
    return True

def _render_live_alert_dashboard() -> bool:
    mode = _get_qparam("mode")
    page = _get_qparam("page")
    if mode != "live_alerts" and page != "alerts_dashboard":
        return False

    user = _get_qparam("user")
    run_id = _get_qparam("run_id")
    exp = _get_qparam("exp")
    sig = _get_qparam("sig")

    st.title("Live Alerts Dashboard")
    acting_user = _get_actor_name()

    if mode == "live_alerts":
        if not user or not run_id or not exp or not sig:
            st.error("Invalid dashboard link. Missing required parameters.")
            return True

        link_signing_secret = _get_secret("LINK_SIGNING_SECRET", "")
        if not link_signing_secret:
            st.error("Missing LINK_SIGNING_SECRET in app secrets.")
            return True

        if not _verify_live_alert_link(user=user, run_id=run_id, exp=exp, sig=sig, secret=link_signing_secret):
            st.error("Link is invalid or expired. Please use the latest alert email link.")
            return True

        st.caption(f"Signed link user: {user} | Run ID: {run_id}")
        dashboard_mode = "signed_run"
    else:
        st.caption(f"Signed in as {acting_user}")
        dashboard_mode = "global"

    try:
        bq_client, project_id, dataset = _build_bq_client_from_secrets()
        _ensure_control_tables(bq_client, project_id, dataset)
    except Exception as exc:
        st.error(f"Could not initialize BigQuery: {exc}")
        return True

    if dashboard_mode == "signed_run":
        snapshot_df = _fetch_snapshot_df(bq_client, project_id, dataset, run_id=run_id)
    else:
        snapshot_df = _fetch_latest_snapshot_global_df(bq_client, project_id, dataset)
    if snapshot_df.empty:
        st.warning("No alert rows found.")
        return True

    if dashboard_mode == "global":
        latest_run_ts = snapshot_df["RUN_TS_UTC"].replace("", pd.NA).dropna()
        latest_run_label = latest_run_ts.max() if not latest_run_ts.empty else "N/A"
        st.caption(f"Latest alert snapshot timestamp (UTC): {latest_run_label}")

    alert_keys = sorted(snapshot_df["ALERT_KEY"].dropna().astype(str).unique().tolist())
    alert_types = sorted(snapshot_df["ALERT_TYPE"].dropna().astype(str).unique().tolist())
    legacy_not_live_refs = sorted(
        snapshot_df.loc[snapshot_df["ALERT_TYPE"] == ALERT_TYPE_NOT_LIVE, "OUR_REF"].dropna().astype(str).unique().tolist()
    )
    snooze_df = _fetch_latest_snooze_df(
        bq_client,
        project_id,
        dataset,
        alert_keys=alert_keys,
        alert_types=alert_types,
        legacy_not_live_refs=legacy_not_live_refs,
    )
    if not snooze_df.empty:
        merged = snapshot_df.merge(snooze_df, on=["ALERT_TYPE", "ALERT_KEY"], how="left")
    else:
        merged = snapshot_df.copy()
        merged["SNOOZE_STATUS"] = ""
        merged["SNOOZE_REASON"] = ""
        merged["SNOOZE_START_DATE"] = ""
        merged["SNOOZE_END_DATE"] = ""
        merged["SNOOZED_BY"] = ""
        merged["DISMISSED_BY"] = ""
        merged["UPDATED_AT"] = ""

    # Merge can suffix duplicate columns (e.g. OUR_REF_x/OUR_REF_y). Normalize for downstream UI logic.
    if "OUR_REF" not in merged.columns and "OUR_REF_x" in merged.columns:
        merged["OUR_REF"] = merged["OUR_REF_x"]

    today = _today_nz()
    statuses = []
    for _, row in merged.iterrows():
        status = str(row.get("SNOOZE_STATUS", "") or "").upper()
        end_date_raw = str(row.get("SNOOZE_END_DATE", "") or "").strip()
        end_date = pd.to_datetime(end_date_raw, errors="coerce")
        if status == "DISMISSED":
            statuses.append("DISMISSED")
        elif status == "ACTIVE" and (not end_date_raw or (not pd.isna(end_date) and end_date.date() >= today)):
            statuses.append("ACTIVE")
        else:
            statuses.append("OPEN")
    merged["LIVE_ALERT_STATE"] = statuses

    active_df = merged[merged["LIVE_ALERT_STATE"] == "OPEN"].copy()

    base_display_cols = [
        "ALERT_TYPE",
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

    tab_active, tab_snoozed = st.tabs(["Active Alerts", "Snoozed"])

    with tab_active:
        active_page = st.selectbox(
            "Alert page",
            options=["ALL"] + sorted(active_df["ALERT_TYPE"].dropna().astype(str).unique().tolist()),
            index=0,
            key="alert_page_select",
        )
        active_page_df = active_df if active_page == "ALL" else active_df[active_df["ALERT_TYPE"] == active_page].copy()

        if active_page_df.empty:
            st.info("No active alerts in this run.")
            edited_active = pd.DataFrame()
            selected_alerts: List[Dict[str, str]] = []
        else:
            active_view = active_page_df[[c for c in base_display_cols if c in active_page_df.columns]].copy()
            active_view.insert(0, "_ROW_ID", active_page_df.index.astype(str))
            active_view.insert(0, "SELECT", False)
            edited_active = st.data_editor(
                active_view,
                use_container_width=True,
                hide_index=True,
                column_config={"SELECT": st.column_config.CheckboxColumn("Select")},
                disabled=[c for c in active_view.columns if c not in {"SELECT"}],
                column_order=["SELECT"] + [c for c in active_view.columns if c not in {"SELECT", "_ROW_ID"}],
                key="active_alerts_editor",
            )
            selected_ids = edited_active.loc[edited_active["SELECT"] == True, "_ROW_ID"].astype(str).tolist()
            our_ref_col = "OUR_REF" if "OUR_REF" in active_page_df.columns else "OUR_REF_x"
            selected_rows = active_page_df.loc[
                active_page_df.index.astype(str).isin(selected_ids), ["ALERT_TYPE", "ALERT_KEY", our_ref_col]
            ].rename(columns={our_ref_col: "OUR_REF"})
            selected_alerts = [
                {
                    "alert_type": str(r["ALERT_TYPE"] or ""),
                    "alert_key": str(r["ALERT_KEY"] or ""),
                    "our_ref": str(r["OUR_REF"] or ""),
                }
                for _, r in selected_rows.iterrows()
            ]
            st.caption(f"Selected alert count: {len(selected_alerts)}")

        snooze_reason = st.text_area("Snooze reason", key="snooze_reason_text")
        permanent_snooze = st.checkbox("Permanent snooze", key="alerts_permanent_snooze_checkbox")
        snooze_end_date = None
        if not permanent_snooze:
            snooze_end_date = st.date_input(
                "Snooze end date (NZ)",
                value=today,
                min_value=today,
                key="snooze_end_date_input",
            )
        if st.button("Snooze Selected Alerts", type="primary"):
            if not selected_alerts:
                st.error("Select at least one alert.")
            elif not snooze_reason.strip():
                st.error("Snooze reason is required.")
            else:
                _upsert_snooze_active_batch(
                    bq_client,
                    project_id,
                    dataset,
                        alerts=selected_alerts,
                        user=acting_user,
                        reason=snooze_reason.strip(),
                        end_date=snooze_end_date,
                        run_id=run_id,
                )
                st.success(f"Snoozed {len(selected_alerts)} alert(s).")
                st.rerun()

    with tab_snoozed:
        global_snoozed_df = _fetch_global_latest_snooze_df(bq_client, project_id, dataset)
        if global_snoozed_df.empty:
            st.info("No global snoozed or dismissed alerts.")
            edited_snoozed = pd.DataFrame()
            selected_snoozed_alerts: List[Dict[str, str]] = []
        else:
            st.caption("Global snoozed/dismissed alerts (not limited to this run).")
            snoozed_view = global_snoozed_df.copy()
            active_permanent_mask = (
                snoozed_view["SNOOZE_STATUS"].astype(str).str.upper().eq("ACTIVE")
                & snoozed_view["SNOOZE_END_DATE"].astype(str).str.strip().eq("")
            )
            snoozed_view.loc[active_permanent_mask, "SNOOZE_END_DATE"] = "Permanent"
            snoozed_view.insert(0, "_ROW_ID", global_snoozed_df.index.astype(str))
            snoozed_view.insert(0, "SELECT", False)
            edited_snoozed = st.data_editor(
                snoozed_view,
                use_container_width=True,
                hide_index=True,
                column_config={"SELECT": st.column_config.CheckboxColumn("Select")},
                disabled=[c for c in snoozed_view.columns if c not in {"SELECT"}],
                column_order=["SELECT"] + [c for c in snoozed_view.columns if c not in {"SELECT", "_ROW_ID"}],
                key="snoozed_alerts_editor",
            )
            selected_ids = edited_snoozed.loc[edited_snoozed["SELECT"] == True, "_ROW_ID"].astype(str).tolist()
            selected_rows = global_snoozed_df.loc[
                global_snoozed_df.index.astype(str).isin(selected_ids), ["ALERT_TYPE", "ALERT_KEY", "OUR_REF"]
            ]
            selected_snoozed_alerts = [
                {
                    "alert_type": str(r["ALERT_TYPE"] or ""),
                    "alert_key": str(r["ALERT_KEY"] or ""),
                    "our_ref": str(r["OUR_REF"] or ""),
                }
                for _, r in selected_rows.iterrows()
            ]
            st.caption(f"Selected alert count: {len(selected_snoozed_alerts)}")

        extend_permanent_snooze = st.checkbox("Set as permanent snooze", key="alerts_extend_permanent_snooze_checkbox")
        extend_end_date = None
        if not extend_permanent_snooze:
            extend_end_date = st.date_input(
                "New snooze end date (NZ)",
                value=today,
                min_value=today,
                key="extend_end_date_input",
            )
        extend_reason = st.text_input("Extend reason (optional)", key="extend_reason_input")
        dismiss_reason = st.text_input("Dismiss reason", key="dismiss_reason_input")
        admin_pass_input = st.text_input("Admin password (required for dismiss)", type="password", key="admin_pass")
        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Unsnooze"):
                if not selected_snoozed_alerts:
                    st.error("Select at least one alert.")
                else:
                    _upsert_unsnooze_batch(
                        bq_client,
                        project_id,
                        dataset,
                        alerts=selected_snoozed_alerts,
                        user=acting_user,
                        run_id=run_id,
                    )
                    st.success(f"Unsnoozed {len(selected_snoozed_alerts)} alert(s).")
                    st.rerun()
        with c2:
            if st.button("Extend Snooze"):
                if not selected_snoozed_alerts:
                    st.error("Select at least one alert.")
                else:
                    reason = extend_reason.strip() or "Snooze extended"
                    _upsert_snooze_active_batch(
                        bq_client,
                        project_id,
                        dataset,
                        alerts=selected_snoozed_alerts,
                        user=acting_user,
                        reason=reason,
                        end_date=extend_end_date,
                        run_id=run_id,
                    )
                    st.success(f"Extended snooze for {len(selected_snoozed_alerts)} alert(s).")
                    st.rerun()
        with c3:
            if st.button("Dismiss"):
                admin_pass = _get_secret("ADMIN_PASS", "")
                if not admin_pass:
                    st.error("ADMIN_PASS is not configured.")
                elif admin_pass_input != admin_pass:
                    st.error("Admin password is incorrect.")
                elif not selected_snoozed_alerts:
                    st.error("Select at least one alert.")
                elif not dismiss_reason.strip():
                    st.error("Dismiss reason is required.")
                else:
                    _upsert_dismiss_batch(
                        bq_client,
                        project_id,
                        dataset,
                        alerts=selected_snoozed_alerts,
                        user=acting_user,
                        reason=dismiss_reason.strip(),
                        run_id=run_id,
                    )
                    st.success(f"Dismissed {len(selected_snoozed_alerts)} alert(s).")
                    st.rerun()

    return True


def _run_script(script_rel_path: str, env_overrides: Dict[str, str] | None = None) -> Tuple[int, str]:
    repo_root = Path(__file__).resolve().parent
    cmd = [sys.executable, script_rel_path]
    run_env = os.environ.copy()
    if env_overrides:
        run_env.update({k: v for k, v in env_overrides.items() if v is not None})
    result = subprocess.run(
        cmd,
        cwd=str(repo_root),
        env=run_env,
        capture_output=True,
        text=True,
    )
    output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
    return result.returncode, output.strip()


def _manual_automation_panel() -> None:
    st.subheader("Automation Control Panel")
    st.caption("Manual triggers for background automations. Useful for testing and one-off reruns.")

    bq_project_id = _get_secret("BQ_PROJECT_ID", "sm-test-391201")
    bq_dataset = _get_secret("BQ_DATASET", "supermetrics_data")
    bq_view = _get_secret("BQ_VIEW", "master_overview")
    bq_sa_json = _get_secret("BQ_SERVICE_ACCOUNT_JSON")
    alert_to = _get_secret("ALERT_EMAIL_TO", "")
    alert_subject = _get_secret("ALERT_EMAIL_SUBJECT")
    dashboard_base_url = _get_secret("ALERT_DASHBOARD_BASE_URL", "")
    link_signing_secret = _get_secret("LINK_SIGNING_SECRET", "")
    alert_link_ttl_days = _get_secret("ALERT_LINK_TTL_DAYS", "7")

    c1, c2 = st.columns(2)

    with c1:
        st.markdown("**Campaign Not-Live Alert**")
        st.caption("Runs BigQuery check and emails one digest if failures exist.")
        alert_to_input = st.text_input(
            "Recipients (comma separated)",
            value=alert_to,
            key="alert_email_to_input",
            placeholder="ashwin@acquirenz.com,zane@acquirenz.com",
        ).strip()
        force_alert = st.checkbox("Force run (ignore NZ 6AM weekday guard)", value=True, key="force_alert_run")
        if st.button("Run Campaign Alert Now", type="primary"):
            env_overrides = {
                "BQ_PROJECT_ID": bq_project_id,
                "BQ_DATASET": bq_dataset,
                "BQ_VIEW": bq_view,
                "BQ_SERVICE_ACCOUNT_JSON": bq_sa_json,
                "GMAIL_CLIENT_ID": _get_secret("GMAIL_CLIENT_ID"),
                "GMAIL_CLIENT_SECRET": _get_secret("GMAIL_CLIENT_SECRET"),
                "GMAIL_REFRESH_TOKEN": _get_secret("GMAIL_REFRESH_TOKEN"),
                "GMAIL_USER": _get_secret("GMAIL_USER", "me"),
                "ALERT_EMAIL_TO": alert_to_input,
                "ALERT_EMAIL_SUBJECT": alert_subject,
                "ALERT_FORCE_RUN": "true" if force_alert else "false",
                "ALERT_DASHBOARD_BASE_URL": dashboard_base_url,
                "LINK_SIGNING_SECRET": link_signing_secret,
                "ALERT_LINK_TTL_DAYS": alert_link_ttl_days,
            }
            with st.spinner("Running campaign alert..."):
                code, output = _run_script("scripts/campaign_not_live_alert.py", env_overrides=env_overrides)
            if code == 0:
                st.success("Campaign alert run completed.")
            else:
                st.error(f"Campaign alert run failed (exit code {code}).")
            if output:
                st.code(output, language="text")

    with c2:
        st.markdown("**Daily Trafficking Dry Run**")
        st.caption("Runs the same script as GitHub Actions for testing.")
        force_dry_run = st.checkbox("Force DRY_RUN_MODE=true", value=True, key="force_daily_dry_run")
        if st.button("Run Daily Trafficking Script Now"):
            env_overrides = {
                "ASANA_ACCESS_TOKEN": _get_secret("ASANA_ACCESS_TOKEN"),
                "ASANA_WORKSPACE_GID": _get_secret("ASANA_WORKSPACE_GID"),
                "ASANA_PROJECT_GID": _get_secret("ASANA_PROJECT_GID"),
                "ASANA_DEDUPE_PROJECT_GIDS": _get_secret("ASANA_DEDUPE_PROJECT_GIDS"),
                "GMAIL_CLIENT_ID": _get_secret("GMAIL_CLIENT_ID"),
                "GMAIL_CLIENT_SECRET": _get_secret("GMAIL_CLIENT_SECRET"),
                "GMAIL_REFRESH_TOKEN": _get_secret("GMAIL_REFRESH_TOKEN"),
                "GMAIL_USER": _get_secret("GMAIL_USER", "me"),
                "GMAIL_SUBJECT_CONTAINS": _get_secret("GMAIL_SUBJECT_CONTAINS"),
                "GMAIL_SEARCH_QUERY": _get_secret("GMAIL_SEARCH_QUERY"),
                "GMAIL_PROCESSED_LABEL": _get_secret("GMAIL_PROCESSED_LABEL"),
                "TRAFFICKING_SKIP_TOP_ROWS": _get_secret("TRAFFICKING_SKIP_TOP_ROWS", "0"),
                "REPORT_EMAIL_TO": _get_secret("REPORT_EMAIL_TO", "ashwin@acquirenz.com"),
                "DRY_RUN_MODE": "true" if force_dry_run else _get_secret("DRY_RUN_MODE", "true"),
                "DEFAULT_ASSIGNEE_GID": _get_secret("DEFAULT_ASSIGNEE_GID"),
                "DASH_ASSIGNEE_GID": _get_secret("DASH_ASSIGNEE_GID"),
            }
            with st.spinner("Running daily trafficking script..."):
                code, output = _run_script("scripts/daily_trafficking_dry_run.py", env_overrides=env_overrides)
            if code == 0:
                st.success("Daily trafficking script run completed.")
            else:
                st.error(f"Daily trafficking script failed (exit code {code}).")
            if output:
                st.code(output, language="text")

    st.divider()


class _InMemoryUpload:
    def __init__(self, name: str, content: bytes):
        self.name = name
        self._content = content

    def getvalue(self) -> bytes:
        return self._content


def _fetch_dedupe_project_tasks(client: AsanaClient, dedupe_project_gids: List[str]) -> List[Dict[str, str]]:
    tasks: List[Dict[str, str]] = []
    for project_gid in dedupe_project_gids:
        tasks.extend(client.list_project_tasks(project_gid))
    return tasks


def _build_existing_parent_index(
    project_tasks: List[Dict[str, str]], candidates: List[Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    existing_by_job: Dict[str, Dict[str, str]] = {}
    for row in candidates:
        existing = find_existing_parent_task(project_tasks, row["campaign_name"], row["job_number"])
        if existing:
            existing_by_job[row["job_number"]] = existing
    return existing_by_job


def main() -> None:
    auth_ready = _render_login_gate()
    if not auth_ready:
        return

    _render_navigation()

    if _render_home_page():
        return
    if _render_margin_dashboard():
        return
    if _render_live_alert_dashboard():
        return

    st.title("Trafficking to Asana")
    st.caption(
        "Uses Trafficking report only: one parent task per unique CampaignName+JobNumber, one source subtask per unique OurRef, and shared control subtasks from the daily automation."
    )
    _manual_automation_panel()

    access_token = _get_secret("ASANA_ACCESS_TOKEN")
    workspace_gid = _get_secret("ASANA_WORKSPACE_GID")
    target_project_gid = _get_secret("ASANA_PROJECT_GID")
    dedupe_project_gids = _split_csv_secret(_get_secret("ASANA_DEDUPE_PROJECT_GIDS"))
    gmail_client_id = _get_secret("GMAIL_CLIENT_ID")
    gmail_client_secret = _get_secret("GMAIL_CLIENT_SECRET")
    gmail_refresh_token = _get_secret("GMAIL_REFRESH_TOKEN")
    gmail_user = _get_secret("GMAIL_USER", "me")
    gmail_subject_contains = _get_secret("GMAIL_SUBJECT_CONTAINS", "Trafficking Report - acquirenz")
    gmail_search_query = _get_secret("GMAIL_SEARCH_QUERY", "")
    max_preview_rows = _as_int_secret("APP_MAX_PREVIEW_ROWS", 30)
    max_candidate_rows = _as_int_secret("APP_MAX_CANDIDATE_ROWS", 25000)
    if not dedupe_project_gids and target_project_gid.strip():
        dedupe_project_gids = [target_project_gid.strip()]

    missing_secrets = []
    if not access_token.strip():
        missing_secrets.append("ASANA_ACCESS_TOKEN")
    if not workspace_gid.strip():
        missing_secrets.append("ASANA_WORKSPACE_GID")
    if not target_project_gid.strip():
        missing_secrets.append("ASANA_PROJECT_GID")

    if missing_secrets:
        st.error(f"Missing required secrets: {', '.join(missing_secrets)}")
        return
    if not GID_RE.match(target_project_gid.strip()):
        st.error("ASANA_PROJECT_GID must contain only digits.")
        return
    invalid_dedupe_gids = [gid for gid in dedupe_project_gids if not GID_RE.match(gid)]
    if invalid_dedupe_gids:
        st.error("Invalid GID(s) in ASANA_DEDUPE_PROJECT_GIDS: " + ", ".join(invalid_dedupe_gids))
        return

    st.subheader("Upload Trafficking Report")
    missing_gmail = []
    if not gmail_client_id.strip():
        missing_gmail.append("GMAIL_CLIENT_ID")
    if not gmail_client_secret.strip():
        missing_gmail.append("GMAIL_CLIENT_SECRET")
    if not gmail_refresh_token.strip():
        missing_gmail.append("GMAIL_REFRESH_TOKEN")
    if missing_gmail:
        st.error("Missing required Gmail secrets: " + ", ".join(missing_gmail))
        return

    fetch_clicked = st.button("Fetch Latest Trafficking Report from Inbox", type="primary")
    if fetch_clicked:
        try:
            inbox_client = GmailInboxClient(
                client_id=gmail_client_id.strip(),
                client_secret=gmail_client_secret.strip(),
                refresh_token=gmail_refresh_token.strip(),
                user_id=gmail_user.strip() or "me",
            )
            attachment = inbox_client.fetch_latest_attachment(
                subject_contains=gmail_subject_contains.strip(),
                allowed_extensions=(".tsv", ".csv", ".xls", ".xlsx"),
                query=gmail_search_query.strip() or None,
            )
            st.session_state["inbox_attachment"] = attachment
            st.success(
                f"Loaded: {attachment.filename} from email '{attachment.subject}' (message {attachment.message_id})."
            )
        except (GmailError, ValueError) as exc:
            st.error(f"Gmail fetch failed: {exc}")
            return
        except Exception as exc:
            st.error(f"Unexpected Gmail error: {exc}")
            return

    attachment: GmailAttachment | None = st.session_state.get("inbox_attachment")
    if not attachment:
        st.info(
            f"Click fetch to pull latest inbox attachment with subject containing '{gmail_subject_contains}'."
        )
        return

    st.caption(
        f"Using inbox file: {attachment.filename} | Subject: {attachment.subject} | Received (UTC): {attachment.received_at}"
    )

    skip_top_rows = st.number_input(
        "Trafficking: skip top rows after inbox fetch",
        min_value=0,
        step=1,
        value=0,
        help="Rows to skip before the header row.",
    )

    try:
        upload_obj = _InMemoryUpload(attachment.filename, attachment.content)
        trafficking_df = clean_dataframe(
            read_table_bytes(upload_obj.name, upload_obj.getvalue(), int(skip_top_rows))
        )
    except Exception as exc:
        st.error(f"Could not read Trafficking Report file: {exc}")
        return

    if trafficking_df.empty:
        st.warning("Trafficking Report file has no rows.")
        return
    if len(trafficking_df.columns) <= 1:
        st.warning("Trafficking parse looks wrong (single column). Adjust skip rows.")
        st.dataframe(trafficking_df.head(20), use_container_width=True)
        return

    missing_cols = [c for c in REQUIRED_TRAFFICKING_COLUMNS if c not in trafficking_df.columns]
    if missing_cols:
        st.error("Trafficking Report is missing required columns: " + ", ".join(missing_cols))
        return

    st.subheader("Trafficking Report Preview")
    st.dataframe(trafficking_df.head(max_preview_rows), use_container_width=True)

    candidates, unmatched = build_candidate_rows(trafficking_df)
    blueprint_map = build_subtask_blueprints(trafficking_df)
    if len(candidates) > max_candidate_rows:
        st.error(
            f"Candidate task count ({len(candidates)}) exceeds APP_MAX_CANDIDATE_ROWS={max_candidate_rows}."
        )
        return

    st.subheader("Preflight Summary")
    c1, c2, c3 = st.columns(3)
    c1.metric("Unique Campaign + Job", len(candidates))
    c2.metric("Candidate Tasks", len(candidates))
    c3.metric("Unmatched Items", len(unmatched))

    if unmatched:
        st.write("Unmatched items")
        st.dataframe(pd.DataFrame(unmatched), use_container_width=True)

    if not candidates:
        st.warning("No candidate tasks found from current Trafficking report.")
        return

    st.write("Candidate tasks")
    st.dataframe(pd.DataFrame(candidates), use_container_width=True)
    st.caption(f"Dedupe projects checked: {', '.join(dedupe_project_gids)}")

    if not dedupe_project_gids:
        st.error("No dedupe projects configured. Add ASANA_DEDUPE_PROJECT_GIDS in secrets.")
        return

    check_clicked = st.button("Check Existing + Build Task + Subtask List", type="primary")
    if not check_clicked:
        st.caption(
            "On click: checks dedupe projects for existing job numbers, then shows parent due dates plus source and control subtasks that would be created."
        )
        return

    client = AsanaClient(access_token=access_token.strip())

    try:
        dedupe_project_tasks = _fetch_dedupe_project_tasks(client, dedupe_project_gids)
        existing_by_job = _build_existing_parent_index(dedupe_project_tasks, candidates)
    except AsanaError as exc:
        st.error(f"Failed while checking existing tasks: {exc}")
        return

    results: List[Dict[str, str]] = []
    progress = st.progress(0)
    total = max(len(candidates), 1)
    for idx, row in enumerate(candidates):
        existing_parent = existing_by_job.get(row["job_number"])
        results.append(
            {
                "task_name": row["task_name"],
                "campaign_name": row["campaign_name"],
                "job_number": row["job_number"],
                "parent_due_on": parent_due_from_blueprints(
                    blueprint_map.get((row["campaign_name"], row["job_number"]), [])
                ),
                "status": "existing_parent" if existing_parent else "would_create",
                "reason": (
                    "Found existing parent task in dedupe projects"
                    if existing_parent
                    else "No existing task found in dedupe projects"
                ),
                "parent_task_gid": existing_parent.get("gid", "") if existing_parent else "",
                "target_project_gid": target_project_gid.strip(),
            }
        )
        progress.progress((idx + 1) / total)

    st.subheader("Task Output (Dry Run)")
    result_df = pd.DataFrame(results)
    st.dataframe(result_df, use_container_width=True)
    st.download_button(
        "Download Parent Dry Run CSV",
        data=result_df.to_csv(index=False).encode("utf-8"),
        file_name="parent_task_dry_run.csv",
        mime="text/csv",
    )

    parent_status_by_job = {row["job_number"]: row["status"] for row in results}
    subtask_rows = build_subtask_rows(candidates, blueprint_map, parent_status_by_job)
    existing_subtasks_by_job: Dict[str, List[Dict[str, str]]] = {}
    for row in results:
        if row["status"] == "existing_parent" and row["parent_task_gid"]:
            try:
                existing_subtasks_by_job[row["job_number"]] = client.list_subtasks(row["parent_task_gid"])
            except AsanaError as exc:
                st.error(f"Failed while checking existing subtasks for job {row['job_number']}: {exc}")
                return

    for subtask_row in subtask_rows:
        if subtask_row["parent_status"] != "existing_parent":
            continue
        existing_subtasks = existing_subtasks_by_job.get(subtask_row["parent_job_number"], [])
        if existing_subtask_matches(subtask_row, existing_subtasks):
            subtask_row["subtask_status"] = "skip_subtask_exists"

    st.subheader("Subtask Output (Dry Run)")
    subtask_df = pd.DataFrame(subtask_rows)
    st.dataframe(subtask_df, use_container_width=True)
    st.download_button(
        "Download Subtask Dry Run CSV",
        data=subtask_df.to_csv(index=False).encode("utf-8"),
        file_name="subtask_dry_run.csv",
        mime="text/csv",
    )

    would_create_count = int((result_df["status"] == "would_create").sum()) if not result_df.empty else 0
    existing_parent_count = int((result_df["status"] == "existing_parent").sum()) if not result_df.empty else 0
    subtask_would_create = (
        int(sum(1 for row in subtask_rows if row["subtask_status"] == "would_create"))
        if subtask_rows
        else 0
    )
    subtask_skipped_existing = (
        int(sum(1 for row in subtask_rows if row["subtask_status"] == "skip_subtask_exists"))
        if subtask_rows
        else 0
    )
    st.success(
        f"Dry run complete. Parent would create: {would_create_count}, existing parents: {existing_parent_count}, subtask would create: {subtask_would_create}, subtask already exists: {subtask_skipped_existing}."
    )


if __name__ == "__main__":
    main()
