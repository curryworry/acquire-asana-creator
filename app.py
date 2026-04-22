import re
import subprocess
import os
import json
import hmac
import hashlib
import time
import sys
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account

from asana_client import AsanaClient, AsanaError
from gmail_client import GmailAttachment, GmailError, GmailInboxClient


st.set_page_config(page_title="Trafficking to Asana", page_icon="✅", layout="wide")

GID_RE = re.compile(r"^\d+$")
ALERT_TYPE = "NOT_LIVE"


def _get_secret(name: str, default: str = "") -> str:
    try:
        return str(st.secrets.get(name, default))
    except Exception:
        return default


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


def _build_bq_client_from_secrets() -> tuple[bigquery.Client, str, str]:
    project_id = _sanitize_id(_get_secret("BQ_PROJECT_ID", "sm-test-391201"), "BQ_PROJECT_ID")
    dataset = _sanitize_id(_get_secret("BQ_DATASET", "supermetrics_data"), "BQ_DATASET")
    sa_json = _get_secret("BQ_SERVICE_ACCOUNT_JSON", "")

    if sa_json:
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=project_id or info.get("project_id"), credentials=creds), project_id, dataset

    return bigquery.Client(project=project_id), project_id, dataset


def _snoozes_table_fqn(project_id: str, dataset: str) -> str:
    return f"`{project_id}.{dataset}.snoozes`"


def _snapshots_table_fqn(project_id: str, dataset: str) -> str:
    return f"`{project_id}.{dataset}.live_alert_snapshots`"


def _ensure_control_tables(client: bigquery.Client, project_id: str, dataset: str) -> None:
    client.query(
        f"""
CREATE TABLE IF NOT EXISTS {_snoozes_table_fqn(project_id, dataset)} (
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
CREATE TABLE IF NOT EXISTS {_snapshots_table_fqn(project_id, dataset)} (
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
  our_ref,
  job_number,
  start_date,
  end_date,
  advertiser,
  campaign,
  location_text,
  property_name,
  booking_status
FROM {_snapshots_table_fqn(project_id, dataset)}
WHERE alert_type = @alert_type
  AND run_id = @run_id
ORDER BY start_date, our_ref
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("alert_type", "STRING", ALERT_TYPE),
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
                "OUR_REF": str(r["our_ref"] or ""),
                "JOB_NUMBER": str(r["job_number"] or ""),
                "START_DATE": str(r["start_date"] or ""),
                "END_DATE": str(r["end_date"] or ""),
                "ADVERTISER": str(r["advertiser"] or ""),
                "CAMPAIGN": str(r["campaign"] or ""),
                "LOCATIONTEXT": str(r["location_text"] or ""),
                "PROPERTYNAME": str(r["property_name"] or ""),
                "BOOKINGSTATUS": str(r["booking_status"] or ""),
            }
        )
    return pd.DataFrame(data)


def _fetch_latest_snooze_df(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    refs: List[str],
) -> pd.DataFrame:
    if not refs:
        return pd.DataFrame()
    query = f"""
WITH latest AS (
  SELECT
    our_ref,
    snooze_status,
    snooze_reason,
    snooze_start_date,
    snooze_end_date,
    snoozed_by,
    dismissed_by,
    updated_at,
    ROW_NUMBER() OVER (PARTITION BY our_ref, snooze_type ORDER BY updated_at DESC) AS rn
  FROM {_snoozes_table_fqn(project_id, dataset)}
  WHERE snooze_type = @alert_type
    AND our_ref IN UNNEST(@refs)
)
SELECT
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
            bigquery.ScalarQueryParameter("alert_type", "STRING", ALERT_TYPE),
            bigquery.ArrayQueryParameter("refs", "STRING", refs),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(
        [
            {
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


def _upsert_snooze_active(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    our_ref: str,
    user: str,
    reason: str,
    end_date: date,
    run_id: str,
) -> None:
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (SELECT @our_ref AS our_ref, @alert_type AS snooze_type) S
ON T.our_ref = S.our_ref AND T.snooze_type = S.snooze_type
WHEN MATCHED THEN UPDATE SET
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
    our_ref, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    @our_ref, @alert_type, 'ACTIVE', @reason, @start_date, @end_date,
    @user, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, NULL, NULL
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref),
            bigquery.ScalarQueryParameter("alert_type", "STRING", ALERT_TYPE),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("start_date", "DATE", _today_nz().isoformat()),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date.isoformat()),
            bigquery.ScalarQueryParameter("user", "STRING", user),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
        ]
    )
    client.query(query, job_config=job_config).result()


def _upsert_unsnooze(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    our_ref: str,
    user: str,
    run_id: str,
) -> None:
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (SELECT @our_ref AS our_ref, @alert_type AS snooze_type) S
ON T.our_ref = S.our_ref AND T.snooze_type = S.snooze_type
WHEN MATCHED THEN UPDATE SET
  snooze_status = 'UNSNOOZED',
  snooze_reason = 'Manual unsnooze',
  run_id = @run_id,
  unsnoozed_by = @user,
  unsnoozed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    @our_ref, @alert_type, 'UNSNOOZED', 'Manual unsnooze', NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), @user, CURRENT_TIMESTAMP(), NULL, NULL
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref),
            bigquery.ScalarQueryParameter("alert_type", "STRING", ALERT_TYPE),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("user", "STRING", user),
        ]
    )
    client.query(query, job_config=job_config).result()


def _upsert_dismiss(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    our_ref: str,
    user: str,
    reason: str,
    run_id: str,
) -> None:
    query = f"""
MERGE {_snoozes_table_fqn(project_id, dataset)} T
USING (SELECT @our_ref AS our_ref, @alert_type AS snooze_type) S
ON T.our_ref = S.our_ref AND T.snooze_type = S.snooze_type
WHEN MATCHED THEN UPDATE SET
  snooze_status = 'DISMISSED',
  snooze_reason = @reason,
  run_id = @run_id,
  dismissed_by = @user,
  dismissed_at = CURRENT_TIMESTAMP(),
  updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (
    our_ref, snooze_type, snooze_status, snooze_reason, snooze_start_date, snooze_end_date,
    snoozed_by, run_id, created_at, updated_at, unsnoozed_by, unsnoozed_at, dismissed_by, dismissed_at
  )
  VALUES (
    @our_ref, @alert_type, 'DISMISSED', @reason, NULL, NULL,
    NULL, @run_id, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), NULL, NULL, @user, CURRENT_TIMESTAMP()
  )
"""
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("our_ref", "STRING", our_ref),
            bigquery.ScalarQueryParameter("alert_type", "STRING", ALERT_TYPE),
            bigquery.ScalarQueryParameter("reason", "STRING", reason),
            bigquery.ScalarQueryParameter("run_id", "STRING", run_id),
            bigquery.ScalarQueryParameter("user", "STRING", user),
        ]
    )
    client.query(query, job_config=job_config).result()


def _render_live_alert_dashboard() -> bool:
    mode = _get_qparam("mode")
    if mode != "live_alerts":
        return False

    user = _get_qparam("user")
    run_id = _get_qparam("run_id")
    exp = _get_qparam("exp")
    sig = _get_qparam("sig")

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

    st.title("Live Alerts Dashboard")
    st.caption(f"User: {user} | Run ID: {run_id}")
    st.text_input("User", value=user, disabled=True)

    try:
        bq_client, project_id, dataset = _build_bq_client_from_secrets()
        _ensure_control_tables(bq_client, project_id, dataset)
    except Exception as exc:
        st.error(f"Could not initialize BigQuery: {exc}")
        return True

    snapshot_df = _fetch_snapshot_df(bq_client, project_id, dataset, run_id=run_id)
    if snapshot_df.empty:
        st.warning("No rows found for this run link.")
        return True

    snooze_df = _fetch_latest_snooze_df(
        bq_client,
        project_id,
        dataset,
        refs=sorted(snapshot_df["OUR_REF"].dropna().astype(str).unique().tolist()),
    )
    if not snooze_df.empty:
        merged = snapshot_df.merge(snooze_df, on="OUR_REF", how="left")
    else:
        merged = snapshot_df.copy()
        merged["SNOOZE_STATUS"] = ""
        merged["SNOOZE_REASON"] = ""
        merged["SNOOZE_START_DATE"] = ""
        merged["SNOOZE_END_DATE"] = ""
        merged["SNOOZED_BY"] = ""
        merged["DISMISSED_BY"] = ""
        merged["UPDATED_AT"] = ""

    today = _today_nz()
    statuses = []
    for _, row in merged.iterrows():
        status = str(row.get("SNOOZE_STATUS", "") or "").upper()
        end_date_raw = str(row.get("SNOOZE_END_DATE", "") or "").strip()
        end_date = pd.to_datetime(end_date_raw, errors="coerce")
        if status == "DISMISSED":
            statuses.append("DISMISSED")
        elif status == "ACTIVE" and not pd.isna(end_date) and end_date.date() >= today:
            statuses.append("ACTIVE")
        else:
            statuses.append("OPEN")
    merged["LIVE_ALERT_STATE"] = statuses

    active_df = merged[merged["LIVE_ALERT_STATE"] == "OPEN"].copy()
    snoozed_df = merged[merged["LIVE_ALERT_STATE"] != "OPEN"].copy()

    tab_active, tab_snoozed = st.tabs(["Active Alerts", "Snoozed"])

    with tab_active:
        if active_df.empty:
            st.info("No active alerts in this run.")
            edited_active = pd.DataFrame()
            selected_refs: List[str] = []
        else:
            active_view = active_df.copy()
            active_view.insert(0, "SELECT", False)
            edited_active = st.data_editor(
                active_view,
                use_container_width=True,
                hide_index=True,
                column_config={"SELECT": st.column_config.CheckboxColumn("Select")},
                disabled=[c for c in active_view.columns if c != "SELECT"],
                key="active_alerts_editor",
            )
            selected_refs = (
                edited_active.loc[edited_active["SELECT"] == True, "OUR_REF"].dropna().astype(str).unique().tolist()
            )
            st.caption(f"Selected OUR_REF count: {len(selected_refs)}")

        snooze_reason = st.text_area("Snooze reason", key="snooze_reason_text")
        snooze_end_date = st.date_input(
            "Snooze end date (NZ)",
            value=today,
            min_value=today,
            key="snooze_end_date_input",
        )
        if st.button("Snooze Selected OUR_REFs", type="primary"):
            if not selected_refs:
                st.error("Select at least one OUR_REF.")
            elif not snooze_reason.strip():
                st.error("Snooze reason is required.")
            else:
                for ref in selected_refs:
                    _upsert_snooze_active(
                        bq_client,
                        project_id,
                        dataset,
                        our_ref=ref,
                        user=user,
                        reason=snooze_reason.strip(),
                        end_date=snooze_end_date,
                        run_id=run_id,
                    )
                st.success(f"Snoozed {len(selected_refs)} OUR_REF(s).")
                st.rerun()

    with tab_snoozed:
        if snoozed_df.empty:
            st.info("No snoozed or dismissed alerts in this run.")
            edited_snoozed = pd.DataFrame()
            selected_snoozed_refs: List[str] = []
        else:
            snoozed_view = snoozed_df.copy()
            snoozed_view.insert(0, "SELECT", False)
            edited_snoozed = st.data_editor(
                snoozed_view,
                use_container_width=True,
                hide_index=True,
                column_config={"SELECT": st.column_config.CheckboxColumn("Select")},
                disabled=[c for c in snoozed_view.columns if c != "SELECT"],
                key="snoozed_alerts_editor",
            )
            selected_snoozed_refs = (
                edited_snoozed.loc[edited_snoozed["SELECT"] == True, "OUR_REF"]
                .dropna()
                .astype(str)
                .unique()
                .tolist()
            )
            st.caption(f"Selected OUR_REF count: {len(selected_snoozed_refs)}")

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
                if not selected_snoozed_refs:
                    st.error("Select at least one OUR_REF.")
                else:
                    for ref in selected_snoozed_refs:
                        _upsert_unsnooze(bq_client, project_id, dataset, our_ref=ref, user=user, run_id=run_id)
                    st.success(f"Unsnoozed {len(selected_snoozed_refs)} OUR_REF(s).")
                    st.rerun()
        with c2:
            if st.button("Extend Snooze"):
                if not selected_snoozed_refs:
                    st.error("Select at least one OUR_REF.")
                else:
                    reason = extend_reason.strip() or "Snooze extended"
                    for ref in selected_snoozed_refs:
                        _upsert_snooze_active(
                            bq_client,
                            project_id,
                            dataset,
                            our_ref=ref,
                            user=user,
                            reason=reason,
                            end_date=extend_end_date,
                            run_id=run_id,
                        )
                    st.success(f"Extended snooze for {len(selected_snoozed_refs)} OUR_REF(s).")
                    st.rerun()
        with c3:
            if st.button("Dismiss"):
                admin_pass = _get_secret("ADMIN_PASS", "")
                if not admin_pass:
                    st.error("ADMIN_PASS is not configured.")
                elif admin_pass_input != admin_pass:
                    st.error("Admin password is incorrect.")
                elif not selected_snoozed_refs:
                    st.error("Select at least one OUR_REF.")
                elif not dismiss_reason.strip():
                    st.error("Dismiss reason is required.")
                else:
                    for ref in selected_snoozed_refs:
                        _upsert_dismiss(
                            bq_client,
                            project_id,
                            dataset,
                            our_ref=ref,
                            user=user,
                            reason=dismiss_reason.strip(),
                            run_id=run_id,
                        )
                    st.success(f"Dismissed {len(selected_snoozed_refs)} OUR_REF(s).")
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


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    clean.columns = [str(c).strip() for c in clean.columns]
    unnamed_cols = [c for c in clean.columns if c.lower().startswith("unnamed:")]
    if unnamed_cols:
        clean = clean.drop(columns=unnamed_cols, errors="ignore")
    return clean.fillna("")


def _read_uploaded_table(uploaded_file: Any, skip_top_rows: int) -> pd.DataFrame:
    filename = str(getattr(uploaded_file, "name", "")).lower()
    raw = uploaded_file.getvalue()

    if filename.endswith(".tsv"):
        for enc in ("utf-8", "cp1252", "latin-1"):
            try:
                return pd.read_csv(
                    BytesIO(raw), sep="\t", skiprows=skip_top_rows, engine="python", encoding=enc
                )
            except Exception:
                continue
        raise ValueError("Could not parse TSV file with supported encodings.")

    if filename.endswith(".csv"):
        return pd.read_csv(BytesIO(raw), skiprows=skip_top_rows)

    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        try:
            return pd.read_excel(BytesIO(raw), skiprows=skip_top_rows)
        except Exception:
            for enc in ("utf-8", "cp1252", "latin-1"):
                try:
                    return pd.read_csv(
                        BytesIO(raw),
                        sep="\t",
                        skiprows=skip_top_rows,
                        engine="python",
                        encoding=enc,
                    )
                except Exception:
                    continue
            raise

    raise ValueError("Unsupported file type. Upload .tsv, .csv, .xls, or .xlsx.")


class _InMemoryUpload:
    def __init__(self, name: str, content: bytes):
        self.name = name
        self._content = content

    def getvalue(self) -> bytes:
        return self._content


def _normalize_campaign_name(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip()
    text = re.sub(r",\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_job_number(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        text = text[:-2]
    return text


def _as_due_on(date_value: Any) -> str:
    raw = str(date_value).strip()
    if re.match(r"^\\d{4}-\\d{2}-\\d{2}$", raw):
        parsed = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    else:
        parsed = pd.to_datetime(raw, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _build_candidate_rows(trafficking_df: pd.DataFrame) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    candidates: List[Dict[str, str]] = []
    unmatched: List[Dict[str, str]] = []

    seen_keys = set()
    missing_campaign = 0
    missing_job = 0

    for _, row in trafficking_df.iterrows():
        campaign_name = _normalize_campaign_name(row.get("CampaignName", ""))
        job_number = _normalize_job_number(row.get("JobNumber", ""))

        if not campaign_name:
            missing_campaign += 1
            continue
        if not job_number:
            missing_job += 1
            continue

        key = (campaign_name, job_number)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        candidates.append(
            {
                "campaign_name": campaign_name,
                "job_number": job_number,
                "task_name": f"{campaign_name} ({job_number})",
            }
        )

    if missing_campaign:
        unmatched.append(
            {
                "item": "Trafficking rows",
                "reason": f"{missing_campaign} rows missing CampaignName",
            }
        )
    if missing_job:
        unmatched.append(
            {
                "item": "Trafficking rows",
                "reason": f"{missing_job} rows missing JobNumber",
            }
        )

    return sorted(candidates, key=lambda x: (x["campaign_name"], x["job_number"])), unmatched


def _build_subtask_rows(
    trafficking_df: pd.DataFrame,
    candidates: List[Dict[str, str]],
    parent_status_by_job: Dict[str, str],
) -> List[Dict[str, str]]:
    by_campaign_job: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    seen_refs = set()

    for _, row in trafficking_df.iterrows():
        campaign_name = _normalize_campaign_name(row.get("CampaignName", ""))
        job_number = _normalize_job_number(row.get("JobNumber", ""))
        if not campaign_name or not job_number:
            continue

        our_ref = str(row.get("OurRef", "")).strip()
        if not our_ref:
            continue

        dedupe_key = (campaign_name, job_number, our_ref)
        if dedupe_key in seen_refs:
            continue
        seen_refs.add(dedupe_key)

        property_value = str(row.get("PropertyName", "")).strip()
        location_value = str(row.get("LocationText", "")).strip()
        ad_unit_value = str(row.get("SpecificationText", "")).strip()
        start_date_raw = str(row.get("StartDate", "")).strip()

        subtask_name = f"({our_ref}) {property_value} - {location_value}: {ad_unit_value}".strip()
        by_campaign_job.setdefault((campaign_name, job_number), []).append(
            {
                "our_ref": our_ref,
                "subtask_name": subtask_name,
                "start_date_raw": start_date_raw,
                "subtask_due_on": _as_due_on(start_date_raw),
            }
        )

    out: List[Dict[str, str]] = []
    for parent in candidates:
        parent_status = parent_status_by_job.get(parent["job_number"], "would_create")
        key = (parent["campaign_name"], parent["job_number"])
        for sub in by_campaign_job.get(key, []):
            out.append(
                {
                    "parent_task_name": parent["task_name"],
                    "parent_job_number": parent["job_number"],
                    "parent_status": parent_status,
                    "our_ref": sub["our_ref"],
                    "subtask_name": sub["subtask_name"],
                    "subtask_due_on": sub["subtask_due_on"],
                    "start_date_raw": sub["start_date_raw"],
                    "subtask_status": (
                        "parent_skip_exists" if parent_status == "skip_exists" else "would_create"
                    ),
                }
            )

    return out


def _check_existing_job_numbers(
    client: AsanaClient, dedupe_project_gids: List[str], jobs_to_check: List[str]
) -> Dict[str, bool]:
    existing_task_names: List[str] = []
    for project_gid in dedupe_project_gids:
        existing_task_names.extend(client.list_project_task_names(project_gid))

    existing_by_job: Dict[str, bool] = {}
    for job in jobs_to_check:
        existing_by_job[job] = any(job in name for name in existing_task_names)

    return existing_by_job


def main() -> None:
    if _render_live_alert_dashboard():
        return

    st.title("Trafficking to Asana")
    st.caption(
        "Uses Trafficking report only: one parent task per unique CampaignName+JobNumber and one subtask per unique OurRef."
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
        trafficking_df = _clean_dataframe(_read_uploaded_table(upload_obj, int(skip_top_rows)))
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

    required_cols = [
        "CampaignName",
        "JobNumber",
        "OurRef",
        "PropertyName",
        "LocationText",
        "SpecificationText",
        "StartDate",
    ]
    missing_cols = [c for c in required_cols if c not in trafficking_df.columns]
    if missing_cols:
        st.error("Trafficking Report is missing required columns: " + ", ".join(missing_cols))
        return

    st.subheader("Trafficking Report Preview")
    st.dataframe(trafficking_df.head(max_preview_rows), use_container_width=True)

    candidates, unmatched = _build_candidate_rows(trafficking_df)
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
            "On click: checks dedupe projects for existing job number in task names, then shows parent tasks and subtasks that would be created."
        )
        return

    client = AsanaClient(access_token=access_token.strip())

    job_numbers = sorted({row["job_number"] for row in candidates})
    try:
        existing_by_job = _check_existing_job_numbers(client, dedupe_project_gids, job_numbers)
    except AsanaError as exc:
        st.error(f"Failed while checking existing tasks: {exc}")
        return

    results: List[Dict[str, str]] = []
    progress = st.progress(0)
    total = max(len(candidates), 1)
    for idx, row in enumerate(candidates):
        exists = existing_by_job.get(row["job_number"], False)
        results.append(
            {
                "task_name": row["task_name"],
                "campaign_name": row["campaign_name"],
                "job_number": row["job_number"],
                "status": "skip_exists" if exists else "would_create",
                "reason": (
                    "Found existing task containing job number in dedupe projects"
                    if exists
                    else "No existing task found in dedupe projects"
                ),
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
    subtask_rows = _build_subtask_rows(trafficking_df, candidates, parent_status_by_job)
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
    skipped_count = int((result_df["status"] == "skip_exists").sum()) if not result_df.empty else 0
    subtask_would_create = (
        int(sum(1 for row in subtask_rows if row["subtask_status"] == "would_create"))
        if subtask_rows
        else 0
    )
    st.success(
        f"Dry run complete. Parent would create: {would_create_count}, parent skipped existing: {skipped_count}, subtask would create: {subtask_would_create}."
    )


if __name__ == "__main__":
    main()
