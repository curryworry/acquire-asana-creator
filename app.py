import re
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from asana_client import AsanaClient, AsanaError


st.set_page_config(page_title="Trafficking to Asana", page_icon="✅", layout="wide")

BILLING_CAMPAIGN_RE = re.compile(r"^[^:]+:\s*(.*?)\s*\(job\s*(\d+)\)\s*$", re.IGNORECASE)


def _get_secret(name: str, default: str = "") -> str:
    try:
        return str(st.secrets.get(name, default))
    except Exception:
        return default


def _split_csv_secret(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _read_uploaded_table(uploaded_file: Any, skip_top_rows: int) -> pd.DataFrame:
    filename = str(getattr(uploaded_file, "name", "")).lower()
    raw = uploaded_file.getvalue()

    if filename.endswith(".csv"):
        return pd.read_csv(BytesIO(raw), skiprows=skip_top_rows)

    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        # Some vendor ".xls" exports are actually tab-delimited text files.
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

    raise ValueError("Unsupported file type. Upload .csv, .xls, or .xlsx.")


def _normalize_campaign_name(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip()
    text = re.sub(r",\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _parse_billing_campaign(raw_campaign: Any) -> Optional[Tuple[str, str]]:
    text = "" if raw_campaign is None else str(raw_campaign).strip()
    match = BILLING_CAMPAIGN_RE.match(text)
    if not match:
        return None

    campaign_name = _normalize_campaign_name(match.group(1))
    job_number = match.group(2).strip()
    if not campaign_name or not job_number:
        return None

    return campaign_name, job_number


def _build_candidate_rows(
    trafficking_df: pd.DataFrame, billing_df: pd.DataFrame
) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    trafficking_campaigns = sorted(
        {
            _normalize_campaign_name(v)
            for v in trafficking_df.get("Campaign", pd.Series(dtype=str)).tolist()
            if _normalize_campaign_name(v)
        }
    )

    billing_map: Dict[str, Dict[str, str]] = {}
    unmatched_billing_count = 0
    for raw in billing_df.get("Campaign", pd.Series(dtype=str)).tolist():
        parsed = _parse_billing_campaign(raw)
        if not parsed:
            unmatched_billing_count += 1
            continue

        campaign_name, job_number = parsed
        billing_map.setdefault(campaign_name, {})[job_number] = str(raw)

    candidates: List[Dict[str, str]] = []
    unmatched_trafficking: List[Dict[str, str]] = []

    for traf_campaign in trafficking_campaigns:
        jobs = billing_map.get(traf_campaign, {})
        if not jobs:
            unmatched_trafficking.append(
                {
                    "trafficking_campaign": traf_campaign,
                    "reason": "No matching billing campaign/job found",
                }
            )
            continue

        for job_number in sorted(jobs.keys()):
            candidates.append(
                {
                    "trafficking_campaign": traf_campaign,
                    "billing_campaign": jobs[job_number],
                    "job_number": job_number,
                    "task_name": f"{traf_campaign} ({job_number})",
                    "billing_parse_status": "ok",
                }
            )

    if unmatched_billing_count > 0:
        unmatched_trafficking.append(
            {
                "trafficking_campaign": "(billing rows)",
                "reason": f"{unmatched_billing_count} billing rows did not match 'Advertiser: Campaign (job 1234)' format",
            }
        )

    return candidates, unmatched_trafficking


def _as_due_on(date_value: Any) -> str:
    parsed = pd.to_datetime(str(date_value).strip(), dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _build_subtask_rows(
    trafficking_df: pd.DataFrame, candidates: List[Dict[str, str]], parent_status_by_job: Dict[str, str]
) -> List[Dict[str, str]]:
    campaign_to_subtasks: Dict[str, List[Dict[str, str]]] = {}
    seen_refs: set[tuple[str, str]] = set()

    for _, row in trafficking_df.iterrows():
        campaign_name = _normalize_campaign_name(row.get("Campaign", ""))
        if not campaign_name:
            continue

        our_ref = str(row.get("Our Ref", "")).strip()
        if not our_ref:
            continue

        key = (campaign_name, our_ref)
        if key in seen_refs:
            continue
        seen_refs.add(key)

        property_value = str(row.get("Property", "")).strip()
        location_value = str(row.get("Location", "")).strip()
        ad_unit_value = str(row.get("Ad Unit", "")).strip()
        start_date_raw = str(row.get("Start Date", "")).strip()

        subtask_name = f"({our_ref}) {property_value} - {location_value}: {ad_unit_value}".strip()
        campaign_to_subtasks.setdefault(campaign_name, []).append(
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
        for sub in campaign_to_subtasks.get(parent["trafficking_campaign"], []):
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
    st.title("Trafficking + Billing to Asana")
    st.caption(
        "Creates one parent task per matched campaign/job and one subtask per unique Our Ref in that campaign."
    )

    access_token = _get_secret("ASANA_ACCESS_TOKEN")
    workspace_gid = _get_secret("ASANA_WORKSPACE_GID")
    target_project_gid = _get_secret("ASANA_PROJECT_GID")
    dedupe_project_gids = _split_csv_secret(_get_secret("ASANA_DEDUPE_PROJECT_GIDS"))
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

    st.subheader("Upload Reports")
    left_col, right_col = st.columns(2)
    with left_col:
        trafficking_file = st.file_uploader(
            "Trafficking Report", type=["csv", "xls", "xlsx"], key="trafficking_report_file"
        )
    with right_col:
        billing_file = st.file_uploader(
            "Billing Report", type=["csv", "xls", "xlsx"], key="billing_report_file"
        )

    if not trafficking_file or not billing_file:
        st.info("Upload both Trafficking and Billing reports to continue.")
        return

    skip_col1, skip_col2 = st.columns(2)
    with skip_col1:
        trafficking_skip_top_rows = st.number_input(
            "Trafficking: skip top rows",
            min_value=0,
            step=1,
            value=3,
            help="Rows to skip before the Trafficking header row.",
        )
    with skip_col2:
        billing_skip_top_rows = st.number_input(
            "Billing: skip top rows",
            min_value=0,
            step=1,
            value=2,
            help="Rows to skip before the Billing header row.",
        )

    try:
        trafficking_df = _read_uploaded_table(trafficking_file, int(trafficking_skip_top_rows)).fillna("")
    except Exception as exc:
        st.error(f"Could not read Trafficking Report file: {exc}")
        return

    try:
        billing_df = _read_uploaded_table(billing_file, int(billing_skip_top_rows)).fillna("")
    except Exception as exc:
        st.error(f"Could not read Billing Report file: {exc}")
        return

    if trafficking_df.empty:
        st.warning("Trafficking Report file has no rows.")
        return
    if billing_df.empty:
        st.warning("Billing Report file has no rows.")
        return

    if len(trafficking_df.columns) <= 1:
        st.warning("Trafficking parse looks wrong (single column). Adjust Trafficking skip rows.")
        st.dataframe(trafficking_df.head(20), use_container_width=True)
        return
    if len(billing_df.columns) <= 1:
        st.warning("Billing parse looks wrong (single column). Adjust Billing skip rows.")
        st.dataframe(billing_df.head(20), use_container_width=True)
        return

    if "Campaign" not in trafficking_df.columns:
        st.error("Trafficking Report is missing required 'Campaign' column.")
        return
    if "Campaign" not in billing_df.columns:
        st.error("Billing Report is missing required 'Campaign' column.")
        return
    required_trafficking_cols = ["Our Ref", "Property", "Location", "Ad Unit", "Start Date"]
    missing_trafficking_cols = [c for c in required_trafficking_cols if c not in trafficking_df.columns]
    if missing_trafficking_cols:
        st.error(
            "Trafficking Report is missing required columns for subtasks: "
            + ", ".join(missing_trafficking_cols)
        )
        return

    st.subheader("Trafficking Report Preview")
    st.dataframe(trafficking_df.head(30), use_container_width=True)

    st.subheader("Billing Report Preview")
    st.dataframe(billing_df.head(30), use_container_width=True)

    candidates, unmatched = _build_candidate_rows(trafficking_df, billing_df)

    st.subheader("Preflight Summary")
    c1, c2, c3 = st.columns(3)
    c1.metric("Unique Trafficking Campaigns", len({_normalize_campaign_name(v) for v in trafficking_df["Campaign"].tolist() if _normalize_campaign_name(v)}))
    c2.metric("Candidate Tasks", len(candidates))
    c3.metric("Unmatched Items", len(unmatched))

    if unmatched:
        st.write("Unmatched items")
        st.dataframe(pd.DataFrame(unmatched), use_container_width=True)

    if not candidates:
        st.warning("No candidate tasks found from current Trafficking/Billing match rules.")
        return

    st.write("Candidate tasks")
    st.dataframe(pd.DataFrame(candidates), use_container_width=True)

    if not dedupe_project_gids:
        st.error("No dedupe projects configured. Add ASANA_DEDUPE_PROJECT_GIDS in secrets.")
        return

    create_clicked = st.button("Check Existing + Build Task + Subtask List", type="primary")
    if not create_clicked:
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

    parent_status_by_job = {row["job_number"]: row["status"] for row in results}
    subtask_rows = _build_subtask_rows(trafficking_df, candidates, parent_status_by_job)
    st.subheader("Subtask Output (Dry Run)")
    st.dataframe(pd.DataFrame(subtask_rows), use_container_width=True)

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
