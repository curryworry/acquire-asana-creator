import os
import re
import sys
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

# Ensure repo root is importable when executed as a script in CI.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from asana_client import AsanaClient, AsanaError
from gmail_client import GmailAttachment, GmailError, GmailInboxClient

GID_RE = re.compile(r"^\d+$")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def split_csv(value: str) -> List[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def normalize_campaign_name(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip()
    text = re.sub(r",\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_job_number(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        text = text[:-2]
    return text


def as_due_on(date_value: Any) -> str:
    raw = str(date_value).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        parsed = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    else:
        parsed = pd.to_datetime(raw, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    clean.columns = [str(c).strip() for c in clean.columns]
    unnamed_cols = [c for c in clean.columns if c.lower().startswith("unnamed:")]
    if unnamed_cols:
        clean = clean.drop(columns=unnamed_cols, errors="ignore")
    return clean.fillna("")


def read_table_from_attachment(attachment: GmailAttachment, skip_top_rows: int) -> pd.DataFrame:
    filename = attachment.filename.lower()
    raw = attachment.content

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

    raise ValueError("Unsupported attachment type.")


def build_candidate_rows(trafficking_df: pd.DataFrame) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    candidates: List[Dict[str, str]] = []
    unmatched: List[Dict[str, str]] = []

    seen_keys = set()
    missing_campaign = 0
    missing_job = 0

    for _, row in trafficking_df.iterrows():
        campaign_name = normalize_campaign_name(row.get("CampaignName", ""))
        job_number = normalize_job_number(row.get("JobNumber", ""))

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
        unmatched.append({"item": "Trafficking rows", "reason": f"{missing_campaign} rows missing CampaignName"})
    if missing_job:
        unmatched.append({"item": "Trafficking rows", "reason": f"{missing_job} rows missing JobNumber"})

    return sorted(candidates, key=lambda x: (x["campaign_name"], x["job_number"])), unmatched


def build_subtask_rows(
    trafficking_df: pd.DataFrame,
    candidates: List[Dict[str, str]],
    parent_status_by_job: Dict[str, str],
) -> List[Dict[str, str]]:
    by_campaign_job: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    seen_refs = set()

    for _, row in trafficking_df.iterrows():
        campaign_name = normalize_campaign_name(row.get("CampaignName", ""))
        job_number = normalize_job_number(row.get("JobNumber", ""))
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
                "subtask_due_on": as_due_on(start_date_raw),
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


def check_existing_job_numbers(
    client: AsanaClient, dedupe_project_gids: List[str], jobs_to_check: List[str]
) -> Dict[str, bool]:
    existing_task_names: List[str] = []
    for project_gid in dedupe_project_gids:
        existing_task_names.extend(client.list_project_task_names(project_gid))

    existing_by_job: Dict[str, bool] = {}
    for job in jobs_to_check:
        existing_by_job[job] = any(job in name for name in existing_task_names)

    return existing_by_job


def require_env(name: str) -> str:
    value = env(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def validate_gid_list(name: str, values: List[str]) -> None:
    bad = [v for v in values if not GID_RE.match(v)]
    if bad:
        raise RuntimeError(f"Invalid GID(s) in {name}: {', '.join(bad)}")


def main() -> int:
    asana_access_token = require_env("ASANA_ACCESS_TOKEN")
    _ = require_env("ASANA_WORKSPACE_GID")
    _ = require_env("ASANA_PROJECT_GID")
    dedupe_gids = split_csv(require_env("ASANA_DEDUPE_PROJECT_GIDS"))
    validate_gid_list("ASANA_DEDUPE_PROJECT_GIDS", dedupe_gids)

    gmail_client_id = require_env("GMAIL_CLIENT_ID")
    gmail_client_secret = require_env("GMAIL_CLIENT_SECRET")
    gmail_refresh_token = require_env("GMAIL_REFRESH_TOKEN")
    gmail_user = env("GMAIL_USER", "me")
    subject_contains = env("GMAIL_SUBJECT_CONTAINS", "Trafficking Report - acquirenz")
    search_query = env("GMAIL_SEARCH_QUERY", "") or None
    processed_label_name = env("GMAIL_PROCESSED_LABEL", "processed")
    report_email_to = require_env("REPORT_EMAIL_TO")
    skip_top_rows = int(env("TRAFFICKING_SKIP_TOP_ROWS", "0") or "0")

    inbox = GmailInboxClient(
        client_id=gmail_client_id,
        client_secret=gmail_client_secret,
        refresh_token=gmail_refresh_token,
        user_id=gmail_user,
    )

    attachment = inbox.fetch_latest_attachment(
        subject_contains=subject_contains,
        allowed_extensions=(".tsv", ".csv", ".xls", ".xlsx"),
        query=search_query,
        max_messages=20,
    )

    df = clean_dataframe(read_table_from_attachment(attachment, skip_top_rows=skip_top_rows))

    required_cols = [
        "CampaignName",
        "JobNumber",
        "OurRef",
        "PropertyName",
        "LocationText",
        "SpecificationText",
        "StartDate",
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise RuntimeError("Trafficking file missing required columns: " + ", ".join(missing_cols))

    candidates, unmatched = build_candidate_rows(df)

    asana_client = AsanaClient(access_token=asana_access_token)
    existing_by_job = check_existing_job_numbers(
        asana_client, dedupe_project_gids=dedupe_gids, jobs_to_check=sorted({r["job_number"] for r in candidates})
    )

    parent_results: List[Dict[str, str]] = []
    for row in candidates:
        exists = existing_by_job.get(row["job_number"], False)
        parent_results.append(
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
            }
        )

    parent_status_by_job = {row["job_number"]: row["status"] for row in parent_results}
    subtask_results = build_subtask_rows(df, candidates, parent_status_by_job)

    parent_df = pd.DataFrame(parent_results)
    subtask_df = pd.DataFrame(subtask_results)
    unmatched_df = pd.DataFrame(unmatched)

    parent_csv = parent_df.to_csv(index=False).encode("utf-8")
    subtask_csv = subtask_df.to_csv(index=False).encode("utf-8")
    unmatched_csv = unmatched_df.to_csv(index=False).encode("utf-8") if not unmatched_df.empty else b"item,reason\n"

    parent_would_create = int((parent_df["status"] == "would_create").sum()) if not parent_df.empty else 0
    parent_skipped = int((parent_df["status"] == "skip_exists").sum()) if not parent_df.empty else 0
    subtask_would_create = (
        int(sum(1 for row in subtask_results if row["subtask_status"] == "would_create"))
        if subtask_results
        else 0
    )

    summary = (
        "Daily Trafficking Dry Run Summary\n\n"
        f"Source email subject: {attachment.subject}\n"
        f"Source email message id: {attachment.message_id}\n"
        f"Source email received (UTC): {attachment.received_at}\n"
        f"Source attachment: {attachment.filename}\n"
        f"Rows parsed: {len(df)}\n"
        f"Parent candidates: {len(parent_df)}\n"
        f"Parent would create: {parent_would_create}\n"
        f"Parent skipped existing: {parent_skipped}\n"
        f"Subtask rows: {len(subtask_df)}\n"
        f"Subtask would create: {subtask_would_create}\n"
        f"Unmatched items: {len(unmatched_df)}\n"
        f"Dedupe projects checked: {', '.join(dedupe_gids)}\n"
    )

    inbox.send_email(
        to_email=report_email_to,
        subject="[Dry Run] Trafficking -> Asana Summary",
        body_text=summary,
        attachments={
            "parent_task_dry_run.csv": parent_csv,
            "subtask_dry_run.csv": subtask_csv,
            "unmatched_items.csv": unmatched_csv,
        },
    )

    label_id = inbox.ensure_label(processed_label_name)
    inbox.mark_read_and_label(message_id=attachment.message_id, label_id=label_id)

    print(summary)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, GmailError, AsanaError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
