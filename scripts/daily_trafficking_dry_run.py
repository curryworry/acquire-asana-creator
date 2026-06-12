import os
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

# Ensure repo root is importable when executed as a script in CI.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from asana_client import AsanaClient, AsanaError
from gmail_client import GmailAttachment, GmailError, GmailInboxClient
from trafficking_logic import (
    REQUIRED_TRAFFICKING_COLUMNS,
    build_candidate_rows,
    build_subtask_blueprints,
    clean_dataframe,
    existing_subtask_matches,
    find_existing_parent_task,
    parent_due_from_blueprints,
    read_table_bytes,
)

GID_RE = re.compile(r"^\d+$")


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def split_csv(value: str) -> List[str]:
    return [x.strip() for x in value.split(",") if x.strip()]


def as_bool(value: str, default: bool = True) -> bool:
    raw = (value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def read_table_from_attachment(attachment: GmailAttachment, skip_top_rows: int) -> pd.DataFrame:
    return read_table_bytes(attachment.filename, attachment.content, skip_top_rows)


def fetch_dedupe_project_tasks(client: AsanaClient, dedupe_project_gids: List[str]) -> List[Dict[str, str]]:
    tasks: List[Dict[str, str]] = []
    for project_gid in dedupe_project_gids:
        tasks.extend(client.list_project_tasks(project_gid))
    return tasks


def build_existing_parent_index(
    project_tasks: List[Dict[str, str]], candidates: List[Dict[str, str]]
) -> Dict[str, Dict[str, str]]:
    existing_by_job: Dict[str, Dict[str, str]] = {}
    for row in candidates:
        existing = find_existing_parent_task(project_tasks, row["campaign_name"], row["job_number"])
        if existing:
            existing_by_job[row["job_number"]] = existing

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
    asana_workspace_gid = require_env("ASANA_WORKSPACE_GID")
    asana_project_gid = require_env("ASANA_PROJECT_GID")
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
    dry_run_mode = as_bool(env("DRY_RUN_MODE", "true"), default=True)
    default_assignee_gid = env("DEFAULT_ASSIGNEE_GID", "")
    if default_assignee_gid:
        validate_gid_list("DEFAULT_ASSIGNEE_GID", [default_assignee_gid])
    dash_assignee_gid = env("DASH_ASSIGNEE_GID", default_assignee_gid)
    if dash_assignee_gid:
        validate_gid_list("DASH_ASSIGNEE_GID", [dash_assignee_gid])

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

    missing_cols = [c for c in REQUIRED_TRAFFICKING_COLUMNS if c not in df.columns]
    if missing_cols:
        raise RuntimeError("Trafficking file missing required columns: " + ", ".join(missing_cols))

    candidates, unmatched = build_candidate_rows(df)
    blueprint_map = build_subtask_blueprints(df)

    asana_client = AsanaClient(access_token=asana_access_token)
    dedupe_project_tasks = fetch_dedupe_project_tasks(asana_client, dedupe_project_gids=dedupe_gids)
    existing_by_job = build_existing_parent_index(dedupe_project_tasks, candidates)

    parent_results: List[Dict[str, str]] = []
    for row in candidates:
        key = (row["campaign_name"], row["job_number"])
        earliest_due_on = parent_due_from_blueprints(blueprint_map.get(key, []))
        existing_parent = existing_by_job.get(row["job_number"])
        parent_results.append(
            {
                "task_name": row["task_name"],
                "campaign_name": row["campaign_name"],
                "job_number": row["job_number"],
                "parent_due_on": earliest_due_on,
                "status": "existing_parent" if existing_parent else "would_create",
                "reason": (
                    "Found existing parent task in dedupe projects"
                    if existing_parent
                    else "No existing task found in dedupe projects"
                ),
                "parent_task_gid": existing_parent.get("gid", "") if existing_parent else "",
            }
        )

    if not dry_run_mode:
        for parent in parent_results:
            if parent["status"] != "would_create":
                continue
            payload: Dict[str, Any] = {
                "workspace": asana_workspace_gid,
                "name": parent["task_name"],
                "projects": [asana_project_gid],
            }
            if default_assignee_gid:
                payload["assignee"] = default_assignee_gid
            if parent["parent_due_on"]:
                payload["due_on"] = parent["parent_due_on"]

            try:
                created = asana_client.create_task(payload)
                parent["status"] = "created"
                parent["reason"] = "Created parent task"
                parent["parent_task_gid"] = str(created.get("gid", ""))
            except AsanaError as exc:
                parent["status"] = "error_parent_create"
                parent["reason"] = str(exc)

    subtask_results: List[Dict[str, str]] = []
    for parent in parent_results:
        key = (parent["campaign_name"], parent["job_number"])
        existing_subtasks: List[Dict[str, str]] = []
        if parent["status"] == "existing_parent" and parent["parent_task_gid"]:
            existing_subtasks = asana_client.list_subtasks(parent["parent_task_gid"])

        for sub in blueprint_map.get(key, []):
            base_row = {
                "parent_task_name": parent["task_name"],
                "parent_job_number": parent["job_number"],
                "parent_status": parent["status"],
                "our_ref": sub["our_ref"],
                "subtask_name": sub["subtask_name"],
                "subtask_due_on": sub["subtask_due_on"],
                "start_date_raw": sub["start_date_raw"],
                "subtask_kind": sub.get("subtask_kind", "source"),
            }

            if dry_run_mode:
                would_create = not (parent["status"] == "existing_parent" and existing_subtask_matches(sub, existing_subtasks))
                subtask_results.append(
                    {
                        **base_row,
                        "subtask_status": "would_create" if would_create else "skip_subtask_exists",
                        "subtask_gid": "",
                        "message": "Dry run",
                    }
                )
                continue

            if parent["status"] == "existing_parent" and existing_subtask_matches(sub, existing_subtasks):
                subtask_results.append(
                    {
                        **base_row,
                        "subtask_status": "skip_subtask_exists",
                        "subtask_gid": "",
                        "message": "Skipped because matching subtask already exists on parent",
                    }
                )
                continue

            if parent["status"] not in {"created", "existing_parent"} or not parent["parent_task_gid"]:
                subtask_results.append(
                    {
                        **base_row,
                        "subtask_status": "error_parent_not_created",
                        "subtask_gid": "",
                        "message": "Parent not created successfully",
                    }
                )
                continue

            payload = {"name": sub["subtask_name"]}
            if sub["subtask_due_on"]:
                payload["due_on"] = sub["subtask_due_on"]
            if sub.get("subtask_kind") == "control_dash" and dash_assignee_gid:
                payload["assignee"] = dash_assignee_gid

            try:
                created_sub = asana_client.create_subtask(parent["parent_task_gid"], payload)
                subtask_results.append(
                    {
                        **base_row,
                        "subtask_status": "created",
                        "subtask_gid": str(created_sub.get("gid", "")),
                        "message": "Created subtask",
                    }
                )
            except AsanaError as exc:
                subtask_results.append(
                    {
                        **base_row,
                        "subtask_status": "error_subtask_create",
                        "subtask_gid": "",
                        "message": str(exc),
                    }
                )

    parent_df = pd.DataFrame(parent_results)
    subtask_df = pd.DataFrame(subtask_results)
    unmatched_df = pd.DataFrame(unmatched)

    parent_csv = parent_df.to_csv(index=False).encode("utf-8")
    subtask_csv = subtask_df.to_csv(index=False).encode("utf-8")
    unmatched_csv = unmatched_df.to_csv(index=False).encode("utf-8") if not unmatched_df.empty else b"item,reason\n"

    parent_would_create = int((parent_df["status"] == "would_create").sum()) if not parent_df.empty else 0
    parent_existing = int((parent_df["status"] == "existing_parent").sum()) if not parent_df.empty else 0
    parent_created = int((parent_df["status"] == "created").sum()) if not parent_df.empty else 0
    parent_errors = int((parent_df["status"] == "error_parent_create").sum()) if not parent_df.empty else 0

    subtask_would_create = (
        int(sum(1 for row in subtask_results if row["subtask_status"] == "would_create"))
        if subtask_results
        else 0
    )
    subtask_skipped_existing = (
        int(sum(1 for row in subtask_results if row.get("subtask_status") == "skip_subtask_exists"))
        if subtask_results
        else 0
    )
    subtask_created = (
        int(sum(1 for row in subtask_results if row.get("subtask_status") == "created"))
        if subtask_results
        else 0
    )
    subtask_errors = (
        int(
            sum(
                1
                for row in subtask_results
                if row.get("subtask_status") in {"error_parent_not_created", "error_subtask_create"}
            )
        )
        if subtask_results
        else 0
    )

    summary = (
        f"Daily Trafficking {'Dry Run' if dry_run_mode else 'Live Create'} Summary\n\n"
        f"Source email subject: {attachment.subject}\n"
        f"Source email message id: {attachment.message_id}\n"
        f"Source email received (UTC): {attachment.received_at}\n"
        f"Source attachment: {attachment.filename}\n"
        f"Mode: {'DRY_RUN' if dry_run_mode else 'LIVE_CREATE'}\n"
        f"Rows parsed: {len(df)}\n"
        f"Parent candidates: {len(parent_df)}\n"
        f"Parent would create: {parent_would_create}\n"
        f"Parent existing: {parent_existing}\n"
        f"Parent created: {parent_created}\n"
        f"Parent creation errors: {parent_errors}\n"
        f"Subtask rows: {len(subtask_df)}\n"
        f"Subtask would create: {subtask_would_create}\n"
        f"Subtask skipped existing: {subtask_skipped_existing}\n"
        f"Subtask created: {subtask_created}\n"
        f"Subtask errors: {subtask_errors}\n"
        f"Unmatched items: {len(unmatched_df)}\n"
        f"Dedupe projects checked: {', '.join(dedupe_gids)}\n"
    )

    inbox.send_email(
        to_email=report_email_to,
        subject=(
            "[Dry Run] Trafficking -> Asana Summary"
            if dry_run_mode
            else "[Live Create] Trafficking -> Asana Summary"
        ),
        body_text=summary,
        attachments={
            "parent_task_results.csv": parent_csv,
            "subtask_results.csv": subtask_csv,
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
