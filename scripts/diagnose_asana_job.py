#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Dict, List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from api.config import get_secret
from asana_client import AsanaClient
from gmail_client import GmailInboxClient
from scripts.daily_trafficking_dry_run import (
    fetch_dedupe_project_tasks,
    read_table_from_attachment,
    split_csv,
)
from trafficking_logic import (
    REQUIRED_TRAFFICKING_COLUMNS,
    build_candidate_rows,
    build_subtask_blueprints,
    clean_dataframe,
    existing_subtask_matches,
    find_existing_parent_task,
    parent_due_from_blueprints,
)


def require_secret(name: str) -> str:
    value = get_secret(name).strip()
    if not value:
        raise RuntimeError(f"Missing required secret/env var: {name}")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only diagnostic for one Asana trafficking job.")
    parser.add_argument("job_number")
    parser.add_argument("--gmail-query", default=get_secret("GMAIL_SEARCH_QUERY", "").strip())
    parser.add_argument("--skip-top-rows", type=int, default=int(get_secret("TRAFFICKING_SKIP_TOP_ROWS", "0") or "0"))
    args = parser.parse_args()

    job_number = args.job_number.strip()
    asana = AsanaClient(access_token=require_secret("ASANA_ACCESS_TOKEN"))
    dedupe_gids = split_csv(require_secret("ASANA_DEDUPE_PROJECT_GIDS"))

    gmail = GmailInboxClient(
        client_id=require_secret("GMAIL_CLIENT_ID"),
        client_secret=require_secret("GMAIL_CLIENT_SECRET"),
        refresh_token=require_secret("GMAIL_REFRESH_TOKEN"),
        user_id=get_secret("GMAIL_USER", "me").strip() or "me",
    )
    attachment = gmail.fetch_latest_attachment(
        subject_contains=get_secret("GMAIL_SUBJECT_CONTAINS", "Trafficking Report - acquirenz").strip(),
        allowed_extensions=(".tsv", ".csv", ".xls", ".xlsx"),
        query=args.gmail_query.strip() or None,
        max_messages=20,
    )
    df = clean_dataframe(read_table_from_attachment(attachment, skip_top_rows=args.skip_top_rows))
    missing_cols = [c for c in REQUIRED_TRAFFICKING_COLUMNS if c not in df.columns]
    if missing_cols:
        raise RuntimeError("Trafficking file missing required columns: " + ", ".join(missing_cols))

    rows_for_job = df[df["JobNumber"].astype(str).str.strip() == job_number]
    candidates, unmatched = build_candidate_rows(df)
    candidate_rows = [row for row in candidates if row["job_number"] == job_number]
    blueprint_map = build_subtask_blueprints(df)

    project_tasks = fetch_dedupe_project_tasks(asana, dedupe_project_gids=dedupe_gids)

    print(f"Source email subject: {attachment.subject}")
    print(f"Source email message id: {attachment.message_id}")
    print(f"Source email received UTC: {attachment.received_at}")
    print(f"Source attachment: {attachment.filename}")
    print(f"Rows parsed: {len(df)}")
    print(f"Rows with JobNumber {job_number}: {len(rows_for_job)}")
    print(f"Parent candidates for {job_number}: {len(candidate_rows)}")
    print("")

    if rows_for_job.empty:
        print(f"No source rows found for JobNumber {job_number} in the latest fetched report.")
        return 0

    print("Source rows for job:")
    display_cols = [
        "CampaignName",
        "JobNumber",
        "OurRef",
        "PropertyName",
        "LocationText",
        "SpecificationText",
        "StartDate",
    ]
    for _, row in rows_for_job.iterrows():
        values = {col: str(row.get(col, "") or "").strip() for col in display_cols}
        print(
            "- "
            + " | ".join(
                [
                    f"OurRef={values['OurRef']}",
                    f"Property={values['PropertyName']}",
                    f"Location={values['LocationText']}",
                    f"Spec={values['SpecificationText']}",
                    f"StartDate={values['StartDate']}",
                ]
            )
        )
    print("")

    if not candidate_rows:
        job_unmatched = [row for row in unmatched if job_number in str(row)]
        print("Rows exist, but no parent candidate was built for this job.")
        print(f"Unmatched rows mentioning job number: {len(job_unmatched)}")
        for row in job_unmatched[:20]:
            print(row)
        return 0

    for candidate in candidate_rows:
        key = (candidate["campaign_name"], candidate["job_number"])
        blueprints = blueprint_map.get(key, [])
        existing_parent = find_existing_parent_task(project_tasks, candidate["campaign_name"], candidate["job_number"])
        existing_subtasks: List[Dict[str, str]] = []
        if existing_parent:
            existing_subtasks = asana.list_subtasks(existing_parent["gid"])

        print(f"Candidate: {candidate['task_name']}")
        print(f"Expected parent due_on: {parent_due_from_blueprints(blueprints)}")
        print(
            "Parent status: "
            + (
                f"existing_parent gid={existing_parent.get('gid')} name={existing_parent.get('name')}"
                if existing_parent
                else "would_create"
            )
        )
        print(f"Expected subtask blueprints: {len(blueprints)}")
        print(f"Existing Asana subtasks on matched parent: {len(existing_subtasks)}")

        missing = []
        matching = []
        for sub in blueprints:
            target = matching if existing_subtask_matches(sub, existing_subtasks) else missing
            target.append(sub)

        print(f"Expected subtasks already matched in Asana: {len(matching)}")
        print(f"Expected subtasks missing from Asana: {len(missing)}")
        if missing:
            print("Missing expected subtasks:")
            for sub in missing:
                print(f"- {sub['subtask_name']} | due_on={sub['subtask_due_on']} | kind={sub.get('subtask_kind', 'source')}")
        print("Existing Asana subtasks:")
        for subtask in existing_subtasks:
            print(f"- {subtask['name']} | gid={subtask['gid']}")
        print("")

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
