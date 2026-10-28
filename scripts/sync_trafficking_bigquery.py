import os
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gmail_client import GmailError, GmailInboxClient
from trafficking_bigquery import (
    DEFAULT_BUY_ENHANCED_VIEW,
    DEFAULT_DATASET,
    DEFAULT_MASTER_ENHANCED_VIEW,
    DEFAULT_PROJECT_ID,
    DEFAULT_TRAFFICKING_TABLE,
    REQUIRED_TRAFFICKING_BQ_COLUMNS,
    build_bigquery_client,
    ensure_enhanced_views,
    prepare_trafficking_records,
    upsert_trafficking_records,
)
from trafficking_logic import clean_dataframe, read_table_bytes


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def require_env(name: str) -> str:
    value = env(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


def latest_report_query(subject_contains: str) -> str:
    configured = env("GMAIL_TRAFFICKING_FORCE_QUERY", "")
    return configured or f'in:anywhere subject:"{subject_contains}" has:attachment'


def main() -> int:
    subject_contains = env("GMAIL_SUBJECT_CONTAINS") or "Trafficking Report - acquirenz"
    skip_top_rows = int(env("TRAFFICKING_SKIP_TOP_ROWS", "0") or "0")
    project_id = env("BQ_PROJECT_ID", DEFAULT_PROJECT_ID)
    dataset = env("BQ_DATASET", DEFAULT_DATASET)
    trafficking_table = env("BQ_TRAFFICKING_TABLE", DEFAULT_TRAFFICKING_TABLE)
    buy_view = env("BQ_BUY_ENHANCED_VIEW", DEFAULT_BUY_ENHANCED_VIEW)
    master_view = env("BQ_MASTER_ENHANCED_VIEW", DEFAULT_MASTER_ENHANCED_VIEW)

    inbox = GmailInboxClient(
        client_id=require_env("GMAIL_CLIENT_ID"),
        client_secret=require_env("GMAIL_CLIENT_SECRET"),
        refresh_token=require_env("GMAIL_REFRESH_TOKEN"),
        user_id=env("GMAIL_USER", "me"),
    )
    attachment = inbox.fetch_latest_attachment(
        subject_contains=subject_contains,
        allowed_extensions=(".tsv", ".csv", ".xls", ".xlsx"),
        query=latest_report_query(subject_contains),
        max_messages=20,
    )
    df = clean_dataframe(read_table_bytes(attachment.filename, attachment.content, skip_top_rows))
    missing = [column for column in REQUIRED_TRAFFICKING_BQ_COLUMNS if column not in df.columns]
    if missing:
        raise RuntimeError(
            "Trafficking file missing columns required for the BigQuery table: "
            + ", ".join(missing)
        )

    client = build_bigquery_client(project_id, env("BQ_SERVICE_ACCOUNT_JSON", ""))
    records = prepare_trafficking_records(df, attachment)
    loaded = upsert_trafficking_records(client, records, project_id, dataset, trafficking_table)
    ensure_enhanced_views(client, project_id, dataset, trafficking_table, buy_view, master_view)

    print("Manual Trafficking BigQuery Sync Summary")
    print(f"Source email subject: {attachment.subject}")
    print(f"Source email received (UTC): {attachment.received_at}")
    print(f"Source attachment: {attachment.filename}")
    print(f"Rows parsed and upserted: {loaded}")
    print(f"Table: {project_id}.{dataset}.{trafficking_table}")
    print(f"Views refreshed: {buy_view}, {master_view}")
    print("Asana actions: none")
    print("Gmail changes: none")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, GmailError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
