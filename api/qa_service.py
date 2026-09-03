from __future__ import annotations

import re
from csv import DictWriter
from datetime import datetime, timezone
from io import BytesIO
from io import StringIO
from typing import Any

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from api.dashboard_service import bq_context, table_fqn
from api.config import get_secret
from gmail_client import GmailAttachment, GmailInboxClient


DEFAULT_TRADEME_VIDEO_SUBJECT = "TradeMe On Video - Last 7 Days"
QA_VIDEO_TRADEME_TABLE = "qa_video_on_trademe"
FOOTER_PREFIXES = (
    "report time",
    "date range",
    "group by",
    "mrc accredited",
    "reporting numbers",
    "filter by",
)


def _as_int(value: str, default: int) -> int:
    try:
        return int(str(value or "").strip())
    except ValueError:
        return default


def _clean_cell(value: Any) -> str:
    if value is None:
        return ""
    if pd.isna(value):
        return ""
    return str(value).strip()


def _normalize_header(value: Any) -> str:
    return re.sub(r"\s+", " ", _clean_cell(value)).strip().rstrip(":").lower()


def _read_raw_attachment(attachment: GmailAttachment) -> pd.DataFrame:
    filename = attachment.filename.lower()
    raw = BytesIO(attachment.content)

    if filename.endswith((".xlsx", ".xls")):
        return pd.read_excel(raw, header=None, dtype=object)

    if filename.endswith(".csv"):
        for encoding in ("utf-8-sig", "utf-16", "latin1"):
            raw.seek(0)
            try:
                return pd.read_csv(raw, header=None, dtype=object, encoding=encoding)
            except UnicodeError:
                continue
            except pd.errors.ParserError:
                raw.seek(0)
                return pd.read_csv(raw, header=None, dtype=object, encoding=encoding, engine="python", on_bad_lines="skip")

    raise ValueError("Unsupported QA attachment type. Expected .csv, .xls, or .xlsx.")


def _find_report_columns(raw_df: pd.DataFrame) -> tuple[int, int, int]:
    for row_idx, row in raw_df.iterrows():
        normalized = [_normalize_header(value) for value in row.tolist()]
        campaign_col = next((idx for idx, value in enumerate(normalized) if value == "campaign"), None)
        impressions_col = next((idx for idx, value in enumerate(normalized) if value == "impressions"), None)
        if campaign_col is not None and impressions_col is not None:
            return int(row_idx), campaign_col, impressions_col
    raise ValueError("Could not find Campaign and Impressions columns in the DV360 report.")


def _parse_impressions(value: Any) -> int:
    text = _clean_cell(value)
    if not text:
        return 0
    clean = re.sub(r"[^0-9.-]", "", text)
    if clean in {"", ".", "-", "-."}:
        return 0
    return int(float(clean))


def _extract_metadata(raw_df: pd.DataFrame) -> dict[str, str]:
    metadata: dict[str, str] = {}
    for _, row in raw_df.iterrows():
        label = _normalize_header(row.iloc[0] if len(row) > 0 else "")
        value = _clean_cell(row.iloc[1] if len(row) > 1 else "")
        if label == "report time":
            metadata["report_time"] = value
        elif label == "date range":
            metadata["date_range"] = value
        elif label == "group by":
            metadata["group_by"] = value
    return metadata


def parse_video_trademe_attachment(attachment: GmailAttachment) -> dict[str, Any]:
    raw_df = _read_raw_attachment(attachment).fillna("")
    header_idx, campaign_col, impressions_col = _find_report_columns(raw_df)
    metadata = _extract_metadata(raw_df)

    rows: list[dict[str, Any]] = []
    total_impressions = 0
    for source_row_number, row in raw_df.iloc[header_idx + 1 :].iterrows():
        campaign = _clean_cell(row.iloc[campaign_col] if campaign_col < len(row) else "")
        first_cell = _normalize_header(row.iloc[0] if len(row) > 0 else "")

        if first_cell.startswith(FOOTER_PREFIXES):
            break
        if not campaign:
            continue

        impressions = _parse_impressions(row.iloc[impressions_col] if impressions_col < len(row) else "")
        if impressions <= 0:
            continue

        total_impressions += impressions
        rows.append(
            {
                "ROW_ID": f"{attachment.message_id}:{source_row_number}",
                "CAMPAIGN": campaign,
                "IMPRESSIONS": impressions,
            }
        )

    return {
        "rows": rows,
        "meta": {
            "subject": attachment.subject,
            "message_id": attachment.message_id,
            "received_at": attachment.received_at,
            "attachment": attachment.filename,
            "report_time": metadata.get("report_time", ""),
            "date_range": metadata.get("date_range", ""),
            "group_by": metadata.get("group_by", ""),
            "total_rows": str(len(rows)),
            "total_impressions": str(total_impressions),
            "refreshed_at": datetime.now(timezone.utc).isoformat(),
        },
    }


def fetch_video_on_trademe_gmail_report() -> dict[str, Any]:
    subject_contains = get_secret("QA_TRADEME_VIDEO_SUBJECT_CONTAINS", "").strip() or DEFAULT_TRADEME_VIDEO_SUBJECT
    search_query = get_secret("QA_TRADEME_VIDEO_GMAIL_SEARCH_QUERY", "").strip()
    max_messages = _as_int(get_secret("QA_TRADEME_VIDEO_MAX_MESSAGES", "20"), 20)

    inbox = GmailInboxClient(
        client_id=get_secret("GMAIL_CLIENT_ID"),
        client_secret=get_secret("GMAIL_CLIENT_SECRET"),
        refresh_token=get_secret("GMAIL_REFRESH_TOKEN"),
        user_id=get_secret("GMAIL_USER", "me").strip() or "me",
    )
    attachment = inbox.fetch_latest_attachment(
        subject_contains=subject_contains,
        allowed_extensions=(".csv", ".xls", ".xlsx"),
        query=search_query or f'in:anywhere subject:"{subject_contains}" has:attachment',
        max_messages=max_messages,
    )
    return parse_video_trademe_attachment(attachment)


def _qa_video_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("row_number", "INTEGER"),
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("last_7_day_impressions", "INTEGER"),
        bigquery.SchemaField("source_subject", "STRING"),
        bigquery.SchemaField("source_message_id", "STRING"),
        bigquery.SchemaField("source_attachment", "STRING"),
        bigquery.SchemaField("email_received_at", "TIMESTAMP"),
        bigquery.SchemaField("report_time", "STRING"),
        bigquery.SchemaField("date_range", "STRING"),
        bigquery.SchemaField("group_by", "STRING"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    ]


def ensure_video_on_trademe_table(client: bigquery.Client, project_id: str, dataset: str) -> None:
    table_id = f"{project_id}.{dataset}.{QA_VIDEO_TRADEME_TABLE}"
    try:
        client.get_table(table_id)
        return
    except NotFound:
        pass

    table = bigquery.Table(table_id, schema=_qa_video_schema())
    client.create_table(table)


def load_video_on_trademe_table(report: dict[str, Any]) -> dict[str, str | int]:
    client, project_id, dataset = bq_context()
    ensure_video_on_trademe_table(client, project_id, dataset)
    table_id = f"{project_id}.{dataset}.{QA_VIDEO_TRADEME_TABLE}"
    meta = report.get("meta", {})
    loaded_at = datetime.now(timezone.utc).isoformat()
    fieldnames = [field.name for field in _qa_video_schema()]
    output = StringIO()
    writer = DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for index, row in enumerate(report.get("rows", []), start=1):
        writer.writerow(
            {
                "row_number": index,
                "campaign": row.get("CAMPAIGN", ""),
                "last_7_day_impressions": int(row.get("IMPRESSIONS", 0) or 0),
                "source_subject": meta.get("subject", ""),
                "source_message_id": meta.get("message_id", ""),
                "source_attachment": meta.get("attachment", ""),
                "email_received_at": meta.get("received_at", ""),
                "report_time": meta.get("report_time", ""),
                "date_range": meta.get("date_range", ""),
                "group_by": meta.get("group_by", ""),
                "loaded_at": loaded_at,
            }
        )

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=_qa_video_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    csv_bytes = output.getvalue().encode("utf-8")
    client.load_table_from_file(BytesIO(csv_bytes), table_id, job_config=job_config).result()
    return {
        "project_id": project_id,
        "dataset": dataset,
        "table": QA_VIDEO_TRADEME_TABLE,
        "rows": len(report.get("rows", [])),
        "total_impressions": int(meta.get("total_impressions", "0") or 0),
    }


def ingest_video_on_trademe_report() -> dict[str, str | int]:
    report = fetch_video_on_trademe_gmail_report()
    return load_video_on_trademe_table(report)


def video_on_trademe_dashboard() -> dict[str, Any]:
    client, project_id, dataset = bq_context()
    ensure_video_on_trademe_table(client, project_id, dataset)
    rows = list(
        client.query(
            f"""
SELECT
  row_number,
  campaign,
  last_7_day_impressions,
  source_subject,
  source_message_id,
  source_attachment,
  email_received_at,
  report_time,
  date_range,
  group_by,
  loaded_at
FROM {table_fqn(project_id, dataset, QA_VIDEO_TRADEME_TABLE)}
ORDER BY last_7_day_impressions DESC, campaign
"""
        ).result()
    )
    data = [
        {
            "ROW_ID": f"{r['source_message_id']}:{r['row_number']}",
            "CAMPAIGN": str(r["campaign"] or ""),
            "IMPRESSIONS": int(r["last_7_day_impressions"] or 0),
        }
        for r in rows
    ]
    first = rows[0] if rows else None
    return {
        "rows": data,
        "meta": {
            "project_id": project_id,
            "dataset": dataset,
            "table": QA_VIDEO_TRADEME_TABLE,
            "subject": str(first["source_subject"] or "") if first else "",
            "message_id": str(first["source_message_id"] or "") if first else "",
            "received_at": first["email_received_at"].isoformat() if first and first["email_received_at"] else "",
            "attachment": str(first["source_attachment"] or "") if first else "",
            "report_time": str(first["report_time"] or "") if first else "",
            "date_range": str(first["date_range"] or "") if first else "",
            "group_by": str(first["group_by"] or "") if first else "",
            "loaded_at": first["loaded_at"].isoformat() if first and first["loaded_at"] else "",
            "total_rows": str(len(data)),
            "total_impressions": str(sum(row["IMPRESSIONS"] for row in data)),
        },
    }
