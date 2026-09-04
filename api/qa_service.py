from __future__ import annotations

import re
from csv import DictReader, DictWriter
from datetime import date, datetime, timezone
from io import BytesIO
from io import StringIO
from typing import Any, TypeVar
from zipfile import ZipFile
from zoneinfo import ZoneInfo

import pandas as pd
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

from api.dashboard_service import bq_context, table_fqn
from api.config import get_secret
from dv360_client import DV360Client
from gmail_client import GmailAttachment, GmailInboxClient


DEFAULT_TRADEME_VIDEO_SUBJECT = "TradeMe On Video - Last 7 Days"
QA_VIDEO_TRADEME_TABLE = "qa_video_on_trademe"
QA_MISSING_INCLUSION_TABLE = "qa_missing_inclusion_list"
DEFAULT_SDF_VERSION = "SDF_VERSION_10_1"
DEFAULT_SDF_TIME_ZONE = "America/New_York"
SDF_FILE_TYPES = [
    "FILE_TYPE_CAMPAIGN",
    "FILE_TYPE_INSERTION_ORDER",
    "FILE_TYPE_LINE_ITEM",
    "FILE_TYPE_LINE_ITEM_QA",
]
SDF_CURRENT_IO_FILE_TYPES = [
    "FILE_TYPE_INSERTION_ORDER",
    "FILE_TYPE_LINE_ITEM",
    "FILE_TYPE_LINE_ITEM_QA",
]
FOOTER_PREFIXES = (
    "report time",
    "date range",
    "group by",
    "mrc accredited",
    "reporting numbers",
    "filter by",
)
T = TypeVar("T")


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


def _qa_missing_inclusion_schema() -> list[bigquery.SchemaField]:
    return [
        bigquery.SchemaField("row_number", "INTEGER"),
        bigquery.SchemaField("partner_id", "STRING"),
        bigquery.SchemaField("advertiser_id", "STRING"),
        bigquery.SchemaField("advertiser_name", "STRING"),
        bigquery.SchemaField("insertion_order_id", "STRING"),
        bigquery.SchemaField("insertion_order_name", "STRING"),
        bigquery.SchemaField("insertion_order_status", "STRING"),
        bigquery.SchemaField("io_budget_start_date", "DATE"),
        bigquery.SchemaField("io_budget_end_date", "DATE"),
        bigquery.SchemaField("line_item_id", "STRING"),
        bigquery.SchemaField("line_item_name", "STRING"),
        bigquery.SchemaField("line_item_status", "STRING"),
        bigquery.SchemaField("line_item_type", "STRING"),
        bigquery.SchemaField("line_item_subtype", "STRING"),
        bigquery.SchemaField("line_item_start_date", "DATE"),
        bigquery.SchemaField("line_item_end_date", "DATE"),
        bigquery.SchemaField("effective_start_date", "DATE"),
        bigquery.SchemaField("effective_end_date", "DATE"),
        bigquery.SchemaField("li_channel_include", "STRING"),
        bigquery.SchemaField("li_site_include", "STRING"),
        bigquery.SchemaField("li_app_include", "STRING"),
        bigquery.SchemaField("li_channel_include_qa", "STRING"),
        bigquery.SchemaField("li_site_include_qa", "STRING"),
        bigquery.SchemaField("li_app_include_qa", "STRING"),
        bigquery.SchemaField("advertiser_channel_include_count", "INTEGER"),
        bigquery.SchemaField("advertiser_has_channel_include", "BOOLEAN"),
        bigquery.SchemaField("missing_reason", "STRING"),
        bigquery.SchemaField("sdf_version", "STRING"),
        bigquery.SchemaField("run_date", "DATE"),
        bigquery.SchemaField("source_advertiser_count", "INTEGER"),
        bigquery.SchemaField("loaded_at", "TIMESTAMP"),
    ]


def ensure_missing_inclusion_table(client: bigquery.Client, project_id: str, dataset: str) -> None:
    table_id = f"{project_id}.{dataset}.{QA_MISSING_INCLUSION_TABLE}"
    try:
        client.get_table(table_id)
        return
    except NotFound:
        pass

    client.create_table(bigquery.Table(table_id, schema=_qa_missing_inclusion_schema()))


def _today_for_sdf() -> date:
    time_zone = get_secret("QA_SDF_TIME_ZONE", DEFAULT_SDF_TIME_ZONE).strip() or DEFAULT_SDF_TIME_ZONE
    return datetime.now(timezone.utc).astimezone(ZoneInfo(time_zone)).date()


def _as_csv_rows(raw_zip: bytes, suffix: str) -> list[dict[str, str]]:
    with ZipFile(BytesIO(raw_zip)) as archive:
        name = next((item for item in archive.namelist() if item.endswith(suffix)), "")
        if not name:
            return []
        text = archive.read(name).decode("utf-8-sig", errors="replace")
    return [dict(row) for row in DictReader(StringIO(text))]


def _parse_sdf_date(value: str) -> date | None:
    text = _clean_cell(value)
    if not text or text.lower() == "same as insertion order":
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _fmt_date(value: date | None) -> str:
    return value.isoformat() if value else ""


def _is_active_status(value: str) -> bool:
    return _clean_cell(value).lower() == "active"


def _parse_io_budget_windows(value: str) -> list[tuple[date | None, date | None]]:
    windows: list[tuple[date | None, date | None]] = []
    for raw_segment in re.findall(r"\(([^)]*)\)", _clean_cell(value)):
        parts = [part.strip() for part in raw_segment.split(";")]
        if len(parts) < 3:
            continue
        start = _parse_sdf_date(parts[1])
        end = _parse_sdf_date(parts[2])
        if start or end:
            windows.append((start, end))
    return windows


def _contains_date(start: date | None, end: date | None, target: date) -> bool:
    if start and target < start:
        return False
    if end and target > end:
        return False
    return True


def _current_io_budget_window(io_row: dict[str, str], run_date: date) -> tuple[date | None, date | None] | None:
    if not _is_active_status(io_row.get("Status", "")):
        return None
    current_windows = [
        window
        for window in _parse_io_budget_windows(io_row.get("Budget Segments", ""))
        if _contains_date(window[0], window[1], run_date)
    ]
    if not current_windows:
        return None
    starts = [window[0] for window in current_windows if window[0]]
    ends = [window[1] for window in current_windows if window[1]]
    return (min(starts) if starts else None, max(ends) if ends else None)


def _uses_io_date(value: str) -> bool:
    return _clean_cell(value).lower() == "same as insertion order"


def _meaningful_targeting(value: str) -> bool:
    text = _clean_cell(value)
    if not text:
        return False
    return text.lower() not in {"none", "not set", "n/a", "na", "[]", "()"}


def _line_item_has_inclusion(line_item: dict[str, str], qa_row: dict[str, str] | None) -> bool:
    values = [
        line_item.get("Channel Targeting - Include", ""),
        line_item.get("Site Targeting - Include", ""),
        line_item.get("App Targeting - Include", ""),
    ]
    if qa_row:
        values.extend(
            [
                qa_row.get("Channel Targeting - Include Qa", ""),
                qa_row.get("Site Targeting - Include Qa", ""),
                qa_row.get("App Targeting - Include Qa", ""),
            ]
        )
    return any(_meaningful_targeting(value) for value in values)


def _sdf_advertiser_ids() -> list[str]:
    raw_ids = get_secret("QA_SDF_ADVERTISER_IDS", "")
    return [item.strip() for item in raw_ids.split(",") if item.strip()]


def _sdf_int_secret(name: str, default: int) -> int:
    return _as_int(get_secret(name, str(default)), default)


def _sdf_bool_secret(name: str, default: bool) -> bool:
    text = get_secret(name, "").strip().lower()
    if not text:
        return default
    return text in {"1", "true", "yes", "y", "on"}


def _chunked(items: list[T], size: int) -> list[list[T]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def _candidate_missing_inclusion_rows_from_sdf_zip(
    partner_id: str,
    advertisers_by_id: dict[str, dict[str, str]],
    raw_zip: bytes,
    run_date: date,
    sdf_version: str,
    source_advertiser_count: int,
    loaded_at: str,
    io_advertiser_ids: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    campaigns: dict[str, str] = {}
    if not io_advertiser_ids:
        campaigns = {
            _clean_cell(row.get("Campaign Id", "")): _clean_cell(row.get("Advertiser Id", ""))
            for row in _as_csv_rows(raw_zip, "SDF-Campaigns.csv")
            if _clean_cell(row.get("Campaign Id", "")) and _clean_cell(row.get("Advertiser Id", ""))
        }
    insertion_orders = {
        _clean_cell(row.get("Io Id", "")): row
        for row in _as_csv_rows(raw_zip, "SDF-InsertionOrders.csv")
        if _clean_cell(row.get("Io Id", ""))
    }
    line_item_qa = {
        _clean_cell(row.get("Line Item Id", "")): row
        for row in _as_csv_rows(raw_zip, "SDF-LineItems-QA.csv")
        if _clean_cell(row.get("Line Item Id", ""))
    }
    line_items = _as_csv_rows(raw_zip, "SDF-LineItems.csv")
    rows: list[dict[str, Any]] = []

    for line_item in line_items:
        if not _is_active_status(line_item.get("Status", "")):
            continue

        io_id = _clean_cell(line_item.get("Io Id", ""))
        io_row = insertion_orders.get(io_id)
        if not io_row:
            continue
        io_window = _current_io_budget_window(io_row, run_date)
        if not io_window:
            continue
        advertiser_id = (io_advertiser_ids or {}).get(io_id, "") or campaigns.get(_clean_cell(io_row.get("Campaign Id", "")), "")
        if not advertiser_id:
            continue
        advertiser = advertisers_by_id.get(advertiser_id, {"advertiser_id": advertiser_id, "advertiser_name": ""})

        li_start_raw = line_item.get("Start Date", "")
        li_end_raw = line_item.get("End Date", "")
        li_start = _parse_sdf_date(li_start_raw)
        li_end = _parse_sdf_date(li_end_raw)
        effective_start = io_window[0] if _uses_io_date(li_start_raw) else li_start
        effective_end = io_window[1] if _uses_io_date(li_end_raw) else li_end
        if not _contains_date(effective_start, effective_end, run_date):
            continue

        qa_row = line_item_qa.get(_clean_cell(line_item.get("Line Item Id", "")))
        if _line_item_has_inclusion(line_item, qa_row):
            continue

        row = {
            "row_number": 0,
            "partner_id": partner_id,
            "advertiser_id": advertiser_id,
            "advertiser_name": advertiser.get("advertiser_name", ""),
            "insertion_order_id": io_id,
            "insertion_order_name": _clean_cell(io_row.get("Name", "")) or _clean_cell(line_item.get("Io Name", "")),
            "insertion_order_status": _clean_cell(io_row.get("Status", "")),
            "io_budget_start_date": _fmt_date(io_window[0]),
            "io_budget_end_date": _fmt_date(io_window[1]),
            "line_item_id": _clean_cell(line_item.get("Line Item Id", "")),
            "line_item_name": _clean_cell(line_item.get("Name", "")),
            "line_item_status": _clean_cell(line_item.get("Status", "")),
            "line_item_type": _clean_cell(line_item.get("Type", "")),
            "line_item_subtype": _clean_cell(line_item.get("Subtype", "")),
            "line_item_start_date": "" if _uses_io_date(li_start_raw) else _fmt_date(li_start),
            "line_item_end_date": "" if _uses_io_date(li_end_raw) else _fmt_date(li_end),
            "effective_start_date": _fmt_date(effective_start),
            "effective_end_date": _fmt_date(effective_end),
            "li_channel_include": _clean_cell(line_item.get("Channel Targeting - Include", "")),
            "li_site_include": _clean_cell(line_item.get("Site Targeting - Include", "")),
            "li_app_include": _clean_cell(line_item.get("App Targeting - Include", "")),
            "li_channel_include_qa": _clean_cell((qa_row or {}).get("Channel Targeting - Include Qa", "")),
            "li_site_include_qa": _clean_cell((qa_row or {}).get("Site Targeting - Include Qa", "")),
            "li_app_include_qa": _clean_cell((qa_row or {}).get("App Targeting - Include Qa", "")),
            "advertiser_channel_include_count": 0,
            "advertiser_has_channel_include": False,
            "missing_reason": "No LI site/app/channel include and no advertiser-level positive channel include",
            "sdf_version": sdf_version,
            "run_date": run_date.isoformat(),
            "source_advertiser_count": source_advertiser_count,
            "loaded_at": loaded_at,
        }
        rows.append(row)
    return rows


def _select_sdf_advertisers(client: DV360Client, partner_id: str) -> list[dict[str, str]]:
    configured_ids = set(_sdf_advertiser_ids())
    if configured_ids:
        advertisers = client.list_advertisers(partner_id, status_filter="")
        by_id = {advertiser["advertiser_id"]: advertiser for advertiser in advertisers}
        return [
            by_id.get(advertiser_id, {"advertiser_id": advertiser_id, "advertiser_name": ""})
            for advertiser_id in sorted(configured_ids)
        ]

    status_filter = get_secret("QA_SDF_ADVERTISER_STATUS_FILTER", 'entityStatus="ENTITY_STATUS_ACTIVE"')
    advertisers = client.list_advertisers(partner_id, status_filter=status_filter)
    limit = _sdf_int_secret("QA_SDF_ADVERTISER_LIMIT", 0)
    if limit > 0:
        advertisers = advertisers[:limit]
    return advertisers


def _current_io_scope(
    client: DV360Client,
    advertisers: list[dict[str, str]],
    run_date: date,
) -> tuple[list[dict[str, str]], dict[str, str], list[str]]:
    advertisers_with_current_budget: list[dict[str, str]] = []
    io_advertiser_ids: dict[str, str] = {}
    errors: list[str] = []
    total = len(advertisers)
    for index, advertiser in enumerate(advertisers, start=1):
        advertiser_id = advertiser["advertiser_id"]
        if index == 1 or index % 25 == 0 or index == total:
            print(f"Checking current IO budgets for advertiser {index}/{total}.", flush=True)
        try:
            current_io_ids = client.list_current_insertion_order_ids(advertiser_id, run_date)
        except Exception as exc:
            errors.append(f"{advertiser_id} current IOs: {exc}")
            continue
        if not current_io_ids:
            continue
        advertisers_with_current_budget.append(advertiser)
        for io_id in current_io_ids:
            io_advertiser_ids[io_id] = advertiser_id
    return advertisers_with_current_budget, io_advertiser_ids, errors


def fetch_missing_inclusion_report() -> dict[str, Any]:
    partner_id = get_secret("DV360_PARTNER_ID", "").strip()
    if not partner_id:
        raise ValueError("DV360_PARTNER_ID is required.")

    sdf_version = get_secret("QA_SDF_VERSION", DEFAULT_SDF_VERSION).strip() or DEFAULT_SDF_VERSION
    run_date = _today_for_sdf()
    loaded_at = datetime.now(timezone.utc).isoformat()
    client = DV360Client(
        client_id=get_secret("DV360_CLIENT_ID"),
        client_secret=get_secret("DV360_CLIENT_SECRET"),
        refresh_token=get_secret("DV360_REFRESH_TOKEN"),
    )
    advertisers = _select_sdf_advertisers(client, partner_id)
    print(f"Missing inclusion QA selected {len(advertisers)} advertisers for partner {partner_id}.", flush=True)
    errors: list[str] = []
    io_advertiser_ids: dict[str, str] = {}
    if _sdf_bool_secret("QA_SDF_FILTER_CURRENT_IO_BUDGETS", True):
        source_advertiser_count = len(advertisers)
        advertisers, io_advertiser_ids, scope_errors = _current_io_scope(client, advertisers, run_date)
        errors.extend(scope_errors)
        print(
            "Missing inclusion QA found "
            f"{len(io_advertiser_ids)} current IOs across {len(advertisers)}/{source_advertiser_count} advertisers.",
            flush=True,
        )

    advertisers_by_id = {advertiser["advertiser_id"]: advertiser for advertiser in advertisers}
    timeout_seconds = _sdf_int_secret("QA_SDF_DOWNLOAD_TIMEOUT_SECONDS", 900)
    poll_seconds = _sdf_int_secret("QA_SDF_POLL_SECONDS", 10)
    batch_size = max(1, _sdf_int_secret("QA_SDF_ADVERTISER_BATCH_SIZE", 25))
    io_batch_size = max(1, _sdf_int_secret("QA_SDF_IO_BATCH_SIZE", 25))

    candidate_rows: list[dict[str, Any]] = []
    if io_advertiser_ids:
        io_ids = sorted(io_advertiser_ids)
        io_batches = _chunked(io_ids, io_batch_size)
        for batch_index, insertion_order_ids in enumerate(io_batches, start=1):
            try:
                print(
                    f"Downloading SDF IO batch {batch_index}/{len(io_batches)} with {len(insertion_order_ids)} insertion orders.",
                    flush=True,
                )
                raw_zip = client.download_insertion_orders_sdf(
                    partner_id=partner_id,
                    insertion_order_ids=insertion_order_ids,
                    file_types=SDF_CURRENT_IO_FILE_TYPES,
                    sdf_version=sdf_version,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                )
                batch_rows = _candidate_missing_inclusion_rows_from_sdf_zip(
                    partner_id=partner_id,
                    advertisers_by_id=advertisers_by_id,
                    raw_zip=raw_zip,
                    run_date=run_date,
                    sdf_version=sdf_version,
                    source_advertiser_count=len(advertisers),
                    loaded_at=loaded_at,
                    io_advertiser_ids=io_advertiser_ids,
                )
                candidate_rows.extend(batch_rows)
                print(f"SDF IO batch {batch_index}/{len(io_batches)} produced {len(batch_rows)} candidate rows.", flush=True)
            except Exception as exc:
                errors.append(f"{','.join(insertion_order_ids)}: {exc}")
                print(f"SDF IO batch {batch_index}/{len(io_batches)} failed: {exc}", flush=True)
    else:
        batches = _chunked(advertisers, batch_size)
        for batch_index, batch in enumerate(batches, start=1):
            advertiser_ids = [advertiser["advertiser_id"] for advertiser in batch]
            try:
                print(
                    f"Downloading SDF advertiser batch {batch_index}/{len(batches)} with {len(advertiser_ids)} advertisers.",
                    flush=True,
                )
                raw_zip = client.download_advertisers_sdf(
                    partner_id=partner_id,
                    advertiser_ids=advertiser_ids,
                    file_types=SDF_FILE_TYPES,
                    sdf_version=sdf_version,
                    timeout_seconds=timeout_seconds,
                    poll_seconds=poll_seconds,
                )
                batch_rows = _candidate_missing_inclusion_rows_from_sdf_zip(
                    partner_id=partner_id,
                    advertisers_by_id=advertisers_by_id,
                    raw_zip=raw_zip,
                    run_date=run_date,
                    sdf_version=sdf_version,
                    source_advertiser_count=len(advertisers),
                    loaded_at=loaded_at,
                )
                candidate_rows.extend(batch_rows)
                print(f"SDF advertiser batch {batch_index}/{len(batches)} produced {len(batch_rows)} candidate rows.", flush=True)
            except Exception as exc:
                errors.append(f"{','.join(advertiser_ids)}: {exc}")
                print(f"SDF advertiser batch {batch_index}/{len(batches)} failed: {exc}", flush=True)

    channel_counts: dict[str, int] = {}
    rows: list[dict[str, Any]] = []
    candidate_advertiser_ids = sorted({str(row["advertiser_id"]) for row in candidate_rows})
    print(
        f"Checking advertiser-level channel targeting for {len(candidate_advertiser_ids)} candidate advertisers.",
        flush=True,
    )
    for advertiser_id in candidate_advertiser_ids:
        try:
            channel_counts[advertiser_id] = client.advertiser_positive_channel_count(advertiser_id)
        except Exception as exc:
            errors.append(f"{advertiser_id} channel targeting: {exc}")
            channel_counts[advertiser_id] = 0

    for row in candidate_rows:
        channel_count = channel_counts.get(str(row["advertiser_id"]), 0)
        if channel_count > 0:
            continue
        row["advertiser_channel_include_count"] = channel_count
        row["advertiser_has_channel_include"] = False
        rows.append(row)
    print(f"Missing inclusion QA final row count: {len(rows)}.", flush=True)

    rows.sort(
        key=lambda row: (
            str(row["advertiser_name"]).lower(),
            str(row["insertion_order_name"]).lower(),
            str(row["line_item_name"]).lower(),
        )
    )
    for index, row in enumerate(rows, start=1):
        row["row_number"] = index

    return {
        "rows": rows,
        "meta": {
            "partner_id": partner_id,
            "sdf_version": sdf_version,
            "run_date": run_date.isoformat(),
            "source_advertiser_count": str(len(advertisers)),
            "errors": "\n".join(errors),
            "loaded_at": loaded_at,
        },
    }


def load_missing_inclusion_table(report: dict[str, Any]) -> dict[str, str | int]:
    client, project_id, dataset = bq_context()
    ensure_missing_inclusion_table(client, project_id, dataset)
    table_id = f"{project_id}.{dataset}.{QA_MISSING_INCLUSION_TABLE}"
    fieldnames = [field.name for field in _qa_missing_inclusion_schema()]
    output = StringIO()
    writer = DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    for row in report.get("rows", []):
        writer.writerow({field: row.get(field, "") for field in fieldnames})

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=_qa_missing_inclusion_schema(),
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    csv_bytes = output.getvalue().encode("utf-8")
    client.load_table_from_file(BytesIO(csv_bytes), table_id, job_config=job_config).result()
    return {
        "project_id": project_id,
        "dataset": dataset,
        "table": QA_MISSING_INCLUSION_TABLE,
        "rows": len(report.get("rows", [])),
        "source_advertiser_count": int(report.get("meta", {}).get("source_advertiser_count", "0") or 0),
    }


def ingest_missing_inclusion_report() -> dict[str, str | int]:
    report = fetch_missing_inclusion_report()
    return load_missing_inclusion_table(report)


def missing_inclusion_dashboard() -> dict[str, Any]:
    client, project_id, dataset = bq_context()
    ensure_missing_inclusion_table(client, project_id, dataset)
    rows = list(
        client.query(
            f"""
SELECT
  row_number,
  partner_id,
  advertiser_id,
  advertiser_name,
  insertion_order_id,
  insertion_order_name,
  insertion_order_status,
  io_budget_start_date,
  io_budget_end_date,
  line_item_id,
  line_item_name,
  line_item_status,
  line_item_type,
  line_item_subtype,
  line_item_start_date,
  line_item_end_date,
  effective_start_date,
  effective_end_date,
  advertiser_channel_include_count,
  missing_reason,
  sdf_version,
  run_date,
  source_advertiser_count,
  loaded_at
FROM {table_fqn(project_id, dataset, QA_MISSING_INCLUSION_TABLE)}
ORDER BY advertiser_name, insertion_order_name, line_item_name
"""
        ).result()
    )
    data = [
        {
            "ROW_ID": f"{r['advertiser_id']}:{r['line_item_id']}",
            "ADVERTISER": str(r["advertiser_name"] or ""),
            "ADVERTISER_ID": str(r["advertiser_id"] or ""),
            "INSERTION_ORDER": str(r["insertion_order_name"] or ""),
            "IO_ID": str(r["insertion_order_id"] or ""),
            "LINE_ITEM": str(r["line_item_name"] or ""),
            "LINE_ITEM_ID": str(r["line_item_id"] or ""),
            "TYPE": str(r["line_item_type"] or ""),
            "SUBTYPE": str(r["line_item_subtype"] or ""),
            "EFFECTIVE_START": r["effective_start_date"].isoformat() if r["effective_start_date"] else "",
            "EFFECTIVE_END": r["effective_end_date"].isoformat() if r["effective_end_date"] else "",
            "IO_BUDGET_START": r["io_budget_start_date"].isoformat() if r["io_budget_start_date"] else "",
            "IO_BUDGET_END": r["io_budget_end_date"].isoformat() if r["io_budget_end_date"] else "",
            "REASON": str(r["missing_reason"] or ""),
            "ADVERTISER_CHANNEL_INCLUDES": int(r["advertiser_channel_include_count"] or 0),
        }
        for r in rows
    ]
    first = rows[0] if rows else None
    return {
        "rows": data,
        "meta": {
            "project_id": project_id,
            "dataset": dataset,
            "table": QA_MISSING_INCLUSION_TABLE,
            "partner_id": str(first["partner_id"] or "") if first else get_secret("DV360_PARTNER_ID", ""),
            "sdf_version": str(first["sdf_version"] or "") if first else get_secret("QA_SDF_VERSION", DEFAULT_SDF_VERSION),
            "run_date": first["run_date"].isoformat() if first and first["run_date"] else "",
            "source_advertiser_count": str(first["source_advertiser_count"] or "") if first else "",
            "loaded_at": first["loaded_at"].isoformat() if first and first["loaded_at"] else "",
            "total_rows": str(len(data)),
        },
    }
