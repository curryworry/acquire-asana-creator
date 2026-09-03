from __future__ import annotations

import re
from datetime import datetime, timezone
from io import BytesIO
from typing import Any

import pandas as pd

from api.config import get_secret
from gmail_client import GmailAttachment, GmailInboxClient


DEFAULT_TRADEME_VIDEO_SUBJECT = "TradeMe On Video - Last 7 Days"
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


def video_on_trademe_dashboard() -> dict[str, Any]:
    subject_contains = get_secret("QA_TRADEME_VIDEO_SUBJECT_CONTAINS", DEFAULT_TRADEME_VIDEO_SUBJECT)
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
