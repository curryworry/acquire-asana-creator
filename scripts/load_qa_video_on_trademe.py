#!/usr/bin/env python3
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

# Ensure repo root is importable when executed as a script in CI.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.qa_service import ingest_video_on_trademe_report
from gmail_client import GmailError


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def as_bool(value: str, default: bool = False) -> bool:
    raw = (value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def should_run_now_eastern() -> bool:
    now_et = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
    return now_et.hour == 9


def main() -> int:
    if not as_bool(env("QA_TRADEME_VIDEO_FORCE_RUN"), default=False) and not should_run_now_eastern():
        now_et = datetime.now(timezone.utc).astimezone(ZoneInfo("America/New_York"))
        print(f"Skipping TradeMe video QA ingestion. Current New York time is {now_et:%Y-%m-%d %H:%M %Z}.")
        return 0

    result = ingest_video_on_trademe_report()
    print(
        "Loaded TradeMe video QA table: "
        f"{result['project_id']}.{result['dataset']}.{result['table']} "
        f"rows={result['rows']} total_impressions={result['total_impressions']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, GmailError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
