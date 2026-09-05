#!/usr/bin/env python3
import os
import sys
from pathlib import Path

# Ensure repo root is importable when executed as a script in CI.
REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from api.qa_service import fetch_missing_inclusion_report, ingest_missing_inclusion_report
from dv360_client import DV360Error


def env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


def as_bool(value: str, default: bool = False) -> bool:
    raw = (value or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def main() -> int:
    if as_bool(env("QA_SDF_DRY_RUN"), default=False):
        report = fetch_missing_inclusion_report()
        meta = report["meta"]
        print(
            "Built missing inclusion QA report without loading BigQuery: "
            f"rows={len(report['rows'])} advertisers={meta['source_advertiser_count']} run_date={meta['run_date']}"
        )
        if meta.get("errors"):
            print(f"Advertiser errors:\n{meta['errors']}", file=sys.stderr)
        return 0

    result = ingest_missing_inclusion_report()
    print(
        "Loaded missing inclusion QA table: "
        f"{result['project_id']}.{result['dataset']}.{result['table']} "
        f"rows={result['rows']} advertisers={result['source_advertiser_count']}"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, ValueError, DV360Error) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1)
