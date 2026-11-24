#!/usr/bin/env python3
"""Synchronize changed Supermetrics BigQuery data into the Acquire project."""

import argparse
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from supermetrics_sync import env_dry_run, result_json, run_sync


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compare source and destination without copying data.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_sync(dry_run=args.dry_run or env_dry_run())
    print(result_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
