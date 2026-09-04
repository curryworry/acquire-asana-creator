import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

from api.config import REPO_ROOT, get_secret


ADMIN_USERNAME = "ashwin@acquirenz.com"
SCRIPT_TIMEOUT_SECONDS = 1800


def is_admin_user(user: dict[str, str]) -> bool:
    return str(user.get("username", "")).strip().lower() == ADMIN_USERNAME


def _secret_env(names: list[str]) -> dict[str, str]:
    return {name: get_secret(name, "") for name in names}


def run_script(script_rel_path: str, env_overrides: dict[str, str]) -> dict[str, str | int | bool]:
    started_at = datetime.now(timezone.utc)
    run_env = os.environ.copy()
    run_env.update({key: value for key, value in env_overrides.items() if value is not None})

    try:
        result = subprocess.run(
            [sys.executable, script_rel_path],
            cwd=str(REPO_ROOT),
            env=run_env,
            capture_output=True,
            text=True,
            timeout=SCRIPT_TIMEOUT_SECONDS,
        )
        output = (result.stdout or "") + ("\n" + result.stderr if result.stderr else "")
        return {
            "status": "ok" if result.returncode == 0 else "failed",
            "exit_code": result.returncode,
            "output": output.strip(),
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + ("\n" + exc.stderr if exc.stderr else "")
        return {
            "status": "timeout",
            "exit_code": 124,
            "output": str(output).strip() or f"Timed out after {SCRIPT_TIMEOUT_SECONDS} seconds.",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.now(timezone.utc).isoformat(),
        }


def campaign_alert_defaults() -> dict[str, str]:
    return {
        "recipients": get_secret("ALERT_EMAIL_TO", ""),
        "subject": get_secret("ALERT_EMAIL_SUBJECT", ""),
        "script": str(Path("scripts") / "campaign_not_live_alert.py"),
    }


def daily_trafficking_defaults() -> dict[str, str]:
    return {
        "report_email_to": get_secret("REPORT_EMAIL_TO", "ashwin@acquirenz.com"),
        "dry_run_mode": get_secret("DRY_RUN_MODE", "true"),
        "script": str(Path("scripts") / "daily_trafficking_dry_run.py"),
    }


def qa_video_on_trademe_defaults() -> dict[str, str]:
    return {
        "subject_contains": get_secret("QA_TRADEME_VIDEO_SUBJECT_CONTAINS", "TradeMe On Video - Last 7 Days"),
        "table": "qa_video_on_trademe",
        "script": str(Path("scripts") / "load_qa_video_on_trademe.py"),
    }


def qa_missing_inclusion_defaults() -> dict[str, str]:
    return {
        "partner_id": get_secret("DV360_PARTNER_ID", ""),
        "sdf_version": get_secret("QA_SDF_VERSION", "SDF_VERSION_10_1"),
        "table": "qa_missing_inclusion_list",
        "script": str(Path("scripts") / "load_qa_missing_inclusion_list.py"),
    }


def run_campaign_alert(recipients: str, force_run: bool) -> dict[str, str | int | bool]:
    env = {
        **_secret_env([
            "BQ_SERVICE_ACCOUNT_JSON",
            "GMAIL_CLIENT_ID",
            "GMAIL_CLIENT_SECRET",
            "GMAIL_REFRESH_TOKEN",
        ]),
        "BQ_PROJECT_ID": get_secret("BQ_PROJECT_ID", "sm-test-391201"),
        "BQ_DATASET": get_secret("BQ_DATASET", "supermetrics_data"),
        "BQ_VIEW": get_secret("BQ_VIEW", "master_overview"),
        "GMAIL_USER": get_secret("GMAIL_USER", "me"),
        "ALERT_EMAIL_TO": recipients,
        "ALERT_EMAIL_SUBJECT": get_secret("ALERT_EMAIL_SUBJECT", ""),
        "ALERT_FORCE_RUN": "true" if force_run else "false",
    }
    return run_script("scripts/campaign_not_live_alert.py", env)


def run_daily_trafficking(force_dry_run: bool) -> dict[str, str | int | bool]:
    env = {
        **_secret_env([
            "ASANA_ACCESS_TOKEN",
            "ASANA_WORKSPACE_GID",
            "ASANA_PROJECT_GID",
            "ASANA_DEDUPE_PROJECT_GIDS",
            "GMAIL_CLIENT_ID",
            "GMAIL_CLIENT_SECRET",
            "GMAIL_REFRESH_TOKEN",
            "GMAIL_SUBJECT_CONTAINS",
            "GMAIL_SEARCH_QUERY",
            "GMAIL_PROCESSED_LABEL",
            "DEFAULT_ASSIGNEE_GID",
            "DASH_ASSIGNEE_GID",
        ]),
        "GMAIL_USER": get_secret("GMAIL_USER", "me"),
        "TRAFFICKING_SKIP_TOP_ROWS": get_secret("TRAFFICKING_SKIP_TOP_ROWS", "0"),
        "REPORT_EMAIL_TO": get_secret("REPORT_EMAIL_TO", "ashwin@acquirenz.com"),
        "DRY_RUN_MODE": "true" if force_dry_run else get_secret("DRY_RUN_MODE", "true"),
    }
    return run_script("scripts/daily_trafficking_dry_run.py", env)


def run_qa_video_on_trademe() -> dict[str, str | int | bool]:
    env = {
        **_secret_env([
            "BQ_SERVICE_ACCOUNT_JSON",
            "GMAIL_CLIENT_ID",
            "GMAIL_CLIENT_SECRET",
            "GMAIL_REFRESH_TOKEN",
            "QA_TRADEME_VIDEO_SUBJECT_CONTAINS",
            "QA_TRADEME_VIDEO_GMAIL_SEARCH_QUERY",
            "QA_TRADEME_VIDEO_MAX_MESSAGES",
        ]),
        "BQ_PROJECT_ID": get_secret("BQ_PROJECT_ID", "sm-test-391201"),
        "BQ_DATASET": get_secret("BQ_DATASET", "supermetrics_data"),
        "GMAIL_USER": get_secret("GMAIL_USER", "me"),
        "QA_TRADEME_VIDEO_FORCE_RUN": "true",
    }
    return run_script("scripts/load_qa_video_on_trademe.py", env)


def run_qa_missing_inclusion() -> dict[str, str | int | bool]:
    env = {
        **_secret_env([
            "BQ_SERVICE_ACCOUNT_JSON",
            "DV360_CLIENT_ID",
            "DV360_CLIENT_SECRET",
            "DV360_REFRESH_TOKEN",
            "DV360_PARTNER_ID",
            "QA_SDF_VERSION",
            "QA_SDF_TIME_ZONE",
            "QA_SDF_ADVERTISER_IDS",
            "QA_SDF_ADVERTISER_STATUS_FILTER",
            "QA_SDF_ADVERTISER_LIMIT",
            "QA_SDF_DOWNLOAD_TIMEOUT_SECONDS",
            "QA_SDF_POLL_SECONDS",
        ]),
        "BQ_PROJECT_ID": get_secret("BQ_PROJECT_ID", "sm-test-391201"),
        "BQ_DATASET": get_secret("BQ_DATASET", "supermetrics_data"),
        "QA_SDF_FORCE_RUN": "true",
    }
    return run_script("scripts/load_qa_missing_inclusion_list.py", env)
