# Acquire Ops

React + FastAPI operations dashboard for Acquire trafficking control, alert review, pacing, margin, QA checks, and automation triggers.

Production UI:
- `https://ops.acquire.agency`

## Local Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

Run the API:

```bash
uvicorn api.main:app --reload --port 8000
```

Run the React frontend:

```bash
cd frontend
npm install
npm run dev
```

Open:

```text
http://localhost:5173
```

The Vite dev server proxies `/api/*` requests to `http://127.0.0.1:8000`.

## Production Deploy

Cloud Run deployment is handled by:
- `.github/workflows/deploy_cloud_run.yml`

The container builds the React app, installs the FastAPI runtime, and serves `api.main:app` through `uvicorn`.

Production secrets are loaded from Google Secret Manager into `STREAMLIT_SECRETS_TOML` for compatibility with the existing secret. The application no longer runs Streamlit.

## Alert Digest Email Automation

Workflow file:
- `.github/workflows/campaign_not_live_alert.yml`

Script:
- `scripts/campaign_not_live_alert.py`

The script covers:
- `NOT_LIVE`
- `STOPPED_IMPRESSIONS`
- `MISSING_OUR_REF`
- `ENDED_BUT_IMPRESSIONS`

Behavior:
- Runs at `18:00` UTC daily.
- Script only executes at `06:00` on weekdays in `Pacific/Auckland`, unless `ALERT_FORCE_RUN=true`.
- Stores each run in `supermetrics_data.live_alert_snapshots`.
- Sends one digest email with CSV attachments.
- Email includes one dashboard URL: `https://ops.acquire.agency`.

Required GitHub repository secrets/vars:
- `BQ_PROJECT_ID`
- `BQ_DATASET`
- `BQ_VIEW`
- `BQ_SERVICE_ACCOUNT_JSON`
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN`
- `ALERT_EMAIL_TO`

Optional:
- `GMAIL_USER` defaults to `me`
- `ALERT_EMAIL_SUBJECT`
- `ALERT_FORCE_RUN`

## Daily Trafficking Automation

Workflow file:
- `.github/workflows/daily_trafficking_dry_run.yml`

Script:
- `scripts/daily_trafficking_dry_run.py`

Flow:
1. Pull latest matching Gmail attachment.
2. Parse trafficking file (`.tsv`, `.csv`, `.xls`, `.xlsx`).
3. Build parent/subtask dry-run outputs.
4. Check dedupe in Asana projects using `JobNumber`.
5. Email summary and CSV attachments to `REPORT_EMAIL_TO`.
6. Mark source email as read and add Gmail label `processed`.

Required secrets:
- `ASANA_ACCESS_TOKEN`
- `ASANA_WORKSPACE_GID`
- `ASANA_PROJECT_GID`
- `ASANA_DEDUPE_PROJECT_GIDS`
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN`
- `REPORT_EMAIL_TO`

Optional:
- `GMAIL_USER`
- `GMAIL_SEARCH_QUERY`
- `GMAIL_PROCESSED_LABEL`
- `TRAFFICKING_SKIP_TOP_ROWS`
- `DRY_RUN_MODE`
- `DEFAULT_ASSIGNEE_GID`
- `DASH_ASSIGNEE_GID`

## QA: TradeMe Video Report

Workflow file:
- `.github/workflows/qa_video_on_trademe.yml`

Script:
- `scripts/load_qa_video_on_trademe.py`

Behavior:
- Pulls the latest Gmail attachment where the subject contains `TradeMe On Video - Last 7 Days`.
- Parses the DV360 CSV campaign/impressions rows.
- Overwrites `supermetrics_data.qa_video_on_trademe`.
- Dashboard endpoint `/api/qa/video-on-trademe` reads the table.

## QA: Missing Inclusion List

Workflow file:
- `.github/workflows/qa_missing_inclusion_list.yml`

Script:
- `scripts/load_qa_missing_inclusion_list.py`

Behavior:
- Runs daily at 9 AM America/New_York.
- Downloads DV360 SDF insertion order, line item, and line item QA files for partner advertisers.
- Treats an insertion order as live only when its SDF status is `Active` and a budget segment covers the run date.
- Flags active line items under those live IOs when there is no LI-level channel/site/app include and no advertiser-level positive channel include.
- Overwrites `supermetrics_data.qa_missing_inclusion_list`.
- Dashboard endpoint `/api/qa/missing-inclusion-list` reads the table.

Required app TOML / Secret Manager values:
- `DV360_CLIENT_ID`
- `DV360_CLIENT_SECRET`
- `DV360_REFRESH_TOKEN`
- `DV360_PARTNER_ID`

## Required Trafficking Columns

- `CampaignName`
- `JobNumber`
- `OurRef`
- `PropertyName`
- `LocationText`
- `SpecificationText`
- `StartDate`

## Asana Naming Rules

- Parent task name: `CampaignName (JobNumber)`
- Subtask name: `(OurRef) PropertyName - LocationText: SpecificationText`
- Subtask due date: `StartDate` converted to `YYYY-MM-DD`
- Dedupe check: if any task name in `ASANA_DEDUPE_PROJECT_GIDS` contains `JobNumber`, parent status is `skip_exists`
