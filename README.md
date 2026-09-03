# Trafficking to Asana (Streamlit + Daily Automation)

This project now supports two modes:
- Streamlit app for interactive dry-run checks
- GitHub Actions daily background dry-run (recommended for automation)

## 1) Setup (local)

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## 2) Run Streamlit locally (optional)

```bash
streamlit run app.py
```

Manual trigger UI:
- The app now includes an **Automation Control Panel** at the top.
- Current manual triggers:
  - Alert digest email (`scripts/campaign_not_live_alert.py`)
  - Daily trafficking script (`scripts/daily_trafficking_dry_run.py`)
- This panel is intended as the central place to add more one-click automation triggers over time.

## 3) Daily automation (GitHub Actions)

Workflow file:
- `.github/workflows/daily_trafficking_dry_run.yml`

Schedule:
- Daily at these UTC times:
  - `13:00` (9:00 AM EDT)
  - `18:15` (2:15 PM EDT)
  - `19:00` (3:00 PM EDT)
  - `02:00` (10:00 PM EDT)
  - `08:00` (4:00 AM EDT)
- Can also be run manually via `workflow_dispatch`

### Required GitHub repository secrets

Asana:
- `ASANA_ACCESS_TOKEN`
- `ASANA_WORKSPACE_GID`
- `ASANA_PROJECT_GID`
- `ASANA_DEDUPE_PROJECT_GIDS`

Gmail OAuth (for `hi@ash.gdn`):
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN`

Reporting / behavior:
- `REPORT_EMAIL_TO` (set to `data@acquirenz.com`)
- `GMAIL_SUBJECT_CONTAINS` (set to `Trafficking Report - acquirenz`)
- `DRY_RUN_MODE` (`true` or `false`; recommended start with `true`)
- `DEFAULT_ASSIGNEE_GID` (optional; e.g. Jasper: `1213009182588007`)
- `DASH_ASSIGNEE_GID` (optional; defaults to `DEFAULT_ASSIGNEE_GID`)

### Optional GitHub repository secrets

- `GMAIL_USER` (default `me`)
- `GMAIL_SEARCH_QUERY` (override query; default includes `-label:processed`)
- `GMAIL_PROCESSED_LABEL` (default `processed`)
- `TRAFFICKING_SKIP_TOP_ROWS` (default `0`)

## 3b) Alert digest email automation (BigQuery + Gmail)

This is separate from Asana creation and sends one digest email for open alert rows. Despite the legacy script name, it now covers all alert types: not live, stopped impressions, missing OUR_REF, and ended but impressions.

Workflow file:
- `.github/workflows/campaign_not_live_alert.yml`

Schedule:
- Runs at `18:00` UTC daily.
- Script only executes at `06:00` on weekdays in `Pacific/Auckland` (handles NZDST/NZST).

Script:
- `scripts/campaign_not_live_alert.py`

Logic:
- Group by `OURREF`
- Campaign is flagged if:
  - `STARTDATE < CURRENT_DATE('Pacific/Auckland')`
  - `ENDDATE IS NOT NULL`
  - `ENDDATE >= CURRENT_DATE('Pacific/Auckland')`
  - `BOOKINGSTATUS = 'Booked'` (case-insensitive)
  - `PROPERTYNAME` contains `Programmatic` (case-insensitive)
  - `PROPERTYNAME` does not contain `Adserving` (case-insensitive)
  - `MAX(IMPRESSIONS) = 0` across all rows for that `OURREF`
- Suppressed refs are excluded:
  - active snoozes in `snoozes` table
  - dismissed refs in `snoozes` table
- Alert run rows are snapshot-stored in `live_alert_snapshots` table and used by the dashboard link
- Sends one digest email with CSV attachment fields:
  - `OUR_REF, JOB_NUMBER, START_DATE, END_DATE, ADVERTISER, CAMPAIGN, LOCATIONTEXT, PROPERTYNAME, BOOKINGSTATUS`
- Email includes a signed dashboard link (`mode=live_alerts&user&run_id&exp&sig`)

Required GitHub repository secrets:
- `BQ_PROJECT_ID` (e.g. `sm-test-391201`)
- `BQ_DATASET` (e.g. `supermetrics_data`)
- `BQ_VIEW` (e.g. `master_overview`)
- `BQ_SERVICE_ACCOUNT_JSON` (service account JSON key as one-line JSON string)
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN` (for `hi@acquire.agency`)
- `ALERT_EMAIL_TO` (CSV list, e.g. `ashwin@acquirenz.com,zane@acquirenz.com`)
- `ALERT_DASHBOARD_BASE_URL` (your hosted Streamlit app URL)
- `LINK_SIGNING_SECRET` (shared secret for signed dashboard links)

Optional secrets:
- `GMAIL_USER` (default `me`)
- `ALERT_EMAIL_SUBJECT`
- `ALERT_NEW_UI_URL` (optional; defaults to the hosted Cloud Run React UI)
- `ALERT_FORCE_RUN` (`true` to bypass NZ 6AM weekday guard, useful for manual tests)
- `ALERT_LINK_TTL_DAYS` (default `7`)
- `ADMIN_PASS` (required in Streamlit app for `Dismiss` action)

## 3c) QA: TradeMe video report ingestion (Gmail + BigQuery)

Workflow file:
- `.github/workflows/qa_video_on_trademe.yml`

Schedule:
- Runs daily at 9:00 AM `America/New_York`.
- GitHub Actions triggers at both `13:00` and `14:00` UTC so daylight saving time is handled. The script exits unless New York local time is 9 AM.

Script:
- `scripts/load_qa_video_on_trademe.py`

Logic:
1. Pull the latest Gmail attachment where the subject contains `TradeMe On Video - Last 7 Days`.
2. Parse the DV360 CSV campaign/impressions rows.
3. Fully overwrite `supermetrics_data.qa_video_on_trademe`.
4. Dashboard endpoint `/api/qa/video-on-trademe` reads BigQuery only.

Required GitHub repository secrets:
- `BQ_PROJECT_ID`
- `BQ_DATASET`
- `BQ_SERVICE_ACCOUNT_JSON`
- `GMAIL_CLIENT_ID`
- `GMAIL_CLIENT_SECRET`
- `GMAIL_REFRESH_TOKEN`

Optional vars/secrets:
- `GMAIL_USER` (default `me`)
- `QA_TRADEME_VIDEO_SUBJECT_CONTAINS` (default `TradeMe On Video - Last 7 Days`)
- `QA_TRADEME_VIDEO_GMAIL_SEARCH_QUERY`
- `QA_TRADEME_VIDEO_MAX_MESSAGES` (default `20`)

Local token check helper:
- `scripts/print_gmail_access_token.py`
- Example:
```bash
python3 scripts/print_gmail_access_token.py \
  --client-id "..." \
  --client-secret "..." \
  --refresh-token "..."
```

## 4) Automation behavior

Daily script:
- `scripts/daily_trafficking_dry_run.py`

Flow:
1. Pull latest matching inbox email attachment
2. Parse trafficking file (`.tsv`, `.csv`, `.xls`, `.xlsx`)
3. Build parent/subtask dry-run outputs
4. Check dedupe in Asana projects using `JobNumber`
5. Email summary + CSV attachments to `REPORT_EMAIL_TO`
6. Mark source email as read and add Gmail label `processed`

`DRY_RUN_MODE` behavior:
- `true`: no Asana writes, report only
- `false`: creates parent tasks (for `would_create`) and subtasks under created parents
- Scheduled runs always use the current secret value, so if left `false`, future daily runs are live

## 5) Required Trafficking columns

- `CampaignName`
- `JobNumber`
- `OurRef`
- `PropertyName`
- `LocationText`
- `SpecificationText`
- `StartDate`

## 6) Rules

- Parent task name: `CampaignName (JobNumber)`
- Subtask name: `(OurRef) PropertyName - LocationText: SpecificationText`
- Subtask due date: `StartDate` converted to `YYYY-MM-DD`
- Dedupe check: if any task name in `ASANA_DEDUPE_PROJECT_GIDS` contains `JobNumber`, parent status is `skip_exists`
