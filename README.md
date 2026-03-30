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

## 3) Daily automation (GitHub Actions)

Workflow file:
- `.github/workflows/daily_trafficking_dry_run.yml`

Schedule:
- Daily at `17:00 UTC` (can be changed in workflow cron)
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

### Optional GitHub repository secrets

- `GMAIL_USER` (default `me`)
- `GMAIL_SEARCH_QUERY` (override query; default includes `-label:processed`)
- `GMAIL_PROCESSED_LABEL` (default `processed`)
- `TRAFFICKING_SKIP_TOP_ROWS` (default `0`)

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
