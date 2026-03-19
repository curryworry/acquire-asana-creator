# Trafficking to Asana (Streamlit)

Simple internal tool to:
- Upload Trafficking report only (`.tsv`, `.csv`, `.xls`, `.xlsx`)
- Build one parent task per unique `CampaignName + JobNumber`
- Build one subtask per unique `OurRef` within each `CampaignName + JobNumber`
- Skip parent task creation when an existing task name already contains that job number
- Output dry-run parent/subtask lists (with CSV download)

## 1) Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

## 2) Run locally

```bash
streamlit run app.py
```

## 3) Configure Asana

Create `.streamlit/secrets.toml`:

```toml
ASANA_ACCESS_TOKEN = "your_token"
ASANA_WORKSPACE_GID = "123456789"
ASANA_PROJECT_GID = "987654321"
ASANA_DEDUPE_PROJECT_GIDS = "987654321,111111111,222222222"
APP_MAX_PREVIEW_ROWS = "30"
APP_MAX_CANDIDATE_ROWS = "25000"
```

## Required Trafficking columns

- `CampaignName`
- `JobNumber`
- `OurRef`
- `PropertyName`
- `LocationText`
- `SpecificationText`
- `StartDate`

## Rules

- Parent task name: `CampaignName (JobNumber)`
- Subtask name: `(OurRef) PropertyName - LocationText: SpecificationText`
- Subtask due date: `StartDate` converted to `YYYY-MM-DD`
- Dedupe check: if any task name in `ASANA_DEDUPE_PROJECT_GIDS` contains `JobNumber`, parent status is `skip_exists`

## Notes

- Use `Trafficking: skip top rows` only if your file has pre-header rows.
- For the provided `.tsv` export, default skip is `0`.
