# Trafficking + Billing to Asana (Streamlit)

Simple internal tool to:
- Upload Trafficking Report and Billing Report (`.csv`, `.xls`, `.xlsx`)
- Match Trafficking campaigns to Billing campaigns/jobs
- Skip creation when an existing task already contains the job number
- Build dry-run outputs for parent tasks and subtasks

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
```

Matching rules:
- Trafficking campaign name comes from `Trafficking Report -> Campaign` (trailing comma removed).
- Billing campaign must match: `Advertiser: Campaign Name (job 1234)`.
- Match key is `Campaign Name` (text after first `:` and before `(job ...)`).
- One parent task is generated per matched job number.
- For each parent task, subtasks are generated from Trafficking rows in the same campaign:
  - Name: `(Our Ref) Property - Location: Ad Unit`
  - Due date: `Start Date` from Trafficking (converted to `YYYY-MM-DD`)
- Before create, tool scans all `ASANA_DEDUPE_PROJECT_GIDS`; if any task name contains the job number, creation is skipped.

## Notes

- Use skip controls if files contain title/blank rows before headers.
- Default skip values for provided reports: Trafficking `3`, Billing `2`.
- Results are shown per task: `created`, `skipped_exists`, or `error`.
