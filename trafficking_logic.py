import re
from datetime import date, datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List, Tuple

import pandas as pd


REQUIRED_TRAFFICKING_COLUMNS = [
    "CampaignName",
    "JobNumber",
    "OurRef",
    "PropertyName",
    "LocationText",
    "SpecificationText",
    "StartDate",
]

FIVE_DIGIT_NUMBER_RE = re.compile(r"(?<!\d)(\d{5})(?!\d)")


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    clean.columns = [str(c).strip() for c in clean.columns]
    unnamed_cols = [c for c in clean.columns if c.lower().startswith("unnamed:")]
    if unnamed_cols:
        clean = clean.drop(columns=unnamed_cols, errors="ignore")
    return clean.fillna("")


def read_table_bytes(filename: str, raw: bytes, skip_top_rows: int) -> pd.DataFrame:
    filename = filename.lower()

    if filename.endswith(".tsv"):
        for enc in ("utf-8", "cp1252", "latin-1"):
            try:
                return pd.read_csv(
                    BytesIO(raw), sep="\t", skiprows=skip_top_rows, engine="python", encoding=enc
                )
            except Exception:
                continue
        raise ValueError("Could not parse TSV file with supported encodings.")

    if filename.endswith(".csv"):
        return pd.read_csv(BytesIO(raw), skiprows=skip_top_rows)

    if filename.endswith(".xls") or filename.endswith(".xlsx"):
        try:
            return pd.read_excel(BytesIO(raw), skiprows=skip_top_rows)
        except Exception:
            for enc in ("utf-8", "cp1252", "latin-1"):
                try:
                    return pd.read_csv(
                        BytesIO(raw),
                        sep="\t",
                        skiprows=skip_top_rows,
                        engine="python",
                        encoding=enc,
                    )
                except Exception:
                    continue
            raise

    raise ValueError("Unsupported file type. Upload .tsv, .csv, .xls, or .xlsx.")


def normalize_campaign_name(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip()
    text = re.sub(r",\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_job_number(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        text = text[:-2]
    return text


def as_due_on(date_value: Any) -> str:
    raw = str(date_value).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        parsed = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    else:
        parsed = pd.to_datetime(raw, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def due_on_to_date(due_on: str) -> date | None:
    if not due_on:
        return None
    try:
        return datetime.strptime(due_on, "%Y-%m-%d").date()
    except ValueError:
        return None


def subtract_weekdays(from_date: date, weekdays: int) -> date:
    d = from_date
    remaining = weekdays
    while remaining > 0:
        d = d - timedelta(days=1)
        if d.weekday() < 5:
            remaining -= 1
    return d


def add_weekdays(from_date: date, weekdays: int) -> date:
    d = from_date
    remaining = weekdays
    while remaining > 0:
        d = d + timedelta(days=1)
        if d.weekday() < 5:
            remaining -= 1
    return d


def build_candidate_rows(trafficking_df: pd.DataFrame) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    candidates: List[Dict[str, str]] = []
    unmatched: List[Dict[str, str]] = []

    seen_keys = set()
    missing_campaign = 0
    missing_job = 0

    for _, row in trafficking_df.iterrows():
        campaign_name = normalize_campaign_name(row.get("CampaignName", ""))
        job_number = normalize_job_number(row.get("JobNumber", ""))

        if not campaign_name:
            missing_campaign += 1
            continue
        if not job_number:
            missing_job += 1
            continue

        key = (campaign_name, job_number)
        if key in seen_keys:
            continue
        seen_keys.add(key)

        candidates.append(
            {
                "campaign_name": campaign_name,
                "job_number": job_number,
                "task_name": f"{campaign_name} ({job_number})",
            }
        )

    if missing_campaign:
        unmatched.append({"item": "Trafficking rows", "reason": f"{missing_campaign} rows missing CampaignName"})
    if missing_job:
        unmatched.append({"item": "Trafficking rows", "reason": f"{missing_job} rows missing JobNumber"})

    return sorted(candidates, key=lambda x: (x["campaign_name"], x["job_number"])), unmatched


def build_subtask_blueprints(trafficking_df: pd.DataFrame) -> Dict[Tuple[str, str], List[Dict[str, str]]]:
    by_campaign_job: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    seen_refs = set()

    for _, row in trafficking_df.iterrows():
        campaign_name = normalize_campaign_name(row.get("CampaignName", ""))
        job_number = normalize_job_number(row.get("JobNumber", ""))
        if not campaign_name or not job_number:
            continue

        our_ref = str(row.get("OurRef", "")).strip()
        if not our_ref:
            continue

        dedupe_key = (campaign_name, job_number, our_ref)
        if dedupe_key in seen_refs:
            continue
        seen_refs.add(dedupe_key)

        property_value = str(row.get("PropertyName", "")).strip()
        location_value = str(row.get("LocationText", "")).strip()
        ad_unit_value = str(row.get("SpecificationText", "")).strip()
        start_date_raw = str(row.get("StartDate", "")).strip()

        subtask_name = f"({our_ref}) {property_value} - {location_value}: {ad_unit_value}".strip()
        by_campaign_job.setdefault((campaign_name, job_number), []).append(
            {
                "our_ref": our_ref,
                "subtask_name": subtask_name,
                "start_date_raw": start_date_raw,
                "subtask_due_on": as_due_on(start_date_raw),
                "subtask_kind": "source",
            }
        )

    for rows in by_campaign_job.values():
        valid_dates = [due_on_to_date(r.get("subtask_due_on", "")) for r in rows]
        valid_dates = [d for d in valid_dates if d is not None]
        if not valid_dates:
            continue

        earliest = min(valid_dates)
        rows.extend(
            [
                {
                    "our_ref": "",
                    "subtask_name": "chase creative",
                    "start_date_raw": earliest.isoformat(),
                    "subtask_due_on": subtract_weekdays(earliest, 4).isoformat(),
                    "subtask_kind": "control",
                },
                {
                    "our_ref": "",
                    "subtask_name": "Check live status",
                    "start_date_raw": earliest.isoformat(),
                    "subtask_due_on": (earliest + timedelta(days=2)).isoformat(),
                    "subtask_kind": "control",
                },
                {
                    "our_ref": "",
                    "subtask_name": "Create and send Dash",
                    "start_date_raw": earliest.isoformat(),
                    "subtask_due_on": add_weekdays(earliest, 3).isoformat(),
                    "subtask_kind": "control_dash",
                },
            ]
        )

    return by_campaign_job


def parent_due_from_blueprints(rows: List[Dict[str, str]]) -> str:
    for row in rows:
        if str(row.get("subtask_name", "")).strip().lower() == "chase creative":
            due = due_on_to_date(row.get("subtask_due_on", ""))
            if due is not None:
                return due.isoformat()

    valid_dates = [due_on_to_date(r.get("subtask_due_on", "")) for r in rows if r.get("subtask_kind") == "source"]
    valid_dates = [d for d in valid_dates if d is not None]
    if not valid_dates:
        return ""
    return min(valid_dates).isoformat()


def build_subtask_rows(
    candidates: List[Dict[str, str]],
    blueprint_map: Dict[Tuple[str, str], List[Dict[str, str]]],
    parent_status_by_job: Dict[str, str],
) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for parent in candidates:
        parent_status = parent_status_by_job.get(parent["job_number"], "would_create")
        key = (parent["campaign_name"], parent["job_number"])
        for sub in blueprint_map.get(key, []):
            out.append(
                {
                    "parent_task_name": parent["task_name"],
                    "parent_job_number": parent["job_number"],
                    "parent_status": parent_status,
                    "our_ref": sub["our_ref"],
                    "subtask_name": sub["subtask_name"],
                    "subtask_due_on": sub["subtask_due_on"],
                    "start_date_raw": sub["start_date_raw"],
                    "subtask_kind": sub.get("subtask_kind", "source"),
                    "subtask_status": "parent_skip_exists" if parent_status == "skip_exists" else "would_create",
                }
            )

    return out


def extract_five_digit_number(value: str) -> str:
    match = FIVE_DIGIT_NUMBER_RE.search(str(value).strip())
    if not match:
        return ""
    return match.group(1).strip()


def find_existing_parent_task(
    project_tasks: List[Dict[str, str]], campaign_name: str, job_number: str
) -> Dict[str, str] | None:
    expected_name = f"{campaign_name} ({job_number})"

    for task in project_tasks:
        if task.get("name", "").strip() == expected_name:
            return task

    for task in project_tasks:
        if job_number and job_number in task.get("name", ""):
            return task

    return None


def existing_subtask_matches(subtask: Dict[str, str], existing_subtasks: List[Dict[str, str]]) -> bool:
    if subtask.get("subtask_kind") == "source":
        source_number = extract_five_digit_number(str(subtask.get("our_ref", "")))
        if not source_number:
            return False
        return any(extract_five_digit_number(existing.get("name", "")) == source_number for existing in existing_subtasks)

    expected_name = str(subtask.get("subtask_name", "")).strip()
    if not expected_name:
        return False
    return any(expected_name == str(existing.get("name", "")).strip() for existing in existing_subtasks)
