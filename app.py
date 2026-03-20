import re
from io import BytesIO
from typing import Any, Dict, List, Tuple

import pandas as pd
import streamlit as st

from asana_client import AsanaClient, AsanaError
from gmail_client import GmailAttachment, GmailError, GmailInboxClient


st.set_page_config(page_title="Trafficking to Asana", page_icon="✅", layout="wide")

GID_RE = re.compile(r"^\d+$")


def _get_secret(name: str, default: str = "") -> str:
    try:
        return str(st.secrets.get(name, default))
    except Exception:
        return default


def _split_csv_secret(value: str) -> List[str]:
    return [v.strip() for v in value.split(",") if v.strip()]


def _as_int_secret(name: str, default: int) -> int:
    raw = _get_secret(name, str(default)).strip()
    try:
        return max(1, int(raw))
    except ValueError:
        return default


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    clean = df.copy()
    clean.columns = [str(c).strip() for c in clean.columns]
    unnamed_cols = [c for c in clean.columns if c.lower().startswith("unnamed:")]
    if unnamed_cols:
        clean = clean.drop(columns=unnamed_cols, errors="ignore")
    return clean.fillna("")


def _read_uploaded_table(uploaded_file: Any, skip_top_rows: int) -> pd.DataFrame:
    filename = str(getattr(uploaded_file, "name", "")).lower()
    raw = uploaded_file.getvalue()

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


class _InMemoryUpload:
    def __init__(self, name: str, content: bytes):
        self.name = name
        self._content = content

    def getvalue(self) -> bytes:
        return self._content


def _normalize_campaign_name(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.strip()
    text = re.sub(r",\s*$", "", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_job_number(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text.endswith(".0") and text.replace(".", "", 1).isdigit():
        text = text[:-2]
    return text


def _as_due_on(date_value: Any) -> str:
    raw = str(date_value).strip()
    if re.match(r"^\\d{4}-\\d{2}-\\d{2}$", raw):
        parsed = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")
    else:
        parsed = pd.to_datetime(raw, dayfirst=True, errors="coerce")
    if pd.isna(parsed):
        return ""
    return parsed.strftime("%Y-%m-%d")


def _build_candidate_rows(trafficking_df: pd.DataFrame) -> Tuple[List[Dict[str, str]], List[Dict[str, str]]]:
    candidates: List[Dict[str, str]] = []
    unmatched: List[Dict[str, str]] = []

    seen_keys = set()
    missing_campaign = 0
    missing_job = 0

    for _, row in trafficking_df.iterrows():
        campaign_name = _normalize_campaign_name(row.get("CampaignName", ""))
        job_number = _normalize_job_number(row.get("JobNumber", ""))

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
        unmatched.append(
            {
                "item": "Trafficking rows",
                "reason": f"{missing_campaign} rows missing CampaignName",
            }
        )
    if missing_job:
        unmatched.append(
            {
                "item": "Trafficking rows",
                "reason": f"{missing_job} rows missing JobNumber",
            }
        )

    return sorted(candidates, key=lambda x: (x["campaign_name"], x["job_number"])), unmatched


def _build_subtask_rows(
    trafficking_df: pd.DataFrame,
    candidates: List[Dict[str, str]],
    parent_status_by_job: Dict[str, str],
) -> List[Dict[str, str]]:
    by_campaign_job: Dict[Tuple[str, str], List[Dict[str, str]]] = {}
    seen_refs = set()

    for _, row in trafficking_df.iterrows():
        campaign_name = _normalize_campaign_name(row.get("CampaignName", ""))
        job_number = _normalize_job_number(row.get("JobNumber", ""))
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
                "subtask_due_on": _as_due_on(start_date_raw),
            }
        )

    out: List[Dict[str, str]] = []
    for parent in candidates:
        parent_status = parent_status_by_job.get(parent["job_number"], "would_create")
        key = (parent["campaign_name"], parent["job_number"])
        for sub in by_campaign_job.get(key, []):
            out.append(
                {
                    "parent_task_name": parent["task_name"],
                    "parent_job_number": parent["job_number"],
                    "parent_status": parent_status,
                    "our_ref": sub["our_ref"],
                    "subtask_name": sub["subtask_name"],
                    "subtask_due_on": sub["subtask_due_on"],
                    "start_date_raw": sub["start_date_raw"],
                    "subtask_status": (
                        "parent_skip_exists" if parent_status == "skip_exists" else "would_create"
                    ),
                }
            )

    return out


def _check_existing_job_numbers(
    client: AsanaClient, dedupe_project_gids: List[str], jobs_to_check: List[str]
) -> Dict[str, bool]:
    existing_task_names: List[str] = []
    for project_gid in dedupe_project_gids:
        existing_task_names.extend(client.list_project_task_names(project_gid))

    existing_by_job: Dict[str, bool] = {}
    for job in jobs_to_check:
        existing_by_job[job] = any(job in name for name in existing_task_names)

    return existing_by_job


def main() -> None:
    st.title("Trafficking to Asana")
    st.caption(
        "Uses Trafficking report only: one parent task per unique CampaignName+JobNumber and one subtask per unique OurRef."
    )

    access_token = _get_secret("ASANA_ACCESS_TOKEN")
    workspace_gid = _get_secret("ASANA_WORKSPACE_GID")
    target_project_gid = _get_secret("ASANA_PROJECT_GID")
    dedupe_project_gids = _split_csv_secret(_get_secret("ASANA_DEDUPE_PROJECT_GIDS"))
    gmail_client_id = _get_secret("GMAIL_CLIENT_ID")
    gmail_client_secret = _get_secret("GMAIL_CLIENT_SECRET")
    gmail_refresh_token = _get_secret("GMAIL_REFRESH_TOKEN")
    gmail_user = _get_secret("GMAIL_USER", "me")
    gmail_subject_contains = _get_secret("GMAIL_SUBJECT_CONTAINS", "Trafficking Report - acquirenz")
    gmail_search_query = _get_secret("GMAIL_SEARCH_QUERY", "")
    max_preview_rows = _as_int_secret("APP_MAX_PREVIEW_ROWS", 30)
    max_candidate_rows = _as_int_secret("APP_MAX_CANDIDATE_ROWS", 25000)
    if not dedupe_project_gids and target_project_gid.strip():
        dedupe_project_gids = [target_project_gid.strip()]

    missing_secrets = []
    if not access_token.strip():
        missing_secrets.append("ASANA_ACCESS_TOKEN")
    if not workspace_gid.strip():
        missing_secrets.append("ASANA_WORKSPACE_GID")
    if not target_project_gid.strip():
        missing_secrets.append("ASANA_PROJECT_GID")

    if missing_secrets:
        st.error(f"Missing required secrets: {', '.join(missing_secrets)}")
        return
    if not GID_RE.match(target_project_gid.strip()):
        st.error("ASANA_PROJECT_GID must contain only digits.")
        return
    invalid_dedupe_gids = [gid for gid in dedupe_project_gids if not GID_RE.match(gid)]
    if invalid_dedupe_gids:
        st.error("Invalid GID(s) in ASANA_DEDUPE_PROJECT_GIDS: " + ", ".join(invalid_dedupe_gids))
        return

    st.subheader("Upload Trafficking Report")
    missing_gmail = []
    if not gmail_client_id.strip():
        missing_gmail.append("GMAIL_CLIENT_ID")
    if not gmail_client_secret.strip():
        missing_gmail.append("GMAIL_CLIENT_SECRET")
    if not gmail_refresh_token.strip():
        missing_gmail.append("GMAIL_REFRESH_TOKEN")
    if missing_gmail:
        st.error("Missing required Gmail secrets: " + ", ".join(missing_gmail))
        return

    fetch_clicked = st.button("Fetch Latest Trafficking Report from Inbox", type="primary")
    if fetch_clicked:
        try:
            inbox_client = GmailInboxClient(
                client_id=gmail_client_id.strip(),
                client_secret=gmail_client_secret.strip(),
                refresh_token=gmail_refresh_token.strip(),
                user_id=gmail_user.strip() or "me",
            )
            attachment = inbox_client.fetch_latest_attachment(
                subject_contains=gmail_subject_contains.strip(),
                allowed_extensions=(".tsv", ".csv", ".xls", ".xlsx"),
                query=gmail_search_query.strip() or None,
            )
            st.session_state["inbox_attachment"] = attachment
            st.success(
                f"Loaded: {attachment.filename} from email '{attachment.subject}' (message {attachment.message_id})."
            )
        except (GmailError, ValueError) as exc:
            st.error(f"Gmail fetch failed: {exc}")
            return
        except Exception as exc:
            st.error(f"Unexpected Gmail error: {exc}")
            return

    attachment: GmailAttachment | None = st.session_state.get("inbox_attachment")
    if not attachment:
        st.info(
            f"Click fetch to pull latest inbox attachment with subject containing '{gmail_subject_contains}'."
        )
        return

    st.caption(
        f"Using inbox file: {attachment.filename} | Subject: {attachment.subject} | Received (UTC): {attachment.received_at}"
    )

    skip_top_rows = st.number_input(
        "Trafficking: skip top rows after inbox fetch",
        min_value=0,
        step=1,
        value=0,
        help="Rows to skip before the header row.",
    )

    try:
        upload_obj = _InMemoryUpload(attachment.filename, attachment.content)
        trafficking_df = _clean_dataframe(_read_uploaded_table(upload_obj, int(skip_top_rows)))
    except Exception as exc:
        st.error(f"Could not read Trafficking Report file: {exc}")
        return

    if trafficking_df.empty:
        st.warning("Trafficking Report file has no rows.")
        return
    if len(trafficking_df.columns) <= 1:
        st.warning("Trafficking parse looks wrong (single column). Adjust skip rows.")
        st.dataframe(trafficking_df.head(20), use_container_width=True)
        return

    required_cols = [
        "CampaignName",
        "JobNumber",
        "OurRef",
        "PropertyName",
        "LocationText",
        "SpecificationText",
        "StartDate",
    ]
    missing_cols = [c for c in required_cols if c not in trafficking_df.columns]
    if missing_cols:
        st.error("Trafficking Report is missing required columns: " + ", ".join(missing_cols))
        return

    st.subheader("Trafficking Report Preview")
    st.dataframe(trafficking_df.head(max_preview_rows), use_container_width=True)

    candidates, unmatched = _build_candidate_rows(trafficking_df)
    if len(candidates) > max_candidate_rows:
        st.error(
            f"Candidate task count ({len(candidates)}) exceeds APP_MAX_CANDIDATE_ROWS={max_candidate_rows}."
        )
        return

    st.subheader("Preflight Summary")
    c1, c2, c3 = st.columns(3)
    c1.metric("Unique Campaign + Job", len(candidates))
    c2.metric("Candidate Tasks", len(candidates))
    c3.metric("Unmatched Items", len(unmatched))

    if unmatched:
        st.write("Unmatched items")
        st.dataframe(pd.DataFrame(unmatched), use_container_width=True)

    if not candidates:
        st.warning("No candidate tasks found from current Trafficking report.")
        return

    st.write("Candidate tasks")
    st.dataframe(pd.DataFrame(candidates), use_container_width=True)
    st.caption(f"Dedupe projects checked: {', '.join(dedupe_project_gids)}")

    if not dedupe_project_gids:
        st.error("No dedupe projects configured. Add ASANA_DEDUPE_PROJECT_GIDS in secrets.")
        return

    check_clicked = st.button("Check Existing + Build Task + Subtask List", type="primary")
    if not check_clicked:
        st.caption(
            "On click: checks dedupe projects for existing job number in task names, then shows parent tasks and subtasks that would be created."
        )
        return

    client = AsanaClient(access_token=access_token.strip())

    job_numbers = sorted({row["job_number"] for row in candidates})
    try:
        existing_by_job = _check_existing_job_numbers(client, dedupe_project_gids, job_numbers)
    except AsanaError as exc:
        st.error(f"Failed while checking existing tasks: {exc}")
        return

    results: List[Dict[str, str]] = []
    progress = st.progress(0)
    total = max(len(candidates), 1)
    for idx, row in enumerate(candidates):
        exists = existing_by_job.get(row["job_number"], False)
        results.append(
            {
                "task_name": row["task_name"],
                "campaign_name": row["campaign_name"],
                "job_number": row["job_number"],
                "status": "skip_exists" if exists else "would_create",
                "reason": (
                    "Found existing task containing job number in dedupe projects"
                    if exists
                    else "No existing task found in dedupe projects"
                ),
                "target_project_gid": target_project_gid.strip(),
            }
        )
        progress.progress((idx + 1) / total)

    st.subheader("Task Output (Dry Run)")
    result_df = pd.DataFrame(results)
    st.dataframe(result_df, use_container_width=True)
    st.download_button(
        "Download Parent Dry Run CSV",
        data=result_df.to_csv(index=False).encode("utf-8"),
        file_name="parent_task_dry_run.csv",
        mime="text/csv",
    )

    parent_status_by_job = {row["job_number"]: row["status"] for row in results}
    subtask_rows = _build_subtask_rows(trafficking_df, candidates, parent_status_by_job)
    st.subheader("Subtask Output (Dry Run)")
    subtask_df = pd.DataFrame(subtask_rows)
    st.dataframe(subtask_df, use_container_width=True)
    st.download_button(
        "Download Subtask Dry Run CSV",
        data=subtask_df.to_csv(index=False).encode("utf-8"),
        file_name="subtask_dry_run.csv",
        mime="text/csv",
    )

    would_create_count = int((result_df["status"] == "would_create").sum()) if not result_df.empty else 0
    skipped_count = int((result_df["status"] == "skip_exists").sum()) if not result_df.empty else 0
    subtask_would_create = (
        int(sum(1 for row in subtask_rows if row["subtask_status"] == "would_create"))
        if subtask_rows
        else 0
    )
    st.success(
        f"Dry run complete. Parent would create: {would_create_count}, parent skipped existing: {skipped_count}, subtask would create: {subtask_would_create}."
    )


if __name__ == "__main__":
    main()
