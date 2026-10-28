import hashlib
import json
import re
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Tuple

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

from gmail_client import GmailAttachment
from trafficking_logic import as_due_on, normalize_job_number


DEFAULT_PROJECT_ID = "sm-test-391201"
DEFAULT_DATASET = "supermetrics_data"
DEFAULT_TRAFFICKING_TABLE = "TRAFFICKING_REPORT"
DEFAULT_BUY_ENHANCED_VIEW = "BUY_DATA_ENHANCED"
DEFAULT_MASTER_ENHANCED_VIEW = "MASTER_ENHANCED"

IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9_-]+$")

SOURCE_FIELD_MAP: List[Tuple[str, str, str]] = [
    ("OurRef", "OUR_REF", "STRING"),
    ("JobNumber", "JOB_NUMBER", "STRING"),
    ("Status", "TRAFFICKING_STATUS", "STRING"),
    ("CreativeReceived", "CREATIVE_RECEIVED", "STRING"),
    ("CreativeTrafficked", "CREATIVE_TRAFFICKED", "STRING"),
    ("CampaignComplete", "CAMPAIGN_COMPLETE", "STRING"),
    ("PropertyName", "PROPERTY_NAME", "STRING"),
    ("LocationText", "LOCATION_TEXT", "STRING"),
    ("SpecificationText", "SPECIFICATION_TEXT", "STRING"),
    ("SpecialRequirements", "SPECIAL_REQUIREMENTS", "STRING"),
    ("AdServing", "AD_SERVING", "STRING"),
    ("StartDate", "START_DATE", "DATE"),
    ("EndDate", "END_DATE", "DATE"),
    ("AdvertiserName", "ADVERTISER_NAME", "STRING"),
    ("CampaignName", "CAMPAIGN_NAME", "STRING"),
    ("SalesChannel", "SALES_CHANNEL", "STRING"),
    ("ClientContact", "CLIENT_CONTACT", "STRING"),
    ("ClientEmail", "CLIENT_EMAIL", "STRING"),
    ("NumUnits", "NUM_UNITS", "FLOAT"),
    ("RatingMethod", "RATING_METHOD", "STRING"),
    ("OurCost", "OUR_COST", "FLOAT"),
    ("TraffickingInstructions", "TRAFFICKING_INSTRUCTIONS", "STRING"),
    ("AccountManagerName", "ACCOUNT_MANAGER_NAME", "STRING"),
    ("TraffickerName", "TRAFFICKER_NAME", "STRING"),
]
REQUIRED_TRAFFICKING_BQ_COLUMNS = [source for source, _, _ in SOURCE_FIELD_MAP]

METADATA_SCHEMA = [
    bigquery.SchemaField("SOURCE_MESSAGE_ID", "STRING"),
    bigquery.SchemaField("SOURCE_EMAIL_SUBJECT", "STRING"),
    bigquery.SchemaField("SOURCE_FILENAME", "STRING"),
    bigquery.SchemaField("REPORT_RECEIVED_AT", "TIMESTAMP"),
    bigquery.SchemaField("ROW_HASH", "STRING"),
]

TARGET_AUDIT_SCHEMA = [
    bigquery.SchemaField("FIRST_SEEN_AT", "TIMESTAMP"),
    bigquery.SchemaField("LAST_UPDATED_AT", "TIMESTAMP"),
    bigquery.SchemaField("LAST_SEEN_AT", "TIMESTAMP"),
]


def _schema_type(type_name: str) -> str:
    return "FLOAT64" if type_name == "FLOAT" else type_name


SOURCE_SCHEMA = [
    bigquery.SchemaField(destination, _schema_type(type_name))
    for _, destination, type_name in SOURCE_FIELD_MAP
]
STAGING_SCHEMA = [*SOURCE_SCHEMA, *METADATA_SCHEMA, bigquery.SchemaField("LOAD_RUN_AT", "TIMESTAMP")]
TARGET_SCHEMA = [*SOURCE_SCHEMA, *METADATA_SCHEMA, *TARGET_AUDIT_SCHEMA]


def build_bigquery_client(project_id: str, service_account_json: str = "") -> bigquery.Client:
    if service_account_json.strip():
        info = json.loads(service_account_json)
        credentials = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=project_id or info.get("project_id"), credentials=credentials)
    return bigquery.Client(project=project_id)


def table_fqn(project_id: str, dataset: str, table_name: str) -> str:
    for value in (project_id, dataset, table_name):
        if not IDENTIFIER_RE.match(value):
            raise ValueError(f"Invalid BigQuery identifier: {value}")
    return f"`{project_id}.{dataset}.{table_name}`"


def _text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _number(value: Any) -> float | None:
    raw = _text(value)
    if not raw:
        return None
    normalized = raw.replace(",", "").replace("$", "").strip()
    if normalized.startswith("(") and normalized.endswith(")"):
        normalized = f"-{normalized[1:-1]}"
    try:
        return float(normalized)
    except ValueError:
        return None


def _timestamp(value: str) -> str | None:
    raw = value.strip()
    if not raw:
        return None
    parsed = pd.to_datetime(raw, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.isoformat()


def prepare_trafficking_records(
    trafficking_df: pd.DataFrame,
    attachment: GmailAttachment,
    loaded_at: datetime | None = None,
) -> List[Dict[str, Any]]:
    run_at = (loaded_at or datetime.now(timezone.utc)).astimezone(timezone.utc).isoformat()
    records: List[Dict[str, Any]] = []
    seen_refs: set[str] = set()
    duplicate_refs: set[str] = set()

    for _, row in trafficking_df.iterrows():
        record: Dict[str, Any] = {}
        for source, destination, type_name in SOURCE_FIELD_MAP:
            value = row.get(source, "")
            if destination == "OUR_REF":
                parsed: Any = _text(value)
            elif destination == "JOB_NUMBER":
                parsed = normalize_job_number(value)
            elif type_name == "DATE":
                parsed = as_due_on(value) or None
            elif type_name == "FLOAT":
                parsed = _number(value)
            else:
                parsed = _text(value)
            record[destination] = parsed

        our_ref = record["OUR_REF"]
        if not our_ref:
            raise ValueError("Trafficking report contains a blank OurRef; BigQuery load was stopped.")
        if our_ref in seen_refs:
            duplicate_refs.add(our_ref)
        seen_refs.add(our_ref)

        hash_payload = {field.name: record.get(field.name) for field in SOURCE_SCHEMA}
        record.update(
            {
                "SOURCE_MESSAGE_ID": attachment.message_id,
                "SOURCE_EMAIL_SUBJECT": attachment.subject,
                "SOURCE_FILENAME": attachment.filename,
                "REPORT_RECEIVED_AT": _timestamp(attachment.received_at),
                "ROW_HASH": hashlib.sha256(
                    json.dumps(hash_payload, sort_keys=True, default=str).encode("utf-8")
                ).hexdigest(),
                "LOAD_RUN_AT": run_at,
            }
        )
        records.append(record)

    if duplicate_refs:
        refs = ", ".join(sorted(duplicate_refs))
        raise ValueError(f"Trafficking report contains duplicate OurRef values: {refs}")

    return records


def ensure_trafficking_table(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    table_name: str = DEFAULT_TRAFFICKING_TABLE,
) -> None:
    table_id = f"{project_id}.{dataset}.{table_name}"
    table = bigquery.Table(table_id, schema=TARGET_SCHEMA)
    table.description = "Latest known trafficking report details, upserted by OUR_REF."
    client.create_table(table, exists_ok=True)


def trafficking_merge_sql(target_fqn: str, staging_fqn: str) -> str:
    source_columns = [field.name for field in SOURCE_SCHEMA]
    metadata_columns = [field.name for field in METADATA_SCHEMA]
    mutable_columns = [*source_columns, *metadata_columns]
    assignments = [f"T.{column} = S.{column}" for column in mutable_columns]
    assignments.extend(
        [
            "T.LAST_UPDATED_AT = IF(T.ROW_HASH IS DISTINCT FROM S.ROW_HASH, S.LOAD_RUN_AT, T.LAST_UPDATED_AT)",
            "T.LAST_SEEN_AT = S.LOAD_RUN_AT",
        ]
    )
    insert_columns = [*mutable_columns, "FIRST_SEEN_AT", "LAST_UPDATED_AT", "LAST_SEEN_AT"]
    insert_values = [*[f"S.{column}" for column in mutable_columns], "S.LOAD_RUN_AT", "S.LOAD_RUN_AT", "S.LOAD_RUN_AT"]
    assignment_sql = ",\n    ".join(assignments)
    insert_column_sql = ", ".join(insert_columns)
    insert_value_sql = ", ".join(insert_values)

    return f"""
MERGE {target_fqn} AS T
USING {staging_fqn} AS S
ON T.OUR_REF = S.OUR_REF
WHEN MATCHED THEN
  UPDATE SET
    {assignment_sql}
WHEN NOT MATCHED THEN
  INSERT ({insert_column_sql})
  VALUES ({insert_value_sql})
""".strip()


def upsert_trafficking_records(
    client: bigquery.Client,
    records: Iterable[Dict[str, Any]],
    project_id: str,
    dataset: str,
    table_name: str = DEFAULT_TRAFFICKING_TABLE,
) -> int:
    rows = list(records)
    if not rows:
        raise ValueError("Trafficking report has no rows; BigQuery load was stopped.")

    ensure_trafficking_table(client, project_id, dataset, table_name)
    staging_name = f"_TRAFFICKING_REPORT_STAGE_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}"
    staging_id = f"{project_id}.{dataset}.{staging_name}"
    staging_fqn = table_fqn(project_id, dataset, staging_name)
    target_fqn = table_fqn(project_id, dataset, table_name)

    load_config = bigquery.LoadJobConfig(
        schema=STAGING_SCHEMA,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    try:
        client.load_table_from_json(rows, staging_id, job_config=load_config).result()
        client.query(trafficking_merge_sql(target_fqn, staging_fqn)).result()
    finally:
        client.delete_table(staging_id, not_found_ok=True)

    return len(rows)


def buy_data_enhanced_sql(
    project_id: str,
    dataset: str,
    trafficking_table: str = DEFAULT_TRAFFICKING_TABLE,
    view_name: str = DEFAULT_BUY_ENHANCED_VIEW,
) -> str:
    buy_fqn = table_fqn(project_id, dataset, "B27F8_BUY_DATA")
    traffic_fqn = table_fqn(project_id, dataset, trafficking_table)
    view_fqn = table_fqn(project_id, dataset, view_name)
    return f"""
CREATE OR REPLACE VIEW {view_fqn} AS
WITH buy_ranked AS (
  SELECT
    b.*,
    COUNT(*) OVER (
      PARTITION BY TRIM(CAST(OURREF AS STRING)), CAST(JOBNUMBER AS STRING), STARTDATE, ENDDATE
    ) AS source_row_count,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(CAST(OURREF AS STRING)), CAST(JOBNUMBER AS STRING), STARTDATE, ENDDATE
      ORDER BY
        CASE LOWER(TRIM(COALESCE(BOOKINGSTATUS, '')))
          WHEN 'booked' THEN 0
          WHEN 'cancelled' THEN 1
          ELSE 2
        END,
        TO_JSON_STRING(b) DESC
    ) AS source_row_rank
  FROM {buy_fqn} AS b
  WHERE OURREF IS NOT NULL
    AND TRIM(CAST(OURREF AS STRING)) != ''
),
buy_deduped AS (
  SELECT * EXCEPT(source_row_rank)
  FROM buy_ranked
  WHERE source_row_rank = 1
)
SELECT
  CONCAT(
    TRIM(CAST(b.OURREF AS STRING)), '|',
    CAST(b.JOBNUMBER AS STRING), '|',
    COALESCE(CAST(b.STARTDATE AS STRING), ''), '|',
    COALESCE(CAST(b.ENDDATE AS STRING), '')
  ) AS BUY_ROW_KEY,
  TRIM(CAST(b.OURREF AS STRING)) AS OUR_REF,
  CAST(b.JOBNUMBER AS STRING) AS JOB_NUMBER,
  b.DATASOURCENAME AS DATASOURCE_NAME,
  b.T_GOAL_TYPE_REPORTING_V2 AS GOAL_TYPE_REPORTING,
  b.ACCOUNTMANAGERNAME AS ACCOUNT_MANAGER_NAME,
  b.ADVERTISERCATEGORY AS ADVERTISER_CATEGORY,
  b.ADVERTISERNAME AS ADVERTISER_NAME,
  b.BOOKINGSTATUS AS BOOKING_STATUS,
  b.CAMPAIGNLEAD AS CAMPAIGN_LEAD,
  b.CAMPAIGNNAME AS CAMPAIGN_NAME,
  b.ENDDATE AS END_DATE,
  b.LOCATIONTEXT AS LOCATION_TEXT,
  b.PROPERTYNAME AS PROPERTY_NAME,
  b.RATINGMETHOD AS RATING_METHOD,
  b.SALESCHANNEL AS SALES_CHANNEL,
  b.STARTDATE AS START_DATE,
  b.STRATEGY,
  b.TRAFFICKERNAME AS TRAFFICKER_NAME,
  b.T_IO_MARGIN_RATE AS IO_MARGIN_RATE,
  b.T_NET_SPEND AS NET_SPEND,
  b.ACTUALPRICE AS ACTUAL_PRICE,
  b.NETBILLABLEAMOUNT AS NET_BILLABLE_AMOUNT,
  b.NUMUNITS AS NUM_UNITS,
  COALESCE(b.OURCOST, t.OUR_COST) AS OUR_COST,
  b.source_row_count AS BUY_SOURCE_ROW_COUNT,
  b.source_row_count > 1 AS BUY_SOURCE_CONFLICT,
  t.OUR_REF IS NOT NULL AS TRAFFICKING_MATCHED,
  t.TRAFFICKING_STATUS,
  t.CREATIVE_RECEIVED,
  t.CREATIVE_TRAFFICKED,
  t.CAMPAIGN_COMPLETE,
  t.SPECIFICATION_TEXT,
  t.SPECIAL_REQUIREMENTS,
  t.AD_SERVING,
  t.CLIENT_CONTACT,
  t.CLIENT_EMAIL,
  t.TRAFFICKING_INSTRUCTIONS,
  t.SOURCE_FILENAME AS TRAFFICKING_SOURCE_FILENAME,
  t.REPORT_RECEIVED_AT AS TRAFFICKING_REPORT_RECEIVED_AT,
  t.LAST_UPDATED_AT AS TRAFFICKING_LAST_UPDATED_AT,
  t.LAST_SEEN_AT AS TRAFFICKING_LAST_SEEN_AT
FROM buy_deduped AS b
LEFT JOIN {traffic_fqn} AS t
  ON TRIM(CAST(b.OURREF AS STRING)) = t.OUR_REF
""".strip()


def master_enhanced_sql(
    project_id: str,
    dataset: str,
    buy_view: str = DEFAULT_BUY_ENHANCED_VIEW,
    view_name: str = DEFAULT_MASTER_ENHANCED_VIEW,
) -> str:
    buy_fqn = table_fqn(project_id, dataset, buy_view)
    delivery_fqn = table_fqn(project_id, dataset, "BLEND_BLEND_5_1_2")
    view_fqn = table_fqn(project_id, dataset, view_name)
    return f"""
CREATE OR REPLACE VIEW {view_fqn} AS
WITH delivery_rollup AS (
  SELECT
    TRIM(CAST(OUR_REF AS STRING)) AS OUR_REF,
    DATE AS DELIVERY_DATE,
    DATASOURCENAME AS DELIVERY_DATASOURCE,
    SUM(COALESCE(CLICKS, 0)) AS CLICKS,
    SUM(COALESCE(IMPRESSIONS, 0)) AS IMPRESSIONS,
    SUM(COALESCE(COST, 0)) AS COST,
    SUM(COALESCE(VIDEO_PLAYS, 0)) AS VIDEO_PLAYS,
    SUM(COALESCE(VIDEO_COMPLETIONS, 0)) AS VIDEO_COMPLETIONS,
    SUM(COALESCE(SPOTS, 0)) AS SPOTS,
    SUM(COALESCE(LINK_CLICKS, 0)) AS LINK_CLICKS,
    SUM(COALESCE(MEASURABLE_IMPRESSIONS_ACTIVE_VIEW, 0)) AS MEASURABLE_IMPRESSIONS_ACTIVE_VIEW,
    SUM(COALESCE(VIEWABLE_IMPRESSIONS_ACTIVE_VIEW, 0)) AS VIEWABLE_IMPRESSIONS_ACTIVE_VIEW
  FROM {delivery_fqn}
  WHERE OUR_REF IS NOT NULL
    AND TRIM(CAST(OUR_REF AS STRING)) != ''
  GROUP BY 1, 2, 3
)
SELECT
  b.*,
  d.DELIVERY_DATE,
  d.DELIVERY_DATASOURCE,
  COALESCE(d.CLICKS, 0) AS CLICKS,
  COALESCE(d.IMPRESSIONS, 0) AS IMPRESSIONS,
  COALESCE(d.COST, 0) AS COST,
  COALESCE(d.VIDEO_PLAYS, 0) AS VIDEO_PLAYS,
  COALESCE(d.VIDEO_COMPLETIONS, 0) AS VIDEO_COMPLETIONS,
  COALESCE(d.SPOTS, 0) AS SPOTS,
  COALESCE(d.LINK_CLICKS, 0) AS LINK_CLICKS,
  COALESCE(d.MEASURABLE_IMPRESSIONS_ACTIVE_VIEW, 0) AS MEASURABLE_IMPRESSIONS_ACTIVE_VIEW,
  COALESCE(d.VIEWABLE_IMPRESSIONS_ACTIVE_VIEW, 0) AS VIEWABLE_IMPRESSIONS_ACTIVE_VIEW
FROM {buy_fqn} AS b
LEFT JOIN delivery_rollup AS d
  ON b.OUR_REF = d.OUR_REF
""".strip()


def ensure_enhanced_views(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    trafficking_table: str = DEFAULT_TRAFFICKING_TABLE,
    buy_view: str = DEFAULT_BUY_ENHANCED_VIEW,
    master_view: str = DEFAULT_MASTER_ENHANCED_VIEW,
) -> None:
    client.query(
        buy_data_enhanced_sql(project_id, dataset, trafficking_table, buy_view)
    ).result()
    client.query(master_enhanced_sql(project_id, dataset, buy_view, master_view)).result()
