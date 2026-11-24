from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Iterable

from google.cloud import bigquery
from google.oauth2.credentials import Credentials

from api.config import get_secret


BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"
TOKEN_URI = "https://oauth2.googleapis.com/token"


@dataclass(frozen=True)
class SyncConfig:
    billing_project: str
    source_project: str
    source_dataset: str
    destination_project: str
    destination_dataset: str
    source_location: str
    destination_location: str
    delivery_table: str
    buy_table: str
    max_partition_copies: int

    @property
    def source_delivery(self) -> str:
        return f"{self.source_project}.{self.source_dataset}.{self.delivery_table}"

    @property
    def destination_delivery(self) -> str:
        return f"{self.destination_project}.{self.destination_dataset}.{self.delivery_table}"

    @property
    def source_buy(self) -> str:
        return f"{self.source_project}.{self.source_dataset}.{self.buy_table}"

    @property
    def destination_buy(self) -> str:
        return f"{self.destination_project}.{self.destination_dataset}.{self.buy_table}"


@dataclass(frozen=True)
class Fingerprint:
    row_count: int
    checksum: int


def _required_secret(name: str) -> str:
    value = get_secret(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing required secret: {name}")
    return value


def load_config() -> SyncConfig:
    destination_project = get_secret("BQ_PROJECT_ID", "sm-test-391201").strip()
    destination_dataset = get_secret("BQ_DATASET", "supermetrics_data").strip()
    return SyncConfig(
        billing_project=get_secret("BQ_SYNC_BILLING_PROJECT", destination_project).strip(),
        source_project=get_secret("BQ_SYNC_SOURCE_PROJECT", "sm-storage-628074").strip(),
        source_dataset=get_secret("BQ_SYNC_SOURCE_DATASET", "Supermetrics").strip(),
        destination_project=destination_project,
        destination_dataset=destination_dataset,
        source_location=get_secret("BQ_SYNC_SOURCE_LOCATION", "europe-west1").strip(),
        destination_location=get_secret("BQ_SYNC_DESTINATION_LOCATION", "US").strip(),
        delivery_table=get_secret("BQ_SYNC_DELIVERY_TABLE", "BLEND_BLEND_5_1_2").strip(),
        buy_table=get_secret("BQ_SYNC_BUY_TABLE", "B27F8_BUY_DATA").strip(),
        max_partition_copies=int(get_secret("BQ_SYNC_MAX_PARTITIONS", "100")),
    )


def oauth_credentials() -> Credentials:
    return Credentials(
        token=None,
        refresh_token=_required_secret("BQ_USER_REFRESH_TOKEN"),
        token_uri=TOKEN_URI,
        client_id=_required_secret("BQ_USER_CLIENT_ID"),
        client_secret=_required_secret("BQ_USER_CLIENT_SECRET"),
        scopes=[BIGQUERY_SCOPE],
    )


def bigquery_client(config: SyncConfig) -> bigquery.Client:
    return bigquery.Client(
        project=config.billing_project,
        credentials=oauth_credentials(),
    )


def _schema_signature(table: bigquery.Table) -> list[tuple[str, str, str]]:
    return [(field.name, field.field_type, field.mode) for field in table.schema]


def validate_compatible_schemas(
    client: bigquery.Client,
    source_table: str,
    destination_table: str,
) -> None:
    source = client.get_table(source_table)
    destination = client.get_table(destination_table)
    if _schema_signature(source) != _schema_signature(destination):
        raise RuntimeError(
            f"Schema mismatch between {source_table} and {destination_table}. "
            "Review the Supermetrics schema change before syncing."
        )


def destination_min_date(
    client: bigquery.Client,
    table: str,
    location: str,
) -> date:
    row = next(
        iter(
            client.query(
                f"SELECT MIN(DATE) AS min_date FROM `{table}` WHERE DATE IS NOT NULL",
                location=location,
            ).result()
        )
    )
    if row.min_date is None:
        raise RuntimeError(f"Destination delivery table is empty: {table}")
    return row.min_date


def partition_fingerprints(
    client: bigquery.Client,
    table: str,
    location: str,
    minimum_date: date,
    dates: Iterable[date] | None = None,
) -> dict[date, Fingerprint]:
    selected_dates = list(dates or [])
    predicates = ["DATE IS NOT NULL", "DATE >= @minimum_date"]
    parameters: list[bigquery.QueryParameter] = [
        bigquery.ScalarQueryParameter("minimum_date", "DATE", minimum_date)
    ]
    if selected_dates:
        predicates.append("DATE IN UNNEST(@selected_dates)")
        parameters.append(bigquery.ArrayQueryParameter("selected_dates", "DATE", selected_dates))

    query = f"""
SELECT
  DATE AS partition_date,
  COUNT(*) AS row_count,
  BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS checksum
FROM `{table}` AS t
WHERE {' AND '.join(predicates)}
GROUP BY partition_date
ORDER BY partition_date
"""
    job_config = bigquery.QueryJobConfig(query_parameters=parameters)
    rows = client.query(query, location=location, job_config=job_config).result()
    return {
        row.partition_date: Fingerprint(
            row_count=int(row.row_count),
            checksum=int(row.checksum),
        )
        for row in rows
    }


def table_fingerprint(
    client: bigquery.Client,
    table: str,
    location: str,
) -> Fingerprint:
    query = f"""
SELECT
  COUNT(*) AS row_count,
  BIT_XOR(FARM_FINGERPRINT(TO_JSON_STRING(t))) AS checksum
FROM `{table}` AS t
"""
    row = next(iter(client.query(query, location=location).result()))
    return Fingerprint(
        row_count=int(row.row_count),
        checksum=int(row.checksum or 0),
    )


def changed_partitions(
    source: dict[date, Fingerprint],
    destination: dict[date, Fingerprint],
) -> list[date]:
    return sorted(
        partition_date
        for partition_date, source_fingerprint in source.items()
        if destination.get(partition_date) != source_fingerprint
    )


def _query_to_json_file(
    client: bigquery.Client,
    query: str,
    location: str,
    query_parameters: list[bigquery.QueryParameter] | None = None,
) -> tuple[object, str, int]:
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters or [])
    query_job = client.query(query, location=location, job_config=job_config)
    rows = query_job.result()
    output = tempfile.TemporaryFile(mode="w+b")
    row_count = 0
    for row in rows:
        output.write(row.row_json.encode("utf-8"))
        output.write(b"\n")
        row_count += 1
    output.seek(0)
    return output, query_job.job_id, row_count


def _load_json_file(
    client: bigquery.Client,
    file_object: object,
    destination_table: str,
    destination_schema: list[bigquery.SchemaField],
    location: str,
    table_label: str,
) -> str:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=destination_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        labels={"automation": "supermetrics-sync", "table": table_label},
    )
    job = client.load_table_from_file(
        file_object,
        destination_table,
        location=location,
        job_config=job_config,
        rewind=True,
    )
    job.result(timeout=1800)
    return job.job_id


def transfer_delivery_partitions(
    client: bigquery.Client,
    config: SyncConfig,
    partitions: list[date],
) -> tuple[list[str], list[str]]:
    if len(partitions) > config.max_partition_copies:
        raise RuntimeError(
            f"Refusing to transfer {len(partitions)} partitions; maximum is "
            f"{config.max_partition_copies}."
        )

    destination_schema = client.get_table(config.destination_delivery).schema
    query_job_ids: list[str] = []
    load_job_ids: list[str] = []
    for partition_date in partitions:
        decorator = partition_date.strftime("%Y%m%d")
        query = f"""
SELECT TO_JSON_STRING(t) AS row_json
FROM `{config.source_delivery}` AS t
WHERE DATE = @partition_date
"""
        json_file, query_job_id, row_count = _query_to_json_file(
            client,
            query,
            config.source_location,
            [bigquery.ScalarQueryParameter("partition_date", "DATE", partition_date)],
        )
        if row_count == 0:
            json_file.close()
            raise RuntimeError(f"Source partition unexpectedly returned no rows: {partition_date}")
        try:
            load_job_ids.append(
                _load_json_file(
                    client,
                    json_file,
                    f"{config.destination_delivery}${decorator}",
                    destination_schema,
                    config.destination_location,
                    "delivery",
                )
            )
        finally:
            json_file.close()
        query_job_ids.append(query_job_id)
    return query_job_ids, load_job_ids


def transfer_buy_table(client: bigquery.Client, config: SyncConfig) -> tuple[str, str]:
    query = f"""
SELECT TO_JSON_STRING(t) AS row_json
FROM `{config.source_buy}` AS t
"""
    json_file, query_job_id, row_count = _query_to_json_file(
        client,
        query,
        config.source_location,
    )
    if row_count == 0:
        json_file.close()
        raise RuntimeError("Source Buy Data table unexpectedly returned no rows.")
    try:
        load_job_id = _load_json_file(
            client,
            json_file,
            config.destination_buy,
            client.get_table(config.destination_buy).schema,
            config.destination_location,
            "buy-data",
        )
    finally:
        json_file.close()
    return query_job_id, load_job_id


def run_sync(*, dry_run: bool = False) -> dict[str, object]:
    started_at = datetime.now(timezone.utc)
    config = load_config()
    client = bigquery_client(config)

    validate_compatible_schemas(client, config.source_delivery, config.destination_delivery)
    validate_compatible_schemas(client, config.source_buy, config.destination_buy)

    minimum_date = destination_min_date(
        client,
        config.destination_delivery,
        config.destination_location,
    )
    source_delivery = partition_fingerprints(
        client,
        config.source_delivery,
        config.source_location,
        minimum_date,
    )
    destination_delivery = partition_fingerprints(
        client,
        config.destination_delivery,
        config.destination_location,
        minimum_date,
    )
    partitions = changed_partitions(source_delivery, destination_delivery)

    source_buy = table_fingerprint(client, config.source_buy, config.source_location)
    destination_buy = table_fingerprint(
        client,
        config.destination_buy,
        config.destination_location,
    )
    buy_changed = source_buy != destination_buy

    result: dict[str, object] = {
        "status": "dry_run" if dry_run else "ok",
        "billing_project": config.billing_project,
        "source": config.source_project,
        "destination": config.destination_project,
        "minimum_delivery_date": minimum_date.isoformat(),
        "delivery_partitions_checked": len(source_delivery),
        "delivery_partitions_changed": len(partitions),
        "delivery_partition_dates": [value.isoformat() for value in partitions],
        "buy_data_changed": buy_changed,
        "buy_source_rows": source_buy.row_count,
        "buy_destination_rows_before": destination_buy.row_count,
        "delivery_query_job_ids": [],
        "delivery_load_job_ids": [],
        "buy_query_job_id": "",
        "buy_load_job_id": "",
        "started_at": started_at.isoformat(),
    }
    if dry_run:
        result["finished_at"] = datetime.now(timezone.utc).isoformat()
        return result

    if partitions:
        query_job_ids, load_job_ids = transfer_delivery_partitions(
            client,
            config,
            partitions,
        )
        result["delivery_query_job_ids"] = query_job_ids
        result["delivery_load_job_ids"] = load_job_ids
    if buy_changed:
        buy_query_job_id, buy_load_job_id = transfer_buy_table(client, config)
        result["buy_query_job_id"] = buy_query_job_id
        result["buy_load_job_id"] = buy_load_job_id

    if partitions:
        refreshed = partition_fingerprints(
            client,
            config.destination_delivery,
            config.destination_location,
            minimum_date,
            dates=partitions,
        )
        expected = {value: source_delivery[value] for value in partitions}
        if refreshed != expected:
            raise RuntimeError("Delivery validation failed after cross-region copy.")

    if buy_changed:
        refreshed_buy = table_fingerprint(
            client,
            config.destination_buy,
            config.destination_location,
        )
        if refreshed_buy != source_buy:
            raise RuntimeError("Buy Data validation failed after cross-region copy.")

    result["finished_at"] = datetime.now(timezone.utc).isoformat()
    return result


def result_json(result: dict[str, object]) -> str:
    return json.dumps(result, indent=2, sort_keys=True)


def env_dry_run() -> bool:
    return os.environ.get("BQ_SYNC_DRY_RUN", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
