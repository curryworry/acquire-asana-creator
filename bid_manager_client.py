from __future__ import annotations

import csv
import time
from datetime import date
from io import StringIO
from typing import Any

import requests
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


BID_MANAGER_SCOPE = "https://www.googleapis.com/auth/doubleclickbidmanager"
DISPLAY_VIDEO_SCOPE = "https://www.googleapis.com/auth/display-video"
DISPLAY_VIDEO_MEDIAPLANNING_SCOPE = "https://www.googleapis.com/auth/display-video-mediaplanning"
DEFAULT_SPEND_METRIC = "METRIC_TOTAL_MEDIA_COST_ADVERTISER"
SPEND_GROUP_BYS = [
    "FILTER_ADVERTISER",
    "FILTER_ADVERTISER_NAME",
    "FILTER_ADVERTISER_CURRENCY",
    "FILTER_INSERTION_ORDER",
    "FILTER_INSERTION_ORDER_NAME",
    "FILTER_LINE_ITEM",
    "FILTER_LINE_ITEM_NAME",
]


class BidManagerError(RuntimeError):
    pass


class BidManagerClient:
    def __init__(self, client_id: str, client_secret: str, refresh_token: str) -> None:
        missing = [
            name
            for name, value in {
                "DV360_CLIENT_ID": client_id,
                "DV360_CLIENT_SECRET": client_secret,
                "DV360_REFRESH_TOKEN": refresh_token,
            }.items()
            if not str(value or "").strip()
        ]
        if missing:
            raise BidManagerError(f"Missing Bid Manager OAuth setting(s): {', '.join(missing)}")

        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[BID_MANAGER_SCOPE, DISPLAY_VIDEO_SCOPE, DISPLAY_VIDEO_MEDIAPLANNING_SCOPE],
        )
        self.service = build("doubleclickbidmanager", "v2", credentials=credentials, cache_discovery=False)

    @staticmethod
    def _api_date(value: dict[str, int] | None) -> date | None:
        if not value:
            return None
        try:
            return date(int(value["year"]), int(value["month"]), int(value["day"]))
        except (KeyError, TypeError, ValueError):
            return None

    def create_previous_day_spend_query(
        self,
        partner_id: str,
        metric: str = DEFAULT_SPEND_METRIC,
    ) -> str:
        body = {
            "metadata": {
                "title": f"Acquire Missing Inclusion Active Spend {int(time.time())}",
                "dataRange": {"range": "PREVIOUS_DAY"},
                "format": "CSV",
                "sendNotification": False,
            },
            "params": {
                "type": "STANDARD",
                "groupBys": SPEND_GROUP_BYS,
                "filters": [{"type": "FILTER_PARTNER", "value": partner_id}],
                "metrics": [metric],
            },
            "schedule": {"frequency": "ONE_TIME"},
        }
        try:
            query = self.service.queries().create(body=body).execute()
        except HttpError as exc:
            raise BidManagerError(f"Failed to create Bid Manager spend query: {exc}") from exc
        query_id = str(query.get("queryId", "")).strip()
        if not query_id:
            raise BidManagerError(f"Bid Manager query was created without a queryId: {query}")
        return query_id

    def delete_query(self, query_id: str) -> None:
        try:
            self.service.queries().delete(queryId=query_id).execute()
        except HttpError as exc:
            raise BidManagerError(f"Failed to delete Bid Manager spend query {query_id}: {exc}") from exc

    def run_query(self, query_id: str) -> str:
        try:
            report = self.service.queries().run(
                queryId=query_id,
                body={"dataRange": {"range": "PREVIOUS_DAY"}},
            ).execute()
        except HttpError as exc:
            raise BidManagerError(f"Failed to run Bid Manager spend query {query_id}: {exc}") from exc
        report_id = str(report.get("key", {}).get("reportId", "")).strip()
        if not report_id:
            raise BidManagerError(f"Bid Manager query run did not return a reportId: {report}")
        return report_id

    def wait_for_report(
        self,
        query_id: str,
        report_id: str,
        timeout_seconds: int = 900,
        poll_seconds: int = 10,
    ) -> dict[str, Any]:
        started = time.monotonic()
        deadline = started + timeout_seconds
        next_log = started
        while True:
            try:
                report = self.service.queries().reports().get(queryId=query_id, reportId=report_id).execute()
            except HttpError as exc:
                raise BidManagerError(f"Failed to read Bid Manager report {report_id}: {exc}") from exc
            state = str(report.get("metadata", {}).get("status", {}).get("state", "")).strip()
            if state == "DONE":
                elapsed = int(time.monotonic() - started)
                print(f"Bid Manager spend report completed after {elapsed}s.", flush=True)
                return report
            if state == "FAILED":
                raise BidManagerError(f"Bid Manager spend report failed: {report}")
            now = time.monotonic()
            if now >= next_log:
                elapsed = int(now - started)
                print(f"Bid Manager spend report still {state or 'pending'} after {elapsed}s.", flush=True)
                next_log = now + 60
            if now >= deadline:
                raise BidManagerError(f"Bid Manager spend report timed out after {timeout_seconds} seconds.")
            time.sleep(poll_seconds)

    @staticmethod
    def download_report_csv(report: dict[str, Any]) -> tuple[list[dict[str, str]], date | None]:
        metadata = report.get("metadata", {})
        url = str(metadata.get("googleCloudStoragePath", "")).strip()
        if not url:
            raise BidManagerError(f"Bid Manager report completed without a download URL: {report}")
        try:
            response = requests.get(url, timeout=120)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise BidManagerError(f"Failed to download Bid Manager report CSV: {exc}") from exc
        text = response.content.decode("utf-8-sig", errors="replace")
        rows = [dict(row) for row in csv.DictReader(StringIO(text))]
        report_date = BidManagerClient._api_date(metadata.get("reportDataStartDate"))
        return rows, report_date

    def previous_day_line_item_spend(
        self,
        partner_id: str,
        metric: str = DEFAULT_SPEND_METRIC,
        timeout_seconds: int = 900,
        poll_seconds: int = 10,
    ) -> dict[str, Any]:
        query_id = self.create_previous_day_spend_query(partner_id=partner_id, metric=metric)
        try:
            report_id = self.run_query(query_id)
            report = self.wait_for_report(
                query_id=query_id,
                report_id=report_id,
                timeout_seconds=timeout_seconds,
                poll_seconds=poll_seconds,
            )
            rows, report_date = self.download_report_csv(report)
            return {
                "rows": rows,
                "report_date": report_date.isoformat() if report_date else "",
                "query_id": query_id,
                "report_id": report_id,
                "metric": metric,
            }
        finally:
            try:
                self.delete_query(query_id)
            except BidManagerError as exc:
                print(str(exc), flush=True)
