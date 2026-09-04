from __future__ import annotations

import time
from datetime import date
from io import BytesIO
from typing import Any

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload


DV360_SCOPE = "https://www.googleapis.com/auth/display-video"


class DV360Error(RuntimeError):
    pass


class DV360Client:
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
            raise DV360Error(f"Missing DV360 OAuth setting(s): {', '.join(missing)}")

        credentials = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=[DV360_SCOPE],
        )
        self.service = build("displayvideo", "v4", credentials=credentials, cache_discovery=False)

    def list_advertisers(self, partner_id: str, status_filter: str = 'entityStatus="ENTITY_STATUS_ACTIVE"') -> list[dict[str, str]]:
        advertisers: list[dict[str, str]] = []
        request: dict[str, Any] = {"partnerId": partner_id, "pageSize": 200}
        if status_filter.strip():
            request["filter"] = status_filter.strip()

        while True:
            response = self.service.advertisers().list(**request).execute()
            for advertiser in response.get("advertisers", []):
                advertisers.append(
                    {
                        "advertiser_id": str(advertiser.get("advertiserId", "")).strip(),
                        "advertiser_name": str(advertiser.get("displayName", "")).strip(),
                    }
                )
            token = response.get("nextPageToken")
            if not token:
                break
            request["pageToken"] = token
        return [advertiser for advertiser in advertisers if advertiser["advertiser_id"]]

    def list_current_insertion_order_ids(self, advertiser_id: str, run_date: date) -> list[str]:
        insertion_order_ids: list[str] = []
        request: dict[str, Any] = {
            "advertiserId": advertiser_id,
            "pageSize": 200,
            "filter": 'entityStatus="ENTITY_STATUS_ACTIVE"',
        }
        while True:
            response = self.service.advertisers().insertionOrders().list(**request).execute()
            for insertion_order in response.get("insertionOrders", []):
                if self._insertion_order_has_current_budget(insertion_order, run_date):
                    insertion_order_ids.append(str(insertion_order.get("insertionOrderId", "")).strip())
            token = response.get("nextPageToken")
            if not token:
                break
            request["pageToken"] = token
        return [item for item in insertion_order_ids if item]

    @staticmethod
    def _api_date(value: dict[str, int] | None) -> date | None:
        if not value:
            return None
        try:
            return date(int(value["year"]), int(value["month"]), int(value["day"]))
        except (KeyError, TypeError, ValueError):
            return None

    @classmethod
    def _insertion_order_has_current_budget(cls, insertion_order: dict[str, Any], run_date: date) -> bool:
        for segment in insertion_order.get("budget", {}).get("budgetSegments", []):
            date_range = segment.get("dateRange", {})
            start = cls._api_date(date_range.get("startDate"))
            end = cls._api_date(date_range.get("endDate"))
            if start and run_date < start:
                continue
            if end and run_date > end:
                continue
            return True
        return False

    def create_sdf_download(
        self,
        partner_id: str,
        advertiser_ids: list[str],
        file_types: list[str],
        sdf_version: str,
    ) -> dict[str, Any]:
        if not advertiser_ids:
            raise DV360Error("At least one advertiser ID is required for SDF download.")
        body = {
            "version": sdf_version,
            "partnerId": partner_id,
            "parentEntityFilter": {
                "fileType": file_types,
                "filterType": "FILTER_TYPE_ADVERTISER_ID",
                "filterIds": advertiser_ids,
            },
        }
        return self.service.sdfdownloadtasks().create(body=body).execute()

    def wait_for_operation(self, name: str, timeout_seconds: int = 1800, poll_seconds: int = 10) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        while True:
            operation = self.service.sdfdownloadtasks().operations().get(name=name).execute()
            if operation.get("done"):
                if "error" in operation:
                    raise DV360Error(f"SDF download operation failed: {operation['error']}")
                return operation
            if time.monotonic() >= deadline:
                raise DV360Error(f"SDF download operation timed out after {timeout_seconds} seconds: {name}")
            time.sleep(poll_seconds)

    def download_sdf_zip(self, resource_name: str) -> bytes:
        output = BytesIO()
        request = self.service.media().download_media(resourceName=resource_name)
        downloader = MediaIoBaseDownload(output, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return output.getvalue()

    def download_advertiser_sdf(
        self,
        partner_id: str,
        advertiser_id: str,
        file_types: list[str],
        sdf_version: str,
        timeout_seconds: int = 1800,
        poll_seconds: int = 10,
    ) -> bytes:
        return self.download_advertisers_sdf(
            partner_id=partner_id,
            advertiser_ids=[advertiser_id],
            file_types=file_types,
            sdf_version=sdf_version,
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )

    def download_advertisers_sdf(
        self,
        partner_id: str,
        advertiser_ids: list[str],
        file_types: list[str],
        sdf_version: str,
        timeout_seconds: int = 1800,
        poll_seconds: int = 10,
    ) -> bytes:
        operation = self.create_sdf_download(partner_id, advertiser_ids, file_types, sdf_version)
        completed = self.wait_for_operation(
            str(operation.get("name", "")),
            timeout_seconds=timeout_seconds,
            poll_seconds=poll_seconds,
        )
        resource_name = completed.get("response", {}).get("resourceName", "")
        if not resource_name:
            raise DV360Error(f"SDF download operation completed without a resource name: {completed}")
        return self.download_sdf_zip(resource_name)

    def advertiser_positive_channel_count(self, advertiser_id: str) -> int:
        request: dict[str, Any] = {
            "advertiserId": advertiser_id,
            "targetingType": "TARGETING_TYPE_CHANNEL",
            "pageSize": 200,
        }
        count = 0
        try:
            while True:
                response = (
                    self.service.advertisers()
                    .targetingTypes()
                    .assignedTargetingOptions()
                    .list(**request)
                    .execute()
                )
                for option in response.get("assignedTargetingOptions", []):
                    details = option.get("channelDetails", {})
                    inheritance = str(option.get("inheritance", ""))
                    if details.get("negative") is False and inheritance != "INHERITED_FROM_PARTNER":
                        count += 1
                token = response.get("nextPageToken")
                if not token:
                    break
                request["pageToken"] = token
        except HttpError as exc:
            raise DV360Error(f"Failed to read advertiser channel targeting for {advertiser_id}: {exc}") from exc
        return count
