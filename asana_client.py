from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


ASANA_API_BASE = "https://app.asana.com/api/1.0"


class AsanaError(Exception):
    pass


class AsanaClient:
    def __init__(self, access_token: str, timeout: int = 20) -> None:
        if not access_token:
            raise ValueError("Asana access token is required.")

        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )
        # Retry transient failures and API throttling without changing business logic.
        retry = Retry(
            total=4,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "POST"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=20)
        self.session.mount("https://", adapter)

    def create_task(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self.session.post(
            f"{ASANA_API_BASE}/tasks",
            json={"data": payload},
            timeout=self.timeout,
        )

        if response.status_code >= 400:
            msg = self._extract_error_message(response)
            raise AsanaError(msg)

        body = response.json()
        return body.get("data", body)

    def list_project_task_names(self, project_gid: str) -> List[str]:
        names: List[str] = []
        offset = None

        while True:
            params: Dict[str, Any] = {
                "limit": 100,
                "opt_fields": "name",
                # Include completed tasks to avoid recreating historical jobs.
                "completed_since": "1970-01-01T00:00:00.000Z",
            }
            if offset:
                params["offset"] = offset

            response = self.session.get(
                f"{ASANA_API_BASE}/projects/{project_gid}/tasks",
                params=params,
                timeout=self.timeout,
            )
            if response.status_code >= 400:
                msg = self._extract_error_message(response)
                raise AsanaError(msg)

            body = response.json()
            for task in body.get("data", []):
                if isinstance(task, dict):
                    name = str(task.get("name", "")).strip()
                    if name:
                        names.append(name)

            next_page = body.get("next_page")
            if not isinstance(next_page, dict) or not next_page.get("offset"):
                break
            offset = next_page["offset"]

        return names

    @staticmethod
    def _extract_error_message(response: requests.Response) -> str:
        try:
            body = response.json()
        except ValueError:
            return f"Asana API error {response.status_code}: {response.text}"

        errors = body.get("errors")
        if isinstance(errors, list) and errors:
            first = errors[0]
            message = first.get("message") if isinstance(first, dict) else None
            if message:
                return f"Asana API error {response.status_code}: {message}"

        return f"Asana API error {response.status_code}: {body}"
