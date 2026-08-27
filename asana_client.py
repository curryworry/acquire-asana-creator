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

    def create_subtask(self, parent_task_gid: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = self.session.post(
            f"{ASANA_API_BASE}/tasks/{parent_task_gid}/subtasks",
            json={"data": payload},
            timeout=self.timeout,
        )

        if response.status_code >= 400:
            msg = self._extract_error_message(response)
            raise AsanaError(msg)

        body = response.json()
        return body.get("data", body)

    def create_task_comment(self, task_gid: str, text: str) -> Dict[str, Any]:
        response = self.session.post(
            f"{ASANA_API_BASE}/tasks/{task_gid}/stories",
            json={"data": {"text": text}},
            timeout=self.timeout,
        )

        if response.status_code >= 400:
            msg = self._extract_error_message(response)
            raise AsanaError(msg)

        body = response.json()
        return body.get("data", body)

    def list_project_tasks(self, project_gid: str) -> List[Dict[str, str]]:
        tasks: List[Dict[str, str]] = []
        offset = None

        while True:
            params: Dict[str, Any] = {
                "limit": 100,
                "opt_fields": "gid,name",
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
                if not isinstance(task, dict):
                    continue
                name = str(task.get("name", "")).strip()
                gid = str(task.get("gid", "")).strip()
                if name and gid:
                    tasks.append({"gid": gid, "name": name})

            next_page = body.get("next_page")
            if not isinstance(next_page, dict) or not next_page.get("offset"):
                break
            offset = next_page["offset"]

        return tasks

    def list_project_task_names(self, project_gid: str) -> List[str]:
        return [task["name"] for task in self.list_project_tasks(project_gid)]

    def list_subtasks(self, parent_task_gid: str) -> List[Dict[str, str]]:
        subtasks: List[Dict[str, str]] = []
        offset = None

        while True:
            params: Dict[str, Any] = {
                "limit": 100,
                "opt_fields": "gid,name",
            }
            if offset:
                params["offset"] = offset

            response = self.session.get(
                f"{ASANA_API_BASE}/tasks/{parent_task_gid}/subtasks",
                params=params,
                timeout=self.timeout,
            )
            if response.status_code >= 400:
                msg = self._extract_error_message(response)
                raise AsanaError(msg)

            body = response.json()
            for task in body.get("data", []):
                if not isinstance(task, dict):
                    continue
                name = str(task.get("name", "")).strip()
                gid = str(task.get("gid", "")).strip()
                if name and gid:
                    subtasks.append({"gid": gid, "name": name})

            next_page = body.get("next_page")
            if not isinstance(next_page, dict) or not next_page.get("offset"):
                break
            offset = next_page["offset"]

        return subtasks

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
