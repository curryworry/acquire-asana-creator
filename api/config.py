import os
import tomllib
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping


REPO_ROOT = Path(__file__).resolve().parents[1]


@lru_cache(maxsize=1)
def _streamlit_secrets() -> dict[str, Any]:
    secrets_path = REPO_ROOT / ".streamlit" / "secrets.toml"
    if not secrets_path.exists():
        return {}
    try:
        return tomllib.loads(secrets_path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def get_secret(name: str, default: str = "") -> str:
    env_value = os.environ.get(name)
    if env_value is not None:
        return env_value

    secrets = _streamlit_secrets()
    value = secrets.get(name, default)
    return str(value if value is not None else default)


def get_auth_users() -> dict[str, dict[str, str]]:
    auth_block = _streamlit_secrets().get("auth", {})
    if not isinstance(auth_block, Mapping):
        return {}

    users = auth_block.get("users", {})
    if not isinstance(users, Mapping):
        return {}

    out: dict[str, dict[str, str]] = {}
    for username, raw_value in users.items():
        if not isinstance(raw_value, Mapping):
            continue
        password = str(raw_value.get("password", "") or "").strip()
        if not password:
            continue
        out[str(username).strip()] = {
            "display_name": str(raw_value.get("display_name", username) or username).strip(),
            "password": password,
        }
    return out

