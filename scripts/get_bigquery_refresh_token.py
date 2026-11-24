#!/usr/bin/env python3
"""Generate and store an offline BigQuery OAuth refresh token."""

import argparse
import json
import re
import tomllib
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow


BIGQUERY_SCOPE = "https://www.googleapis.com/auth/bigquery"
DEFAULT_SECRETS_FILE = ".streamlit/secrets.toml"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--secrets-file", default=DEFAULT_SECRETS_FILE)
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--no-browser", action="store_true")
    return parser.parse_args()


def upsert_root_toml_value(text: str, key: str, value: str) -> str:
    encoded = json.dumps(value)
    pattern = re.compile(rf"^{re.escape(key)}\s*=.*$", re.MULTILINE)
    replacement = f"{key} = {encoded}"
    if pattern.search(text):
        return pattern.sub(replacement, text, count=1)

    # Root TOML values must precede the first table header. Appending would place
    # the secret inside whichever table happens to be last in the file.
    table_header = re.search(r"^\s*\[", text, re.MULTILINE)
    if table_header:
        insert_at = table_header.start()
        prefix = text[:insert_at].rstrip("\n")
        suffix = text[insert_at:].lstrip("\n")
        return f"{prefix}\n{replacement}\n\n{suffix}"
    separator = "" if not text or text.endswith("\n") else "\n"
    return f"{text}{separator}{replacement}\n"


def main() -> int:
    args = parse_args()
    secrets_path = Path(args.secrets_file)
    raw_text = secrets_path.read_text()
    secrets = tomllib.loads(raw_text)
    client_id = str(secrets.get("BQ_USER_CLIENT_ID", "")).strip()
    client_secret = str(secrets.get("BQ_USER_CLIENT_SECRET", "")).strip()

    if not client_id or not client_secret:
        raise RuntimeError(
            "BQ_USER_CLIENT_ID and BQ_USER_CLIENT_SECRET are required in "
            f"{secrets_path}."
        )

    client_config = {
        "installed": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [f"http://localhost:{args.port}/"],
        }
    }
    flow = InstalledAppFlow.from_client_config(
        client_config,
        scopes=[BIGQUERY_SCOPE],
    )
    credentials = flow.run_local_server(
        host="localhost",
        port=args.port,
        open_browser=not args.no_browser,
        access_type="offline",
        prompt="consent",
        login_hint="ashwin@acquirenz.com",
    )

    if not credentials.refresh_token:
        raise RuntimeError(
            "Google did not return a refresh token. Revoke the existing app grant "
            "and run this helper again."
        )

    updated = upsert_root_toml_value(
        raw_text,
        "BQ_USER_REFRESH_TOKEN",
        credentials.refresh_token,
    )
    updated = upsert_root_toml_value(
        updated,
        "BQ_USER_EMAIL",
        "ashwin@acquirenz.com",
    )
    secrets_path.write_text(updated)

    print("OAuth complete. The BigQuery refresh token was stored locally.")
    print("Authorized scopes:", ", ".join(credentials.scopes or []))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
