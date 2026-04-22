#!/usr/bin/env python3
"""Print a fresh Gmail OAuth access token using refresh-token auth."""

import argparse
import json
import tomllib
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Print a fresh Gmail OAuth access token")
    parser.add_argument("--client-id", default="", help="Google OAuth client ID")
    parser.add_argument("--client-secret", default="", help="Google OAuth client secret")
    parser.add_argument("--refresh-token", default="", help="Google OAuth refresh token")
    parser.add_argument(
        "--secrets-file",
        default=".streamlit/secrets.toml",
        help="Path to TOML file containing GMAIL_CLIENT_ID/GMAIL_CLIENT_SECRET/GMAIL_REFRESH_TOKEN",
    )
    return parser.parse_args()


def load_from_toml(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        return {}
    return tomllib.loads(p.read_text())


def main() -> int:
    args = parse_args()
    secrets = load_from_toml(args.secrets_file)

    client_id = (args.client_id or secrets.get("GMAIL_CLIENT_ID", "")).strip()
    client_secret = (args.client_secret or secrets.get("GMAIL_CLIENT_SECRET", "")).strip()
    refresh_token = (args.refresh_token or secrets.get("GMAIL_REFRESH_TOKEN", "")).strip()

    if not client_id or not client_secret or not refresh_token:
        raise RuntimeError(
            "Missing credentials. Provide --client-id/--client-secret/--refresh-token "
            "or set GMAIL_CLIENT_ID/GMAIL_CLIENT_SECRET/GMAIL_REFRESH_TOKEN in TOML."
        )

    creds = Credentials(
        token=None,
        refresh_token=refresh_token,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=client_id,
        client_secret=client_secret,
        scopes=SCOPES,
    )
    creds.refresh(Request())

    print(creds.token)
    print(
        json.dumps(
            {
                "expiry": creds.expiry.isoformat() if creds.expiry else None,
                "scopes": SCOPES,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
