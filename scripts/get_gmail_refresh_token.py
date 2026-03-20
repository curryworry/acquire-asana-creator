#!/usr/bin/env python3
"""One-time Gmail OAuth helper to generate a refresh token.

Usage:
  python3 scripts/get_gmail_refresh_token.py \
    --client-id "..." \
    --client-secret "..."

Then copy the printed REFRESH_TOKEN into GitHub Actions secret GMAIL_REFRESH_TOKEN.
"""

import argparse
import json
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from google_auth_oauthlib.flow import InstalledAppFlow


DEFAULT_SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.modify",
    "https://www.googleapis.com/auth/gmail.send",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate Gmail OAuth refresh token")
    parser.add_argument("--client-id", required=False, default="", help="Google OAuth client ID")
    parser.add_argument("--client-secret", required=False, default="", help="Google OAuth client secret")
    parser.add_argument(
        "--secrets-file",
        default=".streamlit/secrets.toml",
        help="Path to TOML file containing GMAIL_CLIENT_ID/GMAIL_CLIENT_SECRET",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8765,
        help="Local callback port (must be allowed in OAuth redirect URIs for Web client type)",
    )
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Print auth URL instead of opening browser automatically",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client_id = args.client_id.strip()
    client_secret = args.client_secret.strip()

    if not client_id or not client_secret:
        secrets_path = Path(args.secrets_file)
        if not secrets_path.exists():
            raise RuntimeError(
                f"Missing client args and secrets file not found: {secrets_path}"
            )
        secrets = tomllib.loads(secrets_path.read_text())
        client_id = client_id or str(secrets.get("GMAIL_CLIENT_ID", "")).strip()
        client_secret = client_secret or str(secrets.get("GMAIL_CLIENT_SECRET", "")).strip()

    if not client_id or not client_secret:
        raise RuntimeError(
            "Could not resolve Gmail OAuth client values. "
            "Pass --client-id/--client-secret or set GMAIL_CLIENT_ID/GMAIL_CLIENT_SECRET in secrets.toml."
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

    flow = InstalledAppFlow.from_client_config(client_config, scopes=DEFAULT_SCOPES)
    creds = flow.run_local_server(
        host="localhost",
        port=args.port,
        open_browser=not args.no_browser,
        access_type="offline",
        prompt="consent",
    )

    print("\nSUCCESS: OAuth complete.\n")
    print("Copy this value into GitHub secret GMAIL_REFRESH_TOKEN:\n")
    print(creds.refresh_token or "<no_refresh_token_returned>")

    meta = {
        "client_id": client_id,
        "scopes": list(creds.scopes or []),
        "token_generated_utc": datetime.now(timezone.utc).isoformat(),
    }
    print("\nToken metadata (safe to log):")
    print(json.dumps(meta, indent=2))

    if not creds.refresh_token:
        print(
            "\nWARNING: No refresh token returned. Re-run with prompt='consent' and revoke prior app access if needed."
        )
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
