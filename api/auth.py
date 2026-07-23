import base64
import hashlib
import hmac
import json
import time
from typing import Annotated

from fastapi import Depends, Header, HTTPException, status

from api.config import get_auth_users, get_secret


TOKEN_TTL_SECONDS = 60 * 60 * 12


def auth_enabled() -> bool:
    return bool(get_auth_users())


def _session_secret() -> str:
    return (
        get_secret("API_SESSION_SECRET")
        or get_secret("LINK_SIGNING_SECRET")
        or get_secret("ADMIN_PASS")
    )


def verify_password(password: str, expected_password: str) -> bool:
    return hmac.compare_digest(password, expected_password)


def create_token(username: str, display_name: str) -> str:
    secret = _session_secret()
    if not secret:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="API_SESSION_SECRET or LINK_SIGNING_SECRET is required for API login.",
        )

    payload = {
        "username": username,
        "display_name": display_name,
        "exp": int(time.time()) + TOKEN_TTL_SECONDS,
    }
    payload_bytes = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).decode("utf-8").rstrip("=")
    sig = hmac.new(secret.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{sig}"


def decode_token(token: str) -> dict[str, str]:
    secret = _session_secret()
    if not secret:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Auth is not configured.")

    try:
        payload_b64, sig = token.split(".", 1)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.") from exc

    expected = hmac.new(secret.encode("utf-8"), payload_b64.encode("utf-8"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.")

    padded = payload_b64 + "=" * ((4 - len(payload_b64) % 4) % 4)
    try:
        payload = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")))
    except Exception as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid token.") from exc

    if int(payload.get("exp", 0)) < int(time.time()):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Token expired.")

    return {
        "username": str(payload.get("username", "")),
        "display_name": str(payload.get("display_name", "")),
    }


def current_user(authorization: Annotated[str | None, Header()] = None) -> dict[str, str]:
    if not auth_enabled():
        return {"username": "local", "display_name": "Local User"}

    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token.")

    return decode_token(authorization.removeprefix("Bearer ").strip())

