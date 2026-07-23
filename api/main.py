from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.auth import auth_enabled, create_token, current_user, verify_password
from api.config import REPO_ROOT, get_auth_users, get_secret
from api.dashboard_service import alerts_dashboard, margin_dashboard, write_snooze_action
from api.schemas import DismissActionRequest, LoginRequest, LoginResponse, SnoozeActionRequest, UserResponse


app = FastAPI(title="Acquire Asana Creator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIST = REPO_ROOT / "frontend" / "dist"


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/auth/me", response_model=UserResponse)
def me(user: dict[str, str] = Depends(current_user)) -> UserResponse:
    return UserResponse(
        auth_enabled=auth_enabled(),
        username=user.get("username"),
        display_name=user.get("display_name"),
    )


@app.post("/api/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest) -> LoginResponse:
    users = get_auth_users()
    if not users:
        token = create_token("local", "Local User")
        return LoginResponse(token=token, username="local", display_name="Local User")

    user = users.get(payload.username.strip())
    if not user or not verify_password(payload.password, user["password"]):
        raise HTTPException(status_code=401, detail="Invalid username or password.")

    token = create_token(payload.username.strip(), user["display_name"])
    return LoginResponse(token=token, username=payload.username.strip(), display_name=user["display_name"])


@app.get("/api/margin")
def margin(_: dict[str, str] = Depends(current_user)) -> dict:
    return margin_dashboard()


@app.get("/api/alerts")
def alerts(
    _: dict[str, str] = Depends(current_user),
    alert_type: str | None = Query(default=None),
    state: str = Query(default="OPEN"),
    query: str = Query(default=""),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=100, ge=1, le=500),
) -> dict:
    return alerts_dashboard(alert_type=alert_type, state=state, query_text=query, page=page, page_size=page_size)


@app.post("/api/margin/snooze")
def margin_snooze(payload: SnoozeActionRequest, user: dict[str, str] = Depends(current_user)) -> dict[str, str]:
    write_snooze_action("snooze", payload.alerts, user["username"], payload.reason, payload.end_date, payload.run_id)
    return {"status": "ok"}


@app.post("/api/alerts/snooze")
def alerts_snooze(payload: SnoozeActionRequest, user: dict[str, str] = Depends(current_user)) -> dict[str, str]:
    write_snooze_action("snooze", payload.alerts, user["username"], payload.reason, payload.end_date, payload.run_id)
    return {"status": "ok"}


@app.post("/api/alerts/unsnooze")
def alerts_unsnooze(payload: SnoozeActionRequest, user: dict[str, str] = Depends(current_user)) -> dict[str, str]:
    write_snooze_action("unsnooze", payload.alerts, user["username"], "", None, payload.run_id)
    return {"status": "ok"}


@app.post("/api/alerts/dismiss")
def alerts_dismiss(payload: DismissActionRequest, user: dict[str, str] = Depends(current_user)) -> dict[str, str]:
    admin_pass = get_secret("ADMIN_PASS", "")
    if not admin_pass or payload.admin_pass != admin_pass:
        raise HTTPException(status_code=403, detail="Admin password is incorrect.")
    write_snooze_action("dismiss", payload.alerts, user["username"], payload.reason, None, payload.run_id)
    return {"status": "ok"}


if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=Path(FRONTEND_DIST), html=True), name="frontend")
