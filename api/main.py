from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from api.auth import auth_enabled, auth_required, create_token, current_user, verify_password
from api.automation_service import (
    campaign_alert_defaults,
    daily_trafficking_defaults,
    is_admin_user,
    qa_missing_inclusion_defaults,
    qa_video_on_trademe_defaults,
    trafficking_bigquery_sync_defaults,
    run_campaign_alert,
    run_daily_trafficking,
    run_qa_missing_inclusion,
    run_qa_video_on_trademe,
    run_trafficking_bigquery_sync,
)
from api.config import REPO_ROOT, get_auth_users, get_secret
from api.dashboard_service import (
    alerts_bootstrap,
    alerts_dashboard,
    bq_context,
    create_report_token,
    decode_report_token,
    enqueue_snooze_actions,
    ensure_control_tables,
    going_live_soon_dashboard,
    margin_dashboard,
    pacing_dashboard,
    process_asana_comment_action_queue,
    process_snooze_action_queue,
    report_share_url,
    reporting_dashboard,
)
from api.qa_service import missing_inclusion_dashboard, video_on_trademe_dashboard
from api.schemas import (
    AutomationRunResponse,
    CampaignAlertRunRequest,
    DailyTraffickingRunRequest,
    DismissActionRequest,
    LoginRequest,
    LoginResponse,
    ReportCreateRequest,
    SnoozeActionRequest,
    UserResponse,
)


app = FastAPI(title="Acquire Asana Creator API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5177",
        "http://127.0.0.1:5177",
        "https://reports.acquire.agency",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

FRONTEND_DIST = REPO_ROOT / "frontend" / "dist"


@app.on_event("startup")
def ensure_control_tables_on_startup() -> None:
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)


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
        if auth_required():
            raise HTTPException(status_code=500, detail="Auth is required but no users are configured.")
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


@app.get("/api/pacing")
def pacing(_: dict[str, str] = Depends(current_user)) -> dict:
    return pacing_dashboard()


@app.get("/api/dashboard/bootstrap")
def dashboard_bootstrap(_: dict[str, str] = Depends(current_user)) -> dict:
    with ThreadPoolExecutor(max_workers=4) as executor:
        alerts_future = executor.submit(alerts_bootstrap)
        going_live_soon_future = executor.submit(going_live_soon_dashboard)
        margin_future = executor.submit(margin_dashboard)
        pacing_future = executor.submit(pacing_dashboard)
        return {
            "alerts": alerts_future.result(),
            "going_live_soon": going_live_soon_future.result(),
            "margin": margin_future.result(),
            "pacing": pacing_future.result(),
        }


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


@app.get("/api/alerts/bootstrap")
def alerts_bootstrap_route(_: dict[str, str] = Depends(current_user)) -> dict:
    return alerts_bootstrap()


@app.get("/api/alerts/going-live-soon")
def going_live_soon(_: dict[str, str] = Depends(current_user)) -> dict:
    return going_live_soon_dashboard()


@app.get("/api/qa/video-on-trademe")
def qa_video_on_trademe(_: dict[str, str] = Depends(current_user)) -> dict:
    try:
        return video_on_trademe_dashboard()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/qa/missing-inclusion-list")
def qa_missing_inclusion_list(_: dict[str, str] = Depends(current_user)) -> dict:
    try:
        return missing_inclusion_dashboard()
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@app.get("/api/qa/pacing-overview")
def qa_pacing_overview(_: dict[str, str] = Depends(current_user)) -> dict:
    return pacing_dashboard(include_all=True)


@app.post("/api/reports")
def create_report(payload: ReportCreateRequest, _: dict[str, str] = Depends(current_user)) -> dict:
    try:
        token = create_report_token(payload.job_number)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "token": token,
        "job_number": payload.job_number.strip(),
        "share_url": report_share_url(token),
    }


@app.get("/api/reports/{token}")
def public_report(
    token: str,
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
) -> dict:
    try:
        job_number = decode_report_token(token)
        return reporting_dashboard(job_number, start_date=start_date, end_date=end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


def enqueue_snooze_response(
    background_tasks: BackgroundTasks,
    action: str,
    payload: SnoozeActionRequest,
    user: dict[str, str],
    reason: str = "",
    end_date: str | None = None,
) -> dict[str, str | int]:
    action_ids = enqueue_snooze_actions(
        action,
        payload.alerts,
        user.get("display_name") or user["username"],
        reason,
        end_date,
        payload.run_id,
    )
    background_tasks.add_task(process_snooze_action_queue)
    background_tasks.add_task(process_asana_comment_action_queue)
    return {"status": "queued", "queued": len(action_ids)}


@app.post("/api/margin/snooze")
def margin_snooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "snooze", payload, user, payload.reason, payload.end_date)


@app.post("/api/pacing/snooze")
def pacing_snooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "snooze", payload, user, payload.reason, payload.end_date)


@app.post("/api/pacing/unsnooze")
def pacing_unsnooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "unsnooze", payload, user)


@app.post("/api/alerts/snooze")
def alerts_snooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "snooze", payload, user, payload.reason, payload.end_date)


@app.post("/api/alerts/unsnooze")
def alerts_unsnooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "unsnooze", payload, user)


@app.post("/api/qa/video-on-trademe/snooze")
def qa_video_on_trademe_snooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "snooze", payload, user, payload.reason, payload.end_date)


@app.post("/api/qa/video-on-trademe/unsnooze")
def qa_video_on_trademe_unsnooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "unsnooze", payload, user)


@app.post("/api/qa/missing-inclusion-list/snooze")
def qa_missing_inclusion_list_snooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "snooze", payload, user, payload.reason, payload.end_date)


@app.post("/api/qa/missing-inclusion-list/unsnooze")
def qa_missing_inclusion_list_unsnooze(
    payload: SnoozeActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    return enqueue_snooze_response(background_tasks, "unsnooze", payload, user)


@app.post("/api/alerts/dismiss")
def alerts_dismiss(
    payload: DismissActionRequest,
    background_tasks: BackgroundTasks,
    user: dict[str, str] = Depends(current_user),
) -> dict[str, str | int]:
    admin_pass = get_secret("ADMIN_PASS", "")
    if not admin_pass or payload.admin_pass != admin_pass:
        raise HTTPException(status_code=403, detail="Admin password is incorrect.")
    return enqueue_snooze_response(background_tasks, "dismiss", payload, user, payload.reason)


def require_admin(user: dict[str, str]) -> dict[str, str]:
    if not is_admin_user(user):
        raise HTTPException(status_code=403, detail="Admin access is restricted.")
    return user


@app.get("/api/admin/automations")
def admin_automations(user: dict[str, str] = Depends(current_user)) -> dict:
    require_admin(user)
    return {
        "admin": user.get("username"),
        "campaign_alert": campaign_alert_defaults(),
        "daily_trafficking": daily_trafficking_defaults(),
        "trafficking_bigquery_sync": trafficking_bigquery_sync_defaults(),
        "qa_video_on_trademe": qa_video_on_trademe_defaults(),
        "qa_missing_inclusion": qa_missing_inclusion_defaults(),
    }


@app.post("/api/admin/snooze-actions/process")
def admin_process_snooze_actions(user: dict[str, str] = Depends(current_user)) -> dict[str, int]:
    require_admin(user)
    return process_snooze_action_queue()


@app.post("/api/admin/asana-comment-actions/process")
def admin_process_asana_comment_actions(user: dict[str, str] = Depends(current_user)) -> dict[str, int]:
    require_admin(user)
    return process_asana_comment_action_queue()


@app.post("/api/admin/control-tables/ensure")
def admin_ensure_control_tables(user: dict[str, str] = Depends(current_user)) -> dict[str, str]:
    require_admin(user)
    client, project_id, dataset = bq_context()
    ensure_control_tables(client, project_id, dataset)
    return {"status": "ok", "project_id": project_id, "dataset": dataset}


@app.post("/api/admin/automations/campaign-alert", response_model=AutomationRunResponse)
def admin_campaign_alert(
    payload: CampaignAlertRunRequest,
    user: dict[str, str] = Depends(current_user),
) -> AutomationRunResponse:
    require_admin(user)
    recipients = payload.recipients.strip()
    if not recipients:
        raise HTTPException(status_code=400, detail="At least one recipient is required.")
    return AutomationRunResponse(**run_campaign_alert(recipients=recipients, force_run=payload.force_run))


@app.post("/api/admin/automations/daily-trafficking", response_model=AutomationRunResponse)
def admin_daily_trafficking(
    payload: DailyTraffickingRunRequest,
    user: dict[str, str] = Depends(current_user),
) -> AutomationRunResponse:
    require_admin(user)
    return AutomationRunResponse(**run_daily_trafficking(force_dry_run=payload.force_dry_run))


@app.post("/api/admin/automations/trafficking-bigquery-sync", response_model=AutomationRunResponse)
def admin_trafficking_bigquery_sync(
    user: dict[str, str] = Depends(current_user),
) -> AutomationRunResponse:
    require_admin(user)
    return AutomationRunResponse(**run_trafficking_bigquery_sync())


@app.post("/api/admin/automations/qa-video-on-trademe", response_model=AutomationRunResponse)
def admin_qa_video_on_trademe(user: dict[str, str] = Depends(current_user)) -> AutomationRunResponse:
    require_admin(user)
    return AutomationRunResponse(**run_qa_video_on_trademe())


@app.post("/api/admin/automations/qa-missing-inclusion-list", response_model=AutomationRunResponse)
def admin_qa_missing_inclusion_list(user: dict[str, str] = Depends(current_user)) -> AutomationRunResponse:
    require_admin(user)
    return AutomationRunResponse(**run_qa_missing_inclusion())


if FRONTEND_DIST.exists():
    app.mount("/", StaticFiles(directory=Path(FRONTEND_DIST), html=True), name="frontend")
