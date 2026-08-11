from pydantic import BaseModel


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    token: str
    username: str
    display_name: str


class UserResponse(BaseModel):
    auth_enabled: bool
    username: str | None = None
    display_name: str | None = None


class SnoozeActionRequest(BaseModel):
    alerts: list[dict[str, str]]
    reason: str = ""
    end_date: str | None = None
    run_id: str = ""


class DismissActionRequest(BaseModel):
    alerts: list[dict[str, str]]
    reason: str
    admin_pass: str
    run_id: str = ""


class CampaignAlertRunRequest(BaseModel):
    recipients: str
    force_run: bool = True


class DailyTraffickingRunRequest(BaseModel):
    force_dry_run: bool = True


class AutomationRunResponse(BaseModel):
    status: str
    exit_code: int
    output: str
    started_at: str
    finished_at: str
