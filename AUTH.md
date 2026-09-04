# Dashboard Auth

Last updated: 2026-09-04

## Purpose

The app uses a simple built-in username/password login for dashboard access.

This is intended for:
- small internal teams
- fewer than 10 users
- basic auditability for snoozes and similar actions

## How It Works

- Users are defined in the TOML secrets file.
- Passwords are currently stored in plain text in that TOML secret.
- Once signed in, the React frontend stores a bearer token in browser local storage.
- Dashboard actions use the authenticated user automatically.

## Secrets Format

The production Secret Manager value still uses the existing TOML shape. Locally, the ignored compatibility path is `.streamlit/secrets.toml`.

```toml
[auth.users.ashwin]
display_name = "Ashwin"
password = "ashwin-password"

[auth.users.rachel]
display_name = "Rachel Burns"
password = "rachel-password"
```

Notes:
- `ashwin` / `rachel` are the login usernames.
- `display_name` is what appears in the app audit trail.
- `password` is the plain-text password used for login.
- `API_SESSION_SECRET` is the preferred token signing secret.
- `LINK_SIGNING_SECRET` remains accepted as a compatibility fallback so existing production login behavior does not change.
- `ADMIN_PASS` remains the final fallback.

## Behaviour

When auth is configured:
- users see a login page first
- after login they land in the React/FastAPI dashboard
- dashboard actions use the authenticated session user

When auth is not configured:
- the app stays accessible only if `REQUIRE_AUTH` is false

## Operational Management

To remove a user:
- delete that user block from the TOML secret

To rotate a password:
- replace the stored `password`
