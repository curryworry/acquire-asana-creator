# Dashboard Auth

Last updated: 2026-06-15

## Purpose

The app can use a simple built-in username/password login for dashboard access.

This is intended for:
- small internal teams
- fewer than 10 users
- basic auditability for snoozes and similar actions

## How It Works

- users are defined in `.streamlit/secrets.toml`
- passwords are stored in plain text in `.streamlit/secrets.toml`
- once signed in, the username/display name is kept in Streamlit session state
- dashboard actions use the authenticated user automatically

## Secrets Format

Add entries like this to `.streamlit/secrets.toml`:

```toml
[auth.users.ashwin]
display_name = "Ashwin"
password = "ashwin-password"

[auth.users.rachel]
display_name = "Rachel Burns"
password = "rachel-password"
```

Notes:
- `ashwin` / `rachel` are the login usernames
- `display_name` is what appears in the app audit trail
- `password` is the plain-text password used for login

## Behaviour

When auth is configured:
- users see a login page first
- after login they land on a home page
- from there they can open:
  - Margin Dashboard
  - Alerts Dashboard
  - Pacing Dashboard

When auth is not configured:
- the app stays accessible in preview mode
- it shows an informational message that auth is not enabled

## Notes On Existing Alerts Email Links

- signed alert links still work
- if auth is enabled, the user must log in first
- after login, the signed link context can still be opened
- audit actions use the authenticated session user, not the email link recipient

## Operational Management

To remove a user:
- delete that user block from `secrets.toml`

To rotate a password:
- replace the stored `password`
