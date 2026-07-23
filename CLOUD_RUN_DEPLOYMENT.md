# Cloud Run Deployment

Target project:

- `ops-control-center-503319`

Project number:

- `679417827301`

Initial region:

- `australia-southeast1`

Initial service:

- `acquire-ops`

## Current Hosting Shape

- Cloud Run serves the FastAPI app.
- FastAPI serves `/api/*`.
- FastAPI also serves the built React app from `frontend/dist`.
- GitHub Actions continues to run the existing scheduled email/automation jobs.

## Required Local gcloud Context

Use the Acquire account:

```bash
gcloud auth login ashwin@acquirenz.com
gcloud config set account ashwin@acquirenz.com
```

Do not rely on the global gcloud project. Use `--project ops-control-center-503319` on deployment commands.

Check access:

```bash
gcloud projects describe ops-control-center-503319 \
  --account ashwin@acquirenz.com \
  --format='text(projectId,name,lifecycleState,projectNumber)'
```

## Enable APIs

```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  secretmanager.googleapis.com \
  iamcredentials.googleapis.com \
  --project ops-control-center-503319
```

## Runtime Service Account

Create one dedicated service account for the Cloud Run app:

```bash
gcloud iam service-accounts create acquire-ops-runner \
  --project ops-control-center-503319 \
  --display-name "Acquire Ops Cloud Run runtime"
```

Service account email:

```text
acquire-ops-runner@ops-control-center-503319.iam.gserviceaccount.com
```

## BigQuery Access

The app reads and writes BigQuery data in:

```text
sm-test-391201.supermetrics_data
```

Grant the Cloud Run service account permission to run BigQuery jobs in the source project:

```bash
gcloud projects add-iam-policy-binding sm-test-391201 \
  --member serviceAccount:acquire-ops-runner@ops-control-center-503319.iam.gserviceaccount.com \
  --role roles/bigquery.jobUser
```

Grant dataset-level write access. This is required because the dashboard creates/updates control tables and writes snooze/dismiss actions.

The local `bq add-iam-policy-binding` command may not support dataset IAM in every environment. If it fails, add this member in the BigQuery console on the `supermetrics_data` dataset:

```text
serviceAccount:acquire-ops-runner@ops-control-center-503319.iam.gserviceaccount.com
```

Dataset role:

```text
BigQuery Data Editor
```

## Secrets

Use Google Secret Manager in `ops-control-center-503319`.

### App Login Users

The production app uses the same TOML login format as `.streamlit/secrets.toml`.

Create/update the production TOML secret from the local file:

```bash
gcloud secrets create streamlit-secrets-toml \
  --project ops-control-center-503319 \
  --data-file=.streamlit/secrets.toml
```

For later password/user changes, edit `.streamlit/secrets.toml` locally and add a new secret version:

```bash
gcloud secrets versions add streamlit-secrets-toml \
  --project ops-control-center-503319 \
  --data-file=.streamlit/secrets.toml
```

This keeps production login management in one Google Secret Manager secret while preserving the existing TOML format.

### Session Signing Secret

Generate a random value:

```bash
openssl rand -base64 32
```

Store it:

```bash
printf 'PASTE_GENERATED_VALUE_HERE' | gcloud secrets create api-session-secret \
  --project ops-control-center-503319 \
  --data-file=-
```

### Admin Password

Choose and store the admin password used for dismiss actions:

```bash
printf 'PASTE_ADMIN_PASSWORD_HERE' | gcloud secrets create admin-pass \
  --project ops-control-center-503319 \
  --data-file=-
```

## Deploy

Deploys are automatic on pushes to `main` via:

```text
.github/workflows/deploy_cloud_run.yml
```

The workflow uses GitHub OIDC / Google Workload Identity Federation, not a long-lived service account key.

The workflow does not pass `--allow-unauthenticated`. Public access is a service-level IAM setting that was applied during the first manual deploy. Future automated deploys only create new revisions and route traffic.

Google identity used by GitHub Actions:

```text
github-actions-deployer@ops-control-center-503319.iam.gserviceaccount.com
```

Workload identity provider:

```text
projects/679417827301/locations/global/workloadIdentityPools/github-actions/providers/github
```

Manual fallback deploy from the repo root:

```bash
gcloud run deploy acquire-ops \
  --project ops-control-center-503319 \
  --region australia-southeast1 \
  --source . \
  --service-account acquire-ops-runner@ops-control-center-503319.iam.gserviceaccount.com \
  --allow-unauthenticated \
  --set-env-vars REQUIRE_AUTH=true,BQ_PROJECT_ID=sm-test-391201,BQ_DATASET=supermetrics_data \
  --set-secrets STREAMLIT_SECRETS_TOML=streamlit-secrets-toml:latest,API_SESSION_SECRET=api-session-secret:latest,ADMIN_PASS=admin-pass:latest
```

`--allow-unauthenticated` is intentional for the first version because the app itself enforces username/password login. Do not remove `REQUIRE_AUTH=true`.

## Smoke Test

After deploy:

```bash
gcloud run services describe acquire-ops \
  --project ops-control-center-503319 \
  --region australia-southeast1 \
  --format='value(status.url)'
```

Open the returned URL and log in with one of the TOML users.

Health check:

```bash
curl "$(gcloud run services describe acquire-ops \
  --project ops-control-center-503319 \
  --region australia-southeast1 \
  --format='value(status.url)')/api/health"
```

## Email Links

Once the Cloud Run URL is confirmed, update the GitHub Actions variable/secret:

```text
ALERT_DASHBOARD_BASE_URL=https://...
```

This makes alert email links open the hosted React/FastAPI interface.
