# Frontend Rewrite

This branch keeps the Streamlit app live and adds a parallel React interface.

## Local Development

Run the existing Streamlit app exactly as before:

```bash
streamlit run app.py
```

Run the new API adapter:

```bash
uvicorn api.main:app --reload --port 8000
```

Run the React app:

```bash
cd frontend
npm install
npm run dev
```

Open:

```text
http://localhost:5173
```

The Vite dev server proxies `/api/*` requests to `http://127.0.0.1:8000`.

## Scope

The Python business logic remains the source of truth. The new `api/` package is a browser-facing adapter around the current BigQuery dashboard and snooze flows.

Current migrated pages:

- Margin Dashboard
- Alerts Dashboard

Next migration slices:

- Trafficking to Asana Gmail fetch, parse preview, Asana dedupe, and CSV downloads
- Automation run triggers as background jobs with status logs

## Hosting Recommendation

Recommended production target: Google Cloud Run.

Reasons:

- The app already depends on Google Cloud BigQuery.
- Secrets can live in Google Secret Manager instead of a Streamlit TOML file.
- The API can run with a service account that has the exact BigQuery permissions needed.
- The React build can either be served by the same FastAPI container or deployed separately as static hosting.

Preferred deployment shape:

- Single Cloud Run service for the first production version.
- FastAPI serves `/api/*`.
- FastAPI also serves the built React assets from `frontend/dist`.
- Secrets come from Secret Manager or Cloud Run environment variables.

Alternative:

- Vercel or Netlify for the React frontend.
- Cloud Run for the FastAPI API.
- This is fine, but it creates CORS, auth cookie, and deployment coordination work that is not useful until the app needs public edge hosting.

For this project, start with one Cloud Run service. Split the frontend later only if there is a clear operational reason.
