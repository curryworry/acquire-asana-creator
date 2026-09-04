# Frontend Architecture

The production app is a React frontend served by a FastAPI backend from the same Cloud Run service.

## Local Development

Run the API:

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

## Current Pages

- Margin dashboard
- Pacing dashboard
- Alerts dashboard
- QA: Video on TradeMe
- Admin automation triggers

## Hosting

Production runs on Google Cloud Run:
- FastAPI serves `/api/*`.
- FastAPI serves built React assets from `frontend/dist`.
- Secrets are provided by Secret Manager or Cloud Run environment variables.

The public URL is:
- `https://ops.acquire.agency`
