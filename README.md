# Press Intelligence

Press Intelligence is a local-first demo application for
editorial analytics and pipeline operations. It combines:

- `Next.js` frontend for analytics and admin/ops workflows
- `FastAPI` backend for analytics, pipeline, and health APIs
- `Airflow` DAGs for scheduled ingestion/backfills
- `BigQuery` integration for real mode
- seeded mock data for immediate local demos

## Project structure

```text
press-intelligence/
├── backend/
│   ├── airflow/dags
│   ├── src/press_intelligence
│   └── tests
├── frontend/
└── docker-compose.yml
```

## Quick start

1. Copy `.env.example` to `.env`.
2. Keep `DATA_MODE=mock` for a no-credentials local demo.
3. Start the stack:

```bash
docker compose up --build
```

Frontend: `http://localhost:3000`
Backend API: `http://localhost:8000`
Airflow UI: `http://localhost:8080`

## Local development without Docker

Backend:

```bash
cd backend
uv sync
uv run uvicorn press_intelligence.main:app --reload --port 8000
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

## Real integrations

Switch from mock data to real services by setting:

- `DATA_MODE=bigquery`
- `GUARDIAN_API_KEY`
- `GOOGLE_CLOUD_PROJECT`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `BIGQUERY_LOCATION`

Airflow control endpoints use:

- `AIRFLOW_BASE_URL`
- `AIRFLOW_USERNAME`
- `AIRFLOW_PASSWORD`

## Bootstrap BigQuery in real mode

Once real mode env vars are set, initialize datasets/tables and load a first
slice of Guardian data:

```bash
cd backend
set -a && source ../.env && set +a
uv run press-intelligence-bootstrap --days 3
```
