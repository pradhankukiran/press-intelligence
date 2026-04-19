# Press Intelligence

Press Intelligence is an editorial analytics and pipeline operations
application for Guardian content flows. It combines:

- `Next.js` frontend for analytics and admin/ops workflows
- `FastAPI` backend with structured JSON logging, request correlation, and live/ready probes
- `Airflow` DAGs for scheduled ingestion and manual backfills
- `BigQuery` integration (real mode) with parameterized SQL and MERGE-based upserts
- Seeded mock data for no-credentials local demos

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

## Health and readiness

- `GET /api/health/live` — always 200 when the process is up. Use this for
  container liveness probes.
- `GET /api/health/ready` — 200 when BigQuery and Airflow are reachable (or
  running in mock mode); 503 otherwise. Use this for readiness probes.
- `GET /api/health` — alias of `/api/health/ready` (back-compat).

## Logging

Structured logs via `structlog`. Configure with:

- `LOG_LEVEL` — `DEBUG`, `INFO` (default), `WARNING`, `ERROR`.
- `LOG_FORMAT` — `console` (default, human-readable) or `json` (ingestible by
  your observability backend).

Every request is assigned a correlation ID (set or accepted via `X-Request-ID`)
and echoed back on the response. Errors return a uniform envelope:

```json
{ "code": "upstream_unavailable", "message": "...", "request_id": "..." }
```

## Testing

```bash
cd backend
uv run python -m pytest --cov=press_intelligence
```

## Caveats

- Mock-mode backfill state is in-memory and does not survive a restart.
- `docker compose` mounts `./secrets` at `/app/secrets`. Set
  `GOOGLE_APPLICATION_CREDENTIALS=/app/secrets/<file>.json` for the container;
  use a host-absolute path for local runs outside docker.
