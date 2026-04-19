# Press Intelligence Backend

FastAPI, shared ingestion services, and Airflow DAGs for Press Intelligence.

## Run locally

```bash
uv sync
uv run uvicorn press_intelligence.main:app --reload --port 8000
```

The backend defaults to `DATA_MODE=mock`, so it serves seeded analytics and ops
data without requiring Guardian, BigQuery, or Airflow credentials.

## Real integrations

Set these environment variables to switch to live services:

- `DATA_MODE=bigquery`
- `GUARDIAN_API_KEY`
- `GOOGLE_CLOUD_PROJECT`
- `GOOGLE_APPLICATION_CREDENTIALS`
- `AIRFLOW_BASE_URL`
- `AIRFLOW_USERNAME`
- `AIRFLOW_PASSWORD`
