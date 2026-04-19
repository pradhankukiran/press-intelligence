from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import structlog

from press_intelligence.clients.airflow import AirflowClient
from press_intelligence.clients.bigquery import BigQueryWarehouse
from press_intelligence.core.config import Settings
from press_intelligence.models.schemas import BackfillRequest
from press_intelligence.services.mock_store import MockStore

logger = structlog.get_logger(__name__)


class OpsService:
    def __init__(
        self,
        settings: Settings,
        airflow: AirflowClient,
        warehouse: BigQueryWarehouse,
        mock_store: MockStore,
    ) -> None:
        self._settings = settings
        self._airflow = airflow
        self._warehouse = warehouse
        self._mock_store = mock_store

    async def health(self) -> dict[str, object]:
        warehouse_status = await self._warehouse.healthcheck()
        airflow_status = await self._airflow.healthcheck()
        status = "ok" if "degraded" not in {warehouse_status, airflow_status} else "degraded"
        return {
            "status": status,
            "mode": self._settings.data_mode,
            "services": {
                "bigquery": warehouse_status,
                "airflow": airflow_status,
                "guardian": "configured" if self._settings.guardian_api_key else "mock",
            },
        }

    async def status(self) -> dict[str, object]:
        if self._settings.data_mode == "mock":
            return self._mock_store.status()

        airflow_status = await self._airflow.healthcheck()
        metrics_rows = await self._warehouse.query_from_sql(
            "analytics/freshness.sql",
        )
        quality_rows = await self._warehouse.query_from_sql("ops/data_quality.sql")
        metrics = metrics_rows[0] if metrics_rows else {}
        lag_minutes = metrics.get("freshness_lag_minutes")

        return {
            "environment": self._settings.app_env,
            "mode": self._settings.data_mode,
            "last_sync_at": metrics.get("last_sync_at") or "No data",
            "freshness_lag": f"{lag_minutes} min" if lag_minutes is not None else "No data",
            "watermark": metrics.get("watermark") or "No data",
            "dags": [
                {
                    "id": self._settings.airflow_recent_dag_id,
                    "status": "healthy" if airflow_status == "connected" else "degraded",
                },
                {
                    "id": self._settings.airflow_backfill_dag_id,
                    "status": "healthy" if airflow_status == "connected" else "degraded",
                },
            ],
            "checks": [
                {
                    "name": row["name"],
                    "status": row["status"],
                    "observed_value": str(row["observed_value"]),
                    "threshold": str(row["threshold"]),
                    "detail": row["detail"],
                }
                for row in quality_rows
            ],
        }

    async def runs(self, limit: int) -> dict[str, object]:
        if self._settings.data_mode == "mock":
            return self._mock_store.runs(limit)
        await self._sync_pipeline_runs(limit=max(limit, 25))
        rows = await self._warehouse.query_from_sql(
            "ops/pipeline_runs.sql",
            scalars={"row_limit": limit},
        )
        return {"runs": [self._serialize_pipeline_run(row) for row in rows]}

    async def trigger_backfill(self, request: BackfillRequest) -> dict[str, object]:
        if self._settings.data_mode == "mock":
            return self._mock_store.trigger_backfill(request)
        response = await self._airflow.trigger_dag(
            self._settings.airflow_backfill_dag_id,
            conf={
                "start_date": request.start_date,
                "end_date": request.end_date,
            },
        )
        await self._warehouse.upsert_pipeline_runs(
            [
                {
                    "run_id": response.dag_run_id,
                    "dag_id": self._settings.airflow_backfill_dag_id,
                    "status": self._normalize_state(response.state),
                    "trigger": "manual",
                    "started_at": response.logical_date
                    or datetime.now(timezone.utc).isoformat(),
                    "finished_at": response.end_date,
                    "window": self._window_for_backfill_conf(
                        {
                            "start_date": request.start_date,
                            "end_date": request.end_date,
                        }
                    ),
                    "error_summary": None,
                }
            ]
        )
        return {
            "run_id": response.dag_run_id,
            "dag_id": self._settings.airflow_backfill_dag_id,
            "status": self._normalize_state(response.state),
            "message": "Backfill queued in Airflow.",
        }

    async def backfill_status(self, run_id: str) -> dict[str, object] | None:
        if self._settings.data_mode == "mock":
            return self._mock_store.backfill_status(run_id)
        await self._sync_pipeline_runs(limit=100)
        rows = await self._warehouse.query_from_sql(
            "ops/pipeline_runs.sql",
            scalars={"row_limit": 200},
        )
        for row in rows:
            if row["run_id"] == run_id and row["dag_id"] == self._settings.airflow_backfill_dag_id:
                return self._serialize_pipeline_run(row)
        return None

    async def _sync_pipeline_runs(self, limit: int) -> None:
        import httpx

        pipeline_runs: list[dict[str, Any]] = []
        for dag_id, trigger in (
            (self._settings.airflow_recent_dag_id, "system"),
            (self._settings.airflow_backfill_dag_id, "manual"),
        ):
            try:
                runs = await self._airflow.dag_runs(dag_id, limit=limit)
            except httpx.HTTPError as exc:
                logger.warning(
                    "ops.sync.dag_runs.failed",
                    dag_id=dag_id,
                    exc_info=exc,
                )
                continue
            pipeline_runs.extend(
                self._normalize_airflow_run(row, trigger) for row in runs
            )
        if pipeline_runs:
            await self._warehouse.upsert_pipeline_runs(pipeline_runs)

    def _normalize_airflow_run(self, row: Any, trigger: str) -> dict[str, Any]:
        return {
            "run_id": row.dag_run_id,
            "dag_id": row.dag_id,
            "status": self._normalize_state(row.state),
            "trigger": trigger,
            "started_at": row.start_date or row.logical_date,
            "finished_at": row.end_date,
            "window": self._window_for_run(row, trigger),
            "error_summary": None,
        }

    def _window_for_run(self, row: Any, trigger: str) -> str:
        if trigger == "manual":
            return self._window_for_backfill_conf(row.conf)
        return "recent ingest"

    def _window_for_backfill_conf(self, conf: Any) -> str:
        if not isinstance(conf, dict):
            return "manual backfill"
        start_date = conf.get("start_date")
        end_date = conf.get("end_date")
        if start_date and end_date:
            return f"{start_date} to {end_date}"
        return "manual backfill"

    def _normalize_state(self, state: str | None) -> str:
        if state in {"success", "failed", "running", "queued", "scheduled"}:
            return state
        return "scheduled"

    def _serialize_pipeline_run(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "dag_id": row["dag_id"],
            "status": self._normalize_state(row.get("status")),
            "trigger": row["trigger"],
            "started_at": self._serialize_datetime(row.get("started_at")),
            "finished_at": self._serialize_datetime(row.get("finished_at")),
            "window": row.get("window") or row.get("run_window") or "Unknown",
            "error_summary": row.get("error_summary"),
        }

    def _serialize_datetime(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.isoformat()
        return str(value)
