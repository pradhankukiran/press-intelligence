from __future__ import annotations

from typing import Any

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, Field

from press_intelligence.clients._retry import retry_http
from press_intelligence.core.config import Settings

logger = structlog.get_logger(__name__)


class AirflowDagRun(BaseModel):
    model_config = ConfigDict(extra="ignore")

    dag_run_id: str
    dag_id: str
    state: str | None = None
    start_date: str | None = None
    end_date: str | None = None
    logical_date: str | None = None
    conf: dict[str, Any] = Field(default_factory=dict)


class AirflowClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client: httpx.AsyncClient | None = None

    def _build_client(self, timeout: float | None = None) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            auth=(self._settings.airflow_username, self._settings.airflow_password),
            timeout=httpx.Timeout(timeout or self._settings.airflow_timeout_seconds),
        )

    def _shared_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = self._build_client()
        return self._client

    async def aclose(self) -> None:
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()

    async def healthcheck(self) -> str:
        if self._settings.data_mode == "mock":
            return "mock"
        try:
            async with self._build_client(
                timeout=self._settings.airflow_health_timeout_seconds
            ) as client:
                response = await client.get(f"{self._settings.airflow_base_url}/health")
                response.raise_for_status()
            return "connected"
        except httpx.HTTPError as exc:
            logger.warning("airflow.health.degraded", exc_info=exc)
            return "degraded"

    async def trigger_dag(self, dag_id: str, conf: dict[str, Any]) -> AirflowDagRun:
        client = self._shared_client()

        @retry_http("airflow")
        async def _call() -> httpx.Response:
            return await client.post(
                f"{self._settings.airflow_base_url}/dags/{dag_id}/dagRuns",
                json={"conf": conf},
            )

        response = await _call()

        if response.status_code == 409:
            logger.info("airflow.trigger.duplicate", dag_id=dag_id)
            payload = response.json()
            return AirflowDagRun(
                dag_run_id=str(payload.get("detail") or payload.get("dag_run_id") or "duplicate"),
                dag_id=dag_id,
                state="queued",
                conf=conf,
            )
        response.raise_for_status()
        return AirflowDagRun.model_validate(response.json())

    async def dag_runs(self, dag_id: str, limit: int = 10) -> list[AirflowDagRun]:
        client = self._shared_client()

        @retry_http("airflow")
        async def _call() -> httpx.Response:
            return await client.get(
                f"{self._settings.airflow_base_url}/dags/{dag_id}/dagRuns",
                params={"limit": limit, "order_by": "-start_date"},
            )

        response = await _call()
        response.raise_for_status()
        payload = response.json()
        return [
            AirflowDagRun.model_validate(row)
            for row in payload.get("dag_runs", []) or []
        ]
