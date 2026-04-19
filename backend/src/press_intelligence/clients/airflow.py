from __future__ import annotations

from typing import Any

import httpx

from press_intelligence.core.config import Settings


class AirflowClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(self._settings.airflow_timeout_seconds)

    async def healthcheck(self) -> str:
        if self._settings.data_mode == "mock":
            return "mock"
        try:
            async with httpx.AsyncClient(
                auth=(self._settings.airflow_username, self._settings.airflow_password),
                timeout=self._timeout(),
            ) as client:
                response = await client.get(f"{self._settings.airflow_base_url}/health")
                response.raise_for_status()
            return "connected"
        except Exception:
            return "degraded"

    async def trigger_dag(self, dag_id: str, conf: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(
            auth=(self._settings.airflow_username, self._settings.airflow_password),
            timeout=self._timeout(),
        ) as client:
            response = await client.post(
                f"{self._settings.airflow_base_url}/dags/{dag_id}/dagRuns",
                json={"conf": conf},
            )
            response.raise_for_status()
            return response.json()

    async def dag_runs(self, dag_id: str, limit: int = 10) -> dict[str, Any]:
        async with httpx.AsyncClient(
            auth=(self._settings.airflow_username, self._settings.airflow_password),
            timeout=self._timeout(),
        ) as client:
            response = await client.get(
                f"{self._settings.airflow_base_url}/dags/{dag_id}/dagRuns",
                params={"limit": limit, "order_by": "-start_date"},
            )
            response.raise_for_status()
            return response.json()
