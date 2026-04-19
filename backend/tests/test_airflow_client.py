from __future__ import annotations

import httpx
import pytest
import respx

from press_intelligence.clients.airflow import AirflowClient, AirflowDagRun
from press_intelligence.core.config import Settings


def _settings() -> Settings:
    return Settings(
        data_mode="bigquery",
        google_cloud_project="p",
        airflow_base_url="http://airflow.test/api/v1",
    )


async def test_trigger_dag_happy_path() -> None:
    client = AirflowClient(_settings())
    try:
        with respx.mock(base_url="http://airflow.test/api/v1") as router:
            router.post("/dags/dag-a/dagRuns").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "dag_run_id": "run-1",
                        "dag_id": "dag-a",
                        "state": "queued",
                        "logical_date": "2026-03-12T10:00:00+00:00",
                        "conf": {},
                    },
                )
            )
            result = await client.trigger_dag("dag-a", {})
        assert isinstance(result, AirflowDagRun)
        assert result.dag_run_id == "run-1"
        assert result.state == "queued"
    finally:
        await client.aclose()


async def test_trigger_dag_409_treated_as_duplicate_success() -> None:
    client = AirflowClient(_settings())
    try:
        with respx.mock(base_url="http://airflow.test/api/v1") as router:
            router.post("/dags/dag-a/dagRuns").mock(
                return_value=httpx.Response(
                    409,
                    json={"detail": "DagRun already exists"},
                )
            )
            result = await client.trigger_dag("dag-a", {"x": 1})
        assert result.dag_id == "dag-a"
        assert result.state == "queued"
    finally:
        await client.aclose()


async def test_dag_runs_parses_list() -> None:
    client = AirflowClient(_settings())
    try:
        with respx.mock(base_url="http://airflow.test/api/v1") as router:
            router.get("/dags/dag-a/dagRuns").mock(
                return_value=httpx.Response(
                    200,
                    json={
                        "dag_runs": [
                            {
                                "dag_run_id": "r1",
                                "dag_id": "dag-a",
                                "state": "success",
                                "start_date": "2026-03-12T00:00:00+00:00",
                            },
                            {
                                "dag_run_id": "r2",
                                "dag_id": "dag-a",
                                "state": "running",
                                "start_date": "2026-03-12T01:00:00+00:00",
                            },
                        ]
                    },
                )
            )
            runs = await client.dag_runs("dag-a", limit=10)
        assert len(runs) == 2
        assert runs[0].dag_run_id == "r1"
        assert runs[1].state == "running"
    finally:
        await client.aclose()


async def test_healthcheck_degraded_on_http_error() -> None:
    client = AirflowClient(_settings())
    try:
        with respx.mock(base_url="http://airflow.test/api/v1") as router:
            router.get("/health").mock(
                side_effect=httpx.ConnectError("connection refused")
            )
            status = await client.healthcheck()
        assert status == "degraded"
    finally:
        await client.aclose()
