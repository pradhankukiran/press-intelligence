from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from press_intelligence.core.config import get_settings
from press_intelligence.main import create_app


@pytest.fixture(scope="module")
def client() -> TestClient:
    get_settings.cache_clear()
    return TestClient(create_app())


def test_health_live_returns_200(client: TestClient) -> None:
    r = client.get("/api/health/live")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_health_live_echoes_request_id(client: TestClient) -> None:
    r = client.get("/api/health/live", headers={"X-Request-ID": "test-rid-123"})
    assert r.headers["X-Request-ID"] == "test-rid-123"


def test_health_ready_mock_mode_returns_200(client: TestClient) -> None:
    r = client.get("/api/health/ready")
    assert r.status_code == 200


def test_analytics_overview_response_shape(client: TestClient) -> None:
    r = client.get("/api/analytics/overview")
    assert r.status_code == 200
    body = r.json()
    assert "range" in body
    assert isinstance(body["kpis"], list)
    assert all("section" in item and "count" in item for item in body["top_sections"])


def test_ops_status_response_shape(client: TestClient) -> None:
    r = client.get("/api/ops/status")
    assert r.status_code == 200
    body = r.json()
    assert "dags" in body
    assert all({"id", "status"} <= row.keys() for row in body["dags"])


def test_ops_status_shows_only_two_dags_in_mock(client: TestClient) -> None:
    r = client.get("/api/ops/status")
    assert r.status_code == 200
    body = r.json()
    dag_ids = {row["id"] for row in body["dags"]}
    assert dag_ids == {"guardian_ingest_recent", "guardian_backfill_range"}


def test_backfill_invalid_payload_returns_envelope(client: TestClient) -> None:
    r = client.post(
        "/api/ops/backfills",
        json={"start_date": "not-a-date", "end_date": "2026-03-01"},
    )
    assert r.status_code == 422
    body = r.json()
    assert body["code"] == "validation_error"
    assert "details" in body


def test_backfill_missing_run_returns_404_envelope(client: TestClient) -> None:
    r = client.get("/api/ops/backfills/does-not-exist")
    assert r.status_code == 404
    body = r.json()
    assert body["code"] == "run_not_found"
