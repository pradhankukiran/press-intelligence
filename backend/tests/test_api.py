from fastapi.testclient import TestClient

from press_intelligence.main import create_app


client = TestClient(create_app())


def test_health_endpoint_returns_mock_services() -> None:
    response = client.get("/api/health")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ok"
    assert payload["services"]["bigquery"] == "mock"


def test_overview_endpoint_returns_kpis() -> None:
    response = client.get("/api/analytics/overview")

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["kpis"]) == 4
    assert payload["daily_volume"][0]["date"] == "2026-03-01"


def test_backfill_flow_adds_manual_run() -> None:
    create_response = client.post(
        "/api/ops/backfills",
        json={"start_date": "2026-03-01", "end_date": "2026-03-03"},
    )

    assert create_response.status_code == 202
    run_id = create_response.json()["run_id"]

    status_response = client.get(f"/api/ops/backfills/{run_id}")

    assert status_response.status_code == 200
    payload = status_response.json()
    assert payload["run_id"] == run_id
    assert payload["status"] == "queued"
