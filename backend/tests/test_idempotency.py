from __future__ import annotations

from fastapi.testclient import TestClient

from press_intelligence.core.config import get_settings
from press_intelligence.core.dependencies import get_idempotency_cache
from press_intelligence.main import create_app


def test_backfill_idempotent_replay_returns_same_run_id() -> None:
    get_settings.cache_clear()
    get_idempotency_cache.cache_clear()
    client = TestClient(create_app())
    payload = {"start_date": "2026-03-01", "end_date": "2026-03-02"}

    r1 = client.post(
        "/api/ops/backfills",
        json=payload,
        headers={"X-Idempotency-Key": "k1"},
    )
    r2 = client.post(
        "/api/ops/backfills",
        json=payload,
        headers={"X-Idempotency-Key": "k1"},
    )

    assert r1.status_code == 202
    assert r2.status_code == 202
    assert r1.json()["run_id"] == r2.json()["run_id"]


def test_backfill_without_idempotency_key_creates_distinct_runs() -> None:
    get_settings.cache_clear()
    get_idempotency_cache.cache_clear()
    client = TestClient(create_app())
    payload = {"start_date": "2026-03-01", "end_date": "2026-03-02"}

    r1 = client.post("/api/ops/backfills", json=payload)
    r2 = client.post("/api/ops/backfills", json=payload)

    assert r1.json()["run_id"] != r2.json()["run_id"]
