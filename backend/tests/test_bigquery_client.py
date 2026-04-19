from __future__ import annotations

from datetime import datetime

from press_intelligence.clients.bigquery import BigQueryWarehouse
from press_intelligence.core.config import Settings


def _warehouse() -> BigQueryWarehouse:
    settings = Settings(
        data_mode="bigquery",
        google_cloud_project="test-project",
    )
    return BigQueryWarehouse(settings)


def test_identifier_params_exposes_dataset_names() -> None:
    w = _warehouse()
    params = w._identifier_params()
    assert params["google_cloud_project"] == "test-project"
    assert params["bigquery_dataset_raw"] == "raw_guardian"
    assert params["bigquery_dataset_analytics"] == "analytics"
    assert params["bigquery_dataset_ops"] == "ops"


def test_build_query_params_infers_types() -> None:
    w = _warehouse()
    params = w._build_query_params({"from_date": "2026-03-01", "row_limit": 10})
    by_name = {p.name: p for p in params}
    assert by_name["from_date"].type_ == "STRING"
    assert by_name["from_date"].value == "2026-03-01"
    assert by_name["row_limit"].type_ == "INT64"
    assert by_name["row_limit"].value == 10


def test_build_query_params_handles_bool_and_float() -> None:
    w = _warehouse()
    params = w._build_query_params({"enabled": True, "ratio": 0.5})
    by_name = {p.name: p for p in params}
    assert by_name["enabled"].type_ == "BOOL"
    assert by_name["enabled"].value is True
    assert by_name["ratio"].type_ == "FLOAT64"
    assert by_name["ratio"].value == 0.5


def test_build_query_params_empty_returns_empty_list() -> None:
    w = _warehouse()
    assert w._build_query_params(None) == []
    assert w._build_query_params({}) == []


def test_normalize_pipeline_run_row_stringifies_datetimes() -> None:
    w = _warehouse()
    normalized = w._normalize_pipeline_run_row(
        {
            "run_id": 1,
            "dag_id": "dag-a",
            "status": "success",
            "trigger": "manual",
            "started_at": datetime(2026, 3, 12, 10, 0, 0),
            "finished_at": None,
            "window": "2026-03-01 to 2026-03-02",
            "error_summary": None,
        }
    )
    assert normalized["run_id"] == "1"
    assert normalized["started_at"].startswith("2026-03-12T10:00:00")
    assert normalized["finished_at"] is None
