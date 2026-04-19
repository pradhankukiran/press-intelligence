from __future__ import annotations

from freezegun import freeze_time

from press_intelligence.core.config import Settings
from press_intelligence.services.analytics_service import AnalyticsService
from press_intelligence.services.mock_store import MockStore


class StubWarehouse:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    async def query_from_sql(
        self,
        sql_path: str,
        scalars: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        self.calls.append((sql_path, scalars))
        if sql_path == "analytics/overview_metrics.sql":
            return [
                {
                    "total_articles": 120,
                    "active_sections": 6,
                    "last_sync_at": "2026-03-12T10:00:00Z",
                    "watermark": "2026-03-12T09:55:00Z",
                    "freshness_lag_minutes": 5,
                }
            ]
        if sql_path == "analytics/freshness.sql":
            return [
                {
                    "last_sync_at": "2026-03-12T10:00:00Z",
                    "watermark": "2026-03-12T09:55:00Z",
                    "freshness_lag_minutes": 5,
                }
            ]
        if sql_path == "ops/pipeline_runs.sql":
            return []
        if sql_path == "analytics/publishing_volume.sql":
            return [{"date": "2026-03-11", "value": 17}]
        if sql_path == "analytics/top_sections.sql":
            return [{"section": "world", "count": 40}]
        if sql_path == "analytics/sections.sql":
            return [
                {
                    "date": "2026-03-11",
                    "world": 10,
                    "politics": 5,
                    "business": 4,
                    "culture": 3,
                    "climate": 2,
                    "technology": 1,
                }
            ]
        if sql_path == "analytics/tags.sql":
            return [{"tag": "climate", "count": 15, "momentum": "Live"}]
        return []


def _service(mode: str = "bigquery") -> tuple[AnalyticsService, StubWarehouse]:
    settings = Settings(data_mode=mode, google_cloud_project="p")
    warehouse = StubWarehouse()
    service = AnalyticsService(
        settings=settings,
        warehouse=warehouse,  # type: ignore[arg-type]
        mock_store=MockStore(settings.mock_seed_date),
    )
    return service, warehouse


async def test_overview_bigquery_mode_passes_date_window() -> None:
    service, warehouse = _service("bigquery")
    with freeze_time("2026-03-12"):
        result = await service.get_overview(from_date=None, to_date=None)
    assert result["range"] == "2026-03-01 to 2026-03-12"
    for path, scalars in warehouse.calls:
        if path == "analytics/overview_metrics.sql":
            assert scalars == {"from_date": "2026-03-01", "to_date": "2026-03-12"}


async def test_overview_respects_explicit_dates() -> None:
    service, warehouse = _service("bigquery")
    result = await service.get_overview(from_date="2026-02-01", to_date="2026-02-05")
    assert result["range"] == "2026-02-01 to 2026-02-05"
    for path, scalars in warehouse.calls:
        if path == "analytics/overview_metrics.sql":
            assert scalars == {"from_date": "2026-02-01", "to_date": "2026-02-05"}


async def test_tags_uses_row_limit_scalar() -> None:
    service, warehouse = _service("bigquery")
    with freeze_time("2026-03-12"):
        await service.get_tags(from_date=None, to_date=None, limit=5)
    tag_calls = [c for c in warehouse.calls if c[0] == "analytics/tags.sql"]
    assert tag_calls
    assert tag_calls[0][1]["row_limit"] == 5


async def test_overview_mock_mode_uses_store() -> None:
    service, warehouse = _service("mock")
    result = await service.get_overview(from_date=None, to_date=None)
    assert warehouse.calls == []
    assert "kpis" in result
