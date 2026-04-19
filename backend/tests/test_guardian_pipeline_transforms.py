from __future__ import annotations

from press_intelligence.core.config import Settings
from press_intelligence.services.guardian_pipeline import GuardianPipelineService


class FakeWarehouse:
    def __init__(self, failing_paths: set[str] | None = None) -> None:
        self.failing = failing_paths or set()
        self.executed: list[str] = []

    async def ensure_base_resources(self) -> None:
        return None

    async def execute_sql(self, sql_path: str) -> None:
        self.executed.append(sql_path)
        if sql_path in self.failing:
            raise RuntimeError(f"simulated failure for {sql_path}")


def _service(warehouse: FakeWarehouse) -> GuardianPipelineService:
    settings = Settings(data_mode="bigquery", google_cloud_project="p")
    service = GuardianPipelineService(settings)
    service._warehouse = warehouse  # type: ignore[assignment]
    return service


async def test_transforms_materializes_all_in_order() -> None:
    warehouse = FakeWarehouse()
    service = _service(warehouse)
    result = await service.run_transforms()
    assert result["status"] == "materialized"
    assert all(step["status"] == "ok" for step in result["steps"])
    assert warehouse.executed[0] == "materializations/analytics/articles_latest.sql"


async def test_transforms_halt_downstream_when_parent_fails() -> None:
    warehouse = FakeWarehouse(
        failing_paths={"materializations/analytics/articles_latest.sql"}
    )
    service = _service(warehouse)
    result = await service.run_transforms()
    assert result["status"] == "failed"

    step_by_path = {step["sql_path"]: step for step in result["steps"]}
    assert step_by_path["materializations/analytics/articles_latest.sql"]["status"] == "failed"
    # Direct children skipped
    for child in (
        "materializations/analytics/article_tags.sql",
        "materializations/analytics/daily_volume.sql",
        "materializations/analytics/section_counts_daily.sql",
        "materializations/analytics/content_freshness.sql",
    ):
        assert step_by_path[child]["status"] == "skipped"
    # Indirect descendants also skipped (section_daily, tag_counts_daily)
    assert step_by_path["materializations/analytics/section_daily.sql"]["status"] == "skipped"
    assert step_by_path["materializations/analytics/tag_counts_daily.sql"]["status"] == "skipped"


async def test_transforms_isolated_failure_does_not_halt_unrelated_branches() -> None:
    warehouse = FakeWarehouse(
        failing_paths={"materializations/analytics/article_tags.sql"}
    )
    service = _service(warehouse)
    result = await service.run_transforms()
    step_by_path = {step["sql_path"]: step for step in result["steps"]}
    # tag_counts_daily depends on article_tags -> skipped
    assert step_by_path["materializations/analytics/tag_counts_daily.sql"]["status"] == "skipped"
    # daily_volume / section_counts_daily are siblings, unaffected
    assert step_by_path["materializations/analytics/daily_volume.sql"]["status"] == "ok"
    assert step_by_path["materializations/analytics/section_counts_daily.sql"]["status"] == "ok"
