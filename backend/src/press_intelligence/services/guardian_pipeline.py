from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any

from press_intelligence.clients.bigquery import BigQueryWarehouse
from press_intelligence.clients.guardian import GuardianContentClient
from press_intelligence.core.config import Settings


class GuardianPipelineService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._guardian = GuardianContentClient(settings)
        self._warehouse = BigQueryWarehouse(settings)

    async def run_recent_ingest(self) -> dict[str, Any]:
        await self._warehouse.ensure_base_resources()
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=1)
        rows = await self._guardian.fetch_range(start, end)
        enriched = [self._with_ingested_at(row) for row in rows]
        inserted = await self._warehouse.load_articles(enriched)
        return {"mode": "recent", "start_date": start.isoformat(), "end_date": end.isoformat(), "inserted": inserted}

    async def run_backfill(self, start_date: str, end_date: str) -> dict[str, Any]:
        await self._warehouse.ensure_base_resources()
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        total = 0
        cursor = start
        while cursor <= end:
            rows = await self._guardian.fetch_range(cursor, cursor)
            total += await self._warehouse.load_articles([self._with_ingested_at(row) for row in rows])
            cursor += timedelta(days=1)
        return {"mode": "backfill", "start_date": start_date, "end_date": end_date, "inserted": total}

    async def run_transforms(self) -> dict[str, Any]:
        materializations = [
            "materializations/analytics/articles_latest.sql",
            "materializations/analytics/article_tags.sql",
            "materializations/analytics/daily_volume.sql",
            "materializations/analytics/section_counts_daily.sql",
            "materializations/analytics/section_daily.sql",
            "materializations/analytics/tag_counts_daily.sql",
            "materializations/analytics/content_freshness.sql",
        ]

        for sql_path in materializations:
            await self._warehouse.execute_sql(sql_path)

        return {
            "mode": "transform",
            "status": "materialized",
            "tables": [
                "analytics.articles_latest",
                "analytics.article_tags",
                "analytics.daily_volume",
                "analytics.section_counts_daily",
                "analytics.section_daily",
                "analytics.tag_counts_daily",
                "analytics.content_freshness",
            ],
        }

    async def run_quality_checks(self) -> dict[str, Any]:
        await self._warehouse.execute_sql(
            "materializations/ops/data_quality_results.sql",
        )
        checks = await self._warehouse.query_from_sql("ops/data_quality.sql")
        return {
            "mode": "quality",
            "status": "materialized",
            "checks": len(checks),
        }

    async def bootstrap(self, start_date: str, end_date: str, force: bool = False) -> dict[str, Any]:
        await self._warehouse.ensure_base_resources()
        existing_rows = await self._warehouse.raw_article_count()
        if existing_rows > 0 and not force:
            return {
                "mode": "bootstrap",
                "status": "skipped",
                "existing_rows": existing_rows,
            }
        backfill = await self.run_backfill(start_date, end_date)
        transforms = await self.run_transforms()
        quality = await self.run_quality_checks()
        return {
            "mode": "bootstrap",
            "status": "loaded",
            "existing_rows": existing_rows,
            **backfill,
            "transforms": transforms["status"],
            "quality": quality["status"],
        }

    def _with_ingested_at(self, row: dict[str, Any]) -> dict[str, Any]:
        return {**row, "ingested_at": datetime.now(timezone.utc).isoformat()}
