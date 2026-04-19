from __future__ import annotations

import time
from datetime import UTC, date, datetime, timedelta
from typing import Any

import structlog

from press_intelligence.clients.bigquery import BigQueryWarehouse
from press_intelligence.clients.guardian import GuardianContentClient
from press_intelligence.core.config import Settings

logger = structlog.get_logger(__name__)

MATERIALIZATIONS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("materializations/analytics/articles_latest.sql", ()),
    (
        "materializations/analytics/article_tags.sql",
        ("materializations/analytics/articles_latest.sql",),
    ),
    (
        "materializations/analytics/daily_volume.sql",
        ("materializations/analytics/articles_latest.sql",),
    ),
    (
        "materializations/analytics/section_counts_daily.sql",
        ("materializations/analytics/articles_latest.sql",),
    ),
    (
        "materializations/analytics/section_daily.sql",
        ("materializations/analytics/section_counts_daily.sql",),
    ),
    (
        "materializations/analytics/tag_counts_daily.sql",
        ("materializations/analytics/article_tags.sql",),
    ),
    (
        "materializations/analytics/content_freshness.sql",
        ("materializations/analytics/articles_latest.sql",),
    ),
)


class GuardianPipelineService:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._guardian = GuardianContentClient(settings)
        self._warehouse = BigQueryWarehouse(settings)

    async def run_recent_ingest(self) -> dict[str, Any]:
        await self._warehouse.ensure_base_resources()
        end = datetime.now(UTC).date()
        watermark = await self._warehouse.latest_ingested_at()
        if watermark is not None:
            start = watermark.date()
            watermark_source = "warehouse"
        else:
            start = end - timedelta(days=1)
            watermark_source = "default_24h"
        logger.info(
            "pipeline.recent.window",
            start=start.isoformat(),
            end=end.isoformat(),
            watermark_source=watermark_source,
        )
        rows = await self._guardian.fetch_range(start, end)
        enriched = [self._with_ingested_at(row) for row in rows]
        result = await self._warehouse.load_articles(enriched)
        return {
            "mode": "recent",
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "watermark_source": watermark_source,
            "loaded": result["loaded"],
            "rejected": result["rejected"],
        }

    async def run_backfill(self, start_date: str, end_date: str) -> dict[str, Any]:
        await self._warehouse.ensure_base_resources()
        start = date.fromisoformat(start_date)
        end = date.fromisoformat(end_date)
        loaded = 0
        rejected = 0
        cursor = start
        while cursor <= end:
            rows = await self._guardian.fetch_range(cursor, cursor)
            result = await self._warehouse.load_articles(
                [self._with_ingested_at(row) for row in rows]
            )
            loaded += result["loaded"]
            rejected += result["rejected"]
            cursor += timedelta(days=1)
        return {
            "mode": "backfill",
            "start_date": start_date,
            "end_date": end_date,
            "loaded": loaded,
            "rejected": rejected,
        }

    async def run_transforms(self) -> dict[str, Any]:
        steps: list[dict[str, Any]] = []
        failed_paths: set[str] = set()

        for sql_path, deps in MATERIALIZATIONS:
            blocking = [dep for dep in deps if dep in failed_paths]
            if blocking:
                logger.warning(
                    "pipeline.materialization.skipped",
                    sql_path=sql_path,
                    blocked_by=blocking,
                )
                steps.append(
                    {
                        "sql_path": sql_path,
                        "status": "skipped",
                        "blocked_by": blocking,
                    }
                )
                failed_paths.add(sql_path)
                continue

            started = time.perf_counter()
            try:
                await self._warehouse.execute_sql(sql_path)
            except Exception as exc:
                duration_ms = round((time.perf_counter() - started) * 1000, 2)
                logger.warning(
                    "pipeline.materialization.failed",
                    sql_path=sql_path,
                    duration_ms=duration_ms,
                    exc_info=exc,
                )
                steps.append(
                    {
                        "sql_path": sql_path,
                        "status": "failed",
                        "duration_ms": duration_ms,
                        "error": str(exc),
                    }
                )
                failed_paths.add(sql_path)
                continue
            duration_ms = round((time.perf_counter() - started) * 1000, 2)
            logger.info(
                "pipeline.materialization.executed",
                sql_path=sql_path,
                duration_ms=duration_ms,
            )
            steps.append(
                {
                    "sql_path": sql_path,
                    "status": "ok",
                    "duration_ms": duration_ms,
                }
            )

        any_failed = any(step["status"] == "failed" for step in steps)
        overall = "failed" if any_failed else "materialized"
        return {
            "mode": "transform",
            "status": overall,
            "steps": steps,
        }

    async def run_quality_checks(self) -> dict[str, Any]:
        await self._warehouse.execute_sql(
            "materializations/ops/data_quality_results.sql",
            scalars={
                "missing_section_threshold": self._settings.quality_missing_section_threshold,
                "missing_published_at_threshold": self._settings.quality_missing_published_at_threshold,
            },
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
        return {**row, "ingested_at": datetime.now(UTC).isoformat()}
