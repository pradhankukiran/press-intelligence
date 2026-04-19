from __future__ import annotations

from typing import Any

from press_intelligence.clients.bigquery import BigQueryWarehouse
from press_intelligence.core.config import Settings
from press_intelligence.services.mock_store import MockStore


class AnalyticsService:
    def __init__(
        self,
        settings: Settings,
        warehouse: BigQueryWarehouse,
        mock_store: MockStore,
    ) -> None:
        self._settings = settings
        self._warehouse = warehouse
        self._mock_store = mock_store

    async def get_overview(self, from_date: str | None, to_date: str | None) -> dict[str, object]:
        if self._settings.data_mode == "mock":
            return self._mock_store.overview()
        metrics_rows = await self._warehouse.query_from_sql(
            "analytics/overview_metrics.sql",
            params=self._date_params(from_date, to_date),
        )
        freshness_rows = await self._warehouse.query_from_sql("analytics/freshness.sql")
        run_rows = await self._warehouse.query_from_sql("ops/pipeline_runs.sql", {"limit": 50})
        volume_rows = await self._warehouse.query_from_sql(
            "analytics/publishing_volume.sql",
            params=self._date_params(from_date, to_date),
        )
        top_section_rows = await self._warehouse.query_from_sql(
            "analytics/top_sections.sql",
            params=self._date_params(from_date, to_date),
        )

        metrics = metrics_rows[0] if metrics_rows else {}
        freshness = freshness_rows[0] if freshness_rows else metrics
        lag_minutes = freshness.get("freshness_lag_minutes")
        lag_label = f"{lag_minutes} min" if lag_minutes is not None else "No data"
        recent_failures = sum(1 for row in run_rows if row.get("status") == "failed")

        return {
            "range": self._range_label(from_date, to_date),
            "kpis": [
                {
                    "label": "Guardian articles",
                    "value": f"{int(metrics.get('total_articles', 0)):,}",
                    "delta": "Deduped warehouse",
                    "tone": "neutral",
                },
                {
                    "label": "Sections covered",
                    "value": str(int(metrics.get("active_sections", 0))),
                    "delta": "Current range",
                    "tone": "neutral",
                },
                {
                    "label": "Freshness lag",
                    "value": lag_label,
                    "delta": "Current sync",
                    "tone": "neutral",
                },
                {
                    "label": "Recent failures",
                    "value": str(recent_failures),
                    "delta": "Persisted run history",
                    "tone": "neutral",
                },
            ],
            "daily_volume": [
                {"date": row["date"], "value": int(row["value"])} for row in volume_rows
            ],
            "freshness": {
                "last_sync_at": freshness.get("last_sync_at") or "No data",
                "watermark": freshness.get("watermark") or "No data",
                "lag": lag_label,
            },
            "top_sections": [
                {"section": row["section"], "count": int(row["count"])} for row in top_section_rows
            ],
        }

    async def get_sections(self, from_date: str | None, to_date: str | None) -> dict[str, object]:
        if self._settings.data_mode == "mock":
            return self._mock_store.sections()
        rows = await self._warehouse.query_from_sql(
            "analytics/sections.sql",
            params=self._date_params(from_date, to_date),
        )
        leaders = await self._warehouse.query_from_sql(
            "analytics/top_sections.sql",
            params=self._date_params(from_date, to_date),
        )
        return {
            "range": self._range_label(from_date, to_date),
            "series": [
                {
                    "date": row["date"],
                    "world": int(row["world"]),
                    "politics": int(row["politics"]),
                    "business": int(row["business"]),
                    "culture": int(row["culture"]),
                    "climate": int(row["climate"]),
                    "technology": int(row["technology"]),
                }
                for row in rows
            ],
            "leaders": [
                {"section": row["section"], "count": int(row["count"])} for row in leaders
            ],
        }

    async def get_tags(
        self,
        from_date: str | None,
        to_date: str | None,
        limit: int,
    ) -> dict[str, object]:
        if self._settings.data_mode == "mock":
            return self._mock_store.tags(limit)
        rows = await self._warehouse.query_from_sql(
            "analytics/tags.sql",
            params={**self._date_params(from_date, to_date), "limit": limit},
        )
        return {
            "range": self._range_label(from_date, to_date),
            "tags": [
                {
                    "tag": row["tag"],
                    "count": int(row["count"]),
                    "momentum": row.get("momentum") or "Live",
                }
                for row in rows
            ],
        }

    async def get_publishing_volume(
        self,
        from_date: str | None,
        to_date: str | None,
        granularity: str,
    ) -> dict[str, object]:
        if self._settings.data_mode == "mock":
            payload = self._mock_store.publishing_volume()
            payload["granularity"] = granularity
            return payload
        rows = await self._warehouse.query_from_sql(
            "analytics/publishing_volume.sql",
            params=self._date_params(from_date, to_date),
        )
        return {
            "range": self._range_label(from_date, to_date),
            "granularity": granularity,
            "series": [{"date": row["date"], "value": int(row["value"])} for row in rows],
        }

    def _date_params(self, from_date: str | None, to_date: str | None) -> dict[str, str]:
        return {
            "from_date": from_date or "2026-03-01",
            "to_date": to_date or "2026-03-12",
        }

    def _range_label(self, from_date: str | None, to_date: str | None) -> str:
        return f"{from_date or '2026-03-01'} to {to_date or '2026-03-12'}"
