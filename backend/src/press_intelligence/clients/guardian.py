from __future__ import annotations

from collections.abc import Iterable
from datetime import date

import httpx
import structlog

from press_intelligence.core.config import Settings

logger = structlog.get_logger(__name__)


class GuardianContentClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    async def fetch_range(self, start_date: date, end_date: date) -> list[dict[str, object]]:
        if not self._settings.guardian_api_key:
            raise RuntimeError("GUARDIAN_API_KEY is required for real ingestion mode.")

        rows: list[dict[str, object]] = []
        page = 1

        async with httpx.AsyncClient(timeout=30.0) as client:
            while True:
                response = await client.get(
                    f"{self._settings.guardian_base_url}/search",
                    params={
                        "api-key": self._settings.guardian_api_key,
                        "from-date": start_date.isoformat(),
                        "to-date": end_date.isoformat(),
                        "page-size": 50,
                        "show-tags": "keyword",
                        "show-fields": "headline,trailText,byline,bodyText",
                        "order-by": "newest",
                        "page": page,
                    },
                )
                response.raise_for_status()
                payload = response.json()["response"]
                rows.extend(self._normalize_results(payload.get("results", []), page))
                if page >= int(payload["pages"]):
                    break
                page += 1
        return rows

    def _normalize_results(
        self,
        results: Iterable[dict[str, object]],
        page: int,
    ) -> list[dict[str, object]]:
        normalized: list[dict[str, object]] = []
        for result in results:
            tags = [tag.get("webTitle") for tag in result.get("tags", []) if tag.get("webTitle")]
            normalized.append(
                {
                    "guardian_id": result.get("id"),
                    "web_url": result.get("webUrl"),
                    "web_title": result.get("webTitle"),
                    "section_id": result.get("sectionId"),
                    "section_name": result.get("sectionName"),
                    "pillar_id": result.get("pillarId"),
                    "pillar_name": result.get("pillarName"),
                    "published_at": result.get("webPublicationDate"),
                    "tags": tags,
                    "raw_payload": result,
                    "api_response_page": page,
                }
            )
        return normalized
