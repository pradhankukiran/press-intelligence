from __future__ import annotations

import time
from collections.abc import Iterable
from datetime import date

import httpx
import structlog
from tenacity import RetryError

from press_intelligence.clients._retry import retry_http
from press_intelligence.core.config import Settings

logger = structlog.get_logger(__name__)


class GuardianTransientError(RuntimeError):
    """Raised when Guardian responds with a retry-exhausted transient failure."""


class GuardianContentClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings

    def _timeout(self) -> httpx.Timeout:
        return httpx.Timeout(connect=5.0, read=30.0, write=10.0, pool=5.0)

    async def fetch_range(self, start_date: date, end_date: date) -> list[dict[str, object]]:
        if not self._settings.guardian_api_key:
            raise RuntimeError("GUARDIAN_API_KEY is required for real ingestion mode.")

        rows: list[dict[str, object]] = []
        page = 1

        async with httpx.AsyncClient(timeout=self._timeout()) as client:
            while True:
                try:
                    response = await self._fetch_page(client, start_date, end_date, page)
                except RetryError as retry_exc:
                    last = retry_exc.last_attempt.result()
                    status_code = getattr(last, "status_code", "unknown")
                    raise GuardianTransientError(
                        f"Guardian returned {status_code} after retries"
                    ) from retry_exc
                if response.status_code in {401, 403}:
                    response.raise_for_status()
                if response.status_code >= 400:
                    raise GuardianTransientError(
                        f"Guardian returned {response.status_code} after retries: {response.text[:200]}"
                    )

                payload = response.json().get("response", {})
                page_results = payload.get("results", []) or []
                rows.extend(self._normalize_results(page_results, page))

                total_pages = int(payload.get("pages", 1) or 1)
                if page >= total_pages:
                    break
                page += 1
        return rows

    async def _fetch_page(
        self,
        client: httpx.AsyncClient,
        start_date: date,
        end_date: date,
        page: int,
    ) -> httpx.Response:
        started = time.perf_counter()

        @retry_http("guardian")
        async def _call() -> httpx.Response:
            return await client.get(
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

        response = await _call()
        logger.info(
            "guardian.page.fetched",
            page=page,
            status_code=response.status_code,
            duration_ms=round((time.perf_counter() - started) * 1000, 2),
        )
        return response

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
