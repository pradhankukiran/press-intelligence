from __future__ import annotations

from datetime import date

import httpx
import pytest
import respx

from press_intelligence.clients.guardian import GuardianContentClient, GuardianTransientError
from press_intelligence.core.config import Settings


def _settings() -> Settings:
    return Settings(guardian_api_key="test-key", data_mode="bigquery", google_cloud_project="p")


async def test_fetch_range_single_page_success() -> None:
    client = GuardianContentClient(_settings())
    with respx.mock(base_url="https://content.guardianapis.com") as router:
        router.get("/search").mock(
            return_value=httpx.Response(
                200,
                json={
                    "response": {
                        "pages": 1,
                        "results": [
                            {
                                "id": "world/2026/mar/11/story",
                                "webUrl": "https://example.com/story",
                                "webTitle": "Story",
                                "sectionId": "world",
                                "sectionName": "World",
                                "pillarId": "news",
                                "pillarName": "News",
                                "webPublicationDate": "2026-03-11T12:00:00Z",
                                "tags": [{"webTitle": "climate"}],
                            }
                        ],
                    }
                },
            )
        )
        rows = await client.fetch_range(date(2026, 3, 11), date(2026, 3, 11))
    assert len(rows) == 1
    assert rows[0]["guardian_id"] == "world/2026/mar/11/story"
    assert rows[0]["tags"] == ["climate"]


async def test_fetch_range_missing_pages_defaults_to_single_page() -> None:
    client = GuardianContentClient(_settings())
    with respx.mock(base_url="https://content.guardianapis.com") as router:
        router.get("/search").mock(
            return_value=httpx.Response(200, json={"response": {"results": []}}),
        )
        rows = await client.fetch_range(date(2026, 3, 11), date(2026, 3, 11))
    assert rows == []


async def test_fetch_range_retries_on_503_then_succeeds() -> None:
    client = GuardianContentClient(_settings())
    responses = iter(
        [
            httpx.Response(503),
            httpx.Response(503),
            httpx.Response(200, json={"response": {"pages": 1, "results": []}}),
        ]
    )
    with respx.mock(base_url="https://content.guardianapis.com") as router:
        route = router.get("/search").mock(side_effect=lambda request: next(responses))
        rows = await client.fetch_range(date(2026, 3, 11), date(2026, 3, 11))
    assert rows == []
    assert route.call_count == 3


async def test_fetch_range_raises_after_retries_exhausted() -> None:
    client = GuardianContentClient(_settings())
    with respx.mock(base_url="https://content.guardianapis.com") as router:
        router.get("/search").mock(return_value=httpx.Response(503))
        with pytest.raises(GuardianTransientError):
            await client.fetch_range(date(2026, 3, 11), date(2026, 3, 11))


async def test_fetch_range_fails_fast_on_auth_error() -> None:
    client = GuardianContentClient(_settings())
    with respx.mock(base_url="https://content.guardianapis.com") as router:
        router.get("/search").mock(return_value=httpx.Response(401, text="bad key"))
        with pytest.raises(httpx.HTTPStatusError):
            await client.fetch_range(date(2026, 3, 11), date(2026, 3, 11))


async def test_fetch_range_requires_api_key() -> None:
    settings = Settings(guardian_api_key=None, data_mode="bigquery", google_cloud_project="p")
    client = GuardianContentClient(settings)
    with pytest.raises(RuntimeError):
        await client.fetch_range(date(2026, 3, 11), date(2026, 3, 11))
