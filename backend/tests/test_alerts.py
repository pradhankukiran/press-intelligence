from __future__ import annotations

import httpx
import respx

from press_intelligence.core.config import Settings
from press_intelligence.services.alerts import AlertsNotifier


def _settings(url: str | None = "https://hooks.example/test") -> Settings:
    return Settings(
        data_mode="bigquery",
        google_cloud_project="p",
        alerts_webhook_url=url,
    )


async def test_notify_failed_run_posts_webhook() -> None:
    notifier = AlertsNotifier(_settings())
    with respx.mock() as router:
        hook = router.post("https://hooks.example/test").mock(
            return_value=httpx.Response(200)
        )
        sent = await notifier.notify_failed_run(
            {"run_id": "r1", "dag_id": "dag-a", "status": "failed"}
        )
    assert sent is True
    assert hook.called
    body = hook.calls.last.request.read()
    assert b"dag_run_failed" in body
    assert b"r1" in body


async def test_notify_failed_run_dedupes_same_run_id() -> None:
    notifier = AlertsNotifier(_settings())
    with respx.mock() as router:
        router.post("https://hooks.example/test").mock(
            return_value=httpx.Response(200)
        )
        first = await notifier.notify_failed_run(
            {"run_id": "r1", "dag_id": "dag-a", "status": "failed"}
        )
        second = await notifier.notify_failed_run(
            {"run_id": "r1", "dag_id": "dag-a", "status": "failed"}
        )
    assert first is True
    assert second is False


async def test_notify_returns_false_when_no_webhook_configured() -> None:
    notifier = AlertsNotifier(_settings(url=None))
    sent = await notifier.notify_failed_run(
        {"run_id": "r1", "dag_id": "dag-a", "status": "failed"}
    )
    assert sent is False


async def test_notify_swallows_webhook_failure() -> None:
    notifier = AlertsNotifier(_settings())
    with respx.mock() as router:
        router.post("https://hooks.example/test").mock(
            return_value=httpx.Response(500)
        )
        sent = await notifier.notify_failed_run(
            {"run_id": "r1", "dag_id": "dag-a", "status": "failed"}
        )
    assert sent is False
