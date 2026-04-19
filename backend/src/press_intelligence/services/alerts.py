from __future__ import annotations

from collections import OrderedDict
from typing import Any

import httpx
import structlog

from press_intelligence.core.config import Settings

logger = structlog.get_logger(__name__)


class AlertsNotifier:
    def __init__(self, settings: Settings, max_seen: int = 1024) -> None:
        self._settings = settings
        self._seen: OrderedDict[str, None] = OrderedDict()
        self._max_seen = max_seen

    async def notify_failed_run(self, run: dict[str, Any]) -> bool:
        url = self._settings.alerts_webhook_url
        if not url:
            return False

        run_id = str(run.get("run_id", ""))
        if not run_id or run_id in self._seen:
            return False

        payload = {
            "event": "dag_run_failed",
            "run_id": run_id,
            "dag_id": run.get("dag_id"),
            "status": run.get("status"),
            "started_at": run.get("started_at"),
            "window": run.get("window"),
            "error_summary": run.get("error_summary"),
        }
        try:
            async with httpx.AsyncClient(
                timeout=self._settings.alerts_webhook_timeout_seconds
            ) as client:
                response = await client.post(url, json=payload)
                response.raise_for_status()
        except httpx.HTTPError as exc:
            logger.warning(
                "alerts.webhook.failed",
                run_id=run_id,
                dag_id=run.get("dag_id"),
                exc_info=exc,
            )
            return False

        self._seen[run_id] = None
        while len(self._seen) > self._max_seen:
            self._seen.popitem(last=False)
        logger.info(
            "alerts.webhook.sent",
            run_id=run_id,
            dag_id=run.get("dag_id"),
        )
        return True
