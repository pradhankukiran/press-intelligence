from __future__ import annotations

import asyncio
from unittest.mock import patch

from press_intelligence.clients.bigquery import BigQueryWarehouse
from press_intelligence.core.config import Settings


def _warehouse() -> BigQueryWarehouse:
    settings = Settings(data_mode="bigquery", google_cloud_project="p")
    w = BigQueryWarehouse(settings)
    w._resources_ensured = True
    return w


def test_load_articles_rejects_missing_guardian_id() -> None:
    w = _warehouse()
    rows = [
        {"guardian_id": "ok-1", "published_at": "2026-03-12T00:00:00Z"},
        {"published_at": "2026-03-12T00:00:00Z"},  # missing guardian_id
    ]
    with patch.object(w, "_load_articles_sync", return_value=1) as mock_load:
        result = asyncio.run(w.load_articles(rows))
    assert result == {"loaded": 1, "rejected": 1}
    mock_load.assert_called_once()
    passed_rows = mock_load.call_args[0][0]
    assert len(passed_rows) == 1
    assert passed_rows[0]["guardian_id"] == "ok-1"


def test_load_articles_rejects_missing_published_at() -> None:
    w = _warehouse()
    rows = [
        {"guardian_id": "ok-1", "published_at": "2026-03-12T00:00:00Z"},
        {"guardian_id": "bad-1"},  # missing published_at
    ]
    with patch.object(w, "_load_articles_sync", return_value=1):
        result = asyncio.run(w.load_articles(rows))
    assert result == {"loaded": 1, "rejected": 1}


def test_load_articles_all_rejected_skips_load() -> None:
    w = _warehouse()
    rows = [{"guardian_id": None}, {"foo": "bar"}]
    with patch.object(w, "_load_articles_sync") as mock_load:
        result = asyncio.run(w.load_articles(rows))
    assert result == {"loaded": 0, "rejected": 2}
    mock_load.assert_not_called()
