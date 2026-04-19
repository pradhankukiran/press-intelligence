from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from uuid import uuid4

import structlog

from press_intelligence.models.schemas import BackfillRequest

logger = structlog.get_logger(__name__)


class MockStore:
    def __init__(self, seed_date: str) -> None:
        self._seed_date = seed_date
        self._data_dir = Path(__file__).resolve().parents[1] / "mock_data"
        self._runs = self._load_json("runs.json")
        self._status = self._load_json("status.json")

    def overview(self) -> dict[str, object]:
        return self._load_json("overview.json")

    def sections(self) -> dict[str, object]:
        return self._load_json("sections.json")

    def tags(self, limit: int) -> dict[str, object]:
        payload = self._load_json("tags.json")
        payload["tags"] = payload["tags"][:limit]
        return payload

    def publishing_volume(self) -> dict[str, object]:
        return self._load_json("publishing_volume.json")

    def status(self) -> dict[str, object]:
        payload = deepcopy(self._status)
        payload["latest_seed"] = self._seed_date
        return payload

    def runs(self, limit: int) -> dict[str, object]:
        return {"runs": deepcopy(self._runs[:limit])}

    def trigger_backfill(self, request: BackfillRequest) -> dict[str, object]:
        run_id = f"manual__{uuid4().hex[:12]}"
        run = {
            "run_id": run_id,
            "dag_id": "guardian_backfill_range",
            "status": "queued",
            "trigger": "manual",
            "started_at": "2026-03-12T18:05:00Z",
            "finished_at": None,
            "window": f"{request.start_date} to {request.end_date}",
            "error_summary": None,
        }
        self._runs.insert(0, run)
        self._status["last_sync_at"] = "Awaiting completion"
        return {
            "run_id": run_id,
            "dag_id": "guardian_backfill_range",
            "status": "queued",
            "message": (
                f"Queued Guardian backfill for {request.start_date} through {request.end_date}."
            ),
        }

    def backfill_status(self, run_id: str) -> dict[str, object] | None:
        for run in self._runs:
            if run["run_id"] == run_id:
                return deepcopy(run)
        return None

    def _load_json(self, filename: str) -> dict[str, object] | list[dict[str, object]]:
        return json.loads((self._data_dir / filename).read_text(encoding="utf-8"))
