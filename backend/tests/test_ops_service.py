import asyncio

from press_intelligence.clients.airflow import AirflowDagRun
from press_intelligence.core.config import Settings
from press_intelligence.models.schemas import BackfillRequest
from press_intelligence.services.mock_store import MockStore
from press_intelligence.services.ops_service import OpsService


class FakeWarehouse:
    def __init__(self) -> None:
        self.pipeline_runs: dict[tuple[str, str], dict[str, object]] = {}

    async def upsert_pipeline_runs(self, rows: list[dict[str, object]]) -> int:
        for row in rows:
            self.pipeline_runs[(str(row["dag_id"]), str(row["run_id"]))] = dict(row)
        return len(rows)

    async def query_from_sql(
        self,
        sql_path: str,
        scalars: dict[str, object] | None = None,
    ) -> list[dict[str, object]]:
        if sql_path == "ops/pipeline_runs.sql":
            limit = int((scalars or {}).get("row_limit", 10))
            return sorted(
                self.pipeline_runs.values(),
                key=lambda row: str(row.get("started_at") or ""),
                reverse=True,
            )[:limit]
        return []


class FakeAirflow:
    async def dag_runs(self, dag_id: str, limit: int = 10) -> list[AirflowDagRun]:
        if dag_id == "guardian_ingest_recent":
            return [
                AirflowDagRun(
                    dag_run_id="scheduled__2026-03-12T18:00:00+00:00",
                    dag_id=dag_id,
                    state="success",
                    start_date="2026-03-12T18:00:05+00:00",
                    end_date="2026-03-12T18:01:00+00:00",
                    logical_date="2026-03-12T18:00:00+00:00",
                    conf={},
                )
            ]
        return [
            AirflowDagRun(
                dag_run_id="manual__2026-03-12T19:41:25.350569+00:00",
                dag_id=dag_id,
                state="running",
                start_date="2026-03-12T19:41:30+00:00",
                end_date=None,
                logical_date="2026-03-12T19:41:25.350569+00:00",
                conf={"start_date": "2026-03-01", "end_date": "2026-03-03"},
            )
        ]

    async def trigger_dag(self, dag_id: str, conf: dict[str, object]) -> AirflowDagRun:
        return AirflowDagRun(
            dag_run_id="manual__queued_1234",
            dag_id=dag_id,
            state="queued",
            logical_date="2026-03-12T20:10:00+00:00",
            conf=conf,
        )


def build_service() -> tuple[OpsService, FakeWarehouse]:
    settings = Settings(
        data_mode="bigquery",
        google_cloud_project="test-project",
    )
    warehouse = FakeWarehouse()
    service = OpsService(
        settings=settings,
        airflow=FakeAirflow(),
        warehouse=warehouse,  # type: ignore[arg-type]
        mock_store=MockStore(settings.mock_seed_date),
    )
    return service, warehouse


def test_runs_syncs_airflow_history_into_persisted_pipeline_runs() -> None:
    service, warehouse = build_service()

    payload = asyncio.run(service.runs(limit=10))

    assert len(payload["runs"]) == 2
    assert ("guardian_ingest_recent", "scheduled__2026-03-12T18:00:00+00:00") in warehouse.pipeline_runs
    assert (
        "guardian_backfill_range",
        "manual__2026-03-12T19:41:25.350569+00:00",
    ) in warehouse.pipeline_runs
    assert payload["runs"][0]["window"] == "2026-03-01 to 2026-03-03"


def test_trigger_backfill_persists_queued_run_record() -> None:
    service, warehouse = build_service()

    response = asyncio.run(
        service.trigger_backfill(
            BackfillRequest(start_date="2026-03-01", end_date="2026-03-02")
        )
    )

    assert response["status"] == "queued"
    persisted = warehouse.pipeline_runs[("guardian_backfill_range", "manual__queued_1234")]
    assert persisted["status"] == "queued"
    assert persisted["window"] == "2026-03-01 to 2026-03-02"
