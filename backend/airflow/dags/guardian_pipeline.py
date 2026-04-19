from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task

from press_intelligence.core.config import get_settings
from press_intelligence.services.guardian_pipeline import GuardianPipelineService

settings = get_settings()


@task
def run_recent_ingest() -> dict[str, object]:
    import asyncio

    return asyncio.run(GuardianPipelineService(settings).run_recent_ingest())


@task
def run_transforms() -> dict[str, object]:
    import asyncio

    return asyncio.run(GuardianPipelineService(settings).run_transforms())


@task
def run_quality_checks() -> dict[str, object]:
    import asyncio

    return asyncio.run(GuardianPipelineService(settings).run_quality_checks())


@task
def run_backfill(start_date: str, end_date: str) -> dict[str, object]:
    import asyncio

    return asyncio.run(GuardianPipelineService(settings).run_backfill(start_date, end_date))


@dag(
    dag_id=settings.airflow_recent_dag_id,
    start_date=datetime(2026, 3, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["guardian", "ingestion"],
)
def guardian_ingest_recent():
    ingest = run_recent_ingest()
    transforms = run_transforms()
    quality = run_quality_checks()
    ingest >> transforms >> quality


@dag(
    dag_id=settings.airflow_backfill_dag_id,
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    params={"start_date": "2026-03-01", "end_date": "2026-03-02"},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=10)},
    tags=["guardian", "backfill"],
)
def guardian_backfill_range():
    backfill = run_backfill("{{ params.start_date }}", "{{ params.end_date }}")
    transforms = run_transforms()
    quality = run_quality_checks()
    backfill >> transforms >> quality


guardian_ingest_recent()
guardian_backfill_range()
