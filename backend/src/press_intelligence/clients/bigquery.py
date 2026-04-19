from __future__ import annotations

import asyncio
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

from press_intelligence.core.config import Settings

logger = structlog.get_logger(__name__)


def _validate_article_row(row: dict[str, Any]) -> str | None:
    if not isinstance(row, dict):
        return "not_a_dict"
    if not row.get("guardian_id"):
        return "missing_guardian_id"
    if not row.get("published_at"):
        return "missing_published_at"
    return None


class BigQueryWarehouse:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._client = None
        self._resources_ensured = settings.data_mode == "mock"
        self._ensure_lock = asyncio.Lock()

    def _ensure_client(self):
        if self._client is None:
            from google.cloud import bigquery

            if not self._settings.google_cloud_project:
                raise RuntimeError("GOOGLE_CLOUD_PROJECT is required for BigQuery mode.")

            self._client = bigquery.Client(project=self._settings.google_cloud_project)
        return self._client

    async def healthcheck(self) -> str:
        from google.api_core.exceptions import GoogleAPIError

        if self._settings.data_mode == "mock":
            return "mock"
        try:
            await asyncio.to_thread(self._list_datasets_probe)
            return "connected"
        except (GoogleAPIError, OSError) as exc:
            logger.warning("warehouse.health.degraded", exc_info=exc)
            return "degraded"

    async def query_from_sql(
        self,
        sql_path: str,
        scalars: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        await self.ensure_base_resources()
        rendered = self._render_sql(sql_path).format(**self._identifier_params())
        params = self._build_query_params(scalars)
        return await asyncio.to_thread(self._run_query, rendered, params)

    async def execute_sql(self, sql_path: str) -> None:
        await self.ensure_base_resources()
        rendered = self._render_sql(sql_path).format(**self._identifier_params())
        await asyncio.to_thread(self._run_statement, rendered)

    async def load_articles(self, rows: list[dict[str, Any]]) -> dict[str, int]:
        await self.ensure_base_resources()
        valid, rejected = self._partition_articles(rows)
        if not valid:
            return {"loaded": 0, "rejected": len(rejected)}
        loaded = await asyncio.to_thread(self._load_articles_sync, valid)
        return {"loaded": loaded, "rejected": len(rejected)}

    def _partition_articles(
        self, rows: list[dict[str, Any]]
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        valid: list[dict[str, Any]] = []
        rejected: list[dict[str, Any]] = []
        for row in rows:
            reason = _validate_article_row(row)
            if reason is None:
                valid.append(row)
                continue
            rejected.append(row)
            logger.warning(
                "warehouse.articles.rejected",
                reason=reason,
                guardian_id=row.get("guardian_id"),
            )
        return valid, rejected

    async def ensure_base_resources(self) -> None:
        if self._resources_ensured:
            return

        async with self._ensure_lock:
            if self._resources_ensured:
                return
            await asyncio.to_thread(self._ensure_base_resources_sync)
            self._resources_ensured = True

    async def raw_article_count(self) -> int:
        return await asyncio.to_thread(self._raw_article_count_sync)

    async def latest_ingested_at(self) -> datetime | None:
        return await asyncio.to_thread(self._latest_ingested_at_sync)

    async def upsert_pipeline_runs(self, rows: list[dict[str, Any]]) -> int:
        await self.ensure_base_resources()
        if not rows:
            return 0
        return await asyncio.to_thread(self._upsert_pipeline_runs_sync, rows)

    def _list_datasets_probe(self) -> None:
        client = self._ensure_client()
        list(client.list_datasets(page_size=1))

    def _run_query(
        self,
        sql: str,
        params: list[Any] | None = None,
    ) -> list[dict[str, Any]]:
        from google.cloud import bigquery

        client = self._ensure_client()
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        query_job = client.query(sql, job_config=job_config)
        results = query_job.result()
        return [dict(row.items()) for row in results]

    def _run_statement(self, sql: str) -> None:
        client = self._ensure_client()
        query_job = client.query(sql)
        query_job.result()

    def _load_articles_sync(self, rows: list[dict[str, Any]]) -> int:
        from google.cloud import bigquery

        client = self._ensure_client()
        table_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_raw}.articles_raw"
        )
        table = client.get_table(table_id)
        job_config = bigquery.LoadJobConfig(
            schema=table.schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        load_job = client.load_table_from_json(rows, table_id, job_config=job_config)
        load_job.result()
        return len(rows)

    def _upsert_pipeline_runs_sync(self, rows: list[dict[str, Any]]) -> int:
        from google.cloud import bigquery

        client = self._ensure_client()
        target_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_ops}.pipeline_runs"
        )
        target = client.get_table(target_id)

        staging_name = f"pipeline_runs__stg_{uuid.uuid4().hex}"
        staging_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_ops}.{staging_name}"
        )
        normalized = [self._normalize_pipeline_run_row(dict(row)) for row in rows]
        try:
            load_job = client.load_table_from_json(
                normalized,
                staging_id,
                job_config=bigquery.LoadJobConfig(
                    schema=target.schema,
                    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ),
            )
            load_job.result()

            merge_sql = self._render_sql("ops/merge_pipeline_runs.sql").format(
                **self._identifier_params(),
                staging_table=staging_name,
            )
            merge_job = client.query(merge_sql)
            merge_job.result()
        finally:
            try:
                client.delete_table(staging_id, not_found_ok=True)
            except Exception as exc:
                logger.warning(
                    "warehouse.pipeline_runs.staging_cleanup_failed",
                    staging_id=staging_id,
                    exc_info=exc,
                )
        return len(rows)

    def _ensure_base_resources_sync(self) -> None:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        client = self._ensure_client()

        for dataset_name in (
            self._settings.bigquery_dataset_raw,
            self._settings.bigquery_dataset_analytics,
            self._settings.bigquery_dataset_ops,
        ):
            dataset_id = f"{self._settings.google_cloud_project}.{dataset_name}"
            try:
                client.get_dataset(dataset_id)
            except NotFound:
                dataset = bigquery.Dataset(dataset_id)
                dataset.location = self._settings.bigquery_location
                client.create_dataset(dataset)

        raw_table_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_raw}.articles_raw"
        )
        try:
            client.get_table(raw_table_id)
        except NotFound:
            raw_table = bigquery.Table(
                raw_table_id,
                schema=[
                    bigquery.SchemaField("guardian_id", "STRING"),
                    bigquery.SchemaField("web_url", "STRING"),
                    bigquery.SchemaField("web_title", "STRING"),
                    bigquery.SchemaField("section_id", "STRING"),
                    bigquery.SchemaField("section_name", "STRING"),
                    bigquery.SchemaField("pillar_id", "STRING"),
                    bigquery.SchemaField("pillar_name", "STRING"),
                    bigquery.SchemaField("published_at", "TIMESTAMP"),
                    bigquery.SchemaField("ingested_at", "TIMESTAMP"),
                    bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
                    bigquery.SchemaField("api_response_page", "INTEGER"),
                    bigquery.SchemaField("raw_payload", "JSON"),
                ],
            )
            client.create_table(raw_table)

        pipeline_runs_table_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_ops}.pipeline_runs"
        )
        try:
            client.get_table(pipeline_runs_table_id)
        except NotFound:
            client.create_table(
                bigquery.Table(
                    pipeline_runs_table_id,
                    schema=[
                        bigquery.SchemaField("run_id", "STRING"),
                        bigquery.SchemaField("dag_id", "STRING"),
                        bigquery.SchemaField("status", "STRING"),
                        bigquery.SchemaField("trigger", "STRING"),
                        bigquery.SchemaField("started_at", "TIMESTAMP"),
                        bigquery.SchemaField("finished_at", "TIMESTAMP"),
                        bigquery.SchemaField("window", "STRING"),
                        bigquery.SchemaField("error_summary", "STRING"),
                    ],
                )
            )

        quality_table_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_ops}.data_quality_results"
        )
        try:
            client.get_table(quality_table_id)
        except NotFound:
            client.create_table(
                bigquery.Table(
                    quality_table_id,
                    schema=[
                        bigquery.SchemaField("check_name", "STRING"),
                        bigquery.SchemaField("check_date", "DATE"),
                        bigquery.SchemaField("severity", "STRING"),
                        bigquery.SchemaField("status", "STRING"),
                        bigquery.SchemaField("observed_value", "STRING"),
                        bigquery.SchemaField("threshold", "STRING"),
                        bigquery.SchemaField("details_json", "JSON"),
                        bigquery.SchemaField("created_at", "TIMESTAMP"),
                    ],
                )
            )

    def _raw_article_count_sync(self) -> int:
        from google.api_core.exceptions import NotFound

        client = self._ensure_client()
        table_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_raw}.articles_raw"
        )
        try:
            table = client.get_table(table_id)
        except NotFound:
            logger.warning("warehouse.articles_raw.missing", table_id=table_id)
            return 0
        return int(table.num_rows)

    def _latest_ingested_at_sync(self) -> datetime | None:
        from google.api_core.exceptions import NotFound

        client = self._ensure_client()
        table_id = (
            f"{self._settings.google_cloud_project}."
            f"{self._settings.bigquery_dataset_raw}.articles_raw"
        )
        sql = f"SELECT MAX(ingested_at) AS latest FROM `{table_id}`"
        try:
            rows = list(client.query(sql).result())
        except NotFound:
            logger.warning("warehouse.articles_raw.missing", table_id=table_id)
            return None
        if not rows:
            return None
        latest = rows[0].get("latest")
        if isinstance(latest, datetime):
            return latest
        return None

    def _render_sql(self, relative_path: str) -> str:
        base = Path(__file__).resolve().parents[1] / "sql"
        return (base / relative_path).read_text(encoding="utf-8")

    def _normalize_pipeline_run_row(self, row: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(row)
        for key in ("started_at", "finished_at"):
            value = normalized.get(key)
            if value is not None:
                normalized[key] = value.isoformat() if hasattr(value, "isoformat") else str(value)
        for key in ("run_id", "dag_id", "status", "trigger", "window", "error_summary"):
            value = normalized.get(key)
            if value is not None:
                normalized[key] = str(value)
        return normalized

    def _identifier_params(self) -> dict[str, Any]:
        return {
            "google_cloud_project": self._settings.google_cloud_project,
            "bigquery_dataset_raw": self._settings.bigquery_dataset_raw,
            "bigquery_dataset_analytics": self._settings.bigquery_dataset_analytics,
            "bigquery_dataset_ops": self._settings.bigquery_dataset_ops,
        }

    def _build_query_params(self, scalars: dict[str, Any] | None) -> list[Any]:
        if not scalars:
            return []
        from google.cloud import bigquery

        built: list[Any] = []
        for name, value in scalars.items():
            if isinstance(value, bool):
                bq_type = "BOOL"
            elif isinstance(value, int):
                bq_type = "INT64"
            elif isinstance(value, float):
                bq_type = "FLOAT64"
            else:
                bq_type = "STRING"
                value = str(value)
            built.append(bigquery.ScalarQueryParameter(name, bq_type, value))
        return built
