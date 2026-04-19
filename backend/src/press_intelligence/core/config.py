from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


PROJECT_ROOT = Path(__file__).resolve().parents[4]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(PROJECT_ROOT / ".env", ".env"),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    app_env: str = "development"
    data_mode: Literal["mock", "bigquery"] = "mock"
    cors_origins: list[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ]
    )
    guardian_api_key: str | None = None
    guardian_base_url: str = "https://content.guardianapis.com"
    google_cloud_project: str | None = None
    bigquery_location: str = "US"
    bigquery_dataset_raw: str = "raw_guardian"
    bigquery_dataset_analytics: str = "analytics"
    bigquery_dataset_ops: str = "ops"
    airflow_base_url: str = "http://localhost:8080/api/v1"
    airflow_username: str = "airflow"
    airflow_password: str = "airflow"
    airflow_recent_dag_id: str = "guardian_ingest_recent"
    airflow_backfill_dag_id: str = "guardian_backfill_range"
    airflow_transform_dag_id: str = "guardian_transform_reporting"
    airflow_quality_dag_id: str = "guardian_data_quality"
    airflow_timeout_seconds: float = 3.0
    mock_seed_date: str = "2026-03-12"


@lru_cache
def get_settings() -> Settings:
    return Settings()
