from functools import lru_cache

from press_intelligence.clients.airflow import AirflowClient
from press_intelligence.clients.bigquery import BigQueryWarehouse
from press_intelligence.core.config import Settings, get_settings
from press_intelligence.core.idempotency import IdempotencyCache
from press_intelligence.services.analytics_service import AnalyticsService
from press_intelligence.services.mock_store import MockStore
from press_intelligence.services.ops_service import OpsService


@lru_cache
def get_mock_store() -> MockStore:
    settings = get_settings()
    return MockStore(settings.mock_seed_date)


@lru_cache
def get_bigquery_warehouse() -> BigQueryWarehouse:
    return BigQueryWarehouse(get_settings())


@lru_cache
def get_airflow_client() -> AirflowClient:
    return AirflowClient(get_settings())


@lru_cache
def get_idempotency_cache() -> IdempotencyCache:
    return IdempotencyCache()


def get_analytics_service() -> AnalyticsService:
    settings = get_settings()
    return AnalyticsService(
        settings=settings,
        warehouse=get_bigquery_warehouse(),
        mock_store=get_mock_store(),
    )


def get_ops_service() -> OpsService:
    settings = get_settings()
    return OpsService(
        settings=settings,
        airflow=get_airflow_client(),
        warehouse=get_bigquery_warehouse(),
        mock_store=get_mock_store(),
        idempotency_cache=get_idempotency_cache(),
    )


def get_runtime_settings() -> Settings:
    return get_settings()
