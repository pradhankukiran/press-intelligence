from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class KPI(BaseModel):
    label: str
    value: str
    delta: str
    tone: Literal["up", "down", "neutral"] = "neutral"


class TrendPoint(BaseModel):
    date: str
    value: int


class SectionPoint(BaseModel):
    date: str
    world: int = 0
    politics: int = 0
    business: int = 0
    culture: int = 0
    climate: int = 0
    technology: int = 0


class SectionLeader(BaseModel):
    section: str
    count: int


class TagPoint(BaseModel):
    tag: str
    count: int
    momentum: str


class PipelineRun(BaseModel):
    run_id: str
    dag_id: str
    status: Literal["success", "failed", "running", "queued", "scheduled"]
    trigger: Literal["system", "manual"]
    started_at: str
    finished_at: str | None = None
    window: str
    error_summary: str | None = None


class DataQualityCheck(BaseModel):
    name: str
    status: Literal["pass", "warn", "fail"]
    observed_value: str
    threshold: str
    detail: str


class DagStatus(BaseModel):
    id: str
    status: Literal["healthy", "degraded", "idle"]


class FreshnessPayload(BaseModel):
    last_sync_at: str
    watermark: str
    lag: str


class OverviewResponse(BaseModel):
    range: str
    kpis: list[KPI]
    daily_volume: list[TrendPoint]
    freshness: FreshnessPayload
    top_sections: list[SectionLeader]


class SectionsResponse(BaseModel):
    range: str
    series: list[SectionPoint]
    leaders: list[SectionLeader]


class TagsResponse(BaseModel):
    range: str
    tags: list[TagPoint]


class PublishingVolumeResponse(BaseModel):
    range: str
    granularity: Literal["day", "hour"]
    series: list[TrendPoint]


class OpsStatusResponse(BaseModel):
    environment: str
    mode: str
    last_sync_at: str
    freshness_lag: str
    watermark: str
    dags: list[DagStatus]
    checks: list[DataQualityCheck]


class RunsResponse(BaseModel):
    runs: list[PipelineRun]


class HealthResponse(BaseModel):
    status: Literal["ok", "degraded"]
    mode: str
    services: dict[str, str]


class BackfillRequest(BaseModel):
    start_date: str
    end_date: str

    @model_validator(mode="after")
    def validate_range(self) -> "BackfillRequest":
        start = datetime.fromisoformat(f"{self.start_date}T00:00:00")
        end = datetime.fromisoformat(f"{self.end_date}T00:00:00")
        if end < start:
            raise ValueError("end_date must be on or after start_date")
        return self


class BackfillResponse(BaseModel):
    run_id: str
    dag_id: str
    status: Literal["queued", "running", "success", "failed"]
    message: str


class ErrorEnvelope(BaseModel):
    code: str
    message: str
    request_id: str | None = None
    details: dict[str, object] | None = None
