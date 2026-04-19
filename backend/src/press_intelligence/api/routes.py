from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from press_intelligence.core.dependencies import (
    get_analytics_service,
    get_ops_service,
)
from press_intelligence.models.schemas import (
    BackfillRequest,
    BackfillResponse,
    ErrorEnvelope,
    HealthResponse,
    OpsStatusResponse,
    OverviewResponse,
    PipelineRun,
    PublishingVolumeResponse,
    RunsResponse,
    SectionsResponse,
    TagsResponse,
)
from press_intelligence.services.analytics_service import AnalyticsService
from press_intelligence.services.ops_service import OpsService

router = APIRouter()


@router.get("/health/live", tags=["system"], response_model=HealthResponse)
async def health_live() -> HealthResponse:
    return HealthResponse(status="ok", mode="live", services={})


@router.get(
    "/health/ready",
    tags=["system"],
    response_model=HealthResponse,
    responses={503: {"model": HealthResponse}},
)
async def health_ready(
    ops_service: OpsService = Depends(get_ops_service),
) -> JSONResponse:
    payload = await ops_service.health()
    status_code = (
        status.HTTP_200_OK
        if payload.get("status") == "ok"
        else status.HTTP_503_SERVICE_UNAVAILABLE
    )
    return JSONResponse(status_code=status_code, content=payload)


@router.get(
    "/health",
    tags=["system"],
    response_model=HealthResponse,
    responses={503: {"model": HealthResponse}},
)
async def health(
    ops_service: OpsService = Depends(get_ops_service),
) -> JSONResponse:
    return await health_ready(ops_service=ops_service)


@router.get(
    "/analytics/overview",
    tags=["analytics"],
    response_model=OverviewResponse,
)
async def analytics_overview(
    from_date: str | None = None,
    to_date: str | None = None,
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> OverviewResponse:
    data = await analytics_service.get_overview(from_date=from_date, to_date=to_date)
    return OverviewResponse.model_validate(data)


@router.get(
    "/analytics/sections",
    tags=["analytics"],
    response_model=SectionsResponse,
)
async def analytics_sections(
    from_date: str | None = None,
    to_date: str | None = None,
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> SectionsResponse:
    data = await analytics_service.get_sections(from_date=from_date, to_date=to_date)
    return SectionsResponse.model_validate(data)


@router.get(
    "/analytics/tags",
    tags=["analytics"],
    response_model=TagsResponse,
)
async def analytics_tags(
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 8,
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> TagsResponse:
    data = await analytics_service.get_tags(
        from_date=from_date,
        to_date=to_date,
        limit=limit,
    )
    return TagsResponse.model_validate(data)


@router.get(
    "/analytics/publishing-volume",
    tags=["analytics"],
    response_model=PublishingVolumeResponse,
)
async def analytics_publishing_volume(
    from_date: str | None = None,
    to_date: str | None = None,
    granularity: str = "day",
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> PublishingVolumeResponse:
    data = await analytics_service.get_publishing_volume(
        from_date=from_date,
        to_date=to_date,
        granularity=granularity,
    )
    return PublishingVolumeResponse.model_validate(data)


@router.get("/ops/status", tags=["ops"], response_model=OpsStatusResponse)
async def ops_status(
    ops_service: OpsService = Depends(get_ops_service),
) -> OpsStatusResponse:
    data = await ops_service.status()
    return OpsStatusResponse.model_validate(data)


@router.get("/ops/runs", tags=["ops"], response_model=RunsResponse)
async def ops_runs(
    limit: int = 10,
    ops_service: OpsService = Depends(get_ops_service),
) -> RunsResponse:
    data = await ops_service.runs(limit=limit)
    return RunsResponse.model_validate(data)


@router.post(
    "/ops/backfills",
    status_code=status.HTTP_202_ACCEPTED,
    tags=["ops"],
    response_model=BackfillResponse,
)
async def trigger_backfill(
    request: BackfillRequest,
    ops_service: OpsService = Depends(get_ops_service),
) -> BackfillResponse:
    data = await ops_service.trigger_backfill(request)
    return BackfillResponse.model_validate(data)


@router.get(
    "/ops/backfills/{run_id}",
    tags=["ops"],
    response_model=PipelineRun,
    responses={404: {"model": ErrorEnvelope}},
)
async def get_backfill_status(
    run_id: str,
    ops_service: OpsService = Depends(get_ops_service),
) -> PipelineRun:
    result = await ops_service.backfill_status(run_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "run_not_found", "message": "Backfill run was not found."},
        )
    return PipelineRun.model_validate(result)
