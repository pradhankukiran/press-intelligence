from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from press_intelligence.core.dependencies import (
    get_analytics_service,
    get_ops_service,
)
from press_intelligence.models.schemas import BackfillRequest
from press_intelligence.services.analytics_service import AnalyticsService
from press_intelligence.services.ops_service import OpsService

router = APIRouter()


@router.get("/health/live", tags=["system"])
async def health_live() -> dict[str, object]:
    return {"status": "ok", "mode": "live"}


@router.get("/health/ready", tags=["system"])
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


@router.get("/health", tags=["system"])
async def health(
    ops_service: OpsService = Depends(get_ops_service),
) -> JSONResponse:
    return await health_ready(ops_service=ops_service)


@router.get("/analytics/overview", tags=["analytics"])
async def analytics_overview(
    from_date: str | None = None,
    to_date: str | None = None,
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> dict[str, object]:
    return await analytics_service.get_overview(from_date=from_date, to_date=to_date)


@router.get("/analytics/sections", tags=["analytics"])
async def analytics_sections(
    from_date: str | None = None,
    to_date: str | None = None,
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> dict[str, object]:
    return await analytics_service.get_sections(from_date=from_date, to_date=to_date)


@router.get("/analytics/tags", tags=["analytics"])
async def analytics_tags(
    from_date: str | None = None,
    to_date: str | None = None,
    limit: int = 8,
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> dict[str, object]:
    return await analytics_service.get_tags(
        from_date=from_date,
        to_date=to_date,
        limit=limit,
    )


@router.get("/analytics/publishing-volume", tags=["analytics"])
async def analytics_publishing_volume(
    from_date: str | None = None,
    to_date: str | None = None,
    granularity: str = "day",
    analytics_service: AnalyticsService = Depends(get_analytics_service),
) -> dict[str, object]:
    return await analytics_service.get_publishing_volume(
        from_date=from_date,
        to_date=to_date,
        granularity=granularity,
    )


@router.get("/ops/status", tags=["ops"])
async def ops_status(
    ops_service: OpsService = Depends(get_ops_service),
) -> dict[str, object]:
    return await ops_service.status()


@router.get("/ops/runs", tags=["ops"])
async def ops_runs(
    limit: int = 10,
    ops_service: OpsService = Depends(get_ops_service),
) -> dict[str, object]:
    return await ops_service.runs(limit=limit)


@router.post("/ops/backfills", status_code=status.HTTP_202_ACCEPTED, tags=["ops"])
async def trigger_backfill(
    request: BackfillRequest,
    ops_service: OpsService = Depends(get_ops_service),
) -> dict[str, object]:
    return await ops_service.trigger_backfill(request)


@router.get("/ops/backfills/{run_id}", tags=["ops"])
async def get_backfill_status(
    run_id: str,
    ops_service: OpsService = Depends(get_ops_service),
) -> dict[str, object]:
    result = await ops_service.backfill_status(run_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "run_not_found", "message": "Backfill run was not found."},
        )
    return result
