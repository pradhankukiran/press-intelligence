from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

import httpx
import structlog
import uvicorn
from tenacity import RetryError
from fastapi import FastAPI, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from press_intelligence.api.middleware import RequestContextMiddleware
from press_intelligence.api.routes import router
from press_intelligence.core.config import Settings, get_settings
from press_intelligence.core.dependencies import get_airflow_client, get_bigquery_warehouse
from press_intelligence.core.logging import configure_logging, get_logger
from press_intelligence.models.schemas import ErrorEnvelope

logger = get_logger(__name__)


OPENAPI_TAGS = [
    {"name": "system", "description": "Liveness, readiness, and app metadata."},
    {"name": "analytics", "description": "Editorial analytics over Guardian content."},
    {"name": "ops", "description": "Pipeline operations, runs, and backfills."},
]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging(settings)
    logger.info(
        "app.startup",
        app_env=settings.app_env,
        data_mode=settings.data_mode,
        log_level=settings.log_level,
        log_format=settings.log_format,
    )

    if settings.data_mode == "bigquery":
        warehouse = get_bigquery_warehouse()
        try:
            await warehouse.ensure_base_resources()
            logger.info("warehouse.resources.ensured")
        except Exception as exc:
            logger.warning(
                "warehouse.resources.ensure_failed",
                exc_info=exc,
                message="Starting in degraded mode; readiness probe will report unready.",
            )

    try:
        yield
    finally:
        try:
            await get_airflow_client().aclose()
        except Exception as exc:
            logger.warning("airflow.client.close_failed", exc_info=exc)
        logger.info("app.shutdown")


def _error_response(
    status_code: int,
    code: str,
    message: str,
    request: Request,
    details: dict[str, object] | None = None,
) -> JSONResponse:
    request_id = request.headers.get("X-Request-ID")
    envelope = ErrorEnvelope(
        code=code,
        message=message,
        request_id=request_id,
        details=details,
    )
    response = JSONResponse(
        status_code=status_code,
        content=envelope.model_dump(mode="json", exclude_none=True),
    )
    if request_id:
        response.headers["X-Request-ID"] = request_id
    return response


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(RequestValidationError)
    async def handle_validation(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        errors = jsonable_encoder(exc.errors())
        logger.warning("request.validation_error", errors=errors)
        return _error_response(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            code="validation_error",
            message="Request body or parameters failed validation.",
            request=request,
            details={"errors": errors},
        )

    @app.exception_handler(StarletteHTTPException)
    async def handle_http_exception(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        code = "http_error"
        message = str(exc.detail) if exc.detail else "HTTP error"
        details: dict[str, object] | None = None
        if isinstance(exc.detail, dict):
            code = str(exc.detail.get("code") or code)
            message = str(exc.detail.get("message") or message)
            extra = {k: v for k, v in exc.detail.items() if k not in {"code", "message"}}
            details = extra or None
        return _error_response(
            status_code=exc.status_code,
            code=code,
            message=message,
            request=request,
            details=details,
        )

    @app.exception_handler(httpx.HTTPError)
    async def handle_httpx(request: Request, exc: httpx.HTTPError) -> JSONResponse:
        logger.warning("upstream.http_error", exc_info=exc, url=str(exc.request.url) if exc.request else None)
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="upstream_unavailable",
            message="An upstream service is unavailable. Try again shortly.",
            request=request,
        )

    @app.exception_handler(RetryError)
    async def handle_retry_exhausted(request: Request, exc: RetryError) -> JSONResponse:
        logger.warning("upstream.retries_exhausted", exc_info=exc)
        return _error_response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            code="upstream_unavailable",
            message="An upstream service is unavailable after retries. Try again shortly.",
            request=request,
        )

    @app.exception_handler(Exception)
    async def handle_uncaught(request: Request, exc: Exception) -> JSONResponse:
        logger.exception("request.unhandled_exception")
        return _error_response(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            code="internal_error",
            message="An unexpected error occurred.",
            request=request,
        )


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings)

    app = FastAPI(
        title="Press Intelligence API",
        version="0.1.0",
        summary="Editorial analytics and operations API for Guardian content pipelines.",
        openapi_tags=OPENAPI_TAGS,
        lifespan=lifespan,
    )

    app.add_middleware(RequestContextMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    _register_exception_handlers(app)
    app.include_router(router, prefix="/api")

    @app.get("/", tags=["system"])
    async def root() -> dict[str, str]:
        return {
            "name": "press-intelligence",
            "mode": settings.data_mode,
            "status": "ok",
        }

    return app


app = create_app()


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "press_intelligence.main:app",
        host="0.0.0.0",
        port=8000,
        reload=(settings.app_env == "development"),
        log_config=None,
    )
