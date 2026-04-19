from __future__ import annotations

import time
import uuid
from typing import Awaitable, Callable

import structlog
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

_REQUEST_ID_HEADER = "X-Request-ID"

logger = structlog.get_logger(__name__)


class RequestContextMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self,
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        request_id = request.headers.get(_REQUEST_ID_HEADER) or uuid.uuid4().hex
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            request_id=request_id,
            method=request.method,
            path=request.url.path,
        )

        start = time.perf_counter()
        logger.info("request.started")

        try:
            response = await call_next(request)
        except Exception:
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            logger.exception("request.failed", duration_ms=duration_ms)
            structlog.contextvars.clear_contextvars()
            raise

        duration_ms = round((time.perf_counter() - start) * 1000, 2)
        response.headers[_REQUEST_ID_HEADER] = request_id
        logger.info(
            "request.completed",
            status_code=response.status_code,
            duration_ms=duration_ms,
        )
        structlog.contextvars.clear_contextvars()
        return response
