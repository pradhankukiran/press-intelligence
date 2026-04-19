from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import httpx
import structlog
from tenacity import (
    RetryCallState,
    retry,
    retry_if_exception_type,
    retry_if_result,
    stop_after_attempt,
    wait_exponential_jitter,
)

logger = structlog.get_logger(__name__)

RETRYABLE_STATUSES = frozenset({429, 502, 503, 504})

T = TypeVar("T")


def _wait_with_retry_after(
    initial: float = 0.5, cap: float = 10.0
) -> Callable[[RetryCallState], float]:
    exponential = wait_exponential_jitter(initial=initial, max=cap)

    def _wait(retry_state: RetryCallState) -> float:
        outcome = retry_state.outcome
        if outcome is not None and not outcome.failed:
            value = outcome.result()
            if isinstance(value, httpx.Response):
                header = value.headers.get("Retry-After")
                if header:
                    try:
                        return min(cap, max(0.0, float(header)))
                    except ValueError:
                        pass
        return exponential(retry_state)

    return _wait


def _log_before_retry(service: str) -> Callable[[RetryCallState], None]:
    def _hook(retry_state: RetryCallState) -> None:
        exc = retry_state.outcome.exception() if retry_state.outcome else None
        status_code = None
        if retry_state.outcome and not retry_state.outcome.failed:
            value = retry_state.outcome.result()
            if isinstance(value, httpx.Response):
                status_code = value.status_code
        logger.warning(
            f"{service}.call.retrying",
            attempt=retry_state.attempt_number,
            status_code=status_code,
            exception=str(exc) if exc else None,
        )

    return _hook


def _should_retry_result(result: Any) -> bool:
    return isinstance(result, httpx.Response) and result.status_code in RETRYABLE_STATUSES


def retry_http(
    service: str,
    max_attempts: int = 5,
    initial: float = 0.5,
    cap: float = 10.0,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    return retry(
        stop=stop_after_attempt(max_attempts),
        wait=_wait_with_retry_after(initial=initial, cap=cap),
        retry=(
            retry_if_exception_type((httpx.TransportError, httpx.ReadTimeout))
            | retry_if_result(_should_retry_result)
        ),
        before_sleep=_log_before_retry(service),
        reraise=True,
    )
