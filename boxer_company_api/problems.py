from __future__ import annotations

import logging
import re
from typing import Mapping

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


logger = logging.getLogger(__name__)

_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_DEFAULT_TITLES = {
    "invalid_request_id": "Invalid request ID",
    "invalid_traceparent": "Invalid trace context",
    "authentication_failed": "Authentication failed",
    "caller_not_allowed": "Caller is not allowed",
    "validation_failed": "Request validation failed",
    "service_not_ready": "Service is not ready",
    "not_found": "Resource not found",
    "method_not_allowed": "Method not allowed",
    "http_error": "HTTP request failed",
    "internal_error": "Internal server error",
}
_SAFE_RESPONSE_HEADERS = frozenset(
    {
        "Allow",
        "WWW-Authenticate",
    }
)


class CompanyApiProblem(Exception):
    """외부 응답에 안전하게 노출할 수 있는 제한된 API 오류다."""

    def __init__(
        self,
        *,
        status: int,
        code: str,
        request_id: str | None = None,
        retryable: bool = False,
        headers: Mapping[str, str] | None = None,
    ) -> None:
        normalized_status = int(status)
        self.status = (
            normalized_status
            if 400 <= normalized_status <= 599
            else 500
        )
        self.status_code = self.status
        normalized_code = str(code or "").strip()
        self.code = (
            normalized_code
            if normalized_code in _DEFAULT_TITLES
            else "internal_error"
        )
        self.title = _DEFAULT_TITLES[self.code]
        self.request_id = _safe_request_id(request_id)
        self.retryable = bool(retryable)
        self.headers = {
            str(key): str(value)
            for key, value in (headers or {}).items()
            if key in _SAFE_RESPONSE_HEADERS
            and "\r" not in str(value)
            and "\n" not in str(value)
            and len(str(value)) <= 256
        }
        # Exception 문자열에도 내부 상세를 넣지 않아 상위 logger의 우발 노출을 막는다.
        super().__init__(self.code)


def problem_response(
    problem: CompanyApiProblem,
    request_id: str | None = None,
    *,
    traceparent: str | None = None,
) -> JSONResponse:
    effective_request_id = (
        problem.request_id
        or _safe_request_id(request_id)
        or "unavailable"
    )
    headers = {
        "Cache-Control": "no-store",
        "X-Request-ID": effective_request_id,
        **problem.headers,
    }
    if traceparent:
        headers["traceparent"] = traceparent
    return JSONResponse(
        status_code=problem.status,
        media_type="application/problem+json",
        headers=headers,
        content={
            "type": f"urn:boxer-company-api:problem:{problem.code}",
            "title": problem.title,
            "status": problem.status,
            "code": problem.code,
            "requestId": effective_request_id,
            "retryable": problem.retryable,
        },
    )


def install_problem_handlers(app: FastAPI) -> None:
    @app.exception_handler(CompanyApiProblem)
    async def _company_api_problem_handler(
        request: Request,
        exc: CompanyApiProblem,
    ) -> JSONResponse:
        return problem_response(
            exc,
            _request_id_from_request(request),
            traceparent=_traceparent_from_request(request),
        )

    @app.exception_handler(RequestValidationError)
    async def _validation_problem_handler(
        request: Request,
        _exc: RequestValidationError,
    ) -> JSONResponse:
        # Pydantic의 field 이름과 입력값은 응답에 넣지 않아
        # 권한 주입 시도도 반사하지 않는다.
        return problem_response(
            CompanyApiProblem(
                status=422,
                code="validation_failed",
            ),
            _request_id_from_request(request),
            traceparent=_traceparent_from_request(request),
        )

    @app.exception_handler(StarletteHTTPException)
    async def _http_problem_handler(
        request: Request,
        exc: StarletteHTTPException,
    ) -> JSONResponse:
        if exc.status_code == 404:
            code = "not_found"
        elif exc.status_code == 405:
            code = "method_not_allowed"
        elif exc.status_code >= 500:
            code = "internal_error"
        else:
            code = "http_error"
        safe_headers = {}
        if exc.status_code == 405 and exc.headers:
            allow_header = exc.headers.get("Allow")
            if allow_header:
                safe_headers["Allow"] = allow_header
        return problem_response(
            CompanyApiProblem(
                status=exc.status_code,
                code=code,
                retryable=exc.status_code >= 500,
                headers=safe_headers,
            ),
            _request_id_from_request(request),
            traceparent=_traceparent_from_request(request),
        )

    @app.exception_handler(Exception)
    async def _internal_problem_handler(
        request: Request,
        exc: Exception,
    ) -> JSONResponse:
        request_id = _request_id_from_request(request)
        logger.error(
            "Company API request failed request_id=%s error_type=%s",
            request_id or "unavailable",
            type(exc).__name__,
        )
        return problem_response(
            CompanyApiProblem(
                status=500,
                code="internal_error",
                retryable=True,
            ),
            request_id,
            traceparent=_traceparent_from_request(request),
        )


def _request_id_from_request(request: Request) -> str | None:
    state_request_id = getattr(request.state, "request_id", None)
    return (
        _safe_request_id(state_request_id)
        or _safe_request_id(request.headers.get("X-Request-ID"))
    )


def _traceparent_from_request(request: Request) -> str | None:
    value = getattr(request.state, "traceparent", None)
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    return normalized or None


def _safe_request_id(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip()
    if not _REQUEST_ID_PATTERN.fullmatch(normalized):
        return None
    return normalized


__all__ = [
    "CompanyApiProblem",
    "install_problem_handlers",
    "problem_response",
]
