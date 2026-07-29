from __future__ import annotations

import re
import time
from typing import Any, Callable, Protocol, cast
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from boxer_company_api.auth import CallerRegistry
from boxer_company_api.observability import emit_api_event
from boxer_company_api.policies import (
    authorize_turn,
    validate_request_id,
    validate_traceparent,
)
from boxer_company_api.problems import (
    CompanyApiProblem,
    install_problem_handlers,
)
from boxer_company_api.schemas import (
    AssistantTurnInput,
    serialize_result,
)
from boxer_company_api.security import (
    validate_company_api_runtime_security,
)
from boxer_company_api.settings import (
    CompanyApiSettings,
    load_company_api_settings,
)


class _AssistantRuntime(Protocol):
    def answer(self, request: Any) -> Any:
        ...


ReadinessProbe = Callable[[], bool]

_SERVICE_NAME = "boxer-company-api"
_TURN_PATH = "/internal/v1/assistant/turns"
_RUNTIME_UNSET = object()
_PROBE_UNSET = object()
_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)


def create_company_api_app(
    *,
    settings: CompanyApiSettings | None = None,
    assistant_runtime: _AssistantRuntime | None | object = _RUNTIME_UNSET,
    readiness_probe: ReadinessProbe | None | object = _PROBE_UNSET,
) -> FastAPI:
    """내부 인증 경계와 회사 read-only runtime을 조립한다."""

    api_settings = settings or load_company_api_settings()
    runtime, default_runtime_ready = _resolve_runtime(
        api_settings,
        assistant_runtime,
    )
    probe = _resolve_readiness_probe(
        readiness_probe,
        default_runtime_ready=default_runtime_ready,
    )
    caller_registry = CallerRegistry(api_settings)
    app = FastAPI(
        title="Boxer Company API",
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )
    install_problem_handlers(app)

    def is_ready() -> bool:
        if (
            not api_settings.authentication_configured
            or runtime is None
            or api_settings.configuration_error is not None
        ):
            return False
        try:
            # 운영 probe는 설정과 조립 상태만 확인하며 외부 데이터 소스를
            # 새로 호출하지 않도록 factory에서 고정된 함수를 넘긴다.
            return bool(probe())
        except Exception:
            return False

    @app.middleware("http")
    async def _transport_headers(
        request: Request,
        call_next: Callable[[Request], Any],
    ) -> Any:
        started_at = time.monotonic()
        supplied_request_id = request.headers.get("X-Request-ID")
        request.state.request_id = (
            supplied_request_id.strip()
            if (
                isinstance(supplied_request_id, str)
                and _REQUEST_ID_PATTERN.fullmatch(
                    supplied_request_id.strip()
                )
            )
            else uuid4().hex
        )
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        response.headers["Cache-Control"] = "no-store"
        traceparent = getattr(request.state, "traceparent", None)
        if isinstance(traceparent, str) and traceparent:
            response.headers["traceparent"] = traceparent
        if response.status_code >= 400:
            emit_api_event(
                "company_api_request_rejected",
                request_id=request.state.request_id,
                status=response.status_code,
                duration_ms=int(
                    (time.monotonic() - started_at) * 1_000
                ),
            )
        return response

    @app.get("/health/live")
    def liveness() -> dict[str, str]:
        # liveness는 프로세스 event loop만 확인하고 dependency probe를 호출하지 않는다.
        return {
            "status": "ok",
            "service": _SERVICE_NAME,
        }

    @app.get("/health/ready")
    def readiness(request: Request) -> dict[str, Any]:
        if not is_ready():
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request.state.request_id,
                retryable=True,
            )
        return {
            "status": "ready",
            "service": _SERVICE_NAME,
            "checks": {
                "authentication": "ok",
                "runtime": "ok",
                "configuration": "ok",
            },
        }

    @app.post(_TURN_PATH, response_model=None)
    def answer_turn(
        request: Request,
        turn: AssistantTurnInput,
    ) -> JSONResponse:
        request_id = validate_request_id(
            request.headers.get("X-Request-ID")
        )
        request.state.request_id = request_id
        traceparent = validate_traceparent(
            request.headers.get("traceparent"),
            request_id,
        )
        request.state.traceparent = traceparent

        # 인증·caller scope를 먼저 끝낸 뒤에만 runtime과 조회 의존성에 진입한다.
        principal = caller_registry.authenticate(
            request.headers.get("Authorization"),
            request_id,
        )
        authorize_turn(principal, turn, request_id)
        if not is_ready():
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=True,
            )

        started_at = time.monotonic()
        domain_request = turn.to_company_request(request_id)
        try:
            result = cast(_AssistantRuntime, runtime).answer(
                domain_request
            )
            payload = serialize_result(result, request_id)
        except Exception:
            # 예외 문자열에는 credential이나 조회 원문이 섞일 수 있어
            # transport 경계에서는 오류 타입·원문을 응답이나 로그에 넣지 않는다.
            emit_api_event(
                "company_api_turn_failed",
                caller_id=principal.caller_id,
                channel=turn.channel,
                request_id=request_id,
                status=500,
                duration_ms=int(
                    (time.monotonic() - started_at) * 1_000
                ),
            )
            raise CompanyApiProblem(
                status=500,
                code="internal_error",
                request_id=request_id,
                retryable=True,
            ) from None

        emit_api_event(
            "company_api_turn_completed",
            caller_id=principal.caller_id,
            channel=turn.channel,
            request_id=request_id,
            route=str(payload["route"]),
            outcome=str(payload["outcome"]),
            fallback_reason=str(
                payload.get("fallbackReason") or ""
            ),
            source_count=len(payload["sources"]),
            used_llm=bool(payload["usedLlm"]),
            status=200,
            duration_ms=int(
                (time.monotonic() - started_at) * 1_000
            ),
        )
        return JSONResponse(content=payload)

    return app


def _resolve_runtime(
    settings: CompanyApiSettings,
    supplied_runtime: _AssistantRuntime | None | object,
) -> tuple[_AssistantRuntime | None, bool]:
    if supplied_runtime is not _RUNTIME_UNSET:
        return cast(_AssistantRuntime | None, supplied_runtime), True
    if not settings.authentication_configured:
        return None, False

    try:
        from boxer.core.utils import _validate_tokens
        from boxer_company.assistant.factory import (
            create_company_assistant_runtime,
        )

        # 직접 app factory를 쓰는 실행 경로에서도 운영 credential 정책과
        # 기존 데이터 소스 설정 검증을 startup 전에 동일하게 적용한다.
        validate_company_api_runtime_security()
        _validate_tokens(
            include_llm=True,
            include_data_sources=True,
        )
        return create_company_assistant_runtime(), True
    except Exception:
        emit_api_event(
            "company_api_runtime_initialization_failed",
            status=503,
        )
        return None, False


def _resolve_readiness_probe(
    supplied_probe: ReadinessProbe | None | object,
    *,
    default_runtime_ready: bool,
) -> ReadinessProbe:
    if supplied_probe is _PROBE_UNSET or supplied_probe is None:
        return lambda: default_runtime_ready
    return cast(ReadinessProbe, supplied_probe)


__all__ = ["create_company_api_app"]
