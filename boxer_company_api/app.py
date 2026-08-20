from __future__ import annotations

import re
import time
from typing import Any, Callable, Protocol, cast
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from boxer.observability.request_log import (
    _ensure_request_log_schema,
    _save_request_log_record,
)
from boxer_company.assistant.operations import (
    is_uncertain_company_mutation_result,
    match_live_device_company_operation_route,
    match_mutation_capable_company_operation_route,
)
from boxer_company.automation import (
    AutomationCycleContractError,
    build_default_automation_cycle_service,
)
from boxer_company_api.automation import (
    AutomationCycleInput,
    AutomationCycleUncertainError,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
    serialize_automation_cycle_result,
)
from boxer_company_api.auth import CallerRegistry
from boxer_company_api.observability import emit_api_event
from boxer_company_api.policies import (
    authorize_automation_cycle,
    authorize_turn,
    validate_request_id,
    validate_traceparent,
)
from boxer_company_api.problems import (
    CompanyApiProblem,
    install_problem_handlers,
)
from boxer_company_api.request_guard import MutationRequestGuard
from boxer_company_api.schemas import (
    AssistantTurnInput,
    serialize_result,
)
from boxer_company_api.security import (
    validate_company_api_runtime_security,
)
from boxer_company_api.settings import (
    CompanyApiSettings,
    company_api_local_readiness,
    load_company_api_settings,
)
from boxer_company.routers.device_ssh_security import (
    company_api_device_ssh_context,
)


class _AssistantRuntime(Protocol):
    def answer(self, request: Any) -> Any:
        ...

    def answer_stage(self, request: Any, stage: str) -> Any:
        ...


class _AutomationCoordinator(Protocol):
    def run(self, trigger: Any) -> Any:
        ...


ReadinessProbe = Callable[[], bool]

_SERVICE_NAME = "boxer-company-api"
_TURN_PATH = "/internal/v1/assistant/turns"
_AUTOMATION_CYCLE_PATH = "/internal/v1/automation/cycles"
_RUNTIME_UNSET = object()
_PROBE_UNSET = object()
_AUTOMATION_UNSET = object()
_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)


def create_company_api_app(
    *,
    settings: CompanyApiSettings | None = None,
    assistant_runtime: _AssistantRuntime | None | object = _RUNTIME_UNSET,
    readiness_probe: ReadinessProbe | None | object = _PROBE_UNSET,
    automation_coordinator: (
        _AutomationCoordinator | None | object
    ) = _AUTOMATION_UNSET,
) -> FastAPI:
    """내부 인증 경계와 채널 중립 assistant runtime을 조립한다."""

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
    if automation_coordinator is _AUTOMATION_UNSET:
        automation_runtime: _AutomationCoordinator | None = (
            DurableAutomationCycleCoordinator(
                build_default_automation_cycle_service(),
                JsonAutomationCycleStateStore(
                    api_settings.automation_state_path
                ),
            )
            if api_settings.automation_enabled_cycles
            else None
        )
    else:
        automation_runtime = cast(
            _AutomationCoordinator | None,
            automation_coordinator,
        )
    # Dynamo 같은 새 인프라 없이 현 단일 worker 안에서 Slack redelivery와
    # 같은 장비의 동시 mutation만 막는 최소 중복 억제 경계다.
    mutation_request_guard = MutationRequestGuard()
    # 중앙 request-log는 remote operation의 유일한 감사 저장소다. 기동 때
    # 스키마/쓰기 가능 여부를 확인하고 런타임 장애가 나면 readiness를 latch한다.
    request_log_state = {
        "ready": _initialize_request_log_readiness(
            enabled=api_settings.request_log_enabled,
            db_path=api_settings.request_log_path,
        ),
    }
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
            or not request_log_state["ready"]
        ):
            return False
        try:
            # 운영 probe는 설정과 조립 상태만 확인하며 외부 데이터 소스를
            # 새로 호출하지 않도록 factory에서 고정된 함수를 넘긴다.
            return bool(probe()) and company_api_local_readiness(
                api_settings
            )
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
        domain_request = turn.to_company_request(request_id)
        if turn.routeGroup == "operations" and not api_settings.operations_enabled:
            # PII/admin/mutation이 함께 있는 stage는 API와 Slack rollout 양쪽에서
            # 명시적으로 켜기 전에는 runtime에 진입하지 않는다.
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=False,
            )
        live_device_route = (
            "device_detail"
            if turn.routeGroup == "device_detail"
            else match_live_device_company_operation_route(domain_request)
            if turn.routeGroup == "operations"
            else None
        )
        if live_device_route is not None and not api_settings.live_device_enabled:
            # 코드 우선 배포 상태에서는 read-only operations를 유지하되,
            # strict MDA/SSH 정본이 준비되지 않은 live 장비 경로만 닫는다.
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=False,
            )
        if not is_ready():
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=True,
            )

        # operations라는 넓은 transport stage가 아니라 공통 domain matcher가
        # 확정한 side-effect route와 SSH open 가능 device_detail만 예약한다.
        mutation_route: str | None = None
        if turn.routeGroup == "device_detail":
            mutation_route = "device_detail"
        elif turn.routeGroup == "operations":
            mutation_route = match_mutation_capable_company_operation_route(
                domain_request
            )
        guard_decision = mutation_request_guard.reserve(
            caller_id=principal.caller_id,
            request_id=request_id,
            turn=turn,
            mutation_capable=mutation_route is not None,
        )
        if guard_decision.status == "conflict":
            raise CompanyApiProblem(
                status=409,
                code="request_id_conflict",
                request_id=request_id,
                retryable=False,
            )
        if guard_decision.status == "busy":
            raise CompanyApiProblem(
                status=409,
                code="operation_in_progress",
                request_id=request_id,
                retryable=False,
            )
        if guard_decision.status == "replay":
            # 완료된 동일 요청은 runtime을 다시 실행하지 않고 같은 응답을 돌려준다.
            return JSONResponse(content=dict(guard_decision.payload or {}))
        reservation = guard_decision.reservation

        started_at = time.monotonic()
        device_ssh_state = None
        try:
            typed_runtime = cast(_AssistantRuntime, runtime)
            # API EC2에서 실행되는 모든 SSH 경로는 pinned host key와 사설
            # connect host를 강제하고, Slack local rollback만 기존 경계를 쓴다.
            with company_api_device_ssh_context() as device_ssh_state:
                if turn.routeGroup is None:
                    result = typed_runtime.answer(domain_request)
                else:
                    # rollout client가 고른 route group만 실행해 thread scope가
                    # 앞선 DB/MDA route를 뜻하지 않게 선점하는 일을 막는다.
                    # live 장비 상세는 wire 권한 경계는 별도로 유지하되,
                    # runtime에서는 기존 structured stage 안의 전용 route를 실행한다.
                    runtime_stage = (
                        "structured"
                        if turn.routeGroup == "device_detail"
                        else "freeform"
                        if turn.routeGroup in {"health", "fun"}
                        else turn.routeGroup
                    )
                    result = typed_runtime.answer_stage(
                        domain_request,
                        runtime_stage,
                    )
            payload = serialize_result(result, request_id)
        except Exception:
            side_effect_attempted = bool(
                device_ssh_state is not None
                and device_ssh_state.mutation_attempted
            )
            if reservation is not None:
                # 모든 외부 write transport가 공통 request marker를 전송 직전에
                # 세운다. 사전조회 실패는 target을 해제하고, 전송 뒤 오류만
                # 프로세스 생존 동안 불명 상태로 보존한다.
                if side_effect_attempted:
                    mutation_request_guard.mark_uncertain(reservation)
                else:
                    mutation_request_guard.release(reservation)
            # 예외 문자열에는 credential이나 조회 원문이 섞일 수 있어
            # transport 경계에서는 오류 타입·원문을 응답이나 로그에 넣지 않는다.
            emit_api_event(
                "company_api_turn_failed",
                caller_id=principal.caller_id,
                channel=turn.channel,
                route_group=str(turn.routeGroup or "all"),
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
                # 실제 외부 write 전 실패는 재호출 가능하지만, 전송 뒤 오류는
                # 처리 여부가 불명이라 caller에게 자동 재시도를 권하지 않는다.
                retryable=not side_effect_attempted,
            ) from None

        if reservation is not None:
            assert mutation_route is not None
            if is_uncertain_company_mutation_result(
                mutation_route=mutation_route,
                result=result,
                side_effect_attempted=bool(
                    device_ssh_state is not None
                    and device_ssh_state.mutation_attempted
                ),
            ):
                # route가 예외를 안전한 failed 결과로 감싸도 처리 여부가
                # 불명인 mutation은 완료 cache로 바꾸거나 target을 해제하지 않는다.
                mutation_request_guard.mark_uncertain(reservation)
            else:
                mutation_request_guard.complete(reservation, payload)

        emit_api_event(
            "company_api_turn_completed",
            caller_id=principal.caller_id,
            channel=turn.channel,
            route_group=str(turn.routeGroup or "all"),
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
        if not _persist_turn_request_log(
            turn=turn,
            request_id=request_id,
            route=str(payload["route"]),
            outcome=str(payload["outcome"]),
            message_count=len(payload["messages"]),
            enabled=api_settings.request_log_enabled,
            db_path=api_settings.request_log_path,
        ):
            # 이미 수행한 mutation의 성공 응답을 5xx로 뒤집어 사용자 재실행을
            # 유도하지 않는다. 대신 같은 프로세스의 후속 요청을 모두 막는다.
            request_log_state["ready"] = False
        return JSONResponse(content=payload)

    @app.post(_AUTOMATION_CYCLE_PATH, response_model=None)
    def run_automation_cycle(
        request: Request,
        cycle: AutomationCycleInput,
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
        principal = caller_registry.authenticate(
            request.headers.get("Authorization"),
            request_id,
        )
        authorize_automation_cycle(
            principal,
            cycle.tenantId,
            request_id,
        )
        if cycle.cycle not in api_settings.automation_enabled_cycles:
            # capability만으로 feature-off cycle을 실행할 수 없게 운영 flag를
            # endpoint admission에도 다시 적용한다.
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=False,
            )
        if not is_ready() or automation_runtime is None:
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=False,
            )

        started_at = time.monotonic()
        try:
            # daily/health cycle의 SSH도 assistant turn과 같은 pinned host
            # 경계를 통과하고 transport 자체는 한 번만 실행한다.
            with company_api_device_ssh_context():
                result = automation_runtime.run(
                    cycle.to_trigger(request_id)
                )
            payload = serialize_automation_cycle_result(
                result,
                request_id,
            )
        except AutomationCycleUncertainError:
            raise CompanyApiProblem(
                status=409,
                code="operation_in_progress",
                request_id=request_id,
                retryable=False,
            ) from None
        except AutomationCycleContractError:
            raise CompanyApiProblem(
                status=422,
                code="validation_failed",
                request_id=request_id,
                retryable=False,
            ) from None
        except Exception as exc:
            # cycle은 mutation을 포함할 수 있어 서버가 retryable로 안내하지
            # 않고, payload나 예외 문자열 대신 안전한 오류 타입만 남긴다.
            emit_api_event(
                "company_api_automation_failed",
                caller_id=principal.caller_id,
                cycle=cycle.cycle,
                request_id=request_id,
                status=500,
                error_type=type(exc).__name__,
                duration_ms=int(
                    (time.monotonic() - started_at) * 1_000
                ),
            )
            raise CompanyApiProblem(
                status=500,
                code="internal_error",
                request_id=request_id,
                retryable=False,
            ) from None

        emit_api_event(
            "company_api_automation_completed",
            caller_id=principal.caller_id,
            cycle=cycle.cycle,
            request_id=request_id,
            outcome=str(payload["outcome"]),
            delivery_count=len(payload["deliveries"]),
            status=200,
            duration_ms=int(
                (time.monotonic() - started_at) * 1_000
            ),
        )
        return JSONResponse(content=payload)

    return app


def _persist_turn_request_log(
    *,
    turn: AssistantTurnInput,
    request_id: str,
    route: str,
    outcome: str,
    message_count: int,
    enabled: bool,
    db_path: str,
) -> bool:
    """Slack과 분리된 API 프로세스가 회사 요청 감사 저장소를 소유한다."""

    if not enabled:
        return True
    channel_id = (
        turn.scope.channelContextId
        if turn.scope is not None and turn.scope.channelContextId
        else turn.conversationId
    )
    # PII·SQL·mutation이 섞일 수 있는 operations 원문은 중앙 감사 DB에도
    # 복제하지 않고 route/outcome/correlation만 남긴다.
    request_text = (
        "[민감 operations 요청]"
        if turn.routeGroup == "operations"
        else turn.question
    )
    try:
        _save_request_log_record(
            {
                "sourcePlatform": turn.channel,
                "workspaceId": turn.tenantId,
                "eventType": "assistant_turn",
                "routeName": route,
                "routeMode": "remote",
                "handlerType": "company_api",
                "status": outcome,
                "userId": turn.actorId or "unknown",
                "channelId": channel_id,
                "threadId": turn.conversationId,
                "messageId": request_id,
                "isThreadRoot": False,
                "requestText": request_text,
                "normalizedQuestion": request_text,
                "requestKey": request_id,
                "replyCount": max(0, message_count),
                "metadata": {
                    "routeGroup": turn.routeGroup or "all",
                },
            },
            db_path=db_path,
        )
        return True
    except Exception as exc:
        # 감사 저장 장애가 사용자 답변을 뒤집지 않게 하되, 질문·경로·원문은
        # 로그에 다시 쓰지 않고 안전한 상관 ID와 타입만 남긴다.
        emit_api_event(
            "company_api_request_log_persist_failed",
            request_id=request_id,
            status=500,
            error_type=type(exc).__name__,
        )
        return False


def _initialize_request_log_readiness(
    *,
    enabled: bool,
    db_path: str,
) -> bool:
    """외부 복원 없이 API 중앙 감사 SQLite만 기동 시점에 준비한다."""

    if not enabled:
        return True
    try:
        _ensure_request_log_schema(db_path)
        return True
    except Exception as exc:
        emit_api_event(
            "company_api_request_log_startup_failed",
            status=503,
            error_type=type(exc).__name__,
        )
        return False


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
