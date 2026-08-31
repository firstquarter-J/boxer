from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from queue import Empty, SimpleQueue
import re
import stat
import threading
import time
from typing import Any, Callable, Iterator, Protocol, cast
from uuid import uuid4

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from boxer.observability.request_log import (
    _ensure_request_log_schema,
    _initialize_request_log_storage,
    _normalize_request_log_record,
    _save_request_log_record,
)
from boxer.observability.sqlite_store import _connect_sqlite
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantResult,
)
from boxer_company.assistant.operations import (
    is_retryable_company_mutation_result,
    is_uncertain_company_mutation_result,
    match_live_device_company_operation_route,
    match_mutation_capable_company_operation_route,
)
from boxer_company.operation_routing import match_company_operation_route
from boxer_company.assistant.request_log_contract import (
    legacy_company_request_log_route_name,
)
from boxer_company.automation import (
    AutomationCycleContractError,
    build_default_automation_cycle_service,
)
from boxer_company.device_health_alert_ack import (
    claim_device_health_alert_acknowledgement,
    ensure_device_health_alert_ack_schema,
)
from boxer_company.hpa_change_coordinator import (
    HpaChangeCoordinator,
    create_hpa_change_coordinator,
)
from boxer_company_api.automation import (
    AutomationCycleUncertainError,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
)
from boxer_company_api.automation_delivery import (
    AutomationDeliveryAckInput,
    AutomationDeliveryBroker,
    AutomationDeliveryPullInput,
    serialize_automation_delivery_ack,
    serialize_automation_delivery_batch,
)
from boxer_company_api.auth import CallerRegistry
from boxer_company_api.hpa_change_router import create_hpa_change_router
from boxer_company_api.hpa_change_transport import HpaChangeTransportService
from boxer_company_api.observability import emit_api_event
from boxer_company_api.policies import (
    authorize_automation_transport,
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
    DeviceFileDownloadDeliveryActionInput,
    DeviceOperationDeliveryActionInput,
    RequestLogDeliveryActionInput,
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
from boxer_company_api.turn_routing import (
    resolve_effective_turn_route,
)
from boxer_company.routers.device_ssh_security import (
    company_api_device_ssh_context,
)


class _AssistantRuntime(Protocol):
    def answer(
        self,
        request: Any,
        *,
        on_partial_result: Callable[[Any], None] | None = None,
    ) -> Any:
        ...

    def answer_stage(
        self,
        request: Any,
        stage: str,
        *,
        on_partial_result: Callable[[Any], None] | None = None,
    ) -> Any:
        ...


class _AutomationCoordinator(Protocol):
    def run(self, trigger: Any) -> Any:
        ...


class _AutomationDeliveryBroker(Protocol):
    def pull(self, *, tenant_id: str, cycle: str | None = None) -> Any:
        ...

    def acknowledge(
        self,
        *,
        request_id: str,
        tenant_id: str,
        batch_id: str,
        receipts: tuple[Any, ...],
    ) -> Any:
        ...


ReadinessProbe = Callable[[], bool]

_SERVICE_NAME = "boxer-company-api"
_TURN_PATH = "/internal/v1/assistant/turns"
_AUTOMATION_DELIVERY_PULL_PATH = (
    "/internal/v1/automation/deliveries/pull"
)
_AUTOMATION_DELIVERY_ACK_PATH = (
    "/internal/v1/automation/deliveries/ack"
)
_RUNTIME_UNSET = object()
_PROBE_UNSET = object()
_AUTOMATION_UNSET = object()
_AUTOMATION_DELIVERY_UNSET = object()
_HPA_CHANGE_UNSET = object()
_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_NDJSON_MEDIA_TYPE = "application/x-ndjson"
_STREAM_HEARTBEAT_SEC = 15.0
_MAX_STREAM_BYTES = 1_048_576
_BOT_ONLY_MENTION_REQUEST_TEXT = "[Boxer만 멘션한 요청]"


def create_company_api_app(
    *,
    settings: CompanyApiSettings | None = None,
    assistant_runtime: _AssistantRuntime | None | object = _RUNTIME_UNSET,
    readiness_probe: ReadinessProbe | None | object = _PROBE_UNSET,
    automation_coordinator: (
        _AutomationCoordinator | None | object
    ) = _AUTOMATION_UNSET,
    automation_delivery_broker: (
        _AutomationDeliveryBroker | None | object
    ) = _AUTOMATION_DELIVERY_UNSET,
    hpa_change_coordinator: (
        HpaChangeCoordinator | None | object
    ) = _HPA_CHANGE_UNSET,
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
    if hpa_change_coordinator is _HPA_CHANGE_UNSET:
        hpa_change_runtime: HpaChangeCoordinator | None = (
            create_hpa_change_coordinator()
        )
    else:
        hpa_change_runtime = cast(
            HpaChangeCoordinator | None,
            hpa_change_coordinator,
        )
    automation_state_store = (
        JsonAutomationCycleStateStore(api_settings.automation_state_path)
        if api_settings.automation_storage_required
        else None
    )
    if automation_coordinator is _AUTOMATION_UNSET:
        automation_runtime: _AutomationCoordinator | None = (
            DurableAutomationCycleCoordinator(
                build_default_automation_cycle_service(),
                cast(JsonAutomationCycleStateStore, automation_state_store),
            )
            if automation_state_store is not None
            else None
        )
    else:
        automation_runtime = cast(
            _AutomationCoordinator | None,
            automation_coordinator,
        )
    if automation_delivery_broker is _AUTOMATION_DELIVERY_UNSET:
        delivery_runtime: _AutomationDeliveryBroker | None = (
            AutomationDeliveryBroker(
                cast(JsonAutomationCycleStateStore, automation_state_store),
                cast(
                    DurableAutomationCycleCoordinator,
                    automation_runtime,
                ),
            )
            if (
                api_settings.automation_scheduler_enabled
                and automation_state_store is not None
                and automation_runtime is not None
            )
            else None
        )
    else:
        delivery_runtime = cast(
            _AutomationDeliveryBroker | None,
            automation_delivery_broker,
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

    if hpa_change_runtime is not None:
        # HPA HTTP route는 Slack payload 수집과 delivery transport만 받고,
        # GitHub credential·SQLite·상태 전이는 이 API runtime이 소유한다.
        app.include_router(
            create_hpa_change_router(
                service=HpaChangeTransportService(hpa_change_runtime),
                caller_registry=caller_registry,
                is_ready=is_ready,
            )
        )
        # SQLite connection은 app shutdown 때 닫고 Slack process에는 복제하지 않는다.
        app.add_event_handler("shutdown", hpa_change_runtime.close)

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
    ) -> JSONResponse | StreamingResponse:
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
        hinted_domain_request = turn.to_company_request(request_id)
        route_decision = resolve_effective_turn_route(
            hinted_domain_request,
            client_route_group=turn.routeGroup,
        )
        # capability는 caller가 보낸 routeGroup이 아니라 서버 matcher가
        # 확정한 민감 범위로 검사해 None/freeform/structured 우회를 막는다.
        authorize_turn(
            principal,
            turn,
            request_id,
            effective_route_group=route_decision.route_group,
        )
        if route_decision.client_hint_mismatch:
            # 권한·caller scope 확인 뒤 안전한 일반 validation problem만
            # 반환하고 어떤 matcher 이름이나 질문 원문도 노출하지 않는다.
            raise CompanyApiProblem(
                status=422,
                code="validation_failed",
                request_id=request_id,
                retryable=False,
            )
        if route_decision.route_group != turn.routeGroup:
            # 이후 feature flag, request-log, mutation/replay guard와 runtime이
            # 모두 같은 server-owned effective group 하나를 사용한다.
            turn = turn.model_copy(
                update={"routeGroup": route_decision.route_group}
            )
        domain_request = turn.to_company_request(request_id)
        request_log_delivery_receipt = isinstance(
            turn.operationAction,
            RequestLogDeliveryActionInput,
        )
        delivery_receipt = isinstance(
            turn.operationAction,
            (
                DeviceFileDownloadDeliveryActionInput,
                DeviceOperationDeliveryActionInput,
                RequestLogDeliveryActionInput,
            ),
        )
        if (
            turn.routeGroup == "operations"
            and not api_settings.operations_enabled
            and not request_log_delivery_receipt
        ):
            # PII/admin/mutation이 함께 있는 stage는 API와 Slack rollout 양쪽에서
            # 명시적으로 켜기 전에는 runtime에 진입하지 않는다. 이미 수행된
            # Slack 전달 receipt는 domain feature-off와 무관하게 마감한다.
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=False,
            )
        live_device_route = (
            None
            if request_log_delivery_receipt
            else "device_detail"
            if turn.routeGroup == "device_detail"
            else match_live_device_company_operation_route(domain_request)
            if turn.routeGroup == "operations"
            else None
        )
        if live_device_route is not None and not api_settings.live_device_enabled:
            # 코드 우선 배포 상태에서는 read-only operations를 유지하되,
            # MDA/SSH 설정을 검증하지 않은 live 장비 경로만 닫는다.
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

        if request_log_delivery_receipt:
            # Slack 전달 receipt는 domain route를 재실행하지 않고 같은
            # message identity의 pending row만 최종 상태로 upsert한다.
            receipt_started_at = time.monotonic()
            delivery_state = _persist_turn_request_log_delivery(
                turn=turn,
                request_id=request_id,
                action=turn.operationAction,
                enabled=api_settings.request_log_enabled,
                db_path=api_settings.request_log_path,
            )
            if delivery_state == "conflict":
                raise CompanyApiProblem(
                    status=409,
                    code="request_id_conflict",
                    request_id=request_id,
                    retryable=False,
                )
            if delivery_state == "failed":
                request_log_state["ready"] = False
                raise CompanyApiProblem(
                    status=500,
                    code="internal_error",
                    request_id=request_id,
                    retryable=False,
                )
            if delivery_state == "disabled":
                # operations 실행 결과를 받은 뒤 감사 저장소가 꺼진 receipt를
                # 성공으로 가장하지 않는다. 잘못 조립된 direct app도 fail-closed한다.
                request_log_state["ready"] = False
                raise CompanyApiProblem(
                    status=503,
                    code="service_not_ready",
                    request_id=request_id,
                    retryable=False,
                )
            receipt_payload = serialize_result(
                CompanyAssistantResult(
                    route="request_log_delivery",
                    outcome="answered",
                    messages=(
                        AssistantMessage(
                            body="요청 로그 전달 상태를 반영했어",
                            mention_actor=False,
                        ),
                    ),
                ),
                request_id,
            )
            emit_api_event(
                "company_api_turn_completed",
                caller_id=principal.caller_id,
                channel=turn.channel,
                route_group="operations",
                request_id=request_id,
                route="request_log_delivery",
                outcome="answered",
                fallback_reason="",
                source_count=0,
                used_llm=False,
                status=200,
                duration_ms=int(
                    (time.monotonic() - receipt_started_at) * 1_000
                ),
            )
            return JSONResponse(content=receipt_payload)

        # operations라는 넓은 transport stage가 아니라 공통 domain matcher가
        # 확정한 side-effect route와 SSH open 가능 device_detail만 예약한다.
        mutation_route: str | None = None
        if delivery_receipt:
            # 초기 다운로드/장비 작업 turn은 이미 같은 request ID로 완료
            # cache에 있다. URL 없는 typed receipt와 각 route의 completed
            # guard가 별도로 멱등 처리하므로 원 요청 guard를 우회한다.
            mutation_route = None
        elif turn.routeGroup == "device_detail":
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
            # NDJSON을 요청했어도 완료 replay는 기존 JSON 200 하나로 반환한다.
            # client는 이를 final-only 응답으로 해석해 runtime을 재실행하지 않는다.
            return JSONResponse(content=dict(guard_decision.payload or {}))
        reservation = guard_decision.reservation

        operation_action_name = str(
            getattr(turn.operationAction, "name", None) or ""
        )
        staged_request_log_route: str | None = None
        if (
            turn.routeGroup == "operations"
            and turn.auditContext is not None
            and not delivery_receipt
            and operation_action_name
            != "device_diagnostic_followup_probe"
        ):
            # Slack가 local 저장을 닫기 전에 API가 먼저 감사 소유권을
            # 확보한다. runtime/직렬화 예외도 masked row 없이 사라지지 않고,
            # 동일 request ID replay도 pending row보다 먼저 완료되지 않는다.
            staged_request_log_route = (
                match_company_operation_route(domain_request)
                or "operations"
            )
            if not _persist_turn_request_log(
                turn=turn,
                request_id=request_id,
                route=staged_request_log_route,
                outcome="pending_execution",
                message_count=0,
                enabled=api_settings.request_log_enabled,
                db_path=api_settings.request_log_path,
                status_override="pending_execution",
            ):
                request_log_state["ready"] = False
                if reservation is not None:
                    mutation_request_guard.release(reservation)
                raise CompanyApiProblem(
                    status=503,
                    code="service_not_ready",
                    request_id=request_id,
                    retryable=False,
                )

        def execute_turn(
            on_partial_result: Callable[[Any], None] | None = None,
        ) -> dict[str, Any]:
            """runtime 실행과 guard·감사 마감을 하나의 worker 경계에서 끝낸다."""

            started_at = time.monotonic()
            device_ssh_state = None
            try:
                typed_runtime = cast(_AssistantRuntime, runtime)
                # API context는 host 선택을 바꾸지 않고 side-effect marker와
                # 요청당 mutation/open 횟수 제한만 활성화한다.
                with company_api_device_ssh_context() as device_ssh_state:
                    if turn.routeGroup is None:
                        result = (
                            typed_runtime.answer(
                                domain_request,
                                on_partial_result=on_partial_result,
                            )
                            if on_partial_result is not None
                            else typed_runtime.answer(domain_request)
                        )
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
                        result = (
                            typed_runtime.answer_stage(
                                domain_request,
                                runtime_stage,
                                on_partial_result=on_partial_result,
                            )
                            if on_partial_result is not None
                            else typed_runtime.answer_stage(
                                domain_request,
                                runtime_stage,
                            )
                        )
                payload = serialize_result(result, request_id)
            except Exception as exc:
                side_effect_attempted = bool(
                    device_ssh_state is not None
                    and device_ssh_state.mutation_attempted
                )
                if staged_request_log_route is not None and not (
                    _persist_turn_request_log(
                        turn=turn,
                        request_id=request_id,
                        route=staged_request_log_route,
                        outcome=(
                            "uncertain"
                            if side_effect_attempted
                            else "failed"
                        ),
                        message_count=0,
                        enabled=api_settings.request_log_enabled,
                        db_path=api_settings.request_log_path,
                        status_override="error",
                        error_type=type(exc).__name__,
                    )
                ):
                    request_log_state["ready"] = False
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

            mutation_result_uncertain = False
            mutation_result_retryable = False
            if reservation is not None:
                assert mutation_route is not None
                mutation_result_retryable = (
                    is_retryable_company_mutation_result(
                        mutation_route=mutation_route,
                        result=result,
                    )
                )
                mutation_result_uncertain = (
                    is_uncertain_company_mutation_result(
                        mutation_route=mutation_route,
                        result=result,
                        side_effect_attempted=bool(
                            device_ssh_state is not None
                            and device_ssh_state.mutation_attempted
                        ),
                    )
                )
                if mutation_result_uncertain:
                    # route가 예외를 안전한 failed 결과로 감싸도 처리 여부가
                    # 불명인 mutation은 완료 cache로 바꾸거나 target을 해제하지 않는다.
                    mutation_request_guard.mark_uncertain(reservation)
            should_persist_request_log = (
                not delivery_receipt
                and not (
                    getattr(turn.operationAction, "name", None)
                    == "device_diagnostic_followup_probe"
                    and str(payload["outcome"]) == "no_evidence"
                    and str(payload.get("fallbackReason") or "")
                    == "diagnostic_snapshot_missing"
                )
            )
            request_log_persisted = (
                not should_persist_request_log
                or _persist_turn_request_log(
                    turn=turn,
                    request_id=request_id,
                    route=str(payload["route"]),
                    outcome=str(payload["outcome"]),
                    message_count=len(payload["messages"]),
                    enabled=api_settings.request_log_enabled,
                    db_path=api_settings.request_log_path,
                    operation_result=getattr(
                        result,
                        "operation_result",
                        None,
                    ),
                )
            )
            if not request_log_persisted:
                # 이미 수행한 mutation의 성공 응답을 5xx로 뒤집어 사용자 재실행을
                # 유도하지 않는다. 대신 같은 프로세스의 후속 요청을 모두 막는다.
                request_log_state["ready"] = False
            if reservation is not None and not mutation_result_uncertain:
                if request_log_persisted:
                    if mutation_result_retryable:
                        # unique ACK claim은 같은 request ID로 다시 읽어도
                        # 중복 side effect가 없어 transient SQLite 실패를 고정하지 않는다.
                        mutation_request_guard.release(reservation)
                    else:
                        # pending_delivery가 transaction으로 보인 뒤에만 replay에
                        # final payload를 공개해 receipt가 missing row를 보지 않게 한다.
                        mutation_request_guard.complete(reservation, payload)
                else:
                    mutation_request_guard.mark_uncertain(reservation)

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
            return payload

        if _accepts_ndjson(request):
            frame_queue: SimpleQueue[tuple[str, bytes]] = SimpleQueue()

            def enqueue_partial(partial_result: Any) -> None:
                # callback 시점에 바로 직렬화해 mutable domain 결과가 뒤에서
                # 바뀌더라도 Slack에 관찰된 부분 결과 순서를 보존한다.
                frame_queue.put(
                    (
                        "partial",
                        _encode_ndjson_frame(
                            {
                                "type": "partial",
                                "result": serialize_result(
                                    partial_result,
                                    request_id,
                                ),
                            }
                        ),
                    )
                )

            def run_stream_worker() -> None:
                # client 연결 수명과 분리된 daemon worker가 mutation guard와
                # 중앙 request-log를 끝까지 마감한 뒤 terminal frame을 넣는다.
                try:
                    payload = execute_turn(enqueue_partial)
                except CompanyApiProblem as problem:
                    frame_queue.put(
                        (
                            "error",
                            _encode_ndjson_frame(
                                {
                                    "type": "error",
                                    "problem": _serialize_stream_problem(
                                        problem,
                                        request_id,
                                    ),
                                }
                            ),
                        )
                    )
                except Exception:
                    frame_queue.put(
                        (
                            "error",
                            _encode_ndjson_frame(
                                {
                                    "type": "error",
                                    "problem": _serialize_stream_problem(
                                        CompanyApiProblem(
                                            status=500,
                                            code="internal_error",
                                            request_id=request_id,
                                            retryable=False,
                                        ),
                                        request_id,
                                    ),
                                }
                            ),
                        )
                    )
                else:
                    # execute_turn이 guard complete/uncertain과 request-log를
                    # 모두 끝낸 뒤에만 caller가 final을 관찰할 수 있다.
                    frame_queue.put(
                        (
                            "final",
                            _encode_ndjson_frame(
                                {"type": "final", "result": payload}
                            ),
                        )
                    )

            worker = threading.Thread(
                target=run_stream_worker,
                name="boxer-company-api-turn-stream",
                daemon=True,
            )
            try:
                worker.start()
            except Exception as exc:
                if staged_request_log_route is not None and not (
                    _persist_turn_request_log(
                        turn=turn,
                        request_id=request_id,
                        route=staged_request_log_route,
                        outcome="failed",
                        message_count=0,
                        enabled=api_settings.request_log_enabled,
                        db_path=api_settings.request_log_path,
                        status_override="error",
                        error_type=type(exc).__name__,
                    )
                ):
                    request_log_state["ready"] = False
                if reservation is not None:
                    mutation_request_guard.release(reservation)
                raise CompanyApiProblem(
                    status=500,
                    code="internal_error",
                    request_id=request_id,
                    retryable=True,
                ) from None
            return StreamingResponse(
                _stream_ndjson_frames(
                    frame_queue,
                    request_id=request_id,
                ),
                media_type=_NDJSON_MEDIA_TYPE,
                headers={
                    "Cache-Control": "no-store",
                    "X-Accel-Buffering": "no",
                },
            )

        return JSONResponse(content=execute_turn())

    @app.post(_AUTOMATION_DELIVERY_PULL_PATH, response_model=None)
    def pull_automation_delivery(
        request: Request,
        delivery_request: AutomationDeliveryPullInput,
    ) -> JSONResponse:
        request_id, principal = _authenticate_automation_transport_request(
            request,
            caller_registry=caller_registry,
            tenant_id=delivery_request.tenantId,
        )
        if (
            not api_settings.automation_scheduler_enabled
            or delivery_runtime is None
            or not is_ready()
        ):
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=False,
            )
        started_at = time.monotonic()
        try:
            batch = delivery_runtime.pull(
                tenant_id=delivery_request.tenantId,
                cycle=delivery_request.cycle,
            )
        except AutomationCycleContractError:
            raise CompanyApiProblem(
                status=422,
                code="validation_failed",
                request_id=request_id,
                retryable=False,
            ) from None
        except Exception as exc:
            emit_api_event(
                "company_api_automation_delivery_pull_failed",
                caller_id=principal.caller_id,
                request_id=request_id,
                error_type=type(exc).__name__,
                status=500,
            )
            raise CompanyApiProblem(
                status=500,
                code="internal_error",
                request_id=request_id,
                retryable=False,
            ) from None
        payload = serialize_automation_delivery_batch(batch, request_id)
        emit_api_event(
            "company_api_automation_delivery_pulled",
            caller_id=principal.caller_id,
            request_id=request_id,
            cycle=(batch.cycle if batch is not None else ""),
            has_batch=batch is not None,
            status=200,
            duration_ms=int((time.monotonic() - started_at) * 1_000),
        )
        return JSONResponse(content=payload)

    @app.post(_AUTOMATION_DELIVERY_ACK_PATH, response_model=None)
    def acknowledge_automation_delivery(
        request: Request,
        acknowledgement: AutomationDeliveryAckInput,
    ) -> JSONResponse:
        request_id, principal = _authenticate_automation_transport_request(
            request,
            caller_registry=caller_registry,
            tenant_id=acknowledgement.tenantId,
        )
        if (
            not api_settings.automation_scheduler_enabled
            or delivery_runtime is None
            or not is_ready()
        ):
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=False,
            )
        started_at = time.monotonic()
        try:
            result = delivery_runtime.acknowledge(
                request_id=request_id,
                tenant_id=acknowledgement.tenantId,
                batch_id=acknowledgement.batchId,
                receipts=acknowledgement.to_receipts(),
            )
            payload = serialize_automation_delivery_ack(
                batch_id=acknowledgement.batchId,
                result=result,
                request_id=request_id,
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
            emit_api_event(
                "company_api_automation_delivery_ack_failed",
                caller_id=principal.caller_id,
                request_id=request_id,
                error_type=type(exc).__name__,
                status=500,
            )
            raise CompanyApiProblem(
                status=500,
                code="internal_error",
                request_id=request_id,
                retryable=False,
            ) from None
        emit_api_event(
            "company_api_automation_delivery_acknowledged",
            caller_id=principal.caller_id,
            request_id=request_id,
            acknowledged=bool(payload["acknowledged"]),
            status=200,
            duration_ms=int((time.monotonic() - started_at) * 1_000),
        )
        return JSONResponse(content=payload)

    return app


def _authenticate_automation_transport_request(
    request: Request,
    *,
    caller_registry: CallerRegistry,
    tenant_id: str,
) -> tuple[str, Any]:
    """pull/ACK가 같은 request ID·trace·caller 경계를 공유하게 한다."""

    request_id = validate_request_id(request.headers.get("X-Request-ID"))
    request.state.request_id = request_id
    request.state.traceparent = validate_traceparent(
        request.headers.get("traceparent"),
        request_id,
    )
    principal = caller_registry.authenticate(
        request.headers.get("Authorization"),
        request_id,
    )
    authorize_automation_transport(principal, tenant_id, request_id)
    return request_id, principal


def _accepts_ndjson(request: Request) -> bool:
    """명시적으로 NDJSON을 요청한 내부 caller에만 stream transport를 연다."""

    return any(
        item.split(";", 1)[0].strip().lower() == _NDJSON_MEDIA_TYPE
        for item in str(request.headers.get("Accept") or "").split(",")
    )


def _encode_ndjson_frame(frame: dict[str, Any]) -> bytes:
    """frame 하나를 strict UTF-8 JSON object와 단일 newline으로 만든다."""

    return (
        json.dumps(
            frame,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


def _serialize_stream_problem(
    problem: CompanyApiProblem,
    request_id: str,
) -> dict[str, Any]:
    """응답 header가 이미 열린 뒤에도 raw 예외 없이 problem 계약만 보낸다."""

    effective_request_id = problem.request_id or request_id
    return {
        "type": f"urn:boxer-company-api:problem:{problem.code}",
        "title": problem.title,
        "status": problem.status,
        "code": problem.code,
        "requestId": effective_request_id,
        "retryable": problem.retryable,
    }


def _stream_ndjson_frames(
    frame_queue: SimpleQueue[tuple[str, bytes]],
    *,
    request_id: str,
) -> Iterator[bytes]:
    """worker terminal과 무관하게 연결이 살아 있는 동안 heartbeat를 보낸다."""

    emitted_bytes = 0
    budget_error = _encode_ndjson_frame(
        {
            "type": "error",
            "problem": _serialize_stream_problem(
                CompanyApiProblem(
                    status=500,
                    code="internal_error",
                    request_id=request_id,
                    retryable=False,
                ),
                request_id,
            ),
        }
    )

    def emit_budget_error() -> Iterator[bytes]:
        if emitted_bytes + len(budget_error) <= _MAX_STREAM_BYTES:
            yield budget_error

    while True:
        try:
            frame_type, encoded = frame_queue.get(
                timeout=_STREAM_HEARTBEAT_SEC
            )
        except Empty:
            encoded = _encode_ndjson_frame(
                {"type": "heartbeat", "requestId": request_id}
            )
            # 부분/heartbeat 뒤에도 terminal error 하나를 보낼 공간을 남겨
            # stream 전체가 client의 1 MiB 계약을 넘지 않게 한다.
            if (
                emitted_bytes + len(encoded) + len(budget_error)
                > _MAX_STREAM_BYTES
            ):
                yield from emit_budget_error()
                return
            yield encoded
            emitted_bytes += len(encoded)
            continue
        if frame_type in {"final", "error"}:
            if emitted_bytes + len(encoded) > _MAX_STREAM_BYTES:
                yield from emit_budget_error()
                return
            yield encoded
            return
        if (
            emitted_bytes + len(encoded) + len(budget_error)
            > _MAX_STREAM_BYTES
        ):
            yield from emit_budget_error()
            return
        yield encoded
        emitted_bytes += len(encoded)


def _safe_operation_request_log_metadata(
    operation_result: Any,
) -> dict[str, str]:
    """허용한 스캐너 패치 receipt만 중앙 감사 metadata로 축약한다."""

    if not isinstance(operation_result, dict) or frozenset(
        operation_result
    ) != {
        "kind",
        "deviceName",
        "status",
        "scriptSha256",
    }:
        return {}
    device_name = str(operation_result.get("deviceName") or "")
    status = str(operation_result.get("status") or "")
    script_sha = str(operation_result.get("scriptSha256") or "")
    if (
        operation_result.get("kind") != "device_scanner_abi_patch"
        or not re.fullmatch(r"MB2-[A-Z0-9]+", device_name)
        or status not in {"repair_success", "no_action_required"}
        or not re.fullmatch(r"[a-f0-9]{64}", script_sha)
    ):
        return {}
    return {
        "deviceName": device_name,
        "operationStatus": status,
        "scriptSha256": script_sha,
    }


def _persist_turn_request_log(
    *,
    turn: AssistantTurnInput,
    request_id: str,
    route: str,
    outcome: str,
    message_count: int,
    enabled: bool,
    db_path: str,
    status_override: str | None = None,
    error_type: str | None = None,
    operation_result: Any = None,
) -> bool:
    """Slack과 분리된 API 프로세스가 회사 요청 감사 저장소를 소유한다."""

    if not enabled:
        return True
    if not _secure_request_log_leaf(Path(db_path)):
        # startup 뒤 삭제·권한 교체된 DB를 schema helper가 빈 파일로
        # 재생성해 이전 감사 이력을 덮는 일을 막는다.
        return False
    audit_context = turn.auditContext
    channel_id = (
        audit_context.channelId
        if audit_context is not None
        else turn.scope.channelContextId
        if turn.scope is not None and turn.scope.channelContextId
        else turn.conversationId
    )
    # PII·SQL·mutation이 섞일 수 있는 operations 원문은 중앙 감사 DB에도
    # 복제하지 않고 route/outcome/correlation만 남긴다. 질문이 비어 있는
    # freeform은 legacy Slack의 bot-only mention 감사 의미만 안전하게 남긴다.
    if turn.routeGroup == "operations":
        request_text = "[민감 operations 요청]"
        normalized_question: str | None = request_text
    elif turn.question:
        request_text = turn.question
        normalized_question = request_text
    else:
        request_text = _BOT_ONLY_MENTION_REQUEST_TEXT
        normalized_question = None
    # Slack renderer가 최종 전달 receipt를 보내기 전에는 답변 성공으로
    # 확정하지 않는다. auditContext 없는 기존 direct 호출 계약은 유지한다.
    pending_delivery = (
        turn.routeGroup == "operations" and audit_context is not None
    )
    record_status = (
        status_override
        or ("pending_delivery" if pending_delivery else outcome)
    )
    request_log_metadata = {
        "routeGroup": turn.routeGroup or "all",
        "domainOutcome": outcome,
    }
    request_log_metadata.update(
        _safe_operation_request_log_metadata(operation_result)
    )
    try:
        _save_request_log_record(
            {
                "sourcePlatform": turn.channel,
                "workspaceId": turn.tenantId,
                "eventType": (
                    audit_context.eventType
                    if audit_context is not None
                    else "assistant_turn"
                ),
                "routeName": legacy_company_request_log_route_name(route),
                "routeMode": "remote",
                "handlerType": "company_api",
                "status": record_status,
                "userId": turn.actorId or "unknown",
                "userName": (
                    audit_context.userName
                    if audit_context is not None
                    else None
                ),
                "channelId": channel_id,
                "threadId": (
                    audit_context.threadId
                    if audit_context is not None
                    else turn.conversationId
                ),
                "messageId": (
                    audit_context.messageId
                    if audit_context is not None
                    else request_id
                ),
                "isThreadRoot": (
                    audit_context.isThreadRoot
                    if audit_context is not None
                    else False
                ),
                "permalink": (
                    audit_context.permalink
                    if audit_context is not None
                    else None
                ),
                "threadPermalink": (
                    audit_context.threadPermalink
                    if audit_context is not None
                    else None
                ),
                "requestText": request_text,
                "normalizedQuestion": normalized_question,
                "requestKey": request_id,
                "replyCount": (
                    0
                    if record_status
                    in {"pending_execution", "pending_delivery"}
                    else max(0, message_count)
                ),
                "errorType": error_type,
                "metadata": request_log_metadata,
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


def _persist_turn_request_log_delivery(
    *,
    turn: AssistantTurnInput,
    request_id: str,
    action: RequestLogDeliveryActionInput,
    enabled: bool,
    db_path: str,
) -> str:
    """기존 pending row만 transaction 안에서 exact-once로 마감한다."""

    if not enabled:
        return "disabled"
    if not _secure_request_log_leaf(Path(db_path)):
        return "failed"
    audit_context = turn.auditContext
    if audit_context is None:
        return "conflict"
    fingerprint_payload = {
        "delivered": action.delivered,
        "replyCount": action.replyCount,
        "firstRepliedAtUtc": action.firstRepliedAtUtc,
        "errorType": action.errorType,
    }
    receipt_fingerprint = hashlib.sha256(
        json.dumps(
            fingerprint_payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    connection = None
    try:
        # timezone 정규화까지 저장 실패 경계 안에 둬 raw 500이 transport로
        # 새지 않고 readiness latch와 typed problem 계약을 타게 한다.
        normalized_delivery = _normalize_request_log_record(
            {
                "sourcePlatform": "slack",
                "workspaceId": turn.tenantId,
                "eventType": audit_context.eventType,
                "routeName": "unknown",
                "routeMode": "remote",
                "handlerType": "unknown",
                "status": "error",
                "userId": turn.actorId or "unknown",
                "userName": audit_context.userName,
                "channelId": audit_context.channelId,
                "threadId": audit_context.threadId,
                "messageId": audit_context.messageId,
                "isThreadRoot": audit_context.isThreadRoot,
                "permalink": audit_context.permalink,
                "threadPermalink": audit_context.threadPermalink,
                "requestText": "[민감 operations 요청]",
                "normalizedQuestion": "[민감 operations 요청]",
                "requestKey": request_id,
                "replyCount": action.replyCount,
                "firstRepliedAtUtc": action.firstRepliedAtUtc,
                "errorType": action.errorType,
            }
        )
        actual_path = _ensure_request_log_schema(db_path)
        connection = _connect_sqlite(actual_path)
        # check와 update 사이에 다른 receipt가 끼지 않도록 writer lock을
        # 먼저 잡는다. 서로 다른 request ID는 기존 row를 절대 덮지 못한다.
        connection.execute("BEGIN IMMEDIATE")
        row = connection.execute(
            """
            SELECT
                seq,
                workspaceId,
                eventType,
                routeName,
                status,
                userId,
                userName,
                threadId,
                isThreadRoot,
                permalink,
                threadPermalink,
                requestKey,
                errorType,
                metadataJson
            FROM request_log
            WHERE sourcePlatform = ?
              AND channelId = ?
              AND messageId = ?
            """,
            (
                "slack",
                audit_context.channelId,
                audit_context.messageId,
            ),
        ).fetchone()
        expected_identity = (
            turn.tenantId,
            audit_context.eventType,
            turn.actorId or "unknown",
            audit_context.userName,
            audit_context.threadId,
            int(audit_context.isThreadRoot),
            audit_context.permalink,
            audit_context.threadPermalink,
            request_id,
        )
        actual_identity = (
            str(row["workspaceId"] or "") if row is not None else "",
            str(row["eventType"] or "") if row is not None else "",
            str(row["userId"] or "") if row is not None else "",
            row["userName"] if row is not None else None,
            str(row["threadId"] or "") if row is not None else "",
            int(row["isThreadRoot"] or 0) if row is not None else -1,
            row["permalink"] if row is not None else None,
            row["threadPermalink"] if row is not None else None,
            str(row["requestKey"] or "") if row is not None else "",
        )
        if (
            row is None
            or actual_identity != expected_identity
            or str(row["routeName"] or "") in {"", "unknown"}
        ):
            connection.execute("ROLLBACK")
            return "conflict"

        current_status = str(row["status"] or "")
        existing_metadata: dict[str, Any] = {}
        raw_metadata = str(row["metadataJson"] or "").strip()
        if raw_metadata:
            parsed_metadata = json.loads(raw_metadata)
            if not isinstance(parsed_metadata, dict):
                connection.execute("ROLLBACK")
                return "conflict"
            existing_metadata = parsed_metadata
        existing_fingerprint = str(
            existing_metadata.get("deliveryReceiptFingerprint") or ""
        ).strip()
        if existing_fingerprint:
            connection.execute("ROLLBACK")
            return (
                "replay"
                if existing_fingerprint == receipt_fingerprint
                else "conflict"
            )
        domain_outcome = str(
            existing_metadata.get("domainOutcome") or ""
        ).strip()
        pending_domain_outcomes = {
            "answered",
            "no_evidence",
            "needs_input",
            "denied",
            "failed",
        }
        exception_delivery = bool(
            current_status == "error"
            and domain_outcome in {"failed", "uncertain"}
        )
        if not (
            (
                current_status == "pending_delivery"
                and domain_outcome in pending_domain_outcomes
            )
            or exception_delivery
        ):
            connection.execute("ROLLBACK")
            return "conflict"
        normalized_delivery["status"] = (
            "error"
            if exception_delivery or not action.delivered
            else domain_outcome
        )
        if (
            exception_delivery
            and normalized_delivery["errorType"] is None
        ):
            # API 실행 오류를 Slack 실패 안내의 성공 receipt가 지우지 않게
            # 원래 안전한 오류 타입을 보존한다.
            normalized_delivery["errorType"] = row["errorType"]

        existing_metadata.update(
            {
                "routeGroup": "operations",
                "deliveryRequestId": request_id,
                "deliveryReceiptFingerprint": receipt_fingerprint,
            }
        )
        cursor = connection.execute(
            """
            UPDATE request_log
            SET status = ?,
                replyCount = ?,
                firstRepliedAtUtc = ?,
                firstRepliedAtLocal = ?,
                errorType = ?,
                metadataJson = ?
            WHERE seq = ?
              AND status = ?
              AND requestKey = ?
            """,
            (
                normalized_delivery["status"],
                normalized_delivery["replyCount"],
                normalized_delivery["firstRepliedAtUtc"],
                normalized_delivery["firstRepliedAtLocal"],
                normalized_delivery["errorType"],
                json.dumps(
                    existing_metadata,
                    ensure_ascii=False,
                    separators=(",", ":"),
                ),
                row["seq"],
                current_status,
                request_id,
            ),
        )
        if cursor.rowcount != 1:
            connection.execute("ROLLBACK")
            return "conflict"
        connection.execute("COMMIT")
        return "applied"
    except Exception as exc:
        if connection is not None:
            try:
                connection.execute("ROLLBACK")
            except Exception:
                pass
        emit_api_event(
            "company_api_request_log_delivery_persist_failed",
            request_id=request_id,
            status=500,
            error_type=type(exc).__name__,
        )
        return "failed"
    finally:
        if connection is not None:
            connection.close()


def _initialize_request_log_readiness(
    *,
    enabled: bool,
    db_path: str,
) -> bool:
    """private local 경계를 확인한 뒤 restore-before-schema로 준비한다."""

    if not enabled:
        return True
    try:
        path, was_missing = _prepare_request_log_local_path(
            db_path
        )
        # core 정본이 configured S3 restore를 먼저 수행한 다음 schema를
        # 보장한다. 직접 settings를 주입한 테스트도 같은 순서를 유지한 채
        # 마지막 ensure로 schema 존재를 확인한다.
        _initialize_request_log_storage(db_path=path)
        _ensure_request_log_schema(path)
        # 확인 완료는 같은 SQLite 안의 회사 전용 unique claim을 정본으로
        # 쓴다. 서비스가 요청을 받기 전에 두 스키마를 함께 준비한다.
        ensure_device_health_alert_ack_schema(path)
        if was_missing:
            # restore/schema가 만든 leaf만 소유자·파일 종류를 먼저 확인한 뒤
            # 권한을 축소한다. 기존 unsafe leaf는 위 선검사에서 그대로 거부한다.
            if not _request_log_leaf_is_owned_regular(path):
                raise RuntimeError("request log SQLite file is unsafe")
            path.chmod(0o600)
        if not _secure_request_log_leaf(path):
            raise RuntimeError("request log SQLite file is unsafe")
        return True
    except Exception as exc:
        emit_api_event(
            "company_api_request_log_startup_failed",
            status=503,
            error_type=type(exc).__name__,
        )
        return False


def _prepare_request_log_local_path(
    db_path: str,
) -> tuple[Path, bool]:
    """restore 전에 private parent와 기존 leaf만 검사하고 생성하지 않는다."""

    raw_path = Path(str(db_path or "").strip()).expanduser()
    if not raw_path.is_absolute() or raw_path == Path("/"):
        raise ValueError("request log SQLite path must be absolute")
    canonical_path = raw_path.resolve(strict=False)
    if canonical_path != raw_path:
        raise ValueError("request log SQLite path must be canonical")
    parent = raw_path.parent
    parent_stat = parent.lstat()
    if (
        parent.is_symlink()
        or not stat.S_ISDIR(parent_stat.st_mode)
        or parent_stat.st_uid != os.geteuid()
        or parent_stat.st_mode & 0o077
        or not os.access(parent, os.W_OK | os.X_OK)
    ):
        raise PermissionError("request log SQLite parent is unsafe")

    if raw_path.exists() or raw_path.is_symlink():
        if not _secure_request_log_leaf(raw_path):
            raise PermissionError("request log SQLite file is unsafe")
        return raw_path, False
    # only_if_missing restore가 실제로 remote snapshot을 선택할 수 있도록
    # 빈 placeholder도 만들지 않는다. core initializer가 restore/schema를
    # 끝낸 뒤 소유자·regular file·0600을 다시 검증한다.
    return raw_path, True


def _request_log_leaf_is_owned_regular(path: Path) -> bool:
    try:
        leaf_stat = path.lstat()
    except OSError:
        return False
    return bool(
        not path.is_symlink()
        and stat.S_ISREG(leaf_stat.st_mode)
        and leaf_stat.st_uid == os.geteuid()
    )


def _secure_request_log_leaf(path: Path) -> bool:
    try:
        leaf_stat = path.lstat()
    except OSError:
        return False
    return bool(
        not path.is_symlink()
        and stat.S_ISREG(leaf_stat.st_mode)
        and leaf_stat.st_uid == os.geteuid()
        and stat.S_IMODE(leaf_stat.st_mode) == 0o600
        and os.access(path, os.R_OK | os.W_OK)
    )


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
        from boxer_company.assistant.device_health_alert_action_route import (
            DeviceHealthAlertActionRouteDeps,
        )
        from boxer_company.settings import (
            validate_company_data_source_settings,
        )

        # 직접 app factory를 쓰는 실행 경로에서도 운영 credential 정책과
        # 기존 데이터 소스 설정 검증을 startup 전에 동일하게 적용한다.
        validate_company_api_runtime_security()
        _validate_tokens(
            include_llm=True,
            include_data_sources=True,
        )
        validate_company_data_source_settings()

        def claim_mark_done_with_api_storage(**kwargs: Any) -> Any:
            # startup readiness와 실제 claim이 같은 settings 객체의 exact
            # SQLite를 쓰고, startup 뒤 삭제·symlink 교체된 leaf는 기존
            # 감사 저장과 같이 fail-closed해 중복 방지 이력을 초기화하지 않는다.
            request_log_path = Path(settings.request_log_path)
            if (
                not settings.request_log_enabled
                or not _secure_request_log_leaf(request_log_path)
            ):
                raise RuntimeError(
                    "device health alert ack storage is disabled"
                )
            return claim_device_health_alert_acknowledgement(
                **kwargs,
                db_path=request_log_path,
                schema_prepared=True,
            )

        return create_company_assistant_runtime(
            device_health_alert_action_deps=(
                DeviceHealthAlertActionRouteDeps(
                    claim_mark_done=claim_mark_done_with_api_storage,
                )
            ),
        ), True
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
