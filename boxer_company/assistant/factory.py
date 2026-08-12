from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

from boxer import AnswerEngine
from boxer.core import settings as core_settings
from boxer.core.llm import (
    _build_claude_client,
    _check_ollama_health,
)
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer_company.assistant.barcode_log_route import (
    match_barcode_log_route,
)
from boxer_company.assistant.barcode_query_route import (
    match_barcode_query_route,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.device_led_routes import (
    match_device_read_route,
)
from boxer_company.assistant.device_db_detail_route import (
    DeviceDbDetailAssistantRoute,
)
from boxer_company.assistant.freeform_prompt import (
    build_company_freeform_system_prompt,
)
from boxer_company.assistant.knowledge_routes import (
    CompanyReadOnlyKnowledgeRouteDeps,
    build_company_read_only_knowledge_routes,
    match_barcode_evidence_freeform_route,
)
from boxer_company.assistant.notion_route import (
    CompanyNotionAssistantRouteDeps,
)
from boxer_company.assistant.operational_read_routes import (
    WeeklyRecordingsSummaryAssistantRoute,
)
from boxer_company.assistant.recording_failure_route import (
    match_recording_failure_route,
)
from boxer_company.assistant.runtime import (
    CompanyAssistantRuntime,
    CompanyAssistantRuntimeDeps,
)
from boxer_company.notion_playbooks import _select_notion_references
from boxer_company.routers.box_db import (
    _load_recordings_context_by_barcode,
)
from boxer_company.routers.device_diagnostics import (
    _extract_device_name_for_diagnostic_freeform,
    _has_device_diagnostic_start_hint,
    _is_device_diagnostic_freeform_request,
    _select_device_diagnostic_followup_command_keys,
)


DiagnosticSnapshotLoader = Callable[
    [CompanyAssistantRequest],
    dict[str, Any] | None,
]

_RESOURCE_UNSET = object()


def _unavailable_diagnostic_snapshot(
    request: CompanyAssistantRequest,
) -> dict[str, Any] | None:
    del request
    # 프로세스 메모리 snapshot은 adapter 프로세스 경계를 넘지 않는다.
    return None


def _guard_read_only_request(
    request: CompanyAssistantRequest,
) -> CompanyAssistantResult | None:
    barcode_route = match_barcode_query_route(request)
    route_group = str(
        request.metadata.get("route_group") or ""
    ).strip()
    if (
        route_group in {"", "barcode"}
        and barcode_route in {
            "barcode_pink_classification_reason",
            "barcode_validation_status",
        }
    ):
        # 두 route는 이름과 달리 MDA 조회를 포함한다. direct API나 잘못된
        # stage 선택에서도 외부 MDA에 접근하지 않도록 route 실행 전에 닫는다.
        return CompanyAssistantResult(
            route="unsupported_mda_lookup",
            outcome="denied",
            messages=(
                AssistantMessage(
                    body=(
                        "MDA 기반 바코드 판정은 읽기 전용 API에서 "
                        "지원하지 않아"
                    )
                ),
            ),
            fallback_reason="read_only_boundary",
        )

    has_live_start = _has_device_diagnostic_start_hint(
        request.question
    )
    # 장비명이 포함된 DB/S3 조회도 진단 키워드와 겹칠 수 있으므로,
    # 명시적인 진단 시작이 아닌 read-only 경로만 먼저 보존한다.
    if not has_live_start and any(
        matcher(request) is not None
        for matcher in (
            match_device_read_route,
            match_recording_failure_route,
            match_barcode_log_route,
        )
    ):
        return None

    device_name = _extract_device_name_for_diagnostic_freeform(
        request.question
    )
    is_live_followup = _is_device_diagnostic_freeform_request(
        request.question,
        device_name=device_name,
    )
    if (
        not has_live_start
        and not is_live_followup
    ):
        return None

    # 진단 시작·실시간 후속 진단은 SSH 연결이나 원격 명령으로 이어질 수
    # 있으므로 read-only 공통 API에서는 어떤 route보다 먼저 차단한다.
    return CompanyAssistantResult(
        route="unsupported_live_diagnostic",
        outcome="denied",
        messages=(
            AssistantMessage(
                body=(
                    "실시간 장비 진단은 읽기 전용 API에서 "
                    "지원하지 않아"
                )
            ),
        ),
        fallback_reason="read_only_boundary",
    )


def _answer_timeout_message(provider: str) -> str:
    if provider == "claude":
        timeout_sec = max(1, core_settings.ANTHROPIC_TIMEOUT_SEC)
        return (
            f"AI API가 {timeout_sec}초 내 응답하지 않아 "
            "AI 답변 생성이 타임아웃됐어"
        )
    timeout_sec = max(1, core_settings.OLLAMA_TIMEOUT_SEC)
    return (
        f"LLM 서버가 {timeout_sec}초 내 응답하지 않아 "
        "AI 답변 생성이 타임아웃됐어"
    )


def _create_lazy_s3_provider() -> Callable[[], Any]:
    resource: Any = _RESOURCE_UNSET
    lock = threading.Lock()

    def get_s3_client() -> Any:
        nonlocal resource
        if resource is not _RESOURCE_UNSET:
            return resource
        with lock:
            if resource is _RESOURCE_UNSET:
                # client 생성은 요청마다 반복하지 않되 실제 S3 조회는
                # matcher와 설정 가드를 통과한 route에서만 실행한다.
                resource = _build_s3_client()
        return resource

    return get_s3_client


def _create_provider_ready(
    *,
    provider: str,
    claude_client: Any | None,
) -> Callable[[], bool]:
    ollama_health_cache: tuple[
        float,
        dict[str, Any],
    ] | None = None
    lock = threading.Lock()

    def provider_ready() -> bool:
        nonlocal ollama_health_cache
        if provider == "claude":
            return claude_client is not None
        if provider != "ollama":
            return False

        now = time.monotonic()
        with lock:
            if ollama_health_cache is not None:
                cached_at, cached_health = ollama_health_cache
                ttl_sec = 30.0 if cached_health.get("ok") else 2.0
                if now - cached_at < ttl_sec:
                    return bool(cached_health.get("ok"))

            # FastAPI worker 안에서 동시 요청이 와도 health timeout을
            # 한 번만 기다리도록 짧은 TTL 결과를 공유한다.
            health = _check_ollama_health()
            ollama_health_cache = (now, health)
            return bool(health.get("ok"))

    return provider_ready


def create_company_assistant_runtime(
    *,
    diagnostic_snapshot_loader: DiagnosticSnapshotLoader | None = None,
    logger: logging.Logger | None = None,
) -> CompanyAssistantRuntime:
    """Slack/Web adapter 없이 회사 read-only runtime 전체를 조립한다."""
    app_logger = logger or logging.getLogger(__name__)
    provider = str(core_settings.LLM_PROVIDER or "").strip().lower()

    claude_client: Any | None = None
    if provider == "claude":
        try:
            claude_client = _build_claude_client(
                timeout_sec=core_settings.ANTHROPIC_TIMEOUT_SEC
            )
        except Exception as exc:
            # credential 원문이 예외 문자열에 섞일 수 있어 타입만 기록한다.
            app_logger.warning(
                "Company assistant Claude client initialization failed "
                "error_type=%s",
                type(exc).__name__,
            )

    provider_ready = _create_provider_ready(
        provider=provider,
        claude_client=claude_client,
    )
    get_s3_client = _create_lazy_s3_provider()
    timeout_message = _answer_timeout_message(provider)
    answer_engine = AnswerEngine(
        provider=provider,
        provider_client=claude_client,
        logger=app_logger,
    )

    # 별도 API 프로세스는 Slack 프로세스의 메모리 snapshot을 공유하지
    # 못하므로 명시적인 repository가 주입되기 전에는 unavailable이다.
    load_snapshot = (
        diagnostic_snapshot_loader
        if diagnostic_snapshot_loader is not None
        else _unavailable_diagnostic_snapshot
    )

    def should_handle_barcode_evidence(
        request: CompanyAssistantRequest,
    ) -> bool:
        # Slack rollout의 pure matcher를 API에서도 다시 적용한다. 신뢰된
        # service caller가 routeGroup=knowledge를 직접 보내더라도 일반 대화,
        # PII, mutation, 기존 전용 route를 마지막 LLM route가 흡수하지 않는다.
        if (
            match_barcode_evidence_freeform_route(request)
            != "barcode_evidence_freeform"
        ):
            return False
        # live 진단은 SSH open을 유발할 수 있어 read-only HTTP turn이
        # 자유질문으로 흡수하지 않고 후속 action 경계로 넘긴다.
        if _select_device_diagnostic_followup_command_keys(
            request.question
        ):
            return False
        device_name = _extract_device_name_for_diagnostic_freeform(
            request.question
        )
        if _is_device_diagnostic_freeform_request(
            request.question,
            device_name=device_name,
        ):
            return False
        if (
            not core_settings.LLM_SYNTHESIS_ENABLED
            or provider not in {"claude", "ollama"}
        ):
            return False
        return provider_ready()

    def build_knowledge_routes(
        recordings: Any,
        composer: Any,
    ) -> tuple[Any, ...]:
        return build_company_read_only_knowledge_routes(
            recordings,
            composer,
            CompanyReadOnlyKnowledgeRouteDeps(
                load_diagnostic_snapshot=load_snapshot,
                db_configured=lambda: bool(
                    core_settings.DB_HOST
                    and core_settings.DB_USERNAME
                    and core_settings.DB_PASSWORD
                    and core_settings.DB_DATABASE
                ),
                barcode_should_handle=(
                    should_handle_barcode_evidence
                ),
                build_barcode_system_prompt=(
                    lambda request, context_text: (
                        build_company_freeform_system_prompt(
                            request.question,
                            context_text,
                        )
                    )
                ),
                timeout_message=timeout_message,
                include_barcode_evidence=provider
                in {"claude", "ollama"},
            ),
            logger=app_logger,
        )

    return CompanyAssistantRuntime(
        CompanyAssistantRuntimeDeps(
            answer_engine=answer_engine,
            synthesis_enabled=core_settings.LLM_SYNTHESIS_ENABLED,
            provider_ready=provider_ready,
            get_s3_client=get_s3_client,
            recordings_loader=_load_recordings_context_by_barcode,
            notion_reference_loader=_select_notion_references,
            s3_query_enabled=lambda: core_settings.S3_QUERY_ENABLED,
            db_configured=lambda: bool(
                core_settings.DB_HOST
                and core_settings.DB_USERNAME
                and core_settings.DB_PASSWORD
                and core_settings.DB_DATABASE
            ),
            timeout_message=timeout_message,
            notion_route_deps=CompanyNotionAssistantRouteDeps(
                answer_engine=answer_engine,
                synthesis_enabled=(
                    core_settings.LLM_SYNTHESIS_ENABLED
                ),
                provider_ready=provider_ready,
            ),
            request_guard=_guard_read_only_request,
            # API 프로세스에서만 안전한 DB-only 세부 route를 범용
            # structured matcher 앞에 둔다. Slack runtime의 기본 graph에는
            # 주입하지 않아 기존 local/enriched 동작을 보존한다.
            structured_read_routes=(
                WeeklyRecordingsSummaryAssistantRoute(
                    logger=app_logger,
                ),
                DeviceDbDetailAssistantRoute(
                    logger=app_logger,
                ),
            ),
            # 장비 기본 정보는 DB-only로 제공하고 MDA/SSH 보강과
            # sshOrder mutation은 Slack의 live 진단 경계에 남긴다.
            structured_device_filter_enabled=True,
            structured_device_live_enrichment_enabled=False,
            # 로그 분석은 S3/DB까지만 허용하고 MDA sshOrder와 실제
            # 장비 SSH lifecycle 보강은 Slack local runtime에 남긴다.
            log_analysis_live_enrichment_enabled=False,
            # 초기 API 전환은 최대 30일 자동 탐색 없이 날짜 지정
            # 요청만 허용해 S3 호출량과 timeout 범위를 제한한다.
            log_analysis_explicit_date_required=True,
        ),
        knowledge_route_factory=build_knowledge_routes,
        logger=app_logger,
    )


__all__ = [
    "DiagnosticSnapshotLoader",
    "create_company_assistant_runtime",
]
