from __future__ import annotations

from dataclasses import replace
import logging
from typing import Any

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.device_file_operations_route import (
    DEVICE_FILE_DOWNLOAD_ROUTE,
    DEVICE_FILE_LOOKUP_ROUTE,
    DEVICE_FILE_RECOVERY_ROUTE,
    DEVICE_LOG_UPLOAD_ROUTE,
    DeviceFileOperationsAssistantRoute,
    has_ambiguous_device_file_operation_scope,
    match_device_file_operation_route,
    needs_device_file_operation_context,
)
from boxer_company.assistant.device_health_alert_action_route import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
    DeviceHealthAlertActionAssistantRoute,
    match_device_health_alert_action_route,
)
from boxer_company.assistant.device_operations_route import (
    DeviceOperationsAssistantRoute,
    has_ambiguous_device_mutation_target,
    has_device_diagnostic_followup_query,
    match_device_mutation_guard_candidate_route,
    match_device_operation_route,
)
from boxer_company.assistant.knowledge_write_route import (
    ThreadPlaybookLearningAssistantRoute,
    match_thread_playbook_learning_candidate_route,
    match_thread_playbook_learning_route,
)
from boxer_company.assistant.operation_intent import (
    is_explicit_operation_execution,
)
from boxer_company.assistant.private_admin_routes import (
    build_private_operations_routes,
    match_private_operations_route,
)
from boxer_company.assistant.security_review_route import (
    SECURITY_REVIEW_ROUTE,
    SecurityReviewAssistantRoute,
    match_security_review_route,
)
from boxer_company.routers.device_file_probe import (
    _should_probe_device_files,
)


OPERATION_CONFIRMATION_REQUIRED_ROUTE = (
    "operation_confirmation_required"
)
OPERATION_SINGLE_TARGET_REQUIRED_ROUTE = (
    "operation_single_target_required"
)
_MUTATING_OPERATION_ROUTES = frozenset(
    {
        "thread_playbook_learning",
        "recording_streaming_restore",
        DEVICE_LOG_UPLOAD_ROUTE,
        DEVICE_FILE_LOOKUP_ROUTE,
        DEVICE_FILE_DOWNLOAD_ROUTE,
        DEVICE_FILE_RECOVERY_ROUTE,
        "device_voice_change",
        "device_diagnostic_snapshot",
        "device_diagnostic_analysis",
        "device_box_update",
        "device_agent_update",
        "device_power_off",
        "device_memory_patch",
    }
)
_MUTATION_CAPABLE_OPERATION_ROUTES = _MUTATING_OPERATION_ROUTES | frozenset(
    {
        # typed action도 provider/API session state를 바꾸므로 자연어 mutation
        # route와 같은 request-id 중복 억제 경계를 적용한다.
        SECURITY_REVIEW_ROUTE,
        DEVICE_HEALTH_ALERT_SMS_ROUTE,
        DEVICE_HEALTH_ALERT_VOICE_ROUTE,
        # 조회 응답 자체는 read-only여도 endpoint가 없으면 sshOrder(open)으로
        # tunnel lifecycle을 바꿀 수 있어 같은 mutation guard가 필요하다.
        "device_diagnostic_followup",
        "device_update_status",
        "device_audio_probe",
        "device_remote_access_probe",
        "device_pm2_probe",
        "device_captureboard_probe",
        "device_led_probe",
        "device_status_probe",
    }
)
# API EC2의 MDA/SSH·장비 파일 경계를 실제로 사용하는 route 정본이다.
# app-user/admin/Notion/security-review/SMS처럼 장비 접속과 무관한 operation은
# live-device feature가 꺼져 있어도 독립적으로 이전할 수 있게 제외한다.
_LIVE_DEVICE_OPERATION_ROUTES = frozenset(
    {
        "recording_streaming_restore",
        DEVICE_LOG_UPLOAD_ROUTE,
        DEVICE_FILE_DOWNLOAD_ROUTE,
        DEVICE_FILE_RECOVERY_ROUTE,
        DEVICE_HEALTH_ALERT_VOICE_ROUTE,
        "device_voice_change",
        "device_diagnostic_snapshot",
        "device_diagnostic_analysis",
        "device_diagnostic_followup",
        "device_update_status",
        "device_box_update",
        "device_agent_update",
        "device_power_off",
        "device_audio_probe",
        "device_remote_access_probe",
        "device_memory_patch",
        "device_pm2_probe",
        "device_captureboard_probe",
        "device_led_probe",
        "device_status_probe",
    }
)
_UNCERTAIN_MUTATION_FALLBACK_REASONS = frozenset(
    {
        "sms_delivery_receipt_persist_failed",
        "sms_delivery_confirmation_required",
        "voice_guide_dispatch_uncertain",
    }
)
_ATTEMPT_REQUIRED_UNCERTAIN_FALLBACK_REASONS = frozenset(
    {"operation_error", "knowledge_write_failed"}
)
_UNCERTAIN_MUTATION_FALLBACK_REASONS_BY_ROUTE = {
    # 이 route는 공용 fallback 이름을 쓰지만, guard admission 뒤의
    # dependency 실패는 mutation 전후를 구분할 수 없어 재실행하면 안 된다.
    "recording_streaming_restore": frozenset(
        {"dependency_error", "empty_result"}
    ),
}


class OperationConfirmationAssistantRoute:
    """질문·설명·부정형 mutation 후보를 실행 없이 결정적으로 종결한다."""

    name = OPERATION_CONFIRMATION_REQUIRED_ROUTE

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_operation_execution_guard_route(request) is None:
            return None
        return build_operation_confirmation_required_result()


class OperationSingleTargetAssistantRoute:
    """복수·교차 actor target 요청을 외부 호출 없이 종결한다."""

    name = OPERATION_SINGLE_TARGET_REQUIRED_ROUTE

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_operation_target_guard_route(request) is None:
            return None
        return build_operation_single_target_required_result()


def build_operation_confirmation_required_result() -> CompanyAssistantResult:
    """local/remote gateway가 공유하는 mutation 미실행 안내다."""

    return CompanyAssistantResult(
        route=OPERATION_CONFIRMATION_REQUIRED_ROUTE,
        outcome="needs_input",
        messages=(
            AssistantMessage(
                body=(
                    "방법·가능 여부 질문으로는 작업을 실행하지 않아. "
                    "실제 실행 요청이면 질문형 없이 작업을 명시해서 다시 요청해줘"
                )
            ),
        ),
        fallback_reason="explicit_execution_required",
    )


def build_operation_single_target_required_result() -> CompanyAssistantResult:
    """장비·바코드·같은 actor thread scope를 하나로 다시 받는다."""

    return CompanyAssistantResult(
        route=OPERATION_SINGLE_TARGET_REQUIRED_ROUTE,
        outcome="needs_input",
        messages=(
            AssistantMessage(
                body=(
                    "작업 대상을 하나로 확정할 수 없어 실행하지 않았어. "
                    "장비명 또는 바코드 하나를 현재 요청에 직접 적어서 다시 요청해줘"
                )
            ),
        ),
        fallback_reason="single_operation_target_required",
    )


def as_operations_request(
    request: CompanyAssistantRequest,
) -> CompanyAssistantRequest:
    """Slack의 원 요청을 실행 없이 operations 분류 문맥으로 좁힌다."""

    if str(request.metadata.get("route_group") or "").strip() == "operations":
        return request
    metadata: dict[str, Any] = dict(request.metadata)
    metadata["route_group"] = "operations"
    return replace(request, metadata=metadata)


def match_company_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """Slack/API가 공유하는 operation 전체 우선순위를 순수하게 판정한다."""

    scoped = as_operations_request(request)
    # Slack 보안검토 transport가 만든 typed action은 자연어 mutation
    # matcher보다 먼저 확정해 probe 본문을 명령으로 오인하지 않게 한다.
    security_review = match_security_review_route(scoped)
    if security_review is not None:
        return security_review
    # Slack action은 이미 typed target과 명시적 action name을 담으므로
    # 자연어 mutation guard보다 먼저 질문 비의존 matcher로 확정한다.
    alert_action = match_device_health_alert_action_route(scoped)
    if alert_action is not None:
        return alert_action
    target_guarded = match_operation_target_guard_route(scoped)
    if target_guarded is not None:
        return target_guarded
    guarded = match_operation_execution_guard_route(scoped)
    if guarded is not None:
        return guarded
    for matcher in (
        match_thread_playbook_learning_route,
        # 일자 단위 파일 복구를 먼저 분류하고, 월 단위 recordings 복원은
        # file matcher가 넘긴 뒤 private operation이 소유한다.
        match_device_file_operation_route,
        match_private_operations_route,
        match_device_operation_route,
    ):
        matched = matcher(scoped)
        if matched is not None:
            return matched
    return None


def is_mutation_capable_company_operation(
    request: CompanyAssistantRequest,
) -> bool:
    """공통 matcher가 확정한 실제 side-effect route만 guard 대상으로 고른다."""

    return match_mutation_capable_company_operation_route(request) is not None


def match_mutation_capable_company_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """공통 operation 우선순위를 보존해 guard 대상 route를 한 번만 확정한다."""

    matched = match_company_operation_route(request)
    return matched if matched in _MUTATION_CAPABLE_OPERATION_ROUTES else None


def match_live_device_company_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """MDA/SSH·장비 파일 설정을 요구하는 operation만 식별한다."""

    matched = match_company_operation_route(request)
    if matched == DEVICE_FILE_LOOKUP_ROUTE:
        # fileId 기반 DB/S3 확인은 live 장비 접속이 없고, 실제 장비 파일을
        # 보겠다는 질문만 MDA/SSH preflight를 수행한다.
        return matched if _should_probe_device_files(request.question) else None
    return matched if matched in _LIVE_DEVICE_OPERATION_ROUTES else None


def is_uncertain_company_mutation_result(
    *,
    mutation_route: str,
    result: CompanyAssistantResult,
    side_effect_attempted: bool = False,
) -> bool:
    """정상 반환됐지만 side effect 완료 여부가 불명인 결과만 식별한다."""

    if result.outcome != "failed":
        return False
    fallback_reason = str(result.fallback_reason or "").strip()
    if fallback_reason in _UNCERTAIN_MUTATION_FALLBACK_REASONS:
        return True
    if fallback_reason in _ATTEMPT_REQUIRED_UNCERTAIN_FALLBACK_REASONS:
        return side_effect_attempted
    if mutation_route == "device_detail":
        # device_detail은 DB/MDA precheck와 sshOrder 이후 오류가 같은 fallback을
        # 공유하므로, request-scoped open 시도가 관찰된 경우에만 sticky다.
        return side_effect_attempted and fallback_reason in {
            "dependency_error",
            "query_error",
        }
    return side_effect_attempted and fallback_reason in (
        _UNCERTAIN_MUTATION_FALLBACK_REASONS_BY_ROUTE.get(
            mutation_route,
            frozenset(),
        )
    )


def match_operation_target_guard_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """복수 target과 actor-safe thread scope 실패를 mode와 무관하게 막는다."""

    scoped = as_operations_request(request)
    if has_ambiguous_device_mutation_target(
        scoped
    ) or has_ambiguous_device_file_operation_scope(scoped):
        return OPERATION_SINGLE_TARGET_REQUIRED_ROUTE
    return None


def match_operation_execution_guard_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """mutation 후보의 비실행 의도를 외부 호출 없이 먼저 분류한다."""

    scoped = as_operations_request(request)
    for matcher in (
        match_thread_playbook_learning_candidate_route,
        match_device_file_operation_route,
        match_private_operations_route,
        match_device_mutation_guard_candidate_route,
    ):
        candidate = matcher(scoped)
        if candidate not in _MUTATING_OPERATION_ROUTES:
            continue
        if not is_explicit_operation_execution(scoped.question):
            return OPERATION_CONFIRMATION_REQUIRED_ROUTE
        return None
    return None


def build_company_operation_routes(
    *,
    context_max_chars: int,
    logger: logging.Logger | None = None,
) -> tuple[Any, ...]:
    """공통 API 프로세스에서만 operation 구현을 고정 순서로 조립한다."""

    return (
        OperationSingleTargetAssistantRoute(),
        OperationConfirmationAssistantRoute(),
        SecurityReviewAssistantRoute(),
        DeviceHealthAlertActionAssistantRoute(logger=logger),
        DeviceFileOperationsAssistantRoute(logger=logger),
        *build_private_operations_routes(logger=logger),
        DeviceOperationsAssistantRoute(logger=logger),
        ThreadPlaybookLearningAssistantRoute(
            context_max_chars=context_max_chars,
            logger=logger,
        ),
    )


def company_operation_route_names() -> frozenset[str]:
    """rollout 응답 검증에 쓰는 고정 allowlist를 구현과 함께 관리한다."""

    # route 인스턴스를 만들면 S3/MDA/LLM 호출은 발생하지 않지만, 이름만을
    # 얻기 위해 설정 의존 객체를 조립할 필요도 없도록 상수 집합으로 둔다.
    return frozenset(
        {
            OPERATION_CONFIRMATION_REQUIRED_ROUTE,
            OPERATION_SINGLE_TARGET_REQUIRED_ROUTE,
            SECURITY_REVIEW_ROUTE,
            DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
            DEVICE_HEALTH_ALERT_SMS_ROUTE,
            DEVICE_HEALTH_ALERT_VOICE_ROUTE,
            DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
            "thread_playbook_learning",
            "app_user_baby_selection_analysis",
            "app_user_lookup",
            "recording_streaming_restore",
            "barcode_pink_classification_reason",
            "barcode_validation_status",
            "admin_s3_ultrasound",
            "admin_s3_device_log",
            "admin_request_log",
            "admin_readonly_sql",
            DEVICE_LOG_UPLOAD_ROUTE,
            DEVICE_FILE_LOOKUP_ROUTE,
            DEVICE_FILE_DOWNLOAD_ROUTE,
            DEVICE_FILE_RECOVERY_ROUTE,
            "device_voice_catalog",
            "device_voice_change",
            "device_diagnostic_snapshot",
            "device_diagnostic_analysis",
            "device_diagnostic_followup",
            "device_update_status",
            "device_box_update",
            "device_agent_update",
            "device_power_off",
            "device_audio_probe",
            "device_remote_access_probe",
            "device_memory_patch",
            "device_pm2_probe",
            "device_captureboard_probe",
            "device_led_probe",
            "device_status_probe",
        }
    )


__all__ = [
    "OPERATION_CONFIRMATION_REQUIRED_ROUTE",
    "OPERATION_SINGLE_TARGET_REQUIRED_ROUTE",
    "OperationConfirmationAssistantRoute",
    "OperationSingleTargetAssistantRoute",
    "as_operations_request",
    "build_operation_confirmation_required_result",
    "build_operation_single_target_required_result",
    "build_company_operation_routes",
    "company_operation_route_names",
    "has_device_diagnostic_followup_query",
    "is_mutation_capable_company_operation",
    "is_uncertain_company_mutation_result",
    "match_company_operation_route",
    "match_live_device_company_operation_route",
    "match_mutation_capable_company_operation_route",
    "match_operation_execution_guard_route",
    "match_operation_target_guard_route",
    "needs_device_file_operation_context",
]
