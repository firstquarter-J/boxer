from __future__ import annotations

import logging
from typing import Any

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
)
from boxer_company.assistant.device_file_operations_route import (
    DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE,
    DEVICE_FILE_DOWNLOAD_ROUTE,
    DEVICE_FILE_LOOKUP_ROUTE,
    DEVICE_FILE_RECOVERY_ROUTE,
    DEVICE_LOG_UPLOAD_ROUTE,
    DeviceFileOperationsAssistantRoute,
)
from boxer_company.assistant.device_health_alert_action_route import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
    DeviceHealthAlertActionAssistantRoute,
    DeviceHealthAlertActionRouteDeps,
)
from boxer_company.assistant.device_operations_route import (
    DEVICE_OPERATION_DELIVERY_ACTION,
    DeviceOperationsAssistantRoute,
)
from boxer_company.assistant.knowledge_write_route import (
    ThreadPlaybookLearningAssistantRoute,
)
from boxer_company.assistant.private_admin_routes import (
    build_private_operations_routes,
)
from boxer_company.assistant.security_review_route import (
    SECURITY_REVIEW_ROUTE,
    SecurityReviewAssistantRoute,
)
# 실행 route 조립은 provider-free 정본이 확정한 우선순위만 소비한다.
from boxer_company.operation_routing import (
    _LEGACY_EARLY_DEVICE_OPERATION_ROUTES,
    _LEGACY_PRE_DEVICE_PRIVATE_ROUTES,
    match_company_operation_route,
)
from boxer_company.routers.device_file_probe import (
    _should_probe_device_files,
)
from boxer_company.routers.device_scanner_abi_patch import (
    DEVICE_SCANNER_ABI_PATCH_ROUTE,
)
from boxer_company.transport_contracts import company_operation_route_names


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
        DEVICE_SCANNER_ABI_PATCH_ROUTE,
        DEVICE_OPERATION_DELIVERY_ACTION,
        "device_memory_patch",
    }
)
_DEVICE_FILE_OPERATION_ROUTES = frozenset(
    {
        DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE,
        DEVICE_LOG_UPLOAD_ROUTE,
        DEVICE_FILE_LOOKUP_ROUTE,
        DEVICE_FILE_DOWNLOAD_ROUTE,
        DEVICE_FILE_RECOVERY_ROUTE,
    }
)
_DEVICE_OPERATION_ROUTES = frozenset(
    {
        "device_voice_catalog",
        "device_voice_change",
        "device_diagnostic_snapshot",
        "device_diagnostic_analysis",
        "device_diagnostic_followup",
        "device_update_status",
        "device_box_update",
        "device_agent_update",
        "device_power_off",
        DEVICE_SCANNER_ABI_PATCH_ROUTE,
        DEVICE_OPERATION_DELIVERY_ACTION,
        "device_audio_probe",
        "device_remote_access_probe",
        "device_memory_patch",
        "device_pm2_probe",
        "device_captureboard_probe",
        "device_led_probe",
        "device_status_probe",
    }
)
_LEGACY_LATE_DEVICE_OPERATION_ROUTES = (
    _DEVICE_OPERATION_ROUTES - _LEGACY_EARLY_DEVICE_OPERATION_ROUTES
)
_MUTATION_CAPABLE_OPERATION_ROUTES = _MUTATING_OPERATION_ROUTES | frozenset(
    {
        # typed action도 provider/API session state를 바꾸므로 자연어 mutation
        # route와 같은 request-id 중복 억제 경계를 적용한다.
        SECURITY_REVIEW_ROUTE,
        DEVICE_HEALTH_ALERT_SMS_ROUTE,
        DEVICE_HEALTH_ALERT_VOICE_ROUTE,
        DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
        # Slack UI POST 결과도 같은 request-id guard 아래 한 번만 JSONL에 쓴다.
        DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
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
        DEVICE_SCANNER_ABI_PATCH_ROUTE,
        DEVICE_OPERATION_DELIVERY_ACTION,
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
        "device_operation_delivery_receipt_in_progress",
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


class _ExactCompanyOperationRoute:
    """공통 matcher가 확정한 route 묶음만 실제 handler에 진입시킨다."""

    def __init__(
        self,
        *,
        name: str,
        route: Any,
        accepted_routes: frozenset[str],
    ) -> None:
        self.name = name
        self._route = route
        self._accepted_routes = accepted_routes

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        matched = match_company_operation_route(request)
        if matched not in self._accepted_routes:
            return None
        result = self._route.handle(request)
        return self._validate_result(matched, result)

    def handle_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Any,
    ) -> CompanyAssistantResult | None:
        matched = match_company_operation_route(request)
        if matched not in self._accepted_routes:
            return None
        progressive_handler = getattr(
            self._route,
            "handle_with_progress",
            None,
        )
        result = (
            progressive_handler(request, on_partial_result)
            if callable(progressive_handler)
            else self._route.handle(request)
        )
        return self._validate_result(matched, result)

    @staticmethod
    def _validate_result(
        matched: str,
        result: CompanyAssistantResult | None,
    ) -> CompanyAssistantResult:
        if result is not None and result.route == matched:
            return result
        # matcher와 executor가 다시 어긋나도 뒤 route나 mutation으로 계속
        # 내려가지 않고, 확정 route 이름만 담은 안전한 실패로 끝낸다.
        return CompanyAssistantResult(
            route=matched,
            outcome="failed",
            messages=(
                AssistantMessage(
                    body="요청 경로를 실행할 수 없어. 잠시 후 다시 시도해줘"
                ),
            ),
            fallback_reason="operation_dispatch_mismatch",
        )


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


def is_retryable_company_mutation_result(
    *,
    mutation_route: str,
    result: CompanyAssistantResult,
) -> bool:
    """영속 unique claim을 다시 읽어도 안전한 저장소 실패만 해제한다."""

    return bool(
        mutation_route == DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE
        and result.outcome == "failed"
        and str(result.fallback_reason or "").strip()
        == "device_health_alert_ack_store_failed"
    )


def build_company_operation_routes(
    *,
    context_max_chars: int,
    claude_client: Any = None,
    answer_composer: CompanyEvidenceAnswerComposer | None = None,
    timeout_message: str | None = None,
    logger: logging.Logger | None = None,
    device_health_alert_action_deps: (
        DeviceHealthAlertActionRouteDeps | None
    ) = None,
) -> tuple[Any, ...]:
    """공통 API 프로세스에서만 operation 구현을 고정 순서로 조립한다."""

    private_routes = build_private_operations_routes(
        answer_composer=answer_composer,
        timeout_message=timeout_message,
        logger=logger,
    )
    pre_device_private_routes = tuple(
        route
        for route in private_routes
        if route.name in _LEGACY_PRE_DEVICE_PRIVATE_ROUTES
    )
    post_device_private_routes = tuple(
        route
        for route in private_routes
        if route.name not in _LEGACY_PRE_DEVICE_PRIVATE_ROUTES
    )
    security_route = SecurityReviewAssistantRoute()
    alert_route = DeviceHealthAlertActionAssistantRoute(
        deps=device_health_alert_action_deps,
        logger=logger,
    )
    learning_route = ThreadPlaybookLearningAssistantRoute(
        context_max_chars=context_max_chars,
        claude_client=claude_client,
        logger=logger,
    )
    file_route = DeviceFileOperationsAssistantRoute(logger=logger)
    device_route = DeviceOperationsAssistantRoute(
        answer_composer=answer_composer,
        timeout_message=(
            timeout_message
            or "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
        ),
        logger=logger,
    )

    def exact(
        route: Any,
        accepted: frozenset[str],
        *,
        name: str | None = None,
    ) -> Any:
        return _ExactCompanyOperationRoute(
            name=name or route.name,
            route=route,
            accepted_routes=accepted,
        )

    return (
        exact(security_route, frozenset({SECURITY_REVIEW_ROUTE})),
        exact(
            alert_route,
            frozenset(
                {
                    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
                    DEVICE_HEALTH_ALERT_SMS_ROUTE,
                    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
                    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
                    DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
                }
            ),
        ),
        exact(learning_route, frozenset({"thread_playbook_learning"})),
        *(
            exact(route, frozenset({route.name}))
            for route in pre_device_private_routes
        ),
        exact(
            device_route,
            _LEGACY_EARLY_DEVICE_OPERATION_ROUTES,
            name="device_operations_early",
        ),
        exact(file_route, _DEVICE_FILE_OPERATION_ROUTES),
        exact(device_route, _LEGACY_LATE_DEVICE_OPERATION_ROUTES),
        *(
            exact(route, frozenset({route.name}))
            for route in post_device_private_routes
        ),
    )


__all__ = [
    "build_company_operation_routes",
    "company_operation_route_names",
    "is_retryable_company_mutation_result",
    "is_uncertain_company_mutation_result",
    "match_live_device_company_operation_route",
    "match_mutation_capable_company_operation_route",
]
