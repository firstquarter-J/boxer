from __future__ import annotations

from dataclasses import replace
import logging
from typing import Any, Literal

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
    match_device_file_operation_route,
    needs_device_file_operation_context,
)
from boxer_company.assistant.device_health_alert_action_route import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
    DeviceHealthAlertActionAssistantRoute,
    match_device_health_alert_action_route,
)
from boxer_company.assistant.device_led_routes import (
    match_device_read_route,
)
from boxer_company.assistant.device_operations_route import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_OPERATION_DELIVERY_ACTION,
    DeviceOperationsAssistantRoute,
    has_device_diagnostic_followup_query,
    match_device_operation_route,
)
from boxer_company.assistant.knowledge_write_route import (
    ThreadPlaybookLearningAssistantRoute,
    match_thread_playbook_learning_route,
)
from boxer_company.assistant.knowledge_routes import (
    match_notion_playbook_route,
)
from boxer_company.assistant.private_admin_routes import (
    ADMIN_READONLY_SQL_ROUTE,
    ADMIN_REQUEST_LOG_ROUTE,
    ADMIN_S3_DEVICE_LOG_ROUTE,
    ADMIN_S3_ULTRASOUND_ROUTE,
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
from boxer_company.routers.recording_streaming_restore import (
    _extract_recording_streaming_restore_month,
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
        DEVICE_OPERATION_DELIVERY_ACTION,
        "device_memory_patch",
    }
)
_LEGACY_PRE_DEVICE_PRIVATE_ROUTES = frozenset(
    {
        ADMIN_S3_ULTRASOUND_ROUTE,
        ADMIN_S3_DEVICE_LOG_ROUTE,
        ADMIN_READONLY_SQL_ROUTE,
        ADMIN_REQUEST_LOG_ROUTE,
    }
)
_LEGACY_EARLY_DEVICE_OPERATION_ROUTES = frozenset(
    {
        "device_voice_catalog",
        "device_voice_change",
        "device_diagnostic_snapshot",
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
    device_action_route = match_device_operation_route(scoped)
    if device_action_route == DEVICE_OPERATION_DELIVERY_ACTION:
        # Slack 최종 메시지 성공 뒤 온 typed receipt는 질문 속 학습/admin/file
        # 표현보다 먼저 잡아 원 장비 명령을 다시 실행하지 않는다.
        return device_action_route
    raw_operation_action = scoped.metadata.get("operation_action")
    if (
        isinstance(raw_operation_action, dict)
        and raw_operation_action
        == {"name": DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION}
    ):
        # knowledge 위치에서 adapter가 보낸 typed snapshot probe는 질문에
        # 섞인 다른 자연어 matcher보다 정확한 route를 먼저 확정한다.
        return "device_diagnostic_followup"
    learning_route = match_thread_playbook_learning_route(scoped)
    if learning_route is not None:
        return learning_route

    private_route = match_private_operations_route(scoped)
    if private_route in _LEGACY_PRE_DEVICE_PRIVATE_ROUTES:
        # 기존 Slack의 admin route는 Notion·장비 route보다 먼저 실행됐다.
        return private_route
    if private_route == "recording_streaming_restore":
        try:
            _extract_recording_streaming_restore_month(scoped.question)
        except ValueError:
            pass
        else:
            # 기존 device handler는 유효한 월 단위 streaming 복원 문구를 첫
            # guard에서 바로 barcode handler로 넘겼다. 음성·업데이트·전원 표현이
            # 함께 있어도 장비 mutation이 복원을 선점하지 않게 이 위치를 지킨다.
            return private_route

    file_route = match_device_file_operation_route(scoped)
    if file_route == DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE:
        # 바코드 없는 다운로드 안내는 기존 device handler의 첫 guard였다.
        return file_route

    device_route = match_device_operation_route(scoped)
    if device_route in _LEGACY_EARLY_DEVICE_OPERATION_ROUTES:
        # 음성 변경과 진단 시작은 기존 device handler에서 LED/file보다 앞이다.
        return device_route
    if match_device_read_route(scoped) is not None:
        # LED S3 분석·패턴 안내는 operation이 아니며 기존에는 file보다 먼저
        # 처리됐다. operation gateway가 이 조회를 선점하지 않게 넘긴다.
        return None
    if file_route is not None:
        return file_route
    if device_route in {
        "device_diagnostic_followup",
        "device_diagnostic_analysis",
    }:
        # app-user·barcode 판정·streaming restore는 모두 knowledge 진단보다
        # 앞이었다. 이 요청은 API barcode stage가 먼저 소유한다.
        if private_route is not None:
            return private_route
        if (
            device_route == "device_diagnostic_analysis"
            and match_notion_playbook_route(scoped) is not None
        ):
            # Notion playbook도 freeform live 진단보다 먼저 답했다.
            return None
        return device_route
    if device_route is not None:
        return device_route
    return private_route


CompanyOperationLegacyStage = Literal[
    "pre_notion",
    "device",
    "barcode",
    "knowledge",
]


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


def company_operation_legacy_stage(
    request: CompanyAssistantRequest,
) -> CompanyOperationLegacyStage | None:
    """기존 Slack handler 순서에서 operation이 실행되던 위치를 반환한다."""

    scoped = as_operations_request(request)
    matched = match_company_operation_route(scoped)
    if matched is None:
        return None
    if matched in (
        {
            SECURITY_REVIEW_ROUTE,
            "thread_playbook_learning",
        }
        | set(_LEGACY_PRE_DEVICE_PRIVATE_ROUTES)
        | {
            DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
            DEVICE_HEALTH_ALERT_SMS_ROUTE,
            DEVICE_HEALTH_ALERT_VOICE_ROUTE,
            DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
            DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
        }
    ):
        # 학습·보안·admin은 기존 Notion matcher보다 앞이었다.
        return "pre_notion"
    if matched in {
        "device_diagnostic_followup",
        "device_diagnostic_analysis",
    }:
        # 저장 snapshot 후속과 자유 진단은 기존 모든 barcode route 뒤의
        # knowledge 위치에서만 실행됐다.
        return "knowledge"
    if (
        match_device_file_operation_route(scoped) == matched
        or match_device_operation_route(scoped) == matched
    ):
        # 장비와 일자 파일 작업은 Notion 다음, 분석 route들보다 앞이었다.
        return "device"
    # app-user·streaming restore·barcode 판정은 기존 barcode handler 위치다.
    return "barcode"


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


def build_company_operation_routes(
    *,
    context_max_chars: int,
    claude_client: Any = None,
    answer_composer: CompanyEvidenceAnswerComposer | None = None,
    timeout_message: str | None = None,
    logger: logging.Logger | None = None,
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
    alert_route = DeviceHealthAlertActionAssistantRoute(logger=logger)
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


def company_operation_route_names() -> frozenset[str]:
    """rollout 응답 검증에 쓰는 고정 allowlist를 구현과 함께 관리한다."""

    # route 인스턴스를 만들면 S3/MDA/LLM 호출은 발생하지 않지만, 이름만을
    # 얻기 위해 설정 의존 객체를 조립할 필요도 없도록 상수 집합으로 둔다.
    return frozenset(
        {
            SECURITY_REVIEW_ROUTE,
            DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
            DEVICE_HEALTH_ALERT_SMS_ROUTE,
            DEVICE_HEALTH_ALERT_VOICE_ROUTE,
            DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
            DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
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
            DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE,
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


__all__ = [
    "as_operations_request",
    "build_company_operation_routes",
    "company_operation_legacy_stage",
    "company_operation_route_names",
    "has_device_diagnostic_followup_query",
    "is_mutation_capable_company_operation",
    "is_uncertain_company_mutation_result",
    "match_company_operation_route",
    "match_live_device_company_operation_route",
    "match_mutation_capable_company_operation_route",
    "needs_device_file_operation_context",
]
