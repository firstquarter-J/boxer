"""회사 operation의 provider-free 전체 우선순위 정본이다."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from typing import Any, Literal

from boxer_company._operation_routing_common import CompanyOperationRequestContract
from boxer_company._operation_routing_device import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_OPERATION_DELIVERY_ACTION,
    _extract_device_name_for_audio_probe,
    _extract_device_name_for_diagnostic_freeform,
    _extract_device_name_for_diagnostic_start,
    _extract_device_name_for_remote_access_probe,
    _extract_device_name_for_status_probe,
    _extract_device_name_for_update,
    _has_device_diagnostic_start_hint,
    _is_device_agent_update_request,
    _is_device_audio_probe_request,
    _is_device_box_update_request,
    _is_device_captureboard_probe_request,
    _is_device_diagnostic_freeform_request,
    _is_device_diagnostic_start_request,
    _is_device_led_log_analysis_request,
    _is_device_led_pattern_help_request,
    _is_device_led_probe_request,
    _is_device_memory_patch_request,
    _is_device_pm2_probe_request,
    _is_device_power_off_request,
    _is_device_remote_access_probe_request,
    _is_device_status_probe_request,
    _is_device_update_status_request,
    _is_device_voice_catalog_request,
    _is_device_voice_change_request,
    _select_device_diagnostic_followup_command_keys,
    match_device_operation_route,
    match_device_read_route,
)
from boxer_company._operation_routing_file import (
    DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE,
    _extract_hospital_room_scope_for_log_upload,
    _extract_recording_streaming_restore_month,
    _is_barcode_device_file_probe_request,
    _is_device_log_upload_check_request,
    _is_recording_streaming_restore_request,
    _should_download_device_files,
    _should_recover_device_files,
    match_device_file_operation_route,
)
from boxer_company._operation_routing_knowledge import (
    _is_thread_playbook_learning_request,
    match_notion_playbook_route,
    match_thread_playbook_learning_route,
)
from boxer_company._operation_routing_private import (
    ADMIN_READONLY_SQL_ROUTE,
    ADMIN_REQUEST_LOG_ROUTE,
    ADMIN_S3_DEVICE_LOG_ROUTE,
    ADMIN_S3_ULTRASOUND_ROUTE,
    APP_USER_BABY_ANALYSIS_ROUTE,
    APP_USER_PROFILE_ROUTE,
    BARCODE_PINK_CLASSIFICATION_ROUTE,
    BARCODE_VALIDATION_STATUS_ROUTE,
    RECORDING_STREAMING_RESTORE_ROUTE,
    RequestLogQuerySpec,
    _extract_db_query,
    _extract_request_log_query,
    _extract_s3_request,
    _is_barcode_pink_classification_reason_request,
    _is_barcode_validation_status_request,
    _should_analyze_app_user_baby_selection,
    _should_lookup_barcode,
    match_private_operations_route,
)
from boxer_company.transport_contracts import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ACTION,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ACTION,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
    DEVICE_SCANNER_ABI_PATCH_ROUTE,
)


SECURITY_REVIEW_ACTION = "security_review"


SECURITY_REVIEW_ROUTE = "security_review"


def match_security_review_route(
    request: CompanyOperationRequestContract,
) -> str | None:
    if str(request.metadata.get("route_group") or "").strip() != "operations":
        return None
    action = request.metadata.get("operation_action")
    if not isinstance(action, Mapping):
        return None
    if str(action.get("name") or "").strip() != SECURITY_REVIEW_ACTION:
        return None
    return SECURITY_REVIEW_ROUTE


_DEVICE_HEALTH_ALERT_ACTION_ROUTES = {
    DEVICE_HEALTH_ALERT_VOICE_ACTION: DEVICE_HEALTH_ALERT_VOICE_ROUTE,
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION: DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
}


def match_device_health_alert_action_route(
    request: CompanyOperationRequestContract,
) -> str | None:
    """question 내용과 무관하게 검증된 typed metadata만 분류한다."""

    if str(request.metadata.get("route_group") or "").strip() != "operations":
        return None
    raw_action = request.metadata.get("operation_action")
    if not isinstance(raw_action, Mapping):
        return None
    action_name = str(raw_action.get("name") or "").strip()
    if action_name == DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION:
        return (
            DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE
            if str(raw_action.get("phase") or "").strip() == "receipt"
            else None
        )
    if action_name == DEVICE_HEALTH_ALERT_SMS_ACTION:
        phase = str(raw_action.get("phase") or "").strip()
        if phase == "prepare":
            return DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE
        if phase == "execute":
            return DEVICE_HEALTH_ALERT_SMS_ROUTE
        return None
    return _DEVICE_HEALTH_ALERT_ACTION_ROUTES.get(action_name)


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


def as_operations_request(
    request: CompanyOperationRequestContract,
) -> CompanyOperationRequestContract:
    """Slack의 원 요청을 실행 없이 operations 분류 문맥으로 좁힌다."""

    if str(request.metadata.get("route_group") or "").strip() == "operations":
        return request
    metadata: dict[str, Any] = dict(request.metadata)
    metadata["route_group"] = "operations"
    return replace(request, metadata=metadata)


def match_company_operation_route(
    request: CompanyOperationRequestContract,
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
    if device_action_route == DEVICE_SCANNER_ABI_PATCH_ROUTE:
        # 스캐너 패치와 학습/admin/file 등 다른 명령을 섞어도 일부만 먼저
        # 실행하지 않는다. 전용 exact parser가 전체 문장을 fail-closed한다.
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


def company_operation_legacy_stage(
    request: CompanyOperationRequestContract,
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


__all__ = [
    "ADMIN_READONLY_SQL_ROUTE",
    "ADMIN_REQUEST_LOG_ROUTE",
    "ADMIN_S3_DEVICE_LOG_ROUTE",
    "ADMIN_S3_ULTRASOUND_ROUTE",
    "APP_USER_BABY_ANALYSIS_ROUTE",
    "APP_USER_PROFILE_ROUTE",
    "BARCODE_PINK_CLASSIFICATION_ROUTE",
    "BARCODE_VALIDATION_STATUS_ROUTE",
    "CompanyOperationLegacyStage",
    "CompanyOperationRequestContract",
    "RECORDING_STREAMING_RESTORE_ROUTE",
    "RequestLogQuerySpec",
    "SECURITY_REVIEW_ACTION",
    "SECURITY_REVIEW_ROUTE",
    "_extract_db_query",
    "_extract_device_name_for_audio_probe",
    "_extract_device_name_for_diagnostic_freeform",
    "_extract_device_name_for_diagnostic_start",
    "_extract_device_name_for_remote_access_probe",
    "_extract_device_name_for_status_probe",
    "_extract_device_name_for_update",
    "_extract_hospital_room_scope_for_log_upload",
    "_extract_recording_streaming_restore_month",
    "_extract_request_log_query",
    "_extract_s3_request",
    "_has_device_diagnostic_start_hint",
    "_is_barcode_device_file_probe_request",
    "_is_barcode_pink_classification_reason_request",
    "_is_barcode_validation_status_request",
    "_is_device_agent_update_request",
    "_is_device_audio_probe_request",
    "_is_device_box_update_request",
    "_is_device_captureboard_probe_request",
    "_is_device_diagnostic_freeform_request",
    "_is_device_diagnostic_start_request",
    "_is_device_led_log_analysis_request",
    "_is_device_led_pattern_help_request",
    "_is_device_led_probe_request",
    "_is_device_log_upload_check_request",
    "_is_device_memory_patch_request",
    "_is_device_pm2_probe_request",
    "_is_device_power_off_request",
    "_is_device_remote_access_probe_request",
    "_is_device_status_probe_request",
    "_is_device_update_status_request",
    "_is_device_voice_catalog_request",
    "_is_device_voice_change_request",
    "_is_recording_streaming_restore_request",
    "_is_thread_playbook_learning_request",
    "_select_device_diagnostic_followup_command_keys",
    "_should_analyze_app_user_baby_selection",
    "_should_download_device_files",
    "_should_lookup_barcode",
    "_should_recover_device_files",
    "as_operations_request",
    "company_operation_legacy_stage",
    "match_company_operation_route",
    "match_device_health_alert_action_route",
    "match_security_review_route",
]
