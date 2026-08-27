from dataclasses import replace
import hashlib
import logging
import time
from typing import Any
from urllib.parse import urlsplit

import pymysql
from botocore.exceptions import BotoCoreError, ClientError
from slack_bolt import App

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _load_slack_permalink,
    _load_slack_user_name,
    _merge_request_log_metadata,
    _set_request_log_route,
    _set_request_log_skip_persist,
    _set_request_log_status,
    create_slack_app,
)
from boxer_adapter_slack.context import (
    _load_slack_thread_context,
    load_slack_thread_context_entries,
)
from boxer_company_adapter_slack.barcode_logs import (
    _needs_barcode_log_fallback,
    _split_barcode_log_reply,
)
from boxer_company_adapter_slack.barcode_query_routes import (
    BarcodeQueryRoutesContext,
    BarcodeQueryRoutesDeps,
    _handle_barcode_query_routes,
)
from boxer_company_adapter_slack.assistant_bridge import (
    assistant_slack_route_name,
    build_company_assistant_request,
    render_company_assistant_result,
    render_device_file_download_delivery,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
    CompanyApiClientSettings,
    CompanyAssistantApiClient,
    load_company_api_client_settings,
)
from boxer_company_adapter_slack.hpa_change_api_client import (
    HpaChangeApiClient,
    build_hpa_change_remote_routes_config,
)
from boxer_company_adapter_slack.hpa_change_remote_reporter import (
    attach_hpa_change_remote_reporter,
)
from boxer_company_adapter_slack.automation_api_client import (
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.company_api_rollout import (
    BoundedShadowRunner,
    wrap_company_barcode_freeform_service,
    wrap_company_barcode_log_service,
    wrap_company_barcode_residual_service,
    wrap_company_barcode_service,
    wrap_company_barcode_timeline_service,
    wrap_company_device_db_detail_service,
    wrap_company_device_filter_service,
    wrap_company_device_service,
    wrap_company_freeform_service,
    wrap_company_notion_service,
    wrap_company_operations_service,
    wrap_company_playbook_service,
    wrap_company_recording_failure_service,
    wrap_company_structured_service,
    wrap_company_weekly_summary_service,
)
from boxer_company_adapter_slack.barcode_routes import (
    BarcodeLogRouteContext,
    BarcodeLogRouteDeps,
    _handle_barcode_log_analysis_request,
)
from boxer_company_adapter_slack.admin_routes import (
    AdminRoutesContext,
    AdminRoutesDeps,
    _handle_admin_routes,
)
from boxer_company_adapter_slack.access_routes import (
    BASE_ACCESS_DENIED_REPLY,
    build_slack_base_access_runtime,
    handle_base_access_management_command,
)
from boxer_company_adapter_slack.company_notion_routes import (
    CompanyNotionRoutesContext,
    CompanyNotionRoutesDeps,
    _handle_company_notion_routes,
)
from boxer_company_adapter_slack.device_activity import (
    _build_device_download_activity_input,
)
from boxer_company_adapter_slack.device_routes import (
    DeviceRoutesContext,
    DeviceRoutesDeps,
    _handle_device_routes,
    _lookup_device_file_scope_from_mda_recovery_thread,
)
from boxer_company_adapter_slack.fun import handle_fun_message, is_human_fun_trigger
from boxer_company_adapter_slack.health import (
    _build_dependency_failure_reply,
    _format_ping_llm_status,
)
from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRoutesContext,
    HpaChangeRoutesDeps,
    _handle_hpa_change_request,
)
from boxer_company_adapter_slack.knowledge_routes import (
    KnowledgeRoutesContext,
    KnowledgeRoutesDeps,
    _handle_knowledge_routes,
)
from boxer_company_adapter_slack.notion_freeform import (
    _append_company_notion_doc_section,
    _append_notion_playbook_section,
    _build_freeform_chat_system_prompt,
    _build_freeform_response_rules,
    _build_notion_doc_fallback,
    _build_notion_doc_query_text,
    _build_notion_doc_security_refusal,
    _classify_freeform_response_mode,
    _get_freeform_system_prompt,
    _is_generic_count_or_existence_request,
    _is_notion_doc_exfiltration_attempt,
    _looks_like_notion_doc_followup,
    _looks_like_notion_doc_question,
    _needs_notion_doc_fallback,
    _needs_notion_doc_security_refusal,
    _normalize_notion_doc_answer_style,
    _resolve_notion_doc_thread_context,
    _sanitize_freeform_reply,
    _sanitize_notion_references_for_llm,
    _sanitize_notion_doc_thread_context,
)
from boxer_company_adapter_slack.recording_failure_routes import (
    RecordingFailureRouteContext,
    RecordingFailureRouteDeps,
    _handle_recording_failure_analysis_request,
)
from boxer_company_adapter_slack.security_review_routes import (
    SecurityReviewMessageContext,
    SecurityReviewRoutesContext,
    _handle_security_review_bot_message,
    _handle_security_review_request,
)
from boxer_company_adapter_slack.structured_routes import (
    StructuredRoutesContext,
    _handle_structured_routes,
)
from boxer_company_adapter_slack.thread_learning_routes import (
    ThreadLearningRoutesContext,
    _handle_thread_learning_routes,
    _load_thread_context_entries_for_learning,
)
from boxer_company_adapter_slack.daily_device_round_reporter import attach_daily_device_round_reporter
from boxer_company_adapter_slack.device_health_monitor_reporter import (
    _send_device_health_monitor_auto_sms_for_item,
    attach_device_health_monitor_reporter,
)
from boxer_company_adapter_slack.device_health_alert_api import (
    DeviceHealthAlertApiBridge,
)
from boxer_company_adapter_slack.device_notification_alert_reporter import (
    attach_device_notification_alert_reporter,
)
from boxer_company_adapter_slack.weekly_reports import (
    _build_weekly_recordings_report_reply_payload,
    _extract_optional_requested_date,
    _is_weekly_recordings_report_request,
)
from boxer_company_adapter_slack.weekly_recordings_reporter import attach_weekly_recordings_reporter
from boxer_company_adapter_slack.startup_guard import _validate_ec2_runtime_aws_env
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.notion_links import select_company_notion_doc_links
from boxer_company.notion_playbooks import _select_notion_references
from boxer_company.notion_workspace_search import (
    _build_company_notion_source_docs,
    _extract_company_notion_search_query,
    _is_company_notion_search_configured,
    _load_company_notion_references,
    _looks_like_company_notion_search,
    _search_company_notion,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.assistant import (
    CompanyAssistantService,
    CompanyAssistantRuntime,
    CompanyAssistantRuntimeDeps,
    CompanyAssistantRequest,
    CompanyNotionAssistantRouteDeps,
    CompanyReadOnlyKnowledgeRouteDeps,
    build_company_read_only_knowledge_routes,
)
from boxer_company.assistant.knowledge_routes import (
    match_barcode_evidence_freeform_route,
)
from boxer_company.assistant.device_file_operations_route import (
    DEVICE_FILE_DOWNLOAD_ROUTE,
    DEVICE_FILE_LOOKUP_ROUTE,
    DEVICE_FILE_RECOVERY_ROUTE,
    build_trusted_mda_recovery_scope_metadata,
    resolve_device_file_operation_scope,
)
from boxer_company.assistant.operations import (
    company_operation_legacy_stage,
    company_operation_route_names,
    match_company_operation_route,
    needs_device_file_operation_context,
)
from boxer_company.assistant.device_operations_route import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_OPERATION_DELIVERY_ACTION,
)
from boxer_company import settings as cs
# 기존 characterization test와 외부 patch 지점만 유지하고 실제 추출은 runtime이 맡는다.
from boxer_company.utils import _extract_barcode
from boxer import AnswerEngine, synthesize_retrieval_answer
from boxer.context.builder import _build_model_input
from boxer.context.entries import ContextEntry
from boxer.core import settings as s
from boxer.core.llm import (
    _ask_claude,
    _ask_ollama_chat,
    _build_claude_client,
    _check_claude_health,
    _check_ollama_health,
)
from boxer.core.utils import _validate_tokens
from boxer.retrieval.connectors.notion import _is_notion_configured
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer_company.routers.app_user import _lookup_app_user_by_barcode, _should_lookup_barcode
from boxer_company.routers.barcode_log import (
    _analyze_barcode_log_scan_events,
    _extract_capture_seq_filters,
    _extract_device_flag_filters,
    _extract_device_name_scope,
    _extract_device_seq_filter,
    _extract_device_status_filter,
    _extract_hospital_room_scope,
    _extract_leading_hospital_scope,
    _extract_log_date,
    _is_barcode_log_analysis_request,
    _is_barcode_last_recorded_at_request,
    _is_barcode_video_recorded_on_date_request,
)
from boxer_company.routers.device_file_probe import (
    _build_device_file_download_config_message,
    _build_device_file_probe_config_message,
    _build_device_file_recovery_config_message,
    _build_device_file_scope_request_message,
    _is_barcode_device_file_probe_request,
    _locate_barcode_file_candidates,
    _should_download_device_files,
    _should_probe_device_files,
    _should_recover_device_files,
    _should_render_compact_file_id_result,
    _should_render_compact_device_download_result,
    _should_render_compact_device_file_list,
    _should_render_compact_device_recovery_result,
)
from boxer_company.routers.device_diagnostics import (
    _extract_device_name_for_diagnostic_freeform,
    _is_device_diagnostic_freeform_request,
    _load_device_diagnostic_snapshot,
    _select_device_diagnostic_followup_command_keys,
)
from boxer_company.routers.device_audio_probe import (
    _build_device_audio_probe_config_message,
    _extract_device_name_for_audio_probe,
    _is_device_audio_probe_request,
    _probe_device_audio_output,
)
from boxer_company.routers.device_update import (
    _build_device_update_config_message,
    _extract_device_name_for_update,
    _is_device_agent_update_request,
    _is_device_box_update_request,
    _is_device_update_status_request,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
)
from boxer_company.routers.device_status_probe import (
    _build_device_memory_patch_config_message,
    _build_device_status_probe_config_message,
    _extract_device_name_for_status_probe,
    _is_device_captureboard_probe_request,
    _is_device_led_probe_request,
    _is_device_led_pattern_help_request,
    _is_device_memory_patch_request,
    _is_device_pm2_probe_request,
    _is_device_status_probe_request,
    _patch_device_pm2_memory,
    _probe_device_runtime_component,
    _probe_device_status_overview,
)
from boxer_company.routers.request_log_query import (
    _extract_request_log_query,
)
from boxer_company.routers.recording_failure_analysis import (
    _is_recording_failure_analysis_request,
)
from boxer_company.routers.box_db import (
    _load_recordings_context_by_barcode,
    _lookup_device_contexts_by_hospital_room,
)
from boxer_company.routers.s3_domain import (
    _extract_s3_request,
)
from boxer_company.routers.usage_help import (
    _build_usage_help_response,
    _is_usage_help_request,
)

# 기존 테스트·외부 patch 지점을 유지하되 실제 구현은 공개 facade를 통한다.
def _synthesize_retrieval_answer(
    question: str,
    thread_context: str,
    evidence_payload: Any,
    *,
    provider: str,
    provider_client: Any | None = None,
    timeout_sec: int | None = None,
    claude_client: Any | None = None,
    system_prompt: str | None = None,
    extra_rules: str = "",
    evidence_transform: Any | None = None,
    max_tokens: int | None = None,
    ollama_timeout_sec: int | None = None,
) -> str:
    """기존 Slack 호출 규격을 공개 provider 중립 facade로 연결한다."""
    return synthesize_retrieval_answer(
        question,
        thread_context,
        evidence_payload,
        provider=provider,
        provider_client=(
            provider_client
            if provider_client is not None
            else claude_client
        ),
        system_prompt=system_prompt,
        extra_rules=extra_rules,
        evidence_transform=evidence_transform,
        max_tokens=max_tokens,
        timeout_sec=(
            timeout_sec
            if timeout_sec is not None
            else ollama_timeout_sec
        ),
    )


def _has_enabled_local_data_reporter(
    *,
    automation_remote_cycles: tuple[str, ...] = (),
) -> bool:
    """Slack 프로세스에서 DB/S3 근거를 직접 읽는 리포터 활성 여부다."""

    # cycle별로 소유권을 바꾸므로 allowlist 밖에 남은 리포터는
    # 기존 Slack DB·S3·MDA 설정을 계속 요구한다.
    remote_cycles = frozenset(automation_remote_cycles)
    return any(
        (
            cs.WEEKLY_RECORDINGS_REPORT_ENABLED
            and "weekly_recordings" not in remote_cycles,
            cs.DEVICE_HEALTH_MONITOR_ENABLED
            and "device_health_monitor" not in remote_cycles,
            cs.DEVICE_NOTIFICATION_ALERT_ENABLED
            and "device_notification_alert" not in remote_cycles,
            cs.DAILY_DEVICE_ROUND_ENABLED
            and "daily_device_round" not in remote_cycles,
        )
    )


def _validate_automation_sms_cycle_ownership(
    settings: CompanyApiClientSettings,
) -> None:
    """Solapi outbox producer와 consumer가 다른 프로세스로 갈라지지 않게 한다."""

    remote_cycles = (
        frozenset(settings.automation_remote_cycles)
        if settings.automation_mode == "remote"
        else frozenset()
    )
    enabled_producers = (
        (
            "device_health_monitor",
            bool(cs.DEVICE_HEALTH_MONITOR_ENABLED),
        ),
        (
            "device_notification_alert",
            bool(cs.DEVICE_NOTIFICATION_ALERT_ENABLED),
        ),
    )
    has_local_producer = any(
        enabled and cycle not in remote_cycles
        for cycle, enabled in enabled_producers
    )
    has_remote_producer = any(
        enabled and cycle in remote_cycles
        for cycle, enabled in enabled_producers
    )
    sms_delivery_remote = "sms_delivery" in remote_cycles
    manual_api_solapi_producer = bool(
        settings.operations_mode == "remote"
        and any(enabled for _cycle, enabled in enabled_producers)
        and str(cs.DEVICE_HEALTH_MONITOR_SMS_PROVIDER or "")
        .strip()
        .lower()
        == "solapi"
    )

    # local producer가 Slack 상태에 기록한 delivery를 API outbox
    # consumer가 볼 수 없으므로 혼합 소유권을 기동 전에 차단한다.
    if has_local_producer and sms_delivery_remote:
        raise CompanyApiContractError(
            "company_api_remote_sms_with_local_producer_unsafe"
        )
    if (
        has_remote_producer
        and cs.SMS_DELIVERY_REPORTER_ENABLED
        and not sms_delivery_remote
    ):
        raise CompanyApiContractError(
            "company_api_remote_sms_producer_without_consumer_unsafe"
        )
    if manual_api_solapi_producer and (
        has_local_producer
        or not cs.SMS_DELIVERY_REPORTER_ENABLED
        or not sms_delivery_remote
    ):
        # operations remote의 수동 문자도 API outbox producer다. 로컬 자동
        # producer와 공존하거나 API SMS drain이 없으면 어느 한쪽 receipt가
        # 영구 고립되므로 source cycle과 consumer를 함께 넘긴다.
        raise CompanyApiContractError(
            "company_api_remote_action_sms_ownership_unsafe"
        )


def _require_transport_only_remote_settings(
    settings: CompanyApiClientSettings,
) -> None:
    """production Slack entry에서 legacy 실행 경계를 다시 열지 못하게 한다."""

    if (
        settings.transport_only_remote
        and bool(
            getattr(
                cs,
                "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
                False,
            )
        )
    ):
        return
    # mode뿐 아니라 scheduler 소유권 flag도 함께 고정해 설정 실수로
    # Slack due timer와 domain cycle endpoint가 다시 열리지 않게 한다.
    raise CompanyApiContractError(
        "company_api_transport_only_remote_required"
    )


def create_app() -> App:
    _validate_ec2_runtime_aws_env()
    app_logger = logging.getLogger(__name__)
    company_api_settings = load_company_api_client_settings()
    _require_transport_only_remote_settings(company_api_settings)
    _validate_automation_sms_cycle_ownership(company_api_settings)
    transport_only_remote = company_api_settings.transport_only_remote
    local_llm_required = not transport_only_remote
    local_data_sources_required = (
        not transport_only_remote
        or _has_enabled_local_data_reporter(
            automation_remote_cycles=(
                company_api_settings.automation_remote_cycles
            ),
        )
    )
    _validate_tokens(
        include_llm=local_llm_required,
        include_data_sources=local_data_sources_required,
    )
    base_access_runtime = build_slack_base_access_runtime(logger=app_logger)
    company_api_client = (
        CompanyAssistantApiClient(company_api_settings)
        if company_api_settings.enabled
        else None
    )
    automation_api_client = (
        CompanyAutomationApiClient(
            company_api_settings,
            logger=app_logger,
        )
        if company_api_settings.automation_mode == "remote"
        else None
    )

    def _automation_client_for_cycle(
        cycle: str,
    ) -> CompanyAutomationApiClient | None:
        """allowlist에 명시된 cycle에만 remote transport를 주입한다."""

        if (
            automation_api_client is not None
            and company_api_settings.is_automation_cycle_remote(cycle)
        ):
            return automation_api_client
        return None

    weekly_automation_client = _automation_client_for_cycle(
        "weekly_recordings"
    )
    daily_automation_client = _automation_client_for_cycle(
        "daily_device_round"
    )
    health_automation_client = _automation_client_for_cycle(
        "device_health_monitor"
    )
    notification_automation_client = _automation_client_for_cycle(
        "device_notification_alert"
    )
    sms_delivery_automation_client = _automation_client_for_cycle(
        "sms_delivery"
    )
    device_health_alert_api_bridge = (
        DeviceHealthAlertApiBridge(company_api_client)
        if (
            company_api_client is not None
            and company_api_settings.operations_mode == "remote"
        )
        else None
    )
    company_api_shadow_runner = (
        BoundedShadowRunner(logger=app_logger)
        if company_api_settings.shadow_enabled
        else None
    )

    def _remote_ping_health(payload: MentionPayload) -> bool | None:
        """remote freeform 소유권에서는 provider probe도 공통 API에서 읽는다."""

        if company_api_client is None:
            raise CompanyApiContractError("company_api_client_not_configured")
        request = build_company_assistant_request(payload)
        result = company_api_client.answer(request, route_group="health")
        if (
            result.route != "company_llm_health"
            or result.outcome != "answered"
            or result.used_llm
            or len(result.messages) != 1
            or result.sources
            or result.messages[0].delivery_scope != "conversation"
            or result.messages[0].mention_actor
            or result.messages[0].private_links
        ):
            raise CompanyApiContractError("company_api_health_result_invalid")
        status = str(result.messages[0].body or "").strip()
        if status == "available":
            return True
        if status == "unavailable":
            return False
        if status == "unconfigured":
            return None
        raise CompanyApiContractError("company_api_health_result_invalid")

    def _remote_fun_reply(
        payload: Any,
        raw_text: str,
        thread_context: str,
        speaker_user_id: str,
    ) -> tuple[str, str, bool]:
        """Slack context만 직렬화하고 fun 생성 자체는 공통 API에 맡긴다."""

        if company_api_client is None:
            raise CompanyApiContractError("company_api_client_not_configured")
        if speaker_user_id != str(payload.get("user_id") or "").strip():
            raise CompanyApiContractError("company_api_fun_actor_invalid")
        request_payload = dict(payload)
        request_payload["question"] = raw_text
        request = build_company_assistant_request(
            request_payload,
            metadata={"team_fun_context": thread_context},
        )
        result = company_api_client.answer(request, route_group="fun")
        valid_message = bool(
            len(result.messages) == 1
            and not result.sources
            and result.messages[0].delivery_scope == "conversation"
            and not result.messages[0].private_links
            and str(result.messages[0].body or "").strip()
        )
        if result.route != "company_team_fun" or not valid_message:
            raise CompanyApiContractError("company_api_fun_result_invalid")
        body = str(result.messages[0].body or "").strip()
        if result.outcome == "answered" and isinstance(result.used_llm, bool):
            # 기존 fun은 provider 장애의 template fallback도 정상 답변으로
            # 취급하고 디디 mention 여부를 유지한다.
            return body, "company_api", True
        if (
            result.outcome == "denied"
            and result.fallback_reason == "prompt_security"
            and result.used_llm is False
        ):
            # prompt 판정도 API가 소유하되 기존 거절문은 디디 mention 없이
            # Slack adapter가 그대로 전달한다.
            return body, "company_api_prompt_security", False
        raise CompanyApiContractError("company_api_fun_result_invalid")

    def _remote_fortune_reply(
        payload: Any,
        raw_text: str,
        thread_root_text: str,
        speaker_user_id: str,
    ) -> str | None:
        """bot event 문맥만 보내고 운세 의미 판정과 조립은 API에 맡긴다."""

        if company_api_client is None:
            raise CompanyApiContractError("company_api_client_not_configured")
        expected_bot_actor_id = str(
            payload.get("user_id")
            or payload.get("bot_user_id")
            or payload.get("bot_id")
            or payload.get("app_id")
            or ""
        ).strip()
        if (
            not speaker_user_id
            or speaker_user_id != expected_bot_actor_id
        ):
            raise CompanyApiContractError("company_api_fun_actor_invalid")
        request_payload = dict(payload)
        request_payload["question"] = raw_text
        # Slack bot event에는 user가 없을 수 있어 bot_user_id를 서버가 검증할
        # non-null actor로 승격하되, payload 밖 값을 받아들이지는 않는다.
        request_payload["user_id"] = speaker_user_id
        context_entries: tuple[ContextEntry, ...] = (
            (
                {
                    "kind": "message",
                    "source": "slack",
                    "text": thread_root_text,
                },
            )
            if thread_root_text
            else ()
        )
        request = build_company_assistant_request(
            request_payload,
            context_entries=context_entries,
        )
        result = company_api_client.answer(request, route_group="fun")

        if (
            result.route == "unhandled"
            and result.outcome == "no_evidence"
            and not result.used_llm
            and result.fallback_reason == "no_matching_route"
            and len(result.messages) == 1
            and not result.sources
            and result.messages[0].delivery_scope == "conversation"
            and result.messages[0].mention_actor
            and not result.messages[0].private_links
        ):
            # 같은 채널의 다른 bot thread는 API의 no-match를 그대로 무응답 처리한다.
            return None
        if (
            result.route != "company_daily_fortune"
            or result.outcome != "answered"
            or result.used_llm
            or result.fallback_reason is not None
            or len(result.messages) != 1
            or result.sources
            or result.messages[0].delivery_scope != "conversation"
            or result.messages[0].mention_actor
            or result.messages[0].private_links
        ):
            raise CompanyApiContractError("company_api_fortune_result_invalid")
        body = str(result.messages[0].body or "").strip()
        if not body:
            raise CompanyApiContractError("company_api_fortune_result_invalid")
        return body

    app_logger.info(
        "Company API rollout configured "
        "notion_mode=%s notion_local_fallback=%s "
        "structured_mode=%s structured_local_fallback=%s "
        "device_mode=%s device_local_fallback=%s "
        "device_detail_mode=%s device_detail_local_fallback=%s "
        "weekly_summary_mode=%s weekly_summary_local_fallback=%s "
        "recording_failure_mode=%s recording_failure_local_fallback=%s "
        "barcode_log_mode=%s barcode_log_local_fallback=%s "
        "barcode_mode=%s barcode_local_fallback=%s "
        "barcode_residual_mode=%s barcode_residual_local_fallback=%s "
        "barcode_timeline_mode=%s barcode_timeline_local_fallback=%s "
        "playbook_mode=%s playbook_local_fallback=%s "
        "barcode_freeform_mode=%s barcode_freeform_local_fallback=%s "
        "freeform_mode=%s freeform_local_fallback=%s "
        "operations_mode=%s operations_local_fallback=%s "
        "automation_mode=%s automation_local_fallback=%s",
        company_api_settings.notion_mode,
        company_api_settings.notion_fallback_enabled,
        company_api_settings.structured_mode,
        company_api_settings.structured_fallback_enabled,
        company_api_settings.device_mode,
        company_api_settings.device_fallback_enabled,
        company_api_settings.device_detail_mode,
        company_api_settings.device_detail_fallback_enabled,
        company_api_settings.weekly_summary_mode,
        company_api_settings.weekly_summary_fallback_enabled,
        company_api_settings.recording_failure_mode,
        company_api_settings.recording_failure_fallback_enabled,
        company_api_settings.barcode_log_mode,
        company_api_settings.barcode_log_fallback_enabled,
        company_api_settings.barcode_mode,
        company_api_settings.barcode_fallback_enabled,
        company_api_settings.barcode_residual_mode,
        company_api_settings.barcode_residual_fallback_enabled,
        company_api_settings.barcode_timeline_mode,
        company_api_settings.barcode_timeline_fallback_enabled,
        company_api_settings.playbook_mode,
        company_api_settings.playbook_fallback_enabled,
        company_api_settings.barcode_freeform_mode,
        company_api_settings.barcode_freeform_fallback_enabled,
        company_api_settings.freeform_mode,
        company_api_settings.freeform_fallback_enabled,
        company_api_settings.operations_mode,
        company_api_settings.operations_fallback_enabled,
        company_api_settings.automation_mode,
        company_api_settings.automation_fallback_enabled,
    )
    # 완전 remote 상태에서는 provider 이름조차 local AnswerEngine에 넣지
    # 않아 Slack 프로세스가 LLM client나 health probe를 소유하지 않는다.
    local_answer_provider = (
        (s.LLM_PROVIDER or "").lower().strip()
        if local_llm_required
        else ""
    )
    claude_client = None
    if local_answer_provider == "claude":
        try:
            claude_client = _build_claude_client(timeout_sec=s.ANTHROPIC_TIMEOUT_SEC)
        except Exception:
            app_logger.warning("Failed to initialize Claude client; continuing without it", exc_info=True)
    hpa_change_routes_config = build_hpa_change_remote_routes_config()
    hpa_change_api_client = (
        HpaChangeApiClient(
            company_api_settings,
            workspace_id=company_api_settings.automation_tenant_id,
            logger=app_logger,
        )
        if hpa_change_routes_config.enabled
        else None
    )

    def _submit_hpa_change_request(request: Any) -> Any:
        if hpa_change_api_client is None:
            raise CompanyApiContractError(
                "company_api_hpa_transport_disabled"
            )
        return hpa_change_api_client.submit_request(request)

    def _lookup_hpa_change_thread(*args: Any, **kwargs: Any) -> Any:
        if hpa_change_api_client is None:
            raise CompanyApiContractError(
                "company_api_hpa_transport_disabled"
            )
        return hpa_change_api_client.lookup_thread_job(*args, **kwargs)
    s3_client: Any | None = None

    def _get_s3_client() -> Any:
        nonlocal s3_client
        if s3_client is None:
            s3_client = _build_s3_client()
        return s3_client

    ollama_health_cache: tuple[float, dict[str, Any]] | None = None

    def _get_ollama_health() -> dict[str, Any]:
        nonlocal ollama_health_cache
        now = time.monotonic()
        if ollama_health_cache is not None:
            cached_at, cached_health = ollama_health_cache
            ttl = 30.0 if cached_health.get("ok") else 2.0
            if now - cached_at < ttl:
                return cached_health
        health = _check_ollama_health()
        # 같은 요청이 core 위임 뒤 legacy 안내로 내려가도
        # health timeout을 다시 기다리지 않도록 전체 결과를 공유한다.
        ollama_health_cache = (now, health)
        return health

    def _is_answer_provider_ready() -> bool:
        provider = local_answer_provider
        if provider == "claude":
            return claude_client is not None
        if provider == "ollama":
            return bool(_get_ollama_health()["ok"])
        return False

    def _answer_timeout_reply_text() -> str:
        provider = local_answer_provider
        if provider == "claude":
            timeout_sec = max(1, s.ANTHROPIC_TIMEOUT_SEC)
            return f"AI API가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"
        timeout_sec = max(1, s.OLLAMA_TIMEOUT_SEC)
        return f"LLM 서버가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"

    company_answer_engine = AnswerEngine(
        provider=local_answer_provider,
        provider_client=claude_client,
        synthesize=_synthesize_retrieval_answer,
        logger=app_logger,
    )

    def _load_diagnostic_snapshot(
        request: CompanyAssistantRequest,
    ) -> dict[str, Any] | None:
        metadata_channel_id = request.metadata.get("channel_id")
        channel_key = (
            str(metadata_channel_id).strip()
            if isinstance(metadata_channel_id, str)
            else request.channel
        )
        return _load_device_diagnostic_snapshot(
            workspace_id=request.tenant_id,
            channel_id=channel_key,
            thread_ts=request.conversation_id,
        )

    def _load_read_only_diagnostic_snapshot(
        request: CompanyAssistantRequest,
    ) -> dict[str, Any] | None:
        # live 진단은 sshOrder를 보낼 수 있으므로 공통 read-only route가
        # 저장 snapshot만 답할 수 있는 질문에서만 기존 저장소를 연다.
        if _select_device_diagnostic_followup_command_keys(
            request.question
        ):
            return None
        return _load_diagnostic_snapshot(request)

    def _should_handle_barcode_evidence(
        request: CompanyAssistantRequest,
    ) -> bool:
        # live 장비 진단과 provider 장애 안내는 기존 Slack 전용 경로가
        # 먼저 처리하도록 공통 read-only 자유질문에서 제외한다.
        command_keys = _select_device_diagnostic_followup_command_keys(
            request.question
        )
        if command_keys and _load_diagnostic_snapshot(request) is not None:
            return False
        device_name = _extract_device_name_for_diagnostic_freeform(
            request.question
        )
        if _is_device_diagnostic_freeform_request(
            request.question,
            device_name=device_name,
        ):
            return False
        provider = local_answer_provider
        if (
            not s.LLM_SYNTHESIS_ENABLED
            or provider not in {"claude", "ollama"}
        ):
            return False
        return (
            _is_answer_provider_ready()
            and match_barcode_evidence_freeform_route(request)
            == "barcode_evidence_freeform"
        )

    def _build_read_only_knowledge_routes(
        recordings,
        composer,
    ):
        provider = local_answer_provider
        return build_company_read_only_knowledge_routes(
            recordings,
            composer,
            CompanyReadOnlyKnowledgeRouteDeps(
                load_diagnostic_snapshot=(
                    _load_read_only_diagnostic_snapshot
                ),
                barcode_should_handle=_should_handle_barcode_evidence,
                db_configured=lambda: bool(
                    s.DB_HOST
                    and s.DB_USERNAME
                    and s.DB_PASSWORD
                    and s.DB_DATABASE
                ),
                build_barcode_system_prompt=(
                    lambda request, context_text: (
                        _get_freeform_system_prompt(
                            request.question,
                            context_text,
                        )
                    )
                ),
                timeout_message=_answer_timeout_reply_text(),
                # provider가 없으면 기존 일반 사용법 fallback까지 내려간다.
                include_barcode_evidence=provider in {"claude", "ollama"},
            ),
            logger=app_logger,
        )

    company_assistant_runtime = CompanyAssistantRuntime(
        CompanyAssistantRuntimeDeps(
            answer_engine=company_answer_engine,
            synthesis_enabled=s.LLM_SYNTHESIS_ENABLED,
            provider_ready=_is_answer_provider_ready,
            get_s3_client=_get_s3_client,
            recordings_loader=_load_recordings_context_by_barcode,
            notion_reference_loader=_select_notion_references,
            s3_query_enabled=lambda: s.S3_QUERY_ENABLED,
            db_configured=lambda: bool(
                s.DB_HOST
                and s.DB_USERNAME
                and s.DB_PASSWORD
                and s.DB_DATABASE
            ),
            # Slack local runtime에서만 장비 SSH lifecycle 보강을 허용한다.
            # 공통 API는 factory에서 항상 false로 고정한다.
            log_analysis_live_enrichment_enabled=True,
            timeout_message=_answer_timeout_reply_text(),
            notion_route_deps=CompanyNotionAssistantRouteDeps(
                answer_engine=company_answer_engine,
                synthesis_enabled=s.LLM_SYNTHESIS_ENABLED,
                provider_ready=_is_answer_provider_ready,
                looks_like_search=_looks_like_company_notion_search,
                is_search_configured=_is_company_notion_search_configured,
                extract_query=_extract_company_notion_search_query,
                search=_search_company_notion,
                load_references=_load_company_notion_references,
            ),
        ),
        knowledge_route_factory=_build_read_only_knowledge_routes,
        logger=app_logger,
    )

    def _handle_company_mention(
        payload: MentionPayload,
        reply: SlackReplyFn,
        client: Any,
        logger: logging.Logger,
    ) -> None:
        text = payload["text"]
        question = payload["question"]
        user_id = payload["user_id"]
        workspace_id = payload["workspace_id"]
        channel_id = payload["channel_id"]
        current_ts = payload["current_ts"]
        thread_ts = payload["thread_ts"]

        # 현의 exact 관리 명령만 일반 진입권 검사보다 먼저 처리한다.
        if handle_base_access_management_command(
            payload,
            reply,
            client,
            logger,
            runtime=base_access_runtime,
        ):
            return
        if not base_access_runtime.is_allowed(workspace_id, user_id):
            _set_request_log_route(payload, "base_access")
            _set_request_log_status(payload, "denied")
            reply(BASE_ACCESS_DENIED_REPLY)
            return

        # 코드 변경 요청은 일반 질의나 ping보다 먼저 격리 worker intake로 고정한다.
        if _handle_hpa_change_request(
            HpaChangeRoutesContext(
                question=question,
                payload=payload,
                user_id=user_id,
                workspace_id=workspace_id,
                channel_id=channel_id,
                current_ts=current_ts,
                thread_ts=thread_ts,
                reply=reply,
                client=client,
                logger=logger,
            ),
            hpa_change_routes_config,
            HpaChangeRoutesDeps(
                submit_request=_submit_hpa_change_request,
                lookup_thread_job=_lookup_hpa_change_thread,
            ),
        ):
            return

        if "ping" in text:
            _set_request_log_route(payload, "ping")
            if company_api_settings.freeform_mode == "remote":
                try:
                    health_ok = _remote_ping_health(payload)
                except Exception as exc:
                    # remote 전환 뒤 API 장애를 Slack-local provider probe로
                    # 우회하지 않고 안전한 불가 상태로만 응답한다.
                    logger.warning(
                        "Company API ping health failed thread_ts=%s error_type=%s",
                        thread_ts,
                        type(exc).__name__,
                    )
                    health_ok = False
                reply(
                    f"🏓 pong\n• llm: {_format_ping_llm_status(health_ok)}"
                )
                logger.info(
                    "Responded with remote ping health in thread_ts=%s ok=%s",
                    thread_ts,
                    health_ok,
                )
                return
            provider = local_answer_provider
            if provider == "ollama":
                health = _get_ollama_health()
                reply(f"🏓 pong\n• llm: {_format_ping_llm_status(bool(health['ok']))}")
                logger.info(
                    "Responded with ping health in thread_ts=%s provider=ollama ok=%s",
                    thread_ts,
                    health["ok"],
                )
                return
            if provider == "claude":
                health = _check_claude_health()
                reply(f"🏓 pong\n• llm: {_format_ping_llm_status(bool(health['ok']))}")
                logger.info(
                    "Responded with ping health in thread_ts=%s provider=claude ok=%s summary=%s",
                    thread_ts,
                    health["ok"],
                    health["summary"],
                )
                return

            reply(f"🏓 pong\n• llm: {_format_ping_llm_status(None)}")
            logger.info("Responded with ping health in thread_ts=%s provider=none", thread_ts)
            return

        if _is_usage_help_request(question):
            _set_request_log_route(payload, "usage_help", route_mode="guide")
            reply(_build_usage_help_response(), mention_user=False)
            logger.info("Responded with usage help in thread_ts=%s", thread_ts)
            return

        def _timeout_reply_text() -> str:
            return _answer_timeout_reply_text()

        def _llm_unavailable_reply_text(summary: str | None = None) -> str:
            provider = local_answer_provider
            if provider == "claude":
                base = "AI API가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
            else:
                base = "LLM 서버가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
            detail = (summary or "").strip()
            if not detail:
                return base
            return f"{base}\n• 상태: {detail}"

        def _is_timeout_error(exc: Exception) -> bool:
            lowered = str(exc).lower()
            return "timeout" in lowered or "timed out" in lowered

        def _send_dm_message(target_user_id: str | None, message_text: str) -> bool:
            if not target_user_id or not (message_text or "").strip():
                return False
            try:
                response = client.conversations_open(users=[target_user_id])
                dm_channel = ((response or {}).get("channel") or {}).get("id")
                if not dm_channel:
                    return False
                client.chat_postMessage(
                    channel=dm_channel,
                    text=message_text,
                    unfurl_links=False,
                    unfurl_media=False,
                )
                return True
            except Exception:
                logger.exception("Failed to send DM to user=%s", target_user_id)
                return False

        assistant_context_entries: tuple[ContextEntry, ...] = ()
        assistant_context_loaded = False

        def _get_assistant_context_entries() -> tuple[ContextEntry, ...]:
            nonlocal assistant_context_entries, assistant_context_loaded
            if assistant_context_loaded:
                return assistant_context_entries
            assistant_context_loaded = True
            assistant_context_entries = tuple(
                load_slack_thread_context_entries(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
            )
            return assistant_context_entries

        def _should_load_llm_context() -> bool:
            if (
                not s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT
                or not s.LLM_SYNTHESIS_ENABLED
            ):
                return False
            return _is_answer_provider_ready()

        def _should_load_route_synthesis_context(
            route_mode: str,
        ) -> bool:
            # remote provider가 합성하는 route는 Slack-local provider 상태와
            # 무관하게 설정된 bounded context를 전달한다. local rollback은
            # 기존 provider-ready 조건을 그대로 유지한다.
            if str(route_mode or "").strip() != "local":
                return bool(
                    s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT
                    and s.LLM_SYNTHESIS_ENABLED
                )
            return _should_load_llm_context()

        def _needs_recording_failure_analysis_fallback(
            synthesized: str,
            fallback_text: str,
            route_name: str,
        ) -> bool:
            if route_name != "recording failure analysis":
                return False

            normalized_synth = (synthesized or "").strip()
            normalized_fallback = (fallback_text or "").strip()
            required_bullets = (
                "• 핵심 원인:",
                "• 운영 근거:",
                "• 영향:",
                "• 권장 조치:",
                "• 확실도:",
            )
            reasoning_leak_tokens = (
                "</think>",
                "<think>",
                "let me ",
                "i need to",
                "the user",
                "based on",
                "looking at",
                "now, checking",
                "wait,",
                "wait ",
                "for the ",
                "the error",
            )

            if normalized_fallback.startswith("*녹화 실패 원인 분석*") and not normalized_synth.startswith("*녹화 실패 원인 분석*"):
                return True

            lowered = normalized_synth.lower()
            if any(token in lowered for token in reasoning_leak_tokens):
                return True

            for bullet in required_bullets:
                if bullet in normalized_fallback and bullet not in normalized_synth:
                    return True

            if "캡처보드" in normalized_fallback and "캡처보드" not in normalized_synth:
                return True

            return False

        def _needs_device_audio_probe_fallback(
            synthesized: str,
            fallback_text: str,
            route_name: str,
        ) -> bool:
            if route_name != "device audio probe":
                return False

            normalized_synth = (synthesized or "").strip()
            normalized_fallback = (fallback_text or "").strip()
            required_bullets = (
                "• 장비:",
                "• 판정:",
                "• 근거:",
                "• 안내:",
            )

            if normalized_fallback.startswith("*장비 소리 출력 점검*") and not normalized_synth.startswith("*장비 소리 출력 점검*"):
                return True

            for bullet in required_bullets:
                if bullet in normalized_fallback and bullet not in normalized_synth:
                    return True

            return False

        def _needs_device_led_pattern_fallback(
            synthesized: str,
            fallback_text: str,
            route_name: str,
        ) -> bool:
            if route_name != "device led pattern guide":
                return False

            normalized_synth = (synthesized or "").strip()
            normalized_fallback = (fallback_text or "").strip()
            required_bullets = (
                "• 결론:",
                "• 근거:",
                "• 참고 상태:",
                "• 안내:",
            )

            if normalized_fallback.startswith("*LED 증상 안내*") and not normalized_synth.startswith("*LED 증상 안내*"):
                return True

            for bullet in required_bullets:
                if bullet in normalized_fallback and bullet not in normalized_synth:
                    return True

            return False

        def _attach_notion_playbooks_to_evidence(
            evidence_payload: dict[str, Any] | None,
        ) -> list[dict[str, Any]]:
            if not isinstance(evidence_payload, dict):
                return []

            existing = evidence_payload.get("notionPlaybooks")
            if isinstance(existing, list) and existing:
                return [item for item in existing if isinstance(item, dict)]
            return []

        def _reply_with_retrieval_synthesis(
            fallback_text: str,
            evidence_payload: dict[str, Any],
            route_name: str,
            *,
            max_tokens: int | None = None,
        ) -> None:
            _set_request_log_route(payload, route_name, handler_type="router")
            notion_playbooks = _attach_notion_playbooks_to_evidence(evidence_payload)
            evidence_route = str(evidence_payload.get("route") or "").strip().lower()
            company_notion_docs: list[dict[str, str]] = []
            if evidence_route == "company_notion_qa":
                references = evidence_payload.get("companyNotionReferences")
                company_notion_docs = _build_company_notion_source_docs(
                    references if isinstance(references, list) else []
                )
                fallback_with_references = _append_company_notion_doc_section(
                    fallback_text,
                    company_notion_docs,
                )
            elif evidence_route == "notion_playbook_qa":
                request_payload = evidence_payload.get("request") if isinstance(evidence_payload.get("request"), dict) else {}
                notion_link_query = str(request_payload.get("contextualQuestion") or question).strip() or question
                company_notion_docs = select_company_notion_doc_links(
                    notion_link_query,
                    notion_playbooks=notion_playbooks,
                    max_results=3,
                )
                fallback_with_references = _append_company_notion_doc_section(
                    fallback_text,
                    company_notion_docs,
                )
            elif evidence_route == "device_led_pattern_guide":
                request_payload = evidence_payload.get("request") if isinstance(evidence_payload.get("request"), dict) else {}
                notion_link_query = str(request_payload.get("contextualQuestion") or question).strip() or question
                company_notion_docs = select_company_notion_doc_links(
                    notion_link_query,
                    notion_playbooks=notion_playbooks,
                    max_results=3,
                )
                fallback_with_references = _append_company_notion_doc_section(
                    fallback_text,
                    company_notion_docs,
                )
                fallback_with_references = _append_notion_playbook_section(
                    fallback_with_references,
                    notion_playbooks,
                )
            else:
                fallback_with_references = _append_notion_playbook_section(
                    fallback_text,
                    notion_playbooks,
                )
            prefer_fallback_on_timeout = evidence_route in {
                "company_notion_qa",
                "notion_playbook_qa",
            }

            if route_name == "barcode log analysis":
                chunks = _split_barcode_log_reply(fallback_with_references)
                if not chunks:
                    reply(fallback_with_references)
                else:
                    for index, chunk in enumerate(chunks):
                        reply(chunk, mention_user=index == 0)
                logger.info(
                    "Responded with %s (direct, preserve format, chunks=%s)",
                    route_name,
                    max(1, len(chunks)),
                )
                return

            provider = local_answer_provider
            if not s.LLM_SYNTHESIS_ENABLED or not question:
                reply(fallback_with_references)
                logger.info("Responded with %s (direct)", route_name)
                return
            if provider not in {"claude", "ollama"}:
                reply(fallback_with_references)
                logger.info("Responded with %s (direct, unsupported provider=%s)", route_name, provider)
                return
            if provider == "ollama":
                health = _get_ollama_health()
                if not health["ok"]:
                    reply(fallback_with_references)
                    logger.warning(
                        "Responded with %s (direct, ollama unavailable=%s)",
                        route_name,
                        health["summary"],
                    )
                    return
            if provider == "claude":
                if claude_client is None:
                    reply(fallback_with_references)
                    logger.info("Responded with %s (direct, claude client unavailable)", route_name)
                    return
            try:
                thread_context = ""
                # 전사 Work Board 답변에는 기존 마미박스 스레드 문맥을 섞지 않고
                # 조회한 문서 발췌문만 근거로 사용한다.
                if evidence_route != "company_notion_qa" and (
                    evidence_route == "notion_playbook_qa"
                    or s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT
                ):
                    thread_context = _load_slack_thread_context(
                        client,
                        logger,
                        channel_id,
                        thread_ts,
                        current_ts,
                    )
                if evidence_route == "notion_playbook_qa":
                    thread_context = _resolve_notion_doc_thread_context(question, thread_context)
                synthesized_text = _synthesize_retrieval_answer(
                    question=question,
                    thread_context=thread_context,
                    evidence_payload=evidence_payload,
                    provider=provider,
                    claude_client=claude_client,
                    # 전사 문서는 마미박스용으로 커스터마이즈될 수 있는 회사 프롬프트 대신
                    # 공개 코어의 일반 근거 합성 프롬프트를 사용한다.
                    system_prompt=(
                        None
                        if evidence_route == "company_notion_qa"
                        else cs.RETRIEVAL_SYSTEM_PROMPT or None
                    ),
                    extra_rules=_build_company_retrieval_rules(evidence_payload),
                    evidence_transform=_transform_company_retrieval_payload,
                    max_tokens=max_tokens,
                )
                synthesized_text = _normalize_notion_doc_answer_style(synthesized_text, route_name)
                final_text = synthesized_text or fallback_with_references
                if "다른 바코드" in final_text and "다른 바코드" not in fallback_text:
                    final_text = fallback_with_references
                if "다른 barcode" in final_text and "다른 barcode" not in fallback_text:
                    final_text = fallback_with_references
                if _needs_barcode_log_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_with_references
                if _needs_recording_failure_analysis_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_with_references
                if _needs_device_audio_probe_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_with_references
                if _needs_device_led_pattern_fallback(final_text, fallback_text, route_name):
                    final_text = fallback_with_references
                if _needs_notion_doc_fallback(final_text, route_name, fallback_text):
                    final_text = fallback_with_references
                if _needs_notion_doc_security_refusal(final_text, route_name):
                    final_text = _build_notion_doc_security_refusal()
                elif evidence_route == "company_notion_qa":
                    final_text = _append_company_notion_doc_section(final_text, company_notion_docs)
                elif evidence_route == "notion_playbook_qa":
                    final_text = _append_company_notion_doc_section(final_text, company_notion_docs)
                elif evidence_route == "device_led_pattern_guide":
                    final_text = _append_company_notion_doc_section(final_text, company_notion_docs)
                    final_text = _append_notion_playbook_section(final_text, notion_playbooks)
                else:
                    final_text = _append_notion_playbook_section(final_text, notion_playbooks)
                reply(final_text)
                logger.info(
                    "Responded with %s (%s) in thread_ts=%s",
                    route_name,
                    "synthesized" if synthesized_text else "direct_fallback",
                    thread_ts,
                )
            except TimeoutError:
                logger.warning("Retrieval synthesis timeout for route=%s", route_name)
                reply(fallback_with_references if prefer_fallback_on_timeout else _timeout_reply_text())
            except RuntimeError as exc:
                if _is_timeout_error(exc):
                    logger.warning("Retrieval synthesis timeout for route=%s", route_name)
                    reply(fallback_with_references if prefer_fallback_on_timeout else _timeout_reply_text())
                    return
                logger.exception("Retrieval synthesis failed for route=%s", route_name)
                reply(fallback_with_references)
            except Exception:
                logger.exception("Retrieval synthesis failed for route=%s", route_name)
                reply(fallback_with_references)

        # operation 후보 판정은 한 번만 하되 실제 API 호출 위치는 기존
        # Slack handler 순서(pre-Notion -> device -> barcode)를 그대로 따른다.
        # 그래야 `노션에서 ... 복구 방법 찾아줘` 같은 조회를 뒤의 장비
        # mutation matcher가 선점하지 않는다.
        operation_context_entries: tuple[ContextEntry, ...] = ()
        if needs_device_file_operation_context(question):
            # 파일 후속 범위에만 bounded thread context를 쓴다. 진단 snapshot
            # 존재 여부는 아래 typed probe가 API process의 실제 저장소에서
            # 확인하므로 `진단 시작` 문구를 권한이나 존재 근거로 쓰지 않는다.
            operation_context_entries = _get_assistant_context_entries()
        operation_probe_request = build_company_assistant_request(
            payload,
            context_entries=operation_context_entries,
        )
        operation_route = match_company_operation_route(
            operation_probe_request
        )
        operation_execution_context_entries = operation_context_entries
        if (
            operation_route
            in {
                "admin_s3_ultrasound",
                "admin_s3_device_log",
                "admin_readonly_sql",
                "device_audio_probe",
                "device_diagnostic_analysis",
            }
            and _should_load_route_synthesis_context(
                company_api_settings.operations_mode
            )
        ):
            # 기존 합성 route는 Slack thread 최신 window를 LLM 근거로 함께
            # 썼다. matcher에는 주입하지 않고 실제 API 실행에만 전달해
            # `진단 시작` 문구가 snapshot 존재 판정을 대신하지 않게 한다.
            operation_execution_context_entries = (
                _get_assistant_context_entries()
            )

        def _remote_operation_service() -> Any:
            return wrap_company_operations_service(
                CompanyAssistantService(()),
                company_api_settings,
                company_api_client,
                logger,
                match_company_operation_route,
                company_operation_route_names(),
                shadow_runner=company_api_shadow_runner,
            )

        operation_audit_context: dict[str, Any] | None = None

        def _safe_loaded_slack_permalink(
            value: str | None,
        ) -> str | None:
            normalized = str(value or "").strip()
            try:
                parsed = urlsplit(normalized)
            except ValueError:
                return None
            if (
                parsed.scheme != "https"
                or not str(parsed.hostname or "").casefold().endswith(
                    ".slack.com"
                )
                or parsed.username is not None
                or parsed.password is not None
                or not parsed.path.startswith(
                    f"/archives/{channel_id}/p"
                )
                or parsed.fragment
            ):
                return None
            return normalized

        def _get_remote_operation_audit_context() -> dict[str, Any]:
            nonlocal operation_audit_context
            if operation_audit_context is not None:
                return dict(operation_audit_context)

            request_log = payload.get("request_log")
            request_log_context = (
                request_log if isinstance(request_log, dict) else {}
            )
            user_name = str(
                request_log_context.get("user_name") or ""
            ).strip() or _load_slack_user_name(
                client,
                workspace_id,
                str(user_id or "").strip(),
                logger,
            )
            if user_name:
                request_log_context["user_name"] = user_name

            permalink = _safe_loaded_slack_permalink(
                str(request_log_context.get("permalink") or "").strip()
                or _load_slack_permalink(
                    client,
                    channel_id,
                    current_ts,
                    logger,
                )
            )
            if permalink:
                request_log_context["permalink"] = permalink

            thread_permalink: str | None = None
            if thread_ts != current_ts:
                thread_permalink = _safe_loaded_slack_permalink(
                    str(
                        request_log_context.get("thread_permalink") or ""
                    ).strip()
                    or _load_slack_permalink(
                        client,
                        channel_id,
                        thread_ts,
                        logger,
                    )
                )
                if thread_permalink:
                    request_log_context["thread_permalink"] = (
                        thread_permalink
                    )

            operation_audit_context = {
                "event_type": "app_mention",
                "channel_id": channel_id,
                "message_id": current_ts,
                "thread_id": thread_ts,
                "is_thread_root": current_ts == thread_ts,
            }
            if user_name:
                operation_audit_context["user_name"] = user_name
            if permalink:
                operation_audit_context["permalink"] = permalink
            if thread_permalink:
                operation_audit_context["thread_permalink"] = (
                    thread_permalink
                )
            return dict(operation_audit_context)

        def _acknowledge_remote_request_log_delivery(
            operation_request: Any,
            *,
            delivered: bool,
            error_type: str | None = None,
        ) -> None:
            request_log = payload.get("request_log")
            request_log_context = (
                request_log if isinstance(request_log, dict) else {}
            )
            reply_count = max(
                0,
                int(request_log_context.get("reply_count") or 0),
            )
            first_replied_at_utc = (
                request_log_context.get("first_replied_at_utc")
                if reply_count > 0
                else None
            )
            try:
                if company_api_client is None:
                    raise CompanyApiContractError(
                        "company_api_client_disabled",
                        request_id=operation_request.request_id,
                    )
                receipt_result = (
                    company_api_client.acknowledge_request_log_delivery(
                        operation_request,
                        delivered=delivered,
                        reply_count=reply_count,
                        first_replied_at_utc=first_replied_at_utc,
                        error_type=error_type,
                    )
                )
                if (
                    receipt_result.route != "request_log_delivery"
                    or receipt_result.outcome != "answered"
                ):
                    logger.warning(
                        "Company API request-log delivery receipt rejected "
                        "request_id=%s",
                        operation_request.request_id,
                    )
            except Exception as exc:
                # Slack 결과는 이미 확정됐다. 감사 receipt 장애 때문에
                # 메시지를 더 보내거나 local 실행/저장으로 되돌리지 않는다.
                logger.warning(
                    "Company API request-log delivery receipt failed "
                    "request_id=%s error_type=%s",
                    operation_request.request_id,
                    type(exc).__name__,
                )
            _set_request_log_status(
                payload,
                "handled" if delivered else "error",
                error_type=error_type,
            )

        def _handle_remote_diagnostic_probe() -> bool:
            if (
                not question
                or company_api_settings.operations_mode != "remote"
            ):
                return False

            # 같은 Slack event에서 snapshot miss 뒤 explicit live analysis가
            # 이어질 수 있다. stable 파생 ID로 probe guard와 원 operation
            # guard를 분리하면서 redelivery에는 같은 ID를 재사용한다.
            request_id_digest = hashlib.sha256(
                operation_probe_request.request_id.encode("utf-8")
            ).hexdigest()[:32]
            diagnostic_probe_request = replace(
                build_company_assistant_request(
                    payload,
                    context_entries=(
                        _get_assistant_context_entries()
                        if _should_load_route_synthesis_context(
                            company_api_settings.operations_mode
                        )
                        else ()
                    ),
                    metadata={
                        "audit_context": (
                            _get_remote_operation_audit_context()
                        ),
                        "operation_action": {
                            "name": (
                                DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION
                            ),
                        },
                    },
                ),
                request_id=f"diag-probe:{request_id_digest}",
            )
            request_log = payload.get("request_log")
            request_log_context = (
                request_log if isinstance(request_log, dict) else {}
            )
            had_skip_persist = "skip_persist" in request_log_context
            previous_skip_persist = request_log_context.get("skip_persist")
            # API가 answered를 반환하거나 transport가 실패한 remote probe는
            # Slack local request-log로 중복 저장하지 않는다. typed miss만
            # 아래에서 기존 상태를 복원해 다음 route의 legacy 로그를 허용한다.
            _set_request_log_skip_persist(payload, True)
            probe_result = _remote_operation_service().answer(
                diagnostic_probe_request
            )
            if probe_result is None:
                raise CompanyApiContractError(
                    "company_api_diagnostic_probe_result_invalid",
                    request_id=diagnostic_probe_request.request_id,
                )
            if (
                probe_result.route == "device_diagnostic_followup"
                and probe_result.outcome == "no_evidence"
                and probe_result.fallback_reason
                == "diagnostic_snapshot_missing"
            ):
                # API snapshot이 없다는 typed no-match만 기존 knowledge의
                # 다음 route로 넘기고 안내 본문은 Slack에 노출하지 않는다.
                if had_skip_persist:
                    request_log_context["skip_persist"] = bool(
                        previous_skip_persist
                    )
                else:
                    request_log_context.pop("skip_persist", None)
                return False
            _set_request_log_route(
                payload,
                assistant_slack_route_name(probe_result.route),
                handler_type="company_api",
                route_mode="remote",
            )
            _set_request_log_status(payload, probe_result.outcome)
            try:
                render_company_assistant_result(
                    probe_result,
                    reply=reply,
                    actor_id=user_id,
                    client=client,
                    logger=logger,
                )
            except Exception as exc:
                _acknowledge_remote_request_log_delivery(
                    diagnostic_probe_request,
                    delivered=False,
                    error_type=type(exc).__name__,
                )
                raise
            _acknowledge_remote_request_log_delivery(
                diagnostic_probe_request,
                delivered=True,
            )
            return True

        def _handle_remote_operation_stage(
            expected_stage: str,
        ) -> bool:
            if (
                operation_route is None
                or company_api_settings.operations_mode != "remote"
                or company_operation_legacy_stage(
                    operation_probe_request
                )
                != expected_stage
                or (
                    expected_stage == "knowledge"
                    and operation_route == "device_diagnostic_followup"
                )
            ):
                return False

            # remote로 소유권이 확정된 순간부터 Slack local request-log를
            # 닫는다. thread/permalink/actor 보강 자체가 실패해도 민감 원문을
            # local SQLite에 되돌려 쓰지 않는다.
            _set_request_log_skip_persist(payload, True)
            request_context_entries = operation_execution_context_entries
            operation_metadata: dict[str, Any] = {}
            if operation_route == "thread_playbook_learning":
                # 학습은 일반 답변보다 긴 기존 fetch/문맥 한도를 그대로 쓰고,
                # Notion 원문 링크도 기존 local learner와 같은 값으로 전달한다.
                request_context_entries = (
                    _load_thread_context_entries_for_learning(
                        client,
                        logger,
                        channel_id=channel_id,
                        thread_ts=thread_ts,
                        current_ts=current_ts,
                    )
                )
                operation_metadata["thread_permalink"] = (
                    _load_slack_permalink(
                        client,
                        channel_id,
                        thread_ts,
                        logger,
                    )
                )
            if operation_route in {
                "recording_streaming_restore",
                "device_box_update",
                "device_agent_update",
                "device_power_off",
                "device_file_download",
            }:
                # 복원·업데이트·전원·다운로드 이력의 requester 표시는 기존
                # Slack 경로처럼 이름을 우선하고 실패 시 actor ID로 fallback한다.
                operation_metadata["actor_name"] = _load_slack_user_name(
                    client,
                    str(payload.get("workspace_id") or "").strip(),
                    str(user_id or "").strip(),
                    logger,
                )
            if operation_route in {
                DEVICE_FILE_LOOKUP_ROUTE,
                DEVICE_FILE_DOWNLOAD_ROUTE,
                DEVICE_FILE_RECOVERY_ROUTE,
            }:
                # recordings 매핑이 비어 있던 기존 MDA 복구 알림 thread에서는
                # Slack bot/root 검증만 adapter가 하고 exact scope를 API로 넘긴다.
                operation_barcode, operation_log_date = (
                    resolve_device_file_operation_scope(
                        operation_probe_request
                    )
                )
                if operation_barcode and operation_log_date:
                    trusted_contexts = (
                        _lookup_device_file_scope_from_mda_recovery_thread(
                            client=client,
                            logger=logger,
                            channel_id=channel_id,
                            thread_ts=thread_ts,
                            requested_barcode=operation_barcode,
                            requested_date=operation_log_date,
                        )
                    )
                    if len(trusted_contexts) == 1:
                        operation_metadata.update(
                            build_trusted_mda_recovery_scope_metadata(
                                barcode=operation_barcode,
                                log_date=operation_log_date,
                                device_context=trusted_contexts[0],
                            )
                        )
            operation_metadata["audit_context"] = (
                _get_remote_operation_audit_context()
            )
            operation_request = build_company_assistant_request(
                payload,
                context_entries=request_context_entries,
                metadata=operation_metadata,
            )
            operation_service = _remote_operation_service()

            # remote operations의 원문은 API 중앙 저장소만 소유한다.
            _set_request_log_route(
                payload,
                assistant_slack_route_name(operation_route),
                handler_type="company_api",
                route_mode="remote",
            )

            def render_operation_partial(partial_result: Any) -> None:
                try:
                    # 기존 update helper의 dispatch callback 시점 그대로
                    # 완료 poll보다 먼저 비멘션 진행 메시지를 보낸다.
                    render_company_assistant_result(
                        partial_result,
                        reply=reply,
                        actor_id=user_id,
                        client=client,
                        logger=logger,
                    )
                except Exception as exc:
                    # 중간 Slack 발송 실패가 이미 실행 중인 장비 poll을
                    # 중단시키거나 최종 결과·activity receipt를 막지 않는다.
                    logger.warning(
                        "Remote device operation progress delivery failed "
                        "request_id=%s error_type=%s",
                        operation_request.request_id,
                        type(exc).__name__,
                    )

            if operation_route in {
                "device_box_update",
                "device_agent_update",
                "device_power_off",
            }:
                operation_result = operation_service.answer_with_progress(
                    operation_request,
                    render_operation_partial,
                )
            else:
                operation_result = operation_service.answer(
                    operation_request
                )
            if operation_result is not None:
                if (
                    operation_result.route == "device_diagnostic_followup"
                    and operation_result.outcome == "no_evidence"
                    and operation_result.fallback_reason
                    == "diagnostic_snapshot_missing"
                ):
                    # API 재시작 등으로 실제 snapshot이 없으면 기존 Slack처럼
                    # 이 후보를 소비하지 않고 Notion/device/freeform 순서를 잇는다.
                    request_log = payload.get("request_log")
                    if isinstance(request_log, dict):
                        request_log.pop("skip_persist", None)
                    return False
                # PII·SQL·복구·장비 변경 원문은 API 감사 DB가 마스킹해
                # 소유하므로 Slack 로컬 request-log에 중복 저장하지 않는다.
                _set_request_log_route(
                    payload,
                    assistant_slack_route_name(operation_result.route),
                    handler_type="company_api",
                    route_mode="remote",
                )
                _set_request_log_status(payload, operation_result.outcome)
                operation_receipt = operation_result.operation_result
                if (
                    operation_result.route == DEVICE_FILE_DOWNLOAD_ROUTE
                    and isinstance(operation_receipt, dict)
                    and operation_receipt.get("kind")
                    == "device_file_download_delivery"
                ):
                    try:
                        dm_sent = render_device_file_download_delivery(
                            operation_result,
                            reply=reply,
                            actor_id=user_id,
                            client=client,
                            logger=logger,
                        )
                    except Exception as exc:
                        _acknowledge_remote_request_log_delivery(
                            operation_request,
                            delivered=False,
                            error_type=type(exc).__name__,
                        )
                        raise
                    if not dm_sent:
                        # 기존 Slack처럼 DM 실패 시 공개 실패 안내만 남기고
                        # activity를 실행하는 API receipt는 보내지 않는다.
                        _acknowledge_remote_request_log_delivery(
                            operation_request,
                            delivered=True,
                        )
                        return True
                    try:
                        if company_api_client is None:
                            raise CompanyApiContractError(
                                "company_api_client_disabled",
                                request_id=operation_request.request_id,
                            )
                        delivered_result = (
                            company_api_client
                            .acknowledge_device_file_download(
                                operation_request,
                                operation_receipt,
                            )
                        )
                    except Exception as exc:
                        # receipt는 operations transport 정책상 자동 재시도나
                        # Slack-local activity fallback 없이 한 번만 보낸다.
                        logger.warning(
                            "Company API download delivery receipt failed "
                            "request_id=%s error_type=%s",
                            operation_request.request_id,
                            type(exc).__name__,
                        )
                        try:
                            reply(
                                "다운로드 링크는 DM으로 보냈지만 완료 내역을 "
                                "기록하지 못했어. 잠시 후 관리자에게 확인해줘"
                            )
                        except Exception as render_exc:
                            _acknowledge_remote_request_log_delivery(
                                operation_request,
                                delivered=False,
                                error_type=type(render_exc).__name__,
                            )
                            raise
                        _acknowledge_remote_request_log_delivery(
                            operation_request,
                            delivered=True,
                        )
                        return True
                    try:
                        render_company_assistant_result(
                            delivered_result,
                            reply=reply,
                            actor_id=user_id,
                            client=client,
                            logger=logger,
                        )
                    except Exception as exc:
                        _acknowledge_remote_request_log_delivery(
                            operation_request,
                            delivered=False,
                            error_type=type(exc).__name__,
                        )
                        raise
                    _acknowledge_remote_request_log_delivery(
                        operation_request,
                        delivered=True,
                    )
                    return True
                if (
                    operation_result.route
                    in {
                        "device_box_update",
                        "device_agent_update",
                        "device_power_off",
                    }
                    and isinstance(operation_receipt, dict)
                    and operation_receipt.get("kind")
                    == DEVICE_OPERATION_DELIVERY_ACTION
                    and operation_receipt.get("status") == "pending"
                ):
                    # 최종 대화 응답이 성공한 뒤에만 기존 MDA activity를
                    # 복원하는 same-ID receipt를 전송한다.
                    try:
                        sent_count = render_company_assistant_result(
                            operation_result,
                            reply=reply,
                            actor_id=user_id,
                            client=client,
                            logger=logger,
                        )
                    except Exception as exc:
                        _acknowledge_remote_request_log_delivery(
                            operation_request,
                            delivered=False,
                            error_type=type(exc).__name__,
                        )
                        raise
                    if sent_count > 0:
                        try:
                            if company_api_client is None:
                                raise CompanyApiContractError(
                                    "company_api_client_disabled",
                                    request_id=operation_request.request_id,
                                )
                            delivery_ack = (
                                company_api_client
                                .acknowledge_device_operation_delivery(
                                    operation_request,
                                    operation_receipt,
                                )
                            )
                            if (
                                delivery_ack.route
                                != DEVICE_OPERATION_DELIVERY_ACTION
                                or delivery_ack.outcome != "answered"
                            ):
                                logger.warning(
                                    "Company API device operation delivery "
                                    "receipt rejected request_id=%s",
                                    operation_request.request_id,
                                )
                        except Exception as exc:
                            # 최종 Slack 응답은 이미 전달됐다. activity receipt
                            # 실패를 추가 Slack 메시지나 local write로 보상하지 않는다.
                            logger.warning(
                                "Company API device operation delivery "
                                "receipt failed request_id=%s error_type=%s",
                                operation_request.request_id,
                                type(exc).__name__,
                            )
                    _acknowledge_remote_request_log_delivery(
                        operation_request,
                        delivered=True,
                    )
                    return True
                try:
                    render_company_assistant_result(
                        operation_result,
                        reply=reply,
                        actor_id=user_id,
                        client=client,
                        logger=logger,
                    )
                except Exception as exc:
                    _acknowledge_remote_request_log_delivery(
                        operation_request,
                        delivered=False,
                        error_type=type(exc).__name__,
                    )
                    raise
                _acknowledge_remote_request_log_delivery(
                    operation_request,
                    delivered=True,
                )
                return True
            return False

        if _handle_remote_operation_stage("pre_notion"):
            return

        if _handle_thread_learning_routes(
            ThreadLearningRoutesContext(
                question=question,
                payload=payload,
                user_id=user_id,
                workspace_id=str(payload.get("workspace_id") or "").strip(),
                channel_id=channel_id,
                current_ts=current_ts,
                thread_ts=thread_ts,
                reply=reply,
                logger=logger,
                client=client,
                claude_client=claude_client,
            )
        ):
            return

        if _handle_security_review_request(
            SecurityReviewRoutesContext(
                question=question,
                payload=payload,
                user_id=user_id,
                channel_id=channel_id,
                thread_ts=thread_ts,
                reply=reply,
                client=client,
                logger=logger,
                api_client=company_api_client,
                operations_remote=(
                    company_api_settings.operations_mode == "remote"
                ),
            )
        ):
            return

        if _handle_admin_routes(
            AdminRoutesContext(
                question=question,
                payload=payload,
                user_id=user_id,
                thread_ts=thread_ts,
                reply=reply,
                logger=logger,
            ),
            AdminRoutesDeps(
                get_s3_client=_get_s3_client,
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
            ),
        ):
            return

        notion_turn = company_assistant_runtime.start_turn(
            build_company_assistant_request(payload)
        )
        # 첫 HTTP 전환은 순수 read-only 회사 Notion matcher로만 제한한다.
        # 나머지 stage와 Slack 전용 mutation 경로는 기존 순서를 그대로 탄다.
        notion_assistant_service = wrap_company_notion_service(
            notion_turn.service_for_stage("notion"),
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        if _handle_company_notion_routes(
            CompanyNotionRoutesContext(
                question=question,
                user_id=user_id,
                payload=payload,
                thread_ts=thread_ts,
                reply=reply,
                logger=logger,
                client=client,
            ),
            CompanyNotionRoutesDeps(
                assistant_service=notion_assistant_service,
            ),
        ):
            return

        if _handle_remote_operation_stage("device"):
            return

        scope_context_entries = (
            _get_assistant_context_entries()
            if company_assistant_runtime.needs_scope_context(question)
            else ()
        )
        assistant_turn = company_assistant_runtime.start_turn(
            build_company_assistant_request(
                payload,
                context_entries=scope_context_entries,
            )
        )
        barcode = assistant_turn.barcode
        phase2_hospital_name = assistant_turn.hospital_name
        phase2_room_name = assistant_turn.room_name
        thread_context_for_scope = assistant_turn.thread_context
        is_phase2_scope_followup = assistant_turn.is_scope_followup
        is_failure_phase2_scope_followup = bool(
            assistant_turn.is_scope_followup
            and assistant_turn.has_failure_context_hint
        )
        recordings_scope = assistant_turn.recordings

        def _get_recordings_context() -> dict[str, Any]:
            return recordings_scope.get()

        def _attach_recordings_context_to_evidence(
            evidence: dict[str, Any],
            context: dict[str, Any],
        ) -> None:
            recordings_scope.attach_to_evidence(evidence, context)

        def _has_recordings_device_mapping(context: dict[str, Any]) -> bool:
            return recordings_scope.has_device_mapping(context)

        # 바코드 자유질문의 evidence 조립도 runtime knowledge route가 소유한다.
        def _build_barcode_fallback_evidence() -> None:
            return None

        # 장비 stage에서는 S3 LED 조회·가이드만 API 대상이고 실제
        # 상태 점검과 MDA/SSH 작업은 앞선 Slack 전용 handler가 처리한다.
        device_assistant_service = wrap_company_device_service(
            assistant_turn.service_for_stage("device"),
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        if _handle_device_routes(
            DeviceRoutesContext(
                question=question,
                barcode=barcode,
                phase2_hospital_name=phase2_hospital_name,
                phase2_room_name=phase2_room_name,
                payload=payload,
                user_id=user_id,
                workspace_id=workspace_id,
                channel_id=channel_id,
                thread_ts=thread_ts,
                reply=reply,
                client=client,
                logger=logger,
                assistant_service=device_assistant_service,
                context_entries=(
                    _get_assistant_context_entries()
                    if (
                        _is_device_led_pattern_help_request(question)
                        and _should_load_route_synthesis_context(
                            company_api_settings.device_mode
                        )
                    )
                    else ()
                ),
            ),
            DeviceRoutesDeps(
                get_s3_client=_get_s3_client,
                get_recordings_context=_get_recordings_context,
                has_recordings_device_mapping=_has_recordings_device_mapping,
                send_dm_message=_send_dm_message,
                build_dependency_failure_reply=_build_dependency_failure_reply,
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
                automation_remote=(
                    company_api_settings.automation_mode == "remote"
                ),
            ),
        ):
            return

        recording_failure_assistant_service = (
            wrap_company_recording_failure_service(
                assistant_turn.service_for_stage("failure"),
                company_api_settings,
                company_api_client,
                logger,
                shadow_runner=company_api_shadow_runner,
            )
        )
        if _handle_recording_failure_analysis_request(
            RecordingFailureRouteContext(
                question=question,
                barcode=barcode,
                is_failure_phase2_scope_followup=is_failure_phase2_scope_followup,
                phase2_hospital_name=phase2_hospital_name,
                phase2_room_name=phase2_room_name,
                thread_context_for_scope=thread_context_for_scope,
                thread_ts=thread_ts,
                user_id=user_id,
                channel_id=channel_id,
                current_ts=current_ts,
                reply=reply,
                logger=logger,
                client=client,
                payload=payload,
                assistant_service=(
                    recording_failure_assistant_service
                ),
                context_entries=(
                    _get_assistant_context_entries()
                    if (
                        _is_recording_failure_analysis_request(
                            question,
                            barcode,
                        )
                        or is_failure_phase2_scope_followup
                    )
                    else ()
                ),
            ),
            RecordingFailureRouteDeps(
                get_s3_client=_get_s3_client,
                get_recordings_context=_get_recordings_context,
                has_recordings_device_mapping=_has_recordings_device_mapping,
                attach_recordings_context_to_evidence=_attach_recordings_context_to_evidence,
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
                build_dependency_failure_reply=_build_dependency_failure_reply,
            ),
        ):
            return

        barcode_log_assistant_service = wrap_company_barcode_log_service(
            assistant_turn.service_for_stage("log"),
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        if _handle_barcode_log_analysis_request(
            BarcodeLogRouteContext(
                question=question,
                barcode=barcode,
                is_phase2_scope_followup=is_phase2_scope_followup,
                phase2_hospital_name=phase2_hospital_name,
                phase2_room_name=phase2_room_name,
                thread_ts=thread_ts,
                user_id=user_id,
                channel_id=channel_id,
                current_ts=current_ts,
                reply=reply,
                logger=logger,
                claude_client=claude_client,
                client=client,
                payload=payload,
                assistant_service=barcode_log_assistant_service,
                context_entries=(
                    _get_assistant_context_entries()
                    if (
                        is_phase2_scope_followup
                        or (
                            _is_barcode_log_analysis_request(
                                question,
                                barcode,
                            )
                            and _should_load_route_synthesis_context(
                                company_api_settings.barcode_log_mode
                            )
                        )
                    )
                    else ()
                ),
            ),
            BarcodeLogRouteDeps(
                get_s3_client=_get_s3_client,
                get_recordings_context=_get_recordings_context,
                has_recordings_device_mapping=_has_recordings_device_mapping,
                attach_recordings_context_to_evidence=_attach_recordings_context_to_evidence,
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
                build_dependency_failure_reply=_build_dependency_failure_reply,
                is_timeout_error=_is_timeout_error,
                attach_notion_playbooks_to_evidence=_attach_notion_playbooks_to_evidence,
            ),
        ):
            return

        # 기존 네 종류 DB route에 장비 개수·존재와 live-enriched 장비 필터,
        # 사용자 요청형 주간 요약을 독립 rollout으로 더한다. 비-count 장비
        # 조회는 remote에서 공통 API turn 한 번으로 처리하고, 명시적 상태
        # 점검·PM2·복구 같은 operation은 앞선 공통 API gateway가 처리한다.
        structured_assistant_service = wrap_company_structured_service(
            assistant_turn.service_for_stage("structured"),
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        structured_assistant_service = wrap_company_device_filter_service(
            structured_assistant_service,
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        structured_assistant_service = wrap_company_device_db_detail_service(
            structured_assistant_service,
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        structured_assistant_service = wrap_company_weekly_summary_service(
            structured_assistant_service,
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        if _handle_structured_routes(
            StructuredRoutesContext(
                question=question,
                barcode=barcode,
                payload=payload,
                thread_ts=thread_ts,
                reply=reply,
                logger=logger,
                assistant_service=structured_assistant_service,
                client=client,
            )
        ):
            return

        if _handle_remote_operation_stage("barcode"):
            return

        barcode_assistant_service = wrap_company_barcode_service(
            assistant_turn.service_for_stage("barcode"),
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        # 이미 remote인 결정적 5개 route와 분리해 새 DB-only 조회가
        # 배포만으로 즉시 전환되지 않도록 독립 rollout 경계를 둔다.
        barcode_assistant_service = wrap_company_barcode_residual_service(
            barcode_assistant_service,
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        # LLM 합성이 가능한 녹화 시점 조회는 Baby AI와 독립적인
        # local -> shadow -> remote 전환 경계를 사용한다.
        barcode_assistant_service = wrap_company_barcode_timeline_service(
            barcode_assistant_service,
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        if _handle_barcode_query_routes(
            BarcodeQueryRoutesContext(
                question=question,
                barcode=barcode,
                user_id=user_id,
                thread_ts=thread_ts,
                reply=reply,
                logger=logger,
                payload=payload,
                assistant_service=barcode_assistant_service,
                client=client,
                context_entries=(
                    _get_assistant_context_entries()
                    if (
                        (
                            _is_barcode_last_recorded_at_request(
                                question,
                                barcode,
                            )
                            or _is_barcode_video_recorded_on_date_request(
                                question,
                                barcode,
                            )
                        )
                        and _should_load_route_synthesis_context(
                            company_api_settings.barcode_timeline_mode
                        )
                    )
                    else ()
                ),
            ),
            BarcodeQueryRoutesDeps(
                get_recordings_context=_get_recordings_context,
                attach_recordings_context_to_evidence=_attach_recordings_context_to_evidence,
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
                resolve_user_name=lambda target_user_id: _load_slack_user_name(
                    client,
                    workspace_id,
                    target_user_id or "",
                    logger,
                ),
            ),
        ):
            return

        if _handle_remote_diagnostic_probe():
            return

        if _handle_remote_operation_stage("knowledge"):
            return

        knowledge_context_entries = _get_assistant_context_entries()
        knowledge_turn = company_assistant_runtime.start_turn(
            build_company_assistant_request(
                payload,
                context_entries=knowledge_context_entries,
            )
        )
        knowledge_routes = knowledge_turn.routes_for_stage("knowledge")
        playbook_route_index = next(
            (
                index
                for index, route in enumerate(knowledge_routes)
                if route.name == "notion_playbook_qa"
            ),
            len(knowledge_routes),
        )
        knowledge_precedence_service = CompanyAssistantService(
            ()
            if company_api_settings.operations_mode == "remote"
            else knowledge_routes[:playbook_route_index]
        )
        # Slack thread 수집과 최종 렌더링은 adapter에 남기고, 운영
        # 플레이북 조회·근거 합성만 독립 API rollout 경계로 감싼다.
        # operations remote에서는 진단 snapshot도 API 프로세스가 소유하므로
        # Slack-local snapshot/LLM precedence를 비워 로컬 실행 누수를 막는다.
        knowledge_assistant_service = wrap_company_playbook_service(
            knowledge_turn.service_for_stage("knowledge"),
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
            precedence_service=knowledge_precedence_service,
        )
        # 운영 문서와 live 진단 우선순위는 그대로 두고, 현재 질문이
        # recordings 근거 해석을 명시한 경우만 knowledge API로 보낸다.
        knowledge_assistant_service = (
            wrap_company_barcode_freeform_service(
                knowledge_assistant_service,
                company_api_settings,
                company_api_client,
                logger,
                shadow_runner=company_api_shadow_runner,
                precedence_service=knowledge_precedence_service,
            )
        )
        # 모든 전용 knowledge route가 답하지 않았을 때만 일반 대화를
        # freeform stage로 보내고, local mode에서는 기존 Slack LLM 흐름을 유지한다.
        knowledge_assistant_service = wrap_company_freeform_service(
            knowledge_assistant_service,
            company_api_settings,
            company_api_client,
            logger,
            shadow_runner=company_api_shadow_runner,
        )
        if _handle_knowledge_routes(
            KnowledgeRoutesContext(
                question=question,
                barcode=knowledge_turn.barcode,
                user_id=user_id,
                payload=payload,
                thread_ts=thread_ts,
                channel_id=channel_id,
                current_ts=current_ts,
                reply=reply,
                logger=logger,
                client=client,
                claude_client=claude_client,
                assistant_service=knowledge_assistant_service,
                context_entries=knowledge_context_entries,
            ),
            KnowledgeRoutesDeps(
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
                timeout_reply_text=_timeout_reply_text,
                llm_unavailable_reply_text=_llm_unavailable_reply_text,
                is_timeout_error=_is_timeout_error,
                build_barcode_fallback_evidence=_build_barcode_fallback_evidence,
                check_ollama_health=_get_ollama_health,
            ),
        ):
            return

        reply("지원 기능이 궁금하면 `사용법`이라고 보내줘", mention_user=False)

    def _handle_company_message(
        payload: Any,
        reply: Any,
        client: Any,
        logger: logging.Logger,
    ) -> None:
        if _handle_security_review_bot_message(
            SecurityReviewMessageContext(
                payload=payload,
                reply=reply,
                client=client,
                logger=logger,
                api_client=company_api_client,
                operations_remote=(
                    company_api_settings.operations_mode == "remote"
                ),
            )
        ):
            return

        # 무관한 일반 대화에는 조회도 응답도 하지 않고 실제 사람 fun trigger만 검사한다.
        if is_human_fun_trigger(payload) and not base_access_runtime.is_allowed(
            payload.get("workspace_id"),
            payload.get("user_id"),
        ):
            _set_request_log_route(payload, "base_access")
            _set_request_log_status(payload, "denied")
            reply(BASE_ACCESS_DENIED_REPLY, thread=True)
            return

        handle_fun_message(
            payload,
            reply,
            client,
            logger,
            claude_client=claude_client,
            remote_reply_generator=(
                (
                    lambda raw_text, thread_context, speaker_user_id: (
                        _remote_fun_reply(
                            payload,
                            raw_text,
                            thread_context,
                            speaker_user_id,
                        )
                    )
                )
                if company_api_settings.freeform_mode == "remote"
                else None
            ),
            remote_fortune_reply_generator=(
                (
                    lambda raw_text, thread_root_text, speaker_user_id: (
                        _remote_fortune_reply(
                            payload,
                            raw_text,
                            thread_root_text,
                            speaker_user_id,
                        )
                    )
                )
                if company_api_settings.freeform_mode == "remote"
                else None
            ),
        )

    app = create_slack_app(_handle_company_mention, _handle_company_message)
    if hpa_change_api_client is not None:
        attach_hpa_change_remote_reporter(
            app,
            hpa_change_api_client,
            poll_interval_sec=cs.HPA_CHANGE_POLL_INTERVAL_SEC,
            logger=app_logger,
        )
    attach_weekly_recordings_reporter(
        app,
        logger=app_logger,
        automation_client=weekly_automation_client,
    )
    attach_device_health_monitor_reporter(
        app,
        logger=app_logger,
        base_access_checker=base_access_runtime.is_allowed,
        action_api_bridge=device_health_alert_api_bridge,
        automation_client=health_automation_client,
        notification_automation_client=notification_automation_client,
        sms_delivery_automation_client=sms_delivery_automation_client,
    )
    # 실시간 장비 이벤트도 상태 모니터와 같은 번호 판정·공급자·감사 로그 경로를 사용한다.
    attach_device_notification_alert_reporter(
        app,
        logger=app_logger,
        auto_sms_sender=(
            _send_device_health_monitor_auto_sms_for_item
            if notification_automation_client is None
            else None
        ),
        automation_client=notification_automation_client,
    )
    attach_daily_device_round_reporter(
        app,
        logger=app_logger,
        automation_client=daily_automation_client,
    )
    return app
