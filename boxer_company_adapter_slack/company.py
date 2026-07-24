import logging
import time
from typing import Any

import pymysql
from botocore.exceptions import BotoCoreError, ClientError
from slack_bolt import App

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _load_slack_user_name,
    _merge_request_log_metadata,
    _set_request_log_route,
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
    build_company_assistant_request,
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
)
from boxer_company_adapter_slack.fun import handle_fun_message
from boxer_company_adapter_slack.health import (
    _build_dependency_failure_reply,
    _format_ping_llm_status,
)
from boxer_company_adapter_slack.hpa_change_reporter import attach_hpa_change_reporter
from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRoutesContext,
    HpaChangeRoutesDeps,
    _handle_hpa_change_request,
)
from boxer_company_adapter_slack.hpa_change_runtime import create_hpa_change_runtime
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
)
from boxer_company_adapter_slack.daily_device_round_reporter import attach_daily_device_round_reporter
from boxer_company_adapter_slack.device_health_monitor_reporter import (
    _send_device_health_monitor_auto_sms_for_item,
    attach_device_health_monitor_reporter,
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
    _is_company_notion_search_allowed,
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
    CompanyAssistantRuntime,
    CompanyAssistantRuntimeDeps,
    CompanyAssistantRequest,
    CompanyNotionAssistantRouteDeps,
    CompanyReadOnlyKnowledgeRouteDeps,
    build_company_read_only_knowledge_routes,
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


def create_app() -> App:
    _validate_ec2_runtime_aws_env()
    _validate_tokens(include_llm=True, include_data_sources=True)
    app_logger = logging.getLogger(__name__)
    claude_client = None
    if s.LLM_PROVIDER == "claude":
        try:
            claude_client = _build_claude_client(timeout_sec=s.ANTHROPIC_TIMEOUT_SEC)
        except Exception:
            app_logger.warning("Failed to initialize Claude client; continuing without it", exc_info=True)
    hpa_change_runtime = create_hpa_change_runtime(logger=app_logger)
    s3_client: Any | None = None

    def _get_s3_client() -> Any:
        nonlocal s3_client
        if s3_client is None:
            s3_client = _build_s3_client()
        return s3_client

    def _is_claude_allowed_user(target_user_id: str | None) -> bool:
        if not cs.CLAUDE_ALLOWED_USER_IDS:
            return True
        return bool(target_user_id) and target_user_id in cs.CLAUDE_ALLOWED_USER_IDS

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
        provider = (s.LLM_PROVIDER or "").lower().strip()
        if provider == "claude":
            return claude_client is not None
        if provider == "ollama":
            return bool(_get_ollama_health()["ok"])
        return False

    def _answer_timeout_reply_text() -> str:
        provider = (s.LLM_PROVIDER or "").lower().strip()
        if provider == "claude":
            timeout_sec = max(1, s.ANTHROPIC_TIMEOUT_SEC)
            return f"AI API가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"
        timeout_sec = max(1, s.OLLAMA_TIMEOUT_SEC)
        return f"LLM 서버가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"

    company_answer_engine = AnswerEngine(
        provider=s.LLM_PROVIDER,
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
        provider = (s.LLM_PROVIDER or "").lower().strip()
        if (
            not s.LLM_SYNTHESIS_ENABLED
            or provider not in {"claude", "ollama"}
        ):
            return False
        return _is_answer_provider_ready()

    def _build_read_only_knowledge_routes(
        recordings,
        composer,
    ):
        provider = (s.LLM_PROVIDER or "").lower().strip()
        return build_company_read_only_knowledge_routes(
            recordings,
            composer,
            CompanyReadOnlyKnowledgeRouteDeps(
                load_diagnostic_snapshot=(
                    _load_read_only_diagnostic_snapshot
                ),
                # 기존 Slack mention은 내부 채널 정책을 통과한 요청이다.
                notion_is_allowed=lambda request: True,
                barcode_is_allowed=lambda request: (
                    provider != "claude"
                    or _is_claude_allowed_user(request.actor_id)
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
            actor_allowed_for_llm=_is_claude_allowed_user,
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
            timeout_message=_answer_timeout_reply_text(),
            notion_route_deps=CompanyNotionAssistantRouteDeps(
                answer_engine=company_answer_engine,
                synthesis_enabled=s.LLM_SYNTHESIS_ENABLED,
                provider_ready=_is_answer_provider_ready,
                actor_allowed_for_llm=_is_claude_allowed_user,
                looks_like_search=_looks_like_company_notion_search,
                is_search_allowed=_is_company_notion_search_allowed,
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
            hpa_change_runtime.routes_config,
            HpaChangeRoutesDeps(
                submit_request=hpa_change_runtime.submit_request,
                lookup_thread_job=hpa_change_runtime.lookup_thread_job,
            ),
        ):
            return

        if "ping" in text:
            _set_request_log_route(payload, "ping")
            provider = (s.LLM_PROVIDER or "").lower().strip()
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
            provider = (s.LLM_PROVIDER or "").lower().strip()
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
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "claude" and not _is_claude_allowed_user(user_id):
                return False
            return _is_answer_provider_ready()

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

            provider = (s.LLM_PROVIDER or "").lower().strip()
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
                if not _is_claude_allowed_user(user_id):
                    reply(fallback_with_references)
                    logger.info(
                        "Responded with %s (direct, claude synthesis not allowed for user=%s)",
                        route_name,
                        user_id,
                    )
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
                assistant_service=notion_turn.service_for_stage(
                    "notion"
                ),
            ),
        ):
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
                assistant_service=assistant_turn.service_for_stage(
                    "device"
                ),
                context_entries=(
                    _get_assistant_context_entries()
                    if (
                        _is_device_led_pattern_help_request(question)
                        and _should_load_llm_context()
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
            ),
        ):
            return

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
                assistant_service=assistant_turn.service_for_stage(
                    "failure"
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
                assistant_service=assistant_turn.service_for_stage(
                    "log"
                ),
                context_entries=(
                    _get_assistant_context_entries()
                    if (
                        is_phase2_scope_followup
                        or (
                            _is_barcode_log_analysis_request(
                                question,
                                barcode,
                            )
                            and _should_load_llm_context()
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
                is_claude_allowed_user=_is_claude_allowed_user,
                is_timeout_error=_is_timeout_error,
                attach_notion_playbooks_to_evidence=_attach_notion_playbooks_to_evidence,
            ),
        ):
            return

        if _handle_structured_routes(
            StructuredRoutesContext(
                question=question,
                barcode=barcode,
                payload=payload,
                thread_ts=thread_ts,
                reply=reply,
                logger=logger,
                assistant_service=assistant_turn.service_for_stage(
                    "structured"
                ),
                client=client,
            )
        ):
            return

        if _handle_barcode_query_routes(
            BarcodeQueryRoutesContext(
                question=question,
                barcode=barcode,
                user_id=user_id,
                thread_ts=thread_ts,
                reply=reply,
                logger=logger,
                payload=payload,
                assistant_service=assistant_turn.service_for_stage(
                    "barcode"
                ),
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
                        and _should_load_llm_context()
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

        knowledge_context_entries = _get_assistant_context_entries()
        knowledge_turn = company_assistant_runtime.start_turn(
            build_company_assistant_request(
                payload,
                context_entries=knowledge_context_entries,
            )
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
                assistant_service=knowledge_turn.service_for_stage(
                    "knowledge"
                ),
                context_entries=knowledge_context_entries,
            ),
            KnowledgeRoutesDeps(
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
                timeout_reply_text=_timeout_reply_text,
                llm_unavailable_reply_text=_llm_unavailable_reply_text,
                is_timeout_error=_is_timeout_error,
                is_claude_allowed_user=_is_claude_allowed_user,
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
            )
        ):
            return

        handle_fun_message(
            payload,
            reply,
            client,
            logger,
            claude_client=claude_client,
        )

    app = create_slack_app(_handle_company_mention, _handle_company_message)
    attach_hpa_change_reporter(app, hpa_change_runtime, logger=app_logger)
    attach_weekly_recordings_reporter(app, logger=app_logger)
    attach_device_health_monitor_reporter(app, logger=app_logger)
    # 실시간 장비 이벤트도 상태 모니터와 같은 번호 판정·공급자·감사 로그 경로를 사용한다.
    attach_device_notification_alert_reporter(
        app,
        logger=app_logger,
        auto_sms_sender=_send_device_health_monitor_auto_sms_for_item,
    )
    attach_daily_device_round_reporter(app, logger=app_logger)
    return app
