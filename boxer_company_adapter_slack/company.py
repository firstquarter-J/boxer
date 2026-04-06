import logging
from typing import Any

import pymysql
from anthropic import Anthropic
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
from boxer_adapter_slack.context import _load_slack_thread_context
from boxer_company_adapter_slack.barcode_logs import (
    _needs_barcode_log_fallback,
    _split_barcode_log_reply,
)
from boxer_company_adapter_slack.barcode_query_routes import (
    BarcodeQueryRoutesContext,
    BarcodeQueryRoutesDeps,
    _handle_barcode_query_routes,
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
from boxer_company_adapter_slack.device_activity import (
    _build_device_download_activity_input,
    _extract_latest_barcode_from_thread_context,
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
from boxer_company_adapter_slack.structured_routes import (
    StructuredRoutesContext,
    _handle_structured_routes,
)
from boxer_company_adapter_slack.weekly_reports import (
    _build_weekly_recordings_report_reply_payload,
    _extract_optional_requested_date,
    _is_weekly_recordings_report_request,
)
from boxer_company_adapter_slack.weekly_recordings_reporter import attach_weekly_recordings_reporter
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.notion_links import select_company_notion_doc_links
from boxer_company.notion_playbooks import _select_notion_references
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company import settings as cs
from boxer_company.utils import _extract_barcode
from boxer.context.builder import _build_model_input
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat, _check_claude_health, _check_ollama_health
from boxer.core.utils import _validate_tokens
from boxer.retrieval.connectors.notion import _is_notion_configured
from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer.retrieval.synthesis import _synthesize_retrieval_answer
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
    _extract_log_date_with_presence,
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
    _has_recording_failure_analysis_hints,
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


def create_app() -> App:
    _validate_tokens(include_llm=True, include_data_sources=True)
    claude_client = (
        Anthropic(
            api_key=s.ANTHROPIC_API_KEY,
            timeout=s.ANTHROPIC_TIMEOUT_SEC,
        )
        if s.LLM_PROVIDER == "claude"
        else None
    )
    s3_client: Any | None = None

    def _get_s3_client() -> Any:
        nonlocal s3_client
        if s3_client is None:
            s3_client = _build_s3_client()
        return s3_client

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

        if "ping" in text:
            _set_request_log_route(payload, "ping")
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "ollama":
                health = _check_ollama_health()
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

        def _is_claude_allowed_user(target_user_id: str | None) -> bool:
            if not cs.CLAUDE_ALLOWED_USER_IDS:
                return True
            return bool(target_user_id) and target_user_id in cs.CLAUDE_ALLOWED_USER_IDS

        def _timeout_reply_text() -> str:
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "claude":
                timeout_sec = max(1, s.ANTHROPIC_TIMEOUT_SEC)
                return f"Claude API가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"
            timeout_sec = max(1, s.OLLAMA_TIMEOUT_SEC)
            return f"LLM 서버가 {timeout_sec}초 내 응답하지 않아 AI 답변 생성이 타임아웃됐어"

        def _llm_unavailable_reply_text(summary: str | None = None) -> str:
            provider = (s.LLM_PROVIDER or "").lower().strip()
            if provider == "claude":
                base = "Claude API가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
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
                client.chat_postMessage(channel=dm_channel, text=message_text)
                return True
            except Exception:
                logger.exception("Failed to send DM to user=%s", target_user_id)
                return False

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
            if evidence_route == "notion_playbook_qa":
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
            else:
                fallback_with_references = _append_notion_playbook_section(
                    fallback_text,
                    notion_playbooks,
                )
            prefer_fallback_on_timeout = evidence_route == "notion_playbook_qa"

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
                health = _check_ollama_health()
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
                if evidence_route == "notion_playbook_qa" or s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
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
                    system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
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
                if _needs_notion_doc_fallback(final_text, route_name, fallback_text):
                    final_text = fallback_with_references
                if _needs_notion_doc_security_refusal(final_text, route_name):
                    final_text = _build_notion_doc_security_refusal()
                elif evidence_route == "notion_playbook_qa":
                    final_text = _append_company_notion_doc_section(final_text, company_notion_docs)
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

        barcode = _extract_barcode(question)
        phase2_hospital_name, phase2_room_name = _extract_hospital_room_scope(question)
        has_phase2_scope = bool(phase2_hospital_name and phase2_room_name)
        phase2_has_requested_date = False
        thread_context_for_scope = ""

        if has_phase2_scope:
            try:
                _, phase2_has_requested_date = _extract_log_date_with_presence(question)
            except ValueError:
                phase2_has_requested_date = True

        if has_phase2_scope and phase2_has_requested_date:
            thread_context_for_scope = _load_slack_thread_context(
                client,
                logger,
                channel_id,
                thread_ts,
                current_ts,
            )

        if not barcode and has_phase2_scope and phase2_has_requested_date:
            recovered_barcode = _extract_latest_barcode_from_thread_context(thread_context_for_scope)
            if recovered_barcode:
                barcode = recovered_barcode
                logger.info(
                    "Recovered barcode from thread context for phase2 scope follow-up in thread_ts=%s barcode=%s",
                    thread_ts,
                    barcode,
                )
        recordings_context: dict[str, Any] | None = None
        recordings_context_prefetch_error: Exception | None = None

        if barcode:
            try:
                recordings_context = _load_recordings_context_by_barcode(barcode)
                prefetch_summary = recordings_context.get("summary") or {}
                logger.info(
                    "Prefetched recordings context in thread_ts=%s barcode=%s count=%s",
                    thread_ts,
                    barcode,
                    int(prefetch_summary.get("recordingCount") or 0),
                )
            except Exception as exc:
                recordings_context_prefetch_error = exc
                logger.warning(
                    "Failed to prefetch recordings context in thread_ts=%s barcode=%s error=%s",
                    thread_ts,
                    barcode,
                    type(exc).__name__,
                )

        def _get_recordings_context() -> dict[str, Any]:
            nonlocal recordings_context, recordings_context_prefetch_error
            if recordings_context is not None:
                return recordings_context
            if recordings_context_prefetch_error is not None:
                raise recordings_context_prefetch_error
            if not barcode:
                raise ValueError("바코드가 필요해")
            recordings_context = _load_recordings_context_by_barcode(barcode)
            return recordings_context

        def _build_recordings_rows_evidence(context: dict[str, Any]) -> list[dict[str, Any]]:
            rows = context.get("rows") or []
            return [
                {
                    "seq": row.get("seq"),
                    "hospitalSeq": row.get("hospitalSeq"),
                    "hospitalRoomSeq": row.get("hospitalRoomSeq"),
                    "hospitalName": row.get("hospitalName"),
                    "roomName": row.get("roomName"),
                    "deviceSeq": row.get("deviceSeq"),
                    "videoLength": row.get("videoLength"),
                    "streamingStatus": row.get("streamingStatus"),
                    "recordedAt": row.get("recordedAt"),
                    "createdAt": row.get("createdAt"),
                }
                for row in rows
            ]

        def _attach_recordings_context_to_evidence(
            evidence: dict[str, Any],
            context: dict[str, Any],
        ) -> None:
            evidence["recordingsSummary"] = context.get("summary")
            evidence["recordingsContextLimit"] = context.get("limit")
            evidence["recordingsHasMore"] = context.get("has_more")
            evidence["recordingsRows"] = _build_recordings_rows_evidence(context)

        def _has_recordings_device_mapping(context: dict[str, Any]) -> bool:
            rows = context.get("rows") or []
            return any(row.get("deviceSeq") is not None for row in rows)

        def _build_barcode_fallback_evidence() -> dict[str, Any] | None:
            if not barcode:
                return None

            evidence: dict[str, Any] = {
                "route": "llm_barcode_fallback",
                "source": "box_db.recordings",
                "request": {
                    "barcode": barcode,
                    "question": question,
                },
            }

            if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
                evidence["warning"] = "DB 접속 정보(DB_*)가 없어 recordings 컨텍스트를 넣지 못했어"
                return evidence

            try:
                context = _get_recordings_context()
            except Exception as exc:
                logger.exception("Failed to load recordings context for llm fallback barcode=%s", barcode)
                evidence["warning"] = f"recordings 컨텍스트 조회 실패: {type(exc).__name__}"
                return evidence

            _attach_recordings_context_to_evidence(evidence, context)
            return evidence

        is_phase2_scope_followup = bool(barcode and has_phase2_scope and phase2_has_requested_date)
        is_failure_phase2_scope_followup = bool(
            barcode
            and has_phase2_scope
            and phase2_has_requested_date
            and _has_recording_failure_analysis_hints(thread_context_for_scope)
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
            ),
            BarcodeQueryRoutesDeps(
                get_recordings_context=_get_recordings_context,
                attach_recordings_context_to_evidence=_attach_recordings_context_to_evidence,
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
            ),
        ):
            return

        if _handle_knowledge_routes(
            KnowledgeRoutesContext(
                question=question,
                barcode=barcode,
                user_id=user_id,
                payload=payload,
                thread_ts=thread_ts,
                channel_id=channel_id,
                current_ts=current_ts,
                reply=reply,
                logger=logger,
                client=client,
                claude_client=claude_client,
            ),
            KnowledgeRoutesDeps(
                reply_with_retrieval_synthesis=_reply_with_retrieval_synthesis,
                timeout_reply_text=_timeout_reply_text,
                llm_unavailable_reply_text=_llm_unavailable_reply_text,
                is_timeout_error=_is_timeout_error,
                is_claude_allowed_user=_is_claude_allowed_user,
                build_barcode_fallback_evidence=_build_barcode_fallback_evidence,
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
        handle_fun_message(
            payload,
            reply,
            client,
            logger,
            claude_client=claude_client,
        )

    app = create_slack_app(_handle_company_mention, _handle_company_message)
    attach_weekly_recordings_reporter(app, logger=logging.getLogger(__name__))
    return app
