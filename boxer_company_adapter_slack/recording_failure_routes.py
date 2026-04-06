import logging
from dataclasses import dataclass
from typing import Any, Callable

import pymysql
from botocore.exceptions import BotoCoreError, ClientError

from boxer_adapter_slack.common import SlackReplyFn
from boxer_adapter_slack.context import _load_slack_thread_context
from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.routers.barcode_log import (
    _analyze_barcode_log_errors,
    _analyze_barcode_log_phase1_window,
    _build_phase2_scope_request_message,
    _extract_log_date_with_presence,
)
from boxer_company.routers.box_db import _lookup_device_contexts_by_hospital_room
from boxer_company.routers.recording_failure_analysis import (
    _build_recording_failure_analysis_evidence,
    _is_recording_failure_analysis_request,
    _narrow_recording_failure_analysis_evidence,
    _render_recording_failure_analysis_fallback,
)
from boxer_company_adapter_slack.device_activity import _extract_user_only_thread_text
from boxer_company_adapter_slack.weekly_reports import _rewrite_phase2_scope_request_message


@dataclass(frozen=True)
class RecordingFailureRouteContext:
    question: str
    barcode: str | None
    is_failure_phase2_scope_followup: bool
    phase2_hospital_name: str | None
    phase2_room_name: str | None
    thread_context_for_scope: str
    thread_ts: str
    user_id: str | None
    channel_id: str
    current_ts: str
    reply: SlackReplyFn
    logger: logging.Logger
    client: Any


@dataclass(frozen=True)
class RecordingFailureRouteDeps:
    get_s3_client: Callable[[], Any]
    get_recordings_context: Callable[[], dict[str, Any]]
    has_recordings_device_mapping: Callable[[dict[str, Any]], bool]
    attach_recordings_context_to_evidence: Callable[[dict[str, Any], dict[str, Any]], None]
    reply_with_retrieval_synthesis: Callable[..., None]
    build_dependency_failure_reply: Callable[[str, Exception], str]


def _handle_recording_failure_analysis_request(
    context: RecordingFailureRouteContext,
    deps: RecordingFailureRouteDeps,
) -> bool:
    question = context.question
    barcode = context.barcode

    if not (_is_recording_failure_analysis_request(question, barcode) or context.is_failure_phase2_scope_followup):
        return False

    if not s.S3_QUERY_ENABLED:
        context.reply("녹화 실패 원인 분석을 위해 S3_QUERY_ENABLED=true가 필요해")
        return True

    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        context.reply("녹화 실패 원인 분석을 위해 DB 접속 정보(DB_*)가 필요해")
        return True

    try:
        log_date, has_requested_date = _extract_log_date_with_presence(question)
        recordings_context = deps.get_recordings_context()
        summary = recordings_context.get("summary") or {}
        recording_count = int(summary.get("recordingCount") or 0)
        has_device_mapping = deps.has_recordings_device_mapping(recordings_context)
        used_manual_scope = False
        analysis_mode = "phase1_window"
        result_text = ""
        log_analysis_payload: dict[str, Any] | None = None

        if has_requested_date:
            if recording_count <= 0 or not has_device_mapping:
                if not context.phase2_hospital_name or not context.phase2_room_name:
                    context.reply(
                        _build_phase2_scope_request_message(
                            barcode or "",
                            "recordings 장비 매핑이 없어 2차 입력이 필요해",
                            "*녹화 실패 원인 분석*",
                            example_action="녹화 실패 원인 분석",
                        )
                    )
                    context.logger.info(
                        "Responded with recording failure scope guidance in thread_ts=%s barcode=%s mode=scope_required",
                        context.thread_ts,
                        barcode,
                    )
                    return True

                manual_device_contexts = _lookup_device_contexts_by_hospital_room(
                    context.phase2_hospital_name,
                    context.phase2_room_name,
                )
                if not manual_device_contexts:
                    context.reply(
                        _build_phase2_scope_request_message(
                            barcode or "",
                            "입력한 병원명/병실명으로 장비를 찾지 못했어. MDA 표시 이름과 정확히 일치하게 입력해줘",
                            "*녹화 실패 원인 분석*",
                            example_action="녹화 실패 원인 분석",
                        )
                    )
                    context.logger.info(
                        "Responded with recording failure scope guidance in thread_ts=%s barcode=%s mode=scope_not_found",
                        context.thread_ts,
                        barcode,
                    )
                    return True

                used_manual_scope = True
                analysis_mode = "error_manual_scope"
                result_text, log_analysis_payload = _analyze_barcode_log_errors(
                    deps.get_s3_client(),
                    barcode or "",
                    log_date,
                    recordings_context=recordings_context,
                    device_contexts=manual_device_contexts,
                )
            else:
                analysis_mode = "error"
                result_text, log_analysis_payload = _analyze_barcode_log_errors(
                    deps.get_s3_client(),
                    barcode or "",
                    log_date,
                    recordings_context=recordings_context,
                )
        else:
            result_text, log_analysis_payload = _analyze_barcode_log_phase1_window(
                deps.get_s3_client(),
                barcode or "",
                recordings_context=recordings_context,
                max_days=cs.LOG_PHASE1_MAX_DAYS,
            )
            if "• 2차 조회를 위해 아래 3가지를 같이 입력해줘:" in result_text:
                context.reply(
                    _rewrite_phase2_scope_request_message(
                        result_text,
                        "*녹화 실패 원인 분석*",
                        "녹화 실패 원인 분석",
                    )
                )
                context.logger.info(
                    "Responded with recording failure scope guidance in thread_ts=%s barcode=%s mode=phase1_scope_required",
                    context.thread_ts,
                    barcode,
                )
                return True

        failure_evidence = _build_recording_failure_analysis_evidence(
            question=question,
            summary_payload=log_analysis_payload,
        )
        failure_thread_context = context.thread_context_for_scope or _load_slack_thread_context(
            context.client,
            context.logger,
            context.channel_id,
            context.thread_ts,
            context.current_ts,
        )
        failure_user_thread_text = _extract_user_only_thread_text(failure_thread_context, context.user_id)
        selector_text = "\n".join(
            part for part in (failure_user_thread_text, question) if (part or "").strip()
        ).strip()
        failure_evidence, session_scope_message = _narrow_recording_failure_analysis_evidence(
            failure_evidence,
            selector_text,
        )
        if session_scope_message:
            context.reply(session_scope_message)
            context.logger.info(
                "Responded with recording failure session scope guidance in thread_ts=%s barcode=%s",
                context.thread_ts,
                barcode,
            )
            return True
        request_payload = failure_evidence.get("request") if isinstance(failure_evidence, dict) else None
        if isinstance(request_payload, dict):
            request_payload["mode"] = analysis_mode
            request_payload["phase2HospitalName"] = context.phase2_hospital_name
            request_payload["phase2RoomName"] = context.phase2_room_name
            request_payload["usedManualScope"] = used_manual_scope
        deps.attach_recordings_context_to_evidence(failure_evidence, recordings_context)
        fallback_text = _render_recording_failure_analysis_fallback(failure_evidence)
        deps.reply_with_retrieval_synthesis(
            fallback_text,
            failure_evidence,
            route_name="recording failure analysis",
            max_tokens=cs.RECORDING_FAILURE_ANALYSIS_MAX_TOKENS,
        )
    except ValueError as exc:
        context.reply(f"녹화 실패 원인 분석 요청 형식 오류: {exc}")
    except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError) as exc:
        context.logger.exception("Recording failure analysis failed")
        context.reply(deps.build_dependency_failure_reply("녹화 실패 원인 분석", exc))
    except Exception:
        context.logger.exception("Recording failure analysis failed")
        context.reply("녹화 실패 원인 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘")
    return True
