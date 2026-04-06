import logging
from dataclasses import dataclass
from typing import Any, Callable

import pymysql
from botocore.exceptions import BotoCoreError, ClientError

from boxer_adapter_slack.common import SlackReplyFn
from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.routers.barcode_log import (
    _analyze_barcode_log_errors,
    _analyze_barcode_log_phase1_window,
    _analyze_barcode_log_scan_events,
    _build_phase2_scope_request_message,
    _extract_log_date_with_presence,
    _is_barcode_log_analysis_request,
    _is_error_focused_request,
    _is_scan_focused_request,
)
from boxer_company.routers.box_db import _lookup_device_contexts_by_hospital_room
from boxer_company_adapter_slack.barcode_logs import _reply_with_barcode_log_error_summary


@dataclass(frozen=True)
class BarcodeLogRouteContext:
    question: str
    barcode: str | None
    is_phase2_scope_followup: bool
    phase2_hospital_name: str | None
    phase2_room_name: str | None
    thread_ts: str
    user_id: str | None
    channel_id: str
    current_ts: str
    reply: SlackReplyFn
    logger: logging.Logger
    claude_client: Any
    client: Any


@dataclass(frozen=True)
class BarcodeLogRouteDeps:
    get_s3_client: Callable[[], Any]
    get_recordings_context: Callable[[], dict[str, Any]]
    has_recordings_device_mapping: Callable[[dict[str, Any]], bool]
    attach_recordings_context_to_evidence: Callable[[dict[str, Any], dict[str, Any]], None]
    reply_with_retrieval_synthesis: Callable[..., None]
    build_dependency_failure_reply: Callable[[str, Exception], str]
    is_claude_allowed_user: Callable[[str | None], bool]
    is_timeout_error: Callable[[Exception], bool]
    attach_notion_playbooks_to_evidence: Callable[[dict[str, Any] | None], list[dict[str, Any]]]


def _handle_barcode_log_analysis_request(
    context: BarcodeLogRouteContext,
    deps: BarcodeLogRouteDeps,
) -> bool:
    question = context.question
    barcode = context.barcode

    if not (_is_barcode_log_analysis_request(question, barcode) or context.is_phase2_scope_followup):
        return False

    if not s.S3_QUERY_ENABLED:
        context.reply("로그 분석 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘")
        return True

    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        context.reply("바코드 로그 분석을 위해 DB 접속 정보(DB_*)가 필요해")
        return True

    try:
        log_date, has_requested_date = _extract_log_date_with_presence(question)
        analysis_mode = "phase1_window"
        recordings_context = deps.get_recordings_context()
        log_analysis_payload: dict[str, Any] | None = None
        summary = recordings_context.get("summary") or {}
        recording_count = int(summary.get("recordingCount") or 0)
        has_device_mapping = deps.has_recordings_device_mapping(recordings_context)
        used_manual_scope = False

        if has_requested_date:
            base_mode = (
                "error"
                if _is_error_focused_request(question) and not _is_scan_focused_request(question)
                else "scan"
            )

            if recording_count <= 0 or not has_device_mapping:
                if not context.phase2_hospital_name or not context.phase2_room_name:
                    analysis_mode = "scope_required"
                    result_text = _build_phase2_scope_request_message(
                        barcode or "",
                        "recordings 장비 매핑이 없어 2차 입력이 필요해",
                        "*로그 분석 결과 (2차 수동 범위)*",
                    )
                else:
                    manual_device_contexts = _lookup_device_contexts_by_hospital_room(
                        context.phase2_hospital_name,
                        context.phase2_room_name,
                    )
                    if not manual_device_contexts:
                        analysis_mode = "scope_not_found"
                        result_text = _build_phase2_scope_request_message(
                            barcode or "",
                            "입력한 병원명/병실명으로 장비를 찾지 못했어. MDA 표시 이름과 정확히 일치하게 입력해줘",
                            "*로그 분석 결과 (2차 수동 범위)*",
                        )
                    else:
                        used_manual_scope = True
                        analysis_mode = f"{base_mode}_manual_scope"
                        if base_mode == "error":
                            result_text, log_analysis_payload = _analyze_barcode_log_errors(
                                deps.get_s3_client(),
                                barcode or "",
                                log_date,
                                recordings_context=recordings_context,
                                device_contexts=manual_device_contexts,
                            )
                        else:
                            result_text, log_analysis_payload = _analyze_barcode_log_scan_events(
                                deps.get_s3_client(),
                                barcode or "",
                                log_date,
                                recordings_context=recordings_context,
                                device_contexts=manual_device_contexts,
                            )
            else:
                analysis_mode = base_mode
                if base_mode == "error":
                    result_text, log_analysis_payload = _analyze_barcode_log_errors(
                        deps.get_s3_client(),
                        barcode or "",
                        log_date,
                        recordings_context=recordings_context,
                    )
                else:
                    result_text, log_analysis_payload = _analyze_barcode_log_scan_events(
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

        if analysis_mode in {"scope_required", "scope_not_found"}:
            context.reply(result_text)
            context.logger.info(
                "Responded with barcode log scope guidance in thread_ts=%s barcode=%s mode=%s",
                context.thread_ts,
                barcode,
                analysis_mode,
            )
            return True

        evidence_payload = {
            "route": "barcode_log_analysis",
            "source": "box_db+s3",
            "request": {
                "barcode": barcode,
                "date": log_date,
                "hasRequestedDate": has_requested_date,
                "mode": analysis_mode,
                "phase1MaxDays": cs.LOG_PHASE1_MAX_DAYS,
                "recordingsCount": recording_count,
                "recordingsHasDeviceMapping": has_device_mapping,
                "phase2HospitalName": context.phase2_hospital_name,
                "phase2RoomName": context.phase2_room_name,
                "usedManualScope": used_manual_scope,
            },
            "analysisResult": result_text,
        }
        if log_analysis_payload is not None:
            evidence_payload["errorSummaryEvidence"] = log_analysis_payload
        deps.attach_recordings_context_to_evidence(evidence_payload, recordings_context)
        deps.reply_with_retrieval_synthesis(
            result_text,
            evidence_payload,
            route_name="barcode log analysis",
        )
        _reply_with_barcode_log_error_summary(
            log_analysis_payload,
            question=question,
            reply=lambda text: context.reply(text, mention_user=False),
            logger=context.logger,
            thread_ts=context.thread_ts,
            user_id=context.user_id,
            claude_client=context.claude_client,
            client=context.client,
            channel_id=context.channel_id,
            current_ts=context.current_ts,
            is_claude_allowed_user=deps.is_claude_allowed_user,
            is_timeout_error=deps.is_timeout_error,
            attach_notion_playbooks_to_evidence=deps.attach_notion_playbooks_to_evidence,
        )
    except ValueError as exc:
        context.reply(f"로그 분석 요청 형식 오류: {exc}")
    except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError) as exc:
        context.logger.exception("Barcode log analysis failed")
        context.reply(deps.build_dependency_failure_reply("바코드 로그 분석", exc))
    except Exception:
        context.logger.exception("Barcode log analysis failed")
        context.reply("바코드 로그 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘")
    return True
