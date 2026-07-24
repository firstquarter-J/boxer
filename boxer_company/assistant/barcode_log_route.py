from __future__ import annotations

import logging
import re
from typing import Any, Callable

import pymysql
from botocore.exceptions import BotoCoreError, ClientError

from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.dependency_errors import (
    build_dependency_failure_message,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.service import (
    RecordingsContextBarcodeMismatch,
    RequestScopedRecordingsContext,
)
from boxer_company.assistant.scope_guard import (
    AssistantRequestScopeMismatch,
    build_scope_mismatch_result,
    resolve_assistant_request_scope,
    window_assistant_context_entries,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.routers.barcode_log import (
    _analyze_barcode_log_errors,
    _analyze_barcode_log_phase1_window,
    _analyze_barcode_log_scan_events,
    _build_phase2_scope_request_message,
    _extract_device_name_scope,
    _extract_hospital_room_scope,
    _extract_log_date_with_presence,
    _is_barcode_log_analysis_request,
    _is_error_focused_request,
    _is_scan_focused_request,
    _is_normal_video_status,
)
from boxer_company.routers.box_db import (
    _lookup_device_contexts_by_barcode_on_date,
    _lookup_device_contexts_by_hospital_room,
)
from boxer_company.routers.recording_failure_analysis import (
    _build_cause_line,
    _classify_record,
    _get_top_error_group,
)
from boxer_company.utils import _extract_barcode


ConfigCheck = Callable[[], bool]
GetS3Client = Callable[[], Any]
_SUMMARY_BAD_PATTERNS = (
    "</think>",
    "<think>",
    "let me",
    "wait,",
    "wait ",
    "i should",
    "the error",
    "the user",
    "now,",
    "now ",
    "therefore",
    "looking at",
    "based on",
    "i need",
    "check if",
)


class PartialResultDeliveryError(RuntimeError):
    """부분 결과 전달 실패를 조회·분석 실패와 구분한다."""


def _default_s3_query_enabled() -> bool:
    return bool(s.S3_QUERY_ENABLED)


def _default_db_configured() -> bool:
    return bool(s.DB_HOST and s.DB_USERNAME and s.DB_PASSWORD and s.DB_DATABASE)


class BarcodeLogAssistantRoute:
    """바코드 기준 DB/S3 로그 분석을 채널 중립 결과로 반환한다."""

    name = "barcode_log_analysis"

    def __init__(
        self,
        recordings: RequestScopedRecordingsContext,
        get_s3_client: GetS3Client,
        composer: CompanyEvidenceAnswerComposer,
        *,
        s3_query_enabled: ConfigCheck = _default_s3_query_enabled,
        db_configured: ConfigCheck = _default_db_configured,
        logger: logging.Logger | None = None,
    ) -> None:
        self._recordings = recordings
        self._get_s3_client = get_s3_client
        self._composer = composer
        self._s3_query_enabled = s3_query_enabled
        self._db_configured = db_configured
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        return self._handle(request)

    def handle_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult | None:
        return self._handle(
            request,
            on_partial_result=on_partial_result,
        )

    def _handle(
        self,
        request: CompanyAssistantRequest,
        *,
        on_partial_result: Callable[
            [CompanyAssistantResult],
            None,
        ]
        | None = None,
    ) -> CompanyAssistantResult | None:
        question = request.question
        try:
            scope = resolve_assistant_request_scope(request)
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        barcode = scope.barcode or self._resolve_barcode(request)
        hospital_name = scope.hospital_name
        room_name = scope.room_name
        has_context_log_request = _context_has_log_request(request)

        # 병원/병실/날짜만 다시 입력한 2차 요청은 이전 정규화 문맥에
        # 로그 요청이 있을 때만 이어서 처리한다.
        try:
            _, has_scope_date = _extract_log_date_with_presence(question)
        except ValueError:
            has_scope_date = _looks_like_scope_followup(
                barcode=barcode,
                hospital_name=hospital_name,
                room_name=room_name,
                has_context_log_request=has_context_log_request,
            )
        is_scope_followup = bool(
            barcode
            and hospital_name
            and room_name
            and has_scope_date
            and has_context_log_request
        )
        if not (
            _is_barcode_log_analysis_request(question, barcode)
            or is_scope_followup
        ):
            return None

        try:
            self._recordings.validate_barcode(barcode)
        except RecordingsContextBarcodeMismatch:
            return build_scope_mismatch_result(
                AssistantRequestScopeMismatch("barcode")
            )

        if not self._s3_query_enabled():
            return _result(
                outcome="failed",
                body=(
                    "로그 분석 기능이 꺼져 있어. "
                    ".env에서 S3_QUERY_ENABLED=true로 설정해줘"
                ),
                fallback_reason="s3_query_disabled",
            )
        if not self._db_configured():
            return _result(
                outcome="failed",
                body=(
                    "바코드 로그 분석을 위해 "
                    "DB 접속 정보(DB_*)가 필요해"
                ),
                fallback_reason="db_not_configured",
            )

        try:
            log_date, has_requested_date = _extract_log_date_with_presence(question)
            recordings_context = self._recordings.get(
                requested_barcode=barcode,
            )
            result_text, analysis_payload, evidence_request = self._analyze(
                question=question,
                barcode=barcode or "",
                log_date=log_date,
                has_requested_date=has_requested_date,
                hospital_name=hospital_name,
                room_name=room_name,
                recordings_context=recordings_context,
            )
            scope_reason = _scope_guidance_reason(result_text)
            if scope_reason is not None:
                return _result(
                    outcome="needs_input",
                    body=_to_commonmark(result_text),
                    fallback_reason=scope_reason,
                )

            evidence_payload: dict[str, Any] = {
                "route": self.name,
                "source": "box_db+s3",
                "request": evidence_request,
                "analysisResult": result_text,
            }
            if analysis_payload is not None:
                evidence_payload["errorSummaryEvidence"] = analysis_payload
            self._recordings.attach_to_evidence(
                evidence_payload,
                recordings_context,
            )

            main_result = CompanyAssistantResult(
                route=self.name,
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body=_to_commonmark(result_text)
                    ),
                ),
            )
            if on_partial_result is not None:
                # 확정된 DB/S3 본문은 세션별 LLM 요약을 기다리지 않고 먼저 전달한다.
                try:
                    on_partial_result(main_result)
                except Exception as exc:
                    raise PartialResultDeliveryError(
                        "부분 결과 전달에 실패했어"
                    ) from exc

            summary_result = self._compose_error_summary(
                request,
                analysis_payload=analysis_payload,
            )
            summary_messages: tuple[AssistantMessage, ...] = ()
            if summary_result is not None:
                summary_messages = tuple(
                    AssistantMessage(
                        body=_to_commonmark(message.body),
                        delivery_scope=message.delivery_scope,
                        mention_actor=False,
                    )
                    for message in summary_result.messages
                    if message.body.strip()
                )

            if on_partial_result is not None:
                return CompanyAssistantResult(
                    route=self.name,
                    outcome="answered",
                    messages=summary_messages,
                    used_llm=bool(
                        summary_result and summary_result.used_llm
                    ),
                )
            return CompanyAssistantResult(
                route=self.name,
                outcome="answered",
                messages=(
                    *main_result.messages,
                    *summary_messages,
                ),
                used_llm=bool(summary_result and summary_result.used_llm),
            )
        except PartialResultDeliveryError:
            # 전송 실패를 조회 형식이나 의존성 오류로 오분류하지 않는다.
            raise
        except ValueError as exc:
            return _result(
                outcome="needs_input",
                body=f"로그 분석 요청 형식 오류: {exc}",
                fallback_reason="invalid_request",
            )
        except (BotoCoreError, ClientError, pymysql.MySQLError, RuntimeError) as exc:
            self._logger.warning(
                "Barcode log assistant dependency failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                outcome="failed",
                body=build_dependency_failure_message(
                    "바코드 로그 분석",
                    exc,
                ),
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            # 예상 밖 분석 오류는 사용자 응답과 분리해 내부 traceback을 남긴다.
            self._logger.exception(
                "Barcode log assistant analysis failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                outcome="failed",
                body=(
                    "바코드 로그 분석 중 오류가 발생했어. "
                    "잠시 후 다시 시도해줘"
                ),
                fallback_reason="analysis_error",
            )

    def _analyze(
        self,
        *,
        question: str,
        barcode: str,
        log_date: str,
        has_requested_date: bool,
        hospital_name: str | None,
        room_name: str | None,
        recordings_context: dict[str, Any],
    ) -> tuple[str, dict[str, Any] | None, dict[str, Any]]:
        summary = recordings_context.get("summary") or {}
        recording_count = int(summary.get("recordingCount") or 0)
        has_device_mapping = self._recordings.has_device_mapping(
            recordings_context
        )
        analysis_mode = "phase1_window"
        used_manual_scope = False
        used_recordings_scope = False

        if not has_requested_date:
            result_text, payload = _analyze_barcode_log_phase1_window(
                self._get_s3_client(),
                barcode,
                recordings_context=recordings_context,
                max_days=cs.LOG_PHASE1_MAX_DAYS,
            )
        else:
            base_mode = (
                "error"
                if _is_error_focused_request(question)
                and not _is_scan_focused_request(question)
                else "scan"
            )
            direct_device_name = _extract_device_name_scope(question)
            direct_device_contexts = (
                [
                    {
                        "deviceName": direct_device_name,
                        "hospitalName": hospital_name,
                        "roomName": room_name,
                    }
                ]
                if direct_device_name
                else []
            )
            auto_device_contexts = None
            if (
                not direct_device_contexts
                and recording_count > 0
                and not has_device_mapping
            ):
                auto_device_contexts = (
                    _lookup_device_contexts_by_barcode_on_date(
                        barcode,
                        log_date,
                    )
                )

            if direct_device_contexts:
                used_manual_scope = True
                analysis_mode = f"{base_mode}_device_scope"
                result_text, payload = self._analyze_for_mode(
                    mode=base_mode,
                    barcode=barcode,
                    log_date=log_date,
                    recordings_context=recordings_context,
                    device_contexts=direct_device_contexts,
                )
            elif recording_count <= 0 or not has_device_mapping:
                if not hospital_name or not room_name:
                    if auto_device_contexts:
                        used_recordings_scope = True
                        analysis_mode = f"{base_mode}_recordings_scope"
                        result_text, payload = self._analyze_for_mode(
                            mode=base_mode,
                            barcode=barcode,
                            log_date=log_date,
                            recordings_context=recordings_context,
                            device_contexts=auto_device_contexts,
                        )
                    else:
                        analysis_mode = "scope_required"
                        result_text = _build_phase2_scope_request_message(
                            barcode,
                            "recordings 장비 매핑이 없어 2차 입력이 필요해",
                            "*로그 분석 결과 (2차 수동 범위)*",
                        )
                        payload = None
                else:
                    manual_device_contexts = (
                        _lookup_device_contexts_by_hospital_room(
                            hospital_name,
                            room_name,
                        )
                    )
                    if not manual_device_contexts:
                        analysis_mode = "scope_not_found"
                        result_text = _build_phase2_scope_request_message(
                            barcode,
                            (
                                "입력한 병원명/병실명으로 장비를 찾지 못했어. "
                                "MDA 표시 이름과 정확히 일치하게 입력해줘"
                            ),
                            "*로그 분석 결과 (2차 수동 범위)*",
                        )
                        payload = None
                    else:
                        used_manual_scope = True
                        analysis_mode = f"{base_mode}_manual_scope"
                        result_text, payload = self._analyze_for_mode(
                            mode=base_mode,
                            barcode=barcode,
                            log_date=log_date,
                            recordings_context=recordings_context,
                            device_contexts=manual_device_contexts,
                        )
            else:
                analysis_mode = base_mode
                result_text, payload = self._analyze_for_mode(
                    mode=base_mode,
                    barcode=barcode,
                    log_date=log_date,
                    recordings_context=recordings_context,
                    device_contexts=None,
                )

        evidence_request = {
            "barcode": barcode,
            "date": log_date,
            "hasRequestedDate": has_requested_date,
            "mode": analysis_mode,
            "phase1MaxDays": int(cs.LOG_PHASE1_MAX_DAYS),
            "recordingsCount": recording_count,
            "recordingsHasDeviceMapping": has_device_mapping,
            "hospitalName": hospital_name,
            "roomName": room_name,
            "usedManualScope": used_manual_scope,
            "usedRecordingsScopeFallback": used_recordings_scope,
        }
        return result_text, payload, evidence_request

    def _analyze_for_mode(
        self,
        *,
        mode: str,
        barcode: str,
        log_date: str,
        recordings_context: dict[str, Any],
        device_contexts: list[dict[str, Any]] | None,
    ) -> tuple[str, dict[str, Any]]:
        analyzer = (
            _analyze_barcode_log_errors
            if mode == "error"
            else _analyze_barcode_log_scan_events
        )
        return analyzer(
            self._get_s3_client(),
            barcode,
            log_date,
            recordings_context=recordings_context,
            device_contexts=device_contexts,
        )

    def _compose_error_summary(
        self,
        request: CompanyAssistantRequest,
        *,
        analysis_payload: dict[str, Any] | None,
    ) -> CompanyAssistantResult | None:
        if not _has_reportable_error_summary(analysis_payload):
            return None
        if not isinstance(analysis_payload, dict):
            return None

        session_entries = _collect_interesting_barcode_log_error_sessions(
            analysis_payload
        )
        rendered_sections: list[str] = []
        used_llm = False
        for session_entry in session_entries:
            session_payload = (
                _build_barcode_log_error_summary_session_payload(
                    analysis_payload,
                    session_entry,
                )
            )
            if not session_payload:
                continue
            fallback_section = "\n".join(
                _build_barcode_log_error_session_section(session_entry)
            ).strip()
            if not fallback_section:
                continue
            fallback_section = _ensure_barcode_log_error_session_heading(
                fallback_section,
                session_entry,
            )
            try:
                # 세션별 검증을 composer policy에도 걸고,
                # 대체 composer가 validator를 무시해도 반환값을 재검증한다.
                composed = self._composer.compose(
                    request,
                    evidence=session_payload,
                    policy=CompanyEvidenceAnswerPolicy(
                        route="barcode_log_error_summary_session",
                        fallback_message=fallback_section,
                        include_context=bool(
                            s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT
                        ),
                        system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                        extra_rules=_build_company_retrieval_rules(
                            session_payload
                        ),
                        evidence_transform=(
                            _transform_company_retrieval_payload
                        ),
                        max_tokens=(
                            cs.BARCODE_LOG_ERROR_SUMMARY_MAX_TOKENS
                        ),
                        answer_validator=(
                            lambda text, payload=session_payload: not (
                                _needs_barcode_log_error_summary_session_fallback(
                                    text,
                                    payload,
                                )
                            )
                        ),
                    ),
                )
                candidate = (
                    composed.messages[0].body
                    if composed.messages
                    else fallback_section
                )
                rejected = (
                    _needs_barcode_log_error_summary_session_fallback(
                        candidate,
                        session_payload,
                    )
                )
                final_section = fallback_section if rejected else candidate
                used_llm = used_llm or bool(
                    composed.used_llm and not rejected
                )
            except Exception as exc:
                # 보조 요약 실패가 이미 끝난 read-only 본문 분석까지
                # 실패로 바꾸지 않도록 해당 세션의 확정 fallback을 쓴다.
                self._logger.exception(
                    "Barcode log session summary failed request_id=%s error_type=%s",
                    request.request_id,
                    type(exc).__name__,
                )
                final_section = fallback_section

            rendered_sections.append(
                _ensure_barcode_log_error_session_heading(
                    final_section,
                    session_entry,
                )
            )

        if not rendered_sections:
            return None
        return CompanyAssistantResult(
            route="barcode_log_error_summary",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=_compose_barcode_log_error_summary_text(
                        _build_barcode_log_error_summary_fallback(
                            analysis_payload
                        ),
                        rendered_sections
                    )
                ),
            ),
            used_llm=used_llm,
        )

    @staticmethod
    def _resolve_barcode(request: CompanyAssistantRequest) -> str | None:
        metadata_barcode = _primitive_scope_text(
            request.metadata.get("barcode")
        )
        if metadata_barcode:
            return metadata_barcode
        direct_barcode = _extract_barcode(request.question)
        if direct_barcode:
            return direct_barcode

        # follow-up에서는 최신 대화부터 확인해
        # 동일 actor가 남긴 이전 요청의 바코드만 복원한다.
        for entry in reversed(window_assistant_context_entries(request)):
            author_id = str(entry.get("author_id") or "").strip()
            if not request.actor_id or author_id != request.actor_id:
                continue
            recovered = _extract_barcode(str(entry.get("text") or ""))
            if recovered:
                return recovered
        return None

    @staticmethod
    def _resolve_manual_scope(
        request: CompanyAssistantRequest,
    ) -> tuple[str | None, str | None]:
        parsed_hospital, parsed_room = _extract_hospital_room_scope(
            request.question
        )
        hospital_name = (
            _primitive_scope_text(request.metadata.get("hospital_name"))
            or _primitive_scope_text(
                request.metadata.get("phase2_hospital_name")
            )
            or parsed_hospital
        )
        room_name = (
            _primitive_scope_text(request.metadata.get("room_name"))
            or _primitive_scope_text(request.metadata.get("phase2_room_name"))
            or parsed_room
        )
        return hospital_name, room_name


def _context_has_log_request(request: CompanyAssistantRequest) -> bool:
    return any(
        (
            "로그" in str(entry.get("text") or "")
            or bool(
                re.search(
                    r"\blog\b",
                    str(entry.get("text") or "").lower(),
                )
            )
        )
        for entry in window_assistant_context_entries(request)
        if (
            request.actor_id
            and str(entry.get("author_id") or "").strip()
            == request.actor_id
        )
    )


def _looks_like_scope_followup(
    *,
    barcode: str | None,
    hospital_name: str | None,
    room_name: str | None,
    has_context_log_request: bool,
) -> bool:
    return bool(
        barcode and hospital_name and room_name and has_context_log_request
    )


def _primitive_scope_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    return normalized or None


def _scope_guidance_reason(result_text: str) -> str | None:
    normalized = result_text or ""
    if "2차 조회를 위해" not in normalized:
        return None
    if "상한" in normalized and "초과" in normalized:
        return "analysis_range_exceeded"
    if "찾지 못했어" in normalized:
        return "scope_not_found"
    return "scope_required"


def _has_reportable_error_summary(
    payload: dict[str, Any] | None,
) -> bool:
    if not isinstance(payload, dict):
        return False
    summary = payload.get("summary")
    if not isinstance(summary, dict):
        return False
    return any(
        _safe_int(summary.get(key)) > 0
        for key in (
            "errorLineCount",
            "abnormalSessionCount",
            "restartEventCount",
        )
    )


def _iter_barcode_log_error_summary_sessions(
    summary_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    request = (
        summary_payload.get("request")
        if isinstance(summary_payload, dict)
        else {}
    )
    records = (
        summary_payload.get("records")
        if isinstance(summary_payload, dict)
        else []
    )
    barcode = str((request or {}).get("barcode") or "미확인").strip()
    barcode = barcode or "미확인"
    session_entries: list[dict[str, Any]] = []
    if not isinstance(records, list):
        return session_entries

    for record in records:
        if not isinstance(record, dict):
            continue
        session_details = record.get("sessionDetails")
        if not isinstance(session_details, list):
            continue
        for detail in session_details:
            if not isinstance(detail, dict):
                continue
            sessions = record.get("sessions")
            sessions = sessions if isinstance(sessions, dict) else {}
            session_entries.append(
                {
                    "barcode": barcode,
                    "deviceName": (
                        str(record.get("deviceName") or "미확인").strip()
                        or "미확인"
                    ),
                    "hospitalName": (
                        str(record.get("hospitalName") or "미확인").strip()
                        or "미확인"
                    ),
                    "roomName": (
                        str(record.get("roomName") or "미확인").strip()
                        or "미확인"
                    ),
                    "date": (
                        str(
                            record.get("date")
                            or (request or {}).get("date")
                            or "미확인"
                        ).strip()
                        or "미확인"
                    ),
                    "recordingsOnDateCount": _safe_int(
                        record.get("recordingsOnDateCount")
                    ),
                    "deviceSessionCount": _safe_int(
                        sessions.get("sessionCount")
                    ),
                    "detail": detail,
                }
            )
    return session_entries


def _is_interesting_barcode_log_error_session(
    session_entry: dict[str, Any],
) -> bool:
    detail = (
        session_entry.get("detail")
        if isinstance(session_entry, dict)
        else {}
    )
    if not isinstance(detail, dict):
        return False
    video_status = str(
        detail.get("videoStatus") or detail.get("recordingResult") or ""
    ).strip()
    return bool(
        detail.get("restartDetected")
        or not bool(detail.get("normalClosed"))
        or _safe_int(detail.get("errorLineCount")) > 0
        or (video_status and not _is_normal_video_status(video_status))
    )


def _build_barcode_log_error_session_record(
    session_entry: dict[str, Any],
) -> dict[str, Any]:
    detail = (
        session_entry.get("detail")
        if isinstance(session_entry, dict)
        else {}
    )
    if not isinstance(detail, dict):
        return {}

    session_recordings_count = _safe_int(
        detail.get("sessionRecordingsCount")
        or session_entry.get("sessionRecordingsCount")
        or session_entry.get("recordingsOnDateCount")
    )
    normal_closed = bool(detail.get("normalClosed"))
    session_diagnostic = (
        detail.get("sessionDiagnostic")
        if isinstance(detail.get("sessionDiagnostic"), dict)
        else {}
    )
    record = {
        "deviceName": session_entry.get("deviceName"),
        "hospitalName": session_entry.get("hospitalName"),
        "roomName": session_entry.get("roomName"),
        "date": session_entry.get("date"),
        "recordingsOnDateCount": session_recordings_count,
        "sessions": {
            "sessionCount": 1,
            "normalCount": 1 if normal_closed else 0,
            "abnormalCount": 0 if normal_closed else 1,
        },
        "restartDetected": bool(detail.get("restartDetected")),
        "errorLineCount": _safe_int(detail.get("errorLineCount")),
        "errorGroups": [
            group
            for group in (detail.get("errorGroups") or [])
            if isinstance(group, dict)
        ],
        "firstFfmpegError": (
            detail.get("firstFfmpegError")
            if isinstance(detail.get("firstFfmpegError"), dict)
            else {}
        ),
        "preRecordingStopDetected": bool(
            detail.get("preRecordingStopDetected")
        ),
        "sessionDiagnostics": (
            [session_diagnostic] if session_diagnostic else []
        ),
    }
    record["classificationTags"] = _classify_record(record)
    return record


def _build_barcode_log_error_session_section(
    session_entry: dict[str, Any],
) -> list[str]:
    detail = (
        session_entry.get("detail")
        if isinstance(session_entry, dict)
        else {}
    )
    if not isinstance(detail, dict):
        return []

    session_index = detail.get("index")
    barcode = str(session_entry.get("barcode") or "미확인").strip()
    barcode = barcode or "미확인"
    hospital_name = str(
        session_entry.get("hospitalName") or "미확인"
    ).strip() or "미확인"
    room_name = str(
        session_entry.get("roomName") or "미확인"
    ).strip() or "미확인"
    date_label = str(
        session_entry.get("date") or "미확인"
    ).strip() or "미확인"
    session_recordings_count = _safe_int(
        detail.get("sessionRecordingsCount")
        or session_entry.get("sessionRecordingsCount")
        or session_entry.get("recordingsOnDateCount")
    )
    start_time = str(
        detail.get("startTime") or "시간미상"
    ).strip() or "시간미상"
    stop_time = str(
        detail.get("stopTime") or "미확인"
    ).strip() or "미확인"
    normal_closed = bool(detail.get("normalClosed"))
    restart_detected = bool(detail.get("restartDetected"))
    termination_status = str(
        detail.get("terminationStatus")
        or ("정상 종료" if normal_closed else "비정상 종료")
    ).strip() or "미확인"
    recording_result = str(
        detail.get("videoStatus")
        or detail.get("recordingResult")
        or "추가 확인 필요"
    ).strip() or "추가 확인 필요"
    session_record = _build_barcode_log_error_session_record(session_entry)
    tags = set(session_record.get("classificationTags") or [])
    error_line_count = _safe_int(session_record.get("errorLineCount"))
    error_groups = (
        session_record.get("errorGroups")
        if isinstance(session_record.get("errorGroups"), list)
        else []
    )
    top_group = _get_top_error_group(session_record)
    top_component = str(
        top_group.get("component") or "미확인"
    ).strip() or "미확인"
    top_signature = str(
        top_group.get("signature") or "미확인"
    ).strip() or "미확인"
    top_count = _safe_int(top_group.get("count"))
    first_ffmpeg_error = (
        session_record.get("firstFfmpegError")
        if isinstance(session_record.get("firstFfmpegError"), dict)
        else {}
    )
    ffmpeg_time = str(first_ffmpeg_error.get("timeLabel") or "").strip()
    session_diagnostic = (
        detail.get("sessionDiagnostic")
        if isinstance(detail.get("sessionDiagnostic"), dict)
        else {}
    )
    diagnostic_severity = str(
        session_diagnostic.get("severity") or ""
    ).strip()
    pre_recording_stop_detected = bool(
        detail.get("preRecordingStopDetected")
    )
    pre_recording_stop_label = (
        str(
            detail.get("preRecordingStopLabel")
            or "모션 감지 단계에서 종료 스캔"
        ).strip()
        or "모션 감지 단계에서 종료 스캔"
    )

    first_ffmpeg_text = " ".join(
        str(first_ffmpeg_error.get(key) or "").strip().lower()
        for key in ("message", "raw")
    )
    is_ffmpeg_error = "ffmpeg_error" in tags
    is_standby_ffmpeg_error = (
        "standby error" in first_ffmpeg_text
        or any(
            "standby error"
            in str(group.get("signature") or "").strip().lower()
            for group in error_groups
            if isinstance(group, dict)
        )
    )
    is_ffmpeg_timestamp_error = "ffmpeg_timestamp_error" in tags
    is_recording_stalled = "recording_stalled" in tags
    all_network_side_effect_errors = "status_network_error" in tags
    router_cause_hint = _build_cause_line(session_record)

    if restart_detected:
        cause_line = (
            "• 핵심 원인: 세션 중 장비 재시작이 확인돼 "
            "정상 녹화 실패로 판단해"
        )
        impact_line = (
            "• 영향: 세션 중 장비 재시작으로 정상 녹화 실패가 "
            "발생한 것으로 봐야 해"
        )
    elif pre_recording_stop_detected:
        if "device_busy" in tags:
            cause_line = (
                f"• 핵심 원인: {pre_recording_stop_label}돼 "
                "녹화 취소로 끝났고, 직전 `/dev/video0` 점유 오류가 "
                "있어 녹화 전환이 막힌 정황이야"
            )
        elif is_ffmpeg_error:
            cause_line = (
                f"• 핵심 원인: {pre_recording_stop_label}돼 "
                "녹화 취소로 끝났고, 세션 초반 ffmpeg 오류로 "
                "본 녹화 전환이 안 된 정황이야"
            )
        else:
            cause_line = (
                f"• 핵심 원인: {pre_recording_stop_label}돼 "
                "녹화 취소로 끝났고 실녹화가 시작되지 않았어"
            )
        impact_line = (
            "• 영향: 종료 스캔은 있었지만 본 녹화 시작 전이라 "
            "정상 녹화 실패로 봐야 해"
        )
    elif not normal_closed:
        cause_line = (
            "• 핵심 원인: 종료 스캔이 없어 세션이 비정상 종료됐어"
        )
        impact_line = (
            "• 영향: 종료 처리가 끝나지 않아 정상 녹화 실패로 봐야 해"
        )
    elif session_recordings_count <= 0 and (
        is_ffmpeg_error
        or is_recording_stalled
        or diagnostic_severity == "high"
    ):
        if is_recording_stalled and is_ffmpeg_error:
            cause_line = (
                "• 핵심 원인: 녹화 중 파일 증가율 저하(stall)와 "
                "ffmpeg 종료가 함께 확인됐고 세션 기준 DB 영상 기록이 "
                "없어 녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 "
                "캡처보드 연결 불량을 우선 의심해"
            )
        elif is_recording_stalled:
            cause_line = (
                "• 핵심 원인: 녹화 중 파일 증가율 저하(stall)가 "
                "반복됐고 세션 기준 DB 영상 기록이 없어 "
                "녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 "
                "캡처보드 연결 불량을 우선 의심해"
            )
        else:
            cause_line = f"• 핵심 원인: {router_cause_hint}"
        impact_line = (
            "• 영향: 세션 기준 DB 영상 기록이 "
            f"`{session_recordings_count}개`라 녹화 파일 저장/업로드가 "
            "실패한 상태야"
        )
    elif (
        all_network_side_effect_errors
        and normal_closed
        and diagnostic_severity != "high"
    ):
        if session_recordings_count > 0:
            cause_line = (
                "• 핵심 원인: JWT 갱신/상태 전송/업로드 통신 오류가 "
                "있었지만 녹화 실패 원인이라기보다 네트워크/DNS 통신 "
                "이상으로 봐야 해"
            )
            impact_line = (
                "• 영향: 세션 기준 DB 영상 기록 "
                f"`{session_recordings_count}개`가 있어 녹화는 성공했고 "
                "통신 오류는 별도야"
            )
        else:
            cause_line = (
                "• 핵심 원인: 업로드/상태 전송 통신 오류가 반복됐고 "
                "세션 기준 DB 영상 기록이 없어 업로드 실패 가능성이 있어"
            )
            impact_line = (
                "• 영향: 녹화 흐름은 종료됐지만 업로드/상태 전송 단계 "
                "실패 가능성이 있어"
            )
    elif diagnostic_severity == "high":
        cause_line = (
            "• 핵심 원인: 종료 처리 지연과 종료 후 장치 오류가 이어져 "
            "실제 영상 손상 가능성이 높아"
        )
        impact_line = (
            f"• 영향: 종료는 됐지만 `{recording_result}` 상태로 봐야 해"
        )
    elif is_standby_ffmpeg_error and normal_closed:
        cause_line = (
            "• 핵심 원인: standby ffmpeg 오류가 확인돼 영상 손상 "
            "가능성을 의심해야 하고 캡처보드 이상을 우선 점검해야 해"
        )
        impact_line = (
            f"• 영향: 종료는 정상이어도 `{recording_result}` 상태로 봐야 해"
        )
    elif is_ffmpeg_timestamp_error:
        cause_line = (
            "• 핵심 원인: ffmpeg DTS/PTS 타임스탬프 이상이 확인돼 "
            "캡처보드 연결 불량 또는 캡처보드 고장을 우선 의심해"
        )
        impact_line = (
            f"• 영향: 종료는 됐지만 `{recording_result}` 상태로 봐야 해"
        )
    elif top_signature != "미확인" and top_count >= 2:
        cause_line = (
            f"• 핵심 원인: `{top_component}` 오류가 반복돼 "
            "원인 점검이 필요해"
        )
        impact_line = (
            f"• 영향: error 라인 `{error_line_count}줄`이 확인됐고 "
            f"`{recording_result}` 상태야"
        )
    elif top_signature != "미확인" and top_count == 1:
        cause_line = (
            f"• 핵심 원인: `{top_component}` 오류가 1회 확인돼 "
            "영향 여부 점검이 필요해"
        )
        impact_line = (
            f"• 영향: 종료 상태는 `{termination_status}`인데 영상 "
            f"상태는 `{recording_result}`야"
        )
    else:
        cause_line = "• 핵심 원인: 운영 근거상 추가 확인이 필요해"
        impact_line = f"• 영향: 현재 판정은 `{recording_result}`이야"

    action_lines: list[str] = []
    if restart_detected:
        action_lines.append("전원 차단/전원 버튼 오입력 여부 확인")
    if pre_recording_stop_detected:
        action_lines.append("종료 스캔 시점과 녹화 취소 안내 여부 확인")
    if (
        is_recording_stalled
        or is_ffmpeg_timestamp_error
        or is_standby_ffmpeg_error
        or is_ffmpeg_error
    ):
        action_lines.append("캡처보드 연결 상태와 입력 신호 점검")
    if is_recording_stalled:
        action_lines.append("저장 경로 쓰기 상태와 파일 증가율 저하 원인 확인")
    if top_signature != "미확인":
        action_lines.append(f"{top_component} 관련 장치/프로세스 상태 확인")
    if not action_lines:
        action_lines.append("동일 시각 장비 상태와 관련 프로세스 로그 확인")

    time_label = (
        f"{start_time} ~ {stop_time}"
        if stop_time != "미확인"
        else start_time
    )
    if ffmpeg_time:
        time_label = f"{time_label} (첫 ffmpeg 오류 {ffmpeg_time})"
    lines = [
        (
            f"• 바코드: `{barcode}` | 병원: `{hospital_name}` | "
            f"병실: `{room_name}` | 날짜: `{date_label}` | "
            f"시간: `{time_label}`"
        ),
        cause_line,
        impact_line,
        f"• 조치: {' / '.join(action_lines[:3])}",
    ]
    normalized_index = _safe_int(session_index)
    if normalized_index > 0:
        return [f"*세션 {normalized_index}*", *lines]
    return lines


def _build_barcode_log_error_summary_session_payload(
    summary_payload: dict[str, Any],
    session_entry: dict[str, Any],
) -> dict[str, Any]:
    request = (
        summary_payload.get("request")
        if isinstance(summary_payload, dict)
        else {}
    )
    detail = (
        session_entry.get("detail")
        if isinstance(session_entry, dict)
        else {}
    )
    if not isinstance(request, dict) or not isinstance(detail, dict):
        return {}

    session_record = _build_barcode_log_error_session_record(session_entry)
    error_groups = (
        session_record.get("errorGroups")
        if isinstance(session_record.get("errorGroups"), list)
        else []
    )
    session_diagnostic = (
        detail.get("sessionDiagnostic")
        if isinstance(detail.get("sessionDiagnostic"), dict)
        else {}
    )
    representative_error_group = _get_top_error_group(session_record)
    time_range = str(
        detail.get("startTime") or "시간미상"
    ).strip() or "시간미상"
    stop_time = str(
        detail.get("stopTime") or "미확인"
    ).strip() or "미확인"
    if stop_time != "미확인":
        time_range = f"{time_range} ~ {stop_time}"

    return {
        "route": "barcode_log_error_summary_session",
        "source": summary_payload.get("source"),
        "request": {
            "mode": request.get("mode"),
            "barcode": request.get("barcode"),
            "date": session_entry.get("date"),
        },
        "session": {
            "barcode": session_entry.get("barcode"),
            "deviceName": session_entry.get("deviceName"),
            "hospitalName": session_entry.get("hospitalName"),
            "roomName": session_entry.get("roomName"),
            "date": session_entry.get("date"),
            "time": time_range,
            "sessionIndex": detail.get("index"),
            "stopToken": detail.get("stopToken"),
            "normalClosed": detail.get("normalClosed"),
            "restartDetected": detail.get("restartDetected"),
            "terminationStatus": detail.get("terminationStatus"),
            "videoStatus": detail.get("videoStatus"),
            "recordingResult": detail.get("recordingResult"),
            "recordingsOnDateCount": session_entry.get(
                "recordingsOnDateCount"
            ),
            "errorLineCount": detail.get("errorLineCount"),
            "firstFfmpegError": detail.get("firstFfmpegError"),
            "preRecordingStopDetected": detail.get(
                "preRecordingStopDetected"
            ),
            "preRecordingStopLabel": detail.get("preRecordingStopLabel"),
            "classificationTags": (
                session_record.get("classificationTags") or []
            ),
            "routerCauseHint": _build_cause_line(session_record),
            "representativeErrorGroup": {
                "component": representative_error_group.get("component"),
                "signature": representative_error_group.get("signature"),
                "count": representative_error_group.get("count"),
                "sampleTime": representative_error_group.get("sampleTime"),
                "sampleMessage": representative_error_group.get(
                    "sampleMessage"
                ),
            },
            "errorGroups": [
                {
                    "component": group.get("component"),
                    "signature": group.get("signature"),
                    "count": group.get("count"),
                    "sampleTime": group.get("sampleTime"),
                    "sampleMessage": group.get("sampleMessage"),
                }
                for group in error_groups[:6]
                if isinstance(group, dict)
            ],
            "sessionDiagnostic": {
                "severity": session_diagnostic.get("severity"),
                "finishDelay": session_diagnostic.get("finishDelay"),
                "postStopScanCount": session_diagnostic.get(
                    "postStopScanCount"
                ),
                "postStopStopCount": session_diagnostic.get(
                    "postStopStopCount"
                ),
                "postStopSnapCount": session_diagnostic.get(
                    "postStopSnapCount"
                ),
                "postStopDeviceErrorCount": session_diagnostic.get(
                    "postStopDeviceErrorCount"
                ),
                "displayText": session_diagnostic.get("displayText"),
            },
        },
    }


def _ensure_barcode_log_error_session_heading(
    section_text: str,
    session_entry: dict[str, Any],
) -> str:
    normalized = str(section_text or "").strip()
    detail = (
        session_entry.get("detail")
        if isinstance(session_entry, dict)
        else {}
    )
    session_index = (
        detail.get("index") if isinstance(detail, dict) else None
    )
    normalized_index = _safe_int(session_index)
    if normalized_index <= 0:
        return normalized

    heading = f"*세션 {normalized_index}*"
    commonmark_heading = f"**세션 {normalized_index}**"
    if normalized.startswith((heading, commonmark_heading)):
        return normalized
    if not normalized:
        return heading
    return f"{heading}\n{normalized}"


def _build_barcode_log_error_summary_fallback(
    summary_payload: dict[str, Any],
) -> str:
    summary = (
        summary_payload.get("summary")
        if isinstance(summary_payload, dict)
        else None
    )
    if not isinstance(summary, dict):
        return ""

    session_entries = _iter_barcode_log_error_summary_sessions(
        summary_payload
    )
    interesting_entries = [
        entry
        for entry in session_entries
        if _is_interesting_barcode_log_error_session(entry)
    ]
    if not interesting_entries:
        interesting_entries = session_entries
    if not interesting_entries:
        return ""

    lines = ["*세션별 에러 분석*"]
    for session_entry in interesting_entries:
        section_lines = _build_barcode_log_error_session_section(
            session_entry
        )
        if not section_lines:
            continue
        section_text = _ensure_barcode_log_error_session_heading(
            "\n".join(section_lines),
            session_entry,
        )
        if not section_text:
            continue
        lines.extend(("", *section_text.splitlines()))
    return "\n".join(lines).strip()


def _is_bad_barcode_log_error_summary_session(text: str) -> bool:
    normalized = (text or "").strip()
    if not normalized:
        return True

    required_markers = (
        "• 바코드:",
        "• 핵심 원인:",
        "• 영향:",
        "• 조치:",
    )
    if any(marker not in normalized for marker in required_markers):
        return True

    lowered = normalized.lower()
    return any(pattern in lowered for pattern in _SUMMARY_BAD_PATTERNS)


def _needs_barcode_log_error_summary_session_fallback(
    synthesized: str,
    session_payload: dict[str, Any],
) -> bool:
    if _is_bad_barcode_log_error_summary_session(synthesized):
        return True

    session = (
        session_payload.get("session")
        if isinstance(session_payload, dict)
        else {}
    )
    if not isinstance(session, dict):
        return False

    tags = {
        str(tag).strip()
        for tag in (session.get("classificationTags") or [])
        if str(tag).strip()
    }
    recordings_on_date_count = _safe_int(
        session.get("recordingsOnDateCount")
    )
    pre_recording_stop_detected = bool(
        session.get("preRecordingStopDetected")
    )
    normalized = (synthesized or "").strip()
    lowered = normalized.lower()

    if pre_recording_stop_detected and not any(
        token in normalized
        for token in (
            "녹화 취소",
            "실녹화",
            "본 녹화 시작 전",
            "모션 감지 단계",
        )
    ):
        return True

    if recordings_on_date_count <= 0 and tags.intersection(
        {"ffmpeg_error", "ffmpeg_sigterm", "recording_stalled"}
    ):
        if "녹화 & 업로드 실패" not in normalized:
            return True
        if not any(
            token in normalized
            for token in (
                "ffmpeg",
                "SIGTERM",
                "sigterm",
                "stall",
                "캡처보드",
                "영상 입력",
            )
        ):
            return True
        if "recording_stalled" in tags and "캡처보드" not in normalized:
            return True

    representative = session.get("representativeErrorGroup")
    if isinstance(representative, dict):
        representative_text = " ".join(
            str(representative.get(key) or "").strip().lower()
            for key in ("component", "signature")
        )
        if any(
            token in representative_text
            for token in (
                "ffmpeg",
                "sigterm",
                "recording may be stalled",
                "stalled",
            )
        ):
            if "app 오류" in normalized and not any(
                token in lowered
                for token in ("ffmpeg", "sigterm", "stall")
            ):
                return True

    return False


def _collect_interesting_barcode_log_error_sessions(
    summary_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    session_entries = _iter_barcode_log_error_summary_sessions(
        summary_payload
    )
    interesting_entries = [
        entry
        for entry in session_entries
        if _is_interesting_barcode_log_error_session(entry)
    ]
    return interesting_entries or session_entries


def _compose_barcode_log_error_summary_text(
    fallback_text: str,
    rendered_sections: list[str],
) -> str:
    if rendered_sections:
        return "*세션별 에러 분석*\n\n" + "\n\n".join(
            rendered_sections
        )
    return fallback_text.strip() or "*세션별 에러 분석*"


def _safe_int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_commonmark(text: str) -> str:
    return slack_mrkdwn_to_commonmark(str(text or ""))


def _result(
    *,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=BarcodeLogAssistantRoute.name,
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = ["BarcodeLogAssistantRoute"]
