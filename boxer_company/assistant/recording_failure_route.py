from __future__ import annotations

from dataclasses import replace
from datetime import date
import logging
import re
from typing import Any, Callable, Mapping

import pymysql
from botocore.exceptions import BotoCoreError, ClientError

from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
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
    _build_phase2_scope_request_message,
    _extract_device_name_scope,
    _extract_hospital_room_scope,
    _extract_log_date_with_presence,
)
from boxer_company.routers.box_db import (
    _lookup_device_contexts_by_barcode_on_date,
    _lookup_device_contexts_by_hospital_room,
)
from boxer_company.routers.recording_failure_analysis import (
    _build_recording_failure_analysis_evidence,
    _has_recording_failure_analysis_hints,
    _is_recording_failure_analysis_request,
    _narrow_recording_failure_analysis_evidence,
    _render_recording_failure_analysis_fallback,
)
from boxer_company.utils import _extract_barcode

_DEPENDENCY_ERRORS = (
    BotoCoreError,
    ClientError,
    pymysql.MySQLError,
    RuntimeError,
)


class RecordingFailureAssistantRoute:
    name = "recording_failure_analysis"

    def __init__(
        self,
        recordings: RequestScopedRecordingsContext,
        get_s3_client: Callable[[], Any],
        composer: CompanyEvidenceAnswerComposer,
        *,
        s3_query_enabled: bool | None = None,
        db_configured: bool | None = None,
        timeout_message: str = (
            "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
        ),
        logger: logging.Logger | None = None,
    ) -> None:
        self._recordings = recordings
        self._get_s3_client = get_s3_client
        self._composer = composer
        self._s3_query_enabled = (
            bool(s.S3_QUERY_ENABLED)
            if s3_query_enabled is None
            else bool(s3_query_enabled)
        )
        self._db_configured = (
            bool(s.DB_HOST and s.DB_USERNAME and s.DB_PASSWORD and s.DB_DATABASE)
            if db_configured is None
            else bool(db_configured)
        )
        self._timeout_message = timeout_message
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        question = request.question
        context_text = _context_text(request)
        try:
            resolve_assistant_request_scope(request)
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        barcode = _resolve_barcode(request)
        hospital_name, room_name = _resolve_hospital_room(request)
        direct_match = _is_recording_failure_analysis_request(
            question,
            barcode,
        )
        explicit_followup = _metadata_bool(
            request,
            "is_failure_phase2_scope_followup",
            "isFailurePhase2ScopeFollowup",
        )
        contextual_followup = bool(
            barcode
            and hospital_name
            and room_name
            and _has_recording_failure_analysis_hints(context_text)
        )
        if not (direct_match or explicit_followup or contextual_followup):
            return None

        try:
            self._recordings.validate_barcode(barcode)
        except RecordingsContextBarcodeMismatch:
            return build_scope_mismatch_result(
                AssistantRequestScopeMismatch("barcode")
            )

        if not self._s3_query_enabled:
            return _result(
                outcome="failed",
                body="녹화 실패 원인 분석을 위해 S3_QUERY_ENABLED=true가 필요해",
                fallback_reason="s3_not_configured",
            )
        if not self._db_configured:
            return _result(
                outcome="failed",
                body="녹화 실패 원인 분석을 위해 DB 접속 정보(DB_*)가 필요해",
                fallback_reason="db_not_configured",
            )

        try:
            log_date, has_requested_date = _resolve_log_date(request)
        except ValueError as exc:
            return _result(
                outcome="needs_input",
                body=f"녹화 실패 원인 분석 요청 형식 오류: {exc}",
                fallback_reason="invalid_request",
            )

        is_followup = _is_failure_scope_followup(
            request,
            barcode=barcode,
            hospital_name=hospital_name,
            room_name=room_name,
            has_requested_date=has_requested_date,
            context_text=context_text,
        )
        if not (direct_match or is_followup):
            return None

        try:
            return self._analyze(
                request,
                barcode=barcode or "",
                log_date=log_date,
                has_requested_date=has_requested_date,
                hospital_name=hospital_name,
                room_name=room_name,
            )
        except ValueError as exc:
            return _result(
                outcome="needs_input",
                body=f"녹화 실패 원인 분석 요청 형식 오류: {exc}",
                fallback_reason="invalid_request",
            )
        except _DEPENDENCY_ERRORS as exc:
            self._logger.warning(
                "Recording failure assistant dependency failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                outcome="failed",
                body=_build_dependency_failure_message(exc),
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            # 예상 밖 분석 오류는 안전한 응답 뒤에서 내부 traceback으로 추적한다.
            self._logger.exception(
                "Recording failure assistant failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                outcome="failed",
                body="녹화 실패 원인 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="analysis_error",
            )

    def _analyze(
        self,
        request: CompanyAssistantRequest,
        *,
        barcode: str,
        log_date: str,
        has_requested_date: bool,
        hospital_name: str | None,
        room_name: str | None,
    ) -> CompanyAssistantResult:
        recordings_context = self._recordings.get(
            requested_barcode=barcode,
        )
        summary = recordings_context.get("summary") or {}
        recording_count = int(summary.get("recordingCount") or 0)
        has_device_mapping = self._recordings.has_device_mapping(
            recordings_context
        )
        direct_device_name = _resolve_device_name(request)
        used_manual_scope = False
        used_recordings_scope = False
        analysis_mode = "phase1_window"
        result_text = ""
        log_analysis_payload: dict[str, Any] | None = None

        if has_requested_date:
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
            if direct_device_contexts:
                # 지정 장비가 있으면 recordings 매핑이 없는 첫 진료도 해당 장비 로그로 본다.
                used_manual_scope = True
                analysis_mode = "error_device_scope"
                result_text, log_analysis_payload = _analyze_barcode_log_errors(
                    self._get_s3_client(),
                    barcode,
                    log_date,
                    recordings_context=recordings_context,
                    device_contexts=direct_device_contexts,
                )
            elif recording_count <= 0 or not has_device_mapping:
                if not hospital_name or not room_name:
                    auto_device_contexts = (
                        _lookup_device_contexts_by_barcode_on_date(
                            barcode,
                            log_date,
                        )
                        if recording_count > 0
                        else []
                    )
                    if auto_device_contexts:
                        used_recordings_scope = True
                        analysis_mode = "error_recordings_scope"
                        result_text, log_analysis_payload = (
                            _analyze_barcode_log_errors(
                                self._get_s3_client(),
                                barcode,
                                log_date,
                                recordings_context=recordings_context,
                                device_contexts=auto_device_contexts,
                            )
                        )
                    else:
                        return _scope_result(
                            barcode,
                            "recordings 장비 매핑이 없어 2차 입력이 필요해",
                            fallback_reason="scope_required",
                        )
                else:
                    manual_device_contexts = (
                        _lookup_device_contexts_by_hospital_room(
                            hospital_name,
                            room_name,
                        )
                    )
                    if not manual_device_contexts:
                        return _scope_result(
                            barcode,
                            (
                                "입력한 병원명/병실명으로 장비를 찾지 못했어. "
                                "MDA 표시 이름과 정확히 일치하게 입력해줘"
                            ),
                            fallback_reason="scope_not_found",
                        )

                    used_manual_scope = True
                    analysis_mode = "error_manual_scope"
                    result_text, log_analysis_payload = _analyze_barcode_log_errors(
                        self._get_s3_client(),
                        barcode,
                        log_date,
                        recordings_context=recordings_context,
                        device_contexts=manual_device_contexts,
                    )
            else:
                analysis_mode = "error"
                result_text, log_analysis_payload = _analyze_barcode_log_errors(
                    self._get_s3_client(),
                    barcode,
                    log_date,
                    recordings_context=recordings_context,
                )
        else:
            result_text, log_analysis_payload = (
                _analyze_barcode_log_phase1_window(
                    self._get_s3_client(),
                    barcode,
                    recordings_context=recordings_context,
                    max_days=cs.LOG_PHASE1_MAX_DAYS,
                )
            )
            if "• 2차 조회를 위해 아래 3가지를 같이 입력해줘:" in result_text:
                return _result(
                    outcome="needs_input",
                    body=_rewrite_phase2_scope_message(result_text, barcode),
                    fallback_reason="scope_required",
                )

        failure_evidence = _build_recording_failure_analysis_evidence(
            question=request.question,
            summary_payload=log_analysis_payload,
        )
        failure_evidence, session_scope_message = (
            _narrow_recording_failure_analysis_evidence(
                failure_evidence,
                _selector_text(request),
            )
        )
        if session_scope_message:
            return _result(
                outcome="needs_input",
                body=_to_commonmark(session_scope_message),
                fallback_reason="session_selector_required",
            )
        if failure_evidence is None:
            return _result(
                outcome="needs_input",
                body="분석할 녹화 세션을 지정해줘",
                fallback_reason="session_selector_required",
            )

        request_payload = failure_evidence.get("request")
        if isinstance(request_payload, dict):
            request_payload["mode"] = analysis_mode
            request_payload["phase2HospitalName"] = hospital_name
            request_payload["phase2RoomName"] = room_name
            request_payload["usedManualScope"] = used_manual_scope
            request_payload["usedRecordingsScopeFallback"] = (
                used_recordings_scope
            )
        self._recordings.attach_to_evidence(
            failure_evidence,
            recordings_context,
        )
        fallback_text = _to_commonmark(
            _render_recording_failure_analysis_fallback(failure_evidence)
        )
        composed = self._composer.compose(
            request,
            evidence=failure_evidence,
            policy=CompanyEvidenceAnswerPolicy(
                route=self.name,
                fallback_message=fallback_text,
                include_context=bool(
                    s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT
                ),
                timeout_message=self._timeout_message,
                system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                extra_rules=_build_company_retrieval_rules(failure_evidence),
                evidence_transform=_transform_company_retrieval_payload,
                max_tokens=cs.RECORDING_FAILURE_ANALYSIS_MAX_TOKENS,
                answer_validator=_build_recording_failure_answer_validator(
                    fallback_text
                ),
            ),
        )
        return replace(
            composed,
            messages=tuple(
                replace(
                    message,
                    body=_to_commonmark(message.body),
                )
                for message in composed.messages
            ),
        )


def _context_text(request: CompanyAssistantRequest) -> str:
    return "\n".join(
        str(entry.get("text") or "").strip()
        for entry in window_assistant_context_entries(request)
        if (
            isinstance(entry, Mapping)
            and request.actor_id
            and str(entry.get("author_id") or "").strip()
            == request.actor_id
            and str(entry.get("text") or "").strip()
        )
    )


def _metadata_text(
    request: CompanyAssistantRequest,
    *keys: str,
) -> str | None:
    for key in keys:
        value = request.metadata.get(key)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    return None


def _metadata_bool(
    request: CompanyAssistantRequest,
    *keys: str,
) -> bool:
    return any(request.metadata.get(key) is True for key in keys)


def _resolve_barcode(
    request: CompanyAssistantRequest,
) -> str | None:
    explicit = (
        _metadata_text(request, "barcode")
        or _extract_barcode(request.question)
    )
    if explicit:
        return explicit
    # 다른 참여자의 바코드를 현재 요청 범위로 승격하지 않고
    # 동일 actor가 남긴 최신 메시지만 쓴다.
    for entry in reversed(window_assistant_context_entries(request)):
        if not isinstance(entry, Mapping):
            continue
        author_id = str(entry.get("author_id") or "").strip()
        if not request.actor_id or author_id != request.actor_id:
            continue
        barcode = _extract_barcode(str(entry.get("text") or ""))
        if barcode:
            return barcode
    return None


def _resolve_hospital_room(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    question_hospital, question_room = _extract_hospital_room_scope(
        request.question
    )
    hospital_name = (
        _metadata_text(
            request,
            "hospital_name",
            "hospitalName",
            "phase2_hospital_name",
            "phase2HospitalName",
        )
        or question_hospital
    )
    room_name = (
        _metadata_text(
            request,
            "room_name",
            "roomName",
            "phase2_room_name",
            "phase2RoomName",
        )
        or question_room
    )
    return hospital_name, room_name


def _resolve_device_name(
    request: CompanyAssistantRequest,
) -> str | None:
    return _metadata_text(
        request,
        "device_name",
        "deviceName",
    ) or _extract_device_name_scope(request.question)


def _resolve_log_date(
    request: CompanyAssistantRequest,
) -> tuple[str, bool]:
    log_date, has_requested_date = _extract_log_date_with_presence(
        request.question
    )
    metadata_date = _metadata_text(request, "log_date", "logDate")
    if has_requested_date or not metadata_date:
        return log_date, has_requested_date

    try:
        return date.fromisoformat(metadata_date).isoformat(), True
    except ValueError as exc:
        raise ValueError("날짜는 YYYY-MM-DD 형식으로 입력해줘") from exc


def _is_failure_scope_followup(
    request: CompanyAssistantRequest,
    *,
    barcode: str | None,
    hospital_name: str | None,
    room_name: str | None,
    has_requested_date: bool,
    context_text: str,
) -> bool:
    explicit_followup = _metadata_bool(
        request,
        "is_failure_phase2_scope_followup",
        "isFailurePhase2ScopeFollowup",
    )
    return bool(
        barcode
        and hospital_name
        and room_name
        and has_requested_date
        and (
            explicit_followup
            or _has_recording_failure_analysis_hints(context_text)
        )
    )


def _selector_text(request: CompanyAssistantRequest) -> str:
    actor_context: list[str] = []
    for entry in window_assistant_context_entries(request):
        if not isinstance(entry, Mapping):
            continue
        author_id = str(entry.get("author_id") or "").strip()
        # actor가 없거나 작성자가 불명확하면 다른 참여자의 세션 지시를
        # 요청자 선택으로 오인하지 않도록 문맥 selector에서 제외한다.
        if not request.actor_id or author_id != request.actor_id:
            continue
        text = str(entry.get("text") or "").strip()
        if text:
            actor_context.append(text)

    metadata_selector = _metadata_text(
        request,
        "selector",
        "selector_text",
        "selectorText",
    )
    return "\n".join(
        part
        for part in (
            *actor_context,
            metadata_selector or "",
            request.question,
        )
        if part.strip()
    ).strip()


def _scope_result(
    barcode: str,
    reason: str,
    *,
    fallback_reason: str,
) -> CompanyAssistantResult:
    message = _build_phase2_scope_request_message(
        barcode,
        reason,
        "*녹화 실패 원인 분석*",
        example_action="녹화 실패 원인 분석",
    )
    return _result(
        outcome="needs_input",
        body=_to_commonmark(message),
        fallback_reason=fallback_reason,
    )


def _rewrite_phase2_scope_message(
    result_text: str,
    fallback_barcode: str,
) -> str:
    barcode_match = re.search(r"• 바코드: `([^`]+)`", result_text or "")
    reason_match = re.search(r"• 사유: (.+)", result_text or "")
    return _to_commonmark(
        _build_phase2_scope_request_message(
            (
                barcode_match.group(1).strip()
                if barcode_match
                else fallback_barcode
            ),
            (
                reason_match.group(1).strip()
                if reason_match
                else "2차 입력이 필요해"
            ),
            "*녹화 실패 원인 분석*",
            example_action="녹화 실패 원인 분석",
        )
    )


def _build_recording_failure_answer_validator(
    fallback_text: str,
) -> Callable[[str], bool]:
    normalized_fallback = (fallback_text or "").strip()
    required_bullets = tuple(
        bullet
        for bullet in (
            "• 핵심 원인:",
            "• 운영 근거:",
            "• 영향:",
            "• 권장 조치:",
            "• 확실도:",
        )
        if bullet in normalized_fallback
    )
    requires_title = normalized_fallback.startswith(
        ("*녹화 실패 원인 분석*", "**녹화 실패 원인 분석**")
    )
    requires_capture_board = "캡처보드" in normalized_fallback
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

    def is_valid(answer_text: str) -> bool:
        # 기존 Slack 경로의 형식 검증을 순수 predicate로 뒤집어 composer 정책에 전달한다.
        normalized_answer = (answer_text or "").strip()
        if requires_title and not normalized_answer.startswith(
            ("*녹화 실패 원인 분석*", "**녹화 실패 원인 분석**")
        ):
            return False
        lowered = normalized_answer.lower()
        if any(token in lowered for token in reasoning_leak_tokens):
            return False
        if any(bullet not in normalized_answer for bullet in required_bullets):
            return False
        if requires_capture_board and "캡처보드" not in normalized_answer:
            return False
        return True

    return is_valid


def _build_dependency_failure_message(exc: Exception) -> str:
    base = "녹화 실패 원인 분석 중 오류가 발생했어."
    if isinstance(exc, pymysql.MySQLError):
        return f"{base} DB 연결 또는 조회에 실패했어"
    if isinstance(exc, ClientError):
        code = str(exc.response.get("Error", {}).get("Code", "")).strip()
        if code in {
            "403",
            "AccessDenied",
            "InvalidAccessKeyId",
            "SignatureDoesNotMatch",
        }:
            return f"{base} S3 접근 권한을 확인해줘"
        return f"{base} S3 로그 접근에 실패했어"
    if isinstance(exc, BotoCoreError):
        return f"{base} S3 로그 접근에 실패했어"
    if isinstance(exc, RuntimeError):
        lowered = str(exc).lower()
        if any(token in lowered for token in ("db", "mysql", "read-only")):
            return f"{base} DB 연결 또는 조회에 실패했어"
        if any(token in lowered for token in ("s3", "bucket", "credential")):
            return f"{base} S3 로그 접근에 실패했어"
    return f"{base} 잠시 후 다시 시도해줘"


def _to_commonmark(text: str) -> str:
    return slack_mrkdwn_to_commonmark(text)


def _result(
    *,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route="recording_failure_analysis",
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "RecordingFailureAssistantRoute",
]
