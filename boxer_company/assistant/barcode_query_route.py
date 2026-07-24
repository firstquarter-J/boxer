from __future__ import annotations

import logging
from typing import Callable

import pymysql

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
)
from boxer_company.routers.barcode_log import (
    _extract_log_date,
    _extract_log_date_with_presence,
    _is_baby_ai_list_request_without_barcode,
    _is_barcode_all_recorded_dates_request,
    _is_barcode_baby_ai_list_request,
    _is_barcode_last_recorded_at_request,
    _is_barcode_video_count_request,
    _is_barcode_video_info_request,
    _is_barcode_video_length_request,
    _is_barcode_video_list_request,
    _is_barcode_video_recorded_on_date_request,
)
from boxer_company.routers.barcode_validation import (
    _is_barcode_pink_classification_reason_request,
    _is_barcode_validation_status_request,
    _query_barcode_pink_classification_reason,
    _query_barcode_validation_status,
)
from boxer_company.routers.box_db import (
    _query_all_recorded_dates_by_barcode,
    _query_baby_ai_list_by_barcode,
    _query_last_recorded_at_by_barcode,
    _query_recordings_count_by_barcode,
    _query_recordings_detail_by_barcode,
    _query_recordings_length_by_barcode,
    _query_recordings_length_on_date_by_barcode,
    _query_recordings_list_by_barcode,
    _query_recordings_on_date_by_barcode,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.routers.recording_streaming_restore import (
    _is_recording_streaming_restore_request,
)
class BarcodeQueryAssistantRoute:
    name = "barcode_query"

    def __init__(
        self,
        recordings: RequestScopedRecordingsContext,
        *,
        answer_composer: CompanyEvidenceAnswerComposer | None = None,
        timeout_message: str = "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘",
        logger: logging.Logger | None = None,
    ) -> None:
        self._recordings = recordings
        self._answer_composer = answer_composer
        self._timeout_message = timeout_message
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        question = request.question
        try:
            barcode = resolve_assistant_request_scope(request).barcode
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        try:
            self._recordings.validate_barcode(barcode)
        except RecordingsContextBarcodeMismatch:
            return build_scope_mismatch_result(
                AssistantRequestScopeMismatch("barcode")
            )

        if _is_barcode_pink_classification_reason_request(question, barcode):
            return self._run_query(
                route="barcode_pink_classification_reason",
                query=lambda: _query_barcode_pink_classification_reason(barcode or ""),
                format_error_prefix="핑크 바코드 분류 근거 확인 요청 형식 오류",
                dependency_error="핑크 바코드 분류 근거 확인 중 오류가 발생했어. DB/MDA 연결 상태를 확인해줘",
                retry_error="핑크 바코드 분류 근거 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_barcode_validation_status_request(question, barcode):
            return self._run_query(
                route="barcode_validation_status",
                query=lambda: _query_barcode_validation_status(barcode or ""),
                format_error_prefix="바코드 유효성 검사 확인 요청 형식 오류",
                dependency_error="바코드 유효성 검사 확인 중 오류가 발생했어. MDA 연결 상태를 확인해줘",
                retry_error="바코드 유효성 검사 확인 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_recording_streaming_restore_request(question, barcode):
            # 복원은 7단계 action API 대상이므로 기존 정책 가드와 Slack 경로에 남긴다.
            return None

        if _is_barcode_video_count_request(question, barcode):
            return self._run_query(
                route="barcode_video_count",
                query=lambda: _query_recordings_count_by_barcode(
                    barcode or "",
                    recordings_context=self._recordings.get(),
                ),
                format_error_prefix="영상 개수 조회 요청 형식 오류",
                dependency_error="영상 개수 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="영상 개수 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_baby_ai_list_request_without_barcode(question, barcode):
            return _result(
                route="baby_ai_list",
                outcome="needs_input",
                body="베이비매직 조회는 바코드가 필요해. 예: `12345678910 베이비매직 목록`",
                fallback_reason="missing_barcode",
            )

        if _is_barcode_baby_ai_list_request(question, barcode):
            def query_baby_ai() -> str:
                target_date, has_requested_date = _extract_log_date_with_presence(
                    question
                )
                return _query_baby_ai_list_by_barcode(
                    barcode or "",
                    target_date if has_requested_date else None,
                )

            return self._run_query(
                route="barcode_baby_ai_list",
                query=query_baby_ai,
                format_error_prefix="베이비매직 목록 조회 요청 형식 오류",
                dependency_error="베이비매직 목록 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="베이비매직 목록 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_barcode_video_info_request(question, barcode):
            return self._run_recordings_query(
                route="barcode_video_info",
                query=lambda context: _query_recordings_detail_by_barcode(
                    barcode or "",
                    recordings_context=context,
                ),
                dependency_error="영상 정보 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="영상 정보 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_barcode_video_list_request(question, barcode):
            return self._run_recordings_query(
                route="barcode_video_list",
                query=lambda context: _query_recordings_list_by_barcode(
                    barcode or "",
                    recordings_context=context,
                ),
                dependency_error="영상 목록 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="영상 목록 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_barcode_video_length_request(question, barcode):
            def query_length() -> str:
                context = self._recordings.get()
                target_date, has_requested_date = _extract_log_date_with_presence(
                    question
                )
                if has_requested_date:
                    return _query_recordings_length_on_date_by_barcode(
                        barcode or "",
                        target_date,
                        recordings_context=context,
                    )
                return _query_recordings_length_by_barcode(
                    barcode or "",
                    recordings_context=context,
                )

            return self._run_query(
                route="barcode_video_length",
                query=query_length,
                format_error_prefix="영상 길이 조회 요청 형식 오류",
                dependency_error="영상 길이 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="영상 길이 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_barcode_all_recorded_dates_request(question, barcode):
            return self._run_recordings_query(
                route="barcode_all_recorded_dates",
                query=lambda context: _query_all_recorded_dates_by_barcode(
                    barcode or "",
                    recordings_context=context,
                ),
                dependency_error="전체 녹화 날짜 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="전체 녹화 날짜 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_barcode_last_recorded_at_request(question, barcode):
            return self._run_composed_recordings_query(
                request,
                route="barcode last recordedAt",
                evidence_route="barcode_last_recorded_at",
                query=lambda context: _query_last_recorded_at_by_barcode(
                    barcode or "",
                    recordings_context=context,
                ),
                request_evidence={
                    "barcode": barcode,
                    "question": question,
                },
                format_error_prefix="마지막 녹화 날짜 조회 요청 형식 오류",
                dependency_error="마지막 녹화 날짜 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="마지막 녹화 날짜 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
            )

        if _is_barcode_video_recorded_on_date_request(question, barcode):
            try:
                target_date = _extract_log_date(question)
            except ValueError as exc:
                return _result(
                    route="barcode recordedAt-on-date",
                    outcome="needs_input",
                    body=f"영상 날짜 조회 요청 형식 오류: {exc}",
                    fallback_reason="invalid_request",
                )
            return self._run_composed_recordings_query(
                request,
                route="barcode recordedAt-on-date",
                evidence_route="barcode_recorded_on_date",
                query=lambda context: _query_recordings_on_date_by_barcode(
                    barcode or "",
                    target_date,
                    recordings_context=context,
                ),
                request_evidence={
                    "barcode": barcode,
                    "question": question,
                    "targetDate": target_date,
                },
                format_error_prefix="영상 날짜 조회 요청 형식 오류",
                dependency_error="날짜별 녹화 여부 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="날짜별 녹화 여부 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
            )
        return None

    def _run_composed_recordings_query(
        self,
        request: CompanyAssistantRequest,
        *,
        route: str,
        evidence_route: str,
        query: Callable[[dict], str],
        request_evidence: dict,
        format_error_prefix: str,
        dependency_error: str,
        retry_error: str,
    ) -> CompanyAssistantResult | None:
        if self._answer_composer is None:
            # 전환 중인 adapter는 composer가 없는 경우 기존 handler로 안전하게 내려간다.
            return None
        try:
            recordings_context = self._recordings.get()
            fallback_text = _to_commonmark(query(recordings_context))
            evidence_payload = {
                "route": evidence_route,
                "source": "box_db.recordings",
                "request": request_evidence,
                "queryResult": fallback_text,
            }
            self._recordings.attach_to_evidence(
                evidence_payload,
                recordings_context,
            )
            return self._answer_composer.compose(
                request,
                evidence=evidence_payload,
                policy=CompanyEvidenceAnswerPolicy(
                    route=route,
                    fallback_message=fallback_text,
                    fallback_outcome="answered",
                    fallback_on_timeout=False,
                    timeout_message=self._timeout_message,
                    include_context=True,
                    system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                    extra_rules=_build_company_retrieval_rules(
                        evidence_payload
                    ),
                    evidence_transform=_transform_company_retrieval_payload,
                    answer_validator=_is_safe_barcode_answer,
                ),
            )
        except ValueError as exc:
            return _result(
                route=route,
                outcome="needs_input",
                body=f"{format_error_prefix}: {exc}",
                fallback_reason="invalid_request",
            )
        except (pymysql.MySQLError, RuntimeError) as exc:
            self._logger.warning(
                "Barcode assistant dependency failed route=%s request_id=%s error_type=%s",
                route,
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=route,
                outcome="failed",
                body=dependency_error,
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            # 예상 밖 query 오류는 사용자 응답과 분리해 내부 traceback을 남긴다.
            self._logger.exception(
                "Barcode assistant query failed route=%s request_id=%s error_type=%s",
                route,
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=route,
                outcome="failed",
                body=retry_error,
                fallback_reason="query_error",
            )

    def _run_recordings_query(
        self,
        *,
        route: str,
        query: Callable[[dict], str],
        dependency_error: str,
        retry_error: str,
        request_id: str,
    ) -> CompanyAssistantResult:
        return self._run_query(
            route=route,
            query=lambda: query(self._recordings.get()),
            format_error_prefix="바코드 조회 요청 형식 오류",
            dependency_error=dependency_error,
            retry_error=retry_error,
            request_id=request_id,
        )

    def _run_query(
        self,
        *,
        route: str,
        query: Callable[[], str],
        format_error_prefix: str,
        dependency_error: str,
        retry_error: str,
        request_id: str,
    ) -> CompanyAssistantResult:
        try:
            return _result(
                route=route,
                outcome="answered",
                body=_to_commonmark(query()),
            )
        except ValueError as exc:
            return _result(
                route=route,
                outcome="needs_input",
                body=f"{format_error_prefix}: {exc}",
                fallback_reason="invalid_request",
            )
        except (pymysql.MySQLError, RuntimeError) as exc:
            self._logger.warning(
                "Barcode assistant dependency failed route=%s request_id=%s error_type=%s",
                route,
                request_id,
                type(exc).__name__,
            )
            return _result(
                route=route,
                outcome="failed",
                body=dependency_error,
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            # fallback query도 원인 추적이 가능하도록 내부 traceback을 보존한다.
            self._logger.exception(
                "Barcode assistant query failed route=%s request_id=%s error_type=%s",
                route,
                request_id,
                type(exc).__name__,
            )
            return _result(
                route=route,
                outcome="failed",
                body=retry_error,
                fallback_reason="query_error",
            )


def _to_commonmark(text: str) -> str:
    return slack_mrkdwn_to_commonmark(text)


def _is_safe_barcode_answer(text: str) -> bool:
    lowered = (text or "").lower()
    return "다른 바코드" not in text and "다른 barcode" not in lowered


def _result(
    *,
    route: str,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = ["BarcodeQueryAssistantRoute"]
