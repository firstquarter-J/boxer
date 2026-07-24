from __future__ import annotations

import logging
from typing import Callable

import pymysql

from boxer_company import settings as cs
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.scope_guard import (
    AssistantRequestScopeMismatch,
    build_scope_mismatch_result,
    resolve_assistant_request_scope,
)
from boxer_company.routers.barcode_log import (
    _extract_capture_seq_filters,
    _extract_device_flag_filters,
    _extract_device_name_scope,
    _extract_device_seq_filter,
    _extract_device_status_filter,
    _extract_hospital_room_scope,
    _extract_leading_hospital_scope,
    _extract_log_date_with_presence,
    _extract_year_filter,
    _is_devices_filter_query_request,
    _is_hospitals_filter_query_request,
    _is_hospital_rooms_filter_query_request,
    _is_recordings_filter_query_request,
    _is_ultrasound_capture_filter_query_request,
)
from boxer_company.routers.box_db import (
    _query_devices_by_filters,
    _query_hospitals_by_filters,
    _query_hospital_rooms_by_filters,
    _query_recordings_by_filters,
    _query_ultrasound_captures_by_filters,
)
from boxer_company.routers.recording_streaming_restore import (
    _is_recording_streaming_restore_request,
)
from boxer_company.weekly_recordings_report import (
    _is_weekly_recordings_report_request,
)

class StructuredAssistantRoute:
    name = "structured"

    def __init__(
        self,
        *,
        is_weekly_report_request: Callable[..., bool] = (
            _is_weekly_recordings_report_request
        ),
        logger: logging.Logger | None = None,
    ) -> None:
        self._is_weekly_report_request = is_weekly_report_request
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
        if _is_recording_streaming_restore_request(question, barcode):
            # 복원은 상태 변경 작업이라 read-only assistant service 밖에 유지한다.
            return None

        try:
            parsed_date, has_requested_date = _extract_log_date_with_presence(question)
            target_date = parsed_date if has_requested_date else None
        except ValueError as exc:
            target_date = None
            date_error: ValueError | None = exc
        else:
            date_error = None

        target_year = _extract_year_filter(question)
        if target_year is not None and target_date is None:
            date_error = None
        hospital_name, room_name = _extract_hospital_room_scope(question)
        if not hospital_name:
            hospital_name = _extract_leading_hospital_scope(question)
        hospital_seq, hospital_room_seq = _extract_capture_seq_filters(question)
        device_name = _extract_device_name_scope(question)
        device_seq = _extract_device_seq_filter(question)
        device_status = _extract_device_status_filter(question)
        active_flag, install_flag = _extract_device_flag_filters(question)
        count_only = _is_generic_count_or_existence_request(question)

        if _is_hospitals_filter_query_request(
            question,
            target_date=target_date,
            target_year=target_year,
            hospital_name=hospital_name,
            hospital_seq=hospital_seq,
        ):
            return self._run_query(
                route="hospitals_filter",
                query=lambda: _raise_or_call(
                    date_error,
                    lambda: _query_hospitals_by_filters(
                        hospital_name=hospital_name,
                        hospital_seq=hospital_seq,
                        target_date=target_date,
                        target_year=target_year,
                        count_only=count_only,
                    ),
                ),
                format_error_prefix="병원 조회 요청 형식 오류",
                dependency_error="병원 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="병원 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_hospital_rooms_filter_query_request(
            question,
            hospital_name=hospital_name,
            room_name=room_name,
            hospital_seq=hospital_seq,
            hospital_room_seq=hospital_room_seq,
        ):
            return self._run_query(
                route="hospital_rooms_filter",
                query=lambda: _query_hospital_rooms_by_filters(
                    hospital_name=hospital_name,
                    room_name=room_name,
                    hospital_seq=hospital_seq,
                    hospital_room_seq=hospital_room_seq,
                    count_only=count_only,
                ),
                format_error_prefix="병실 조회 요청 형식 오류",
                dependency_error="병실 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="병실 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_devices_filter_query_request(
            question,
            device_name=device_name,
            device_seq=device_seq,
            hospital_name=hospital_name,
            room_name=room_name,
            hospital_seq=hospital_seq,
            hospital_room_seq=hospital_room_seq,
            status=device_status,
            active_flag=active_flag,
            install_flag=install_flag,
        ):
            return self._run_query(
                route="devices_filter",
                query=lambda: _query_devices_by_filters(
                    device_name=device_name,
                    device_seq=device_seq,
                    hospital_name=hospital_name,
                    room_name=room_name,
                    hospital_seq=hospital_seq,
                    hospital_room_seq=hospital_room_seq,
                    status=device_status,
                    active_flag=active_flag,
                    install_flag=install_flag,
                    count_only=count_only,
                ),
                format_error_prefix="장비 조회 요청 형식 오류",
                dependency_error="장비 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="장비 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if self._is_weekly_report_request(
            question,
            barcode=barcode,
            target_date=target_date,
        ):
            # Slack Block을 쓰는 주간 리포트는 adapter가 기존 형식으로 렌더링한다.
            return None

        if _is_ultrasound_capture_filter_query_request(
            question,
            barcode=barcode,
            target_date=target_date,
            target_year=target_year,
            hospital_name=hospital_name,
            room_name=room_name,
            hospital_seq=hospital_seq,
            hospital_room_seq=hospital_room_seq,
        ):
            return self._run_query(
                route="ultrasound_captures_filter",
                query=lambda: _raise_or_call(
                    date_error,
                    lambda: _query_ultrasound_captures_by_filters(
                        barcode=barcode,
                        target_date=target_date,
                        target_year=target_year,
                        hospital_name=hospital_name,
                        room_name=room_name,
                        hospital_seq=hospital_seq,
                        hospital_room_seq=hospital_room_seq,
                        count_only=count_only,
                    ),
                ),
                format_error_prefix="캡처 조회 요청 형식 오류",
                dependency_error="캡처 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="캡처 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if _is_recordings_filter_query_request(
            question,
            barcode=barcode,
            target_date=target_date,
            target_year=target_year,
            hospital_name=hospital_name,
            room_name=room_name,
            hospital_seq=hospital_seq,
            hospital_room_seq=hospital_room_seq,
        ):
            return self._run_query(
                route="recordings_filter",
                query=lambda: _raise_or_call(
                    date_error,
                    lambda: _query_recordings_by_filters(
                        barcode=barcode,
                        target_date=target_date,
                        target_year=target_year,
                        hospital_name=hospital_name,
                        room_name=room_name,
                        hospital_seq=hospital_seq,
                        hospital_room_seq=hospital_room_seq,
                        count_only=count_only,
                    ),
                ),
                format_error_prefix="영상 조회 요청 형식 오류",
                dependency_error="영상 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="영상 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )
        return None

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
                "Structured assistant dependency failed route=%s request_id=%s error_type=%s",
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
            # 예상 밖 query 오류는 사용자 응답과 분리해 내부 traceback을 남긴다.
            self._logger.exception(
                "Structured assistant query failed route=%s request_id=%s error_type=%s",
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


def _raise_or_call(
    error: ValueError | None,
    query: Callable[[], str],
) -> str:
    if error is not None:
        raise error
    return query()


def _is_generic_count_or_existence_request(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(token in text for token in cs.VIDEO_COUNT_HINT_TOKENS) or any(
        token in text
        for token in ("있나", "있어", "있는지", "유무", "존재", "몇")
    ) or "count" in lowered


def _to_commonmark(text: str) -> str:
    return slack_mrkdwn_to_commonmark(text)


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


__all__ = ["StructuredAssistantRoute"]
