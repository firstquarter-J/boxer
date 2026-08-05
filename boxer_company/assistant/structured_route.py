from __future__ import annotations

from dataclasses import dataclass
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
    _is_barcode_all_recorded_dates_request,
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


@dataclass(frozen=True, slots=True)
class _StructuredQueryMatch:
    """DB 호출 전에 확정한 구조화 조회 종류와 파싱 결과다."""

    route: str
    barcode: str | None
    target_date: str | None
    target_year: int | None
    hospital_name: str | None
    room_name: str | None
    hospital_seq: int | None
    hospital_room_seq: int | None
    device_name: str | None
    device_seq: int | None
    device_status: str | None
    active_flag: int | None
    install_flag: int | None
    count_only: bool
    date_error: ValueError | None


class StructuredAssistantRoute:
    name = "structured"

    def __init__(
        self,
        *,
        is_weekly_report_request: Callable[..., bool] = (
            _is_weekly_recordings_report_request
        ),
        device_filter_enabled: bool = True,
        device_live_enrichment_enabled: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        self._is_weekly_report_request = is_weekly_report_request
        self._device_filter_enabled = device_filter_enabled
        self._device_live_enrichment_enabled = (
            device_live_enrichment_enabled
        )
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        try:
            matched = _build_structured_query_match(
                request,
                is_weekly_report_request=self._is_weekly_report_request,
            )
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        if matched is None:
            return None

        if (
            matched.route == "devices_filter"
            and not self._device_filter_enabled
        ):
            # 기존 장비 상세 보강은 MDA SSH open mutation과 실제 SSH 연결로
            # 이어질 수 있어 순수 DB 경로로 분리되기 전에는 API runtime이 막는다.
            return CompanyAssistantResult(
                route="unsupported_device_enrichment",
                outcome="denied",
                messages=(
                    AssistantMessage(
                        body=(
                            "장비 상세 조회는 읽기 전용 API에서 "
                            "지원하지 않아"
                        )
                    ),
                ),
                fallback_reason="read_only_boundary",
            )

        if matched.route == "hospitals_filter":
            return self._run_query(
                route="hospitals_filter",
                query=lambda: _raise_or_call(
                    matched.date_error,
                    lambda: _query_hospitals_by_filters(
                        hospital_name=matched.hospital_name,
                        hospital_seq=matched.hospital_seq,
                        target_date=matched.target_date,
                        target_year=matched.target_year,
                        count_only=matched.count_only,
                    ),
                ),
                format_error_prefix="병원 조회 요청 형식 오류",
                dependency_error="병원 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="병원 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if matched.route == "hospital_rooms_filter":
            return self._run_query(
                route="hospital_rooms_filter",
                query=lambda: _query_hospital_rooms_by_filters(
                    hospital_name=matched.hospital_name,
                    room_name=matched.room_name,
                    hospital_seq=matched.hospital_seq,
                    hospital_room_seq=matched.hospital_room_seq,
                    count_only=matched.count_only,
                ),
                format_error_prefix="병실 조회 요청 형식 오류",
                dependency_error="병실 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="병실 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if matched.route == "devices_filter":
            return self._run_query(
                route="devices_filter",
                query=lambda: _query_devices_by_filters(
                    device_name=matched.device_name,
                    device_seq=matched.device_seq,
                    hospital_name=matched.hospital_name,
                    room_name=matched.room_name,
                    hospital_seq=matched.hospital_seq,
                    hospital_room_seq=matched.hospital_room_seq,
                    status=matched.device_status,
                    active_flag=matched.active_flag,
                    install_flag=matched.install_flag,
                    count_only=matched.count_only,
                    include_live_enrichment=(
                        self._device_live_enrichment_enabled
                    ),
                ),
                format_error_prefix="장비 조회 요청 형식 오류",
                dependency_error="장비 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="장비 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if matched.route == "ultrasound_captures_filter":
            return self._run_query(
                route="ultrasound_captures_filter",
                query=lambda: _raise_or_call(
                    matched.date_error,
                    lambda: _query_ultrasound_captures_by_filters(
                        barcode=matched.barcode,
                        target_date=matched.target_date,
                        target_year=matched.target_year,
                        hospital_name=matched.hospital_name,
                        room_name=matched.room_name,
                        hospital_seq=matched.hospital_seq,
                        hospital_room_seq=matched.hospital_room_seq,
                        count_only=matched.count_only,
                    ),
                ),
                format_error_prefix="캡처 조회 요청 형식 오류",
                dependency_error="캡처 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                retry_error="캡처 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                request_id=request.request_id,
            )

        if matched.route == "recordings_filter":
            return self._run_query(
                route="recordings_filter",
                query=lambda: _raise_or_call(
                    matched.date_error,
                    lambda: _query_recordings_by_filters(
                        barcode=matched.barcode,
                        target_date=matched.target_date,
                        target_year=matched.target_year,
                        hospital_name=matched.hospital_name,
                        room_name=matched.room_name,
                        hospital_seq=matched.hospital_seq,
                        hospital_room_seq=matched.hospital_room_seq,
                        count_only=matched.count_only,
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


def match_structured_read_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """조회 없이 구조화 route만 분류해 adapter의 HTTP 전환 범위를 고정한다."""

    try:
        matched = _build_structured_query_match(
            request,
            is_weekly_report_request=(
                _is_weekly_recordings_report_request
            ),
        )
    except AssistantRequestScopeMismatch:
        # scope 불일치는 기존 local guard가 값 노출 없이 처리하게 둔다.
        return None
    return matched.route if matched is not None else None


def match_structured_device_count_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """live enrichment가 필요 없는 장비 개수·존재 조회만 분류한다."""

    try:
        matched = _build_structured_query_match(
            request,
            is_weekly_report_request=(
                _is_weekly_recordings_report_request
            ),
        )
    except AssistantRequestScopeMismatch:
        return None
    if (
        matched is not None
        and matched.route == "devices_filter"
        and matched.count_only
    ):
        # 개수 응답은 row 상세를 만들기 전에 반환하므로 MDA/SSH 보강과
        # sshOrder mutation을 호출하지 않는다.
        return matched.route
    return None


def _build_structured_query_match(
    request: CompanyAssistantRequest,
    *,
    is_weekly_report_request: Callable[..., bool],
) -> _StructuredQueryMatch | None:
    """기존 route 우선순위를 보존하며 파싱만 하고 외부 조회는 하지 않는다."""

    question = request.question
    barcode = resolve_assistant_request_scope(request).barcode
    if _is_recording_streaming_restore_request(question, barcode):
        # 복원은 상태 변경 작업이라 read-only assistant service 밖에 유지한다.
        return None
    if _is_barcode_all_recorded_dates_request(question, barcode):
        # 더 구체적인 바코드 날짜 route가 뒤 stage에서 응답하도록
        # 일반 recordings 필터가 같은 질문을 먼저 흡수하지 않는다.
        return None

    try:
        parsed_date, has_requested_date = (
            _extract_log_date_with_presence(question)
        )
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
    hospital_seq, hospital_room_seq = _extract_capture_seq_filters(
        question
    )
    device_name = _extract_device_name_scope(question)
    device_seq = _extract_device_seq_filter(question)
    device_status = _extract_device_status_filter(question)
    active_flag, install_flag = _extract_device_flag_filters(question)
    count_only = _is_generic_count_or_existence_request(question)

    route: str | None = None
    if _is_hospitals_filter_query_request(
        question,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        hospital_seq=hospital_seq,
    ):
        route = "hospitals_filter"
    elif _is_hospital_rooms_filter_query_request(
        question,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
    ):
        route = "hospital_rooms_filter"
    elif _is_devices_filter_query_request(
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
        route = "devices_filter"
    elif is_weekly_report_request(
        question,
        barcode=barcode,
        target_date=target_date,
    ):
        # Slack Block을 쓰는 주간 리포트는 adapter가 기존 형식으로 렌더링한다.
        return None
    elif _is_ultrasound_capture_filter_query_request(
        question,
        barcode=barcode,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
    ):
        route = "ultrasound_captures_filter"
    elif _is_recordings_filter_query_request(
        question,
        barcode=barcode,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
    ):
        route = "recordings_filter"

    if route is None:
        return None
    return _StructuredQueryMatch(
        route=route,
        barcode=barcode,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
        device_name=device_name,
        device_seq=device_seq,
        device_status=device_status,
        active_flag=active_flag,
        install_flag=install_flag,
        count_only=count_only,
        date_error=date_error,
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


__all__ = [
    "StructuredAssistantRoute",
    "match_structured_device_count_route",
    "match_structured_read_route",
]
