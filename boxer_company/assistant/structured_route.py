from __future__ import annotations

# 구조화 실행은 provider-free 정본이 확정한 파싱 결과만 소비한다.
from boxer_company.read_routing import (
    AssistantRequestScopeMismatch,
    _build_structured_query_match,
    _is_weekly_recordings_report_request,
)

import logging
from typing import Callable

import pymysql
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.scope_guard import (
    build_scope_mismatch_result,
)
from boxer_company.routers.box_db import (
    _query_devices_by_filters,
    _query_hospitals_by_filters,
    _query_hospital_rooms_by_filters,
    _query_recordings_by_filters,
    _query_ultrasound_captures_by_filters,
)
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


def _raise_or_call(
    error: ValueError | None,
    query: Callable[[], str],
) -> str:
    if error is not None:
        raise error
    return query()


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
]
