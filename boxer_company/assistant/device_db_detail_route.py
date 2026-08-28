from __future__ import annotations

# 장비 read parser는 transport와 API 실행부가 공유하는 순수 정본을 쓴다.
from boxer_company.read_routing import (
    AssistantRequestScopeMismatch,
    _build_device_db_detail_match,
    _build_device_detail_match,
    _is_exact_device_detail_request,
)

import logging
import re
from typing import Callable

import pymysql

from boxer_company.assistant.commonmark import (
    slack_mrkdwn_to_commonmark,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.scope_guard import (
    build_scope_mismatch_result,
)
from boxer_company.routers.box_db import _query_devices_by_filters


DeviceQuery = Callable[..., str]

_DEVICE_DETAIL_ROUTE_GROUP = "device_detail"

_UNSAFE_DETAIL_LABELS = (
    "버전:",
    "version:",
    "캡처보드",
    "캡쳐보드",
    "captureboard",
    "capture board",
    "capture card",
    "ssh",
    "mda",
    "엠디에이",
    "원격 접속",
    "원격접속",
    "pm2",
    "초음파 영상 다운로드 가능 상태",
)
_DB_STATUS_LABEL_PATTERN = re.compile(
    r"(?<![A-Za-z가-힣])status\s*:",
    re.IGNORECASE,
)
class DeviceDetailAssistantRoute:
    """기존 Slack의 live-enriched 장비 조회 전체를 API에서 실행한다."""

    name = "device_detail"

    def __init__(
        self,
        *,
        query_devices: DeviceQuery = _query_devices_by_filters,
        logger: logging.Logger | None = None,
    ) -> None:
        self._query_devices = query_devices
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        route_group = str(
            request.metadata.get("route_group") or ""
        ).strip()
        if route_group != _DEVICE_DETAIL_ROUTE_GROUP:
            # 명시된 API stage에서만 live 보강을 열어 기존 direct/API 요청은
            # 뒤의 DB-only route로 계속 처리한다.
            return None

        try:
            matched = _build_device_detail_match(request)
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        if matched is None:
            # count/existence와 명시적인 live operation은 각각 기존 structured,
            # operations 경계가 처리하므로 이 mutation-capable stage가 넘긴다.
            return None

        result_route = (
            _DEVICE_DETAIL_ROUTE_GROUP
            if _is_exact_device_detail_request(matched)
            else "devices_filter"
        )
        try:
            # 채널만 API로 옮겨도 기존 Slack 장비 필터와 같은
            # DB/MDA/SSH 기본 lifecycle을 그대로 쓴다.
            raw = self._query_devices(
                device_name=matched.device_name,
                device_seq=matched.device_seq,
                hospital_name=matched.hospital_name,
                room_name=matched.room_name,
                hospital_seq=matched.hospital_seq,
                hospital_room_seq=matched.hospital_room_seq,
                status=matched.device_status,
                active_flag=matched.active_flag,
                install_flag=matched.install_flag,
                count_only=False,
                include_live_enrichment=True,
                # API turn은 최초 open 한 번 뒤 poll 조회만 허용한다.
                allow_ssh_open_resend=False,
            )
        except ValueError as exc:
            return _result(
                route=result_route,
                outcome="needs_input",
                body=f"장비 조회 요청 형식 오류: {exc}",
                fallback_reason="invalid_request",
            )
        except (pymysql.MySQLError, RuntimeError) as exc:
            self._logger.warning(
                "Device detail dependency failed request_id=%s "
                "error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=result_route,
                outcome="failed",
                body=(
                    "장비 상세 조회 중 오류가 발생했어. "
                    "DB/MDA/SSH 연결 상태를 확인해줘"
                ),
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            # traceback의 exception 문자열에 dependency 응답이나 credential이
            # 섞일 수 있어 운영 로그에는 request ID와 타입만 남긴다.
            self._logger.warning(
                "Device detail query failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route=result_route,
                outcome="failed",
                body=(
                    "장비 상세 조회 중 오류가 발생했어. "
                    "잠시 후 다시 시도해줘"
                ),
                fallback_reason="query_error",
            )

        return _result(
            route=result_route,
            outcome="answered",
            body=slack_mrkdwn_to_commonmark(raw),
        )


class DeviceDbDetailAssistantRoute:
    """장비 DB row만 조회하고 live 보강 필드를 응답에서 제거한다."""

    name = "device_db_detail"

    def __init__(
        self,
        *,
        query_devices: DeviceQuery = _query_devices_by_filters,
        logger: logging.Logger | None = None,
    ) -> None:
        self._query_devices = query_devices
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        try:
            matched = _build_device_db_detail_match(request)
        except AssistantRequestScopeMismatch as mismatch:
            return build_scope_mismatch_result(mismatch)
        if matched is None:
            return None

        try:
            # API 경로는 devices 테이블만 읽는다. 이 두 고정값이 MDA
            # enrichment와 SSH status probe를 실행하지 않는 핵심 경계다.
            raw = self._query_devices(
                device_name=matched.device_name,
                device_seq=matched.device_seq,
                hospital_name=matched.hospital_name,
                room_name=matched.room_name,
                hospital_seq=matched.hospital_seq,
                hospital_room_seq=matched.hospital_room_seq,
                status=matched.device_status,
                active_flag=matched.active_flag,
                install_flag=matched.install_flag,
                count_only=False,
                include_live_enrichment=False,
            )
        except ValueError as exc:
            return _result(
                route="device_db_detail",
                outcome="needs_input",
                body=f"장비 조회 요청 형식 오류: {exc}",
                fallback_reason="invalid_request",
            )
        except (pymysql.MySQLError, RuntimeError) as exc:
            self._logger.warning(
                "Device DB detail dependency failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route="device_db_detail",
                outcome="failed",
                body=(
                    "장비 조회 중 오류가 발생했어. "
                    "DB 연결 정보와 네트워크 상태를 확인해줘"
                ),
                fallback_reason="dependency_error",
            )
        except Exception as exc:
            self._logger.exception(
                "Device DB detail query failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return _result(
                route="device_db_detail",
                outcome="failed",
                body="장비 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="query_error",
            )

        return _result(
            route="device_db_detail",
            outcome="answered",
            body=_format_db_only_device_result(raw),
        )


def _format_db_only_device_result(raw: str) -> str:
    """live 필드가 upstream 문자열에 섞여도 transport 전에 제거한다."""

    safe_lines: list[str] = []
    for line in str(raw or "").splitlines():
        comparable = re.sub(r"[*_`]", "", line).lower()
        if any(label in comparable for label in _UNSAFE_DETAIL_LABELS):
            continue
        safe_lines.append(
            _DB_STATUS_LABEL_PATTERN.sub("DB 저장 status:", line)
        )

    boundary = (
        "• 상태 기준: `devices.status` DB 저장값 "
        "(실시간 온라인/연결 상태 아님)"
    )
    if safe_lines:
        safe_lines.insert(1, boundary)
    else:
        safe_lines.append(boundary)
    return slack_mrkdwn_to_commonmark("\n".join(safe_lines))


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
    "DeviceDetailAssistantRoute",
    "DeviceDbDetailAssistantRoute",
]
