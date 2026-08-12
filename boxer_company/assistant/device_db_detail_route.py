from __future__ import annotations

from dataclasses import dataclass
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
    AssistantRequestScopeMismatch,
    build_scope_mismatch_result,
)
from boxer_company.assistant.structured_route import (
    _StructuredQueryMatch,
    _build_structured_query_match,
)
from boxer_company.routers.box_db import _query_devices_by_filters
from boxer_company.weekly_recordings_report import (
    _is_weekly_recordings_report_request,
)


DeviceQuery = Callable[..., str]

# 이 표현들은 DB row 조회보다 현재 장비를 직접 확인하려는 의도가 강하다.
# 공통 API가 MDA/SSH/프로세스 probe를 대신 수행하거나 DB status를 live
# 상태로 오해하게 하지 않고 기존 Slack local 경로에 남긴다.
_LIVE_DEVICE_INTENT_TOKENS = (
    "온라인",
    "오프라인",
    "연결 상태",
    "연결상태",
    "연결 확인",
    "접속 상태",
    "접속상태",
    "접속 확인",
    "응답 확인",
    "마지막 보고",
    "최근 보고",
    "heartbeat",
    "last seen",
    "lastseen",
    "reachable",
    "connectivity",
    "uptime",
    "health check",
    "healthcheck",
    "status probe",
    "online",
    "offline",
    "connected",
    "disconnected",
    "connection check",
    "핑",
    "ping",
    "pong",
    "ssh",
    "mda",
    "엠디에이",
    "원격 접속",
    "원격접속",
    "버전",
    "version",
    "캡처보드",
    "캡쳐보드",
    "캡처 카드",
    "캡쳐 카드",
    "captureboard",
    "capture board",
    "capture card",
    "pm2",
    "프로세스 상태",
    "프로세스상태",
    "process status",
    # 읽기처럼 보이는 장비 필터 문장에 변경 동사가 붙어도 공통 turn은
    # 절대 mutation으로 확장하지 않고 기존 Slack action 경계에 남긴다.
    "삭제",
    "수정",
    "변경",
    "업데이트",
    "바꿔",
    "재부팅",
    "재시작",
    "종료",
    "전원",
    "초기화",
    "등록",
    "해제",
)
_LIVE_STATUS_PROBE_PATTERNS = (
    re.compile(r"(?:현재|지금|실시간).{0,12}상태", re.IGNORECASE),
    re.compile(r"상태.{0,12}(?:현재|지금|실시간)", re.IGNORECASE),
    re.compile(
        r"(?:장비\s*)?상태\s*"
        r"(?:확인|체크|점검|봐|보여|알려|어때|어떤|정상|작동|동작)",
        re.IGNORECASE,
    ),
    re.compile(r"(?:장비\s*)?상태\s*[?!.,~]*$", re.IGNORECASE),
    re.compile(
        r"(?:device\s+)?status\s*(?:check|probe|show|tell|what|\?)",
        re.IGNORECASE,
    ),
    re.compile(r"(?:device\s+)?status\s*[?!.,~]*$", re.IGNORECASE),
    re.compile(
        r"장비.{0,12}정상\s*(?:이야|인가|맞아|해|한지|인지|\?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:정상\s*)?(?:작동|동작)\s*(?:중|해|하나|하니|하는지)",
        re.IGNORECASE,
    ),
    re.compile(r"(?:살아\s*있|alive|responding)", re.IGNORECASE),
)
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


@dataclass(frozen=True, slots=True)
class _DeviceDbDetailMatch:
    """기존 structured parser가 확정한 DB 장비 상세 조회 인자다."""

    device_name: str | None
    device_seq: int | None
    hospital_name: str | None
    room_name: str | None
    hospital_seq: int | None
    hospital_room_seq: int | None
    device_status: str | None
    active_flag: int | None
    install_flag: int | None


def match_device_db_detail_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 조회 없이 DB-only 장비 상세·목록 요청만 분류한다."""

    try:
        matched = _build_device_db_detail_match(request)
    except AssistantRequestScopeMismatch:
        # scope 불일치는 기존 공통 guard가 값 노출 없이 처리하게 둔다.
        return None
    return "device_db_detail" if matched is not None else None


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
                outcome="failed",
                body="장비 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="query_error",
            )

        return _result(
            outcome="answered",
            body=_format_db_only_device_result(raw),
        )


def _build_device_db_detail_match(
    request: CompanyAssistantRequest,
) -> _DeviceDbDetailMatch | None:
    if _has_live_device_intent(request.question):
        return None

    parsed: _StructuredQueryMatch | None = _build_structured_query_match(
        request,
        is_weekly_report_request=_is_weekly_recordings_report_request,
    )
    if (
        parsed is None
        or parsed.route != "devices_filter"
        or parsed.count_only
    ):
        # 개수·존재는 기존 devices_filter rollout에 유지한다.
        return None

    return _DeviceDbDetailMatch(
        device_name=parsed.device_name,
        device_seq=parsed.device_seq,
        hospital_name=parsed.hospital_name,
        room_name=parsed.room_name,
        hospital_seq=parsed.hospital_seq,
        hospital_room_seq=parsed.hospital_room_seq,
        device_status=parsed.device_status,
        active_flag=parsed.active_flag,
        install_flag=parsed.install_flag,
    )


def _has_live_device_intent(question: str) -> bool:
    normalized = " ".join(str(question or "").split())
    lowered = normalized.lower()
    if any(token in lowered for token in _LIVE_DEVICE_INTENT_TOKENS):
        return True
    return any(pattern.search(normalized) for pattern in _LIVE_STATUS_PROBE_PATTERNS)


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
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route="device_db_detail",
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "DeviceDbDetailAssistantRoute",
    "match_device_db_detail_route",
]
