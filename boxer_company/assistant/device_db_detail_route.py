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
    _ambiguous_device_scope_result,
    _build_structured_query_match,
    _has_multiple_explicit_device_names,
)
from boxer_company.routers.box_db import _query_devices_by_filters
from boxer_company.weekly_recordings_report import (
    _is_weekly_recordings_report_request,
)


DeviceQuery = Callable[..., str]

_DEVICE_DETAIL_ROUTE_GROUP = "device_detail"

# 이 표현들은 구조화 DB 필터보다 별도의 실시간 진단·변경 의도가 강하다.
# 구조화 장비 조회는 모두 API로 옮기되 status probe/PM2 같은 operation은
# 앞선 operations stage가 계속 소유하도록 여기서 선점하지 않는다.
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
_DEVICE_MUTATION_INTENT_TOKENS = (
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
_DEVICE_NAME_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"([A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*)"
    r"(?![A-Za-z0-9])",
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


def match_device_detail_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """live 보강이 필요한 모든 비-count 장비 조회의 결과 route를 고른다."""

    try:
        matched = _build_device_detail_match(request)
    except AssistantRequestScopeMismatch:
        # matcher는 transport 전 분류만 맡고 scope 오류 응답은 route가 만든다.
        return None
    if matched is None:
        return None
    # exact 장비명 상세는 이미 공개된 device_detail 계약을 유지하고,
    # deviceSeq·병원·병실·status·목록은 기존 Slack route 이름을 보존한다.
    return (
        _DEVICE_DETAIL_ROUTE_GROUP
        if _is_exact_device_detail_request(request, matched)
        else "devices_filter"
    )


def extract_device_detail_name(
    request: CompanyAssistantRequest,
) -> str | None:
    """full 장비 상세 rollout의 exact 장비명만 같은 parser에서 꺼낸다."""

    try:
        matched = _build_device_detail_match(request)
    except AssistantRequestScopeMismatch:
        return None
    if (
        matched is None
        or not _is_exact_device_detail_request(request, matched)
    ):
        return None
    # live 상세은 MDA의 canonical deviceName을 요구한다. deviceSeq 단독
    # 요청은 이름을 추측하지 않고 기존 local/DB-only 경계에 남긴다.
    return str(matched.device_name or "").strip() or None


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

        if _has_multiple_explicit_device_names(request.question):
            # structured parser가 첫 장비만 선택해 live 보강하는 것을 막는다.
            return _ambiguous_device_scope_result()

        result_route = (
            _DEVICE_DETAIL_ROUTE_GROUP
            if _is_exact_device_detail_request(request, matched)
            else "devices_filter"
        )
        if _has_device_mutation_intent(request.question):
            # 미지원 변경 명령이 legacy structured 조회로 내려가 DB/MDA/SSH를
            # 실행하지 않게 API가 결정적으로 종결한다. 실제 지원 operation은
            # Slack gateway의 operations matcher가 이 stage보다 먼저 처리한다.
            return _result(
                route=result_route,
                outcome="denied",
                body=(
                    "이 장비 변경 요청은 지원하는 작업으로 분류되지 않았어. "
                    "장비명과 실행할 작업을 하나씩 명확히 적어줘"
                ),
                fallback_reason="unsupported_device_mutation",
            )

        try:
            # 기존 Slack 장비 필터와 같은 DB/MDA/SSH 흐름을 쓰되, 결과가
            # 한 대라 endpoint를 열더라도 sshOrder는 한 번만 보내고 poll은 조회만 한다.
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

        if _has_multiple_explicit_device_names(request.question):
            return _ambiguous_device_scope_result()

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


def _build_device_db_detail_match(
    request: CompanyAssistantRequest,
) -> _DeviceDbDetailMatch | None:
    """MDA/SSH 의도는 제외하고 devices DB-only 조회만 분류한다."""

    if _has_live_device_intent(request.question):
        return None

    return _build_device_query_match(request)


def _build_device_detail_match(
    request: CompanyAssistantRequest,
) -> _DeviceDbDetailMatch | None:
    """Slack에 남아 있던 live 장비 조회까지 API 대상으로 분류한다."""

    matched = _build_device_query_match(request)
    if matched is not None:
        return matched

    # 기존 structured parser가 `오프라인인지` 같은 표현을 놓쳐도 질문에
    # 명시된 장비가 정확히 하나면 Slack local probe로 빠지지 않게 한다.
    explicit_names = {
        token.casefold(): token
        for token in _DEVICE_NAME_TOKEN_PATTERN.findall(request.question)
    }
    if len(explicit_names) != 1 or not _has_live_device_intent(
        request.question
    ):
        return None
    device_name = next(iter(explicit_names.values()))
    return _DeviceDbDetailMatch(
        device_name=device_name,
        device_seq=None,
        hospital_name=None,
        room_name=None,
        hospital_seq=None,
        hospital_room_seq=None,
        device_status=None,
        active_flag=None,
        install_flag=None,
    )


def _build_device_query_match(
    request: CompanyAssistantRequest,
) -> _DeviceDbDetailMatch | None:
    """공통 structured parser 결과를 장비 상세 인자로 한 번만 변환한다."""

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


def _is_exact_device_detail_match(
    matched: _DeviceDbDetailMatch,
) -> bool:
    """live 보강 대상이 고유 장비 식별자로만 제한됐는지 확인한다."""

    has_exact_identifier = bool(
        matched.device_name or matched.device_seq is not None
    )
    has_list_filter = any(
        (
            matched.hospital_name,
            matched.room_name,
            matched.hospital_seq is not None,
            matched.hospital_room_seq is not None,
            matched.device_status,
            matched.active_flag is not None,
            matched.install_flag is not None,
        )
    )
    return has_exact_identifier and not has_list_filter


def _is_exact_device_detail_request(
    request: CompanyAssistantRequest,
    matched: _DeviceDbDetailMatch,
) -> bool:
    """질문에 명시된 target 하나와 parser 결과가 정확히 같은지 확인한다."""

    if not _is_exact_device_detail_match(matched) or not matched.device_name:
        # live 상세은 canonical MDA deviceName만 사용한다. deviceSeq 단독
        # 요청과 metadata/context로만 복원된 target은 기존 local/DB-only다.
        return False
    explicit_names = {
        token.casefold()
        for token in _DEVICE_NAME_TOKEN_PATTERN.findall(
            str(request.question or "")
        )
    }
    return explicit_names == {matched.device_name.casefold()}


def _has_live_device_intent(question: str) -> bool:
    normalized = " ".join(str(question or "").split())
    lowered = normalized.lower()
    if any(token in lowered for token in _LIVE_DEVICE_INTENT_TOKENS):
        return True
    return any(pattern.search(normalized) for pattern in _LIVE_STATUS_PROBE_PATTERNS)


def _has_device_mutation_intent(question: str) -> bool:
    lowered = " ".join(str(question or "").split()).lower()
    return any(token in lowered for token in _DEVICE_MUTATION_INTENT_TOKENS)


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
    "extract_device_detail_name",
    "match_device_detail_route",
    "match_device_db_detail_route",
]
