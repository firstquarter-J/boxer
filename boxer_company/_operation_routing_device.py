"""장비 read/mutation operation의 provider-free 분류 정본이다."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from boxer_company._operation_routing_common import (
    CompanyOperationRequestContract as CompanyAssistantRequest,
    _extract_device_name_scope,
    _extract_log_date_with_presence,
    _normalize_spaces,
)
from boxer_company.transport_contracts import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_OPERATION_DELIVERY_ACTION,
    DEVICE_SCANNER_ABI_PATCH_ROUTE,
    _is_device_scanner_abi_patch_intent,
)


_DEVICE_AUDIO_HINTS = (
    "소리",
    "오디오",
    "사운드",
    "스피커",
    "음량",
    "볼륨",
    "mute",
    "muted",
)


_DEVICE_AUDIO_PROBE_HINTS = (
    "점검",
    "체크",
    "확인",
    "테스트",
    "진단",
    "출력",
    "재생",
    "무음",
    "안나와",
    "안 나와",
    "안들려",
    "안 들려",
    "문제",
)


_LEADING_DEVICE_AUDIO_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)


def _normalize_device_audio_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _has_device_audio_hint(text: str) -> bool:
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    has_audio = any(token in normalized or token in lowered for token in _DEVICE_AUDIO_HINTS)
    has_probe = any(token in normalized or token in lowered for token in _DEVICE_AUDIO_PROBE_HINTS)
    return bool(has_audio and has_probe)


def _extract_device_name_for_audio_probe(question: str) -> str | None:
    normalized = _normalize_device_audio_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _has_device_audio_hint(normalized):
        return extracted

    matched = _LEADING_DEVICE_AUDIO_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _has_device_audio_hint(remainder):
        return None
    return candidate


def _is_device_audio_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_audio_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_audio_probe(normalized) or "").strip()
    if not resolved_device_name:
        return False
    return _has_device_audio_hint(normalized)


_LEADING_DEVICE_DIAGNOSTIC_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)


_DEVICE_DIAGNOSTIC_START_HINTS = (
    "진단 시작",
    "진단시작",
)


_DEVICE_DIAGNOSTIC_LIVE_FOLLOWUP_HINTS = (
    "왜",
    "원인",
    "로그",
    "log",
    "앱",
    "app",
    "pm2",
    "시스템",
    "system",
    "os",
    "journal",
    "꺼졌",
    "꺼짐",
    "종료",
    "전원",
    "재시작",
    "재부팅",
    "크래시",
    "에러",
    "오류",
    "실패",
    "메모리",
    "memory",
    "디스크",
    "disk",
)


_DEVICE_DIAGNOSTIC_APP_LOG_HINTS = (
    "왜",
    "원인",
    "로그",
    "log",
    "앱",
    "app",
    "pm2",
    "재시작",
    "크래시",
    "에러",
    "오류",
    "실패",
)


_DEVICE_DIAGNOSTIC_SYSTEM_LOG_HINTS = (
    "시스템",
    "system",
    "os",
    "journal",
    "꺼졌",
    "꺼짐",
    "종료",
    "전원",
    "재부팅",
    "크래시",
)


_DEVICE_DIAGNOSTIC_MEMORY_HINTS = ("메모리", "memory", "oom", "out of memory")


_DEVICE_DIAGNOSTIC_DISK_HINTS = ("디스크", "disk", "용량", "저장공간")


def _normalize_device_diagnostic_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _has_device_diagnostic_start_hint(question: str) -> bool:
    normalized = _normalize_device_diagnostic_question(question)
    return any(hint in normalized for hint in _DEVICE_DIAGNOSTIC_START_HINTS)


def _extract_device_name_for_diagnostic_start(question: str) -> str | None:
    normalized = _normalize_device_diagnostic_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _has_device_diagnostic_start_hint(normalized):
        return extracted

    matched = _LEADING_DEVICE_DIAGNOSTIC_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _has_device_diagnostic_start_hint(remainder):
        return None
    return candidate


def _is_device_diagnostic_start_request(question: str, device_name: str | None = None) -> bool:
    resolved_device_name = str(device_name or _extract_device_name_for_diagnostic_start(question) or "").strip()
    return bool(resolved_device_name and _has_device_diagnostic_start_hint(question))


def _extract_device_name_for_diagnostic_freeform(question: str) -> str | None:
    device_name = _extract_device_name_scope(_normalize_device_diagnostic_question(question))
    if not device_name:
        return None
    if not _select_device_diagnostic_followup_command_keys(question):
        return None
    return device_name


def _is_device_diagnostic_freeform_request(question: str, device_name: str | None = None) -> bool:
    resolved_device_name = str(device_name or _extract_device_name_for_diagnostic_freeform(question) or "").strip()
    return bool(resolved_device_name and _select_device_diagnostic_followup_command_keys(question))


def _has_any_device_diagnostic_hint(question: str, hints: tuple[str, ...]) -> bool:
    text = _normalize_device_diagnostic_question(question)
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in hints)


def _select_device_diagnostic_followup_command_keys(question: str) -> list[str]:
    if not _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_LIVE_FOLLOWUP_HINTS):
        return []

    selected: list[str] = ["pm2_jlist"]
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_APP_LOG_HINTS):
        selected.extend(["pm2_describe_box", "pm2_describe_agent", "pm2_logs_box", "pm2_logs_agent", "app_recent_logs"])
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_SYSTEM_LOG_HINTS):
        selected.extend(["reboot_history", "system_journal_recent", "kernel_oom"])
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_MEMORY_HINTS):
        selected.extend(["memory", "kernel_oom"])
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_DISK_HINTS):
        selected.append("disk")

    deduped: list[str] = []
    for key in selected:
        if key not in deduped:
            deduped.append(key)
    return deduped


_DEVICE_LED_LOG_HINTS = ("led", "엘이디")


_DEVICE_LOG_HINTS = ("로그", "log")


_DEVICE_LED_LOG_INVESTIGATION_HINTS = (
    "이상",
    "문제",
    "조사",
    "분석",
    "확인",
    "찾아",
    "있을까",
    "있나",
    "있어",
    "원인",
    "전원오프",
    "전원 오프",
    "오프상태",
)


def _is_device_led_log_analysis_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_spaces(question).lower()
    if not normalized:
        return False
    resolved_device_name = str(device_name or _extract_device_name_scope(question) or "").strip()
    if not resolved_device_name:
        return False
    has_led_hint = any(hint in normalized for hint in _DEVICE_LED_LOG_HINTS)
    has_log_hint = any(hint in normalized for hint in _DEVICE_LOG_HINTS)
    has_investigation_hint = any(hint in normalized for hint in _DEVICE_LED_LOG_INVESTIGATION_HINTS)
    try:
        _, has_requested_date = _extract_log_date_with_presence(question)
    except ValueError:
        has_requested_date = False
    return bool(has_led_hint and (has_log_hint or (has_requested_date and has_investigation_hint)))


_LEADING_DEVICE_PROBE_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)


_DEVICE_STATUS_HINTS = (
    "장비 상태",
    "장비 연결 상태",
    "장비연결상태",
    "연결 상태",
    "연결상태",
    "상태 점검",
    "장비 점검",
    "전체 상태",
    "종합 상태",
    "health check",
    "healthcheck",
    "헬스 체크",
    "헬스체크",
)


_DEVICE_PM2_HINTS = ("pm2",)


_DEVICE_MEMORY_PATCH_HINTS = (
    "메모리 패치",
    "메모리패치",
    "memory patch",
)


_DEVICE_MEMORY_PATCH_BLOCKING_HINTS = (
    "방법",
    "어떻게",
    "확인 방법",
    "문제 확인",
    "가이드",
    "설명",
    "뭐야",
    "무엇",
    "왜",
)


_DEVICE_CAPTUREBOARD_HINTS = (
    "캡처보드",
    "캡쳐보드",
    "captureboard",
    "capture board",
)


_DEVICE_LED_HINTS = ("led", "엘이디")


_DEVICE_REMOTE_ACCESS_HINTS = (
    "ssh",
    "원격 접속",
    "원격접속",
    "원격 연결",
    "원격연결",
    "원격 진단",
    "원격진단",
    "원격 접근",
    "원격접근",
    "방화벽",
)


_DEVICE_REMOTE_ACCESS_ACTION_HINTS = (
    "ping",
    "핑",
    "확인",
    "점검",
    "체크",
    "테스트",
)


_DEVICE_REMOTE_ACCESS_FAILURE_HINTS = (
    "안 돼",
    "안되",
    "안 돼서",
    "안돼서",
    "안 됨",
    "안됨",
    "불가",
    "실패",
    "막혀",
    "닫혀",
    "접속 안",
    "연결 안",
    "안 열",
    "안열",
    "못 붙",
    "못붙",
)


_DEVICE_LED_PATTERN_EXPLAIN_HINTS = (
    "증상",
    "패턴",
    "의미",
    "뜻",
    "무슨 상태",
    "어떤 상태",
    "어떨 때",
    "언제",
    "왜",
    "원인",
    "나타나",
    "나와",
    "설명",
)


_DEVICE_LED_COLOR_HINTS = (
    "초록불",
    "녹색불",
    "빨간불",
    "적색불",
    "파란불",
    "청색불",
    "초록",
    "녹색",
    "빨강",
    "빨간",
    "적색",
    "파랑",
    "파란",
    "청색",
    "깜빡",
    "깜빡이",
    "blink",
)


_DEVICE_STATUS_ALL_HINTS = (
    *_DEVICE_STATUS_HINTS,
    *_DEVICE_PM2_HINTS,
    *_DEVICE_MEMORY_PATCH_HINTS,
    *_DEVICE_CAPTUREBOARD_HINTS,
    *_DEVICE_LED_HINTS,
)


def _normalize_device_status_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _contains_hint(text: str, hints: tuple[str, ...]) -> bool:
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    return any(hint in normalized or hint in lowered for hint in hints)


def _extract_device_name_for_status_probe(question: str) -> str | None:
    normalized = _normalize_device_status_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _contains_hint(normalized, _DEVICE_STATUS_ALL_HINTS):
        return extracted

    matched = _LEADING_DEVICE_PROBE_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _contains_hint(remainder, _DEVICE_STATUS_ALL_HINTS):
        return None
    return candidate


def _extract_device_name_for_remote_access_probe(question: str) -> str | None:
    normalized = _normalize_device_status_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _contains_hint(normalized, _DEVICE_REMOTE_ACCESS_HINTS):
        return extracted

    matched = _LEADING_DEVICE_PROBE_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _contains_hint(remainder, _DEVICE_REMOTE_ACCESS_HINTS):
        return None
    return candidate


def _is_device_pm2_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_PM2_HINTS))


def _is_device_memory_patch_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(
        device_name or _extract_device_name_scope(normalized) or _extract_device_name_for_status_probe(normalized) or ""
    ).strip()
    if not resolved_device_name or not _contains_hint(normalized, _DEVICE_MEMORY_PATCH_HINTS):
        return False
    return not _contains_hint(normalized, _DEVICE_MEMORY_PATCH_BLOCKING_HINTS)


def _is_device_captureboard_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_CAPTUREBOARD_HINTS))


def _is_device_led_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_LED_HINTS))


def _is_device_led_pattern_help_request(question: str) -> bool:
    normalized = _normalize_device_status_question(question)
    if not normalized:
        return False
    has_led_context = _contains_hint(normalized, _DEVICE_LED_HINTS) or _contains_hint(normalized, _DEVICE_LED_COLOR_HINTS)
    if not has_led_context:
        return False
    return _contains_hint(normalized, _DEVICE_LED_PATTERN_EXPLAIN_HINTS)


def _is_device_remote_access_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_remote_access_probe(normalized) or "").strip()
    if not resolved_device_name or not _contains_hint(normalized, _DEVICE_REMOTE_ACCESS_HINTS):
        return False
    return _contains_hint(normalized, _DEVICE_REMOTE_ACCESS_ACTION_HINTS) or _contains_hint(
        normalized,
        _DEVICE_REMOTE_ACCESS_FAILURE_HINTS,
    )


def _is_device_status_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    if not resolved_device_name:
        return False
    if (
        _is_device_pm2_probe_request(normalized, resolved_device_name)
        or _is_device_captureboard_probe_request(normalized, resolved_device_name)
        or _is_device_led_probe_request(normalized, resolved_device_name)
    ):
        return False
    return _contains_hint(normalized, _DEVICE_STATUS_HINTS)


_LEADING_DEVICE_UPDATE_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)


_UPDATE_HINTS = (
    "업데이트",
    "update",
    "upgrade",
    "패치",
)


_UPDATE_STATUS_HINTS = (
    "상태",
    "현황",
    "확인",
    "체크",
)


_GENERIC_DEVICE_HINTS = (
    "장비",
    "device",
)


_BOX_UPDATE_HINTS = (
    "박스",
    "box",
    "mommybox-v2",
    "momybox-v2",
    "mommybox",
    "momybox",
)


_AGENT_UPDATE_HINTS = (
    "에이전트",
    "agent",
    "mommybox-v2-agent",
    "momybox-v2-agent",
    "mommybox-agent",
    "momybox-agent",
)


_POWER_HINTS = (
    "전원",
    "power",
)


_POWER_OFF_HINTS = (
    "장비 종료",
    "장비종료",
    "전원 종료",
    "전원종료",
    "전원 꺼",
    "전원꺼",
    "전원 끄기",
    "전원 꺼줘",
    "꺼버려",
    "power off",
    "poweroff",
    "shutdown",
)


def _normalize_device_update_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _extract_device_name_for_update(question: str) -> str | None:
    normalized = _normalize_device_update_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _contains_hint(normalized, _UPDATE_HINTS + _UPDATE_STATUS_HINTS):
        return extracted

    matched = _LEADING_DEVICE_UPDATE_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _contains_hint(remainder, _UPDATE_HINTS + _UPDATE_STATUS_HINTS):
        return None
    return candidate


def _is_device_update_status_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_update_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_update(normalized) or "").strip()
    if not resolved_device_name:
        return False
    return _contains_hint(normalized, _UPDATE_HINTS) and _contains_hint(normalized, _UPDATE_STATUS_HINTS)


def _is_device_agent_update_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_update_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_update(normalized) or "").strip()
    if not resolved_device_name or _is_device_update_status_request(normalized, resolved_device_name):
        return False
    return _contains_hint(normalized, _UPDATE_HINTS) and _contains_hint(normalized, _AGENT_UPDATE_HINTS)


def _is_device_box_update_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_update_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_update(normalized) or "").strip()
    if not resolved_device_name or _is_device_update_status_request(normalized, resolved_device_name):
        return False
    if _contains_hint(normalized, _AGENT_UPDATE_HINTS):
        return False
    return _contains_hint(normalized, _UPDATE_HINTS) and (
        _contains_hint(normalized, _BOX_UPDATE_HINTS)
        or _contains_hint(normalized, _GENERIC_DEVICE_HINTS)
    )


def _is_device_power_off_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_update_question(question)
    matched = _LEADING_DEVICE_UPDATE_SCOPE_PATTERN.search(normalized)
    leading_device_name = " ".join(str(matched.group(1) or "").split()).strip() if matched else ""
    resolved_device_name = str(
        device_name
        or _extract_device_name_scope(normalized)
        or leading_device_name
        or _extract_device_name_for_update(normalized)
        or ""
    ).strip()
    if not resolved_device_name:
        return False
    if (
        _is_device_update_status_request(normalized, resolved_device_name)
        or _is_device_agent_update_request(normalized, resolved_device_name)
        or _is_device_box_update_request(normalized, resolved_device_name)
    ):
        return False
    if _contains_hint(normalized, _POWER_OFF_HINTS):
        return True
    return _contains_hint(normalized, _POWER_HINTS) and (
        "꺼" in normalized
        or "끄" in normalized
        or "종료" in normalized
        or "off" in normalized.lower()
    )


_DEVICE_VOICE_CHANGE_HINTS = (
    "바꿔",
    "바꾸",
    "변경해",
    "변경하",
    "설정해",
    "설정하",
    "적용해",
    "적용하",
    "전환해",
    "전환하",
)


_DEVICE_VOICE_CATALOG_HINTS = (
    "음성 세트 목록",
    "음성세트 목록",
    "음성 목록",
    "음성목록",
    "음성 세트 종류",
    "음성세트 종류",
    "음성 종류",
    "지원 음성",
    "음성 선택지",
    "음성 스캔 명령",
    "음성 명령어",
)


def _is_device_voice_change_request(question: str) -> bool:
    text = " ".join(str(question or "").split())
    return "음성" in text and any(hint in text for hint in _DEVICE_VOICE_CHANGE_HINTS)


def _is_device_voice_catalog_request(question: str) -> bool:
    text = " ".join(str(question or "").split())
    return any(hint in text for hint in _DEVICE_VOICE_CATALOG_HINTS)


def match_device_read_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 조회 없이 API 전환 가능한 장비 read-only route만 고른다."""

    question = request.question
    device_name = _resolve_device_name(request)
    # 실제 장비 접속이 필요한 진단·상태 probe는 이 matcher에 넣지 않는다.
    if _is_device_led_log_analysis_request(
        question,
        device_name=device_name,
    ):
        return "device_led_log_analysis"
    if _is_device_led_pattern_help_request(question):
        return "device_led_pattern_guide"
    return None


def _resolve_device_name(request: CompanyAssistantRequest) -> str:
    metadata_name = (
        request.metadata.get("device_name")
        or request.metadata.get("deviceName")
    )
    return str(
        metadata_name or _extract_device_name_scope(request.question) or ""
    ).strip()


_OPERATIONS_ROUTE_GROUP = "operations"


def match_device_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """operations stage에서 기존 Slack matcher 결과를 그대로 반환한다."""

    return match_device_operation_candidate_route(request)


def match_device_operation_candidate_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """안내 질문도 포함해 guard가 선점할 operation 후보만 분류한다."""

    route_group = str(
        request.metadata.get("route_group") or ""
    ).strip()
    if route_group != _OPERATIONS_ROUTE_GROUP:
        return None

    operation_action = request.metadata.get("operation_action")
    if _has_device_operation_delivery_action(operation_action):
        # 전달 receipt가 변조됐더라도 자연어 update matcher로 다시 내려가
        # 장비 명령을 재실행하지 않는다. 상세 형식은 handler에서 fail-closed한다.
        return DEVICE_OPERATION_DELIVERY_ACTION
    if (
        isinstance(operation_action, dict)
        and operation_action
        == {"name": DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION}
    ):
        # Slack의 기존 knowledge 위치는 bounded thread 문구가 아니라 API
        # 프로세스의 실제 `(tenant, channel, thread)` snapshot 존재 여부를
        # 확인했다. 이 typed probe만 context start hint 없이 같은 조회를 연다.
        return "device_diagnostic_followup"

    question = request.question
    if _is_device_scanner_abi_patch_intent(question):
        # 다른 장비 mutation과 섞인 문장도 일부 실행하지 않도록 가장 먼저
        # 전용 route에 격리하고 exact parser에서 전체 문장을 거부한다.
        return DEVICE_SCANNER_ABI_PATCH_ROUTE
    is_voice_change = _is_device_voice_change_request(question)
    if (
        _is_device_voice_catalog_request(question)
        and not is_voice_change
    ):
        # catalog는 장비에 명령을 보내지 않는 전역 목록이라 target이 없어도 된다.
        return "device_voice_catalog"
    structured_device_name = _extract_device_name_scope(question)
    diagnostic_device_name = (
        _extract_device_name_for_diagnostic_start(question)
        or structured_device_name
    )
    update_device_name = (
        _extract_device_name_for_update(question) or structured_device_name
    )
    audio_device_name = (
        _extract_device_name_for_audio_probe(question)
        or structured_device_name
    )
    remote_access_device_name = (
        _extract_device_name_for_remote_access_probe(question)
        or structured_device_name
    )
    status_device_name = (
        _extract_device_name_for_status_probe(question)
        or structured_device_name
    )

    if is_voice_change:
        # 기존 Slack은 음성 변경을 LED read assistant보다 먼저 처리했다.
        # 혼합 문장도 local mutation으로 새지 않고 operations가 소유한다.
        return "device_voice_change"

    # 기존 read-only LED 로그/패턴 안내를 live component probe가 선점하면
    # S3 근거와 가이드 대신 장비 SSH를 실행하므로 명시적으로 넘긴다.
    if _is_device_led_log_analysis_request(
        question,
        device_name=structured_device_name,
    ) or _is_device_led_pattern_help_request(question):
        return None

    # 구체적인 mutation과 component probe를 generic 상태보다 먼저 판정한다.
    if _has_device_diagnostic_start_hint(question) and not (
        diagnostic_device_name
    ):
        # Slack 로컬도 진단 의도만 있으면 route가 장비명 보강 안내를 맡았다.
        return "device_diagnostic_snapshot"
    if _is_device_diagnostic_start_request(
        question,
        device_name=diagnostic_device_name,
    ):
        return "device_diagnostic_snapshot"
    if _is_device_update_status_request(
        question,
        device_name=update_device_name,
    ):
        return "device_update_status"
    if _is_device_box_update_request(
        question,
        device_name=update_device_name,
    ):
        return "device_box_update"
    if _is_device_agent_update_request(
        question,
        device_name=update_device_name,
    ):
        return "device_agent_update"
    if _is_device_power_off_request(
        question,
        device_name=update_device_name,
    ):
        # 기존 Slack은 업데이트 뒤, audio/status probe보다 전원 종료를
        # 먼저 판정했다. 혼합 문장도 같은 장비 명령을 고른다.
        return "device_power_off"
    if _is_device_audio_probe_request(
        question,
        device_name=audio_device_name,
    ):
        return "device_audio_probe"
    if _is_device_remote_access_probe_request(
        question,
        device_name=remote_access_device_name,
    ):
        return "device_remote_access_probe"
    if _is_device_memory_patch_request(
        question,
        device_name=status_device_name,
    ):
        return "device_memory_patch"
    if _is_device_pm2_probe_request(
        question,
        device_name=status_device_name,
    ):
        return "device_pm2_probe"
    if _is_device_captureboard_probe_request(
        question,
        device_name=status_device_name,
    ):
        return "device_captureboard_probe"
    if _is_device_led_probe_request(
        question,
        device_name=status_device_name,
    ):
        return "device_led_probe"
    if _is_device_status_probe_request(
        question,
        device_name=status_device_name,
    ):
        return "device_status_probe"
    if _is_device_diagnostic_followup_request(request):
        # 기존 Slack도 명시 장비 operation을 모두 판정한 뒤 저장된 진단
        # snapshot을 일반 thread 후속 질문의 근거로 사용했다.
        return "device_diagnostic_followup"
    if (
        _is_device_diagnostic_freeform_request(
            question,
            device_name=(
                _extract_device_name_for_diagnostic_freeform(question)
                or structured_device_name
            ),
        )
    ):
        return "device_diagnostic_analysis"
    return None


def _has_device_operation_delivery_action(value: Any) -> bool:
    """동일 이름의 malformed action도 자연어 mutation보다 먼저 격리한다."""

    return bool(
        isinstance(value, Mapping)
        and value.get("name") == DEVICE_OPERATION_DELIVERY_ACTION
    )


def _is_device_diagnostic_followup_request(
    request: CompanyAssistantRequest,
) -> bool:
    if not str(request.question or "").strip():
        return False
    # Slack 로컬은 thread key에 저장된 snapshot이 있으면 요청자 구분 없이
    # 후속 질문에 재사용했다. API matcher도 전달된 thread 시작 문맥만 본다.
    return any(
        _has_device_diagnostic_start_hint(str(entry.get("text") or ""))
        for entry in request.context_entries
        if isinstance(entry, dict)
    )
