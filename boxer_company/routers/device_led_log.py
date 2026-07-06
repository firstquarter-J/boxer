import re
from collections import Counter
from typing import Any

from boxer.core import settings as s
from boxer.core.utils import _display_value, _format_size, _normalize_spaces, _truncate_text
from boxer_company.routers.barcode_log import _extract_device_name_scope, _extract_log_date_with_presence
from boxer_company.routers.s3_domain import _fetch_s3_device_log_lines


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
_LED_COMMAND_PATTERN = re.compile(
    r"^(?P<timestamp>\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}\.\d{3}).*"
    r"\[MmtLED\]\s+info:\s+Sending Command\s+(?P<command>LC:[A-Z0-9:]+)",
    re.IGNORECASE,
)
_LED_RESPONSE_OK_PATTERN = re.compile(r"\[MmtLED\].*LED response ok:\s*LC:OK", re.IGNORECASE)
_LED_HARDWARE_FAILURE_PATTERN = re.compile(
    r"\[MmtLED\].*(fail|failed|error|timeout|cannot|not found|no such|denied)",
    re.IGNORECASE,
)
_LOG_TIMESTAMP_PATTERN = re.compile(r"^(?P<timestamp>\d{4}-\d{2}-\d{2}_\d{2}:\d{2}:\d{2}\.\d{3})")
_SENSITIVE_LINE_HINTS = (
    "All Configs:",
    '"jwt"',
    "AUTHORIZED_KEYS",
    "PASSWORD",
    "SECRET",
    "TOKEN",
)
_CAUSE_HINTS = (
    "acpi",
    "button/power",
    "sigint",
    "sigterm",
    "exit",
    "power",
    "전원",
    "dark",
    "luminance",
    "empty",
    "error",
    "failed",
    "fail",
    "no such file",
    "/dev/video",
    "forcedstate",
    "captureboardstatus",
    "standby error",
    "video signal",
)
_COMMAND_SPECS: dict[str, dict[str, str]] = {
    "LC:ON:G:": {
        "state": "ready",
        "label": "정상 대기",
        "meaning": "정상 대기/준비 상태",
    },
    "LC:ON:B:": {
        "state": "motion",
        "label": "모션 감지 대기",
        "meaning": "모션 감지 대기 상태",
    },
    "LC:BR:R:": {
        "state": "recording",
        "label": "녹화 중",
        "meaning": "녹화 중",
    },
    "LC:BL:B:": {
        "state": "paused",
        "label": "일시정지",
        "meaning": "일시정지",
    },
    "LC:FBL:R:G:": {
        "state": "warning",
        "label": "경고 표시",
        "meaning": "이미지 품질 이상, 녹화 정체, 비디오 길이 불일치",
    },
    "LC:FBL:R:B:": {
        "state": "error",
        "label": "에러 표시",
        "meaning": "단색 화면, 입력 없음, 화면 어두움, 캡처 입력 이상",
    },
    "LC:3C:": {
        "state": "busy",
        "label": "종료/재시작 계열 표시",
        "meaning": "종료/재시작 같은 busy 상태",
    },
}
_NORMAL_STATES = {"ready", "motion", "recording", "paused"}


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


def _format_led_log_date_required_message(device_name: str | None = None) -> str:
    example_device = device_name or "MB2-C00419"
    return (
        "LED 로그 확인은 날짜가 필요해.\n"
        f"예: `{example_device} 2026-07-04 LED 로그 확인`"
    )


def _is_sensitive_log_line(line: str) -> bool:
    upper_line = str(line or "").upper()
    return any(hint.upper() in upper_line for hint in _SENSITIVE_LINE_HINTS)


def _compact_log_line(line: str) -> str:
    sanitized = " ".join(str(line or "").split()).strip()
    if not sanitized:
        return ""
    return _truncate_text(sanitized, 220)


def _line_timestamp(line: str) -> str:
    matched = _LOG_TIMESTAMP_PATTERN.match(str(line or ""))
    return str(matched.group("timestamp") or "") if matched else ""


def _is_cause_line(line: str) -> bool:
    if not line or "[MmtLED]" in line or _is_sensitive_log_line(line):
        return False
    lowered = line.lower()
    return any(hint in lowered for hint in _CAUSE_HINTS)


def _collect_led_cause_lines(lines: list[str], index: int, *, window: int = 10) -> list[str]:
    start = max(0, index - window)
    end = min(len(lines), index + window + 1)
    cause_lines: list[str] = []
    seen: set[str] = set()
    for line in lines[start:end]:
        if not _is_cause_line(line):
            continue
        compacted = _compact_log_line(line)
        if not compacted or compacted in seen:
            continue
        seen.add(compacted)
        cause_lines.append(compacted)
        if len(cause_lines) >= 4:
            break
    return cause_lines


def _build_led_event(lines: list[str], index: int, matched: re.Match[str]) -> dict[str, Any]:
    command = str(matched.group("command") or "").strip()
    spec = _COMMAND_SPECS.get(command) or {
        "state": "unknown",
        "label": "알 수 없는 LED 명령",
        "meaning": "등록되지 않은 LED 명령",
    }
    return {
        "timestamp": str(matched.group("timestamp") or "").strip(),
        "command": command,
        "state": spec["state"],
        "label": spec["label"],
        "meaning": spec["meaning"],
        "lineNumber": index + 1,
        "causeLines": _collect_led_cause_lines(lines, index),
    }


def _extract_led_connection_lines(lines: list[str]) -> list[str]:
    connection_lines: list[str] = []
    for line in lines:
        if "[MmtLED]" not in line or _is_sensitive_log_line(line):
            continue
        lowered = line.lower()
        if not any(
            token in lowered
            for token in (
                "found device",
                "connected successfully",
                "configured led usb",
                "failed",
                "error",
                "timeout",
            )
        ):
            continue
        compacted = _compact_log_line(line)
        if compacted:
            connection_lines.append(compacted)
        if len(connection_lines) >= 5:
            break
    return connection_lines


def _analyze_device_led_log(
    s3_client: Any,
    device_name: str,
    log_date: str,
) -> tuple[str, dict[str, Any]]:
    log_data = _fetch_s3_device_log_lines(s3_client, device_name, log_date, tail_only=False)
    if not log_data["found"]:
        payload = {
            "route": "device_led_log_analysis",
            "source": "s3_device_log",
            "request": {
                "deviceName": device_name,
                "logDate": log_date,
            },
            "logFound": False,
            "s3Key": log_data.get("key"),
        }
        return f"S3 로그 파일을 찾지 못했어: `{log_data['key']}`", payload

    lines = [str(line or "") for line in (log_data.get("lines") or [])]
    events: list[dict[str, Any]] = []
    for index, line in enumerate(lines):
        matched = _LED_COMMAND_PATTERN.search(line)
        if matched:
            events.append(_build_led_event(lines, index, matched))

    command_counts = Counter(str(event.get("command") or "") for event in events)
    state_counts = Counter(str(event.get("state") or "") for event in events)
    response_ok_count = sum(1 for line in lines if _LED_RESPONSE_OK_PATTERN.search(line))
    hardware_failure_lines = [
        _compact_log_line(line)
        for line in lines
        if _LED_HARDWARE_FAILURE_PATTERN.search(line) and not _is_sensitive_log_line(line)
    ]
    hardware_failure_lines = [line for line in hardware_failure_lines if line]
    connection_lines = _extract_led_connection_lines(lines)

    # 현장 이슈 판단에는 정상 상태 전환보다 warning/error/busy 계열 LED 명령을 우선 보여준다.
    notable_events = [
        event for event in events if str(event.get("state") or "") not in _NORMAL_STATES
    ]

    payload = {
        "route": "device_led_log_analysis",
        "source": "s3_device_log",
        "request": {
            "deviceName": device_name,
            "logDate": log_date,
        },
        "logFound": True,
        "s3Bucket": s.S3_LOG_BUCKET,
        "s3Key": log_data.get("key"),
        "contentLength": log_data.get("content_length"),
        "lineCount": len(lines),
        "ledEventCount": len(events),
        "ledResponseOkCount": response_ok_count,
        "commandCounts": dict(command_counts),
        "stateCounts": dict(state_counts),
        "hardwareFailureLines": hardware_failure_lines[:5],
        "connectionLines": connection_lines,
        "notableEvents": notable_events[:12],
    }
    return _render_device_led_log_analysis(payload), payload


def _format_state_count(payload: dict[str, Any], state: str, label: str) -> str | None:
    count = int((payload.get("stateCounts") or {}).get(state) or 0)
    if count <= 0:
        return None
    return f"{label} `{count}건`"


def _render_device_led_log_analysis(payload: dict[str, Any]) -> str:
    request = payload.get("request") if isinstance(payload.get("request"), dict) else {}
    device_name = _display_value(request.get("deviceName"), default="unknown")
    log_date = _display_value(request.get("logDate"), default="unknown")
    notable_events = payload.get("notableEvents") or []
    hardware_failure_lines = payload.get("hardwareFailureLines") or []

    notable_parts = [
        part
        for part in (
            _format_state_count(payload, "busy", "종료/재시작 계열"),
            _format_state_count(payload, "error", "에러 표시"),
            _format_state_count(payload, "warning", "경고 표시"),
            _format_state_count(payload, "unknown", "알 수 없는 LED 명령"),
        )
        if part
    ]
    if hardware_failure_lines:
        conclusion = "LED 통신/연결 실패 로그가 있어. 아래 근거를 봐야 해"
    elif notable_events:
        conclusion = "LED 표시 이상으로 볼 만한 로그가 있어"
    else:
        conclusion = "LED 표시 이상으로 볼 만한 경고/에러/종료 계열 명령은 안 보여"

    lines = [
        "*장비 LED 로그 확인*",
        f"• 장비: `{device_name}`",
        f"• 날짜: `{log_date}`",
        f"• 파일: `{_display_value(payload.get('s3Key'), default='unknown')}` (`{_format_size(payload.get('contentLength'))}`)",
        f"• 결론: {conclusion}",
        (
            "• LED 통신: "
            f"`LC:OK` 응답 `{int(payload.get('ledResponseOkCount') or 0)}건`, "
            f"실패 로그 `{len(hardware_failure_lines)}건`"
        ),
    ]
    if notable_parts:
        lines.append(f"• 이상 표시 요약: {', '.join(notable_parts)}")
    else:
        lines.append("• 이상 표시 요약: 없음")

    connection_lines = payload.get("connectionLines") or []
    if connection_lines:
        lines.append("• 연결 근거: " + _truncate_text(" / ".join(connection_lines[:2]), 500))

    if notable_events:
        lines.append("")
        lines.append("*주요 로그*")
        for event in notable_events[:8]:
            timestamp = _display_value(event.get("timestamp"), default="-")
            command = _display_value(event.get("command"), default="-")
            label = _display_value(event.get("label"), default="-")
            meaning = _display_value(event.get("meaning"), default="-")
            lines.append(f"- `{timestamp}` `{command}` {label}: {meaning}")
            cause_lines = event.get("causeLines") or []
            if cause_lines:
                lines.append(f"  근거: {_truncate_text(' / '.join(cause_lines), 500)}")

    if hardware_failure_lines:
        lines.append("")
        lines.append("*LED 통신 실패 로그*")
        for line in hardware_failure_lines[:5]:
            lines.append(f"- `{_line_timestamp(line) or '-'}` {_truncate_text(line, 260)}")

    return _truncate_text("\n".join(lines), s.S3_QUERY_MAX_RESULT_CHARS)


__all__ = [
    "_analyze_device_led_log",
    "_format_led_log_date_required_message",
    "_is_device_led_log_analysis_request",
]
