import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.core.utils import _display_value, _format_size, _truncate_text
from boxer.routers.company.box_db import _lookup_device_contexts_by_barcode
from boxer.routers.company.s3_domain import _fetch_s3_device_log_lines

_NUMERIC_YMD_PATTERN = re.compile(r"(?<!\d)(\d{2,4})\s*[-./]\s*(\d{1,2})\s*[-./]\s*(\d{1,2})(?!\d)")
_KOREAN_YMD_PATTERN = re.compile(
    r"(?<!\d)(\d{2,4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일(?!\d)"
)
_NUMERIC_MD_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*[./]\s*(\d{1,2})(?!\d)")
_KOREAN_MD_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*월\s*(\d{1,2})\s*일(?!\d)")
_MOTION_STOP_STATUS_PATTERN = re.compile(
    r"Motion detected:\s*(true|false)\s*,\s*Error:\s*(true|false)",
    re.IGNORECASE,
)
_RESTART_START_PATTERN = re.compile(r"mommybox starting", re.IGNORECASE)
_RESTART_APP_VERSION_PATTERN = re.compile(r"app version:\s*(.+)$", re.IGNORECASE)
_RESTART_NODE_VERSION_PATTERN = re.compile(r"node\.js version:\s*(.+)$", re.IGNORECASE)
_RESTART_PLATFORM_PATTERN = re.compile(r"platform:\s*(.+)$", re.IGNORECASE)
_RESTART_START_TIME_PATTERN = re.compile(r"start time:\s*(.+)$", re.IGNORECASE)
_HOSPITAL_SCOPE_PATTERN = re.compile(
    r"병원명\s*[:=]?\s*(.+?)(?=\s+(?:병실명|진료실명|날짜|로그|분석)\b|$)"
)
_ROOM_SCOPE_PATTERN = re.compile(
    r"(?:병실명|진료실명)\s*[:=]?\s*(.+?)(?=\s+(?:날짜|로그|분석)\b|$)"
)
_RAW_LOG_LEVEL_PATTERN = re.compile(
    r"^\[[^\]]+\]\s+\[[^\]]+\]\s+\[\s*([A-Za-z]+)\s*\]",
    re.IGNORECASE,
)
_NORMALIZED_LOG_LEVEL_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}[_ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\s+\[[^\]]+\]\s+([A-Za-z]+):",
    re.IGNORECASE,
)
_TODAY_HINTS = ("오늘", "금일", "today")
_DAY_BEFORE_YESTERDAY_HINTS = ("그제", "엊그제", "day before yesterday")
_TOMORROW_HINTS = ("내일", "tomorrow")


def _current_local_date() -> datetime.date:
    tz_name = os.getenv("TZ", "Asia/Seoul")
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        try:
            return datetime.now(ZoneInfo("Asia/Seoul")).date()
        except Exception:
            return datetime.utcnow().date()


def _normalize_year(raw_year: int) -> int:
    if raw_year < 100:
        return 2000 + raw_year
    return raw_year


def _try_format_date(year: int, month: int, day: int) -> str | None:
    try:
        parsed = datetime(year=year, month=month, day=day)
    except ValueError:
        return None
    return parsed.strftime("%Y-%m-%d")


def _parse_explicit_date_expression(text: str) -> tuple[bool, str | None]:
    candidates: list[tuple[int, str, re.Match[str]]] = []
    for kind, pattern in (
        ("korean_ymd", _KOREAN_YMD_PATTERN),
        ("numeric_ymd", _NUMERIC_YMD_PATTERN),
        ("korean_md", _KOREAN_MD_PATTERN),
        ("numeric_md", _NUMERIC_MD_PATTERN),
    ):
        matched = pattern.search(text)
        if matched:
            candidates.append((matched.start(), kind, matched))

    if not candidates:
        return False, None

    _, kind, matched = min(candidates, key=lambda item: item[0])

    if kind in {"korean_ymd", "numeric_ymd"}:
        year = _normalize_year(int(matched.group(1)))
        month = int(matched.group(2))
        day = int(matched.group(3))
        return True, _try_format_date(year, month, day)

    local_year = _current_local_date().year
    month = int(matched.group(1))
    day = int(matched.group(2))
    return True, _try_format_date(local_year, month, day)


def _extract_relative_day_offset(text: str, lowered: str) -> int | None:
    if any(token in text or token in lowered for token in _DAY_BEFORE_YESTERDAY_HINTS):
        return -2
    if any(token in text or token in lowered for token in cs.YESTERDAY_HINTS):
        return -1
    if any(token in text or token in lowered for token in _TOMORROW_HINTS):
        return 1
    if any(token in text or token in lowered for token in _TODAY_HINTS):
        return 0
    return None


def _has_relative_date_token(text: str, lowered: str) -> bool:
    return _extract_relative_day_offset(text, lowered) is not None


def _extract_log_date(question: str) -> str:
    parsed_date, _ = _extract_log_date_with_presence(question)
    return parsed_date


def _extract_log_date_with_presence(question: str) -> tuple[str, bool]:
    text = (question or "").strip()
    lowered = text.lower()

    has_explicit_date, parsed_explicit_date = _parse_explicit_date_expression(text)
    if has_explicit_date:
        if parsed_explicit_date is None:
            raise ValueError("날짜 형식을 확인해줘. 예: 2026-03-03, 26.03.03, 3/3, 3월 3일")
        return parsed_explicit_date, True

    base_date = _current_local_date()
    relative_offset = _extract_relative_day_offset(text, lowered)
    if relative_offset is not None:
        base_date = base_date + timedelta(days=relative_offset)
        return base_date.strftime("%Y-%m-%d"), True
    return base_date.strftime("%Y-%m-%d"), False


def _is_barcode_log_analysis_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    has_log_hint = ("로그" in text and "로그인" not in text) or bool(
        re.search(r"\blog\b", lowered)
    )
    return has_log_hint


def _is_barcode_video_count_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()

    if "로그" in text or re.search(r"\blog\b", lowered):
        return False

    has_video_hint = any(token in text for token in cs.VIDEO_HINT_TOKENS) or any(
        token in lowered for token in cs.VIDEO_HINT_TOKENS
    )
    if not has_video_hint:
        return False

    has_count_hint = any(token in text for token in cs.VIDEO_COUNT_HINT_TOKENS) or ("몇" in text)
    return has_count_hint


def _is_barcode_video_list_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False

    text = (question or "").strip()
    lowered = text.lower()

    if "로그" in text or re.search(r"\blog\b", lowered):
        return False

    has_video_hint = any(token in text for token in cs.VIDEO_HINT_TOKENS) or any(
        token in lowered for token in cs.VIDEO_HINT_TOKENS
    )
    if not has_video_hint:
        return False

    has_list_hint = any(token in text for token in ("목록", "리스트")) or any(
        token in lowered for token in ("list", "items")
    )
    has_all_date_hint = any(token in text for token in ("모든", "전체", "전부", "다")) and any(
        token in text for token in ("날짜", "일자")
    )
    return has_list_hint and not has_all_date_hint


def _is_barcode_last_recorded_at_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()

    if "로그" in text or re.search(r"\blog\b", lowered):
        return False

    has_video_hint = any(token in text for token in cs.VIDEO_HINT_TOKENS) or any(
        token in lowered for token in cs.VIDEO_HINT_TOKENS
    ) or any(token in text for token in ("녹화", "촬영"))
    if not has_video_hint:
        return False

    has_last_hint = any(token in text for token in ("마지막", "최근", "최신")) or any(
        token in lowered for token in ("last", "latest", "recent")
    )
    if not has_last_hint:
        return False

    has_date_hint = any(token in text for token in ("날짜", "일자", "언제", "기록")) or any(
        token in lowered for token in ("date", "recordedat", "recorded at")
    )
    if has_date_hint:
        return True

    # "최신 영상은?" 같은 문구는 날짜 단어가 없어도 마지막 녹화 시점 조회로 해석
    return has_video_hint


def _is_barcode_video_recorded_on_date_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    if _is_barcode_last_recorded_at_request(question, barcode):
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False

    text = (question or "").strip()
    lowered = text.lower()

    if "로그" in text or re.search(r"\blog\b", lowered):
        return False

    has_video_hint = any(token in text for token in cs.VIDEO_HINT_TOKENS) or any(
        token in lowered for token in cs.VIDEO_HINT_TOKENS
    ) or any(token in text for token in ("녹화", "촬영", "recordedAt"))
    if not has_video_hint:
        return False

    has_explicit_date, _ = _parse_explicit_date_expression(text)
    has_date_token = has_explicit_date or _has_relative_date_token(text, lowered)
    if not has_date_token:
        return False

    return True


def _is_barcode_all_recorded_dates_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    if _is_barcode_last_recorded_at_request(question, barcode):
        return False
    if _is_barcode_video_recorded_on_date_request(question, barcode):
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False

    text = (question or "").strip()
    lowered = text.lower()

    if "로그" in text or re.search(r"\blog\b", lowered):
        return False

    has_video_hint = any(token in text for token in cs.VIDEO_HINT_TOKENS) or any(
        token in lowered for token in cs.VIDEO_HINT_TOKENS
    ) or any(token in text for token in ("녹화", "촬영", "recordedAt"))
    if not has_video_hint:
        return False

    has_all_hint = any(token in text for token in ("모든", "전체", "전부", "다")) or any(
        token in lowered for token in ("all", "entire")
    )
    has_date_hint = any(token in text for token in ("날짜", "일자", "목록", "리스트")) or any(
        token in lowered for token in ("date", "dates", "list")
    )
    return has_all_hint and has_date_hint


def _find_error_lines(lines: list[str]) -> list[tuple[int, str]]:
    matches: list[tuple[int, str]] = []
    for line_no, line in enumerate(lines, start=1):
        if _is_actual_error_line(line):
            matches.append((line_no, line))
    return matches


def _extract_explicit_log_level(line: str) -> str:
    for pattern in (_RAW_LOG_LEVEL_PATTERN, _NORMALIZED_LOG_LEVEL_PATTERN):
        matched = pattern.search(line or "")
        if matched:
            return matched.group(1).strip().lower()
    return ""


def _is_actual_error_line(line: str) -> bool:
    lowered = (line or "").lower()
    if "low growth rate detected:" in lowered:
        return False

    explicit_level = _extract_explicit_log_level(line)
    if explicit_level:
        return explicit_level in {"error", "fatal", "panic"}

    return any(token in lowered for token in ("traceback", "unhandled exception", "fatal error", "panic:"))


def _is_error_focused_request(question: str) -> bool:
    lowered = (question or "").lower()
    return any(keyword in lowered for keyword in cs.LOG_ERROR_KEYWORDS)


def _is_scan_focused_request(question: str) -> bool:
    lowered = (question or "").lower()
    return any(keyword in lowered for keyword in cs.SCAN_FOCUSED_HINTS)


def _extract_time_label_from_line(line: str) -> str:
    matched = cs.LOG_LINE_TIME_PATTERN.search(line)
    if matched:
        return matched.group(1)
    return "시간미상"


def _strip_leading_log_timestamp(line: str) -> str:
    text = (line or "").strip()
    text = re.sub(r"^\[\d{1,2}:\d{2}:\d{2}(?:[.,]\d{1,6})?\]\s*", "", text)
    text = re.sub(r"^\d{4}-\d{2}-\d{2}[_ T]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?\s*", "", text)
    return text.strip()


def _parse_scanned_event(line: str) -> str | None:
    matched = cs.SCANNED_TOKEN_PATTERN.search(line)
    if not matched:
        return None
    return matched.group(1).strip().strip("`'\",;:()[]{}")


def _extract_scan_events_with_line_no(lines: list[str]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    latest_time_label: str | None = None
    for line_no, line in enumerate(lines, start=1):
        line_time_label = _extract_time_label_from_line(line)
        if line_time_label != "시간미상":
            latest_time_label = line_time_label

        parsed = _parse_scanned_event(line)
        if not parsed:
            continue
        token = parsed
        time_label = line_time_label
        if time_label == "시간미상" and latest_time_label:
            time_label = latest_time_label
        events.append(
            {
                "line_no": line_no,
                "time_label": time_label,
                "token": token,
                "raw_line": _strip_leading_log_timestamp(line),
            }
        )
    return events


def _extract_motion_events_with_line_no(lines: list[str]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    latest_time_label: str | None = None

    for line_no, line in enumerate(lines, start=1):
        line_time_label = _extract_time_label_from_line(line)
        if line_time_label != "시간미상":
            latest_time_label = line_time_label

        lowered = line.lower()
        event_type = ""
        label = ""
        motion_detected: bool | None = None
        error_flag: bool | None = None

        if "motion detection process initiated successfully" in lowered:
            event_type = "motion_start"
            label = "모션 감지 시작(정상)"
        elif (
            "motion detected for" in lowered
            and "stopping detection to start recording" in lowered
        ):
            event_type = "motion_trigger"
            label = "모션 감지 성공(녹화 전환)"
        elif "stopping motion detection." in lowered:
            event_type = "motion_stop"
            label = "모션 감지 종료"
            matched = _MOTION_STOP_STATUS_PATTERN.search(line)
            if matched:
                motion_detected = matched.group(1).lower() == "true"
                error_flag = matched.group(2).lower() == "true"
        else:
            continue

        time_label = line_time_label
        if time_label == "시간미상" and latest_time_label:
            time_label = latest_time_label

        events.append(
            {
                "line_no": line_no,
                "time_label": time_label,
                "event_type": event_type,
                "label": label,
                "motion_detected": motion_detected,
                "error": error_flag,
            }
        )
    return events


def _extract_restart_events_with_line_no(lines: list[str]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    latest_time_label: str | None = None

    for line_no, line in enumerate(lines, start=1):
        line_time_label = _extract_time_label_from_line(line)
        if line_time_label != "시간미상":
            latest_time_label = line_time_label

        stripped = _strip_leading_log_timestamp(line)
        if not _RESTART_START_PATTERN.search(stripped):
            continue

        time_label = line_time_label
        if time_label == "시간미상" and latest_time_label:
            time_label = latest_time_label

        details: dict[str, str] = {}
        for follow_index in range(line_no + 1, min(len(lines), line_no + 8) + 1):
            follow_line = lines[follow_index - 1]
            follow_stripped = _strip_leading_log_timestamp(follow_line)
            for key, pattern in (
                ("appVersion", _RESTART_APP_VERSION_PATTERN),
                ("nodeVersion", _RESTART_NODE_VERSION_PATTERN),
                ("platform", _RESTART_PLATFORM_PATTERN),
                ("startTime", _RESTART_START_TIME_PATTERN),
            ):
                matched = pattern.search(follow_stripped)
                if matched and key not in details:
                    details[key] = matched.group(1).strip()

        events.append(
            {
                "line_no": line_no,
                "time_label": time_label,
                "label": "장비 재시작 감지",
                "raw_line": stripped,
                "details": details,
            }
        )

    return events


def _summarize_motion_session(
    motion_events: list[dict[str, Any]],
) -> dict[str, str]:
    if not motion_events:
        return {
            "start_time": "미확인",
            "end_time": "미확인",
            "success": "미확인",
            "stop_status": "미확인",
        }

    start_time = "미확인"
    end_time = "미확인"
    success = "미확인"
    stop_status = "미확인"

    start_event = next(
        (event for event in motion_events if event.get("event_type") == "motion_start"),
        None,
    )
    if start_event is not None:
        start_time = _display_value(start_event.get("time_label"), default="미확인")

    stop_events = [event for event in motion_events if event.get("event_type") == "motion_stop"]
    if stop_events:
        stop_event = stop_events[-1]
        end_time = _display_value(stop_event.get("time_label"), default="미확인")
        motion_detected = stop_event.get("motion_detected")
        error_flag = stop_event.get("error")
        if motion_detected is not None and error_flag is not None:
            stop_status = f"motionDetected={str(bool(motion_detected)).lower()}, error={str(bool(error_flag)).lower()}"
            if bool(motion_detected) and not bool(error_flag):
                success = "성공"
            elif bool(error_flag):
                success = "실패"

    trigger_event = next(
        (event for event in motion_events if event.get("event_type") == "motion_trigger"),
        None,
    )
    if trigger_event is not None:
        success = "성공"
        if start_time == "미확인":
            start_time = _display_value(trigger_event.get("time_label"), default="미확인")

    if end_time == "미확인" and trigger_event is not None:
        end_time = _display_value(trigger_event.get("time_label"), default="미확인")

    return {
        "start_time": start_time,
        "end_time": end_time,
        "success": success,
        "stop_status": stop_status,
    }


def _extract_recording_sessions(
    lines: list[str],
    barcode: str,
    safety_lines: int,
    scan_events: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    if not barcode:
        return []

    sessions: list[dict[str, Any]] = []
    active: dict[str, Any] | None = None
    normalized_barcode = barcode.strip()
    safe_extra = max(0, min(500, safety_lines))
    events = scan_events if scan_events is not None else _extract_scan_events_with_line_no(lines)

    for event in events:
        token = str(event["token"])
        line_no = int(event["line_no"])
        time_label = str(event["time_label"])
        upper_token = token.upper()
        is_barcode_token = re.fullmatch(r"\d{11}", token) is not None

        # 종료 스캔이 누락돼도, 다음 바코드 스캔이 오면 기존 세션은 종료로 본다.
        if active is not None and is_barcode_token and token != normalized_barcode:
            active["end_line_no"] = max(int(active["start_line_no"]), line_no - 1)
            sessions.append(active)
            active = None

        if token == normalized_barcode:
            if active is not None:
                active["end_line_no"] = max(int(active["start_line_no"]), line_no - 1)
                sessions.append(active)
            active = {
                "start_line_no": line_no,
                "start_time_label": time_label,
                "stop_line_no": None,
                "stop_time_label": None,
                "end_line_no": len(lines),
            }
            continue

        if upper_token == "C_STOPSESS" and active is not None:
            active["stop_line_no"] = line_no
            active["stop_time_label"] = time_label
            active["end_line_no"] = min(len(lines), line_no + safe_extra)
            sessions.append(active)
            active = None

    if active is not None:
        active["end_line_no"] = len(lines)
        sessions.append(active)

    return sessions


def _append_session_closure_status(lines: list[str], sessions: list[dict[str, Any]]) -> None:
    if not sessions:
        return

    normal_count = sum(1 for session in sessions if session.get("stop_line_no") is not None)
    abnormal_count = len(sessions) - normal_count
    if abnormal_count <= 0:
        lines.append("• 종료 상태: 모든 세션 정상 종료 (`C_STOPSESS` 확인)")
        return

    if normal_count <= 0:
        lines.append(f"• 종료 상태: 정상 종료되지 않은 세션 *{abnormal_count}건* (`C_STOPSESS` 없음)")
        return

    lines.append(
        f"• 종료 상태: 정상 종료 *{normal_count}건*, 정상 종료되지 않은 세션 *{abnormal_count}건* (`C_STOPSESS` 없음)"
    )


def _events_in_session(events: list[dict[str, Any]], session: dict[str, Any]) -> list[dict[str, Any]]:
    start_line_no = int(session["start_line_no"])
    end_line_no = int(session["end_line_no"])
    return [
        event
        for event in events
        if start_line_no <= int(event["line_no"]) <= end_line_no
    ]


def _events_in_sessions(events: list[dict[str, Any]], sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not sessions:
        return []
    return [
        event
        for event in events
        if _line_in_any_session(int(event["line_no"]), sessions)
    ]


def _error_lines_in_sessions(
    error_lines: list[tuple[int, str]],
    sessions: list[dict[str, Any]],
) -> list[tuple[int, str]]:
    if not sessions:
        return []
    return [
        (line_no, content)
        for (line_no, content) in error_lines
        if _line_in_any_session(line_no, sessions)
    ]


def _session_closure_counts(sessions: list[dict[str, Any]]) -> dict[str, int | bool]:
    normal_count = sum(1 for session in sessions if session.get("stop_line_no") is not None)
    abnormal_count = max(0, len(sessions) - normal_count)
    return {
        "sessionCount": len(sessions),
        "normalCount": normal_count,
        "abnormalCount": abnormal_count,
        "allClosedNormally": abnormal_count == 0 and len(sessions) > 0,
    }


def _parse_structured_log_line(line: str) -> dict[str, str]:
    stripped = _strip_leading_log_timestamp(line)
    raw_match = re.match(r"^\[\s*([^\]]+?)\s*\]\s+\[\s*([^\]]+?)\s*\]\s*(.*)$", stripped)
    if raw_match:
        return {
            "component": raw_match.group(1).strip(),
            "level": raw_match.group(2).strip().lower(),
            "message": raw_match.group(3).strip(),
            "raw": stripped,
        }

    normalized_match = re.match(r"^\[([^\]]+)\]\s+([A-Za-z]+):\s*(.*)$", stripped)
    if normalized_match:
        return {
            "component": normalized_match.group(1).strip(),
            "level": normalized_match.group(2).strip().lower(),
            "message": normalized_match.group(3).strip(),
            "raw": stripped,
        }

    return {
        "component": "",
        "level": "",
        "message": stripped,
        "raw": stripped,
    }


def _serialize_scan_events_for_evidence(scan_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for event in sorted(scan_events, key=lambda item: int(item.get("line_no") or 0)):
        items.append(
            {
                "lineNo": int(event.get("line_no") or 0),
                "timeLabel": _display_value(event.get("time_label"), default="시간미상"),
                "token": _display_value(event.get("token"), default=""),
                "rawLine": _display_value(event.get("raw_line"), default=""),
            }
        )
    return items


def _serialize_motion_events_for_evidence(motion_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for event in sorted(motion_events, key=lambda item: int(item.get("line_no") or 0)):
        row: dict[str, Any] = {
            "lineNo": int(event.get("line_no") or 0),
            "timeLabel": _display_value(event.get("time_label"), default="시간미상"),
            "eventType": _display_value(event.get("event_type"), default=""),
            "label": _display_value(event.get("label"), default=""),
        }
        if event.get("motion_detected") is not None:
            row["motionDetected"] = bool(event.get("motion_detected"))
        if event.get("error") is not None:
            row["error"] = bool(event.get("error"))
        items.append(row)
    return items


def _serialize_restart_events_for_evidence(restart_events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for event in sorted(restart_events, key=lambda item: int(item.get("line_no") or 0)):
        items.append(
            {
                "lineNo": int(event.get("line_no") or 0),
                "timeLabel": _display_value(event.get("time_label"), default="시간미상"),
                "label": _display_value(event.get("label"), default="장비 재시작 감지"),
                "rawLine": _display_value(event.get("raw_line"), default=""),
                "details": event.get("details") or {},
            }
        )
    return items


def _serialize_error_lines_for_evidence(error_lines: list[tuple[int, str]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for line_no, content in error_lines:
        parsed = _parse_structured_log_line(content)
        items.append(
            {
                "lineNo": int(line_no),
                "timeLabel": _extract_time_label_from_line(content),
                "component": parsed["component"],
                "level": parsed["level"],
                "message": parsed["message"],
                "raw": parsed["raw"],
            }
        )
    return items


def _build_error_groups(error_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    for item in error_items:
        component = str(item.get("component") or "").strip()
        message = str(item.get("message") or item.get("raw") or "").strip()
        key = (component, message)
        existing = groups.get(key)
        if existing is None:
            groups[key] = {
                "component": component or "unknown",
                "signature": message,
                "count": 1,
                "firstTime": _display_value(item.get("timeLabel"), default="시간미상"),
                "lastTime": _display_value(item.get("timeLabel"), default="시간미상"),
                "levels": [str(item.get("level") or "").strip()],
                "sampleLines": [str(item.get("raw") or "").strip()],
            }
            continue

        existing["count"] = int(existing["count"]) + 1
        existing["lastTime"] = _display_value(item.get("timeLabel"), default="시간미상")
        level = str(item.get("level") or "").strip()
        if level and level not in existing["levels"]:
            existing["levels"].append(level)
        raw = str(item.get("raw") or "").strip()
        if raw and raw not in existing["sampleLines"] and len(existing["sampleLines"]) < 3:
            existing["sampleLines"].append(raw)

    ordered = sorted(
        groups.values(),
        key=lambda item: (-int(item["count"]), str(item["component"]), str(item["signature"])),
    )
    return ordered[:12]


def _build_log_analysis_record(
    *,
    device_name: str,
    hospital_name: str,
    room_name: str,
    log_key: str,
    log_date: str,
    line_count: int,
    sessions: list[dict[str, Any]],
    session_scans: list[dict[str, Any]],
    session_motions: list[dict[str, Any]],
    session_restarts: list[dict[str, Any]],
    session_error_lines: list[tuple[int, str]],
) -> dict[str, Any]:
    closure = _session_closure_counts(sessions)
    scan_items = _serialize_scan_events_for_evidence(session_scans)
    motion_items = _serialize_motion_events_for_evidence(session_motions)
    restart_items = _serialize_restart_events_for_evidence(session_restarts)
    error_items = _serialize_error_lines_for_evidence(session_error_lines)
    return {
        "deviceName": device_name,
        "hospitalName": hospital_name,
        "roomName": room_name,
        "date": log_date,
        "logKey": log_key,
        "lineCount": int(line_count),
        "sessions": closure,
        "scanEventCount": len(scan_items),
        "scanEvents": scan_items,
        "motionEvents": motion_items,
        "restartEventCount": len(restart_items),
        "restartDetected": len(restart_items) > 0,
        "restartEvents": restart_items,
        "errorLineCount": len(error_items),
        "errorLines": error_items,
        "errorGroups": _build_error_groups(error_items),
    }


def _build_log_analysis_payload(
    *,
    mode: str,
    barcode: str,
    request_date: str | None,
    date_range: str | None,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    all_error_items: list[dict[str, Any]] = []
    total_sessions = 0
    total_abnormal = 0
    total_scan_events = 0
    total_restart_events = 0
    for record in records:
        all_error_items.extend(record.get("errorLines") or [])
        sessions = record.get("sessions") or {}
        total_sessions += int(sessions.get("sessionCount") or 0)
        total_abnormal += int(sessions.get("abnormalCount") or 0)
        total_scan_events += int(record.get("scanEventCount") or 0)
        total_restart_events += int(record.get("restartEventCount") or 0)

    return {
        "route": "barcode_log_error_summary",
        "source": "box_db+s3",
        "request": {
            "mode": mode,
            "barcode": barcode,
            "date": request_date,
            "dateRange": date_range,
        },
        "summary": {
            "recordCount": len(records),
            "sessionCount": total_sessions,
            "abnormalSessionCount": total_abnormal,
            "scanEventCount": total_scan_events,
            "restartEventCount": total_restart_events,
            "errorLineCount": len(all_error_items),
            "errorGroupCount": len(_build_error_groups(all_error_items)),
        },
        "records": records,
        "errorGroups": _build_error_groups(all_error_items),
    }


def _append_session_summaries(
    lines: list[str],
    barcode: str,
    sessions: list[dict[str, Any]],
    scan_events: list[dict[str, Any]],
    motion_events: list[dict[str, Any]],
) -> None:
    lines.append(f"• 요청 바코드 세션 상세: *{len(sessions)}건*")
    if not sessions:
        lines.append("- 없음")
        return

    max_sessions = max(1, min(50, cs.LOG_SCAN_MAX_EVENTS))
    display_sessions = sessions[-max_sessions:]
    if len(sessions) > len(display_sessions):
        lines.append(f"• 참고: 세션이 많아서 최근 `{len(display_sessions)}건`만 표시해")

    start_index = len(sessions) - len(display_sessions) + 1
    for index, session in enumerate(display_sessions, start=start_index):
        start_time = _display_value(session.get("start_time_label"), default="시간미상")
        stop_time = _display_value(session.get("stop_time_label"), default="미확인")
        session_scan_events = _events_in_session(scan_events, session)
        session_motion_events = _events_in_session(motion_events, session)
        motion_summary = _summarize_motion_session(session_motion_events)

        lines.append(
            f"- 세션 {index}: 시작 `{start_time}`, 종료 `{stop_time}`, scanned `{len(session_scan_events)}건`, "
            f"모션 시작 `{motion_summary['start_time']}`, 모션 종료 `{motion_summary['end_time']}`, "
            f"모션 성공 `{motion_summary['success']}`"
        )
        if motion_summary["stop_status"] != "미확인":
            lines.append(f"  모션 종료 상태: `{motion_summary['stop_status']}`")


def _line_in_any_session(line_no: int, sessions: list[dict[str, Any]]) -> bool:
    for session in sessions:
        if int(session["start_line_no"]) <= line_no <= int(session["end_line_no"]):
            return True
    return False


def _to_local_date(value: object) -> date | None:
    if not isinstance(value, datetime):
        return None

    tz_name = os.getenv("TZ", "Asia/Seoul")
    try:
        local_tz = ZoneInfo(tz_name)
    except Exception:
        local_tz = ZoneInfo("Asia/Seoul")

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(local_tz).date()


def _build_phase2_scope_request_message(
    barcode: str,
    reason: str,
    title: str,
) -> str:
    return "\n".join(
        [
            title,
            f"• 바코드: `{barcode}`",
            f"• 사유: {reason}",
            "• 2차 조회를 위해 아래 3가지를 같이 입력해줘:",
            "- 병원명 (MDA에 표시된 정확한 이름)",
            "- 병실명 (MDA에 표시된 정확한 이름)",
            "- 날짜(KST, YYYY-MM-DD)",
            f"예: `{barcode} 병원명 세화병원(부산) 병실명 7진료실 2026-01-30 로그 분석`",
        ]
    )


def _extract_hospital_room_scope(question: str) -> tuple[str | None, str | None]:
    text = (question or "").strip()
    hospital_match = _HOSPITAL_SCOPE_PATTERN.search(text)
    room_match = _ROOM_SCOPE_PATTERN.search(text)

    def _clean(value: str) -> str:
        normalized = " ".join(value.split()).strip().strip("`'\"")
        normalized = re.sub(r"\s+\d{2,4}[./-]\d{1,2}[./-]\d{1,2}\s*$", "", normalized)
        normalized = re.sub(r"\s+\d{1,2}\s*월\s*\d{1,2}\s*일\s*$", "", normalized)
        normalized = re.sub(r"\s*(?:로그|분석)\s*$", "", normalized)
        return normalized.strip()

    hospital_name = _clean(hospital_match.group(1)) if hospital_match else ""
    room_name = _clean(room_match.group(1)) if room_match else ""
    return (hospital_name or None, room_name or None)


def _extract_phase1_date_window(recordings_context: dict[str, Any]) -> tuple[date, date] | None:
    summary = recordings_context.get("summary") or {}
    last_recorded_at = summary.get("lastRecordedAt")
    last_recorded_date = _to_local_date(last_recorded_at)
    if last_recorded_date is None:
        return None

    today = _current_local_date()
    if last_recorded_date > today:
        last_recorded_date = today
    return last_recorded_date, today


def _iter_date_labels(start_date: date, end_date: date) -> list[str]:
    if start_date > end_date:
        return []

    labels: list[str] = []
    cursor = start_date
    while cursor <= end_date:
        labels.append(cursor.strftime("%Y-%m-%d"))
        cursor += timedelta(days=1)
    return labels


def _append_scan_events_section(
    lines: list[str],
    scan_events: list[dict[str, Any]],
    motion_events: list[dict[str, Any]] | None = None,
) -> None:
    ordered_scans = sorted(scan_events, key=lambda item: int(item.get("line_no") or 0))
    ordered_motions = sorted((motion_events or []), key=lambda item: int(item.get("line_no") or 0))

    timeline: list[tuple[int, str, str, str]] = []
    for event in ordered_scans:
        time_label = _display_value(event.get("time_label"), default="시간미상")
        raw_line = _display_value(event.get("raw_line"), default="Scanned 이벤트")
        timeline.append((int(event.get("line_no") or 0), time_label, raw_line, ""))

    for event in ordered_motions:
        time_label = _display_value(event.get("time_label"), default="시간미상")
        label = _display_value(event.get("label"), default="모션 이벤트")
        detail = ""
        if event.get("event_type") == "motion_stop":
            motion_detected = event.get("motion_detected")
            error_flag = event.get("error")
            if motion_detected is not None and error_flag is not None:
                detail = (
                    f"motionDetected={str(bool(motion_detected)).lower()}, "
                    f"error={str(bool(error_flag)).lower()}"
                )
        timeline.append((int(event.get("line_no") or 0), time_label, label, detail))

    if not timeline:
        lines.append("• scanned 이벤트: 없음")
        return

    lines.append(f"• scanned 이벤트: *{len(ordered_scans)}건*")

    timeline_rows: list[str] = []
    for _, time_label, label, detail in sorted(timeline, key=lambda item: item[0]):
        base = f"{time_label:>8}  {label}"
        if detail:
            base = f"{base} | {detail}"
        timeline_rows.append(base)

    # Slack mrkdwn can mis-render a fence immediately after a bullet line.
    lines.append("")
    lines.append("```")
    lines.extend(timeline_rows)
    lines.append("```")


def _append_error_lines_section(
    lines: list[str],
    error_lines: list[tuple[int, str]],
    *,
    show_all: bool = False,
) -> None:
    label = "• error 라인"
    if not error_lines:
        lines.append(f"{label}: 없음")
        return
    lines.append(f"{label}: *{len(error_lines)}줄*")

    display_error_lines = error_lines
    if not show_all:
        sample_limit = max(1, min(50, cs.LOG_ANALYSIS_MAX_SAMPLES * 5))
        display_error_lines = error_lines[-sample_limit:]
        if len(error_lines) > len(display_error_lines):
            lines.append(f"• 참고: error 라인이 많아서 최근 `{len(display_error_lines)}줄`만 표시해")

    rows: list[str] = []
    for line_no, content in display_error_lines:
        time_label = _extract_time_label_from_line(content)
        sample = content.strip()
        if len(sample) > 220:
            sample = sample[:220] + "...(truncated)"
        rows.append(f"{time_label:>8}  [{line_no}] {sample}")

    # Keep the fence detached from the bullet header for stable Slack rendering.
    lines.append("")
    lines.append("```")
    lines.extend(rows)
    lines.append("```")


def _append_restart_events_section(
    lines: list[str],
    restart_events: list[dict[str, Any]],
) -> None:
    if not restart_events:
        return

    lines.append(f"• 세션 중 재시작 감지: *{len(restart_events)}건* (정상 녹화 실패로 판단)")

    rows: list[str] = []
    for event in sorted(restart_events, key=lambda item: int(item.get("line_no") or 0)):
        time_label = _display_value(event.get("time_label"), default="시간미상")
        raw_line = _display_value(event.get("raw_line"), default="Mommybox Starting")
        details = event.get("details") or {}
        detail_parts: list[str] = []
        if details.get("appVersion"):
            detail_parts.append(f"App Version {details['appVersion']}")
        if details.get("startTime"):
            detail_parts.append(f"Start Time {details['startTime']}")
        detail_text = f" | {' | '.join(detail_parts)}" if detail_parts else ""
        rows.append(f"{time_label:>8}  {raw_line}{detail_text}")

    lines.append("")
    lines.append("```")
    lines.extend(rows)
    lines.append("```")


def _analyze_barcode_log_phase1_window(
    s3_client: Any,
    barcode: str,
    recordings_context: dict[str, Any],
    max_days: int,
) -> tuple[str, dict[str, Any]]:
    title = "*바코드 로그 분석 결과 (1차 자동 범위)*"
    summary = recordings_context.get("summary") or {}
    recording_count = int(summary.get("recordingCount") or 0)
    if recording_count <= 0:
        result_text = _build_phase2_scope_request_message(
            barcode,
            "recordings 데이터가 없어 자동 범위를 계산할 수 없어",
            title,
        )
        return result_text, _build_log_analysis_payload(
            mode="phase1_window",
            barcode=barcode,
            request_date=None,
            date_range=None,
            records=[],
        )

    date_window = _extract_phase1_date_window(recordings_context)
    if date_window is None:
        result_text = _build_phase2_scope_request_message(
            barcode,
            "마지막 recordedAt 정보를 찾지 못했어",
            title,
        )
        return result_text, _build_log_analysis_payload(
            mode="phase1_window",
            barcode=barcode,
            request_date=None,
            date_range=None,
            records=[],
        )
    start_date, end_date = date_window
    day_span = (end_date - start_date).days + 1
    bounded_max_days = max(1, max_days)
    if day_span > bounded_max_days:
        result_text = _build_phase2_scope_request_message(
            barcode,
            (
                f"1차 범위가 `{day_span}일`(시작 `{start_date:%Y-%m-%d}`)이라 "
                f"상한 `{bounded_max_days}일`을 초과했어"
            ),
            title,
        )
        return result_text, _build_log_analysis_payload(
            mode="phase1_window",
            barcode=barcode,
            request_date=None,
            date_range=f"{start_date:%Y-%m-%d} ~ {end_date:%Y-%m-%d}",
            records=[],
        )

    device_contexts = _lookup_device_contexts_by_barcode(
        barcode,
        recordings_context=recordings_context,
    )
    if not device_contexts:
        result_text = _build_phase2_scope_request_message(
            barcode,
            "장비 매핑 정보를 찾지 못했어",
            title,
        )
        return result_text, _build_log_analysis_payload(
            mode="phase1_window",
            barcode=barcode,
            request_date=None,
            date_range=f"{start_date:%Y-%m-%d} ~ {end_date:%Y-%m-%d}",
            records=[],
        )

    max_devices = max(1, min(20, cs.LOG_ANALYSIS_MAX_DEVICES))
    target_device_contexts = device_contexts[:max_devices]
    omitted_device_count = max(0, len(device_contexts) - len(target_device_contexts))
    target_date_labels = _iter_date_labels(start_date, end_date)

    found_log_files = 0
    matched_scope_count = 0
    total_sessions = 0
    analysis_records: list[dict[str, Any]] = []
    lines = [
        title,
        f"• 바코드: `{barcode}`",
        f"• 분석 범위(KST): `{start_date:%Y-%m-%d}` ~ `{end_date:%Y-%m-%d}` (`{day_span}일`)",
        f"• 매핑 장비: `{len(device_contexts)}개`",
    ]
    if omitted_device_count > 0:
        lines.append(f"• 참고: 장비가 많아서 상위 `{len(target_device_contexts)}개`만 분석했어")

    for date_label in target_date_labels:
        for device_context in target_device_contexts:
            device_name = str(device_context.get("deviceName") or "")
            if not device_name:
                continue

            log_data = _fetch_s3_device_log_lines(
                s3_client,
                device_name,
                date_label,
                tail_only=False,
            )
            if not log_data["found"]:
                continue

            found_log_files += 1
            source_lines = log_data["lines"]
            events = _extract_scan_events_with_line_no(source_lines)
            motion_events = _extract_motion_events_with_line_no(source_lines)
            restart_events = _extract_restart_events_with_line_no(source_lines)
            sessions = _extract_recording_sessions(
                source_lines,
                barcode,
                cs.LOG_SESSION_SAFETY_LINES,
                scan_events=events,
            )
            if not sessions:
                continue

            matched_scope_count += 1
            total_sessions += len(sessions)
            error_lines = _find_error_lines(source_lines)
            session_events = [
                event
                for event in events
                if _line_in_any_session(int(event["line_no"]), sessions)
            ]
            session_motion_events = [
                event
                for event in motion_events
                if _line_in_any_session(int(event["line_no"]), sessions)
            ]
            session_restart_events = [
                event
                for event in restart_events
                if _line_in_any_session(int(event["line_no"]), sessions)
            ]
            raw_session_error_lines = [
                (line_no, content)
                for (line_no, content) in error_lines
                if _line_in_any_session(line_no, sessions)
            ]
            session_error_lines = raw_session_error_lines

            hospital_name = _display_value(device_context.get("hospitalName"), default="미확인")
            room_name = _display_value(device_context.get("roomName"), default="미확인")
            analysis_records.append(
                _build_log_analysis_record(
                    device_name=device_name,
                    hospital_name=hospital_name,
                    room_name=room_name,
                    log_key=str(log_data["key"]),
                    log_date=date_label,
                    line_count=len(source_lines),
                    sessions=sessions,
                    session_scans=session_events,
                    session_motions=session_motion_events,
                    session_restarts=session_restart_events,
                    session_error_lines=session_error_lines,
                )
            )

            lines.append("")
            lines.append(f"*장비 `{device_name}` | 날짜 `{date_label}`*")
            lines.append(f"• 병원: `{hospital_name}`")
            lines.append(f"• 병실: `{room_name}`")
            lines.append(f"• 요청 바코드 녹화 세션: *{len(sessions)}건*")
            _append_session_closure_status(lines, sessions)
            _append_restart_events_section(lines, session_restart_events)
            _append_scan_events_section(lines, session_events, session_motion_events)
            _append_error_lines_section(
                lines,
                session_error_lines,
                show_all=True,
            )

    if found_log_files == 0:
        result_text = (
            f"{title}\n"
            f"• 바코드: `{barcode}`\n"
            f"• 분석 범위(KST): `{start_date:%Y-%m-%d}` ~ `{end_date:%Y-%m-%d}` (`{day_span}일`)\n"
            f"• 매핑 장비: `{len(device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`\n"
            "*요약*: 범위 내 로그 파일을 찾지 못했어"
        )
        return result_text, _build_log_analysis_payload(
            mode="phase1_window",
            barcode=barcode,
            request_date=None,
            date_range=f"{start_date:%Y-%m-%d} ~ {end_date:%Y-%m-%d}",
            records=[],
        )

    lines.append("")
    lines.append(f"• 확인한 로그 파일: `{found_log_files}개`")
    if matched_scope_count > 0:
        lines.append(f"• 요청 바코드 세션이 확인된 로그 범위: `{matched_scope_count}개`")
        lines.append(f"*요약*: 범위 내 요청 바코드 녹화 세션 `{total_sessions}건`을 찾았어")
    else:
        lines.append("• 요청 바코드 세션이 확인된 로그 범위: `0개`")
        lines.append("*요약*: 범위 내 로그는 확인했지만 요청 바코드 세션은 찾지 못했어")

    max_result_chars = max(s.S3_QUERY_MAX_RESULT_CHARS, 38000)
    result_text = _truncate_text("\n".join(lines), max_result_chars)
    return result_text, _build_log_analysis_payload(
        mode="phase1_window",
        barcode=barcode,
        request_date=None,
        date_range=f"{start_date:%Y-%m-%d} ~ {end_date:%Y-%m-%d}",
        records=analysis_records,
    )


def _analyze_barcode_log_scan_events(
    s3_client: Any,
    barcode: str,
    log_date: str,
    recordings_context: dict[str, Any] | None = None,
    device_contexts: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    all_device_contexts = device_contexts
    if all_device_contexts is None:
        all_device_contexts = _lookup_device_contexts_by_barcode(
            barcode,
            recordings_context=recordings_context,
        )

    if not all_device_contexts:
        result_text = (
            "*바코드 로그 스캔 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• recordings/devices에서 매핑된 장비명을 찾지 못했어"
        )
        return result_text, _build_log_analysis_payload(
            mode="scan",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )

    max_devices = max(1, min(20, cs.LOG_ANALYSIS_MAX_DEVICES))
    target_device_contexts = all_device_contexts[:max_devices]
    omitted_device_count = max(0, len(all_device_contexts) - len(target_device_contexts))
    total_session_count = 0
    logs_found_any = 0
    logs_with_session = 0
    devices_with_session = 0
    analysis_records: list[dict[str, Any]] = []

    lines = [
        "*바코드 로그 스캔 분석 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(all_device_contexts)}개`",
    ]
    if omitted_device_count > 0:
        lines.append(f"• 참고: 장비가 많아서 상위 `{len(target_device_contexts)}개`만 분석했어")

    for device_context in target_device_contexts:
        device_name = str(device_context.get("deviceName") or "")
        if not device_name:
            continue

        log_data = _fetch_s3_device_log_lines(
            s3_client,
            device_name,
            log_date,
            tail_only=False,
        )

        if not log_data["found"]:
            # 요청한 정책: 로그가 없는 장비는 응답에서 제외
            continue

        source_lines = log_data["lines"]
        logs_found_any += 1
        events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        restart_events = _extract_restart_events_with_line_no(source_lines)
        error_lines = _find_error_lines(source_lines)
        sessions = _extract_recording_sessions(
            source_lines,
            barcode,
            cs.LOG_SESSION_SAFETY_LINES,
            scan_events=events,
        )
        session_count = len(sessions)
        total_session_count += session_count
        session_scoped_events = _events_in_sessions(events, sessions)
        session_motion_events = _events_in_sessions(motion_events, sessions)
        session_restart_events = _events_in_sessions(restart_events, sessions)
        raw_session_error_lines = _error_lines_in_sessions(error_lines, sessions)
        session_error_lines = raw_session_error_lines

        if session_count == 0:
            continue

        logs_with_session += 1
        lines.append("")
        lines.append(f"• 매핑 장비: `{device_name}`")

        hospital_name = _display_value(device_context.get("hospitalName"), default="미확인")
        room_name = _display_value(device_context.get("roomName"), default="미확인")
        analysis_records.append(
            _build_log_analysis_record(
                device_name=device_name,
                hospital_name=hospital_name,
                room_name=room_name,
                log_key=str(log_data["key"]),
                log_date=log_date,
                line_count=len(source_lines),
                sessions=sessions,
                session_scans=session_scoped_events,
                session_motions=session_motion_events,
                session_restarts=session_restart_events,
                session_error_lines=session_error_lines,
            )
        )

        lines.append(f"• 파일: `{log_data['key']}`")
        lines.append(f"• 병원: `{hospital_name}`")
        lines.append(f"• 병실: `{room_name}`")
        lines.append(f"• 날짜: `{log_date}`")
        lines.append(f"• 분석 범위: 전체 `{len(source_lines)}줄`")
        _append_session_closure_status(lines, sessions)
        _append_restart_events_section(lines, session_restart_events)
        _append_scan_events_section(lines, session_scoped_events, session_motion_events)
        _append_error_lines_section(
            lines,
            session_error_lines,
            show_all=True,
        )
        lines.append(f"• 요청 바코드 녹화 세션: *{session_count}건*")

        devices_with_session += 1

    if logs_found_any == 0:
        result_text = (
            "*바코드 로그 스캔 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`\n"
            "*요약*: 요청 날짜의 로그 파일을 찾지 못했어"
        )
        return result_text, _build_log_analysis_payload(
            mode="scan",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )

    if logs_with_session == 0:
        result_text = (
            "*바코드 로그 스캔 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            f"• 확인한 로그 파일: `{logs_found_any}개`\n"
            "• 요청 바코드 세션이 확인된 장비: `0개`\n"
            f"*요약*: 로그 파일은 확인했지만 요청 바코드 `{barcode}` 세션은 찾지 못했어"
        )
        return result_text, _build_log_analysis_payload(
            mode="scan",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )

    lines.append("")
    lines.append(f"• 확인한 로그 파일: `{logs_with_session}개`")
    lines.append(f"• 요청 바코드 세션이 확인된 장비: `{devices_with_session}개`")
    if total_session_count > 0:
        lines.append(f"*요약*: 분석 범위에서 요청 바코드 녹화 세션 `{total_session_count}건`을 찾았어")
    else:
        lines.append(f"*요약*: 로그 파일은 확인했지만 요청 바코드 `{barcode}` 세션은 찾지 못했어")

    max_result_chars = max(s.S3_QUERY_MAX_RESULT_CHARS, 38000)
    result_text = _truncate_text("\n".join(lines), max_result_chars)
    return result_text, _build_log_analysis_payload(
        mode="scan",
        barcode=barcode,
        request_date=log_date,
        date_range=None,
        records=analysis_records,
    )


def _analyze_barcode_log_errors(
    s3_client: Any,
    barcode: str,
    log_date: str,
    recordings_context: dict[str, Any] | None = None,
    device_contexts: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    all_device_contexts = device_contexts
    if all_device_contexts is None:
        all_device_contexts = _lookup_device_contexts_by_barcode(
            barcode,
            recordings_context=recordings_context,
        )

    if not all_device_contexts:
        result_text = (
            "*바코드 로그 에러 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• recordings/devices에서 매핑된 장비명을 찾지 못했어"
        )
        return result_text, _build_log_analysis_payload(
            mode="error",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )

    max_devices = max(1, min(20, cs.LOG_ANALYSIS_MAX_DEVICES))
    target_device_contexts = all_device_contexts[:max_devices]
    omitted_device_count = max(0, len(all_device_contexts) - len(target_device_contexts))

    total_session_error_lines = 0
    logs_found_any = 0
    logs_with_session = 0
    total_session_count = 0
    devices_with_session = 0
    analysis_records: list[dict[str, Any]] = []
    lines = [
        "*바코드 로그 에러 분석 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(all_device_contexts)}개`",
    ]
    if omitted_device_count > 0:
        lines.append(f"• 참고: 장비가 많아서 상위 `{len(target_device_contexts)}개`만 분석했어")

    for device_context in target_device_contexts:
        device_name = str(device_context.get("deviceName") or "")
        if not device_name:
            continue

        log_data = _fetch_s3_device_log_lines(
            s3_client,
            device_name,
            log_date,
            tail_only=False,
        )

        if not log_data["found"]:
            # 요청한 정책: 로그가 없는 장비는 응답에서 제외
            continue

        source_lines = log_data["lines"]
        logs_found_any += 1
        events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        restart_events = _extract_restart_events_with_line_no(source_lines)
        sessions = _extract_recording_sessions(
            source_lines,
            barcode,
            cs.LOG_SESSION_SAFETY_LINES,
            scan_events=events,
        )
        session_count = len(sessions)
        total_session_count += session_count
        error_lines = _find_error_lines(source_lines)
        session_scoped_events = _events_in_sessions(events, sessions)
        session_motion_events = _events_in_sessions(motion_events, sessions)
        session_restart_events = _events_in_sessions(restart_events, sessions)
        raw_session_error_lines = _error_lines_in_sessions(error_lines, sessions)
        session_error_lines = raw_session_error_lines
        total_session_error_lines += len(session_error_lines)

        if session_count == 0:
            continue

        logs_with_session += 1
        lines.append("")
        lines.append(f"• 매핑 장비: `{device_name}`")

        hospital_name = _display_value(device_context.get("hospitalName"), default="미확인")
        room_name = _display_value(device_context.get("roomName"), default="미확인")
        analysis_records.append(
            _build_log_analysis_record(
                device_name=device_name,
                hospital_name=hospital_name,
                room_name=room_name,
                log_key=str(log_data["key"]),
                log_date=log_date,
                line_count=len(source_lines),
                sessions=sessions,
                session_scans=session_scoped_events,
                session_motions=session_motion_events,
                session_restarts=session_restart_events,
                session_error_lines=session_error_lines,
            )
        )

        lines.append(f"• 파일: `{log_data['key']}`")
        lines.append(f"• 병원: `{hospital_name}`")
        lines.append(f"• 병실: `{room_name}`")
        lines.append(f"• 날짜: `{log_date}`")
        lines.append(f"• 파일 크기: `{_format_size(log_data['content_length'])}`")
        lines.append(f"• 분석 범위: 전체 `{len(source_lines)}줄`")
        _append_session_closure_status(lines, sessions)
        _append_restart_events_section(lines, session_restart_events)
        _append_scan_events_section(lines, session_scoped_events, session_motion_events)
        _append_error_lines_section(
            lines,
            session_error_lines,
            show_all=True,
        )
        lines.append(f"• 요청 바코드 녹화 세션: *{session_count}건*")

        devices_with_session += 1

    if logs_found_any == 0:
        result_text = (
            "*바코드 로그 에러 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`\n"
            "*요약*: 요청 날짜의 로그 파일을 찾지 못했어"
        )
        return result_text, _build_log_analysis_payload(
            mode="error",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )

    if logs_with_session == 0:
        result_text = (
            "*바코드 로그 에러 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            f"• 확인한 로그 파일: `{logs_found_any}개`\n"
            "• 요청 바코드 세션이 확인된 장비: `0개`\n"
            f"*요약*: 로그 파일은 확인했지만 요청 바코드 `{barcode}` 세션은 찾지 못했어"
        )
        return result_text, _build_log_analysis_payload(
            mode="error",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )

    lines.append("")
    lines.append(f"• 확인한 로그 파일: `{logs_with_session}개`")
    lines.append(f"• 요청 바코드 세션이 확인된 장비: `{devices_with_session}개`")
    lines.append(f"• 세션 구간 error 라인: `{total_session_error_lines}줄`")
    if total_session_error_lines > 0:
        lines.append("*요약*: 요청 바코드 세션 구간에서 error 패턴 라인을 확인했어")
    else:
        lines.append("*요약*: 요청 바코드 세션 구간에서 error 패턴 라인을 찾지 못했어")

    max_result_chars = max(s.S3_QUERY_MAX_RESULT_CHARS, 38000)
    result_text = _truncate_text("\n".join(lines), max_result_chars)
    return result_text, _build_log_analysis_payload(
        mode="error",
        barcode=barcode,
        request_date=log_date,
        date_range=None,
        records=analysis_records,
    )
