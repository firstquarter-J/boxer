import os
import re
from datetime import date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.core.utils import _display_value, _format_size, _truncate_text
from boxer.routers.company.box_db import (
    _load_recordings_rows_on_date_by_barcode,
    _lookup_device_contexts_by_barcode,
    _lookup_device_contexts_by_hospital_seqs,
)
from boxer.routers.company.s3_domain import _fetch_s3_device_log_lines

_NUMERIC_YMD_PATTERN = re.compile(r"(?<!\d)(\d{2,4})\s*[-./]\s*(\d{1,2})\s*[-./]\s*(\d{1,2})(?!\d)")
_KOREAN_YMD_PATTERN = re.compile(
    r"(?<!\d)(\d{2,4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일(?!\d)"
)
_NUMERIC_MD_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*[./]\s*(\d{1,2})(?!\d)")
_NUMERIC_MD_DASH_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*-\s*(\d{1,2})(?!\d)")
_KOREAN_MD_PATTERN = re.compile(r"(?<!\d)(\d{1,2})\s*월\s*(\d{1,2})\s*일(?!\d)")
_COMPACT_YYYYMMDD_PATTERN = re.compile(r"(?<!\d)(20\d{2}|19\d{2})(\d{2})(\d{2})(?!\d)")
_COMPACT_YYMMDD_PATTERN = re.compile(r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)")
_COMPACT_MMDD_PATTERN = re.compile(r"(?<!\d)(\d{2})(\d{2})(?!\d)")
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
    r"(?:^|\s)병원(?:명)?\s*[:=]?\s*(.+?)(?=\s*(?:병실(?:명)?|진료실명|날짜|로그|분석)\s*[:=]?|$)"
)
_ROOM_SCOPE_PATTERN = re.compile(
    r"(?:^|[\s)])(?:병실(?:명)?|진료실명)\s*[:=]?\s*(.+?)(?=\s*(?:날짜|로그|분석)\s*[:=]?|$)"
)
_ROOM_TOKEN_PATTERN = re.compile(r"([^\s`'\",]*(?:진료실|병실)[^\s`'\",]*)")
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


def _dedupe_device_contexts_by_name(device_contexts: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen_device_names: set[str] = set()
    for device_context in device_contexts:
        device_name = _display_value(device_context.get("deviceName"), default="")
        if not device_name or device_name in seen_device_names:
            continue
        seen_device_names.add(device_name)
        items.append(device_context)
    return items


def _expand_device_contexts_to_recordings_hospital_scope(
    recordings_context: dict[str, Any] | None,
    existing_device_contexts: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if recordings_context is None:
        return []

    hospital_seqs: list[int] = []
    for row in recordings_context.get("rows") or []:
        hospital_seq = row.get("hospitalSeq")
        if hospital_seq is None:
            continue
        try:
            hospital_seqs.append(int(hospital_seq))
        except (TypeError, ValueError):
            continue

    expanded_contexts = _lookup_device_contexts_by_hospital_seqs(hospital_seqs)
    existing_names = {
        _display_value(item.get("deviceName"), default="")
        for item in existing_device_contexts
        if _display_value(item.get("deviceName"), default="")
    }
    additional_contexts = [
        item
        for item in expanded_contexts
        if _display_value(item.get("deviceName"), default="") not in existing_names
    ]
    return _dedupe_device_contexts_by_name(additional_contexts)


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
        ("numeric_md_dash", _NUMERIC_MD_DASH_PATTERN),
        ("compact_yyyymmdd", _COMPACT_YYYYMMDD_PATTERN),
        ("compact_yymmdd", _COMPACT_YYMMDD_PATTERN),
        ("compact_mmdd", _COMPACT_MMDD_PATTERN),
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

    if kind == "compact_yyyymmdd":
        year = int(matched.group(1))
        month = int(matched.group(2))
        day = int(matched.group(3))
        return True, _try_format_date(year, month, day)

    if kind == "compact_yymmdd":
        year = _normalize_year(int(matched.group(1)))
        month = int(matched.group(2))
        day = int(matched.group(3))
        return True, _try_format_date(year, month, day)

    local_year = _current_local_date().year
    month = int(matched.group(1))
    day = int(matched.group(2))
    return True, _try_format_date(local_year, month, day)


def _looks_like_unparsed_date_token(text: str) -> bool:
    stripped = (text or "").strip()
    if not stripped:
        return False

    patterns = (
        r"(?<!\d)\d{1,2}\s*-\s*\d{1,2}(?!\d)",
        r"(?<!\d)\d{4}(?!\d)",
        r"(?<!\d)\d{6}(?!\d)",
        r"(?<!\d)\d{8}(?!\d)",
        r"\d{1,2}\s*월",
        r"\d{1,2}\s*[./]\s*\d{1,2}",
    )
    return any(re.search(pattern, stripped) for pattern in patterns)


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
            raise ValueError("날짜 형식을 확인해줘. 예: 2026-03-03, 26.03.03, 3/3, 3월 3일, 02-20, 260220")
        return parsed_explicit_date, True

    base_date = _current_local_date()
    relative_offset = _extract_relative_day_offset(text, lowered)
    if relative_offset is not None:
        base_date = base_date + timedelta(days=relative_offset)
        return base_date.strftime("%Y-%m-%d"), True

    if _looks_like_unparsed_date_token(text):
        raise ValueError("날짜 형식을 확인해줘. 예: 2026-03-03, 26.03.03, 3/3, 3월 3일, 02-20, 260220")
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
    if _is_barcode_video_length_request(question, barcode):
        return False
    if _is_barcode_all_recorded_dates_request(question, barcode):
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


def _is_barcode_video_info_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False
    if _is_barcode_video_length_request(question, barcode):
        return False
    if _is_barcode_all_recorded_dates_request(question, barcode):
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

    return any(token in text for token in ("정보", "상세", "상세정보", "세부", "상태"))


def _is_barcode_video_length_request(question: str, barcode: str | None) -> bool:
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

    has_length_hint = any(token in text for token in ("길이", "재생시간", "재생 시간", "duration", "videoLength"))
    return has_length_hint


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
    has_per_video_hint = any(
        token in text
        for token in (
            "영상별",
            "비디오별",
            "동영상별",
            "녹화별",
            "촬영별",
            "각 영상",
            "각 녹화",
            "각 촬영",
            "영상마다",
            "녹화마다",
        )
    )
    has_date_list_phrase = any(
        token in text
        for token in (
            "날짜 목록",
            "날짜 리스트",
            "일자 목록",
            "일자 리스트",
            "날짜별 목록",
            "일자별 목록",
            "영상 날짜",
            "영상 날짜 목록",
            "영상 날짜별 목록",
            "영상별 날짜",
            "영상별 날짜 목록",
            "비디오 날짜",
            "비디오 날짜 목록",
            "녹화 날짜",
            "녹화 날짜 목록",
            "촬영 날짜",
            "촬영 날짜 목록",
        )
    )
    has_date_hint = any(token in text for token in ("날짜", "일자", "목록", "리스트")) or any(
        token in lowered for token in ("date", "dates", "list")
    )
    return has_date_hint and (has_all_hint or has_per_video_hint or has_date_list_phrase)


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
    motion_counter_active = False

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
            motion_counter_active = False
        elif "motion detection :" in lowered:
            if motion_counter_active:
                continue
            event_type = "motion_start"
            label = "모션 감지 시작"
            motion_counter_active = True
        elif "motion detection passed" in lowered:
            event_type = "motion_trigger"
            label = "모션 감지 성공(녹화 전환)"
            motion_counter_active = False
        elif (
            "motion detected for" in lowered
            and "stopping detection to start recording" in lowered
        ):
            event_type = "motion_trigger"
            label = "모션 감지 성공(녹화 전환)"
            motion_counter_active = False
        elif "stopping motion detection." in lowered:
            event_type = "motion_stop"
            label = "모션 감지 종료"
            matched = _MOTION_STOP_STATUS_PATTERN.search(line)
            if matched:
                motion_detected = matched.group(1).lower() == "true"
                error_flag = matched.group(2).lower() == "true"
            motion_counter_active = False
        else:
            if "motion detection" not in lowered:
                motion_counter_active = False
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
                "stop_token": None,
                "end_line_no": len(lines),
            }
            continue

        if upper_token in cs.SESSION_STOP_TOKENS and active is not None:
            active["stop_line_no"] = line_no
            active["stop_time_label"] = time_label
            active["stop_token"] = upper_token
            active["end_line_no"] = min(len(lines), line_no + safe_extra)
            sessions.append(active)
            active = None

    if active is not None:
        active["end_line_no"] = len(lines)
        sessions.append(active)

    return sessions


def _format_session_stop_marker(session: dict[str, Any]) -> str:
    token = str(session.get("stop_token") or "").strip().upper()
    if not token:
        return "종료 스캔"
    return f"`{token}`"


def _append_session_state_summary(
    lines: list[str],
    source_lines: list[str],
    sessions: list[dict[str, Any]],
    restart_events: list[dict[str, Any]],
    session_error_lines: list[tuple[int, str]] | None = None,
    diagnostic_scan_events: list[dict[str, Any]] | None = None,
    recordings_on_date_count: int | None = None,
) -> None:
    session_error_lines = session_error_lines or []
    diagnostic_scan_events = diagnostic_scan_events or []

    if not sessions and not restart_events:
        return
    if not sessions:
        if restart_events:
            lines.append("• *종료 상태:* 마미박스 비정상 종료 추정 (세션 중 재시작 로그만 확인됨)")
            lines.append("• *녹화 결과:* 정상 녹화 실패로 판단")
        return

    normal_count = 0
    stop_missing_count = 0
    reboot_count = 0

    for session in sessions:
        has_restart = any(
            int(session["start_line_no"]) <= int(event.get("line_no") or 0) <= int(session["end_line_no"])
            for event in restart_events
        )
        has_stop = session.get("stop_line_no") is not None

        if has_restart:
            reboot_count += 1
        elif has_stop:
            normal_count += 1
        else:
            stop_missing_count += 1

    if len(sessions) <= 1:
        session = sessions[0]
        if reboot_count > 0:
            lines.append("• *종료 상태:* 마미박스 비정상 종료 (세션 중 재시작 감지)")
            lines.append("• *녹화 결과:* 정상 녹화 실패로 판단")
            return
        if stop_missing_count > 0:
            lines.append("• *종료 상태:* 비정상 종료 (종료 스캔 없음)")
            lines.append("• *녹화 결과:* 정상 녹화 실패로 판단")
            return
        lines.append(f"• *종료 상태:* 정상 종료 ({_format_session_stop_marker(session)} 확인)")
        recording_result, recovery_context, post_stop_context = _build_session_recording_result_text(
            source_lines,
            session,
            restart_events,
            session_error_lines,
            diagnostic_scan_events,
            recordings_on_date_count=recordings_on_date_count,
        )
        lines.append(f"• *녹화 결과:* {recording_result}")
        if isinstance(post_stop_context, dict):
            post_stop_text = str(post_stop_context.get("displayText") or "").strip()
            if post_stop_text:
                lines.append(f"• 종료 후 이상 징후: {post_stop_text}")
        if recovery_context is not None:
            recovery_parts: list[str] = []
            started_recording = recovery_context.get("startedRecording") if isinstance(recovery_context, dict) else None
            spawned_recording = recovery_context.get("spawnedRecordingFfmpeg") if isinstance(recovery_context, dict) else None
            if isinstance(started_recording, dict):
                recovery_parts.append(
                    f"Started recording `{_display_value(started_recording.get('timeLabel'), default='시간미상')}`"
                )
            if isinstance(spawned_recording, dict):
                recovery_parts.append(
                    f"RECORDING ffmpeg 시작 `{_display_value(spawned_recording.get('timeLabel'), default='시간미상')}`"
                )
            if recovery_parts:
                lines.append(f"• 녹화 시작 로그: {', '.join(recovery_parts)}")
        return

    termination_parts: list[str] = []
    outcome_parts: list[str] = []
    if normal_count > 0:
        termination_parts.append(f"정상 종료 *{normal_count}건*")
    if stop_missing_count > 0:
        termination_parts.append(f"비정상 종료 *{stop_missing_count}건* (종료 스캔 없음)")
    if reboot_count > 0:
        termination_parts.append(f"마미박스 비정상 종료 *{reboot_count}건* (세션 중 재시작 감지)")

    if reboot_count > 0:
        outcome_parts.append(f"정상 녹화 실패 *{reboot_count}건* (세션 중 재시작 감지)")
    if stop_missing_count > 0:
        outcome_parts.append(f"정상 녹화 실패 *{stop_missing_count}건* (종료 스캔 없음)")

    ffmpeg_affected_count = 0
    severe_post_stop_count = 0
    for session in sessions:
        session_error_subset = _error_lines_in_session(session_error_lines, session)
        session_ffmpeg_error = _find_first_ffmpeg_error_context(
            session_error_subset,
            [session],
        )
        _, _, post_stop_context = _build_session_recording_result_text(
            source_lines,
            session,
            restart_events,
            session_error_subset,
            diagnostic_scan_events,
        )
        has_restart = any(
            int(session["start_line_no"]) <= int(event.get("line_no") or 0) <= int(session["end_line_no"])
            for event in restart_events
        )
        has_stop = session.get("stop_line_no") is not None
        if session_ffmpeg_error is not None and not has_restart and has_stop:
            ffmpeg_affected_count += 1
        if isinstance(post_stop_context, dict) and str(post_stop_context.get("severity") or "") == "high":
            severe_post_stop_count += 1

    if severe_post_stop_count > 0:
        outcome_parts.append(f"영상 손상 가능성 높음 *{severe_post_stop_count}건* (종료 후 처리 이상)")
    if ffmpeg_affected_count > severe_post_stop_count:
        outcome_parts.append(f"영상 손상 가능성 의심 *{ffmpeg_affected_count - severe_post_stop_count}건* (ffmpeg 오류)")

    if not termination_parts:
        lines.append("• *종료 상태:* 판단 불가")
        return

    lines.append(f"• *종료 상태:* {', '.join(termination_parts)}")
    if outcome_parts:
        lines.append(f"• *녹화 결과:* {', '.join(outcome_parts)}")


def _events_in_session(events: list[dict[str, Any]], session: dict[str, Any]) -> list[dict[str, Any]]:
    start_line_no = int(session["start_line_no"])
    end_line_no = int(session["end_line_no"])
    return [
        event
        for event in events
        if start_line_no <= int(event["line_no"]) <= end_line_no
    ]


def _find_session_for_line(line_no: int, sessions: list[dict[str, Any]]) -> dict[str, Any] | None:
    for session in sessions:
        if int(session["start_line_no"]) <= int(line_no) <= int(session["end_line_no"]):
            return session
    return None


def _time_label_to_seconds(label: str) -> int | None:
    try:
        hour, minute, second = [int(part) for part in str(label).strip().split(":")]
    except (TypeError, ValueError):
        return None
    if hour < 0 or minute < 0 or minute >= 60 or second < 0 or second >= 60:
        return None
    return hour * 3600 + minute * 60 + second


def _format_elapsed_seconds(total_seconds: int | None) -> str | None:
    if total_seconds is None or total_seconds < 0:
        return None
    minutes, seconds = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours}시간 {minutes}분 {seconds}초"
    if minutes > 0:
        return f"{minutes}분 {seconds}초"
    return f"{seconds}초"


def _find_first_ffmpeg_error_context(
    error_lines: list[tuple[int, str]],
    sessions: list[dict[str, Any]],
) -> dict[str, Any] | None:
    for line_no, content in error_lines:
        parsed = _parse_structured_log_line(content)
        raw = str(parsed.get("raw") or content or "").strip()
        if "ffmpeg" not in raw.lower():
            continue

        error_time = _extract_time_label_from_line(content)
        session = _find_session_for_line(line_no, sessions)
        session_start_time = _display_value(
            session.get("start_time_label") if isinstance(session, dict) else None,
            default="미확인",
        )
        elapsed = None
        start_seconds = _time_label_to_seconds(session_start_time)
        error_seconds = _time_label_to_seconds(error_time)
        if start_seconds is not None and error_seconds is not None:
            elapsed = _format_elapsed_seconds(error_seconds - start_seconds)

        return {
            "lineNo": int(line_no),
            "timeLabel": error_time,
            "component": parsed.get("component") or "unknown",
            "message": parsed.get("message") or raw,
            "raw": raw,
            "sessionStartTime": session_start_time,
            "elapsedFromSessionStart": elapsed,
        }
    return None


def _find_recording_recovery_context(
    lines: list[str],
    session: dict[str, Any],
    after_line_no: int | None = None,
) -> dict[str, Any] | None:
    start_line_no = int(session["start_line_no"])
    end_line_no = int(session["end_line_no"])
    cursor = max(start_line_no, int(after_line_no) + 1 if after_line_no is not None else start_line_no)

    added_recording: dict[str, Any] | None = None
    started_recording: dict[str, Any] | None = None
    spawned_recording_ffmpeg: dict[str, Any] | None = None
    spawned_motion_ffmpeg: dict[str, Any] | None = None

    for line_no in range(cursor, end_line_no + 1):
        raw_line = lines[line_no - 1]
        stripped = _strip_leading_log_timestamp(raw_line)
        lowered = stripped.lower()
        time_label = _extract_time_label_from_line(raw_line)

        if added_recording is None and "addrecording(" in lowered:
            added_recording = {
                "lineNo": line_no,
                "timeLabel": time_label,
                "rawLine": stripped,
            }

        if started_recording is None and "started recording" in lowered:
            started_recording = {
                "lineNo": line_no,
                "timeLabel": time_label,
                "rawLine": stripped,
            }

        if spawned_recording_ffmpeg is None and "spawned recording ffmpeg" in lowered:
            spawned_recording_ffmpeg = {
                "lineNo": line_no,
                "timeLabel": time_label,
                "rawLine": stripped,
            }

        if spawned_motion_ffmpeg is None and "spawned motion ffmpeg" in lowered:
            spawned_motion_ffmpeg = {
                "lineNo": line_no,
                "timeLabel": time_label,
                "rawLine": stripped,
            }

        if started_recording is not None and spawned_recording_ffmpeg is not None:
            break

    if added_recording is None and started_recording is None and spawned_recording_ffmpeg is None and spawned_motion_ffmpeg is None:
        return None

    primary = started_recording or added_recording or spawned_recording_ffmpeg or spawned_motion_ffmpeg or {}
    return {
        "addedRecording": added_recording,
        "startedRecording": started_recording,
        "spawnedRecordingFfmpeg": spawned_recording_ffmpeg,
        "spawnedMotionFfmpeg": spawned_motion_ffmpeg,
        "fileId": _extract_file_id_from_recovery_events(
            added_recording,
            started_recording,
            spawned_recording_ffmpeg,
            spawned_motion_ffmpeg,
        ),
        "timeLabel": _display_value(primary.get("timeLabel"), default="시간미상"),
    }


def _extract_file_id_from_recovery_events(
    added_recording: dict[str, Any] | None,
    started_recording: dict[str, Any] | None,
    spawned_recording_ffmpeg: dict[str, Any] | None,
    spawned_motion_ffmpeg: dict[str, Any] | None = None,
) -> str | None:
    candidates = [
        str((added_recording or {}).get("rawLine") or "").strip(),
        str((started_recording or {}).get("rawLine") or "").strip(),
        str((spawned_recording_ffmpeg or {}).get("rawLine") or "").strip(),
        str((spawned_motion_ffmpeg or {}).get("rawLine") or "").strip(),
    ]
    patterns = (
        r"addrecording\(([a-z0-9]+)\)",
        r"started recording\s*:\s*([a-z0-9]+)",
        r"/Videos/([a-z0-9]+)\.mp4",
        r"/Videos/([a-z0-9]+)\.motion\.mp4",
    )
    for candidate in candidates:
        lowered = candidate.lower()
        for pattern in patterns:
            matched = re.search(pattern, lowered, re.IGNORECASE)
            if matched:
                return matched.group(1)
    return None


def _find_session_post_stop_context(
    lines: list[str],
    scan_events: list[dict[str, Any]],
    session: dict[str, Any],
    file_id: str | None,
) -> dict[str, Any] | None:
    stop_line_no = int(session.get("stop_line_no") or 0)
    if stop_line_no <= 0:
        return None

    stop_time = _display_value(session.get("stop_time_label"), default="미확인")
    next_barcode_line_no: int | None = None
    for event in scan_events:
        event_line_no = int(event.get("line_no") or 0)
        if event_line_no <= stop_line_no:
            continue
        token = str(event.get("token") or "").strip()
        if cs.BARCODE_PATTERN.fullmatch(token):
            next_barcode_line_no = event_line_no
            break

    upper_bound = min(
        len(lines),
        stop_line_no + max(1, cs.LOG_POST_STOP_MAX_LINES),
        (next_barcode_line_no - 1) if next_barcode_line_no else len(lines),
    )
    finish_line_no: int | None = None
    finish_time_label = "미확인"
    finish_count = 0
    file_id_lower = (file_id or "").lower().strip()

    for line_no in range(stop_line_no + 1, upper_bound + 1):
        raw_line = lines[line_no - 1]
        lowered = _strip_leading_log_timestamp(raw_line).lower()
        if file_id_lower:
            if f"finishrecording({file_id_lower}" not in lowered:
                continue
        elif "finishrecording(" not in lowered:
            continue

        finish_count += 1
        if finish_line_no is None:
            finish_line_no = line_no
            finish_time_label = _extract_time_label_from_line(raw_line)

    finish_delay_seconds: int | None = None
    finish_delay_label: str | None = None
    stop_seconds = _time_label_to_seconds(stop_time)
    finish_seconds = _time_label_to_seconds(finish_time_label)
    if stop_seconds is not None and finish_seconds is not None:
        finish_delay_seconds = finish_seconds - stop_seconds
        finish_delay_label = _format_elapsed_seconds(finish_delay_seconds)

    scan_upper_bound = min(finish_line_no or upper_bound, upper_bound)
    post_stop_scan_events = [
        event
        for event in scan_events
        if stop_line_no < int(event.get("line_no") or 0) < scan_upper_bound
    ]
    post_stop_stop_count = sum(
        1 for event in post_stop_scan_events if str(event.get("token") or "").strip().upper() == "C_STOPSESS"
    )
    post_stop_snap_count = sum(
        1
        for event in post_stop_scan_events
        if str(event.get("token") or "").strip().upper() == "SPECIAL_TAKE_SNAP"
    )

    device_error_upper_bound = min(len(lines), max(finish_line_no or upper_bound, upper_bound))
    post_stop_device_errors: list[dict[str, Any]] = []
    for line_no in range(stop_line_no + 1, device_error_upper_bound + 1):
        raw_line = lines[line_no - 1]
        stripped = _strip_leading_log_timestamp(raw_line)
        lowered = stripped.lower()
        explicit_level = _extract_explicit_log_level(raw_line)
        has_device_error = (
            (explicit_level in {"error", "fatal", "panic"} and (
                "/dev/video0" in lowered or "no such file or directory" in lowered
            ))
            or "videodevice : error" in lowered
            or "video device : error" in lowered
            or "/dev/video0 has been removed" in lowered
        )
        if not has_device_error:
            continue
        post_stop_device_errors.append(
            {
                "lineNo": line_no,
                "timeLabel": _extract_time_label_from_line(raw_line),
                "rawLine": stripped,
            }
        )

    abnormal_parts: list[str] = []
    if finish_delay_seconds is not None and finish_delay_seconds >= 30 and finish_delay_label:
        abnormal_parts.append(f"종료 처리 지연 `{finish_delay_label}`")
    if len(post_stop_device_errors) > 0:
        abnormal_parts.append(f"종료 후 장치 오류 `{len(post_stop_device_errors)}건`")
    if finish_count > 1:
        abnormal_parts.append(f"finishRecording 중복 `{finish_count}회`")

    severity = "normal"
    if abnormal_parts:
        severity = "high"
    elif finish_delay_seconds is not None and finish_delay_seconds >= 10:
        severity = "suspect"

    return {
        "stopTimeLabel": stop_time,
        "finishTimeLabel": finish_time_label,
        "finishLineNo": finish_line_no,
        "finishCount": finish_count,
        "finishDelaySeconds": finish_delay_seconds,
        "finishDelayLabel": finish_delay_label,
        "postStopScanCount": len(post_stop_scan_events),
        "postStopStopCount": post_stop_stop_count,
        "postStopSnapCount": post_stop_snap_count,
        "postStopDeviceErrorCount": len(post_stop_device_errors),
        "postStopDeviceErrors": post_stop_device_errors,
        "severity": severity,
        "displayText": ", ".join(abnormal_parts),
    }


def _build_session_recording_result_text(
    source_lines: list[str],
    session: dict[str, Any],
    restart_events: list[dict[str, Any]],
    session_error_lines: list[tuple[int, str]],
    scan_events: list[dict[str, Any]] | None = None,
    recordings_on_date_count: int | None = None,
) -> tuple[str, dict[str, Any] | None, dict[str, Any] | None]:
    has_restart = any(
        int(session["start_line_no"]) <= int(event.get("line_no") or 0) <= int(session["end_line_no"])
        for event in restart_events
    )
    has_stop = session.get("stop_line_no") is not None
    first_ffmpeg_error = _find_first_ffmpeg_error_context(session_error_lines, [session])
    has_standby_ffmpeg_error = any(
        "ffmpeg" in content.lower() and "standby error" in content.lower()
        for _, content in session_error_lines
    )
    recovery_context = (
        _find_recording_recovery_context(
            source_lines,
            session,
            after_line_no=int(first_ffmpeg_error.get("lineNo")) if isinstance(first_ffmpeg_error, dict) else None,
        )
        if has_standby_ffmpeg_error
        else None
    )
    post_stop_context = (
        _find_session_post_stop_context(
            source_lines,
            scan_events or [],
            session,
            str((recovery_context or {}).get("fileId") or "").strip() or None,
        )
        if has_stop
        else None
    )

    if has_restart:
        return "정상 녹화 실패로 판단", recovery_context, post_stop_context
    if not has_stop:
        return "정상 녹화 실패로 판단", recovery_context, post_stop_context
    if first_ffmpeg_error is not None:
        error_time = _display_value(first_ffmpeg_error.get("timeLabel"), default="시간미상")
        elapsed = _display_value(first_ffmpeg_error.get("elapsedFromSessionStart"), default="")
        if recordings_on_date_count == 0:
            detail_parts = [f"첫 ffmpeg 오류 `{error_time}`"]
            if elapsed:
                detail_parts.append(f"세션 시작 후 `{elapsed}`")
            if isinstance(post_stop_context, dict):
                anomaly_text = str(post_stop_context.get("displayText") or "").strip()
                if anomaly_text:
                    detail_parts.append(anomaly_text)
            detail_parts.append("날짜 기준 DB 영상 기록 없음")
            return (
                f"녹화 & 업로드 실패로 판단 ({', '.join(detail_parts)})",
                recovery_context,
                post_stop_context,
            )
        if isinstance(post_stop_context, dict) and str(post_stop_context.get("severity") or "") == "high":
            detail_parts = [f"첫 오류 `{error_time}`"]
            if elapsed:
                detail_parts.append(f"세션 시작 후 `{elapsed}`")
            anomaly_text = str(post_stop_context.get("displayText") or "").strip()
            if anomaly_text:
                detail_parts.append(anomaly_text)
            return (
                f"영상 손상 가능성 높음 ({', '.join(detail_parts)})",
                recovery_context,
                post_stop_context,
            )
        if has_standby_ffmpeg_error:
            detail_parts = [f"첫 ffmpeg 오류 `{error_time}`"]
            if elapsed:
                detail_parts.append(f"세션 시작 후 `{elapsed}`")
            return (
                f"영상 손상 가능성 의심 ({', '.join(detail_parts)}, 실제 영상 확인 필요)",
                recovery_context,
                post_stop_context,
            )

        detail_parts = [f"첫 ffmpeg 오류 `{error_time}`"]
        if elapsed:
            detail_parts.append(f"세션 시작 후 `{elapsed}`")
        return f"영상 손상 가능성 의심 ({', '.join(detail_parts)})", recovery_context, post_stop_context
    non_recording_network_context = _describe_non_recording_network_error_context(session_error_lines)
    if non_recording_network_context:
        if _has_uploader_network_error(session_error_lines) and recordings_on_date_count == 0:
            return (
                f"영상 업로드 실패 가능성 높음 ({non_recording_network_context}, 날짜 기준 DB 영상 기록 없음)",
                recovery_context,
                post_stop_context,
            )
        if recordings_on_date_count and recordings_on_date_count > 0:
            return (
                f"정상 녹화로 판단 (날짜 기준 DB 영상 기록 `{recordings_on_date_count}개` 확인, {non_recording_network_context} 별도)",
                recovery_context,
                post_stop_context,
            )
        return (
            f"정상 녹화로 판단 ({non_recording_network_context} 별도)",
            recovery_context,
            post_stop_context,
        )
    if session_error_lines:
        return f"이상 징후 있음 (error 라인 `{len(session_error_lines)}줄`)", recovery_context, post_stop_context
    return "정상 녹화로 판단", recovery_context, post_stop_context


def _events_in_sessions(events: list[dict[str, Any]], sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not sessions:
        return []
    return [
        event
        for event in events
        if _line_in_any_session(int(event["line_no"]), sessions)
    ]


def _error_lines_in_session(
    error_lines: list[tuple[int, str]],
    session: dict[str, Any],
) -> list[tuple[int, str]]:
    start_line_no = int(session["start_line_no"])
    end_line_no = int(session["end_line_no"])
    return [
        (line_no, content)
        for (line_no, content) in error_lines
        if start_line_no <= int(line_no) <= end_line_no
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


def _is_non_recording_network_error_line(content: str) -> bool:
    parsed = _parse_structured_log_line(content)
    component = str(parsed.get("component") or "").strip().lower()
    message = str(parsed.get("message") or "").strip().lower()
    raw = str(parsed.get("raw") or content or "").strip().lower()
    combined = " ".join(part for part in (component, message, raw) if part)

    if any(token in combined for token in ("ffmpeg", "/dev/video0", "videodevice : error", "video device : error")):
        return False

    if component not in {"endpoint", "endpointclient", "uploader"}:
        return False

    network_hints = (
        "couldn't renew jwt",
        "send status: failed",
        "sendcurrentframesnapbase64",
        "sendscreenshotbase64",
        "senddailylog",
        "getaddrinfo eai_again",
        "status.kr.mmtalkbox.com",
        "stream.kr.mmtalkbox.com",
        "couldn't be sent",
        "throttling:",
    )
    return any(token in combined for token in network_hints)


def _describe_non_recording_network_error_context(error_lines: list[tuple[int, str]]) -> str | None:
    if not error_lines:
        return None
    if not all(_is_non_recording_network_error_line(content) for _, content in error_lines):
        return None

    has_endpoint = False
    has_uploader = False
    has_jwt = False
    has_status = False
    has_upload = False

    for _, content in error_lines:
        lowered = str(content or "").lower()
        if "[endpoint" in lowered or "endpoint]" in lowered or "endpointclient" in lowered:
            has_endpoint = True
        if "[uploader" in lowered or "uploader]" in lowered:
            has_uploader = True
        if "jwt" in lowered:
            has_jwt = True
        if any(token in lowered for token in ("send status", "sendscreenshotbase64", "sendcurrentframesnapbase64", "senddailylog")):
            has_status = True
        if "couldn't be sent" in lowered or "stream.kr.mmtalkbox.com" in lowered or "throttling:" in lowered:
            has_upload = True

    parts: list[str] = []
    if has_jwt or has_status:
        parts.append("JWT/상태 전송 통신 오류")
    if has_upload:
        parts.append("업로드 통신 오류")
    if not parts and has_endpoint:
        parts.append("서버 통신 오류")
    if not parts and has_uploader:
        parts.append("업로드 오류")
    if not parts:
        parts.append("서버 통신 오류")
    return ", ".join(parts)


def _has_uploader_network_error(error_lines: list[tuple[int, str]]) -> bool:
    for _, content in error_lines:
        parsed = _parse_structured_log_line(content)
        component = str(parsed.get("component") or "").strip().lower()
        message = str(parsed.get("message") or "").strip().lower()
        raw = str(parsed.get("raw") or content or "").strip().lower()
        combined = " ".join(part for part in (component, message, raw) if part)
        if component != "uploader":
            continue
        if any(token in combined for token in ("couldn't be sent", "throttling:", "stream.kr.mmtalkbox.com", "getaddrinfo eai_again")):
            return True
    return False


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
    source_lines: list[str],
    device_name: str,
    hospital_name: str,
    room_name: str,
    log_key: str,
    log_date: str,
    line_count: int,
    sessions: list[dict[str, Any]],
    session_scans: list[dict[str, Any]],
    all_scan_events: list[dict[str, Any]],
    session_motions: list[dict[str, Any]],
    session_restarts: list[dict[str, Any]],
    session_error_lines: list[tuple[int, str]],
    recordings_on_date_count: int = 0,
    recordings_on_date_statuses: list[str] | None = None,
) -> dict[str, Any]:
    closure = _session_closure_counts(sessions)
    scan_items = _serialize_scan_events_for_evidence(session_scans)
    motion_items = _serialize_motion_events_for_evidence(session_motions)
    restart_items = _serialize_restart_events_for_evidence(session_restarts)
    error_items = _serialize_error_lines_for_evidence(session_error_lines)
    first_ffmpeg_error = _find_first_ffmpeg_error_context(session_error_lines, sessions)
    session_diagnostics: list[dict[str, Any]] = []
    session_details: list[dict[str, Any]] = []
    for index, session in enumerate(sessions, start=1):
        session_scan_events = _events_in_session(session_scans, session)
        session_motion_events = _events_in_session(session_motions, session)
        session_restart_events = _events_in_session(session_restarts, session)
        session_error_subset = _error_lines_in_session(session_error_lines, session)
        recording_result, recovery_context, post_stop_context = _build_session_recording_result_text(
            source_lines,
            session,
            session_restart_events,
            session_error_subset,
            all_scan_events,
            recordings_on_date_count,
        )
        session_error_items = _serialize_error_lines_for_evidence(session_error_subset)
        session_first_ffmpeg_error = _find_first_ffmpeg_error_context(session_error_subset, [session])
        session_detail = {
            "index": index,
            "startTime": _display_value(session.get("start_time_label"), default="시간미상"),
            "stopTime": _display_value(session.get("stop_time_label"), default="미확인"),
            "stopToken": _display_value(session.get("stop_token"), default="미확인"),
            "normalClosed": session.get("stop_line_no") is not None,
            "restartDetected": bool(session_restart_events),
            "recordingResult": recording_result,
            "fileId": _display_value((recovery_context or {}).get("fileId"), default=""),
            "scanEventCount": len(_serialize_scan_events_for_evidence(session_scan_events)),
            "motionEventCount": len(_serialize_motion_events_for_evidence(session_motion_events)),
            "errorLineCount": len(session_error_items),
            "errorGroups": _build_error_groups(session_error_items),
            "firstFfmpegError": session_first_ffmpeg_error or {},
        }
        session_diagnostics.append(
            {
                "index": index,
                "startTime": _display_value(session.get("start_time_label"), default="시간미상"),
                "stopTime": _display_value(session.get("stop_time_label"), default="미확인"),
                "severity": _display_value((post_stop_context or {}).get("severity"), default="normal"),
                "finishDelay": _display_value((post_stop_context or {}).get("finishDelayLabel"), default=""),
                "postStopScanCount": int((post_stop_context or {}).get("postStopScanCount") or 0),
                "postStopStopCount": int((post_stop_context or {}).get("postStopStopCount") or 0),
                "postStopSnapCount": int((post_stop_context or {}).get("postStopSnapCount") or 0),
                "postStopDeviceErrorCount": int((post_stop_context or {}).get("postStopDeviceErrorCount") or 0),
                "displayText": _display_value((post_stop_context or {}).get("displayText"), default=""),
            }
        )
        session_detail["sessionDiagnostic"] = session_diagnostics[-1]
        session_details.append(session_detail)
    return {
        "deviceName": device_name,
        "hospitalName": hospital_name,
        "roomName": room_name,
        "date": log_date,
        "logKey": log_key,
        "lineCount": int(line_count),
        "sessions": closure,
        "firstSessionStartTime": _display_value(
            sessions[0].get("start_time_label") if sessions else None,
            default="미확인",
        ),
        "lastSessionStopTime": _display_value(
            sessions[-1].get("stop_time_label") if sessions else None,
            default="미확인",
        ),
        "scanEventCount": len(scan_items),
        "scanEvents": scan_items,
        "motionEvents": motion_items,
        "restartEventCount": len(restart_items),
        "restartDetected": len(restart_items) > 0,
        "restartEvents": restart_items,
        "errorLineCount": len(error_items),
        "errorLines": error_items,
        "errorGroups": _build_error_groups(error_items),
        "firstFfmpegError": first_ffmpeg_error,
        "sessionDiagnostics": session_diagnostics,
        "sessionDetails": session_details,
        "recordingsOnDateCount": int(recordings_on_date_count),
        "recordingsOnDateStatuses": recordings_on_date_statuses or [],
    }


def _append_session_timing_summary(
    lines: list[str],
    sessions: list[dict[str, Any]],
    session_error_lines: list[tuple[int, str]],
    restart_events: list[dict[str, Any]] | None = None,
) -> None:
    restart_events = restart_events or []
    if not sessions:
        return

    if len(sessions) == 1:
        start_time = _display_value(sessions[0].get("start_time_label"), default="미확인")
        if start_time != "미확인":
            lines.append(f"• 세션 시작: `{start_time}`")

    has_restart = any(
        _find_session_for_line(int(event.get("line_no") or 0), sessions) is not None
        for event in restart_events
    )
    if has_restart:
        return

    first_ffmpeg_error = _find_first_ffmpeg_error_context(session_error_lines, sessions)
    if not first_ffmpeg_error:
        return

    error_time = _display_value(first_ffmpeg_error.get("timeLabel"), default="시간미상")
    session_start_time = _display_value(first_ffmpeg_error.get("sessionStartTime"), default="미확인")
    elapsed = _display_value(first_ffmpeg_error.get("elapsedFromSessionStart"), default="")

    detail_parts: list[str] = []
    if session_start_time != "미확인":
        detail_parts.append(f"세션 시작 `{session_start_time}`")
    if elapsed:
        detail_parts.append(f"시작 후 `{elapsed}`")

    detail_suffix = f" ({', '.join(detail_parts)})" if detail_parts else ""
    lines.append(f"• 첫 ffmpeg 에러: `{error_time}`{detail_suffix}")


def _append_session_sections(
    lines: list[str],
    source_lines: list[str],
    sessions: list[dict[str, Any]],
    scan_events: list[dict[str, Any]],
    motion_events: list[dict[str, Any]],
    restart_events: list[dict[str, Any]],
    error_lines: list[tuple[int, str]],
    diagnostic_scan_events: list[dict[str, Any]] | None = None,
    recordings_on_date_count: int | None = None,
) -> None:
    diagnostic_scan_events = diagnostic_scan_events or scan_events
    if not sessions:
        _append_session_state_summary(
            lines,
            source_lines,
            sessions,
            restart_events,
            error_lines,
            diagnostic_scan_events,
            recordings_on_date_count,
        )
        _append_session_timing_summary(lines, sessions, error_lines, restart_events)
        _append_restart_events_section(lines, restart_events)
        _append_scan_events_section(lines, scan_events, motion_events)
        _append_error_lines_section(lines, error_lines, show_all=True)
        return

    if len(sessions) <= 1:
        _append_session_state_summary(
            lines,
            source_lines,
            sessions,
            restart_events,
            error_lines,
            diagnostic_scan_events,
            recordings_on_date_count,
        )
        _append_session_timing_summary(lines, sessions, error_lines, restart_events)
        _append_restart_events_section(lines, restart_events)
        _append_scan_events_section(lines, scan_events, motion_events)
        _append_error_lines_section(lines, error_lines, show_all=True)
        return

    lines.append(f"• 세션 수: *{len(sessions)}건*")

    for index, session in enumerate(sessions, start=1):
        session_scan_events = _events_in_session(scan_events, session)
        session_motion_events = _events_in_session(motion_events, session)
        session_restart_events = _events_in_session(restart_events, session)
        session_error_lines = _error_lines_in_session(error_lines, session)
        start_time = _display_value(session.get("start_time_label"), default="시간미상")
        stop_time = _display_value(session.get("stop_time_label"), default="미확인")

        lines.append("")
        lines.append(f"*세션 {index}* (`{start_time}` ~ `{stop_time}`)")
        _append_session_state_summary(
            lines,
            source_lines,
            [session],
            session_restart_events,
            session_error_lines,
            diagnostic_scan_events,
            recordings_on_date_count if len(sessions) == 1 else None,
        )
        _append_session_timing_summary(lines, [session], session_error_lines, session_restart_events)
        _append_restart_events_section(lines, session_restart_events)
        _append_scan_events_section(lines, session_scan_events, session_motion_events)
        _append_error_lines_section(lines, session_error_lines, show_all=True)


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
        normalized = re.sub(r"(?<!\d)\d{11}(?!\d)", "", normalized)
        normalized = re.sub(r"\s+\d{2,4}[./-]\d{1,2}[./-]\d{1,2}\s*$", "", normalized)
        normalized = re.sub(r"\s+\d{1,2}\s*월\s*\d{1,2}\s*일\s*$", "", normalized)
        normalized = re.sub(r"(?<!\d)\d{4}(?!\d)\s*$", "", normalized)
        normalized = re.sub(r"^\s*병원(?:명)?\s*[:=]?\s*", "", normalized)
        normalized = re.sub(r"^\s*(?:병실(?:명)?|진료실명)\s*[:=]?\s*", "", normalized)
        normalized = re.sub(r"^\s*날짜\s*[:=]?\s*", "", normalized)
        normalized = re.sub(r"\s*(?:로그|분석)\s*$", "", normalized)
        return normalized.strip()

    hospital_name = _clean(hospital_match.group(1)) if hospital_match else ""
    room_name = _clean(room_match.group(1)) if room_match else ""

    if hospital_name and room_name:
        return (hospital_name or None, room_name or None)

    fallback_text = text
    for pattern in (
        _KOREAN_YMD_PATTERN,
        _NUMERIC_YMD_PATTERN,
        _KOREAN_MD_PATTERN,
        _NUMERIC_MD_PATTERN,
        _NUMERIC_MD_DASH_PATTERN,
        _COMPACT_YYYYMMDD_PATTERN,
        _COMPACT_YYMMDD_PATTERN,
        _COMPACT_MMDD_PATTERN,
    ):
        fallback_text = pattern.sub(" ", fallback_text)
    fallback_text = re.sub(r"(?<!\d)\d{11}(?!\d)", " ", fallback_text)
    fallback_text = re.sub(r"\b(?:로그|분석)\b", " ", fallback_text)
    fallback_text = " ".join(fallback_text.split()).strip()

    room_token_match = _ROOM_TOKEN_PATTERN.search(fallback_text)
    if not room_name and room_token_match:
        room_name = _clean(room_token_match.group(1))

    if not hospital_name and room_token_match:
        hospital_candidate = _clean(fallback_text[: room_token_match.start()])
        hospital_name = hospital_candidate

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

    lines.append(f"• 재시작 로그: *{len(restart_events)}건*")

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
    use_db_upload_cross_check = recordings_context is not None and day_span <= 1

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
            device_seq = device_context.get("deviceSeq")
            recordings_on_date_rows = (
                _load_recordings_rows_on_date_by_barcode(
                    barcode,
                    date_label,
                    device_seq=int(device_seq) if device_seq is not None else None,
                )
                if barcode and use_db_upload_cross_check
                else []
            )
            recordings_on_date_statuses = sorted(
                {
                    _display_value(row.get("streamingStatus"), default="미확인")
                    for row in recordings_on_date_rows
                }
            )

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
                    source_lines=source_lines,
                    device_name=device_name,
                    hospital_name=hospital_name,
                    room_name=room_name,
                    log_key=str(log_data["key"]),
                    log_date=date_label,
                    line_count=len(source_lines),
                    sessions=sessions,
                    session_scans=session_events,
                    all_scan_events=events,
                    session_motions=session_motion_events,
                    session_restarts=session_restart_events,
                    session_error_lines=session_error_lines,
                    recordings_on_date_count=len(recordings_on_date_rows),
                    recordings_on_date_statuses=recordings_on_date_statuses,
                )
            )

            lines.append("")
            lines.append(f"*장비 `{device_name}` | 날짜 `{date_label}`*")
            lines.append(f"• 병원: `{hospital_name}`")
            lines.append(f"• 병실: `{room_name}`")
            lines.append(f"• DB 영상 기록(날짜 기준): `{len(recordings_on_date_rows)}개`")
            _append_session_sections(
                lines,
                source_lines,
                sessions,
                session_events,
                session_motion_events,
                session_restart_events,
                session_error_lines,
                diagnostic_scan_events=events,
                recordings_on_date_count=len(recordings_on_date_rows),
            )

    if found_log_files == 0:
        result_text = (
            f"{title}\n"
            f"• 바코드: `{barcode}`\n"
            f"• 분석 범위(KST): `{start_date:%Y-%m-%d}` ~ `{end_date:%Y-%m-%d}` (`{day_span}일`)\n"
            f"• 매핑 장비: `{len(device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`"
        )
        return result_text, _build_log_analysis_payload(
            mode="phase1_window",
            barcode=barcode,
            request_date=None,
            date_range=f"{start_date:%Y-%m-%d} ~ {end_date:%Y-%m-%d}",
            records=[],
        )
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

    def _analyze_device_context_batch(device_context_batch: list[dict[str, Any]]) -> None:
        nonlocal total_session_count, logs_found_any, logs_with_session, devices_with_session

        for device_context in device_context_batch:
            device_name = str(device_context.get("deviceName") or "")
            if not device_name:
                continue
            device_seq = device_context.get("deviceSeq")
            recordings_on_date_rows = (
                _load_recordings_rows_on_date_by_barcode(
                    barcode,
                    log_date,
                    device_seq=int(device_seq) if device_seq is not None else None,
                )
                if barcode and recordings_context is not None
                else []
            )
            recordings_on_date_statuses = sorted(
                {
                    _display_value(row.get("streamingStatus"), default="미확인")
                    for row in recordings_on_date_rows
                }
            )

            log_data = _fetch_s3_device_log_lines(
                s3_client,
                device_name,
                log_date,
                tail_only=False,
            )

            if not log_data["found"]:
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
            session_error_lines = _error_lines_in_sessions(error_lines, sessions)

            if session_count == 0:
                continue

            logs_with_session += 1
            lines.append("")
            lines.append(f"• 매핑 장비: `{device_name}`")

            hospital_name = _display_value(device_context.get("hospitalName"), default="미확인")
            room_name = _display_value(device_context.get("roomName"), default="미확인")
            analysis_records.append(
                _build_log_analysis_record(
                    source_lines=source_lines,
                    device_name=device_name,
                    hospital_name=hospital_name,
                    room_name=room_name,
                    log_key=str(log_data["key"]),
                    log_date=log_date,
                    line_count=len(source_lines),
                    sessions=sessions,
                    session_scans=session_scoped_events,
                    all_scan_events=events,
                    session_motions=session_motion_events,
                    session_restarts=session_restart_events,
                    session_error_lines=session_error_lines,
                    recordings_on_date_count=len(recordings_on_date_rows),
                    recordings_on_date_statuses=recordings_on_date_statuses,
                )
            )

            lines.append(f"• 파일: `{log_data['key']}`")
            lines.append(f"• 병원: `{hospital_name}`")
            lines.append(f"• 병실: `{room_name}`")
            lines.append(f"• 날짜: `{log_date}`")
            lines.append(f"• DB 영상 기록(날짜 기준): `{len(recordings_on_date_rows)}개`")
            lines.append(f"• 분석 범위: 전체 `{len(source_lines)}줄`")
            _append_session_sections(
                lines,
                source_lines,
                sessions,
                session_scoped_events,
                session_motion_events,
                session_restart_events,
                session_error_lines,
                diagnostic_scan_events=events,
                recordings_on_date_count=len(recordings_on_date_rows),
            )
            devices_with_session += 1

    _analyze_device_context_batch(target_device_contexts)

    if logs_with_session == 0:
        expanded_device_contexts = _expand_device_contexts_to_recordings_hospital_scope(
            recordings_context,
            target_device_contexts,
        )
        if expanded_device_contexts:
            lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비로 확장 검색했어")
            _analyze_device_context_batch(expanded_device_contexts[: max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 4))])

    if logs_found_any == 0:
        result_text = (
            "*바코드 로그 스캔 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`"
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
            f"• 확인한 로그 파일: `{logs_found_any}개`"
        )
        return result_text, _build_log_analysis_payload(
            mode="scan",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )
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

    def _analyze_device_context_batch(device_context_batch: list[dict[str, Any]]) -> None:
        nonlocal total_session_error_lines, logs_found_any, logs_with_session, total_session_count, devices_with_session

        for device_context in device_context_batch:
            device_name = str(device_context.get("deviceName") or "")
            if not device_name:
                continue
            device_seq = device_context.get("deviceSeq")
            recordings_on_date_rows = (
                _load_recordings_rows_on_date_by_barcode(
                    barcode,
                    log_date,
                    device_seq=int(device_seq) if device_seq is not None else None,
                )
                if barcode and recordings_context is not None
                else []
            )
            recordings_on_date_statuses = sorted(
                {
                    _display_value(row.get("streamingStatus"), default="미확인")
                    for row in recordings_on_date_rows
                }
            )

            log_data = _fetch_s3_device_log_lines(
                s3_client,
                device_name,
                log_date,
                tail_only=False,
            )

            if not log_data["found"]:
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
            session_error_lines = _error_lines_in_sessions(error_lines, sessions)
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
                    source_lines=source_lines,
                    device_name=device_name,
                    hospital_name=hospital_name,
                    room_name=room_name,
                    log_key=str(log_data["key"]),
                    log_date=log_date,
                    line_count=len(source_lines),
                    sessions=sessions,
                    session_scans=session_scoped_events,
                    all_scan_events=events,
                    session_motions=session_motion_events,
                    session_restarts=session_restart_events,
                    session_error_lines=session_error_lines,
                    recordings_on_date_count=len(recordings_on_date_rows),
                    recordings_on_date_statuses=recordings_on_date_statuses,
                )
            )

            lines.append(f"• 파일: `{log_data['key']}`")
            lines.append(f"• 병원: `{hospital_name}`")
            lines.append(f"• 병실: `{room_name}`")
            lines.append(f"• 날짜: `{log_date}`")
            lines.append(f"• DB 영상 기록(날짜 기준): `{len(recordings_on_date_rows)}개`")
            lines.append(f"• 파일 크기: `{_format_size(log_data['content_length'])}`")
            lines.append(f"• 분석 범위: 전체 `{len(source_lines)}줄`")
            _append_session_sections(
                lines,
                source_lines,
                sessions,
                session_scoped_events,
                session_motion_events,
                session_restart_events,
                session_error_lines,
                diagnostic_scan_events=events,
                recordings_on_date_count=len(recordings_on_date_rows),
            )
            devices_with_session += 1

    _analyze_device_context_batch(target_device_contexts)

    if logs_with_session == 0:
        expanded_device_contexts = _expand_device_contexts_to_recordings_hospital_scope(
            recordings_context,
            target_device_contexts,
        )
        if expanded_device_contexts:
            lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비로 확장 검색했어")
            _analyze_device_context_batch(expanded_device_contexts[: max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 4))])

    if logs_found_any == 0:
        result_text = (
            "*바코드 로그 에러 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`"
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
            f"• 확인한 로그 파일: `{logs_found_any}개`"
        )
        return result_text, _build_log_analysis_payload(
            mode="error",
            barcode=barcode,
            request_date=log_date,
            date_range=None,
            records=[],
        )
    max_result_chars = max(s.S3_QUERY_MAX_RESULT_CHARS, 38000)
    result_text = _truncate_text("\n".join(lines), max_result_chars)
    return result_text, _build_log_analysis_payload(
        mode="error",
        barcode=barcode,
        request_date=log_date,
        date_range=None,
        records=analysis_records,
    )
