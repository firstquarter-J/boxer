import os
import re
from datetime import datetime, timedelta
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
    text = (question or "").strip()
    lowered = text.lower()

    has_explicit_date, parsed_explicit_date = _parse_explicit_date_expression(text)
    if has_explicit_date:
        if parsed_explicit_date is None:
            raise ValueError("날짜 형식을 확인해줘. 예: 2026-03-03, 26.03.03, 3/3, 3월 3일")
        return parsed_explicit_date

    base_date = _current_local_date()
    relative_offset = _extract_relative_day_offset(text, lowered)
    if relative_offset is not None:
        base_date = base_date + timedelta(days=relative_offset)
    return base_date.strftime("%Y-%m-%d")


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
        lowered = line.lower()
        if any(keyword in lowered for keyword in cs.LOG_ERROR_KEYWORDS):
            matches.append((line_no, line))
    return matches


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


def _parse_scanned_event(line: str) -> tuple[str, str] | None:
    matched = cs.SCANNED_TOKEN_PATTERN.search(line)
    if not matched:
        return None
    token = matched.group(1).strip().strip("`'\",;:()[]{}")
    upper_token = token.upper()
    if upper_token in cs.SCAN_CODE_LABELS:
        return token, cs.SCAN_CODE_LABELS[upper_token]
    if re.fullmatch(r"\d{11}", token):
        return token, "녹화 시작 바코드 스캔"
    return token, f"기타 스캔 ({token})"


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
        token, label = parsed
        time_label = line_time_label
        if time_label == "시간미상" and latest_time_label:
            time_label = latest_time_label
        events.append(
            {
                "line_no": line_no,
                "time_label": time_label,
                "label": label,
                "token": token,
            }
        )
    return events


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


def _events_in_session(events: list[dict[str, Any]], session: dict[str, Any]) -> list[dict[str, Any]]:
    start_line_no = int(session["start_line_no"])
    end_line_no = int(session["end_line_no"])
    return [
        event
        for event in events
        if start_line_no <= int(event["line_no"]) <= end_line_no
    ]


def _line_in_any_session(line_no: int, sessions: list[dict[str, Any]]) -> bool:
    for session in sessions:
        if int(session["start_line_no"]) <= line_no <= int(session["end_line_no"]):
            return True
    return False


def _append_scan_events_section(lines: list[str], events: list[dict[str, Any]]) -> None:
    lines.append(f"• scanned 이벤트: *{len(events)}건*")
    if not events:
        lines.append("- 없음")
        return

    for event in events:
        time_label = _display_value(event.get("time_label"), default="시간미상")
        label = _display_value(event.get("label"), default="기타 스캔")
        token = _display_value(event.get("token"), default="unknown")
        lines.append(f"- {time_label}: {label} (`{token}`)")


def _append_error_lines_section(lines: list[str], error_lines: list[tuple[int, str]]) -> None:
    lines.append(f"• error 라인: *{len(error_lines)}줄*")
    if not error_lines:
        lines.append("- 없음")
        return

    sample_limit = max(1, min(50, cs.LOG_ANALYSIS_MAX_SAMPLES * 5))
    display_error_lines = error_lines[-sample_limit:]
    if len(error_lines) > len(display_error_lines):
        lines.append(f"• 참고: error 라인이 많아서 최근 `{len(display_error_lines)}줄`만 표시해")

    for line_no, content in display_error_lines:
        sample = content.strip()
        if len(sample) > 220:
            sample = sample[:220] + "...(truncated)"
        lines.append(f"- [{line_no}] {sample}")


def _analyze_barcode_log_scan_events(
    s3_client: Any,
    barcode: str,
    log_date: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    device_contexts = _lookup_device_contexts_by_barcode(
        barcode,
        recordings_context=recordings_context,
    )
    if not device_contexts:
        return (
            "*바코드 로그 스캔 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• recordings/devices에서 매핑된 장비명을 찾지 못했어"
        )

    max_devices = max(1, min(20, cs.LOG_ANALYSIS_MAX_DEVICES))
    target_device_contexts = device_contexts[:max_devices]
    omitted_device_count = max(0, len(device_contexts) - len(target_device_contexts))
    total_session_count = 0
    found_log_files = 0
    devices_with_session = 0

    lines = [
        "*바코드 로그 스캔 분석 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(device_contexts)}개`",
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

        found_log_files += 1
        lines.append("")
        lines.append(f"*장비 `{device_name}`*")
        source_lines = log_data["lines"]
        events = _extract_scan_events_with_line_no(source_lines)
        error_lines = _find_error_lines(source_lines)
        sessions = _extract_recording_sessions(
            source_lines,
            barcode,
            cs.LOG_SESSION_SAFETY_LINES,
            scan_events=events,
        )
        session_count = len(sessions)
        total_session_count += session_count

        hospital_name = _display_value(device_context.get("hospitalName"), default="미확인")
        room_name = _display_value(device_context.get("roomName"), default="미확인")

        lines.append(f"• 파일: `{log_data['key']}`")
        lines.append(f"• 병원: `{hospital_name}`")
        lines.append(f"• 병실: `{room_name}`")
        lines.append(f"• 날짜: `{log_date}`")
        lines.append(f"• 분석 범위: 전체 `{len(source_lines)}줄`")
        _append_scan_events_section(lines, events)
        _append_error_lines_section(lines, error_lines)
        lines.append(f"• 요청 바코드 녹화 세션: *{session_count}건*")

        if session_count == 0:
            lines.append("• 결과: 요청 바코드로 시작된 녹화 세션이 없어")
            continue

        devices_with_session += 1
        last_session = sessions[-1]
        start_time_label = _display_value(last_session.get("start_time_label"), default="시간미상")
        stop_time_label = _display_value(last_session.get("stop_time_label"), default="미확인")
        lines.append(f"• 마지막 세션 시작: `{start_time_label}`")
        lines.append(f"• 마지막 세션 종료: `{stop_time_label}`")
        lines.append(
            f"• 세션 기준: `C_STOPSESS` 이후 `{max(0, cs.LOG_SESSION_SAFETY_LINES)}줄` 포함"
        )

        session_events = _events_in_session(events, last_session)
        lines.append(f"• 마지막 세션 스캔 이벤트: *{len(session_events)}건*")

        if not session_events:
            lines.append("• 타임라인: 없음")
            continue

        for event in session_events:
            time_label = _display_value(event.get("time_label"), default="시간미상")
            label = _display_value(event.get("label"), default="기타 스캔")
            token = _display_value(event.get("token"), default="unknown")
            lines.append(f"- {time_label}: {label} (`{token}`)")

    if found_log_files == 0:
        return (
            "*바코드 로그 스캔 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`\n"
            "*요약*: 요청 날짜의 로그 파일을 찾지 못했어"
        )

    lines.append("")
    lines.append(f"• 확인한 로그 파일: `{found_log_files}개`")
    lines.append(f"• 요청 바코드 세션이 확인된 장비: `{devices_with_session}개`")
    if total_session_count > 0:
        lines.append(f"*요약*: 분석 범위에서 요청 바코드 녹화 세션 `{total_session_count}건`을 찾았어")
    else:
        lines.append(f"*요약*: 로그 파일은 확인했지만 요청 바코드 `{barcode}` 세션은 찾지 못했어")
    lines.append("※ 세션 규칙: 바코드 스캔 시작 ~ C_STOPSESS + 안전 라인")

    max_result_chars = max(s.S3_QUERY_MAX_RESULT_CHARS, 20000)
    return _truncate_text("\n".join(lines), max_result_chars)


def _analyze_barcode_log_errors(
    s3_client: Any,
    barcode: str,
    log_date: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    device_contexts = _lookup_device_contexts_by_barcode(
        barcode,
        recordings_context=recordings_context,
    )
    if not device_contexts:
        return (
            "*바코드 로그 에러 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• recordings/devices에서 매핑된 장비명을 찾지 못했어"
        )

    max_devices = max(1, min(20, cs.LOG_ANALYSIS_MAX_DEVICES))
    target_device_contexts = device_contexts[:max_devices]
    omitted_device_count = max(0, len(device_contexts) - len(target_device_contexts))

    total_error_lines = 0
    found_log_files = 0
    total_session_count = 0
    devices_with_session = 0
    lines = [
        "*바코드 로그 에러 분석 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(device_contexts)}개`",
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

        found_log_files += 1
        lines.append("")
        lines.append(f"*장비 `{device_name}`*")
        source_lines = log_data["lines"]
        events = _extract_scan_events_with_line_no(source_lines)
        sessions = _extract_recording_sessions(
            source_lines,
            barcode,
            cs.LOG_SESSION_SAFETY_LINES,
            scan_events=events,
        )
        session_count = len(sessions)
        total_session_count += session_count
        error_lines = _find_error_lines(source_lines)
        total_error_lines += len(error_lines)
        session_error_lines = [
            (line_no, content)
            for (line_no, content) in error_lines
            if _line_in_any_session(line_no, sessions)
        ]

        hospital_name = _display_value(device_context.get("hospitalName"), default="미확인")
        room_name = _display_value(device_context.get("roomName"), default="미확인")

        lines.append(f"• 파일: `{log_data['key']}`")
        lines.append(f"• 병원: `{hospital_name}`")
        lines.append(f"• 병실: `{room_name}`")
        lines.append(f"• 날짜: `{log_date}`")
        lines.append(f"• 파일 크기: `{_format_size(log_data['content_length'])}`")
        lines.append(f"• 분석 범위: 전체 `{len(source_lines)}줄`")
        _append_scan_events_section(lines, events)
        _append_error_lines_section(lines, error_lines)
        lines.append(f"• 요청 바코드 녹화 세션: *{session_count}건*")

        if session_count == 0:
            lines.append("• 결과: 요청 바코드로 시작된 녹화 세션이 없어")
            continue

        devices_with_session += 1
        lines.append(f"• 세션 구간 에러 패턴 라인 수: *{len(session_error_lines)}줄*")

        if not session_error_lines:
            lines.append("• 샘플: 없음")
            continue

        sample_count = max(1, min(10, cs.LOG_ANALYSIS_MAX_SAMPLES))
        for index, (line_no, content) in enumerate(session_error_lines[-sample_count:], start=1):
            sample = content.strip()
            if len(sample) > 220:
                sample = sample[:220] + "...(truncated)"
            lines.append(f"{index}. [{line_no}] {sample}")

    if found_log_files == 0:
        return (
            "*바코드 로그 에러 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`\n"
            "*요약*: 요청 날짜의 로그 파일을 찾지 못했어"
        )

    lines.append("")
    lines.append(f"• 확인한 로그 파일: `{found_log_files}개`")
    lines.append(f"• 요청 바코드 세션이 확인된 장비: `{devices_with_session}개`")
    lines.append(f"• 로그 전체 error 라인: `{total_error_lines}줄`")
    if total_error_lines > 0:
        lines.append("*요약*: 로그에서 error 패턴 라인을 확인했고 세션 구간 결과를 함께 표시했어")
    else:
        lines.append("*요약*: 로그에서 error 패턴 라인을 찾지 못했어")
    lines.append("※ 세션 규칙: 바코드 스캔 시작 ~ C_STOPSESS + 안전 라인")

    max_result_chars = max(s.S3_QUERY_MAX_RESULT_CHARS, 20000)
    return _truncate_text("\n".join(lines), max_result_chars)
