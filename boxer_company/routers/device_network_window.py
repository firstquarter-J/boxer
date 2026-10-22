"""네트워크 진단에만 적용하는 한국 시간 하루 조회 범위다."""

import math
import re
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from boxer_company import _operation_routing_common as dates

KST = ZoneInfo("Asia/Seoul")
_VERSION_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9.])(?:v\s*\d+(?:\.\d+){1,3}|\d\.\d+\.\d+)(?:[-+][A-Za-z0-9.-]+)?(?![A-Za-z0-9.])"
    r"|(?:버전(?:은|는|이|의)?|version|node(?:\.js)?)\s*[:=]?\s*v?\s*\d+(?:\.\d+){1,3}(?:[-+][A-Za-z0-9.-]+)?(?![A-Za-z0-9.])",
    re.IGNORECASE,
)


class NetworkDateWindowError(ValueError):
    """장비 접속 전에 사용자에게 안내할 수 있는 고정된 날짜 오류다."""


def _date_expressions(question: str) -> list[str]:
    # 공용 parser와 같은 경계 규칙을 쓰되 겹치는 ISO/월일 후보는 한 날짜로 센다.
    candidates = []
    for kind, pattern in (
        ("korean_ymd", dates._KOREAN_YMD_PATTERN),
        ("numeric_ymd", dates._NUMERIC_YMD_PATTERN),
        ("korean_md", dates._KOREAN_MD_PATTERN),
        ("numeric_md", dates._NUMERIC_MD_PATTERN),
        ("numeric_md_dash", dates._NUMERIC_MD_DASH_PATTERN),
        ("compact_yyyymmdd", dates._COMPACT_YYYYMMDD_PATTERN),
        ("compact_yymmdd", dates._COMPACT_YYMMDD_PATTERN),
        ("compact_mmdd", dates._COMPACT_MMDD_PATTERN),
    ):
        for matched in pattern.finditer(question):
            if not dates._is_embedded_numeric_date_match(question, matched, kind):
                candidates.append((matched.start(), matched.end(), matched.group(0)))
    selected = []
    for start, end, text in sorted(candidates, key=lambda row: (row[0], -(row[1] - row[0]))):
        if not any(start < previous_end and end > previous_start for previous_start, previous_end, _ in selected):
            selected.append((start, end, text))
    return [text for _, _, text in selected]


def _relative_offsets(question: str) -> set[int]:
    lowered = question.lower()
    matches = []
    for offset, hints in (
            (-2, dates._DAY_BEFORE_YESTERDAY_HINTS),
            (-1, dates.cs.YESTERDAY_HINTS),
            (0, dates._TODAY_HINTS),
            (1, dates._TOMORROW_HINTS),
        ):
        for hint in hints:
            matches.extend((matched.start(), matched.end(), offset) for matched in re.finditer(re.escape(hint.lower()), lowered))
    selected = []
    for start, end, offset in sorted(matches, key=lambda row: (row[0], -(row[1] - row[0]))):
        if not any(start < previous_end and end > previous_start for previous_start, previous_end, _ in selected):
            selected.append((start, end, offset))
    return {offset for _, _, offset in selected}


def _reject_unsupported_period(question: str, expressions: list[str]) -> None:
    # 단일 날짜 이외의 기간을 오늘이나 첫 날짜로 조용히 축소하지 않는다.
    unsupported = r"(?:[월화수목금토일]요일|(?:지난|이번|다음)\s*(?:주|달|월|년)|(?:최근|지난)\s*\d+\s*(?:일|시간|주|개월)|\d+\s*일\s*(?:전|후))"
    relative_hints = (*dates._TODAY_HINTS, *dates._DAY_BEFORE_YESTERDAY_HINTS, *dates.cs.YESTERDAY_HINTS, *dates._TOMORROW_HINTS)
    tokens = expressions + [hint for hint in relative_hints if hint.lower() in question.lower()]
    has_range = any(re.search(re.escape(token) + r"\s*(?:부터|까지|[~〜∼])", question, re.IGNORECASE) for token in tokens)
    remainder = question
    for expression in expressions:
        remainder = remainder.replace(expression, " ", 1)
    unsupported_single = re.search(r"(?<!\d)\d{1,2}\s*(?:월|일)", remainder)
    unsupported_english = re.search(r"\b(?:(?:last|this|next)\s+(?:week|month|year)|(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)|\d+\s+days?\s+ago)\b", question, re.IGNORECASE)
    if re.search(unsupported, question) or has_range or unsupported_single or unsupported_english:
        raise NetworkDateWindowError("네트워크 로그는 날짜를 하루만 지정해줘. 예: 2026-10-20")


def _window(start: datetime, end: datetime, current_day: bool) -> dict[str, Any]:
    return {
        "startEpoch": start.timestamp(), "endEpoch": end.timestamp(),
        "date": start.date().isoformat(), "timezone": "Asia/Seoul",
        "label": start.strftime("%Y-%m-%d %H:%M:%S") + " ~ " + end.strftime("%Y-%m-%d %H:%M:%S") + " KST",
        "mode": "today" if current_day else "date", "isCurrentDay": current_day,
    }


def resolve_network_window(question: str, now: datetime | None = None) -> dict[str, Any]:
    reference = now or datetime.now(KST)
    if not isinstance(reference, datetime):
        raise NetworkDateWindowError("조회 기준 시각을 확인해줘")
    reference = reference.replace(tzinfo=KST) if reference.tzinfo is None else reference.astimezone(KST)
    # 명백한 버전 토큰만 제외한다. 2026.10.21/26.10.21 같은 날짜는 그대로 파싱한다.
    text = _VERSION_TOKEN_PATTERN.sub(" ", str(question or "").strip())
    try:
        # invalid 뒤 valid가 붙은 질문도 오늘이나 첫 유효일로 바꾸지 않고 거절한다.
        expressions = _date_expressions(text)
        _reject_unsupported_period(text, expressions)
        parsed = [dates._extract_log_date_with_presence(value, reference_date=reference.date())[0] for value in expressions]
        offsets = _relative_offsets(text)
        requested = set(parsed) | {(reference.date() + timedelta(days=offset)).isoformat() for offset in offsets}
        if len(expressions) > 1 or len(offsets) > 1 or len(requested) > 1:
            raise NetworkDateWindowError("네트워크 로그는 날짜를 하루만 지정해줘")
        day, _ = dates._extract_log_date_with_presence(text, reference_date=reference.date())
        selected = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=KST)
    except NetworkDateWindowError:
        raise
    except ValueError as error:
        raise NetworkDateWindowError("날짜 형식을 확인해줘. 예: 어제, 10월 21일, 2026-10-20") from error
    if selected.date() > reference.date():
        raise NetworkDateWindowError("미래 날짜의 네트워크 로그는 조회할 수 없어. 오늘이나 지난 날짜를 지정해줘")
    current_day = selected.date() == reference.date()
    return _window(selected, reference if current_day else selected + timedelta(days=1), current_day)


def validate_network_window(window: dict[str, Any]) -> dict[str, Any]:
    # 원격 코드에는 이 검증을 거친 숫자 epoch만 넣는다. 사용자 날짜 문자열은 넣지 않는다.
    if not isinstance(window, dict):
        raise NetworkDateWindowError("조회 날짜 범위를 확인해줘")
    start_epoch, end_epoch = window.get("startEpoch"), window.get("endEpoch")
    if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) for value in (start_epoch, end_epoch)):
        raise NetworkDateWindowError("조회 날짜 범위를 확인해줘")
    if not 0 <= end_epoch - start_epoch <= 86400:
        raise NetworkDateWindowError("조회 날짜 범위를 확인해줘")
    try:
        start, end = (datetime.fromtimestamp(value, KST) for value in (start_epoch, end_epoch))
    except (ValueError, OverflowError, OSError) as error:
        raise NetworkDateWindowError("조회 날짜 범위를 확인해줘") from error
    if start != start.replace(hour=0, minute=0, second=0, microsecond=0):
        raise NetworkDateWindowError("조회 날짜 범위를 확인해줘")
    return _window(start, end, window.get("isCurrentDay") is True)
