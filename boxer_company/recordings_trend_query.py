"""녹화 추이의 기간과 연속 감소 조건을 해석하는 provider-free 계약."""

import calendar
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from boxer_company.recordings_report_options import RecordingsReportOptionsError

_RECENT = re.compile(r"(?:최근|지난)\s*([+-]?\d+)\s*(주(?:간)?|개월|달|일)")
_DECLINE = re.compile(r"([+-]?\d+)\s*주\s*(?:이상\s*)?(?:연속\s*)?(?:감소(?:한|하는)?|줄어든)")
_DATE = r"(?:\d{4}[-./]\d{1,2}[-./]\d{1,2}|(?:(?:\d{4}년|올해|작년)\s*)?\d{1,2}월(?:\s*\d{1,2}일)?)"
_RANGE = re.compile(rf"({_DATE})\s*(?:부터|~|～|〜|–|—|\s-\s)\s*({_DATE}|지금|현재|오늘)(?:까지)?")


@dataclass(frozen=True, slots=True)
class RecordingsTrendQuery:
    start: date
    end: date
    decline_weeks: int | None = None


def has_recordings_trend_intent(question: str) -> bool:
    """잘못된 기간도 추이 경로에서 안내하고 한 주 요약으로 축소하지 않는다."""

    return bool(re.search(r"추이|추세|트렌드|trend", question, re.IGNORECASE) or _DECLINE.search(question))


def is_bare_recordings_trend_request(question: str) -> bool:
    """지표를 생략한 기간+추이 요청만 허용해 다른 도메인의 추이를 선점하지 않는다."""

    text = re.sub(r"<@[A-Z0-9]+>", "", question).replace("`", "")
    text = _RANGE.sub("", text)
    text = _RECENT.sub("", text)
    text = _DECLINE.sub("", text)
    text = re.sub(r"이번\s*달|지난\s*달|이번\s*주|지난\s*주", "", text)
    text = re.sub(r"주간|주별|추이|추세|트렌드|분석|데이터|조회|확인|보여줘|알려줘|해줘|해|줘|의|를|좀", "", text)
    return not text.strip(" .,?!")


def strip_trend_periods(question: str) -> str:
    """병원 필드 뒤 기간이 병원명으로 붙지 않도록 해석한 시간 표현만 제거한다."""

    text = _DECLINE.sub("", _RECENT.sub("", _RANGE.sub("", question)))
    return re.sub(r"이번\s*달|지난\s*달|이번\s*주|지난\s*주", "", text)


def _date_token(value: str, *, today: date, end: bool) -> date:
    if value in {"지금", "현재", "오늘"}:
        return today
    if re.fullmatch(r"\d{4}[-./]\d{1,2}[-./]\d{1,2}", value):
        return date(*(int(part) for part in re.split(r"[-./]", value)))
    match = re.fullmatch(r"(?:(\d{4}년|올해|작년)\s*)?(\d{1,2})월(?:\s*(\d{1,2})일)?", value)
    year = today.year - 1 if match[1] == "작년" else today.year
    if match[1] and match[1].endswith("년") and match[1] != "작년":
        year = int(match[1][:-1])
    month = int(match[2])
    day = int(match[3]) if match[3] else calendar.monthrange(year, month)[1] if end else 1
    return date(year, month, day)


def parse_recordings_trend_query(question: str, *, now: datetime) -> RecordingsTrendQuery:
    """최근 N주, 월/날짜부터 현재, 날짜 범위를 KST 주별 집계 기간으로 정규화한다."""

    text = question.replace("`", "")
    tz = ZoneInfo("Asia/Seoul")
    today = (now.replace(tzinfo=tz) if now.tzinfo is None else now.astimezone(tz)).date()
    monday = today - timedelta(days=today.weekday())
    declines = list(_DECLINE.finditer(text))
    if len(declines) > 1:
        raise RecordingsReportOptionsError("연속 감소 조건은 하나만 지정해줘")
    decline = int(declines[0][1]) if declines else None
    if decline is not None and not 1 <= decline <= 51:
        raise RecordingsReportOptionsError("연속 감소는 1~51주 사이로 지정해줘")
    if re.search(r"일별|월별", text):
        raise RecordingsReportOptionsError("추이는 주별로 비교해. '주별 추이'로 요청해줘")

    # 감소 횟수의 N주는 조회 기간과 별개다. 기간 생략 시 최소 N+1주를 확보한다.
    period_text = _DECLINE.sub("", text)
    ranges = list(_RANGE.finditer(period_text))
    recent = list(_RECENT.finditer(period_text))
    calendar_periods = list(re.finditer(r"이번\s*달|지난\s*달|이번\s*주|지난\s*주", period_text))
    if len(ranges) + len(recent) + len(calendar_periods) > 1:
        raise RecordingsReportOptionsError("조회 기간은 하나만 지정해줘")
    remaining = _RANGE.sub("", _RECENT.sub("", period_text))
    if (recent or calendar_periods) and re.search(_DATE, remaining):
        raise RecordingsReportOptionsError("조회 기간은 하나만 지정해줘")
    try:
        if ranges:
            match = ranges[0]
            start = _date_token(match[1], today=today, end=False)
            end = _date_token(match[2], today=today, end=True)
            # 해석하지 못한 날짜가 남으면 임의로 첫 기간만 실행하지 않는다.
            if re.search(_DATE, period_text[:match.start()] + period_text[match.end():]):
                raise RecordingsReportOptionsError("조회 기간은 하나만 지정해줘")
        elif recent:
            count = int(recent[0][1])
            if not 1 <= count <= 366:
                raise RecordingsReportOptionsError("조회 기간은 1~366일 또는 최대 52주로 지정해줘")
            unit = recent[0][2]
            if unit.startswith("주"):
                start, end = monday - timedelta(weeks=count), monday - timedelta(days=1)
            elif unit == "일":
                start, end = today - timedelta(days=count - 1), today
            else:
                # 최근 N개월은 오늘에서 달력 N개월 전 같은 일자부터 현재까지다.
                month_index = today.year * 12 + today.month - 1 - count
                year, month = divmod(month_index, 12)
                start = date(year, month + 1, min(today.day, calendar.monthrange(year, month + 1)[1]))
                end = today
        elif calendar_periods:
            token = calendar_periods[0][0].replace(" ", "")
            if token == "이번달":
                start, end = today.replace(day=1), today
            elif token == "지난달":
                end = today.replace(day=1) - timedelta(days=1)
                start = end.replace(day=1)
            elif token == "이번주":
                start, end = monday, today
            else:
                start, end = monday - timedelta(days=7), monday - timedelta(days=1)
        else:
            if re.search(r"부터|까지|최근|\d\s*(?:주|월|일)|\d{4}[-./]", period_text):
                raise RecordingsReportOptionsError("기간을 '최근 4주', '8월부터 지금까지', '2026-08-01 ~ 2026-09-20'처럼 알려줘")
            start = monday - timedelta(weeks=max(4, (decline or 0) + 1))
            end = monday - timedelta(days=1)
    except (ValueError, OverflowError) as exc:
        if isinstance(exc, RecordingsReportOptionsError):
            raise
        raise RecordingsReportOptionsError("실제 존재하는 날짜로 조회 기간을 확인해줘") from exc

    if start > end or end > today:
        raise RecordingsReportOptionsError("시작일·종료일 순서를 확인하고 오늘까지의 기간으로 지정해줘")
    if (end - start).days >= 366:
        raise RecordingsReportOptionsError("추이 조회는 한 번에 최대 366일까지 가능해")
    query = RecordingsTrendQuery(start, end, decline)
    if decline is not None and sum(complete for _, _, complete in trend_periods(query, today=today)) < decline + 1:
        raise RecordingsReportOptionsError(f"{decline}주 연속 감소는 완료된 온전한 주 {decline + 1}개 이상이 필요해")
    return query


def trend_periods(query: RecordingsTrendQuery, *, today: date) -> tuple[tuple[date, date, bool], ...]:
    """양끝의 부분 주를 보존하며 온전하고 완료된 월~일 주간만 비교 대상으로 표시한다."""

    periods = []
    start = query.start
    while start <= query.end:
        end = min(start + timedelta(days=6 - start.weekday()), query.end)
        periods.append((start, end, start.weekday() == 0 and end.weekday() == 6 and end < today))
        start = end + timedelta(days=1)
    return tuple(periods)
