"""대화 질문의 날짜 범위를 해석하고 저장 기록의 지원 시작일을 검사한다."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo


_TIMESTAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
# 날짜 양옆의 단어 경계를 지켜 버전, 기기 번호, 오늘날 같은 일반 문구를 날짜로 읽지 않는다.
_TOKEN_START = r"(?:(?<![가-힣a-z0-9_./-])|(?<=부터))"
_TOKEN_END = r"(?=(?:부터|까지|이전|이후)|(?:은|는|이|가|을|를|의|도|만|에|엔|과|와|랑)?(?![가-힣a-z0-9_./-]))"
_EXPRESSION = re.compile(
    _TOKEN_START + r"(?:"
    r"(?P<numeric>[0-9]{4}[-./][0-9]{1,3}[-./][0-9]{1,3})|"
    r"(?P<calendar>(?:[0-9]{4}\s*년(?:\s*[0-9]{1,3}\s*월(?:\s*[0-9]{1,3}\s*일)?)?|"
    r"(?:올해|작년)\s*[0-9]{1,3}\s*월(?:\s*[0-9]{1,3}\s*일)?|"
    r"[0-9]{1,3}\s*월(?:\s*[0-9]{1,3}\s*일)?))|"
    r"(?P<recent>(?:최근|지난)\s*[0-9]+\s*일)|"
    r"(?P<relative>오늘|어제|그제|올해|작년|이번\s*달|지난\s*달|이번\s*주|지난\s*주)"
    r")" + _TOKEN_END,
)
_SHORT_RANGE_END = re.compile(r"\s*(?:~|부터)\s*(?P<day>[0-9]{1,3})\s*일" + _TOKEN_END)
_RANGE_JOIN = re.compile(r"\s*(?:~|부터)\s*")
_OPEN_RANGE = re.compile(r"\s*(?:~|부터|까지)")
# '2026년이전대화'처럼 붙여 쓴 날짜 방향도 같은 과거 범위로 판정한다.
_DATE_DIRECTION = re.compile(r"\s*(이전|이후|전|후)(?:까지|부터)?(?:의|에|도|만|과|와)?(?=$|[^가-힣a-z0-9_./-]|대화|발언|기록|얘기|이야기|한\s*말)")
_DIRECTION_WORD = re.compile(r"(?<![가-힣a-z0-9_./-])(?:이전|이후|전|후)(?:까지|부터)?(?:의|에|도|만|과|와)?(?![가-힣a-z0-9_./-])")
_SINCE_RANGE = re.compile(r"\s*부터(?:\s*(?:지금|현재)\s*까지)?(?=$|[\s?!.,])")
_BOUNDARY_TIMESTAMP = re.compile(r"-?[0-9]{1,20}\.[0-9]{1,9}")
_UNITLESS_RANGE = re.compile(r"(?<![a-z0-9_./-])[0-9]+\s*~\s*[0-9]+\s*(?:월|일)")
_RELATIVE_LABEL = r"오늘|어제|그제|올해|작년|이번\s*달|지난\s*달|이번\s*주|지난\s*주"
_CONNECTED_RELATIVES = re.compile(
    _TOKEN_START + r"(?P<left>" + _RELATIVE_LABEL + r")\s*(?:과|와)\s*(?P<right>"
    + _RELATIVE_LABEL + r")" + _TOKEN_END,
)
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_KST = ZoneInfo("Asia/Seoul")
# 날짜 해석 자체는 일반형으로 두고 저장된 대화의 지원 하한만 공통으로 검사한다.
STORED_HISTORY_START_TS = "1767193200.000000"


@dataclass(frozen=True)
class _DateExpression:
    first: int
    last: int
    start: datetime
    end: datetime
    kind: str


def _month_end(start: datetime) -> datetime:
    return start.replace(year=start.year + 1, month=1) if start.month == 12 else start.replace(month=start.month + 1)


def _expression_bounds(match: re.Match[str], day: datetime) -> tuple[datetime, datetime, str]:
    label = re.sub(r"\s+", "", match.group())
    if match.group("relative"):
        if label in {"오늘", "어제", "그제"}:
            start = day - timedelta(days={"오늘": 0, "어제": 1, "그제": 2}[label])
            return start, start + timedelta(days=1), {"오늘": "today", "어제": "yesterday", "그제": "date"}[label]
        if label in {"올해", "작년"}:
            start = day.replace(year=day.year - (label == "작년"), month=1, day=1)
            return start, start.replace(year=start.year + 1), "year"
        if label in {"이번달", "지난달"}:
            start = day.replace(day=1)
            if label == "지난달":
                start = (start - timedelta(days=1)).replace(day=1)
            return start, _month_end(start), "month"
        start = day - timedelta(days=day.weekday() + (7 if label == "지난주" else 0))
        return start, start + timedelta(days=7), "week"
    if match.group("recent"):
        count = int(re.search(r"[0-9]+", label).group())
        if not 1 <= count <= 3660:
            raise ValueError("recent_day_count_invalid")
        return day - timedelta(days=count - 1), day + timedelta(days=1), "recent_days"
    if match.group("numeric"):
        parts = re.split(r"[-./]", label)
        if len(set(re.findall(r"[-./]", label))) != 1:
            raise ValueError("date_separator_invalid")
        start = day.replace(year=int(parts[0]), month=int(parts[1]), day=int(parts[2]))
        return start, start + timedelta(days=1), "date"
    year = re.search(r"([0-9]+)년", label)
    month = re.search(r"([0-9]+)월", label)
    date = re.search(r"([0-9]+)일", label)
    start = day.replace(year=int(year.group(1)) if year else day.year - label.startswith("작년"),
                        month=int(month.group(1)) if month else 1, day=int(date.group(1)) if date else 1)
    if date:
        return start, start + timedelta(days=1), "date"
    if month:
        return start, _month_end(start), "month"
    return start, start.replace(year=start.year + 1), "year"


def _analyze_live_query_period(question: str, before_ts: str) -> tuple[dict[str, str] | None, str | None]:
    # 해석과 오류 분류가 같은 경로를 쓰므로 API와 Slack이 잘못된 날짜를 최근 조회로 우회하지 않는다.
    if not isinstance(question, str) or not isinstance(before_ts, str) or _TIMESTAMP.fullmatch(before_ts) is None:
        return None, None
    try:
        seconds, fraction = before_ts.split(".")
        # 날짜 계산만 microsecond로 자르고 실제 포함 여부는 아래 Decimal 비교로 정밀도를 유지한다.
        asked_at = (_EPOCH + timedelta(seconds=int(seconds), microseconds=int(fraction[:6].ljust(6, "0")))).astimezone(_KST)
    except (ValueError, OverflowError):
        return None, None
    text = unicodedata.normalize("NFKC", question).casefold()
    # 단위가 생략된 범위를 일부 월로 오독하지 않고 명백히 붙은 복수 상대 날짜도 닫는다.
    if _UNITLESS_RANGE.search(text):
        return None, "invalid"
    if any(re.sub(r"\s+", "", match.group("left")) != re.sub(r"\s+", "", match.group("right"))
           for match in _CONNECTED_RELATIVES.finditer(text)):
        return None, "ambiguous"
    day = asked_at.replace(hour=0, minute=0, second=0, microsecond=0)
    expressions: list[_DateExpression] = []
    try:
        for match in _EXPRESSION.finditer(text):
            start, end, kind = _expression_bounds(match, day)
            expressions.append(_DateExpression(match.start(), match.end(), start, end, kind))
        if not expressions:
            return None, None
        # 끝의 '15일'은 범위 안에서만 시작 월과 연도를 상속하며 역전 날짜를 자동 이월하지 않는다.
        if len(expressions) == 1:
            first = expressions[0]
            short = _SHORT_RANGE_END.match(text, first.last)
            if short:
                start = first.start.replace(day=int(short.group("day")))
                expressions.append(_DateExpression(short.start("day"), short.end(), start, start + timedelta(days=1), "date"))
        directions = [_DATE_DIRECTION.match(text, item.last) for item in expressions]
        if any(directions):
            # '2026년 이전'을 2026년 한 해로 오독하지 않는다. 시작 없는 과거 범위는
            # 저장 지원 하한으로 잘라낼 수 없으며 여러 날짜나 방향이 섞이면 먼저 닫는다.
            if len(expressions) != 1:
                return None, "ambiguous"
            direction = directions[0]
            if _DIRECTION_WORD.search(text[direction.end():]):
                return None, "ambiguous"
            return None, "unbounded_past" if direction.group(1) in {"이전", "전"} else "invalid"
        if any(re.match(r"\s*(?:이전|이후)", text[item.last:]) for item in expressions):
            return None, "invalid"
        if len(expressions) == 2 and _RANGE_JOIN.fullmatch(text[expressions[0].last:expressions[1].first]):
            first, last = expressions
            if first.start > last.start:
                return None, "invalid"
            start, end, kind = first.start, last.end, "range"
        else:
            if len({(item.start, item.end) for item in expressions}) != 1:
                return None, "ambiguous"
            first = expressions[0]
            since = _SINCE_RANGE.match(text, first.last) if len(expressions) == 1 else None
            if since:
                if _DIRECTION_WORD.search(text[since.end():]):
                    return None, "ambiguous"
                # 종료일을 생략한 '부터'는 질문일까지 열되 실제 발언은 원 질문 cutoff까지만 받는다.
                start, end, kind = first.start, day + timedelta(days=1), "range"
            elif _OPEN_RANGE.match(text, first.last):
                return None, "invalid"
            else:
                start, end, kind = first.start, first.end, first.kind
        if start > asked_at:
            return None, "future"
    except (ValueError, OverflowError):
        return None, "invalid"
    # float 변환 없이 UTC 경계를 계산하고 원 질문의 소수 초는 cutoff에 그대로 둔다.
    def timestamp(value: datetime) -> str:
        delta = value.astimezone(timezone.utc) - _EPOCH
        return str(delta.days * 86400 + delta.seconds) + ".000000"
    return {"kind": kind, "referenceDateKst": asked_at.date().isoformat(),
            "startTs": timestamp(start), "endTs": timestamp(end), "startAtKst": start.isoformat(),
            "endAtKst": end.isoformat(), "cutoffTs": before_ts}, None


def resolve_live_query_period(question: str, before_ts: str) -> dict[str, str] | None:
    """질문의 날짜를 KST 자정 기준 반개구간으로 해석한다."""
    return _analyze_live_query_period(question, before_ts)[0]


def live_query_period_error(question: str, before_ts: str) -> str | None:
    """명시 날짜의 오류를 구분하며 날짜 없는 질문과 손상된 기준 시각에는 추측하지 않는다."""
    return _analyze_live_query_period(question, before_ts)[1]


def is_supported_stored_period(period: Mapping[str, str] | None) -> bool:
    """2026년 KST 시작 이전을 포함하는 기간은 일부만 잘라 조회하지 않고 거절한다."""
    if not isinstance(period, Mapping):
        return False
    values = (period.get("startTs"), period.get("endTs"))
    if any(not isinstance(value, str) or _BOUNDARY_TIMESTAMP.fullmatch(value) is None for value in values):
        return False
    start, end = (Decimal(value) for value in values)
    return Decimal(STORED_HISTORY_START_TS) <= start < end


def strip_live_query_period(question: str, before_ts: str) -> str:
    """유효한 기간 문구만 빼서 날짜 숫자가 원문 검색어로 남지 않게 한다."""
    if resolve_live_query_period(question, before_ts) is None:
        return question
    # casefold가 문자를 늘리는 경우에도 검색 위치와 제거할 본문이 같은 문자열을 가리켜야 한다.
    text = unicodedata.normalize("NFKC", question).casefold()
    matches = list(_EXPRESSION.finditer(text))
    spans = [(match.start(), match.end()) for match in matches]
    if len(matches) == 1:
        short = _SHORT_RANGE_END.match(text, matches[0].end())
        if short:
            spans = [(matches[0].start(), short.end())]
        else:
            since = _SINCE_RANGE.match(text, matches[0].end())
            if since:
                spans = [(matches[0].start(), since.end())]
    elif len(matches) == 2 and _RANGE_JOIN.fullmatch(text[matches[0].end():matches[1].start()]):
        spans = [(matches[0].start(), matches[1].end())]
    # 날짜 바로 뒤의 조사만 함께 제거하며 일반 본문, 버전과 제품 숫자는 그대로 둔다.
    for first, last in reversed(spans):
        suffix = re.match(r"(?:까지)?(?:은|는|이|가|을|를|의|도|만|에|엔|과|와|랑)?", text[last:])
        text = text[:first] + " " + text[last + suffix.end():]
    return re.sub(r"\s+", " ", text).strip()


def is_within_live_query_period(message_ts: str, period: Mapping[str, str]) -> bool:
    """시작 자정 포함·다음 자정 제외와 원 질문 시각 포함을 함께 적용한다."""
    values = (message_ts, period.get("startTs"), period.get("endTs"), period.get("cutoffTs"))
    patterns = (_TIMESTAMP, _BOUNDARY_TIMESTAMP, _BOUNDARY_TIMESTAMP, _TIMESTAMP)
    if any(not isinstance(value, str) or pattern.fullmatch(value) is None for value, pattern in zip(values, patterns)):
        return False
    stamp, start, end, cutoff = (Decimal(value) for value in values)
    return start <= stamp < end and stamp <= cutoff


def wants_private_live_sources(question: str, author_ids: Sequence[str] = ()) -> bool:
    if any(author_ids):
        return True
    # 인사·일반 지식은 조회 실패 안내를 붙이지 않고 기존 답변 흐름을 유지한다.
    text = unicodedata.normalize("NFKC", question).casefold()
    reference = re.search(
        r"슬랙|slack|비공개|채널|대화|발언|이야기|얘기|소식|"
        r"(?:^|\s)(?:한\s*)?말(?:은|이|을|들)?(?=\s|[?!？]|$)", text,
    )
    query = re.search(r"최근|요즘|요새|방금|오늘|어제|지난|마지막|조회|검색|찾|보여|알려|뭐|무슨|어떤|있", text)
    return bool(reference and (query or _EXPRESSION.search(text)))


def wants_stored_history_sources(question: str, author_ids: Sequence[str] = ()) -> bool:
    """기록이나 사람의 저장 사실을 묻는 질문만 고르고 이름이 들어간 일반 요청은 유지한다."""
    if not isinstance(question, str):
        return False
    text = re.sub(r"\s+", "", unicodedata.normalize("NFKC", question).casefold())
    # 현재 초안의 편집이나 창작 요청은 의문형으로 끝나도 저장 사실 조회로 바꾸지 않는다.
    if re.search(
        r"(?:(?:다듬어|고쳐|수정해|편집해|번역해|바꿔|줄여|늘려|써|작성해|만들어|놀려)"
        r"(?:줘|주라|줄래|줄까|줄수있어|줄수있을까)|"
        r"(?:다듬을|고칠|수정할|편집할|번역할|바꿀|줄일|늘릴|쓸|작성할|만들|놀릴)까)[?!.。！？]*$", text,
    ):
        return False
    reference = re.search(r"슬랙|slack|대화|발언|얘기|이야기|(?:한|했던|하던|무슨)말", text)
    lookup = re.search(
        r"(?:(?:요약|정리|조회|검색|확인)(?:해(?:줘|줄래|줄수있어|줄수있을까)?|부탁해)?|"
        r"(?:찾아|보여|알려)(?:줘|주라|줄래|줄수있어|줄수있을까|봐))[?!.。！？]*$", text,
    )
    # '대화하며 행운을 찾아보세요' 같은 서술은 조회 요청이 아니다. 조회 동사나
    # 실제 의문형 발언 표현을 요구해 원문 편집과 팀 fun 문맥을 잘못 지우지 않는다.
    spoken_question = re.search(
        r"(?:뭐랬(?:어|지|니)|뭐라고(?:말)?했(?:어|지|니)|"
        r"(?:무슨|어떤)(?:말|얘기|이야기).{0,20}(?:했|했었|하던)(?:어|지|니|는지)|"
        r"(?:말|얘기|이야기)했(?:어|지|니))[?!.。！？]*$", text,
    )
    interrogative = re.search(r"뭐|무엇|무슨|어떤|누가|언제|어디|어떻게", text)
    question_ending = re.search(r"(?:야|어|지|니|까|나요|예요|인지)[?!.。！？]*$|[?？]$", text)
    explicit_question = interrogative and question_ending
    # 날짜와 기록 이름만 적은 짧은 검색도 문장형 질문과 같은 DB 조회로 취급한다.
    record_fragment = re.search(r"(?:(?:한|했던|하던)말|발언|대화)(?:기록|목록|내역)?[?!.。！？]*$", text)
    if reference and (lookup or spoken_question or explicit_question or record_fragment):
        return True
    if not any(author_ids):
        return False
    if spoken_question:
        return True
    # 사람 이름만으로는 조회하지 않는다. 실제 저장 사실을 묻는 대상과 의문형이 함께 있어야 한다.
    person_fact = re.search(r"사람|성격|성향|취향|선호|좋아하|싫어하|관심사|생일|입사|담당|직무|역할|업무", text)
    about_person = re.search(r"에대해|에관해", text)
    return bool((person_fact and (lookup or explicit_question)) or (about_person and lookup))
