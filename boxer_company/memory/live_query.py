"""API와 Slack이 같은 질문에만 비공개 원문 조회를 실행한다."""

from __future__ import annotations

import re
import unicodedata
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo


_TIMESTAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_RELATIVE_DAY = re.compile(r"(?<![가-힣a-z0-9_])(오늘|어제)(?:은|는|이|가|을|를|의|도|만|에|엔|과|와|랑)?(?![가-힣a-z0-9_])")
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_KST = ZoneInfo("Asia/Seoul")


def resolve_live_query_period(question: str, before_ts: str) -> dict[str, str] | None:
    """명확한 오늘·어제만 질문이 작성된 KST 날짜의 반개구간으로 해석한다."""
    if not isinstance(question, str) or not isinstance(before_ts, str) or _TIMESTAMP.fullmatch(before_ts) is None:
        return None
    days = {match.group(1) for match in _RELATIVE_DAY.finditer(unicodedata.normalize("NFKC", question).casefold())}
    if len(days) != 1:
        return None
    try:
        seconds, fraction = before_ts.split(".")
        # 날짜 계산만 microsecond로 자르고 실제 포함 여부는 아래 Decimal 비교로 정밀도를 유지한다.
        asked_at = (_EPOCH + timedelta(seconds=int(seconds), microseconds=int(fraction[:6].ljust(6, "0")))).astimezone(_KST)
        label = next(iter(days))
        start = asked_at.replace(hour=0, minute=0, second=0, microsecond=0)
        if label == "어제":
            start -= timedelta(days=1)
        end = start + timedelta(days=1)
        # float 변환 없이 UTC 경계의 정수 초를 계산해 자정·질문 시각 경계를 보존한다.
        start_delta, end_delta = start.astimezone(timezone.utc) - _EPOCH, end.astimezone(timezone.utc) - _EPOCH
        start_ts = str(start_delta.days * 86400 + start_delta.seconds) + ".000000"
        end_ts = str(end_delta.days * 86400 + end_delta.seconds) + ".000000"
        return {"kind": "today" if label == "오늘" else "yesterday", "referenceDateKst": asked_at.date().isoformat(),
                "startTs": start_ts, "endTs": end_ts, "startAtKst": start.isoformat(),
                "endAtKst": end.isoformat(), "cutoffTs": before_ts}
    except (ValueError, OverflowError):
        return None


def is_within_live_query_period(message_ts: str, period: Mapping[str, str]) -> bool:
    """시작 자정 포함·다음 자정 제외와 원 질문 시각 포함을 함께 적용한다."""
    values = (message_ts, period.get("startTs"), period.get("endTs"), period.get("cutoffTs"))
    if any(not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None for value in values):
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
    return bool(reference and query)
