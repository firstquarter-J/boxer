"""저장된 욕 발언 발췌를 시간순과 저장 주제로만 표시하는 결정적 formatter다."""

from __future__ import annotations

import re
from collections import OrderedDict
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from .conversation_count import (
    PROFANITY_CLASSIFIER_VERSION,
    PROFANITY_EXPRESSION_ALLOWLIST,
    _period_label,
    _timestamp,
    classify_profanity_message,
    format_conversation_count_answer,
)
from .live_query import is_supported_stored_period
from .policy import is_safe_memory_text
from .raw_presentation import is_safe_raw_display_text

MAX_HISTORY_MESSAGES = 20
MAX_HISTORY_ANSWER_CHARS = 16_000
_MAX_EXCERPT_CHARS = 40000
_DISPLAY_EXCERPT_CHARS = 240
_MAX_TOPIC_CHARS = 160
_STAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_URL = re.compile(r"<(?:https?://|www\.)[^>\r\n]*>|(?:https?://|ftp://|www\.)[^\s<>]+", re.IGNORECASE)
_MENTION = re.compile(r"<(?:@|!)[^>\r\n]*>")
_PLAIN_SYMBOLS = str.maketrans({
    "@": "＠", "<": "＜", ">": "＞", "`": "｀", "*": "＊", "_": "＿",
    "~": "～", "[": "［", "]": "］", "(": "（", ")": "）", "#": "＃",
    "|": "｜", "\\": "＼", "&": "＆",
    ".": "．", ":": "：", "/": "／", "!": "！",
})
_INVALID = "저장된 발췌의 발언 목록을 확인할 수 없어. 집계와 목록 근거의 확인이 필요해."
_UNKNOWN_TOPIC = "주제 확인 어려움"


def _safe_text(value: Any, limit: int, *, raw: bool = False) -> bool:
    return bool(
        isinstance(value, str) and value.strip() and len(value) <= limit
        and not any((ord(char) < 32 and char not in "\n\r\t") or ord(char) == 127 for char in value)
        and (is_safe_raw_display_text(value) if raw else is_safe_memory_text(value))
    )


def _display_text(value: str) -> str:
    # Slack bridge도 서식 토큰을 찾으므로 표시용 문자는 평문으로 치환한다.
    # 링크와 멘션은 원문에서 가져와도 실행 가능한 전송 토큰으로 만들지 않는다.
    text = _MENTION.sub("[멘션 생략]", value)
    text = _URL.sub("[링크 생략]", text)
    return " ".join(text.split()).translate(_PLAIN_SYMBOLS)


def _valid_summary(result: Mapping[str, Any]) -> bool:
    counts = [result.get(key) for key in (
        "matchedMessageCount", "ambiguousMessageCount", "scannedMessageCount",
        "unavailableExcerptMessageCount", "eligibleStoredMessageCount",
    )]
    if any(type(value) is not int or not 0 <= value < 2**63 for value in counts):
        return False
    matched, ambiguous, scanned, unavailable, eligible = counts
    raw_count = result.get("storedRawMessageCount", 0)
    if type(raw_count) is not int or not 0 <= raw_count <= scanned:
        return False
    expressions = result.get("matchedExpressions")
    return bool(
        result.get("scope") == "stored_excerpts"
        and result.get("classifierVersion") == PROFANITY_CLASSIFIER_VERSION
        and matched + ambiguous <= scanned and scanned + unavailable == eligible
        and isinstance(expressions, list) and len(expressions) <= len(PROFANITY_EXPRESSION_ALLOWLIST)
        and all(isinstance(value, str) and value in PROFANITY_EXPRESSION_ALLOWLIST for value in expressions)
        and len(expressions) == len(set(expressions)) and bool(matched) == bool(expressions)
    )


def _validated_messages(result: Mapping[str, Any]) -> list[Mapping[str, Any]] | None:
    messages, limit, truncated = (result.get(key) for key in ("matchedMessages", "messageLimit", "messagesTruncated"))
    matched = result["matchedMessageCount"]
    if (not isinstance(messages, list) or type(limit) is not int or limit != MAX_HISTORY_MESSAGES
            or type(truncated) is not bool or len(messages) > min(limit, matched)
            or truncated != (len(messages) < matched)):
        return None
    period = result["period"]
    start, end, cutoff = (Decimal(period[key]) for key in ("startTs", "endTs", "cutoffTs"))
    all_expressions = set(result["matchedExpressions"])
    shown_expressions: set[str] = set()
    excerpt_limit = _MAX_EXCERPT_CHARS if result.get("storedRawMessageCount") else 1000
    for message in messages:
        if not isinstance(message, Mapping):
            return None
        stamp, excerpt, expressions, topic = (message.get(key) for key in (
            "messageTs", "excerpt", "matchedExpressions", "topic",
        ))
        if (not isinstance(stamp, str) or _STAMP.fullmatch(stamp) is None or _timestamp(stamp) is None
                or not start <= Decimal(stamp) < end or Decimal(stamp) > cutoff
                or not _safe_text(excerpt, excerpt_limit, raw=bool(result.get("storedRawMessageCount")))
                or not isinstance(expressions, list) or not expressions
                or len(expressions) > len(PROFANITY_EXPRESSION_ALLOWLIST)
                or any(not isinstance(value, str) or value not in PROFANITY_EXPRESSION_ALLOWLIST for value in expressions)
                or len(expressions) != len(set(expressions))
                or topic is not None and not _safe_text(topic, _MAX_TOPIC_CHARS)):
            return None
        # 주제 요약이나 선언된 표현이 아닌 실제 저장 발췌의 동일 사전 일치만 표시한다.
        classified = classify_profanity_message(excerpt)
        actual = set(classified["matchedExpressions"])
        if classified["classification"] != "matched" or actual != set(expressions) or not actual <= all_expressions:
            return None
        shown_expressions.update(actual)
    if not truncated and shown_expressions != all_expressions:
        return None
    # 같은 시각의 서로 다른 채널 메시지는 저장소 순서를 안정적으로 보존한다.
    return sorted(messages, key=lambda message: Decimal(message["messageTs"]))


def format_conversation_history_answer(result: Mapping[str, Any]) -> str:
    """기존 집계 안내 뒤에 검증된 발췌 목록과 저장 주제별 항목 번호를 붙인다."""
    try:
        summary = format_conversation_count_answer(result)
        if result.get("status") != "stored" or result.get("periodError"):
            return summary
        period = result.get("period")
        if not isinstance(period, Mapping) or _period_label(period) == "지정한 기간" or not is_supported_stored_period(period):
            return summary
        if not _valid_summary(result):
            return _INVALID
        messages = _validated_messages(result)
        if messages is None:
            return _INVALID
        matched, unavailable = result["matchedMessageCount"], result["unavailableExcerptMessageCount"]
        if matched == 0:
            # 발췌 없는 메시지는 욕의 존재나 주제를 알 수 없으며 사례를 채워 넣지 않는다.
            answer = summary + "\n\n시간순으로 보여줄 욕 발언은 저장된 발췌에서 확인되지 않았어. 전체 발언이 0회라는 뜻은 아니야."
            if unavailable:
                answer += f" 발췌가 없는 {unavailable}건의 발언 내용과 주제는 확인할 수 없어."
            return answer if len(answer) <= MAX_HISTORY_ANSWER_CHARS else _INVALID

        material_note = ("보존된 원문과 발췌의 일부를 표시했어. 질문 시점의 Slack 원문을 새로 읽지는 않았어. 전후 대화의 맥락은 저장된 근거로 확인되는 범위만 다뤄."
                         if result.get("storedRawMessageCount") else
                         "저장된 짧은 발췌만 표시했어. Slack 원문을 새로 확인한 결과는 아니야. 원문 전문이나 전후 대화는 확인할 수 없어.")
        parts = [summary, "", material_note,
                 f"확인된 {matched}건 중 표시 가능한 발췌 {len(messages)}건을 최대 {MAX_HISTORY_MESSAGES}건 범위로 표시했어."]
        if result["messagesTruncated"]:
            parts.append(f"나머지 {matched - len(messages)}건은 표시 상한 또는 발췌 표시 조건 때문에 생략됐어.")
        if not messages:
            parts.append("시간순으로 보여줄 수 있는 발췌와 저장된 주제를 확인할 수 없어.")
        else:
            parts.extend(("", "**날짜와 시간순 (KST)**"))
            topics: OrderedDict[str, list[int]] = OrderedDict()
            for number, message in enumerate(messages, start=1):
                shown = _display_text(message["excerpt"])
                if len(shown) > _DISPLAY_EXCERPT_CHARS:
                    shown = shown[:_DISPLAY_EXCERPT_CHARS] + "… [발췌 일부 생략]"
                stamp = _timestamp(message["messageTs"])
                # 발췌 앞부분이 잘려도 이 메시지에서 확인한 안전 표현은 항상 보인다.
                expressions = ", ".join(f"`{value}`" for value in sorted(message["matchedExpressions"]))
                parts.extend((f"{number}. {stamp:%Y-%m-%d %H:%M:%S} KST",
                              f"   확인된 표현: {expressions}", f"   발췌: {shown}"))
                topics.setdefault(message["topic"] or _UNKNOWN_TOPIC, []).append(number)
            parts.extend(("", "**저장된 주제별**"))
            for topic, numbers in topics.items():
                references = ", ".join(f"{number}번" for number in numbers)
                parts.append(f"- {_display_text(topic)}: {references}")
            parts.append("주제는 표시한 발췌에 연결된 저장 분류야.")
        answer = "\n".join(parts)
        return answer if len(answer) <= MAX_HISTORY_ANSWER_CHARS else _INVALID
    except (KeyError, TypeError, ValueError, OverflowError):
        # 손상된 내부 결과를 부분 목록이나 모델 추정으로 대체하지 않는다.
        return _INVALID


__all__ = ["format_conversation_history_answer"]
