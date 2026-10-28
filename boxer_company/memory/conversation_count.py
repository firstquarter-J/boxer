"""저장된 발췌의 사전 일치와 집계 안내만 담당하는 provider 없는 계약이다."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from boxer_company.memory.live_query import (
    is_supported_stored_period,
    live_query_period_error,
    resolve_live_query_period,
)

PROFANITY_CLASSIFIER_VERSION = "ko_profanity_messages_v1"
CONVERSATION_COUNT_KIND = "profanity_messages"

_KST = ZoneInfo("Asia/Seoul")
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_TIMESTAMP = re.compile(r"-?[0-9]{1,20}\.[0-9]{1,9}")
_AUTHOR_ID = re.compile(r"[UW][A-Z0-9]{1,30}")
_QUERY_SUBJECT = re.compile(
    r"(?<![가-힣a-z0-9_])욕(?:설|\s*표현)?(?=$|[\s?!.,]|을|은|는|이|가|도|만|한|하|했|해|들|횟수|개수)"
)
_QUERY_COUNT = re.compile(r"몇\s*(?:번|건|개|회)|횟수|개수|(?:메시지|발언)\s*수|얼마나\s*(?:했|많)")
_QUERY_NUMBER = re.compile(r"욕(?:설|\s*표현)?\s*(?:메시지\s*)?수(?=$|[\s?!.,]|가|는|를)")
_QUERY_POLICY = re.compile(r"몇\s*(?:번|건|개|회).{0,20}(?:하면|해야|까지).{0,20}(?:차단|금지|제재|허용)")
# 실제로 한 욕의 표현 조회도 같은 DB 판정으로 보낸다. 단순한 뜻, 예시와 창작 요청은 고르지 않는다.
_QUERY_SPOKEN_SUBJECT = re.compile(
    r"(?<![가-힣a-z0-9_])(?:한|했던|하던|했었던|쓴|썼던|내뱉은|사용한)\s*욕(?:설|\s*표현)?|"
    r"(?<![가-힣a-z0-9_])욕(?:설|\s*표현)?\s*(?:을|를)?\s*(?:한|했던|하던|쓴|썼던)"
    r"\s*(?:메시지|발언|기록|목록|내역|모음|말|것|거)(?=$|[\s?!.,]|들|을|를|은|는)"
)
# 검색창처럼 과거 발언 대상만 적은 명사구도 조회다. 전체 대안을 끝까지 닫아 뒤의 설명이나 명령은 선점하지 않는다.
_QUERY_SPOKEN_NOUN_END = re.compile(
    r"(?:" + _QUERY_SPOKEN_SUBJECT.pattern + r")"
    r"(?:\s*(?:메시지|발언|기록|목록|내역|모음))?(?:들)?[?!.。！？]*\s*$"
)
# 닫힌 명사구라도 제목이나 예시로 제시한 텍스트는 실제 과거 발언 조회가 아니다.
_QUERY_NOUN_META = re.compile(
    r"(?<![가-힣a-z0-9_])(?:제목|문구|문장|문자열|소재|예시|예제|인용)(?=$|[\s:=]|은|는|이|가|을|를|로)"
)
_QUERY_NOUN_UNSPOKEN_PREFIX = re.compile(
    r"(?:(?<![가-힣a-z0-9_])(?:안|못)|려(?:고|던)|고자|기\s*위해)\s*$"
)
_QUERY_LOOKUP_END = re.compile(
    r"(?:(?:보여|알려|찾아|정리해)\s*(?:줘|주라|줄래|주세요|줄\s*수\s*있(?:어|을까))|"
    r"목록|모음|내역|뭐야|뭐였어|무엇이야)[?!.。！？]*\s*$"
)
_QUERY_SPOKEN_QUESTION = re.compile(
    r"(?:무슨|어떤)\s*욕(?:설|\s*표현)?\s*(?:을|를)?\s*(?:했|썼|내뱉었|사용했)"
    r"(?:어|지|니|는지)(?:\s*(?:알려|보여)\s*(?:줘|줄래|주세요))?[?!.。！？]*\s*$"
)
_QUERY_LOOKUP_TRANSFORM = re.compile(
    r"다듬(?:어|어서)|고쳐|수정해|편집해|번역해|바꿔|줄여|늘려|작성해|만들어|생성해|써서|소재로|"
    r"(?:문구|문장|문자열).{0,40}(?:뜻|의미|설명)"
)

# 사전은 표현의 출현만 판정한다. '개'나 '죽' 같은 일반 어근을 욕설로 확장하지 않는다.
# 호환 자모를 NFKC로 정규화하므로 초성 표현도 같은 표기로 비교한다.
_LEFT = r"(?<![가-힣a-z0-9_])"
_TERMS = (
    ("byeongsin", re.compile(_LEFT + r"(?:개)?병신")),
    ("ssibal", re.compile(_LEFT + r"(?:개|아)?(?:씨발|시발(?!점|역|차|열차|시간)|시바(?!견|이누|신))")),
    ("jonna", re.compile(_LEFT + r"(?:존나|" + unicodedata.normalize("NFKC", "ㅈㄴ") + r")")),
    ("gaesaekki", re.compile(_LEFT + r"개새끼")),
    ("jiral", re.compile(_LEFT + r"지랄")),
    ("jot", re.compile(_LEFT + r"좆")),
)
# matcher의 유한 일치 표기만 표시한다. 호환 초성은 정규화 전 사용자 표기로 되돌린다.
PROFANITY_EXPRESSION_ALLOWLIST = frozenset({
    "병신", "개병신", "씨발", "개씨발", "아씨발", "시발", "개시발", "아시발",
    "시바", "개시바", "아시바", "존나", "ㅈㄴ", "개새끼", "지랄", "좆",
})
_EXPRESSION_DISPLAY = {unicodedata.normalize("NFKC", value): value for value in PROFANITY_EXPRESSION_ALLOWLIST}
_CODE = re.compile(r"```[\s\S]*?(?:```|$)|~~~[\s\S]*?(?:~~~|$)|`[^`\n]*(?:`|$)")
_QUOTED = re.compile(r'"[^"\n]*"|“[^”\n]*”|‘[^’\n]*’|\'[^\'\n]*\'|「[^」\n]*」|『[^』\n]*』')
_BLOCK_QUOTED = re.compile(r"^\s*(?:>|&gt;)[^\n]*", re.MULTILINE)
_META = re.compile(
    r"(?:욕설|비속어|금칙어|단어|용어|표현|초성).{0,40}"
    r"(?:뜻|의미|설명|정의|예시|예제|분류|판정|사전|정규식|필터|금지|차단|거절|정책|규칙|쓰지|하지\s*마|사용하지)|"
    r"(?:뜻|의미|정의|정규식|regex|금칙어\s*목록).{0,40}(?:뭐|무엇|알려|설명|예시|추가|포함)"
)
_REPORTED = re.compile(r"\s*(?:이?라고|이?라며|이?라는|이?란|이라니|라니)")
_EXPRESSION_REQUEST = re.compile(
    r"\s*(?:은|는|을|를)?\s*(?:쓰지|사용하지|말하지|출력하지|금지|차단|거절|필터|말해\s*줘|써\s*줘|출력해\s*줘|답해\s*줘)"
)
_NON_PROFANE_CONTEXTS = (
    ("병신", re.compile(r"육십갑자|간지")),
    ("시바", re.compile(r"힌두|파괴의\s*신|인도\s*신|시바\s*신")),
)


def parse_conversation_count_query(
    question: str, before_ts: str, author_ids: Sequence[str] = (),
) -> dict[str, Any] | None:
    """욕 표현의 메시지 수와 실제 표현 조회를 고르고 이름 해석과 접근 권한은 호출자에게 맡긴다."""
    if not isinstance(question, str):
        return None
    text = unicodedata.normalize("NFKC", question).casefold()
    count = _QUERY_COUNT.search(text) or _QUERY_NUMBER.search(text)
    # 인용한 질문과 '문구를 다듬어서 보여줘' 같은 창작은 실제 발언 조회로 선점하지 않는다.
    lookup_text = text
    for pattern in (_CODE, _QUOTED, _BLOCK_QUOTED):
        lookup_text = pattern.sub(" ", lookup_text)
    # 명사구 앞의 부정과 계획은 실제로 한 발언과 구분한다. 기존 명령형의 판정은 유지한다.
    noun = _QUERY_SPOKEN_NOUN_END.search(lookup_text)
    noun_lookup = noun and not _QUERY_NOUN_META.search(lookup_text) and not _QUERY_NOUN_UNSPOKEN_PREFIX.search(
        lookup_text[:noun.start()])
    lookup = not _QUERY_LOOKUP_TRANSFORM.search(lookup_text) and (
        (_QUERY_SPOKEN_SUBJECT.search(lookup_text) and _QUERY_LOOKUP_END.search(lookup_text))
        or _QUERY_SPOKEN_QUESTION.search(lookup_text)
        or noun_lookup
    )
    if not _QUERY_SUBJECT.search(text) or not (count or lookup) or _QUERY_POLICY.search(text):
        return None
    # 잘못된 날짜도 집계 의도로 남겨 일반 최근 조회로 빠지지 않게 한다. 기간 누락은 별도 확인한다.
    period = resolve_live_query_period(question, before_ts)
    error = live_query_period_error(question, before_ts)
    authors = list(dict.fromkeys(value for value in author_ids if isinstance(value, str) and _AUTHOR_ID.fullmatch(value)))
    return {"kind": CONVERSATION_COUNT_KIND, "unit": "messages", "classifierVersion": PROFANITY_CLASSIFIER_VERSION,
            "authorIds": authors, "period": period, "periodError": error}


def _sentence_contexts(text: str) -> list[tuple[int, bool, frozenset[str]]]:
    # 각 문장은 한 번만 검사한다. 같은 욕 표현이 긴 문장에서 반복돼도 전체 문장을 다시 검색하지 않는다.
    # 동음이의어 문맥은 해당 표기에만 적용해 같은 문장의 다른 직접 표현을 지우지 않는다.
    contexts: list[tuple[int, bool, frozenset[str]]] = []
    first = 0
    for match in re.finditer(r"[\n.!?]", text):
        sentence = text[first:match.start()]
        contexts.append((match.start(), bool(_META.search(sentence)), frozenset(
            word for word, pattern in _NON_PROFANE_CONTEXTS if pattern.search(sentence))))
        first = match.end()
    sentence = text[first:]
    contexts.append((len(text), bool(_META.search(sentence)), frozenset(
        word for word, pattern in _NON_PROFANE_CONTEXTS if pattern.search(sentence))))
    return contexts


def classify_profanity_message(text: str) -> dict[str, Any]:
    """고정 사전 일치 메시지를 한 건으로 분류하며 의미나 모욕 의도를 추정하지 않는다."""
    result = {"classifierVersion": PROFANITY_CLASSIFIER_VERSION, "classification": "not_matched",
              "matchedTerms": [], "matchedExpressions": [], "reasons": []}
    if not isinstance(text, str) or not text:
        return result
    normalized = unicodedata.normalize("NFKC", text).casefold()
    # 인용 구간도 일치마다 전체 목록을 순회하지 않는다. 각 패턴의 서로 겹치지 않는 범위를 표시한다.
    protected = bytearray(len(normalized))
    for pattern in (_CODE, _QUOTED, _BLOCK_QUOTED):
        for match in pattern.finditer(normalized):
            protected[match.start():match.end()] = b"\1" * (match.end() - match.start())
    contexts = _sentence_contexts(normalized)
    clear, clear_expressions, uncertain = set(), set(), set()
    for term, pattern in _TERMS:
        context_index = 0
        for match in pattern.finditer(normalized):
            # 각 사전 패턴의 match는 시간순이므로 문장 경계도 앞으로만 진행한다.
            while contexts[context_index][0] < match.end():
                context_index += 1
            if protected[match.start()]:
                uncertain.add("quoted_or_code")
            elif (contexts[context_index][1] or match.group() in contexts[context_index][2]
                  or _REPORTED.match(normalized, match.end())
                  or _EXPRESSION_REQUEST.match(normalized, match.end())
                  or normalized.startswith("병신년", match.start())):
                uncertain.add("mention_or_explanation")
            else:
                clear.add(term)
                clear_expressions.add(_EXPRESSION_DISPLAY[match.group()])
    # 같은 메시지의 명확한 직접 표현은 한 번만 센다. 인용만 있는 메시지는 애매한 집합에만 둔다.
    result["classification"] = "matched" if clear else "ambiguous" if uncertain else "not_matched"
    result["matchedTerms"] = sorted(clear)
    result["matchedExpressions"] = sorted(clear_expressions)
    result["reasons"] = sorted(uncertain)
    return result


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value):
        return None
    try:
        stamp = Decimal(value)
        seconds = int(stamp)
        return (_EPOCH + timedelta(seconds=seconds, microseconds=int((stamp - seconds) * 1_000_000))).astimezone(_KST)
    except (ValueError, OverflowError):
        return None


def _period_label(period: Any) -> str:
    if not isinstance(period, Mapping):
        return "지정한 기간"
    start, end, cutoff = (_timestamp(period.get(key)) for key in ("startTs", "endTs", "cutoffTs"))
    if start is None or end is None or cutoff is None or not start < end or start > cutoff:
        return "지정한 기간"
    if Decimal(period["cutoffTs"]) < Decimal(period["endTs"]):
        return f"{start:%Y-%m-%d}부터 질문 시각({cutoff:%Y-%m-%d %H:%M:%S} KST)까지"
    return f"{start:%Y-%m-%d}~{end - timedelta(microseconds=1):%Y-%m-%d}(KST)"


def format_conversation_count_answer(result: Mapping[str, Any]) -> str:
    """저장된 발췌의 판정만 안내하고 비어 있는 자료를 실제 발언 0회로 바꾸지 않는다."""
    failure = "저장된 발췌의 집계 결과를 확인할 수 없어. 전체 횟수는 확인할 수 없어."
    unsupported = "2026년 1월 1일부터 저장된 대화만 지원해. 그 이전이 포함되지 않도록 기간을 지정해줘."
    status = result.get("status")
    if status == "unsupported_period":
        return unsupported
    period_error = result.get("periodError")
    if isinstance(period_error, str) and period_error in {"invalid", "ambiguous", "future"}:
        return {"invalid": "날짜나 범위가 올바르지 않아. 집계할 기간을 다시 알려줘.",
                "ambiguous": "집계할 기간이 여러 가지로 읽혀. 하나의 기간을 지정해줘.",
                "future": "미래에 시작하는 기간은 집계할 수 없어."}[period_error]
    if (status != "stored" or result.get("scope") != "stored_excerpts"
            or result.get("classifierVersion") != PROFANITY_CLASSIFIER_VERSION):
        return failure
    label = _period_label(result.get("period"))
    if label == "지정한 기간":
        return "집계할 기간을 정확히 지정해줘. 전체 횟수는 아직 확인할 수 없어."
    if not is_supported_stored_period(result.get("period")):
        return unsupported
    # 이름은 서버에서 확인한 단순 표시 문자열만 허용하고 임의 원문이나 상태 오류는 내보내지 않는다.
    display_name = result.get("authorDisplayName")
    target = display_name + "의 " if isinstance(display_name, str) and re.fullmatch(r"[가-힣a-zA-Z0-9 _.-]{1,80}", display_name) else ""
    counts = [result.get(key) for key in ("matchedMessageCount", "ambiguousMessageCount", "scannedMessageCount",
                                        "unavailableExcerptMessageCount", "eligibleStoredMessageCount")]
    if (any(type(value) is not int or value < 0 for value in counts)
            or sum(counts[:2]) > counts[2] or counts[2] + counts[3] != counts[4]):
        return failure
    matched, ambiguous, scanned, unavailable, _ = counts
    # 발췌 원문을 그대로 출력하지 않고 명확한 일치 수와 일관된 유한 표시값만 받아들인다.
    expressions = result.get("matchedExpressions")
    if (not isinstance(expressions, list)
            or any(not isinstance(value, str) or value not in PROFANITY_EXPRESSION_ALLOWLIST for value in expressions)
            or bool(matched) != bool(expressions)):
        return failure
    # source metadata만 남았거나 저장 자료 자체가 없으면 0건으로 답하지 않는다.
    if scanned == 0:
        answer = f"{label}, {target}판정할 저장 발췌가 없어 욕 표현이 확인된 메시지 수를 확인할 수 없어. 0회라는 뜻은 아니야."
    else:
        answer = (f"{label}, {target}저장된 발췌 {scanned}건 중 욕 표현이 확인된 메시지는 {matched}건이야. "
                  f"애매한 인용이나 설명 {ambiguous}건은 합계에서 제외했어. "
                  "한 메시지에 여러 표현이 있어도 1건으로 셌어. 전체 대화의 욕설 횟수는 아니야.")
    if matched:
        answer += "\n확인된 욕 표현: " + ", ".join(f"`{value}`" for value in sorted(set(expressions))) + "."
    if unavailable:
        answer += f" 판정에 쓸 수 있는 발췌가 없는 저장 메시지 {unavailable}건은 제외했어."
    return answer
