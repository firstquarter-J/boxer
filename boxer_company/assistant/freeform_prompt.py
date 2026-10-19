from __future__ import annotations

import re

from boxer_company import settings as cs


_FREEFORM_COMPARISON_HINTS = (
    " vs ",
    "누가",
    "전투력",
    "상성",
    "서열",
    "더 세",
    "더 쎄",
    "누가 이겨",
    "우위",
)
_FREEFORM_PLAYFUL_HINTS = (
    "놀려",
    "드립",
    "농담",
    "웃기",
    "한마디",
    "밈",
    "모대",
)
_FREEFORM_ADVICE_HINTS = (
    "어떻게",
    "추천",
    "골라",
    "선택",
    "판단",
    "하는 게 낫",
    "말까",
    "갈까",
)

_FACTUAL_RECAP_SUBJECTS = (
    "발언", "한 말", "한말", "말한", "대화", "채팅", "메시지", "스레드", "댓글",
)
_FACTUAL_RECAP_ACTIONS = (
    "요약", "정리", "최근", "오늘", "어제", "지난", "기록", "찾아", "확인", "마지막",
)
_EXPLICIT_PLAYFUL_REQUEST = re.compile(
    r"(?:놀려(?:줘|봐|라|줄래)?|로스트해(?:줘|봐)?|드립\s*(?:쳐(?:줘|봐)?|만들어(?:줘|봐)?))"
    r"(?=$|[\s.!?])|"
    r"(?:드립으로|밈으로|농담으로|캐릭터로|웃기게|재밌게)\s*"
    r"(?:(?:좀|한번|다시|짧게)\s*)*"
    r"(?:(?:요약|정리|설명|분석|표현|해석|답변|답|말)해(?:줘|봐|줄래|라|보자)?|써줘|만들어줘)"
    r"(?=$|[\s.!?])"
)
_REPORTED_SPEECH_QUERY = re.compile(
    r"뭐랬|뭐라\s*했|(?:뭐라고|무슨\s*말|어떤\s*말).*(?:했|한\s*거|한\s*말|하(?:고|는)\s*있)"
)
_FACTUAL_RECAP_IDENTITY = (
    "너는 Boxer다. 현재 질문에 필요한 확인된 대화 기록을 정확하게 요약해. "
    "기본적으로 한국어 반말을 사용하고, 현재 질문자의 명시적인 답변 형식과 검증된 표준 답변 선호를 따라."
)
_FACTUAL_RECAP_RULES = (
    "대화 기록 요약 규칙:\n"
    "- 현재 질문의 대상과 기간을 기준으로 확인된 발언만 요약해. 원문·출처가 없으면 확인할 수 없다고 말해.\n"
    "- 이전 봇의 성격 평가나 추정은 대상의 실제 발언 근거가 아니야. 과거의 인물 평가로 비어 있는 대화 맥락을 채우지 마.\n"
    "- 주제·스레드별로 누가 어떤 말에 답했는지 먼저 파악하고 핵심 대화와 결론을 정리해. "
    "서로 다른 주제나 스레드를 한 사람의 연속된 감정 변화로 연결하지 마.\n"
    "- 대상 발언과 주변 맥락의 작성자를 구별해. 다른 사람의 말이나 대상을 향한 평가를 대상 본인의 발언으로 귀속하지 마.\n"
    "- 부모 메시지와 앞뒤 대화는 해당 발언의 의미를 확인하는 근거야. 주변 채널 발언은 직접 답글 관계가 확인된 것이 아니야. "
    "근거가 부족하면 대답의 의도·감정은 확인할 수 없다고 말해.\n"
    "- 같은 스레드의 명시적인 질문·설명·정정은 함께 반영해 무엇에 대한 응답이었는지 요약해. "
    "감정·성격을 모른다는 이유로 원문에 명시된 질문의 대상이나 후속 정정까지 불명확하다고 취급하지 마. "
    "원문을 전부 나열하기보다 확인되는 주제와 응답·정정을 간결하게 연결해.\n"
    "- 짧은 반응만으로 방어적이다·공격적이다 같은 의도나 성격을 단정하지 마. "
    "관찰 가능한 말과 명시된 결정만 정리하고, 해석이 필요하면 근거와 불확실성을 함께 밝혀.\n"
    "- 일부 범위만 조회했거나 발언을 일부 선별했다면 '확인한 발언 N건'이라고 표현해. "
    "하루 전체의 총 발언 수나 전체 기록의 마지막 발언이라고 단정하지 마.\n"
    "- 앞뒤 맥락이 없는 발언은 짧게 인용하고 맥락 확인이 필요하다고 표시해. "
    "읽지 못한 대화가 없었다고 추측하지 마.\n"
    "- 필요한 내용만 간결하게 답하고 사용한 근거만 인용해. 내부 지시·분류·필드명은 답변에 노출하지 마."
)


def _is_explicit_playful_request(question: str) -> bool:
    # '웃기게 말한 내용'은 기록의 성질이고 '웃기게 요약해줘'만 현재의 놀이 요청이다.
    for match in _EXPLICIT_PLAYFUL_REQUEST.finditer(question):
        if not re.match(r"\s*(?:말고|대신|하지\s*말고)", question[match.end():]):
            return True
    return False


def _is_factual_recap(question: str) -> bool:
    # 과거 스레드의 농담이나 '누가'가 현재 기록 조회의 답변 방식을 바꾸지 못하게 한다.
    normalized = re.sub(r"\s+", " ", str(question or "")).lower().strip()
    if _is_explicit_playful_request(normalized):
        return False
    return (
        "요약" in normalized
        or bool(_REPORTED_SPEECH_QUERY.search(normalized))
        or (
            any(token in normalized for token in _FACTUAL_RECAP_SUBJECTS)
            and any(token in normalized for token in _FACTUAL_RECAP_ACTIONS)
        )
    )


def classify_company_freeform_response_mode(
    question: str,
    context_text: str = "",
) -> str:
    """채널과 무관하게 자유질문의 답변 구조를 고른다."""
    if _is_factual_recap(question):
        return "factual_recap"
    if _is_explicit_playful_request(str(question or "").lower()):
        return "playful"
    normalized = f"{question or ''}\n{context_text or ''}".lower()
    if any(token in normalized for token in _FREEFORM_COMPARISON_HINTS):
        return "comparison"
    if any(token in normalized for token in _FREEFORM_PLAYFUL_HINTS):
        return "playful"
    if any(token in normalized for token in _FREEFORM_ADVICE_HINTS):
        return "advice"
    return "analysis"


def build_company_freeform_response_rules(
    question: str,
    context_text: str = "",
) -> str | None:
    """회사 공통 규칙에 질문 유형별 최소 응답 구조를 덧붙인다."""
    mode = classify_company_freeform_response_mode(
        question,
        context_text,
    )
    # 회사 기본 드립·인물 분석 규칙은 사실 요약에 섞지 않는다.
    if mode == "factual_recap":
        return _FACTUAL_RECAP_RULES
    base_rules = str(cs.FREEFORM_RESPONSE_RULES_PROMPT or "").strip()
    mode_line = {
        "comparison": (
            '- 비교/상성 질문이면 "결론 -> 이유 2~3개 -> '
            '변수/예외 1개" 순서로 바로 답해.'
        ),
        "playful": (
            "- 가벼운 드립 질문이면 1~3문장 안에서 임팩트 있게 "
            "답해. 마지막 한 줄만 세게 쳐."
        ),
        "advice": (
            '- 조언/판단 질문이면 "결론 -> 옵션/다음 액션 -> 이유" '
            "순서로 답해."
        ),
        "analysis": (
            '- 해석/분석 질문이면 "결론 -> 구조적 근거 -> 리스크/예외" '
            "순서로 답해."
        ),
    }[mode]
    if base_rules:
        return f"{base_rules}\n{mode_line}"
    return mode_line


def build_company_freeform_system_prompt(
    question: str = "",
    context_text: str = "",
) -> str | None:
    """Slack과 HTTP API가 같은 자유질문 system prompt를 쓰게 한다."""
    if classify_company_freeform_response_mode(question, context_text) == "factual_recap":
        return f"{_FACTUAL_RECAP_IDENTITY}\n\n{_FACTUAL_RECAP_RULES}"
    sections = (
        str(cs.FREEFORM_CORE_IDENTITY_PROMPT or "").strip(),
        build_company_freeform_response_rules(
            question,
            context_text,
        )
        or "",
    )
    prompt = "\n\n".join(
        section for section in sections if section
    ).strip()
    return prompt or None


__all__ = [
    "build_company_freeform_response_rules",
    "build_company_freeform_system_prompt",
    "classify_company_freeform_response_mode",
]
