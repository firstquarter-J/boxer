from __future__ import annotations

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


def classify_company_freeform_response_mode(
    question: str,
    context_text: str = "",
) -> str:
    """채널과 무관하게 자유질문의 답변 구조를 고른다."""
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
    base_rules = str(cs.FREEFORM_RESPONSE_RULES_PROMPT or "").strip()
    mode = classify_company_freeform_response_mode(
        question,
        context_text,
    )
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
