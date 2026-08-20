from __future__ import annotations

import re


_NON_EXECUTION_HINTS = (
    "왜",
    "이유",
    "원인",
    "분석",
    "방법",
    "가능",
    "어떻게",
    "알려",
    "설명",
    "확인해줘",
    "확인해 주세요",
    "여부",
    "되나",
    "될까",
    "해도 돼",
    "해도돼",
    "해도 되",
    "도 돼",
    "도돼",
    "면 돼",
    "면돼",
    "괜찮",
    "할 수 있",
    "있나",
    "해야",
    "하지마",
    "하지 마",
    "하지말",
    "하지 말",
    "안 해",
    "안해",
    "말아",
    "취소",
    "중단",
)
_SIDE_EFFECT_PERMISSION_HINTS = (
    "가능",
    "방법",
    "어떻게",
    "해야",
    "될까",
    "되나",
    "해도",
    "도 돼",
    "도돼",
    "면 돼",
    "면돼",
    "괜찮",
    "할 수 있",
    "있나",
    "확인해줘",
    "확인해 주세요",
)
_NON_EXECUTION_ENGLISH_PATTERN = re.compile(
    r"\b(?:why|how|can|could|should|explain|do\s+not|don't)\b",
    re.IGNORECASE,
)


def is_explicit_operation_execution(question: str) -> bool:
    """설명·질문·부정형을 confirmation 없는 실행 의도에서 제외한다."""

    normalized = " ".join(str(question or "").split()).strip()
    lowered = normalized.lower()
    if not normalized or "?" in normalized or "？" in normalized:
        return False
    if any(hint in lowered for hint in _NON_EXECUTION_HINTS):
        return False
    return _NON_EXECUTION_ENGLISH_PATTERN.search(lowered) is None


def is_side_effect_permission_question(question: str) -> bool:
    """read/probe처럼 보여도 tunnel·파일 동작을 묻는 질문형을 식별한다."""

    normalized = " ".join(str(question or "").split()).strip().lower()
    if not normalized:
        return False
    return bool(
        "?" in normalized
        or "？" in normalized
        or any(hint in normalized for hint in _SIDE_EFFECT_PERMISSION_HINTS)
        or _NON_EXECUTION_ENGLISH_PATTERN.search(normalized)
    )


__all__ = [
    "is_explicit_operation_execution",
    "is_side_effect_permission_question",
]
