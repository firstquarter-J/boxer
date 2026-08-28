from importlib import import_module
from typing import Any


# Adapter가 package metadata만 읽을 때 LLM/provider 구현을 당기지 않도록
# 공개 facade는 실제 심볼 접근 시점에만 해당 모듈을 연다.
_LAZY_EXPORTS = {
    "AnswerEngine": ("boxer.answering", "AnswerEngine"),
    "AnswerRequest": ("boxer.answering", "AnswerRequest"),
    "AnswerResult": ("boxer.answering", "AnswerResult"),
    "ContextEntry": ("boxer.context.entries", "ContextEntry"),
    "create_answer_engine_from_settings": (
        "boxer.answering",
        "create_answer_engine_from_settings",
    ),
    "synthesize_retrieval_answer": (
        "boxer.answering",
        "synthesize_retrieval_answer",
    ),
}


def __getattr__(name: str) -> Any:
    target = _LAZY_EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attribute_name = target
    value = getattr(import_module(module_name), attribute_name)
    globals()[name] = value
    return value

__all__ = [
    "AnswerEngine",
    "AnswerRequest",
    "AnswerResult",
    "ContextEntry",
    "create_answer_engine_from_settings",
    "synthesize_retrieval_answer",
]
