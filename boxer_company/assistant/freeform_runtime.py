from __future__ import annotations

import logging
from typing import Any, Callable

from boxer.context.builder import _build_model_input
from boxer.core import settings as core_settings
from boxer.core.llm import _ask_claude, _ask_ollama_chat
from boxer_company.assistant.freeform_route import (
    CompanyFreeformAssistantRoute,
    FreeformAnswerer,
)


def build_company_provider_answerer(
    *,
    provider: str,
    claude_client: Any | None,
) -> FreeformAnswerer:
    """API가 소유한 한 provider 호출기를 일반 대화와 fun route가 공유한다."""

    normalized_provider = str(provider or "").strip().lower()

    def answer(
        question: str,
        context_text: str,
        system_prompt: str | None,
    ) -> str:
        # adapter가 넘긴 bounded context만 model input으로 만들고, provider
        # 선택이나 credential은 서버 설정에서만 가져온다.
        model_input = _build_model_input(question, context_text)
        if normalized_provider == "claude":
            if claude_client is None:
                raise RuntimeError("claude provider unavailable")
            return _ask_claude(
                claude_client,
                model_input,
                system_prompt=system_prompt,
            )
        if normalized_provider == "ollama":
            return _ask_ollama_chat(
                model_input,
                system_prompt=system_prompt,
                think=False,
            )
        raise RuntimeError("unsupported provider")

    return answer


def build_company_freeform_route(
    *,
    provider: str,
    claude_client: Any | None,
    provider_ready: Callable[[], bool],
    timeout_message: str,
    logger: logging.Logger | None = None,
) -> CompanyFreeformAssistantRoute:
    """API 프로세스의 provider를 채널 중립 자유대화 route에 연결한다."""
    answer = build_company_provider_answerer(
        provider=provider,
        claude_client=claude_client,
    )

    return CompanyFreeformAssistantRoute(
        answer,
        provider_ready=provider_ready,
        context_max_chars=max(1, core_settings.THREAD_CONTEXT_MAX_CHARS),
        timeout_message=timeout_message,
        logger=logger,
    )


__all__ = [
    "build_company_freeform_route",
    "build_company_provider_answerer",
]
