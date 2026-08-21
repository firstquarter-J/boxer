from __future__ import annotations

import logging
from typing import Any, Callable

from boxer.context.builder import _build_model_input
from boxer.core import settings as core_settings
from boxer.core.llm import (
    _ask_claude,
    _ask_ollama_chat,
    _check_ollama_health,
)
from boxer_company.assistant.freeform_route import (
    CompanyFreeformAssistantRoute,
    FreeformAnswerer,
)
from boxer_company.assistant.team_fun_route import (
    TEAM_FUN_LLM_MAX_TOKENS,
    TEAM_FUN_LLM_TIMEOUT_SEC,
    TEAM_FUN_OLLAMA_MODEL,
    CompanyTeamFunAssistantRoute,
)


def build_company_provider_answerer(
    *,
    provider: str,
    claude_client: Any | None,
) -> FreeformAnswerer:
    """API가 소유한 일반 대화용 provider 호출기를 만든다."""

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
    provider_unavailable_summary: Callable[[], str | None] | None = None,
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
        provider=provider,
        provider_unavailable_summary=provider_unavailable_summary,
        logger=logger,
    )


def build_company_team_fun_route(
    *,
    provider: str,
    claude_client: Any | None,
    context_max_chars: int,
    logger: logging.Logger | None = None,
) -> CompanyTeamFunAssistantRoute:
    """기존 Slack fun의 model·timeout·template fallback 계약을 API에 조립한다."""

    normalized_provider = str(provider or "").strip().lower()

    def provider_ready() -> bool:
        if normalized_provider == "claude":
            return claude_client is not None
        if normalized_provider == "ollama":
            health = _check_ollama_health(
                timeout_sec=min(
                    core_settings.OLLAMA_HEALTH_TIMEOUT_SEC,
                    2,
                ),
                model=TEAM_FUN_OLLAMA_MODEL,
            )
            return bool(health.get("ok"))
        return False

    def answer(
        prompt: str,
        context_text: str,
        system_prompt: str | None,
    ) -> str:
        del context_text
        if normalized_provider == "claude":
            if claude_client is None:
                raise RuntimeError("claude provider unavailable")
            return _ask_claude(
                claude_client,
                prompt,
                system_prompt=system_prompt,
                max_tokens=TEAM_FUN_LLM_MAX_TOKENS,
            )
        if normalized_provider == "ollama":
            return _ask_ollama_chat(
                prompt,
                system_prompt=system_prompt,
                model=TEAM_FUN_OLLAMA_MODEL,
                timeout_sec=TEAM_FUN_LLM_TIMEOUT_SEC,
                max_tokens=TEAM_FUN_LLM_MAX_TOKENS,
                temperature=0.5,
                think=False,
            )
        raise RuntimeError("unsupported provider")

    return CompanyTeamFunAssistantRoute(
        answer,
        provider_ready=provider_ready,
        context_max_chars=max(1, context_max_chars),
        logger=logger,
    )

__all__ = [
    "build_company_freeform_route",
    "build_company_provider_answerer",
    "build_company_team_fun_route",
]
