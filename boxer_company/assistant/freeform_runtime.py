from __future__ import annotations

import logging
from typing import Any, Callable

from boxer.context.builder import _build_model_input
from boxer.core import settings as core_settings
from boxer.core.llm import (
    _ask_claude_with_meta,
    _ask_ollama_chat,
    _check_ollama_health,
)
from boxer_company.assistant.freeform_route import (
    CompanyFreeformAssistantRoute,
    FreeformAnswerer,
    AnswerTokenLimitError,
)
from boxer_company.assistant.team_fun_route import (
    TEAM_FUN_LLM_MAX_TOKENS,
    TEAM_FUN_LLM_TIMEOUT_SEC,
    TEAM_FUN_OLLAMA_MODEL,
    CompanyTeamFunAssistantRoute,
)


def _complete_claude_answer(client: Any, question: str, system_prompt: str | None) -> str:
    # 사고 토큰도 출력 한도를 쓰므로 회사 대화는 최소 2,048토큰을 확보한다.
    # 실제 잘림 때만 한 번 더 생성하고, 미완성 답변을 성공으로 반환하지 않는다.
    budget = max(2048, core_settings.ANTHROPIC_MAX_TOKENS)
    for attempt in range(2):
        result = _ask_claude_with_meta(client, question, system_prompt=system_prompt, max_tokens=budget)
        if result.get("stop_reason") in {"end_turn", "refusal"}:
            return result["text"]
        if result.get("stop_reason") != "max_tokens":
            # 도구 요청·일시 중지·종료 사유 누락은 완성된 최종 답변이 아니다.
            raise RuntimeError("incomplete_model_turn")
        if attempt or budget >= 8192:
            break
        budget = min(8192, budget * 2)
    raise AnswerTokenLimitError("answer_token_limit")


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
            return _complete_claude_answer(
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
            return _complete_claude_answer(
                claude_client,
                prompt,
                system_prompt=system_prompt,
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
