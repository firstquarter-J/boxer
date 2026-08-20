from __future__ import annotations

import re
from collections.abc import Callable
import logging

from boxer.context.windowing import _render_context_text
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.freeform_prompt import (
    build_company_freeform_system_prompt,
)
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.team_chat_context import build_team_freeform_context


FreeformAnswerer = Callable[[str, str, str | None], str]

_META_LINE_PATTERNS = (
    re.compile(r"(?mi)^\s*현재 요청 적용\s*:\s*.+$"),
    re.compile(
        r"(?mi)^\s*(?:팀원별 컨텍스트|현재 화자 스타일|언급된 대상 반응 가이드)\s*:\s*$"
    ),
)
_META_PREFIX_REWRITES: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"^\s*(?:캐릭터|대화|채팅)\s*로그\s*기준(?:으로)?\s*"
            r"(?:해석하면|보면)\s*[,:\-]?\s*",
            re.IGNORECASE,
        ),
        "",
    ),
    (
        re.compile(
            r"^\s*(?:채팅\s*밈|오늘\s*로그|캐릭터상(?:으로)?)\s*기준(?:으로)?\s*"
            r"(?:해석하면|보면)\s*[,:\-]?\s*",
            re.IGNORECASE,
        ),
        "",
    ),
)


def match_company_freeform_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """adapter가 명시한 마지막 자유대화 stage만 처리한다."""

    route_group = str(request.metadata.get("route_group") or "").strip()
    if route_group != "freeform":
        return None
    if not str(request.question or "").strip():
        return None
    # direct caller가 final fallback stage를 골라 PII·장비 변경·복원 같은
    # operation capability를 우회하지 못하게 같은 순수 matcher를 재검사한다.
    from boxer_company.assistant.operations import (
        match_company_operation_route,
    )

    if match_company_operation_route(request) is not None:
        return None
    return "company_freeform"


class CompanyFreeformAssistantRoute:
    """Slack 문맥을 채널 중립 entry로 받아 회사 자유대화를 생성한다."""

    name = "company_freeform"

    def __init__(
        self,
        answerer: FreeformAnswerer,
        *,
        provider_ready: Callable[[], bool],
        context_max_chars: int,
        timeout_message: str,
        logger: logging.Logger | None = None,
    ) -> None:
        self._answerer = answerer
        self._provider_ready = provider_ready
        self._context_max_chars = max(1, context_max_chars)
        self._timeout_message = str(timeout_message or "").strip() or (
            "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
        )
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_company_freeform_route(request) is None:
            return None

        context_text = _render_context_text(
            list(request.context_entries),
            max_chars=self._context_max_chars,
        )
        if is_prompt_exfiltration_attempt(request.question, context_text):
            return CompanyAssistantResult(
                route=self.name,
                outcome="denied",
                messages=(
                    AssistantMessage(body=build_prompt_security_refusal()),
                ),
                fallback_reason="prompt_security",
            )
        if not self._provider_ready():
            return CompanyAssistantResult(
                route=self.name,
                outcome="failed",
                messages=(
                    AssistantMessage(
                        body="지금은 AI 답변 기능을 사용할 수 없어. 잠시 후 다시 시도해줘"
                    ),
                ),
                fallback_reason="provider_unavailable",
            )

        base_prompt = build_company_freeform_system_prompt(
            request.question,
            context_text,
        )
        team_context = build_team_freeform_context(
            request.question,
            context_text,
            speaker_user_id=str(request.actor_id or ""),
        )
        system_prompt = "\n\n".join(
            section for section in (base_prompt, team_context) if section
        ).strip() or None

        try:
            answer = _sanitize_freeform_answer(
                self._answerer(
                    request.question,
                    context_text,
                    system_prompt,
                )
            )
        except TimeoutError:
            return CompanyAssistantResult(
                route=self.name,
                outcome="failed",
                messages=(AssistantMessage(body=self._timeout_message),),
                fallback_reason="timeout",
            )
        except Exception as exc:
            # provider 오류 원문에는 credential이나 prompt가 섞일 수 있어 타입만 남긴다.
            self._logger.warning(
                "Company freeform answer failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return CompanyAssistantResult(
                route=self.name,
                outcome="failed",
                messages=(
                    AssistantMessage(
                        body="AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘"
                    ),
                ),
                fallback_reason="provider_error",
            )

        if not answer:
            return CompanyAssistantResult(
                route=self.name,
                outcome="failed",
                messages=(
                    AssistantMessage(body="답변을 생성하지 못했어. 다시 질문해줘"),
                ),
                fallback_reason="empty_response",
            )
        return CompanyAssistantResult(
            route=self.name,
            outcome="answered",
            messages=(AssistantMessage(body=answer),),
            used_llm=True,
        )


def _sanitize_freeform_answer(text: str) -> str:
    """내부 persona 메타 문구가 사용자 응답에 노출되지 않게 정리한다."""

    normalized = str(text or "").strip()
    if not normalized:
        return ""
    cleaned = normalized
    for pattern in _META_LINE_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    for pattern, replacement in _META_PREFIX_REWRITES:
        cleaned = pattern.sub(replacement, cleaned)
    return re.sub(r"\n{3,}", "\n\n", cleaned).strip()


__all__ = [
    "CompanyFreeformAssistantRoute",
    "FreeformAnswerer",
    "match_company_freeform_route",
]
