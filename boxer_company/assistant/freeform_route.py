from __future__ import annotations

import re
from collections.abc import Callable
import logging

import anthropic

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
ProviderUnavailableSummary = Callable[[], str | None]

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
    (
        re.compile(r"\bfictional framing\b", re.IGNORECASE),
        "밈 프레임",
    ),
)


def match_company_freeform_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """adapter가 명시한 마지막 자유대화 stage만 처리한다."""

    route_group = str(request.metadata.get("route_group") or "").strip()
    if route_group != "freeform":
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
        provider: str = "",
        provider_unavailable_summary: ProviderUnavailableSummary | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._answerer = answerer
        self._provider_ready = provider_ready
        self._context_max_chars = max(1, context_max_chars)
        self._provider = str(provider or "").strip().lower()
        self._provider_unavailable_summary = (
            provider_unavailable_summary or (lambda: None)
        )
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
        if not str(request.question or "").strip():
            return CompanyAssistantResult(
                route=self.name,
                outcome="needs_input",
                messages=(
                    AssistantMessage(
                        body=(
                            "질문 내용을 같이 보내줘. 지원 기능이 궁금하면 "
                            "`사용법`이라고 보내줘"
                        )
                    ),
                ),
                fallback_reason="missing_question",
            )

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
        provider_check_failed = False
        try:
            provider_ready = self._provider_ready()
        except Exception as exc:
            self._logger.warning(
                "Company freeform provider check failed request_id=%s "
                "error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            provider_ready = False
            provider_check_failed = True
        if not provider_ready:
            unavailable_summary: str | None = None
            if not provider_check_failed:
                try:
                    unavailable_summary = (
                        self._provider_unavailable_summary()
                    )
                except Exception as exc:
                    # health 상세 조회 실패는 provider 장애 안내 자체를 막지 않는다.
                    self._logger.warning(
                        "Company freeform provider summary failed "
                        "request_id=%s error_type=%s",
                        request.request_id,
                        type(exc).__name__,
                    )
            return CompanyAssistantResult(
                route=self.name,
                outcome="failed",
                messages=(
                    AssistantMessage(
                        body=_provider_unavailable_message(
                            self._provider,
                            unavailable_summary,
                        )
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
        except anthropic.AuthenticationError:
            return _provider_failure_result(
                self.name,
                _claude_api_key_invalid_message(),
                "provider_authentication_error",
            )
        except anthropic.RateLimitError as exc:
            body = (
                _claude_credit_unavailable_message()
                if _is_claude_credit_unavailable_error(exc)
                else (
                    "API 호출 제한으로 지금은 AI 답변을 생성할 수 없어. "
                    "잠시 후 다시 시도해줘"
                )
            )
            return _provider_failure_result(
                self.name,
                body,
                "provider_rate_limit",
            )
        except anthropic.PermissionDeniedError as exc:
            body = (
                _claude_credit_unavailable_message()
                if _is_claude_credit_unavailable_error(exc)
                else _claude_permission_denied_message()
            )
            return _provider_failure_result(
                self.name,
                body,
                "provider_permission_denied",
            )
        except anthropic.APIStatusError as exc:
            body = (
                _claude_credit_unavailable_message()
                if _is_claude_credit_unavailable_error(exc)
                else "AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘"
            )
            return _provider_failure_result(
                self.name,
                body,
                "provider_error",
            )
        except RuntimeError as exc:
            if self._provider == "ollama" and _is_timeout_error(exc):
                return _provider_failure_result(
                    self.name,
                    self._timeout_message,
                    "timeout",
                )
            return _provider_failure_result(
                self.name,
                (
                    "Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘"
                    if self._provider == "ollama"
                    else "AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘"
                ),
                "provider_error",
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
                        body=(
                            "Ollama 응답 중 오류가 발생했어. "
                            "서버 연결 상태를 확인해줘"
                            if self._provider == "ollama"
                            else (
                                "AI 응답 중 오류가 발생했어. "
                                "잠시 후 다시 시도해줘"
                            )
                        )
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
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    # 기존 Slack sanitizer처럼 정리 규칙이 본문 전체를 지우면 원문을 보존한다.
    return cleaned or normalized


def _provider_unavailable_message(
    provider: str,
    summary: str | None = None,
) -> str:
    if provider == "claude":
        return (
            "인증값이 설정되지 않아 지금은 AI 답변을 생성할 수 없어. "
            "서버의 `ANTHROPIC_API_KEY` 또는 `ANTHROPIC_AUTH_TOKEN`을 확인해줘"
        )
    if provider == "ollama":
        base = "LLM 서버가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
        detail = str(summary or "").strip()
        # 기존 Slack freeform은 health summary가 있을 때 같은 줄을 덧붙였다.
        return f"{base}\n• 상태: {detail}" if detail else base
    return "지금은 AI 답변 기능을 사용할 수 없어. 잠시 후 다시 시도해줘"


def _claude_api_key_invalid_message() -> str:
    return (
        "인증값이 유효하지 않아 지금은 AI 답변을 생성할 수 없어. "
        "서버의 `ANTHROPIC_API_KEY` 또는 `ANTHROPIC_AUTH_TOKEN`을 확인해줘"
    )


def _claude_permission_denied_message() -> str:
    return (
        "인증값 권한이 없어 지금은 AI 답변을 생성할 수 없어. "
        "서버의 `ANTHROPIC_API_KEY` 또는 `ANTHROPIC_AUTH_TOKEN`을 확인해줘"
    )


def _claude_credit_unavailable_message() -> str:
    return "토큰이 충전되지 않아 답변할 수 없어. 추가 결제가 필요해."


def _flatten_error_text(value: object) -> str:
    if isinstance(value, dict):
        return " ".join(
            part
            for key, item in value.items()
            for part in (str(key), _flatten_error_text(item))
            if part
        )
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_error_text(item) for item in value)
    return str(value or "")


def _is_claude_credit_unavailable_error(exc: Exception) -> bool:
    body_text = _flatten_error_text(getattr(exc, "body", None))
    combined = (
        f"{body_text} {getattr(exc, 'message', '')} {exc}"
    ).lower()
    return any(
        token in combined
        for token in (
            "credit balance",
            "credits",
            "insufficient_quota",
            "insufficient quota",
            "quota_exceeded",
            "billing",
            "payment",
            "prepaid",
        )
    )


def _is_timeout_error(exc: Exception) -> bool:
    lowered = str(exc).lower()
    return "timeout" in lowered or "timed out" in lowered


def _provider_failure_result(
    route: str,
    body: str,
    fallback_reason: str,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome="failed",
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "CompanyFreeformAssistantRoute",
    "FreeformAnswerer",
    "ProviderUnavailableSummary",
    "match_company_freeform_route",
]
