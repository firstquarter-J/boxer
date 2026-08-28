from __future__ import annotations

from dataclasses import dataclass, field
import logging
from typing import Any, Callable

from boxer.context.entries import ContextEntry
from boxer.context.windowing import _render_context_text
from boxer.core import settings as s
from boxer.core.llm import _build_claude_client
from boxer.retrieval.synthesis import _synthesize_retrieval_answer


EvidenceTransform = Callable[[Any], Any]
SynthesizeFn = Callable[..., str]


@dataclass(frozen=True, slots=True)
class AnswerRequest:
    question: str
    evidence: Any
    context_entries: tuple[ContextEntry, ...] = field(default_factory=tuple)
    system_prompt: str | None = None
    extra_rules: str = ""
    evidence_transform: EvidenceTransform | None = None
    max_tokens: int | None = None
    timeout_sec: int | None = None


@dataclass(frozen=True, slots=True)
class AnswerResult:
    text: str
    provider: str
    used_llm: bool
    failure_reason: str | None = None


def synthesize_retrieval_answer(
    question: str,
    thread_context: str,
    evidence_payload: Any,
    *,
    provider: str,
    provider_client: Any | None = None,
    system_prompt: str | None = None,
    extra_rules: str = "",
    evidence_transform: EvidenceTransform | None = None,
    max_tokens: int | None = None,
    timeout_sec: int | None = None,
) -> str:
    """provider별 client와 timeout 이름을 숨긴 공개 retrieval synthesis facade다."""
    normalized_provider = (provider or "").strip().lower()
    return _synthesize_retrieval_answer(
        question=question,
        thread_context=thread_context,
        evidence_payload=evidence_payload,
        provider=normalized_provider,
        claude_client=(
            provider_client if normalized_provider == "claude" else None
        ),
        system_prompt=system_prompt,
        extra_rules=extra_rules,
        evidence_transform=evidence_transform,
        max_tokens=max_tokens,
        ollama_timeout_sec=(
            timeout_sec if normalized_provider == "ollama" else None
        ),
    )


class AnswerEngine:
    def __init__(
        self,
        *,
        provider: str,
        provider_client: Any | None = None,
        context_max_chars: int | None = None,
        synthesize: SynthesizeFn = synthesize_retrieval_answer,
        logger: logging.Logger | None = None,
    ) -> None:
        self._provider = (provider or "").strip().lower()
        self._provider_client = provider_client
        self._context_max_chars = max(
            1,
            context_max_chars
            if context_max_chars is not None
            else s.THREAD_CONTEXT_MAX_CHARS,
        )
        self._synthesize = synthesize
        self._logger = logger or logging.getLogger(__name__)

    @property
    def provider(self) -> str:
        return self._provider

    def answer(self, request: AnswerRequest) -> AnswerResult:
        if not self._provider:
            return AnswerResult(
                text="",
                provider="",
                used_llm=False,
                failure_reason="provider_unconfigured",
            )
        if self._provider not in {"claude", "ollama"}:
            return AnswerResult(
                text="",
                provider=self._provider,
                used_llm=False,
                failure_reason="unsupported_provider",
            )

        # adapter가 전달한 정규화 entry만 렌더링해 채널 원본 payload가 prompt로 새지 않게 한다.
        context_text = _render_context_text(
            list(request.context_entries),
            max_chars=self._context_max_chars,
        )
        try:
            text = self._synthesize(
                question=request.question,
                thread_context=context_text,
                evidence_payload=request.evidence,
                provider=self._provider,
                provider_client=self._provider_client,
                system_prompt=request.system_prompt,
                extra_rules=request.extra_rules,
                evidence_transform=request.evidence_transform,
                max_tokens=request.max_tokens,
                timeout_sec=request.timeout_sec,
            ).strip()
        except TimeoutError:
            self._logger.warning("Evidence answer provider timed out provider=%s", self._provider)
            return AnswerResult(
                text="",
                provider=self._provider,
                used_llm=False,
                failure_reason="timeout",
            )
        except RuntimeError as exc:
            lowered = str(exc).lower()
            reason = "timeout" if "timeout" in lowered or "timed out" in lowered else "provider_error"
            self._logger.warning(
                "Evidence answer provider failed provider=%s reason=%s",
                self._provider,
                reason,
            )
            return AnswerResult(
                text="",
                provider=self._provider,
                used_llm=False,
                failure_reason=reason,
            )
        except Exception as exc:
            lowered = f"{type(exc).__name__} {exc}".lower()
            reason = (
                "timeout"
                if "timeout" in lowered or "timed out" in lowered
                else "provider_error"
            )
            self._logger.warning(
                "Evidence answer provider failed provider=%s error_type=%s reason=%s",
                self._provider,
                type(exc).__name__,
                reason,
            )
            return AnswerResult(
                text="",
                provider=self._provider,
                used_llm=False,
                failure_reason=reason,
            )

        if not text:
            return AnswerResult(
                text="",
                provider=self._provider,
                used_llm=False,
                failure_reason="empty_response",
            )
        return AnswerResult(
            text=text,
            provider=self._provider,
            used_llm=True,
        )


def create_answer_engine_from_settings() -> AnswerEngine:
    """공개 adapter가 같은 feature flag와 provider credential 조립을 사용하게 한다."""
    # 합성 flag가 꺼진 프로세스는 provider client도 만들지 않아 모든 채널이 같은 경계에서 닫힌다.
    provider = s.LLM_PROVIDER if s.LLM_SYNTHESIS_ENABLED else ""
    provider_client: Any | None = None
    has_claude_credentials = any(
        value and "REPLACE_ME" not in value
        for value in (
            s.ANTHROPIC_API_KEY,
            s.ANTHROPIC_AUTH_TOKEN,
            s.ANTHROPIC_AUTH_TOKEN_COMMAND,
        )
    )
    if provider == "claude" and has_claude_credentials:
        provider_client = _build_claude_client()
    return AnswerEngine(
        provider=provider,
        provider_client=provider_client,
    )


__all__ = [
    "AnswerEngine",
    "AnswerRequest",
    "AnswerResult",
    "EvidenceTransform",
    "create_answer_engine_from_settings",
    "synthesize_retrieval_answer",
]
