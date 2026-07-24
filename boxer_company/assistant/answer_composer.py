from __future__ import annotations

from dataclasses import dataclass
import logging
from typing import Any, Callable

from boxer.answering import AnswerEngine, AnswerRequest, EvidenceTransform
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)


AnswerValidator = Callable[[str], bool]
ProviderReady = Callable[[], bool]
ActorAllowedForLlm = Callable[[str | None], bool]


@dataclass(frozen=True, slots=True)
class CompanyEvidenceAnswerComposerDeps:
    answer_engine: AnswerEngine
    synthesis_enabled: bool
    provider_ready: ProviderReady
    actor_allowed_for_llm: ActorAllowedForLlm


@dataclass(frozen=True, slots=True)
class CompanyEvidenceAnswerPolicy:
    route: str
    fallback_message: str
    fallback_outcome: AssistantOutcome = "answered"
    fallback_on_timeout: bool = False
    timeout_message: str = "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
    include_context: bool = True
    system_prompt: str | None = None
    extra_rules: str = ""
    evidence_transform: EvidenceTransform | None = None
    max_tokens: int | None = None
    timeout_sec: int | None = None
    answer_validator: AnswerValidator | None = None


class CompanyEvidenceAnswerComposer:
    """조회 근거를 LLM 답변 또는 안전한 route fallback으로 조립한다."""

    def __init__(
        self,
        deps: CompanyEvidenceAnswerComposerDeps,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps
        self._logger = logger or logging.getLogger(__name__)

    def compose(
        self,
        request: CompanyAssistantRequest,
        *,
        evidence: Any,
        policy: CompanyEvidenceAnswerPolicy,
        sources: tuple[SourceReference, ...] = (),
    ) -> CompanyAssistantResult:
        provider = self._deps.answer_engine.provider
        unavailable_reason = self._preflight_failure_reason(
            provider=provider,
            actor_id=request.actor_id,
        )
        if unavailable_reason is not None:
            return self._fallback_result(
                policy,
                sources=sources,
                fallback_reason=unavailable_reason,
            )

        try:
            # adapter가 정규화한 ContextEntry만 core로 전달해
            # 채널 원본 객체가 service 경계 안으로 들어오지 않게 한다.
            answer = self._deps.answer_engine.answer(
                AnswerRequest(
                    question=request.question,
                    evidence=evidence,
                    context_entries=(
                        request.context_entries if policy.include_context else ()
                    ),
                    system_prompt=policy.system_prompt,
                    extra_rules=policy.extra_rules,
                    evidence_transform=policy.evidence_transform,
                    max_tokens=policy.max_tokens,
                    timeout_sec=policy.timeout_sec,
                )
            )
        except TimeoutError:
            # AnswerEngine이 실패를 구조화하지만 대체 구현도
            # service 경계를 깨지 않게 방어한다.
            self._logger.warning(
                "Company evidence answer timed out route=%s provider=%s",
                policy.route,
                provider,
            )
            return self._timeout_result(policy, sources=sources)
        except Exception as exc:
            # provider 구현의 예상 밖 실패는 fallback과 별개로 traceback을 보존한다.
            self._logger.exception(
                "Company evidence answer failed route=%s provider=%s error_type=%s",
                policy.route,
                provider,
                type(exc).__name__,
            )
            return self._fallback_result(
                policy,
                sources=sources,
                fallback_reason="provider_error",
            )

        # 기존 회사 prompt가 단일 별표 강조를 반환해도 공통 결과 계약은
        # Slack 문법이 아닌 CommonMark로 고정한다.
        normalized_text = slack_mrkdwn_to_commonmark(
            answer.text or ""
        ).strip()
        failure_reason = answer.failure_reason
        if failure_reason == "timeout":
            return self._timeout_result(policy, sources=sources)
        if not answer.used_llm:
            return self._fallback_result(
                policy,
                sources=sources,
                fallback_reason=failure_reason or "empty_response",
            )
        if not normalized_text:
            return self._fallback_result(
                policy,
                sources=sources,
                fallback_reason="empty_response",
            )
        if not self._is_answer_valid(normalized_text, policy):
            return self._fallback_result(
                policy,
                sources=sources,
                fallback_reason="answer_validation_failed",
            )

        return CompanyAssistantResult(
            route=policy.route,
            outcome="answered",
            messages=(AssistantMessage(body=normalized_text),),
            sources=sources,
            used_llm=True,
        )

    def _preflight_failure_reason(
        self,
        *,
        provider: str,
        actor_id: str | None,
    ) -> str | None:
        if not self._deps.synthesis_enabled:
            return "synthesis_disabled"
        if provider not in {"claude", "ollama"}:
            return "provider_unavailable"

        # 현재 actor allowlist는 외부 Claude 호출에만 적용하고
        # 로컬 Ollama에는 적용하지 않는다.
        if provider == "claude":
            try:
                if not self._deps.actor_allowed_for_llm(actor_id):
                    return "actor_not_allowed_for_llm"
            except Exception as exc:
                self._logger.warning(
                    "Company evidence actor policy failed provider=%s error_type=%s",
                    provider,
                    type(exc).__name__,
                )
                return "actor_not_allowed_for_llm"

        try:
            if not self._deps.provider_ready():
                return "provider_unavailable"
        except Exception as exc:
            self._logger.warning(
                "Company evidence provider health check failed provider=%s error_type=%s",
                provider,
                type(exc).__name__,
            )
            return "provider_unavailable"
        return None

    def _is_answer_valid(
        self,
        answer_text: str,
        policy: CompanyEvidenceAnswerPolicy,
    ) -> bool:
        if policy.answer_validator is not None:
            try:
                if not policy.answer_validator(answer_text):
                    return False
            except Exception as exc:
                # validator 구현 오류도 생성 답변을 그대로 노출하지 않고
                # route fallback으로 닫는다.
                self._logger.warning(
                    "Company evidence answer validation failed route=%s error_type=%s",
                    policy.route,
                    type(exc).__name__,
                )
                return False

        answer_lowered = answer_text.lower()
        fallback_lowered = policy.fallback_message.lower()
        # 근거 fallback에 없던 다른 바코드를 생성 답변이 새로 권하면
        # 조회 대상이 바뀌는 위험한 추론이므로 모든 회사 route에서 막는다.
        return not any(
            token in answer_lowered and token not in fallback_lowered
            for token in ("다른 바코드", "다른 barcode")
        )

    @staticmethod
    def _fallback_result(
        policy: CompanyEvidenceAnswerPolicy,
        *,
        sources: tuple[SourceReference, ...],
        fallback_reason: str,
    ) -> CompanyAssistantResult:
        return CompanyAssistantResult(
            route=policy.route,
            outcome=policy.fallback_outcome,
            messages=(AssistantMessage(body=policy.fallback_message),),
            sources=sources,
            used_llm=False,
            fallback_reason=fallback_reason,
        )

    @classmethod
    def _timeout_result(
        cls,
        policy: CompanyEvidenceAnswerPolicy,
        *,
        sources: tuple[SourceReference, ...],
    ) -> CompanyAssistantResult:
        if policy.fallback_on_timeout:
            return cls._fallback_result(
                policy,
                sources=sources,
                fallback_reason="timeout",
            )
        timeout_message = (
            policy.timeout_message.strip() or policy.fallback_message
        )
        return CompanyAssistantResult(
            route=policy.route,
            outcome=policy.fallback_outcome,
            messages=(AssistantMessage(body=timeout_message),),
            sources=sources,
            used_llm=False,
            fallback_reason="timeout",
        )


__all__ = [
    "ActorAllowedForLlm",
    "AnswerValidator",
    "CompanyEvidenceAnswerComposer",
    "CompanyEvidenceAnswerComposerDeps",
    "CompanyEvidenceAnswerPolicy",
    "ProviderReady",
]
