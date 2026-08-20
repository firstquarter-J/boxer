from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

from boxer.context.entries import ContextEntry


AssistantChannel = Literal["slack", "web", "api", "test"]
AssistantOutcome = Literal[
    "answered",
    "no_evidence",
    "needs_input",
    "denied",
    "failed",
]
DeliveryScope = Literal["conversation", "requester"]


@dataclass(frozen=True, slots=True)
class CompanyAssistantRequest:
    request_id: str
    tenant_id: str
    actor_id: str | None
    channel: AssistantChannel
    conversation_id: str
    question: str
    locale: str
    context_entries: tuple[ContextEntry, ...] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SourceReference:
    source_id: str
    title: str
    uri: str
    score: float | None = None


@dataclass(frozen=True, slots=True)
class AssistantLink:
    """요청자 전용 메시지에만 붙일 수 있는 신뢰된 실행 결과 링크다."""

    label: str
    uri: str


@dataclass(frozen=True, slots=True)
class AssistantMessage:
    body: str
    delivery_scope: DeliveryScope = "conversation"
    mention_actor: bool = True
    format: Literal["commonmark"] = "commonmark"
    private_links: tuple[AssistantLink, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class SuggestedAction:
    action: str
    label: str
    requires_confirmation: bool = False
    parameters: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CompanyAssistantResult:
    route: str
    outcome: AssistantOutcome
    messages: tuple[AssistantMessage, ...]
    sources: tuple[SourceReference, ...] = field(default_factory=tuple)
    used_llm: bool = False
    fallback_reason: str | None = None
    suggested_action: SuggestedAction | None = None
    async_job: Mapping[str, Any] | None = None
    # 실행 receipt는 대화 본문에 섞지 않고 adapter의 후속 추적 저장에만 쓴다.
    # transport는 route별 고정 schema로 다시 검증해야 한다.
    operation_result: Mapping[str, Any] | None = None


__all__ = [
    "AssistantChannel",
    "AssistantLink",
    "AssistantMessage",
    "AssistantOutcome",
    "CompanyAssistantRequest",
    "CompanyAssistantResult",
    "DeliveryScope",
    "SourceReference",
    "SuggestedAction",
]
