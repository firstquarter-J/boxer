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
class AssistantMessage:
    body: str
    delivery_scope: DeliveryScope = "conversation"
    mention_actor: bool = True
    format: Literal["commonmark"] = "commonmark"


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


__all__ = [
    "AssistantChannel",
    "AssistantMessage",
    "AssistantOutcome",
    "CompanyAssistantRequest",
    "CompanyAssistantResult",
    "DeliveryScope",
    "SourceReference",
    "SuggestedAction",
]
