from __future__ import annotations

from datetime import datetime
import math
import re
from typing import Any, Literal
from urllib.parse import parse_qsl, urlsplit

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from boxer.context.entries import (
    ContextEntry,
    ContextEntrySource,
)
from boxer_company.assistant.contracts import (
    CompanyAssistantRequest,
    CompanyAssistantResult,
)


_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
_LOCALE_PATTERN = r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$"
_MAX_CONTEXT_ENTRIES = 12
_MAX_CONTEXT_CHARS = 5_000
_MAX_QUESTION_CHARS = 4_000


class _StrictInputModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ContextEntryInput(_StrictInputModel):
    kind: Literal["message"] = "message"
    source: ContextEntrySource
    authorId: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )
    text: str = Field(min_length=1, max_length=4_000)
    createdAt: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
    )

    @field_validator("text")
    @classmethod
    def _validate_text(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("context text must not be blank")
        return normalized

    @field_validator("createdAt")
    @classmethod
    def _validate_created_at(
        cls,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        try:
            datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError as exc:
            # Slack ts도 ContextEntry의 정규화된 생성시각 표현으로 사용한다.
            if not re.fullmatch(r"\d{1,20}(?:\.\d{1,9})?", normalized):
                raise ValueError(
                    "createdAt must be an ISO-8601 timestamp or channel timestamp"
                ) from exc
        return normalized

    def to_context_entry(self) -> ContextEntry:
        entry: ContextEntry = {
            "kind": self.kind,
            "source": self.source,
            "text": self.text,
        }
        if self.authorId is not None:
            entry["author_id"] = self.authorId
        if self.createdAt is not None:
            entry["created_at"] = self.createdAt
        return entry


class AssistantTurnScopeInput(_StrictInputModel):
    barcode: str | None = Field(
        default=None,
        pattern=r"^\d{11}$",
    )
    hospitalName: str | None = Field(
        default=None,
        min_length=1,
        max_length=160,
    )
    roomName: str | None = Field(
        default=None,
        min_length=1,
        max_length=160,
    )
    deviceName: str | None = Field(
        default=None,
        min_length=1,
        max_length=160,
    )
    channelContextId: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )

    @field_validator(
        "hospitalName",
        "roomName",
        "deviceName",
    )
    @classmethod
    def _normalize_scope_text(
        cls,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None
        normalized = " ".join(value.split())
        if not normalized:
            raise ValueError("scope text must not be blank")
        return normalized

    @model_validator(mode="after")
    def _validate_hospital_room_pair(
        self,
    ) -> "AssistantTurnScopeInput":
        if (self.hospitalName is None) != (self.roomName is None):
            raise ValueError(
                "hospitalName and roomName must be provided together"
            )
        return self

    def to_metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {}
        for key, value in (
            ("barcode", self.barcode),
            ("hospital_name", self.hospitalName),
            ("room_name", self.roomName),
            ("device_name", self.deviceName),
            ("channel_id", self.channelContextId),
        ):
            if value is not None:
                metadata[key] = value
        return metadata


class AssistantTurnInput(_StrictInputModel):
    tenantId: str = Field(
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )
    actorId: str | None = Field(
        default=None,
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )
    channel: Literal["slack", "web", "api"]
    conversationId: str = Field(
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )
    question: str = Field(
        min_length=1,
        max_length=_MAX_QUESTION_CHARS,
    )
    locale: str = Field(
        min_length=2,
        max_length=35,
        pattern=_LOCALE_PATTERN,
    )
    contextEntries: list[ContextEntryInput] = Field(
        default_factory=list,
        max_length=_MAX_CONTEXT_ENTRIES,
    )
    scope: AssistantTurnScopeInput | None = None

    @field_validator("question")
    @classmethod
    def _normalize_question(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("question must not be blank")
        return normalized

    @model_validator(mode="after")
    def _validate_context_budget(self) -> "AssistantTurnInput":
        if (
            sum(len(entry.text) for entry in self.contextEntries)
            > _MAX_CONTEXT_CHARS
        ):
            raise ValueError("contextEntries exceed the text budget")
        return self

    def to_company_request(
        self,
        request_id: str,
    ) -> CompanyAssistantRequest:
        # HTTP에서 허용한 scope만 중립 metadata로 옮겨 임의 권한·role 주입을 막는다.
        metadata = (
            self.scope.to_metadata()
            if self.scope is not None
            else {}
        )
        return CompanyAssistantRequest(
            request_id=request_id,
            tenant_id=self.tenantId,
            actor_id=self.actorId,
            channel=self.channel,
            conversation_id=self.conversationId,
            question=self.question,
            locale=self.locale,
            context_entries=tuple(
                entry.to_context_entry()
                for entry in self.contextEntries
            ),
            metadata=metadata,
        )


class AssistantMessageOutput(BaseModel):
    body: str
    deliveryScope: Literal["conversation", "requester"]
    mentionActor: bool
    format: Literal["commonmark"]


class SourceReferenceOutput(BaseModel):
    sourceId: str
    title: str
    uri: str
    score: float | None


class AssistantTurnOutput(BaseModel):
    requestId: str
    route: str
    outcome: Literal[
        "answered",
        "no_evidence",
        "needs_input",
        "denied",
        "failed",
    ]
    messages: list[AssistantMessageOutput]
    sources: list[SourceReferenceOutput]
    usedLlm: bool
    fallbackReason: str | None
    suggestedAction: None = None
    asyncJob: None = None


def serialize_result(
    result: CompanyAssistantResult | None,
    request_id: str,
) -> dict[str, Any]:
    """도메인 결과에서 HTTP에 허용한 필드만 명시적으로 직렬화한다."""

    if result is None:
        payload = AssistantTurnOutput(
            requestId=request_id,
            route="unhandled",
            outcome="no_evidence",
            messages=[],
            sources=[],
            usedLlm=False,
            fallbackReason="no_matching_route",
        )
        return payload.model_dump(mode="json")

    sources: list[SourceReferenceOutput] = []
    for source in result.sources:
        safe_uri = _safe_source_uri(str(source.uri))
        if safe_uri is None:
            continue
        sources.append(
            SourceReferenceOutput(
                sourceId=str(source.source_id),
                title=str(source.title),
                uri=safe_uri,
                score=_safe_score(source.score),
            )
        )
    payload = AssistantTurnOutput(
        requestId=request_id,
        route=result.route,
        outcome=result.outcome,
        messages=[
            AssistantMessageOutput(
                body=message.body,
                deliveryScope=message.delivery_scope,
                mentionActor=message.mention_actor,
                format=message.format,
            )
            for message in result.messages
        ],
        sources=sources,
        usedLlm=result.used_llm,
        fallbackReason=result.fallback_reason,
        # 변경 작업과 job payload는 별도 typed API가 생기기 전에는 노출하지 않는다.
        suggestedAction=None,
        asyncJob=None,
    )
    return payload.model_dump(mode="json")


def _safe_source_uri(uri: str) -> str | None:
    normalized = (uri or "").strip()
    if (
        not normalized
        or len(normalized) > 2_048
        or "\r" in normalized
        or "\n" in normalized
    ):
        return None
    parsed = urlsplit(normalized)
    query_keys = {
        key.strip().lower()
        for key, _value in parse_qsl(
            parsed.query,
            keep_blank_values=True,
        )
    }
    if not (
        parsed.scheme.lower() == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
        and not any(
            marker in key
            for key in query_keys
            for marker in (
                "api_key",
                "apikey",
                "credential",
                "secret",
                "signature",
                "token",
            )
        )
    ):
        return None
    return normalized


def _safe_score(value: object) -> float | None:
    if value is None:
        return None
    try:
        normalized = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return normalized if math.isfinite(normalized) else None


__all__ = [
    "AssistantMessageOutput",
    "AssistantTurnInput",
    "AssistantTurnOutput",
    "AssistantTurnScopeInput",
    "ContextEntryInput",
    "SourceReferenceOutput",
    "serialize_result",
]
