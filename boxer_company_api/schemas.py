from __future__ import annotations

from datetime import datetime
import json
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
_MAX_RESPONSE_MESSAGES = 8
_MAX_RESPONSE_SOURCES = 20
_MAX_MESSAGE_CHARS = 30_000
_MAX_RESPONSE_BYTES = 1_048_576
_TRUNCATED_MARKER = "...(truncated)"
_SENSITIVE_SOURCE_PARAMETER_EXACT_NAMES = frozenset(
    {
        "auth",
        "key",
        "sig",
    }
)
_SENSITIVE_SOURCE_PARAMETER_MARKERS = (
    "accesskey",
    "apikey",
    "authorization",
    "credential",
    "secret",
    "signature",
    "token",
)


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
    followupKind: Literal[
        "recording_failure",
        "barcode_log",
    ] | None = None

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
            ("followup_kind", self.followupKind),
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
    routeGroup: Literal[
        "notion",
        "device",
        "failure",
        "log",
        "structured",
        "barcode",
        "knowledge",
    ] | None = None

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
        if self.routeGroup is not None:
            # strict enum 검증을 통과한 실행 범위만 request guard에 알려
            # 선택되지 않은 stage의 외부 조회 가드가 오탐하지 않게 한다.
            metadata["route_group"] = self.routeGroup
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
    body: str = Field(min_length=1, max_length=_MAX_MESSAGE_CHARS)
    deliveryScope: Literal["conversation", "requester"]
    mentionActor: bool
    format: Literal["commonmark"]


class SourceReferenceOutput(BaseModel):
    sourceId: str = Field(min_length=1, max_length=512)
    title: str = Field(min_length=1, max_length=2_000)
    uri: str = Field(min_length=1, max_length=2_048)
    score: float | None


class AssistantTurnOutput(BaseModel):
    requestId: str = Field(min_length=1, max_length=128)
    route: str = Field(min_length=1, max_length=256)
    outcome: Literal[
        "answered",
        "no_evidence",
        "needs_input",
        "denied",
        "failed",
    ]
    messages: list[AssistantMessageOutput] = Field(
        min_length=1,
        max_length=_MAX_RESPONSE_MESSAGES
    )
    sources: list[SourceReferenceOutput] = Field(
        max_length=_MAX_RESPONSE_SOURCES
    )
    usedLlm: bool
    fallbackReason: str | None = Field(default=None, max_length=256)
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
            messages=[
                AssistantMessageOutput(
                    body="처리할 수 있는 read-only 경로를 찾지 못했어",
                    deliveryScope="conversation",
                    mentionActor=True,
                    format="commonmark",
                )
            ],
            sources=[],
            usedLlm=False,
            fallbackReason="no_matching_route",
        )
        return payload.model_dump(mode="json")

    sources: list[SourceReferenceOutput] = []
    for source in result.sources[:_MAX_RESPONSE_SOURCES]:
        safe_uri = _safe_source_uri(str(source.uri))
        safe_source_id = _safe_source_text(
            source.source_id,
            maximum=512,
        )
        safe_title = _safe_source_text(
            source.title,
            maximum=2_000,
        )
        if (
            safe_uri is None
            or safe_source_id is None
            or safe_title is None
        ):
            continue
        sources.append(
            SourceReferenceOutput(
                sourceId=safe_source_id,
                title=safe_title,
                uri=safe_uri,
                score=_safe_score(source.score),
            )
        )
    payload = AssistantTurnOutput(
        requestId=request_id,
        route=result.route,
        outcome=result.outcome,
        # 긴 로그 결과도 client의 30,000자/8개 계약 안에서만 내보낸다.
        # 원문은 route 결과에 남기고 HTTP 표현에서만 안전하게 windowing한다.
        messages=_serialize_messages(result.messages),
        sources=sources,
        usedLlm=result.used_llm,
        fallbackReason=result.fallback_reason,
        # 변경 작업과 job payload는 별도 typed API가 생기기 전에는 노출하지 않는다.
        suggestedAction=None,
        asyncJob=None,
    )
    return _fit_response_byte_budget(payload).model_dump(mode="json")


def _serialize_messages(
    messages: tuple[Any, ...],
) -> list[AssistantMessageOutput]:
    chunks: list[AssistantMessageOutput] = []
    was_truncated = False
    for message_index, message in enumerate(messages):
        body = str(message.body or "")
        if not body.strip():
            continue
        for offset in range(0, len(body), _MAX_MESSAGE_CHARS):
            if len(chunks) >= _MAX_RESPONSE_MESSAGES:
                was_truncated = True
                break
            chunk = body[offset : offset + _MAX_MESSAGE_CHARS]
            chunks.append(
                AssistantMessageOutput(
                    body=chunk,
                    deliveryScope=message.delivery_scope,
                    mentionActor=(
                        message.mention_actor and offset == 0
                    ),
                    format=message.format,
                )
            )
        if len(chunks) >= _MAX_RESPONSE_MESSAGES:
            # 현재 본문의 잔여분이나 뒤 메시지가 있으면 마지막 조각에
            # 잘림을 명시해 조용한 데이터 손실을 피한다.
            was_truncated = was_truncated or (
                offset + _MAX_MESSAGE_CHARS < len(body)
            )
            if message_index < len(messages) - 1:
                was_truncated = True
            break

    if was_truncated and chunks:
        last = chunks[-1]
        marker_budget = _MAX_MESSAGE_CHARS - len(_TRUNCATED_MARKER)
        chunks[-1] = last.model_copy(
            update={
                "body": last.body[:marker_budget] + _TRUNCATED_MARKER
            }
        )
    return chunks


def _fit_response_byte_budget(
    payload: AssistantTurnOutput,
) -> AssistantTurnOutput:
    """UTF-8 JSON 본문이 client의 1MiB 상한을 넘지 않게 줄인다."""

    messages = list(payload.messages)
    fitted = payload
    while (
        _serialized_response_size(fitted) > _MAX_RESPONSE_BYTES
        and len(messages) > 1
    ):
        # 뒤쪽 transport chunk부터 제거하고 마지막 보존 chunk에 잘림을
        # 표시해 silent truncation을 피한다.
        messages.pop()
        messages[-1] = _with_truncated_marker(messages[-1])
        fitted = payload.model_copy(update={"messages": messages})

    if _serialized_response_size(fitted) <= _MAX_RESPONSE_BYTES:
        return fitted

    # source 최대 계약만으로도 1MiB보다 작으므로 마지막 한 메시지만
    # binary search로 줄이면 항상 예산 안에 들어온다.
    last = messages[-1]
    raw_body = _without_truncated_marker(last.body)
    low = 0
    high = min(len(raw_body), _MAX_MESSAGE_CHARS - len(_TRUNCATED_MARKER))
    best = _TRUNCATED_MARKER
    while low <= high:
        midpoint = (low + high) // 2
        candidate_body = raw_body[:midpoint] + _TRUNCATED_MARKER
        candidate_messages = [
            *messages[:-1],
            last.model_copy(update={"body": candidate_body}),
        ]
        candidate = payload.model_copy(
            update={"messages": candidate_messages}
        )
        if _serialized_response_size(candidate) <= _MAX_RESPONSE_BYTES:
            best = candidate_body
            low = midpoint + 1
        else:
            high = midpoint - 1
    messages[-1] = last.model_copy(update={"body": best})
    return payload.model_copy(update={"messages": messages})


def _with_truncated_marker(
    message: AssistantMessageOutput,
) -> AssistantMessageOutput:
    body = _without_truncated_marker(message.body)
    marker_budget = _MAX_MESSAGE_CHARS - len(_TRUNCATED_MARKER)
    return message.model_copy(
        update={
            "body": body[:marker_budget] + _TRUNCATED_MARKER
        }
    )


def _without_truncated_marker(body: str) -> str:
    if body.endswith(_TRUNCATED_MARKER):
        return body[: -len(_TRUNCATED_MARKER)]
    return body


def _serialized_response_size(payload: AssistantTurnOutput) -> int:
    return len(
        json.dumps(
            payload.model_dump(mode="json"),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def _safe_source_text(
    value: object,
    *,
    maximum: int,
) -> str | None:
    normalized = str(value or "").strip()
    if (
        not normalized
        or "\r" in normalized
        or "\n" in normalized
    ):
        return None
    return normalized[:maximum]


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
    if not (
        parsed.scheme.lower() == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
        and not _contains_sensitive_source_parameter(parsed.query)
        and not _contains_sensitive_source_parameter(parsed.fragment)
    ):
        return None
    return normalized


def _contains_sensitive_source_parameter(raw_parameters: str) -> bool:
    """서명 URL과 OAuth fragment가 근거 링크로 되돌아가지 않게 막는다."""

    candidates = [raw_parameters]
    if "?" in raw_parameters:
        # 일반 anchor 뒤에 query 형식의 OAuth fragment가 붙는 경우도 검사한다.
        candidates.append(raw_parameters.split("?", 1)[1])

    for candidate in candidates:
        for key, _value in parse_qsl(
            candidate,
            keep_blank_values=True,
        ):
            normalized_key = re.sub(
                r"[^a-z0-9]",
                "",
                key.strip().lower(),
            )
            if (
                normalized_key in _SENSITIVE_SOURCE_PARAMETER_EXACT_NAMES
                or any(
                    marker in normalized_key
                    for marker in _SENSITIVE_SOURCE_PARAMETER_MARKERS
                )
            ):
                return True
    return False


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
