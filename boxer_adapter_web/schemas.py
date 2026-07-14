from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

SupportedLanguage = Literal["en", "ko"]


class WidgetIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str
    email: str | None = None
    name: str | None = None


class WidgetContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    language: SupportedLanguage | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class SessionInitPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sessionId: str | None = None
    identity: WidgetIdentity | None = None
    context: WidgetContext | None = None


class MessageSendPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sessionId: str
    text: str


class WorkflowStartPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sessionId: str
    workflowKey: str


class HandoffRequestPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sessionId: str
    reason: str | None = None


class SessionEndPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sessionId: str


class AdminLoginInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: str
    password: str


class AdminReplyInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str


class SourceReferenceDto(BaseModel):
    documentId: str
    title: str
    score: float
    sourceUri: str


class MessageDto(BaseModel):
    id: str
    senderType: Literal["user", "assistant", "system", "admin"]
    senderName: str | None = None
    body: str
    sourceRefs: list[SourceReferenceDto] = Field(default_factory=list)
    createdAt: str


class ConversationSummaryDto(BaseModel):
    id: str
    sessionId: str
    customerId: str | None = None
    customerName: str | None = None
    customerEmail: str | None = None
    context: dict[str, Any] = Field(default_factory=dict)
    status: str = "starter"
    workflowKey: str | None = None
    workflowState: dict[str, Any] = Field(default_factory=dict)
    assignedAdminUserId: str | None = None
    assignedAdminUserName: str | None = None
    handoffRequestedAt: str | None = None
    handoffStartedAt: str | None = None
    closedAt: str | None = None
    lastMessagePreview: str | None = None
    createdAt: str
    updatedAt: str


class ConversationSnapshotDto(ConversationSummaryDto):
    messages: list[MessageDto] = Field(default_factory=list)


class AdminUserDto(BaseModel):
    id: str
    email: str
    name: str


class AdminAuthDto(BaseModel):
    adminUser: AdminUserDto
    csrfToken: str


class KnowledgeDocumentSummaryDto(BaseModel):
    id: str
    title: str
    sourceType: str
    sourceUri: str
    excerpt: str
    syncedAt: str


class KnowledgeDocumentDetailDto(KnowledgeDocumentSummaryDto):
    content: str
    metadata: dict[str, Any] = Field(default_factory=dict)


class KnowledgeSyncRunDto(BaseModel):
    id: int
    sourceType: str
    status: str
    documentCount: int
    errorMessage: str | None = None
    startedAt: str
    finishedAt: str | None = None


class KnowledgeStatusDto(BaseModel):
    activeSource: str
    documentCount: int
    lastSync: KnowledgeSyncRunDto | None = None


class PaginationDto(BaseModel):
    limit: int
    offset: int
    total: int


class ConversationListDto(BaseModel):
    conversations: list[ConversationSnapshotDto]
    pagination: PaginationDto


class WebSocketEvent(BaseModel):
    type: str
    payload: dict[str, Any]


class ErrorPayloadDto(BaseModel):
    code: str
    message: str


class WidgetSessionReadyEventDto(BaseModel):
    type: Literal["session.ready"] = "session.ready"
    payload: ConversationSnapshotDto


class WidgetMessageCreatedEventDto(BaseModel):
    type: Literal["message.created"] = "message.created"
    payload: MessageDto


class WidgetConversationUpdatedEventDto(BaseModel):
    type: Literal["conversation.updated"] = "conversation.updated"
    payload: ConversationSnapshotDto


class WidgetSessionEndedEventDto(BaseModel):
    type: Literal["session.ended"] = "session.ended"
    payload: ConversationSnapshotDto


class SocketErrorEventDto(BaseModel):
    type: Literal["error"] = "error"
    payload: ErrorPayloadDto


class AdminReadyPayloadDto(BaseModel):
    adminUser: AdminUserDto


class AdminReadyEventDto(BaseModel):
    type: Literal["admin.ready"] = "admin.ready"
    payload: AdminReadyPayloadDto


class AdminMessageCreatedPayloadDto(BaseModel):
    conversationId: str
    message: MessageDto


class AdminMessageCreatedEventDto(BaseModel):
    type: Literal["message.created"] = "message.created"
    payload: AdminMessageCreatedPayloadDto


class AdminConversationUpdatedEventDto(BaseModel):
    type: Literal["conversation.updated"] = "conversation.updated"
    payload: ConversationSnapshotDto


def isoformat_utc(value: str | datetime) -> str:
    if isinstance(value, str):
        return value
    return value.replace(microsecond=0).isoformat()
