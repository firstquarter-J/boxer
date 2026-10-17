"""Slack 원본과 수집 lease만 받는 회사 기억 transport 계약."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


Identifier = Annotated[str, Field(min_length=1, max_length=256, pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@/-]*$")]
SlackTs = Annotated[str, Field(pattern=r"^\d{10,16}\.\d{6}$")]
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class MemoryInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class MemoryVerificationInput(MemoryInput):
    sourceId: Identifier
    revisionHash: Digest
    status: Literal["verified", "changed", "deleted", "unavailable"]


class MemoryContextInput(MemoryInput):
    workspaceId: Identifier
    channelId: Identifier
    threadTs: SlackTs
    currentMessageTs: SlackTs
    actorId: Identifier
    channelKind: Literal["channel", "private_channel", "dm", "group_dm"] = "channel"
    triggerKind: Literal["app_mention", "message"] = "app_mention"
    currentMessagePermalink: str | None = Field(default=None, max_length=2048)
    currentMessageText: str | None = Field(default=None, max_length=16000, repr=False)
    currentMessageEditedAt: str | None = Field(default=None, max_length=64)
    retrievalId: Identifier | None = None
    verifications: list[MemoryVerificationInput] = Field(default_factory=list, max_length=64)

    @model_validator(mode="after")
    def validate_order(self) -> "MemoryContextInput":
        # Slack ts는 float로 반올림하지 않고 정수부·소수부로 비교한다.
        if tuple(map(int, self.threadTs.split("."))) > tuple(map(int, self.currentMessageTs.split("."))):
            raise ValueError("invalid cutoff")
        return self


class MemorySourceInput(MemoryInput):
    messageTs: SlackTs
    revisionHash: Digest
    authorId: Identifier
    authorKind: Literal["human", "boxer"]
    text: str = Field(min_length=1, max_length=16000, repr=False)
    permalink: str = Field(min_length=1, max_length=2048)
    createdAt: str = Field(min_length=1, max_length=64)
    editedAt: str | None = Field(default=None, max_length=64)
    route: str | None = Field(default=None, max_length=64)
    deliveryVerified: bool = False
    sourceId: Identifier | None = None
    # 장문은 전체 메시지로 가장하지 않고 구간과 원문 revision을 보존한다.
    segmentStart: int | None = Field(default=None, ge=0)
    segmentEnd: int | None = Field(default=None, ge=1)


class MemoryLiveSourceInput(MemoryInput):
    channelId: Identifier
    channelKind: Literal["private_channel"]
    threadTs: SlackTs
    messageTs: SlackTs
    revisionHash: Digest
    authorId: Identifier
    text: str = Field(min_length=1, max_length=8000, repr=False)
    permalink: str = Field(min_length=1, max_length=2048)
    editedAt: str = Field(default="", max_length=64)


class MemoryLiveResultInput(MemoryInput):
    tenantId: Identifier
    actorId: Identifier
    channelId: Identifier
    requestId: Identifier
    sources: list[MemoryLiveSourceInput] = Field(default_factory=list, max_length=8)
    coverage: Literal["complete", "partial", "unavailable"]
    channelCount: int = Field(default=0, ge=0, le=200)

    @model_validator(mode="after")
    def validate_budget(self) -> "MemoryLiveResultInput":
        # 원문은 한 turn의 짧은 검증 캐시에만 전달하고 과도한 payload는 받지 않는다.
        if sum(len(source.text) for source in self.sources) > 8000:
            raise ValueError("live source budget exceeded")
        if self.coverage == "unavailable" and self.sources:
            raise ValueError("unavailable result cannot contain sources")
        return self


class MemoryChunkInput(MemoryInput):
    tenantId: Identifier
    actorId: Identifier
    claimToken: Identifier
    chunkNo: int = Field(ge=0, le=9999)
    sources: list[MemorySourceInput] = Field(max_length=100)
    coverage: Literal["complete", "partial", "unavailable"]
    isLast: bool = False
    totalChunks: int | None = Field(default=None, ge=1, le=10000)
    inputDigest: Digest | None = None
    chunkDigest: Digest | None = None

    @model_validator(mode="after")
    def validate_budget(self) -> "MemoryChunkInput":
        if sum(len(source.text) for source in self.sources) > 16000:
            raise ValueError("source budget exceeded")
        if self.isLast and (self.totalChunks is None or self.inputDigest is None):
            raise ValueError("final chunk requires manifest")
        return self


class MemoryPullInput(MemoryInput):
    tenantId: Identifier
    workerId: Identifier
    limit: int = Field(default=10, ge=1, le=25)


class MemoryDeliveryReceiptInput(MemoryInput):
    deliveryId: Identifier
    messageTs: SlackTs
    partIndex: int = Field(default=0, ge=0, le=63)
    clientMsgId: str | None = Field(default=None, max_length=128)
    authorId: Identifier | None = None
    textHash: Digest | None = None


class MemoryCheckpointInput(MemoryInput):
    tenantId: Identifier
    actorId: Identifier
    claimToken: Identifier
    cursor: str | None = Field(default=None, max_length=4096)
    coverage: Literal["complete", "partial", "unavailable"] | None = None
    state: Literal["canceled", "unavailable"] | None = None
    deliveryReceipts: list[MemoryDeliveryReceiptInput] = Field(default_factory=list, max_length=64)


class MemorySourceEventInput(MemoryInput):
    tenantId: Identifier
    channelId: Identifier
    messageTs: SlackTs
    eventType: Literal["changed", "deleted", "unavailable"]
    eventId: Identifier
    revisionHash: Digest | None = None


class MemoryAnswerPartOutput(MemoryInput):
    partIndex: int = Field(ge=0, le=63)
    deliveryId: Identifier
    textHash: Digest


class MemoryReceivedChunkOutput(MemoryInput):
    chunkNo: int = Field(ge=0)
    chunkDigest: Digest


class MemoryDeliveryReceiptOutput(MemoryDeliveryReceiptInput):
    # 발송 영수증의 작성자 미확인은 유지하되 자동으로 봇 출처를 확정하지 않는다.
    authorId: Identifier | Literal[""] | None = None
    originTextHash: Digest | None = None


class MemoryCheckpointOutput(MemoryInput):
    cursor: str | None = Field(default=None, max_length=4096)
    coverage: Literal["complete", "partial", "unavailable"] = "partial"
    deliveryReceipts: list[MemoryDeliveryReceiptOutput] = Field(default_factory=list, max_length=64)


class MemoryCaptureOutput(MemoryInput):
    captureId: Identifier
    tenantId: Identifier
    actorId: Identifier
    channelId: Identifier
    threadTs: SlackTs
    currentMessageTs: SlackTs
    channelKind: Literal["channel", "private_channel", "dm", "group_dm"]
    requestId: Identifier
    route: str = Field(min_length=1, max_length=64)
    state: str = Field(min_length=1, max_length=32)
    deletionEpoch: int = Field(ge=0)
    claimToken: Identifier | None = None
    leaseUntil: str | None = Field(default=None, max_length=64)
    expiresAt: str | None = Field(default=None, max_length=64)
    nextChunkNo: int = Field(default=0, ge=0, le=10000)
    checkpoint: MemoryCheckpointOutput = Field(default_factory=MemoryCheckpointOutput)
    receivedChunks: list[MemoryReceivedChunkOutput] = Field(default_factory=list, max_length=10000)
    # 답변 본문 없이 stable part identity만 adapter로 돌려준다.
    answerParts: list[MemoryAnswerPartOutput] = Field(default_factory=list, max_length=64)
