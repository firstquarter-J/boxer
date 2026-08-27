from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from boxer_company.hpa_change_coordinator import (
    HpaChangeCoordinator,
    HpaChangeDelivery,
    HpaChangeDeliveryAck,
    HpaChangeSubmissionAttachment,
    HpaChangeSubmissionRequest,
)
from boxer_company.hpa_change_workflow import HpaChangePollState


HPA_CHANGE_SUBMIT_PATH = "/internal/v1/hpa-change/requests"
HPA_CHANGE_LOOKUP_PATH = "/internal/v1/hpa-change/threads/lookup"
HPA_CHANGE_DELIVERY_PULL_PATH = "/internal/v1/hpa-change/deliveries/pull"
HPA_CHANGE_DELIVERY_ACK_PATH = "/internal/v1/hpa-change/deliveries/ack"
_SLACK_TS_RE = re.compile(r"^[0-9]{1,20}(?:\.[0-9]{1,9})?$")
_SLACK_CHANNEL_RE = re.compile(r"^[CDG][A-Z0-9]{5,30}$")
_SLACK_USER_RE = re.compile(r"^[UW][A-Z0-9]{5,30}$")
_TASK_ID_RE = re.compile(r"^hpa-[a-zA-Z0-9-]{8,100}$")
_DELIVERY_ID_RE = re.compile(r"^hpa-delivery:[0-9a-f]{64}$")


class _StrictInput(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HpaChangeAttachmentInput(_StrictInput):
    name: str = Field(min_length=1, max_length=255)
    content: str = Field(max_length=524_288)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("name")
    @classmethod
    def _normalize_name(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized or "/" in normalized or "\\" in normalized:
            raise ValueError("attachment name must be a basename")
        return normalized


class HpaChangeSubmitInput(_StrictInput):
    workspaceId: str = Field(min_length=1, max_length=64)
    requestKey: str = Field(min_length=1, max_length=512)
    channelId: str = Field(pattern=r"^[CDG][A-Z0-9]{5,30}$")
    threadTs: str = Field(pattern=r"^[0-9]{1,20}(?:\.[0-9]{1,9})?$")
    threadUrl: str = Field(min_length=1, max_length=2_048)
    eventTs: str = Field(pattern=r"^[0-9]{1,20}(?:\.[0-9]{1,9})?$")
    requesterUserId: str = Field(pattern=r"^[UW][A-Z0-9]{5,30}$")
    initiatorUserId: str = Field(pattern=r"^[UW][A-Z0-9]{5,30}$")
    threadText: str = Field(min_length=1, max_length=60_000)
    attachments: tuple[HpaChangeAttachmentInput, ...] = Field(
        default=(),
        max_length=10,
    )
    sourceChannelId: str = Field(pattern=r"^[CDG][A-Z0-9]{5,30}$")
    sourceMessageTs: str = Field(pattern=r"^[0-9]{1,20}(?:\.[0-9]{1,9})?$")
    selectionMode: Literal["thread", "linked_message"] = "thread"
    responseThreadUrl: str = Field(default="", max_length=2_048)
    continuationOfRequestId: str = Field(default="", max_length=128)

    @model_validator(mode="after")
    def _validate_actor_and_continuation(self) -> "HpaChangeSubmitInput":
        if self.requesterUserId != self.initiatorUserId:
            raise ValueError("requester and initiator must be identical")
        if self.continuationOfRequestId and not _TASK_ID_RE.fullmatch(
            self.continuationOfRequestId
        ):
            raise ValueError("continuation task id is invalid")
        return self

    def to_domain(self) -> HpaChangeSubmissionRequest:
        return HpaChangeSubmissionRequest(
            request_key=self.requestKey,
            workspace_id=self.workspaceId,
            channel_id=self.channelId,
            thread_ts=self.threadTs,
            thread_url=self.threadUrl,
            event_ts=self.eventTs,
            requester_user_id=self.requesterUserId,
            initiator_user_id=self.initiatorUserId,
            thread_text=self.threadText,
            attachments=tuple(
                HpaChangeSubmissionAttachment(
                    name=item.name,
                    content=item.content,
                    sha256=item.sha256,
                )
                for item in self.attachments
            ),
            source_channel_id=self.sourceChannelId,
            source_message_ts=self.sourceMessageTs,
            selection_mode=self.selectionMode,
            response_thread_url=self.responseThreadUrl,
            continuation_of_request_id=self.continuationOfRequestId,
        )


class HpaChangeThreadLookupInput(_StrictInput):
    workspaceId: str = Field(min_length=1, max_length=64)
    channelId: str = Field(pattern=r"^[CDG][A-Z0-9]{5,30}$")
    threadTs: str = Field(pattern=r"^[0-9]{1,20}(?:\.[0-9]{1,9})?$")
    eventTs: str = Field(pattern=r"^[0-9]{1,20}(?:\.[0-9]{1,9})?$")


class HpaChangeDeliveryPullInput(_StrictInput):
    workspaceId: str = Field(min_length=1, max_length=64)
    limit: int = Field(default=20, ge=1, le=100)


class HpaChangeDeliveryAckInput(_StrictInput):
    workspaceId: str = Field(min_length=1, max_length=64)
    taskId: str = Field(pattern=r"^hpa-[a-zA-Z0-9-]{8,100}$")
    deliveryId: str = Field(pattern=r"^hpa-delivery:[0-9a-f]{64}$")
    state: Literal[
        "running",
        "review_ready",
        "needs_clarification",
        "pr_opened",
        "no_change_needed",
        "failed",
    ]


@dataclass(slots=True)
class HpaChangeTransportService:
    """HTTP app이 인증 뒤 호출하는 HPA coordinator transport facade다."""

    coordinator: HpaChangeCoordinator

    def submit(self, request_id: str, body: HpaChangeSubmitInput) -> dict[str, Any]:
        result = self.coordinator.submit(body.to_domain())
        return {
            "requestId": request_id,
            "status": result.state.value,
            "hpaRequestId": result.request_id,
            "message": result.user_message,
            "autoRetryAllowed": False,
        }

    def lookup(
        self,
        request_id: str,
        body: HpaChangeThreadLookupInput,
    ) -> dict[str, Any]:
        result = self.coordinator.lookup_thread_job(
            body.workspaceId,
            body.channelId,
            body.threadTs,
            body.eventTs,
        )
        return {
            "requestId": request_id,
            "state": result.state.value,
            "hpaRequestId": result.request_id,
            "jobStatus": result.job_status,
            "eventTs": result.event_ts,
            "currentEvent": result.current_event,
        }

    def pull(
        self,
        request_id: str,
        body: HpaChangeDeliveryPullInput,
    ) -> dict[str, Any]:
        deliveries = self.coordinator.pull_pending(
            workspace_id=body.workspaceId,
            limit=body.limit,
        )
        return {
            "requestId": request_id,
            "deliveries": [serialize_hpa_change_delivery(item) for item in deliveries],
            "autoRetryAllowed": False,
        }

    def acknowledge(
        self,
        request_id: str,
        body: HpaChangeDeliveryAckInput,
    ) -> dict[str, Any]:
        result = self.coordinator.acknowledge_delivery(
            HpaChangeDeliveryAck(
                delivery_id=body.deliveryId,
                task_id=body.taskId,
                workspace_id=body.workspaceId,
                state=HpaChangePollState(body.state),
            )
        )
        return {
            "requestId": request_id,
            "deliveryId": body.deliveryId,
            "acknowledged": result.acknowledged,
            "hpaRequestId": result.task_id,
            "jobStatus": result.job_status,
            "implementationDispatchStarted": (
                result.implementation_dispatch_started
            ),
            "autoRetryAllowed": False,
        }


def serialize_hpa_change_delivery(delivery: HpaChangeDelivery) -> dict[str, Any]:
    """Slack renderer에 필요한 값만 보내고 GitHub URL·오류·credential은 제외한다."""

    return {
        "deliveryId": delivery.delivery_id,
        "hpaRequestId": delivery.task_id,
        "workspaceId": delivery.workspace_id,
        "channelId": delivery.channel_id,
        "threadTs": delivery.thread_ts,
        "state": delivery.state.value,
        "workflowPhase": delivery.workflow_phase,
        "result": dict(delivery.result),
        "prUrls": list(delivery.pr_urls),
        "requestSource": {
            "text": delivery.request_text,
            "attachmentNames": list(delivery.attachment_names),
        },
    }


__all__ = [
    "HPA_CHANGE_DELIVERY_ACK_PATH",
    "HPA_CHANGE_DELIVERY_PULL_PATH",
    "HPA_CHANGE_LOOKUP_PATH",
    "HPA_CHANGE_SUBMIT_PATH",
    "HpaChangeAttachmentInput",
    "HpaChangeDeliveryAckInput",
    "HpaChangeDeliveryPullInput",
    "HpaChangeSubmitInput",
    "HpaChangeThreadLookupInput",
    "HpaChangeTransportService",
    "serialize_hpa_change_delivery",
]
