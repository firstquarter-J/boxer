from __future__ import annotations

from datetime import datetime, timezone
import json
import math
import re
from typing import Any, Literal
from urllib.parse import parse_qsl, urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)

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
_MAX_OPERATION_CONTEXT_ENTRIES = 100
_MAX_OPERATION_CONTEXT_CHARS = 12_000
_MAX_CONTEXT_ENTRY_CHARS = _MAX_CONTEXT_CHARS
_MAX_QUESTION_CHARS = 40_000
_MAX_RESPONSE_MESSAGES = 8
_MAX_RESPONSE_SOURCES = 20
_MAX_PRIVATE_LINK_URI_CHARS = 16_384
_MAX_MESSAGE_CHARS = 30_000
_MAX_RESPONSE_BYTES = 1_048_576
_TRUNCATED_MARKER = "...(truncated)"
_SLACK_CHANNEL_ID_PATTERN = r"^[CDG][A-Z0-9]{1,20}$"
_SLACK_MESSAGE_TS_PATTERN = r"^\d{1,20}(?:\.\d{1,9})?$"
_SAFE_ERROR_TYPE_PATTERN = r"^[A-Za-z][A-Za-z0-9_.]{0,159}$"
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
    # operations의 thread learning은 기존 단일 5k+ 메시지를
    # 잘라서는 안 된다. routeGroup별 실제 상한은 turn validator가
    # 아래에서 전체 budget과 함께 검증한다.
    text: str = Field(
        min_length=1,
        max_length=_MAX_OPERATION_CONTEXT_CHARS,
    )
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


class TrustedMdaRecoveryScopeInput(_StrictInputModel):
    """Slack adapter가 현재 bot의 복구 알림 root에서 검증한 exact scope다."""

    barcode: str = Field(pattern=r"^\d{11}$")
    logDate: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    deviceName: str = Field(
        min_length=3,
        max_length=64,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]{2,}$",
    )
    hospitalName: str = Field(min_length=1, max_length=200)
    roomName: str = Field(min_length=1, max_length=200)

    @field_validator("logDate")
    @classmethod
    def _validate_log_date(cls, value: str) -> str:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("logDate must be a valid date") from exc
        return value

    @field_validator("hospitalName", "roomName")
    @classmethod
    def _normalize_display_scope(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized or not normalized.isprintable():
            raise ValueError("display scope must be printable")
        return normalized

    def to_metadata(self) -> dict[str, str]:
        return {
            "barcode": self.barcode,
            "logDate": self.logDate,
            "deviceName": self.deviceName,
            "hospitalName": self.hospitalName,
            "roomName": self.roomName,
        }


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
    actorName: str | None = Field(
        default=None,
        min_length=1,
        max_length=160,
    )
    threadPermalink: str | None = Field(
        default=None,
        min_length=1,
        max_length=2_048,
    )
    trustedMdaRecoveryScope: TrustedMdaRecoveryScopeInput | None = None

    @field_validator(
        "hospitalName",
        "roomName",
        "deviceName",
        "actorName",
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

    @field_validator("threadPermalink")
    @classmethod
    def _validate_thread_permalink(
        cls,
        value: str | None,
    ) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        parsed = urlsplit(normalized)
        hostname = str(parsed.hostname or "").casefold()
        if (
            parsed.scheme != "https"
            or not hostname.endswith(".slack.com")
            or parsed.username is not None
            or parsed.password is not None
            or not parsed.path.startswith("/archives/")
            or parsed.fragment
        ):
            raise ValueError("threadPermalink must be a Slack HTTPS permalink")
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
            ("actor_name", self.actorName),
            ("thread_permalink", self.threadPermalink),
        ):
            if value is not None:
                metadata[key] = value
        if self.trustedMdaRecoveryScope is not None:
            metadata["trusted_mda_recovery_scope"] = (
                self.trustedMdaRecoveryScope.to_metadata()
            )
        return metadata


def _validate_slack_permalink(
    value: str,
    *,
    channel_id: str,
    message_ts: str,
    thread_ts: str,
) -> str:
    """Slack permalink가 exact channel/message/thread 범위를 가리키는지 본다."""

    normalized = value.strip()
    try:
        parsed = urlsplit(normalized)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("Slack permalink is invalid") from exc
    hostname = str(parsed.hostname or "").casefold()
    matched_path = re.fullmatch(
        rf"/archives/({re.escape(channel_id)})/p(\d+)/?",
        parsed.path,
    )
    if (
        parsed.scheme != "https"
        or not hostname.endswith(".slack.com")
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or matched_path is None
        or matched_path.group(2) != message_ts.replace(".", "")
        or parsed.fragment
    ):
        raise ValueError("Slack permalink is invalid")

    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_keys = [key for key, _item in query_pairs]
    if (
        len(query_keys) != len(set(query_keys))
        or any(key not in {"thread_ts", "cid"} for key in query_keys)
    ):
        raise ValueError("Slack permalink query is invalid")
    query = dict(query_pairs)
    if query.get("cid", channel_id) != channel_id:
        raise ValueError("Slack permalink channel is invalid")
    if query.get("thread_ts", thread_ts) != thread_ts:
        raise ValueError("Slack permalink thread is invalid")
    return normalized


class AssistantTurnAuditContextInput(_StrictInputModel):
    """Slack만 확정할 수 있는 request-log identity와 permalink다."""

    eventType: Literal["app_mention"]
    userName: str | None = Field(default=None, min_length=1, max_length=160)
    channelId: str = Field(pattern=_SLACK_CHANNEL_ID_PATTERN)
    messageId: str = Field(pattern=_SLACK_MESSAGE_TS_PATTERN)
    threadId: str = Field(pattern=_SLACK_MESSAGE_TS_PATTERN)
    isThreadRoot: bool = Field(strict=True)
    permalink: str | None = Field(default=None, min_length=1, max_length=2_048)
    threadPermalink: str | None = Field(
        default=None,
        min_length=1,
        max_length=2_048,
    )

    @field_validator("userName")
    @classmethod
    def _validate_user_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        if not normalized or not normalized.isprintable():
            raise ValueError("audit userName is invalid")
        return normalized

    @model_validator(mode="after")
    def _validate_slack_scope(self) -> "AssistantTurnAuditContextInput":
        if self.isThreadRoot != (self.messageId == self.threadId):
            raise ValueError("audit root scope is inconsistent")
        if self.permalink is not None:
            self.permalink = _validate_slack_permalink(
                self.permalink,
                channel_id=self.channelId,
                message_ts=self.messageId,
                thread_ts=self.threadId,
            )
        if self.threadPermalink is not None:
            self.threadPermalink = _validate_slack_permalink(
                self.threadPermalink,
                channel_id=self.channelId,
                message_ts=self.threadId,
                thread_ts=self.threadId,
            )
        return self


class RequestLogDeliveryActionInput(_StrictInputModel):
    """Slack 최종 전달 결과로 중앙 request-log row를 마감한다."""

    name: Literal["request_log_delivery"]
    phase: Literal["receipt"]
    delivered: bool = Field(strict=True)
    replyCount: int = Field(strict=True, ge=0, le=10_000)
    firstRepliedAtUtc: str | None = Field(
        default=None,
        min_length=1,
        max_length=64,
    )
    errorType: str | None = Field(
        default=None,
        pattern=_SAFE_ERROR_TYPE_PATTERN,
    )

    @model_validator(mode="after")
    def _validate_delivery(self) -> "RequestLogDeliveryActionInput":
        parsed_first_reply: datetime | None = None
        if self.firstRepliedAtUtc is not None:
            try:
                parsed_first_reply = datetime.fromisoformat(
                    self.firstRepliedAtUtc.replace("Z", "+00:00")
                )
            except ValueError as exc:
                raise ValueError(
                    "firstRepliedAtUtc must be ISO-8601"
                ) from exc
            if (
                parsed_first_reply.tzinfo is None
                or parsed_first_reply.utcoffset()
                != timezone.utc.utcoffset(parsed_first_reply)
            ):
                raise ValueError("firstRepliedAtUtc must be UTC")
        if (self.replyCount > 0) != (parsed_first_reply is not None):
            raise ValueError("reply count and first reply must match")
        if self.delivered == (self.errorType is not None):
            raise ValueError("delivery status and errorType must match")
        return self

    def to_metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "phase": self.phase,
            "delivered": self.delivered,
            "reply_count": self.replyCount,
            "first_replied_at_utc": self.firstRepliedAtUtc,
            "error_type": self.errorType,
        }


class DeviceFileDownloadDeliveryRecordInput(_StrictInputModel):
    deviceName: str = Field(min_length=1, max_length=160)
    deviceSeq: int | None = Field(default=None, ge=1)
    hospitalSeq: int | None = Field(default=None, ge=1)
    hospitalRoomSeq: int | None = Field(default=None, ge=1)
    hospitalName: str = Field(min_length=1, max_length=200)
    roomName: str = Field(min_length=1, max_length=200)
    fileNames: list[str]
    downloadFileNames: list[str] = Field(min_length=1)

    @field_validator("deviceName", "hospitalName", "roomName")
    @classmethod
    def _normalize_display_text(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if not normalized or not normalized.isprintable():
            raise ValueError("download delivery display text is invalid")
        return normalized

    @field_validator("fileNames", "downloadFileNames")
    @classmethod
    def _validate_file_names(cls, value: list[str]) -> list[str]:
        normalized = [str(item).strip() for item in value]
        if any(
            not item
            or len(item) > 255
            or not item.isprintable()
            or "/" in item
            or "\\" in item
            for item in normalized
        ):
            raise ValueError("download delivery file name is invalid")
        return normalized

    def to_metadata(self) -> dict[str, Any]:
        return {
            "device_name": self.deviceName,
            "device_seq": self.deviceSeq,
            "hospital_seq": self.hospitalSeq,
            "hospital_room_seq": self.hospitalRoomSeq,
            "hospital_name": self.hospitalName,
            "room_name": self.roomName,
            "file_names": list(self.fileNames),
            "download_file_names": list(self.downloadFileNames),
        }


class DeviceFileDownloadDeliveryManifestInput(_StrictInputModel):
    barcode: str = Field(pattern=r"^\d{11}$")
    logDate: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    usedExpandedScope: bool
    records: list[DeviceFileDownloadDeliveryRecordInput] = Field(
        min_length=1
    )

    @field_validator("logDate")
    @classmethod
    def _validate_log_date(cls, value: str) -> str:
        try:
            datetime.strptime(value, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("logDate must be a valid date") from exc
        return value

    def to_metadata(self) -> dict[str, Any]:
        return {
            "barcode": self.barcode,
            "log_date": self.logDate,
            "used_expanded_scope": self.usedExpandedScope,
            "records": [record.to_metadata() for record in self.records],
        }


class DeviceFileDownloadDeliveryActionInput(_StrictInputModel):
    """Slack이 모든 다운로드 DM을 보낸 뒤 보내는 성공 receipt다."""

    name: Literal["device_file_download_delivery"]
    phase: Literal["delivered"]
    delivery: DeviceFileDownloadDeliveryManifestInput

    def to_metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "phase": self.phase,
            "delivery": self.delivery.to_metadata(),
        }


class DeviceOperationDeliveryManifestInput(_StrictInputModel):
    """Slack 최종 응답 성공 뒤 activity에 필요한 최소 장비 작업 manifest다."""

    route: Literal[
        "device_box_update",
        "device_agent_update",
        "device_power_off",
    ]
    deviceName: str = Field(
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$",
    )
    requestedVersion: str = Field(max_length=80)
    currentBoxVersion: str = Field(max_length=80)
    dispatchMessage: str = Field(max_length=300)
    waitStatus: Literal["completed", "timed_out"]
    waitOk: bool = Field(strict=True)

    @model_validator(mode="after")
    def _validate_route_payload(self) -> "DeviceOperationDeliveryManifestInput":
        version_pattern = r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,79}$"
        if self.route == "device_box_update":
            requested_version_valid = bool(
                re.fullmatch(version_pattern, self.requestedVersion)
            )
        elif self.route == "device_agent_update":
            requested_version_valid = self.requestedVersion == "latest"
        else:
            requested_version_valid = self.requestedVersion == ""
        if (
            not requested_version_valid
            or (
                self.currentBoxVersion
                and re.fullmatch(
                    version_pattern,
                    self.currentBoxVersion,
                )
                is None
            )
            or self.dispatchMessage != self.dispatchMessage.strip()
            or (
                self.dispatchMessage
                and not self.dispatchMessage.isprintable()
            )
            or ((self.waitStatus == "completed") is not self.waitOk)
        ):
            raise ValueError("device operation delivery is invalid")
        return self

    def to_metadata(self) -> dict[str, Any]:
        return {
            "route": self.route,
            "device_name": self.deviceName,
            "requested_version": self.requestedVersion,
            "current_box_version": self.currentBoxVersion,
            "dispatch_message": self.dispatchMessage,
            "wait_status": self.waitStatus,
            "wait_ok": self.waitOk,
        }


class DeviceOperationDeliveryActionInput(_StrictInputModel):
    """Slack이 최종 장비 작업 메시지를 보낸 뒤 보내는 activity receipt다."""

    name: Literal["device_operation_delivery"]
    phase: Literal["delivered"]
    delivery: DeviceOperationDeliveryManifestInput

    def to_metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "phase": self.phase,
            "delivery": self.delivery.to_metadata(),
        }


class DeviceHealthAlertTargetInput(_StrictInputModel):
    """Slack button value에서 API가 다시 검증할 exact 장비 대상을 받는다."""

    hospitalSeq: int = Field(ge=1)
    hospitalName: str = Field(min_length=1, max_length=160)
    hospitalLabel: str = Field(default="", max_length=320)
    roomName: str = Field(min_length=1, max_length=160)
    deviceName: str = Field(
        min_length=1,
        max_length=160,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,159}$",
    )
    issue: str = Field(min_length=1, max_length=1_000)
    alertCategory: str = Field(default="", max_length=80)
    mdaUrl: str = Field(default="", max_length=2_048)
    problemComponents: list[str] = Field(
        default_factory=list,
        max_length=16,
    )

    @field_validator(
        "hospitalName",
        "hospitalLabel",
        "roomName",
        "issue",
        "alertCategory",
    )
    @classmethod
    def _normalize_text(cls, value: str) -> str:
        return " ".join(value.split())

    @field_validator("problemComponents")
    @classmethod
    def _normalize_components(cls, value: list[str]) -> list[str]:
        normalized = [" ".join(component.split()) for component in value]
        if any(not component or len(component) > 80 for component in normalized):
            raise ValueError("problem component is invalid")
        if len(set(normalized)) != len(normalized):
            raise ValueError("problem components must be unique")
        return normalized

    def to_metadata(self) -> dict[str, Any]:
        return {
            "hospital_seq": self.hospitalSeq,
            "hospital_name": self.hospitalName,
            "hospital_label": self.hospitalLabel,
            "room_name": self.roomName,
            "device_name": self.deviceName,
            "issue": self.issue,
            "alert_category": self.alertCategory,
            "mda_url": self.mdaUrl.strip(),
            "problem_components": list(self.problemComponents),
        }


class DeviceHealthAlertSmsInput(_StrictInputModel):
    phoneNumber: str = Field(
        min_length=10,
        max_length=24,
        pattern=r"^[+0-9() -]+$",
    )
    message: str = Field(min_length=1, max_length=1_000)

    @field_validator("phoneNumber", "message")
    @classmethod
    def _strip_value(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("SMS value must not be blank")
        return normalized

    def to_metadata(self) -> dict[str, str]:
        return {
            "phone_number": self.phoneNumber,
            "message": self.message,
        }


class DeviceHealthAlertActionInput(_StrictInputModel):
    """질문 추론 없이 실행할 수 있는 장비 이상 알림 action 계약이다."""

    name: Literal[
        "device_health_alert_contact_hospital",
        "device_health_alert_device_voice_guide",
        "device_health_alert_mark_done",
    ]
    phase: Literal["prepare", "execute"]
    target: DeviceHealthAlertTargetInput
    sms: DeviceHealthAlertSmsInput | None = None

    @model_validator(mode="after")
    def _validate_sms_payload(self) -> "DeviceHealthAlertActionInput":
        is_sms_action = self.name == "device_health_alert_contact_hospital"
        if not is_sms_action and self.phase != "execute":
            raise ValueError("non-SMS action phase must be execute")
        if not is_sms_action and self.sms is not None:
            raise ValueError("SMS payload is only valid for the SMS action")
        if is_sms_action and self.phase == "prepare" and self.sms is not None:
            raise ValueError("SMS prepare phase must not include SMS input")
        if is_sms_action and self.phase == "execute" and self.sms is None:
            raise ValueError("SMS execute phase requires SMS input")
        return self

    def to_metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": self.name,
            "phase": self.phase,
            "target": self.target.to_metadata(),
        }
        if self.sms is not None:
            metadata["sms"] = self.sms.to_metadata()
        return metadata


class DeviceHealthAlertUiReceiptInput(_StrictInputModel):
    """Slack만 아는 modal open 결과를 API event writer로 돌려보낸다."""

    name: Literal["device_health_alert_ui_receipt"]
    phase: Literal["receipt"]
    eventType: Literal["alert_contact_sms_modal_requested"]
    actionId: Literal[
        "device_health_alert_contact_hospital",
        "device_health_alert_view_auto_sms",
    ]
    mode: Literal["send", "view_auto_sent"]
    target: DeviceHealthAlertTargetInput
    messageTs: str = Field(pattern=r"^\d{1,20}(?:\.\d{1,9})?$")
    threadTs: str = Field(pattern=r"^\d{1,20}(?:\.\d{1,9})?$")
    occurredAt: str = Field(min_length=1, max_length=64)
    status: Literal[
        "missing_trigger_id",
        "modal_opened",
        "modal_open_failed",
    ]
    ok: bool
    errorType: str = Field(default="", max_length=160)

    @field_validator("occurredAt")
    @classmethod
    def _validate_occurred_at(cls, value: str) -> str:
        normalized = value.strip()
        try:
            parsed = datetime.fromisoformat(
                normalized.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError("occurredAt must be an ISO-8601 timestamp") from exc
        if parsed.tzinfo is None:
            raise ValueError("occurredAt must include timezone")
        return normalized

    @model_validator(mode="after")
    def _validate_result(self) -> "DeviceHealthAlertUiReceiptInput":
        if self.ok != (self.status == "modal_opened"):
            raise ValueError("modal receipt status and ok must match")
        if self.errorType and self.status != "modal_open_failed":
            raise ValueError("errorType is only valid for modal_open_failed")
        return self

    def to_metadata(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "phase": self.phase,
            "event_type": self.eventType,
            "action_id": self.actionId,
            "mode": self.mode,
            "target": self.target.to_metadata(),
            "message_ts": self.messageTs,
            "thread_ts": self.threadTs,
            "occurred_at": self.occurredAt,
            "status": self.status,
            "ok": self.ok,
            "error_type": self.errorType,
        }


class SecurityReviewTargetInput(_StrictInputModel):
    """Slack이 확인한 봇 identity만 받고 사용자 profile 원문은 받지 않는다."""

    userId: str = Field(
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )
    botId: str = Field(
        default="",
        max_length=256,
        pattern=r"^(?:[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255})?$",
    )
    appId: str = Field(
        default="",
        max_length=256,
        pattern=r"^(?:[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255})?$",
    )
    name: str = Field(default="", max_length=160)

    @field_validator("name")
    @classmethod
    def _normalize_name(cls, value: str) -> str:
        normalized = " ".join(value.split())
        if any(ord(character) < 32 for character in normalized):
            raise ValueError("security review target name is invalid")
        return normalized

    def to_metadata(self) -> dict[str, str]:
        return {
            "user_id": self.userId,
            "bot_id": self.botId,
            "app_id": self.appId,
            "name": self.name,
        }


class SecurityReviewActionInput(_StrictInputModel):
    """Slack UI와 API 소유 보안검토 state machine 사이의 typed action이다."""

    name: Literal["security_review"]
    phase: Literal["start", "respond", "summary", "cancel"]
    target: SecurityReviewTargetInput | None = None
    responseText: str = Field(default="", max_length=30_000)

    @field_validator("responseText")
    @classmethod
    def _strip_response_text(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def _validate_phase_payload(self) -> "SecurityReviewActionInput":
        if self.phase in {"start", "respond"} and self.target is None:
            raise ValueError("security review phase requires target")
        if self.phase != "respond" and self.responseText:
            raise ValueError("security review response is only valid for respond")
        if self.phase in {"summary", "cancel"} and self.target is not None:
            raise ValueError("security review terminal phase must not include target")
        return self

    def to_metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": self.name,
            "phase": self.phase,
            "response_text": self.responseText,
        }
        if self.target is not None:
            metadata["target"] = self.target.to_metadata()
        return metadata


class DeviceDiagnosticFollowupProbeActionInput(_StrictInputModel):
    """API process가 소유한 현재 thread의 진단 snapshot만 확인한다."""

    name: Literal["device_diagnostic_followup_probe"]

    def to_metadata(self) -> dict[str, str]:
        return {"name": self.name}


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
    question: str = Field(max_length=_MAX_QUESTION_CHARS)
    locale: str = Field(
        min_length=2,
        max_length=35,
        pattern=_LOCALE_PATTERN,
    )
    contextEntries: list[ContextEntryInput] = Field(
        default_factory=list,
        max_length=_MAX_OPERATION_CONTEXT_ENTRIES,
    )
    scope: AssistantTurnScopeInput | None = None
    auditContext: AssistantTurnAuditContextInput | None = None
    routeGroup: Literal[
        "notion",
        "device",
        "device_detail",
        "failure",
        "log",
        "structured",
        "barcode",
        "knowledge",
        "freeform",
        "health",
        "fun",
        "operations",
    ] | None = None
    operationAction: (
        DeviceFileDownloadDeliveryActionInput
        | DeviceOperationDeliveryActionInput
        | RequestLogDeliveryActionInput
        | DeviceHealthAlertActionInput
        | DeviceHealthAlertUiReceiptInput
        | SecurityReviewActionInput
        | DeviceDiagnosticFollowupProbeActionInput
        | None
    ) = None
    # 사람 team-fun은 Slack이 이미 최신 5k window로 렌더한 문자열을
    # 그대로 넘긴다. 일반 context entry와 별도의 typed 필드로 유지한다.
    funContext: str | None = Field(default=None, max_length=_MAX_CONTEXT_CHARS)

    @field_validator("question")
    @classmethod
    def _normalize_question(cls, value: str) -> str:
        return value.strip()

    @model_validator(mode="after")
    def _validate_question_scope(self) -> "AssistantTurnInput":
        # bot만 멘션한 turn은 기존 freeform의 missing_question 안내가
        # 처리한다. 다른 stage는 빈 요청으로 matcher를 우회하지 못하게 한다.
        if not self.question and self.routeGroup != "freeform":
            raise ValueError("question must not be blank")
        return self

    @model_validator(mode="after")
    def _validate_context_budget(self) -> "AssistantTurnInput":
        max_entries = (
            _MAX_OPERATION_CONTEXT_ENTRIES
            if self.routeGroup == "operations"
            else _MAX_CONTEXT_ENTRIES
        )
        max_chars = (
            _MAX_OPERATION_CONTEXT_CHARS
            if self.routeGroup == "operations"
            else _MAX_CONTEXT_CHARS
        )
        if (
            len(self.contextEntries) > max_entries
            or sum(len(entry.text) for entry in self.contextEntries)
            > max_chars
            or (
                self.routeGroup != "operations"
                and any(
                    len(entry.text) > _MAX_CONTEXT_ENTRY_CHARS
                    for entry in self.contextEntries
                )
            )
        ):
            raise ValueError("contextEntries exceed the text budget")
        return self

    @model_validator(mode="after")
    def _validate_operation_action_scope(self) -> "AssistantTurnInput":
        if self.operationAction is not None and self.routeGroup != "operations":
            raise ValueError("operationAction requires routeGroup=operations")
        if self.auditContext is not None:
            if self.routeGroup != "operations" or self.channel != "slack":
                raise ValueError(
                    "auditContext requires Slack routeGroup=operations"
                )
            if (
                self.conversationId != self.auditContext.threadId
                or self.scope is None
                or self.scope.channelContextId
                != self.auditContext.channelId
            ):
                # tenant/actor는 top-level caller 경계가 정본이고, 채널과
                # thread만 auditContext의 Slack identity와 교차 검증한다.
                raise ValueError("auditContext scope is inconsistent")
        if (
            isinstance(self.operationAction, RequestLogDeliveryActionInput)
            and self.auditContext is None
        ):
            raise ValueError("request-log delivery requires auditContext")
        if self.funContext is not None and self.routeGroup != "fun":
            raise ValueError("funContext requires routeGroup=fun")
        if (
            isinstance(
                self.operationAction,
                (
                    DeviceHealthAlertActionInput,
                    DeviceHealthAlertUiReceiptInput,
                ),
            )
            and self.scope is not None
            and self.scope.deviceName is not None
            and self.scope.deviceName.casefold()
            != self.operationAction.target.deviceName.casefold()
        ):
            # scope는 target을 보충하지 않고 같은 exact 장비인지 검증만 한다.
            raise ValueError("scope deviceName must match operationAction target")
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
        if self.operationAction is not None:
            # 질문 원문과 분리된 typed action만 operation route가 읽는다.
            metadata["operation_action"] = self.operationAction.to_metadata()
        if self.funContext is not None:
            # fun 전용 typed context는 일반 ContextEntry 렌더 접두사 없이
            # 기존 Slack prompt에 쓰던 최신-window 문자열을 보존한다.
            metadata["team_fun_context"] = self.funContext
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


class AssistantLinkOutput(BaseModel):
    label: str = Field(min_length=1, max_length=255)
    uri: str = Field(
        min_length=1,
        max_length=_MAX_PRIVATE_LINK_URI_CHARS,
    )


class AssistantMessageOutput(BaseModel):
    body: str = Field(min_length=1, max_length=_MAX_MESSAGE_CHARS)
    deliveryScope: Literal["conversation", "requester"]
    mentionActor: bool
    format: Literal["commonmark"]
    privateLinks: list[AssistantLinkOutput] = Field(
        default_factory=list,
    )


class SourceReferenceOutput(BaseModel):
    sourceId: str = Field(min_length=1, max_length=512)
    title: str = Field(min_length=1, max_length=2_000)
    uri: str = Field(min_length=1, max_length=2_048)
    score: float | None


class SmsDeliveryTargetOutput(_StrictInputModel):
    hospital: str = Field(min_length=1, max_length=160)
    room: str = Field(min_length=1, max_length=160)
    device: str = Field(min_length=1, max_length=160)
    components: list[str] = Field(default_factory=list, max_length=16)
    issue: str = Field(min_length=1, max_length=1_000)


class SmsDeliveryOperationResultOutput(_StrictInputModel):
    """전화번호·본문 없이 delivery reporter가 보존할 최소 provider receipt다."""

    kind: Literal["sms_delivery"]
    provider: Literal["solapi"]
    deliveryStatus: Literal[
        "accepted",
        "delivered",
        "delivery_failed",
        "confirm_required",
    ]
    groupId: str = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[A-Za-z0-9._:@/-]+$",
    )
    messageId: str = Field(
        default="",
        max_length=256,
        pattern=r"^[A-Za-z0-9._:@/-]*$",
    )
    acceptedAt: str = Field(min_length=1, max_length=64)
    target: SmsDeliveryTargetOutput

    @field_validator("acceptedAt")
    @classmethod
    def _validate_accepted_at(cls, value: str) -> str:
        normalized = value.strip()
        try:
            datetime.fromisoformat(normalized.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("acceptedAt must be ISO-8601") from exc
        return normalized


class SmsContactPreparationOutput(_StrictInputModel):
    """모달 렌더에만 쓰고 대화 본문/로그에는 넣지 않는 requester PII다."""

    kind: Literal["sms_contact_preparation"]
    deliveryScope: Literal["requester"]
    phoneNumber: str = Field(
        default="",
        max_length=24,
        pattern=r"^[0-9]*$",
    )
    message: str = Field(default="", max_length=1_000)
    templateId: str = Field(min_length=1, max_length=80)
    target: SmsDeliveryTargetOutput


class DeviceHealthAlertAcknowledgementOutput(_StrictInputModel):
    """Slack 카드 갱신에 필요한 최초 담당자·시간만 내보낸다."""

    kind: Literal["device_health_alert_ack"]
    created: StrictBool
    actorUserId: str = Field(
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )
    acknowledgedAt: str = Field(min_length=1, max_length=64)
    target: SmsDeliveryTargetOutput

    @field_validator("acknowledgedAt")
    @classmethod
    def _validate_acknowledged_at(cls, value: str) -> str:
        normalized = value.strip()
        try:
            parsed = datetime.fromisoformat(
                normalized.replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise ValueError(
                "acknowledgedAt must be ISO-8601"
            ) from exc
        if parsed.tzinfo is None:
            raise ValueError("acknowledgedAt must include timezone")
        return normalized


class SecurityReviewStepOutput(_StrictInputModel):
    """Slack renderer가 다음 probe 또는 최종 report만 전달받는 안전한 DTO다."""

    kind: Literal["security_review_step"]
    status: Literal[
        "started",
        "continued",
        "completed",
        "summary",
        "no_session",
        "ignored",
        "cancelled",
    ]
    targetUserId: str = Field(
        default="",
        max_length=256,
        pattern=r"^(?:[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255})?$",
    )
    probeIndex: int = Field(ge=0, le=128)
    probeTotal: int = Field(ge=1, le=128)
    probeTitle: str = Field(default="", max_length=160)
    probePrompt: str = Field(default="", max_length=4_000)
    report: str = Field(default="", max_length=20_000)

    @model_validator(mode="after")
    def _validate_status_payload(self) -> "SecurityReviewStepOutput":
        if self.status in {"started", "continued"}:
            if (
                not self.targetUserId
                or not self.probeTitle.strip()
                or not self.probePrompt.strip()
                or self.report
                or not 1 <= self.probeIndex <= self.probeTotal
            ):
                raise ValueError("security review probe result is invalid")
        elif self.status in {"completed", "summary"}:
            if (
                not self.targetUserId
                or self.probeTitle
                or self.probePrompt
                or not self.report.strip()
                or self.probeIndex > self.probeTotal
            ):
                raise ValueError("security review report result is invalid")
        elif self.probeTitle or self.probePrompt or self.report:
            raise ValueError("security review empty result is invalid")
        return self


class DeviceFileDownloadLinkContextOutput(_StrictInputModel):
    """presigned URL과 분리해 Slack 링크 DM 표시에만 쓰는 문맥이다."""

    deviceName: str = Field(min_length=1, max_length=160)
    fileName: str = Field(min_length=1, max_length=255)


class DeviceFileDownloadDeliveryOutput(_StrictInputModel):
    """Slack이 모든 requester-only 링크를 전달하기 전의 typed 결과다."""

    kind: Literal["device_file_download_delivery"]
    status: Literal["pending"]
    failureNotice: str = Field(min_length=1, max_length=_MAX_MESSAGE_CHARS)
    linkCount: int = Field(ge=1)
    links: list[DeviceFileDownloadLinkContextOutput]
    delivery: DeviceFileDownloadDeliveryManifestInput

    @model_validator(mode="after")
    def _validate_link_count(self) -> "DeviceFileDownloadDeliveryOutput":
        if self.linkCount != len(self.links):
            raise ValueError("download delivery link count is invalid")
        return self


class DeviceOperationDeliveryOutput(_StrictInputModel):
    """Slack 최종 응답 전까지 activity receipt를 보류하는 typed 결과다."""

    kind: Literal["device_operation_delivery"]
    status: Literal["pending"]
    delivery: DeviceOperationDeliveryManifestInput


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
    operationResult: (
        DeviceFileDownloadDeliveryOutput
        | DeviceOperationDeliveryOutput
        | SmsDeliveryOperationResultOutput
        | SmsContactPreparationOutput
        | DeviceHealthAlertAcknowledgementOutput
        | SecurityReviewStepOutput
        | None
    ) = None


def _serialize_operation_result(
    value: Any,
    ) -> (
    DeviceFileDownloadDeliveryOutput
    | DeviceOperationDeliveryOutput
    | SmsDeliveryOperationResultOutput
    | SmsContactPreparationOutput
    | DeviceHealthAlertAcknowledgementOutput
    | SecurityReviewStepOutput
    | None
):
    if not isinstance(value, dict):
        return None
    # route가 만든 allowlisted receipt만 직렬화해 전화번호·문자본문 같은
    # provider 요청 원문이 HTTP 응답에 섞이지 않게 한다.
    if value.get("kind") == "device_file_download_delivery":
        return DeviceFileDownloadDeliveryOutput.model_validate(value)
    if value.get("kind") == "device_operation_delivery":
        return DeviceOperationDeliveryOutput.model_validate(value)
    if value.get("kind") == "sms_delivery":
        return SmsDeliveryOperationResultOutput.model_validate(value)
    if value.get("kind") == "sms_contact_preparation":
        return SmsContactPreparationOutput.model_validate(value)
    if value.get("kind") == "device_health_alert_ack":
        return DeviceHealthAlertAcknowledgementOutput.model_validate(value)
    if value.get("kind") == "security_review_step":
        return SecurityReviewStepOutput.model_validate(value)
    return None


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
        return _dump_turn_output(payload)

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
        operationResult=_serialize_operation_result(
            result.operation_result
        ),
    )
    return _dump_turn_output(_fit_response_byte_budget(payload))


def _serialize_messages(
    messages: tuple[Any, ...],
) -> list[AssistantMessageOutput]:
    chunks: list[AssistantMessageOutput] = []
    was_truncated = False
    links_by_message = tuple(
        _serialize_private_links(message) for message in messages
    )
    private_uris = {
        link.uri
        for links in links_by_message
        for link in links
    }
    for message_index, message in enumerate(messages):
        body = str(message.body or "")
        private_links = links_by_message[message_index]
        # presigned URL은 typed privateLinks 필드 하나로만 전달한다. 다른
        # 공개 메시지나 code fence에 같은 URI가 섞여도 전역으로 제거한다.
        for private_uri in private_uris:
            body = body.replace(private_uri, "[비공개 링크 생략]")
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
                    # presigned URL은 요청자 전용 메시지의 첫 transport
                    # chunk에만 싣고 공개 메시지나 후속 chunk로 복제하지 않는다.
                    privateLinks=(
                        private_links
                        if offset == 0
                        else []
                    ),
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


def _serialize_private_links(message: Any) -> list[AssistantLinkOutput]:
    if getattr(message, "delivery_scope", None) != "requester":
        return []
    serialized: list[AssistantLinkOutput] = []
    for link in tuple(getattr(message, "private_links", ()) or ()):
        label = _safe_private_link_label(getattr(link, "label", None))
        uri = _safe_private_link_uri(getattr(link, "uri", None))
        if label is None or uri is None:
            continue
        serialized.append(AssistantLinkOutput(label=label, uri=uri))
    return serialized


def _safe_private_link_label(value: object) -> str | None:
    normalized = str(value or "").strip()
    if (
        not normalized
        or any(ord(character) < 32 for character in normalized)
    ):
        return None
    return normalized[:255]


def _safe_private_link_uri(value: object) -> str | None:
    # source와 달리 requester-only 다운로드 링크는 서명 query를 보존한다.
    normalized = str(value or "").strip()
    if (
        not normalized
        or len(normalized) > _MAX_PRIVATE_LINK_URI_CHARS
        or any(
            character.isspace() or ord(character) < 32
            for character in normalized
        )
        or any(character in normalized for character in "<>|")
    ):
        return None
    try:
        parsed = urlsplit(normalized)
    except ValueError:
        return None
    if not (
        parsed.scheme.lower() == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
    ):
        return None
    return normalized


def _dump_turn_output(payload: AssistantTurnOutput) -> dict[str, Any]:
    # 기존 client와의 additive 호환을 위해 링크가 없는 메시지에는 새 키를
    # 내보내지 않는다. 링크가 있을 때만 requester 전용 확장 계약을 사용한다.
    serialized = payload.model_dump(mode="json")
    for message in serialized.get("messages") or []:
        if not message.get("privateLinks"):
            message.pop("privateLinks", None)
    if serialized.get("operationResult") is None:
        # 기존 read-only client에는 action 전용 확장 키를 내보내지 않는다.
        serialized.pop("operationResult", None)
    return serialized


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
    "AssistantLinkOutput",
    "AssistantMessageOutput",
    "AssistantTurnInput",
    "AssistantTurnOutput",
    "AssistantTurnScopeInput",
    "ContextEntryInput",
    "SourceReferenceOutput",
    "serialize_result",
]
