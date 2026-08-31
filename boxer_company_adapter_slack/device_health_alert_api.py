from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
import re
from typing import Any, Mapping, Protocol

from boxer_company.assistant.contracts import (
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.transport_contracts import (
    DEVICE_HEALTH_ALERT_ACK_REQUEST_ID_PREFIX,
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
    DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ACTION,
    DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
    DEVICE_HEALTH_ALERT_SMS_ROUTE,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
    DEVICE_HEALTH_ALERT_VOICE_ACTION,
    DEVICE_HEALTH_ALERT_VOICE_ROUTE,
)
from boxer_company import settings as company_settings
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
)


_FIXED_ACTION_QUESTION = "device health alert action"
_SMS_PHONE_PATTERN = re.compile(r"^[+0-9() -]{10,24}$")
_SCOPED_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$")
_ACTION_ROUTES = {
    (DEVICE_HEALTH_ALERT_SMS_ACTION, "prepare"): (
        DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE
    ),
    (DEVICE_HEALTH_ALERT_SMS_ACTION, "execute"): (
        DEVICE_HEALTH_ALERT_SMS_ROUTE
    ),
    (DEVICE_HEALTH_ALERT_VOICE_ACTION, "execute"): (
        DEVICE_HEALTH_ALERT_VOICE_ROUTE
    ),
    (DEVICE_HEALTH_ALERT_MARK_DONE_ACTION, "execute"): (
        DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE
    ),
    (DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION, "receipt"): (
        DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE
    ),
}


class _CompanyAssistantApiClient(Protocol):
    def answer(
        self,
        request: CompanyAssistantRequest,
        *,
        route_group: str | None = None,
    ) -> CompanyAssistantResult: ...


@dataclass(frozen=True, slots=True)
class DeviceHealthAlertApiTarget:
    hospital_seq: int
    hospital_name: str
    room_name: str
    device_name: str
    issue: str
    alert_category: str = ""
    problem_components: tuple[str, ...] = ()
    hospital_label: str = ""
    mda_url: str = ""

    def to_metadata(self) -> dict[str, Any]:
        return {
            "hospital_seq": self.hospital_seq,
            "hospital_name": self.hospital_name,
            "room_name": self.room_name,
            "device_name": self.device_name,
            "issue": self.issue,
            "alert_category": self.alert_category,
            "problem_components": list(self.problem_components),
            "hospital_label": self.hospital_label,
            "mda_url": self.mda_url,
        }


@dataclass(frozen=True, slots=True)
class DeviceHealthAlertApiResult:
    route: str
    outcome: str
    messages: tuple[str, ...]
    operation_result: Mapping[str, Any] | None


def build_device_health_alert_api_target(
    raw_item: Mapping[str, Any],
) -> DeviceHealthAlertApiTarget:
    """Slack button value를 exact API target으로 fail-closed 정규화한다."""

    try:
        hospital_seq = int(raw_item.get("hospitalSeq") or 0)
    except (TypeError, ValueError) as exc:
        raise CompanyApiContractError(
            "device_health_alert_hospital_seq_invalid"
        ) from exc
    hospital_name = _normalized_text(raw_item.get("hospitalName"))
    room_name = _normalized_text(raw_item.get("room"))
    device_name = str(raw_item.get("device") or "").strip()
    issue = _normalized_text(raw_item.get("issue"))
    alert_category = str(raw_item.get("alertCategory") or "").strip()
    components = _normalized_components(raw_item.get("problemComponents"))
    if (
        hospital_seq <= 0
        or not hospital_name
        or not room_name
        or not device_name
        or not company_settings.S3_DEVICE_NAME_PATTERN.fullmatch(device_name)
        or not issue
    ):
        raise CompanyApiContractError(
            "device_health_alert_target_invalid"
        )
    return DeviceHealthAlertApiTarget(
        hospital_seq=hospital_seq,
        hospital_name=hospital_name,
        room_name=room_name,
        device_name=device_name,
        issue=issue,
        alert_category=alert_category,
        problem_components=components,
        hospital_label=_normalized_text(raw_item.get("hospital")),
        mda_url=str(raw_item.get("mdaUrl") or "").strip(),
    )


def build_device_health_alert_request_id(
    *,
    workspace_id: str,
    actor_user_id: str,
    channel_id: str,
    message_ts: str,
    interaction_id: str,
    action_name: str,
    phase: str,
) -> str:
    """Slack redelivery가 같은 ID를 만들도록 불변 interaction identity를 해시한다."""

    parts = tuple(
        str(value or "").strip()
        for value in (
            workspace_id,
            actor_user_id,
            channel_id,
            message_ts,
            interaction_id,
            action_name,
            phase,
        )
    )
    if any(not part for part in parts):
        raise CompanyApiContractError(
            "device_health_alert_request_identity_missing"
        )
    if (action_name, phase) not in _ACTION_ROUTES:
        raise CompanyApiContractError(
            "device_health_alert_action_invalid"
        )
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()
    prefix = (
        DEVICE_HEALTH_ALERT_ACK_REQUEST_ID_PREFIX
        if (
            action_name == DEVICE_HEALTH_ALERT_MARK_DONE_ACTION
            and phase == "execute"
        )
        else "slack-device-alert-"
    )
    return f"{prefix}{digest[:40]}"


class DeviceHealthAlertApiBridge:
    """Slack action을 fallback/retry 없이 operations API 한 번으로 연결한다."""

    def __init__(self, client: _CompanyAssistantApiClient) -> None:
        self._client = client

    def prepare_sms(
        self,
        *,
        request_id: str,
        workspace_id: str,
        actor_user_id: str,
        channel_id: str,
        conversation_id: str,
        target: DeviceHealthAlertApiTarget,
    ) -> DeviceHealthAlertApiResult:
        return self._execute(
            request_id=request_id,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            channel_id=channel_id,
            conversation_id=conversation_id,
            action_name=DEVICE_HEALTH_ALERT_SMS_ACTION,
            phase="prepare",
            target=target,
        )

    def send_sms(
        self,
        *,
        request_id: str,
        workspace_id: str,
        actor_user_id: str,
        channel_id: str,
        conversation_id: str,
        target: DeviceHealthAlertApiTarget,
        phone_number: str,
        message: str,
    ) -> DeviceHealthAlertApiResult:
        normalized_phone = str(phone_number or "").strip()
        normalized_message = str(message or "").strip()
        if not _SMS_PHONE_PATTERN.fullmatch(normalized_phone):
            raise CompanyApiContractError(
                "device_health_alert_sms_phone_invalid"
            )
        if not normalized_message or len(normalized_message) > 1_000:
            raise CompanyApiContractError(
                "device_health_alert_sms_message_invalid"
            )
        return self._execute(
            request_id=request_id,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            channel_id=channel_id,
            conversation_id=conversation_id,
            action_name=DEVICE_HEALTH_ALERT_SMS_ACTION,
            phase="execute",
            target=target,
            sms={
                "phone_number": normalized_phone,
                "message": normalized_message,
            },
        )

    def send_voice_guide(
        self,
        *,
        request_id: str,
        workspace_id: str,
        actor_user_id: str,
        channel_id: str,
        conversation_id: str,
        target: DeviceHealthAlertApiTarget,
    ) -> DeviceHealthAlertApiResult:
        return self._execute(
            request_id=request_id,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            channel_id=channel_id,
            conversation_id=conversation_id,
            action_name=DEVICE_HEALTH_ALERT_VOICE_ACTION,
            phase="execute",
            target=target,
        )

    def mark_done(
        self,
        *,
        request_id: str,
        workspace_id: str,
        actor_user_id: str,
        channel_id: str,
        conversation_id: str,
        target: DeviceHealthAlertApiTarget,
    ) -> DeviceHealthAlertApiResult:
        return self._execute(
            request_id=request_id,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            channel_id=channel_id,
            conversation_id=conversation_id,
            action_name=DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
            phase="execute",
            target=target,
        )

    def record_modal_receipt(
        self,
        *,
        request_id: str,
        workspace_id: str,
        actor_user_id: str,
        channel_id: str,
        conversation_id: str,
        target: DeviceHealthAlertApiTarget,
        action_id: str,
        mode: str,
        message_ts: str,
        thread_ts: str,
        occurred_at: str,
        status: str,
        ok: bool,
        error_type: str = "",
    ) -> DeviceHealthAlertApiResult:
        """Slack views_open 결과만 별도 typed receipt로 한 번 전달한다."""

        return self._execute(
            request_id=request_id,
            workspace_id=workspace_id,
            actor_user_id=actor_user_id,
            channel_id=channel_id,
            conversation_id=conversation_id,
            action_name=DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION,
            phase="receipt",
            target=target,
            ui_receipt={
                "event_type": "alert_contact_sms_modal_requested",
                "action_id": str(action_id or "").strip(),
                "mode": str(mode or "").strip(),
                "message_ts": str(message_ts or "").strip(),
                "thread_ts": str(thread_ts or "").strip(),
                "occurred_at": str(occurred_at or "").strip(),
                "status": str(status or "").strip(),
                "ok": bool(ok),
                "error_type": str(error_type or "").strip(),
            },
        )

    def _execute(
        self,
        *,
        request_id: str,
        workspace_id: str,
        actor_user_id: str,
        channel_id: str,
        conversation_id: str,
        action_name: str,
        phase: str,
        target: DeviceHealthAlertApiTarget,
        sms: Mapping[str, str] | None = None,
        ui_receipt: Mapping[str, Any] | None = None,
    ) -> DeviceHealthAlertApiResult:
        expected_route = _ACTION_ROUTES.get((action_name, phase))
        if expected_route is None:
            raise CompanyApiContractError(
                "device_health_alert_action_invalid",
                request_id=request_id,
            )
        metadata: dict[str, Any] = {
            "route_group": "operations",
            "channel_id": channel_id,
            "operation_action": {
                "name": action_name,
                "phase": phase,
                "target": target.to_metadata(),
            },
        }
        if sms is not None:
            metadata["operation_action"]["sms"] = dict(sms)
        if ui_receipt is not None:
            metadata["operation_action"].update(dict(ui_receipt))
        request = CompanyAssistantRequest(
            request_id=request_id,
            tenant_id=workspace_id,
            actor_id=actor_user_id,
            channel="slack",
            conversation_id=conversation_id,
            question=_FIXED_ACTION_QUESTION,
            locale="ko",
            metadata=metadata,
        )
        # CompanyAssistantApiClient는 operations를 0 retry로 전송한다. 이
        # bridge도 예외를 잡아 local 실행하거나 같은 요청을 다시 보내지 않는다.
        result = self._client.answer(request, route_group="operations")
        validated = _validate_device_health_alert_api_result(
            result,
            expected_route=expected_route,
            target=target,
        )
        receipt = validated.operation_result
        if (
            expected_route == DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE
            and isinstance(receipt, Mapping)
            and receipt.get("created") is True
            and str(receipt.get("actorUserId") or "").strip()
            != str(actor_user_id or "").strip()
        ):
            # 최초 claim은 현재 요청자여야 한다. 재클릭(created=false)만
            # 앞선 담당자의 ID를 합법적으로 돌려줄 수 있다.
            raise CompanyApiContractError(
                "device_health_alert_ack_actor_mismatch"
            )
        return validated


def _validate_device_health_alert_api_result(
    result: CompanyAssistantResult,
    *,
    expected_route: str,
    target: DeviceHealthAlertApiTarget,
) -> DeviceHealthAlertApiResult:
    if result.route != expected_route:
        raise CompanyApiContractError(
            "device_health_alert_route_mismatch"
        )
    if (
        result.used_llm
        or result.sources
    ):
        raise CompanyApiContractError(
            "device_health_alert_response_unsafe"
        )
    messages = tuple(str(message.body or "") for message in result.messages)
    if not messages or any(not message.strip() for message in messages):
        raise CompanyApiContractError(
            "device_health_alert_messages_invalid"
        )
    receipt = result.operation_result
    if expected_route == DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE:
        if (
            not isinstance(receipt, Mapping)
            or receipt.get("kind") != "sms_contact_preparation"
            or receipt.get("deliveryScope") != "requester"
        ):
            raise CompanyApiContractError(
                "device_health_alert_prepare_receipt_invalid"
            )
        _validate_receipt_target(receipt, target)
        if any(message.delivery_scope != "requester" for message in result.messages):
            raise CompanyApiContractError(
                "device_health_alert_prepare_scope_invalid"
            )
    elif expected_route == DEVICE_HEALTH_ALERT_SMS_ROUTE:
        if receipt is not None:
            if (
                not isinstance(receipt, Mapping)
                or receipt.get("kind") != "sms_delivery"
                or any(key in receipt for key in ("phoneNumber", "message", "sms"))
            ):
                raise CompanyApiContractError(
                    "device_health_alert_sms_receipt_invalid"
                )
            _validate_receipt_target(receipt, target)
            group_id = str(receipt.get("groupId") or "")
            if group_id and any(group_id in body for body in messages):
                raise CompanyApiContractError(
                    "device_health_alert_receipt_leaked"
                )
    elif expected_route == DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE:
        if result.outcome == "answered" and receipt is not None:
            if (
                not isinstance(receipt, Mapping)
                or frozenset(receipt)
                != {
                    "kind",
                    "created",
                    "actorUserId",
                    "acknowledgedAt",
                    "target",
                }
                or receipt.get("kind") != "device_health_alert_ack"
                or not isinstance(receipt.get("created"), bool)
                or not _valid_scoped_id(receipt.get("actorUserId"))
                or not _valid_acknowledged_at(
                    receipt.get("acknowledgedAt")
                )
            ):
                raise CompanyApiContractError(
                    "device_health_alert_ack_receipt_invalid"
                )
            _validate_receipt_target(receipt, target)
        elif result.outcome != "answered" and receipt is not None:
            # 실패·거절 응답이 완료 상태를 섞어 보내면 버튼 제거 여부를
            # 판단할 수 없으므로 fail-closed한다.
            raise CompanyApiContractError(
                "device_health_alert_ack_receipt_invalid"
            )
    elif receipt is not None:
        raise CompanyApiContractError(
            "device_health_alert_unexpected_receipt"
        )
    return DeviceHealthAlertApiResult(
        route=result.route,
        outcome=result.outcome,
        messages=messages,
        operation_result=receipt,
    )


def _validate_receipt_target(
    receipt: Mapping[str, Any],
    target: DeviceHealthAlertApiTarget,
) -> None:
    raw_target = receipt.get("target")
    if not isinstance(raw_target, Mapping) or (
        _normalized_text(raw_target.get("hospital")) != target.hospital_name
        or _normalized_text(raw_target.get("room")) != target.room_name
        or str(raw_target.get("device") or "").strip().casefold()
        != target.device_name.casefold()
    ):
        raise CompanyApiContractError(
            "device_health_alert_receipt_target_mismatch"
        )


def _normalized_text(value: Any) -> str:
    return " ".join(str(value or "").split())


def _valid_scoped_id(value: Any) -> bool:
    return _SCOPED_ID_PATTERN.fullmatch(str(value or "").strip()) is not None


def _valid_acknowledged_at(value: Any) -> bool:
    try:
        parsed = datetime.fromisoformat(
            str(value or "").strip().replace("Z", "+00:00")
        )
    except ValueError:
        return False
    return parsed.tzinfo is not None


def _normalized_components(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    components: list[str] = []
    for raw_component in value:
        component = _normalized_text(raw_component)
        if not component or len(component) > 80 or component in components:
            continue
        components.append(component)
    return tuple(components[:16])


__all__ = [
    "DeviceHealthAlertApiBridge",
    "DeviceHealthAlertApiResult",
    "DeviceHealthAlertApiTarget",
    "build_device_health_alert_api_target",
    "build_device_health_alert_request_id",
]
