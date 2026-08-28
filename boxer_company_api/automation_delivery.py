from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import re
from typing import Any, Callable, Mapping

from pydantic import BaseModel, ConfigDict, Field

from boxer_company.automation import (
    AutomationCycleName,
    AutomationCycleResult,
    AutomationDelivery,
    AutomationDeliveryReceipt,
)
from boxer_company_api.automation import (
    AutomationCycleContractError,
    AutomationDeliveryReceiptInput,
    AutomationCycleTrigger,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
)


_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
)
_CHANNEL_ID_PATTERN = re.compile(r"^[CGD][A-Z0-9]{5,31}$")
_SUPPORTED_CYCLES = frozenset(
    {
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
    }
)


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AutomationDeliveryPullInput(_StrictModel):
    tenantId: str = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$",
    )
    cycle: str | None = Field(default=None, max_length=64)


class AutomationDeliveryAckInput(_StrictModel):
    tenantId: str = Field(
        min_length=1,
        max_length=256,
        pattern=r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$",
    )
    batchId: str = Field(pattern=r"^batch:[0-9a-f]{64}$")
    deliveryReceipts: list[AutomationDeliveryReceiptInput] = Field(
        min_length=1,
        max_length=100,
    )

    def to_receipts(self) -> tuple[AutomationDeliveryReceipt, ...]:
        ids = [item.deliveryId for item in self.deliveryReceipts]
        if len(ids) != len(set(ids)):
            raise AutomationCycleContractError(
                "automation delivery acknowledgement is invalid"
            )
        return tuple(
            AutomationDeliveryReceipt(
                delivery_id=item.deliveryId,
                status=item.status,
                external_message_id=item.externalMessageId,
                permalink=item.permalink,
                delivered_at=item.deliveredAt,
            )
            for item in self.deliveryReceipts
        )


@dataclass(frozen=True, slots=True)
class AutomationDeliveryBatch:
    """API domain state에 남아 있는 Slack 전달 대상 한 묶음이다."""

    batch_id: str
    tenant_id: str
    cycle: AutomationCycleName
    cycle_key: str
    scheduled_at: datetime
    channel_id: str
    conversation: Mapping[str, Any] = field(default_factory=dict)
    deliveries: tuple[AutomationDelivery, ...] = field(
        default_factory=tuple,
        repr=False,
    )


class AutomationDeliveryBroker:
    """domain pending을 제거하지 않고 pull하고 exact receipt만 ACK한다."""

    def __init__(
        self,
        state_store: JsonAutomationCycleStateStore,
        coordinator: DurableAutomationCycleCoordinator,
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._state_store = state_store
        self._coordinator = coordinator
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def pull(
        self,
        *,
        tenant_id: str,
        cycle: str | None = None,
    ) -> AutomationDeliveryBatch | None:
        self._validate_pull_scope(tenant_id, cycle)
        candidates: list[tuple[str, AutomationDeliveryBatch]] = []
        with self._state_store.locked_snapshot() as snapshot:
            for state_key, state in snapshot.document["cycles"].items():
                if not isinstance(state, Mapping):
                    raise AutomationCycleContractError(
                        "automation delivery state is invalid"
                    )
                batch = _batch_from_state(state, state_key=state_key)
                if batch is None or batch.tenant_id != tenant_id:
                    continue
                if cycle is not None and batch.cycle != cycle:
                    continue
                # ACK hook의 완료 여부가 불명확한 batch는 Slack에 다시
                # 전달하지 않고 운영자 확인이 끝날 때까지 fail-closed한다.
                if isinstance(state.get("ackInFlight"), Mapping):
                    continue
                created_at = str(
                    state.get("pendingCreatedAt")
                    or state.get("lastCompletedAt")
                    or ""
                )
                candidates.append((created_at, batch))
        if not candidates:
            return None
        candidates.sort(key=lambda item: (item[0], item[1].batch_id))
        return candidates[0][1]

    def acknowledge(
        self,
        *,
        request_id: str,
        tenant_id: str,
        batch_id: str,
        receipts: tuple[AutomationDeliveryReceipt, ...],
    ) -> AutomationCycleResult:
        if (
            not _IDENTIFIER_PATTERN.fullmatch(str(request_id or ""))
            or not _IDENTIFIER_PATTERN.fullmatch(str(tenant_id or ""))
            or not re.fullmatch(r"batch:[0-9a-f]{64}", str(batch_id or ""))
            or not receipts
        ):
            raise AutomationCycleContractError(
                "automation delivery acknowledgement is invalid"
            )
        receipt_ids = tuple(receipt.delivery_id for receipt in receipts)
        if len(receipt_ids) != len(set(receipt_ids)):
            raise AutomationCycleContractError(
                "automation delivery acknowledgement is invalid"
            )

        target: AutomationDeliveryBatch | None = None
        duplicate_cycle: AutomationCycleName | None = None
        with self._state_store.locked_snapshot() as snapshot:
            for state_key, state in snapshot.document["cycles"].items():
                if not isinstance(state, Mapping):
                    raise AutomationCycleContractError(
                        "automation delivery state is invalid"
                    )
                identity = _validated_identity(state.get("identity"))
                if identity is None or identity[0] != tenant_id:
                    continue
                candidate_id = _build_batch_id(
                    tenant_id=identity[0],
                    cycle=identity[1],
                    cycle_key=identity[2],
                    delivery_ids=receipt_ids,
                )
                if candidate_id != batch_id:
                    continue
                pending_batch = _batch_from_state(
                    state,
                    state_key=state_key,
                )
                if pending_batch is not None:
                    pending_ids = {
                        delivery.delivery_id
                        for delivery in pending_batch.deliveries
                    }
                    if pending_ids != set(receipt_ids):
                        raise AutomationCycleContractError(
                            "automation delivery acknowledgement is partial"
                        )
                    target = pending_batch
                    break
                acknowledged = {
                    str(item)
                    for item in (
                        state.get("acknowledgedDeliveryIds") or []
                    )
                }
                if set(receipt_ids).issubset(acknowledged):
                    duplicate_cycle = identity[1]
                    break
                raise AutomationCycleContractError(
                    "automation delivery batch is not pending"
                )

        if target is None:
            if duplicate_cycle is not None:
                return AutomationCycleResult(
                    cycle=duplicate_cycle,
                    outcome="no_change",
                    cursor={},
                    metrics={"deliveryCount": 0, "duplicateAck": True},
                )
            raise AutomationCycleContractError(
                "automation delivery batch is unknown"
            )

        return self._coordinator.run(
            AutomationCycleTrigger(
                request_id=request_id,
                tenant_id=tenant_id,
                cycle=target.cycle,
                cycle_key=target.cycle_key,
                scheduled_at=self._aware_now(),
                delivery_receipts=receipts,
                ack_only=True,
            )
        )

    def _aware_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            raise AutomationCycleContractError(
                "automation delivery clock is invalid"
            )
        return value

    def _validate_pull_scope(
        self,
        tenant_id: str,
        cycle: str | None,
    ) -> None:
        if not _IDENTIFIER_PATTERN.fullmatch(str(tenant_id or "")):
            raise AutomationCycleContractError(
                "automation delivery tenant is invalid"
            )
        # 실행 feature flag는 새 cycle admission만 제어한다. 이미 durable
        # pending이 생긴 supported Slack cycle은 flag-off 재시작 뒤에도
        # pull/ACK로 끝까지 drain할 수 있어야 한다.
        if cycle is not None and cycle not in _SUPPORTED_CYCLES:
            raise AutomationCycleContractError(
                "automation delivery cycle is invalid"
            )


def validate_automation_delivery_state(
    state_store: JsonAutomationCycleStateStore,
) -> None:
    """기존 pending이 transport에서 숨지 않도록 raw state를 무수정 검사한다."""

    with state_store.locked_snapshot() as snapshot:
        for state_key, state in snapshot.document["cycles"].items():
            if not isinstance(state, Mapping):
                raise AutomationCycleContractError(
                    "automation delivery state is invalid"
                )
            # 빈 state와 ACK 완료 state는 transport metadata가 없어도 된다.
            # non-empty pending만 새 API-owned exact 계약을 반드시 만족한다.
            _batch_from_state(state, state_key=state_key)


def serialize_automation_delivery_batch(
    batch: AutomationDeliveryBatch | None,
    request_id: str,
) -> dict[str, Any]:
    """credential 없는 strict transport payload로 pending batch를 직렬화한다."""

    return {
        "requestId": request_id,
        "batch": (
            {
                "batchId": batch.batch_id,
                "tenantId": batch.tenant_id,
                "cycle": batch.cycle,
                "cycleKey": batch.cycle_key,
                "scheduledAt": batch.scheduled_at.isoformat(),
                "channelId": batch.channel_id,
                "conversation": dict(batch.conversation),
                "deliveries": [
                    {
                        "deliveryId": delivery.delivery_id,
                        "kind": delivery.kind,
                        "payload": dict(delivery.payload),
                    }
                    for delivery in batch.deliveries
                ],
            }
            if batch is not None
            else None
        ),
        "autoRetryAllowed": False,
    }


def serialize_automation_delivery_ack(
    *,
    batch_id: str,
    result: AutomationCycleResult,
    request_id: str,
) -> dict[str, Any]:
    """failed receipt가 pending을 남기면 Slack journal도 닫지 않게 알린다."""

    pending_count = len(result.deliveries)
    return {
        "requestId": request_id,
        "batchId": batch_id,
        "acknowledged": pending_count == 0,
        "pendingDeliveryCount": pending_count,
        "autoRetryAllowed": False,
    }


def _batch_from_state(
    state: Mapping[str, Any],
    *,
    state_key: str | None = None,
) -> AutomationDeliveryBatch | None:
    raw_pending = state.get("pendingDeliveries")
    if raw_pending in (None, []):
        return None
    if not isinstance(raw_pending, list) or not raw_pending:
        raise AutomationCycleContractError(
            "automation pending deliveries are invalid"
        )
    identity = _validated_identity(state.get("identity"))
    target = state.get("deliveryTarget")
    if identity is None or not isinstance(target, Mapping):
        # 구 /cycles state를 조용히 숨기면 scheduler도 pending 때문에 skip해
        # 영구 고립된다. 상태를 추측·수정하지 않고 startup/readiness를 닫는다.
        raise AutomationCycleContractError(
            "automation pending delivery metadata is missing"
        )
    if set(target) != {"channelId", "conversation"}:
        raise AutomationCycleContractError(
            "automation delivery target is invalid"
        )
    if state_key is not None and state_key != _build_state_key(
        tenant_id=identity[0],
        cycle=identity[1],
        cycle_key=identity[2],
    ):
        raise AutomationCycleContractError(
            "automation delivery identity does not match state"
        )
    channel_id = str(target.get("channelId") or "").strip()
    conversation = target.get("conversation")
    if (
        not _CHANNEL_ID_PATTERN.fullmatch(channel_id)
        or not isinstance(conversation, Mapping)
    ):
        raise AutomationCycleContractError(
            "automation delivery target is invalid"
        )
    deliveries = tuple(_restore_delivery(item) for item in raw_pending)
    delivery_ids = tuple(item.delivery_id for item in deliveries)
    if len(delivery_ids) != len(set(delivery_ids)):
        raise AutomationCycleContractError(
            "automation pending deliveries are invalid"
        )
    scheduled_text = str(
        state.get("pendingScheduledAt")
        or state.get("lastCompletedAt")
        or ""
    )
    try:
        scheduled_at = datetime.fromisoformat(scheduled_text)
    except ValueError as exc:
        raise AutomationCycleContractError(
            "automation delivery schedule is invalid"
        ) from exc
    if scheduled_at.tzinfo is None:
        raise AutomationCycleContractError(
            "automation delivery schedule is invalid"
        )
    batch_id = _build_batch_id(
        tenant_id=identity[0],
        cycle=identity[1],
        cycle_key=identity[2],
        delivery_ids=delivery_ids,
    )
    stored_batch_id = str(state.get("pendingBatchId") or "").strip()
    if stored_batch_id != batch_id:
        raise AutomationCycleContractError(
            "automation delivery batch identity changed"
        )
    return AutomationDeliveryBatch(
        batch_id=batch_id,
        tenant_id=identity[0],
        cycle=identity[1],
        cycle_key=identity[2],
        scheduled_at=scheduled_at,
        channel_id=channel_id,
        conversation=dict(conversation),
        deliveries=deliveries,
    )


def _validated_identity(
    value: Any,
) -> tuple[str, AutomationCycleName, str] | None:
    if value is None:
        return None
    if not isinstance(value, Mapping) or set(value) != {
        "tenantId",
        "cycle",
        "cycleKey",
    }:
        raise AutomationCycleContractError(
            "automation delivery identity is invalid"
        )
    if not all(
        isinstance(value.get(key), str)
        for key in ("tenantId", "cycle", "cycleKey")
    ):
        raise AutomationCycleContractError(
            "automation delivery identity is invalid"
        )
    tenant_id = value["tenantId"].strip()
    cycle = value["cycle"].strip()
    cycle_key = value["cycleKey"].strip()
    if (
        not _IDENTIFIER_PATTERN.fullmatch(tenant_id)
        or cycle not in _SUPPORTED_CYCLES
        or not _IDENTIFIER_PATTERN.fullmatch(cycle_key)
    ):
        raise AutomationCycleContractError(
            "automation delivery identity is invalid"
        )
    return tenant_id, cycle, cycle_key  # type: ignore[return-value]


def _restore_delivery(value: Any) -> AutomationDelivery:
    if not isinstance(value, Mapping):
        raise AutomationCycleContractError(
            "automation pending delivery is invalid"
        )
    try:
        return AutomationDelivery(
            delivery_id=str(value.get("deliveryId") or ""),
            kind=str(value.get("kind") or ""),
            payload=dict(value.get("payload") or {}),
        )
    except (TypeError, ValueError) as exc:
        raise AutomationCycleContractError(
            "automation pending delivery is invalid"
        ) from exc


def _build_batch_id(
    *,
    tenant_id: str,
    cycle: str,
    cycle_key: str,
    delivery_ids: tuple[str, ...],
) -> str:
    raw = "\0".join(
        (tenant_id, cycle, cycle_key, *sorted(delivery_ids))
    )
    return f"batch:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def _build_state_key(
    *,
    tenant_id: str,
    cycle: str,
    cycle_key: str,
) -> str:
    raw = "\0".join((tenant_id, cycle, cycle_key))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


__all__ = [
    "AutomationDeliveryAckInput",
    "AutomationDeliveryBatch",
    "AutomationDeliveryBroker",
    "AutomationDeliveryPullInput",
    "serialize_automation_delivery_ack",
    "serialize_automation_delivery_batch",
    "validate_automation_delivery_state",
]
