from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from boxer_company.automation import (
    AutomationCycleRequest,
    AutomationCycleResult,
    AutomationCycleService,
    AutomationDelivery,
    AutomationDeliveryReceipt,
)
from boxer_company_api.automation import (
    AutomationCycleContractError,
    AutomationCycleTrigger,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
)
from boxer_company_api.automation_delivery import (
    AutomationDeliveryBroker,
    validate_automation_delivery_state,
)


_NOW = datetime(2026, 8, 27, 14, 0, tzinfo=ZoneInfo("Asia/Seoul"))


def _replace_cycle_state(
    store: JsonAutomationCycleStateStore,
    key: str,
    state: dict[str, Any],
) -> None:
    """테스트 state 주입도 production의 단일-cycle atomic mutation을 쓴다."""

    store.mutate_cycle(
        key,
        lambda _exists, _current: (state, None),
    )


@dataclass
class _NotificationHandler:
    name: str = "device_notification_alert"
    runs: int = 0
    acknowledgements: int = 0

    def validate(self, request: AutomationCycleRequest) -> None:
        assert request.options == {}

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult:
        self.runs += 1
        return AutomationCycleResult(
            cycle=self.name,
            outcome="completed",
            cursor={
                **dict(request.cursor),
                "cycleCompleted": False,
            },
            deliveries=(
                AutomationDelivery(
                    delivery_id="device_notification_alert:event:1",
                    kind="device_notification_alert",
                    payload={"alert": {"device": "MB2-TEST"}},
                ),
            ),
            metrics={"deliveryCount": 1},
        )

    def acknowledge(
        self,
        request: AutomationCycleRequest,
        receipts: tuple[AutomationDeliveryReceipt, ...],
    ) -> dict[str, Any]:
        self.acknowledgements += 1
        return dict(request.cursor)


def _prepared_broker(
    tmp_path: Path,
) -> tuple[
    AutomationDeliveryBroker,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
    _NotificationHandler,
]:
    handler = _NotificationHandler()
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        store,
        clock=lambda: _NOW,
    )
    trigger = AutomationCycleTrigger(
        request_id="scheduler:notification:1",
        tenant_id="T1",
        cycle="device_notification_alert",
        cycle_key="continuous",
        scheduled_at=_NOW,
        delivery_target={
            "channelId": "C123456",
            "conversation": {"mode": "root"},
        },
    )
    # scheduler trigger의 목적지가 pending 생성과 같은 atomic finalize에
    # 저장돼 broker가 별도 채널 설정을 읽지 않게 한다.
    coordinator.run(trigger)
    return (
        AutomationDeliveryBroker(store, coordinator, clock=lambda: _NOW),
        coordinator,
        store,
        handler,
    )


def test_pull_keeps_pending_and_returns_stable_server_target(
    tmp_path: Path,
) -> None:
    broker, _coordinator, store, handler = _prepared_broker(tmp_path)

    # scheduler가 만든 current pending은 startup preflight를 통과한다.
    validate_automation_delivery_state(store)

    first = broker.pull(tenant_id="T1")
    repeated = broker.pull(tenant_id="T1")

    assert first is not None
    assert repeated is not None
    assert first.batch_id == repeated.batch_id
    assert first.channel_id == "C123456"
    assert first.deliveries[0].delivery_id == (
        "device_notification_alert:event:1"
    )
    assert handler.runs == 1


def test_exact_ack_runs_hook_once_and_duplicate_is_noop(tmp_path: Path) -> None:
    broker, _coordinator, _store, handler = _prepared_broker(tmp_path)
    batch = broker.pull(tenant_id="T1")
    assert batch is not None
    receipt = AutomationDeliveryReceipt(
        delivery_id=batch.deliveries[0].delivery_id,
        status="sent",
        external_message_id="1723600000.001",
        delivered_at=_NOW,
    )

    first = broker.acknowledge(
        request_id="transport:ack:1",
        tenant_id="T1",
        batch_id=batch.batch_id,
        receipts=(receipt,),
    )
    duplicate = broker.acknowledge(
        request_id="transport:ack:2",
        tenant_id="T1",
        batch_id=batch.batch_id,
        receipts=(receipt,),
    )

    assert first.outcome == "no_change"
    assert duplicate.metrics["duplicateAck"] is True
    assert broker.pull(tenant_id="T1") is None
    assert handler.acknowledgements == 1


def test_partial_or_unknown_ack_is_rejected(tmp_path: Path) -> None:
    broker, _coordinator, _store, _handler = _prepared_broker(tmp_path)
    batch = broker.pull(tenant_id="T1")
    assert batch is not None

    with pytest.raises(AutomationCycleContractError):
        broker.acknowledge(
            request_id="transport:ack:bad",
            tenant_id="T1",
            batch_id=batch.batch_id,
            receipts=(
                AutomationDeliveryReceipt(
                    delivery_id="device_notification_alert:other",
                    status="sent",
                    delivered_at=_NOW,
                ),
            ),
        )


def test_clean_state_passes_delivery_preflight(tmp_path: Path) -> None:
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")

    validate_automation_delivery_state(store)


def test_legacy_pending_without_transport_metadata_fails_closed(
    tmp_path: Path,
) -> None:
    handler = _NotificationHandler()
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        store,
        clock=lambda: _NOW,
    )
    coordinator.run(
        AutomationCycleTrigger(
            request_id="legacy:notification:1",
            tenant_id="T1",
            cycle="device_notification_alert",
            cycle_key="continuous",
            scheduled_at=_NOW,
        )
    )
    broker = AutomationDeliveryBroker(store, coordinator, clock=lambda: _NOW)
    state_path = tmp_path / "automation.json"
    state_before = state_path.read_bytes()

    with pytest.raises(AutomationCycleContractError):
        validate_automation_delivery_state(store)
    with pytest.raises(AutomationCycleContractError):
        broker.pull(tenant_id="T1")
    # 구 pending을 빈 state로 덮거나 metadata를 추측해 채우지 않는다.
    assert state_path.read_bytes() == state_before


def test_missing_or_changed_pending_batch_id_fails_preflight(
    tmp_path: Path,
) -> None:
    _broker, _coordinator, store, _handler = _prepared_broker(tmp_path)
    with store.locked_snapshot() as snapshot:
        state_key, state = next(iter(snapshot.document["cycles"].items()))

    for stored_batch_id in (None, "batch:" + "0" * 64):
        invalid_state = dict(state)
        if stored_batch_id is None:
            invalid_state.pop("pendingBatchId", None)
        else:
            invalid_state["pendingBatchId"] = stored_batch_id
        _replace_cycle_state(store, state_key, invalid_state)

        with pytest.raises(AutomationCycleContractError):
            validate_automation_delivery_state(store)


def test_pending_identity_and_target_metadata_are_exact(
    tmp_path: Path,
) -> None:
    _broker, _coordinator, store, _handler = _prepared_broker(tmp_path)
    with store.locked_snapshot() as snapshot:
        state_key, current = next(iter(snapshot.document["cycles"].items()))

    invalid_states: list[dict[str, Any]] = []
    identity_extra = deepcopy(current)
    identity_extra["identity"]["unexpected"] = True
    invalid_states.append(identity_extra)
    target_extra = deepcopy(current)
    target_extra["deliveryTarget"]["unexpected"] = True
    invalid_states.append(target_extra)
    null_conversation = deepcopy(current)
    null_conversation["deliveryTarget"]["conversation"] = None
    invalid_states.append(null_conversation)
    non_string_identity = deepcopy(current)
    non_string_identity["identity"]["tenantId"] = 1
    invalid_states.append(non_string_identity)

    for invalid_state in invalid_states:
        _replace_cycle_state(store, state_key, invalid_state)
        with pytest.raises(AutomationCycleContractError):
            validate_automation_delivery_state(store)


def test_feature_off_reconstruction_still_drains_existing_pending(
    tmp_path: Path,
) -> None:
    _enabled_broker, coordinator, store, handler = _prepared_broker(tmp_path)
    # reporter flag가 꺼져 신규 실행 worker가 사라진 재시작을 흉내 낸다.
    # transport broker는 supported cycle 전체의 기존 state만 보고 drain한다.
    drain_broker = AutomationDeliveryBroker(
        store,
        coordinator,
        clock=lambda: _NOW,
    )
    batch = drain_broker.pull(
        tenant_id="T1",
        cycle="device_notification_alert",
    )
    assert batch is not None

    result = drain_broker.acknowledge(
        request_id="transport:off-drain:1",
        tenant_id="T1",
        batch_id=batch.batch_id,
        receipts=(
            AutomationDeliveryReceipt(
                delivery_id=batch.deliveries[0].delivery_id,
                status="sent",
                delivered_at=_NOW,
            ),
        ),
    )

    assert result.outcome == "no_change"
    assert drain_broker.pull(tenant_id="T1") is None
    assert handler.runs == 1
    assert handler.acknowledgements == 1
