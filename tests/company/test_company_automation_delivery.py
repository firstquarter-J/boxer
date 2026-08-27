from __future__ import annotations

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
from boxer_company_api.automation_delivery import AutomationDeliveryBroker


_NOW = datetime(2026, 8, 27, 14, 0, tzinfo=ZoneInfo("Asia/Seoul"))


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
    broker, _coordinator, _store, handler = _prepared_broker(tmp_path)

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


def test_old_pending_without_server_target_is_never_pulled(
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

    assert broker.pull(tenant_id="T1") is None
