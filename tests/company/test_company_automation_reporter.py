from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from boxer_company_adapter_slack import automation_reporter
from boxer_company_adapter_slack.automation_reporter import (
    AutomationSlackDelivery,
    build_automation_request_id,
    flush_automation_deliveries,
    remember_automation_delivery,
    remember_automation_deliveries,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAvailabilityError,
    CompanyApiContractError,
)


_NOW = datetime(2026, 8, 14, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))


class _ApiClient:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.calls: list[dict[str, object]] = []
        self.error = error

    def run(self, **kwargs: object) -> SimpleNamespace:
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return SimpleNamespace(deliveries=(), metrics={})


def _remember(path: Path) -> None:
    remember_automation_delivery(
        cycle="device_health_monitor",
        cycle_key="continuous",
        delivery=AutomationSlackDelivery(
            delivery_id="device_health_monitor:abc",
            external_message_id="1723600000.000100",
            permalink="https://lifex.slack.com/archives/C1/p1",
            delivered_at=_NOW,
        ),
        state_path=path,
    )


def test_delivery_client_msg_id_is_stable_per_exact_message_part() -> None:
    first = automation_reporter.build_automation_delivery_client_msg_id(
        cycle="daily_device_round",
        cycle_key="2026-08-14",
        delivery_id="daily:42",
        part="chunk:1",
    )
    replay = automation_reporter.build_automation_delivery_client_msg_id(
        cycle="daily_device_round",
        cycle_key="2026-08-14",
        delivery_id="daily:42",
        part="chunk:1",
    )
    next_part = automation_reporter.build_automation_delivery_client_msg_id(
        cycle="daily_device_round",
        cycle_key="2026-08-14",
        delivery_id="daily:42",
        part="chunk:2",
    )

    assert first == replay
    assert first != next_part
    assert len(first) == 36


def test_delivery_state_contains_receipt_only_and_is_private(
    tmp_path: Path,
) -> None:
    path = tmp_path / "delivery.json"
    _remember(path)

    payload = json.loads(path.read_text(encoding="utf-8"))
    serialized = json.dumps(payload)
    assert "payload" not in serialized.lower()
    assert "phone" not in serialized.lower()
    assert "messageText" not in serialized
    assert oct(os.stat(path).st_mode & 0o777) == "0o600"


def test_flush_sends_receipt_once_then_clears_state(tmp_path: Path) -> None:
    path = tmp_path / "delivery.json"
    _remember(path)
    client = _ApiClient()

    flushed = flush_automation_deliveries(
        client,  # type: ignore[arg-type]
        cycle="device_health_monitor",
        cycle_key="continuous",
        scheduled_at=_NOW,
        state_path=path,
    )

    assert flushed is True
    assert len(client.calls) == 1
    call = client.calls[0]
    assert call["ack_only"] is True
    assert len(call["receipts"]) == 1  # type: ignore[arg-type]
    assert (
        call["receipts"][0].delivery_id  # type: ignore[index,union-attr]
        == "device_health_monitor:abc"
    )
    assert flush_automation_deliveries(
        client,  # type: ignore[arg-type]
        cycle="device_health_monitor",
        cycle_key="continuous",
        scheduled_at=_NOW,
        state_path=path,
    ) is False
    assert len(client.calls) == 1


def test_aggregated_slack_receipts_are_stored_and_flushed_together(
    tmp_path: Path,
) -> None:
    path = tmp_path / "delivery.json"
    deliveries = tuple(
        AutomationSlackDelivery(
            delivery_id=f"device_health_monitor:{suffix}",
            external_message_id="1723600000.000300",
            permalink="https://lifex.slack.com/archives/C1/p3",
            delivered_at=_NOW,
        )
        for suffix in ("abc", "def")
    )

    # 한 Slack 집계 메시지에 속한 domain receipt를 하나의
    # replace로 남겨 일부만 ack되는 상태를 만들지 않는다.
    remember_automation_deliveries(
        cycle="device_health_monitor",
        cycle_key="continuous",
        deliveries=deliveries,
        state_path=path,
    )
    stored = json.loads(path.read_text(encoding="utf-8"))
    assert len(
        stored["cycles"]["device_health_monitor"]["receipts"]
    ) == 2

    client = _ApiClient()
    assert flush_automation_deliveries(
        client,  # type: ignore[arg-type]
        cycle="device_health_monitor",
        cycle_key="continuous",
        scheduled_at=_NOW,
        state_path=path,
    )
    assert {
        receipt.delivery_id
        for receipt in client.calls[0]["receipts"]  # type: ignore[union-attr]
    } == {
        "device_health_monitor:abc",
        "device_health_monitor:def",
    }


def test_flush_failure_preserves_receipt_without_local_fallback(
    tmp_path: Path,
) -> None:
    path = tmp_path / "delivery.json"
    _remember(path)
    client = _ApiClient(
        error=CompanyApiAvailabilityError("unavailable")
    )

    with pytest.raises(CompanyApiAvailabilityError):
        flush_automation_deliveries(
            client,  # type: ignore[arg-type]
            cycle="device_health_monitor",
            cycle_key="continuous",
            scheduled_at=_NOW,
            state_path=path,
        )

    assert json.loads(path.read_text(encoding="utf-8"))["cycles"][
        "device_health_monitor"
    ]["receipts"]


def test_pending_receipt_cannot_be_replaced_by_another_cycle_key(
    tmp_path: Path,
) -> None:
    path = tmp_path / "delivery.json"
    _remember(path)

    with pytest.raises(CompanyApiContractError):
        remember_automation_delivery(
            cycle="device_health_monitor",
            cycle_key="other",
            delivery=AutomationSlackDelivery(
                delivery_id="device_health_monitor:def",
                external_message_id="1723600000.000200",
                permalink="",
                delivered_at=_NOW,
            ),
            state_path=path,
        )


def test_request_id_is_stable_and_contains_no_cycle_key() -> None:
    first = build_automation_request_id(
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=_NOW,
    )
    second = build_automation_request_id(
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=_NOW,
    )

    assert first == second
    assert "2026-08-03" not in first
