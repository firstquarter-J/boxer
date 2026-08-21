from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import json
from pathlib import Path
import stat
from typing import Any
from unittest.mock import patch
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from boxer_company.automation import (
    AutomationCycleContractError,
    AutomationCycleRequest,
    AutomationCycleResult,
    AutomationCycleService,
    AutomationDelivery,
    AutomationDeliveryReceipt,
    DeviceHealthMonitorCycleHandler,
)
from boxer_company.device_health_monitor_cycle import (
    build_device_health_monitor_seed_cursor,
    DeviceHealthMonitorCycleDeps,
)
from boxer_company_api.automation import (
    AutomationCycleInput,
    AutomationCycleTrigger,
    AutomationCycleUncertainError,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
    serialize_automation_cycle_result,
    validate_automation_trigger_admission,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)


_NOW = datetime(2026, 8, 10, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
_TOKEN = "automation-token-" + ("x" * 40)


@dataclass
class _WeeklyHandler:
    name: str = "weekly_recordings"
    calls: int = 0
    error: Exception | None = None
    acknowledge_calls: int = 0
    acknowledge_error: Exception | None = None

    def run(
        self,
        request: AutomationCycleRequest,
    ) -> AutomationCycleResult:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return AutomationCycleResult(
            cycle="weekly_recordings",
            outcome="completed",
            cursor={"cycleCompleted": True},
            deliveries=(
                AutomationDelivery(
                    delivery_id="weekly_recordings:2026-08-03",
                    kind="weekly_recordings_report",
                    payload={"totalCount": 10},
                ),
            ),
            metrics={"recordingCount": 10},
        )

    def acknowledge(
        self,
        request: AutomationCycleRequest,
        receipts: tuple[AutomationDeliveryReceipt, ...],
    ) -> dict[str, Any]:
        self.acknowledge_calls += 1
        if self.acknowledge_error is not None:
            raise self.acknowledge_error
        return {
            **dict(request.cursor),
            "acknowledgedCount": len(receipts),
        }


@dataclass
class _DailyHandler:
    name: str = "daily_device_round"
    requests: list[AutomationCycleRequest] = field(default_factory=list)

    def run(
        self,
        request: AutomationCycleRequest,
    ) -> AutomationCycleResult:
        self.requests.append(request)
        return AutomationCycleResult(
            cycle="daily_device_round",
            outcome="no_change",
            cursor={**dict(request.cursor), "cycleCompleted": True},
            metrics={"deliveryCount": 0},
        )


def _trigger(
    request_id: str,
    *,
    receipts: tuple[AutomationDeliveryReceipt, ...] = (),
    ack_only: bool = False,
    options: dict[str, Any] | None = None,
) -> AutomationCycleTrigger:
    return AutomationCycleTrigger(
        request_id=request_id,
        tenant_id="T1",
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=_NOW,
        options=options or {},
        delivery_receipts=receipts,
        ack_only=ack_only,
    )


def _coordinator(
    state_path: Path,
    handler: _WeeklyHandler,
) -> DurableAutomationCycleCoordinator:
    return DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: _NOW,
    )


def test_coordinator_persists_delivery_before_return_and_does_not_rerun(
    tmp_path: Path,
) -> None:
    handler = _WeeklyHandler()
    state_path = tmp_path / "automation.json"
    coordinator = _coordinator(state_path, handler)

    first = coordinator.run(_trigger("cycle:first"))
    repeated_poll = coordinator.run(_trigger("cycle:second"))

    assert handler.calls == 1
    assert repeated_poll.deliveries == first.deliveries
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600


def test_delivery_ack_closes_cycle_without_replaying_sent_payload(
    tmp_path: Path,
) -> None:
    handler = _WeeklyHandler()
    coordinator = _coordinator(tmp_path / "automation.json", handler)
    first = coordinator.run(_trigger("cycle:first"))
    delivery_id = first.deliveries[0].delivery_id

    ack = coordinator.run(
        _trigger(
            "cycle:ack",
            receipts=(
                AutomationDeliveryReceipt(
                    delivery_id=delivery_id,
                    status="sent",
                    external_message_id="171.001",
                    permalink="https://example.slack.com/archives/C1/p1",
                    delivered_at=_NOW,
                ),
            ),
            ack_only=True,
        )
    )
    completed_poll = coordinator.run(_trigger("cycle:after-ack"))
    old_request_replay = coordinator.run(_trigger("cycle:first"))

    assert ack.outcome == "no_change"
    assert completed_poll.outcome == "no_change"
    assert old_request_replay.outcome == "no_change"
    assert not old_request_replay.deliveries
    assert handler.calls == 1
    assert handler.acknowledge_calls == 1


def test_delivery_ack_can_close_previous_week_after_window_rollover(
    tmp_path: Path,
) -> None:
    handler = _WeeklyHandler()
    state_path = tmp_path / "automation.json"
    first_coordinator = _coordinator(state_path, handler)
    first = first_coordinator.run(_trigger("cycle:first"))
    next_week = _NOW + timedelta(days=7)
    restarted = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: next_week,
    )

    result = restarted.run(
        AutomationCycleTrigger(
            request_id="cycle:late-ack",
            tenant_id="T1",
            cycle="weekly_recordings",
            cycle_key="weekly:2026-08-03",
            scheduled_at=next_week,
            delivery_receipts=(
                AutomationDeliveryReceipt(
                    delivery_id=first.deliveries[0].delivery_id,
                    status="sent",
                    external_message_id="171.001",
                    permalink="https://example.slack.com/archives/C1/p1",
                    delivered_at=_NOW,
                ),
            ),
            ack_only=True,
        )
    )

    assert result.outcome == "no_change"
    assert not result.deliveries
    assert handler.calls == 1
    assert handler.acknowledge_calls == 1


def test_delivery_ack_hook_failure_keeps_receipt_unapplied_and_blocks_retry(
    tmp_path: Path,
) -> None:
    handler = _WeeklyHandler(
        acknowledge_error=RuntimeError("private sheet response")
    )
    coordinator = _coordinator(tmp_path / "automation.json", handler)
    first = coordinator.run(_trigger("cycle:first"))
    receipt = AutomationDeliveryReceipt(
        delivery_id=first.deliveries[0].delivery_id,
        status="sent",
        external_message_id="171.001",
        permalink="https://example.slack.com/archives/C1/p1",
        delivered_at=_NOW,
    )

    with pytest.raises(RuntimeError):
        coordinator.run(
            _trigger(
                "cycle:ack",
                receipts=(receipt,),
                ack_only=True,
            )
        )
    with pytest.raises(AutomationCycleUncertainError):
        coordinator.run(
            _trigger(
                "cycle:ack-again",
                receipts=(receipt,),
                ack_only=True,
            )
        )

    assert handler.acknowledge_calls == 1


def test_failed_handler_leaves_durable_uncertain_guard(
    tmp_path: Path,
) -> None:
    handler = _WeeklyHandler(error=RuntimeError("secret provider response"))
    coordinator = _coordinator(tmp_path / "automation.json", handler)

    with pytest.raises(RuntimeError):
        coordinator.run(_trigger("cycle:first"))
    with pytest.raises(AutomationCycleUncertainError):
        coordinator.run(_trigger("cycle:second"))

    assert handler.calls == 1


def test_invalid_request_is_rejected_before_inflight_is_persisted(
    tmp_path: Path,
) -> None:
    handler = _WeeklyHandler()
    state_path = tmp_path / "automation.json"
    coordinator = _coordinator(state_path, handler)

    with pytest.raises(AutomationCycleContractError):
        coordinator.run(
            _trigger(
                "cycle:invalid",
                options={"apiToken": "must-not-be-stored"},
            )
        )
    valid = coordinator.run(_trigger("cycle:valid"))

    assert valid.outcome == "completed"
    assert handler.calls == 1
    assert "must-not-be-stored" not in state_path.read_text(encoding="utf-8")


def test_admission_keeps_slack_schedule_when_api_clock_differs(
    tmp_path: Path,
) -> None:
    handler = _WeeklyHandler()
    state_path = tmp_path / "automation.json"
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: _NOW + timedelta(minutes=3),
    )

    result = coordinator.run(_trigger("cycle:stale"))

    assert result.outcome == "completed"
    assert handler.calls == 1
    assert state_path.exists()


def test_coordinator_preserves_slack_scheduled_at_for_domain_execution(
    tmp_path: Path,
) -> None:
    handler = _DailyHandler()
    state_path = tmp_path / "automation.json"
    server_now = datetime(
        2026,
        8,
        10,
        23,
        0,
        tzinfo=ZoneInfo("Asia/Seoul"),
    )
    client_time = server_now + timedelta(seconds=119)
    canonical = {
        "autoUpdateAgent": False,
        "autoUpdateBoxFree": False,
        "autoUpdateBoxPaid": False,
        "autoCleanupTrashCan": False,
        "autoPowerOff": False,
    }
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: server_now,
    )

    coordinator.run(
        AutomationCycleTrigger(
            request_id="cycle:server-clock",
            tenant_id="T1",
            cycle="daily_device_round",
            cycle_key="daily:2026-08-10",
            scheduled_at=client_time,
            options=canonical,
        )
    )

    assert handler.requests[0].scheduled_at == client_time
    assert client_time != server_now


def test_daily_progress_is_cas_persisted_before_failed_run_returns(
    tmp_path: Path,
) -> None:
    @dataclass
    class _FailingProgressHandler:
        name: str = "daily_device_round"

        def run(
            self,
            request: AutomationCycleRequest,
        ) -> AutomationCycleResult:
            assert request.progress_callback is not None
            request.progress_callback(
                {
                    **dict(request.cursor),
                    "activeHospitalSeq": 22,
                    "activeHospitalName": "테스트병원",
                    "activeHospitalStartedAt": _NOW.isoformat(),
                    "activeHospitalDeviceCount": 2,
                    "activeDeviceIndex": 1,
                    "activeDeviceName": "MB2-TEST",
                    "activeDeviceUpdatedAt": _NOW.isoformat(),
                }
            )
            raise RuntimeError("synthetic device failure")

    state_path = tmp_path / "daily-progress.json"
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((_FailingProgressHandler(),)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: _NOW,
    )
    options = {
        "autoUpdateAgent": False,
        "autoUpdateBoxFree": False,
        "autoUpdateBoxPaid": False,
        "autoCleanupTrashCan": False,
        "autoPowerOff": False,
    }

    with pytest.raises(RuntimeError, match="synthetic device failure"):
        coordinator.run(
            AutomationCycleTrigger(
                request_id="cycle:daily:progress-failed",
                tenant_id="T1",
                cycle="daily_device_round",
                cycle_key="daily:2026-08-10",
                scheduled_at=_NOW,
                options=options,
            )
        )

    document = json.loads(state_path.read_text(encoding="utf-8"))
    state = next(iter(document["cycles"].values()))
    # handler 완료를 기다리지 않고 device_started 시점의 active cursor와
    # 같은 request in-flight가 한 revision에 함께 남는다.
    assert state["cursor"]["activeHospitalSeq"] == 22
    assert state["cursor"]["activeDeviceIndex"] == 1
    assert state["cursor"]["activeDeviceName"] == "MB2-TEST"
    assert state["inFlight"]["requestId"] == (
        "cycle:daily:progress-failed"
    )


@pytest.mark.parametrize(
    ("cycle", "cycle_key"),
    (
        ("device_health_monitor", "continuous:forged"),
        ("device_notification_alert", "daily:2026-08-10"),
        ("sms_delivery", "sms:2026-08-10"),
        ("weekly_recordings", "weekly:2026-08-10"),
    ),
)
def test_admission_rejects_noncanonical_cycle_keys(
    cycle: str,
    cycle_key: str,
) -> None:
    trigger = AutomationCycleTrigger(
        request_id="cycle:forged",
        tenant_id="T1",
        cycle=cycle,  # type: ignore[arg-type]
        cycle_key=cycle_key,
        scheduled_at=_NOW,
    )

    with pytest.raises(AutomationCycleContractError):
        validate_automation_trigger_admission(trigger)


def test_health_input_rejects_slack_owned_alert_delivery_option() -> None:
    with pytest.raises(ValidationError):
        AutomationCycleInput(
            tenantId="T1",
            cycle="device_health_monitor",
            cycleKey="continuous",
            scheduledAt=_NOW,
            options={"alertDeliveryEnabled": True},
        )


@pytest.mark.parametrize(
    "corruption",
    (
        "missing_seed",
        "cursor_list",
        "missing_alerts",
        "alerts_list",
        "partial_entry",
    ),
)
def test_health_seed_schema_is_validated_before_pending_delivery_replay(
    tmp_path: Path,
    corruption: str,
) -> None:
    external_calls: list[str] = []
    deps = DeviceHealthMonitorCycleDeps(
        load_devices=lambda: external_calls.append("load_devices") or [],
    )
    handler = DeviceHealthMonitorCycleHandler(deps=deps)
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    trigger = AutomationCycleTrigger(
        request_id=f"cycle:health:{corruption}",
        tenant_id="T1",
        cycle="device_health_monitor",
        cycle_key="continuous",
        scheduled_at=_NOW,
    )
    raw_key = "\0".join(("T1", "device_health_monitor", "continuous"))
    state_key = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
    cursor = build_device_health_monitor_seed_cursor(
        legacy_alert_delivery_enabled=True,
        alert_fingerprints={},
        pending_alert_fingerprints={},
        pending_decision="preserve",
        seeded_at=_NOW,
    )
    if corruption == "missing_seed":
        cursor = {}
    elif corruption == "cursor_list":
        # list-of-pairs를 dict로 바꾸면 유효 seed처럼 보일 수 있으므로 raw
        # top-level type 자체를 pending replay보다 먼저 거부한다.
        cursor = list(cursor.items())
    elif corruption == "missing_alerts":
        cursor.pop("alertFingerprints")
    elif corruption == "alerts_list":
        cursor["alertFingerprints"] = []
    else:
        cursor["alertFingerprints"] = {
            "#20 테스트병원|1진료실|MB2-C1|LED 오류": {
                "firstAlertedAt": _NOW.isoformat(),
                "count": 1,
            }
        }
    pending = {
        "deliveryId": "device_health_monitor:pending",
        "kind": "device_health_alert",
        "payload": {"safe": True},
    }
    store.save(
        state_key,
        {
            "cursor": cursor,
            "pendingDeliveries": [pending],
            "domainCycleComplete": False,
            "cycleCompleted": False,
        },
    )
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),
        store,
        clock=lambda: _NOW,
    )

    with pytest.raises(
        AutomationCycleContractError,
        match="API state seed is required",
    ):
        coordinator.run(trigger)

    assert external_calls == []
    assert store.load(state_key)["pendingDeliveries"] == [pending]


def test_daily_admission_uses_slack_window_key_and_runtime_options() -> None:
    daily_now = datetime(2026, 8, 10, 23, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    runtime_options = {
        "autoUpdateAgent": True,
        "autoUpdateBoxFree": True,
        "autoUpdateBoxPaid": True,
        "autoCleanupTrashCan": True,
        "autoPowerOff": False,
    }
    valid = AutomationCycleTrigger(
        request_id="cycle:daily:valid",
        tenant_id="T1",
        cycle="daily_device_round",
        cycle_key="daily:2026-08-10",
        scheduled_at=daily_now,
        options=runtime_options,
    )
    validate_automation_trigger_admission(valid)
    # API 서버의 별도 window 재판정 없이 Slack이 due로 확정한 같은 날짜를 받는다.
    validate_automation_trigger_admission(
        AutomationCycleTrigger(
            request_id="cycle:daily:slack-midday",
            tenant_id="T1",
            cycle="daily_device_round",
            cycle_key="daily:2026-08-10",
            scheduled_at=daily_now.replace(hour=12),
            options=runtime_options,
        )
    )

    for invalid in (
        AutomationCycleTrigger(
            request_id="cycle:daily:wrong-key",
            tenant_id="T1",
            cycle="daily_device_round",
            cycle_key="daily:2026-08-09",
            scheduled_at=daily_now,
            options=runtime_options,
        ),
        AutomationCycleTrigger(
            request_id="cycle:daily:missing-option",
            tenant_id="T1",
            cycle="daily_device_round",
            cycle_key="daily:2026-08-10",
            scheduled_at=daily_now,
            options={
                key: value
                for key, value in runtime_options.items()
                if key != "autoPowerOff"
            },
        ),
        AutomationCycleTrigger(
            request_id="cycle:daily:non-bool-option",
            tenant_id="T1",
            cycle="daily_device_round",
            cycle_key="daily:2026-08-10",
            scheduled_at=daily_now,
            options={**runtime_options, "autoPowerOff": 1},
        ),
    ):
        with pytest.raises(AutomationCycleContractError):
            validate_automation_trigger_admission(invalid)


@pytest.mark.parametrize(
    ("cycle", "cycle_key"),
    (
        ("weekly_recordings", "weekly:2026-08-03"),
        ("daily_device_round", "daily:2026-08-10"),
        ("device_health_monitor", "continuous"),
    ),
)
def test_ack_only_accepts_exact_historical_key_without_domain_admission(
    cycle: str,
    cycle_key: str,
) -> None:
    trigger = AutomationCycleTrigger(
        request_id="cycle:historical-ack",
        tenant_id="T1",
        cycle=cycle,  # type: ignore[arg-type]
        cycle_key=cycle_key,
        scheduled_at=_NOW,
        options={"autoPowerOff": True} if cycle == "daily_device_round" else {},
        ack_only=True,
    )

    validate_automation_trigger_admission(trigger)


@pytest.mark.parametrize(
    ("cycle", "cycle_key"),
    (
        ("weekly_recordings", "weekly:2026-08-04"),
        ("daily_device_round", "daily:invalid"),
        ("sms_delivery", "daily:2026-08-10"),
    ),
)
def test_ack_only_still_rejects_malformed_or_cross_cycle_key(
    cycle: str,
    cycle_key: str,
) -> None:
    trigger = AutomationCycleTrigger(
        request_id="cycle:invalid-historical-ack",
        tenant_id="T1",
        cycle=cycle,  # type: ignore[arg-type]
        cycle_key=cycle_key,
        scheduled_at=_NOW,
        ack_only=True,
    )

    with pytest.raises(AutomationCycleContractError):
        validate_automation_trigger_admission(trigger)


def test_daily_coordinator_accepts_runtime_override_and_seeds_window(
    tmp_path: Path,
) -> None:
    now = datetime(2026, 8, 11, 1, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    runtime_options = {
        "autoUpdateAgent": True,
        "autoUpdateBoxFree": True,
        "autoUpdateBoxPaid": True,
        "autoCleanupTrashCan": True,
        "autoPowerOff": False,
    }
    state_path = tmp_path / "daily-automation.json"
    handler = _DailyHandler()
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: now,
    )

    with pytest.raises(AutomationCycleContractError):
        coordinator.run(
            AutomationCycleTrigger(
                request_id="cycle:daily:forged-options",
                tenant_id="T1",
                cycle="daily_device_round",
                cycle_key="daily:2026-08-10",
                scheduled_at=now,
                options={"autoPowerOff": True},
            )
        )
    assert not state_path.exists()
    assert not handler.requests

    coordinator.run(
        AutomationCycleTrigger(
            request_id="cycle:daily:valid-options",
            tenant_id="T1",
            cycle="daily_device_round",
            cycle_key="daily:2026-08-10",
            scheduled_at=now,
            options=runtime_options,
        )
    )

    assert len(handler.requests) == 1
    assert handler.requests[0].cursor["windowKey"] == "2026-08-10"


def test_cycle_schema_rejects_credentialed_permalink() -> None:
    with pytest.raises(ValidationError):
        AutomationCycleInput.model_validate(
            {
                "tenantId": "T1",
                "cycle": "weekly_recordings",
                "cycleKey": "weekly:2026-08-03",
                "scheduledAt": _NOW.isoformat(),
                "deliveryReceipts": [
                    {
                        "deliveryId": "weekly_recordings:2026-08-03",
                        "status": "sent",
                        "permalink": "https://user:password@example.com/path",
                    }
                ],
                "ackOnly": True,
            }
        )


def test_wire_result_does_not_expose_server_cursor() -> None:
    result = AutomationCycleResult(
        cycle="weekly_recordings",
        outcome="completed",
        cursor={"internalState": "must-not-cross-wire"},
        deliveries=(),
        metrics={"deliveryCount": 0},
    )

    payload = serialize_automation_cycle_result(result, "cycle:first")

    assert "cursor" not in payload
    assert "must-not-cross-wire" not in str(payload)


def _api_settings(
    tmp_path: Path,
    *,
    capabilities: frozenset[str],
    enabled_cycles: frozenset[str] | None = None,
) -> CompanyApiSettings:
    return CompanyApiSettings(
        host="127.0.0.1",
        port=8010,
        callers=(
            CompanyApiCallerSettings(
                caller_id="slack-automation",
                token=_TOKEN,
                tenant_ids=frozenset({"T1"}),
                channels=frozenset({"slack"}),
                actor_ids=frozenset({"U1"}),
                allow_anonymous_actor=False,
                capabilities=capabilities,
            ),
        ),
        automation_state_path=str(tmp_path / "automation.json"),
        automation_enabled_cycles=(
            enabled_cycles
            if enabled_cycles is not None
            else frozenset(
                {
                    "weekly_recordings",
                    "daily_device_round",
                    "device_health_monitor",
                    "device_notification_alert",
                    "sms_delivery",
                }
            )
        ),
    )


def _cycle_payload() -> dict[str, Any]:
    return {
        "tenantId": "T1",
        "cycle": "weekly_recordings",
        "cycleKey": "weekly:2026-08-03",
        "scheduledAt": _NOW.isoformat(),
    }


def _cycle_headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_TOKEN}",
        "X-Request-ID": "automation:http:1",
    }


class _CapturingCoordinator:
    def __init__(self, *, error: Exception | None = None) -> None:
        self.error = error
        self.triggers: list[AutomationCycleTrigger] = []

    def run(self, trigger: AutomationCycleTrigger) -> AutomationCycleResult:
        self.triggers.append(trigger)
        if self.error is not None:
            raise self.error
        return AutomationCycleResult(
            cycle=trigger.cycle,
            outcome="completed",
            cursor={"serverOnly": True},
            deliveries=(
                AutomationDelivery(
                    delivery_id="weekly_recordings:2026-08-03",
                    kind="weekly_recordings_report",
                    payload={"totalCount": 12},
                ),
            ),
            metrics={"deliveryCount": 1},
        )


def test_api_cycle_uses_machine_capability_without_human_actor(
    tmp_path: Path,
) -> None:
    coordinator = _CapturingCoordinator()
    app = create_company_api_app(
        settings=_api_settings(
            tmp_path,
            capabilities=frozenset(
                {
                    "assistant.turn.read",
                    "assistant.automation.execute",
                }
            ),
        ),
        assistant_runtime=object(),  # type: ignore[arg-type]
        readiness_probe=lambda: True,
        automation_coordinator=coordinator,
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/cycles",
            headers=_cycle_headers(),
            json=_cycle_payload(),
        )

    assert response.status_code == 200
    assert response.json()["autoRetryAllowed"] is False
    assert "cursor" not in response.json()
    assert len(coordinator.triggers) == 1
    assert coordinator.triggers[0].tenant_id == "T1"


def test_api_cycle_uses_per_device_ssh_open_budget(
    tmp_path: Path,
) -> None:
    coordinator = _CapturingCoordinator()
    app = create_company_api_app(
        settings=_api_settings(
            tmp_path,
            capabilities=frozenset(
                {
                    "assistant.turn.read",
                    "assistant.automation.execute",
                }
            ),
        ),
        assistant_runtime=object(),  # type: ignore[arg-type]
        readiness_probe=lambda: True,
        automation_coordinator=coordinator,
    )

    with patch(
        "boxer_company_api.app.company_api_device_ssh_context"
    ) as ssh_context:
        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/automation/cycles",
                headers=_cycle_headers(),
                json=_cycle_payload(),
            )

    assert response.status_code == 200
    ssh_context.assert_called_once_with(per_device_open_budget=True)


def test_api_cycle_rejects_caller_without_automation_capability(
    tmp_path: Path,
) -> None:
    coordinator = _CapturingCoordinator()
    app = create_company_api_app(
        settings=_api_settings(
            tmp_path,
            capabilities=frozenset({"assistant.turn.read"}),
        ),
        assistant_runtime=object(),  # type: ignore[arg-type]
        readiness_probe=lambda: True,
        automation_coordinator=coordinator,
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/cycles",
            headers=_cycle_headers(),
            json=_cycle_payload(),
        )

    assert response.status_code == 403
    assert not coordinator.triggers


def test_api_cycle_feature_flag_blocks_capable_caller_before_coordinator(
    tmp_path: Path,
) -> None:
    coordinator = _CapturingCoordinator()
    app = create_company_api_app(
        settings=_api_settings(
            tmp_path,
            capabilities=frozenset(
                {
                    "assistant.turn.read",
                    "assistant.automation.execute",
                }
            ),
            enabled_cycles=frozenset(),
        ),
        assistant_runtime=object(),  # type: ignore[arg-type]
        readiness_probe=lambda: True,
        automation_coordinator=coordinator,
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/cycles",
            headers=_cycle_headers(),
            json=_cycle_payload(),
        )

    assert response.status_code == 503
    assert response.json()["code"] == "service_not_ready"
    assert response.json()["retryable"] is False
    assert not coordinator.triggers


def test_api_cycle_returns_non_retryable_uncertain_problem(
    tmp_path: Path,
) -> None:
    coordinator = _CapturingCoordinator(
        error=AutomationCycleUncertainError("private state")
    )
    app = create_company_api_app(
        settings=_api_settings(
            tmp_path,
            capabilities=frozenset(
                {
                    "assistant.turn.read",
                    "assistant.automation.execute",
                }
            ),
        ),
        assistant_runtime=object(),  # type: ignore[arg-type]
        readiness_probe=lambda: True,
        automation_coordinator=coordinator,
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/cycles",
            headers=_cycle_headers(),
            json=_cycle_payload(),
        )

    assert response.status_code == 409
    assert response.json()["code"] == "operation_in_progress"
    assert response.json()["retryable"] is False
    assert "private state" not in response.text
