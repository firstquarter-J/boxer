from __future__ import annotations

from concurrent.futures import (
    FIRST_COMPLETED,
    Future,
    ThreadPoolExecutor,
    wait,
)
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import hashlib
import json
import logging
from pathlib import Path
import stat
import threading
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from requests.exceptions import ReadTimeout

from boxer_company import sms_delivery_cycle
from boxer_company.automation import (
    AutomationCycleContractError,
    AutomationCycleRequest,
    AutomationCycleResult,
    AutomationCycleService,
    AutomationDelivery,
    AutomationDeliveryReceipt,
    DeviceHealthMonitorCycleHandler,
    SmsDeliveryCycleHandler,
)
from boxer_company.device_health_monitor_cycle import (
    build_clean_device_health_monitor_cursor,
    DeviceHealthMonitorCycleDeps,
)
from boxer_company_api.automation import (
    AutomationCycleTrigger,
    AutomationCycleUncertainError,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
    validate_automation_trigger_admission,
)


_NOW = datetime(2026, 8, 10, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))


def _replace_cycle_state(
    store: JsonAutomationCycleStateStore,
    key: str,
    state: dict[str, Any],
) -> None:
    """테스트 fixture도 production의 atomic cycle mutation만 사용한다."""

    store.mutate_cycle(
        key,
        lambda _exists, _current: (state, None),
    )


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


@dataclass
class _ConcurrentWeeklyHandler:
    """외부 실행을 멈춰 다른 coordinator의 예약 경쟁을 관찰한다."""

    name: str = "weekly_recordings"
    block_run: bool = False
    block_acknowledge: bool = False
    calls: int = 0
    acknowledge_calls: int = 0
    run_started: threading.Event = field(default_factory=threading.Event)
    run_release: threading.Event = field(default_factory=threading.Event)
    acknowledge_started: threading.Event = field(
        default_factory=threading.Event
    )
    acknowledge_release: threading.Event = field(
        default_factory=threading.Event
    )
    _counter_lock: threading.Lock = field(
        default_factory=threading.Lock,
        repr=False,
    )

    def run(
        self,
        request: AutomationCycleRequest,
    ) -> AutomationCycleResult:
        del request
        with self._counter_lock:
            self.calls += 1
        if self.block_run:
            self.run_started.set()
            if not self.run_release.wait(timeout=10):
                raise RuntimeError("concurrent run test timed out")
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
        with self._counter_lock:
            self.acknowledge_calls += 1
        if self.block_acknowledge:
            self.acknowledge_started.set()
            if not self.acknowledge_release.wait(timeout=10):
                raise RuntimeError("concurrent acknowledgement test timed out")
        return {
            **dict(request.cursor),
            "acknowledgedCount": len(receipts),
        }


class _BarrierFirstLoadStateStore(JsonAutomationCycleStateStore):
    """분리 load/save 회귀 시 두 coordinator가 같은 revision을 읽게 한다."""

    def __init__(self, path: Path, barrier: threading.Barrier) -> None:
        super().__init__(path)
        self._first_load_barrier = barrier
        self._first_load_used = False

    def load(self, key: str) -> dict[str, Any]:
        state = super().load(key)
        if not self._first_load_used:
            self._first_load_used = True
            self._first_load_barrier.wait(timeout=5)
        return state


class _FinalizeGateStateStore(JsonAutomationCycleStateStore):
    """finalize CAS 직전에 peer write를 결정적으로 끼워 넣는다."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.before_finalize = threading.Event()
        self.resume_finalize = threading.Event()
        self._mutation_count = 0
        self._mutation_count_lock = threading.Lock()

    def mutate_cycle(
        self,
        key: str,
        updater: Any,
        *,
        expected_document_digest: str | None = None,
    ) -> Any:
        with self._mutation_count_lock:
            self._mutation_count += 1
            mutation_count = self._mutation_count
        if mutation_count == 2:
            self.before_finalize.set()
            if not self.resume_finalize.wait(timeout=10):
                raise RuntimeError("finalize test timed out")
        return super().mutate_cycle(
            key,
            updater,
            expected_document_digest=expected_document_digest,
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


def _state_key(trigger: AutomationCycleTrigger) -> str:
    raw = "\0".join(
        (trigger.tenant_id, trigger.cycle, trigger.cycle_key)
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _finish_concurrent_reservations(
    futures: tuple[
        Future[AutomationCycleResult],
        Future[AutomationCycleResult],
    ],
    release: threading.Event,
) -> AutomationCycleResult:
    """한 예약만 외부 실행에 들어갔고 다른 예약은 막혔는지 확인한다."""

    try:
        completed, _ = wait(
            futures,
            timeout=3,
            return_when=FIRST_COMPLETED,
        )
        assert len(completed) == 1
        with pytest.raises(AutomationCycleUncertainError):
            next(iter(completed)).result()
    finally:
        release.set()

    successful: list[AutomationCycleResult] = []
    uncertain_count = 0
    for future in futures:
        try:
            successful.append(future.result(timeout=5))
        except AutomationCycleUncertainError:
            uncertain_count += 1
    assert uncertain_count == 1
    assert len(successful) == 1
    return successful[0]


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


def test_sms_safe_sheet_read_timeout_finalizes_and_allows_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scan_attempts = 0

    def _empty_reconcile(*_args: Any, **_kwargs: Any) -> int:
        return 0

    def _load_pending() -> list[dict[str, Any]]:
        nonlocal scan_attempts
        scan_attempts += 1
        if scan_attempts == 1:
            raise ReadTimeout("private Sheets response")
        return []

    monkeypatch.setattr(
        sms_delivery_cycle,
        "_reconcile_sms_delivery_outbox_once",
        _empty_reconcile,
    )
    monkeypatch.setattr(
        sms_delivery_cycle,
        "_load_sms_delivery_outbox_items",
        lambda: [],
    )
    monkeypatch.setattr(
        sms_delivery_cycle,
        "_load_device_health_sheet_pending_sms_deliveries",
        _load_pending,
    )

    state_path = tmp_path / "sms-safe-read.json"
    state_store = JsonAutomationCycleStateStore(state_path)
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService(
            (
                SmsDeliveryCycleHandler(
                    logger=logging.getLogger("test.sms-safe-read")
                ),
            )
        ),
        state_store,
        clock=lambda: _NOW,
    )

    def _sms_trigger(request_id: str) -> AutomationCycleTrigger:
        return AutomationCycleTrigger(
            request_id=request_id,
            tenant_id="T1",
            cycle="sms_delivery",
            cycle_key="continuous",
            scheduled_at=_NOW,
        )

    first = _sms_trigger("cycle:sms-safe-read:first")
    first_result = coordinator.run(first)
    first_state = state_store.load(_state_key(first))

    # 순수 Sheet GET timeout은 no_change로 finalize돼 inFlight를 닫고
    # 같은 continuous cycle의 다음 fixed-delay 실행을 허용한다.
    assert first_result.outcome == "no_change"
    assert first_result.metrics == {"updatedCount": 0, "deliveryCount": 0}
    assert "inFlight" not in first_state
    assert first_state["lastCompletedAt"] == _NOW.isoformat()

    second = _sms_trigger("cycle:sms-safe-read:second")
    second_result = coordinator.run(second)
    second_state = state_store.load(_state_key(second))

    assert second_result.outcome == "no_change"
    assert scan_attempts == 2
    assert "inFlight" not in second_state
    assert second_state["lastRequestId"] == second.request_id


def test_concurrent_coordinator_store_instances_reserve_run_once(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "concurrent-run.json"
    handler = _ConcurrentWeeklyHandler(block_run=True)
    load_barrier = threading.Barrier(2)
    first = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        _BarrierFirstLoadStateStore(state_path, load_barrier),
        clock=lambda: _NOW,
    )
    second = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        _BarrierFirstLoadStateStore(state_path, load_barrier),
        clock=lambda: _NOW,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(first.run, _trigger("cycle:concurrent:first")),
            executor.submit(second.run, _trigger("cycle:concurrent:second")),
        )
        assert handler.run_started.wait(timeout=3)
        result = _finish_concurrent_reservations(
            futures,
            handler.run_release,
        )

    assert result.outcome == "completed"
    assert handler.calls == 1


def test_concurrent_coordinator_store_instances_call_ack_hook_once(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "concurrent-ack.json"
    handler = _ConcurrentWeeklyHandler()
    initial = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: _NOW,
    ).run(_trigger("cycle:initial"))
    receipt = AutomationDeliveryReceipt(
        delivery_id=initial.deliveries[0].delivery_id,
        status="sent",
        external_message_id="171.001",
        permalink="https://example.slack.com/archives/C1/p1",
        delivered_at=_NOW,
    )
    handler.block_acknowledge = True
    load_barrier = threading.Barrier(2)
    first = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        _BarrierFirstLoadStateStore(state_path, load_barrier),
        clock=lambda: _NOW,
    )
    second = DurableAutomationCycleCoordinator(
        AutomationCycleService((handler,)),  # type: ignore[arg-type]
        _BarrierFirstLoadStateStore(state_path, load_barrier),
        clock=lambda: _NOW,
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(
                first.run,
                _trigger(
                    "cycle:ack:concurrent:first",
                    receipts=(receipt,),
                    ack_only=True,
                ),
            ),
            executor.submit(
                second.run,
                _trigger(
                    "cycle:ack:concurrent:second",
                    receipts=(receipt,),
                    ack_only=True,
                ),
            ),
        )
        assert handler.acknowledge_started.wait(timeout=3)
        result = _finish_concurrent_reservations(
            futures,
            handler.acknowledge_release,
        )

    state = next(
        iter(json.loads(state_path.read_text(encoding="utf-8"))["cycles"].values())
    )
    assert result.outcome == "no_change"
    assert handler.acknowledge_calls == 1
    assert state["pendingDeliveries"] == []
    assert "ackInFlight" not in state


def test_run_finalize_merges_latest_revision_without_lost_update(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "finalize-cas.json"
    trigger = _trigger("cycle:finalize-cas")
    state_key = _state_key(trigger)
    gated_store = _FinalizeGateStateStore(state_path)
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((_WeeklyHandler(),)),  # type: ignore[arg-type]
        gated_store,
        clock=lambda: _NOW,
    )
    peer_store = JsonAutomationCycleStateStore(state_path)

    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(coordinator.run, trigger)
        try:
            assert gated_store.before_finalize.wait(timeout=3)

            def _write_peer_field(
                exists: bool,
                current: dict[str, Any],
            ) -> tuple[dict[str, Any], None]:
                assert exists
                assert "inFlight" in current
                return {
                    **current,
                    "concurrentAudit": {"writer": "peer"},
                }, None

            peer_store.mutate_cycle(state_key, _write_peer_field)
        finally:
            gated_store.resume_finalize.set()
        result = future.result(timeout=5)

    state = next(
        iter(json.loads(state_path.read_text(encoding="utf-8"))["cycles"].values())
    )
    assert result.outcome == "completed"
    assert state["concurrentAudit"] == {"writer": "peer"}
    assert state["lastRequestId"] == trigger.request_id
    assert "inFlight" not in state


def test_run_finalize_rejects_same_request_id_with_replaced_marker(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "marker-mismatch.json"
    trigger = _trigger("cycle:marker-mismatch")
    state_key = _state_key(trigger)
    peer_store = JsonAutomationCycleStateStore(state_path)

    @dataclass
    class _MarkerReplacingHandler:
        name: str = "weekly_recordings"

        def run(
            self,
            request: AutomationCycleRequest,
        ) -> AutomationCycleResult:
            def _replace_marker(
                exists: bool,
                current: dict[str, Any],
            ) -> tuple[dict[str, Any], None]:
                assert exists
                marker = dict(current["inFlight"])
                assert marker["requestId"] == request.request_id
                marker["startedAt"] = (
                    _NOW + timedelta(seconds=1)
                ).isoformat()
                return {**current, "inFlight": marker}, None

            peer_store.mutate_cycle(state_key, _replace_marker)
            return AutomationCycleResult(
                cycle="weekly_recordings",
                outcome="completed",
                cursor={"cycleCompleted": True},
                metrics={"recordingCount": 0},
            )

    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((_MarkerReplacingHandler(),)),  # type: ignore[arg-type]
        JsonAutomationCycleStateStore(state_path),
        clock=lambda: _NOW,
    )

    with pytest.raises(
        AutomationCycleUncertainError,
        match="state changed during execution",
    ):
        coordinator.run(trigger)

    state = next(
        iter(json.loads(state_path.read_text(encoding="utf-8"))["cycles"].values())
    )
    assert state["inFlight"]["requestId"] == trigger.request_id
    assert state["inFlight"]["startedAt"] == (
        _NOW + timedelta(seconds=1)
    ).isoformat()
    assert "lastResult" not in state


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
    cursor = build_clean_device_health_monitor_cursor(
        alert_delivery_enabled=True,
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
    _replace_cycle_state(
        store,
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
