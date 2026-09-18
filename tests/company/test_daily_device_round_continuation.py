from __future__ import annotations

import hashlib
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from boxer_company import daily_device_round as rounder
from boxer_company.automation import (
    AutomationCycleContractError,
    AutomationCycleResult,
    AutomationCycleService,
    AutomationDeliveryReceipt,
    DailyDeviceRoundCycleHandler,
)
from boxer_company.automation_schedule import AutomationScheduleConfig
from boxer_company.protected_json import create_protected_json_file
from boxer_company_api.automation import (
    AutomationCycleTrigger,
    AutomationCycleUncertainError,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
)
from boxer_company_api.automation_scheduler import (
    AutomationDeliveryTarget,
    AutomationScheduler,
    AutomationSchedulerSettings,
    ScheduledAutomationRun,
)

_OPTIONS = {
    "autoUpdateAgent": True,
    "autoUpdateBoxFree": False,
    "autoUpdateBoxPaid": False,
    "autoCleanupTrashCan": False,
    "autoPowerOff": False,
}


def _key(day: str, tenant: str = "T1") -> str:
    return hashlib.sha256(
        f"{tenant}\0daily_device_round\0daily:{day}".encode()
    ).hexdigest()


def _trigger(day: str) -> AutomationCycleTrigger:
    return AutomationCycleTrigger(
        request_id=f"daily-test:{day}",
        tenant_id="T1",
        cycle="daily_device_round",
        cycle_key=f"daily:{day}",
        scheduled_at=datetime.fromisoformat(day + "T22:00:00").replace(
            tzinfo=ZoneInfo("Asia/Seoul")
        ),
        options=_OPTIONS,
        delivery_target={"channelId": "C123456", "conversation": {}},
    )


class _Rounds:
    def __init__(self, path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        self.path = path
        self.store = JsonAutomationCycleStateStore(path)
        create_protected_json_file(
            path, {"version": 1, "cycles": {}}, label="automation"
        )
        self.candidates = [1, 2, 84]
        self.visited: list[int] = []
        # DB/장비 경계만 대체하고 실제 선정·cursor·scheduler·ACK를 연결한다.
        monkeypatch.setattr(
            rounder,
            "_load_daily_device_round_hospital_candidates",
            lambda **_: [
                {"hospitalSeq": seq, "hospitalName": f"병원 {seq}", "deviceCount": 0}
                for seq in self.candidates
            ],
        )
        monkeypatch.setattr(rounder, "_load_daily_device_round_devices", self.devices)

    def devices(self, seq: int) -> list[Any]:
        self.visited.append(seq)
        return []

    def coordinator(self) -> DurableAutomationCycleCoordinator:
        # 매 호출마다 새 store/coordinator를 만들어 재시작 후에도 파일만으로 이어간다.
        return DurableAutomationCycleCoordinator(
            AutomationCycleService((DailyDeviceRoundCycleHandler(),)),
            JsonAutomationCycleStateStore(self.path),
        )

    def run(self, day: str) -> AutomationCycleResult:
        return self.coordinator().run(_trigger(day))

    def ack(self, day: str, result: AutomationCycleResult) -> None:
        self.coordinator().run(
            replace(
                _trigger(day),
                ack_only=True,
                request_id=f"ack-test:{day}",
                delivery_receipts=tuple(
                    AutomationDeliveryReceipt(
                        delivery_id=item.delivery_id, status="sent"
                    )
                    for item in result.deliveries
                ),
            )
        )

    def seed(self, day: str, processed: list[int], **extra: Any) -> dict[str, Any]:
        state = {
            "identity": {
                "tenantId": "T1",
                "cycle": "daily_device_round",
                "cycleKey": f"daily:{day}",
            },
            "deliveryTarget": {"channelId": "C123456", "conversation": {}},
            "cursor": {
                "windowKey": day,
                "processedHospitalSeqs": processed,
                "lastHospitalSeq": processed[-1] if processed else None,
                "nextHospitalSeq": 2,
            },
            "cycleCompleted": False,
            **extra,
        }
        self.store.mutate_cycle(_key(day), lambda *_: (state, None))
        return state


@pytest.fixture
def rounds(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> _Rounds:
    return _Rounds(tmp_path / "automation.json", monkeypatch)


def test_scheduler_continues_next_night_and_resets_only_after_full_sweep(
    rounds: _Rounds,
) -> None:
    results: list[AutomationCycleResult] = []

    def run_cycle(run: ScheduledAutomationRun) -> None:
        results.append(
            rounds.coordinator().run(
                AutomationCycleTrigger(
                    request_id=run.request_id,
                    tenant_id=run.tenant_id,
                    cycle=run.cycle,
                    cycle_key=run.cycle_key,
                    scheduled_at=run.scheduled_at,
                    options=run.options,
                    delivery_target={"channelId": "C123456", "conversation": {}},
                )
            )
        )

    def tick(day: str, hour: int = 22) -> tuple[str, ...]:
        settings = AutomationSchedulerSettings(
            tenant_id="T1",
            state_path=str(rounds.path),
            enabled_cycles=("daily_device_round",),
            schedule=AutomationScheduleConfig(),
            delivery_targets={
                "daily_device_round": AutomationDeliveryTarget("C123456")
            },
            daily_options=_OPTIONS,
        )
        return (
            AutomationScheduler(settings, rounds.store, run_cycle)
            .run_once(now=_trigger(day).scheduled_at.replace(hour=hour))
            .attempted
        )

    tick("2026-09-17")
    rounds.ack("2026-09-17", results[-1])
    old_state = rounds.store.load(_key("2026-09-17"))
    assert tick("2026-09-18", hour=6) == ()
    tick("2026-09-18")
    assert results[-1].cursor["processedHospitalSeqs"] == [1, 2]
    assert results[-1].deliveries[0].delivery_id == "daily_device_round:2026-09-18:2"
    assert rounds.store.load(_key("2026-09-17")) == old_state
    rounds.ack("2026-09-18", results[-1])
    tick("2026-09-18")
    rounds.ack("2026-09-18", results[-1])
    assert results[-1].cursor["cycleCompleted"] is True
    assert tick("2026-09-18") == ()
    tick("2026-09-19")
    assert rounds.visited == [1, 2, 84, 1]
    assert results[-1].cursor["processedHospitalSeqs"] == [1]


def test_legacy_latest_cursor_is_carried_across_missed_days_and_month_order_change(
    rounds: _Rounds,
) -> None:
    rounds.seed("2026-08-29", [1, 2])
    previous = rounds.seed("2026-08-31", [1])
    rounds.candidates = [84, 2, 1]
    result = rounds.run("2026-09-03")
    assert rounds.visited == [2]
    assert result.cursor["processedHospitalSeqs"] == [1, 2]
    assert result.cursor["windowKey"] == "2026-09-03"
    assert rounds.store.load(_key("2026-08-31")) == previous


def test_removed_hospitals_do_not_make_sweep_complete_early(rounds: _Rounds) -> None:
    rounds.seed("2026-09-17", [10, 11, 1])
    result = rounds.run("2026-09-18")
    assert rounds.visited == [2]
    assert result.cursor["cycleCompleted"] is False
    rounds.ack("2026-09-18", result)
    last = rounds.run("2026-09-18")
    assert rounds.visited == [2, 84]
    assert last.cursor["cycleCompleted"] is True


def test_removed_next_hospital_falls_back_to_remaining_candidate(
    rounds: _Rounds,
) -> None:
    rounds.seed("2026-09-17", [1])
    rounds.candidates = [1, 84]
    result = rounds.run("2026-09-18")
    assert rounds.visited == [84]
    assert result.cursor["cycleCompleted"] is True


def test_pending_old_report_blocks_new_date_until_exact_ack(rounds: _Rounds) -> None:
    result = rounds.run("2026-09-17")
    before = rounds.path.read_bytes()
    blocked = rounds.run("2026-09-18")
    assert blocked.outcome == "no_change"
    assert rounds.visited == [1]
    assert rounds.path.read_bytes() == before
    rounds.ack("2026-09-17", result)
    next_result = rounds.run("2026-09-18")
    assert rounds.visited == [1, 2]
    assert next_result.deliveries[0].delivery_id.endswith(":2026-09-18:2")


@pytest.mark.parametrize("marker", ["inFlight", "ackInFlight"])
@pytest.mark.parametrize("value", [{"requestId": "uncertain-old-run"}, {}, None])
def test_any_unresolved_older_execution_blocks_rollover(
    rounds: _Rounds,
    marker: str,
    value: Any,
) -> None:
    rounds.seed("2026-09-15", [1], **{marker: value})
    rounds.seed("2026-09-17", [1, 2])
    before = rounds.path.read_bytes()
    with pytest.raises(AutomationCycleUncertainError):
        rounds.run("2026-09-18")
    assert rounds.path.read_bytes() == before
    assert rounds.visited == []


@pytest.mark.parametrize("cursor", [None, {}, {"processedHospitalSeqs": [True]}])
def test_malformed_previous_cursor_is_not_silently_reset(
    rounds: _Rounds, cursor: Any
) -> None:
    rounds.seed("2026-09-17", [1], cursor=cursor)
    before = rounds.path.read_bytes()
    with pytest.raises(AutomationCycleContractError):
        rounds.run("2026-09-18")
    assert rounds.path.read_bytes() == before
    assert rounds.visited == []


def test_other_tenant_state_never_supplies_progress_or_blocks_execution(
    rounds: _Rounds,
) -> None:
    rounds.store.mutate_cycle(
        _key("2026-09-17", "T2"),
        lambda *_: (
            {
                "identity": {
                    "tenantId": "T2",
                    "cycle": "daily_device_round",
                    "cycleKey": "daily:2026-09-17",
                },
                "inFlight": {},
                "cursor": {"processedHospitalSeqs": [1, 2]},
            },
            None,
        ),
    )
    rounds.run("2026-09-18")
    assert rounds.visited == [1]


def test_operator_resolved_first_hospital_keeps_active_position(
    rounds: _Rounds,
) -> None:
    # 첫 progress checkpoint에는 아직 완료 병원 목록이 없을 수 있다.
    # 이 재개는 불명 marker가 운영자에 의해 해제된 뒤에만 가능하다.
    rounds.seed(
        "2026-09-17",
        [],
        cursor={
            "windowKey": "2026-09-17",
            "activeHospitalSeq": 84,
        },
    )
    result = rounds.run("2026-09-18")
    assert rounds.visited == [84]
    assert result.cursor["processedHospitalSeqs"] == [84]


def test_old_window_cannot_execute_after_next_window_started(rounds: _Rounds) -> None:
    rounds.seed("2026-09-17", [1])
    rounds.run("2026-09-18")
    before = rounds.path.read_bytes()
    with pytest.raises(AutomationCycleContractError):
        rounds.run("2026-09-17")
    assert rounds.path.read_bytes() == before
    assert rounds.visited == [2]


def test_rollover_cannot_race_an_in_progress_previous_night(
    rounds: _Rounds,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = threading.Event(), threading.Event()

    def blocked_devices(seq: int) -> list[Any]:
        rounds.visited.append(seq)
        entered.set()
        assert release.wait(5)
        return []

    monkeypatch.setattr(rounder, "_load_daily_device_round_devices", blocked_devices)
    with ThreadPoolExecutor(max_workers=1) as pool:
        earlier = pool.submit(rounds.run, "2026-09-17")
        try:
            assert entered.wait(5)
            with pytest.raises(AutomationCycleUncertainError):
                rounds.run("2026-09-18")
            assert rounds.store.load(_key("2026-09-18")) == {}
        finally:
            release.set()
        result = earlier.result(timeout=5)
    rounds.ack("2026-09-17", result)
    rounds.run("2026-09-18")
    assert rounds.visited == [1, 2]
