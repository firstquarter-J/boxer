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


def test_scheduler_repeats_completed_sweep_and_carries_unfinished_round_next_night(
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
    # 마지막 보고서 ACK 뒤 같은 야간 창에서 다음 회차를 시작한다.
    assert tick("2026-09-18") == ("daily_device_round",)
    assert results[-1].cursor["roundNumber"] == 2
    assert results[-1].cursor["processedHospitalSeqs"] == [1]
    rounds.ack("2026-09-18", results[-1])
    assert tick("2026-09-19", hour=6) == ()
    tick("2026-09-19")
    assert rounds.visited == [1, 2, 84, 1, 2]
    assert results[-1].cursor["processedHospitalSeqs"] == [1, 2]
    assert results[-1].cursor["roundNumber"] == 1
    assert results[-1].deliveries[0].delivery_id == "daily_device_round:2026-09-19:2"


def test_repeated_sweeps_require_each_exact_ack_and_keep_distinct_deliveries(
    rounds: _Rounds,
) -> None:
    rounds.candidates = [1]
    day = "2026-09-18"
    deliveries = []
    for number in range(1, 5):
        result = rounds.run(day)
        deliveries.append(result.deliveries[0].delivery_id)
        assert result.cursor["roundNumber"] == number
        assert result.cursor["cycleCompleted"] is True
        # 완료한 병원 보고서가 대기 중이면 재시작해도 장비를 다시 실행하지 않는다.
        assert rounds.run(day).deliveries == result.deliveries
        if number > 1:
            rounds.coordinator().run(replace(
                _trigger(day), ack_only=True,
                delivery_receipts=(AutomationDeliveryReceipt(
                    delivery_id=deliveries[-2], status="sent",
                ),),
            ))
            assert rounds.run(day).deliveries == result.deliveries
        assert rounds.visited == [1] * number
        rounds.ack(day, result)
    assert deliveries == [
        "daily_device_round:2026-09-18:1",
        "daily_device_round:2026-09-18:1:round:2",
        "daily_device_round:2026-09-18:1:round:3",
        "daily_device_round:2026-09-18:1:round:4",
    ]
    assert rounds.store.load(_key(day))["acknowledgedDeliveryIds"] == deliveries


def test_legacy_completed_state_resumes_same_night_with_new_delivery_identity(
    rounds: _Rounds,
) -> None:
    day = "2026-09-18"
    old_id = "daily_device_round:2026-09-18:1"
    rounds.seed(day, [1, 2, 84], cycleCompleted=True,
                domainCycleComplete=True, acknowledgedDeliveryIds=[old_id])
    result = rounds.run(day)
    assert rounds.visited == [1]
    assert result.cursor["roundNumber"] == 2
    assert result.cursor["processedHospitalSeqs"] == [1]
    assert result.deliveries[0].delivery_id == old_id + ":round:2"
    assert rounds.store.load(_key(day))["acknowledgedDeliveryIds"] == [old_id]


@pytest.mark.parametrize("number", [None, True, 0, -1, "2", 10**100])
@pytest.mark.parametrize("completed", [False, True])
def test_invalid_round_number_cannot_start_device_work(
    rounds: _Rounds, number: Any, completed: bool,
) -> None:
    day = "2026-09-18"
    rounds.seed(day, [], cycleCompleted=completed,
                cursor={"windowKey": day, "roundNumber": number})
    before = rounds.path.read_bytes()
    with pytest.raises(AutomationCycleContractError):
        rounds.run(day)
    assert rounds.visited == []
    assert rounds.path.read_bytes() == before


def test_round_identity_fits_transport_limit_before_device_work(rounds: _Rounds) -> None:
    day = "2026-09-18"
    rounds.candidates = [18446744073709551615]
    maximum = DailyDeviceRoundCycleHandler._MAX_ROUND_NUMBER
    rounds.seed(day, [], cursor={"windowKey": day, "roundNumber": maximum})
    result = rounds.run(day)
    assert len(result.deliveries[0].delivery_id) == 128
    rounds.ack(day, result)
    before = rounds.path.read_bytes()
    # ID에 더 이상 담을 수 없는 훼손 상태는 새 장비 작업 전에 차단한다.
    with pytest.raises(AutomationCycleContractError):
        rounds.run(day)
    assert rounds.visited == [18446744073709551615]
    assert rounds.path.read_bytes() == before


def test_new_round_reservation_survives_failure_before_first_progress(
    rounds: _Rounds, monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = "2026-09-18"
    rounds.seed(day, [1, 2, 84], cycleCompleted=True, domainCycleComplete=True)

    def fail_candidates(**_: Any) -> list[Any]:
        raise RuntimeError("candidate lookup unavailable")

    # 외부 조회 전에 실패해도 새 회차와 불명 실행 표식을 함께 보존한다.
    monkeypatch.setattr(rounder, "_load_daily_device_round_hospital_candidates", fail_candidates)
    with pytest.raises(RuntimeError, match="candidate lookup unavailable"):
        rounds.run(day)
    state = rounds.store.load(_key(day))
    assert state["cursor"] == {
        "roundNumber": 2, "processedHospitalSeqs": [], "windowKey": day,
    }
    assert state["cycleCompleted"] is False
    assert state["domainCycleComplete"] is False
    assert state["inFlight"]["requestId"] == _trigger(day).request_id
    before = rounds.path.read_bytes()
    with pytest.raises(AutomationCycleUncertainError):
        rounds.run(day)
    assert rounds.path.read_bytes() == before


def test_empty_scope_can_discover_hospitals_on_later_poll(rounds: _Rounds) -> None:
    day = "2026-09-18"
    rounds.candidates = []
    empty = rounds.run(day)
    assert not empty.deliveries
    assert empty.cursor["cycleCompleted"] is True
    rounds.candidates = [1]
    # 빈 후보를 본 요청의 재전송은 유지하고 다음 예약에서 대상을 다시 찾는다.
    assert rounds.run(day) == empty
    later = rounds.coordinator().run(replace(_trigger(day), request_id="next-poll"))
    assert rounds.visited == [1]
    assert later.cursor["roundNumber"] == 2


def test_concurrent_restart_reserves_next_round_only_once(
    rounds: _Rounds, monkeypatch: pytest.MonkeyPatch,
) -> None:
    day = "2026-09-18"
    rounds.seed(day, [1, 2, 84], cycleCompleted=True, domainCycleComplete=True)
    entered, release = threading.Event(), threading.Event()

    def blocked_devices(seq: int) -> list[Any]:
        rounds.visited.append(seq)
        entered.set()
        assert release.wait(5)
        return []

    # 재시작한 두 coordinator가 동시에 다음 회차를 열어도 한 번만 예약한다.
    monkeypatch.setattr(rounder, "_load_daily_device_round_devices", blocked_devices)
    with ThreadPoolExecutor(max_workers=1) as pool:
        first = pool.submit(rounds.run, day)
        try:
            assert entered.wait(5)
            before = rounds.path.read_bytes()
            with pytest.raises(AutomationCycleUncertainError):
                rounds.run(day)
            assert rounds.path.read_bytes() == before
            assert rounds.store.load(_key(day))["cursor"]["roundNumber"] == 2
        finally:
            release.set()
        result = first.result(timeout=5)
    assert rounds.visited == [1]
    assert result.cursor["roundNumber"] == 2


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
