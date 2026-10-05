from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from boxer_company.automation_schedule import AutomationScheduleConfig
from boxer_company.protected_json import create_protected_json_file
from boxer_company_api.automation import JsonAutomationCycleStateStore
from boxer_company_api.automation_scheduler import (
    AutomationDeliveryTarget,
    AutomationScheduler,
    AutomationSchedulerSettings,
    ScheduledAutomationRun,
    load_automation_scheduler_settings,
)

_KST = ZoneInfo("Asia/Seoul")
_TENANT = "T1"
_DAILY_OPTIONS = {
    "autoUpdateAgent": False,
    "autoUpdateBoxFree": False,
    "autoUpdateBoxPaid": False,
    "autoCleanupTrashCan": False,
    "autoPowerOff": False,
}


def _settings(
    tmp_path: Path,
    *cycles: str,
    schedule: AutomationScheduleConfig | None = None,
) -> AutomationSchedulerSettings:
    # 운영은 CLI로 생성한 파일만 사용한다. daily 설정이 사라진 파일을
    # 환경 기본값으로 되돌리지 않도록 테스트에도 초기 revision을 준비한다.
    state_path = tmp_path / "automation.json"
    if not state_path.exists():
        create_protected_json_file(
            state_path, {"version": 1, "cycles": {}}, label="automation",
        )
    return AutomationSchedulerSettings(
        tenant_id=_TENANT,
        state_path=str(tmp_path / "automation.json"),
        enabled_cycles=cycles,  # type: ignore[arg-type]
        schedule=schedule or AutomationScheduleConfig(),
        delivery_targets={
            cycle: AutomationDeliveryTarget("C123456")
            for cycle in cycles
            if cycle != "sms_delivery"
        },  # type: ignore[arg-type]
        daily_options=_DAILY_OPTIONS,
    )


def _state_key(cycle: str, cycle_key: str) -> str:
    return hashlib.sha256(
        "\0".join((_TENANT, cycle, cycle_key)).encode()
    ).hexdigest()


def test_weekly_scheduler_runs_due_identity_once(tmp_path: Path) -> None:
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    runs: list[ScheduledAutomationRun] = []

    def run_cycle(run: ScheduledAutomationRun) -> None:
        runs.append(run)
        state_key = _state_key(run.cycle, run.cycle_key)

        def complete(
            _exists: bool,
            state: dict[str, Any],
        ) -> tuple[dict[str, Any], None]:
            return {
                **state,
                "cycleCompleted": True,
                "lastCompletedAt": run.scheduled_at.isoformat(),
            }, None

        store.mutate_cycle(state_key, complete)

    scheduler = AutomationScheduler(
        _settings(tmp_path, "weekly_recordings"),
        store,
        run_cycle,
    )
    now = datetime(2026, 8, 24, 9, 0, tzinfo=_KST)

    first = scheduler.run_once(now=now)
    repeated = scheduler.run_once(now=now + timedelta(minutes=1))

    assert first.attempted == ("weekly_recordings",)
    assert repeated.attempted == ()
    assert len(runs) == 1
    assert runs[0].cycle_key == "weekly:2026-08-17"
    assert runs[0].delivery_target == AutomationDeliveryTarget("C123456")


def test_scheduler_does_not_run_weekly_catchup_on_tuesday(
    tmp_path: Path,
) -> None:
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    runs: list[ScheduledAutomationRun] = []
    scheduler = AutomationScheduler(
        _settings(tmp_path, "weekly_recordings"),
        store,
        runs.append,
    )

    tick = scheduler.run_once(
        now=datetime(2026, 8, 25, 9, 0, tzinfo=_KST)
    )

    assert tick.attempted == ()
    assert runs == []


def test_weekly_query_retry_survives_restart_and_stops_after_delivery(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pymysql.err import OperationalError

    from boxer_company import automation
    from boxer_company.automation import (
        AutomationCycleService,
        AutomationDeliveryReceipt,
        WeeklyRecordingsCycleHandler,
    )
    from boxer_company_api.automation import (
        AutomationCycleTrigger,
        DurableAutomationCycleCoordinator,
    )

    # 실제 handler/coordinator/scheduler를 연결해 실패 완료와 발송 ACK를 구분한다.
    now = datetime(2026, 10, 5, 9, 0, tzinfo=_KST)
    calls = 0

    def summary(**_kwargs: Any) -> dict[str, Any]:
        nonlocal calls, now
        calls += 1
        if calls == 1:
            now += timedelta(seconds=25)
            raise OperationalError(3024, "private-db-timeout")
        return {"weekStartDate": "2026-09-28", "weekEndDate": "2026-10-04", "totalCount": 3}

    monkeypatch.setattr(automation, "_build_weekly_recordings_report_summary", summary)
    settings = _settings(tmp_path, "weekly_recordings")
    store = JsonAutomationCycleStateStore(settings.state_path)
    coordinator = DurableAutomationCycleCoordinator(
        AutomationCycleService((WeeklyRecordingsCycleHandler(),)), store, clock=lambda: now,
    )
    triggers = []

    def run_cycle(run: ScheduledAutomationRun) -> None:
        trigger = AutomationCycleTrigger(
            request_id=run.request_id, tenant_id=run.tenant_id, cycle=run.cycle,
            cycle_key=run.cycle_key, scheduled_at=run.scheduled_at,
            delivery_target={"channelId": "C123456", "conversation": {}},
        )
        triggers.append(trigger)
        coordinator.run(trigger)

    scheduler = AutomationScheduler(settings, store, run_cycle)
    scheduler.run_once(now=now)
    key = _state_key("weekly_recordings", "weekly:2026-09-28")
    failed_state = store.load(key)
    assert calls == 1
    assert "inFlight" not in failed_state
    assert failed_state["cycleCompleted"] is False
    assert failed_state["pendingDeliveries"] == []
    assert failed_state["cursor"]["queryRetryable"] is True
    assert failed_state["lastCompletedAt"] == now.isoformat()

    # 재시작 후에도 시작 시각이 아닌 실패 완료 시각부터 정확히 5분을 기다린다.
    scheduler = AutomationScheduler(settings, JsonAutomationCycleStateStore(settings.state_path), run_cycle)
    assert scheduler.run_once(now=now + timedelta(minutes=5, microseconds=-1)).attempted == ()
    assert calls == 1
    now += timedelta(minutes=5)
    assert scheduler.run_once(now=now).attempted == ("weekly_recordings",)
    state = store.load(key)
    assert calls == 2
    assert triggers[0].request_id != triggers[1].request_id
    assert "queryRetryable" not in state["cursor"]
    assert "inFlight" not in state
    assert len(state["pendingDeliveries"]) == 1
    assert state["cycleCompleted"] is False
    assert scheduler.run_once(now=now + timedelta(minutes=6)).attempted == ()

    coordinator.run(AutomationCycleTrigger(
        request_id="weekly:ack", tenant_id=_TENANT, cycle="weekly_recordings",
        cycle_key="weekly:2026-09-28", scheduled_at=now, ack_only=True,
        delivery_receipts=(AutomationDeliveryReceipt(
            delivery_id="weekly_recordings:2026-09-28", status="sent", delivered_at=now,
        ),),
    ))
    assert store.load(key)["cycleCompleted"] is True
    assert scheduler.run_once(now=now + timedelta(minutes=7)).attempted == ()
    assert calls == 2


@pytest.mark.parametrize("marker", ["inFlight", "ackInFlight", "pendingDeliveries"])
def test_weekly_retry_never_clears_uncertain_or_pending_state(tmp_path: Path, marker: str) -> None:
    settings = _settings(tmp_path, "weekly_recordings")
    store = JsonAutomationCycleStateStore(settings.state_path)
    state = {
        "cursor": {"queryRetryable": True},
        "lastCompletedAt": "2026-10-05T09:00:00+09:00",
        marker: [{"deliveryId": "existing"}] if marker == "pendingDeliveries" else {"requestId": "existing"},
    }
    key = _state_key("weekly_recordings", "weekly:2026-09-28")
    store.mutate_cycle(key, lambda *_: (state, None))
    runs = []
    scheduler = AutomationScheduler(settings, store, runs.append)
    assert scheduler.run_once(now=datetime(2026, 10, 5, 10, 0, tzinfo=_KST)).attempted == ()
    assert runs == []
    assert store.load(key) == state


@pytest.mark.parametrize("completed_at", [None, "invalid", "2026-10-05T09:00:00"])
def test_weekly_retry_requires_valid_durable_completion_time(tmp_path: Path, completed_at: str | None) -> None:
    settings = _settings(tmp_path, "weekly_recordings")
    store = JsonAutomationCycleStateStore(settings.state_path)
    key = _state_key("weekly_recordings", "weekly:2026-09-28")
    state = {"cursor": {"queryRetryable": True}, "lastCompletedAt": completed_at}
    store.mutate_cycle(key, lambda *_: (state, None))
    runs = []
    scheduler = AutomationScheduler(settings, store, runs.append)
    with pytest.raises(ValueError):
        scheduler.run_once(now=datetime(2026, 10, 5, 10, 0, tzinfo=_KST))
    assert runs == []
    assert store.load(key) == state


def test_daily_scheduler_uses_server_options_and_window_identity(
    tmp_path: Path,
) -> None:
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    runs: list[ScheduledAutomationRun] = []
    settings = _settings(
        tmp_path,
        "daily_device_round",
        schedule=AutomationScheduleConfig(
            daily_start_hour=22,
            daily_end_hour=6,
        ),
    )
    scheduler = AutomationScheduler(settings, store, runs.append)

    tick = scheduler.run_once(
        now=datetime(2026, 8, 25, 0, 1, tzinfo=_KST)
    )

    assert tick.attempted == ("daily_device_round",)
    assert runs[0].cycle_key == "daily:2026-08-24"
    assert dict(runs[0].options) == _DAILY_OPTIONS


def test_pending_or_uncertain_state_blocks_domain_rerun(tmp_path: Path) -> None:
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    runs: list[ScheduledAutomationRun] = []
    state_key = _state_key("device_notification_alert", "continuous")

    def seed(
        _exists: bool,
        _state: dict[str, Any],
    ) -> tuple[dict[str, Any], None]:
        return {
            "pendingDeliveries": [
                {
                    "deliveryId": "device_notification_alert:event:1",
                    "kind": "device_notification_alert",
                    "payload": {},
                }
            ]
        }, None

    store.mutate_cycle(state_key, seed)
    scheduler = AutomationScheduler(
        _settings(tmp_path, "device_notification_alert"),
        store,
        runs.append,
    )

    tick = scheduler.run_once(
        now=datetime(2026, 8, 24, 12, 0, tzinfo=_KST)
    )

    assert tick.attempted == ()
    assert runs == []


def test_continuous_schedule_anchors_next_run_at_last_completion(
    tmp_path: Path,
) -> None:
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    runs: list[ScheduledAutomationRun] = []
    state_key = _state_key("sms_delivery", "continuous")
    completed_at = datetime(2026, 8, 24, 12, 0, tzinfo=_KST)

    def seed(
        _exists: bool,
        _state: dict[str, Any],
    ) -> tuple[dict[str, Any], None]:
        return {
            "lastCompletedAt": completed_at.isoformat(),
            "cycleCompleted": False,
        }, None

    store.mutate_cycle(state_key, seed)
    scheduler = AutomationScheduler(
        _settings(
            tmp_path,
            "sms_delivery",
            schedule=AutomationScheduleConfig(
                sms_delivery_interval=timedelta(seconds=30)
            ),
        ),
        store,
        runs.append,
    )

    before = scheduler.run_once(
        now=completed_at + timedelta(seconds=29)
    )
    boundary = scheduler.run_once(
        now=completed_at + timedelta(seconds=30)
    )

    assert before.attempted == ()
    assert boundary.attempted == ("sms_delivery",)
    assert runs[0].delivery_target is None


def test_forever_uses_independent_workers_for_long_running_cycles(
    tmp_path: Path,
) -> None:
    store = JsonAutomationCycleStateStore(tmp_path / "automation.json")
    daily_started = threading.Event()
    release_daily = threading.Event()
    health_started = threading.Event()
    stop = threading.Event()
    now = datetime(2026, 8, 24, 22, 0, tzinfo=_KST)

    def run_cycle(run: ScheduledAutomationRun) -> None:
        if run.cycle == "daily_device_round":
            daily_started.set()
            assert release_daily.wait(timeout=2)
            return
        health_started.set()
        stop.set()

    scheduler = AutomationScheduler(
        _settings(
            tmp_path,
            "daily_device_round",
            "device_health_monitor",
            schedule=AutomationScheduleConfig(
                daily_start_hour=22,
                daily_end_hour=6,
            ),
        ),
        store,
        run_cycle,
        clock=lambda: now,
    )
    runner = threading.Thread(target=scheduler.run_forever, args=(stop,))
    runner.start()
    try:
        assert daily_started.wait(timeout=1)
        # 일일 순회가 반환되기 전에도 health worker가 독립적으로 실행돼야 한다.
        assert health_started.wait(timeout=1)
    finally:
        release_daily.set()
        stop.set()
        runner.join(timeout=2)
    assert not runner.is_alive()


def test_settings_require_targets_only_for_slack_delivery_cycles() -> None:
    settings = load_automation_scheduler_settings(
        {
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true",
            "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
            "BOXER_COMPANY_API_AUTOMATION_STATE_PATH": "/tmp/state.json",
            "DEVICE_HEALTH_MONITOR_ENABLED": "true",
            "DEVICE_HEALTH_MONITOR_CHANNEL_ID": "C123456",
            "SMS_DELIVERY_REPORTER_ENABLED": "true",
            "SMS_DELIVERY_REPORTER_POLL_INTERVAL_SEC": "17",
        }
    )

    assert settings.enabled_cycles == (
        "device_health_monitor",
        "sms_delivery",
    )
    assert set(settings.delivery_targets) == {"device_health_monitor"}
    assert settings.schedule.sms_delivery_interval == timedelta(seconds=17)

    legacy_alias = load_automation_scheduler_settings(
        {
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true",
            "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
            "BOXER_COMPANY_API_AUTOMATION_STATE_PATH": "/tmp/state.json",
            "SMS_DELIVERY_REPORTER_ENABLED": "true",
            "SOLAPI_DELIVERY_REPORT_POLL_INTERVAL_SEC": "19",
        }
    )
    assert legacy_alias.schedule.sms_delivery_interval == timedelta(seconds=19)

    new_key_wins = load_automation_scheduler_settings(
        {
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true",
            "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
            "BOXER_COMPANY_API_AUTOMATION_STATE_PATH": "/tmp/state.json",
            "SMS_DELIVERY_REPORTER_ENABLED": "true",
            "SMS_DELIVERY_REPORTER_POLL_INTERVAL_SEC": "23",
            "SOLAPI_DELIVERY_REPORT_POLL_INTERVAL_SEC": "29",
        }
    )
    assert new_key_wins.schedule.sms_delivery_interval == timedelta(seconds=23)


@pytest.mark.parametrize(
    "env_patch",
    (
        {"DEVICE_HEALTH_MONITOR_ENABLED": "sometimes"},
        {
            "DEVICE_HEALTH_MONITOR_ENABLED": "true",
            "DEVICE_HEALTH_MONITOR_CHANNEL_ID": "",
        },
        {"BOXER_COMPANY_API_AUTOMATION_SCHEDULER_POLL_INTERVAL_SEC": "0"},
    ),
)
def test_settings_fail_closed_on_invalid_scheduler_configuration(
    env_patch: dict[str, str],
) -> None:
    env = {
        "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true",
        "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
        "BOXER_COMPANY_API_AUTOMATION_STATE_PATH": "/tmp/state.json",
        **env_patch,
    }

    with pytest.raises(ValueError):
        load_automation_scheduler_settings(env)
