from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
from pathlib import Path
import threading
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from boxer_company.automation_schedule import AutomationScheduleConfig
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
