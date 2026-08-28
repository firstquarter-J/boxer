from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from boxer_company.automation_schedule import (
    AutomationScheduleConfig,
    next_fixed_delay_due_at,
    plan_automation_cycle,
)


_KST = ZoneInfo("Asia/Seoul")


def _kst(
    year: int,
    month: int,
    day: int,
    hour: int,
    minute: int = 0,
) -> datetime:
    return datetime(year, month, day, hour, minute, tzinfo=_KST)


def test_weekly_is_due_at_monday_boundary_for_previous_monday() -> None:
    config = AutomationScheduleConfig(
        weekly_hour=9,
        weekly_minute=0,
    )

    before = plan_automation_cycle(
        "weekly_recordings",
        now=_kst(2026, 8, 24, 8, 59),
        config=config,
    )
    boundary = plan_automation_cycle(
        "weekly_recordings",
        # timezone-aware UTC 입력도 config timezone으로 정규화한다.
        now=datetime(2026, 8, 24, 0, 0, tzinfo=UTC),
        config=config,
    )

    assert before.due is False
    assert before.cycle_key == "weekly:2026-08-17"
    assert boundary.due is True
    assert boundary.cycle_key == "weekly:2026-08-17"
    assert boundary.scheduled_at == _kst(2026, 8, 24, 9, 0)


def test_weekly_does_not_catch_up_on_tuesday() -> None:
    config = AutomationScheduleConfig(weekly_hour=9, weekly_minute=0)

    tuesday = plan_automation_cycle(
        "weekly_recordings",
        now=_kst(2026, 8, 25, 9, 0),
        config=config,
    )

    # 월요일 전체를 놓친 경우 과거 identity를 화요일에 소급 실행하지 않는다.
    assert tuesday.due is False
    assert tuesday.cycle_key is None


def test_weekly_completed_identity_waits_for_next_monday() -> None:
    decision = plan_automation_cycle(
        "weekly_recordings",
        now=_kst(2026, 8, 24, 9, 1),
        config=AutomationScheduleConfig(),
        completed_cycle_key="weekly:2026-08-17",
    )

    assert decision.due is False
    assert decision.cycle_key == "weekly:2026-08-17"


@pytest.mark.parametrize(
    ("now", "expected_key"),
    (
        (_kst(2026, 8, 24, 22, 0), "daily:2026-08-24"),
        (_kst(2026, 8, 25, 0, 0), "daily:2026-08-24"),
        (_kst(2026, 8, 25, 5, 59), "daily:2026-08-24"),
    ),
)
def test_daily_overnight_window_is_start_inclusive_and_crosses_midnight(
    now: datetime,
    expected_key: str,
) -> None:
    decision = plan_automation_cycle(
        "daily_device_round",
        now=now,
        config=AutomationScheduleConfig(
            daily_start_hour=22,
            daily_end_hour=6,
        ),
    )

    assert decision.due is True
    assert decision.cycle_key == expected_key


def test_daily_overnight_window_end_is_exclusive() -> None:
    decision = plan_automation_cycle(
        "daily_device_round",
        now=_kst(2026, 8, 25, 6, 0),
        config=AutomationScheduleConfig(
            daily_start_hour=22,
            daily_end_hour=6,
        ),
    )

    assert decision.due is False
    assert decision.cycle_key is None


def test_daily_outside_window_pending_ack_is_not_a_planner_run() -> None:
    decision = plan_automation_cycle(
        "daily_device_round",
        now=_kst(2026, 8, 25, 6, 1),
        config=AutomationScheduleConfig(
            daily_start_hour=22,
            daily_end_hour=6,
        ),
    )

    # `daily:2026-08-24` pending ACK는 transport가 exact key로 계속 처리한다.
    # planner는 종료된 window의 새 domain run identity를 다시 내지 않는다.
    assert decision.due is False
    assert decision.cycle_key is None


def test_daily_completed_window_identity_waits_for_next_window() -> None:
    decision = plan_automation_cycle(
        "daily_device_round",
        now=_kst(2026, 8, 25, 0, 0),
        config=AutomationScheduleConfig(
            daily_start_hour=22,
            daily_end_hour=6,
        ),
        completed_cycle_key="daily:2026-08-24",
    )

    assert decision.due is False
    assert decision.cycle_key == "daily:2026-08-24"


def test_daily_equal_start_and_end_is_a_24_hour_calendar_window() -> None:
    config = AutomationScheduleConfig(
        daily_start_hour=22,
        daily_end_hour=22,
    )

    before_midnight = plan_automation_cycle(
        "daily_device_round",
        now=_kst(2026, 8, 24, 23, 59),
        config=config,
    )
    midnight = plan_automation_cycle(
        "daily_device_round",
        now=_kst(2026, 8, 25, 0, 0),
        config=config,
    )

    assert before_midnight.due is True
    assert before_midnight.cycle_key == "daily:2026-08-24"
    assert midnight.due is True
    assert midnight.cycle_key == "daily:2026-08-25"


@pytest.mark.parametrize(
    ("cycle", "interval"),
    (
        ("device_health_monitor", timedelta(seconds=61)),
        ("device_notification_alert", timedelta(seconds=31)),
        ("sms_delivery", timedelta(seconds=11)),
    ),
)
def test_continuous_cycles_respect_fixed_delay_boundary(
    cycle: str,
    interval: timedelta,
) -> None:
    config = AutomationScheduleConfig(
        device_health_monitor_interval=timedelta(seconds=61),
        device_notification_alert_interval=timedelta(seconds=31),
        sms_delivery_interval=timedelta(seconds=11),
    )
    completed_at = _kst(2026, 8, 24, 12, 0)

    before = plan_automation_cycle(
        cycle,
        now=completed_at + interval - timedelta(microseconds=1),
        config=config,
        last_completed_at=completed_at,
    )
    boundary = plan_automation_cycle(
        cycle,
        now=completed_at + interval,
        config=config,
        last_completed_at=completed_at,
    )

    assert before.due is False
    assert before.cycle_key == "continuous"
    assert boundary.due is True
    assert boundary.scheduled_at == completed_at + interval


def test_continuous_first_run_is_immediately_eligible() -> None:
    now = _kst(2026, 8, 24, 12, 0)

    decision = plan_automation_cycle(
        "device_health_monitor",
        now=now,
        config=AutomationScheduleConfig(),
    )

    assert decision.due is True
    assert decision.cycle_key == "continuous"
    assert decision.scheduled_at == now


def test_next_fixed_delay_anchors_at_actual_completion_without_catch_up() -> None:
    old_completion = _kst(2026, 8, 1, 0, 0)
    restarted_at = _kst(2026, 8, 24, 12, 0)
    interval = timedelta(minutes=1)

    overdue = plan_automation_cycle(
        "device_health_monitor",
        now=restarted_at,
        config=AutomationScheduleConfig(
            device_health_monitor_interval=interval,
        ),
        last_completed_at=old_completion,
    )
    next_due = next_fixed_delay_due_at(restarted_at, interval)
    after_one_run = plan_automation_cycle(
        "device_health_monitor",
        now=restarted_at + timedelta(seconds=1),
        config=AutomationScheduleConfig(
            device_health_monitor_interval=interval,
        ),
        last_completed_at=restarted_at,
    )

    # 놓친 interval 수와 무관하게 재기동 시 한 번만 실행하고 실제 완료부터 쉰다.
    assert overdue.due is True
    assert overdue.scheduled_at == restarted_at
    assert next_due == _kst(2026, 8, 24, 12, 1)
    assert after_one_run.due is False


def test_schedule_rejects_naive_now_and_invalid_config() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        plan_automation_cycle(
            "sms_delivery",
            now=datetime(2026, 8, 24, 12, 0),
            config=AutomationScheduleConfig(),
        )
    with pytest.raises(ValueError, match="weekly_hour"):
        AutomationScheduleConfig(weekly_hour=24)
    with pytest.raises(ValueError, match="positive timedelta"):
        AutomationScheduleConfig(
            sms_delivery_interval=timedelta(0),
        )
