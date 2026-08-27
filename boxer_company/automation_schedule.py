from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, time, timedelta, tzinfo
from typing import Literal, cast
from zoneinfo import ZoneInfo


AutomationScheduleCycle = Literal[
    "weekly_recordings",
    "daily_device_round",
    "device_health_monitor",
    "device_notification_alert",
    "sms_delivery",
]
AutomationScheduleCadence = Literal[
    "weekly",
    "daily_window",
    "fixed_delay",
]

_ALL_CYCLES = frozenset(
    {
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    }
)
_CONTINUOUS_CYCLES = frozenset(
    {
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    }
)


@dataclass(frozen=True, slots=True)
class AutomationScheduleConfig:
    """환경변수와 분리된 API-owned automation 일정 설정이다."""

    timezone: tzinfo = field(
        default_factory=lambda: ZoneInfo("Asia/Seoul")
    )
    weekly_hour: int = 9
    weekly_minute: int = 0
    daily_start_hour: int = 22
    daily_start_minute: int = 0
    daily_end_hour: int = 6
    daily_end_minute: int = 0
    device_health_monitor_interval: timedelta = timedelta(seconds=60)
    device_notification_alert_interval: timedelta = timedelta(seconds=30)
    sms_delivery_interval: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        _validate_timezone(self.timezone)
        for field_name in (
            "weekly_hour",
            "daily_start_hour",
            "daily_end_hour",
        ):
            _validate_clock_component(
                field_name,
                getattr(self, field_name),
                maximum=23,
            )
        for field_name in (
            "weekly_minute",
            "daily_start_minute",
            "daily_end_minute",
        ):
            _validate_clock_component(
                field_name,
                getattr(self, field_name),
                maximum=59,
            )
        for field_name in (
            "device_health_monitor_interval",
            "device_notification_alert_interval",
            "sms_delivery_interval",
        ):
            _validate_fixed_delay(
                getattr(self, field_name),
                field_name=field_name,
            )

    def fixed_delay_for(
        self,
        cycle: AutomationScheduleCycle,
    ) -> timedelta:
        """continuous cycle의 주입된 fixed-delay를 반환한다."""

        if cycle == "device_health_monitor":
            return self.device_health_monitor_interval
        if cycle == "device_notification_alert":
            return self.device_notification_alert_interval
        if cycle == "sms_delivery":
            return self.sms_delivery_interval
        raise ValueError("cycle does not use a fixed-delay schedule")


@dataclass(frozen=True, slots=True)
class AutomationScheduleDecision:
    """한 시점에 domain run이 가능한지와 그 durable identity를 나타낸다."""

    cycle: AutomationScheduleCycle
    cadence: AutomationScheduleCadence
    due: bool
    cycle_key: str | None
    scheduled_at: datetime | None
    eligible_at: datetime | None
    next_due_at: datetime | None
    window_end_at: datetime | None = None
    fixed_delay: timedelta | None = None


def plan_automation_cycle(
    cycle: AutomationScheduleCycle | str,
    *,
    now: datetime,
    config: AutomationScheduleConfig,
    completed_cycle_key: str | None = None,
    last_completed_at: datetime | None = None,
) -> AutomationScheduleDecision:
    """현재 시각에서 cycle 하나의 실행 가능 여부를 순수 계산한다.

    weekly/daily의 이미 끝난 identity는 ``completed_cycle_key``로, continuous
    cycle의 마지막 논리 완료 시각은 ``last_completed_at``으로 주입한다.
    pending delivery와 in-flight marker는 durable coordinator가 별도로 막는다.
    """

    _validate_aware_datetime(now, field_name="now")
    if cycle not in _ALL_CYCLES:
        raise ValueError("unsupported automation schedule cycle")
    normalized_cycle = cast(AutomationScheduleCycle, cycle)
    local_now = now.astimezone(config.timezone)

    if normalized_cycle == "weekly_recordings":
        return _plan_weekly(
            local_now,
            config,
            completed_cycle_key=completed_cycle_key,
        )
    if normalized_cycle == "daily_device_round":
        return _plan_daily(
            local_now,
            config,
            completed_cycle_key=completed_cycle_key,
        )
    return _plan_continuous(
        normalized_cycle,
        local_now,
        config,
        last_completed_at=last_completed_at,
    )


def next_fixed_delay_due_at(
    completed_at: datetime,
    fixed_delay: timedelta,
) -> datetime:
    """실제 완료 시각부터 한 번만 delay를 더해 다음 due를 정한다.

    이전의 놓친 slot을 반복해서 더하지 않기 때문에 재기동 뒤 catch-up
    storm이 생기지 않는다. UTC에서 elapsed time을 더한 뒤 입력 timezone으로
    돌려 DST가 있는 주입 timezone에서도 fixed-delay 의미를 유지한다.
    """

    _validate_aware_datetime(completed_at, field_name="completed_at")
    _validate_fixed_delay(fixed_delay, field_name="fixed_delay")
    assert completed_at.tzinfo is not None
    return (
        completed_at.astimezone(UTC) + fixed_delay
    ).astimezone(completed_at.tzinfo)


def _plan_weekly(
    local_now: datetime,
    config: AutomationScheduleConfig,
    *,
    completed_cycle_key: str | None,
) -> AutomationScheduleDecision:
    local_date = local_now.date()
    if local_now.weekday() != 0:
        days_until_monday = 7 - local_now.weekday()
        next_eligible = _local_datetime(
            local_date + timedelta(days=days_until_monday),
            config.weekly_hour,
            config.weekly_minute,
            config.timezone,
        )
        # 기존 의미대로 월요일 전체를 놓쳤으면 화요일에 소급 실행하지 않는다.
        return AutomationScheduleDecision(
            cycle="weekly_recordings",
            cadence="weekly",
            due=False,
            cycle_key=None,
            scheduled_at=None,
            eligible_at=next_eligible,
            next_due_at=next_eligible,
        )

    eligible_at = _local_datetime(
        local_date,
        config.weekly_hour,
        config.weekly_minute,
        config.timezone,
    )
    target_week_start = local_date - timedelta(days=7)
    cycle_key = f"weekly:{target_week_start.isoformat()}"
    completed = completed_cycle_key == cycle_key
    eligible = _at_or_after(local_now, eligible_at)
    due = eligible and not completed
    if completed:
        next_due_at = eligible_at + timedelta(days=7)
    elif eligible:
        next_due_at = None
    else:
        next_due_at = eligible_at
    return AutomationScheduleDecision(
        cycle="weekly_recordings",
        cadence="weekly",
        due=due,
        cycle_key=cycle_key,
        scheduled_at=local_now if due else None,
        eligible_at=eligible_at,
        next_due_at=next_due_at,
    )


def _plan_daily(
    local_now: datetime,
    config: AutomationScheduleConfig,
    *,
    completed_cycle_key: str | None,
) -> AutomationScheduleDecision:
    window = _current_daily_window(local_now, config)
    if window is None:
        next_eligible = _next_daily_window_start(local_now, config)
        # 종료된 window의 pending ACK는 transport가 과거 exact cycleKey로
        # 처리한다. planner의 no-due는 새 domain run만 금지하는 의미다.
        return AutomationScheduleDecision(
            cycle="daily_device_round",
            cadence="daily_window",
            due=False,
            cycle_key=None,
            scheduled_at=None,
            eligible_at=next_eligible,
            next_due_at=next_eligible,
        )

    window_date, window_start, window_end = window
    cycle_key = f"daily:{window_date.isoformat()}"
    completed = completed_cycle_key == cycle_key
    due = not completed
    return AutomationScheduleDecision(
        cycle="daily_device_round",
        cadence="daily_window",
        due=due,
        cycle_key=cycle_key,
        scheduled_at=local_now if due else None,
        eligible_at=window_start,
        next_due_at=(
            _next_daily_window_start(local_now, config)
            if completed
            else None
        ),
        window_end_at=window_end,
    )


def _plan_continuous(
    cycle: AutomationScheduleCycle,
    local_now: datetime,
    config: AutomationScheduleConfig,
    *,
    last_completed_at: datetime | None,
) -> AutomationScheduleDecision:
    if cycle not in _CONTINUOUS_CYCLES:
        raise ValueError("cycle does not use a continuous schedule")
    fixed_delay = config.fixed_delay_for(cycle)
    if last_completed_at is None:
        eligible_at = local_now
    else:
        eligible_at = next_fixed_delay_due_at(
            last_completed_at,
            fixed_delay,
        ).astimezone(config.timezone)
    due = _at_or_after(local_now, eligible_at)
    return AutomationScheduleDecision(
        cycle=cycle,
        cadence="fixed_delay",
        due=due,
        cycle_key="continuous",
        scheduled_at=local_now if due else None,
        eligible_at=eligible_at,
        next_due_at=None if due else eligible_at,
        fixed_delay=fixed_delay,
    )


def _current_daily_window(
    local_now: datetime,
    config: AutomationScheduleConfig,
) -> tuple[date, datetime, datetime] | None:
    local_date = local_now.date()
    start_minutes = (
        config.daily_start_hour * 60 + config.daily_start_minute
    )
    end_minutes = config.daily_end_hour * 60 + config.daily_end_minute
    current_minutes = local_now.hour * 60 + local_now.minute

    if start_minutes == end_minutes:
        # 기존 24h 모드는 설정 시각이 아니라 local calendar date를 identity로
        # 사용한다. 따라서 새 identity 경계는 매일 자정이다.
        window_start = _local_datetime(
            local_date,
            0,
            0,
            config.timezone,
        )
        return (
            local_date,
            window_start,
            window_start + timedelta(days=1),
        )

    if start_minutes < end_minutes:
        if not start_minutes <= current_minutes < end_minutes:
            return None
        return (
            local_date,
            _daily_boundary(local_date, config, start=True),
            _daily_boundary(local_date, config, start=False),
        )

    if current_minutes >= start_minutes:
        window_date = local_date
        return (
            window_date,
            _daily_boundary(window_date, config, start=True),
            _daily_boundary(
                window_date + timedelta(days=1),
                config,
                start=False,
            ),
        )
    if current_minutes < end_minutes:
        window_date = local_date - timedelta(days=1)
        return (
            window_date,
            _daily_boundary(window_date, config, start=True),
            _daily_boundary(local_date, config, start=False),
        )
    return None


def _next_daily_window_start(
    local_now: datetime,
    config: AutomationScheduleConfig,
) -> datetime:
    local_date = local_now.date()
    start_minutes = (
        config.daily_start_hour * 60 + config.daily_start_minute
    )
    end_minutes = config.daily_end_hour * 60 + config.daily_end_minute
    if start_minutes == end_minutes:
        return _local_datetime(
            local_date + timedelta(days=1),
            0,
            0,
            config.timezone,
        )

    candidate = _daily_boundary(local_date, config, start=True)
    if local_now < candidate:
        return candidate
    return _daily_boundary(
        local_date + timedelta(days=1),
        config,
        start=True,
    )


def _daily_boundary(
    boundary_date: date,
    config: AutomationScheduleConfig,
    *,
    start: bool,
) -> datetime:
    return _local_datetime(
        boundary_date,
        config.daily_start_hour if start else config.daily_end_hour,
        config.daily_start_minute if start else config.daily_end_minute,
        config.timezone,
    )


def _local_datetime(
    local_date: date,
    hour: int,
    minute: int,
    local_timezone: tzinfo,
) -> datetime:
    return datetime.combine(
        local_date,
        time(hour=hour, minute=minute),
        tzinfo=local_timezone,
    )


def _at_or_after(value: datetime, boundary: datetime) -> bool:
    return value.astimezone(UTC) >= boundary.astimezone(UTC)


def _validate_aware_datetime(value: datetime, *, field_name: str) -> None:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{field_name} must be timezone-aware")


def _validate_timezone(value: tzinfo) -> None:
    if not isinstance(value, tzinfo):
        raise ValueError("timezone must be a tzinfo instance")
    try:
        offset = datetime(2026, 1, 1, tzinfo=value).utcoffset()
    except Exception as exc:
        raise ValueError("timezone must provide an UTC offset") from exc
    if offset is None:
        raise ValueError("timezone must provide an UTC offset")


def _validate_clock_component(
    field_name: str,
    value: int,
    *,
    maximum: int,
) -> None:
    if type(value) is not int or not 0 <= value <= maximum:
        raise ValueError(f"{field_name} is outside its clock range")


def _validate_fixed_delay(
    value: timedelta,
    *,
    field_name: str,
) -> None:
    if not isinstance(value, timedelta) or value <= timedelta(0):
        raise ValueError(f"{field_name} must be a positive timedelta")


__all__ = [
    "AutomationScheduleCadence",
    "AutomationScheduleConfig",
    "AutomationScheduleCycle",
    "AutomationScheduleDecision",
    "next_fixed_delay_due_at",
    "plan_automation_cycle",
]
