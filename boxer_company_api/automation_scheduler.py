from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import logging
import os
import re
import threading
from typing import Any, Callable, Mapping

from boxer_company.automation import (
    AutomationCycleName,
    build_default_automation_cycle_service,
)
from boxer_company.automation_schedule import (
    AutomationScheduleConfig,
    AutomationScheduleCycle,
    plan_automation_cycle,
)
from boxer_company_api.automation import (
    AutomationCycleTrigger,
    DurableAutomationCycleCoordinator,
    JsonAutomationCycleStateStore,
)
from boxer_company_api.security import validate_company_api_runtime_security


_CYCLE_ORDER: tuple[AutomationScheduleCycle, ...] = (
    "weekly_recordings",
    "daily_device_round",
    "device_health_monitor",
    "device_notification_alert",
    "sms_delivery",
)
_CYCLE_FLAGS = {
    "weekly_recordings": "WEEKLY_RECORDINGS_REPORT_ENABLED",
    "daily_device_round": "DAILY_DEVICE_ROUND_ENABLED",
    "device_health_monitor": "DEVICE_HEALTH_MONITOR_ENABLED",
    "device_notification_alert": "DEVICE_NOTIFICATION_ALERT_ENABLED",
    "sms_delivery": "SMS_DELIVERY_REPORTER_ENABLED",
}
_CYCLE_CHANNEL_KEYS = {
    "weekly_recordings": "WEEKLY_RECORDINGS_REPORT_CHANNEL_ID",
    "daily_device_round": "DAILY_DEVICE_ROUND_CHANNEL_ID",
    "device_health_monitor": "DEVICE_HEALTH_MONITOR_CHANNEL_ID",
    "device_notification_alert": "DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
}
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})
_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
)
_CHANNEL_ID_PATTERN = re.compile(r"^[CGD][A-Z0-9]{5,31}$")


@dataclass(frozen=True, slots=True)
class AutomationDeliveryTarget:
    """scheduler가 실행 시점에 고정하는 Slack transport 목적지다."""

    channel_id: str
    conversation: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not _CHANNEL_ID_PATTERN.fullmatch(self.channel_id):
            raise ValueError("automation delivery channel is invalid")
        if not isinstance(self.conversation, Mapping):
            raise ValueError("automation delivery conversation is invalid")
        object.__setattr__(self, "conversation", dict(self.conversation))


@dataclass(frozen=True, slots=True)
class AutomationSchedulerSettings:
    """API companion 하나가 소유하는 tenant·일정·목적지 설정이다."""

    tenant_id: str
    state_path: str
    enabled_cycles: tuple[AutomationScheduleCycle, ...]
    schedule: AutomationScheduleConfig
    delivery_targets: Mapping[
        AutomationScheduleCycle,
        AutomationDeliveryTarget,
    ]
    daily_options: Mapping[str, bool]
    poll_interval: timedelta = timedelta(seconds=5)

    def __post_init__(self) -> None:
        if not _IDENTIFIER_PATTERN.fullmatch(self.tenant_id):
            raise ValueError("automation scheduler tenant is invalid")
        if not str(self.state_path).startswith("/"):
            raise ValueError("automation scheduler state path is invalid")
        if (
            not self.poll_interval > timedelta(0)
            or self.poll_interval > timedelta(minutes=5)
        ):
            raise ValueError("automation scheduler poll interval is invalid")
        if len(self.enabled_cycles) != len(set(self.enabled_cycles)):
            raise ValueError("automation scheduler cycles are duplicated")
        if any(cycle not in _CYCLE_ORDER for cycle in self.enabled_cycles):
            raise ValueError("automation scheduler cycle is invalid")
        expected_targets = {
            cycle
            for cycle in self.enabled_cycles
            if cycle != "sms_delivery"
        }
        if set(self.delivery_targets) != expected_targets:
            raise ValueError("automation scheduler delivery targets are invalid")
        expected_daily_options = {
            "autoUpdateAgent",
            "autoUpdateBoxFree",
            "autoUpdateBoxPaid",
            "autoCleanupTrashCan",
            "autoPowerOff",
        }
        if set(self.daily_options) != expected_daily_options or any(
            type(value) is not bool for value in self.daily_options.values()
        ):
            raise ValueError("automation scheduler daily options are invalid")
        object.__setattr__(
            self,
            "delivery_targets",
            dict(self.delivery_targets),
        )
        object.__setattr__(self, "daily_options", dict(self.daily_options))


@dataclass(frozen=True, slots=True)
class ScheduledAutomationRun:
    """순수 planner가 확정하고 coordinator에 넘기는 실행 입력이다."""

    request_id: str
    tenant_id: str
    cycle: AutomationCycleName
    cycle_key: str
    scheduled_at: datetime
    options: Mapping[str, bool]
    delivery_target: AutomationDeliveryTarget | None


@dataclass(frozen=True, slots=True)
class AutomationSchedulerTick:
    """한 poll에서 실제로 실행한 cycle만 안전한 식별자로 보고한다."""

    attempted: tuple[AutomationCycleName, ...] = ()


class AutomationScheduler:
    """durable state를 읽고 due인 domain cycle만 한 번 호출한다."""

    def __init__(
        self,
        settings: AutomationSchedulerSettings,
        state_store: JsonAutomationCycleStateStore,
        run_cycle: Callable[[ScheduledAutomationRun], Any],
        *,
        clock: Callable[[], datetime] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._settings = settings
        self._state_store = state_store
        self._run_cycle = run_cycle
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._logger = logger or logging.getLogger(__name__)

    def run_once(self, *, now: datetime | None = None) -> AutomationSchedulerTick:
        actual_now = now or self._clock()
        if actual_now.tzinfo is None:
            raise ValueError("automation scheduler clock is invalid")
        attempted: list[AutomationCycleName] = []
        for cycle in self._settings.enabled_cycles:
            tick = self.run_cycle_once(cycle, now=actual_now)
            attempted.extend(tick.attempted)
        return AutomationSchedulerTick(attempted=tuple(attempted))

    def run_cycle_once(
        self,
        cycle: AutomationScheduleCycle,
        *,
        now: datetime | None = None,
    ) -> AutomationSchedulerTick:
        """cycle별 worker가 다른 장기 순회에 막히지 않게 한 종류만 본다."""

        if cycle not in self._settings.enabled_cycles:
            raise ValueError("automation scheduler cycle is not enabled")
        actual_now = now or self._clock()
        if actual_now.tzinfo is None:
            raise ValueError("automation scheduler clock is invalid")
        candidate = plan_automation_cycle(
            cycle,
            now=actual_now,
            config=self._settings.schedule,
        )
        if candidate.cycle_key is None:
            return AutomationSchedulerTick()
        state_key = _state_key(
            self._settings.tenant_id,
            cycle,
            candidate.cycle_key,
        )
        state = self._state_store.load(state_key)
        if (
            state.get("inFlight")
            or state.get("ackInFlight")
            or state.get("pendingDeliveries")
        ):
            # 불명 marker와 아직 전달하지 않은 payload는 operator/transport가
            # 닫기 전까지 scheduler가 같은 domain target을 재실행하지 않는다.
            return AutomationSchedulerTick()
        decision = plan_automation_cycle(
            cycle,
            now=actual_now,
            config=self._settings.schedule,
            completed_cycle_key=(
                candidate.cycle_key
                if (
                    cycle in {"weekly_recordings", "daily_device_round"}
                    and state.get("cycleCompleted") is True
                )
                else None
            ),
            last_completed_at=(
                _parse_state_datetime(state.get("lastCompletedAt"))
                if cycle
                in {
                    "device_health_monitor",
                    "device_notification_alert",
                    "sms_delivery",
                }
                else None
            ),
        )
        if (
            not decision.due
            or decision.cycle_key is None
            or decision.scheduled_at is None
        ):
            return AutomationSchedulerTick()
        scheduled_run = ScheduledAutomationRun(
            request_id=_build_request_id(
                tenant_id=self._settings.tenant_id,
                cycle=cycle,
                cycle_key=decision.cycle_key,
                scheduled_at=decision.scheduled_at,
            ),
            tenant_id=self._settings.tenant_id,
            cycle=cycle,
            cycle_key=decision.cycle_key,
            scheduled_at=decision.scheduled_at,
            options=(
                self._settings.daily_options
                if cycle == "daily_device_round"
                else {}
            ),
            delivery_target=self._settings.delivery_targets.get(cycle),
        )
        self._run_cycle(scheduled_run)
        return AutomationSchedulerTick(attempted=(cycle,))

    def run_forever(self, stop_event: threading.Event | None = None) -> None:
        stopper = stop_event or threading.Event()
        # 일일 장비 순회가 수십 분 걸려도 LED·notification·SMS poll이
        # 멈추지 않도록 cycle마다 독립 fixed-delay worker를 둔다.
        workers = [
            threading.Thread(
                target=self._run_cycle_forever,
                args=(cycle, stopper),
                name=f"boxer-automation-{cycle}",
                daemon=False,
            )
            for cycle in self._settings.enabled_cycles
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()

    def _run_cycle_forever(
        self,
        cycle: AutomationScheduleCycle,
        stop_event: threading.Event,
    ) -> None:
        """한 cycle 오류가 다른 worker와 process를 종료하지 않게 격리한다."""

        while not stop_event.is_set():
            try:
                tick = self.run_cycle_once(cycle)
                if tick.attempted:
                    self._logger.info(
                        "Automation scheduler completed cycles=%s",
                        ",".join(tick.attempted),
                    )
            except Exception as exc:
                # 원문에는 provider/장비 정보가 섞일 수 있어 type만 남긴다.
                # coordinator marker가 같은 target의 자동 재시도를 차단한다.
                self._logger.error(
                    "Automation scheduler cycle failed error_type=%s",
                    type(exc).__name__,
                )
            stop_event.wait(self._settings.poll_interval.total_seconds())


def load_automation_scheduler_settings(
    env: Mapping[str, str] | None = None,
) -> AutomationSchedulerSettings:
    """기존 reporter env를 API-owned scheduler 계약으로 엄격히 읽는다."""

    source = env if env is not None else os.environ
    if not _read_bool(
        source,
        "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
        default=False,
    ):
        raise ValueError("automation scheduler is disabled")
    enabled = tuple(
        cycle
        for cycle in _CYCLE_ORDER
        if _read_bool(source, _CYCLE_FLAGS[cycle], default=False)
    )
    if not enabled:
        raise ValueError("automation scheduler has no enabled cycles")
    tenant_id = str(
        source.get("BOXER_COMPANY_API_AUTOMATION_TENANT_ID", "")
    ).strip()
    targets: dict[AutomationScheduleCycle, AutomationDeliveryTarget] = {}
    for cycle in enabled:
        channel_key = _CYCLE_CHANNEL_KEYS.get(cycle)
        if channel_key is None:
            continue
        targets[cycle] = AutomationDeliveryTarget(
            channel_id=str(source.get(channel_key, "")).strip(),
        )
    # 새 API-owned 이름을 우선하되 기존 운영 env의 조용한 30초 리셋을
    # 막기 위해 Solapi poll 이름을 한시적 읽기 alias로만 수용한다.
    sms_poll_interval_key = (
        "SMS_DELIVERY_REPORTER_POLL_INTERVAL_SEC"
        if "SMS_DELIVERY_REPORTER_POLL_INTERVAL_SEC" in source
        else "SOLAPI_DELIVERY_REPORT_POLL_INTERVAL_SEC"
    )
    schedule = AutomationScheduleConfig(
        weekly_hour=_read_int(
            source,
            "WEEKLY_RECORDINGS_REPORT_HOUR_KST",
            default=9,
            minimum=0,
            maximum=23,
        ),
        weekly_minute=_read_int(
            source,
            "WEEKLY_RECORDINGS_REPORT_MINUTE_KST",
            default=0,
            minimum=0,
            maximum=59,
        ),
        daily_start_hour=_read_int(
            source,
            "DAILY_DEVICE_ROUND_HOUR_KST",
            default=22,
            minimum=0,
            maximum=23,
        ),
        daily_start_minute=_read_int(
            source,
            "DAILY_DEVICE_ROUND_MINUTE_KST",
            default=0,
            minimum=0,
            maximum=59,
        ),
        daily_end_hour=_read_int(
            source,
            "DAILY_DEVICE_ROUND_END_HOUR_KST",
            default=6,
            minimum=0,
            maximum=23,
        ),
        daily_end_minute=_read_int(
            source,
            "DAILY_DEVICE_ROUND_END_MINUTE_KST",
            default=0,
            minimum=0,
            maximum=59,
        ),
        device_health_monitor_interval=timedelta(
            seconds=_read_int(
                source,
                "DEVICE_HEALTH_MONITOR_POLL_INTERVAL_SEC",
                default=60,
                minimum=1,
                maximum=86_400,
            )
        ),
        device_notification_alert_interval=timedelta(
            seconds=_read_int(
                source,
                "DEVICE_NOTIFICATION_ALERT_POLL_INTERVAL_SEC",
                default=30,
                minimum=1,
                maximum=86_400,
            )
        ),
        sms_delivery_interval=timedelta(
            seconds=_read_int(
                source,
                sms_poll_interval_key,
                default=30,
                minimum=1,
                maximum=86_400,
            )
        ),
    )
    return AutomationSchedulerSettings(
        tenant_id=tenant_id,
        state_path=str(
            source.get(
                "BOXER_COMPANY_API_AUTOMATION_STATE_PATH",
                "/var/lib/boxer-company-api/automation_state.json",
            )
        ).strip(),
        enabled_cycles=enabled,
        schedule=schedule,
        delivery_targets=targets,
        daily_options={
            "autoUpdateAgent": _read_bool(
                source,
                "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT",
                default=False,
            ),
            "autoUpdateBoxFree": _read_bool(
                source,
                "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE",
                default=False,
            ),
            "autoUpdateBoxPaid": _read_bool(
                source,
                "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID",
                default=False,
            ),
            "autoCleanupTrashCan": _read_bool(
                source,
                "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN",
                default=False,
            ),
            "autoPowerOff": _read_bool(
                source,
                "DAILY_DEVICE_ROUND_AUTO_POWER_OFF",
                default=False,
            ),
        },
        poll_interval=timedelta(
            seconds=_read_int(
                source,
                "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_POLL_INTERVAL_SEC",
                default=5,
                minimum=1,
                maximum=300,
            )
        ),
    )


def main() -> None:
    """FastAPI worker와 분리된 단일 scheduler companion을 실행한다."""

    validate_company_api_runtime_security()
    settings = load_automation_scheduler_settings()
    logging.basicConfig(level=logging.INFO)
    state_store = JsonAutomationCycleStateStore(settings.state_path)
    coordinator = DurableAutomationCycleCoordinator(
        build_default_automation_cycle_service(),
        state_store,
    )

    def run_cycle(scheduled: ScheduledAutomationRun) -> Any:
        target = scheduled.delivery_target
        return coordinator.run(
            AutomationCycleTrigger(
                request_id=scheduled.request_id,
                tenant_id=scheduled.tenant_id,
                cycle=scheduled.cycle,
                cycle_key=scheduled.cycle_key,
                scheduled_at=scheduled.scheduled_at,
                options=dict(scheduled.options),
                delivery_target=(
                    {
                        "channelId": target.channel_id,
                        "conversation": dict(target.conversation),
                    }
                    if target is not None
                    else None
                ),
            )
        )

    AutomationScheduler(settings, state_store, run_cycle).run_forever()


def _state_key(tenant_id: str, cycle: str, cycle_key: str) -> str:
    raw = "\0".join((tenant_id, cycle, cycle_key))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _parse_state_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError("automation scheduler state time is invalid") from exc
    if parsed.tzinfo is None:
        raise ValueError("automation scheduler state time is invalid")
    return parsed


def _build_request_id(
    *,
    tenant_id: str,
    cycle: str,
    cycle_key: str,
    scheduled_at: datetime,
) -> str:
    raw = "\0".join(
        (tenant_id, cycle, cycle_key, scheduled_at.isoformat())
    )
    digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
    return f"automation-scheduler:{cycle}:{digest}"


def _read_bool(
    env: Mapping[str, str],
    key: str,
    *,
    default: bool,
) -> bool:
    raw = str(env.get(key, "")).strip().lower()
    if not raw:
        return default
    if raw in _TRUE_VALUES:
        return True
    if raw in _FALSE_VALUES:
        return False
    raise ValueError("automation scheduler boolean is invalid")


def _read_int(
    env: Mapping[str, str],
    key: str,
    *,
    default: int,
    minimum: int,
    maximum: int,
) -> int:
    raw = str(env.get(key, "")).strip()
    try:
        value = default if not raw else int(raw)
    except ValueError as exc:
        raise ValueError("automation scheduler integer is invalid") from exc
    if not minimum <= value <= maximum:
        raise ValueError("automation scheduler integer is invalid")
    return value


__all__ = [
    "AutomationDeliveryTarget",
    "AutomationScheduler",
    "AutomationSchedulerSettings",
    "AutomationSchedulerTick",
    "ScheduledAutomationRun",
    "load_automation_scheduler_settings",
    "main",
]
