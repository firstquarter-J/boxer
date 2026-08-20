from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
import logging
import re
from typing import Any, Literal, Mapping, Protocol, Sequence

from boxer_company.daily_device_round import (
    _build_daily_device_round_summary,
)
from boxer_company.device_health_monitor_cycle import (
    DeviceHealthMonitorCycleDeps,
    acknowledge_device_health_monitor_deliveries,
    device_health_monitor_cursor_digest,
    run_device_health_monitor_cycle,
)
from boxer_company.sms_delivery_cycle import run_sms_delivery_cycle_once
from boxer_company.weekly_recordings_report import (
    _build_weekly_recordings_report_summary,
    _resolve_weekly_recordings_report_target_week,
)


AutomationCycleName = Literal[
    "weekly_recordings",
    "daily_device_round",
    "device_health_monitor",
    "device_notification_alert",
    "sms_delivery",
]
AutomationCycleOutcome = Literal["completed", "no_change"]
AutomationDeliveryStatus = Literal["sent", "failed"]

_AUTOMATION_CYCLE_NAMES = frozenset(
    {
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    }
)
_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_TENANT_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
)
_SENSITIVE_KEY_MARKERS = (
    "apikey",
    "authorization",
    "credential",
    "password",
    "privatekey",
    "secret",
    "token",
)
_RAW_ERROR_KEYS = frozenset(
    {
        "error",
        "exception",
        "stacktrace",
        "traceback",
    }
)
_SENSITIVE_VALUE_PATTERN = re.compile(
    r"(?i)(?:bearer\s+\S+|(?:api[_-]?key|password|secret|token)\s*[:=]\s*\S+)"
)
_PRESENTATION_VERSION_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,63}$"
)
_DAILY_STATUS_LABELS = frozenset(
    {"정상", "확인 필요", "이상", "점검 불가"}
)
_DAILY_COMPONENT_NAMES = {
    "audio": "오디오",
    "pm2": "PM2",
    "storage": "스토리지",
    "captureboard": "캡처보드",
    "led": "LED",
}
_DAILY_COUNT_KEYS = {
    "statusCounts": ("정상", "확인 필요", "이상", "점검 불가"),
    "updateCounts": (
        "agentCandidates",
        "agentUpdated",
        "agentUpdateFailed",
        "boxCandidates",
        "boxUpdated",
        "boxUpdateFailed",
    ),
    "cleanupCounts": ("candidates", "executed", "failed"),
    "powerCounts": (
        "requested",
        "poweredOff",
        "alreadyOffline",
        "powerOffFailed",
    ),
}


class AutomationCycleContractError(ValueError):
    """공통 API와 adapter 사이 cycle 계약이 올바르지 않을 때 발생한다."""


@dataclass(frozen=True, slots=True)
class AutomationCycleRequest:
    """Slack 스케줄러가 공통 API에 한 번만 넘기는 주기 실행 요청이다."""

    request_id: str
    tenant_id: str
    cycle: AutomationCycleName
    scheduled_at: datetime
    # cursor와 options는 연락처나 장비 식별자를 포함할 수 있어 repr에서 숨긴다.
    cursor: Mapping[str, Any] = field(default_factory=dict, repr=False)
    options: Mapping[str, Any] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not _REQUEST_ID_PATTERN.fullmatch(str(self.request_id or "")):
            raise AutomationCycleContractError("invalid automation request id")
        if not _TENANT_ID_PATTERN.fullmatch(str(self.tenant_id or "")):
            raise AutomationCycleContractError("invalid automation tenant id")
        if self.cycle not in _AUTOMATION_CYCLE_NAMES:
            raise AutomationCycleContractError("unsupported automation cycle")
        if self.scheduled_at.tzinfo is None:
            raise AutomationCycleContractError(
                "automation scheduled_at must be timezone-aware"
            )
        _validate_cycle_mapping(self.cursor, path="cursor")
        _validate_cycle_mapping(self.options, path="options")
        _assert_no_sensitive_keys(self.cursor, path="cursor")
        _assert_no_sensitive_keys(self.options, path="options")
        object.__setattr__(self, "cursor", deepcopy(dict(self.cursor)))
        object.__setattr__(self, "options", deepcopy(dict(self.options)))


@dataclass(frozen=True, slots=True)
class AutomationDelivery:
    """채널별 렌더링 전의 도메인 결과와 중복 방지 키다."""

    delivery_id: str
    kind: str
    payload: Mapping[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        if not _REQUEST_ID_PATTERN.fullmatch(str(self.delivery_id or "")):
            raise AutomationCycleContractError("invalid automation delivery id")
        if not _TENANT_ID_PATTERN.fullmatch(str(self.kind or "")):
            raise AutomationCycleContractError("invalid automation delivery kind")
        _validate_cycle_mapping(self.payload, path="delivery.payload")
        _assert_safe_cycle_output(self.payload, path="delivery.payload")
        object.__setattr__(self, "payload", deepcopy(dict(self.payload)))


@dataclass(frozen=True, slots=True)
class AutomationDeliveryReceipt:
    """Slack 발송 뒤 API domain hook이 소비하는 최소 delivery receipt다."""

    delivery_id: str
    status: AutomationDeliveryStatus
    external_message_id: str = ""
    permalink: str = field(default="", repr=False)
    delivered_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AutomationCycleResult:
    """API가 반환하고 Slack이 발송 상태와 함께 저장할 cycle 결과다."""

    cycle: AutomationCycleName
    outcome: AutomationCycleOutcome
    cursor: Mapping[str, Any] = field(default_factory=dict, repr=False)
    deliveries: tuple[AutomationDelivery, ...] = field(default_factory=tuple)
    metrics: Mapping[str, Any] = field(default_factory=dict, repr=False)
    # mutation 포함 여부와 관계없이 cycle transport는 자동 재시도를 허용하지 않는다.
    auto_retry_allowed: Literal[False] = False

    def __post_init__(self) -> None:
        if self.cycle not in _AUTOMATION_CYCLE_NAMES:
            raise AutomationCycleContractError("unsupported automation cycle")
        if self.outcome not in {"completed", "no_change"}:
            raise AutomationCycleContractError("invalid automation outcome")
        if self.auto_retry_allowed is not False:
            raise AutomationCycleContractError(
                "automation cycles must not enable automatic retries"
            )
        _validate_cycle_mapping(self.cursor, path="result.cursor")
        _validate_cycle_mapping(self.metrics, path="result.metrics")
        _assert_safe_cycle_output(self.cursor, path="result.cursor")
        _assert_safe_cycle_output(self.metrics, path="result.metrics")
        delivery_ids = [delivery.delivery_id for delivery in self.deliveries]
        if len(delivery_ids) != len(set(delivery_ids)):
            raise AutomationCycleContractError(
                "automation delivery ids must be unique"
            )
        object.__setattr__(self, "cursor", deepcopy(dict(self.cursor)))
        object.__setattr__(self, "metrics", deepcopy(dict(self.metrics)))


class AutomationCycleHandler(Protocol):
    @property
    def name(self) -> AutomationCycleName: ...

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult: ...


class AutomationCycleService:
    """등록된 handler를 정확히 한 번 호출하는 동기 cycle dispatcher다."""

    def __init__(
        self,
        handlers: Sequence[AutomationCycleHandler],
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._handlers = {handler.name: handler for handler in handlers}
        if len(self._handlers) != len(handlers):
            raise AutomationCycleContractError(
                "automation cycle handler names must be unique"
            )
        self._logger = logger or logging.getLogger(__name__)

    @property
    def cycle_names(self) -> tuple[AutomationCycleName, ...]:
        return tuple(self._handlers)

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult:
        handler = self._handlers.get(request.cycle)
        if handler is None:
            raise AutomationCycleContractError(
                "automation cycle handler is not configured"
            )
        validator = getattr(handler, "validate", None)
        if callable(validator):
            validator(request)
        try:
            # mutation 가능 cycle도 transport 안에서는 한 번만 호출한다.
            result = handler.run(request)
        except Exception as exc:
            # 상태, payload, 예외 문자열에는 PII나 credential이 섞일 수 있어
            # cycle명과 오류 타입만 기록한다.
            self._logger.warning(
                "Automation cycle failed cycle=%s error_type=%s",
                request.cycle,
                type(exc).__name__,
            )
            raise
        if result.cycle != request.cycle:
            raise AutomationCycleContractError(
                "automation handler returned a different cycle"
            )
        self._logger.info(
            "Automation cycle completed cycle=%s outcome=%s deliveries=%s",
            request.cycle,
            result.outcome,
            len(result.deliveries),
        )
        return result

    def validate(self, request: AutomationCycleRequest) -> None:
        """외부 실행 전에 handler별 option 계약만 선검증한다."""

        handler = self._handlers.get(request.cycle)
        if handler is None:
            raise AutomationCycleContractError(
                "automation cycle handler is not configured"
            )
        validator = getattr(handler, "validate", None)
        if callable(validator):
            validator(request)

    def has_acknowledger(self, cycle: AutomationCycleName) -> bool:
        handler = self._handlers.get(cycle)
        return handler is not None and callable(
            getattr(handler, "acknowledge", None)
        )

    def acknowledge(
        self,
        request: AutomationCycleRequest,
        receipts: tuple[AutomationDeliveryReceipt, ...],
    ) -> Mapping[str, Any]:
        """delivery 후 domain mutation hook을 정확히 한 번 호출한다."""

        self.validate(request)
        handler = self._handlers.get(request.cycle)
        if handler is None:
            raise AutomationCycleContractError(
                "automation cycle handler is not configured"
            )
        acknowledger = getattr(handler, "acknowledge", None)
        if not callable(acknowledger):
            return dict(request.cursor)
        updated_cursor = acknowledger(request, receipts)
        if updated_cursor is None:
            return dict(request.cursor)
        _validate_cycle_mapping(
            updated_cursor,
            path="acknowledgement.cursor",
        )
        _assert_safe_cycle_output(
            updated_cursor,
            path="acknowledgement.cursor",
        )
        return deepcopy(dict(updated_cursor))


class WeeklyRecordingsCycleHandler:
    name: AutomationCycleName = "weekly_recordings"

    def validate(self, request: AutomationCycleRequest) -> None:
        if request.options:
            raise AutomationCycleContractError(
                "weekly recordings cycle does not accept options"
            )

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult:
        self.validate(request)
        week_start, week_end = _resolve_weekly_recordings_report_target_week(
            now=request.scheduled_at
        )
        summary = _build_weekly_recordings_report_summary(
            target_date=week_start,
            now=request.scheduled_at,
        )
        safe_summary = _redact_cycle_payload(summary)
        return AutomationCycleResult(
            cycle=self.name,
            outcome="completed",
            cursor={
                "lastReportedWeekStartDate": week_start.isoformat(),
                "lastReportedWeekEndDate": week_end.isoformat(),
                # API durable coordinator가 같은 주차를 다시 실행하지 않게
                # delivery ack 뒤 닫을 수 있는 명시적 완료 표식이다.
                "cycleCompleted": True,
            },
            deliveries=(
                AutomationDelivery(
                    delivery_id=f"weekly_recordings:{week_start.isoformat()}",
                    kind="weekly_recordings_report",
                    payload=safe_summary,
                ),
            ),
            metrics={
                "hospitalCount": max(
                    0,
                    int(summary.get("hospitalCount") or 0),
                ),
                "recordingCount": max(
                    0,
                    int(summary.get("totalCount") or 0),
                ),
            },
        )


class DailyDeviceRoundCycleHandler:
    name: AutomationCycleName = "daily_device_round"
    _OPTION_KEYS = frozenset(
        {
            "autoUpdateAgent",
            "autoUpdateBoxFree",
            "autoUpdateBoxPaid",
            "autoCleanupTrashCan",
            "autoPowerOff",
        }
    )

    def validate(self, request: AutomationCycleRequest) -> None:
        unknown_options = set(request.options) - self._OPTION_KEYS
        if unknown_options:
            raise AutomationCycleContractError(
                "daily device round contains unsupported options"
            )
        for key in self._OPTION_KEYS:
            _require_bool_option(request.options, key)

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult:
        self.validate(request)
        options = {
            key: _require_bool_option(request.options, key)
            for key in self._OPTION_KEYS
        }
        # 기존 동기 구현을 그대로 한 번 실행한다. SSH open, update, cleanup,
        # power-off의 HTTP transport 재시도는 이 경계에서 추가하지 않는다.
        summary = _build_daily_device_round_summary(
            now=request.scheduled_at,
            state=dict(request.cursor),
            auto_update_agent=options["autoUpdateAgent"],
            auto_update_box_free=options["autoUpdateBoxFree"],
            auto_update_box_paid=options["autoUpdateBoxPaid"],
            auto_cleanup_trashcan=options["autoCleanupTrashCan"],
            auto_power_off=options["autoPowerOff"],
        )
        # 장비 실행 결과 전체에는 SSH endpoint, command, provider 응답이
        # 포함된다. Slack wire에는 renderer가 쓰는 의미 값만 새 DTO로 만든다.
        presentation = _project_daily_device_round_presentation(
            summary,
            scheduled_at=request.scheduled_at,
        )
        hospital_seq = _coerce_positive_int(summary.get("hospitalSeq"))
        processed_hospital_seqs = _coerce_positive_int_list(
            request.cursor.get("processedHospitalSeqs")
        )
        if hospital_seq is not None and hospital_seq not in processed_hospital_seqs:
            processed_hospital_seqs.append(hospital_seq)
        candidate_count = max(
            0,
            int(summary.get("candidateHospitalCount") or 0),
        )
        window_key = str(
            request.cursor.get("windowKey")
            or request.scheduled_at.date().isoformat()
        ).strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", window_key):
            raise AutomationCycleContractError(
                "daily device round window key is invalid"
            )
        completed = (
            hospital_seq is None
            or (
                candidate_count > 0
                and len(processed_hospital_seqs) >= candidate_count
            )
        )
        cursor = {
            "windowKey": window_key,
            "hospitalScope": str(summary.get("hospitalScope") or ""),
            "hospitalOrder": str(summary.get("hospitalOrder") or ""),
            "processedHospitalSeqs": processed_hospital_seqs,
            "lastHospitalSeq": hospital_seq,
            "nextHospitalSeq": _coerce_positive_int(
                summary.get("nextHospitalSeq")
            ),
            "windowCompletedAt": (
                request.scheduled_at.isoformat() if completed else ""
            ),
            "cycleCompleted": completed,
            "lastRunDate": request.scheduled_at.date().isoformat(),
            "statusCounts": _project_daily_counts(summary, "statusCounts"),
            "updateCounts": _project_daily_counts(summary, "updateCounts"),
            "cleanupCounts": _project_daily_counts(summary, "cleanupCounts"),
            "powerCounts": _project_daily_counts(summary, "powerCounts"),
        }
        deliveries: tuple[AutomationDelivery, ...] = ()
        outcome: AutomationCycleOutcome = "no_change"
        if hospital_seq is not None:
            deliveries = (
                AutomationDelivery(
                    delivery_id=(
                        f"daily_device_round:"
                        f"{window_key}:"
                        f"{hospital_seq}"
                    ),
                    kind="daily_device_round_report",
                    payload=presentation,
                ),
            )
            outcome = "completed"
        return AutomationCycleResult(
            cycle=self.name,
            outcome=outcome,
            cursor=cursor,
            deliveries=deliveries,
            metrics={
                "candidateHospitalCount": candidate_count,
                "processedHospitalCount": len(processed_hospital_seqs),
                "deviceCount": max(
                    0,
                    int(summary.get("deviceCount") or 0),
                ),
                "deliveryCount": len(deliveries),
            },
        )


class DeviceHealthMonitorCycleHandler:
    """Slack은 poll/렌더/발송만 맡고 장비 점검과 mutation은 API에서 실행한다."""

    name: AutomationCycleName = "device_health_monitor"
    def __init__(
        self,
        *,
        deps: DeviceHealthMonitorCycleDeps | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps or DeviceHealthMonitorCycleDeps()
        self._logger = logger or logging.getLogger(__name__)

    def validate(self, request: AutomationCycleRequest) -> None:
        if request.options:
            raise AutomationCycleContractError(
                "device health monitor contains unsupported options"
            )
        try:
            # seed 검증은 coordinator의 durable in-flight 기록과 외부 조회보다
            # 먼저 실행돼 미이관 상태가 mutation을 시작하지 못하게 한다.
            device_health_monitor_cursor_digest(request.cursor)
        except ValueError as exc:
            raise AutomationCycleContractError(
                "device health monitor API state seed is required"
            ) from exc

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult:
        self.validate(request)
        cycle_run = run_device_health_monitor_cycle(
            request_id=request.request_id,
            now=request.scheduled_at,
            cursor=request.cursor,
            options=request.options,
            deps=self._deps,
            logger=self._logger,
        )
        deliveries = tuple(
            AutomationDelivery(
                delivery_id=item.delivery_id,
                kind="device_health_alert",
                payload=item.payload,
            )
            for item in cycle_run.deliveries
        )
        return AutomationCycleResult(
            cycle=self.name,
            outcome="completed" if deliveries else "no_change",
            cursor=cycle_run.cursor,
            deliveries=deliveries,
            metrics=cycle_run.metrics,
        )

    def acknowledge(
        self,
        request: AutomationCycleRequest,
        receipts: tuple[AutomationDeliveryReceipt, ...],
    ) -> Mapping[str, Any]:
        # Slack permalink가 생긴 뒤 API가 Sheets outbox를 한 번만 닫는다.
        return acknowledge_device_health_monitor_deliveries(
            cursor=request.cursor,
            receipts=receipts,
            deps=self._deps,
            logger=self._logger,
        )


class SmsDeliveryCycleHandler:
    """Solapi 조회와 Sheet 갱신을 Slack 프로세스 밖에서 한 번 실행한다."""

    name: AutomationCycleName = "sms_delivery"

    def __init__(
        self,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._logger = logger or logging.getLogger(__name__)

    def validate(self, request: AutomationCycleRequest) -> None:
        if request.options:
            raise AutomationCycleContractError(
                "sms delivery cycle does not accept options"
            )

    def run(self, request: AutomationCycleRequest) -> AutomationCycleResult:
        self.validate(request)
        # provider GET과 조건부 Sheet 갱신은 내부 service가 기존 sync
        # semantics로 한 번만 수행하고 transport 계층은 재호출하지 않는다.
        updated_count = max(
            0,
            int(
                run_sms_delivery_cycle_once(
                    self._logger,
                    now=request.scheduled_at,
                )
                or 0
            ),
        )
        return AutomationCycleResult(
            cycle=self.name,
            outcome="completed" if updated_count else "no_change",
            cursor={
                "lastRunAt": request.scheduled_at.isoformat(),
                "cycleCompleted": False,
            },
            deliveries=(),
            metrics={
                "updatedCount": updated_count,
                "deliveryCount": 0,
            },
        )


def build_default_automation_cycle_service(
    *,
    logger: logging.Logger | None = None,
) -> AutomationCycleService:
    """이미 회사 도메인으로 분리된 cycle만 공통 API용으로 조립한다."""

    # 계약 타입이 정의된 뒤 import해 notification handler가 같은 계약을
    # 사용하면서도 module import 순환을 만들지 않게 한다.
    from boxer_company.device_notification_cycle import (
        DeviceNotificationAlertCycleHandler,
    )

    return AutomationCycleService(
        (
            WeeklyRecordingsCycleHandler(),
            DailyDeviceRoundCycleHandler(),
            DeviceHealthMonitorCycleHandler(logger=logger),
            DeviceNotificationAlertCycleHandler(logger=logger),
            SmsDeliveryCycleHandler(logger=logger),
        ),
        logger=logger,
    )


def _normalize_key(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value or "").lower())


def _is_sensitive_key(value: Any) -> bool:
    normalized = _normalize_key(value)
    return any(marker in normalized for marker in _SENSITIVE_KEY_MARKERS)


def _validate_cycle_mapping(value: Any, *, path: str) -> None:
    if not isinstance(value, Mapping):
        raise AutomationCycleContractError(f"{path} must be an object")
    _validate_cycle_json_value(value, path=path)


def _validate_cycle_json_value(value: Any, *, path: str) -> None:
    if value is None or isinstance(value, (str, int, float, bool)):
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                raise AutomationCycleContractError(
                    f"{path} contains an invalid key"
                )
            _validate_cycle_json_value(item, path=f"{path}.{key}")
        return
    if isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _validate_cycle_json_value(item, path=f"{path}[{index}]")
        return
    raise AutomationCycleContractError(
        f"{path} contains a non-JSON value"
    )


def _assert_no_sensitive_keys(value: Any, *, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if _is_sensitive_key(key):
                raise AutomationCycleContractError(
                    f"{path} contains a sensitive field"
                )
            _assert_no_sensitive_keys(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_no_sensitive_keys(item, path=f"{path}[{index}]")


def _assert_safe_cycle_output(value: Any, *, path: str) -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if _is_sensitive_key(key) or _normalize_key(key) in _RAW_ERROR_KEYS:
                raise AutomationCycleContractError(
                    f"{path} contains a sensitive output field"
                )
            _assert_safe_cycle_output(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_safe_cycle_output(item, path=f"{path}[{index}]")
    elif isinstance(value, str) and _SENSITIVE_VALUE_PATTERN.search(value):
        raise AutomationCycleContractError(
            f"{path} contains a sensitive output value"
        )


def _redact_cycle_payload(value: Any) -> Any:
    """도메인 예외 원문·credential 필드를 delivery payload에서 제거한다."""

    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key, item in value.items():
            if _is_sensitive_key(key) or _normalize_key(key) in _RAW_ERROR_KEYS:
                continue
            result[str(key)] = _redact_cycle_payload(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_redact_cycle_payload(item) for item in value]
    if isinstance(value, str):
        return _SENSITIVE_VALUE_PATTERN.sub("[REDACTED]", value)
    return value


def _require_bool_option(options: Mapping[str, Any], key: str) -> bool:
    value = options.get(key, False)
    if not isinstance(value, bool):
        raise AutomationCycleContractError(
            "daily device round options must be booleans"
        )
    return value


def _project_daily_device_round_presentation(
    summary: Mapping[str, Any],
    *,
    scheduled_at: datetime,
) -> dict[str, Any]:
    """실행용 raw 결과를 Slack 표시 전용 allowlist DTO로 축소한다."""

    hospital_seq = _coerce_positive_int(summary.get("hospitalSeq"))
    status_counts = _project_daily_counts(summary, "statusCounts")
    result: dict[str, Any] = {
        "runDate": scheduled_at.date().isoformat(),
        "hospitalSeq": hospital_seq,
        "hospitalName": _safe_presentation_name(
            summary.get("hospitalName"),
            default="미선정" if hospital_seq is None else "미확인",
        ),
        "deviceCount": _coerce_nonnegative_int(summary.get("deviceCount")),
        "scheduledDeviceCount": _coerce_nonnegative_int(
            summary.get("scheduledDeviceCount")
        ),
        "statusCounts": status_counts,
        "updateCounts": _project_daily_counts(summary, "updateCounts"),
        "cleanupCounts": _project_daily_counts(summary, "cleanupCounts"),
        "powerCounts": _project_daily_counts(summary, "powerCounts"),
        "summaryLine": _build_daily_presentation_summary_line(
            hospital_seq,
            status_counts,
        ),
        "deviceResults": [],
    }
    raw_devices = summary.get("deviceResults")
    if isinstance(raw_devices, (list, tuple)):
        result["deviceResults"] = [
            _project_daily_device_presentation(item)
            for item in raw_devices
            if isinstance(item, Mapping)
        ]
    return result


def _project_daily_device_presentation(
    item: Mapping[str, Any],
) -> dict[str, Any]:
    component_labels = _project_daily_component_labels(
        item.get("componentLabels")
    )
    status_payload = (
        item.get("statusPayload")
        if isinstance(item.get("statusPayload"), Mapping)
        else {}
    )
    ssh_payload = (
        status_payload.get("ssh")
        if isinstance(status_payload.get("ssh"), Mapping)
        else {}
    )
    overview = (
        status_payload.get("overview")
        if isinstance(status_payload.get("overview"), Mapping)
        else {}
    )
    storage = (
        overview.get("storage")
        if isinstance(overview.get("storage"), Mapping)
        else {}
    )
    network_unavailable = bool(ssh_payload) and not bool(
        ssh_payload.get("ready")
    )
    overall_label = _safe_daily_status_label(
        item.get("overallLabel"),
        default="점검 불가",
    )
    return {
        "deviceName": _safe_presentation_name(
            item.get("deviceName"),
            default="미확인",
        ),
        "roomName": _safe_presentation_name(
            item.get("roomName"),
            default="미확인",
        ),
        "overallLabel": overall_label,
        "networkUnavailable": network_unavailable,
        "issueSummary": _build_daily_issue_presentation(
            component_labels,
            overall_label=overall_label,
            network_unavailable=network_unavailable,
        ),
        "storage": _project_daily_storage_presentation(storage),
        "cleanup": _project_daily_cleanup_presentation(
            item.get("trashcanCleanup")
        ),
        "agentUpdate": _project_daily_update_presentation(
            item,
            route_kind="agent",
        ),
        "boxUpdate": _project_daily_update_presentation(
            item,
            route_kind="box",
        ),
        "power": _project_daily_power_presentation(
            item.get("powerAction")
        ),
    }


def _project_daily_counts(
    summary: Mapping[str, Any],
    key: str,
) -> dict[str, int]:
    source = summary.get(key)
    values = source if isinstance(source, Mapping) else {}
    return {
        count_key: _coerce_nonnegative_int(values.get(count_key))
        for count_key in _DAILY_COUNT_KEYS[key]
    }


def _project_daily_component_labels(value: Any) -> dict[str, str]:
    source = value if isinstance(value, Mapping) else {}
    return {
        key: _safe_daily_status_label(
            source.get(key),
            default="점검 불가",
        )
        for key in _DAILY_COMPONENT_NAMES
    }


def _project_daily_storage_presentation(
    storage: Mapping[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "label": _safe_daily_status_label(
            storage.get("label"),
            default="확인 필요",
        )
    }
    # 저장소 수치는 renderer에 필요한 값만 숫자로 변환하고 command 결과
    # 문자열이나 경로는 어떤 이름으로도 넘기지 않는다.
    for source_key, output_key in (
        ("filesystemUsedPercent", "filesystemUsedPercent"),
        ("filesystemAvailableBytes", "filesystemAvailableBytes"),
        ("filesystemSizeBytes", "filesystemSizeBytes"),
        ("directorySizeBytes", "directorySizeBytes"),
        ("directorySharePercent", "directorySharePercent"),
        ("fileCount", "fileCount"),
        ("expiredFileCount", "expiredFileCount"),
        ("cleanupAgeDays", "cleanupAgeDays"),
    ):
        number = _coerce_nonnegative_number(storage.get(source_key))
        if number is not None:
            result[output_key] = number
    return result


def _project_daily_cleanup_presentation(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, Mapping) else {}
    status = str(source.get("status") or "").strip().lower()
    required = bool(source.get("required"))
    executed = bool(source.get("executed"))
    if executed or status == "completed":
        return {
            "visible": True,
            "statusKind": "success",
            "label": "성공",
            "summary": "정리 실행 완료",
        }
    if status == "failed":
        return {
            "visible": True,
            "statusKind": "failed",
            "label": "실패",
            "summary": "정리 실행 실패",
        }
    if status == "unavailable":
        return {
            "visible": True,
            "statusKind": "check",
            "label": "실행 불가",
            "summary": "장비 연결 상태 확인 필요",
        }
    if required or status == "candidate":
        return {
            "visible": True,
            "statusKind": "pending",
            "label": "대상",
            "summary": "정리 대상",
        }
    return {
        "visible": False,
        "statusKind": "latest",
        "label": "불필요",
        "summary": "정리 대상 아님",
    }


def _project_daily_update_presentation(
    item: Mapping[str, Any],
    *,
    route_kind: Literal["agent", "box"],
) -> dict[str, Any]:
    final_plan = (
        item.get("finalPlan")
        if isinstance(item.get("finalPlan"), Mapping)
        else {}
    )
    plan = (
        final_plan.get(route_kind)
        if isinstance(final_plan.get(route_kind), Mapping)
        else {}
    )
    initial_plan = (
        item.get("initialPlan")
        if isinstance(item.get("initialPlan"), Mapping)
        else {}
    )
    initial_route = (
        initial_plan.get(route_kind)
        if isinstance(initial_plan.get(route_kind), Mapping)
        else {}
    )
    action_key = "agentAction" if route_kind == "agent" else "boxAction"
    action = (
        item.get(action_key)
        if isinstance(item.get(action_key), Mapping)
        else {}
    )
    status = str(action.get("status") or "").strip().lower()
    ok = bool(action.get("ok"))
    current_version = _safe_presentation_version(
        plan.get("currentVersion")
    )
    latest_version = _safe_presentation_version(plan.get("latestVersion"))
    previous_version = _safe_presentation_version(
        initial_route.get("currentVersion")
    )
    final_version = current_version or latest_version

    if action:
        if ok and status in {"completed", "already_latest"}:
            summary = _build_daily_version_summary(
                previous_version,
                final_version,
            )
            return {
                "actionable": True,
                "statusKind": "success",
                "label": "업데이트 완료",
                "summary": summary,
            }
        if status == "dispatch_failed":
            return {
                "actionable": True,
                "statusKind": "failed",
                "label": "업데이트 실패",
                "summary": "업데이트 요청 실패",
            }
        return {
            "actionable": True,
            "statusKind": "check",
            "label": "확인 필요",
            "summary": "업데이트 상태 재확인 필요",
        }

    is_latest = bool(
        plan.get("isHealthy")
        or plan.get("isLatest")
        or plan.get("alreadyLatest")
    )
    if is_latest:
        return {
            "actionable": False,
            "statusKind": "latest",
            "label": "업데이트 불필요",
            "summary": _build_daily_version_summary("", final_version),
        }
    if bool(plan.get("shouldUpdate")):
        return {
            "actionable": True,
            "statusKind": "pending",
            "label": "업데이트 필요",
            "summary": _build_daily_version_summary(
                current_version,
                latest_version,
            ),
        }
    return {
        "actionable": True,
        "statusKind": "check",
        "label": "확인 필요",
        "summary": "업데이트 상태 확인 필요",
    }


def _project_daily_power_presentation(value: Any) -> dict[str, Any]:
    source = value if isinstance(value, Mapping) else {}
    if not source:
        return {
            "visible": False,
            "statusKind": "latest",
            "label": "미실행",
            "summary": "종료 요청 없음",
        }
    status = str(source.get("status") or "").strip().lower()
    ok = bool(source.get("ok"))
    if status == "already_offline" and ok:
        return {
            "visible": True,
            "statusKind": "latest",
            "label": "종료 불필요",
            "summary": "이미 오프라인",
        }
    if status == "completed" and ok:
        return {
            "visible": True,
            "statusKind": "success",
            "label": "종료 완료",
            "summary": "오프라인 확인",
        }
    if status == "dispatch_failed":
        return {
            "visible": True,
            "statusKind": "failed",
            "label": "종료 실패",
            "summary": "종료 요청 실패",
        }
    return {
        "visible": True,
        "statusKind": "check",
        "label": "확인 필요",
        "summary": "전원 종료 재확인 필요",
    }


def _build_daily_issue_presentation(
    component_labels: Mapping[str, str],
    *,
    overall_label: str,
    network_unavailable: bool,
) -> str:
    if network_unavailable:
        return "장비 종료 또는 네트워크 연결 불가로 점검 불가"
    issues = [
        f"{_DAILY_COMPONENT_NAMES[key]} {label}"
        for key, label in component_labels.items()
        if label != "정상"
    ]
    if issues:
        return " / ".join(issues)
    if overall_label != "정상":
        return "장비 상태 확인 필요"
    return ""


def _build_daily_presentation_summary_line(
    hospital_seq: int | None,
    status_counts: Mapping[str, int],
) -> str:
    if hospital_seq is None:
        return "이번 야간 업데이트 창에서 처리할 병원이 없어"
    return (
        f"정상 {status_counts['정상']} / "
        f"확인 필요 {status_counts['확인 필요']} / "
        f"이상 {status_counts['이상']} / "
        f"점검 불가 {status_counts['점검 불가']}"
    )


def _build_daily_version_summary(previous: str, current: str) -> str:
    if previous and current and previous != current:
        return f"버전 {previous} -> {current}"
    if current:
        return f"버전 {current}"
    return "버전 미확인"


def _safe_daily_status_label(value: Any, *, default: str) -> str:
    normalized = str(value or "").strip()
    return normalized if normalized in _DAILY_STATUS_LABELS else default


def _safe_presentation_name(value: Any, *, default: str) -> str:
    normalized = " ".join(str(value or "").split())
    if (
        not normalized
        or len(normalized) > 120
        or _SENSITIVE_VALUE_PATTERN.search(normalized)
        or (
            normalized[:1] in {"{", "["}
            and normalized[-1:] in {"}", "]"}
        )
    ):
        return default
    return normalized


def _safe_presentation_version(value: Any) -> str:
    normalized = str(value or "").strip()
    if not _PRESENTATION_VERSION_PATTERN.fullmatch(normalized):
        return ""
    return normalized


def _coerce_nonnegative_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return max(0, number)


def _coerce_nonnegative_number(value: Any) -> int | float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if number < 0 or number != number or number in {float("inf"), float("-inf")}:
        return None
    return int(number) if number.is_integer() else number


def _coerce_positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _coerce_positive_int_list(value: Any) -> list[int]:
    if not isinstance(value, (list, tuple)):
        return []
    result: list[int] = []
    for item in value:
        number = _coerce_positive_int(item)
        if number is not None and number not in result:
            result.append(number)
    return result


__all__ = [
    "AutomationCycleContractError",
    "AutomationCycleHandler",
    "AutomationCycleName",
    "AutomationCycleOutcome",
    "AutomationCycleRequest",
    "AutomationCycleResult",
    "AutomationCycleService",
    "AutomationDelivery",
    "AutomationDeliveryReceipt",
    "AutomationDeliveryStatus",
    "DailyDeviceRoundCycleHandler",
    "DeviceHealthMonitorCycleHandler",
    "SmsDeliveryCycleHandler",
    "WeeklyRecordingsCycleHandler",
    "build_default_automation_cycle_service",
]
