"""Adapter와 API가 함께 쓰는 provider-free 자동화 transport 계약이다."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime
import re
from typing import Any, Callable, Literal, Mapping, Protocol


AutomationCycleName = Literal[
    "weekly_recordings",
    "daily_device_round",
    "device_health_monitor",
    "device_notification_alert",
    "sms_delivery",
]
AutomationCycleOutcome = Literal["completed", "no_change"]
AutomationDeliveryStatus = Literal["sent", "failed"]
AutomationProgressCallback = Callable[[Mapping[str, Any]], None]

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


class AutomationCycleContractError(ValueError):
    """API와 adapter 사이 자동화 계약이 올바르지 않을 때 발생한다."""


@dataclass(frozen=True, slots=True)
class AutomationCycleRequest:
    """API coordinator가 한 cycle을 실행할 때 쓰는 채널 중립 요청이다."""

    request_id: str
    tenant_id: str
    cycle: AutomationCycleName
    scheduled_at: datetime
    # cursor와 options는 장비 식별자를 포함할 수 있어 repr에서 숨긴다.
    cursor: Mapping[str, Any] = field(default_factory=dict, repr=False)
    options: Mapping[str, Any] = field(default_factory=dict, repr=False)
    progress_callback: AutomationProgressCallback | None = field(
        default=None,
        repr=False,
        compare=False,
    )

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
        if self.progress_callback is not None and not callable(
            self.progress_callback
        ):
            raise AutomationCycleContractError(
                "automation progress callback is invalid"
            )
        _validate_cycle_mapping(self.cursor, path="cursor")
        _validate_cycle_mapping(self.options, path="options")
        _assert_no_sensitive_keys(self.cursor, path="cursor")
        _assert_no_sensitive_keys(self.options, path="options")
        object.__setattr__(self, "cursor", deepcopy(dict(self.cursor)))
        object.__setattr__(self, "options", deepcopy(dict(self.options)))


@dataclass(frozen=True, slots=True)
class AutomationDelivery:
    """Slack이 렌더링하는 API domain 결과와 중복 방지 키다."""

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
    """Slack 발송 뒤 API domain hook이 소비하는 최소 receipt다."""

    delivery_id: str
    status: AutomationDeliveryStatus
    external_message_id: str = ""
    permalink: str = field(default="", repr=False)
    delivered_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AutomationCycleResult:
    """API cycle 실행 결과와 Slack 전달 payload 묶음이다."""

    cycle: AutomationCycleName
    outcome: AutomationCycleOutcome
    cursor: Mapping[str, Any] = field(default_factory=dict, repr=False)
    deliveries: tuple[AutomationDelivery, ...] = field(default_factory=tuple)
    metrics: Mapping[str, Any] = field(default_factory=dict, repr=False)
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
    raise AutomationCycleContractError(f"{path} contains a non-JSON value")


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
    """예외 원문·credential 필드를 delivery payload에서 제거한다."""

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


__all__ = [
    "AutomationCycleContractError",
    "AutomationCycleHandler",
    "AutomationCycleName",
    "AutomationCycleOutcome",
    "AutomationCycleRequest",
    "AutomationCycleResult",
    "AutomationDelivery",
    "AutomationDeliveryReceipt",
    "AutomationDeliveryStatus",
    "AutomationProgressCallback",
]
