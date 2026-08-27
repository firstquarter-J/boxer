from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import stat
import tempfile
import threading
from typing import Any, Callable, Iterator, Literal, Mapping, TypeVar
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from boxer_company.automation import (
    AutomationCycleContractError,
    AutomationCycleName,
    AutomationCycleRequest,
    AutomationCycleResult,
    AutomationCycleService,
    AutomationDelivery,
    AutomationDeliveryReceipt,
)


_CYCLE_KEY_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,191}$"
_IDENTIFIER_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
_DELIVERY_ID_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
_MAX_RECEIPTS = 100
_MAX_ACKNOWLEDGED_IDS = 500
_STATE_VERSION = 1
_MAX_STATE_BYTES = 16 * 1024 * 1024
_STATE_KEY_RE = re.compile(r"^[0-9a-f]{64}$")
_SLACK_CHANNEL_ID_RE = re.compile(r"^[CGD][A-Z0-9]{5,31}$")
_MISSING_STATE_DIGEST = hashlib.sha256(
    b"boxer.automation-state.missing.v1"
).hexdigest()
_CONTINUOUS_CYCLES = frozenset(
    {
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    }
)
_DAILY_OPTION_KEYS = (
    "autoUpdateAgent",
    "autoUpdateBoxFree",
    "autoUpdateBoxPaid",
    "autoCleanupTrashCan",
    "autoPowerOff",
)
_KST = ZoneInfo("Asia/Seoul")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class AutomationDeliveryReceiptInput(_StrictModel):
    deliveryId: str = Field(
        min_length=1,
        max_length=128,
        pattern=_DELIVERY_ID_PATTERN,
    )
    status: Literal["sent", "failed"]
    externalMessageId: str = Field(default="", max_length=256)
    permalink: str = Field(default="", max_length=2_048)
    deliveredAt: datetime | None = None

    @field_validator("externalMessageId")
    @classmethod
    def _validate_external_message_id(cls, value: str) -> str:
        normalized = value.strip()
        if normalized and not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}",
            normalized,
        ):
            raise ValueError("external message id is invalid")
        return normalized

    @field_validator("permalink")
    @classmethod
    def _validate_permalink(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            return ""
        parsed = urlsplit(normalized)
        if not (
            parsed.scheme == "https"
            and parsed.hostname
            and parsed.username is None
            and parsed.password is None
            and not parsed.query
            and not parsed.fragment
        ):
            raise ValueError("delivery permalink is invalid")
        return normalized


class AutomationCycleInput(_StrictModel):
    tenantId: str = Field(
        min_length=1,
        max_length=256,
        pattern=_IDENTIFIER_PATTERN,
    )
    cycle: Literal[
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    ]
    cycleKey: str = Field(
        min_length=1,
        max_length=192,
        pattern=_CYCLE_KEY_PATTERN,
    )
    scheduledAt: datetime
    options: dict[str, Any] = Field(default_factory=dict)
    deliveryReceipts: list[AutomationDeliveryReceiptInput] = Field(
        default_factory=list,
        max_length=_MAX_RECEIPTS,
    )
    ackOnly: bool = False

    @field_validator("scheduledAt")
    @classmethod
    def _validate_scheduled_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("scheduledAt must be timezone-aware")
        return value

    @model_validator(mode="after")
    def _validate_receipts(self) -> "AutomationCycleInput":
        ids = [item.deliveryId for item in self.deliveryReceipts]
        if len(ids) != len(set(ids)):
            raise ValueError("delivery receipts must be unique")
        allowed_options: dict[str, frozenset[str]] = {
            "weekly_recordings": frozenset(),
            "daily_device_round": frozenset(
                {
                    "autoUpdateAgent",
                    "autoUpdateBoxFree",
                    "autoUpdateBoxPaid",
                    "autoCleanupTrashCan",
                    "autoPowerOff",
                }
            ),
            # health delivery override와 fingerprint dedupe는 API durable
            # cursor가 소유하므로 Slack 요청 option으로 바꿀 수 없다.
            "device_health_monitor": frozenset(),
            "device_notification_alert": frozenset(),
            "sms_delivery": frozenset(),
        }
        if set(self.options) - allowed_options[self.cycle]:
            raise ValueError("automation options contain unsupported fields")
        if any(not isinstance(value, bool) for value in self.options.values()):
            raise ValueError("automation options must be booleans")
        # handler 진입 전 cycle별 mutation option을 고정해 잘못된 입력이
        # durable in-flight 상태를 만들지 않게 한다.
        return self

    def to_trigger(self, request_id: str) -> "AutomationCycleTrigger":
        return AutomationCycleTrigger(
            request_id=request_id,
            tenant_id=self.tenantId,
            cycle=self.cycle,
            cycle_key=self.cycleKey,
            scheduled_at=self.scheduledAt,
            options=dict(self.options),
            delivery_receipts=tuple(
                AutomationDeliveryReceipt(
                    delivery_id=item.deliveryId,
                    status=item.status,
                    external_message_id=item.externalMessageId,
                    permalink=item.permalink,
                    delivered_at=item.deliveredAt,
                )
                for item in self.deliveryReceipts
            ),
            ack_only=self.ackOnly,
        )


class AutomationDeliveryOutput(_StrictModel):
    deliveryId: str
    kind: str
    payload: dict[str, Any]


class AutomationCycleOutput(_StrictModel):
    requestId: str
    cycle: str
    outcome: Literal["completed", "no_change"]
    deliveries: list[AutomationDeliveryOutput]
    metrics: dict[str, Any]
    autoRetryAllowed: Literal[False] = False


@dataclass(frozen=True, slots=True)
class AutomationCycleTrigger:
    request_id: str
    tenant_id: str
    cycle: AutomationCycleName
    cycle_key: str
    scheduled_at: datetime
    options: Mapping[str, Any] = field(default_factory=dict, repr=False)
    delivery_receipts: tuple[AutomationDeliveryReceipt, ...] = field(
        default_factory=tuple,
        repr=False,
    )
    ack_only: bool = False
    # HTTP compatibility trigger에는 없고 API scheduler가 만든 새 run에만
    # 있다. pending이 생길 때 이 snapshot을 durable transport 정본으로 쓴다.
    delivery_target: Mapping[str, Any] | None = field(
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        if self.delivery_target is None:
            return
        _validate_delivery_target(self.delivery_target)
        object.__setattr__(
            self,
            "delivery_target",
            {
                "channelId": str(self.delivery_target["channelId"]),
                "conversation": dict(
                    self.delivery_target.get("conversation") or {}
                ),
            },
        )


class AutomationCycleUncertainError(RuntimeError):
    """앞선 mutation cycle의 완료 여부를 몰라 자동 재실행을 막는 상태다."""


@dataclass(frozen=True, slots=True)
class AutomationStateSnapshot:
    """보호된 automation 파일 한 revision의 raw document다."""

    document: dict[str, Any]
    digest: str
    exists: bool

    def cycle(self, key: str) -> tuple[bool, dict[str, Any]]:
        cycles = self.document["cycles"]
        if key not in cycles:
            return False, {}
        return True, dict(cycles[key])


@dataclass(frozen=True, slots=True)
class _CoordinatorTransition:
    """한 atomic state 전이가 예약한 다음 coordinator 동작이다."""

    kind: Literal["return", "run", "acknowledge"]
    result: AutomationCycleResult | None = None
    request: AutomationCycleRequest | None = None
    receipts: tuple[AutomationDeliveryReceipt, ...] = field(
        default_factory=tuple,
        repr=False,
    )
    marker: Mapping[str, Any] = field(default_factory=dict, repr=False)


_MutationResult = TypeVar("_MutationResult")
_STATE_PATH_LOCKS_GUARD = threading.Lock()
_STATE_PATH_LOCKS: dict[str, threading.RLock] = {}


class JsonAutomationCycleStateStore:
    """보호된 JSON revision과 host-local lock을 공유하는 cycle state다."""

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path).expanduser()
        if not self._path.is_absolute() or self._path == Path("/"):
            raise AutomationCycleContractError(
                "automation state path must be an explicit absolute file"
            )
        # 같은 process에서 새 store instance를 만들어도 file CAS 앞의 thread
        # 구간까지 한 lock으로 묶는다. 별도 process는 adjacent flock이 막는다.
        normalized_path = str(self._path.absolute())
        with _STATE_PATH_LOCKS_GUARD:
            self._lock = _STATE_PATH_LOCKS.setdefault(
                normalized_path,
                threading.RLock(),
            )

    def load(self, key: str) -> dict[str, Any]:
        self._validate_state_key(key)
        with self.locked_snapshot() as snapshot:
            _, state = snapshot.cycle(key)
            return state

    def save(self, key: str, state: Mapping[str, Any]) -> None:
        self.mutate_cycle(key, lambda _exists, _state: (dict(state), None))

    def mutate_cycle(
        self,
        key: str,
        updater: Callable[
            [bool, dict[str, Any]],
            tuple[Mapping[str, Any], _MutationResult],
        ],
        *,
        expected_document_digest: str | None = None,
    ) -> _MutationResult:
        """한 flock 안에서 exact revision을 읽고 cycle 하나를 원자 교체한다."""

        self._validate_state_key(key)
        with self._lock:
            with self._exclusive_file_lock():
                snapshot = self._load_snapshot_unlocked()
                if (
                    expected_document_digest is not None
                    and snapshot.digest != expected_document_digest
                ):
                    raise AutomationCycleContractError(
                        "automation state changed"
                    )
                exists, current = snapshot.cycle(key)
                next_state, result = updater(exists, current)
                if not isinstance(next_state, Mapping):
                    raise AutomationCycleContractError(
                        "automation state update is invalid"
                    )
                if exists and dict(next_state) == current:
                    return result
                cycles = dict(snapshot.document["cycles"])
                cycles[key] = dict(next_state)
                self._write_document_unlocked(
                    {"version": _STATE_VERSION, "cycles": cycles}
                )
                return result

    def mutate_cycles(
        self,
        keys: tuple[str, ...],
        updater: Callable[
            [dict[str, tuple[bool, dict[str, Any]]]],
            tuple[Mapping[str, Mapping[str, Any]], _MutationResult],
        ],
        *,
        expected_document_digest: str | None = None,
    ) -> _MutationResult:
        """한 document revision에서 여러 cycle을 한 번에 CAS 교체한다."""

        if not keys or len(set(keys)) != len(keys):
            raise AutomationCycleContractError(
                "automation state keys are invalid"
            )
        for key in keys:
            self._validate_state_key(key)
        with self._lock:
            with self._exclusive_file_lock():
                snapshot = self._load_snapshot_unlocked()
                if (
                    expected_document_digest is not None
                    and snapshot.digest != expected_document_digest
                ):
                    raise AutomationCycleContractError(
                        "automation state changed"
                    )
                current = {
                    key: snapshot.cycle(key)
                    for key in keys
                }
                next_states, result = updater(current)
                if set(next_states) != set(keys) or any(
                    not isinstance(next_states.get(key), Mapping)
                    for key in keys
                ):
                    raise AutomationCycleContractError(
                        "automation state update is invalid"
                    )
                if all(
                    exists and dict(next_states[key]) == state
                    for key, (exists, state) in current.items()
                ):
                    return result
                cycles = dict(snapshot.document["cycles"])
                for key in keys:
                    cycles[key] = dict(next_states[key])
                # health/notification cutover seed가 서로 다른 revision에
                # 남으면 cursor gap이 생기므로 문서 write는 정확히 한 번이다.
                self._write_document_unlocked(
                    {"version": _STATE_VERSION, "cycles": cycles}
                )
                return result

    @contextmanager
    def locked_snapshot(self) -> Iterator[AutomationStateSnapshot]:
        """runtime/recovery가 같은 flock 아래 한 raw revision을 검사하게 한다."""

        with self._lock:
            with self._exclusive_file_lock():
                yield self._load_snapshot_unlocked()

    def _load_snapshot_unlocked(self) -> AutomationStateSnapshot:
        try:
            file_stat = self._path.lstat()
        except FileNotFoundError:
            return AutomationStateSnapshot(
                document={"version": _STATE_VERSION, "cycles": {}},
                digest=_MISSING_STATE_DIGEST,
                exists=False,
            )
        except OSError as exc:
            raise AutomationCycleContractError(
                "automation state is unreadable"
            ) from exc
        if stat.S_ISLNK(file_stat.st_mode):
            raise AutomationCycleContractError(
                "automation state is not protected"
            )
        flags = os.O_RDONLY
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        try:
            descriptor = os.open(self._path, flags)
        except OSError as exc:
            raise AutomationCycleContractError(
                "automation state is unreadable"
            ) from exc
        chunks: list[bytes] = []
        try:
            before = os.fstat(descriptor)
            if (
                not stat.S_ISREG(before.st_mode)
                or before.st_uid not in {0, os.geteuid()}
                or stat.S_IMODE(before.st_mode) != 0o600
                or before.st_size > _MAX_STATE_BYTES
            ):
                raise AutomationCycleContractError(
                    "automation state is not protected"
                )
            size = 0
            while True:
                chunk = os.read(
                    descriptor,
                    min(64 * 1024, _MAX_STATE_BYTES + 1 - size),
                )
                if not chunk:
                    break
                chunks.append(chunk)
                size += len(chunk)
                if size > _MAX_STATE_BYTES:
                    raise AutomationCycleContractError(
                        "automation state is too large"
                    )
            after = os.fstat(descriptor)
            if (
                before.st_dev,
                before.st_ino,
                before.st_size,
                before.st_mtime_ns,
            ) != (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mtime_ns,
            ):
                raise AutomationCycleContractError(
                    "automation state changed while reading"
                )
        finally:
            os.close(descriptor)
        raw = b"".join(chunks)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except Exception as exc:
            # 손상된 state를 빈 값으로 덮으면 mutation을 재실행할 수 있어
            # 오류 타입만 올리고 fail closed한다.
            raise AutomationCycleContractError(
                "automation state is unreadable"
            ) from exc
        if (
            not isinstance(payload, dict)
            or set(payload) != {"version", "cycles"}
            or type(payload.get("version")) is not int
            or payload.get("version") != _STATE_VERSION
            or not isinstance(payload.get("cycles"), dict)
        ):
            raise AutomationCycleContractError(
                "automation state has an invalid format"
            )
        cycles = payload["cycles"]
        if any(
            not isinstance(key, str)
            or not _STATE_KEY_RE.fullmatch(key)
            or not isinstance(value, dict)
            for key, value in cycles.items()
        ):
            # target 값이 list/null이어도 absent state로 정규화하지 않는다.
            raise AutomationCycleContractError(
                "automation state has an invalid cycle"
            )
        return AutomationStateSnapshot(
            document={
                "version": _STATE_VERSION,
                "cycles": {
                    key: dict(value) for key, value in cycles.items()
                },
            },
            digest=hashlib.sha256(raw).hexdigest(),
            exists=True,
        )

    def _write_document_unlocked(self, payload: Mapping[str, Any]) -> None:
        parent = self._path.parent
        self._validate_protected_parent(parent)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{self._path.name}.",
            suffix=".tmp",
            dir=str(parent),
        )
        temporary_path = Path(temporary_name)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                os.fchmod(handle.fileno(), 0o600)
                json.dump(
                    payload,
                    handle,
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                )
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary_path, self._path)
            directory_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if temporary_path.exists():
                temporary_path.unlink()

    @contextmanager
    def _exclusive_file_lock(self) -> Iterator[None]:
        parent = self._path.parent
        self._validate_protected_parent(parent)
        lock_path = self._path.with_name(f".{self._path.name}.lock")
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        created = False
        try:
            descriptor = os.open(lock_path, flags, 0o600)
            created = True
        except FileExistsError:
            flags &= ~os.O_EXCL
            try:
                descriptor = os.open(lock_path, flags, 0o600)
            except OSError as exc:
                raise AutomationCycleContractError(
                    "automation state lock is unavailable"
                ) from exc
        except OSError as exc:
            raise AutomationCycleContractError(
                "automation state lock is unavailable"
            ) from exc
        try:
            if created:
                os.fchmod(descriptor, 0o600)
            lock_stat = os.fstat(descriptor)
            if (
                not stat.S_ISREG(lock_stat.st_mode)
                or lock_stat.st_uid not in {0, os.geteuid()}
                or stat.S_IMODE(lock_stat.st_mode) != 0o600
            ):
                raise AutomationCycleContractError(
                    "automation state lock is not protected"
                )
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)

    @staticmethod
    def _validate_state_key(key: str) -> None:
        if not isinstance(key, str) or not _STATE_KEY_RE.fullmatch(key):
            raise AutomationCycleContractError(
                "automation state key is invalid"
            )

    @staticmethod
    def _validate_protected_parent(parent: Path) -> None:
        current = parent
        while True:
            try:
                current_stat = current.lstat()
            except OSError as exc:
                raise AutomationCycleContractError(
                    "automation state parent is invalid"
                ) from exc
            if (
                stat.S_ISLNK(current_stat.st_mode)
                or not stat.S_ISDIR(current_stat.st_mode)
                or current_stat.st_uid not in {0, os.geteuid()}
                or current_stat.st_mode & 0o022
            ):
                raise AutomationCycleContractError(
                    "automation state parent is not protected"
                )
            if current.parent == current:
                break
            current = current.parent


class DurableAutomationCycleCoordinator:
    """실행 전 in-flight를 저장하고 delivery ack까지 보존하는 최소 coordinator다."""

    def __init__(
        self,
        service: AutomationCycleService,
        state_store: JsonAutomationCycleStateStore,
        *,
        logger: logging.Logger | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._service = service
        self._state_store = state_store
        self._logger = logger or logging.getLogger(__name__)
        self._clock = clock or (lambda: datetime.now(timezone.utc))

    def run(self, trigger: AutomationCycleTrigger) -> AutomationCycleResult:
        # Slack scheduler가 확정한 기존 실행 시각·옵션을 domain 입력으로
        # 유지하고, API는 형식과 cycle 논리만 먼저 검증한다.
        admission_now = self._clock()
        validate_automation_trigger_admission(trigger)
        if trigger.cycle not in self._service.cycle_names:
            raise AutomationCycleContractError(
                "automation cycle handler is not configured"
            )
        state_key = _build_state_key(trigger)
        while True:
            # 현재 revision 판정과 marker 예약을 한 flock 안에서 끝내 별도
            # coordinator/process가 같은 외부 mutation을 함께 시작하지 못한다.
            transition = self._state_store.mutate_cycle(
                state_key,
                lambda exists, current: self._reserve_transition(
                    exists=exists,
                    current=current,
                    state_key=state_key,
                    trigger=trigger,
                    admission_now=admission_now,
                ),
            )
            if transition.kind == "return":
                if transition.result is None:
                    raise AutomationCycleContractError(
                        "automation coordinator transition is invalid"
                    )
                return transition.result
            if transition.request is None or not transition.marker:
                raise AutomationCycleContractError(
                    "automation coordinator transition is invalid"
                )

            if transition.kind == "acknowledge":
                # provider/Sheet 후속 mutation 중에는 file lock을 잡지 않는다.
                # 실패하면 finalize가 실행되지 않아 exact marker가 남는다.
                updated_cursor = self._service.acknowledge(
                    transition.request,
                    transition.receipts,
                )
                self._finalize_acknowledgement(
                    state_key=state_key,
                    trigger=trigger,
                    expected_marker=transition.marker,
                    updated_cursor=updated_cursor,
                )
                # ACK가 닫은 최신 state에서 ackOnly/pending/completed 판정을
                # 다시 원자 수행한다. 이미 적용한 receipt는 hook을 재호출하지 않는다.
                continue

            if transition.kind != "run":
                raise AutomationCycleContractError(
                    "automation coordinator transition is invalid"
                )
            # 외부 조회·장비/provider mutation은 durable marker가 보이는 동안
            # 실행하되 flock은 잡지 않는다. 실패 시 marker를 보존한다.
            result = self._service.run(transition.request)
            completion_now = self._clock()
            if completion_now.tzinfo is None:
                raise AutomationCycleContractError(
                    "automation completion clock is invalid"
                )
            self._finalize_run(
                state_key=state_key,
                trigger=trigger,
                completion_now=completion_now,
                expected_marker=transition.marker,
                result=result,
            )
            self._logger.info(
                "Automation coordinator stored result cycle=%s outcome=%s deliveries=%s",
                trigger.cycle,
                result.outcome,
                len(result.deliveries),
            )
            return result

    def _reserve_transition(
        self,
        *,
        exists: bool,
        current: dict[str, Any],
        state_key: str,
        trigger: AutomationCycleTrigger,
        admission_now: datetime,
    ) -> tuple[Mapping[str, Any], _CoordinatorTransition]:
        del exists
        state = dict(current)
        if trigger.cycle == "device_health_monitor":
            # health pending replay와 ackOnly도 API-owned seed에서만
            # 유효하다. raw durable cursor를 marker 판정보다 먼저 검증한다.
            raw_health_cursor = state.get("cursor")
            if not isinstance(raw_health_cursor, Mapping):
                raise AutomationCycleContractError(
                    "device health monitor API state seed is required"
                )
            self._service.validate(
                AutomationCycleRequest(
                    request_id=trigger.request_id,
                    tenant_id=trigger.tenant_id,
                    cycle=trigger.cycle,
                    scheduled_at=trigger.scheduled_at,
                    cursor=raw_health_cursor,
                    options=dict(trigger.options),
                )
            )
        if "inFlight" in state or "ackInFlight" in state:
            # 값이 malformed/falsey여도 완료 여부를 증명할 수 없으므로
            # 운영자 exact-marker resolve 전에는 fail-closed한다.
            raise AutomationCycleUncertainError(
                "automation cycle execution is uncertain"
            )

        sent_receipts = _unapplied_sent_receipts(
            state,
            trigger.delivery_receipts,
        )
        if sent_receipts and self._service.has_acknowledger(trigger.cycle):
            acknowledgement_request = AutomationCycleRequest(
                request_id=trigger.request_id,
                tenant_id=trigger.tenant_id,
                cycle=trigger.cycle,
                # 기존 local reporter가 사용하던 poll 시각을 후속
                # Sheet/provider 처리에도 그대로 전달한다.
                scheduled_at=trigger.scheduled_at,
                cursor=dict(state.get("cursor") or {}),
                options=dict(trigger.options),
            )
            self._service.validate(acknowledgement_request)
            marker = {
                "requestId": trigger.request_id,
                "deliveryIds": [
                    receipt.delivery_id for receipt in sent_receipts
                ],
                "startedAt": admission_now.isoformat(),
            }
            return {
                **state,
                "ackInFlight": marker,
            }, _CoordinatorTransition(
                kind="acknowledge",
                request=acknowledgement_request,
                receipts=sent_receipts,
                marker=marker,
            )

        # receipt-only와 replay 판정도 같은 revision에 반영해 concurrent
        # poll이 이미 ACK한 delivery나 stale response cache를 되살리지 못한다.
        state = _apply_delivery_receipts(
            state,
            trigger.delivery_receipts,
        )
        if (
            not trigger.delivery_receipts
            and not trigger.ack_only
            and state.get("lastRequestId") == trigger.request_id
            and isinstance(state.get("lastResult"), dict)
        ):
            return state, _CoordinatorTransition(
                kind="return",
                result=_restore_result(state["lastResult"]),
            )

        pending_result = _pending_result(trigger.cycle, state)
        if trigger.ack_only or pending_result.deliveries:
            return state, _CoordinatorTransition(
                kind="return",
                result=pending_result,
            )
        if state.get("cycleCompleted"):
            return state, _CoordinatorTransition(
                kind="return",
                result=AutomationCycleResult(
                    cycle=trigger.cycle,
                    outcome="no_change",
                    cursor={},
                    metrics={"deliveryCount": 0},
                ),
            )

        # cursor/options의 일반 계약과 handler별 option 계약을 durable
        # in-flight보다 먼저 끝내 입력 오류가 cycle을 잠그지 않게 한다.
        domain_cursor = dict(state.get("cursor") or {})
        if trigger.cycle == "daily_device_round":
            # 자정을 넘기는 야간 창도 server-derived cycle key 날짜를
            # delivery id와 domain cursor가 그대로 공유하게 한다.
            domain_cursor.setdefault(
                "windowKey",
                trigger.cycle_key.removeprefix("daily:"),
            )
        marker = {
            "requestId": trigger.request_id,
            "startedAt": admission_now.isoformat(),
        }
        scheduler_metadata = _scheduler_transport_metadata(trigger)
        if scheduler_metadata is not None:
            # exact marker에도 목적지 snapshot을 넣어 외부 실행 중 설정이
            # 바뀌어도 finalize가 다른 channel로 pending을 만들지 못하게 한다.
            marker["deliveryTarget"] = dict(
                scheduler_metadata["deliveryTarget"]
            )
        domain_request = AutomationCycleRequest(
            request_id=trigger.request_id,
            tenant_id=trigger.tenant_id,
            cycle=trigger.cycle,
            scheduled_at=trigger.scheduled_at,
            cursor=domain_cursor,
            options=dict(trigger.options),
            progress_callback=(
                (
                    lambda progress_cursor: self._checkpoint_progress(
                        state_key=state_key,
                        expected_marker=marker,
                        cursor=progress_cursor,
                    )
                )
                if trigger.cycle == "daily_device_round"
                else None
            ),
        )
        self._service.validate(domain_request)
        reserved_state = {
            **state,
            "inFlight": marker,
        }
        if scheduler_metadata is not None:
            reserved_state.update(scheduler_metadata)
        return reserved_state, _CoordinatorTransition(
            kind="run",
            request=domain_request,
            marker=marker,
        )

    def _finalize_acknowledgement(
        self,
        *,
        state_key: str,
        trigger: AutomationCycleTrigger,
        expected_marker: Mapping[str, Any],
        updated_cursor: Mapping[str, Any],
    ) -> None:
        def _update(
            exists: bool,
            current: dict[str, Any],
        ) -> tuple[Mapping[str, Any], None]:
            self._require_exact_marker(
                exists=exists,
                current=current,
                marker_name="ackInFlight",
                expected_marker=expected_marker,
                phase="acknowledgement",
            )
            next_state = {
                **current,
                "cursor": dict(updated_cursor),
                "domainCycleComplete": bool(
                    updated_cursor.get("cycleCompleted")
                ),
            }
            next_state = _apply_delivery_receipts(
                next_state,
                trigger.delivery_receipts,
            )
            next_state.pop("ackInFlight", None)
            return next_state, None

        self._state_store.mutate_cycle(state_key, _update)

    def _finalize_run(
        self,
        *,
        state_key: str,
        trigger: AutomationCycleTrigger,
        completion_now: datetime,
        expected_marker: Mapping[str, Any],
        result: AutomationCycleResult,
    ) -> None:
        # 직렬화 실패도 외부 실행의 완료 여부를 자동 단정하지 않도록
        # marker를 건드리기 전에 결과 payload를 완전히 준비한다.
        serialized_result = _serialize_result_state(result)
        pending_deliveries = [
            _serialize_delivery(delivery) for delivery in result.deliveries
        ]
        domain_complete = bool(result.cursor.get("cycleCompleted"))
        scheduler_metadata = _scheduler_transport_metadata(trigger)
        pending_metadata: dict[str, Any] = {}
        if pending_deliveries and scheduler_metadata is not None:
            pending_ids = tuple(
                str(item["deliveryId"]) for item in pending_deliveries
            )
            pending_metadata = {
                **scheduler_metadata,
                "pendingScheduledAt": trigger.scheduled_at.isoformat(),
                "pendingCreatedAt": completion_now.isoformat(),
                "pendingBatchId": _build_delivery_batch_id(
                    tenant_id=trigger.tenant_id,
                    cycle=trigger.cycle,
                    cycle_key=trigger.cycle_key,
                    delivery_ids=pending_ids,
                ),
            }

        def _update(
            exists: bool,
            current: dict[str, Any],
        ) -> tuple[Mapping[str, Any], None]:
            self._require_exact_marker(
                exists=exists,
                current=current,
                marker_name="inFlight",
                expected_marker=expected_marker,
                phase="execution",
            )
            next_state = {
                **current,
                "cursor": dict(result.cursor),
                "pendingDeliveries": pending_deliveries,
                "domainCycleComplete": domain_complete,
                "cycleCompleted": domain_complete and not pending_deliveries,
                "lastRequestId": trigger.request_id,
                "lastResult": serialized_result,
                # fixed-delay는 실제 handler 완료 시각부터 계산한다.
                "lastCompletedAt": completion_now.isoformat(),
                **pending_metadata,
            }
            if not pending_deliveries:
                for key in (
                    "pendingScheduledAt",
                    "pendingCreatedAt",
                    "pendingBatchId",
                ):
                    next_state.pop(key, None)
            next_state.pop("inFlight", None)
            return next_state, None

        self._state_store.mutate_cycle(state_key, _update)

    @staticmethod
    def _require_exact_marker(
        *,
        exists: bool,
        current: Mapping[str, Any],
        marker_name: Literal["inFlight", "ackInFlight"],
        expected_marker: Mapping[str, Any],
        phase: str,
    ) -> None:
        actual_marker = current.get(marker_name)
        opposite_marker = (
            "ackInFlight" if marker_name == "inFlight" else "inFlight"
        )
        if (
            not exists
            or marker_name not in current
            or not isinstance(actual_marker, Mapping)
            or dict(actual_marker) != dict(expected_marker)
            or opposite_marker in current
        ):
            raise AutomationCycleUncertainError(
                f"automation cycle state changed during {phase}"
            )

    def _checkpoint_progress(
        self,
        *,
        state_key: str,
        expected_marker: Mapping[str, Any],
        cursor: Mapping[str, Any],
    ) -> None:
        """현재 in-flight와 같은 실행의 active cursor만 원자 저장한다."""

        def _update(
            exists: bool,
            current: dict[str, Any],
        ) -> tuple[Mapping[str, Any], None]:
            self._require_exact_marker(
                exists=exists,
                current=current,
                marker_name="inFlight",
                expected_marker=expected_marker,
                phase="progress",
            )
            return {
                **current,
                # hospital_started와 각 device_started마다 기존 local
                # reporter와 같은 active 필드 snapshot을 교체한다.
                "cursor": dict(cursor),
            }, None

        self._state_store.mutate_cycle(state_key, _update)


def serialize_automation_cycle_result(
    result: AutomationCycleResult,
    request_id: str,
) -> dict[str, Any]:
    payload = AutomationCycleOutput(
        requestId=request_id,
        cycle=result.cycle,
        outcome=result.outcome,
        deliveries=[
            AutomationDeliveryOutput(
                deliveryId=delivery.delivery_id,
                kind=delivery.kind,
                payload=dict(delivery.payload),
            )
            for delivery in result.deliveries
        ],
        metrics=dict(result.metrics),
        autoRetryAllowed=False,
    )
    return payload.model_dump(mode="json")


def validate_automation_trigger_admission(
    trigger: AutomationCycleTrigger,
) -> None:
    """기존 Slack scheduler의 cycle identity와 option 형식만 검증한다."""

    if trigger.scheduled_at.tzinfo is None:
        raise AutomationCycleContractError(
            "automation schedule must be timezone-aware"
        )
    _validate_delivery_target(trigger.delivery_target)

    if trigger.ack_only:
        # Slack 발송 직후 재시작하거나 window가 넘어가도 과거 pending
        # delivery를 닫을 수 있어야 한다. ackOnly는 domain run을 호출하지
        # 않고 아래 exact state의 pending delivery ID를 다시 대조한다.
        _validate_ack_cycle_key(trigger.cycle, trigger.cycle_key)
        return

    if trigger.cycle in _CONTINUOUS_CYCLES:
        if trigger.cycle_key != "continuous" or trigger.options:
            raise AutomationCycleContractError(
                "continuous automation cycle key is invalid"
            )
        return

    local_scheduled_at = trigger.scheduled_at.astimezone(_KST)
    if trigger.cycle == "weekly_recordings":
        current_week_start = local_scheduled_at.date() - timedelta(
            days=local_scheduled_at.weekday()
        )
        target_week_start = current_week_start - timedelta(days=7)
        expected_key = f"weekly:{target_week_start.isoformat()}"
        if trigger.cycle_key != expected_key or trigger.options:
            raise AutomationCycleContractError(
                "weekly automation cycle key is invalid"
            )
        return

    if trigger.cycle != "daily_device_round":
        raise AutomationCycleContractError(
            "unsupported automation cycle"
        )
    try:
        window_date = date.fromisoformat(
            trigger.cycle_key.removeprefix("daily:")
        )
    except ValueError as exc:
        raise AutomationCycleContractError(
            "daily automation cycle key is invalid"
        ) from exc
    # 기존 overnight window는 자정 뒤 poll도 전날 cycle key를 쓴다.
    current_date = local_scheduled_at.date()
    previous_window_after_midnight = bool(
        window_date == current_date - timedelta(days=1)
        and local_scheduled_at.hour < 12
    )
    if window_date != current_date and not previous_window_after_midnight:
        raise AutomationCycleContractError(
            "daily automation cycle key is invalid"
        )
    trigger_options = dict(trigger.options)
    if (
        set(trigger_options) != set(_DAILY_OPTION_KEYS)
        or any(type(value) is not bool for value in trigger_options.values())
    ):
        raise AutomationCycleContractError(
            "daily automation options are invalid"
        )


def _validate_ack_cycle_key(
    cycle: AutomationCycleName,
    cycle_key: str,
) -> None:
    if cycle in _CONTINUOUS_CYCLES:
        if cycle_key != "continuous":
            raise AutomationCycleContractError(
                "continuous automation cycle key is invalid"
            )
        return
    prefix = (
        "weekly:"
        if cycle == "weekly_recordings"
        else "daily:"
        if cycle == "daily_device_round"
        else ""
    )
    if not prefix or not cycle_key.startswith(prefix):
        raise AutomationCycleContractError(
            "automation acknowledgement cycle key is invalid"
        )
    try:
        cycle_date = date.fromisoformat(cycle_key.removeprefix(prefix))
    except ValueError as exc:
        raise AutomationCycleContractError(
            "automation acknowledgement cycle key is invalid"
        ) from exc
    if cycle == "weekly_recordings" and cycle_date.weekday() != 0:
        raise AutomationCycleContractError(
            "weekly automation cycle key is invalid"
        )


def _build_state_key(trigger: AutomationCycleTrigger) -> str:
    raw = "\0".join(
        (trigger.tenant_id, trigger.cycle, trigger.cycle_key)
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _scheduler_transport_metadata(
    trigger: AutomationCycleTrigger,
) -> dict[str, Any] | None:
    target = trigger.delivery_target
    if target is None:
        return None
    _validate_delivery_target(target)
    return {
        "identity": {
            "tenantId": trigger.tenant_id,
            "cycle": trigger.cycle,
            "cycleKey": trigger.cycle_key,
        },
        "deliveryTarget": {
            "channelId": str(target["channelId"]),
            "conversation": dict(target.get("conversation") or {}),
        },
    }


def _validate_delivery_target(value: Mapping[str, Any] | None) -> None:
    if value is None:
        return
    if not isinstance(value, Mapping) or set(value) != {
        "channelId",
        "conversation",
    }:
        raise AutomationCycleContractError(
            "automation delivery target is invalid"
        )
    channel_id = str(value.get("channelId") or "").strip()
    conversation = value.get("conversation")
    if (
        not _SLACK_CHANNEL_ID_RE.fullmatch(channel_id)
        or not isinstance(conversation, Mapping)
    ):
        raise AutomationCycleContractError(
            "automation delivery target is invalid"
        )


def _build_delivery_batch_id(
    *,
    tenant_id: str,
    cycle: str,
    cycle_key: str,
    delivery_ids: tuple[str, ...],
) -> str:
    raw = "\0".join(
        (tenant_id, cycle, cycle_key, *sorted(delivery_ids))
    )
    return f"batch:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def _apply_delivery_receipts(
    state: Mapping[str, Any],
    receipts: tuple[AutomationDeliveryReceipt, ...],
) -> dict[str, Any]:
    next_state = dict(state)
    pending = [
        dict(item)
        for item in (next_state.get("pendingDeliveries") or [])
        if isinstance(item, dict)
    ]
    acknowledged = list(next_state.get("acknowledgedDeliveryIds") or [])
    acknowledged_set = set(acknowledged)
    accepted_receipt = False
    pending_by_id = {
        str(item.get("deliveryId") or ""): item for item in pending
    }
    for receipt in receipts:
        if receipt.delivery_id in acknowledged_set:
            continue
        if receipt.delivery_id not in pending_by_id:
            raise AutomationCycleContractError(
                "delivery receipt does not match a pending delivery"
            )
        if receipt.status != "sent":
            # 실패 receipt는 delivery를 유지해 Slack adapter가 같은 payload를
            # 재전송할지 운영자가 결정할 수 있게 한다.
            continue
        pending_by_id.pop(receipt.delivery_id, None)
        acknowledged.append(receipt.delivery_id)
        acknowledged_set.add(receipt.delivery_id)
        accepted_receipt = True
    next_state["pendingDeliveries"] = list(pending_by_id.values())
    next_state["acknowledgedDeliveryIds"] = acknowledged[-_MAX_ACKNOWLEDGED_IDS:]
    if not next_state["pendingDeliveries"] and next_state.get(
        "domainCycleComplete"
    ):
        next_state["cycleCompleted"] = True
    if not next_state["pendingDeliveries"]:
        # batch metadata는 duplicate ACK 판정에 필요하지 않다. durable
        # acknowledged IDs만 남기고 새 batch와 섞이지 않게 즉시 닫는다.
        next_state.pop("pendingScheduledAt", None)
        next_state.pop("pendingCreatedAt", None)
        next_state.pop("pendingBatchId", None)
    if accepted_receipt:
        # 발송 완료된 과거 응답을 같은 request ID replay가 다시 delivery로
        # 내보내지 않도록 receipt 반영과 함께 response cache를 닫는다.
        next_state.pop("lastRequestId", None)
        next_state.pop("lastResult", None)
    return next_state


def _unapplied_sent_receipts(
    state: Mapping[str, Any],
    receipts: tuple[AutomationDeliveryReceipt, ...],
) -> tuple[AutomationDeliveryReceipt, ...]:
    pending_ids = {
        str(item.get("deliveryId") or "")
        for item in (state.get("pendingDeliveries") or [])
        if isinstance(item, dict)
    }
    acknowledged_ids = set(state.get("acknowledgedDeliveryIds") or [])
    selected: list[AutomationDeliveryReceipt] = []
    for receipt in receipts:
        if receipt.delivery_id in acknowledged_ids:
            continue
        if receipt.delivery_id not in pending_ids:
            raise AutomationCycleContractError(
                "delivery receipt does not match a pending delivery"
            )
        if receipt.status == "sent":
            selected.append(receipt)
    return tuple(selected)


def _pending_result(
    cycle: AutomationCycleName,
    state: Mapping[str, Any],
) -> AutomationCycleResult:
    deliveries = tuple(
        _restore_delivery(item)
        for item in (state.get("pendingDeliveries") or [])
        if isinstance(item, dict)
    )
    return AutomationCycleResult(
        cycle=cycle,
        outcome="completed" if deliveries else "no_change",
        cursor={},
        deliveries=deliveries,
        metrics={"deliveryCount": len(deliveries)},
    )


def _serialize_delivery(delivery: AutomationDelivery) -> dict[str, Any]:
    return {
        "deliveryId": delivery.delivery_id,
        "kind": delivery.kind,
        "payload": dict(delivery.payload),
    }


def _restore_delivery(payload: Mapping[str, Any]) -> AutomationDelivery:
    return AutomationDelivery(
        delivery_id=str(payload.get("deliveryId") or ""),
        kind=str(payload.get("kind") or ""),
        payload=dict(payload.get("payload") or {}),
    )


def _serialize_result_state(result: AutomationCycleResult) -> dict[str, Any]:
    return {
        "cycle": result.cycle,
        "outcome": result.outcome,
        "cursor": dict(result.cursor),
        "deliveries": [
            _serialize_delivery(delivery) for delivery in result.deliveries
        ],
        "metrics": dict(result.metrics),
    }


def _restore_result(payload: Mapping[str, Any]) -> AutomationCycleResult:
    return AutomationCycleResult(
        cycle=str(payload.get("cycle") or ""),  # type: ignore[arg-type]
        outcome=str(payload.get("outcome") or "no_change"),  # type: ignore[arg-type]
        cursor=dict(payload.get("cursor") or {}),
        deliveries=tuple(
            _restore_delivery(item)
            for item in (payload.get("deliveries") or [])
            if isinstance(item, dict)
        ),
        metrics=dict(payload.get("metrics") or {}),
    )


__all__ = [
    "AutomationCycleInput",
    "AutomationCycleOutput",
    "AutomationCycleTrigger",
    "AutomationCycleUncertainError",
    "AutomationStateSnapshot",
    "AutomationDeliveryOutput",
    "AutomationDeliveryReceipt",
    "AutomationDeliveryReceiptInput",
    "DurableAutomationCycleCoordinator",
    "JsonAutomationCycleStateStore",
    "serialize_automation_cycle_result",
    "validate_automation_trigger_admission",
]
