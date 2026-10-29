"""Slack 원본 무효화 식별자만 보관하는 영속 전송 대기열."""

from __future__ import annotations

import fcntl
import json
import os
import re
import stat
import tempfile
import threading
from collections.abc import Mapping
from contextlib import contextmanager
from decimal import Decimal
from pathlib import Path
from typing import Any

from boxer_company.memory.contracts import canonical_digest
from boxer_company.protected_json import (
    ProtectedJsonFileError,
    _validate_protected_parent_chain,
    create_protected_json_file,
)

_LOCK = threading.RLock()
_MAX_EVENTS = 10_000
_MAX_BYTES = 8 * 1024 * 1024
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$")
_TIMESTAMP = re.compile(r"^\d{10,16}\.\d{6}$")
_DIGEST = re.compile(r"^[a-f0-9]{64}$")
_EVENT_FIELDS = frozenset({"tenantId", "channelId", "messageTs", "eventId", "eventType", "revisionHash"})
_RAW_FIELDS = frozenset({"rawEventType", "threadTs", "eventTs", "authorId"})


class MemoryEventOutboxError(RuntimeError):
    """원문이나 경로를 포함하지 않는 기억 전송 상태 오류."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class MemorySourceEventOutbox:
    def __init__(self, path: str | Path, *, max_events: int = _MAX_EVENTS) -> None:
        self.path = Path(path).expanduser()
        self.max_events = min(_MAX_EVENTS, max(1, max_events))
        self._validate_path()
        # create-only 초기화는 기존 receipt를 덮지 않으며 재시작 때 전체 문서를 검증한다.
        with self._locked():
            if not self.path.exists():
                try:
                    create_protected_json_file(self.path, {"version": 1, "events": {}}, label="memory event outbox")
                except ProtectedJsonFileError as exc:
                    raise MemoryEventOutboxError("memory_event_state_create_failed") from exc
            self._load()

    def enqueue(self, event: Mapping[str, Any]) -> None:
        payload = _validated_event(event)
        key = _event_key(payload)
        with self._locked():
            state = self._load()
            previous = state["events"].get(key)
            if previous is not None:
                if previous != payload:
                    raise MemoryEventOutboxError("memory_event_identity_conflict")
                return
            if len(state["events"]) >= self.max_events:
                raise MemoryEventOutboxError("memory_event_outbox_full")
            state["events"][key] = payload
            self._write(state)

    def pending(self, *, limit: int = 25) -> list[dict[str, str]]:
        with self._locked():
            state = self._load()
            return [dict(event) for event in list(state["events"].values())[:max(0, min(limit, 100))]]

    def pending_count(self) -> int:
        with self._locked():
            return len(self._load()["events"])

    def acknowledge(self, event: Mapping[str, Any]) -> None:
        payload = _validated_event(event)
        key = _event_key(payload)
        with self._locked():
            state = self._load()
            previous = state["events"].get(key)
            if previous is None:
                return
            if previous != payload:
                raise MemoryEventOutboxError("memory_event_identity_conflict")
            del state["events"][key]
            self._write(state)

    def _validate_path(self) -> None:
        if (not self.path.is_absolute() or self.path == Path("/")
                or self.path.resolve(strict=False) != self.path):
            raise MemoryEventOutboxError("memory_event_state_path_invalid")
        try:
            _validate_protected_parent_chain(self.path, label="memory event outbox")
            parent = self.path.parent.lstat()
            if (parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) != 0o700
                    or not os.access(self.path.parent, os.W_OK | os.X_OK)):
                raise MemoryEventOutboxError("memory_event_directory_unsafe")
        except (OSError, ProtectedJsonFileError) as exc:
            raise MemoryEventOutboxError("memory_event_directory_unsafe") from exc

    @contextmanager
    def _locked(self):
        # 별도 0600 lock inode는 atomic replace와 무관하게 프로세스·스레드 간 RMW를 묶는다.
        with _LOCK:
            self._validate_path()
            descriptor = os.open(str(self.path) + ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
            try:
                _validate_file(os.fstat(descriptor))
                fcntl.flock(descriptor, fcntl.LOCK_EX)
                yield
            finally:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
                os.close(descriptor)

    def _load(self) -> dict[str, Any]:
        descriptor = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW)
        try:
            _validate_file(os.fstat(descriptor))
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read(_MAX_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(raw) > _MAX_BYTES:
            raise MemoryEventOutboxError("memory_event_state_too_large")
        try:
            state = json.loads(raw, object_pairs_hook=_unique_object)
        except (ValueError, UnicodeError) as exc:
            raise MemoryEventOutboxError("memory_event_state_invalid") from exc
        if (not isinstance(state, dict) or set(state) != {"version", "events"}
                or type(state["version"]) is not int or state["version"] != 1
                or not isinstance(state["events"], dict) or len(state["events"]) > _MAX_EVENTS):
            raise MemoryEventOutboxError("memory_event_state_invalid")
        for key, event in state["events"].items():
            if _event_key(_validated_event(event)) != key:
                raise MemoryEventOutboxError("memory_event_state_invalid")
        return state

    def _write(self, state: Mapping[str, Any]) -> None:
        # fsync→atomic replace→directory fsync 이후에만 영속 접수/ACK 성공을 반환한다.
        raw = (json.dumps(state, ensure_ascii=True, separators=(",", ":")) + "\n").encode("utf-8")
        if len(raw) > _MAX_BYTES:
            raise MemoryEventOutboxError("memory_event_state_too_large")
        descriptor, name = tempfile.mkstemp(prefix="." + self.path.name + ".", suffix=".tmp", dir=self.path.parent)
        temporary = Path(name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(raw)
                stream.flush()
                os.fsync(stream.fileno())
            self._validate_path()
            # 초기 검증 후 교체된 symlink·외부 hard link도 덮지 않는다.
            _validate_file(self.path.lstat())
            os.replace(temporary, self.path)
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            temporary.unlink(missing_ok=True)


def _validate_file(metadata: os.stat_result) -> None:
    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_nlink != 1):
        raise MemoryEventOutboxError("memory_event_file_unsafe")


def _validated_event(event: Mapping[str, Any]) -> dict[str, str]:
    if (not isinstance(event, Mapping) or not set(event) <= _EVENT_FIELDS | _RAW_FIELDS
            or not (_EVENT_FIELDS - {"revisionHash"}) <= set(event)):
        raise MemoryEventOutboxError("memory_event_invalid")
    if (any(not isinstance(event[key], str) or not _IDENTIFIER.fullmatch(event[key])
            for key in ("tenantId", "channelId", "eventId"))
            or not isinstance(event["messageTs"], str) or not _TIMESTAMP.fullmatch(event["messageTs"])
            or not isinstance(event["eventType"], str) or event["eventType"] not in {"changed", "deleted", "unavailable"}
            or ("revisionHash" in event and (not isinstance(event["revisionHash"], str)
                                           or not _DIGEST.fullmatch(event["revisionHash"])))):
        raise MemoryEventOutboxError("memory_event_invalid")
    # 원문 수집 receipt도 같은 파일에 두되 본문과 링크 없이 재조회할 식별자만 허용한다.
    if "rawEventType" in event:
        raw_type = event["rawEventType"]
        if not isinstance(raw_type, str):
            raise MemoryEventOutboxError("memory_raw_event_invalid")
        expected_base = "changed" if raw_type in {"created", "changed"} else raw_type
        if (raw_type not in {"created", "changed", "deleted", "unavailable"}
                or event["eventType"] != expected_base
                or not event["eventId"].startswith("memory.raw_event:")
                or not isinstance(event.get("threadTs"), str) or not _TIMESTAMP.fullmatch(event["threadTs"])
                or not isinstance(event.get("eventTs"), str) or not _TIMESTAMP.fullmatch(event["eventTs"])
                or Decimal(event["threadTs"]) > Decimal(event["messageTs"])
                or Decimal(event["eventTs"]) < Decimal(event["messageTs"])
                or (raw_type != "deleted" and "revisionHash" not in event)
                or (raw_type in {"created", "changed"} and (not isinstance(event.get("authorId"), str)
                    or not re.fullmatch(r"[UW][A-Z0-9]{1,127}", event["authorId"])))
                or ("authorId" in event and (not isinstance(event["authorId"], str)
                    or not re.fullmatch(r"[UW][A-Z0-9]{1,127}", event["authorId"])))):
            raise MemoryEventOutboxError("memory_raw_event_invalid")
    elif set(event) & _RAW_FIELDS:
        raise MemoryEventOutboxError("memory_raw_event_invalid")
    return dict(event)


def _event_key(event: Mapping[str, str]) -> str:
    return canonical_digest([event["tenantId"], event["eventId"]])


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise MemoryEventOutboxError("memory_event_state_invalid")
        result[key] = value
    return result


__all__ = ["MemoryEventOutboxError", "MemorySourceEventOutbox"]
