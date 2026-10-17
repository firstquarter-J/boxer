"""암호화 snapshot과 삭제 hash chain을 함께 검증하는 기억 전용 백업."""

from __future__ import annotations

from dataclasses import dataclass
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Callable, Mapping
import uuid

from .contracts import canonical_digest


class MemoryBackupError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MemoryBackupSettings:
    bucket: str
    prefix: str
    kms_key_id: str

    def __post_init__(self) -> None:
        if not self.bucket or not self.prefix or not self.kms_key_id:
            raise MemoryBackupError("memory_backup_not_configured")
        if not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._/-]{0,199}", self.prefix) or ".." in self.prefix or self.prefix.startswith("/"):
            raise MemoryBackupError("memory_backup_prefix_invalid")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> MemoryBackupSettings | None:
        values = env if env is not None else os.environ
        parts = [str(values.get("BOXER_COMPANY_API_MEMORY_BACKUP_" + key, "")).strip() for key in ("BUCKET", "PREFIX", "KMS_KEY_ID")]
        if not any(parts):
            return None
        return cls(parts[0], parts[1].strip("/"), parts[2])


def _encode(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _error_code(exc: Exception) -> str:
    return str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))


class MemoryBackup:
    """head CAS가 API 삭제와 companion flush의 동시 기록을 직렬화한다."""

    def __init__(self, settings: MemoryBackupSettings, *, client: Any | None = None) -> None:
        self.settings = settings
        if client is None:
            from boxer.retrieval.connectors.s3 import _build_s3_client

            client = _build_s3_client()
        self.client = client
        self._conditional_headers: ContextVar[dict[str, str]] = ContextVar("memory_s3_conditions_" + uuid.uuid4().hex, default={})
        self._legacy_conditions = False
        meta = getattr(client, "meta", None)
        if meta is not None:
            members = meta.service_model.operation_model("PutObject").input_shape.members
            self._legacy_conditions = not {"IfMatch", "IfNoneMatch"}.issubset(members)
            if self._legacy_conditions:
                # 저장소의 기존 boto3 pin은 조건 필드 모델이 없다. 공식 event hook에서
                # 서명 이전 header만 더하며 ContextVar로 동시 요청 조건을 분리한다.
                meta.events.register("before-call.s3.PutObject", self._inject_conditions)

    def _inject_conditions(self, params: dict[str, Any], **kwargs: Any) -> None:
        params.setdefault("headers", {}).update(self._conditional_headers.get())

    def _key(self, suffix: str) -> str:
        return self.settings.prefix.rstrip("/") + "/" + suffix

    def _put(self, key: str, body: bytes, *, expected_etag: str | None = None, create: bool = False) -> None:
        kwargs: dict[str, Any] = {"Bucket": self.settings.bucket, "Key": key, "Body": body, "ServerSideEncryption": "aws:kms", "SSEKMSKeyId": self.settings.kms_key_id}
        condition = {"If-None-Match": "*"} if create else ({"If-Match": expected_etag} if expected_etag is not None else {})
        token = self._conditional_headers.set(condition)
        try:
            if not self._legacy_conditions:
                if create:
                    kwargs["IfNoneMatch"] = "*"
                elif expected_etag is not None:
                    kwargs["IfMatch"] = expected_etag
            self.client.put_object(**kwargs)
        finally:
            self._conditional_headers.reset(token)

    def _get(self, key: str, *, max_bytes: int = 1_000_000) -> tuple[bytes, str]:
        response = self.client.get_object(Bucket=self.settings.bucket, Key=key)
        stream = response["Body"]
        try:
            if response.get("ServerSideEncryption") != "aws:kms":
                raise MemoryBackupError("memory_backup_encryption_invalid")
            body = stream.read(max_bytes + 1)
        finally:
            stream.close()
        if len(body) > max_bytes:
            raise MemoryBackupError("memory_backup_object_too_large")
        return body, str(response.get("ETag", ""))

    def initialize_journal(self) -> None:
        # 원격 head 누락을 복구 중 빈 journal로 해석하지 않고 명시 초기화만 허용한다.
        self._put(self._key("journal/head.json"), _encode({"version": 1, "sequence": 0, "eventHash": ""}), create=True)
        self.verify_journal()

    def _read_head(self) -> tuple[dict[str, Any], str]:
        try:
            body, etag = self._get(self._key("journal/head.json"))
            head = json.loads(body)
        except Exception as exc:
            raise MemoryBackupError("memory_journal_head_unavailable") from exc
        if not isinstance(head, dict) or set(head) != {"version", "sequence", "eventHash"} or head["version"] != 1 or type(head["sequence"]) is not int or head["sequence"] < 0 or not etag:
            raise MemoryBackupError("memory_journal_head_invalid")
        if (head["sequence"] == 0 and head["eventHash"] != "") or (head["sequence"] > 0 and not re.fullmatch(r"[a-f0-9]{64}", str(head["eventHash"]))):
            raise MemoryBackupError("memory_journal_head_invalid")
        return head, etag

    def verify_journal(self) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        head, _ = self._read_head()
        expected_hash = head["eventHash"]
        events: list[dict[str, Any]] = []
        for sequence in range(head["sequence"], 0, -1):
            try:
                body, _ = self._get(self._key("journal/events/" + expected_hash + ".json"))
                event = json.loads(body)
            except Exception as exc:
                raise MemoryBackupError("memory_journal_event_unavailable") from exc
            if not isinstance(event, dict) or set(event) != {"version", "sequence", "previousHash", "event"} or event.get("version") != 1 or event.get("sequence") != sequence or canonical_digest(event) != expected_hash:
                raise MemoryBackupError("memory_journal_chain_invalid")
            if not isinstance(event.get("event"), dict) or not event["event"].get("id"):
                raise MemoryBackupError("memory_journal_event_invalid")
            events.append({**event, "hash": expected_hash})
            expected_hash = event["previousHash"]
        if expected_hash:
            raise MemoryBackupError("memory_journal_chain_invalid")
        return head, list(reversed(events))

    def append_deletion(self, event: Mapping[str, Any]) -> int:
        # 본문 없는 tombstone 필드만 journal에 나간다. 실수로 text가 섞여도 저장하지 않는다.
        allowed = {"id", "tenantId", "scopeChannelId", "memoryId", "sourceId", "eventType", "actorId", "requestId", "deletionKind", "subjectId", "topicKeyHash", "sourceMessageId", "cutoffAt", "reallowRequestId", "createdAt"}
        payload = {key: value for key, value in event.items() if key in allowed}
        if not payload.get("id") or not payload.get("tenantId") or payload.get("deletionKind") not in {"item", "source", "source_revision", "source_redaction", "subject_topic"}:
            raise MemoryBackupError("memory_deletion_event_invalid")
        for _ in range(5):
            head, etag = self._read_head()
            verified_head, existing = self.verify_journal()
            if head != verified_head:
                continue
            for previous in existing:
                if previous["event"]["id"] == payload["id"]:
                    if previous["event"] != payload:
                        raise MemoryBackupError("memory_deletion_event_conflict")
                    return int(previous["sequence"])
            entry = {"version": 1, "sequence": head["sequence"] + 1, "previousHash": head["eventHash"], "event": payload}
            digest = canonical_digest(entry)
            key = self._key("journal/events/" + digest + ".json")
            try:
                self._put(key, _encode(entry), create=True)
            except Exception as exc:
                if _error_code(exc) not in {"PreconditionFailed", "412"}:
                    raise
                stored, _ = self._get(key)
                if stored != _encode(entry):
                    raise MemoryBackupError("memory_journal_event_conflict") from exc
            new_head = {"version": 1, "sequence": entry["sequence"], "eventHash": digest}
            try:
                self._put(self._key("journal/head.json"), _encode(new_head), expected_etag=etag)
            except Exception as exc:
                if _error_code(exc) in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                    continue
                raise
            # ACK 전에 head와 hash chain을 다시 읽어 실제 내구성을 확인한다.
            _, persisted = self.verify_journal()
            if any(row["hash"] == digest for row in persisted):
                return int(entry["sequence"])
        raise MemoryBackupError("memory_journal_write_conflict")

    def flush_deletions(self, store: Any) -> int:
        count = 0
        for event in store.deletion_events():
            if event.get("durabilityState") == "durable":
                continue
            sequence = self.append_deletion(event)
            store.mark_deletion_durable(event["id"], sequence)
            count += 1
        return count

    def create_snapshot(self, store: Any, *, now: datetime | None = None) -> dict[str, Any]:
        self.flush_deletions(store)
        head, _ = self.verify_journal()
        when = now or datetime.now(timezone.utc)
        if when.tzinfo is None:
            raise MemoryBackupError("memory_backup_time_invalid")
        name = when.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex
        with tempfile.TemporaryDirectory(prefix="boxer-memory-snapshot-") as directory:
            path = Path(directory).resolve() / "memory.db"
            store.snapshot(path)
            os.chmod(path, 0o600)
            data = path.read_bytes()
        key = self._key("snapshots/" + name + ".db")
        metadata = {"version": 1, "key": key, "sha256": hashlib.sha256(data).hexdigest(), "createdAt": when.isoformat(), "deletionSequence": head["sequence"], "deletionHash": head["eventHash"]}
        self._put(key, data, create=True)
        self._put(key + ".json", _encode(metadata), create=True)
        return metadata

    def restore_snapshot(self, key: str, destination: str | Path, *, store_factory: Callable[[Path], Any]) -> Any:
        # 운영 서비스를 멈춘 CLI만 create-only destination으로 복구한다. 기존 DB를 덮지 않는다.
        prefix = self._key("snapshots/")
        if not key.startswith(prefix) or not re.fullmatch(r"\d{8}T\d{6}Z-[a-f0-9]{32}\.db", key[len(prefix):]):
            raise MemoryBackupError("memory_snapshot_key_invalid")
        target = Path(destination)
        if target.exists() or target.is_symlink() or not target.is_absolute() or target.resolve(strict=False) != target:
            raise MemoryBackupError("memory_restore_destination_invalid")
        if not target.parent.is_dir() or target.parent.is_symlink() or target.parent.stat().st_mode & 0o077 or target.parent.stat().st_uid != os.geteuid():
            raise MemoryBackupError("memory_restore_directory_invalid")
        head, events = self.verify_journal()
        meta_bytes, _ = self._get(key + ".json")
        metadata = json.loads(meta_bytes)
        sequence = metadata.get("deletionSequence")
        if metadata.get("version") != 1 or metadata.get("key") != key or type(sequence) is not int or not 0 <= sequence <= head["sequence"]:
            raise MemoryBackupError("memory_snapshot_metadata_invalid")
        expected_hash = events[sequence - 1]["hash"] if sequence else ""
        if metadata.get("deletionHash") != expected_hash:
            raise MemoryBackupError("memory_snapshot_journal_mismatch")
        data, _ = self._get(key, max_bytes=512 * 1024 * 1024)
        if hashlib.sha256(data).hexdigest() != metadata.get("sha256"):
            raise MemoryBackupError("memory_snapshot_hash_mismatch")
        # 모든 journal event를 replay해 오래된 snapshot의 suppression도 다시 확인한다.
        staging = target.with_name(target.name + ".restore-" + uuid.uuid4().hex)
        store = None
        created_target = False
        try:
            descriptor = os.open(staging, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            with os.fdopen(descriptor, "wb") as output:
                output.write(data)
                output.flush()
                os.fsync(output.fileno())
            store = store_factory(staging)
            for event in events:
                store.apply_deletion_event(event["event"])
                store.mark_deletion_durable(event["event"]["id"], event["sequence"])
            store.cleanup(restored=True)
            store.snapshot(target)
            created_target = True
            os.chmod(target, 0o600)
            final_head, _ = self._read_head()
            if final_head != head:
                raise MemoryBackupError("memory_restore_journal_changed")
            return {"path": str(target), "deletionSequence": head["sequence"]}
        except Exception:
            if created_target and target.exists():
                target.unlink()
            raise
        finally:
            if store is not None and callable(getattr(store, "close", None)):
                store.close()
            for suffix in ("", "-wal", "-shm"):
                path = Path(str(staging) + suffix)
                if path.exists():
                    path.unlink()

    def prune_snapshots(self, *, now: datetime | None = None, retention_days: int = 30) -> int:
        if retention_days < 30:
            raise MemoryBackupError("memory_retention_too_short")
        # journal은 별도 보존하며 snapshot prefix 밖의 객체는 절대 삭제하지 않는다.
        actual_now = now or datetime.now(timezone.utc)
        if actual_now.tzinfo is None:
            raise MemoryBackupError("memory_backup_time_invalid")
        cutoff = actual_now - timedelta(days=retention_days)
        prefix = self._key("snapshots/")
        cursor = None
        removed = 0
        while True:
            kwargs: dict[str, Any] = {"Bucket": self.settings.bucket, "Prefix": prefix}
            if cursor:
                kwargs["ContinuationToken"] = cursor
            page = self.client.list_objects_v2(**kwargs)
            for obj in page.get("Contents", []):
                key = str(obj["Key"])
                if not key.startswith(prefix) or not re.fullmatch(r"\d{8}T\d{6}Z-[a-f0-9]{32}\.db(?:\.json)?", key[len(prefix):]):
                    continue
                if obj["LastModified"] < cutoff:
                    self.client.delete_object(Bucket=self.settings.bucket, Key=key)
                    removed += 1
            cursor = page.get("NextContinuationToken") if page.get("IsTruncated") else None
            if not cursor:
                return removed


def configured_memory_backup(env: Mapping[str, str] | None = None) -> MemoryBackup | None:
    settings = MemoryBackupSettings.from_env(env)
    return MemoryBackup(settings) if settings else None
