"""프로세스 간 claim과 출처 revision을 검증하는 회사 기억 SQLite 저장소."""

from __future__ import annotations

import json
import os
import re
import sqlite3
import stat
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from .access import can_read_memory_source, is_private_memory_hub
from .contracts import (
    ELIGIBLE_ROUTES,
    EXTRACTOR_VERSION,
    MAX_CANDIDATES,
    MAX_CHUNK_CHARS,
    MAX_CHUNK_SOURCES,
    MEMORY_SCHEMA_VERSION,
    PERSONAL_STYLE_TOPICS,
    MemoryError,
    canonical_digest,
)
from .live_query import is_within_live_query_period
from .policy import (
    has_explicit_completion,
    is_safe_memory_text,
    normalize_source,
    normalize_topic_key,
    personal_style_text,
    standard_preference_value,
    validate_candidate,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS memory_threads (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, channel_id TEXT NOT NULL,
 thread_ts TEXT NOT NULL, channel_kind TEXT NOT NULL, thread_permalink TEXT NOT NULL DEFAULT '',
 input_revision INTEGER NOT NULL DEFAULT 0, memory_generation INTEGER NOT NULL DEFAULT 0,
 coverage TEXT NOT NULL DEFAULT 'partial', collected_through_ts TEXT NOT NULL DEFAULT '',
 last_collected_at REAL NOT NULL DEFAULT 0, deletion_epoch INTEGER NOT NULL DEFAULT 0,
 UNIQUE(tenant_id,id), UNIQUE(tenant_id,channel_id,thread_ts)
);
CREATE TABLE IF NOT EXISTS memory_sources (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, channel_id TEXT NOT NULL,
 thread_id TEXT NOT NULL, message_ts TEXT NOT NULL, revision_hash TEXT NOT NULL,
 author_id TEXT NOT NULL, author_kind TEXT NOT NULL, created_at TEXT NOT NULL,
 edited_at TEXT NOT NULL DEFAULT '', permalink TEXT NOT NULL, excerpt TEXT NOT NULL,
 state TEXT NOT NULL DEFAULT 'current', last_verified_at REAL NOT NULL,
 UNIQUE(tenant_id,id), UNIQUE(tenant_id,channel_id,message_ts,revision_hash),
 FOREIGN KEY(tenant_id,thread_id) REFERENCES memory_threads(tenant_id,id)
);
CREATE TABLE IF NOT EXISTS memory_items (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, scope_channel_id TEXT NOT NULL,
 origin_thread_id TEXT NOT NULL, kind TEXT NOT NULL, category TEXT NOT NULL,
 subject_user_id TEXT, topic_key TEXT NOT NULL, text TEXT NOT NULL,
 evidence_type TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active',
 valid_from REAL NOT NULL, valid_until REAL, observed_at REAL NOT NULL, updated_at REAL NOT NULL,
 replaces_id TEXT, version INTEGER NOT NULL DEFAULT 1, extractor_version TEXT NOT NULL,
 UNIQUE(tenant_id,id), FOREIGN KEY(tenant_id,origin_thread_id) REFERENCES memory_threads(tenant_id,id)
);
CREATE TABLE IF NOT EXISTS memory_subjects (
 tenant_id TEXT NOT NULL, memory_id TEXT NOT NULL, user_id TEXT NOT NULL, role TEXT NOT NULL,
 PRIMARY KEY(tenant_id,memory_id,user_id,role),
 FOREIGN KEY(tenant_id,memory_id) REFERENCES memory_items(tenant_id,id)
);
CREATE TABLE IF NOT EXISTS memory_item_sources (
 tenant_id TEXT NOT NULL, memory_id TEXT NOT NULL, source_id TEXT NOT NULL,
 relation TEXT NOT NULL, evidence_span TEXT NOT NULL,
 PRIMARY KEY(tenant_id,memory_id,source_id,relation),
 FOREIGN KEY(tenant_id,memory_id) REFERENCES memory_items(tenant_id,id),
 FOREIGN KEY(tenant_id,source_id) REFERENCES memory_sources(tenant_id,id)
);
CREATE TABLE IF NOT EXISTS memory_summaries (
 tenant_id TEXT NOT NULL, scope_channel_id TEXT NOT NULL, target_kind TEXT NOT NULL,
 target_id TEXT NOT NULL, summary TEXT NOT NULL, source_ids TEXT NOT NULL,
 base_items_digest TEXT NOT NULL, coverage TEXT NOT NULL, updated_at REAL NOT NULL,
 PRIMARY KEY(tenant_id,scope_channel_id,target_kind,target_id)
);
CREATE TABLE IF NOT EXISTS memory_jobs (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, capture_id TEXT NOT NULL,
 trigger_request_id TEXT NOT NULL, thread_id TEXT NOT NULL, input_digest TEXT NOT NULL DEFAULT '',
 base_revision INTEGER NOT NULL DEFAULT 0, deletion_epoch INTEGER NOT NULL DEFAULT 0,
 extractor_version TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'collecting',
 lease_owner TEXT NOT NULL DEFAULT '', claim_token TEXT NOT NULL DEFAULT '', lease_until REAL NOT NULL DEFAULT 0,
 attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at REAL NOT NULL DEFAULT 0,
 processed_chunk_count INTEGER NOT NULL DEFAULT 0, last_error_type TEXT NOT NULL DEFAULT '',
 model TEXT NOT NULL DEFAULT '', input_tokens INTEGER NOT NULL DEFAULT 0,
 output_tokens INTEGER NOT NULL DEFAULT 0, cache_tokens INTEGER NOT NULL DEFAULT 0,
 metadata TEXT NOT NULL, created_at REAL NOT NULL, expires_at REAL NOT NULL,
 UNIQUE(tenant_id,id), UNIQUE(tenant_id,capture_id),
 UNIQUE(tenant_id,trigger_request_id,extractor_version),
 FOREIGN KEY(tenant_id,thread_id) REFERENCES memory_threads(tenant_id,id)
);
CREATE TABLE IF NOT EXISTS memory_job_inputs (
 tenant_id TEXT NOT NULL, capture_id TEXT NOT NULL, chunk_no INTEGER NOT NULL,
 chunk_digest TEXT NOT NULL, payload TEXT NOT NULL, candidate_result TEXT,
 result_version TEXT, received_at REAL NOT NULL, expires_at REAL NOT NULL, is_last INTEGER NOT NULL,
 PRIMARY KEY(tenant_id,capture_id,chunk_no),
 FOREIGN KEY(tenant_id,capture_id) REFERENCES memory_jobs(tenant_id,capture_id)
);
CREATE TABLE IF NOT EXISTS memory_events (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, scope_channel_id TEXT NOT NULL,
 memory_id TEXT, source_id TEXT, event_type TEXT NOT NULL, actor_id TEXT,
 request_id TEXT NOT NULL, from_version INTEGER, to_version INTEGER, reason_code TEXT NOT NULL DEFAULT '',
 deletion_kind TEXT, subject_id TEXT, topic_key_hash TEXT, source_message_id TEXT,
 cutoff_at REAL, reallow_request_id TEXT, remote_sequence INTEGER,
 durability_state TEXT NOT NULL DEFAULT 'not_required', created_at REAL NOT NULL,
 UNIQUE(tenant_id,request_id,event_type,source_message_id,source_id)
);
CREATE INDEX IF NOT EXISTS memory_items_actor ON memory_items(tenant_id,scope_channel_id,status,subject_user_id);
CREATE INDEX IF NOT EXISTS memory_items_topic ON memory_items(tenant_id,scope_channel_id,topic_key,updated_at);
CREATE INDEX IF NOT EXISTS memory_sources_message ON memory_sources(tenant_id,channel_id,message_ts);
CREATE INDEX IF NOT EXISTS memory_jobs_due ON memory_jobs(state,next_attempt_at,lease_until);
"""

# 명시 승인된 고정 범위의 조회 원문만 별도 보관한다. 요약과 추출 입력으로 사용하지 않는다.
_RAW_SCHEMA = """
-- 요약 테이블은 유지하고 승인된 조회 원문과 범위 manifest만 별도 연결한다.
CREATE TABLE IF NOT EXISTS memory_raw_imports (
 id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL, channel_id TEXT NOT NULL,
 author_id TEXT NOT NULL, operator_id TEXT NOT NULL, start_ts TEXT NOT NULL, cutoff_ts TEXT NOT NULL,
 manifest_digest TEXT NOT NULL, state TEXT NOT NULL, created_at REAL NOT NULL,
 UNIQUE(tenant_id,id)
);
CREATE TABLE IF NOT EXISTS memory_source_texts (
 tenant_id TEXT NOT NULL, source_id TEXT NOT NULL, import_id TEXT NOT NULL,
 text TEXT NOT NULL, stored_at REAL NOT NULL,
 PRIMARY KEY(tenant_id,source_id),
 FOREIGN KEY(tenant_id,source_id) REFERENCES memory_sources(tenant_id,id),
 FOREIGN KEY(tenant_id,import_id) REFERENCES memory_raw_imports(tenant_id,id)
);
CREATE INDEX IF NOT EXISTS memory_raw_imports_scope ON memory_raw_imports(tenant_id,channel_id,state);
"""

# 실시간 원문 이벤트의 최신 revision을 기록하되 추출 queue와 연결하지 않는다.
_RAW_EVENT_SCHEMA = """
CREATE TABLE IF NOT EXISTS memory_raw_event_states (
 tenant_id TEXT NOT NULL, channel_id TEXT NOT NULL, message_ts TEXT NOT NULL,
 event_ts TEXT NOT NULL, revision_hash TEXT NOT NULL, state TEXT NOT NULL,
 PRIMARY KEY(tenant_id,channel_id,message_ts)
);
"""


def _validate_legacy_schema(connection: sqlite3.Connection, version: int) -> None:
    # user_version만 바꾼 다른 DB를 migration하지 않도록 기존 테이블과 tenant FK를 확인한다.
    expected = sqlite3.connect(":memory:")
    try:
        forbidden = ("memory_raw_imports", "memory_source_texts", "memory_raw_event_states") if version == 1 else ("memory_raw_event_states",)
        if any(connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone()
               for name in forbidden):
            raise MemoryError("memory_schema_unsupported")
        expected.executescript(_SCHEMA + (_RAW_SCHEMA if version == 2 else ""))
        for (table,) in expected.execute("SELECT name FROM sqlite_master WHERE type='table'"):
            for pragma in ("table_info", "foreign_key_list"):
                if connection.execute(f"PRAGMA {pragma}({table})").fetchall() != expected.execute(f"PRAGMA {pragma}({table})").fetchall():
                    raise MemoryError("memory_schema_unsupported")
    finally:
        expected.close()


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _iso(value: float) -> str:
    return datetime.fromtimestamp(value, timezone.utc).isoformat().replace("+00:00", "Z")


def _raw_only_scope(tenant: str, channel: str, message_ts: str) -> bool:
    # 원문 보존 정책은 회사 고정 범위만 판정하며 추출 모듈과의 import 순환을 피한다.
    from .raw_policy import is_raw_only_scope

    return is_raw_only_scope(tenant, channel, message_ts)


def _raw_only_channel(tenant: str, channel: str) -> bool:
    from .raw_policy import is_raw_only_channel

    return is_raw_only_channel(tenant, channel)


def _path(raw: str | Path, *, missing: bool = False) -> Path:
    path = Path(raw).expanduser()
    if not path.is_absolute() or path.resolve(strict=False) != path:
        raise MemoryError("memory_path_invalid")
    try:
        parent = path.parent.lstat()
        if (not stat.S_ISDIR(parent.st_mode) or path.parent.is_symlink()
                or parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o077):
            raise MemoryError("memory_directory_unsafe")
        leaf = path.lstat()
    except FileNotFoundError as exc:
        if missing and path.parent.exists():
            return path
        raise MemoryError("memory_storage_missing") from exc
    if (not stat.S_ISREG(leaf.st_mode) or path.is_symlink()
            or leaf.st_uid != os.geteuid() or stat.S_IMODE(leaf.st_mode) != 0o600):
        raise MemoryError("memory_file_unsafe")
    for suffix in ("-wal", "-shm"):
        sidecar = Path(str(path) + suffix)
        if sidecar.exists() or sidecar.is_symlink():
            metadata = sidecar.lstat()
            if sidecar.is_symlink() or not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o600:
                raise MemoryError("memory_sidecar_unsafe")
    return path


def initialize_memory_store(path: str | Path) -> Path:
    """명시적 초기화에서만 파일을 만들고 version을 준비한다. 실행 중 자동 생성하지 않는다."""

    target = Path(path).expanduser()
    if not target.is_absolute() or target.resolve(strict=False) != target:
        raise MemoryError("memory_path_invalid")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = _path(target, missing=True)
    if not target.exists():
        descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.close(descriptor)
    connection = sqlite3.connect(target, isolation_level=None, timeout=5)
    try:
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=FULL")
        version = connection.execute("PRAGMA user_version").fetchone()[0]
        if version not in {0, 1, 2, MEMORY_SCHEMA_VERSION}:
            raise MemoryError("memory_schema_unsupported")
        # 기존 v1/v2의 원문과 삭제 이력은 보존하고 실패하면 추가 DDL과 버전을 함께 되돌린다.
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if version == 0 and tables:
            raise MemoryError("memory_schema_unknown")
        if version in {1, 2}:
            _validate_legacy_schema(connection, version)
        connection.executescript("BEGIN IMMEDIATE;\n" + _SCHEMA + _RAW_SCHEMA + _RAW_EVENT_SCHEMA + f"\nPRAGMA user_version={MEMORY_SCHEMA_VERSION};\nCOMMIT;")
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    return _path(target)


class MemoryStore:
    def __init__(self, path: str | Path, *, clock: Callable[[], float] = time.time, debounce_seconds: float = 10) -> None:
        self.path = _path(path)
        self._clock = clock
        self._debounce = max(0, min(60, debounce_seconds))
        if not self.health():
            raise MemoryError("memory_storage_unhealthy")

    @contextmanager
    def _connection(self, *, write: bool = False) -> Iterator[sqlite3.Connection]:
        # mode=rw는 시작 뒤 사라진 DB를 빈 파일로 다시 만드는 sqlite 기본 동작을 막는다.
        path = _path(self.path)
        connection = sqlite3.connect(path.as_uri() + "?mode=rw", uri=True, timeout=5, isolation_level=None)
        connection.row_factory = sqlite3.Row
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=5000")
            connection.execute("PRAGMA secure_delete=ON")
            if write:
                connection.execute("BEGIN IMMEDIATE")
            yield connection
            if write:
                connection.execute("COMMIT")
        except BaseException:
            if write and connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        finally:
            connection.close()

    def health(self) -> bool:
        try:
            with self._connection() as connection:
                # 런타임은 schema를 만들지 않는다. version만 올리고 빠뜨린 원문 테이블도 닫는다.
                connection.execute("SELECT id,tenant_id,channel_id,author_id,operator_id,start_ts,cutoff_ts,manifest_digest,state,created_at FROM memory_raw_imports LIMIT 0")
                connection.execute("SELECT tenant_id,source_id,import_id,text,stored_at FROM memory_source_texts LIMIT 0")
                connection.execute("SELECT tenant_id,channel_id,message_ts,event_ts,revision_hash,state FROM memory_raw_event_states LIMIT 0")
                return (connection.execute("PRAGMA user_version").fetchone()[0] == MEMORY_SCHEMA_VERSION
                        and connection.execute("PRAGMA quick_check").fetchone()[0] == "ok")
        except (OSError, sqlite3.Error, MemoryError):
            return False

    def _thread(self, connection: sqlite3.Connection, tenant: str, channel: str, thread_ts: str, kind: str = "channel") -> sqlite3.Row:
        identity = "thr_" + canonical_digest([tenant, channel, thread_ts])
        connection.execute("INSERT OR IGNORE INTO memory_threads(id,tenant_id,channel_id,thread_ts,channel_kind) VALUES(?,?,?,?,?)", (identity, tenant, channel, thread_ts, kind))
        return connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?", (tenant, identity)).fetchone()

    def _capture(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        metadata = json.loads(row["metadata"])
        thread = connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?", (row["tenant_id"], row["thread_id"])).fetchone()
        chunks = connection.execute("SELECT chunk_no,chunk_digest FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? ORDER BY chunk_no", (row["tenant_id"], row["capture_id"])).fetchall()
        return {
            "captureId": row["capture_id"], "tenantId": row["tenant_id"], "actorId": metadata["actorId"],
            "channelId": thread["channel_id"], "threadTs": thread["thread_ts"], "channelKind": thread["channel_kind"],
            "currentMessageTs": metadata["currentMessageTs"], "requestId": row["trigger_request_id"],
            "route": metadata["route"], "state": row["state"], "deletionEpoch": row["deletion_epoch"],
            "claimToken": row["claim_token"], "leaseUntil": _iso(row["lease_until"]),
            "expiresAt": _iso(row["expires_at"]), "checkpoint": {**metadata.get("checkpoint", {}), "coverage": metadata.get("coverage", metadata.get("checkpoint", {}).get("coverage", "partial"))},
            "receivedChunks": [{"chunkNo": item["chunk_no"], "chunkDigest": item["chunk_digest"]} for item in chunks],
            "nextChunkNo": max((item["chunk_no"] for item in chunks), default=-1) + 1,
            "answerParts": metadata.get("answerParts", []),
        }

    def create_capture(self, *, tenant_id: str, actor_id: str, channel_id: str, thread_ts: str,
                       current_message_ts: str, channel_kind: str, request_id: str, route: str,
                       answer_parts: Sequence[Any] = ()) -> dict[str, Any]:
        if route not in ELIGIBLE_ROUTES:
            raise MemoryError("capture_scope_invalid")
        return self._create_capture(tenant_id=tenant_id, actor_id=actor_id, channel_id=channel_id, thread_ts=thread_ts,
                                    current_message_ts=current_message_ts, channel_kind=channel_kind, request_id=request_id,
                                    route=route, answer_parts=answer_parts)

    def create_import_capture(self, *, tenant_id: str, operator_id: str, channel_id: str, thread_ts: str,
                              cutoff_ts: str, channel_kind: str, import_id: str, input_digest: str) -> dict[str, Any]:
        if not import_id or not re.fullmatch(r"[0-9a-f]{64}", input_digest):
            raise MemoryError("memory_import_identity_invalid")
        # 과거 수집은 멘션·답변 receipt를 위조하지 않는 별도 운영자 진입점이다.
        return self._create_capture(tenant_id=tenant_id, actor_id=operator_id, channel_id=channel_id, thread_ts=thread_ts,
                                    current_message_ts=cutoff_ts, channel_kind=channel_kind,
                                    request_id="history_import:" + canonical_digest([import_id, thread_ts]),
                                    route="history_import", import_metadata={"triggerKind": "history_import", "importId": import_id, "inputDigest": input_digest})

    def bind_history_import(self, *, tenant_id: str, channel_id: str, operator_id: str,
                            import_id: str, manifest_digest: str) -> None:
        if not all((tenant_id, channel_id, operator_id, import_id)) or not re.fullmatch(r"[0-9a-f]{64}", manifest_digest):
            raise MemoryError("memory_import_identity_invalid")
        identity = "evt_history_" + canonical_digest([tenant_id, import_id])
        with self._connection(write=True) as connection:
            previous = connection.execute("SELECT * FROM memory_events WHERE id=?", (identity,)).fetchone()
            if previous:
                if (previous["tenant_id"], previous["scope_channel_id"], previous["actor_id"], previous["request_id"], previous["reason_code"]) != (tenant_id, channel_id, operator_id, import_id, manifest_digest):
                    raise MemoryError("memory_import_manifest_conflict")
                return
            # 전체 import 범위와 원문 manifest는 해시만 보존해 재개 때 추가 thread 삽입도 막는다.
            connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,event_type,actor_id,request_id,reason_code,durability_state,created_at) VALUES(?,?,?,'history_import',?,?,?,'not_required',?)", (identity, tenant_id, channel_id, operator_id, import_id, manifest_digest, self._clock()))

    def _create_capture(self, *, tenant_id: str, actor_id: str, channel_id: str, thread_ts: str,
                        current_message_ts: str, channel_kind: str, request_id: str, route: str,
                        answer_parts: Sequence[Any] = (), import_metadata: Mapping[str, str] | None = None) -> dict[str, Any]:
        if (not all((tenant_id, actor_id, channel_id, thread_ts, current_message_ts, request_id))
                or not re.fullmatch(r"\d{1,20}\.\d{1,9}", thread_ts)
                or not re.fullmatch(r"\d{1,20}\.\d{1,9}", current_message_ts)
                or Decimal(current_message_ts) < Decimal(thread_ts)):
            raise MemoryError("capture_scope_invalid")
        if _raw_only_channel(tenant_id, channel_id):
            raise MemoryError("memory_raw_only_scope")
        now = self._clock()
        with self._connection(write=True) as connection:
            existing = connection.execute("SELECT * FROM memory_jobs WHERE tenant_id=? AND trigger_request_id=? AND extractor_version=?", (tenant_id, request_id, EXTRACTOR_VERSION)).fetchone()
            if existing:
                result = self._capture(connection, existing)
                metadata = json.loads(existing["metadata"])
                if (result["actorId"], result["channelId"], result["threadTs"], result["currentMessageTs"], result["route"], result["channelKind"]) != (actor_id, channel_id, thread_ts, current_message_ts, route, channel_kind):
                    raise MemoryError("capture_identity_conflict")
                if import_metadata:
                    if any(metadata.get(key) != value for key, value in import_metadata.items()):
                        raise MemoryError("memory_import_manifest_conflict")
                    if existing["state"] in {"canceled", "superseded", "failed"} or existing["expires_at"] <= now:
                        raise MemoryError("memory_import_terminal")
                    if existing["state"] == "collecting":
                        connection.execute("UPDATE memory_jobs SET claim_token=?,lease_owner='history_import',lease_until=? WHERE id=?", (uuid.uuid4().hex, now + 120, existing["id"]))
                        return self._capture(connection, connection.execute("SELECT * FROM memory_jobs WHERE id=?", (existing["id"],)).fetchone())
                return result
            thread = self._thread(connection, tenant_id, channel_id, thread_ts, channel_kind)
            capture_id, job_id = "cap_" + uuid.uuid4().hex, "job_" + uuid.uuid4().hex
            parts = []
            for index, part in enumerate(answer_parts):
                text = str(part.get("text", "") if isinstance(part, Mapping) else part)
                origin_hash = str(part.get("textHash") or canonical_digest(text.strip())) if isinstance(part, Mapping) else canonical_digest(text.strip())
                parts.append({"partIndex": index, "deliveryId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"boxer-memory:{capture_id}:{index}")), "textHash": origin_hash})
            metadata = {"actorId": actor_id, "currentMessageTs": current_message_ts, "route": route, "answerParts": parts, "checkpoint": {"cursor": None, "coverage": "partial", "deliveryReceipts": []}}
            if import_metadata:
                metadata.update(import_metadata)
            connection.execute("""INSERT INTO memory_jobs(id,tenant_id,capture_id,trigger_request_id,thread_id,base_revision,deletion_epoch,extractor_version,lease_owner,claim_token,lease_until,metadata,created_at,expires_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (job_id, tenant_id, capture_id, request_id, thread["id"], thread["input_revision"], thread["deletion_epoch"], EXTRACTOR_VERSION, "creator", uuid.uuid4().hex, now + 120, _json(metadata), now, now + 7 * 86400))
            return self._capture(connection, connection.execute("SELECT * FROM memory_jobs WHERE id=?", (job_id,)).fetchone())

    def pull_captures(self, tenant_id: str, worker_id: str, limit: int = 10) -> list[dict[str, Any]]:
        now = self._clock()
        with self._connection(write=True) as connection:
            rows = connection.execute("SELECT * FROM memory_jobs WHERE tenant_id=? AND state='collecting' AND lease_until<=? AND expires_at>? AND COALESCE(json_extract(metadata,'$.triggerKind'),'')!='history_import' ORDER BY created_at LIMIT ?", (tenant_id, now, now, min(50, max(1, limit)))).fetchall()
            results = []
            for row in rows:
                if self._job_is_raw_only(connection, row):
                    self._cancel_one_job(connection, row)
                    continue
                connection.execute("UPDATE memory_jobs SET lease_owner=?,claim_token=?,lease_until=? WHERE id=?", (worker_id, uuid.uuid4().hex, now + 120, row["id"]))
                results.append(self._capture(connection, connection.execute("SELECT * FROM memory_jobs WHERE id=?", (row["id"],)).fetchone()))
            return results

    def _claimed(self, connection: sqlite3.Connection, tenant_id: str, *, capture_id: str | None = None, job_id: str | None = None, claim_token: str, state: str) -> sqlite3.Row:
        key, value = ("capture_id", capture_id) if capture_id else ("id", job_id)
        row = connection.execute(f"SELECT * FROM memory_jobs WHERE tenant_id=? AND {key}=?", (tenant_id, value)).fetchone()
        if not row or row["state"] != state or not claim_token or row["claim_token"] != claim_token or row["lease_until"] <= self._clock() or row["expires_at"] <= self._clock():
            raise MemoryError("memory_claim_stale")
        thread = connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?", (tenant_id, row["thread_id"])).fetchone()
        if row["deletion_epoch"] != thread["deletion_epoch"]:
            raise MemoryError("memory_deletion_epoch_stale")
        if state == "running" and row["base_revision"] != thread["input_revision"]:
            raise MemoryError("memory_revision_stale")
        if self._job_is_raw_only(connection, row):
            raise MemoryError("memory_raw_only_scope")
        return row

    def _job_is_raw_only(self, connection: sqlite3.Connection, job: sqlite3.Row) -> bool:
        thread = connection.execute("SELECT channel_id FROM memory_threads WHERE tenant_id=? AND id=?",
                                    (job["tenant_id"], job["thread_id"])).fetchone()
        # 원문을 수집하지 않는 다른 달에도 같은 채널의 과거 job을 다시 추출하지 않는다.
        return _raw_only_channel(job["tenant_id"], thread["channel_id"])

    def _cancel_one_job(self, connection: sqlite3.Connection, job: sqlite3.Row) -> None:
        connection.execute("UPDATE memory_jobs SET state='canceled',claim_token='',lease_until=0 WHERE id=?", (job["id"],))
        connection.execute("DELETE FROM memory_job_inputs WHERE tenant_id=? AND capture_id=?", (job["tenant_id"], job["capture_id"]))
        self._clear_job_context(connection, job["id"])

    def checkpoint(self, *, tenant_id: str, capture_id: str, claim_token: str, actor_id: str | None = None, cursor: str | None = None,
                   coverage: str | None = "partial", state: str | None = None, delivery_receipts: Sequence[Mapping[str, Any]] = ()) -> dict[str, Any]:
        coverage = coverage or "partial"
        if coverage not in {"complete", "partial", "unavailable"} or state not in {None, "canceled", "unavailable"}:
            raise MemoryError("capture_checkpoint_invalid")
        with self._connection(write=True) as connection:
            row = self._claimed(connection, tenant_id, capture_id=capture_id, claim_token=claim_token, state="collecting")
            metadata = json.loads(row["metadata"])
            if actor_id is not None and actor_id != metadata["actorId"]:
                raise MemoryError("capture_actor_invalid")
            receipts = {str(item["messageTs"]): item for item in metadata["checkpoint"].get("deliveryReceipts", [])}
            for receipt in delivery_receipts:
                index = receipt.get("partIndex")
                if not isinstance(index, int) or isinstance(index, bool) or not 0 <= index < len(metadata["answerParts"]):
                    raise MemoryError("memory_delivery_invalid")
                expected = metadata["answerParts"][index]
                if (receipt.get("deliveryId", expected["deliveryId"]) != expected["deliveryId"]
                        or not re.fullmatch(r"[a-f0-9]{64}", str(receipt.get("textHash") or "")) or not receipt.get("messageTs")):
                    raise MemoryError("memory_delivery_invalid")
                receipts[str(receipt["messageTs"])] = {"messageTs": str(receipt["messageTs"]), "partIndex": index, "deliveryId": expected["deliveryId"], "textHash": str(receipt["textHash"]), "originTextHash": expected["textHash"], "authorId": str(receipt.get("authorId") or ""), "clientMsgId": str(receipt.get("clientMsgId") or "")}
            metadata["checkpoint"] = {"cursor": cursor, "coverage": coverage, "deliveryReceipts": list(receipts.values())}
            # 발송 receipt 기록 뒤에는 pull worker가 즉시 수집을 이어받는다.
            connection.execute("UPDATE memory_jobs SET metadata=?,state=?,lease_until=? WHERE id=?", (_json(metadata), "canceled" if state else "collecting", 0 if delivery_receipts else self._clock() + 120, row["id"]))
            if state:
                connection.execute("DELETE FROM memory_job_inputs WHERE tenant_id=? AND capture_id=?", (tenant_id, capture_id))
            return self._capture(connection, connection.execute("SELECT * FROM memory_jobs WHERE id=?", (row["id"],)).fetchone())

    def put_chunk(self, *, tenant_id: str, capture_id: str, actor_id: str, claim_token: str, chunk_no: int,
                  sources: Sequence[Mapping[str, Any]], coverage: str = "partial", is_last: bool = False,
                  total_chunks: int | None = None, input_digest: str | None = None) -> dict[str, Any]:
        if (not isinstance(chunk_no, int) or isinstance(chunk_no, bool) or chunk_no < 0 or chunk_no > 10000
                or coverage not in {"complete", "partial", "unavailable"} or len(sources) > MAX_CHUNK_SOURCES
                or sum(len(str(source.get("text", ""))) for source in sources) > MAX_CHUNK_CHARS):
            raise MemoryError("memory_chunk_invalid")
        digest = canonical_digest(list(sources))
        with self._connection(write=True) as connection:
            raw = connection.execute("SELECT * FROM memory_jobs WHERE tenant_id=? AND capture_id=?", (tenant_id, capture_id)).fetchone()
            if not raw or json.loads(raw["metadata"])["actorId"] != actor_id:
                raise MemoryError("capture_actor_invalid")
            if self._job_is_raw_only(connection, raw):
                raise MemoryError("memory_raw_only_scope")
            prior = connection.execute("SELECT * FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? AND chunk_no=?", (tenant_id, capture_id, chunk_no)).fetchone()
            if prior and prior["chunk_digest"] != digest:
                raise MemoryError("memory_chunk_conflict")
            if prior and raw["state"] in {"ready", "running", "succeeded"}:
                current_thread = connection.execute("SELECT deletion_epoch FROM memory_threads WHERE tenant_id=? AND id=?", (tenant_id, raw["thread_id"])).fetchone()
                if current_thread["deletion_epoch"] != raw["deletion_epoch"]:
                    raise MemoryError("memory_deletion_epoch_stale")
                return self._capture(connection, raw)
            row = self._claimed(connection, tenant_id, capture_id=capture_id, claim_token=claim_token, state="collecting")
            metadata = json.loads(row["metadata"])
            thread = connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?", (tenant_id, row["thread_id"])).fetchone()
            accepted = []
            for source in sources:
                normalized = normalize_source(source, tenant_id=tenant_id, channel_id=thread["channel_id"], thread_ts=thread["thread_ts"], current_message_ts=metadata["currentMessageTs"])
                if normalized is None:
                    coverage = "partial"
                    continue
                if _raw_only_channel(tenant_id, thread["channel_id"]):
                    raise MemoryError("memory_raw_only_scope")
                if normalized["authorKind"] == "boxer":
                    receipt = next((item for item in metadata["checkpoint"].get("deliveryReceipts", []) if item["messageTs"] == normalized["messageTs"] and item["authorId"] == normalized["authorId"] and item["textHash"] == canonical_digest(normalized["text"])), None)
                    if receipt is None:
                        coverage = "partial"
                        continue
                if self._source_deleted(connection, tenant_id, thread["channel_id"], normalized["messageTs"]):
                    coverage = "partial"
                    continue
                invalidated = connection.execute("SELECT 1 FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND deletion_kind='source_revision' AND source_id=?", (tenant_id, thread["channel_id"], normalized["sourceId"])).fetchone()
                known = connection.execute("SELECT state FROM memory_sources WHERE tenant_id=? AND id=?", (tenant_id, normalized["sourceId"])).fetchone()
                current_revisions = connection.execute("SELECT revision_hash,edited_at,message_ts FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=? AND state='current'", (tenant_id, thread["channel_id"], normalized["messageTs"])).fetchall()
                stale = any(source["revision_hash"] != normalized["revisionHash"] and Decimal(source["edited_at"] or source["message_ts"]) >= Decimal(normalized["editedAt"] or normalized["messageTs"]) for source in current_revisions)
                if invalidated or (known and known["state"] != "current") or stale:
                    # 복원·과거 import로 들어온 철회 revision은 임시 payload와 LLM 입력에도 남기지 않는다.
                    coverage = "partial"
                    continue
                accepted.append(normalized)
            now = self._clock()
            # 앞선 chunk에서 제외한 원문이 있으면 마지막 complete가 누락을 덮지 못한다.
            metadata["hasFilteredSources"] = bool(metadata.get("hasFilteredSources") or len(accepted) < len(sources))
            if metadata["hasFilteredSources"]:
                coverage = "partial"
            metadata["coverage"] = coverage
            connection.execute("UPDATE memory_jobs SET metadata=? WHERE id=?", (_json(metadata), row["id"]))
            connection.execute("INSERT OR IGNORE INTO memory_job_inputs(tenant_id,capture_id,chunk_no,chunk_digest,payload,received_at,expires_at,is_last) VALUES(?,?,?,?,?,?,?,?)", (tenant_id, capture_id, chunk_no, digest, _json(accepted), now, row["expires_at"], int(is_last)))
            if is_last:
                chunks = connection.execute("SELECT * FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? ORDER BY chunk_no", (tenant_id, capture_id)).fetchall()
                if total_chunks != len(chunks) or [item["chunk_no"] for item in chunks] != list(range(len(chunks))) or chunk_no != len(chunks) - 1:
                    raise MemoryError("memory_capture_incomplete")
                actual_digest = canonical_digest([item["chunk_digest"] for item in chunks])
                if input_digest != actual_digest:
                    raise MemoryError("memory_input_digest_invalid")
                if metadata.get("triggerKind") == "history_import" and metadata.get("inputDigest") != actual_digest:
                    raise MemoryError("memory_import_manifest_conflict")
                # 완성된 immutable snapshot만 source 정본과 연결한다. 중간 전송은 노출하지 않는다.
                changed = False
                for chunk in chunks:
                    for source in json.loads(chunk["payload"]):
                        changed = self._register_source(connection, tenant_id, thread, source) or changed
                revision = thread["input_revision"] + int(changed)
                # 과거 수집을 나중에 추가해도 이미 확인한 최신 수집 경계는 되돌리지 않는다.
                collected_through = max(thread["collected_through_ts"] or "0", metadata["currentMessageTs"], key=Decimal)
                connection.execute("UPDATE memory_threads SET input_revision=?,coverage=?,last_collected_at=?,collected_through_ts=? WHERE id=?", (revision, coverage, now, collected_through, thread["id"]))
                # 부분 snapshot에도 이전 미처리 source가 빠지지 않도록 먼저 내부 chunk로 이월한다.
                if changed:
                    stale = connection.execute("SELECT id,capture_id FROM memory_jobs WHERE tenant_id=? AND thread_id=? AND id!=? AND state IN ('ready','running','retry') AND base_revision<?", (tenant_id, thread["id"], row["id"], revision)).fetchall()
                    self._carry_pending_inputs(connection, tenant_id, row, chunks, stale)
                    actual_digest = canonical_digest([item[0] for item in connection.execute("SELECT chunk_digest FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? ORDER BY chunk_no", (tenant_id, capture_id))])
                    for previous_job in stale:
                        connection.execute("UPDATE memory_jobs SET state='superseded',claim_token='',lease_until=0 WHERE id=?", (previous_job["id"],))
                        connection.execute("DELETE FROM memory_job_inputs WHERE tenant_id=? AND capture_id=?", (tenant_id, previous_job["capture_id"]))
                        self._clear_job_context(connection, previous_job["id"])
                connection.execute("UPDATE memory_jobs SET state=?,input_digest=?,base_revision=?,lease_owner='',claim_token='',lease_until=0,next_attempt_at=? WHERE id=?", ("ready" if changed else "succeeded", actual_digest, revision, now + self._debounce, row["id"]))
                if not changed:
                    connection.execute("DELETE FROM memory_job_inputs WHERE tenant_id=? AND capture_id=?", (tenant_id, capture_id))
            else:
                connection.execute("UPDATE memory_jobs SET lease_until=? WHERE id=?", (now + 120, row["id"]))
            return self._capture(connection, connection.execute("SELECT * FROM memory_jobs WHERE id=?", (row["id"],)).fetchone())

    def _carry_pending_inputs(self, connection: sqlite3.Connection, tenant: str, target: sqlite3.Row,
                              chunks: Sequence[sqlite3.Row], stale_jobs: Sequence[sqlite3.Row]) -> None:
        known = {source["sourceId"] for chunk in chunks for source in json.loads(chunk["payload"])}
        pending: list[dict[str, Any]] = []
        for old in stale_jobs:
            for chunk in connection.execute("SELECT payload FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? ORDER BY chunk_no", (tenant, old["capture_id"])).fetchall():
                for source in json.loads(chunk["payload"]):
                    if source["sourceId"] in known:
                        continue
                    current = connection.execute("SELECT 1 FROM memory_sources WHERE tenant_id=? AND id=? AND state='current'", (tenant, source["sourceId"])).fetchone()
                    if current:
                        pending.append(source)
                        known.add(source["sourceId"])
        chunk_no = len(chunks)
        buffer: list[dict[str, Any]] = []
        characters = 0
        for source in pending:
            if buffer and (len(buffer) >= MAX_CHUNK_SOURCES or characters + len(source["text"]) > MAX_CHUNK_CHARS):
                self._insert_internal_chunk(connection, tenant, target, chunk_no, buffer)
                chunk_no, buffer, characters = chunk_no + 1, [], 0
            buffer.append(source)
            characters += len(source["text"])
        if buffer:
            self._insert_internal_chunk(connection, tenant, target, chunk_no, buffer)

    def _insert_internal_chunk(self, connection: sqlite3.Connection, tenant: str, job: sqlite3.Row, chunk_no: int,
                               sources: Sequence[Mapping[str, Any]]) -> None:
        connection.execute("INSERT INTO memory_job_inputs(tenant_id,capture_id,chunk_no,chunk_digest,payload,received_at,expires_at,is_last) VALUES(?,?,?,?,?,?,?,0)", (tenant, job["capture_id"], chunk_no, canonical_digest(list(sources)), _json(list(sources)), self._clock(), job["expires_at"]))

    def _source_deleted(self, connection: sqlite3.Connection, tenant: str, channel: str, message: str) -> bool:
        return connection.execute("SELECT 1 FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND deletion_kind IN ('source','source_redaction') AND source_message_id=? LIMIT 1", (tenant, channel, message)).fetchone() is not None

    def _register_source(self, connection: sqlite3.Connection, tenant: str, thread: sqlite3.Row, source: Mapping[str, Any]) -> bool:
        blocked = connection.execute("SELECT 1 FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND deletion_kind='source_revision' AND source_id=? LIMIT 1", (tenant, thread["channel_id"], source["sourceId"])).fetchone()
        if blocked or self._source_deleted(connection, tenant, thread["channel_id"], source["messageTs"]):
            return False
        existing = connection.execute("SELECT state FROM memory_sources WHERE tenant_id=? AND id=?", (tenant, source["sourceId"])).fetchone()
        if existing:
            return False
        # 같은 메시지의 오래된 revision이 새 확정 revision을 덮지 않게 최신 편집 시점을 비교한다.
        previous = connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=? AND state='current'", (tenant, thread["channel_id"], source["messageTs"])).fetchall()
        for old in previous:
            if Decimal(old["edited_at"] or old["message_ts"]) > Decimal(source["editedAt"] or source["messageTs"]):
                return False
            connection.execute("UPDATE memory_sources SET state='superseded',excerpt='' WHERE id=?", (old["id"],))
            self._invalidate_source_items(connection, tenant, old["id"], deleted=False)
            self._record_revision_invalidation(connection, old, "revision:" + str(source["sourceId"]))
        connection.execute("""INSERT INTO memory_sources(id,tenant_id,channel_id,thread_id,message_ts,revision_hash,author_id,author_kind,created_at,edited_at,permalink,excerpt,last_verified_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (source["sourceId"], tenant, thread["channel_id"], thread["id"], source["messageTs"], source["revisionHash"], source["authorId"], source["authorKind"], source["createdAt"], source["editedAt"], source["permalink"], "", self._clock()))
        connection.execute("DELETE FROM memory_summaries WHERE tenant_id=? AND scope_channel_id=?", (tenant, thread["channel_id"]))
        return True

    def claim_job(self, worker_id: str, *, lease_seconds: int = 120) -> dict[str, Any] | None:
        now = self._clock()
        with self._connection(write=True) as connection:
            while True:
                row = connection.execute("SELECT j.* FROM memory_jobs j JOIN memory_threads t ON t.tenant_id=j.tenant_id AND t.id=j.thread_id WHERE j.expires_at>? AND j.attempts<3 AND j.deletion_epoch=t.deletion_epoch AND j.base_revision=t.input_revision AND ((j.state IN ('ready','retry') AND j.next_attempt_at<=?) OR (j.state='running' AND j.lease_until<=?)) ORDER BY j.created_at LIMIT 1", (now, now, now)).fetchone()
                if not row:
                    return None
                if not self._job_is_raw_only(connection, row):
                    break
                # 배포 전에 이미 대기하던 원문 전용 job도 모델 호출 전에 영구 취소한다.
                self._cancel_one_job(connection, row)
            token = uuid.uuid4().hex
            connection.execute("UPDATE memory_jobs SET state='running',lease_owner=?,claim_token=?,lease_until=?,attempts=attempts+1 WHERE id=?", (worker_id, token, now + max(30, lease_seconds), row["id"]))
            capture = self._capture(connection, connection.execute("SELECT * FROM memory_jobs WHERE id=?", (row["id"],)).fetchone())
            # 동시 스레드가 같은 사람 기억을 바꿔도 완료 시 잃어버린 갱신을 쓰지 않는다.
            metadata = json.loads(row["metadata"])
            thread = connection.execute("SELECT channel_id FROM memory_threads WHERE id=?", (row["thread_id"],)).fetchone()
            digest = self._items_digest(connection, row["tenant_id"], thread["channel_id"], actor=metadata["actorId"])
            if metadata.get("baseItemsDigest") and metadata["baseItemsDigest"] != digest:
                connection.execute("UPDATE memory_job_inputs SET candidate_result=NULL,result_version=NULL WHERE tenant_id=? AND capture_id=?", (row["tenant_id"], row["capture_id"]))
                connection.execute("UPDATE memory_jobs SET processed_chunk_count=0 WHERE id=?", (row["id"],))
                metadata.pop("reconciled", None)
                metadata.pop("reconciledDigest", None)
            metadata["baseItemsDigest"] = digest
            previous = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND status='active' ORDER BY updated_at DESC LIMIT 8", (row["tenant_id"], thread["channel_id"])).fetchall()
            metadata["previousItems"] = []
            # 다른 채널의 본인 표준 선호는 정정 판단에 필요한 고정 문구만 공유한다.
            styles = self._personal_style_candidates(connection, row["tenant_id"], metadata["actorId"])
            for item in styles:
                metadata["previousItems"].append({"memoryId": item["memoryId"], "version": item["version"], "kind": "person", "category": "preference", "subjectUserId": metadata["actorId"], "topicKey": item["topicKey"], "text": personal_style_text(item["topicKey"], item["styleValue"]), "subjects": [{"userId": metadata["actorId"], "role": "subject"}], "latestSourceTs": max((source["messageTs"] for source in item["sources"]), key=Decimal)})
            for item in previous:
                if len(metadata["previousItems"]) >= 8 or any(prior["memoryId"] == item["id"] for prior in metadata["previousItems"]):
                    continue
                subjects = [{"userId": person[0], "role": person[1]} for person in connection.execute("SELECT user_id,role FROM memory_subjects WHERE tenant_id=? AND memory_id=?", (row["tenant_id"], item["id"]))]
                source_times = [ref[0] for ref in connection.execute("SELECT s.message_ts FROM memory_item_sources r JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE r.tenant_id=? AND r.memory_id=?", (row["tenant_id"], item["id"]))]
                metadata["previousItems"].append({"memoryId": item["id"], "version": item["version"], "kind": item["kind"], "category": item["category"], "subjectUserId": item["subject_user_id"], "topicKey": item["topic_key"], "text": item["text"][:350], "subjects": subjects, "latestSourceTs": max(source_times, key=Decimal) if source_times else None})
            connection.execute("UPDATE memory_jobs SET metadata=? WHERE id=?", (_json(metadata), row["id"]))
            capture.update({"jobId": row["id"], "claimToken": token, "inputDigest": row["input_digest"], "baseRevision": row["base_revision"], "extractorVersion": row["extractor_version"]})
            return capture

    def inputs(self, *, tenant_id: str, job_id: str, claim_token: str) -> list[dict[str, Any]]:
        with self._connection() as connection:
            job = self._claimed(connection, tenant_id, job_id=job_id, claim_token=claim_token, state="running")
            rows = connection.execute("SELECT * FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? ORDER BY chunk_no", (tenant_id, job["capture_id"])).fetchall()
            metadata = json.loads(job["metadata"])
            previous = metadata.get("previousItems", [])
            return [{"chunkNo": row["chunk_no"], "sources": json.loads(row["payload"]), "previousItems": previous, "candidates": json.loads(row["candidate_result"]) if row["candidate_result"] is not None else None, "reconciled": metadata.get("reconciled") is True} for row in rows]

    def renew_claim(self, *, tenant_id: str, job_id: str, claim_token: str, lease_seconds: int = 120) -> None:
        with self._connection(write=True) as connection:
            self._claimed(connection, tenant_id, job_id=job_id, claim_token=claim_token, state="running")
            connection.execute("UPDATE memory_jobs SET lease_until=? WHERE tenant_id=? AND id=?", (self._clock() + max(30, lease_seconds), tenant_id, job_id))

    def stage_reconciled_candidates(self, *, tenant_id: str, job_id: str, claim_token: str,
                                    candidates: Sequence[Mapping[str, Any]], usage: Mapping[str, Any] | None = None) -> None:
        with self._connection(write=True) as connection:
            job = self._claimed(connection, tenant_id, job_id=job_id, claim_token=claim_token, state="running")
            chunks = connection.execute("SELECT * FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? ORDER BY chunk_no", (tenant_id, job["capture_id"])).fetchall()
            if not chunks or any(chunk["candidate_result"] is None for chunk in chunks):
                raise MemoryError("memory_candidates_incomplete")
            if len(candidates) > len(chunks) * MAX_CANDIDATES:
                raise MemoryError("memory_candidate_limit")
            # 같은 job의 실제 원문만 교차 참조하고 최종 결과 교체는 하나의 트랜잭션으로 끝낸다.
            sources = {source["sourceId"]: source for chunk in chunks for source in json.loads(chunk["payload"])}
            normalized = [validate_candidate(candidate, sources) for candidate in candidates]
            digest = canonical_digest(normalized)
            metadata = json.loads(job["metadata"])
            if metadata.get("reconciled"):
                if metadata.get("reconciledDigest") != digest:
                    raise MemoryError("memory_result_conflict")
                return
            connection.execute("UPDATE memory_job_inputs SET candidate_result='[]',result_version=? WHERE tenant_id=? AND capture_id=?", (job["extractor_version"], tenant_id, job["capture_id"]))
            connection.execute("UPDATE memory_job_inputs SET candidate_result=? WHERE tenant_id=? AND capture_id=? AND chunk_no=?", (_json(normalized), tenant_id, job["capture_id"], chunks[0]["chunk_no"]))
            metadata.update(reconciled=True, reconciledDigest=digest)
            metrics = dict(usage or {})
            connection.execute("UPDATE memory_jobs SET metadata=?,lease_until=?,model=?,input_tokens=input_tokens+?,output_tokens=output_tokens+?,cache_tokens=cache_tokens+? WHERE tenant_id=? AND id=?", (_json(metadata), self._clock() + 120, str(metrics.get("model", ""))[:128], max(0, int(metrics.get("inputTokens", 0))), max(0, int(metrics.get("outputTokens", 0))), max(0, int(metrics.get("cacheTokens", 0))), tenant_id, job_id))

    def stage_candidates(self, *, tenant_id: str, job_id: str, claim_token: str, chunk_no: int,
                         candidates: Sequence[Mapping[str, Any]], usage: Mapping[str, Any] | None = None) -> None:
        if len(candidates) > MAX_CANDIDATES:
            raise MemoryError("memory_candidate_limit")
        with self._connection(write=True) as connection:
            job = self._claimed(connection, tenant_id, job_id=job_id, claim_token=claim_token, state="running")
            chunk = connection.execute("SELECT * FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? AND chunk_no=?", (tenant_id, job["capture_id"], chunk_no)).fetchone()
            if chunk is None:
                raise MemoryError("memory_chunk_missing")
            sources = {item["sourceId"]: item for item in json.loads(chunk["payload"])}
            normalized = [validate_candidate(item, sources) for item in candidates]
            if chunk["candidate_result"] is not None:
                if canonical_digest(json.loads(chunk["candidate_result"])) != canonical_digest(normalized):
                    raise MemoryError("memory_result_conflict")
                return
            connection.execute("UPDATE memory_job_inputs SET candidate_result=?,result_version=? WHERE tenant_id=? AND capture_id=? AND chunk_no=?", (_json(normalized), job["extractor_version"], tenant_id, job["capture_id"], chunk_no))
            metrics = dict(usage or {})
            connection.execute("UPDATE memory_jobs SET processed_chunk_count=processed_chunk_count+1,lease_until=?,model=?,input_tokens=input_tokens+?,output_tokens=output_tokens+?,cache_tokens=cache_tokens+? WHERE id=?", (self._clock() + 120, str(metrics.get("model", ""))[:128], max(0, int(metrics.get("inputTokens", 0))), max(0, int(metrics.get("outputTokens", 0))), max(0, int(metrics.get("cacheTokens", 0))), job_id))

    def complete(self, *, tenant_id: str, job_id: str, claim_token: str) -> dict[str, Any]:
        with self._connection(write=True) as connection:
            job = self._claimed(connection, tenant_id, job_id=job_id, claim_token=claim_token, state="running")
            rows = connection.execute("SELECT * FROM memory_job_inputs WHERE tenant_id=? AND capture_id=? ORDER BY chunk_no", (tenant_id, job["capture_id"])).fetchall()
            if not rows or any(row["candidate_result"] is None for row in rows) or canonical_digest([row["chunk_digest"] for row in rows]) != job["input_digest"]:
                raise MemoryError("memory_candidates_incomplete")
            thread = connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?", (tenant_id, job["thread_id"])).fetchone()
            metadata = json.loads(job["metadata"])
            if metadata.get("baseItemsDigest") != self._items_digest(connection, tenant_id, thread["channel_id"], actor=metadata["actorId"]):
                raise MemoryError("memory_items_revision_stale")
            published = 0
            candidates = [candidate for row in rows for candidate in json.loads(row["candidate_result"])]
            # 여러 chunk가 같은 기존 ID를 정정하면 원본 시간상 가장 최신 정정만 반영한다.
            latest: dict[tuple[Any, ...], tuple[Decimal, Mapping[str, Any]]] = {}
            additions = []
            for candidate in candidates:
                if candidate["action"] in {"replace", "resolve"}:
                    key = (candidate.get("replacesId"), candidate["kind"], candidate["category"], candidate.get("subjectUserId"), candidate["topicKey"])
                    observed = self._candidate_latest_time(connection, tenant_id, candidate)
                    if key not in latest or observed > latest[key][0]:
                        latest[key] = (observed, candidate)
                else:
                    additions.append(candidate)
            # 오래된 add는 정정 완료 뒤 publish가 최신 근거 시간과 대조해 제외한다.
            ordered = [candidate for _, candidate in sorted(latest.values(), key=lambda value: value[0])] + sorted(additions, key=lambda candidate: self._candidate_latest_time(connection, tenant_id, candidate))
            for candidate in ordered:
                published += int(self._publish_candidate(connection, tenant_id, thread, candidate, job["trigger_request_id"], history_import=metadata.get("triggerKind") == "history_import"))
            connection.execute("UPDATE memory_threads SET memory_generation=memory_generation+1 WHERE id=?", (thread["id"],))
            connection.execute("DELETE FROM memory_summaries WHERE tenant_id=? AND scope_channel_id=?", (tenant_id, thread["channel_id"]))
            self._refresh_summaries(connection, tenant_id, thread["channel_id"])
            connection.execute("UPDATE memory_jobs SET state='succeeded',lease_until=0,claim_token='' WHERE id=?", (job_id,))
            self._clear_job_context(connection, job_id)
            connection.execute("DELETE FROM memory_job_inputs WHERE tenant_id=? AND capture_id=?", (tenant_id, job["capture_id"]))
            return {"state": "succeeded", "publishedCount": published}

    def _publish_candidate(self, connection: sqlite3.Connection, tenant: str, thread: sqlite3.Row, candidate: Mapping[str, Any], request_id: str, *, history_import: bool = False) -> bool:
        candidate = {**candidate, "topicKey": normalize_topic_key(candidate["topicKey"])}
        source_rows = []
        for ref in candidate["sources"]:
            source = connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND id=? AND state='current'", (tenant, ref["sourceId"])).fetchone()
            if not source or source["channel_id"] != thread["channel_id"] or self._source_deleted(connection, tenant, thread["channel_id"], source["message_ts"]):
                return False
            if _raw_only_channel(tenant, source["channel_id"]):
                return False
            source_rows.append(source)
        subject = candidate["subjectUserId"]
        topic_hash = canonical_digest(candidate["topicKey"].strip().casefold())
        suppression = connection.execute("SELECT * FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND deletion_kind IN ('subject_topic','item') AND COALESCE(subject_id,'')=COALESCE(?,'') AND topic_key_hash=? ORDER BY created_at DESC LIMIT 1", (tenant, thread["channel_id"], subject, topic_hash)).fetchone()
        if suppression:
            reallow = connection.execute("SELECT * FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND event_type='reallow' AND subject_id=? AND topic_key_hash=? ORDER BY created_at DESC LIMIT 1", (tenant, thread["channel_id"], subject, topic_hash)).fetchone()
            if not reallow or reallow["created_at"] < suppression["created_at"] or any(Decimal(source["message_ts"]) < Decimal(reallow["source_message_id"] or "0") for source in source_rows):
                return False
        if history_import and candidate["action"] == "add":
            # 뒤늦게 적재한 과거 업무·합의는 더 최신인 현재/완료 상태를 충돌·미완료로 되돌리지 않는다.
            # 실시간 처리와 개인 표준 선호의 기존 채널 간 시간 비교는 아래 경계를 그대로 따른다.
            # 사람 기억만 subject를 비교한다. 스레드·팀은 모델의 null/작성자 표기 차이로 보호를 우회하지 않는다.
            observed = max((Decimal(source["message_ts"]) for source in source_rows), default=Decimal(0))
            previous = connection.execute("""SELECT i.topic_key,s.message_ts FROM memory_items i
                JOIN memory_item_sources r ON r.tenant_id=i.tenant_id AND r.memory_id=i.id
                JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id
                WHERE i.tenant_id=? AND i.scope_channel_id=? AND i.kind=? AND i.category=?
                AND (i.kind!='person' OR COALESCE(i.subject_user_id,'')=COALESCE(?,''))
                AND i.status IN ('active','conflicted','resolved') AND s.state='current'""",
                (tenant, thread["channel_id"], candidate["kind"], candidate["category"], subject)).fetchall()
            if any(normalize_topic_key(item["topic_key"]) == candidate["topicKey"] and Decimal(item["message_ts"]) > observed for item in previous):
                return False
        if candidate["kind"] == "thread" and candidate["category"] == "task" and candidate["action"] == "add":
            # 이미 완료된 동일 원문을 다시 요약한 결과만 제거한다. 새 근거의 새 업무는 합치지 않는다.
            source_ids = {source["id"] for source in source_rows}
            resolved = connection.execute("SELECT id,topic_key FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND kind='thread' AND category='task' AND status='resolved' AND COALESCE(subject_user_id,'')=COALESCE(?,'')", (tenant, thread["channel_id"], subject)).fetchall()
            for previous in resolved:
                if normalize_topic_key(previous["topic_key"]) != candidate["topicKey"]:
                    continue
                prior_sources = {row[0] for row in connection.execute("SELECT source_id FROM memory_item_sources WHERE tenant_id=? AND memory_id=?", (tenant, previous["id"]))}
                if source_ids and source_ids.issubset(prior_sources):
                    return False
        observing = False
        if candidate["category"] == "interest":
            # 반복 관심사는 같은 스레드·봇 재진술을 반복 근거로 세지 않는다.
            independent = {row[0] for row in connection.execute("SELECT DISTINCT s.thread_id FROM memory_items i JOIN memory_item_sources r ON r.tenant_id=i.tenant_id AND r.memory_id=i.id JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE i.tenant_id=? AND i.scope_channel_id=? AND i.subject_user_id=? AND i.topic_key=? AND s.author_kind='human' AND s.author_id=? AND s.state='current'", (tenant, thread["channel_id"], subject, candidate["topicKey"], subject))}
            independent.update(source["thread_id"] for source in source_rows if source["author_kind"] == "human" and source["author_id"] == subject)
            observing = len(independent) < 3
        rows = connection.execute("SELECT i.* FROM memory_items i WHERE i.tenant_id=? AND i.kind=? AND i.category=? AND COALESCE(i.subject_user_id,'')=COALESCE(?,'') AND i.status IN ('active','conflicted','observing') AND NOT EXISTS (SELECT 1 FROM memory_item_sources r JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE r.tenant_id=i.tenant_id AND r.memory_id=i.id AND s.state!='current') ORDER BY i.updated_at DESC", (tenant, candidate["kind"], candidate["category"], subject)).fetchall()
        topic_rows = [row for row in rows if normalize_topic_key(row["topic_key"]) == candidate["topicKey"]]
        same = [row for row in topic_rows if row["scope_channel_id"] == thread["channel_id"]]
        standard_style = candidate["kind"] == "person" and candidate["category"] == "preference" and candidate["evidenceType"] == "self_statement" and standard_preference_value(candidate["topicKey"], candidate["text"]) is not None
        global_styles = [row for row in topic_rows if row["evidence_type"] == "self_statement" and standard_preference_value(candidate["topicKey"], row["text"]) is not None] if standard_style else []
        prior_for_time = global_styles if standard_style else same
        if candidate["kind"] == "person" and candidate["evidenceType"] == "self_statement" and prior_for_time:
            observed = max((Decimal(source["message_ts"]) for source in source_rows), default=Decimal(0))
            latest_existing = max((Decimal(source[0]) for item in prior_for_time for source in connection.execute("SELECT s.message_ts FROM memory_item_sources r JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE r.tenant_id=? AND r.memory_id=? AND s.state='current'", (tenant, item["id"]))), default=Decimal(0))
            if observed < latest_existing:
                return False
        preference_value = None
        if candidate["kind"] == "person" and candidate["category"] == "preference" and candidate["evidenceType"] == "self_statement" and candidate["action"] == "add":
            preference_value = standard_preference_value(candidate["topicKey"], candidate["text"])
        identical = next((row for row in same if row["text"] == candidate["text"] or candidate["category"] == "interest" or (
            preference_value is not None and row["evidence_type"] == "self_statement"
            and standard_preference_value(row["topic_key"], row["text"]) == preference_value
        )), None)
        if standard_style and candidate["action"] == "replace":
            identical = None
        now = self._clock()
        if identical:
            memory_id = identical["id"]
            if candidate["category"] == "interest" and not observing:
                connection.execute("UPDATE memory_items SET status='active',version=version+1,updated_at=?,valid_until=? WHERE id=?", (now, now + 90 * 86400, memory_id))
        else:
            memory_id = "mem_" + uuid.uuid4().hex
            status = "observing" if observing else "active"
            replace = candidate.get("replacesId")
            if candidate["action"] in {"replace", "resolve"}:
                target = next((row for row in (global_styles if standard_style else same) if row["id"] == replace), None)
                human_authors = {row["author_id"] for row in source_rows if row["author_kind"] == "human"}
                task_resolution = candidate["action"] == "resolve" and candidate["kind"] == "thread" and candidate["category"] == "task"
                if not target:
                    return False
                prior_times = [row[0] for row in connection.execute("SELECT s.message_ts FROM memory_item_sources r JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE r.tenant_id=? AND r.memory_id=?", (tenant, replace))]
                latest = max((Decimal(value) for value in prior_times), default=Decimal(0))
                if any(Decimal(source["message_ts"]) <= latest for source in source_rows):
                    return False
                if task_resolution:
                    assignees = {row[0] for row in connection.execute("SELECT user_id FROM memory_subjects WHERE tenant_id=? AND memory_id=? AND role='assignee'", (tenant, replace))}
                    has_completion = any(ref["relation"] != "disputes" and has_explicit_completion(ref["quote"]) and any(source["id"] == ref["sourceId"] and source["author_kind"] == "human" and source["author_id"] in assignees for source in source_rows) for ref in candidate["sources"])
                    if not human_authors or not human_authors.issubset(assignees) or not has_completion or candidate["evidenceType"] != "attributed_statement":
                        return False
                elif not subject or human_authors != {subject} or candidate["evidenceType"] != "self_statement":
                    return False
                elif candidate["action"] == "replace" and not any(ref["relation"] == "corrects" for ref in candidate["sources"]):
                    return False
                connection.execute("UPDATE memory_items SET status='superseded',version=version+1,updated_at=? WHERE id=?", (now, replace))
                if standard_style:
                    # 본인의 더 최신 직접 정정은 같은 표준 선호의 과거 충돌도 해소한다.
                    for previous in global_styles:
                        if previous["id"] != replace:
                            connection.execute("UPDATE memory_items SET status='superseded',version=version+1,updated_at=? WHERE id=?", (now, previous["id"]))
                    connection.execute("DELETE FROM memory_summaries WHERE tenant_id=? AND target_kind='person' AND target_id=?", (tenant, subject))
                if candidate["action"] == "resolve":
                    status = "resolved"
            elif same:
                status = "conflicted"
                connection.execute("UPDATE memory_items SET status='conflicted',version=version+1,updated_at=? WHERE tenant_id=? AND id IN (" + ",".join("?" for _ in same) + ")", (now, tenant, *(row["id"] for row in same)))
            elif candidate["kind"] == "thread" and candidate["category"] == "task" and candidate["evidenceType"] == "attributed_statement":
                # 한 수집 작업 안에서 처음 발견한 일도 담당자의 직접 완료 근거가 있으면 완료로 저장한다.
                assignees = {person["userId"] for person in candidate.get("subjects", []) if person["role"] == "assignee"}
                authors = {source["author_id"] for source in source_rows if source["author_kind"] == "human"}
                completed_at = [Decimal(source["message_ts"]) for source in source_rows if source["author_kind"] == "human" and source["author_id"] in assignees and any(ref["sourceId"] == source["id"] and ref["relation"] != "disputes" and has_explicit_completion(ref["quote"]) for ref in candidate["sources"])]
                if assignees and authors and authors.issubset(assignees) and completed_at and max(completed_at) >= max(Decimal(source["message_ts"]) for source in source_rows):
                    status = "resolved"
            connection.execute("INSERT INTO memory_items(id,tenant_id,scope_channel_id,origin_thread_id,kind,category,subject_user_id,topic_key,text,evidence_type,status,valid_from,valid_until,observed_at,updated_at,replaces_id,extractor_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (memory_id, tenant, thread["channel_id"], thread["id"], candidate["kind"], candidate["category"], subject, candidate["topicKey"], candidate["text"], candidate["evidenceType"], status, now, now + 90 * 86400 if candidate["evidenceType"] == "inference" else None, now, now, replace, EXTRACTOR_VERSION))
        for ref in candidate["sources"]:
            connection.execute("INSERT OR IGNORE INTO memory_item_sources(tenant_id,memory_id,source_id,relation,evidence_span) VALUES(?,?,?,?,?)", (tenant, memory_id, ref["sourceId"], ref["relation"], ref["quote"]))
        for person in candidate.get("subjects", []):
            connection.execute("INSERT OR IGNORE INTO memory_subjects(tenant_id,memory_id,user_id,role) VALUES(?,?,?,?)", (tenant, memory_id, person["userId"], person["role"]))
        for ref in candidate["sources"]:
            spans = [row[0] for row in connection.execute("SELECT DISTINCT r.evidence_span FROM memory_item_sources r JOIN memory_items i ON i.tenant_id=r.tenant_id AND i.id=r.memory_id WHERE r.tenant_id=? AND r.source_id=? AND i.status IN ('active','resolved','observing') AND r.evidence_span!='' ORDER BY r.evidence_span", (tenant, ref["sourceId"]))]
            # 원문은 임시 payload에만 두고 영속 source에는 실제 채택한 짧은 인용만 둔다.
            connection.execute("UPDATE memory_sources SET excerpt=? WHERE tenant_id=? AND id=? AND state='current'", ("\n".join(spans)[:1000], tenant, ref["sourceId"]))
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,memory_id,event_type,request_id,created_at) VALUES(?,?,?,?,?,?,?)", ("evt_" + uuid.uuid4().hex, tenant, thread["channel_id"], memory_id, "upsert", request_id, now))
        return identical is None

    def fail(self, *, tenant_id: str, job_id: str, claim_token: str, error_type: str, retry: bool = True,
             usage: Mapping[str, Any] | None = None) -> dict[str, Any]:
        with self._connection(write=True) as connection:
            row = connection.execute("SELECT * FROM memory_jobs WHERE tenant_id=? AND id=?", (tenant_id, job_id)).fetchone()
            if not row or row["state"] != "running" or row["claim_token"] != claim_token:
                raise MemoryError("memory_claim_stale")
            thread = connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?", (tenant_id, row["thread_id"])).fetchone()
            state = "retry" if retry and row["attempts"] < 3 else "failed"
            if thread["deletion_epoch"] != row["deletion_epoch"] or thread["input_revision"] != row["base_revision"]:
                state = "superseded"
            safe_error = error_type if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,79}", error_type) else "MemoryExtractionError"
            connection.execute("UPDATE memory_jobs SET state=?,claim_token='',lease_until=0,last_error_type=?,next_attempt_at=? WHERE id=?", (state, safe_error, self._clock() + 10 * max(1, row["attempts"]), job_id))
            if usage:
                connection.execute("UPDATE memory_jobs SET model=?,input_tokens=input_tokens+?,output_tokens=output_tokens+?,cache_tokens=cache_tokens+? WHERE id=?", (str(usage.get("model", ""))[:128], max(0, int(usage.get("inputTokens", 0))), max(0, int(usage.get("outputTokens", 0))), max(0, int(usage.get("cacheTokens", 0))), job_id))
            if state in {"failed", "superseded"}:
                connection.execute("DELETE FROM memory_job_inputs WHERE tenant_id=? AND capture_id=?", (tenant_id, row["capture_id"]))
                self._clear_job_context(connection, job_id)
            return {"state": state}

    def _candidate_latest_time(self, connection: sqlite3.Connection, tenant: str, candidate: Mapping[str, Any]) -> Decimal:
        times = []
        for ref in candidate["sources"]:
            row = connection.execute("SELECT message_ts FROM memory_sources WHERE tenant_id=? AND id=?", (tenant, ref["sourceId"])).fetchone()
            if row:
                times.append(Decimal(row[0]))
        return max(times, default=Decimal(0))

    def _item(self, connection: sqlite3.Connection, row: sqlite3.Row) -> dict[str, Any]:
        thread = connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?", (row["tenant_id"], row["origin_thread_id"])).fetchone()
        sources = connection.execute("SELECT s.*,r.relation FROM memory_item_sources r JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE r.tenant_id=? AND r.memory_id=? ORDER BY s.message_ts,s.id", (row["tenant_id"], row["id"])).fetchall()
        references = []
        for source in sources:
            # 허브의 교차 채널 조회도 요청 메타데이터가 아닌 저장된 원본 채널 종류로 판정한다.
            source_thread = connection.execute("SELECT thread_ts,channel_kind FROM memory_threads WHERE tenant_id=? AND channel_id=? AND id=?", (row["tenant_id"], source["channel_id"], source["thread_id"])).fetchone()
            if source_thread is None:
                raise MemoryError("memory_source_scope_invalid")
            references.append({"sourceId": source["id"], "channelId": source["channel_id"], "channelKind": source_thread["channel_kind"], "threadTs": source_thread["thread_ts"], "messageTs": source["message_ts"], "revisionHash": source["revision_hash"], "permalink": source["permalink"], "authorId": source["author_id"], "authorKind": source["author_kind"], "state": source["state"], "relation": source["relation"]})
        return {
            "memoryId": row["id"], "kind": row["kind"], "category": row["category"],
            "subjectUserId": row["subject_user_id"], "topicKey": row["topic_key"], "text": row["text"],
            "evidenceType": row["evidence_type"], "status": row["status"], "version": row["version"],
            "updatedAt": _iso(row["updated_at"]), "threadId": thread["id"], "threadTs": thread["thread_ts"],
            "deletionEpoch": thread["deletion_epoch"], "sources": references,
        }

    def _personal_style_candidates(self, connection: sqlite3.Connection, tenant: str, actor: str) -> list[dict[str, Any]]:
        # 원문·출처는 검증용 후보에만 남긴다. 반대 값이나 미해결 충돌은 전 채널에서 적용하지 않는다.
        if not actor:
            return []
        rows = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND kind='person' AND category='preference' AND subject_user_id=? AND evidence_type='self_statement' AND status IN ('active','conflicted') AND (valid_until IS NULL OR valid_until>?) ORDER BY updated_at DESC LIMIT 256", (tenant, actor, self._clock())).fetchall()
        grouped: dict[str, list[tuple[str | None, dict[str, Any]]]] = {}
        for row in rows:
            topic = normalize_topic_key(row["topic_key"])
            if topic not in PERSONAL_STYLE_TOPICS:
                continue
            item = self._item(connection, row)
            if not item["sources"] or any(source["state"] != "current" or source["authorKind"] != "human" or source["authorId"] != actor or source["relation"] == "disputes" for source in item["sources"]):
                continue
            item["topicKey"] = topic
            value = standard_preference_value(topic, row["text"])
            grouped.setdefault(topic, []).append((value, item))
        result = []
        for topic, choices in sorted(grouped.items()):
            values = {value for value, _ in choices}
            if None in values or len(values) != 1 or any(item["status"] != "active" for _, item in choices):
                continue
            value, item = max(choices, key=lambda pair: (max(Decimal(source["messageTs"]) for source in pair[1]["sources"]), pair[1]["memoryId"]))
            result.append({**item, "styleOnly": True, "styleValue": value})
        return result

    def retrieval_candidates(self, tenant_id: str, channel_id: str, actor_id: str, thread_ts: str,
                             question: str, limit: int = 8, mentioned_user_ids: Sequence[str] = (),
                             period: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
        # scope를 SQL에서 먼저 제한하고 사용자의 질문으로만 관련성을 평가한다.
        words = set(re.findall(r"[가-힣A-Za-z0-9_]{2,}", question.casefold()))
        dated = period is not None and not mentioned_user_ids
        period_sql, period_arguments = "", ()
        if period is not None:
            values = [period.get(key) for key in ("startTs", "endTs", "cutoffTs")] if isinstance(period, Mapping) else []
            if (len(values) != 3 or any(not isinstance(value, str) or not re.fullmatch(
                    r"-?[0-9]{1,20}\.[0-9]{1,9}" if index < 2 else r"[0-9]{1,20}\.[0-9]{1,9}", value)
                    for index, value in enumerate(values)) or Decimal(values[0]) >= Decimal(values[1])
                    or any(not Decimal(-(2**63)) <= Decimal(value) < Decimal(2**63) for value in values)):
                raise MemoryError("memory_query_period_invalid")
            bounds = []
            for value in values:
                boundary = Decimal(value)
                seconds = int(boundary)
                fraction = int((boundary - seconds) * 1_000_000_000)
                if fraction < 0:
                    seconds, fraction = seconds - 1, fraction + 1_000_000_000
                bounds.extend((seconds, f"{fraction:09d}"))
            # SQL의 256개 제한 전에 모든 근거 시각을 비교한다. 정수 초와 9자리 소수를
            # 나눠 비교해 REAL 반올림 없이 자정·원 질문 cutoff의 경계를 보존한다.
            stamp = "(CAST(s.message_ts AS INTEGER), substr(substr(s.message_ts,instr(s.message_ts,'.')+1)||'000000000',1,9))"
            within = f"{stamp}>=(?,?) AND {stamp}<(?,?) AND {stamp}<=(?,?)"
            edited_stamp = stamp.replace("s.message_ts", "s.edited_at")
            # 기간 밖의 나중 편집은 허용하되 원 질문 이후 편집된 내용은 당시 근거로 쓰지 않는다.
            within += f" AND (s.edited_at='' OR {edited_stamp}<=(?,?))"
            sources = "FROM memory_item_sources r JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE r.tenant_id=i.tenant_id AND r.memory_id=i.id"
            period_sql = f" AND EXISTS (SELECT 1 {sources} AND ({within})) AND NOT EXISTS (SELECT 1 {sources} AND NOT ({within}))"
            source_bounds = (*bounds, *bounds[-2:])
            period_arguments = (*source_bounds, *source_bounds)
        with self._connection() as connection:
            # 고정 허브만 같은 tenant의 비공개 원본 채널을 더 검색한다. DM·공개 채널은 그대로 격리한다.
            rows = connection.execute("SELECT i.*,t.thread_ts FROM memory_items i JOIN memory_threads t ON t.tenant_id=i.tenant_id AND t.id=i.origin_thread_id WHERE i.tenant_id=? AND (i.scope_channel_id=? OR (? AND t.channel_kind='private_channel' AND t.channel_id=i.scope_channel_id)) AND i.status IN ('active','resolved') AND (i.valid_until IS NULL OR i.valid_until>?)" + period_sql + " ORDER BY i.updated_at DESC LIMIT 256", (tenant_id, channel_id, is_private_memory_hub(tenant_id, channel_id), self._clock(), *period_arguments)).fetchall()
            ranked = [(100, 0, item) for item in self._personal_style_candidates(connection, tenant_id, actor_id)]
            for row in rows:
                own = row["kind"] == "person" and row["subject_user_id"] == actor_id and row["category"] == "preference"
                if own and normalize_topic_key(row["topic_key"]) in PERSONAL_STYLE_TOPICS and standard_preference_value(normalize_topic_key(row["topic_key"]), row["text"]) is not None:
                    # 현재 채널의 같은 선호도 표준 값으로만 보내 전 채널 충돌 판정을 우회하지 않는다.
                    continue
                target = row["subject_user_id"] in mentioned_user_ids
                if not target and mentioned_user_ids:
                    target = any(person[0] in mentioned_user_ids for person in connection.execute("SELECT user_id FROM memory_subjects WHERE tenant_id=? AND memory_id=?", (tenant_id, row["id"])))
                # 교차 채널에서 같은 timestamp가 있어도 현재 스레드로 가산하지 않는다.
                current = row["scope_channel_id"] == channel_id and row["thread_ts"] == thread_ts
                matching = sum(word in (row["topic_key"] + " " + row["text"]).casefold() for word in words)
                # 작성자 없는 날짜 요약은 기간 자체로 후보를 고르고 주제 점수는 그대로 쓴다.
                if not (own or target or current or matching or dated):
                    continue
                item = self._item(connection, row)
                # 정밀 helper도 ranking과 8개/3스레드/본문 예산보다 먼저 적용한다.
                # 본인 표준 선호는 별도 ranked 목록의 표준 선호 예외로 그대로 유지한다.
                if period is not None and (not item["sources"] or any(
                        not is_within_live_query_period(source["messageTs"], period) for source in item["sources"])):
                    continue
                if not item["sources"] or any(
                    source["state"] != "current" or not can_read_memory_source(
                        tenant_id, channel_id, source["channelId"], source["channelKind"])
                    for source in item["sources"]
                ):
                    continue
                ranked.append((int(own) * 10 + int(target) * 9 + int(current) * 6 + matching, row["updated_at"], item))
            ranked.sort(key=lambda value: (value[0], value[1]), reverse=True)
            selected: list[dict[str, Any]] = []
            threads: set[tuple[str, str]] = set()
            chars = 0
            for _, _, item in ranked:
                candidate_threads = threads | {(source["channelId"], source["threadTs"]) for source in item["sources"]}
                if len(candidate_threads) > 3 or chars + len(item["text"]) > 4000:
                    continue
                selected.append(item)
                threads, chars = candidate_threads, chars + len(item["text"])
                if len(selected) >= min(8, max(1, limit)):
                    break
            return selected

    def load_verified_items(self, *, tenant_id: str, channel_id: str, candidates: Sequence[Mapping[str, Any]],
                            verified_sources: Sequence[Mapping[str, Any]], limit: int = 8,
                            actor_id: str | None = None) -> list[dict[str, Any]]:
        return self._load_items(tenant_id=tenant_id, channel_id=channel_id, candidates=candidates,
                                verified_sources=verified_sources, limit=limit, actor_id=actor_id)

    def load_stored_items(self, *, tenant_id: str, channel_id: str, candidates: Sequence[Mapping[str, Any]],
                          limit: int = 8, actor_id: str | None = None) -> list[dict[str, Any]]:
        """현재 DB의 저장 근거만 검증한다. Slack 현재 원문을 확인했다는 의미는 아니다."""
        return self._load_items(tenant_id=tenant_id, channel_id=channel_id, candidates=candidates,
                                verified_sources=None, limit=limit, actor_id=actor_id)

    def _load_items(self, *, tenant_id: str, channel_id: str, candidates: Sequence[Mapping[str, Any]],
                    verified_sources: Sequence[Mapping[str, Any]] | None, limit: int,
                    actor_id: str | None) -> list[dict[str, Any]]:
        verified = ({str(source.get("sourceId") or ""): str(source.get("revisionHash") or "")
                     for source in verified_sources if source.get("status") == "verified"}
                    if verified_sources is not None else None)
        with self._connection() as connection:
            # 버전, 출처 digest와 삭제 이력은 하나의 저장 스냅샷에서 비교한다.
            connection.execute("BEGIN")
            result = []
            styles = {item["memoryId"]: item for item in self._personal_style_candidates(connection, tenant_id, actor_id or "")}
            for candidate in candidates[:8]:
                if candidate.get("styleOnly") is True:
                    item = styles.get(str(candidate.get("memoryId") or ""))
                    if item is None or item["version"] != candidate.get("version") or item["styleValue"] != candidate.get("styleValue"):
                        continue
                else:
                    # 검증 결과를 받아도 현재 DB의 tenant·원채널 종류를 다시 확인한다.
                    row = connection.execute("SELECT i.* FROM memory_items i JOIN memory_threads t ON t.tenant_id=i.tenant_id AND t.id=i.origin_thread_id WHERE i.tenant_id=? AND (i.scope_channel_id=? OR (? AND t.channel_kind='private_channel' AND t.channel_id=i.scope_channel_id)) AND i.id=? AND i.status IN ('active','resolved') AND (i.valid_until IS NULL OR i.valid_until>?)", (tenant_id, channel_id, is_private_memory_hub(tenant_id, channel_id), candidate.get("memoryId"), self._clock())).fetchone()
                    if not row or row["version"] != candidate.get("version"):
                        continue
                    item = self._item(connection, row)
                if item["deletionEpoch"] != candidate.get("deletionEpoch") or canonical_digest(item["sources"]) != canonical_digest(candidate.get("sources", [])):
                    continue
                if not item["sources"] or any(source["state"] != "current" or (
                        verified is not None and verified.get(source["sourceId"]) != source["revisionHash"])
                        for source in item["sources"]):
                    continue
                if candidate.get("styleOnly") is not True and any(
                    not can_read_memory_source(tenant_id, channel_id, source["channelId"], source["channelKind"])
                    for source in item["sources"]
                ):
                    continue
                # actor·충돌·원본 revision을 확인한 뒤 모델용 DTO에는 출처 메타데이터조차 넣지 않는다.
                result.append({"kind": "personal_style", "topicKey": item["topicKey"], "value": item["styleValue"]} if candidate.get("styleOnly") is True else item)
                if len(result) >= min(8, max(1, limit)):
                    break
            connection.execute("COMMIT")
            return result

    def list_own_memories(self, tenant_id: str, channel_id: str, actor_id: str, category: str | None = None) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND kind='person' AND subject_user_id=? AND status IN ('active','conflicted','resolved') AND (? IS NULL OR category=?) ORDER BY updated_at DESC LIMIT 100", (tenant_id, channel_id, actor_id, category, category)).fetchall()
            return [self._item(connection, row) for row in rows]

    def _invalidate_source_items(self, connection: sqlite3.Connection, tenant: str, source_id: str, *, deleted: bool) -> int:
        # 조회 원문은 출처 revision이 무효화되는 같은 transaction에서 제거한다.
        connection.execute("DELETE FROM memory_source_texts WHERE tenant_id=? AND source_id=?", (tenant, source_id))
        ids = [row[0] for row in connection.execute("SELECT memory_id FROM memory_item_sources WHERE tenant_id=? AND source_id=?", (tenant, source_id))]
        for identity in ids:
            connection.execute("UPDATE memory_items SET status=?,text=CASE WHEN ? THEN '' ELSE text END,version=version+1,updated_at=? WHERE tenant_id=? AND id=?", ("deleted" if deleted else "conflicted", int(deleted), self._clock(), tenant, identity))
        # 원본의 발췌뿐 아니라 relationship의 복제된 인용도 제거한다.
        connection.execute("UPDATE memory_item_sources SET evidence_span='' WHERE tenant_id=? AND source_id=?", (tenant, source_id))
        return len(ids)

    def _cancel_scope_jobs(self, connection: sqlite3.Connection, tenant: str, channel: str) -> None:
        connection.execute("UPDATE memory_threads SET deletion_epoch=deletion_epoch+1,memory_generation=memory_generation+1 WHERE tenant_id=? AND channel_id=?", (tenant, channel))
        connection.execute("DELETE FROM memory_job_inputs WHERE tenant_id=? AND capture_id IN (SELECT j.capture_id FROM memory_jobs j JOIN memory_threads t ON t.tenant_id=j.tenant_id AND t.id=j.thread_id WHERE j.tenant_id=? AND t.channel_id=? AND j.state IN ('collecting','ready','running','retry'))", (tenant, tenant, channel))
        connection.execute("UPDATE memory_jobs SET state='canceled',claim_token='',lease_until=0 WHERE tenant_id=? AND thread_id IN (SELECT id FROM memory_threads WHERE tenant_id=? AND channel_id=?) AND state IN ('collecting','ready','running','retry')", (tenant, tenant, channel))
        connection.execute("DELETE FROM memory_summaries WHERE tenant_id=? AND scope_channel_id=?", (tenant, channel))
        for row in connection.execute("SELECT id FROM memory_jobs WHERE tenant_id=? AND thread_id IN (SELECT id FROM memory_threads WHERE tenant_id=? AND channel_id=?)", (tenant, tenant, channel)).fetchall():
            self._clear_job_context(connection, row["id"])

    def source_event(self, *, tenant_id: str, channel_id: str, message_ts: str, event_type: str,
                     event_id: str, revision_hash: str | None = None) -> dict[str, Any]:
        if event_type not in {"changed", "deleted", "unavailable"}:
            raise MemoryError("memory_source_event_invalid")
        with self._connection(write=True) as connection:
            previous = connection.execute("SELECT id,scope_channel_id FROM memory_events WHERE tenant_id=? AND request_id=? AND event_type=?", (tenant_id, event_id, "source_" + event_type)).fetchall()
            if previous:
                if previous[0]["scope_channel_id"] != channel_id:
                    raise MemoryError("memory_event_scope_conflict")
                return {"invalidatedCount": 0, "eventId": previous[0]["id"]}
            sources = connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=?", (tenant_id, channel_id, message_ts)).fetchall()
            known_message = bool(sources)
            if event_type == "changed" and revision_hash:
                sources = [source for source in sources if source["revision_hash"] != revision_hash]
            pending = []
            for chunk in connection.execute("SELECT p.payload FROM memory_job_inputs p JOIN memory_jobs j ON j.tenant_id=p.tenant_id AND j.capture_id=p.capture_id JOIN memory_threads t ON t.tenant_id=j.tenant_id AND t.id=j.thread_id WHERE p.tenant_id=? AND t.channel_id=? AND j.state='collecting'", (tenant_id, channel_id)).fetchall():
                for source in json.loads(chunk["payload"]):
                    if source["messageTs"] == message_ts and (event_type != "changed" or source["revisionHash"] != revision_hash):
                        pending.append({"id": source["sourceId"], "tenant_id": tenant_id, "channel_id": channel_id, "message_ts": message_ts, "state": "pending"})
            if not sources and not pending:
                collecting = connection.execute("SELECT 1 FROM memory_jobs j JOIN memory_threads t ON t.tenant_id=j.tenant_id AND t.id=j.thread_id WHERE j.tenant_id=? AND t.channel_id=? AND j.state='collecting' LIMIT 1", (tenant_id, channel_id)).fetchone()
                if collecting:
                    # 읽었지만 아직 전송하지 않은 원문도 오래된 capture token으로 저장하지 못한다.
                    self._cancel_scope_jobs(connection, tenant_id, channel_id)
                raw_only = _raw_only_scope(tenant_id, channel_id, message_ts)
                if not known_message and (raw_only or self._raw_import_covers(connection, tenant_id, channel_id, message_ts)):
                    if raw_only and event_type != "deleted":
                        # 새 편집 원문은 live writer가 최신 revision으로 보존한다. 메시지 전체 금지는 남기지 않는다.
                        return {"invalidatedCount": 0, "eventId": None}
                    # 원문 수집 전 도착한 수정/삭제도 승인 범위에만 기록해 옛 snapshot 재유입을 막는다.
                    identity = "evt_" + uuid.uuid4().hex
                    self._apply_event(connection, {"id": identity, "tenantId": tenant_id,
                        "scopeChannelId": channel_id, "eventType": "source_" + event_type,
                        "requestId": event_id, "deletionKind": "source" if event_type == "deleted" else "source_redaction",
                        "sourceMessageId": message_ts, "cutoffAt": self._clock(), "durabilityState": "pending"})
                    return {"invalidatedCount": 0, "eventId": identity}
                # 이 endpoint는 새 source나 새 scope를 만드는 수집 경로가 아니다.
                return {"invalidatedCount": 0, "eventId": None}
            count = 0
            for source in sources:
                count += self._invalidate_source_items(connection, tenant_id, source["id"], deleted=event_type == "deleted")
                connection.execute("UPDATE memory_sources SET state=?,excerpt='' WHERE tenant_id=? AND id=?", ("deleted" if event_type == "deleted" else "unavailable", tenant_id, source["id"]))
            identity = "evt_" + uuid.uuid4().hex
            # changed도 과거 snapshot의 이전 revision을 되살리지 않도록 journal에 보존한다.
            if event_type == "deleted":
                connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,event_type,request_id,deletion_kind,source_message_id,cutoff_at,durability_state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)", (identity, tenant_id, channel_id, "source_deleted", event_id, "source", message_ts, self._clock(), "pending", self._clock()))
            else:
                for known_source in [*sources, *pending]:
                    identity = self._record_revision_invalidation(connection, known_source, event_id, event_type=event_type)
            if pending or any(source["state"] == "current" for source in sources):
                self._cancel_scope_jobs(connection, tenant_id, channel_id)
            return {"invalidatedCount": count, "eventId": identity}

    def _raw_import_covers(self, connection: sqlite3.Connection, tenant: str, channel: str, message_ts: str) -> bool:
        pattern = r"[0-9]{1,20}\.[0-9]{1,9}"
        if not isinstance(message_ts, str) or not re.fullmatch(pattern, message_ts):
            return False
        stamp = Decimal(message_ts)
        ranges = connection.execute("SELECT start_ts,cutoff_ts FROM memory_raw_imports WHERE tenant_id=? AND channel_id=? AND state='collecting'", (tenant, channel)).fetchall()
        return any(re.fullmatch(pattern, row["start_ts"]) and re.fullmatch(pattern, row["cutoff_ts"])
                   and Decimal(row["start_ts"]) <= stamp <= Decimal(row["cutoff_ts"]) for row in ranges)

    def _record_revision_invalidation(self, connection: sqlite3.Connection, source: Mapping[str, Any], request_id: str, *, event_type: str = "changed") -> str:
        identity = "evt_" + canonical_digest([source["tenant_id"], request_id, source["id"], event_type])
        connection.execute("INSERT OR IGNORE INTO memory_events(id,tenant_id,scope_channel_id,source_id,event_type,request_id,deletion_kind,source_message_id,cutoff_at,durability_state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)", (identity, source["tenant_id"], source["channel_id"], source["id"], "source_" + event_type, request_id, "source_revision", source["message_ts"], self._clock(), "pending", self._clock()))
        return identity

    def forget_item(self, *, tenant_id: str, channel_id: str, actor_id: str, memory_id: str, request_id: str) -> dict[str, Any]:
        with self._connection(write=True) as connection:
            existing = connection.execute("SELECT * FROM memory_events WHERE tenant_id=? AND request_id=? AND event_type='forget_item'", (tenant_id, request_id)).fetchone()
            if existing:
                if existing["memory_id"] != memory_id or existing["actor_id"] != actor_id or existing["scope_channel_id"] != channel_id:
                    raise MemoryError("memory_forget_identity_conflict")
                return self._event(existing)
            item = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND id=? AND kind='person' AND subject_user_id=?", (tenant_id, channel_id, memory_id, actor_id)).fetchone()
            if item is None:
                raise MemoryError("memory_own_item_required")
            identity = "evt_" + uuid.uuid4().hex
            event = {"id": identity, "tenantId": tenant_id, "scopeChannelId": channel_id, "memoryId": memory_id, "sourceId": None, "eventType": "forget_item", "actorId": actor_id, "requestId": request_id, "deletionKind": "item", "subjectId": actor_id, "topicKeyHash": canonical_digest(normalize_topic_key(item["topic_key"])), "sourceMessageId": None, "cutoffAt": self._clock(), "durabilityState": "pending"}
            self._apply_event(connection, event)
            return self._event(connection.execute("SELECT * FROM memory_events WHERE id=?", (identity,)).fetchone())

    def suppress_topic(self, *, tenant_id: str, channel_id: str, actor_id: str, topic_key: str, request_id: str) -> dict[str, Any]:
        """본인의 같은 주제를 다음 자동 추출에서도 제외하는 내용 없는 suppression이다."""

        topic = normalize_topic_key(topic_key)
        if not topic or len(topic) > 160 or not is_safe_memory_text(topic):
            raise MemoryError("memory_topic_invalid")
        with self._connection(write=True) as connection:
            previous = connection.execute("SELECT * FROM memory_events WHERE tenant_id=? AND request_id=? AND event_type='suppress_topic'", (tenant_id, request_id)).fetchone()
            if previous:
                if previous["actor_id"] != actor_id or previous["scope_channel_id"] != channel_id or previous["topic_key_hash"] != canonical_digest(topic):
                    raise MemoryError("memory_topic_request_conflict")
                return self._event(previous)
            event = {"id": "evt_" + uuid.uuid4().hex, "tenantId": tenant_id, "scopeChannelId": channel_id,
                     "eventType": "suppress_topic", "actorId": actor_id, "requestId": request_id,
                     "deletionKind": "subject_topic", "subjectId": actor_id, "topicKeyHash": canonical_digest(topic),
                     "cutoffAt": self._clock(), "durabilityState": "pending"}
            self._apply_event(connection, event)
            return self._event(connection.execute("SELECT * FROM memory_events WHERE id=?", (event["id"],)).fetchone())

    def operator_forget(self, *, tenant_id: str, channel_id: str, memory_id: str, request_id: str) -> dict[str, Any]:
        """HTTP에 노출하지 않는 로컬 운영자 삭제 경계다."""

        with self._connection(write=True) as connection:
            previous = connection.execute("SELECT * FROM memory_events WHERE tenant_id=? AND request_id=? AND event_type='operator_forget'", (tenant_id, request_id)).fetchone()
            if previous:
                if previous["memory_id"] != memory_id or previous["scope_channel_id"] != channel_id:
                    raise MemoryError("memory_forget_identity_conflict")
                return self._event(previous)
            item = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND id=?", (tenant_id, channel_id, memory_id)).fetchone()
            if item is None:
                raise MemoryError("memory_item_missing")
            event = {"id": "evt_" + uuid.uuid4().hex, "tenantId": tenant_id, "scopeChannelId": channel_id,
                     "memoryId": memory_id, "eventType": "operator_forget", "requestId": request_id,
                     "deletionKind": "item", "subjectId": item["subject_user_id"],
                     "topicKeyHash": canonical_digest(normalize_topic_key(item["topic_key"])),
                     "cutoffAt": self._clock(), "durabilityState": "pending"}
            self._apply_event(connection, event)
            return self._event(connection.execute("SELECT * FROM memory_events WHERE id=?", (event["id"],)).fetchone())

    def set_own_preference(self, *, tenant_id: str, channel_id: str, actor_id: str, thread_ts: str,
                           current_message_ts: str, request_id: str, topic_key: str, text: str,
                           source: Mapping[str, Any]) -> dict[str, Any]:
        topic_key = normalize_topic_key(topic_key)
        if _raw_only_channel(tenant_id, channel_id):
            raise MemoryError("memory_raw_only_scope")
        if topic_key not in PERSONAL_STYLE_TOPICS or standard_preference_value(topic_key, text) is None or not is_safe_memory_text(text) or len(text) > 200:
            raise MemoryError("memory_preference_invalid")
        normalized = normalize_source(source, tenant_id=tenant_id, channel_id=channel_id, thread_ts=thread_ts, current_message_ts=current_message_ts)
        if normalized is None or normalized["authorKind"] != "human" or normalized["authorId"] != actor_id or normalized["messageTs"] != current_message_ts:
            raise MemoryError("memory_preference_author_invalid")
        with self._connection(write=True) as connection:
            previous = connection.execute("SELECT i.* FROM memory_events e JOIN memory_items i ON i.tenant_id=e.tenant_id AND i.id=e.memory_id WHERE e.tenant_id=? AND e.request_id=? AND e.event_type='upsert' LIMIT 1", (tenant_id, request_id)).fetchone()
            if previous:
                if previous["scope_channel_id"] != channel_id or previous["subject_user_id"] != actor_id or normalize_topic_key(previous["topic_key"]) != topic_key or previous["text"] != text or previous["status"] != "active":
                    raise MemoryError("memory_preference_request_conflict")
                return self._item(connection, previous)
            thread = self._thread(connection, tenant_id, channel_id, thread_ts)
            self._register_source(connection, tenant_id, thread, normalized)
            # 명시 재허용은 이 새 요청 이후의 직접 근거만 허용한다.
            connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,source_id,event_type,actor_id,request_id,subject_id,topic_key_hash,source_message_id,cutoff_at,reallow_request_id,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", ("evt_" + uuid.uuid4().hex, tenant_id, channel_id, normalized["sourceId"], "reallow", actor_id, request_id, actor_id, canonical_digest(topic_key), current_message_ts, self._clock(), request_id, self._clock()))
            old = [item for item in connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND subject_user_id=? AND kind='person' AND category='preference' AND status IN ('active','conflicted')", (tenant_id, actor_id)) if normalize_topic_key(item["topic_key"]) == topic_key]
            latest = max((Decimal(ref[0]) for row in old for ref in connection.execute("SELECT COALESCE(NULLIF(s.edited_at,''),s.message_ts) FROM memory_item_sources r JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE r.tenant_id=? AND r.memory_id=?", (tenant_id, row["id"]))), default=Decimal(0))
            if latest > Decimal(normalized["editedAt"] or current_message_ts):
                raise MemoryError("memory_preference_stale")
            # 개인 표준 선호의 명시 변경만 채널 전체에서 이전 값을 대체한다.
            for row in old:
                connection.execute("UPDATE memory_items SET status='superseded',version=version+1,updated_at=? WHERE id=?", (self._clock(), row["id"]))
            candidate = {"kind": "person", "category": "preference", "subjectUserId": actor_id, "topicKey": topic_key, "text": text, "evidenceType": "self_statement", "sources": [{"sourceId": normalized["sourceId"], "quote": normalized["text"], "relation": "supports"}], "subjects": [{"userId": actor_id, "role": "subject"}], "action": "add", "replacesId": None}
            self._publish_candidate(connection, tenant_id, thread, candidate, request_id)
            connection.execute("UPDATE memory_threads SET input_revision=input_revision+1,memory_generation=memory_generation+1 WHERE tenant_id=? AND channel_id=?", (tenant_id, channel_id))
            affected_channels = {channel_id, *(row["scope_channel_id"] for row in old)}
            for affected in affected_channels:
                connection.execute("DELETE FROM memory_summaries WHERE tenant_id=? AND scope_channel_id=?", (tenant_id, affected))
            row = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND subject_user_id=? AND topic_key=? AND status='active' ORDER BY updated_at DESC LIMIT 1", (tenant_id, channel_id, actor_id, topic_key)).fetchone()
            if row is None:
                raise MemoryError("memory_preference_not_saved")
            return self._item(connection, row)

    def _event(self, row: sqlite3.Row) -> dict[str, Any]:
        result = {"id": row["id"], "eventId": row["id"], "tenantId": row["tenant_id"], "scopeChannelId": row["scope_channel_id"], "memoryId": row["memory_id"], "sourceId": row["source_id"], "eventType": row["event_type"], "actorId": row["actor_id"], "requestId": row["request_id"], "deletionKind": row["deletion_kind"], "subjectId": row["subject_id"], "topicKeyHash": row["topic_key_hash"], "sourceMessageId": row["source_message_id"], "cutoffAt": row["cutoff_at"], "reallowRequestId": row["reallow_request_id"], "remoteSequence": row["remote_sequence"], "durabilityState": row["durability_state"], "createdAt": _iso(row["created_at"])}
        if row["deletion_kind"] in {"derived_item", "raw_state"}:
            # 기존 journal payload는 바꾸지 않고 파생물 purge의 요청 범위 hash만 새 이벤트에 보존한다.
            result["reasonCode"] = row["reason_code"]
            if row["event_type"] == "derived_purge_scope":
                result["expectedItemCount"] = row["from_version"]
        return result

    def deletion_events(self, after_sequence: int = 0) -> list[dict[str, Any]]:
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM memory_events WHERE deletion_kind IS NOT NULL AND (remote_sequence IS NULL OR remote_sequence>?) ORDER BY rowid", (after_sequence,)).fetchall()
            return [self._event(row) for row in rows]

    def mark_deletion_durable(self, event_id: str, remote_sequence: int) -> None:
        if remote_sequence < 1:
            raise MemoryError("memory_deletion_sequence_invalid")
        with self._connection(write=True) as connection:
            row = connection.execute("SELECT * FROM memory_events WHERE id=? AND deletion_kind IS NOT NULL", (event_id,)).fetchone()
            if row is None or (row["remote_sequence"] is not None and row["remote_sequence"] != remote_sequence):
                raise MemoryError("memory_deletion_sequence_conflict")
            connection.execute("UPDATE memory_events SET remote_sequence=?,durability_state='durable' WHERE id=?", (remote_sequence, event_id))

    def _apply_event(self, connection: sqlite3.Connection, event: Mapping[str, Any]) -> None:
        tenant, channel = str(event["tenantId"]), str(event["scopeChannelId"])
        identity = str(event.get("id") or event.get("eventId") or "")
        if not identity or event.get("deletionKind") not in {"item", "subject_topic", "source", "source_revision", "source_redaction", "derived_item", "raw_state"}:
            raise MemoryError("memory_deletion_event_invalid")
        existing = connection.execute("SELECT 1 FROM memory_events WHERE id=?", (identity,)).fetchone()
        if existing:
            return
        if event["deletionKind"] == "raw_state":
            self._apply_raw_state(connection, event)
        elif event["deletionKind"] == "derived_item":
            if not re.fullmatch(r"[a-f0-9]{64}", str(event.get("reasonCode") or "")):
                raise MemoryError("memory_derived_event_invalid")
            if event.get("eventType") == "derived_purge_scope" and (type(event.get("expectedItemCount")) is not int
                    or not 0 <= event["expectedItemCount"] < 2**63):
                raise MemoryError("memory_derived_event_invalid")
            memory_id = event.get("memoryId")
            if memory_id is not None:
                # 원문, source와 기존 명시 삭제 tombstone은 그대로 두고 파생된 값과 연결만 제거한다.
                row = connection.execute("SELECT id FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND id=?",
                                         (tenant, channel, memory_id)).fetchone()
                if row:
                    self._clear_derived_job_context(connection, tenant, memory_id)
                    connection.execute("DELETE FROM memory_subjects WHERE tenant_id=? AND memory_id=?", (tenant, memory_id))
                    connection.execute("DELETE FROM memory_item_sources WHERE tenant_id=? AND memory_id=?", (tenant, memory_id))
                    connection.execute("UPDATE memory_items SET text='',topic_key='',subject_user_id=NULL,evidence_type='',status='deleted',version=version+1,updated_at=? WHERE tenant_id=? AND id=?",
                                       (self._clock(), tenant, memory_id))
            connection.execute("DELETE FROM memory_summaries WHERE tenant_id=? AND scope_channel_id=?", (tenant, channel))
        elif event["deletionKind"] == "source_redaction":
            # 원본 재학습만 차단하며 같은 메시지에 근거한 다른 확정 사실은 보존한다.
            connection.execute("DELETE FROM memory_source_texts WHERE tenant_id=? AND source_id IN (SELECT id FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=?)", (tenant, tenant, channel, event.get("sourceMessageId")))
            connection.execute("UPDATE memory_sources SET excerpt='' WHERE tenant_id=? AND channel_id=? AND message_ts=?", (tenant, channel, event.get("sourceMessageId")))
            connection.execute("UPDATE memory_item_sources SET evidence_span='' WHERE tenant_id=? AND source_id IN (SELECT id FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=?)", (tenant, tenant, channel, event.get("sourceMessageId")))
        elif event["deletionKind"] in {"source", "source_revision"}:
            if event["deletionKind"] == "source":
                sources = connection.execute("SELECT id FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=?", (tenant, channel, event.get("sourceMessageId"))).fetchall()
            else:
                sources = connection.execute("SELECT id FROM memory_sources WHERE tenant_id=? AND channel_id=? AND id=?", (tenant, channel, event.get("sourceId"))).fetchall()
            for source in sources:
                self._invalidate_source_items(connection, tenant, source["id"], deleted=True)
                connection.execute("UPDATE memory_sources SET state='deleted',excerpt='' WHERE tenant_id=? AND id=?", (tenant, source["id"]))
        else:
            rows = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND COALESCE(subject_user_id,'')=COALESCE(?,'')", (tenant, channel, event.get("subjectId"))).fetchall()
            for row in rows:
                matches = row["id"] == event.get("memoryId") if event["deletionKind"] == "item" else event.get("topicKeyHash") in {canonical_digest(row["topic_key"].strip().casefold()), canonical_digest(normalize_topic_key(row["topic_key"]))}
                if not matches:
                    continue
                for source in connection.execute("SELECT s.message_ts FROM memory_sources s JOIN memory_item_sources r ON r.tenant_id=s.tenant_id AND r.source_id=s.id WHERE r.tenant_id=? AND r.memory_id=?", (tenant, row["id"])).fetchall():
                    child = {"id": "evt_" + canonical_digest([identity, source["message_ts"]]), "tenantId": tenant,
                             "scopeChannelId": channel, "eventType": "source_redaction", "requestId": str(event.get("requestId") or identity),
                             "actorId": event.get("actorId"), "deletionKind": "source_redaction", "sourceMessageId": source["message_ts"],
                             "cutoffAt": event.get("cutoffAt", self._clock()), "durabilityState": event.get("durabilityState", "pending")}
                    self._apply_event(connection, child)
                # 공유 source 전체를 지우지 않되, 잊은 문장이 포함될 수 있는 원문 snippet은 제거한다.
                connection.execute("DELETE FROM memory_source_texts WHERE tenant_id=? AND source_id IN (SELECT source_id FROM memory_item_sources WHERE tenant_id=? AND memory_id=?)", (tenant, tenant, row["id"]))
                connection.execute("UPDATE memory_sources SET excerpt='' WHERE tenant_id=? AND id IN (SELECT source_id FROM memory_item_sources WHERE tenant_id=? AND memory_id=?)", (tenant, tenant, row["id"]))
                connection.execute("UPDATE memory_item_sources SET evidence_span='' WHERE tenant_id=? AND source_id IN (SELECT source_id FROM memory_item_sources WHERE tenant_id=? AND memory_id=?)", (tenant, tenant, row["id"]))
                connection.execute("DELETE FROM memory_item_sources WHERE tenant_id=? AND memory_id=?", (tenant, row["id"]))
                connection.execute("UPDATE memory_items SET text='',status='deleted',version=version+1,updated_at=? WHERE tenant_id=? AND id=?", (self._clock(), tenant, row["id"]))
        # scope marker를 먼저 재적용한 purge의 개별 item마다 전체 channel job을 반복 정리하지 않는다.
        marker = event["deletionKind"] == "derived_item" and event.get("memoryId") is not None and connection.execute(
            "SELECT 1 FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND request_id=? AND event_type='derived_purge_scope'",
            (tenant, channel, event.get("requestId"))).fetchone()
        if not marker:
            self._cancel_scope_jobs(connection, tenant, channel)
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,memory_id,source_id,event_type,actor_id,request_id,deletion_kind,subject_id,topic_key_hash,source_message_id,cutoff_at,reallow_request_id,remote_sequence,durability_state,created_at,reason_code,from_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (identity, tenant, channel, event.get("memoryId"), event.get("sourceId"), str(event.get("eventType") or "restored_deletion"), event.get("actorId"), str(event.get("requestId") or identity), event["deletionKind"], event.get("subjectId"), event.get("topicKeyHash"), event.get("sourceMessageId"), event.get("cutoffAt", self._clock()), event.get("reallowRequestId"), event.get("remoteSequence"), str(event.get("durabilityState") or "durable"), self._clock(), str(event.get("reasonCode") or ""), event.get("expectedItemCount")))

    def _apply_raw_state(self, connection: sqlite3.Connection, event: Mapping[str, Any]) -> None:
        from .raw_state import RAW_STATE_PRIORITY, parse_raw_state_event, raw_state_stamp

        payload = parse_raw_state_event(event)
        tenant, channel, message = event["tenantId"], event["scopeChannelId"], event["sourceMessageId"]
        previous = connection.execute("SELECT * FROM memory_raw_event_states WHERE tenant_id=? AND channel_id=? AND message_ts=?",
                                      (tenant, channel, message)).fetchone()
        if previous:
            prior_stamp = raw_state_stamp(previous["event_ts"])
            incoming_stamp = raw_state_stamp(payload["eventTs"])
            if (previous["state"] == "deleted" or incoming_stamp < prior_stamp
                    or incoming_stamp == prior_stamp
                    and (RAW_STATE_PRIORITY[payload["state"]] < RAW_STATE_PRIORITY[previous["state"]]
                         or RAW_STATE_PRIORITY[payload["state"]] == RAW_STATE_PRIORITY[previous["state"]]
                         and previous["revision_hash"] != payload["revisionHash"])):
                return
        sources = connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=? AND state='current'",
                                     (tenant, channel, message)).fetchall()
        if any(raw_state_stamp(source["edited_at"] or source["message_ts"]) > raw_state_stamp(payload["eventTs"])
               for source in sources):
            # 더 최신 snapshot의 현재 본문과 watermark를 과거 journal 순서로 되돌리지 않는다.
            return
        for source in sources:
            if payload["state"] == "current" and source["revision_hash"] == payload["revisionHash"]:
                continue
            # 새 본문은 journal에 저장하지 않는다. 일치하지 않는 이전 본문만 복원에서 제거한다.
            deleted = payload["state"] == "deleted"
            self._invalidate_source_items(connection, tenant, source["id"], deleted=deleted)
            connection.execute("UPDATE memory_sources SET state=?,excerpt='' WHERE tenant_id=? AND id=?",
                               ("deleted" if deleted else "unavailable", tenant, source["id"]))
        connection.execute("""INSERT INTO memory_raw_event_states(tenant_id,channel_id,message_ts,event_ts,revision_hash,state)
            VALUES(?,?,?,?,?,?) ON CONFLICT(tenant_id,channel_id,message_ts) DO UPDATE SET
            event_ts=excluded.event_ts,revision_hash=excluded.revision_hash,state=excluded.state""",
            (tenant, channel, message, payload["eventTs"], payload["revisionHash"], payload["state"]))

    def _clear_derived_job_context(self, connection: sqlite3.Connection, tenant: str, memory_id: str) -> None:
        # 본인 선호가 다른 channel job의 이전 문맥에도 공유될 수 있어 그 사본까지 취소한다.
        jobs = connection.execute("""SELECT DISTINCT j.* FROM memory_jobs j,json_each(j.metadata,'$.previousItems') p
            WHERE j.tenant_id=? AND json_extract(p.value,'$.memoryId')=?""", (tenant, memory_id)).fetchall()
        for job in jobs:
            self._cancel_one_job(connection, job)

    def apply_deletion_event(self, event: Mapping[str, Any]) -> None:
        with self._connection(write=True) as connection:
            self._apply_event(connection, event)

    def cleanup(self, *, restored: bool = False) -> dict[str, int]:
        now = self._clock()
        with self._connection(write=True) as connection:
            if restored:
                payloads = connection.execute("DELETE FROM memory_job_inputs").rowcount
                connection.execute("UPDATE memory_jobs SET state='canceled',claim_token='',lease_until=0 WHERE state IN ('collecting','ready','running','retry')")
            else:
                payloads = connection.execute("DELETE FROM memory_job_inputs WHERE expires_at<=?", (now,)).rowcount
                connection.execute("UPDATE memory_jobs SET state='canceled',claim_token='',lease_until=0 WHERE expires_at<=? AND state IN ('collecting','ready','running','retry')", (now,))
            snippets = connection.execute("UPDATE memory_sources SET excerpt='' WHERE last_verified_at<? AND excerpt!=''", (now - 90 * 86400,)).rowcount
            connection.execute("UPDATE memory_item_sources SET evidence_span='' WHERE source_id IN (SELECT id FROM memory_sources WHERE excerpt='')")
            # 명시 보존 원문에는 발췌의 90일 만료를 적용하지 않는다. 철회된 출처만 정리한다.
            source_texts = connection.execute(
                "DELETE FROM memory_source_texts WHERE NOT EXISTS (SELECT 1 FROM memory_sources s "
                "WHERE s.tenant_id=memory_source_texts.tenant_id AND s.id=memory_source_texts.source_id AND s.state='current') "
                "OR EXISTS (SELECT 1 FROM memory_sources s JOIN memory_events e "
                "ON e.tenant_id=s.tenant_id AND e.scope_channel_id=s.channel_id "
                "WHERE s.tenant_id=memory_source_texts.tenant_id AND s.id=memory_source_texts.source_id "
                "AND ((e.deletion_kind IN ('source','source_redaction') AND e.source_message_id=s.message_ts) "
                "OR (e.deletion_kind='source_revision' AND e.source_id=s.id)))"
            ).rowcount
            expired = connection.execute("UPDATE memory_items SET status='expired',version=version+1 WHERE status='active' AND valid_until IS NOT NULL AND valid_until<=?", (now,)).rowcount
            connection.execute("DELETE FROM memory_summaries WHERE updated_at<?", (now - 365 * 86400,))
            if expired or restored:
                connection.execute("DELETE FROM memory_summaries")
            for row in connection.execute("SELECT id FROM memory_jobs WHERE state IN ('canceled','failed','superseded','succeeded')").fetchall():
                self._clear_job_context(connection, row["id"])
        # checkpoint 이후 남는 page까지 즉시 완전 삭제됐다고 약속하지 않는다.
        with self._connection() as connection:
            connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
        return {"payloadsRemoved": payloads, "snippetsRemoved": snippets, "itemsExpired": expired,
                "sourceTextsRemoved": source_texts}

    def snapshot(self, destination: str | Path) -> Path:
        target = Path(destination)
        if not target.is_absolute() or target.resolve(strict=False) != target:
            raise MemoryError("memory_snapshot_path_invalid")
        descriptor = os.open(target, os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.close(descriptor)
        try:
            with self._connection() as source:
                destination_connection = sqlite3.connect(target)
                try:
                    source.backup(destination_connection)
                finally:
                    destination_connection.close()
        except BaseException:
            target.unlink(missing_ok=True)
            raise
        return target

    def _items_digest(self, connection: sqlite3.Connection, tenant: str, channel: str, *, actor: str | None = None) -> str:
        rows = connection.execute("SELECT i.id,i.version,i.status,s.id,s.revision_hash,s.state FROM memory_items i LEFT JOIN memory_item_sources r ON r.tenant_id=i.tenant_id AND r.memory_id=i.id LEFT JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id WHERE i.tenant_id=? AND (i.scope_channel_id=? OR (i.kind='person' AND i.category='preference' AND i.subject_user_id=?)) ORDER BY i.id,s.id", (tenant, channel, actor)).fetchall()
        return canonical_digest([list(row) for row in rows])

    def _clear_job_context(self, connection: sqlite3.Connection, job_id: str) -> None:
        row = connection.execute("SELECT metadata FROM memory_jobs WHERE id=?", (job_id,)).fetchone()
        metadata = json.loads(row["metadata"])
        metadata.pop("previousItems", None)
        connection.execute("UPDATE memory_jobs SET metadata=? WHERE id=?", (_json(metadata), job_id))

    def _refresh_summaries(self, connection: sqlite3.Connection, tenant: str, channel: str) -> None:
        # 요약 cache는 검증된 item의 짧은 모음일 뿐 새로운 사실을 생성하지 않는다.
        rows = connection.execute("SELECT * FROM memory_items WHERE tenant_id=? AND scope_channel_id=? AND status IN ('active','resolved') ORDER BY updated_at DESC", (tenant, channel)).fetchall()
        groups: dict[tuple[str, str], list[sqlite3.Row]] = {}
        for row in rows:
            target = row["subject_user_id"] if row["kind"] == "person" else row["origin_thread_id"] if row["kind"] == "thread" else channel
            groups.setdefault((row["kind"], target), []).append(row)
        digest = self._items_digest(connection, tenant, channel)
        for (kind, target), items in groups.items():
            source_ids = sorted({source[0] for item in items for source in connection.execute("SELECT source_id FROM memory_item_sources WHERE tenant_id=? AND memory_id=?", (tenant, item["id"]))})
            text = "\n".join(item["text"] for item in items[:8])[:1200]
            connection.execute("INSERT OR REPLACE INTO memory_summaries(tenant_id,scope_channel_id,target_kind,target_id,summary,source_ids,base_items_digest,coverage,updated_at) VALUES(?,?,?,?,?,?,?,?,?)", (tenant, channel, kind, target, text, _json(source_ids), digest, "partial", self._clock()))
