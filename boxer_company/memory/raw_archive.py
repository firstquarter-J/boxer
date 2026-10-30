"""승인된 유한 Slack 원문 snapshot을 요약과 분리해 저장하는 운영자 경계."""

from __future__ import annotations

import re
import sqlite3
from decimal import Decimal
from typing import Any

from .contracts import MemoryError, canonical_digest
from .raw_source_policy import normalize_raw_source
from .store import MemoryStore

_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_TIMESTAMP = re.compile(r"^[0-9]{1,20}\.[0-9]{1,9}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_KEYS = frozenset({
    "sourceId", "messageTs", "revisionHash", "authorId", "authorKind", "text",
    "permalink", "createdAt", "editedAt", "route", "deliveryVerified", "threadTs",
})
_MAX_SOURCES = 10_000
_MAX_TEXT_CHARS = 40_000
_MAX_TOTAL_CHARS = 16 * 1024 * 1024


def _identifier(value: Any) -> bool:
    return isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None


def _timestamp(value: Any) -> bool:
    return bool(isinstance(value, str) and _TIMESTAMP.fullmatch(value)
                and 0 <= Decimal(value) < 2**63)


def _metadata(row: sqlite3.Row) -> dict[str, Any]:
    # 조회와 모델 입력에 원문이 섞이지 않도록 상태 응답은 식별자와 고정 범위만 담는다.
    return {"importId": row["id"], "state": row["state"], "startTs": row["start_ts"],
            "cutoffTs": row["cutoff_ts"], "manifestDigest": row["manifest_digest"]}


def begin_raw_import(store: MemoryStore, *, import_id: str, tenant_id: str,
                     channel_id: str, author_id: str, operator_id: str,
                     start_ts: str, cutoff_ts: str, manifest_digest: str) -> dict[str, Any]:
    """운영자가 승인한 작성자, 비공개 채널, 기간과 불변 manifest를 먼저 고정한다."""

    if (not all(_identifier(value) for value in (import_id, tenant_id, channel_id, author_id, operator_id))
            or not channel_id.startswith(("C", "G")) or not author_id.startswith(("U", "W"))
            or not operator_id.startswith(("U", "W"))
            or not _timestamp(start_ts) or not _timestamp(cutoff_ts)
            or not isinstance(manifest_digest, str) or not _DIGEST.fullmatch(manifest_digest)
            or not Decimal(0) < Decimal(cutoff_ts) - Decimal(start_ts) <= 31 * 86400):
        raise MemoryError("raw_import_scope_invalid")
    with store._connection(write=True) as connection:
        previous = connection.execute("SELECT * FROM memory_raw_imports WHERE id=?", (import_id,)).fetchone()
        if previous:
            expected = (tenant_id, channel_id, author_id, operator_id, start_ts, cutoff_ts, manifest_digest)
            actual = tuple(previous[key] for key in (
                "tenant_id", "channel_id", "author_id", "operator_id", "start_ts", "cutoff_ts", "manifest_digest"))
            if actual != expected:
                raise MemoryError("raw_import_identity_conflict")
            return _metadata(previous)
        # DM과 공개 channel로 이미 알려진 범위는 운영자 입력으로 비공개로 바꾸지 않는다.
        conflict = connection.execute(
            "SELECT 1 FROM memory_threads WHERE tenant_id=? AND channel_id=? AND channel_kind!='private_channel' LIMIT 1",
            (tenant_id, channel_id),
        ).fetchone()
        if conflict:
            raise MemoryError("raw_import_private_channel_required")
        connection.execute("""INSERT INTO memory_raw_imports
            (id,tenant_id,channel_id,author_id,operator_id,start_ts,cutoff_ts,manifest_digest,state,created_at)
            VALUES(?,?,?,?,?,?,?,?,'collecting',?)""",
            (import_id, tenant_id, channel_id, author_id, operator_id, start_ts, cutoff_ts,
             manifest_digest, store._clock()))
        return _metadata(connection.execute("SELECT * FROM memory_raw_imports WHERE id=?", (import_id,)).fetchone())


def _validate_sources(row: sqlite3.Row, sources: list[dict[str, Any]]) -> list[tuple[str, dict[str, Any], str]]:
    """입력 전체를 쓰기 전에 확인해 잘못된 한 건도 부분 저장으로 숨기지 않는다."""

    if not isinstance(sources, list) or len(sources) > _MAX_SOURCES:
        raise MemoryError("raw_import_sources_invalid")
    if sum(len(source.get("text", "")) for source in sources
           if isinstance(source, dict) and isinstance(source.get("text"), str)) > _MAX_TOTAL_CHARS:
        raise MemoryError("raw_import_sources_invalid")
    try:
        actual_digest = canonical_digest({
            "tenantId": row["tenant_id"], "channelId": row["channel_id"],
            "authorId": row["author_id"], "startTs": row["start_ts"],
            "cutoffTs": row["cutoff_ts"], "sources": sources,
        })
    except (TypeError, ValueError, OverflowError) as exc:
        raise MemoryError("raw_import_manifest_invalid") from exc
    if actual_digest != row["manifest_digest"]:
        raise MemoryError("raw_import_manifest_conflict")
    normalized_sources = []
    seen_messages: set[Decimal] = set()
    for source in sources:
        if not isinstance(source, dict) or set(source) - _SOURCE_KEYS:
            raise MemoryError("raw_import_source_invalid")
        thread_ts, message_ts = source.get("threadTs"), source.get("messageTs")
        edited_at = source.get("editedAt") or ""
        text = source.get("text")
        if (not _timestamp(thread_ts) or not _timestamp(message_ts)
                or source.get("authorId") != row["author_id"] or source.get("authorKind") != "human"
                or not isinstance(text, str) or len(text) > _MAX_TEXT_CHARS
                or source.get("createdAt") != message_ts
                or (source.get("editedAt") is not None and not isinstance(source.get("editedAt"), str))
                or (source.get("route") is not None
                    and (not isinstance(source.get("route"), str) or len(source["route"]) > 64))
                or not isinstance(source.get("permalink"), str) or len(source["permalink"]) > 2048
                or source.get("deliveryVerified", False) is not False
                or (edited_at and (not _timestamp(edited_at)
                                   or not Decimal(message_ts) <= Decimal(edited_at) <= Decimal(row["cutoff_ts"])))
                or not Decimal(row["start_ts"]) <= Decimal(message_ts) <= Decimal(row["cutoff_ts"])
                or Decimal(message_ts) in seen_messages):
            raise MemoryError("raw_import_source_invalid")
        # 승인된 고정 범위만 원문 내용 필터를 분리하고 출처 검증과 기존 오류 계약은 유지한다.
        try:
            normalized = normalize_raw_source(source, tenant_id=row["tenant_id"], channel_id=row["channel_id"],
                                              thread_ts=thread_ts, current_message_ts=row["cutoff_ts"])
        except MemoryError as error:
            if error.code == "source_identity_mismatch":
                raise MemoryError("raw_import_source_identity_invalid") from error
            raise
        if normalized is None:
            raise MemoryError("raw_import_source_not_allowed")
        if source.get("sourceId", normalized["sourceId"]) != normalized["sourceId"]:
            raise MemoryError("raw_import_source_identity_invalid")
        seen_messages.add(Decimal(message_ts))
        normalized_sources.append((thread_ts, normalized, text))
    return normalized_sources


def _check_current_source(store: MemoryStore, connection: sqlite3.Connection, row: sqlite3.Row,
                          thread_ts: str, source: dict[str, Any]) -> sqlite3.Row | None:
    tenant, channel = row["tenant_id"], row["channel_id"]
    watermark = connection.execute(
        "SELECT state,event_ts,revision_hash FROM memory_raw_event_states WHERE tenant_id=? AND channel_id=? AND message_ts=?",
        (tenant, channel, source["messageTs"]),
    ).fetchone()
    # 수집 중 무효화된 옛 export가 실시간 수정이나 삭제보다 뒤늦게 본문을 되살리지 못한다.
    if watermark is not None and (watermark["state"] in {"deleted", "unavailable"}
            or watermark["revision_hash"] != source["revisionHash"]
            and Decimal(source["editedAt"] or source["messageTs"]) <= Decimal(watermark["event_ts"])):
        raise MemoryError("raw_import_source_stale")
    if store._source_deleted(connection, tenant, channel, source["messageTs"]):
        raise MemoryError("raw_import_source_stale")
    invalidated = connection.execute("""SELECT 1 FROM memory_events WHERE tenant_id=?
        AND scope_channel_id=? AND deletion_kind='source_revision' AND source_id=? LIMIT 1""",
        (tenant, channel, source["sourceId"])).fetchone()
    known = connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND id=?",
                               (tenant, source["sourceId"])).fetchone()
    if invalidated or (known and known["state"] != "current"):
        raise MemoryError("raw_import_source_stale")
    revisions = connection.execute("""SELECT * FROM memory_sources WHERE tenant_id=?
        AND channel_id=? AND message_ts=? AND state='current'""",
        (tenant, channel, source["messageTs"])).fetchall()
    for previous in revisions:
        previous_thread = connection.execute("SELECT thread_ts,channel_kind FROM memory_threads WHERE tenant_id=? AND id=?",
                                             (tenant, previous["thread_id"])).fetchone()
        if (previous["author_id"] != source["authorId"] or previous["author_kind"] != "human"
                or not previous_thread or previous_thread["thread_ts"] != thread_ts
                or previous_thread["channel_kind"] != "private_channel"
                or not _timestamp(previous["message_ts"])
                or previous["edited_at"] and not _timestamp(previous["edited_at"])):
            # Slack 편집은 작성자와 소속 스레드를 바꾸지 않으므로 revision 차이로 우회할 수 없다.
            raise MemoryError("raw_import_source_identity_conflict")
    if any(item["revision_hash"] != source["revisionHash"]
           and Decimal(item["edited_at"] or item["message_ts"]) >= Decimal(source["editedAt"] or source["messageTs"])
           for item in revisions):
        raise MemoryError("raw_import_source_stale")
    if known:
        thread = connection.execute("SELECT * FROM memory_threads WHERE tenant_id=? AND id=?",
                                    (tenant, known["thread_id"])).fetchone()
        expected = (channel, source["messageTs"], source["revisionHash"], source["authorId"],
                    "human", source["createdAt"], source["editedAt"], source["permalink"])
        actual = tuple(known[key] for key in (
            "channel_id", "message_ts", "revision_hash", "author_id", "author_kind", "created_at", "edited_at", "permalink"))
        if (actual != expected or not thread or thread["thread_ts"] != thread_ts
                or thread["channel_kind"] != "private_channel"):
            raise MemoryError("raw_import_source_identity_conflict")
    return known


def import_raw_messages(store: MemoryStore, *, import_id: str, manifest_digest: str,
                        sources: list[dict[str, Any]]) -> dict[str, Any]:
    """검증한 snapshot의 원문만 저장하며 추출 job, 요약과 모델 입력을 만들지 않는다."""

    if not _identifier(import_id) or not isinstance(manifest_digest, str) or not _DIGEST.fullmatch(manifest_digest):
        raise MemoryError("raw_import_identity_invalid")
    with store._connection(write=True) as connection:
        row = connection.execute("SELECT * FROM memory_raw_imports WHERE id=?", (import_id,)).fetchone()
        if not row:
            raise MemoryError("raw_import_missing")
        if row["manifest_digest"] != manifest_digest:
            raise MemoryError("raw_import_manifest_conflict")
        if row["state"] not in {"collecting", "completed"}:
            raise MemoryError("raw_import_terminal")
        normalized = _validate_sources(row, sources)
        # 저장 중 들어오는 수정/삭제와 같은 SQLite 쓰기 lock에서 revision을 최종 대조한다.
        existing_count = 0
        for thread_ts, source, text in normalized:
            _check_current_source(store, connection, row, thread_ts, source)
            archived = connection.execute("SELECT text FROM memory_source_texts WHERE tenant_id=? AND source_id=?",
                                          (row["tenant_id"], source["sourceId"])).fetchone()
            if archived:
                if archived["text"] != text:
                    raise MemoryError("raw_import_text_conflict")
                existing_count += 1
            elif row["state"] == "completed":
                # 완료 manifest를 재실행해 지워진 원문을 새로 생성하지 않는다.
                raise MemoryError("raw_import_source_stale")
        for thread_ts, source, text in normalized:
            thread = store._thread(connection, row["tenant_id"], row["channel_id"], thread_ts, "private_channel")
            if thread["channel_kind"] != "private_channel":
                raise MemoryError("raw_import_private_channel_required")
            store._register_source(connection, row["tenant_id"], thread, source)
            current = connection.execute("SELECT state FROM memory_sources WHERE tenant_id=? AND id=?",
                                         (row["tenant_id"], source["sourceId"])).fetchone()
            if not current or current["state"] != "current":
                raise MemoryError("raw_import_source_stale")
            # source excerpt를 원문으로 대체하지 않아 자동 기억 추출의 보존 규칙은 그대로 둔다.
            connection.execute("""INSERT OR IGNORE INTO memory_source_texts
                (tenant_id,source_id,import_id,text,stored_at) VALUES(?,?,?,?,?)""",
                (row["tenant_id"], source["sourceId"], import_id, text, store._clock()))
        connection.execute("UPDATE memory_raw_imports SET state='completed' WHERE id=?", (import_id,))
        return {"state": "completed", "sourceCount": len(normalized),
                "storedCount": len(normalized) - existing_count, "existingCount": existing_count}
