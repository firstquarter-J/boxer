"""Slack의 승인된 지속 이벤트를 추출 job 없이 조회용 원문으로 저장한다."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES

from .contracts import MemoryError, canonical_digest, source_identity, source_revision
from .policy import _valid_permalink, normalize_source
from .raw_policy import RAW_ONLY_CUTOFF_TS, RAW_ONLY_START_TS, is_raw_only_scope
from .raw_state import RAW_STATE_PRIORITY

if TYPE_CHECKING:
    import sqlite3

    from .store import MemoryStore

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}")
_TIMESTAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_SOURCE_KEYS = frozenset({"sourceId", "messageTs", "threadTs", "revisionHash", "authorId", "authorKind", "text",
                          "permalink", "createdAt", "editedAt", "route", "deliveryVerified"})
_MAX_TEXT_CHARS = 40_000


def _stamp(value: Any) -> Decimal:
    if not isinstance(value, str) or not _TIMESTAMP.fullmatch(value) or not 0 <= Decimal(value) < 2**63:
        raise MemoryError("raw_event_timestamp_invalid")
    return Decimal(value)


def _source(source: Any, *, tenant: str, channel: str, actor: str | None, message: str,
            event_ts: str) -> tuple[dict[str, Any] | None, str, str]:
    profiles = {str(profile.get("user_id") or "") for profile in TEAM_MEMBER_PROFILES}
    if (not isinstance(source, Mapping) or set(source) - _SOURCE_KEYS or not actor or actor not in profiles
            or source.get("authorId") != actor or source.get("authorKind") != "human"
            or source.get("messageTs") != message or source.get("createdAt", message) != message
            or source.get("deliveryVerified", False) is not False):
        raise MemoryError("raw_event_source_invalid")
    text, thread = source.get("text"), source.get("threadTs", message)
    edited, revision = source.get("editedAt") or "", source.get("revisionHash")
    if (not isinstance(text, str) or len(text) > _MAX_TEXT_CHARS
            or _stamp(thread) > _stamp(message)
            or (source.get("editedAt") is not None and not isinstance(source.get("editedAt"), str))
            or edited and not _stamp(message) <= _stamp(edited) <= _stamp(event_ts)
            or revision != source_revision(text, edited)
            or not isinstance(source.get("permalink"), str)
            or len(source["permalink"]) > 2048
            or not _valid_permalink(source["permalink"], channel, message, thread)
            or source.get("route") is not None and (not isinstance(source["route"], str) or len(source["route"]) > 64)):
        raise MemoryError("raw_event_source_invalid")
    identity = source_identity(tenant, channel, message, revision)
    if source.get("sourceId") is not None and source["sourceId"] != identity:
        raise MemoryError("raw_event_source_invalid")
    # provenance의 범위는 발언 생성 월이다. normalize cutoff는 eventTs라 11월 이후 편집도 대조한다.
    normalized = normalize_source(source, tenant_id=tenant, channel_id=channel,
                                  thread_ts=thread, current_message_ts=event_ts)
    return normalized, text, thread


def _invalidate(store: MemoryStore, connection: sqlite3.Connection, *, tenant: str, channel: str,
                message: str, event_id: str) -> None:
    for old in connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=? AND state='current'",
                                  (tenant, channel, message)).fetchall():
        store._invalidate_source_items(connection, tenant, old["id"], deleted=False)
        connection.execute("UPDATE memory_sources SET state='unavailable',excerpt='' WHERE tenant_id=? AND id=?", (tenant, old["id"]))
        store._record_revision_invalidation(connection, old, "raw:" + event_id)
    store._cancel_scope_jobs(connection, tenant, channel)


def ingest_raw_event(store: MemoryStore, *, tenant_id: str, channel_id: str, event_id: str, event_ts: str,
                     event_type: str, source: dict[str, Any] | None, message_ts: str,
                     actor_id: str | None = None) -> dict[str, Any]:
    """영수증, revision, 원문과 watermark를 한 transaction에 묶고 본문은 반환하지 않는다."""
    if (not is_raw_only_scope(tenant_id, channel_id, message_ts)
            or not isinstance(event_id, str) or not _IDENTIFIER.fullmatch(event_id)
            or not isinstance(event_type, str) or event_type not in {"created", "changed", "deleted", "unavailable"}
            or actor_id is not None and (not isinstance(actor_id, str) or not _IDENTIFIER.fullmatch(actor_id))
            or source is not None and not isinstance(source, dict)):
        raise MemoryError("raw_event_scope_invalid")
    if _stamp(event_ts) < _stamp(message_ts):
        raise MemoryError("raw_event_timestamp_invalid")
    normalized, text, thread = None, "", ""
    if event_type in {"created", "changed"}:
        normalized, text, thread = _source(source, tenant=tenant_id, channel=channel_id, actor=actor_id,
                                           message=message_ts, event_ts=event_ts)
    elif source is not None:
        raise MemoryError("raw_event_source_invalid")
    payload = {"tenantId": tenant_id, "channelId": channel_id, "eventId": event_id, "eventTs": event_ts,
               "eventType": event_type, "source": source, "messageTs": message_ts, "actorId": actor_id}
    try:
        digest = canonical_digest(payload)
    except (TypeError, ValueError, OverflowError) as exc:
        raise MemoryError("raw_event_source_invalid") from exc
    receipt_id = "evt_raw_" + canonical_digest([tenant_id, event_id])
    with store._connection(write=True) as connection:
        receipt = connection.execute("SELECT reason_code FROM memory_events WHERE id=?", (receipt_id,)).fetchone()
        if receipt:
            if receipt["reason_code"] != digest:
                raise MemoryError("raw_event_identity_conflict")
            return {"status": "duplicate", "eventId": event_id, "messageTs": message_ts, "stored": False}
        if event_type in {"created", "changed"}:
            # 안전정책에서 제외된 편집도 기존 발언의 작성자나 스레드를 바꿀 권한은 없다.
            identities = connection.execute("SELECT s.author_id,s.author_kind,s.state,s.message_ts,s.edited_at,t.thread_ts,t.channel_kind FROM memory_sources s LEFT JOIN memory_threads t ON t.tenant_id=s.tenant_id AND t.id=s.thread_id AND t.channel_id=s.channel_id WHERE s.tenant_id=? AND s.channel_id=? AND s.message_ts=?",
                                            (tenant_id, channel_id, message_ts)).fetchall()
            if any(old["author_id"] != actor_id or old["author_kind"] != "human" or old["thread_ts"] != thread
                   or old["channel_kind"] != "private_channel" for old in identities):
                raise MemoryError("raw_event_source_identity_conflict")

        def acknowledge(status: str) -> dict[str, Any]:
            connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,event_type,actor_id,request_id,reason_code,source_message_id,durability_state,created_at) VALUES(?,?,?,'raw_ingest',?,?,?,?,'not_required',?)",
                               (receipt_id, tenant_id, channel_id, actor_id, event_id, digest, message_ts, store._clock()))
            return {"status": status, "eventId": event_id, "messageTs": message_ts, "stored": status == "stored"}

        previous = connection.execute("SELECT * FROM memory_raw_event_states WHERE tenant_id=? AND channel_id=? AND message_ts=?",
                                      (tenant_id, channel_id, message_ts)).fetchone()
        incoming_state = ("deleted" if event_type == "deleted" else "unavailable"
                          if event_type == "unavailable" or normalized is None else "current")
        # 응답 유실 뒤 권한이 철회된 receipt도 같은 시각의 unavailable로 기존 본문을 제거해야 한다.
        # 같은 시각은 deleted > unavailable > current만 허용하고 삭제는 다음 시각에도 영구 우선한다.
        if previous and (previous["state"] == "deleted" or _stamp(event_ts) < _stamp(previous["event_ts"])
                         or _stamp(event_ts) == _stamp(previous["event_ts"])
                         and RAW_STATE_PRIORITY[incoming_state] <= RAW_STATE_PRIORITY[previous["state"]]):
            return acknowledge("stale")
        if event_type in {"created", "changed"} and any(
                old["state"] == "current" and _stamp(old["edited_at"] or old["message_ts"]) > _stamp(source.get("editedAt") or message_ts)
                for old in identities):
            # backfill에는 live watermark가 없을 수 있다. 늦게 온 위험 편집도 더 최신 snapshot을 지우지 않는다.
            return acknowledge("stale")
        revision = source["revisionHash"] if source is not None else previous["revision_hash"] if previous else ""
        if event_type == "deleted":
            store._apply_event(connection, {"id": "evt_raw_delete_" + canonical_digest([tenant_id, event_id]),
                "tenantId": tenant_id, "scopeChannelId": channel_id, "requestId": event_id,
                "eventType": "raw_source_deleted", "deletionKind": "source", "sourceMessageId": message_ts,
                "cutoffAt": store._clock(), "durabilityState": "pending"})
            state, status = "deleted", "deleted"
        elif event_type == "unavailable" or normalized is None:
            _invalidate(store, connection, tenant=tenant_id, channel=channel_id, message=message_ts, event_id=event_id)
            if source is not None:
                store._record_revision_invalidation(connection, {"id": source_identity(tenant_id, channel_id, message_ts, revision),
                    "tenant_id": tenant_id, "channel_id": channel_id, "message_ts": message_ts}, "raw:" + event_id)
            state, status = "unavailable", "unavailable"
        else:
            if store._source_deleted(connection, tenant_id, channel_id, message_ts):
                return acknowledge("stale")
            revoked = connection.execute("SELECT 1 FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND deletion_kind='source_revision' AND source_id=? LIMIT 1",
                                         (tenant_id, channel_id, normalized["sourceId"])).fetchone()
            known = connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND id=?", (tenant_id, normalized["sourceId"])).fetchone()
            current = connection.execute("SELECT * FROM memory_sources WHERE tenant_id=? AND channel_id=? AND message_ts=? AND state='current'",
                                         (tenant_id, channel_id, message_ts)).fetchall()
            if revoked or known and known["state"] != "current" or any(
                    _stamp(old["edited_at"] or old["message_ts"]) > _stamp(normalized["editedAt"] or message_ts) for old in current):
                return acknowledge("stale")
            old_text = connection.execute("SELECT text FROM memory_source_texts WHERE tenant_id=? AND source_id=?",
                                          (tenant_id, normalized["sourceId"])).fetchone()
            if old_text and old_text["text"] != text:
                raise MemoryError("raw_event_text_conflict")
            # 이벤트별 불변 provenance를 만들어 완료된 snapshot에 새 본문을 덧붙이지 않는다.
            import_id = "live_" + canonical_digest([tenant_id, event_id])
            connection.execute("INSERT INTO memory_raw_imports(id,tenant_id,channel_id,author_id,operator_id,start_ts,cutoff_ts,manifest_digest,state,created_at) VALUES(?,?,?,?,?,?,?,?,'completed',?)",
                               (import_id, tenant_id, channel_id, actor_id, actor_id, RAW_ONLY_START_TS,
                                RAW_ONLY_CUTOFF_TS, digest, store._clock()))
            metadata = store._thread(connection, tenant_id, channel_id, thread, "private_channel")
            if metadata["channel_kind"] != "private_channel":
                raise MemoryError("raw_event_private_channel_required")
            store._register_source(connection, tenant_id, metadata, normalized)
            stored = connection.execute("SELECT state FROM memory_sources WHERE tenant_id=? AND id=?",
                                        (tenant_id, normalized["sourceId"])).fetchone()
            if not stored or stored["state"] != "current":
                raise MemoryError("raw_event_source_stale")
            connection.execute("INSERT OR IGNORE INTO memory_source_texts(tenant_id,source_id,import_id,text,stored_at) VALUES(?,?,?,?,?)",
                               (tenant_id, normalized["sourceId"], import_id, text, store._clock()))
            state, status = "current", "stored"
        connection.execute("INSERT INTO memory_raw_event_states(tenant_id,channel_id,message_ts,event_ts,revision_hash,state) VALUES(?,?,?,?,?,?) ON CONFLICT(tenant_id,channel_id,message_ts) DO UPDATE SET event_ts=excluded.event_ts,revision_hash=excluded.revision_hash,state=excluded.state",
                           (tenant_id, channel_id, message_ts, event_ts, revision, state))
        if event_type in {"changed", "unavailable", "deleted"}:
            # snapshot 이후의 무효화 순서도 복원할 수 있게 본문 없는 watermark를 같은 삭제 journal에 남긴다.
            store._apply_event(connection, {"id": "evt_raw_state_" + canonical_digest([tenant_id, event_id]),
                "tenantId": tenant_id, "scopeChannelId": channel_id, "eventType": "raw_state",
                "requestId": event_id, "deletionKind": "raw_state", "sourceMessageId": message_ts,
                "reasonCode": json.dumps({"eventTs": event_ts, "revisionHash": revision, "state": state},
                                         ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                "cutoffAt": store._clock(), "durabilityState": "pending"})
        return acknowledge(status)
