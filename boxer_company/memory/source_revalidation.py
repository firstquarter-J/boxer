"""현재 Slack 원본을 다시 확인한 운영자만 옛 원문 수정 무효화를 재승인한다.

계획은 등록 뒤, 원본 확인 전에 저장한다. 재승인은 본문 없는 journal 이벤트이며
내구화된 정확한 대상 하나만 대체한다. 실제 본문은 별도 fresh import가 저장한다.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES

from .contracts import MemoryError, canonical_digest, source_identity
from .raw_policy import (
    RAW_ONLY_CHANNEL_IDS,
    RAW_ONLY_END_TS,
    RAW_ONLY_START_TS,
    RAW_ONLY_TENANT_ID,
    is_raw_only_channel,
    is_raw_only_scope,
)

if TYPE_CHECKING:
    from .store import MemoryStore

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")
_SQL_ALIAS = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_DIGEST = re.compile(r"[a-f0-9]{64}")
_EVENT_TYPE = "raw_source_revalidated"
_PLAN_TYPE = "raw_revalidation_plan"


def _fail(suffix: str) -> None:
    raise MemoryError("raw_import_revalidation_" + suffix)


def _identifier(value: Any) -> bool:
    return isinstance(value, str) and _IDENTIFIER.fullmatch(value) is not None


def _release_match_sql(alias: str, release_alias: str) -> str:
    if any(not isinstance(value, str) or not _SQL_ALIAS.fullmatch(value) for value in (alias, release_alias)):
        raise MemoryError("memory_raw_revalidation_alias_invalid")
    channels = ",".join("'" + channel + "'" for channel in sorted(RAW_ONLY_CHANNEL_IDS))
    # 승인 범위와 정확한 옛 이벤트를 고정하며 pending journal은 차단을 해제하지 않는다.
    return f"""{release_alias}.tenant_id={alias}.tenant_id
        AND {release_alias}.scope_channel_id={alias}.scope_channel_id
        AND {release_alias}.source_id={alias}.source_id
        AND {release_alias}.source_message_id={alias}.source_message_id
        AND {release_alias}.reallow_request_id={alias}.id
        AND {release_alias}.event_type='{_EVENT_TYPE}' AND {release_alias}.deletion_kind='raw_state'
        AND {release_alias}.durability_state='durable' AND {release_alias}.remote_sequence>0
        AND {alias}.deletion_kind='source_revision' AND {alias}.event_type='source_changed'
        AND substr({alias}.request_id,1,4)='raw:'
        AND {alias}.tenant_id='{RAW_ONLY_TENANT_ID}' AND {alias}.scope_channel_id IN ({channels})
        AND CAST({alias}.source_message_id AS REAL)>=CAST('{RAW_ONLY_START_TS}' AS REAL)
        AND CAST({alias}.source_message_id AS REAL)<CAST('{RAW_ONLY_END_TS}' AS REAL)"""


def _release_sql(alias: str) -> str:
    # 호출자가 같은 alias를 썼더라도 안쪽 조회가 바깥 이벤트를 가리지 않게 한다.
    release_alias = "raw_release_check" if isinstance(alias, str) and alias.casefold() == "raw_release" else "raw_release"
    return f"EXISTS (SELECT 1 FROM memory_events {release_alias} WHERE {_release_match_sql(alias, release_alias)})"


def effective_revision_sql(alias: str = "e") -> str:
    """source_revision 행이 현재도 차단하는지 검사하는 공통 SQL 조건이다."""
    return "NOT " + _release_sql(alias)


def is_source_revision_blocked(connection: sqlite3.Connection, tenant_id: str,
                               channel_id: str, source_id: str) -> bool:
    return connection.execute(f"""SELECT 1 FROM memory_events e WHERE e.tenant_id=?
        AND e.scope_channel_id=? AND e.source_id=? AND e.deletion_kind='source_revision'
        AND {effective_revision_sql('e')} LIMIT 1""", (tenant_id, channel_id, source_id)).fetchone() is not None


def is_revision_event_revalidated(connection: sqlite3.Connection, event: Mapping[str, Any]) -> bool:
    """복원할 옛 행이 아직 없어도 같은 공통 조건으로 정확한 재승인 여부를 확인한다."""
    if not isinstance(event, Mapping):
        return False
    values = (event.get("id") or event.get("eventId"), event.get("tenantId"),
              event.get("scopeChannelId"), event.get("sourceId"), event.get("sourceMessageId"),
              event.get("deletionKind"), event.get("eventType"), event.get("requestId"))
    return bool(connection.execute(f"""SELECT {_release_sql('e')} FROM
        (SELECT ? AS id,? AS tenant_id,? AS scope_channel_id,? AS source_id,
         ? AS source_message_id,? AS deletion_kind,? AS event_type,? AS request_id) e""", values).fetchone()[0])


def is_raw_invalidation_event_revalidated(connection: sqlite3.Connection, event: Mapping[str, Any],
                                         payload: Mapping[str, str]) -> bool:
    """같은 원인의 옛 unavailable만 건너뛴다. 새 영수증과 명시 삭제는 항상 적용한다."""
    if (payload.get("state") != "unavailable" or event.get("deletionKind") != "raw_state"
            or event.get("eventType") != "raw_state" or not _identifier(event.get("requestId"))):
        return False
    reason = json.dumps({"eventTs": payload["eventTs"], "revisionHash": payload["revisionHash"],
                         "state": "current"}, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return connection.execute(f"""SELECT 1 FROM memory_events e,memory_events r
        WHERE e.tenant_id=? AND e.scope_channel_id=? AND e.source_message_id=? AND e.request_id=?
        AND {_release_match_sql('e', 'r')} AND r.reason_code=? LIMIT 1""",
        (event.get("tenantId"), event.get("scopeChannelId"), event.get("sourceMessageId"),
         "raw:" + event["requestId"], reason)).fetchone() is not None


def validate_revalidation_event(event: Mapping[str, Any], parsed_payload: Mapping[str, str]) -> None:
    """journal의 구조만 확인하며 과거 재승인을 현재 프로필 설정으로 재판정하지 않는다."""
    tenant, channel, message = event.get("tenantId"), event.get("scopeChannelId"), event.get("sourceMessageId")
    revision = parsed_payload.get("revisionHash")
    if (event.get("deletionKind") != "raw_state" or event.get("eventType") != _EVENT_TYPE
            or not is_raw_only_scope(tenant, channel, message)
            or set(parsed_payload) != {"eventTs", "revisionHash", "state"}
            or parsed_payload.get("state") != "current" or not isinstance(revision, str)
            or not _DIGEST.fullmatch(revision)
            or event.get("sourceId") != source_identity(tenant, channel, message, revision)
            or not all(_identifier(event.get(key)) for key in ("id", "actorId", "requestId", "reallowRequestId"))
            or not event["actorId"].startswith(("U", "W"))
            or event.get("memoryId") is not None or event.get("subjectId") is not None
            or event.get("topicKeyHash") is not None):
        raise MemoryError("memory_raw_revalidation_event_invalid")


def _hard_blocked(connection: sqlite3.Connection, tenant: str, channel: str, message: str) -> bool:
    return connection.execute("""SELECT 1 FROM memory_events WHERE tenant_id=? AND scope_channel_id=?
        AND source_message_id=? AND deletion_kind IN ('source','source_redaction') LIMIT 1""",
        (tenant, channel, message)).fetchone() is not None


def _old_target(connection: sqlite3.Connection, *, tenant: str, channel: str, message: str,
                source_id: str, target_id: str) -> sqlite3.Row:
    target = connection.execute("SELECT * FROM memory_events WHERE id=?", (target_id,)).fetchone()
    if (target is None or (target["tenant_id"], target["scope_channel_id"], target["source_id"],
            target["source_message_id"], target["deletion_kind"], target["event_type"])
            != (tenant, channel, source_id, message, "source_revision", "source_changed")
            or not str(target["request_id"]).startswith("raw:") or target["durability_state"] != "durable"
            or type(target["remote_sequence"]) is not int or target["remote_sequence"] < 1):
        _fail("target_invalid")
    return target


def apply_revalidation_metadata(connection: sqlite3.Connection, event: Mapping[str, Any]) -> bool:
    """정확한 옛 상태만 현재로 바꾼다. 본문 생성과 옛 무효화 행 수정은 하지 않는다."""
    from .raw_state import parse_raw_state_event

    payload = parse_raw_state_event(event)
    validate_revalidation_event(event, payload)
    tenant, channel, message, source_id = (event[key] for key in
                                          ("tenantId", "scopeChannelId", "sourceMessageId", "sourceId"))
    _old_target(connection, tenant=tenant, channel=channel, message=message,
                source_id=source_id, target_id=event["reallowRequestId"])
    if _hard_blocked(connection, tenant, channel, message):
        return False
    watermark = connection.execute("""SELECT * FROM memory_raw_event_states
        WHERE tenant_id=? AND channel_id=? AND message_ts=?""", (tenant, channel, message)).fetchone()
    if watermark is None:
        _fail("watermark_required")
    if (watermark["state"], watermark["event_ts"], watermark["revision_hash"]) != (
            "unavailable", payload["eventTs"], payload["revisionHash"]):
        # 더 새로운 삭제나 편집은 재승인 이벤트보다 항상 우선한다.
        return False
    other = connection.execute(f"""SELECT 1 FROM memory_events e WHERE e.tenant_id=?
        AND e.scope_channel_id=? AND e.source_id=? AND e.deletion_kind='source_revision'
        AND e.id!=? AND {effective_revision_sql('e')} LIMIT 1""",
        (tenant, channel, source_id, event["reallowRequestId"])).fetchone()
    if other:
        return False
    consumed = connection.execute("""SELECT id FROM memory_events WHERE tenant_id=?
        AND reallow_request_id=? AND event_type=? AND deletion_kind='raw_state' AND id!=? LIMIT 1""",
        (tenant, event["reallowRequestId"], _EVENT_TYPE, event["id"])).fetchone()
    if consumed:
        _fail("target_consumed")
    current = connection.execute("""SELECT edited_at,message_ts FROM memory_sources
        WHERE tenant_id=? AND channel_id=? AND message_ts=? AND state='current'""",
        (tenant, channel, message)).fetchall()
    if any(Decimal(source["edited_at"] or source["message_ts"]) > Decimal(payload["eventTs"]) for source in current):
        # 옛 snapshot의 재승인도 더 최신 source metadata나 원문을 되돌리지는 않는다.
        return False
    # 현재 본문이 있더라도 별도 원본 재검증 import만 다시 채울 수 있게 먼저 제거한다.
    connection.execute("DELETE FROM memory_source_texts WHERE tenant_id=? AND source_id=?", (tenant, source_id))
    connection.execute("UPDATE memory_sources SET state='current',excerpt='' WHERE tenant_id=? AND channel_id=? AND id=? AND message_ts=? AND revision_hash=?",
                       (tenant, channel, source_id, message, payload["revisionHash"]))
    connection.execute("""UPDATE memory_raw_event_states SET state='current'
        WHERE tenant_id=? AND channel_id=? AND message_ts=?""", (tenant, channel, message))
    return True


def _registration(connection: sqlite3.Connection, *, import_id: str, tenant_id: str,
                  channel_id: str, author_id: str, operator_id: str,
                  manifest_digest: str) -> sqlite3.Row:
    profiles = {str(profile.get("user_id") or "") for profile in TEAM_MEMBER_PROFILES}
    if (not is_raw_only_channel(tenant_id, channel_id)
            or not all(_identifier(value) for value in (import_id, author_id, operator_id))
            or not author_id.startswith(("U", "W")) or not operator_id.startswith(("U", "W"))
            or author_id not in profiles or operator_id not in profiles):
        _fail("scope_invalid")
    row = connection.execute("SELECT * FROM memory_raw_imports WHERE id=?", (import_id,)).fetchone()
    if (row is None or (row["tenant_id"], row["channel_id"], row["author_id"], row["operator_id"],
            row["manifest_digest"]) != (tenant_id, channel_id, author_id, operator_id, manifest_digest)
            or row["state"] not in {"collecting", "completed"}
            or not is_raw_only_scope(tenant_id, channel_id, row["start_ts"])
            or not is_raw_only_scope(tenant_id, channel_id, row["cutoff_ts"])):
        _fail("registration_required")
    if connection.execute("""SELECT 1 FROM memory_threads WHERE tenant_id=? AND channel_id=?
        AND channel_kind!='private_channel' LIMIT 1""", (tenant_id, channel_id)).fetchone():
        _fail("private_channel_required")
    return row


def _known_identity(connection: sqlite3.Connection, row: sqlite3.Row, thread: str,
                    source: Mapping[str, Any]) -> sqlite3.Row | None:
    tenant, channel, message = row["tenant_id"], row["channel_id"], source["messageTs"]
    previous = connection.execute("""SELECT s.*,t.thread_ts,t.channel_kind FROM memory_sources s
        LEFT JOIN memory_threads t ON t.tenant_id=s.tenant_id AND t.id=s.thread_id
        AND t.channel_id=s.channel_id WHERE s.tenant_id=? AND s.channel_id=? AND s.message_ts=?""",
        (tenant, channel, message)).fetchall()
    known = None
    for old in previous:
        if (old["author_id"] != row["author_id"] or old["author_kind"] != "human"
                or old["thread_ts"] != thread or old["channel_kind"] != "private_channel"):
            _fail("source_identity_conflict")
        if old["id"] == source["sourceId"]:
            actual = tuple(old[key] for key in ("revision_hash", "created_at", "edited_at", "permalink"))
            expected = tuple(source[key] for key in ("revisionHash", "createdAt", "editedAt", "permalink"))
            if actual != expected:
                _fail("source_identity_conflict")
            known = old
        elif old["state"] == "current" and Decimal(old["edited_at"] or old["message_ts"]) >= Decimal(
                source["editedAt"] or source["messageTs"]):
            _fail("source_stale")
    return known


def _latest_plan(connection: sqlite3.Connection, row: sqlite3.Row) -> sqlite3.Row | None:
    plan = connection.execute("""SELECT * FROM memory_events WHERE tenant_id=? AND request_id=?
        AND event_type=? ORDER BY rowid DESC LIMIT 1""", (row["tenant_id"], row["id"], _PLAN_TYPE)).fetchone()
    if plan is not None and (plan["scope_channel_id"] != row["channel_id"]
                            or plan["actor_id"] != row["operator_id"]):
        _fail("plan_conflict")
    return plan


def _own_reauthorizations(connection: sqlite3.Connection, plan: sqlite3.Row | None) -> list[sqlite3.Row]:
    if plan is None:
        return []
    rows = connection.execute("""SELECT * FROM memory_events WHERE tenant_id=? AND request_id=?
        AND event_type=? ORDER BY id""", (plan["tenant_id"], plan["id"], _EVENT_TYPE)).fetchall()
    if rows and (len(rows) != plan["from_version"] or any(
            event["scope_channel_id"] != plan["scope_channel_id"] or event["actor_id"] != plan["actor_id"]
            or event["deletion_kind"] != "raw_state" for event in rows)):
        _fail("plan_conflict")
    return rows


def _event_metadata(event: sqlite3.Row) -> dict[str, Any]:
    return {"id": event["id"], "tenantId": event["tenant_id"],
        "scopeChannelId": event["scope_channel_id"], "sourceMessageId": event["source_message_id"],
        "sourceId": event["source_id"], "eventType": event["event_type"], "actorId": event["actor_id"],
        "requestId": event["request_id"], "reallowRequestId": event["reallow_request_id"],
        "deletionKind": event["deletion_kind"], "reasonCode": event["reason_code"]}


def _checkpoint(connection: sqlite3.Connection, row: sqlite3.Row,
                normalized: list[tuple[str, dict[str, Any], str]],
                own: list[sqlite3.Row]) -> str:
    from .raw_state import parse_raw_state_event

    messages = {source["messageTs"] for _, source, _ in normalized}
    source_ids = {source["sourceId"] for _, source, _ in normalized}
    records: dict[str, dict[str, Any]] = {}
    for _, source, _ in normalized:
        watermark = connection.execute("""SELECT event_ts,revision_hash,state FROM memory_raw_event_states
            WHERE tenant_id=? AND channel_id=? AND message_ts=?""",
            (row["tenant_id"], row["channel_id"], source["messageTs"])).fetchone()
        records[source["messageTs"]] = {"sourceId": source["sourceId"], "watermark":
            dict(watermark) if watermark is not None else None, "events": [], "otherCurrentSources": []}
    own_ids = {event["id"] for event in own}
    # 자체 재승인만 원래 unavailable로 정규화한다. 새로운 영수증이나 무효화는 그대로 hash에 남는다.
    for event in own:
        parsed = parse_raw_state_event(_event_metadata(event))
        record = records.get(event["source_message_id"])
        if record is None or record["sourceId"] != event["source_id"]:
            _fail("plan_conflict")
        watermark = record["watermark"]
        if watermark == {"event_ts": parsed["eventTs"], "revision_hash": parsed["revisionHash"], "state": "current"}:
            watermark["state"] = "unavailable"
    events = connection.execute("""SELECT id,source_id,source_message_id,event_type,deletion_kind,reason_code
        FROM memory_events WHERE tenant_id=? AND scope_channel_id=? AND
        (event_type='raw_ingest' OR deletion_kind IN ('source_revision','raw_state','source','source_redaction'))
        ORDER BY id""", (row["tenant_id"], row["channel_id"])).fetchall()
    for event in events:
        if event["source_message_id"] in messages and event["id"] not in own_ids:
            records[event["source_message_id"]]["events"].append(list(event))
    # 영수증 없는 다른 snapshot이 더 최신 revision을 등록하는 경쟁도 감지한다.
    current = connection.execute("""SELECT id,message_ts,revision_hash,edited_at FROM memory_sources
        WHERE tenant_id=? AND channel_id=? AND state='current' ORDER BY id""",
        (row["tenant_id"], row["channel_id"])).fetchall()
    for source in current:
        if source["message_ts"] in messages and source["id"] not in source_ids:
            records[source["message_ts"]]["otherCurrentSources"].append(list(source))
    return canonical_digest({"tenantId": row["tenant_id"], "channelId": row["channel_id"],
        "authorId": row["author_id"], "operatorId": row["operator_id"], "importId": row["id"],
        "manifestDigest": row["manifest_digest"], "sources": records})


def _targets(connection: sqlite3.Connection, row: sqlite3.Row,
             normalized: list[tuple[str, dict[str, Any], str]]) -> list[tuple[dict[str, Any], sqlite3.Row, sqlite3.Row]]:
    targets = []
    for thread, source, _ in normalized:
        tenant, channel, message = row["tenant_id"], row["channel_id"], source["messageTs"]
        if not is_raw_only_scope(tenant, channel, message) or _hard_blocked(connection, tenant, channel, message):
            _fail("source_deleted")
        known = _known_identity(connection, row, thread, source)
        watermark = connection.execute("""SELECT * FROM memory_raw_event_states WHERE tenant_id=?
            AND channel_id=? AND message_ts=?""", (tenant, channel, message)).fetchone()
        blocked = connection.execute(f"""SELECT * FROM memory_events e WHERE e.tenant_id=?
            AND e.scope_channel_id=? AND e.source_id=? AND e.deletion_kind='source_revision'
            AND {effective_revision_sql('e')} ORDER BY e.id""", (tenant, channel, source["sourceId"])).fetchall()
        if watermark is not None and watermark["state"] == "deleted":
            _fail("source_deleted")
        if not blocked:
            if watermark is not None and watermark["state"] == "unavailable" or known is not None and known["state"] != "current":
                _fail("target_required")
            continue
        if (len(blocked) != 1 or known is None or watermark is None or watermark["state"] != "unavailable"
                or watermark["revision_hash"] != source["revisionHash"]
                or known["state"] not in {"unavailable", "deleted"}
                or Decimal(source["editedAt"] or message) > Decimal(watermark["event_ts"])):
            _fail("target_invalid")
        target = _old_target(connection, tenant=tenant, channel=channel, message=message,
                             source_id=source["sourceId"], target_id=blocked[0]["id"])
        if connection.execute("""SELECT 1 FROM memory_events WHERE tenant_id=? AND reallow_request_id=?
            AND event_type=? LIMIT 1""", (tenant, target["id"], _EVENT_TYPE)).fetchone():
            _fail("target_consumed")
        targets.append((source, target, watermark))
    if targets and row["state"] == "completed":
        _fail("new_import_required")
    return targets


def _plan_metadata(plan: sqlite3.Row) -> dict[str, Any]:
    return {"state": "prepared", "planId": plan["id"], "checkpointSha256": plan["reason_code"],
            "targetCount": plan["from_version"], "createdAt": plan["created_at"]}


class _ConnectionReader:
    """외부 read-only snapshot에서도 등록 검증이 추가 연결이나 PRAGMA 없이 동작하게 한다."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        yield self.connection


def _registered_readiness(store: MemoryStore, connection: sqlite3.Connection, payload: Mapping[str, Any], *,
                          tenant_id: str, channel_id: str, author_id: str, operator_id: str,
                          now: float | None) -> tuple[sqlite3.Row, sqlite3.Row, list[sqlite3.Row],
                                                    list[tuple[dict[str, Any], sqlite3.Row, sqlite3.Row]]]:
    from .raw_archive import (
        _check_current_source,
        _validate_sources,
        verified_raw_registration,
    )
    from .raw_state import parse_raw_state_event

    if not isinstance(payload, Mapping) or not all(key in payload for key in ("importId", "manifestDigest", "sources", "verifiedAt")):
        _fail("payload_invalid")
    # 읽기 검증과 실제 기록은 같은 snapshot과 등록 이후 300초의 계약을 공유한다.
    verified_raw_registration(_ConnectionReader(connection), import_id=payload["importId"], tenant_id=tenant_id,
                              channel_id=channel_id, author_id=author_id, operator_id=operator_id,
                              manifest_digest=payload["manifestDigest"], verified_at=payload["verifiedAt"], now=now)
    row = _registration(connection, import_id=payload["importId"], tenant_id=tenant_id, channel_id=channel_id,
                        author_id=author_id, operator_id=operator_id, manifest_digest=payload["manifestDigest"])
    normalized = _validate_sources(row, payload["sources"])
    plan = _latest_plan(connection, row)
    if plan is None or Decimal(str(plan["created_at"])) > Decimal(payload["verifiedAt"]):
        _fail("plan_required")
    own = _own_reauthorizations(connection, plan)
    if _checkpoint(connection, row, normalized, own) != plan["reason_code"]:
        _fail("checkpoint_changed")
    for thread, source, _ in normalized:
        _known_identity(connection, row, thread, source)
    if own:
        # pending 재시도도 자체의 정확한 승인만 허용하며 새로운 무효화는 제외하지 않는다.
        for event in own:
            parsed = parse_raw_state_event(_event_metadata(event))
            _old_target(connection, tenant=tenant_id, channel=channel_id, message=event["source_message_id"],
                        source_id=event["source_id"], target_id=event["reallow_request_id"])
            watermark = connection.execute("""SELECT state,event_ts,revision_hash FROM memory_raw_event_states
                WHERE tenant_id=? AND channel_id=? AND message_ts=?""",
                (tenant_id, channel_id, event["source_message_id"])).fetchone()
            known = connection.execute("SELECT state FROM memory_sources WHERE tenant_id=? AND id=?",
                                       (tenant_id, event["source_id"])).fetchone()
            if (watermark is None or tuple(watermark) != ("current", parsed["eventTs"], parsed["revisionHash"])
                    or known is None or known["state"] != "current"):
                _fail("checkpoint_changed")
        targets = []
        exempt = {event["source_id"] for event in own}
    else:
        targets = _targets(connection, row, normalized)
        if len(targets) != plan["from_version"]:
            _fail("checkpoint_changed")
        exempt = {source["sourceId"] for source, _, _ in targets}
    for thread, source, text in normalized:
        if source["sourceId"] not in exempt:
            _check_current_source(store, connection, row, thread, source)
        archived = connection.execute("SELECT text FROM memory_source_texts WHERE tenant_id=? AND source_id=?",
                                      (tenant_id, source["sourceId"])).fetchone()
        if archived is not None and archived["text"] != text:
            raise MemoryError("raw_import_text_conflict")
        if archived is None and row["state"] == "completed":
            _fail("new_import_required")
    return row, plan, own, targets


def validate_registered_revalidation(store: MemoryStore, payload: Mapping[str, Any], *, tenant_id: str,
                                     channel_id: str, author_id: str, operator_id: str,
                                     now: float | None = None,
                                     connection: sqlite3.Connection | None = None) -> dict[str, Any]:
    """운영 사전 확인도 정식 기록 처리와 같은 검증을 읽기 전용으로 실행한다."""
    if connection is None:
        with store._connection() as reader:
            return validate_registered_revalidation(store, payload, tenant_id=tenant_id, channel_id=channel_id,
                author_id=author_id, operator_id=operator_id, now=now, connection=reader)
    _, plan, own, targets = _registered_readiness(store, connection, payload, tenant_id=tenant_id,
        channel_id=channel_id, author_id=author_id, operator_id=operator_id, now=now)
    target_ids = sorted(event["source_id"] for event in own) if own else sorted(source["sourceId"] for source, _, _ in targets)
    return {"state": "validated", "planId": plan["id"], "checkpointSha256": plan["reason_code"],
            "targetCount": len(target_ids), "targetSourceIds": target_ids, "databaseWrites": 0}


def prepare_revalidation_plan(store: MemoryStore, *, import_id: str, tenant_id: str,
                              channel_id: str, author_id: str, operator_id: str,
                              manifest_digest: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
    """등록과 SDK 원본 재검증 사이에 원문 없는 체크포인트를 불변 기록한다."""
    from .raw_archive import _validate_sources

    with store._connection(write=True) as connection:
        row = _registration(connection, import_id=import_id, tenant_id=tenant_id, channel_id=channel_id,
                            author_id=author_id, operator_id=operator_id, manifest_digest=manifest_digest)
        normalized = _validate_sources(row, sources)
        plan = _latest_plan(connection, row)
        own = _own_reauthorizations(connection, plan)
        checkpoint = _checkpoint(connection, row, normalized, own)
        if plan is not None and plan["reason_code"] == checkpoint:
            return _plan_metadata(plan)
        if own and any(event["durability_state"] != "durable" for event in own):
            _fail("checkpoint_changed")
        targets = _targets(connection, row, normalized)
        # 새 계획에는 이전 계획의 재승인도 이미 존재하는 과거 metadata로 고정한다.
        checkpoint = _checkpoint(connection, row, normalized, [])
        identity = "evt_raw_revalidation_plan_" + canonical_digest([tenant_id, import_id, checkpoint])
        previous = connection.execute("SELECT * FROM memory_events WHERE id=?", (identity,)).fetchone()
        if previous is not None:
            if (previous["actor_id"] != operator_id or previous["scope_channel_id"] != channel_id
                    or previous["reason_code"] != checkpoint or previous["from_version"] != len(targets)):
                _fail("plan_conflict")
            return _plan_metadata(previous)
        connection.execute("""INSERT INTO memory_events(id,tenant_id,scope_channel_id,event_type,
            actor_id,request_id,reason_code,from_version,durability_state,created_at)
            VALUES(?,?,?,?,?,?,?,?,'not_required',?)""", (identity, tenant_id, channel_id, _PLAN_TYPE,
            operator_id, import_id, checkpoint, len(targets), store._clock()))
        return _plan_metadata(connection.execute("SELECT * FROM memory_events WHERE id=?", (identity,)).fetchone())


def revalidate_registered_sources(store: MemoryStore, payload: Mapping[str, Any], *, tenant_id: str,
                                  channel_id: str, author_id: str, operator_id: str,
                                  now: float | None = None) -> dict[str, Any]:
    """원본 재검증 뒤 같은 transaction에서 계획을 대조해 metadata만 재승인한다."""
    with store._connection(write=True) as connection:
        _, plan, own, targets = _registered_readiness(store, connection, payload, tenant_id=tenant_id,
            channel_id=channel_id, author_id=author_id, operator_id=operator_id, now=now)
        if own:
            # 재시도는 본문 삭제를 반복하지 않고 같은 journal의 내구화만 진행한다.
            events = own
        else:
            events = []
            for source, target, watermark in targets:
                identity = "evt_raw_revalidated_" + canonical_digest([tenant_id, plan["id"], source["sourceId"], target["id"]])
                event = {"id": identity, "tenantId": tenant_id, "scopeChannelId": channel_id,
                    "sourceId": source["sourceId"], "sourceMessageId": source["messageTs"],
                    "eventType": _EVENT_TYPE, "deletionKind": "raw_state", "actorId": operator_id,
                    "requestId": plan["id"], "reallowRequestId": target["id"],
                    "reasonCode": json.dumps({"eventTs": watermark["event_ts"],
                        "revisionHash": source["revisionHash"], "state": "current"},
                        sort_keys=True, ensure_ascii=False, separators=(",", ":")),
                    "cutoffAt": store._clock(), "durabilityState": "pending"}
                store._apply_event(connection, event)
                events.append(connection.execute("SELECT * FROM memory_events WHERE id=?", (identity,)).fetchone())
        return {"state": "revalidated", "planId": plan["id"], "checkpointSha256": plan["reason_code"],
            "reauthorizedSourceCount": len(events), "reauthorizationEventIds": [event["id"] for event in events],
            "durability": "not_required" if not events else "durable" if all(
                event["durability_state"] == "durable" and event["remote_sequence"] is not None for event in events) else "pending"}


__all__ = [
    "apply_revalidation_metadata",
    "effective_revision_sql",
    "is_raw_invalidation_event_revalidated",
    "is_revision_event_revalidated",
    "is_source_revision_blocked",
    "prepare_revalidation_plan",
    "revalidate_registered_sources",
    "validate_registered_revalidation",
    "validate_revalidation_event",
]
