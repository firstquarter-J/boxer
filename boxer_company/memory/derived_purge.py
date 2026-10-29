"""원문과 출처는 남기고 파생 기억만 지우는 명시적 운영자 경계."""

from __future__ import annotations

import re
from collections.abc import Sequence
from decimal import Decimal
from typing import Any

from .contracts import MemoryError, canonical_digest
from .store import MemoryStore

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}")
_STAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")


def _stamp(value: Any) -> Decimal:
    if not isinstance(value, str) or not _STAMP.fullmatch(value) or not 0 <= Decimal(value) < 2**63:
        raise MemoryError("memory_derived_period_invalid")
    return Decimal(value)


def operator_purge_derived(store: MemoryStore, *, tenant_id: str, channel_ids: Sequence[str],
                           operator_id: str, request_id: str, start_ts: str | None = None,
                           end_ts: str | None = None) -> dict[str, Any]:
    """선택한 채널 전체 또는 근거 하나라도 기간 안에 드는 파생 item 전체를 제거한다."""

    if (any(not isinstance(value, str) or not _IDENTIFIER.fullmatch(value)
            for value in (tenant_id, operator_id, request_id))
            or not isinstance(channel_ids, (list, tuple)) or not 0 < len(channel_ids) <= 100
            or any(not isinstance(value, str) or not _IDENTIFIER.fullmatch(value) for value in channel_ids)
            or len(set(channel_ids)) != len(channel_ids)):
        raise MemoryError("memory_derived_scope_invalid")
    if (start_ts is None) != (end_ts is None):
        raise MemoryError("memory_derived_period_invalid")
    bounds = None if start_ts is None else (_stamp(start_ts), _stamp(end_ts))
    if bounds and bounds[0] >= bounds[1]:
        raise MemoryError("memory_derived_period_invalid")
    channels = sorted(channel_ids)
    manifest = canonical_digest({"tenantId": tenant_id, "channelIds": channels,
                                 "operatorId": operator_id, "requestId": request_id,
                                 "startTs": start_ts, "endTs": end_ts})
    with store._connection(write=True) as connection:
        previous = connection.execute("""SELECT reason_code FROM memory_events WHERE tenant_id=? AND request_id=?
            AND event_type IN ('derived_purge','derived_purge_scope')""", (tenant_id, request_id)).fetchall()
        if any(row["reason_code"] != manifest for row in previous):
            raise MemoryError("memory_derived_request_conflict")
        markers = connection.execute("""SELECT scope_channel_id,from_version FROM memory_events WHERE tenant_id=?
            AND request_id=? AND event_type='derived_purge_scope'""", (tenant_id, request_id)).fetchall()
        prior_counts = dict(connection.execute("""SELECT scope_channel_id,COUNT(*) FROM memory_events WHERE tenant_id=?
            AND request_id=? AND deletion_kind='derived_item' AND memory_id IS NOT NULL GROUP BY scope_channel_id""",
            (tenant_id, request_id)).fetchall())
        complete = len(markers) == len(channels) and all(type(row["from_version"]) is int
            and row["from_version"] == prior_counts.get(row["scope_channel_id"], 0) for row in markers)
        if not previous or not complete:
            placeholders = ",".join("?" for _ in channels)
            rows = connection.execute(f"SELECT id,scope_channel_id FROM memory_items WHERE tenant_id=? AND scope_channel_id IN ({placeholders})",
                                      (tenant_id, *channels)).fetchall()
            selected = []
            for row in rows:
                if bounds is not None:
                    timestamps = connection.execute("""SELECT s.message_ts FROM memory_item_sources r
                        JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id
                        WHERE r.tenant_id=? AND r.memory_id=?""", (tenant_id, row["id"])).fetchall()
                    if not any(bounds[0] <= _stamp(source["message_ts"]) < bounds[1] for source in timestamps):
                        continue
                selected.append(row)
            # 채널 marker도 journal에 보존해 item이 0건인 요청의 cache 정리와 범위 고정을 복원한다.
            for channel in channels:
                event = {"id": "evt_derived_scope_" + canonical_digest([tenant_id, request_id, channel]),
                         "tenantId": tenant_id, "scopeChannelId": channel, "memoryId": None,
                         "eventType": "derived_purge_scope", "actorId": operator_id,
                         "requestId": request_id, "deletionKind": "derived_item", "reasonCode": manifest,
                         "expectedItemCount": sum(row["scope_channel_id"] == channel for row in selected),
                         "cutoffAt": store._clock(), "durabilityState": "pending"}
                store._apply_event(connection, event)
            for row in selected:
                event = {"id": "evt_derived_item_" + canonical_digest([tenant_id, request_id, row["id"]]),
                         "tenantId": tenant_id, "scopeChannelId": row["scope_channel_id"], "memoryId": row["id"],
                         "eventType": "derived_purge_item", "actorId": operator_id,
                         "requestId": request_id, "deletionKind": "derived_item", "reasonCode": manifest,
                         "cutoffAt": store._clock(), "durabilityState": "pending"}
                store._apply_event(connection, event)
            # 운영자 재호출도 새 item을 덧붙이지 않고 최초 고정 manifest의 완료 상태만 반환한다.
            connection.execute("""INSERT OR IGNORE INTO memory_events
                (id,tenant_id,scope_channel_id,event_type,actor_id,request_id,reason_code,created_at)
                VALUES(?,?,?,'derived_purge',?,?,?,?)""",
                ("evt_derived_request_" + canonical_digest([tenant_id, request_id]), tenant_id, channels[0],
                 operator_id, request_id, manifest, store._clock()))
        events = connection.execute("""SELECT memory_id,durability_state FROM memory_events WHERE tenant_id=?
            AND request_id=? AND deletion_kind='derived_item'""", (tenant_id, request_id)).fetchall()
        # journal 일부만 복원됐다면 같은 요청으로 나머지 item을 처리한 뒤에만 완료로 본다.
        expected = dict(connection.execute("""SELECT scope_channel_id,from_version FROM memory_events WHERE tenant_id=?
            AND request_id=? AND event_type='derived_purge_scope'""", (tenant_id, request_id)).fetchall())
        actual = dict(connection.execute("""SELECT scope_channel_id,COUNT(*) FROM memory_events WHERE tenant_id=?
            AND request_id=? AND deletion_kind='derived_item' AND memory_id IS NOT NULL GROUP BY scope_channel_id""",
            (tenant_id, request_id)).fetchall())
        if set(expected) != set(channels) or any(expected[channel] != actual.get(channel, 0) for channel in channels):
            raise MemoryError("memory_derived_recovery_incomplete")
        return {"state": "purged", "purgedItemCount": sum(row["memory_id"] is not None for row in events),
                "scopeChannelCount": len(channels), "journalEventCount": len(events),
                "durability": "durable" if all(row["durability_state"] == "durable" for row in events) else "pending"}


__all__ = ["operator_purge_derived"]
