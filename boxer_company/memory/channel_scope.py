"""Slack 비공개 확인을 마친 고정 원문 채널의 과거 종류 metadata만 정정한다."""

from __future__ import annotations

import re
from typing import Any

from .contracts import MemoryError, canonical_digest
from .raw_policy import is_raw_only_channel
from .store import MemoryStore

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}")
_EVENT_TYPE = "private_channel_reconcile"


def operator_reconcile_private_channel(
    store: MemoryStore, *, tenant_id: str, channel_id: str, operator_id: str,
    request_id: str, confirm_slack_private_channel: bool,
) -> dict[str, Any]:
    """정식 운영자 확인으로 legacy channel만 정정하며 본문과 일반 권한은 바꾸지 않는다."""
    if (any(not isinstance(value, str) or not _IDENTIFIER.fullmatch(value)
            for value in (tenant_id, channel_id, operator_id, request_id))
            or not operator_id.startswith(("U", "W"))
            or not is_raw_only_channel(tenant_id, channel_id)
            or confirm_slack_private_channel is not True):
        raise MemoryError("memory_channel_scope_invalid")
    manifest = canonical_digest({"tenantId": tenant_id, "channelId": channel_id,
        "operatorId": operator_id, "fromKind": "channel", "toKind": "private_channel"})
    identity = "evt_private_channel_" + canonical_digest([tenant_id, request_id])
    with store._connection(write=True) as connection:
        previous = connection.execute("""SELECT * FROM memory_events WHERE tenant_id=?
            AND request_id=? AND event_type=?""", (tenant_id, request_id, _EVENT_TYPE)).fetchall()
        if len(previous) > 1 or any(row["id"] != identity or row["reason_code"] != manifest
                or row["scope_channel_id"] != channel_id or row["actor_id"] != operator_id for row in previous):
            raise MemoryError("memory_channel_request_conflict")
        counts = dict(connection.execute("""SELECT channel_kind,COUNT(*) FROM memory_threads
            WHERE tenant_id=? AND channel_id=? GROUP BY channel_kind""", (tenant_id, channel_id)).fetchall())
        # DM이나 불명확한 종류는 운영자 flag로 비공개 채널로 승격할 수 없다.
        if set(counts) - {"channel", "private_channel"}:
            raise MemoryError("memory_channel_kind_conflict")
        changed = counts.get("channel", 0)
        private = counts.get("private_channel", 0) + changed
        if previous:
            # 최초 완료 뒤 새 legacy 행이 생겼다면 재시도를 성공으로 꾸미지 않는다.
            if changed:
                raise MemoryError("memory_channel_reconcile_state_changed")
            event = previous[0]
            changed, private = event["from_version"], event["to_version"]
        else:
            # 같은 쓰기 transaction에서 종류만 바꾸고 감사 기록까지 함께 남긴다.
            connection.execute("""UPDATE memory_threads SET channel_kind='private_channel'
                WHERE tenant_id=? AND channel_id=? AND channel_kind='channel'""", (tenant_id, channel_id))
            # 이 metadata event의 version 두 칸은 최초 정정 수와 정정 후 채널 수다.
            # deletion_kind가 없어 삭제 journal과 원문 철회 경로에는 들어가지 않는다.
            connection.execute("""INSERT INTO memory_events
                (id,tenant_id,scope_channel_id,event_type,actor_id,request_id,from_version,to_version,reason_code,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?)""", (identity, tenant_id, channel_id, _EVENT_TYPE, operator_id,
                    request_id, changed, private, manifest, store._clock()))
        return {"state": "reconciled", "channelId": channel_id, "changedThreadCount": changed,
                "privateThreadCount": private, "eventId": identity, "scopeDigest": manifest,
                "replayed": bool(previous)}


__all__ = ["operator_reconcile_private_channel"]
