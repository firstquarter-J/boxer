"""모델 추출을 거치지 않는, 명시 승인된 과거 원문 적재의 운영자 경계다."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from boxer_company.memory.contracts import MemoryError, canonical_digest
from boxer_company.memory.raw_archive import begin_raw_import, import_raw_messages
from boxer_company.memory.store import MemoryStore

_STAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")


def import_raw_archive(store: MemoryStore, payload: dict[str, Any], *, tenant_id: str,
                       channel_id: str, author_id: str, operator_id: str,
                       begin_only: bool = False, now: float | None = None) -> dict[str, Any]:
    """범위를 먼저 등록하고, 그 이후 원본 검증을 끝낸 동일 manifest만 게시한다."""
    required = {"schemaVersion", "kind", "importId", "tenantId", "channelId", "channelKind",
                "authorId", "startTs", "cutoffTs", "manifestDigest", "sources", "coverage"}
    if (not isinstance(payload, dict) or not required <= payload.keys()
            or payload.keys() - required - {"verifiedAt", "excludedCounts"}
            or any(not isinstance(payload.get(key), str) or not _IDENTIFIER.fullmatch(payload[key])
                   for key in ("importId", "tenantId", "channelId", "authorId"))
            or not isinstance(payload["coverage"], str) or not isinstance(payload["sources"], list)
            or type(payload["schemaVersion"]) is not int or payload["schemaVersion"] != 1
            or payload["kind"] != "slack_raw_archive" or payload["channelKind"] != "private_channel"
            or payload["coverage"] not in {"complete", "partial"}
            or (payload["tenantId"], payload["channelId"], payload["authorId"]) != (tenant_id, channel_id, author_id)):
        raise MemoryError("raw_import_scope_invalid")
    manifest = {key: payload[key] for key in ("tenantId", "channelId", "authorId", "startTs", "cutoffTs", "sources")}
    try:
        digest = canonical_digest(manifest)
    except (TypeError, ValueError, OverflowError) as exc:
        raise MemoryError("raw_import_manifest_invalid") from exc
    if digest != payload["manifestDigest"]:
        raise MemoryError("raw_import_manifest_invalid")
    excluded = payload.get("excludedCounts", {})
    if (not isinstance(excluded, Mapping)
            or any(not isinstance(key, str) or not re.fullmatch(r"[a-z_]{1,64}", key)
                   or type(value) is not int or not 0 <= value <= 100000 for key, value in excluded.items())):
        raise MemoryError("raw_import_exclusions_invalid")
    identity = payload["importId"]
    if begin_only:
        return begin_raw_import(store, import_id=identity, tenant_id=tenant_id, channel_id=channel_id,
                                author_id=author_id, operator_id=operator_id, start_ts=payload["startTs"],
                                cutoff_ts=payload["cutoffTs"], manifest_digest=payload["manifestDigest"])
    verified = payload.get("verifiedAt")
    clock = time.time() if now is None else now
    if (not isinstance(verified, str) or not _STAMP.fullmatch(verified)
            or not 0 <= Decimal(str(clock)) - Decimal(verified) <= 300):
        raise MemoryError("raw_import_verification_stale")
    with store._connection() as connection:
        row = connection.execute("SELECT * FROM memory_raw_imports WHERE tenant_id=? AND id=?", (tenant_id, identity)).fetchone()
        # 등록 전의 원본 확인은 수집 중 수정/삭제 경쟁을 막지 못하므로 인정하지 않는다.
        if (row is None or row["operator_id"] != operator_id or row["manifest_digest"] != payload["manifestDigest"]
                or Decimal(str(row["created_at"])) > Decimal(verified)):
            raise MemoryError("raw_import_registration_required")
    result = import_raw_messages(store, import_id=identity, manifest_digest=payload["manifestDigest"], sources=payload["sources"])
    return {**result, "coverage": payload["coverage"], "excludedCounts": dict(excluded)}
