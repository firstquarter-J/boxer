"""모델 추출을 거치지 않는, 명시 승인된 과거 원문 적재의 운영자 경계다."""

from __future__ import annotations

import re
import time
from collections.abc import Mapping
from typing import Any

from boxer_company.memory.contracts import MemoryError, canonical_digest
from boxer_company.memory.raw_archive import (
    begin_raw_import,
    import_raw_messages,
    verified_raw_registration,
)
from boxer_company.memory.store import MemoryStore

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}")


def _validate_raw_archive_payload(payload: dict[str, Any], *, tenant_id: str,
                                  channel_id: str, author_id: str) -> tuple[str, dict[str, int]]:
    """일반 import와 명시 재검증이 같은 파일, 범위와 manifest 계약을 확인한다."""
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
    return payload["importId"], dict(excluded)


def import_raw_archive(store: MemoryStore, payload: dict[str, Any], *, tenant_id: str,
                       channel_id: str, author_id: str, operator_id: str,
                       begin_only: bool = False, plan_revalidation: bool = False,
                       now: float | None = None) -> dict[str, Any]:
    """범위를 먼저 등록하고, 그 이후 원본 검증을 끝낸 동일 manifest만 게시한다."""
    identity, excluded = _validate_raw_archive_payload(payload, tenant_id=tenant_id,
        channel_id=channel_id, author_id=author_id)
    if plan_revalidation and not begin_only:
        raise MemoryError("raw_import_revalidation_plan_requires_registration")
    if begin_only:
        result = begin_raw_import(store, import_id=identity, tenant_id=tenant_id, channel_id=channel_id,
                                author_id=author_id, operator_id=operator_id, start_ts=payload["startTs"],
                                cutoff_ts=payload["cutoffTs"], manifest_digest=payload["manifestDigest"])
        if plan_revalidation:
            from boxer_company.memory.source_revalidation import (
                prepare_revalidation_plan,
            )

            # SDK를 읽기 전에 정확한 무효화와 영수증 상태를 고정한다.
            plan = prepare_revalidation_plan(store, import_id=identity, tenant_id=tenant_id,
                channel_id=channel_id, author_id=author_id, operator_id=operator_id,
                manifest_digest=payload["manifestDigest"], sources=payload["sources"])
            result = {**result, "revalidationPlan": plan}
        return result
    verified_raw_registration(store, import_id=identity, tenant_id=tenant_id, channel_id=channel_id,
        author_id=author_id, operator_id=operator_id, manifest_digest=payload["manifestDigest"],
        verified_at=payload.get("verifiedAt"), now=time.time() if now is None else now)
    result = import_raw_messages(store, import_id=identity, manifest_digest=payload["manifestDigest"], sources=payload["sources"])
    return {**result, "coverage": payload["coverage"], "excludedCounts": excluded}


def revalidate_raw_archive(store: MemoryStore, payload: dict[str, Any], *, tenant_id: str,
                           channel_id: str, author_id: str, operator_id: str,
                           now: float | None = None) -> dict[str, Any]:
    """등록 후 확인한 원문만 재검증하며 본문 저장은 일반 import가 수행한다."""
    _validate_raw_archive_payload(payload, tenant_id=tenant_id, channel_id=channel_id, author_id=author_id)
    from boxer_company.memory.source_revalidation import revalidate_registered_sources

    return revalidate_registered_sources(store, payload, tenant_id=tenant_id, channel_id=channel_id,
        author_id=author_id, operator_id=operator_id, now=time.time() if now is None else now)
