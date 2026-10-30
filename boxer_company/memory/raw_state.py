"""원문 없는 revision watermark journal의 엄격한 공통 검증 계약."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from .contracts import MemoryError
from .raw_policy import is_raw_only_scope

_STAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_DIGEST = re.compile(r"[a-f0-9]{64}")
# 동일 시각 전달 충돌은 더 보수적인 상태만 채택하고 삭제는 영구 보존한다.
RAW_STATE_PRIORITY = {"current": 0, "unavailable": 1, "deleted": 2}


def raw_state_stamp(value: Any) -> Decimal:
    if not isinstance(value, str) or not _STAMP.fullmatch(value) or not 0 <= Decimal(value) < 2**63:
        raise MemoryError("memory_raw_state_invalid")
    return Decimal(value)


def parse_raw_state_event(event: Mapping[str, Any]) -> dict[str, str]:
    """정확한 세 metadata 필드와 원문 전용 범위만 받으며 임의 본문은 journal에 싣지 않는다."""

    reason = event.get("reasonCode")
    if (event.get("deletionKind") != "raw_state" or not isinstance(reason, str) or len(reason) > 512
            or not is_raw_only_scope(event.get("tenantId"), event.get("scopeChannelId"), event.get("sourceMessageId"))):
        raise MemoryError("memory_raw_state_invalid")
    try:
        payload = json.loads(reason)
    except (ValueError, TypeError) as exc:
        raise MemoryError("memory_raw_state_invalid") from exc
    if (not isinstance(payload, dict) or set(payload) != {"eventTs", "revisionHash", "state"}
            or not isinstance(payload["state"], str) or payload["state"] not in {"current", "unavailable", "deleted"}
            or not isinstance(payload["revisionHash"], str)
            or (payload["revisionHash"] != "" and not _DIGEST.fullmatch(payload["revisionHash"]))
            or (payload["state"] == "current" and not _DIGEST.fullmatch(payload["revisionHash"]))
            or raw_state_stamp(payload["eventTs"]) < raw_state_stamp(event["sourceMessageId"])):
        raise MemoryError("memory_raw_state_invalid")
    if json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) != reason:
        raise MemoryError("memory_raw_state_invalid")
    if event.get("eventType") == "raw_source_revalidated":
        from .source_revalidation import validate_revalidation_event

        # 재허용은 운영자가 등록 후 재검증한 정확한 무효화 이벤트에만 연결한다.
        validate_revalidation_event(event, payload)
    return payload


__all__ = ["RAW_STATE_PRIORITY", "parse_raw_state_event", "raw_state_stamp"]
