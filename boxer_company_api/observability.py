from __future__ import annotations

import json
import logging
from typing import Any

_LOGGER = logging.getLogger("boxer.company_api")
_ALLOWED_FIELDS = {
    "caller_id",
    "channel",
    "duration_ms",
    "fallback_reason",
    "outcome",
    "request_id",
    "route",
    "source_count",
    "status",
    "used_llm",
}


def emit_api_event(event: str, **fields: Any) -> None:
    """질문·근거·예외 원문 없이 API 처리 결과만 구조화해 남긴다."""

    safe_fields: dict[str, Any] = {}
    for key, value in fields.items():
        if (
            key not in _ALLOWED_FIELDS
            or not isinstance(value, (str, int, float, bool))
        ):
            continue
        # correlation/route code가 변조돼도 단일 로그 event 크기를 제한한다.
        safe_fields[key] = value[:256] if isinstance(value, str) else value
    payload = {
        "event": str(event),
        **safe_fields,
    }
    _LOGGER.info(
        json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
