"""실시간 대화 조회의 시간과 원문 및 전송 예산을 모든 경계에서 공유한다."""

import json
from collections.abc import Mapping, Sequence
from typing import Any

LIVE_QUERY_SECONDS = 60.0
LIVE_RETRIEVAL_LEASE_SECONDS = 120
LIVE_SOURCE_MAX_CHARS = 16_000
LIVE_TOTAL_MAX_CHARS = 80_000
LIVE_BUNDLE_MAX_BYTES = 1_048_576
LIVE_REQUEST_MAX_BYTES = 4_194_304
LIVE_CACHE_MAX_BYTES = 8_388_608
LIVE_RENDER_MAX_CHARS = 160_000
LIVE_RENDER_MAX_BYTES = 1_048_576


def live_bundle_size(sources: Sequence[Mapping[str, Any]],
                     context_messages: Sequence[Mapping[str, Any]] = ()) -> int:
    # 개수가 아닌 실제 원문과 메타데이터 크기로 전송 예산을 동일하게 계산한다.
    return len(json.dumps({"sources": list(sources), "contextMessages": list(context_messages)},
                          ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8"))
