"""허용된 실시간 Slack 원문을 DB 기록 없이 검증된 참고 데이터로 바꾼다."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re
from typing import Any, Mapping, Sequence

from .access import is_private_memory_hub
from .contracts import MemoryError, canonical_digest
from .live_query import is_within_live_query_period, resolve_live_query_period
from .policy import normalize_source


_TIMESTAMP = re.compile(r"[0-9]{1,12}\.[0-9]{1,6}")
_REQUIRED_FIELDS = frozenset({
    "channelId", "channelKind", "threadTs", "messageTs", "revisionHash", "authorId", "text", "permalink",
})


def _timestamp_iso(value: str) -> str:
    if not isinstance(value, str) or _TIMESTAMP.fullmatch(value) is None:
        raise MemoryError("memory_live_timestamp_invalid")
    seconds, fraction = value.split(".")
    try:
        instant = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
            seconds=int(seconds), microseconds=int(fraction.ljust(6, "0")),
        )
    except (ValueError, OverflowError) as exc:
        raise MemoryError("memory_live_timestamp_invalid") from exc
    return instant.isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize_live_sources(
    sources: Sequence[Mapping[str, Any]], *, tenant_id: str, destination_channel_id: str,
    before_ts: str, author_ids: Sequence[str], question: str = "",
) -> tuple[dict[str, Any], ...]:
    """고정 허브의 비공개 채널 원문만 현재 질문 시점과 작성자 범위 안에서 받는다."""
    if (not isinstance(tenant_id, str) or not isinstance(destination_channel_id, str)
            or not is_private_memory_hub(tenant_id, destination_channel_id)):
        raise MemoryError("memory_live_destination_forbidden")
    _timestamp_iso(before_ts)
    period = resolve_live_query_period(question, before_ts)
    if (not isinstance(sources, Sequence) or isinstance(sources, (str, bytes))
            or len(sources) > 8):
        raise MemoryError("memory_live_sources_invalid")
    if (not isinstance(author_ids, Sequence) or isinstance(author_ids, (str, bytes))
            or any(not isinstance(author, str) or not author or len(author) > 128 for author in author_ids)):
        raise MemoryError("memory_live_authors_invalid")
    allowed_authors = frozenset(author_ids)
    total_chars = 0
    normalized: dict[str, dict[str, Any]] = {}
    seen_messages: dict[tuple[str, str], str] = {}
    for source in sources:
        if (not isinstance(source, Mapping) or not _REQUIRED_FIELDS <= source.keys()
                or any(not isinstance(source[field], str) for field in _REQUIRED_FIELDS)
                or any(not source[field] for field in _REQUIRED_FIELDS - {"text"})
                or not isinstance(source.get("editedAt", ""), str)):
            raise MemoryError("memory_live_source_invalid")
        # tenant·출처 종류는 adapter의 실제 조회 결과와 고정 허브 정책 양쪽에서 확인한다.
        if (("tenantId" in source and source["tenantId"] != tenant_id)
                or source["channelKind"] != "private_channel"):
            raise MemoryError("memory_live_source_scope_invalid")
        _timestamp_iso(source["threadTs"])
        updated_at = _timestamp_iso(source["messageTs"])
        if source.get("editedAt"):
            _timestamp_iso(source["editedAt"])
            # 발언 자체가 오래됐어도 질문 이후 편집된 본문을 당시 원문으로 제시하지 않는다.
            if Decimal(source["editedAt"]) > Decimal(before_ts):
                raise MemoryError("source_edited_after_capture_cutoff")
        if allowed_authors and source["authorId"] not in allowed_authors:
            raise MemoryError("memory_live_source_author_invalid")
        total_chars += len(source["text"])
        if total_chars > 8000:
            raise MemoryError("memory_live_sources_too_large")
        # sourceId·작성자 종류는 입력값을 채택하지 않고 기존 원문 검증으로 다시 계산한다.
        accepted = normalize_source(
            {**source, "authorKind": "human"}, tenant_id=tenant_id, channel_id=source["channelId"],
            thread_ts=source["threadTs"], current_message_ts=before_ts,
        )
        if accepted is None:
            continue
        # adapter의 기간 필터를 신뢰해 생략하지 않고 API의 최종 원문 경계에서도 강제한다.
        if period is not None and not is_within_live_query_period(accepted["messageTs"], period):
            continue
        key = (source["channelId"], accepted["messageTs"])
        if key in seen_messages and seen_messages[key] != accepted["revisionHash"]:
            raise MemoryError("memory_live_source_revision_conflict")
        seen_messages[key] = accepted["revisionHash"]
        reference = {
            "sourceId": accepted["sourceId"], "channelId": source["channelId"],
            "channelKind": "private_channel", "threadTs": source["threadTs"],
            "messageTs": accepted["messageTs"], "revisionHash": accepted["revisionHash"],
            "permalink": accepted["permalink"], "authorId": accepted["authorId"], "authorKind": "human",
            "state": "current", "relation": "supports",
        }
        item = {
            "memoryId": "live_" + accepted["sourceId"], "kind": "utterance", "category": "statement",
            "topicKey": "live_slack_utterance", "evidenceType": "direct_statement", "status": "active",
            "version": 1, "subjectUserId": accepted["authorId"], "text": accepted["text"][:1000],
            "excerptOnly": len(accepted["text"]) > 1000, "liveSource": True, "updatedAt": updated_at,
            "threadTs": source["threadTs"],
            "threadId": "live_thr_" + canonical_digest([tenant_id, source["channelId"], source["threadTs"]]),
            "deletionEpoch": 0, "sources": [reference],
        }
        previous = normalized.get(accepted["sourceId"])
        if previous is not None and previous != item:
            raise MemoryError("memory_live_source_conflict")
        normalized[accepted["sourceId"]] = item
    return tuple(sorted(normalized.values(), key=lambda item: (
        Decimal(item["sources"][0]["messageTs"]), item["sources"][0]["sourceId"],
    ), reverse=True))
