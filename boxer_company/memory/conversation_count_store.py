"""새 수집이나 저장 없이 기존 기억 DB의 발췌만 메시지 단위로 집계한다."""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .access import is_private_memory_hub
from .contracts import MemoryError
from .conversation_count import PROFANITY_CLASSIFIER_VERSION, classify_profanity_message
from .live_query import is_supported_stored_period

if TYPE_CHECKING:
    from .store import MemoryStore

_IDENTIFIER = re.compile(r"[A-Z0-9_]{1,128}")
_STAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_CLASSIFICATION_ORDER = {None: 0, "not_matched": 1, "ambiguous": 2, "matched": 3}


def _stamp(value: Any) -> Decimal:
    if not isinstance(value, str) or not _STAMP.fullmatch(value):
        raise MemoryError("conversation_count_timestamp_invalid")
    result = Decimal(value)
    if not 0 <= result < 2**63:
        raise MemoryError("conversation_count_timestamp_invalid")
    return result


def count_stored_messages(store: MemoryStore, *, tenant_id: str, channel_id: str,
                          author_id: str, period: Mapping[str, str]) -> dict[str, Any]:
    """판정 단위는 저장 발췌를 가진 메시지다. Slack 전체 발언 횟수로 해석하지 않는다."""
    if any(not isinstance(value, str) or not _IDENTIFIER.fullmatch(value)
           for value in (tenant_id, channel_id, author_id)):
        raise MemoryError("conversation_count_scope_invalid")
    if not isinstance(period, Mapping) or not is_supported_stored_period(period):
        raise MemoryError("conversation_count_period_unsupported")
    start, end, cutoff = (_stamp(period.get(key)) for key in ("startTs", "endTs", "cutoffTs"))
    if not start < end or cutoff < start:
        raise MemoryError("conversation_count_period_invalid")

    hub = is_private_memory_hub(tenant_id, channel_id)
    with store._connection() as connection:
        # 출처와 삭제 이력은 동일 snapshot에서 읽고, 분류 전에 DB 읽기를 마친다.
        connection.execute("BEGIN")
        events = connection.execute(
            "SELECT scope_channel_id,source_id,source_message_id,deletion_kind FROM memory_events "
            "WHERE tenant_id=? AND (scope_channel_id=? OR ?) "
            "AND deletion_kind IN ('source','source_redaction','source_revision')",
            (tenant_id, channel_id, hub),
        ).fetchall()
        rows = connection.execute(
            "SELECT s.channel_id,s.message_ts,s.edited_at,s.id,s.revision_hash,s.excerpt "
            "FROM memory_sources s JOIN memory_threads t "
            "ON t.tenant_id=s.tenant_id AND t.id=s.thread_id AND t.channel_id=s.channel_id "
            "WHERE s.tenant_id=? AND s.author_id=? AND s.author_kind='human' AND s.state='current' "
            "AND (s.channel_id=? OR (? AND t.channel_kind='private_channel'))",
            (tenant_id, author_id, channel_id, hub),
        ).fetchall()
        connection.execute("COMMIT")

    deleted = {(row["scope_channel_id"], _stamp(row["source_message_id"])) for row in events
               if row["deletion_kind"] in {"source", "source_redaction"} and row["source_message_id"]}
    revisions = {(row["scope_channel_id"], row["source_id"]) for row in events
                 if row["deletion_kind"] == "source_revision"}
    messages: dict[tuple[str, Decimal], tuple[str, str | None, frozenset[str]]] = {}
    for row in rows:
        stamp = _stamp(row["message_ts"])
        key = row["channel_id"], stamp
        if (not start <= stamp < end or stamp > cutoff or key in deleted
                or (row["channel_id"], row["id"]) in revisions
                or row["edited_at"] and _stamp(row["edited_at"]) > cutoff):
            continue
        revision = row["revision_hash"]
        previous = messages.get(key)
        # 같은 메시지가 여러 기억에 쓰여도 한 번만 센다. 충돌하는 current revision은 제외한다.
        if previous is not None and previous[0] != revision:
            messages[key] = ("", None, frozenset())
            continue
        excerpt = row["excerpt"]
        classified = classify_profanity_message(excerpt) if excerpt else None
        classification = classified["classification"] if classified is not None else None
        expressions = (frozenset(classified["matchedExpressions"])
                       if classified is not None and classification == "matched" else frozenset())
        if previous is not None:
            if _CLASSIFICATION_ORDER[classification] < _CLASSIFICATION_ORDER[previous[1]]:
                classification = previous[1]
            expressions |= previous[2]
        messages[key] = revision, classification, expressions

    matched = sum(classification == "matched" for _, classification, _ in messages.values())
    ambiguous = sum(classification == "ambiguous" for _, classification, _ in messages.values())
    unavailable = sum(classification is None for _, classification, _ in messages.values())
    # 최종 유효 메시지에 남은 안전 표기만 합친다. 뒤늦게 충돌하거나 제외된 출처의 표현은 남기지 않는다.
    matched_expressions = sorted({expression for _, classification, values in messages.values()
                                  if classification == "matched" for expression in values})
    return {"status": "stored", "scope": "stored_excerpts", "classifierVersion": PROFANITY_CLASSIFIER_VERSION,
            "period": dict(period), "matchedMessageCount": matched, "ambiguousMessageCount": ambiguous,
            "matchedExpressions": matched_expressions,
            "scannedMessageCount": len(messages) - unavailable, "unavailableExcerptMessageCount": unavailable,
            "eligibleStoredMessageCount": len(messages), "channelCount": len({key[0] for key in messages})}
