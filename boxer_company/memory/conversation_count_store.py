"""새 수집이나 저장 없이 기존 기억 DB의 발췌만 메시지 단위로 집계한다."""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from .access import can_read_memory_source, is_private_memory_hub
from .contracts import MemoryError, canonical_digest, source_revision
from .conversation_count import PROFANITY_CLASSIFIER_VERSION, classify_profanity_message
from .live_query import is_supported_stored_period
from .policy import is_safe_memory_text, normalize_topic_key

if TYPE_CHECKING:
    from sqlite3 import Row

    from .store import MemoryStore

_IDENTIFIER = re.compile(r"[A-Z0-9_]{1,128}")
_STAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_CLASSIFICATION_ORDER = {None: 0, "not_matched": 1, "ambiguous": 2, "matched": 3}
_MESSAGE_LIMIT = 20


def _stamp(value: Any) -> Decimal:
    if not isinstance(value, str) or not _STAMP.fullmatch(value):
        raise MemoryError("conversation_count_timestamp_invalid")
    result = Decimal(value)
    if not 0 <= result < 2**63:
        raise MemoryError("conversation_count_timestamp_invalid")
    return result


def _message_topics(rows: list[Row], current: list[Row], events: list[Row], *, tenant_id: str,
                    channel_id: str, start: Decimal, end: Decimal, cutoff: Decimal,
                    deleted: set[tuple[str, Decimal]], revisions: set[tuple[str, str]]) -> dict[str, set[str]]:
    """같은 snapshot의 모든 연결 출처가 유효할 때 실제 욕 발췌에 연결된 주제만 남긴다."""
    current_revisions: dict[tuple[str, Decimal], set[str]] = {}
    for source in current:
        try:
            stamp = _stamp(source["message_ts"])
            key = source["channel_id"], stamp
            if (not start <= stamp < end or stamp > cutoff or key in deleted
                    or (source["channel_id"], source["id"]) in revisions
                    or source["edited_at"] and _stamp(source["edited_at"]) > cutoff):
                continue
        except MemoryError:
            continue
        current_revisions.setdefault(key, set()).add(source["revision_hash"])
    removed_items = {(event["scope_channel_id"], event["memory_id"]) for event in events
                     if event["deletion_kind"] == "item"}
    removed_topics = {(event["scope_channel_id"], event["subject_id"] or "", event["topic_key_hash"])
                      for event in events if event["deletion_kind"] == "subject_topic"}
    grouped: dict[str, list[Row]] = {}
    for row in rows:
        grouped.setdefault(row["memory_id"], []).append(row)
    topics: dict[str, set[str]] = {}
    for references in grouped.values():
        item = references[0]
        topic = item["topic_key"]
        if (not is_safe_memory_text(topic) or len(topic) > 160
                or (item["scope_channel_id"], item["memory_id"]) in removed_items
                or any((item["scope_channel_id"], item["subject_user_id"] or "", topic_hash) in removed_topics
                       for topic_hash in (canonical_digest(topic.strip().casefold()),
                                          canonical_digest(normalize_topic_key(topic))))):
            continue
        valid = True
        matched_sources = set()
        for source in references:
            # 한 출처만 유효해도 주제를 되살리지 않는다. 다른 연결 출처의 삭제와 충돌도 차단한다.
            if (source["source_tenant"] != tenant_id or source["state"] != "current"
                    or source["author_kind"] != "human" or not source["channel_kind"]
                    or not can_read_memory_source(tenant_id, channel_id, source["channel_id"], source["channel_kind"])):
                valid = False
                break
            try:
                stamp = _stamp(source["message_ts"])
                key = source["channel_id"], stamp
                if (not start <= stamp < end or stamp > cutoff or key in deleted
                        or (source["channel_id"], source["source_id"]) in revisions
                        or current_revisions.get(key) != {source["revision_hash"]}
                        or source["edited_at"] and _stamp(source["edited_at"]) > cutoff):
                    valid = False
                    break
            except MemoryError:
                valid = False
                break
            span, excerpt = source["evidence_span"], source["excerpt"]
            if (not span or not excerpt or span not in excerpt
                    or not is_safe_memory_text(span) or not is_safe_memory_text(excerpt)):
                valid = False
                break
            if classify_profanity_message(span)["classification"] == "matched":
                matched_sources.add(source["source_id"])
        if valid:
            for source_id in matched_sources:
                topics.setdefault(source_id, set()).add(topic)
    return topics


def count_stored_messages(store: MemoryStore, *, tenant_id: str, channel_id: str,
                          author_id: str, period: Mapping[str, str], include_messages: bool = False) -> dict[str, Any]:
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
        event_kinds = "'source','source_redaction','source_revision'"
        if include_messages:
            event_kinds += ",'item','subject_topic'"
        events = connection.execute(
            "SELECT scope_channel_id,source_id,source_message_id,deletion_kind,memory_id,subject_id,topic_key_hash "
            "FROM memory_events "
            "WHERE tenant_id=? AND (scope_channel_id=? OR ?) "
            f"AND deletion_kind IN ({event_kinds})",
            (tenant_id, channel_id, hub),
        ).fetchall()
        rows = connection.execute(
            "SELECT s.channel_id,s.message_ts,s.edited_at,s.id,s.revision_hash,s.excerpt,raw.text AS raw_text "
            "FROM memory_sources s JOIN memory_threads t "
            "ON t.tenant_id=s.tenant_id AND t.id=s.thread_id AND t.channel_id=s.channel_id "
            "LEFT JOIN memory_source_texts raw ON raw.tenant_id=s.tenant_id AND raw.source_id=s.id "
            "AND EXISTS (SELECT 1 FROM memory_raw_imports archive WHERE archive.tenant_id=raw.tenant_id "
            "AND archive.id=raw.import_id AND archive.state='completed' AND archive.channel_id=s.channel_id "
            "AND archive.author_id=s.author_id) "
            "WHERE s.tenant_id=? AND s.author_id=? AND s.author_kind='human' AND s.state='current' "
            "AND (s.channel_id=? OR (? AND t.channel_kind='private_channel'))",
            (tenant_id, author_id, channel_id, hub),
        ).fetchall()
        topic_rows, current = [], []
        if include_messages:
            # 요약 text는 읽지 않는다. 후보 기억의 모든 연결 출처를 LEFT JOIN해 누락도 실패로 처리한다.
            topic_rows = connection.execute(
                "SELECT i.id AS memory_id,i.scope_channel_id,i.subject_user_id,i.topic_key,"
                "r.source_id,r.evidence_span,s.tenant_id AS source_tenant,s.channel_id,s.message_ts,"
                "s.edited_at,s.revision_hash,s.excerpt,s.state,s.author_kind,t.channel_kind "
                "FROM memory_items i JOIN memory_threads origin "
                "ON origin.tenant_id=i.tenant_id AND origin.id=i.origin_thread_id "
                "AND origin.channel_id=i.scope_channel_id "
                "JOIN memory_item_sources r ON r.tenant_id=i.tenant_id AND r.memory_id=i.id "
                "LEFT JOIN memory_sources s ON s.tenant_id=r.tenant_id AND s.id=r.source_id "
                "LEFT JOIN memory_threads t ON t.tenant_id=s.tenant_id AND t.id=s.thread_id AND t.channel_id=s.channel_id "
                "WHERE i.tenant_id=? AND (i.scope_channel_id=? OR (? AND origin.channel_kind='private_channel')) "
                "AND i.status IN ('active','resolved') AND (i.valid_until IS NULL OR i.valid_until>?) "
                "AND EXISTS (SELECT 1 FROM memory_item_sources target_r JOIN memory_sources target_s "
                "ON target_s.tenant_id=target_r.tenant_id AND target_s.id=target_r.source_id "
                "WHERE target_r.tenant_id=i.tenant_id AND target_r.memory_id=i.id AND target_s.author_id=?)",
                (tenant_id, channel_id, hub, store._clock(), author_id),
            ).fetchall()
            current = connection.execute(
                "SELECT s.channel_id,s.message_ts,s.revision_hash,s.id,s.edited_at "
                "FROM memory_sources s JOIN memory_threads t "
                "ON t.tenant_id=s.tenant_id AND t.id=s.thread_id AND t.channel_id=s.channel_id "
                "WHERE s.tenant_id=? AND s.state='current' "
                "AND (s.channel_id=? OR (? AND t.channel_kind='private_channel'))",
                (tenant_id, channel_id, hub),
            ).fetchall()
        connection.execute("COMMIT")

    deleted = {(row["scope_channel_id"], _stamp(row["source_message_id"])) for row in events
               if row["deletion_kind"] in {"source", "source_redaction"} and row["source_message_id"]}
    revisions = {(row["scope_channel_id"], row["source_id"]) for row in events
                 if row["deletion_kind"] == "source_revision"}
    messages: dict[tuple[str, Decimal], tuple[str, str | None, frozenset[str]]] = {}
    raw_messages: set[tuple[str, Decimal]] = set()
    message_rows: dict[tuple[str, Decimal], list[Row]] = {}
    for row in rows:
        row = dict(row)
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
            raw_messages.discard(key)
            continue
        # 승인된 원문은 요약 발췌와 별도로 읽고 현재 revision의 실제 본문인지 다시 대조한다.
        raw = row.pop("raw_text")
        if raw is not None:
            if (len(raw) <= 40000 and is_safe_memory_text(raw)
                    and source_revision(raw, row["edited_at"]) == revision):
                row["excerpt"] = raw
                row["stored_raw"] = True
                raw_messages.add(key)
            else:
                row["excerpt"] = ""
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
        if include_messages:
            message_rows.setdefault(key, []).append(row)

    matched = sum(classification == "matched" for _, classification, _ in messages.values())
    ambiguous = sum(classification == "ambiguous" for _, classification, _ in messages.values())
    unavailable = sum(classification is None for _, classification, _ in messages.values())
    # 최종 유효 메시지에 남은 안전 표기만 합친다. 뒤늦게 충돌하거나 제외된 출처의 표현은 남기지 않는다.
    matched_expressions = sorted({expression for _, classification, values in messages.values()
                                  if classification == "matched" for expression in values})
    result = {"status": "stored", "scope": "stored_excerpts", "classifierVersion": PROFANITY_CLASSIFIER_VERSION,
              "period": dict(period), "matchedMessageCount": matched, "ambiguousMessageCount": ambiguous,
              "matchedExpressions": matched_expressions,
              "scannedMessageCount": len(messages) - unavailable, "unavailableExcerptMessageCount": unavailable,
              "eligibleStoredMessageCount": len(messages), "channelCount": len({key[0] for key in messages})}
    if raw_messages:
        result["storedRawMessageCount"] = len(raw_messages)
    if include_messages:
        topics = _message_topics(topic_rows, current, events, tenant_id=tenant_id, channel_id=channel_id,
                                 start=start, end=end, cutoff=cutoff, deleted=deleted, revisions=revisions)
        details = []
        # 최종 집계에서 살아남은 메시지만 시간, 채널, 출처 ID 순서로 고정한다.
        for key in sorted(message_rows, key=lambda key: (key[1], key[0], min(row["id"] for row in message_rows[key]))):
            if messages[key][1] != "matched":
                continue
            candidates = []
            for row in sorted(message_rows[key], key=lambda row: row["id"]):
                if not is_safe_memory_text(row["excerpt"]):
                    continue
                limit = 40000 if row.get("stored_raw") else 1000
                classified = classify_profanity_message(row["excerpt"][:limit])
                # 요약 발췌의 기존 상한은 유지하고 명시 보존 원문만 전체 본문을 검사한다.
                if (classified["classification"] == "matched"
                        and frozenset(classified["matchedExpressions"]) == messages[key][2]):
                    candidates.append(row)
            if not candidates:
                continue
            source = candidates[0]
            candidate_topics = {topic for row in candidates for topic in topics.get(row["id"], set())}
            limit = 40000 if source.get("stored_raw") else 1000
            details.append({"messageTs": source["message_ts"], "excerpt": source["excerpt"][:limit],
                            "matchedExpressions": sorted(messages[key][2]),
                            "topic": next(iter(candidate_topics)) if len(candidate_topics) == 1 else None})
            if len(details) == _MESSAGE_LIMIT:
                break
        # 위험 발췌 생략도 상한 초과와 같이 표시하되 기존 집계값은 바꾸지 않는다.
        result.update(matchedMessages=details, messageLimit=_MESSAGE_LIMIT, messagesTruncated=matched > len(details))
    return result
