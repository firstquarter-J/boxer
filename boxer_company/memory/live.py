"""허용된 실시간 Slack 원문을 DB 기록 없이 검증된 참고 데이터로 바꾼다."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re
from typing import Any, Mapping, Sequence

from .access import can_query_private_live_sources, can_read_memory_source
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
    destination_channel_kind: str | None = None,
    context_messages: Sequence[Mapping[str, Any]] = (),
) -> tuple[dict[str, Any], ...]:
    """비공개 원채널 또는 고정 허브의 원문을 현재 질문·작성자 범위 안에서 받는다."""
    if not can_query_private_live_sources(tenant_id, destination_channel_id, destination_channel_kind):
        raise MemoryError("memory_live_destination_forbidden")
    _timestamp_iso(before_ts)
    period = resolve_live_query_period(question, before_ts)
    if (not isinstance(sources, Sequence) or isinstance(sources, (str, bytes))
            or len(sources) > 8):
        raise MemoryError("memory_live_sources_invalid")
    if (not isinstance(context_messages, Sequence) or isinstance(context_messages, (str, bytes))
            or len(sources) + len(context_messages) > 12):
        raise MemoryError("memory_live_contexts_invalid")
    if (not isinstance(author_ids, Sequence) or isinstance(author_ids, (str, bytes))
            or any(not isinstance(author, str) or not author or len(author) > 128 for author in author_ids)):
        raise MemoryError("memory_live_authors_invalid")
    allowed_authors = frozenset(author_ids)
    total_chars = 0
    context_counts: dict[tuple[str, str], int] = {}
    # 문맥도 전체 원문 예산에 포함한다. 작성자·날짜 예외는 아래의 연결 검증 뒤에만 적용한다.
    for context in context_messages:
        if (not isinstance(context, Mapping) or not _REQUIRED_FIELDS <= context.keys()
                or any(not isinstance(context[field], str) for field in _REQUIRED_FIELDS)
                or any(not context[field] for field in _REQUIRED_FIELDS - {"text"})
                or not isinstance(context.get("editedAt", ""), str)
                or not isinstance(context.get("anchorMessageTs"), str)
                or not isinstance(context.get("relation"), str)
                or context.get("relation") not in {"thread_parent", "thread_neighbor", "channel_neighbor"}):
            raise MemoryError("memory_live_context_invalid")
        _timestamp_iso(context["anchorMessageTs"])
        key = context["channelId"], context["anchorMessageTs"]
        context_counts[key] = context_counts.get(key, 0) + 1
        if context_counts[key] > 5:
            raise MemoryError("memory_live_contexts_too_many")
        total_chars += len(context["text"])
    if total_chars > 8000:
        raise MemoryError("memory_live_sources_too_large")
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
                or source["channelKind"] != "private_channel"
                or not can_read_memory_source(tenant_id, destination_channel_id,
                    source["channelId"], source["channelKind"])):
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
            "contextMessages": [], "contextCoverage": "unavailable",
        }
        previous = normalized.get(accepted["sourceId"])
        if previous is not None and previous != item:
            raise MemoryError("memory_live_source_conflict")
        normalized[accepted["sourceId"]] = item
    # 대상 발언이 안전·작성자·기간 검증을 통과한 뒤에만 다른 사람/다른 날짜의 문맥을 붙인다.
    anchors = {(item["sources"][0]["channelId"], item["sources"][0]["messageTs"]): item
               for item in normalized.values()}
    identities = {(source["channelId"], source["messageTs"]): (
        source["revisionHash"], source["authorId"], source["threadTs"])
        for item in normalized.values() for source in item["sources"]}
    contexts: dict[tuple[str, str], dict[str, Any]] = {}
    for context in context_messages:
        anchor = anchors.get((context["channelId"], context["anchorMessageTs"]))
        if anchor is None:
            raise MemoryError("memory_live_context_anchor_invalid")
        anchor_source = anchor["sources"][0]
        if (("tenantId" in context and context["tenantId"] != tenant_id)
                or context["channelKind"] != "private_channel"
                or not can_read_memory_source(tenant_id, destination_channel_id,
                    context["channelId"], context["channelKind"])):
            raise MemoryError("memory_live_source_scope_invalid")
        _timestamp_iso(context["threadTs"])
        _timestamp_iso(context["messageTs"])
        relation = context["relation"]
        if ((relation in {"thread_parent", "thread_neighbor"}
                and context["threadTs"] != anchor_source["threadTs"])
                or (relation == "thread_parent" and context["messageTs"] != context["threadTs"])
                or (relation == "channel_neighbor" and abs(
                    Decimal(context["messageTs"]) - Decimal(anchor_source["messageTs"])) > 900)):
            raise MemoryError("memory_live_context_relation_invalid")
        if context.get("editedAt"):
            _timestamp_iso(context["editedAt"])
            if Decimal(context["editedAt"]) > Decimal(before_ts):
                raise MemoryError("source_edited_after_capture_cutoff")
        # 나중에 민감 본문으로 수정된 중복도 앞서 읽은 안전한 옛 본문을 되살리지 못하게 한다.
        key = context["channelId"], context["messageTs"]
        identity = context["revisionHash"], context["authorId"], context["threadTs"]
        previous = identities.get(key)
        if previous is not None and previous != identity:
            code = "memory_live_source_revision_conflict" if previous[0] != identity[0] else "memory_live_source_conflict"
            raise MemoryError(code)
        identities[key] = identity
        # 작성자는 원문의 실제 사람 ID를 유지한다. 날짜 예외가 cutoff·revision·안전 검사를 생략하지 않는다.
        accepted = normalize_source({**context, "authorKind": "human"}, tenant_id=tenant_id,
            channel_id=context["channelId"], thread_ts=context["threadTs"], current_message_ts=before_ts)
        if accepted is None:
            continue
        if accepted["sourceId"] == anchor_source["sourceId"]:
            continue
        row = {"text": accepted["text"][:1000], "authorId": accepted["authorId"], "authorKind": "human",
            "sourceId": accepted["sourceId"], "relation": relation, "messageTs": accepted["messageTs"],
            "threadTs": context["threadTs"], "channelId": context["channelId"], "channelKind": "private_channel",
            "revisionHash": accepted["revisionHash"], "permalink": accepted["permalink"],
            "editedAt": context.get("editedAt", ""), "excerptOnly": len(accepted["text"]) > 1000,
            "anchorMessageTs": context["anchorMessageTs"]}
        context_key = anchor["memoryId"], accepted["sourceId"]
        if context_key in contexts and contexts[context_key] != row:
            raise MemoryError("memory_live_context_conflict")
        contexts[context_key] = row
    for (memory_id, _), row in sorted(contexts.items(), key=lambda entry: (
            Decimal(entry[1]["messageTs"]), entry[1]["sourceId"])):
        anchor = next(item for item in normalized.values() if item["memoryId"] == memory_id)
        anchor["contextMessages"].append(row)
        # 모든 문맥 출처도 기존 마지막 수정/삭제 검사의 대상이 되어 묶음 전체를 무효화한다.
        anchor["sources"].append({key: row[key] for key in (
            "sourceId", "channelId", "channelKind", "threadTs", "messageTs", "revisionHash",
            "permalink", "authorId", "authorKind")})
        anchor["sources"][-1].update(state="current", relation="context")
        anchor["contextCoverage"] = "partial"
    return tuple(sorted(normalized.values(), key=lambda item: (
        Decimal(item["sources"][0]["messageTs"]), item["sources"][0]["sourceId"],
    ), reverse=True))
