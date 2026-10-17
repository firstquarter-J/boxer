"""명시적 운영자 CLI에서만 과거 대화를 검증하고 기존 추출 queue에 적재한다."""
from __future__ import annotations

from collections import Counter
from datetime import datetime
from decimal import Decimal
import json
import os
from pathlib import Path
import stat
from typing import Literal

from pydantic import Field, model_validator

from boxer_company.memory.contracts import MemoryError, canonical_digest
from boxer_company.memory.policy import normalize_source
from boxer_company.memory.store import MemoryStore
from boxer_company.protected_json import _validate_protected_parent_chain
from boxer_company_api.memory_schemas import Digest, Identifier, MemoryInput, MemorySourceInput, SlackTs


class HistoryPeriod(MemoryInput):
    fromKst: str = Field(max_length=64)
    throughKst: str = Field(max_length=64)
    oldestTs: SlackTs
    cutoffTs: SlackTs

    @model_validator(mode="after")
    def validate_window(self):
        start, end = datetime.fromisoformat(self.fromKst), datetime.fromisoformat(self.throughKst)
        if start.utcoffset() is None or end.utcoffset() is None or start >= end or (end - start).total_seconds() > 31 * 86400:
            raise ValueError("invalid import window")
        for value, stamp in ((start, self.oldestTs), (end, self.cutoffTs)):
            expected = Decimal(int(value.timestamp())) + Decimal(value.microsecond) / 1_000_000
            if value.utcoffset().total_seconds() != 9 * 3600 or expected != Decimal(stamp):
                raise ValueError("import timestamp mismatch")
        return self


class HistoryChunk(MemoryInput):
    chunkNo: int = Field(ge=0, le=9999)
    sources: list[MemorySourceInput] = Field(min_length=1, max_length=100)
    chunkDigest: Digest


class HistoryThread(MemoryInput):
    threadTs: SlackTs
    cutoffTs: SlackTs
    coverage: Literal["complete", "partial"]
    chunks: list[HistoryChunk] = Field(min_length=1, max_length=10000)


class HistoryImport(MemoryInput):
    schemaVersion: Literal[1]
    kind: Literal["slack_history_import"]
    importId: Identifier
    tenantId: Identifier
    channelId: Identifier
    channelKind: Literal["channel", "private_channel"]
    period: HistoryPeriod
    authorIds: list[Identifier] = Field(min_length=1, max_length=100)
    coverage: Literal["complete", "partial"]
    coverageNotes: list[str] = Field(default_factory=list, max_length=100)
    threads: list[HistoryThread] = Field(max_length=10000)
    inputDigest: Digest


def read_history_import(path: str | Path) -> dict:
    # 이 파일은 일시적으로 원문을 담으므로 symlink·hardlink·공유 쓰기 경로를 거부한다.
    target = Path(path)
    if not target.is_absolute() or target.resolve(strict=False) != target:
        raise MemoryError("import_path_invalid")
    _validate_protected_parent_chain(target, label="memory import")
    parent = target.parent.stat()
    if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) != 0o700:
        raise MemoryError("import_directory_unsafe")
    descriptor = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
        metadata = os.fstat(stream.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
                or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_nlink != 1
                or metadata.st_size > 16 * 1024 * 1024):
            raise MemoryError("import_file_unsafe")
        return json.load(stream)


def enqueue_history_import(store: MemoryStore, payload: dict, *, tenant_id: str,
                           channel_id: str, operator_id: str, authorized_author_ids: set[str]) -> dict:
    document = HistoryImport.model_validate(payload)
    if (document.tenantId != tenant_id or document.channelId != channel_id or not operator_id
            or not authorized_author_ids or not set(document.authorIds).issubset(authorized_author_ids)):
        raise MemoryError("import_scope_invalid")
    if canonical_digest(payload["threads"]) != document.inputDigest:
        raise MemoryError("import_manifest_invalid")
    # 전체 입력을 검증한 뒤 쓰고 재개 시에도 동일한 스레드 요청 ID를 사용한다.
    seen_threads, seen_messages = set(), set()
    count = 0
    for thread, raw_thread in zip(document.threads, payload["threads"], strict=True):
        if (thread.threadTs in seen_threads or thread.cutoffTs != document.period.cutoffTs
                or Decimal(thread.threadTs) > Decimal(thread.cutoffTs)):
            raise MemoryError("import_thread_invalid")
        seen_threads.add(thread.threadTs)
        for index, (chunk, raw_chunk) in enumerate(zip(thread.chunks, raw_thread["chunks"], strict=True)):
            if (chunk.chunkNo != index or canonical_digest(raw_chunk["sources"]) != chunk.chunkDigest
                    or sum(len(source.text) for source in chunk.sources) > 16000):
                raise MemoryError("import_chunk_invalid")
            for source, raw_source in zip(chunk.sources, raw_chunk["sources"], strict=True):
                count += 1
                if (source.authorKind != "human" or source.authorId not in document.authorIds
                        or source.messageTs in seen_messages or source.deliveryVerified
                        or source.createdAt != source.messageTs or source.segmentStart is not None or source.segmentEnd is not None
                        or (source.editedAt and Decimal(source.editedAt) > Decimal(thread.cutoffTs))
                        or not Decimal(document.period.oldestTs) <= Decimal(source.messageTs) <= Decimal(thread.cutoffTs)
                        or Decimal(source.messageTs) < Decimal(thread.threadTs)):
                    raise MemoryError("import_source_invalid")
                seen_messages.add(source.messageTs)
                if normalize_source(raw_source, tenant_id=tenant_id, channel_id=channel_id,
                                    thread_ts=thread.threadTs, current_message_ts=thread.cutoffTs) is None:
                    raise MemoryError("import_source_not_allowed")
    if count > 10000:
        raise MemoryError("import_source_limit")
    # 동일 import ID에 새 스레드를 덧붙이는 manifest 변경도 이력의 digest로 차단한다.
    store.bind_history_import(tenant_id=tenant_id, channel_id=channel_id, operator_id=operator_id,
        import_id=document.importId, manifest_digest=canonical_digest({"payload":payload,"operatorId":operator_id,"authorIds":sorted(authorized_author_ids)}))
    states = Counter()
    actual_coverage = document.coverage
    for thread, raw_thread in zip(document.threads, payload["threads"], strict=True):
        manifest = canonical_digest([chunk.chunkDigest for chunk in thread.chunks])
        try:
            capture = store.create_import_capture(tenant_id=tenant_id, operator_id=operator_id,
                channel_id=channel_id, thread_ts=thread.threadTs, cutoff_ts=thread.cutoffTs,
                channel_kind=document.channelKind, import_id=document.importId, input_digest=manifest)
        except MemoryError as exc:
            if exc.code != "memory_import_terminal":
                raise
            # 명시 삭제·실패된 capture는 살리지 않고 다른 스레드의 안전한 재개를 계속한다.
            states["terminal_skipped"] += 1
            actual_coverage = "partial"
            continue
        if capture["state"] == "collecting":
            for chunk in raw_thread["chunks"]:
                capture = store.put_chunk(tenant_id=tenant_id, actor_id=operator_id,
                    capture_id=capture["captureId"], claim_token=capture["claimToken"],
                    chunk_no=chunk["chunkNo"], sources=chunk["sources"], coverage=thread.coverage,
                    is_last=chunk["chunkNo"] == len(thread.chunks) - 1,
                    total_chunks=len(thread.chunks), input_digest=manifest)
        states[capture["state"]] += 1
        if capture.get("checkpoint", {}).get("coverage") != "complete":
            actual_coverage = "partial"
    return {"importId": document.importId, "threads": len(document.threads), "sources": count,
            "authorCount": len({source.authorId for thread in document.threads for chunk in thread.chunks for source in chunk.sources}),
            "coverage": actual_coverage, "states": dict(states)}
