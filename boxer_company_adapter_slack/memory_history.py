"""명시적 과거 대화 수집: 메타데이터 조사와 본문 수집을 분리한다."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from decimal import Decimal
import fcntl
import json
import os
from pathlib import Path
import re
import stat
import time
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from boxer_company.memory.contracts import MAX_CHUNK_CHARS, canonical_digest, source_revision
from boxer_company.memory.policy import is_allowed_conversation_source
from boxer_company.protected_json import (
    _fsync_directory, _validate_protected_parent_chain, _write_protected_temporary,
    create_protected_json_file,
)
from boxer_company_adapter_slack.conversation_memory import _data, _safe_permalink


IMPORT_CHANNEL = "C0621TL2HSB"
_TS = re.compile(r"^[0-9]{10,16}\.[0-9]{6}$")
_MAX_BYTES = 16 * 1024 * 1024
_MAX_ROOTS = 10_000
_MAX_SOURCES = 10_000


class HistoryImportError(RuntimeError):
    """원문·credential 없이 고정 오류 코드만 전달한다."""


class HistoryRateLimited(HistoryImportError):
    def __init__(self, retry_after: int) -> None:
        self.retry_after = max(1, min(86400, retry_after))
        super().__init__("slack_rate_limited")


def _stamp(value: Any) -> str:
    if not isinstance(value, str) or not _TS.fullmatch(value):
        raise HistoryImportError("invalid_timestamp")
    return value


def period(from_kst: str, through_kst: str) -> dict[str, str]:
    """Asia/Seoul 기준의 고정 창을 Slack의 정밀 timestamp로 표현한다."""
    start, end = datetime.fromisoformat(from_kst), datetime.fromisoformat(through_kst)
    zone = ZoneInfo("Asia/Seoul")
    start = start.replace(tzinfo=zone) if start.tzinfo is None else start.astimezone(zone)
    end = end.replace(tzinfo=zone) if end.tzinfo is None else end.astimezone(zone)
    if start >= end or (end - start).days > 31:
        raise HistoryImportError("invalid_period")
    def timestamp(value: datetime) -> str:
        return f"{int(value.timestamp())}.{value.microsecond:06d}"
    return {"fromKst": start.isoformat(), "throughKst": end.isoformat(),
            "oldestTs": timestamp(start), "cutoffTs": timestamp(end)}


def _validate_path(path: Path) -> None:
    if not path.is_absolute() or path.resolve(strict=False) != path:
        raise HistoryImportError("unsafe_path")
    _validate_protected_parent_chain(path, label="memory history")
    metadata = path.parent.lstat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise HistoryImportError("unsafe_directory")


def _validate_file(metadata: os.stat_result) -> None:
    if (not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid()
            or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_nlink != 1
            or metadata.st_size > _MAX_BYTES):
        raise HistoryImportError("unsafe_file")


def _read(path: Path) -> dict[str, Any]:
    _validate_path(path)
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "r", encoding="utf-8") as stream:
        _validate_file(os.fstat(stream.fileno()))
        value = json.load(stream)
    if not isinstance(value, dict):
        raise HistoryImportError("invalid_state")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    _validate_path(path)
    raw = (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode()
    if len(raw) > _MAX_BYTES:
        raise HistoryImportError("state_too_large")
    temporary = _write_protected_temporary(path, raw)
    try:
        _validate_file(path.lstat())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


@contextmanager
def _locked(path: Path):
    _validate_path(path)
    descriptor = os.open(str(path) + ".lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
    try:
        _validate_file(os.fstat(descriptor))
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


class SlackHistoryCollector:
    def __init__(self, client: Any, *, state_path: str | Path, tenant_id: str,
                 author_ids: set[str], is_allowed: Callable[[str, str], bool],
                 window: Mapping[str, str] | None = None, all_history_roots: bool = False,
                 channel_id: str = IMPORT_CHANNEL, channel_kind: str = "channel") -> None:
        if channel_id != IMPORT_CHANNEL or channel_kind not in {"channel", "private_channel"} or not tenant_id or not author_ids:
            raise HistoryImportError("scope_invalid")
        self.client, self.path = client, Path(state_path).expanduser()
        self.is_allowed = is_allowed
        with _locked(self.path):
            if self.path.exists():
                state = _read(self.path)
                if (state.get("tenantId") != tenant_id or state.get("channelId") != channel_id
                        or set(state.get("authorIds", [])) != author_ids
                        or bool(state.get("allHistoryRoots")) != all_history_roots
                        or (window is not None and state.get("period") != dict(window))):
                    raise HistoryImportError("resume_scope_changed")
                if state.get("channelKind", channel_kind) != channel_kind:
                    raise HistoryImportError("channel_kind_changed")
                if "channelKind" not in state:
                    state["channelKind"] = channel_kind
                    _write(self.path, state)
            else:
                if window is None:
                    raise HistoryImportError("period_required")
                identity = {"tenantId": tenant_id, "channelId": channel_id,
                            "period": dict(window), "authorIds": sorted(author_ids)}
                state = {"schemaVersion": 1, **identity,
                         "importId": "imp_" + canonical_digest(identity),
                         "allHistoryRoots": all_history_roots, "channelKind": channel_kind,
                         "historyCursor": "", "historyDone": False, "roots": {},
                         "historyPages": 0, "historyMessages": 0, "replyPages": 0,
                         "excludedMessages": 0, "sourceCount": 0}
                create_protected_json_file(self.path, state, label="memory history")

    @contextmanager
    def _operation(self):
        with _locked(self.path):
            state = _read(self.path)
            remaining = float(state.get("retryNotBefore", 0)) - time.time()
            if remaining > 0:
                raise HistoryRateLimited(int(remaining) + 1)
            try:
                yield state
            except HistoryRateLimited as exc:
                # 자동 재시도하지 않고 서버의 대기 시각을 재시작 이후에도 유지한다.
                state["retryNotBefore"] = time.time() + exc.retry_after
                _write(self.path, state)
                raise

    def _call(self, method: str, **kwargs: Any) -> Mapping[str, Any]:
        try:
            response = getattr(self.client, method)(**kwargs)
        except Exception as exc:
            response = getattr(exc, "response", None)
            if _data(response).get("error") == "ratelimited" or getattr(response, "status_code", None) == 429:
                headers = getattr(response, "headers", {}) or {}
                raw = headers.get("Retry-After") or headers.get("retry-after") or "60"
                try:
                    retry = int(raw)
                except (ValueError, TypeError):
                    retry = 60
                raise HistoryRateLimited(retry) from None
            if method == "conversations_replies" and _data(response).get("error") == "thread_not_found":
                return {"ok": False, "error": "thread_not_found"}
            raise
        result = _data(response)
        if result.get("error") == "ratelimited":
            raise HistoryRateLimited(60)
        return result

    def scan(self, *, max_pages: int = 5) -> dict[str, Any]:
        """history 응답의 text는 저장하지 않고 수집 대상 root metadata만 남긴다."""
        with self._operation() as state:
            for _ in range(max(1, min(1000, max_pages))):
                if state["historyDone"]:
                    break
                window = state["period"]
                response = self._call("conversations_history",
                    channel=state["channelId"], latest=window["cutoffTs"],
                    oldest="0" if state["allHistoryRoots"] else window["oldestTs"],
                    # 내부 앱의 history 공식 최대치(999)로 메타데이터 왕복을 줄인다.
                    inclusive=True, limit=999, cursor=state["historyCursor"] or None)
                self._check(response)
                for message in response.get("messages") or []:
                    if not isinstance(message, Mapping):
                        continue
                    ts = _stamp(message.get("ts"))
                    if Decimal(ts) > Decimal(window["cutoffTs"]):
                        continue
                    thread = _stamp(message.get("thread_ts") or ts)
                    replies = int(message.get("reply_count") or 0)
                    latest = str(message.get("latest_reply") or "")
                    recent = Decimal(ts) >= Decimal(window["oldestTs"])
                    possible_reply = replies > 0 and (not _TS.fullmatch(latest) or Decimal(latest) >= Decimal(window["oldestTs"]))
                    allowed_root = (message.get("user") in state["authorIds"] and not message.get("bot_id")
                                    and not message.get("subtype"))
                    if (recent and allowed_root) or possible_reply or (recent and thread != ts):
                        if thread not in state["roots"]:
                            if len(state["roots"]) >= _MAX_ROOTS:
                                raise HistoryImportError("too_many_roots")
                            state["roots"][thread] = {"threadTs": thread, "replyCount": replies,
                                "latestReplyTs": latest, "cursor": "", "done": False,
                                "coverage": "complete", "contextGap": False, "sources": {}, "excludedTs": []}
                    state["historyMessages"] += 1
                self._advance(state, response, cursor_key="historyCursor", done_key="historyDone")
                state["historyPages"] += 1
                _write(self.path, state)
            return self._summary(state)

    def collect(self, *, max_pages: int = 5) -> dict[str, Any]:
        """조사가 끝난 뒤에만 본문을 읽고 매 페이지를 원자 checkpoint한다."""
        with self._operation() as state:
            if not state["historyDone"]:
                raise HistoryImportError("metadata_scan_incomplete")
            for _ in range(max(1, min(1000, max_pages))):
                unfinished = next((row for _, row in sorted(state["roots"].items(), key=lambda pair: Decimal(pair[0])) if not row["done"]), None)
                if unfinished is None:
                    break
                window = state["period"]
                response = self._call("conversations_replies",
                    channel=state["channelId"], ts=unfinished["threadTs"],
                    oldest=window["oldestTs"], latest=window["cutoffTs"],
                    inclusive=True, limit=100, cursor=unfinished["cursor"] or None)
                if response.get("error") == "thread_not_found":
                    # 조사 뒤 삭제된 root는 오래된 본문 없이 누락을 명시하고 다음 대상으로 간다.
                    state["sourceCount"] -= len(unfinished["sources"])
                    unfinished.update({"done": True, "coverage": "partial", "sources": {}, "unavailable": True})
                    state["replyPages"] += 1
                    _write(self.path, state)
                    continue
                self._check(response)
                for message in response.get("messages") or []:
                    if not isinstance(message, Mapping):
                        continue
                    ts = _stamp(message.get("ts"))
                    if not Decimal(window["oldestTs"]) <= Decimal(ts) <= Decimal(window["cutoffTs"]):
                        continue
                    if ts in unfinished["excludedTs"]:
                        continue
                    source = None if unfinished["contextGap"] else self._source(state, message)
                    if source is None or unfinished["contextGap"]:
                        unfinished["coverage"], unfinished["contextGap"] = "partial", True
                        unfinished["excludedTs"].append(ts)
                        state["excludedMessages"] += 1
                        if unfinished["sources"].pop(ts, None) is not None:
                            state["sourceCount"] -= 1
                        continue
                    previous = unfinished["sources"].get(ts)
                    if previous is not None and previous != source:
                        # 재시작 중 바뀐 원문은 이전 revision을 섞지 않고 제외한다.
                        unfinished["sources"].pop(ts)
                        unfinished["excludedTs"].append(ts)
                        unfinished["coverage"], unfinished["contextGap"] = "partial", True
                        state["sourceCount"] -= 1
                        state["excludedMessages"] += 1
                        continue
                    if previous is None:
                        if state["sourceCount"] >= _MAX_SOURCES:
                            raise HistoryImportError("too_many_sources")
                        unfinished["sources"][ts] = source
                        state["sourceCount"] += 1
                self._advance(unfinished, response, cursor_key="cursor", done_key="done")
                state["replyPages"] += 1
                _write(self.path, state)
            return self._summary(state)

    def export(self, output_path: str | Path) -> dict[str, Any]:
        with _locked(self.path):
            state = _read(self.path)
            if not state["historyDone"] or any(not row["done"] for row in state["roots"].values()):
                raise HistoryImportError("collection_incomplete")
            notes = [] if state["allHistoryRoots"] else ["older_thread_roots_not_scanned"]
            if state["excludedMessages"]:
                notes.append("unsafe_or_context_dependent_sources_excluded")
            if any(row.get("unavailable") for row in state["roots"].values()):
                notes.append("unavailable_threads_excluded")
            threads = []
            for ts, row in sorted(state["roots"].items(), key=lambda pair: Decimal(pair[0])):
                sources = sorted(row["sources"].values(), key=lambda source: Decimal(source["messageTs"]))
                if not sources:
                    continue
                # membership 변경은 저장 직전에도 재확인한다. 권한이 바뀌면 재수집이 필요하다.
                if any(not self.is_allowed(state["tenantId"], source["authorId"]) for source in sources):
                    raise HistoryImportError("membership_changed")
                chunks, chunk, chars = [], [], 0
                for source in sources:
                    if chunk and (len(chunk) >= 100 or chars + len(source["text"]) > MAX_CHUNK_CHARS):
                        chunks.append({"chunkNo": len(chunks), "sources": chunk, "chunkDigest": canonical_digest(chunk)})
                        chunk, chars = [], 0
                    chunk.append(source)
                    chars += len(source["text"])
                if chunk:
                    chunks.append({"chunkNo": len(chunks), "sources": chunk, "chunkDigest": canonical_digest(chunk)})
                threads.append({"threadTs": ts, "cutoffTs": state["period"]["cutoffTs"],
                                "coverage": row["coverage"], "chunks": chunks})
            result = {key: state[key] for key in ("schemaVersion", "importId", "tenantId", "channelId", "period", "authorIds")}
            result.update({"kind": "slack_history_import", "channelKind": state["channelKind"], "threads": threads,
                           "coverage": "partial" if notes else "complete", "coverageNotes": notes})
            result["inputDigest"] = canonical_digest(threads)
            output = Path(output_path).expanduser()
            _validate_path(output)
            if output.exists():
                if _read(output) != result:
                    raise HistoryImportError("output_conflict")
            else:
                create_protected_json_file(output, result, label="memory history import")
            return self._summary(state)

    def _source(self, state: Mapping[str, Any], message: Mapping[str, Any]) -> dict[str, Any] | None:
        actor, text = str(message.get("user") or ""), str(message.get("text") or "").strip()
        if (actor not in state["authorIds"] or not self.is_allowed(state["tenantId"], actor)
                or message.get("bot_id") or message.get("subtype") or message.get("files") or message.get("attachments")
                or len(text) > MAX_CHUNK_CHARS or not is_allowed_conversation_source(text)):
            return None
        ts = _stamp(message.get("ts"))
        edited = str(_data(message.get("edited")).get("ts") or "")
        if edited:
            _stamp(edited)
            if Decimal(edited) > Decimal(state["period"]["cutoffTs"]):
                return None
        response = self._call("chat_getPermalink", channel=state["channelId"], message_ts=ts)
        self._check(response)
        permalink = _safe_permalink(response.get("permalink"), state["channelId"], ts)
        if not permalink:
            raise HistoryImportError("invalid_permalink")
        return {"messageTs": ts, "authorId": actor, "authorKind": "human", "text": text,
                "revisionHash": source_revision(text, edited), "permalink": permalink,
                "createdAt": ts, "editedAt": edited}

    @staticmethod
    def _check(response: Mapping[str, Any]) -> None:
        if response.get("ok") is not True:
            raise HistoryImportError("slack_read_failed")

    @staticmethod
    def _advance(state: dict[str, Any], response: Mapping[str, Any], *, cursor_key: str, done_key: str) -> None:
        cursor = str(_data(response.get("response_metadata")).get("next_cursor") or "")
        if (cursor and cursor == state[cursor_key]) or (response.get("has_more") and not cursor):
            raise HistoryImportError("pagination_incomplete")
        state[cursor_key], state[done_key] = cursor, not bool(cursor)

    @staticmethod
    def _summary(state: Mapping[str, Any]) -> dict[str, Any]:
        # CLI stdout에는 본문·인물별 발언 없이 진행량과 고정 범위만 남긴다.
        return {"importId": state["importId"], "period": state["period"],
                "historyDone": state["historyDone"], "historyPages": state["historyPages"],
                "historyMessages": state["historyMessages"], "candidateThreads": len(state["roots"]),
                "completedThreads": sum(row["done"] for row in state["roots"].values()),
                "replyPages": state["replyPages"], "sourceCount": state["sourceCount"],
                "excludedMessages": state["excludedMessages"], "modelCalls": 0}
