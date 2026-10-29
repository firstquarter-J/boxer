"""본문 없는 raw_state journal도 오래된 snapshot의 원문 부활과 watermark 역전을 막는다."""

from __future__ import annotations

import json
import sqlite3
from contextlib import closing
from types import SimpleNamespace

import pytest

from boxer_company.memory.backup import MemoryBackupError
from boxer_company.memory.contracts import MemoryError, source_revision
from boxer_company.memory.policy import normalize_source
from boxer_company.memory.raw_state import parse_raw_state_event
from boxer_company.memory.store import MemoryStore, initialize_memory_store

TENANT = "T19PA3EPL"
CHANNEL = "C0621TL2HSB"
MESSAGE = "1792000000.000001"
EVENT = "1792000002.000001"


@pytest.fixture
def store(tmp_path):
    return MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db"))


def source(text="이전 원문 발언", *, edited=""):
    return {"threadTs": MESSAGE, "messageTs": MESSAGE, "createdAt": MESSAGE, "editedAt": edited,
            "authorId": "U1", "authorKind": "human", "text": text, "revisionHash": source_revision(text, edited),
            "permalink": f"https://test.slack.com/archives/{CHANNEL}/p{MESSAGE.replace('.', '')}"}


def seed(store, item=None):
    item = item or source()
    normalized = normalize_source(item, tenant_id=TENANT, channel_id=CHANNEL,
                                  thread_ts=MESSAGE, current_message_ts=item["editedAt"] or MESSAGE)
    with store._connection(write=True) as connection:
        thread = store._thread(connection, TENANT, CHANNEL, MESSAGE, "private_channel")
        store._register_source(connection, TENANT, thread, normalized)
        connection.execute("""INSERT INTO memory_raw_imports VALUES(?,?,?,?,?,?,?,?,'completed',0)""",
            (normalized["sourceId"], TENANT, CHANNEL, "U1", "UOP", MESSAGE, EVENT, "a" * 64))
        connection.execute("INSERT INTO memory_source_texts VALUES(?,?,?,?,0)",
                           (TENANT, normalized["sourceId"], normalized["sourceId"], item["text"]))
    return normalized


def event(state="unavailable", *, revision="", timestamp=EVENT, identity="raw-state1"):
    return {"id": identity, "tenantId": TENANT, "scopeChannelId": CHANNEL, "sourceMessageId": MESSAGE,
            "eventType": "raw_state", "requestId": identity, "deletionKind": "raw_state",
            "reasonCode": json.dumps({"eventTs": timestamp, "revisionHash": revision, "state": state},
                                     sort_keys=True, separators=(",", ":")), "durabilityState": "pending"}


def state(store):
    with store._connection() as connection:
        row = connection.execute("SELECT event_ts,revision_hash,state FROM memory_raw_event_states").fetchone()
        return tuple(row) if row else None


def raw_count(store):
    with store._connection() as connection:
        return connection.execute("SELECT COUNT(*) FROM memory_source_texts").fetchone()[0]


@pytest.mark.parametrize("mode", ["unavailable", "deleted", "current"])
def test_replay_removes_old_body_without_ever_reconstructing_new_body(store, mode):
    seed(store)
    change = event(mode, revision="b" * 64)
    store.apply_deletion_event(change)
    store.apply_deletion_event(change)
    assert raw_count(store) == 0
    assert state(store) == (EVENT, "b" * 64, mode)
    with store._connection() as connection:
        assert connection.execute("SELECT state FROM memory_sources").fetchone()[0] == ("deleted" if mode == "deleted" else "unavailable")
        assert connection.execute("SELECT COUNT(*) FROM memory_events").fetchone()[0] == 1


def test_same_current_watermark_keeps_newly_registered_matching_body(store):
    latest = seed(store, source("최신 원문 발언", edited=EVENT))
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_raw_event_states VALUES(?,?,?,?,?,?)",
                           (TENANT, CHANNEL, MESSAGE, EVENT, latest["revisionHash"], "current"))
    store.apply_deletion_event(event("current", revision=latest["revisionHash"]))
    assert raw_count(store) == 1 and state(store) == (EVENT, latest["revisionHash"], "current")


@pytest.mark.parametrize("with_watermark", [False, True])
def test_older_journal_never_downgrades_newer_snapshot_body_or_watermark(store, with_watermark):
    newer = "1792000010.000001"
    latest = seed(store, source("나중에 확인한 원문", edited=newer))
    if with_watermark:
        with store._connection(write=True) as connection:
            connection.execute("INSERT INTO memory_raw_event_states VALUES(?,?,?,?,?,?)",
                               (TENANT, CHANNEL, MESSAGE, newer, latest["revisionHash"], "current"))
    store.apply_deletion_event(event("unavailable", revision="b" * 64))
    assert raw_count(store) == 1
    assert state(store) == ((newer, latest["revisionHash"], "current") if with_watermark else None)


def test_deletion_wins_equal_timestamp_and_cannot_be_reopened_by_later_current_event(store):
    current = seed(store)
    store.apply_deletion_event(event("current", revision=current["revisionHash"], identity="current"))
    store.apply_deletion_event(event("deleted", revision=current["revisionHash"], identity="deleted"))
    store.apply_deletion_event(event("current", revision="b" * 64, timestamp="1792000010.000001", identity="late-current"))
    assert state(store) == (EVENT, current["revisionHash"], "deleted")
    assert raw_count(store) == 0


def test_same_time_privacy_withdrawal_replaces_current_but_current_cannot_reopen_it(store):
    current = seed(store)
    store.apply_deletion_event(event("current", revision=current["revisionHash"], identity="same-current"))
    store.apply_deletion_event(event("unavailable", revision="b" * 64, identity="same-unavailable"))
    assert state(store) == (EVENT, "b" * 64, "unavailable") and raw_count(store) == 0
    store.apply_deletion_event(event("current", revision=current["revisionHash"], identity="same-late-current"))
    assert state(store) == (EVENT, "b" * 64, "unavailable") and raw_count(store) == 0
    store.apply_deletion_event(event("deleted", identity="same-deleted"))
    assert state(store) == (EVENT, "", "deleted")


@pytest.mark.parametrize("changes", [
    {"state": "unknown"}, {"state": []}, {"revisionHash": "bad"}, {"revisionHash": 42},
    {"state": "current", "revisionHash": ""}, {"eventTs": "NaN"}, {"eventTs": "1791999999.000001"},
    {"eventTs": "99999999999999999999.000001"}, {"text": "본문은 허용하지 않아"},
])
def test_invalid_state_payload_never_updates_watermark_or_journal(store, changes):
    change = event()
    payload = json.loads(change["reasonCode"])
    payload.update(changes)
    change["reasonCode"] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    with pytest.raises(MemoryError, match="memory_raw_state_invalid"):
        store.apply_deletion_event(change)
    assert state(store) is None and store.deletion_events() == []


@pytest.mark.parametrize("reason", ["not-json", "{}", '{"eventTs":"1792000002.000001","revisionHash":"","state":"unavailable", "state":"deleted"}',
                                     '{ "eventTs":"1792000002.000001","revisionHash":"","state":"unavailable"}'])
def test_reason_json_is_exact_canonical_and_rejects_duplicate_or_unknown_fields(reason):
    with pytest.raises(MemoryError, match="memory_raw_state_invalid"):
        parse_raw_state_event({**event(), "reasonCode": reason})


@pytest.mark.parametrize("changes", [{"tenantId": "TOTHER"}, {"scopeChannelId": "COTHER"}, {"sourceMessageId": "1793458800.000000"}])
def test_raw_state_is_fixed_to_approved_message_month_and_channels(changes):
    with pytest.raises(MemoryError, match="memory_raw_state_invalid"):
        parse_raw_state_event({**event(), **changes})


@pytest.mark.parametrize("snapshot_version", [1, 2, 3])
@pytest.mark.parametrize("mode", ["unavailable", "current", "deleted"])
def test_real_backup_restore_replays_state_before_exposing_old_snapshot(tmp_path, snapshot_version, mode):
    from tests.company.test_memory_backup import backup

    tmp_path.chmod(0o700)
    service, _ = backup()
    original = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    seed(original)
    def snapshot(path):
        original.snapshot(path)
        with closing(sqlite3.connect(path)) as connection:
            if snapshot_version < 3:
                connection.execute("DROP TABLE memory_raw_event_states")
            if snapshot_version < 2:
                connection.execute("DROP TABLE memory_source_texts")
                connection.execute("DROP TABLE memory_raw_imports")
            connection.execute(f"PRAGMA user_version={snapshot_version}")
    saved = service.create_snapshot(SimpleNamespace(deletion_events=original.deletion_events, snapshot=snapshot))
    original.apply_deletion_event(event(mode, revision="b" * 64))
    assert service.flush_deletions(original) == 1
    target = tmp_path / "restored.db"
    service.restore_snapshot(saved["key"], target, store_factory=MemoryStore)
    restored = MemoryStore(target)
    assert raw_count(restored) == 0 and state(restored) == (EVENT, "b" * 64, mode)
    assert restored.deletion_events()[0]["durabilityState"] == "durable"


def test_unknown_unsafe_edit_watermark_survives_restore_and_blocks_late_original_creation(tmp_path, monkeypatch):
    from boxer_company.memory import raw_live
    from tests.company.test_memory_backup import backup

    monkeypatch.setattr(raw_live, "TEAM_MEMBER_PROFILES", [{"user_id": "U1"}])
    tmp_path.chmod(0o700)
    service, _ = backup()
    original = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    saved = service.create_snapshot(original)
    original.apply_deletion_event(event("unavailable", revision="b" * 64))
    service.flush_deletions(original)
    target = tmp_path / "restored.db"
    service.restore_snapshot(saved["key"], target, store_factory=MemoryStore)
    restored = MemoryStore(target)
    result = raw_live.ingest_raw_event(restored, tenant_id=TENANT, channel_id=CHANNEL, event_id="late-created",
        event_ts=MESSAGE, event_type="created", source=source(), message_ts=MESSAGE, actor_id="U1")
    assert result["status"] == "stale" and raw_count(restored) == 0


def test_backup_rejects_raw_state_with_unknown_content_before_remote_write():
    from tests.company.test_memory_backup import backup

    service, client = backup()
    change = event()
    payload = json.loads(change["reasonCode"])
    payload["text"] = "저장하면 안 되는 본문"
    change["reasonCode"] = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    before = dict(client.objects)
    with pytest.raises(MemoryBackupError, match="memory_deletion_event_invalid"):
        service.append_deletion(change)
    assert client.objects == before


def test_same_time_unavailable_journal_clears_snapshot_current_body_and_blocks_old_archive_import(tmp_path):
    from boxer_company.memory.contracts import canonical_digest
    from boxer_company.memory.raw_archive import begin_raw_import, import_raw_messages
    from boxer_company.memory.raw_policy import RAW_ONLY_CUTOFF_TS, RAW_ONLY_START_TS
    from tests.company.test_memory_backup import backup

    tmp_path.chmod(0o700)
    service, _ = backup()
    original = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    current = seed(original)
    original.apply_deletion_event(event("current", revision=current["revisionHash"], identity="before-snapshot"))
    saved = service.create_snapshot(original)
    original.apply_deletion_event(event("unavailable", revision=current["revisionHash"], identity="after-lost-response"))
    assert service.flush_deletions(original) == 1
    target = tmp_path / "restored-same-time.db"
    service.restore_snapshot(saved["key"], target, store_factory=MemoryStore)
    restored = MemoryStore(target)
    assert state(restored) == (EVENT, current["revisionHash"], "unavailable") and raw_count(restored) == 0
    inputs = [{**source(), "sourceId": current["sourceId"]}]
    manifest = canonical_digest({"tenantId": TENANT, "channelId": CHANNEL, "authorId": "U1",
        "startTs": RAW_ONLY_START_TS, "cutoffTs": RAW_ONLY_CUTOFF_TS, "sources": inputs})
    begin_raw_import(restored, import_id="late-archive", tenant_id=TENANT, channel_id=CHANNEL,
        author_id="U1", operator_id="UOP", start_ts=RAW_ONLY_START_TS, cutoff_ts=RAW_ONLY_CUTOFF_TS,
        manifest_digest=manifest)
    with pytest.raises(MemoryError, match="raw_import_source_stale"):
        import_raw_messages(restored, import_id="late-archive", manifest_digest=manifest, sources=inputs)
    assert raw_count(restored) == 0
