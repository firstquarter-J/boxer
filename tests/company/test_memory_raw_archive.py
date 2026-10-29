from __future__ import annotations

import sqlite3

import pytest

from boxer_company.memory.contracts import (
    MemoryError,
    canonical_digest,
    source_identity,
    source_revision,
)
from boxer_company.memory.raw_archive import begin_raw_import, import_raw_messages
from boxer_company.memory.store import MemoryStore, initialize_memory_store

START = "1790780400.000000"
CUTOFF = "1793206800.000000"
MESSAGE = "1792000000.000001"


@pytest.fixture
def store(tmp_path):
    # 임시 DB와 고정 시각으로 실제 운영자 import의 원자성과 재시도 경계를 확인한다.
    path = tmp_path / "private" / "memory.db"
    initialize_memory_store(path)
    return MemoryStore(path, clock=lambda: 1_793_206_800.0)


def source(text="오늘 회의는 정말 힘들었어", *, ts=MESSAGE, edited="", **changes):
    return {"threadTs": ts, "messageTs": ts, "revisionHash": source_revision(text, edited),
            "authorId": "U1", "authorKind": "human", "text": text,
            "createdAt": ts, "editedAt": edited,
            "permalink": f"https://team.slack.com/archives/C1/p{ts.replace('.', '')}", **changes}


def digest(sources, **changes):
    return canonical_digest({"tenantId": "T1", "channelId": "C1", "authorId": "U1",
                             "startTs": START, "cutoffTs": CUTOFF, "sources": sources, **changes})


def begin(store, sources, *, import_id="october", **changes):
    return begin_raw_import(store, import_id=import_id, tenant_id="T1", channel_id="C1", author_id="U1",
                            operator_id="U2", start_ts=START, cutoff_ts=CUTOFF,
                            manifest_digest=digest(sources), **changes)


def perform(store, sources, *, import_id="october"):
    begin(store, sources, import_id=import_id)
    return import_raw_messages(store, import_id=import_id, manifest_digest=digest(sources), sources=sources)


def values(store, query, parameters=()):
    with store._connection() as connection:
        return [tuple(row) for row in connection.execute(query, parameters)]


def register_metadata(store, item, *, state="current"):
    from boxer_company.memory.policy import normalize_source

    normalized = normalize_source(item, tenant_id="T1", channel_id="C1", thread_ts=item["threadTs"],
                                  current_message_ts=CUTOFF)
    with store._connection(write=True) as connection:
        thread = store._thread(connection, "T1", "C1", item["threadTs"], "private_channel")
        store._register_source(connection, "T1", thread, normalized)
        connection.execute("UPDATE memory_sources SET state=? WHERE id=?", (state, normalized["sourceId"]))
    return normalized


def test_import_retains_entire_text_outside_summaries_and_extraction(store):
    item = source("  " + "오늘 회의에서 무슨 일이 있었는지 이야기했어. " * 600 + "  ")
    result = perform(store, [item])
    assert result == {"state": "completed", "sourceCount": 1, "storedCount": 1, "existingCount": 0}
    assert values(store, "SELECT text FROM memory_source_texts") == [(item["text"],)]
    assert values(store, "SELECT excerpt,author_id,state FROM memory_sources") == [("", "U1", "current")]
    for table in ("memory_items", "memory_summaries", "memory_jobs", "memory_job_inputs"):
        assert values(store, f"SELECT COUNT(*) FROM {table}") == [(0,)]
    assert item["text"] not in str(result)


def test_begin_and_completed_import_repeat_without_duplicate_sources(store):
    items = [source()]
    first = begin(store, items)
    assert begin(store, items) == first
    assert first["state"] == "collecting"
    import_raw_messages(store, import_id="october", manifest_digest=digest(items), sources=items)
    assert begin(store, items)["state"] == "completed"
    assert import_raw_messages(store, import_id="october", manifest_digest=digest(items), sources=items) == {
        "state": "completed", "sourceCount": 1, "storedCount": 0, "existingCount": 1}
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(1,)]
    assert values(store, "SELECT COUNT(*) FROM memory_source_texts") == [(1,)]


def test_second_authorized_manifest_deduplicates_existing_raw_source(store):
    item = source()
    perform(store, [item])
    result = perform(store, [item], import_id="october-repeat")
    assert result["storedCount"] == 0 and result["existingCount"] == 1
    assert values(store, "SELECT import_id FROM memory_source_texts") == [("october",)]


@pytest.mark.parametrize(("field", "value"), [
    ("tenant_id", "T2"), ("channel_id", "C2"), ("author_id", "U3"), ("operator_id", "U4"),
    ("start_ts", "1790780401.000000"), ("cutoff_ts", "1793206801.000000"),
    ("manifest_digest", "f" * 64),
])
def test_import_id_cannot_change_any_approved_boundary(store, field, value):
    items = [source()]
    begin(store, items)
    arguments = {"import_id": "october", "tenant_id": "T1", "channel_id": "C1", "author_id": "U1",
                 "operator_id": "U2", "start_ts": START, "cutoff_ts": CUTOFF,
                 "manifest_digest": digest(items), field: value}
    with pytest.raises(MemoryError, match="raw_import_identity_conflict"):
        begin_raw_import(store, **arguments)


@pytest.mark.parametrize(("field", "value"), [
    ("import_id", ""), ("channel_id", "D1"), ("author_id", "B1"), ("operator_id", "B2"),
    ("start_ts", "NaN"), ("cutoff_ts", START), ("cutoff_ts", "1793558801.000000"),
    ("manifest_digest", "no-digest"), ("tenant_id", 42), ("cutoff_ts", "99999999999999999999.0"),
])
def test_invalid_begin_rejected_without_any_import_record(store, field, value):
    arguments = {"import_id": "october", "tenant_id": "T1", "channel_id": "C1", "author_id": "U1",
                 "operator_id": "U2", "start_ts": START, "cutoff_ts": CUTOFF,
                 "manifest_digest": digest([]), field: value}
    with pytest.raises(MemoryError, match="raw_import_scope_invalid"):
        begin_raw_import(store, **arguments)
    assert values(store, "SELECT COUNT(*) FROM memory_raw_imports") == [(0,)]


def test_preexisting_public_or_dm_scope_cannot_be_relabelled_private(store):
    with store._connection(write=True) as connection:
        store._thread(connection, "T1", "C1", MESSAGE, "channel")
    with pytest.raises(MemoryError, match="raw_import_private_channel_required"):
        begin(store, [source()])
    assert values(store, "SELECT channel_kind FROM memory_threads") == [("channel",)]


def test_entire_manifest_and_list_order_are_immutable(store):
    items = [source(), source("다음 회의 준비했어", ts="1792000001.000001")]
    begin(store, items)
    for replacement in (items[:1], list(reversed(items)), [source("다른 내용")]):
        with pytest.raises(MemoryError, match="raw_import_manifest_conflict"):
            import_raw_messages(store, import_id="october", manifest_digest=digest(items), sources=replacement)
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(0,)]


@pytest.mark.parametrize("item", [
    source(authorId="U3"), source(authorKind="boxer"), source(deliveryVerified=True),
    source(createdAt="1792000001.000001"), source(ts="1790780399.999999"),
    source(ts="1793206800.000001"), source(threadTs="1792000001.000001"),
    source(edited="1793206801.000001"), source(edited="1791999999.000001"), source(threadTs="bad"),
    source(revisionHash="f" * 64), source(permalink="https://example.com/message"),
    source(sourceId="src_wrong"), source(unexpected="not in raw contract"),
    source(text="가" * 40_001), source(text=123), source(editedAt=False), source(route=[]),
])
def test_malformed_source_rejects_entire_snapshot(store, item):
    items = [source("정상 발언", ts="1791000000.000001"), item]
    begin(store, items)
    with pytest.raises(MemoryError):
        import_raw_messages(store, import_id="october", manifest_digest=digest(items), sources=items)
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(0,)]
    assert values(store, "SELECT COUNT(*) FROM memory_source_texts") == [(0,)]
    assert values(store, "SELECT state FROM memory_raw_imports") == [("collecting",)]


@pytest.mark.parametrize("text", ["비밀번호를 적어뒀어", "test@example.com", "장비 로그 조회해줘", "sudo reboot"])
def test_sensitive_or_operation_source_is_not_archived(store, text):
    items = [source(text)]
    begin(store, items)
    with pytest.raises(MemoryError, match="raw_import_source_not_allowed"):
        import_raw_messages(store, import_id="october", manifest_digest=digest(items), sources=items)
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(0,)]


def test_duplicate_message_is_not_a_second_source_even_with_another_revision(store):
    items = [source(), source("고친 내용", edited="1792000002.000001")]
    begin(store, items)
    with pytest.raises(MemoryError, match="raw_import_source_invalid"):
        import_raw_messages(store, import_id="october", manifest_digest=digest(items), sources=items)
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(0,)]


def test_duplicate_timestamp_numeric_alias_is_not_a_second_message(store):
    items = [source(ts="1792000000.1"), source("다른 내용", ts="1792000000.100")]
    begin(store, items)
    with pytest.raises(MemoryError, match="raw_import_source_invalid"):
        import_raw_messages(store, import_id="october", manifest_digest=digest(items), sources=items)
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(0,)]


def test_later_revision_cannot_change_the_original_author(store):
    register_metadata(store, source())
    with store._connection(write=True) as connection:
        connection.execute("UPDATE memory_sources SET author_id='U3'")
    item = source("새 내용", edited="1792000003.000001")
    with pytest.raises(MemoryError, match="raw_import_source_identity_conflict"):
        perform(store, [item])
    assert values(store, "SELECT author_id,state FROM memory_sources") == [("U3", "current")]


@pytest.mark.parametrize("event_type", ["deleted", "changed", "unavailable"])
def test_known_source_event_before_import_never_revives_original(store, event_type):
    item = source()
    register_metadata(store, item)
    begin(store, [item])
    store.source_event(tenant_id="T1", channel_id="C1", message_ts=MESSAGE,
                       event_type=event_type, event_id="revision-before-import")
    with pytest.raises(MemoryError, match="raw_import_(?:source_stale|terminal)"):
        import_raw_messages(store, import_id="october", manifest_digest=digest([item]), sources=[item])
    assert values(store, "SELECT COUNT(*) FROM memory_source_texts") == [(0,)]


@pytest.mark.parametrize("event_type", ["deleted", "changed", "unavailable"])
def test_unknown_source_event_during_approved_collection_blocks_late_snapshot(store, event_type):
    # 아직 source metadata가 없는 발언도 승인 기간 안의 이벤트 tombstone을 먼저 확인한다.
    item = source()
    begin(store, [item])
    store.source_event(tenant_id="T1", channel_id="C1", message_ts=MESSAGE,
                       event_type=event_type, event_id="before-any-source-metadata")
    with pytest.raises(MemoryError, match="raw_import_(?:source_stale|terminal)"):
        import_raw_messages(store, import_id="october", manifest_digest=digest([item]), sources=[item])
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(0,)]


def test_matching_changed_event_does_not_revoke_the_already_verified_revision(store):
    item = source()
    register_metadata(store, item)
    begin(store, [item])
    store.source_event(tenant_id="T1", channel_id="C1", message_ts=MESSAGE,
                       event_type="changed", revision_hash=item["revisionHash"], event_id="matching-current-revision")
    assert import_raw_messages(store, import_id="october", manifest_digest=digest([item]), sources=[item])["storedCount"] == 1


@pytest.mark.parametrize("event_type", ["deleted", "changed", "unavailable"])
def test_completed_manifest_never_restores_text_after_source_event(store, event_type):
    item = source()
    perform(store, [item])
    store.source_event(tenant_id="T1", channel_id="C1", message_ts=MESSAGE,
                       event_type=event_type, event_id="revision-after-import")
    with pytest.raises(MemoryError, match="raw_import_(?:source_stale|terminal)"):
        import_raw_messages(store, import_id="october", manifest_digest=digest([item]), sources=[item])
    assert values(store, "SELECT COUNT(*) FROM memory_source_texts") == [(0,)]


def test_newer_revision_cannot_be_downgraded_by_old_snapshot(store):
    old = source()
    latest = source("새로 고친 내용", edited="1792000003.000001")
    normalized = register_metadata(store, latest)
    begin(store, [old])
    with pytest.raises(MemoryError, match="raw_import_source_stale"):
        import_raw_messages(store, import_id="october", manifest_digest=digest([old]), sources=[old])
    assert values(store, "SELECT id,state FROM memory_sources") == [(normalized["sourceId"], "current")]


@pytest.mark.parametrize("state", ["deleted", "unavailable", "superseded"])
def test_preexisting_noncurrent_metadata_cannot_be_revived(store, state):
    item = source()
    register_metadata(store, item, state=state)
    with pytest.raises(MemoryError, match="raw_import_source_stale"):
        perform(store, [item])
    assert values(store, "SELECT state FROM memory_sources") == [(state,)]


@pytest.mark.parametrize(("column", "value"), [
    ("author_id", "U3"), ("permalink", "https://team.slack.com/archives/C1/p000"),
    ("created_at", "1792000001.000001"), ("thread_id", "other-thread"),
])
def test_source_identity_conflict_cannot_be_hidden_by_matching_hash(store, column, value):
    item = source()
    normalized = register_metadata(store, item)
    with store._connection(write=True) as connection:
        if column == "thread_id":
            thread = store._thread(connection, "T1", "C1", "1791000000.000001", "private_channel")
            value = thread["id"]
        connection.execute(f"UPDATE memory_sources SET {column}=? WHERE id=?", (value, normalized["sourceId"]))
    with pytest.raises(MemoryError, match="raw_import_source_identity_conflict"):
        perform(store, [item])
    assert values(store, "SELECT COUNT(*) FROM memory_source_texts") == [(0,)]


def test_existing_excerpt_is_unchanged_by_archiving_same_source(store):
    item = source()
    normalized = register_metadata(store, item)
    with store._connection(write=True) as connection:
        connection.execute("UPDATE memory_sources SET excerpt='기존 근거 발췌' WHERE id=?", (normalized["sourceId"],))
    perform(store, [item])
    assert values(store, "SELECT excerpt FROM memory_sources") == [("기존 근거 발췌",)]


def test_same_revision_with_different_whitespace_cannot_overwrite_archived_original(store):
    item = source()
    perform(store, [item])
    with pytest.raises(MemoryError, match="raw_import_text_conflict"):
        perform(store, [source("  " + item["text"] + "  ")], import_id="october-whitespace")
    assert values(store, "SELECT text FROM memory_source_texts") == [(item["text"],)]


def test_empty_approved_snapshot_can_complete_without_fake_sources(store):
    assert perform(store, []) == {"state": "completed", "sourceCount": 0, "storedCount": 0, "existingCount": 0}
    assert values(store, "SELECT COUNT(*) FROM memory_sources") == [(0,)]


def test_unknown_import_and_wrong_digest_fail_closed(store):
    with pytest.raises(MemoryError, match="raw_import_missing"):
        import_raw_messages(store, import_id="unknown", manifest_digest=digest([]), sources=[])
    begin(store, [])
    with pytest.raises(MemoryError, match="raw_import_manifest_conflict"):
        import_raw_messages(store, import_id="october", manifest_digest="f" * 64, sources=[])


def test_schema_keeps_archive_sources_in_same_tenant(store):
    item = source()
    perform(store, [item])
    identity = source_identity("T1", "C1", MESSAGE, item["revisionHash"])
    with pytest.raises(sqlite3.IntegrityError), store._connection(write=True) as connection:
        connection.execute("""INSERT INTO memory_source_texts(tenant_id,source_id,import_id,text,stored_at)
            VALUES('T2',?,'october','누출 금지',0)""", (identity,))
