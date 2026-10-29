"""파생 기억 삭제는 원문과 출처를 보존하고 journal 재적용 때도 같은 경계를 지킨다."""

from __future__ import annotations

import json

import pytest

from boxer_company.memory.contracts import (
    MemoryError,
    canonical_digest,
    source_revision,
)
from boxer_company.memory.derived_purge import operator_purge_derived
from boxer_company.memory.policy import normalize_source
from boxer_company.memory.store import MemoryStore, initialize_memory_store


@pytest.fixture
def store(tmp_path):
    return MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db")), tmp_path


def seed(store, *, channel="C1", stamp="1792000000.000001", item_id=None):
    text = "이 발언은 원문으로 계속 보존해"
    source = normalize_source({"messageTs": stamp, "createdAt": stamp, "authorId": "U1", "authorKind": "human",
        "text": text, "revisionHash": source_revision(text), "permalink":
        f"https://test.slack.com/archives/{channel}/p{stamp.replace('.', '')}"},
        tenant_id="T1", channel_id=channel, thread_ts=stamp, current_message_ts=stamp)
    item_id = item_id or "item_" + canonical_digest([channel, stamp])
    raw_id = "raw_" + source["sourceId"]
    with store._connection(write=True) as connection:
        thread = store._thread(connection, "T1", channel, stamp, "private_channel")
        store._register_source(connection, "T1", thread, source)
        connection.execute("UPDATE memory_sources SET excerpt='보존할 발췌' WHERE id=?", (source["sourceId"],))
        connection.execute("""INSERT OR IGNORE INTO memory_items(id,tenant_id,scope_channel_id,origin_thread_id,
            kind,category,subject_user_id,topic_key,text,evidence_type,status,valid_from,observed_at,updated_at,extractor_version)
            VALUES(?,'T1',?,?,'person','fact','U1','예전주제','지울 파생 요약','self_statement','active',0,0,0,'memory-v1')""",
            (item_id, channel, thread["id"]))
        connection.execute("INSERT OR IGNORE INTO memory_subjects VALUES('T1',?,'U1','subject')", (item_id,))
        connection.execute("INSERT INTO memory_item_sources VALUES('T1',?,?,'supports','보존할 발췌')", (item_id, source["sourceId"]))
        connection.execute("""INSERT INTO memory_raw_imports VALUES(?,'T1',?,'U1','UOP','1000.000001',
            '2000000000.000001',?,'completed',0)""", (raw_id, channel, "a" * 64))
        connection.execute("INSERT INTO memory_source_texts VALUES('T1',?,?,?,0)", (source["sourceId"], raw_id, text))
        store._refresh_summaries(connection, "T1", channel)
    return source["sourceId"], item_id


def purge(store, **changes):
    arguments = {"tenant_id": "T1", "channel_ids": ["C1"], "operator_id": "UOP", "request_id": "purge1", **changes}
    return operator_purge_derived(store, **arguments)


def rows(store, query):
    with store._connection() as connection:
        return [tuple(row) for row in connection.execute(query)]


def test_purge_clears_only_derived_values_and_preserves_full_source_and_raw(store):
    store, _ = store
    seed(store)
    before = {table: rows(store, f"SELECT * FROM {table}") for table in
              ("memory_sources", "memory_source_texts", "memory_raw_imports")}
    result = purge(store)
    assert result == {"state": "purged", "purgedItemCount": 1, "scopeChannelCount": 1,
                      "journalEventCount": 2, "durability": "pending"}
    assert rows(store, "SELECT text,topic_key,subject_user_id,evidence_type,status,version FROM memory_items") == [
        ("", "", None, "", "deleted", 2)]
    for table in ("memory_subjects", "memory_item_sources", "memory_summaries"):
        assert rows(store, f"SELECT COUNT(*) FROM {table}") == [(0,)]
    assert {table: rows(store, f"SELECT * FROM {table}") for table in before} == before
    assert all(event["deletionKind"] == "derived_item" and len(event["reasonCode"]) == 64 for event in store.deletion_events())


def test_period_selects_any_matching_source_and_deletes_mixed_period_item_entirely(store):
    store, _ = store
    _, mixed = seed(store, stamp="1790700000.000001", item_id="mixed")
    seed(store, stamp="1792000000.000001", item_id=mixed)
    _, outside = seed(store, stamp="1790600000.000001", item_id="outside")
    result = purge(store, start_ts="1790780400.000000", end_ts="1793458800.000000")
    assert result["purgedItemCount"] == 1
    assert rows(store, "SELECT id,status FROM memory_items ORDER BY id") == [(mixed, "deleted"), (outside, "active")]
    assert rows(store, "SELECT COUNT(*) FROM memory_source_texts") == [(3,)]


def test_period_is_start_inclusive_end_exclusive_and_does_not_touch_other_channel(store):
    store, _ = store
    seed(store, stamp="1790780400.000000", item_id="start")
    seed(store, stamp="1793458800.000000", item_id="end")
    seed(store, channel="C2", stamp="1790780400.000000", item_id="other")
    purge(store, start_ts="1790780400.000000", end_ts="1793458800.000000")
    assert rows(store, "SELECT id,status FROM memory_items ORDER BY id") == [("end", "active"), ("other", "active"), ("start", "deleted")]


def test_same_request_is_idempotent_and_channel_order_does_not_change_manifest(store):
    store, _ = store
    seed(store)
    seed(store, channel="C2")
    first = purge(store, channel_ids=["C1", "C2"])
    before = rows(store, "SELECT * FROM memory_events")
    assert purge(store, channel_ids=["C2", "C1"]) == first
    assert rows(store, "SELECT * FROM memory_events") == before
    assert rows(store, "SELECT version FROM memory_items") == [(2,), (2,)]


@pytest.mark.parametrize("change", [
    {"channel_ids": ["C1", "C2"]}, {"operator_id": "UOTHER"},
    {"start_ts": "1790780400.000000", "end_ts": "1793458800.000000"},
])
def test_same_request_cannot_expand_or_change_scope(store, change):
    store, _ = store
    seed(store)
    purge(store)
    with pytest.raises(MemoryError, match="memory_derived_request_conflict"):
        purge(store, **change)


@pytest.mark.parametrize("change", [
    {"channel_ids": []}, {"channel_ids": ["C1", "C1"]}, {"channel_ids": "C1"},
    {"operator_id": ""}, {"request_id": []}, {"start_ts": "1.0"},
    {"start_ts": "2.0", "end_ts": "1.0"}, {"start_ts": "NaN", "end_ts": "2.0"},
])
def test_invalid_scope_never_mutates_data(store, change):
    store, _ = store
    seed(store)
    with pytest.raises(MemoryError):
        purge(store, **change)
    assert rows(store, "SELECT status FROM memory_items") == [("active",)]
    assert rows(store, "SELECT COUNT(*) FROM memory_events") == [(0,)]


def test_empty_scope_still_journals_marker_and_preserves_existing_source_tombstones(store):
    store, _ = store
    store.apply_deletion_event({"id": "existing-delete", "tenantId": "T1", "scopeChannelId": "C1",
                               "eventType": "source_deleted", "requestId": "delete1",
                               "deletionKind": "source", "sourceMessageId": "1792000000.000001"})
    before = next(event for event in store.deletion_events() if event["id"] == "existing-delete")
    assert purge(store)["journalEventCount"] == 1
    assert next(event for event in store.deletion_events() if event["id"] == "existing-delete") == before


def test_purge_cancels_pending_payload_and_cross_channel_copies_of_previous_items(store):
    store, _ = store
    _, item_id = seed(store)
    for channel in ("C1", "C2"):
        capture = store.create_capture(tenant_id="T1", actor_id="U1", channel_id=channel, thread_ts="1800000000.000001",
            current_message_ts="1800000000.000001", channel_kind="private_channel", request_id=channel, route="freeform")
        with store._connection(write=True) as connection:
            job = connection.execute("SELECT * FROM memory_jobs WHERE capture_id=?", (capture["captureId"],)).fetchone()
            metadata = json.loads(job["metadata"])
            metadata["previousItems"] = [{"memoryId": item_id, "text": "지울 파생 요약"}]
            connection.execute("UPDATE memory_jobs SET metadata=? WHERE id=?", (json.dumps(metadata), job["id"]))
            connection.execute("INSERT INTO memory_job_inputs VALUES('T1',?,0,?,'[]','[]',NULL,0,9999999999,1)",
                               (capture["captureId"], "a" * 64))
    purge(store)
    assert rows(store, "SELECT state FROM memory_jobs") == [("canceled",), ("canceled",)]
    assert rows(store, "SELECT COUNT(*) FROM memory_job_inputs") == [(0,)]
    assert all("previousItems" not in metadata for (metadata,) in rows(store, "SELECT metadata FROM memory_jobs"))


def test_purge_replay_clears_old_snapshot_but_never_the_original_raw(store):
    store, tmp_path = store
    seed(store)
    snapshot = tmp_path / "private" / "old.db"
    store.snapshot(snapshot)
    purge(store)
    restored = MemoryStore(snapshot)
    for event in store.deletion_events():
        restored.apply_deletion_event(event)
        restored.apply_deletion_event(event)
    assert rows(restored, "SELECT text,status,version FROM memory_items") == [("", "deleted", 2)]
    assert rows(restored, "SELECT COUNT(*) FROM memory_source_texts") == [(1,)]
    assert rows(restored, "SELECT excerpt,state FROM memory_sources") == [("보존할 발췌", "current")]
    assert purge(restored)["purgedItemCount"] == 1
    with pytest.raises(MemoryError, match="memory_derived_request_conflict"):
        purge(restored, channel_ids=["C1", "C2"])


def test_purge_failure_rolls_back_all_items_markers_and_cached_summaries(store, monkeypatch):
    store, _ = store
    seed(store)
    apply = store._apply_event
    def fail_item(connection, event):
        if event.get("memoryId"):
            raise MemoryError("synthetic_failure")
        apply(connection, event)
    monkeypatch.setattr(store, "_apply_event", fail_item)
    with pytest.raises(MemoryError, match="synthetic_failure"):
        purge(store)
    assert rows(store, "SELECT status FROM memory_items") == [("active",)]
    assert rows(store, "SELECT COUNT(*) FROM memory_events") == [(0,)]
    assert rows(store, "SELECT COUNT(*) FROM memory_summaries")[0][0] > 0


@pytest.mark.parametrize("limited", [False, True])
def test_partial_journal_restore_cannot_mistake_scope_marker_for_completed_item_deletion(store, limited):
    store, tmp_path = store
    seed(store)
    snapshot = tmp_path / "private" / "before-partial.db"
    store.snapshot(snapshot)
    window = {"start_ts": "1790780400.000000", "end_ts": "1793458800.000000"} if limited else {}
    purge(store, **window)
    marker = next(event for event in store.deletion_events() if event["memoryId"] is None)
    assert marker["expectedItemCount"] == 1
    restored = MemoryStore(snapshot)
    restored.apply_deletion_event(marker)
    restored.mark_deletion_durable(marker["id"], 1)
    # 첫 marker만 원격에 내구화된 실패를 재현한다. 재요청은 남은 item을 삭제하고 pending을 유지해야 한다.
    assert rows(restored, "SELECT status FROM memory_items") == [("active",)]
    result = purge(restored, **window)
    assert result["purgedItemCount"] == 1 and result["durability"] == "pending"
    assert rows(restored, "SELECT status FROM memory_items") == [("deleted",)]
    assert rows(restored, "SELECT COUNT(*) FROM memory_source_texts") == [(1,)]
