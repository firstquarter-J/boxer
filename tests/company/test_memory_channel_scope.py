"""고정 비공개 채널의 legacy 종류만 감사 기록과 함께 원자적으로 정정한다."""

from __future__ import annotations

import sqlite3

import pytest

from boxer_company.memory.channel_scope import operator_reconcile_private_channel
from boxer_company.memory.contracts import MemoryError, source_revision
from boxer_company.memory.policy import normalize_source
from boxer_company.memory.raw_archive import begin_raw_import
from boxer_company.memory.raw_policy import RAW_ONLY_TENANT_ID
from boxer_company.memory.store import MemoryStore, initialize_memory_store

TENANT, CHANNEL, OTHER_CHANNEL = RAW_ONLY_TENANT_ID, "C068FVD5V7Y", "C0621TL2HSB"


@pytest.fixture
def store(tmp_path):
    return MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db"))


def reconcile(store, **changes):
    arguments = {"tenant_id": TENANT, "channel_id": CHANNEL, "operator_id": "UOPERATOR",
        "request_id": "reconcile-private-1", "confirm_slack_private_channel": True, **changes}
    return operator_reconcile_private_channel(store, **arguments)


def thread(store, *, kind="channel", channel=CHANNEL, tenant=TENANT, stamp="1792000000.000001"):
    with store._connection(write=True) as connection:
        return dict(store._thread(connection, tenant, channel, stamp, kind))


def tables(store):
    with store._connection() as connection:
        names = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'memory_%'")]
        return {name: [tuple(row) for row in connection.execute(f"SELECT * FROM {name} ORDER BY rowid")] for name in names}


def seed_content(store, row):
    # 원문, 출처, 파생물과 대기 job이 실제로 존재해도 이 연산은 종류만 바꿔야 한다.
    text, stamp = "합성 원문은 그대로 보존해", row["thread_ts"]
    source = normalize_source({"messageTs": stamp, "createdAt": stamp, "authorId": "UAUTHOR", "authorKind": "human",
        "text": text, "revisionHash": source_revision(text),
        "permalink": f"https://test.slack.com/archives/{CHANNEL}/p{stamp.replace('.', '')}"},
        tenant_id=TENANT, channel_id=CHANNEL, thread_ts=stamp, current_message_ts=stamp)
    with store._connection(write=True) as connection:
        store._register_source(connection, TENANT, row, source)
        connection.execute("UPDATE memory_sources SET excerpt='보존할 발췌' WHERE id=?", (source["sourceId"],))
        connection.execute("""INSERT INTO memory_raw_imports VALUES(?,?,?,?,?,'1790780400.000000',
            '1793000000.000000',?,'completed',0)""", ("raw-seed", TENANT, CHANNEL, "UAUTHOR", "UOPERATOR", "a" * 64))
        connection.execute("INSERT INTO memory_source_texts VALUES(?,?,?,?,0)", (TENANT, source["sourceId"], "raw-seed", text))
        connection.execute("""INSERT INTO memory_items(id,tenant_id,scope_channel_id,origin_thread_id,kind,category,
            subject_user_id,topic_key,text,evidence_type,status,valid_from,observed_at,updated_at,extractor_version)
            VALUES('item',?,?,?,'person','fact','UAUTHOR','합성주제','합성 파생물','self_statement','active',0,0,0,'memory-v1')""",
            (TENANT, CHANNEL, row["id"]))
        connection.execute("INSERT INTO memory_subjects VALUES(?,'item','UAUTHOR','subject')", (TENANT,))
        connection.execute("INSERT INTO memory_item_sources VALUES(?,'item',?,'supports','합성 근거')", (TENANT, source["sourceId"]))
        connection.execute("""INSERT INTO memory_jobs(id,tenant_id,capture_id,trigger_request_id,thread_id,
            extractor_version,metadata,created_at,expires_at) VALUES('job',?,'capture','trigger',?,'memory-v1','{}',0,9999999999)""",
            (TENANT, row["id"]))
        connection.execute("""INSERT INTO memory_job_inputs VALUES(?,'capture',0,'digest',
            '{"previousItems":["합성 대기 내용"]}',NULL,NULL,0,9999999999,1)""", (TENANT,))
        connection.execute("INSERT INTO memory_summaries VALUES(?,?,'channel','summary','합성 요약','[]','digest','partial',0)",
                           (TENANT, CHANNEL))
        connection.execute("INSERT INTO memory_raw_event_states VALUES(?,?,?,?,'','deleted')", (TENANT, CHANNEL, stamp, stamp))


def test_reconcile_only_changes_legacy_kind_and_adds_metadata_audit(store):
    target = thread(store)
    thread(store, kind="private_channel", stamp="1792000000.000002")
    thread(store, channel=OTHER_CHANNEL)
    thread(store, tenant="TOTHER")
    seed_content(store, target)
    before = tables(store)
    result = reconcile(store)
    after = tables(store)
    assert result["state"] == "reconciled" and result["changedThreadCount"] == 1
    assert result["privateThreadCount"] == 2 and result["replayed"] is False
    for name in before.keys() - {"memory_threads", "memory_events"}:
        assert after[name] == before[name]
    expected = [list(row) for row in before["memory_threads"]]
    for row in expected:
        if row[0] == target["id"]:
            row[4] = "private_channel"
    assert after["memory_threads"] == [tuple(row) for row in expected]
    with store._connection() as connection:
        event = dict(connection.execute("SELECT * FROM memory_events").fetchone())
    assert event["reason_code"] == result["scopeDigest"] and len(event["reason_code"]) == 64
    assert event["event_type"] == "private_channel_reconcile" and event["actor_id"] == "UOPERATOR"
    assert event["memory_id"] is event["source_id"] is event["deletion_kind"] is None
    assert event["durability_state"] == "not_required" and store.deletion_events() == []


def test_reconcile_unblocks_formal_private_import_without_weakening_its_guard(store):
    thread(store)
    arguments = {"import_id": "raw-after-reconcile", "tenant_id": TENANT, "channel_id": CHANNEL,
        "author_id": "UAUTHOR", "operator_id": "UOPERATOR", "start_ts": "1790780400.000000",
        "cutoff_ts": "1792000000.000001", "manifest_digest": "a" * 64}
    with pytest.raises(MemoryError, match="raw_import_private_channel_required"):
        begin_raw_import(store, **arguments)
    reconcile(store)
    assert begin_raw_import(store, **arguments)["state"] == "collecting"


@pytest.mark.parametrize("kind", ["dm", "group_dm", "unknown", "public_channel", ""])
def test_unknown_or_dm_kind_rejects_entire_channel_without_partial_update(store, kind):
    thread(store)
    thread(store, kind=kind, stamp="1792000000.000002")
    before = tables(store)
    with pytest.raises(MemoryError, match="memory_channel_kind_conflict"):
        reconcile(store)
    assert tables(store) == before


@pytest.mark.parametrize("change", [{"tenant_id": "TOTHER"}, {"channel_id": "COTHER"},
    {"channel_id": []}, {"operator_id": ""}, {"operator_id": "BOT"}, {"request_id": []},
    {"confirm_slack_private_channel": False}, {"confirm_slack_private_channel": "true"}])
def test_fixed_scope_and_explicit_slack_confirmation_are_required(store, change):
    thread(store)
    before = tables(store)
    with pytest.raises(MemoryError, match="memory_channel_scope_invalid"):
        reconcile(store, **change)
    assert tables(store) == before


def test_same_request_is_idempotent_and_changed_scope_or_operator_conflicts(store):
    thread(store)
    first = reconcile(store)
    before = tables(store)
    assert reconcile(store) == {**first, "replayed": True}
    assert tables(store) == before
    for change in ({"channel_id": OTHER_CHANNEL}, {"operator_id": "UOTHER"}):
        with pytest.raises(MemoryError, match="memory_channel_request_conflict"):
            reconcile(store, **change)
        assert tables(store) == before


@pytest.mark.parametrize("kind", [None, "private_channel"])
def test_empty_or_already_private_scope_has_zero_changes_and_one_idempotent_audit(store, kind):
    if kind:
        thread(store, kind=kind)
    result = reconcile(store)
    assert result["changedThreadCount"] == 0 and result["privateThreadCount"] == int(kind is not None)
    assert reconcile(store) == {**result, "replayed": True}
    assert len(tables(store)["memory_events"]) == 1


def test_audit_failure_rolls_back_kind_change(store):
    thread(store)
    # 감사 event를 쓸 수 없으면 앞선 UPDATE도 같은 transaction에서 취소돼야 한다.
    with store._connection(write=True) as connection:
        connection.execute("""CREATE TRIGGER reject_reconcile BEFORE INSERT ON memory_events
            WHEN NEW.event_type='private_channel_reconcile'
            BEGIN SELECT RAISE(ABORT,'synthetic_audit_failure'); END""")
    before = tables(store)
    with pytest.raises(sqlite3.IntegrityError, match="synthetic_audit_failure"):
        reconcile(store)
    assert tables(store) == before


def test_idempotent_replay_never_hides_new_legacy_or_unknown_rows(store):
    thread(store)
    reconcile(store)
    thread(store, stamp="1792000000.000002")
    before = tables(store)
    with pytest.raises(MemoryError, match="memory_channel_reconcile_state_changed"):
        reconcile(store)
    assert tables(store) == before
