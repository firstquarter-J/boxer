"""실제 기억 SQLite만 읽어 저장 발췌의 범위, 단위와 삭제 경계를 검증한다."""

from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from boxer_company.memory.contracts import (
    MemoryError,
    canonical_digest,
    source_identity,
    source_revision,
)
from boxer_company.memory.conversation_count_store import count_stored_messages
from boxer_company.memory.store import MemoryStore, initialize_memory_store

TENANT, CHANNEL, AUTHOR = "T1", "C1", "UDD"
STAMP, CUTOFF = "1792100001.000001", "1792670000.000001"
PERIOD = {"kind": "year", "startTs": "1767193200.000000", "endTs": "1798729200.000000", "cutoffTs": CUTOFF}


@pytest.fixture
def store(tmp_path):
    path = tmp_path / "private" / "memory.db"
    initialize_memory_store(path)
    return MemoryStore(path, clock=lambda: float(CUTOFF), debounce_seconds=0)


def add_source(store, *, excerpt="씨발 피곤하다", tenant=TENANT, channel=CHANNEL, author=AUTHOR,
               stamp=STAMP, edited="", state="current", kind="private_channel", author_kind="human", raw=None):
    revision = source_revision(excerpt if raw is None else raw, edited)
    identity = source_identity(tenant, channel, stamp, revision)
    thread = "thr_" + canonical_digest([tenant, channel])
    with store._connection(write=True) as connection:
        connection.execute("INSERT OR IGNORE INTO memory_threads(id,tenant_id,channel_id,thread_ts,channel_kind) VALUES(?,?,?,?,?)",
                           (thread, tenant, channel, "1760000000.000001", kind))
        connection.execute("INSERT INTO memory_sources(id,tenant_id,channel_id,thread_id,message_ts,revision_hash,author_id,"
                           "author_kind,created_at,edited_at,permalink,excerpt,state,last_verified_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (identity, tenant, channel, thread, stamp, revision, author, author_kind, stamp, edited,
                            f"https://team.slack.com/archives/{channel}/p{stamp.replace('.', '')}", excerpt, state, float(CUTOFF)))
    return identity


def add_item(store, source_id, *, text="회의 담당이야", name="item1"):
    # 실제 채택 인용과 달리 요약 text는 집계 입력으로 쓰면 안 된다.
    with store._connection(write=True) as connection:
        source = connection.execute("SELECT * FROM memory_sources WHERE id=?", (source_id,)).fetchone()
        connection.execute("INSERT INTO memory_items(id,tenant_id,scope_channel_id,origin_thread_id,kind,category,subject_user_id,"
                           "topic_key,text,evidence_type,valid_from,observed_at,updated_at,extractor_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (name, source["tenant_id"], source["channel_id"], source["thread_id"], "person", "fact", source["author_id"],
                            "회의 담당", text, "self_statement", 0, 0, 0, "test"))
        connection.execute("INSERT INTO memory_item_sources(tenant_id,memory_id,source_id,relation,evidence_span) VALUES(?,?,?,'supports',?)",
                           (source["tenant_id"], name, source_id, source["excerpt"]))
        return store._item(connection, connection.execute("SELECT * FROM memory_items WHERE id=?", (name,)).fetchone())


def count(store, *, tenant=TENANT, channel=CHANNEL, author=AUTHOR, period=None):
    return count_stored_messages(store, tenant_id=tenant, channel_id=channel, author_id=author,
                                 period=PERIOD if period is None else period)


def test_count_reads_existing_tables_without_creating_state_or_writing_source_text(store, monkeypatch):
    add_source(store)
    statements = []
    connect = sqlite3.connect

    def traced(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite3, "connect", traced)
    result = count(store)
    assert result["status"] == "stored" and result["scope"] == "stored_excerpts"
    assert result["matchedMessageCount"] == result["scannedMessageCount"] == 1
    assert result["matchedExpressions"] == ["씨발"]
    assert not any(statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE", "CREATE")) for statement in statements)
    with store._connection() as connection:
        assert not connection.execute("SELECT name FROM sqlite_master WHERE name LIKE 'memory_count_%'").fetchall()
        assert connection.execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0] == 0


def test_only_stored_excerpt_is_classified_and_empty_excerpt_stays_unavailable(store):
    source_id = add_source(store, excerpt="내일 회의야", raw="씨발 내일 회의야")
    add_item(store, source_id, text="씨발 회의 준비")
    add_source(store, excerpt="", raw="씨발 빈 발췌의 원문", stamp="1792100002.000001")
    add_source(store, excerpt='"씨발"이라는 단어의 뜻', stamp="1792100003.000001")
    result = count(store)
    assert result["eligibleStoredMessageCount"] == 3 and result["scannedMessageCount"] == 2
    assert result["matchedMessageCount"] == 0 and result["ambiguousMessageCount"] == 1
    assert result["unavailableExcerptMessageCount"] == 1
    assert result["matchedExpressions"] == []


def test_multiple_memories_and_repeated_terms_count_one_source_message(store):
    source_id = add_source(store, excerpt="씨발 존나 피곤해 씨발")
    add_item(store, source_id, name="item1")
    add_item(store, source_id, name="item2")
    result = count(store)
    assert result["eligibleStoredMessageCount"] == result["scannedMessageCount"] == result["matchedMessageCount"] == 1
    assert result["matchedExpressions"] == ["씨발", "존나"]


def test_expressions_use_actual_safe_spellings_and_union_across_valid_messages(store):
    add_source(store, excerpt="씨발 존나 피곤해")
    add_source(store, excerpt="씨발 지랄하지 마", stamp="1792100002.000001")
    # 인용-only의 표현은 목록에 넣지 않고 실제 확인한 표기만 중복 없이 정렬한다.
    add_source(store, excerpt='"개새끼"라는 단어의 뜻', stamp="1792100003.000001")
    result = count(store)
    assert result["matchedMessageCount"] == 2 and result["ambiguousMessageCount"] == 1
    assert result["matchedExpressions"] == ["씨발", "존나", "지랄"]


def test_conflicting_current_revisions_are_one_unavailable_message(store):
    add_source(store)
    add_source(store, excerpt="회의는 내일이야", edited="1792100002.000001")
    add_source(store, excerpt="세 번째 충돌도 임의로 채택하지 않아", edited="1792100003.000001")
    result = count(store)
    assert result["eligibleStoredMessageCount"] == result["unavailableExcerptMessageCount"] == 1
    assert result["scannedMessageCount"] == result["matchedMessageCount"] == 0
    assert result["matchedExpressions"] == []


def test_normal_scope_filters_tenant_author_channel_bot_and_noncurrent_sources(store):
    add_source(store)
    add_source(store, excerpt="지랄", tenant="T2")
    add_source(store, excerpt="개새끼", channel="COTHER")
    add_source(store, excerpt="존나", author="UOTHER", stamp="1792100002.000001")
    add_source(store, excerpt="병신", state="superseded", stamp="1792100003.000001")
    add_source(store, excerpt="좆", author_kind="boxer", stamp="1792100004.000001")
    result = count(store)
    assert result["eligibleStoredMessageCount"] == 1 and result["matchedExpressions"] == ["씨발"]
    empty = count(store, channel="CEMPTY")
    assert empty["eligibleStoredMessageCount"] == 0 and empty["matchedExpressions"] == []


def test_fixed_hub_only_extends_to_same_tenant_private_channels(store):
    tenant, hub = "T19PA3EPL", "C068FVD5V7Y"
    add_source(store, tenant=tenant, channel=hub)
    add_source(store, excerpt="존나", tenant=tenant, channel="CPRIVATE")
    add_source(store, excerpt="지랄", tenant=tenant, channel="CPUBLIC", kind="channel")
    add_source(store, excerpt="개새끼", tenant=tenant, channel="D1", kind="dm")
    add_source(store, excerpt="병신", tenant=tenant, channel="G1", kind="group_dm")
    add_source(store, excerpt="좆", tenant="OTHER", channel="CPRIVATE")
    result = count(store, tenant=tenant, channel=hub)
    assert result["eligibleStoredMessageCount"] == 2 and result["channelCount"] == 2
    assert result["matchedExpressions"] == ["씨발", "존나"]
    assert count(store, tenant="OTHER", channel=hub)["eligibleStoredMessageCount"] == 0


def test_period_has_precise_kst_floor_exclusive_end_and_original_question_cutoff(store):
    for stamp in ("1767193199.999999", "1767193200.000000", CUTOFF, "1792670000.000002", "1798729200.000000"):
        add_source(store, stamp=stamp)
    add_source(store, stamp="1792100002.000001", edited="1792670000.000002")
    result = count(store)
    assert result["eligibleStoredMessageCount"] == result["matchedMessageCount"] == 2
    ended = {**PERIOD, "endTs": CUTOFF}
    assert count(store, period=ended)["eligibleStoredMessageCount"] == 1


def test_expressions_from_out_of_period_or_later_edited_sources_are_not_returned(store):
    add_source(store)
    add_source(store, excerpt="지랄", stamp="1767193199.999999")
    add_source(store, excerpt="병신", stamp="1792670000.000002")
    add_source(store, excerpt="개새끼", stamp="1798729200.000000")
    add_source(store, excerpt="존나", stamp="1792100002.000001", edited="1792670000.000002")
    assert count(store)["matchedExpressions"] == ["씨발"]


@pytest.mark.parametrize("period", [
    {}, {**PERIOD, "startTs": "1767193199.999999"}, {**PERIOD, "startTs": "not-a-date"},
    {**PERIOD, "endTs": PERIOD["startTs"]}, {**PERIOD, "cutoffTs": "not-a-date"},
    {**PERIOD, "cutoffTs": "1767193199.999999"},
])
def test_unsupported_or_invalid_period_never_queries_database(store, monkeypatch, period):
    monkeypatch.setattr(store, "_connection", lambda **_: pytest.fail("기간 검증 전에 DB를 읽으면 안 돼"))
    with pytest.raises(MemoryError):
        count(store, period=period)


@pytest.mark.parametrize("field", ["tenant", "channel", "author"])
def test_missing_scope_is_rejected_before_read(store, field):
    with pytest.raises(MemoryError, match="scope_invalid"):
        count(store, **{field: ""})


@pytest.mark.parametrize("event_type", ["changed", "deleted", "unavailable"])
def test_existing_source_events_remove_stored_excerpt_from_the_next_result(store, event_type):
    add_source(store)
    assert count(store)["matchedMessageCount"] == 1
    store.source_event(tenant_id=TENANT, channel_id=CHANNEL, message_ts=STAMP,
                       event_type=event_type, event_id="source-change", revision_hash="a" * 64)
    assert count(store)["matchedMessageCount"] == count(store)["eligibleStoredMessageCount"] == 0
    assert count(store)["matchedExpressions"] == []


@pytest.mark.parametrize("kind", ["source", "source_redaction", "source_revision"])
def test_deletion_journal_alone_excludes_an_older_current_source_record(store, kind):
    source_id = add_source(store)
    # 삭제 이력이 존재하면 옛 current 표시가 남아 있어도 다시 집계하지 않는다.
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,source_id,event_type,request_id,"
                           "deletion_kind,source_message_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                           ("evt1", TENANT, CHANNEL, source_id, "test", "event1", kind, STAMP, 0))
    assert count(store)["eligibleStoredMessageCount"] == 0
    assert count(store)["matchedExpressions"] == []


def test_count_sources_and_deletion_journal_share_a_read_snapshot(store, monkeypatch):
    add_source(store)
    connect = sqlite3.connect
    raced = False

    with ThreadPoolExecutor(max_workers=1) as executor:
        def trace(statement):
            nonlocal raced
            if not raced and statement.startswith("SELECT s.channel_id,s.message_ts"):
                raced = True
                # journal을 읽은 뒤 별도 연결에서 삭제를 완료해도 출처는 같은 snapshot에서 읽는다.
                executor.submit(store.source_event, tenant_id=TENANT, channel_id=CHANNEL,
                                message_ts=STAMP, event_type="deleted", event_id="concurrent-delete").result(timeout=5)

        def traced(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connection.set_trace_callback(trace)
            return connection

        monkeypatch.setattr(sqlite3, "connect", traced)
        observed = count(store)
        assert observed["matchedMessageCount"] == 1 and raced
        assert observed["matchedExpressions"] == ["씨발"]
    current = count(store)
    assert current["eligibleStoredMessageCount"] == 0 and current["matchedExpressions"] == []


def test_stored_item_read_needs_db_integrity_but_verified_read_still_needs_external_receipt(store):
    candidate = add_item(store, add_source(store))
    args = {"tenant_id": TENANT, "channel_id": CHANNEL, "actor_id": "UREADER", "candidates": [candidate]}
    assert store.load_stored_items(**args) == [candidate]
    assert store.load_verified_items(**args, verified_sources=[]) == []
    receipts = [{"status": "verified", "sourceId": ref["sourceId"], "revisionHash": ref["revisionHash"]}
                for ref in candidate["sources"]]
    assert store.load_verified_items(**args, verified_sources=receipts) == [candidate]


@pytest.mark.parametrize("field", ["version", "deletionEpoch", "sources"])
def test_stored_item_read_rejects_changed_version_epoch_or_source_digest(store, field):
    candidate = add_item(store, add_source(store))
    stale = deepcopy(candidate)
    if field == "sources":
        stale["sources"][0]["revisionHash"] = "a" * 64
    else:
        stale[field] += 1
    assert store.load_stored_items(tenant_id=TENANT, channel_id=CHANNEL, candidates=[stale]) == []


def test_stored_item_read_keeps_tenant_channel_and_source_change_boundaries(store):
    candidate = add_item(store, add_source(store))
    assert store.load_stored_items(tenant_id="T2", channel_id=CHANNEL, candidates=[candidate]) == []
    assert store.load_stored_items(tenant_id=TENANT, channel_id="COTHER", candidates=[candidate]) == []
    store.source_event(tenant_id=TENANT, channel_id=CHANNEL, message_ts=STAMP,
                       event_type="deleted", event_id="delete-before-load")
    assert store.load_stored_items(tenant_id=TENANT, channel_id=CHANNEL, candidates=[candidate]) == []


def test_stored_personal_style_can_only_follow_its_own_actor_across_channels(store):
    from tests.company.test_memory_hub_store import publish

    publish(store, "CPRIVATE", preference=True)
    candidates = store.retrieval_candidates("T19PA3EPL", "CDEST", "UDD", "other-thread", "답변 선호")
    assert candidates and candidates[0]["styleOnly"] is True
    args = {"tenant_id": "T19PA3EPL", "channel_id": "CDEST", "candidates": candidates}
    assert store.load_stored_items(**args, actor_id="UDD") == [
        {"kind": "personal_style", "topicKey": "response_length", "value": "short"}]
    assert store.load_stored_items(**args, actor_id="UOTHER") == []
    assert store.load_stored_items(**args) == []


def test_forgetting_one_item_preserves_other_confirmed_facts_from_the_same_source(store):
    source_id = add_source(store)
    add_item(store, source_id, name="forget-this")
    add_item(store, source_id, name="keep-this", text="백업 담당이야")
    store.forget_item(tenant_id=TENANT, channel_id=CHANNEL, actor_id=AUTHOR,
                      memory_id="forget-this", request_id="forget-one-item")
    with store._connection() as connection:
        row = connection.execute("SELECT * FROM memory_items WHERE id='keep-this'").fetchone()
        remaining = store._item(connection, row)
    # 발췌의 재사용은 막아도 같은 출처를 공유하는 다른 확정 사실까지 잊지는 않는다.
    assert count(store)["eligibleStoredMessageCount"] == 0
    assert store.load_stored_items(tenant_id=TENANT, channel_id=CHANNEL, candidates=[remaining]) == [remaining]
