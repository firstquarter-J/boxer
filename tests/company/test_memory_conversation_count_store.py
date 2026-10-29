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


def add_item(store, source_id, *, text="회의 담당이야", name="item1", topic="회의 담당",
             span=None, status="active", valid_until=None):
    # 실제 채택 인용과 달리 요약 text는 집계 입력으로 쓰면 안 된다.
    with store._connection(write=True) as connection:
        source = connection.execute("SELECT * FROM memory_sources WHERE id=?", (source_id,)).fetchone()
        connection.execute("INSERT INTO memory_items(id,tenant_id,scope_channel_id,origin_thread_id,kind,category,subject_user_id,"
                           "topic_key,text,evidence_type,valid_from,observed_at,updated_at,extractor_version,status,valid_until) "
                           "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (name, source["tenant_id"], source["channel_id"], source["thread_id"], "person", "fact", source["author_id"],
                            topic, text, "self_statement", 0, 0, 0, "test", status, valid_until))
        connection.execute("INSERT INTO memory_item_sources(tenant_id,memory_id,source_id,relation,evidence_span) VALUES(?,?,?,'supports',?)",
                           (source["tenant_id"], name, source_id, source["excerpt"] if span is None else span))
        return store._item(connection, connection.execute("SELECT * FROM memory_items WHERE id=?", (name,)).fetchone())


def count(store, *, tenant=TENANT, channel=CHANNEL, author=AUTHOR, period=None, include_messages=False):
    return count_stored_messages(store, tenant_id=tenant, channel_id=channel, author_id=author,
                                 period=PERIOD if period is None else period, include_messages=include_messages)


@pytest.mark.parametrize("include_messages", [False, True])
def test_count_reads_existing_tables_without_creating_state_or_writing_source_text(store, monkeypatch, include_messages):
    add_source(store)
    statements = []
    connect = sqlite3.connect

    def traced(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(sqlite3, "connect", traced)
    result = count(store, include_messages=include_messages)
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


def test_message_details_are_opt_in_and_only_quote_actual_matched_stored_excerpt(store):
    excerpt = "씨발 존나 회의 피곤하다"
    source_id = add_source(store, excerpt=excerpt, raw="원문 전문은 저장하지 않아")
    add_item(store, source_id, text="요약의 별도 표현인 지랄은 인용하지 않아", topic="회의")
    add_source(store, excerpt='"개새끼"라는 단어의 뜻', stamp="1792100002.000001")
    plain = count(store)
    result = count(store, include_messages=True)
    assert {key: value for key, value in result.items()
            if key not in {"matchedMessages", "messageLimit", "messagesTruncated"}} == plain
    assert result["matchedMessages"] == [{"messageTs": STAMP, "excerpt": excerpt,
                                         "matchedExpressions": ["씨발", "존나"], "topic": "회의"}]
    assert result["messageLimit"] == 20 and result["messagesTruncated"] is False


def test_details_keep_decimal_time_and_separate_same_time_private_channels_in_stable_order(store):
    tenant, hub = "T19PA3EPL", "C068FVD5V7Y"
    # 삽입 순서와 문자열 정렬 대신 정확한 시각, 원채널, 출처 ID 순서를 사용한다.
    for channel, stamp, excerpt in (("CZ", STAMP, "씨발 늦은 채널"),
                                    ("CA", "1792100000.999999999", "존나 앞선 시각"),
                                    ("CA", STAMP, "지랄 같은 시각")):
        add_source(store, tenant=tenant, channel=channel, stamp=stamp, excerpt=excerpt)
    result = count(store, tenant=tenant, channel=hub, include_messages=True)
    assert [item["excerpt"] for item in result["matchedMessages"]] == [
        "존나 앞선 시각", "지랄 같은 시각", "씨발 늦은 채널"]
    assert result["matchedMessageCount"] == 3 and result["messagesTruncated"] is False


def test_details_limit_is_applied_after_valid_matches_and_preserves_full_count(store):
    for index in reversed(range(22)):
        add_source(store, stamp=f"179210{index:04d}.000001", excerpt=f"씨발 회의 {index}")
    result = count(store, include_messages=True)
    assert result["matchedMessageCount"] == 22 and result["messageLimit"] == 20
    assert len(result["matchedMessages"]) == 20 and result["messagesTruncated"] is True
    assert [item["messageTs"] for item in result["matchedMessages"]] == [
        f"179210{index:04d}.000001" for index in range(20)]


def test_unsafe_excerpt_is_omitted_from_details_without_changing_counts_and_safe_excerpt_is_bounded(store):
    add_source(store, excerpt="씨발 비밀번호는 여기에 쓰지 마")
    excerpt = "존나 피곤하다 " + "가" * 1100
    add_source(store, excerpt=excerpt, stamp="1792100002.000001")
    plain = count(store)
    result = count(store, include_messages=True)
    assert result["matchedMessageCount"] == plain["matchedMessageCount"] == 2
    assert len(result["matchedMessages"]) == 1 and result["messagesTruncated"] is True
    assert result["matchedMessages"][0]["excerpt"] == excerpt[:1000]


def test_long_excerpt_is_omitted_when_the_display_prefix_cannot_prove_every_matched_expression(store):
    add_source(store, excerpt="씨발 " + "가" * 1000 + " 존나 피곤해")
    result = count(store, include_messages=True)
    assert result["matchedMessageCount"] == 1 and result["matchedExpressions"] == ["씨발", "존나"]
    assert result["matchedMessages"] == [] and result["messagesTruncated"] is True


@pytest.mark.parametrize("status,until,expected", [
    ("active", None, "회의"), ("resolved", None, "회의"), ("active", float(CUTOFF) + 1, "회의"),
    ("active", float(CUTOFF), None), ("expired", None, None), ("conflicted", None, None),
    ("observing", None, None), ("deleted", None, None),
])
def test_topic_only_uses_current_active_or_resolved_unexpired_items(store, status, until, expected):
    add_item(store, add_source(store), topic="회의", status=status, valid_until=until)
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] == expected


@pytest.mark.parametrize("span,topic", [
    ("내일 회의야", "회의"), ("씨발 저장되지 않은 문장", "회의"), ("", "회의"),
    ('"씨발"이라는 단어의 뜻', "회의"), ("씨발 피곤하다", "비밀번호"), ("씨발 피곤하다", "가" * 161),
])
def test_topic_requires_safe_matched_evidence_span_inside_the_actual_stored_excerpt(store, span, topic):
    source = add_source(store, excerpt='씨발 피곤하다\n내일 회의야\n"씨발"이라는 단어의 뜻')
    add_item(store, source, topic=topic, span=span, text="씨발 요약만으로 주제를 연결하지 않아")
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] is None


def test_topic_requires_one_unique_candidate_even_when_multiple_items_share_the_source(store):
    source = add_source(store)
    add_item(store, source, name="item1", topic="회의")
    add_item(store, source, name="item2", topic="회의")
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] == "회의"
    add_item(store, source, name="item3", topic="퇴근")
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] is None


def link_topic_source(store, source_id, *, name="item1", span=None):
    with store._connection(write=True) as connection:
        source = connection.execute("SELECT * FROM memory_sources WHERE id=?", (source_id,)).fetchone()
        connection.execute("INSERT INTO memory_item_sources(tenant_id,memory_id,source_id,relation,evidence_span) "
                           "VALUES(?,?,?,'supports',?)", (source["tenant_id"], name, source_id,
                                                        source["excerpt"] if span is None else span))


@pytest.mark.parametrize("other", [
    {"channel": "COTHER"}, {"state": "deleted"}, {"state": "superseded"}, {"author_kind": "boxer"},
    {"stamp": "1767193199.999999"}, {"stamp": "1792670000.000002"}, {"stamp": "1798729200.000000"},
    {"edited": "1792670000.000002"}, {"excerpt": ""}, {"excerpt": "비밀번호는 쓰지 않아"},
    {"stamp": "invalid-timestamp"},
])
def test_topic_is_rejected_when_any_other_related_source_breaks_scope_period_or_current_boundary(store, other):
    source = add_source(store)
    add_item(store, source, topic="회의")
    related = add_source(store, **{"excerpt": "내일 회의야", "stamp": "1792100002.000001",
                                 "author": "UOTHER", **other})
    link_topic_source(store, related)
    result = count(store, include_messages=True)
    assert result["matchedMessageCount"] == 1 and result["matchedMessages"][0]["topic"] is None


@pytest.mark.parametrize("kind", ["source", "source_redaction", "source_revision"])
def test_topic_cannot_reappear_through_another_source_with_a_journal_only_deletion(store, kind):
    add_item(store, add_source(store), topic="회의")
    related_stamp = "1792100002.000001"
    related = add_source(store, excerpt="내일 회의야", stamp=related_stamp, author="UOTHER")
    link_topic_source(store, related)
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,source_id,event_type,request_id,"
                           "deletion_kind,source_message_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                           ("evt1", TENANT, CHANNEL, related, "test", "event1", kind, related_stamp, 0))
    result = count(store, include_messages=True)
    assert result["matchedMessageCount"] == 1 and result["matchedMessages"][0]["topic"] is None


@pytest.mark.parametrize("kind", ["item", "subject_topic"])
def test_topic_honors_item_and_subject_topic_deletion_journal_without_state_replay(store, kind):
    add_item(store, add_source(store), topic="회의 담당")
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,memory_id,event_type,request_id,"
                           "deletion_kind,subject_id,topic_key_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                           ("evt1", TENANT, CHANNEL, "item1", "test", "event1", kind, AUTHOR,
                            canonical_digest("회의담당"), 0))
    result = count(store, include_messages=True)
    assert result["matchedMessageCount"] == 1 and result["matchedMessages"][0]["topic"] is None


def test_topic_rejects_another_source_with_conflicting_current_decimal_revisions(store):
    add_item(store, add_source(store), topic="회의")
    related = add_source(store, excerpt="내일 회의야", stamp="1792100002.000001", author="UOTHER")
    link_topic_source(store, related)
    # 문자열은 달라도 같은 Decimal 시각의 충돌 revision이면 주제에 사용할 수 없다.
    add_source(store, excerpt="회의는 취소야", stamp="1792100002.0000010", author="UOTHER")
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] is None


def test_topic_revision_conflict_uses_the_same_final_journal_filter_as_message_counts(store):
    add_item(store, add_source(store), topic="회의")
    related = add_source(store, excerpt="내일 회의야", stamp="1792100002.000001", author="UOTHER")
    link_topic_source(store, related)
    old = add_source(store, excerpt="옛 회의 문장", stamp="1792100002.0000010", author="UOTHER")
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,source_id,event_type,request_id,"
                           "deletion_kind,created_at) VALUES(?,?,?,?,?,?,?,?)",
                           ("evt1", TENANT, CHANNEL, old, "test", "event1", "source_revision", 0))
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] == "회의"


def test_topic_does_not_cross_item_scope_or_a_corrupt_cross_tenant_source_link(store):
    source = add_source(store)
    add_item(store, add_source(store, channel="COTHER", excerpt="회의는 내일이야"), topic="다른 채널 주제")
    link_topic_source(store, source)
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] is None
    add_item(store, source, name="same-scope", topic="회의")
    foreign = add_source(store, tenant="T2", excerpt="다른 회사 근거", author="UOTHER")
    # 정상 FK로 만들 수 없는 옛 손상 관계도 tenant JOIN에서 누락시켜 주제만 거절한다.
    with sqlite3.connect(store.path) as connection:
        connection.execute("INSERT INTO memory_item_sources(tenant_id,memory_id,source_id,relation,evidence_span) "
                           "VALUES(?,?,?,'supports',?)", (TENANT, "same-scope", foreign, "다른 회사 근거"))
    assert count(store, include_messages=True)["matchedMessages"][0]["topic"] is None


@pytest.mark.parametrize("kind,expected", [("private_channel", "회의"), ("channel", None), ("dm", None), ("group_dm", None)])
def test_hub_topic_checks_every_related_source_channel_kind(store, kind, expected):
    tenant, hub = "T19PA3EPL", "C068FVD5V7Y"
    add_item(store, add_source(store, tenant=tenant, channel=hub), topic="회의")
    related = add_source(store, tenant=tenant, channel="COTHER", kind=kind, author="UOTHER", excerpt="회의는 내일이야")
    link_topic_source(store, related)
    assert count(store, tenant=tenant, channel=hub, include_messages=True)["matchedMessages"][0]["topic"] == expected


def test_details_are_generated_after_conflicts_and_journal_deletions_are_final(store):
    add_source(store)
    add_source(store, excerpt="씨발 다른 revision", edited="1792100002.000001")
    deleted = add_source(store, excerpt="존나 피곤하다", stamp="1792100003.000001")
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,source_id,event_type,request_id,"
                           "deletion_kind,source_message_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                           ("evt1", TENANT, CHANNEL, deleted, "test", "event1", "source", "1792100003.000001", 0))
    result = count(store, include_messages=True)
    assert result["matchedMessageCount"] == 0 and result["matchedMessages"] == []
    assert result["messagesTruncated"] is False


def test_topic_relations_sources_and_journal_use_one_read_only_snapshot(store, monkeypatch):
    add_item(store, add_source(store), topic="회의")
    related = add_source(store, excerpt="회의는 내일이야", stamp="1792100002.000001", author="UOTHER")
    link_topic_source(store, related)
    connect = sqlite3.connect
    statements, raced = [], False
    with ThreadPoolExecutor(max_workers=1) as executor:
        def trace(statement):
            nonlocal raced
            statements.append(statement)
            if not raced and statement.startswith("SELECT i.id AS memory_id"):
                raced = True
                executor.submit(store.source_event, tenant_id=TENANT, channel_id=CHANNEL,
                                message_ts="1792100002.000001", event_type="deleted",
                                event_id="topic-concurrent-delete").result(timeout=5)

        def traced(*args, **kwargs):
            connection = connect(*args, **kwargs)
            connection.set_trace_callback(trace)
            return connection

        monkeypatch.setattr(sqlite3, "connect", traced)
        assert count(store, include_messages=True)["matchedMessages"][0]["topic"] == "회의"
    assert raced and count(store, include_messages=True)["matchedMessages"][0]["topic"] is None
    # 동시 삭제 writer 이외에 조회 자신은 쓰기나 요약 text 조회를 만들지 않는다.
    assert not any("i.text" in statement for statement in statements)
