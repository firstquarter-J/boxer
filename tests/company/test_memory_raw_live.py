"""지속 원문 이벤트의 영수증, 수정과 삭제 순서를 실제 SQLite로 검증한다."""

from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from boxer_company.memory import raw_live
from boxer_company.memory.contracts import (
    MemoryError,
    canonical_digest,
    source_revision,
)
from boxer_company.memory.raw_policy import (
    RAW_ONLY_CUTOFF_TS,
    RAW_ONLY_START_TS,
    RAW_ONLY_TENANT_ID,
)
from boxer_company.memory.store import MemoryStore, initialize_memory_store

TENANT, CHANNEL, AUTHOR = RAW_ONLY_TENANT_ID, "C0621TL2HSB", "UTEST1"
MESSAGE, THREAD = "1792100001.000001", "1760000000.000001"
EVENT = "1792100002.000001"
TEXT = "  합성 원문은 공백까지 보존해\n"


@pytest.fixture
def store(tmp_path, monkeypatch):
    # 외부 권한 파일을 읽지 않고 서버 팀원 목록만 합성 값으로 대체한다.
    monkeypatch.setattr(raw_live, "TEAM_MEMBER_PROFILES", ({"user_id": AUTHOR}, {"user_id": "UTEST2"}))
    return MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db"), clock=lambda: 1_800_000_000.0)


def source(text=TEXT, *, author=AUTHOR, edited="", thread=THREAD, message=MESSAGE):
    return {"authorId": author, "authorKind": "human", "messageTs": message, "threadTs": thread,
            "text": text, "editedAt": edited, "createdAt": message, "revisionHash": source_revision(text, edited),
            "permalink": "https://offline.slack.com/archives/" + CHANNEL + "/p" + message.replace(".", "")}


def ingest(store, **changes):
    values = {"tenant_id": TENANT, "channel_id": CHANNEL, "message_ts": MESSAGE, "actor_id": AUTHOR,
              "event_id": "event1", "event_ts": EVENT, "event_type": "created", "source": source()}
    return raw_live.ingest_raw_event(store, **{**values, **changes})


def text_rows(store):
    with store._connection() as connection:
        return [row[0] for row in connection.execute("SELECT text FROM memory_source_texts")]


def watermark(store):
    with store._connection() as connection:
        return dict(connection.execute("SELECT * FROM memory_raw_event_states").fetchone())


def test_created_event_stores_exact_raw_text_without_jobs_summaries_or_body_receipts(store):
    result = ingest(store)
    assert result == {"status": "stored", "eventId": "event1", "messageTs": MESSAGE, "stored": True}
    assert text_rows(store) == [TEXT] and TEXT not in json.dumps(result, ensure_ascii=False)
    with store._connection() as connection:
        for table in ("memory_jobs", "memory_job_inputs", "memory_items", "memory_summaries"):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
        receipt = connection.execute("SELECT * FROM memory_events").fetchone()
        assert receipt["id"] == "evt_raw_" + canonical_digest([TENANT, "event1"])
        assert receipt["event_type"] == "raw_ingest" and receipt["durability_state"] == "not_required"
        assert receipt["request_id"] == "event1" and len(receipt["reason_code"]) == 64
        provenance = connection.execute("SELECT * FROM memory_raw_imports").fetchone()
        assert provenance["id"] == "live_" + canonical_digest([TENANT, "event1"])
        assert provenance["start_ts"] == RAW_ONLY_START_TS and provenance["cutoff_ts"] == RAW_ONLY_CUTOFF_TS
        assert provenance["author_id"] == AUTHOR and provenance["state"] == "completed"
        assert connection.execute("SELECT excerpt FROM memory_sources").fetchone()[0] == ""
    assert watermark(store)["state"] == "current"


def test_duplicate_is_idempotent_and_event_identity_rejects_changed_body_or_channel(store):
    ingest(store)
    assert ingest(store)["status"] == "duplicate"
    with pytest.raises(MemoryError, match="identity_conflict"):
        ingest(store, source=source("다른 합성 원문"))
    second = source()
    second["permalink"] = second["permalink"].replace(CHANNEL, "C068FVD5V7Y")
    with pytest.raises(MemoryError, match="identity_conflict"):
        ingest(store, channel_id="C068FVD5V7Y", source=second)
    with store._connection() as connection:
        for table in ("memory_raw_imports", "memory_source_texts", "memory_sources", "memory_raw_event_states", "memory_events"):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 1


def test_edit_replaces_raw_revision_and_old_events_cannot_replay(store):
    ingest(store)
    edited = source("수정된 합성 원문", edited="1792100003.000001")
    assert ingest(store, event_id="edit", event_type="changed", event_ts="1792100004.000001", source=edited)["stored"]
    assert ingest(store, event_id="late-old", event_ts="1792100002.000002")["status"] == "stale"
    assert text_rows(store) == [edited["text"]]
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_sources WHERE state='current'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM memory_events WHERE deletion_kind='source_revision'").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM memory_raw_imports").fetchone()[0] == 2


def test_edit_received_before_creation_preserves_newer_revision(store):
    edited = source("먼저 도착한 수정본", edited="1792100003.000001")
    ingest(store, event_id="edit-first", event_type="changed", event_ts="1792100004.000001", source=edited)
    assert ingest(store)["status"] == "stale"
    assert text_rows(store) == [edited["text"]]


@pytest.mark.parametrize("first", [True, False])
def test_delete_is_permanent_even_when_it_arrives_before_creation(store, first):
    if not first:
        ingest(store)
    deleted = ingest(store, event_id="deleted", event_type="deleted", event_ts="1792100005.000001", source=None, actor_id=None)
    assert deleted["status"] == "deleted" and not deleted["stored"]
    assert ingest(store, event_id="late-new", event_ts="1792100010.000001")["status"] == "stale"
    assert not text_rows(store) and watermark(store)["state"] == "deleted"
    with store._connection() as connection:
        event = connection.execute("SELECT * FROM memory_events WHERE deletion_kind='source'").fetchone()
        assert event["source_message_id"] == MESSAGE and event["durability_state"] == "pending"


def test_same_timestamp_delete_wins_and_other_conflicts_cannot_replace_text(store):
    ingest(store)
    assert ingest(store, event_id="same-time-change", event_type="changed", source=source("같은 시각의 다른 본문"))["status"] == "stale"
    assert text_rows(store) == [TEXT]
    assert ingest(store, event_id="same-time-delete", event_type="deleted", source=None, actor_id=None)["status"] == "deleted"
    assert not text_rows(store)


@pytest.mark.parametrize("first_type", ["created", "changed"])
def test_lost_success_response_then_actorless_unavailable_at_same_time_erases_raw(store, monkeypatch, first_type):
    initial = source(edited=MESSAGE) if first_type == "changed" else source()
    assert ingest(store, event_type=first_type, source=initial)["stored"]
    # 응답 유실 후 작성자 권한이 사라져도 원문 없는 철회 이벤트는 기존 저장 시각과 같게 처리한다.
    monkeypatch.setattr(raw_live, "TEAM_MEMBER_PROFILES", ())
    unavailable = {"event_id": "privacy-unavailable", "event_type": "unavailable", "actor_id": None, "source": None}
    assert ingest(store, **unavailable)["status"] == "unavailable"
    assert not text_rows(store) and watermark(store)["state"] == "unavailable"
    assert ingest(store, **unavailable)["status"] == "duplicate"
    monkeypatch.setattr(raw_live, "TEAM_MEMBER_PROFILES", ({"user_id": AUTHOR},))
    assert ingest(store, event_id="late-original", event_type=first_type, source=initial)["status"] == "stale"
    assert not text_rows(store)


@pytest.mark.parametrize("previous", ["current", "unavailable", "deleted"])
@pytest.mark.parametrize("incoming", ["current", "unavailable", "deleted"])
def test_same_time_state_priority_is_deleted_then_unavailable_then_current(store, previous, incoming):
    def apply(mode, identity):
        return ingest(store, event_id=identity, event_type="created" if mode == "current" else mode,
                      source=source() if mode == "current" else None, actor_id=AUTHOR if mode == "current" else None)
    apply(previous, "first-priority")
    result = apply(incoming, "second-priority")
    ranks = {"current": 0, "unavailable": 1, "deleted": 2}
    winner = incoming if ranks[incoming] > ranks[previous] else previous
    assert watermark(store)["state"] == winner
    assert bool(text_rows(store)) == (winner == "current")
    assert result["status"] == (incoming if ranks[incoming] > ranks[previous] else "stale")


def test_unsafe_edit_erases_old_body_but_a_later_safe_edit_is_allowed(store):
    ingest(store)
    unsafe = source("장비 로그 조회해줘", edited="1792100003.000001")
    result = ingest(store, event_id="unsafe", event_type="changed", event_ts="1792100004.000001", source=unsafe)
    assert result["status"] == "unavailable" and not text_rows(store)
    assert watermark(store)["state"] == "unavailable"
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_events WHERE deletion_kind='source_redaction'").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_events WHERE deletion_kind='source_revision'").fetchone()[0] == 2
    safe = source("나중에 수정한 안전한 합성 원문", edited="1792100005.000001")
    assert ingest(store, event_id="safe-again", event_type="changed", event_ts="1792100006.000001", source=safe)["stored"]
    assert text_rows(store) == [safe["text"]]
    assert ingest(store, event_id="old-revision-new-event", event_ts="1792100010.000001")["status"] == "stale"
    assert text_rows(store) == [safe["text"]]


def test_unsafe_creation_is_not_stored_or_sent_to_extraction(store):
    assert ingest(store, source=source("비밀번호는 저장하지 않아"))["status"] == "unavailable"
    assert not text_rows(store)
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0] == 0


@pytest.mark.parametrize("text", ["", " \n"])
def test_empty_edited_body_invalidates_old_raw_instead_of_retaining_its_old_content(store, text):
    ingest(store)
    assert ingest(store, event_id="empty-edit", event_type="changed", event_ts="1792100005.000001",
                  source=source(text, edited="1792100004.000001"))["status"] == "unavailable"
    assert not text_rows(store)


def test_late_unsafe_edit_cannot_erase_newer_snapshot_without_a_live_watermark(store):
    edited = source("더 최신 snapshot 합성 원문", edited="1792100010.000001")
    ingest(store, event_ts="1792100011.000001", source=edited)
    with store._connection(write=True) as connection:
        connection.execute("DELETE FROM memory_raw_event_states")
    unsafe = source("장비 로그 조회해줘", edited="1792100004.000001")
    assert ingest(store, event_id="older-unsafe", event_type="changed", event_ts="1792100005.000001", source=unsafe)["status"] == "stale"
    assert text_rows(store) == [edited["text"]]


def test_unavailable_without_actor_invalidates_body_and_blocks_old_events(store):
    ingest(store)
    assert ingest(store, event_id="missing", event_type="unavailable", event_ts="1792100005.000001", source=None, actor_id=None)["status"] == "unavailable"
    assert not text_rows(store)
    assert ingest(store, event_id="old-retry")["status"] == "stale"


def test_future_edit_uses_event_cutoff_while_provenance_keeps_october_message_scope(store):
    ingest(store)
    edited = source("11월에 편집한 10월 발언", edited="1793550000.000001")
    assert ingest(store, event_id="november-edit", event_type="changed", event_ts="1793550001.000001", source=edited)["stored"]
    assert text_rows(store) == [edited["text"]]
    with store._connection() as connection:
        assert {row[0] for row in connection.execute("SELECT cutoff_ts FROM memory_raw_imports")} == {RAW_ONLY_CUTOFF_TS}


@pytest.mark.parametrize("change", [
    {"actor_id": "UTEST2"}, {"actor_id": "UOUTSIDE", "source": source(author="UOUTSIDE")},
    {"actor_id": None}, {"source": None}, {"source": "invalid"},
    {"event_type": "other"}, {"event_type": []}, {"tenant_id": "OTHER"}, {"channel_id": "COTHER"},
    {"event_ts": "NaN"}, {"event_ts": "1792100000.000001"},
    {"message_ts": "1793458800.000000"},
])
def test_invalid_scope_or_actor_never_writes(store, change):
    with pytest.raises(MemoryError):
        ingest(store, **change)
    with store._connection() as connection:
        for table in ("memory_sources", "memory_source_texts", "memory_raw_imports", "memory_events", "memory_raw_event_states"):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


@pytest.mark.parametrize("field,value", [
    ("revisionHash", "f" * 64), ("sourceId", "src_forged"), ("authorKind", "boxer"),
    ("permalink", "https://offline.slack.com/archives/COTHER/p1792100001000001"),
    ("messageTs", "1792100002.000001"), ("createdAt", "1792100000.000001"),
    ("editedAt", "1792100099.000001"), ("threadTs", "1792100099.000001"),
    ("text", 1), ("text", "가" * 40_001), ("deliveryVerified", True),
])
def test_invalid_source_never_writes_or_erases_existing_raw(store, field, value):
    ingest(store)
    forged = source()
    forged[field] = value
    with pytest.raises(MemoryError):
        ingest(store, event_id="forged", event_type="changed", event_ts="1792100005.000001", source=forged)
    assert text_rows(store) == [TEXT]


@pytest.mark.parametrize("unsafe", [False, True])
def test_actor_or_thread_forgery_cannot_erase_another_authors_body(store, unsafe):
    ingest(store)
    forged = source("장비 로그 조회해줘" if unsafe else "조작한 합성 원문", author="UTEST2", edited="1792100003.000001")
    with pytest.raises(MemoryError, match="identity_conflict"):
        ingest(store, actor_id="UTEST2", event_id="forged-owner", event_type="changed", event_ts="1792100005.000001", source=forged)
    forged = source("장비 로그 조회해줘" if unsafe else "다른 스레드", thread="1760000001.000001")
    with pytest.raises(MemoryError, match="identity_conflict"):
        ingest(store, event_id="forged-thread", event_type="changed", event_ts="1792100005.000001", source=forged)
    assert text_rows(store) == [TEXT]


def test_raw_text_preserves_40000_characters_and_original_spacing(store):
    text = " " + "가" * 39_998 + "\n"
    assert ingest(store, source=source(text))["stored"]
    assert text_rows(store) == [text]


def test_optional_source_identity_is_derived_and_unthreaded_message_defaults_to_its_own_timestamp(store):
    value = source()
    value.pop("threadTs")
    value.pop("createdAt")
    value["sourceId"] = None
    assert ingest(store, source=value)["stored"]
    with store._connection() as connection:
        assert connection.execute("SELECT thread_ts FROM memory_threads").fetchone()[0] == MESSAGE


def test_receipt_failure_rolls_back_body_deletion_watermark_and_journal(store):
    ingest(store)
    with store._connection(write=True) as connection:
        connection.execute("CREATE TRIGGER reject_receipt BEFORE INSERT ON memory_events WHEN NEW.event_type='raw_ingest' BEGIN SELECT RAISE(ABORT,'synthetic'); END")
    with pytest.raises(sqlite3.IntegrityError):
        ingest(store, event_id="delete-aborted", event_type="deleted", event_ts="1792100005.000001", source=None, actor_id=None)
    assert text_rows(store) == [TEXT] and watermark(store)["state"] == "current"
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_events").fetchone()[0] == 1


def test_text_insert_failure_rolls_back_revision_invalidation_and_provenance(store):
    ingest(store)
    with store._connection(write=True) as connection:
        connection.execute("CREATE TRIGGER reject_text BEFORE INSERT ON memory_source_texts BEGIN SELECT RAISE(ABORT,'synthetic'); END")
    edited = source("수정 실패할 합성 원문", edited="1792100003.000001")
    with pytest.raises(sqlite3.IntegrityError):
        ingest(store, event_id="edit-aborted", event_type="changed", event_ts="1792100005.000001", source=edited)
    assert text_rows(store) == [TEXT] and watermark(store)["event_ts"] == EVENT
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_raw_imports").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 1


def test_concurrent_create_and_edit_keep_newest_revision(store):
    edited = source("동시에 도착한 최신 합성 원문", edited="1792100003.000001")
    with ThreadPoolExecutor(max_workers=2) as executor:
        created = executor.submit(ingest, store)
        changed = executor.submit(ingest, store, event_id="concurrent-edit", event_type="changed", event_ts="1792100005.000001", source=edited)
        created.result(timeout=5)
        changed.result(timeout=5)
    assert text_rows(store) == [edited["text"]]
    assert watermark(store)["event_ts"] == "1792100005.000001"


def test_revoked_snapshot_source_cannot_reappear_with_new_live_event(store):
    ingest(store)
    with store._connection(write=True) as connection:
        stored = connection.execute("SELECT * FROM memory_sources").fetchone()
        store._record_revision_invalidation(connection, stored, "revoked-snapshot")
        store._invalidate_source_items(connection, TENANT, stored["id"], deleted=False)
    assert ingest(store, event_id="revive-attempt", event_ts="1792100010.000001")["status"] == "stale"
    assert not text_rows(store)


@pytest.mark.parametrize("event_type,state", [("changed", "current"), ("unavailable", "unavailable"), ("deleted", "deleted")])
def test_mutations_journal_only_canonical_watermark_metadata_for_snapshot_replay(store, event_type, state):
    ingest(store)
    edited = source("journal에는 넣지 않는 새 합성 본문", edited="1792100004.000001")
    values = {"event_id": "journal-event", "event_type": event_type, "event_ts": "1792100005.000001",
              "source": edited if event_type == "changed" else None, "actor_id": AUTHOR if event_type == "changed" else None}
    ingest(store, **values)
    assert ingest(store, **values)["status"] == "duplicate"
    with store._connection() as connection:
        events = connection.execute("SELECT * FROM memory_events WHERE deletion_kind='raw_state'").fetchall()
        assert len(events) == 1
        event = events[0]
        assert event["id"] == "evt_raw_state_" + canonical_digest([TENANT, "journal-event"])
        assert event["request_id"] == "journal-event" and event["durability_state"] == "pending"
        assert event["source_message_id"] == MESSAGE
        expected = {"eventTs": "1792100005.000001", "state": state,
                    "revisionHash": edited["revisionHash"] if event_type == "changed" else source()["revisionHash"]}
        assert event["reason_code"] == json.dumps(expected, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        assert edited["text"] not in event["reason_code"]


def test_raw_watermark_journal_failure_rolls_back_text_revision_and_receipt(store):
    ingest(store)
    with store._connection(write=True) as connection:
        connection.execute("CREATE TRIGGER reject_raw_state BEFORE INSERT ON memory_events WHEN NEW.deletion_kind='raw_state' BEGIN SELECT RAISE(ABORT,'synthetic'); END")
    with pytest.raises(sqlite3.IntegrityError):
        ingest(store, event_id="journal-aborted", event_type="changed", event_ts="1792100005.000001",
               source=source("rollback할 합성 편집본", edited="1792100004.000001"))
    assert text_rows(store) == [TEXT] and watermark(store)["event_ts"] == EVENT
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_raw_imports").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM memory_events").fetchone()[0] == 1
