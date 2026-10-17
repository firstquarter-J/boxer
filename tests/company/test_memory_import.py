from copy import deepcopy

import pytest

from boxer_company.memory.contracts import MemoryError, canonical_digest, source_revision
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api.memory_import import enqueue_history_import


def payload():
    source = {"messageTs":"1790773200.000001", "authorId":"U1", "authorKind":"human",
              "text":"앞으로 짧은 답변을 선호해", "createdAt":"1790773200.000001",
              "permalink":"https://example.slack.com/archives/C1/p1790773200000001",
              "revisionHash":source_revision("앞으로 짧은 답변을 선호해")}
    chunks = [{"chunkNo":0,"sources":[source],"chunkDigest":canonical_digest([source])}]
    threads = [{"threadTs":source["messageTs"],"cutoffTs":"1790866800.000000","coverage":"complete","chunks":chunks}]
    return {"schemaVersion":1,"kind":"slack_history_import","importId":"imp_test",
            "tenantId":"T1","channelId":"C1","channelKind":"private_channel",
            "period":{"fromKst":"2026-10-01T00:00:00+09:00","throughKst":"2026-10-02T00:00:00+09:00",
                      "oldestTs":"1790780400.000000","cutoffTs":"1790866800.000000"},
            "authorIds":["U1"],"coverage":"complete","coverageNotes":[],"threads":threads,"inputDigest":canonical_digest(threads)}


@pytest.fixture
def memory_store(tmp_path):
    folder = tmp_path / "private"
    folder.mkdir(mode=0o700)
    return MemoryStore(initialize_memory_store(folder / "memory.db"), debounce_seconds=0)


def valid_payload():
    value = payload()
    source = value["threads"][0]["chunks"][0]["sources"][0]
    source["messageTs"] = source["createdAt"] = "1790780400.000001"
    source["permalink"] = "https://example.slack.com/archives/C1/p1790780400000001"
    value["threads"][0]["threadTs"] = source["messageTs"]
    return rehash(value)


def rehash(value):
    for thread in value["threads"]:
        for chunk in thread["chunks"]:
            chunk["chunkDigest"] = canonical_digest(chunk["sources"])
    value["inputDigest"] = canonical_digest(value["threads"])
    return value


def enqueue(store, value, **kwargs):
    return enqueue_history_import(store, value, tenant_id="T1",channel_id="C1",operator_id="UOP",
                                  authorized_author_ids=kwargs.get("authors", {"U1"}))


def test_history_import_is_separate_from_answer_delivery_and_idempotent(memory_store):
    # 가짜 봇 답변이나 발송 영수증 없이 역사적 출처만 한 번 적재한다.
    value = valid_payload()
    first = enqueue(memory_store, value)
    second = enqueue(memory_store, deepcopy(value))
    assert first == second
    assert first["states"] == {"ready":1}
    with memory_store._connection() as connection:
        assert connection.execute("SELECT count(*) FROM memory_jobs").fetchone()[0] == 1
        assert connection.execute("SELECT count(*) FROM memory_sources").fetchone()[0] == 1
        assert 'history_import' in connection.execute("SELECT metadata FROM memory_jobs").fetchone()[0]


@pytest.mark.parametrize("corruption", ["author", "period", "digest", "permalink", "bot", "duplicate"])
def test_import_rejects_scope_corruption_before_any_write(memory_store, corruption):
    value = valid_payload()
    source = value["threads"][0]["chunks"][0]["sources"][0]
    if corruption == "author": source["authorId"] = "U2"
    elif corruption == "period": source["messageTs"] = "1790700000.000001"
    elif corruption == "permalink": source["permalink"] = "https://example.slack.com/archives/COTHER/p1790780400000001"
    elif corruption == "bot": source["authorKind"] = "boxer"
    elif corruption == "duplicate": value["threads"].append(deepcopy(value["threads"][0]))
    rehash(value)
    if corruption == "digest": value["inputDigest"] = "a" * 64
    with pytest.raises(MemoryError): enqueue(memory_store, value)
    with memory_store._connection() as connection:
        assert connection.execute("SELECT count(*) FROM memory_jobs").fetchone()[0] == 0


def test_changed_payload_cannot_reuse_an_import_identity(memory_store):
    value = valid_payload()
    enqueue(memory_store, value)
    source = value["threads"][0]["chunks"][0]["sources"][0]
    source["text"] = "앞으로 자세한 답변을 선호해"
    source["revisionHash"] = source_revision(source["text"])
    with pytest.raises(MemoryError): enqueue(memory_store, rehash(value))


@pytest.mark.parametrize("event_type", ["changed", "deleted", "unavailable"])
def test_import_does_not_restore_revoked_source_to_temporary_model_payload(memory_store, event_type):
    # 과거 export를 새 import ID로 재접수해도 철회 원문은 LLM 입력에 다시 나타나면 안 된다.
    value = valid_payload()
    enqueue(memory_store, value)
    origin = value["threads"][0]["chunks"][0]["sources"][0]
    memory_store.source_event(tenant_id="T1", channel_id="C1", message_ts=origin["messageTs"],
                              event_type=event_type, event_id="source-event")
    value["importId"] = "reimport_old_export"
    result = enqueue(memory_store, value)
    assert result["coverage"] == "partial"
    with memory_store._connection() as connection:
        rows = connection.execute("SELECT payload FROM memory_job_inputs").fetchall()
        assert all(row["payload"] == "[]" for row in rows)
        assert all(origin["text"] not in row["payload"] for row in rows)


def test_import_identity_binds_whole_thread_manifest(memory_store):
    value = valid_payload()
    enqueue(memory_store, value)
    another = deepcopy(value["threads"][0])
    origin = another["chunks"][0]["sources"][0]
    origin["messageTs"] = origin["createdAt"] = "1790780401.000001"
    origin["permalink"] = "https://example.slack.com/archives/C1/p1790780401000001"
    another["threadTs"] = origin["messageTs"]
    value["threads"].append(another)
    with pytest.raises(MemoryError, match="manifest_conflict"):
        enqueue(memory_store, rehash(value))
    with memory_store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0] == 1


def test_terminal_import_replay_reports_skip_without_reviving_deleted_capture(memory_store):
    value = valid_payload()
    enqueue(memory_store, value)
    origin = value["threads"][0]["chunks"][0]["sources"][0]
    memory_store.source_event(tenant_id="T1", channel_id="C1", message_ts=origin["messageTs"],
                              event_type="deleted", event_id="delete")
    result = enqueue(memory_store, value)
    assert result["states"] == {"terminal_skipped": 1}
    with memory_store._connection() as connection:
        assert connection.execute("SELECT state FROM memory_jobs").fetchone()[0] == "canceled"
