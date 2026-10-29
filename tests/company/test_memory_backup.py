from __future__ import annotations

import hashlib
import io
import json
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from boxer_company.memory.backup import (
    MemoryBackup,
    MemoryBackupError,
    MemoryBackupSettings,
)
from boxer_company.memory.contracts import (
    canonical_digest,
    source_revision,
)
from boxer_company.memory.store import MemoryStore, initialize_memory_store


class S3Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
        self.gets = []
        self.deleted = []

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        existing = self.objects.get(key)
        if kwargs.get("IfNoneMatch") == "*" and existing is not None:
            raise S3Error("PreconditionFailed")
        if kwargs.get("IfMatch") is not None and (not existing or existing["ETag"] != kwargs["IfMatch"]):
            raise S3Error("PreconditionFailed")
        self.puts.append(kwargs)
        self.objects[key] = {"data": kwargs["Body"], "ETag": hashlib.sha256(kwargs["Body"]).hexdigest(),
                             "ServerSideEncryption": kwargs["ServerSideEncryption"],
                             "LastModified": datetime.now(timezone.utc)}
        return {}

    def get_object(self, **kwargs):
        self.gets.append(kwargs)
        if kwargs["Key"] not in self.objects:
            raise S3Error("NoSuchKey")
        obj = self.objects[kwargs["Key"]]
        return {**obj, "Body": io.BytesIO(obj["data"])}

    def list_objects_v2(self, **kwargs):
        return {"Contents": [{"Key": key, "LastModified": value["LastModified"]} for key, value in self.objects.items() if key.startswith(kwargs["Prefix"])]}

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs["Key"])
        self.objects.pop(kwargs["Key"], None)


def backup():
    client = FakeS3()
    result = MemoryBackup(MemoryBackupSettings("bucket", "memory", "kms-key"), client=client)
    result.initialize_journal()
    return result, client


def archive_source_text(store, text):
    # 별도 원문도 기존 source와 import의 tenant FK로 묶어 snapshot에 포함한다.
    with store._connection(write=True) as connection:
        source = connection.execute("SELECT * FROM memory_sources WHERE tenant_id='T1' LIMIT 1").fetchone()
        connection.execute("INSERT INTO memory_raw_imports VALUES(?,?,?,?,?,?,?,?,?,?)",
                           ("raw1", "T1", source["channel_id"], source["author_id"], "UOP", source["message_ts"],
                            source["message_ts"], canonical_digest("raw1"), "completed", store._clock()))
        connection.execute("INSERT INTO memory_source_texts VALUES(?,?,?,?,?)",
                           ("T1", source["id"], "raw1", text, store._clock()))


def event(event_id="delete-1"):
    return {"id": event_id, "tenantId": "T", "scopeChannelId": "C", "deletionKind": "subject_topic",
            "subjectId": "U", "topicKeyHash": "topic-hash", "cutoffAt": "2026-10-16T00:00:00+00:00"}


def test_deletion_chain_is_encrypted_idempotent_and_has_no_original_text():
    service, client = backup()
    first = {**event(), "text": "민감한 옛 기억"}
    assert service.append_deletion(first) == 1
    assert service.append_deletion(first) == 1
    assert service.append_deletion(event("delete-2")) == 2
    head, entries = service.verify_journal()
    assert head["sequence"] == 2
    assert entries[1]["previousHash"] == entries[0]["hash"]
    assert all(p["ServerSideEncryption"] == "aws:kms" and p["SSEKMSKeyId"] == "kms-key" for p in client.puts)
    assert "민감한 옛 기억" not in json.dumps(entries, ensure_ascii=False)


def test_missing_head_does_not_silently_initialize_new_journal():
    service, client = backup()
    client.objects.pop("memory/journal/head.json")
    with pytest.raises(MemoryBackupError, match="head_unavailable"):
        service.append_deletion(event())


@pytest.mark.parametrize("change", ["missing", "tamper", "unencrypted"])
def test_restore_blocks_missing_tampered_or_unencrypted_deletion_chain(change):
    service, client = backup()
    service.append_deletion(event())
    key = next(k for k in client.objects if "/events/" in k)
    if change == "missing":
        del client.objects[key]
    elif change == "tamper":
        client.objects[key]["data"] = b'{}'
    else:
        client.objects[key]["ServerSideEncryption"] = "AES256"
    with pytest.raises(MemoryBackupError):
        service.verify_journal()


def test_flush_marks_durable_only_after_verified_remote_head():
    service, client = backup()
    marks = []
    store = SimpleNamespace(deletion_events=lambda: [event()], mark_deletion_durable=lambda *a: marks.append(a))
    assert service.flush_deletions(store) == 1
    assert marks == [("delete-1", 1)]
    client.objects.pop("memory/journal/head.json")
    with pytest.raises(MemoryBackupError):
        service.flush_deletions(store)
    assert len(marks) == 1


def test_snapshot_restore_replays_deletions_and_discards_pending_inputs(tmp_path):
    service, _ = backup()
    tmp_path.chmod(0o700)
    original = SimpleNamespace(deletion_events=list, snapshot=initialize_memory_store)
    metadata = service.create_snapshot(original)
    service.append_deletion(event())
    operations = []

    def factory(path):
        assert MemoryStore(path).health()
        return SimpleNamespace(
            apply_deletion_event=lambda e: operations.append(("delete", e["id"])),
            mark_deletion_durable=lambda *args: operations.append(("durable", args)),
            cleanup=lambda **kwargs: operations.append(("cleanup", kwargs)),
            snapshot=lambda target: Path(target).write_bytes(b"deletion reapplied"),
        )

    target = tmp_path / "restored.db"
    result = service.restore_snapshot(metadata["key"], target, store_factory=factory)
    assert result["deletionSequence"] == 1
    assert target.read_bytes() == b"deletion reapplied"
    assert operations[0] == ("delete", "delete-1")
    assert operations[-1] == ("cleanup", {"restored": True})
    assert target.stat().st_mode & 0o777 == 0o600


def test_restore_never_overwrites_existing_database(tmp_path):
    service, _ = backup()
    target = tmp_path / "live.db"
    target.write_bytes(b"live")
    with pytest.raises(MemoryBackupError, match="destination_invalid"):
        service.restore_snapshot("memory/snapshots/20261016T000000Z-" + "0" * 32 + ".db", target, store_factory=lambda _: None)
    assert target.read_bytes() == b"live"


def test_snapshot_pruning_does_not_delete_journal_or_unknown_objects():
    service, client = backup()
    service.append_deletion(event())
    old = datetime.now(timezone.utc) - timedelta(days=40)
    for key in ["memory/snapshots/20260901T000000Z-" + "a" * 32 + ".db", "memory/snapshots/do-not-delete.txt"]:
        client.objects[key] = {"data": b"", "LastModified": old}
    for value in client.objects.values():
        value["LastModified"] = old
    assert service.prune_snapshots() == 1
    assert all("/journal/" not in key for key in client.deleted)
    assert "memory/snapshots/do-not-delete.txt" in client.objects


def test_pinned_botocore_injects_conditions_before_request_signing(monkeypatch):
    import boto3
    from botocore.awsrequest import AWSResponse

    # 실제 HTTP 전송 대신 endpoint를 교체해 설치된 SDK의 serializer와 event를 검증한다.
    client = boto3.client("s3", region_name="ap-northeast-2", aws_access_key_id="test", aws_secret_access_key="test")
    requests = []

    def send(operation_model, request, context):
        requests.append(request)
        return AWSResponse(request["url"], 200, {}, None), {}

    monkeypatch.setattr(client, "_make_request", send)
    service = MemoryBackup(MemoryBackupSettings("bucket", "memory", "kms-key"), client=client)
    service._put("memory/test", b"a", create=True)
    service._put("memory/test", b"b", expected_etag='"etag"')
    assert requests[0]["headers"]["If-None-Match"] == "*"
    assert "If-Match" not in requests[0]["headers"]
    assert requests[1]["headers"]["If-Match"] == '"etag"'
    assert "If-None-Match" not in requests[1]["headers"]


@pytest.mark.parametrize("removal", ["forget", "source_delete", "source_change"])
def test_real_sqlite_restore_replays_later_deletions_without_resurrection(tmp_path, removal):
    tmp_path.chmod(0o700)
    service, _ = backup()
    store = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    text = "앞으로 짧은 답변을 선호해"
    item = store.set_own_preference(
        tenant_id="T1", channel_id="C1", actor_id="U1", thread_ts="1000.000001", current_message_ts="1000.000001",
        request_id="remember", topic_key="response_length", text="짧은 답변 선호",
        source={"messageTs": "1000.000001", "authorId": "U1", "authorKind": "human", "text": text,
                "revisionHash": source_revision(text), "permalink": "https://test.slack.com/archives/C1/p1000000001"},
    )
    archive_source_text(store, text)
    snapshot = service.create_snapshot(store)
    if removal == "forget":
        store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U1", memory_id=item["memoryId"], request_id="forget")
    else:
        store.source_event(tenant_id="T1", channel_id="C1", message_ts="1000.000001", event_type="deleted" if removal == "source_delete" else "changed", event_id="source-event")
    assert service.flush_deletions(store) == (2 if removal == "forget" else 1)
    restored = tmp_path / "restored.db"
    service.restore_snapshot(snapshot["key"], restored, store_factory=MemoryStore)
    restored_store = MemoryStore(restored)
    assert restored_store.list_own_memories("T1", "C1", "U1") == []
    assert restored_store.retrieval_candidates("T1", "C1", "U1", "1000.000001", "선호") == []
    with sqlite3.connect(restored) as db:
        assert db.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM memory_sources WHERE excerpt!=''").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM memory_items WHERE text!=''").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM memory_source_texts").fetchone()[0] == 0


def test_restore_does_not_remove_another_process_destination_on_create_race(tmp_path):
    service, _ = backup()
    tmp_path.chmod(0o700)
    metadata = service.create_snapshot(SimpleNamespace(deletion_events=list, snapshot=initialize_memory_store))
    target = tmp_path / "race.db"

    def racing_snapshot(path):
        path.write_bytes(b"another process owns this")
        raise FileExistsError()

    factory = lambda _: SimpleNamespace(cleanup=lambda **k: None, snapshot=racing_snapshot)
    with pytest.raises(FileExistsError):
        service.restore_snapshot(metadata["key"], target, store_factory=factory)
    assert target.read_bytes() == b"another process owns this"


def test_restore_refuses_to_publish_when_deletion_head_advances_during_restore(tmp_path):
    service, _ = backup()
    tmp_path.chmod(0o700)
    metadata = service.create_snapshot(SimpleNamespace(deletion_events=list, snapshot=initialize_memory_store))
    target = tmp_path / "stale.db"

    def racing_snapshot(path):
        path.write_bytes(b"stale content")
        service.append_deletion(event())

    factory = lambda _: SimpleNamespace(cleanup=lambda **k: None, snapshot=racing_snapshot)
    with pytest.raises(MemoryBackupError, match="journal_changed"):
        service.restore_snapshot(metadata["key"], target, store_factory=factory)
    assert not target.exists()


def test_old_revision_snapshot_cannot_reappear_after_multiple_source_edits(tmp_path):
    tmp_path.chmod(0o700)
    service, _ = backup()
    store = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    scope = {"tenant_id": "T1", "channel_id": "C1", "actor_id": "U1", "thread_ts": "1000.000001",
             "current_message_ts": "1000.000001", "topic_key": "response_length"}
    first = "앞으로 짧게 답해줘"
    origin = {"messageTs": "1000.000001", "authorId": "U1", "authorKind": "human", "text": first,
              "revisionHash": source_revision(first), "permalink": "https://test.slack.com/archives/C1/p1000000001"}
    store.set_own_preference(**scope, request_id="remember1", text="짧은 답변 선호", source=origin)
    snapshot = service.create_snapshot(store)
    edited_text = "앞으로 자세하게 답해줘"
    edited = {**origin, "text": edited_text, "editedAt": "1001.000001", "revisionHash": source_revision(edited_text, "1001.000001")}
    store.set_own_preference(**scope, request_id="remember2", text="자세한 답변 선호", source=edited)
    store.source_event(tenant_id="T1", channel_id="C1", message_ts="1000.000001", event_type="changed", event_id="changed-again")
    assert service.flush_deletions(store) >= 2
    restored = tmp_path / "restored.db"
    service.restore_snapshot(snapshot["key"], restored, store_factory=MemoryStore)
    assert MemoryStore(restored).list_own_memories("T1", "C1", "U1") == []
    with sqlite3.connect(restored) as db:
        assert db.execute("SELECT COUNT(*) FROM memory_items WHERE text!=''").fetchone()[0] == 0


def test_operator_cli_deletes_exact_item_and_reports_remote_durability(tmp_path, monkeypatch, capsys):
    from boxer_company_api import memory_cli

    tmp_path.chmod(0o700)
    service, _ = backup()
    path = initialize_memory_store(tmp_path / "memory.db")
    store = MemoryStore(path)
    text = "앞으로 답변은 짧게 해줘"
    item = store.set_own_preference(
        tenant_id="T1", channel_id="C1", actor_id="U1", thread_ts="1000.000001", current_message_ts="1000.000001",
        request_id="remember", topic_key="response_length", text="짧은 답변 선호",
        source={"messageTs": "1000.000001", "authorId": "U1", "authorKind": "human", "text": text,
                "revisionHash": source_revision(text), "permalink": "https://test.slack.com/archives/C1/p1000000001"},
    )
    monkeypatch.setattr(memory_cli, "configured_memory_backup", lambda: service)
    memory_cli.main(["delete-item", "--path", str(path), "--tenant-id", "T1", "--channel-id", "C1",
                     "--memory-id", item["memoryId"], "--request-id", "operator-delete"])
    output = json.loads(capsys.readouterr().out)
    assert output["state"] == "hidden" and output["durability"] == "durable"
    assert store.list_own_memories("T1", "C1", "U1") == []
    assert service.verify_journal()[0]["sequence"] == 2
    assert all(event["durabilityState"] == "durable" for event in store.deletion_events())


def test_redaction_survives_snapshot_from_before_forgotten_item_was_created(tmp_path):
    tmp_path.chmod(0o700)
    service, _ = backup()
    store = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    text = "앞으로 답변은 짧게 해줘"
    original = {"messageTs": "1000.000001", "authorId": "U1", "authorKind": "human", "text": text,
                "revisionHash": source_revision(text), "permalink": "https://test.slack.com/archives/C1/p1000000001"}
    scope = {"tenant_id": "T1", "actor_id": "U1", "channel_id": "C1", "thread_ts": "1000.000001"}
    capture = store.create_capture(**scope, current_message_ts="1000.000001", channel_kind="channel", request_id="first-capture", route="freeform")
    store.put_chunk(tenant_id="T1", actor_id="U1", capture_id=capture["captureId"], claim_token=capture["claimToken"],
                    chunk_no=0, sources=[original], is_last=True, total_chunks=1, input_digest=canonical_digest([canonical_digest([original])]))
    # snapshot에는 source·대기 원문만 있고 이후 잊을 memory ID는 아직 없다.
    snapshot = service.create_snapshot(store)
    item = store.set_own_preference(**scope, current_message_ts="1000.000001", request_id="remember",
                                    topic_key="response_length", text="짧은 답변 선호", source=original)
    store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U1", memory_id=item["memoryId"], request_id="forget")
    assert service.flush_deletions(store) == 2
    restored_path = tmp_path / "restored.db"
    service.restore_snapshot(snapshot["key"], restored_path, store_factory=MemoryStore)
    restored = MemoryStore(restored_path)
    capture = restored.create_capture(**scope, current_message_ts="1002.000001", channel_kind="channel", request_id="recollect", route="freeform")
    new = {**original, "messageTs": "1002.000001", "text": "점심 메뉴 알려줘", "revisionHash": source_revision("점심 메뉴 알려줘"),
           "permalink": "https://test.slack.com/archives/C1/p1002000001"}
    sources = [original, new]
    restored.put_chunk(tenant_id="T1", actor_id="U1", capture_id=capture["captureId"], claim_token=capture["claimToken"],
                       chunk_no=0, sources=sources, is_last=True, total_chunks=1, input_digest=canonical_digest([canonical_digest(sources)]))
    with sqlite3.connect(restored.path) as db:
        payloads = [row[0] for row in db.execute("SELECT payload FROM memory_job_inputs")]
        assert payloads and all(text not in payload for payload in payloads)
        assert db.execute("SELECT COUNT(*) FROM memory_events WHERE deletion_kind='source_redaction'").fetchone()[0] == 1


@pytest.mark.parametrize("legacy_version", [1, 2])
def test_legacy_snapshot_is_migrated_only_in_staging_before_deletion_replay(tmp_path, legacy_version):
    tmp_path.chmod(0o700)
    service, client = backup()
    store = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    text = "앞으로 짧게 답해줘"
    item = store.set_own_preference(tenant_id="T1", channel_id="C1", actor_id="U1", thread_ts="1000.000001",
        current_message_ts="1000.000001", request_id="remember", topic_key="response_length", text="짧은 답변 선호",
        source={"messageTs": "1000.000001", "authorId": "U1", "authorKind": "human", "text": text,
                "revisionHash": source_revision(text), "permalink": "https://test.slack.com/archives/C1/p1000000001"})

    def legacy_snapshot(path):
        store.snapshot(path)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute("DROP TABLE memory_raw_event_states")
            if legacy_version == 1:
                connection.execute("DROP TABLE memory_source_texts")
                connection.execute("DROP TABLE memory_raw_imports")
            connection.execute(f"PRAGMA user_version={legacy_version}")

    metadata = service.create_snapshot(SimpleNamespace(deletion_events=store.deletion_events, snapshot=legacy_snapshot))
    original_bytes = client.objects[metadata["key"]]["data"]
    store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U1", memory_id=item["memoryId"], request_id="forget")
    service.flush_deletions(store)
    observed = {}

    def factory(path):
        with sqlite3.connect(path) as connection:
            observed["version"] = connection.execute("PRAGMA user_version").fetchone()[0]
            observed["items"] = connection.execute("SELECT COUNT(*) FROM memory_items WHERE text!=''").fetchone()[0]
            assert connection.execute("SELECT COUNT(*) FROM memory_source_texts").fetchone()[0] == 0
        return MemoryStore(path)

    target = tmp_path / "restored.db"
    service.restore_snapshot(metadata["key"], target, store_factory=factory)
    assert observed == {"version": 3, "items": 1}
    assert client.objects[metadata["key"]]["data"] == original_bytes
    with sqlite3.connect(target) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert connection.execute("SELECT COUNT(*) FROM memory_items WHERE text!=''").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_events WHERE deletion_kind IS NOT NULL").fetchone()[0] == 2


def test_snapshot_hash_failure_prevents_schema_migration_or_store_factory(tmp_path, monkeypatch):
    from boxer_company.memory import backup as module

    tmp_path.chmod(0o700)
    service, client = backup()
    store = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    metadata = service.create_snapshot(store)
    client.objects[metadata["key"]]["data"] += b"changed"
    monkeypatch.setattr(module, "initialize_memory_store", lambda _: pytest.fail("hash 검증 전에 migration하면 안 돼"))
    with pytest.raises(MemoryBackupError, match="snapshot_hash_mismatch"):
        service.restore_snapshot(metadata["key"], tmp_path / "restored.db",
                                 store_factory=lambda _: pytest.fail("hash 검증 전에 store를 열면 안 돼"))
    assert not list(tmp_path.glob("*.restore-*"))


def test_journal_validation_failure_prevents_schema_migration_or_store_factory(tmp_path, monkeypatch):
    from boxer_company.memory import backup as module

    tmp_path.chmod(0o700)
    service, client = backup()
    metadata = service.create_snapshot(MemoryStore(initialize_memory_store(tmp_path / "original.db")))
    client.objects.pop("memory/journal/head.json")
    monkeypatch.setattr(module, "initialize_memory_store", lambda _: pytest.fail("journal 검증 전에 migration하면 안 돼"))
    with pytest.raises(MemoryBackupError, match="head_unavailable"):
        service.restore_snapshot(metadata["key"], tmp_path / "restored.db",
                                 store_factory=lambda _: pytest.fail("journal 검증 전에 store를 열면 안 돼"))
    assert not list(tmp_path.glob("*.restore-*"))


@pytest.mark.parametrize("version", [0, 999])
def test_unsupported_snapshot_schema_fails_without_publishing_or_leaking_staging(tmp_path, version):
    tmp_path.chmod(0o700)
    service, _ = backup()

    def unknown_snapshot(path):
        initialize_memory_store(path)
        with closing(sqlite3.connect(path)) as connection:
            connection.execute(f"PRAGMA user_version={version}")

    metadata = service.create_snapshot(SimpleNamespace(deletion_events=list, snapshot=unknown_snapshot))
    target = tmp_path / "restored.db"
    with pytest.raises(MemoryBackupError, match="schema_unsupported"):
        service.restore_snapshot(metadata["key"], target,
                                 store_factory=lambda _: pytest.fail("알 수 없는 schema는 열면 안 돼"))
    assert not target.exists() and not list(tmp_path.glob("*.restore-*"))


def test_raw_snapshot_unknown_edit_tombstone_survives_restore_before_any_source_exists(tmp_path):
    tmp_path.chmod(0o700)
    service, _ = backup()
    store = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_raw_imports VALUES(?,?,?,?,?,?,?,?,?,?)",
                           ("raw1", "T1", "C1", "U1", "UOP", "1000.000001", "1001.000001",
                            canonical_digest("raw1"), "collecting", store._clock()))
    snapshot = service.create_snapshot(store)
    store.source_event(tenant_id="T1", channel_id="C1", message_ts="1000.000001", event_type="changed", event_id="before-source")
    assert service.flush_deletions(store) == 1
    target = tmp_path / "restored.db"
    service.restore_snapshot(snapshot["key"], target, store_factory=MemoryStore)
    restored = MemoryStore(target)
    with restored._connection() as connection:
        assert not connection.execute("SELECT * FROM memory_sources").fetchall()
        assert restored._source_deleted(connection, "T1", "C1", "1000.000001")


@pytest.mark.parametrize("snapshot_version", [2, 3])
def test_derived_purge_journal_replays_old_snapshot_without_deleting_raw_or_sources(tmp_path, snapshot_version):
    from boxer_company.memory.derived_purge import operator_purge_derived

    tmp_path.chmod(0o700)
    service, _ = backup()
    store = MemoryStore(initialize_memory_store(tmp_path / "original.db"))
    text = "앞으로 짧게 답해줘"
    item = store.set_own_preference(tenant_id="T1", channel_id="C1", actor_id="U1", thread_ts="1000.000001",
        current_message_ts="1000.000001", request_id="remember", topic_key="response_length", text="짧은 답변 선호",
        source={"messageTs": "1000.000001", "authorId": "U1", "authorKind": "human", "text": text,
                "revisionHash": source_revision(text), "permalink": "https://test.slack.com/archives/C1/p1000000001"})
    source_id = item["sources"][0]["sourceId"]
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_raw_imports VALUES('raw1','T1','C1','U1','UOP','1000.000001','1001.000001',?,'completed',0)", ("a" * 64,))
        connection.execute("INSERT INTO memory_source_texts VALUES('T1',?,'raw1',?,0)", (source_id, text))

    def snapshot(path):
        store.snapshot(path)
        if snapshot_version == 2:
            with closing(sqlite3.connect(path)) as connection:
                connection.execute("DROP TABLE memory_raw_event_states")
                connection.execute("PRAGMA user_version=2")

    old = service.create_snapshot(SimpleNamespace(deletion_events=store.deletion_events, snapshot=snapshot))
    arguments = {"tenant_id": "T1", "channel_ids": ["C1"], "operator_id": "UOP", "request_id": "derived1"}
    assert operator_purge_derived(store, **arguments)["purgedItemCount"] == 1
    assert service.flush_deletions(store) == 2
    target = tmp_path / "restored-derived.db"
    service.restore_snapshot(old["key"], target, store_factory=MemoryStore)
    restored = MemoryStore(target)
    with restored._connection() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert tuple(connection.execute("SELECT text,topic_key,subject_user_id,status FROM memory_items").fetchone()) == ("", "", None, "deleted")
        assert connection.execute("SELECT text FROM memory_source_texts").fetchone()[0] == text
        assert tuple(connection.execute("SELECT excerpt,state FROM memory_sources").fetchone()) == (text, "current")
        assert connection.execute("SELECT COUNT(*) FROM memory_events WHERE deletion_kind='source_redaction'").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_subjects").fetchone()[0] == 0
    assert operator_purge_derived(restored, **arguments)["durability"] == "durable"
    with pytest.raises(Exception, match="memory_derived_request_conflict"):
        operator_purge_derived(restored, **{**arguments, "channel_ids": ["C1", "C2"]})


def test_derived_journal_requires_fixed_scope_hash_and_never_contains_summary_text():
    service, client = backup()
    event = {"id": "derived", "tenantId": "T1", "scopeChannelId": "C1", "memoryId": "item1",
             "eventType": "derived_purge_item", "requestId": "request1", "deletionKind": "derived_item",
             "reasonCode": "a" * 64, "text": "지울 파생 요약"}
    assert service.append_deletion(event) == 1
    assert "지울 파생 요약" not in str(client.objects)
    with pytest.raises(MemoryBackupError, match="memory_deletion_event_invalid"):
        service.append_deletion({**event, "id": "bad-derived", "reasonCode": "not-a-hash"})


class PendingDeletionStore:
    def __init__(self, events):
        self.events = events
        self.marks = []

    def deletion_events(self):
        return self.events

    def mark_deletion_durable(self, identity, sequence):
        self.marks.append((identity, sequence))
        for row in self.events:
            if row["id"] == identity:
                row["durabilityState"] = "durable"


def batch_events(count=3):
    return [{**event(f"batch-{index}"), "durabilityState": "pending"} for index in range(count)]


def test_flush_511_pending_events_uses_one_head_commit_and_linear_s3_gets():
    service, client = backup()
    service.append_deletion(event("existing1"))
    service.append_deletion(event("existing2"))
    # 실제 적용과 같은 2개 scope marker와 509개 파생 item을 한 번에 내구화한다.
    pending = [{"id": f"purge-{index}", "tenantId": "T", "scopeChannelId": "C1" if index % 2 == 0 else "C2",
                "deletionKind": "derived_item", "reasonCode": "a" * 64, "durabilityState": "pending",
                **({"eventType": "derived_purge_scope", "memoryId": None, "expectedItemCount": 255 if index == 0 else 254}
                   if index < 2 else {"eventType": "derived_purge_item", "memoryId": f"item-{index}"})}
               for index in range(511)]
    store = PendingDeletionStore(pending)
    client.gets.clear()
    client.puts.clear()
    assert service.flush_deletions(store) == 511
    # 기존 2건을 두 번, 신규 511건을 한 번 읽고 고정 head 4회만 더 확인한다.
    assert len(client.gets) == 511 + 2 * 2 + 4
    assert len([row for row in client.puts if row["Key"] == "memory/journal/head.json"]) == 1
    assert len(client.puts) == 512
    assert store.marks == [(f"purge-{index}", index + 3) for index in range(511)]
    before = len(client.gets), len(client.puts)
    assert service.flush_deletions(store) == 0
    assert (len(client.gets), len(client.puts)) == before
    head, saved = service.verify_journal()
    assert head["sequence"] == 513 and len(saved) == 513


def test_batch_reuses_existing_event_sequence_and_deduplicates_identical_local_ids():
    service, client = backup()
    original = event("already-remote")
    service.append_deletion(original)
    pending = [dict(original), event("new"), dict(original)]
    store = PendingDeletionStore(pending)
    client.puts.clear()
    assert service.flush_deletions(store) == 2
    assert store.marks == [("already-remote", 1), ("new", 2)]
    assert len([row for row in client.puts if "/events/" in row["Key"]]) == 1
    assert service.verify_journal()[0]["sequence"] == 2


@pytest.mark.parametrize("existing_remote", [False, True])
def test_conflicting_event_id_fails_before_any_batch_upload_or_local_ack(existing_remote):
    service, client = backup()
    original = event("same-id")
    altered = {**original, "subjectId": "different-subject"}
    if existing_remote:
        service.append_deletion(original)
        pending = [event("new-before-conflict"), altered]
    else:
        pending = [event("new-before-conflict"), original, altered]
    store = PendingDeletionStore(pending)
    client.puts.clear()
    with pytest.raises(MemoryBackupError, match="memory_deletion_event_conflict"):
        service.flush_deletions(store)
    assert not client.puts and not store.marks
    assert all(row.get("durabilityState") != "durable" for row in store.events)


@pytest.mark.parametrize("stale_part", ["head", "etag"])
def test_batch_revalidates_head_and_etag_before_uploading_any_new_events(monkeypatch, stale_part):
    service, client = backup()
    stale = dict(client.objects["memory/journal/head.json"])
    service.append_deletion(event("concurrent-earlier"))
    get = client.get_object
    injected = False
    def read(**kwargs):
        nonlocal injected
        if kwargs["Key"] == "memory/journal/head.json" and not injected:
            injected = True
            if stale_part == "head":
                return {**stale, "Body": io.BytesIO(stale["data"])}
            return {**get(**kwargs), "ETag": "stale-etag"}
        return get(**kwargs)
    monkeypatch.setattr(client, "get_object", read)
    store = PendingDeletionStore([event("new")])
    assert service.flush_deletions(store) == 1
    assert store.marks == [("new", 2)]
    assert [row["event"]["id"] for row in service.verify_journal()[1]] == ["concurrent-earlier", "new"]


@pytest.mark.parametrize("competitor_reuses_id", [False, True])
def test_head_cas_race_rebuilds_batch_and_reuses_committed_ids_without_losing_either_writer(monkeypatch, competitor_reuses_id):
    service, client = backup()
    competitor = MemoryBackup(service.settings, client=client)
    put = client.put_object
    raced = False
    def racing_put(**kwargs):
        nonlocal raced
        if kwargs["Key"] == "memory/journal/head.json" and kwargs.get("IfMatch") and not raced:
            raced = True
            competitor.append_deletion(event("batch-0" if competitor_reuses_id else "competitor"))
        return put(**kwargs)
    monkeypatch.setattr(client, "put_object", racing_put)
    store = PendingDeletionStore(batch_events(2))
    assert service.flush_deletions(store) == 2
    head, saved = service.verify_journal()
    expected = ["batch-0", "batch-1"] if competitor_reuses_id else ["competitor", "batch-0", "batch-1"]
    assert head["sequence"] == len(expected)
    assert [row["event"]["id"] for row in saved] == expected
    assert store.marks == ([("batch-0", 1), ("batch-1", 2)] if competitor_reuses_id else [("batch-0", 2), ("batch-1", 3)])


def test_repeated_conditional_head_conflict_is_bounded_to_five_attempts_and_keeps_local_pending(monkeypatch):
    service, client = backup()
    put = client.put_object
    attempts = 0
    def conflict(**kwargs):
        nonlocal attempts
        if kwargs["Key"] == "memory/journal/head.json" and kwargs.get("IfMatch"):
            attempts += 1
            raise S3Error("ConditionalRequestConflict")
        return put(**kwargs)
    monkeypatch.setattr(client, "put_object", conflict)
    store = PendingDeletionStore(batch_events())
    with pytest.raises(MemoryBackupError, match="memory_journal_write_conflict"):
        service.flush_deletions(store)
    assert attempts == 5 and not store.marks
    assert service.verify_journal()[0]["sequence"] == 0


@pytest.mark.parametrize("failure", ["partial_upload", "upload_response_lost", "commit_response_lost", "verification_read_lost"])
def test_network_failure_never_marks_local_durable_and_restart_reuses_immutable_remote_events(monkeypatch, failure):
    service, client = backup()
    put, get = client.put_object, client.get_object
    injected = False
    committed = False
    uploads = 0
    def faulty_put(**kwargs):
        nonlocal injected, committed, uploads
        if "/events/" in kwargs["Key"]:
            uploads += 1
            if not injected and uploads == 2 and failure in {"partial_upload", "upload_response_lost"}:
                injected = True
                if failure == "upload_response_lost":
                    put(**kwargs)
                raise TimeoutError("synthetic upload response is unavailable")
        result = put(**kwargs)
        if kwargs["Key"] == "memory/journal/head.json" and kwargs.get("IfMatch"):
            committed = True
            if failure == "commit_response_lost" and not injected:
                injected = True
                raise TimeoutError("synthetic commit response is unavailable")
        return result
    def faulty_get(**kwargs):
        nonlocal injected
        if failure == "verification_read_lost" and committed and "/events/" in kwargs["Key"] and not injected:
            injected = True
            raise TimeoutError("synthetic verification response is unavailable")
        return get(**kwargs)
    monkeypatch.setattr(client, "put_object", faulty_put)
    monkeypatch.setattr(client, "get_object", faulty_get)
    store = PendingDeletionStore(batch_events())
    with pytest.raises((TimeoutError, MemoryBackupError)):
        service.flush_deletions(store)
    assert injected and not store.marks
    assert all(row["durabilityState"] == "pending" for row in store.events)
    assert service.flush_deletions(store) == 3
    assert store.marks == [("batch-0", 1), ("batch-1", 2), ("batch-2", 3)]
    assert [row["event"]["id"] for row in service.verify_journal()[1]] == ["batch-0", "batch-1", "batch-2"]


def test_immutable_event_collision_requires_identical_bytes_and_cannot_publish_corrupted_orphan():
    service, client = backup()
    payload = service._deletion_payload(event())
    entry = {"version": 1, "sequence": 1, "previousHash": "", "event": payload}
    key = "memory/journal/events/" + canonical_digest(entry) + ".json"
    client.objects[key] = {"data": b"{}", "ETag": "different", "ServerSideEncryption": "aws:kms"}
    store = PendingDeletionStore([event()])
    with pytest.raises(MemoryBackupError, match="memory_journal_event_conflict"):
        service.flush_deletions(store)
    assert not store.marks and service.verify_journal()[0]["sequence"] == 0


@pytest.mark.parametrize("failure", ["head_not_committed", "missing_remote_event", "verification_omits_event"])
def test_successful_transport_without_verified_full_batch_keeps_every_local_event_pending(monkeypatch, failure):
    service, client = backup()
    put, verify = client.put_object, service.verify_journal
    def incomplete_put(**kwargs):
        if kwargs["Key"] == "memory/journal/head.json" and kwargs.get("IfMatch"):
            if failure == "head_not_committed":
                return {}
            result = put(**kwargs)
            if failure == "missing_remote_event":
                key = next(key for key in client.objects if "/events/" in key)
                del client.objects[key]
            return result
        return put(**kwargs)
    verify_calls = 0
    def incomplete_verify():
        nonlocal verify_calls
        verify_calls += 1
        head, events = verify()
        return (head, events[:-1]) if failure == "verification_omits_event" and verify_calls == 2 else (head, events)
    monkeypatch.setattr(client, "put_object", incomplete_put)
    monkeypatch.setattr(service, "verify_journal", incomplete_verify)
    store = PendingDeletionStore(batch_events())
    with pytest.raises(MemoryBackupError, match="memory_journal_(?:write_unverified|event_unavailable)"):
        service.flush_deletions(store)
    assert not store.marks and all(row["durabilityState"] == "pending" for row in store.events)


@pytest.mark.parametrize("bad", [
    {**event("invalid"), "deletionKind": []},
    {**event("invalid"), "deletionKind": "derived_item", "reasonCode": "plain text"},
    {**event("invalid"), "deletionKind": "derived_item", "reasonCode": int("1" * 64)},
    {**event("invalid"), "deletionKind": "derived_item", "eventType": "derived_purge_scope", "reasonCode": "a" * 64, "expectedItemCount": True},
    {**event("invalid"), "deletionKind": "raw_state", "reasonCode": "{}"},
])
def test_batch_validates_every_typed_payload_before_any_remote_read_or_write(bad):
    service, client = backup()
    client.gets.clear()
    client.puts.clear()
    store = PendingDeletionStore([event("valid"), bad])
    with pytest.raises(MemoryBackupError, match="memory_deletion_event_invalid"):
        service.flush_deletions(store)
    assert not client.gets and not client.puts and not store.marks
