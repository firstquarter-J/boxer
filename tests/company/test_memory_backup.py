from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import io
import json
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from boxer_company.memory.backup import MemoryBackup, MemoryBackupError, MemoryBackupSettings
from boxer_company.memory.contracts import canonical_digest, source_revision
from boxer_company.memory.store import MemoryStore, initialize_memory_store


class S3Error(Exception):
    def __init__(self, code):
        self.response = {"Error": {"Code": code}}


class FakeS3:
    def __init__(self):
        self.objects = {}
        self.puts = []
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
    original = SimpleNamespace(deletion_events=lambda: [], snapshot=lambda path: Path(path).write_bytes(b"old snapshot"))
    metadata = service.create_snapshot(original)
    service.append_deletion(event())
    operations = []

    def factory(path):
        assert path.read_bytes() == b"old snapshot"
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


def test_restore_does_not_remove_another_process_destination_on_create_race(tmp_path):
    service, _ = backup()
    tmp_path.chmod(0o700)
    metadata = service.create_snapshot(SimpleNamespace(deletion_events=lambda: [], snapshot=lambda path: Path(path).write_bytes(b"snapshot")))
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
    metadata = service.create_snapshot(SimpleNamespace(deletion_events=lambda: [], snapshot=lambda path: Path(path).write_bytes(b"snapshot")))
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
