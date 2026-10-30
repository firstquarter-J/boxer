"""명시 원문 재검증의 경쟁, 내구화와 전체 journal 복원 경계를 합성 DB로 검증한다."""
from __future__ import annotations

import copy
import json
import sqlite3
import time
from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from boxer_company.memory import raw_live, raw_source_policy
from boxer_company.memory.contracts import (
    MemoryError,
    canonical_digest,
    source_identity,
    source_revision,
)
from boxer_company.memory.conversation_count_store import count_stored_messages
from boxer_company.memory.raw_policy import RAW_ONLY_CUTOFF_TS, RAW_ONLY_START_TS
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api.memory_raw_import import import_raw_archive
from boxer_company_api.memory_schemas import MemoryRawEventInput
from tests.company.test_memory_backup import backup

TENANT, CHANNEL, AUTHOR, OPERATOR = "T19PA3EPL", "C0621TL2HSB", "UTEST1", "UOPERATOR"
STAMP = "1792100001.000001"


def source(index):
    stamp = format(Decimal(STAMP) + index * 10, ".6f")
    text = f"  SYNTHETIC_REVALIDATION_BODY_{index} 씨발\n"
    revision = source_revision(text, "")
    return {"sourceId": source_identity(TENANT, CHANNEL, stamp, revision), "messageTs": stamp,
        "threadTs": stamp, "createdAt": stamp, "editedAt": "", "authorId": AUTHOR,
        "authorKind": "human", "text": text, "revisionHash": revision,
        "permalink": f"https://synthetic.slack.com/archives/{CHANNEL}/p{stamp.replace('.', '')}"}


def rows(store, sql, args=()):
    with store._connection() as connection:
        return [dict(row) for row in connection.execute(sql, args)]


def raw_texts(store):
    return [row["text"] for row in rows(store, "SELECT text FROM memory_source_texts ORDER BY source_id")]


def revisions(store):
    return rows(store, "SELECT * FROM memory_events WHERE deletion_kind='source_revision' ORDER BY id")


def create_event(store, item, identity):
    return raw_live.ingest_raw_event(store, tenant_id=TENANT, channel_id=CHANNEL,
        event_id=identity, event_ts=item["messageTs"], event_type="created", source=item,
        message_ts=item["messageTs"], actor_id=AUTHOR)


def unavailable_event(store, item, identity):
    return raw_live.ingest_raw_event(store, tenant_id=TENANT, channel_id=CHANNEL,
        event_id=identity, event_ts=format(Decimal(item["messageTs"]) + 1, ".6f"), event_type="unavailable",
        source=None, message_ts=item["messageTs"], actor_id=None)


@pytest.fixture
def case(tmp_path, monkeypatch):
    from boxer_company.memory import source_revalidation

    clock = SimpleNamespace(value=1793356000.0)
    monkeypatch.setattr(time, "time", lambda: clock.value)
    profiles = ({"user_id": AUTHOR}, {"user_id": OPERATOR})
    for module in (raw_live, raw_source_policy, source_revalidation):
        monkeypatch.setattr(module, "TEAM_MEMBER_PROFILES", profiles)
    store = MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db"), clock=lambda: clock.value)
    service, client = backup()
    items = [source(index) for index in range(6)]
    for index, item in enumerate(items):
        assert create_event(store, item, f"created-{index}")["stored"]
    # 과거 snapshot에는 본문이 있다. 이후 전체 journal이 이를 지우고 메타데이터만 재승인해야 한다.
    old_snapshot = service.create_snapshot(store)
    for index, item in enumerate(items):
        assert unavailable_event(store, item, f"withdrawn-{index}")["status"] == "unavailable"
    service.flush_deletions(store)
    old_revisions = revisions(store)
    assert len(old_revisions) == 6 and not raw_texts(store)
    document = {"schemaVersion": 1, "kind": "slack_raw_archive", "importId": "registered-revalidation",
        "tenantId": TENANT, "channelId": CHANNEL, "channelKind": "private_channel", "authorId": AUTHOR,
        "startTs": RAW_ONLY_START_TS, "cutoffTs": RAW_ONLY_CUTOFF_TS, "sources": copy.deepcopy(items[:5]),
        "coverage": "complete", "excludedCounts": {}, "verifiedAt": format(clock.value, ".6f")}
    document["manifestDigest"] = canonical_digest({key: document[key] for key in
        ("tenantId", "channelId", "authorId", "startTs", "cutoffTs", "sources")})
    return SimpleNamespace(store=store, service=service, client=client, clock=clock, items=items,
        document=document, old_revisions=old_revisions, old_snapshot=old_snapshot,
        module=source_revalidation, parent=tmp_path)


def register(case):
    result = import_raw_archive(case.store, case.document, tenant_id=TENANT, channel_id=CHANNEL,
        author_id=AUTHOR, operator_id=OPERATOR, begin_only=True, now=case.clock.value)
    assert result["state"] == "collecting"
    return result


def prepare(case, **changes):
    return case.module.prepare_revalidation_plan(case.store, **{
        "import_id": case.document["importId"], "tenant_id": TENANT, "channel_id": CHANNEL,
        "author_id": AUTHOR, "operator_id": OPERATOR, "manifest_digest": case.document["manifestDigest"],
        "sources": case.document["sources"], **changes})


def import_body(case, store=None):
    payload = {**case.document, "verifiedAt": format(case.clock.value, ".6f")}
    return import_raw_archive(store or case.store, payload, tenant_id=TENANT, channel_id=CHANNEL,
        author_id=AUTHOR, operator_id=OPERATOR, now=case.clock.value)


def counted(case, store=None):
    return count_stored_messages(store or case.store, tenant_id=TENANT, channel_id=CHANNEL, author_id=AUTHOR,
        include_messages=True, period={"kind": "month", "startTs": RAW_ONLY_START_TS,
            "endTs": "1793458800.000000", "cutoffTs": RAW_ONLY_CUTOFF_TS})


def reauthorize(case, **changes):
    payload = {**copy.deepcopy(case.document), "verifiedAt": format(case.clock.value, ".6f"), **changes}
    return case.module.revalidate_registered_sources(case.store, payload, tenant_id=TENANT,
        channel_id=CHANNEL, author_id=AUTHOR, operator_id=OPERATOR, now=case.clock.value)


def prepared(case):
    register(case)
    case.clock.value += 1
    plan = prepare(case)
    # 실제 SDK 조회는 하지 않는다. 이 시각 이후의 동일 manifest 원본 확인만 가정한다.
    case.clock.value += 1
    return plan


def blocked(case, item, store=None):
    with (store or case.store)._connection() as connection:
        return case.module.is_source_revision_blocked(connection, TENANT, CHANNEL, item["sourceId"])


def restore(case, snapshot, name):
    parent = case.parent / name
    parent.mkdir(mode=0o700)
    case.service.restore_snapshot(snapshot["key"], parent / "memory.db",
        store_factory=lambda path: MemoryStore(path, clock=lambda: case.clock.value))
    return MemoryStore(parent / "memory.db", clock=lambda: case.clock.value)


def assert_no_derived_state(store):
    for table in ("memory_jobs", "memory_job_inputs", "memory_items", "memory_summaries"):
        assert rows(store, f"SELECT COUNT(*) AS n FROM {table}")[0]["n"] == 0


def add_blocker(case, kind):
    item = case.items[0]
    if kind == "deleted_watermark":
        raw_live.ingest_raw_event(case.store, tenant_id=TENANT, channel_id=CHANNEL,
            event_id="later-deleted", event_ts=format(Decimal(item["messageTs"]) + 2, ".6f"),
            event_type="deleted", source=None, message_ts=item["messageTs"])
        return
    # 별도의 변경, 삭제나 강제 가림은 특정 과거 revision 한 건의 재승인으로 지울 수 없다.
    case.store.apply_deletion_event({"id": "later-" + kind, "tenantId": TENANT,
        "scopeChannelId": CHANNEL, "sourceId": item["sourceId"], "sourceMessageId": item["messageTs"],
        "eventType": "source_changed" if kind == "source_revision" else "source_deleted",
        "requestId": "later-" + kind, "deletionKind": kind,
        "cutoffAt": case.clock.value, "durabilityState": "pending"})


def test_raw_revalidation_plan_requires_registered_identical_manifest(case):
    with pytest.raises(MemoryError):
        prepare(case)
    assert revisions(case.store) == case.old_revisions and not raw_texts(case.store)


def test_raw_revalidation_plan_is_metadata_only_and_does_not_restore_body(case):
    register(case)
    plan = prepare(case)
    assert plan["state"] == "prepared" and plan["targetCount"] == 5
    assert plan["planId"] and len(plan["checkpointSha256"]) == 64
    assert "SYNTHETIC_REVALIDATION_BODY" not in json.dumps(plan)
    assert "SYNTHETIC_REVALIDATION_BODY" not in json.dumps(rows(case.store, "SELECT * FROM memory_events"))
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


def test_ordinary_import_keeps_original_privacy_guard_without_reauthorization(case):
    register(case)
    prepare(case)
    case.clock.value += 1
    with pytest.raises(MemoryError):
        import_body(case)
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


@pytest.mark.parametrize("changes", [
    {"tenant_id": "TOTHER"}, {"channel_id": "COTHER"}, {"author_id": "UOTHER"}, {"operator_id": "UOTHER"},
    {"manifest_digest": "f" * 64}, {"import_id": "missing"},
])
def test_revalidation_plan_cannot_expand_registered_tenant_channel_author_or_operator(case, changes):
    register(case)
    with pytest.raises(MemoryError):
        prepare(case, **changes)
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


def test_revalidation_requires_a_plan_captured_before_sdk_verification(case):
    register(case)
    with pytest.raises(MemoryError):
        reauthorize(case)
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


def test_revalidation_rechecks_same_timestamp_receipt_without_watermark_change(case):
    prepared(case)
    before = rows(case.store, "SELECT * FROM memory_raw_event_states ORDER BY message_ts")
    # 같은 시각의 stale 알림도 receipt는 새롭다. watermark만 비교하면 이 경쟁을 놓친다.
    assert unavailable_event(case.store, case.items[0], "same-time-racing-receipt")["status"] == "stale"
    assert rows(case.store, "SELECT * FROM memory_raw_event_states ORDER BY message_ts") == before
    with pytest.raises(MemoryError):
        reauthorize(case)
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


@pytest.mark.parametrize("kind", ["source_revision", "source", "source_redaction", "deleted_watermark"])
@pytest.mark.parametrize("after_plan", [False, True])
def test_revalidation_never_overrides_another_revision_delete_redaction_or_deleted_watermark(case, kind, after_plan):
    register(case)
    if after_plan:
        prepare(case)
    add_blocker(case, kind)
    if after_plan:
        case.clock.value += 1
        with pytest.raises(MemoryError):
            reauthorize(case)
    else:
        with pytest.raises(MemoryError):
            prepare(case)
    assert not raw_texts(case.store) and blocked(case, case.items[0])
    assert not rows(case.store, "SELECT id FROM memory_events WHERE event_type='raw_source_revalidated'")


@pytest.mark.parametrize("verified_offset", [-1, 1, -301])
def test_revalidation_verification_must_follow_plan_and_remain_fresh(case, verified_offset):
    register(case)
    case.clock.value += 2
    prepare(case)
    with pytest.raises(MemoryError):
        reauthorize(case, verifiedAt=format(case.clock.value + verified_offset, ".6f"))
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


@pytest.mark.parametrize("when", ["plan", "apply"])
@pytest.mark.parametrize("guard", ["profile", "private"])
def test_revalidation_preserves_current_profile_and_actual_private_thread_checks(case, monkeypatch, when, guard):
    register(case)
    if when == "apply":
        prepare(case)
    if guard == "profile":
        for module in (case.module, raw_source_policy):
            monkeypatch.setattr(module, "TEAM_MEMBER_PROFILES", ({"user_id": OPERATOR},))
    else:
        with case.store._connection(write=True) as connection:
            connection.execute("UPDATE memory_threads SET channel_kind='channel'")
    with pytest.raises(MemoryError):
        prepare(case) if when == "plan" else reauthorize(case)
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


def test_reauthorization_is_metadata_only_exactly_five_targets_and_blocks_until_backup_is_durable(case):
    plan = prepared(case)
    result = reauthorize(case)
    assert result["state"] == "revalidated" and result["reauthorizedSourceCount"] == 5
    assert result["planId"] == plan["planId"] and result["checkpointSha256"] == plan["checkpointSha256"]
    assert len(set(result["reauthorizationEventIds"])) == 5
    events = rows(case.store, "SELECT * FROM memory_events WHERE event_type='raw_source_revalidated'")
    expected_ids = {item["sourceId"] for item in case.items[:5]}
    targets = {item["source_id"]: item["id"] for item in case.old_revisions if item["source_id"] in expected_ids}
    assert {event["source_id"]: event["reallow_request_id"] for event in events} == targets
    assert all(set(json.loads(event["reason_code"])) == {"eventTs", "revisionHash", "state"} for event in events)
    assert "SYNTHETIC_REVALIDATION_BODY" not in json.dumps(rows(case.store, "SELECT * FROM memory_events"))
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions
    assert all(blocked(case, item) for item in case.items)
    with pytest.raises(MemoryError):
        import_body(case)
    case.service.flush_deletions(case.store)
    assert all(not blocked(case, item) for item in case.items[:5]) and blocked(case, case.items[5])
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions
    assert counted(case).get("storedRawMessageCount", 0) == 0
    assert_no_derived_state(case.store)


@pytest.mark.parametrize("field, value", [
    ("text", "다른 합성 본문"), ("threadTs", "1792100000.000001"),
    ("authorId", "UOTHER"), ("revisionHash", "a" * 64),
])
def test_revalidation_rejects_source_changes_to_registered_immutable_manifest(case, field, value):
    prepared(case)
    changed = copy.deepcopy(case.document["sources"])
    changed[0][field] = value
    with pytest.raises(MemoryError):
        reauthorize(case, sources=changed)
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


def test_separate_official_import_restores_only_reauthorized_body_and_retry_never_erases_it(case):
    prepared(case)
    result = reauthorize(case)
    case.service.flush_deletions(case.store)
    assert import_body(case)["storedCount"] == 5
    assert sorted(raw_texts(case.store)) == sorted(item["text"] for item in case.items[:5])
    assert counted(case)["matchedMessageCount"] == counted(case)["storedRawMessageCount"] == 5
    # 같은 재승인의 재전송이 이미 게시한 본문을 다시 지우면 안 된다.
    replay = reauthorize(case)
    assert set(replay["reauthorizationEventIds"]) == set(result["reauthorizationEventIds"])
    assert len(raw_texts(case.store)) == 5 and revisions(case.store) == case.old_revisions
    assert blocked(case, case.items[5])
    assert_no_derived_state(case.store)


def test_old_snapshot_full_journal_replay_cannot_reconstruct_body_from_metadata_only_reauthorization(case):
    prepared(case)
    reauthorize(case)
    case.service.flush_deletions(case.store)
    restored = restore(case, case.old_snapshot, "old-restored")
    assert not raw_texts(restored)
    assert counted(case, restored).get("storedRawMessageCount", 0) == 0
    assert all(not blocked(case, item, restored) for item in case.items[:5])
    assert blocked(case, case.items[5], restored)
    assert_no_derived_state(restored)


def test_new_snapshot_full_journal_replay_and_cleanup_keep_imported_current_body(case):
    prepared(case)
    reauthorize(case)
    case.service.flush_deletions(case.store)
    import_body(case)
    latest = case.service.create_snapshot(case.store)
    restored = restore(case, latest, "new-restored")
    restored.cleanup(restored=True)
    assert sorted(raw_texts(restored)) == sorted(item["text"] for item in case.items[:5])
    assert counted(case, restored)["matchedMessageCount"] == counted(case, restored)["storedRawMessageCount"] == 5
    assert blocked(case, case.items[5], restored)
    assert_no_derived_state(restored)


@pytest.mark.parametrize("kind", ["source_revision", "source", "source_redaction", "deleted_watermark"])
def test_later_invalidation_always_wins_over_old_durable_reauthorization_in_restore(case, kind):
    prepared(case)
    reauthorize(case)
    case.service.flush_deletions(case.store)
    import_body(case)
    add_blocker(case, kind)
    # 한 번의 재승인은 이후의 수정이나 삭제를 무시하는 영구 예외가 아니다.
    assert len(raw_texts(case.store)) == counted(case)["storedRawMessageCount"] == 4
    with pytest.raises(MemoryError):
        reauthorize(case)
    case.service.flush_deletions(case.store)
    restored = restore(case, case.service.create_snapshot(case.store), "later-restored")
    assert sorted(raw_texts(restored)) == sorted(item["text"] for item in case.items[1:5])
    assert counted(case, restored)["storedRawMessageCount"] == 4
    assert_no_derived_state(restored)


@pytest.mark.parametrize("changes", [
    {"eventType": "raw_source_revalidated"}, {"reallowRequestId": "old-event"},
    {"confirmAuthorizedRevalidation": True}, {"revalidationPlanId": "operator-plan"},
])
def test_http_raw_event_cannot_express_operator_reauthorization(changes):
    # 내부 전송 API의 source DTO에 운영자 전용 재승인 권한을 추가하지 않는다.
    payload = {"tenantId": TENANT, "channelId": CHANNEL, "actorId": AUTHOR,
        "messageTs": STAMP, "eventId": "http-revalidation-attempt", "eventTs": STAMP,
        "eventType": "created", "source": source(0)}
    assert MemoryRawEventInput.model_validate(payload).eventType == "created"
    with pytest.raises(ValidationError):
        MemoryRawEventInput.model_validate({**payload, **changes})


def cli_setup(case, monkeypatch):
    from boxer_company_api import memory_cli

    parent = case.parent / "operator-input"
    parent.mkdir(mode=0o700)
    path = parent / "archive.json"
    path.write_text(json.dumps(case.document), encoding="utf-8")
    path.chmod(0o600)
    monkeypatch.setattr(memory_cli, "validate_company_api_runtime_security", lambda: None)
    monkeypatch.setattr(memory_cli, "MemoryStore", lambda _: case.store)
    monkeypatch.setattr(memory_cli, "configured_memory_backup", lambda: case.service)
    args = ["--input", str(path), "--operator-id", OPERATOR, "--tenant-id", TENANT,
        "--channel-id", CHANNEL, "--author-id", AUTHOR, "--confirm-authorized-import"]
    return memory_cli, path, args


def update_verified_file(case, path):
    case.clock.value += 1
    payload = {**case.document, "verifiedAt": format(case.clock.value, ".6f")}
    path.write_text(json.dumps(payload), encoding="utf-8")


@pytest.mark.parametrize("missing_at", ["begin", "import"])
def test_cli_cannot_reauthorize_when_explicit_confirmation_is_missing(case, monkeypatch, missing_at):
    cli, path, args = cli_setup(case, monkeypatch)
    flag = ["--confirm-current-source-revalidation"]
    cli.main(["begin-raw-import", *args, *(flag if missing_at != "begin" else [])])
    update_verified_file(case, path)
    with pytest.raises(SystemExit):
        cli.main(["import-raw", *args, *(flag if missing_at != "import" else [])])
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions


def test_cli_revalidation_flushes_durable_journal_then_separately_imports_without_printing_body(case, monkeypatch, capsys):
    cli, path, args = cli_setup(case, monkeypatch)
    args.append("--confirm-current-source-revalidation")
    cli.main(["begin-raw-import", *args])
    assert not raw_texts(case.store)
    update_verified_file(case, path)
    cli.main(["import-raw", *args])
    outputs = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert outputs[-1]["storedCount"] == 5 and len(raw_texts(case.store)) == 5
    assert "SYNTHETIC_REVALIDATION_BODY" not in json.dumps(outputs)
    assert all(event["durabilityState"] == "durable" for event in case.store.deletion_events())
    assert revisions(case.store) == case.old_revisions and blocked(case, case.items[5])


@pytest.mark.parametrize("failure", ["unconfigured", "flush_failure", "not_durable"])
def test_cli_backup_failure_cannot_publish_reauthorized_text(case, monkeypatch, failure):
    cli, path, args = cli_setup(case, monkeypatch)
    args.append("--confirm-current-source-revalidation")
    cli.main(["begin-raw-import", *args])
    update_verified_file(case, path)
    if failure == "unconfigured":
        monkeypatch.setattr(cli, "configured_memory_backup", lambda: None)
    elif failure == "not_durable":
        monkeypatch.setattr(case.service, "flush_deletions", lambda _: 0)
    else:
        def fail_flush(_):
            raise RuntimeError("synthetic_journal_failure")
        monkeypatch.setattr(case.service, "flush_deletions", fail_flush)
    with pytest.raises(SystemExit):
        cli.main(["import-raw", *args])
    assert not raw_texts(case.store) and revisions(case.store) == case.old_revisions
    assert all(blocked(case, item) for item in case.items)


@pytest.mark.parametrize("command", ["health", "cleanup", "snapshot", "process-pending"])
def test_revalidation_flag_on_other_cli_commands_fails_before_security_or_database_access(monkeypatch, command):
    from boxer_company_api import memory_cli

    def forbidden(*_args, **_kwargs):
        pytest.fail("범위 밖 CLI flag는 보안 설정과 DB 접근 전에 거절해야 해")
    monkeypatch.setattr(memory_cli, "validate_company_api_runtime_security", forbidden)
    monkeypatch.setattr(memory_cli, "MemoryStore", forbidden)
    with pytest.raises(SystemExit) as raised:
        memory_cli.main([command, "--confirm-current-source-revalidation"])
    assert raised.value.code == 2


@pytest.mark.parametrize("alias", ["raw_release", "RaW_ReLeAsE", "raw_release_check"])
def test_effective_revision_sql_keeps_outer_event_when_alias_collides(case, alias):
    prepared(case)
    reauthorize(case)
    predicate = case.module.effective_revision_sql(alias)
    statement = f"SELECT id FROM memory_events {alias} WHERE deletion_kind='source_revision' AND {predicate}"
    assert {row["id"] for row in rows(case.store, statement)} == {row["id"] for row in case.old_revisions}
    case.service.flush_deletions(case.store)
    expected = {row["id"] for row in case.old_revisions if row["source_id"] == case.items[5]["sourceId"]}
    assert {row["id"] for row in rows(case.store, statement)} == expected


def test_old_revalidation_metadata_cannot_rewind_a_newer_current_source_without_watermark_receipt(case):
    prepared(case)
    item = {**case.items[0], "text": "새 snapshot에만 있는 합성 최신 본문", "editedAt": "1792100004.000001"}
    item["revisionHash"] = source_revision(item["text"], item["editedAt"])
    item["sourceId"] = source_identity(TENANT, CHANNEL, item["messageTs"], item["revisionHash"])
    normalized = raw_source_policy.normalize_raw_source(item, tenant_id=TENANT, channel_id=CHANNEL,
        thread_ts=item["threadTs"], current_message_ts=item["editedAt"])
    # 과거 snapshot의 watermark가 남았어도 더 최신 current metadata를 옛 승인으로 되돌리지 않는다.
    with case.store._connection(write=True) as connection:
        thread = case.store._thread(connection, TENANT, CHANNEL, item["threadTs"], "private_channel")
        assert case.store._register_source(connection, TENANT, thread, normalized)
        provenance = "live_" + canonical_digest([TENANT, "created-0"])
        connection.execute("INSERT INTO memory_source_texts VALUES(?,?,?,?,?)",
            (TENANT, item["sourceId"], provenance, item["text"], case.clock.value))
    target = next(row for row in case.old_revisions if row["source_id"] == case.items[0]["sourceId"])
    event = {"id": "synthetic-old-reauthorization", "tenantId": TENANT, "scopeChannelId": CHANNEL,
        "sourceId": case.items[0]["sourceId"], "sourceMessageId": STAMP, "eventType": "raw_source_revalidated",
        "deletionKind": "raw_state", "actorId": OPERATOR, "requestId": "synthetic-old-plan",
        "reallowRequestId": target["id"], "reasonCode": json.dumps({"eventTs": "1792100002.000001",
            "revisionHash": case.items[0]["revisionHash"], "state": "current"}, sort_keys=True, separators=(",", ":"))}
    before_sources = rows(case.store, "SELECT * FROM memory_sources ORDER BY id")
    before_watermarks = rows(case.store, "SELECT * FROM memory_raw_event_states ORDER BY message_ts")
    with case.store._connection(write=True) as connection:
        assert case.module.apply_revalidation_metadata(connection, event) is False
    assert rows(case.store, "SELECT * FROM memory_sources ORDER BY id") == before_sources
    assert rows(case.store, "SELECT * FROM memory_raw_event_states ORDER BY message_ts") == before_watermarks
    assert raw_texts(case.store) == [item["text"]]


def readiness(case, *, connection=None, **changes):
    payload = {**copy.deepcopy(case.document), "verifiedAt": format(case.clock.value, ".6f"), **changes}
    return case.module.validate_registered_revalidation(case.store, payload, tenant_id=TENANT,
        channel_id=CHANNEL, author_id=AUTHOR, operator_id=OPERATOR, now=case.clock.value, connection=connection)


def test_revalidation_readiness_uses_only_supplied_readonly_snapshot_and_never_writes(case, monkeypatch):
    plan = prepared(case)
    before_events = rows(case.store, "SELECT * FROM memory_events ORDER BY id")
    with sqlite3.connect(case.store.path.as_uri() + "?mode=ro", uri=True) as connection:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        connection.execute("BEGIN")
        changes = connection.total_changes
        statements = []
        connection.set_trace_callback(statements.append)
        def forbidden(*_args, **_kwargs):
            pytest.fail("readiness는 전달한 read-only snapshot만 써야 해")
        with monkeypatch.context() as scoped:
            scoped.setattr(case.store, "_connection", forbidden)
            result = readiness(case, connection=connection)
        assert result == {"state": "validated", "planId": plan["planId"],
            "checkpointSha256": plan["checkpointSha256"], "targetCount": 5,
            "targetSourceIds": sorted(item["sourceId"] for item in case.items[:5]), "databaseWrites": 0}
        assert connection.in_transaction and connection.total_changes == changes == 0
        assert all(statement.lstrip().upper().startswith("SELECT") for statement in statements)
        assert connection.execute("PRAGMA query_only").fetchone()[0] == 1
    assert rows(case.store, "SELECT * FROM memory_events ORDER BY id") == before_events
    assert not raw_texts(case.store)


@pytest.mark.parametrize("change", ["checkpoint", "new_receipt", "expired_verification"])
def test_readonly_readiness_rejects_changed_checkpoint_new_receipt_or_stale_verification(case, change):
    prepared(case)
    if change == "checkpoint":
        with case.store._connection(write=True) as connection:
            connection.execute("UPDATE memory_events SET reason_code=? WHERE event_type='raw_revalidation_plan'", ("f" * 64,))
    elif change == "new_receipt":
        unavailable_event(case.store, case.items[0], "new-readiness-receipt")
    before = rows(case.store, "SELECT * FROM memory_events ORDER BY id")
    payload = {"verifiedAt": format(case.clock.value - 301, ".6f")} if change == "expired_verification" else {}
    with pytest.raises(MemoryError):
        readiness(case, **payload)
    assert rows(case.store, "SELECT * FROM memory_events ORDER BY id") == before and not raw_texts(case.store)


def test_readiness_accepts_own_pending_retry_without_publishing_text_or_changing_metadata(case):
    prepared(case)
    result = reauthorize(case)
    assert result["durability"] == "pending"
    before = rows(case.store, "SELECT * FROM memory_events ORDER BY id")
    assert readiness(case)["targetCount"] == 5
    assert rows(case.store, "SELECT * FROM memory_events ORDER BY id") == before
    assert all(blocked(case, item) for item in case.items) and not raw_texts(case.store)


def test_readiness_also_rejects_new_revision_of_normal_source_outside_reauthorization_targets(case):
    item = {**case.items[5], "text": "정상 최신 합성 발언", "editedAt": "1792100054.000001"}
    item["revisionHash"] = source_revision(item["text"], item["editedAt"])
    item["sourceId"] = source_identity(TENANT, CHANNEL, item["messageTs"], item["revisionHash"])
    raw_live.ingest_raw_event(case.store, tenant_id=TENANT, channel_id=CHANNEL,
        event_id="normal-current", event_ts=item["editedAt"], event_type="changed",
        source=item, message_ts=item["messageTs"], actor_id=AUTHOR)
    case.document["sources"].append(item)
    case.document["manifestDigest"] = canonical_digest({key: case.document[key] for key in
        ("tenantId", "channelId", "authorId", "startTs", "cutoffTs", "sources")})
    prepared(case)
    assert readiness(case)["targetCount"] == 5
    # 재승인 대상이 아닌 정상 source도 게시 전 같은 최신 revision 검증을 통과해야 한다.
    newer = {**item, "text": "그다음 수정된 정상 합성 발언", "editedAt": "1792100056.000001"}
    newer["revisionHash"] = source_revision(newer["text"], newer["editedAt"])
    newer["sourceId"] = source_identity(TENANT, CHANNEL, newer["messageTs"], newer["revisionHash"])
    raw_live.ingest_raw_event(case.store, tenant_id=TENANT, channel_id=CHANNEL,
        event_id="normal-newer", event_ts=newer["editedAt"], event_type="changed",
        source=newer, message_ts=newer["messageTs"], actor_id=AUTHOR)
    before = rows(case.store, "SELECT * FROM memory_events ORDER BY id")
    with pytest.raises(MemoryError):
        readiness(case)
    assert rows(case.store, "SELECT * FROM memory_events ORDER BY id") == before
