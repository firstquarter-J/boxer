"""운영자 원문 적재는 scope 등록 후의 재검증만 받으며 질문 때 저장 자료만 읽는다."""

import json
from copy import deepcopy

import pytest

from boxer_company.memory.contracts import (
    MemoryError,
    canonical_digest,
    source_revision,
)
from boxer_company.memory.conversation_count_store import count_stored_messages
from boxer_company.memory.conversation_history import format_conversation_history_answer
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api.memory_raw_import import import_raw_archive

NOW = 1792670000.0
STAMP = "1792100001.000001"


@pytest.fixture
def store(tmp_path):
    return MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db"), clock=lambda: NOW)


def document(text="씨발 오늘 회의가 길었어"):
    source = {"messageTs": STAMP, "threadTs": STAMP, "createdAt": STAMP, "authorId": "UDD",
              "authorKind": "human", "text": text, "revisionHash": source_revision(text),
              "permalink": "https://team.slack.com/archives/C1/p" + STAMP.replace(".", "")}
    result = {"schemaVersion": 1, "kind": "slack_raw_archive", "importId": "raw_october",
              "tenantId": "T1", "channelId": "C1", "channelKind": "private_channel", "authorId": "UDD",
              "startTs": "1790780400.000000", "cutoffTs": "1792670000.000000", "sources": [source],
              "coverage": "complete", "excludedCounts": {"unsafe": 2}, "verifiedAt": "1792670000.000000"}
    result["manifestDigest"] = canonical_digest({key: result[key] for key in (
        "tenantId", "channelId", "authorId", "startTs", "cutoffTs", "sources")})
    return result


def run(store, payload, **kwargs):
    return import_raw_archive(store, payload, tenant_id="T1", channel_id="C1", author_id="UDD",
                              operator_id="UOP", now=NOW, **kwargs)


def query(store):
    return count_stored_messages(store, tenant_id="T1", channel_id="C1", author_id="UDD", include_messages=True,
        period={"kind": "year", "startTs": "1767193200.000000", "endTs": "1798729200.000000",
                "cutoffTs": "1792670000.000000"})


def test_raw_archive_reaches_count_and_history_without_summary_or_jobs(store):
    payload = document()
    run(store, payload, begin_only=True)
    result = run(store, payload)
    assert result["storedCount"] == 1 and result["excludedCounts"] == {"unsafe": 2}
    found = query(store)
    assert found["matchedMessageCount"] == found["storedRawMessageCount"] == 1
    assert found["matchedMessages"][0]["excerpt"] == payload["sources"][0]["text"]
    answer = format_conversation_history_answer(found)
    assert "2026-10-16" in answer and "회의가 길었어" in answer and "보존된 원문" in answer
    with store._connection() as connection:
        assert connection.execute("SELECT excerpt FROM memory_sources").fetchone()[0] == ""
        for table in ("memory_jobs", "memory_job_inputs", "memory_items"):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0


@pytest.mark.parametrize("change", ["unregistered", "stale", "future", "before_registration", "author", "digest"])
def test_publish_requires_matching_scope_and_fresh_verification_after_registration(store, change):
    payload = document()
    if change != "unregistered":
        run(store, payload, begin_only=True)
    if change == "stale": payload["verifiedAt"] = str(NOW - 301)
    elif change == "future": payload["verifiedAt"] = str(NOW + 1)
    elif change == "before_registration": payload["verifiedAt"] = str(NOW - 1)
    elif change == "author": payload["authorId"] = "UOTHER"
    elif change == "digest": payload["sources"][0]["text"] += " 변경"
    with pytest.raises(MemoryError):
        run(store, payload)
    with store._connection() as connection:
        assert connection.execute("SELECT count(*) FROM memory_source_texts").fetchone()[0] == 0


@pytest.mark.parametrize("event_type", ["changed", "deleted", "unavailable"])
def test_source_events_remove_raw_text_and_query_evidence(store, event_type):
    payload = document()
    run(store, payload, begin_only=True)
    run(store, payload)
    store.source_event(tenant_id="T1", channel_id="C1", message_ts=STAMP,
                       event_type=event_type, event_id="evt_raw")
    assert query(store)["matchedMessageCount"] == 0
    with store._connection() as connection:
        assert connection.execute("SELECT count(*) FROM memory_source_texts").fetchone()[0] == 0
    with pytest.raises(MemoryError):
        run(store, deepcopy(payload))


def test_raw_revision_tampering_is_not_counted_or_displayed(store):
    payload = document()
    run(store, payload, begin_only=True)
    run(store, payload)
    with store._connection(write=True) as connection:
        connection.execute("UPDATE memory_source_texts SET text='존나 변조된 원문'")
    result = query(store)
    assert result["matchedMessageCount"] == 0 and result["unavailableExcerptMessageCount"] == 1
    assert "변조" not in format_conversation_history_answer(result)


def test_long_raw_message_is_classified_beyond_old_excerpt_limit(store):
    # 원문 중간의 욕이 요약 발췌 1000자 바깥에 있어도 실제 저장 본문으로 판정한다.
    payload = document("회의 이야기 " * 200 + "씨발 일정이 너무 길어")
    run(store, payload, begin_only=True)
    run(store, payload)
    result = query(store)
    assert result["matchedMessageCount"] == 1 and len(result["matchedMessages"]) == 1
    assert "확인된 표현: `씨발`" in format_conversation_history_answer(result)


def test_operator_cli_uses_protected_file_and_outputs_no_raw_text(store, tmp_path, monkeypatch, capsys):
    from boxer_company_api import memory_cli, memory_raw_import

    payload = document()
    parent = tmp_path / "operator"
    parent.mkdir(mode=0o700)
    path = parent / "archive.json"
    path.write_text(json.dumps(payload))
    path.chmod(0o600)
    monkeypatch.setattr(memory_cli, "validate_company_api_runtime_security", lambda: None)
    monkeypatch.setattr(memory_cli, "MemoryStore", lambda _path: store)
    monkeypatch.setattr(memory_raw_import.time, "time", lambda: NOW)
    args = ["--input", str(path), "--operator-id", "UOP", "--tenant-id", "T1", "--channel-id", "C1",
            "--author-id", "UDD", "--confirm-authorized-import"]
    memory_cli.main(["begin-raw-import", *args])
    memory_cli.main(["import-raw", *args])
    output = capsys.readouterr().out
    assert payload["sources"][0]["text"] not in output
    assert json.loads(output.splitlines()[-1])["storedCount"] == 1


def test_cli_migration_requires_stopped_service_and_reports_current_schema(tmp_path, monkeypatch, capsys):
    from boxer_company.memory.contracts import MEMORY_SCHEMA_VERSION
    from boxer_company_api import memory_cli

    monkeypatch.setattr(memory_cli, "validate_company_api_runtime_security", lambda: None)
    path = tmp_path / "private" / "memory.db"
    with pytest.raises(SystemExit):
        memory_cli.main(["init", "--path", str(path)])
    assert not path.exists()
    memory_cli.main(["init", "--path", str(path), "--confirm-service-stopped"])
    assert json.loads(capsys.readouterr().out)["schema"] == MEMORY_SCHEMA_VERSION
