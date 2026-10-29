"""원문 전용 수집 HTTP 경계와 자동 추출 차단을 실제 SQLite로 확인한다."""
from copy import deepcopy

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.memory.contracts import source_revision
from boxer_company.memory.raw_policy import RAW_ONLY_CHANNEL_IDS, RAW_ONLY_TENANT_ID
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api.auth import CallerRegistry
from boxer_company_api.memory_router import create_memory_router
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.problems import install_problem_handlers
from boxer_company_api.settings import CompanyApiCallerSettings

TENANT, CHANNEL = RAW_ONLY_TENANT_ID, min(RAW_ONLY_CHANNEL_IDS)
AUTHOR, STAMP = "UACTOR", "1792100000.000001"
TOKEN = "raw-event-test-" + "x" * 40


@pytest.fixture
def state(tmp_path, monkeypatch):
    from boxer_company.memory import raw_live
    monkeypatch.setattr(raw_live, "TEAM_MEMBER_PROFILES", ({"user_id": AUTHOR},))
    store = MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db"))
    service = MemoryService(store, enabled=True, read_enabled=True, profile_ids=frozenset({AUTHOR}))
    caller = CompanyApiCallerSettings(caller_id="raw-test", token=TOKEN, tenant_ids=frozenset({TENANT}),
        channels=frozenset({"slack"}), actor_ids=frozenset({AUTHOR}), capabilities=frozenset({"assistant.turn.read"}))
    app = FastAPI()
    install_problem_handlers(app)
    app.include_router(create_memory_router(service=service, caller_registry=CallerRegistry([caller])))
    with TestClient(app) as client:
        yield store, service, client


def payload():
    text = "  오늘 회의에서 일정 이야기를 했어.\n"
    return {"tenantId": TENANT, "channelId": CHANNEL, "actorId": AUTHOR, "messageTs": STAMP,
            "eventId": "raw-event-1", "eventTs": STAMP, "eventType": "created",
            "source": {"threadTs": STAMP, "messageTs": STAMP, "authorId": AUTHOR, "authorKind": "human",
                       "text": text, "revisionHash": source_revision(text), "createdAt": STAMP,
                       "permalink": f"https://lifexio.slack.com/archives/{CHANNEL}/p{STAMP.replace('.', '')}"}}


def post(client, value, *, token=TOKEN):
    return client.post("/internal/v1/memory/sources/raw", json=value,
                       headers={"Authorization": "Bearer " + token, "X-Request-ID": "raw-source-http"})


def test_raw_event_is_persisted_without_extraction_and_response_contains_no_text(state):
    store, _, client = state
    value = payload()
    result = post(client, value)
    assert result.status_code == 200 and result.json()["stored"] is True
    assert value["source"]["text"] not in result.text
    assert post(client, value).status_code == 200
    with store._connection() as connection:
        assert connection.execute("SELECT text FROM memory_source_texts").fetchone()[0] == value["source"]["text"]
        assert connection.execute("SELECT count(*) FROM memory_source_texts").fetchone()[0] == 1
        for table in ("memory_items", "memory_jobs", "memory_job_inputs"):
            assert connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0] == 0


@pytest.mark.parametrize("change,status", [("token", 401), ("actor", 403), ("author", 422),
    ("scope", 409), ("month", 409), ("bot", 422), ("body_extra", 422)])
def test_auth_and_source_scope_fail_closed_without_body_reflection(state, change, status):
    store, _, client = state
    value = payload()
    if change == "actor":
        value["actorId"] = value["source"]["authorId"] = "UOTHER"
    elif change == "author":
        value["source"]["authorId"] = "UOTHER"
    elif change == "scope":
        value["channelId"] = "COTHER"
    elif change == "month":
        value["messageTs"] = value["source"]["messageTs"] = value["eventTs"] = "1793458800.000001"
    elif change == "bot":
        value["source"]["authorKind"] = "boxer"
    elif change == "body_extra":
        value["source"]["unexpected"] = "private-data"
    result = post(client, value, token="bad" if change == "token" else TOKEN)
    assert result.status_code == status
    assert "private-data" not in result.text and value["source"]["text"] not in result.text
    with store._connection() as connection:
        assert connection.execute("SELECT count(*) FROM memory_source_texts").fetchone()[0] == 0


def test_authorless_delete_can_only_invalidate_and_never_recreate(state):
    store, _, client = state
    value = payload()
    assert post(client, value).status_code == 200
    deleted = {k: v for k, v in value.items() if k not in {"source", "actorId"}}
    deleted.update(eventId="delete-1", eventType="deleted", eventTs="1792100001.000001")
    assert post(client, deleted).status_code == 200
    replay = deepcopy(value)
    replay.update(eventId="late-create", eventTs="1792100002.000001")
    assert post(client, replay).status_code == 200
    with store._connection() as connection:
        assert connection.execute("SELECT count(*) FROM memory_source_texts").fetchone()[0] == 0


@pytest.mark.parametrize("length,status", [(40000, 200), (40001, 422)])
def test_raw_text_size_boundary_preserves_full_body_or_rejects_without_reflection(state, length, status):
    store, _, client = state
    value = payload()
    text = "문" * length
    value["source"].update(text=text, revisionHash=source_revision(text))
    response = post(client, value)
    assert response.status_code == status and text not in response.text
    with store._connection() as connection:
        row = connection.execute("SELECT text FROM memory_source_texts").fetchone()
        assert row[0] == text if status == 200 else row is None


@pytest.mark.parametrize("stamp", ["1789000000.000001", STAMP, "1793458800.000001"])
def test_raw_only_answer_does_not_create_a_summary_capture(state, stamp):
    store, service, _ = state
    request = CompanyAssistantRequest(request_id="raw-only-answer", tenant_id=TENANT, actor_id=AUTHOR,
        channel="slack", conversation_id=stamp, question="오늘도 좋은 하루", locale="ko",
        metadata={"channel_id": CHANNEL}, memory_context={"workspaceId": TENANT, "actorId": AUTHOR,
            "channelId": CHANNEL, "threadTs": stamp, "currentMessageTs": stamp,
            "channelKind": "private_channel", "triggerKind": "app_mention"})
    result = CompanyAssistantResult(route="company_freeform", outcome="answered",
                                    messages=(AssistantMessage(body="좋은 하루 보내"),))
    assert service.capture(request, result).memory_capture is None
    with store._connection() as connection:
        assert connection.execute("SELECT count(*) FROM memory_jobs").fetchone()[0] == 0


def test_raw_changes_close_prepared_live_sources_and_skip_stale_events(state, monkeypatch):
    _, service, client = state
    invalidations = []
    monkeypatch.setattr(service, "invalidate_live_source", lambda **metadata: invalidations.append(metadata))
    value = payload()
    assert post(client, value).status_code == 200
    assert invalidations == []
    value.update(eventId="changed-cache", eventType="changed", eventTs="1792100001.000001")
    value["source"].update(text="수정된 발언", revisionHash=source_revision("수정된 발언", value["eventTs"]),
                           editedAt=value["eventTs"])
    assert post(client, value).status_code == 200
    removed = {key: content for key, content in value.items() if key not in {"source", "actorId"}}
    removed.update(eventId="unavailable-cache", eventType="unavailable", eventTs="1792100002.000001")
    assert post(client, removed).status_code == 200
    value["eventId"] = "stale-cache"
    assert post(client, value).json()["status"] == "stale"
    assert [entry["event_type"] for entry in invalidations] == ["changed", "unavailable"]
    assert all("text" not in entry for entry in invalidations)
