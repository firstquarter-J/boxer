"""질문 때 Slack을 읽거나 새 작업을 만들지 않고 기존 DB만 집계하는 HTTP 회귀."""

import logging
from dataclasses import replace

import pytest
from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings
from tests.company.test_memory_conversation_count_store import add_source

TENANT, CHANNEL, ACTOR, DD = "T1", "C1", "UA", "UDD"
NOW = "1792650000.000001"
TOKEN = "conversation-count-test-" + "x" * 40


def request(question="디디가 올해 욕 몇 번 했어?", request_id="stored-count-1", **changes):
    value = CompanyAssistantRequest(request_id=request_id, tenant_id=TENANT, actor_id=ACTOR,
        channel="slack", conversation_id=NOW, question=question, locale="ko",
        metadata={"channel_id": CHANNEL, "message_id": NOW}, memory_context={"workspaceId": TENANT,
            "actorId": ACTOR, "channelId": CHANNEL, "threadTs": NOW, "currentMessageTs": NOW,
            "channelKind": "private_channel", "triggerKind": "app_mention"})
    return replace(value, **changes)


def headers(request_id="stored-count-1", token=TOKEN):
    return {"Authorization": "Bearer " + token, "X-Request-ID": request_id}


def turn(value):
    return {"tenantId": value.tenant_id, "actorId": value.actor_id, "channel": "slack",
        "conversationId": value.conversation_id, "question": value.question, "locale": "ko",
        "contextEntries": [], "memoryContext": dict(value.memory_context), "routeGroup": "freeform",
        "scope": {"channelContextId": CHANNEL}}


@pytest.fixture
def state(tmp_path, monkeypatch):
    from boxer_company import team_chat_context
    monkeypatch.setattr(team_chat_context, "TEAM_MEMBER_PROFILES", (
        {"name": "DD", "aliases": ("dd", "디디"), "user_id": DD},
        {"name": "Hyun", "aliases": ("현",), "user_id": ACTOR},
    ))
    path = tmp_path / "private" / "memory.db"
    initialize_memory_store(path)
    store = MemoryStore(path)
    memory = MemoryService(store, enabled=True, read_enabled=True, profile_ids=frozenset({ACTOR, DD}))

    class Runtime:
        def answer_stage(self, *_args, **_kwargs):
            raise AssertionError("저장 집계와 지원 이전 대화 질문을 모델에 넘기면 안 됨")

        def answer(self, *_args, **_kwargs):
            raise AssertionError("routeGroup을 생략해도 저장 집계와 지원 이전 질문을 모델에 넘기면 안 됨")

    settings = CompanyApiSettings(host="127.0.0.1", port=8010, memory_enabled=True,
        memory_read_enabled=True, memory_state_path=str(path), callers=(CompanyApiCallerSettings(
            caller_id="count-test", token=TOKEN, tenant_ids=frozenset({TENANT}),
            channels=frozenset({"slack"}), actor_ids=frozenset({ACTOR}),
            capabilities=frozenset({"assistant.turn.read"})),))
    app = create_company_api_app(settings=settings, assistant_runtime=Runtime(), readiness_probe=lambda: True,
        memory_service=memory, automation_coordinator=None, automation_delivery_broker=None,
        hpa_change_coordinator=None)
    with TestClient(app) as client:
        yield memory, store, client


@pytest.mark.parametrize("question", [
    "올해 디디가 한 욕 몇개야?", "디디가 올해 한 욕 보여줘", "디디가 올해 어떤 욕을 했는지 알려줘",
])
@pytest.mark.parametrize("route_group", ["freeform", "fun", None])
def test_existing_db_excerpts_are_counted_synchronously_without_new_state(state, question, route_group):
    memory, store, client = state
    for index, excerpt in enumerate(("병신들", "ㅈㄴ 피곤해", '"씨발"이라는 단어', "일반 발언", "")):
        add_source(store, excerpt=excerpt, stamp=f"179210000{index}.000001")
    value = request(question)
    assert memory.prepare(value) == {"retrievalId": None, "sources": [], "storedOnly": True}
    payload = turn(value)
    if route_group is None:
        payload.pop("routeGroup")
    else:
        payload["routeGroup"] = route_group
    response = client.post("/internal/v1/assistant/turns", headers=headers(), json=payload)
    assert response.status_code == 200, response.text
    answer = response.json()
    assert answer["route"] == "company_conversation_count" and answer["usedLlm"] is False
    body = answer["messages"][0]["body"]
    assert "저장된 발췌 4건" in body and "메시지는 2건" in body and "설명 1건" in body
    # 실제 HTTP 답변에도 명확한 표현만 표시하고 인용에만 등장한 욕은 포함하지 않는다.
    assert "확인된 욕 표현:" in body and "`병신`" in body and "`ㅈㄴ`" in body
    assert "`씨발`" not in body
    assert "발췌가 없는 저장 메시지 1건" in body and "전체 대화의 욕설 횟수는 아니야" in body
    assert "memoryCapture" not in answer and "집계 중" not in body
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0] == 0
        assert not connection.execute("SELECT name FROM sqlite_master WHERE name LIKE 'memory_count_%'").fetchall()


@pytest.mark.parametrize("question", ["디디가 올해 욕 몇 번 했어?", "디디가 올해 한 욕 보여줘"])
def test_slack_client_and_rollout_return_db_answer_without_any_slack_sdk_call(state, question):
    from boxer_company_adapter_slack.company_api_client import (
        CompanyApiClientSettings,
        CompanyAssistantApiClient,
    )
    from boxer_company_adapter_slack.company_api_rollout import (
        CompanyFreeformApiRolloutService,
    )
    from boxer_company_adapter_slack.conversation_memory import SlackConversationMemory
    from boxer_company_adapter_slack.memory_api_client import CompanyMemoryApiClient
    from tests.company.test_memory_end_to_end import LocalSession

    _, store, http = state
    add_source(store)
    settings = CompanyApiClientSettings(base_url="http://127.0.0.1:8010", token=TOKEN, max_retries=0)
    session = LocalSession(http)
    transport = SlackConversationMemory(CompanyMemoryApiClient(settings, session=session), is_allowed=lambda *_: True)

    class NoSlackReads:
        def __getattr__(self, name):
            pytest.fail("저장 집계에서 Slack SDK 호출: " + name)

    transport.client = NoSlackReads()
    api = CompanyAssistantApiClient(settings, session=session)
    api.memory_request_preparer = transport.prepare
    rollout = CompanyFreeformApiRolloutService(None, api_client=api, logger=logging.getLogger(__name__))
    result = rollout.answer(request(question))
    assert result.route == "company_conversation_count" and result.outcome == "answered"
    assert "메시지는 1건" in result.messages[0].body and result.used_llm is False
    assert "확인된 욕 표현: `씨발`" in result.messages[0].body


@pytest.mark.parametrize("question", ["디디가 2025년 욕 몇 번 했어?", "디디가 2025-12-31~2026-01-02 욕 몇 번 했어?",
                                     "디디가 2025년에 한 말은 뭐야?", "2026년 이전 대화 요약해줘",
                                     "2026년 1월 1일 이전 대화 요약해줘", "2025년 이전 대화 요약해줘",
                                     "2026년이전대화요약해줘", "2026년1월1일이전대화기록",
                                     "디디가 2025년에 한 욕 보여줘", "디디가 2026년 이전 어떤 욕 했어?",
                                     "디디가 2026년 이전 욕 몇 번 했어?", "디디가 2026년 10월 이전 욕 몇 번 했어?"])
def test_before_supported_period_is_rejected_before_counting_or_model_call(state, monkeypatch, question):
    import boxer_company.memory.conversation_count_store as counting
    monkeypatch.setattr(counting, "count_stored_messages", lambda *_args, **_kwargs: pytest.fail("unsupported DB count"))
    _, _, client = state
    response = client.post("/internal/v1/assistant/turns", headers=headers(), json=turn(request(question)))
    assert response.status_code == 200, response.text
    answer = response.json()
    assert "2026년 1월 1일" in answer["messages"][0]["body"] and answer["usedLlm"] is False
    assert answer["outcome"] == "needs_input"


@pytest.mark.parametrize("question,route", [
    ("디디가 2025년에 한 말은 뭐야?", "company_freeform"),
    ("2026년 이전 대화 요약해줘", "company_freeform"),
    ("디디가 2025년 욕 몇 번 했어?", "company_conversation_count"),
])
def test_omitting_route_group_cannot_bypass_supported_history_period(state, monkeypatch, question, route):
    import boxer_company.memory.conversation_count_store as counting

    monkeypatch.setattr(counting, "count_stored_messages", lambda *_args, **_kwargs: pytest.fail("unsupported DB count"))
    _, _, client = state
    body = turn(request(question))
    body.pop("routeGroup")
    # 기본 runtime 경로로 보내도 지원 이전 기록을 조회하거나 모델 답변으로 우회하지 않는다.
    response = client.post("/internal/v1/assistant/turns", headers=headers(), json=body)
    assert response.status_code == 200, response.text
    answer = response.json()
    assert answer["route"] == route and answer["usedLlm"] is False and answer["outcome"] == "needs_input"
    assert "2026년 1월 1일" in answer["messages"][0]["body"]


@pytest.mark.parametrize("question", ["디디가 올해 욕 몇 번 했어?", "디디가 올해 한 욕 보여줘"])
def test_omitting_route_group_still_counts_supported_existing_db_excerpts(state, question):
    _, store, client = state
    add_source(store)
    body = turn(request(question))
    body.pop("routeGroup")
    # routeGroup은 선택 필드이므로 생략 여부가 DB 전용 집계의 실행 경계를 바꾸면 안 된다.
    response = client.post("/internal/v1/assistant/turns", headers=headers(), json=body)
    assert response.status_code == 200, response.text
    answer = response.json()
    assert answer["route"] == "company_conversation_count" and answer["outcome"] == "answered"
    assert answer["usedLlm"] is False and "메시지는 1건" in answer["messages"][0]["body"]
    assert "전체 대화의 욕설 횟수는 아니야" in answer["messages"][0]["body"]
    assert "memoryCapture" not in answer


@pytest.mark.parametrize("question", ["누가 올해 욕 몇 번 했어?", "디디가 욕 몇 번 했어?", "디디가 2월 30일 욕 몇 번 했어?",
                                     "올해 한 욕 보여줘", "디디가 한 욕 보여줘", "디디가 2월 30일에 한 욕 보여줘"])
def test_missing_target_or_invalid_period_never_guesses_a_count(state, question):
    _, _, client = state
    response = client.post("/internal/v1/assistant/turns", headers=headers(), json=turn(request(question)))
    assert response.status_code == 200, response.text
    assert response.json()["outcome"] == "needs_input" and response.json()["usedLlm"] is False


@pytest.mark.parametrize("question", ["디디가 올해 욕 몇 번 했어?", "디디가 올해 한 욕 보여줘"])
def test_missing_excerpt_is_unavailable_and_next_query_observes_db_deletion(state, question):
    _, store, client = state
    add_source(store, excerpt="", stamp="1792100001.000001")
    first = client.post("/internal/v1/assistant/turns", headers=headers(), json=turn(request(question))).json()
    assert "확인할 수 없어" in first["messages"][0]["body"] and "0회라는 뜻은 아니야" in first["messages"][0]["body"]
    assert "확인된 욕 표현:" not in first["messages"][0]["body"]
    add_source(store, stamp="1792100002.000001")
    current = client.post("/internal/v1/assistant/turns", headers=headers("after-insert"),
        json=turn(request(question, request_id="after-insert"))).json()
    assert "메시지는 1건" in current["messages"][0]["body"]
    assert "`씨발`" in current["messages"][0]["body"]
    store.source_event(tenant_id=TENANT, channel_id=CHANNEL, message_ts="1792100002.000001",
        event_type="deleted", event_id="delete-stored-message")
    following = client.post("/internal/v1/assistant/turns", headers=headers("after-delete"),
        json=turn(request(question, request_id="after-delete"))).json()
    assert "확인할 수 없어" in following["messages"][0]["body"] and "메시지는 1건" not in following["messages"][0]["body"]
    assert "확인된 욕 표현:" not in following["messages"][0]["body"]


def test_count_routes_are_absent_and_existing_caller_authentication_is_required(state):
    _, _, client = state
    assert not any("/counts/" in route.path or route.path.endswith("/count-scope") for route in client.app.routes)
    denied = client.post("/internal/v1/assistant/turns", headers=headers(token="invalid"), json=turn(request()))
    assert denied.status_code == 401
