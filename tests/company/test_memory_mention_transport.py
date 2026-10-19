"""Slack 이벤트 질문의 사람 멘션이 회사 HTTP 작성자 필터까지 보존되는지 검증한다."""

from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient

from boxer_adapter_slack import common
from boxer_company_adapter_slack.assistant_bridge import build_company_assistant_request
from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings


TENANT = "T19PA3EPL"
HUB = "C068FVD5V7Y"
TOKEN = "mention-transport-test-" + "x" * 40


class FakeApp:
    def __init__(self, **_kwargs):
        self.handlers = {}

    def event(self, name):
        def register(handler):
            self.handlers[name] = handler
            return handler
        return register


class CandidateStore:
    def __init__(self):
        self.queries = []

    def health(self):
        return True

    def retrieval_candidates(self, **query):
        self.queries.append(query)
        return []


@pytest.mark.parametrize("raw,expected", [
    ("<@UBOT> 오늘 <@UTARGET> 가 한 말 요약해줘", "오늘 <@UTARGET> 가 한 말 요약해줘"),
    ("오늘 <@UTARGET> 가 한 말 <@UBOT> 요약해줘", "오늘 <@UTARGET> 가 한 말  요약해줘"),
    ("<@UBOT|helper> 오늘 <@UTARGET|person> 가 한 말 요약해줘", "오늘 <@UTARGET|person> 가 한 말 요약해줘"),
])
def test_slack_event_question_reaches_http_memory_author_scope(monkeypatch, raw, expected):
    from boxer_company import team_chat_context

    # Slack·LLM·DB를 호출하지 않고 실제 이벤트 전처리, 회사 DTO, HTTP schema, 기억 계획을 잇는다.
    monkeypatch.setattr(common, "App", FakeApp)
    monkeypatch.setattr(common.ss, "validate_slack_tokens", lambda: None)
    monkeypatch.setattr(common, "_validate_tokens", lambda **_: None)
    monkeypatch.setattr(common.s, "REQUEST_LOG_SQLITE_ENABLED", False)
    monkeypatch.setattr(team_chat_context, "TEAM_MEMBER_PROFILES", ())
    requests = []
    def handle(payload, *_args):
        requests.append(build_company_assistant_request(payload))
    app = common.create_slack_app(handle)
    say = Mock(side_effect=AssertionError("회귀 검증에서 Slack 발송은 금지"))
    app.handlers["app_mention"]({"text": raw, "user": "UACTOR", "channel": HUB,
        "team": TENANT, "ts": "1792392600.000001"}, say, object(), context={"bot_user_id": "UBOT"})
    (request,) = requests
    assert request.question == expected
    say.assert_not_called()
    store = CandidateStore()
    memory = MemoryService(store, enabled=True, read_enabled=True, profile_ids=frozenset({"UACTOR"}))
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, callers=(CompanyApiCallerSettings(
        caller_id="mention-transport", token=TOKEN, tenant_ids=frozenset({TENANT}),
        channels=frozenset({"slack"}), actor_ids=frozenset({"UACTOR"}),
        capabilities=frozenset({"assistant.turn.read"})),))
    api = create_company_api_app(settings=settings, assistant_runtime=SimpleNamespace(),
        readiness_probe=lambda: True, automation_coordinator=None,
        automation_delivery_broker=None, hpa_change_coordinator=None, memory_service=memory)
    with TestClient(api) as http:
        response = http.post("/internal/v1/memory/retrievals", headers={"Authorization": f"Bearer {TOKEN}",
            "X-Request-ID": "mention-transport-http"},
            json={"tenantId": request.tenant_id, "actorId": request.actor_id,
                  "channel": request.channel, "conversationId": request.conversation_id,
                  "question": request.question, "requestId": request.request_id, "contextEntries": [],
                  "memoryContext": {"workspaceId": TENANT, "actorId": "UACTOR", "channelId": HUB,
                      "threadTs": request.conversation_id, "currentMessageTs": request.metadata["message_id"],
                      "channelKind": "private_channel", "triggerKind": "app_mention"}})
    assert response.status_code == 200
    plan = response.json()["liveQuery"]
    assert plan["question"] == expected
    assert plan["authorIds"] == ["UTARGET"]
    assert plan["beforeTs"] == "1792392600.000001"
    assert store.queries[0]["mentioned_user_ids"] == ("UTARGET",)
