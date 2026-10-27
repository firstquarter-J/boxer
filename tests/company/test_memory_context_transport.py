"""저장 DB 조회 계약은 Slack을 읽지 않고 관리 명령의 원문 확인만 유지한다."""

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from urllib.parse import urlsplit

from fastapi.testclient import TestClient
import pytest

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.memory.contracts import source_revision
from boxer_company_adapter_slack.company_api_client import CompanyApiClientSettings, CompanyApiContractError
from boxer_company_adapter_slack.conversation_memory import SlackConversationMemory
from boxer_company_adapter_slack.memory_api_client import CompanyMemoryApiClient
from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings


TENANT, ORIGIN, HUB = "T19PA3EPL", "CCTX", "C068FVD5V7Y"
PARENT, ANCHOR, FOLLOW, CUTOFF = ("1792335000.000001", "1792371600.000001",
                                   "1792371660.000001", "1792396000.000001")
TOKEN = "synthetic-context-transport-" + "x" * 40


class ReadOnlyStore:
    def __init__(self, items=()):
        self.items = list(items)

    def health(self):
        return True

    def retrieval_candidates(self, **_kwargs):
        return self.items

    def load_stored_items(self, **_kwargs):
        return self.items

    def create_capture(self, **_kwargs):
        raise AssertionError("실시간 문맥은 영속 capture를 만들지 않는다")


class SyntheticSlack:
    def __init__(self):
        self.messages = [
            {"ts": PARENT, "user": "UDOYUN", "text": "나래가 이번 발표 우수상을 받게 됐어요. 소감 부탁해요.",
             "reply_count": 2, "latest_reply": FOLLOW},
            {"ts": ANCHOR, "thread_ts": PARENT, "user": "UNARAE", "text": "제가 왜요"},
            {"ts": FOLLOW, "thread_ts": PARENT, "user": "UDOYUN", "text": "발표를 잘해 주셔서 상을 드려요."},
        ]
        self.calls = []

    @staticmethod
    def info():
        return {"id": ORIGIN, "is_private": True, "is_member": True, "context_team_id": TENANT}

    def conversations_info(self, **kwargs):
        self.calls.append(("info", kwargs))
        return {"ok": True, "channel": self.info()}

    def conversations_list(self, **kwargs):
        self.calls.append(("list", kwargs))
        return {"ok": True, "channels": [self.info()]}

    def conversations_history(self, **kwargs):
        self.calls.append(("history", kwargs))
        return {"ok": True, "messages": self.messages[:1], "has_more": False}

    def conversations_replies(self, **kwargs):
        self.calls.append(("replies", kwargs))
        return {"ok": True, "has_more": False, "messages": [message for message in self.messages
            if Decimal(kwargs.get("oldest", "0")) <= Decimal(message["ts"]) <= Decimal(kwargs["latest"])]}

    def chat_getPermalink(self, **kwargs):
        self.calls.append(("permalink", kwargs))
        return {"ok": True, "permalink": f"https://synthetic.slack.com/archives/{ORIGIN}/p{kwargs['message_ts'].replace('.', '')}"}


class HttpSession:
    def __init__(self, web):
        self.web, self.payloads = web, []

    def post(self, url, *, json, headers, **_kwargs):
        self.payloads.append((urlsplit(url).path, json))
        return self.web.post(urlsplit(url).path, json=json, headers=headers)


def test_live_transport_rejects_oversized_metadata_before_http_send():
    # 본문이 짧아도 메타데이터가 큰 결과는 동일한 byte 예산으로 송신 전에 차단한다.
    class NoSendSession:
        def post(self, *args, **kwargs):
            raise AssertionError("과대 원문 bundle은 HTTP로 보내면 안 된다")

    api = CompanyMemoryApiClient(CompanyApiClientSettings(base_url="http://127.0.0.1:8010", token=TOKEN),
        session=NoSendSession())
    with pytest.raises(CompanyApiContractError, match="company_memory_payload_too_large"):
        api.call("/internal/v1/memory/retrievals/example/live", {
            "sources": [{"text": "가", "metadata": "x" * 2000} for _ in range(600)]})


def memory_request(destination=ORIGIN, **changes):
    request = CompanyAssistantRequest(request_id="context-http-test", tenant_id=TENANT, actor_id="UDOYUN",
        channel="slack", conversation_id=CUTOFF, question="오늘 나래가 한 말 요약해줘", locale="ko-KR",
        metadata={"route_group": "freeform", "channel_id": destination},
        memory_context={"workspaceId": TENANT, "actorId": "UDOYUN", "channelId": destination,
            "channelKind": "private_channel", "threadTs": CUTOFF, "currentMessageTs": CUTOFF, "triggerKind": "app_mention"})
    return replace(request, **changes)


def reference():
    return {"sourceId": "src_narae", "channelId": ORIGIN, "channelKind": "private_channel",
            "threadTs": PARENT, "messageTs": ANCHOR, "revisionHash": source_revision("제가 왜요")}


class PreparedApi:
    def __init__(self, response):
        self.response, self.calls = response, []

    def call(self, path, payload):
        self.calls.append((path, payload))
        return self.response


@pytest.mark.parametrize("destination", [ORIGIN, HUB])
def test_stored_only_candidates_skip_all_slack_reads_and_bind_the_retrieval_id(monkeypatch, destination):
    # 후보와 옛 liveQuery가 함께 와도 storedOnly 계약은 저장 DB 결과만 연결한다.
    api = PreparedApi({"retrievalId": "stored_one", "sources": [reference()], "storedOnly": True,
                       "liveQuery": {"authorIds": ["UNARAE"]}})
    slack = SyntheticSlack()
    adapter = SlackConversationMemory(api, is_allowed=lambda *_: True)
    adapter.client = slack
    monkeypatch.setattr(adapter, "_bounded_client", lambda _timeout: slack)
    request = memory_request(destination)
    prepared = adapter.prepare(request)
    assert prepared.memory_context["retrievalId"] == "stored_one"
    assert "verifications" not in prepared.memory_context
    assert request.memory_context.get("retrievalId") is None
    assert slack.calls == []
    assert len(api.calls) == 1 and api.calls[0][0].endswith("/retrievals")


def test_count_without_a_retrieval_does_not_start_another_transport_or_slack_read():
    api = PreparedApi({"retrievalId": None, "sources": []})
    adapter = SlackConversationMemory(api, is_allowed=lambda *_: True)
    adapter.client = slack = SyntheticSlack()
    request = memory_request(question="디디가 올해 욕 몇 번 했어?")
    assert adapter.prepare(request) is request
    assert len(api.calls) == 1 and slack.calls == []


def test_memory_management_keeps_source_revision_verification_even_with_stored_only_marker(monkeypatch):
    # 관리 명령은 저장된 기억의 정정·삭제 대상 검증을 기존처럼 수행한다.
    api = PreparedApi({"retrievalId": "manage_one", "sources": [reference()], "storedOnly": True})
    adapter = SlackConversationMemory(api, is_allowed=lambda *_: True)
    adapter.client = slack = SyntheticSlack()
    monkeypatch.setattr(adapter, "_bounded_client", lambda _timeout: slack)
    prepared = adapter.prepare(memory_request(question="내 기억 보여줘"))
    assert prepared.memory_context["retrievalId"] == "manage_one"
    assert prepared.memory_context["verifications"] == [{"sourceId": "src_narae",
        "revisionHash": source_revision("제가 왜요"), "status": "verified"}]
    assert [method for method, _ in slack.calls] == ["replies"]


@pytest.mark.parametrize("destination", [ORIGIN, HUB])
@pytest.mark.parametrize("has_stored", [False, True])
def test_stored_only_contract_crosses_actual_http_without_starting_slack_collection(monkeypatch, destination, has_stored):
    # 저장된 기억만 전달하며 비어 있을 때도 Slack에서 새 원문을 채우지 않는다.
    item = {"memoryId": "mem_stored", "kind": "person_fact", "subjectUserId": "UNARAE",
        "text": "나래가 발표 우수상을 받은 소감을 이야기함", "status": "active", "sources": [reference()]}
    items = [item] if has_stored else []
    service = MemoryService(ReadOnlyStore(items), enabled=True, read_enabled=True, profile_ids=frozenset({"UDOYUN"}))
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, memory_enabled=True, memory_read_enabled=True,
        callers=(CompanyApiCallerSettings(caller_id="context-test", token=TOKEN, tenant_ids=frozenset({TENANT}),
            channels=frozenset({"slack"}), actor_ids=frozenset({"UDOYUN"}),
            capabilities=frozenset({"assistant.turn.read"})),))
    app = create_company_api_app(settings=settings, assistant_runtime=SimpleNamespace(), readiness_probe=lambda: True,
        automation_coordinator=None, automation_delivery_broker=None, hpa_change_coordinator=None, memory_service=service)
    slack = SyntheticSlack()
    with TestClient(app) as web:
        session = HttpSession(web)
        api = CompanyMemoryApiClient(CompanyApiClientSettings(base_url="http://127.0.0.1:8010", token=TOKEN), session=session)
        adapter = SlackConversationMemory(api, is_allowed=lambda tenant, user: tenant == TENANT and user in {"UNARAE", "UDOYUN"})
        adapter.client = slack
        monkeypatch.setattr(adapter, "_bounded_client", lambda _timeout: slack)
        prepared = adapter.prepare(memory_request(destination))
        assert prepared.memory_context.get("retrievalId")
        assert "verifications" not in prepared.memory_context
        bound = service.bind_verified(prepared)
        assert [entry for entry in bound.memory_entries if entry.get("kind") != "retrieval_status"] == items
        assert not any(entry.get("liveSource") is True for entry in bound.memory_entries)
    assert [path for path, _ in session.payloads] == ["/internal/v1/memory/retrievals"]
    assert slack.calls == []
    assert not service._cache
