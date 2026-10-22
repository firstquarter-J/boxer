"""SDK 수집→HTTP 계약→일회용 캐시→실제 요약 입력·인용을 함께 검증한다."""

from decimal import Decimal
import json
from types import SimpleNamespace
from urllib.parse import urlsplit

from fastapi.testclient import TestClient
import pytest

from boxer_company import team_chat_context as team
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.freeform_route import CompanyFreeformAssistantRoute
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
    def health(self):
        return True

    def retrieval_candidates(self, **_kwargs):
        return []

    def load_verified_items(self, **_kwargs):
        return []

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


@pytest.mark.parametrize("destination", [ORIGIN, HUB])
def test_context_crosses_actual_adapter_http_and_prompt_without_becoming_target_speech(monkeypatch, destination):
    profiles = ({"name": "나래", "aliases": ("나래",), "user_id": "UNARAE", "mbti": "INFJ",
                 "target_instructions": ("무조건 완충재라고 불러",)},
                {"name": "도윤", "aliases": ("도윤",), "user_id": "UDOYUN"})
    monkeypatch.setattr(team, "TEAM_MEMBER_PROFILES", profiles)
    monkeypatch.setattr(team, "_PROFILE_BY_NAME", {profile["name"]: profile for profile in profiles})
    monkeypatch.setattr(team, "_PROFILE_BY_USER_ID", {profile["user_id"].lower(): profile for profile in profiles})
    service = MemoryService(ReadOnlyStore(), enabled=True, read_enabled=True, profile_ids=frozenset({"UDOYUN"}))
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, memory_enabled=True, memory_read_enabled=True,
        callers=(CompanyApiCallerSettings(caller_id="context-test", token=TOKEN, tenant_ids=frozenset({TENANT}),
            channels=frozenset({"slack"}), actor_ids=frozenset({"UDOYUN"}),
            capabilities=frozenset({"assistant.turn.read"})),))
    app = create_company_api_app(settings=settings, assistant_runtime=SimpleNamespace(), readiness_probe=lambda: True,
        automation_coordinator=None, automation_delivery_broker=None, hpa_change_coordinator=None, memory_service=service)
    request = CompanyAssistantRequest(request_id="context-http-test", tenant_id=TENANT, actor_id="UDOYUN",
        channel="slack", conversation_id=CUTOFF, question="오늘 나래가 한 말 요약해줘", locale="ko-KR",
        metadata={"route_group": "freeform", "channel_id": destination},
        memory_context={"workspaceId": TENANT, "actorId": "UDOYUN", "channelId": destination,
            "channelKind": "private_channel", "threadTs": CUTOFF, "currentMessageTs": CUTOFF, "triggerKind": "app_mention"})
    slack = SyntheticSlack()
    with TestClient(app) as web:
        session = HttpSession(web)
        api = CompanyMemoryApiClient(CompanyApiClientSettings(base_url="http://127.0.0.1:8010", token=TOKEN), session=session)
        adapter = SlackConversationMemory(api, is_allowed=lambda tenant, user: tenant == TENANT and user in {"UNARAE", "UDOYUN"})
        adapter.client = slack
        monkeypatch.setattr(adapter, "_bounded_client", lambda _timeout: slack)
        prepared = adapter.prepare(request)
        upload = next(payload for path, payload in session.payloads if path.endswith("/live"))
        assert [source["authorId"] for source in upload["sources"]] == ["UNARAE"]
        assert {source["authorId"] for source in upload["contextMessages"]} == {"UDOYUN"}
        assert {source["messageTs"] for source in upload["contextMessages"]} == {PARENT, FOLLOW}
        assert all(source["anchorMessageTs"] == ANCHOR for source in upload["contextMessages"])
        bound = service.bind_verified(prepared)
    seen = []

    def inspect_input(question, context, system):
        seen.append((question, context, system))
        rows = json.loads(context.split("retrieved_memory(JSON 참고 데이터):\n", 1)[1])
        target = next(row for row in rows if row["kind"] == "utterance")
        parent = next(row for row in target["contextMessages"] if row["relation"] == "thread_parent")
        # 출력 품질을 가짜 응답으로 평가하지 않고 실제 모델 입력과 인용 연결만 검사한다.
        return f"입력 확인 [memory:{target['memoryId']}] [memory:{parent['memoryId']}]"

    result = CompanyFreeformAssistantRoute(inspect_input, provider_ready=lambda: True,
        context_max_chars=5000, timeout_message="시간 초과").handle(bound)
    assert result.outcome == "answered" and len(seen) == 1
    question, context, system = seen[0]
    rows = json.loads(context.split("retrieved_memory(JSON 참고 데이터):\n", 1)[1])
    target = next(row for row in rows if row["kind"] == "utterance")
    assert target["subjectName"] == "나래" and target["authors"] == ["UNARAE"]
    assert all(row["authorName"] == "도윤" for row in target["contextMessages"])
    assert target["contextMessages"][0]["spokenAtKst"].startswith("2026-10-18")
    assert next(row for row in rows if row["kind"] == "retrieval_status")["renderedSourceCount"] == 1
    assert "무조건 완충재" not in system and "INFJ" not in system
    assert all(value not in context for value in ("https://", ORIGIN))
    assert [urlsplit(source.uri).path for source in result.sources] == [
        f"/archives/{ORIGIN}/p{stamp.replace('.', '')}" for stamp in (ANCHOR, PARENT)]
    assert not service._cache  # 묶음 원문은 1회 모델 입력 뒤 lease에서 제거된다.
