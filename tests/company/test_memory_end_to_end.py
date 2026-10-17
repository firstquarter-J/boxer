"""실제 HTTP 계약·SQLite와 mock Slack/model을 연결하는 기억 회귀 검증."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from urllib.parse import urlsplit

from fastapi.testclient import TestClient

from boxer_company.assistant.freeform_route import CompanyFreeformAssistantRoute
from boxer_company.memory.extraction import MemoryExtractor, ProviderResult
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company.memory.worker import MemoryWorker
from boxer_company_adapter_slack.assistant_bridge import build_company_assistant_request
from boxer_company_adapter_slack.company_api_client import CompanyApiClientSettings, CompanyAssistantApiClient
from boxer_company_adapter_slack.conversation_memory import SlackConversationMemory
from boxer_company_adapter_slack.memory_api_client import CompanyMemoryApiClient
from boxer_company_adapter_slack.memory_event_outbox import MemorySourceEventOutbox
from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings


TOKEN = "memory-test-token-" + "x" * 40
FIRST = "1792100000.000001"
BOT = "1792100001.000001"
SECOND = "1792100010.000001"
THIRD = "1792100020.000001"
PREFERENCE = "앞으로 결론부터 짧게 말해줘"


class LocalSession:
    def __init__(self, client):
        self.client = client
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((urlsplit(url).path, kwargs.get("json")))
        return self.client.post(urlsplit(url).path, json=kwargs.get("json"), headers=kwargs.get("headers"), follow_redirects=False)


class Slack:
    def __init__(self):
        self.threads = {("C1", FIRST): [{"ts": FIRST, "thread_ts": FIRST, "user": "UA", "text": PREFERENCE}]}

    def auth_test(self):
        return {"ok": True, "user_id": "UBOT"}

    def chat_getPermalink(self, *, channel, message_ts):
        return {"ok": True, "permalink": f"https://example.slack.com/archives/{channel}/p{message_ts.replace('.', '')}"}

    def conversations_replies(self, *, channel, ts, oldest=None, latest=None, **kwargs):
        del kwargs
        return {"ok": True, "messages": [m for m in self.threads.get((channel, ts), [])
                if (oldest is None or Decimal(m["ts"]) >= Decimal(oldest))
                and (latest is None or Decimal(m["ts"]) <= Decimal(latest))], "has_more": False}


class Runtime:
    def __init__(self):
        self.requests = []
        self.inputs = []
        self.route = CompanyFreeformAssistantRoute(self.answerer, provider_ready=lambda: True,
            context_max_chars=5000, timeout_message="시간 초과", provider="claude")

    def answerer(self, question, context, system):
        self.inputs.append((question, context, system))
        if "retrieved_memory(JSON 참고 데이터):\n" in context:
            memory = json.loads(context.split("retrieved_memory(JSON 참고 데이터):\n", 1)[1])[0]
            return "결론부터 짧게 답하는 걸 선호해 [memory:" + memory["memoryId"] + "]"
        return "결론부터 짧게 답할게"

    def answer_stage(self, request, stage, **kwargs):
        self.requests.append(request)
        return self.route.handle(request)


class MemoryEndToEndTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        path = Path(self.directory.name).resolve() / "memory.db"
        initialize_memory_store(path)
        self.store = MemoryStore(path, debounce_seconds=0)
        self.service = MemoryService(self.store, enabled=True, read_enabled=True, profile_ids=frozenset({"UA"}))
        settings = CompanyApiSettings(host="127.0.0.1", port=8010, callers=(CompanyApiCallerSettings(
            caller_id="slack-memory-test", token=TOKEN, tenant_ids=frozenset({"T1"}),
            channels=frozenset({"slack"}), actor_ids=frozenset({"UA"}),
            capabilities=frozenset({"assistant.turn.read", "assistant.operation.execute"})),),
            memory_enabled=True, memory_read_enabled=True, memory_state_path=str(path))
        self.runtime = Runtime()
        app = create_company_api_app(settings=settings, assistant_runtime=self.runtime,
            readiness_probe=lambda: True, automation_coordinator=None,
            automation_delivery_broker=None, hpa_change_coordinator=None, memory_service=self.service)
        self.web = TestClient(app)
        self.addCleanup(self.web.close)
        self.session = LocalSession(self.web)
        client_settings = CompanyApiClientSettings(base_url="http://127.0.0.1:8010", token=TOKEN,
            connect_timeout_sec=2, read_timeout_sec=30, max_retries=0)
        self.client = CompanyAssistantApiClient(client_settings, session=self.session)
        self.slack = Slack()
        self.memory = SlackConversationMemory(CompanyMemoryApiClient(client_settings, session=self.session),
            is_allowed=lambda tenant, actor: tenant == "T1" and actor == "UA",
            event_outbox=MemorySourceEventOutbox(path.parent / "events.json"))
        self.memory._profile_ids = {"UA"}
        self.memory.client, self.memory.tenant_id = self.slack, "T1"
        self.memory.bot_user_id = "UBOT"
        self.client.memory_request_preparer = self.memory.prepare

    def turn(self, question, timestamp, *, channel="C1", group="freeform"):
        payload = {"workspace_id": "T1", "user_id": "UA", "channel_id": channel,
                   "current_ts": timestamp, "thread_ts": timestamp, "question": question,
                   "raw_text": question, "text": question, "request_log": {}}
        request = build_company_assistant_request(payload)
        request = replace(request, memory_context=self.memory.context_for(payload, manage=group == "operations"))
        return self.client.answer(request, route_group=group)

    def seed(self):
        result = self.turn(PREFERENCE, FIRST)
        self.assertIsNotNone(result.memory_capture, "정상 최종 응답에 capture를 전달해야 함")
        capture = result.memory_capture
        receipts = []
        def send(text, **kwargs):
            message = {"ts": BOT, "thread_ts": FIRST, "user": "UBOT", "bot_id": "B1",
                       "text": "<@UA> " + text, "client_msg_id": kwargs["client_msg_id"]}
            self.slack.threads[("C1", FIRST)].append(message)
            return {"ok": True, "ts": BOT, "message": message}
        self.memory.delivery_reply(send, capture, receipts)(result.messages[0].body)
        self.memory.record_delivery(capture, receipts)
        self.memory.poll_once()
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM memory_sources").fetchone()[0], 2,
                             "실제 전달 receipt와 사람 원문을 모두 저장해야 함")

        def model(system, text):
            del system
            sources = json.loads(text)["sources"]
            candidates = []
            for source in sources:
                if source["authorKind"] == "human" and PREFERENCE in source["text"]:
                    candidates.append({"kind": "person", "category": "preference", "subjectUserId": "UA",
                        "topicKey": "answer_style", "text": "결론부터 짧게 답하는 방식을 선호함",
                        "evidenceType": "self_statement", "sources": [{"sourceId": source["sourceId"],
                        "quote": PREFERENCE, "relation": "supports"}], "subjects": [], "action": "add"})
            return ProviderResult(json.dumps({"candidates": candidates}, ensure_ascii=False), "stub", 10, 10)
        outcome = MemoryWorker(self.store, MemoryExtractor(model)).run_once()
        self.assertEqual(outcome["state"], "succeeded", outcome)

    def test_real_capture_delivery_extract_and_retrieval_roundtrip(self):
        self.seed()
        result = self.turn("나는 어떤 답변을 좋아하지?", SECOND)
        self.assertEqual(len(self.runtime.requests[-1].memory_entries), 1)
        self.assertIn("결론부터 짧게", self.runtime.inputs[-1][1])
        self.assertTrue(result.sources)
        self.assertIn("/archives/C1/", result.sources[0].uri)
        self.assertTrue(any(path.endswith("/retrievals") for path, _ in self.session.calls))

    def test_other_channel_and_deleted_source_are_not_reused(self):
        self.seed()
        self.turn("내 답변 선호가 뭐였지?", SECOND, channel="C2")
        self.assertEqual(self.runtime.requests[-1].memory_entries, ())
        self.memory.source_event({"subtype": "message_deleted", "channel": "C1", "deleted_ts": FIRST},
                                 {"team_id": "T1", "event_id": "Ev-delete"}, self.slack)
        self.turn("내 답변 선호가 뭐였지?", THIRD)
        self.assertEqual(self.runtime.requests[-1].memory_entries, ())

    def test_manual_preference_uses_original_source_and_same_actor_scope(self):
        text = "내 답변 선호를 짧게로 바꿔줘"
        result = self.turn(text, FIRST, group="operations")
        self.assertEqual(result.route, "conversation_memory_manage")
        self.assertEqual(result.outcome, "answered", result.messages)
        self.assertIsNone(result.memory_capture)

    def test_wire_split_messages_keep_matching_capture_delivery_parts(self):
        # API가 본문을 나눠도 Slack의 각 실제 발송과 capture part가 1:1로 맞아야 한다.
        self.runtime.route._answerer = lambda *_: "가" * 30001
        result = self.turn(PREFERENCE, FIRST)
        self.assertEqual(len(result.messages), 2)
        self.assertEqual(len(result.memory_capture["answerParts"]), 2)
        receipts = []
        def send(text, **kwargs):
            timestamp = f"{1792100001 + len(receipts)}.000001"
            message = {"ts": timestamp, "thread_ts": FIRST, "user": "UBOT", "bot_id": "B1",
                       "text": text, "client_msg_id": kwargs["client_msg_id"]}
            self.slack.threads[("C1", FIRST)].append(message)
            return {"ok": True, "ts": timestamp, "message": message}
        reply = self.memory.delivery_reply(send, result.memory_capture, receipts)
        for message in result.messages:
            reply(message.body)
        self.memory.record_delivery(result.memory_capture, receipts)
        with sqlite3.connect(self.store.path) as db:
            metadata = json.loads(db.execute("SELECT metadata FROM memory_jobs").fetchone()[0])
            self.assertEqual(len(metadata["checkpoint"]["deliveryReceipts"]), 2)
        self.memory.poll_once()
        with sqlite3.connect(self.store.path) as db:
            # 16k 초과한 첫 봇 part는 제외하되 작은 둘째 part의 전달 근거는 보존한다.
            self.assertEqual(db.execute("SELECT count(*) FROM memory_sources").fetchone()[0], 2)
            self.assertEqual(db.execute("SELECT coverage FROM memory_threads").fetchone()[0], "partial")


if __name__ == "__main__":
    unittest.main()
