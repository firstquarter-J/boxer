"""실제 HTTP 계약·SQLite와 mock Slack/model을 연결하는 기억 회귀 검증."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json
import logging
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import urlsplit

from fastapi.testclient import TestClient
from httpx import Response
import pytest

from boxer_company.assistant.freeform_route import CompanyFreeformAssistantRoute
from boxer_company.memory.contracts import canonical_digest, source_revision
from boxer_company.memory.extraction import MemoryExtractor, ProviderResult
from boxer_company.memory.store import MemoryStore, initialize_memory_store
from boxer_company.memory.worker import MemoryWorker
from boxer_company_adapter_slack.assistant_bridge import (
    build_company_assistant_request,
    render_company_assistant_result,
)
from boxer_company_adapter_slack.company_api_client import CompanyApiClientSettings, CompanyAssistantApiClient
from boxer_company_adapter_slack.company_api_rollout import CompanyFreeformApiRolloutService
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
        self.responses = []

    def post(self, url, **kwargs):
        self.calls.append((urlsplit(url).path, kwargs.get("json")))
        response = self.client.post(urlsplit(url).path, json=kwargs.get("json"), headers=kwargs.get("headers"), follow_redirects=False)
        self.responses.append(response)
        return response


class Slack:
    def __init__(self):
        self.threads = {("C1", FIRST): [{"ts": FIRST, "thread_ts": FIRST, "user": "UA", "text": PREFERENCE}]}

    def auth_test(self):
        return {"ok": True, "user_id": "UBOT"}

    def conversations_info(self, *, channel, include_num_members=False):
        # 실제 adapter와 같은 채널 종류 확인을 제공하되 외부 Slack은 호출하지 않는다.
        return {"ok": True, "channel": {"id": channel, "is_private": False}}

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
            # 저장 조회 상태는 근거 항목과 구별하며 빈 DB의 상태만으로 인용하지 않는다.
            rows = json.loads(context.split("retrieved_memory(JSON 참고 데이터):\n", 1)[1])
            memory = next((row for row in rows if row.get("memoryId")), None)
            if memory is not None:
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

    def turn(self, question, timestamp, *, channel="C1", group="freeform", through_rollout=False):
        payload = {"workspace_id": "T1", "user_id": "UA", "channel_id": channel,
                   "current_ts": timestamp, "thread_ts": timestamp, "question": question,
                   "raw_text": question, "text": question, "request_log": {}}
        request = build_company_assistant_request(payload)
        request = replace(request, memory_context=self.memory.context_for(payload, manage=group == "operations"))
        if through_rollout:
            service = CompanyFreeformApiRolloutService(None, api_client=self.client,
                logger=logging.getLogger(__name__))
            return service.answer(request)
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
        # 일반 채널의 저장 기억 인용도 실제 Slack wrapper를 지나 사용자 출력까지 살아야 한다.
        result = self.turn("나는 어떤 답변을 좋아하지?", SECOND, through_rollout=True)
        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, "answered")
        evidence = [entry for entry in self.runtime.requests[-1].memory_entries if entry.get("kind") != "retrieval_status"]
        self.assertEqual(len(evidence), 1)
        self.assertIn("결론부터 짧게", self.runtime.inputs[-1][1])
        self.assertTrue(result.sources)
        self.assertIn("/archives/C1/", result.sources[0].uri)
        self.assertTrue(any(path.endswith("/retrievals") for path, _ in self.session.calls))
        reply = Mock()
        sent = render_company_assistant_result(result, reply=reply, actor_id="UA", client=None,
            logger=logging.getLogger(__name__))
        self.assertEqual(sent, 1)
        reply.assert_called_once()
        rendered = reply.call_args.args[0]
        self.assertIn("결론부터 짧게", rendered)
        self.assertIn("<" + result.sources[0].uri + "|이전 대화 근거>", rendered)
        self.assertNotIn("[memory:", rendered)
        self.assertNotIn("일반 답변 서비스 상태를 확인할 수 없어", rendered)

    def test_other_channel_and_deleted_source_are_not_reused(self):
        self.seed()
        self.turn("내 답변 선호가 뭐였지?", SECOND, channel="C2")
        self.assertFalse([entry for entry in self.runtime.requests[-1].memory_entries if entry.get("kind") != "retrieval_status"])
        self.assertTrue(self.memory.source_event({"subtype": "message_deleted", "channel": "C1", "deleted_ts": FIRST},
                                 {"team_id": "T1", "event_id": "Ev-delete"}, self.slack))
        with sqlite3.connect(self.store.path) as db:
            self.assertEqual(db.execute("SELECT state FROM memory_sources WHERE message_ts=?", (FIRST,)).fetchone()[0], "deleted")
        self.turn("내 답변 선호가 뭐였지?", THIRD)
        self.assertFalse([entry for entry in self.runtime.requests[-1].memory_entries if entry.get("kind") != "retrieval_status"])

    def source_event(self, kind, *, identity="Ev-source", timestamp=FIRST, text="앞으로 자세히 답해줘"):
        event = {"subtype": "message_" + kind, "channel": "C1"}
        if kind == "deleted":
            event["deleted_ts"] = timestamp
        else:
            event["message"] = {"ts": timestamp, "text": text}
        return self.memory.source_event(event, {"team_id": "T1", "event_id": identity}, self.slack)

    def source_event_state(self):
        # 내부 journal ID와 기억 version을 함께 비교해 ACK 재시도가 재적용으로 바뀌지 않게 한다.
        with sqlite3.connect(self.store.path) as db:
            return {
                "events": db.execute("SELECT id,request_id,event_type,durability_state FROM memory_events WHERE event_type LIKE 'source_%' ORDER BY id").fetchall(),
                "items": db.execute("SELECT id,status,version FROM memory_items ORDER BY id").fetchall(),
                "sources": db.execute("SELECT id,state FROM memory_sources ORDER BY id").fetchall(),
            }

    def assert_source_event_applied_and_replay_acknowledged(self, kind):
        self.seed()
        self.assertTrue(self.source_event(kind))
        self.assertEqual(self.session.responses[-1].json(), {"eventId": "Ev-source", "invalidatedCount": 1})
        self.assertEqual(self.memory.status(), {"sourceEvents": "ok", "pendingSourceEvents": 0})
        applied = self.source_event_state()
        self.assertEqual(len(applied["events"]), 1)
        internal_id, request_id, event_type, durability = applied["events"][0]
        self.assertTrue(internal_id.startswith("evt_"))
        self.assertNotEqual(internal_id, request_id)
        self.assertEqual((request_id, event_type, durability), ("Ev-source", "source_" + kind, "pending"))
        self.assertEqual(applied["items"][0][1], "deleted" if kind == "deleted" else "conflicted")
        # 동일 Slack 이벤트가 다시 전달되어도 transport ID로 ACK하며 내부 삭제 이력은 보존한다.
        self.assertTrue(self.source_event(kind))
        self.assertEqual(self.session.responses[-1].json(), {"eventId": "Ev-source", "invalidatedCount": 0})
        self.assertEqual(self.memory.event_outbox.pending_count(), 0)
        self.assertEqual(self.source_event_state(), applied)

    def test_known_deleted_source_ack_preserves_internal_journal_and_replay(self):
        self.assert_source_event_applied_and_replay_acknowledged("deleted")

    def test_known_changed_source_ack_preserves_internal_journal_and_replay(self):
        self.assert_source_event_applied_and_replay_acknowledged("changed")

    def test_unknown_source_noop_is_acknowledged_without_creating_scope(self):
        # 학습하지 않은 일반 메시지의 변경/삭제도 대기열을 막지 않으며 새 source를 만들지 않는다.
        for kind in ("changed", "deleted"):
            for _ in range(2):
                self.assertTrue(self.source_event(kind, identity="Ev-noop-" + kind))
                self.assertEqual(self.session.responses[-1].json(),
                                 {"eventId": "Ev-noop-" + kind, "invalidatedCount": 0})
                self.assertEqual(self.memory.status(), {"sourceEvents": "ok", "pendingSourceEvents": 0})
        for index in range(2):
            self.memory.event_outbox.enqueue({"tenantId": "T1", "channelId": "C1", "messageTs": FIRST,
                "eventType": "deleted", "eventId": "Ev-queued-" + str(index)})
        self.assertEqual(self.memory.drain_source_events(), 2)
        self.assertEqual(self.memory.event_outbox.pending_count(), 0)
        with sqlite3.connect(self.store.path) as db:
            for table in ("memory_threads", "memory_sources", "memory_items", "memory_jobs", "memory_events"):
                self.assertEqual(db.execute("SELECT count(*) FROM " + table).fetchone()[0], 0)

    def test_unchanged_known_revision_noop_preserves_memory_and_acknowledges(self):
        self.seed()
        before = self.source_event_state()
        self.assertTrue(self.source_event("changed", text=PREFERENCE))
        self.assertEqual(self.session.responses[-1].json(), {"eventId": "Ev-source", "invalidatedCount": 0})
        self.assertEqual(self.memory.event_outbox.pending_count(), 0)
        self.assertEqual(self.source_event_state(), before)

    def test_wrong_ack_keeps_outbox_then_real_api_replay_drains_without_reapplying(self):
        self.seed()
        original_post = self.session.post

        def wrong_ack(url, **kwargs):
            # 실제 저장소 적용 뒤 응답 ID만 망가뜨려 전송 결과 유실·오연결 상황을 재현한다.
            response = original_post(url, **kwargs)
            return Response(response.status_code, json={**response.json(), "eventId": "wrong-event"})

        with patch.object(self.session, "post", side_effect=wrong_ack):
            self.assertTrue(self.source_event("deleted"))
        self.assertEqual(self.memory.status(), {"sourceEvents": "degraded", "pendingSourceEvents": 1})
        applied = self.source_event_state()
        self.assertEqual(len(applied["events"]), 1)
        self.assertEqual(self.memory.drain_source_events(), 1)
        self.assertEqual(self.session.responses[-1].json(), {"eventId": "Ev-source", "invalidatedCount": 0})
        self.assertEqual(self.memory.status(), {"sourceEvents": "ok", "pendingSourceEvents": 0})
        self.assertEqual(self.source_event_state(), applied)

    def test_failed_store_keeps_outbox_until_successful_retry(self):
        self.seed()
        before = self.source_event_state()
        # HTTP 503은 성공 ACK로 바꾸지 않는다. 보호 원문 같은 예외 내용도 응답에서 제외한다.
        with patch.object(self.store, "source_event", side_effect=RuntimeError("private error body")):
            self.assertTrue(self.source_event("deleted"))
        response = self.session.responses[-1]
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("private error body", response.text)
        self.assertEqual(self.memory.status(), {"sourceEvents": "degraded", "pendingSourceEvents": 1})
        self.assertEqual(self.source_event_state(), before)
        self.assertEqual(self.memory.drain_source_events(), 1)
        self.assertEqual(self.session.responses[-1].json(), {"eventId": "Ev-source", "invalidatedCount": 1})
        self.assertEqual(self.memory.status(), {"sourceEvents": "ok", "pendingSourceEvents": 0})
        self.assertEqual(len(self.source_event_state()["events"]), 1)

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


@pytest.mark.parametrize("destination,origin,private_destination,allowed_stored_scope", [
    ("C068FVD5V7Y", "CPRIVATE", True, True),
    ("COTHER", "CPRIVATE", True, False),
    ("COTHER", "COTHER", True, True),
    ("CPUBLIC", "CPRIVATE", False, False),
])
@pytest.mark.parametrize("has_stored", [False, True])
def test_stored_scope_uses_only_existing_memory_without_reading_unstored_slack_reply(
    tmp_path, monkeypatch, destination, origin, private_destination, allowed_stored_scope, has_stored,
):
    from boxer_company import team_chat_context

    tenant, author = "T19PA3EPL", "UMARU"
    statement = "나는 내일 회의에서 안내문 문구부터 정리할 거야"
    stored_text = "회의에서는 안내문 문구부터 정리하자는 의견을 냄"
    monkeypatch.setattr(team_chat_context, "TEAM_MEMBER_PROFILES", (
        {"name": "Maru", "aliases": ("maru", "마루"), "user_id": author},
    ))

    class PrivateSlack(Slack):
        def __init__(self):
            super().__init__()
            self.raw_reads = []
            self.threads[(origin, FIRST)] = [
                {"ts": FIRST, "thread_ts": FIRST, "user": "UA", "text": "회의 순서 얘기하자"},
                {"ts": SECOND, "thread_ts": FIRST, "user": author, "text": statement},
            ]

        def conversations_info(self, *, channel, **kwargs):
            return {"ok": True, "channel": {"id": channel, "is_private": private_destination,
                                           "is_member": True, "context_team_id": tenant}}

        def conversations_list(self, **kwargs):
            self.raw_reads.append("list")
            raise AssertionError("저장 DB 조회는 Slack 채널 목록을 읽지 않는다")

        def conversations_history(self, **kwargs):
            self.raw_reads.append("history")
            raise AssertionError("저장 DB 조회는 Slack history를 읽지 않는다")

        def conversations_replies(self, **kwargs):
            self.raw_reads.append("replies")
            raise AssertionError("저장 DB 조회는 Slack 댓글을 읽지 않는다")

        def chat_getPermalink(self, **kwargs):
            self.raw_reads.append("permalink")
            raise AssertionError("저장 DB 조회는 Slack 원문 링크를 새로 만들지 않는다")

    path = tmp_path / "private" / "memory.db"
    initialize_memory_store(path)
    store = MemoryStore(path, debounce_seconds=0)
    if has_stored:
        # 조회 전에 정상 capture·추출 경로로 채택한 DB 기억을 준비한다. 조회 자체는 새 자료를 저장하지 않는다.
        capture = store.create_capture(tenant_id=tenant, actor_id=author, channel_id=origin, thread_ts=FIRST,
            current_message_ts=SECOND, channel_kind="private_channel", request_id="stored-scope-fixture", route="company_freeform")
        sources = [{"messageTs": SECOND, "revisionHash": source_revision(statement), "authorId": author,
            "authorKind": "human", "text": statement, "createdAt": SECOND, "editedAt": "",
            "permalink": f"https://example.slack.com/archives/{origin}/p{SECOND.replace('.', '')}"}]
        store.put_chunk(tenant_id=tenant, actor_id=author, capture_id=capture["captureId"], claim_token=capture["claimToken"],
            chunk_no=0, sources=sources, coverage="complete", is_last=True, total_chunks=1,
            input_digest=canonical_digest([canonical_digest(sources)]))

        def extract(system, text):
            source = json.loads(text)["sources"][0]
            candidate = {"kind": "person", "category": "fact", "subjectUserId": author, "topicKey": "회의 준비",
                "text": stored_text, "evidenceType": "self_statement", "sources": [{"sourceId": source["sourceId"],
                "quote": statement, "relation": "supports"}], "subjects": [], "action": "add"}
            return ProviderResult(json.dumps({"candidates": [candidate]}, ensure_ascii=False), "stub", 10, 10)

        assert MemoryWorker(store, MemoryExtractor(extract)).run_once()["state"] == "succeeded"
        with sqlite3.connect(path) as db:
            assert db.execute("SELECT count(*) FROM memory_items WHERE status='active'").fetchone()[0] == 1
    memory_service = MemoryService(store, enabled=True, read_enabled=True, profile_ids=frozenset({"UA"}))
    inputs = []
    expected_stored = has_stored and allowed_stored_scope

    def answerer(question, context, system):
        inputs.append((question, context, system))
        rows = json.loads(context.split("retrieved_memory(JSON 참고 데이터):\n", 1)[1])
        assert any(row.get("sourceMode") == "stored" for row in rows if row.get("kind") == "retrieval_status")
        memories = [row for row in rows if row.get("memoryId")]
        assert not any(row.get("kind") == "utterance" for row in memories)
        if not memories:
            return "저장된 근거가 없어"
        assert memories[0]["text"] == stored_text
        assert '"referenceDateKst":"2026-10-16"' in system
        return "저장된 기억에서 마루는 안내문 문구부터 정리하자고 했어 [memory:" + memories[0]["memoryId"] + "]"

    route = CompanyFreeformAssistantRoute(answerer, provider_ready=lambda: True,
        context_max_chars=5000, timeout_message="시간 초과")
    runtime = type("StoredRuntime", (), {"answer_stage": lambda _self, request, stage, **kwargs: route.handle(request)})()
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, memory_enabled=True, memory_read_enabled=True,
        memory_state_path=str(path), callers=(CompanyApiCallerSettings(caller_id="stored-scope", token=TOKEN,
            tenant_ids=frozenset({tenant}), channels=frozenset({"slack"}), actor_ids=frozenset({"UA"}),
            capabilities=frozenset({"assistant.turn.read"})),))
    app = create_company_api_app(settings=settings, assistant_runtime=runtime,
        readiness_probe=lambda: True, memory_service=memory_service)
    with TestClient(app) as web:
        session = LocalSession(web)
        client_settings = CompanyApiClientSettings(base_url="http://127.0.0.1:8010", token=TOKEN,
            connect_timeout_sec=2, read_timeout_sec=30, max_retries=0)
        api = CompanyAssistantApiClient(client_settings, session=session)
        slack = PrivateSlack()
        memory = SlackConversationMemory(CompanyMemoryApiClient(client_settings, session=session),
            is_allowed=lambda workspace, actor: workspace == tenant and actor in {"UA", author})
        memory.client, memory.tenant_id, memory.bot_user_id = slack, tenant, "UBOT"
        memory._profile_ids = {"UA"}
        api.memory_request_preparer = memory.prepare
        payload = {"workspace_id": tenant, "user_id": "UA", "channel_id": destination,
                   "current_ts": THIRD, "thread_ts": THIRD, "question": "오늘 마루가 한 말 요약해줘",
                   "raw_text": "오늘 마루가 한 말 요약해줘", "text": "오늘 마루가 한 말 요약해줘", "request_log": {}}
        request = build_company_assistant_request(payload)
        request = replace(request, memory_context=memory.context_for(payload))

        # 실제 HTTP와 SQLite의 저장 조회 범위부터 Slack wrapper의 인용 렌더링까지 연결한다.
        rollout = CompanyFreeformApiRolloutService(None, api_client=api, logger=logging.getLogger(__name__))
        result = rollout.answer(request)
        assert result is not None
        reply = Mock()
        sent = render_company_assistant_result(result, reply=reply, actor_id="UA", client=None,
            logger=logging.getLogger(__name__))

    assert result.outcome == "answered"
    assert sent == 1
    reply.assert_called_once()
    rendered = reply.call_args.args[0]
    assert "일반 답변 서비스 상태를 확인할 수 없어" not in rendered
    assert "[memory:" not in rendered
    assert bool(result.sources) is expected_stored
    assert slack.raw_reads == []
    assert not any(path.endswith("/live") for path, _ in session.calls)
    if expected_stored:
        assert stored_text in inputs[0][1] and "https://" not in inputs[0][1]
        assert result.sources[0].uri.endswith("/p" + SECOND.replace(".", ""))
        assert "안내문" in rendered
        assert "<" + result.sources[0].uri + "|이전 대화 근거>" in rendered
    else:
        assert "https://" not in rendered
        assert stored_text not in inputs[0][1]
        assert "저장된 근거가 없어" in rendered
    with sqlite3.connect(path) as db:
        assert db.execute("SELECT count(*) FROM memory_sources").fetchone()[0] == int(has_stored)
        assert db.execute("SELECT count(*) FROM memory_job_inputs").fetchone()[0] == 0


if __name__ == "__main__":
    unittest.main()
