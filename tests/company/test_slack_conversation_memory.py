from __future__ import annotations

from dataclasses import replace
import json
import logging
import unittest

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.memory.contracts import MAX_CHUNK_CHARS, canonical_digest, source_revision
from boxer_company_adapter_slack.conversation_memory import SlackConversationMemory, delivery_client_id
from boxer_company_adapter_slack.company_api_client import _deserialize_result_payload, _serialize_request


ROOT = "1792100000.000001"
NOW = "1792100001.000001"
AFTER = "1792100002.000001"
SILENT = logging.getLogger(__name__ + ".silent")
SILENT.disabled = True


def human(ts=ROOT, user="UA", text="앞으로 결론부터 말해줘"):
    return {"ts": ts, "user": user, "text": text}


def capture(**changes):
    value = {"captureId": "cap_1", "tenantId": "T1", "actorId": "UA",
             "channelId": "C1", "threadTs": ROOT, "currentMessageTs": NOW,
             "channelKind": "channel", "requestId": "request1", "route": "company_freeform",
             "state": "collecting", "deletionEpoch": 0, "claimToken": "claim1",
             "checkpoint": {}, "receivedChunks": [], "answerParts": []}
    value.update(changes)
    return value


def request():
    return CompanyAssistantRequest("request1", "T1", "UA", "slack", ROOT,
        "이전 취향 기억해?", "ko", metadata={"channel_id": "C1", "message_id": NOW})


class Api:
    def __init__(self):
        self.calls = []
        self.retrieval = {"retrievalId": "retrieval1", "sources": []}

    def call(self, path, payload):
        self.calls.append((path, payload))
        if path.endswith("/sources/events"):
            return {"eventId": payload["eventId"], "invalidatedCount": 1}
        return self.retrieval if path.endswith("/retrievals") else {}


class Outbox:
    def __init__(self):
        self.events = {}

    def enqueue(self, event):
        self.events[event["eventId"]] = dict(event)

    def pending(self, *, limit=25):
        return list(self.events.values())[:limit]

    def pending_count(self):
        return len(self.events)

    def acknowledge(self, event):
        self.events.pop(event["eventId"], None)


class Slack:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def conversations_replies(self, **kwargs):
        self.calls.append(kwargs)
        return self.pages[kwargs.get("cursor") or ""]

    def chat_getPermalink(self, *, channel, message_ts):
        return {"permalink": f"https://unit.slack.com/archives/{channel}/p{message_ts.replace('.', '')}"}


def bridge(slack=None):
    memory = SlackConversationMemory(Api(), is_allowed=lambda tenant, actor: tenant == "T1" and actor in {"UA", "UB"}, logger=SILENT, event_outbox=Outbox())
    memory._profile_ids = {"UA"}
    memory.client = slack
    memory.bot_user_id = "UBOT"
    return memory


class SlackConversationMemoryTests(unittest.TestCase):
    def test_pagination_keeps_authors_and_current_message_but_excludes_later_messages(self):
        # 멘션 직후 새 댓글이 들어와도 cutoff 이후 사람 발언을 기억하지 않는다.
        slack = Slack({"": {"ok": True, "messages": [human()], "has_more": True, "response_metadata": {"next_cursor": "page2"}},
                       "page2": {"ok": True, "messages": [human(NOW, "UB", "내가 작업을 맡을게"), human(AFTER)], "has_more": False}})
        memory = bridge(slack)
        memory.collect_capture(capture())
        uploads = [body for path, body in memory.api.calls if path.endswith("/chunks")]
        sources = [source for upload in uploads for source in upload["sources"]]
        self.assertEqual([(s["messageTs"], s["authorId"]) for s in sources], [(ROOT, "UA"), (NOW, "UB")])
        self.assertEqual(slack.calls[1]["cursor"], "page2")
        self.assertTrue(uploads[-1]["isLast"])
        self.assertEqual(uploads[-1]["inputDigest"], canonical_digest([canonical_digest(row["sources"]) for row in uploads]))

    def test_sensitive_nonmember_bot_and_oversized_are_excluded_with_partial_coverage(self):
        sources = [human(), human(NOW, "UX"), {**human(NOW), "bot_id": "B1"},
                   human(NOW, text="api_key=private-value"), human(NOW, text="가" * (MAX_CHUNK_CHARS + 1))]
        memory = bridge(Slack({"": {"messages": sources}}))
        memory.collect_capture(capture())
        uploads = [body for path, body in memory.api.calls if path.endswith("/chunks")]
        self.assertEqual(sum(len(row["sources"]) for row in uploads), 1)
        self.assertEqual(uploads[-1]["coverage"], "partial")

    def test_resume_uses_committed_cursor_and_matching_chunk_manifest(self):
        memory = bridge(Slack({"page2": {"messages": [human(NOW)]}}))
        old_digest = canonical_digest([{"previous": "source"}])
        memory.collect_capture(capture(checkpoint={"cursor": json.dumps({"slackCursor": "page2", "nextChunkNo": 1}), "coverage": "complete"},
                                       receivedChunks=[{"chunkNo": 0, "chunkDigest": old_digest}]))
        uploads = [body for path, body in memory.api.calls if path.endswith("/chunks")]
        self.assertEqual(uploads[0]["chunkNo"], 1)
        self.assertEqual(uploads[-1]["totalChunks"], 3)

    def test_operations_and_context_dependent_followups_are_excluded_across_pages(self):
        # 제외된 운영 대화의 '그거'를 사람 취향으로 오인하지 않고 새 현재 멘션부터 재개한다.
        before = "1792099999.000001"
        slack = Slack({"": {"messages": [human(before, text="<@UBOT> 마미박스 무료병원 자동 업데이트 켜줘")],
                             "has_more": True, "response_metadata": {"next_cursor": "page2"}},
                       "page2": {"messages": [human(ROOT, text="응 앞으로도 그렇게 해줘"), human(NOW, text="답변은 결론부터 듣는 게 좋아")]}})
        memory = bridge(slack)
        memory.collect_capture(capture(threadTs=before))
        uploads = [body for path, body in memory.api.calls if path.endswith("/chunks")]
        self.assertEqual([source["messageTs"] for row in uploads for source in row["sources"]], [NOW])
        self.assertEqual(uploads[-1]["coverage"], "partial")
        saved = [body for path, body in memory.api.calls if path.endswith("/checkpoint") and body.get("cursor")]
        self.assertTrue(json.loads(saved[0]["cursor"])["contextGap"])

    def test_resumed_gap_skips_uncertain_human_and_allows_explicit_preference_command(self):
        memory = bridge(Slack({"page2": {"messages": [human(ROOT, text="그거 해줘"), human(NOW)]}}))
        memory.collect_capture(capture(checkpoint={"cursor": json.dumps({"slackCursor": "page2", "contextGap": True})}))
        uploads = [body for path, body in memory.api.calls if path.endswith("/chunks")]
        self.assertEqual([source["messageTs"] for row in uploads for source in row["sources"]], [NOW])
        self.assertIsNotNone(memory._human_source(human(text="<@UBOT> 내 답변 선호를 짧게로 바꿔줘"), tenant="T1", channel="C1"))
        self.assertIsNone(memory._human_source(human(text="<@UBOT> 이 스레드 학습해줘"), tenant="T1", channel="C1"))

    def test_collection_renews_lease_without_advancing_uncommitted_page(self):
        # permalink 조회가 느려도 확정 전 페이지 위치를 유지하며 lease만 연장한다.
        now = [0.0]
        slack = Slack({"page2": {"messages": [human(ROOT), human(NOW)]}})
        original = slack.chat_getPermalink
        def slow_permalink(**kwargs):
            now[0] += 25
            return original(**kwargs)
        slack.chat_getPermalink = slow_permalink
        memory = bridge(slack)
        memory.clock = lambda: now[0]
        saved_cursor = json.dumps({"slackCursor": "page2", "nextChunkNo": 0})
        memory.collect_capture(capture(checkpoint={"cursor": saved_cursor}))
        renewals = [body for path, body in memory.api.calls if path.endswith("/checkpoint")]
        self.assertGreaterEqual(len(renewals), 2)
        self.assertTrue(all(body["cursor"] == saved_cursor for body in renewals))

    def test_missing_or_oversized_delivered_parts_are_partial(self):
        # 발송 영수증 일부만 있거나 봇 본문이 입력 예산을 넘으면 전체 수집으로 표시하지 않는다.
        memory = bridge(Slack({"": {"messages": [human(NOW), {**human(AFTER, "UBOT", "가" * (MAX_CHUNK_CHARS + 1)), "bot_id": "B1"}]}}))
        memory.collect_capture(capture(answerParts=[{"partIndex": 0}, {"partIndex": 1}],
                                       checkpoint={"deliveryReceipts": [{"partIndex": 0, "messageTs": AFTER}]}))
        uploads = [body for path, body in memory.api.calls if path.endswith("/chunks")]
        self.assertEqual(uploads[-1]["coverage"], "partial")
        self.assertEqual(sum(len(row["sources"]) for row in uploads), 1)

    def test_unfinished_page_is_never_finalized(self):
        memory = bridge(Slack({"": {"messages": [human()], "has_more": True}}))
        memory.collect_capture(capture())
        self.assertFalse(any(body.get("isLast") for _, body in memory.api.calls))

    def test_revoked_trigger_cancels_without_slack_read(self):
        memory = bridge(Slack({}))
        memory.collect_capture(capture(actorId="UX"))
        self.assertEqual(memory.client.calls, [])
        self.assertEqual(memory.api.calls[0][1]["state"], "canceled")

    def test_verification_is_scope_bounded_and_marks_changed_or_absent(self):
        memory = bridge(Slack({"": {"messages": [human(text="이제 자세히 설명해줘")]}}))
        sources = [{"sourceId": "s1", "channelId": "C1", "threadTs": ROOT, "messageTs": ROOT, "revisionHash": source_revision("옛날 내용")},
                   {"sourceId": "s2", "channelId": "C1", "threadTs": ROOT, "messageTs": NOW, "revisionHash": source_revision("삭제된 내용")},
                   {"sourceId": "private", "channelId": "C9", "threadTs": ROOT, "messageTs": NOW, "revisionHash": source_revision("비공개") }]
        result = memory.verify_sources(sources, channel_id="C1", tenant_id="T1")
        self.assertEqual([(row["sourceId"], row["status"]) for row in result], [("s1", "changed"), ("s2", "deleted")])

    def test_delivery_uses_real_message_id_and_never_infers_missing_receipt(self):
        memory, receipts, ids = bridge(), [], []
        def reply(text, **kwargs):
            ids.append(kwargs["client_msg_id"])
            if len(ids) == 1:
                return None
            return {"ts": AFTER, "message": {"user": "UBOT", "text": "<@UA> 응답"}}
        send = memory.delivery_reply(reply, capture(), receipts)
        send("one")
        send("two")
        self.assertNotEqual(ids[0], ids[1])
        self.assertEqual(receipts[0]["partIndex"], 1)
        self.assertEqual(receipts[0]["textHash"], canonical_digest("<@UA> 응답"))
        self.assertEqual(ids[0], delivery_client_id("cap_1", 0))

    def test_source_verifications_never_exceed_turn_wire_budget(self):
        # API가 잘못 많은 후보를 반환해도 일반 turn의 64개 검증 상한을 넘지 않는다.
        messages = [human(str(1792100000 + index) + ".000001") for index in range(65)]
        memory = bridge(Slack({"": {"messages": messages}}))
        sources = [{"sourceId": "s" + str(index), "channelId": "C1", "threadTs": ROOT,
                    "messageTs": message["ts"], "revisionHash": source_revision(message["text"])}
                   for index, message in enumerate(messages)]
        self.assertEqual(len(memory.verify_sources(sources, channel_id="C1", tenant_id="T1")), 64)

    def test_cross_channel_style_requires_origin_membership_and_self_authorship(self):
        # 공개 채널을 봇이 읽을 수 있어도 요청자 소속·자기 발언을 모두 확인해야 한다.
        slack = Slack({"": {"messages": [human(), human(NOW, "UB")]}})
        membership_calls = []
        def members(**kwargs):
            membership_calls.append(kwargs)
            return {"ok": True, "members": ["UA"]}
        slack.conversations_members = members
        memory = bridge(slack)
        sources = [{"sourceId": f"s{i}", "channelId": "C9", "threadTs": ROOT,
                    "messageTs": ts, "revisionHash": source_revision("앞으로 결론부터 말해줘"),
                    "styleOnly": True} for i, ts in enumerate((ROOT, NOW))]
        result = memory.verify_sources(sources, channel_id="C1", tenant_id="T1", actor_id="UA")
        self.assertEqual([row["status"] for row in result], ["verified", "unavailable"])
        self.assertEqual(slack.calls[0]["channel"], "C9")
        self.assertEqual(len(membership_calls), 1)
        slack.calls.clear()
        slack.conversations_members = lambda **_: {"ok": True, "members": ["UB"]}
        self.assertEqual(memory.verify_sources(sources, channel_id="C1", tenant_id="T1", actor_id="UA"), [])
        self.assertEqual(slack.calls, [])

    def test_cross_channel_membership_failure_or_missing_actor_never_reads_origin(self):
        slack = Slack({})
        slack.conversations_members = lambda **_: {"ok": False, "error": "missing_scope"}
        memory = bridge(slack)
        source = {"sourceId": "s1", "channelId": "C9", "threadTs": ROOT,
                  "messageTs": ROOT, "revisionHash": "a" * 64, "styleOnly": True}
        self.assertEqual(memory.verify_sources([source], channel_id="C1", tenant_id="T1", actor_id="UA"), [])
        self.assertEqual(memory.verify_sources([source], channel_id="C1", tenant_id="T1"), [])
        self.assertEqual(slack.calls, [])

    def test_source_delete_does_not_require_deleted_author_membership(self):
        memory = bridge()
        memory.source_event({"subtype": "message_deleted", "channel": "C1", "deleted_ts": ROOT}, {"team_id": "T1", "event_id": "Ev1"}, None)
        self.assertEqual(memory.api.calls[0][1]["eventType"], "deleted")
        self.assertNotIn("actorId", memory.api.calls[0][1])
        self.assertEqual(memory.status(), {"sourceEvents": "ok", "pendingSourceEvents": 0})

    def test_profile_gate_and_management_context(self):
        memory = bridge()
        payload = {"user_id": "UB", "workspace_id": "T1", "channel_id": "C1", "thread_ts": ROOT, "current_ts": NOW}
        self.assertIsNone(memory.context_for(payload))
        self.assertIsNotNone(memory.context_for(payload, manage=True))

    def test_wire_context_and_capture_are_optional_and_isolated(self):
        memory = bridge()
        payload = {"user_id": "UA", "workspace_id": "T1", "channel_id": "C1", "thread_ts": ROOT, "current_ts": NOW}
        original = request()
        contextual = replace(original, memory_context=memory.context_for(payload))
        self.assertNotIn("memoryContext", _serialize_request(original, route_group="freeform"))
        self.assertIn("memoryContext", _serialize_request(contextual, route_group="freeform"))
        self.assertNotIn("memoryContext", _serialize_request(contextual, route_group="structured"))
        response = {"requestId": "request1", "route": "company_freeform", "outcome": "answered", "messages": [{"body": "기억했어", "deliveryScope": "conversation", "mentionActor": True, "format": "commonmark"}], "sources": [], "usedLlm": True, "fallbackReason": None}
        self.assertIsNone(_deserialize_result_payload(response, "request1").memory_capture)
        response["memoryCapture"] = capture()
        self.assertEqual(_deserialize_result_payload(response, "request1").memory_capture["captureId"], "cap_1")
        response["memoryCapture"] = {"requestId": "wrong"}
        self.assertIsNone(_deserialize_result_payload(response, "request1").memory_capture)


if __name__ == "__main__":
    unittest.main()
