"""원문 없는 Slack 삭제 receipt의 장애·재시작·파일 경계를 검증한다."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
import logging
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from boxer_company_adapter_slack.conversation_memory import (
    SlackConversationMemory,
    build_slack_conversation_memory,
)
from boxer_company_adapter_slack.memory_event_outbox import MemoryEventOutboxError, MemorySourceEventOutbox


TIMESTAMP = "1792100000.000001"
SILENT = logging.getLogger(__name__ + ".silent")
SILENT.disabled = True


def event(identity="Ev1"):
    return {"tenantId": "T1", "channelId": "C1", "messageTs": TIMESTAMP,
            "eventId": identity, "eventType": "deleted"}


class Api:
    def __init__(self, *, failing=False, wrong_ack=False):
        self.calls = []
        self.failing, self.wrong_ack = failing, wrong_ack

    def call(self, path, body):
        self.calls.append((path, body))
        if self.failing:
            raise TimeoutError("offline")
        if path.endswith("/sources/events"):
            return {"eventId": "wrong" if self.wrong_ack else body["eventId"], "invalidatedCount": 0}
        return {"captures": []}


class Slack:
    def auth_test(self):
        return {"user_id": "UBOT"}


class MemoryEventOutboxTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name).resolve() / "source-events.json"

    def memory(self, api, outbox=None):
        result = SlackConversationMemory(api, is_allowed=lambda *_: False, logger=SILENT,
            event_outbox=outbox or MemorySourceEventOutbox(self.path))
        result.tenant_id, result.client = "T1", Slack()
        return result

    def test_api_failure_persists_before_send_then_restart_poll_acknowledges(self):
        # 삭제는 작성자나 membership 없이 durable 접수하고 새 worker가 같은 ID로 재전송한다.
        api = Api(failing=True)
        memory = self.memory(api)
        with patch.object(api, "call", side_effect=lambda *args: self._assert_persisted_then_fail()):
            self.assertTrue(memory.source_event({"subtype": "message_deleted", "channel": "C1", "deleted_ts": TIMESTAMP},
                                               {"team_id": "T1", "event_id": "Ev1"}, None))
        self.assertEqual(memory.status(), {"sourceEvents": "degraded", "pendingSourceEvents": 1})
        replacement = self.memory(Api())
        replacement.poll_once()
        self.assertEqual(replacement.status(), {"sourceEvents": "ok", "pendingSourceEvents": 0})
        self.assertEqual(replacement.api.calls[0][1], event())
        self.assertEqual(os.stat(self.path).st_mode & 0o777, 0o600)
        self.assertEqual(os.stat(str(self.path) + ".lock").st_mode & 0o777, 0o600)

    def _assert_persisted_then_fail(self):
        self.assertEqual(MemorySourceEventOutbox(self.path).pending(), [event()])
        raise TimeoutError("offline")

    def test_wrong_ack_and_ack_write_failure_keep_exact_event_for_retry(self):
        outbox = MemorySourceEventOutbox(self.path)
        outbox.enqueue(event())
        memory = self.memory(Api(wrong_ack=True), outbox)
        self.assertEqual(memory.drain_source_events(), 0)
        self.assertEqual(outbox.pending_count(), 1)
        memory.api.wrong_ack = False
        with patch.object(outbox, "acknowledge", side_effect=OSError("disk unavailable")):
            self.assertEqual(memory.drain_source_events(), 0)
        self.assertEqual(MemorySourceEventOutbox(self.path).pending(), [event()])
        self.assertEqual(memory.drain_source_events(), 1)

    def test_capacity_and_identity_conflict_preserve_existing_queue_without_false_success(self):
        outbox = MemorySourceEventOutbox(self.path, max_events=1)
        outbox.enqueue(event())
        outbox.enqueue(event())
        with self.assertRaises(MemoryEventOutboxError):
            outbox.enqueue({**event(), "channelId": "C2"})
        memory = self.memory(Api(), outbox)
        self.assertFalse(memory.source_event({"subtype": "message_deleted", "channel": "C1", "deleted_ts": TIMESTAMP},
                                            {"team_id": "T1", "event_id": "Ev2"}, None))
        self.assertEqual(memory.api.calls, [])
        self.assertEqual(outbox.pending(), [event()])
        self.assertEqual(memory.status()["sourceEvents"], "degraded")

    def test_changed_event_stores_revision_but_never_message_text(self):
        memory = self.memory(Api(failing=True))
        self.assertTrue(memory.source_event({"subtype": "message_changed", "channel": "C1",
                                            "message": {"ts": TIMESTAMP, "text": "private original body"}},
                                           {"team_id": "T1", "event_id": "Ev1"}, None))
        raw = self.path.read_text()
        self.assertNotIn("private original body", raw)
        pending = MemorySourceEventOutbox(self.path).pending()[0]
        self.assertEqual(set(pending), set(event()) | {"revisionHash"})
        with self.assertRaises(MemoryEventOutboxError):
            memory.event_outbox.enqueue({**event("Ev2"), "text": "unexpected body"})

    def test_atomic_write_failure_leaves_prior_events_and_reports_failure(self):
        outbox = MemorySourceEventOutbox(self.path)
        outbox.enqueue(event())
        before = self.path.read_bytes()
        memory = self.memory(Api(), outbox)
        with patch("boxer_company_adapter_slack.memory_event_outbox.os.replace", side_effect=OSError("disk unavailable")):
            self.assertFalse(memory.source_event({"subtype": "message_deleted", "channel": "C1", "deleted_ts": TIMESTAMP},
                                                {"team_id": "T1", "event_id": "Ev2"}, None))
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual(memory.api.calls, [])
        self.assertEqual(list(self.path.parent.glob("*.tmp")), [])
        # durable 접수가 실패해도 원본 재확인 경계를 우회해 검증 완료로 만들지 않는다.
        memory.client.conversations_replies = lambda **_: {"messages": [], "has_more": False}
        checks = memory.verify_sources([{"sourceId": "src1", "channelId": "C1", "threadTs": TIMESTAMP,
                                         "messageTs": TIMESTAMP, "revisionHash": "0" * 64}],
                                       channel_id="C1", tenant_id="T1")
        self.assertEqual(checks[0]["status"], "deleted")

    def test_separate_instances_do_not_lose_concurrent_events(self):
        # event handler와 worker가 별도 instance여도 read-modify-write 사이 이벤트를 잃지 않는다.
        outboxes = [MemorySourceEventOutbox(self.path), MemorySourceEventOutbox(self.path)]
        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(lambda index: outboxes[index % 2].enqueue(event("Ev" + str(index))), range(20)))
        self.assertEqual(MemorySourceEventOutbox(self.path).pending_count(), 20)

    def test_unsafe_files_and_corrupt_documents_are_not_overwritten(self):
        outbox = MemorySourceEventOutbox(self.path)
        outbox.enqueue(event())
        os.chmod(self.path, 0o644)
        with self.assertRaises(MemoryEventOutboxError):
            MemorySourceEventOutbox(self.path)
        os.chmod(self.path, 0o600)
        self.path.write_text('{"version":1,"events":{},"events":{}}')
        with self.assertRaises(MemoryEventOutboxError):
            MemorySourceEventOutbox(self.path)
        self.path.unlink()
        self.path.symlink_to(self.path.parent / "target.json")
        with self.assertRaises(MemoryEventOutboxError):
            MemorySourceEventOutbox(self.path)

    def test_builder_disables_only_memory_when_path_missing_or_unsafe(self):
        environment = {"BOXER_COMPANY_SLACK_MEMORY_ENABLED": "true"}
        self.assertIsNone(build_slack_conversation_memory(Api(), is_allowed=lambda *_: True, logger=SILENT, environ=environment))
        environment["BOXER_COMPANY_SLACK_MEMORY_EVENT_STATE_PATH"] = str(self.path)
        os.chmod(self.path.parent, 0o755)
        self.assertIsNone(build_slack_conversation_memory(Api(), is_allowed=lambda *_: True, logger=SILENT, environ=environment))
        os.chmod(self.path.parent, 0o700)
        memory = build_slack_conversation_memory(Api(), is_allowed=lambda *_: True, logger=SILENT, environ=environment)
        self.assertIsNotNone(memory)
        self.assertEqual(json.loads(self.path.read_text()), {"version": 1, "events": {}})


if __name__ == "__main__":
    unittest.main()
