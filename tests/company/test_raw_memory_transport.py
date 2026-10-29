"""원문 없는 영수증의 장애 복구와 native 원문 전송 경계를 mock으로 검증한다."""

from __future__ import annotations

import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pytest

from boxer_adapter_slack import common
from boxer_company.memory.contracts import canonical_digest, source_revision
from boxer_company_adapter_slack.conversation_memory import SlackConversationMemory
from boxer_company_adapter_slack.memory_event_outbox import (
    MemoryEventOutboxError,
    MemorySourceEventOutbox,
)

TENANT, CHANNEL, AUTHOR = "T19PA3EPL", "C0621TL2HSB", "UOFFLINEAA"
STAMP, EDITED, LATER = "1792100000.000001", "1792100001.000001", "1792100002.000001"
TEXT = "  오늘은 첫 번째 안으로 정했어\n"
SILENT = logging.getLogger(__name__ + ".silent")
SILENT.disabled = True


def message(text=TEXT, *, edited=""):
    return {"ts": STAMP, "user": AUTHOR, "text": text,
            **({"edited": {"ts": edited}} if edited else {})}


def event(text=TEXT, *, edited=""):
    if edited:
        return {"subtype": "message_changed", "channel": CHANNEL, "event_ts": edited,
                "message": message(text, edited=edited)}
    return {**message(text), "channel": CHANNEL, "event_ts": LATER}


class Slack:
    def __init__(self):
        self.current = message()
        self.private = True
        self.allowed = True
        self.calls = []

    def conversations_info(self, **kwargs):
        self.calls.append(("info", kwargs))
        return {"ok": True, "channel": {"id": kwargs["channel"], "is_private": self.private,
                "is_member": True, "context_team_id": TENANT}}

    def conversations_replies(self, **kwargs):
        self.calls.append(("replies", kwargs))
        if not self.allowed:
            return {"ok": False, "error": "not_in_channel"}
        return {"ok": True, "messages": [self.current] if self.current is not None else [], "has_more": False}


class Api:
    def __init__(self):
        self.calls, self.bindings = [], {}
        self.offline, self.wrong_ack = False, False
        self.latest = "0"

    def call(self, path, payload):
        self.calls.append((path, dict(payload)))
        if path.endswith("/sources/events"):
            return {"eventId": payload["eventId"], "invalidatedCount": 0}
        if self.offline:
            raise TimeoutError("external failure body must stay hidden")
        digest = canonical_digest(payload)
        previous = self.bindings.get(payload["eventId"])
        if previous is not None:
            if previous != digest:
                raise AssertionError("event payload changed under same identity")
            status = "duplicate"
        elif payload["eventTs"] < self.latest:
            status = "stale"
        else:
            self.bindings[payload["eventId"]] = digest
            self.latest = payload["eventTs"]
            status = "stored" if payload["eventType"] in {"created", "changed"} else payload["eventType"]
        return {"eventId": "wrong" if self.wrong_ack else payload["eventId"],
                "messageTs": payload["messageTs"], "status": status, "stored": status == "stored"}


class RawMemoryTransportTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name).resolve() / "events.json"
        self.slack, self.api = Slack(), Api()
        self.allowed_authors = {AUTHOR}
        self.memory = self.new_memory()

    def new_memory(self):
        memory = SlackConversationMemory(self.api, is_allowed=lambda tenant, actor: tenant == TENANT and actor in self.allowed_authors,
            logger=SILENT, event_outbox=MemorySourceEventOutbox(self.path))
        memory._profile_ids = {AUTHOR}
        memory.client, memory.tenant_id = self.slack, TENANT
        return memory

    def stage(self, value=None, *, identity="Ev1"):
        return self.memory.raw_event(value or event(), {"team_id": TENANT, "event_id": identity}, self.slack)

    def test_created_native_and_mention_have_same_identity_payload_and_preserve_spaces(self):
        self.assertTrue(self.stage())
        self.assertTrue(self.stage({**event(), "type": "app_mention", "event_ts": EDITED}, identity="Ev2"))
        first, second = [body for _, body in self.api.calls]
        self.assertEqual(first, second)
        self.assertEqual(first["eventTs"], STAMP)
        self.assertEqual(first["source"]["text"], TEXT)
        self.assertEqual(first["source"]["threadTs"], STAMP)
        self.assertEqual(self.memory.event_outbox.pending_count(), 0)

    def test_changed_and_unavailable_preserve_native_event_time_separate_from_edited_at(self):
        value = {**event("수정된 문장", edited=EDITED), "event_ts": LATER}
        self.assertTrue(self.stage(value))
        body = self.api.calls[-1][1]
        self.assertEqual(body["eventTs"], LATER)
        self.assertEqual(body["source"]["editedAt"], EDITED)
        unavailable = {**event("api_key=synthetic-private", edited=EDITED), "event_ts": LATER}
        self.assertTrue(self.stage(unavailable))
        self.assertEqual(self.api.calls[-1][1]["eventTs"], LATER)
        self.assertEqual(self.api.calls[-1][1]["eventType"], "unavailable")

    def test_raw_text_limit_keeps_40000_chars_and_skips_larger_created_message(self):
        self.assertTrue(self.stage(event("가" * 40_000)))
        self.assertEqual(len(self.api.calls[-1][1]["source"]["text"]), 40_000)
        count = len(self.api.calls)
        self.assertFalse(self.stage(event("가" * 40_001)))
        self.assertEqual(len(self.api.calls), count)

    def test_failure_persists_only_metadata_then_same_revision_replays_exact_payload(self):
        self.api.offline = True
        self.assertTrue(self.stage())
        before = self.api.calls[-1][1]
        contents = self.path.read_text()
        self.assertNotIn(TEXT.strip(), contents)
        self.assertNotIn("permalink", contents)
        receipt = self.memory.event_outbox.pending()[0]
        self.assertEqual(receipt["rawEventType"], "created")
        self.assertEqual(receipt["eventType"], "changed")
        self.assertEqual(receipt["revisionHash"], source_revision(TEXT))
        self.api.offline = False
        replacement = self.new_memory()
        self.assertEqual(replacement.drain_source_events(), 1)
        self.assertEqual(self.api.calls[-1][1], before)
        self.assertEqual(replacement.event_outbox.pending_count(), 0)
        exact = [kwargs for kind, kwargs in self.slack.calls if kind == "replies"][-1]
        self.assertEqual(exact["oldest"], STAMP)
        self.assertEqual(exact["latest"], STAMP)

    def test_new_revision_uses_refresh_identity_and_acknowledges_original_receipt(self):
        self.api.offline = True
        self.assertTrue(self.stage(event("첫 번째 수정", edited=EDITED)))
        receipt = self.memory.event_outbox.pending()[0]
        self.slack.current = message("두 번째 수정", edited=LATER)
        self.api.offline = False
        self.assertEqual(self.new_memory().drain_source_events(), 1)
        fresh = self.api.calls[-1][1]
        self.assertEqual(fresh["eventId"], "memory.raw_event:refresh_" + canonical_digest([
            receipt["eventId"], source_revision("두 번째 수정", LATER)]))
        self.assertEqual(fresh["source"]["text"], "두 번째 수정")
        self.assertEqual(fresh["eventTs"], LATER)
        self.assertEqual(MemorySourceEventOutbox(self.path).pending_count(), 0)

    def test_permission_failure_during_refresh_keeps_receipt_without_invalidation(self):
        self.api.offline = True
        self.stage()
        count = len(self.api.calls)
        self.api.offline, self.slack.allowed = False, False
        self.assertEqual(self.new_memory().drain_source_events(), 0)
        self.assertEqual(len(self.api.calls), count)
        self.assertEqual(MemorySourceEventOutbox(self.path).pending_count(), 1)

    def test_same_text_revision_but_changed_attachment_state_gets_new_binding(self):
        self.api.offline = True
        self.stage()
        original = self.api.calls[-1][1]
        # 응답만 유실된 뒤 첨부 상태가 달라져도 같은 event ID에 다른 payload를 보내지 않는다.
        self.api.bindings[original["eventId"]] = canonical_digest(original)
        self.slack.current = {**message(), "files": [{"id": "F1"}]}
        self.api.offline = False
        self.assertEqual(self.new_memory().drain_source_events(), 1)
        refreshed = self.api.calls[-1][1]
        self.assertNotEqual(refreshed["eventId"], original["eventId"])
        self.assertEqual(refreshed["eventType"], "unavailable")
        self.assertNotIn("source", refreshed)

    def test_unsafe_and_attachment_edits_invalidate_without_generic_redaction(self):
        for value in (event("api_key=synthetic-private", edited=EDITED),
                      {**event("첨부로 바뀐 발언", edited=LATER),
                       "message": {**message("첨부로 바뀐 발언", edited=LATER), "files": [{"id": "F1"}]}}):
            with self.subTest(kind=value["message"].get("files")):
                self.assertTrue(self.memory.source_event(value, {"team_id": TENANT}, self.slack))
                path, payload = self.api.calls[-1]
                self.assertTrue(path.endswith("/sources/raw"))
                self.assertEqual(payload["eventType"], "unavailable")
                self.assertNotIn("source", payload)
        self.assertTrue(all(path.endswith("/sources/raw") for path, _ in self.api.calls))

    def test_unsafe_new_messages_and_nonmembers_bots_public_or_other_scope_are_excluded(self):
        cases = [event("api_key=synthetic-private"), {**event(), "bot_id": "B1"},
            {**event(), "bot_profile": {"id": "B1"}}, {**event(), "app_id": "A1"},
            {**event(), "user": "UOTHER"}, {**event(), "files": [{"id": "F1"}]},
            {**event(), "channel": "DPRIVATE"}, {**event(), "ts": "1790780399.999999"}]
        for value in cases:
            with self.subTest(kind=value.get("bot_id") or value.get("channel")):
                self.assertFalse(self.stage(value))
        self.slack.private = False
        self.assertFalse(self.stage())
        self.assertEqual(self.memory.event_outbox.pending_count(), 0)
        self.assertEqual(self.api.calls, [])

    def test_explicit_delete_without_actor_never_reads_or_sends_original(self):
        value = {"channel": CHANNEL, "subtype": "message_deleted", "deleted_ts": STAMP, "event_ts": LATER}
        self.assertTrue(self.stage(value))
        body = self.api.calls[-1][1]
        self.assertEqual(body["eventType"], "deleted")
        self.assertNotIn("actorId", body)
        self.assertNotIn("source", body)
        self.assertFalse(any(kind == "replies" for kind, _ in self.slack.calls))

    def test_author_revoked_delete_and_unsafe_edit_invalidate_without_actor_or_exact_body_read(self):
        self.allowed_authors.clear()
        changed = event("수정된 새 원문", edited=EDITED)
        self.assertTrue(self.memory.source_event(changed, {"team_id": TENANT}, self.slack))
        self.assertTrue(self.api.calls[-1][0].endswith("/sources/raw"))
        self.assertEqual(self.api.calls[-1][1]["eventType"], "unavailable")
        self.assertNotIn("actorId", self.api.calls[-1][1])
        self.assertNotIn("source", self.api.calls[-1][1])
        deleted = {"channel": CHANNEL, "subtype": "message_deleted", "deleted_ts": STAMP,
                   "event_ts": LATER, "previous_message": message()}
        self.assertTrue(self.stage(deleted))
        self.assertEqual(self.api.calls[-1][1]["eventType"], "deleted")
        self.assertNotIn("actorId", self.api.calls[-1][1])
        self.assertFalse(any(kind == "replies" for kind, _ in self.slack.calls))

    def test_queued_body_and_legacy_invalidation_receipts_survive_author_revocation(self):
        self.api.offline = True
        self.stage(event("대기 중이던 수정 원문", edited=EDITED))
        receipt = self.memory.event_outbox.pending()[0]
        self.allowed_authors.clear()
        self.api.offline = False
        self.assertEqual(self.memory.drain_source_events(), 1)
        invalidated = self.api.calls[-1][1]
        self.assertNotEqual(invalidated["eventId"], receipt["eventId"])
        self.assertEqual(invalidated["eventType"], "unavailable")
        self.assertNotIn("source", invalidated)
        self.assertNotIn("actorId", invalidated)
        self.assertFalse(any(kind == "replies" for kind, _ in self.slack.calls))
        for raw_type in ("unavailable", "deleted"):
            legacy = {**receipt, "eventId": "memory.raw_event:" + raw_type,
                      "eventType": raw_type, "rawEventType": raw_type, "authorId": AUTHOR}
            self.memory.event_outbox.enqueue(legacy)
            self.assertEqual(self.memory.drain_source_events(), 1)
            self.assertNotIn("actorId", self.api.calls[-1][1])
            self.assertNotIn("source", self.api.calls[-1][1])

    def test_registered_company_observer_and_change_handler_send_each_edit_once(self):
        class App:
            def __init__(self, **_kwargs):
                self.handlers = {}
            def event(self, name):
                def register(handler):
                    self.handlers[name] = handler
                    return handler
                return register
        with patch.object(common.ss, "validate_slack_tokens"), patch.object(common, "_validate_tokens"), \
                patch.object(common.s, "REQUEST_LOG_SQLITE_ENABLED", False), \
                patch.object(common.s, "REQUEST_LOG_SQLITE_INIT_ON_STARTUP", False), patch.object(common, "App", App):
            app = common.create_slack_app(lambda *_: None, message_observer=self.memory.observe_message,
                                         message_change_handler=self.memory.source_event)
            app.handlers["message"](event("수정된 문장", edited=EDITED), lambda **_: None,
                                    self.slack, {"team_id": TENANT})
            self.assertEqual(len(self.api.calls), 1)
            app.handlers["message"]({"channel": CHANNEL, "subtype": "message_deleted", "deleted_ts": STAMP,
                "event_ts": LATER}, lambda **_: None, self.slack, {"team_id": TENANT})
            self.assertEqual(len(self.api.calls), 2)

    def test_wrong_ack_preserves_receipt_and_old_format_uses_existing_endpoint(self):
        self.api.wrong_ack = True
        self.stage()
        self.assertEqual(self.memory.event_outbox.pending_count(), 1)
        self.api.wrong_ack = False
        self.assertEqual(self.memory.drain_source_events(), 1)
        self.memory.event_outbox.enqueue({"tenantId": "T1", "channelId": "C1", "messageTs": STAMP,
            "eventId": "EvLegacy", "eventType": "deleted"})
        self.assertEqual(self.memory.drain_source_events(), 1)
        self.assertTrue(self.api.calls[-1][0].endswith("/sources/events"))

    def test_out_of_order_edits_keep_source_event_time_and_accept_stale_ack(self):
        self.assertTrue(self.stage(event("새 수정", edited=LATER)))
        self.assertTrue(self.stage(event("예전 수정", edited=EDITED)))
        self.assertEqual([body["eventTs"] for _, body in self.api.calls], [LATER, EDITED])
        self.assertEqual(self.memory.event_outbox.pending_count(), 0)

    def test_raw_receipt_rejects_body_extra_fields_and_missing_bindings(self):
        self.api.offline = True
        self.stage()
        receipt = self.memory.event_outbox.pending()[0]
        bad = [{**receipt, "text": "본문 저장 금지"}, {**receipt, "source": {}},
               {key: value for key, value in receipt.items() if key != "threadTs"},
               {key: value for key, value in receipt.items() if key != "authorId"},
               {key: value for key, value in receipt.items() if key != "revisionHash"},
               {**receipt, "eventType": "created"}, {**receipt, "eventId": "generic-event"}]
        for value in bad:
            with self.subTest(keys=list(value)), self.assertRaises(MemoryEventOutboxError):
                self.memory.event_outbox.enqueue(value)
        self.assertEqual(json.loads(self.path.read_text())["version"], 1)


@pytest.mark.parametrize("case", ["deleted", "changed", "legacy_deleted", "legacy_unavailable",
                                  "queued_created", "queued_changed"])
def test_revoked_author_invalidation_removes_actual_sqlite_raw_body(tmp_path, monkeypatch, case):
    """작성자 권한이 사라진 뒤에도 이미 저장된 실제 원문을 삭제/무효화할 수 있어야 한다."""
    from boxer_company.memory import raw_live
    from boxer_company.memory.store import MemoryStore, initialize_memory_store

    monkeypatch.setattr(raw_live, "TEAM_MEMBER_PROFILES", ({"user_id": AUTHOR},))
    store = MemoryStore(initialize_memory_store(tmp_path / "database" / "memory.db"))

    class LiveApi:
        def __init__(self):
            self.fail_after_commit = False
            self.calls = []

        def call(self, path, body):
            self.calls.append((path, dict(body)))
            assert path.endswith("/sources/raw")
            result = raw_live.ingest_raw_event(store, tenant_id=body["tenantId"], channel_id=body["channelId"],
                event_id=body["eventId"], event_ts=body["eventTs"], event_type=body["eventType"],
                source=body.get("source"), message_ts=body["messageTs"], actor_id=body.get("actorId"))
            if self.fail_after_commit:
                raise TimeoutError("response lost after local transaction")
            return result

    api, slack, allowed = LiveApi(), Slack(), {AUTHOR}
    outbox_directory = tmp_path / "outbox"
    outbox_directory.mkdir(mode=0o700)
    outbox = MemorySourceEventOutbox(outbox_directory / "events.json")
    memory = SlackConversationMemory(api, is_allowed=lambda tenant, actor: tenant == TENANT and actor in allowed,
                                     event_outbox=outbox, logger=SILENT)
    memory._profile_ids = {AUTHOR}
    memory.client, memory.tenant_id = slack, TENANT
    assert memory.raw_event(event(), {"team_id": TENANT}, slack)
    with store._connection() as connection:
        assert connection.execute("SELECT text FROM memory_source_texts").fetchone()[0] == TEXT
    if case.startswith("queued_"):
        api.fail_after_commit = True
        value = event("응답만 유실된 수정 원문", edited=EDITED) if case == "queued_changed" else event()
        assert memory.raw_event(value, {"team_id": TENANT}, slack)
        assert outbox.pending_count() == 1
        api.fail_after_commit = False
    elif case.startswith("legacy_"):
        if case == "legacy_deleted":
            value = {"subtype": "message_deleted", "channel": CHANNEL, "deleted_ts": STAMP,
                     "event_ts": LATER, "previous_message": message()}
        else:
            value = event("api_key=synthetic-private", edited=EDITED)
        receipt, _ = memory._raw_transport.stage(value, {"team_id": TENANT}, slack)
        outbox.enqueue({**receipt, "authorId": AUTHOR})
    allowed.clear()
    # 재조회 권한이 없어도 명시 무효화에는 원문을 다시 읽을 필요가 없다.
    slack.allowed = False
    if case == "deleted":
        assert memory.source_event({"subtype": "message_deleted", "channel": CHANNEL,
            "deleted_ts": STAMP, "event_ts": LATER, "previous_message": message()}, {"team_id": TENANT}, slack)
    elif case == "changed":
        assert memory.source_event(event("이제 허용되지 않는 수정 원문", edited=EDITED), {"team_id": TENANT}, slack)
    else:
        assert memory.drain_source_events() == 1
    assert outbox.pending_count() == 0
    assert "actorId" not in api.calls[-1][1] and "source" not in api.calls[-1][1]
    assert not any(kind == "replies" for kind, _ in slack.calls)
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_source_texts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0] == 0


if __name__ == "__main__":
    unittest.main()
