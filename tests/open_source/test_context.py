import logging
import unittest

from boxer.context.builder import _build_model_input
from boxer.context.windowing import (
    _limit_context_entries,
    _render_context_text,
    window_context_entries,
)
import boxer_adapter_slack
from boxer_adapter_slack.context import (
    _load_slack_thread_context,
    _normalize_slack_context_entries,
    load_slack_thread_context_entries,
)


class ContextBuilderTests(unittest.TestCase):
    def test_build_model_input_returns_question_when_context_is_empty(self) -> None:
        self.assertEqual(_build_model_input("ping", ""), "ping")

    def test_build_model_input_includes_context_block(self) -> None:
        result = _build_model_input("ping", "U1: hello")

        self.assertIn("Thread context", result)
        self.assertIn("U1: hello", result)
        self.assertIn("Current user question", result)
        self.assertIn("ping", result)


class ContextWindowingTests(unittest.TestCase):
    def test_limit_context_entries_keeps_latest_entries(self) -> None:
        entries = [
            {"author_id": "U1", "text": "one"},
            {"author_id": "U2", "text": "two"},
            {"author_id": "U3", "text": "three"},
        ]

        self.assertEqual(
            _limit_context_entries(entries, 2),
            [
                {"author_id": "U2", "text": "two"},
                {"author_id": "U3", "text": "three"},
            ],
        )

    def test_render_context_text_formats_author_and_text(self) -> None:
        rendered = _render_context_text(
            [
                {"kind": "message", "source": "slack", "author_id": "U1", "text": "hello"},
                {"kind": "message", "source": "slack", "author_id": "U2", "text": "world"},
            ],
            max_chars=100,
        )

        self.assertEqual(rendered, "U1: hello\nU2: world")

    def test_render_context_text_falls_back_to_source_and_kind_without_author(self) -> None:
        rendered = _render_context_text(
            [
                {
                    "kind": "profile",
                    "source": "team_profile",
                    "text": "차분하고 짧게 답함",
                }
            ],
            max_chars=100,
        )

        self.assertEqual(rendered, "team_profile/profile: 차분하고 짧게 답함")

    def test_char_window_drops_stale_entries_when_latest_is_too_large(
        self,
    ) -> None:
        entries = [
            {"author_id": "U1", "text": "세션 1로 봐줘"},
            {"author_id": "U1", "text": "x" * 100},
        ]

        self.assertEqual(
            window_context_entries(entries, max_chars=50),
            [],
        )
        self.assertEqual(
            _render_context_text(entries, max_chars=50),
            "",
        )


class SlackContextTests(unittest.TestCase):
    def test_package_exports_channel_neutral_thread_entries_loader(self) -> None:
        self.assertIs(
            boxer_adapter_slack.load_slack_thread_context_entries,
            load_slack_thread_context_entries,
        )

    def test_normalize_slack_context_entries_filters_current_and_empty_text(self) -> None:
        messages = [
            {"user": "U1", "text": "older", "ts": "1.0"},
            {"user": "U2", "text": "", "ts": "2.0"},
            {"user": "U3", "text": "current", "ts": "3.0"},
        ]

        result = _normalize_slack_context_entries(messages, current_ts="3.0")

        self.assertEqual(
            result,
            [
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "U1",
                    "text": "older",
                    "created_at": "1.0",
                }
            ],
        )

    def test_load_slack_thread_context_formats_replies(self) -> None:
        class FakeClient:
            def conversations_replies(self, *, channel: str, ts: str, limit: int, inclusive: bool) -> dict:
                self.called = {
                    "channel": channel,
                    "ts": ts,
                    "limit": limit,
                    "inclusive": inclusive,
                }
                return {
                    "messages": [
                        {"user": "U1", "text": "hello", "ts": "1.0"},
                        {"user": "U2", "text": "world", "ts": "2.0"},
                    ]
                }

        client = FakeClient()

        result = _load_slack_thread_context(
            client,
            logging.getLogger("test"),
            "C123",
            "1.0",
            "3.0",
        )

        self.assertEqual(result, "U1: hello\nU2: world")
        self.assertEqual(client.called["channel"], "C123")
        self.assertEqual(client.called["ts"], "1.0")

    def test_load_slack_thread_context_entries_keeps_normalized_order(self) -> None:
        class FakeClient:
            def conversations_replies(self, **kwargs) -> dict:
                return {
                    "messages": [
                        {"user": "U1", "text": "older", "ts": "1.0"},
                        {"user": "U2", "text": "current", "ts": "2.0"},
                    ]
                }

        result = load_slack_thread_context_entries(
            FakeClient(),
            logging.getLogger("test"),
            "C123",
            "1.0",
            "2.0",
        )

        self.assertEqual(
            result,
            [
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "U1",
                    "text": "older",
                    "created_at": "1.0",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
