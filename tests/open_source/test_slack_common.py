import logging
import unittest
from typing import Any
from unittest.mock import patch

from boxer_adapter_slack import common


class _FakeApp:
    def __init__(self, *args, **kwargs) -> None:
        self.args = args
        self.kwargs = kwargs
        self.handlers: dict[str, Any] = {}

    def event(self, event_name: str):
        def decorator(func):
            self.handlers[event_name] = func
            return func

        return decorator


class SlackCommonTests(unittest.TestCase):
    def test_app_mention_reply_supports_blocks(self) -> None:
        say_calls: list[dict[str, Any]] = []

        def mention_handler(payload, reply, client, logger: logging.Logger) -> None:
            reply(
                "주간 Recordings 요약",
                mention_user=False,
                blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
            )

        def fake_say(**kwargs) -> None:
            say_calls.append(kwargs)

        with (
            patch.object(common.ss, "validate_slack_tokens"),
            patch.object(common, "_validate_tokens"),
            patch.object(common.s, "REQUEST_LOG_SQLITE_ENABLED", False),
            patch.object(common.s, "REQUEST_LOG_SQLITE_INIT_ON_STARTUP", False),
            patch.object(common, "App", _FakeApp),
        ):
            app = common.create_slack_app(mention_handler)
            event_handler = app.handlers["app_mention"]
            event_handler(
                {
                    "text": "<@U_BOT> 지난주 초음파 영상 현황",
                    "user": "U_TEST",
                    "channel": "C_TEST",
                    "ts": "123.456",
                    "team": "T_TEST",
                },
                fake_say,
                object(),
            )

        self.assertEqual(len(say_calls), 1)
        self.assertEqual(say_calls[0]["text"], "주간 Recordings 요약")
        self.assertEqual(
            say_calls[0]["blocks"],
            [{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
        )
        self.assertEqual(say_calls[0]["thread_ts"], "123.456")

    def test_app_mention_reply_stays_in_existing_thread(self) -> None:
        say_calls: list[dict[str, Any]] = []

        def mention_handler(payload, reply, client, logger: logging.Logger) -> None:
            reply("검토 결과", mention_user=False)

        with (
            patch.object(common.ss, "validate_slack_tokens"),
            patch.object(common, "_validate_tokens"),
            patch.object(common.s, "REQUEST_LOG_SQLITE_ENABLED", False),
            patch.object(common.s, "REQUEST_LOG_SQLITE_INIT_ON_STARTUP", False),
            patch.object(common, "App", _FakeApp),
        ):
            app = common.create_slack_app(mention_handler)
            app.handlers["app_mention"](
                {
                    "text": "<@U_BOT> HPA 변경 요청 검토",
                    "user": "U_TEST",
                    "channel": "C_TEST",
                    "ts": "222.222",
                    "thread_ts": "111.111",
                    "team": "T_TEST",
                },
                lambda **kwargs: say_calls.append(kwargs),
                object(),
            )

        self.assertEqual(say_calls[0]["thread_ts"], "111.111")

    def test_message_event_preserves_bot_user_id(self) -> None:
        payloads: list[dict[str, Any]] = []

        def mention_handler(payload, reply, client, logger: logging.Logger) -> None:
            raise AssertionError("mention handler should not run")

        def message_handler(payload, reply, client, logger: logging.Logger) -> None:
            payloads.append(payload)

        with (
            patch.object(common.ss, "validate_slack_tokens"),
            patch.object(common, "_validate_tokens"),
            patch.object(common.s, "REQUEST_LOG_SQLITE_ENABLED", False),
            patch.object(common.s, "REQUEST_LOG_SQLITE_INIT_ON_STARTUP", False),
            patch.object(common, "App", _FakeApp),
        ):
            app = common.create_slack_app(mention_handler, message_handler)
            event_handler = app.handlers["message"]
            event_handler(
                {
                    "text": "bot reply",
                    "subtype": "bot_message",
                    "bot_id": "B_TARGET",
                    "bot_profile": {
                        "user_id": "UTARGET",
                        "name": "buddy",
                        "app_id": "A_TARGET",
                    },
                    "channel": "C_TEST",
                    "ts": "123.456",
                    "team": "T_TEST",
                },
                lambda **kwargs: None,
                object(),
            )

        self.assertEqual(len(payloads), 1)
        self.assertEqual(payloads[0]["bot_user_id"], "UTARGET")
        self.assertEqual(payloads[0]["bot_id"], "B_TARGET")


if __name__ == "__main__":
    unittest.main()
