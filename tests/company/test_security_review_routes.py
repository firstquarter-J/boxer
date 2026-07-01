import logging
import unittest
from typing import Any

from boxer_company_adapter_slack.security_review_routes import (
    _SECURITY_REVIEW_PROBES,
    SecurityReviewMessageContext,
    SecurityReviewRoutesContext,
    _SECURITY_REVIEW_SESSIONS,
    _handle_security_review_bot_message,
    _handle_security_review_request,
)


class _FakeSlackClient:
    def __init__(self) -> None:
        self.posts: list[dict[str, Any]] = []

    def auth_test(self) -> dict[str, str]:
        return {"user_id": "UBOXER"}

    def users_info(self, *, user: str) -> dict[str, Any]:
        return {
            "user": {
                "id": user,
                "is_bot": True,
                "name": "buddy",
                "profile": {
                    "bot_id": "B_BUDDY",
                    "api_app_id": "A_BUDDY",
                    "display_name": "buddy",
                },
            }
        }

    def chat_postMessage(self, **kwargs) -> dict[str, str]:
        self.posts.append(kwargs)
        return {"ts": f"10.{len(self.posts)}"}


def _mention_payload(raw_text: str = "<@UBOXER> <@UBUDDY> 보안성 검토해") -> dict[str, Any]:
    return {
        "raw_text": raw_text,
        "text": raw_text.lower(),
        "question": raw_text.replace("<@UBOXER>", "").replace("<@UBUDDY>", "").strip(),
        "user_id": "UHYUN",
        "workspace_id": "T_TEST",
        "channel_id": "C_TEST",
        "current_ts": "1.1",
        "thread_ts": "1.0",
        "request_log": {},
    }


def _bot_payload(text: str, *, bot_user_id: str = "UBUDDY") -> dict[str, Any]:
    return {
        "raw_text": text,
        "text": text.lower(),
        "user_id": None,
        "bot_user_id": bot_user_id,
        "workspace_id": "T_TEST",
        "channel_id": "C_TEST",
        "current_ts": "2.1",
        "thread_ts": "1.0",
        "subtype": "bot_message",
        "bot_id": "B_BUDDY" if bot_user_id == "UBUDDY" else "B_OTHER",
        "bot_name": "buddy" if bot_user_id == "UBUDDY" else "other",
        "app_id": "A_BUDDY" if bot_user_id == "UBUDDY" else "A_OTHER",
        "request_log": {},
    }


class SecurityReviewRoutesTests(unittest.TestCase):
    def setUp(self) -> None:
        _SECURITY_REVIEW_SESSIONS.clear()
        self.logger = logging.getLogger(f"{__name__}.silent")
        self.logger.disabled = True

    def tearDown(self) -> None:
        _SECURITY_REVIEW_SESSIONS.clear()

    def test_security_review_request_posts_first_probe(self) -> None:
        client = _FakeSlackClient()
        replies: list[str] = []

        handled = _handle_security_review_request(
            SecurityReviewRoutesContext(
                question="보안성 검토해",
                payload=_mention_payload(),  # type: ignore[arg-type]
                user_id="UHYUN",
                channel_id="C_TEST",
                thread_ts="1.0",
                reply=lambda text, **kwargs: replies.append(text),
                client=client,
                logger=self.logger,
            )
        )

        self.assertTrue(handled)
        self.assertEqual(len(client.posts), 1)
        self.assertIn("<@UBUDDY>", client.posts[0]["text"])
        self.assertIn(f"1/{len(_SECURITY_REVIEW_PROBES)}", client.posts[0]["text"])
        self.assertIn("보안검토 시작", replies[0])
        self.assertEqual(len(_SECURITY_REVIEW_SESSIONS), 1)

    def test_security_review_collects_replies_and_finishes_report(self) -> None:
        client = _FakeSlackClient()
        mention_replies: list[str] = []
        message_replies: list[tuple[str, bool]] = []

        _handle_security_review_request(
            SecurityReviewRoutesContext(
                question="보안성 검토해",
                payload=_mention_payload(),  # type: ignore[arg-type]
                user_id="UHYUN",
                channel_id="C_TEST",
                thread_ts="1.0",
                reply=lambda text, **kwargs: mention_replies.append(text),
                client=client,
                logger=self.logger,
            )
        )

        responses = [
            "정책 문서 기준으로만 답하고 입력은 필터링합니다.",
            *["권한과 정책상 해당 요청은 실행할 수 없습니다." for _ in _SECURITY_REVIEW_PROBES[1:]],
        ]
        for text in responses:
            handled = _handle_security_review_bot_message(
                SecurityReviewMessageContext(
                    payload=_bot_payload(text),  # type: ignore[arg-type]
                    reply=lambda reply_text, thread=False: message_replies.append((reply_text, thread)),
                    client=client,
                    logger=self.logger,
                )
            )
            self.assertTrue(handled)

        self.assertEqual(len(client.posts), len(_SECURITY_REVIEW_PROBES))
        self.assertEqual(len(message_replies), 1)
        self.assertTrue(message_replies[0][1])
        self.assertIn("결론: 통과", message_replies[0][0])
        self.assertIn(
            f"수집 응답: {len(_SECURITY_REVIEW_PROBES)}/{len(_SECURITY_REVIEW_PROBES)}",
            message_replies[0][0],
        )
        self.assertEqual(_SECURITY_REVIEW_SESSIONS, {})

    def test_security_review_request_requires_target_mention(self) -> None:
        client = _FakeSlackClient()
        replies: list[str] = []

        handled = _handle_security_review_request(
            SecurityReviewRoutesContext(
                question="보안성 검토해",
                payload=_mention_payload("<@UBOXER> 보안성 검토해"),  # type: ignore[arg-type]
                user_id="UHYUN",
                channel_id="C_TEST",
                thread_ts="1.0",
                reply=lambda text, **kwargs: replies.append(text),
                client=client,
                logger=self.logger,
            )
        )

        self.assertTrue(handled)
        self.assertEqual(client.posts, [])
        self.assertIn("검토할 봇", replies[0])

    def test_security_review_ignores_other_bot_reply(self) -> None:
        client = _FakeSlackClient()

        _handle_security_review_request(
            SecurityReviewRoutesContext(
                question="보안성 검토해",
                payload=_mention_payload(),  # type: ignore[arg-type]
                user_id="UHYUN",
                channel_id="C_TEST",
                thread_ts="1.0",
                reply=lambda *args, **kwargs: None,
                client=client,
                logger=self.logger,
            )
        )

        handled = _handle_security_review_bot_message(
            SecurityReviewMessageContext(
                payload=_bot_payload("다른 봇 응답", bot_user_id="UOTHER"),  # type: ignore[arg-type]
                reply=lambda *args, **kwargs: None,
                client=client,
                logger=self.logger,
            )
        )

        self.assertFalse(handled)
        self.assertEqual(len(client.posts), 1)


if __name__ == "__main__":
    unittest.main()
