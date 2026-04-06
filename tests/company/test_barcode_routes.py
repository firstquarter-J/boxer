import logging
import unittest
from unittest.mock import patch

from boxer_company_adapter_slack.barcode_routes import (
    BarcodeLogRouteContext,
    BarcodeLogRouteDeps,
    _handle_barcode_log_analysis_request,
)


def _build_deps() -> BarcodeLogRouteDeps:
    return BarcodeLogRouteDeps(
        get_s3_client=lambda: None,
        get_recordings_context=lambda: {},
        has_recordings_device_mapping=lambda context: False,
        attach_recordings_context_to_evidence=lambda evidence, context: None,
        reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
        is_claude_allowed_user=lambda user_id: True,
        is_timeout_error=lambda exc: False,
        attach_notion_playbooks_to_evidence=lambda evidence: [],
    )


class BarcodeRouteHandlerTests(unittest.TestCase):
    def test_returns_false_when_question_is_not_barcode_log_route(self) -> None:
        replies: list[tuple[str, bool]] = []

        handled = _handle_barcode_log_analysis_request(
            BarcodeLogRouteContext(
                question="핑",
                barcode=None,
                is_phase2_scope_followup=False,
                phase2_hospital_name=None,
                phase2_room_name=None,
                thread_ts="1",
                user_id="U123",
                channel_id="C123",
                current_ts="1.1",
                reply=lambda text, mention_user=True: replies.append((text, mention_user)),
                logger=logging.getLogger(__name__),
                claude_client=None,
                client=None,
            ),
            _build_deps(),
        )

        self.assertFalse(handled)
        self.assertEqual(replies, [])

    def test_replies_with_s3_config_message_when_s3_query_disabled(self) -> None:
        replies: list[tuple[str, bool]] = []

        with patch("boxer_company_adapter_slack.barcode_routes.s.S3_QUERY_ENABLED", False):
            handled = _handle_barcode_log_analysis_request(
                BarcodeLogRouteContext(
                    question="12345678901 로그 분석해줘",
                    barcode="12345678901",
                    is_phase2_scope_followup=False,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    thread_ts="1",
                    user_id="U123",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda text, mention_user=True: replies.append((text, mention_user)),
                    logger=logging.getLogger(__name__),
                    claude_client=None,
                    client=None,
                ),
                _build_deps(),
            )

        self.assertTrue(handled)
        self.assertEqual(
            replies,
            [("로그 분석 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘", True)],
        )


if __name__ == "__main__":
    unittest.main()
