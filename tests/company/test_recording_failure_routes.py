import logging
import unittest
from unittest.mock import patch

from boxer_company_adapter_slack.recording_failure_routes import (
    RecordingFailureRouteContext,
    RecordingFailureRouteDeps,
    _handle_recording_failure_analysis_request,
)


def _build_deps() -> RecordingFailureRouteDeps:
    return RecordingFailureRouteDeps(
        get_s3_client=lambda: None,
        get_recordings_context=lambda: {},
        has_recordings_device_mapping=lambda context: False,
        attach_recordings_context_to_evidence=lambda evidence, context: None,
        reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
    )


class RecordingFailureRouteHandlerTests(unittest.TestCase):
    def test_returns_false_when_question_is_not_recording_failure_route(self) -> None:
        replies: list[tuple[str, bool]] = []

        handled = _handle_recording_failure_analysis_request(
            RecordingFailureRouteContext(
                question="핑",
                barcode=None,
                is_failure_phase2_scope_followup=False,
                phase2_hospital_name=None,
                phase2_room_name=None,
                thread_context_for_scope="",
                thread_ts="1",
                user_id="U123",
                channel_id="C123",
                current_ts="1.1",
                reply=lambda text, mention_user=True: replies.append((text, mention_user)),
                logger=logging.getLogger(__name__),
                client=None,
            ),
            _build_deps(),
        )

        self.assertFalse(handled)
        self.assertEqual(replies, [])

    def test_replies_with_s3_config_message_when_s3_query_disabled(self) -> None:
        replies: list[tuple[str, bool]] = []

        with patch("boxer_company_adapter_slack.recording_failure_routes.s.S3_QUERY_ENABLED", False):
            handled = _handle_recording_failure_analysis_request(
                RecordingFailureRouteContext(
                    question="12345678901 녹화 실패 원인 분석해줘",
                    barcode="12345678901",
                    is_failure_phase2_scope_followup=False,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    thread_context_for_scope="",
                    thread_ts="1",
                    user_id="U123",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda text, mention_user=True: replies.append((text, mention_user)),
                    logger=logging.getLogger(__name__),
                    client=None,
                ),
                _build_deps(),
            )

        self.assertTrue(handled)
        self.assertEqual(
            replies,
            [("녹화 실패 원인 분석을 위해 S3_QUERY_ENABLED=true가 필요해", True)],
        )


if __name__ == "__main__":
    unittest.main()
