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

    def test_uses_recordings_scope_fallback_when_device_mapping_is_missing(self) -> None:
        replies: list[tuple[str, bool]] = []
        synth_calls: list[tuple[str, dict[str, object], str]] = []

        deps = RecordingFailureRouteDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {"summary": {"recordingCount": 1}, "rows": []},
            has_recordings_device_mapping=lambda context: False,
            attach_recordings_context_to_evidence=lambda evidence, context: None,
            reply_with_retrieval_synthesis=lambda fallback_text, evidence_payload, route_name, **kwargs: synth_calls.append(
                (fallback_text, evidence_payload, route_name)
            ),
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
        )

        with (
            patch("boxer_company_adapter_slack.recording_failure_routes.s.S3_QUERY_ENABLED", True),
            patch("boxer_company_adapter_slack.recording_failure_routes.s.DB_HOST", "db-host"),
            patch("boxer_company_adapter_slack.recording_failure_routes.s.DB_USERNAME", "db-user"),
            patch("boxer_company_adapter_slack.recording_failure_routes.s.DB_PASSWORD", "db-pass"),
            patch("boxer_company_adapter_slack.recording_failure_routes.s.DB_DATABASE", "db-name"),
            patch(
                "boxer_company_adapter_slack.recording_failure_routes._extract_log_date_with_presence",
                return_value=("2026-04-18", True),
            ),
            patch(
                "boxer_company_adapter_slack.recording_failure_routes._lookup_device_contexts_by_barcode_on_date",
                return_value=[{"deviceName": "MB2-C00419"}],
            ),
            patch(
                "boxer_company_adapter_slack.recording_failure_routes._analyze_barcode_log_errors",
                return_value=("*바코드 로그 에러 분석 결과*\n• 판단: 테스트", {"records": []}),
            ),
            patch(
                "boxer_company_adapter_slack.recording_failure_routes._build_recording_failure_analysis_evidence",
                return_value={"request": {}},
            ),
            patch(
                "boxer_company_adapter_slack.recording_failure_routes._narrow_recording_failure_analysis_evidence",
                side_effect=lambda evidence, selector_text: (evidence, None),
            ),
            patch(
                "boxer_company_adapter_slack.recording_failure_routes._render_recording_failure_analysis_fallback",
                return_value="*녹화 실패 원인 분석*\n• 판단: 테스트",
            ),
            patch(
                "boxer_company_adapter_slack.recording_failure_routes._load_slack_thread_context",
                return_value="",
            ),
        ):
            handled = _handle_recording_failure_analysis_request(
                RecordingFailureRouteContext(
                    question="13194526492 2026-04-18 녹화 실패 원인 분석",
                    barcode="13194526492",
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
                deps,
            )

        self.assertTrue(handled)
        self.assertEqual(replies, [])
        self.assertEqual(len(synth_calls), 1)
        self.assertEqual(synth_calls[0][0], "*녹화 실패 원인 분석*\n• 판단: 테스트")
        self.assertEqual(synth_calls[0][2], "recording failure analysis")
        self.assertTrue(synth_calls[0][1]["request"]["usedRecordingsScopeFallback"])


if __name__ == "__main__":
    unittest.main()
