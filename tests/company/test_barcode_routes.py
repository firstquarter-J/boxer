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

    def test_uses_recordings_scope_fallback_when_device_mapping_is_missing(self) -> None:
        replies: list[tuple[str, bool]] = []
        synth_calls: list[tuple[str, dict[str, object], str]] = []

        deps = BarcodeLogRouteDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {"summary": {"recordingCount": 1}, "rows": []},
            has_recordings_device_mapping=lambda context: False,
            attach_recordings_context_to_evidence=lambda evidence, context: None,
            reply_with_retrieval_synthesis=lambda fallback_text, evidence_payload, route_name, **kwargs: synth_calls.append(
                (fallback_text, evidence_payload, route_name)
            ),
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            is_claude_allowed_user=lambda user_id: True,
            is_timeout_error=lambda exc: False,
            attach_notion_playbooks_to_evidence=lambda evidence: [],
        )

        with (
            patch("boxer_company_adapter_slack.barcode_routes.s.S3_QUERY_ENABLED", True),
            patch("boxer_company_adapter_slack.barcode_routes.s.DB_HOST", "db-host"),
            patch("boxer_company_adapter_slack.barcode_routes.s.DB_USERNAME", "db-user"),
            patch("boxer_company_adapter_slack.barcode_routes.s.DB_PASSWORD", "db-pass"),
            patch("boxer_company_adapter_slack.barcode_routes.s.DB_DATABASE", "db-name"),
            patch(
                "boxer_company_adapter_slack.barcode_routes._extract_log_date_with_presence",
                return_value=("2026-04-18", True),
            ),
            patch(
                "boxer_company_adapter_slack.barcode_routes._lookup_device_contexts_by_barcode_on_date",
                return_value=[{"deviceName": "MB2-C00419"}],
            ),
            patch(
                "boxer_company_adapter_slack.barcode_routes._analyze_barcode_log_scan_events",
                return_value=("*로그 분석 결과*\n• 판단: 테스트", {"records": []}),
            ) as analyze_scan,
            patch(
                "boxer_company_adapter_slack.barcode_routes._reply_with_barcode_log_error_summary",
                return_value=None,
            ),
        ):
            handled = _handle_barcode_log_analysis_request(
                BarcodeLogRouteContext(
                    question="13194526492 2026-04-18 로그",
                    barcode="13194526492",
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
                deps,
            )

        self.assertTrue(handled)
        self.assertEqual(replies, [])
        self.assertEqual(len(synth_calls), 1)
        self.assertEqual(synth_calls[0][0], "*로그 분석 결과*\n• 판단: 테스트")
        self.assertEqual(synth_calls[0][2], "barcode log analysis")
        self.assertTrue(synth_calls[0][1]["request"]["usedRecordingsScopeFallback"])
        analyze_scan.assert_called_once()


if __name__ == "__main__":
    unittest.main()
