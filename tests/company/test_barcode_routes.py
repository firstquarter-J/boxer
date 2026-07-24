import logging
import unittest
from unittest.mock import patch

from boxer_company.assistant import AssistantMessage, CompanyAssistantResult
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
    def test_channel_neutral_service_preserves_slack_chunking_and_mentions(self) -> None:
        replies: list[tuple[str, bool]] = []
        captured_requests: list[object] = []

        class Service:
            def answer(self, request):
                captured_requests.append(request)
                long_body = "\n".join(
                    f"• 로그 줄 {index}: {'x' * 40}"
                    for index in range(100)
                )
                return CompanyAssistantResult(
                    route="barcode_log_analysis",
                    outcome="answered",
                    messages=(
                        AssistantMessage(body=f"**로그 분석**\n{long_body}"),
                        AssistantMessage(
                            body="**세션별 에러 분석**\n• 조치: 확인",
                            mention_actor=False,
                        ),
                    ),
                )

        payload = {
            "text": "12345678901 로그 분석",
            "question": "12345678901 로그 분석",
            "user_id": "U123",
            "workspace_id": "W123",
            "channel_id": "C123",
            "current_ts": "1.1",
            "thread_ts": "1",
        }
        handled = _handle_barcode_log_analysis_request(
            BarcodeLogRouteContext(
                question="12345678901 로그 분석",
                barcode="12345678901",
                is_phase2_scope_followup=False,
                phase2_hospital_name=None,
                phase2_room_name=None,
                thread_ts="1",
                user_id="U123",
                channel_id="C123",
                current_ts="1.1",
                reply=lambda text, mention_user=True: replies.append(
                    (text, mention_user)
                ),
                logger=logging.getLogger(__name__),
                claude_client=None,
                client=None,
                payload=payload,  # type: ignore[arg-type]
                assistant_service=Service(),  # type: ignore[arg-type]
                context_entries=(
                    {
                        "kind": "message",
                        "source": "slack",
                        "author_id": "U123",
                        "text": "이전 로그 질문",
                    },
                ),
            ),
            _build_deps(),
        )

        self.assertTrue(handled)
        self.assertGreaterEqual(len(replies), 3)
        self.assertTrue(replies[0][1])
        self.assertTrue(all(not mention for _, mention in replies[1:]))
        self.assertTrue(all(len(text) <= 3000 for text, _ in replies))
        self.assertEqual(
            captured_requests[0].context_entries[0]["text"],
            "이전 로그 질문",
        )
        self.assertEqual(
            payload["request_log"]["route_name"],
            "barcode log analysis",
        )

    def test_progress_renders_main_then_summary_without_duplicate_mention(
        self,
    ) -> None:
        replies: list[tuple[str, bool]] = []

        class ProgressiveService:
            def answer_with_progress(self, request, on_partial_result):
                on_partial_result(
                    CompanyAssistantResult(
                        route="barcode_log_analysis",
                        outcome="answered",
                        messages=(
                            AssistantMessage(body="**확정 본문**"),
                        ),
                    )
                )
                return CompanyAssistantResult(
                    route="barcode_log_analysis",
                    outcome="answered",
                    messages=(
                        AssistantMessage(
                            body="**세션별 요약**",
                            mention_actor=False,
                        ),
                    ),
                    used_llm=True,
                )

        payload = {
            "question": "12345678901 로그 분석",
            "user_id": "U123",
            "workspace_id": "W123",
            "channel_id": "C123",
            "current_ts": "1.1",
            "thread_ts": "1",
        }
        handled = _handle_barcode_log_analysis_request(
            BarcodeLogRouteContext(
                question="12345678901 로그 분석",
                barcode="12345678901",
                is_phase2_scope_followup=False,
                phase2_hospital_name=None,
                phase2_room_name=None,
                thread_ts="1",
                user_id="U123",
                channel_id="C123",
                current_ts="1.1",
                reply=lambda text, mention_user=True: replies.append(
                    (text, mention_user)
                ),
                logger=logging.getLogger(__name__),
                claude_client=None,
                client=None,
                payload=payload,  # type: ignore[arg-type]
                assistant_service=ProgressiveService(),  # type: ignore[arg-type]
            ),
            _build_deps(),
        )

        self.assertTrue(handled)
        self.assertEqual(
            replies,
            [("*확정 본문*", True), ("*세션별 요약*", False)],
        )
        self.assertEqual(
            payload["request_log"]["route_name"],
            "barcode log analysis",
        )
        self.assertTrue(
            payload["request_log"]["metadata"]["assistantUsedLlm"]
        )

    def test_progress_empty_terminal_result_is_still_handled_once(self) -> None:
        replies: list[tuple[str, bool]] = []

        class ProgressiveService:
            def answer_with_progress(self, request, on_partial_result):
                on_partial_result(
                    CompanyAssistantResult(
                        route="barcode_log_analysis",
                        outcome="answered",
                        messages=(AssistantMessage(body="확정 본문"),),
                    )
                )
                return CompanyAssistantResult(
                    route="barcode_log_analysis",
                    outcome="answered",
                    messages=(),
                )

        payload = {
            "question": "12345678901 로그 분석",
            "user_id": "U123",
            "workspace_id": "W123",
            "channel_id": "C123",
            "current_ts": "1.1",
            "thread_ts": "1",
        }
        handled = _handle_barcode_log_analysis_request(
            BarcodeLogRouteContext(
                question="12345678901 로그 분석",
                barcode="12345678901",
                is_phase2_scope_followup=False,
                phase2_hospital_name=None,
                phase2_room_name=None,
                thread_ts="1",
                user_id="U123",
                channel_id="C123",
                current_ts="1.1",
                reply=lambda text, mention_user=True: replies.append(
                    (text, mention_user)
                ),
                logger=logging.getLogger(__name__),
                claude_client=None,
                client=None,
                payload=payload,  # type: ignore[arg-type]
                assistant_service=ProgressiveService(),  # type: ignore[arg-type]
            ),
            _build_deps(),
        )

        self.assertTrue(handled)
        self.assertEqual(replies, [("확정 본문", True)])

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

    def test_direct_device_name_scope_is_used_for_dated_log_analysis(self) -> None:
        replies: list[tuple[str, bool]] = []
        synth_calls: list[tuple[str, dict[str, object], str]] = []

        deps = BarcodeLogRouteDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {"summary": {"recordingCount": 0}, "rows": []},
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
                    question=(
                        "16971952215 나무정원여성병원(양주) 2층1-1진료실 "
                        "MB2-A00313 2026-04-22 로그 분석"
                    ),
                    barcode="16971952215",
                    is_phase2_scope_followup=False,
                    phase2_hospital_name="나무정원여성병원(양주)",
                    phase2_room_name="2층1-1진료실",
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
        self.assertEqual(
            analyze_scan.call_args.kwargs["device_contexts"],
            [
                {
                    "deviceName": "MB2-A00313",
                    "hospitalName": "나무정원여성병원(양주)",
                    "roomName": "2층1-1진료실",
                }
            ],
        )


if __name__ == "__main__":
    unittest.main()
