from contextlib import ExitStack
import logging
from types import SimpleNamespace
from typing import Any, Callable
import unittest
from unittest.mock import Mock, patch

from boxer_company_adapter_slack import company


_ROUTE_HANDLER_ORDER = (
    "_handle_hpa_change_request",
    "_handle_thread_learning_routes",
    "_handle_security_review_request",
    "_handle_admin_routes",
    "_handle_company_notion_routes",
    "_handle_device_routes",
    "_handle_recording_failure_analysis_request",
    "_handle_barcode_log_analysis_request",
    "_handle_structured_routes",
    "_handle_barcode_query_routes",
    "_handle_knowledge_routes",
)


def _mention_payload(*, text: str, question: str) -> dict[str, Any]:
    return {
        "raw_text": text,
        "text": text,
        "question": question,
        "user_id": "U-CONTRACT",
        "workspace_id": "T-CONTRACT",
        "channel_id": "C-CONTRACT",
        "current_ts": "1784800000.000002",
        "thread_ts": "1784800000.000001",
        "request_log": {},
    }


def _silent_logger() -> logging.Logger:
    logger = logging.getLogger(f"{__name__}.silent")
    logger.disabled = True
    return logger


class CompanyRouteContractTests(unittest.TestCase):
    def _invoke_mention(
        self,
        *,
        text: str = "일반 질문",
        question: str = "일반 질문",
        barcode: str | None = None,
        route_results: dict[str, bool] | None = None,
        real_handlers: set[str] | None = None,
        llm_provider: str = "",
        llm_synthesis_enabled: bool = False,
        synthesized_text: str = "",
        synthesis_side_effect: Exception | None = None,
    ) -> SimpleNamespace:
        route_results = route_results or {}
        real_handlers = real_handlers or set()
        route_calls: list[str] = []
        reply_calls: list[tuple[str, dict[str, Any]]] = []
        captured_handlers: dict[str, Callable[..., None]] = {}
        fake_app = SimpleNamespace(client=object())
        fake_runtime = SimpleNamespace(
            routes_config=SimpleNamespace(enabled=False),
            submit_request=Mock(),
            lookup_thread_job=Mock(),
        )
        payload = _mention_payload(text=text, question=question)

        def fake_create_slack_app(
            mention_handler: Callable[..., None],
            message_handler: Callable[..., None],
        ) -> Any:
            captured_handlers["mention"] = mention_handler
            captured_handlers["message"] = message_handler
            return fake_app

        def reply(reply_text: str, **kwargs: Any) -> None:
            reply_calls.append((reply_text, kwargs))

        def record_prefetch(target_barcode: str) -> dict[str, Any]:
            route_calls.append("recordings_context_prefetch")
            return {
                "summary": {"recordingCount": 0},
                "rows": [],
                "limit": 30,
                "has_more": False,
                "barcode": target_barcode,
            }

        with ExitStack() as stack:
            stack.enter_context(patch.object(company, "_validate_ec2_runtime_aws_env"))
            stack.enter_context(patch.object(company, "_validate_tokens"))
            stack.enter_context(patch.object(company.s, "LLM_PROVIDER", llm_provider))
            stack.enter_context(
                patch.object(
                    company.s,
                    "LLM_SYNTHESIS_ENABLED",
                    llm_synthesis_enabled,
                )
            )
            stack.enter_context(
                patch.object(
                    company.s,
                    "LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT",
                    False,
                )
            )
            stack.enter_context(
                patch.object(company.cs, "CLAUDE_ALLOWED_USER_IDS", set())
            )
            stack.enter_context(
                patch.object(company, "_build_claude_client", return_value=object())
            )
            synthesis_patch_kwargs: dict[str, Any]
            if synthesis_side_effect is None:
                synthesis_patch_kwargs = {"return_value": synthesized_text}
            else:
                synthesis_patch_kwargs = {"side_effect": synthesis_side_effect}
            synthesis_mock = stack.enter_context(
                patch.object(
                    company,
                    "_synthesize_retrieval_answer",
                    **synthesis_patch_kwargs,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "create_hpa_change_runtime",
                    return_value=fake_runtime,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "create_slack_app",
                    side_effect=fake_create_slack_app,
                )
            )
            for reporter_name in (
                "attach_hpa_change_reporter",
                "attach_weekly_recordings_reporter",
                "attach_device_health_monitor_reporter",
                "attach_device_notification_alert_reporter",
                "attach_daily_device_round_reporter",
            ):
                stack.enter_context(patch.object(company, reporter_name))
            stack.enter_context(
                patch.object(company, "_extract_barcode", return_value=barcode)
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_extract_hospital_room_scope",
                    return_value=(None, None),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_load_recordings_context_by_barcode",
                    side_effect=record_prefetch,
                )
            )

            for handler_name in _ROUTE_HANDLER_ORDER:
                original_handler = getattr(company, handler_name)

                def route_side_effect(
                    *args: Any,
                    _handler_name: str = handler_name,
                    _original_handler: Callable[..., bool] = original_handler,
                    **kwargs: Any,
                ) -> bool:
                    route_calls.append(_handler_name)
                    if _handler_name in real_handlers:
                        return _original_handler(*args, **kwargs)
                    return route_results.get(_handler_name, False)

                stack.enter_context(
                    patch.object(
                        company,
                        handler_name,
                        side_effect=route_side_effect,
                    )
                )

            app = company.create_app()
            captured_handlers["mention"](
                payload,
                reply,
                Mock(),
                _silent_logger(),
            )

        return SimpleNamespace(
            app=app,
            payload=payload,
            route_calls=route_calls,
            reply_calls=reply_calls,
            synthesis_mock=synthesis_mock,
        )

    def test_route_handlers_keep_golden_order_and_short_circuit(self) -> None:
        # 각 라우터가 매칭되는 지점마다 이후 라우터가 실행되지 않는지 함께 고정한다.
        for index, matched_handler in enumerate(_ROUTE_HANDLER_ORDER):
            with self.subTest(matched_handler=matched_handler):
                result = self._invoke_mention(
                    route_results={matched_handler: True},
                )

                self.assertEqual(
                    result.route_calls,
                    list(_ROUTE_HANDLER_ORDER[: index + 1]),
                )

    def test_recordings_context_prefetch_stays_between_notion_and_device(self) -> None:
        result = self._invoke_mention(
            text="12345678910 일반 질문",
            question="12345678910 일반 질문",
            barcode="12345678910",
        )

        self.assertEqual(
            result.route_calls,
            [
                *_ROUTE_HANDLER_ORDER[:5],
                "recordings_context_prefetch",
                *_ROUTE_HANDLER_ORDER[5:],
            ],
        )
        self.assertEqual(
            result.reply_calls,
            [
                (
                    "지원 기능이 궁금하면 `사용법`이라고 보내줘",
                    {"mention_user": False},
                )
            ],
        )

    def test_hpa_then_ping_then_usage_help_are_priority_gates(self) -> None:
        hpa_result = self._invoke_mention(
            text="HPA 반영 요청 ping",
            question="HPA 반영 요청 ping",
            route_results={"_handle_hpa_change_request": True},
        )
        self.assertEqual(hpa_result.route_calls, ["_handle_hpa_change_request"])
        self.assertEqual(hpa_result.reply_calls, [])

        ping_result = self._invoke_mention(text="ping", question="ping")
        self.assertEqual(ping_result.route_calls, ["_handle_hpa_change_request"])
        self.assertEqual(
            ping_result.reply_calls,
            [("🏓 pong\n• llm: 미설정", {})],
        )
        self.assertEqual(
            ping_result.payload["request_log"]["route_name"],
            "ping",
        )

        usage_result = self._invoke_mention(text="사용법", question="사용법")
        self.assertEqual(usage_result.route_calls, ["_handle_hpa_change_request"])
        self.assertEqual(len(usage_result.reply_calls), 1)
        self.assertTrue(usage_result.reply_calls[0][0].startswith("*사용법*\n"))
        self.assertEqual(
            usage_result.reply_calls[0][1],
            {"mention_user": False},
        )
        self.assertEqual(
            usage_result.payload["request_log"]["route_name"],
            "usage_help",
        )

    def test_evidence_route_keeps_direct_and_llm_synthesis_outcomes(self) -> None:
        notion_reference = {
            "title": "Commerce",
            "url": "https://app.notion.com/p/commerce-contract",
            "objectType": "page",
            "lastEditedTime": "2026-07-23T00:00:00.000Z",
            "excerpts": ["Commerce는 커머스 사업을 담당해."],
            "blockCount": 1,
            "contentTruncated": False,
        }
        with (
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_is_company_notion_search_allowed",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_is_company_notion_search_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_search_company_notion",
                return_value=[object()],
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_load_company_notion_references",
                return_value=[notion_reference],
            ),
        ):
            direct_result = self._invoke_mention(
                text="회사 노션에서 Commerce 찾아줘",
                question="회사 노션에서 Commerce 찾아줘",
                real_handlers={"_handle_company_notion_routes"},
                llm_provider="claude",
                llm_synthesis_enabled=False,
            )
            synthesized_result = self._invoke_mention(
                text="회사 노션에서 Commerce 찾아줘",
                question="회사 노션에서 Commerce 찾아줘",
                real_handlers={"_handle_company_notion_routes"},
                llm_provider="claude",
                llm_synthesis_enabled=True,
                synthesized_text="Commerce는 커머스 사업을 담당해.",
            )

        self.assertEqual(len(direct_result.reply_calls), 1)
        self.assertIn(
            "관련 문서는 찾았지만 지금은 답변을 만들지 못했어",
            direct_result.reply_calls[0][0],
        )
        self.assertIn(
            notion_reference["url"],
            direct_result.reply_calls[0][0],
        )
        direct_result.synthesis_mock.assert_not_called()

        self.assertEqual(len(synthesized_result.reply_calls), 1)
        self.assertIn(
            "Commerce는 커머스 사업을 담당해.",
            synthesized_result.reply_calls[0][0],
        )
        self.assertIn(
            notion_reference["url"],
            synthesized_result.reply_calls[0][0],
        )
        synthesized_result.synthesis_mock.assert_called_once()
        self.assertEqual(
            synthesized_result.synthesis_mock.call_args.kwargs["provider"],
            "claude",
        )
        self.assertEqual(
            synthesized_result.synthesis_mock.call_args.kwargs[
                "thread_context"
            ],
            "",
        )
        self.assertIsNone(
            synthesized_result.synthesis_mock.call_args.kwargs[
                "system_prompt"
            ]
        )
        self.assertEqual(
            synthesized_result.payload["request_log"]["route_name"],
            "company_notion_qa",
        )

    def test_company_notion_timeout_preserves_safe_fallback_and_source(self) -> None:
        notion_reference = {
            "title": "영업 안내",
            "url": "https://app.notion.com/p/sales-contract",
            "objectType": "page",
            "lastEditedTime": "2026-07-23T00:00:00.000Z",
            "excerpts": ["영업 관련 근거"],
            "blockCount": 1,
            "contentTruncated": False,
        }
        with (
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_is_company_notion_search_allowed",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_is_company_notion_search_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_search_company_notion",
                return_value=[object()],
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes."
                "_load_company_notion_references",
                return_value=[notion_reference],
            ),
        ):
            result = self._invoke_mention(
                text="회사 노션에서 영업 찾아줘",
                question="회사 노션에서 영업 찾아줘",
                real_handlers={"_handle_company_notion_routes"},
                llm_provider="claude",
                llm_synthesis_enabled=True,
                synthesis_side_effect=TimeoutError("contract timeout"),
            )

        self.assertEqual(len(result.reply_calls), 1)
        self.assertIn(
            "관련 문서는 찾았지만 지금은 답변을 만들지 못했어",
            result.reply_calls[0][0],
        )
        self.assertIn(
            notion_reference["url"],
            result.reply_calls[0][0],
        )
        self.assertNotIn("타임아웃", result.reply_calls[0][0])
        result.synthesis_mock.assert_called_once()

    def test_real_structured_question_matches_before_barcode_and_knowledge(self) -> None:
        with patch(
            "boxer_company_adapter_slack.structured_routes."
            "_query_hospital_rooms_by_filters",
            return_value="*병실 조회*\n• 서울병원 병실 2개",
        ) as query_mock:
            result = self._invoke_mention(
                text="병원명 서울병원 병실 목록",
                question="병원명 서울병원 병실 목록",
                real_handlers={"_handle_structured_routes"},
            )

        self.assertEqual(
            result.route_calls,
            list(
                _ROUTE_HANDLER_ORDER[
                    : _ROUTE_HANDLER_ORDER.index(
                        "_handle_structured_routes"
                    )
                    + 1
                ]
            ),
        )
        self.assertEqual(
            result.reply_calls,
            [("*병실 조회*\n• 서울병원 병실 2개", {})],
        )
        query_mock.assert_called_once_with(
            hospital_name="서울병원",
            room_name=None,
            hospital_seq=None,
            hospital_room_seq=None,
            count_only=False,
        )

    def test_real_barcode_question_matches_after_structured_route(self) -> None:
        with patch(
            "boxer_company_adapter_slack.barcode_query_routes."
            "_query_recordings_count_by_barcode",
            return_value="*영상 개수*\n• 총 0개",
        ) as query_mock:
            result = self._invoke_mention(
                text="12345678910 영상 개수",
                question="12345678910 영상 개수",
                barcode="12345678910",
                real_handlers={"_handle_barcode_query_routes"},
            )

        barcode_route_index = _ROUTE_HANDLER_ORDER.index(
            "_handle_barcode_query_routes"
        )
        self.assertEqual(
            result.route_calls,
            [
                *_ROUTE_HANDLER_ORDER[:5],
                "recordings_context_prefetch",
                *_ROUTE_HANDLER_ORDER[5 : barcode_route_index + 1],
            ],
        )
        self.assertEqual(
            result.reply_calls,
            [("*영상 개수*\n• 총 0개", {})],
        )
        query_mock.assert_called_once()
        self.assertEqual(
            query_mock.call_args.args[0],
            "12345678910",
        )
        self.assertEqual(
            query_mock.call_args.kwargs["recordings_context"]["summary"],
            {"recordingCount": 0},
        )

    def test_company_notion_permission_denial_is_terminal(self) -> None:
        with patch(
            "boxer_company_adapter_slack.company_notion_routes."
            "_is_company_notion_search_allowed",
            return_value=False,
        ):
            result = self._invoke_mention(
                text="회사 노션에서 영업 찾아줘",
                question="회사 노션에서 영업 찾아줘",
                real_handlers={"_handle_company_notion_routes"},
            )

        self.assertEqual(
            result.route_calls,
            list(_ROUTE_HANDLER_ORDER[:5]),
        )
        self.assertEqual(
            result.reply_calls,
            [("회사 Notion 검색은 아직 허용된 사용자만 쓸 수 있어", {})],
        )
        self.assertEqual(
            result.payload["request_log"]["route_name"],
            "company_notion_search",
        )


if __name__ == "__main__":
    unittest.main()
