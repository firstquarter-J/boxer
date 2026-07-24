from dataclasses import fields
import logging
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from boxer import AnswerRequest, AnswerResult
from boxer_company.assistant import (
    AssistantMessage,
    BarcodeQueryAssistantRoute,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    CompanyAssistantService,
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerComposerDeps,
    CompanyNotionAssistantRoute,
    CompanyNotionAssistantRouteDeps,
    RequestScopedRecordingsContext,
    SourceReference,
    StructuredAssistantRoute,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.notion_workspace_search import CompanyNotionSearchResult


def _request(question: str = "질문") -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
        context_entries=(
            {
                "kind": "message",
                "source": "slack",
                "author_id": "U-OLD",
                "text": "섞이면 안 되는 이전 Slack 문맥",
            },
        ),
    )


class _FakeRoute:
    def __init__(
        self,
        name: str,
        calls: list[str],
        result: CompanyAssistantResult | None,
    ) -> None:
        self.name = name
        self._calls = calls
        self._result = result

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        self._calls.append(self.name)
        return self._result


class _FakeAnswerEngine:
    def __init__(
        self,
        result: AnswerResult,
        *,
        provider: str = "claude",
    ) -> None:
        self.provider = provider
        self.result = result
        self.requests: list[AnswerRequest] = []

    def answer(self, request: AnswerRequest) -> AnswerResult:
        self.requests.append(request)
        return self.result


class CompanyAssistantServiceTests(unittest.TestCase):
    def test_slack_formatter_output_is_normalized_to_commonmark(self) -> None:
        self.assertEqual(
            slack_mrkdwn_to_commonmark(
                "*조회 결과*\n• 개수: *2개*\n"
                "• 파일: <https://example.com/file|열기>"
            ),
            "**조회 결과**\n• 개수: **2개**\n"
            "• 파일: [열기](https://example.com/file)",
        )
        self.assertEqual(
            slack_mrkdwn_to_commonmark(
                "```\n10:00 *fatal* <https://internal/x|raw>\n```"
            ),
            "```\n10:00 *fatal* <https://internal/x|raw>\n```",
        )

    def test_contracts_have_only_channel_neutral_fields(self) -> None:
        self.assertEqual(
            [field.name for field in fields(CompanyAssistantRequest)],
            [
                "request_id",
                "tenant_id",
                "actor_id",
                "channel",
                "conversation_id",
                "question",
                "locale",
                "context_entries",
                "metadata",
            ],
        )
        self.assertEqual(
            [field.name for field in fields(CompanyAssistantResult)],
            [
                "route",
                "outcome",
                "messages",
                "sources",
                "used_llm",
                "fallback_reason",
                "suggested_action",
                "async_job",
            ],
        )

    def test_routes_keep_order_and_stop_at_first_terminal_result(self) -> None:
        for outcome in (
            "answered",
            "no_evidence",
            "needs_input",
            "denied",
            "failed",
        ):
            with self.subTest(outcome=outcome):
                calls: list[str] = []
                terminal = CompanyAssistantResult(
                    route="second",
                    outcome=outcome,  # type: ignore[arg-type]
                    messages=(AssistantMessage(body="응답"),),
                )
                service = CompanyAssistantService(
                    (
                        _FakeRoute("first", calls, None),
                        _FakeRoute("second", calls, terminal),
                        _FakeRoute("third", calls, None),
                    )
                )

                self.assertIs(service.answer(_request()), terminal)
                self.assertEqual(calls, ["first", "second"])
                self.assertEqual(
                    service.route_names,
                    ("first", "second", "third"),
                )

    def test_recordings_context_memoizes_success_and_attaches_safe_rows(self) -> None:
        calls: list[str] = []
        loaded = {
            "summary": {"recordingCount": 1},
            "limit": 30,
            "has_more": False,
            "rows": [
                {
                    "seq": 1,
                    "hospitalName": "병원",
                    "deviceSeq": 7,
                    "privateColumn": "제외",
                }
            ],
        }
        scope = RequestScopedRecordingsContext(
            barcode="12345678910",
            loader=lambda barcode: calls.append(barcode) or loaded,
        )

        self.assertIs(scope.prefetch(), loaded)
        self.assertIs(scope.get(), loaded)
        evidence: dict = {}
        scope.attach_to_evidence(evidence, scope.get())

        self.assertEqual(calls, ["12345678910"])
        self.assertTrue(scope.has_device_mapping(loaded))
        self.assertNotIn("privateColumn", evidence["recordingsRows"][0])
        self.assertEqual(evidence["recordingsSummary"], {"recordingCount": 1})

    def test_recordings_context_memoizes_the_same_failure(self) -> None:
        calls: list[str] = []
        expected = RuntimeError("db unavailable")

        def fail(barcode: str):
            calls.append(barcode)
            raise expected

        scope = RequestScopedRecordingsContext(
            barcode="12345678910",
            loader=fail,
        )
        raised: list[Exception] = []
        for _ in range(2):
            with self.assertRaises(RuntimeError) as captured:
                scope.get()
            raised.append(captured.exception)

        self.assertEqual(calls, ["12345678910"])
        self.assertIs(raised[0], expected)
        self.assertIs(raised[1], expected)

    def test_recordings_context_rejects_mismatched_barcode_before_cached_value(self) -> None:
        calls: list[str] = []
        scope = RequestScopedRecordingsContext(
            barcode="12345678910",
            loader=lambda barcode: calls.append(barcode) or {"rows": []},
        )
        self.assertEqual(
            scope.get(requested_barcode="12345678910"),
            {"rows": []},
        )

        # 이미 채운 캐시도 다른 바코드 요청에는 반환하면 안 된다.
        with self.assertRaises(ValueError):
            scope.get(requested_barcode="10987654321")

        self.assertEqual(calls, ["12345678910"])


class CompanyNotionAssistantRouteTests(unittest.TestCase):
    def _route(
        self,
        *,
        answer_result: AnswerResult | None = None,
        provider: str = "claude",
        allowed: bool = True,
        configured: bool = True,
        query: str = "Commerce",
        search_results: list[CompanyNotionSearchResult] | None = None,
        references: list[dict] | None = None,
        synthesis_enabled: bool = True,
        provider_ready: bool = True,
    ) -> tuple[CompanyNotionAssistantRoute, SimpleNamespace]:
        result = CompanyNotionSearchResult(
            object_id="PAGE-1",
            object_type="page",
            title="Commerce",
            url="https://app.notion.com/p/commerce",
            last_edited_time="",
        )
        engine = _FakeAnswerEngine(
            answer_result
            or AnswerResult(
                text="Commerce는 커머스 사업을 담당해.",
                provider=provider,
                used_llm=True,
            ),
            provider=provider,
        )
        search_calls: list[str] = []
        deps = CompanyNotionAssistantRouteDeps(
            answer_engine=engine,  # type: ignore[arg-type]
            synthesis_enabled=synthesis_enabled,
            provider_ready=lambda: provider_ready,
            actor_allowed_for_llm=lambda actor_id: True,
            looks_like_search=lambda question: "회사 노션" in question,
            is_search_allowed=lambda actor_id: allowed,
            is_search_configured=lambda: configured,
            extract_query=lambda question: query,
            search=lambda target: search_calls.append(target)
            or (search_results if search_results is not None else [result]),
            load_references=lambda target, results: (
                references
                if references is not None
                else [
                    {
                        "title": "Commerce",
                        "url": result.url,
                        "excerpts": ["Commerce 근거"],
                    }
                ]
            ),
        )
        return (
            CompanyNotionAssistantRoute(deps),
            SimpleNamespace(engine=engine, search_calls=search_calls),
        )

    def test_unrelated_and_denied_requests_are_terminal_without_search(self) -> None:
        route, state = self._route(allowed=False)

        self.assertIsNone(route.handle(_request("일반 질문")))
        denied = route.handle(_request("회사 노션에서 Commerce 찾아줘"))

        self.assertIsNotNone(denied)
        self.assertEqual(denied.outcome, "denied")
        self.assertEqual(denied.route, "company_notion_search")
        self.assertEqual(state.search_calls, [])

    def test_missing_query_and_no_result_return_structured_outcomes(self) -> None:
        missing_route, _ = self._route(query="")
        missing = missing_route.handle(_request("회사 노션 조회해줘"))
        self.assertEqual(missing.outcome, "needs_input")
        self.assertEqual(missing.fallback_reason, "missing_query")

        empty_route, _ = self._route(search_results=[])
        empty = empty_route.handle(_request("회사 노션에서 Unknown 찾아줘"))
        self.assertEqual(empty.outcome, "no_evidence")
        self.assertEqual(empty.fallback_reason, "no_search_results")
        self.assertEqual(empty.sources, ())

    def test_answer_uses_only_notion_evidence_and_preserves_source(self) -> None:
        route, state = self._route()

        result = route.handle(_request("회사 노션에서 Commerce 찾아줘"))

        self.assertEqual(result.outcome, "answered")
        self.assertTrue(result.used_llm)
        self.assertEqual(
            result.sources,
            (
                SourceReference(
                    source_id="https://app.notion.com/p/commerce",
                    title="Commerce",
                    uri="https://app.notion.com/p/commerce",
                ),
            ),
        )
        self.assertEqual(len(state.engine.requests), 1)
        self.assertEqual(state.engine.requests[0].context_entries, ())
        self.assertEqual(
            state.engine.requests[0].evidence["route"],
            "company_notion_qa",
        )

    def test_timeout_and_unavailable_provider_keep_safe_source_fallback(self) -> None:
        timeout_route, _ = self._route(
            answer_result=AnswerResult(
                text="",
                provider="claude",
                used_llm=False,
                failure_reason="timeout",
            )
        )
        timeout = timeout_route.handle(
            _request("회사 노션에서 Commerce 찾아줘")
        )
        self.assertEqual(timeout.outcome, "no_evidence")
        self.assertEqual(timeout.fallback_reason, "timeout")
        self.assertEqual(len(timeout.sources), 1)
        self.assertNotIn("timeout", timeout.messages[0].body.lower())

        unavailable_route, state = self._route(provider_ready=False)
        unavailable = unavailable_route.handle(
            _request("회사 노션에서 Commerce 찾아줘")
        )
        self.assertEqual(unavailable.fallback_reason, "provider_unavailable")
        self.assertEqual(state.engine.requests, [])

    def test_generated_internal_context_leak_is_blocked_without_sources(self) -> None:
        route, _ = self._route(
            answer_result=AnswerResult(
                text="답변이야. thread context: 비공개 대화 전체",
                provider="claude",
                used_llm=True,
            )
        )

        result = route.handle(
            _request("회사 노션에서 Commerce 찾아줘")
        )

        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "unsafe_generated_answer",
        )
        self.assertIn("보안 위반 시도", result.messages[0].body)
        self.assertNotIn("비공개 대화", result.messages[0].body)
        self.assertEqual(result.sources, ())


class StructuredAssistantRouteTests(unittest.TestCase):
    def test_hospital_room_query_returns_channel_neutral_result(self) -> None:
        route = StructuredAssistantRoute(
            is_weekly_report_request=lambda *args, **kwargs: False,
        )
        with patch(
            "boxer_company.assistant.structured_route."
            "_query_hospital_rooms_by_filters",
            return_value="*병실 조회*\n• 서울병원 병실 2개",
        ) as query:
            result = route.handle(_request("병원명 서울병원 병실 목록"))

        self.assertEqual(result.route, "hospital_rooms_filter")
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(
            result.messages[0].body,
            "**병실 조회**\n• 서울병원 병실 2개",
        )
        query.assert_called_once_with(
            hospital_name="서울병원",
            room_name=None,
            hospital_seq=None,
            hospital_room_seq=None,
            count_only=False,
        )

    def test_weekly_report_and_restore_stay_in_slack_adapter(self) -> None:
        weekly = StructuredAssistantRoute(
            is_weekly_report_request=lambda *args, **kwargs: True,
        )
        self.assertIsNone(
            weekly.handle(_request("이번 주 영상 현황 리포트"))
        )

        restore = StructuredAssistantRoute(
            is_weekly_report_request=lambda *args, **kwargs: False,
        )
        self.assertIsNone(
            restore.handle(_request("35033165423 2024년 4월 영상 복원"))
        )

    def test_dependency_failure_returns_safe_result_without_exception_text(self) -> None:
        route = StructuredAssistantRoute(
            is_weekly_report_request=lambda *args, **kwargs: False,
        )
        with patch(
            "boxer_company.assistant.structured_route."
            "_query_hospital_rooms_by_filters",
            side_effect=RuntimeError("secret db endpoint"),
        ):
            result = route.handle(_request("병원명 서울병원 병실 목록"))

        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "dependency_error")
        self.assertNotIn("secret", result.messages[0].body)

    def test_mismatched_barcode_scope_is_denied_before_query(self) -> None:
        route = StructuredAssistantRoute()
        request = CompanyAssistantRequest(
            request_id="REQ-SCOPE",
            tenant_id="TENANT-1",
            actor_id="ACTOR-1",
            channel="test",
            conversation_id="CONVERSATION-1",
            question="10987654321 2026-07-01 영상 조회",
            locale="ko",
            metadata={"barcode": "12345678910"},
        )
        with patch(
            "boxer_company.assistant.structured_route."
            "_query_recordings_by_filters"
        ) as query:
            result = route.handle(request)

        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "barcode_scope_mismatch",
        )
        self.assertNotIn("12345678910", result.messages[0].body)
        self.assertNotIn("10987654321", result.messages[0].body)
        query.assert_not_called()


class BarcodeQueryAssistantRouteTests(unittest.TestCase):
    def _route(
        self,
        *,
        answer_result: AnswerResult | None = None,
    ) -> tuple[BarcodeQueryAssistantRoute, list[str], _FakeAnswerEngine | None]:
        calls: list[str] = []
        scope = RequestScopedRecordingsContext(
            barcode="12345678910",
            loader=lambda barcode: calls.append(barcode)
            or {
                "summary": {"recordingCount": 0},
                "rows": [],
                "limit": 30,
                "has_more": False,
            },
        )
        if answer_result is None:
            return BarcodeQueryAssistantRoute(scope), calls, None
        engine = _FakeAnswerEngine(answer_result)
        composer = CompanyEvidenceAnswerComposer(
            CompanyEvidenceAnswerComposerDeps(
                answer_engine=engine,  # type: ignore[arg-type]
                synthesis_enabled=True,
                provider_ready=lambda: True,
                actor_allowed_for_llm=lambda actor_id: True,
            )
        )
        return (
            BarcodeQueryAssistantRoute(
                scope,
                answer_composer=composer,
                timeout_message="합성 시간 초과",
            ),
            calls,
            engine,
        )

    def test_video_count_reuses_request_scoped_recordings_context(self) -> None:
        route, load_calls, _ = self._route()
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_recordings_count_by_barcode",
            return_value="*영상 개수*\n• 총 0개",
        ) as query:
            first = route.handle(_request("12345678910 영상 개수"))
            second = route.handle(_request("12345678910 영상 개수"))

        self.assertEqual(first.route, "barcode_video_count")
        self.assertEqual(first.outcome, "answered")
        self.assertEqual(first.messages[0].body, "**영상 개수**\n• 총 0개")
        self.assertEqual(second, first)
        self.assertEqual(load_calls, ["12345678910"])
        self.assertEqual(query.call_count, 2)

    def test_barcode_scope_mismatch_is_denied_without_cache_or_query(self) -> None:
        route, load_calls, _ = self._route()
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_recordings_count_by_barcode",
        ) as query:
            result = route.handle(_request("10987654321 영상 개수"))

        self.assertEqual(result.route, "barcode_scope_guard")
        self.assertEqual(result.outcome, "denied")
        self.assertEqual(result.fallback_reason, "barcode_scope_mismatch")
        self.assertNotIn("12345678910", result.messages[0].body)
        self.assertNotIn("10987654321", result.messages[0].body)
        self.assertEqual(load_calls, [])
        query.assert_not_called()

    def test_metadata_and_question_barcode_mismatch_is_denied(self) -> None:
        route, load_calls, _ = self._route()
        request = CompanyAssistantRequest(
            request_id="REQ-1",
            tenant_id="TENANT-1",
            actor_id="ACTOR-1",
            channel="test",
            conversation_id="CONVERSATION-1",
            question="10987654321 영상 개수",
            locale="ko",
            metadata={"barcode": "12345678910"},
        )

        result = route.handle(request)

        self.assertEqual(result.outcome, "denied")
        self.assertEqual(result.fallback_reason, "barcode_scope_mismatch")
        self.assertEqual(load_calls, [])

    def test_video_count_converts_slack_link_to_commonmark(self) -> None:
        route, _, _ = self._route()
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_recordings_count_by_barcode",
            return_value=(
                "*영상 개수*\n"
                "<https://example.invalid/recordings?id=1|녹화 목록>에서 확인해"
            ),
        ):
            result = route.handle(_request("12345678910 영상 개수"))

        self.assertEqual(
            result.messages[0].body,
            "**영상 개수**\n"
            "[녹화 목록](https://example.invalid/recordings?id=1)에서 확인해",
        )

    def test_missing_barcode_is_needs_input_and_mutation_is_delegated(self) -> None:
        route, _, _ = self._route()

        missing = route.handle(_request("베이비매직 목록"))
        self.assertEqual(missing.outcome, "needs_input")
        self.assertEqual(missing.fallback_reason, "missing_barcode")

        self.assertIsNone(
            route.handle(_request("12345678910 2024년 4월 영상 복원"))
        )

    def test_last_recorded_at_uses_shared_composer_and_safe_fallback(self) -> None:
        route, load_calls, engine = self._route(
            answer_result=AnswerResult(
                text="마지막 녹화는 2026-07-01이야",
                provider="claude",
                used_llm=True,
            )
        )
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_last_recorded_at_by_barcode",
            return_value="*마지막 녹화*\n• 2026-07-01",
        ):
            result = route.handle(_request("12345678910 마지막 녹화 날짜"))

        self.assertEqual(result.route, "barcode last recordedAt")
        self.assertEqual(result.outcome, "answered")
        self.assertTrue(result.used_llm)
        self.assertEqual(load_calls, ["12345678910"])
        self.assertEqual(len(engine.requests), 1)
        self.assertEqual(
            engine.requests[0].evidence["recordingsSummary"],
            {"recordingCount": 0},
        )

        unsafe_route, _, _ = self._route(
            answer_result=AnswerResult(
                text="다른 바코드로 확인해",
                provider="claude",
                used_llm=True,
            )
        )
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_last_recorded_at_by_barcode",
            return_value="*마지막 녹화*\n• 2026-07-01",
        ):
            unsafe = unsafe_route.handle(
                _request("12345678910 마지막 녹화 날짜")
            )

        self.assertFalse(unsafe.used_llm)
        self.assertEqual(
            unsafe.fallback_reason,
            "answer_validation_failed",
        )
        self.assertEqual(
            unsafe.messages[0].body,
            "**마지막 녹화**\n• 2026-07-01",
        )

    def test_recordings_failure_returns_safe_dependency_result(self) -> None:
        logger = logging.Logger("test.barcode.assistant")
        logger.disabled = True
        scope = RequestScopedRecordingsContext(
            barcode="12345678910",
            loader=lambda barcode: (_ for _ in ()).throw(
                RuntimeError("secret db endpoint")
            ),
        )
        route = BarcodeQueryAssistantRoute(scope, logger=logger)

        result = route.handle(_request("12345678910 영상 개수"))

        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "dependency_error")
        self.assertNotIn("secret", result.messages[0].body)


if __name__ == "__main__":
    unittest.main()
