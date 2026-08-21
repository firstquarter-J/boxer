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
from boxer_company.assistant.barcode_query_route import (
    COMMON_API_BARCODE_QUERY_ROUTES,
    is_safe_baby_magic_source_uri,
    match_barcode_timeline_route,
    match_common_api_barcode_query_route,
)
from boxer_company.assistant.structured_route import (
    match_structured_read_route,
)
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
                "operation_result",
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
            looks_like_search=lambda question: "회사 노션" in question,
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

    def test_unrelated_and_unconfigured_requests_do_not_search(self) -> None:
        route, state = self._route(configured=False)

        self.assertIsNone(route.handle(_request("일반 질문")))
        unavailable = route.handle(
            _request("회사 노션에서 Commerce 찾아줘")
        )

        self.assertIsNotNone(unavailable)
        self.assertEqual(unavailable.outcome, "failed")
        self.assertEqual(unavailable.route, "company_notion_search")
        self.assertEqual(unavailable.fallback_reason, "not_configured")
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
    def test_pure_classifier_identifies_all_structured_read_routes_in_priority_order(
        self,
    ) -> None:
        cases = (
            ("2026년 병원 목록", "hospitals_filter"),
            (
                "병원명 서울병원 병실 목록",
                "hospital_rooms_filter",
            ),
            ("MB2-C00419 장비 정보", "devices_filter"),
            (
                # 캡처와 영상 힌트가 함께 있어도 기존 순서상 캡처가 먼저다.
                "12345678910 2026-07-01 초음파 캡처 영상 조회",
                "ultrasound_captures_filter",
            ),
            (
                "12345678910 2026-07-01 영상 조회",
                "recordings_filter",
            ),
        )

        for question, expected_route in cases:
            with self.subTest(route=expected_route):
                self.assertEqual(
                    match_structured_read_route(_request(question)),
                    expected_route,
                )

    def test_pure_classifier_excludes_adapter_only_mutation_and_invalid_scope(
        self,
    ) -> None:
        # 주간 리포트와 복원은 Slack 전용이고 일반 질문은 공통 API 전환
        # 후보가 아니므로 조회 실행 전 classifier에서 제외한다.
        for question in (
            "이번 주 영상 현황 리포트",
            "35033165423 2024년 4월 영상 복원",
            # 전용 barcode stage가 정규화된 날짜 목록 형식으로 답한다.
            "12345678910 전체 녹화 날짜",
            "오늘 점심 뭐 먹지?",
        ):
            with self.subTest(question=question):
                self.assertIsNone(
                    match_structured_read_route(_request(question))
                )

        mismatched = CompanyAssistantRequest(
            request_id="REQ-CLASSIFIER-SCOPE",
            tenant_id="TENANT-1",
            actor_id="ACTOR-1",
            channel="test",
            conversation_id="CONVERSATION-1",
            question="10987654321 2026-07-01 영상 조회",
            locale="ko",
            metadata={"barcode": "12345678910"},
        )
        self.assertIsNone(match_structured_read_route(mismatched))

    def test_timeline_questions_defer_but_general_date_queries_stay_structured(
        self,
    ) -> None:
        timeline_cases = (
            ("12345678910 마지막 녹화 날짜", "barcode last recordedAt"),
            ("12345678910 최신 영상은?", "barcode last recordedAt"),
            (
                "12345678910 2026-08-04 녹화됐어?",
                "barcode recordedAt-on-date",
            ),
            (
                "12345678910 어제 영상 있어?",
                "barcode recordedAt-on-date",
            ),
        )
        for question, expected in timeline_cases:
            with self.subTest(question=question):
                request = _request(question)
                self.assertEqual(
                    match_barcode_timeline_route(request),
                    expected,
                )
                self.assertIsNone(match_structured_read_route(request))

        # 날짜만으로는 timeline 존재 질문이 아니다. 목록·조회·개수는
        # 기존 structured route가 전체 DB 범위를 그대로 처리한다.
        for question in (
            "12345678910 2026-08-04 영상 조회",
            "12345678910 2026-08-04 영상 목록",
            "12345678910 2026-08-04 영상 몇 개야?",
        ):
            with self.subTest(question=question):
                request = _request(question)
                self.assertIsNone(match_barcode_timeline_route(request))
                self.assertEqual(
                    match_structured_read_route(request),
                    "recordings_filter",
                )

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

    def test_hospital_count_question_does_not_use_question_as_name(self) -> None:
        route = StructuredAssistantRoute(
            is_weekly_report_request=lambda *args, **kwargs: False,
        )
        with patch(
            "boxer_company.assistant.structured_route."
            "_query_hospitals_by_filters",
            return_value="*병원 조회 결과*\n• hospitals row 수: *0개*",
        ) as query:
            result = route.handle(_request("2099년 병원 몇 개야?"))

        self.assertEqual(result.route, "hospitals_filter")
        self.assertEqual(result.outcome, "answered")
        # 수량 질의 문구가 hospital_name으로 전달되지 않아야 한다.
        query.assert_called_once_with(
            hospital_name=None,
            hospital_seq=None,
            target_date=None,
            target_year=2099,
            count_only=True,
        )

    def test_device_query_remains_enabled_for_local_runtime(
        self,
    ) -> None:
        route = StructuredAssistantRoute(
            is_weekly_report_request=lambda *args, **kwargs: False,
        )
        with patch(
            "boxer_company.assistant.structured_route."
            "_query_devices_by_filters",
            return_value="*장비 조회 결과*\n• MB2-C00419",
        ) as query:
            result = route.handle(_request("MB2-C00419 장비 정보"))

        self.assertEqual(result.route, "devices_filter")
        self.assertEqual(result.outcome, "answered")
        query.assert_called_once_with(
            device_name="MB2-C00419",
            device_seq=None,
            hospital_name=None,
            room_name=None,
            hospital_seq=None,
            hospital_room_seq=None,
            status=None,
            active_flag=None,
            install_flag=None,
            count_only=False,
            include_live_enrichment=True,
        )

    def test_multiple_device_names_keep_legacy_first_device_query(self) -> None:
        # 기존 parser가 선택한 첫 장비로 그대로 조회한다.
        route = StructuredAssistantRoute(
            is_weekly_report_request=lambda *args, **kwargs: False,
        )
        with patch(
            "boxer_company.assistant.structured_route."
            "_query_devices_by_filters",
            return_value="*장비 조회 결과*\n• MB2-C00419",
        ) as query:
            result = route.handle(
                _request("MB2-C00419와 MB2-C00999 장비 정보")
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "devices_filter")
        self.assertEqual(result.outcome, "answered")
        query.assert_called_once_with(
            device_name="MB2-C00419",
            device_seq=None,
            hospital_name=None,
            room_name=None,
            hospital_seq=None,
            hospital_room_seq=None,
            status=None,
            active_flag=None,
            install_flag=None,
            count_only=False,
            include_live_enrichment=True,
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

    def test_multiple_question_barcodes_keep_legacy_first_barcode_query(self) -> None:
        route, load_calls, _ = self._route()
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_recordings_count_by_barcode",
            return_value="*영상 개수*\n• 총 0개",
        ) as query:
            result = route.handle(
                _request("12345678910 10987654321 영상 개수")
            )

        self.assertEqual(result.route, "barcode_video_count")
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(load_calls, ["12345678910"])
        query.assert_called_once()
        self.assertEqual(query.call_args.args[0], "12345678910")

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

    def test_common_api_matcher_includes_db_only_residual_routes(self) -> None:
        cases = (
            ("베이비매직 목록", "baby_ai_list"),
            ("12345678910 베이비매직 목록", "barcode_baby_ai_list"),
        )

        # Slack 표현을 분류할 뿐 DB/MDA/LLM은 호출하지 않는 공통 계약이다.
        for question, expected_route in cases:
            with self.subTest(route=expected_route):
                self.assertIn(
                    expected_route,
                    COMMON_API_BARCODE_QUERY_ROUTES,
                )
                self.assertEqual(
                    match_common_api_barcode_query_route(
                        _request(question)
                    ),
                    expected_route,
                )

    def test_common_api_matcher_excludes_mda_mutation_and_precedence_routes(
        self,
    ) -> None:
        for question in (
            "10255657857 이건 유효성 검사에 걸리는 바코드냐",
            "58291583958 왜 핑크바코드로 분류되지 않았어?",
            "12345678910 2024년 4월 영상 복원",
            "12345678910 마지막 녹화 날짜",
            "12345678910 2026-08-04 녹화됐어?",
        ):
            with self.subTest(question=question):
                self.assertIsNone(
                    match_common_api_barcode_query_route(
                        _request(question)
                    )
                )

    def test_baby_ai_list_is_channel_neutral_db_result(self) -> None:
        route, load_calls, _ = self._route()
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_baby_ai_list_by_barcode",
            return_value=(
                "*베이비매직 목록*\n"
                "• 결과: "
                "<https://cdn-kr.mmtalkbox.com/result.jpg|열기>"
            ),
        ) as query:
            result = route.handle(
                _request("12345678910 2026-08-04 베이비매직 목록")
            )

        self.assertEqual(result.route, "barcode_baby_ai_list")
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(
            result.messages[0].body,
            "**베이비매직 목록**\n"
            "• 결과: "
            "[열기](https://cdn-kr.mmtalkbox.com/result.jpg)",
        )
        self.assertEqual(len(result.sources), 1)
        self.assertEqual(
            result.sources[0].uri,
            "https://cdn-kr.mmtalkbox.com/result.jpg",
        )
        self.assertEqual(load_calls, [])
        query.assert_called_once_with("12345678910", "2026-08-04")

    def test_baby_ai_sources_ignore_env_host_and_sensitive_urls(self) -> None:
        route, _, _ = self._route()
        unsafe_urls = (
            "https://example.invalid/result.jpg",
            "https://cdn-kr.mmtalkbox.com/result.jpg?token=x",
            "https://cdn-kr.mmtalkbox.com/result.jpg#token",
            "https://user@cdn-kr.mmtalkbox.com/result.jpg",
        )
        for unsafe_url in unsafe_urls:
            with self.subTest(unsafe_url=unsafe_url), patch(
                "boxer_company.assistant.barcode_query_route."
                "cs.BABY_MAGIC_CDN_BASE_URL",
                "https://example.invalid",
            ), patch(
                "boxer_company.assistant.barcode_query_route."
                "_query_baby_ai_list_by_barcode",
                return_value=f"• 결과: <{unsafe_url}|열기>",
            ):
                result = route.handle(
                    _request("12345678910 베이비매직 목록")
                )

            self.assertEqual(result.outcome, "answered")
            self.assertEqual(result.sources, ())
            self.assertNotIn(unsafe_url, result.messages[0].body)
            self.assertIn(
                "열기 [링크 생략]",
                result.messages[0].body,
            )

        # Transport validator는 renderer의 2차 필터에 기대지 않고
        # parser 혼동 문자와 제어문자를 source 계약에서 직접 거부한다.
        for unsafe_url in (
            "https://cdn-kr.mmtalkbox.com/x<y.jpg",
            "https://cdn-kr.mmtalkbox.com/x>y.jpg",
            "https://cdn-kr.mmtalkbox.com/x|y.jpg",
            "https://cdn-kr.mmtalkbox.com/x\\y.jpg",
            "https://cdn-kr.mmtalkbox.com/x\x00y.jpg",
        ):
            with self.subTest(unsafe_transport_url=unsafe_url):
                self.assertFalse(
                    is_safe_baby_magic_source_uri(unsafe_url)
                )

    def test_recorded_on_date_uses_db_context_and_shared_composer(self) -> None:
        route, load_calls, engine = self._route(
            answer_result=AnswerResult(
                text="해당 날짜에 녹화 1건이 있어",
                provider="claude",
                used_llm=True,
            )
        )
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_recordings_on_date_by_barcode",
            return_value="*날짜별 녹화 여부*\n• 1개",
        ) as query:
            result = route.handle(
                _request("12345678910 2026-08-04 녹화됐어?")
            )

        self.assertEqual(result.route, "barcode recordedAt-on-date")
        self.assertEqual(result.outcome, "answered")
        self.assertTrue(result.used_llm)
        self.assertEqual(load_calls, ["12345678910"])
        self.assertEqual(len(engine.requests), 1)
        self.assertEqual(
            engine.requests[0].evidence["request"]["targetDate"],
            "2026-08-04",
        )
        query.assert_called_once()
        self.assertEqual(
            query.call_args.args[:2],
            ("12345678910", "2026-08-04"),
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
