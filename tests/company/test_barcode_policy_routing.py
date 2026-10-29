from __future__ import annotations

import logging
import unittest
from unittest.mock import Mock, patch

from boxer_company._operation_routing_knowledge import (
    looks_like_barcode_policy_question,
    match_notion_playbook_route,
)
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerComposerDeps,
)
from boxer_company.assistant.contracts import (
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.knowledge_routes import (
    NotionPlaybookQAAssistantRoute,
    NotionPlaybookQARouteDeps,
)
from boxer_company.assistant.runtime import (
    CompanyAssistantRuntime,
    CompanyAssistantRuntimeDeps,
)
from boxer_company.notion_playbooks import _LOCAL_PLAYBOOKS, _select_notion_references
from boxer_company.operation_routing import match_company_operation_route
from boxer_company.read_routing import (
    _build_structured_query_match,
    match_barcode_query_route,
    match_structured_read_route,
)
from boxer_company_adapter_slack.company_api_rollout import (
    CompanyPlaybookApiRolloutService,
    CompanyStructuredApiRolloutService,
)

_POLICY_BODY = (
    "1. 1번병원에서 영상을 찍었다가 삭제를 함\n"
    "2. 몇개월 이 지난 후 다른 산모님에게 이 바코드가 지급됨\n"
    "3. 첫영상이 삭제된게 25년10월\n"
    "4. 현시점 1년이 지남 바코드사용 불가 상태\n"
    "5. 첫영상을 다른곳으로 이전을 하면 현재 바코드가 사용이 가능해질가요? 안될가요??"
)
_POLICY_QUESTION = f"바코드 정책 질의응답\n{_POLICY_BODY}"
_POLICY_PAGE_ID = "local-playbook:barcode-expiry-recording-transfer-policy"
_POLICY_VARIANTS = (
    _POLICY_QUESTION,
    _POLICY_BODY,
    "바코드 첫 영상을 삭제하면 사용할 수 있어?",
    "바코드 첫 영상을 이전하면 사용 가능해?",
    "바코드 사용기간 알려줘",
    "바코드 첫 영상 이전하면 만료가 초기화돼?",
    "12345678910 바코드 사용 기간 지나면 차단돼?",
    "12345678910 바코드 첫 녹화 삭제해도 무료 바코드 사용 가능해?",
)
_BLOCKING_POLICY_VARIANTS = (
    "바코드 차단 정책 알려줘",
    "바코드 차단은 어떻게 동작해? 예시로 설명해줘",
    "1년 차단과 핑크 차단 차이 알려줘",
    "핑크바코드 차단은 처음부터 막는 거야?",
    "핑크 기간차단은 설정 전 사용자도 적용돼?",
    "핑크바코드기간차단 설명해줘",
    "핑크기간차단 어떻게 동작해?",
    "1년차단 설명해줘",
    "핑크바코드 차단과 기간 차단을 같이 켜면 어떻게 돼?",
    "12345678910 핑크바코드를 10월 1일 처음 쓰고 10월 29일 기간 차단을 설정하면 언제부터 막혀?",
)
_BLOCKING_POLICY_TITLE = "바코드 차단 정책: 1년 차단, 핑크 차단, 핑크 기간 차단"


def _request(question: str) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="slack:T1:C1:barcode-policy",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="C1",
        question=question,
        locale="ko",
    )


class _RuntimeApiClient:
    def __init__(self, runtime: CompanyAssistantRuntime) -> None:
        self.runtime = runtime
        self.calls: list[tuple[CompanyAssistantRequest, str | None]] = []

    def answer(
        self,
        request: CompanyAssistantRequest,
        *,
        route_group: str | None = None,
    ) -> CompanyAssistantResult | None:
        self.calls.append((request, route_group))
        # HTTP 전송만 대체하고, API의 실제 knowledge route 실행은 유지한다.
        if route_group != "knowledge":
            raise AssertionError("바코드 정책은 knowledge stage로만 전달해야 해")
        return self.runtime.answer_stage(request, "knowledge")


class BarcodePolicyRoutingTests(unittest.TestCase):
    def test_case_question_bypasses_structured_date_parser(self) -> None:
        # 사례 속 월과 경과 기간은 DB 날짜 필터가 아니며 제목 없이도 같아야 한다.
        for question in _POLICY_VARIANTS:
            with self.subTest(question=question):
                request = _request(question)
                self.assertTrue(looks_like_barcode_policy_question(question))
                self.assertIsNone(_build_structured_query_match(request))
                self.assertIsNone(match_structured_read_route(request))
                self.assertIsNone(match_barcode_query_route(request))
                self.assertIsNone(match_company_operation_route(request))
                self.assertEqual(
                    match_notion_playbook_route(request),
                    "notion_playbook_qa",
                )

    def test_blocking_policy_names_and_dated_examples_use_knowledge_route(self) -> None:
        # 정책명만 있는 질문과 날짜/번호를 예시로 쓴 질문 모두 실제 조회를 실행하지 않는다.
        for question in _BLOCKING_POLICY_VARIANTS:
            with self.subTest(question=question):
                request = _request(question)
                self.assertTrue(looks_like_barcode_policy_question(question))
                self.assertIsNone(_build_structured_query_match(request))
                self.assertIsNone(match_structured_read_route(request))
                self.assertIsNone(match_barcode_query_route(request))
                self.assertIsNone(match_company_operation_route(request))
                self.assertEqual(match_notion_playbook_route(request), "notion_playbook_qa")

    def test_actual_recording_queries_keep_structured_route(self) -> None:
        for question, count_only in (
            ("서울병원 2026-10-25 영상 목록 보여줘", False),
            ("서울병원 2026-10-25 영상 몇 개야?", True),
        ):
            with self.subTest(question=question):
                request = _request(question)
                self.assertFalse(looks_like_barcode_policy_question(question))
                match = _build_structured_query_match(request)
                self.assertIsNotNone(match)
                assert match is not None
                self.assertEqual(match.route, "recordings_filter")
                self.assertEqual(match.count_only, count_only)
                self.assertIsNone(match_notion_playbook_route(request))

    def test_barcode_status_and_mutation_requests_are_not_policy_cases(self) -> None:
        # 번호가 있는 실데이터 검증과 실행 지시를 새 정책 matcher가 선점하지 않는다.
        for status_question in (
            "12345678910 바코드 유효성 검사 상태 확인해줘",
            "12345678910 바코드 유효기간 지난 상태야? 현재 차단 대상인지 확인해줘",
            "12345678910 바코드 첫 영상 삭제했는데 현재 사용 가능한지 차단 여부 확인해줘",
            "12345678910 핑크바코드 기간차단 대상인지 현재 상태 확인해줘",
        ):
            with self.subTest(question=status_question):
                self.assertFalse(looks_like_barcode_policy_question(status_question))
                self.assertEqual(
                    match_barcode_query_route(_request(status_question)),
                    "barcode_validation_status",
                )
                self.assertEqual(
                    match_company_operation_route(_request(status_question)),
                    "barcode_validation_status",
                )
        for question in (
            "12345678910 바코드 첫영상을 삭제해줘",
            "12345678910 바코드 첫영상을 다른 곳으로 이전해줘",
            "바코드 만료 정책을 초기화해줘",
            "바코드 차단 설정 바꿔줘",
            "핑크바코드 기간차단 켜줄래?",
            "핑크바코드 기간차단 켜줘",
            "핑크 차단 꺼주세요",
            "바코드 차단 정책을 변경해줘",
            "핑크바코드 기간차단 60일로 설정해줘",
            "핑크바코드 차단 적용해줘",
            "12345678910 바코드 차단해줘",
        ):
            with self.subTest(question=question):
                self.assertFalse(looks_like_barcode_policy_question(question))

    def test_device_settings_and_actual_barcode_logs_are_not_policy_questions(self) -> None:
        # 장비에 지금 저장된 설정과 실제 차단 로그는 정책 설명으로 대신 답하지 않는다.
        for question in (
            "MB2-X00001 현재 핑크바코드 차단 설정 확인해줘",
            "MB2-X00001 핑크 기간차단 상태 조회해줘",
            "12345678910 바코드 2026-10-29 차단 로그 분석해줘",
            "12345678910 핑크바코드 차단돼?",
            "MB2-X00001 바코드 차단 로그 분석해줘",
            "바코드 차단 로그 확인해줘",
            "MB2-X00001 핑크바코드 차단 설정 알려줘",
            "12345678910 바코드 차단 로그 어떻게 나왔어?",
            "12345678910 바코드 차단 로그 정리해줘",
        ):
            with self.subTest(question=question):
                self.assertFalse(looks_like_barcode_policy_question(question))

    def test_blocking_policy_reference_contains_examples_without_refund(self) -> None:
        # 일반 차단 정책은 오래된 유효성 검사 일괄 설명 대신 세 정책을 근거로 삼는다.
        with patch(
            "boxer_company.notion_playbooks._is_company_notion_configured",
            return_value=False,
        ):
            for question in _BLOCKING_POLICY_VARIANTS:
                with self.subTest(question=question):
                    references = _select_notion_references(question)
                    self.assertTrue(references)
                    self.assertEqual(references[0]["title"], _BLOCKING_POLICY_TITLE)
                    preview = "\n".join(references[0]["previewLines"])
                    for token in ("1년 차단", "핑크 차단", "핑크 기간 차단", "12개월", "60일", "11월 30일"):
                        self.assertIn(token, preview)
                    self.assertNotIn("환불", preview)
                    self.assertNotIn("REFUND", preview)

    def test_local_policy_reference_keeps_conclusion_and_expiry_basis(self) -> None:
        policy = next(
            entry for entry in _LOCAL_PLAYBOOKS if entry["pageId"] == _POLICY_PAGE_ID
        )
        with patch(
            "boxer_company.notion_playbooks._is_company_notion_configured",
            return_value=False,
        ):
            for question in _POLICY_VARIANTS:
                with self.subTest(question=question):
                    references = _select_notion_references(question)
                    self.assertTrue(references)
                    self.assertEqual(references[0]["title"], policy["title"])
                    previews = "\n".join(references[0]["previewLines"])
                    for token in ("삭제", "이전", "초기화"):
                        self.assertIn(token, references[0]["previewLines"][0])
                    self.assertTrue(
                        "registeredAt" in previews or "최초 병원 등록일" in previews
                    )
                    self.assertIn("장비", previews)
                    self.assertIn("차단", previews)
                    self.assertIn("설정", previews)

            # 이전 절차를 찾는 문장에 사용 기간 정책을 대신 내보내지 않는다.
            references = _select_notion_references("바코드 영상 이전 방법 알려줘")
            self.assertNotIn(policy["title"], [item["title"] for item in references])

    def test_slack_chain_sends_policy_to_api_knowledge_and_answers_without_providers(
        self,
    ) -> None:
        # Slack의 앞선 structured wrapper부터 API 실제 route까지 연결해
        # 날짜 오류가 다시 선점하거나 외부 provider가 필요한 회귀를 막는다.
        logger = logging.getLogger("test.barcode-policy")
        logger.disabled = True
        engine = Mock(provider="claude")
        engine.answer.side_effect = AssertionError("LLM을 호출하면 안 돼")
        recordings_loader = Mock(side_effect=AssertionError("DB 조회를 하면 안 돼"))
        composer = CompanyEvidenceAnswerComposer(
            CompanyEvidenceAnswerComposerDeps(
                answer_engine=engine,
                synthesis_enabled=False,
                provider_ready=lambda: False,
            ),
            logger=logger,
        )
        runtime = CompanyAssistantRuntime(
            CompanyAssistantRuntimeDeps(
                answer_engine=engine,
                provider_ready=lambda: False,
                get_s3_client=Mock(side_effect=AssertionError("S3를 호출하면 안 돼")),
                synthesis_enabled=False,
                recordings_loader=recordings_loader,
                s3_query_enabled=lambda: False,
                db_configured=lambda: False,
            ),
            knowledge_routes=(
                NotionPlaybookQAAssistantRoute(
                    NotionPlaybookQARouteDeps(answer_composer=composer),
                    logger=logger,
                ),
            ),
            logger=logger,
        )
        api_client = _RuntimeApiClient(runtime)
        playbook = CompanyPlaybookApiRolloutService(
            None,
            api_client=api_client,  # type: ignore[arg-type]
            logger=logger,
        )
        service = CompanyStructuredApiRolloutService(
            playbook,
            api_client=api_client,  # type: ignore[arg-type]
            logger=logger,
        )

        with (
            patch(
                "boxer_company.notion_playbooks._is_company_notion_configured",
                return_value=False,
            ),
            patch(
                "boxer_company.notion_playbooks._select_notion_playbooks"
            ) as remote_notion,
            patch(
                "boxer_company.notion_playbooks._fetch_all_notion_blocks"
            ) as fetch_notion,
        ):
            for question in (_POLICY_QUESTION, _POLICY_BODY, *_BLOCKING_POLICY_VARIANTS):
                with self.subTest(question=question):
                    result = service.answer(_request(question))
                    self.assertIsNotNone(result)
                    assert result is not None
                    self.assertEqual(result.route, "notion_playbook_qa")
                    self.assertEqual(result.outcome, "answered")
                    self.assertFalse(result.used_llm)
                    self.assertEqual(
                        result.fallback_reason,
                        None if question in _BLOCKING_POLICY_VARIANTS else "synthesis_disabled",
                    )
                    body = result.messages[0].body
                    self.assertNotIn("날짜 형식", body)
                    if question in _BLOCKING_POLICY_VARIANTS:
                        for token in ("1년 차단", "핑크 차단", "핑크 기간 차단", "12개월", "60일", "11월 30일", "기존"):
                            self.assertIn(token, body)
                        self.assertTrue("하나라도" in body or "함께" in body or "같이" in body)
                        self.assertNotIn("환불", body)
                        self.assertNotIn("REFUND", body)
                    else:
                        for token in ("삭제", "이전", "초기화"):
                            self.assertIn(token, body)
            remote_notion.assert_not_called()
            fetch_notion.assert_not_called()

        self.assertEqual(
            [route_group for _, route_group in api_client.calls],
            ["knowledge"] * (2 + len(_BLOCKING_POLICY_VARIANTS)),
        )
        engine.answer.assert_not_called()
        recordings_loader.assert_not_called()

    def test_blocking_policy_keeps_examples_and_scope_without_generated_rewrite(self) -> None:
        # 합성이 가능한 운영 환경에서도 확정된 정책과 설명 제외 범위를 그대로 지킨다.
        composer = Mock()
        composer.compose.side_effect = AssertionError("정책을 LLM으로 재작성하면 안 돼")
        route = NotionPlaybookQAAssistantRoute(
            NotionPlaybookQARouteDeps(answer_composer=composer),
        )
        with patch("boxer_company.notion_playbooks._is_company_notion_configured", return_value=True):
            result = route.handle(_request("마미박스 바코드 차단 정책 설명해줘"))

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        self.assertFalse(result.used_llm)
        body = result.messages[0].body
        for token in ("2026년 10월 1일", "11월 30일", "첫 영상 서버 등록", "다음 스캔", "핑크 차단은 끄고", "기존 영상 조회"):
            self.assertIn(token, body)
        self.assertNotIn("환불", body)
        self.assertNotIn("REFUND", body)
        self.assertNotIn("·", body)
        composer.compose.assert_not_called()


if __name__ == "__main__":
    unittest.main()
