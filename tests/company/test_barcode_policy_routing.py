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
        ):
            with self.subTest(question=question):
                self.assertFalse(looks_like_barcode_policy_question(question))

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
            for question in (_POLICY_QUESTION, _POLICY_BODY):
                with self.subTest(question=question):
                    result = service.answer(_request(question))
                    self.assertIsNotNone(result)
                    assert result is not None
                    self.assertEqual(result.route, "notion_playbook_qa")
                    self.assertEqual(result.outcome, "answered")
                    self.assertFalse(result.used_llm)
                    self.assertEqual(result.fallback_reason, "synthesis_disabled")
                    body = result.messages[0].body
                    self.assertNotIn("날짜 형식", body)
                    for token in ("삭제", "이전", "초기화"):
                        self.assertIn(token, body)
            remote_notion.assert_not_called()
            fetch_notion.assert_not_called()

        self.assertEqual(
            [route_group for _, route_group in api_client.calls],
            ["knowledge", "knowledge"],
        )
        engine.answer.assert_not_called()
        recordings_loader.assert_not_called()


if __name__ == "__main__":
    unittest.main()
