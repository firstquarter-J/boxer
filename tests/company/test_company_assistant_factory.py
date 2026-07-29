from __future__ import annotations

import unittest
from unittest.mock import patch

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.factory import (
    CompanyAssistantRuntimePolicy,
    create_company_assistant_runtime,
)
from boxer_company.assistant.freeform_prompt import (
    build_company_freeform_system_prompt,
)


_BARCODE = "12345678910"


def _request(question: str) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-FACTORY-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
    )


class CompanyAssistantRuntimeFactoryTests(unittest.TestCase):
    def test_factory_builds_complete_read_only_route_order(self) -> None:
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "claude",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_SYNTHESIS_ENABLED",
                True,
            ),
            patch(
                "boxer_company.assistant.factory."
                "_build_claude_client",
                return_value=object(),
            ),
        ):
            runtime = create_company_assistant_runtime()

        turn = runtime.start_turn(_request("일반 질문"))

        self.assertEqual(
            turn.route_names,
            (
                "company_notion",
                "device_led_log_analysis",
                "device_led_pattern_guide",
                "recording_failure_analysis",
                "barcode_log_analysis",
                "structured",
                "barcode_query",
                "device_diagnostic_followup",
                "notion_playbook_qa",
                "barcode_evidence_freeform",
            ),
        )

    def test_separate_process_snapshot_is_unavailable_by_default(
        self,
    ) -> None:
        with patch(
            "boxer_company.assistant.factory."
            "core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime()

        turn = runtime.start_turn(_request("왜 반복 재시작해?"))
        diagnostic_route = turn.routes_for_stage("knowledge")[0]

        # 별도 API 프로세스는 Slack의 메모리 snapshot을 읽지 않는다.
        self.assertIsNone(diagnostic_route.handle(turn.request))

    def test_factory_wires_company_notion_actor_policy(self) -> None:
        policy = CompanyAssistantRuntimePolicy(
            company_notion_search_allowed=lambda actor_id: False,
        )
        with patch(
            "boxer_company.assistant.factory."
            "core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime(policy=policy)

        result = runtime.answer(
            _request("회사 노션에서 커머스 찾아줘")
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.route, "company_notion_search")
        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "actor_not_allowed",
        )

    def test_barcode_policy_denies_before_recordings_lookup(self) -> None:
        policy = CompanyAssistantRuntimePolicy(
            barcode_evidence_allowed=lambda request: False,
        )
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "claude",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_SYNTHESIS_ENABLED",
                True,
            ),
            patch(
                "boxer_company.assistant.factory."
                "_build_claude_client",
                return_value=object(),
            ),
            patch(
                "boxer_company.assistant.factory."
                "_load_recordings_context_by_barcode",
            ) as recordings_loader,
        ):
            runtime = create_company_assistant_runtime(policy=policy)
            result = runtime.answer(
                _request(f"{_BARCODE} 이 근거를 요약해줘")
            )

        self.assertIsNotNone(result)
        self.assertEqual(result.route, "barcode_evidence_freeform")
        self.assertEqual(result.outcome, "denied")
        self.assertEqual(result.fallback_reason, "actor_not_allowed")
        recordings_loader.assert_not_called()

    def test_live_device_diagnostic_is_not_absorbed_by_freeform(
        self,
    ) -> None:
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "claude",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_SYNTHESIS_ENABLED",
                True,
            ),
            patch(
                "boxer_company.assistant.factory."
                "_build_claude_client",
                return_value=object(),
            ),
            patch(
                "boxer_company.assistant.factory."
                "_load_recordings_context_by_barcode",
            ) as recordings_loader,
        ):
            runtime = create_company_assistant_runtime()
            for question in (
                "MB2-C00419 진단 시작",
                f"{_BARCODE} MB2-C00419 PM2 상태 진단해줘",
            ):
                with self.subTest(question=question):
                    result = runtime.answer(_request(question))

                    self.assertIsNotNone(result)
                    self.assertEqual(
                        result.route,
                        "unsupported_live_diagnostic",
                    )
                    self.assertEqual(result.outcome, "denied")
                    self.assertEqual(
                        result.fallback_reason,
                        "read_only_boundary",
                    )
        recordings_loader.assert_not_called()

    def test_ollama_health_result_is_cached_for_requests(self) -> None:
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "ollama",
            ),
            patch(
                "boxer_company.assistant.factory."
                "_check_ollama_health",
                return_value={"ok": True, "summary": "정상"},
            ) as check_health,
        ):
            runtime = create_company_assistant_runtime()

            self.assertTrue(runtime._deps.provider_ready())
            self.assertTrue(runtime._deps.provider_ready())

        check_health.assert_called_once_with()

    def test_freeform_prompt_keeps_channel_neutral_response_mode(
        self,
    ) -> None:
        prompt = build_company_freeform_system_prompt(
            "A vs B 중 누가 더 세?"
        )

        self.assertIsNotNone(prompt)
        self.assertIn(
            "결론 -> 이유 2~3개 -> 변수/예외 1개",
            prompt,
        )


if __name__ == "__main__":
    unittest.main()
