from __future__ import annotations

import unittest
from unittest.mock import patch

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.factory import (
    _guard_read_only_request,
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
                "MB2-C00419 PM2 로그 확인해줘",
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

    def test_read_only_guard_preserves_supported_s3_log_routes(
        self,
    ) -> None:
        # 장비명과 로그/실패 단어가 있어도 날짜 지정 DB/S3 조회는
        # live 진단으로 오분류하지 않고 각 read-only route에 넘긴다.
        for question in (
            "MB2-C00570 2026-08-04 LED 로그 확인",
            f"{_BARCODE} MB2-C00570 2026-08-04 로그 분석",
            (
                f"{_BARCODE} MB2-C00570 2026-08-04 "
                "녹화 실패 원인 분석"
            ),
        ):
            with self.subTest(question=question):
                self.assertIsNone(
                    _guard_read_only_request(_request(question))
                )

    def test_runtime_reaches_supported_s3_log_routes_after_guard(
        self,
    ) -> None:
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.S3_QUERY_ENABLED",
                False,
            ),
            patch(
                "boxer_company.assistant.factory.core_settings.DB_HOST",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_USERNAME",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_PASSWORD",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_DATABASE",
                "",
            ),
        ):
            runtime = create_company_assistant_runtime()
            for question, expected_route in (
                (
                    "MB2-C00570 2026-08-04 LED 로그 확인",
                    "device_led_log_analysis",
                ),
                (
                    f"{_BARCODE} MB2-C00570 2026-08-04 로그 분석",
                    "barcode_log_analysis",
                ),
                (
                    (
                        f"{_BARCODE} MB2-C00570 2026-08-04 "
                        "녹화 실패 원인 분석"
                    ),
                    "recording_failure_analysis",
                ),
            ):
                with self.subTest(question=question):
                    result = runtime.answer(_request(question))

                    self.assertIsNotNone(result)
                    self.assertEqual(result.route, expected_route)
                    self.assertEqual(result.outcome, "failed")

    def test_read_only_guard_keeps_explicit_live_start_denied(
        self,
    ) -> None:
        result = _guard_read_only_request(
            _request(
                "MB2-C00570 2026-08-04 LED 로그 확인 후 진단 시작"
            )
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.route, "unsupported_live_diagnostic")
        self.assertEqual(result.outcome, "denied")

    def test_device_detail_query_uses_db_without_live_enrichment(
        self,
    ) -> None:
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ),
            patch(
                "boxer_company.assistant.structured_route."
                "_query_devices_by_filters",
                return_value="*장비 조회 결과*\n• MB2-C00419",
            ) as device_query,
        ):
            runtime = create_company_assistant_runtime()
            result = runtime.answer(
                _request("MB2-C00419 장비 정보")
            )

        self.assertIsNotNone(result)
        self.assertEqual(result.route, "devices_filter")
        self.assertEqual(result.outcome, "answered")
        device_query.assert_called_once_with(
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
            include_live_enrichment=False,
        )

    def test_api_factory_disables_live_enrichment_for_all_log_routes(
        self,
    ) -> None:
        with patch(
            "boxer_company.assistant.factory."
            "core_settings.LLM_PROVIDER",
            "",
        ):
            turn = create_company_assistant_runtime().start_turn(
                _request(f"{_BARCODE} 2026-08-04 로그 분석")
            )

        # API factory의 두 로그 경로가 MDA sshOrder·장비 SSH를 열 수 없게
        # 같은 fail-closed 설정을 공유하는지 조립 경계에서 고정한다.
        failure_route = turn.routes_for_stage("failure")[0]
        barcode_log_route = turn.routes_for_stage("log")[0]
        self.assertFalse(failure_route._live_enrichment_enabled)
        self.assertFalse(barcode_log_route._live_enrichment_enabled)
        self.assertTrue(failure_route._explicit_date_required)
        self.assertTrue(barcode_log_route._explicit_date_required)

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
