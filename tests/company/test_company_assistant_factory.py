from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.factory import (
    _guard_read_only_request,
    create_company_assistant_runtime,
)
from boxer_company.assistant.freeform_prompt import (
    build_company_freeform_system_prompt,
)


_BARCODE = "12345678910"


def _request(
    question: str,
    *,
    route_group: str | None = None,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-FACTORY-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
        metadata=(
            {"route_group": route_group}
            if route_group is not None
            else {}
        ),
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
                "weekly_recordings_summary",
                "device_detail",
                "device_db_detail",
                "structured",
                "barcode_query",
                "device_diagnostic_followup",
                "notion_playbook_qa",
                "barcode_evidence_freeform",
                "company_llm_health",
                "company_daily_fortune",
                "company_team_fun",
                "company_freeform",
                "operation_single_target_required",
                "operation_confirmation_required",
                "security_review",
                "device_health_alert_action",
                "device_file_operations",
                "app_user_baby_selection_analysis",
                "app_user_lookup",
                "recording_streaming_restore",
                "barcode_pink_classification_reason",
                "barcode_validation_status",
                "admin_s3_ultrasound",
                "admin_s3_device_log",
                "admin_request_log",
                "admin_readonly_sql",
                "device_operations",
                "thread_playbook_learning",
            ),
        )

    def test_factory_assembles_operations_behind_explicit_stage(
        self,
    ) -> None:
        with patch(
            "boxer_company.assistant.factory.core_settings.LLM_PROVIDER",
            "",
        ):
            turn = create_company_assistant_runtime().start_turn(
                _request("일반 질문", route_group="operations")
            )

        # operations capability는 HTTP 정책에서 검사하고 factory는
        # 구체 route를 전용 stage에만 조립한다.
        self.assertEqual(
            turn.service_for_stage("operations").route_names,
            (
                "operation_single_target_required",
                "operation_confirmation_required",
                "security_review",
                "device_health_alert_action",
                "device_file_operations",
                "app_user_baby_selection_analysis",
                "app_user_lookup",
                "recording_streaming_restore",
                "barcode_pink_classification_reason",
                "barcode_validation_status",
                "admin_s3_ultrasound",
                "admin_s3_device_log",
                "admin_request_log",
                "admin_readonly_sql",
                "device_operations",
                "thread_playbook_learning",
            ),
        )
        self.assertIsNone(_guard_read_only_request(turn.request))

    def test_factory_assembles_freeform_behind_explicit_stage(
        self,
    ) -> None:
        with patch(
            "boxer_company.assistant.factory.core_settings.LLM_PROVIDER",
            "",
        ):
            turn = create_company_assistant_runtime().start_turn(
                _request("오늘 기분 어때?", route_group="freeform")
            )

        # 채널 중립 provider route는 전용 final fallback stage에만 있다.
        self.assertEqual(
            turn.service_for_stage("freeform").route_names,
            (
                "company_llm_health",
                "company_daily_fortune",
                "company_team_fun",
                "company_freeform",
            ),
        )
        self.assertNotIn(
            "company_freeform",
            turn.service_for_stage("knowledge").route_names,
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

    def test_knowledge_stage_rechecks_freeform_rollout_boundary(self) -> None:
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
            # routeGroup은 권한이 아니라 실행 범위 힌트다. API runtime도
            # 일반 질문과 PII 의도를 자체 matcher로 다시 거부해야 한다.
            for question in (
                f"{_BARCODE} 오늘 기분 어때?",
                f"{_BARCODE} 산모 전화번호를 녹화 기록 근거로 알려줘",
                f"{_BARCODE} 녹화 기록을 수정해줘",
            ):
                with self.subTest(question=question):
                    result = runtime.answer_stage(
                        _request(question),
                        "knowledge",
                    )
                    self.assertIsNone(result)

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

    def test_read_only_guard_blocks_mda_barcode_routes_before_lookup(
        self,
    ) -> None:
        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_barcode_validation_status"
        ) as mda_query:
            with patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ):
                runtime = create_company_assistant_runtime()
                result = runtime.answer(
                    _request(
                        f"{_BARCODE} 유효성 검사 결과 알려줘"
                    )
                )

        self.assertIsNotNone(result)
        self.assertEqual(result.route, "unsupported_mda_lookup")
        self.assertEqual(result.outcome, "denied")
        self.assertEqual(result.fallback_reason, "read_only_boundary")
        mda_query.assert_not_called()

    def test_device_detail_query_uses_db_without_live_enrichment(
        self,
    ) -> None:
        with patch(
            "boxer_company.assistant.factory."
            "core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime()
            turn = runtime.start_turn(
                _request("MB2-C00419 장비 정보")
            )

        # factory가 실제로 주입한 API 전용 route의 DB 함수를 대체해
        # live enrichment=false 계약과 선행 순서를 함께 검증한다.
        device_query = Mock(
            return_value="*장비 조회 결과*\n• MB2-C00419"
        )
        device_route = turn.routes_for_stage("structured")[2]
        device_route._query_devices = device_query
        result = turn.answer()

        self.assertIsNotNone(result)
        self.assertEqual(result.route, "device_db_detail")
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

    def test_explicit_device_detail_stage_uses_live_enrichment(
        self,
    ) -> None:
        with patch(
            "boxer_company.assistant.factory."
            "core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime()
            turn = runtime.start_turn(
                _request(
                    "MB2-C00419 장비 정보",
                    route_group="device_detail",
                )
            )

        # full route가 기존 Slack 보강 흐름을 쓰되 poll 중 open을
        # 재전송하지 않는지와 CommonMark 계약을 함께 고정한다.
        device_query = Mock(
            return_value=(
                "*장비 조회 결과*\n"
                "• 장비명: `MB2-C00419`\n"
                "• 버전: `2.11.307`\n"
                "• SSH 연결 상태: 🔵 *연결 가능*"
            )
        )
        full_route = turn.routes_for_stage("structured")[1]
        full_route._query_devices = device_query
        result = turn.answer()

        self.assertIsNotNone(result)
        self.assertEqual(result.route, "device_detail")
        self.assertEqual(result.outcome, "answered")
        self.assertIn("버전: `2.11.307`", result.messages[0].body)
        self.assertTrue(
            device_query.call_args.kwargs["include_live_enrichment"]
        )
        self.assertFalse(
            device_query.call_args.kwargs["allow_ssh_open_resend"]
        )

    def test_api_factory_enables_bounded_undated_db_s3_log_routes(
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

        # 날짜 없는 요청은 route의 고정 phase1 window로 허용하되 API에서
        # MDA sshOrder·장비 SSH를 열 수 없는 설정을 조립 경계에 고정한다.
        failure_route = turn.routes_for_stage("failure")[0]
        barcode_log_route = turn.routes_for_stage("log")[0]
        self.assertFalse(failure_route._live_enrichment_enabled)
        self.assertFalse(barcode_log_route._live_enrichment_enabled)
        self.assertFalse(failure_route._explicit_date_required)
        self.assertFalse(barcode_log_route._explicit_date_required)

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
