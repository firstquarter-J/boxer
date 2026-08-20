from __future__ import annotations

from dataclasses import replace
import unittest
from unittest.mock import Mock

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_operations_route import (
    DeviceOperationsAssistantRoute,
    DeviceOperationsRouteDeps,
    match_device_operation_route,
)


def _request(
    question: str,
    *,
    route_group: str | None = "operations",
    metadata: dict[str, str] | None = None,
    context_entries: tuple[dict[str, str], ...] = (),
) -> CompanyAssistantRequest:
    request_metadata = dict(metadata or {})
    if route_group is not None:
        request_metadata["route_group"] = route_group
    return CompanyAssistantRequest(
        request_id="REQ-DEVICE-OP-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="THREAD-1",
        question=question,
        locale="ko",
        context_entries=context_entries,  # type: ignore[arg-type]
        metadata=request_metadata,
    )


class DeviceOperationsMatcherTests(unittest.TestCase):
    def test_matcher_classifies_supported_operations(self) -> None:
        expected = {
            "지원 음성 종류 알려줘": "device_voice_catalog",
            "MB2-C00419 귀여운 음성으로 바꿔줘": "device_voice_change",
            "MB2-C00419 진단 시작": "device_diagnostic_snapshot",
            "MB2-C00419 최근 종료 원인 알려줘": "device_diagnostic_analysis",
            "MB2-C00419 업데이트 상태": "device_update_status",
            "MB2-C00419 박스 업데이트": "device_box_update",
            "MB2-C00419 에이전트 업데이트": "device_agent_update",
            "MB2-C00419 장비 종료": "device_power_off",
            "MB2-C00419 장비 소리 출력 점검": "device_audio_probe",
            "MB2-C00419 원격 접속 ping 확인": "device_remote_access_probe",
            "MB2-C00419 메모리 패치": "device_memory_patch",
            "MB2-C00419 pm2 상태": "device_pm2_probe",
            "MB2-C00419 캡처보드 상태": "device_captureboard_probe",
            "MB2-C00419 LED 상태": "device_led_probe",
            "MB2-C00419 장비 상태": "device_status_probe",
        }

        for question, route in expected.items():
            with self.subTest(question=question):
                self.assertEqual(
                    match_device_operation_route(_request(question)),
                    route,
                )

    def test_matcher_requires_operations_route_group(self) -> None:
        for route_group in (None, "device", "device_detail"):
            with self.subTest(route_group=route_group):
                self.assertIsNone(
                    match_device_operation_route(
                        _request(
                            "MB2-C00419 장비 상태",
                            route_group=route_group,
                        )
                    )
                )

    def test_matcher_rejects_missing_ambiguous_and_mismatched_device(self) -> None:
        requests = (
            _request("장비 상태"),
            _request(
                "장비 업데이트",
                metadata={"device_name": "MB2-C00419"},
            ),
            _request(
                "귀여운 음성으로 바꿔줘",
                metadata={"device_name": "MB2-C00419"},
            ),
            _request("MB2-C00419 MB2-C00999 장비 상태"),
            _request(
                "MB2-C00419 장비 상태",
                metadata={"device_name": "MB2-C00999"},
            ),
        )

        for request in requests:
            with self.subTest(question=request.question, metadata=request.metadata):
                self.assertIsNone(match_device_operation_route(request))

    def test_voice_catalog_is_the_only_targetless_operation(self) -> None:
        self.assertEqual(
            match_device_operation_route(_request("지원 음성 종류 알려줘")),
            "device_voice_catalog",
        )
        self.assertIsNone(
            match_device_operation_route(_request("귀여운 음성으로 바꿔줘"))
        )

    def test_read_only_led_questions_are_not_claimed_as_live_probe(self) -> None:
        for question in (
            "MB2-C00570 2026-07-04 LED 로그 확인",
            "MB2-C00419 LED 패턴 의미 알려줘",
        ):
            with self.subTest(question=question):
                self.assertIsNone(
                    match_device_operation_route(_request(question))
                )

    def test_power_off_requires_an_explicit_execution_command(self) -> None:
        self.assertEqual(
            match_device_operation_route(
                _request("MB2-C00419 전원 꺼진 이유 분석해줘")
            ),
            "device_diagnostic_analysis",
        )
        self.assertEqual(
            match_device_operation_route(
                _request("MB2-C00419 장비 종료해")
            ),
            "device_power_off",
        )

    def test_diagnostic_followup_requires_prior_start_context(self) -> None:
        question = "최근 종료 원인"
        self.assertIsNone(match_device_operation_route(_request(question)))
        self.assertEqual(
            match_device_operation_route(
                _request(
                    question,
                    context_entries=(
                        {
                            "kind": "message",
                            "source": "slack",
                            "author_id": "ACTOR-1",
                            "text": "MB2-C00419 진단 시작",
                        },
                    ),
                )
            ),
            "device_diagnostic_followup",
        )
        self.assertIsNone(
            match_device_operation_route(
                _request(
                    question,
                    context_entries=(
                        {
                            "kind": "message",
                            "source": "slack",
                            "author_id": "OTHER-ACTOR",
                            "text": "MB2-C00419 진단 시작",
                        },
                    ),
                )
            )
        )


class DeviceOperationsRouteTests(unittest.TestCase):
    def test_mutation_question_does_not_call_domain_function(self) -> None:
        box_update = Mock()
        power_off = Mock()
        start_diagnostic = Mock()
        analyze_diagnostic = Mock()
        patch_memory = Mock()
        deps = replace(
            DeviceOperationsRouteDeps(),
            request_box_update=box_update,
            request_power_off=power_off,
            start_diagnostic=start_diagnostic,
            start_diagnostic_analysis=analyze_diagnostic,
            patch_pm2_memory=patch_memory,
        )
        result = DeviceOperationsAssistantRoute(
            deps
        ).handle(_request("MB2-C00419 박스 업데이트 방법 알려줘"))

        self.assertIsNone(result)
        for question in (
            "MB2-C00419 전원 꺼도 돼",
            "MB2-C00419 진단 시작해도 돼",
            "MB2-C00419 메모리 패치 괜찮아",
        ):
            with self.subTest(question=question):
                self.assertIsNone(
                    DeviceOperationsAssistantRoute(deps).handle(
                        _request(question)
                    )
                )
        box_update.assert_not_called()
        power_off.assert_not_called()
        start_diagnostic.assert_not_called()
        analyze_diagnostic.assert_not_called()
        patch_memory.assert_not_called()

    def test_catalog_returns_commonmark_and_calls_builder_once(self) -> None:
        build_catalog = Mock(return_value="*장비 음성 목록*\n• 음성: *귀여운 음성*")
        deps = replace(
            DeviceOperationsRouteDeps(),
            build_voice_catalog=build_catalog,
        )

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request("지원 음성 종류 알려줘")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "device_voice_catalog")
        self.assertEqual(result.outcome, "answered")
        self.assertIn("**장비 음성 목록**", result.messages[0].body)
        self.assertIn("**귀여운 음성**", result.messages[0].body)
        build_catalog.assert_called_once_with()

    def test_voice_change_dispatches_once_with_fixed_dependency(self) -> None:
        change_voice = Mock(return_value=("*장비 음성 변경 완료*", {}))
        dispatcher = Mock()
        deps = replace(
            DeviceOperationsRouteDeps(),
            change_voice=change_voice,
            send_mda_command=dispatcher,
        )

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request("MB2-C00419 귀여운 음성으로 바꿔줘")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        change_voice.assert_called_once_with(
            "MB2-C00419",
            "귀여운 음성",
            command_dispatcher=dispatcher,
        )

    def test_diagnostic_uses_channel_neutral_request_scope_once(self) -> None:
        start_diagnostic = Mock(return_value=("*장비 진단 시작*", {}))
        deps = replace(
            DeviceOperationsRouteDeps(),
            start_diagnostic=start_diagnostic,
        )

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request(
                "MB2-C00419 진단 시작",
                metadata={"channel_id": "CHANNEL-1"},
            )
        )

        self.assertIsNotNone(result)
        start_diagnostic.assert_called_once_with(
            device_name="MB2-C00419",
            question="MB2-C00419 진단 시작",
            workspace_id="TENANT-1",
            channel_id="CHANNEL-1",
            thread_ts="THREAD-1",
            requested_by="ACTOR-1",
            resend_ssh_open=False,
        )

    def test_diagnostic_followup_reuses_api_process_snapshot_once(self) -> None:
        snapshot = {
            "request": {
                "deviceName": "MB2-C00419",
                "requestedBy": "ACTOR-1",
            }
        }
        evidence = {
            **snapshot,
            "followupLiveCheck": {"performed": True},
        }
        load_snapshot = Mock(return_value=snapshot)
        build_evidence = Mock(return_value=evidence)
        build_fallback = Mock(return_value="*장비 진단 답변*")
        deps = replace(
            DeviceOperationsRouteDeps(),
            load_diagnostic_snapshot=load_snapshot,
            build_diagnostic_followup_evidence=build_evidence,
            build_diagnostic_followup_fallback=build_fallback,
        )
        request = _request(
            "최근 종료 원인",
            metadata={"channel_id": "CHANNEL-1"},
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "ACTOR-1",
                    "text": "MB2-C00419 진단 시작",
                },
            ),
        )

        result = DeviceOperationsAssistantRoute(deps).handle(request)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "device_diagnostic_followup")
        self.assertEqual(result.outcome, "answered")
        self.assertIn("**장비 진단 답변**", result.messages[0].body)
        load_snapshot.assert_called_once_with(
            workspace_id="TENANT-1",
            channel_id="CHANNEL-1",
            thread_ts="THREAD-1",
        )
        build_evidence.assert_called_once_with(
            "최근 종료 원인",
            snapshot,
            resend_ssh_open=False,
        )
        build_fallback.assert_called_once_with(
            "최근 종료 원인",
            evidence,
        )

    def test_contextless_diagnostic_analysis_runs_once_and_saves_snapshot(self) -> None:
        analyze = Mock(return_value=("*장비 진단 답변*", {"saved": True}))
        deps = replace(
            DeviceOperationsRouteDeps(),
            start_diagnostic_analysis=analyze,
        )
        request = _request(
            "MB2-C00419 최근 종료 원인 알려줘",
            metadata={"channel_id": "CHANNEL-1"},
        )

        result = DeviceOperationsAssistantRoute(deps).handle(request)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "device_diagnostic_analysis")
        self.assertEqual(result.outcome, "answered")
        self.assertIn("**장비 진단 답변**", result.messages[0].body)
        analyze.assert_called_once_with(
            question="MB2-C00419 최근 종료 원인 알려줘",
            device_name="MB2-C00419",
            workspace_id="TENANT-1",
            channel_id="CHANNEL-1",
            thread_ts="THREAD-1",
            requested_by="ACTOR-1",
            resend_ssh_open=False,
        )

    def test_diagnostic_followup_rejects_explicit_snapshot_device_mismatch(
        self,
    ) -> None:
        load_snapshot = Mock(
            return_value={
                "request": {
                    "deviceName": "MB2-C00419",
                    "requestedBy": "ACTOR-1",
                }
            }
        )
        build_evidence = Mock()
        deps = replace(
            DeviceOperationsRouteDeps(),
            load_diagnostic_snapshot=load_snapshot,
            build_diagnostic_followup_evidence=build_evidence,
        )
        request = _request(
            "MB2-C00570 최근 종료 원인",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "ACTOR-1",
                    "text": "MB2-C00419 진단 시작",
                },
            ),
        )

        result = DeviceOperationsAssistantRoute(deps).handle(request)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(result.fallback_reason, "invalid_request")
        load_snapshot.assert_called_once()
        build_evidence.assert_not_called()

    def test_diagnostic_followup_rejects_snapshot_from_another_actor(
        self,
    ) -> None:
        load_snapshot = Mock(
            return_value={
                "request": {
                    "deviceName": "MB2-C00419",
                    "requestedBy": "OTHER-ACTOR",
                }
            }
        )
        build_evidence = Mock()
        deps = replace(
            DeviceOperationsRouteDeps(),
            load_diagnostic_snapshot=load_snapshot,
            build_diagnostic_followup_evidence=build_evidence,
        )
        request = _request(
            "최근 종료 원인",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "ACTOR-1",
                    "text": "MB2-C00419 진단 시작",
                },
            ),
        )

        result = DeviceOperationsAssistantRoute(deps).handle(request)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "needs_input")
        build_evidence.assert_not_called()

    def test_simple_operations_call_exactly_one_domain_function(self) -> None:
        cases = (
            (
                "MB2-C00419 업데이트 상태",
                "query_update_status",
                (("MB2-C00419",), {"resend_ssh_open": False}),
            ),
            (
                "MB2-C00419 박스 업데이트",
                "request_box_update",
                (
                    ("MB2-C00419 박스 업데이트",),
                    {
                        "device_name": "MB2-C00419",
                        "resend_ssh_open": False,
                    },
                ),
            ),
            (
                "MB2-C00419 에이전트 업데이트",
                "request_agent_update",
                (
                    ("MB2-C00419 에이전트 업데이트",),
                    {
                        "device_name": "MB2-C00419",
                        "resend_ssh_open": False,
                    },
                ),
            ),
            (
                "MB2-C00419 장비 종료",
                "request_power_off",
                (
                    ("MB2-C00419 장비 종료",),
                    {
                        "device_name": "MB2-C00419",
                        "resend_ssh_open": False,
                    },
                ),
            ),
            (
                "MB2-C00419 장비 소리 출력 점검",
                "probe_audio",
                (("MB2-C00419",), {"resend_ssh_open": False}),
            ),
            (
                "MB2-C00419 원격 접속 ping 확인",
                "probe_remote_access",
                (("MB2-C00419",), {}),
            ),
            (
                "MB2-C00419 메모리 패치",
                "patch_pm2_memory",
                (("MB2-C00419",), {"resend_ssh_open": False}),
            ),
            (
                "MB2-C00419 장비 상태",
                "probe_status",
                (
                    ("MB2-C00419",),
                    {
                        "resend_ssh_open": False,
                        "allow_force_reopen": False,
                    },
                ),
            ),
        )

        for question, field_name, expected_call in cases:
            with self.subTest(question=question):
                operation = Mock(return_value=("*최종 결과*", {}))
                deps = replace(
                    DeviceOperationsRouteDeps(),
                    **{field_name: operation},
                )
                result = DeviceOperationsAssistantRoute(deps).handle(
                    _request(question)
                )

                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(result.outcome, "answered")
                self.assertEqual(operation.call_count, 1)
                operation.assert_called_once_with(
                    *expected_call[0],
                    **expected_call[1],
                )

    def test_component_probes_keep_the_fixed_component(self) -> None:
        expected = {
            "MB2-C00419 pm2 상태": "pm2",
            "MB2-C00419 캡처보드 상태": "captureboard",
            "MB2-C00419 LED 상태": "led",
        }

        for question, component in expected.items():
            with self.subTest(question=question):
                probe = Mock(return_value=("*점검 결과*", {}))
                deps = replace(
                    DeviceOperationsRouteDeps(),
                    probe_runtime_component=probe,
                )
                result = DeviceOperationsAssistantRoute(deps).handle(
                    _request(question)
                )

                self.assertIsNotNone(result)
                probe.assert_called_once_with(
                    "MB2-C00419",
                    component=component,
                    resend_ssh_open=False,
                    allow_force_reopen=False,
                )

    def test_mutation_failure_is_not_retried_or_exposed(self) -> None:
        update = Mock(side_effect=RuntimeError("secret-mda-token"))
        deps = replace(
            DeviceOperationsRouteDeps(),
            request_box_update=update,
        )

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request("MB2-C00419 박스 업데이트")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "device_box_update")
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "operation_error")
        self.assertNotIn("secret-mda-token", result.messages[0].body)
        update.assert_called_once_with(
            "MB2-C00419 박스 업데이트",
            device_name="MB2-C00419",
            resend_ssh_open=False,
        )

    def test_invalid_request_exception_detail_is_not_exposed(self) -> None:
        status = Mock(side_effect=ValueError("secret-validation-detail"))
        deps = replace(
            DeviceOperationsRouteDeps(),
            query_update_status=status,
        )

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request("MB2-C00419 업데이트 상태")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "needs_input")
        self.assertNotIn("secret-validation-detail", result.messages[0].body)
        status.assert_called_once_with(
            "MB2-C00419",
            resend_ssh_open=False,
        )

    def test_route_has_no_progress_callback_contract(self) -> None:
        route = DeviceOperationsAssistantRoute()

        self.assertFalse(hasattr(route, "handle_with_progress"))


if __name__ == "__main__":
    unittest.main()
