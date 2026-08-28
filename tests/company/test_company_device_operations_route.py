from __future__ import annotations

from dataclasses import replace
from typing import Any
import unittest
from unittest.mock import ANY, Mock, patch

from boxer_company import settings as cs
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.device_operations_route import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_OPERATION_DELIVERY_ACTION,
    DeviceOperationsAssistantRoute,
    DeviceOperationsRouteDeps,
)
from boxer_company.operation_routing import match_device_operation_route


def _request(
    question: str,
    *,
    route_group: str | None = "operations",
    metadata: dict[str, Any] | None = None,
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
    def test_typed_followup_probe_does_not_require_bounded_start_context(
        self,
    ) -> None:
        request = _request("그럼 원인이 뭐야?")
        request = replace(
            request,
            metadata={
                **request.metadata,
                "operation_action": {
                    "name": DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
                },
            },
        )

        self.assertEqual(
            match_device_operation_route(request),
            "device_diagnostic_followup",
        )

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

    def test_matcher_uses_first_question_target_like_slack(self) -> None:
        # 장비가 필요한 legacy matcher는 metadata로 보강하지 않지만,
        # 본문에 여러 장비가 있으면 parser가 찾은 첫 장비로 route를 고른다.
        self.assertIsNone(match_device_operation_route(_request("장비 상태")))
        self.assertIsNone(
            match_device_operation_route(
                _request(
                    "장비 업데이트",
                    metadata={"device_name": "MB2-C00419"},
                )
            )
        )
        self.assertEqual(
            match_device_operation_route(
                _request("MB2-C00419 MB2-C00999 장비 상태")
            ),
            "device_status_probe",
        )
        self.assertEqual(
            match_device_operation_route(
                _request(
                    "MB2-C00419 장비 상태",
                    metadata={"device_name": "MB2-C00999"},
                )
            ),
            "device_status_probe",
        )

    def test_voice_change_route_returns_target_and_voice_input_guides(self) -> None:
        self.assertEqual(
            match_device_operation_route(_request("지원 음성 종류 알려줘")),
            "device_voice_catalog",
        )
        self.assertEqual(
            match_device_operation_route(_request("귀여운 음성으로 바꿔줘")),
            "device_voice_change",
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

    def test_power_off_keeps_legacy_question_style_precedence(self) -> None:
        for question in (
            "MB2-C00419 전원 꺼진 이유 분석해줘",
            "MB2-C00419 전원 꺼도 돼",
            "MB2-C00419 장비 종료해",
        ):
            with self.subTest(question=question):
                self.assertEqual(
                    match_device_operation_route(_request(question)),
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
        self.assertEqual(
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
            ),
            "device_diagnostic_followup",
        )


class DeviceOperationsRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        # route/domain 호출 테스트는 로컬 .env 유무와 분리하고, 설정 안내
        # 자체는 predicate를 명시적으로 false로 주입한 테스트에서 검증한다.
        configured = patch.multiple(
            cs,
            MDA_GRAPHQL_URL="https://mda.example/graphql",
            MDA_ADMIN_USER_PASSWORD="test-password",
            DEVICE_SSH_PASSWORD="test-password",
        )
        configured.start()
        self.addCleanup(configured.stop)

    def test_missing_runtime_returns_legacy_route_specific_config_guides(
        self,
    ) -> None:
        cases = {
            "MB2-C00419 귀여운 음성으로 바꿔줘": "장비 음성 변경을 위해",
            "MB2-C00419 진단 시작": "장비 진단 설정이 부족해",
            "MB2-C00419 박스 업데이트": "장비 업데이트 기능 설정이 부족해",
            "MB2-C00419 장비 종료": "장비 종료 기능 설정이 부족해",
            "MB2-C00419 장비 소리 출력 점검": "장비 소리 출력 점검 설정이 부족해",
            "MB2-C00419 원격 접속 ping 확인": "장비 원격 접속 점검 설정이 부족해",
            "MB2-C00419 메모리 패치": "장비 메모리 패치 설정이 부족해",
            "MB2-C00419 PM2 상태": "장비 상태 점검 설정이 부족해",
        }
        deps = replace(
            DeviceOperationsRouteDeps(),
            device_runtime_configured=lambda: False,
            mda_configured=lambda: False,
        )
        route = DeviceOperationsAssistantRoute(deps)

        for question, expected in cases.items():
            with self.subTest(question=question):
                result = route.handle(_request(question))
                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(result.outcome, "failed")
                self.assertEqual(
                    result.fallback_reason,
                    "device_runtime_not_configured",
                )
                self.assertIn(expected, result.messages[0].body)

    def test_question_style_mutations_keep_legacy_execution(self) -> None:
        box_update = Mock(return_value=("박스 업데이트", {}))
        power_off = Mock(return_value=("전원 종료", {}))
        start_diagnostic = Mock(return_value=("진단 시작", {}))
        patch_memory = Mock(return_value=("메모리 패치", {}))
        deps = replace(
            DeviceOperationsRouteDeps(),
            request_box_update=box_update,
            request_power_off=power_off,
            start_diagnostic=start_diagnostic,
            patch_pm2_memory=patch_memory,
        )
        route = DeviceOperationsAssistantRoute(deps)

        results = (
            route.handle(_request("MB2-C00419 박스 업데이트 방법 알려줘")),
            route.handle(_request("MB2-C00419 전원 꺼도 돼")),
            route.handle(_request("MB2-C00419 진단 시작해도 돼")),
            route.handle(_request("MB2-C00419 메모리 패치 괜찮아")),
        )

        self.assertTrue(all(result is not None for result in results))
        box_update.assert_called_once_with(
            "MB2-C00419 박스 업데이트 방법 알려줘",
            device_name="MB2-C00419",
            on_dispatched=ANY,
            resend_ssh_open=False,
        )
        power_off.assert_called_once_with(
            "MB2-C00419 전원 꺼도 돼",
            device_name="MB2-C00419",
            on_dispatched=ANY,
            resend_ssh_open=False,
        )
        start_diagnostic.assert_called_once()
        patch_memory.assert_called_once_with(
            "MB2-C00419",
            resend_ssh_open=False,
        )

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

    def test_diagnostic_followup_without_api_snapshot_returns_typed_no_match(
        self,
    ) -> None:
        load_snapshot = Mock(return_value=None)
        deps = replace(
            DeviceOperationsRouteDeps(),
            load_diagnostic_snapshot=load_snapshot,
        )
        request = _request(
            "휴가 규정 알려줘",
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
        self.assertEqual(result.outcome, "no_evidence")
        self.assertEqual(
            result.fallback_reason,
            "diagnostic_snapshot_missing",
        )
        load_snapshot.assert_called_once()

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

    def test_diagnostic_followup_reuses_snapshot_despite_device_text(
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
        evidence = {"request": {"deviceName": "MB2-C00419"}}
        build_evidence = Mock(return_value=evidence)
        build_fallback = Mock(return_value="진단 후속 답변")
        deps = replace(
            DeviceOperationsRouteDeps(),
            load_diagnostic_snapshot=load_snapshot,
            build_diagnostic_followup_evidence=build_evidence,
            build_diagnostic_followup_fallback=build_fallback,
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
        self.assertEqual(result.outcome, "answered")
        load_snapshot.assert_called_once()
        build_evidence.assert_called_once()
        build_fallback.assert_called_once_with(
            "MB2-C00570 최근 종료 원인",
            evidence,
        )

    def test_diagnostic_followup_reuses_snapshot_from_thread_actor(
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
        evidence = {"request": {"deviceName": "MB2-C00419"}}
        build_evidence = Mock(return_value=evidence)
        build_fallback = Mock(return_value="진단 후속 답변")
        deps = replace(
            DeviceOperationsRouteDeps(),
            load_diagnostic_snapshot=load_snapshot,
            build_diagnostic_followup_evidence=build_evidence,
            build_diagnostic_followup_fallback=build_fallback,
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
        self.assertEqual(result.outcome, "answered")
        build_evidence.assert_called_once()
        build_fallback.assert_called_once_with("최근 종료 원인", evidence)

    def test_multiple_device_probe_executes_first_parsed_target(self) -> None:
        # Slack의 search parser는 첫 번째 장비명을 domain 함수에 넘겼다.
        probe = Mock(return_value=("장비 상태", {}))
        deps = replace(DeviceOperationsRouteDeps(), probe_status=probe)

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request("MB2-C00419 MB2-C00999 장비 상태")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        probe.assert_called_once_with(
            "MB2-C00419",
            resend_ssh_open=False,
            allow_force_reopen=False,
        )

    def test_targetless_voice_and_diagnostic_return_legacy_guides(self) -> None:
        route = DeviceOperationsAssistantRoute()

        voice = route.handle(_request("귀여운 음성으로 바꿔줘"))
        diagnostic = route.handle(_request("진단 시작"))

        self.assertIsNotNone(voice)
        self.assertIsNotNone(diagnostic)
        assert voice is not None
        assert diagnostic is not None
        self.assertIn("음성을 바꿀 장비명이 필요해", voice.messages[0].body)
        self.assertIn("진단 시작은 장비명이 필요해", diagnostic.messages[0].body)

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
                        "on_dispatched": ANY,
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
                        "on_dispatched": ANY,
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
                        "on_dispatched": ANY,
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
            on_dispatched=ANY,
            resend_ssh_open=False,
        )

    def test_update_preserves_progress_order_and_logs_mda_activity(self) -> None:
        payload = {
            "route": "device_box_update",
            "dispatch": {"status": True},
        }

        def update(
            question: str,
            *,
            device_name: str,
            on_dispatched: object,
            resend_ssh_open: bool,
        ) -> tuple[str, dict[str, object]]:
            self.assertEqual(question, "MB2-C00419 박스 업데이트")
            self.assertEqual(device_name, "MB2-C00419")
            self.assertFalse(resend_ssh_open)
            assert callable(on_dispatched)
            on_dispatched("*박스 업데이트 요청 전송 완료*")
            return "*박스 업데이트 완료*", payload

        build_activity = Mock(return_value={"activityType": "device.edit"})
        create_activity = Mock(return_value={"status": True})
        deps = replace(
            DeviceOperationsRouteDeps(),
            request_box_update=Mock(side_effect=update),
            build_update_activity_input=build_activity,
            create_activity_log=create_activity,
        )

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request(
                "MB2-C00419 박스 업데이트",
                metadata={
                    "channel_id": "CHANNEL-1",
                    "actor_name": "홍 길동",
                },
            )
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(
            [message.body for message in result.messages],
            [
                "**박스 업데이트 요청 전송 완료**",
                "**박스 업데이트 완료**",
            ],
        )
        self.assertTrue(
            all(not message.mention_actor for message in result.messages)
        )
        build_activity.assert_called_once_with(
            question="MB2-C00419 박스 업데이트",
            user_id="ACTOR-1",
            user_name="홍 길동",
            channel_id="CHANNEL-1",
            thread_ts="THREAD-1",
            result_payload=payload,
        )
        create_activity.assert_called_once_with(
            {"activityType": "device.edit"}
        )

    def test_progressive_updates_emit_before_completion_and_defer_activity(
        self,
    ) -> None:
        cases = (
            (
                "MB2-C00419 박스 업데이트",
                "device_box_update",
                "request_box_update",
                "2.11.300",
            ),
            (
                "MB2-C00419 에이전트 업데이트",
                "device_agent_update",
                "request_agent_update",
                "latest",
            ),
            (
                "MB2-C00419 장비 종료",
                "device_power_off",
                "request_power_off",
                "",
            ),
        )
        for question, expected_route, dependency_name, version in cases:
            with self.subTest(route=expected_route):
                partials: list[CompanyAssistantResult] = []
                build_activity = Mock(return_value={"ok": True})
                create_activity = Mock(return_value={"status": True})
                payload = {
                    "route": expected_route,
                    "request": {
                        "deviceName": "MB2-C00419",
                        "requestedVersion": version,
                    },
                    "device": {
                        "deviceName": "MB2-C00419",
                        "version": "2.11.299",
                    },
                    "dispatch": {
                        "status": True,
                        "message": "dispatch accepted",
                    },
                    "wait": {"status": "completed", "ok": True},
                }

                def operation(
                    raw_question: str,
                    *,
                    device_name: str,
                    on_dispatched: object,
                    resend_ssh_open: bool,
                ) -> tuple[str, dict[str, Any]]:
                    self.assertEqual(raw_question, question)
                    self.assertEqual(device_name, "MB2-C00419")
                    self.assertFalse(resend_ssh_open)
                    assert callable(on_dispatched)
                    on_dispatched("*장비 작업 진행 중*")
                    # domain helper가 poll/최종 결과를 반환하기 전 callback이
                    # 관찰되고, 이 시점에는 activity가 아직 없어야 한다.
                    self.assertEqual(len(partials), 1)
                    create_activity.assert_not_called()
                    return "*장비 작업 완료*", payload

                operation_mock = Mock(side_effect=operation)
                deps = replace(
                    DeviceOperationsRouteDeps(),
                    **{dependency_name: operation_mock},
                    build_update_activity_input=build_activity,
                    create_activity_log=create_activity,
                )
                route = DeviceOperationsAssistantRoute(deps)

                result = route.handle_with_progress(
                    _request(question),
                    partials.append,
                )

                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(len(partials), 1)
                self.assertEqual(partials[0].route, expected_route)
                self.assertEqual(len(partials[0].messages), 1)
                self.assertFalse(partials[0].messages[0].mention_actor)
                self.assertEqual(
                    partials[0].messages[0].body,
                    "**장비 작업 진행 중**",
                )
                # progressive final에는 이미 보낸 prefix가 다시 들어가지 않는다.
                self.assertEqual(
                    [message.body for message in result.messages],
                    ["**장비 작업 완료**"],
                )
                self.assertEqual(
                    result.operation_result,
                    {
                        "kind": DEVICE_OPERATION_DELIVERY_ACTION,
                        "status": "pending",
                        "delivery": {
                            "route": expected_route,
                            "deviceName": "MB2-C00419",
                            "requestedVersion": version,
                            "currentBoxVersion": "2.11.299",
                            "dispatchMessage": "dispatch accepted",
                            "waitStatus": "completed",
                            "waitOk": True,
                        },
                    },
                )
                build_activity.assert_not_called()
                create_activity.assert_not_called()
                operation_mock.assert_called_once()

    def test_delivery_receipt_logs_once_and_rejects_changed_replay(
        self,
    ) -> None:
        payload = {
            "route": "device_box_update",
            "request": {
                "deviceName": "MB2-C00419",
                "requestedVersion": "2.11.300",
            },
            "device": {
                "deviceName": "MB2-C00419",
                "version": "2.11.299",
            },
            "dispatch": {
                "status": True,
                "message": "dispatch accepted",
            },
            "wait": {"status": "completed", "ok": True},
        }

        def update(
            question: str,
            *,
            device_name: str,
            on_dispatched: object,
            resend_ssh_open: bool,
        ) -> tuple[str, dict[str, Any]]:
            self.assertFalse(resend_ssh_open)
            assert callable(on_dispatched)
            on_dispatched("진행 중")
            return "완료", payload

        operation = Mock(side_effect=update)
        build_activity = Mock(return_value={"activityType": "device.edit"})
        create_activity = Mock(return_value={"status": True})
        route = DeviceOperationsAssistantRoute(
            replace(
                DeviceOperationsRouteDeps(),
                request_box_update=operation,
                build_update_activity_input=build_activity,
                create_activity_log=create_activity,
            )
        )
        request = _request(
            "MB2-C00419 박스 업데이트",
            metadata={
                "channel_id": "CHANNEL-1",
                "actor_name": "홍 길동",
            },
        )
        initial = route.handle_with_progress(request, lambda result: None)
        self.assertIsNotNone(initial)
        assert initial is not None
        assert initial.operation_result is not None
        outgoing = initial.operation_result["delivery"]
        assert isinstance(outgoing, dict)
        receipt_delivery = {
            "route": outgoing["route"],
            "device_name": outgoing["deviceName"],
            "requested_version": outgoing["requestedVersion"],
            "current_box_version": outgoing["currentBoxVersion"],
            "dispatch_message": outgoing["dispatchMessage"],
            "wait_status": outgoing["waitStatus"],
            "wait_ok": outgoing["waitOk"],
        }
        receipt = replace(
            request,
            metadata={
                **request.metadata,
                "operation_action": {
                    "name": DEVICE_OPERATION_DELIVERY_ACTION,
                    "phase": "delivered",
                    "delivery": receipt_delivery,
                },
            },
        )

        ack = route.handle(receipt)
        duplicate = route.handle(receipt)
        altered = replace(
            receipt,
            metadata={
                **receipt.metadata,
                "operation_action": {
                    "name": DEVICE_OPERATION_DELIVERY_ACTION,
                    "phase": "delivered",
                    "delivery": {
                        **receipt_delivery,
                        "dispatch_message": "different accepted message",
                    },
                },
            },
        )
        conflict = route.handle(altered)

        self.assertIsNotNone(ack)
        self.assertEqual(duplicate, ack)
        assert ack is not None
        self.assertEqual(ack.route, DEVICE_OPERATION_DELIVERY_ACTION)
        self.assertEqual(ack.outcome, "answered")
        self.assertFalse(ack.messages[0].mention_actor)
        self.assertIsNotNone(conflict)
        assert conflict is not None
        self.assertEqual(conflict.outcome, "denied")
        self.assertEqual(
            conflict.fallback_reason,
            "device_operation_delivery_receipt_conflict",
        )
        build_activity.assert_called_once_with(
            question="MB2-C00419 박스 업데이트",
            user_id="ACTOR-1",
            user_name="홍 길동",
            channel_id="CHANNEL-1",
            thread_ts="THREAD-1",
            result_payload=payload,
        )
        create_activity.assert_called_once_with(
            {"activityType": "device.edit"}
        )
        operation.assert_called_once()

    def test_delivery_receipt_requires_original_question_route_match(
        self,
    ) -> None:
        operation = Mock()
        create_activity = Mock()
        route = DeviceOperationsAssistantRoute(
            replace(
                DeviceOperationsRouteDeps(),
                request_box_update=operation,
                create_activity_log=create_activity,
            )
        )
        request = _request(
            "MB2-C00419 박스 업데이트",
            metadata={
                "operation_action": {
                    "name": DEVICE_OPERATION_DELIVERY_ACTION,
                    "phase": "delivered",
                    "delivery": {
                        "route": "device_agent_update",
                        "device_name": "MB2-C00419",
                        "requested_version": "latest",
                        "current_box_version": "2.11.299",
                        "dispatch_message": "dispatch accepted",
                        "wait_status": "completed",
                        "wait_ok": True,
                    },
                }
            },
        )

        result = route.handle(request)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "device_operation_delivery_receipt_invalid",
        )
        operation.assert_not_called()
        create_activity.assert_not_called()

    def test_activity_log_failure_does_not_replace_update_result(self) -> None:
        update = Mock(
            return_value=(
                "*장비 종료 완료*",
                {
                    "route": "device_power_off",
                    "dispatch": {"status": True},
                },
            )
        )
        deps = replace(
            DeviceOperationsRouteDeps(),
            request_power_off=update,
            build_update_activity_input=Mock(return_value={"ok": True}),
            create_activity_log=Mock(side_effect=RuntimeError("mda down")),
        )

        result = DeviceOperationsAssistantRoute(deps).handle(
            _request("MB2-C00419 장비 종료")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(result.messages[-1].body, "**장비 종료 완료**")

    def test_audio_and_diagnostic_routes_use_legacy_evidence_synthesis(self) -> None:
        cases = (
            (
                "MB2-C00419 장비 소리 출력 점검",
                "device_audio_probe",
                280,
                "probe_audio",
                ("*장비 소리 출력 점검*\n• 장비: `MB2-C00419`",),
                {"route": "device_audio_probe"},
            ),
            (
                "MB2-C00419 최근 종료 원인 알려줘",
                "device_diagnostic_analysis",
                500,
                "start_diagnostic_analysis",
                ("*장비 진단 답변*",),
                {"route": "device_diagnostic_freeform"},
            ),
        )
        for (
            question,
            expected_route,
            max_tokens,
            dep_name,
            fallback_parts,
            evidence,
        ) in cases:
            with self.subTest(route=expected_route):
                operation = Mock(
                    return_value=("\n".join(fallback_parts), evidence)
                )
                deps = replace(
                    DeviceOperationsRouteDeps(),
                    **{dep_name: operation},
                )
                composer = Mock()
                composer.compose.return_value = CompanyAssistantResult(
                    route=expected_route,
                    outcome="answered",
                    messages=(AssistantMessage(body="합성 답변"),),
                    used_llm=True,
                )

                result = DeviceOperationsAssistantRoute(
                    deps,
                    answer_composer=composer,
                    timeout_message="timeout",
                ).handle(_request(question))

                self.assertIsNotNone(result)
                assert result is not None
                self.assertTrue(result.used_llm)
                self.assertEqual(result.messages[0].body, "합성 답변")
                composer.compose.assert_called_once()
                self.assertEqual(
                    composer.compose.call_args.kwargs["evidence"],
                    evidence,
                )
                policy = composer.compose.call_args.kwargs["policy"]
                self.assertEqual(policy.route, expected_route)
                self.assertEqual(policy.max_tokens, max_tokens)
                self.assertEqual(policy.timeout_message, "timeout")

    def test_diagnostic_followup_passes_snapshot_evidence_to_composer(self) -> None:
        snapshot = {"request": {"deviceName": "MB2-C00419"}}
        evidence = {**snapshot, "followupLiveCheck": {"performed": True}}
        deps = replace(
            DeviceOperationsRouteDeps(),
            load_diagnostic_snapshot=Mock(return_value=snapshot),
            build_diagnostic_followup_evidence=Mock(return_value=evidence),
            build_diagnostic_followup_fallback=Mock(
                return_value="*장비 진단 답변*"
            ),
        )
        composer = Mock()
        composer.compose.return_value = CompanyAssistantResult(
            route="device_diagnostic_followup",
            outcome="answered",
            messages=(AssistantMessage(body="후속 합성 답변"),),
            used_llm=True,
        )
        request = _request(
            "최근 종료 원인",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "OTHER-ACTOR",
                    "text": "MB2-C00419 진단 시작",
                },
            ),
        )

        result = DeviceOperationsAssistantRoute(
            deps,
            answer_composer=composer,
        ).handle(request)

        self.assertIsNotNone(result)
        composer.compose.assert_called_once()
        self.assertEqual(
            composer.compose.call_args.kwargs["evidence"], evidence
        )
        self.assertEqual(
            composer.compose.call_args.kwargs["policy"].max_tokens,
            500,
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

    def test_route_exposes_progress_callback_contract(self) -> None:
        route = DeviceOperationsAssistantRoute()

        self.assertTrue(hasattr(route, "handle_with_progress"))


if __name__ == "__main__":
    unittest.main()
