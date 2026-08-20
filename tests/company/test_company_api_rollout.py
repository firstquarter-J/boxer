from __future__ import annotations

import logging
from types import SimpleNamespace
import threading
import time
from typing import Any, Callable
import unittest
from unittest.mock import patch

from boxer_company.assistant import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
    SuggestedAction,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiAvailabilityError,
    CompanyApiContractError,
    CompanyApiPolicyError,
)
from boxer_company_adapter_slack.company_api_rollout import (
    BoundedShadowRunner,
    CompanyBarcodeApiRolloutService,
    CompanyBarcodeFreeformApiRolloutService,
    CompanyBarcodeResidualApiRolloutService,
    CompanyBarcodeTimelineApiRolloutService,
    CompanyBarcodeLogApiRolloutService,
    CompanyDeviceApiRolloutService,
    CompanyDeviceDbDetailApiRolloutService,
    CompanyDeviceFilterApiRolloutService,
    CompanyFreeformApiRolloutService,
    CompanyNotionApiRolloutService,
    CompanyOperationsApiRolloutService,
    CompanyPlaybookApiRolloutService,
    CompanyRecordingFailureApiRolloutService,
    CompanyStructuredApiRolloutService,
    CompanyWeeklySummaryApiRolloutService,
    wrap_company_device_db_detail_service,
    wrap_company_freeform_service,
    wrap_company_notion_service,
    wrap_company_operations_service,
    wrap_company_barcode_freeform_service,
    wrap_company_barcode_timeline_service,
    wrap_company_playbook_service,
    wrap_company_structured_service,
    wrap_company_weekly_summary_service,
)


def _request(
    question: str = "회사 노션에서 Commerce 찾아줘",
    *,
    context_entries: tuple[dict[str, str], ...] = (),
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="slack:T1:C1:1.0",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="1.0",
        question=question,
        locale="ko",
        context_entries=context_entries,
    )


def _result(
    *,
    route: str = "company_notion_qa",
    outcome: str = "answered",
    body: str = "문서 답변",
    fallback_reason: str | None = None,
    used_llm: bool = False,
    delivery_scope: str = "conversation",
    sources: tuple[SourceReference, ...] = (),
    extra_messages: tuple[AssistantMessage, ...] = (),
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,  # type: ignore[arg-type]
        messages=(
            AssistantMessage(
                body=body,
                delivery_scope=delivery_scope,  # type: ignore[arg-type]
            ),
            *extra_messages,
        ),
        sources=sources,
        fallback_reason=fallback_reason,
        used_llm=used_llm,
    )


def _settings(
    mode: str,
    *,
    fallback_enabled: bool = True,
    structured_mode: str = "local",
    structured_fallback_enabled: bool = False,
    device_mode: str = "local",
    device_fallback_enabled: bool = False,
    device_detail_mode: str = "local",
    device_detail_fallback_enabled: bool = False,
    recording_failure_mode: str = "local",
    recording_failure_fallback_enabled: bool = False,
    barcode_log_mode: str = "local",
    barcode_log_fallback_enabled: bool = False,
    barcode_mode: str = "local",
    barcode_fallback_enabled: bool = False,
    barcode_residual_mode: str = "local",
    barcode_residual_fallback_enabled: bool = False,
    barcode_timeline_mode: str = "local",
    barcode_timeline_fallback_enabled: bool = False,
    barcode_freeform_mode: str = "local",
    barcode_freeform_fallback_enabled: bool = False,
    freeform_mode: str = "local",
    freeform_fallback_enabled: bool = False,
    playbook_mode: str = "local",
    playbook_fallback_enabled: bool = False,
    weekly_summary_mode: str = "local",
    weekly_summary_fallback_enabled: bool = False,
    operations_mode: str = "local",
    operations_fallback_enabled: bool = False,
) -> SimpleNamespace:
    # rollout 단위 테스트는 transport 설정과 분리해 전환 필드만 고정한다.
    return SimpleNamespace(
        notion_mode=mode,
        notion_fallback_enabled=fallback_enabled,
        structured_mode=structured_mode,
        structured_fallback_enabled=structured_fallback_enabled,
        device_mode=device_mode,
        device_fallback_enabled=device_fallback_enabled,
        device_detail_mode=device_detail_mode,
        device_detail_fallback_enabled=(
            device_detail_fallback_enabled
        ),
        recording_failure_mode=recording_failure_mode,
        recording_failure_fallback_enabled=(
            recording_failure_fallback_enabled
        ),
        barcode_log_mode=barcode_log_mode,
        barcode_log_fallback_enabled=barcode_log_fallback_enabled,
        barcode_mode=barcode_mode,
        barcode_fallback_enabled=barcode_fallback_enabled,
        barcode_residual_mode=barcode_residual_mode,
        barcode_residual_fallback_enabled=(
            barcode_residual_fallback_enabled
        ),
        barcode_timeline_mode=barcode_timeline_mode,
        barcode_timeline_fallback_enabled=(
            barcode_timeline_fallback_enabled
        ),
        barcode_freeform_mode=barcode_freeform_mode,
        barcode_freeform_fallback_enabled=(
            barcode_freeform_fallback_enabled
        ),
        freeform_mode=freeform_mode,
        freeform_fallback_enabled=freeform_fallback_enabled,
        playbook_mode=playbook_mode,
        playbook_fallback_enabled=playbook_fallback_enabled,
        weekly_summary_mode=weekly_summary_mode,
        weekly_summary_fallback_enabled=(
            weekly_summary_fallback_enabled
        ),
        operations_mode=operations_mode,
        operations_fallback_enabled=operations_fallback_enabled,
    )


def _structured_settings(
    mode: str,
    *,
    fallback_enabled: bool = False,
) -> SimpleNamespace:
    return _settings(
        "local",
        fallback_enabled=False,
        structured_mode=mode,
        structured_fallback_enabled=fallback_enabled,
    )


class _FakeLocalService:
    def __init__(
        self,
        result: CompanyAssistantResult | None,
    ) -> None:
        self.result = result
        self.requests: list[CompanyAssistantRequest] = []

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        self.requests.append(request)
        return self.result


class _FakeApiClient:
    def __init__(
        self,
        result: CompanyAssistantResult | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.requests: list[CompanyAssistantRequest] = []
        self.route_groups: list[str | None] = []

    def answer(
        self,
        request: CompanyAssistantRequest,
        *,
        route_group: str | None = None,
    ) -> CompanyAssistantResult | None:
        self.requests.append(request)
        self.route_groups.append(route_group)
        if self.error is not None:
            raise self.error
        return self.result


class _FakeProgressLocalService(_FakeLocalService):
    def __init__(
        self,
        partial: CompanyAssistantResult,
        final: CompanyAssistantResult,
    ) -> None:
        super().__init__(final)
        self.partial = partial

    def answer_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult:
        self.requests.append(request)
        on_partial_result(self.partial)
        assert self.result is not None
        return self.result


class _CapturingShadowRunner:
    def __init__(self, *, accepted: bool = True) -> None:
        self.accepted = accepted
        self.tasks: list[Callable[[], None]] = []

    def submit(self, task: Callable[[], None]) -> bool:
        if self.accepted:
            self.tasks.append(task)
        return self.accepted


class CompanyApiRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def test_local_mode_returns_original_service_and_never_calls_api(
        self,
    ) -> None:
        local = _FakeLocalService(_result())
        api = _FakeApiClient(_result())

        wrapped = wrap_company_notion_service(
            local,  # type: ignore[arg-type]
            _settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )

        self.assertIs(wrapped, local)
        self.assertIs(wrapped.answer(_request()), local.result)
        self.assertEqual(len(local.requests), 1)
        self.assertEqual(api.requests, [])

    def test_shadow_returns_only_local_result_and_compares_async(
        self,
    ) -> None:
        local_result = _result(
            body="LOCAL-SECRET-BODY",
            fallback_reason="local_fallback",
            sources=(
                SourceReference(
                    source_id="LOCAL-SECRET-SOURCE-ID",
                    title="로컬 문서",
                    uri="https://docs.test/local?token=LOCAL-SECRET-TOKEN",
                ),
            ),
            extra_messages=(AssistantMessage(body="두 번째 로컬 답변"),),
        )
        remote_result = _result(
            route="company_notion_search",
            outcome="no_evidence",
            body="REMOTE-SECRET-BODY",
            fallback_reason="remote_fallback",
            used_llm=True,
            sources=(
                SourceReference(
                    source_id="REMOTE-SECRET-SOURCE-ID",
                    title="원격 문서",
                    uri=(
                        "https://app.notion.com/p/"
                        "REMOTE-SECRET-TOKEN"
                    ),
                ),
            ),
        )
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(remote_result)
        runner = _CapturingShadowRunner()
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings("shadow"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )
        request = _request("회사 노션에서 SECRET-QUESTION 찾아줘")

        returned = wrapped.answer(request)

        # remote 결과를 호출자에게 합치거나 두 번째로 반환하지 않는다.
        self.assertIs(returned, local_result)
        self.assertEqual(len(local.requests), 1)
        self.assertEqual(api.requests, [])
        self.assertEqual(len(runner.tasks), 1)

        with self.assertLogs(self.logger, level="INFO") as captured:
            runner.tasks[0]()

        self.assertEqual(api.requests, [request])
        logs = "\n".join(captured.output)
        self.assertIn("route_match=False", logs)
        self.assertIn("outcome_match=False", logs)
        self.assertIn("fallback_match=False", logs)
        self.assertIn("used_llm_match=False", logs)
        self.assertIn("source_set_match=False", logs)
        # 같은 conversation 메시지의 transport 분할 수는 scope 차이가 아니다.
        self.assertIn("message_scope_match=True", logs)
        for secret in (
            "SECRET-QUESTION",
            "LOCAL-SECRET-BODY",
            "REMOTE-SECRET-BODY",
            "LOCAL-SECRET-SOURCE-ID",
            "REMOTE-SECRET-SOURCE-ID",
            "LOCAL-SECRET-TOKEN",
            "REMOTE-SECRET-TOKEN",
        ):
            self.assertNotIn(secret, logs)

    def test_shadow_ignores_api_transport_chunk_boundaries(self) -> None:
        body = "가" * 30_001
        local_result = _result(body=body)
        remote_result = _result(
            body=body[:30_000],
            extra_messages=(
                AssistantMessage(
                    body=body[30_000:],
                    mention_actor=False,
                ),
            ),
        )
        runner = _CapturingShadowRunner()
        wrapped = CompanyNotionApiRolloutService(
            _FakeLocalService(local_result),  # type: ignore[arg-type]
            settings=_settings("shadow"),  # type: ignore[arg-type]
            api_client=_FakeApiClient(remote_result),  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )

        wrapped.answer(_request())
        with self.assertLogs(self.logger, level="INFO") as captured:
            runner.tasks[0]()

        logs = "\n".join(captured.output)
        self.assertIn("message_scope_match=True", logs)
        self.assertIn("message_body_match=True", logs)

    def test_shadow_only_submits_after_local_notion_match(self) -> None:
        local = _FakeLocalService(
            _result(route="hospitals_filter")
        )
        api = _FakeApiClient(_result())
        runner = _CapturingShadowRunner()
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings("shadow"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )

        returned = wrapped.answer(_request())

        self.assertIs(returned, local.result)
        self.assertEqual(runner.tasks, [])
        self.assertEqual(api.requests, [])

    def test_shadow_never_bypasses_local_actor_denial(self) -> None:
        local_result = _result(
            route="company_notion_search",
            outcome="denied",
            fallback_reason="actor_not_allowed",
        )
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(_result())
        runner = _CapturingShadowRunner()
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings("shadow"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )

        returned = wrapped.answer(_request())

        self.assertIs(returned, local_result)
        self.assertEqual(runner.tasks, [])
        self.assertEqual(api.requests, [])

    def test_shadow_error_log_does_not_include_exception_or_question(
        self,
    ) -> None:
        secret = "SECRET-TOKEN-AND-BODY"
        local = _FakeLocalService(_result())
        api = _FakeApiClient(
            error=CompanyApiContractError(secret)
        )
        runner = _CapturingShadowRunner()
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings("shadow"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )
        request = _request(
            f"회사 노션에서 {secret} 찾아줘"
        )

        self.assertIs(wrapped.answer(request), local.result)
        with self.assertLogs(self.logger, level="WARNING") as captured:
            runner.tasks[0]()

        logs = "\n".join(captured.output)
        self.assertIn("reason=contract", logs)
        self.assertNotIn(secret, logs)

    def test_remote_returns_allowed_answered_denied_or_failed_unchanged(
        self,
    ) -> None:
        remote_results = (
            _result(outcome="answered", body="원격 문서 답변"),
            _result(
                route="company_notion_search",
                outcome="denied",
                body="허용되지 않은 사용자야",
                fallback_reason="actor_not_allowed",
            ),
            _result(
                route="company_notion_search",
                outcome="failed",
                body="원격 조회 실패",
                fallback_reason="retrieval_error",
            ),
        )
        for remote_result in remote_results:
            with self.subTest(outcome=remote_result.outcome):
                local = _FakeLocalService(_result())
                api = _FakeApiClient(remote_result)
                wrapped = CompanyNotionApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=_settings("remote"),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                returned = wrapped.answer(_request())

                self.assertIs(returned, remote_result)
                self.assertEqual(local.requests, [])
                self.assertEqual(len(api.requests), 1)

    def test_remote_availability_failure_uses_enabled_local_fallback(
        self,
    ) -> None:
        local = _FakeLocalService(_result(body="로컬 fallback"))
        api = _FakeApiClient(
            error=CompanyApiAvailabilityError("service_not_ready")
        )
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings(
                "remote",
                fallback_enabled=True,
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        returned = wrapped.answer(_request())

        self.assertIs(returned, local.result)
        self.assertEqual(len(local.requests), 1)

    def test_remote_availability_without_fallback_fails_closed(
        self,
    ) -> None:
        local = _FakeLocalService(_result())
        api = _FakeApiClient(
            error=CompanyApiAvailabilityError("service_not_ready")
        )
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings(
                "remote",
                fallback_enabled=False,
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        returned = wrapped.answer(_request())

        self.assertEqual(returned.outcome, "failed")
        self.assertEqual(
            returned.fallback_reason,
            "company_api_availability",
        )
        self.assertEqual(local.requests, [])

    def test_remote_policy_contract_and_ambiguous_timeout_fail_closed(
        self,
    ) -> None:
        error_cases = (
            (
                CompanyApiPolicyError("caller_not_allowed"),
                "company_api_policy",
            ),
            (
                CompanyApiContractError("invalid_response"),
                "company_api_contract",
            ),
            (
                CompanyApiAmbiguousTimeoutError("read_timeout"),
                "company_api_ambiguous_timeout",
            ),
        )
        for error, expected_reason in error_cases:
            with self.subTest(error=type(error).__name__):
                local = _FakeLocalService(_result())
                api = _FakeApiClient(error=error)
                wrapped = CompanyNotionApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=_settings(
                        "remote",
                        fallback_enabled=True,
                    ),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                returned = wrapped.answer(_request())

                self.assertEqual(returned.outcome, "failed")
                self.assertEqual(
                    returned.fallback_reason,
                    expected_reason,
                )
                self.assertEqual(local.requests, [])

    def test_remote_unexpected_or_unhandled_route_fails_closed(
        self,
    ) -> None:
        for remote_result in (
            None,
            _result(route="unhandled", outcome="no_evidence"),
            _result(route="hospitals_filter"),
        ):
            with self.subTest(
                route=getattr(remote_result, "route", None)
            ):
                local = _FakeLocalService(_result(body="로컬"))
                api = _FakeApiClient(remote_result)
                wrapped = CompanyNotionApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=_settings("remote"),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                returned = wrapped.answer(_request())

                # remote cutover 뒤 route drift가 Slack-local Notion/LLM을
                # 다시 실행하지 않도록 실패 결과만 반환한다.
                self.assertEqual(returned.outcome, "failed")
                self.assertIn(
                    returned.fallback_reason,
                    {
                        "company_api_unexpected_route",
                        "company_api_route_mismatch",
                    },
                )
                self.assertEqual(local.requests, [])

    def test_remote_unexpected_denial_does_not_bypass_api_policy(
        self,
    ) -> None:
        local = _FakeLocalService(_result(body="로컬"))
        api = _FakeApiClient(
            _result(route="api_policy", outcome="denied")
        )
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        returned = wrapped.answer(_request())

        self.assertEqual(returned.outcome, "failed")
        self.assertEqual(
            returned.fallback_reason,
            "company_api_policy",
        )
        self.assertEqual(local.requests, [])

    def test_remote_requester_message_fails_closed_without_dm_fallback(
        self,
    ) -> None:
        local = _FakeLocalService(_result())
        api = _FakeApiClient(
            _result(delivery_scope="requester")
        )
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        returned = wrapped.answer(_request())

        self.assertEqual(returned.outcome, "failed")
        self.assertEqual(
            returned.fallback_reason,
            "company_api_unsafe_message_scope",
        )
        self.assertEqual(local.requests, [])
        self.assertTrue(
            all(
                message.delivery_scope == "conversation"
                for message in returned.messages
            )
        )

    def test_remote_non_notion_source_fails_closed(self) -> None:
        local = _FakeLocalService(_result())
        api = _FakeApiClient(
            _result(
                sources=(
                    SourceReference(
                        source_id="unexpected-source",
                        title="외부 문서",
                        uri="https://external.example/document",
                    ),
                ),
            )
        )
        wrapped = CompanyNotionApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        returned = wrapped.answer(_request())

        self.assertEqual(returned.outcome, "failed")
        self.assertEqual(
            returned.fallback_reason,
            "company_api_unsafe_source_host",
        )
        self.assertEqual(local.requests, [])

    def test_non_notion_and_live_diagnostic_never_call_api(self) -> None:
        for question in (
            "서울병원 병실 목록",
            "MB2-U00001 장비 진단해줘",
            "12345678910 영상 복원해줘",
        ):
            with self.subTest(question=question):
                local = _FakeLocalService(None)
                api = _FakeApiClient(_result())
                wrapped = CompanyNotionApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=_settings("remote"),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                returned = wrapped.answer(_request(question))

                self.assertIsNone(returned)
                self.assertEqual(len(local.requests), 1)
                self.assertEqual(api.requests, [])

    def test_shadow_runner_is_daemon_and_rejects_over_capacity(
        self,
    ) -> None:
        runner = BoundedShadowRunner(
            max_pending=1,
            logger=self.logger,
        )
        started = threading.Event()
        release = threading.Event()
        completed = threading.Event()
        daemon_values: list[bool] = []

        def blocking_task() -> None:
            daemon_values.append(threading.current_thread().daemon)
            started.set()
            release.wait(timeout=2)
            completed.set()

        self.assertTrue(runner.submit(blocking_task))
        self.assertTrue(started.wait(timeout=1))
        self.assertFalse(runner.submit(lambda: None))
        release.set()
        self.assertTrue(completed.wait(timeout=1))

        # slot 반환은 task 완료 직후 일어나므로 짧게 poll해 경합을 제거한다.
        deadline = time.monotonic() + 1
        while time.monotonic() < deadline:
            if runner.submit(lambda: None):
                break
            time.sleep(0.01)
        else:
            self.fail("shadow runner did not release its bounded slot")
        self.assertEqual(daemon_values, [True])


class CompanyPlaybookApiRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def _settings(
        self,
        mode: str,
        *,
        fallback_enabled: bool = False,
    ) -> SimpleNamespace:
        return _settings(
            "local",
            fallback_enabled=False,
            playbook_mode=mode,
            playbook_fallback_enabled=fallback_enabled,
        )

    def test_local_mode_keeps_original_knowledge_service(self) -> None:
        local = _FakeLocalService(
            _result(route="notion_playbook_qa")
        )
        api = _FakeApiClient(
            _result(route="notion_playbook_qa")
        )

        wrapped = wrap_company_playbook_service(
            local,  # type: ignore[arg-type]
            self._settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )

        self.assertIs(wrapped, local)
        self.assertIs(
            wrapped.answer(_request("마미박스 초기화 방법 알려줘")),
            local.result,
        )
        self.assertEqual(api.requests, [])

    def test_remote_preserves_preceding_local_diagnostic_route(self) -> None:
        diagnostic = _result(
            route="device_diagnostic_followup",
            body="로컬 진단 snapshot 답변",
        )
        precedence = _FakeLocalService(diagnostic)
        local = _FakeLocalService(
            _result(route="notion_playbook_qa", body="로컬 문서 답변")
        )
        api = _FakeApiClient(
            _result(route="notion_playbook_qa", body="원격 문서 답변")
        )
        service = CompanyPlaybookApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
            precedence_service=precedence,  # type: ignore[arg-type]
        )
        request = _request("증상은 어때?")

        self.assertIs(service.answer(request), diagnostic)
        self.assertEqual(precedence.requests, [request])
        self.assertEqual(local.requests, [])
        self.assertEqual(api.requests, [])

    def test_remote_moves_direct_and_contextual_followup_to_api(self) -> None:
        remote = _result(
            route="notion_playbook_qa",
            body="**문서 기반 답변**\n• 결론: 운영 문서 기준",
            used_llm=True,
            sources=(
                SourceReference(
                    source_id="https://www.notion.so/playbook",
                    title="운영 플레이북",
                    uri="https://www.notion.so/playbook",
                ),
            ),
        )
        local = _FakeLocalService(
            _result(route="notion_playbook_qa", body="로컬 답변")
        )
        api = _FakeApiClient(remote)
        service = CompanyPlaybookApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )
        direct = _request("마미박스 초기화 방법 알려줘")
        followup = _request(
            "그럼 다른 방법은?",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "U2",
                    "text": (
                        "**문서 기반 답변**\n"
                        "• 결론: 초기화 전 상태 확인\n"
                        "**함께 참고할 문서**"
                    ),
                },
            ),
        )

        self.assertIs(service.answer(direct), remote)
        self.assertIs(service.answer(followup), remote)
        self.assertEqual(api.requests, [direct, followup])
        self.assertEqual(api.route_groups, ["knowledge", "knowledge"])
        self.assertEqual(local.requests, [])

    def test_remote_leaves_unrelated_and_live_requests_local(self) -> None:
        local_result = _result(route="barcode_evidence_freeform")
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(_result(route="notion_playbook_qa"))
        service = CompanyPlaybookApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        # 이 스위치는 플레이북 질문만 담당하고 일반 대화·실시간 진단을
        # 공통 read-only turn으로 잘못 보내지 않는다.
        for question in ("안녕?", "MB2-C00419 PM2 상태 진단해줘"):
            with self.subTest(question=question):
                self.assertIs(
                    service.answer(_request(question)),
                    local_result,
                )
        self.assertEqual(api.requests, [])
        self.assertEqual(len(local.requests), 2)

    def test_shadow_never_returns_remote_and_unsafe_source_fails_closed(
        self,
    ) -> None:
        request = _request("마미박스 초기화 방법 알려줘")
        local_result = _result(route="notion_playbook_qa", body="로컬")
        runner = _CapturingShadowRunner()
        shadow_api = _FakeApiClient(
            _result(route="notion_playbook_qa", body="원격")
        )
        shadow = CompanyPlaybookApiRolloutService(
            _FakeLocalService(local_result),  # type: ignore[arg-type]
            settings=self._settings("shadow"),  # type: ignore[arg-type]
            api_client=shadow_api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )

        self.assertIs(shadow.answer(request), local_result)
        self.assertEqual(shadow_api.requests, [])
        self.assertEqual(len(runner.tasks), 1)
        runner.tasks[0]()
        self.assertEqual(shadow_api.requests, [request])

        unsafe = CompanyPlaybookApiRolloutService(
            _FakeLocalService(local_result),  # type: ignore[arg-type]
            settings=self._settings(
                "remote",
                fallback_enabled=True,
            ),  # type: ignore[arg-type]
            api_client=_FakeApiClient(
                _result(
                    route="notion_playbook_qa",
                    sources=(
                        SourceReference(
                            source_id="unsafe",
                            title="외부 문서",
                            uri="https://external.example/playbook",
                        ),
                    ),
                )
            ),  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        failed = unsafe.answer(request)
        self.assertEqual(failed.outcome, "failed")
        self.assertEqual(
            failed.fallback_reason,
            "company_api_unsafe_source_host",
        )


class CompanyBarcodeFreeformApiRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def _settings(
        self,
        mode: str,
        *,
        fallback_enabled: bool = False,
    ) -> SimpleNamespace:
        return _settings(
            "local",
            fallback_enabled=False,
            barcode_freeform_mode=mode,
            barcode_freeform_fallback_enabled=fallback_enabled,
        )

    def test_local_mode_preserves_original_service_without_api(self) -> None:
        local = _FakeLocalService(
            _result(route="barcode_evidence_freeform", body="로컬 답변")
        )
        api = _FakeApiClient(
            _result(route="barcode_evidence_freeform", body="원격 답변")
        )

        wrapped = wrap_company_barcode_freeform_service(
            local,  # type: ignore[arg-type]
            self._settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )
        request = _request(
            "12345678910 녹화 기록들 사이 간격이 일정한지 설명해줘"
        )

        self.assertIs(wrapped, local)
        self.assertIs(wrapped.answer(request), local.result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_remote_moves_only_explicit_evidence_request_to_knowledge_api(
        self,
    ) -> None:
        remote = _result(
            route="barcode_evidence_freeform",
            body="최근 녹화 근거를 종합하면 간격이 일정해.",
            used_llm=True,
        )
        local = _FakeLocalService(
            _result(route="barcode_evidence_freeform", body="로컬 답변")
        )
        api = _FakeApiClient(remote)
        service = CompanyBarcodeFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )
        request = _request(
            "12345678910 녹화 기록들 사이 간격이 일정한지 설명해줘"
        )

        self.assertIs(service.answer(request), remote)
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["knowledge"])
        self.assertEqual(local.requests, [])

    def test_remote_preserves_local_diagnostic_snapshot_precedence(
        self,
    ) -> None:
        diagnostic = _result(
            route="device_diagnostic_followup",
            body="저장된 진단 근거 답변",
        )
        precedence = _FakeLocalService(diagnostic)
        local = _FakeLocalService(
            _result(route="barcode_evidence_freeform")
        )
        api = _FakeApiClient(
            _result(route="barcode_evidence_freeform", used_llm=True)
        )
        service = CompanyBarcodeFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
            precedence_service=precedence,  # type: ignore[arg-type]
        )
        request = _request(
            "12345678910 녹화 기록들 사이 간격이 일정한지 설명해줘"
        )

        self.assertIs(service.answer(request), diagnostic)
        self.assertEqual(precedence.requests, [request])
        self.assertEqual(local.requests, [])
        self.assertEqual(api.requests, [])

    def test_remote_keeps_other_security_and_existing_routes_local(
        self,
    ) -> None:
        # 명시적인 recordings 근거 해석 외에는 기존 Slack 권한·route
        # 우선순위를 그대로 거치게 해 원격 matcher 범위를 넓히지 않는다.
        local_questions = (
            "오늘 기분 어때?",
            "12345678910 산모 전화번호를 녹화 기록 근거로 확인해줘",
            "12345678910 녹화 기록을 수정해줘",
            "12345678910 MB2-C00419 PM2 기록을 근거로 분석해줘",
            "12345678910 마미박스 초기화 방법을 녹화 기록 근거로 설명해줘",
            "12345678910 2026-08-04 영상 조회",
            "12345678910 영상 개수",
            "12345678910 마지막 녹화 날짜",
            "12345678910 베이비매직 목록",
        )
        for question in local_questions:
            with self.subTest(question=question):
                local_result = _result(route="local_or_existing_route")
                local = _FakeLocalService(local_result)
                api = _FakeApiClient(
                    _result(route="barcode_evidence_freeform")
                )
                service = CompanyBarcodeFreeformApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=self._settings("remote"),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )
                request = _request(question)

                self.assertIs(service.answer(request), local_result)
                self.assertEqual(local.requests, [request])
                self.assertEqual(api.requests, [])

    def test_remote_allows_llm_but_rejects_side_effect_contracts(
        self,
    ) -> None:
        request = _request(
            "12345678910 녹화 기록들 사이 간격이 일정한지 설명해줘"
        )
        unsafe_results = (
            (
                _result(
                    route="barcode_evidence_freeform",
                    sources=(
                        SourceReference(
                            source_id="unexpected",
                            title="unexpected",
                            uri="https://example.invalid/evidence",
                        ),
                    ),
                    used_llm=True,
                ),
                "company_api_unexpected_sources",
            ),
            (
                CompanyAssistantResult(
                    route="barcode_evidence_freeform",
                    outcome="answered",
                    messages=(AssistantMessage(body="결과"),),
                    used_llm=True,
                    suggested_action=SuggestedAction(
                        action="unsafe",
                        label="실행",
                    ),
                ),
                "company_api_unsafe_action",
            ),
            (
                CompanyAssistantResult(
                    route="barcode_evidence_freeform",
                    outcome="answered",
                    messages=(AssistantMessage(body="결과"),),
                    used_llm=True,
                    async_job={"jobId": "unexpected"},
                ),
                "company_api_unsafe_action",
            ),
            (
                _result(
                    route="barcode_evidence_freeform",
                    delivery_scope="requester",
                    used_llm=True,
                ),
                "company_api_unsafe_message_scope",
            ),
        )
        for remote, expected_reason in unsafe_results:
            with self.subTest(expected_reason=expected_reason):
                local = _FakeLocalService(
                    _result(route="barcode_evidence_freeform")
                )
                api = _FakeApiClient(remote)
                service = CompanyBarcodeFreeformApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=self._settings(
                        "remote",
                        fallback_enabled=True,
                    ),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                result = service.answer(request)

                self.assertEqual(result.outcome, "failed")
                self.assertEqual(result.fallback_reason, expected_reason)
                self.assertEqual(local.requests, [])
                self.assertEqual(api.requests, [request])

    def test_shadow_calls_local_and_api_once_but_returns_local(self) -> None:
        request = _request(
            "12345678910 녹화 기록들 사이 간격이 일정한지 설명해줘"
        )
        local_result = _result(
            route="barcode_evidence_freeform",
            body="로컬 근거 답변",
            used_llm=True,
        )
        remote = _result(
            route="barcode_evidence_freeform",
            body="원격 근거 답변",
            used_llm=True,
        )
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(remote)
        runner = _CapturingShadowRunner()
        service = CompanyBarcodeFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("shadow"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )

        self.assertIs(service.answer(request), local_result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])
        self.assertEqual(len(runner.tasks), 1)

        runner.tasks[0]()

        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["knowledge"])

    def test_remote_route_mismatch_fails_closed_without_local_fallback(
        self,
    ) -> None:
        request = _request(
            "12345678910 녹화 기록들 사이 간격이 일정한지 설명해줘"
        )
        local = _FakeLocalService(
            _result(route="barcode_evidence_freeform", body="로컬 fallback")
        )
        api = _FakeApiClient(_result(route="notion_playbook_qa"))
        service = CompanyBarcodeFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings(
                "remote",
                fallback_enabled=True,
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        result = service.answer(request)

        self.assertEqual(result.outcome, "failed")
        self.assertIn(
            result.fallback_reason,
            {
                "company_api_route_mismatch",
                "company_api_unexpected_route",
            },
        )
        self.assertEqual(local.requests, [])
        self.assertEqual(api.requests, [request])


class CompanyFreeformApiRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False

    def _settings(
        self,
        mode: str,
        *,
        fallback_enabled: bool = False,
    ) -> SimpleNamespace:
        return _settings(
            "local",
            fallback_enabled=False,
            freeform_mode=mode,
            freeform_fallback_enabled=fallback_enabled,
        )

    def test_local_mode_preserves_existing_slack_chain(self) -> None:
        local = _FakeLocalService(None)
        api = _FakeApiClient(
            _result(route="company_freeform", used_llm=True)
        )

        wrapped = wrap_company_freeform_service(
            local,  # type: ignore[arg-type]
            self._settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )
        request = _request("오늘 기분 어때?")

        self.assertIs(wrapped, local)
        self.assertIsNone(wrapped.answer(request))
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_remote_calls_precedence_and_freeform_api_once(self) -> None:
        request = _request("오늘 기분 어때?")
        remote = _result(
            route="company_freeform",
            body="좋아. 오늘도 차근차근 해보자.",
            used_llm=True,
        )
        local = _FakeLocalService(None)
        api = _FakeApiClient(remote)
        service = CompanyFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        self.assertIs(service.answer(request), remote)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["freeform"])

    def test_remote_preserves_prior_knowledge_route_precedence(self) -> None:
        request = _request("마미박스 초기화 방법 알려줘")
        prior_result = _result(
            route="notion_playbook_qa",
            body="기존 운영 문서 답변",
        )
        local = _FakeLocalService(prior_result)
        api = _FakeApiClient(
            _result(route="company_freeform", used_llm=True)
        )
        service = CompanyFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        self.assertIs(service.answer(request), prior_result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_shadow_probes_once_then_continues_legacy_freeform(self) -> None:
        request = _request("오늘 기분 어때?")
        local = _FakeLocalService(None)
        api = _FakeApiClient(
            _result(route="company_freeform", used_llm=True)
        )
        runner = _CapturingShadowRunner()
        service = CompanyFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("shadow"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )

        # None은 outer Slack knowledge handler가 기존 freeform을 딱 한 번
        # 실행하라는 계약이고 shadow 결과는 사용자에게 반환하지 않는다.
        self.assertIsNone(service.answer(request))
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])
        self.assertEqual(len(runner.tasks), 1)

        runner.tasks[0]()

        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["freeform"])

    def test_availability_fallback_continues_legacy_freeform_once(self) -> None:
        request = _request("오늘 기분 어때?")
        local = _FakeLocalService(None)
        api = _FakeApiClient(
            error=CompanyApiAvailabilityError("unavailable")
        )
        service = CompanyFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings(
                "remote",
                fallback_enabled=True,
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        self.assertIsNone(service.answer(request))
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["freeform"])

    def test_operation_request_never_reaches_freeform_api(self) -> None:
        request = _request("MB2-C00419 PM2 상태 확인해줘")
        local = _FakeLocalService(None)
        api = _FakeApiClient(
            _result(route="company_freeform", used_llm=True)
        )
        service = CompanyFreeformApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        self.assertIsNone(service.answer(request))
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_remote_rejects_sources_and_side_effect_contracts(self) -> None:
        request = _request("오늘 기분 어때?")
        unsafe_results = (
            (
                _result(
                    route="company_freeform",
                    sources=(
                        SourceReference(
                            source_id="unexpected",
                            title="unexpected",
                            uri="https://example.invalid/evidence",
                        ),
                    ),
                    used_llm=True,
                ),
                "company_api_unexpected_sources",
            ),
            (
                CompanyAssistantResult(
                    route="company_freeform",
                    outcome="answered",
                    messages=(AssistantMessage(body="결과"),),
                    used_llm=True,
                    suggested_action=SuggestedAction(
                        action="unsafe",
                        label="실행",
                    ),
                ),
                "company_api_unsafe_action",
            ),
        )
        for remote, expected_reason in unsafe_results:
            with self.subTest(expected_reason=expected_reason):
                local = _FakeLocalService(None)
                service = CompanyFreeformApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=self._settings(
                        "remote",
                        fallback_enabled=True,
                    ),  # type: ignore[arg-type]
                    api_client=_FakeApiClient(remote),  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                result = service.answer(request)

                self.assertIsNotNone(result)
                self.assertEqual(result.outcome, "failed")
                self.assertEqual(result.fallback_reason, expected_reason)
                self.assertEqual(local.requests, [request])


class CompanyRemainingReadApiRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def test_device_rollout_moves_only_led_and_count_reads(self) -> None:
        settings = _settings(
            "local",
            device_mode="remote",
        )
        for question, route, service_type in (
            (
                "MB2-B00045 2026-07-01 LED 로그 분석",
                "device_led_log_analysis",
                CompanyDeviceApiRolloutService,
            ),
            (
                "빨간 LED가 깜빡이면 무슨 뜻이야?",
                "device_led_pattern_guide",
                CompanyDeviceApiRolloutService,
            ),
            (
                "활성 장비 몇 개야?",
                "devices_filter",
                CompanyDeviceFilterApiRolloutService,
            ),
        ):
            with self.subTest(route=route):
                remote = _result(route=route, body="원격 조회")
                local = _FakeLocalService(
                    _result(route=route, body="로컬 조회")
                )
                api = _FakeApiClient(remote)
                service = service_type(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )
                request = _request(question)

                self.assertIs(service.answer(request), remote)
                self.assertEqual(api.requests, [request])
                self.assertEqual(
                    api.route_groups,
                    [
                        "structured"
                        if route == "devices_filter"
                        else "device"
                    ],
                )
                self.assertEqual(local.requests, [])

        # 개별 상태·상세는 MDA/SSH 의미를 보존하기 위해 local에 남긴다.
        for question in (
            "MB2-B00045 장비상태",
            "MB2-B00045 장비정보",
            "MB2-B00045 진단 시작",
        ):
            with self.subTest(question=question):
                local_result = _result(route="devices_filter")
                local = _FakeLocalService(local_result)
                api = _FakeApiClient(_result(route="devices_filter"))
                service = CompanyDeviceFilterApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                self.assertIs(service.answer(_request(question)), local_result)
                self.assertEqual(api.requests, [])

    def test_barcode_rollout_accepts_only_five_deterministic_reads(
        self,
    ) -> None:
        settings = _settings("local", barcode_mode="remote")
        allowed = (
            ("12345678910 영상 개수", "barcode_video_count"),
            ("12345678910 영상 정보", "barcode_video_info"),
            ("12345678910 영상 목록", "barcode_video_list"),
            ("12345678910 영상 길이", "barcode_video_length"),
            (
                "12345678910 전체 녹화 날짜",
                "barcode_all_recorded_dates",
            ),
        )
        for question, route in allowed:
            with self.subTest(route=route):
                remote = _result(route=route, body="원격 DB 결과")
                local = _FakeLocalService(_result(route=route))
                api = _FakeApiClient(remote)
                service = CompanyBarcodeApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                self.assertIs(service.answer(_request(question)), remote)
                self.assertEqual(len(api.requests), 1)
                self.assertEqual(local.requests, [])

        # MDA·복구·Baby AI·LLM 합성 경로는 첫 전환 allowlist 밖이다.
        for question in (
            "10255657857 이건 유효성 검사에 걸리는 바코드냐",
            "58291583958 왜 핑크바코드로 분류되지 않았어?",
            "12345678910 베이비매직 목록",
            "12345678910 마지막 녹화 날짜",
            "12345678910 2026-07-01 영상 있어?",
            "12345678910 2026년 7월 영상 복원",
        ):
            with self.subTest(question=question):
                local_result = _result(route="local_only")
                local = _FakeLocalService(local_result)
                api = _FakeApiClient(_result(route="barcode_video_count"))
                service = CompanyBarcodeApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                self.assertIs(service.answer(_request(question)), local_result)
                self.assertEqual(api.requests, [])

    def test_barcode_residual_rollout_moves_only_baby_ai_reads(self) -> None:
        settings = _settings(
            "local",
            barcode_residual_mode="remote",
        )
        allowed = (
            ("베이비매직 목록", "baby_ai_list"),
            (
                "12345678910 베이비매직 목록",
                "barcode_baby_ai_list",
            ),
        )
        for question, route in allowed:
            with self.subTest(route=route):
                remote = _result(
                    route=route,
                    body="원격 DB 결과",
                )
                local = _FakeLocalService(_result(route=route))
                api = _FakeApiClient(remote)
                service = CompanyBarcodeResidualApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )
                request = _request(question)

                self.assertIs(service.answer(request), remote)
                self.assertEqual(api.requests, [request])
                self.assertEqual(api.route_groups, ["barcode"])
                self.assertEqual(local.requests, [])

        # MDA 판정, 복원 mutation, 기존 다섯 route와 아직 structured
        # 우선순위를 분리하지 않은 날짜 route는 새 스위치가 선점하지 않는다.
        for question in (
            "10255657857 이건 유효성 검사에 걸리는 바코드냐",
            "58291583958 왜 핑크바코드로 분류되지 않았어?",
            "12345678910 2024년 4월 영상 복원",
            "12345678910 영상 개수",
            "12345678910 마지막 녹화 날짜",
            "12345678910 2026-08-04 녹화됐어?",
        ):
            with self.subTest(question=question):
                local_result = _result(route="local_or_existing_rollout")
                local = _FakeLocalService(local_result)
                api = _FakeApiClient(_result(route="baby_ai_list"))
                service = CompanyBarcodeResidualApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                self.assertIs(service.answer(_request(question)), local_result)
                self.assertEqual(api.requests, [])

    def test_barcode_residual_allows_only_fixed_baby_magic_cdn_sources(
        self,
    ) -> None:
        settings = _settings(
            "local",
            barcode_residual_mode="remote",
        )
        safe_source = SourceReference(
            source_id="baby-magic-1",
            title="베이비매직 결과 1",
            uri="https://cdn-kr.mmtalkbox.com/results/one.jpg",
        )
        safe_remote = _result(
            route="barcode_baby_ai_list",
            body=(
                "[열기](https://cdn-kr.mmtalkbox.com/results/one.jpg)"
            ),
            sources=(safe_source,),
        )
        safe_service = CompanyBarcodeResidualApiRolloutService(
            _FakeLocalService(_result(route="local")),  # type: ignore[arg-type]
            settings=settings,  # type: ignore[arg-type]
            api_client=_FakeApiClient(safe_remote),  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        self.assertIs(
            safe_service.answer(
                _request("12345678910 베이비매직 목록")
            ),
            safe_remote,
        )

        unsafe_uris = (
            "https://example.invalid/results/one.jpg",
            "https://cdn-kr.mmtalkbox.com/results/one.jpg?token=x",
            "https://cdn-kr.mmtalkbox.com/results/one.jpg#token",
            "https://user@cdn-kr.mmtalkbox.com/results/one.jpg",
            "https://cdn-kr.mmtalkbox.com/results/x<y.jpg",
            "https://cdn-kr.mmtalkbox.com/results/x|y.jpg",
            "https://cdn-kr.mmtalkbox.com/results/x\x00y.jpg",
        )
        for uri in unsafe_uris:
            with self.subTest(uri=uri):
                remote = _result(
                    route="barcode_baby_ai_list",
                    sources=(
                        SourceReference(
                            source_id="unsafe",
                            title="unsafe",
                            uri=uri,
                        ),
                    ),
                )
                service = CompanyBarcodeResidualApiRolloutService(
                    _FakeLocalService(_result(route="local")),  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=_FakeApiClient(remote),  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                result = service.answer(
                    _request("12345678910 베이비매직 목록")
                )

                self.assertEqual(result.outcome, "failed")
                self.assertEqual(
                    result.fallback_reason,
                    "company_api_unsafe_source_host",
                )

    def test_barcode_residual_keeps_llm_action_and_dm_forbidden(self) -> None:
        settings = _settings(
            "local",
            barcode_residual_mode="remote",
        )
        unsafe_results = (
            _result(route="barcode_baby_ai_list", used_llm=True),
            _result(
                route="barcode_baby_ai_list",
                delivery_scope="requester",
            ),
            CompanyAssistantResult(
                route="barcode_baby_ai_list",
                outcome="answered",
                messages=(AssistantMessage(body="결과"),),
                suggested_action=SuggestedAction(
                    action="unsafe",
                    label="실행",
                ),
            ),
        )
        for remote in unsafe_results:
            with self.subTest(
                used_llm=remote.used_llm,
                scope=remote.messages[0].delivery_scope,
                has_action=remote.suggested_action is not None,
            ):
                service = CompanyBarcodeResidualApiRolloutService(
                    _FakeLocalService(_result(route="local")),  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=_FakeApiClient(remote),  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                result = service.answer(
                    _request("12345678910 베이비매직 목록")
                )

                self.assertEqual(result.outcome, "failed")
                self.assertTrue(
                    str(result.fallback_reason).startswith("company_api_")
                )

    def test_barcode_timeline_rollout_is_independent_and_allows_llm(
        self,
    ) -> None:
        settings = _settings(
            "local",
            barcode_residual_mode="local",
            barcode_timeline_mode="remote",
        )
        for question, route in (
            (
                "12345678910 마지막 녹화 날짜",
                "barcode last recordedAt",
            ),
            (
                "12345678910 2026-08-04 녹화됐어?",
                "barcode recordedAt-on-date",
            ),
        ):
            with self.subTest(route=route):
                remote = _result(
                    route=route,
                    body="원격 timeline 답변",
                    used_llm=True,
                )
                local = _FakeLocalService(_result(route="local"))
                api = _FakeApiClient(remote)
                service = CompanyBarcodeTimelineApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )
                request = _request(question)

                self.assertIs(service.answer(request), remote)
                self.assertEqual(api.requests, [request])
                self.assertEqual(api.route_groups, ["barcode"])
                self.assertEqual(local.requests, [])

        # 날짜를 포함해도 일반 조회·목록·개수와 Baby AI는 이 mode가
        # 선점하지 않고 원래 stage/별도 rollout에 남는다.
        for question in (
            "12345678910 2026-08-04 영상 조회",
            "12345678910 2026-08-04 영상 목록",
            "12345678910 2026-08-04 영상 개수",
            "12345678910 베이비매직 목록",
        ):
            with self.subTest(question=question):
                local_result = _result(route="local")
                local = _FakeLocalService(local_result)
                api = _FakeApiClient(
                    _result(route="barcode last recordedAt")
                )
                service = CompanyBarcodeTimelineApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                self.assertIs(service.answer(_request(question)), local_result)
                self.assertEqual(api.requests, [])

    def test_barcode_timeline_shadow_compares_llm_without_replacing_local(
        self,
    ) -> None:
        local_result = _result(
            route="barcode last recordedAt",
            body="로컬 답변",
        )
        remote = _result(
            route="barcode last recordedAt",
            body="원격 문장화 답변",
            used_llm=True,
        )
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(remote)
        runner = _CapturingShadowRunner()
        service = CompanyBarcodeTimelineApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings(
                "local",
                barcode_timeline_mode="shadow",
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )
        request = _request("12345678910 마지막 녹화 날짜")

        self.assertIs(service.answer(request), local_result)
        self.assertEqual(api.requests, [])
        self.assertEqual(len(runner.tasks), 1)
        with self.assertLogs(self.logger, level="INFO") as captured:
            runner.tasks[0]()
        self.assertEqual(api.requests, [request])
        self.assertIn("accepted=true", "\n".join(captured.output))
        self.assertIn("used_llm_match=False", "\n".join(captured.output))

    def test_barcode_timeline_rejects_sources_action_and_requester_dm(
        self,
    ) -> None:
        settings = _settings(
            "local",
            barcode_timeline_mode="remote",
        )
        unsafe_results = (
            _result(
                route="barcode last recordedAt",
                sources=(
                    SourceReference(
                        source_id="unexpected",
                        title="unexpected",
                        uri="https://cdn-kr.mmtalkbox.com/result.jpg",
                    ),
                ),
            ),
            _result(
                route="barcode last recordedAt",
                delivery_scope="requester",
            ),
            CompanyAssistantResult(
                route="barcode last recordedAt",
                outcome="answered",
                messages=(AssistantMessage(body="결과"),),
                suggested_action=SuggestedAction(
                    action="unsafe",
                    label="실행",
                ),
            ),
        )
        for remote in unsafe_results:
            with self.subTest(
                scope=remote.messages[0].delivery_scope,
                source_count=len(remote.sources),
                has_action=remote.suggested_action is not None,
            ):
                service = CompanyBarcodeTimelineApiRolloutService(
                    _FakeLocalService(_result(route="local")),  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=_FakeApiClient(remote),  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                result = service.answer(
                    _request("12345678910 마지막 녹화 날짜")
                )

                self.assertEqual(result.outcome, "failed")
                self.assertTrue(
                    str(result.fallback_reason).startswith("company_api_")
                )

    def test_barcode_timeline_local_wrapper_preserves_original_service(
        self,
    ) -> None:
        local = _FakeLocalService(_result(route="local"))
        api = _FakeApiClient(_result(route="barcode last recordedAt"))

        wrapped = wrap_company_barcode_timeline_service(
            local,  # type: ignore[arg-type]
            _settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )

        self.assertIs(wrapped, local)
        request = _request("12345678910 마지막 녹화 날짜")
        self.assertIs(wrapped.answer(request), local.result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_s3_rollout_routes_undated_needs_input_without_local_retry(
        self,
    ) -> None:
        cases = (
            (
                CompanyRecordingFailureApiRolloutService,
                "recording_failure_mode",
                "12345678910 녹화 실패 원인 분석",
                "recording_failure_analysis",
            ),
            (
                CompanyBarcodeLogApiRolloutService,
                "barcode_log_mode",
                "12345678910 로그 분석",
                "barcode_log_analysis",
            ),
        )
        for service_type, mode_key, undated, route in cases:
            with self.subTest(route=route):
                settings = _settings("local", **{mode_key: "remote"})
                remote = CompanyAssistantResult(
                    route=route,
                    outcome="needs_input",
                    messages=(
                        AssistantMessage(
                            body="병원명, 병실명, 날짜를 입력해줘"
                        ),
                    ),
                    fallback_reason="scope_required",
                )
                local_result = _result(route=route, body="로컬 결과")
                local = _FakeLocalService(local_result)
                api = _FakeApiClient(remote)
                service = service_type(
                    local,  # type: ignore[arg-type]
                    settings=settings,  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )

                self.assertIs(service.answer(_request(undated)), remote)
                self.assertEqual(local.requests, [])
                self.assertEqual(len(api.requests), 1)

    def test_barcode_log_shadow_preserves_partial_delivery_once(
        self,
    ) -> None:
        main = _result(
            route="barcode_log_analysis",
            body="확정 DB/S3 본문",
        )
        summary = _result(
            route="barcode_log_analysis",
            body="후속 LLM 요약",
            used_llm=True,
        )
        remote = _result(
            route="barcode_log_analysis",
            body="확정 DB/S3 본문",
            used_llm=True,
            extra_messages=(
                AssistantMessage(
                    body="후속 LLM 요약",
                    mention_actor=False,
                ),
            ),
        )
        local = _FakeProgressLocalService(main, summary)
        api = _FakeApiClient(remote)
        runner = _CapturingShadowRunner()
        service = CompanyBarcodeLogApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings(
                "local",
                barcode_log_mode="shadow",
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )
        partials: list[CompanyAssistantResult] = []
        request = _request("12345678910 2026-07-01 로그 분석")

        returned = service.answer_with_progress(
            request,
            partials.append,
        )

        self.assertIs(returned, summary)
        self.assertEqual(partials, [main])
        self.assertEqual(len(local.requests), 1)
        self.assertEqual(api.requests, [])
        self.assertEqual(len(runner.tasks), 1)
        with self.assertLogs(self.logger, level="INFO") as captured:
            runner.tasks[0]()
        self.assertEqual(api.requests, [request])
        self.assertIn("message_body_match=True", "\n".join(captured.output))


class CompanyOperationalReadApiRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def _device_settings(self, mode: str) -> SimpleNamespace:
        return _settings(
            "local",
            fallback_enabled=False,
            device_detail_mode=mode,
        )

    def _weekly_settings(self, mode: str) -> SimpleNamespace:
        return _settings(
            "local",
            fallback_enabled=False,
            weekly_summary_mode=mode,
        )

    def test_device_detail_local_wrapper_preserves_original_service(
        self,
    ) -> None:
        local = _FakeLocalService(_result(route="devices_filter"))
        api = _FakeApiClient(_result(route="device_detail"))

        wrapped = wrap_company_device_db_detail_service(
            local,  # type: ignore[arg-type]
            self._device_settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )
        request = _request("MB2-C00419 장비 정보")

        self.assertIs(wrapped, local)
        self.assertIs(wrapped.answer(request), local.result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_device_detail_remote_calls_single_turn_without_local(
        self,
    ) -> None:
        remote = _result(route="device_detail", body="원격 전체 상세")
        local = _FakeLocalService(_result(route="devices_filter"))
        api = _FakeApiClient(remote)
        wrapped = wrap_company_device_db_detail_service(
            local,  # type: ignore[arg-type]
            self._device_settings("remote"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )
        request = _request("MB2-C00419 장비 정보")

        self.assertIs(wrapped.answer(request), remote)
        self.assertEqual(local.requests, [])
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["device_detail"])

    def test_device_detail_remote_covers_every_non_count_filter(
        self,
    ) -> None:
        questions = (
            "deviceSeq=42 devices",
            "status=ACTIVE 장비 목록",
            "activeFlag=1 장비 목록",
            "installFlag=1 장비 목록",
            "병원=아이사랑산부인과 장비 목록",
            "병원=아이사랑산부인과 병실=2진료실 장비 목록",
        )
        for question in questions:
            with self.subTest(question=question):
                remote = _result(
                    route="devices_filter",
                    body="원격 live-enriched 장비 조회",
                )
                local = _FakeLocalService(
                    _result(route="devices_filter", body="로컬 조회")
                )
                api = _FakeApiClient(remote)
                service = CompanyDeviceDbDetailApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=self._device_settings("remote"),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )
                request = _request(question)

                self.assertIs(service.answer(request), remote)
                self.assertEqual(local.requests, [])
                self.assertEqual(api.requests, [request])
                self.assertEqual(api.route_groups, ["device_detail"])

    def test_device_detail_count_query_remains_on_count_rollout(
        self,
    ) -> None:
        local_result = _result(route="devices_filter")
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(_result(route="device_detail"))
        service = CompanyDeviceDbDetailApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._device_settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )
        request = _request("MB2-C00419 장비 몇 개야")

        self.assertIs(service.answer(request), local_result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_device_detail_live_query_uses_remote_without_local(self) -> None:
        remote = _result(route="device_detail")
        local = _FakeLocalService(_result(route="devices_filter"))
        api = _FakeApiClient(remote)
        service = CompanyDeviceDbDetailApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._device_settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )
        request = _request("MB2-C00419 장비 상태 확인")

        self.assertIs(service.answer(request), remote)
        self.assertEqual(local.requests, [])
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["device_detail"])

    def test_device_detail_remote_unsafe_results_fail_closed(
        self,
    ) -> None:
        unsafe_results = (
            (
                _result(route="device_detail", used_llm=True),
                "company_api_unexpected_llm",
            ),
            (
                _result(
                    route="device_detail",
                    sources=(
                        SourceReference(
                            source_id="unexpected",
                            title="unexpected",
                            uri="https://example.invalid/device",
                        ),
                    ),
                ),
                "company_api_unexpected_sources",
            ),
            (
                CompanyAssistantResult(
                    route="device_detail",
                    outcome="answered",
                    messages=(AssistantMessage(body="결과"),),
                    suggested_action=SuggestedAction(
                        action="unsafe",
                        label="실행",
                    ),
                ),
                "company_api_unsafe_action",
            ),
            (
                _result(
                    route="device_detail",
                    delivery_scope="requester",
                ),
                "company_api_unsafe_message_scope",
            ),
        )
        for remote, expected_reason in unsafe_results:
            with self.subTest(expected_reason=expected_reason):
                local = _FakeLocalService(
                    _result(route="devices_filter")
                )
                api = _FakeApiClient(remote)
                service = CompanyDeviceDbDetailApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=self._device_settings("remote"),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )
                request = _request("MB2-C00419 장비 정보")

                result = service.answer(request)

                self.assertEqual(result.outcome, "failed")
                self.assertEqual(result.fallback_reason, expected_reason)
                self.assertEqual(local.requests, [])
                self.assertEqual(api.requests, [request])

    def test_device_detail_availability_never_replays_local_probe(
        self,
    ) -> None:
        # stale fallback 설정이 남아 있어도 tunnel lifecycle을 가진 Slack
        # legacy enrichment로 cutover가 되돌아가지 않게 한다.
        local = _FakeLocalService(_result(route="devices_filter"))
        api = _FakeApiClient(
            error=CompanyApiAvailabilityError("service_not_ready")
        )
        service = CompanyDeviceDbDetailApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_settings(
                "local",
                device_detail_mode="remote",
                device_detail_fallback_enabled=True,
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        result = service.answer(_request("MB2-C00419 장비 정보"))

        self.assertEqual(result.outcome, "failed")
        self.assertEqual(
            result.fallback_reason,
            "company_api_availability",
        )
        self.assertEqual(local.requests, [])
        self.assertEqual(len(api.requests), 1)
        self.assertEqual(api.route_groups, ["device_detail"])

    def test_device_detail_ambiguous_turn_never_calls_local(
        self,
    ) -> None:
        request = _request("MB2-C00419 장비 정보")
        local = _FakeLocalService(_result(route="devices_filter"))
        api = _FakeApiClient(
            error=CompanyApiAmbiguousTimeoutError("ambiguous"),
        )
        service = CompanyDeviceDbDetailApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._device_settings("remote"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )

        result = service.answer(request)

        self.assertEqual(result.outcome, "failed")
        self.assertEqual(
            result.fallback_reason,
            "company_api_ambiguous_timeout",
        )
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["device_detail"])
        self.assertEqual(local.requests, [])

    def test_device_detail_shadow_returns_local_without_api_call(
        self,
    ) -> None:
        local_result = _result(route="devices_filter", body="로컬 상세")
        remote = _result(route="device_detail", body="로컬 상세")
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(remote)
        runner = _CapturingShadowRunner()
        service = CompanyDeviceDbDetailApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=self._device_settings("shadow"),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner,
        )
        request = _request("MB2-C00419 장비 정보")

        self.assertIs(service.answer(request), local_result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])
        self.assertEqual(api.route_groups, [])
        self.assertEqual(runner.tasks, [])

    def test_weekly_local_wrapper_preserves_legacy_service(self) -> None:
        local = _FakeLocalService(None)
        api = _FakeApiClient(
            _result(route="weekly_recordings_summary")
        )
        wrapped = wrap_company_weekly_summary_service(
            local,  # type: ignore[arg-type]
            self._weekly_settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )
        request = _request("지난주 초음파 영상 현황")

        self.assertIs(wrapped, local)
        self.assertIsNone(wrapped.answer(request))
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_weekly_remote_calls_api_once_without_legacy_local(
        self,
    ) -> None:
        remote = _result(
            route="weekly_recordings_summary",
            body="원격 주간 요약",
        )
        local = _FakeLocalService(None)
        api = _FakeApiClient(remote)
        wrapped = wrap_company_weekly_summary_service(
            local,  # type: ignore[arg-type]
            self._weekly_settings("remote"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
            shadow_runner=_CapturingShadowRunner(),
        )
        request = _request("지난주 초음파 영상 현황")

        self.assertIs(wrapped.answer(request), remote)
        self.assertEqual(local.requests, [])
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["structured"])

    def test_weekly_shadow_compares_and_returns_same_local_summary_once(
        self,
    ) -> None:
        local = _FakeLocalService(None)
        api = _FakeApiClient(
            _result(
                route="weekly_recordings_summary",
                body="원격 주간 요약",
            )
        )
        runner = _CapturingShadowRunner()
        wrapped = wrap_company_weekly_summary_service(
            local,  # type: ignore[arg-type]
            self._weekly_settings("shadow"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
            shadow_runner=runner,
        )
        request = _request("지난주 초음파 영상 현황")

        # 비교용 DB 요약을 그대로 사용자 결과로 반환해 legacy handler가
        # 동일 집계를 다시 실행하지 않게 한다.
        with (
            patch(
                "boxer_company.assistant.operational_read_routes."
                "_build_weekly_recordings_report_summary",
                return_value={"totalCount": 3},
            ) as summary_builder,
            patch(
                "boxer_company.assistant.operational_read_routes."
                "_format_weekly_recordings_report",
                return_value="*주간 초음파 촬영 요약*",
            ),
        ):
            returned = wrapped.answer(request)

        self.assertIsNotNone(returned)
        self.assertEqual(returned.route, "weekly_recordings_summary")
        summary_builder.assert_called_once()
        self.assertEqual(local.requests, [])
        self.assertEqual(api.requests, [])
        self.assertEqual(len(runner.tasks), 1)

        runner.tasks[0]()

        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["structured"])

    def test_weekly_route_drift_and_unsafe_results_fail_closed(
        self,
    ) -> None:
        remote_results = (
            (
                _result(route="device_db_detail"),
                "company_api_unexpected_route",
            ),
            (
                _result(
                    route="weekly_recordings_summary",
                    used_llm=True,
                ),
                "company_api_unexpected_llm",
            ),
            (
                _result(
                    route="weekly_recordings_summary",
                    sources=(
                        SourceReference(
                            source_id="unexpected",
                            title="unexpected",
                            uri="https://example.invalid/weekly",
                        ),
                    ),
                ),
                "company_api_unexpected_sources",
            ),
            (
                CompanyAssistantResult(
                    route="weekly_recordings_summary",
                    outcome="answered",
                    messages=(AssistantMessage(body="결과"),),
                    suggested_action=SuggestedAction(
                        action="unsafe",
                        label="실행",
                    ),
                ),
                "company_api_unsafe_action",
            ),
            (
                _result(
                    route="weekly_recordings_summary",
                    delivery_scope="requester",
                ),
                "company_api_unsafe_message_scope",
            ),
        )
        for remote, expected_reason in remote_results:
            with self.subTest(expected_reason=expected_reason):
                local = _FakeLocalService(None)
                api = _FakeApiClient(remote)
                service = CompanyWeeklySummaryApiRolloutService(
                    local,  # type: ignore[arg-type]
                    settings=self._weekly_settings("remote"),  # type: ignore[arg-type]
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                    shadow_runner=_CapturingShadowRunner(),
                )
                request = _request("지난주 초음파 영상 현황")

                result = service.answer(request)

                self.assertEqual(result.outcome, "failed")
                self.assertEqual(result.fallback_reason, expected_reason)
                self.assertEqual(local.requests, [])
                self.assertEqual(api.requests, [request])


class CompanyStructuredApiRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def _service(
        self,
        local: _FakeLocalService,
        api: _FakeApiClient,
        *,
        mode: str,
        fallback_enabled: bool = False,
        runner: _CapturingShadowRunner | None = None,
    ) -> CompanyStructuredApiRolloutService:
        return CompanyStructuredApiRolloutService(
            local,  # type: ignore[arg-type]
            settings=_structured_settings(
                mode,
                fallback_enabled=fallback_enabled,
            ),  # type: ignore[arg-type]
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            shadow_runner=runner or _CapturingShadowRunner(),
        )

    def test_structured_local_mode_returns_original_service_without_api(
        self,
    ) -> None:
        local = _FakeLocalService(
            _result(route="hospital_rooms_filter")
        )
        api = _FakeApiClient(
            _result(route="hospital_rooms_filter")
        )

        wrapped = wrap_company_structured_service(
            local,  # type: ignore[arg-type]
            _structured_settings("local"),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
        )

        self.assertIs(wrapped, local)
        returned = wrapped.answer(
            _request("병원명 서울병원 병실 목록")
        )
        self.assertIs(returned, local.result)
        self.assertEqual(len(local.requests), 1)
        self.assertEqual(api.requests, [])

    def test_structured_remote_accepts_only_the_four_db_routes(
        self,
    ) -> None:
        cases = (
            ("병원명 서울병원 병원 조회", "hospitals_filter"),
            (
                "병원명 서울병원 병실 목록",
                "hospital_rooms_filter",
            ),
            (
                "12345678910 2026-07-01 초음파 캡처 조회",
                "ultrasound_captures_filter",
            ),
            (
                "12345678910 2026-07-01 영상 조회",
                "recordings_filter",
            ),
        )
        for question, route in cases:
            with self.subTest(route=route):
                remote = _result(route=route, body=f"{route} 원격 결과")
                local = _FakeLocalService(
                    _result(route=route, body="로컬 결과")
                )
                api = _FakeApiClient(remote)
                service = self._service(
                    local,
                    api,
                    mode="remote",
                )
                request = _request(question)

                returned = service.answer(request)

                self.assertIs(returned, remote)
                self.assertEqual(local.requests, [])
                self.assertEqual(api.requests, [request])

    def test_structured_unrelated_device_weekly_and_restore_never_call_api(
        self,
    ) -> None:
        cases = (
            (
                "회사 노션에서 Commerce 찾아줘",
                _result(route="company_notion_qa"),
            ),
            (
                "MB2-B00045 장비정보",
                _result(route="devices_filter"),
            ),
            ("이번 주 영상 현황 리포트", None),
            ("35033165423 2024년 4월 영상 복원", None),
        )
        for question, local_result in cases:
            with self.subTest(question=question):
                local = _FakeLocalService(local_result)
                api = _FakeApiClient(
                    _result(route="hospital_rooms_filter")
                )
                service = self._service(
                    local,
                    api,
                    mode="remote",
                )

                returned = service.answer(_request(question))

                self.assertIs(returned, local_result)
                self.assertEqual(len(local.requests), 1)
                self.assertEqual(api.requests, [])

    def test_structured_shadow_returns_local_once_and_logs_body_match_only(
        self,
    ) -> None:
        local_result = _result(
            route="hospital_rooms_filter",
            body="LOCAL-SECRET-STRUCTURED-BODY",
        )
        remote_result = _result(
            route="hospital_rooms_filter",
            body="REMOTE-SECRET-STRUCTURED-BODY",
        )
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(remote_result)
        runner = _CapturingShadowRunner()
        service = self._service(
            local,
            api,
            mode="shadow",
            runner=runner,
        )
        request = _request("병원명 SECRET병원 병실 목록")

        returned = service.answer(request)

        self.assertIs(returned, local_result)
        self.assertEqual(len(local.requests), 1)
        self.assertEqual(api.requests, [])
        self.assertEqual(len(runner.tasks), 1)

        with self.assertLogs(self.logger, level="INFO") as captured:
            runner.tasks[0]()

        self.assertEqual(api.requests, [request])
        logs = "\n".join(captured.output)
        self.assertIn("message_body_match=False", logs)
        for secret in (
            "SECRET병원",
            "LOCAL-SECRET-STRUCTURED-BODY",
            "REMOTE-SECRET-STRUCTURED-BODY",
        ):
            self.assertNotIn(secret, logs)

    def test_structured_remote_availability_fallback_is_explicit(
        self,
    ) -> None:
        for fallback_enabled in (False, True):
            with self.subTest(fallback_enabled=fallback_enabled):
                local = _FakeLocalService(
                    _result(
                        route="hospital_rooms_filter",
                        body="로컬 fallback",
                    )
                )
                api = _FakeApiClient(
                    error=CompanyApiAvailabilityError(
                        "service_not_ready"
                    )
                )
                service = self._service(
                    local,
                    api,
                    mode="remote",
                    fallback_enabled=fallback_enabled,
                )

                returned = service.answer(
                    _request("병원명 서울병원 병실 목록")
                )

                if fallback_enabled:
                    self.assertIs(returned, local.result)
                    self.assertEqual(len(local.requests), 1)
                else:
                    self.assertEqual(returned.outcome, "failed")
                    self.assertTrue(
                        str(returned.fallback_reason).startswith(
                            "company_api_"
                        )
                    )
                    self.assertEqual(local.requests, [])

    def test_structured_policy_contract_and_timeout_fail_closed(
        self,
    ) -> None:
        errors = (
            CompanyApiPolicyError("caller_not_allowed"),
            CompanyApiContractError("invalid_response"),
            CompanyApiAmbiguousTimeoutError("read_timeout"),
        )
        for error in errors:
            with self.subTest(error=type(error).__name__):
                local = _FakeLocalService(
                    _result(route="hospital_rooms_filter")
                )
                api = _FakeApiClient(error=error)
                service = self._service(
                    local,
                    api,
                    mode="remote",
                    fallback_enabled=True,
                )

                returned = service.answer(
                    _request("병원명 서울병원 병실 목록")
                )

                self.assertEqual(returned.outcome, "failed")
                self.assertTrue(
                    str(returned.fallback_reason).startswith(
                        "company_api_"
                    )
                )
                self.assertEqual(local.requests, [])

    def test_structured_route_drift_never_uses_local_fallback(
        self,
    ) -> None:
        remote_results = (
            _result(route="recordings_filter"),
            _result(route="unhandled", outcome="no_evidence"),
            None,
        )
        for remote_result in remote_results:
            with self.subTest(
                route=getattr(remote_result, "route", None)
            ):
                local = _FakeLocalService(
                    _result(route="hospital_rooms_filter")
                )
                api = _FakeApiClient(remote_result)
                service = self._service(
                    local,
                    api,
                    mode="remote",
                    fallback_enabled=True,
                )

                returned = service.answer(
                    _request("병원명 서울병원 병실 목록")
                )

                self.assertEqual(returned.outcome, "failed")
                self.assertIn(
                    returned.fallback_reason,
                    {
                        "company_api_route_mismatch",
                        "company_api_unexpected_route",
                    },
                )
                self.assertEqual(local.requests, [])

    def test_structured_sources_requester_scope_and_llm_fail_closed(
        self,
    ) -> None:
        unsafe_results = (
            _result(
                route="hospital_rooms_filter",
                sources=(
                    SourceReference(
                        source_id="unexpected-source",
                        title="구조화 조회에 없어야 할 출처",
                        uri="https://app.notion.com/p/unexpected",
                    ),
                ),
            ),
            _result(
                route="hospital_rooms_filter",
                delivery_scope="requester",
            ),
            _result(
                route="hospital_rooms_filter",
                used_llm=True,
            ),
        )
        for unsafe_result in unsafe_results:
            with self.subTest(
                delivery_scope=(
                    unsafe_result.messages[0].delivery_scope
                ),
                source_count=len(unsafe_result.sources),
                used_llm=unsafe_result.used_llm,
            ):
                local = _FakeLocalService(
                    _result(route="hospital_rooms_filter")
                )
                api = _FakeApiClient(unsafe_result)
                service = self._service(
                    local,
                    api,
                    mode="remote",
                )

                returned = service.answer(
                    _request("병원명 서울병원 병실 목록")
                )

                self.assertEqual(returned.outcome, "failed")
                self.assertTrue(
                    str(returned.fallback_reason).startswith(
                        "company_api_"
                    )
                )
                self.assertEqual(local.requests, [])
                self.assertTrue(
                    all(
                        message.delivery_scope == "conversation"
                        for message in returned.messages
                    )
                )


class CompanyOperationsApiRolloutTests(unittest.TestCase):
    _ALLOWED_ROUTES = frozenset({"device_update_operation"})

    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.handlers.clear()
        self.logger.addHandler(logging.NullHandler())
        self.logger.propagate = False

    @staticmethod
    def _matcher(request: CompanyAssistantRequest) -> str | None:
        return (
            "device_update_operation"
            if "업데이트" in request.question
            else None
        )

    def _wrap(
        self,
        local: _FakeLocalService,
        api: _FakeApiClient,
        *,
        mode: str = "remote",
        fallback_enabled: bool = False,
    ) -> Any:
        return wrap_company_operations_service(
            local,  # type: ignore[arg-type]
            _settings(
                "local",
                fallback_enabled=False,
                operations_mode=mode,
                operations_fallback_enabled=fallback_enabled,
            ),  # type: ignore[arg-type]
            api,  # type: ignore[arg-type]
            self.logger,
            self._matcher,
            self._ALLOWED_ROUTES,
            _CapturingShadowRunner(),
        )

    def test_local_mode_returns_original_service(self) -> None:
        local = _FakeLocalService(
            _result(route="device_update_operation")
        )
        api = _FakeApiClient(
            _result(route="device_update_operation")
        )

        wrapped = self._wrap(local, api, mode="local")

        self.assertIs(wrapped, local)
        self.assertIsNotNone(wrapped.answer(_request("장비 업데이트")))
        self.assertEqual(len(local.requests), 1)
        self.assertEqual(api.requests, [])

    def test_remote_match_calls_operations_api_once_without_local(
        self,
    ) -> None:
        request = _request("장비 업데이트")
        remote = _result(
            route="device_update_operation",
            delivery_scope="requester",
        )
        local = _FakeLocalService(remote)
        api = _FakeApiClient(remote)

        wrapped = self._wrap(local, api)
        result = wrapped.answer(request)

        self.assertIsInstance(
            wrapped,
            CompanyOperationsApiRolloutService,
        )
        self.assertIs(result, remote)
        self.assertEqual(
            result.messages[0].delivery_scope,
            "requester",
        )
        self.assertEqual(api.requests, [request])
        self.assertEqual(api.route_groups, ["operations"])
        self.assertEqual(local.requests, [])

    def test_non_matching_request_stays_local_without_api(self) -> None:
        request = _request("장비 정보 보여줘")
        local_result = _result(route="devices_filter")
        local = _FakeLocalService(local_result)
        api = _FakeApiClient(
            _result(route="device_update_operation")
        )

        wrapped = self._wrap(local, api)

        self.assertIs(wrapped.answer(request), local_result)
        self.assertEqual(local.requests, [request])
        self.assertEqual(api.requests, [])

    def test_remote_errors_never_replay_local_operation(self) -> None:
        errors = (
            CompanyApiAvailabilityError("unavailable"),
            CompanyApiAmbiguousTimeoutError("ambiguous"),
            CompanyApiPolicyError("policy"),
            CompanyApiContractError("contract"),
            RuntimeError("unexpected"),
        )
        for error in errors:
            with self.subTest(error_type=type(error).__name__):
                local = _FakeLocalService(
                    _result(route="device_update_operation")
                )
                api = _FakeApiClient(error=error)
                wrapped = self._wrap(local, api)

                result = wrapped.answer(_request("장비 업데이트"))

                self.assertIsNotNone(result)
                self.assertEqual(result.outcome, "failed")
                self.assertEqual(local.requests, [])
                self.assertEqual(len(api.requests), 1)
                self.assertEqual(api.route_groups, ["operations"])

    def test_remote_route_mismatch_fails_without_local_fallback(
        self,
    ) -> None:
        local = _FakeLocalService(
            _result(route="device_update_operation")
        )
        api = _FakeApiClient(_result(route="another_operation"))
        wrapped = self._wrap(local, api)

        result = wrapped.answer(_request("장비 업데이트"))

        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(
            result.fallback_reason,
            "company_api_unexpected_route",
        )
        self.assertEqual(local.requests, [])

    def test_shadow_and_fallback_settings_are_rejected(self) -> None:
        for mode, fallback_enabled in (
            ("shadow", False),
            ("remote", True),
        ):
            with self.subTest(
                mode=mode,
                fallback_enabled=fallback_enabled,
            ):
                with self.assertRaises(CompanyApiContractError):
                    self._wrap(
                        _FakeLocalService(None),
                        _FakeApiClient(),
                        mode=mode,
                        fallback_enabled=fallback_enabled,
                    )


if __name__ == "__main__":
    unittest.main()
