from __future__ import annotations

import logging
from types import SimpleNamespace
import threading
import time
from typing import Callable
import unittest

from boxer_company.assistant import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
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
    CompanyBarcodeLogApiRolloutService,
    CompanyDeviceApiRolloutService,
    CompanyDeviceFilterApiRolloutService,
    CompanyNotionApiRolloutService,
    CompanyRecordingFailureApiRolloutService,
    CompanyStructuredApiRolloutService,
    wrap_company_notion_service,
    wrap_company_structured_service,
)


def _request(
    question: str = "회사 노션에서 Commerce 찾아줘",
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="slack:T1:C1:1.0",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="1.0",
        question=question,
        locale="ko",
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
    recording_failure_mode: str = "local",
    recording_failure_fallback_enabled: bool = False,
    barcode_log_mode: str = "local",
    barcode_log_fallback_enabled: bool = False,
    barcode_mode: str = "local",
    barcode_fallback_enabled: bool = False,
) -> SimpleNamespace:
    # rollout 단위 테스트는 transport 설정과 분리해 전환 필드만 고정한다.
    return SimpleNamespace(
        notion_mode=mode,
        notion_fallback_enabled=fallback_enabled,
        structured_mode=structured_mode,
        structured_fallback_enabled=structured_fallback_enabled,
        device_mode=device_mode,
        device_fallback_enabled=device_fallback_enabled,
        recording_failure_mode=recording_failure_mode,
        recording_failure_fallback_enabled=(
            recording_failure_fallback_enabled
        ),
        barcode_log_mode=barcode_log_mode,
        barcode_log_fallback_enabled=barcode_log_fallback_enabled,
        barcode_mode=barcode_mode,
        barcode_fallback_enabled=barcode_fallback_enabled,
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

    def answer(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        self.requests.append(request)
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

    def test_remote_unexpected_or_unhandled_route_uses_local(
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

                self.assertIs(returned, local.result)
                self.assertEqual(len(local.requests), 1)

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

    def test_s3_rollout_requires_an_explicit_date(self) -> None:
        cases = (
            (
                CompanyRecordingFailureApiRolloutService,
                "recording_failure_mode",
                "12345678910 녹화 실패 원인 분석",
                "12345678910 2026-07-01 녹화 실패 원인 분석",
                "recording_failure_analysis",
            ),
            (
                CompanyBarcodeLogApiRolloutService,
                "barcode_log_mode",
                "12345678910 로그 분석",
                "12345678910 2026-07-01 로그 분석",
                "barcode_log_analysis",
            ),
        )
        for service_type, mode_key, undated, dated, route in cases:
            with self.subTest(route=route):
                settings = _settings("local", **{mode_key: "remote"})
                remote = _result(
                    route=route,
                    body="원격 S3 결과",
                    used_llm=True,
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

                self.assertIs(service.answer(_request(undated)), local_result)
                self.assertEqual(api.requests, [])
                self.assertIs(service.answer(_request(dated)), remote)
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


if __name__ == "__main__":
    unittest.main()
