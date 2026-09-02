from __future__ import annotations

import inspect
import logging
from typing import Any, Callable
import unittest

from boxer_company.assistant import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company_adapter_slack import company_api_rollout
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAvailabilityError,
    CompanyApiContractError,
)
from boxer_company_adapter_slack.company_api_rollout import (
    CompanyBarcodeApiRolloutService,
    CompanyBarcodeFreeformApiRolloutService,
    CompanyBarcodeLogApiRolloutService,
    CompanyBarcodeResidualApiRolloutService,
    CompanyBarcodeTimelineApiRolloutService,
    CompanyDeviceApiRolloutService,
    CompanyDeviceDbDetailApiRolloutService,
    CompanyDeviceFilterApiRolloutService,
    CompanyFreeformApiRolloutService,
    CompanyNotionApiRolloutService,
    CompanyOperationsApiRolloutService,
    CompanyPlaybookApiRolloutService,
    CompanyRecordingFailureApiRolloutService,
    CompanyStructuredApiRolloutService,
    CompanyUsageHelpApiRolloutService,
    CompanyWeeklySummaryApiRolloutService,
    wrap_company_notion_service,
)


def _request(question: str) -> CompanyAssistantRequest:
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
    route: str,
    *,
    used_llm: bool = False,
    delivery_scope: str = "conversation",
    sources: tuple[SourceReference, ...] = (),
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome="answered",
        messages=(
            AssistantMessage(
                body=f"{route} 답변",
                delivery_scope=delivery_scope,  # type: ignore[arg-type]
            ),
        ),
        sources=sources,
        used_llm=used_llm,
    )


class _NextService:
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


class _ApiClient:
    def __init__(
        self,
        result: CompanyAssistantResult | None,
        *,
        error: Exception | None = None,
        partial: CompanyAssistantResult | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.partial = partial
        self.calls: list[tuple[CompanyAssistantRequest, str | None]] = []
        self.progress_calls: list[
            tuple[CompanyAssistantRequest, str | None]
        ] = []

    def answer(
        self,
        request: CompanyAssistantRequest,
        *,
        route_group: str | None = None,
    ) -> CompanyAssistantResult | None:
        self.calls.append((request, route_group))
        if self.error is not None:
            raise self.error
        return self.result

    def answer_with_progress(
        self,
        request: CompanyAssistantRequest,
        *,
        route_group: str | None = None,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult | None:
        self.progress_calls.append((request, route_group))
        if self.error is not None:
            raise self.error
        if self.partial is not None:
            on_partial_result(self.partial)
        return self.result


class CompanyApiRemoteRolloutTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        self.logger.disabled = True

    def _service_cases(
        self,
    ) -> tuple[
        tuple[
            type[Any],
            str,
            str,
            str,
            bool,
        ],
        ...,
    ]:
        # 실제 pure matcher 예시를 하나씩 통과시켜 15개 route wrapper가
        # 고정 routeGroup만 API로 보내는지 검증한다.
        return (
            (
                CompanyNotionApiRolloutService,
                "회사 노션에서 Commerce 찾아줘",
                "company_notion_qa",
                "notion",
                False,
            ),
            (
                CompanyStructuredApiRolloutService,
                "병원명 서울병원 병원 조회",
                "hospitals_filter",
                "structured",
                False,
            ),
            (
                CompanyDeviceApiRolloutService,
                "MB2-B00045 2026-07-01 LED 로그 분석",
                "device_led_log_analysis",
                "device",
                False,
            ),
            (
                CompanyDeviceFilterApiRolloutService,
                "활성 장비 몇 개야?",
                "devices_filter",
                "structured",
                False,
            ),
            (
                CompanyDeviceDbDetailApiRolloutService,
                "MB2-B00045 장비정보",
                "device_detail",
                "device_detail",
                False,
            ),
            (
                CompanyWeeklySummaryApiRolloutService,
                "지난주 초음파 영상 현황",
                "weekly_recordings_summary",
                "structured",
                False,
            ),
            (
                CompanyRecordingFailureApiRolloutService,
                "12345678910 2026-07-01 녹화 실패 원인 분석",
                "recording_failure_analysis",
                "failure",
                False,
            ),
            (
                CompanyBarcodeLogApiRolloutService,
                "MB2-C00419 2026-07-01 12345678910 로그 분석",
                "barcode_log_analysis",
                "log",
                False,
            ),
            (
                CompanyBarcodeApiRolloutService,
                "12345678910 영상 개수",
                "barcode_video_count",
                "barcode",
                False,
            ),
            (
                CompanyPlaybookApiRolloutService,
                "녹화 취소 음성 운영 문서로 알려줘",
                "notion_playbook_qa",
                "knowledge",
                False,
            ),
            (
                CompanyBarcodeFreeformApiRolloutService,
                "12345678910 녹화 기록들 사이 간격이 일정한지 설명해줘",
                "barcode_evidence_freeform",
                "knowledge",
                True,
            ),
            (
                CompanyFreeformApiRolloutService,
                "안녕?",
                "company_freeform",
                "freeform",
                True,
            ),
            (
                CompanyBarcodeResidualApiRolloutService,
                "12345678910 베이비매직 목록",
                "barcode_baby_ai_list",
                "barcode",
                False,
            ),
            (
                CompanyBarcodeTimelineApiRolloutService,
                "12345678910 마지막 녹화 날짜",
                "barcode last recordedAt",
                "barcode",
                True,
            ),
        )

    def test_each_read_wrapper_calls_only_its_remote_group(self) -> None:
        for (
            service_type,
            question,
            route,
            route_group,
            used_llm,
        ) in self._service_cases():
            with self.subTest(service=service_type.__name__):
                remote = _result(route, used_llm=used_llm)
                api = _ApiClient(remote)
                service = service_type(
                    None,
                    api_client=api,  # type: ignore[arg-type]
                    logger=self.logger,
                )
                request = _request(question)

                self.assertIs(service.answer(request), remote)
                self.assertEqual(api.calls, [(request, route_group)])

    def test_operations_wrapper_is_remote_only_and_supports_progress(
        self,
    ) -> None:
        partial = _result("device_box_update")
        final = _result(
            "device_box_update",
            delivery_scope="requester",
        )
        api = _ApiClient(final, partial=partial)
        service = CompanyOperationsApiRolloutService(
            None,
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
            matcher=lambda _request: "device_box_update",
            allowed_routes=frozenset({"device_box_update"}),
        )
        request = _request("MB2-B00045 박스 업데이트")
        seen: list[CompanyAssistantResult] = []

        self.assertIs(
            service.answer_with_progress(request, seen.append),
            final,
        )
        self.assertEqual(seen, [partial])
        self.assertEqual(
            api.progress_calls,
            [(request, "operations")],
        )

    def test_unmatched_request_delegates_without_http(self) -> None:
        fallback = _result("next_remote_stage")
        next_service = _NextService(fallback)
        api = _ApiClient(_result("company_notion_qa"))
        service = CompanyNotionApiRolloutService(
            next_service,
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
        )
        request = _request("안녕?")

        self.assertIs(service.answer(request), fallback)
        self.assertEqual(next_service.requests, [request])
        self.assertEqual(api.calls, [])

    def test_freeform_runs_remote_precedence_before_final_api(self) -> None:
        precedence = _result("notion_playbook_qa")
        next_service = _NextService(precedence)
        api = _ApiClient(_result("company_freeform", used_llm=True))
        service = CompanyFreeformApiRolloutService(
            next_service,
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
        )
        request = _request("운영 문서로 알려줘")

        self.assertIs(service.answer(request), precedence)
        self.assertEqual(next_service.requests, [request])
        self.assertEqual(api.calls, [])

    def test_usage_help_is_remote_deterministic_before_final_freeform(
        self,
    ) -> None:
        request = _request("사용법")
        usage_result = CompanyAssistantResult(
            route="usage_help",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body="지원 기능 안내",
                    mention_actor=False,
                ),
            ),
        )
        usage_api = _ApiClient(usage_result)
        usage_service = CompanyUsageHelpApiRolloutService(
            None,
            api_client=usage_api,  # type: ignore[arg-type]
            logger=self.logger,
        )
        freeform_api = _ApiClient(
            _result("company_freeform", used_llm=True)
        )
        service = CompanyFreeformApiRolloutService(
            usage_service,
            api_client=freeform_api,  # type: ignore[arg-type]
            logger=self.logger,
        )

        self.assertIs(service.answer(request), usage_result)
        self.assertEqual(usage_api.calls, [(request, "freeform")])
        self.assertEqual(freeform_api.calls, [])

    def test_usage_help_rejects_non_transport_presentation(self) -> None:
        invalid_results = (
            _result("usage_help"),
            CompanyAssistantResult(
                route="usage_help",
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body="지원 기능 안내",
                        mention_actor=False,
                    ),
                    AssistantMessage(
                        body="중복 안내",
                        mention_actor=False,
                    ),
                ),
            ),
        )
        for remote in invalid_results:
            with self.subTest(messages=len(remote.messages)):
                service = CompanyUsageHelpApiRolloutService(
                    None,
                    api_client=_ApiClient(remote),  # type: ignore[arg-type]
                    logger=self.logger,
                )

                result = service.answer(_request("사용법"))

                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(result.outcome, "failed")
                self.assertEqual(
                    result.fallback_reason,
                    "company_api_invalid_presentation",
                )

    def test_api_failure_never_calls_next_service(self) -> None:
        next_service = _NextService(_result("local_fallback"))
        api = _ApiClient(
            None,
            error=CompanyApiAvailabilityError("unavailable"),
        )
        service = CompanyNotionApiRolloutService(
            next_service,
            api_client=api,  # type: ignore[arg-type]
            logger=self.logger,
        )

        result = service.answer(
            _request("회사 노션에서 Commerce 찾아줘")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(
            result.fallback_reason,
            "company_api_availability",
        )
        self.assertEqual(next_service.requests, [])

    def test_route_drift_and_unsafe_response_fail_closed(self) -> None:
        cases = (
            _result("company_notion_search", delivery_scope="requester"),
            _result(
                "company_notion_search",
                sources=(
                    SourceReference(
                        source_id="x",
                        title="x",
                        uri="https://example.invalid/x",
                    ),
                ),
            ),
            _result("company_freeform"),
        )
        for remote in cases:
            with self.subTest(route=remote.route):
                service = CompanyNotionApiRolloutService(
                    None,
                    api_client=_ApiClient(remote),  # type: ignore[arg-type]
                    logger=self.logger,
                )
                result = service.answer(
                    _request("회사 노션에서 Commerce 찾아줘")
                )

                self.assertIsNotNone(result)
                assert result is not None
                self.assertEqual(result.outcome, "failed")
                self.assertTrue(
                    str(result.fallback_reason).startswith(
                        "company_api_"
                    )
                )

    def test_wrapper_contract_has_no_rollout_or_shadow_parameters(
        self,
    ) -> None:
        self.assertFalse(
            hasattr(company_api_rollout, "BoundedShadowRunner")
        )
        for callable_object in (
            CompanyNotionApiRolloutService,
            wrap_company_notion_service,
        ):
            parameter_names = set(
                inspect.signature(callable_object).parameters
            )
            self.assertNotIn("settings", parameter_names)
            self.assertNotIn("shadow_runner", parameter_names)
            self.assertNotIn("fallback_enabled", parameter_names)

    def test_operation_route_allowlist_must_be_concrete(self) -> None:
        with self.assertRaisesRegex(
            CompanyApiContractError,
            "company_api_operations_routes_invalid",
        ):
            CompanyOperationsApiRolloutService(
                None,
                api_client=_ApiClient(None),  # type: ignore[arg-type]
                logger=self.logger,
                matcher=lambda _request: None,
                allowed_routes=frozenset(),
            )


if __name__ == "__main__":
    unittest.main()
