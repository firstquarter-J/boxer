from __future__ import annotations

import logging
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from boxer import AnswerRequest, AnswerResult
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerComposerDeps,
)
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_led_routes import (
    DeviceLedLogAssistantRoute,
    DeviceLedPatternGuideAssistantRoute,
)
from boxer_company.assistant.service import CompanyAssistantService


def _request(
    question: str,
    *,
    metadata: dict | None = None,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-LED-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
        metadata=metadata or {},
    )


def _quiet_logger() -> logging.Logger:
    logger = logging.Logger("test.company.device_led_routes")
    logger.disabled = True
    return logger


class _FakeAnswerEngine:
    def __init__(
        self,
        result: AnswerResult,
        *,
        provider: str = "claude",
    ) -> None:
        self.provider = provider
        self._result = result
        self.requests: list[AnswerRequest] = []

    def answer(self, request: AnswerRequest) -> AnswerResult:
        self.requests.append(request)
        return self._result


def _composer(
    engine: _FakeAnswerEngine,
    *,
    synthesis_enabled: bool = True,
) -> CompanyEvidenceAnswerComposer:
    return CompanyEvidenceAnswerComposer(
        CompanyEvidenceAnswerComposerDeps(
            answer_engine=engine,  # type: ignore[arg-type]
            synthesis_enabled=synthesis_enabled,
            provider_ready=lambda: True,
            actor_allowed_for_llm=lambda actor_id: True,
        ),
        logger=_quiet_logger(),
    )


class DeviceLedLogAssistantRouteTests(unittest.TestCase):
    def _route(
        self,
        *,
        enabled: bool = True,
        s3_calls: list[str] | None = None,
    ) -> DeviceLedLogAssistantRoute:
        calls = s3_calls if s3_calls is not None else []
        return DeviceLedLogAssistantRoute(
            lambda: calls.append("get") or "s3-client",
            s3_enabled=enabled,
            logger=_quiet_logger(),
        )

    def test_unrelated_question_returns_none(self) -> None:
        self.assertIsNone(
            self._route().handle(_request("오늘 촬영 영상 개수 알려줘"))
        )

    def test_missing_and_invalid_dates_are_distinct_input_outcomes(self) -> None:
        missing = self._route().handle(
            _request("MB2-C00570 LED 로그 확인")
        )
        invalid = self._route().handle(
            _request("MB2-C00570 2026-99-04 LED 로그 확인")
        )

        self.assertEqual(missing.outcome, "needs_input")
        self.assertEqual(missing.fallback_reason, "missing_date")
        self.assertIn("날짜가 필요해", missing.messages[0].body)
        self.assertEqual(invalid.outcome, "needs_input")
        self.assertEqual(invalid.fallback_reason, "invalid_date")
        self.assertIn("요청 형식 오류", invalid.messages[0].body)

    def test_s3_disabled_is_terminal_without_building_client(self) -> None:
        calls: list[str] = []

        result = self._route(enabled=False, s3_calls=calls).handle(
            _request("MB2-C00570 2026-07-04 LED 로그 확인")
        )

        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "s3_disabled")
        self.assertIn("S3_QUERY_ENABLED=true", result.messages[0].body)
        self.assertEqual(calls, [])

    def test_mismatched_device_scope_is_denied_before_s3_lookup(self) -> None:
        calls: list[str] = []
        with patch(
            "boxer_company.assistant.device_led_routes."
            "_analyze_device_led_log"
        ) as analyze:
            result = self._route(s3_calls=calls).handle(
                _request(
                    "MB2-C00002 2026-07-04 LED 로그 확인",
                    metadata={"device_name": "MB2-C00001"},
                )
            )

        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "device_scope_mismatch",
        )
        self.assertNotIn("MB2-C00001", result.messages[0].body)
        self.assertNotIn("MB2-C00002", result.messages[0].body)
        self.assertEqual(calls, [])
        analyze.assert_not_called()

    def test_success_uses_injected_s3_client_and_normalizes_markdown(self) -> None:
        calls: list[str] = []
        with patch(
            "boxer_company.assistant.device_led_routes._analyze_device_led_log",
            return_value=(
                "*장비 LED 로그 확인*\n• 결론: 테스트",
                {
                    "route": "device_led_log_analysis",
                    "logFound": True,
                },
            ),
        ) as analyze:
            result = self._route(s3_calls=calls).handle(
                _request("MB2-C00570 2026-07-04 LED 로그 확인")
            )

        self.assertEqual(result.outcome, "answered")
        self.assertIsNone(result.fallback_reason)
        self.assertTrue(
            result.messages[0].body.startswith("**장비 LED 로그 확인**")
        )
        self.assertEqual(calls, ["get"])
        analyze.assert_called_once_with(
            "s3-client",
            "MB2-C00570",
            "2026-07-04",
        )

    def test_dependency_and_unexpected_errors_have_distinct_reasons(self) -> None:
        for error, reason, expected_text in (
            (
                RuntimeError("s3 unavailable"),
                "dependency_error",
                "S3 로그 접근",
            ),
            (
                ClientError(
                    {"Error": {"Code": "AccessDenied"}},
                    "GetObject",
                ),
                "dependency_error",
                "S3 접근 권한",
            ),
            (
                OSError("unexpected"),
                "analysis_error",
                "잠시 후 다시 시도",
            ),
        ):
            with self.subTest(reason=reason), patch(
                "boxer_company.assistant.device_led_routes._analyze_device_led_log",
                side_effect=error,
            ):
                result = self._route().handle(
                    _request(
                        "MB2-C00570 2026-07-04 LED 로그 확인"
                    )
                )

            self.assertEqual(result.outcome, "failed")
            self.assertEqual(result.fallback_reason, reason)
            self.assertIn(expected_text, result.messages[0].body)

    def test_missing_log_is_no_evidence(self) -> None:
        with patch(
            "boxer_company.assistant.device_led_routes._analyze_device_led_log",
            return_value=(
                "S3 로그 파일을 찾지 못했어: `key`",
                {
                    "route": "device_led_log_analysis",
                    "logFound": False,
                },
            ),
        ):
            result = self._route().handle(
                _request("MB2-C00570 2026-07-04 LED 로그 확인")
            )

        self.assertEqual(result.outcome, "no_evidence")
        self.assertEqual(result.fallback_reason, "log_not_found")


class DeviceLedPatternGuideAssistantRouteTests(unittest.TestCase):
    @staticmethod
    def _reference_loader(calls: list[dict]) -> object:
        def load(question: str, **kwargs):
            calls.append(
                {
                    "question": question,
                    "evidence_payload": kwargs["evidence_payload"],
                    "max_results": kwargs["max_results"],
                }
            )
            return [
                {
                    "pageId": "PAGE-LED",
                    "title": "마미박스 장비 LED 상태표시등",
                    "url": "https://app.notion.com/p/led-guide",
                    "matchedKeywords": ["LED", "증상"],
                    "previewLines": ["warning 상태 설명"],
                }
            ]

        return load

    def test_unrelated_question_returns_none(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="사용 안 됨",
                provider="claude",
                used_llm=True,
            )
        )
        route = DeviceLedPatternGuideAssistantRoute(
            _composer(engine),
            lambda *args, **kwargs: [],
            logger=_quiet_logger(),
        )

        self.assertIsNone(route.handle(_request("장비 영상 목록 알려줘")))
        self.assertEqual(engine.requests, [])

    def test_direct_fallback_preserves_notion_reference(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="사용 안 됨",
                provider="claude",
                used_llm=True,
            )
        )
        reference_calls: list[dict] = []
        route = DeviceLedPatternGuideAssistantRoute(
            _composer(engine, synthesis_enabled=False),
            self._reference_loader(reference_calls),  # type: ignore[arg-type]
            logger=_quiet_logger(),
        )

        result = route.handle(_request("LED 증상은 어떨 때 나타나?"))

        self.assertEqual(result.outcome, "answered")
        self.assertFalse(result.used_llm)
        self.assertEqual(result.fallback_reason, "synthesis_disabled")
        self.assertIn("**LED 증상 안내**", result.messages[0].body)
        self.assertIn("**참고 플레이북**", result.messages[0].body)
        self.assertIn(
            "[마미박스 장비 LED 상태표시등]"
            "(https://app.notion.com/p/led-guide)",
            result.messages[0].body,
        )
        self.assertEqual(result.sources[0].source_id, "PAGE-LED")
        self.assertEqual(engine.requests, [])
        self.assertEqual(reference_calls[0]["max_results"], 2)

    def test_composed_answer_receives_evidence_and_keeps_reference(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text=(
                    "*LED 증상 안내*\n"
                    "• 결론: warning 상태야\n"
                    "• 근거: 초록/빨강 점멸이야\n"
                    "• 참고 상태: `warning`\n"
                    "• 안내: 영상 품질을 확인해"
                ),
                provider="claude",
                used_llm=True,
            )
        )
        reference_calls: list[dict] = []
        route = DeviceLedPatternGuideAssistantRoute(
            _composer(engine),
            self._reference_loader(reference_calls),  # type: ignore[arg-type]
            logger=_quiet_logger(),
        )

        result = route.handle(
            _request("LED 초록불과 빨간불 반복 패턴 의미가 뭐야?")
        )

        self.assertEqual(result.outcome, "answered")
        self.assertTrue(result.used_llm)
        self.assertTrue(
            result.messages[0].body.startswith("**LED 증상 안내**")
        )
        self.assertIn("**참고 플레이북**", result.messages[0].body)
        self.assertEqual(len(engine.requests), 1)
        answer_request = engine.requests[0]
        self.assertEqual(
            answer_request.evidence["route"],
            "device_led_pattern_guide",
        )
        self.assertEqual(
            answer_request.evidence["notionPlaybooks"],
            answer_request.evidence["notionReferences"],
        )
        self.assertTrue(answer_request.extra_rules)

    def test_log_match_wins_when_both_led_matchers_overlap(self) -> None:
        question = (
            "MB2-C00570 2026-07-04 LED 이상 조사. "
            "대기모드일때는 초록색만 나와야하는데 "
            "전원오프상태의 led가 표시됐다고 해"
        )
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="사용되면 안 됨",
                provider="claude",
                used_llm=True,
            )
        )
        service = CompanyAssistantService(
            (
                DeviceLedLogAssistantRoute(
                    lambda: "s3-client",
                    s3_enabled=True,
                    logger=_quiet_logger(),
                ),
                DeviceLedPatternGuideAssistantRoute(
                    _composer(engine),
                    lambda *args, **kwargs: [],
                    logger=_quiet_logger(),
                ),
            )
        )

        with patch(
            "boxer_company.assistant.device_led_routes._analyze_device_led_log",
            return_value=(
                "*장비 LED 로그 확인*\n• 결론: 로그 우선",
                {"logFound": True},
            ),
        ):
            result = service.answer(_request(question))

        self.assertEqual(result.route, "device_led_log_analysis")
        self.assertIn("로그 우선", result.messages[0].body)
        self.assertEqual(engine.requests, [])

    def test_reference_loader_failure_does_not_block_direct_guide(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="사용 안 됨",
                provider="claude",
                used_llm=True,
            )
        )
        route = DeviceLedPatternGuideAssistantRoute(
            _composer(engine, synthesis_enabled=False),
            lambda *args, **kwargs: (_ for _ in ()).throw(
                RuntimeError("notion unavailable")
            ),
            logger=_quiet_logger(),
        )

        result = route.handle(_request("LED 패턴 의미가 뭐야?"))

        self.assertEqual(result.outcome, "answered")
        self.assertIn("**LED 증상 안내**", result.messages[0].body)
        self.assertNotIn("참고 플레이북", result.messages[0].body)

    def test_reference_title_cannot_inject_commonmark_link(self) -> None:
        engine = _FakeAnswerEngine(
            AnswerResult(
                text="사용 안 됨",
                provider="claude",
                used_llm=True,
            )
        )
        malicious_title = "안전](https://evil.test/phish)[가짜"
        route = DeviceLedPatternGuideAssistantRoute(
            _composer(engine, synthesis_enabled=False),
            lambda *args, **kwargs: [
                {
                    "pageId": "PAGE-LED",
                    "title": malicious_title,
                    "url": "https://app.notion.com/p/led-guide",
                    "matchedKeywords": ["LED"],
                }
            ],
            logger=_quiet_logger(),
        )

        result = route.handle(_request("LED 패턴 의미가 뭐야?"))

        self.assertNotIn("https://evil.test", result.messages[0].body)
        self.assertIn("안전］", result.messages[0].body)
        self.assertEqual(
            result.sources[0].uri,
            "https://app.notion.com/p/led-guide",
        )


if __name__ == "__main__":
    unittest.main()
