from __future__ import annotations

import logging
import unittest
from unittest.mock import patch

from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.recording_failure_route import (
    RecordingFailureAssistantRoute,
    _resolve_barcode,
    _selector_text,
)
from boxer_company.assistant.service import RequestScopedRecordingsContext

_BARCODE = "12345678910"


def _request(
    question: str,
    *,
    context_entries: tuple[dict, ...] = (),
    metadata: dict | None = None,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-FAILURE-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
        context_entries=context_entries,  # type: ignore[arg-type]
        metadata=metadata or {},
    )


def _recordings_context(
    *,
    recording_count: int = 1,
    has_device_mapping: bool = True,
) -> dict:
    return {
        "summary": {"recordingCount": recording_count},
        "limit": 30,
        "has_more": False,
        "rows": (
            [{"seq": 1, "deviceSeq": 7}]
            if has_device_mapping
            else [{"seq": 1, "deviceSeq": None}]
        ),
    }


class _FakeComposer:
    def __init__(self) -> None:
        self.calls: list[dict] = []
        self.result = CompanyAssistantResult(
            route="recording_failure_analysis",
            outcome="answered",
            messages=(AssistantMessage(body="*합성된 실패 분석*"),),
            used_llm=True,
        )

    def compose(
        self,
        request: CompanyAssistantRequest,
        *,
        evidence,
        policy: CompanyEvidenceAnswerPolicy,
        sources=(),
    ) -> CompanyAssistantResult:
        self.calls.append(
            {
                "request": request,
                "evidence": evidence,
                "policy": policy,
                "sources": sources,
            }
        )
        return self.result


class RecordingFailureAssistantRouteTests(unittest.TestCase):
    def test_selector_context_requires_exact_actor_identity(self) -> None:
        entries = (
            {
                "kind": "message",
                "source": "web",
                "author_id": None,
                "text": "세션 1",
            },
            {
                "kind": "message",
                "source": "web",
                "author_id": "OTHER",
                "text": "세션 2",
            },
        )
        anonymous = _request("현재 질문", context_entries=entries)
        anonymous = CompanyAssistantRequest(
            request_id=anonymous.request_id,
            tenant_id=anonymous.tenant_id,
            actor_id=None,
            channel=anonymous.channel,
            conversation_id=anonymous.conversation_id,
            question=anonymous.question,
            locale=anonymous.locale,
            context_entries=anonymous.context_entries,
        )

        self.assertEqual(_selector_text(anonymous), "현재 질문")
        self.assertEqual(
            _selector_text(
                _request(
                    "현재 질문",
                    context_entries=entries,
                )
            ),
            "현재 질문",
        )

    def test_followup_recovers_latest_barcode_from_normalized_context(self) -> None:
        request = _request(
            "병원명 테스트병원 병실명 1진료실 날짜 2026-07-20",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "ACTOR-1",
                    "text": "11111111111 녹화 실패",
                },
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "ACTOR-1",
                    "text": f"{_BARCODE} 녹화 실패",
                },
            ),
        )

        self.assertEqual(_resolve_barcode(request), _BARCODE)

    def test_context_barcode_recovery_ignores_other_actor(self) -> None:
        request = _request(
            "병원명 테스트병원 병실명 1진료실 날짜 2026-07-20",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "OTHER-ACTOR",
                    "text": f"{_BARCODE} 녹화 실패",
                },
            ),
        )

        self.assertIsNone(_resolve_barcode(request))

    def _route(
        self,
        *,
        context: dict | None = None,
        loader=None,
        s3_query_enabled: bool = True,
        db_configured: bool = True,
    ) -> tuple[RecordingFailureAssistantRoute, _FakeComposer]:
        composer = _FakeComposer()
        recordings = RequestScopedRecordingsContext(
            barcode=_BARCODE,
            loader=loader or (lambda barcode: context or _recordings_context()),
        )
        logger = logging.Logger("test.recording_failure_route")
        logger.disabled = True
        return (
            RecordingFailureAssistantRoute(
                recordings,
                get_s3_client=lambda: object(),
                composer=composer,  # type: ignore[arg-type]
                s3_query_enabled=s3_query_enabled,
                db_configured=db_configured,
                logger=logger,
            ),
            composer,
        )

    def test_unrelated_request_returns_none_without_loading_dependencies(self) -> None:
        loaded: list[str] = []
        route, composer = self._route(
            loader=lambda barcode: loaded.append(barcode) or {}
        )

        result = route.handle(_request(f"{_BARCODE} 영상 개수 알려줘"))

        self.assertIsNone(result)
        self.assertEqual(loaded, [])
        self.assertEqual(composer.calls, [])

    def test_other_actor_failure_hint_does_not_select_failure_route(
        self,
    ) -> None:
        loaded: list[str] = []
        route, composer = self._route(
            loader=lambda barcode: loaded.append(barcode) or {}
        )

        result = route.handle(
            _request(
                "병원명 테스트병원 병실명 1진료실 날짜 2026-07-20",
                metadata={"barcode": _BARCODE},
                context_entries=(
                    {
                        "kind": "message",
                        "source": "slack",
                        "author_id": "OTHER-ACTOR",
                        "text": "녹화 실패 원인 분석",
                    },
                ),
            )
        )

        self.assertIsNone(result)
        self.assertEqual(loaded, [])
        self.assertEqual(composer.calls, [])

    def test_mismatched_barcode_scope_is_denied_before_loading(self) -> None:
        loaded: list[str] = []
        route, composer = self._route(
            loader=lambda barcode: loaded.append(barcode) or {}
        )

        result = route.handle(
            _request(
                "10987654321 녹화 실패 원인 분석",
                metadata={"barcode": _BARCODE},
            )
        )

        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "barcode_scope_mismatch",
        )
        self.assertNotIn(_BARCODE, result.messages[0].body)
        self.assertNotIn("10987654321", result.messages[0].body)
        self.assertEqual(loaded, [])
        self.assertEqual(composer.calls, [])

    def test_mismatched_hospital_room_scope_is_denied_before_loading(
        self,
    ) -> None:
        loaded: list[str] = []
        route, composer = self._route(
            loader=lambda barcode: loaded.append(barcode) or {}
        )

        result = route.handle(
            _request(
                (
                    f"{_BARCODE} 병원명 다른병원 병실명 2진료실 "
                    "녹화 실패 원인 분석"
                ),
                metadata={
                    "barcode": _BARCODE,
                    "hospital_name": "테스트병원",
                    "room_name": "1진료실",
                },
            )
        )

        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "hospital_room_scope_mismatch",
        )
        self.assertEqual(loaded, [])
        self.assertEqual(composer.calls, [])

    def test_mismatched_device_scope_is_denied_before_loading(self) -> None:
        loaded: list[str] = []
        route, composer = self._route(
            loader=lambda barcode: loaded.append(barcode) or {}
        )

        result = route.handle(
            _request(
                (
                    f"{_BARCODE} MB2-C00002 "
                    "녹화 실패 원인 분석"
                ),
                metadata={
                    "barcode": _BARCODE,
                    "device_name": "MB2-C00001",
                },
            )
        )

        self.assertEqual(result.outcome, "denied")
        self.assertEqual(
            result.fallback_reason,
            "device_scope_mismatch",
        )
        self.assertEqual(loaded, [])
        self.assertEqual(composer.calls, [])

    def test_missing_s3_or_db_configuration_returns_failed_result(self) -> None:
        cases = (
            (
                False,
                True,
                "S3_QUERY_ENABLED=true",
                "s3_not_configured",
            ),
            (
                True,
                False,
                "DB 접속 정보(DB_*)",
                "db_not_configured",
            ),
        )
        for s3_enabled, db_configured, expected_text, expected_reason in cases:
            with self.subTest(reason=expected_reason):
                route, composer = self._route(
                    s3_query_enabled=s3_enabled,
                    db_configured=db_configured,
                )

                result = route.handle(
                    _request(f"{_BARCODE} 녹화 실패 원인 분석")
                )

                self.assertIsNotNone(result)
                self.assertEqual(result.outcome, "failed")
                self.assertIn(expected_text, result.messages[0].body)
                self.assertEqual(result.fallback_reason, expected_reason)
                self.assertEqual(composer.calls, [])

    def test_configuration_gate_precedes_invalid_date_for_direct_request(
        self,
    ) -> None:
        route, composer = self._route(s3_query_enabled=False)

        result = route.handle(
            _request(f"{_BARCODE} 2026-99-40 녹화 실패 원인 분석")
        )

        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "s3_not_configured")
        self.assertEqual(composer.calls, [])

    def test_explicit_scope_followup_claims_invalid_date(self) -> None:
        route, composer = self._route()

        result = route.handle(
            _request(
                "병원명 테스트병원 병실명 1진료실 날짜 2026-99-40",
                metadata={
                    "barcode": _BARCODE,
                    "phase2_hospital_name": "테스트병원",
                    "phase2_room_name": "1진료실",
                    "is_failure_phase2_scope_followup": True,
                },
            )
        )

        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(result.fallback_reason, "invalid_request")
        self.assertIn("날짜", result.messages[0].body)
        self.assertEqual(composer.calls, [])

    def test_missing_device_mapping_returns_phase2_scope_guidance(self) -> None:
        route, composer = self._route(
            context=_recordings_context(
                recording_count=0,
                has_device_mapping=False,
            )
        )

        result = route.handle(
            _request(f"{_BARCODE} 2026-07-20 녹화 실패 원인 분석")
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(result.fallback_reason, "scope_required")
        self.assertIn("**녹화 실패 원인 분석**", result.messages[0].body)
        self.assertIn("병원명", result.messages[0].body)
        self.assertIn("병실명", result.messages[0].body)
        self.assertEqual(composer.calls, [])

    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_render_recording_failure_analysis_fallback"
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_narrow_recording_failure_analysis_evidence"
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_build_recording_failure_analysis_evidence"
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_analyze_barcode_log_errors"
    )
    def test_success_composes_evidence_with_pure_fallback_validator(
        self,
        analyze_errors,
        build_evidence,
        narrow_evidence,
        render_fallback,
    ) -> None:
        evidence = {
            "request": {"barcode": _BARCODE},
            "records": [{"deviceName": "MB2-A00001"}],
        }
        analyze_errors.return_value = ("기존 분석", {"raw": True})
        build_evidence.return_value = evidence
        narrow_evidence.side_effect = (
            lambda payload, selector: (payload, None)
        )
        fallback = "\n".join(
            [
                "*녹화 실패 원인 분석*",
                "• 핵심 원인: 캡처보드 입력 오류",
                "• 운영 근거: ffmpeg 오류",
                "• 영향: 녹화 실패",
                "• 권장 조치: 캡처보드 점검",
                "• 확실도: 높음",
            ]
        )
        render_fallback.return_value = fallback
        route, composer = self._route()

        result = route.handle(
            _request(f"{_BARCODE} 2026-07-20 녹화 실패 원인 분석")
        )

        self.assertEqual(
            result.messages[0].body,
            "**합성된 실패 분석**",
        )
        self.assertEqual(len(composer.calls), 1)
        call = composer.calls[0]
        self.assertEqual(call["evidence"]["request"]["mode"], "error")
        self.assertEqual(
            call["evidence"]["recordingsSummary"],
            {"recordingCount": 1},
        )
        policy = call["policy"]
        self.assertEqual(policy.route, "recording_failure_analysis")
        self.assertEqual(
            policy.max_tokens,
            __import__(
                "boxer_company.settings",
                fromlist=["RECORDING_FAILURE_ANALYSIS_MAX_TOKENS"],
            ).RECORDING_FAILURE_ANALYSIS_MAX_TOKENS,
        )
        self.assertTrue(policy.answer_validator(policy.fallback_message))
        self.assertFalse(
            policy.answer_validator(
                "**녹화 실패 원인 분석**\n• 핵심 원인: 입력 오류"
            )
        )
        self.assertFalse(
            policy.answer_validator(
                policy.fallback_message + "\nLet me reason about this"
            )
        )

    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_render_recording_failure_analysis_fallback",
        return_value=(
            "*녹화 실패 원인 분석*\n"
            "• 핵심 원인: 캡처보드 입력 오류\n"
            "• 운영 근거: ffmpeg 오류\n"
            "• 영향: 녹화 실패\n"
            "• 권장 조치: 캡처보드 점검\n"
            "• 확실도: 높음"
        ),
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_narrow_recording_failure_analysis_evidence",
        side_effect=lambda payload, selector: (payload, None),
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_build_recording_failure_analysis_evidence",
        return_value={"request": {}, "records": [{}]},
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_analyze_barcode_log_errors",
        return_value=("기존 분석", {"raw": True}),
    )
    def test_llm_context_setting_is_respected(
        self,
        analyze_errors,
        build_evidence,
        narrow_evidence,
        render_fallback,
    ) -> None:
        route, composer = self._route()
        request = _request(
            f"{_BARCODE} 2026-07-20 녹화 실패 원인 분석",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "ACTOR-1",
                    "text": "비공개 스레드 문맥",
                },
            ),
        )

        with patch.object(
            __import__("boxer.core.settings", fromlist=["settings"]),
            "LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT",
            False,
        ):
            route.handle(request)

        self.assertEqual(len(composer.calls), 1)
        self.assertFalse(composer.calls[0]["policy"].include_context)

    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_narrow_recording_failure_analysis_evidence"
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_build_recording_failure_analysis_evidence",
        return_value={"request": {}, "records": [{}, {}]},
    )
    @patch(
        "boxer_company.assistant.recording_failure_route."
        "_analyze_barcode_log_errors",
        return_value=("기존 분석", {"raw": True}),
    )
    def test_multiple_sessions_without_actor_selector_returns_needs_input(
        self,
        analyze_errors,
        build_evidence,
        narrow_evidence,
    ) -> None:
        def require_selector(payload, selector):
            self.assertIn("이전 녹화 실패 원인", selector)
            self.assertNotIn("세션 1", selector)
            return None, "세션이 여러 건이라 분석 대상을 지정해줘"

        narrow_evidence.side_effect = require_selector
        route, composer = self._route()
        entries = (
            {
                "kind": "message",
                "source": "slack",
                "author_id": "OTHER-ACTOR",
                "text": "세션 1",
            },
            {
                "kind": "message",
                "source": "slack",
                "author_id": "ACTOR-1",
                "text": "이전 녹화 실패 원인",
            },
        )

        result = route.handle(
            _request(
                f"{_BARCODE} 2026-07-20 녹화 실패 원인 분석",
                context_entries=entries,
            )
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(
            result.fallback_reason,
            "session_selector_required",
        )
        self.assertIn("분석 대상을 지정", result.messages[0].body)
        self.assertEqual(composer.calls, [])

    def test_recordings_dependency_error_returns_failed_result(self) -> None:
        route, composer = self._route(
            loader=lambda barcode: (_ for _ in ()).throw(
                RuntimeError("DB read-only connection unavailable")
            )
        )

        result = route.handle(
            _request(f"{_BARCODE} 2026-07-20 녹화 실패 원인 분석")
        )

        self.assertIsNotNone(result)
        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "dependency_error")
        self.assertIn("DB 연결 또는 조회에 실패", result.messages[0].body)
        self.assertEqual(composer.calls, [])


if __name__ == "__main__":
    unittest.main()
