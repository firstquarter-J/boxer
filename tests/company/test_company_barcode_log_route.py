import logging
import unittest
from unittest.mock import patch

from botocore.exceptions import ClientError

from boxer.core import settings as s
from boxer_company.assistant.answer_composer import CompanyEvidenceAnswerPolicy
from boxer_company.assistant.barcode_log_route import (
    BarcodeLogAssistantRoute,
    PartialResultDeliveryError,
    _build_barcode_log_error_session_section,
    _build_barcode_log_error_summary_session_payload,
    _needs_barcode_log_error_summary_session_fallback,
)
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.service import RequestScopedRecordingsContext


_BARCODE = "12345678910"


def _request(
    question: str,
    *,
    metadata: dict | None = None,
    context_entries: tuple[dict, ...] = (),
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-BARCODE-LOG",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
        metadata=metadata or {},
        context_entries=context_entries,
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
            [{"seq": 1, "deviceSeq": 7, "hospitalName": "테스트병원"}]
            if has_device_mapping
            else [{"seq": 1, "deviceSeq": None}]
        ),
    }


def _analysis_payload(*, with_error: bool = False) -> dict:
    return {
        "route": "barcode_log_error_summary",
        "source": "box_db+s3",
        "request": {
            "mode": "error" if with_error else "scan",
            "barcode": _BARCODE,
            "date": "2026-07-20",
            "dateRange": None,
        },
        "summary": {
            "recordCount": 1 if with_error else 0,
            "sessionCount": 1 if with_error else 0,
            "abnormalSessionCount": 1 if with_error else 0,
            "scanEventCount": 1 if with_error else 0,
            "restartEventCount": 0,
            "errorLineCount": 1 if with_error else 0,
            "errorGroupCount": 1 if with_error else 0,
        },
        "records": (
            [
                {
                    "deviceName": "MB2-T00001",
                    "hospitalName": "테스트병원",
                    "roomName": "1진료실",
                    "date": "2026-07-20",
                    "recordingsOnDateCount": 0,
                    "sessionDetails": [
                        {
                            "index": 1,
                            "startTime": "10:00:00",
                            "stopTime": "미확인",
                            "normalClosed": False,
                            "restartDetected": False,
                            "errorLineCount": 1,
                            "videoStatus": "녹화 실패",
                        }
                    ],
                }
            ]
            if with_error
            else []
        ),
        "errorGroups": [],
    }


def _session_entry(
    detail: dict,
    *,
    recordings_count: int = 0,
) -> dict:
    return {
        "barcode": _BARCODE,
        "deviceName": "MB2-T00001",
        "hospitalName": "테스트병원",
        "roomName": "1진료실",
        "date": "2026-07-20",
        "recordingsOnDateCount": recordings_count,
        "deviceSessionCount": 1,
        "detail": {
            "index": 1,
            "startTime": "10:00:00",
            "stopTime": "10:05:00",
            "normalClosed": True,
            "restartDetected": False,
            "errorLineCount": 0,
            "videoStatus": "정상 녹화",
            **detail,
        },
    }


class _FakeComposer:
    def __init__(self, result: CompanyAssistantResult | None = None) -> None:
        self.result = result
        self.calls: list[dict] = []

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
        if self.result is not None:
            return self.result
        return CompanyAssistantResult(
            route=policy.route,
            outcome="answered",
            messages=(AssistantMessage(body=policy.fallback_message),),
            used_llm=False,
            fallback_reason="synthesis_disabled",
        )


class BarcodeLogAssistantRouteTests(unittest.TestCase):
    def _route(
        self,
        *,
        context: dict | None = None,
        composer: _FakeComposer | None = None,
        s3_enabled: bool = True,
        db_configured: bool = True,
        s3_calls: list[str] | None = None,
        loader=None,
    ) -> tuple[BarcodeLogAssistantRoute, _FakeComposer]:
        resolved_context = context or _recordings_context()
        resolved_composer = composer or _FakeComposer()
        calls = s3_calls if s3_calls is not None else []
        recordings = RequestScopedRecordingsContext(
            barcode=_BARCODE,
            loader=loader or (lambda barcode: resolved_context),
        )
        logger = logging.getLogger(
            f"{__name__}.{self._testMethodName}"
        )
        logger.disabled = True
        route = BarcodeLogAssistantRoute(
            recordings,
            lambda: calls.append("s3") or "S3-CLIENT",
            resolved_composer,  # type: ignore[arg-type]
            s3_query_enabled=lambda: s3_enabled,
            db_configured=lambda: db_configured,
            logger=logger,
        )
        return route, resolved_composer

    def test_unrelated_request_is_not_claimed(self) -> None:
        route, composer = self._route()

        result = route.handle(
            _request(
                f"{_BARCODE} 영상 개수 알려줘",
                metadata={"barcode": _BARCODE},
            )
        )

        self.assertIsNone(result)
        self.assertEqual(composer.calls, [])

    def test_other_actor_log_context_does_not_select_followup_route(
        self,
    ) -> None:
        loaded: list[str] = []
        route, composer = self._route(
            loader=lambda barcode: loaded.append(barcode) or {}
        )
        request = _request(
            "병원명 테스트병원 병실명 1진료실 날짜 2026-07-20",
            metadata={"barcode": _BARCODE},
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "OTHER-ACTOR",
                    "text": f"{_BARCODE} 로그 분석해줘",
                },
            ),
        )

        result = route.handle(request)

        self.assertIsNone(result)
        self.assertEqual(loaded, [])
        self.assertEqual(composer.calls, [])

    def test_context_barcode_recovery_ignores_other_actor(self) -> None:
        route, _ = self._route()
        request = _request(
            "다시 분석해줘",
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "OTHER-ACTOR",
                    "text": f"{_BARCODE} 로그 분석해줘",
                },
            ),
        )

        self.assertIsNone(route._resolve_barcode(request))

    def test_mismatched_barcode_scope_is_denied_before_loading(self) -> None:
        route, composer = self._route()

        result = route.handle(
            _request(
                "10987654321 2026-07-20 로그 분석",
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
        self.assertEqual(composer.calls, [])

    def test_mismatched_hospital_room_scope_is_denied_before_lookup(
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
                    "2026-07-20 로그 분석"
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

    def test_mismatched_device_scope_is_denied_before_lookup(self) -> None:
        loaded: list[str] = []
        route, composer = self._route(
            loader=lambda barcode: loaded.append(barcode) or {}
        )

        result = route.handle(
            _request(
                (
                    f"{_BARCODE} MB2-C00002 2026-07-20 "
                    "로그 분석"
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

    def test_s3_and_db_configuration_failures_have_distinct_reasons(self) -> None:
        cases = (
            (False, True, "s3_query_disabled", "S3"),
            (True, False, "db_not_configured", "DB"),
        )
        for s3_enabled, db_configured, reason, marker in cases:
            with self.subTest(reason=reason):
                s3_calls: list[str] = []
                route, _ = self._route(
                    s3_enabled=s3_enabled,
                    db_configured=db_configured,
                    s3_calls=s3_calls,
                )

                result = route.handle(
                    _request(f"{_BARCODE} 2026-07-20 로그 분석")
                )

                self.assertEqual(result.outcome, "failed")
                self.assertEqual(result.fallback_reason, reason)
                self.assertIn(marker, result.messages[0].body)
                self.assertEqual(s3_calls, [])

    def test_missing_device_scope_returns_complete_phase2_guidance(self) -> None:
        s3_calls: list[str] = []
        route, _ = self._route(
            context=_recordings_context(
                recording_count=0,
                has_device_mapping=False,
            ),
            s3_calls=s3_calls,
        )

        result = route.handle(
            _request(f"{_BARCODE} 2026-07-20 로그 분석")
        )

        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(result.fallback_reason, "scope_required")
        self.assertIn("2차 조회를 위해", result.messages[0].body)
        self.assertIn("병원명", result.messages[0].body)
        self.assertIn("병실명", result.messages[0].body)
        self.assertIn("날짜", result.messages[0].body)
        self.assertEqual(s3_calls, [])

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_lookup_device_contexts_by_hospital_room"
    )
    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_scan_events"
    )
    def test_scope_followup_uses_normalized_context_entries(
        self,
        analyze_scan,
        lookup_room,
    ) -> None:
        lookup_room.return_value = [
            {
                "deviceName": "MB2-T00001",
                "hospitalName": "테스트병원",
                "roomName": "1진료실",
            }
        ]
        analyze_scan.return_value = (
            "*로그 분석 결과*\n• 바코드: `12345678910`",
            _analysis_payload(),
        )
        route, _ = self._route(
            context=_recordings_context(
                recording_count=0,
                has_device_mapping=False,
            )
        )
        request = _request(
            "병원명 테스트병원 병실명 1진료실 날짜 2026-07-20",
            metadata={
                "hospital_name": "테스트병원",
                "room_name": "1진료실",
            },
            context_entries=(
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "ACTOR-1",
                    "text": f"{_BARCODE} 로그 분석해줘",
                },
            ),
        )

        result = route.handle(request)

        self.assertEqual(result.outcome, "answered")
        lookup_room.assert_called_once_with("테스트병원", "1진료실")
        _, called_barcode, called_date = analyze_scan.call_args.args
        self.assertEqual(called_barcode, _BARCODE)
        self.assertEqual(called_date, "2026-07-20")
        self.assertEqual(
            analyze_scan.call_args.kwargs["device_contexts"],
            lookup_room.return_value,
        )

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_scan_events"
    )
    def test_direct_result_stays_deterministic_and_commonmark(
        self,
        analyze_scan,
    ) -> None:
        analyze_scan.return_value = (
            (
                "*로그 분석 결과*\n"
                f"• 바코드: `{_BARCODE}`\n"
                "• 날짜: `2026-07-20`\n"
                "• 확인 세션: `1건`"
            ),
            _analysis_payload(),
        )
        s3_calls: list[str] = []
        route, composer = self._route(s3_calls=s3_calls)

        result = route.handle(
            _request(f"{_BARCODE} 2026-07-20 로그 분석")
        )

        self.assertEqual(result.outcome, "answered")
        self.assertEqual(result.route, "barcode_log_analysis")
        self.assertTrue(
            result.messages[0].body.startswith("**로그 분석 결과**")
        )
        self.assertIn("확인 세션", result.messages[0].body)
        self.assertFalse(result.used_llm)
        self.assertEqual(composer.calls, [])
        self.assertEqual(s3_calls, ["s3"])

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_errors"
    )
    def test_error_summary_is_second_non_mention_message(
        self,
        analyze_errors,
    ) -> None:
        analyze_errors.return_value = (
            (
                "*바코드 로그 에러 분석 결과*\n"
                f"• 바코드: `{_BARCODE}`\n"
                "• 이상 세션: `1건`"
            ),
            _analysis_payload(with_error=True),
        )
        summary_result = CompanyAssistantResult(
            route="barcode_log_error_summary",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=(
                        "**세션별 에러 분석**\n"
                        f"• 바코드: `{_BARCODE}`\n"
                        "• 핵심 원인: 종료 스캔 누락\n"
                        "• 영향: 정상 녹화 실패\n"
                        "• 조치: 장비 상태 확인"
                    )
                ),
            ),
            used_llm=True,
        )
        composer = _FakeComposer(summary_result)
        route, _ = self._route(composer=composer)
        entries = (
            {
                "kind": "message",
                "source": "slack",
                "author_id": "ACTOR-1",
                "text": "앞선 장비 증상 문맥",
            },
        )
        request = _request(
            f"{_BARCODE} 2026-07-20 에러 로그 분석",
            context_entries=entries,
        )

        result = route.handle(request)

        self.assertEqual(len(result.messages), 2)
        self.assertTrue(result.messages[0].mention_actor)
        self.assertFalse(result.messages[1].mention_actor)
        self.assertTrue(result.used_llm)
        self.assertEqual(len(composer.calls), 1)
        self.assertIs(composer.calls[0]["request"], request)
        self.assertEqual(
            composer.calls[0]["policy"].include_context,
            bool(s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT),
        )
        self.assertEqual(
            composer.calls[0]["request"].context_entries,
            entries,
        )

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_errors"
    )
    def test_progress_emits_main_before_summary_and_returns_summary_only(
        self,
        analyze_errors,
    ) -> None:
        analyze_errors.return_value = (
            (
                "*바코드 로그 에러 분석 결과*\n"
                f"• 바코드: `{_BARCODE}`\n"
                "• 이상 세션: `1건`"
            ),
            _analysis_payload(with_error=True),
        )
        summary = CompanyAssistantResult(
            route="barcode_log_error_summary",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=(
                        "**세션별 에러 분석**\n"
                        f"• 바코드: `{_BARCODE}`\n"
                        "• 핵심 원인: 종료 스캔 누락\n"
                        "• 영향: 정상 녹화 실패\n"
                        "• 조치: 장비 상태 확인"
                    )
                ),
            ),
            used_llm=True,
        )
        composer = _FakeComposer(summary)
        route, _ = self._route(composer=composer)
        partials: list[CompanyAssistantResult] = []

        def capture_partial(
            result: CompanyAssistantResult,
        ) -> None:
            # 본문 callback 시점에는 느린 세션별 요약을 아직 시작하지 않아야 한다.
            self.assertEqual(composer.calls, [])
            partials.append(result)

        final = route.handle_with_progress(
            _request(f"{_BARCODE} 2026-07-20 에러 로그 분석"),
            capture_partial,
        )

        self.assertEqual(len(partials), 1)
        self.assertIn("바코드 로그 에러 분석 결과", partials[0].messages[0].body)
        self.assertEqual(len(composer.calls), 1)
        self.assertEqual(len(final.messages), 1)
        self.assertIn("세션별 에러 분석", final.messages[0].body)
        self.assertFalse(final.messages[0].mention_actor)
        self.assertNotIn(
            "바코드 로그 에러 분석 결과",
            final.messages[0].body,
        )
        self.assertTrue(final.used_llm)

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_scan_events"
    )
    def test_progress_without_summary_keeps_empty_terminal_result_handled(
        self,
        analyze_scan,
    ) -> None:
        analyze_scan.return_value = (
            "*로그 분석 결과*\n• 확인 세션: `1건`",
            _analysis_payload(),
        )
        route, composer = self._route()
        partials: list[CompanyAssistantResult] = []

        final = route.handle_with_progress(
            _request(f"{_BARCODE} 2026-07-20 로그 분석"),
            partials.append,
        )

        self.assertEqual(len(partials), 1)
        self.assertEqual(final.outcome, "answered")
        self.assertEqual(final.messages, ())
        self.assertFalse(final.used_llm)
        self.assertEqual(composer.calls, [])

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_scan_events"
    )
    def test_progress_delivery_error_is_not_misclassified_as_analysis_error(
        self,
        analyze_scan,
    ) -> None:
        analyze_scan.return_value = (
            "*로그 분석 결과*\n• 확인 세션: `1건`",
            _analysis_payload(),
        )
        route, composer = self._route()

        def fail_delivery(result: CompanyAssistantResult) -> None:
            raise OSError("slack write failed")

        with self.assertRaises(PartialResultDeliveryError):
            route.handle_with_progress(
                _request(f"{_BARCODE} 2026-07-20 로그 분석"),
                fail_delivery,
            )

        self.assertEqual(composer.calls, [])

    def test_session_fallback_preserves_operational_classification(self) -> None:
        cases = (
            (
                "restart",
                _session_entry(
                    {
                        "normalClosed": False,
                        "restartDetected": True,
                    }
                ),
                ("장비 재시작", "정상 녹화 실패", "전원 차단"),
            ),
            (
                "pre_recording_stop",
                _session_entry(
                    {
                        "preRecordingStopDetected": True,
                        "preRecordingStopLabel": "모션 감지 단계에서 종료 스캔",
                        "errorLineCount": 1,
                        "errorGroups": [
                            {
                                "component": "ffmpeg",
                                "signature": (
                                    "/dev/video0 Device or resource busy"
                                ),
                                "count": 1,
                            }
                        ],
                    }
                ),
                ("녹화 취소", "/dev/video0", "본 녹화 시작 전"),
            ),
            (
                "ffmpeg_stall",
                _session_entry(
                    {
                        "errorLineCount": 2,
                        "videoStatus": "녹화 실패",
                        "errorGroups": [
                            {
                                "component": "RecordingMonitor",
                                "signature": "Recording may be stalled",
                                "count": 2,
                            },
                            {
                                "component": "ffmpeg",
                                "signature": (
                                    "process killed with signal SIGTERM"
                                ),
                                "count": 1,
                            },
                        ],
                    }
                ),
                (
                    "녹화 & 업로드 실패",
                    "stall",
                    "캡처보드",
                ),
            ),
            (
                "network_side_effect",
                _session_entry(
                    {
                        "errorLineCount": 1,
                        "errorGroups": [
                            {
                                "component": "endpoint",
                                "signature": "getaddrinfo EAI_AGAIN",
                                "count": 2,
                            }
                        ],
                    },
                    recordings_count=1,
                ),
                ("네트워크/DNS", "녹화는 성공", "통신 오류는 별도"),
            ),
            (
                "ffmpeg_timestamp",
                _session_entry(
                    {
                        "errorLineCount": 1,
                        "videoStatus": "영상 손상 의심",
                        "errorGroups": [
                            {
                                "component": "ffmpeg",
                                "signature": "Non-monotonous DTS detected",
                                "count": 1,
                            }
                        ],
                    },
                    recordings_count=1,
                ),
                ("DTS/PTS", "캡처보드 연결 불량", "영상 손상 의심"),
            ),
        )

        for name, entry, expected_fragments in cases:
            with self.subTest(name=name):
                section = "\n".join(
                    _build_barcode_log_error_session_section(entry)
                )
                for fragment in expected_fragments:
                    self.assertIn(fragment, section)

    def test_session_validator_rejects_lost_failure_evidence(self) -> None:
        stall_entry = _session_entry(
            {
                "errorLineCount": 2,
                "videoStatus": "녹화 실패",
                "errorGroups": [
                    {
                        "component": "RecordingMonitor",
                        "signature": "Recording may be stalled",
                        "count": 2,
                    },
                    {
                        "component": "ffmpeg",
                        "signature": "killed with signal SIGTERM",
                        "count": 1,
                    },
                ],
            }
        )
        payload = _build_barcode_log_error_summary_session_payload(
            {
                "source": "box_db+s3",
                "request": {
                    "mode": "error",
                    "barcode": _BARCODE,
                    "date": "2026-07-20",
                },
            },
            stall_entry,
        )
        generic = (
            f"• 바코드: `{_BARCODE}`\n"
            "• 핵심 원인: app 오류\n"
            "• 영향: 확인 필요\n"
            "• 조치: 장비 확인"
        )
        grounded = (
            f"• 바코드: `{_BARCODE}`\n"
            "• 핵심 원인: ffmpeg stall과 캡처보드 이상으로 "
            "녹화 & 업로드 실패\n"
            "• 영향: DB 영상 기록 없음\n"
            "• 조치: 캡처보드와 영상 입력 확인"
        )

        self.assertTrue(
            _needs_barcode_log_error_summary_session_fallback(
                generic,
                payload,
            )
        )
        self.assertFalse(
            _needs_barcode_log_error_summary_session_fallback(
                grounded,
                payload,
            )
        )

        pre_stop_entry = _session_entry(
            {
                "preRecordingStopDetected": True,
                "preRecordingStopLabel": "모션 감지 단계에서 종료 스캔",
            }
        )
        pre_stop_payload = (
            _build_barcode_log_error_summary_session_payload(
                {
                    "source": "box_db+s3",
                    "request": {
                        "mode": "error",
                        "barcode": _BARCODE,
                        "date": "2026-07-20",
                    },
                },
                pre_stop_entry,
            )
        )
        self.assertTrue(
            _needs_barcode_log_error_summary_session_fallback(
                generic,
                pre_stop_payload,
            )
        )
        self.assertFalse(
            _needs_barcode_log_error_summary_session_fallback(
                grounded.replace(
                    "ffmpeg stall과 캡처보드 이상으로 "
                    "녹화 & 업로드 실패",
                    "본 녹화 시작 전 종료 스캔으로 녹화 취소",
                ),
                pre_stop_payload,
            )
        )

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_errors"
    )
    def test_each_session_is_composed_then_joined_into_one_message(
        self,
        analyze_errors,
    ) -> None:
        payload = _analysis_payload(with_error=True)
        payload["summary"].update(
            {
                "sessionCount": 2,
                "abnormalSessionCount": 2,
                "errorLineCount": 2,
            }
        )
        payload["records"][0]["sessionDetails"] = [
            {
                "index": 1,
                "startTime": "10:00:00",
                "stopTime": "미확인",
                "normalClosed": False,
                "restartDetected": True,
                "errorLineCount": 1,
                "videoStatus": "녹화 실패",
            },
            {
                "index": 2,
                "startTime": "11:00:00",
                "stopTime": "11:01:00",
                "normalClosed": True,
                "restartDetected": False,
                "preRecordingStopDetected": True,
                "preRecordingStopLabel": "모션 감지 단계에서 종료 스캔",
                "errorLineCount": 1,
                "videoStatus": "녹화 취소",
            },
        ]
        analyze_errors.return_value = (
            (
                "*바코드 로그 에러 분석 결과*\n"
                f"• 바코드: `{_BARCODE}`\n"
                "• 이상 세션: `2건`"
            ),
            payload,
        )
        composed = CompanyAssistantResult(
            route="barcode_log_error_summary_session",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=(
                        f"• 바코드: `{_BARCODE}`\n"
                        "• 핵심 원인: 장비 오류\n"
                        "• 영향: 정상 녹화 실패\n"
                        "• 조치: 장비 상태 확인"
                    )
                ),
            ),
            used_llm=True,
        )
        composer = _FakeComposer(composed)
        route, _ = self._route(composer=composer)

        with patch.object(
            s,
            "LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT",
            True,
        ):
            result = route.handle(
                _request(f"{_BARCODE} 2026-07-20 에러 로그 분석")
            )

        self.assertEqual(len(composer.calls), 2)
        self.assertTrue(
            all(
                call["policy"].include_context
                for call in composer.calls
            )
        )
        self.assertEqual(len(result.messages), 2)
        self.assertFalse(result.messages[1].mention_actor)
        self.assertIn("**세션별 에러 분석**", result.messages[1].body)
        self.assertIn("**세션 1**", result.messages[1].body)
        self.assertIn("**세션 2**", result.messages[1].body)
        self.assertIn("녹화 취소", result.messages[1].body)

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_phase1_window"
    )
    def test_invalid_date_and_range_errors_are_needs_input(
        self,
        analyze_phase1,
    ) -> None:
        route, _ = self._route()

        invalid_date = route.handle(
            _request(f"{_BARCODE} 2026-13-40 로그 분석")
        )

        self.assertEqual(invalid_date.outcome, "needs_input")
        self.assertEqual(invalid_date.fallback_reason, "invalid_request")
        self.assertIn("날짜 형식", invalid_date.messages[0].body)

        analyze_phase1.return_value = (
            (
                "*로그 분석 결과 (1차 자동 범위)*\n"
                f"• 바코드: `{_BARCODE}`\n"
                "• 사유: 1차 범위가 `90일`이라 상한 `30일`을 초과했어\n"
                "• 2차 조회를 위해 병원명, 병실명, 날짜를 입력해줘"
            ),
            _analysis_payload(),
        )
        range_error = route.handle(
            _request(f"{_BARCODE} 로그 분석")
        )

        self.assertEqual(range_error.outcome, "needs_input")
        self.assertEqual(
            range_error.fallback_reason,
            "analysis_range_exceeded",
        )

    @patch(
        "boxer_company.assistant.barcode_log_route."
        "_analyze_barcode_log_scan_events"
    )
    def test_dependency_error_is_structured_failure(
        self,
        analyze_scan,
    ) -> None:
        analyze_scan.side_effect = ClientError(
            {"Error": {"Code": "AccessDenied"}},
            "GetObject",
        )
        route, _ = self._route()

        result = route.handle(
            _request(f"{_BARCODE} 2026-07-20 로그 분석")
        )

        self.assertEqual(result.outcome, "failed")
        self.assertEqual(result.fallback_reason, "dependency_error")
        self.assertIn("S3 접근 권한", result.messages[0].body)


if __name__ == "__main__":
    unittest.main()
