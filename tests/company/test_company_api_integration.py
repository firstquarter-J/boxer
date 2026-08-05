from __future__ import annotations

import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from boxer_company.assistant.factory import (
    create_company_assistant_runtime,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)


class CompanyApiRuntimeIntegrationTests(unittest.TestCase):
    def test_http_contract_runs_device_count_without_live_enrichment(
        self,
    ) -> None:
        token = "d" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="device-count-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset({"assistant.turn.read"}),
                ),
            ),
        )
        with patch(
            "boxer_company.assistant.factory."
            "core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime()
        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with patch(
            "boxer_company.assistant.structured_route."
            "_query_devices_by_filters",
            return_value="*장비 조회 결과*\n• devices row 수: *12개*",
        ) as query:
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Request-ID": "req-device-count-001",
                    },
                    json={
                        "tenantId": "TENANT-1",
                        "actorId": "ACTOR-1",
                        "channel": "slack",
                        "conversationId": "THREAD-DEVICE-1",
                        "question": "활성 장비 몇 개야?",
                        "locale": "ko",
                        "contextEntries": [],
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "devices_filter")
        query.assert_called_once()
        self.assertFalse(
            query.call_args.kwargs["include_live_enrichment"]
        )

    def test_http_contract_runs_deterministic_barcode_count_route(
        self,
    ) -> None:
        token = "b" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="barcode-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset({"assistant.turn.read"}),
                ),
            ),
        )
        recordings_context = {
            "summary": {"recordingCount": 2},
            "rows": [{"seq": 1, "deviceSeq": 7}],
            "limit": 30,
            "has_more": False,
        }
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "_load_recordings_context_by_barcode",
                return_value=recordings_context,
            ),
        ):
            runtime = create_company_assistant_runtime()
        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with patch(
            "boxer_company.assistant.barcode_query_route."
            "_query_recordings_count_by_barcode",
            return_value="*영상 개수*\n• 총 2개",
        ) as query:
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Request-ID": "req-barcode-count-001",
                    },
                    json={
                        "tenantId": "TENANT-1",
                        "actorId": "ACTOR-1",
                        "channel": "slack",
                        "conversationId": "THREAD-BARCODE-1",
                        "question": "12345678910 영상 개수",
                        "locale": "ko",
                        "contextEntries": [],
                        "scope": {"barcode": "12345678910"},
                    },
                )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["route"],
            "barcode_video_count",
        )
        query.assert_called_once()

    def test_http_log_route_never_enables_device_ssh_enrichment(
        self,
    ) -> None:
        token = "l" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="barcode-log-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset({"assistant.turn.read"}),
                ),
            ),
        )
        recordings_context = {
            "summary": {"recordingCount": 1},
            "rows": [{"seq": 1, "deviceSeq": 7}],
            "limit": 30,
            "has_more": False,
        }
        analysis_payload = {
            "summary": {
                "recordCount": 0,
                "sessionCount": 0,
                "abnormalSessionCount": 0,
                "scanEventCount": 0,
                "restartEventCount": 0,
                "errorLineCount": 0,
                "errorGroupCount": 0,
            },
            "records": [],
        }
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "_load_recordings_context_by_barcode",
                return_value=recordings_context,
            ),
        ):
            runtime = create_company_assistant_runtime()
        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.S3_QUERY_ENABLED",
                True,
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_HOST",
                "db-host",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_USERNAME",
                "db-user",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_PASSWORD",
                "db-pass",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_DATABASE",
                "db-name",
            ),
            patch(
                "boxer_company.assistant.factory._build_s3_client",
                return_value=object(),
            ),
            patch(
                "boxer_company.assistant.barcode_log_route."
                "_analyze_barcode_log_scan_events",
                return_value=("*로그 분석 결과*\n• 이상 없음", analysis_payload),
            ) as analyzer,
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Request-ID": "req-barcode-log-001",
                    },
                    json={
                        "tenantId": "TENANT-1",
                        "actorId": "ACTOR-1",
                        "channel": "slack",
                        "conversationId": "THREAD-LOG-1",
                        "question": "12345678910 2026-08-04 로그 분석",
                        "locale": "ko",
                        "contextEntries": [],
                        "scope": {"barcode": "12345678910"},
                    },
                )
                undated = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Request-ID": "req-barcode-log-undated-001",
                    },
                    json={
                        "tenantId": "TENANT-1",
                        "actorId": "ACTOR-1",
                        "channel": "slack",
                        "conversationId": "THREAD-LOG-1",
                        "question": "12345678910 로그 분석",
                        "locale": "ko",
                        "contextEntries": [],
                        "scope": {"barcode": "12345678910"},
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["route"],
            "barcode_log_analysis",
        )
        self.assertFalse(
            analyzer.call_args.kwargs["include_live_enrichment"]
        )
        self.assertEqual(undated.status_code, 200)
        self.assertEqual(undated.json()["outcome"], "needs_input")
        self.assertEqual(
            undated.json()["fallbackReason"],
            "explicit_date_required",
        )
        analyzer.assert_called_once()

    def test_http_typed_failure_followup_reenters_route_without_context(
        self,
    ) -> None:
        token = "f" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="failure-followup-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset({"assistant.turn.read"}),
                ),
            ),
        )
        recordings_context = {
            "summary": {"recordingCount": 0},
            "rows": [],
            "limit": 30,
            "has_more": False,
        }
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "_load_recordings_context_by_barcode",
                return_value=recordings_context,
            ),
        ):
            runtime = create_company_assistant_runtime()
        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        evidence = {"request": {}, "records": [{}]}
        fallback = (
            "*녹화 실패 원인 분석*\n"
            "• 핵심 원인: 로그 근거 확인\n"
            "• 운영 근거: 테스트 로그\n"
            "• 영향: 확인 필요\n"
            "• 권장 조치: 장비 점검\n"
            "• 확실도: 중간"
        )

        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.S3_QUERY_ENABLED",
                True,
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_HOST",
                "db-host",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_USERNAME",
                "db-user",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_PASSWORD",
                "db-pass",
            ),
            patch(
                "boxer_company.assistant.factory."
                "core_settings.DB_DATABASE",
                "db-name",
            ),
            patch(
                "boxer_company.assistant.factory._build_s3_client",
                return_value=object(),
            ),
            patch(
                "boxer_company.assistant.recording_failure_route."
                "_lookup_device_contexts_by_hospital_room",
                return_value=[{"deviceName": "MB2-T00001"}],
            ),
            patch(
                "boxer_company.assistant.recording_failure_route."
                "_analyze_barcode_log_errors",
                return_value=("기존 분석", {"raw": True}),
            ) as analyzer,
            patch(
                "boxer_company.assistant.recording_failure_route."
                "_build_recording_failure_analysis_evidence",
                return_value=evidence,
            ),
            patch(
                "boxer_company.assistant.recording_failure_route."
                "_narrow_recording_failure_analysis_evidence",
                side_effect=lambda payload, selector: (payload, None),
            ),
            patch(
                "boxer_company.assistant.recording_failure_route."
                "_render_recording_failure_analysis_fallback",
                return_value=fallback,
            ),
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Request-ID": "req-failure-followup-001",
                    },
                    json={
                        "tenantId": "TENANT-1",
                        "actorId": "ACTOR-1",
                        "channel": "slack",
                        "conversationId": "THREAD-FAILURE-1",
                        "question": "날짜 2026-08-04",
                        "locale": "ko",
                        "contextEntries": [],
                        "scope": {
                            "barcode": "12345678910",
                            "hospitalName": "테스트병원",
                            "roomName": "1진료실",
                            "followupKind": "recording_failure",
                        },
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["route"],
            "recording_failure_analysis",
        )
        self.assertFalse(
            analyzer.call_args.kwargs["include_live_enrichment"]
        )

    def test_http_contract_runs_structured_room_query_with_real_runtime(
        self,
    ) -> None:
        token = "r" * 48
        request_id = "req-real-runtime-structured-001"
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="structured-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset(
                        {"assistant.turn.read"}
                    ),
                ),
            ),
        )
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "_select_notion_references",
                return_value=[],
            ),
        ):
            runtime = create_company_assistant_runtime()

        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        with patch(
            "boxer_company.assistant.structured_route."
            "_query_hospital_rooms_by_filters",
            return_value="*병실 조회 결과*\n• 서울병원 병실 2개",
        ) as query:
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Request-ID": request_id,
                    },
                    json={
                        "tenantId": "TENANT-1",
                        "actorId": "ACTOR-1",
                        "channel": "slack",
                        "conversationId": "THREAD-STRUCTURED-1",
                        "question": "병원명 서울병원 병실 목록",
                        "locale": "ko",
                        "contextEntries": [],
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-request-id"], request_id)
        body = response.json()
        self.assertEqual(body["requestId"], request_id)
        self.assertEqual(body["route"], "hospital_rooms_filter")
        self.assertEqual(body["outcome"], "answered")
        self.assertEqual(
            body["messages"][0]["body"],
            "**병실 조회 결과**\n• 서울병원 병실 2개",
        )
        self.assertFalse(body["usedLlm"])
        self.assertEqual(body["sources"], [])
        query.assert_called_once_with(
            hospital_name="서울병원",
            room_name=None,
            hospital_seq=None,
            hospital_room_seq=None,
            count_only=False,
        )

    def test_http_contract_runs_real_company_read_only_runtime(
        self,
    ) -> None:
        token = "i" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset(
                        {"assistant.turn.read"}
                    ),
                ),
            ),
        )
        with (
            patch(
                "boxer_company.assistant.factory."
                "core_settings.LLM_PROVIDER",
                "",
            ),
            patch(
                "boxer_company.assistant.factory."
                "_select_notion_references",
                return_value=[],
            ),
        ):
            runtime = create_company_assistant_runtime()

        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Request-ID": "req-real-runtime-001",
                },
                json={
                    "tenantId": "TENANT-1",
                    "actorId": "ACTOR-1",
                    "channel": "slack",
                    "conversationId": "THREAD-1",
                    "question": "MB2-C00419 진단 시작",
                    "locale": "ko",
                    "contextEntries": [],
                },
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["route"],
            "unsupported_live_diagnostic",
        )
        self.assertEqual(response.json()["outcome"], "denied")
        self.assertEqual(
            response.json()["fallbackReason"],
            "read_only_boundary",
        )

        # 같은 HTTP 경계에서 실제 deterministic read-only route도
        # 외부 LLM이나 Notion 호출 없이 정상 답변까지 완주한다.
        with TestClient(app) as client:
            answered = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Request-ID": "req-real-runtime-002",
                },
                json={
                    "tenantId": "TENANT-1",
                    "actorId": "ACTOR-1",
                    "channel": "slack",
                    "conversationId": "THREAD-1",
                    "question": "빨간 LED가 깜빡이면 무슨 뜻이야?",
                    "locale": "ko",
                    "contextEntries": [],
                },
            )

        self.assertEqual(answered.status_code, 200)
        self.assertEqual(
            answered.json()["route"],
            "device_led_pattern_guide",
        )
        self.assertEqual(answered.json()["outcome"], "answered")
        self.assertTrue(answered.json()["messages"][0]["body"])


if __name__ == "__main__":
    unittest.main()
