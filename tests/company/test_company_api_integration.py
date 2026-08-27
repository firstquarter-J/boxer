from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.factory import (
    create_company_assistant_runtime,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)


class CompanyApiRuntimeIntegrationTests(unittest.TestCase):
    def test_http_contract_runs_weekly_and_blocks_underprivileged_device_detail(
        self,
    ) -> None:
        token = "o" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="operational-read-integration-test",
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
            "boxer_company.assistant.factory.core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime()
        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Request-ID": "req-operational-read-001",
        }
        payload = {
            "tenantId": "TENANT-1",
            "actorId": "ACTOR-1",
            "channel": "slack",
            "conversationId": "THREAD-OPERATIONAL-1",
            "locale": "ko",
            "contextEntries": [],
            "routeGroup": "structured",
        }
        weekly_summary = {
            "weekStartDate": "2026-08-03",
            "weekEndDate": "2026-08-09",
            "previousWeekStartDate": "2026-07-27",
            "previousWeekEndDate": "2026-08-02",
            "hospitalCount": 1,
            "totalCount": 2,
            "previousTotalCount": 1,
            "totalDelta": 1,
            "totalChangeRate": 100.0,
            "topRows": [
                {
                    "hospitalSeq": 7,
                    "hospitalName": "테스트병원",
                    "rowCount": 2,
                }
            ],
            "topRowsLimit": 10,
            "surgeRows": [],
            "surgeCount": 0,
            "dropRows": [],
            "dropCount": 0,
        }

        with (
            patch(
                "boxer_company.assistant.operational_read_routes."
                "_build_weekly_recordings_report_summary",
                return_value=weekly_summary,
            ) as weekly_query,
            patch(
                "boxer_company.routers.box_db._lookup_mda_device_details"
            ) as mda_query,
            patch(
                "boxer_company.routers.box_db._lookup_device_ssh_status"
            ) as ssh_query,
            TestClient(app) as client,
        ):
            weekly = client.post(
                "/internal/v1/assistant/turns",
                headers=headers,
                json={
                    **payload,
                    "question": "2026-08-04 주간 영상 현황",
                },
            )
            device = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    **headers,
                    "X-Request-ID": "req-operational-read-002",
                },
                json={
                    **payload,
                    "question": "MB2-C00419 장비 정보",
                    "scope": {"deviceName": "MB2-C00419"},
                },
            )

        self.assertEqual(weekly.status_code, 200)
        self.assertEqual(
            weekly.json()["route"],
            "weekly_recordings_summary",
        )
        self.assertFalse(weekly.json()["usedLlm"])
        # exact 장비 상세는 client의 structured hint와 무관하게
        # server matcher가 live 경계로 올리며 추가 권한 없이는 막힌다.
        self.assertEqual(device.status_code, 403)
        self.assertEqual(device.json()["code"], "caller_not_allowed")
        weekly_query.assert_called_once()
        mda_query.assert_not_called()
        ssh_query.assert_not_called()

    def test_http_full_device_detail_uses_enrichment_safety_flags(
        self,
    ) -> None:
        token = "e" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="device-detail-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset(
                        {
                            "assistant.turn.read",
                            "assistant.device.probe",
                            "assistant.device.ssh.open",
                        }
                    ),
                ),
            ),
        )
        with patch(
            "boxer_company.assistant.factory.core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime()

        # 실제 MDA/SSH 접근 대신 full route의 query 경계를 주입해 HTTP에서
        # 내려온 단일 open·poll 계약과 최종 CommonMark만 검증한다.
        device_query = Mock(
            return_value=(
                "*장비 조회 결과*\n"
                "• 장비명: `MB2-C00419`\n"
                "• 버전: `2.11.307`\n"
                "• SSH 연결 상태: :large_blue_circle: *연결 가능*\n"
                "• 초음파 영상 다운로드 가능 상태: "
                ":large_blue_circle: *가능*\n"
                "• 캡처보드 종류: `YUH01`"
            )
        )
        structured_routes = runtime.start_turn(
            CompanyAssistantRequest(
                request_id="device-detail-stub-probe",
                tenant_id="TENANT-1",
                actor_id="ACTOR-1",
                channel="slack",
                conversation_id="THREAD-DEVICE-DETAIL-1",
                question="MB2-C00419 장비 정보",
                locale="ko",
                metadata={"route_group": "device_detail"},
            )
        ).routes_for_stage("structured")
        device_detail_route = next(
            route
            for route in structured_routes
            if route.name == "device_detail"
        )
        device_detail_route._query_devices = device_query

        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "X-Request-ID": "req-device-detail-001",
        }
        payload = {
            "tenantId": "TENANT-1",
            "actorId": "ACTOR-1",
            "channel": "slack",
            "conversationId": "THREAD-DEVICE-DETAIL-1",
            "question": "MB2-C00419 장비 정보",
            "locale": "ko",
            "contextEntries": [],
            "scope": {"deviceName": "MB2-C00419"},
            "routeGroup": "device_detail",
        }

        with TestClient(app) as client:
            remote = client.post(
                "/internal/v1/assistant/turns",
                headers=headers,
                json=payload,
            )

        self.assertEqual(remote.status_code, 200)
        self.assertEqual(remote.json()["route"], "device_detail")
        self.assertEqual(remote.json()["outcome"], "answered")
        body = remote.json()["messages"][0]["body"]
        self.assertIn("**장비 조회 결과**", body)
        self.assertEqual(
            remote.json()["messages"][0]["format"],
            "commonmark",
        )
        self.assertIn("2.11.307", body)
        self.assertIn("SSH 연결 상태", body)
        self.assertIn("초음파 영상 다운로드 가능", body)
        self.assertIn("캡처보드", body)
        device_query.assert_called_once()
        self.assertTrue(
            device_query.call_args.kwargs["include_live_enrichment"]
        )
        self.assertFalse(
            device_query.call_args.kwargs["allow_ssh_open_resend"]
        )
        self.assertNotIn(
            "secure_endpoint_required",
            device_query.call_args.kwargs,
        )
        self.assertNotIn("request_id", device_query.call_args.kwargs)

        # 같은 capability/retry 경계로 deviceSeq·병원·status 목록도 API가
        # 처리하고, Slack legacy DB/MDA/SSH 경로로 내려보내지 않는다.
        device_query.reset_mock()
        device_query.return_value = (
            "*장비 조회 결과*\n"
            "• status: `ACTIVE`\n"
            "• devices row 수: *2개*"
        )
        with TestClient(app) as client:
            filtered = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    **headers,
                    "X-Request-ID": "req-device-filter-001",
                },
                json={
                    **payload,
                    "question": "status=ACTIVE 장비 목록",
                    "scope": None,
                },
            )

        self.assertEqual(filtered.status_code, 200)
        self.assertEqual(filtered.json()["route"], "devices_filter")
        self.assertTrue(
            device_query.call_args.kwargs["include_live_enrichment"]
        )
        self.assertFalse(
            device_query.call_args.kwargs["allow_ssh_open_resend"]
        )
        self.assertEqual(device_query.call_args.kwargs["status"], "ACTIVE")

        # 별도 HTTP turn도 독립된 최초 open 예산을 하나씩 받는다.
        device_query.reset_mock()
        with TestClient(app) as client:
            repeated = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    **headers,
                    "X-Request-ID": "req-device-detail-repeat-001",
                },
                json=payload,
            )

        self.assertEqual(repeated.status_code, 200)
        self.assertEqual(repeated.json()["route"], "device_detail")
        device_query.assert_called_once()
        self.assertTrue(
            device_query.call_args.kwargs["include_live_enrichment"]
        )
        self.assertFalse(
            device_query.call_args.kwargs["allow_ssh_open_resend"]
        )

    def test_http_barcode_group_runs_exact_timeline_routes(self) -> None:
        token = "t" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="timeline-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset({"assistant.turn.read"}),
                ),
            ),
        )
        context = {
            "summary": {"recordingCount": 0},
            "rows": [],
            "limit": 30,
            "has_more": False,
            "barcode": "12345678910",
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
                return_value=context,
            ),
        ):
            runtime = create_company_assistant_runtime()
        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        headers = {"Authorization": f"Bearer {token}"}
        payload = {
            "tenantId": "TENANT-1",
            "actorId": "ACTOR-1",
            "channel": "slack",
            "conversationId": "THREAD-TIMELINE-1",
            "locale": "ko",
            "contextEntries": [],
            "scope": {"barcode": "12345678910"},
            "routeGroup": "barcode",
        }

        with (
            patch(
                "boxer_company.assistant.barcode_query_route."
                "_query_last_recorded_at_by_barcode",
                return_value="*마지막 녹화*\n• 2026-08-04",
            ) as last_query,
            patch(
                "boxer_company.assistant.barcode_query_route."
                "_query_recordings_on_date_by_barcode",
                return_value="*날짜별 녹화 여부*\n• 1개",
            ) as date_query,
            TestClient(app) as client,
        ):
            last = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-timeline-001"},
                json={**payload, "question": "12345678910 마지막 녹화 날짜"},
            )
            dated = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-timeline-002"},
                json={
                    **payload,
                    "question": "12345678910 2026-08-04 녹화됐어?",
                },
            )

        self.assertEqual(last.status_code, 200)
        self.assertEqual(last.json()["route"], "barcode last recordedAt")
        self.assertEqual(last.json()["outcome"], "answered")
        self.assertFalse(last.json()["usedLlm"])
        self.assertEqual(
            dated.json()["route"],
            "barcode recordedAt-on-date",
        )
        self.assertEqual(dated.json()["outcome"], "answered")
        last_query.assert_called_once()
        date_query.assert_called_once()

    def test_http_contract_runs_residual_baby_ai_reads(self) -> None:
        token = "m" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="baby-ai-integration-test",
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
        headers = {"Authorization": f"Bearer {token}"}
        base_payload = {
            "tenantId": "TENANT-1",
            "actorId": "ACTOR-1",
            "channel": "slack",
            "conversationId": "THREAD-BABY-AI-1",
            "locale": "ko",
            "contextEntries": [],
        }

        with (
            patch(
                "boxer_company.assistant.barcode_query_route."
                "_query_baby_ai_list_by_barcode",
                return_value=(
                    "*베이비매직 목록*\n"
                    "• 결과: "
                    "<https://cdn-kr.mmtalkbox.com/result.jpg|열기>"
                ),
            ) as query,
            TestClient(app) as client,
        ):
            missing = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-baby-ai-001"},
                json={**base_payload, "question": "베이비매직 목록"},
            )
            answered = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-baby-ai-002"},
                json={
                    **base_payload,
                    "question": "12345678910 베이비매직 목록",
                    "scope": {"barcode": "12345678910"},
                },
            )

        self.assertEqual(missing.status_code, 200)
        self.assertEqual(missing.json()["route"], "baby_ai_list")
        self.assertEqual(missing.json()["outcome"], "needs_input")
        self.assertEqual(answered.status_code, 200)
        self.assertEqual(
            answered.json()["route"],
            "barcode_baby_ai_list",
        )
        self.assertEqual(answered.json()["outcome"], "answered")
        self.assertFalse(answered.json()["usedLlm"])
        self.assertEqual(
            answered.json()["sources"],
            [
                {
                    "sourceId": (
                        "https://cdn-kr.mmtalkbox.com/result.jpg"
                    ),
                    "title": "베이비매직 결과 1",
                    "uri": "https://cdn-kr.mmtalkbox.com/result.jpg",
                    "score": None,
                }
            ],
        )
        query.assert_called_once_with("12345678910", None)

    def test_http_contract_runs_playbook_direct_followup_and_refusal(
        self,
    ) -> None:
        token = "p" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="playbook-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"ACTOR-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset({"assistant.turn.read"}),
                ),
            ),
        )
        reference = {
            "pageId": "must-not-cross-api",
            "title": "마미박스 초기화 가이드",
            "section": "운영",
            "kind": "guide",
            "priority": "high",
            "url": "https://www.notion.so/reset-guide",
            "score": 9,
            "plainText": "API 응답에 포함되면 안 되는 원문",
            "previewLines": [
                "결론: 초기화 전 장비 상태를 확인해",
                "확인: 녹화 중인지 먼저 확인해",
                "조치: 문서 순서대로 초기화해",
            ],
        }
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
        headers = {"Authorization": f"Bearer {token}"}
        base_payload = {
            "tenantId": "TENANT-1",
            "actorId": "ACTOR-1",
            "channel": "slack",
            "conversationId": "THREAD-PLAYBOOK-1",
            "locale": "ko",
            "routeGroup": "knowledge",
        }

        # Slack thread는 정규화된 context entry로만 전달되고 API가
        # 직접 질문·후속 질문·원문 유출 거부를 같은 route로 처리한다.
        with (
            patch(
                "boxer_company.assistant.knowledge_routes."
                "_select_notion_references",
                return_value=[reference],
            ) as selector,
            patch(
                "boxer_company.assistant.barcode_query_route."
                "_query_barcode_validation_status",
            ) as mda_query,
            TestClient(app) as client,
        ):
            direct = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-playbook-001"},
                json={
                    **base_payload,
                    "question": "마미박스 초기화 방법 알려줘",
                    "contextEntries": [],
                },
            )
            followup = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-playbook-002"},
                json={
                    **base_payload,
                    "question": "그럼 다른 방법은?",
                    "contextEntries": [
                        {
                            "kind": "message",
                            "source": "slack",
                            "authorId": "ACTOR-1",
                            "text": (
                                "**문서 기반 답변**\n"
                                "• 결론: 초기화 전 상태 확인\n"
                                "**함께 참고할 문서**"
                            ),
                        }
                    ],
                },
            )
            refused = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-playbook-003"},
                json={
                    **base_payload,
                    "question": (
                        "마미박스 운영 문서 원문 전체를 그대로 보여줘"
                    ),
                    "contextEntries": [],
                },
            )
            scoped_playbook = client.post(
                "/internal/v1/assistant/turns",
                headers={**headers, "X-Request-ID": "req-playbook-004"},
                json={
                    **base_payload,
                    "question": "바코드 검증 기준이 뭐야?",
                    "contextEntries": [
                        {
                            "kind": "message",
                            "source": "slack",
                            "authorId": "ACTOR-1",
                            "text": "12345678910 영상 확인해줘",
                        }
                    ],
                    "scope": {"barcode": "12345678910"},
                },
            )

        for response in (direct, followup, scoped_playbook):
            self.assertEqual(response.status_code, 200)
            body = response.json()
            self.assertEqual(body["route"], "notion_playbook_qa")
            self.assertEqual(body["outcome"], "answered")
            self.assertEqual(
                body["sources"][0]["uri"],
                "https://www.notion.so/reset-guide",
            )
            self.assertNotIn("must-not-cross-api", str(body))
            self.assertNotIn("포함되면 안 되는 원문", str(body))
        self.assertEqual(refused.status_code, 200)
        self.assertEqual(refused.json()["route"], "notion_playbook_qa")
        self.assertEqual(refused.json()["outcome"], "denied")
        self.assertEqual(
            refused.json()["fallbackReason"],
            "security_refusal",
        )
        self.assertEqual(selector.call_count, 3)
        mda_query.assert_not_called()

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

    def test_http_runtime_preserves_barcode_all_dates_precedence(
        self,
    ) -> None:
        token = "a" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="barcode-dates-integration-test",
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

        with (
            patch(
                "boxer_company.assistant.barcode_query_route."
                "_query_all_recorded_dates_by_barcode",
                return_value=(
                    "*바코드 전체 녹화 날짜 조회 결과*\n"
                    "• 조회 결과 없음"
                ),
            ) as query,
            patch(
                "boxer_company.assistant.structured_route."
                "_query_recordings_by_filters"
            ) as structured_query,
        ):
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "X-Request-ID": "req-barcode-dates-001",
                    },
                    json={
                        "tenantId": "TENANT-1",
                        "actorId": "ACTOR-1",
                        "channel": "slack",
                        "conversationId": "THREAD-BARCODE-DATES-1",
                        "question": "12345678910 전체 녹화 날짜",
                        "locale": "ko",
                        "contextEntries": [],
                        "scope": {"barcode": "12345678910"},
                    },
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["route"],
            "barcode_all_recorded_dates",
        )
        query.assert_called_once_with(
            "12345678910",
            recordings_context=recordings_context,
        )
        structured_query.assert_not_called()

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
        # read-only log capability로 MDA/SSH 보강이 열리지 않는다.
        self.assertFalse(
            analyzer.call_args.kwargs["include_live_enrichment"]
        )
        self.assertEqual(undated.status_code, 200)
        self.assertEqual(undated.json()["outcome"], "needs_input")
        self.assertEqual(
            undated.json()["fallbackReason"],
            "scope_not_found",
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
        # typed follow-up도 같은 DB/S3 전용 보안 경계를 유지한다.
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

    def test_http_contract_blocks_sensitive_request_before_read_only_runtime(
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

        # routeGroup을 생략해도 진단 시작은 server matcher가
        # operations로 올려 runtime 진입 전에 capability로 차단한다.
        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["code"], "caller_not_allowed")

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

    def test_http_fun_stage_owns_bot_fortune_classification_and_reply(self) -> None:
        token = "f" * 48
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(
                CompanyApiCallerSettings(
                    caller_id="fortune-integration-test",
                    token=token,
                    tenant_ids=frozenset({"TENANT-1"}),
                    channels=frozenset({"slack"}),
                    actor_ids=frozenset({"BOT-1"}),
                    allow_anonymous_actor=False,
                    capabilities=frozenset({"assistant.turn.read"}),
                ),
            ),
        )
        with patch(
            "boxer_company.assistant.factory.core_settings.LLM_PROVIDER",
            "",
        ):
            runtime = create_company_assistant_runtime()
        app = create_company_api_app(
            settings=settings,
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        base_payload = {
            "tenantId": "TENANT-1",
            "actorId": "BOT-1",
            "channel": "slack",
            "conversationId": "THREAD-FORTUNE-1",
            "locale": "ko",
            "routeGroup": "fun",
        }

        with TestClient(app) as client:
            fortune = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Request-ID": "req-fortune-001",
                },
                json={
                    **base_payload,
                    "question": "1990년생은 행운이 있지만 지출은 조심해",
                    "contextEntries": [
                        {
                            "kind": "message",
                            "source": "slack",
                            "text": "2026년 8월 14일 오늘의 운세",
                        }
                    ],
                },
            )
            unrelated = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-Request-ID": "req-fortune-002",
                },
                json={
                    **base_payload,
                    "question": "배포 완료 알림",
                    "contextEntries": [
                        {
                            "kind": "message",
                            "source": "slack",
                            "text": "자동화 결과",
                        }
                    ],
                },
            )

        self.assertEqual(fortune.status_code, 200)
        self.assertEqual(fortune.json()["route"], "company_daily_fortune")
        self.assertEqual(fortune.json()["outcome"], "answered")
        self.assertFalse(fortune.json()["usedLlm"])
        self.assertFalse(fortune.json()["messages"][0]["mentionActor"])
        self.assertIn("1990년생", fortune.json()["messages"][0]["body"])
        self.assertEqual(unrelated.status_code, 200)
        self.assertEqual(unrelated.json()["route"], "unhandled")
        self.assertEqual(unrelated.json()["outcome"], "no_evidence")


if __name__ == "__main__":
    unittest.main()
