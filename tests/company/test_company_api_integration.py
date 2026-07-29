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
