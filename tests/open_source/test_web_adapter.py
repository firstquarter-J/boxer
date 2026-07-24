from contextlib import contextmanager
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

import boxer_adapter_web
from boxer.retrieval import KnowledgeDocument
from boxer_adapter_web.app import create_web_app
from boxer_adapter_web.auth import hash_password
from boxer_adapter_web.storage import WebChatStore
from boxer_adapter_web.workflows import WorkflowCatalog


# Node BFF 전환 때도 그대로 유지해야 하는 외부 transport 필드 집합을 명시한다.
_SOURCE_REFERENCE_CONTRACT_FIELDS = {
    "documentId",
    "title",
    "score",
    "sourceUri",
}
_MESSAGE_CONTRACT_FIELDS = {
    "id",
    "senderType",
    "senderName",
    "body",
    "sourceRefs",
    "createdAt",
}
_CONVERSATION_CONTRACT_FIELDS = {
    "id",
    "sessionId",
    "customerId",
    "customerName",
    "customerEmail",
    "context",
    "status",
    "workflowKey",
    "workflowState",
    "assignedAdminUserId",
    "assignedAdminUserName",
    "handoffRequestedAt",
    "handoffStartedAt",
    "closedAt",
    "lastMessagePreview",
    "createdAt",
    "updatedAt",
    "messages",
}
_ADMIN_USER_CONTRACT_FIELDS = {"id", "email", "name"}


class BoxerWebAdapterTests(unittest.TestCase):
    def test_public_package_exports_create_web_app(self) -> None:
        self.assertIs(boxer_adapter_web.create_web_app, create_web_app)

    def test_admin_login_and_knowledge_preview(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                response = client.post(
                    "/api/admin/auth/login",
                    json={
                        "email": "admin@example.com",
                        "password": "admin1234",
                    },
                )

                self.assertEqual(response.status_code, 200)
                login_payload = response.json()
                self.assertEqual(set(login_payload), {"adminUser", "csrfToken"})
                self.assertEqual(
                    set(login_payload["adminUser"]),
                    _ADMIN_USER_CONTRACT_FIELDS,
                )
                self.assertEqual(login_payload["adminUser"]["email"], "admin@example.com")
                self.assertTrue(login_payload["csrfToken"])
                self.assertTrue(client.cookies.get("boxer_web_admin_csrf"))

                # 공개 alpha의 관리 화면은 원문 편집 대신 sync/preview를 제공해야 한다.
                status_response = client.get("/api/admin/knowledge/status")
                self.assertEqual(status_response.status_code, 200)
                status_payload = status_response.json()
                self.assertEqual(
                    set(status_payload),
                    {"activeSource", "documentCount", "lastSync"},
                )
                self.assertEqual(status_payload["activeSource"], "markdown")
                self.assertGreaterEqual(status_payload["documentCount"], 1)
                self.assertEqual(
                    set(status_payload["lastSync"]),
                    {
                        "id",
                        "sourceType",
                        "status",
                        "documentCount",
                        "errorMessage",
                        "startedAt",
                        "finishedAt",
                    },
                )

                documents_response = client.get("/api/admin/knowledge/documents")
                self.assertEqual(documents_response.status_code, 200)
                documents_payload = documents_response.json()
                self.assertEqual(set(documents_payload), {"documents"})
                self.assertGreaterEqual(len(documents_payload["documents"]), 1)
                document_summary = documents_payload["documents"][0]
                self.assertEqual(
                    set(document_summary),
                    {
                        "id",
                        "title",
                        "sourceType",
                        "sourceUri",
                        "excerpt",
                        "syncedAt",
                    },
                )

                detail_response = client.get(
                    f"/api/admin/knowledge/documents/{document_summary['id']}"
                )
                self.assertEqual(detail_response.status_code, 200)
                self.assertEqual(set(detail_response.json()), {"document"})
                self.assertEqual(
                    set(detail_response.json()["document"]),
                    {
                        "id",
                        "title",
                        "sourceType",
                        "sourceUri",
                        "excerpt",
                        "syncedAt",
                        "content",
                        "metadata",
                    },
                )

                sync_response = client.post(
                    "/api/admin/knowledge/sync",
                    headers=self._admin_csrf_headers(client),
                )
                self.assertEqual(sync_response.status_code, 200)
                self.assertEqual(
                    set(sync_response.json()),
                    {
                        "id",
                        "sourceType",
                        "status",
                        "documentCount",
                        "errorMessage",
                        "startedAt",
                        "finishedAt",
                    },
                )

    def test_web_adapter_serves_only_admin_frontend(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            admin_dist_path = Path(temp_dir) / "admin-dist"
            admin_dist_path.mkdir()
            (admin_dist_path / "index.html").write_text(
                "<html><body>Boxer Admin Build</body></html>",
                encoding="utf-8",
            )
            with self._create_client(
                temp_dir,
                env_overrides={"BOXER_WEB_ADMIN_DIST_PATH": str(admin_dist_path)},
            ) as client:
                admin_response = client.get("/admin/")

                self.assertEqual(admin_response.status_code, 200)
                self.assertIn("Boxer Admin Build", admin_response.text)
                self.assertEqual(client.get("/widget").status_code, 404)
                self.assertEqual(client.get("/sdk/index.js").status_code, 404)
                self.assertEqual(client.get("/demo").status_code, 404)

    def test_widget_config_cors_allows_only_service_origin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(
                temp_dir,
                env_overrides={"BOXER_WEB_WIDGET_ALLOWED_ORIGINS": "https://service.example"},
            ) as client:
                allowed_response = client.get(
                    "/api/widget/config",
                    headers={"origin": "https://service.example"},
                )
                preflight_response = client.options(
                    "/api/widget/config",
                    headers={
                        "origin": "https://service.example",
                        "access-control-request-method": "GET",
                        "access-control-request-headers": "content-type",
                    },
                )
                rejected_response = client.get(
                    "/api/widget/config",
                    headers={"origin": "https://evil.example"},
                )

                self.assertEqual(allowed_response.status_code, 200)
                self.assertEqual(
                    allowed_response.headers["access-control-allow-origin"],
                    "https://service.example",
                )
                self.assertEqual(preflight_response.status_code, 204)
                self.assertEqual(
                    preflight_response.headers["access-control-allow-origin"],
                    "https://service.example",
                )
                self.assertEqual(rejected_response.status_code, 403)

    def test_widget_config_exposes_welcome_message_and_starter_options(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                response = client.get("/api/widget/config")

                self.assertEqual(response.status_code, 200)
                self.assertEqual(
                    response.json(),
                    {
                        "welcomeTitle": "Support desk",
                        "welcomeMessage": "Welcome to the help center",
                        "starterOptions": ["Account access", "Refund request"],
                        "starterEntries": [
                            {"key": "account_access", "label": "Account access"},
                            {"key": "refund_request", "label": "Refund request"},
                        ],
                        "workflowOptions": {
                            "account_access": [
                                {
                                    "field": "email",
                                    "inputType": "text",
                                    "skipAllowed": False,
                                    "choices": [],
                                },
                                {
                                    "field": "summary",
                                    "inputType": "text",
                                    "skipAllowed": False,
                                    "choices": [],
                                },
                            ]
                        },
                        "workflowConfigVersion": "1",
                        "welcomeTimeZones": {
                            "ko": "Asia/Seoul",
                            "en": "America/Los_Angeles",
                        },
                    },
                )

    def test_http_endpoint_inventory_is_stable_for_widget_and_admin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                openapi_paths = client.get("/openapi.json").json()["paths"]

        # WebSocket은 OpenAPI 밖에서 별도 검증하고 HTTP 공개 표면만 exact match로 잠근다.
        http_methods = {"get", "post", "put", "patch", "delete"}
        actual_inventory = {
            path: {
                method
                for method in operations
                if method in http_methods
            }
            for path, operations in openapi_paths.items()
            if path == "/api/widget/config" or path.startswith("/api/admin/")
        }
        self.assertEqual(
            actual_inventory,
            {
                "/api/widget/config": {"get"},
                "/api/admin/auth/login": {"post"},
                "/api/admin/auth/logout": {"post"},
                "/api/admin/auth/me": {"get"},
                "/api/admin/knowledge/status": {"get"},
                "/api/admin/knowledge/sync": {"post"},
                "/api/admin/knowledge/documents": {"get"},
                "/api/admin/knowledge/documents/{document_id}": {"get"},
                "/api/admin/conversations": {"get"},
                "/api/admin/conversations/{conversation_id}": {"get"},
                "/api/admin/conversations/{conversation_id}/claim": {"post"},
                "/api/admin/conversations/{conversation_id}/release": {"post"},
                "/api/admin/conversations/{conversation_id}/close": {"post"},
                "/api/admin/conversations/{conversation_id}/reply": {"post"},
            },
        )

    def test_workflow_catalog_exposes_widget_choice_metadata(self) -> None:
        catalog = WorkflowCatalog.from_config(
            {
                "starterEntries": [{"key": "device_support", "label": "Device support"}],
                "workflows": {
                    "device_support": {
                        "label": "Device support",
                        "steps": [
                            {
                                "field": "issue_type",
                                "prompt": "Choose an issue.",
                                "choices": ["audio_issue", "upload_issue"],
                                "choiceLabels": {
                                    "audio_issue": {"en": "Audio issue", "ko": "소리 문제"},
                                    "upload_issue": "Upload issue",
                                },
                                "skipAllowed": True,
                            }
                        ],
                        "completionMessage": "Captured.",
                    }
                },
            },
            fallback_options=[],
        )

        self.assertEqual(
            catalog.to_widget_option_payload(),
            {
                "device_support": [
                    {
                        "field": "issue_type",
                        "inputType": "text",
                        "skipAllowed": True,
                        "choices": [
                            {
                                "value": "audio_issue",
                                "label": "Audio issue",
                                "labels": {"en": "Audio issue", "ko": "소리 문제"},
                            },
                            {
                                "value": "upload_issue",
                                "label": "Upload issue",
                                "labels": "Upload issue",
                            },
                        ],
                    }
                ]
            },
        )

    @patch("boxer_adapter_web.chat._synthesize_retrieval_answer", return_value="Refunds are reviewed within 3 business days.")
    def test_widget_websocket_roundtrip(self, _mocked_synthesis) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "identity": {
                                    "id": "customer-01",
                                    "name": "Avery",
                                }
                            }
                        }
                    )
                    ready_event = websocket.receive_json()
                    self.assertEqual(ready_event["type"], "session.ready")
                    session_id = ready_event["payload"]["sessionId"]

                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "What is the refund policy?",
                            },
                        }
                    )

                    user_event = websocket.receive_json()
                    assistant_event = websocket.receive_json()
                    updated_event = websocket.receive_json()

                    self.assertEqual(set(user_event), {"type", "payload"})
                    self.assertEqual(user_event["type"], "message.created")
                    self.assertEqual(
                        set(user_event["payload"]),
                        _MESSAGE_CONTRACT_FIELDS,
                    )
                    self.assertEqual(user_event["payload"]["senderType"], "user")
                    self.assertEqual(set(assistant_event), {"type", "payload"})
                    self.assertEqual(assistant_event["type"], "message.created")
                    self.assertEqual(
                        set(assistant_event["payload"]),
                        _MESSAGE_CONTRACT_FIELDS,
                    )
                    self.assertEqual(assistant_event["payload"]["senderType"], "assistant")
                    self.assertGreaterEqual(
                        len(assistant_event["payload"]["sourceRefs"]),
                        1,
                    )
                    for source_ref in assistant_event["payload"]["sourceRefs"]:
                        self.assertEqual(
                            set(source_ref),
                            _SOURCE_REFERENCE_CONTRACT_FIELDS,
                        )
                    self.assertIn("3 business days", assistant_event["payload"]["body"])
                    self.assertEqual(set(updated_event), {"type", "payload"})
                    self.assertEqual(updated_event["type"], "conversation.updated")
                    self._assert_conversation_contract(updated_event["payload"])
                    self.assertEqual(updated_event["payload"]["status"], "ai_active")
                    self.assertEqual(len(updated_event["payload"]["messages"]), 2)

    def test_widget_workflow_completion_routes_to_handoff_queue(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "context": {
                                    "language": "en",
                                }
                            },
                        }
                    )
                    ready_event = websocket.receive_json()
                    session_id = ready_event["payload"]["sessionId"]

                    websocket.send_json(
                        {
                            "type": "workflow.start",
                            "payload": {
                                "sessionId": session_id,
                                "workflowKey": "account_access",
                            },
                        }
                    )

                    selected_event = websocket.receive_json()
                    prompt_event = websocket.receive_json()
                    updated_event = websocket.receive_json()
                    self.assertEqual(selected_event["payload"]["senderType"], "user")
                    self.assertEqual(selected_event["payload"]["body"], "Account access")
                    self.assertEqual(prompt_event["payload"]["body"], "Which account email should support check?")
                    self.assertEqual(updated_event["payload"]["status"], "workflow_active")
                    self.assertEqual(updated_event["payload"]["workflowKey"], "account_access")

                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "avery@example.com",
                            },
                        }
                    )

                    websocket.receive_json()
                    next_prompt_event = websocket.receive_json()
                    updated_event = websocket.receive_json()
                    self.assertEqual(next_prompt_event["payload"]["body"], "What happened?")
                    self.assertEqual(updated_event["payload"]["workflowState"]["currentStepIndex"], 1)

                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "I cannot sign in.",
                            },
                        }
                    )

                    websocket.receive_json()
                    completion_event = websocket.receive_json()
                    updated_event = websocket.receive_json()
                    self.assertEqual(completion_event["payload"]["senderType"], "assistant")
                    self.assertEqual(completion_event["payload"]["body"], "Captured. Support will continue.")
                    self.assertEqual(updated_event["payload"]["status"], "handoff_pending")
                    self.assertTrue(updated_event["payload"]["workflowState"]["completed"])

    def test_admin_can_claim_and_reply_to_handoff_conversation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json({"type": "session.init", "payload": {}})
                    ready_event = websocket.receive_json()
                    session_id = ready_event["payload"]["sessionId"]

                    websocket.send_json(
                        {
                            "type": "handoff.request",
                            "payload": {
                                "sessionId": session_id,
                                "reason": "I need a human.",
                            },
                        }
                    )
                    websocket.receive_json()
                    websocket.receive_json()
                    handoff_updated_event = websocket.receive_json()
                    conversation_id = handoff_updated_event["payload"]["id"]
                    self.assertEqual(handoff_updated_event["payload"]["status"], "handoff_pending")

                    login_response = client.post(
                        "/api/admin/auth/login",
                        json={
                            "email": "admin@example.com",
                            "password": "admin1234",
                        },
                    )
                    self.assertEqual(login_response.status_code, 200)

                    claim_response = client.post(
                        f"/api/admin/conversations/{conversation_id}/claim",
                        json={},
                        headers=self._admin_csrf_headers(client),
                    )
                    self.assertEqual(claim_response.status_code, 200)
                    self.assertEqual(claim_response.json()["conversation"]["status"], "handoff_live")
                    self.assertEqual(claim_response.json()["conversation"]["assignedAdminUserName"], "Admin")
                    claim_broadcast_event = websocket.receive_json()
                    self.assertEqual(claim_broadcast_event["type"], "conversation.updated")
                    self.assertEqual(claim_broadcast_event["payload"]["status"], "handoff_live")

                    reply_response = client.post(
                        f"/api/admin/conversations/{conversation_id}/reply",
                        json={"text": "Support is checking this now."},
                        headers=self._admin_csrf_headers(client),
                    )
                    self.assertEqual(reply_response.status_code, 200)
                    self.assertEqual(reply_response.json()["message"]["senderType"], "admin")
                    self.assertEqual(reply_response.json()["message"]["senderName"], "Admin")

                    admin_message_event = websocket.receive_json()
                    broadcast_updated_event = websocket.receive_json()
                    self.assertEqual(admin_message_event["type"], "message.created")
                    self.assertEqual(admin_message_event["payload"]["senderType"], "admin")
                    self.assertEqual(admin_message_event["payload"]["body"], "Support is checking this now.")
                    self.assertEqual(broadcast_updated_event["type"], "conversation.updated")
                    self.assertEqual(broadcast_updated_event["payload"]["status"], "handoff_live")

    def test_widget_can_end_session_and_start_new_session(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "context": {
                                    "language": "ko",
                                }
                            },
                        }
                    )
                    ready_event = websocket.receive_json()
                    original_session_id = ready_event["payload"]["sessionId"]

                    websocket.send_json(
                        {
                            "type": "session.end",
                            "payload": {
                                "sessionId": original_session_id,
                            },
                        }
                    )

                    system_event = websocket.receive_json()
                    updated_event = websocket.receive_json()
                    ended_event = websocket.receive_json()
                    self.assertEqual(system_event["payload"]["senderType"], "system")
                    self.assertEqual(system_event["payload"]["body"], "상담이 종료됐어. 새 문의는 다시 시작해 줘.")
                    self.assertEqual(updated_event["payload"]["status"], "closed")
                    self.assertIsNotNone(updated_event["payload"]["closedAt"])
                    self.assertEqual(set(ended_event), {"type", "payload"})
                    self.assertEqual(ended_event["type"], "session.ended")
                    self.assertEqual(
                        set(ended_event["payload"]),
                        _CONVERSATION_CONTRACT_FIELDS,
                    )
                    self.assertEqual(ended_event["payload"]["sessionId"], original_session_id)

                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "context": {
                                    "language": "ko",
                                }
                            },
                        }
                    )
                    new_ready_event = websocket.receive_json()
                    self.assertEqual(new_ready_event["type"], "session.ready")
                    self.assertNotEqual(new_ready_event["payload"]["sessionId"], original_session_id)
                    self.assertEqual(new_ready_event["payload"]["status"], "starter")

    def test_widget_uses_selected_language_for_small_talk(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with patch.object(
                    WebChatStore,
                    "search_knowledge_documents",
                    side_effect=AssertionError("small talk must not hit knowledge search"),
                ):
                    with client.websocket_connect("/ws/widget") as websocket:
                        websocket.send_json(
                            {
                                "type": "session.init",
                                "payload": {
                                    "context": {
                                        "language": "ko",
                                    }
                                },
                            }
                        )
                        ready_event = websocket.receive_json()
                        self.assertEqual(ready_event["type"], "session.ready")
                        session_id = ready_event["payload"]["sessionId"]

                        for greeting in ["안녕?", "ㅎㅇ"]:
                            websocket.send_json(
                                {
                                    "type": "message.send",
                                    "payload": {
                                        "sessionId": session_id,
                                        "text": greeting,
                                    },
                                }
                            )

                            websocket.receive_json()
                            assistant_event = websocket.receive_json()
                            updated_event = websocket.receive_json()
                            self.assertEqual(assistant_event["payload"]["senderType"], "assistant")
                            self.assertEqual(
                                assistant_event["payload"]["body"],
                                "안녕하세요. 도움말 문서를 기준으로 답변할게요. 궁금한 내용을 보내 주세요.",
                            )
                            self.assertEqual(assistant_event["payload"]["sourceRefs"], [])
                            self.assertNotEqual(updated_event["payload"]["status"], "handoff_pending")

    def test_widget_uses_selected_language_for_fallback_message(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "context": {
                                    "language": "ko",
                                }
                            },
                        }
                    )
                    ready_event = websocket.receive_json()
                    self.assertEqual(ready_event["type"], "session.ready")
                    self.assertEqual(ready_event["payload"]["context"]["language"], "ko")
                    session_id = ready_event["payload"]["sessionId"]

                    # FAQ와 겹치지 않는 질문은 언어 선택값에 맞는 fallback 문구를 내려줘야 한다.
                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "주문 상태는 어디서 확인해?",
                            },
                        }
                    )

                    websocket.receive_json()
                    assistant_event = websocket.receive_json()
                    handoff_event = websocket.receive_json()
                    updated_event = websocket.receive_json()
                    self.assertEqual(assistant_event["payload"]["senderType"], "assistant")
                    self.assertEqual(
                        assistant_event["payload"]["body"],
                        "질문과 연결된 도움말 근거를 찾지 못했습니다.",
                    )
                    self.assertEqual(handoff_event["payload"]["senderType"], "assistant")
                    self.assertEqual(
                        handoff_event["payload"]["body"],
                        "상담원 연결을 요청했어. 이제 상담원이 확인하고 이어서 답변할게.",
                    )
                    self.assertEqual(updated_event["payload"]["status"], "handoff_pending")

    def test_widget_websocket_rejects_disallowed_origin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(
                temp_dir,
                env_overrides={"BOXER_WEB_WIDGET_ALLOWED_ORIGINS": "https://allowed.example"},
            ) as client:
                # 임베드 허용 도메인이 설정된 경우 다른 origin의 위젯 연결은 accept 전에 거부한다.
                with self.assertRaises(WebSocketDisconnect):
                    with client.websocket_connect("/ws/widget", headers={"origin": "https://evil.example"}):
                        pass

    def test_widget_websocket_rate_limit_returns_error_event(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(
                temp_dir,
                env_overrides={"BOXER_WEB_WS_RATE_LIMIT_PER_MINUTE": "1"},
            ) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json({"type": "session.init", "payload": {}})
                    ready_event = websocket.receive_json()
                    self.assertEqual(ready_event["type"], "session.ready")

                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": ready_event["payload"]["sessionId"],
                                "text": "hello",
                            },
                        }
                    )
                    error_event = websocket.receive_json()
                    self.assertEqual(error_event["type"], "error")
                    self.assertEqual(error_event["payload"]["code"], "rate_limited")

    def test_widget_websocket_contract_keeps_exact_event_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "identity": {
                                    "id": "contract-customer",
                                    "name": "Contract Customer",
                                },
                                "context": {
                                    "language": "ko",
                                    "tags": ["contract"],
                                    "metadata": {"source": "test"},
                                },
                            },
                        }
                    )
                    ready_event = websocket.receive_json()

                    self.assertEqual(set(ready_event), {"type", "payload"})
                    self.assertEqual(ready_event["type"], "session.ready")
                    self.assertEqual(
                        set(ready_event["payload"]),
                        _CONVERSATION_CONTRACT_FIELDS,
                    )
                    self.assertEqual(ready_event["payload"]["messages"], [])

                    websocket.send_json(
                        {
                            "type": "contract.unknown",
                            "payload": {},
                        }
                    )
                    self.assertEqual(
                        websocket.receive_json(),
                        {
                            "type": "error",
                            "payload": {
                                "code": "unknown_event",
                                "message": (
                                    "Unsupported event type: contract.unknown"
                                ),
                            },
                        },
                    )

                    # unknown event 뒤에도 연결이 유지되는 현재 복구 계약을 고정한다.
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "sessionId": ready_event["payload"]["sessionId"],
                            },
                        }
                    )
                    resumed_event = websocket.receive_json()
                    self.assertEqual(resumed_event["type"], "session.ready")
                    self.assertEqual(
                        set(resumed_event["payload"]),
                        _CONVERSATION_CONTRACT_FIELDS,
                    )

    def test_admin_post_requires_csrf_token(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                login_response = client.post(
                    "/api/admin/auth/login",
                    json={
                        "email": "admin@example.com",
                        "password": "admin1234",
                    },
                )
                self.assertEqual(login_response.status_code, 200)

                missing_csrf_response = client.post("/api/admin/knowledge/sync", json={})
                self.assertEqual(missing_csrf_response.status_code, 403)

    def test_admin_auth_and_websocket_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                # 관리자 WebSocket은 세션 cookie가 생기기 전에는 accept되지 않아야 한다.
                with self.assertRaises(WebSocketDisconnect):
                    with client.websocket_connect("/ws/admin"):
                        pass

                login_response = client.post(
                    "/api/admin/auth/login",
                    json={
                        "email": "admin@example.com",
                        "password": "admin1234",
                    },
                )
                self.assertEqual(login_response.status_code, 200)
                login_payload = login_response.json()
                self.assertEqual(set(login_payload), {"adminUser", "csrfToken"})
                self.assertEqual(
                    set(login_payload["adminUser"]),
                    _ADMIN_USER_CONTRACT_FIELDS,
                )

                me_response = client.get("/api/admin/auth/me")
                self.assertEqual(me_response.status_code, 200)
                self.assertEqual(set(me_response.json()), {"adminUser", "csrfToken"})
                self.assertEqual(
                    set(me_response.json()["adminUser"]),
                    _ADMIN_USER_CONTRACT_FIELDS,
                )

                with client.websocket_connect("/ws/admin") as websocket:
                    self.assertEqual(
                        websocket.receive_json(),
                        {
                            "type": "admin.ready",
                            "payload": {
                                "adminUser": login_payload["adminUser"],
                            },
                        },
                    )

                logout_response = client.post(
                    "/api/admin/auth/logout",
                    headers=self._admin_csrf_headers(client),
                )
                self.assertEqual(logout_response.status_code, 200)
                self.assertEqual(logout_response.json(), {"ok": True})
                self.assertEqual(client.get("/api/admin/auth/me").status_code, 401)

    def test_admin_conversation_http_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "identity": {
                                    "id": "contract-customer",
                                    "name": "Contract Customer",
                                }
                            },
                        }
                    )
                    session_id = websocket.receive_json()["payload"]["sessionId"]
                    websocket.send_json(
                        {
                            "type": "handoff.request",
                            "payload": {
                                "sessionId": session_id,
                                "reason": "Contract handoff",
                            },
                        }
                    )
                    websocket.receive_json()
                    websocket.receive_json()
                    handoff_event = websocket.receive_json()
                    conversation_id = handoff_event["payload"]["id"]

                client.post(
                    "/api/admin/auth/login",
                    json={
                        "email": "admin@example.com",
                        "password": "admin1234",
                    },
                )
                csrf_headers = self._admin_csrf_headers(client)

                list_response = client.get("/api/admin/conversations")
                self.assertEqual(list_response.status_code, 200)
                list_payload = list_response.json()
                self.assertEqual(
                    set(list_payload),
                    {"conversations", "pagination"},
                )
                self.assertEqual(
                    set(list_payload["pagination"]),
                    {"limit", "offset", "total"},
                )
                summary = next(
                    conversation
                    for conversation in list_payload["conversations"]
                    if conversation["id"] == conversation_id
                )
                self.assertEqual(
                    set(summary),
                    _CONVERSATION_CONTRACT_FIELDS,
                )
                self.assertEqual(summary["messages"], [])

                detail_response = client.get(
                    f"/api/admin/conversations/{conversation_id}"
                )
                self.assertEqual(detail_response.status_code, 200)
                self.assertEqual(set(detail_response.json()), {"conversation"})
                detail = detail_response.json()["conversation"]
                self.assertEqual(
                    set(detail),
                    _CONVERSATION_CONTRACT_FIELDS,
                )
                self.assertGreaterEqual(len(detail["messages"]), 2)
                for message in detail["messages"]:
                    self.assertEqual(
                        set(message),
                        _MESSAGE_CONTRACT_FIELDS,
                    )

                claim_response = client.post(
                    f"/api/admin/conversations/{conversation_id}/claim",
                    headers=csrf_headers,
                )
                self.assertEqual(claim_response.status_code, 200)
                self.assertEqual(set(claim_response.json()), {"conversation"})
                self.assertEqual(
                    set(claim_response.json()["conversation"]),
                    _CONVERSATION_CONTRACT_FIELDS,
                )

                reply_response = client.post(
                    f"/api/admin/conversations/{conversation_id}/reply",
                    json={"text": "Contract reply"},
                    headers=csrf_headers,
                )
                self.assertEqual(reply_response.status_code, 200)
                self.assertEqual(
                    set(reply_response.json()),
                    {"message", "conversation"},
                )
                self.assertEqual(
                    set(reply_response.json()["message"]),
                    _MESSAGE_CONTRACT_FIELDS,
                )
                self.assertEqual(
                    set(reply_response.json()["conversation"]),
                    _CONVERSATION_CONTRACT_FIELDS,
                )

                release_response = client.post(
                    f"/api/admin/conversations/{conversation_id}/release",
                    headers=csrf_headers,
                )
                self.assertEqual(release_response.status_code, 200)
                self.assertEqual(
                    set(release_response.json()["conversation"]),
                    _CONVERSATION_CONTRACT_FIELDS,
                )
                self.assertEqual(
                    release_response.json()["conversation"]["status"],
                    "handoff_pending",
                )

                reclaim_response = client.post(
                    f"/api/admin/conversations/{conversation_id}/claim",
                    headers=csrf_headers,
                )
                self.assertEqual(reclaim_response.status_code, 200)
                close_response = client.post(
                    f"/api/admin/conversations/{conversation_id}/close",
                    headers=csrf_headers,
                )
                self.assertEqual(close_response.status_code, 200)
                self.assertEqual(
                    set(close_response.json()["conversation"]),
                    _CONVERSATION_CONTRACT_FIELDS,
                )
                self.assertEqual(
                    close_response.json()["conversation"]["status"],
                    "closed",
                )

    def test_web_store_search_matches_korean_question(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            store = WebChatStore(Path(temp_dir) / "web_chat.db")
            store.initialize()
            store.replace_knowledge_documents(
                source_type="markdown",
                documents=[
                    KnowledgeDocument(
                        id="doc-01",
                        title="환불 정책",
                        content="환불은 영업일 3일 안에 처리됩니다.",
                        source_type="markdown",
                        source_uri="memory://refund",
                        metadata={},
                    )
                ],
                started_at="2026-04-24T00:00:00+00:00",
                finished_at="2026-04-24T00:00:01+00:00",
            )

            results = store.search_knowledge_documents("환불은 얼마나 걸려?", limit=5)

        self.assertGreaterEqual(len(results), 1)
        self.assertEqual(results[0].document.title, "환불 정책")
        self.assertGreater(results[0].score, 0)

    def test_admin_conversations_support_pagination_and_search(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                for customer_name in ["Alpha User", "Beta User"]:
                    with client.websocket_connect("/ws/widget") as websocket:
                        websocket.send_json(
                            {
                                "type": "session.init",
                                "payload": {
                                    "identity": {
                                        "id": customer_name.lower().replace(" ", "-"),
                                        "name": customer_name,
                                    }
                                },
                            }
                        )
                        websocket.receive_json()

                client.post(
                    "/api/admin/auth/login",
                    json={
                        "email": "admin@example.com",
                        "password": "admin1234",
                    },
                )
                first_page = client.get("/api/admin/conversations?limit=1&offset=0")
                self.assertEqual(first_page.status_code, 200)
                self.assertEqual(len(first_page.json()["conversations"]), 1)
                self.assertGreaterEqual(first_page.json()["pagination"]["total"], 2)

                search_response = client.get("/api/admin/conversations?q=Alpha")
                self.assertEqual(search_response.status_code, 200)
                self.assertGreaterEqual(len(search_response.json()["conversations"]), 1)
                self.assertEqual(search_response.json()["conversations"][0]["customerName"], "Alpha User")

    def test_admin_websocket_rejects_disallowed_origin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(
                temp_dir,
                env_overrides={"BOXER_WEB_ADMIN_ALLOWED_ORIGINS": "https://allowed.example"},
            ) as client:
                login_response = client.post(
                    "/api/admin/auth/login",
                    json={
                        "email": "admin@example.com",
                        "password": "admin1234",
                    },
                )
                self.assertEqual(login_response.status_code, 200)

                with self.assertRaises(WebSocketDisconnect):
                    with client.websocket_connect("/ws/admin", headers={"origin": "https://evil.example"}):
                        pass

    def test_admin_websocket_receives_conversation_updates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                login_response = client.post(
                    "/api/admin/auth/login",
                    json={
                        "email": "admin@example.com",
                        "password": "admin1234",
                    },
                )
                self.assertEqual(login_response.status_code, 200)

                with client.websocket_connect("/ws/admin") as admin_websocket:
                    ready_event = admin_websocket.receive_json()
                    self.assertEqual(ready_event["type"], "admin.ready")

                    with client.websocket_connect("/ws/widget") as widget_websocket:
                        widget_websocket.send_json({"type": "session.init", "payload": {}})
                        session_id = widget_websocket.receive_json()["payload"]["sessionId"]
                        widget_websocket.send_json(
                            {
                                "type": "handoff.request",
                                "payload": {
                                    "sessionId": session_id,
                                    "reason": "I need a human.",
                                },
                            }
                        )
                        widget_websocket.receive_json()
                        widget_websocket.receive_json()
                        widget_websocket.receive_json()

                    admin_message_event = admin_websocket.receive_json()
                    admin_notice_event = admin_websocket.receive_json()
                    admin_update_event = admin_websocket.receive_json()
                    self.assertEqual(admin_message_event["type"], "message.created")
                    self.assertEqual(
                        set(admin_message_event["payload"]),
                        {"conversationId", "message"},
                    )
                    self.assertEqual(
                        set(admin_message_event["payload"]["message"]),
                        _MESSAGE_CONTRACT_FIELDS,
                    )
                    self.assertEqual(admin_notice_event["type"], "message.created")
                    self.assertEqual(
                        set(admin_notice_event["payload"]),
                        {"conversationId", "message"},
                    )
                    self.assertEqual(
                        set(admin_notice_event["payload"]["message"]),
                        _MESSAGE_CONTRACT_FIELDS,
                    )
                    self.assertEqual(admin_update_event["type"], "conversation.updated")
                    self._assert_conversation_contract(admin_update_event["payload"])
                    self.assertEqual(admin_update_event["payload"]["status"], "handoff_pending")

    def test_live_handoff_requests_keep_assigned_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json({"type": "session.init", "payload": {}})
                    ready_event = websocket.receive_json()
                    session_id = ready_event["payload"]["sessionId"]

                    websocket.send_json(
                        {
                            "type": "handoff.request",
                            "payload": {
                                "sessionId": session_id,
                                "reason": "사람이 필요해",
                            },
                        }
                    )
                    websocket.receive_json()
                    websocket.receive_json()
                    handoff_updated_event = websocket.receive_json()
                    conversation_id = handoff_updated_event["payload"]["id"]
                    self.assertEqual(handoff_updated_event["payload"]["status"], "handoff_pending")

                    login_response = client.post(
                        "/api/admin/auth/login",
                        json={
                            "email": "admin@example.com",
                            "password": "admin1234",
                        },
                    )
                    self.assertEqual(login_response.status_code, 200)

                    claim_response = client.post(
                        f"/api/admin/conversations/{conversation_id}/claim",
                        json={},
                        headers=self._admin_csrf_headers(client),
                    )
                    self.assertEqual(claim_response.status_code, 200)
                    claim_broadcast_event = websocket.receive_json()
                    self.assertEqual(claim_broadcast_event["payload"]["status"], "handoff_live")

                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "상담원 답변 언제 와?",
                            },
                        }
                    )
                    live_user_event = websocket.receive_json()
                    live_updated_event = websocket.receive_json()
                    self.assertEqual(live_user_event["payload"]["senderType"], "user")
                    self.assertEqual(live_updated_event["payload"]["status"], "handoff_live")
                    self.assertEqual(live_updated_event["payload"]["assignedAdminUserName"], "Admin")

                    websocket.send_json(
                        {
                            "type": "handoff.request",
                            "payload": {
                                "sessionId": session_id,
                                "reason": "다시 연결해 줘",
                            },
                        }
                    )
                    repeat_user_event = websocket.receive_json()
                    repeat_updated_event = websocket.receive_json()
                    self.assertEqual(repeat_user_event["payload"]["senderType"], "user")
                    self.assertEqual(repeat_updated_event["payload"]["status"], "handoff_live")
                    self.assertEqual(repeat_updated_event["payload"]["assignedAdminUserName"], "Admin")

    def test_missing_evidence_can_prompt_before_handoff(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(
                temp_dir,
                env_overrides={"BOXER_WEB_HANDOFF_PROMPT_BEFORE_QUEUE": "true"},
            ) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json(
                        {
                            "type": "session.init",
                            "payload": {
                                "context": {
                                    "language": "ko",
                                }
                            },
                        }
                    )
                    session_id = websocket.receive_json()["payload"]["sessionId"]
                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "주문 상태는 어디서 확인해?",
                            },
                        }
                    )

                    websocket.receive_json()
                    missing_event = websocket.receive_json()
                    offer_event = websocket.receive_json()
                    updated_event = websocket.receive_json()
                    self.assertEqual(missing_event["payload"]["body"], "질문과 연결된 도움말 근거를 찾지 못했습니다.")
                    self.assertEqual(offer_event["payload"]["body"], "도움말 근거로는 확답하기 어려워. 상담원에게 연결할까?")
                    self.assertEqual(updated_event["payload"]["status"], "handoff_offered")

                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "네",
                            },
                        }
                    )
                    websocket.receive_json()
                    handoff_event = websocket.receive_json()
                    handoff_updated_event = websocket.receive_json()
                    self.assertEqual(handoff_event["payload"]["body"], "상담원 연결을 요청했어. 이제 상담원이 확인하고 이어서 답변할게.")
                    self.assertEqual(handoff_updated_event["payload"]["status"], "handoff_pending")

    def test_workflow_step_validation_retries_same_step(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            with self._create_client(temp_dir) as client:
                with client.websocket_connect("/ws/widget") as websocket:
                    websocket.send_json({"type": "session.init", "payload": {}})
                    session_id = websocket.receive_json()["payload"]["sessionId"]
                    websocket.send_json(
                        {
                            "type": "workflow.start",
                            "payload": {
                                "sessionId": session_id,
                                "workflowKey": "account_access",
                            },
                        }
                    )
                    websocket.receive_json()
                    websocket.receive_json()
                    websocket.receive_json()

                    websocket.send_json(
                        {
                            "type": "message.send",
                            "payload": {
                                "sessionId": session_id,
                                "text": "not-an-email",
                            },
                        }
                    )

                    websocket.receive_json()
                    retry_event = websocket.receive_json()
                    updated_event = websocket.receive_json()
                    self.assertEqual(retry_event["payload"]["body"], "Enter a valid email.")
                    self.assertEqual(updated_event["payload"]["workflowState"]["currentStepIndex"], 0)

    @contextmanager
    def _create_client(self, temp_dir: str, env_overrides: dict[str, str] | None = None):
        markdown_root = Path(temp_dir) / "knowledge"
        markdown_root.mkdir(parents=True, exist_ok=True)
        (markdown_root / "refund-policy.md").write_text(
            "# Refund policy\n\nRefund requests are reviewed within 3 business days.\n",
            encoding="utf-8",
        )
        data_path = Path(temp_dir) / "web_chat.db"
        workflow_config_path = Path(temp_dir) / "workflow.json"
        workflow_config_path.write_text(
            json.dumps(
                {
                    "starterEntries": [
                        {"key": "account_access", "label": "Account access"},
                        {"key": "refund_request", "label": "Refund request"},
                    ],
                    "workflows": {
                        "account_access": {
                            "label": "Account access",
                            "steps": [
                                {
                                    "field": "email",
                                    "prompt": "Which account email should support check?",
                                    "validationRegex": "^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$",
                                    "retryPrompt": "Enter a valid email.",
                                },
                                {
                                    "field": "summary",
                                    "prompt": "What happened?",
                                },
                            ],
                            "completionMessage": "Captured. Support will continue.",
                        }
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

        env = {
            "BOXER_SKIP_DOTENV": "true",
            "BOXER_WEB_DATA_PATH": str(data_path),
            "BOXER_WEB_MARKDOWN_ROOT": str(markdown_root),
            "BOXER_WEB_KNOWLEDGE_SOURCE": "markdown",
            "BOXER_WEB_SECRET_KEY": "test-secret",
            "BOXER_WEB_WELCOME_TITLE": "Support desk",
            "BOXER_WEB_WELCOME_MESSAGE": "Welcome to the help center",
            "BOXER_WEB_STARTER_OPTIONS": "Account access||Refund request",
            "BOXER_WEB_WORKFLOW_CONFIG_PATH": str(workflow_config_path),
            "BOXER_WEB_WELCOME_TIMEZONE_KO": "Asia/Seoul",
            "BOXER_WEB_WELCOME_TIMEZONE_EN": "America/Los_Angeles",
            "LLM_PROVIDER": "ollama",
        }
        env.update(env_overrides or {})

        with patch.dict(
            os.environ,
            env,
            clear=False,
        ):
            store = WebChatStore(data_path)
            store.initialize()
            store.upsert_admin_user(
                email="admin@example.com",
                name="Admin",
                password_hash=hash_password("admin1234"),
            )
            with TestClient(create_web_app()) as client:
                yield client

    def _admin_csrf_headers(self, client: TestClient) -> dict[str, str]:
        csrf_token = client.cookies.get("boxer_web_admin_csrf") or ""
        return {"X-Boxer-Csrf-Token": csrf_token}

    def _assert_message_contract(self, message: dict[str, object]) -> None:
        self.assertEqual(set(message), _MESSAGE_CONTRACT_FIELDS)
        for source_ref in message["sourceRefs"]:
            self.assertEqual(
                set(source_ref),
                _SOURCE_REFERENCE_CONTRACT_FIELDS,
            )

    def _assert_conversation_contract(self, conversation: dict[str, object]) -> None:
        # 대화 이벤트의 중첩 메시지와 출처까지 함께 고정해야 BFF 전환 시 드리프트를 잡는다.
        self.assertEqual(set(conversation), _CONVERSATION_CONTRACT_FIELDS)
        for message in conversation["messages"]:
            self._assert_message_contract(message)


if __name__ == "__main__":
    unittest.main()
