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
                self.assertEqual(response.json()["adminUser"]["email"], "admin@example.com")
                self.assertTrue(response.json()["csrfToken"])
                self.assertTrue(client.cookies.get("boxer_web_admin_csrf"))

                # 공개 alpha의 관리 화면은 원문 편집 대신 sync/preview를 제공해야 한다.
                status_response = client.get("/api/admin/knowledge/status")
                self.assertEqual(status_response.status_code, 200)
                self.assertEqual(status_response.json()["activeSource"], "markdown")
                self.assertGreaterEqual(status_response.json()["documentCount"], 1)

                documents_response = client.get("/api/admin/knowledge/documents")
                self.assertEqual(documents_response.status_code, 200)
                self.assertGreaterEqual(len(documents_response.json()["documents"]), 1)

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
                self.assertEqual(response.json()["welcomeTitle"], "Support desk")
                self.assertEqual(response.json()["welcomeMessage"], "Welcome to the help center")
                self.assertEqual(
                    response.json()["starterOptions"],
                    ["Account access", "Refund request"],
                )
                self.assertEqual(
                    response.json()["starterEntries"],
                    [
                        {"key": "account_access", "label": "Account access"},
                        {"key": "refund_request", "label": "Refund request"},
                    ],
                )
                self.assertEqual(response.json()["workflowConfigVersion"], "1")
                self.assertEqual(response.json()["workflowOptions"]["account_access"][0]["field"], "email")
                self.assertEqual(
                    response.json()["welcomeTimeZones"],
                    {"ko": "Asia/Seoul", "en": "America/Los_Angeles"},
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

                    self.assertEqual(user_event["type"], "message.created")
                    self.assertEqual(user_event["payload"]["senderType"], "user")
                    self.assertEqual(assistant_event["type"], "message.created")
                    self.assertEqual(assistant_event["payload"]["senderType"], "assistant")
                    self.assertIn("3 business days", assistant_event["payload"]["body"])
                    self.assertEqual(updated_event["type"], "conversation.updated")
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
                    self.assertEqual(ended_event["type"], "session.ended")
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
                    self.assertEqual(admin_notice_event["type"], "message.created")
                    self.assertEqual(admin_update_event["type"], "conversation.updated")
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


if __name__ == "__main__":
    unittest.main()
