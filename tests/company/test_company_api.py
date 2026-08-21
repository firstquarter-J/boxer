from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import sqlite3
import stat
import tempfile
import threading
import unittest
from typing import Any
from unittest.mock import patch

from fastapi import Request
from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import (
    AssistantLink,
    AssistantMessage,
    CompanyAssistantResult,
    SourceReference,
    SuggestedAction,
)
from boxer_company_adapter_slack.company_api_client import (
    _deserialize_result,
)
from boxer_company_api.app import (
    _initialize_request_log_readiness,
    _secure_request_log_leaf,
    create_company_api_app,
)
from boxer_company_api.schemas import AssistantTurnInput
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)


_TOKEN = "s" * 48
_REQUEST_ID = "req-company-api-001"


class _FakeRuntime:
    def __init__(
        self,
        result: CompanyAssistantResult | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.requests: list[Any] = []
        self.stages: list[str] = []

    def answer(self, request: Any) -> CompanyAssistantResult | None:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        return self.result

    def answer_stage(
        self,
        request: Any,
        stage: str,
        *,
        on_partial_result: Any = None,
    ) -> CompanyAssistantResult | None:
        del on_partial_result
        self.requests.append(request)
        self.stages.append(stage)
        if self.error is not None:
            raise self.error
        return self.result


def _settings(
    *,
    tenant_ids: tuple[str, ...] = ("TENANT-1",),
    channels: tuple[str, ...] = ("slack",),
    actor_ids: tuple[str, ...] = ("ACTOR-1",),
    allow_anonymous_actor: bool = False,
    capabilities: tuple[str, ...] = ("assistant.turn.read",),
    configuration_error: str | None = None,
    live_device_enabled: bool = True,
    operations_enabled: bool = True,
    request_log_enabled: bool = False,
    request_log_path: str = "/var/lib/boxer-company-api/request_log.db",
) -> CompanyApiSettings:
    callers = ()
    if configuration_error is None:
        callers = (
            CompanyApiCallerSettings(
                caller_id="slack-prod",
                token=_TOKEN,
                tenant_ids=frozenset(tenant_ids),
                channels=frozenset(channels),
                actor_ids=frozenset(actor_ids),
                allow_anonymous_actor=allow_anonymous_actor,
                capabilities=frozenset(capabilities),
            ),
        )
    return CompanyApiSettings(
        host="127.0.0.1",
        port=8010,
        callers=callers,
        configuration_error=configuration_error,
        live_device_enabled=live_device_enabled,
        operations_enabled=operations_enabled,
        request_log_enabled=request_log_enabled,
        request_log_path=request_log_path,
    )


def _headers(
    *,
    token: str = _TOKEN,
    request_id: str = _REQUEST_ID,
) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "X-Request-ID": request_id,
    }


def _payload(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "tenantId": "TENANT-1",
        "actorId": "ACTOR-1",
        "channel": "slack",
        "conversationId": "THREAD-1",
        "question": "12345678910 최근 촬영 영상 몇 개야?",
        "locale": "ko",
        "contextEntries": [
            {
                "kind": "message",
                "source": "slack",
                "authorId": "ACTOR-1",
                "text": "이전 질문",
                "createdAt": "2026-07-28T01:02:03Z",
            }
        ],
        "scope": {
            "barcode": "12345678910",
            "channelContextId": "C01",
        },
    }
    payload.update(overrides)
    return payload


def _audit_context(
    *,
    message_id: str = "1784800000.000002",
    user_name: str = "테스트 사용자",
) -> dict[str, Any]:
    """실제 Slack reply가 가진 thread/permalink identity를 고정한다."""

    thread_id = "1784800000.000001"
    return {
        "eventType": "app_mention",
        "userName": user_name,
        "channelId": "C01",
        "messageId": message_id,
        "threadId": thread_id,
        "isThreadRoot": False,
        "permalink": (
            "https://workspace.slack.com/archives/C01/"
            f"p{message_id.replace('.', '')}"
            f"?thread_ts={thread_id}&cid=C01"
        ),
        "threadPermalink": (
            "https://workspace.slack.com/archives/C01/"
            "p1784800000000001"
        ),
    }


def _operations_audit_payload(
    *,
    message_id: str = "1784800000.000002",
    operation_action: dict[str, Any] | None = None,
) -> dict[str, Any]:
    audit_context = _audit_context(message_id=message_id)
    overrides: dict[str, Any] = {
        "question": "MB2-C00419 박스 2.4.1 업데이트",
        "conversationId": audit_context["threadId"],
        "routeGroup": "operations",
        "scope": {"channelContextId": audit_context["channelId"]},
        "auditContext": audit_context,
        "contextEntries": [],
    }
    if operation_action is not None:
        overrides["operationAction"] = operation_action
    return _payload(**overrides)


def _read_request_log_rows(db_path: str) -> list[dict[str, Any]]:
    connection = sqlite3.connect(db_path)
    connection.row_factory = sqlite3.Row
    try:
        return [
            dict(row)
            for row in connection.execute(
                "SELECT * FROM request_log ORDER BY seq"
            ).fetchall()
        ]
    finally:
        connection.close()


def _direct_turn_request(
    *,
    request_id: str,
    accept_ndjson: bool,
) -> Request:
    """TestClient body buffering 없이 endpoint 연결 수명만 분리해 검증한다."""

    headers = [
        (b"authorization", f"Bearer {_TOKEN}".encode("ascii")),
        (b"x-request-id", request_id.encode("ascii")),
    ]
    if accept_ndjson:
        headers.append((b"accept", b"application/x-ndjson"))
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/internal/v1/assistant/turns",
            "raw_path": b"/internal/v1/assistant/turns",
            "query_string": b"",
            "headers": headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
    )


class CompanyApiContractTests(unittest.TestCase):
    def test_audit_context_and_request_log_receipt_reject_loose_wire_types(
        self,
    ) -> None:
        valid_receipt = _operations_audit_payload(
            operation_action={
                "name": "request_log_delivery",
                "phase": "receipt",
                "delivered": True,
                "replyCount": 1,
                "firstRepliedAtUtc": "2026-08-21T01:02:03Z",
                "errorType": None,
            }
        )
        invalid_payloads = (
            {
                **valid_receipt,
                "operationAction": {
                    **valid_receipt["operationAction"],
                    "replyCount": "1",
                },
            },
            {
                **valid_receipt,
                "auditContext": {
                    **valid_receipt["auditContext"],
                    "permalink": (
                        "https://evil.example.com/archives/C01/"
                        "p1784800000000002"
                    ),
                },
            },
            {
                **valid_receipt,
                "auditContext": {
                    **valid_receipt["auditContext"],
                    "messageId": 1784800000000002,
                },
            },
        )

        for payload in invalid_payloads:
            with self.subTest(payload=payload["auditContext"]):
                with self.assertRaises(ValueError):
                    AssistantTurnInput.model_validate(payload)

    def test_typed_diagnostic_probe_is_strict_and_reaches_operations(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_diagnostic_followup",
                outcome="no_evidence",
                messages=(AssistantMessage(body="진단 상태 없음"),),
                fallback_reason="diagnostic_snapshot_missing",
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="최근 종료 원인 알려줘",
            routeGroup="operations",
            contextEntries=[],
            operationAction={
                "name": "device_diagnostic_followup_probe",
            },
        )

        with TestClient(app) as client:
            accepted = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="diag-probe:accepted"),
                json=payload,
            )
            rejected = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="diag-probe:rejected"),
                json={
                    **payload,
                    "operationAction": {
                        "name": "device_diagnostic_followup_probe",
                        "phase": "probe",
                    },
                },
            )

        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(rejected.status_code, 422)
        self.assertEqual(rejected.json()["code"], "validation_failed")
        self.assertEqual(runtime.stages, ["operations"])
        self.assertEqual(
            runtime.requests[0].metadata["operation_action"],
            {"name": "device_diagnostic_followup_probe"},
        )
        self.assertEqual(runtime.requests[0].context_entries, ())

    def test_only_freeform_schema_accepts_blank_question(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="company_freeform",
                outcome="needs_input",
                messages=(AssistantMessage(body="질문 내용을 같이 보내줘"),),
                fallback_reason="missing_question",
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            accepted = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-empty-freeform"),
                json=_payload(
                    question="   ",
                    routeGroup="freeform",
                    contextEntries=[],
                ),
            )
            rejected = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-empty-knowledge"),
                json=_payload(
                    question="   ",
                    routeGroup="knowledge",
                    contextEntries=[],
                ),
            )

        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(accepted.json()["fallbackReason"], "missing_question")
        self.assertEqual(rejected.status_code, 422)
        self.assertEqual(rejected.json()["code"], "validation_failed")
        self.assertEqual(runtime.stages, ["freeform"])
        self.assertEqual(runtime.requests[0].question, "")

    def test_fun_context_is_typed_and_keeps_five_k_verbatim(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="company_team_fun",
                outcome="answered",
                messages=(AssistantMessage(body="배포도 쉽지 않모대?"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        rendered_context = ("오" * 4_000) + "최신핵심" + ("신" * 996)

        with TestClient(app) as client:
            accepted = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-fun-context"),
                json=_payload(
                    question="배포도 쉽지 모대",
                    routeGroup="fun",
                    contextEntries=[],
                    funContext=rendered_context,
                ),
            )
            rejected = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-fun-context-wrong-stage"),
                json=_payload(
                    question="일반 질문",
                    routeGroup="freeform",
                    contextEntries=[],
                    funContext=rendered_context,
                ),
            )

        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(rejected.status_code, 422)
        self.assertEqual(
            runtime.requests[0].metadata["team_fun_context"],
            rendered_context,
        )

    def test_live_device_feature_off_blocks_only_live_routes(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="app_user_lookup",
                outcome="answered",
                messages=(AssistantMessage(body="조회 결과"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.device.probe",
                    "assistant.device.ssh.open",
                    "assistant.operation.execute",
                    "assistant.device.alert.execute",
                ),
                live_device_enabled=False,
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            device_detail = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-live-detail-off"),
                json=_payload(
                    question="MB2-C00419 장비 정보",
                    routeGroup="device_detail",
                    scope={"deviceName": "MB2-C00419"},
                ),
            )
            device_operation = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-live-operation-off"),
                json=_payload(
                    question="MB2-C00419 장비 종료해줘",
                    routeGroup="operations",
                    scope={"deviceName": "MB2-C00419"},
                ),
            )
            private_read = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-private-read-on"),
                json=_payload(
                    question="12345678910 유저 조회",
                    routeGroup="operations",
                ),
            )

        self.assertEqual(device_detail.status_code, 503)
        self.assertFalse(device_detail.json()["retryable"])
        self.assertEqual(device_operation.status_code, 503)
        self.assertFalse(device_operation.json()["retryable"])
        self.assertEqual(private_read.status_code, 200)
        self.assertEqual(len(runtime.requests), 1)

    def test_operations_feature_off_blocks_before_runtime(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="app_user_lookup",
                outcome="answered",
                messages=(AssistantMessage(body="조회 결과"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
                operations_enabled=False,
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-operations-off"),
                json=_payload(
                    question="12345678910 유저 조회",
                    routeGroup="operations",
                ),
            )

        self.assertEqual(response.status_code, 503)
        self.assertFalse(response.json()["retryable"])
        self.assertEqual(runtime.requests, [])

    def test_operation_request_id_replay_returns_cached_result_once(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_power_off",
                outcome="answered",
                messages=(AssistantMessage(body="종료 요청 완료"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="MB2-C00419 장비 종료해줘",
            routeGroup="operations",
            scope={"deviceName": "MB2-C00419", "channelContextId": "C01"},
        )

        with TestClient(app) as client:
            first = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=payload,
            )
            replay = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=payload,
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(replay.json(), first.json())
        self.assertEqual(len(runtime.requests), 1)

    def test_download_delivery_ack_reuses_request_id_after_initial_turn(
        self,
    ) -> None:
        download_uri = "https://download.example/a.motion.mp4?token=opaque"
        delivery = {
            "barcode": "48194663047",
            "logDate": "2026-03-06",
            "usedExpandedScope": False,
            "records": [
                {
                    "deviceName": "MB2-C00419",
                    "deviceSeq": 41,
                    "hospitalSeq": 5,
                    "hospitalRoomSeq": 8,
                    "hospitalName": "테스트병원",
                    "roomName": "1진료실",
                    "fileNames": ["a.motion.mp4"],
                    "downloadFileNames": ["a.motion.mp4"],
                }
            ],
        }
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_file_download",
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body="**장비 영상 다운로드 결과**",
                        delivery_scope="requester",
                        mention_actor=False,
                        private_links=(
                            AssistantLink(
                                label="a.motion.mp4",
                                uri=download_uri,
                            ),
                        ),
                    ),
                ),
                operation_result={
                    "kind": "device_file_download_delivery",
                    "status": "pending",
                    "failureNotice": "DM 전송 실패",
                    "linkCount": 1,
                    "links": [
                        {
                            "deviceName": "MB2-C00419",
                            "fileName": "a.motion.mp4",
                        }
                    ],
                    "delivery": delivery,
                },
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        request_id = "req-download-delivery-same-id"
        initial_payload = _payload(
            question="48194663047 2026-03-06 영상 다운로드",
            routeGroup="operations",
            scope={"channelContextId": "C01"},
        )

        with patch(
            "boxer_company_api.app._persist_turn_request_log",
            return_value=True,
        ) as persist_request_log:
            with TestClient(app) as client:
                initial = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id=request_id),
                    json=initial_payload,
                )
                runtime.result = CompanyAssistantResult(
                    route="device_file_download",
                    outcome="answered",
                    messages=(AssistantMessage(body="DM으로 보냈어"),),
                )
                receipt = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id=request_id),
                    json={
                        **initial_payload,
                        "operationAction": {
                            "name": "device_file_download_delivery",
                            "phase": "delivered",
                            "delivery": initial.json()["operationResult"][
                                "delivery"
                            ],
                        },
                    },
                )

        self.assertEqual(initial.status_code, 200)
        self.assertEqual(receipt.status_code, 200)
        self.assertEqual(len(runtime.requests), 2)
        self.assertEqual(runtime.stages, ["operations", "operations"])
        self.assertEqual(
            runtime.requests[1].metadata["operation_action"],
            {
                "name": "device_file_download_delivery",
                "phase": "delivered",
                "delivery": {
                    "barcode": "48194663047",
                    "log_date": "2026-03-06",
                    "used_expanded_scope": False,
                    "records": [
                        {
                            "device_name": "MB2-C00419",
                            "device_seq": 41,
                            "hospital_seq": 5,
                            "hospital_room_seq": 8,
                            "hospital_name": "테스트병원",
                            "room_name": "1진료실",
                            "file_names": ["a.motion.mp4"],
                            "download_file_names": ["a.motion.mp4"],
                        }
                    ],
                },
            },
        )
        persist_request_log.assert_called_once()

    def test_device_operation_delivery_ack_reuses_request_id_and_guard(
        self,
    ) -> None:
        delivery = {
            "route": "device_box_update",
            "deviceName": "MB2-C00419",
            "requestedVersion": "2.11.300",
            "currentBoxVersion": "2.11.299",
            "dispatchMessage": "dispatch accepted",
            "waitStatus": "completed",
            "waitOk": True,
        }
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_box_update",
                outcome="answered",
                messages=(AssistantMessage(body="장비 업데이트 완료"),),
                operation_result={
                    "kind": "device_operation_delivery",
                    "status": "pending",
                    "delivery": delivery,
                },
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        request_id = "req-device-operation-delivery-same-id"
        initial_payload = _payload(
            question="MB2-C00419 박스 2.11.300 버전으로 업데이트해줘",
            routeGroup="operations",
            scope={
                "deviceName": "MB2-C00419",
                "channelContextId": "C01",
            },
        )

        with patch(
            "boxer_company_api.app._persist_turn_request_log",
            return_value=True,
        ) as persist_request_log:
            with TestClient(app) as client:
                initial = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id=request_id),
                    json=initial_payload,
                )
                runtime.result = CompanyAssistantResult(
                    route="device_operation_delivery",
                    outcome="answered",
                    messages=(
                        AssistantMessage(
                            body="장비 작업 전달 결과를 확인했어",
                            mention_actor=False,
                        ),
                    ),
                )
                receipt = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id=request_id),
                    json={
                        **initial_payload,
                        "operationAction": {
                            "name": "device_operation_delivery",
                            "phase": "delivered",
                            "delivery": initial.json()["operationResult"][
                                "delivery"
                            ],
                        },
                    },
                )

        self.assertEqual(initial.status_code, 200)
        self.assertEqual(
            initial.json()["operationResult"],
            {
                "kind": "device_operation_delivery",
                "status": "pending",
                "delivery": delivery,
            },
        )
        self.assertEqual(receipt.status_code, 200)
        self.assertEqual(len(runtime.requests), 2)
        self.assertEqual(runtime.stages, ["operations", "operations"])
        self.assertEqual(
            runtime.requests[1].metadata["operation_action"],
            {
                "name": "device_operation_delivery",
                "phase": "delivered",
                "delivery": {
                    "route": "device_box_update",
                    "device_name": "MB2-C00419",
                    "requested_version": "2.11.300",
                    "current_box_version": "2.11.299",
                    "dispatch_message": "dispatch accepted",
                    "wait_status": "completed",
                    "wait_ok": True,
                },
            },
        )
        # initial operation만 중앙 감사 로그를 남기고 같은 ID receipt는
        # route 자체 멱등 guard만 사용한다.
        persist_request_log.assert_called_once()

    def test_device_operation_delivery_schema_rejects_invalid_manifest(
        self,
    ) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        valid_delivery = {
            "route": "device_box_update",
            "deviceName": "MB2-C00419",
            "requestedVersion": "2.11.300",
            "currentBoxVersion": "2.11.299",
            "dispatchMessage": "dispatch accepted",
            "waitStatus": "completed",
            "waitOk": True,
        }
        invalid_deliveries = (
            {
                key: value
                for key, value in valid_delivery.items()
                if key != "currentBoxVersion"
            },
            {**valid_delivery, "waitStatus": "timed_out"},
            {**valid_delivery, "requestedVersion": ""},
            {**valid_delivery, "unexpected": "field"},
        )

        with TestClient(app) as client:
            for index, delivery in enumerate(invalid_deliveries):
                with self.subTest(index=index):
                    response = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(
                            request_id=f"req-invalid-device-receipt-{index}"
                        ),
                        json=_payload(
                            question=(
                                "MB2-C00419 박스 2.11.300 버전으로 업데이트해줘"
                            ),
                            routeGroup="operations",
                            operationAction={
                                "name": "device_operation_delivery",
                                "phase": "delivered",
                                "delivery": delivery,
                            },
                        ),
                    )
                    self.assertEqual(response.status_code, 422)
                    self.assertEqual(
                        response.json()["code"],
                        "validation_failed",
                    )

        self.assertEqual(runtime.requests, [])

    def test_ndjson_stream_emits_partial_then_final_with_strict_frames(
        self,
    ) -> None:
        partial = CompanyAssistantResult(
            route="barcode_log_analysis",
            outcome="answered",
            messages=(AssistantMessage(body="DB/S3 분석 결과"),),
        )
        final = CompanyAssistantResult(
            route="barcode_log_analysis",
            outcome="answered",
            messages=(AssistantMessage(body="오류 요약"),),
        )

        class _ProgressRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
                *,
                on_partial_result: Any = None,
            ) -> CompanyAssistantResult:
                self.requests.append(request)
                self.stages.append(stage)
                self.assert_callback_order.append("runtime_started")
                assert callable(on_partial_result)
                on_partial_result(partial)
                self.assert_callback_order.append("partial_returned")
                return final

        runtime = _ProgressRuntime()
        runtime.assert_callback_order = []
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        request_id = "req-ndjson-partial-final"
        headers = {
            **_headers(request_id=request_id),
            "Accept": "application/x-ndjson",
        }

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=headers,
                json=_payload(
                    question="12345678910 로그 분석해줘",
                    routeGroup="log",
                ),
            )

        frames = [
            json.loads(line)
            for line in response.content.decode("utf-8").splitlines()
        ]
        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.headers["content-type"],
            "application/x-ndjson",
        )
        self.assertEqual(response.headers["cache-control"], "no-store")
        self.assertEqual(response.headers["x-accel-buffering"], "no")
        self.assertEqual([frame["type"] for frame in frames], ["partial", "final"])
        self.assertEqual(set(frames[0]), {"type", "result"})
        self.assertEqual(set(frames[1]), {"type", "result"})
        self.assertEqual(frames[0]["result"]["requestId"], request_id)
        self.assertEqual(frames[0]["result"]["route"], "barcode_log_analysis")
        self.assertEqual(frames[1]["result"]["requestId"], request_id)
        self.assertEqual(frames[1]["result"]["messages"][0]["body"], "오류 요약")
        self.assertEqual(
            runtime.assert_callback_order,
            ["runtime_started", "partial_returned"],
        )

    def test_ndjson_stream_emits_heartbeat_while_runtime_is_blocked(
        self,
    ) -> None:
        final = CompanyAssistantResult(
            route="barcode_log_analysis",
            outcome="answered",
            messages=(AssistantMessage(body="완료"),),
        )
        release = threading.Event()

        class _BlockingRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
                *,
                on_partial_result: Any = None,
            ) -> CompanyAssistantResult:
                del on_partial_result
                self.requests.append(request)
                self.stages.append(stage)
                release.wait(0.04)
                return final

        runtime = _BlockingRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        request_id = "req-ndjson-heartbeat"

        with patch("boxer_company_api.app._STREAM_HEARTBEAT_SEC", 0.005):
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        **_headers(request_id=request_id),
                        "Accept": "application/x-ndjson",
                    },
                    json=_payload(
                        question="12345678910 로그 분석해줘",
                        routeGroup="log",
                    ),
                )

        frames = [
            json.loads(line)
            for line in response.content.decode("utf-8").splitlines()
        ]
        self.assertEqual(frames[-1]["type"], "final")
        heartbeat_frames = [
            frame for frame in frames if frame["type"] == "heartbeat"
        ]
        self.assertGreaterEqual(len(heartbeat_frames), 1)
        self.assertTrue(
            all(
                frame == {"type": "heartbeat", "requestId": request_id}
                for frame in heartbeat_frames
            )
        )

    def test_ndjson_runtime_failure_uses_only_safe_problem_frame(self) -> None:
        runtime = _FakeRuntime(error=RuntimeError("secret runtime detail"))
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        request_id = "req-ndjson-safe-error"

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    **_headers(request_id=request_id),
                    "Accept": "application/x-ndjson",
                },
                json=_payload(
                    question="12345678910 로그 분석해줘",
                    routeGroup="log",
                ),
            )

        frames = [
            json.loads(line)
            for line in response.content.decode("utf-8").splitlines()
        ]
        self.assertEqual(len(frames), 1)
        self.assertEqual(set(frames[0]), {"type", "problem"})
        self.assertEqual(frames[0]["type"], "error")
        self.assertEqual(
            set(frames[0]["problem"]),
            {
                "type",
                "title",
                "status",
                "code",
                "requestId",
                "retryable",
            },
        )
        self.assertEqual(frames[0]["problem"]["code"], "internal_error")
        self.assertEqual(frames[0]["problem"]["requestId"], request_id)
        self.assertNotIn("secret runtime detail", response.text)

    def test_ndjson_stream_enforces_one_total_byte_budget(self) -> None:
        partial = CompanyAssistantResult(
            route="barcode_log_analysis",
            outcome="answered",
            messages=(AssistantMessage(body="p" * 600),),
        )
        final = CompanyAssistantResult(
            route="barcode_log_analysis",
            outcome="answered",
            messages=(AssistantMessage(body="f" * 600),),
        )

        class _LargeProgressRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
                *,
                on_partial_result: Any = None,
            ) -> CompanyAssistantResult:
                self.requests.append(request)
                self.stages.append(stage)
                assert callable(on_partial_result)
                on_partial_result(partial)
                return final

        runtime = _LargeProgressRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        request_id = "req-ndjson-byte-budget"

        # 작은 동일 비율 budget으로 검증해 실제 1 MiB 경계가 부분 결과의
        # 단순 합으로 뚫리지 않고 safe terminal error 공간을 남기는지 확인한다.
        with patch("boxer_company_api.app._MAX_STREAM_BYTES", 1_024):
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers={
                        **_headers(request_id=request_id),
                        "Accept": "application/x-ndjson",
                    },
                    json=_payload(
                        question="12345678910 로그 분석해줘",
                        routeGroup="log",
                    ),
                )

        frames = [
            json.loads(line)
            for line in response.content.decode("utf-8").splitlines()
        ]
        self.assertLessEqual(len(response.content), 1_024)
        self.assertEqual(frames[-1]["type"], "error")
        self.assertEqual(frames[-1]["problem"]["code"], "internal_error")
        self.assertFalse(any(frame["type"] == "final" for frame in frames))

    def test_disconnected_ndjson_consumer_does_not_cancel_worker_finalization(
        self,
    ) -> None:
        started = threading.Event()
        release = threading.Event()
        finalized = threading.Event()
        final = CompanyAssistantResult(
            route="device_box_update",
            outcome="answered",
            messages=(AssistantMessage(body="업데이트 완료"),),
        )

        class _BlockingMutationRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
                *,
                on_partial_result: Any = None,
            ) -> CompanyAssistantResult:
                self.requests.append(request)
                self.stages.append(stage)
                assert callable(on_partial_result)
                on_partial_result(
                    CompanyAssistantResult(
                        route="device_box_update",
                        outcome="answered",
                        messages=(AssistantMessage(body="업데이트 진행 중"),),
                    )
                )
                started.set()
                if not release.wait(2):
                    raise RuntimeError("test release timeout")
                return final

        runtime = _BlockingMutationRuntime()
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        endpoint = next(
            route.endpoint
            for route in app.routes
            if getattr(route, "path", None)
            == "/internal/v1/assistant/turns"
        )
        request_id = "req-ndjson-disconnect"
        turn = AssistantTurnInput.model_validate(
            _payload(
                question="MB2-C00419 박스 2.11.300 버전으로 업데이트해줘",
                routeGroup="operations",
                scope={
                    "deviceName": "MB2-C00419",
                    "channelContextId": "C01",
                },
            )
        )

        def observe_event(event: str, **_fields: Any) -> None:
            if event == "company_api_turn_completed":
                finalized.set()

        with patch(
            "boxer_company_api.app.emit_api_event",
            side_effect=observe_event,
        ):
            response = endpoint(
                _direct_turn_request(
                    request_id=request_id,
                    accept_ndjson=True,
                ),
                turn,
            )
            try:
                self.assertTrue(started.wait(1))
                # body iterator를 읽지 않고 닫아 client disconnect를 모사해도
                # 별도 daemon worker는 요청 소유권과 마감을 유지한다.
                asyncio.run(response.body_iterator.aclose())
            finally:
                release.set()
            self.assertTrue(finalized.wait(1))
            replay = endpoint(
                _direct_turn_request(
                    request_id=request_id,
                    accept_ndjson=False,
                ),
                turn,
            )

        self.assertEqual(replay.status_code, 200)
        replay_payload = json.loads(bytes(replay.body).decode("utf-8"))
        self.assertEqual(replay_payload["route"], "device_box_update")
        self.assertEqual(len(runtime.requests), 1)

    def test_ndjson_marks_uncertain_mutation_before_final_frame(self) -> None:
        uncertain = CompanyAssistantResult(
            route="device_power_off",
            outcome="failed",
            messages=(AssistantMessage(body="종료 결과를 확인하지 못했어"),),
            fallback_reason="operation_error",
        )

        class _UncertainMutationRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
                *,
                on_partial_result: Any = None,
            ) -> CompanyAssistantResult:
                from boxer_company.routers.device_ssh_security import (
                    _mark_company_api_mutation_attempted,
                )

                del on_partial_result
                self.requests.append(request)
                self.stages.append(stage)
                _mark_company_api_mutation_attempted()
                return uncertain

        runtime = _UncertainMutationRuntime()
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        request_id = "req-ndjson-uncertain-final"
        payload = _payload(
            question="MB2-C00419 장비 종료해줘",
            routeGroup="operations",
        )

        with TestClient(app) as client:
            streamed = client.post(
                "/internal/v1/assistant/turns",
                headers={
                    **_headers(request_id=request_id),
                    "Accept": "application/x-ndjson",
                },
                json=payload,
            )
            replay = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id=request_id),
                json=payload,
            )

        frames = [
            json.loads(line)
            for line in streamed.content.decode("utf-8").splitlines()
        ]
        self.assertEqual(frames[-1]["type"], "final")
        self.assertEqual(frames[-1]["result"]["outcome"], "failed")
        # final 관찰 시점에는 이미 uncertain marker가 고정돼 같은 ID를
        # 다시 실행할 수 없어야 한다.
        self.assertEqual(replay.status_code, 409)
        self.assertEqual(replay.json()["code"], "operation_in_progress")
        self.assertEqual(len(runtime.requests), 1)

    def test_operation_request_id_conflict_fails_before_second_runtime(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_power_off",
                outcome="answered",
                messages=(AssistantMessage(body="종료 요청 완료"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            first = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(
                    question="MB2-C00419 장비 종료해줘",
                    routeGroup="operations",
                ),
            )
            conflict = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(
                    question="MB2-C00570 장비 종료해줘",
                    routeGroup="operations",
                ),
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(conflict.status_code, 409)
        self.assertEqual(conflict.json()["code"], "request_id_conflict")
        self.assertEqual(len(runtime.requests), 1)

    def test_failed_operation_request_stays_uncertain_without_reexecution(self) -> None:
        class _AttemptedMutationRuntime(_FakeRuntime):
            def answer_stage(self, request: Any, stage: str) -> CompanyAssistantResult:
                from boxer_company.routers.device_ssh_security import (
                    _mark_company_api_mutation_attempted,
                )

                self.requests.append(request)
                self.stages.append(stage)
                # 실제 write 직전 marker가 있으면 예외가 결과 불명 상태다.
                _mark_company_api_mutation_attempted()
                raise RuntimeError("mutation status unknown")

        runtime = _AttemptedMutationRuntime()
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="MB2-C00419 장비 종료해줘",
            routeGroup="operations",
        )

        with TestClient(app, raise_server_exceptions=False) as client:
            first = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=payload,
            )
            replay = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=payload,
            )

        self.assertEqual(first.status_code, 500)
        self.assertFalse(first.json()["retryable"])
        self.assertEqual(replay.status_code, 409)
        self.assertEqual(replay.json()["code"], "operation_in_progress")
        self.assertEqual(len(runtime.requests), 1)

    def test_precheck_operation_exception_releases_target(self) -> None:
        runtime = _FakeRuntime(error=RuntimeError("precheck unavailable"))
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="MB2-C00419 장비 종료해줘",
            routeGroup="operations",
        )

        with TestClient(app, raise_server_exceptions=False) as client:
            first = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-precheck-exception-1"),
                json=payload,
            )
            second = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-precheck-exception-2"),
                json=payload,
            )

        self.assertEqual(first.status_code, 500)
        self.assertTrue(first.json()["retryable"])
        self.assertEqual(second.status_code, 500)
        self.assertTrue(second.json()["retryable"])
        self.assertEqual(len(runtime.requests), 2)

    def test_read_only_operation_failure_does_not_lock_later_mutation(self) -> None:
        # app-user/S3/admin 조회 예외는 mutation registry를 만들지 않고,
        # 서로 다른 실제 mutation도 기존 Slack처럼 각각 실행한다.
        read_only_questions = (
            "12345678910 유저 조회",
            "s3 영상 12345678910",
            "db 조회 select seq from recordings limit 1",
        )
        for index, read_only_question in enumerate(read_only_questions):
            with self.subTest(question=read_only_question):
                class _ConditionalAttemptRuntime(_FakeRuntime):
                    def answer_stage(
                        self,
                        request: Any,
                        stage: str,
                    ) -> CompanyAssistantResult:
                        self.requests.append(request)
                        self.stages.append(stage)
                        if "장비 종료" in request.question:
                            from boxer_company.routers.device_ssh_security import (
                                _mark_company_api_mutation_attempted,
                            )

                            # read-only precheck와 달리 mutation 전송 뒤 오류를
                            # 재현해 tenant-wide 불명 lock 계약을 검증한다.
                            _mark_company_api_mutation_attempted()
                        raise RuntimeError("dependency failed")

                runtime = _ConditionalAttemptRuntime()
                app = create_company_api_app(
                    settings=_settings(
                        capabilities=(
                            "assistant.turn.read",
                            "assistant.operation.execute",
                        )
                    ),
                    assistant_runtime=runtime,
                    readiness_probe=lambda: True,
                )

                with TestClient(
                    app,
                    raise_server_exceptions=False,
                ) as client:
                    read_failure = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(request_id=f"req-read-{index}"),
                        json=_payload(
                            question=read_only_question,
                            routeGroup="operations",
                        ),
                    )
                    mutation_failure = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(request_id=f"req-mutation-{index}"),
                        json=_payload(
                            question="MB2-C00419 장비 종료해줘",
                            routeGroup="operations",
                        ),
                    )
                    next_mutation = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(request_id=f"req-next-{index}"),
                        json=_payload(
                            question="MB2-C00570 장비 종료해줘",
                            routeGroup="operations",
                        ),
                    )

                self.assertEqual(read_failure.status_code, 500)
                self.assertTrue(read_failure.json()["retryable"])
                self.assertEqual(mutation_failure.status_code, 500)
                self.assertFalse(mutation_failure.json()["retryable"])
                self.assertEqual(next_mutation.status_code, 500)
                self.assertFalse(next_mutation.json()["retryable"])
                self.assertEqual(len(runtime.requests), 3)

    def test_failed_mutation_result_blocks_only_the_same_request_id(self) -> None:
        # route 내부 catch가 HTTP 예외 대신 failed 결과를 반환해도 실제
        # mutation 처리 여부가 불명인 동일 request ID만 재실행하지 않는다.
        result = CompanyAssistantResult(
            route="device_power_off",
            outcome="failed",
            messages=(AssistantMessage(body="처리 여부 불명"),),
            fallback_reason="operation_error",
        )

        class _AttemptedMutationRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
            ) -> CompanyAssistantResult:
                from boxer_company.routers.mda_graphql import (
                    _send_mda_device_command,
                )

                self.requests.append(request)
                self.stages.append(stage)
                try:
                    _send_mda_device_command(
                        "MB2-C00419",
                        command="power_off",
                    )
                except TimeoutError:
                    pass
                return result

        runtime = _AttemptedMutationRuntime()
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="MB2-C00419 장비 종료해줘",
            routeGroup="operations",
        )

        with patch(
            "boxer_company.routers.mda_graphql._get_mda_access_token",
            return_value="access-token",
        ), patch(
            "boxer_company.routers.mda_graphql._execute_mda_graphql_request",
            side_effect=TimeoutError("result unknown"),
        ):
            with TestClient(app) as client:
                first = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-uncertain-result-1"),
                    json=payload,
                )
                second = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-uncertain-result-2"),
                    json=payload,
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json()["outcome"], "failed")
        self.assertEqual(second.status_code, 200)
        self.assertEqual(second.json()["outcome"], "failed")
        self.assertEqual(len(runtime.requests), 2)

    def test_precheck_operation_error_releases_mutation_target(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_file_lookup",
                outcome="failed",
                messages=(AssistantMessage(body="사전 조회 실패"),),
                fallback_reason="operation_error",
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="12345678910 2026-03-06 영상 다운로드",
            routeGroup="operations",
        )

        with TestClient(app) as client:
            first = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-operation-precheck-1"),
                json=payload,
            )
            second = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-operation-precheck-2"),
                json=payload,
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(runtime.requests), 2)

    def test_known_precheck_failure_releases_mutation_target(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="recording_streaming_restore",
                outcome="denied",
                messages=(AssistantMessage(body="기능 비활성"),),
                fallback_reason="feature_disabled",
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="12345678910 2026년 3월 영상 복원",
            routeGroup="operations",
        )

        with TestClient(app) as client:
            first = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-known-failure-1"),
                json=payload,
            )
            second = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-known-failure-2"),
                json=payload,
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(runtime.requests), 2)

    def test_device_detail_preopen_failure_releases_mutation_target(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_detail",
                outcome="failed",
                messages=(AssistantMessage(body="DB 조회 실패"),),
                fallback_reason="dependency_error",
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.device.probe",
                    "assistant.device.ssh.open",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="MB2-C00419 장비 정보",
            routeGroup="device_detail",
        )

        with TestClient(app) as client:
            first = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-detail-preopen-1"),
                json=payload,
            )
            second = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-detail-preopen-2"),
                json=payload,
            )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(runtime.requests), 2)

    def test_device_detail_unknown_failure_blocks_only_same_request_id(
        self,
    ) -> None:
        result = CompanyAssistantResult(
            route="device_detail",
            outcome="failed",
            messages=(AssistantMessage(body="SSH open 결과 불명"),),
            fallback_reason="dependency_error",
        )

        class _SshOpenFailureRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
            ) -> CompanyAssistantResult:
                from boxer_company.routers.mda_graphql import (
                    _open_mda_device_ssh,
                )

                self.requests.append(request)
                self.stages.append(stage)
                try:
                    _open_mda_device_ssh(
                        "MB2-C00419",
                        host="private-ssh-host",
                    )
                except TimeoutError:
                    pass
                return result

        runtime = _SshOpenFailureRuntime()
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.device.probe",
                    "assistant.device.ssh.open",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="MB2-C00419 장비 정보",
            routeGroup="device_detail",
        )

        with patch(
            "boxer_company.routers.mda_graphql._get_mda_access_token",
            return_value="access-token",
        ), patch(
            "boxer_company.routers.mda_graphql._execute_mda_graphql_request",
            side_effect=TimeoutError("result unknown"),
        ):
            with TestClient(app) as client:
                first = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-detail-open-1"),
                    json=payload,
                )
                second = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-detail-open-2"),
                    json=payload,
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(runtime.requests), 2)

    def test_device_detail_auth_failure_before_ssh_open_releases_target(
        self,
    ) -> None:
        result = CompanyAssistantResult(
            route="device_detail",
            outcome="failed",
            messages=(AssistantMessage(body="MDA 인증 실패"),),
            fallback_reason="dependency_error",
        )

        class _SshAuthFailureRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
            ) -> CompanyAssistantResult:
                from boxer_company.routers.mda_graphql import (
                    _open_mda_device_ssh,
                )

                self.requests.append(request)
                self.stages.append(stage)
                try:
                    _open_mda_device_ssh(
                        "MB2-C00419",
                        host="private-ssh-host",
                    )
                except TimeoutError:
                    pass
                return result

        runtime = _SshAuthFailureRuntime()
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.device.probe",
                    "assistant.device.ssh.open",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload(
            question="MB2-C00419 장비 정보",
            routeGroup="device_detail",
        )

        with patch(
            "boxer_company.routers.mda_graphql._get_mda_access_token",
            side_effect=TimeoutError("auth unavailable"),
        ):
            with TestClient(app) as client:
                first = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-detail-auth-1"),
                    json=payload,
                )
                second = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-detail-auth-2"),
                    json=payload,
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)
        self.assertEqual(len(runtime.requests), 2)

    def test_route_group_executes_only_the_requested_runtime_stage(
        self,
    ) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="notion_playbook_qa",
                outcome="answered",
                messages=(AssistantMessage(body="운영 문서 답변"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(routeGroup="knowledge"),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "notion_playbook_qa")
        self.assertEqual(runtime.stages, ["knowledge"])
        self.assertEqual(len(runtime.requests), 1)

    def test_unknown_route_group_is_rejected_before_runtime(self) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(routeGroup="unsafe"),
            )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["code"], "validation_failed")
        self.assertEqual(runtime.requests, [])

    def test_freeform_route_group_runs_only_freeform_stage(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="company_freeform",
                outcome="answered",
                messages=(AssistantMessage(body="일반 답변"),),
                used_llm=True,
            )
        )
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(
                    question="오늘 기분 어때?",
                    routeGroup="freeform",
                ),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "company_freeform")
        self.assertEqual(runtime.stages, ["freeform"])
        self.assertEqual(
            runtime.requests[0].metadata["route_group"],
            "freeform",
        )

    def test_health_and_fun_route_groups_share_only_freeform_runtime_stage(
        self,
    ) -> None:
        # wire routeGroup은 관찰·matcher 경계로 보존하고 실제 route graph는
        # provider-backed freeform stage 하나만 실행한다.
        cases = (
            ("health", "company_llm_health", "available", False),
            ("fun", "company_team_fun", "배포도 쉽지 모대?", True),
        )
        for route_group, route_name, body, used_llm in cases:
            with self.subTest(route_group=route_group):
                runtime = _FakeRuntime(
                    CompanyAssistantResult(
                        route=route_name,
                        outcome="answered",
                        messages=(AssistantMessage(body=body),),
                        used_llm=used_llm,
                    )
                )
                app = create_company_api_app(
                    settings=_settings(),
                    assistant_runtime=runtime,
                    readiness_probe=lambda: True,
                )

                with TestClient(app) as client:
                    response = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(
                            request_id=f"req-{route_group}-route"
                        ),
                        json=_payload(
                            question=(
                                "ping"
                                if route_group == "health"
                                else "배포도 쉽지 모대"
                            ),
                            routeGroup=route_group,
                        ),
                    )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["route"], route_name)
                self.assertEqual(runtime.stages, ["freeform"])
                self.assertEqual(
                    runtime.requests[0].metadata["route_group"],
                    route_group,
                )

    def test_device_detail_requires_probe_and_open_capabilities_before_runtime(
        self,
    ) -> None:
        # 단일 turn이 probe와 필요 시 tunnel open까지 수행하므로 어느 한
        # 권한이라도 빠지면 runtime 호출 전에 거절해야 한다.
        for capabilities in (
            ("assistant.turn.read",),
            ("assistant.turn.read", "assistant.device.probe"),
            ("assistant.turn.read", "assistant.device.ssh.open"),
        ):
            with self.subTest(capabilities=capabilities):
                runtime = _FakeRuntime()
                app = create_company_api_app(
                    settings=_settings(capabilities=capabilities),
                    assistant_runtime=runtime,
                    readiness_probe=lambda: True,
                )

                with TestClient(app) as client:
                    response = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(),
                        json=_payload(routeGroup="device_detail"),
                    )

                self.assertEqual(response.status_code, 403)
                self.assertEqual(
                    response.json()["code"],
                    "caller_not_allowed",
                )
                self.assertEqual(runtime.requests, [])
                self.assertEqual(runtime.stages, [])

    def test_device_detail_maps_to_structured_and_preserves_wire_scope(
        self,
    ) -> None:
        # transport의 별도 권한·관찰 범위는 device_detail로 유지하면서
        # 회사 runtime에는 기존 structured stage로 안전하게 연결한다.
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_detail",
                outcome="answered",
                messages=(AssistantMessage(body="장비 상세 결과"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.device.probe",
                    "assistant.device.ssh.open",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with patch(
            "boxer_company_api.app.emit_api_event"
        ) as emit_api_event:
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_payload(routeGroup="device_detail"),
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "device_detail")
        self.assertEqual(runtime.stages, ["structured"])
        self.assertEqual(len(runtime.requests), 1)
        self.assertEqual(
            runtime.requests[0].metadata["route_group"],
            "device_detail",
        )
        completed_events = [
            call
            for call in emit_api_event.call_args_list
            if call.args == ("company_api_turn_completed",)
        ]
        self.assertEqual(len(completed_events), 1)
        self.assertEqual(
            completed_events[0].kwargs["route_group"],
            "device_detail",
        )

    def test_operations_requires_execute_capability_before_runtime(
        self,
    ) -> None:
        # 기본 turn read 권한만으로는 mutation stage를 실행할
        # 수 없고 runtime 진입 전에 fail-closed한다.
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(routeGroup="operations"),
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(response.json()["code"], "caller_not_allowed")
        self.assertEqual(runtime.requests, [])
        self.assertEqual(runtime.stages, [])

    def test_operations_capability_runs_only_operations_stage(
        self,
    ) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_update_operation",
                outcome="answered",
                messages=(AssistantMessage(body="작업 접수"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(routeGroup="operations"),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "device_update_operation")
        self.assertEqual(runtime.stages, ["operations"])
        self.assertEqual(
            runtime.requests[0].metadata["route_group"],
            "operations",
        )

    def test_success_persists_central_api_request_log_best_effort(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="barcode_query",
                outcome="answered",
                messages=(AssistantMessage(body="조회 결과"),),
            )
        )
        with patch(
            "boxer_company_api.app._initialize_request_log_readiness",
            return_value=True,
        ), patch(
            "boxer_company_api.app._secure_request_log_leaf",
            return_value=True,
        ), patch(
            "boxer_company_api.app._ensure_request_log_schema"
        ), patch(
            "boxer_company_api.app._save_request_log_record"
        ) as save_request_log:
            app = create_company_api_app(
                settings=_settings(request_log_enabled=True),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_payload(routeGroup="barcode"),
                )

        self.assertEqual(response.status_code, 200)
        save_request_log.assert_called_once()
        record = save_request_log.call_args.args[0]
        self.assertEqual(record["sourcePlatform"], "slack")
        self.assertEqual(record["workspaceId"], "TENANT-1")
        self.assertEqual(record["channelId"], "C01")
        self.assertEqual(record["messageId"], _REQUEST_ID)
        self.assertEqual(record["routeName"], "barcode_query")
        self.assertEqual(record["status"], "answered")
        self.assertEqual(record["replyCount"], 1)
        self.assertEqual(
            record["requestText"],
            "12345678910 최근 촬영 영상 몇 개야?",
        )
        self.assertEqual(
            record["normalizedQuestion"],
            "12345678910 최근 촬영 영상 몇 개야?",
        )

    def test_blank_freeform_persists_bot_only_mention_request_log(
        self,
    ) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="company_freeform",
                outcome="needs_input",
                messages=(AssistantMessage(body="질문 내용을 같이 보내줘"),),
                fallback_reason="missing_question",
            )
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            app = create_company_api_app(
                settings=_settings(
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-empty-freeform-log"),
                    json=_payload(
                        question="   ",
                        routeGroup="freeform",
                        contextEntries=[],
                    ),
                )
                rows = _read_request_log_rows(db_path)
                readiness = client.get("/health/ready")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "company_freeform")
        self.assertEqual(response.json()["outcome"], "needs_input")
        self.assertEqual(readiness.status_code, 200)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        # legacy Slack bot-only mention은 질문 정규화 결과가 없었으므로,
        # 원문 대신 비민감 marker만 남기고 normalizedQuestion은 비워 둔다.
        self.assertEqual(row["requestText"], "[Boxer만 멘션한 요청]")
        self.assertIsNone(row["normalizedQuestion"])
        self.assertEqual(row["routeName"], "llm_freeform")
        self.assertEqual(row["status"], "needs_input")
        self.assertEqual(row["replyCount"], 1)
        self.assertEqual(
            json.loads(row["metadataJson"])["domainOutcome"],
            "needs_input",
        )

    def test_request_log_failure_latches_readiness_after_success_response(
        self,
    ) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="barcode_query",
                outcome="answered",
                messages=(AssistantMessage(body="조회 결과"),),
            )
        )
        with patch(
            "boxer_company_api.app._initialize_request_log_readiness",
            return_value=True,
        ), patch(
            "boxer_company_api.app._secure_request_log_leaf",
            return_value=True,
        ), patch(
            "boxer_company_api.app._ensure_request_log_schema"
        ), patch(
            "boxer_company_api.app._save_request_log_record",
            side_effect=RuntimeError("secret-must-not-leak"),
        ), patch("boxer_company_api.app.emit_api_event") as emit_api_event:
            app = create_company_api_app(
                settings=_settings(request_log_enabled=True),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_payload(routeGroup="barcode"),
                )
                readiness = client.get("/health/ready")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(readiness.status_code, 503)
        failed_events = [
            call
            for call in emit_api_event.call_args_list
            if call.args == ("company_api_request_log_persist_failed",)
        ]
        self.assertEqual(len(failed_events), 1)
        self.assertEqual(failed_events[0].kwargs["error_type"], "RuntimeError")

    def test_request_log_schema_failure_blocks_startup_readiness(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            request_log_path = str(
                Path(temp_dir).resolve() / "request-log.db"
            )
            with patch(
                "boxer_company_api.app._initialize_request_log_storage",
                return_value=None,
            ), patch(
                "boxer_company_api.app._ensure_request_log_schema",
                side_effect=OSError("raw-path-must-not-leak"),
            ), patch(
                "boxer_company_api.app.emit_api_event"
            ) as emit_api_event:
                app = create_company_api_app(
                    settings=_settings(
                        request_log_enabled=True,
                        request_log_path=request_log_path,
                    ),
                    assistant_runtime=_FakeRuntime(),
                    readiness_probe=lambda: True,
                )
                with TestClient(app) as client:
                    readiness = client.get("/health/ready")

        self.assertEqual(readiness.status_code, 503)
        startup_events = [
            call
            for call in emit_api_event.call_args_list
            if call.args == ("company_api_request_log_startup_failed",)
        ]
        self.assertEqual(len(startup_events), 1)
        self.assertEqual(startup_events[0].kwargs["error_type"], "OSError")

    def test_operations_request_log_does_not_copy_sensitive_question(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="app_user_lookup",
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body="민감 조회 결과",
                        delivery_scope="requester",
                    ),
                ),
            )
        )
        question = "12345678910 유저 전화번호 조회"

        with patch(
            "boxer_company_api.app._initialize_request_log_readiness",
            return_value=True,
        ), patch(
            "boxer_company_api.app._secure_request_log_leaf",
            return_value=True,
        ), patch(
            "boxer_company_api.app._ensure_request_log_schema"
        ), patch(
            "boxer_company_api.app._save_request_log_record"
        ) as save_request_log:
            app = create_company_api_app(
                settings=_settings(
                    capabilities=(
                        "assistant.turn.read",
                        "assistant.operation.execute",
                    ),
                    request_log_enabled=True,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_payload(
                        question=question,
                        routeGroup="operations",
                    ),
                )

        self.assertEqual(response.status_code, 200)
        record = save_request_log.call_args.args[0]
        self.assertEqual(record["requestText"], "[민감 operations 요청]")
        self.assertEqual(
            record["normalizedQuestion"],
            "[민감 operations 요청]",
        )
        self.assertNotIn(question, str(record))

    def test_remote_operation_request_log_is_finalized_by_same_id_receipt(
        self,
    ) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_box_update",
                outcome="answered",
                messages=(AssistantMessage(body="장비 업데이트 완료"),),
            )
        )
        audit_context = _audit_context()
        initial_payload = _operations_audit_payload()
        receipt_payload = _operations_audit_payload(
            operation_action={
                "name": "request_log_delivery",
                "phase": "receipt",
                "delivered": True,
                # 다운로드처럼 256개를 넘는 실제 Slack 전달도 중앙 row에
                # 그대로 기록돼야 한다.
                "replyCount": 300,
                "firstRepliedAtUtc": "2026-08-21T01:02:03+00:00",
                "errorType": None,
            }
        )
        capabilities = (
            "assistant.turn.read",
            "assistant.operation.execute",
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            app = create_company_api_app(
                settings=_settings(
                    capabilities=capabilities,
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                initial = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=initial_payload,
                )
                pending_rows = _read_request_log_rows(db_path)

                # 다른 X-Request-ID는 같은 Slack message row를 마감하지 못한다.
                different_id = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-company-api-other"),
                    json=receipt_payload,
                )
                still_pending_rows = _read_request_log_rows(db_path)

                delivered = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=receipt_payload,
                )
                finalized_rows = _read_request_log_rows(db_path)

                # exact duplicate만 idempotent replay되고, altered receipt는
                # 이미 마감된 row를 다시 쓰지 못한다.
                replay = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=receipt_payload,
                )
                altered = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json={
                        **receipt_payload,
                        "operationAction": {
                            **receipt_payload["operationAction"],
                            "replyCount": 301,
                        },
                    },
                )

        self.assertEqual(initial.status_code, 200)
        self.assertEqual(len(pending_rows), 1)
        pending = pending_rows[0]
        self.assertEqual(pending["sourcePlatform"], "slack")
        self.assertEqual(pending["workspaceId"], "TENANT-1")
        self.assertEqual(pending["eventType"], "app_mention")
        self.assertEqual(pending["routeName"], "device box update")
        self.assertEqual(pending["routeMode"], "remote")
        self.assertEqual(pending["handlerType"], "company_api")
        self.assertEqual(pending["status"], "pending_delivery")
        self.assertEqual(pending["userId"], "ACTOR-1")
        self.assertEqual(pending["userName"], "테스트 사용자")
        self.assertEqual(pending["channelId"], "C01")
        self.assertEqual(pending["threadId"], audit_context["threadId"])
        self.assertEqual(pending["messageId"], audit_context["messageId"])
        self.assertEqual(pending["isThreadRoot"], 0)
        self.assertEqual(pending["permalink"], audit_context["permalink"])
        self.assertEqual(
            pending["threadPermalink"],
            audit_context["threadPermalink"],
        )
        self.assertEqual(pending["requestKey"], _REQUEST_ID)
        self.assertEqual(pending["requestText"], "[민감 operations 요청]")
        self.assertEqual(
            pending["normalizedQuestion"],
            "[민감 operations 요청]",
        )
        self.assertNotIn(initial_payload["question"], str(pending))
        self.assertEqual(pending["replyCount"], 0)
        self.assertIsNone(pending["firstRepliedAtUtc"])

        self.assertEqual(different_id.status_code, 409)
        self.assertEqual(different_id.json()["code"], "request_id_conflict")
        self.assertEqual(still_pending_rows[0]["status"], "pending_delivery")
        self.assertEqual(still_pending_rows[0]["requestKey"], _REQUEST_ID)
        self.assertEqual(delivered.status_code, 200)
        self.assertEqual(delivered.json()["route"], "request_log_delivery")
        self.assertEqual(len(finalized_rows), 1)
        finalized = finalized_rows[0]
        self.assertEqual(finalized["status"], "answered")
        self.assertEqual(finalized["replyCount"], 300)
        self.assertEqual(
            finalized["firstRepliedAtUtc"],
            "2026-08-21T01:02:03+00:00",
        )
        self.assertIsNone(finalized["errorType"])
        metadata = json.loads(finalized["metadataJson"])
        self.assertEqual(metadata["domainOutcome"], "answered")
        self.assertEqual(metadata["deliveryRequestId"], _REQUEST_ID)
        self.assertTrue(metadata["deliveryReceiptFingerprint"])
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(altered.status_code, 409)
        # receipt endpoint는 domain operation을 절대 다시 실행하지 않는다.
        self.assertEqual(runtime.stages, ["operations"])

    def test_request_log_receipt_rejects_missing_or_changed_identity(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="app_user_lookup",
                outcome="answered",
                messages=(AssistantMessage(body="조회 완료"),),
            )
        )
        action = {
            "name": "request_log_delivery",
            "phase": "receipt",
            "delivered": False,
            "replyCount": 0,
            "firstRepliedAtUtc": None,
            "errorType": "RuntimeError",
        }
        capabilities = (
            "assistant.turn.read",
            "assistant.operation.execute",
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            app = create_company_api_app(
                settings=_settings(
                    capabilities=capabilities,
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                missing = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(
                        message_id="1784800000.000003",
                        operation_action=action,
                    ),
                )
                initial = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(),
                )
                changed_identity_payload = _operations_audit_payload(
                    operation_action=action,
                )
                changed_identity_payload["auditContext"] = {
                    **changed_identity_payload["auditContext"],
                    "userName": "다른 사용자",
                }
                changed_identity = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=changed_identity_payload,
                )
                rows = _read_request_log_rows(db_path)

        self.assertEqual(missing.status_code, 409)
        self.assertEqual(initial.status_code, 200)
        self.assertEqual(changed_identity.status_code, 409)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "pending_delivery")
        self.assertEqual(rows[0]["userName"], "테스트 사용자")

    def test_attempted_mutation_exception_remains_in_central_request_log(
        self,
    ) -> None:
        class _AttemptedMutationRuntime(_FakeRuntime):
            def answer_stage(
                self,
                request: Any,
                stage: str,
            ) -> CompanyAssistantResult:
                from boxer_company.routers.device_ssh_security import (
                    _mark_company_api_mutation_attempted,
                )

                self.requests.append(request)
                self.stages.append(stage)
                _mark_company_api_mutation_attempted()
                raise RuntimeError("secret-must-not-reach-audit")

        runtime = _AttemptedMutationRuntime()
        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            app = create_company_api_app(
                settings=_settings(
                    capabilities=(
                        "assistant.turn.read",
                        "assistant.operation.execute",
                    ),
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app, raise_server_exceptions=False) as client:
                failed = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(),
                )
                replay = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(),
                )
                receipt_payload = _operations_audit_payload(
                    operation_action={
                        "name": "request_log_delivery",
                        "phase": "receipt",
                        "delivered": True,
                        "replyCount": 1,
                        "firstRepliedAtUtc": "2026-08-21T01:02:03Z",
                        "errorType": None,
                    }
                )
                receipt = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=receipt_payload,
                )
                altered_receipt = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json={
                        **receipt_payload,
                        "operationAction": {
                            **receipt_payload["operationAction"],
                            "replyCount": 2,
                        },
                    },
                )
                rows = _read_request_log_rows(db_path)

        self.assertEqual(failed.status_code, 500)
        self.assertEqual(replay.status_code, 409)
        self.assertEqual(replay.json()["code"], "operation_in_progress")
        self.assertEqual(receipt.status_code, 200)
        self.assertEqual(altered_receipt.status_code, 409)
        self.assertEqual(len(runtime.requests), 1)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["status"], "error")
        self.assertEqual(row["errorType"], "RuntimeError")
        self.assertEqual(row["requestText"], "[민감 operations 요청]")
        self.assertNotIn("secret-must-not-reach-audit", str(row))
        metadata = json.loads(row["metadataJson"])
        self.assertEqual(metadata["domainOutcome"], "uncertain")
        self.assertTrue(metadata["deliveryReceiptFingerprint"])
        self.assertEqual(row["replyCount"], 1)

    def test_delivery_receipt_preserves_failed_or_denied_domain_status(
        self,
    ) -> None:
        for outcome in ("failed", "denied"):
            with (
                self.subTest(outcome=outcome),
                tempfile.TemporaryDirectory() as temp_dir,
                patch(
                    "boxer_company_api.app._initialize_request_log_storage",
                    return_value=None,
                ),
            ):
                runtime = _FakeRuntime(
                    CompanyAssistantResult(
                        route="device_box_update",
                        outcome=outcome,
                        messages=(AssistantMessage(body="처리 결과"),),
                    )
                )
                db_path = str(Path(temp_dir).resolve() / "request-log.db")
                app = create_company_api_app(
                    settings=_settings(
                        capabilities=(
                            "assistant.turn.read",
                            "assistant.operation.execute",
                        ),
                        request_log_enabled=True,
                        request_log_path=db_path,
                    ),
                    assistant_runtime=runtime,
                    readiness_probe=lambda: True,
                )
                receipt_payload = _operations_audit_payload(
                    operation_action={
                        "name": "request_log_delivery",
                        "phase": "receipt",
                        "delivered": True,
                        "replyCount": 1,
                        "firstRepliedAtUtc": "2026-08-21T01:02:03Z",
                        "errorType": None,
                    }
                )
                with TestClient(app) as client:
                    initial = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(),
                        json=_operations_audit_payload(),
                    )
                    receipt = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(),
                        json=receipt_payload,
                    )
                    rows = _read_request_log_rows(db_path)

                self.assertEqual(initial.status_code, 200)
                self.assertEqual(receipt.status_code, 200)
                self.assertEqual(rows[0]["status"], outcome)
                metadata = json.loads(rows[0]["metadataJson"])
                self.assertEqual(metadata["domainOutcome"], outcome)

    def test_mutation_replay_waits_until_pending_delivery_is_persisted(
        self,
    ) -> None:
        from boxer_company_api import app as app_module

        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_box_update",
                outcome="answered",
                messages=(AssistantMessage(body="업데이트 완료"),),
            )
        )
        final_row_persisted = threading.Event()
        release_final_persist = threading.Event()
        original_persist = app_module._persist_turn_request_log

        def persist_with_barrier(**kwargs: Any) -> bool:
            persisted = original_persist(**kwargs)
            if (
                kwargs.get("status_override") is None
                and kwargs.get("outcome") == "answered"
            ):
                final_row_persisted.set()
                if not release_final_persist.wait(2):
                    raise RuntimeError("test release timeout")
            return persisted

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            app = create_company_api_app(
                settings=_settings(
                    capabilities=(
                        "assistant.turn.read",
                        "assistant.operation.execute",
                    ),
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            first_result: dict[str, Any] = {}
            with patch(
                "boxer_company_api.app._persist_turn_request_log",
                side_effect=persist_with_barrier,
            ), TestClient(app) as first_client, TestClient(app) as replay_client:
                worker = threading.Thread(
                    target=lambda: first_result.setdefault(
                        "response",
                        first_client.post(
                            "/internal/v1/assistant/turns",
                            headers=_headers(),
                            json=_operations_audit_payload(),
                        ),
                    )
                )
                worker.start()
                try:
                    self.assertTrue(final_row_persisted.wait(1))
                    busy = replay_client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(),
                        json=_operations_audit_payload(),
                    )
                finally:
                    release_final_persist.set()
                    worker.join(2)
                replay = replay_client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(),
                )
                receipt = replay_client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(
                        operation_action={
                            "name": "request_log_delivery",
                            "phase": "receipt",
                            "delivered": True,
                            "replyCount": 1,
                            "firstRepliedAtUtc": (
                                "2026-08-21T01:02:03Z"
                            ),
                            "errorType": None,
                        }
                    ),
                )
                rows = _read_request_log_rows(db_path)

        self.assertFalse(worker.is_alive())
        self.assertEqual(first_result["response"].status_code, 200)
        self.assertEqual(busy.status_code, 409)
        self.assertEqual(busy.json()["code"], "operation_in_progress")
        self.assertEqual(replay.status_code, 200)
        self.assertEqual(receipt.status_code, 200)
        self.assertEqual(rows[0]["status"], "answered")
        self.assertEqual(len(runtime.requests), 1)

    def test_renderer_failure_receipt_preserves_partial_reply_count(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_box_update",
                outcome="answered",
                messages=(AssistantMessage(body="최종 응답"),),
            )
        )
        capabilities = (
            "assistant.turn.read",
            "assistant.operation.execute",
        )
        failure_receipt = _operations_audit_payload(
            operation_action={
                "name": "request_log_delivery",
                "phase": "receipt",
                "delivered": False,
                "replyCount": 1,
                "firstRepliedAtUtc": "2026-08-21T01:02:03Z",
                "errorType": "SlackApiError",
            }
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            app = create_company_api_app(
                settings=_settings(
                    capabilities=capabilities,
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                initial = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(),
                )
                failed = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=failure_receipt,
                )
                rows = _read_request_log_rows(db_path)

        self.assertEqual(initial.status_code, 200)
        self.assertEqual(failed.status_code, 200)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "error")
        self.assertEqual(rows[0]["replyCount"], 1)
        self.assertEqual(
            rows[0]["firstRepliedAtUtc"],
            "2026-08-21T01:02:03+00:00",
        )
        self.assertEqual(rows[0]["errorType"], "SlackApiError")

    def test_diagnostic_probe_miss_creates_no_central_request_log_row(
        self,
    ) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_diagnostic_followup",
                outcome="no_evidence",
                messages=(AssistantMessage(body="진단 상태 없음"),),
                fallback_reason="diagnostic_snapshot_missing",
            )
        )
        capabilities = (
            "assistant.turn.read",
            "assistant.operation.execute",
        )
        probe_action = {"name": "device_diagnostic_followup_probe"}

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            app = create_company_api_app(
                settings=_settings(
                    capabilities=capabilities,
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                missed = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="diag-probe:missing"),
                    json=_operations_audit_payload(
                        operation_action=probe_action,
                    ),
                )
                missed_rows = _read_request_log_rows(db_path)

                runtime.result = CompanyAssistantResult(
                    route="device_diagnostic_followup",
                    outcome="answered",
                    messages=(AssistantMessage(body="진단 결과"),),
                )
                answered = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="diag-probe:answered"),
                    json=_operations_audit_payload(
                        message_id="1784800000.000003",
                        operation_action=probe_action,
                    ),
                )
                answered_rows = _read_request_log_rows(db_path)

        self.assertEqual(missed.status_code, 200)
        self.assertEqual(missed_rows, [])
        self.assertEqual(answered.status_code, 200)
        self.assertEqual(len(answered_rows), 1)
        self.assertEqual(answered_rows[0]["status"], "pending_delivery")
        self.assertEqual(
            answered_rows[0]["routeName"],
            "device diagnostic followup",
        )

    def test_request_log_receipt_storage_failure_is_not_reported_as_success(
        self,
    ) -> None:
        receipt_payload = _operations_audit_payload(
            operation_action={
                "name": "request_log_delivery",
                "phase": "receipt",
                "delivered": True,
                "replyCount": 1,
                "firstRepliedAtUtc": "2026-08-21T01:02:03Z",
                "errorType": None,
            }
        )
        capabilities = (
            "assistant.turn.read",
            "assistant.operation.execute",
        )
        with patch(
            "boxer_company_api.app._initialize_request_log_readiness",
            return_value=True,
        ), patch(
            "boxer_company_api.app._persist_turn_request_log_delivery",
            return_value="failed",
        ):
            app = create_company_api_app(
                settings=_settings(
                    capabilities=capabilities,
                    request_log_enabled=True,
                ),
                assistant_runtime=_FakeRuntime(),
                readiness_probe=lambda: True,
            )
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=receipt_payload,
                )
                readiness = client.get("/health/ready")

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()["code"], "internal_error")
        self.assertFalse(response.json()["retryable"])
        self.assertEqual(readiness.status_code, 503)

    def test_request_log_receipt_requires_enabled_storage(self) -> None:
        receipt_payload = _operations_audit_payload(
            operation_action={
                "name": "request_log_delivery",
                "phase": "receipt",
                "delivered": True,
                "replyCount": 1,
                "firstRepliedAtUtc": "2026-08-21T01:02:03Z",
                "errorType": None,
            }
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                ),
                request_log_enabled=False,
            ),
            assistant_runtime=_FakeRuntime(),
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=receipt_payload,
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["code"], "service_not_ready")
        self.assertFalse(response.json()["retryable"])

    def test_request_log_receipt_finalizes_when_domain_features_are_off(
        self,
    ) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="device_box_update",
                outcome="answered",
                messages=(AssistantMessage(body="최종 응답"),),
            )
        )
        receipt_payload = _operations_audit_payload(
            operation_action={
                "name": "request_log_delivery",
                "phase": "receipt",
                "delivered": True,
                "replyCount": 1,
                "firstRepliedAtUtc": "2026-08-21T01:02:03Z",
                "errorType": None,
            }
        )
        capabilities = (
            "assistant.turn.read",
            "assistant.operation.execute",
        )

        with tempfile.TemporaryDirectory() as temp_dir, patch(
            "boxer_company_api.app._initialize_request_log_storage",
            return_value=None,
        ):
            db_path = str(Path(temp_dir).resolve() / "request-log.db")
            enabled_app = create_company_api_app(
                settings=_settings(
                    capabilities=capabilities,
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(enabled_app) as client:
                initial = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=_operations_audit_payload(),
                )

            feature_off_app = create_company_api_app(
                settings=_settings(
                    capabilities=capabilities,
                    operations_enabled=False,
                    live_device_enabled=False,
                    request_log_enabled=True,
                    request_log_path=db_path,
                ),
                assistant_runtime=runtime,
                readiness_probe=lambda: True,
            )
            with TestClient(feature_off_app) as client:
                receipt = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(),
                    json=receipt_payload,
                )
                rows = _read_request_log_rows(db_path)

        self.assertEqual(initial.status_code, 200)
        self.assertEqual(receipt.status_code, 200)
        self.assertEqual(receipt.json()["route"], "request_log_delivery")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "answered")
        # feature-off receipt도 기존 domain operation을 재실행하지 않는다.
        self.assertEqual(runtime.stages, ["operations"])

    def test_request_log_startup_prepares_private_leaf_after_restore(self) -> None:
        events: list[str] = []
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir).resolve() / "request-log.db"

            def initialize(*, db_path: Path) -> None:
                self.assertEqual(
                    db_path,
                    Path(temp_dir).resolve() / "request-log.db",
                )
                events.append("restore")

            def ensure(db_path: Path) -> Path:
                events.append("schema")
                db_path.touch(mode=0o644)
                return db_path

            with patch(
                "boxer_company_api.app._initialize_request_log_storage",
                side_effect=initialize,
            ), patch(
                "boxer_company_api.app._ensure_request_log_schema",
                side_effect=ensure,
            ):
                ready = _initialize_request_log_readiness(
                    enabled=True,
                    db_path=str(db_path),
                )

            self.assertTrue(ready)
            self.assertEqual(events, ["restore", "schema"])
            self.assertEqual(stat.S_IMODE(db_path.stat().st_mode), 0o600)

    def test_request_log_startup_fails_when_required_restore_did_not_finish(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = Path(temp_dir).resolve() / "request-log.db"
            with patch(
                "boxer_company_api.app._initialize_request_log_storage",
                side_effect=RuntimeError("configured restore failed"),
            ), patch(
                "boxer_company_api.app._ensure_request_log_schema"
            ) as ensure:
                ready = _initialize_request_log_readiness(
                    enabled=True,
                    db_path=str(db_path),
                )

            self.assertFalse(ready)
            self.assertFalse(db_path.exists())
            ensure.assert_not_called()

    def test_request_log_startup_rejects_unsafe_local_paths_without_repair(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir).resolve()
            unsafe_mode = root / "unsafe-mode.db"
            unsafe_mode.touch(mode=0o600)
            unsafe_mode.chmod(0o644)
            with patch(
                "boxer_company_api.app._initialize_request_log_storage"
            ) as initialize:
                mode_ready = _initialize_request_log_readiness(
                    enabled=True,
                    db_path=str(unsafe_mode),
                )
            self.assertFalse(mode_ready)
            initialize.assert_not_called()
            self.assertEqual(
                stat.S_IMODE(unsafe_mode.stat().st_mode),
                0o644,
            )

            target = root / "target.db"
            target.touch(mode=0o600)
            symlink = root / "request-log-link.db"
            symlink.symlink_to(target)
            with patch(
                "boxer_company_api.app._initialize_request_log_storage"
            ) as initialize:
                symlink_ready = _initialize_request_log_readiness(
                    enabled=True,
                    db_path=str(symlink),
                )
            self.assertFalse(symlink_ready)
            initialize.assert_not_called()
            self.assertTrue(symlink.is_symlink())

            owned = root / "owner.db"
            owned.touch(mode=0o600)
            with patch(
                "boxer_company_api.app.os.geteuid",
                return_value=os.geteuid() + 1,
            ), patch(
                "boxer_company_api.app._initialize_request_log_storage"
            ) as initialize:
                self.assertFalse(_secure_request_log_leaf(owned))
                owner_ready = _initialize_request_log_readiness(
                    enabled=True,
                    db_path=str(owned),
                )
            self.assertFalse(owner_ready)
            initialize.assert_not_called()

    def test_request_log_startup_rejects_public_parent_before_creation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir).resolve()
            parent.chmod(0o755)
            db_path = parent / "request-log.db"
            with patch(
                "boxer_company_api.app._initialize_request_log_storage"
            ) as initialize:
                ready = _initialize_request_log_readiness(
                    enabled=True,
                    db_path=str(db_path),
                )

            self.assertFalse(ready)
            self.assertFalse(db_path.exists())
            initialize.assert_not_called()

    def test_long_message_is_windowed_at_client_contract_boundary(
        self,
    ) -> None:
        # 로그 분석 본문이 30,000자를 넘겨도 API가 계약 오류를 만들지 않고
        # 의미를 보존한 여러 메시지로 직렬화해야 한다.
        for body_size, expected_messages in (
            (29_999, 1),
            (30_000, 1),
            (30_001, 2),
        ):
            with self.subTest(body_size=body_size):
                body = "가" * body_size
                runtime = _FakeRuntime(
                    CompanyAssistantResult(
                        route="barcode_log_analysis",
                        outcome="answered",
                        messages=(AssistantMessage(body=body),),
                    )
                )
                app = create_company_api_app(
                    settings=_settings(),
                    assistant_runtime=runtime,
                    readiness_probe=lambda: True,
                )

                with TestClient(app) as client:
                    response = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(),
                        json=_payload(),
                    )

                self.assertEqual(response.status_code, 200)
                messages = response.json()["messages"]
                self.assertEqual(len(messages), expected_messages)
                self.assertTrue(
                    all(len(item["body"]) <= 30_000 for item in messages)
                )
                self.assertEqual(
                    "".join(item["body"] for item in messages),
                    body,
                )
                self.assertTrue(messages[0]["mentionActor"])
                if expected_messages > 1:
                    self.assertFalse(messages[1]["mentionActor"])

    def test_response_windowing_respects_utf8_byte_budget_and_source_contract(
        self,
    ) -> None:
        # 문자 수가 같아도 4-byte emoji와 최대 source가 합쳐지면 client의
        # 1MiB 상한을 넘을 수 있어 최종 JSON byte 크기까지 고정한다.
        uri_prefix = "https://example.com/"
        source_uri = uri_prefix + "a" * (2_048 - len(uri_prefix))
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="barcode_log_analysis",
                outcome="answered",
                messages=tuple(
                    AssistantMessage(body="😀" * 30_000)
                    for _ in range(8)
                ),
                sources=tuple(
                    SourceReference(
                        source_id="😀" * 512,
                        title="😀" * 2_000,
                        uri=source_uri,
                    )
                    for _ in range(21)
                ),
            )
        )
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(),
            )

        self.assertEqual(response.status_code, 200)
        self.assertLessEqual(len(response.content), 1_048_576)
        payload = response.json()
        self.assertLessEqual(len(payload["messages"]), 8)
        self.assertEqual(len(payload["sources"]), 20)
        self.assertTrue(payload["messages"][-1]["body"].endswith("...(truncated)"))
        # 실제 Slack API client 역직렬화 계약도 같은 HTTP 응답을 수용한다.
        deserialized = _deserialize_result(response, _REQUEST_ID)
        self.assertEqual(deserialized.route, "barcode_log_analysis")

    def test_exposes_only_internal_assistant_and_health_endpoints(
        self,
    ) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        route_paths = {
            route.path
            for route in app.routes
            if getattr(route, "path", None)
        }

        self.assertEqual(
            route_paths,
            {
                "/health/live",
                "/health/ready",
                "/internal/v1/assistant/turns",
                "/internal/v1/automation/cycles",
            },
        )
        with TestClient(app) as client:
            not_found = client.get("/docs")
            self.assertEqual(not_found.status_code, 404)
            self.assertEqual(
                not_found.headers["content-type"],
                "application/problem+json",
            )
            self.assertEqual(not_found.json()["code"], "not_found")
            self.assertNotIn("detail", not_found.json())
            self.assertEqual(client.get("/openapi.json").status_code, 404)
            self.assertEqual(client.get("/widget").status_code, 404)

    def test_liveness_does_not_require_auth_or_run_readiness_probe(self) -> None:
        probe_calls: list[str] = []
        app = create_company_api_app(
            settings=_settings(configuration_error="caller_registry_missing"),
            assistant_runtime=None,
            readiness_probe=lambda: probe_calls.append("ready") or False,
        )

        with TestClient(app) as client:
            response = client.get("/health/live")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": "ok",
                "service": "boxer-company-api",
            },
        )
        self.assertEqual(probe_calls, [])

    def test_readiness_reports_only_sanitized_component_state(self) -> None:
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=_FakeRuntime(),
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.get("/health/ready")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json(),
            {
                "status": "ready",
                "service": "boxer-company-api",
                "checks": {
                    "authentication": "ok",
                    "runtime": "ok",
                    "configuration": "ok",
                },
            },
        )

    @patch(
        "boxer_company_api.app.company_api_local_readiness",
        return_value=False,
    )
    def test_readiness_fails_closed_when_local_automation_state_is_unsafe(
        self,
        _local_readiness: Any,
    ) -> None:
        # dependency path나 credential 원문은 응답하지 않고 공통 503만 낸다.
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=_FakeRuntime(),
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.get(
                "/health/ready",
                headers={"X-Request-ID": _REQUEST_ID},
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["code"], "service_not_ready")
        self.assertNotIn("automation", response.text)

    def test_not_ready_uses_problem_json_without_configuration_detail(self) -> None:
        app = create_company_api_app(
            settings=_settings(configuration_error="secret-value-must-not-leak"),
            assistant_runtime=None,
            readiness_probe=lambda: False,
        )

        with TestClient(app) as client:
            response = client.get(
                "/health/ready",
                headers={"X-Request-ID": _REQUEST_ID},
            )

        self.assertEqual(response.status_code, 503)
        self.assertEqual(
            set(response.json()),
            {
                "type",
                "title",
                "status",
                "code",
                "requestId",
                "retryable",
            },
        )
        self.assertEqual(response.json()["code"], "service_not_ready")
        self.assertNotIn("secret-value-must-not-leak", response.text)
        self.assertEqual(response.headers["content-type"], "application/problem+json")

    def test_turn_requires_request_id_before_runtime_execution(self) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers={"Authorization": f"Bearer {_TOKEN}"},
                json=_payload(),
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], "invalid_request_id")
        self.assertEqual(runtime.requests, [])

    def test_turn_rejects_invalid_traceparent_before_runtime_execution(self) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        headers = _headers()
        headers["traceparent"] = "not-a-traceparent"

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=headers,
                json=_payload(),
            )

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["code"], "invalid_traceparent")
        self.assertEqual(runtime.requests, [])

    def test_missing_and_invalid_tokens_share_the_same_safe_problem(self) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            missing = client.post(
                "/internal/v1/assistant/turns",
                headers={"X-Request-ID": _REQUEST_ID},
                json=_payload(),
            )
            invalid = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(token="x" * 48),
                json=_payload(),
            )

        self.assertEqual(missing.status_code, 401)
        self.assertEqual(invalid.status_code, 401)
        self.assertEqual(missing.json()["code"], "authentication_failed")
        self.assertEqual(invalid.json()["code"], "authentication_failed")
        self.assertEqual(
            missing.headers["www-authenticate"],
            "Bearer",
        )
        self.assertEqual(runtime.requests, [])

    def test_caller_scope_is_checked_before_runtime_execution(self) -> None:
        cases = (
            ("tenant", _payload(tenantId="TENANT-2")),
            ("channel", _payload(channel="web")),
            ("actor", _payload(actorId="ACTOR-2")),
            (
                "context source",
                _payload(
                    contextEntries=[
                        {
                            "kind": "message",
                            "source": "widget",
                            "authorId": "ACTOR-1",
                            "text": "위조된 다른 채널 문맥",
                        }
                    ]
                ),
            ),
            (
                "anonymous actor",
                _payload(actorId=None),
            ),
        )
        for label, payload in cases:
            with self.subTest(label=label):
                runtime = _FakeRuntime()
                app = create_company_api_app(
                    settings=_settings(),
                    assistant_runtime=runtime,
                    readiness_probe=lambda: True,
                )
                with TestClient(app) as client:
                    response = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(),
                        json=payload,
                    )

                self.assertEqual(response.status_code, 403)
                self.assertEqual(response.json()["code"], "caller_not_allowed")
                self.assertEqual(runtime.requests, [])

    def test_body_cannot_supply_role_or_capabilities(self) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        payload = _payload()
        payload["role"] = "admin"
        payload["capabilities"] = ["*"]

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=payload,
            )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["code"], "validation_failed")
        self.assertNotIn("admin", response.text)
        self.assertNotIn("capabilities", response.text)
        self.assertEqual(runtime.requests, [])

    def test_context_shape_and_size_are_bounded(self) -> None:
        runtime = _FakeRuntime()
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        too_many_entries = [
            {
                "kind": "message",
                "source": "slack",
                "authorId": "ACTOR-1",
                "text": f"문맥 {index}",
            }
            for index in range(13)
        ]

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(contextEntries=too_many_entries),
            )

        self.assertEqual(response.status_code, 422)
        self.assertEqual(response.json()["code"], "validation_failed")
        self.assertEqual(runtime.requests, [])

    def test_question_uses_the_slack_forty_thousand_char_limit(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="company_freeform",
                outcome="answered",
                messages=(AssistantMessage(body="답변"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            accepted = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-question-40000"),
                json=_payload(question="q" * 40_000),
            )
            rejected = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-question-40001"),
                json=_payload(question="q" * 40_001),
            )

        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(rejected.status_code, 422)
        self.assertEqual(rejected.json()["code"], "validation_failed")
        self.assertEqual(len(runtime.requests), 1)
        self.assertEqual(runtime.requests[0].question, "q" * 40_000)

    def test_operations_accepts_the_legacy_learning_context_budget(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="thread_playbook_learning",
                outcome="answered",
                messages=(AssistantMessage(body="학습 완료"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        context_entries = [
            {
                "kind": "message",
                "source": "slack",
                "authorId": "ACTOR-1",
                "text": f"{index:02d}-" + ("x" * 597),
            }
            for index in range(20)
        ]

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(
                    routeGroup="operations",
                    question="이 스레드 학습해줘",
                    contextEntries=context_entries,
                ),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(runtime.requests[0].context_entries), 20)
        self.assertEqual(
            sum(
                len(str(entry["text"]))
                for entry in runtime.requests[0].context_entries
            ),
            12_000,
        )

    def test_all_routes_keep_one_five_thousand_char_entry_byte_for_byte(
        self,
    ) -> None:
        prefix = "BEGIN\n"
        suffix = "\nEND"
        thread_text = (
            prefix
            + ("x" * (5_000 - len(prefix) - len(suffix)))
            + suffix
        )
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="thread_playbook_learning",
                outcome="answered",
                messages=(AssistantMessage(body="학습 완료"),),
            )
        )
        app = create_company_api_app(
            settings=_settings(
                capabilities=(
                    "assistant.turn.read",
                    "assistant.operation.execute",
                )
            ),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        entry = {
            "kind": "message",
            "source": "slack",
            "authorId": "ACTOR-1",
            "text": thread_text,
        }

        with TestClient(app) as client:
            accepted = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-learning-entry-5000"),
                json=_payload(
                    routeGroup="operations",
                    question="이 스레드 학습해줘",
                    contextEntries=[entry],
                ),
            )
            non_operation = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-read-entry-5000"),
                json=_payload(
                    routeGroup="barcode",
                    contextEntries=[entry],
                ),
            )
            oversized = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(request_id="req-read-entry-5001"),
                json=_payload(
                    routeGroup="barcode",
                    contextEntries=[
                        {
                            **entry,
                            "text": thread_text + "x",
                        }
                    ],
                ),
            )

        self.assertEqual(accepted.status_code, 200)
        self.assertEqual(non_operation.status_code, 200)
        self.assertEqual(oversized.status_code, 422)
        self.assertEqual(
            runtime.requests[0].context_entries[0]["text"].encode(),
            thread_text.encode(),
        )
        self.assertEqual(
            runtime.requests[1].context_entries[0]["text"].encode(),
            thread_text.encode(),
        )

    def test_success_converts_transport_to_neutral_request_and_exact_dto(self) -> None:
        result = CompanyAssistantResult(
            route="barcode_query",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body="조회 결과야",
                    delivery_scope="conversation",
                    mention_actor=False,
                ),
            ),
            sources=(
                SourceReference(
                    source_id="source-1",
                    title="운영 문서",
                    uri="https://www.notion.so/source-1",
                    score=0.9,
                ),
            ),
            used_llm=True,
        )
        runtime = _FakeRuntime(result)
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        headers = _headers()
        headers["traceparent"] = traceparent
        payload = _payload()
        payload["scope"]["followupKind"] = "barcode_log"
        payload["scope"]["actorName"] = "테스트 사용자"
        payload["scope"]["threadPermalink"] = (
            "https://workspace.slack.com/archives/C01/"
            "p1785312000000001?thread_ts=1785312000.000001&cid=C01"
        )
        payload["scope"]["trustedMdaRecoveryScope"] = {
            "barcode": "12345678910",
            "logDate": "2026-03-06",
            "deviceName": "MB2-C00419",
            "hospitalName": "테스트 병원",
            "roomName": "검사실",
        }

        with self.assertLogs(
            "boxer.company_api",
            level="INFO",
        ) as captured_logs:
            with TestClient(app) as client:
                response = client.post(
                    "/internal/v1/assistant/turns",
                    headers=headers,
                    json=payload,
                )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["x-request-id"], _REQUEST_ID)
        self.assertEqual(response.headers["traceparent"], traceparent)
        self.assertEqual(response.headers["cache-control"], "no-store")
        body = response.json()
        self.assertEqual(
            set(body),
            {
                "requestId",
                "route",
                "outcome",
                "messages",
                "sources",
                "usedLlm",
                "fallbackReason",
                "suggestedAction",
                "asyncJob",
            },
        )
        self.assertEqual(body["requestId"], _REQUEST_ID)
        self.assertEqual(body["route"], "barcode_query")
        self.assertEqual(body["messages"][0]["body"], "조회 결과야")
        self.assertEqual(
            set(body["messages"][0]),
            {"body", "deliveryScope", "mentionActor", "format"},
        )
        self.assertEqual(
            set(body["sources"][0]),
            {"sourceId", "title", "uri", "score"},
        )
        self.assertTrue(body["usedLlm"])

        self.assertEqual(len(runtime.requests), 1)
        request = runtime.requests[0]
        self.assertEqual(request.request_id, _REQUEST_ID)
        self.assertEqual(request.tenant_id, "TENANT-1")
        self.assertEqual(request.actor_id, "ACTOR-1")
        self.assertEqual(request.channel, "slack")
        self.assertEqual(request.metadata["barcode"], "12345678910")
        self.assertEqual(request.metadata["channel_id"], "C01")
        self.assertEqual(
            request.metadata["followup_kind"],
            "barcode_log",
        )
        self.assertEqual(
            request.metadata["actor_name"],
            "테스트 사용자",
        )
        self.assertEqual(
            request.metadata["thread_permalink"],
            payload["scope"]["threadPermalink"],
        )
        self.assertEqual(
            request.metadata["trusted_mda_recovery_scope"],
            payload["scope"]["trustedMdaRecoveryScope"],
        )
        self.assertNotIn("role", request.metadata)
        self.assertNotIn("capabilities", request.metadata)
        self.assertEqual(request.context_entries[0]["source"], "slack")
        safe_logs = "\n".join(captured_logs.output)
        self.assertNotIn(_payload()["question"], safe_logs)
        self.assertNotIn("조회 결과야", safe_logs)
        self.assertNotIn(_TOKEN, safe_logs)

    def test_runtime_none_is_normalized_to_no_evidence(self) -> None:
        runtime = _FakeRuntime(None)
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(question="처리되지 않는 질문", scope=None),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["route"], "unhandled")
        self.assertEqual(response.json()["outcome"], "no_evidence")
        self.assertEqual(len(response.json()["messages"]), 1)
        self.assertFalse(response.json()["usedLlm"])
        self.assertEqual(response.json()["fallbackReason"], "no_matching_route")

    def test_domain_denied_and_failed_results_remain_http_200(self) -> None:
        for outcome in ("denied", "failed"):
            with self.subTest(outcome=outcome):
                runtime = _FakeRuntime(
                    CompanyAssistantResult(
                        route="policy",
                        outcome=outcome,  # type: ignore[arg-type]
                        messages=(AssistantMessage(body="안전한 안내"),),
                    )
                )
                app = create_company_api_app(
                    settings=_settings(),
                    assistant_runtime=runtime,
                    readiness_probe=lambda: True,
                )
                with TestClient(app) as client:
                    response = client.post(
                        "/internal/v1/assistant/turns",
                        headers=_headers(),
                        json=_payload(),
                    )

                self.assertEqual(response.status_code, 200)
                self.assertEqual(response.json()["outcome"], outcome)

    def test_internal_actions_jobs_and_unsafe_sources_are_not_exposed(self) -> None:
        runtime = _FakeRuntime(
            CompanyAssistantResult(
                route="safe_read",
                outcome="answered",
                messages=(AssistantMessage(body="안전한 결과"),),
                sources=(
                    SourceReference(
                        source_id="bad",
                        title="bad",
                        uri="file:///tmp/secret",
                    ),
                    SourceReference(
                        source_id="signed",
                        title="signed",
                        uri=(
                            "https://storage.example/file?"
                            "X-Amz-Signature=must-not-leak"
                        ),
                    ),
                    SourceReference(
                        source_id="azure-signed",
                        title="azure-signed",
                        uri=(
                            "https://storage.example/file?"
                            "sig=must-not-leak"
                        ),
                    ),
                    SourceReference(
                        source_id="fragment-token",
                        title="fragment-token",
                        uri=(
                            "https://identity.example/callback"
                            "#access_token=must-not-leak"
                        ),
                    ),
                    SourceReference(
                        source_id="safe-anchor",
                        title="safe-anchor",
                        uri="https://www.notion.so/source#safe-heading",
                    ),
                ),
                suggested_action=SuggestedAction(
                    action="dangerous_internal_action",
                    label="실행",
                    parameters={"secret": "must-not-leak"},
                ),
                async_job={"secret": "must-not-leak"},
            )
        )
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(),
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(
            response.json()["sources"],
            [
                {
                    "sourceId": "safe-anchor",
                    "title": "safe-anchor",
                    "uri": "https://www.notion.so/source#safe-heading",
                    "score": None,
                }
            ],
        )
        self.assertIsNone(response.json()["suggestedAction"])
        self.assertIsNone(response.json()["asyncJob"])
        self.assertNotIn("must-not-leak", response.text)

    def test_runtime_exception_is_sanitized(self) -> None:
        runtime = _FakeRuntime(
            error=RuntimeError("secret-database-host must not leak"),
        )
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=runtime,
            readiness_probe=lambda: True,
        )

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.post(
                "/internal/v1/assistant/turns",
                headers=_headers(),
                json=_payload(),
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(response.json()["code"], "internal_error")
        self.assertNotIn("secret-database-host", response.text)
        self.assertEqual(
            set(response.json()),
            {
                "type",
                "title",
                "status",
                "code",
                "requestId",
                "retryable",
            },
        )

    def test_no_cors_policy_is_installed(self) -> None:
        app = create_company_api_app(
            settings=_settings(),
            assistant_runtime=_FakeRuntime(),
            readiness_probe=lambda: True,
        )

        with TestClient(app) as client:
            response = client.options(
                "/internal/v1/assistant/turns",
                headers={
                    "Origin": "https://service.example",
                    "Access-Control-Request-Method": "POST",
                },
            )

        self.assertEqual(response.status_code, 405)
        self.assertEqual(
            response.headers["content-type"],
            "application/problem+json",
        )
        self.assertEqual(
            response.json()["code"],
            "method_not_allowed",
        )
        self.assertNotIn("access-control-allow-origin", response.headers)


if __name__ == "__main__":
    unittest.main()
