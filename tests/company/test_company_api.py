from __future__ import annotations

import unittest
from typing import Any

from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantResult,
    SourceReference,
    SuggestedAction,
)
from boxer_company_adapter_slack.company_api_client import (
    _deserialize_result,
)
from boxer_company_api.app import create_company_api_app
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
    ) -> CompanyAssistantResult | None:
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


class CompanyApiContractTests(unittest.TestCase):
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

    def test_exposes_only_internal_turn_and_health_endpoints(self) -> None:
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
