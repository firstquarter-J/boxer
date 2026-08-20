from __future__ import annotations

import unittest
from typing import Any
from unittest.mock import patch

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
    live_device_enabled: bool = True,
    operations_enabled: bool = True,
    request_log_enabled: bool = False,
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
        # 뒤이은 실제 mutation 실패만 tenant-wide uncertain 상태로 남긴다.
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
                    blocked_mutation = client.post(
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
                self.assertEqual(blocked_mutation.status_code, 409)
                self.assertEqual(
                    blocked_mutation.json()["code"],
                    "operation_in_progress",
                )
                self.assertEqual(len(runtime.requests), 2)

    def test_failed_mutation_result_with_unknown_status_stays_sticky(self) -> None:
        # route 내부 catch가 HTTP 예외 대신 failed 결과를 반환해도 실제
        # mutation 처리 여부가 불명이면 다른 request ID로 재실행하지 않는다.
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
                blocked = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-uncertain-result-2"),
                    json=payload,
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(first.json()["outcome"], "failed")
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(blocked.json()["code"], "operation_in_progress")
        self.assertEqual(len(runtime.requests), 1)

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

    def test_device_detail_failure_after_ssh_open_attempt_stays_sticky(
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
                blocked = client.post(
                    "/internal/v1/assistant/turns",
                    headers=_headers(request_id="req-detail-open-2"),
                    json=payload,
                )

        self.assertEqual(first.status_code, 200)
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(blocked.json()["code"], "operation_in_progress")
        self.assertEqual(len(runtime.requests), 1)

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
        with patch(
            "boxer_company_api.app._ensure_request_log_schema",
            side_effect=OSError("raw-path-must-not-leak"),
        ), patch("boxer_company_api.app.emit_api_event") as emit_api_event:
            app = create_company_api_app(
                settings=_settings(request_log_enabled=True),
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
        self.assertNotIn(question, str(record))

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
