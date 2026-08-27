from __future__ import annotations

import hashlib
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import CompanyAssistantResult
from boxer_company.hpa_change_coordinator import (
    HpaChangeSubmissionResult,
    HpaChangeSubmissionState,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.auth import CallerRegistry
from boxer_company_api.hpa_change_router import (
    HPA_CHANGE_EXECUTE_CAPABILITY,
    create_hpa_change_router,
)
from boxer_company_api.problems import install_problem_handlers
from boxer_company_api.settings import CompanyApiCallerSettings
from boxer_company_api.settings import CompanyApiSettings


_TOKEN = "t" * 48


class _Service:
    def __init__(self, *, enabled: bool = True) -> None:
        self.coordinator = type("Coordinator", (), {"enabled": enabled})()
        self.calls: list[str] = []

    def submit(self, request_id: str, _body: Any) -> dict[str, Any]:
        self.calls.append("submit")
        return {"requestId": request_id, "status": "accepted"}

    def lookup(self, request_id: str, _body: Any) -> dict[str, Any]:
        self.calls.append("lookup")
        return {"requestId": request_id, "state": "none"}

    def pull(self, request_id: str, _body: Any) -> dict[str, Any]:
        self.calls.append("pull")
        return {"requestId": request_id, "deliveries": []}

    def acknowledge(self, request_id: str, _body: Any) -> dict[str, Any]:
        self.calls.append("ack")
        return {"requestId": request_id, "acknowledged": True}


def _app(
    *,
    capabilities: frozenset[str] = frozenset({HPA_CHANGE_EXECUTE_CAPABILITY}),
    actor_ids: frozenset[str] = frozenset({"U07A5FM5XPD"}),
    enabled: bool = True,
) -> tuple[FastAPI, _Service]:
    registry = CallerRegistry(
        (
            CompanyApiCallerSettings(
                caller_id="slack-prod",
                token=_TOKEN,
                tenant_ids=frozenset({"TWORK"}),
                channels=frozenset({"slack"}),
                actor_ids=actor_ids,
                allow_anonymous_actor=False,
                capabilities=capabilities,
            ),
        )
    )
    service = _Service(enabled=enabled)
    app = FastAPI()
    install_problem_handlers(app)
    app.include_router(
        create_hpa_change_router(
            service=service,  # type: ignore[arg-type]
            caller_registry=registry,
            is_ready=lambda: True,
        )
    )
    return app, service


def _headers(request_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_TOKEN}",
        "X-Request-ID": request_id,
    }


def _submit_payload() -> dict[str, Any]:
    content = "prompt body"
    return {
        "workspaceId": "TWORK",
        "requestKey": "slack:TWORK:C02C08K7YEN:1720580400.000100",
        "channelId": "C02C08K7YEN",
        "threadTs": "1720580000.000001",
        "threadUrl": (
            "https://lifexio.slack.com/archives/C068FVD5V7Y/"
            "p1720580000000001"
        ),
        "eventTs": "1720580400.000100",
        "requesterUserId": "U07A5FM5XPD",
        "initiatorUserId": "U07A5FM5XPD",
        "threadText": "Bonus 프롬프트 변경",
        "attachments": [
            {
                "name": "handoff.txt",
                "content": content,
                "sha256": hashlib.sha256(content.encode()).hexdigest(),
            }
        ],
        "sourceChannelId": "C068FVD5V7Y",
        "sourceMessageTs": "1720580000.000001",
        "selectionMode": "linked_message",
    }


def test_router_authenticates_dedicated_hpa_capability_and_actor() -> None:
    app, service = _app()

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/hpa-change/requests",
            headers=_headers("hpa:submit:1"),
            json=_submit_payload(),
        )

    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert service.calls == ["submit"]


def test_router_fails_closed_without_capability_or_actor_scope() -> None:
    denied_apps = (
        _app(capabilities=frozenset())[0],
        _app(actor_ids=frozenset({"UOTHER"}))[0],
    )
    statuses: list[int] = []
    for index, app in enumerate(denied_apps):
        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/hpa-change/requests",
                headers=_headers(f"hpa:denied:{index}"),
                json=_submit_payload(),
            )
        statuses.append(response.status_code)

    assert statuses == [403, 403]


def test_router_fails_closed_when_api_coordinator_is_disabled() -> None:
    app, service = _app(enabled=False)

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/hpa-change/deliveries/pull",
            headers=_headers("hpa:pull:disabled"),
            json={"workspaceId": "TWORK"},
        )

    assert response.status_code == 503
    assert response.json()["code"] == "service_not_ready"
    assert service.calls == []


class _AssistantRuntime:
    def answer(self, _request: Any, **_kwargs: Any) -> CompanyAssistantResult:
        return CompanyAssistantResult(route="none", outcome="no_evidence")

    def answer_stage(
        self,
        _request: Any,
        _stage: str,
        **_kwargs: Any,
    ) -> CompanyAssistantResult:
        return CompanyAssistantResult(route="none", outcome="no_evidence")


class _AppCoordinator:
    enabled = True

    def __init__(self) -> None:
        self.closed = False
        self.submissions = 0

    def submit(self, _request: Any) -> HpaChangeSubmissionResult:
        self.submissions += 1
        return HpaChangeSubmissionResult(
            HpaChangeSubmissionState.ACCEPTED,
            request_id="hpa-20260827140000-12345678-12345678",
            user_message="격리 worker에 전달했어",
        )

    def close(self) -> None:
        self.closed = True


def test_company_api_app_wires_hpa_router_and_closes_coordinator() -> None:
    caller = CompanyApiCallerSettings(
        caller_id="slack-prod",
        token=_TOKEN,
        tenant_ids=frozenset({"TWORK"}),
        channels=frozenset({"slack"}),
        actor_ids=frozenset({"U07A5FM5XPD"}),
        allow_anonymous_actor=False,
        capabilities=frozenset(
            {"assistant.turn.read", HPA_CHANGE_EXECUTE_CAPABILITY}
        ),
    )
    coordinator = _AppCoordinator()
    app = create_company_api_app(
        settings=CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(caller,),
            live_device_enabled=False,
            operations_enabled=False,
            automation_enabled_cycles=frozenset(),
        ),
        assistant_runtime=_AssistantRuntime(),
        readiness_probe=lambda: True,
        automation_coordinator=None,
        automation_delivery_broker=None,
        hpa_change_coordinator=coordinator,  # type: ignore[arg-type]
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/hpa-change/requests",
            headers=_headers("hpa:app:submit:1"),
            json=_submit_payload(),
        )

    assert response.status_code == 200
    assert response.json()["status"] == "accepted"
    assert coordinator.submissions == 1
    assert coordinator.closed is True
