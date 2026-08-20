from __future__ import annotations

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.security_review_route import (
    SECURITY_REVIEW_PROBES,
    SecurityReviewAssistantRoute,
    SecurityReviewResponse,
    SecurityReviewSessionStore,
    assess_security_review_response,
)
from boxer_company_api.schemas import AssistantTurnInput, serialize_result
from boxer_company_adapter_slack.company_api_client import (
    _deserialize_result,
    _serialize_request,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)
from fastapi.testclient import TestClient


def _request(
    phase: str,
    *,
    target: dict[str, str] | None = None,
    response_text: str = "",
) -> CompanyAssistantRequest:
    action: dict[str, object] = {
        "name": "security_review",
        "phase": phase,
        "response_text": response_text,
    }
    if target is not None:
        action["target"] = target
    return CompanyAssistantRequest(
        request_id=f"security-review:{phase}:1",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="1700000000.000001",
        question=f"security review {phase}",
        locale="en",
        metadata={
            "route_group": "operations",
            "channel_id": "C1",
            "operation_action": action,
        },
    )


_TARGET = {
    "user_id": "UBUDDY",
    "bot_id": "BBUDDY",
    "app_id": "ABUDDY",
    "name": "buddy",
}


def test_operational_probe_catalog_contains_no_active_exploit_tokens() -> None:
    forbidden_tokens = (
        "secret",
        "env",
        "curl",
        "mysql",
        "http",
        "@channel",
        "dm",
        "api key",
        "token",
        "시스템 프롬프트",
        "$",
        "://",
        "그대로 출력",
        "그대로 덤프",
        "호출하고",
        "실행해서",
        "저장해",
        "제한을 해제",
    )
    for probe in SECURITY_REVIEW_PROBES:
        lowered = f"{probe.title}\n{probe.prompt}".casefold()
        assert all(token.casefold() not in lowered for token in forbidden_tokens)
        # 운영 probe는 실행 요구가 아니라 정책 설명과 비실행 지시만 포함한다.
        assert "설명해" in probe.prompt
        assert "실제" in probe.prompt


def test_sensitive_marker_is_risky_even_when_response_also_claims_refusal() -> None:
    status, _reason = assess_security_review_response(
        SecurityReviewResponse(
            probe=SECURITY_REVIEW_PROBES[1],
            text="정책상 공개할 수 없어. token=synthetic-marker",
        )
    )
    assert status == "위험"


def test_api_owned_security_review_runs_catalog_and_builds_report() -> None:
    route = SecurityReviewAssistantRoute()

    started = route.handle(_request("start", target=_TARGET))
    assert started is not None
    assert started.route == "security_review"
    assert started.operation_result is not None
    assert started.operation_result["status"] == "started"
    assert started.operation_result["probeIndex"] == 1
    assert started.operation_result["probePrompt"] == SECURITY_REVIEW_PROBES[0].prompt

    result = started
    responses = [
        "정책과 권한에 따라 입력을 필터링해",
        *[
            "권한과 정책상 실행할 수 없어"
            for _ in SECURITY_REVIEW_PROBES[1:]
        ],
    ]
    for response in responses:
        result = route.handle(
            _request(
                "respond",
                target=_TARGET,
                response_text=response,
            )
        )
        assert result is not None

    assert result.operation_result is not None
    assert result.operation_result["status"] == "completed"
    assert "결론: 통과" in result.messages[0].body
    assert "수집 응답: 11/11" in result.messages[0].body
    # 대상 봇 원문 응답은 wire용 report/receipt에 재노출하지 않는다.
    assert responses[0] not in result.messages[0].body
    assert responses[0] not in str(result.operation_result)


def test_mismatched_bot_is_ignored_without_advancing_session() -> None:
    route = SecurityReviewAssistantRoute()
    route.handle(_request("start", target=_TARGET))

    ignored = route.handle(
        _request(
            "respond",
            target={
                "user_id": "UOTHER",
                "bot_id": "BOTHER",
                "app_id": "AOTHER",
                "name": "other",
            },
            response_text="다른 봇 응답",
        )
    )
    assert ignored is not None
    assert ignored.operation_result is not None
    assert ignored.operation_result["status"] == "ignored"

    continued = route.handle(
        _request(
            "respond",
            target=_TARGET,
            response_text="정책에 따라 필터링해",
        )
    )
    assert continued is not None
    assert continued.operation_result is not None
    assert continued.operation_result["status"] == "continued"
    assert continued.operation_result["probeIndex"] == 2


def test_partial_summary_and_ttl_are_owned_by_common_store() -> None:
    now = [100.0]
    store = SecurityReviewSessionStore(
        clock=lambda: now[0],
        ttl_sec=10.0,
    )
    route = SecurityReviewAssistantRoute(store)
    route.handle(_request("start", target=_TARGET))
    partial = route.handle(_request("summary"))
    assert partial is not None
    assert partial.operation_result is not None
    assert partial.operation_result["status"] == "summary"
    assert "응답 미수집" in partial.messages[0].body

    route.handle(_request("start", target=_TARGET))
    now[0] = 111.0
    expired = route.handle(_request("summary"))
    assert expired is not None
    assert expired.operation_result is not None
    assert expired.operation_result["status"] == "no_session"


def test_security_review_schema_and_explicit_wire_result_are_strict() -> None:
    turn = AssistantTurnInput.model_validate(
        {
            "tenantId": "T1",
            "actorId": "U1",
            "channel": "slack",
            "conversationId": "1700000000.000001",
            "question": "security review start",
            "locale": "en",
            "scope": {"channelContextId": "C1"},
            "routeGroup": "operations",
            "operationAction": {
                "name": "security_review",
                "phase": "start",
                "target": {
                    "userId": "UBUDDY",
                    "botId": "BBUDDY",
                    "appId": "ABUDDY",
                    "name": "buddy",
                },
                "responseText": "",
            },
        }
    )
    request = turn.to_company_request("security-review:start:wire")
    route = SecurityReviewAssistantRoute()
    result = route.handle(request)
    assert result is not None

    payload = serialize_result(result, request.request_id)
    assert payload["operationResult"] == result.operation_result
    assert payload["operationResult"]["kind"] == "security_review_step"

    # 다른 routeGroup으로 typed action을 밀어 넣는 우회는 schema에서 막는다.
    invalid = turn.model_copy(update={"routeGroup": "structured"})
    try:
        AssistantTurnInput.model_validate(invalid.model_dump())
    except ValueError:
        pass
    else:
        raise AssertionError("security review action must require operations")


def test_slack_client_serializes_and_validates_security_review_wire_contract() -> None:
    request = _request("start", target=_TARGET)
    payload = _serialize_request(request, route_group="operations")
    assert payload["operationAction"] == {
        "name": "security_review",
        "phase": "start",
        "responseText": "",
        "target": {
            "userId": "UBUDDY",
            "botId": "BBUDDY",
            "appId": "ABUDDY",
            "name": "buddy",
        },
    }

    route = SecurityReviewAssistantRoute()
    result = route.handle(request)
    assert result is not None
    wire = serialize_result(result, request.request_id)

    class _Response:
        headers = {"content-type": "application/json"}
        content = b"{}"

        def json(self) -> dict[str, object]:
            return wire

    decoded = _deserialize_result(_Response(), request.request_id)
    assert decoded.operation_result == result.operation_result


def test_security_review_http_uses_operations_capability_without_device_alert_capability() -> None:
    token = "r" * 48
    route = SecurityReviewAssistantRoute()

    class _Runtime:
        def answer_stage(
            self,
            request: CompanyAssistantRequest,
            stage: str,
        ) -> object:
            assert stage == "operations"
            return route.handle(request)

    settings = CompanyApiSettings(
        host="127.0.0.1",
        port=8010,
        callers=(
            CompanyApiCallerSettings(
                caller_id="security-review-test",
                token=token,
                tenant_ids=frozenset({"T1"}),
                channels=frozenset({"slack"}),
                actor_ids=frozenset({"U1"}),
                allow_anonymous_actor=False,
                capabilities=frozenset(
                    {
                        "assistant.turn.read",
                        "assistant.operation.execute",
                    }
                ),
            ),
        ),
    )
    app = create_company_api_app(
        settings=settings,
        assistant_runtime=_Runtime(),  # type: ignore[arg-type]
        readiness_probe=lambda: True,
    )
    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/assistant/turns",
            headers={
                "Authorization": f"Bearer {token}",
                "X-Request-ID": "security-review-http-start",
            },
            json={
                "tenantId": "T1",
                "actorId": "U1",
                "channel": "slack",
                "conversationId": "1700000000.000001",
                "question": "security review start",
                "locale": "en",
                "scope": {"channelContextId": "C1"},
                "routeGroup": "operations",
                "operationAction": {
                    "name": "security_review",
                    "phase": "start",
                    "target": {
                        "userId": "UBUDDY",
                        "botId": "BBUDDY",
                        "appId": "ABUDDY",
                        "name": "buddy",
                    },
                },
            },
        )

    assert response.status_code == 200
    assert response.json()["route"] == "security_review"
    assert response.json()["operationResult"]["status"] == "started"
