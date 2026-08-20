from unittest.mock import Mock, patch

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.freeform_runtime import (
    build_company_freeform_route,
)


def _request() -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="req-freeform-runtime",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="1700000000.000001",
        question="오늘 상태 어때?",
        locale="ko",
        metadata={"route_group": "freeform"},
    )


def test_claude_runtime_uses_server_client_once() -> None:
    client = Mock()
    with patch(
        "boxer_company.assistant.freeform_runtime._ask_claude",
        return_value="정상",
    ) as ask:
        route = build_company_freeform_route(
            provider="claude",
            claude_client=client,
            provider_ready=lambda: True,
            timeout_message="시간 초과",
        )

        result = route.handle(_request())

    assert result is not None
    assert result.messages[0].body == "정상"
    assert ask.call_count == 1
    assert ask.call_args.args[0] is client


def test_ollama_runtime_disables_thinking_output() -> None:
    with patch(
        "boxer_company.assistant.freeform_runtime._ask_ollama_chat",
        return_value="정상",
    ) as ask:
        route = build_company_freeform_route(
            provider="ollama",
            claude_client=None,
            provider_ready=lambda: True,
            timeout_message="시간 초과",
        )

        result = route.handle(_request())

    assert result is not None
    assert result.messages[0].body == "정상"
    assert ask.call_args.kwargs["think"] is False
