from unittest.mock import Mock, patch

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.freeform_runtime import (
    build_company_freeform_route,
    build_company_team_fun_route,
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


def _fun_request() -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="req-fun-runtime",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="1700000000.000001",
        question="배포도 쉽지 모대",
        locale="ko",
        metadata={"route_group": "fun"},
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


def test_fun_runtime_keeps_legacy_ollama_contract() -> None:
    with (
        patch(
            "boxer_company.assistant.freeform_runtime._check_ollama_health",
            return_value={"ok": True},
        ) as health,
        patch(
            "boxer_company.assistant.freeform_runtime._ask_ollama_chat",
            return_value="배포가 또 삐끗했네 모대?",
        ) as ask,
    ):
        route = build_company_team_fun_route(
            provider="ollama",
            claude_client=None,
            context_max_chars=5_000,
        )
        result = route.handle(_fun_request())

    assert result is not None
    assert result.messages[0].body == "배포가 또 삐끗했네 모대?"
    health.assert_called_once_with(
        timeout_sec=2,
        model="qwen2.5:1.5b",
    )
    ask_kwargs = ask.call_args.kwargs
    assert "기본 템플릿보다 이상하면" in ask_kwargs["system_prompt"]
    assert ask_kwargs["model"] == "qwen2.5:1.5b"
    assert ask_kwargs["timeout_sec"] == 60
    assert ask_kwargs["max_tokens"] == 48
    assert ask_kwargs["temperature"] == 0.5
    assert ask_kwargs["think"] is False


def test_fun_runtime_uses_template_when_legacy_provider_is_down() -> None:
    with patch(
        "boxer_company.assistant.freeform_runtime._check_ollama_health",
        return_value={"ok": False},
    ):
        route = build_company_team_fun_route(
            provider="ollama",
            claude_client=None,
            context_max_chars=5_000,
        )
        result = route.handle(_fun_request())

    assert result is not None
    assert result.outcome == "answered"
    assert result.used_llm is False
    assert result.fallback_reason == "provider_unavailable"
    assert result.messages[0].body.endswith("모대?")
