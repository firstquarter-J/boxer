"""실제 긴 장애 질문이 Slack부터 API 실행 경계까지 같은 진단을 고르는지 검증한다."""

import logging
from dataclasses import replace
from unittest.mock import Mock

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_operations_route import (
    DeviceOperationsAssistantRoute,
    DeviceOperationsRouteDeps,
)
from boxer_company.assistant.factory import _guard_read_only_request
from boxer_company.assistant.runtime import (
    CompanyAssistantRuntime,
    CompanyAssistantRuntimeDeps,
)
from boxer_company.operation_routing import (
    company_operation_legacy_stage,
    match_company_operation_route,
)
from boxer_company.read_routing import match_notion_playbook_route
from boxer_company.transport_contracts import company_operation_route_names
from boxer_company_adapter_slack.company_api_rollout import (
    CompanyOperationsApiRolloutService,
    CompanyPlaybookApiRolloutService,
)


def test_reported_long_network_question_keeps_operations_and_read_only_boundary():
    # 짧은 질문에서 빠졌던 '마미박스' 문서 토큰과 실제 로그 조사 요청의 충돌을 재현한다.
    question = (
        "MB2-C00330 네트워크 오류 원인 확인해줘. 마미박스 앱 문제인지 유선 네트워크 연결 문제인지, "
        "시스템 로그와 앱 로그를 함께 보고 근거랑 점검할 부분까지 알려줘."
    )
    request = CompanyAssistantRequest(
        request_id="local-network-routing-contract",
        tenant_id="test-tenant",
        actor_id="test-actor",
        channel="slack",
        conversation_id="test-thread",
        question=question,
        locale="ko",
        metadata={"channel_id": "test-channel"},
    )
    expected_route = "device_diagnostic_analysis"
    assert match_company_operation_route(request) == expected_route
    assert company_operation_legacy_stage(request) == "knowledge"
    assert match_notion_playbook_route(request) is None

    diagnostic = Mock(return_value=("장비 네트워크 진단 결과", {"route": "device_diagnostic_freeform"}))
    forbidden_provider = Mock(side_effect=AssertionError("외부 조회나 모델 호출을 실행하면 안 돼"))
    answer_engine = Mock()
    answer_engine.answer = forbidden_provider
    runtime = CompanyAssistantRuntime(
        CompanyAssistantRuntimeDeps(
            answer_engine=answer_engine,
            provider_ready=lambda: False,
            get_s3_client=forbidden_provider,
            recordings_loader=forbidden_provider,
            db_configured=lambda: False,
            s3_query_enabled=lambda: False,
            request_guard=_guard_read_only_request,
            operation_routes=(
                DeviceOperationsAssistantRoute(
                    deps=DeviceOperationsRouteDeps(
                        start_diagnostic_analysis=diagnostic,
                        device_runtime_configured=lambda: True,
                    ),
                ),
            ),
        ),
    )

    def answer_in_process(original, *, route_group):
        # HTTP 전송만 대체하고 API의 실제 stage 선택, 읽기 가드와 장비 실행 route는 통과한다.
        assert route_group == "operations"
        domain_request = replace(original, metadata={**original.metadata, "route_group": route_group})
        assert match_company_operation_route(domain_request) == expected_route
        return runtime.answer_stage(domain_request, route_group)

    api = Mock()
    api.answer.side_effect = answer_in_process
    operation = CompanyOperationsApiRolloutService(
        None,
        api_client=api,
        logger=logging.getLogger(__name__),
        matcher=match_company_operation_route,
        allowed_routes=company_operation_route_names(),
    )
    result = operation.answer(request)
    assert result is not None
    assert result.route == expected_route
    assert result.outcome == "answered"
    assert result.messages[0].body == "장비 네트워크 진단 결과"
    assert result.fallback_reason is None
    api.answer.assert_called_once_with(request, route_group="operations")
    diagnostic.assert_called_once_with(
        question=question,
        device_name="MB2-C00330",
        workspace_id="test-tenant",
        channel_id="test-channel",
        thread_ts="test-thread",
        requested_by="test-actor",
        resend_ssh_open=False,
    )

    playbook_api = Mock()
    playbook = CompanyPlaybookApiRolloutService(
        None, api_client=playbook_api, logger=logging.getLogger(__name__),
    )
    assert playbook.answer(request) is None
    playbook_api.answer.assert_not_called()

    # 라우팅 수정으로 해결해야 한다. 읽기 stage에서 실장비 조사를 허용하는 우회는 금지한다.
    for stage in ("knowledge", "freeform"):
        read_request = replace(request, metadata={**request.metadata, "route_group": stage})
        denied = runtime.answer_stage(read_request, stage)
        assert denied is not None
        assert denied.route == "unsupported_live_diagnostic"
        assert denied.outcome == "denied"
        assert denied.fallback_reason == "read_only_boundary"
    assert diagnostic.call_count == 1
    forbidden_provider.assert_not_called()
