from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.operations import (
    OPERATION_CONFIRMATION_REQUIRED_ROUTE,
    OPERATION_SINGLE_TARGET_REQUIRED_ROUTE,
    OperationConfirmationAssistantRoute,
    OperationSingleTargetAssistantRoute,
    as_operations_request,
    company_operation_route_names,
    is_mutation_capable_company_operation,
    is_uncertain_company_mutation_result,
    match_company_operation_route,
    match_live_device_company_operation_route,
)


def _request(
    question: str,
    *,
    context_entries: tuple[dict[str, str], ...] = (),
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="req-operation-router",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="1700000000.000001",
        question=question,
        locale="ko",
        context_entries=context_entries,  # type: ignore[arg-type]
        metadata={"channel_id": "C1"},
    )


def test_slack_request_is_scoped_only_for_pure_operation_matching() -> None:
    request = _request("MB2-C00419 PM2 상태 확인")

    scoped = as_operations_request(request)

    assert request.metadata.get("route_group") is None
    assert scoped.metadata["route_group"] == "operations"
    assert match_company_operation_route(request) == "device_pm2_probe"


def test_private_and_write_routes_share_the_same_operation_matcher() -> None:
    assert (
        match_company_operation_route(
            _request("12345678910 유저 조회")
        )
        == "app_user_lookup"
    )
    assert (
        match_company_operation_route(_request("이 스레드 학습해줘"))
        == "thread_playbook_learning"
    )
    assert (
        match_company_operation_route(
            _request("MB2-C00419 로그 업로드 확인")
        )
        == "device_log_upload"
    )
    assert (
        match_company_operation_route(
            _request("48194663047 2026-03-06 영상 다운로드")
        )
        == "device_file_download"
    )


def test_generic_question_does_not_enter_operation_transport() -> None:
    assert match_company_operation_route(_request("오늘 점심 뭐 먹지?")) is None
    assert "device_pm2_probe" in company_operation_route_names()
    assert "device_file_download" in company_operation_route_names()


def test_question_and_negative_mutation_candidates_stop_at_common_guard() -> None:
    questions = (
        "MB2-C00419 박스 업데이트 방법 알려줘",
        "MB2-C00419 박스 업데이트하면 돼",
        "MB2-C00419 에이전트 업데이트 가능해?",
        "MB2-C00419 전원 꺼도 돼",
        "MB2-C00419 진단 시작하지마",
        "MB2-C00419 진단 시작해도 돼",
        "MB2-C00419 메모리 패치 괜찮아",
        "MB2-C00419 로그 업로드 가능한지 확인해줘",
        "MB2-C00419 로그 업로드해도 돼",
        "48194663047 2026-03-06 영상 복구 가능한지",
        "48194663047 2026-03-06 영상 복구해도 돼",
        "48194663047 2026년 3월 스트리밍 종료 영상 복원 가능한지",
        "48194663047 2026년 3월 스트리밍 종료 영상 복원할 수 있어",
        "이 스레드 학습하는 방법 알려줘",
        "이 스레드 학습해도 돼",
        "이 스레드 학습시켜도 돼",
        "48194663047 2026-03-06 장비 파일 있나",
        "48194663047 2026-03-06 영상 다운로드 가능해?",
    )

    guard = OperationConfirmationAssistantRoute()
    for question in questions:
        request = _request(question)
        assert (
            match_company_operation_route(request)
            == OPERATION_CONFIRMATION_REQUIRED_ROUTE
        )
        result = guard.handle(as_operations_request(request))
        assert result is not None
        assert result.outcome == "needs_input"
        assert result.fallback_reason == "explicit_execution_required"


def test_historical_power_question_stays_read_only_diagnostic_analysis() -> None:
    assert (
        match_company_operation_route(
            _request("MB2-C00419 전원 꺼진 이유 분석해줘")
        )
        == "device_diagnostic_analysis"
    )


def test_multiple_or_actor_ambiguous_targets_stop_at_common_guard() -> None:
    questions = (
        "MB2-C00419 MB2-C00570 전원 꺼줘",
        "MB2-C00419 MB2-C00570 박스 업데이트",
        "MB2-C00419 MB2-C00570 PM2 상태",
        "MB2-C00419 MB2-C00570 캡처보드 상태",
        "MB2-C00419 MB2-C00570 LED 상태",
        "MB2-C00419 MB2-C00570 장비 상태",
        "MB2-C00419 MB2-C00570 장비 소리 출력 점검",
        "MB2-C00419 MB2-C00570 원격 접속 ping 확인",
        "48194663047 48194663048 2026년 3월 스트리밍 종료 영상 복원",
        "48194663047 48194663048 2026-03-06 영상 복구",
    )
    guard = OperationSingleTargetAssistantRoute()
    for question in questions:
        request = _request(question)
        assert (
            match_company_operation_route(request)
            == OPERATION_SINGLE_TARGET_REQUIRED_ROUTE
        )
        result = guard.handle(as_operations_request(request))
        assert result is not None
        assert result.outcome == "needs_input"
        assert result.fallback_reason == "single_operation_target_required"

    # 다른 참여자의 target과 같은 actor의 복수 target도 실행 scope로 쓰지 않는다.
    for context_entries in (
        (
            {
                "kind": "message",
                "source": "slack",
                "author_id": "OTHER",
                "text": "48194663047",
            },
        ),
        (
            {
                "kind": "message",
                "source": "slack",
                "author_id": "U1",
                "text": "48194663047 48194663048",
            },
        ),
    ):
        assert (
            match_company_operation_route(
                _request(
                    "2026-03-06 영상 복구",
                    context_entries=context_entries,
                )
            )
            == OPERATION_SINGLE_TARGET_REQUIRED_ROUTE
        )


def test_explicit_mutation_commands_keep_their_operation_routes() -> None:
    expected = {
        "MB2-C00419 박스 업데이트": "device_box_update",
        "MB2-C00419 에이전트 업데이트": "device_agent_update",
        "MB2-C00419 장비 종료해": "device_power_off",
        "MB2-C00419 로그 업로드 확인": "device_log_upload",
        "48194663047 2026-03-06 영상 복구": "device_file_recovery",
        (
            "48194663047 2026년 3월 스트리밍 종료 영상 복원"
        ): "recording_streaming_restore",
        "이 스레드 학습해줘": "thread_playbook_learning",
    }
    for question, route in expected.items():
        assert match_company_operation_route(_request(question)) == route
        assert is_mutation_capable_company_operation(_request(question))


def test_read_only_operations_do_not_enter_mutation_guard() -> None:
    questions = (
        "12345678910 유저 조회",
        "s3 영상 12345678910",
        "s3 로그 MB2-C00419 2026-03-04",
        "db 조회 select seq from recordings limit 1",
        "요청 로그 오늘 최근 5",
    )

    for question in questions:
        request = _request(question)
        assert match_company_operation_route(request) is not None
        assert not is_mutation_capable_company_operation(request)


def test_live_device_gate_excludes_private_and_file_id_only_reads() -> None:
    live_questions = (
        "MB2-C00419 PM2 상태 확인",
        "48194663047 2026-03-06 장비 파일 확인",
        "48194663047 2026-03-06 영상 다운로드",
    )
    non_live_questions = (
        "12345678910 유저 조회",
        "48194663047 2026-03-06 fileId 확인",
        "장비 음성 목록 알려줘",
        "db 조회 select seq from recordings limit 1",
    )

    for question in live_questions:
        assert match_live_device_company_operation_route(_request(question))
    for question in non_live_questions:
        assert (
            match_live_device_company_operation_route(_request(question))
            is None
        )


def test_live_device_reads_that_can_open_ssh_enter_mutation_guard() -> None:
    expected = {
        "MB2-C00419 업데이트 상태": "device_update_status",
        "MB2-C00419 장비 소리 출력 점검": "device_audio_probe",
        "MB2-C00419 원격 접속 ping 확인": "device_remote_access_probe",
        "MB2-C00419 pm2 상태": "device_pm2_probe",
        "MB2-C00419 캡처보드 상태": "device_captureboard_probe",
        "MB2-C00419 LED 상태": "device_led_probe",
        "MB2-C00419 장비 상태": "device_status_probe",
    }

    for question, route in expected.items():
        request = _request(question)
        assert match_company_operation_route(request) == route
        assert is_mutation_capable_company_operation(request)

    followup = _request(
        "최근 종료 원인",
        context_entries=(
            {
                "kind": "message",
                "source": "slack",
                "author_id": "U1",
                "text": "MB2-C00419 진단 시작",
            },
        ),
    )
    assert (
        match_company_operation_route(followup)
        == "device_diagnostic_followup"
    )
    assert is_mutation_capable_company_operation(followup)


def test_only_ambiguous_failed_mutation_results_stay_uncertain() -> None:
    def result(fallback_reason: str) -> CompanyAssistantResult:
        return CompanyAssistantResult(
            route="device_power_off",
            outcome="failed",
            messages=(AssistantMessage(body="실패"),),
            fallback_reason=fallback_reason,
        )

    for fallback_reason in (
        "sms_delivery_receipt_persist_failed",
        "sms_delivery_confirmation_required",
        "voice_guide_dispatch_uncertain",
    ):
        assert is_uncertain_company_mutation_result(
            mutation_route="device_power_off",
            result=result(fallback_reason),
        )

    for fallback_reason in (
        "operation_error",
        "knowledge_write_failed",
    ):
        assert is_uncertain_company_mutation_result(
            mutation_route="device_power_off",
            result=result(fallback_reason),
            side_effect_attempted=True,
        )
        assert not is_uncertain_company_mutation_result(
            mutation_route="device_power_off",
            result=result(fallback_reason),
            side_effect_attempted=False,
        )

    assert is_uncertain_company_mutation_result(
        mutation_route="recording_streaming_restore",
        result=result("dependency_error"),
        side_effect_attempted=True,
    )
    assert not is_uncertain_company_mutation_result(
        mutation_route="recording_streaming_restore",
        result=result("dependency_error"),
        side_effect_attempted=False,
    )
    assert is_uncertain_company_mutation_result(
        mutation_route="device_detail",
        result=result("query_error"),
        side_effect_attempted=True,
    )
    assert not is_uncertain_company_mutation_result(
        mutation_route="device_detail",
        result=result("query_error"),
        side_effect_attempted=False,
    )
    for fallback_reason in (
        "feature_disabled",
        "device_health_alert_target_lookup_failed",
        "voice_guide_dispatch_failed",
    ):
        assert not is_uncertain_company_mutation_result(
            mutation_route="device_power_off",
            result=result(fallback_reason),
        )
