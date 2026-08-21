from dataclasses import replace
from unittest.mock import patch

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.operations import (
    as_operations_request,
    build_company_operation_routes,
    company_operation_legacy_stage,
    company_operation_route_names,
    is_mutation_capable_company_operation,
    is_uncertain_company_mutation_result,
    match_company_operation_route,
    match_live_device_company_operation_route,
)
from boxer_company.assistant.device_operations_route import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_OPERATION_DELIVERY_ACTION,
)
from boxer_company.assistant.service import CompanyAssistantService


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


def test_operation_overlap_keeps_legacy_handler_stage_and_exact_executor() -> None:
    cases = (
        (
            "이 스레드 학습해. 48194663047 2026-03-06 장비 파일 복구해줘",
            "thread_playbook_learning",
            "pre_notion",
        ),
        (
            "s3 로그 MMB001 2026-03-06 48194663047 장비 파일 복구해줘",
            "admin_s3_device_log",
            "pre_notion",
        ),
        (
            "48194663047 2026-03-06 장비 파일 복구해줘",
            "device_file_recovery",
            "device",
        ),
        (
            "48194663047 유저 조회",
            "app_user_lookup",
            "barcode",
        ),
        (
            "MB2-C00419 진단 시작하고 48194663047 2026-03-06 장비 파일 복구해줘",
            "device_diagnostic_snapshot",
            "device",
        ),
        (
            "MB2-C00419 음성을 지니로 바꾸고 48194663047 2026-03-06 파일 복구해줘",
            "device_voice_change",
            "device",
        ),
        (
            "MB2-C00419 전원 꺼주고 48194663047 바코드 검증해줘",
            "device_power_off",
            "device",
        ),
        (
            "48194663047 유효성 검사하고 2024년 4월 영상 복원해줘",
            "barcode_validation_status",
            "barcode",
        ),
        (
            "MB2-C00419 음성을 지니로 바꾸고 장비 파일 다운로드해줘",
            "device_file_download_barcode_required",
            "device",
        ),
    )
    service = CompanyAssistantService(
        build_company_operation_routes(context_max_chars=12_000)
    )

    with (
        patch(
            "boxer_company.assistant.knowledge_write_route."
            "company_settings.THREAD_PLAYBOOK_LEARNING_ENABLED",
            False,
        ),
        patch(
            "boxer_company.assistant.private_admin_routes."
            "core_settings.S3_QUERY_ENABLED",
            False,
        ),
    ):
        for question, route, stage in cases:
            request = _request(question)
            assert match_company_operation_route(request) == route
            assert company_operation_legacy_stage(request) == stage
            if route in {"thread_playbook_learning", "admin_s3_device_log"}:
                # 충돌 재현 두 건은 feature-off 결과만으로도 API executor가
                # 파일 mutation보다 정확한 legacy route에서 멈췄음을 증명한다.
                result = service.answer(as_operations_request(request))
                assert result is not None
                assert result.route == route
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


def test_typed_diagnostic_probe_uses_the_exact_knowledge_route() -> None:
    request = _request("48194663047 유저 조회")
    request = replace(
        request,
        metadata={
            **request.metadata,
            "operation_action": {
                "name": DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
            },
        },
    )
    assert match_company_operation_route(request) == "device_diagnostic_followup"
    assert company_operation_legacy_stage(request) == "knowledge"


def test_device_delivery_receipt_precedes_natural_language_routes() -> None:
    request = _request(
        "이 스레드 학습해. MB2-C00419 박스 업데이트",
    )
    request = replace(
        request,
        metadata={
            **request.metadata,
            "operation_action": {
                "name": DEVICE_OPERATION_DELIVERY_ACTION,
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
        },
    )

    assert (
        match_company_operation_route(request)
        == DEVICE_OPERATION_DELIVERY_ACTION
    )
    assert company_operation_legacy_stage(request) == "device"
    assert is_mutation_capable_company_operation(request)
    assert (
        match_live_device_company_operation_route(request)
        == DEVICE_OPERATION_DELIVERY_ACTION
    )
    assert DEVICE_OPERATION_DELIVERY_ACTION in company_operation_route_names()


def test_diagnostic_candidates_keep_their_legacy_knowledge_precedence() -> None:
    diagnostic_context = (
        {
            "kind": "message",
            "source": "slack",
            "author_id": "U1",
            "text": "MB2-C00419 진단 시작",
        },
    )
    followup = _request(
        "48194663047 영상 개수 알려줘",
        context_entries=diagnostic_context,
    )
    assert match_company_operation_route(followup) == "device_diagnostic_followup"
    assert company_operation_legacy_stage(followup) == "knowledge"

    for question, route in (
        ("48194663047 유저 조회", "app_user_lookup"),
        ("48194663047 유효성 검사해줘", "barcode_validation_status"),
        (
            "48194663047 2024년 4월 영상 복원해줘",
            "recording_streaming_restore",
        ),
    ):
        request = _request(question, context_entries=diagnostic_context)
        assert match_company_operation_route(request) == route
        assert company_operation_legacy_stage(request) == "barcode"

    # Notion playbook은 기존 knowledge handler에서 freeform live 진단보다
    # 먼저였으므로 문서형 질문을 operation으로 올리지 않는다.
    playbook_question = _request("MB2-C00419 ffmpeg 로그 확인해줘")
    assert match_company_operation_route(playbook_question) is None


def test_streaming_restore_keeps_the_legacy_pre_device_guard() -> None:
    for question in (
        (
            "MB2-C00419 음성을 지니로 바꾸고 48194663047 "
            "2026년 3월 스트리밍 종료 영상 복원해줘"
        ),
        (
            "MB2-C00419 진단 시작하고 48194663047 "
            "2026년 3월 스트리밍 종료 영상 복원해줘"
        ),
        (
            "MB2-C00419 박스 업데이트하고 48194663047 "
            "2026년 3월 스트리밍 종료 영상 복원해줘"
        ),
        (
            "MB2-C00419 전원 꺼주고 48194663047 "
            "2026년 3월 스트리밍 종료 영상 복원해줘"
        ),
    ):
        request = _request(question)
        assert (
            match_company_operation_route(request)
            == "recording_streaming_restore"
        )
        assert company_operation_legacy_stage(request) == "barcode"


def test_question_and_negative_forms_keep_legacy_operation_matching() -> None:
    expected = {
        "MB2-C00419 박스 업데이트 방법 알려줘": "device_box_update",
        "MB2-C00419 박스 업데이트하면 돼": "device_box_update",
        "MB2-C00419 에이전트 업데이트 가능해?": "device_agent_update",
        "MB2-C00419 전원 꺼도 돼": "device_power_off",
        "MB2-C00419 진단 시작하지마": "device_diagnostic_snapshot",
        "MB2-C00419 진단 시작해도 돼": "device_diagnostic_snapshot",
        "MB2-C00419 메모리 패치 괜찮아": "device_memory_patch",
        "MB2-C00419 로그 업로드 가능한지 확인해줘": "device_log_upload",
        "MB2-C00419 로그 업로드해도 돼": "device_log_upload",
        "48194663047 2026-03-06 영상 복구 가능한지": "device_file_recovery",
        "48194663047 2026-03-06 영상 복구해도 돼": "device_file_recovery",
        (
            "48194663047 2026년 3월 스트리밍 종료 영상 복원 가능한지"
        ): "recording_streaming_restore",
        (
            "48194663047 2026년 3월 스트리밍 종료 영상 복원할 수 있어"
        ): "recording_streaming_restore",
        "이 스레드 학습하는 방법 알려줘": "thread_playbook_learning",
        "이 스레드 학습해도 돼": "thread_playbook_learning",
        "이 스레드 학습시켜도 돼": "thread_playbook_learning",
        "48194663047 2026-03-06 장비 파일 있나": "device_file_lookup",
        "48194663047 2026-03-06 영상 다운로드 가능해?": "device_file_download",
    }

    for question, route in expected.items():
        assert match_company_operation_route(_request(question)) == route


def test_historical_power_question_keeps_legacy_power_matcher_precedence() -> None:
    assert (
        match_company_operation_route(
            _request("MB2-C00419 전원 꺼진 이유 분석해줘")
        )
        == "device_power_off"
    )


def test_multiple_and_thread_targets_keep_legacy_first_scope_matching() -> None:
    expected = {
        "MB2-C00419 MB2-C00570 전원 꺼줘": "device_power_off",
        "MB2-C00419 MB2-C00570 박스 업데이트": "device_box_update",
        "MB2-C00419 MB2-C00570 PM2 상태": "device_pm2_probe",
        "MB2-C00419 MB2-C00570 캡처보드 상태": "device_captureboard_probe",
        "MB2-C00419 MB2-C00570 LED 상태": "device_led_probe",
        "MB2-C00419 MB2-C00570 장비 상태": "device_status_probe",
        "MB2-C00419 MB2-C00570 장비 소리 출력 점검": "device_audio_probe",
        "MB2-C00419 MB2-C00570 원격 접속 ping 확인": "device_remote_access_probe",
        (
            "48194663047 48194663048 2026년 3월 스트리밍 종료 영상 복원"
        ): "recording_streaming_restore",
        "48194663047 48194663048 2026-03-06 영상 복구": "device_file_recovery",
    }
    for question, route in expected.items():
        assert match_company_operation_route(_request(question)) == route

    # 기존 Slack은 author별 scope를 분리하지 않고 thread의 첫 바코드를 쓴다.
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
        assert match_company_operation_route(
            _request(
                "2026-03-06 영상 복구",
                context_entries=context_entries,
            )
        ) == "device_file_recovery"


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
        "device_operation_delivery_receipt_in_progress",
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
