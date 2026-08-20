from __future__ import annotations

from dataclasses import replace
from unittest.mock import Mock

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_file_operations_route import (
    DeviceFileOperationsAssistantRoute,
    DeviceFileOperationsRouteDeps,
    match_device_file_operation_route,
    needs_device_file_operation_context,
)


_BARCODE = "48194663047"
_DEVICE = "MB2-C00419"


def _request(
    question: str,
    *,
    route_group: str | None = "operations",
    metadata: dict[str, str] | None = None,
    context_entries: tuple[dict[str, str], ...] = (),
) -> CompanyAssistantRequest:
    request_metadata = dict(metadata or {})
    if route_group is not None:
        request_metadata["route_group"] = route_group
    return CompanyAssistantRequest(
        request_id="REQ-FILE-OP-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
        context_entries=context_entries,  # type: ignore[arg-type]
        metadata=request_metadata,
    )


def _deps(**changes: object) -> DeviceFileOperationsRouteDeps:
    base = DeviceFileOperationsRouteDeps(
        s3_client_factory=Mock(return_value=object()),
        recordings_loader=Mock(return_value={"rows": []}),
        device_contexts_loader=Mock(
            return_value=[{"deviceName": _DEVICE}]
        ),
        hospital_room_device_loader=Mock(
            return_value=[{"deviceName": _DEVICE}]
        ),
        locate_files=Mock(return_value=("*파일 조회 결과*", {"records": []})),
        check_log_upload=Mock(
            return_value=("*장비 로그 업로드 확인*", {})
        ),
        send_device_command=Mock(return_value={"ok": True}),
        s3_query_enabled=lambda: True,
        db_configured=lambda: True,
        mda_configured=lambda: True,
        device_runtime_configured=lambda: True,
        download_configured=lambda: True,
        recovery_enabled=lambda: True,
        recovery_configured=lambda: True,
    )
    return replace(base, **changes)


def test_matcher_requires_exact_explicit_scope_and_preserves_month_restore() -> None:
    expected = {
        f"{_DEVICE} 로그 업로드 확인": "device_log_upload",
        f"{_BARCODE} 2026-03-06 파일 있나": "device_file_lookup",
        f"{_BARCODE} 2026-03-06 영상 다운로드": "device_file_download",
        f"{_BARCODE} 2026-03-06 영상 복구": "device_file_recovery",
    }
    for question, route in expected.items():
        assert match_device_file_operation_route(_request(question)) == route

    # 월 단위 recordings 복원은 기존 전용 operation에 넘긴다.
    assert (
        match_device_file_operation_route(
            _request(f"{_BARCODE} 2026년 3월 영상 복구")
        )
        is None
    )
    for request in (
        _request("로그 업로드 확인", metadata={"device_name": _DEVICE}),
        _request(f"{_DEVICE} MB2-C00999 로그 업로드 확인"),
        _request(f"{_BARCODE} 12345678910 2026-03-06 영상 다운로드"),
        _request(
            f"{_BARCODE} 2026-03-06 영상 다운로드",
            route_group="device",
        ),
    ):
        assert match_device_file_operation_route(request) is None


def test_matcher_accepts_exact_hospital_room_and_same_actor_thread_scope() -> None:
    assert (
        match_device_file_operation_route(
            _request("나무정원여성병원(양주) 2층1-1진료실 로그 업로드 확인")
        )
        == "device_log_upload"
    )
    thread_context = (
        {
            "kind": "message",
            "source": "slack",
            "author_id": "ACTOR-1",
            "text": f"{_BARCODE} 파일을 확인해줘",
        },
    )
    assert (
        match_device_file_operation_route(
            _request(
                "2026-03-06 영상 다운로드",
                context_entries=thread_context,
            )
        )
        == "device_file_download"
    )
    # 다른 actor의 thread target이나 여러 바코드는 실행 scope로 쓰지 않는다.
    assert (
        match_device_file_operation_route(
            _request(
                "2026-03-06 영상 다운로드",
                context_entries=(
                    {
                        **thread_context[0],
                        "author_id": "OTHER-ACTOR",
                    },
                ),
            )
        )
        is None
    )
    assert (
        match_device_file_operation_route(
            _request(
                "2026-03-06 영상 다운로드",
                context_entries=(
                    thread_context[0],
                    {
                        **thread_context[0],
                        "text": "12345678910 파일 확인",
                    },
                ),
            )
        )
        is None
    )


def test_gateway_loads_context_only_when_current_target_is_missing() -> None:
    assert needs_device_file_operation_context("2026-03-06 영상 다운로드")
    assert needs_device_file_operation_context("로그 업로드 확인")
    assert not needs_device_file_operation_context(
        f"{_BARCODE} 2026-03-06 영상 다운로드"
    )
    assert not needs_device_file_operation_context(
        f"{_DEVICE} 로그 업로드 확인"
    )


def test_log_upload_dispatches_mda_command_at_most_once() -> None:
    send_command = Mock(return_value={"ok": True})

    def check_log_upload(
        _s3_client: object,
        device_name: str,
        _log_date: str,
        **kwargs: object,
    ) -> tuple[str, dict[str, object]]:
        dispatcher = kwargs["dispatch_device_command"]
        assert callable(dispatcher)
        dispatcher(device_name, "fdl")
        return "*장비 로그 업로드 확인*\n• 결과: 요청 완료", {}

    check = Mock(side_effect=check_log_upload)
    route = DeviceFileOperationsAssistantRoute(
        _deps(check_log_upload=check, send_device_command=send_command)
    )

    result = route.handle(
        _request(f"{_DEVICE} 2026-03-06 로그 업로드 확인")
    )

    assert result is not None
    assert result.route == "device_log_upload"
    assert result.outcome == "answered"
    check.assert_called_once()
    send_command.assert_called_once_with(_DEVICE, command="fdl")


def test_hospital_room_log_upload_resolves_exactly_one_device() -> None:
    hospital_loader = Mock(return_value=[{"deviceName": _DEVICE}])
    check = Mock(return_value=("*장비 로그 업로드 확인*", {}))
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            hospital_room_device_loader=hospital_loader,
            check_log_upload=check,
        )
    )

    result = route.handle(
        _request("나무정원여성병원(양주) 2층1-1진료실 로그 업로드 확인")
    )

    assert result is not None
    assert result.outcome == "answered"
    hospital_loader.assert_called_once_with(
        "나무정원여성병원(양주)",
        "2층1-1진료실",
    )
    assert check.call_count == 1
    assert check.call_args.args[1] == _DEVICE


def test_file_id_lookup_does_not_require_live_device_runtime() -> None:
    locate = Mock(return_value=("*fileId 조회 결과*", {"records": []}))
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            locate_files=locate,
            device_runtime_configured=lambda: False,
        )
    )

    result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 fileId 확인")
    )

    assert result is not None
    assert result.outcome == "answered"
    assert locate.call_count == 1
    assert locate.call_args.kwargs["probe_remote_files"] is False


def test_download_uses_one_device_no_retry_and_private_links_only() -> None:
    long_url = (
        "https://download.example/a.motion.mp4?X-Amz-Security-Token="
        + "a" * 3_500
    )
    payload = {
        "request": {"usedExpandedScope": False},
        "records": [
            {
                "sessions": [
                    {
                        "download": {
                            "downloads": [
                                {
                                    "ok": True,
                                    "fileName": "a.motion.mp4",
                                    "url": long_url,
                                }
                            ]
                        }
                    }
                ]
            }
        ],
    }
    locate = Mock(
        return_value=(
            f"*장비 영상 다운로드 결과*\n• 파일: <{long_url}|a.motion.mp4>",
            payload,
        )
    )
    recordings_loader = Mock(return_value={"rows": []})
    contexts_loader = Mock(return_value=[{"deviceName": _DEVICE}])
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            locate_files=locate,
            recordings_loader=recordings_loader,
            device_contexts_loader=contexts_loader,
        )
    )

    result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 영상 다운로드")
    )

    assert result is not None
    assert result.route == "device_file_download"
    assert result.outcome == "answered"
    assert len(result.messages) == 2
    public_message, requester_message = result.messages
    assert public_message.delivery_scope == "conversation"
    assert long_url not in public_message.body
    assert requester_message.delivery_scope == "requester"
    assert long_url not in requester_message.body
    assert requester_message.private_links[0].uri == long_url
    recordings_loader.assert_called_once_with(_BARCODE)
    contexts_loader.assert_called_once()
    locate.assert_called_once()
    assert locate.call_args.kwargs["recordings_context"] is None
    assert locate.call_args.kwargs["device_contexts"] == [
        {"deviceName": _DEVICE}
    ]
    assert locate.call_args.kwargs["retry_remote_probe"] is False
    assert locate.call_args.kwargs["resend_ssh_open"] is False


def test_ambiguous_device_scope_stops_before_probe_or_mutation() -> None:
    locate = Mock()
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            locate_files=locate,
            device_contexts_loader=Mock(
                return_value=[
                    {"deviceName": _DEVICE},
                    {"deviceName": "MB2-C00999"},
                ]
            ),
        )
    )

    result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 영상 복구")
    )

    assert result is not None
    assert result.outcome == "needs_input"
    assert result.fallback_reason == "device_scope_required"
    locate.assert_not_called()


def test_mutation_questions_stop_before_log_dispatch_or_file_recovery() -> None:
    check_log = Mock()
    locate = Mock()
    route = DeviceFileOperationsAssistantRoute(
        _deps(check_log_upload=check_log, locate_files=locate)
    )

    log_result = route.handle(
        _request(f"{_DEVICE} 로그 업로드 가능한지 확인해줘")
    )
    recovery_result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 영상 복구 가능한지")
    )
    lookup_result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 장비 파일 있나")
    )
    download_result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 영상 다운로드 가능해?")
    )

    assert log_result is not None
    assert recovery_result is not None
    assert lookup_result is not None
    assert download_result is not None
    assert log_result.outcome == "needs_input"
    assert recovery_result.outcome == "needs_input"
    assert lookup_result.outcome == "needs_input"
    assert download_result.outcome == "needs_input"
    check_log.assert_not_called()
    locate.assert_not_called()


def test_domain_exception_does_not_expose_raw_detail() -> None:
    locate = Mock(side_effect=RuntimeError("secret-presigned-url"))
    route = DeviceFileOperationsAssistantRoute(
        _deps(locate_files=locate)
    )

    result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 영상 다운로드")
    )

    assert result is not None
    assert result.outcome == "failed"
    assert "secret-presigned-url" not in result.messages[0].body
