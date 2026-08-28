from __future__ import annotations

from dataclasses import replace
import json
from unittest.mock import Mock

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_file_operations_route import (
    DeviceFileOperationsAssistantRoute,
    DeviceFileOperationsRouteDeps,
    build_trusted_mda_recovery_scope_metadata,
    needs_device_file_operation_context,
)
from boxer_company.operation_routing import match_device_file_operation_route
from boxer_company_api.schemas import (
    DeviceFileDownloadDeliveryActionInput,
)


_BARCODE = "48194663047"
_DEVICE = "MB2-C00419"


def _request(
    question: str,
    *,
    route_group: str | None = "operations",
    metadata: dict[str, object] | None = None,
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
        recordings_loader=Mock(
            return_value={
                "summary": {"recordingCount": 1},
                "rows": [{"deviceSeq": None}],
            }
        ),
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
        create_activity_log=Mock(return_value={"status": True}),
        s3_query_enabled=lambda: True,
        db_configured=lambda: True,
        mda_configured=lambda: True,
        device_runtime_configured=lambda: True,
        download_configured=lambda: True,
        recovery_enabled=lambda: True,
        recovery_configured=lambda: True,
    )
    return replace(base, **changes)


def test_matcher_uses_first_explicit_scope_and_preserves_month_restore() -> None:
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
    # legacy parser는 본문 target을 metadata보다 우선하고 복수 표현의
    # 첫 장비·첫 바코드로 route를 확정한다.
    assert (
        match_device_file_operation_route(
            _request("로그 업로드 확인", metadata={"device_name": _DEVICE})
        )
        is None
    )
    assert (
        match_device_file_operation_route(
            _request(f"{_DEVICE} MB2-C00999 로그 업로드 확인")
        )
        == "device_log_upload"
    )
    assert (
        match_device_file_operation_route(
            _request(
                f"{_BARCODE} 12345678910 2026-03-06 영상 다운로드"
            )
        )
        == "device_file_download"
    )
    assert (
        match_device_file_operation_route(
            _request(
                f"{_BARCODE} 2026-03-06 영상 다운로드",
                route_group="device",
            )
        )
        is None
    )


def test_matcher_uses_latest_thread_scope_regardless_of_actor() -> None:
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
    # Slack 로컬은 thread 전체의 최신 범위를 사용했다.
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
        == "device_file_download"
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
        == "device_file_download"
    )


def test_missing_barcode_download_keeps_legacy_guard_before_voice_change() -> None:
    question = f"{_DEVICE} 음성을 지니로 바꾸고 장비 파일 다운로드해줘"
    route = DeviceFileOperationsAssistantRoute(_deps())

    assert match_device_file_operation_route(_request(question)) == (
        "device_file_download_barcode_required"
    )
    result = route.handle(_request(question))

    assert result is not None
    assert result.route == "device_file_download_barcode_required"
    assert result.outcome == "needs_input"
    assert result.fallback_reason == "barcode_scope_required"
    assert result.messages[0].body == (
        "영상 다운로드는 바코드 없이는 특정할 수 없어.\n"
        "11자리 바코드랑 날짜를 같이 보내줘. "
        "예: `12345678910 2026-04-28 영상 다운로드`"
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


def test_file_execution_uses_first_explicit_barcode_and_device() -> None:
    # Slack parser가 앞에서 찾은 대상만 file domain helper에 전달한다.
    recordings_loader = Mock(
        return_value={
            "summary": {"recordingCount": 1},
            "rows": [{"deviceSeq": 1}],
        }
    )
    locate = Mock(return_value=("*파일 조회 결과*", {"records": []}))
    route = DeviceFileOperationsAssistantRoute(
        _deps(recordings_loader=recordings_loader, locate_files=locate)
    )

    result = route.handle(
        _request(
            f"{_BARCODE} 12345678910 2026-03-06 "
            f"{_DEVICE} MB2-C00999 영상 복구"
        )
    )

    assert result is not None
    assert result.outcome == "answered"
    recordings_loader.assert_called_once_with(_BARCODE)
    locate.assert_called_once()
    assert locate.call_args.args[1] == _BARCODE
    assert locate.call_args.kwargs["device_contexts"] == [
        {
            "deviceName": _DEVICE,
            "hospitalName": None,
            "roomName": None,
        }
    ]


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


def test_download_uses_single_api_probe_with_private_links() -> None:
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
    recordings_context = {
        "summary": {"recordingCount": 1},
        "rows": [{"deviceSeq": None}],
    }
    recordings_loader = Mock(return_value=recordings_context)
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
    assert len(result.messages) == 1
    requester_message = result.messages[0]
    assert requester_message.delivery_scope == "requester"
    assert long_url not in requester_message.body
    assert requester_message.private_links[0].uri == long_url
    assert result.operation_result is not None
    assert result.operation_result["kind"] == (
        "device_file_download_delivery"
    )
    assert result.operation_result["status"] == "pending"
    assert result.operation_result["failureNotice"] == (
            "**장비 영상 다운로드 결과**\n"
            f"• 바코드: `{_BARCODE}`\n"
            "• 날짜: `2026-03-06`\n\n"
            "• 장비: `미확인`\n"
            "• 병원: `미확인`\n"
            "• 병실: `미확인`\n"
            "• 장비에 존재하는 영상 목록: `0개`\n"
            "• 다운로드 링크: DM 전송 실패. 봇 DM 권한을 확인해줘"
    )
    assert result.operation_result["linkCount"] == 1
    assert result.operation_result["links"] == [
        {"deviceName": "미확인", "fileName": "a.motion.mp4"}
    ]
    assert result.operation_result["delivery"] == {
        "barcode": _BARCODE,
        "logDate": "2026-03-06",
        "usedExpandedScope": False,
        "records": [
            {
                "deviceName": "미확인",
                "deviceSeq": None,
                "hospitalSeq": None,
                "hospitalRoomSeq": None,
                "hospitalName": "미확인",
                "roomName": "미확인",
                "fileNames": [],
                "downloadFileNames": ["a.motion.mp4"],
            }
        ],
    }
    recordings_loader.assert_called_once_with(_BARCODE)
    contexts_loader.assert_called_once()
    locate.assert_called_once()
    assert locate.call_args.kwargs["recordings_context"] is recordings_context
    assert locate.call_args.kwargs["device_contexts"] == [
        {"deviceName": _DEVICE}
    ]
    assert locate.call_args.kwargs["retry_remote_probe"] is False
    assert locate.call_args.kwargs["resend_ssh_open"] is False


def test_download_logs_mda_activity_with_request_scope() -> None:
    link = "https://download.example/a.motion.mp4?token=opaque"
    payload = {
        "records": [
            {
                "deviceName": _DEVICE,
                "deviceSeq": 41,
                "hospitalSeq": 5,
                "hospitalRoomSeq": 8,
                "hospitalName": "테스트병원",
                "roomName": "1진료실",
                "sessions": [
                    {
                        "probe": {
                            "ok": True,
                            "files": ["/AppData/Videos/a.motion.mp4"],
                        },
                        "download": {
                            "downloads": [
                                {
                                    "ok": True,
                                    "fileName": "a.motion.mp4",
                                    "url": link,
                                }
                            ]
                        },
                    }
                ],
            }
        ]
    }
    create_activity = Mock(return_value={"status": True})
    deps = _deps(
        locate_files=Mock(
            return_value=("*장비 영상 다운로드 결과*", payload)
        ),
        create_activity_log=create_activity,
    )
    route = DeviceFileOperationsAssistantRoute(deps)

    original_request = _request(
        f"{_BARCODE} 2026-03-06 영상 다운로드",
        metadata={
            "channel_id": "CHANNEL-1",
            "actor_name": "홍 길동",
        },
    )
    result = route.handle(original_request)

    assert result is not None
    assert result.outcome == "answered"
    assert result.operation_result is not None
    create_activity.assert_not_called()

    action = DeviceFileDownloadDeliveryActionInput.model_validate(
        {
            "name": "device_file_download_delivery",
            "phase": "delivered",
            "delivery": result.operation_result["delivery"],
        }
    )
    # 초기 응답과 receipt 사이 API worker가 재시작돼도 URL 없는 typed
    # manifest만으로 activity와 공개 성공 안내를 복원한다.
    restarted_route = DeviceFileOperationsAssistantRoute(deps)
    delivered = restarted_route.handle(
        replace(
            original_request,
            metadata={
                **original_request.metadata,
                "operation_action": action.to_metadata(),
            },
        )
    )

    assert delivered is not None
    assert "다운로드 내역 기록되었습니다" in delivered.messages[0].body
    create_activity.assert_called_once()
    activity = create_activity.call_args.args[0]
    assert activity["activityType"] == "recording.download"
    assert activity["deviceSeq"] == 41
    detail = json.loads(activity["detailLog"])
    assert detail["slackUserId"] == "ACTOR-1"
    assert detail["slackUserName"] == "홍 길동"
    assert detail["slackChannelId"] == "CHANNEL-1"
    assert detail["slackThreadTs"] == "CONVERSATION-1"
    assert detail["downloadFileNames"] == ["a.motion.mp4"]

    replayed = restarted_route.handle(
        replace(
            original_request,
            metadata={
                **original_request.metadata,
                "operation_action": action.to_metadata(),
            },
        )
    )

    assert replayed == delivered
    create_activity.assert_called_once()


def test_trusted_mda_recovery_scope_restores_zero_recording_target() -> None:
    trusted_metadata = build_trusted_mda_recovery_scope_metadata(
        barcode=_BARCODE,
        log_date="2026-03-06",
        device_context={
            "deviceName": _DEVICE,
            "hospitalName": "테스트병원",
            "roomName": "1진료실",
        },
    )
    locate = Mock(return_value=("*파일 복구 결과*", {"records": []}))
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            recordings_loader=Mock(
                return_value={
                    "summary": {"recordingCount": 0},
                    "rows": [],
                }
            ),
            locate_files=locate,
        )
    )

    result = route.handle(
        _request(
            f"{_BARCODE} 2026-03-06 영상 복구",
            metadata=trusted_metadata,
        )
    )

    assert result is not None
    assert result.outcome == "answered"
    assert locate.call_args.kwargs["device_contexts"] == [
        {
            "deviceName": _DEVICE,
            "hospitalName": "테스트병원",
            "roomName": "1진료실",
        }
    ]


def test_recovery_registers_the_exact_mda_search_link_as_a_source() -> None:
    mda_uri = f"https://mda.kr.mmtalkbox.com/cs?search={_BARCODE}"
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            recordings_loader=Mock(
                return_value={
                    "summary": {"recordingCount": 1},
                    "rows": [{"deviceSeq": 41}],
                }
            ),
            locate_files=Mock(
                return_value=(
                    f"*파일 복구 결과*\n• MDA: <{mda_uri}|열기>",
                    {"records": []},
                )
            ),
        )
    )

    result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 영상 복구")
    )

    assert result is not None
    assert result.outcome == "answered"
    assert f"[열기]({mda_uri})" in result.messages[0].body
    assert len(result.sources) == 1
    assert result.sources[0].uri == mda_uri


def test_trusted_mda_recovery_scope_must_match_turn_barcode_and_date() -> None:
    trusted_metadata = build_trusted_mda_recovery_scope_metadata(
        barcode="12345678910",
        log_date="2026-03-06",
        device_context={
            "deviceName": _DEVICE,
            "hospitalName": "테스트병원",
            "roomName": "1진료실",
        },
    )
    locate = Mock(return_value=("unexpected", {}))
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            recordings_loader=Mock(
                return_value={
                    "summary": {"recordingCount": 0},
                    "rows": [],
                }
            ),
            locate_files=locate,
        )
    )

    result = route.handle(
        _request(
            f"{_BARCODE} 2026-03-06 영상 복구",
            metadata=trusted_metadata,
        )
    )

    assert result is not None
    assert result.outcome == "needs_input"
    assert result.fallback_reason == "device_scope_required"
    locate.assert_not_called()


def test_multiple_mapped_devices_keep_existing_multi_device_search() -> None:
    locate = Mock(return_value=("*파일 조회 결과*", {"records": []}))
    contexts = [
        {"deviceName": _DEVICE},
        {"deviceName": "MB2-C00999"},
    ]
    route = DeviceFileOperationsAssistantRoute(
        _deps(
            locate_files=locate,
            device_contexts_loader=Mock(return_value=contexts),
        )
    )

    result = route.handle(
        _request(f"{_BARCODE} 2026-03-06 영상 복구")
    )

    assert result is not None
    assert result.outcome == "answered"
    locate.assert_called_once()
    assert locate.call_args.kwargs["device_contexts"] == contexts


def test_question_style_requests_keep_existing_execution_behavior() -> None:
    check_log = Mock(return_value=("*업로드 확인*", {}))
    locate = Mock(return_value=("*파일 조회 결과*", {"records": []}))
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
    assert log_result.outcome == "answered"
    assert recovery_result.outcome == "answered"
    assert lookup_result.outcome == "answered"
    assert download_result.outcome == "answered"
    check_log.assert_called_once()
    assert locate.call_count == 3


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
