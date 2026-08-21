from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
from typing import Callable
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest

from boxer_company import device_notification_cycle as cycle
from boxer_company.automation import (
    AutomationCycleContractError,
    AutomationCycleRequest,
    AutomationDeliveryReceipt,
    build_default_automation_cycle_service,
)
from boxer_company.device_notification_cycle import (
    DeviceNotificationAlertCycleHandler,
    DeviceNotificationCycleDeps,
)
from boxer_company.device_health_state_bundle import (
    build_device_notification_api_cursor,
)


_KST = ZoneInfo("Asia/Seoul")
_NOW = datetime(2026, 8, 14, 10, 0, tzinfo=_KST)


def _request(
    *,
    cursor: dict | None = None,
    scheduled_at: datetime = _NOW,
    options: dict | None = None,
) -> AutomationCycleRequest:
    return AutomationCycleRequest(
        request_id="notification:cycle:1",
        tenant_id="lifex",
        cycle="device_notification_alert",
        scheduled_at=scheduled_at,
        cursor=cursor or {},
        options=options or {},
    )


def _captureboard_event(notification_id: int = 12) -> dict:
    return {
        "notificationId": notification_id,
        "deviceSeq": 992,
        "deviceName": "MB2-C00992",
        "deviceVersion": "2.11.308",
        "code": "captureboard_connection_error",
        "details": {"voiceType": "n"},
        "occurredAt": "2026-08-14T00:59:00+00:00",
        "hospitalSeq": 69,
        "hospitalName": "뉴서울여성의원(인천)",
        "hospitalTelephone": "032-123-4567",
        "hospitalDeviceAlertPhone": "010-1234-5678",
        "hospitalRoomSeq": 1,
        "roomName": "1진료실",
    }


def _recording_stall_event(
    notification_id: int,
    *,
    duration_seconds: int,
    occurred_at: str,
) -> dict:
    return {
        **_captureboard_event(notification_id),
        "code": "recording_critically_stalled",
        "fileId": "recording-file",
        "barcode": "81000000000",
        "occurredAt": occurred_at,
        "details": {
            "voiceType": "s",
            "currentSize": 1000,
            "growthRate": 0,
            "durationSeconds": duration_seconds,
            "currentStatus": "recording",
        },
    }


def _deps(
    *,
    latest_id: int = 0,
    next_result: tuple[int, dict | None] = (0, None),
    sheet_incidents: dict | None = None,
    sheet_rows: int | None = None,
    sms_result: dict | None = None,
    clock: Callable[[], datetime] | None = None,
) -> tuple[DeviceNotificationCycleDeps, dict[str, Mock]]:
    mocks = {
        "latest": Mock(return_value=latest_id),
        "next": Mock(return_value=next_result),
        "load_sheet": Mock(return_value=sheet_incidents),
        "append_sheet": Mock(return_value=sheet_rows),
        "send_sms": Mock(
            return_value=sms_result
            or {
                "status": "sent",
                "ok": True,
                "provider": "provider-private-marker",
                "groupId": "group-private-marker",
                "messageId": "message-private-marker",
                "providerStatusCode": "2000",
                "smsDeliveryStatus": "accepted",
            }
        ),
        "claim_sms": Mock(return_value=True),
        "remember_sms": Mock(return_value=True),
    }
    return (
        DeviceNotificationCycleDeps(
            load_latest_id=mocks["latest"],
            load_next_event=mocks["next"],
            load_sheet_incidents=mocks["load_sheet"],
            append_sheet_alerts=mocks["append_sheet"],
            send_sms=mocks["send_sms"],
            claim_sms_delivery=mocks["claim_sms"],
            hold_sms_delivery_claim=Mock(return_value=True),
            clock=clock or (lambda: _NOW),
            remember_sms_delivery=mocks["remember_sms"],
        ),
        mocks,
    )


def _initialized_cursor(last_seen_id: int = 11) -> dict:
    return {
        "initialized": True,
        "lastSeenId": last_seen_id,
        "pendingDeliveryContexts": {},
    }


def test_first_cycle_initializes_at_latest_id_without_replaying_history() -> None:
    deps, mocks = _deps(latest_id=1200)
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(_request())

    assert result.outcome == "no_change"
    assert result.cursor["initialized"] is True
    assert result.cursor["lastSeenId"] == 1200
    assert result.cursor["cycleCompleted"] is False
    assert result.deliveries == ()
    mocks["latest"].assert_called_once_with()
    mocks["next"].assert_not_called()
    mocks["send_sms"].assert_not_called()


def test_production_cycle_loads_one_fixed_legacy_batch_then_drains_receipts(
) -> None:
    first_event = _captureboard_event(12)
    second_event = {
        **_captureboard_event(14),
        "code": "segmented_recordings_merge_error",
        "message": "녹화 병합 실패",
        "details": {"voiceType": "n", "segmentCount": 2},
    }
    load_batch = Mock(return_value=(14, [first_event, second_event]))
    base_deps, mocks = _deps(sheet_incidents={}, sheet_rows=1)
    deps = DeviceNotificationCycleDeps(
        load_latest_id=base_deps.load_latest_id,
        # default function identity가 실제 API의 batch 경로를 선택한다.
        load_next_event=cycle._load_next_device_notification,
        load_event_batch=load_batch,
        load_sheet_incidents=base_deps.load_sheet_incidents,
        append_sheet_alerts=base_deps.append_sheet_alerts,
        send_sms=base_deps.send_sms,
        claim_sms_delivery=base_deps.claim_sms_delivery,
        hold_sms_delivery_claim=base_deps.hold_sms_delivery_claim,
        clock=base_deps.clock,
        remember_sms_delivery=base_deps.remember_sms_delivery,
    )
    handler = DeviceNotificationAlertCycleHandler(deps)

    first = handler.run(_request(cursor=_initialized_cursor()))
    assert first.cursor["lastSeenId"] == 14
    assert [
        item["notificationId"] for item in first.cursor["pendingEvents"]
    ] == [12, 14]
    first_delivery = first.deliveries[0]
    acknowledged = handler.acknowledge(
        _request(cursor=dict(first.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=first_delivery.delivery_id,
                status="sent",
                external_message_id="1710000000.001",
                delivered_at=_NOW,
            ),
        ),
    )

    second = handler.run(_request(cursor=dict(acknowledged)))
    assert second.deliveries[0].delivery_id == "device_notification:14"
    assert load_batch.call_count == 1
    mocks["next"].assert_not_called()


def test_migrated_cursor_processes_cutover_gap_instead_of_skipping_to_latest() -> None:
    observed_at = _NOW.isoformat()
    migrated_cursor = build_device_notification_api_cursor(
        {
            "initialized": True,
            "initializedAt": observed_at,
            "lastSeenId": 11,
            "lastPolledAt": observed_at,
            "pendingEvents": [],
            "recentCaptureboardAlerts": {},
            "recordingStallIncidents": {},
            "captureboardIncidents": {},
            "captureboardIncidentsLastSheetCheckedAt": "",
            "lastSentAt": "",
            "lastSentNotificationId": 0,
            "lastSlackMessageTs": "",
            "lastSlackPermalink": "",
        }
    )
    deps, mocks = _deps(
        latest_id=1200,
        next_result=(12, _captureboard_event()),
        sheet_incidents={},
    )
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(_request(cursor=migrated_cursor))

    assert result.cursor["lastSeenId"] == 12
    assert len(result.deliveries) == 1
    mocks["latest"].assert_not_called()
    mocks["next"].assert_called_once_with(11)


def test_migrated_recording_incident_continues_in_original_thread() -> None:
    first_occurred_at = "2026-08-14T00:55:00+00:00"
    event = _recording_stall_event(
        12,
        duration_seconds=240,
        occurred_at="2026-08-14T00:59:00+00:00",
    )
    legacy_key = "MB2-C00992|recording-file|81000000000|recording"
    cursor = build_device_notification_api_cursor(
        {
            "initialized": True,
            "initializedAt": first_occurred_at,
            "lastSeenId": 11,
            "lastPolledAt": first_occurred_at,
            "pendingEvents": [],
            "recentCaptureboardAlerts": {},
            "recordingStallIncidents": {
                legacy_key: {
                    "phase": "alerted",
                    "deviceName": "MB2-C00992",
                    "barcode": "81000000000",
                    "fileId": "recording-file",
                    "fileType": "recording",
                    "currentStatus": "recording",
                    "firstNotificationId": 11,
                    "firstOccurredAt": first_occurred_at,
                    "firstDurationSeconds": 120,
                    "lastNotificationId": 11,
                    "lastOccurredAt": first_occurred_at,
                    "lastDurationSeconds": 120,
                    "lastCurrentSize": 1000,
                    "slackMessageTs": "1710000000.000100",
                    "slackPermalink": "https://example.slack.com/archives/C1/p1",
                    "lastCommentNotificationId": None,
                }
            },
            "captureboardIncidents": {},
            "captureboardIncidentsLastSheetCheckedAt": "",
            "lastSentAt": first_occurred_at,
            "lastSentNotificationId": 11,
            "lastSlackMessageTs": "1710000000.000100",
            "lastSlackPermalink": "https://example.slack.com/archives/C1/p1",
        }
    )
    deps, mocks = _deps(next_result=(12, event), sheet_incidents={})
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(_request(cursor=cursor))

    assert len(result.deliveries) == 1
    assert result.deliveries[0].kind == "device_notification_thread_reply"
    assert result.deliveries[0].payload["replyToExternalMessageId"] == (
        "1710000000.000100"
    )
    mocks["next"].assert_called_once_with(11)


def test_captureboard_cycle_preserves_contact_and_hides_sms_identifiers() -> None:
    event = _captureboard_event()
    deps, mocks = _deps(
        next_result=(12, event),
        sheet_incidents={},
        sheet_rows=1,
    )
    handler = DeviceNotificationAlertCycleHandler(
        deps,
        logger=logging.getLogger("test.device_notification_cycle"),
    )

    result = handler.run(_request(cursor=_initialized_cursor()))

    assert result.outcome == "completed"
    assert len(result.deliveries) == 1
    delivery = result.deliveries[0]
    assert delivery.kind == "device_notification_alert"
    assert delivery.payload["alertSummary"]["deviceResults"][0][
        "alertCategory"
    ] == "video_signal"
    assert delivery.payload["smsReceipt"] == {
        "attempted": True,
        "status": "sent",
        "ok": True,
        "statusText": "문자 발송 접수",
        "contactActionEnabled": False,
        "deliveryStatus": "accepted",
        "templateId": "captureboard_disconnected",
    }
    serialized = json.dumps(delivery.payload, ensure_ascii=False)
    # 기존 Slack 카드와 자동발송 확인 버튼의 번호·본문은 유지하되
    # provider 추적 ID는 conversation payload에 포함하지 않는다.
    assert "010-1234-5678" in serialized
    assert "032-123-4567" in serialized
    device_result = delivery.payload["alertSummary"]["deviceResults"][0]
    assert device_result["smsPhoneNumber"] == "01012345678"
    assert "초음파 진단기와 캡처보드" in device_result["smsMessage"]
    assert device_result["smsTemplateId"]
    assert "provider-private-marker" not in serialized
    assert "group-private-marker" not in serialized
    assert "message-private-marker" not in serialized
    mocks["send_sms"].assert_called_once()
    # 실제 provider 호출 내부에서만 번호와 고정 본문을 사용한다.
    sms_payload = mocks["send_sms"].call_args.args[0]
    assert sms_payload["sms"]["to"] == "01012345678"
    assert "초음파 진단기와 캡처보드" in sms_payload["sms"]["message"]
    assert mocks["append_sheet"].call_count == 0


def test_notification_delivery_keeps_legacy_event_message_and_merge_error() -> None:
    captureboard = _captureboard_event(13)
    captureboard["message"] = "현장 캡처보드 장애 메시지"
    captureboard_deps, _ = _deps(
        next_result=(13, captureboard),
        sheet_incidents={},
    )
    captureboard_result = DeviceNotificationAlertCycleHandler(
        captureboard_deps
    ).run(_request(cursor=_initialized_cursor(last_seen_id=12)))
    captureboard_issue = captureboard_result.deliveries[0].payload[
        "alertSummary"
    ]["deviceResults"][0]["priorityReason"]
    assert "현장 캡처보드 장애 메시지" in captureboard_issue

    merge_event = {
        **_captureboard_event(14),
        "code": "segmented_recordings_merge_error",
        "message": "녹화 병합 실패",
        "details": {
            "voiceType": "n",
            "segmentCount": 3,
            "error": "ffmpeg exit 17",
        },
    }
    merge_deps, _ = _deps(
        next_result=(14, merge_event),
        sheet_incidents={},
    )
    merge_result = DeviceNotificationAlertCycleHandler(merge_deps).run(
        _request(cursor=_initialized_cursor(last_seen_id=13))
    )
    merge_issue = merge_result.deliveries[0].payload["alertSummary"][
        "deviceResults"
    ][0]["priorityReason"]
    assert "녹화 병합 실패" in merge_issue
    assert "분할 파일 3개" in merge_issue
    assert "ffmpeg exit 17" in merge_issue


def test_sms_claim_uses_provider_immediate_server_clock() -> None:
    provider_now = _NOW + timedelta(minutes=5)
    deps, mocks = _deps(
        next_result=(12, _captureboard_event()),
        sheet_incidents={},
        clock=lambda: provider_now,
    )
    handler = DeviceNotificationAlertCycleHandler(deps)

    handler.run(
        _request(
            cursor=_initialized_cursor(),
            scheduled_at=_NOW - timedelta(minutes=5),
        )
    )

    assert mocks["claim_sms"].call_args.kwargs["claimed_at"] == provider_now


def test_sent_receipt_appends_sheet_then_closes_delivery_context() -> None:
    deps, mocks = _deps(
        next_result=(12, _captureboard_event()),
        sheet_incidents={},
        sheet_rows=1,
    )
    handler = DeviceNotificationAlertCycleHandler(deps)
    result = handler.run(_request(cursor=_initialized_cursor()))
    delivery_id = result.deliveries[0].delivery_id

    failed_cursor = handler.acknowledge(
        _request(cursor=dict(result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery_id,
                status="failed",
            ),
        ),
    )
    assert delivery_id in failed_cursor["pendingDeliveryContexts"]
    mocks["append_sheet"].assert_not_called()

    acknowledged = handler.acknowledge(
        _request(cursor=dict(failed_cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery_id,
                status="sent",
                external_message_id="1710000000.001",
                permalink="https://lifexio.slack.com/archives/C1/p1",
                delivered_at=_NOW + timedelta(seconds=2),
            ),
        ),
    )

    assert delivery_id not in acknowledged["pendingDeliveryContexts"]
    assert acknowledged["lastSentNotificationId"] == 12
    assert acknowledged["recentCaptureboardAlerts"]["MB2-C00992"][
        "notificationId"
    ] == 12
    assert acknowledged["captureboardIncidents"]["MB2-C00992"][
        "status"
    ] == "대기"
    mocks["append_sheet"].assert_called_once()
    sheet_item = mocks["append_sheet"].call_args.args[0][0]
    assert sheet_item["smsGroupId"] == "group-private-marker"
    assert sheet_item["smsMessageId"] == "message-private-marker"
    # legacy pending 이벤트와 Sheet 호출도 자동발송 확인값을 유지했다.
    assert sheet_item["smsPhoneNumber"] == "01012345678"
    assert sheet_item["smsMessage"]
    assert mocks["append_sheet"].call_args.kwargs["slack_permalink"] == (
        "https://lifexio.slack.com/archives/C1/p1"
    )


def test_recording_stall_followup_becomes_channel_neutral_thread_delivery() -> None:
    first_event = _recording_stall_event(
        31,
        duration_seconds=120,
        occurred_at="2026-08-14T01:00:00+00:00",
    )
    first_deps, first_mocks = _deps(
        next_result=(31, first_event),
        sheet_incidents={},
    )
    handler = DeviceNotificationAlertCycleHandler(first_deps)
    first_result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=30))
    )
    first_cursor = handler.acknowledge(
        _request(cursor=dict(first_result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=first_result.deliveries[0].delivery_id,
                status="sent",
                external_message_id="1710000000.031",
                permalink="https://lifexio.slack.com/archives/C1/p31",
            ),
        ),
    )

    second_event = _recording_stall_event(
        32,
        duration_seconds=180,
        occurred_at="2026-08-14T01:01:00+00:00",
    )
    second_deps, second_mocks = _deps(
        next_result=(32, second_event),
        sheet_incidents={},
    )
    second_handler = DeviceNotificationAlertCycleHandler(second_deps)
    second_result = second_handler.run(
        _request(
            cursor=dict(first_cursor),
            scheduled_at=_NOW + timedelta(minutes=1),
        )
    )

    assert second_result.deliveries[0].kind == (
        "device_notification_thread_reply"
    )
    assert second_result.deliveries[0].payload[
        "replyToExternalMessageId"
    ] == "1710000000.031"
    assert second_result.deliveries[0].payload["durationText"] == (
        "180초 (3분)"
    )
    first_mocks["send_sms"].assert_called_once()
    second_mocks["send_sms"].assert_not_called()


def test_open_sheet_incident_suppresses_duplicate_without_sms() -> None:
    event = _captureboard_event(40)
    deps, mocks = _deps(
        next_result=(40, event),
        sheet_incidents={
            "MB2-C00992": {
                "deviceName": "MB2-C00992",
                "status": "처리중",
                "slackPermalink": "https://lifexio.slack.com/archives/C1/p1",
                "rowNumber": 3,
            }
        },
    )
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=39))
    )

    assert result.outcome == "no_change"
    assert result.deliveries == ()
    assert result.cursor["captureboardIncidents"]["MB2-C00992"][
        "suppressedCount"
    ] == 1
    mocks["send_sms"].assert_not_called()


def test_provider_exception_is_not_retried_and_is_marked_uncertain() -> None:
    deps, mocks = _deps(
        next_result=(50, _captureboard_event(50)),
        sheet_incidents={},
    )
    mocks["send_sms"].side_effect = TimeoutError("private provider detail")
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=49))
    )

    assert mocks["send_sms"].call_count == 1
    receipt = result.deliveries[0].payload["smsReceipt"]
    assert receipt["deliveryStatus"] == "confirm_required"
    assert receipt["contactActionEnabled"] is False
    assert "private provider detail" not in repr(result)


def test_confirm_required_receipt_with_group_is_persisted_immediately() -> None:
    deps, mocks = _deps(
        next_result=(53, _captureboard_event(53)),
        sheet_incidents={},
        sms_result={
            "status": "error",
            "ok": False,
            "provider": "solapi",
            "groupId": "group-confirm-required",
            "messageId": "message-confirm-required",
            "smsDeliveryStatus": "confirm_required",
        },
    )
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=52))
    )

    mocks["send_sms"].assert_called_once()
    mocks["remember_sms"].assert_called_once()
    remembered_item = mocks["remember_sms"].call_args.args[0]
    assert remembered_item["smsDeliveryStatus"] == "confirm_required"
    assert remembered_item["smsGroupId"] == "group-confirm-required"
    assert result.deliveries[0].payload["smsReceipt"][
        "contactActionEnabled"
    ] is False
    serialized = json.dumps(result.deliveries[0].payload, ensure_ascii=False)
    assert "group-confirm-required" not in serialized
    assert "message-confirm-required" not in serialized


def test_receipt_persist_failure_is_confirm_safe_without_provider_retry() -> None:
    deps, mocks = _deps(
        next_result=(51, _captureboard_event(51)),
        sheet_incidents={},
    )
    mocks["remember_sms"].side_effect = OSError("private outbox failure")
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=50))
    )

    assert mocks["send_sms"].call_count == 1
    assert mocks["remember_sms"].call_count == 1
    receipt = result.deliveries[0].payload["smsReceipt"]
    assert receipt["status"] == "receipt_persist_failed"
    assert receipt["deliveryStatus"] == "confirm_required"
    assert receipt["contactActionEnabled"] is False
    assert "group-private-marker" not in json.dumps(
        result.deliveries[0].payload,
        ensure_ascii=False,
    )
    with pytest.raises(
        AutomationCycleContractError,
        match="receipt is still pending",
    ):
        handler.run(_request(cursor=dict(result.cursor)))
    assert mocks["send_sms"].call_count == 1


def test_sheet_append_failure_moves_to_non_blocking_outbox_repair_marker() -> None:
    deps, mocks = _deps(
        next_result=(52, _captureboard_event(52)),
        sheet_incidents={},
    )
    mocks["append_sheet"].side_effect = TimeoutError("private sheet failure")
    handler = DeviceNotificationAlertCycleHandler(deps)
    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=51))
    )
    delivery_id = result.deliveries[0].delivery_id

    acknowledged = handler.acknowledge(
        _request(cursor=dict(result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery_id,
                status="sent",
                external_message_id="1710000000.052",
                permalink="https://lifexio.slack.com/archives/C1/p52",
            ),
        ),
    )

    # Slack 성공 context는 delivery 재발송 queue에서는 닫되, outbox repair
    # 표식을 남겨 Sheet transient를 관찰 가능하게 한다.
    assert delivery_id not in acknowledged["pendingDeliveryContexts"]
    assert acknowledged["pendingSheetRepairs"][delivery_id]["status"] == (
        "outbox_pending"
    )
    assert acknowledged["lastSheetWriteStatus"] == "repair_pending"
    assert mocks["remember_sms"].call_count == 2

    mocks["next"].return_value = (52, None)
    next_result = handler.run(
        _request(
            cursor=dict(acknowledged),
            scheduled_at=_NOW + timedelta(minutes=1),
        )
    )
    assert next_result.outcome == "no_change"
    assert next_result.cursor["pendingSheetRepairs"] == {}
    assert next_result.cursor["lastSheetWriteStatus"] == "repair_queued"
    assert mocks["send_sms"].call_count == 1


@pytest.mark.parametrize(
    ("repair_result", "expected_status"),
    ((1, "repair_completed"), (None, "disabled")),
)
def test_sheet_append_failure_without_group_retries_directly_from_safe_cursor(
    repair_result: int | None,
    expected_status: str,
) -> None:
    event = _captureboard_event(54)
    event["hospitalDeviceAlertPhone"] = ""
    deps, mocks = _deps(
        next_result=(54, event),
        sheet_incidents={},
    )
    # 최초 ack만 실패하고 다음 poll의 direct repair는 성공 또는 disabled로
    # 확정되어 같은 cursor 항목을 영구 보존하지 않아야 한다.
    mocks["append_sheet"].side_effect = [
        TimeoutError("private sheet failure"),
        TimeoutError("private sheet retry failure"),
        repair_result,
    ]
    handler = DeviceNotificationAlertCycleHandler(deps)
    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=53))
    )
    delivery_id = result.deliveries[0].delivery_id

    acknowledged = handler.acknowledge(
        _request(cursor=dict(result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery_id,
                status="sent",
                external_message_id="1710000000.054",
                permalink="https://lifexio.slack.com/archives/C1/p54",
            ),
        ),
    )

    repair = acknowledged["pendingSheetRepairs"][delivery_id]
    assert repair["status"] == "sheet_pending"
    assert repair["item"]["smsGroupId"] == ""
    assert repair["item"]["hospitalName"] == "뉴서울여성의원(인천)"
    serialized_repair = json.dumps(repair, ensure_ascii=False)
    assert "010-1234-5678" not in serialized_repair
    assert "초음파 진단기와 캡처보드" not in serialized_repair
    mocks["remember_sms"].assert_not_called()

    mocks["next"].return_value = (54, None)
    still_pending = handler.run(
        _request(
            cursor=dict(acknowledged),
            scheduled_at=_NOW + timedelta(minutes=1),
        )
    )
    assert delivery_id in still_pending.cursor["pendingSheetRepairs"]
    assert still_pending.cursor["lastSheetWriteStatus"] == "repair_pending"

    next_result = handler.run(
        _request(
            cursor=dict(still_pending.cursor),
            scheduled_at=_NOW + timedelta(minutes=2),
        )
    )

    assert next_result.cursor["pendingSheetRepairs"] == {}
    assert next_result.cursor["lastSheetWriteStatus"] == expected_status
    assert mocks["append_sheet"].call_count == 3
    repaired_item = mocks["append_sheet"].call_args.args[0][0]
    assert repaired_item["hospitalName"] == "뉴서울여성의원(인천)"
    mocks["remember_sms"].assert_not_called()


def test_sheet_pending_reconciles_committed_timeout_without_duplicate_append() -> None:
    event = _captureboard_event(55)
    event["hospitalDeviceAlertPhone"] = ""
    deps, mocks = _deps(
        next_result=(55, event),
        sheet_incidents={},
    )
    committed_delivery_ids: set[str] = set()
    physical_append_count = 0

    def append_after_commit_then_timeout(
        items: list[dict],
        *,
        detected_at: datetime,
        slack_permalink: str,
    ) -> int:
        nonlocal physical_append_count
        del detected_at, slack_permalink
        delivery_id = str(items[0].get("sheetDeliveryId") or "")
        if delivery_id in committed_delivery_ids:
            # 실제 Sheet helper가 T열 hash를 찾은 재시도는 쓰지 않고 성공 처리한다.
            return 0
        committed_delivery_ids.add(delivery_id)
        physical_append_count += 1
        raise TimeoutError("Sheet append committed before response timeout")

    mocks["append_sheet"].side_effect = append_after_commit_then_timeout
    handler = DeviceNotificationAlertCycleHandler(deps)
    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=54))
    )
    delivery_id = result.deliveries[0].delivery_id
    acknowledged = handler.acknowledge(
        _request(cursor=dict(result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery_id,
                status="sent",
                external_message_id="1710000000.055",
                permalink="https://lifexio.slack.com/archives/C1/p55",
            ),
        ),
    )

    repair = acknowledged["pendingSheetRepairs"][delivery_id]
    assert repair["status"] == "sheet_pending"
    assert repair["item"]["sheetDeliveryId"] == delivery_id

    mocks["next"].return_value = (55, None)
    reconciled = handler.run(
        _request(
            cursor=dict(acknowledged),
            scheduled_at=_NOW + timedelta(minutes=1),
        )
    )

    assert reconciled.cursor["pendingSheetRepairs"] == {}
    assert reconciled.cursor["lastSheetWriteStatus"] == "repair_completed"
    assert reconciled.deliveries == ()
    assert committed_delivery_ids == {delivery_id}
    assert physical_append_count == 1
    assert mocks["append_sheet"].call_count == 2
    # group 없는 경로는 provider/SMS 호출 없이 Sheet만 reconcile한다.
    mocks["send_sms"].assert_not_called()
    mocks["remember_sms"].assert_not_called()


def test_old_direct_sheet_repair_is_not_expired_before_success() -> None:
    deps, mocks = _deps(next_result=(54, None), sheet_rows=1)
    handler = DeviceNotificationAlertCycleHandler(deps)
    cursor = {
        **_initialized_cursor(last_seen_id=54),
        "pendingSheetRepairs": {
            "device_notification:54": {
                "queuedAt": (_NOW - timedelta(days=10)).isoformat(),
                "detectedAt": (_NOW - timedelta(days=10)).isoformat(),
                "permalink": "https://lifexio.slack.com/archives/C1/p54",
                "item": {
                    "device": "MB2-C00992",
                    "hospitalName": "뉴서울여성의원(인천)",
                    "room": "1진료실",
                    "problemComponents": ["캡처보드"],
                    "issue": "캡처보드 연결 확인 필요",
                    "smsDeliveryStatus": "not_sent",
                    "smsGroupId": "",
                },
                "status": "sheet_pending",
            }
        },
        "lastSheetWriteStatus": "repair_pending",
    }

    result = handler.run(_request(cursor=cursor))

    assert result.cursor["pendingSheetRepairs"] == {}
    assert result.cursor["lastSheetWriteStatus"] == "repair_completed"
    mocks["append_sheet"].assert_called_once()
    mocks["remember_sms"].assert_not_called()


def test_cycle_rejects_options_before_any_external_call() -> None:
    deps, mocks = _deps(next_result=(12, _captureboard_event()))
    handler = DeviceNotificationAlertCycleHandler(deps)

    with pytest.raises(AutomationCycleContractError):
        handler.run(
            _request(
                cursor=_initialized_cursor(),
                options={"autoSms": True},
            )
        )

    mocks["next"].assert_not_called()
    mocks["send_sms"].assert_not_called()


def test_default_service_registers_notification_cycle() -> None:
    service = build_default_automation_cycle_service()

    assert "device_notification_alert" in service.cycle_names
