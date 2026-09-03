from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import os
import subprocess
import sys
from typing import Callable
from unittest.mock import MagicMock, Mock
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


def _video_duration_mismatch_event(
    notification_id: int = 80,
    *,
    occurred_at: str = "2026-08-14T00:59:00+00:00",
) -> dict:
    """단말 원문 식별자가 deferred cursor에서 제거되는 입력을 만든다."""

    return {
        **_captureboard_event(notification_id),
        "code": "video_duration_mismatch",
        "message": "비디오 길이 불일치: 예상 600초, 실제 120초 (20%)",
        "fileId": "private-file-id",
        "barcode": "81000000000",
        "occurredAt": occurred_at,
        "details": {
            "voiceType": "s",
            "fileId": "private-file-id",
            "barcode": "81000000000",
            "expectedDuration": 600,
            "actualDuration": 120,
            "ratio": 0.2,
        },
    }


def _deps(
    *,
    latest_id: int = 0,
    next_result: tuple[int, dict | None] = (0, None),
    sheet_rows: int | None = None,
    sms_result: dict | None = None,
    clock: Callable[[], datetime] | None = None,
) -> tuple[DeviceNotificationCycleDeps, dict[str, Mock]]:
    mocks = {
        "latest": Mock(return_value=latest_id),
        "next": Mock(return_value=next_result),
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
        "verify_video": Mock(),
    }
    return (
        DeviceNotificationCycleDeps(
            load_latest_id=mocks["latest"],
            load_next_event=mocks["next"],
            verify_video_duration_mismatch=mocks["verify_video"],
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


def _recording_incident_key(
    device_name: str,
    file_id: str,
    barcode: str,
    file_type: str = "recording",
) -> str:
    return hashlib.sha256(
        "\0".join(
            (device_name, file_id, barcode, file_type)
        ).encode("utf-8")
    ).hexdigest()


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
    base_deps, mocks = _deps(sheet_rows=1)
    deps = DeviceNotificationCycleDeps(
        load_latest_id=base_deps.load_latest_id,
        # default function identity가 실제 API의 batch 경로를 선택한다.
        load_next_event=cycle._load_next_device_notification,
        load_event_batch=load_batch,
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


def test_production_batch_query_includes_video_duration_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = {"latestId": 80}
    cursor.fetchall.return_value = [_video_duration_mismatch_event(80)]
    monkeypatch.setattr(
        cycle,
        "_create_db_connection",
        Mock(return_value=connection),
    )

    next_cursor, events = cycle._load_device_notification_batch(79)

    assert next_cursor == 80
    assert [item["code"] for item in events] == ["video_duration_mismatch"]
    sql, params = cursor.execute.call_args_list[1].args
    assert "n.code IN (%s, %s, %s, %s)" in sql
    assert "video_duration_mismatch" in params
    connection.close.assert_called_once_with()


def test_duration_mismatch_inside_grace_is_deferred_without_identifiers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
        True,
    )
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC",
        1800,
    )
    deps, mocks = _deps(
        next_result=(80, _video_duration_mismatch_event(80)),
    )

    result = DeviceNotificationAlertCycleHandler(deps).run(
        _request(cursor=_initialized_cursor(last_seen_id=79))
    )

    assert result.outcome == "no_change"
    assert result.deliveries == ()
    assert result.cursor["pendingEvents"] == []
    deferred = result.cursor["pendingVideoVerifications"]
    assert deferred["80"]["notificationId"] == 80
    assert deferred["80"]["attemptCount"] == 0
    assert deferred["80"]["verifyAfter"] == (
        datetime(2026, 8, 14, 1, 29, tzinfo=timezone.utc).isoformat()
    )
    serialized = json.dumps(result.cursor, ensure_ascii=False)
    assert "private-file-id" not in serialized
    assert "81000000000" not in serialized
    assert "010-1234-5678" not in serialized
    mocks["verify_video"].assert_not_called()
    mocks["send_sms"].assert_not_called()


def test_duration_mismatch_rollout_off_ignores_new_and_pauses_existing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
        False,
    )
    deps, mocks = _deps(
        next_result=(80, _video_duration_mismatch_event(80)),
    )
    cursor = {
        **_initialized_cursor(last_seen_id=79),
        "pendingVideoVerifications": {
            "78": {
                "notificationId": 78,
                "verifyAfter": "2026-08-14T00:00:00+00:00",
                "attemptCount": 2,
                "lastAttemptAt": "2026-08-13T23:00:00+00:00",
                "lastReason": "provider_error",
            }
        },
    }

    result = DeviceNotificationAlertCycleHandler(deps).run(
        _request(cursor=cursor)
    )

    assert result.deliveries == ()
    assert result.cursor["pendingEvents"] == []
    assert tuple(result.cursor["pendingVideoVerifications"]) == ("78",)
    assert result.cursor["videoVerificationDisabledCount"] == 1
    assert result.cursor["videoVerificationLastDisabledNotificationId"] == 80
    mocks["verify_video"].assert_not_called()
    mocks["send_sms"].assert_not_called()


def test_duration_mismatch_rollout_flag_rejects_ambiguous_value() -> None:
    # scheduler도 같은 module constant를 읽으므로 오타를 false로 축약하지 않는다.
    completed = subprocess.run(
        [sys.executable, "-c", "import boxer_company.settings"],
        cwd=str(cycle.core_settings.PROJECT_ROOT),
        env={
            **os.environ,
            "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED": "maybe",
        },
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode != 0
    assert "must be a boolean" in completed.stderr


def test_deferred_mismatch_survives_restart_and_normalized_upload_suppresses(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
        True,
    )
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC",
        1800,
    )
    event = _video_duration_mismatch_event(81)
    deps, mocks = _deps(next_result=(81, event))
    handler = DeviceNotificationAlertCycleHandler(deps)
    deferred = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=80))
    )
    mocks["next"].return_value = (81, None)
    mocks["verify_video"].return_value = (
        cycle.VideoDurationMismatchVerification(
            status=cycle._VIDEO_UPLOAD_NORMALIZED,
            reason="central_object_available",
            event=event,
        )
    )

    result = handler.run(
        _request(
            cursor=json.loads(json.dumps(deferred.cursor)),
            scheduled_at=_NOW + timedelta(minutes=31),
        )
    )

    assert result.outcome == "no_change"
    assert result.deliveries == ()
    assert result.cursor["pendingVideoVerifications"] == {}
    assert result.metrics["processedCount"] == 1
    mocks["verify_video"].assert_called_once_with(81)
    mocks["send_sms"].assert_not_called()
    mocks["append_sheet"].assert_not_called()


def test_missing_central_video_builds_slack_only_upload_alert_and_ack_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
        True,
    )
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC",
        1800,
    )
    event = _video_duration_mismatch_event(
        82,
        occurred_at="2026-08-14T00:00:00+00:00",
    )
    # recordings row가 없는 실제 알림은 감지 시각과 예상 길이로 시작을 추정한다.
    event["sessionBarcode"] = "81000000000"
    event["expectedDuration"] = 600
    deps, mocks = _deps(next_result=(82, event), sheet_rows=1)
    mocks["verify_video"].return_value = (
        cycle.VideoDurationMismatchVerification(
            status=cycle._VIDEO_UPLOAD_UNAVAILABLE,
            reason=cycle._VIDEO_UPLOAD_MISSING_RECORDING,
            event=event,
        )
    )
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=81))
    )

    assert len(result.deliveries) == 1
    delivery = result.deliveries[0]
    assert delivery.kind == "device_notification_alert"
    assert delivery.payload["code"] == "video_duration_mismatch"
    assert delivery.payload["render"] == {
        "type": "device_health_abnormal_alert",
        "includeActions": False,
        "includeDeviceVoiceAction": False,
    }
    device_result = delivery.payload["alertSummary"]["deviceResults"][0]
    assert device_result["alertCategory"] == "upload"
    assert device_result["barcode"] == "81000000000"
    assert device_result["sessionAtLabel"] == "세션 시작(추정)"
    assert device_result["sessionAt"] == "2026-08-14 08:50:00 KST"
    assert "영상 업로드가 확인되지 않았어" in device_result[
        "priorityReason"
    ]
    assert delivery.payload["smsReceipt"]["status"] == "not_applicable"
    assert result.cursor["pendingVideoVerifications"] == {}
    assert delivery.delivery_id in result.cursor["pendingDeliveryContexts"]
    mocks["send_sms"].assert_not_called()
    mocks["claim_sms"].assert_not_called()
    mocks["remember_sms"].assert_not_called()

    failed = handler.acknowledge(
        _request(cursor=dict(result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery.delivery_id,
                status="failed",
            ),
        ),
    )
    assert delivery.delivery_id in failed["pendingDeliveryContexts"]
    assert failed["pendingVideoVerifications"] == {}

    acknowledged = handler.acknowledge(
        _request(cursor=dict(failed)),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery.delivery_id,
                status="sent",
                external_message_id="1710000000.082",
                permalink="https://lifexio.slack.com/archives/C1/p82",
                delivered_at=_NOW,
            ),
        ),
    )
    assert acknowledged["pendingDeliveryContexts"] == {}
    assert acknowledged["pendingVideoVerifications"] == {}
    mocks["append_sheet"].assert_called_once()


@pytest.mark.parametrize(
    ("reason", "expected_text"),
    (
        (
            cycle._VIDEO_UPLOAD_MISSING_RECORDING,
            "영상 업로드가 확인되지 않았어",
        ),
        (
            cycle._VIDEO_UPLOAD_MISSING_OBJECT,
            "업로드된 영상 파일을 찾을 수 없어",
        ),
        (
            cycle._VIDEO_UPLOAD_UNDERSIZED_OBJECT,
            "업로드된 영상 파일의 크기가 비정상적으로 작아",
        ),
    ),
)
def test_video_mismatch_alert_copy_avoids_internal_storage_terms(
    reason: str,
    expected_text: str,
) -> None:
    # 같은 업로드 경고라도 비개발자가 조치 대상을 바로 이해하는 문구를 유지한다.
    summary, sheet_item, _recording_context = cycle._build_root_alert(
        {
            **_video_duration_mismatch_event(820),
            "videoAvailabilityReason": reason,
        }
    )

    priority_reason = summary["deviceResults"][0]["priorityReason"]
    assert expected_text in priority_reason
    assert "중앙 녹화" not in priority_reason
    assert "S3" not in priority_reason
    assert sheet_item["problemComponents"] == ["영상 업로드"]


def test_due_missing_video_waits_behind_following_device_alert(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
        True,
    )
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC",
        1800,
    )
    mismatch = _video_duration_mismatch_event(
        83,
        occurred_at="2026-08-14T00:00:00+00:00",
    )
    captureboard = _captureboard_event(84)
    load_batch = Mock(return_value=(84, [mismatch, captureboard]))
    base_deps, mocks = _deps()
    mocks["verify_video"].return_value = (
        cycle.VideoDurationMismatchVerification(
            status=cycle._VIDEO_UPLOAD_UNAVAILABLE,
            reason=cycle._VIDEO_UPLOAD_MISSING_RECORDING,
            event=mismatch,
        )
    )
    deps = DeviceNotificationCycleDeps(
        load_latest_id=base_deps.load_latest_id,
        load_next_event=cycle._load_next_device_notification,
        load_event_batch=load_batch,
        verify_video_duration_mismatch=mocks["verify_video"],
        append_sheet_alerts=base_deps.append_sheet_alerts,
        send_sms=base_deps.send_sms,
        claim_sms_delivery=base_deps.claim_sms_delivery,
        hold_sms_delivery_claim=base_deps.hold_sms_delivery_claim,
        clock=base_deps.clock,
        remember_sms_delivery=base_deps.remember_sms_delivery,
    )

    result = DeviceNotificationAlertCycleHandler(deps).run(
        _request(cursor=_initialized_cursor(last_seen_id=82))
    )

    assert result.deliveries[0].payload["code"] == (
        "captureboard_connection_error"
    )
    retry = result.cursor["pendingVideoVerifications"]["83"]
    assert retry["attemptCount"] == 0
    assert retry["lastReason"] == ""
    assert load_batch.call_count == 1
    mocks["verify_video"].assert_not_called()


def test_verification_provider_error_is_retried_without_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
        True,
    )
    deps, mocks = _deps(next_result=(83, None))
    mocks["verify_video"].side_effect = TimeoutError("private S3 timeout")
    cursor = {
        **_initialized_cursor(last_seen_id=83),
        "pendingVideoVerifications": {
            "83": {
                "notificationId": 83,
                "verifyAfter": "2026-08-14T00:00:00+00:00",
                "attemptCount": 0,
                "lastAttemptAt": "",
                "lastReason": "",
            }
        },
    }

    result = DeviceNotificationAlertCycleHandler(deps).run(
        _request(cursor=cursor)
    )

    assert result.deliveries == ()
    retry = result.cursor["pendingVideoVerifications"]["83"]
    assert retry["attemptCount"] == 1
    assert retry["lastReason"] == "provider_error"
    assert "private S3 timeout" not in json.dumps(result.cursor)
    mocks["verify_video"].assert_called_once_with(83)


def test_full_video_verification_queue_drops_only_overflow_without_blocking(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
        True,
    )
    pending_verifications = {
        str(notification_id): {
            "notificationId": notification_id,
            "verifyAfter": (_NOW + timedelta(days=1)).isoformat(),
            "attemptCount": 1,
            "lastAttemptAt": _NOW.isoformat(),
            "lastReason": "provider_error",
        }
        for notification_id in range(1, 501)
    }
    mismatch = _video_duration_mismatch_event(
        600,
        occurred_at="2026-08-14T00:00:00+00:00",
    )
    captureboard = _captureboard_event(601)
    load_batch = Mock(return_value=(601, [mismatch, captureboard]))
    base_deps, mocks = _deps()
    deps = DeviceNotificationCycleDeps(
        load_latest_id=base_deps.load_latest_id,
        load_next_event=cycle._load_next_device_notification,
        load_event_batch=load_batch,
        verify_video_duration_mismatch=mocks["verify_video"],
        append_sheet_alerts=base_deps.append_sheet_alerts,
        send_sms=base_deps.send_sms,
        claim_sms_delivery=base_deps.claim_sms_delivery,
        hold_sms_delivery_claim=base_deps.hold_sms_delivery_claim,
        clock=base_deps.clock,
        remember_sms_delivery=base_deps.remember_sms_delivery,
    )
    caplog.set_level(logging.ERROR)

    result = DeviceNotificationAlertCycleHandler(deps).run(
        _request(
            cursor={
                **_initialized_cursor(last_seen_id=599),
                "pendingVideoVerifications": pending_verifications,
            }
        )
    )

    assert result.deliveries[0].payload["code"] == (
        "captureboard_connection_error"
    )
    assert len(result.cursor["pendingVideoVerifications"]) == 500
    assert "600" not in result.cursor["pendingVideoVerifications"]
    assert result.cursor["videoVerificationDroppedCount"] == 1
    assert result.cursor["videoVerificationLastDroppedNotificationId"] == 600
    assert result.metrics["droppedVideoVerificationCount"] == 1
    assert "verification dropped queue_full" in caplog.text
    mocks["verify_video"].assert_not_called()


def test_cursor_normalization_records_deferred_overflow_without_identifiers(
) -> None:
    raw_verifications = {
        str(notification_id): {
            "notificationId": notification_id,
            "verifyAfter": (_NOW + timedelta(days=1)).isoformat(),
            "attemptCount": 0,
            "lastAttemptAt": "",
            "lastReason": "",
        }
        for notification_id in range(1, 502)
    }
    raw_verifications["999"] = {"notificationId": 999}

    normalized = cycle._normalize_cursor(
        {
            **_initialized_cursor(last_seen_id=999),
            "pendingVideoVerifications": raw_verifications,
        },
        now=_NOW,
    )

    assert len(normalized["pendingVideoVerifications"]) == 500
    assert normalized["videoVerificationDroppedCount"] == 2
    assert normalized["videoVerificationLastDroppedNotificationId"] == 999
    assert normalized["videoVerificationFirstDroppedAt"] == (
        _NOW.astimezone(timezone.utc).isoformat()
    )
    assert "fileId" not in json.dumps(normalized)


def test_video_verifier_uses_exact_join_for_short_duration_central_object(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        {
            **_video_duration_mismatch_event(85),
            "deviceSeq": None,
            "hasFileId": 1,
            "recordingSeq": 9001,
            "recordingDeviceSeq": 992,
            "recordingRecordedAt": datetime(2026, 8, 14, 0, 49),
            "streamingStatus": "AVAILABLE",
            "s3Bucket": "ultrasound-prod-kr",
            "s3FileKey": "81000000000/private-file-id.mp4",
        }
    ]
    s3_client = Mock()
    # 재생 시간이 짧아도 uploader의 파일 크기 invariant를 만족하면 억제한다.
    s3_client.head_object.return_value = {
        "ContentLength": 128_000,
        "StorageClass": "STANDARD",
    }
    monkeypatch.setattr(
        cycle,
        "_create_db_connection",
        Mock(return_value=connection),
    )
    monkeypatch.setattr(cycle, "_build_s3_client", Mock(return_value=s3_client))
    monkeypatch.setattr(
        cycle.cs,
        "S3_ULTRASOUND_BUCKET",
        "ultrasound-prod-kr",
    )
    monkeypatch.setattr(
        cycle.cs,
        "S3_ULTRASOUND_BUCKET_OWNER_ID",
        "123456789012",
    )
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_MIN_OBJECT_BYTES",
        128_000,
    )

    result = cycle._verify_video_duration_mismatch(85)

    assert result.status == cycle._VIDEO_UPLOAD_NORMALIZED
    assert result.event["sessionBarcode"] == "81000000000"
    assert result.event["recordingRecordedAt"] == "2026-08-14T00:49:00+00:00"
    sql, params = cursor.execute.call_args.args
    assert "ON r.fileId = n.fileId" in sql
    assert "n.fileId AS" not in sql
    assert params == (85, "video_duration_mismatch")
    s3_client.head_object.assert_called_once_with(
        Bucket="ultrasound-prod-kr",
        Key="81000000000/private-file-id.mp4",
        ExpectedBucketOwner="123456789012",
    )
    connection.close.assert_called_once_with()


@pytest.mark.parametrize(
    (
        "recording_seq",
        "recording_device_seq",
        "s3_error_code",
        "content_length",
        "expected_status",
        "expected_reason",
    ),
    (
        (None, None, "", 128_000, "unavailable", "missing_recording"),
        (9001, 992, "404", 128_000, "unavailable", "missing_object"),
        (9001, 992, "", 127_999, "unavailable", "undersized_object"),
        (9001, 993, "", 128_000, "unknown", "recording_device_mismatch"),
    ),
)
def test_video_verifier_only_marks_confirmed_absence_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    recording_seq: int | None,
    recording_device_seq: int | None,
    s3_error_code: str,
    content_length: int,
    expected_status: str,
    expected_reason: str,
) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        {
            **_video_duration_mismatch_event(86),
            "hasFileId": 1,
            "recordingSeq": recording_seq,
            "recordingDeviceSeq": recording_device_seq,
            "streamingStatus": "AVAILABLE",
            "s3Bucket": "ultrasound-prod-kr",
            "s3FileKey": "81000000000/private-file-id.mp4",
        }
    ]
    s3_client = Mock()
    s3_client.head_object.return_value = {
        "ContentLength": content_length,
        "StorageClass": "STANDARD",
    }
    if s3_error_code:
        error = RuntimeError("private provider message")
        error.response = {"Error": {"Code": s3_error_code}}
        s3_client.head_object.side_effect = error
    monkeypatch.setattr(
        cycle,
        "_create_db_connection",
        Mock(return_value=connection),
    )
    build_s3 = Mock(return_value=s3_client)
    monkeypatch.setattr(cycle, "_build_s3_client", build_s3)
    monkeypatch.setattr(
        cycle.cs,
        "S3_ULTRASOUND_BUCKET",
        "ultrasound-prod-kr",
    )
    monkeypatch.setattr(
        cycle.cs,
        "S3_ULTRASOUND_BUCKET_OWNER_ID",
        "123456789012",
    )
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_VIDEO_MIN_OBJECT_BYTES",
        128_000,
    )

    result = cycle._verify_video_duration_mismatch(86)

    assert result.status == expected_status
    assert result.reason == expected_reason
    if recording_seq is None:
        # 업로드 row가 없어도 notification 원문으로 세션 식별값을 복원한다.
        assert result.event["sessionBarcode"] == "81000000000"
        assert result.event["expectedDuration"] == 600
        build_s3.assert_not_called()


def test_video_verifier_does_not_turn_s3_access_denied_into_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [
        {
            **_video_duration_mismatch_event(87),
            "hasFileId": 1,
            "recordingSeq": 9001,
            "recordingDeviceSeq": 992,
            "streamingStatus": "AVAILABLE",
            "s3Bucket": "ultrasound-prod-kr",
            "s3FileKey": "81000000000/private-file-id.mp4",
        }
    ]
    error = RuntimeError("private access detail")
    error.response = {"Error": {"Code": "AccessDenied"}}
    s3_client = Mock()
    s3_client.head_object.side_effect = error
    monkeypatch.setattr(
        cycle,
        "_create_db_connection",
        Mock(return_value=connection),
    )
    monkeypatch.setattr(cycle, "_build_s3_client", Mock(return_value=s3_client))
    monkeypatch.setattr(
        cycle.cs,
        "S3_ULTRASOUND_BUCKET",
        "ultrasound-prod-kr",
    )
    monkeypatch.setattr(
        cycle.cs,
        "S3_ULTRASOUND_BUCKET_OWNER_ID",
        "123456789012",
    )
    with pytest.raises(RuntimeError, match="private access detail"):
        cycle._verify_video_duration_mismatch(87)


def test_persisted_cursor_processes_gap_instead_of_skipping_to_latest() -> None:
    persisted_cursor = _initialized_cursor(last_seen_id=11)
    deps, mocks = _deps(
        latest_id=1200,
        next_result=(12, _captureboard_event()),
    )
    handler = DeviceNotificationAlertCycleHandler(deps)

    result = handler.run(_request(cursor=persisted_cursor))

    assert result.cursor["lastSeenId"] == 12
    assert len(result.deliveries) == 1
    mocks["latest"].assert_not_called()
    mocks["next"].assert_called_once_with(11)


def test_persisted_recording_incident_continues_in_original_thread() -> None:
    first_occurred_at = "2026-08-14T00:55:00+00:00"
    event = _recording_stall_event(
        12,
        duration_seconds=240,
        occurred_at="2026-08-14T00:59:00+00:00",
    )
    incident_key = _recording_incident_key(
        "MB2-C00992",
        "recording-file",
        "81000000000",
    )
    cursor = {
        **_initialized_cursor(last_seen_id=11),
        "recordingStallIncidents": {
            incident_key: {
                "phase": "alerted",
                "deviceName": "MB2-C00992",
                "lastNotificationId": 11,
                "lastOccurredAt": first_occurred_at,
                "lastDurationSeconds": 120,
                "lastCurrentSize": 1000,
                "rootExternalMessageId": "1710000000.000100",
                "rootPermalink": "https://example.slack.com/archives/C1/p1",
                "lastCommentNotificationId": None,
            }
        },
    }
    deps, mocks = _deps(next_result=(12, event))
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


def test_merge_error_ack_does_not_open_captureboard_incident() -> None:
    event = {
        **_captureboard_event(15),
        "code": "segmented_recordings_merge_error",
        "message": "녹화 병합 실패",
        "details": {"voiceType": "n", "segmentCount": 3},
    }
    deps, _ = _deps(next_result=(15, event), sheet_rows=1)
    handler = DeviceNotificationAlertCycleHandler(deps)
    result = handler.run(_request(cursor=_initialized_cursor(last_seen_id=14)))

    acknowledged = handler.acknowledge(
        _request(cursor=dict(result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=result.deliveries[0].delivery_id,
                status="sent",
                external_message_id="1710000000.015",
                delivered_at=_NOW,
            ),
        ),
    )

    assert acknowledged["captureboardIncidents"] == {}


def test_sms_claim_uses_provider_immediate_server_clock() -> None:
    provider_now = _NOW + timedelta(minutes=5)
    deps, mocks = _deps(
        next_result=(12, _captureboard_event()),
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
    assert failed_cursor["captureboardIncidents"] == {}
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
    incident = acknowledged["captureboardIncidents"]["MB2-C00992"]
    assert incident["deviceSeq"] == 992
    assert incident["openedAt"] == (
        _NOW + timedelta(seconds=2)
    ).astimezone(timezone.utc).isoformat()
    assert incident["lastSuppressedAt"] == ""
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
    assert first_cursor["captureboardIncidents"]["MB2-C00992"][
        "openedCode"
    ] == "recording_critically_stalled"

    second_event = _recording_stall_event(
        32,
        duration_seconds=180,
        occurred_at="2026-08-14T01:01:00+00:00",
    )
    second_deps, second_mocks = _deps(
        next_result=(32, second_event),
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


def test_sent_incident_suppresses_repeat_after_cursor_restart_without_sheet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_CAPTUREBOARD_INCIDENT_QUIET_SEC",
        600,
    )
    first_deps, first_mocks = _deps(
        next_result=(40, _captureboard_event(40)),
        sheet_rows=None,
    )
    first_handler = DeviceNotificationAlertCycleHandler(first_deps)
    first_result = first_handler.run(
        _request(cursor=_initialized_cursor(last_seen_id=39))
    )
    first_delivery = first_result.deliveries[0]
    acknowledged = first_handler.acknowledge(
        _request(cursor=dict(first_result.cursor)),
        (
            AutomationDeliveryReceipt(
                delivery_id=first_delivery.delivery_id,
                status="sent",
                external_message_id="1710000000.040",
                permalink="https://lifexio.slack.com/archives/C1/p40",
                delivered_at=_NOW,
            ),
        ),
    )

    # API 재시작 때와 같은 JSON round-trip 뒤에도 ACK로 연 incident가 정본이다.
    restarted_cursor = json.loads(json.dumps(acknowledged))
    second_deps, second_mocks = _deps(
        next_result=(41, _captureboard_event(41)),
    )
    second_result = DeviceNotificationAlertCycleHandler(second_deps).run(
        _request(
            cursor=restarted_cursor,
            scheduled_at=_NOW + timedelta(seconds=30),
        )
    )

    assert acknowledged["captureboardIncidents"]["MB2-C00992"][
        "openedNotificationId"
    ] == 40
    assert first_mocks["append_sheet"].called
    assert second_result.outcome == "no_change"
    assert second_result.deliveries == ()
    assert second_result.cursor["lastSeenId"] == 41
    incident = second_result.cursor["captureboardIncidents"]["MB2-C00992"]
    assert incident["suppressedCount"] == 1
    assert incident["lastSuppressedNotificationId"] == 41
    assert incident["lastSuppressedAt"] == (
        _NOW + timedelta(seconds=30)
    ).isoformat()
    second_mocks["send_sms"].assert_not_called()


def test_captureboard_flapping_sequence_stays_in_one_sliding_incident(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_CAPTUREBOARD_INCIDENT_QUIET_SEC",
        600,
    )
    event_times = (
        datetime(2026, 8, 25, 15, 55, 18, tzinfo=_KST),
        datetime(2026, 8, 25, 15, 55, 48, tzinfo=_KST),
        datetime(2026, 8, 25, 16, 0, 46, tzinfo=_KST),
        datetime(2026, 8, 25, 16, 2, 16, tzinfo=_KST),
    )
    deps, mocks = _deps(sheet_rows=None)
    mocks["next"].side_effect = [
        (notification_id, _captureboard_event(notification_id))
        for notification_id in range(40, 44)
    ]
    handler = DeviceNotificationAlertCycleHandler(deps)

    first = handler.run(
        _request(
            cursor=_initialized_cursor(last_seen_id=39),
            scheduled_at=event_times[0],
        )
    )
    cursor = handler.acknowledge(
        _request(cursor=dict(first.cursor), scheduled_at=event_times[0]),
        (
            AutomationDeliveryReceipt(
                delivery_id=first.deliveries[0].delivery_id,
                status="sent",
                external_message_id="1710000000.040",
                delivered_at=event_times[0],
            ),
        ),
    )

    # 30초, 4분 58초, 1분 30초 간격의 물리 재끊김은 마지막 발생 기준
    # 10분 window를 계속 전진시키며 같은 incident 하나로 묶는다.
    for event_time in event_times[1:]:
        repeated = handler.run(
            _request(cursor=dict(cursor), scheduled_at=event_time)
        )
        assert repeated.deliveries == ()
        cursor = dict(repeated.cursor)

    incident = cursor["captureboardIncidents"]["MB2-C00992"]
    assert incident["openedNotificationId"] == 40
    assert incident["lastSuppressedNotificationId"] == 43
    assert incident["lastSuppressedAt"] == event_times[-1].isoformat()
    assert incident["suppressedCount"] == 3
    assert mocks["send_sms"].call_count == 1
    assert mocks["append_sheet"].call_count == 1


def test_captureboard_quiet_window_slides_and_expires(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_CAPTUREBOARD_INCIDENT_QUIET_SEC",
        600,
    )
    cursor = {
        **_initialized_cursor(last_seen_id=49),
        "captureboardIncidents": {
            "MB2-C00992": {
                "deviceName": "MB2-C00992",
                "status": "대기",
                "openedNotificationId": 40,
                "openedCode": "captureboard_connection_error",
                "openedAt": (_NOW - timedelta(minutes=30)).isoformat(),
                "lastSuppressedAt": (_NOW - timedelta(minutes=9)).isoformat(),
                "suppressedCount": 2,
            }
        },
    }
    active_deps, active_mocks = _deps(
        next_result=(50, _captureboard_event(50)),
    )

    active_result = DeviceNotificationAlertCycleHandler(active_deps).run(
        _request(cursor=cursor, scheduled_at=_NOW)
    )

    assert active_result.deliveries == ()
    active_incident = active_result.cursor["captureboardIncidents"][
        "MB2-C00992"
    ]
    assert active_incident["suppressedCount"] == 3
    assert active_incident["lastSuppressedAt"] == _NOW.isoformat()
    active_mocks["send_sms"].assert_not_called()

    expired_deps, expired_mocks = _deps(
        next_result=(51, _captureboard_event(51)),
    )
    expired_result = DeviceNotificationAlertCycleHandler(expired_deps).run(
        _request(
            cursor=dict(active_result.cursor),
            scheduled_at=_NOW + timedelta(minutes=10),
        )
    )

    # 마지막 반복 뒤 10분 동안 조용했으면 같은 장비라도 새 루트를 만든다.
    assert len(expired_result.deliveries) == 1
    expired_mocks["send_sms"].assert_called_once()


@pytest.mark.parametrize(
    ("opened_at", "last_suppressed_at"),
    (
        ("not-a-timestamp", ""),
        (_NOW.isoformat(), "not-a-timestamp"),
        ((_NOW + timedelta(seconds=1)).isoformat(), ""),
    ),
)
def test_invalid_or_future_incident_timestamp_allows_new_root(
    monkeypatch: pytest.MonkeyPatch,
    opened_at: str,
    last_suppressed_at: str,
) -> None:
    monkeypatch.setattr(
        cycle.cs,
        "DEVICE_NOTIFICATION_CAPTUREBOARD_INCIDENT_QUIET_SEC",
        600,
    )
    cursor = {
        **_initialized_cursor(last_seen_id=59),
        "captureboardIncidents": {
            "MB2-C00992": {
                "deviceName": "MB2-C00992",
                "status": "대기",
                "openedNotificationId": 40,
                "openedCode": "captureboard_connection_error",
                "openedAt": opened_at,
                "lastSuppressedAt": last_suppressed_at,
                "suppressedCount": 1,
            }
        },
    }
    deps, mocks = _deps(next_result=(60, _captureboard_event(60)))

    result = DeviceNotificationAlertCycleHandler(deps).run(
        _request(cursor=cursor, scheduled_at=_NOW)
    )

    assert len(result.deliveries) == 1
    mocks["send_sms"].assert_called_once()


def test_provider_exception_is_not_retried_and_is_marked_uncertain() -> None:
    deps, mocks = _deps(
        next_result=(50, _captureboard_event(50)),
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
    assert acknowledged["captureboardIncidents"]["MB2-C00992"][
        "openedNotificationId"
    ] == 52
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
