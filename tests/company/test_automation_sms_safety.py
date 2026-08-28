from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import json
import logging
import threading
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pytest

from boxer_company import sms_delivery_cycle as sms_cycle
from boxer_company.automation import (
    AutomationCycleRequest,
    AutomationDeliveryReceipt,
)
from boxer_company.device_health_monitor_cycle import (
    DeviceHealthMonitorCycleDeps,
    build_clean_device_health_monitor_cursor,
    run_device_health_monitor_cycle,
)
from boxer_company.device_notification_cycle import (
    DeviceNotificationAlertCycleHandler,
    DeviceNotificationCycleDeps,
)
from boxer_company.sms_delivery_cycle import (
    _SMS_AUTOMATION_RUNTIME_CLAIMS,
    _SMS_AUTOMATION_RUNTIME_CLAIMS_LOCK,
    _load_sms_delivery_outbox_items,
    acquire_automatic_sms_runtime_claim,
    build_automatic_sms_runtime_claim_key,
    claim_automatic_sms_delivery,
    publish_automatic_sms_runtime_claim_result,
    remember_sms_delivery_sheet_record,
)


_KST = ZoneInfo("Asia/Seoul")
_NOW = datetime(2026, 8, 14, 10, 0, tzinfo=_KST)
_DEVICE_NAME = "MB2-SHARED-SMS"


@pytest.fixture(autouse=True)
def _clear_runtime_sms_claims() -> Any:
    # process 전역 60초 claim이 테스트 case 사이로 새지 않게 격리한다.
    with _SMS_AUTOMATION_RUNTIME_CLAIMS_LOCK:
        _SMS_AUTOMATION_RUNTIME_CLAIMS.clear()
    yield
    with _SMS_AUTOMATION_RUNTIME_CLAIMS_LOCK:
        _SMS_AUTOMATION_RUNTIME_CLAIMS.clear()


def test_runtime_sms_claim_keeps_legacy_60_second_failure_cooldown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    moments = iter((10.0, 69.0, 70.0))
    monkeypatch.setattr(
        sms_cycle.time,
        "monotonic",
        lambda: next(moments),
    )
    claim_key = build_automatic_sms_runtime_claim_key(
        {
            "hospitalSeq": "20",
            "device": _DEVICE_NAME,
            "alertCategory": "video_signal",
            "problemComponents": ["캡처보드"],
            "issue": "캡처보드 연결 장애",
        }
    )

    is_owner, claim = acquire_automatic_sms_runtime_claim(claim_key)
    assert is_owner is True
    publish_automatic_sms_runtime_claim_result(
        claim,
        {"status": "error", "ok": False},
    )

    # provider 실패도 legacy와 같이 즉시 release하지 않고 60초까지 막는다.
    second_owner, second_claim = acquire_automatic_sms_runtime_claim(
        claim_key
    )
    assert second_owner is False
    assert second_claim is claim

    # monotonic 기준 정확히 60초가 지나면 다음 시도를 다시 허용한다.
    third_owner, third_claim = acquire_automatic_sms_runtime_claim(claim_key)
    assert third_owner is True
    assert third_claim is not claim


def _notification_request(*, cursor: Mapping[str, Any]) -> AutomationCycleRequest:
    return AutomationCycleRequest(
        request_id="notification:shared-sms:1",
        tenant_id="lifex",
        cycle="device_notification_alert",
        scheduled_at=_NOW,
        cursor=cursor,
    )


def _notification_event() -> dict[str, Any]:
    return {
        "notificationId": 12,
        "deviceSeq": 10,
        "deviceName": _DEVICE_NAME,
        "deviceVersion": "2.11.308",
        "code": "captureboard_connection_error",
        "details": {"voiceType": "n"},
        "occurredAt": _NOW.isoformat(),
        "hospitalSeq": 20,
        "hospitalName": "테스트병원",
        "hospitalDeviceAlertPhone": "01012345678",
        "hospitalRoomSeq": 30,
        "roomName": "2진료실",
    }


def _health_deps(
    *,
    send_sms: Any,
    claim_sms: Any,
    remember_sms: Any,
    append_sheet: Any | None = None,
) -> DeviceHealthMonitorCycleDeps:
    device = {
        "deviceSeq": 10,
        "deviceName": _DEVICE_NAME,
        "hospitalSeq": 20,
        "hospitalRoomSeq": 30,
        "hospitalName": "테스트병원",
        "hospitalTelephone": "0212345678",
        "hospitalDeviceAlertPhone": "01012345678",
        "roomName": "2진료실",
    }

    def verify(
        value: Mapping[str, Any],
        now: datetime,
    ) -> Mapping[str, Any]:
        del now
        return {
            **dict(value),
            "overallLabel": "이상",
            "componentLabels": {
                "audio": "정상",
                "pm2": "정상",
                "storage": "정상",
                "captureboard": "정상",
                "led": "이상",
            },
            "deviceVersion": "2.11.308",
            "voiceType": "",
            "issue": "LED USB 장치를 찾지 못했어",
            "sshReady": True,
            "sshReason": "ready",
        }

    return DeviceHealthMonitorCycleDeps(
        load_devices=lambda: [device],
        load_redis_snapshot=lambda names: {
            _DEVICE_NAME: {
                "deviceState": {
                    "isConnected": True,
                    "updatedAt": _NOW.isoformat(),
                    "status": "RUNNING",
                    "captureBoardStatus": "missing",
                    "captureBoardType": "YUH01",
                    "acme": {
                        "usbList": [
                            {
                                "type": "CAPTUREBOARD",
                                "deviceId": "1164:f57a",
                            }
                        ]
                    },
                },
                "agentState": {"isConnected": True},
            }
        },
        verify_device=verify,
        ssh_verification_configured=lambda: True,
        load_captureboard_incidents=lambda: {},
        send_sms=send_sms,
        claim_sms_delivery=claim_sms,
        hold_sms_delivery_claim=lambda *args, **kwargs: True,
        clock=lambda: _NOW,
        remember_sms_delivery=remember_sms,
        append_sheet_alerts=append_sheet or (
            lambda items, detected_at, permalink: 1
        ),
        write_event=lambda event_type, now, payload: True,
        start_event_archive=lambda now, logger: False,
    )


def _notification_handler(
    *,
    send_sms: Any,
    claim_sms: Any,
    remember_sms: Any,
    append_sheet: Any | None = None,
) -> DeviceNotificationAlertCycleHandler:
    deps = DeviceNotificationCycleDeps(
        load_latest_id=lambda: 12,
        load_next_event=lambda last_seen_id: (12, _notification_event()),
        append_sheet_alerts=append_sheet or (
            lambda items, **kwargs: 1
        ),
        send_sms=send_sms,
        claim_sms_delivery=claim_sms,
        hold_sms_delivery_claim=lambda *args, **kwargs: True,
        clock=lambda: _NOW,
        remember_sms_delivery=remember_sms,
    )
    return DeviceNotificationAlertCycleHandler(
        deps,
        logger=logging.getLogger("test.automation_sms_safety"),
    )


@pytest.mark.parametrize("concurrent", [False, True])
def test_led_health_and_captureboard_notification_use_independent_provider_claims(
    tmp_path: Any,
    concurrent: bool,
) -> None:
    outbox_path = tmp_path / "sms-outbox.json"
    provider_calls: list[dict[str, Any]] = []
    provider_lock = threading.Lock()

    def send_sms(payload: Mapping[str, Any], logger: Any) -> dict[str, Any]:
        del logger
        with provider_lock:
            provider_calls.append(dict(payload))
            call_number = len(provider_calls)
        return {
            "status": "sent",
            "ok": True,
            "provider": "solapi",
            "groupId": f"group-{call_number}",
            "messageId": f"message-{call_number}",
            "smsDeliveryStatus": "accepted",
        }

    def remember_sms(alert_item: dict[str, Any], **kwargs: Any) -> bool:
        return remember_sms_delivery_sheet_record(
            alert_item,
            outbox_path=outbox_path,
            **kwargs,
        )

    health_deps = _health_deps(
        send_sms=send_sms,
        # default production function identity는 durable 파일이 아니라
        # 두 cycle이 공유하는 legacy process-memory claim 경로를 선택한다.
        claim_sms=claim_automatic_sms_delivery,
        remember_sms=remember_sms,
    )
    # hardware 경보의 두 번 확인 규칙은 provider claim 전에 미리 통과시킨다.
    health_first = run_device_health_monitor_cycle(
        request_id="health:shared-sms:seed",
        now=_NOW - timedelta(minutes=1),
        cursor=build_clean_device_health_monitor_cursor(
            alert_delivery_enabled=True,
            seeded_at=_NOW - timedelta(minutes=2),
        ),
        deps=health_deps,
    )
    notification_handler = _notification_handler(
        send_sms=send_sms,
        claim_sms=claim_automatic_sms_delivery,
        remember_sms=remember_sms,
    )

    def run_health() -> Any:
        return run_device_health_monitor_cycle(
            request_id="health:shared-sms:send",
            now=_NOW,
            cursor=health_first.cursor,
            deps=health_deps,
        )

    def run_notification() -> Any:
        return notification_handler.run(
            _notification_request(
                cursor={
                    "initialized": True,
                    "lastSeenId": 11,
                    "pendingDeliveryContexts": {},
                }
            )
        )

    if concurrent:
        with ThreadPoolExecutor(max_workers=2) as executor:
            health_future = executor.submit(run_health)
            notification_future = executor.submit(run_notification)
            health_result = health_future.result()
            notification_result = notification_future.result()
    else:
        health_result = run_health()
        notification_result = run_notification()

    # LED와 장비 캡처보드 이벤트는 서로 다른 장애 family라 각각 한 번만
    # provider claim을 잡고 독립적으로 발송한다.
    assert len(provider_calls) == 2
    assert len(health_result.deliveries) == 1
    assert len(notification_result.deliveries) == 1
    serialized_deliveries = json.dumps(
        [
            health_result.deliveries[0].payload,
            notification_result.deliveries[0].payload,
        ],
        ensure_ascii=False,
    )
    # legacy Slack 자동발송 확인 action은 대상·본문을 유지한다.
    assert "01012345678" in serialized_deliveries
    assert "초음파 진단기와 캡처보드" in serialized_deliveries
    assert "LED USB 케이블" in serialized_deliveries
    for private_value in ("group-1", "group-2", "message-1", "message-2", "solapi"):
        assert private_value not in serialized_deliveries
    assert len(_load_sms_delivery_outbox_items(outbox_path=outbox_path)) == 2


def test_notification_sheet_timeout_keeps_durable_receipt_for_repair(
    tmp_path: Any,
) -> None:
    outbox_path = tmp_path / "sms-outbox.json"

    def claim_sms(
        device_name: str,
        alert_category: str,
        *,
        claimed_at: datetime,
    ) -> bool:
        return claim_automatic_sms_delivery(
            device_name,
            alert_category,
            claimed_at=claimed_at,
            outbox_path=outbox_path,
        )

    def remember_sms(alert_item: dict[str, Any], **kwargs: Any) -> bool:
        return remember_sms_delivery_sheet_record(
            alert_item,
            outbox_path=outbox_path,
            **kwargs,
        )

    def send_sms(payload: Mapping[str, Any], logger: Any) -> dict[str, Any]:
        del payload, logger
        return {
            "status": "sent",
            "ok": True,
            "provider": "solapi",
            "groupId": "group-sheet-repair",
            "messageId": "message-sheet-repair",
            "smsDeliveryStatus": "accepted",
        }

    def append_failure(
        items: Sequence[Mapping[str, Any]],
        **kwargs: Any,
    ) -> int:
        del items, kwargs
        raise TimeoutError("private sheet timeout")

    handler = _notification_handler(
        send_sms=send_sms,
        claim_sms=claim_sms,
        remember_sms=remember_sms,
        append_sheet=append_failure,
    )
    result = handler.run(
        _notification_request(
            cursor={
                "initialized": True,
                "lastSeenId": 11,
                "pendingDeliveryContexts": {},
            }
        )
    )
    delivery_id = result.deliveries[0].delivery_id
    acknowledged = handler.acknowledge(
        _notification_request(cursor=result.cursor),
        (
            AutomationDeliveryReceipt(
                delivery_id=delivery_id,
                status="sent",
                external_message_id="1710000000.012",
                permalink="https://lifexio.slack.com/archives/C1/p12",
            ),
        ),
    )

    items = _load_sms_delivery_outbox_items(outbox_path=outbox_path)
    assert len(items) == 1
    assert items[0]["smsGroupId"] == "group-sheet-repair"
    assert items[0]["permalink"] == (
        "https://lifexio.slack.com/archives/C1/p12"
    )
    assert acknowledged["pendingSheetRepairs"][delivery_id]["status"] == (
        "outbox_pending"
    )
    assert delivery_id not in acknowledged["pendingDeliveryContexts"]
