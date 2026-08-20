from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

import pytest

from boxer_company import device_health_monitor_cycle as cycle
from boxer_company.automation import (
    AutomationCycleRequest,
    DeviceHealthMonitorCycleHandler,
)
from boxer_company.device_health_monitor_cycle import (
    DeviceHealthMonitorCycleDeps,
    acknowledge_device_health_monitor_deliveries,
    build_device_health_monitor_seed_cursor,
    device_health_monitor_cursor_digest,
    run_device_health_monitor_cycle,
    update_device_health_monitor_alert_delivery_override,
)
from boxer_company.redis_device_state import DeviceStateRedisUnavailable
from boxer_company_adapter_slack.daily_device_round_reporter import (
    _collect_daily_device_round_abnormal_alert_items,
)
from boxer_company_adapter_slack.device_health_monitor_reporter import (
    _build_device_health_monitor_alert_fingerprint,
)


_KST = ZoneInfo("Asia/Seoul")
_NOW = datetime(2026, 8, 14, 9, 0, tzinfo=_KST)


def _seed_cursor(
    *,
    enabled: bool = True,
    alerts: Mapping[str, Any] | None = None,
    pending: Mapping[str, Any] | None = None,
    pending_decision: str = "preserve",
) -> dict[str, Any]:
    return build_device_health_monitor_seed_cursor(
        legacy_alert_delivery_enabled=enabled,
        alert_fingerprints=alerts or {},
        pending_alert_fingerprints=pending or {},
        pending_decision=pending_decision,
        seeded_at=_NOW - timedelta(minutes=5),
    )


def _device() -> dict[str, Any]:
    return {
        "deviceSeq": 10,
        "deviceName": "MB2-CYCLE",
        "hospitalSeq": 20,
        "hospitalRoomSeq": 30,
        "hospitalName": "테스트병원",
        "hospitalTelephone": "0212345678",
        "hospitalDeviceAlertPhone": "01012345678",
        "roomName": "2진료실",
    }


def _redis_snapshot() -> dict[str, dict[str, Any]]:
    return {
        "MB2-CYCLE": {
            "deviceState": {
                "isConnected": True,
                "updatedAt": _NOW.isoformat(),
                "status": "RUNNING",
                "captureBoardStatus": "missing",
                "captureBoardType": "YUH01",
                "acme": {
                    "usbList": [
                        {"name": "MMTLED", "type": "serial"},
                    ]
                },
            },
            "agentState": {"isConnected": True},
        }
    }


def _verified_abnormal(
    device: Mapping[str, Any],
    now: datetime,
) -> Mapping[str, Any]:
    del now
    return {
        **dict(device),
        "overallLabel": "이상",
        "componentLabels": {
            "audio": "정상",
            "pm2": "정상",
            "storage": "정상",
            "captureboard": "이상",
            "led": "정상",
        },
        "deviceVersion": "2.11.308",
        "voiceType": "n",
        "issue": "캡처보드 USB 연결을 확인할 수 없어",
        "sshReady": True,
        "sshReason": "ready",
    }


def test_api_and_legacy_collectors_share_canonical_hospital_fingerprint() -> None:
    device = _device()
    result = dict(_verified_abnormal(device, _NOW))
    # production SSH verifier가 collector 전에 적용하는 동일 issue 요약을
    # 재현해 실제 API/legacy 생성 경계의 key parity를 비교한다.
    result["issue"] = cycle._build_daily_device_round_issue_summary(result)
    api_item = cycle._collect_alert_items((result,))[0]
    legacy_item = _collect_daily_device_round_abnormal_alert_items(
        {
            "hospitalSeq": device["hospitalSeq"],
            "hospitalName": device["hospitalName"],
            "deviceResults": [result],
        }
    )[0]

    assert api_item["hospital"] == "#20 테스트병원"
    assert legacy_item["hospital"] == "#20 테스트병원"
    assert cycle._alert_fingerprint(api_item) == (
        _build_device_health_monitor_alert_fingerprint(legacy_item)
    )


def _deps(
    *,
    calls: dict[str, Any],
    append_sheet: Any | None = None,
    claim_sms: Any | None = None,
    remember_sms: Any | None = None,
    sms_result: Mapping[str, Any] | None = None,
) -> DeviceHealthMonitorCycleDeps:
    def _send_sms(
        payload: Mapping[str, Any],
        logger: Any,
    ) -> Mapping[str, Any]:
        del logger
        calls.setdefault("sms", []).append(dict(payload))
        return dict(
            sms_result
            if sms_result is not None
            else {
                "status": "sent",
                "ok": True,
                "provider": "solapi",
                "groupId": "group-1",
                "messageId": "message-1",
                "smsDeliveryStatus": "accepted",
            }
        )

    def _append_sheet(
        items: Sequence[Mapping[str, Any]],
        detected_at: datetime,
        permalink: str,
    ) -> int:
        calls.setdefault("sheet", []).append(
            (list(items), detected_at, permalink)
        )
        return 1

    def _archive(
        request_id: str,
        now: datetime,
        payload: Mapping[str, Any],
    ) -> bool:
        calls.setdefault("archive", []).append((request_id, now, dict(payload)))
        return True

    return DeviceHealthMonitorCycleDeps(
        load_devices=lambda: [_device()],
        load_redis_snapshot=lambda names: (
            calls.setdefault("redis", []).append(list(names)) or _redis_snapshot()
        ),
        verify_device=lambda device, now: (
            calls.setdefault("verify", []).append((dict(device), now))
            or _verified_abnormal(device, now)
        ),
        load_captureboard_incidents=lambda: {},
        send_sms=_send_sms,
        # 일반 cycle 테스트는 공통 claim/outbox의 파일 I/O와 분리하고,
        # 전용 안전성 테스트에서 실제 원자 파일 동작을 검증한다.
        claim_sms_delivery=claim_sms or (lambda *args, **kwargs: True),
        hold_sms_delivery_claim=lambda *args, **kwargs: True,
        clock=lambda: _NOW,
        remember_sms_delivery=remember_sms
        or (lambda *args, **kwargs: True),
        append_sheet_alerts=append_sheet or _append_sheet,
        archive_event=_archive,
    )


def test_cycle_confirms_hardware_twice_then_sends_once_without_pii() -> None:
    calls: dict[str, Any] = {}
    deps = _deps(calls=calls)

    first = run_device_health_monitor_cycle(
        request_id="health:cycle:1",
        now=_NOW,
        cursor=_seed_cursor(),
        deps=deps,
    )

    # USB 계열은 순간 플래핑을 피하려고 첫 poll에는 pending만 남긴다.
    assert first.deliveries == ()
    assert first.cursor["pendingAlertFingerprints"]
    assert calls.get("sms", []) == []
    assert calls.get("sheet", []) == []

    second = run_device_health_monitor_cycle(
        request_id="health:cycle:2",
        now=_NOW + timedelta(minutes=1),
        cursor=first.cursor,
        deps=deps,
    )

    assert len(second.deliveries) == 1
    assert len(calls["sms"]) == 1
    assert calls.get("sheet", []) == []
    assert second.cursor["cycleCompleted"] is False
    assert second.cursor["pendingSheetAlerts"]
    provider_payload = calls["sms"][0]
    assert provider_payload["sms"]["to"] == "01012345678"
    assert provider_payload["sms"]["message"]

    # 전화번호와 문자 본문은 provider 호출을 벗어나 delivery/cursor에 남지 않는다.
    serialized_delivery = json.dumps(
        second.deliveries[0].payload,
        ensure_ascii=False,
    )
    serialized_cursor = json.dumps(second.cursor, ensure_ascii=False)
    assert "01012345678" not in serialized_delivery
    assert "01012345678" not in serialized_cursor
    assert "phone" not in serialized_delivery.lower()
    assert "telephone" not in serialized_delivery.lower()
    assert "smsGroupId" not in second.deliveries[0].payload["alert"]
    assert "smsMessageId" not in second.deliveries[0].payload["alert"]
    pending_sheet = next(iter(second.cursor["pendingSheetAlerts"].values()))
    assert pending_sheet["item"]["smsGroupId"] == "group-1"

    third = run_device_health_monitor_cycle(
        request_id="health:cycle:3",
        now=_NOW + timedelta(minutes=2),
        cursor=second.cursor,
        deps=deps,
    )
    assert third.deliveries == ()
    assert len(calls["sms"]) == 1


def test_receipt_persist_failure_does_not_retry_and_locks_resend() -> None:
    calls: dict[str, Any] = {}

    def fail_remember(*args: Any, **kwargs: Any) -> bool:
        del args, kwargs
        raise OSError("private outbox failure")

    deps = _deps(calls=calls, remember_sms=fail_remember)
    first = run_device_health_monitor_cycle(
        request_id="health:persist:1",
        now=_NOW,
        cursor=_seed_cursor(),
        deps=deps,
    )
    second = run_device_health_monitor_cycle(
        request_id="health:persist:2",
        now=_NOW + timedelta(minutes=1),
        cursor=first.cursor,
        deps=deps,
    )
    third = run_device_health_monitor_cycle(
        request_id="health:persist:3",
        now=_NOW + timedelta(minutes=2),
        cursor=second.cursor,
        deps=deps,
    )

    # provider 성공 뒤 outbox fsync 실패는 결과 불명으로 잠그고 같은 장애를
    # 자동 재시도하지 않는다.
    assert len(calls["sms"]) == 1
    assert second.deliveries[0].payload["alert"]["smsDeliveryStatus"] == (
        "confirm_required"
    )
    assert second.deliveries[0].payload["alert"][
        "smsContactActionEnabled"
    ] is False
    assert third.deliveries == ()
    assert "private outbox failure" not in repr(second)


@dataclass(frozen=True)
class _Receipt:
    delivery_id: str
    status: str
    permalink: str
    delivered_at: datetime


def test_acknowledge_writes_sheet_once_with_slack_permalink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}
    deps = _deps(calls=calls)
    monkeypatch.setattr(
        cycle.company_settings,
        "DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC",
        86_400,
    )
    first = run_device_health_monitor_cycle(
        request_id="health:ack:1",
        now=_NOW,
        cursor=_seed_cursor(
            pending={
                "#999 테스트병원|테스트병실|MB2-C99999|테스트 장애": {
                    "firstSeenAt": (_NOW - timedelta(minutes=1)).isoformat(),
                    "lastSeenAt": (_NOW - timedelta(minutes=1)).isoformat(),
                    "count": 1,
                }
            },
        ),
        deps=deps,
    )
    # 테스트용 seed fingerprint는 실제 fingerprint와 다르므로 한 번 더 실행해 확정한다.
    if not first.deliveries:
        first = run_device_health_monitor_cycle(
            request_id="health:ack:2",
            now=_NOW + timedelta(minutes=1),
            cursor=first.cursor,
            deps=deps,
        )
    delivery = first.deliveries[0]
    receipt = _Receipt(
        delivery_id=delivery.delivery_id,
        status="sent",
        permalink="https://example.slack.com/archives/C1/p1",
        delivered_at=_NOW + timedelta(minutes=2),
    )

    cursor = acknowledge_device_health_monitor_deliveries(
        cursor=first.cursor,
        receipts=(receipt,),
        deps=deps,
    )

    assert len(calls["sheet"]) == 1
    assert calls["sheet"][0][2] == receipt.permalink
    assert delivery.delivery_id not in cursor["pendingSheetAlerts"]
    assert cursor["lastSheetRowCount"] == 1

    reminder = run_device_health_monitor_cycle(
        request_id="health:ack:3",
        now=_NOW + timedelta(hours=7),
        cursor=cursor,
        deps=deps,
    )
    assert len(reminder.deliveries) == 1
    # coordinator가 과거 ack로 reminder를 삼키지 않게 발생 시각까지 ID에 넣는다.
    assert reminder.deliveries[0].delivery_id != delivery.delivery_id


def test_sheet_append_failure_moves_to_non_blocking_outbox_repair() -> None:
    calls: dict[str, Any] = {"remember": []}

    def remember(*args: Any, **kwargs: Any) -> bool:
        calls["remember"].append((args, kwargs))
        return True

    def append_failure(*args: Any, **kwargs: Any) -> int:
        del args, kwargs
        raise TimeoutError("private sheet timeout")

    deps = _deps(
        calls=calls,
        append_sheet=append_failure,
        remember_sms=remember,
    )
    first = run_device_health_monitor_cycle(
        request_id="health:repair:1",
        now=_NOW,
        cursor=_seed_cursor(),
        deps=deps,
    )
    second = run_device_health_monitor_cycle(
        request_id="health:repair:2",
        now=_NOW + timedelta(minutes=1),
        cursor=first.cursor,
        deps=deps,
    )
    delivery = second.deliveries[0]

    acknowledged = acknowledge_device_health_monitor_deliveries(
        cursor=second.cursor,
        receipts=(
            _Receipt(
                delivery_id=delivery.delivery_id,
                status="sent",
                permalink="https://example.slack.com/archives/C1/p2",
                delivered_at=_NOW + timedelta(minutes=2),
            ),
        ),
        deps=deps,
    )

    assert delivery.delivery_id not in acknowledged["pendingSheetAlerts"]
    assert delivery.delivery_id in acknowledged["pendingSheetRepairs"]
    assert acknowledged["lastSheetWriteStatus"] == "repair_pending"

    # 다음 health poll은 repair context를 outbox로 재등록한 뒤 정상 점검을
    # 계속하며 같은 Slack/SMS delivery를 다시 만들지 않는다.
    repaired = run_device_health_monitor_cycle(
        request_id="health:repair:3",
        now=_NOW + timedelta(minutes=3),
        cursor=acknowledged,
        deps=deps,
    )
    assert repaired.cursor["pendingSheetRepairs"] == {}
    assert repaired.cursor["lastSheetWriteStatus"] == "repair_queued"
    assert repaired.deliveries == ()
    assert len(calls["sms"]) == 1


@pytest.mark.parametrize(
    ("repair_result", "expected_status"),
    ((1, "repair_completed"), (None, "disabled")),
)
def test_sheet_append_failure_without_group_retries_directly_from_safe_cursor(
    repair_result: int | None,
    expected_status: str,
) -> None:
    calls: dict[str, Any] = {"append": [], "remember": []}

    def append_flaky(
        items: Sequence[Mapping[str, Any]],
        detected_at: datetime,
        permalink: str,
    ) -> int | None:
        calls["append"].append((list(items), detected_at, permalink))
        if len(calls["append"]) <= 2:
            raise TimeoutError("private sheet timeout")
        return repair_result

    def remember(*args: Any, **kwargs: Any) -> bool:
        calls["remember"].append((args, kwargs))
        return True

    deps = _deps(
        calls=calls,
        append_sheet=append_flaky,
        remember_sms=remember,
        # provider group이 없는 발송 실패도 Slack 알림과 Sheet 기록 대상이다.
        sms_result={
            "status": "error",
            "ok": False,
            "provider": "solapi",
            "smsDeliveryStatus": "request_failed",
        },
    )
    first = run_device_health_monitor_cycle(
        request_id="health:direct-repair:1",
        now=_NOW,
        cursor=_seed_cursor(),
        deps=deps,
    )
    second = run_device_health_monitor_cycle(
        request_id="health:direct-repair:2",
        now=_NOW + timedelta(minutes=1),
        cursor=first.cursor,
        deps=deps,
    )
    delivery = second.deliveries[0]

    acknowledged = acknowledge_device_health_monitor_deliveries(
        cursor=second.cursor,
        receipts=(
            _Receipt(
                delivery_id=delivery.delivery_id,
                status="sent",
                permalink="https://example.slack.com/archives/C1/p-direct",
                delivered_at=_NOW + timedelta(minutes=2),
            ),
        ),
        deps=deps,
    )

    repair = acknowledged["pendingSheetRepairs"][delivery.delivery_id]
    assert repair["status"] == "sheet_pending"
    assert repair["item"]["smsGroupId"] == ""
    # cursor repair context에는 provider 호출의 번호·본문을 넣지 않는다.
    serialized_repair = json.dumps(repair, ensure_ascii=False)
    assert "01012345678" not in serialized_repair
    assert '"sms"' not in serialized_repair.lower()
    assert calls["remember"] == []

    still_pending = run_device_health_monitor_cycle(
        request_id="health:direct-repair:3",
        now=_NOW + timedelta(minutes=3),
        cursor=acknowledged,
        deps=deps,
    )
    assert delivery.delivery_id in still_pending.cursor["pendingSheetRepairs"]
    assert still_pending.cursor["lastSheetWriteStatus"] == "repair_pending"

    repaired = run_device_health_monitor_cycle(
        request_id="health:direct-repair:4",
        now=_NOW + timedelta(minutes=4),
        cursor=still_pending.cursor,
        deps=deps,
    )

    assert repaired.cursor["pendingSheetRepairs"] == {}
    assert repaired.cursor["lastSheetWriteStatus"] == expected_status
    assert repaired.deliveries == ()
    assert len(calls["append"]) == 3
    assert calls["append"][2][2].endswith("/p-direct")
    assert calls["remember"] == []


def test_sheet_pending_reconciles_committed_timeout_without_duplicate_append() -> None:
    calls: dict[str, Any] = {"append": [], "committed": set()}

    def append_after_commit_then_timeout(
        items: Sequence[Mapping[str, Any]],
        detected_at: datetime,
        permalink: str,
    ) -> int:
        item = dict(items[0])
        delivery_id = str(item.get("sheetDeliveryId") or "")
        calls["append"].append((delivery_id, detected_at, permalink))
        if delivery_id in calls["committed"]:
            # 실제 Sheet helper의 read-before-append reconciliation을 재현한다.
            return 0
        calls["committed"].add(delivery_id)
        raise TimeoutError("Sheet append committed before response timeout")

    deps = _deps(
        calls=calls,
        append_sheet=append_after_commit_then_timeout,
        sms_result={
            "status": "error",
            "ok": False,
            "provider": "solapi",
            "smsDeliveryStatus": "request_failed",
        },
    )
    first = run_device_health_monitor_cycle(
        request_id="health:committed-timeout:1",
        now=_NOW,
        cursor=_seed_cursor(),
        deps=deps,
    )
    second = run_device_health_monitor_cycle(
        request_id="health:committed-timeout:2",
        now=_NOW + timedelta(minutes=1),
        cursor=first.cursor,
        deps=deps,
    )
    delivery = second.deliveries[0]
    acknowledged = acknowledge_device_health_monitor_deliveries(
        cursor=second.cursor,
        receipts=(
            _Receipt(
                delivery_id=delivery.delivery_id,
                status="sent",
                permalink="https://example.slack.com/archives/C1/p-committed",
                delivered_at=_NOW + timedelta(minutes=2),
            ),
        ),
        deps=deps,
    )

    repair = acknowledged["pendingSheetRepairs"][delivery.delivery_id]
    assert repair["status"] == "sheet_pending"
    assert repair["item"]["sheetDeliveryId"] == delivery.delivery_id

    reconciled = run_device_health_monitor_cycle(
        request_id="health:committed-timeout:3",
        now=_NOW + timedelta(minutes=3),
        cursor=acknowledged,
        deps=deps,
    )

    assert reconciled.cursor["pendingSheetRepairs"] == {}
    assert reconciled.cursor["lastSheetWriteStatus"] == "repair_completed"
    assert reconciled.deliveries == ()
    assert calls["committed"] == {delivery.delivery_id}
    assert len(calls["append"]) == 2
    # Sheet reconciliation만 재시도하고 provider/Slack delivery는 다시 만들지 않는다.
    assert len(calls["sms"]) == 1


def test_alerts_off_keeps_pending_and_sends_immediately_after_enable() -> None:
    calls: dict[str, Any] = {}
    deps = _deps(calls=calls)
    cursor: Mapping[str, Any] = _seed_cursor(enabled=False)
    for index in range(2):
        result = run_device_health_monitor_cycle(
            request_id=f"health:off:{index}",
            now=_NOW + timedelta(minutes=index),
            cursor=cursor,
            deps=deps,
        )
        cursor = result.cursor
        assert result.deliveries == ()

    cursor = update_device_health_monitor_alert_delivery_override(
        cursor,
        enabled=True,
        updated_at=_NOW + timedelta(minutes=2),
    )
    enabled = run_device_health_monitor_cycle(
        request_id="health:on:1",
        now=_NOW + timedelta(minutes=2),
        cursor=cursor,
        deps=deps,
    )

    assert len(enabled.deliveries) == 1
    assert len(calls["sms"]) == 1


def test_invalid_option_is_rejected_before_external_calls() -> None:
    calls: dict[str, Any] = {}
    deps = _deps(calls=calls)

    with pytest.raises(ValueError, match="unsupported options"):
        run_device_health_monitor_cycle(
            request_id="health:invalid:1",
            now=_NOW,
            cursor=_seed_cursor(),
            options={"autoRetry": True},
            deps=deps,
        )

    assert calls == {}


def test_missing_api_seed_is_rejected_before_external_calls() -> None:
    calls: dict[str, Any] = {}

    with pytest.raises(ValueError, match="state seed is required"):
        run_device_health_monitor_cycle(
            request_id="health:unseeded:1",
            now=_NOW,
            deps=_deps(calls=calls),
        )

    assert calls == {}


def test_offline_override_changes_exact_durable_cursor_revision() -> None:
    seeded = _seed_cursor(enabled=False)
    previous_digest = device_health_monitor_cursor_digest(seeded)

    updated = update_device_health_monitor_alert_delivery_override(
        seeded,
        enabled=True,
        updated_at=_NOW,
    )

    assert updated["alertDeliveryOverride"]["enabled"] is True
    assert updated["stateOwnership"]["overrideRevision"] == 1
    assert device_health_monitor_cursor_digest(updated) != previous_digest


def test_automation_handler_returns_typed_delivery_without_auto_retry() -> None:
    calls: dict[str, Any] = {}
    handler = DeviceHealthMonitorCycleHandler(deps=_deps(calls=calls))
    first_request = AutomationCycleRequest(
        request_id="health:handler:1",
        tenant_id="lifex",
        cycle="device_health_monitor",
        scheduled_at=_NOW,
        cursor=_seed_cursor(),
    )
    first = handler.run(first_request)
    second = handler.run(
        AutomationCycleRequest(
            request_id="health:handler:2",
            tenant_id="lifex",
            cycle="device_health_monitor",
            scheduled_at=_NOW + timedelta(minutes=1),
            cursor=first.cursor,
        )
    )

    assert second.outcome == "completed"
    assert second.auto_retry_allowed is False
    assert second.deliveries[0].kind == "device_health_alert"
    assert second.cursor["cycleCompleted"] is False


def test_redis_failure_does_not_fallback_to_mda_or_ssh() -> None:
    calls: dict[str, Any] = {}
    deps = _deps(calls=calls)

    def _redis_failure(names: Sequence[str]) -> Mapping[str, Mapping[str, Any]]:
        del names
        raise DeviceStateRedisUnavailable("private redis detail")

    deps = DeviceHealthMonitorCycleDeps(
        load_devices=deps.load_devices,
        load_redis_snapshot=_redis_failure,
        verify_device=deps.verify_device,
        load_captureboard_incidents=deps.load_captureboard_incidents,
        send_sms=deps.send_sms,
        claim_sms_delivery=deps.claim_sms_delivery,
        hold_sms_delivery_claim=deps.hold_sms_delivery_claim,
        clock=deps.clock,
        remember_sms_delivery=deps.remember_sms_delivery,
        append_sheet_alerts=deps.append_sheet_alerts,
        archive_event=deps.archive_event,
    )
    result = run_device_health_monitor_cycle(
        request_id="health:redis:1",
        now=_NOW,
        cursor=_seed_cursor(),
        deps=deps,
    )

    assert result.deliveries == ()
    assert result.cursor["monitorUnavailableReason"] == "redis_unavailable"
    assert "private redis detail" not in repr(result)
    assert calls.get("verify", []) == []
    assert calls.get("sms", []) == []


def test_runtime_verification_disables_ssh_open_resend_and_force_reopen(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    def _fake_collect(
        device_name: str,
        component: str,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
        captured.update(
            {"deviceName": device_name, "component": component, **kwargs}
        )
        return (
            {"ssh": {"ready": False, "reason": "not_ready"}},
            {"deviceName": device_name},
            {},
        )

    monkeypatch.setattr(cycle, "_collect_runtime_checks", _fake_collect)
    result = cycle._verify_device_health_runtime(_device(), now=_NOW)

    assert captured == {
        "deviceName": "MB2-CYCLE",
        "component": "all",
        "resend_ssh_open": False,
        "allow_force_reopen": False,
    }
    assert result["sshReady"] is False


def test_s3_archive_uses_one_total_attempt_and_one_put(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {"configs": [], "puts": []}

    class _Client:
        def put_object(self, **kwargs: Any) -> None:
            calls["puts"].append(kwargs)

    class _Boto3:
        @staticmethod
        def client(*args: Any, **kwargs: Any) -> _Client:
            calls["client"] = (args, kwargs)
            return _Client()

    def _config(**kwargs: Any) -> dict[str, Any]:
        calls["configs"].append(kwargs)
        return kwargs

    monkeypatch.setattr(
        cycle.company_settings,
        "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET",
        "health-events",
    )
    monkeypatch.setattr(
        cycle.company_settings,
        "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX",
        "health",
    )
    monkeypatch.setattr(
        cycle,
        "_load_boto3_components",
        lambda: (_Boto3(), _config),
    )

    archived = cycle._archive_device_health_cycle_event(
        request_id="health:s3:1",
        now=_NOW,
        payload={"deliveryCount": 1},
    )

    assert archived is True
    assert calls["configs"][0]["retries"] == {
        "total_max_attempts": 1,
        "mode": "standard",
    }
    assert len(calls["puts"]) == 1
    assert json.loads(calls["puts"][0]["Body"])["deliveryCount"] == 1
