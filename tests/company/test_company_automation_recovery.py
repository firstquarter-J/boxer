from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import threading

import pytest

from boxer_company.automation import AutomationCycleContractError
from boxer_company_api import automation_recovery
from boxer_company_api.automation import JsonAutomationCycleStateStore
from boxer_company_api.automation_recovery import (
    AutomationRecoveryError,
    import_device_health_monitor_forward_bundle,
    inspect_device_health_monitor_state,
    main,
    override_device_health_monitor_alert_delivery,
    resolve_automatic_sms_uncertain_claim,
    resolve_automation_uncertain_state,
    seed_device_health_monitor_state,
)
from boxer_company.device_health_state_bundle import (
    build_device_health_forward_bundle,
    create_protected_json_file,
)
from boxer_company.sms_delivery_cycle import (
    claim_automatic_sms_delivery,
    hold_automatic_sms_delivery_claim,
    inspect_automatic_sms_recovery_state,
)
from boxer_company_adapter_slack.device_health_state_migration import (
    export_device_health_forward_bundle,
)
from boxer_company_adapter_slack import device_health_state_migration


_TENANT = "lifex"
_CYCLE = "device_health_monitor"
_CYCLE_KEY = "continuous"
_REQUEST_ID = "automation:device-health:run:abc"
_NOW = datetime(2026, 8, 20, 1, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _configured_initialized_sms_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """recovery 테스트도 운영과 같은 canonical initialized 두 파일을 쓴다."""

    outbox = tmp_path / "sms-outbox.json"
    claims = outbox.with_name(f"{outbox.name}.automatic-claims.json")
    outbox.write_text('{"items":[],"version":1}\n', encoding="utf-8")
    claims.write_text('{"claims":{},"version":2}\n', encoding="utf-8")
    os.chmod(outbox, 0o600)
    os.chmod(claims, 0o600)
    monkeypatch.setenv("SMS_DELIVERY_OUTBOX_PATH", str(outbox))
    monkeypatch.setattr(
        device_health_state_migration.company_settings,
        "SMS_DELIVERY_OUTBOX_PATH",
        str(outbox),
    )
    return outbox


def _state_key() -> str:
    raw = "\0".join((_TENANT, _CYCLE, _CYCLE_KEY))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _notification_state_key() -> str:
    raw = "\0".join(
        (_TENANT, "device_notification_alert", "continuous")
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _write_automation_document(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o600)


def _legacy_health_state(*, enabled: bool = False) -> dict[str, object]:
    observed_at = "2026-08-20T09:00:00+09:00"
    return {
        "version": 1,
        "legacyAlertDeliveryEnabled": enabled,
        "alertFingerprints": {
            "hospital|room|MB2-OLD|audio": {
                "firstAlertedAt": observed_at,
                "lastAlertedAt": observed_at,
                "lastSeenAt": observed_at,
                "count": 1,
            }
        },
        "pendingAlertFingerprints": {
            "hospital|room|MB2-PENDING|video": {
                "firstSeenAt": observed_at,
                "lastSeenAt": observed_at,
                "count": 1,
            }
        },
    }


def _legacy_notification_state() -> dict[str, object]:
    """forward import가 쓰는 initialized, drained notification 상태다."""

    observed_at = "2026-08-20T09:00:00+09:00"
    return {
        "initialized": True,
        "initializedAt": observed_at,
        "lastSeenId": 12,
        "lastPolledAt": observed_at,
        "pendingEvents": [],
        "recentCaptureboardAlerts": {},
        "recordingStallIncidents": {},
        "captureboardIncidents": {},
        "captureboardIncidentsLastSheetCheckedAt": "",
        "lastSentAt": observed_at,
        "lastSentNotificationId": 12,
        "lastSlackMessageTs": "1710000000.000100",
        "lastSlackPermalink": "https://example.slack.com/archives/C1/p1",
    }


def _strict_sms_state(
    outbox: Path,
    *,
    now: datetime | None = None,
) -> dict[str, object]:
    return inspect_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        require_initialized=True,
        now=now,
    )


def _create_forward_bundle(path: Path) -> str:
    """SMS target CAS 테스트가 공유하는 검증된 forward bundle을 만든다."""

    seed = _legacy_health_state(enabled=True)
    bundle = build_device_health_forward_bundle(
        legacy_state={
            "alertDeliveryOverride": {
                "enabled": True,
                "updatedAt": "2026-08-20T09:00:00+09:00",
                "updatedBy": "slack_admin",
            },
            "alertFingerprints": seed["alertFingerprints"],
            "pendingAlertFingerprints": seed["pendingAlertFingerprints"],
        },
        legacy_notification_state=_legacy_notification_state(),
        source_state_digest="1" * 64,
        notification_source_state_digest="2" * 64,
        sms_state_digest="3" * 64,
        exported_at=_NOW,
    )
    return create_protected_json_file(
        path,
        bundle,
        label="test forward bundle",
    )


def test_device_health_seed_preserves_reviewed_override_and_pending_once(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    result = seed_device_health_monitor_state(
        state_path=path,
        tenant_id=_TENANT,
        legacy_state=_legacy_health_state(enabled=False),
        pending_decision="preserve",
        now=datetime(2026, 8, 20, 1, 0, tzinfo=timezone.utc),
    )

    assert result["alertDeliveryEnabled"] is False
    assert result["alertFingerprintCount"] == 1
    assert result["pendingFingerprintCount"] == 1
    inspected = inspect_device_health_monitor_state(
        state_path=path,
        tenant_id=_TENANT,
    )
    assert inspected["cursorDigest"] == result["cursorDigest"]
    assert inspected["pendingDeliveryCount"] == 0
    assert inspected["inFlight"] is False

    # 최초 seed는 기존 state에 병합하거나 재적용하지 않는다.
    with pytest.raises(AutomationRecoveryError, match="not empty"):
        seed_device_health_monitor_state(
            state_path=path,
            tenant_id=_TENANT,
            legacy_state=_legacy_health_state(enabled=False),
            pending_decision="preserve",
        )


def test_device_health_seed_can_assume_uncertain_pending_was_delivered(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    result = seed_device_health_monitor_state(
        state_path=path,
        tenant_id=_TENANT,
        legacy_state=_legacy_health_state(enabled=True),
        pending_decision="assume_delivered",
        now=datetime(2026, 8, 20, 1, 0, tzinfo=timezone.utc),
    )

    assert result["pendingFingerprintCount"] == 0
    assert result["alertFingerprintCount"] == 2
    state = JsonAutomationCycleStateStore(path).load(_state_key())
    assert state["cursor"]["stateOwnership"]["pendingDecision"] == (
        "assume_delivered"
    )

    malformed = _legacy_health_state(enabled=True)
    malformed["version"] = True
    with pytest.raises(AutomationRecoveryError, match="payload is invalid"):
        seed_device_health_monitor_state(
            state_path=tmp_path / "bool-version.json",
            tenant_id=_TENANT,
            legacy_state=malformed,
            pending_decision="preserve",
        )


@pytest.mark.parametrize(
    "document",
    (
        {"version": True, "cycles": {}},
        {"version": 1, "cycles": {_state_key(): None}},
        {"version": 1, "cycles": {_state_key(): []}},
        {"version": 1, "cycles": [], "extra": {}},
    ),
)
def test_state_store_rejects_malformed_raw_document_without_empty_fallback(
    tmp_path: Path,
    document: object,
) -> None:
    path = tmp_path / "automation.json"
    _write_automation_document(path, document)

    with pytest.raises(AutomationCycleContractError):
        JsonAutomationCycleStateStore(path).load(_state_key())
    with pytest.raises(AutomationRecoveryError):
        inspect_device_health_monitor_state(
            state_path=path,
            tenant_id=_TENANT,
        )


def test_state_store_rejects_unprotected_leaf_parent_and_symlink(
    tmp_path: Path,
) -> None:
    payload = {"version": 1, "cycles": {}}
    loose_file = tmp_path / "loose.json"
    _write_automation_document(loose_file, payload)
    os.chmod(loose_file, 0o644)
    with pytest.raises(AutomationCycleContractError, match="not protected"):
        JsonAutomationCycleStateStore(loose_file).load(_state_key())

    protected_target = tmp_path / "target.json"
    _write_automation_document(protected_target, payload)
    symlink_path = tmp_path / "linked.json"
    symlink_path.symlink_to(protected_target)
    with pytest.raises(AutomationCycleContractError, match="not protected"):
        JsonAutomationCycleStateStore(symlink_path).load(_state_key())

    loose_parent = tmp_path / "loose-parent"
    loose_parent.mkdir(mode=0o700)
    os.chmod(loose_parent, 0o777)
    with pytest.raises(AutomationCycleContractError, match="parent"):
        JsonAutomationCycleStateStore(
            loose_parent / "state.json"
        ).load(_state_key())


def test_present_empty_health_target_is_not_absent_seed_target(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    store = JsonAutomationCycleStateStore(path)
    store.save(_state_key(), {})

    with pytest.raises(AutomationRecoveryError, match="invalid"):
        inspect_device_health_monitor_state(
            state_path=path,
            tenant_id=_TENANT,
        )
    with pytest.raises(AutomationRecoveryError, match="not empty"):
        seed_device_health_monitor_state(
            state_path=path,
            tenant_id=_TENANT,
            legacy_state=_legacy_health_state(enabled=False),
            pending_decision="preserve",
        )


def test_runtime_and_recovery_store_instances_share_atomic_file_lock(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    first = JsonAutomationCycleStateStore(path)
    second = JsonAutomationCycleStateStore(path)
    other_key = hashlib.sha256(b"other-cycle").hexdigest()
    entered = threading.Event()
    release = threading.Event()
    second_done = threading.Event()

    def hold_recovery_cas(
        _exists: bool,
        _state: dict[str, object],
    ) -> tuple[dict[str, object], None]:
        entered.set()
        assert release.wait(timeout=2)
        return {"manualResolution": True}, None

    first_thread = threading.Thread(
        target=lambda: first.mutate_cycle(_state_key(), hold_recovery_cas)
    )
    second_thread = threading.Thread(
        target=lambda: (
            second.save(other_key, {"runtimeWrite": True}),
            second_done.set(),
        )
    )
    first_thread.start()
    assert entered.wait(timeout=2)
    second_thread.start()
    assert not second_done.wait(timeout=0.05)
    release.set()
    first_thread.join(timeout=2)
    second_thread.join(timeout=2)

    # 두 writer가 같은 raw document를 CAS하므로 다른 cycle update도 유실되지 않는다.
    assert first.load(_state_key()) == {"manualResolution": True}
    assert first.load(other_key) == {"runtimeWrite": True}


def test_device_health_override_requires_exact_offline_cursor_and_no_delivery(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    seeded = seed_device_health_monitor_state(
        state_path=path,
        tenant_id=_TENANT,
        legacy_state=_legacy_health_state(enabled=False),
        pending_decision="preserve",
    )
    with pytest.raises(AutomationRecoveryError, match="state changed"):
        override_device_health_monitor_alert_delivery(
            state_path=path,
            tenant_id=_TENANT,
            expected_cursor_digest="0" * 24,
            expected_enabled=False,
            enabled=True,
        )

    updated = override_device_health_monitor_alert_delivery(
        state_path=path,
        tenant_id=_TENANT,
        expected_cursor_digest=str(seeded["cursorDigest"]),
        expected_enabled=False,
        enabled=True,
    )
    assert updated["previousEnabled"] is False
    assert updated["alertDeliveryEnabled"] is True
    assert updated["cursorDigest"] != seeded["cursorDigest"]

    replay = override_device_health_monitor_alert_delivery(
        state_path=path,
        tenant_id=_TENANT,
        expected_cursor_digest=str(updated["cursorDigest"]),
        expected_enabled=True,
        enabled=True,
    )
    assert replay["updated"] is False
    assert replay["cursorDigest"] == updated["cursorDigest"]

    store = JsonAutomationCycleStateStore(path)
    state = store.load(_state_key())
    state["pendingDeliveries"] = [
        {"deliveryId": "device_health_monitor:pending"}
    ]
    store.save(_state_key(), state)
    with pytest.raises(AutomationRecoveryError, match="pending deliveries"):
        override_device_health_monitor_alert_delivery(
            state_path=path,
            tenant_id=_TENANT,
            expected_cursor_digest=str(updated["cursorDigest"]),
            expected_enabled=True,
            enabled=False,
        )


def test_device_health_forward_bundle_cli_requires_stopped_api_and_empty_target_cas(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    state_path = tmp_path / "automation.json"
    legacy_path = tmp_path / "legacy-health.json"
    bundle_path = tmp_path / "health-forward.bundle.json"
    seed = _legacy_health_state(enabled=False)
    legacy_path.write_text(
        json.dumps(
            {
                "alertDeliveryOverride": {
                    "enabled": False,
                    "updatedAt": "2026-08-20T09:00:00+09:00",
                    "updatedBy": "slack_admin",
                },
                "alertFingerprints": seed["alertFingerprints"],
                "pendingAlertFingerprints": seed[
                    "pendingAlertFingerprints"
                ],
            }
        ),
        encoding="utf-8",
    )
    os.chmod(legacy_path, 0o600)
    legacy_digest = hashlib.sha256(legacy_path.read_bytes()).hexdigest()
    notification_path = tmp_path / "legacy-notification.json"
    _write_automation_document(
        notification_path,
        _legacy_notification_state(),
    )
    notification_digest = hashlib.sha256(
        notification_path.read_bytes()
    ).hexdigest()
    sms_outbox = tmp_path / "sms-outbox.json"
    sms_state = _strict_sms_state(sms_outbox)
    exported = export_device_health_forward_bundle(
        state_path=legacy_path,
        notification_state_path=notification_path,
        bundle_path=bundle_path,
        expected_state_digest=legacy_digest,
        expected_notification_state_digest=notification_digest,
        expected_sms_state_digest=str(sms_state["stateDigest"]),
        sms_outbox_path=sms_outbox,
        now=datetime(2026, 8, 20, 1, 0, tzinfo=timezone.utc),
    )
    target_state = inspect_device_health_monitor_state(
        state_path=state_path,
        tenant_id=_TENANT,
    )
    target_digest = target_state["targetStateDigest"]
    notification_target_digest = target_state[
        "notificationTargetStateDigest"
    ]

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--state-path",
                str(state_path),
                "--tenant-id",
                _TENANT,
                "--import-device-health-forward-bundle-path",
                str(bundle_path),
                "--expected-bundle-digest",
                str(exported["bundleDigest"]),
                "--expected-target-state-digest",
                str(target_digest),
                "--expected-notification-target-state-digest",
                str(notification_target_digest),
                "--sms-outbox-path",
                str(sms_outbox),
                "--expected-sms-state-digest",
                str(sms_state["stateDigest"]),
                "--pending-decision",
                "preserve",
            ]
        )
    assert str(exc_info.value) == (
        "automation_recovery_requires_stopped_service"
    )

    assert main(
        [
            "--state-path",
            str(state_path),
            "--tenant-id",
            _TENANT,
            "--import-device-health-forward-bundle-path",
            str(bundle_path),
            "--expected-bundle-digest",
            str(exported["bundleDigest"]),
            "--expected-target-state-digest",
            str(target_digest),
            "--expected-notification-target-state-digest",
            str(notification_target_digest),
            "--sms-outbox-path",
            str(sms_outbox),
            "--expected-sms-state-digest",
            str(sms_state["stateDigest"]),
            "--pending-decision",
            "preserve",
            "--confirm-service-stopped",
        ]
    ) == 0
    output = json.loads(capsys.readouterr().out)
    assert output["seeded"] is True
    assert output["pendingFingerprintCount"] == 1
    assert output["notificationLastSeenId"] == 12
    assert output["targetSmsStateDigest"] == sms_state["stateDigest"]
    notification = JsonAutomationCycleStateStore(state_path).load(
        _notification_state_key()
    )
    assert notification["cursor"]["lastSeenId"] == 12
    assert "MB2-PENDING" not in json.dumps(output)


def test_api_recovery_cli_exposes_forward_migration_only() -> None:
    """API recovery CLI가 Slack-local export를 다시 제공하지 않게 고정한다."""

    help_text = automation_recovery._build_parser().format_help()

    assert "--import-device-health-forward-bundle-path" in help_text
    assert "--export-device-health-rollback-bundle-path" not in help_text
    assert "--inspect-device-health-rollback-source" not in help_text


def test_forward_bundle_digest_drift_writes_zero_api_state(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "automation.json"
    bundle_path = tmp_path / "forward.bundle.json"
    seed = _legacy_health_state(enabled=True)
    bundle = {
        "schema": "boxer.device_health_state_bundle",
        "version": 1,
        "direction": "slack_to_api",
        "exportedAt": "2026-08-20T01:00:00+00:00",
        "sourceDigest": "1" * 64,
        "payload": {
            "alertDeliveryEnabled": True,
            "alertFingerprints": seed["alertFingerprints"],
            "pendingAlertFingerprints": seed[
                "pendingAlertFingerprints"
            ],
            "notificationState": _legacy_notification_state(),
        },
        "safety": {
            "notificationPendingEventCount": 0,
            "notificationStateDigest": "2" * 64,
            "smsOutboxItemCount": 0,
            "smsUnresolvedClaimCount": 0,
            "activeSettledClaimCount": 0,
            "smsStateDigest": "3" * 64,
        },
    }
    bundle_digest = create_protected_json_file(
        bundle_path,
        bundle,
        label="test forward bundle",
    )
    target_state = inspect_device_health_monitor_state(
        state_path=state_path,
        tenant_id=_TENANT,
    )
    target_digest = target_state["targetStateDigest"]
    bundle["sourceDigest"] = "2" * 64
    bundle_path.write_text(json.dumps(bundle), encoding="utf-8")
    os.chmod(bundle_path, 0o600)

    with pytest.raises(AutomationRecoveryError, match="bundle changed"):
        import_device_health_monitor_forward_bundle(
            state_path=state_path,
            tenant_id=_TENANT,
            bundle_path=bundle_path,
            expected_bundle_digest=bundle_digest,
            expected_target_state_digest=str(target_digest),
            expected_notification_target_state_digest=str(
                target_state["notificationTargetStateDigest"]
            ),
            sms_outbox_path=tmp_path / "sms-outbox.json",
            expected_sms_state_digest=str(
                _strict_sms_state(tmp_path / "sms-outbox.json")[
                    "stateDigest"
                ]
            ),
            pending_decision="preserve",
        )

    assert JsonAutomationCycleStateStore(state_path).load(_state_key()) == {}


def test_forward_bundle_empty_target_digest_drift_does_not_merge(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "automation.json"
    bundle_path = tmp_path / "forward.bundle.json"
    seed = _legacy_health_state(enabled=True)
    bundle_digest = create_protected_json_file(
        bundle_path,
        {
            "schema": "boxer.device_health_state_bundle",
            "version": 1,
            "direction": "slack_to_api",
            "exportedAt": "2026-08-20T01:00:00+00:00",
            "sourceDigest": "1" * 64,
            "payload": {
                "alertDeliveryEnabled": True,
                "alertFingerprints": seed["alertFingerprints"],
                "pendingAlertFingerprints": seed[
                    "pendingAlertFingerprints"
                ],
                "notificationState": _legacy_notification_state(),
            },
            "safety": {
                "notificationPendingEventCount": 0,
                "notificationStateDigest": "2" * 64,
                "smsOutboxItemCount": 0,
                "smsUnresolvedClaimCount": 0,
                "activeSettledClaimCount": 0,
                "smsStateDigest": "3" * 64,
            },
        },
        label="test forward bundle",
    )
    empty_target_state = inspect_device_health_monitor_state(
        state_path=state_path,
        tenant_id=_TENANT,
    )
    empty_target_digest = empty_target_state["targetStateDigest"]
    store = JsonAutomationCycleStateStore(state_path)
    existing = {"manualMarker": {"keep": True}}
    store.save(_state_key(), existing)

    with pytest.raises(AutomationRecoveryError, match="not exact empty"):
        import_device_health_monitor_forward_bundle(
            state_path=state_path,
            tenant_id=_TENANT,
            bundle_path=bundle_path,
            expected_bundle_digest=bundle_digest,
            expected_target_state_digest=str(empty_target_digest),
            expected_notification_target_state_digest=str(
                empty_target_state["notificationTargetStateDigest"]
            ),
            sms_outbox_path=tmp_path / "sms-outbox.json",
            expected_sms_state_digest=str(
                _strict_sms_state(tmp_path / "sms-outbox.json")[
                    "stateDigest"
                ]
            ),
            pending_decision="preserve",
        )

    assert store.load(_state_key()) == existing


def test_forward_import_requires_canonical_initialized_drained_sms_target(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "automation.json"
    bundle_path = tmp_path / "forward.bundle.json"
    bundle_digest = _create_forward_bundle(bundle_path)
    target = inspect_device_health_monitor_state(
        state_path=state_path,
        tenant_id=_TENANT,
    )
    sms_outbox = tmp_path / "sms-outbox.json"

    assert claim_automatic_sms_delivery(
        "MB2-C00419",
        "video_signal",
        claimed_at=_NOW,
        outbox_path=sms_outbox,
    )
    unresolved = _strict_sms_state(sms_outbox, now=_NOW)
    with pytest.raises(AutomationRecoveryError, match="not exact drained"):
        import_device_health_monitor_forward_bundle(
            state_path=state_path,
            tenant_id=_TENANT,
            bundle_path=bundle_path,
            expected_bundle_digest=bundle_digest,
            expected_target_state_digest=str(target["targetStateDigest"]),
            expected_notification_target_state_digest=str(
                target["notificationTargetStateDigest"]
            ),
            sms_outbox_path=sms_outbox,
            expected_sms_state_digest=str(unresolved["stateDigest"]),
            pending_decision="preserve",
            now=_NOW,
        )
    assert JsonAutomationCycleStateStore(state_path).load(_state_key()) == {}

    # configured path가 아닌 별도 initialized 파일도 clean target으로 쓰지 않는다.
    alternate = tmp_path / "alternate-sms.json"
    _write_automation_document(alternate, {"version": 1, "items": []})
    _write_automation_document(
        alternate.with_name(f"{alternate.name}.automatic-claims.json"),
        {"version": 2, "claims": {}},
    )
    alternate_state = inspect_automatic_sms_recovery_state(
        outbox_path=alternate,
        expected_outbox_path=alternate,
        require_initialized=True,
        now=_NOW,
    )
    with pytest.raises(AutomationRecoveryError, match="unreadable"):
        import_device_health_monitor_forward_bundle(
            state_path=state_path,
            tenant_id=_TENANT,
            bundle_path=bundle_path,
            expected_bundle_digest=bundle_digest,
            expected_target_state_digest=str(target["targetStateDigest"]),
            expected_notification_target_state_digest=str(
                target["notificationTargetStateDigest"]
            ),
            sms_outbox_path=alternate,
            expected_sms_state_digest=str(alternate_state["stateDigest"]),
            pending_decision="preserve",
            now=_NOW,
        )
    assert JsonAutomationCycleStateStore(state_path).load(_state_key()) == {}


def test_forward_import_rechecks_sms_target_inside_and_after_cycle_cas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bundle_path = tmp_path / "forward.bundle.json"
    bundle_digest = _create_forward_bundle(bundle_path)
    sms_outbox = tmp_path / "sms-outbox.json"
    clean_sms = _strict_sms_state(sms_outbox, now=_NOW)
    real_inspector = automation_recovery._inspect_strict_sms_recovery_state

    for drift_call, expect_seeded in ((2, False), (3, True)):
        state_path = tmp_path / f"automation-{drift_call}.json"
        target = inspect_device_health_monitor_state(
            state_path=state_path,
            tenant_id=_TENANT,
        )
        calls = 0

        def _drifting_inspector(
            path: str | Path,
            *,
            now: datetime | None = None,
        ) -> dict[str, object]:
            nonlocal calls
            calls += 1
            state = dict(real_inspector(path, now=now))
            if calls == drift_call:
                state["stateDigest"] = "f" * 64
            return state

        monkeypatch.setattr(
            automation_recovery,
            "_inspect_strict_sms_recovery_state",
            _drifting_inspector,
        )
        with pytest.raises(AutomationRecoveryError, match="not exact drained"):
            import_device_health_monitor_forward_bundle(
                state_path=state_path,
                tenant_id=_TENANT,
                bundle_path=bundle_path,
                expected_bundle_digest=bundle_digest,
                expected_target_state_digest=str(target["targetStateDigest"]),
                expected_notification_target_state_digest=str(
                    target["notificationTargetStateDigest"]
                ),
                sms_outbox_path=sms_outbox,
                expected_sms_state_digest=str(clean_sms["stateDigest"]),
                pending_decision="preserve",
                now=_NOW,
            )
        inspected = inspect_device_health_monitor_state(
            state_path=state_path,
            tenant_id=_TENANT,
        )
        # CAS 직전 drift는 write 0, 직후 drift는 성공 보고를 막고 수동
        # 확인이 필요한 seeded fail-closed 상태를 보존한다.
        assert inspected["seeded"] is expect_seeded
        monkeypatch.setattr(
            automation_recovery,
            "_inspect_strict_sms_recovery_state",
            real_inspector,
        )


def test_forward_import_seeds_health_and_notification_in_one_document_write(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "automation.json"
    bundle_path = tmp_path / "forward.bundle.json"
    seed = _legacy_health_state(enabled=True)
    bundle_digest = create_protected_json_file(
        bundle_path,
        {
            "schema": "boxer.device_health_state_bundle",
            "version": 1,
            "direction": "slack_to_api",
            "exportedAt": "2026-08-20T01:00:00+00:00",
            "sourceDigest": "1" * 64,
            "payload": {
                "alertDeliveryEnabled": True,
                "alertFingerprints": seed["alertFingerprints"],
                "pendingAlertFingerprints": seed[
                    "pendingAlertFingerprints"
                ],
                "notificationState": _legacy_notification_state(),
            },
            "safety": {
                "notificationPendingEventCount": 0,
                "notificationStateDigest": "2" * 64,
                "smsOutboxItemCount": 0,
                "smsUnresolvedClaimCount": 0,
                "activeSettledClaimCount": 0,
                "smsStateDigest": "3" * 64,
            },
        },
        label="test forward bundle",
    )
    empty = inspect_device_health_monitor_state(
        state_path=state_path,
        tenant_id=_TENANT,
    )
    store = JsonAutomationCycleStateStore(state_path)
    store.save(_notification_state_key(), {"manualMarker": True})

    with pytest.raises(AutomationRecoveryError, match="targets are not exact empty"):
        import_device_health_monitor_forward_bundle(
            state_path=state_path,
            tenant_id=_TENANT,
            bundle_path=bundle_path,
            expected_bundle_digest=bundle_digest,
            expected_target_state_digest=str(empty["targetStateDigest"]),
            expected_notification_target_state_digest=str(
                empty["notificationTargetStateDigest"]
            ),
            sms_outbox_path=tmp_path / "sms-outbox.json",
            expected_sms_state_digest=str(
                _strict_sms_state(tmp_path / "sms-outbox.json")[
                    "stateDigest"
                ]
            ),
            pending_decision="preserve",
        )

    # notification precondition이 어긋나면 health만 먼저 seed하지 않는다.
    inspected = inspect_device_health_monitor_state(
        state_path=state_path,
        tenant_id=_TENANT,
    )
    assert inspected["seeded"] is False
    assert store.load(_notification_state_key()) == {"manualMarker": True}


def test_retry_clears_only_exact_inflight_marker(tmp_path: Path) -> None:
    path = tmp_path / "automation.json"
    store = JsonAutomationCycleStateStore(path)
    store.save(
        _state_key(),
        {
            "cursor": {"lastRunAt": "before"},
            "inFlight": {
                "requestId": _REQUEST_ID,
                "startedAt": "2026-08-14T01:00:00+00:00",
            },
        },
    )

    result = resolve_automation_uncertain_state(
        state_path=path,
        tenant_id=_TENANT,
        cycle=_CYCLE,
        cycle_key=_CYCLE_KEY,
        expected_request_id=_REQUEST_ID,
        decision="retry",
        now=datetime(2026, 8, 14, 2, 0, tzinfo=timezone.utc),
    )

    state = store.load(_state_key())
    assert result["marker"] == "inFlight"
    assert "inFlight" not in state
    assert state["cursor"] == {"lastRunAt": "before"}
    assert state["manualResolutions"][0]["requestHash"]
    assert _REQUEST_ID not in str(result)


def test_assume_completed_ack_closes_matching_delivery_context(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    store = JsonAutomationCycleStateStore(path)
    store.save(
        _state_key(),
        {
            "cursor": {
                "pendingSheetAlerts": {
                    "delivery:1": {"private": "state"},
                    "delivery:2": {"private": "state"},
                }
            },
            "pendingDeliveries": [
                {"deliveryId": "delivery:1", "kind": "alert", "payload": {}},
                {"deliveryId": "delivery:2", "kind": "alert", "payload": {}},
            ],
            "domainCycleComplete": False,
            "ackInFlight": {
                "requestId": _REQUEST_ID,
                "deliveryIds": ["delivery:1"],
            },
        },
    )

    resolve_automation_uncertain_state(
        state_path=path,
        tenant_id=_TENANT,
        cycle=_CYCLE,
        cycle_key=_CYCLE_KEY,
        expected_request_id=_REQUEST_ID,
        decision="assume_completed",
    )

    state = store.load(_state_key())
    assert "ackInFlight" not in state
    assert [item["deliveryId"] for item in state["pendingDeliveries"]] == [
        "delivery:2"
    ]
    assert state["acknowledgedDeliveryIds"] == ["delivery:1"]
    assert set(state["cursor"]["pendingSheetAlerts"]) == {"delivery:2"}


def test_resolution_fails_closed_for_wrong_request_or_marker_shape(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    store = JsonAutomationCycleStateStore(path)
    store.save(
        _state_key(),
        {
            "inFlight": {"requestId": _REQUEST_ID},
            "ackInFlight": {"requestId": _REQUEST_ID},
        },
    )

    with pytest.raises(AutomationRecoveryError):
        resolve_automation_uncertain_state(
            state_path=path,
            tenant_id=_TENANT,
            cycle=_CYCLE,
            cycle_key=_CYCLE_KEY,
            expected_request_id="automation:wrong",
            decision="retry",
        )


def test_inflight_cannot_be_assumed_completed_without_cursor_recovery(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    store = JsonAutomationCycleStateStore(path)
    store.save(
        _state_key(),
        {"inFlight": {"requestId": _REQUEST_ID}},
    )

    with pytest.raises(AutomationRecoveryError):
        resolve_automation_uncertain_state(
            state_path=path,
            tenant_id=_TENANT,
            cycle=_CYCLE,
            cycle_key=_CYCLE_KEY,
            expected_request_id=_REQUEST_ID,
            decision="assume_completed",
        )

    assert store.load(_state_key())["inFlight"]["requestId"] == _REQUEST_ID


def test_window_inflight_also_cannot_be_assumed_completed(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"
    tenant = _TENANT
    cycle = "daily_device_round"
    cycle_key = "2026-08-14"
    raw = "\0".join((tenant, cycle, cycle_key))
    state_key = hashlib.sha256(raw.encode("utf-8")).hexdigest()
    store = JsonAutomationCycleStateStore(path)
    store.save(
        state_key,
        {"inFlight": {"requestId": _REQUEST_ID}},
    )

    with pytest.raises(AutomationRecoveryError):
        resolve_automation_uncertain_state(
            state_path=path,
            tenant_id=tenant,
            cycle=cycle,
            cycle_key=cycle_key,
            expected_request_id=_REQUEST_ID,
            decision="assume_completed",
        )

    state = store.load(state_key)
    assert state["inFlight"]["requestId"] == _REQUEST_ID


def test_sms_recovery_settles_only_exact_claim_after_verification(
    tmp_path: Path,
) -> None:
    outbox = tmp_path / "sms-outbox.json"
    now = datetime(2026, 8, 14, 2, 0, tzinfo=timezone.utc)
    assert claim_automatic_sms_delivery(
        "MB2-C00419",
        "video_signal",
        claimed_at=now,
        outbox_path=outbox,
    )
    assert claim_automatic_sms_delivery(
        "MB2-C00570",
        "audio",
        claimed_at=now,
        outbox_path=outbox,
    )

    result = resolve_automatic_sms_uncertain_claim(
        outbox_path=outbox,
        device_name="MB2-C00419",
        alert_category="video_signal",
        now=now,
    )

    assert result["previousState"] == "pending"
    assert result["state"] == "settled"
    assert "MB2-C00419" not in str(result)
    assert claim_automatic_sms_delivery(
        "MB2-C00419",
        "video_signal",
        claimed_at=now.replace(minute=1),
        outbox_path=outbox,
    )
    assert not claim_automatic_sms_delivery(
        "MB2-C00570",
        "audio",
        claimed_at=now.replace(day=15),
        outbox_path=outbox,
    )


def test_sms_recovery_cli_rejects_running_service_edit(tmp_path: Path) -> None:
    outbox = tmp_path / "sms-outbox.json"

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--sms-outbox-path",
                str(outbox),
                "--sms-device-name",
                "MB2-C00419",
                "--sms-alert-category",
                "video_signal",
                "--confirm-provider-verified",
            ]
        )

    assert str(exc_info.value) == (
        "automation_recovery_requires_stopped_service"
    )


def test_accepted_sms_claim_requires_explicit_recovery_opt_in(
    tmp_path: Path,
) -> None:
    outbox = tmp_path / "sms-outbox.json"
    now = datetime(2026, 8, 14, 2, 0, tzinfo=timezone.utc)
    assert claim_automatic_sms_delivery(
        "MB2-C00419",
        "video_signal",
        claimed_at=now,
        outbox_path=outbox,
    )
    assert hold_automatic_sms_delivery_claim(
        "MB2-C00419",
        "video_signal",
        held_at=now,
        state="accepted",
        group_id="group-accepted",
        outbox_path=outbox,
    )

    with pytest.raises(AutomationRecoveryError):
        resolve_automatic_sms_uncertain_claim(
            outbox_path=outbox,
            device_name="MB2-C00419",
            alert_category="video_signal",
            now=now,
        )

    resolved = resolve_automatic_sms_uncertain_claim(
        outbox_path=outbox,
        device_name="MB2-C00419",
        alert_category="video_signal",
        allow_accepted=True,
        now=now,
    )
    assert resolved["previousState"] == "accepted"
