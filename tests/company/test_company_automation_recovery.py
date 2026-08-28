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
    initialize_clean_automation_state,
    inspect_device_health_monitor_state,
    main,
    override_device_health_monitor_alert_delivery,
    resolve_automatic_sms_uncertain_claim,
    resolve_automation_uncertain_state,
)
from boxer_company.sms_delivery_cycle import (
    claim_automatic_sms_delivery,
    hold_automatic_sms_delivery_claim,
    inspect_automatic_sms_recovery_state,
)


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
    return outbox


def _state_key() -> str:
    raw = "\0".join((_TENANT, _CYCLE, _CYCLE_KEY))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _replace_cycle_state(
    store: JsonAutomationCycleStateStore,
    key: str,
    state: dict[str, object],
) -> None:
    """복구 fixture도 runtime과 같은 atomic cycle 교체 경로를 쓴다."""

    store.mutate_cycle(
        key,
        lambda _exists, _current: (state, None),
    )


def _write_automation_document(path: Path, payload: object) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o600)


def test_clean_initializer_creates_one_verified_health_state_once(
    tmp_path: Path,
) -> None:
    path = tmp_path / "automation.json"

    initialized = initialize_clean_automation_state(
        state_path=path,
        tenant_id=_TENANT,
        initial_alert_delivery_enabled=False,
        now=_NOW,
    )

    assert initialized["initialized"] is True
    assert initialized["alertDeliveryEnabled"] is False
    assert initialized["alertFingerprintCount"] == 0
    assert initialized["pendingFingerprintCount"] == 0
    assert (path.stat().st_mode & 0o777) == 0o600
    document = json.loads(path.read_text(encoding="utf-8"))
    assert set(document) == {"version", "cycles"}
    assert set(document["cycles"]) == {_state_key()}
    inspected = inspect_device_health_monitor_state(
        state_path=path,
        tenant_id=_TENANT,
    )
    assert inspected["seeded"] is True
    assert inspected["cursorDigest"] == initialized["cursorDigest"]

    previous = path.read_bytes()
    with pytest.raises(AutomationRecoveryError, match="already exists"):
        initialize_clean_automation_state(
            state_path=path,
            tenant_id=_TENANT,
            initial_alert_delivery_enabled=True,
            now=_NOW,
        )
    assert path.read_bytes() == previous


def test_clean_initializer_cli_requires_stopped_service_and_explicit_alert_mode(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    path = tmp_path / "automation.json"
    args = [
        "--state-path",
        str(path),
        "--tenant-id",
        _TENANT,
        "--initialize-clean-automation-state",
        "--initial-alert-delivery-enabled",
        "true",
    ]
    with pytest.raises(SystemExit, match="requires_stopped_service"):
        main(args)

    assert main([*args, "--confirm-service-stopped"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["initialized"] is True
    assert payload["alertDeliveryEnabled"] is True
    help_text = automation_recovery._build_parser().format_help()
    assert "--initialize-clean-automation-state" in help_text
    assert "--import-device-health-forward-bundle-path" not in help_text


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
    _replace_cycle_state(store, _state_key(), {})

    with pytest.raises(AutomationRecoveryError, match="invalid"):
        inspect_device_health_monitor_state(
            state_path=path,
            tenant_id=_TENANT,
        )
    with pytest.raises(AutomationRecoveryError, match="already exists"):
        initialize_clean_automation_state(
            state_path=path,
            tenant_id=_TENANT,
            initial_alert_delivery_enabled=False,
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
            _replace_cycle_state(second, other_key, {"runtimeWrite": True}),
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
    seeded = initialize_clean_automation_state(
        state_path=path,
        tenant_id=_TENANT,
        initial_alert_delivery_enabled=False,
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
    _replace_cycle_state(store, _state_key(), state)
    with pytest.raises(AutomationRecoveryError, match="pending deliveries"):
        override_device_health_monitor_alert_delivery(
            state_path=path,
            tenant_id=_TENANT,
            expected_cursor_digest=str(updated["cursorDigest"]),
            expected_enabled=True,
            enabled=False,
        )


def test_retry_clears_only_exact_inflight_marker(tmp_path: Path) -> None:
    path = tmp_path / "automation.json"
    store = JsonAutomationCycleStateStore(path)
    _replace_cycle_state(
        store,
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
    _replace_cycle_state(
        store,
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
    _replace_cycle_state(
        store,
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
    _replace_cycle_state(
        store,
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
    _replace_cycle_state(
        store,
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
