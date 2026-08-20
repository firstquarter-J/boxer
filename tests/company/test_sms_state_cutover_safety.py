from __future__ import annotations

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path

import pytest

from boxer_company.device_health_state_bundle import (
    DeviceHealthStateBundleError,
    validate_device_health_state_bundle,
)
from boxer_company.sms_delivery_cycle import (
    inspect_automatic_sms_recovery_state,
)
from boxer_company_adapter_slack import device_health_state_migration
from boxer_company_adapter_slack.device_health_state_migration import (
    export_device_health_forward_bundle,
)


_NOW = datetime(2026, 8, 20, 1, 0, tzinfo=timezone.utc)


def _write_protected(path: Path, payload: object) -> str:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _legacy_health_state() -> dict[str, object]:
    return {
        "alertDeliveryOverride": {
            "enabled": False,
            "updatedAt": "2026-08-20T00:00:00+00:00",
            "updatedBy": "slack_admin",
        },
        "alertFingerprints": {},
        "pendingAlertFingerprints": {},
    }


def _drained_notification_state() -> dict[str, object]:
    return {
        "initialized": False,
        "lastSeenId": 0,
        "pendingEvents": [],
        "recentCaptureboardAlerts": {},
        "recordingStallIncidents": {},
        "captureboardIncidents": {},
        "captureboardIncidentsLastSheetCheckedAt": "",
    }


def test_forward_export_waits_for_settled_claim_window_and_api_validates_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    health_path = tmp_path / "health.json"
    health_digest = _write_protected(health_path, _legacy_health_state())
    notification_path = tmp_path / "notification.json"
    notification_digest = _write_protected(
        notification_path,
        _drained_notification_state(),
    )
    outbox = tmp_path / "sms-outbox.json"
    _write_protected(outbox, {"version": 1, "items": []})
    claim_path = outbox.with_name(f"{outbox.name}.automatic-claims.json")
    _write_protected(
        claim_path,
        {
            "version": 2,
            "claims": {
                "a" * 64: {
                    "claimedAt": (_NOW - timedelta(seconds=59)).isoformat(),
                    "state": "settled",
                    "groupHash": "",
                }
            },
        },
    )
    monkeypatch.setattr(
        device_health_state_migration.company_settings,
        "SMS_DELIVERY_OUTBOX_PATH",
        str(outbox),
    )
    active_sms_state = inspect_automatic_sms_recovery_state(
        outbox_path=outbox,
        expected_outbox_path=outbox,
        require_initialized=True,
        now=_NOW,
    )
    bundle_path = tmp_path / "forward.bundle.json"

    with pytest.raises(DeviceHealthStateBundleError, match="exact drained"):
        export_device_health_forward_bundle(
            state_path=health_path,
            notification_state_path=notification_path,
            bundle_path=bundle_path,
            expected_state_digest=health_digest,
            expected_notification_state_digest=notification_digest,
            expected_sms_state_digest=str(active_sms_state["stateDigest"]),
            sms_outbox_path=outbox,
            now=_NOW,
        )
    assert not bundle_path.exists()

    # stopped-service server clock으로 exact 60초가 지난 뒤에만
    # payload를 넘기지 않고도 cooldown 안전성이 유지된다.
    expired_now = _NOW + timedelta(seconds=1)
    result = export_device_health_forward_bundle(
        state_path=health_path,
        notification_state_path=notification_path,
        bundle_path=bundle_path,
        expected_state_digest=health_digest,
        expected_notification_state_digest=notification_digest,
        expected_sms_state_digest=str(active_sms_state["stateDigest"]),
        sms_outbox_path=outbox,
        now=expired_now,
    )

    assert result["exported"] is True
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert bundle["safety"]["activeSettledClaimCount"] == 0
    tampered = json.loads(json.dumps(bundle))
    tampered["safety"]["activeSettledClaimCount"] = 1
    with pytest.raises(DeviceHealthStateBundleError, match="payload is invalid"):
        validate_device_health_state_bundle(
            tampered,
            direction="slack_to_api",
        )
