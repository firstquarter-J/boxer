from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import stat
from typing import Mapping

import pytest

from boxer_company.device_health_state_bundle import (
    build_device_notification_api_cursor,
    DeviceHealthStateBundleError,
)
from boxer_company.sms_delivery_cycle import (
    inspect_automatic_sms_recovery_state,
)
from boxer_company_adapter_slack import device_health_state_migration
from boxer_company_adapter_slack.device_health_state_migration import (
    export_device_health_forward_bundle,
    inspect_slack_device_health_state,
    main,
)


_NOW = datetime(2026, 8, 20, 1, 0, tzinfo=timezone.utc)
_OBSERVED_AT = "2026-08-20T09:00:00+09:00"


def _fingerprints() -> tuple[dict[str, object], dict[str, object]]:
    alerts = {
        "#69 수지미래산부인과|1진료실|MB2-C1|LED 연결 오류": {
            "firstAlertedAt": _OBSERVED_AT,
            "lastAlertedAt": _OBSERVED_AT,
            "lastSeenAt": _OBSERVED_AT,
            "count": 2,
        }
    }
    pending = {
        "수지미래산부인과 (#69)|2진료실|MB2-C2|캡처보드 연결 오류": {
            "firstSeenAt": _OBSERVED_AT,
            "lastSeenAt": _OBSERVED_AT,
            "count": 1,
        }
    }
    return alerts, pending


def _write_protected(path: Path, payload: object) -> str:
    path.write_text(
        json.dumps(payload, ensure_ascii=True, sort_keys=True),
        encoding="utf-8",
    )
    os.chmod(path, 0o600)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _legacy_state() -> dict[str, object]:
    alerts, pending = _fingerprints()
    return {
        "unrelatedLocalCursor": {"keep": True},
        "alertDeliveryOverride": {
            "enabled": False,
            "updatedAt": _OBSERVED_AT,
            "updatedBy": "slack_admin",
        },
        "alertFingerprints": alerts,
        "pendingAlertFingerprints": pending,
    }


def _legacy_notification_state() -> dict[str, object]:
    """실제 local writer가 저장하는 drained notification raw schema다."""

    recording_key = "MB2-C1|file-1|barcode-1|recording"
    return {
        "initialized": True,
        "initializedAt": _OBSERVED_AT,
        "lastSeenId": 12,
        "lastPolledAt": _OBSERVED_AT,
        "pendingEvents": [],
        "recentCaptureboardAlerts": {
            "MB2-C1": {
                "lastAlertedAt": _OBSERVED_AT,
                "notificationId": 12,
            }
        },
        "recordingStallIncidents": {
            recording_key: {
                "phase": "alerted",
                "deviceName": "MB2-C1",
                "barcode": "barcode-1",
                "fileId": "file-1",
                "fileType": "recording",
                "currentStatus": "recording",
                "firstNotificationId": 11,
                "firstOccurredAt": _OBSERVED_AT,
                "firstDurationSeconds": 120,
                "lastNotificationId": 11,
                "lastOccurredAt": _OBSERVED_AT,
                "lastDurationSeconds": 120,
                "lastCurrentSize": 1_024,
                "slackMessageTs": "1710000000.000100",
                "slackPermalink": "https://example.slack.com/archives/C1/p1",
                "lastCommentNotificationId": None,
            }
        },
        "captureboardIncidents": {
            "MB2-C1": {
                "deviceName": "MB2-C1",
                "deviceSeq": 1,
                "status": "대기",
                "slackMessageTs": "1710000000.000100",
                "slackPermalink": "https://example.slack.com/archives/C1/p1",
                "rowNumber": 2,
                "openedNotificationId": 12,
                "openedCode": "captureboard_connection_error",
                "openedAt": _OBSERVED_AT,
                "lastSheetCheckedAt": _OBSERVED_AT,
                "lastSuppressedAt": "",
                "lastSuppressedNotificationId": None,
                "lastSuppressedCode": "",
                "suppressedCount": 0,
            }
        },
        "captureboardIncidentsLastSheetCheckedAt": _OBSERVED_AT,
        "lastSentAt": _OBSERVED_AT,
        "lastSentNotificationId": 12,
        "lastSlackMessageTs": "1710000000.000100",
        "lastSlackPermalink": "https://example.slack.com/archives/C1/p1",
    }


def _notification_export_args(tmp_path: Path) -> dict[str, object]:
    path = tmp_path / "notification.json"
    digest = _write_protected(path, _legacy_notification_state())
    return {
        "notification_state_path": path,
        "expected_notification_state_digest": digest,
    }


def test_captureboard_quiet_incident_forward_cursor_preserves_activity_state() -> None:
    legacy_state = _legacy_notification_state()
    legacy_state["lastSeenId"] = 15
    incident = legacy_state["captureboardIncidents"]["MB2-C1"]
    # quiet window의 재시작 정본이 forward 중 초기값으로 되돌아가지 않게 한다.
    incident.update(
        {
            "slackMessageTs": "1710000000.000150",
            "slackPermalink": "https://example.slack.com/archives/C1/p150",
            "lastSuppressedAt": "2026-08-20T09:05:00+09:00",
            "lastSuppressedNotificationId": 15,
            "lastSuppressedCode": "captureboard_connection_error",
            "suppressedCount": 3,
        }
    )

    api_cursor = build_device_notification_api_cursor(legacy_state)
    api_incident = api_cursor["captureboardIncidents"]["MB2-C1"]

    assert api_incident["rootExternalMessageId"] == incident["slackMessageTs"]
    assert api_incident["rootPermalink"] == incident["slackPermalink"]
    for field in (
        "lastSuppressedAt",
        "lastSuppressedNotificationId",
        "lastSuppressedCode",
        "suppressedCount",
    ):
        assert api_incident[field] == incident[field]


def _initialize_sms_recovery_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    *,
    items: list[object] | None = None,
    claims: dict[str, object] | None = None,
) -> tuple[Path, dict[str, object]]:
    """실제 env와 같은 initialized/protected SMS 두 파일을 만든다."""

    outbox_path = tmp_path / "sms-outbox.json"
    claim_path = tmp_path / "sms-outbox.json.automatic-claims.json"
    _write_protected(
        outbox_path,
        {"version": 1, "items": list(items or [])},
    )
    _write_protected(
        claim_path,
        {"version": 2, "claims": dict(claims or {})},
    )
    monkeypatch.setattr(
        device_health_state_migration.company_settings,
        "SMS_DELIVERY_OUTBOX_PATH",
        str(outbox_path),
    )
    state = inspect_automatic_sms_recovery_state(
        outbox_path=outbox_path,
        expected_outbox_path=outbox_path,
        require_initialized=True,
    )
    return outbox_path, state


def test_forward_export_uses_live_digest_and_creates_0600_canonical_bundle(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "legacy.json"
    bundle_path = tmp_path / "forward.bundle.json"
    state_digest = _write_protected(state_path, _legacy_state())
    sms_outbox_path, sms_state = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )

    result = export_device_health_forward_bundle(
        state_path=state_path,
        **_notification_export_args(tmp_path),
        bundle_path=bundle_path,
        expected_state_digest=state_digest,
        expected_sms_state_digest=str(sms_state["stateDigest"]),
        sms_outbox_path=sms_outbox_path,
        now=_NOW,
    )

    assert result["exported"] is True
    assert stat.S_IMODE(bundle_path.stat().st_mode) == 0o600
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    assert bundle["direction"] == "slack_to_api"
    assert bundle["safety"] == {
        "notificationPendingEventCount": 0,
        "notificationStateDigest": result["notificationStateDigest"],
        "smsOutboxItemCount": 0,
        "smsUnresolvedClaimCount": 0,
        "activeSettledClaimCount": 0,
        "smsStateDigest": sms_state["stateDigest"],
    }
    assert bundle["payload"]["notificationState"]["lastSeenId"] == 12
    assert all(
        key.startswith("sha256:")
        for key in bundle["payload"]["notificationState"][
            "recordingStallIncidents"
        ]
    )
    assert "barcode-1" not in json.dumps(bundle)
    assert "file-1" not in json.dumps(bundle)
    assert set(bundle["payload"]["pendingAlertFingerprints"]) == {
        "#69 수지미래산부인과|2진료실|MB2-C2|캡처보드 연결 오류"
    }
    assert "MB2-C2" not in json.dumps(result)


def test_slack_bundle_mutation_cli_requires_stopped_service(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "legacy.json"
    state_digest = _write_protected(state_path, _legacy_state())
    bundle_path = tmp_path / "forward.bundle.json"

    with pytest.raises(SystemExit) as exc_info:
        main(
            [
                "--state-path",
                str(state_path),
                "--export-forward-bundle-path",
                str(bundle_path),
                "--expected-state-digest",
                state_digest,
            ]
        )

    assert str(exc_info.value) == (
        "device_health_state_migration_requires_stopped_slack"
    )
    assert not bundle_path.exists()


def test_slack_migration_cli_exposes_forward_direction_only() -> None:
    """Slack host CLI가 API→Slack import를 다시 열지 않게 고정한다."""

    help_text = device_health_state_migration._build_parser().format_help()

    assert "--export-forward-bundle-path" in help_text
    assert "--import-rollback-bundle-path" not in help_text


def test_forward_export_state_drift_and_insecure_parent_write_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "legacy.json"
    state = _legacy_state()
    expected_digest = _write_protected(state_path, state)
    state["unrelatedLocalCursor"] = {"keep": False}
    _write_protected(state_path, state)
    drift_bundle = tmp_path / "drift.bundle.json"
    sms_outbox_path, sms_state = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )

    with pytest.raises(DeviceHealthStateBundleError, match="changed"):
        export_device_health_forward_bundle(
            state_path=state_path,
            **_notification_export_args(tmp_path),
            bundle_path=drift_bundle,
            expected_state_digest=expected_digest,
            expected_sms_state_digest=str(sms_state["stateDigest"]),
            sms_outbox_path=sms_outbox_path,
            now=_NOW,
        )
    assert not drift_bundle.exists()

    current_digest = inspect_slack_device_health_state(
        state_path=state_path,
        notification_state_path=(tmp_path / "notification.json"),
        sms_outbox_path=sms_outbox_path,
    )["stateDigest"]
    insecure_parent = tmp_path / "insecure"
    insecure_parent.mkdir(mode=0o700)
    os.chmod(insecure_parent, 0o777)
    with pytest.raises(DeviceHealthStateBundleError, match="not protected"):
        export_device_health_forward_bundle(
            state_path=state_path,
            **_notification_export_args(tmp_path),
            bundle_path=insecure_parent / "forward.bundle.json",
            expected_state_digest=str(current_digest),
            expected_sms_state_digest=str(sms_state["stateDigest"]),
            sms_outbox_path=sms_outbox_path,
            now=_NOW,
        )
    assert not (insecure_parent / "forward.bundle.json").exists()


def test_forward_export_rejects_unconfigured_or_missing_sms_state_write_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "legacy.json"
    state_digest = _write_protected(state_path, _legacy_state())
    configured_outbox, _ = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )
    alternate_dir = tmp_path / "alternate"
    alternate_dir.mkdir(mode=0o700)
    alternate_outbox, alternate_state = _initialize_sms_recovery_state(
        alternate_dir,
        monkeypatch,
    )
    # helper가 바꿔 놓은 설정을 실제 서비스 경로로 되돌린다.
    monkeypatch.setattr(
        device_health_state_migration.company_settings,
        "SMS_DELIVERY_OUTBOX_PATH",
        str(configured_outbox),
    )
    bundle_path = tmp_path / "alternate.bundle.json"

    with pytest.raises(DeviceHealthStateBundleError, match="state is invalid"):
        export_device_health_forward_bundle(
            state_path=state_path,
            **_notification_export_args(tmp_path),
            bundle_path=bundle_path,
            expected_state_digest=state_digest,
            expected_sms_state_digest=str(alternate_state["stateDigest"]),
            sms_outbox_path=alternate_outbox,
            now=_NOW,
        )
    assert not bundle_path.exists()

    missing_outbox = tmp_path / "missing-sms-outbox.json"
    monkeypatch.setattr(
        device_health_state_migration.company_settings,
        "SMS_DELIVERY_OUTBOX_PATH",
        str(missing_outbox),
    )
    with pytest.raises(DeviceHealthStateBundleError, match="state is invalid"):
        export_device_health_forward_bundle(
            state_path=state_path,
            **_notification_export_args(tmp_path),
            bundle_path=bundle_path,
            expected_state_digest=state_digest,
            expected_sms_state_digest="0" * 64,
            sms_outbox_path=missing_outbox,
            now=_NOW,
        )
    assert not bundle_path.exists()
    assert not missing_outbox.with_name(f"{missing_outbox.name}.lock").exists()


@pytest.mark.parametrize(
    ("items", "claims"),
    (
        (
            [
                {
                    "smsGroupId": "group-1",
                    "smsDeliveryStatus": "accepted",
                    "detectedAt": _OBSERVED_AT,
                }
            ],
            {},
        ),
        (
            [],
            {
                "a" * 64: {
                    "claimedAt": _OBSERVED_AT,
                    "state": "uncertain",
                    "groupHash": "",
                }
            },
        ),
    ),
)
def test_forward_export_requires_exact_drained_sms_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    items: list[object],
    claims: dict[str, object],
) -> None:
    state_path = tmp_path / "legacy.json"
    state_digest = _write_protected(state_path, _legacy_state())
    sms_outbox_path, sms_state = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
        items=items,
        claims=claims,
    )
    bundle_path = tmp_path / "blocked.bundle.json"

    with pytest.raises(DeviceHealthStateBundleError, match="exact drained"):
        export_device_health_forward_bundle(
            state_path=state_path,
            **_notification_export_args(tmp_path),
            bundle_path=bundle_path,
            expected_state_digest=state_digest,
            expected_sms_state_digest=str(sms_state["stateDigest"]),
            sms_outbox_path=sms_outbox_path,
            now=_NOW,
        )

    assert not bundle_path.exists()


def test_forward_export_rejects_sms_digest_drift_write_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "legacy.json"
    state_digest = _write_protected(state_path, _legacy_state())
    sms_outbox_path, original_sms_state = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )
    # settled claim도 safety count는 0이지만 exact revision은 다르다.
    _write_protected(
        tmp_path / "sms-outbox.json.automatic-claims.json",
        {
            "version": 2,
            "claims": {
                "b" * 64: {
                    "claimedAt": _OBSERVED_AT,
                    "state": "settled",
                    "groupHash": "",
                }
            },
        },
    )
    bundle_path = tmp_path / "drifted-sms.bundle.json"

    with pytest.raises(DeviceHealthStateBundleError, match="SMS recovery state changed"):
        export_device_health_forward_bundle(
            state_path=state_path,
            **_notification_export_args(tmp_path),
            bundle_path=bundle_path,
            expected_state_digest=state_digest,
            expected_sms_state_digest=str(original_sms_state["stateDigest"]),
            sms_outbox_path=sms_outbox_path,
            now=_NOW,
        )

    assert not bundle_path.exists()


def test_forward_export_rejects_pending_notification_event_write_zero(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "legacy.json"
    state_digest = _write_protected(state_path, _legacy_state())
    notification_state = _legacy_notification_state()
    notification_state["pendingEvents"] = [{"notificationId": 13}]
    notification_path = tmp_path / "notification.json"
    notification_digest = _write_protected(
        notification_path,
        notification_state,
    )
    sms_outbox_path, sms_state = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )
    bundle_path = tmp_path / "pending-notification.bundle.json"

    with pytest.raises(DeviceHealthStateBundleError, match="not drained"):
        export_device_health_forward_bundle(
            state_path=state_path,
            notification_state_path=notification_path,
            bundle_path=bundle_path,
            expected_state_digest=state_digest,
            expected_notification_state_digest=notification_digest,
            expected_sms_state_digest=str(sms_state["stateDigest"]),
            sms_outbox_path=sms_outbox_path,
            now=_NOW,
        )

    assert not bundle_path.exists()


def test_notification_inspect_rejects_unknown_raw_schema(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = tmp_path / "legacy.json"
    _write_protected(state_path, _legacy_state())
    notification_state = {
        **_legacy_notification_state(),
        "unknownCursor": 1,
    }
    notification_path = tmp_path / "notification.json"
    _write_protected(notification_path, notification_state)
    sms_outbox_path, _ = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )

    with pytest.raises(DeviceHealthStateBundleError, match="schema"):
        inspect_slack_device_health_state(
            state_path=state_path,
            notification_state_path=notification_path,
            sms_outbox_path=sms_outbox_path,
        )


@pytest.mark.parametrize("target", ("outbox", "claims"))
def test_strict_sms_recovery_rejects_boolean_schema_version(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    sms_outbox_path, _ = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )
    target_path = (
        sms_outbox_path
        if target == "outbox"
        else tmp_path / "sms-outbox.json.automatic-claims.json"
    )
    _write_protected(
        target_path,
        (
            {"version": True, "items": []}
            if target == "outbox"
            else {"version": True, "claims": {}}
        ),
    )

    with pytest.raises(ValueError, match="형식이 올바르지 않아"):
        inspect_automatic_sms_recovery_state(
            outbox_path=sms_outbox_path,
            expected_outbox_path=sms_outbox_path,
            require_initialized=True,
        )


@pytest.mark.parametrize(
    ("fingerprint", "count", "message"),
    (
        ("missing-parts", 1, "fingerprint key"),
        ("hospital||device|issue", 1, "fingerprint key"),
        ("hospital|room|device|issue", True, "fingerprint count"),
    ),
)
def test_forward_export_rejects_ambiguous_fingerprint_or_boolean_count(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fingerprint: str,
    count: object,
    message: str,
) -> None:
    state = _legacy_state()
    state["alertFingerprints"] = {
        fingerprint: {
            "firstAlertedAt": _OBSERVED_AT,
            "lastAlertedAt": _OBSERVED_AT,
            "lastSeenAt": _OBSERVED_AT,
            "count": count,
        }
    }
    state_path = tmp_path / "legacy.json"
    state_digest = _write_protected(state_path, state)
    sms_outbox_path, sms_state = _initialize_sms_recovery_state(
        tmp_path,
        monkeypatch,
    )
    bundle_path = tmp_path / "invalid-fingerprint.bundle.json"

    with pytest.raises(DeviceHealthStateBundleError, match=message):
        export_device_health_forward_bundle(
            state_path=state_path,
            **_notification_export_args(tmp_path),
            bundle_path=bundle_path,
            expected_state_digest=state_digest,
            expected_sms_state_digest=str(sms_state["stateDigest"]),
            sms_outbox_path=sms_outbox_path,
            now=_NOW,
        )

    assert not bundle_path.exists()
