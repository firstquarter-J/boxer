from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Mapping, Sequence

from boxer_company import settings as company_settings
from boxer_company.device_health_state_bundle import (
    build_device_health_forward_bundle,
    create_protected_json_file,
    DeviceHealthStateBundleError,
    FILE_DIGEST_PATTERN,
    load_protected_json_file,
    validate_device_notification_legacy_state,
)
from boxer_company.sms_delivery_cycle import (
    inspect_automatic_sms_recovery_state,
)


def inspect_slack_device_health_state(
    *,
    state_path: str | Path,
    notification_state_path: str | Path,
    sms_outbox_path: str | Path | None = None,
) -> Mapping[str, Any]:
    """live health/notification/SMS revision과 안전한 개수만 출력한다."""

    state, state_digest = load_protected_json_file(
        state_path,
        label="Slack device health state",
    )
    if not isinstance(state, Mapping):
        raise DeviceHealthStateBundleError("Slack device health state is invalid")
    notification_state, notification_state_digest = load_protected_json_file(
        notification_state_path,
        label="Slack device notification state",
    )
    notification = validate_device_notification_legacy_state(
        notification_state,
        require_drained=True,
    )
    sms_state = _inspect_configured_sms_recovery_state(
        sms_outbox_path=sms_outbox_path,
    )
    override = state.get("alertDeliveryOverride")
    alerts = state.get("alertFingerprints")
    pending = state.get("pendingAlertFingerprints")
    return {
        "kind": "slack_device_health_state",
        "stateDigest": state_digest,
        "alertDeliveryEnabled": (
            override.get("enabled")
            if isinstance(override, Mapping)
            and type(override.get("enabled")) is bool
            else None
        ),
        "alertFingerprintCount": len(alerts) if isinstance(alerts, Mapping) else 0,
        "pendingFingerprintCount": (
            len(pending) if isinstance(pending, Mapping) else 0
        ),
        "notificationStateDigest": notification_state_digest,
        "notificationInitialized": notification["initialized"],
        "notificationLastSeenId": notification["lastSeenId"],
        "notificationPendingEventCount": 0,
        "notificationRecordingIncidentCount": len(
            notification["recordingStallIncidents"]
        ),
        "notificationCaptureboardIncidentCount": len(
            notification["captureboardIncidents"]
        ),
        "smsStateDigest": sms_state["stateDigest"],
        "smsOutboxItemCount": sms_state["outboxItemCount"],
        "smsUnresolvedClaimCount": sms_state["unresolvedClaimCount"],
        "smsActiveSettledClaimCount": sms_state[
            "activeSettledClaimCount"
        ],
    }


def export_device_health_forward_bundle(
    *,
    state_path: str | Path,
    notification_state_path: str | Path,
    bundle_path: str | Path,
    expected_state_digest: str,
    expected_notification_state_digest: str,
    expected_sms_state_digest: str,
    sms_outbox_path: str | Path | None = None,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """중지된 Slack live state exact revision을 forward bundle로 내보낸다."""

    if (
        not FILE_DIGEST_PATTERN.fullmatch(str(expected_state_digest or ""))
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_notification_state_digest or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_sms_state_digest or "")
        )
    ):
        raise DeviceHealthStateBundleError("Slack state digest is invalid")
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise DeviceHealthStateBundleError("forward export time is invalid")
    state, state_digest = load_protected_json_file(
        state_path,
        label="Slack device health state",
    )
    if not isinstance(state, Mapping) or state_digest != expected_state_digest:
        raise DeviceHealthStateBundleError("Slack device health state changed")
    notification_state, notification_state_digest = load_protected_json_file(
        notification_state_path,
        label="Slack device notification state",
    )
    if notification_state_digest != expected_notification_state_digest:
        raise DeviceHealthStateBundleError(
            "Slack device notification state changed"
        )
    notification = validate_device_notification_legacy_state(
        notification_state,
        require_drained=True,
    )
    sms_state = _inspect_configured_sms_recovery_state(
        sms_outbox_path=sms_outbox_path,
        now=actual_now,
    )
    if sms_state["stateDigest"] != expected_sms_state_digest:
        raise DeviceHealthStateBundleError("Slack SMS recovery state changed")
    if (
        sms_state["outboxItemCount"] != 0
        or sms_state["unresolvedClaimCount"] != 0
        or sms_state["activeSettledClaimCount"] != 0
    ):
        raise DeviceHealthStateBundleError(
            "Slack SMS recovery state is not exact drained"
        )
    bundle = build_device_health_forward_bundle(
        legacy_state=state,
        legacy_notification_state=notification,
        source_state_digest=state_digest,
        notification_source_state_digest=notification_state_digest,
        sms_state_digest=str(sms_state["stateDigest"]),
        exported_at=actual_now,
    )
    # bundle write 직전 source와 SMS 두 revision을 모두 다시 읽어
    # inspect/export 사이 drift를 write 0으로 끝낸다.
    _, final_state_digest = load_protected_json_file(
        state_path,
        label="Slack device health state",
    )
    if final_state_digest != state_digest:
        raise DeviceHealthStateBundleError("Slack device health state changed")
    _, final_notification_state_digest = load_protected_json_file(
        notification_state_path,
        label="Slack device notification state",
    )
    if final_notification_state_digest != notification_state_digest:
        raise DeviceHealthStateBundleError(
            "Slack device notification state changed"
        )
    final_sms_state = _inspect_configured_sms_recovery_state(
        sms_outbox_path=sms_outbox_path,
        now=actual_now,
    )
    if (
        final_sms_state["stateDigest"] != sms_state["stateDigest"]
        or final_sms_state["outboxItemCount"] != 0
        or final_sms_state["unresolvedClaimCount"] != 0
        or final_sms_state["activeSettledClaimCount"] != 0
    ):
        raise DeviceHealthStateBundleError("Slack SMS recovery state changed")
    bundle_digest = create_protected_json_file(
        bundle_path,
        bundle,
        label="device health forward bundle",
    )
    payload = bundle["payload"]
    return {
        "exported": True,
        "kind": "device_health_forward_bundle",
        "sourceStateDigest": state_digest,
        "bundleDigest": bundle_digest,
        "alertDeliveryEnabled": payload["alertDeliveryEnabled"],
        "alertFingerprintCount": len(payload["alertFingerprints"]),
        "pendingFingerprintCount": len(payload["pendingAlertFingerprints"]),
        "notificationStateDigest": notification_state_digest,
        "notificationLastSeenId": notification["lastSeenId"],
        "notificationRecordingIncidentCount": len(
            notification["recordingStallIncidents"]
        ),
        "notificationCaptureboardIncidentCount": len(
            notification["captureboardIncidents"]
        ),
        "smsStateDigest": sms_state["stateDigest"],
    }


def _inspect_configured_sms_recovery_state(
    *,
    sms_outbox_path: str | Path | None,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """서비스 env의 canonical SMS 경로만 initialized recovery 상태로 인정한다."""

    configured_path = str(company_settings.SMS_DELIVERY_OUTBOX_PATH or "").strip()
    if not configured_path:
        raise DeviceHealthStateBundleError(
            "configured Slack SMS outbox path is required"
        )
    actual_path = sms_outbox_path or configured_path
    try:
        state = inspect_automatic_sms_recovery_state(
            outbox_path=actual_path,
            expected_outbox_path=configured_path,
            require_initialized=True,
            now=now,
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        raise DeviceHealthStateBundleError(
            f"Slack SMS recovery state is invalid: {exc}"
        ) from exc
    if (
        not isinstance(state, Mapping)
        or set(state)
        != {
            "stateDigest",
            "outboxItemCount",
            "unresolvedClaimCount",
            "settledClaimCount",
            "activeSettledClaimCount",
        }
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(state.get("stateDigest") or "")
        )
        or any(
            type(state.get(key)) is not int or state.get(key) < 0
            for key in (
                "outboxItemCount",
                "unresolvedClaimCount",
                "settledClaimCount",
                "activeSettledClaimCount",
            )
        )
    ):
        raise DeviceHealthStateBundleError(
            "Slack SMS recovery state is invalid"
        )
    return state


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect or export device health state on the Slack host."
    )
    parser.add_argument(
        "--state-path",
        default=os.getenv("DEVICE_HEALTH_MONITOR_STATE_PATH", ""),
    )
    parser.add_argument(
        "--notification-state-path",
        default=company_settings.DEVICE_NOTIFICATION_ALERT_STATE_PATH,
    )
    parser.add_argument(
        "--sms-outbox-path",
        default=company_settings.SMS_DELIVERY_OUTBOX_PATH,
    )
    parser.add_argument("--inspect-state", action="store_true")
    parser.add_argument("--export-forward-bundle-path")
    parser.add_argument("--expected-state-digest")
    parser.add_argument("--expected-notification-state-digest")
    parser.add_argument("--expected-sms-state-digest")
    parser.add_argument(
        "--confirm-slack-service-stopped",
        action="store_true",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    modes = sum(
        bool(value)
        for value in (
            args.inspect_state,
            args.export_forward_bundle_path,
        )
    )
    if modes != 1:
        raise SystemExit("device_health_state_migration_mode_invalid")
    if not args.inspect_state and not args.confirm_slack_service_stopped:
        raise SystemExit("device_health_state_migration_requires_stopped_slack")
    try:
        if args.inspect_state:
            if not args.state_path:
                raise DeviceHealthStateBundleError("Slack state path is required")
            result = inspect_slack_device_health_state(
                state_path=args.state_path,
                notification_state_path=args.notification_state_path,
                sms_outbox_path=args.sms_outbox_path,
            )
        else:
            if not all(
                (
                    args.state_path,
                    args.notification_state_path,
                    args.expected_state_digest,
                    args.expected_notification_state_digest,
                    args.expected_sms_state_digest,
                )
            ):
                raise DeviceHealthStateBundleError(
                    "forward export confirmation is incomplete"
                )
            result = export_device_health_forward_bundle(
                state_path=args.state_path,
                notification_state_path=args.notification_state_path,
                bundle_path=args.export_forward_bundle_path,
                expected_state_digest=args.expected_state_digest,
                expected_notification_state_digest=(
                    args.expected_notification_state_digest
                ),
                expected_sms_state_digest=args.expected_sms_state_digest,
                sms_outbox_path=args.sms_outbox_path,
            )
    except DeviceHealthStateBundleError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "export_device_health_forward_bundle",
    "inspect_slack_device_health_state",
    "main",
]
