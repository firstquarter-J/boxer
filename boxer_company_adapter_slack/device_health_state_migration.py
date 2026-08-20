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
    replace_protected_json_file,
    validate_device_notification_legacy_state,
    validate_device_health_state_bundle,
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


def _inspect_exact_drained_sms_recovery_target(
    *,
    sms_outbox_path: str | Path,
    expected_state_digest: str,
    now: datetime,
) -> Mapping[str, Any]:
    """Slack runtime의 canonical SMS revision과 세 drain 조건을 묶는다."""

    state = _inspect_configured_sms_recovery_state(
        sms_outbox_path=sms_outbox_path,
        now=now,
    )
    if (
        state["stateDigest"] != expected_state_digest
        or state["outboxItemCount"] != 0
        or state["unresolvedClaimCount"] != 0
        or state["activeSettledClaimCount"] != 0
    ):
        raise DeviceHealthStateBundleError(
            "Slack rollback import SMS target is not exact drained"
        )
    return state


def import_device_health_rollback_bundle(
    *,
    bundle_path: str | Path,
    expected_bundle_digest: str,
    state_path: str | Path,
    expected_state_digest: str,
    notification_state_path: str | Path,
    expected_notification_state_digest: str,
    sms_outbox_path: str | Path,
    expected_sms_state_digest: str,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """검증된 API rollback bundle을 exact Slack state와 SMS target에 import한다."""

    if (
        not FILE_DIGEST_PATTERN.fullmatch(str(expected_bundle_digest or ""))
        or not FILE_DIGEST_PATTERN.fullmatch(str(expected_state_digest or ""))
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_notification_state_digest or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_sms_state_digest or "")
        )
        or not str(sms_outbox_path or "").strip()
    ):
        raise DeviceHealthStateBundleError("rollback import digest is invalid")
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise DeviceHealthStateBundleError("rollback import time is invalid")
    raw_bundle, bundle_digest = load_protected_json_file(
        bundle_path,
        label="device health rollback bundle",
    )
    if bundle_digest != expected_bundle_digest:
        raise DeviceHealthStateBundleError("rollback bundle changed")
    bundle = validate_device_health_state_bundle(
        raw_bundle,
        direction="api_to_slack",
    )
    # import 시작 전에 실제 Slack runtime이 쓸 canonical SMS 두 파일이
    # initialized/protected/drained인지 exact digest로 먼저 확정한다.
    _inspect_exact_drained_sms_recovery_target(
        sms_outbox_path=sms_outbox_path,
        expected_state_digest=expected_sms_state_digest,
        now=actual_now,
    )
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
    current_notification = validate_device_notification_legacy_state(
        notification_state,
        require_drained=True,
    )
    payload = bundle["payload"]
    existing_marker = state.get("apiRollbackMigration")
    raw_notification_marker = (
        notification_state.get("apiRollbackMigration")
        if isinstance(notification_state, Mapping)
        else None
    )
    notification_matches = (
        current_notification == payload["notificationState"]
        and isinstance(raw_notification_marker, Mapping)
        and raw_notification_marker.get("bundleDigest") == bundle_digest
    )
    health_matches = (
        state.get("alertDeliveryOverride") == payload["alertDeliveryOverride"]
        and state.get("alertFingerprints") == payload["alertFingerprints"]
        and state.get("pendingAlertFingerprints")
        == payload["pendingAlertFingerprints"]
        and isinstance(existing_marker, Mapping)
        and existing_marker.get("bundleDigest") == bundle_digest
    )
    if (
        health_matches
        and notification_matches
    ):
        final_sms_state = _inspect_exact_drained_sms_recovery_target(
            sms_outbox_path=sms_outbox_path,
            expected_state_digest=expected_sms_state_digest,
            now=actual_now,
        )
        return {
            "updated": False,
            "kind": "device_health_rollback_import",
            "bundleDigest": bundle_digest,
            "slackStateDigest": state_digest,
            "slackNotificationStateDigest": notification_state_digest,
            "targetSmsStateDigest": final_sms_state["stateDigest"],
            "pendingDecision": payload["pendingDecision"],
        }
    if health_matches and not notification_matches:
        # health marker는 transaction의 마지막 commit marker다. 이것만 먼저
        # 존재하면 중지 전 partial import 여부를 안전하게 판정할 수 없다.
        raise DeviceHealthStateBundleError(
            "Slack rollback state is inconsistent"
        )

    next_notification_state_digest = notification_state_digest
    if not notification_matches:
        next_notification_state = {
            **payload["notificationState"],
            "apiRollbackMigration": {
                "version": 1,
                "migratedAt": actual_now.astimezone(timezone.utc).isoformat(),
                "bundleDigest": bundle_digest,
                "apiNotificationStateDigest": bundle["safety"][
                    "notificationStateDigest"
                ],
                "previousSlackStateDigest": notification_state_digest,
            },
        }
        # notification을 먼저 commit하고 health marker를 마지막에 써서 중간
        # 종료도 stopped-service 상태에서 같은 bundle로 재개할 수 있게 한다.
        _, final_bundle_digest = load_protected_json_file(
            bundle_path,
            label="device health rollback bundle",
        )
        if final_bundle_digest != bundle_digest:
            raise DeviceHealthStateBundleError("rollback bundle changed")
        # notification CAS 직전에 SMS target을 다시 확인해 partial import가
        # 다른 provider revision과 결합되지 않게 한다.
        _inspect_exact_drained_sms_recovery_target(
            sms_outbox_path=sms_outbox_path,
            expected_state_digest=expected_sms_state_digest,
            now=actual_now,
        )
        next_notification_state_digest = replace_protected_json_file(
            notification_state_path,
            next_notification_state,
            expected_existing_digest=notification_state_digest,
            label="Slack device notification state",
        )
        try:
            _inspect_exact_drained_sms_recovery_target(
                sms_outbox_path=sms_outbox_path,
                expected_state_digest=expected_sms_state_digest,
                now=actual_now,
            )
        except DeviceHealthStateBundleError as exc:
            # notification은 이미 CAS됐으므로 write 0처럼 숨기지 않는다.
            # Slack은 계속 중지한 채 같은 bundle의 partial marker를 수동 검토한다.
            raise DeviceHealthStateBundleError(
                "Slack rollback notification state was applied; "
                "SMS target requires manual verification"
            ) from exc
    next_state = {
        **dict(state),
        "alertDeliveryOverride": payload["alertDeliveryOverride"],
        "alertFingerprints": payload["alertFingerprints"],
        "pendingAlertFingerprints": payload["pendingAlertFingerprints"],
        "apiRollbackMigration": {
            "version": 1,
            "migratedAt": actual_now.astimezone(timezone.utc).isoformat(),
            "bundleDigest": bundle_digest,
            "apiCursorDigest": bundle["sourceDigest"],
            "previousSlackStateDigest": state_digest,
            "pendingDecision": payload["pendingDecision"],
        },
    }
    # health commit marker 직전에도 bundle bytes를 다시 확인한다.
    _, final_bundle_digest = load_protected_json_file(
        bundle_path,
        label="device health rollback bundle",
    )
    if final_bundle_digest != bundle_digest:
        raise DeviceHealthStateBundleError("rollback bundle changed")
    _inspect_exact_drained_sms_recovery_target(
        sms_outbox_path=sms_outbox_path,
        expected_state_digest=expected_sms_state_digest,
        now=actual_now,
    )
    next_state_digest = replace_protected_json_file(
        state_path,
        next_state,
        expected_existing_digest=state_digest,
        label="Slack device health state",
    )
    try:
        final_sms_state = _inspect_exact_drained_sms_recovery_target(
            sms_outbox_path=sms_outbox_path,
            expected_state_digest=expected_sms_state_digest,
            now=actual_now,
        )
    except DeviceHealthStateBundleError as exc:
        # health commit marker까지 기록됐으므로 성공으로 축약하지 않고
        # 적용 완료 상태 그대로 서비스 중지·수동 확인을 요구한다.
        raise DeviceHealthStateBundleError(
            "Slack rollback state was applied; "
            "SMS target requires manual verification"
        ) from exc
    return {
        "updated": True,
        "kind": "device_health_rollback_import",
        "bundleDigest": bundle_digest,
        "previousSlackStateDigest": state_digest,
        "slackStateDigest": next_state_digest,
        "previousSlackNotificationStateDigest": notification_state_digest,
        "slackNotificationStateDigest": next_notification_state_digest,
        "targetSmsStateDigest": final_sms_state["stateDigest"],
        "pendingDecision": payload["pendingDecision"],
        "alertFingerprintCount": len(payload["alertFingerprints"]),
        "pendingFingerprintCount": len(payload["pendingAlertFingerprints"]),
        "notificationLastSeenId": payload["notificationState"]["lastSeenId"],
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Export/import offline device health state bundles on Slack host."
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
    parser.add_argument("--import-rollback-bundle-path")
    parser.add_argument("--expected-state-digest")
    parser.add_argument("--expected-notification-state-digest")
    parser.add_argument("--expected-sms-state-digest")
    parser.add_argument("--expected-bundle-digest")
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
            args.import_rollback_bundle_path,
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
        elif args.export_forward_bundle_path:
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
        else:
            if not all(
                (
                    args.state_path,
                    args.notification_state_path,
                    args.expected_state_digest,
                    args.expected_notification_state_digest,
                    args.expected_bundle_digest,
                    args.sms_outbox_path,
                    args.expected_sms_state_digest,
                )
            ):
                raise DeviceHealthStateBundleError(
                    "rollback import confirmation is incomplete"
                )
            result = import_device_health_rollback_bundle(
                bundle_path=args.import_rollback_bundle_path,
                expected_bundle_digest=args.expected_bundle_digest,
                state_path=args.state_path,
                expected_state_digest=args.expected_state_digest,
                notification_state_path=args.notification_state_path,
                expected_notification_state_digest=(
                    args.expected_notification_state_digest
                ),
                sms_outbox_path=args.sms_outbox_path,
                expected_sms_state_digest=args.expected_sms_state_digest,
            )
    except DeviceHealthStateBundleError as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "export_device_health_forward_bundle",
    "import_device_health_rollback_bundle",
    "inspect_slack_device_health_state",
    "main",
]
