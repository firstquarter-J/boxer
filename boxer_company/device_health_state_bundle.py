from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Literal, Mapping

from boxer_company.device_health_fingerprint import (
    validate_and_canonicalize_device_health_alert_fingerprint_key,
)
from boxer_company.device_health_monitor_cycle import (
    build_device_health_monitor_seed_cursor,
)


DEVICE_HEALTH_STATE_BUNDLE_SCHEMA = "boxer.device_health_state_bundle"
DEVICE_HEALTH_STATE_BUNDLE_VERSION = 1
FILE_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MISSING_PROTECTED_FILE_DIGEST = hashlib.sha256(
    b"boxer.protected-json.missing.v1"
).hexdigest()
_MAX_PROTECTED_JSON_BYTES = 16 * 1024 * 1024
_FORWARD_BUNDLE_KEYS = frozenset(
    {
        "schema",
        "version",
        "direction",
        "exportedAt",
        "sourceDigest",
        "payload",
        "safety",
    }
)
_ROLLBACK_BUNDLE_KEYS = _FORWARD_BUNDLE_KEYS
_FORWARD_PAYLOAD_KEYS = frozenset(
    {
        "alertDeliveryEnabled",
        "alertFingerprints",
        "pendingAlertFingerprints",
        "notificationState",
    }
)
_ROLLBACK_PAYLOAD_KEYS = frozenset(
    {
        "alertDeliveryOverride",
        "alertFingerprints",
        "pendingAlertFingerprints",
        "pendingDecision",
        "notificationState",
    }
)
_FORWARD_SAFETY_KEYS = frozenset(
    {
        "notificationPendingEventCount",
        "notificationStateDigest",
        "smsOutboxItemCount",
        "smsUnresolvedClaimCount",
        "activeSettledClaimCount",
        "smsStateDigest",
    }
)
_ROLLBACK_SAFETY_KEYS = frozenset(
    {
        "inFlightCount",
        "ackInFlightCount",
        "pendingDeliveryCount",
        "pendingDeliveryContextCount",
        "pendingSheetAlertCount",
        "pendingSheetRepairCount",
        "smsOutboxItemCount",
        "smsUnresolvedClaimCount",
        "activeSettledClaimCount",
        "smsStateDigest",
        "healthStateDigest",
        "notificationStateDigest",
    }
)

_LEGACY_NOTIFICATION_REQUIRED_KEYS = frozenset(
    {
        "initialized",
        "lastSeenId",
        "pendingEvents",
        "recentCaptureboardAlerts",
        "recordingStallIncidents",
        "captureboardIncidents",
        "captureboardIncidentsLastSheetCheckedAt",
    }
)
_LEGACY_NOTIFICATION_OPTIONAL_KEYS = frozenset(
    {
        "initializedAt",
        "lastPolledAt",
        "lastSentAt",
        "lastSentNotificationId",
        "lastSlackMessageTs",
        "lastSlackPermalink",
        "apiRollbackMigration",
    }
)
_API_NOTIFICATION_CURSOR_KEYS = frozenset(
    {
        "initialized",
        "initializedAt",
        "lastSeenId",
        "lastPolledAt",
        "pendingDeliveryContexts",
        "recordingStallIncidents",
        "captureboardIncidents",
        "recentCaptureboardAlerts",
        "pendingSheetRepairs",
        "lastSheetWriteStatus",
        "autoSmsClaims",
        "cycleCompleted",
        "captureboardIncidentsLastSheetCheckedAt",
        "lastSentAt",
        "lastSentNotificationId",
        "lastExternalMessageId",
        "lastPermalink",
    }
)
_RECORDING_INCIDENT_HASH_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_LEGACY_RECORDING_INCIDENT_HASH_PATTERN = re.compile(
    r"^sha256:([0-9a-f]{64})$"
)
_CAPTUREBOARD_INCIDENT_STATUSES = frozenset({"대기", "처리중", "진행중"})
_CAPTUREBOARD_INCIDENT_CODES = frozenset(
    {"", "captureboard_connection_error", "recording_critically_stalled"}
)


class DeviceHealthStateBundleError(RuntimeError):
    """offline migration bundle이나 protected file 계약 위반이다."""


def build_device_health_forward_bundle(
    *,
    legacy_state: Mapping[str, Any],
    legacy_notification_state: Mapping[str, Any],
    source_state_digest: str,
    notification_source_state_digest: str,
    sms_state_digest: str,
    exported_at: datetime,
) -> dict[str, Any]:
    """Slack live health/notification state에서 API import bundle을 만든다."""

    if not FILE_DIGEST_PATTERN.fullmatch(str(source_state_digest or "")):
        raise DeviceHealthStateBundleError("forward source digest is invalid")
    if not FILE_DIGEST_PATTERN.fullmatch(str(sms_state_digest or "")):
        raise DeviceHealthStateBundleError("forward SMS state digest is invalid")
    if not FILE_DIGEST_PATTERN.fullmatch(
        str(notification_source_state_digest or "")
    ):
        raise DeviceHealthStateBundleError(
            "forward notification source digest is invalid"
        )
    if exported_at.tzinfo is None:
        raise DeviceHealthStateBundleError("forward export time is invalid")
    override = legacy_state.get("alertDeliveryOverride")
    if (
        not isinstance(override, Mapping)
        or type(override.get("enabled")) is not bool
        or not isinstance(legacy_state.get("alertFingerprints"), Mapping)
        or not isinstance(
            legacy_state.get("pendingAlertFingerprints"), Mapping
        )
    ):
        # 파일 밖 env 기본값을 추측하면 forward bundle이 live 정본과 달라진다.
        raise DeviceHealthStateBundleError(
            "legacy alert delivery override is required"
        )
    try:
        validated_cursor = build_device_health_monitor_seed_cursor(
            legacy_alert_delivery_enabled=override["enabled"],
            alert_fingerprints=legacy_state.get("alertFingerprints"),
            pending_alert_fingerprints=legacy_state.get(
                "pendingAlertFingerprints"
            ),
            pending_decision="preserve",
            seeded_at=exported_at,
        )
    except (TypeError, ValueError) as exc:
        raise DeviceHealthStateBundleError(str(exc)) from exc
    notification_state = build_device_notification_legacy_state(
        build_device_notification_api_cursor(legacy_notification_state)
    )
    bundle = {
        "schema": DEVICE_HEALTH_STATE_BUNDLE_SCHEMA,
        "version": DEVICE_HEALTH_STATE_BUNDLE_VERSION,
        "direction": "slack_to_api",
        "exportedAt": exported_at.astimezone(timezone.utc).isoformat(),
        "sourceDigest": source_state_digest,
        "payload": {
            "alertDeliveryEnabled": override["enabled"],
            "alertFingerprints": validated_cursor["alertFingerprints"],
            "pendingAlertFingerprints": validated_cursor[
                "pendingAlertFingerprints"
            ],
            "notificationState": notification_state,
        },
        "safety": {
            "notificationPendingEventCount": 0,
            "notificationStateDigest": notification_source_state_digest,
            "smsOutboxItemCount": 0,
            "smsUnresolvedClaimCount": 0,
            "activeSettledClaimCount": 0,
            "smsStateDigest": sms_state_digest,
        },
    }
    validate_device_health_state_bundle(bundle, direction="slack_to_api")
    return bundle


def build_device_health_rollback_bundle(
    *,
    cursor: Mapping[str, Any],
    notification_cursor: Mapping[str, Any],
    source_cursor_digest: str,
    sms_state_digest: str,
    health_state_digest: str,
    notification_state_digest: str,
    exported_at: datetime,
) -> dict[str, Any]:
    """API cursor에서 Slack import에 필요한 dedupe/override만 투영한다."""

    if not re.fullmatch(r"[0-9a-f]{24}", str(source_cursor_digest or "")):
        raise DeviceHealthStateBundleError("rollback cursor digest is invalid")
    if not FILE_DIGEST_PATTERN.fullmatch(str(sms_state_digest or "")):
        raise DeviceHealthStateBundleError("rollback SMS state digest is invalid")
    if (
        not FILE_DIGEST_PATTERN.fullmatch(str(health_state_digest or ""))
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(notification_state_digest or "")
        )
    ):
        raise DeviceHealthStateBundleError("rollback cycle state digest is invalid")
    if exported_at.tzinfo is None:
        raise DeviceHealthStateBundleError("rollback export time is invalid")
    override = cursor.get("alertDeliveryOverride")
    ownership = cursor.get("stateOwnership")
    if not isinstance(override, Mapping) or not isinstance(ownership, Mapping):
        raise DeviceHealthStateBundleError("rollback cursor is invalid")
    payload = {
        "alertDeliveryOverride": dict(override),
        "alertFingerprints": _canonical_fingerprint_state(
            cursor.get("alertFingerprints")
        ),
        "pendingAlertFingerprints": _canonical_fingerprint_state(
            cursor.get("pendingAlertFingerprints")
        ),
        "pendingDecision": str(ownership.get("pendingDecision") or ""),
        "notificationState": build_device_notification_legacy_state(
            notification_cursor
        ),
    }
    bundle = {
        "schema": DEVICE_HEALTH_STATE_BUNDLE_SCHEMA,
        "version": DEVICE_HEALTH_STATE_BUNDLE_VERSION,
        "direction": "api_to_slack",
        "exportedAt": exported_at.astimezone(timezone.utc).isoformat(),
        "sourceDigest": source_cursor_digest,
        "payload": payload,
        "safety": {
            "inFlightCount": 0,
            "ackInFlightCount": 0,
            "pendingDeliveryCount": 0,
            "pendingDeliveryContextCount": 0,
            "pendingSheetAlertCount": 0,
            "pendingSheetRepairCount": 0,
            "smsOutboxItemCount": 0,
            "smsUnresolvedClaimCount": 0,
            # rollback 직후 Slack producer가 같은 장애를 다시 claim할 수
            # 있으므로 settled claim의 60초 안전창도 완전히 drain한다.
            "activeSettledClaimCount": 0,
            "smsStateDigest": sms_state_digest,
            "healthStateDigest": health_state_digest,
            "notificationStateDigest": notification_state_digest,
        },
    }
    validate_device_health_state_bundle(bundle, direction="api_to_slack")
    return bundle


def validate_device_health_state_bundle(
    value: Any,
    *,
    direction: Literal["slack_to_api", "api_to_slack"],
) -> dict[str, Any]:
    """전송된 bundle의 version/direction/payload를 strict 검증한다."""

    if (
        not isinstance(value, Mapping)
        or set(value)
        != (
            _FORWARD_BUNDLE_KEYS
            if direction == "slack_to_api"
            else _ROLLBACK_BUNDLE_KEYS
        )
        or value.get("schema") != DEVICE_HEALTH_STATE_BUNDLE_SCHEMA
        or type(value.get("version")) is not int
        or value.get("version") != DEVICE_HEALTH_STATE_BUNDLE_VERSION
        or value.get("direction") != direction
        or _parse_aware_datetime(value.get("exportedAt")) is None
        or not isinstance(value.get("payload"), Mapping)
    ):
        raise DeviceHealthStateBundleError("device health bundle is invalid")
    source_digest = str(value.get("sourceDigest") or "")
    expected_source_pattern = (
        FILE_DIGEST_PATTERN
        if direction == "slack_to_api"
        else re.compile(r"^[0-9a-f]{24}$")
    )
    if not expected_source_pattern.fullmatch(source_digest):
        raise DeviceHealthStateBundleError("device health bundle source is invalid")
    payload = dict(value["payload"])
    if direction == "slack_to_api":
        safety = value.get("safety")
        if (
            set(payload) != _FORWARD_PAYLOAD_KEYS
            or type(payload.get("alertDeliveryEnabled")) is not bool
            or not isinstance(payload.get("alertFingerprints"), Mapping)
            or not isinstance(
                payload.get("pendingAlertFingerprints"), Mapping
            )
            or not isinstance(payload.get("notificationState"), Mapping)
            or not isinstance(safety, Mapping)
            or set(safety) != _FORWARD_SAFETY_KEYS
            or any(
                type(safety.get(key)) is not int or safety.get(key) != 0
                for key in (
                    "notificationPendingEventCount",
                    "smsOutboxItemCount",
                    "smsUnresolvedClaimCount",
                    "activeSettledClaimCount",
                )
            )
            or not FILE_DIGEST_PATTERN.fullmatch(
                str(safety.get("notificationStateDigest") or "")
            )
            or not FILE_DIGEST_PATTERN.fullmatch(
                str(safety.get("smsStateDigest") or "")
            )
        ):
            raise DeviceHealthStateBundleError("forward bundle payload is invalid")
        try:
            cursor = build_device_health_monitor_seed_cursor(
                legacy_alert_delivery_enabled=payload[
                    "alertDeliveryEnabled"
                ],
                alert_fingerprints=payload.get("alertFingerprints"),
                pending_alert_fingerprints=payload.get(
                    "pendingAlertFingerprints"
                ),
                pending_decision="preserve",
                seeded_at=_parse_aware_datetime(value["exportedAt"])
                or datetime.now(timezone.utc),
            )
        except (TypeError, ValueError) as exc:
            raise DeviceHealthStateBundleError(str(exc)) from exc
        payload["alertFingerprints"] = cursor["alertFingerprints"]
        payload["pendingAlertFingerprints"] = cursor[
            "pendingAlertFingerprints"
        ]
        payload["notificationState"] = validate_device_notification_legacy_state(
            payload["notificationState"],
            require_drained=True,
        )
    else:
        safety = value.get("safety")
        override = payload.get("alertDeliveryOverride")
        pending_decision = str(payload.get("pendingDecision") or "")
        if (
            set(payload) != _ROLLBACK_PAYLOAD_KEYS
            or not isinstance(override, Mapping)
            or set(override) != {"enabled", "updatedAt", "updatedBy"}
            or type(override.get("enabled")) is not bool
            or _parse_aware_datetime(override.get("updatedAt")) is None
            or str(override.get("updatedBy") or "")
            not in {"manual_cutover_seed", "manual_offline_override"}
            or pending_decision not in {"preserve", "assume_delivered"}
            or not isinstance(safety, Mapping)
            or set(safety) != _ROLLBACK_SAFETY_KEYS
            or any(
                type(safety.get(key)) is not int or safety.get(key) != 0
                for key in (
                    "inFlightCount",
                    "ackInFlightCount",
                    "pendingDeliveryCount",
                    "pendingDeliveryContextCount",
                    "pendingSheetAlertCount",
                    "pendingSheetRepairCount",
                    "smsOutboxItemCount",
                    "smsUnresolvedClaimCount",
                    "activeSettledClaimCount",
                )
            )
            or not FILE_DIGEST_PATTERN.fullmatch(
                str(safety.get("smsStateDigest") or "")
            )
            or not FILE_DIGEST_PATTERN.fullmatch(
                str(safety.get("healthStateDigest") or "")
            )
            or not FILE_DIGEST_PATTERN.fullmatch(
                str(safety.get("notificationStateDigest") or "")
            )
        ):
            raise DeviceHealthStateBundleError("rollback bundle payload is invalid")
        try:
            cursor = build_device_health_monitor_seed_cursor(
                legacy_alert_delivery_enabled=override["enabled"],
                alert_fingerprints=payload.get("alertFingerprints"),
                pending_alert_fingerprints=payload.get(
                    "pendingAlertFingerprints"
                ),
                pending_decision="preserve",
                seeded_at=_parse_aware_datetime(value["exportedAt"])
                or datetime.now(timezone.utc),
            )
        except (TypeError, ValueError) as exc:
            raise DeviceHealthStateBundleError(str(exc)) from exc
        payload["alertDeliveryOverride"] = dict(override)
        payload["alertFingerprints"] = cursor["alertFingerprints"]
        payload["pendingAlertFingerprints"] = cursor[
            "pendingAlertFingerprints"
        ]
        payload["pendingDecision"] = pending_decision
        payload["notificationState"] = validate_device_notification_legacy_state(
            payload.get("notificationState"),
            require_drained=True,
        )
    return {**dict(value), "payload": payload}


def validate_device_notification_legacy_state(
    value: Any,
    *,
    require_drained: bool,
) -> dict[str, Any]:
    """Slack notification raw state를 누락·묵시적 정규화 없이 검사한다."""

    if not isinstance(value, Mapping):
        raise DeviceHealthStateBundleError(
            "Slack device notification state is invalid"
        )
    keys = set(value)
    if (
        not _LEGACY_NOTIFICATION_REQUIRED_KEYS.issubset(keys)
        or keys
        - _LEGACY_NOTIFICATION_REQUIRED_KEYS
        - _LEGACY_NOTIFICATION_OPTIONAL_KEYS
    ):
        raise DeviceHealthStateBundleError(
            "Slack device notification state schema is invalid"
        )
    _validate_rollback_marker(value.get("apiRollbackMigration"))
    initialized = value.get("initialized")
    last_seen_id = value.get("lastSeenId")
    pending_events = value.get("pendingEvents")
    if (
        type(initialized) is not bool
        or type(last_seen_id) is not int
        or last_seen_id < 0
        or not isinstance(pending_events, list)
    ):
        raise DeviceHealthStateBundleError(
            "Slack device notification cursor is invalid"
        )
    if require_drained and pending_events:
        raise DeviceHealthStateBundleError(
            "Slack device notification pending events are not drained"
        )
    if pending_events:
        # bundle에는 DB event 원문을 싣지 않는다. drain되지 않은 queue를
        # 일부만 정규화하면 cutover 발송 유실 여부를 증명할 수 없다.
        raise DeviceHealthStateBundleError(
            "Slack device notification pending events are invalid"
        )

    initialized_at = _strict_optional_datetime_text(
        value.get("initializedAt"),
        field="notification initializedAt",
    )
    last_polled_at = _strict_optional_datetime_text(
        value.get("lastPolledAt"),
        field="notification lastPolledAt",
    )
    if initialized and (not initialized_at or not last_polled_at):
        raise DeviceHealthStateBundleError(
            "Slack device notification initialized cursor is invalid"
        )
    last_sent_at = _strict_optional_datetime_text(
        value.get("lastSentAt"),
        field="notification lastSentAt",
    )
    last_sent_id = _strict_nonnegative_int(
        value.get("lastSentNotificationId", 0),
        field="notification lastSentNotificationId",
    )
    if last_sent_id > last_seen_id or (last_sent_id and not last_sent_at):
        raise DeviceHealthStateBundleError(
            "Slack device notification sent cursor is invalid"
        )

    recent_alerts = _strict_recent_captureboard_alerts(
        value.get("recentCaptureboardAlerts")
    )
    recording_incidents = _strict_legacy_recording_incidents(
        value.get("recordingStallIncidents")
    )
    captureboard_incidents = _strict_legacy_captureboard_incidents(
        value.get("captureboardIncidents")
    )
    observed_ids = [last_sent_id]
    observed_ids.extend(
        int(item.get("notificationId") or 0)
        for item in recent_alerts.values()
    )
    observed_ids.extend(
        int(item.get("lastNotificationId") or 0)
        for item in recording_incidents.values()
    )
    observed_ids.extend(
        int(item.get("openedNotificationId") or 0)
        for item in captureboard_incidents.values()
    )
    if max(observed_ids, default=0) > last_seen_id:
        raise DeviceHealthStateBundleError(
            "Slack device notification incident cursor is ahead of lastSeenId"
        )
    return {
        "initialized": initialized,
        "initializedAt": initialized_at,
        "lastSeenId": last_seen_id,
        "lastPolledAt": last_polled_at,
        "pendingEvents": [],
        "recentCaptureboardAlerts": recent_alerts,
        "recordingStallIncidents": recording_incidents,
        "captureboardIncidents": captureboard_incidents,
        "captureboardIncidentsLastSheetCheckedAt": _strict_optional_datetime_text(
            value.get("captureboardIncidentsLastSheetCheckedAt"),
            field="notification captureboard sheet check",
        ),
        "lastSentAt": last_sent_at,
        "lastSentNotificationId": last_sent_id,
        "lastSlackMessageTs": _strict_text(
            value.get("lastSlackMessageTs"),
            field="notification Slack message id",
            max_length=256,
        ),
        "lastSlackPermalink": _strict_text(
            value.get("lastSlackPermalink"),
            field="notification Slack permalink",
            max_length=2_048,
        ),
    }


def build_device_notification_api_cursor(
    legacy_state: Mapping[str, Any],
) -> dict[str, Any]:
    """drained Slack state를 API notification cursor 표현으로 변환한다."""

    state = validate_device_notification_legacy_state(
        legacy_state,
        require_drained=True,
    )
    recording_incidents: dict[str, dict[str, Any]] = {}
    for legacy_key, item in state["recordingStallIncidents"].items():
        digest = _recording_incident_digest(legacy_key, item)
        if digest in recording_incidents:
            raise DeviceHealthStateBundleError(
                "notification recording incident hash collision requires manual review"
            )
        recording_incidents[digest] = {
            "phase": item["phase"],
            "deviceName": item["deviceName"],
            "lastNotificationId": item["lastNotificationId"],
            "lastOccurredAt": item["lastOccurredAt"],
            "lastDurationSeconds": item["lastDurationSeconds"],
            "lastCurrentSize": item["lastCurrentSize"],
            "rootExternalMessageId": item["slackMessageTs"],
            "rootPermalink": item["slackPermalink"],
            "lastCommentNotificationId": item["lastCommentNotificationId"],
        }
    captureboard_incidents = {
        device_name: {
            "deviceName": item["deviceName"],
            "deviceSeq": item["deviceSeq"],
            "status": item["status"],
            "rootExternalMessageId": item["slackMessageTs"],
            "rootPermalink": item["slackPermalink"],
            "rowNumber": item["rowNumber"],
            "openedNotificationId": item["openedNotificationId"],
            "openedCode": item["openedCode"],
            "openedAt": item["openedAt"],
            "lastSheetCheckedAt": item["lastSheetCheckedAt"],
            "lastSuppressedAt": item["lastSuppressedAt"],
            "lastSuppressedNotificationId": item[
                "lastSuppressedNotificationId"
            ],
            "lastSuppressedCode": item["lastSuppressedCode"],
            "suppressedCount": item["suppressedCount"],
        }
        for device_name, item in state["captureboardIncidents"].items()
    }
    return {
        "initialized": state["initialized"],
        "initializedAt": state["initializedAt"],
        "lastSeenId": state["lastSeenId"],
        "lastPolledAt": state["lastPolledAt"],
        "pendingDeliveryContexts": {},
        "recordingStallIncidents": recording_incidents,
        "captureboardIncidents": captureboard_incidents,
        "recentCaptureboardAlerts": state["recentCaptureboardAlerts"],
        "pendingSheetRepairs": {},
        "lastSheetWriteStatus": "",
        "autoSmsClaims": {},
        "cycleCompleted": False,
        "captureboardIncidentsLastSheetCheckedAt": state[
            "captureboardIncidentsLastSheetCheckedAt"
        ],
        "lastSentAt": state["lastSentAt"],
        "lastSentNotificationId": state["lastSentNotificationId"],
        "lastExternalMessageId": state["lastSlackMessageTs"],
        "lastPermalink": state["lastSlackPermalink"],
    }


def build_device_notification_legacy_state(
    notification_cursor: Mapping[str, Any],
) -> dict[str, Any]:
    """drained API notification cursor를 Slack rollback 표현으로 변환한다."""

    cursor = _validate_api_notification_cursor(notification_cursor)
    recording_incidents = {
        f"sha256:{digest}": {
            "phase": item["phase"],
            "deviceName": item["deviceName"],
            "barcode": "",
            "fileId": "",
            "fileType": "recording",
            "currentStatus": "recording",
            "firstNotificationId": item["lastNotificationId"],
            "firstOccurredAt": item["lastOccurredAt"],
            "firstDurationSeconds": item["lastDurationSeconds"],
            "lastNotificationId": item["lastNotificationId"],
            "lastOccurredAt": item["lastOccurredAt"],
            "lastDurationSeconds": item["lastDurationSeconds"],
            "lastCurrentSize": item["lastCurrentSize"],
            "slackMessageTs": item["rootExternalMessageId"],
            "slackPermalink": item["rootPermalink"],
            "lastCommentNotificationId": item["lastCommentNotificationId"],
        }
        for digest, item in cursor["recordingStallIncidents"].items()
    }
    captureboard_incidents = {
        device_name: {
            "deviceName": item["deviceName"],
            "deviceSeq": item["deviceSeq"],
            "status": item["status"],
            "slackMessageTs": item["rootExternalMessageId"],
            "slackPermalink": item["rootPermalink"],
            "rowNumber": item["rowNumber"],
            "openedNotificationId": item["openedNotificationId"],
            "openedCode": item["openedCode"],
            "openedAt": item["openedAt"],
            "lastSheetCheckedAt": item["lastSheetCheckedAt"],
            "lastSuppressedAt": item["lastSuppressedAt"],
            "lastSuppressedNotificationId": item[
                "lastSuppressedNotificationId"
            ],
            "lastSuppressedCode": item["lastSuppressedCode"],
            "suppressedCount": item["suppressedCount"],
        }
        for device_name, item in cursor["captureboardIncidents"].items()
    }
    state = {
        "initialized": cursor["initialized"],
        "initializedAt": cursor["initializedAt"],
        "lastSeenId": cursor["lastSeenId"],
        "lastPolledAt": cursor["lastPolledAt"],
        "pendingEvents": [],
        "recentCaptureboardAlerts": cursor["recentCaptureboardAlerts"],
        "recordingStallIncidents": recording_incidents,
        "captureboardIncidents": captureboard_incidents,
        "captureboardIncidentsLastSheetCheckedAt": cursor[
            "captureboardIncidentsLastSheetCheckedAt"
        ],
        "lastSentAt": cursor["lastSentAt"],
        "lastSentNotificationId": cursor["lastSentNotificationId"],
        "lastSlackMessageTs": cursor["lastExternalMessageId"],
        "lastSlackPermalink": cursor["lastPermalink"],
    }
    return validate_device_notification_legacy_state(
        state,
        require_drained=True,
    )


def load_protected_json_file(
    path_value: str | Path,
    *,
    label: str,
) -> tuple[Any, str]:
    """owner-only regular file을 한 revision으로 읽고 raw SHA-256을 반환한다."""

    path = Path(path_value).expanduser()
    if not path.is_absolute() or path == Path("/"):
        raise DeviceHealthStateBundleError(f"{label} path is invalid")
    _validate_protected_parent_chain(path, label=label)
    flags = os.O_RDONLY
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        raise DeviceHealthStateBundleError(f"{label} file is unreadable") from exc
    chunks: list[bytes] = []
    try:
        before = os.fstat(descriptor)
        if (
            not stat.S_ISREG(before.st_mode)
            or before.st_uid not in {0, os.geteuid()}
            or before.st_mode & 0o077
            or before.st_size > _MAX_PROTECTED_JSON_BYTES
        ):
            raise DeviceHealthStateBundleError(f"{label} file is not protected")
        size = 0
        while True:
            chunk = os.read(
                descriptor,
                min(64 * 1024, _MAX_PROTECTED_JSON_BYTES + 1 - size),
            )
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > _MAX_PROTECTED_JSON_BYTES:
                raise DeviceHealthStateBundleError(f"{label} file is too large")
        after = os.fstat(descriptor)
        if (
            before.st_dev,
            before.st_ino,
            before.st_size,
            before.st_mtime_ns,
        ) != (
            after.st_dev,
            after.st_ino,
            after.st_size,
            after.st_mtime_ns,
        ):
            raise DeviceHealthStateBundleError(f"{label} file changed while reading")
    finally:
        os.close(descriptor)
    raw = b"".join(chunks)
    try:
        payload = json.loads(raw.decode("utf-8"))
    except Exception as exc:
        raise DeviceHealthStateBundleError(f"{label} file is unreadable") from exc
    return payload, hashlib.sha256(raw).hexdigest()


def create_protected_json_file(
    path_value: str | Path,
    payload: Mapping[str, Any],
    *,
    label: str,
) -> str:
    """기존 target을 덮지 않고 0600 JSON bundle을 원자 생성한다."""

    path = _validated_output_path(path_value, label=label)
    raw = _json_bytes(payload)
    temporary_path = _write_protected_temporary(path, raw)
    try:
        try:
            os.link(temporary_path, path)
        except FileExistsError as exc:
            raise DeviceHealthStateBundleError(f"{label} already exists") from exc
        _fsync_directory(path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)
    return hashlib.sha256(raw).hexdigest()


def replace_protected_json_file(
    path_value: str | Path,
    payload: Mapping[str, Any],
    *,
    expected_existing_digest: str,
    label: str,
) -> str:
    """live state digest를 재확인한 뒤 0600 파일로 원자 교체한다."""

    if not FILE_DIGEST_PATTERN.fullmatch(str(expected_existing_digest or "")):
        raise DeviceHealthStateBundleError(f"{label} digest is invalid")
    path = _validated_output_path(path_value, label=label)
    raw = _json_bytes(payload)
    temporary_path = _write_protected_temporary(path, raw)
    try:
        # stopped-service 확인과 별개로 동시 recovery CLI 둘도 같은 CAS
        # revision을 통과하지 못하게 host-local advisory lock으로 직렬화한다.
        with _exclusive_protected_path_lock(path, label=label):
            _, current_digest = load_protected_json_file(path, label=label)
            if current_digest != expected_existing_digest:
                raise DeviceHealthStateBundleError(f"{label} changed")
            os.replace(temporary_path, path)
            _fsync_directory(path.parent)
    finally:
        temporary_path.unlink(missing_ok=True)
    return hashlib.sha256(raw).hexdigest()


def inspect_protected_json_target_revision(
    path_value: str | Path,
    *,
    label: str,
) -> tuple[Any | None, str]:
    """import target의 protected existing revision 또는 명시적 missing을 읽는다."""

    path = _validated_output_path(path_value, label=label)
    try:
        path.lstat()
    except FileNotFoundError:
        return None, MISSING_PROTECTED_FILE_DIGEST
    except OSError as exc:
        raise DeviceHealthStateBundleError(f"{label} file is unreadable") from exc
    return load_protected_json_file(path, label=label)


def _canonical_fingerprint_state(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise DeviceHealthStateBundleError("fingerprint state is invalid")
    result: dict[str, dict[str, Any]] = {}
    for raw_key, raw_item in value.items():
        if not isinstance(raw_item, Mapping):
            raise DeviceHealthStateBundleError("fingerprint state is invalid")
        try:
            key = validate_and_canonicalize_device_health_alert_fingerprint_key(
                raw_key
            )
        except ValueError as exc:
            raise DeviceHealthStateBundleError(str(exc)) from exc
        if key in result:
            raise DeviceHealthStateBundleError(
                "canonical fingerprint collision requires manual review"
            )
        result[key] = dict(raw_item)
    return result


def _validate_rollback_marker(value: Any) -> None:
    if value is None:
        return
    if (
        not isinstance(value, Mapping)
        or set(value)
        != {
            "version",
            "migratedAt",
            "bundleDigest",
            "apiNotificationStateDigest",
            "previousSlackStateDigest",
        }
        or type(value.get("version")) is not int
        or value.get("version") != 1
        or _parse_aware_datetime(value.get("migratedAt")) is None
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(value.get("bundleDigest") or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(value.get("apiNotificationStateDigest") or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(value.get("previousSlackStateDigest") or "")
        )
    ):
        raise DeviceHealthStateBundleError(
            "Slack device notification rollback marker is invalid"
        )


def _strict_recent_captureboard_alerts(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise DeviceHealthStateBundleError(
            "device notification recent alerts are invalid"
        )
    result: dict[str, dict[str, Any]] = {}
    for raw_device_name, raw_item in value.items():
        device_name = _strict_required_text(
            raw_device_name,
            field="notification recent alert device",
            max_length=255,
        )
        if (
            not isinstance(raw_item, Mapping)
            or set(raw_item) != {"lastAlertedAt", "notificationId"}
        ):
            raise DeviceHealthStateBundleError(
                "device notification recent alert is invalid"
            )
        notification_id = _strict_optional_positive_int(
            raw_item.get("notificationId"),
            field="notification recent alert id",
        )
        result[device_name] = {
            "lastAlertedAt": _strict_required_datetime_text(
                raw_item.get("lastAlertedAt"),
                field="notification recent alert time",
            ),
            "notificationId": notification_id,
        }
    return result


def _strict_legacy_recording_incidents(
    value: Any,
) -> dict[str, dict[str, Any]]:
    expected_keys = {
        "phase",
        "deviceName",
        "barcode",
        "fileId",
        "fileType",
        "currentStatus",
        "firstNotificationId",
        "firstOccurredAt",
        "firstDurationSeconds",
        "lastNotificationId",
        "lastOccurredAt",
        "lastDurationSeconds",
        "lastCurrentSize",
        "slackMessageTs",
        "slackPermalink",
        "lastCommentNotificationId",
    }
    if not isinstance(value, Mapping):
        raise DeviceHealthStateBundleError(
            "device notification recording incidents are invalid"
        )
    result: dict[str, dict[str, Any]] = {}
    for raw_key, raw_item in value.items():
        key = _strict_required_text(
            raw_key,
            field="notification recording incident key",
            max_length=1_024,
        )
        if not isinstance(raw_item, Mapping) or set(raw_item) != expected_keys:
            raise DeviceHealthStateBundleError(
                "device notification recording incident is invalid"
            )
        phase = str(raw_item.get("phase") or "")
        if phase not in {"candidate", "alerted"}:
            raise DeviceHealthStateBundleError(
                "device notification recording incident phase is invalid"
            )
        item = {
            "phase": phase,
            "deviceName": _strict_required_text(
                raw_item.get("deviceName"),
                field="notification recording device",
                max_length=255,
            ),
            "barcode": _strict_text(
                raw_item.get("barcode"),
                field="notification recording barcode",
                max_length=255,
            ),
            "fileId": _strict_text(
                raw_item.get("fileId"),
                field="notification recording file id",
                max_length=255,
            ),
            "fileType": _strict_text(
                raw_item.get("fileType"),
                field="notification recording file type",
                max_length=64,
            ),
            "currentStatus": _strict_text(
                raw_item.get("currentStatus"),
                field="notification recording status",
                max_length=64,
            ),
            "firstNotificationId": _strict_positive_int(
                raw_item.get("firstNotificationId"),
                field="notification recording first id",
            ),
            "firstOccurredAt": _strict_required_datetime_text(
                raw_item.get("firstOccurredAt"),
                field="notification recording first time",
            ),
            "firstDurationSeconds": _strict_positive_int(
                raw_item.get("firstDurationSeconds"),
                field="notification recording first duration",
            ),
            "lastNotificationId": _strict_positive_int(
                raw_item.get("lastNotificationId"),
                field="notification recording last id",
            ),
            "lastOccurredAt": _strict_required_datetime_text(
                raw_item.get("lastOccurredAt"),
                field="notification recording last time",
            ),
            "lastDurationSeconds": _strict_positive_int(
                raw_item.get("lastDurationSeconds"),
                field="notification recording last duration",
            ),
            "lastCurrentSize": _strict_optional_nonnegative_int(
                raw_item.get("lastCurrentSize"),
                field="notification recording current size",
            ),
            "slackMessageTs": _strict_text(
                raw_item.get("slackMessageTs"),
                field="notification recording Slack message id",
                max_length=256,
            ),
            "slackPermalink": _strict_text(
                raw_item.get("slackPermalink"),
                field="notification recording Slack permalink",
                max_length=2_048,
            ),
            "lastCommentNotificationId": _strict_optional_positive_int(
                raw_item.get("lastCommentNotificationId"),
                field="notification recording comment id",
            ),
        }
        if phase == "alerted" and not item["slackMessageTs"]:
            raise DeviceHealthStateBundleError(
                "device notification recording root message is invalid"
            )
        _recording_incident_digest(key, item)
        result[key] = item
    return result


def _strict_legacy_captureboard_incidents(
    value: Any,
) -> dict[str, dict[str, Any]]:
    expected_keys = {
        "deviceName",
        "deviceSeq",
        "status",
        "slackMessageTs",
        "slackPermalink",
        "rowNumber",
        "openedNotificationId",
        "openedCode",
        "openedAt",
        "lastSheetCheckedAt",
        "lastSuppressedAt",
        "lastSuppressedNotificationId",
        "lastSuppressedCode",
        "suppressedCount",
    }
    if not isinstance(value, Mapping):
        raise DeviceHealthStateBundleError(
            "device notification captureboard incidents are invalid"
        )
    result: dict[str, dict[str, Any]] = {}
    for raw_device_name, raw_item in value.items():
        device_name = _strict_required_text(
            raw_device_name,
            field="notification captureboard device",
            max_length=255,
        )
        if not isinstance(raw_item, Mapping) or set(raw_item) != expected_keys:
            raise DeviceHealthStateBundleError(
                "device notification captureboard incident is invalid"
            )
        if raw_item.get("deviceName") != device_name:
            raise DeviceHealthStateBundleError(
                "device notification captureboard incident key is invalid"
            )
        status = str(raw_item.get("status") or "")
        opened_code = str(raw_item.get("openedCode") or "")
        if (
            status not in _CAPTUREBOARD_INCIDENT_STATUSES
            or opened_code not in _CAPTUREBOARD_INCIDENT_CODES
        ):
            raise DeviceHealthStateBundleError(
                "device notification captureboard incident status is invalid"
            )
        result[device_name] = {
            "deviceName": device_name,
            "deviceSeq": _strict_optional_positive_int(
                raw_item.get("deviceSeq"),
                field="notification captureboard device seq",
            ),
            "status": status,
            "slackMessageTs": _strict_text(
                raw_item.get("slackMessageTs"),
                field="notification captureboard Slack message id",
                max_length=256,
            ),
            "slackPermalink": _strict_text(
                raw_item.get("slackPermalink"),
                field="notification captureboard Slack permalink",
                max_length=2_048,
            ),
            "rowNumber": _strict_optional_positive_int(
                raw_item.get("rowNumber"),
                field="notification captureboard row",
            ),
            "openedNotificationId": _strict_optional_positive_int(
                raw_item.get("openedNotificationId"),
                field="notification captureboard opened id",
            ),
            "openedCode": opened_code,
            "openedAt": _strict_optional_datetime_text(
                raw_item.get("openedAt"),
                field="notification captureboard opened time",
            ),
            "lastSheetCheckedAt": _strict_optional_datetime_text(
                raw_item.get("lastSheetCheckedAt"),
                field="notification captureboard sheet check",
            ),
            "lastSuppressedAt": _strict_optional_datetime_text(
                raw_item.get("lastSuppressedAt"),
                field="notification captureboard suppressed time",
            ),
            "lastSuppressedNotificationId": _strict_optional_positive_int(
                raw_item.get("lastSuppressedNotificationId"),
                field="notification captureboard suppressed id",
            ),
            "lastSuppressedCode": _strict_text(
                raw_item.get("lastSuppressedCode"),
                field="notification captureboard suppressed code",
                max_length=64,
            ),
            "suppressedCount": _strict_nonnegative_int(
                raw_item.get("suppressedCount"),
                field="notification captureboard suppressed count",
            ),
        }
    return result


def _validate_api_notification_cursor(
    value: Any,
) -> dict[str, Any]:
    if not isinstance(value, Mapping) or set(value) != _API_NOTIFICATION_CURSOR_KEYS:
        raise DeviceHealthStateBundleError(
            "API device notification cursor schema is invalid"
        )
    if any(
        not isinstance(value.get(key), Mapping) or value.get(key)
        for key in (
            "pendingDeliveryContexts",
            "pendingSheetRepairs",
            "autoSmsClaims",
        )
    ):
        raise DeviceHealthStateBundleError(
            "API device notification cursor is not drained"
        )
    if type(value.get("cycleCompleted")) is not bool:
        raise DeviceHealthStateBundleError(
            "API device notification cursor is invalid"
        )
    initialized = value.get("initialized")
    last_seen_id = value.get("lastSeenId")
    if (
        type(initialized) is not bool
        or type(last_seen_id) is not int
        or last_seen_id < 0
    ):
        raise DeviceHealthStateBundleError(
            "API device notification cursor is invalid"
        )
    initialized_at = _strict_optional_datetime_text(
        value.get("initializedAt"),
        field="API notification initializedAt",
    )
    last_polled_at = _strict_optional_datetime_text(
        value.get("lastPolledAt"),
        field="API notification lastPolledAt",
    )
    if initialized and (not initialized_at or not last_polled_at):
        raise DeviceHealthStateBundleError(
            "API device notification initialized cursor is invalid"
        )
    last_sent_at = _strict_optional_datetime_text(
        value.get("lastSentAt"),
        field="API notification lastSentAt",
    )
    last_sent_id = _strict_nonnegative_int(
        value.get("lastSentNotificationId"),
        field="API notification last sent id",
    )
    if last_sent_id > last_seen_id or (last_sent_id and not last_sent_at):
        raise DeviceHealthStateBundleError(
            "API device notification sent cursor is invalid"
        )
    recent_alerts = _strict_recent_captureboard_alerts(
        value.get("recentCaptureboardAlerts")
    )
    recording_incidents = _strict_api_recording_incidents(
        value.get("recordingStallIncidents")
    )
    captureboard_incidents = _strict_api_captureboard_incidents(
        value.get("captureboardIncidents")
    )
    observed_ids = [last_sent_id]
    observed_ids.extend(
        int(item.get("notificationId") or 0)
        for item in recent_alerts.values()
    )
    observed_ids.extend(
        int(item.get("lastNotificationId") or 0)
        for item in recording_incidents.values()
    )
    observed_ids.extend(
        int(item.get("openedNotificationId") or 0)
        for item in captureboard_incidents.values()
    )
    if max(observed_ids, default=0) > last_seen_id:
        raise DeviceHealthStateBundleError(
            "API device notification incident cursor is ahead of lastSeenId"
        )
    return {
        **dict(value),
        "initializedAt": initialized_at,
        "lastPolledAt": last_polled_at,
        "recordingStallIncidents": recording_incidents,
        "captureboardIncidents": captureboard_incidents,
        "recentCaptureboardAlerts": recent_alerts,
        "captureboardIncidentsLastSheetCheckedAt": _strict_optional_datetime_text(
            value.get("captureboardIncidentsLastSheetCheckedAt"),
            field="API notification captureboard sheet check",
        ),
        "lastSentAt": last_sent_at,
        "lastSentNotificationId": last_sent_id,
        "lastExternalMessageId": _strict_text(
            value.get("lastExternalMessageId"),
            field="API notification external message id",
            max_length=256,
        ),
        "lastPermalink": _strict_text(
            value.get("lastPermalink"),
            field="API notification permalink",
            max_length=2_048,
        ),
        "lastSheetWriteStatus": _strict_text(
            value.get("lastSheetWriteStatus"),
            field="API notification Sheet status",
            max_length=64,
        ),
    }


def _strict_api_recording_incidents(value: Any) -> dict[str, dict[str, Any]]:
    expected_keys = {
        "phase",
        "deviceName",
        "lastNotificationId",
        "lastOccurredAt",
        "lastDurationSeconds",
        "lastCurrentSize",
        "rootExternalMessageId",
        "rootPermalink",
        "lastCommentNotificationId",
    }
    if not isinstance(value, Mapping):
        raise DeviceHealthStateBundleError(
            "API device notification recording incidents are invalid"
        )
    result: dict[str, dict[str, Any]] = {}
    for raw_digest, raw_item in value.items():
        digest = str(raw_digest or "")
        if (
            not _RECORDING_INCIDENT_HASH_PATTERN.fullmatch(digest)
            or not isinstance(raw_item, Mapping)
            or set(raw_item) != expected_keys
        ):
            raise DeviceHealthStateBundleError(
                "API device notification recording incident is invalid"
            )
        phase = str(raw_item.get("phase") or "")
        if phase not in {"candidate", "alerted"}:
            raise DeviceHealthStateBundleError(
                "API device notification recording incident phase is invalid"
            )
        item = {
            "phase": phase,
            "deviceName": _strict_required_text(
                raw_item.get("deviceName"),
                field="API notification recording device",
                max_length=255,
            ),
            "lastNotificationId": _strict_positive_int(
                raw_item.get("lastNotificationId"),
                field="API notification recording last id",
            ),
            "lastOccurredAt": _strict_required_datetime_text(
                raw_item.get("lastOccurredAt"),
                field="API notification recording last time",
            ),
            "lastDurationSeconds": _strict_positive_int(
                raw_item.get("lastDurationSeconds"),
                field="API notification recording duration",
            ),
            "lastCurrentSize": _strict_optional_nonnegative_int(
                raw_item.get("lastCurrentSize"),
                field="API notification recording current size",
            ),
            "rootExternalMessageId": _strict_text(
                raw_item.get("rootExternalMessageId"),
                field="API notification recording root message",
                max_length=256,
            ),
            "rootPermalink": _strict_text(
                raw_item.get("rootPermalink"),
                field="API notification recording permalink",
                max_length=2_048,
            ),
            "lastCommentNotificationId": _strict_optional_positive_int(
                raw_item.get("lastCommentNotificationId"),
                field="API notification recording comment id",
            ),
        }
        if phase == "alerted" and not item["rootExternalMessageId"]:
            raise DeviceHealthStateBundleError(
                "API device notification recording root message is invalid"
            )
        result[digest] = item
    return result


def _strict_api_captureboard_incidents(value: Any) -> dict[str, dict[str, Any]]:
    allowed_keys = {
        "deviceName",
        "deviceSeq",
        "status",
        "rootExternalMessageId",
        "rootPermalink",
        "rowNumber",
        "openedNotificationId",
        "openedCode",
        "openedAt",
        "lastSheetCheckedAt",
        "lastSuppressedAt",
        "lastSuppressedNotificationId",
        "lastSuppressedCode",
        "suppressedCount",
    }
    if not isinstance(value, Mapping):
        raise DeviceHealthStateBundleError(
            "API device notification captureboard incidents are invalid"
        )
    result: dict[str, dict[str, Any]] = {}
    for raw_device_name, raw_item in value.items():
        device_name = _strict_required_text(
            raw_device_name,
            field="API notification captureboard device",
            max_length=255,
        )
        if (
            not isinstance(raw_item, Mapping)
            or not {"deviceName", "status"}.issubset(raw_item)
            or set(raw_item) - allowed_keys
            or raw_item.get("deviceName") != device_name
            or str(raw_item.get("status") or "")
            not in _CAPTUREBOARD_INCIDENT_STATUSES
        ):
            raise DeviceHealthStateBundleError(
                "API device notification captureboard incident is invalid"
            )
        opened_code = str(raw_item.get("openedCode") or "")
        if opened_code not in _CAPTUREBOARD_INCIDENT_CODES:
            raise DeviceHealthStateBundleError(
                "API device notification captureboard incident code is invalid"
            )
        result[device_name] = {
            "deviceName": device_name,
            "deviceSeq": _strict_optional_positive_int(
                raw_item.get("deviceSeq"),
                field="API notification captureboard device seq",
            ),
            "status": str(raw_item["status"]),
            "rootExternalMessageId": _strict_text(
                raw_item.get("rootExternalMessageId"),
                field="API notification captureboard root message",
                max_length=256,
            ),
            "rootPermalink": _strict_text(
                raw_item.get("rootPermalink"),
                field="API notification captureboard permalink",
                max_length=2_048,
            ),
            "rowNumber": _strict_optional_positive_int(
                raw_item.get("rowNumber"),
                field="API notification captureboard row",
            ),
            "openedNotificationId": _strict_optional_positive_int(
                raw_item.get("openedNotificationId"),
                field="API notification captureboard opened id",
            ),
            "openedCode": opened_code,
            "openedAt": _strict_optional_datetime_text(
                raw_item.get("openedAt"),
                field="API notification captureboard opened time",
            ),
            "lastSheetCheckedAt": _strict_optional_datetime_text(
                raw_item.get("lastSheetCheckedAt"),
                field="API notification captureboard sheet check",
            ),
            "lastSuppressedAt": _strict_optional_datetime_text(
                raw_item.get("lastSuppressedAt"),
                field="API notification captureboard suppressed time",
            ),
            "lastSuppressedNotificationId": _strict_optional_positive_int(
                raw_item.get("lastSuppressedNotificationId"),
                field="API notification captureboard suppressed id",
            ),
            "lastSuppressedCode": _strict_text(
                raw_item.get("lastSuppressedCode"),
                field="API notification captureboard suppressed code",
                max_length=64,
            ),
            "suppressedCount": _strict_nonnegative_int(
                raw_item.get("suppressedCount", 0),
                field="API notification captureboard suppressed count",
            ),
        }
    return result


def _recording_incident_digest(
    key: str,
    item: Mapping[str, Any],
) -> str:
    matched = _LEGACY_RECORDING_INCIDENT_HASH_PATTERN.fullmatch(key)
    if matched:
        return matched.group(1)
    expected_key = "|".join(
        (
            str(item.get("deviceName") or "").strip(),
            str(item.get("fileId") or "-").strip(),
            str(item.get("barcode") or "-").strip(),
            str(item.get("fileType") or "recording").strip(),
        )
    )
    if key != expected_key:
        raise DeviceHealthStateBundleError(
            "device notification recording incident key is invalid"
        )
    # API event normalizer와 같은 NUL 구분자를 써 forward seed가 다음 DB
    # event의 incidentDiscriminator와 정확히 이어지게 한다.
    api_key = "\0".join(
        (
            str(item.get("deviceName") or "").strip(),
            str(item.get("fileId") or "-").strip(),
            str(item.get("barcode") or "-").strip(),
            str(item.get("fileType") or "recording").strip(),
        )
    )
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def _strict_text(value: Any, *, field: str, max_length: int) -> str:
    if value is None:
        return ""
    if not isinstance(value, str) or len(value) > max_length:
        raise DeviceHealthStateBundleError(f"{field} is invalid")
    return value.strip()


def _strict_required_text(value: Any, *, field: str, max_length: int) -> str:
    text = _strict_text(value, field=field, max_length=max_length)
    if not text:
        raise DeviceHealthStateBundleError(f"{field} is invalid")
    return text


def _strict_optional_datetime_text(value: Any, *, field: str) -> str:
    text = _strict_text(value, field=field, max_length=64)
    if text and _parse_aware_datetime(text) is None:
        raise DeviceHealthStateBundleError(f"{field} is invalid")
    return text


def _strict_required_datetime_text(value: Any, *, field: str) -> str:
    text = _strict_optional_datetime_text(value, field=field)
    if not text:
        raise DeviceHealthStateBundleError(f"{field} is invalid")
    return text


def _strict_nonnegative_int(value: Any, *, field: str) -> int:
    if type(value) is not int or value < 0:
        raise DeviceHealthStateBundleError(f"{field} is invalid")
    return value


def _strict_positive_int(value: Any, *, field: str) -> int:
    result = _strict_nonnegative_int(value, field=field)
    if result <= 0:
        raise DeviceHealthStateBundleError(f"{field} is invalid")
    return result


def _strict_optional_nonnegative_int(value: Any, *, field: str) -> int | None:
    if value is None:
        return None
    return _strict_nonnegative_int(value, field=field)


def _strict_optional_positive_int(value: Any, *, field: str) -> int | None:
    if value is None:
        return None
    return _strict_positive_int(value, field=field)


def _validated_output_path(path_value: str | Path, *, label: str) -> Path:
    path = Path(path_value).expanduser()
    if not path.is_absolute() or path == Path("/"):
        raise DeviceHealthStateBundleError(f"{label} path is invalid")
    _validate_protected_parent_chain(path, label=label)
    parent = path.parent
    if not os.access(parent, os.W_OK | os.X_OK):
        raise DeviceHealthStateBundleError(f"{label} parent is not writable")
    return path


def _validate_protected_parent_chain(path: Path, *, label: str) -> None:
    """leaf부터 root까지 symlink·타인 writable directory를 모두 거부한다."""

    current = path.parent
    while True:
        try:
            current_stat = current.lstat()
        except OSError as exc:
            raise DeviceHealthStateBundleError(
                f"{label} parent is invalid"
            ) from exc
        if (
            stat.S_ISLNK(current_stat.st_mode)
            or not stat.S_ISDIR(current_stat.st_mode)
            or current_stat.st_uid not in {0, os.geteuid()}
            or current_stat.st_mode & 0o022
        ):
            raise DeviceHealthStateBundleError(f"{label} parent is not protected")
        if current.parent == current:
            break
        current = current.parent


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(
            dict(payload),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _write_protected_temporary(path: Path, raw: bytes) -> Path:
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary_path = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(raw)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        temporary_path.unlink(missing_ok=True)
        raise
    return temporary_path


@contextmanager
def _exclusive_protected_path_lock(path: Path, *, label: str) -> Any:
    """같은 host의 offline writer를 exact digest 검사부터 swap까지 직렬화한다."""

    lock_path = path.with_name(f".{path.name}.migration.lock")
    flags = os.O_CREAT | os.O_RDWR
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(lock_path, flags, 0o600)
    except OSError as exc:
        raise DeviceHealthStateBundleError(f"{label} lock is unavailable") from exc
    try:
        lock_stat = os.fstat(descriptor)
        if (
            not stat.S_ISREG(lock_stat.st_mode)
            or lock_stat.st_uid not in {0, os.geteuid()}
            or lock_stat.st_mode & 0o077
        ):
            raise DeviceHealthStateBundleError(f"{label} lock is not protected")
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _parse_aware_datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


__all__ = [
    "build_device_notification_api_cursor",
    "build_device_notification_legacy_state",
    "build_device_health_forward_bundle",
    "build_device_health_rollback_bundle",
    "create_protected_json_file",
    "DeviceHealthStateBundleError",
    "FILE_DIGEST_PATTERN",
    "inspect_protected_json_target_revision",
    "load_protected_json_file",
    "MISSING_PROTECTED_FILE_DIGEST",
    "replace_protected_json_file",
    "validate_device_notification_legacy_state",
    "validate_device_health_state_bundle",
]
