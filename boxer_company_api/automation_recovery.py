from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Iterator, Literal, Mapping, Sequence

from boxer_company.sms_delivery_cycle import (
    inspect_automatic_sms_recovery_state,
    settle_automatic_sms_delivery_claim_for_recovery,
)
from boxer_company.device_health_monitor_cycle import (
    build_device_health_monitor_seed_cursor,
    device_health_monitor_cursor_digest,
    update_device_health_monitor_alert_delivery_override,
)
from boxer_company.automation import AutomationCycleContractError
from boxer_company.device_health_state_bundle import (
    build_device_notification_api_cursor,
    build_device_notification_legacy_state,
    build_device_health_rollback_bundle,
    create_protected_json_file,
    DeviceHealthStateBundleError,
    FILE_DIGEST_PATTERN,
    load_protected_json_file,
    validate_device_health_state_bundle,
)
from boxer_company_api.automation import (
    AutomationStateSnapshot,
    JsonAutomationCycleStateStore,
)


_CYCLES = frozenset(
    {
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    }
)
_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
)
_CYCLE_KEY_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,191}$"
)
_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_MAX_RESOLUTION_HISTORY = 20
_SMS_DEVICE_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_SMS_CATEGORY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_CURSOR_DIGEST_PATTERN = re.compile(r"^[0-9a-f]{24}$")
_DEVICE_HEALTH_SEED_KEYS = frozenset(
    {
        "version",
        "legacyAlertDeliveryEnabled",
        "alertFingerprints",
        "pendingAlertFingerprints",
    }
)


class AutomationRecoveryError(RuntimeError):
    """불명 상태를 안전하게 식별하거나 해제할 수 없을 때 발생한다."""


def seed_device_health_monitor_state(
    *,
    state_path: str | Path,
    tenant_id: str,
    legacy_state: Mapping[str, Any],
    pending_decision: Literal["preserve", "assume_delivered"],
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """서비스 중지 뒤 검토된 legacy override/dedupe를 API에 최초 seed한다."""

    _validate_identity(
        tenant_id=tenant_id,
        cycle="device_health_monitor",
        cycle_key="continuous",
        request_id="device-health-state-seed",
    )
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise AutomationRecoveryError("device health seed time is invalid")
    _, state, result = _build_device_health_seed_state(
        legacy_state=legacy_state,
        pending_decision=pending_decision,
        now=actual_now,
    )

    store = JsonAutomationCycleStateStore(state_path)
    state_key = _state_key(
        tenant_id,
        "device_health_monitor",
        "continuous",
    )

    def _seed_once(
        exists: bool,
        _current: dict[str, Any],
    ) -> tuple[Mapping[str, Any], None]:
        if exists:
            # 기존 empty object도 과거 실행이 남긴 revision일 수 있으므로
            # absent target과 같다고 보지 않고 수동 검토 전까지 막는다.
            raise AutomationRecoveryError(
                "device health automation state is not empty"
            )
        return state, None

    try:
        store.mutate_cycle(state_key, _seed_once)
    except AutomationCycleContractError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    return result


def import_device_health_monitor_forward_bundle(
    *,
    state_path: str | Path,
    tenant_id: str,
    bundle_path: str | Path,
    expected_bundle_digest: str,
    expected_target_state_digest: str,
    expected_notification_target_state_digest: str,
    sms_outbox_path: str | Path,
    expected_sms_state_digest: str,
    pending_decision: Literal["preserve", "assume_delivered"],
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """API host의 empty health/notification/SMS target에 한 번만 import한다."""

    if (
        not FILE_DIGEST_PATTERN.fullmatch(str(expected_bundle_digest or ""))
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_target_state_digest or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_notification_target_state_digest or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_sms_state_digest or "")
        )
    ):
        raise AutomationRecoveryError(
            "device health forward import confirmation is invalid"
        )
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise AutomationRecoveryError("device health forward import time is invalid")
    try:
        raw_bundle, bundle_digest = load_protected_json_file(
            bundle_path,
            label="device health forward bundle",
        )
        if bundle_digest != expected_bundle_digest:
            raise AutomationRecoveryError("device health forward bundle changed")
        bundle = validate_device_health_state_bundle(
            raw_bundle,
            direction="slack_to_api",
        )
    except DeviceHealthStateBundleError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    payload = bundle["payload"]
    _, seed_state, seed_result = _build_device_health_seed_state(
        legacy_state={
            "version": 1,
            "legacyAlertDeliveryEnabled": payload[
                "alertDeliveryEnabled"
            ],
            "alertFingerprints": payload["alertFingerprints"],
            "pendingAlertFingerprints": payload[
                "pendingAlertFingerprints"
            ],
        },
        pending_decision=pending_decision,
        now=actual_now,
    )
    try:
        notification_cursor = build_device_notification_api_cursor(
            payload["notificationState"]
        )
    except DeviceHealthStateBundleError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    notification_seed_state = {
        "cursor": notification_cursor,
        "pendingDeliveries": [],
        "acknowledgedDeliveryIds": [],
        "domainCycleComplete": False,
        "cycleCompleted": False,
    }
    store = JsonAutomationCycleStateStore(state_path)
    health_state_key = _state_key(
        tenant_id,
        "device_health_monitor",
        "continuous",
    )
    notification_state_key = _state_key(
        tenant_id,
        "device_notification_alert",
        "continuous",
    )

    # clean API host도 runtime이 실제 사용할 canonical SMS 두 파일을 먼저
    # 초기화하고 drain해야 한다. 임의의 빈 파일이나 stale cooldown을
    # health/notification seed와 별개로 승인하지 않는다.
    _inspect_exact_drained_sms_recovery_state(
        sms_outbox_path,
        expected_state_digest=expected_sms_state_digest,
        now=actual_now,
        operation="forward import",
    )

    # bundle digest 재확인과 target absent 판정을 한 state flock 안의 CAS
    # writer에 바로 연결해 두 recovery process가 동시에 seed하지 못하게 한다.
    try:
        _, final_bundle_digest = load_protected_json_file(
            bundle_path,
            label="device health forward bundle",
        )
    except DeviceHealthStateBundleError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    if final_bundle_digest != bundle_digest:
        raise AutomationRecoveryError("device health forward bundle changed")

    target_digest = ""
    notification_target_digest = ""

    def _import_absent_targets(
        current_states: dict[str, tuple[bool, dict[str, Any]]],
    ) -> tuple[Mapping[str, Mapping[str, Any]], None]:
        nonlocal target_digest, notification_target_digest
        exists, current = current_states[health_state_key]
        notification_exists, notification_current = current_states[
            notification_state_key
        ]
        target_digest = _automation_target_state_digest(
            current,
            exists=exists,
        )
        notification_target_digest = _automation_target_state_digest(
            notification_current,
            exists=notification_exists,
        )
        if (
            target_digest != expected_target_state_digest
            or notification_target_digest
            != expected_notification_target_state_digest
            or exists
            or notification_exists
        ):
            raise AutomationRecoveryError(
                "device health forward targets are not exact empty state"
            )
        # source bundle도 target document flock 안에서 다시 확인해 두 cycle
        # seed가 한 revision의 exact source에만 결합되게 한다.
        try:
            _, locked_bundle_digest = load_protected_json_file(
                bundle_path,
                label="device health forward bundle",
            )
        except DeviceHealthStateBundleError as exc:
            raise AutomationRecoveryError(str(exc)) from exc
        if locked_bundle_digest != bundle_digest:
            raise AutomationRecoveryError(
                "device health forward bundle changed"
            )
        _inspect_exact_drained_sms_recovery_state(
            sms_outbox_path,
            expected_state_digest=expected_sms_state_digest,
            now=actual_now,
            operation="forward import",
        )
        return {
            health_state_key: seed_state,
            notification_state_key: notification_seed_state,
        }, None

    try:
        store.mutate_cycles(
            (health_state_key, notification_state_key),
            _import_absent_targets,
        )
    except AutomationCycleContractError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    # SMS는 automation document와 별도 lock domain이다. 서비스가 중지된
    # offline 절차에서도 CAS 직후 exact revision을 다시 확인해 target SMS
    # state drift가 seed 성공으로 보고되지 않게 한다.
    final_sms_state = _inspect_exact_drained_sms_recovery_state(
        sms_outbox_path,
        expected_state_digest=expected_sms_state_digest,
        now=actual_now,
        operation="forward import",
    )
    return {
        **seed_result,
        "bundleDigest": bundle_digest,
        "sourceSlackStateDigest": bundle["sourceDigest"],
        "previousTargetStateDigest": target_digest,
        "sourceSlackNotificationStateDigest": bundle["safety"][
            "notificationStateDigest"
        ],
        "previousNotificationTargetStateDigest": notification_target_digest,
        "notificationStateDigest": _automation_target_state_digest(
            notification_seed_state,
            exists=True,
        ),
        "notificationLastSeenId": notification_cursor["lastSeenId"],
        "targetSmsStateDigest": final_sms_state["stateDigest"],
    }


def inspect_device_health_monitor_state(
    *,
    state_path: str | Path,
    tenant_id: str,
) -> Mapping[str, Any]:
    """원문 식별자를 출력하지 않고 offline override 확인값만 반환한다."""

    _validate_identity(
        tenant_id=tenant_id,
        cycle="device_health_monitor",
        cycle_key="continuous",
        request_id="device-health-state-inspect",
    )
    store = JsonAutomationCycleStateStore(state_path)
    state_key = _state_key(
        tenant_id,
        "device_health_monitor",
        "continuous",
    )
    notification_state_key = _state_key(
        tenant_id,
        "device_notification_alert",
        "continuous",
    )
    try:
        with store.locked_snapshot() as snapshot:
            exists, state = snapshot.cycle(state_key)
            notification_exists, notification_state = snapshot.cycle(
                notification_state_key
            )
    except AutomationCycleContractError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    target_state_digest = _automation_target_state_digest(
        state,
        exists=exists,
    )
    notification_target_state_digest = _automation_target_state_digest(
        notification_state,
        exists=notification_exists,
    )
    cursor = state.get("cursor")
    if not isinstance(cursor, Mapping):
        if exists:
            # present-but-malformed target은 forward import가 쓸 수 있는 empty
            # state가 아니며 inspect에서도 정상 미seed로 축약하지 않는다.
            raise AutomationRecoveryError(
                "device health automation state is invalid"
            )
        return {
            "seeded": False,
            "cycle": "device_health_monitor",
            "alertFingerprintCount": 0,
            "pendingFingerprintCount": 0,
            "pendingDeliveryCount": len(state.get("pendingDeliveries") or []),
            "inFlight": isinstance(state.get("inFlight"), Mapping),
            "ackInFlight": isinstance(state.get("ackInFlight"), Mapping),
            "targetStateDigest": target_state_digest,
            "healthStateDigest": target_state_digest,
            "notificationStatePresent": notification_exists,
            "notificationTargetStateDigest": (
                notification_target_state_digest
            ),
        }
    try:
        cursor_digest = device_health_monitor_cursor_digest(cursor)
    except ValueError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    override = cursor.get("alertDeliveryOverride")
    if not isinstance(override, Mapping):
        raise AutomationRecoveryError("device health automation state is invalid")
    return {
        "seeded": True,
        "cycle": "device_health_monitor",
        "alertDeliveryEnabled": bool(override.get("enabled")),
        "alertFingerprintCount": len(cursor.get("alertFingerprints") or {}),
        "pendingFingerprintCount": len(
            cursor.get("pendingAlertFingerprints") or {}
        ),
        "pendingDeliveryCount": len(state.get("pendingDeliveries") or []),
        "inFlight": isinstance(state.get("inFlight"), Mapping),
        "ackInFlight": isinstance(state.get("ackInFlight"), Mapping),
        "cursorDigest": cursor_digest,
        "targetStateDigest": target_state_digest,
        "healthStateDigest": target_state_digest,
        "notificationStatePresent": notification_exists,
        "notificationTargetStateDigest": notification_target_state_digest,
    }


def inspect_device_health_monitor_rollback_source(
    *,
    state_path: str | Path,
    tenant_id: str,
    sms_outbox_path: str | Path,
) -> Mapping[str, Any]:
    """rollback 전 두 API cycle과 SMS outbox의 exact revision을 확인한다."""

    _validate_identity(
        tenant_id=tenant_id,
        cycle="device_health_monitor",
        cycle_key="continuous",
        request_id="device-health-state-rollback-inspect",
    )
    store = JsonAutomationCycleStateStore(state_path)
    try:
        with store.locked_snapshot() as snapshot:
            cycle_states = _inspect_rollback_cycle_states(
                snapshot,
                tenant_id=tenant_id,
                require_drained=False,
            )
    except AutomationCycleContractError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    sms_state = _inspect_strict_sms_recovery_state(sms_outbox_path)
    return {
        **cycle_states["healthSummary"],
        "kind": "device_health_rollback_source",
        "healthStateDigest": cycle_states["healthStateDigest"],
        "notificationStateDigest": cycle_states[
            "notificationStateDigest"
        ],
        "notificationStatePresent": cycle_states[
            "notificationStatePresent"
        ],
        "healthPendingCounts": cycle_states["healthPendingCounts"],
        "notificationPendingCounts": cycle_states[
            "notificationPendingCounts"
        ],
        "smsStateDigest": sms_state["stateDigest"],
        "smsOutboxItemCount": sms_state["outboxItemCount"],
        "smsUnresolvedClaimCount": sms_state[
            "unresolvedClaimCount"
        ],
        "smsSettledClaimCount": sms_state["settledClaimCount"],
        "smsActiveSettledClaimCount": sms_state[
            "activeSettledClaimCount"
        ],
    }


def export_device_health_monitor_rollback_bundle(
    *,
    state_path: str | Path,
    tenant_id: str,
    sms_outbox_path: str | Path,
    bundle_path: str | Path,
    expected_cursor_digest: str,
    expected_health_state_digest: str,
    expected_notification_state_digest: str,
    expected_sms_state_digest: str,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """drained API state exact revision을 Slack용 rollback bundle로 내보낸다."""

    _validate_identity(
        tenant_id=tenant_id,
        cycle="device_health_monitor",
        cycle_key="continuous",
        request_id="device-health-state-rollback-export",
    )
    if (
        not _CURSOR_DIGEST_PATTERN.fullmatch(str(expected_cursor_digest or ""))
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_health_state_digest or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_notification_state_digest or "")
        )
        or not FILE_DIGEST_PATTERN.fullmatch(
            str(expected_sms_state_digest or "")
        )
    ):
        raise AutomationRecoveryError(
            "device health rollback export confirmation is invalid"
        )
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise AutomationRecoveryError("device health rollback export time is invalid")
    store = JsonAutomationCycleStateStore(state_path)
    try:
        # inspect부터 bundle 원자 생성까지 runtime writer와 동일한 state flock을
        # 유지해 health/notification 두 revision의 TOCTOU를 막는다.
        with store.locked_snapshot() as snapshot:
            cycle_states = _inspect_drained_rollback_cycle_states(
                snapshot,
                tenant_id=tenant_id,
            )
            cursor = cycle_states["healthCursor"]
            cursor_digest = cycle_states["cursorDigest"]
            if (
                cursor_digest != expected_cursor_digest
                or cycle_states["healthStateDigest"]
                != expected_health_state_digest
                or cycle_states["notificationStateDigest"]
                != expected_notification_state_digest
            ):
                raise AutomationRecoveryError(
                    "device health rollback API state changed"
                )
            sms_state = _inspect_exact_drained_sms_recovery_state(
                sms_outbox_path,
                expected_state_digest=expected_sms_state_digest,
                now=actual_now,
                operation="rollback export",
            )
            try:
                bundle = build_device_health_rollback_bundle(
                    cursor=cursor,
                    notification_cursor=cycle_states[
                        "notificationCursor"
                    ],
                    source_cursor_digest=cursor_digest,
                    health_state_digest=cycle_states[
                        "healthStateDigest"
                    ],
                    notification_state_digest=cycle_states[
                        "notificationStateDigest"
                    ],
                    sms_state_digest=str(sms_state["stateDigest"]),
                    exported_at=actual_now,
                )
                bundle_digest = create_protected_json_file(
                    bundle_path,
                    bundle,
                    label="device health rollback bundle",
                )
            except DeviceHealthStateBundleError as exc:
                raise AutomationRecoveryError(str(exc)) from exc
            # SMS는 별도 lock domain이라 생성 직후 exact digest를 다시
            # 확인한다. drift 시 만들어진 bundle을 제거하고 fail closed한다.
            try:
                final_sms_state = _inspect_exact_drained_sms_recovery_state(
                    sms_outbox_path,
                    expected_state_digest=expected_sms_state_digest,
                    now=actual_now,
                    operation="rollback export",
                )
            except AutomationRecoveryError:
                Path(bundle_path).unlink(missing_ok=True)
                raise
            if final_sms_state["stateDigest"] != sms_state["stateDigest"]:
                Path(bundle_path).unlink(missing_ok=True)
                raise AutomationRecoveryError(
                    "device health rollback source changed"
                )
    except AutomationCycleContractError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    payload = bundle["payload"]
    return {
        "exported": True,
        "kind": "device_health_rollback_bundle",
        "cursorDigest": cursor_digest,
        "healthStateDigest": cycle_states["healthStateDigest"],
        "notificationStateDigest": cycle_states[
            "notificationStateDigest"
        ],
        "smsStateDigest": sms_state["stateDigest"],
        "bundleDigest": bundle_digest,
        "pendingDecision": payload["pendingDecision"],
        "alertFingerprintCount": len(payload["alertFingerprints"]),
        "pendingFingerprintCount": len(payload["pendingAlertFingerprints"]),
    }


def override_device_health_monitor_alert_delivery(
    *,
    state_path: str | Path,
    tenant_id: str,
    expected_cursor_digest: str,
    expected_enabled: bool,
    enabled: bool,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """중지된 API state의 exact revision에서 delivery override만 바꾼다."""

    _validate_identity(
        tenant_id=tenant_id,
        cycle="device_health_monitor",
        cycle_key="continuous",
        request_id="device-health-state-override",
    )
    if (
        not _CURSOR_DIGEST_PATTERN.fullmatch(expected_cursor_digest)
        or type(expected_enabled) is not bool
        or type(enabled) is not bool
    ):
        raise AutomationRecoveryError("device health override confirmation is invalid")
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise AutomationRecoveryError("device health override time is invalid")

    store = JsonAutomationCycleStateStore(state_path)
    state_key = _state_key(
        tenant_id,
        "device_health_monitor",
        "continuous",
    )

    def _override_exact_state(
        exists: bool,
        state: dict[str, Any],
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        if not exists or any(
            marker in state for marker in ("inFlight", "ackInFlight")
        ):
            raise AutomationRecoveryError(
                "device health automation state is missing or uncertain"
            )
        pending_deliveries = state.get("pendingDeliveries")
        if not isinstance(pending_deliveries, list):
            raise AutomationRecoveryError(
                "device health automation pending deliveries are invalid"
            )
        if pending_deliveries:
            # 이미 API가 만든 delivery는 override와 무관하게 다음 poll에서 다시
            # 반환되므로 운영자가 ack/전달 상태를 먼저 확정해야 한다.
            raise AutomationRecoveryError(
                "device health automation has pending deliveries"
            )
        cursor = state.get("cursor")
        if not isinstance(cursor, Mapping):
            raise AutomationRecoveryError(
                "device health automation state is not seeded"
            )
        try:
            current_digest = device_health_monitor_cursor_digest(cursor)
        except ValueError as exc:
            raise AutomationRecoveryError(str(exc)) from exc
        override = cursor.get("alertDeliveryOverride")
        current_enabled = (
            override.get("enabled")
            if isinstance(override, Mapping)
            else None
        )
        if (
            current_digest != expected_cursor_digest
            or current_enabled != expected_enabled
        ):
            raise AutomationRecoveryError(
                "device health override state changed"
            )
        if current_enabled == enabled:
            return state, {
                "updated": False,
                "cycle": "device_health_monitor",
                "previousEnabled": current_enabled,
                "alertDeliveryEnabled": enabled,
                "cursorDigest": current_digest,
            }
        try:
            updated_cursor = (
                update_device_health_monitor_alert_delivery_override(
                    cursor,
                    enabled=enabled,
                    updated_at=actual_now,
                )
            )
            updated_digest = device_health_monitor_cursor_digest(
                updated_cursor
            )
        except ValueError as exc:
            raise AutomationRecoveryError(str(exc)) from exc
        next_state = {
            **state,
            "cursor": updated_cursor,
            "domainCycleComplete": False,
            "cycleCompleted": False,
        }
        # 과거 response cache가 이전 override 의미를 재노출하지 않게 닫는다.
        next_state.pop("lastRequestId", None)
        next_state.pop("lastResult", None)
        return next_state, {
            "updated": True,
            "cycle": "device_health_monitor",
            "previousEnabled": current_enabled,
            "alertDeliveryEnabled": enabled,
            "cursorDigest": updated_digest,
        }

    try:
        return store.mutate_cycle(state_key, _override_exact_state)
    except AutomationCycleContractError as exc:
        raise AutomationRecoveryError(str(exc)) from exc


def resolve_automation_uncertain_state(
    *,
    state_path: str | Path,
    tenant_id: str,
    cycle: str,
    cycle_key: str,
    expected_request_id: str,
    decision: Literal["retry", "assume_completed"],
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """서비스 중지 뒤 exact marker만 명시적으로 해제한다.

    자동화 mutation은 완료 여부를 추측할 수 없으므로 timeout만으로 marker를
    지우지 않는다. 운영자가 MDA·provider·Sheet 상태를 확인한 뒤 exact request
    ID와 결정을 전달했을 때만 이 함수가 state를 원자 교체한다.
    """

    _validate_identity(
        tenant_id=tenant_id,
        cycle=cycle,
        cycle_key=cycle_key,
        request_id=expected_request_id,
    )
    if decision not in {"retry", "assume_completed"}:
        raise AutomationRecoveryError("automation resolution is invalid")
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise AutomationRecoveryError("automation resolution time is invalid")

    store = JsonAutomationCycleStateStore(state_path)
    state_key = _state_key(tenant_id, cycle, cycle_key)

    def _resolve_exact_marker(
        exists: bool,
        state: dict[str, Any],
    ) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
        if not exists:
            raise AutomationRecoveryError(
                "automation state does not contain one uncertain marker"
            )
        markers = [
            (name, state.get(name))
            for name in ("inFlight", "ackInFlight")
            if isinstance(state.get(name), dict)
        ]
        # marker key가 있으나 dict가 아닌 것도 malformed state라 자동으로
        # absent 취급하지 않고 같은 fail-closed 경계에서 멈춘다.
        if len(markers) != 1 or any(
            name in state and not isinstance(state.get(name), dict)
            for name in ("inFlight", "ackInFlight")
        ):
            raise AutomationRecoveryError(
                "automation state does not contain one uncertain marker"
            )
        marker_name, marker = markers[0]
        assert isinstance(marker, dict)
        if str(marker.get("requestId") or "") != expected_request_id:
            raise AutomationRecoveryError(
                "automation request id does not match"
            )

        next_state = dict(state)
        if decision == "assume_completed":
            if marker_name == "ackInFlight":
                next_state = _assume_delivery_ack_completed(
                    next_state,
                    marker,
                )
            else:
                raise AutomationRecoveryError(
                    "in-flight automation requires a verified retry or cursor recovery"
                )
        next_state.pop(marker_name, None)
        history = [
            dict(item)
            for item in (next_state.get("manualResolutions") or [])
            if isinstance(item, dict)
        ]
        history.append(
            {
                "marker": marker_name,
                "decision": decision,
                "requestHash": hashlib.sha256(
                    expected_request_id.encode("utf-8")
                ).hexdigest()[:24],
                "resolvedAt": actual_now.astimezone(timezone.utc).isoformat(),
            }
        )
        next_state["manualResolutions"] = history[
            -_MAX_RESOLUTION_HISTORY:
        ]
        return next_state, {
            "resolved": True,
            "cycle": cycle,
            "marker": marker_name,
            "decision": decision,
            "requestHash": history[-1]["requestHash"],
        }

    try:
        return store.mutate_cycle(state_key, _resolve_exact_marker)
    except AutomationCycleContractError as exc:
        raise AutomationRecoveryError(str(exc)) from exc


def resolve_automatic_sms_uncertain_claim(
    *,
    outbox_path: str | Path,
    device_name: str,
    alert_category: str,
    allow_accepted: bool = False,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """provider 확인 뒤 hash로 식별한 SMS claim 한 건만 settle한다."""

    path = Path(outbox_path).expanduser()
    if not path.is_absolute() or path == Path("/"):
        raise AutomationRecoveryError("sms recovery outbox path is invalid")
    if (
        not _SMS_DEVICE_PATTERN.fullmatch(str(device_name or ""))
        or not _SMS_CATEGORY_PATTERN.fullmatch(str(alert_category or ""))
    ):
        raise AutomationRecoveryError("sms recovery identity is invalid")
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise AutomationRecoveryError("sms recovery time is invalid")
    # arbitrary/missing sidecar를 빈 정본으로 만든 뒤 claim을 해제하지 않도록
    # configured canonical 두 파일을 먼저 strict 검사한다.
    _inspect_strict_sms_recovery_state(path)
    try:
        result = settle_automatic_sms_delivery_claim_for_recovery(
            device_name,
            alert_category,
            settled_at=actual_now,
            allow_accepted=allow_accepted,
            outbox_path=path,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    _inspect_strict_sms_recovery_state(path)
    return {"resolved": True, "kind": "sms_claim", **result}


def _assume_delivery_ack_completed(
    state: Mapping[str, Any],
    marker: Mapping[str, Any],
) -> dict[str, Any]:
    raw_delivery_ids = marker.get("deliveryIds")
    if not isinstance(raw_delivery_ids, list) or any(
        not isinstance(value, str) or not value.strip()
        for value in raw_delivery_ids
    ):
        raise AutomationRecoveryError(
            "automation acknowledgement marker has invalid deliveries"
        )
    delivery_ids = {
        str(value).strip()
        for value in raw_delivery_ids
        if str(value).strip()
    }
    if not delivery_ids or len(delivery_ids) != len(raw_delivery_ids):
        raise AutomationRecoveryError(
            "automation acknowledgement marker has no deliveries"
        )
    next_state = dict(state)
    raw_pending = next_state.get("pendingDeliveries")
    if not isinstance(raw_pending, list) or any(
        not isinstance(item, Mapping)
        or not isinstance(item.get("deliveryId"), str)
        or not item.get("deliveryId")
        for item in raw_pending
    ):
        raise AutomationRecoveryError(
            "automation pending deliveries are invalid"
        )
    pending = [
        dict(item)
        for item in raw_pending
    ]
    pending_ids = {
        str(item.get("deliveryId") or "") for item in pending
    }
    if not delivery_ids.issubset(pending_ids):
        raise AutomationRecoveryError(
            "automation acknowledgement deliveries do not match"
        )
    next_state["pendingDeliveries"] = [
        item
        for item in pending
        if str(item.get("deliveryId") or "") not in delivery_ids
    ]
    raw_acknowledged = next_state.get("acknowledgedDeliveryIds") or []
    if not isinstance(raw_acknowledged, list) or any(
        not isinstance(value, str) or not value
        for value in raw_acknowledged
    ):
        raise AutomationRecoveryError(
            "automation acknowledged deliveries are invalid"
        )
    acknowledged = list(raw_acknowledged)
    next_state["acknowledgedDeliveryIds"] = (
        acknowledged + sorted(delivery_ids)
    )[-500:]

    # Health/notification ack outbox도 같은 delivery ID를 사용한다. 외부
    # Sheet 반영을 확인한 운영자 결정일 때만 내부 pending context를 닫는다.
    raw_cursor = next_state.get("cursor")
    if not isinstance(raw_cursor, Mapping):
        raise AutomationRecoveryError("automation cursor is invalid")
    cursor = dict(raw_cursor)
    for key in ("pendingSheetAlerts", "pendingDeliveryContexts"):
        if key not in cursor:
            continue
        values = cursor.get(key)
        if not isinstance(values, Mapping):
            raise AutomationRecoveryError(
                "automation acknowledgement cursor outbox is invalid"
            )
        if isinstance(values, Mapping):
            cursor[key] = {
                item_key: item_value
                for item_key, item_value in values.items()
                if str(item_key) not in delivery_ids
            }
    next_state["cursor"] = cursor
    if (
        not next_state["pendingDeliveries"]
        and next_state.get("domainCycleComplete")
    ):
        next_state["cycleCompleted"] = True
    next_state.pop("lastRequestId", None)
    next_state.pop("lastResult", None)
    return next_state


def _state_key(tenant_id: str, cycle: str, cycle_key: str) -> str:
    raw = "\0".join((tenant_id, cycle, cycle_key))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _automation_target_state_digest(
    state: Mapping[str, Any],
    *,
    exists: bool,
) -> str:
    """absent와 present-empty도 구분하는 cycle state CAS digest다."""

    return hashlib.sha256(
        json.dumps(
            {"exists": exists, "state": dict(state)},
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()


def _build_device_health_seed_state(
    *,
    legacy_state: Mapping[str, Any],
    pending_decision: Literal["preserve", "assume_delivered"],
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """검토한 legacy payload를 저장 직전까지 완전히 검증한다."""

    if now.tzinfo is None:
        raise AutomationRecoveryError("device health seed time is invalid")
    payload = _validate_device_health_seed_payload(legacy_state)
    try:
        cursor = build_device_health_monitor_seed_cursor(
            legacy_alert_delivery_enabled=payload[
                "legacyAlertDeliveryEnabled"
            ],
            alert_fingerprints=payload["alertFingerprints"],
            pending_alert_fingerprints=payload[
                "pendingAlertFingerprints"
            ],
            pending_decision=pending_decision,
            seeded_at=now,
        )
        cursor_digest = device_health_monitor_cursor_digest(cursor)
    except (TypeError, ValueError) as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    state = {
        "cursor": cursor,
        "pendingDeliveries": [],
        "acknowledgedDeliveryIds": [],
        "domainCycleComplete": False,
        "cycleCompleted": False,
    }
    return cursor, state, {
        "seeded": True,
        "cycle": "device_health_monitor",
        "alertDeliveryEnabled": bool(
            cursor["alertDeliveryOverride"]["enabled"]
        ),
        "alertFingerprintCount": len(cursor["alertFingerprints"]),
        "pendingFingerprintCount": len(
            cursor["pendingAlertFingerprints"]
        ),
        "pendingDecision": pending_decision,
        "cursorDigest": cursor_digest,
        "healthStateDigest": _automation_target_state_digest(
            state,
            exists=True,
        ),
    }


def _validate_device_health_seed_payload(
    value: Mapping[str, Any],
) -> dict[str, Any]:
    if (
        not isinstance(value, Mapping)
        or set(value) != _DEVICE_HEALTH_SEED_KEYS
        or type(value.get("version")) is not int
        or value.get("version") != 1
        or type(value.get("legacyAlertDeliveryEnabled")) is not bool
        or not isinstance(value.get("alertFingerprints"), Mapping)
        or not isinstance(value.get("pendingAlertFingerprints"), Mapping)
    ):
        raise AutomationRecoveryError("device health seed payload is invalid")
    return {
        "legacyAlertDeliveryEnabled": value[
            "legacyAlertDeliveryEnabled"
        ],
        "alertFingerprints": dict(value["alertFingerprints"]),
        "pendingAlertFingerprints": dict(
            value["pendingAlertFingerprints"]
        ),
    }


def _inspect_drained_rollback_cycle_states(
    snapshot: AutomationStateSnapshot,
    *,
    tenant_id: str,
) -> dict[str, Any]:
    return _inspect_rollback_cycle_states(
        snapshot,
        tenant_id=tenant_id,
        require_drained=True,
    )


def _inspect_rollback_cycle_states(
    snapshot: AutomationStateSnapshot,
    *,
    tenant_id: str,
    require_drained: bool,
) -> dict[str, Any]:
    """health/notification raw state를 정규화 없이 검사하고 digest한다."""

    health_key = _state_key(
        tenant_id,
        "device_health_monitor",
        "continuous",
    )
    health_exists, health_state = snapshot.cycle(health_key)
    health_state_digest = _automation_target_state_digest(
        health_state,
        exists=health_exists,
    )
    if not health_exists:
        raise AutomationRecoveryError(
            "device health rollback source is not seeded"
        )
    health_markers = _strict_marker_counts(
        health_state,
        label="device health automation",
    )
    health_pending = health_state.get("pendingDeliveries")
    if not isinstance(health_pending, list):
        raise AutomationRecoveryError(
            "device health automation pending deliveries are invalid"
        )
    cursor = health_state.get("cursor")
    if not isinstance(cursor, Mapping):
        raise AutomationRecoveryError(
            "device health automation state is not seeded"
        )
    try:
        cursor_digest = device_health_monitor_cursor_digest(cursor)
    except ValueError as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    health_outbox_counts = _strict_pending_cursor_counts(
        cursor,
        label="device health rollback cursor",
    )

    notification_key = _state_key(
        tenant_id,
        "device_notification_alert",
        "continuous",
    )
    notification_exists, notification_state = snapshot.cycle(
        notification_key
    )
    notification_state_digest = _automation_target_state_digest(
        notification_state,
        exists=notification_exists,
    )
    if not notification_exists:
        raise AutomationRecoveryError(
            "device notification rollback source is not seeded"
        )
    notification_markers = _strict_marker_counts(
        notification_state,
        label="device notification automation",
    )
    raw_pending = notification_state.get("pendingDeliveries")
    if not isinstance(raw_pending, list):
        raise AutomationRecoveryError(
            "device notification pending deliveries are invalid"
        )
    notification_pending = raw_pending
    notification_cursor = notification_state.get("cursor")
    if not isinstance(notification_cursor, Mapping):
        raise AutomationRecoveryError(
            "device notification automation cursor is invalid"
        )
    notification_outbox_counts = _strict_pending_cursor_counts(
        notification_cursor,
        label="device notification rollback cursor",
    )
    if not any(notification_outbox_counts.values()):
        try:
            # drained cursor는 inspect도 export와 같은 strict schema를 써
            # malformed incident를 정상 count로 축약하지 않는다.
            build_device_notification_legacy_state(notification_cursor)
        except DeviceHealthStateBundleError as exc:
            raise AutomationRecoveryError(str(exc)) from exc

    if require_drained and any(health_markers.values()):
        raise AutomationRecoveryError(
            "device health automation state is missing or uncertain"
        )
    if require_drained and health_pending:
        raise AutomationRecoveryError(
            "device health automation has pending deliveries"
        )
    if require_drained and any(health_outbox_counts.values()):
        raise AutomationRecoveryError(
            "device health rollback cursor outbox is not drained"
        )
    if require_drained and any(notification_markers.values()):
        raise AutomationRecoveryError(
            "device notification automation state is uncertain"
        )
    if require_drained and notification_pending:
        raise AutomationRecoveryError(
            "device notification automation has pending deliveries"
        )
    if require_drained and any(notification_outbox_counts.values()):
        raise AutomationRecoveryError(
            "device notification rollback cursor outbox is not drained"
        )

    override = cursor.get("alertDeliveryOverride")
    if not isinstance(override, Mapping) or type(override.get("enabled")) is not bool:
        raise AutomationRecoveryError(
            "device health automation state is invalid"
        )
    health_summary = {
        "seeded": True,
        "cycle": "device_health_monitor",
        "alertDeliveryEnabled": override["enabled"],
        "alertFingerprintCount": len(cursor.get("alertFingerprints") or {}),
        "pendingFingerprintCount": len(
            cursor.get("pendingAlertFingerprints") or {}
        ),
        "pendingDeliveryCount": len(health_pending),
        "inFlight": bool(health_markers["inFlightCount"]),
        "ackInFlight": bool(health_markers["ackInFlightCount"]),
        "cursorDigest": cursor_digest,
        "targetStateDigest": health_state_digest,
    }
    return {
        "healthCursor": dict(cursor),
        "cursorDigest": cursor_digest,
        "healthStateDigest": health_state_digest,
        "notificationStateDigest": notification_state_digest,
        "notificationStatePresent": notification_exists,
        "notificationCursor": dict(notification_cursor),
        "healthSummary": health_summary,
        "healthPendingCounts": {
            **health_markers,
            "pendingDeliveryCount": len(health_pending),
            **health_outbox_counts,
        },
        "notificationPendingCounts": {
            **notification_markers,
            "pendingDeliveryCount": len(notification_pending),
            **notification_outbox_counts,
        },
    }


def _strict_marker_counts(
    state: Mapping[str, Any],
    *,
    label: str,
) -> dict[str, int]:
    result: dict[str, int] = {}
    for key, result_key in (
        ("inFlight", "inFlightCount"),
        ("ackInFlight", "ackInFlightCount"),
    ):
        if key not in state:
            result[result_key] = 0
            continue
        if not isinstance(state.get(key), Mapping):
            raise AutomationRecoveryError(
                f"{label} state is missing or uncertain"
            )
        result[result_key] = 1
    return result


def _strict_pending_cursor_counts(
    cursor: Mapping[str, Any],
    *,
    label: str,
) -> dict[str, int]:
    result: dict[str, int] = {}
    for key, result_key in (
        ("pendingDeliveryContexts", "pendingDeliveryContextCount"),
        ("pendingSheetAlerts", "pendingSheetAlertCount"),
        ("pendingSheetRepairs", "pendingSheetRepairCount"),
    ):
        if key not in cursor:
            result[result_key] = 0
            continue
        value = cursor.get(key)
        if not isinstance(value, Mapping):
            raise AutomationRecoveryError(f"{label} outbox is invalid")
        result[result_key] = len(value)
    return result


def _inspect_strict_sms_recovery_state(
    sms_outbox_path: str | Path,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """configured canonical SMS files만 initialized/protected로 인정한다."""

    configured_path = str(os.getenv("SMS_DELIVERY_OUTBOX_PATH", "")).strip()
    if not configured_path:
        configured_path = "/var/lib/boxer-company-api/sms_delivery_outbox.json"
    try:
        return inspect_automatic_sms_recovery_state(
            outbox_path=sms_outbox_path,
            expected_outbox_path=configured_path,
            require_initialized=True,
            now=now,
        )
    except (OSError, RuntimeError, ValueError) as exc:
        raise AutomationRecoveryError(
            "device health SMS state is unreadable"
        ) from exc


def _inspect_exact_drained_sms_recovery_state(
    sms_outbox_path: str | Path,
    *,
    expected_state_digest: str,
    now: datetime,
    operation: str,
) -> dict[str, Any]:
    """한 canonical SMS revision에서 outbox·claim·cooldown을 모두 drain한다."""

    state = _inspect_strict_sms_recovery_state(
        sms_outbox_path,
        now=now,
    )
    if (
        state["stateDigest"] != expected_state_digest
        or state["outboxItemCount"] != 0
        or state["unresolvedClaimCount"] != 0
        or state["activeSettledClaimCount"] != 0
    ):
        raise AutomationRecoveryError(
            f"device health {operation} SMS outbox is not exact drained state"
        )
    return state


def _parse_cli_bool(value: str | None, *, field: str) -> bool:
    normalized = str(value or "").strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise AutomationRecoveryError(f"{field} must be true or false")


def _validate_identity(
    *,
    tenant_id: str,
    cycle: str,
    cycle_key: str,
    request_id: str,
) -> None:
    if (
        not _IDENTIFIER_PATTERN.fullmatch(str(tenant_id or ""))
        or cycle not in _CYCLES
        or not _CYCLE_KEY_PATTERN.fullmatch(str(cycle_key or ""))
        or not _REQUEST_ID_PATTERN.fullmatch(str(request_id or ""))
    ):
        raise AutomationRecoveryError("automation identity is invalid")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Resolve one Boxer automation marker after external state "
            "verification and API service shutdown."
        )
    )
    parser.add_argument(
        "--state-path",
        default=os.getenv("BOXER_COMPANY_API_AUTOMATION_STATE_PATH", ""),
    )
    parser.add_argument("--tenant-id")
    parser.add_argument("--cycle", choices=sorted(_CYCLES))
    parser.add_argument("--cycle-key")
    parser.add_argument("--expected-request-id")
    parser.add_argument(
        "--decision",
        choices=("retry", "assume_completed"),
    )
    parser.add_argument("--sms-outbox-path")
    parser.add_argument("--sms-device-name")
    parser.add_argument("--sms-alert-category")
    parser.add_argument(
        "--allow-accepted-sms-claim",
        action="store_true",
        help="Allow settling an accepted claim after outbox/provider verification.",
    )
    parser.add_argument(
        "--confirm-provider-verified",
        action="store_true",
        help="Confirm that provider/outbox state was verified before SMS recovery.",
    )
    parser.add_argument(
        "--confirm-service-stopped",
        action="store_true",
        help="Confirm that boxer-company-api is stopped before editing state.",
    )
    parser.add_argument("--import-device-health-forward-bundle-path")
    parser.add_argument("--export-device-health-rollback-bundle-path")
    parser.add_argument("--expected-bundle-digest")
    parser.add_argument("--expected-target-state-digest")
    parser.add_argument("--expected-notification-target-state-digest")
    parser.add_argument("--expected-health-state-digest")
    parser.add_argument("--expected-notification-state-digest")
    parser.add_argument("--expected-sms-state-digest")
    parser.add_argument(
        "--pending-decision",
        choices=("preserve", "assume_delivered"),
    )
    parser.add_argument(
        "--inspect-device-health-state",
        action="store_true",
    )
    parser.add_argument(
        "--inspect-device-health-rollback-source",
        action="store_true",
    )
    parser.add_argument(
        "--set-alert-delivery-enabled",
        choices=("true", "false"),
    )
    parser.add_argument(
        "--expected-alert-delivery-enabled",
        choices=("true", "false"),
    )
    parser.add_argument("--expected-cursor-digest")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    sms_mode = any(
        (
            args.sms_device_name,
            args.sms_alert_category,
        )
    )
    device_health_modes = sum(
        bool(value)
        for value in (
            args.import_device_health_forward_bundle_path,
            args.export_device_health_rollback_bundle_path,
            args.inspect_device_health_state,
            args.inspect_device_health_rollback_source,
            args.set_alert_delivery_enabled,
        )
    )
    if device_health_modes > 1 or (sms_mode and device_health_modes):
        raise SystemExit("automation_recovery_mode_conflict")
    if not args.confirm_service_stopped:
        raise SystemExit("automation_recovery_requires_stopped_service")
    try:
        if args.import_device_health_forward_bundle_path:
            if not all(
                (
                    args.state_path,
                    args.tenant_id,
                    args.pending_decision,
                    args.expected_bundle_digest,
                    args.expected_target_state_digest,
                    args.expected_notification_target_state_digest,
                    args.sms_outbox_path,
                    args.expected_sms_state_digest,
                )
            ):
                raise AutomationRecoveryError(
                    "device health forward import confirmation is incomplete"
                )
            result = import_device_health_monitor_forward_bundle(
                state_path=args.state_path,
                tenant_id=args.tenant_id,
                bundle_path=args.import_device_health_forward_bundle_path,
                expected_bundle_digest=args.expected_bundle_digest,
                expected_target_state_digest=args.expected_target_state_digest,
                expected_notification_target_state_digest=(
                    args.expected_notification_target_state_digest
                ),
                sms_outbox_path=args.sms_outbox_path,
                expected_sms_state_digest=args.expected_sms_state_digest,
                pending_decision=args.pending_decision,
            )
        elif args.inspect_device_health_state:
            if not all((args.state_path, args.tenant_id)):
                raise AutomationRecoveryError(
                    "device health inspect identity is incomplete"
                )
            result = inspect_device_health_monitor_state(
                state_path=args.state_path,
                tenant_id=args.tenant_id,
            )
        elif args.inspect_device_health_rollback_source:
            if not all(
                (args.state_path, args.tenant_id, args.sms_outbox_path)
            ):
                raise AutomationRecoveryError(
                    "device health rollback inspect identity is incomplete"
                )
            result = inspect_device_health_monitor_rollback_source(
                state_path=args.state_path,
                tenant_id=args.tenant_id,
                sms_outbox_path=args.sms_outbox_path,
            )
        elif args.export_device_health_rollback_bundle_path:
            if not all(
                (
                    args.state_path,
                    args.tenant_id,
                    args.expected_cursor_digest,
                    args.expected_health_state_digest,
                    args.expected_notification_state_digest,
                    args.sms_outbox_path,
                    args.expected_sms_state_digest,
                )
            ):
                raise AutomationRecoveryError(
                    "device health rollback export confirmation is incomplete"
                )
            result = export_device_health_monitor_rollback_bundle(
                state_path=args.state_path,
                tenant_id=args.tenant_id,
                sms_outbox_path=args.sms_outbox_path,
                bundle_path=args.export_device_health_rollback_bundle_path,
                expected_cursor_digest=args.expected_cursor_digest,
                expected_health_state_digest=(
                    args.expected_health_state_digest
                ),
                expected_notification_state_digest=(
                    args.expected_notification_state_digest
                ),
                expected_sms_state_digest=args.expected_sms_state_digest,
            )
        elif args.set_alert_delivery_enabled:
            if not all(
                (
                    args.state_path,
                    args.tenant_id,
                    args.expected_alert_delivery_enabled,
                    args.expected_cursor_digest,
                )
            ):
                raise AutomationRecoveryError(
                    "device health override confirmation is incomplete"
                )
            result = override_device_health_monitor_alert_delivery(
                state_path=args.state_path,
                tenant_id=args.tenant_id,
                expected_cursor_digest=args.expected_cursor_digest,
                expected_enabled=_parse_cli_bool(
                    args.expected_alert_delivery_enabled,
                    field="expected alert delivery enabled",
                ),
                enabled=_parse_cli_bool(
                    args.set_alert_delivery_enabled,
                    field="alert delivery enabled",
                ),
            )
        elif sms_mode:
            if not all(
                (
                    args.sms_outbox_path,
                    args.sms_device_name,
                    args.sms_alert_category,
                    args.confirm_provider_verified,
                )
            ):
                raise AutomationRecoveryError(
                    "sms recovery requires exact target and provider verification"
                )
            result = resolve_automatic_sms_uncertain_claim(
                outbox_path=args.sms_outbox_path,
                device_name=args.sms_device_name,
                alert_category=args.sms_alert_category,
                allow_accepted=args.allow_accepted_sms_claim,
            )
        else:
            if not all(
                (
                    args.state_path,
                    args.tenant_id,
                    args.cycle,
                    args.cycle_key,
                    args.expected_request_id,
                    args.decision,
                )
            ):
                raise AutomationRecoveryError(
                    "automation recovery identity is incomplete"
                )
            result = resolve_automation_uncertain_state(
                state_path=args.state_path,
                tenant_id=args.tenant_id,
                cycle=args.cycle,
                cycle_key=args.cycle_key,
                expected_request_id=args.expected_request_id,
                decision=args.decision,
            )
    except (AutomationRecoveryError, AutomationCycleContractError) as exc:
        raise SystemExit(str(exc)) from exc
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


__all__ = [
    "AutomationRecoveryError",
    "export_device_health_monitor_rollback_bundle",
    "import_device_health_monitor_forward_bundle",
    "inspect_device_health_monitor_state",
    "inspect_device_health_monitor_rollback_source",
    "main",
    "override_device_health_monitor_alert_delivery",
    "resolve_automatic_sms_uncertain_claim",
    "resolve_automation_uncertain_state",
    "seed_device_health_monitor_state",
]
