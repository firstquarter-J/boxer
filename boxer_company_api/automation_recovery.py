from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
from typing import Any, Literal, Mapping, Sequence

from boxer_company.sms_delivery_cycle import (
    inspect_automatic_sms_recovery_state,
    settle_automatic_sms_delivery_claim_for_recovery,
)
from boxer_company.device_health_monitor_cycle import (
    build_clean_device_health_monitor_cursor,
    device_health_monitor_cursor_digest,
    update_device_health_monitor_alert_delivery_override,
)
from boxer_company.automation import AutomationCycleContractError
from boxer_company.protected_json import (
    create_protected_json_file,
    ProtectedJsonFileError,
)
from boxer_company_api.automation import (
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


class AutomationRecoveryError(RuntimeError):
    """불명 상태를 안전하게 식별하거나 해제할 수 없을 때 발생한다."""


def initialize_clean_automation_state(
    *,
    state_path: str | Path,
    tenant_id: str,
    initial_alert_delivery_enabled: bool,
    now: datetime | None = None,
) -> Mapping[str, Any]:
    """새 API host에 전체 automation document를 create-only로 초기화한다."""

    _validate_identity(
        tenant_id=tenant_id,
        cycle="device_health_monitor",
        cycle_key="continuous",
        request_id="automation-state-initialize",
    )
    if type(initial_alert_delivery_enabled) is not bool:
        raise AutomationRecoveryError(
            "initial alert delivery setting is invalid"
        )
    actual_now = now or datetime.now(timezone.utc)
    if actual_now.tzinfo is None:
        raise AutomationRecoveryError("automation initialization time is invalid")

    # 신규 host에는 과거 Slack fingerprint를 추측해 복원하지 않는다. 빈
    # dedupe와 명시적 alert override만 기존 cursor 계약으로 함께 만든다.
    health_state, result = _build_clean_device_health_state(
        initial_alert_delivery_enabled=initial_alert_delivery_enabled,
        now=actual_now,
    )
    state_key = _state_key(
        tenant_id,
        "device_health_monitor",
        "continuous",
    )
    document = {
        "version": 1,
        "cycles": {state_key: health_state},
    }
    try:
        state_digest = create_protected_json_file(
            state_path,
            document,
            label="automation state",
        )
        store = JsonAutomationCycleStateStore(state_path)
        with store.locked_snapshot() as snapshot:
            exists, stored_health_state = snapshot.cycle(state_key)
            if (
                not snapshot.exists
                or snapshot.digest != state_digest
                or snapshot.document != document
                or not exists
                or stored_health_state != health_state
            ):
                # 생성 후 검증 실패 시 자동 삭제하지 않는다. 운영자가 원본
                # revision을 보존한 채 원인을 확인해야 재초기화를 막을 수 있다.
                raise AutomationRecoveryError(
                    "automation state initialization verification failed"
                )
    except (ProtectedJsonFileError, AutomationCycleContractError) as exc:
        raise AutomationRecoveryError(str(exc)) from exc
    return {
        **result,
        "initialized": True,
        "automationStateDigest": state_digest,
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
            # present-but-malformed target은 신규 초기화 대상이 아니며
            # inspect에서도 정상 미seed로 축약하지 않는다.
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


def _build_clean_device_health_state(
    *,
    initial_alert_delivery_enabled: bool,
    now: datetime,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """과거 source/export 없이 신규 host의 최소 health state만 만든다."""

    if now.tzinfo is None:
        raise AutomationRecoveryError("device health state time is invalid")
    try:
        cursor = build_clean_device_health_monitor_cursor(
            alert_delivery_enabled=initial_alert_delivery_enabled,
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
    return state, {
        "seeded": True,
        "cycle": "device_health_monitor",
        "alertDeliveryEnabled": bool(
            cursor["alertDeliveryOverride"]["enabled"]
        ),
        "alertFingerprintCount": len(cursor["alertFingerprints"]),
        "pendingFingerprintCount": len(
            cursor["pendingAlertFingerprints"]
        ),
        "cursorDigest": cursor_digest,
        "healthStateDigest": _automation_target_state_digest(
            state,
            exists=True,
        ),
    }


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
    parser.add_argument(
        "--initialize-clean-automation-state",
        action="store_true",
    )
    parser.add_argument(
        "--initial-alert-delivery-enabled",
        choices=("true", "false"),
    )
    parser.add_argument(
        "--inspect-device-health-state",
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
            args.initialize_clean_automation_state,
            args.inspect_device_health_state,
            args.set_alert_delivery_enabled,
        )
    )
    if device_health_modes > 1 or (sms_mode and device_health_modes):
        raise SystemExit("automation_recovery_mode_conflict")
    if not args.confirm_service_stopped:
        raise SystemExit("automation_recovery_requires_stopped_service")
    try:
        if args.initialize_clean_automation_state:
            if not all(
                (
                    args.state_path,
                    args.tenant_id,
                    args.initial_alert_delivery_enabled,
                )
            ):
                raise AutomationRecoveryError(
                    "automation initialization confirmation is incomplete"
                )
            result = initialize_clean_automation_state(
                state_path=args.state_path,
                tenant_id=args.tenant_id,
                initial_alert_delivery_enabled=_parse_cli_bool(
                    args.initial_alert_delivery_enabled,
                    field="initial alert delivery enabled",
                ),
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
    "initialize_clean_automation_state",
    "inspect_device_health_monitor_state",
    "main",
    "override_device_health_monitor_alert_delivery",
    "resolve_automatic_sms_uncertain_claim",
    "resolve_automation_uncertain_state",
]
