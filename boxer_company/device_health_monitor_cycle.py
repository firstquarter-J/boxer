from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import hashlib
import json
import logging
import re
from typing import Any, Callable, Mapping, Sequence

from boxer.core import settings as core_settings
from boxer.core.utils import _display_value
from boxer.retrieval.connectors.db import _create_db_connection
from boxer.retrieval.connectors.s3 import _load_boto3_components
from boxer_company import settings as company_settings
from boxer_company.assistant.device_health_alert_action_route import (
    DeviceHealthAlertActionTarget,
    _build_device_health_alert_sms_guide,
    _is_mobile_phone_number,
    _normalize_phone_number,
    _send_device_health_alert_sms,
)
from boxer_company.daily_device_round import (
    _build_daily_device_round_issue_summary,
    _build_daily_device_round_priority,
    _build_daily_device_round_storage_details,
    _coerce_daily_device_round_now,
    _daily_device_round_status_label,
)
from boxer_company.device_health_sheet import (
    _append_device_health_sheet_alerts,
    _load_device_health_sheet_captureboard_incidents,
)
from boxer_company.device_health_fingerprint import (
    canonical_device_health_alert_fingerprint,
    canonicalize_device_health_alert_fingerprint_key,
    validate_and_canonicalize_device_health_alert_fingerprint_key,
)
from boxer_company.redis_device_state import DeviceStateRedisClient
from boxer_company.routers.device_status_probe import (
    _build_trashcan_storage_summary_from_checks,
    _collect_runtime_checks,
    _parse_device_path_list,
    _parse_pm2_processes,
    _parse_usb_devices,
    _parse_voice_config,
    _summarize_audio_path_probe,
    _summarize_captureboard_probe,
    _summarize_led_probe,
    _summarize_pm2_probe,
)
from boxer_company.sms_delivery import (
    _SMS_DELIVERY_ACCEPTED,
    _SMS_DELIVERY_CONFIRM_REQUIRED,
    _SMS_DELIVERY_DELIVERED,
    _SMS_DELIVERY_FAILED,
    _SMS_DELIVERY_NOT_SENT,
    _SMS_DELIVERY_REQUEST_FAILED,
)
from boxer_company.sms_delivery_cycle import (
    claim_automatic_sms_delivery,
    hold_automatic_sms_delivery_claim,
    remember_sms_delivery_sheet_record,
)


_MONITOR_OPTION_KEYS: frozenset[str] = frozenset()
_MONITOR_STATE_OWNER = "company_api"
_MONITOR_STATE_VERSION = 1
_MONITOR_PENDING_DECISIONS = frozenset({"preserve", "assume_delivered"})
_MAX_MIGRATED_FINGERPRINTS = 10_000
_COMPONENT_KEYS = ("audio", "pm2", "storage", "captureboard", "led")
_COMPONENT_LABELS = {
    "audio": "스피커",
    "pm2": "PM2",
    "storage": "저장 공간",
    "captureboard": "캡처보드",
    "led": "LED",
}
_COMPONENT_CATEGORIES = {
    "audio": "audio",
    "pm2": "application",
    "storage": "storage",
    "captureboard": "video_signal",
    "led": "led",
}
_CAPTUREBOARD_OPEN_STATUSES = frozenset({"대기", "처리중", "진행중"})
_STANDBY_REDIS_STATUSES = frozenset({"NOSESS", "STANDBY"})
_SUPPORTED_VOICE_TYPES = frozenset({"n", "s", "ln", "ls"})


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class DeviceHealthMonitorCycleDelivery:
    """Slack 표현을 포함하지 않는 장비 이상 delivery 하나다."""

    delivery_id: str
    payload: Mapping[str, Any] = field(repr=False)


@dataclass(frozen=True, slots=True)
class DeviceHealthMonitorCycleRun:
    """automation handler가 공통 계약으로 옮길 채널 중립 실행 결과다."""

    cursor: Mapping[str, Any] = field(repr=False)
    deliveries: tuple[DeviceHealthMonitorCycleDelivery, ...]
    metrics: Mapping[str, Any] = field(repr=False)


@dataclass(frozen=True, slots=True)
class DeviceHealthMonitorCycleDeps:
    """외부 조회와 mutation을 한 번씩만 호출하도록 고정한 실행 port다."""

    load_devices: Callable[[], list[dict[str, Any]]] = (
        lambda: _load_device_health_monitor_devices()
    )
    load_redis_snapshot: Callable[
        [Sequence[str]], Mapping[str, Mapping[str, Any]]
    ] = lambda names: _load_device_health_monitor_redis_snapshot(names)
    verify_device: Callable[
        [Mapping[str, Any], datetime], Mapping[str, Any]
    ] = lambda device, now: _verify_device_health_runtime(device, now=now)
    load_captureboard_incidents: Callable[
        [], Mapping[str, Mapping[str, Any]] | None
    ] = lambda: _load_device_health_sheet_captureboard_incidents()
    send_sms: Callable[
        [Mapping[str, Any], logging.Logger], Mapping[str, Any]
    ] = lambda payload, logger: _send_device_health_alert_sms(
        dict(payload),
        logger=logger,
    )
    claim_sms_delivery: Callable[..., bool] = claim_automatic_sms_delivery
    hold_sms_delivery_claim: Callable[..., bool] = (
        hold_automatic_sms_delivery_claim
    )
    clock: Callable[[], datetime] = _utc_now
    remember_sms_delivery: Callable[..., bool] = (
        remember_sms_delivery_sheet_record
    )
    append_sheet_alerts: Callable[
        [Sequence[Mapping[str, Any]], datetime, str], int | None
    ] = lambda items, detected_at, permalink: _append_device_health_sheet_alerts(
        [dict(item) for item in items],
        detected_at=detected_at,
        slack_permalink=permalink,
    )
    archive_event: Callable[
        [str, datetime, Mapping[str, Any]], bool
    ] = lambda request_id, now, payload: _archive_device_health_cycle_event(
        request_id=request_id,
        now=now,
        payload=payload,
    )


def build_device_health_monitor_seed_cursor(
    *,
    legacy_alert_delivery_enabled: bool,
    alert_fingerprints: Mapping[str, Any] | None,
    pending_alert_fingerprints: Mapping[str, Any] | None,
    pending_decision: str,
    seeded_at: datetime,
) -> dict[str, Any]:
    """검토된 Slack legacy 상태만 API durable cursor로 좁혀 이관한다."""

    if type(legacy_alert_delivery_enabled) is not bool:
        raise ValueError("legacy alert delivery setting must be a boolean")
    if seeded_at.tzinfo is None:
        raise ValueError("device health monitor seed time must be timezone-aware")
    if pending_decision not in _MONITOR_PENDING_DECISIONS:
        raise ValueError("device health monitor pending decision is invalid")

    migrated_alerts = _validate_migrated_fingerprint_state(
        alert_fingerprints,
        timestamp_key="firstAlertedAt",
    )
    migrated_pending = _validate_migrated_fingerprint_state(
        pending_alert_fingerprints,
        timestamp_key="firstSeenAt",
    )
    seeded_at_text = seeded_at.astimezone(timezone.utc).isoformat()
    if pending_decision == "assume_delivered":
        # legacy pending의 실제 발송 여부가 불명확하면 최근 발송으로 승격해
        # 첫 API poll이 Slack/SMS mutation을 중복 실행하지 않게 한다.
        for fingerprint, pending in migrated_pending.items():
            migrated_alerts.setdefault(
                fingerprint,
                {
                    "firstAlertedAt": seeded_at_text,
                    "lastAlertedAt": seeded_at_text,
                    "lastSeenAt": str(
                        pending.get("lastSeenAt") or seeded_at_text
                    ),
                    "count": max(1, int(pending.get("count") or 0)),
                },
            )
        migrated_pending = {}

    seed_payload = {
        "legacyAlertDeliveryEnabled": legacy_alert_delivery_enabled,
        "alertFingerprints": migrated_alerts,
        "pendingAlertFingerprints": migrated_pending,
        "pendingDecision": pending_decision,
    }
    seed_digest = hashlib.sha256(
        json.dumps(
            seed_payload,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]
    return _normalize_monitor_cursor(
        {
            "stateOwnership": {
                "owner": _MONITOR_STATE_OWNER,
                "version": _MONITOR_STATE_VERSION,
                "seededAt": seeded_at_text,
                "seedDigest": seed_digest,
                "pendingDecision": pending_decision,
                "overrideRevision": 0,
            },
            "alertDeliveryOverride": {
                "enabled": legacy_alert_delivery_enabled,
                "updatedAt": seeded_at_text,
                "updatedBy": "manual_cutover_seed",
            },
            "alertFingerprints": migrated_alerts,
            "pendingAlertFingerprints": migrated_pending,
        }
    )


def update_device_health_monitor_alert_delivery_override(
    cursor: Mapping[str, Any],
    *,
    enabled: bool,
    updated_at: datetime,
) -> dict[str, Any]:
    """중지된 API의 검증된 cursor에서 alert override만 원자 갱신한다."""

    if type(enabled) is not bool:
        raise ValueError("device health monitor alert override must be a boolean")
    if updated_at.tzinfo is None:
        raise ValueError("device health monitor override time must be timezone-aware")
    _validate_raw_monitor_cursor(cursor)
    state = _normalize_monitor_cursor(cursor)
    _validate_monitor_state_and_options(state, {})
    ownership = dict(state["stateOwnership"])
    ownership["overrideRevision"] = max(
        0,
        int(ownership.get("overrideRevision") or 0),
    ) + 1
    state["stateOwnership"] = ownership
    state["alertDeliveryOverride"] = {
        "enabled": enabled,
        "updatedAt": updated_at.astimezone(timezone.utc).isoformat(),
        "updatedBy": "manual_offline_override",
    }
    return state


def device_health_monitor_cursor_digest(cursor: Mapping[str, Any]) -> str:
    """운영자가 원문을 출력하지 않고 exact cursor를 확인하는 digest다."""

    _validate_raw_monitor_cursor(cursor)
    state = _normalize_monitor_cursor(cursor)
    _validate_monitor_state_and_options(state, {})
    # migration CAS는 normalize가 버리는 outbox/미래 필드까지 포함한 raw
    # cursor 전체 revision을 묶어야 source drift를 숨기지 않는다.
    return hashlib.sha256(
        json.dumps(
            dict(cursor),
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:24]


def run_device_health_monitor_cycle(
    *,
    request_id: str,
    now: datetime,
    cursor: Mapping[str, Any] | None = None,
    options: Mapping[str, Any] | None = None,
    deps: DeviceHealthMonitorCycleDeps | None = None,
    logger: logging.Logger | None = None,
) -> DeviceHealthMonitorCycleRun:
    """Redis 선별 뒤 필요한 장비만 SSH 검증하고 alert mutation을 한 번 실행한다."""

    actual_deps = deps or DeviceHealthMonitorCycleDeps()
    actual_logger = logger or logging.getLogger(__name__)
    local_now = _coerce_daily_device_round_now(now)
    # remote health는 Slack option이나 legacy 파일이 아니라 API durable cursor만
    # 정본으로 쓴다. 명시적 seed가 없으면 외부 조회·mutation 전에 닫힌다.
    _validate_raw_monitor_cursor(cursor)
    state = _normalize_monitor_cursor(cursor)
    alert_delivery_enabled = _validate_monitor_state_and_options(
        state,
        options,
    )
    _flush_device_health_sheet_repairs(
        state,
        deps=actual_deps,
        logger=actual_logger,
    )

    try:
        devices = actual_deps.load_devices()
    except Exception as exc:
        return _unavailable_cycle_run(
            request_id=request_id,
            now=local_now,
            state=state,
            reason="device_query_unavailable",
            error_type=type(exc).__name__,
            deps=actual_deps,
            logger=actual_logger,
        )

    device_names = [
        _text(device.get("deviceName"))
        for device in devices
        if _text(device.get("deviceName"))
    ]
    try:
        redis_snapshot = actual_deps.load_redis_snapshot(device_names)
    except Exception as exc:
        return _unavailable_cycle_run(
            request_id=request_id,
            now=local_now,
            state=state,
            reason="redis_unavailable",
            error_type=type(exc).__name__,
            deps=actual_deps,
            logger=actual_logger,
            checked_device_count=len(devices),
        )

    device_results: list[dict[str, Any]] = []
    verification_error_count = 0
    ssh_verified_count = 0
    ssh_records = dict(state.get("sshTunnelRecords") or {})
    abnormal_candidate_count = 0
    for device in devices:
        device_name = _text(device.get("deviceName"))
        redis_result, requires_ssh = _result_from_redis(
            device,
            redis_snapshot.get(device_name, {}),
            now=local_now,
        )
        if requires_ssh:
            abnormal_candidate_count += 1
            try:
                # strict API runtime은 sshOrder를 스스로 재전송하거나 force reopen하지 않는다.
                verified = dict(actual_deps.verify_device(device, local_now))
                ssh_verified_count += 1
                device_results.append(verified)
                ssh_records[device_name] = {
                    "lastVerifiedAt": local_now.isoformat(),
                    "ready": bool(verified.get("sshReady")),
                    "reasonCode": _text(verified.get("sshReason")),
                }
            except Exception as exc:
                verification_error_count += 1
                device_results.append(
                    {
                        **redis_result,
                        "overallLabel": "점검 불가",
                        "errorType": type(exc).__name__,
                    }
                )
                ssh_records[device_name] = {
                    "lastVerifiedAt": local_now.isoformat(),
                    "ready": False,
                    "reasonCode": "verification_failed",
                    "errorType": type(exc).__name__,
                }
        else:
            device_results.append(redis_result)

    status_counts = _status_counts(device_results)
    alert_items = _collect_alert_items(device_results)
    (
        alertable,
        next_alerts,
        next_pending,
    ) = _collect_alert_updates(
        alert_items,
        state,
        now=local_now,
        delivery_enabled=alert_delivery_enabled,
    )

    sheet_read_status = "not_needed"
    if any(
        _alert_fingerprint(item) in alertable
        and _suppressible_captureboard(item)
        for item in alert_items
    ):
        try:
            incidents = actual_deps.load_captureboard_incidents()
            sheet_read_status = "disabled" if incidents is None else "completed"
            alertable, next_alerts, next_pending = _suppress_open_sheet_incidents(
                alert_items,
                alertable,
                next_alerts,
                next_pending,
                previous_state=state,
                incidents=incidents or {},
                now=local_now,
            )
        except Exception as exc:
            # 운영 시트 장애는 실제 장비 이상을 숨기지 않는 기존 fail-open 정책을 유지한다.
            sheet_read_status = "failed"
            actual_logger.warning(
                "Device health sheet read failed error_type=%s",
                type(exc).__name__,
            )

    alert_items_by_fingerprint = {
        _alert_fingerprint(item): item for item in alert_items
    }
    delivered_items: list[dict[str, Any]] = []
    for fingerprint in sorted(alertable):
        raw_item = alert_items_by_fingerprint.get(fingerprint)
        if raw_item is None:
            continue
        delivered_items.append(
            _apply_automatic_sms_once(
                raw_item,
                request_id=request_id,
                now=local_now,
                deps=actual_deps,
                logger=actual_logger,
            )
        )

    pending_sheet_alerts = dict(state.get("pendingSheetAlerts") or {})
    for item in delivered_items:
        # Slack 성공 receipt의 permalink를 받은 뒤 API ack hook이 Sheets를 기록한다.
        pending_sheet_alerts[_delivery_id(item)] = {
            "detectedAt": local_now.isoformat(),
            "item": _sheet_alert_item(item),
        }
    next_cursor = {
        "lastRunAt": local_now.isoformat(),
        "cycleCompleted": False,
        "checkedDeviceCount": len(devices),
        "abnormalCandidateCount": abnormal_candidate_count,
        "sshVerifiedCandidateCount": ssh_verified_count,
        "monitorUnavailableReason": "",
        "monitorUnavailableErrorType": "",
        "statusCounts": status_counts,
        "alertFingerprints": next_alerts,
        "pendingAlertFingerprints": next_pending,
        "sshTunnelRecords": ssh_records,
        # 연락처를 durable cursor에 넣지 않고 식별·표시용 최소 캐시만 보존한다.
        "deviceCandidateCache": [_safe_device_cache_item(item) for item in devices],
        "deviceCandidateCachedAt": local_now.isoformat(),
        "pendingSheetAlerts": pending_sheet_alerts,
        "pendingSheetRepairs": dict(state.get("pendingSheetRepairs") or {}),
        "lastSheetWriteStatus": _text(state.get("lastSheetWriteStatus")),
        "lastSheetRepairDeliveryId": _text(
            state.get("lastSheetRepairDeliveryId")
        ),
        "stateOwnership": dict(state.get("stateOwnership") or {}),
        "alertDeliveryOverride": dict(
            state.get("alertDeliveryOverride") or {}
        ),
    }
    deliveries = tuple(
        DeviceHealthMonitorCycleDelivery(
            delivery_id=_delivery_id(item),
            payload=_delivery_payload(
                item,
                now=local_now,
                checked_device_count=len(devices),
                abnormal_candidate_count=abnormal_candidate_count,
            ),
        )
        for item in delivered_items
    )
    archive_status = _archive_event_best_effort(
        request_id=request_id,
        now=local_now,
        payload={
            "eventType": "device_health_monitor_cycle",
            "checkedDeviceCount": len(devices),
            "abnormalCandidateCount": abnormal_candidate_count,
            "sshVerifiedCandidateCount": ssh_verified_count,
            "deliveryCount": len(deliveries),
            "statusCounts": status_counts,
            "alertHashes": [
                hashlib.sha256(_alert_fingerprint(item).encode("utf-8")).hexdigest()[:24]
                for item in delivered_items
            ],
        },
        deps=actual_deps,
        logger=actual_logger,
    )
    return DeviceHealthMonitorCycleRun(
        cursor=next_cursor,
        deliveries=deliveries,
        metrics={
            "checkedDeviceCount": len(devices),
            "abnormalCandidateCount": abnormal_candidate_count,
            "sshVerifiedCandidateCount": ssh_verified_count,
            "verificationErrorCount": verification_error_count,
            "deliveryCount": len(deliveries),
            "sheetReadStatus": sheet_read_status,
            "sheetWriteStatus": "pending" if delivered_items else "not_needed",
            "sheetRowCount": 0,
            "archiveStatus": archive_status,
        },
    )


def acknowledge_device_health_monitor_deliveries(
    *,
    cursor: Mapping[str, Any],
    receipts: Sequence[Any],
    deps: DeviceHealthMonitorCycleDeps | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """Slack sent receipt 뒤 Sheets를 기록하고 실패는 SMS outbox 복구로 넘긴다."""

    actual_deps = deps or DeviceHealthMonitorCycleDeps()
    actual_logger = logger or logging.getLogger(__name__)
    next_cursor = _normalize_monitor_cursor(cursor)
    pending = dict(next_cursor.get("pendingSheetAlerts") or {})
    total_rows = 0
    last_write_at = ""
    for receipt in receipts:
        delivery_id = _text(getattr(receipt, "delivery_id", ""))
        pending_record = pending.get(delivery_id)
        if not isinstance(pending_record, Mapping):
            continue
        status = _text(getattr(receipt, "status", ""))
        if status == "sent":
            detected_at = _parse_datetime(pending_record.get("detectedAt"))
            item = pending_record.get("item")
            if detected_at is None or not isinstance(item, Mapping):
                raise ValueError("device health sheet outbox is invalid")
            permalink = _text(getattr(receipt, "permalink", ""))
            # Sheet append가 실제 반영 뒤 timeout 나도 다음 poll이 같은 T열
            # delivery hash를 읽어 duplicate row를 만들지 않게 한다.
            sheet_item = {**dict(item), "sheetDeliveryId": delivery_id}
            outbox_ready = False
            if _text(item.get("smsGroupId")):
                try:
                    # 최초 receipt outbox에 Slack permalink를 병합해 direct Sheet
                    # append가 실패해도 reconciliation cycle이 같은 행을 복구한다.
                    outbox_ready = actual_deps.remember_sms_delivery(
                        dict(item),
                        detected_at=detected_at,
                        sms_accepted_at=item.get("smsAcceptedAt") or detected_at,
                        permalink=permalink,
                    )
                except Exception as exc:
                    actual_logger.warning(
                        "Device health SMS outbox permalink update failed "
                        "error_type=%s",
                        type(exc).__name__,
                    )
            try:
                written = actual_deps.append_sheet_alerts(
                    (sheet_item,),
                    detected_at,
                    permalink,
                )
            except Exception as exc:
                # Slack delivery는 이미 성공했다. durable SMS outbox가 있는 경우
                # repair cycle에 넘기고 cursor는 닫아 monitor 전체를 막지 않는다.
                actual_logger.warning(
                    "Device health delivery acknowledgement failed error_type=%s",
                    type(exc).__name__,
                )
                next_cursor["lastSheetWriteStatus"] = "repair_pending"
                next_cursor["lastSheetRepairDeliveryId"] = delivery_id
                next_cursor["pendingSheetRepairs"][delivery_id] = {
                    "queuedAt": (
                        getattr(receipt, "delivered_at", None) or detected_at
                    ).isoformat(),
                    "detectedAt": detected_at.isoformat(),
                    "permalink": permalink,
                    # provider group이 없을 때도 다음 poll이 Sheets만 직접
                    # 재시도할 수 있도록 non-PII allowlist item을 보존한다.
                    "item": _sheet_alert_item(sheet_item),
                    "status": (
                        "outbox_pending"
                        if _text(item.get("smsGroupId"))
                        else "sheet_pending"
                    ),
                }
                written = 0
            else:
                if written == 0 and outbox_ready:
                    next_cursor["lastSheetWriteStatus"] = "repair_pending"
                    next_cursor["lastSheetRepairDeliveryId"] = delivery_id
                    next_cursor["pendingSheetRepairs"][delivery_id] = {
                        "queuedAt": (
                            getattr(receipt, "delivered_at", None)
                            or detected_at
                        ).isoformat(),
                        "detectedAt": detected_at.isoformat(),
                        "permalink": permalink,
                        "item": _sheet_alert_item(sheet_item),
                        "status": "outbox_pending",
                    }
                else:
                    next_cursor["lastSheetWriteStatus"] = (
                        "disabled" if written is None else "completed"
                    )
                    next_cursor["lastSheetRepairDeliveryId"] = ""
                    next_cursor["pendingSheetRepairs"].pop(delivery_id, None)
            total_rows += max(0, int(written or 0))
            last_write_at = (
                getattr(receipt, "delivered_at", None) or detected_at
            ).isoformat()
        # failed receipt도 같은 Slack delivery를 자동 재실행하지 않게 outbox에서 닫는다.
        pending.pop(delivery_id, None)
    next_cursor["pendingSheetAlerts"] = pending
    if last_write_at:
        next_cursor["lastSheetWriteAt"] = last_write_at
        next_cursor["lastSheetRowCount"] = total_rows
    return next_cursor


def _flush_device_health_sheet_repairs(
    state: dict[str, Any],
    *,
    deps: DeviceHealthMonitorCycleDeps,
    logger: logging.Logger,
) -> None:
    """Slack ack 뒤 Sheet 실패를 provider outbox 또는 direct append로 복구한다."""

    pending_repairs = state["pendingSheetRepairs"]
    for delivery_id, repair in tuple(pending_repairs.items()):
        item = repair.get("item")
        detected_at = repair.get("detectedAt")
        if not isinstance(item, dict) or not detected_at:
            continue
        group_id = _text(item.get("smsGroupId"))
        if group_id:
            try:
                # provider group이 있는 항목은 기존 durable outbox가 최종
                # delivery 상태 확인과 Sheet reconciliation을 계속 소유한다.
                remembered = deps.remember_sms_delivery(
                    dict(item),
                    detected_at=detected_at,
                    sms_accepted_at=item.get("smsAcceptedAt") or detected_at,
                    permalink=repair.get("permalink") or "",
                )
            except Exception as exc:
                logger.warning(
                    "Device health Sheet repair queue failed error_type=%s",
                    type(exc).__name__,
                )
                continue
            if remembered:
                pending_repairs.pop(delivery_id, None)
                state["lastSheetWriteStatus"] = "repair_queued"
                state["lastSheetRepairDeliveryId"] = ""
            continue

        if _text(repair.get("status")) != "sheet_pending":
            continue
        parsed_detected_at = _parse_datetime(detected_at)
        if parsed_detected_at is None:
            continue
        try:
            # provider group이 없는 수동/실패 SMS 결과는 outbox가 받을 수
            # 없으므로 저장된 allowlist context로 Sheet append만 재시도한다.
            # map key를 정본 delivery ID로 다시 주입해 오래된 cursor도 이후
            # timeout부터는 같은 T열 hash로 멱등 복구할 수 있게 한다.
            repair_item = {**dict(item), "sheetDeliveryId": delivery_id}
            written = deps.append_sheet_alerts(
                (repair_item,),
                parsed_detected_at,
                _text(repair.get("permalink")),
            )
        except Exception as exc:
            logger.warning(
                "Device health direct Sheet repair failed error_type=%s",
                type(exc).__name__,
            )
            continue
        pending_repairs.pop(delivery_id, None)
        state["lastSheetWriteStatus"] = (
            "disabled" if written is None else "repair_completed"
        )
        state["lastSheetRepairDeliveryId"] = ""


def _validate_monitor_state_and_options(
    state: Mapping[str, Any],
    options: Mapping[str, Any] | None,
) -> bool:
    payload = dict(options or {})
    if set(payload) - _MONITOR_OPTION_KEYS:
        raise ValueError("device health monitor contains unsupported options")
    ownership = _normalize_monitor_state_ownership(
        state.get("stateOwnership")
    )
    override = _normalize_monitor_alert_delivery_override(
        state.get("alertDeliveryOverride")
    )
    if ownership is None or override is None:
        raise ValueError("device health monitor API state seed is required")
    return bool(override["enabled"])


def _validate_raw_monitor_cursor(value: Mapping[str, Any] | None) -> None:
    """필수 fingerprint map 손상을 normalize가 빈 state로 숨기지 못하게 한다."""

    if not isinstance(value, Mapping):
        raise ValueError("device health monitor API state seed is required")
    for key, timestamp_key in (
        ("alertFingerprints", "firstAlertedAt"),
        ("pendingAlertFingerprints", "firstSeenAt"),
    ):
        raw_state = value.get(key)
        if key not in value or not isinstance(raw_state, Mapping):
            raise ValueError("device health monitor fingerprint state is invalid")
        _validate_migrated_fingerprint_state(
            raw_state,
            timestamp_key=timestamp_key,
        )


def _load_device_health_monitor_devices() -> list[dict[str, Any]]:
    if not core_settings.DB_QUERY_ENABLED:
        raise RuntimeError("DB query is disabled")
    connection = _create_db_connection(core_settings.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            # 자동문자는 전용 번호만 내부에서 사용하고 API delivery에는 싣지 않는다.
            cursor.execute(
                "SELECT d.seq AS deviceSeq, d.deviceName, d.hospitalSeq, "
                "d.hospitalRoomSeq, h.hospitalName, h.telephone AS hospitalTelephone, "
                "h.deviceAlertPhone AS hospitalDeviceAlertPhone, hr.roomName "
                "FROM devices d "
                "INNER JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "WHERE d.hospitalSeq IS NOT NULL "
                "AND COALESCE(d.deviceName, '') <> '' "
                "AND COALESCE(h.hospitalName, '') NOT REGEXP '^[0-9]+_' "
                "AND COALESCE(d.activeFlag, 1) = 1 "
                "AND COALESCE(d.installFlag, 1) = 1 "
                "ORDER BY d.hospitalSeq ASC, COALESCE(hr.roomName, '') ASC, "
                "d.deviceName ASC, d.seq DESC"
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    result: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in rows:
        device_name = _text(row.get("deviceName"))
        hospital_name = _text(row.get("hospitalName"))
        if not device_name or device_name in seen or re.match(r"^\d+_", hospital_name):
            continue
        seen.add(device_name)
        result.append(
            {
                "deviceSeq": _positive_int(row.get("deviceSeq")),
                "deviceName": device_name,
                "hospitalSeq": _positive_int(row.get("hospitalSeq")),
                "hospitalRoomSeq": _positive_int(row.get("hospitalRoomSeq")),
                "hospitalName": hospital_name or "미확인",
                "hospitalTelephone": _text(row.get("hospitalTelephone")),
                "hospitalDeviceAlertPhone": _text(
                    row.get("hospitalDeviceAlertPhone")
                ),
                "roomName": _text(row.get("roomName")) or "미확인",
            }
        )
    return result


def _load_device_health_monitor_redis_snapshot(
    device_names: Sequence[str],
) -> Mapping[str, Mapping[str, Any]]:
    client = DeviceStateRedisClient.from_settings()
    client.ping()
    return client.load_device_and_agent_states(list(device_names))


def _result_from_redis(
    device: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    now: datetime,
) -> tuple[dict[str, Any], bool]:
    device_state = snapshot.get("deviceState")
    agent_state = snapshot.get("agentState")
    device_state = device_state if isinstance(device_state, Mapping) else None
    agent_state = agent_state if isinstance(agent_state, Mapping) else None
    if _redis_unavailable_reasons(device_state, agent_state, now=now):
        return _base_device_result(device, overall_label="점검 불가"), False

    requires_ssh = _redis_requires_ssh_verification(device_state)
    if requires_ssh:
        return _base_device_result(device, overall_label="확인 필요"), True
    return _base_device_result(device, overall_label="정상"), False


def _redis_unavailable_reasons(
    device_state: Mapping[str, Any] | None,
    agent_state: Mapping[str, Any] | None,
    *,
    now: datetime,
) -> list[str]:
    reasons: list[str] = []
    if device_state is None or _is_redis_state_stale(device_state, now=now):
        reasons.append("device_state_unavailable")
    elif device_state.get("isConnected") is not True:
        reasons.append("device_disconnected")
    if agent_state is None or agent_state.get("isConnected") is not True:
        reasons.append("agent_disconnected")
    status = _text((device_state or {}).get("status")).upper()
    if any(marker in status for marker in ("EXIT", "DISCONNECT", "OFFLINE")):
        reasons.append("device_offline_status")
    return reasons


def _is_redis_state_stale(state: Mapping[str, Any], *, now: datetime) -> bool:
    updated_at = _parse_datetime(state.get("updatedAt"))
    if updated_at is None:
        return True
    status = _text(state.get("status")).upper()
    threshold = (
        max(30, int(company_settings.DEVICE_HEALTH_MONITOR_REDIS_STANDBY_STALE_SEC))
        if status in _STANDBY_REDIS_STATUSES
        else max(30, int(company_settings.DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC))
    )
    return now - updated_at > timedelta(seconds=threshold)


def _redis_requires_ssh_verification(
    device_state: Mapping[str, Any] | None,
) -> bool:
    payload = device_state or {}
    usb_items = _redis_usb_items(payload)
    usb_text = " ".join(
        " ".join(_text(item.get(key)) for key in ("name", "alias", "type", "deviceId"))
        for item in usb_items
    ).lower()
    capture_status = _text(payload.get("captureBoardStatus")).lower()
    capture_type = _text(payload.get("captureBoardType"))
    capture_missing = bool(
        capture_status in {"false", "none", "missing"}
        or "disconnect" in capture_status
        or "offline" in capture_status
        or (
            usb_items
            and capture_type
            and not any(
                marker in usb_text
                for marker in ("captureboard", "capture", "ls_hdmi", "easycap")
            )
        )
    )
    led_missing = bool(
        usb_items
        and not any(marker in usb_text for marker in ("led", "mmtled"))
    )
    disk_percent = _redis_disk_percent(payload)
    return capture_missing or led_missing or (
        disk_percent is not None and disk_percent >= 90.0
    )


def _redis_usb_items(state: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    acme = state.get("acme") if isinstance(state.get("acme"), Mapping) else {}
    values = acme.get("usbList") if isinstance(acme, Mapping) else []
    return [item for item in values if isinstance(item, Mapping)] if isinstance(values, list) else []


def _redis_disk_percent(state: Mapping[str, Any]) -> float | None:
    candidates = [state.get("diskUsage")]
    acme = state.get("acme") if isinstance(state.get("acme"), Mapping) else {}
    system = acme.get("systemInfo") if isinstance(acme, Mapping) and isinstance(acme.get("systemInfo"), Mapping) else {}
    candidates.append(system.get("hddUsage") if isinstance(system, Mapping) else None)
    for value in candidates:
        try:
            return float(str(value).strip().replace("%", ""))
        except (TypeError, ValueError):
            continue
    return None


def _verify_device_health_runtime(
    device: Mapping[str, Any],
    *,
    now: datetime,
) -> Mapping[str, Any]:
    device_name = _text(device.get("deviceName"))
    if not device_name:
        raise ValueError("device name is missing")
    evidence, device_info, checks = _collect_runtime_checks(
        device_name,
        "all",
        resend_ssh_open=False,
        allow_force_reopen=False,
    )
    ssh = evidence.get("ssh") if isinstance(evidence.get("ssh"), dict) else {}
    if not ssh.get("ready"):
        return {
            **_base_device_result(device, overall_label="점검 불가"),
            "sshReady": False,
            "sshReason": _text(ssh.get("reason")) or "agent_ssh_not_ready",
        }

    overview = {
        "audio": _summarize_audio_path_probe(checks),
        "pm2": _summarize_pm2_probe(
            _parse_pm2_processes(
                _text((checks.get("pm2_jlist") or {}).get("output"))
            )
        ),
        "storage": _build_trashcan_storage_summary_from_checks(
            checks,
            cleanup_threshold_percent=(
                company_settings.DAILY_DEVICE_ROUND_TRASHCAN_USAGE_THRESHOLD_PERCENT
            ),
            cleanup_age_days=company_settings.DAILY_DEVICE_ROUND_TRASHCAN_DELETE_AGE_DAYS,
        ),
    }
    usb_devices = _parse_usb_devices(
        _text((checks.get("lsusb") or {}).get("output"))
    )
    overview["captureboard"] = _summarize_captureboard_probe(
        device_info=dict(device_info),
        usb_devices=usb_devices,
        video_devices=_parse_device_path_list(
            _text((checks.get("video_devices") or {}).get("output")),
            missing_token="no_video_device",
        ),
        v4l2_devices=_text((checks.get("v4l2_devices") or {}).get("output")),
    )
    overview["led"] = _summarize_led_probe(
        usb_devices=usb_devices,
        serial_devices=_parse_device_path_list(
            _text((checks.get("serial_devices") or {}).get("output")),
            missing_token="no_serial_device",
        ),
    )
    voice_config = _parse_voice_config(
        _text((checks.get("voice_config") or {}).get("output"))
    )
    voice_type = _text(voice_config.get("voiceType")).lower()
    status_payload = {
        "route": "device_health_monitor",
        "source": "mda_graphql+ssh_linux_commands",
        "device": {
            "deviceName": _text(device_info.get("deviceName")) or device_name,
            "version": _text(device_info.get("version")),
            "voiceType": voice_type if voice_type in _SUPPORTED_VOICE_TYPES else "",
        },
        "ssh": {"ready": True, "reason": "ready"},
        "overview": overview,
    }
    priority = _build_daily_device_round_priority(status_payload)
    result = {
        **_base_device_result(
            device,
            overall_label=_daily_device_round_status_label(status_payload),
        ),
        "deviceVersion": _text(status_payload["device"].get("version")),
        "voiceType": _text(status_payload["device"].get("voiceType")),
        "priorityReason": _text(priority.get("reason")),
        "componentLabels": {
            key: _text((overview.get(key) or {}).get("label")) or "확인 필요"
            for key in _COMPONENT_KEYS
        },
        "storageDetails": _build_daily_device_round_storage_details(status_payload),
        # issue builder만 사용하고 raw SSH/check output은 cursor와 delivery에 넣지 않는다.
        "statusPayload": status_payload,
        "sshReady": True,
        "sshReason": "ready",
    }
    result["issue"] = _build_daily_device_round_issue_summary(result)
    result.pop("statusPayload", None)
    return result


def _base_device_result(
    device: Mapping[str, Any],
    *,
    overall_label: str,
) -> dict[str, Any]:
    component_label = "정상" if overall_label == "정상" else overall_label
    return {
        "deviceSeq": _positive_int(device.get("deviceSeq")),
        "deviceName": _text(device.get("deviceName")),
        "hospitalSeq": _positive_int(device.get("hospitalSeq")),
        "hospitalName": _text(device.get("hospitalName")) or "미확인",
        "hospitalTelephone": _text(device.get("hospitalTelephone")),
        "hospitalDeviceAlertPhone": _text(
            device.get("hospitalDeviceAlertPhone")
        ),
        "roomName": _text(device.get("roomName")) or "미확인",
        "overallLabel": overall_label,
        "componentLabels": {
            key: component_label for key in _COMPONENT_KEYS
        },
        "issue": "",
        "sshReady": False,
        "sshReason": "not_required",
    }


def _collect_alert_items(
    device_results: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for result in device_results:
        if _text(result.get("overallLabel")) != "이상":
            continue
        component_labels = (
            result.get("componentLabels")
            if isinstance(result.get("componentLabels"), Mapping)
            else {}
        )
        components = [
            _COMPONENT_LABELS[key]
            for key in _COMPONENT_KEYS
            if _text(component_labels.get(key)) == "이상"
        ]
        issue = _text(result.get("issue")) or _text(result.get("priorityReason"))
        if not issue:
            issue = "상세 확인 필요"
        category = _alert_category(components, issue)
        items.append(
            {
                "hospitalSeq": str(_positive_int(result.get("hospitalSeq")) or ""),
                "hospitalName": _text(result.get("hospitalName")) or "병원 미확인",
                "hospital": _hospital_label(result),
                "telephone": _text(result.get("hospitalTelephone")),
                "deviceAlertPhone": _text(result.get("hospitalDeviceAlertPhone")),
                "room": _text(result.get("roomName")) or "병실 미확인",
                "device": _text(result.get("deviceName")) or "장비명 미확인",
                "deviceVersion": _text(result.get("deviceVersion")),
                "voiceType": _text(result.get("voiceType")),
                "issue": issue,
                "problemComponents": components,
                "alertCategory": category,
            }
        )
    return items


def _collect_alert_updates(
    items: Sequence[Mapping[str, Any]],
    state: Mapping[str, Any],
    *,
    now: datetime,
    delivery_enabled: bool,
) -> tuple[set[str], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    previous_alerts = _normalize_fingerprint_state(state.get("alertFingerprints"))
    previous_pending = _normalize_fingerprint_state(
        state.get("pendingAlertFingerprints"),
        timestamp_key="firstSeenAt",
    )
    current = {_alert_fingerprint(item): item for item in items}
    reminder = timedelta(
        hours=max(1, int(company_settings.DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS))
    )
    now_text = now.isoformat()
    next_alerts: dict[str, dict[str, Any]] = {}
    next_pending: dict[str, dict[str, Any]] = {}
    alertable: set[str] = set()

    for fingerprint, previous in previous_alerts.items():
        last_alerted = _parse_datetime(previous.get("lastAlertedAt"))
        if fingerprint not in current and last_alerted and now - last_alerted < reminder:
            next_alerts[fingerprint] = previous

    for fingerprint, item in current.items():
        previous = previous_alerts.get(fingerprint, {})
        last_alerted = _parse_datetime(previous.get("lastAlertedAt"))
        pending = previous_pending.get(fingerprint, {})
        count = max(0, int(pending.get("count") or 0)) + 1
        confirmed = count >= _required_confirmation_polls(item)
        reminder_due = bool(last_alerted and now - last_alerted >= reminder)
        should_deliver = delivery_enabled and (reminder_due or (not last_alerted and confirmed))
        if should_deliver:
            alertable.add(fingerprint)
            next_alerts[fingerprint] = {
                "firstAlertedAt": str(previous.get("firstAlertedAt") or now_text),
                "lastAlertedAt": now_text,
                "lastSeenAt": now_text,
                "count": max(0, int(previous.get("count") or 0)) + 1,
            }
        elif last_alerted:
            next_alerts[fingerprint] = {
                "firstAlertedAt": str(previous.get("firstAlertedAt") or now_text),
                "lastAlertedAt": str(previous.get("lastAlertedAt") or ""),
                "lastSeenAt": now_text,
                "count": max(0, int(previous.get("count") or 0)) + 1,
            }
        else:
            # alerts off 상태에서는 완료로 승격하지 않아 다시 켜면 즉시 전달한다.
            next_pending[fingerprint] = {
                "firstSeenAt": str(pending.get("firstSeenAt") or now_text),
                "lastSeenAt": now_text,
                "count": count,
            }
    return alertable, next_alerts, next_pending


def _suppress_open_sheet_incidents(
    items: Sequence[Mapping[str, Any]],
    alertable: set[str],
    next_alerts: dict[str, dict[str, Any]],
    next_pending: dict[str, dict[str, Any]],
    *,
    previous_state: Mapping[str, Any],
    incidents: Mapping[str, Mapping[str, Any]],
    now: datetime,
) -> tuple[set[str], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    item_by_fingerprint = {_alert_fingerprint(item): item for item in items}
    previous_alerts = _normalize_fingerprint_state(previous_state.get("alertFingerprints"))
    previous_pending = _normalize_fingerprint_state(
        previous_state.get("pendingAlertFingerprints"),
        timestamp_key="firstSeenAt",
    )
    filtered = set(alertable)
    for fingerprint in tuple(alertable):
        item = item_by_fingerprint.get(fingerprint, {})
        components = set(item.get("problemComponents") or [])
        if components != {"캡처보드"}:
            continue
        incident = incidents.get(_text(item.get("device")))
        status = _text((incident or {}).get("status")).replace(" ", "")
        if status not in _CAPTUREBOARD_OPEN_STATUSES:
            continue
        filtered.discard(fingerprint)
        previous = previous_alerts.get(fingerprint, {})
        if _parse_datetime(previous.get("lastAlertedAt")):
            next_alerts[fingerprint] = {
                **previous,
                "lastSeenAt": now.isoformat(),
                "count": max(0, int(previous.get("count") or 0)) + 1,
            }
            next_pending.pop(fingerprint, None)
        else:
            pending = previous_pending.get(fingerprint, {})
            next_alerts.pop(fingerprint, None)
            next_pending[fingerprint] = {
                "firstSeenAt": str(pending.get("firstSeenAt") or now.isoformat()),
                "lastSeenAt": now.isoformat(),
                "count": max(1, int(pending.get("count") or 0) + 1),
            }
    return filtered, next_alerts, next_pending


def _apply_automatic_sms_once(
    item: Mapping[str, Any],
    *,
    request_id: str,
    now: datetime,
    deps: DeviceHealthMonitorCycleDeps,
    logger: logging.Logger,
) -> dict[str, Any]:
    result = {
        **dict(item),
        # 같은 장애의 reminder도 coordinator에서 새 delivery로 식별되게 한다.
        "alertOccurrenceAt": now.isoformat(),
    }
    phone_number = _normalize_phone_number(item.get("deviceAlertPhone"))
    target = DeviceHealthAlertActionTarget(
        hospital_seq=_positive_int(item.get("hospitalSeq")) or 0,
        hospital_name=_text(item.get("hospitalName")),
        room_name=_text(item.get("room")),
        device_name=_text(item.get("device")),
        issue=_text(item.get("issue")),
        alert_category=_text(item.get("alertCategory")),
        problem_components=tuple(str(value) for value in item.get("problemComponents") or ()),
    )
    guide = _build_device_health_alert_sms_guide(target)
    if not _is_mobile_phone_number(phone_number):
        return {
            **result,
            "smsStatusText": "수동 발송 필요",
            "smsContactActionEnabled": True,
            "smsDeliveryStatus": _SMS_DELIVERY_NOT_SENT,
        }
    sms_message = _text(guide.get("message"))
    if not guide.get("supported") or not sms_message:
        return {
            **result,
            "smsStatusText": "지원 템플릿 없음 - 수동 발송 필요",
            "smsContactActionEnabled": True,
            "smsDeliveryStatus": _SMS_DELIVERY_NOT_SENT,
        }

    # 긴 장비 순회 시작 시각이 아니라 실제 provider 호출 직전 API 시각으로
    # 공통 claim을 잡아 health/notification 사이 60초 창을 왜곡하지 않는다.
    claim_now = deps.clock()
    try:
        claimed = deps.claim_sms_delivery(
            target.device_name,
            target.alert_category,
            claimed_at=claim_now,
        )
    except Exception as exc:
        # claim 저장을 확인하지 못하면 provider를 호출하지 않는 쪽으로 닫아
        # crash/restart 경계에서도 중복 발송 가능성을 만들지 않는다.
        logger.warning(
            "Device health automatic SMS claim failed error_type=%s",
            type(exc).__name__,
        )
        return {
            **result,
            "smsStatusText": "문자 발송 여부 확인 필요",
            "smsContactActionEnabled": False,
            "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
        }
    if not claimed:
        return {
            **result,
            "smsStatusText": (
                "동일 장애 문자 중복 발송 생략 - 기존 알림에서 발송 여부 확인 필요"
            ),
            "smsContactActionEnabled": False,
            "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
        }

    # 전화번호와 본문은 provider 호출 payload 안에서만 사용하고 반환 item에서는 제거한다.
    payload = {
        "actionId": "device_health_alert_contact_hospital",
        "requestType": "sms",
        "createdAt": now.isoformat(),
        "actorUserId": "automation",
        "hospital": {
            "seq": target.hospital_seq,
            "name": target.hospital_name,
            "phoneNumber": phone_number,
        },
        "device": {
            "name": target.device_name,
            "room": target.room_name,
            "issue": target.issue,
        },
        "sms": {
            "to": phone_number,
            "templateId": _text(guide.get("templateId")),
            "message": sms_message,
            "testMode": False,
        },
        "origin": {
            "channel": "automation",
            "conversationId": "device_health_monitor",
            "requestId": request_id,
        },
    }
    try:
        sent = dict(deps.send_sms(payload, logger))
    except Exception as exc:
        logger.warning(
            "Device health automatic SMS failed error_type=%s",
            type(exc).__name__,
        )
        sent = {
            "status": "error",
            "ok": False,
            "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
        }
    accepted = bool(sent.get("ok")) and _text(sent.get("status")) == "sent"
    delivery_status = _text(sent.get("smsDeliveryStatus")) or (
        _SMS_DELIVERY_CONFIRM_REQUIRED if accepted else _SMS_DELIVERY_REQUEST_FAILED
    )
    status_text = (
        "문자 발송 접수"
        if delivery_status in {_SMS_DELIVERY_ACCEPTED, _SMS_DELIVERY_DELIVERED}
        else (
            "문자 발송 여부 확인 필요"
            if delivery_status == _SMS_DELIVERY_CONFIRM_REQUIRED
            else "문자 자동발송 실패 - 수동 발송 가능"
        )
    )
    sms_result = {
        **result,
        "smsStatusText": status_text,
        # 결과 불명은 공급자가 접수했을 수 있어 수동 재발송 버튼도 잠근다.
        "smsContactActionEnabled": (
            not accepted and delivery_status != _SMS_DELIVERY_CONFIRM_REQUIRED
        ),
        "smsProvider": _text(sent.get("provider")),
        "smsGroupId": _text(sent.get("groupId")),
        "smsMessageId": _text(sent.get("messageId")),
        "smsDeliveryStatus": delivery_status,
        "smsAcceptedAt": now.isoformat() if accepted else "",
    }
    group_id = _text(sms_result.get("smsGroupId"))
    remembered = False
    if delivery_status in {
        _SMS_DELIVERY_ACCEPTED,
        _SMS_DELIVERY_DELIVERED,
        _SMS_DELIVERY_CONFIRM_REQUIRED,
    }:
        if group_id:
            try:
                # provider mutation 직후 API-local outbox에 먼저 fsync해 Slack/Sheets
                # 장애 뒤에도 group 최종 상태를 별도 cycle이 이어서 추적하게 한다.
                remembered = deps.remember_sms_delivery(
                    _sheet_alert_item(sms_result),
                    detected_at=now,
                    sms_accepted_at=sms_result.get("smsAcceptedAt") or now,
                )
            except Exception as exc:
                logger.warning(
                    "Device health automatic SMS receipt persist failed "
                    "error_type=%s",
                    type(exc).__name__,
                )
        if not remembered and delivery_status != _SMS_DELIVERY_DELIVERED:
            # 공급자 성공 뒤 저장 실패는 재발송하지 않고 결과 불명으로만 노출한다.
            sms_result.update(
                {
                    "smsStatusText": "문자 발송 여부 확인 필요",
                    "smsContactActionEnabled": False,
                    "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                }
            )
    claim_state = (
        "settled"
        if delivery_status in {
            _SMS_DELIVERY_REQUEST_FAILED,
            _SMS_DELIVERY_FAILED,
        }
        else (
            "accepted"
            if remembered and group_id
            else "uncertain"
        )
    )
    try:
        # accepted는 outbox 최종 reconcile까지, uncertain은 운영 확인까지
        # sticky하게 유지해 timeout·저장 실패 뒤 자동 재발송을 막는다.
        deps.hold_sms_delivery_claim(
            target.device_name,
            target.alert_category,
            held_at=deps.clock(),
            state=claim_state,
            group_id=group_id if claim_state == "accepted" else None,
        )
    except Exception as exc:
        # 최초 pending claim 자체가 이미 sticky라 hold 갱신 실패도 재발송을
        # 열지는 않는다. 응답에는 비민감한 확인 필요 상태만 남긴다.
        logger.warning(
            "Device health automatic SMS claim hold failed error_type=%s",
            type(exc).__name__,
        )
        sms_result.update(
            {
                "smsStatusText": "문자 발송 여부 확인 필요",
                "smsContactActionEnabled": False,
                "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
            }
        )
    return sms_result


def _delivery_payload(
    item: Mapping[str, Any],
    *,
    now: datetime,
    checked_device_count: int,
    abnormal_candidate_count: int,
) -> dict[str, Any]:
    # conversation delivery에는 연락처, SMS 본문, provider payload를 절대 포함하지 않는다.
    return {
        "detectedAt": now.isoformat(),
        "checkedDeviceCount": checked_device_count,
        "abnormalCandidateCount": abnormal_candidate_count,
        "alert": {
            "hospitalSeq": _text(item.get("hospitalSeq")),
            "hospitalName": _text(item.get("hospitalName")),
            "hospital": _text(item.get("hospital")),
            "room": _text(item.get("room")),
            "device": _text(item.get("device")),
            "deviceVersion": _text(item.get("deviceVersion")),
            "voiceType": _text(item.get("voiceType")),
            "issue": _text(item.get("issue")),
            "problemComponents": list(item.get("problemComponents") or []),
            "alertCategory": _text(item.get("alertCategory")),
            "smsStatusText": _text(item.get("smsStatusText")),
            "smsContactActionEnabled": bool(item.get("smsContactActionEnabled", True)),
            "smsDeliveryStatus": _text(item.get("smsDeliveryStatus")),
            "smsAcceptedAt": _text(item.get("smsAcceptedAt")),
        },
    }


def _unavailable_cycle_run(
    *,
    request_id: str,
    now: datetime,
    state: Mapping[str, Any],
    reason: str,
    error_type: str,
    deps: DeviceHealthMonitorCycleDeps,
    logger: logging.Logger,
    checked_device_count: int = 0,
) -> DeviceHealthMonitorCycleRun:
    cursor = {
        **state,
        "lastRunAt": now.isoformat(),
        "cycleCompleted": False,
        "checkedDeviceCount": checked_device_count,
        "monitorUnavailableReason": reason,
        "monitorUnavailableErrorType": error_type,
    }
    archive_status = _archive_event_best_effort(
        request_id=request_id,
        now=now,
        payload={
            "eventType": "device_health_monitor_unavailable",
            "reasonCode": reason,
            "errorType": error_type,
            "checkedDeviceCount": checked_device_count,
        },
        deps=deps,
        logger=logger,
    )
    return DeviceHealthMonitorCycleRun(
        cursor=cursor,
        deliveries=(),
        metrics={
            "checkedDeviceCount": checked_device_count,
            "deliveryCount": 0,
            "monitorUnavailableReason": reason,
            "monitorUnavailableErrorType": error_type,
            "archiveStatus": archive_status,
        },
    )


def _archive_event_best_effort(
    *,
    request_id: str,
    now: datetime,
    payload: Mapping[str, Any],
    deps: DeviceHealthMonitorCycleDeps,
    logger: logging.Logger,
) -> str:
    try:
        return "completed" if deps.archive_event(request_id, now, payload) else "disabled"
    except Exception as exc:
        # 감사 보관 실패는 장비 alert 전달을 되돌리지 않고 자동 재시도하지 않는다.
        logger.warning(
            "Device health event archive failed error_type=%s",
            type(exc).__name__,
        )
        return "failed"


def _archive_device_health_cycle_event(
    *,
    request_id: str,
    now: datetime,
    payload: Mapping[str, Any],
) -> bool:
    bucket = _text(company_settings.DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET)
    if not bucket:
        return False
    prefix = _text(
        company_settings.DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX
    ).strip("/")
    request_hash = hashlib.sha256(request_id.encode("utf-8")).hexdigest()
    key = f"{now:%Y/%m/%d}/{request_hash}.json"
    if prefix:
        key = f"{prefix}/{key}"
    boto3, boto_config = _load_boto3_components()
    # put_object 자체는 한 번만 호출하고 SDK 재시도도 0회로 고정한다.
    timeout_sec = max(1, int(core_settings.S3_QUERY_TIMEOUT_SEC))
    client = boto3.client(
        "s3",
        region_name=core_settings.AWS_REGION,
        config=boto_config(
            region_name=core_settings.AWS_REGION,
            connect_timeout=timeout_sec,
            read_timeout=timeout_sec,
            retries={"total_max_attempts": 1, "mode": "standard"},
        ),
    )
    body = json.dumps(
        {"recordedAt": now.isoformat(), **dict(payload)},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        ServerSideEncryption="AES256",
    )
    return True


def _normalize_monitor_cursor(value: Mapping[str, Any] | None) -> dict[str, Any]:
    state = dict(value or {})
    raw_sheet_repairs = state.get("pendingSheetRepairs")
    raw_sheet_repair_items = [
        (key, item)
        for key, item in (
            raw_sheet_repairs.items()
            if isinstance(raw_sheet_repairs, Mapping)
            else []
        )
        if isinstance(item, Mapping)
    ]
    # group repair는 durable SMS outbox가 정본이므로 최근 표식만 두되,
    # direct Sheet repair는 cursor가 유일한 복구 context라 성공 전까지 보존한다.
    selected_sheet_repair_items = [
        (key, item)
        for key, item in raw_sheet_repair_items
        if _text(item.get("status")) == "sheet_pending"
    ] + [
        (key, item)
        for key, item in raw_sheet_repair_items
        if _text(item.get("status")) != "sheet_pending"
    ][-100:]
    pending_sheet_repairs = {
        str(key): {
            "queuedAt": _text(item.get("queuedAt")),
            "detectedAt": _text(item.get("detectedAt")),
            "permalink": _text(item.get("permalink")),
            "item": _sheet_alert_item(
                item.get("item")
                if isinstance(item.get("item"), Mapping)
                else {}
            ),
            "status": _text(item.get("status"))[:64],
        }
        for key, item in selected_sheet_repair_items
    }
    return {
        "lastRunAt": _text(state.get("lastRunAt")),
        "cycleCompleted": False,
        "checkedDeviceCount": max(0, int(state.get("checkedDeviceCount") or 0)),
        "abnormalCandidateCount": max(0, int(state.get("abnormalCandidateCount") or 0)),
        "sshVerifiedCandidateCount": max(0, int(state.get("sshVerifiedCandidateCount") or 0)),
        "monitorUnavailableReason": _text(state.get("monitorUnavailableReason")),
        "monitorUnavailableErrorType": _text(state.get("monitorUnavailableErrorType")),
        "statusCounts": dict(state.get("statusCounts") or {}),
        "alertFingerprints": _normalize_fingerprint_state(state.get("alertFingerprints")),
        "pendingAlertFingerprints": _normalize_fingerprint_state(
            state.get("pendingAlertFingerprints"),
            timestamp_key="firstSeenAt",
        ),
        "sshTunnelRecords": _normalize_ssh_records(state.get("sshTunnelRecords")),
        "deviceCandidateCache": [
            _safe_device_cache_item(item)
            for item in state.get("deviceCandidateCache") or []
            if isinstance(item, Mapping)
        ],
        "deviceCandidateCachedAt": _text(state.get("deviceCandidateCachedAt")),
        "pendingSheetAlerts": {
            str(key): dict(item)
            for key, item in (state.get("pendingSheetAlerts") or {}).items()
            if isinstance(item, Mapping)
        }
        if isinstance(state.get("pendingSheetAlerts"), Mapping)
        else {},
        "pendingSheetRepairs": pending_sheet_repairs,
        "lastSheetWriteAt": _text(state.get("lastSheetWriteAt")),
        "lastSheetRowCount": max(0, int(state.get("lastSheetRowCount") or 0)),
        "lastSheetWriteStatus": _text(state.get("lastSheetWriteStatus")),
        "lastSheetRepairDeliveryId": _text(
            state.get("lastSheetRepairDeliveryId")
        ),
        "stateOwnership": (
            _normalize_monitor_state_ownership(state.get("stateOwnership"))
            or {}
        ),
        "alertDeliveryOverride": (
            _normalize_monitor_alert_delivery_override(
                state.get("alertDeliveryOverride")
            )
            or {}
        ),
    }


def _normalize_monitor_state_ownership(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    owner = _text(value.get("owner"))
    try:
        version = int(value.get("version") or 0)
        override_revision = max(
            0,
            int(value.get("overrideRevision") or 0),
        )
    except (TypeError, ValueError):
        return None
    seed_digest = _text(value.get("seedDigest"))
    pending_decision = _text(value.get("pendingDecision"))
    seeded_at = _parse_datetime(value.get("seededAt"))
    if (
        owner != _MONITOR_STATE_OWNER
        or version != _MONITOR_STATE_VERSION
        or not re.fullmatch(r"[0-9a-f]{24}", seed_digest)
        or pending_decision not in _MONITOR_PENDING_DECISIONS
        or seeded_at is None
    ):
        return None
    return {
        "owner": owner,
        "version": version,
        "seededAt": _text(value.get("seededAt")),
        "seedDigest": seed_digest,
        "pendingDecision": pending_decision,
        "overrideRevision": override_revision,
    }


def _normalize_monitor_alert_delivery_override(
    value: Any,
) -> dict[str, Any] | None:
    if (
        not isinstance(value, Mapping)
        or type(value.get("enabled")) is not bool
        or _parse_datetime(value.get("updatedAt")) is None
    ):
        return None
    updated_by = _text(value.get("updatedBy"))
    if updated_by not in {
        "manual_cutover_seed",
        "manual_offline_override",
    }:
        return None
    return {
        "enabled": value["enabled"],
        "updatedAt": _text(value.get("updatedAt")),
        "updatedBy": updated_by,
    }


def _validate_migrated_fingerprint_state(
    value: Mapping[str, Any] | None,
    *,
    timestamp_key: str,
) -> dict[str, dict[str, Any]]:
    if value is None:
        return {}
    if not isinstance(value, Mapping) or len(value) > _MAX_MIGRATED_FINGERPRINTS:
        raise ValueError("device health monitor fingerprint seed is invalid")
    result: dict[str, dict[str, Any]] = {}
    for raw_key, raw_item in value.items():
        key = raw_key.strip() if isinstance(raw_key, str) else ""
        expected_item_keys = {
            timestamp_key,
            "lastSeenAt",
            "count",
            *({"lastAlertedAt"} if timestamp_key == "firstAlertedAt" else set()),
        }
        if (
            not key
            or len(key) > 1_024
            or not key.isprintable()
            or not isinstance(raw_item, Mapping)
            or set(raw_item) != expected_item_keys
        ):
            raise ValueError("device health monitor fingerprint seed is invalid")
        count = raw_item.get("count")
        if type(count) is not int or not 0 <= count <= 1_000_000:
            raise ValueError("device health monitor fingerprint count is invalid")
        first_at = raw_item.get(timestamp_key)
        last_seen_at = raw_item.get("lastSeenAt")
        if (
            not isinstance(first_at, str)
            or not isinstance(last_seen_at, str)
            or _parse_seed_datetime(first_at) is None
            or _parse_seed_datetime(last_seen_at) is None
        ):
            raise ValueError("device health monitor fingerprint time is invalid")
        normalized = {
            timestamp_key: first_at,
            "lastSeenAt": last_seen_at,
            "count": count,
        }
        if timestamp_key == "firstAlertedAt":
            last_alerted_at = raw_item.get("lastAlertedAt")
            if (
                not isinstance(last_alerted_at, str)
                or _parse_seed_datetime(last_alerted_at) is None
            ):
                raise ValueError("device health monitor fingerprint time is invalid")
            normalized["lastAlertedAt"] = last_alerted_at
        # 이전 bundle은 dedupe key의 4개 축을 모두 보존해야 한다.
        canonical_key = (
            validate_and_canonicalize_device_health_alert_fingerprint_key(key)
        )
        result[canonical_key] = _merge_fingerprint_state(
            result.get(canonical_key),
            normalized,
            timestamp_key=timestamp_key,
        )
    return result


def _parse_seed_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def _normalize_fingerprint_state(
    value: Any,
    *,
    timestamp_key: str = "firstAlertedAt",
) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, dict[str, Any]] = {}
    for key, item in value.items():
        if not isinstance(item, Mapping):
            continue
        normalized = {
            timestamp_key: _text(item.get(timestamp_key)),
            "lastSeenAt": _text(item.get("lastSeenAt")),
            "count": max(0, int(item.get("count") or 0)),
        }
        if timestamp_key == "firstAlertedAt":
            normalized["lastAlertedAt"] = _text(item.get("lastAlertedAt"))
        canonical_key = canonicalize_device_health_alert_fingerprint_key(key)
        if not canonical_key:
            continue
        result[canonical_key] = _merge_fingerprint_state(
            result.get(canonical_key),
            normalized,
            timestamp_key=timestamp_key,
        )
    return result


def _merge_fingerprint_state(
    current: Mapping[str, Any] | None,
    incoming: Mapping[str, Any],
    *,
    timestamp_key: str,
) -> dict[str, Any]:
    """두 historical label key가 합쳐져도 최근 발송 dedupe를 보존한다."""

    if not current:
        return dict(incoming)
    merged = dict(current)
    merged[timestamp_key] = _select_fingerprint_timestamp(
        current.get(timestamp_key),
        incoming.get(timestamp_key),
        latest=False,
    )
    merged["lastSeenAt"] = _select_fingerprint_timestamp(
        current.get("lastSeenAt"),
        incoming.get("lastSeenAt"),
        latest=True,
    )
    merged["count"] = max(
        int(current.get("count") or 0),
        int(incoming.get("count") or 0),
    )
    if timestamp_key == "firstAlertedAt":
        merged["lastAlertedAt"] = _select_fingerprint_timestamp(
            current.get("lastAlertedAt"),
            incoming.get("lastAlertedAt"),
            latest=True,
        )
    return merged


def _select_fingerprint_timestamp(
    left: Any,
    right: Any,
    *,
    latest: bool,
) -> str:
    candidates = [
        (_parse_datetime(value), _text(value))
        for value in (left, right)
        if _parse_datetime(value) is not None
    ]
    if not candidates:
        return _text(right) or _text(left)
    selected = max(candidates) if latest else min(candidates)
    return selected[1]


def _normalize_ssh_records(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, Mapping):
        return {}
    return {
        str(key): {
            "lastVerifiedAt": _text(item.get("lastVerifiedAt")),
            "ready": bool(item.get("ready")),
            "reasonCode": _text(item.get("reasonCode")),
            **({"errorType": _text(item.get("errorType"))} if item.get("errorType") else {}),
        }
        for key, item in value.items()
        if isinstance(item, Mapping)
    }


def _safe_device_cache_item(item: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "deviceSeq": _positive_int(item.get("deviceSeq")),
        "deviceName": _text(item.get("deviceName")),
        "hospitalSeq": _positive_int(item.get("hospitalSeq")),
        "hospitalRoomSeq": _positive_int(item.get("hospitalRoomSeq")),
        "hospitalName": _text(item.get("hospitalName")),
        "roomName": _text(item.get("roomName")),
    }


def _sheet_alert_item(item: Mapping[str, Any]) -> dict[str, Any]:
    """Sheets row와 SMS delivery poll에 필요한 non-PII 필드만 남긴다."""

    return {
        "hospitalSeq": _text(item.get("hospitalSeq")),
        "hospitalName": _text(item.get("hospitalName")),
        "hospital": _text(item.get("hospital")),
        "room": _text(item.get("room")),
        "device": _text(item.get("device")),
        "issue": _text(item.get("issue")),
        "problemComponents": list(item.get("problemComponents") or []),
        "alertCategory": _text(item.get("alertCategory")),
        "smsStatusText": _text(item.get("smsStatusText")),
        "smsProvider": _text(item.get("smsProvider")),
        "smsGroupId": _text(item.get("smsGroupId")),
        "smsMessageId": _text(item.get("smsMessageId")),
        "smsDeliveryStatus": _text(item.get("smsDeliveryStatus")),
        "smsAcceptedAt": _text(item.get("smsAcceptedAt")),
        "sheetDeliveryId": _text(item.get("sheetDeliveryId")),
    }


def _status_counts(items: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    counts = {"정상": 0, "확인 필요": 0, "이상": 0, "점검 불가": 0}
    for item in items:
        label = _text(item.get("overallLabel"))
        counts[label if label in counts else "점검 불가"] += 1
    return counts


def _alert_fingerprint(item: Mapping[str, Any]) -> str:
    return canonical_device_health_alert_fingerprint(item)


def _required_confirmation_polls(item: Mapping[str, Any]) -> int:
    components = set(item.get("problemComponents") or [])
    issue = _text(item.get("issue"))
    if components & {"캡처보드", "LED"}:
        return 2
    lowered = issue.lower()
    if "캡처보드" in issue or "비디오 장치" in issue or "led" in lowered:
        return 2
    return 1


def _suppressible_captureboard(item: Mapping[str, Any]) -> bool:
    return set(item.get("problemComponents") or []) == {"캡처보드"}


def _alert_category(components: Sequence[str], issue: str) -> str:
    normalized = set(components)
    categories = {
        _COMPONENT_CATEGORIES[key]
        for key, label in _COMPONENT_LABELS.items()
        if label in normalized
    }
    lowered = issue.lower()
    if any(marker in lowered for marker in ("병합", "ffmpeg", "merge")):
        return "recording_processing"
    if len(categories) > 1:
        return "mixed"
    if categories:
        return next(iter(categories))
    if any(marker in lowered for marker in ("offline", "disconnect", "연결")):
        return "device_connection"
    return "device_connection"


def _hospital_label(item: Mapping[str, Any]) -> str:
    name = _text(item.get("hospitalName")) or "병원 미확인"
    seq = _positive_int(item.get("hospitalSeq"))
    # Slack legacy collector와 같은 표시를 써 card payload와 durable
    # fingerprint가 전환 전후 동일하게 유지되게 한다.
    return f"#{seq} {name}" if seq else name


def _delivery_id(item: Mapping[str, Any]) -> str:
    stable_occurrence = "\0".join(
        (_alert_fingerprint(item), _text(item.get("alertOccurrenceAt")))
    )
    digest = hashlib.sha256(stable_occurrence.encode("utf-8")).hexdigest()[:32]
    return f"device_health_monitor:{digest}"


def _parse_datetime(value: Any) -> datetime | None:
    text = _text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return _coerce_daily_device_round_now(parsed)


def _positive_int(value: Any) -> int | None:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return None
    return number if number > 0 else None


def _text(value: Any) -> str:
    return _display_value(value, default="").strip()


__all__ = [
    "acknowledge_device_health_monitor_deliveries",
    "build_device_health_monitor_seed_cursor",
    "device_health_monitor_cursor_digest",
    "DeviceHealthMonitorCycleDelivery",
    "DeviceHealthMonitorCycleDeps",
    "DeviceHealthMonitorCycleRun",
    "run_device_health_monitor_cycle",
    "update_device_health_monitor_alert_delivery_override",
]
