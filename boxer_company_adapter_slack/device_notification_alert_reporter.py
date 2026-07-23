import json
import logging
import threading
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company import settings as cs
from boxer_company.device_health_sheet import (
    _append_device_health_sheet_alerts,
    _load_device_health_sheet_captureboard_incidents,
)
from boxer_company.sms_delivery import (
    _SMS_DELIVERY_CONFIRM_REQUIRED,
    _SMS_DELIVERY_REQUEST_FAILED,
)
from boxer_company_adapter_slack.daily_device_round_reporter import (
    _collect_daily_device_round_abnormal_alert_items,
    _post_daily_device_round_abnormal_alert,
)
from boxer_company_adapter_slack.sms_delivery_reporter import (
    remember_sms_delivery_sheet_record,
)

_CAPTUREBOARD_CONNECTION_ERROR = "captureboard_connection_error"
_RECORDING_CRITICALLY_STALLED = "recording_critically_stalled"
_SEGMENTED_RECORDINGS_MERGE_ERROR = "segmented_recordings_merge_error"
_CAPTUREBOARD_INCIDENT_CODES = {
    _CAPTUREBOARD_CONNECTION_ERROR,
    _RECORDING_CRITICALLY_STALLED,
}
_CAPTUREBOARD_INCIDENT_OPEN_STATUSES = {"대기", "처리중", "진행중"}
_SUPPORTED_DEVICE_NOTIFICATION_CODES = (
    _CAPTUREBOARD_CONNECTION_ERROR,
    _RECORDING_CRITICALLY_STALLED,
    _SEGMENTED_RECORDINGS_MERGE_ERROR,
)
_DEVICE_NOTIFICATION_ALERT_BATCH_SIZE = 200
_DEVICE_NOTIFICATION_ALERT_TIMEZONE = ZoneInfo("Asia/Seoul")
_RECORDING_STALL_MIN_DURATION_SECONDS = 120
_RECORDING_STALL_MAX_EVENT_GAP_SECONDS = 300
_DEVICE_NOTIFICATION_ALERT_THREAD: threading.Thread | None = None
_DEVICE_NOTIFICATION_ALERT_THREAD_LOCK = threading.Lock()
_DEVICE_NOTIFICATION_AUTO_SMS_SENT_TEXT = "문자 발송 접수"
_DEVICE_NOTIFICATION_AUTO_SMS_FAILED_TEXT = "문자 자동발송 실패 - 수동 발송 가능"
_DeviceNotificationAutoSmsSender = Callable[..., dict[str, Any]]


def _device_notification_alert_state_path() -> Path:
    return Path(cs.DEVICE_NOTIFICATION_ALERT_STATE_PATH).expanduser()


def _coerce_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, dict) else {}


def _coerce_device_notification_alert_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(_DEVICE_NOTIFICATION_ALERT_TIMEZONE)
    if now.tzinfo is None:
        return now.replace(tzinfo=_DEVICE_NOTIFICATION_ALERT_TIMEZONE)
    return now.astimezone(_DEVICE_NOTIFICATION_ALERT_TIMEZONE)


def _serialize_db_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        # MDA의 TypeORM 연결은 UTC를 사용하므로 timezone 없는 DATETIME도 UTC로 보존한다.
        actual = (
            value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        )
        return actual.isoformat()
    return str(value or "").strip()


def _format_device_notification_occurred_at(value: Any) -> str:
    if isinstance(value, datetime):
        actual = (
            value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        )
    else:
        text = str(value or "").strip()
        if not text:
            return "미확인"
        try:
            actual = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return text
        if actual.tzinfo is None:
            actual = actual.replace(tzinfo=timezone.utc)
    return actual.astimezone(_DEVICE_NOTIFICATION_ALERT_TIMEZONE).strftime(
        "%Y-%m-%d %H:%M:%S KST"
    )


def _format_device_notification_issue_with_occurred_at(
    issue: str,
    occurred_at: Any,
) -> str:
    # 공통 Slack 카드가 감지 내용 전체를 code 스타일로 감싸므로 시각에는 backtick을 중첩하지 않는다.
    return (
        f"{issue} "
        f"(발생 {_format_device_notification_occurred_at(occurred_at)})"
    )


def _normalize_pending_event(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    notification_id = _coerce_int(value.get("notificationId"))
    code = str(value.get("code") or "").strip()
    if notification_id <= 0 or code not in _SUPPORTED_DEVICE_NOTIFICATION_CODES:
        return None
    return {
        "notificationId": notification_id,
        "deviceSeq": _coerce_int(value.get("deviceSeq")) or None,
        "deviceName": str(value.get("deviceName") or "").strip(),
        "code": code,
        "message": str(value.get("message") or "").strip(),
        "barcode": str(value.get("barcode") or "").strip(),
        "fileId": str(value.get("fileId") or "").strip(),
        "details": _normalize_json_object(value.get("details")),
        "occurredAt": _serialize_db_datetime(value.get("occurredAt")),
        "hospitalSeq": _coerce_int(value.get("hospitalSeq")) or None,
        "hospitalName": str(value.get("hospitalName") or "").strip(),
        "hospitalTelephone": str(value.get("hospitalTelephone") or "").strip(),
        "hospitalDeviceAlertPhone": str(
            value.get("hospitalDeviceAlertPhone") or ""
        ).strip(),
        "hospitalRoomSeq": _coerce_int(value.get("hospitalRoomSeq")) or None,
        "roomName": str(value.get("roomName") or "").strip(),
        # Slack 발송 재시도에서 같은 notificationId의 문자를 다시 보내지 않도록
        # 첫 시도 결과를 pending 이벤트 자체에 보존한다.
        "autoSms": _normalize_device_notification_auto_sms_result(
            value.get("autoSms")
        ),
    }


def _normalize_device_notification_auto_sms_result(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or not value.get("attempted"):
        return {}

    raw_action_enabled = value.get("smsContactActionEnabled")
    if isinstance(raw_action_enabled, bool):
        action_enabled = raw_action_enabled
    elif raw_action_enabled is None:
        action_enabled = str(value.get("status") or "").strip() != "sent"
    else:
        action_enabled = str(raw_action_enabled).strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }

    return {
        "attempted": True,
        "attemptedAt": str(value.get("attemptedAt") or "").strip(),
        "status": str(value.get("status") or "").strip(),
        "ok": bool(value.get("ok")),
        "smsStatusText": str(value.get("smsStatusText") or "").strip(),
        "smsContactActionEnabled": action_enabled,
        "smsPhoneNumber": str(value.get("smsPhoneNumber") or "").strip(),
        "smsMessage": str(value.get("smsMessage") or "").strip(),
        "smsTemplateId": str(value.get("smsTemplateId") or "").strip(),
        "smsProvider": str(value.get("smsProvider") or "").strip(),
        "smsGroupId": str(value.get("smsGroupId") or "").strip(),
        "smsMessageId": str(value.get("smsMessageId") or "").strip(),
        "smsDeliveryStatus": str(value.get("smsDeliveryStatus") or "").strip(),
        # Slack 재시도 상태에서도 최초 공급자 접수 시각을 잃지 않게 보존한다.
        "smsAcceptedAt": str(value.get("smsAcceptedAt") or "").strip(),
    }


def _normalize_recording_stall_incident(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    phase = str(value.get("phase") or "").strip()
    device_name = str(value.get("deviceName") or "").strip()
    last_notification_id = _coerce_int(value.get("lastNotificationId"))
    last_duration_seconds = _coerce_int(value.get("lastDurationSeconds"))
    last_occurred_at = str(value.get("lastOccurredAt") or "").strip()
    if (
        phase not in {"candidate", "alerted"}
        or not device_name
        or last_notification_id <= 0
        or last_duration_seconds <= 0
        or not last_occurred_at
    ):
        return None
    slack_message_ts = str(value.get("slackMessageTs") or "").strip()
    if phase == "alerted" and not slack_message_ts:
        return None
    return {
        "phase": phase,
        "deviceName": device_name,
        "barcode": str(value.get("barcode") or "").strip(),
        "fileId": str(value.get("fileId") or "").strip(),
        "fileType": str(value.get("fileType") or "").strip(),
        "currentStatus": str(value.get("currentStatus") or "").strip(),
        "firstNotificationId": _coerce_int(value.get("firstNotificationId"))
        or last_notification_id,
        "firstOccurredAt": str(value.get("firstOccurredAt") or "").strip()
        or last_occurred_at,
        "firstDurationSeconds": _coerce_int(value.get("firstDurationSeconds"))
        or last_duration_seconds,
        "lastNotificationId": last_notification_id,
        "lastOccurredAt": last_occurred_at,
        "lastDurationSeconds": last_duration_seconds,
        "lastCurrentSize": _coerce_optional_int(value.get("lastCurrentSize")),
        "slackMessageTs": slack_message_ts,
        "slackPermalink": str(value.get("slackPermalink") or "").strip(),
        "lastCommentNotificationId": _coerce_optional_int(
            value.get("lastCommentNotificationId")
        ),
    }


def _normalize_captureboard_incident_status(value: Any) -> str:
    # TA가 드롭다운 값에 공백을 넣어도 같은 처리 상태로 인식한다.
    return "".join(str(value or "").split())


def _normalize_captureboard_incident(
    value: Any,
    *,
    fallback_device_name: str = "",
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    device_name = str(
        value.get("deviceName") or fallback_device_name or ""
    ).strip()
    status = _normalize_captureboard_incident_status(value.get("status"))
    if not device_name or status not in _CAPTUREBOARD_INCIDENT_OPEN_STATUSES:
        return None

    opened_code = str(value.get("openedCode") or "").strip()
    if opened_code not in _CAPTUREBOARD_INCIDENT_CODES:
        opened_code = ""
    row_number = _coerce_optional_int(value.get("rowNumber"))
    return {
        "deviceName": device_name,
        "deviceSeq": _coerce_optional_int(value.get("deviceSeq")),
        "status": status,
        "slackMessageTs": str(value.get("slackMessageTs") or "").strip(),
        "slackPermalink": str(value.get("slackPermalink") or "").strip(),
        "rowNumber": row_number if row_number is not None and row_number > 0 else None,
        "openedNotificationId": _coerce_optional_int(
            value.get("openedNotificationId")
        ),
        "openedCode": opened_code,
        "openedAt": str(value.get("openedAt") or "").strip(),
        "lastSheetCheckedAt": str(value.get("lastSheetCheckedAt") or "").strip(),
        "lastSuppressedAt": str(value.get("lastSuppressedAt") or "").strip(),
        "lastSuppressedNotificationId": _coerce_optional_int(
            value.get("lastSuppressedNotificationId")
        ),
        "lastSuppressedCode": str(value.get("lastSuppressedCode") or "").strip(),
        "suppressedCount": max(0, _coerce_int(value.get("suppressedCount"))),
    }


def _normalize_device_notification_alert_state(value: Any) -> dict[str, Any]:
    state = dict(value) if isinstance(value, dict) else {}
    normalized_pending: list[dict[str, Any]] = []
    seen_ids: set[int] = set()
    raw_pending = state.get("pendingEvents")
    if isinstance(raw_pending, list):
        for item in raw_pending:
            event = _normalize_pending_event(item)
            if event is None or event["notificationId"] in seen_ids:
                continue
            seen_ids.add(event["notificationId"])
            normalized_pending.append(event)

    recent_captureboard_alerts: dict[str, dict[str, Any]] = {}
    raw_recent_alerts = state.get("recentCaptureboardAlerts")
    if isinstance(raw_recent_alerts, dict):
        for raw_device_name, raw_alert in raw_recent_alerts.items():
            device_name = str(raw_device_name or "").strip()
            if not device_name or not isinstance(raw_alert, dict):
                continue
            last_alerted_at = str(raw_alert.get("lastAlertedAt") or "").strip()
            if not last_alerted_at:
                continue
            recent_captureboard_alerts[device_name] = {
                "lastAlertedAt": last_alerted_at,
                "notificationId": _coerce_int(raw_alert.get("notificationId")) or None,
            }

    recording_stall_incidents: dict[str, dict[str, Any]] = {}
    raw_recording_incidents = state.get("recordingStallIncidents")
    if isinstance(raw_recording_incidents, dict):
        for raw_key, raw_incident in raw_recording_incidents.items():
            incident_key = str(raw_key or "").strip()
            incident = _normalize_recording_stall_incident(raw_incident)
            if incident_key and incident is not None:
                recording_stall_incidents[incident_key] = incident

    captureboard_incidents: dict[str, dict[str, Any]] = {}
    raw_captureboard_incidents = state.get("captureboardIncidents")
    if isinstance(raw_captureboard_incidents, dict):
        for raw_device_name, raw_incident in raw_captureboard_incidents.items():
            device_name = str(raw_device_name or "").strip()
            incident = _normalize_captureboard_incident(
                raw_incident,
                fallback_device_name=device_name,
            )
            if incident is not None:
                captureboard_incidents[incident["deviceName"]] = incident

    return {
        **state,
        "initialized": bool(state.get("initialized")),
        "lastSeenId": max(0, _coerce_int(state.get("lastSeenId"))),
        "pendingEvents": normalized_pending,
        "recentCaptureboardAlerts": recent_captureboard_alerts,
        "recordingStallIncidents": recording_stall_incidents,
        "captureboardIncidents": captureboard_incidents,
        "captureboardIncidentsLastSheetCheckedAt": str(
            state.get("captureboardIncidentsLastSheetCheckedAt") or ""
        ).strip(),
    }


def _load_device_notification_alert_state(
    state_path: Path | None = None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    path = state_path or _device_notification_alert_state_path()
    if not path.exists():
        return _normalize_device_notification_alert_state({})
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        if logger is not None:
            logger.warning(
                "장비 이벤트 알림 상태 파일을 읽지 못했어: %s",
                path,
                exc_info=True,
            )
        # 손상된 상태를 빈 커서로 덮으면 미발송 이벤트를 건너뛸 수 있으므로 복구 전까지 중단한다.
        raise RuntimeError(f"장비 이벤트 알림 상태 파일을 읽지 못했어: {path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"장비 이벤트 알림 상태 형식이 올바르지 않아: {path}")
    return _normalize_device_notification_alert_state(payload)


def _save_device_notification_alert_state(
    state: dict[str, Any],
    state_path: Path | None = None,
) -> None:
    path = state_path or _device_notification_alert_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp")
    payload = _normalize_device_notification_alert_state(state)

    # 커서와 발송 대기 목록은 같은 파일로 원자 교체해 둘 중 하나만 반영되는 상태를 막는다.
    try:
        temp_path.write_text(
            json.dumps(payload, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        temp_path.replace(path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def _load_latest_device_notification_id() -> int:
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(MAX(id), 0) AS latestId FROM device_notification"
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()
    return max(0, _coerce_int(row.get("latestId")))


def _load_device_notification_batch(
    last_seen_id: int,
    *,
    batch_size: int = _DEVICE_NOTIFICATION_ALERT_BATCH_SIZE,
) -> tuple[int, list[dict[str, Any]]]:
    normalized_last_seen_id = max(0, _coerce_int(last_seen_id))
    normalized_batch_size = max(1, min(500, _coerce_int(batch_size, 200)))
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COALESCE(MAX(id), 0) AS latestId FROM device_notification"
            )
            latest_row = cursor.fetchone() or {}
            latest_id = max(0, _coerce_int(latest_row.get("latestId")))
            if latest_id <= normalized_last_seen_id:
                return normalized_last_seen_id, []

            # 조회 시작 시점의 상한을 고정해 조회 중 추가된 이벤트는 다음 poll에서 처리한다.
            cursor.execute(
                "SELECT "
                "n.id AS notificationId, "
                "n.deviceSeq AS deviceSeq, "
                "n.deviceName AS deviceName, "
                "n.code AS code, "
                "n.message AS message, "
                "n.barcode AS barcode, "
                "n.fileId AS fileId, "
                "n.details AS details, "
                "n.occurredAt AS occurredAt, "
                "d.hospitalSeq AS hospitalSeq, "
                "d.hospitalRoomSeq AS hospitalRoomSeq, "
                "h.hospitalName AS hospitalName, "
                "h.telephone AS hospitalTelephone, "
                "h.deviceAlertPhone AS hospitalDeviceAlertPhone, "
                "hr.roomName AS roomName "
                "FROM device_notification n "
                "LEFT JOIN devices d ON n.deviceSeq = d.seq "
                "LEFT JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "WHERE n.id > %s "
                "AND n.id <= %s "
                "AND n.code IN (%s, %s, %s) "
                "ORDER BY n.id ASC "
                "LIMIT %s",
                (
                    normalized_last_seen_id,
                    latest_id,
                    _CAPTUREBOARD_CONNECTION_ERROR,
                    _RECORDING_CRITICALLY_STALLED,
                    _SEGMENTED_RECORDINGS_MERGE_ERROR,
                    normalized_batch_size,
                ),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    events = [
        event for row in rows if (event := _normalize_pending_event(row)) is not None
    ]
    if len(rows) >= normalized_batch_size and events:
        return events[-1]["notificationId"], events
    return latest_id, events


def _append_pending_events(
    state: dict[str, Any],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    pending = list(state.get("pendingEvents") or [])
    pending_ids = {
        _coerce_int(item.get("notificationId"))
        for item in pending
        if isinstance(item, dict)
    }
    for raw_event in events:
        event = _normalize_pending_event(raw_event)
        if event is None or event["notificationId"] in pending_ids:
            continue
        pending_ids.add(event["notificationId"])
        pending.append(event)
    return {**state, "pendingEvents": pending}


def _captureboard_incident_pending_device_names(
    state: dict[str, Any],
) -> set[str]:
    return {
        str(event.get("deviceName") or "").strip()
        for event in state.get("pendingEvents") or []
        if isinstance(event, dict)
        and str(event.get("code") or "").strip() in _CAPTUREBOARD_INCIDENT_CODES
        and str(event.get("deviceName") or "").strip()
    }


def _recording_stall_alerted_device_names(state: dict[str, Any]) -> set[str]:
    return {
        str(incident.get("deviceName") or "").strip()
        for incident in (state.get("recordingStallIncidents") or {}).values()
        if isinstance(incident, dict)
        and incident.get("phase") == "alerted"
        and str(incident.get("deviceName") or "").strip()
    }


def _clear_recording_stall_incidents_for_devices(
    state: dict[str, Any],
    device_names: set[str],
) -> dict[str, Any]:
    normalized_device_names = {
        str(device_name or "").strip()
        for device_name in device_names
        if str(device_name or "").strip()
    }
    if not normalized_device_names:
        return state
    incidents = {
        incident_key: incident
        for incident_key, incident in (
            state.get("recordingStallIncidents") or {}
        ).items()
        if not isinstance(incident, dict)
        or str(incident.get("deviceName") or "").strip()
        not in normalized_device_names
    }
    return {**state, "recordingStallIncidents": incidents}


def _normalize_sheet_captureboard_incident(
    value: Any,
    *,
    fallback_device_name: str,
) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    device_name = str(
        value.get("deviceName") or fallback_device_name or ""
    ).strip()
    if not device_name:
        return None
    row_number = _coerce_optional_int(value.get("rowNumber"))
    return {
        "deviceName": device_name,
        "status": _normalize_captureboard_incident_status(value.get("status")),
        "slackPermalink": str(value.get("slackPermalink") or "").strip(),
        "rowNumber": row_number if row_number is not None and row_number > 0 else None,
    }


def _refresh_captureboard_incidents_from_sheet(
    state: dict[str, Any],
    *,
    now: datetime,
    logger: logging.Logger,
) -> dict[str, Any]:
    current_incidents = dict(state.get("captureboardIncidents") or {})
    pending_device_names = _captureboard_incident_pending_device_names(state)
    # 완료 여부는 다음 관련 이벤트를 판단할 때만 필요하므로 빈 poll이나 merge 이벤트에서
    # 전체 Sheet를 반복 조회하지 않는다.
    if not pending_device_names:
        return state

    legacy_alerted_device_names = _recording_stall_alerted_device_names(state)
    try:
        loaded = _load_device_health_sheet_captureboard_incidents()
    except Exception:
        # Sheet 상태를 확인할 수 없을 때는 unified open 연결만 해제해 후속 이벤트를
        # 숨기지 않는다. Sheet와 연결되지 않은 기존 녹화 스레드는 재시도 흐름을 유지한다.
        reset_device_names = set(current_incidents)
        next_state = _clear_recording_stall_incidents_for_devices(
            state,
            reset_device_names,
        )
        logger.warning(
            "캡처보드 장애 처리 상태를 Google Sheets에서 읽지 못했어. "
            "후속 이벤트를 다시 알릴게 devices=%s",
            ",".join(sorted(reset_device_names)) or "없음",
            exc_info=True,
        )
        return {**next_state, "captureboardIncidents": {}}

    if loaded is None:
        # 비활성 Sheet의 unified open 상태만 해제하고 기존 녹화 스레드 동작은 보존한다.
        reset_device_names = set(current_incidents)
        next_state = _clear_recording_stall_incidents_for_devices(
            state,
            reset_device_names,
        )
        return {**next_state, "captureboardIncidents": {}}

    sheet_incidents: dict[str, dict[str, Any]] = {}
    if isinstance(loaded, dict):
        for raw_device_name, raw_incident in loaded.items():
            incident = _normalize_sheet_captureboard_incident(
                raw_incident,
                fallback_device_name=str(raw_device_name or "").strip(),
            )
            if incident is not None:
                sheet_incidents[incident["deviceName"]] = incident

    checked_at = now.isoformat()
    next_incidents: dict[str, dict[str, Any]] = {}
    reset_device_names: set[str] = set()
    tracked_device_names = (
        set(current_incidents)
        | pending_device_names
        | legacy_alerted_device_names
    )
    for device_name in tracked_device_names:
        sheet_incident = sheet_incidents.get(device_name)
        sheet_status = (
            str(sheet_incident.get("status") or "").strip()
            if isinstance(sheet_incident, dict)
            else ""
        )
        if sheet_status in _CAPTUREBOARD_INCIDENT_OPEN_STATUSES:
            previous = current_incidents.get(device_name, {})
            next_incidents[device_name] = {
                **previous,
                "deviceName": device_name,
                "status": sheet_status,
                "slackPermalink": str(
                    sheet_incident.get("slackPermalink")
                    or previous.get("slackPermalink")
                    or ""
                ).strip(),
                "rowNumber": sheet_incident.get("rowNumber"),
                "openedAt": str(previous.get("openedAt") or checked_at),
                "lastSheetCheckedAt": checked_at,
            }
            # Sheet에 열린 행이 있으면 기존 녹화 후보·스레드는 같은 장애에 속하므로 폐기한다.
            reset_device_names.add(device_name)
        elif device_name in current_incidents:
            # Sheet에 실제로 연결했던 장애만 완료·이상없음·행 없음일 때 닫는다.
            # Sheet 기록에 실패한 녹화 알림 상태까지 지우면 후속 이벤트가 새 루트·문자로 중복된다.
            reset_device_names.add(device_name)

    next_state = _clear_recording_stall_incidents_for_devices(
        state,
        reset_device_names,
    )
    return {
        **next_state,
        "captureboardIncidents": next_incidents,
        "captureboardIncidentsLastSheetCheckedAt": checked_at,
    }


def _mark_captureboard_incident_open(
    state: dict[str, Any],
    event: dict[str, Any],
    delivery: dict[str, str],
    *,
    now: datetime,
) -> dict[str, Any]:
    device_name = str(event.get("deviceName") or "").strip()
    if not device_name:
        return state
    incidents = dict(state.get("captureboardIncidents") or {})
    incidents[device_name] = {
        "deviceName": device_name,
        "deviceSeq": _coerce_optional_int(event.get("deviceSeq")),
        "status": "대기",
        "slackMessageTs": str(delivery.get("messageTs") or "").strip(),
        "slackPermalink": str(delivery.get("permalink") or "").strip(),
        "rowNumber": None,
        "openedNotificationId": _coerce_optional_int(event.get("notificationId")),
        "openedCode": str(event.get("code") or "").strip(),
        "openedAt": now.isoformat(),
        "lastSheetCheckedAt": "",
        "lastSuppressedAt": "",
        "lastSuppressedNotificationId": None,
        "lastSuppressedCode": "",
        "suppressedCount": 0,
    }
    next_state = {**state, "captureboardIncidents": incidents}
    # 같은 batch의 다음 녹화 이벤트가 이전 후보나 스레드로 이어지지 않게 즉시 정리한다.
    return _clear_recording_stall_incidents_for_devices(
        next_state,
        {device_name},
    )


def _suppress_open_captureboard_incident_event(
    state: dict[str, Any],
    event: dict[str, Any],
    *,
    now: datetime,
) -> dict[str, Any] | None:
    code = str(event.get("code") or "").strip()
    device_name = str(event.get("deviceName") or "").strip()
    if code not in _CAPTUREBOARD_INCIDENT_CODES or not device_name:
        return None
    incidents = dict(state.get("captureboardIncidents") or {})
    incident = incidents.get(device_name)
    if not isinstance(incident, dict):
        return None
    incidents[device_name] = {
        **incident,
        "lastSuppressedAt": now.isoformat(),
        "lastSuppressedNotificationId": _coerce_optional_int(
            event.get("notificationId")
        ),
        "lastSuppressedCode": code,
        "suppressedCount": max(0, _coerce_int(incident.get("suppressedCount")))
        + 1,
    }
    return {**state, "captureboardIncidents": incidents}


def _load_recent_captureboard_notification_alerts(
    *,
    now: datetime,
    state_path: Path | None = None,
) -> dict[str, datetime]:
    try:
        state = _load_device_notification_alert_state(state_path)
    except RuntimeError:
        return {}

    reminder_delta = timedelta(
        hours=max(1, int(cs.DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS))
    )
    recent: dict[str, datetime] = {}
    for device_name, payload in state["recentCaptureboardAlerts"].items():
        try:
            alerted_at = datetime.fromisoformat(str(payload["lastAlertedAt"]))
        except (KeyError, TypeError, ValueError):
            continue
        if alerted_at.tzinfo is None:
            alerted_at = alerted_at.replace(tzinfo=_DEVICE_NOTIFICATION_ALERT_TIMEZONE)
        alerted_at = alerted_at.astimezone(_DEVICE_NOTIFICATION_ALERT_TIMEZONE)
        if now - alerted_at < reminder_delta:
            recent[device_name] = alerted_at
    return recent


def _build_captureboard_notification_alert_summary(
    event: dict[str, Any],
) -> dict[str, Any]:
    hospital_seq = _coerce_int(event.get("hospitalSeq")) or None
    hospital_name = str(event.get("hospitalName") or "").strip() or "병원 미확인"
    room_name = str(event.get("roomName") or "").strip() or "병실 미확인"
    device_name = str(event.get("deviceName") or "").strip() or "장비명 미확인"
    message = str(event.get("message") or "").strip()
    issue = message or "캡처보드 연결 장애가 발생했어"
    issue = _format_device_notification_issue_with_occurred_at(
        issue,
        event.get("occurredAt"),
    )

    return {
        "hospitalSeq": hospital_seq,
        "hospitalName": hospital_name,
        "statusCounts": {
            "정상": 0,
            "확인 필요": 0,
            "이상": 1,
            "점검 불가": 0,
        },
        "deviceResults": [
            {
                "hospitalSeq": hospital_seq,
                "hospitalName": hospital_name,
                "hospitalTelephone": str(event.get("hospitalTelephone") or "").strip(),
                "hospitalDeviceAlertPhone": str(
                    event.get("hospitalDeviceAlertPhone") or ""
                ).strip(),
                "hospitalRoomSeq": _coerce_int(event.get("hospitalRoomSeq")) or None,
                "roomName": room_name,
                "deviceName": device_name,
                "overallLabel": "이상",
                "priorityReason": issue,
                # 이벤트 코드를 사용자 영향 중심의 공통 Slack 제목 범주로 전달한다.
                "alertCategory": "video_signal",
                "componentLabels": {
                    "audio": "정상",
                    "pm2": "정상",
                    "storage": "정상",
                    "captureboard": "이상",
                    "led": "정상",
                },
                "statusPayload": {
                    "overview": {
                        "captureboard": {
                            "status": "fail",
                            "label": "이상",
                            "summary": issue,
                        }
                    }
                },
            }
        ],
    }


def _build_segmented_recordings_merge_alert_summary(
    event: dict[str, Any],
) -> dict[str, Any]:
    hospital_seq = _coerce_int(event.get("hospitalSeq")) or None
    hospital_name = str(event.get("hospitalName") or "").strip() or "병원 미확인"
    room_name = str(event.get("roomName") or "").strip() or "병실 미확인"
    device_name = str(event.get("deviceName") or "").strip() or "장비명 미확인"
    details = _normalize_json_object(event.get("details"))
    segment_count = _coerce_optional_int(details.get("segmentCount"))
    error_detail = str(details.get("error") or "").strip()
    if len(error_detail) > 300:
        error_detail = f"{error_detail[:297]}..."

    issue_parts = [
        str(event.get("message") or "").strip()
        or "분할된 녹화 파일 병합에 실패했어"
    ]
    if isinstance(segment_count, int) and segment_count > 0:
        issue_parts.append(f"분할 파일 {segment_count}개")
    if error_detail:
        issue_parts.append(f"오류: {error_detail}")
    issue = _format_device_notification_issue_with_occurred_at(
        " / ".join(issue_parts),
        event.get("occurredAt"),
    )

    return {
        "hospitalSeq": hospital_seq,
        "hospitalName": hospital_name,
        "statusCounts": {
            "정상": 0,
            "확인 필요": 0,
            "이상": 1,
            "점검 불가": 0,
        },
        "deviceResults": [
            {
                "hospitalSeq": hospital_seq,
                "hospitalName": hospital_name,
                "hospitalTelephone": str(event.get("hospitalTelephone") or "").strip(),
                "hospitalDeviceAlertPhone": str(
                    event.get("hospitalDeviceAlertPhone") or ""
                ).strip(),
                "hospitalRoomSeq": _coerce_int(event.get("hospitalRoomSeq")) or None,
                "roomName": room_name,
                "deviceName": device_name,
                "overallLabel": "이상",
                "priorityReason": issue,
                # 병합 구현명보다 사용자가 이해하기 쉬운 녹화 파일 처리 범주로 표시한다.
                "alertCategory": "recording_processing",
                # 병합 실패만으로 캡처보드나 저장장치 원인을 단정하지 않는다.
                "componentLabels": {
                    "audio": "정상",
                    "pm2": "정상",
                    "storage": "정상",
                    "captureboard": "정상",
                    "led": "정상",
                },
            }
        ],
    }


def _parse_device_notification_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        actual = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            actual = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if actual.tzinfo is None:
        actual = actual.replace(tzinfo=timezone.utc)
    return actual.astimezone(timezone.utc)


def _record_device_notification_sheet_alert_best_effort(
    alert_summary: dict[str, Any],
    event: dict[str, Any],
    *,
    fallback_detected_at: datetime,
    slack_permalink: str,
    logger: logging.Logger,
) -> bool:
    detected_at = _parse_device_notification_datetime(event.get("occurredAt"))
    if detected_at is None:
        detected_at = _coerce_device_notification_alert_now(fallback_detected_at)
    alert_items = _collect_daily_device_round_abnormal_alert_items(alert_summary)
    if cs.DEVICE_HEALTH_SHEET_ENABLED:
        for alert_item in alert_items:
            try:
                # Sheet append보다 먼저 접수 결과를 보존해 일시적인 Sheets 장애 뒤에도 재기록한다.
                remember_sms_delivery_sheet_record(
                    alert_item,
                    detected_at=detected_at,
                    permalink=str(slack_permalink or "").strip(),
                )
            except Exception as exc:
                logger.warning(
                    "장비 이벤트 문자 발송 추적값을 outbox에 저장하지 못했어 "
                    "notification_id=%s device=%s error_type=%s",
                    event.get("notificationId"),
                    str(alert_item.get("device") or "").strip(),
                    type(exc).__name__,
                )
    try:
        # Slack 루트 알림이 발송된 이벤트만 공통 A:R 형식으로 기록해 스레드 진행 답변은 중복 행을 만들지 않는다.
        row_count = _append_device_health_sheet_alerts(
            alert_items,
            detected_at=detected_at,
            slack_permalink=str(slack_permalink or "").strip(),
        )
    except Exception:
        # Sheets 일시 오류가 이미 성공한 Slack 알림과 이벤트 커서 처리를 되돌리지 않게 한다.
        logger.warning(
            "장비 이벤트 알림을 Google Sheets에 기록하지 못했어 "
            "notification_id=%s code=%s",
            event.get("notificationId"),
            event.get("code"),
            exc_info=True,
        )
        return False
    if row_count is not None:
        logger.info(
            "Recorded Boxer device notification alert rows=%s notification_id=%s code=%s",
            row_count,
            event.get("notificationId"),
            event.get("code"),
        )
    # 비활성 Sheet나 빈 append는 TA가 완료할 행이 없으므로 open incident로 보지 않는다.
    return row_count is not None and row_count > 0


def _recording_stall_context(event: dict[str, Any]) -> dict[str, Any] | None:
    device_name = str(event.get("deviceName") or "").strip()
    if not device_name:
        return None
    details = _normalize_json_object(event.get("details"))
    file_type = str(details.get("fileType") or "").strip().lower()
    return {
        "deviceName": device_name,
        "barcode": str(event.get("barcode") or details.get("barcode") or "").strip(),
        "fileId": str(event.get("fileId") or details.get("fileId") or "").strip(),
        "fileType": file_type,
        "currentStatus": str(details.get("currentStatus") or "").strip().lower(),
        "durationSeconds": _coerce_optional_int(details.get("durationSeconds")),
        "growthRate": _coerce_optional_float(details.get("growthRate")),
        "expectedMinGrowth": _coerce_optional_float(details.get("expectedMinGrowth")),
        "currentSize": _coerce_optional_int(details.get("currentSize")),
        "occurredAt": _serialize_db_datetime(event.get("occurredAt")),
    }


def _recording_stall_incident_key(context: dict[str, Any]) -> str:
    # 장비 앱이 fileId를 보내기 시작하면 녹화 단위로 자동 분리하고, 현재 payload는 장비 단위로 묶는다.
    return "|".join(
        (
            str(context.get("deviceName") or "").strip(),
            str(context.get("fileId") or "-").strip(),
            str(context.get("barcode") or "-").strip(),
            str(context.get("fileType") or "recording").strip(),
        )
    )


def _is_recording_stall_scope(context: dict[str, Any]) -> bool:
    duration_seconds = context.get("durationSeconds")
    return (
        context.get("currentStatus") == "recording"
        and context.get("fileType") != "motion"
        and isinstance(duration_seconds, int)
        and duration_seconds >= _RECORDING_STALL_MIN_DURATION_SECONDS
    )


def _is_zero_growth_recording_stall_candidate(context: dict[str, Any]) -> bool:
    growth_rate = context.get("growthRate")
    return (
        _is_recording_stall_scope(context)
        and isinstance(growth_rate, (int, float))
        and growth_rate == 0
        and context.get("currentSize") is not None
    )


def _is_recording_stall_continuation(
    incident: dict[str, Any],
    context: dict[str, Any],
) -> bool:
    duration_seconds = _coerce_int(context.get("durationSeconds"))
    previous_duration_seconds = _coerce_int(incident.get("lastDurationSeconds"))
    occurred_at = _parse_device_notification_datetime(context.get("occurredAt"))
    previous_occurred_at = _parse_device_notification_datetime(
        incident.get("lastOccurredAt")
    )
    if occurred_at is None or previous_occurred_at is None:
        return False
    gap_seconds = (occurred_at - previous_occurred_at).total_seconds()
    return (
        duration_seconds > previous_duration_seconds
        and 0 < gap_seconds <= _RECORDING_STALL_MAX_EVENT_GAP_SECONDS
    )


def _new_recording_stall_candidate(
    event: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    notification_id = _coerce_int(event.get("notificationId"))
    duration_seconds = _coerce_int(context.get("durationSeconds"))
    occurred_at = str(context.get("occurredAt") or "").strip()
    return {
        "phase": "candidate",
        "deviceName": context["deviceName"],
        "barcode": context.get("barcode") or "",
        "fileId": context.get("fileId") or "",
        "fileType": context.get("fileType") or "",
        "currentStatus": context.get("currentStatus") or "",
        "firstNotificationId": notification_id,
        "firstOccurredAt": occurred_at,
        "firstDurationSeconds": duration_seconds,
        "lastNotificationId": notification_id,
        "lastOccurredAt": occurred_at,
        "lastDurationSeconds": duration_seconds,
        "lastCurrentSize": context.get("currentSize"),
        "slackMessageTs": "",
        "slackPermalink": "",
        "lastCommentNotificationId": None,
    }


def _format_recording_stall_duration(duration_seconds: int) -> str:
    if duration_seconds >= 60 and duration_seconds % 60 == 0:
        return f"{duration_seconds}초 ({duration_seconds // 60}분)"
    return f"{duration_seconds}초"


def _format_recording_stall_growth_rate(value: Any) -> str:
    growth_rate = _coerce_optional_float(value)
    if growth_rate is None:
        return "미확인"
    return f"{growth_rate / 1024:.2f} KB/sec"


def _build_recording_stall_alert_summary(
    event: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    hospital_seq = _coerce_int(event.get("hospitalSeq")) or None
    hospital_name = str(event.get("hospitalName") or "").strip() or "병원 미확인"
    room_name = str(event.get("roomName") or "").strip() or "병실 미확인"
    duration = _format_recording_stall_duration(
        _coerce_int(context.get("durationSeconds"))
    )
    growth_rate = _format_recording_stall_growth_rate(context.get("growthRate"))
    issue = _format_device_notification_issue_with_occurred_at(
        f"녹화 파일 증가 정지가 {duration} 동안 지속됐어: "
        f"{growth_rate}",
        event.get("occurredAt"),
    )
    return {
        "hospitalSeq": hospital_seq,
        "hospitalName": hospital_name,
        "statusCounts": {
            "정상": 0,
            "확인 필요": 0,
            "이상": 1,
            "점검 불가": 0,
        },
        "deviceResults": [
            {
                "hospitalSeq": hospital_seq,
                "hospitalName": hospital_name,
                "hospitalTelephone": str(event.get("hospitalTelephone") or "").strip(),
                "hospitalDeviceAlertPhone": str(
                    event.get("hospitalDeviceAlertPhone") or ""
                ).strip(),
                "hospitalRoomSeq": _coerce_int(event.get("hospitalRoomSeq")) or None,
                "roomName": room_name,
                "deviceName": context["deviceName"],
                "overallLabel": "이상",
                "priorityReason": issue,
                # 파일 증가 정지는 원인이 확정되지 않았으므로 녹화 상태 확인으로 안내한다.
                "alertCategory": "recording",
                # 원인은 아직 특정하지 않았으므로 캡처보드나 저장장치 이상으로 표시하지 않는다.
                "componentLabels": {
                    "audio": "정상",
                    "pm2": "정상",
                    "storage": "정상",
                    "captureboard": "정상",
                    "led": "정상",
                },
            }
        ],
    }


def _extract_slack_message_ts(response: Any) -> str:
    direct = str(
        getattr(response, "get", lambda *_args, **_kwargs: "")("ts") or ""
    ).strip()
    if direct:
        return direct
    response_data = getattr(response, "data", None)
    return str(
        getattr(response_data, "get", lambda *_args, **_kwargs: "")("ts") or ""
    ).strip()


def _post_recording_stall_thread_reply(
    client: Any,
    event: dict[str, Any],
    context: dict[str, Any],
    *,
    channel_id: str,
    thread_ts: str,
    logger: logging.Logger,
) -> dict[str, str] | None:
    duration = _format_recording_stall_duration(
        _coerce_int(context.get("durationSeconds"))
    )
    growth_rate = _format_recording_stall_growth_rate(context.get("growthRate"))
    occurred_at = _format_device_notification_occurred_at(event.get("occurredAt"))
    text = "\n".join(
        (
            ":warning: *녹화 파일 증가 정지 지속*",
            f"> *지속 시간*  `{duration}`",
            f"> *현재 증가율*  `{growth_rate}`",
            f"> *발생 시각*  `{occurred_at}`",
        )
    )
    try:
        response = client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=text,
            unfurl_links=False,
            unfurl_media=False,
        )
    except Exception:
        logger.warning(
            "녹화 정지 반복 댓글을 보내지 못했어 channel=%s thread_ts=%s notification_id=%s",
            channel_id,
            thread_ts,
            event.get("notificationId"),
            exc_info=True,
        )
        return None
    return {
        "messageTs": _extract_slack_message_ts(response),
        "permalink": "",
    }


def _apply_device_notification_auto_sms_result(
    alert_summary: dict[str, Any],
    auto_sms_result: dict[str, Any],
) -> dict[str, Any]:
    device_results = (
        alert_summary.get("deviceResults")
        if isinstance(alert_summary.get("deviceResults"), list)
        else []
    )
    next_device_results: list[Any] = []
    applied = False
    for device_result in device_results:
        if (
            applied
            or not isinstance(device_result, dict)
            or str(device_result.get("overallLabel") or "").strip() != "이상"
        ):
            next_device_results.append(device_result)
            continue

        next_device_result = dict(device_result)
        next_device_result["smsContactActionEnabled"] = (
            "true"
            if auto_sms_result.get("smsContactActionEnabled", True)
            else "false"
        )
        for key in (
            "smsStatusText",
            "smsPhoneNumber",
            "smsMessage",
            "smsTemplateId",
            "smsProvider",
            "smsGroupId",
            "smsMessageId",
            "smsDeliveryStatus",
            "smsAcceptedAt",
        ):
            value = str(auto_sms_result.get(key) or "").strip()
            if value:
                next_device_result[key] = value
        next_device_results.append(next_device_result)
        applied = True

    return {**alert_summary, "deviceResults": next_device_results}


def _prepare_device_notification_auto_sms(
    state: dict[str, Any],
    event: dict[str, Any],
    alert_summary: dict[str, Any],
    *,
    channel_id: str,
    now: datetime,
    state_path: Path | None,
    logger: logging.Logger,
    auto_sms_sender: _DeviceNotificationAutoSmsSender | None,
) -> dict[str, Any]:
    if auto_sms_sender is None:
        return alert_summary

    cached_result = _normalize_device_notification_auto_sms_result(
        event.get("autoSms")
    )
    if not cached_result:
        alert_items = _collect_daily_device_round_abnormal_alert_items(alert_summary)
        if not alert_items:
            return alert_summary

        # provider 응답 뒤에만 결과를 쓰면 발송 직후 프로세스 종료 시 같은 문자를
        # 재시도할 수 있다. 시도 claim을 먼저 원자 저장하되 실제 결과를 알 수 없는
        # crash window를 발송 실패로 단정하지 않는다.
        event["autoSms"] = _normalize_device_notification_auto_sms_result(
            {
                "attempted": True,
                "attemptedAt": now.isoformat(),
                "status": "attempting",
                "ok": False,
                "smsDeliveryStatus": _SMS_DELIVERY_CONFIRM_REQUIRED,
                "smsStatusText": "문자 발송 여부 확인 필요",
                "smsContactActionEnabled": False,
            }
        )
        _save_device_notification_alert_state(state, state_path)

        try:
            raw_result = auto_sms_sender(
                alert_items[0],
                channel_id=channel_id,
                now=now,
                logger=logger,
            )
            if not isinstance(raw_result, dict):
                raw_result = {"status": "invalid_result", "ok": False}
        except Exception as exc:
            # 문자 공급자 장애가 Slack·Sheet 장애 알림까지 막지 않도록 실패를 캐시하고 계속 진행한다.
            logger.warning(
                "장비 이벤트 자동문자 발송 중 오류가 발생했어 notification_id=%s code=%s",
                event.get("notificationId"),
                event.get("code"),
                exc_info=True,
            )
            raw_result = {
                "status": "error",
                "ok": False,
                "smsDeliveryStatus": _SMS_DELIVERY_REQUEST_FAILED,
                "smsStatusText": _DEVICE_NOTIFICATION_AUTO_SMS_FAILED_TEXT,
                "smsContactActionEnabled": True,
            }

        sent = bool(raw_result.get("ok")) and str(
            raw_result.get("status") or ""
        ).strip() == "sent"
        status = str(raw_result.get("status") or "").strip()
        status_text = str(raw_result.get("smsStatusText") or "").strip()
        if not status_text and sent:
            status_text = _DEVICE_NOTIFICATION_AUTO_SMS_SENT_TEXT
        elif not status_text and status not in {
            "manual_required",
            "unsupported_issue",
        }:
            status_text = _DEVICE_NOTIFICATION_AUTO_SMS_FAILED_TEXT

        cached_result = _normalize_device_notification_auto_sms_result(
            {
                **raw_result,
                "attempted": True,
                "attemptedAt": now.isoformat(),
                "smsStatusText": status_text,
                "smsContactActionEnabled": raw_result.get(
                    "smsContactActionEnabled",
                    not sent,
                ),
            }
        )
        # provider 호출 결과를 Slack보다 먼저 저장해 Slack 실패 재시도에서도
        # 같은 notificationId의 문자를 다시 호출하지 않는다.
        event["autoSms"] = cached_result
        _save_device_notification_alert_state(state, state_path)

    return _apply_device_notification_auto_sms_result(
        alert_summary,
        cached_result,
    )


def _process_recording_stall_event(
    client: Any,
    logger: logging.Logger,
    state: dict[str, Any],
    event: dict[str, Any],
    *,
    channel_id: str,
    now: datetime,
    state_path: Path | None = None,
    auto_sms_sender: _DeviceNotificationAutoSmsSender | None = None,
) -> tuple[dict[str, Any], bool, dict[str, str]] | None:
    context = _recording_stall_context(event)
    if context is None or not _is_recording_stall_scope(context):
        logger.info(
            "Skipped recording stall event outside alert scope notification_id=%s device=%s",
            event.get("notificationId"),
            event.get("deviceName"),
        )
        return state, False, {}

    incident_key = _recording_stall_incident_key(context)
    incidents = dict(state.get("recordingStallIncidents") or {})
    incident = incidents.get(incident_key)
    is_zero_growth_candidate = _is_zero_growth_recording_stall_candidate(context)

    if isinstance(incident, dict) and incident.get("phase") == "alerted":
        if _is_recording_stall_continuation(incident, context):
            thread_ts = str(incident.get("slackMessageTs") or "").strip()
            delivery = _post_recording_stall_thread_reply(
                client,
                event,
                context,
                channel_id=channel_id,
                thread_ts=thread_ts,
                logger=logger,
            )
            if delivery is None:
                return None
            incidents[incident_key] = {
                **incident,
                "lastNotificationId": _coerce_int(event.get("notificationId")),
                "lastOccurredAt": context["occurredAt"],
                "lastDurationSeconds": _coerce_int(context.get("durationSeconds")),
                "lastCurrentSize": context.get("currentSize"),
                "lastCommentNotificationId": _coerce_int(event.get("notificationId")),
            }
            return {**state, "recordingStallIncidents": incidents}, True, delivery

        # 지속 시간이 다시 시작했거나 이벤트 간격이 끊기면 현재 이벤트를 새 장애로 판단한다.
        incidents.pop(incident_key, None)
    elif isinstance(incident, dict) and incident.get("phase") == "candidate":
        # 배포 전 저장된 2분 후보도 다음 poll에서 현재 기준으로 바로 알릴 수 있게 이어받는다.
        incidents.pop(incident_key, None)

    if not is_zero_growth_candidate:
        return {**state, "recordingStallIncidents": incidents}, False, {}

    # 장비의 critical 이벤트가 이미 2분 연속 무증가를 검증하므로 Boxer에서
    # 같은 크기의 두 번째 이벤트를 기다리지 않고 첫 120초 이벤트를 즉시 알린다.
    alert_summary = _build_recording_stall_alert_summary(event, context)
    alert_summary = _prepare_device_notification_auto_sms(
        state,
        event,
        alert_summary,
        channel_id=channel_id,
        now=now,
        state_path=state_path,
        logger=logger,
        auto_sms_sender=auto_sms_sender,
    )
    delivery = _post_daily_device_round_abnormal_alert(
        client,
        alert_summary,
        channel_id=channel_id,
        message_ts="",
        logger=logger,
        include_blocks=True,
        include_actions=auto_sms_sender is not None,
        include_device_voice_action=False,
    )
    message_ts = str((delivery or {}).get("messageTs") or "").strip()
    if delivery is None or not message_ts:
        logger.warning(
            "녹화 정지 루트 알림을 보내지 못했어 notification_id=%s device=%s",
            event.get("notificationId"),
            context.get("deviceName"),
        )
        return None

    sheet_recorded = _record_device_notification_sheet_alert_best_effort(
        alert_summary,
        event,
        fallback_detected_at=now,
        slack_permalink=str(delivery.get("permalink") or "").strip(),
        logger=logger,
    )
    opened_incident = (
        incident
        if isinstance(incident, dict) and incident.get("phase") == "candidate"
        else _new_recording_stall_candidate(event, context)
    )
    incidents[incident_key] = {
        **opened_incident,
        "phase": "alerted",
        "lastNotificationId": _coerce_int(event.get("notificationId")),
        "lastOccurredAt": context["occurredAt"],
        "lastDurationSeconds": _coerce_int(context.get("durationSeconds")),
        "lastCurrentSize": context.get("currentSize"),
        "slackMessageTs": message_ts,
        "slackPermalink": str(delivery.get("permalink") or "").strip(),
    }
    next_state = {**state, "recordingStallIncidents": incidents}
    if sheet_recorded:
        # Sheet의 대기 행이 생성된 순간부터 같은 장비의 후속 이벤트를 한 장애로 묶는다.
        next_state = _mark_captureboard_incident_open(
            next_state,
            event,
            delivery,
            now=now,
        )
    return next_state, True, delivery


def _deliver_pending_device_notification_alerts(
    client: Any,
    logger: logging.Logger,
    state: dict[str, Any],
    *,
    channel_id: str,
    now: datetime,
    state_path: Path | None = None,
    auto_sms_sender: _DeviceNotificationAutoSmsSender | None = None,
) -> tuple[dict[str, Any], int]:
    next_state = _normalize_device_notification_alert_state(state)
    next_state = _refresh_captureboard_incidents_from_sheet(
        next_state,
        now=now,
        logger=logger,
    )
    sent_count = 0

    while next_state["pendingEvents"]:
        event = next_state["pendingEvents"][0]
        code = str(event.get("code") or "").strip()
        delivery: dict[str, str] = {}
        slack_sent = False

        suppressed_state = _suppress_open_captureboard_incident_event(
            next_state,
            event,
            now=now,
        )
        if suppressed_state is not None:
            # 원본 DB 이벤트는 남아 있으므로 발송 queue에서만 소비하고 억제 근거를 상태에 보존한다.
            next_state = {
                **suppressed_state,
                "pendingEvents": suppressed_state["pendingEvents"][1:],
            }
            _save_device_notification_alert_state(next_state, state_path)
            logger.info(
                "Suppressed open captureboard incident event notification_id=%s code=%s device=%s",
                event.get("notificationId"),
                code,
                event.get("deviceName"),
            )
            continue

        if code == _CAPTUREBOARD_CONNECTION_ERROR:
            alert_summary = _build_captureboard_notification_alert_summary(event)
            alert_summary = _prepare_device_notification_auto_sms(
                next_state,
                event,
                alert_summary,
                channel_id=channel_id,
                now=now,
                state_path=state_path,
                logger=logger,
                auto_sms_sender=auto_sms_sender,
            )
            posted = _post_daily_device_round_abnormal_alert(
                client,
                alert_summary,
                channel_id=channel_id,
                message_ts="",
                logger=logger,
                include_blocks=True,
                include_actions=auto_sms_sender is not None,
                include_device_voice_action=False,
            )
            if posted is None:
                logger.warning(
                    "장비 이벤트 Slack 알림을 보내지 못했어 notification_id=%s code=%s",
                    event.get("notificationId"),
                    code,
                )
                break
            sheet_recorded = _record_device_notification_sheet_alert_best_effort(
                alert_summary,
                event,
                fallback_detected_at=now,
                slack_permalink=str(posted.get("permalink") or "").strip(),
                logger=logger,
            )
            delivery = posted
            slack_sent = True
            device_name = str(event.get("deviceName") or "").strip()
            recent_captureboard_alerts = dict(next_state["recentCaptureboardAlerts"])
            if device_name:
                # 기존 Redis·SSH 모니터가 같은 장비를 다시 알리지 않도록 성공한 발송만 공유한다.
                recent_captureboard_alerts[device_name] = {
                    "lastAlertedAt": now.isoformat(),
                    "notificationId": event["notificationId"],
                }
            next_state = {
                **next_state,
                "recentCaptureboardAlerts": recent_captureboard_alerts,
            }
            if sheet_recorded:
                # append 직후에는 다시 Sheet를 읽지 않고도 같은 batch 후속 이벤트를 억제한다.
                next_state = _mark_captureboard_incident_open(
                    next_state,
                    event,
                    delivery,
                    now=now,
                )
        elif code == _RECORDING_CRITICALLY_STALLED:
            processed = _process_recording_stall_event(
                client,
                logger,
                next_state,
                event,
                channel_id=channel_id,
                now=now,
                state_path=state_path,
                auto_sms_sender=auto_sms_sender,
            )
            if processed is None:
                break
            next_state, slack_sent, delivery = processed
        elif code == _SEGMENTED_RECORDINGS_MERGE_ERROR:
            # 실제 FFmpeg 병합 실패 이벤트이므로 추가 장비 검증 없이 즉시 알린다.
            alert_summary = _build_segmented_recordings_merge_alert_summary(event)
            alert_summary = _prepare_device_notification_auto_sms(
                next_state,
                event,
                alert_summary,
                channel_id=channel_id,
                now=now,
                state_path=state_path,
                logger=logger,
                auto_sms_sender=auto_sms_sender,
            )
            posted = _post_daily_device_round_abnormal_alert(
                client,
                alert_summary,
                channel_id=channel_id,
                message_ts="",
                logger=logger,
                include_blocks=True,
                include_actions=auto_sms_sender is not None,
                include_device_voice_action=False,
            )
            if posted is None:
                logger.warning(
                    "분할 녹화 병합 실패 Slack 알림을 보내지 못했어 "
                    "notification_id=%s code=%s",
                    event.get("notificationId"),
                    code,
                )
                break
            _record_device_notification_sheet_alert_best_effort(
                alert_summary,
                event,
                fallback_detected_at=now,
                slack_permalink=str(posted.get("permalink") or "").strip(),
                logger=logger,
            )
            delivery = posted
            slack_sent = True
        else:
            logger.warning(
                "지원하지 않는 장비 이벤트가 대기열에 있어 제거할게 notification_id=%s code=%s",
                event.get("notificationId"),
                code,
            )

        next_state = {
            **next_state,
            "pendingEvents": next_state["pendingEvents"][1:],
        }
        if slack_sent:
            next_state = {
                **next_state,
                "lastSentAt": now.isoformat(),
                "lastSentNotificationId": event["notificationId"],
                "lastSlackMessageTs": str(delivery.get("messageTs") or "").strip(),
                "lastSlackPermalink": str(delivery.get("permalink") or "").strip(),
            }
        _save_device_notification_alert_state(next_state, state_path)
        if slack_sent:
            sent_count += 1
            logger.info(
                "Posted Boxer device notification alert channel=%s notification_id=%s code=%s device=%s",
                channel_id,
                event.get("notificationId"),
                code,
                event.get("deviceName"),
            )

    return next_state, sent_count


def _run_device_notification_alert_once(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
    state_path: Path | None = None,
    auto_sms_sender: _DeviceNotificationAutoSmsSender | None = None,
) -> bool:
    if not cs.DEVICE_NOTIFICATION_ALERT_ENABLED:
        return False
    if not s.DB_QUERY_ENABLED:
        logger.warning("장비 이벤트 알림을 켤 수 없어. DB_QUERY_ENABLED가 비활성이야")
        return False

    channel_id = str(cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID or "").strip()
    if not channel_id:
        logger.warning(
            "장비 이벤트 알림 채널 ID가 없어. DEVICE_NOTIFICATION_ALERT_CHANNEL_ID를 확인해줘"
        )
        return False

    local_now = _coerce_device_notification_alert_now(now)
    state = _load_device_notification_alert_state(state_path, logger=logger)
    if not state["initialized"]:
        latest_id = _load_latest_device_notification_id()
        initial_state = {
            **state,
            "initialized": True,
            "initializedAt": local_now.isoformat(),
            "lastSeenId": latest_id,
            "lastPolledAt": local_now.isoformat(),
        }
        _save_device_notification_alert_state(initial_state, state_path)
        logger.info(
            "Initialized Boxer device notification alert cursor latest_id=%s",
            latest_id,
        )
        return False

    state, sent_count = _deliver_pending_device_notification_alerts(
        client,
        logger,
        state,
        channel_id=channel_id,
        now=local_now,
        state_path=state_path,
        auto_sms_sender=auto_sms_sender,
    )
    if state["pendingEvents"]:
        return sent_count > 0

    next_cursor, events = _load_device_notification_batch(state["lastSeenId"])
    state = _append_pending_events(state, events)
    state = {
        **state,
        "lastSeenId": next_cursor,
        "lastPolledAt": local_now.isoformat(),
    }
    # Slack 호출 전에 커서와 이벤트를 함께 저장해야 발송 실패나 프로세스 종료에도 이벤트가 남는다.
    _save_device_notification_alert_state(state, state_path)

    state, newly_sent_count = _deliver_pending_device_notification_alerts(
        client,
        logger,
        state,
        channel_id=channel_id,
        now=local_now,
        state_path=state_path,
        auto_sms_sender=auto_sms_sender,
    )
    return sent_count + newly_sent_count > 0


def _device_notification_alert_loop(
    client: Any,
    logger: logging.Logger,
    auto_sms_sender: _DeviceNotificationAutoSmsSender | None = None,
) -> None:
    poll_interval_sec = max(
        10,
        int(cs.DEVICE_NOTIFICATION_ALERT_POLL_INTERVAL_SEC),
    )
    while True:
        try:
            _run_device_notification_alert_once(
                client,
                logger,
                auto_sms_sender=auto_sms_sender,
            )
        except Exception:
            logger.exception("장비 이벤트 알림 처리 중 오류가 발생했어")
        time.sleep(poll_interval_sec)


def attach_device_notification_alert_reporter(
    app: Any,
    *,
    logger: logging.Logger | None = None,
    auto_sms_sender: _DeviceNotificationAutoSmsSender | None = None,
) -> None:
    if not cs.DEVICE_NOTIFICATION_ALERT_ENABLED:
        return

    actual_logger = logger or logging.getLogger(__name__)
    if not s.DB_QUERY_ENABLED:
        actual_logger.warning(
            "장비 이벤트 알림이 활성화됐는데 DB_QUERY_ENABLED가 꺼져 있어 시작하지 않을게"
        )
        return

    channel_id = str(cs.DEVICE_NOTIFICATION_ALERT_CHANNEL_ID or "").strip()
    if not channel_id:
        actual_logger.warning(
            "장비 이벤트 알림이 활성화됐는데 채널 ID가 없어. "
            "DEVICE_NOTIFICATION_ALERT_CHANNEL_ID를 확인해줘"
        )
        return

    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("장비 이벤트 알림을 시작하지 못했어. Slack client가 없어")
        return

    global _DEVICE_NOTIFICATION_ALERT_THREAD
    with _DEVICE_NOTIFICATION_ALERT_THREAD_LOCK:
        if (
            _DEVICE_NOTIFICATION_ALERT_THREAD is not None
            and _DEVICE_NOTIFICATION_ALERT_THREAD.is_alive()
        ):
            return
        _DEVICE_NOTIFICATION_ALERT_THREAD = threading.Thread(
            target=_device_notification_alert_loop,
            args=(client, actual_logger, auto_sms_sender),
            name="boxer-device-notification-alert",
            daemon=True,
        )
        _DEVICE_NOTIFICATION_ALERT_THREAD.start()

    actual_logger.info(
        "Started Boxer device notification alert channel=%s interval=%ss codes=%s",
        channel_id,
        max(10, int(cs.DEVICE_NOTIFICATION_ALERT_POLL_INTERVAL_SEC)),
        ",".join(_SUPPORTED_DEVICE_NOTIFICATION_CODES),
    )
