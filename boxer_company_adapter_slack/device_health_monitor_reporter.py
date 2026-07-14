import hashlib
import hmac
import json
import logging
import re
import secrets
import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests

from boxer.core import settings as s
from boxer.core.utils import _display_value
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company import settings as cs
from boxer_company.daily_device_round import (
    _build_daily_device_round_priority,
    _build_daily_device_round_storage_details,
    _coerce_daily_device_round_hospital_seqs,
    _coerce_daily_device_round_now,
    _coerce_int,
    _daily_device_round_status_label,
)
from boxer_company.redis_device_state import DeviceStateRedisClient, DeviceStateRedisUnavailable
from boxer_company.routers.device_file_probe import (
    _connect_device_ssh_client,
    _get_active_device_ssh_client_count,
)
from boxer_company.routers.device_status_probe import (
    _PROBE_COMPONENT_COMMAND_KEYS,
    _build_trashcan_storage_summary_from_checks,
    _parse_device_path_list,
    _parse_pm2_processes,
    _parse_usb_devices,
    _run_status_probe_command,
    _summarize_audio_path_probe,
    _summarize_captureboard_probe,
    _summarize_led_probe,
    _summarize_pm2_probe,
)
from boxer_company.routers.mda_graphql import (
    _close_mda_device_ssh,
    _get_mda_device_agent_ssh,
    _open_mda_device_ssh,
)
from boxer_company.device_health_sheet import _append_device_health_sheet_alerts
from boxer_company_adapter_slack.daily_device_round_reporter import (
    _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
    _DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
    _DEVICE_HEALTH_ALERT_ACTION_MARK_DONE,
    _DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS,
    _collect_daily_device_round_abnormal_alert_items,
    _post_daily_device_round_abnormal_alert,
)

_DEVICE_HEALTH_MONITOR_THREAD: threading.Thread | None = None
_DEVICE_HEALTH_MONITOR_THREAD_LOCK = threading.Lock()
_DEVICE_HEALTH_MONITOR_RUNTIME_STATE: dict[str, Any] = {}
_DEVICE_HEALTH_MONITOR_RUNTIME_STATE_LOCK = threading.Lock()
_DEVICE_HEALTH_MONITOR_ACTION_IDS = {
    _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
    _DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
    _DEVICE_HEALTH_ALERT_ACTION_MARK_DONE,
    _DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS,
}
_DEVICE_HEALTH_MONITOR_ACTION_LABELS = {
    _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL: "병원 문자 보내기",
    _DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE: "장비 음성 안내(미구현)",
    _DEVICE_HEALTH_ALERT_ACTION_MARK_DONE: "확인 완료",
    _DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS: "문자 자동발송 완료",
}
_DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_URL_SETTINGS = {
    _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL: "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL",
}
_DEVICE_HEALTH_MONITOR_HOSPITAL_SEQ_PATTERN = re.compile(r"#\s*(\d+)")
_DEVICE_HEALTH_MONITOR_EXCLUDED_HOSPITAL_NAME_PATTERN = re.compile(r"^\d+_")
_DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID = "device_health_alert_contact_hospital_modal"
_DEVICE_HEALTH_MONITOR_SMS_VIEW_MODAL_CALLBACK_ID = "device_health_alert_auto_sms_view_modal"
_DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID = "device_health_alert_sms_phone"
_DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID = "phone_number"
_DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID = "device_health_alert_sms_message"
_DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID = "message"
_DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_SEND = "send"
_DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT = "view_auto_sent"
# 병원 관계자가 첫 줄에서 발신 주체를 바로 알 수 있게 모든 기본 문자에 같은 인사를 쓴다.
_DEVICE_HEALTH_MONITOR_SMS_GREETING = "안녕하세요 마미톡입니다. 🌷"
_DEVICE_HEALTH_MONITOR_SMS_AUTO_SENT_TEXT = "문자 자동발송 완료"
_DEVICE_HEALTH_MONITOR_SMS_AUTO_FAILED_TEXT = "문자 자동발송 실패 - 수동 발송 가능"


def _device_health_monitor_state_path() -> Path:
    return Path(cs.DEVICE_HEALTH_MONITOR_STATE_PATH).expanduser()


def _device_health_monitor_event_log_dir() -> Path:
    return Path(cs.DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR).expanduser()


def _device_health_monitor_event_log_path(now: datetime) -> Path:
    local_now = _coerce_daily_device_round_now(now)
    return _device_health_monitor_event_log_dir() / (
        f"device_health_monitor_events-{local_now.date().isoformat()}.jsonl"
    )


def _append_device_health_monitor_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> None:
    local_now = _coerce_daily_device_round_now(now)
    event_payload = {
        "eventType": _display_value(event_type, default="unknown"),
        "createdAt": local_now.isoformat(),
        **(payload if isinstance(payload, dict) else {}),
    }
    path = _device_health_monitor_event_log_path(local_now)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as event_log:
            event_log.write(
                json.dumps(event_payload, ensure_ascii=True, sort_keys=True, default=str)
                + "\n"
            )
    except Exception:
        if logger is not None:
            logger.warning("장비 상태 모니터 이벤트 로그를 저장하지 못했어: %s", path, exc_info=True)


def _load_device_health_monitor_runtime_state() -> dict[str, Any]:
    with _DEVICE_HEALTH_MONITOR_RUNTIME_STATE_LOCK:
        return dict(_DEVICE_HEALTH_MONITOR_RUNTIME_STATE)


def _remember_device_health_monitor_runtime_state(state: dict[str, Any]) -> dict[str, Any]:
    normalized_state = _normalize_device_health_monitor_state(state)
    with _DEVICE_HEALTH_MONITOR_RUNTIME_STATE_LOCK:
        _DEVICE_HEALTH_MONITOR_RUNTIME_STATE.clear()
        _DEVICE_HEALTH_MONITOR_RUNTIME_STATE.update(normalized_state)
    return normalized_state


def _load_device_health_monitor_state(
    state_path: Path | None = None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    path = state_path or _device_health_monitor_state_path()
    runtime_state = _load_device_health_monitor_runtime_state()
    if not path.exists():
        return runtime_state
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if logger is not None:
            logger.warning("장비 상태 모니터 상태 파일을 읽지 못했어: %s", path, exc_info=True)
        return runtime_state

    state = data if isinstance(data, dict) else {}
    if runtime_state:
        merged_state = dict(state)
        merged_state.update(runtime_state)
        return merged_state
    return state


def _save_device_health_monitor_state(
    state: dict[str, Any],
    state_path: Path | None = None,
) -> None:
    path = state_path or _device_health_monitor_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _persist_device_health_monitor_state_best_effort(
    state: dict[str, Any],
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    normalized_state = _remember_device_health_monitor_runtime_state(state)
    try:
        _save_device_health_monitor_state(normalized_state)
    except Exception:
        if logger is not None:
            logger.warning("장비 상태 모니터 상태를 저장하지 못했어", exc_info=True)
    return normalized_state


def _normalize_device_health_monitor_alerts(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}
    alerts: dict[str, dict[str, Any]] = {}
    for key, raw in value.items():
        if not isinstance(raw, dict):
            continue
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        alerts[normalized_key] = {
            "firstAlertedAt": str(raw.get("firstAlertedAt") or "").strip(),
            "lastAlertedAt": str(raw.get("lastAlertedAt") or "").strip(),
            "lastSeenAt": str(raw.get("lastSeenAt") or "").strip(),
            "count": max(0, int(raw.get("count") or 0)),
        }
    return alerts


def _normalize_device_health_monitor_pending_alerts(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}

    alerts: dict[str, dict[str, Any]] = {}
    for key, raw in value.items():
        if not isinstance(raw, dict):
            continue
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        alerts[normalized_key] = {
            "firstSeenAt": str(raw.get("firstSeenAt") or "").strip(),
            "lastSeenAt": str(raw.get("lastSeenAt") or "").strip(),
            "count": max(0, int(raw.get("count") or 0)),
        }
    return alerts


def _normalize_device_health_monitor_ssh_tunnel_records(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}

    records: dict[str, dict[str, Any]] = {}
    for key, raw in value.items():
        if not isinstance(raw, dict):
            continue
        device_name = str(key or "").strip()
        if not device_name:
            continue
        records[device_name] = {
            "openedAt": str(raw.get("openedAt") or "").strip(),
            "closedAt": str(raw.get("closedAt") or "").strip(),
            "host": str(raw.get("host") or "").strip(),
            "port": max(0, int(_coerce_int(raw.get("port")) or 0)),
            "closeStatus": str(raw.get("closeStatus") or "").strip(),
            "closeError": str(raw.get("closeError") or "").strip(),
            "count": max(0, int(raw.get("count") or 0)),
        }
    return records


def _normalize_device_health_monitor_device_candidate_cache(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []

    items: list[dict[str, Any]] = []
    seen_device_names: set[str] = set()
    for raw in value:
        if not isinstance(raw, dict):
            continue
        device_name = _display_value(raw.get("deviceName"), default="")
        if not device_name or device_name in seen_device_names:
            continue
        # 숫자_ 접두 병원은 출고/작업 상태용 가상 병원이므로 캐시에서도 감시 대상에서 제외한다.
        hospital_name = _display_value(raw.get("hospitalName"), default="미확인")
        if _is_device_health_monitor_excluded_hospital_name(hospital_name):
            continue
        seen_device_names.add(device_name)
        items.append(
            {
                "deviceSeq": _coerce_int(raw.get("deviceSeq")),
                "deviceName": device_name,
                "hospitalSeq": _coerce_int(raw.get("hospitalSeq")),
                "hospitalRoomSeq": _coerce_int(raw.get("hospitalRoomSeq")),
                "hospitalName": hospital_name,
                "hospitalTelephone": _display_value(raw.get("hospitalTelephone"), default=""),
                "hospitalDeviceAlertPhone": _display_value(
                    raw.get("hospitalDeviceAlertPhone"),
                    default="",
                ),
                "roomName": _display_value(raw.get("roomName"), default="미확인"),
            }
        )
    return items


def _is_device_health_monitor_excluded_hospital_name(value: Any) -> bool:
    hospital_name = _display_value(value, default="").strip()
    return bool(_DEVICE_HEALTH_MONITOR_EXCLUDED_HOSPITAL_NAME_PATTERN.match(hospital_name))


def _normalize_device_health_monitor_action_cooldowns(value: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(value, dict):
        return {}

    cooldowns: dict[str, dict[str, Any]] = {}
    for key, raw in value.items():
        if not isinstance(raw, dict):
            continue
        normalized_key = str(key or "").strip()
        if not normalized_key:
            continue
        cooldowns[normalized_key] = {
            "lastTriggeredAt": str(raw.get("lastTriggeredAt") or "").strip(),
            "count": max(0, int(raw.get("count") or 0)),
        }
    return cooldowns


def _normalize_device_health_monitor_alert_delivery_override(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or "enabled" not in value:
        return None
    return {
        "enabled": bool(value.get("enabled")),
        "updatedAt": str(value.get("updatedAt") or "").strip(),
        "updatedBy": str(value.get("updatedBy") or "").strip(),
    }


def _normalize_device_health_monitor_state(state: dict[str, Any]) -> dict[str, Any]:
    state_payload = state if isinstance(state, dict) else {}
    normalized_state = dict(state_payload)
    normalized_state["lastHospitalSeq"] = _coerce_int(state_payload.get("lastHospitalSeq"))
    normalized_state["nextHospitalSeq"] = _coerce_int(state_payload.get("nextHospitalSeq"))
    normalized_state["processedHospitalSeqs"] = _coerce_daily_device_round_hospital_seqs(
        state_payload.get("processedHospitalSeqs")
    )
    normalized_state["alertFingerprints"] = _normalize_device_health_monitor_alerts(
        state_payload.get("alertFingerprints")
    )
    normalized_state["pendingAlertFingerprints"] = _normalize_device_health_monitor_pending_alerts(
        state_payload.get("pendingAlertFingerprints")
    )
    normalized_state["sshTunnelRecords"] = _normalize_device_health_monitor_ssh_tunnel_records(
        state_payload.get("sshTunnelRecords")
    )
    normalized_state["deviceCandidateCache"] = _normalize_device_health_monitor_device_candidate_cache(
        state_payload.get("deviceCandidateCache")
    )
    normalized_state["deviceCandidateCachedAt"] = str(state_payload.get("deviceCandidateCachedAt") or "").strip()
    if "alertActionCooldowns" in state_payload:
        normalized_state["alertActionCooldowns"] = _normalize_device_health_monitor_action_cooldowns(
            state_payload.get("alertActionCooldowns")
        )
    alert_delivery_override = _normalize_device_health_monitor_alert_delivery_override(
        state_payload.get("alertDeliveryOverride")
    )
    if alert_delivery_override is None:
        normalized_state.pop("alertDeliveryOverride", None)
    else:
        normalized_state["alertDeliveryOverride"] = alert_delivery_override
    return normalized_state


def _resolve_device_health_monitor_alert_delivery_status(
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_state = (
        _normalize_device_health_monitor_state(state)
        if isinstance(state, dict)
        else _normalize_device_health_monitor_state(_load_device_health_monitor_state())
    )
    override = _normalize_device_health_monitor_alert_delivery_override(
        normalized_state.get("alertDeliveryOverride")
    )
    env_default = bool(cs.DEVICE_HEALTH_MONITOR_ALERTS_ENABLED)
    enabled = bool(override["enabled"]) if override is not None else env_default
    return {
        "enabled": enabled,
        "label": "켜짐" if enabled else "꺼짐",
        "envDefault": env_default,
        "source": "slack_override" if override is not None else "env",
        "updatedAt": _display_value((override or {}).get("updatedAt"), default=""),
        "updatedBy": _display_value((override or {}).get("updatedBy"), default=""),
        "monitorEnabled": bool(cs.DEVICE_HEALTH_MONITOR_ENABLED),
    }


def _set_device_health_monitor_alert_delivery_enabled(
    enabled: bool,
    *,
    user_id: str | None = None,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    local_now = _coerce_daily_device_round_now(now)
    state = _normalize_device_health_monitor_state(_load_device_health_monitor_state(logger=logger))
    state["alertDeliveryOverride"] = {
        "enabled": bool(enabled),
        "updatedAt": local_now.isoformat(),
        "updatedBy": _display_value(user_id, default=""),
    }
    # Slack 명령으로 바꾼 발송 설정은 재시작 뒤에도 유지되게 상태 파일에 저장한다.
    persisted_state = _persist_device_health_monitor_state_best_effort(state, logger=logger)
    _append_device_health_monitor_event(
        "alert_delivery_control_changed",
        {
            "enabled": bool(enabled),
            "updatedBy": _display_value(user_id, default=""),
            "source": "slack_mention",
        },
        now=local_now,
        logger=logger,
    )
    return _resolve_device_health_monitor_alert_delivery_status(persisted_state)


def _is_device_health_monitor_runtime_configured() -> bool:
    return bool(cs.DEVICE_STATE_REDIS_HOST)


def _is_device_health_monitor_ssh_verification_configured() -> bool:
    return bool(cs.MDA_GRAPHQL_URL and cs.MDA_ADMIN_USER_PASSWORD and cs.DEVICE_SSH_PASSWORD)


def _device_health_monitor_channel_id() -> str:
    return str(cs.DEVICE_HEALTH_MONITOR_CHANNEL_ID or cs.DAILY_DEVICE_ROUND_CHANNEL_ID or "").strip()


def _parse_device_health_monitor_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return _coerce_daily_device_round_now(datetime.fromisoformat(text))
    except ValueError:
        return None


def _device_health_monitor_alert_reminder_delta() -> timedelta:
    hours = max(1, int(cs.DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS))
    return timedelta(hours=hours)


def _build_device_health_monitor_alert_fingerprint(item: dict[str, str]) -> str:
    return "|".join(
        [
            _display_value(item.get("hospital"), default=""),
            _display_value(item.get("room"), default=""),
            _display_value(item.get("device"), default=""),
            _display_value(item.get("issue"), default=""),
        ]
    )


def _device_health_monitor_required_confirmation_polls(item: dict[str, Any]) -> int:
    # USB 장치 계열은 순간 재인식/플래핑이 잦아서 두 번 연속 감지될 때만 신규 알림을 보낸다.
    issue = _display_value(item.get("issue"), default="")
    problem_components = (
        item.get("problemComponents") if isinstance(item.get("problemComponents"), list) else []
    )
    component_labels = {
        _display_value(component, default="")
        for component in problem_components
        if _display_value(component, default="")
    }
    if {"캡처보드", "LED"} & component_labels:
        return 2

    lowered_issue = issue.lower()
    if (
        "캡처보드" in issue
        or "비디오 장치" in issue
        or "led" in lowered_issue
        or "엘이디" in issue
    ):
        return 2
    return 1


def _normalize_device_health_monitor_alert_action_item(value: Any) -> dict[str, str]:
    raw_item = value
    if isinstance(value, str):
        try:
            raw_item = json.loads(value)
        except json.JSONDecodeError:
            raw_item = {}
    item = raw_item if isinstance(raw_item, dict) else {}
    return {
        "hospitalSeq": _display_value(item.get("hospitalSeq"), default=""),
        "hospitalName": _display_value(item.get("hospitalName"), default=""),
        "hospital": _display_value(item.get("hospital"), default="병원 미확인"),
        "telephone": _display_value(item.get("telephone"), default=""),
        "deviceAlertPhone": _display_value(item.get("deviceAlertPhone"), default=""),
        "smsStatusText": _display_value(item.get("smsStatusText"), default=""),
        "smsPhoneNumber": _display_value(item.get("smsPhoneNumber"), default=""),
        "smsMessage": _display_value(item.get("smsMessage"), default=""),
        "smsTemplateId": _display_value(item.get("smsTemplateId"), default=""),
        "smsModalMode": _display_value(item.get("smsModalMode"), default=""),
        "room": _display_value(item.get("room"), default="병실 미확인"),
        "device": _display_value(item.get("device"), default="장비명 미확인"),
        "issue": _display_value(item.get("issue"), default="상세 확인 필요"),
        "mdaUrl": _display_value(item.get("mdaUrl"), default=""),
    }


def _is_device_health_monitor_auto_sent_action_item(item: dict[str, str]) -> bool:
    # 이전 알림 payload에 smsModalMode가 없어도 자동발송 완료 버튼은 재발송 모달로 열지 않는다.
    return (
        _display_value(item.get("smsModalMode"), default="")
        == _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT
        or _display_value(item.get("smsStatusText"), default="")
        == _DEVICE_HEALTH_MONITOR_SMS_AUTO_SENT_TEXT
    )


def _format_device_health_monitor_action_target(item: dict[str, str]) -> str:
    return " / ".join(
        [
            _display_value(item.get("hospital"), default="병원 미확인"),
            _display_value(item.get("room"), default="병실 미확인"),
            _display_value(item.get("device"), default="장비명 미확인"),
        ]
    )


def _extract_device_health_monitor_hospital_seq(item: dict[str, str]) -> int | None:
    explicit_seq = _coerce_int(item.get("hospitalSeq"))
    if explicit_seq is not None:
        return explicit_seq
    matched = _DEVICE_HEALTH_MONITOR_HOSPITAL_SEQ_PATTERN.search(
        _display_value(item.get("hospital"), default="")
    )
    if not matched:
        return None
    return _coerce_int(matched.group(1))


def _normalize_device_health_monitor_phone_number(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if text.startswith("+"):
        return "+" + re.sub(r"\D", "", text[1:])
    return re.sub(r"\D", "", text)


def _device_health_monitor_korean_national_phone_number(value: Any) -> str:
    phone_number = _normalize_device_health_monitor_phone_number(value)
    if phone_number.startswith("+82"):
        return "0" + phone_number[3:]
    if phone_number.startswith("82"):
        return "0" + phone_number[2:]
    return phone_number


def _is_device_health_monitor_mobile_phone_number(value: Any) -> bool:
    # 병원 대표번호/지역번호는 SMS 발송 대상이 아니므로 국내 휴대전화 형식만 허용한다.
    phone_number = _device_health_monitor_korean_national_phone_number(value)
    if not phone_number.isdigit():
        return False
    if phone_number.startswith("010"):
        return len(phone_number) == 11
    if phone_number[:3] in {"011", "016", "017", "018", "019"}:
        return len(phone_number) in {10, 11}
    return False


def _device_health_monitor_sms_target_phone_number(contact: dict[str, str]) -> tuple[str, bool]:
    test_phone_number = _normalize_device_health_monitor_phone_number(
        cs.DEVICE_HEALTH_MONITOR_SMS_TEST_PHONE_NUMBER
    )
    if test_phone_number:
        return test_phone_number, True
    return _display_value(contact.get("phoneNumber"), default=""), False


def _device_health_monitor_default_modal_phone_number(contact: dict[str, str]) -> str:
    # 모달 기본값은 테스트 번호가 아니라 병원 이상 알림 전용 번호가 휴대전화일 때만 채운다.
    phone_number = _display_value(contact.get("phoneNumber"), default="")
    if not _is_device_health_monitor_mobile_phone_number(phone_number):
        return ""
    return _device_health_monitor_korean_national_phone_number(phone_number)


def _lookup_device_health_monitor_hospital_contact(
    hospital_seq: int | None,
) -> dict[str, str]:
    if hospital_seq is None:
        return {
            "status": "missing_hospital_seq",
            "hospitalSeq": "",
            "hospitalName": "",
            "telephone": "",
            "deviceAlertPhone": "",
            "phoneNumber": "",
        }

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT seq, hospitalName, telephone, deviceAlertPhone FROM hospitals WHERE seq = %s LIMIT 1",
                (int(hospital_seq),),
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()

    if not row:
        return {
            "status": "hospital_not_found",
            "hospitalSeq": str(hospital_seq),
            "hospitalName": "",
            "telephone": "",
            "deviceAlertPhone": "",
            "phoneNumber": "",
        }

    telephone = _display_value(row.get("telephone"), default="")
    device_alert_phone = _display_value(row.get("deviceAlertPhone"), default="")
    phone_number = _normalize_device_health_monitor_phone_number(device_alert_phone)
    return {
        "status": "ok" if phone_number else "missing_telephone",
        "hospitalSeq": _display_value(row.get("seq"), default=str(hospital_seq)),
        "hospitalName": _display_value(row.get("hospitalName"), default=""),
        "telephone": telephone,
        "deviceAlertPhone": device_alert_phone,
        "phoneNumber": phone_number,
    }


def _build_device_health_monitor_sms_guide(item: dict[str, str]) -> dict[str, Any]:
    issue = _display_value(item.get("issue"), default="")
    room = _display_value(item.get("room"), default="진료실")
    device = _display_value(item.get("device"), default="마미박스 장비")
    lowered = issue.lower()

    if "캡처보드" in issue or "비디오 장치" in issue or "영상" in issue:
        message = (
            f"{_DEVICE_HEALTH_MONITOR_SMS_GREETING}\n\n"
            f"{room} {device}에서 초음파 영상 입력 장치 연결 확인이 필요합니다.\n\n"
            "초음파 진단기와 캡처보드, 캡처보드와 마미박스가 각각 HDMI 케이블로 연결되어 있습니다.\n"
            "먼저 두 HDMI 케이블을 분리했다가 다시 단단히 연결해 주세요.\n"
            "케이블이 빠져 있거나 헐거우면 영상이 정상적으로 들어오지 않을 수 있습니다."
        )
        return {
            "supported": True,
            "templateId": "captureboard_disconnected",
            "title": "캡처보드 연결 확인 문자",
            "message": message,
        }

    if "led" in lowered or "엘이디" in issue:
        message = (
            f"{_DEVICE_HEALTH_MONITOR_SMS_GREETING}\n\n"
            f"{room} {device}에서 장비 상태 표시등 연결 확인이 필요합니다.\n\n"
            "먼저 LED USB 케이블을 분리했다가 다시 단단히 연결해 주세요.\n"
            "케이블이 빠져 있거나 헐거우면 상태 표시등이 정상적으로 동작하지 않을 수 있습니다."
        )
        return {
            "supported": True,
            "templateId": "led_disconnected",
            "title": "LED 연결 확인 문자",
            "message": message,
        }

    if any(token in lowered for token in ("audio", "sound", "speaker")) or any(
        token in issue for token in ("오디오", "소리", "스피커")
    ):
        message = (
            f"{_DEVICE_HEALTH_MONITOR_SMS_GREETING}\n\n"
            f"{room} {device}에서 소리 출력 상태 확인이 필요합니다.\n\n"
            "먼저 스피커 전원과 오디오 케이블을 분리했다가 다시 단단히 연결해 주세요.\n"
            "케이블이 빠져 있거나 입력 소스가 맞지 않으면 소리가 나오지 않을 수 있습니다."
        )
        return {
            "supported": True,
            "templateId": "audio_output_check",
            "title": "오디오 연결 확인 문자",
            "message": message,
        }

    return {
        "supported": False,
        "templateId": "unsupported_issue",
        "title": "병원 연락 제외 이슈",
        "message": "",
        "reason": "hospital_sms_not_supported_for_issue",
    }


def _device_health_monitor_action_cooldown_key(action_id: str, item: dict[str, str]) -> str:
    return "|".join(
        [
            _display_value(action_id, default="unknown"),
            _display_value(item.get("hospital"), default=""),
            _display_value(item.get("room"), default=""),
            _display_value(item.get("device"), default=""),
            _display_value(item.get("issue"), default=""),
        ]
    )


def _device_health_monitor_voice_guide_cooldown_delta() -> timedelta:
    return timedelta(seconds=max(0, int(cs.DEVICE_HEALTH_MONITOR_VOICE_GUIDE_COOLDOWN_SEC)))


def _check_device_health_monitor_action_cooldown(
    state: dict[str, Any],
    *,
    action_id: str,
    item: dict[str, str],
    now: datetime,
) -> dict[str, Any]:
    if action_id != _DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE:
        return {"active": False, "remainingSeconds": 0}

    cooldown_delta = _device_health_monitor_voice_guide_cooldown_delta()
    if cooldown_delta.total_seconds() <= 0:
        return {"active": False, "remainingSeconds": 0}

    cooldowns = _normalize_device_health_monitor_action_cooldowns(state.get("alertActionCooldowns"))
    cooldown_key = _device_health_monitor_action_cooldown_key(action_id, item)
    last_triggered_at = _parse_device_health_monitor_datetime(
        (cooldowns.get(cooldown_key) or {}).get("lastTriggeredAt")
    )
    if last_triggered_at is None:
        return {"active": False, "remainingSeconds": 0}

    elapsed = now - last_triggered_at
    if elapsed >= cooldown_delta:
        return {"active": False, "remainingSeconds": 0}
    remaining_seconds = int(max(1, (cooldown_delta - elapsed).total_seconds()))
    return {"active": True, "remainingSeconds": remaining_seconds, "lastTriggeredAt": last_triggered_at.isoformat()}


def _remember_device_health_monitor_action_cooldown(
    state: dict[str, Any],
    *,
    action_id: str,
    item: dict[str, str],
    now: datetime,
) -> dict[str, Any]:
    cooldowns = _normalize_device_health_monitor_action_cooldowns(state.get("alertActionCooldowns"))
    cooldown_key = _device_health_monitor_action_cooldown_key(action_id, item)
    previous = cooldowns.get(cooldown_key) or {}
    cooldowns[cooldown_key] = {
        "lastTriggeredAt": now.isoformat(),
        "count": max(0, int(previous.get("count") or 0)) + 1,
    }
    return {**state, "alertActionCooldowns": cooldowns}


def _device_health_monitor_action_webhook_url(action_id: str) -> str:
    setting_name = _DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_URL_SETTINGS.get(action_id)
    if not setting_name:
        return ""
    return str(getattr(cs, setting_name, "") or "").strip()


def _build_solapi_authorization_header() -> str:
    date_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    salt = secrets.token_hex(16)
    signature = hmac.new(
        cs.SOLAPI_API_SECRET.encode("utf-8"),
        f"{date_time}{salt}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    return f"HMAC-SHA256 apiKey={cs.SOLAPI_API_KEY}, date={date_time}, salt={salt}, signature={signature}"


def _device_health_monitor_sms_type(message: str) -> str:
    return "SMS" if len(str(message or "").encode("euc-kr", errors="replace")) <= 90 else "LMS"


def _post_device_health_monitor_solapi_sms(
    payload: dict[str, Any],
    *,
    logger: logging.Logger,
) -> dict[str, Any]:
    if not cs.SOLAPI_API_KEY or not cs.SOLAPI_API_SECRET or not cs.SOLAPI_FROM_NUMBER:
        return {
            "status": "not_configured",
            "ok": False,
            "missingSetting": "SOLAPI_API_KEY/SOLAPI_API_SECRET/SOLAPI_FROM_NUMBER",
        }

    sms_payload = payload.get("sms") if isinstance(payload.get("sms"), dict) else {}
    to_number = _normalize_device_health_monitor_phone_number(sms_payload.get("to"))
    from_number = _normalize_device_health_monitor_phone_number(cs.SOLAPI_FROM_NUMBER)
    message = _display_value(sms_payload.get("message"), default="")
    if not to_number or not from_number or not message:
        return {"status": "invalid_sms_payload", "ok": False}

    request_payload = {
        "messages": [
            {
                "to": to_number,
                "from": from_number,
                "text": message,
                "type": _device_health_monitor_sms_type(message),
                "country": "82",
            }
        ]
    }
    url = f"{cs.SOLAPI_BASE_URL.rstrip('/')}/messages/v4/send-many/detail"
    try:
        response = requests.post(
            url,
            json=request_payload,
            headers={
                "Authorization": _build_solapi_authorization_header(),
                "Content-Type": "application/json",
            },
            timeout=max(1, int(cs.DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC)),
        )
    except Exception as exc:
        logger.warning("Solapi SMS 발송 요청 실패", exc_info=True)
        return {"status": "error", "ok": False, "provider": "solapi", "error": type(exc).__name__}

    response_text = _display_value(response.text, default="")
    try:
        response_payload = response.json() if response_text else {}
    except ValueError:
        response_payload = {}

    if 200 <= int(response.status_code) < 300:
        return {
            "status": "sent",
            "ok": True,
            "provider": "solapi",
            "statusCode": int(response.status_code),
            "groupId": _display_value(response_payload.get("groupId"), default=""),
        }
    return {
        "status": "error",
        "ok": False,
        "provider": "solapi",
        "statusCode": int(response.status_code),
        "error": response_text[:300],
    }


def _post_device_health_monitor_sms_payload(
    payload: dict[str, Any],
    *,
    logger: logging.Logger,
) -> dict[str, Any]:
    provider = str(cs.DEVICE_HEALTH_MONITOR_SMS_PROVIDER or "").strip().lower()
    if provider == "solapi":
        return _post_device_health_monitor_solapi_sms(payload, logger=logger)
    if provider in {"webhook", "http"}:
        webhook_url = _device_health_monitor_action_webhook_url(
            _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL
        )
        if not webhook_url:
            return {
                "status": "not_configured",
                "ok": False,
                "missingSetting": "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL",
            }
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=max(1, int(cs.DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC)),
            )
        except Exception as exc:
            logger.warning("장비 이상 알림 SMS webhook 호출 실패", exc_info=True)
            return {"status": "error", "ok": False, "provider": "webhook", "error": type(exc).__name__}
        if 200 <= int(response.status_code) < 300:
            return {"status": "sent", "ok": True, "provider": "webhook", "statusCode": int(response.status_code)}
        return {
            "status": "error",
            "ok": False,
            "provider": "webhook",
            "statusCode": int(response.status_code),
            "error": _display_value(response.text, default="")[:300],
        }
    return {
        "status": "not_configured",
        "ok": False,
        "missingSetting": "DEVICE_HEALTH_MONITOR_SMS_PROVIDER",
    }


def _build_device_health_monitor_contact_webhook_payload(
    *,
    action_id: str,
    item: dict[str, str],
    actor_user_id: str,
    channel_id: str,
    message_ts: str,
    now: datetime,
    sms_phone_number: str | None = None,
    sms_message: str | None = None,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    sms_guide = _build_device_health_monitor_sms_guide(item)
    manual_message = str(sms_message or "").strip()
    if not sms_guide.get("supported") and not manual_message:
        return None, {
            "status": "unsupported_issue",
            "ok": False,
            "templateId": _display_value(sms_guide.get("templateId"), default="unsupported_issue"),
            "reason": _display_value(
                sms_guide.get("reason"),
                default="hospital_sms_not_supported_for_issue",
            ),
        }

    hospital_seq = _extract_device_health_monitor_hospital_seq(item)
    try:
        contact = _lookup_device_health_monitor_hospital_contact(hospital_seq)
    except Exception:
        if not str(sms_phone_number or "").strip():
            raise
        # 수동으로 받을 번호를 입력한 제출은 DB 연락처 조회 실패가 실제 발송을 막지 않게 한다.
        contact = {}
    contact_status = _display_value(contact.get("status"), default="unknown")
    manual_phone_number = str(sms_phone_number or "").strip()
    if manual_phone_number:
        phone_number = _normalize_device_health_monitor_phone_number(manual_phone_number)
        test_mode = False
    else:
        phone_number, test_mode = _device_health_monitor_sms_target_phone_number(contact)
    if not phone_number:
        return None, {
            "status": "missing_telephone" if manual_phone_number else contact_status,
            "ok": False,
            "templateId": _display_value(sms_guide.get("templateId"), default=""),
            "hospitalSeq": _display_value(contact.get("hospitalSeq"), default=str(hospital_seq or "")),
        }
    if not manual_phone_number and contact_status != "ok" and not test_mode:
        return None, {
            "status": contact_status,
            "ok": False,
            "templateId": _display_value(sms_guide.get("templateId"), default=""),
            "hospitalSeq": _display_value(contact.get("hospitalSeq"), default=str(hospital_seq or "")),
        }
    if not test_mode:
        phone_number = _device_health_monitor_korean_national_phone_number(phone_number)
    if not test_mode and not _is_device_health_monitor_mobile_phone_number(phone_number):
        return None, {
            "status": "non_mobile_telephone",
            "ok": False,
            "templateId": _display_value(sms_guide.get("templateId"), default=""),
            "hospitalSeq": _display_value(contact.get("hospitalSeq"), default=str(hospital_seq or "")),
            "telephone": _display_value(contact.get("telephone"), default=phone_number),
            "deviceAlertPhone": _display_value(contact.get("deviceAlertPhone"), default=phone_number),
        }

    default_message = _display_value(sms_guide.get("message"), default="")
    message = manual_message or default_message
    if not message:
        return None, {
            "status": "missing_sms_message",
            "ok": False,
            "templateId": _display_value(sms_guide.get("templateId"), default="manual"),
            "hospitalSeq": _display_value(contact.get("hospitalSeq"), default=str(hospital_seq or "")),
        }
    template_id = _display_value(
        sms_guide.get("templateId"),
        default="manual",
    )
    if manual_message and manual_message != default_message:
        template_id = "manual"
    title = (
        _display_value(sms_guide.get("title"), default="")
        if template_id != "manual"
        else "직접 작성 문자"
    )

    hospital_name = _display_value(
        contact.get("hospitalName"),
        default=_display_value(item.get("hospitalName"), default=_display_value(item.get("hospital"), default="")),
    )
    payload = {
        "actionId": action_id,
        "actionLabel": _DEVICE_HEALTH_MONITOR_ACTION_LABELS.get(action_id, action_id),
        "requestType": "sms",
        "createdAt": now.isoformat(),
        "actorUserId": actor_user_id,
        "hospital": {
            "seq": _display_value(contact.get("hospitalSeq"), default=str(hospital_seq or "")),
            "name": hospital_name,
            "phoneNumber": phone_number,
        },
        "device": {
            "name": item["device"],
            "room": item["room"],
            "issue": item["issue"],
            "mdaUrl": item["mdaUrl"],
        },
        "sms": {
            "to": phone_number,
            "templateId": template_id,
            "title": title,
            "message": message,
            "testMode": test_mode,
        },
        "slack": {
            "channelId": channel_id,
            "messageTs": message_ts,
        },
    }
    return payload, {
        "status": "prepared",
        "ok": True,
        "templateId": template_id,
        "hospitalSeq": _display_value(contact.get("hospitalSeq"), default=str(hospital_seq or "")),
        "phoneLast4": phone_number[-4:],
        "testMode": test_mode,
    }


def _post_device_health_monitor_action_webhook(
    *,
    action_id: str,
    item: dict[str, str],
    actor_user_id: str,
    channel_id: str,
    message_ts: str,
    now: datetime,
    logger: logging.Logger,
    sms_phone_number: str | None = None,
    sms_message: str | None = None,
) -> dict[str, Any]:
    if action_id == _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL:
        try:
            payload, prepared_result = _build_device_health_monitor_contact_webhook_payload(
                action_id=action_id,
                item=item,
                actor_user_id=actor_user_id,
                channel_id=channel_id,
                message_ts=message_ts,
                now=now,
                sms_phone_number=sms_phone_number,
                sms_message=sms_message,
            )
        except Exception as exc:
            logger.warning("장비 이상 알림 병원 연락 payload 생성 실패", exc_info=True)
            return {"status": "error", "ok": False, "error": type(exc).__name__}
        if payload is None:
            return prepared_result
        send_result = _post_device_health_monitor_sms_payload(payload, logger=logger)
        return {**prepared_result, **send_result}
    else:
        webhook_url = _device_health_monitor_action_webhook_url(action_id)
        if not webhook_url:
            return {"status": "not_configured", "ok": False}
        action_label = _DEVICE_HEALTH_MONITOR_ACTION_LABELS.get(action_id, action_id)
        payload = {
            "actionId": action_id,
            "actionLabel": action_label,
            "createdAt": now.isoformat(),
            "actorUserId": actor_user_id,
            "hospital": item["hospital"],
            "room": item["room"],
            "device": item["device"],
            "issue": item["issue"],
            "mdaUrl": item["mdaUrl"],
            "slack": {
                "channelId": channel_id,
                "messageTs": message_ts,
            },
        }
        prepared_result = {"status": "prepared", "ok": True}
        try:
            response = requests.post(
                webhook_url,
                json=payload,
                timeout=max(1, int(cs.DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC)),
            )
        except Exception as exc:
            logger.warning("장비 이상 알림 action webhook 호출 실패 action=%s", action_id, exc_info=True)
            return {"status": "error", "ok": False, "error": type(exc).__name__}

        if 200 <= int(response.status_code) < 300:
            return {**prepared_result, "status": "sent", "ok": True, "statusCode": int(response.status_code)}
        return {
            **prepared_result,
            "status": "error",
            "ok": False,
            "statusCode": int(response.status_code),
            "error": _display_value(response.text, default="")[:300],
        }


def _device_health_monitor_auto_sms_phone_number(item: dict[str, str]) -> str:
    # 자동발송은 이상 알림 전용 휴대전화번호만 사용하고 대표번호나 테스트 번호 fallback은 쓰지 않는다.
    phone_number = _device_health_monitor_korean_national_phone_number(item.get("deviceAlertPhone"))
    if not _is_device_health_monitor_mobile_phone_number(phone_number):
        return ""
    return phone_number


def _send_device_health_monitor_auto_sms_for_item(
    item: dict[str, str],
    *,
    channel_id: str,
    now: datetime,
    logger: logging.Logger,
) -> dict[str, Any]:
    phone_number = _device_health_monitor_auto_sms_phone_number(item)
    if not phone_number:
        return {"status": "manual_required", "smsContactActionEnabled": True}

    sms_guide = _build_device_health_monitor_sms_guide(item)
    if not sms_guide.get("supported") or not _display_value(sms_guide.get("message"), default=""):
        return {"status": "unsupported_issue", "smsContactActionEnabled": True}

    try:
        payload, prepared_result = _build_device_health_monitor_contact_webhook_payload(
            action_id=_DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
            item=item,
            actor_user_id="",
            channel_id=channel_id,
            message_ts="",
            now=now,
            sms_phone_number=phone_number,
        )
    except Exception as exc:
        logger.warning("장비 이상 알림 문자 자동발송 payload 생성 실패", exc_info=True)
        return {
            "status": "error",
            "ok": False,
            "error": type(exc).__name__,
            "smsStatusText": _DEVICE_HEALTH_MONITOR_SMS_AUTO_FAILED_TEXT,
            "smsContactActionEnabled": True,
        }

    if payload is None:
        return {**prepared_result, "smsContactActionEnabled": True}

    send_result = _post_device_health_monitor_sms_payload(payload, logger=logger)
    result = {**prepared_result, **send_result}
    sent = bool(result.get("ok")) and _display_value(result.get("status"), default="") == "sent"
    status_text = (
        _DEVICE_HEALTH_MONITOR_SMS_AUTO_SENT_TEXT
        if sent
        else _DEVICE_HEALTH_MONITOR_SMS_AUTO_FAILED_TEXT
    )
    _append_device_health_monitor_event(
        "alert_sms_auto_sent" if sent else "alert_sms_auto_failed",
        {
            "actionId": _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
            "channelId": channel_id,
            "hospital": item["hospital"],
            "room": item["room"],
            "device": item["device"],
            "issue": item["issue"],
            "templateId": _display_value(result.get("templateId"), default=""),
            "provider": _display_value(result.get("provider"), default=""),
            "status": _display_value(result.get("status"), default=""),
            "phoneLast4": _display_value(result.get("phoneLast4"), default=""),
        },
        now=now,
        logger=logger,
    )
    return {
        **result,
        "smsStatusText": status_text,
        "smsContactActionEnabled": not sent,
        # 자동발송 완료 버튼에서 실제 발송 대상과 본문을 확인할 수 있게 Slack action payload로 넘긴다.
        "smsPhoneNumber": _display_value(payload.get("sms", {}).get("to"), default=""),
        "smsMessage": _display_value(payload.get("sms", {}).get("message"), default=""),
        "smsTemplateId": _display_value(payload.get("sms", {}).get("templateId"), default=""),
    }


def _apply_device_health_monitor_auto_sms(
    alert_summary: dict[str, Any],
    *,
    channel_id: str,
    now: datetime,
    logger: logging.Logger,
) -> dict[str, Any]:
    device_results = (
        alert_summary.get("deviceResults")
        if isinstance(alert_summary.get("deviceResults"), list)
        else []
    )
    next_device_results: list[Any] = []
    for device_result in device_results:
        if not isinstance(device_result, dict):
            next_device_results.append(device_result)
            continue
        next_device_result = dict(device_result)
        if _display_value(device_result.get("overallLabel"), default="") == "이상":
            single_item_summary = {**alert_summary, "deviceResults": [device_result]}
            alert_items = _collect_daily_device_round_abnormal_alert_items(single_item_summary)
            if alert_items:
                auto_result = _send_device_health_monitor_auto_sms_for_item(
                    alert_items[0],
                    channel_id=channel_id,
                    now=now,
                    logger=logger,
                )
                sms_status_text = _display_value(auto_result.get("smsStatusText"), default="")
                if sms_status_text:
                    next_device_result["smsStatusText"] = sms_status_text
                    next_device_result["smsContactActionEnabled"] = (
                        "true" if auto_result.get("smsContactActionEnabled", True) else "false"
                    )
                    for key in ("smsPhoneNumber", "smsMessage", "smsTemplateId"):
                        value = _display_value(auto_result.get(key), default="")
                        if value:
                            next_device_result[key] = value
        next_device_results.append(next_device_result)
    return {**alert_summary, "deviceResults": next_device_results}


def _build_device_health_monitor_action_reply(
    *,
    action_id: str,
    item: dict[str, str],
    actor_user_id: str,
    result: dict[str, Any],
) -> str:
    action_label = _DEVICE_HEALTH_MONITOR_ACTION_LABELS.get(action_id, action_id)
    target = _format_device_health_monitor_action_target(item)
    user = f"<@{actor_user_id}>" if actor_user_id else "사용자"
    status = _display_value(result.get("status"), default="")

    if status == "recorded":
        return f":white_check_mark: {user} {action_label} 처리했어. `{target}`"
    if status == "cooldown":
        remaining_seconds = max(1, int(result.get("remainingSeconds") or 0))
        remaining_minutes = max(1, (remaining_seconds + 59) // 60)
        return f":hourglass_flowing_sand: {user} 최근에 장비 음성 안내를 보냈어. `{target}`은 약 {remaining_minutes}분 뒤 다시 가능해."
    if status == "not_implemented":
        return f":construction: {user} {action_label}은 아직 실행하지 않았어. 마미박스 장비 코드 추가 후 연결해야 해. `{target}`"
    if status == "sent":
        if action_id == _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL:
            template_id = _display_value(result.get("templateId"), default="")
            return f":white_check_mark: {user} 병원 문자 발송 요청을 보냈어. `{target}` `{template_id}`"
        return f":white_check_mark: {user} {action_label} 요청을 보냈어. `{target}`"
    if status == "unsupported_issue":
        return f":no_entry: {user} 이 이슈는 병원 문자 발송 대상이 아니야. 내부 확인으로 처리해줘. `{target}`"
    if status == "missing_telephone":
        return f":warning: {user} 마미박스 이상 알림 전용 연락 번호가 없어 문자를 보낼 수 없어. hospitals.deviceAlertPhone을 확인해줘. `{target}`"
    if status == "non_mobile_telephone":
        return f":warning: {user} 마미박스 이상 알림 전용 연락 번호가 휴대전화번호가 아니라 문자를 보낼 수 없어. `{target}`"
    if status == "missing_sms_message":
        return f":warning: {user} 문자 내용이 비어 있어서 보낼 수 없어. `{target}`"
    if status in {"missing_hospital_seq", "hospital_not_found"}:
        return f":warning: {user} 병원 정보를 찾지 못해서 문자를 보낼 수 없어. `{target}`"
    if status == "not_configured":
        setting_name = _display_value(
            result.get("missingSetting"),
            default=_DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_URL_SETTINGS.get(action_id, "webhook"),
        )
        return f":warning: {user} {action_label} 버튼은 눌렸지만 `{setting_name}` 설정이 없어. 수동 처리해줘. `{target}`"
    return f":warning: {user} {action_label} 요청이 실패했어. 수동 처리해줘. `{target}`"


def _post_device_health_monitor_action_reply(
    client: Any,
    *,
    channel_id: str,
    thread_ts: str,
    text: str,
    logger: logging.Logger,
) -> None:
    normalized_channel_id = str(channel_id or "").strip()
    if not normalized_channel_id:
        return
    try:
        message_kwargs: dict[str, Any] = {
            "channel": normalized_channel_id,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        normalized_thread_ts = str(thread_ts or "").strip()
        if normalized_thread_ts:
            message_kwargs["thread_ts"] = normalized_thread_ts
        client.chat_postMessage(**message_kwargs)
    except Exception:
        logger.warning("장비 이상 알림 action 응답을 Slack에 남기지 못했어", exc_info=True)


def _build_device_health_monitor_sms_modal_view(
    *,
    item: dict[str, str],
    actor_user_id: str,
    channel_id: str,
    message_ts: str,
    thread_ts: str,
    mode: str = _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_SEND,
) -> dict[str, Any]:
    normalized_mode = _display_value(mode, default=_DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_SEND)
    is_view_auto_sent = normalized_mode == _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT
    sms_guide = _build_device_health_monitor_sms_guide(item)
    hospital_seq = _extract_device_health_monitor_hospital_seq(item)
    if is_view_auto_sent:
        contact = {}
        default_phone_number = _device_health_monitor_korean_national_phone_number(
            _display_value(
                item.get("smsPhoneNumber"),
                default=_display_value(item.get("deviceAlertPhone"), default=""),
            )
        )
        default_message = _display_value(
            item.get("smsMessage"),
            default=_display_value(sms_guide.get("message"), default=""),
        )
    else:
        try:
            # 기본 번호를 채우는 조회가 실패해도 사용자가 직접 입력할 수 있게 모달은 연다.
            contact = _lookup_device_health_monitor_hospital_contact(hospital_seq)
        except Exception:
            contact = {}
        default_phone_number = _device_health_monitor_default_modal_phone_number(contact)
        default_message = _display_value(sms_guide.get("message"), default="")
    target = _format_device_health_monitor_action_target(item)
    metadata = {
        "actionId": (
            _DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS
            if is_view_auto_sent
            else _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL
        ),
        "mode": normalized_mode,
        "actorUserId": actor_user_id,
        "channelId": channel_id,
        "messageTs": message_ts,
        "threadTs": thread_ts,
        "item": item,
    }
    phone_element: dict[str, Any] = {
        "type": "plain_text_input",
        "action_id": _DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID,
        "placeholder": {"type": "plain_text", "text": "휴대전화번호 입력 필요"},
    }
    if default_phone_number:
        phone_element["initial_value"] = default_phone_number
    message_element: dict[str, Any] = {
        "type": "plain_text_input",
        "action_id": _DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID,
        "multiline": True,
        "max_length": 1000,
        "placeholder": {"type": "plain_text", "text": "병원에 보낼 문자 내용을 입력해줘"},
    }
    if default_message:
        message_element["initial_value"] = default_message

    if is_view_auto_sent:
        # 자동발송 확인 모달은 재발송 경로가 없도록 입력/submit 없이 발송된 값만 보여준다.
        return {
            "type": "modal",
            "callback_id": _DEVICE_HEALTH_MONITOR_SMS_VIEW_MODAL_CALLBACK_ID,
            "title": {"type": "plain_text", "text": "병원 문자 확인"},
            "close": {"type": "plain_text", "text": "닫기"},
            "private_metadata": json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*받는 번호*\n{default_phone_number or '미확인'}",
                    },
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": f"*문자 내용*\n```{default_message or '미확인'}```",
                    },
                },
            ],
        }

    view: dict[str, Any] = {
        "type": "modal",
        "callback_id": _DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID,
        "title": {"type": "plain_text", "text": "병원 문자 보내기"},
        "close": {"type": "plain_text", "text": "취소"},
        "private_metadata": json.dumps(metadata, ensure_ascii=False, separators=(",", ":")),
    }
    view["submit"] = {"type": "plain_text", "text": "문자 보내기"}
    view["blocks"] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*대상*  `{target}`"},
        },
        {
            "type": "input",
            "block_id": _DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID,
            "label": {"type": "plain_text", "text": "받는 번호"},
            "element": phone_element,
        },
        {
            "type": "input",
            "block_id": _DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID,
            "label": {"type": "plain_text", "text": "문자 내용"},
            "element": message_element,
        },
    ]
    return view


def _handle_device_health_monitor_contact_modal_action(
    *,
    raw_item: Any,
    actor_user_id: str,
    channel_id: str,
    message_ts: str,
    thread_ts: str,
    trigger_id: str,
    client: Any,
    logger: logging.Logger,
    now: datetime | None = None,
    mode: str = _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_SEND,
) -> dict[str, Any]:
    local_now = _coerce_daily_device_round_now(now)
    item = _normalize_device_health_monitor_alert_action_item(raw_item)
    normalized_mode = _display_value(mode, default="")
    if not normalized_mode:
        normalized_mode = (
            _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT
            if _is_device_health_monitor_auto_sent_action_item(item)
            else _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_SEND
        )
    action_id = (
        _DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS
        if normalized_mode == _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT
        else _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL
    )
    normalized_trigger_id = _display_value(trigger_id, default="")
    if not normalized_trigger_id:
        result = {"status": "missing_trigger_id", "ok": False}
    else:
        try:
            view = _build_device_health_monitor_sms_modal_view(
                item=item,
                actor_user_id=actor_user_id,
                channel_id=channel_id,
                message_ts=message_ts,
                thread_ts=thread_ts,
                mode=normalized_mode,
            )
            client.views_open(trigger_id=normalized_trigger_id, view=view)
            result = {"status": "modal_opened", "ok": True}
        except Exception as exc:
            logger.warning("병원 문자 입력 모달을 열지 못했어", exc_info=True)
            result = {"status": "modal_open_failed", "ok": False, "error": type(exc).__name__}

    _append_device_health_monitor_event(
        "alert_contact_sms_modal_requested",
        {
            "actionId": action_id,
            "mode": normalized_mode,
            "actorUserId": actor_user_id,
            "channelId": channel_id,
            "messageTs": message_ts,
            "threadTs": thread_ts,
            "hospital": item["hospital"],
            "room": item["room"],
            "device": item["device"],
            "issue": item["issue"],
            "mdaUrl": item["mdaUrl"],
            "result": result,
        },
        now=local_now,
        logger=logger,
    )
    if not result.get("ok"):
        _post_device_health_monitor_action_reply(
            client,
            channel_id=channel_id,
            thread_ts=thread_ts or message_ts,
            text=f":warning: 병원 문자 입력창을 열지 못했어. 수동으로 처리해줘. `{_format_device_health_monitor_action_target(item)}`",
            logger=logger,
        )
    return {"item": item, "result": result}


def _extract_device_health_monitor_modal_input_value(
    body: dict[str, Any],
    *,
    block_id: str,
    action_id: str,
) -> str:
    view = body.get("view") if isinstance(body.get("view"), dict) else {}
    state = view.get("state") if isinstance(view.get("state"), dict) else {}
    values = state.get("values") if isinstance(state.get("values"), dict) else {}
    block_values = values.get(block_id) if isinstance(values.get(block_id), dict) else {}
    action_value = block_values.get(action_id) if isinstance(block_values.get(action_id), dict) else {}
    return str(action_value.get("value") or "").strip()


def _extract_device_health_monitor_contact_modal_submission(body: dict[str, Any]) -> dict[str, Any]:
    view = body.get("view") if isinstance(body.get("view"), dict) else {}
    user = body.get("user") if isinstance(body.get("user"), dict) else {}
    try:
        metadata = json.loads(str(view.get("private_metadata") or "{}"))
    except json.JSONDecodeError:
        metadata = {}
    return {
        "actionId": _display_value(
            metadata.get("actionId"),
            default=_DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
        ),
        "mode": _display_value(
            metadata.get("mode"),
            default=_DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_SEND,
        ),
        "rawItem": metadata.get("item"),
        "actorUserId": _display_value(
            user.get("id"),
            default=_display_value(metadata.get("actorUserId"), default=""),
        ),
        "channelId": _display_value(metadata.get("channelId"), default=""),
        "messageTs": _display_value(metadata.get("messageTs"), default=""),
        "threadTs": _display_value(metadata.get("threadTs"), default=""),
        "smsPhoneNumber": _extract_device_health_monitor_modal_input_value(
            body,
            block_id=_DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID,
            action_id=_DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID,
        ),
        "smsMessage": _extract_device_health_monitor_modal_input_value(
            body,
            block_id=_DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID,
            action_id=_DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID,
        ),
    }


def _validate_device_health_monitor_contact_modal_submission(body: dict[str, Any]) -> dict[str, str]:
    payload = _extract_device_health_monitor_contact_modal_submission(body)
    if (
        _display_value(payload.get("mode"), default="")
        == _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT
    ):
        return {}
    errors: dict[str, str] = {}
    phone_number = _display_value(payload.get("smsPhoneNumber"), default="")
    if not phone_number:
        errors[_DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID] = "받는 번호를 입력해줘"
    elif not _is_device_health_monitor_mobile_phone_number(phone_number):
        errors[_DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID] = "휴대전화번호만 입력할 수 있어"
    if not _display_value(payload.get("smsMessage"), default=""):
        errors[_DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID] = "문자 내용을 입력해줘"
    return errors


def _handle_device_health_monitor_contact_modal_submission(
    body: dict[str, Any],
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = _extract_device_health_monitor_contact_modal_submission(body)
    if (
        _display_value(payload.get("mode"), default="")
        == _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT
    ):
        return {
            "item": _normalize_device_health_monitor_alert_action_item(payload.get("rawItem")),
            "result": {"status": "viewed", "ok": True},
        }
    return _handle_device_health_monitor_alert_action(
        action_id=_display_value(
            payload.get("actionId"),
            default=_DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
        ),
        raw_item=payload.get("rawItem"),
        actor_user_id=_display_value(payload.get("actorUserId"), default=""),
        channel_id=_display_value(payload.get("channelId"), default=""),
        message_ts=_display_value(payload.get("messageTs"), default=""),
        thread_ts=_display_value(payload.get("threadTs"), default=""),
        client=client,
        logger=logger,
        now=now,
        sms_phone_number=_display_value(payload.get("smsPhoneNumber"), default=""),
        sms_message=_display_value(payload.get("smsMessage"), default=""),
    )


def _handle_device_health_monitor_alert_action(
    *,
    action_id: str,
    raw_item: Any,
    actor_user_id: str,
    channel_id: str,
    message_ts: str,
    thread_ts: str,
    client: Any,
    logger: logging.Logger,
    now: datetime | None = None,
    sms_phone_number: str | None = None,
    sms_message: str | None = None,
) -> dict[str, Any]:
    local_now = _coerce_daily_device_round_now(now)
    item = _normalize_device_health_monitor_alert_action_item(raw_item)
    state = _normalize_device_health_monitor_state(_load_device_health_monitor_state(logger=logger))
    result: dict[str, Any]

    if action_id == _DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE:
        # 실제 재생은 마미박스 장비 agent 쪽 명령 수신/오디오 재생 코드가 들어간 뒤 활성화한다.
        result = {"status": "not_implemented", "ok": False}
    else:
        cooldown = _check_device_health_monitor_action_cooldown(
            state,
            action_id=action_id,
            item=item,
            now=local_now,
        )
        if cooldown.get("active"):
            result = {"status": "cooldown", "ok": False, **cooldown}
        elif action_id == _DEVICE_HEALTH_ALERT_ACTION_MARK_DONE:
            result = {"status": "recorded", "ok": True}
        elif action_id in _DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_URL_SETTINGS:
            result = _post_device_health_monitor_action_webhook(
                action_id=action_id,
                item=item,
                actor_user_id=actor_user_id,
                channel_id=channel_id,
                message_ts=message_ts,
                now=local_now,
                logger=logger,
                sms_phone_number=sms_phone_number,
                sms_message=sms_message,
            )
        else:
            result = {"status": "unsupported_action", "ok": False}

    _append_device_health_monitor_event(
        "alert_action_requested",
        {
            "actionId": action_id,
            "actionLabel": _DEVICE_HEALTH_MONITOR_ACTION_LABELS.get(action_id, action_id),
            "actorUserId": actor_user_id,
            "channelId": channel_id,
            "messageTs": message_ts,
            "threadTs": thread_ts,
            "hospital": item["hospital"],
            "room": item["room"],
            "device": item["device"],
            "issue": item["issue"],
            "mdaUrl": item["mdaUrl"],
            "result": result,
        },
        now=local_now,
        logger=logger,
    )
    _post_device_health_monitor_action_reply(
        client,
        channel_id=channel_id,
        thread_ts=thread_ts or message_ts,
        text=_build_device_health_monitor_action_reply(
            action_id=action_id,
            item=item,
            actor_user_id=actor_user_id,
            result=result,
        ),
        logger=logger,
    )
    return {"item": item, "state": state, "result": result}


def _extract_device_health_monitor_slack_action_payload(body: dict[str, Any]) -> dict[str, Any]:
    actions = body.get("actions") if isinstance(body.get("actions"), list) else []
    action = actions[0] if actions and isinstance(actions[0], dict) else {}
    user = body.get("user") if isinstance(body.get("user"), dict) else {}
    channel = body.get("channel") if isinstance(body.get("channel"), dict) else {}
    message = body.get("message") if isinstance(body.get("message"), dict) else {}
    return {
        "actionId": _display_value(action.get("action_id"), default=""),
        "value": action.get("value"),
        "actorUserId": _display_value(user.get("id"), default=""),
        "channelId": _display_value(channel.get("id"), default=""),
        "messageTs": _display_value(message.get("ts"), default=""),
        "threadTs": _display_value(message.get("thread_ts"), default=_display_value(message.get("ts"), default="")),
        "triggerId": _display_value(body.get("trigger_id"), default=""),
    }


def _handle_device_health_monitor_slack_action(
    body: dict[str, Any],
    client: Any,
    logger: logging.Logger,
) -> dict[str, Any]:
    payload = _extract_device_health_monitor_slack_action_payload(body)
    action_id = _display_value(payload.get("actionId"), default="")
    if action_id not in _DEVICE_HEALTH_MONITOR_ACTION_IDS:
        return {"result": {"status": "ignored", "ok": False}}
    if action_id in {_DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL, _DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS}:
        action_item = _normalize_device_health_monitor_alert_action_item(payload.get("value"))
        modal_mode = (
            _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT
            if action_id == _DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS
            or _is_device_health_monitor_auto_sent_action_item(action_item)
            else _DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_SEND
        )
        return _handle_device_health_monitor_contact_modal_action(
            raw_item=payload.get("value"),
            actor_user_id=_display_value(payload.get("actorUserId"), default=""),
            channel_id=_display_value(payload.get("channelId"), default=""),
            message_ts=_display_value(payload.get("messageTs"), default=""),
            thread_ts=_display_value(payload.get("threadTs"), default=""),
            trigger_id=_display_value(payload.get("triggerId"), default=""),
            client=client,
            logger=logger,
            mode=modal_mode,
        )
    return _handle_device_health_monitor_alert_action(
        action_id=action_id,
        raw_item=payload.get("value"),
        actor_user_id=_display_value(payload.get("actorUserId"), default=""),
        channel_id=_display_value(payload.get("channelId"), default=""),
        message_ts=_display_value(payload.get("messageTs"), default=""),
        thread_ts=_display_value(payload.get("threadTs"), default=""),
        client=client,
        logger=logger,
    )


def _filter_device_health_monitor_alert_summary(
    report_summary: dict[str, Any],
    alertable_fingerprints: set[str],
) -> dict[str, Any]:
    if not alertable_fingerprints:
        return {**report_summary, "deviceResults": [], "statusCounts": {"이상": 0}}

    device_results = (
        report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []
    )
    alertable_devices: list[dict[str, Any]] = []
    for device_result in device_results:
        if not isinstance(device_result, dict):
            continue
        if _display_value(device_result.get("overallLabel"), default="") != "이상":
            continue
        candidate_summary = {**report_summary, "deviceResults": [device_result]}
        items = _collect_daily_device_round_abnormal_alert_items(candidate_summary)
        if any(_build_device_health_monitor_alert_fingerprint(item) in alertable_fingerprints for item in items):
            alertable_devices.append(device_result)

    return {
        **report_summary,
        "deviceResults": alertable_devices,
        "statusCounts": {
            "정상": 0,
            "확인 필요": 0,
            "이상": len(alertable_devices),
            "점검 불가": 0,
        },
    }


def _build_device_health_monitor_zero_counts() -> dict[str, int]:
    return {
        "정상": 0,
        "확인 필요": 0,
        "이상": 0,
        "점검 불가": 0,
    }


def _load_device_health_monitor_device_candidates() -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            # Redis 상태 감시는 병원 순서를 기다리지 않고 활성/설치 장비 전체를 한 번에 본다.
            cursor.execute(
                "SELECT "
                "d.seq AS deviceSeq, "
                "d.deviceName AS deviceName, "
                "d.hospitalSeq AS hospitalSeq, "
                "d.hospitalRoomSeq AS hospitalRoomSeq, "
                "h.hospitalName AS hospitalName, "
                "h.telephone AS hospitalTelephone, "
                "h.deviceAlertPhone AS hospitalDeviceAlertPhone, "
                "hr.roomName AS roomName "
                "FROM devices d "
                "INNER JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "WHERE d.hospitalSeq IS NOT NULL "
                "AND COALESCE(d.deviceName, '') <> '' "
                "AND COALESCE(h.hospitalName, '') NOT REGEXP '^[0-9]+_' "
                "AND COALESCE(d.activeFlag, 1) = 1 "
                "AND COALESCE(d.installFlag, 1) = 1 "
                "ORDER BY d.hospitalSeq ASC, COALESCE(hr.roomName, '') ASC, d.deviceName ASC, d.seq DESC"
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    items: list[dict[str, Any]] = []
    seen_device_names: set[str] = set()
    for row in rows:
        device_name = _display_value(row.get("deviceName"), default="")
        if not device_name or device_name in seen_device_names:
            continue
        # SQL 필터를 보강해 row 데이터가 직접 주입되는 테스트/대체 DB에서도 같은 제외 규칙을 유지한다.
        hospital_name = _display_value(row.get("hospitalName"), default="미확인")
        if _is_device_health_monitor_excluded_hospital_name(hospital_name):
            continue
        seen_device_names.add(device_name)
        items.append(
            {
                "deviceSeq": _coerce_int(row.get("deviceSeq")),
                "deviceName": device_name,
                "hospitalSeq": _coerce_int(row.get("hospitalSeq")),
                "hospitalRoomSeq": _coerce_int(row.get("hospitalRoomSeq")),
                "hospitalName": hospital_name,
                "hospitalTelephone": _display_value(row.get("hospitalTelephone"), default=""),
                "hospitalDeviceAlertPhone": _display_value(
                    row.get("hospitalDeviceAlertPhone"),
                    default="",
                ),
                "roomName": _display_value(row.get("roomName"), default="미확인"),
            }
        )
    return items


def _device_health_monitor_device_cache_ttl_delta() -> timedelta:
    return timedelta(seconds=max(60, int(cs.DEVICE_HEALTH_MONITOR_DEVICE_CACHE_TTL_SEC)))


def _load_device_health_monitor_device_candidates_cached(
    state: dict[str, Any],
    *,
    now: datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    state_payload = state if isinstance(state, dict) else {}
    raw_cached_devices = state_payload.get("deviceCandidateCache")
    cached_devices = _normalize_device_health_monitor_device_candidate_cache(
        raw_cached_devices
    )
    # 연락처 필드 추가 전의 캐시는 알림/자동문자 판단에 필요한 번호를 채울 수 없어 한 번 새로 조회한다.
    cache_has_hospital_contact_fields = isinstance(raw_cached_devices, list) and all(
        isinstance(raw, dict) and "hospitalTelephone" in raw and "hospitalDeviceAlertPhone" in raw
        for raw in raw_cached_devices
    )
    cached_at_text = _display_value(state_payload.get("deviceCandidateCachedAt"), default="")
    cached_at = _parse_device_health_monitor_datetime(cached_at_text)
    cache_is_fresh = bool(
        cached_devices
        and cache_has_hospital_contact_fields
        and cached_at is not None
        and now - cached_at < _device_health_monitor_device_cache_ttl_delta()
    )
    if cache_is_fresh:
        return cached_devices, {
            "cachedAt": cached_at.isoformat(),
            "refreshed": False,
            "refreshError": "",
            "source": "state_cache",
        }

    try:
        # 활성 장비 목록은 자주 바뀌지 않으므로 TTL 만료 때만 DB에서 새로 가져온다.
        fresh_devices = _load_device_health_monitor_device_candidates()
    except Exception as exc:
        if cached_devices:
            return cached_devices, {
                "cachedAt": cached_at.isoformat() if cached_at else cached_at_text,
                "refreshed": False,
                "refreshError": f"{type(exc).__name__}: {exc}",
                "source": "stale_state_cache",
            }
        raise

    return fresh_devices, {
        "cachedAt": now.isoformat(),
        "refreshed": True,
        "refreshError": "",
        "source": "db",
    }


def _build_device_health_monitor_empty_action_counts() -> dict[str, dict[str, int]]:
    return {
        "updateCounts": {
            "agentCandidates": 0,
            "agentUpdated": 0,
            "agentUpdateFailed": 0,
            "boxCandidates": 0,
            "boxUpdated": 0,
            "boxUpdateFailed": 0,
        },
        "cleanupCounts": {
            "candidates": 0,
            "executed": 0,
            "failed": 0,
        },
        "powerCounts": {
            "requested": 0,
            "poweredOff": 0,
            "alreadyOffline": 0,
            "powerOffFailed": 0,
        },
    }


def _build_device_health_monitor_component_labels(
    status_payload: dict[str, Any],
) -> dict[str, str]:
    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    labels: dict[str, str] = {}
    for key in ("audio", "pm2", "storage", "captureboard", "led"):
        component = overview.get(key) if isinstance(overview.get(key), dict) else {}
        labels[key] = _display_value(component.get("label"), default="확인 필요")
    return labels


def _build_device_health_monitor_run_event_payload(
    report_summary: dict[str, Any],
    *,
    channel_id: str = "",
    alertable_fingerprints: set[str] | None = None,
    channel_missing: bool = False,
) -> dict[str, Any]:
    raw_status_counts = (
        report_summary.get("statusCounts") if isinstance(report_summary.get("statusCounts"), dict) else {}
    )
    status_counts = {
        label: max(0, int(raw_status_counts.get(label) or 0))
        for label in ("정상", "확인 필요", "이상", "점검 불가")
    }
    return {
        "runDate": _display_value(report_summary.get("runDate"), default=""),
        "startedAt": _display_value(report_summary.get("startedAt"), default=""),
        "finishedAt": _display_value(report_summary.get("finishedAt"), default=""),
        "checkedDeviceCount": max(0, int(report_summary.get("checkedDeviceCount") or 0)),
        "scheduledDeviceCount": max(0, int(report_summary.get("scheduledDeviceCount") or 0)),
        "deviceCount": max(0, int(report_summary.get("deviceCount") or 0)),
        "statusCounts": status_counts,
        "abnormalCandidateCount": max(0, int(report_summary.get("abnormalCandidateCount") or 0)),
        "sshVerifiedCandidateCount": max(0, int(report_summary.get("sshVerifiedCandidateCount") or 0)),
        "alertableCount": len(alertable_fingerprints or set()),
        "channelId": _display_value(channel_id, default=""),
        "channelMissing": bool(channel_missing),
        "deviceCacheSource": _display_value(report_summary.get("deviceCacheSource"), default=""),
        "deviceCacheRefreshed": bool(report_summary.get("deviceCacheRefreshed")),
        "deviceCacheRefreshError": _display_value(report_summary.get("deviceCacheRefreshError"), default=""),
        "monitorUnavailableReason": _display_value(report_summary.get("monitorUnavailableReason"), default=""),
        "monitorUnavailableDetail": _display_value(report_summary.get("monitorUnavailableDetail"), default=""),
    }


def _build_device_health_monitor_device_event_payload(device_result: dict[str, Any]) -> dict[str, Any]:
    status_payload = (
        device_result.get("statusPayload") if isinstance(device_result.get("statusPayload"), dict) else {}
    )
    request_payload = (
        status_payload.get("request") if isinstance(status_payload.get("request"), dict) else {}
    )
    redis_payload = status_payload.get("redis") if isinstance(status_payload.get("redis"), dict) else {}
    device_state = redis_payload.get("deviceState") if isinstance(redis_payload.get("deviceState"), dict) else {}
    agent_state = redis_payload.get("agentState") if isinstance(redis_payload.get("agentState"), dict) else {}
    ssh_payload = status_payload.get("ssh") if isinstance(status_payload.get("ssh"), dict) else {}
    ssh_close_payload = ssh_payload.get("close") if isinstance(ssh_payload.get("close"), dict) else {}
    overview_payload = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    checks_payload = status_payload.get("checks") if isinstance(status_payload.get("checks"), dict) else {}
    captureboard_overview = (
        overview_payload.get("captureboard") if isinstance(overview_payload.get("captureboard"), dict) else {}
    )

    # 이벤트 로그에는 추적에 필요한 요약만 남기고 Redis snapshot 원본은 남기지 않는다.
    return {
        "hospitalSeq": _coerce_int(device_result.get("hospitalSeq")),
        "hospitalName": _display_value(device_result.get("hospitalName"), default="미확인"),
        "roomName": _display_value(device_result.get("roomName"), default="미확인"),
        "deviceName": _display_value(device_result.get("deviceName"), default="미확인"),
        "overallLabel": _display_value(device_result.get("overallLabel"), default=""),
        "priorityReason": _display_value(device_result.get("priorityReason"), default=""),
        "statusText": _display_value(device_result.get("statusText"), default=""),
        "error": _display_value(device_result.get("error"), default=""),
        "source": _display_value(status_payload.get("source"), default=""),
        "component": _display_value(request_payload.get("component"), default=""),
        "componentLabels": (
            device_result.get("componentLabels")
            if isinstance(device_result.get("componentLabels"), dict)
            else {}
        ),
        "redis": {
            "checkedAt": _display_value(redis_payload.get("checkedAt"), default=""),
            "availabilityReasons": (
                redis_payload.get("availabilityReasons")
                if isinstance(redis_payload.get("availabilityReasons"), list)
                else []
            ),
            "deviceUpdatedAt": _display_value(device_state.get("updatedAt"), default=""),
            "agentUpdatedAt": _display_value(agent_state.get("updatedAt"), default=""),
            "deviceIsConnected": device_state.get("isConnected"),
            "agentIsConnected": agent_state.get("isConnected"),
            "deviceStatus": _display_value(device_state.get("status"), default=""),
            "captureBoardStatus": _display_value(device_state.get("captureBoardStatus"), default=""),
        },
        "ssh": {
            "ready": bool(ssh_payload.get("ready")),
            "verified": bool(ssh_payload.get("verified")),
            "reason": _display_value(ssh_payload.get("reason"), default=""),
            "openedThisRun": bool(ssh_payload.get("openedThisRun")),
            "reusedExisting": bool(ssh_payload.get("reusedExisting")),
            "openWaitTimeoutSec": max(0, int(_coerce_int(ssh_payload.get("openWaitTimeoutSec")) or 0)),
            "closeStatus": _display_value(ssh_close_payload.get("status"), default=""),
        },
        "probe": {
            "captureboard": {
                "status": _display_value(captureboard_overview.get("status"), default=""),
                "label": _display_value(captureboard_overview.get("label"), default=""),
                "summary": _display_value(captureboard_overview.get("summary"), default=""),
                "evidence": _display_value(captureboard_overview.get("evidence"), default=""),
                "overviewDetail": _display_value(captureboard_overview.get("overviewDetail"), default=""),
            },
            "lsusbOutput": _display_value((checks_payload.get("lsusb") or {}).get("output"), default="")[:1000],
            "videoDevicesOutput": _display_value(
                (checks_payload.get("video_devices") or {}).get("output"),
                default="",
            )[:500],
            "v4l2DevicesOutput": _display_value(
                (checks_payload.get("v4l2_devices") or {}).get("output"),
                default="",
            )[:1000],
        },
    }


def _iter_device_health_monitor_device_events(
    report_summary: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    device_results = (
        report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []
    )
    events: list[tuple[str, dict[str, Any]]] = []
    unavailable_payloads: list[dict[str, Any]] = []
    for device_result in device_results:
        if not isinstance(device_result, dict):
            continue
        payload = _build_device_health_monitor_device_event_payload(device_result)
        label = _display_value(device_result.get("overallLabel"), default="")
        source = _display_value(payload.get("source"), default="")
        if label == "점검 불가":
            unavailable_payloads.append(payload)
            continue
        if source == "redis_device_state" and label in {"확인 필요", "이상"}:
            events.append(("redis_candidate", payload))
            continue
        if source == "mda_graphql+ssh_linux_commands" and label == "이상":
            events.append(("ssh_verified_abnormal", payload))

    if unavailable_payloads:
        # 병원이 장비를 꺼두는 케이스가 많아 전체 원본 대신 샘플만 남긴다.
        sample_limit = 20
        events.insert(
            0,
            (
                "device_unavailable",
                {
                    "count": len(unavailable_payloads),
                    "sampleLimit": sample_limit,
                    "omittedCount": max(0, len(unavailable_payloads) - sample_limit),
                    "sampleDevices": unavailable_payloads[:sample_limit],
                },
            ),
        )
    return events


def _log_device_health_monitor_run_events(
    report_summary: dict[str, Any],
    *,
    now: datetime,
    logger: logging.Logger,
    channel_id: str = "",
    alertable_fingerprints: set[str] | None = None,
    channel_missing: bool = False,
) -> None:
    _append_device_health_monitor_event(
        "run_summary",
        _build_device_health_monitor_run_event_payload(
            report_summary,
            channel_id=channel_id,
            alertable_fingerprints=alertable_fingerprints,
            channel_missing=channel_missing,
        ),
        now=now,
        logger=logger,
    )
    for event_type, payload in _iter_device_health_monitor_device_events(report_summary):
        _append_device_health_monitor_event(
            event_type,
            payload,
            now=now,
            logger=logger,
        )


def _build_device_health_monitor_redis_client() -> DeviceStateRedisClient:
    return DeviceStateRedisClient.from_settings()


def _parse_device_health_monitor_percent(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip().replace("%", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_device_health_monitor_state_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        return _coerce_daily_device_round_now(datetime.fromisoformat(text))
    except ValueError:
        return None


def _is_device_health_monitor_state_stale(
    state_payload: dict[str, Any] | None,
    *,
    now: datetime,
) -> bool:
    if not isinstance(state_payload, dict):
        return True
    updated_at = _parse_device_health_monitor_state_datetime(state_payload.get("updatedAt"))
    if updated_at is None:
        return True
    stale_sec = max(30, int(cs.DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC))
    return now - updated_at > timedelta(seconds=stale_sec)


def _build_device_health_monitor_pass_component(summary: str = "Redis 상태 정상") -> dict[str, str]:
    return {
        "status": "pass",
        "label": "정상",
        "summary": summary,
    }


def _build_device_health_monitor_redis_component(
    *,
    status: str,
    label: str,
    summary: str,
) -> dict[str, str]:
    return {
        "status": status,
        "label": label,
        "summary": summary,
    }


def _trim_device_health_monitor_redis_state(state_payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(state_payload, dict):
        return None
    trimmed = {
        key: value
        for key, value in state_payload.items()
        if key not in {"screenshot"}
    }
    acme = trimmed.get("acme") if isinstance(trimmed.get("acme"), dict) else None
    if acme and isinstance(acme.get("systemInfo"), dict):
        system_info = {
            key: value
            for key, value in acme["systemInfo"].items()
            if key != "raw"
        }
        trimmed["acme"] = {
            **acme,
            "systemInfo": system_info,
        }
    return trimmed


def _extract_device_health_monitor_usb_items(device_state: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(device_state, dict):
        return []
    acme = device_state.get("acme") if isinstance(device_state.get("acme"), dict) else {}
    usb_list = acme.get("usbList") if isinstance(acme.get("usbList"), list) else []
    return [item for item in usb_list if isinstance(item, dict)]


def _device_health_monitor_usb_text(item: dict[str, Any]) -> str:
    return " ".join(
        _display_value(item.get(key), default="")
        for key in ("name", "alias", "type", "deviceId")
    ).lower()


def _device_health_monitor_has_led_usb(device_state: dict[str, Any] | None) -> bool | None:
    usb_items = _extract_device_health_monitor_usb_items(device_state)
    if not usb_items:
        return None
    return any(
        "led" in _device_health_monitor_usb_text(item)
        or "mmtled" in _device_health_monitor_usb_text(item)
        for item in usb_items
    )


def _device_health_monitor_has_captureboard_usb(device_state: dict[str, Any] | None) -> bool | None:
    usb_items = _extract_device_health_monitor_usb_items(device_state)
    if not usb_items:
        return None
    return any(
        "captureboard" in _device_health_monitor_usb_text(item)
        or "capture" in _device_health_monitor_usb_text(item)
        or "ls_hdmi" in _device_health_monitor_usb_text(item)
        or "easycap" in _device_health_monitor_usb_text(item)
        for item in usb_items
    )


def _extract_device_health_monitor_disk_percent(device_state: dict[str, Any] | None) -> float | None:
    if not isinstance(device_state, dict):
        return None
    direct_percent = _parse_device_health_monitor_percent(device_state.get("diskUsage"))
    if direct_percent is not None:
        return direct_percent
    acme = device_state.get("acme") if isinstance(device_state.get("acme"), dict) else {}
    system_info = acme.get("systemInfo") if isinstance(acme.get("systemInfo"), dict) else {}
    return _parse_device_health_monitor_percent(system_info.get("hddUsage"))


def _collect_device_health_monitor_redis_availability_reasons(
    *,
    device_context: dict[str, Any],
    device_state: dict[str, Any] | None,
    agent_state: dict[str, Any] | None,
    now: datetime,
) -> list[str]:
    device_name = _display_value(device_context.get("deviceName"), default="장비명 미확인")
    reasons: list[str] = []
    device_stale = _is_device_health_monitor_state_stale(device_state, now=now)
    agent_stale = _is_device_health_monitor_state_stale(agent_state, now=now)
    if device_stale and agent_stale:
        return [f"{device_name} 상태 정보가 Redis에서 갱신되지 않고 있어"]
    if device_stale:
        reasons.append(f"{device_name} 장비 상태 정보가 Redis에서 갱신되지 않고 있어")
    if agent_stale:
        reasons.append(f"{device_name} agent 상태 정보가 Redis에서 갱신되지 않고 있어")

    device_connected = device_state.get("isConnected") if isinstance(device_state, dict) else None
    if device_connected is False:
        reasons.append("장비 socket 연결이 끊겼어")

    agent_connected = agent_state.get("isConnected") if isinstance(agent_state, dict) else None
    if agent_connected is False:
        reasons.append("장비 agent 연결이 끊겼어")

    device_status = _display_value((device_state or {}).get("status"), default="").strip().upper()
    if any(token in device_status for token in ("EXIT", "DISCONNECT", "OFFLINE")):
        reasons.append(f"장비 상태가 {device_status}로 보고됐어")

    return reasons


def _collect_device_health_monitor_redis_issues(
    *,
    device_context: dict[str, Any],
    device_state: dict[str, Any] | None,
    agent_state: dict[str, Any] | None,
    now: datetime,
) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []

    has_captureboard = _device_health_monitor_has_captureboard_usb(device_state)
    capture_board_type = _display_value((device_state or {}).get("captureBoardType"), default="")
    capture_status = _display_value((device_state or {}).get("captureBoardStatus"), default="").strip().lower()
    if (
        has_captureboard is not True
        and (
            capture_status in {"false", "none", "missing"}
            or "disconnect" in capture_status
            or "offline" in capture_status
        )
    ):
        issues.append(
            {
                "component": "captureboard",
                "status": "warning",
                "label": "확인 필요",
                "summary": "Redis 상태에서 캡처보드 연결 이상 후보가 감지됐어",
                "requiresSshVerification": True,
            }
        )

    if has_captureboard is False and capture_board_type:
        issues.append(
            {
                "component": "captureboard",
                "status": "warning",
                "label": "확인 필요",
                "summary": "Redis USB 목록에서 캡처보드를 찾지 못했어",
                "requiresSshVerification": True,
            }
        )

    has_led = _device_health_monitor_has_led_usb(device_state)
    if has_led is False:
        issues.append(
            {
                "component": "led",
                "status": "warning",
                "label": "확인 필요",
                "summary": "Redis USB 목록에서 LED 장치를 찾지 못했어",
                "requiresSshVerification": True,
            }
        )

    disk_percent = _extract_device_health_monitor_disk_percent(device_state)
    if disk_percent is not None and disk_percent >= 90:
        issues.append(
            {
                "component": "storage",
                "status": "warning",
                "label": "확인 필요",
                "summary": f"Redis 상태에서 디스크 사용량이 {disk_percent:.0f}%로 보고됐어",
                "requiresSshVerification": True,
            }
        )

    return issues


def _build_device_health_monitor_redis_status_payload(
    *,
    device_context: dict[str, Any],
    device_state: dict[str, Any] | None,
    agent_state: dict[str, Any] | None,
    issues: list[dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    device_name = _display_value(device_context.get("deviceName"), default="미확인")
    overview: dict[str, Any] = {
        "audio": _build_device_health_monitor_pass_component(),
        "pm2": _build_device_health_monitor_pass_component(),
        "storage": _build_device_health_monitor_pass_component(),
        "captureboard": _build_device_health_monitor_pass_component(),
        "led": _build_device_health_monitor_pass_component(),
    }
    issues_by_component: dict[str, list[dict[str, Any]]] = {}
    for issue in issues:
        component = _display_value(issue.get("component"), default="")
        if component not in overview:
            continue
        issues_by_component.setdefault(component, []).append(issue)

    for component, component_issues in issues_by_component.items():
        has_fail = any(_display_value(item.get("status"), default="") == "fail" for item in component_issues)
        summaries = [
            _display_value(item.get("summary"), default="")
            for item in component_issues
            if _display_value(item.get("summary"), default="")
        ]
        overview[component] = _build_device_health_monitor_redis_component(
            status="fail" if has_fail else "warning",
            label="이상" if has_fail else "확인 필요",
            summary=" / ".join(summaries) or ("이상 감지" if has_fail else "확인 필요"),
        )

    device_payload = {
        "deviceName": device_name,
        "version": _display_value((device_state or {}).get("version"), default=""),
        "useDiaryCapture": (device_state or {}).get("useDiaryCapture"),
        "checkInvalidBarcode": (device_state or {}).get("checkInvalidBarcode"),
        "captureBoardType": _display_value((device_state or {}).get("captureBoardType"), default=""),
        "hospitalName": _display_value(device_context.get("hospitalName"), default=""),
        "hospitalTelephone": _display_value(device_context.get("hospitalTelephone"), default=""),
        "hospitalDeviceAlertPhone": _display_value(
            device_context.get("hospitalDeviceAlertPhone"),
            default="",
        ),
        "roomName": _display_value(device_context.get("roomName"), default=""),
        "isConnected": bool((device_state or {}).get("isConnected")),
    }
    return {
        "route": "device_health_monitor",
        "source": "redis_device_state",
        "request": {
            "deviceName": device_name,
            "component": "all",
        },
        "device": device_payload,
        # Redis snapshot 자체가 판정 근거인 경우에는 SSH 불가를 점검 불가로 해석하지 않게 ready로 둔다.
        "ssh": {
            "ready": True,
            "reason": "redis_snapshot",
            "verified": False,
        },
        "redis": {
            "checkedAt": now.isoformat(),
            "staleThresholdSec": max(30, int(cs.DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC)),
            "deviceState": _trim_device_health_monitor_redis_state(device_state),
            "agentState": _trim_device_health_monitor_redis_state(agent_state),
        },
        "checks": {},
        "overview": overview,
    }


def _build_device_health_monitor_redis_unavailable_status_payload(
    *,
    device_context: dict[str, Any],
    device_state: dict[str, Any] | None,
    agent_state: dict[str, Any] | None,
    reasons: list[str],
    now: datetime,
) -> dict[str, Any]:
    device_name = _display_value(device_context.get("deviceName"), default="미확인")
    return {
        "route": "device_health_monitor",
        "source": "redis_device_state",
        "request": {
            "deviceName": device_name,
            "component": "availability",
        },
        "device": {
            "deviceName": device_name,
            "version": _display_value((device_state or {}).get("version"), default=""),
            "captureBoardType": _display_value((device_state or {}).get("captureBoardType"), default=""),
            "hospitalName": _display_value(device_context.get("hospitalName"), default=""),
            "hospitalTelephone": _display_value(device_context.get("hospitalTelephone"), default=""),
            "hospitalDeviceAlertPhone": _display_value(
                device_context.get("hospitalDeviceAlertPhone"),
                default="",
            ),
            "roomName": _display_value(device_context.get("roomName"), default=""),
            "isConnected": bool((device_state or {}).get("isConnected")),
        },
        # 병원에서 정상적으로 장비를 꺼둘 수 있으므로 통신 불가만으로 이상 알림을 만들지 않는다.
        "ssh": {
            "ready": False,
            "reason": "device_offline_or_state_stale",
            "verified": False,
        },
        "redis": {
            "checkedAt": now.isoformat(),
            "staleThresholdSec": max(30, int(cs.DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC)),
            "availabilityReasons": reasons,
            "deviceState": _trim_device_health_monitor_redis_state(device_state),
            "agentState": _trim_device_health_monitor_redis_state(agent_state),
        },
        "checks": {},
        "overview": {
            "audio": None,
            "pm2": None,
            "storage": None,
            "captureboard": None,
            "led": None,
        },
    }


def _build_device_health_monitor_result_from_redis(
    device_context: dict[str, Any],
    redis_snapshot: dict[str, Any],
    *,
    now: datetime,
) -> tuple[dict[str, Any], bool]:
    device_state = redis_snapshot.get("deviceState") if isinstance(redis_snapshot, dict) else None
    agent_state = redis_snapshot.get("agentState") if isinstance(redis_snapshot, dict) else None
    availability_reasons = _collect_device_health_monitor_redis_availability_reasons(
        device_context=device_context,
        device_state=device_state if isinstance(device_state, dict) else None,
        agent_state=agent_state if isinstance(agent_state, dict) else None,
        now=now,
    )
    if availability_reasons:
        status_payload = _build_device_health_monitor_redis_unavailable_status_payload(
            device_context=device_context,
            device_state=device_state if isinstance(device_state, dict) else None,
            agent_state=agent_state if isinstance(agent_state, dict) else None,
            reasons=availability_reasons,
            now=now,
        )
        result = _build_device_health_monitor_result(device_context, status_payload)
        return {
            **result,
            "overallLabel": "점검 불가",
            "componentLabels": {
                "audio": "점검 불가",
                "pm2": "점검 불가",
                "storage": "점검 불가",
                "captureboard": "점검 불가",
                "led": "점검 불가",
            },
            "statusText": "장비가 오프라인이거나 상태 미갱신이라 이상 판단을 건너뛰었어",
        }, False

    issues = _collect_device_health_monitor_redis_issues(
        device_context=device_context,
        device_state=device_state if isinstance(device_state, dict) else None,
        agent_state=agent_state if isinstance(agent_state, dict) else None,
        now=now,
    )
    direct_issues = [
        issue
        for issue in issues
        if not bool(issue.get("requiresSshVerification"))
    ]
    if direct_issues:
        # Redis가 이미 오프라인/미갱신을 명확히 말하면 SSH 터널을 열지 않고 그 상태로 알린다.
        status_payload = _build_device_health_monitor_redis_status_payload(
            device_context=device_context,
            device_state=device_state if isinstance(device_state, dict) else None,
            agent_state=agent_state if isinstance(agent_state, dict) else None,
            issues=direct_issues,
            now=now,
        )
        return _build_device_health_monitor_result(device_context, status_payload), False

    requires_ssh = any(bool(issue.get("requiresSshVerification")) for issue in issues)
    status_payload = _build_device_health_monitor_redis_status_payload(
        device_context=device_context,
        device_state=device_state if isinstance(device_state, dict) else None,
        agent_state=agent_state if isinstance(agent_state, dict) else None,
        issues=issues,
        now=now,
    )
    result = _build_device_health_monitor_result(device_context, status_payload)
    if requires_ssh and not _is_device_health_monitor_ssh_verification_configured():
        return {
            **result,
            "statusText": "Redis 이상 후보지만 SSH 검증 설정이 없어 알림 대상에서 제외했어",
        }, False
    return result, requires_ssh


def _extract_device_health_monitor_agent_ssh(
    device_info: dict[str, Any],
) -> tuple[str, int]:
    agent_ssh = device_info.get("agentSsh") if isinstance(device_info.get("agentSsh"), dict) else {}
    host = _display_value(agent_ssh.get("host"), default="")
    port = _coerce_int(agent_ssh.get("port")) or 0
    return host, port


def _is_device_health_monitor_agent_ssh_opening(device_info: dict[str, Any]) -> bool:
    agent_ssh = device_info.get("agentSsh") if isinstance(device_info.get("agentSsh"), dict) else {}
    action = _display_value(agent_ssh.get("action"), default="").strip().lower()
    status = _display_value(agent_ssh.get("status"), default="").strip().lower()
    if action != "open":
        return False
    return status not in {"closed", "close", "failed", "fail", "error", "false"}


def _record_device_health_monitor_ssh_tunnel_open(
    device_name: str,
    ssh_tunnel_records: dict[str, dict[str, Any]],
    *,
    now: datetime,
    host: str,
    port: int,
) -> None:
    previous = ssh_tunnel_records.get(device_name) if isinstance(ssh_tunnel_records, dict) else {}
    ssh_tunnel_records[device_name] = {
        "openedAt": now.isoformat(),
        "closedAt": "",
        "host": _display_value(host, default=""),
        "port": max(0, int(port or 0)),
        "closeStatus": "open",
        "closeError": "",
        "count": max(0, int((previous or {}).get("count") or 0)) + 1,
    }


def _record_device_health_monitor_ssh_tunnel_close(
    device_name: str,
    ssh_tunnel_records: dict[str, dict[str, Any]],
    *,
    now: datetime,
    status: str,
    error: str = "",
) -> None:
    previous = ssh_tunnel_records.get(device_name) if isinstance(ssh_tunnel_records, dict) else {}
    ssh_tunnel_records[device_name] = {
        **previous,
        "closedAt": now.isoformat(),
        "closeStatus": _display_value(status, default="unknown"),
        "closeError": _display_value(error, default=""),
    }


def _device_health_monitor_ssh_open_wait_timeout_sec() -> int:
    return max(0, int(cs.DEVICE_HEALTH_MONITOR_SSH_OPEN_WAIT_SEC))


def _device_health_monitor_ssh_open_poll_interval_sec() -> float:
    return max(0.1, float(cs.DEVICE_HEALTH_MONITOR_SSH_OPEN_POLL_INTERVAL_SEC))


def _wait_device_health_monitor_agent_ssh_ready(
    device_name: str,
    *,
    timeout_sec: int,
) -> tuple[dict[str, Any], int]:
    poll_count = 0
    last_device_info = _get_mda_device_agent_ssh(device_name) or {
        "deviceName": device_name,
    }
    host, port = _extract_device_health_monitor_agent_ssh(last_device_info)
    if host and port > 0 or timeout_sec <= 0:
        return last_device_info, poll_count

    deadline = time.monotonic() + timeout_sec
    interval_sec = _device_health_monitor_ssh_open_poll_interval_sec()
    while time.monotonic() < deadline:
        time.sleep(min(interval_sec, max(0.0, deadline - time.monotonic())))
        poll_count += 1
        last_device_info = _get_mda_device_agent_ssh(device_name) or {
            "deviceName": device_name,
        }
        host, port = _extract_device_health_monitor_agent_ssh(last_device_info)
        if host and port > 0:
            return last_device_info, poll_count
    return last_device_info, poll_count


def _build_device_health_monitor_probe_payload(
    *,
    device_name: str,
    component: str,
    now: datetime | None = None,
    ssh_tunnel_records: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    local_now = _coerce_daily_device_round_now(now)
    tunnel_records = ssh_tunnel_records if isinstance(ssh_tunnel_records, dict) else {}
    device_info = _get_mda_device_agent_ssh(device_name) or {
        "deviceName": device_name,
    }
    host, port = _extract_device_health_monitor_agent_ssh(device_info)
    ready = bool(host and port > 0)
    open_result = None
    open_error = ""
    open_in_progress = _is_device_health_monitor_agent_ssh_opening(device_info)
    wait_poll_count = 0
    wait_timeout_sec = _device_health_monitor_ssh_open_wait_timeout_sec()
    open_requested = False

    if not ready and not open_in_progress:
        try:
            # 터널이 없으면 open 요청 후 짧게 기다려, 열리면 이번 순회에서 바로 점검한다.
            open_result = _open_mda_device_ssh(device_name, host=host or None)
            open_requested = True
        except Exception as exc:
            open_error = f"{type(exc).__name__}: {exc}"

    if (
        not ready
        and not open_error
        and wait_timeout_sec > 0
        and (open_requested or open_in_progress)
    ):
        device_info, wait_poll_count = _wait_device_health_monitor_agent_ssh_ready(
            device_name,
            timeout_sec=wait_timeout_sec,
        )
        host, port = _extract_device_health_monitor_agent_ssh(device_info)
        ready = bool(host and port > 0)
        open_in_progress = _is_device_health_monitor_agent_ssh_opening(device_info)

    if ready and open_requested:
        _record_device_health_monitor_ssh_tunnel_open(
            device_name,
            tunnel_records,
            now=local_now,
            host=host,
            port=port,
        )

    device_payload = {
        "deviceName": _display_value(device_info.get("deviceName"), default=device_name),
        "version": _display_value(device_info.get("version"), default=""),
        "useDiaryCapture": device_info.get("useDiaryCapture"),
        "checkInvalidBarcode": device_info.get("checkInvalidBarcode"),
        "captureBoardType": _display_value(device_info.get("captureBoardType"), default=""),
        "hospitalName": _display_value(device_info.get("hospitalName"), default=""),
        "roomName": _display_value(device_info.get("roomName"), default=""),
        "isConnected": bool(device_info.get("isConnected")),
    }
    ssh_payload: dict[str, Any] = {
        "ready": ready,
        "reason": (
            "ready"
            if ready
            else (
                "agent_ssh_open_timeout"
                if open_requested or wait_poll_count > 0
                else (
                    "agent_ssh_open_in_progress"
                    if open_in_progress
                    else ("ssh_open_failed" if open_error else "agent_ssh_not_ready")
                )
            )
        ),
        "host": host,
        "port": port,
        "pollCount": wait_poll_count,
        "reusedExisting": bool(ready and not open_requested and wait_poll_count <= 0),
        "openedThisRun": open_requested,
        "opened": open_result,
        "openWaitTimeoutSec": wait_timeout_sec,
    }
    if open_error:
        ssh_payload["openError"] = open_error

    return {
        "route": "device_health_monitor",
        "source": "mda_graphql+ssh_linux_commands",
        "request": {
            "deviceName": device_name,
            "component": component,
        },
        "device": device_payload,
        "ssh": ssh_payload,
    }, device_info


def _close_device_health_monitor_owned_ssh_tunnel(
    device_name: str,
    *,
    host: str,
    port: int,
    ssh_tunnel_records: dict[str, dict[str, Any]],
    now: datetime,
) -> dict[str, Any]:
    active_count = _get_active_device_ssh_client_count(host, port)
    if active_count > 0:
        # 다른 boxer 작업이 같은 MDA 터널을 쓰는 중이면 health monitor가 닫지 않는다.
        _record_device_health_monitor_ssh_tunnel_close(
            device_name,
            ssh_tunnel_records,
            now=now,
            status="skipped_active",
        )
        return {
            "status": "skipped_active",
            "activeClientCount": active_count,
        }

    try:
        close_result = _close_mda_device_ssh(device_name, host=host or None)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        _record_device_health_monitor_ssh_tunnel_close(
            device_name,
            ssh_tunnel_records,
            now=now,
            status="failed",
            error=error,
        )
        return {
            "status": "failed",
            "activeClientCount": active_count,
            "error": error,
        }

    _record_device_health_monitor_ssh_tunnel_close(
        device_name,
        ssh_tunnel_records,
        now=now,
        status="closed",
    )
    return {
        "status": "closed",
        "activeClientCount": active_count,
        "result": close_result,
    }


def _collect_device_health_monitor_runtime_checks_once(
    device_name: str,
    component: str,
    *,
    now: datetime | None = None,
    ssh_tunnel_records: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    local_now = _coerce_daily_device_round_now(now)
    tunnel_records = ssh_tunnel_records if isinstance(ssh_tunnel_records, dict) else {}
    evidence_payload, device_info = _build_device_health_monitor_probe_payload(
        device_name=device_name,
        component=component,
        now=now,
        ssh_tunnel_records=tunnel_records,
    )
    ssh_payload = evidence_payload.get("ssh") if isinstance(evidence_payload.get("ssh"), dict) else {}
    if not ssh_payload.get("ready"):
        return evidence_payload, device_info, {}

    host = _display_value(ssh_payload.get("host"), default="")
    port = _coerce_int(ssh_payload.get("port")) or 0
    connection = _connect_device_ssh_client(host, port)
    if not connection.get("ok"):
        evidence_payload["ssh"] = {
            **ssh_payload,
            "ready": False,
            "reason": _display_value(connection.get("reason"), default="ssh_connect_failed"),
        }
        return evidence_payload, device_info, {}

    client = connection["client"]
    checks: dict[str, dict[str, Any]] = {}
    command_error: Exception | None = None
    try:
        # 여기서 닫는 것은 모니터가 만든 Paramiko client뿐이다.
        checks = {
            key: _run_status_probe_command(client, key)
            for key in _PROBE_COMPONENT_COMMAND_KEYS[component]
        }
    except Exception as exc:
        command_error = exc
    finally:
        try:
            client.close()
        except Exception:
            pass
        if ssh_payload.get("openedThisRun"):
            close_payload = _close_device_health_monitor_owned_ssh_tunnel(
                device_name,
                host=host,
                port=port,
                ssh_tunnel_records=tunnel_records,
                now=local_now,
            )
            evidence_payload["ssh"] = {
                **(evidence_payload.get("ssh") if isinstance(evidence_payload.get("ssh"), dict) else {}),
                "close": close_payload,
            }

    if command_error is not None:
        raise command_error
    return evidence_payload, device_info, checks


def _build_device_health_monitor_status_payload(
    *,
    device_name: str,
    evidence_payload: dict[str, Any],
    device_info: dict[str, Any],
    checks: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    ssh = evidence_payload.get("ssh") if isinstance(evidence_payload.get("ssh"), dict) else {}
    overview: dict[str, Any] = {
        "audio": None,
        "pm2": None,
        "storage": None,
        "captureboard": None,
        "led": None,
    }

    if ssh.get("ready"):
        # 24시간 모니터는 앱 업데이트 상태 대신 장비 안의 Linux 명령 결과만 근거로 이상을 판단해.
        overview["audio"] = _summarize_audio_path_probe(checks)
        overview["pm2"] = _summarize_pm2_probe(
            _parse_pm2_processes(_display_value((checks.get("pm2_jlist") or {}).get("output"), default=""))
        )
        overview["storage"] = _build_trashcan_storage_summary_from_checks(
            checks,
            cleanup_threshold_percent=cs.DAILY_DEVICE_ROUND_TRASHCAN_USAGE_THRESHOLD_PERCENT,
            cleanup_age_days=cs.DAILY_DEVICE_ROUND_TRASHCAN_DELETE_AGE_DAYS,
        )
        usb_devices = _parse_usb_devices(
            _display_value((checks.get("lsusb") or {}).get("output"), default="")
        )
        overview["captureboard"] = _summarize_captureboard_probe(
            device_info=device_info,
            usb_devices=usb_devices,
            video_devices=_parse_device_path_list(
                _display_value((checks.get("video_devices") or {}).get("output"), default=""),
                missing_token="no_video_device",
            ),
            v4l2_devices=_display_value((checks.get("v4l2_devices") or {}).get("output"), default=""),
        )
        overview["led"] = _summarize_led_probe(
            usb_devices=usb_devices,
            serial_devices=_parse_device_path_list(
                _display_value((checks.get("serial_devices") or {}).get("output"), default=""),
                missing_token="no_serial_device",
            ),
        )

    return {
        **evidence_payload,
        "route": "device_health_monitor",
        "source": "mda_graphql+ssh_linux_commands",
        "request": {
            "deviceName": device_name,
            "component": "all",
        },
        "checks": checks,
        "overview": overview,
    }


def _build_device_health_monitor_result(
    device_context: dict[str, Any],
    status_payload: dict[str, Any],
) -> dict[str, Any]:
    device_name = _display_value(device_context.get("deviceName"), default="미확인")
    device_payload = status_payload.get("device") if isinstance(status_payload.get("device"), dict) else {}
    priority = _build_daily_device_round_priority(status_payload)
    return {
        "deviceSeq": _coerce_int(device_context.get("deviceSeq")),
        "deviceName": _display_value(device_payload.get("deviceName"), default=device_name),
        "hospitalSeq": _coerce_int(device_context.get("hospitalSeq")),
        "hospitalName": _display_value(
            device_context.get("hospitalName"),
            default=_display_value(device_payload.get("hospitalName"), default="미확인"),
        ),
        "hospitalTelephone": _display_value(
            device_context.get("hospitalTelephone"),
            default=_display_value(device_payload.get("hospitalTelephone"), default=""),
        ),
        "hospitalDeviceAlertPhone": _display_value(
            device_context.get("hospitalDeviceAlertPhone"),
            default=_display_value(device_payload.get("hospitalDeviceAlertPhone"), default=""),
        ),
        "roomName": _display_value(
            device_context.get("roomName"),
            default=_display_value(device_payload.get("roomName"), default="미확인"),
        ),
        "overallLabel": _daily_device_round_status_label(status_payload),
        "priorityEligible": bool(priority.get("eligible")),
        "priorityScore": int(priority.get("score") or 0),
        "priorityLabel": _display_value(priority.get("label"), default="판단 보류"),
        "priorityReason": _display_value(
            priority.get("reason"),
            default="네트워크 연결 불가로 이상 징후 판단 보류",
        ),
        "componentLabels": _build_device_health_monitor_component_labels(status_payload),
        "storageDetails": _build_daily_device_round_storage_details(status_payload),
        "statusPayload": status_payload,
        "statusText": "",
        "trashcanCleanup": {
            "status": "skipped",
            "label": "미실행",
            "detail": "24시간 상태 모니터에서는 정리 작업을 실행하지 않아",
            "required": False,
            "executed": False,
        },
        "initialPlan": {
            "agent": {"shouldUpdate": False, "isLatest": False, "reason": "상태 모니터 대상 아님"},
            "box": {"shouldUpdate": False, "alreadyLatest": False, "reason": "상태 모니터 대상 아님"},
        },
        "finalPlan": {
            "agent": {"shouldUpdate": False, "isLatest": False, "reason": "상태 모니터 대상 아님"},
            "box": {"shouldUpdate": False, "alreadyLatest": False, "reason": "상태 모니터 대상 아님"},
        },
        "agentAction": None,
        "boxAction": None,
        "powerAction": None,
        "agentActionText": "상태 모니터 대상 아님",
        "boxActionText": "상태 모니터 대상 아님",
        "powerActionText": "상태 모니터 대상 아님",
    }


def _build_device_health_monitor_error_result(
    device_context: dict[str, Any],
    exc: Exception,
) -> dict[str, Any]:
    status_payload = {
        "route": "device_health_monitor",
        "source": "mda_graphql+ssh_linux_commands",
        "request": {
            "deviceName": _display_value(device_context.get("deviceName"), default=""),
            "component": "all",
        },
        "device": {
            "deviceName": _display_value(device_context.get("deviceName"), default="미확인"),
        },
        "ssh": {
            "ready": False,
            "reason": type(exc).__name__.lower(),
        },
        "checks": {},
        "overview": {
            "audio": None,
            "pm2": None,
            "storage": None,
            "captureboard": None,
            "led": None,
        },
    }
    result = _build_device_health_monitor_result(device_context, status_payload)
    return {
        **result,
        "overallLabel": "점검 불가",
        "componentLabels": {
            "audio": "점검 불가",
            "pm2": "점검 불가",
            "storage": "점검 불가",
            "captureboard": "점검 불가",
            "led": "점검 불가",
        },
        "statusText": f"점검 실패: {type(exc).__name__}",
        "error": f"{type(exc).__name__}: {exc}",
    }


def _run_device_health_monitor_for_device(
    device_context: dict[str, Any],
    *,
    now: datetime | None = None,
    ssh_tunnel_records: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    device_name = _display_value(device_context.get("deviceName"), default="")
    if not device_name:
        raise ValueError("장비명이 비어 있어")

    evidence_payload, device_info, checks = _collect_device_health_monitor_runtime_checks_once(
        device_name,
        "all",
        now=now,
        ssh_tunnel_records=ssh_tunnel_records,
    )
    status_payload = _build_device_health_monitor_status_payload(
        device_name=device_name,
        evidence_payload=evidence_payload,
        device_info=device_info,
        checks=checks,
    )
    return _build_device_health_monitor_result(device_context, status_payload)


def _build_device_health_monitor_summary(
    *,
    now: datetime | None = None,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    local_now = _coerce_daily_device_round_now(now)
    state_payload = state if isinstance(state, dict) else {}
    ssh_tunnel_records = _normalize_device_health_monitor_ssh_tunnel_records(
        state_payload.get("sshTunnelRecords")
    )
    try:
        devices, device_cache_payload = _load_device_health_monitor_device_candidates_cached(
            state_payload,
            now=local_now,
        )
    except Exception as exc:
        return {
            "runDate": local_now.date().isoformat(),
            "startedAt": local_now.isoformat(),
            "finishedAt": local_now.isoformat(),
            "hospitalSeq": None,
            "hospitalName": "전체 장비",
            "deviceCount": 0,
            "scheduledDeviceCount": 0,
            "autoUpdateAgent": False,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
            "autoPowerOff": False,
            "statusCounts": _build_device_health_monitor_zero_counts(),
            **_build_device_health_monitor_empty_action_counts(),
            "deviceResults": [],
            "nextHospitalSeq": None,
            "candidateHospitalCount": 0,
            "checkedDeviceCount": 0,
            "abnormalCandidateCount": 0,
            "sshVerifiedCandidateCount": 0,
            "sshTunnelRecords": ssh_tunnel_records,
            "deviceCandidateCache": _normalize_device_health_monitor_device_candidate_cache(
                state_payload.get("deviceCandidateCache")
            ),
            "deviceCandidateCachedAt": _display_value(state_payload.get("deviceCandidateCachedAt"), default=""),
            "deviceCacheRefreshed": False,
            "deviceCacheRefreshError": f"{type(exc).__name__}: {exc}",
            "deviceCacheSource": "unavailable",
            "monitorUnavailableReason": "device_cache_unavailable",
            "monitorUnavailableDetail": f"{type(exc).__name__}: {exc}",
            "summaryLine": "활성 장비 목록을 가져오지 못해 장비 상태 감시를 건너뛰었어",
        }

    try:
        redis_client = _build_device_health_monitor_redis_client()
        redis_client.ping()
        redis_snapshot = redis_client.load_device_and_agent_states(
            [_display_value(device.get("deviceName"), default="") for device in devices]
        )
    except DeviceStateRedisUnavailable as exc:
        # Redis를 읽지 못하면 전체 상태 감시 자체가 불가능하므로 SSH 순회 fallback은 하지 않는다.
        return {
            "runDate": local_now.date().isoformat(),
            "startedAt": local_now.isoformat(),
            "finishedAt": local_now.isoformat(),
            "hospitalSeq": None,
            "hospitalName": "전체 장비",
            "deviceCount": 0,
            "scheduledDeviceCount": 0,
            "autoUpdateAgent": False,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
            "autoPowerOff": False,
            "statusCounts": _build_device_health_monitor_zero_counts(),
            **_build_device_health_monitor_empty_action_counts(),
            "deviceResults": [],
            "nextHospitalSeq": None,
            "candidateHospitalCount": 0,
            "checkedDeviceCount": 0,
            "abnormalCandidateCount": 0,
            "sshVerifiedCandidateCount": 0,
            "sshTunnelRecords": ssh_tunnel_records,
            "deviceCandidateCache": devices,
            "deviceCandidateCachedAt": _display_value(device_cache_payload.get("cachedAt"), default=""),
            "deviceCacheRefreshed": bool(device_cache_payload.get("refreshed")),
            "deviceCacheRefreshError": _display_value(device_cache_payload.get("refreshError"), default=""),
            "deviceCacheSource": _display_value(device_cache_payload.get("source"), default=""),
            "monitorUnavailableReason": "redis_unavailable",
            "monitorUnavailableDetail": str(exc),
            "summaryLine": "Redis 상태를 읽지 못해 장비 상태 감시를 건너뛰었어",
        }

    device_results: list[dict[str, Any]] = []
    abnormal_candidate_count = 0
    ssh_verified_candidate_count = 0
    for device_context in devices:
        device_name = _display_value(device_context.get("deviceName"), default="")
        redis_result, requires_ssh = _build_device_health_monitor_result_from_redis(
            device_context,
            redis_snapshot.get(device_name, {}),
            now=local_now,
        )
        if _display_value(redis_result.get("overallLabel"), default="") in {"이상", "확인 필요"}:
            abnormal_candidate_count += 1
        if not requires_ssh:
            device_results.append(redis_result)
            continue

        try:
            # Redis에서 하드웨어 후보를 찾은 장비만 실제 SSH 명령으로 2차 확인한다.
            verified_result = _run_device_health_monitor_for_device(
                device_context,
                now=local_now,
                ssh_tunnel_records=ssh_tunnel_records,
            )
            ssh_verified_candidate_count += 1
            device_results.append(verified_result)
        except Exception as exc:
            device_results.append(
                {
                    **redis_result,
                    "statusText": f"Redis 이상 후보 SSH 검증 실패: {type(exc).__name__}",
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )

    finished_at = _coerce_daily_device_round_now(now)
    status_counts = _build_device_health_monitor_zero_counts()
    for item in device_results:
        label = _display_value(item.get("overallLabel"), default="점검 불가")
        status_counts[label if label in status_counts else "점검 불가"] += 1

    return {
        "runDate": local_now.date().isoformat(),
        "startedAt": local_now.isoformat(),
        "finishedAt": finished_at.isoformat(),
        "hospitalSeq": None,
        "hospitalName": "전체 장비",
        "deviceCount": len(device_results),
        "scheduledDeviceCount": len(devices),
        "autoUpdateAgent": False,
        "autoUpdateBox": False,
        "autoCleanupTrashCan": False,
        "autoPowerOff": False,
        "statusCounts": status_counts,
        **_build_device_health_monitor_empty_action_counts(),
        "deviceResults": device_results,
        "nextHospitalSeq": None,
        "candidateHospitalCount": 0,
        "checkedDeviceCount": len(devices),
        "abnormalCandidateCount": abnormal_candidate_count,
        "sshVerifiedCandidateCount": ssh_verified_candidate_count,
        "sshTunnelRecords": ssh_tunnel_records,
        "deviceCandidateCache": devices,
        "deviceCandidateCachedAt": _display_value(device_cache_payload.get("cachedAt"), default=""),
        "deviceCacheRefreshed": bool(device_cache_payload.get("refreshed")),
        "deviceCacheRefreshError": _display_value(device_cache_payload.get("refreshError"), default=""),
        "deviceCacheSource": _display_value(device_cache_payload.get("source"), default=""),
        "monitorUnavailableReason": "",
        "monitorUnavailableDetail": "",
        "summaryLine": (
            f"정상 {status_counts['정상']} / 확인 필요 {status_counts['확인 필요']} / "
            f"이상 {status_counts['이상']} / 점검 불가 {status_counts['점검 불가']}"
        ),
    }


def _collect_device_health_monitor_alert_updates(
    report_summary: dict[str, Any],
    state: dict[str, Any],
    *,
    now: datetime,
) -> tuple[set[str], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    alert_fingerprints = _normalize_device_health_monitor_alerts(state.get("alertFingerprints"))
    pending_fingerprints = _normalize_device_health_monitor_pending_alerts(
        state.get("pendingAlertFingerprints")
    )
    current_items = _collect_daily_device_round_abnormal_alert_items(report_summary)
    current_items_by_fingerprint = {
        _build_device_health_monitor_alert_fingerprint(item): item
        for item in current_items
    }
    current_fingerprints = set(current_items_by_fingerprint)
    reminder_delta = _device_health_monitor_alert_reminder_delta()
    now_text = now.isoformat()
    alertable_fingerprints: set[str] = set()
    updated_alerts: dict[str, dict[str, Any]] = {}
    updated_pending_alerts: dict[str, dict[str, Any]] = {}

    for fingerprint, previous in alert_fingerprints.items():
        if fingerprint in current_fingerprints:
            continue
        last_alerted_at = _parse_device_health_monitor_datetime(previous.get("lastAlertedAt"))
        if last_alerted_at is not None and now - last_alerted_at < reminder_delta:
            # 한두 번의 Redis/SSH 폴링에서 이상 후보가 사라져도, 최근 발송 기록은 reminder 기간 동안 유지한다.
            updated_alerts[fingerprint] = previous

    # 같은 장비의 같은 이상은 최초 발견 또는 reminder 주기 경과 때만 다시 알림을 보낸다.
    for fingerprint in current_fingerprints:
        previous = alert_fingerprints.get(fingerprint, {})
        last_alerted_at = _parse_device_health_monitor_datetime(previous.get("lastAlertedAt"))
        has_previous_alert = last_alerted_at is not None
        should_alert = has_previous_alert and now - last_alerted_at >= reminder_delta
        pending = pending_fingerprints.get(fingerprint, {})
        pending_count = max(0, int(pending.get("count") or 0)) + 1
        required_confirmation_polls = _device_health_monitor_required_confirmation_polls(
            current_items_by_fingerprint.get(fingerprint, {})
        )
        if not has_previous_alert and pending_count >= required_confirmation_polls:
            should_alert = True
        if should_alert:
            alertable_fingerprints.add(fingerprint)
            updated_alerts[fingerprint] = {
                "firstAlertedAt": str(previous.get("firstAlertedAt") or now_text),
                "lastAlertedAt": now_text,
                "lastSeenAt": now_text,
                "count": max(0, int(previous.get("count") or 0)) + 1,
            }
            continue

        if has_previous_alert:
            updated_alerts[fingerprint] = {
                "firstAlertedAt": str(previous.get("firstAlertedAt") or now_text),
                "lastAlertedAt": str(previous.get("lastAlertedAt") or ""),
                "lastSeenAt": now_text,
                "count": max(0, int(previous.get("count") or 0)) + 1,
            }
            continue

        # 기본값은 즉시 알림이다. pending은 나중에 특정 항목만 지연 확인이 필요할 때를 위한 상태다.
        updated_pending_alerts[fingerprint] = {
            "firstSeenAt": str(pending.get("firstSeenAt") or now_text),
            "lastSeenAt": now_text,
            "count": pending_count,
        }

    return alertable_fingerprints, updated_alerts, updated_pending_alerts


def _collect_device_health_monitor_suppressed_alert_updates(
    report_summary: dict[str, Any],
    state: dict[str, Any],
    *,
    now: datetime,
) -> tuple[set[str], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    alert_fingerprints = _normalize_device_health_monitor_alerts(state.get("alertFingerprints"))
    pending_fingerprints = _normalize_device_health_monitor_pending_alerts(
        state.get("pendingAlertFingerprints")
    )
    current_items = _collect_daily_device_round_abnormal_alert_items(report_summary)
    current_items_by_fingerprint = {
        _build_device_health_monitor_alert_fingerprint(item): item
        for item in current_items
    }
    current_fingerprints = set(current_items_by_fingerprint)
    reminder_delta = _device_health_monitor_alert_reminder_delta()
    now_text = now.isoformat()
    suppressed_alertable_fingerprints: set[str] = set()
    updated_alerts: dict[str, dict[str, Any]] = {}
    updated_pending_alerts: dict[str, dict[str, Any]] = {}

    for fingerprint, previous in alert_fingerprints.items():
        last_alerted_at = _parse_device_health_monitor_datetime(previous.get("lastAlertedAt"))
        if fingerprint in current_fingerprints:
            if last_alerted_at is not None and now - last_alerted_at >= reminder_delta:
                suppressed_alertable_fingerprints.add(fingerprint)
            updated_alerts[fingerprint] = {
                "firstAlertedAt": str(previous.get("firstAlertedAt") or now_text),
                "lastAlertedAt": str(previous.get("lastAlertedAt") or ""),
                "lastSeenAt": now_text,
                "count": max(0, int(previous.get("count") or 0)) + 1,
            }
            continue
        if last_alerted_at is not None and now - last_alerted_at < reminder_delta:
            updated_alerts[fingerprint] = previous

    for fingerprint in current_fingerprints:
        if fingerprint in updated_alerts:
            continue
        pending = pending_fingerprints.get(fingerprint, {})
        pending_count = max(0, int(pending.get("count") or 0)) + 1
        required_confirmation_polls = _device_health_monitor_required_confirmation_polls(
            current_items_by_fingerprint.get(fingerprint, {})
        )
        if pending_count >= required_confirmation_polls:
            suppressed_alertable_fingerprints.add(fingerprint)
        # 발송이 꺼진 동안에는 알림 완료로 기록하지 않고 pending만 유지해서 다시 켜면 즉시 발송될 수 있게 한다.
        updated_pending_alerts[fingerprint] = {
            "firstSeenAt": str(pending.get("firstSeenAt") or now_text),
            "lastSeenAt": now_text,
            "count": pending_count,
        }

    return suppressed_alertable_fingerprints, updated_alerts, updated_pending_alerts


def _build_device_health_monitor_next_state(
    state: dict[str, Any],
    report_summary: dict[str, Any],
    *,
    now: datetime,
    alert_fingerprints: dict[str, dict[str, Any]],
    pending_alert_fingerprints: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        **state,
        # Redis 기반 모니터는 병원 순서 state를 쓰지 않으므로 legacy 순회 포인터는 비워 둔다.
        "processedHospitalSeqs": [],
        "lastRunAt": now.isoformat(),
        "lastHospitalSeq": None,
        "lastHospitalName": "전체 장비",
        "nextHospitalSeq": None,
        "candidateHospitalCount": 0,
        "checkedDeviceCount": max(0, int(report_summary.get("checkedDeviceCount") or 0)),
        "abnormalCandidateCount": max(0, int(report_summary.get("abnormalCandidateCount") or 0)),
        "sshVerifiedCandidateCount": max(0, int(report_summary.get("sshVerifiedCandidateCount") or 0)),
        "monitorUnavailableReason": _display_value(report_summary.get("monitorUnavailableReason"), default=""),
        "monitorUnavailableDetail": _display_value(report_summary.get("monitorUnavailableDetail"), default=""),
        "deviceCandidateCache": _normalize_device_health_monitor_device_candidate_cache(
            report_summary.get(
                "deviceCandidateCache",
                state.get("deviceCandidateCache"),
            )
        ),
        "deviceCandidateCachedAt": _display_value(
            report_summary.get(
                "deviceCandidateCachedAt",
                state.get("deviceCandidateCachedAt"),
            ),
            default="",
        ),
        "deviceCacheRefreshed": bool(report_summary.get("deviceCacheRefreshed")),
        "deviceCacheRefreshError": _display_value(report_summary.get("deviceCacheRefreshError"), default=""),
        "deviceCacheSource": _display_value(report_summary.get("deviceCacheSource"), default=""),
        "statusCounts": report_summary.get("statusCounts"),
        "alertFingerprints": alert_fingerprints,
        "pendingAlertFingerprints": _normalize_device_health_monitor_pending_alerts(
            pending_alert_fingerprints
            if pending_alert_fingerprints is not None
            else state.get("pendingAlertFingerprints")
        ),
        "sshTunnelRecords": _normalize_device_health_monitor_ssh_tunnel_records(
            report_summary.get("sshTunnelRecords")
        ),
    }


def _record_device_health_sheet_alerts_best_effort(
    alert_summary: dict[str, Any],
    *,
    detected_at: datetime,
    slack_permalink: str,
    logger: logging.Logger,
) -> None:
    try:
        # Slack 발송 성공 후에만 기록하고, Sheets 장애는 이미 전달된 운영 알림을 실패로 바꾸지 않는다.
        sheet_row_count = _append_device_health_sheet_alerts(
            _collect_daily_device_round_abnormal_alert_items(alert_summary),
            detected_at=detected_at,
            slack_permalink=slack_permalink,
        )
        if sheet_row_count is None:
            return
        _append_device_health_monitor_event(
            "sheet_alert_rows_written",
            {
                "spreadsheetId": _display_value(
                    cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID,
                    default="",
                ),
                "sheetName": _display_value(cs.DEVICE_HEALTH_SHEET_TAB_NAME, default=""),
                "rowCount": max(0, int(sheet_row_count)),
            },
            now=detected_at,
            logger=logger,
        )
    except Exception as exc:
        _append_device_health_monitor_event(
            "sheet_alert_write_failed",
            {
                "spreadsheetId": _display_value(
                    cs.DEVICE_HEALTH_SHEET_SPREADSHEET_ID,
                    default="",
                ),
                "sheetName": _display_value(cs.DEVICE_HEALTH_SHEET_TAB_NAME, default=""),
                "errorType": type(exc).__name__,
            },
            now=detected_at,
            logger=logger,
        )
        logger.warning("장비 이상 알림을 Google Sheets에 기록하지 못했어", exc_info=True)


def _run_device_health_monitor_once(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> bool:
    if not cs.DEVICE_HEALTH_MONITOR_ENABLED:
        return False
    if not s.DB_QUERY_ENABLED:
        logger.warning("장비 상태 모니터를 켤 수 없어. DB_QUERY_ENABLED가 비활성이야")
        return False

    local_now = _coerce_daily_device_round_now(now)
    state = _normalize_device_health_monitor_state(_load_device_health_monitor_state(logger=logger))
    report_summary = _build_device_health_monitor_summary(
        now=local_now,
        state=state,
    )
    if _display_value(report_summary.get("monitorUnavailableReason"), default=""):
        next_state = _build_device_health_monitor_next_state(
            state,
            report_summary,
            now=local_now,
            alert_fingerprints=_normalize_device_health_monitor_alerts(state.get("alertFingerprints")),
            pending_alert_fingerprints=_normalize_device_health_monitor_pending_alerts(
                state.get("pendingAlertFingerprints")
            ),
        )
        _persist_device_health_monitor_state_best_effort(next_state, logger=logger)
        _append_device_health_monitor_event(
            "monitor_unavailable",
            _build_device_health_monitor_run_event_payload(report_summary),
            now=local_now,
            logger=logger,
        )
        logger.warning(
            "Device health monitor unavailable reason=%s detail=%s",
            report_summary.get("monitorUnavailableReason"),
            report_summary.get("monitorUnavailableDetail"),
        )
        return False

    channel_id = _device_health_monitor_channel_id()
    alert_delivery_status = _resolve_device_health_monitor_alert_delivery_status(state)
    alert_delivery_enabled = bool(alert_delivery_status.get("enabled"))
    if alert_delivery_enabled and not channel_id:
        next_state = _build_device_health_monitor_next_state(
            state,
            report_summary,
            now=local_now,
            alert_fingerprints=_normalize_device_health_monitor_alerts(state.get("alertFingerprints")),
            pending_alert_fingerprints=_normalize_device_health_monitor_pending_alerts(
                state.get("pendingAlertFingerprints")
            ),
        )
        _persist_device_health_monitor_state_best_effort(next_state, logger=logger)
        _log_device_health_monitor_run_events(
            report_summary,
            now=local_now,
            logger=logger,
            channel_missing=True,
        )
        logger.warning("장비 상태 모니터 채널 ID가 없어. DEVICE_HEALTH_MONITOR_CHANNEL_ID를 확인해줘")
        return False

    if not alert_delivery_enabled:
        (
            suppressed_alertable_fingerprints,
            updated_alerts,
            updated_pending_alerts,
        ) = _collect_device_health_monitor_suppressed_alert_updates(
            report_summary,
            state,
            now=local_now,
        )
        next_state = _build_device_health_monitor_next_state(
            state,
            report_summary,
            now=local_now,
            alert_fingerprints=updated_alerts,
            pending_alert_fingerprints=updated_pending_alerts,
        )
        _persist_device_health_monitor_state_best_effort(next_state, logger=logger)
        _log_device_health_monitor_run_events(
            report_summary,
            now=local_now,
            logger=logger,
            channel_id=channel_id,
            alertable_fingerprints=suppressed_alertable_fingerprints,
        )
        if suppressed_alertable_fingerprints:
            _append_device_health_monitor_event(
                "alert_delivery_suppressed",
                {
                    "alertableCount": len(suppressed_alertable_fingerprints),
                    "alertFingerprints": sorted(suppressed_alertable_fingerprints),
                    "checkedDeviceCount": max(0, int(report_summary.get("checkedDeviceCount") or 0)),
                    "abnormalCandidateCount": max(0, int(report_summary.get("abnormalCandidateCount") or 0)),
                    "source": _display_value(alert_delivery_status.get("source"), default=""),
                },
                now=local_now,
                logger=logger,
            )
        logger.info(
            "Suppressed device health alert delivery checkedDevices=%s alertable=%s source=%s",
            report_summary.get("checkedDeviceCount"),
            len(suppressed_alertable_fingerprints),
            alert_delivery_status.get("source"),
        )
        return False

    alertable_fingerprints, updated_alerts, updated_pending_alerts = _collect_device_health_monitor_alert_updates(
        report_summary,
        state,
        now=local_now,
    )
    next_state = _build_device_health_monitor_next_state(
        state,
        report_summary,
        now=local_now,
        alert_fingerprints=updated_alerts,
        pending_alert_fingerprints=updated_pending_alerts,
    )
    _persist_device_health_monitor_state_best_effort(next_state, logger=logger)
    _log_device_health_monitor_run_events(
        report_summary,
        now=local_now,
        logger=logger,
        channel_id=channel_id,
        alertable_fingerprints=alertable_fingerprints,
    )

    if not alertable_fingerprints:
        logger.info(
            "Checked device health channel=%s checkedDevices=%s abnormalCandidates=%s alertable=0",
            channel_id,
            report_summary.get("checkedDeviceCount"),
            report_summary.get("abnormalCandidateCount"),
        )
        return False

    alert_summary = _filter_device_health_monitor_alert_summary(report_summary, alertable_fingerprints)
    alert_summary = _apply_device_health_monitor_auto_sms(
        alert_summary,
        channel_id=channel_id,
        now=local_now,
        logger=logger,
    )
    slack_delivery = _post_daily_device_round_abnormal_alert(
        client,
        alert_summary,
        channel_id=channel_id,
        message_ts="",
        logger=logger,
        include_actions=True,
    )
    if slack_delivery is None:
        logger.warning(
            "Device health Slack alert delivery failed channel=%s alertable=%s",
            channel_id,
            len(alertable_fingerprints),
        )
        return False
    _append_device_health_monitor_event(
        "slack_alert_sent",
        {
            "channelId": channel_id,
            "messageTs": _display_value(slack_delivery.get("messageTs"), default=""),
            "permalink": _display_value(slack_delivery.get("permalink"), default=""),
            "alertableCount": len(alertable_fingerprints),
            "alertFingerprints": sorted(alertable_fingerprints),
            "checkedDeviceCount": max(0, int(report_summary.get("checkedDeviceCount") or 0)),
            "abnormalCandidateCount": max(0, int(report_summary.get("abnormalCandidateCount") or 0)),
        },
        now=local_now,
        logger=logger,
    )
    _record_device_health_sheet_alerts_best_effort(
        alert_summary,
        detected_at=local_now,
        slack_permalink=_display_value(slack_delivery.get("permalink"), default=""),
        logger=logger,
    )
    logger.info(
        "Posted device health alert channel=%s checkedDevices=%s alertable=%s",
        channel_id,
        report_summary.get("checkedDeviceCount"),
        len(alertable_fingerprints),
    )
    return True


def _device_health_monitor_loop(client: Any, logger: logging.Logger) -> None:
    poll_interval_sec = max(30, int(cs.DEVICE_HEALTH_MONITOR_POLL_INTERVAL_SEC))
    while True:
        try:
            _run_device_health_monitor_once(client, logger)
        except Exception:
            logger.exception("장비 상태 모니터 중 오류가 발생했어")
        time.sleep(poll_interval_sec)


def _attach_device_health_monitor_alert_actions(app: Any, logger: logging.Logger) -> None:
    def _build_action_handler(action_id: str):
        def _handle_action(ack, body: dict[str, Any], client: Any) -> None:
            ack()
            action_body = body if isinstance(body, dict) else {}
            # Slack action은 즉시 ack한 뒤, 병원 문자는 모달을 열고 나머지 action은 별도 handler에서 처리한다.
            _handle_device_health_monitor_slack_action(action_body, client, logger)

        return _handle_action

    for action_id in sorted(_DEVICE_HEALTH_MONITOR_ACTION_IDS):
        app.action(action_id)(_build_action_handler(action_id))

    def _handle_contact_modal_submission(ack, body: dict[str, Any], client: Any) -> None:
        action_body = body if isinstance(body, dict) else {}
        errors = _validate_device_health_monitor_contact_modal_submission(action_body)
        if errors:
            ack(response_action="errors", errors=errors)
            return
        ack()
        _handle_device_health_monitor_contact_modal_submission(action_body, client, logger)

    app.view(_DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID)(_handle_contact_modal_submission)


def attach_device_health_monitor_reporter(app: Any, *, logger: logging.Logger | None = None) -> None:
    if not cs.DEVICE_HEALTH_MONITOR_ENABLED:
        return

    actual_logger = logger or logging.getLogger(__name__)
    if not s.DB_QUERY_ENABLED:
        actual_logger.warning(
            "장비 상태 모니터가 활성화됐는데 DB_QUERY_ENABLED가 꺼져 있어 시작하지 않을게"
        )
        return

    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("장비 상태 모니터를 시작하지 못했어. Slack client가 없어")
        return

    _attach_device_health_monitor_alert_actions(app, actual_logger)

    global _DEVICE_HEALTH_MONITOR_THREAD
    with _DEVICE_HEALTH_MONITOR_THREAD_LOCK:
        if _DEVICE_HEALTH_MONITOR_THREAD is not None and _DEVICE_HEALTH_MONITOR_THREAD.is_alive():
            return
        _DEVICE_HEALTH_MONITOR_THREAD = threading.Thread(
            target=_device_health_monitor_loop,
            args=(client, actual_logger),
            name="boxer-device-health-monitor",
            daemon=True,
        )
        _DEVICE_HEALTH_MONITOR_THREAD.start()
        actual_logger.info(
            "Started device health monitor channel=%s interval=%ss",
            _device_health_monitor_channel_id(),
            max(30, int(cs.DEVICE_HEALTH_MONITOR_POLL_INTERVAL_SEC)),
        )
