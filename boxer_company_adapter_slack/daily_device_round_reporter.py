import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

from boxer.core import settings as s
from boxer.core.utils import _display_value
from boxer_company import settings as cs
from boxer_company.daily_device_round import (
    _build_daily_device_round_blocks,
    _build_daily_device_round_issue_summary,
    _build_daily_device_round_summary,
    _coerce_daily_device_round_hospital_seqs,
    _coerce_daily_device_round_now,
    _coerce_int,
    _daily_device_round_hospital_order,
    _daily_device_round_hospital_scope,
    _daily_device_round_timezone,
    _format_daily_device_round_hospital_label,
    _format_daily_device_round_report,
    _normalize_daily_device_round_hospital_order,
    _normalize_daily_device_round_hospital_scope,
)

_DAILY_DEVICE_ROUND_THREAD: threading.Thread | None = None
_DAILY_DEVICE_ROUND_THREAD_LOCK = threading.Lock()
_DAILY_DEVICE_ROUND_RUNTIME_STATE: dict[str, Any] = {}
_DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK = threading.Lock()
_DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE = 40
_DAILY_DEVICE_ROUND_MAX_BLOCK_CHARS_PER_MESSAGE = 12000
_DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE = 3500
_DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT_OVERRIDE_KEY = "autoUpdateAgentOverride"
_DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT_UPDATED_AT_KEY = "autoUpdateAgentUpdatedAt"
_DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT_UPDATED_BY_KEY = "autoUpdateAgentUpdatedBy"
_DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_OVERRIDE_KEY = "autoUpdateBoxOverride"
_DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_UPDATED_AT_KEY = "autoUpdateBoxUpdatedAt"
_DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_UPDATED_BY_KEY = "autoUpdateBoxUpdatedBy"
_DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL = "device_health_alert_contact_hospital"
_DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS = "device_health_alert_view_auto_sms"
_DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE = "device_health_alert_device_voice_guide"
_DEVICE_HEALTH_ALERT_ACTION_MARK_DONE = "device_health_alert_mark_done"
_DEVICE_HEALTH_ALERT_ACTION_ITEM_LIMIT = 10
_DEVICE_HEALTH_ALERT_DEFAULT_TITLE = "장비 상태 확인 필요"
_DEVICE_HEALTH_ALERT_CATEGORY_TITLES = {
    "recording": "녹화 상태 확인 필요",
    "recording_processing": "녹화 파일 처리 확인 필요",
    "video_signal": "영상 신호 확인 필요",
    "led": "LED 연결 확인 필요",
    "audio": "음성 출력 확인 필요",
    "application": "장비 앱 실행 확인 필요",
    "storage": "장비 저장 공간 부족",
    "device_connection": "장비 연결 확인 필요",
    "upload": "영상 업로드 확인 필요",
}
_DEVICE_HEALTH_ALERT_COMPONENT_CATEGORIES = {
    "audio": "audio",
    "pm2": "application",
    "storage": "storage",
    "captureboard": "video_signal",
    "led": "led",
}
_DEVICE_HEALTH_ALERT_SMS_AUTO_SENT_TEXT = "문자 자동발송 완료"
_DEVICE_HEALTH_ALERT_SMS_AUTO_FAILED_TEXT = "문자 자동발송 실패 - 수동 발송 가능"
_DEVICE_HEALTH_ALERT_SMS_MODAL_MODE_VIEW_AUTO_SENT = "view_auto_sent"
_DEVICE_HEALTH_ALERT_COMPONENT_ORDER = ("captureboard", "led", "audio", "pm2")
_DEVICE_HEALTH_ALERT_COMPONENT_NAMES = {
    "captureboard": "캡처보드",
    "led": "LED",
    "audio": "스피커",
    "pm2": "PM2",
}
_DAILY_DEVICE_ROUND_ACTIVE_PROGRESS_KEYS = (
    "activeHospitalSeq",
    "activeHospitalName",
    "activeHospitalStartedAt",
    "activeHospitalDeviceCount",
    "activeDeviceIndex",
    "activeDeviceName",
    "activeDeviceUpdatedAt",
)


def _daily_device_round_state_path() -> Path:
    return Path(cs.DAILY_DEVICE_ROUND_STATE_PATH).expanduser()


def _load_daily_device_round_state(
    state_path: Path | None = None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    path = state_path or _daily_device_round_state_path()
    runtime_state = _load_daily_device_round_runtime_state()
    if not path.exists():
        return runtime_state
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if logger is not None:
            logger.warning("일일 장비 순회 상태 파일을 읽지 못했어: %s", path, exc_info=True)
        return runtime_state
    state = data if isinstance(data, dict) else {}
    if runtime_state:
        merged_state = dict(state)
        merged_state.update(runtime_state)
        return merged_state
    return state


def _save_daily_device_round_state(
    state: dict[str, Any],
    state_path: Path | None = None,
) -> None:
    path = state_path or _daily_device_round_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _load_daily_device_round_runtime_state() -> dict[str, Any]:
    with _DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
        return dict(_DAILY_DEVICE_ROUND_RUNTIME_STATE)


def _remember_daily_device_round_runtime_state(
    state: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    normalized_state = _normalize_daily_device_round_state(state, now=now)
    with _DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
        _DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()
        _DAILY_DEVICE_ROUND_RUNTIME_STATE.update(normalized_state)
    return normalized_state


def _persist_daily_device_round_state_best_effort(
    state: dict[str, Any],
    *,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    normalized_state = _remember_daily_device_round_runtime_state(state, now=now)
    try:
        _save_daily_device_round_state(normalized_state)
    except Exception:
        if logger is not None:
            logger.warning("일일 장비 순회 상태를 즉시 저장하지 못했어", exc_info=True)
    return normalized_state


def _coerce_daily_device_round_optional_bool(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    normalized = str(value or "").strip().lower()
    if normalized in {"1", "true", "yes", "on", "enable", "enabled"}:
        return True
    if normalized in {"0", "false", "no", "off", "disable", "disabled"}:
        return False
    return None


def _daily_device_round_auto_update_keys(target: str) -> tuple[str, str, str]:
    if target == "agent":
        return (
            _DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT_OVERRIDE_KEY,
            _DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT_UPDATED_AT_KEY,
            _DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT_UPDATED_BY_KEY,
        )
    if target == "box":
        return (
            _DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_OVERRIDE_KEY,
            _DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_UPDATED_AT_KEY,
            _DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_UPDATED_BY_KEY,
        )
    raise ValueError(f"지원하지 않는 자동 업데이트 대상이야: {target}")


def _daily_device_round_auto_update_env_default(target: str) -> bool:
    if target == "agent":
        return bool(cs.DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT)
    if target == "box":
        return bool(cs.DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX)
    raise ValueError(f"지원하지 않는 자동 업데이트 대상이야: {target}")


def _daily_device_round_auto_update_label(target: str) -> str:
    if target == "agent":
        return "에이전트"
    if target == "box":
        return "마미박스"
    return target


def _resolve_daily_device_round_auto_update(
    target: str,
    state: dict[str, Any] | None = None,
) -> bool:
    state_payload = state if isinstance(state, dict) else _load_daily_device_round_state()
    override_key, _, _ = _daily_device_round_auto_update_keys(target)
    override = _coerce_daily_device_round_optional_bool(
        state_payload.get(override_key)
    )
    # Slack 명령 override가 있으면 env보다 우선하고, 없으면 기존 env 기본값을 그대로 쓴다.
    if override is not None:
        return override
    return _daily_device_round_auto_update_env_default(target)


def _resolve_daily_device_round_auto_update_agent(
    state: dict[str, Any] | None = None,
) -> bool:
    return _resolve_daily_device_round_auto_update("agent", state)


def _resolve_daily_device_round_auto_update_box(
    state: dict[str, Any] | None = None,
) -> bool:
    return _resolve_daily_device_round_auto_update("box", state)


def _build_daily_device_round_auto_update_target_status(
    target: str,
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state_payload = state if isinstance(state, dict) else _load_daily_device_round_state()
    override_key, updated_at_key, updated_by_key = _daily_device_round_auto_update_keys(target)
    override = _coerce_daily_device_round_optional_bool(
        state_payload.get(override_key)
    )
    return {
        "target": target,
        "label": _daily_device_round_auto_update_label(target),
        "enabled": _resolve_daily_device_round_auto_update(target, state_payload),
        "envDefault": _daily_device_round_auto_update_env_default(target),
        "override": override,
        "source": "slack_override" if override is not None else "env",
        "updatedAt": str(state_payload.get(updated_at_key) or "").strip(),
        "updatedBy": str(state_payload.get(updated_by_key) or "").strip(),
    }


def _build_daily_device_round_auto_update_status(
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state_payload = state if isinstance(state, dict) else _load_daily_device_round_state()
    return {
        "agent": _build_daily_device_round_auto_update_target_status("agent", state_payload),
        "box": _build_daily_device_round_auto_update_target_status("box", state_payload),
    }


def _build_daily_device_round_auto_update_box_status(
    state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _build_daily_device_round_auto_update_target_status("box", state)


def _set_daily_device_round_auto_update(
    target: str,
    enabled: bool,
    *,
    user_id: str | None = None,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    local_now = _coerce_daily_device_round_now(now)
    state = _load_daily_device_round_state(logger=logger)
    override_key, updated_at_key, updated_by_key = _daily_device_round_auto_update_keys(target)
    next_state = {
        **state,
        override_key: bool(enabled),
        updated_at_key: local_now.isoformat(),
        updated_by_key: str(user_id or "").strip(),
    }
    # 사용자가 직접 켜고 끄는 설정은 저장 실패를 숨기지 말고 호출자에게 알려야 한다.
    persisted_state = _remember_daily_device_round_runtime_state(next_state, now=local_now)
    _save_daily_device_round_state(persisted_state)
    return _build_daily_device_round_auto_update_status(persisted_state)


def _set_daily_device_round_auto_update_box(
    enabled: bool,
    *,
    user_id: str | None = None,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    return _set_daily_device_round_auto_update(
        "box",
        enabled,
        user_id=user_id,
        now=now,
        logger=logger,
    )["box"]


def _format_daily_device_round_auto_update_target_line(status: dict[str, Any]) -> list[str]:
    label = str(status.get("label") or "대상").strip()
    enabled = bool(status.get("enabled"))
    source = str(status.get("source") or "").strip()
    updated_at = str(status.get("updatedAt") or "").strip()
    updated_by = str(status.get("updatedBy") or "").strip()
    # 운영자가 보는 기준은 최종 적용값 하나여야 해서 .env 기본값은 응답에 섞어 보여주지 않는다.
    source_label = "저장 설정" if source == "slack_override" else "초기 기본값"
    lines = [
        f"• {label}: *{'켜짐' if enabled else '꺼짐'}* "
        f"| 기준 `{source_label}`"
    ]
    if updated_at:
        actor = f" / <@{updated_by}>" if updated_by else ""
        lines.append(f"  - 마지막 변경: `{updated_at}`{actor}")
    return lines


def _format_daily_device_round_auto_update_status(status: dict[str, Any]) -> str:
    agent_status = status.get("agent") if isinstance(status.get("agent"), dict) else {}
    box_status = status.get("box") if isinstance(status.get("box"), dict) else {}
    lines = ["*데일리 자동 업데이트 설정*"]
    lines.extend(_format_daily_device_round_auto_update_target_line(box_status))
    lines.extend(_format_daily_device_round_auto_update_target_line(agent_status))
    lines.append("• 적용: 다음 데일리 순회부터")
    return "\n".join(lines)


def _format_daily_device_round_auto_update_box_status(status: dict[str, Any]) -> str:
    return _format_daily_device_round_auto_update_status(
        {
            "box": status,
            "agent": _build_daily_device_round_auto_update_target_status("agent"),
        }
    )


def _clear_daily_device_round_active_progress(
    state: dict[str, Any],
) -> dict[str, Any]:
    next_state = dict(state if isinstance(state, dict) else {})
    for key in _DAILY_DEVICE_ROUND_ACTIVE_PROGRESS_KEYS:
        next_state.pop(key, None)
    return next_state


def _merge_daily_device_round_active_progress(
    state: dict[str, Any],
    *,
    hospital_seq: int | None,
    hospital_name: str | None = None,
    hospital_started_at: str | None = None,
    hospital_device_count: int | None = None,
    device_index: int | None = None,
    device_name: str | None = None,
    device_updated_at: str | None = None,
) -> dict[str, Any]:
    next_state = _clear_daily_device_round_active_progress(state)
    if hospital_seq is None:
        return next_state

    next_state["activeHospitalSeq"] = int(hospital_seq)
    next_state["activeHospitalName"] = str(hospital_name or "").strip()
    next_state["activeHospitalStartedAt"] = str(hospital_started_at or "").strip()
    if hospital_device_count is not None:
        next_state["activeHospitalDeviceCount"] = max(0, int(hospital_device_count))
    if device_index is not None:
        next_state["activeDeviceIndex"] = max(1, int(device_index))
    if device_name:
        next_state["activeDeviceName"] = str(device_name).strip()
    if device_updated_at:
        next_state["activeDeviceUpdatedAt"] = str(device_updated_at).strip()
    return next_state


def _estimate_daily_device_round_block_size(block: dict[str, Any]) -> int:
    return len(json.dumps(block, ensure_ascii=False))


def _split_daily_device_round_blocks(
    blocks: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    chunks: list[list[dict[str, Any]]] = []
    current_chunk: list[dict[str, Any]] = []
    current_size = 0

    for block in blocks:
        block_size = _estimate_daily_device_round_block_size(block)
        should_rotate = (
            current_chunk
            and (
                len(current_chunk) >= _DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE
                or current_size + block_size > _DAILY_DEVICE_ROUND_MAX_BLOCK_CHARS_PER_MESSAGE
            )
        )
        if should_rotate:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
        current_chunk.append(block)
        current_size += block_size

    if current_chunk:
        chunks.append(current_chunk)
    return chunks or [[]]


def _split_daily_device_round_text(
    text: str,
) -> list[str]:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return [""]
    if len(normalized_text) <= _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE:
        return [normalized_text]

    chunks: list[str] = []
    current_chunk = ""
    # Slack text fallback도 길이 제한을 넘지 않도록 줄 단위로 먼저 자르고,
    # 한 줄이 너무 길면 그 줄만 다시 잘라서 이어 보내.
    for line in normalized_text.split("\n"):
        line_parts = [line] or [""]
        if len(line) > _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE:
            line_parts = [
                line[index : index + _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE]
                for index in range(0, len(line), _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE)
            ]
        for line_part in line_parts:
            candidate = line_part if not current_chunk else f"{current_chunk}\n{line_part}"
            if current_chunk and len(candidate) > _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE:
                chunks.append(current_chunk)
                current_chunk = line_part
                continue
            current_chunk = candidate

    if current_chunk:
        chunks.append(current_chunk)
    return chunks


def _build_daily_device_round_chunk_text(
    base_text: str,
    *,
    chunk_index: int,
    chunk_count: int,
) -> str:
    if chunk_count <= 1:
        return base_text
    return f"{base_text} | 계속 {chunk_index + 1}/{chunk_count}"


def _is_daily_device_round_runtime_configured() -> bool:
    return bool(
        cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


def _daily_device_round_window_schedule() -> tuple[tuple[int, int], tuple[int, int]]:
    start_hour = max(0, min(23, int(cs.DAILY_DEVICE_ROUND_HOUR_KST)))
    start_minute = max(0, min(59, int(cs.DAILY_DEVICE_ROUND_MINUTE_KST)))
    end_hour = max(0, min(23, int(cs.DAILY_DEVICE_ROUND_END_HOUR_KST)))
    end_minute = max(0, min(59, int(cs.DAILY_DEVICE_ROUND_END_MINUTE_KST)))
    return (start_hour, start_minute), (end_hour, end_minute)


def _daily_device_round_window_key(now: datetime | None) -> str | None:
    local_now = _coerce_daily_device_round_now(now)
    (start_hour, start_minute), (end_hour, end_minute) = _daily_device_round_window_schedule()
    start_minutes = (start_hour * 60) + start_minute
    end_minutes = (end_hour * 60) + end_minute
    current_minutes = (local_now.hour * 60) + local_now.minute

    if start_minutes == end_minutes:
        return local_now.date().isoformat()

    if start_minutes < end_minutes:
        if start_minutes <= current_minutes < end_minutes:
            return local_now.date().isoformat()
        return None

    if current_minutes >= start_minutes:
        return local_now.date().isoformat()
    if current_minutes < end_minutes:
        return (local_now.date() - timedelta(days=1)).isoformat()
    return None


def _normalize_daily_device_round_state(
    state: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    state_payload = state if isinstance(state, dict) else {}
    normalized_state = dict(state_payload)
    current_window_key = _daily_device_round_window_key(now)
    current_hospital_scope = _daily_device_round_hospital_scope()
    current_hospital_order = _daily_device_round_hospital_order()
    normalized_state["lastHospitalSeq"] = _coerce_int(state_payload.get("lastHospitalSeq"))
    normalized_state["nextHospitalSeq"] = _coerce_int(state_payload.get("nextHospitalSeq"))
    normalized_state["hospitalScope"] = current_hospital_scope
    normalized_state["hospitalOrder"] = current_hospital_order
    normalized_state["windowThreadTs"] = str(state_payload.get("windowThreadTs") or "").strip()
    normalized_state["windowThreadChannelId"] = str(state_payload.get("windowThreadChannelId") or "").strip()
    normalized_state["processedHospitalSeqs"] = _coerce_daily_device_round_hospital_seqs(
        state_payload.get("processedHospitalSeqs")
    )
    active_hospital_seq = _coerce_int(state_payload.get("activeHospitalSeq"))
    if active_hospital_seq is not None:
        normalized_state["activeHospitalSeq"] = active_hospital_seq
        normalized_state["activeHospitalName"] = str(state_payload.get("activeHospitalName") or "").strip()
        normalized_state["activeHospitalStartedAt"] = str(state_payload.get("activeHospitalStartedAt") or "").strip()
        active_hospital_device_count = _coerce_int(state_payload.get("activeHospitalDeviceCount"))
        if active_hospital_device_count is not None and active_hospital_device_count > 0:
            normalized_state["activeHospitalDeviceCount"] = active_hospital_device_count
        active_device_index = _coerce_int(state_payload.get("activeDeviceIndex"))
        if active_device_index is not None and active_device_index > 0:
            normalized_state["activeDeviceIndex"] = active_device_index
        active_device_name = str(state_payload.get("activeDeviceName") or "").strip()
        if active_device_name:
            normalized_state["activeDeviceName"] = active_device_name
        active_device_updated_at = str(state_payload.get("activeDeviceUpdatedAt") or "").strip()
        if active_device_updated_at:
            normalized_state["activeDeviceUpdatedAt"] = active_device_updated_at
    if not current_window_key:
        normalized_state["windowKey"] = None
        normalized_state.pop("windowCompletedAt", None)
        normalized_state["windowThreadTs"] = ""
        normalized_state["windowThreadChannelId"] = ""
        return _clear_daily_device_round_active_progress(normalized_state)

    previous_window_key = str(state_payload.get("windowKey") or "").strip()
    previous_hospital_scope_raw = str(state_payload.get("hospitalScope") or "").strip()
    previous_hospital_scope = (
        _normalize_daily_device_round_hospital_scope(previous_hospital_scope_raw)
        if previous_hospital_scope_raw
        else ""
    )
    previous_hospital_order_raw = str(state_payload.get("hospitalOrder") or "").strip()
    previous_hospital_order = (
        _normalize_daily_device_round_hospital_order(previous_hospital_order_raw)
        if previous_hospital_order_raw
        else ""
    )
    normalized_state["windowKey"] = current_window_key
    hospital_scope_changed = previous_hospital_scope != current_hospital_scope
    hospital_order_changed = previous_hospital_order != current_hospital_order
    if previous_window_key != current_window_key or hospital_scope_changed or hospital_order_changed:
        normalized_state["processedHospitalSeqs"] = []
        normalized_state.pop("windowCompletedAt", None)
        normalized_state["windowThreadTs"] = ""
        normalized_state["windowThreadChannelId"] = ""
        if hospital_scope_changed or hospital_order_changed:
            normalized_state["lastHospitalSeq"] = None
            normalized_state["nextHospitalSeq"] = None
        # Legacy fixed-target mode persisted the same hospital as both last/next.
        # Clear that self-loop on a new window so the first run can rotate forward.
        if normalized_state.get("nextHospitalSeq") == normalized_state.get("lastHospitalSeq"):
            normalized_state["nextHospitalSeq"] = None
        return _clear_daily_device_round_active_progress(normalized_state)
    return normalized_state


def _build_daily_device_round_window_title_text(now: datetime | None = None) -> str:
    local_now = _coerce_daily_device_round_now(now)
    return f"마미박스 일일 순회 업데이트 | {local_now:%Y-%m-%d}"


def _extract_daily_device_round_thread_ts(response: Any) -> str:
    thread_ts = str(getattr(response, "get", lambda *_args, **_kwargs: "")("ts") or "").strip()
    if thread_ts:
        return thread_ts
    response_data = getattr(response, "data", None)
    return str(
        getattr(response_data, "get", lambda *_args, **_kwargs: "")("ts") or ""
    ).strip()


def _daily_device_round_has_abnormal_result(report_summary: dict[str, Any]) -> bool:
    status_counts = report_summary.get("statusCounts") if isinstance(report_summary.get("statusCounts"), dict) else {}
    if (_coerce_int(status_counts.get("이상")) or 0) > 0:
        return True

    device_results = (
        report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []
    )
    return any(
        isinstance(item, dict) and _display_value(item.get("overallLabel"), default="") == "이상"
        for item in device_results
    )


def _load_daily_device_round_message_permalink(
    client: Any,
    *,
    channel_id: str,
    message_ts: str,
    logger: logging.Logger,
) -> str | None:
    normalized_channel_id = str(channel_id or "").strip()
    normalized_message_ts = str(message_ts or "").strip()
    if not normalized_channel_id or not normalized_message_ts:
        return None

    try:
        response = client.chat_getPermalink(
            channel=normalized_channel_id,
            message_ts=normalized_message_ts,
        )
    except Exception:
        logger.warning(
            "일일 장비 순회 이상 알림용 Slack permalink를 가져오지 못했어 channel=%s ts=%s",
            normalized_channel_id,
            normalized_message_ts,
            exc_info=True,
        )
        return None

    permalink = str(getattr(response, "get", lambda *_args, **_kwargs: "")("permalink") or "").strip()
    if permalink:
        return permalink
    response_data = getattr(response, "data", None)
    return str(getattr(response_data, "get", lambda *_args, **_kwargs: "")("permalink") or "").strip() or None


def _collect_daily_device_round_abnormal_alert_items(
    report_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    device_results = (
        report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []
    )
    default_hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    default_hospital_name = _display_value(report_summary.get("hospitalName"), default="병원 미확인")
    items: list[dict[str, Any]] = []

    for device_result in device_results:
        if not isinstance(device_result, dict):
            continue
        if _display_value(device_result.get("overallLabel"), default="") != "이상":
            continue
        hospital_seq = _coerce_int(device_result.get("hospitalSeq"))
        if hospital_seq is None:
            hospital_seq = default_hospital_seq
        hospital_name = _display_value(device_result.get("hospitalName"), default=default_hospital_name)
        # 루트 이상 알림만 보고도 병원 대표번호를 확인하고, 자동문자는 전용 휴대전화번호로만 판단하게 둘 다 전달한다.
        hospital_telephone = _display_value(
            device_result.get("hospitalTelephone"),
            default=_display_value(report_summary.get("hospitalTelephone"), default=""),
        )
        hospital_device_alert_phone = _display_value(
            device_result.get("hospitalDeviceAlertPhone"),
            default=_display_value(report_summary.get("hospitalDeviceAlertPhone"), default=""),
        )
        sms_status_text = _display_value(device_result.get("smsStatusText"), default="")
        sms_contact_action_enabled = _display_value(
            device_result.get("smsContactActionEnabled"),
            default="",
        )
        issue = _build_daily_device_round_issue_summary(device_result)
        if not issue:
            issue = _display_value(device_result.get("priorityReason"), default="상세 확인 필요")
        problem_components = _build_device_health_alert_problem_components(
            device_result,
            issue=issue,
        )
        alert_category = _resolve_device_health_alert_category(
            device_result,
            problem_components=problem_components,
            issue=issue,
        )
        device_name = _display_value(device_result.get("deviceName"), default="장비명 미확인")
        items.append(
            {
                "hospitalSeq": str(hospital_seq or ""),
                "hospitalName": hospital_name,
                "hospital": _format_daily_device_round_hospital_label(hospital_name, hospital_seq),
                "telephone": hospital_telephone,
                "deviceAlertPhone": hospital_device_alert_phone,
                "smsStatusText": sms_status_text,
                "smsContactActionEnabled": sms_contact_action_enabled,
                "smsPhoneNumber": _display_value(device_result.get("smsPhoneNumber"), default=""),
                "smsMessage": _display_value(device_result.get("smsMessage"), default=""),
                "smsTemplateId": _display_value(device_result.get("smsTemplateId"), default=""),
                "alertCategory": alert_category,
                "problemComponents": problem_components,
                "room": _display_value(device_result.get("roomName"), default="병실 미확인"),
                "device": device_name,
                "issue": issue,
                "mdaUrl": _build_daily_device_round_mda_monitoring_url(
                    device_name=device_name,
                    hospital_seq=hospital_seq,
                ),
                "mdaHospitalEditUrl": _build_daily_device_round_mda_hospital_edit_url(
                    hospital_name=hospital_name,
                ),
            }
        )

    return items


def _resolve_device_health_alert_category(
    device_result: dict[str, Any],
    *,
    problem_components: list[str],
    issue: str,
) -> str:
    explicit_category = _display_value(device_result.get("alertCategory"), default="")
    if explicit_category in _DEVICE_HEALTH_ALERT_CATEGORY_TITLES:
        return explicit_category

    component_labels = (
        device_result.get("componentLabels")
        if isinstance(device_result.get("componentLabels"), dict)
        else {}
    )
    # 확정 이상인 구성 요소를 우선하고, 레거시 payload만 문제 장치와 문구로 보강해.
    abnormal_categories = {
        category
        for component, category in _DEVICE_HEALTH_ALERT_COMPONENT_CATEGORIES.items()
        if _display_value(component_labels.get(component), default="") == "이상"
    }
    if len(abnormal_categories) == 1:
        return next(iter(abnormal_categories))
    if len(abnormal_categories) > 1:
        return "mixed"

    component_category_by_label = {
        "캡처보드": "video_signal",
        "LED": "led",
        "스피커": "audio",
        "PM2": "application",
    }
    legacy_categories = {
        component_category_by_label[component]
        for component in problem_components
        if component in component_category_by_label
    }
    if len(legacy_categories) == 1:
        return next(iter(legacy_categories))
    if len(legacy_categories) > 1:
        return "mixed"

    issue_text = _display_value(issue, default="")
    lowered_issue = issue_text.lower()
    # 명시적 코드나 구성 요소가 없는 과거 결과도 사용자 영향 기준 제목으로 분류해.
    keyword_categories = (
        ("recording_processing", ("병합", "ffmpeg")),
        ("upload", ("업로드", "upload")),
        ("recording", ("녹화", "recording")),
        ("storage", ("저장 공간", "디스크", "용량", "storage", "disk")),
        ("device_connection", ("오프라인", "연결 끊", "연결이 끊", "offline", "disconnect")),
        ("video_signal", ("캡처보드", "비디오 장치", "영상 신호", "captureboard")),
        ("led", ("led", "엘이디")),
        ("audio", ("스피커", "오디오", "소리", "audio", "speaker")),
        ("application", ("pm2", "프로세스")),
    )
    for category, keywords in keyword_categories:
        if any(keyword in lowered_issue for keyword in keywords):
            return category
    return ""


def _build_device_health_alert_title(alert_items: list[dict[str, Any]]) -> str:
    categories = [
        _display_value(item.get("alertCategory"), default="")
        for item in alert_items
    ]
    # 미분류 항목이나 여러 장애 유형이 섞이면 일부만 대표하지 않도록 공통 제목으로 낮춰.
    if not categories or any(not category for category in categories):
        return _DEVICE_HEALTH_ALERT_DEFAULT_TITLE
    unique_categories = set(categories)
    if len(unique_categories) != 1:
        return _DEVICE_HEALTH_ALERT_DEFAULT_TITLE
    return _DEVICE_HEALTH_ALERT_CATEGORY_TITLES.get(
        next(iter(unique_categories)),
        _DEVICE_HEALTH_ALERT_DEFAULT_TITLE,
    )


def _format_device_health_alert_header(
    alert_items: list[dict[str, Any]],
    *,
    mrkdwn: bool,
) -> str:
    title = _build_device_health_alert_title(alert_items)
    return f":alert: *{title}*" if mrkdwn else f":alert: {title}"


def _build_device_health_alert_problem_components(
    device_result: dict[str, Any],
    *,
    issue: str,
) -> list[str]:
    # componentLabels가 있으면 실제 점검 결과를 우선하고, 테스트/레거시 payload는 이슈 문구로 보강한다.
    component_labels = (
        device_result.get("componentLabels")
        if isinstance(device_result.get("componentLabels"), dict)
        else {}
    )
    components: list[str] = []
    for key in _DEVICE_HEALTH_ALERT_COMPONENT_ORDER:
        label = _display_value(component_labels.get(key), default="")
        if label and label != "정상":
            components.append(_DEVICE_HEALTH_ALERT_COMPONENT_NAMES.get(key, key))

    issue_text = _display_value(issue, default="")
    lowered_issue = issue_text.lower()
    keyword_components = [
        ("캡처보드", ("캡처보드", "비디오 장치", "영상")),
        ("LED", ("led", "엘이디", "LED")),
        ("스피커", ("audio", "sound", "speaker", "오디오", "소리", "스피커")),
        ("PM2", ("pm2", "프로세스")),
    ]
    for component, keywords in keyword_components:
        if component in components:
            continue
        if any(keyword in lowered_issue or keyword in issue_text for keyword in keywords):
            components.append(component)
    return components


def _format_device_health_alert_problem_components(components: Any) -> str:
    if not isinstance(components, list):
        return ""
    labels = [
        _display_value(component, default="")
        for component in components
        if _display_value(component, default="")
    ]
    return " ".join(f"`{label}`" for label in labels)


def _is_device_health_alert_contact_action_enabled(item: dict[str, Any]) -> bool:
    raw_value = _display_value(item.get("smsContactActionEnabled"), default="")
    if not raw_value:
        return True
    return raw_value.lower() not in {"0", "false", "no", "off"}


def _build_daily_device_round_mda_monitoring_url(
    *,
    device_name: str,
    hospital_seq: int | None,
) -> str:
    normalized_device_name = _display_value(device_name, default="")
    if not normalized_device_name or hospital_seq is None:
        return ""

    query = urlencode(
        {
            "focusDevice": normalized_device_name,
            "hospitalSeq": int(hospital_seq),
        }
    )
    return f"{cs.MDA_GRAPHQL_ORIGIN.rstrip('/')}/monitoring?{query}"


def _build_daily_device_round_mda_hospital_edit_url(*, hospital_name: str) -> str:
    normalized_hospital_name = _display_value(hospital_name, default="")
    if not normalized_hospital_name or normalized_hospital_name == "병원 미확인":
        return ""

    # MDA 병원 리스트는 search query를 병원명 LIKE 검색으로 처리하므로 병원명을 바로 넘긴다.
    query = urlencode({"search": normalized_hospital_name})
    return f"{cs.MDA_GRAPHQL_ORIGIN.rstrip('/')}/hospital/list?{query}"


def _format_device_health_alert_device_name(item: dict[str, Any]) -> str:
    device_name = _display_value(item.get("device"), default="장비명 미확인")
    mda_url = _display_value(item.get("mdaUrl"), default="")
    # MDA 확인 목적지가 장비 자체이므로 별도 CTA 대신 장비명에 링크를 직접 걸어.
    if mda_url:
        return f"*<{mda_url}|{device_name}>*"
    return f"`{device_name}`"


def _format_mda_hospital_contact_link(mda_url: str) -> str:
    # 문자 라인 안에서 짧은 CTA만 보여주고, 목적지는 병원 정보 수정 화면으로 보낸다.
    return f"<{mda_url}|번호 추가하기>"


def _format_device_health_alert_sms_contact_value(item: dict[str, Any]) -> str:
    device_alert_phone = _display_value(
        item.get("deviceAlertPhone"),
        default=_display_value(item.get("smsPhoneNumber"), default=""),
    )
    # 전화와 같은 2열 필드에 넣을 수 있도록 문자 값과 보조 링크만 반환해.
    if device_alert_phone:
        return device_alert_phone

    mda_hospital_edit_url = _display_value(item.get("mdaHospitalEditUrl"), default="")
    if mda_hospital_edit_url:
        return (
            "저장된 번호 없음 · 자동발송 불가\n"
            f"{_format_mda_hospital_contact_link(mda_hospital_edit_url)}"
        )
    return "저장된 번호 없음 · 자동발송 불가"


def _build_device_health_alert_item_text_lines(item: dict[str, Any]) -> list[str]:
    # 식별 정보, 장애 내용, 연락처, 행동 순으로 묶어 Slack에서 빠르게 훑을 수 있게 해.
    lines = [
        f"*{item['hospital']}*",
        f"⚙️ *장비*  {_format_device_health_alert_device_name(item)}  ·  🚪 *병실*  `{item['room']}`",
        "",
    ]
    problem_components = _format_device_health_alert_problem_components(
        item.get("problemComponents")
    )
    if problem_components:
        # 장애 부품이 병원·장비 식별 정보 다음으로 가장 먼저 눈에 들어오게 강조해.
        lines.extend(
            [
                f":rotating_light: *문제 장치*\n{problem_components}",
                f"🔎 *감지 내용*\n`{item['issue']}`",
            ]
        )
    else:
        lines.append(f"🔎 *감지 내용*\n`{item['issue']}`")
    lines.extend(
        [
            "",
            f"📞 *전화*\n{_display_value(item.get('telephone'), default='미확인')}",
            f"💬 *문자*\n{_format_device_health_alert_sms_contact_value(item)}",
        ]
    )
    return lines


def _is_device_health_alert_auto_sms_status_button_enabled(item: dict[str, Any]) -> bool:
    return _display_value(item.get("smsStatusText"), default="") == _DEVICE_HEALTH_ALERT_SMS_AUTO_SENT_TEXT


def _build_daily_device_round_abnormal_alert_text(
    report_summary: dict[str, Any],
    permalink: str | None,
) -> str:
    alert_items = _collect_daily_device_round_abnormal_alert_items(report_summary)
    # Slack 커스텀 경고 이모지는 유지하고, 제목은 감지 유형의 사용자 영향에 맞춰 보여줘.
    lines = [_format_device_health_alert_header(alert_items, mrkdwn=True)]
    if alert_items:
        # 실제 block과 fallback text가 같은 정보 구조를 유지하게 공통 formatter를 사용해.
        for item in alert_items:
            if len(lines) > 1:
                lines.append("")
            lines.extend(_build_device_health_alert_item_text_lines(item))
    if permalink:
        if alert_items:
            lines.append("")
        lines.append(f":link: <{permalink}|상세 리포트 보기>")
    return "\n".join(lines)


def _build_device_health_alert_action_value(item: dict[str, Any]) -> str:
    payload = {
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
        "mdaHospitalEditUrl": _display_value(item.get("mdaHospitalEditUrl"), default=""),
    }
    value = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    if len(value) <= 1900:
        return value

    # Slack button value는 길이 제한이 있어 긴 이슈 문구만 줄이고 식별 정보는 보존해.
    payload["issue"] = payload["issue"][:300]
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))[:1900]


def _build_device_health_alert_item_blocks(
    item: dict[str, Any],
    *,
    include_actions: bool = True,
) -> list[dict[str, Any]]:
    problem_components = _format_device_health_alert_problem_components(
        item.get("problemComponents")
    )
    # 문제 장치와 감지 내용도 장비·병실처럼 라벨 아래 값을 두는 2열로 맞춰.
    issue_fields = []
    if problem_components:
        issue_fields.append(
            {"type": "mrkdwn", "text": f":rotating_light: *문제 장치*\n{problem_components}"}
        )
    issue_fields.append(
        {"type": "mrkdwn", "text": f"🔎 *감지 내용*\n`{item['issue']}`"}
    )

    contact_fields = [
        {
            "type": "mrkdwn",
            "text": f"📞 *전화*\n{_display_value(item.get('telephone'), default='미확인')}",
        },
        {
            "type": "mrkdwn",
            "text": f"💬 *문자*\n{_format_device_health_alert_sms_contact_value(item)}",
        },
    ]

    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{item['hospital']}*"},
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"⚙️ *장비*\n{_format_device_health_alert_device_name(item)}",
                },
                {"type": "mrkdwn", "text": f"🚪 *병실*\n`{item['room']}`"},
            ],
        },
        {
            "type": "section",
            "fields": issue_fields,
        },
        {
            "type": "section",
            "fields": contact_fields,
        },
    ]
    if not include_actions:
        # 장비 이벤트 알림도 같은 카드 레이아웃을 쓰되 모니터 전용 조치 버튼은 노출하지 않는다.
        return blocks

    sms_status_text = _display_value(item.get("smsStatusText"), default="")
    sms_status_button_enabled = _is_device_health_alert_auto_sms_status_button_enabled(item)
    action_value = _build_device_health_alert_action_value(item)
    action_elements: list[dict[str, Any]] = []
    if sms_status_button_enabled:
        # 자동발송 완료는 재발송 버튼 대신 확인 버튼으로 노출해 실제 발송 번호와 문구를 다시 볼 수 있게 한다.
        sms_status_action_value = _build_device_health_alert_action_value(
            {**item, "smsModalMode": _DEVICE_HEALTH_ALERT_SMS_MODAL_MODE_VIEW_AUTO_SENT}
        )
        action_elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": sms_status_text},
                "action_id": _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                "value": sms_status_action_value,
                "style": "primary",
            }
        )
    elif _is_device_health_alert_contact_action_enabled(item):
        # 자동 발송이 끝난 장비는 같은 문자가 중복 발송되지 않도록 수동 문자 버튼을 숨긴다.
        action_elements.append(
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "병원 문자 보내기"},
                "action_id": _DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                "value": action_value,
                "style": "primary",
            }
        )
    action_elements.append(
        {
            "type": "button",
            "text": {"type": "plain_text", "text": "장비 음성 안내(미구현)"},
            "action_id": _DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
            "value": action_value,
        }
    )
    blocks.append({"type": "actions", "elements": action_elements})
    return blocks


def _build_daily_device_round_abnormal_alert_blocks(
    report_summary: dict[str, Any],
    permalink: str | None,
    *,
    include_actions: bool = True,
) -> list[dict[str, Any]]:
    alert_items = _collect_daily_device_round_abnormal_alert_items(report_summary)
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": _format_device_health_alert_header(alert_items, mrkdwn=False),
                "emoji": True,
            },
        }
    ]

    # 장비별 카드는 Slack block 제한을 넘지 않게 상위 일부만 노출해.
    for item in alert_items[:_DEVICE_HEALTH_ALERT_ACTION_ITEM_LIMIT]:
        blocks.extend(
            _build_device_health_alert_item_blocks(
                item,
                include_actions=include_actions,
            )
        )

    omitted_count = max(0, len(alert_items) - _DEVICE_HEALTH_ALERT_ACTION_ITEM_LIMIT)
    context_parts: list[dict[str, str]] = []
    if omitted_count:
        context_parts.append(
            {
                "type": "mrkdwn",
                "text": f"알림 카드는 상위 {_DEVICE_HEALTH_ALERT_ACTION_ITEM_LIMIT}건만 표시했어. 나머지 {omitted_count}건은 본문에서 확인해.",
            }
        )
    if permalink:
        context_parts.append({"type": "mrkdwn", "text": f":link: <{permalink}|상세 리포트 보기>"})
    if context_parts:
        blocks.append({"type": "context", "elements": context_parts})
    return blocks


def _post_daily_device_round_abnormal_alert(
    client: Any,
    report_summary: dict[str, Any],
    *,
    channel_id: str,
    message_ts: str,
    logger: logging.Logger,
    include_blocks: bool = False,
    include_actions: bool = False,
) -> dict[str, str] | None:
    if not _daily_device_round_has_abnormal_result(report_summary):
        return

    permalink = _load_daily_device_round_message_permalink(
        client,
        channel_id=channel_id,
        message_ts=message_ts,
        logger=logger,
    )
    try:
        # 이상 장비가 스레드 안에 묻히지 않도록 같은 채널 루트에도 확인 알림을 남겨.
        message_kwargs: dict[str, Any] = {
            "channel": channel_id,
            "text": _build_daily_device_round_abnormal_alert_text(report_summary, permalink),
            "unfurl_links": False,
            "unfurl_media": False,
        }
        # Block Kit 레이아웃과 조치 버튼을 독립적으로 켜 장비 이벤트도 공통 카드 형식을 재사용한다.
        if include_blocks or include_actions:
            message_kwargs["blocks"] = _build_daily_device_round_abnormal_alert_blocks(
                report_summary,
                permalink,
                include_actions=include_actions,
            )
        response = client.chat_postMessage(**message_kwargs)
        posted_message_ts = _extract_daily_device_round_thread_ts(response)
        posted_permalink = _load_daily_device_round_message_permalink(
            client,
            channel_id=channel_id,
            message_ts=posted_message_ts,
            logger=logger,
        )
        return {
            "channelId": str(channel_id or "").strip(),
            "messageTs": posted_message_ts,
            "permalink": str(posted_permalink or "").strip(),
        }
    except Exception:
        logger.warning(
            "일일 장비 순회 이상 알림을 보내지 못했어 channel=%s message_ts=%s",
            channel_id,
            message_ts,
            exc_info=True,
        )
        return None


def _is_daily_device_round_due(
    now: datetime | None,
    state: dict[str, Any],
) -> bool:
    if _daily_device_round_window_key(now) is None:
        return False
    normalized_state = _normalize_daily_device_round_state(state, now=now)
    return not str(normalized_state.get("windowCompletedAt") or "").strip()


def _run_daily_device_round_if_due(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> bool:
    if not cs.DAILY_DEVICE_ROUND_ENABLED:
        return False
    if not s.DB_QUERY_ENABLED:
        logger.warning("일일 장비 순회를 켤 수 없어. DB_QUERY_ENABLED가 비활성이야")
        return False
    if not _is_daily_device_round_runtime_configured():
        logger.warning("일일 장비 순회를 켤 수 없어. MDA/SSH 설정이 부족해")
        return False

    channel_id = str(cs.DAILY_DEVICE_ROUND_CHANNEL_ID or "").strip()
    if not channel_id:
        logger.warning("일일 장비 순회 채널 ID가 없어. DAILY_DEVICE_ROUND_CHANNEL_ID를 확인해줘")
        return False

    local_now = _coerce_daily_device_round_now(now)
    raw_state = _load_daily_device_round_state(logger=logger)
    state = _normalize_daily_device_round_state(raw_state, now=local_now)
    if not _is_daily_device_round_due(local_now, state):
        return False

    window_key = _daily_device_round_window_key(local_now)
    thread_ts = str(state.get("windowThreadTs") or "").strip()
    thread_channel_id = str(state.get("windowThreadChannelId") or "").strip()

    def _ensure_daily_device_round_thread() -> str:
        nonlocal state, thread_ts, thread_channel_id
        if thread_ts and thread_channel_id == channel_id:
            return thread_ts

        title_response = client.chat_postMessage(
            channel=channel_id,
            text=_build_daily_device_round_window_title_text(local_now),
            unfurl_links=False,
            unfurl_media=False,
        )
        thread_ts = _extract_daily_device_round_thread_ts(title_response)
        if not thread_ts:
            raise RuntimeError("일일 장비 순회 제목 메시지 ts를 받지 못했어")
        thread_channel_id = channel_id
        # 병원 점검이 오래 걸리더라도 제목 스레드는 먼저 확보해서 진행 상황이 보이게 해.
        state = _persist_daily_device_round_state_best_effort(
            {
                **state,
                "windowKey": window_key,
                "hospitalScope": _daily_device_round_hospital_scope(),
                "hospitalOrder": _daily_device_round_hospital_order(),
                "windowThreadTs": thread_ts,
                "windowThreadChannelId": channel_id,
                "channelId": channel_id,
            },
            now=local_now,
            logger=logger,
        )
        return thread_ts

    def _persist_active_progress(
        *,
        hospital_seq: int | None,
        hospital_name: str | None = None,
        hospital_started_at: str | None = None,
        hospital_device_count: int | None = None,
        device_index: int | None = None,
        device_name: str | None = None,
        device_updated_at: str | None = None,
    ) -> None:
        nonlocal state
        _ensure_daily_device_round_thread()
        # 재시작 후에도 현재 병원/장비를 이어갈 수 있게 active progress를 중간 저장해.
        state = _persist_daily_device_round_state_best_effort(
            _merge_daily_device_round_active_progress(
                {
                    **state,
                    "windowKey": window_key,
                    "hospitalScope": _daily_device_round_hospital_scope(),
                    "hospitalOrder": _daily_device_round_hospital_order(),
                    "windowThreadTs": thread_ts,
                    "windowThreadChannelId": channel_id,
                    "channelId": channel_id,
                },
                hospital_seq=hospital_seq,
                hospital_name=hospital_name,
                hospital_started_at=hospital_started_at,
                hospital_device_count=hospital_device_count,
                device_index=device_index,
                device_name=device_name,
                device_updated_at=device_updated_at,
            ),
            now=local_now,
            logger=logger,
        )

    def _handle_daily_device_round_progress(event: str, payload: dict[str, Any]) -> None:
        if event == "hospital_started":
            _persist_active_progress(
                hospital_seq=_coerce_int(payload.get("hospitalSeq")),
                hospital_name=_display_value(payload.get("hospitalName"), default=""),
                hospital_started_at=_display_value(payload.get("startedAt"), default=""),
                hospital_device_count=_coerce_int(payload.get("deviceCount")),
            )
            return
        if event == "device_started":
            _persist_active_progress(
                hospital_seq=_coerce_int(payload.get("hospitalSeq")),
                hospital_name=_display_value(
                    payload.get("hospitalName"),
                    default=_display_value(state.get("activeHospitalName"), default=""),
                ),
                hospital_started_at=_display_value(
                    state.get("activeHospitalStartedAt"),
                    default=_display_value(payload.get("updatedAt"), default=""),
                ),
                hospital_device_count=_coerce_int(payload.get("deviceCount")),
                device_index=_coerce_int(payload.get("deviceIndex")),
                device_name=_display_value(payload.get("deviceName"), default=""),
                device_updated_at=_display_value(payload.get("updatedAt"), default=""),
            )

    report_summary = _build_daily_device_round_summary(
        now=local_now,
        state=state,
        auto_update_agent=_resolve_daily_device_round_auto_update_agent(state),
        auto_update_box=_resolve_daily_device_round_auto_update_box(state),
        auto_cleanup_trashcan=bool(cs.DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN),
        auto_power_off=bool(cs.DAILY_DEVICE_ROUND_AUTO_POWER_OFF),
        progress_callback=_handle_daily_device_round_progress,
    )
    base_state = _clear_daily_device_round_active_progress(state)
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    processed_hospital_seqs = _coerce_daily_device_round_hospital_seqs(base_state.get("processedHospitalSeqs"))
    if hospital_seq is not None and hospital_seq not in processed_hospital_seqs:
        processed_hospital_seqs.append(hospital_seq)
    candidate_hospital_count = max(0, int(report_summary.get("candidateHospitalCount") or 0))
    next_state = {
        **base_state,
        "windowKey": window_key,
        "hospitalScope": _daily_device_round_hospital_scope(),
        "hospitalOrder": _daily_device_round_hospital_order(),
        "processedHospitalSeqs": processed_hospital_seqs,
        "windowThreadTs": thread_ts,
        "windowThreadChannelId": thread_channel_id,
        "windowCompletedAt": (
            local_now.isoformat()
            if hospital_seq is None or (
                candidate_hospital_count > 0
                and len(processed_hospital_seqs) >= candidate_hospital_count
            )
            else ""
        ),
        "lastRunDate": _coerce_daily_device_round_now(local_now).date().isoformat(),
        "lastHospitalSeq": report_summary.get("hospitalSeq"),
        "lastHospitalName": report_summary.get("hospitalName"),
        "nextHospitalSeq": report_summary.get("nextHospitalSeq"),
        "lastSentAt": local_now.isoformat(),
        "channelId": channel_id,
        "statusCounts": report_summary.get("statusCounts"),
        "updateCounts": report_summary.get("updateCounts"),
        "cleanupCounts": report_summary.get("cleanupCounts"),
        "powerCounts": report_summary.get("powerCounts"),
    }
    if hospital_seq is None:
        _remember_daily_device_round_runtime_state(next_state, now=local_now)
        _save_daily_device_round_state(next_state)
        logger.info(
            "Daily device round window paused channel=%s windowKey=%s reason=%s",
            channel_id,
            next_state.get("windowKey"),
            report_summary.get("summaryLine"),
        )
        return False

    message_text = _build_daily_device_round_report_text(report_summary, now=local_now)
    message_blocks = _build_daily_device_round_blocks(
        report_summary,
        now=local_now,
        include_header=False,
    )
    message_block_chunks = _split_daily_device_round_blocks(message_blocks)
    thread_ts = _ensure_daily_device_round_thread()
    next_state["windowThreadTs"] = thread_ts
    next_state["windowThreadChannelId"] = channel_id

    try:
        for index, block_chunk in enumerate(message_block_chunks):
            client.chat_postMessage(
                channel=channel_id,
                text=_build_daily_device_round_chunk_text(
                    message_text,
                    chunk_index=index,
                    chunk_count=len(message_block_chunks),
                ),
                blocks=block_chunk,
                thread_ts=thread_ts,
                unfurl_links=False,
                unfurl_media=False,
            )
    except Exception:
        # rich_text/section 길이 같은 block payload 오류가 나면 plain text 전체 리포트로라도 남겨.
        logger.warning(
            "일일 장비 순회 block 전송이 실패해서 text fallback으로 다시 보낼게 channel=%s thread_ts=%s hospitalSeq=%s",
            channel_id,
            thread_ts,
            hospital_seq,
            exc_info=True,
        )
        fallback_text_chunks = _split_daily_device_round_text(
            _format_daily_device_round_report(
                report_summary,
                now=local_now,
                include_title=False,
            )
        )
        for index, text_chunk in enumerate(fallback_text_chunks):
            text_body = text_chunk
            if len(fallback_text_chunks) > 1:
                text_body = f"(계속 {index + 1}/{len(fallback_text_chunks)})\n{text_chunk}"
            client.chat_postMessage(
                channel=channel_id,
                text=text_body,
                thread_ts=thread_ts,
                unfurl_links=False,
                unfurl_media=False,
            )
    _remember_daily_device_round_runtime_state(next_state, now=local_now)
    _save_daily_device_round_state(next_state)
    logger.info(
        "Posted daily device round channel=%s hospitalSeq=%s hospitalName=%s deviceCount=%s windowKey=%s processed=%s/%s",
        channel_id,
        report_summary.get("hospitalSeq"),
        report_summary.get("hospitalName"),
        report_summary.get("deviceCount"),
        next_state.get("windowKey"),
        len(processed_hospital_seqs),
        candidate_hospital_count,
    )
    return True


def _build_daily_device_round_report_text(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
) -> str:
    from boxer_company.daily_device_round import _format_daily_device_round_hospital_label

    hospital_label = _format_daily_device_round_hospital_label(
        report_summary.get("hospitalName"),
        _coerce_int(report_summary.get("hospitalSeq")),
    )
    status_counts = report_summary.get("statusCounts") if isinstance(report_summary.get("statusCounts"), dict) else {}
    update_counts = report_summary.get("updateCounts") if isinstance(report_summary.get("updateCounts"), dict) else {}
    cleanup_counts = report_summary.get("cleanupCounts") if isinstance(report_summary.get("cleanupCounts"), dict) else {}
    power_counts = report_summary.get("powerCounts") if isinstance(report_summary.get("powerCounts"), dict) else {}
    summary_line = _display_value(report_summary.get("summaryLine"), default="점검 결과")

    if _coerce_int(report_summary.get("hospitalSeq")) is None:
        return summary_line

    executed_parts: list[str] = []

    agent_updated = int(update_counts.get("agentUpdated") or 0)
    agent_failed = int(update_counts.get("agentUpdateFailed") or 0)
    box_updated = int(update_counts.get("boxUpdated") or 0)
    box_failed = int(update_counts.get("boxUpdateFailed") or 0)
    update_parts: list[str] = []
    if agent_updated or agent_failed:
        item = f"에이전트 {agent_updated}"
        if agent_failed:
            item = f"{item} 실패 {agent_failed}"
        update_parts.append(item)
    if box_updated or box_failed:
        item = f"박스 {box_updated}"
        if box_failed:
            item = f"{item} 실패 {box_failed}"
        update_parts.append(item)
    if update_parts:
        executed_parts.append("업데이트 " + " / ".join(update_parts))

    cleanup_executed = int(cleanup_counts.get("executed") or 0)
    cleanup_failed = int(cleanup_counts.get("failed") or 0)
    if cleanup_executed or cleanup_failed:
        cleanup_text = f"정리 실행 {cleanup_executed}"
        if cleanup_failed:
            cleanup_text = f"{cleanup_text} / 실패 {cleanup_failed}"
        executed_parts.append(cleanup_text)

    power_completed = int(power_counts.get("poweredOff") or 0)
    power_already_offline = int(power_counts.get("alreadyOffline") or 0)
    power_failed = int(power_counts.get("powerOffFailed") or 0)
    if power_completed or power_already_offline or power_failed:
        power_text = f"장비 종료 {power_completed}"
        if power_already_offline:
            power_text = f"{power_text} / 생략 {power_already_offline}"
        if power_failed:
            power_text = f"{power_text} / 실패 {power_failed}"
        executed_parts.append(power_text)

    if executed_parts:
        return f"{hospital_label} | {' | '.join(executed_parts)}"

    return hospital_label


def _daily_device_round_loop(client: Any, logger: logging.Logger) -> None:
    poll_interval_sec = max(5, int(cs.DAILY_DEVICE_ROUND_POLL_INTERVAL_SEC))
    while True:
        try:
            _run_daily_device_round_if_due(client, logger)
        except Exception:
            logger.exception("일일 장비 순회 중 오류가 발생했어")
        time.sleep(poll_interval_sec)


def attach_daily_device_round_reporter(app: Any, *, logger: logging.Logger | None = None) -> None:
    if not cs.DAILY_DEVICE_ROUND_ENABLED:
        return

    actual_logger = logger or logging.getLogger(__name__)
    if not s.DB_QUERY_ENABLED:
        actual_logger.warning("일일 장비 순회가 활성화됐는데 DB_QUERY_ENABLED가 꺼져 있어 시작하지 않을게")
        return
    if not _is_daily_device_round_runtime_configured():
        actual_logger.warning("일일 장비 순회가 활성화됐는데 MDA/SSH 설정이 부족해 시작하지 않을게")
        return

    channel_id = str(cs.DAILY_DEVICE_ROUND_CHANNEL_ID or "").strip()
    if not channel_id:
        actual_logger.warning(
            "일일 장비 순회가 활성화됐는데 채널 ID가 없어. DAILY_DEVICE_ROUND_CHANNEL_ID를 확인해줘"
        )
        return

    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("일일 장비 순회를 시작하지 못했어. Slack client가 없어")
        return

    global _DAILY_DEVICE_ROUND_THREAD
    with _DAILY_DEVICE_ROUND_THREAD_LOCK:
        if _DAILY_DEVICE_ROUND_THREAD is not None and _DAILY_DEVICE_ROUND_THREAD.is_alive():
            return
        _DAILY_DEVICE_ROUND_THREAD = threading.Thread(
            target=_daily_device_round_loop,
            args=(client, actual_logger),
            name="daily-device-round",
            daemon=True,
        )
        _DAILY_DEVICE_ROUND_THREAD.start()

    local_tz = _daily_device_round_timezone()
    (start_hour, start_minute), (end_hour, end_minute) = _daily_device_round_window_schedule()
    actual_logger.info(
        "Started daily device round scheduler channel=%s every day from %02d:%02d to %02d:%02d %s",
        channel_id,
        start_hour,
        start_minute,
        end_hour,
        end_minute,
        local_tz.key,
    )
