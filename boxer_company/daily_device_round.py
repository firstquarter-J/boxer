from collections.abc import Callable
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.core.utils import _display_value, _format_size
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company import settings as cs
from boxer_company.routers.device_status_probe import (
    _probe_device_status_overview,
    _run_device_trashcan_cleanup,
)
from boxer_company.routers.device_update import (
    _describe_agent_box_update_gate,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
    _request_device_power_off,
    _resolve_agent_runtime_version,
)

_DAILY_DEVICE_ROUND_TIMEZONE = ZoneInfo("Asia/Seoul")
_DAILY_DEVICE_ROUND_TITLE = "일일 장비 순회 점검 & 업데이트"
_DAILY_DEVICE_ROUND_COMPONENT_NAMES = {
    "audio": "오디오",
    "pm2": "pm2",
    "storage": "용량",
    "captureboard": "캡처보드",
    "led": "LED",
}
_DailyDeviceRoundProgressCallback = Callable[[str, dict[str, Any]], None]


def _daily_device_round_timezone() -> ZoneInfo:
    return _DAILY_DEVICE_ROUND_TIMEZONE


def _coerce_daily_device_round_now(now: datetime | None = None) -> datetime:
    report_tz = _daily_device_round_timezone()
    if now is None:
        return datetime.now(report_tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=report_tz)
    return now.astimezone(report_tz)


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _coerce_daily_device_round_hospital_seqs(value: Any) -> list[int]:
    if not isinstance(value, list):
        return []

    items: list[int] = []
    seen: set[int] = set()
    for raw in value:
        hospital_seq = _coerce_int(raw)
        if hospital_seq is None or hospital_seq in seen:
            continue
        items.append(hospital_seq)
        seen.add(hospital_seq)
    return items


def _is_daily_device_round_excluded_hospital_name(hospital_name: Any) -> bool:
    normalized_name = _display_value(hospital_name, default="").strip()
    if not normalized_name:
        return False
    return len(normalized_name) >= 2 and normalized_name[0] in "01234567" and normalized_name[1] == "_"


def _load_daily_device_round_hospital_candidates() -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT "
                "d.hospitalSeq AS hospitalSeq, "
                "h.hospitalName AS hospitalName, "
                "COUNT(DISTINCT d.deviceName) AS deviceCount "
                "FROM devices d "
                "INNER JOIN hospitals h ON d.hospitalSeq = h.seq "
                "WHERE d.hospitalSeq IS NOT NULL "
                "AND COALESCE(d.deviceName, '') <> '' "
                "AND COALESCE(d.activeFlag, 1) = 1 "
                "AND COALESCE(d.installFlag, 1) = 1 "
                "GROUP BY d.hospitalSeq, h.hospitalName "
                "ORDER BY d.hospitalSeq ASC"
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    items: list[dict[str, Any]] = []
    for row in rows:
        hospital_seq = _coerce_int(row.get("hospitalSeq"))
        if hospital_seq is None:
            continue
        hospital_name = _display_value(row.get("hospitalName"), default="미확인")
        if _is_daily_device_round_excluded_hospital_name(hospital_name):
            continue
        items.append(
            {
                "hospitalSeq": hospital_seq,
                "hospitalName": hospital_name,
                "deviceCount": int(row.get("deviceCount") or 0),
            }
        )
    return items


def _select_daily_device_round_hospital(
    candidates: list[dict[str, Any]],
    state: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    if not candidates:
        return None

    normalized_candidates = sorted(
        [item for item in candidates if _coerce_int(item.get("hospitalSeq")) is not None],
        key=lambda item: int(item.get("hospitalSeq") or 0),
    )
    if not normalized_candidates:
        return None

    state_payload = state if isinstance(state, dict) else {}
    processed_hospital_seqs = set(_coerce_daily_device_round_hospital_seqs(state_payload.get("processedHospitalSeqs")))
    selectable_candidates = [
        item
        for item in normalized_candidates
        if int(item.get("hospitalSeq") or 0) not in processed_hospital_seqs
    ]
    if not selectable_candidates:
        return None

    active_hospital_seq = _coerce_int(state_payload.get("activeHospitalSeq"))
    if active_hospital_seq is not None:
        # 병원 처리 중 재시작되면 진행 중이던 병원부터 다시 이어가도록 active 병원을 우선해.
        for item in selectable_candidates:
            if int(item.get("hospitalSeq") or 0) == active_hospital_seq:
                return item

    next_hospital_seq = _coerce_int(state_payload.get("nextHospitalSeq"))
    if next_hospital_seq is not None:
        for item in selectable_candidates:
            if int(item.get("hospitalSeq") or 0) == next_hospital_seq:
                return item

    last_hospital_seq = _coerce_int(state_payload.get("lastHospitalSeq"))
    if last_hospital_seq is None:
        return selectable_candidates[0]

    for item in selectable_candidates:
        if int(item.get("hospitalSeq") or 0) > last_hospital_seq:
            return item
    return selectable_candidates[0]


def _resolve_next_daily_device_round_hospital_seq(
    candidates: list[dict[str, Any]],
    current_hospital_seq: int | None,
) -> int | None:
    normalized_candidates = sorted(
        [item for item in candidates if _coerce_int(item.get("hospitalSeq")) is not None],
        key=lambda item: int(item.get("hospitalSeq") or 0),
    )
    if not normalized_candidates:
        return None
    if current_hospital_seq is None:
        return int(normalized_candidates[0]["hospitalSeq"])

    for index, item in enumerate(normalized_candidates):
        if int(item.get("hospitalSeq") or 0) != current_hospital_seq:
            continue
        next_index = (index + 1) % len(normalized_candidates)
        return int(normalized_candidates[next_index]["hospitalSeq"])
    return int(normalized_candidates[0]["hospitalSeq"])


def _load_daily_device_round_devices(hospital_seq: int) -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT "
                "d.seq AS deviceSeq, "
                "d.deviceName AS deviceName, "
                "d.hospitalSeq AS hospitalSeq, "
                "d.hospitalRoomSeq AS hospitalRoomSeq, "
                "h.hospitalName AS hospitalName, "
                "hr.roomName AS roomName "
                "FROM devices d "
                "INNER JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                "WHERE d.hospitalSeq = %s "
                "AND COALESCE(d.deviceName, '') <> '' "
                "AND COALESCE(d.activeFlag, 1) = 1 "
                "AND COALESCE(d.installFlag, 1) = 1 "
                "ORDER BY COALESCE(hr.roomName, '') ASC, d.deviceName ASC, d.seq DESC",
                (int(hospital_seq),),
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
        seen_device_names.add(device_name)
        items.append(
            {
                "deviceSeq": _coerce_int(row.get("deviceSeq")),
                "deviceName": device_name,
                "hospitalSeq": _coerce_int(row.get("hospitalSeq")),
                "hospitalRoomSeq": _coerce_int(row.get("hospitalRoomSeq")),
                "hospitalName": _display_value(row.get("hospitalName"), default="미확인"),
                "roomName": _display_value(row.get("roomName"), default="미확인"),
            }
        )
    return items


def _daily_device_round_status_label(status_payload: dict[str, Any]) -> str:
    ssh_payload = status_payload.get("ssh") if isinstance(status_payload.get("ssh"), dict) else {}
    if not ssh_payload.get("ready"):
        return "점검 불가"

    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    worst_rank = 0
    for key in ("audio", "pm2", "storage", "captureboard", "led"):
        component = overview.get(key) if isinstance(overview.get(key), dict) else {}
        state = _display_value(component.get("status"), default="check_needed")
        if state == "fail":
            worst_rank = max(worst_rank, 2)
        elif state != "pass":
            worst_rank = max(worst_rank, 1)

    if worst_rank >= 2:
        return "이상"
    if worst_rank == 1:
        return "확인 필요"
    return "정상"


def _format_daily_device_round_component_names(keys: list[str]) -> str:
    return "/".join(
        _DAILY_DEVICE_ROUND_COMPONENT_NAMES.get(key, key)
        for key in keys
        if key
    )


def _build_daily_device_round_priority(status_payload: dict[str, Any]) -> dict[str, Any]:
    ssh_payload = status_payload.get("ssh") if isinstance(status_payload.get("ssh"), dict) else {}
    if not ssh_payload.get("ready"):
        return {
            "eligible": False,
            "score": -1,
            "label": "판단 보류",
            "reason": "네트워크 연결 불가로 이상 징후 판단 보류",
        }

    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    failed_keys: list[str] = []
    warning_keys: list[str] = []
    for key in ("audio", "pm2", "storage", "captureboard", "led"):
        component = overview.get(key) if isinstance(overview.get(key), dict) else {}
        state = _display_value(component.get("status"), default="check_needed")
        if state == "fail":
            failed_keys.append(key)
        elif state != "pass":
            warning_keys.append(key)

    failed_set = set(failed_keys)
    warning_set = set(warning_keys)
    if "pm2" in failed_set or "storage" in failed_set or "captureboard" in failed_set or len(failed_keys) >= 2:
        priority_keys = [key for key in ("pm2", "storage", "captureboard", "audio", "led") if key in failed_set] or failed_keys
        return {
            "eligible": True,
            "score": 3,
            "label": "높음",
            "reason": f"{_format_daily_device_round_component_names(priority_keys)} 이상",
        }
    if "audio" in failed_set:
        return {
            "eligible": True,
            "score": 2,
            "label": "중간",
            "reason": "오디오 이상",
        }
    if "pm2" in warning_set or "storage" in warning_set or "captureboard" in warning_set or len(warning_keys) >= 2:
        priority_keys = [
            key
            for key in ("pm2", "storage", "captureboard", "audio", "led")
            if key in warning_set
        ] or warning_keys
        return {
            "eligible": True,
            "score": 2,
            "label": "중간",
            "reason": f"{_format_daily_device_round_component_names(priority_keys)} 확인 필요",
        }
    if failed_keys or warning_keys:
        priority_keys = failed_keys or warning_keys
        suffix = "이상" if failed_keys else "확인 필요"
        return {
            "eligible": True,
            "score": 1,
            "label": "낮음",
            "reason": f"{_format_daily_device_round_component_names(priority_keys)} {suffix}",
        }
    return {
        "eligible": True,
        "score": 0,
        "label": "정상",
        "reason": "원격 점검상 이상 징후 없음",
    }


def _build_daily_device_round_update_plan(update_payload: dict[str, Any]) -> dict[str, Any]:
    device_payload = update_payload.get("device") if isinstance(update_payload.get("device"), dict) else {}
    box_runtime = update_payload.get("boxRuntime") if isinstance(update_payload.get("boxRuntime"), dict) else {}
    agent_runtime = update_payload.get("agentRuntime") if isinstance(update_payload.get("agentRuntime"), dict) else {}
    agent_gate = update_payload.get("agentGate") if isinstance(update_payload.get("agentGate"), dict) else {}
    if not agent_gate:
        agent_gate = _describe_agent_box_update_gate(agent_runtime)

    latest_box_version = _display_value(update_payload.get("latestVersion"), default="")
    box_process = box_runtime.get("process") if isinstance(box_runtime.get("process"), dict) else {}
    agent_repo = agent_runtime.get("repo") if isinstance(agent_runtime.get("repo"), dict) else {}
    agent_process = agent_runtime.get("process") if isinstance(agent_runtime.get("process"), dict) else {}
    device_connected = bool(device_payload.get("isConnected"))

    current_box_version = _display_value(
        box_process.get("version"),
        default=_display_value(device_payload.get("version"), default=""),
    )
    current_agent_version = _resolve_agent_runtime_version(agent_runtime)
    agent_repo_available = bool(agent_repo.get("available"))
    agent_runtime_status = _display_value(agent_process.get("status"), default="").strip().lower()
    agent_box_ready = bool(agent_gate.get("ok"))
    # install-agent 스크립트 기준에 맞춰 에이전트는 repo 최신 여부보다 실행 가능 상태를 우선 본다.
    agent_healthy = bool(device_connected and agent_runtime_status == "online" and agent_box_ready)
    box_already_latest = bool(latest_box_version and current_box_version == latest_box_version)
    agent_gate_version = _display_value(agent_gate.get("version"), default="")

    if not device_connected:
        agent_reason = "장비 agent 연결 끊김"
    elif agent_runtime_status != "online":
        agent_reason = (
            f"에이전트 {current_agent_version} 실행 상태 확인 필요"
            if current_agent_version
            else "에이전트 실행 상태 확인 필요"
        )
    elif agent_gate_version and not agent_box_ready:
        agent_reason = f"에이전트 {agent_gate_version} 업데이트 필요"
    elif not agent_box_ready:
        agent_reason = "에이전트 버전 확인 필요"
    elif agent_healthy:
        agent_reason = "에이전트 정상"
    else:
        agent_reason = "에이전트 상태 확인 필요"

    if not device_connected:
        box_reason = "장비 agent 연결 끊김"
    elif not latest_box_version:
        box_reason = "최신 박스 버전 확인 불가"
    elif box_already_latest:
        box_reason = "박스 최신"
    elif not bool(agent_gate.get("ok")):
        box_reason = _display_value(agent_gate.get("reason"), default="박스 업데이트 선행조건 미충족")
    else:
        box_reason = f"박스 {current_box_version or '미확인'} -> {latest_box_version}"

    return {
        "agent": {
            "currentVersion": current_agent_version,
            "connected": device_connected,
            "repoAvailable": agent_repo_available,
            "isLatest": agent_healthy,
            "isHealthy": agent_healthy,
            "shouldUpdate": bool(device_connected and not agent_healthy),
            "reason": agent_reason,
            "runtimeStatus": _display_value(agent_process.get("status"), default=""),
        },
        "box": {
            "currentVersion": current_box_version,
            "latestVersion": latest_box_version,
            "connected": device_connected,
            "alreadyLatest": box_already_latest,
            "gateOk": bool(agent_gate.get("ok")),
            "shouldUpdate": bool(device_connected and latest_box_version and not box_already_latest and agent_gate.get("ok")),
            "reason": box_reason,
            "runtimeStatus": _display_value(box_process.get("status"), default=""),
        },
    }


def _build_daily_device_round_action_result(
    *,
    result_text: str,
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    route = _display_value(result_payload.get("route"), default="")
    dispatch_payload = result_payload.get("dispatch") if isinstance(result_payload.get("dispatch"), dict) else {}
    wait_payload = result_payload.get("wait") if isinstance(result_payload.get("wait"), dict) else {}
    status = _display_value(wait_payload.get("status"), default="")
    if not status:
        status = "dispatch_failed" if dispatch_payload and not dispatch_payload.get("status") else "not_requested"
    ok = bool(wait_payload.get("ok")) or status == "already_latest"
    return {
        "route": route,
        "text": result_text,
        "payload": result_payload,
        "dispatchStatus": bool(dispatch_payload.get("status")),
        "status": status,
        "ok": ok,
    }


def _describe_daily_device_round_action(
    action: dict[str, Any] | None,
    *,
    route_kind: str,
    plan: dict[str, Any],
) -> str:
    if action:
        status = _display_value(action.get("status"), default="")
        if route_kind == "agent":
            if status in {"completed", "already_latest"} and action.get("ok"):
                return "에이전트 업데이트 완료"
            if status == "dispatch_failed":
                return "에이전트 업데이트 실패"
            return "에이전트 업데이트 확인 필요"
        if status in {"completed", "already_latest"} and action.get("ok"):
            return "박스 업데이트 완료"
        if status == "dispatch_failed":
            return "박스 업데이트 실패"
        return "박스 업데이트 확인 필요"

    if route_kind == "agent":
        if plan.get("isHealthy") or plan.get("isLatest"):
            return "에이전트 정상"
        if plan.get("shouldUpdate"):
            return "에이전트 업데이트 후보"
        return _display_value(plan.get("reason"), default="에이전트 확인 필요")

    if plan.get("alreadyLatest"):
        return "박스 최신"
    if plan.get("shouldUpdate"):
        return "박스 업데이트 후보"
    return _display_value(plan.get("reason"), default="박스 확인 필요")


def _describe_daily_device_round_power_summary(
    device_result: dict[str, Any],
) -> tuple[str, str, str]:
    action = device_result.get("powerAction") if isinstance(device_result.get("powerAction"), dict) else None
    if not action:
        return "", "", ""

    status = _display_value(action.get("status"), default="")
    payload = action.get("payload") if isinstance(action.get("payload"), dict) else {}
    if status == "already_offline" and action.get("ok"):
        return "latest", "종료 불필요", "이미 오프라인"
    if status == "completed" and action.get("ok"):
        return "success", "종료 완료", "MDA ping 오프라인 확인"
    if status == "dispatch_failed":
        dispatch_payload = payload.get("dispatch") if isinstance(payload.get("dispatch"), dict) else {}
        return "failed", "종료 실패", _display_value(dispatch_payload.get("message"), default="종료 실패")
    return "check", "확인 필요", "전원 종료 재확인 필요"


def _describe_daily_device_round_power_action(
    action: dict[str, Any] | None,
) -> str:
    if not action:
        return "장비 종료 미실행"
    status = _display_value(action.get("status"), default="")
    if status == "already_offline" and action.get("ok"):
        return "장비 종료 생략"
    if status == "completed" and action.get("ok"):
        return "장비 종료 완료"
    if status == "dispatch_failed":
        return "장비 종료 실패"
    return "장비 종료 확인 필요"


def _format_daily_device_round_hospital_label(
    hospital_name: str | None,
    hospital_seq: int | None,
) -> str:
    name = _display_value(hospital_name, default="미선정")
    if hospital_seq is None:
        return name
    return f"#{hospital_seq} {name}"


def _build_daily_device_round_title_text(report_summary: dict[str, Any]) -> str:
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    if hospital_seq is None:
        return _DAILY_DEVICE_ROUND_TITLE
    return (
        f"{_DAILY_DEVICE_ROUND_TITLE} | "
        f"{_format_daily_device_round_hospital_label(report_summary.get('hospitalName'), hospital_seq)}"
    )


def _build_daily_device_round_hospital_heading_text(
    hospital_name: str | None,
    hospital_seq: int | None,
) -> str:
    return f"*{_format_daily_device_round_hospital_label(hospital_name, hospital_seq)}*"


def _describe_daily_device_round_route_summary(
    device_result: dict[str, Any],
    *,
    route_kind: str,
) -> tuple[str, str, str]:
    final_plan = device_result.get("finalPlan") if isinstance(device_result.get("finalPlan"), dict) else {}
    plan = final_plan.get(route_kind) if isinstance(final_plan.get(route_kind), dict) else {}
    action = device_result.get(f"{route_kind}Action") if isinstance(device_result.get(f"{route_kind}Action"), dict) else None
    current_version = _display_value(plan.get("currentVersion"), default="")
    latest_version = _display_value(plan.get("latestVersion"), default="")
    action_payload = action.get("payload") if isinstance((action or {}).get("payload"), dict) else {}

    previous_version = ""
    if route_kind == "agent":
        precheck_runtime = action_payload.get("precheck") if isinstance(action_payload.get("precheck"), dict) else {}
        precheck_process = precheck_runtime.get("process") if isinstance(precheck_runtime.get("process"), dict) else {}
        precheck_repo = precheck_runtime.get("repo") if isinstance(precheck_runtime.get("repo"), dict) else {}
        previous_version = _display_value(
            precheck_repo.get("packageVersion"),
            default=_display_value(precheck_process.get("version"), default=""),
        )
    else:
        precheck_runtime = action_payload.get("precheck") if isinstance(action_payload.get("precheck"), dict) else {}
        precheck_process = precheck_runtime.get("process") if isinstance(precheck_runtime.get("process"), dict) else {}
        device_payload = action_payload.get("device") if isinstance(action_payload.get("device"), dict) else {}
        previous_version = _display_value(
            precheck_process.get("version"),
            default=_display_value(device_payload.get("version"), default=""),
        )

    if route_kind == "agent":
        if action:
            status = _display_value(action.get("status"), default="")
            if status in {"completed", "already_latest"} and action.get("ok"):
                if previous_version and current_version and previous_version != current_version:
                    return "success", "업데이트 완료", f"`{previous_version}` -> `{current_version}`"
                return "success", "업데이트 완료", f"버전 `{current_version or '미확인'}`"
            if status == "dispatch_failed":
                return "failed", "업데이트 실패", _display_value(plan.get("reason"), default="업데이트 실패")
            return "check", "확인 필요", _display_value(plan.get("reason"), default="업데이트 확인 필요")
        if plan.get("isHealthy") or plan.get("isLatest"):
            return "latest", "업데이트 불필요", f"버전 `{current_version or '미확인'}`"
        if plan.get("shouldUpdate"):
            return "pending", "업데이트 필요", _display_value(plan.get("reason"), default="업데이트 후보")
        return "check", "확인 필요", _display_value(plan.get("reason"), default="상태 확인 필요")

    if action:
        status = _display_value(action.get("status"), default="")
        if status in {"completed", "already_latest"} and action.get("ok"):
            final_version = current_version or latest_version
            if previous_version and final_version and previous_version != final_version:
                return "success", "업데이트 완료", f"`{previous_version}` -> `{final_version}`"
            return "success", "업데이트 완료", f"버전 `{final_version or '미확인'}`"
        if status == "dispatch_failed":
            return "failed", "업데이트 실패", _display_value(plan.get("reason"), default="업데이트 실패")
        return "check", "확인 필요", _display_value(plan.get("reason"), default="업데이트 확인 필요")
    if plan.get("alreadyLatest"):
        return "latest", "업데이트 불필요", f"버전 `{current_version or latest_version or '미확인'}`"
    if plan.get("shouldUpdate"):
        return "pending", "업데이트 필요", _display_value(plan.get("reason"), default="업데이트 후보")
    return "check", "확인 필요", _display_value(plan.get("reason"), default="상태 확인 필요")


def _format_daily_device_round_update_badge(status_kind: str, label: str) -> str:
    icon_map = {
        "success": "🟢",
        "latest": "⚪",
        "pending": "🟠",
        "failed": "🔴",
        "check": "🟡",
    }
    icon = icon_map.get(_display_value(status_kind, default=""), "🟡")
    return f"{icon} *{_display_value(label, default='확인 필요')}*"


def _format_daily_device_round_status_badge(label: str) -> str:
    normalized = _display_value(label, default="확인 필요")
    icon_map = {
        "정상": "🟢",
        "확인 필요": "🟠",
        "이상": "🔴",
        "점검 불가": "⚫",
    }
    return f"{icon_map.get(normalized, '🟡')} *{normalized}*"


def _format_daily_device_round_priority_badge(label: str) -> str:
    normalized = _display_value(label, default="판단 보류")
    icon_map = {
        "높음": "🔴",
        "중간": "🟠",
        "낮음": "🟡",
        "정상": "🟢",
        "판단 보류": "⚫",
    }
    return f"{icon_map.get(normalized, '🟡')} *{normalized}*"


def _format_daily_device_round_percent(value: Any) -> str:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return "미확인"

    if numeric.is_integer():
        return f"{int(numeric)}%"
    return f"{numeric:.1f}%"


def _build_daily_device_round_summary_lines(
    report_summary: dict[str, Any],
) -> list[str]:
    status_counts = report_summary.get("statusCounts") if isinstance(report_summary.get("statusCounts"), dict) else {}
    update_counts = report_summary.get("updateCounts") if isinstance(report_summary.get("updateCounts"), dict) else {}
    cleanup_counts = report_summary.get("cleanupCounts") if isinstance(report_summary.get("cleanupCounts"), dict) else {}
    power_counts = report_summary.get("powerCounts") if isinstance(report_summary.get("powerCounts"), dict) else {}

    lines: list[str] = []
    check_needed_count = int(status_counts.get("확인 필요") or 0)
    failed_count = int(status_counts.get("이상") or 0)
    unavailable_count = int(status_counts.get("점검 불가") or 0)
    if check_needed_count:
        lines.append(f"• 🟠 확인 필요 `{check_needed_count}`")
    if failed_count:
        lines.append(f"• 🔴 이상 `{failed_count}`")
    if unavailable_count:
        lines.append(f"• ⚫ 점검 불가 `{unavailable_count}`")
    if not lines:
        lines.append("• 🟢 문제 장비 없음")

    agent_updated = int(update_counts.get("agentUpdated") or 0)
    agent_failed = int(update_counts.get("agentUpdateFailed") or 0)
    box_updated = int(update_counts.get("boxUpdated") or 0)
    box_failed = int(update_counts.get("boxUpdateFailed") or 0)
    agent_candidates = int(update_counts.get("agentCandidates") or 0)
    box_candidates = int(update_counts.get("boxCandidates") or 0)
    agent_pending = max(0, agent_candidates - agent_updated - agent_failed)
    box_pending = max(0, box_candidates - box_updated - box_failed)

    if agent_pending or agent_updated or agent_failed:
        parts = []
        if agent_pending:
            parts.append(f"대상 `{agent_pending}`")
        if agent_updated:
            parts.append(f"성공 `{agent_updated}`")
        if agent_failed:
            parts.append(f"실패 `{agent_failed}`")
        lines.append("• 에이전트 업데이트 " + " / ".join(parts))
    if box_pending or box_updated or box_failed:
        parts = []
        if box_pending:
            parts.append(f"대상 `{box_pending}`")
        if box_updated:
            parts.append(f"성공 `{box_updated}`")
        if box_failed:
            parts.append(f"실패 `{box_failed}`")
        lines.append("• 박스 업데이트 " + " / ".join(parts))

    cleanup_candidates = int(cleanup_counts.get("candidates") or 0)
    cleanup_executed = int(cleanup_counts.get("executed") or 0)
    cleanup_failed = int(cleanup_counts.get("failed") or 0)
    cleanup_pending = max(0, cleanup_candidates - cleanup_executed - cleanup_failed)
    if cleanup_pending or cleanup_executed or cleanup_failed:
        parts = []
        if cleanup_pending:
            parts.append(f"대상 `{cleanup_pending}`")
        if cleanup_executed:
            parts.append(f"실행 `{cleanup_executed}`")
        if cleanup_failed:
            parts.append(f"실패 `{cleanup_failed}`")
        lines.append("• 🧹 디스크 정리 " + " / ".join(parts))

    power_requested = int(power_counts.get("requested") or 0)
    power_completed = int(power_counts.get("poweredOff") or 0)
    power_already_offline = int(power_counts.get("alreadyOffline") or 0)
    power_failed = int(power_counts.get("powerOffFailed") or 0)
    power_pending = max(0, power_requested - power_completed - power_already_offline - power_failed)
    if power_pending or power_completed or power_already_offline or power_failed:
        parts = []
        if power_completed:
            parts.append(f"완료 `{power_completed}`")
        if power_already_offline:
            parts.append(f"생략 `{power_already_offline}`")
        if power_pending:
            parts.append(f"확인 필요 `{power_pending}`")
        if power_failed:
            parts.append(f"실패 `{power_failed}`")
        lines.append("• ⏻ 장비 종료 " + " / ".join(parts))

    return lines


def _build_daily_device_round_summary_rich_text_block(
    report_summary: dict[str, Any],
) -> dict[str, Any]:
    summary_items = []
    for line in _build_daily_device_round_summary_lines(report_summary):
        text = line[2:] if line.startswith("• ") else line
        summary_items.append(
            {
                "type": "rich_text_section",
                "elements": [
                    {
                        "type": "text",
                        "text": text,
                    }
                ],
            }
        )

    return {
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_list",
                "style": "bullet",
                "elements": summary_items,
            },
        ],
    }


def _is_daily_device_round_ssh_unavailable(device_result: dict[str, Any]) -> bool:
    status_payload = device_result.get("statusPayload") if isinstance(device_result.get("statusPayload"), dict) else {}
    ssh_payload = status_payload.get("ssh") if isinstance(status_payload.get("ssh"), dict) else {}
    return bool(ssh_payload) and not bool(ssh_payload.get("ready"))


def _is_daily_device_round_cleanup_actionable(device_result: dict[str, Any]) -> bool:
    cleanup = device_result.get("trashcanCleanup") if isinstance(device_result.get("trashcanCleanup"), dict) else {}
    if cleanup.get("required") or cleanup.get("executed"):
        return True
    status = _display_value(cleanup.get("status"), default="")
    if status in {"failed", "unavailable", "candidate", "completed"}:
        return True
    label = _display_value(cleanup.get("label"), default="")
    return label in {"실패", "실행 불가", "대상", "성공"}


def _is_daily_device_round_route_actionable(
    device_result: dict[str, Any],
    *,
    route_kind: str,
) -> bool:
    status_kind, _, _ = _describe_daily_device_round_route_summary(
        device_result,
        route_kind=route_kind,
    )
    if status_kind == "latest":
        return False
    if status_kind in {"success", "pending", "failed", "check"}:
        return True
    return bool(
        device_result.get(f"{route_kind}Action")
        if isinstance(device_result.get(f"{route_kind}Action"), dict)
        else False
    )


def _is_daily_device_round_actionable_device(device_result: dict[str, Any]) -> bool:
    overall_label = _display_value(device_result.get("overallLabel"), default="확인 필요")
    if overall_label != "정상":
        return True
    if _is_daily_device_round_cleanup_actionable(device_result):
        return True
    if _is_daily_device_round_route_actionable(device_result, route_kind="agent"):
        return True
    if _is_daily_device_round_route_actionable(device_result, route_kind="box"):
        return True
    if isinstance(device_result.get("powerAction"), dict):
        return True
    return False


def _collect_daily_device_round_actionable_device_results(
    device_results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    # Slack 본문 길이를 줄이기 위해 문제 있거나 실제 작업이 있었던 장비만 남겨.
    return [
        item
        for item in device_results
        if isinstance(item, dict) and _is_daily_device_round_actionable_device(item)
    ]


def _describe_daily_device_round_trashcan_cleanup(
    device_result: dict[str, Any],
) -> tuple[str, str]:
    cleanup = device_result.get("trashcanCleanup") if isinstance(device_result.get("trashcanCleanup"), dict) else {}
    return (
        _display_value(cleanup.get("label"), default="미확인"),
        _display_value(cleanup.get("detail"), default="정리 정보가 없어"),
    )


def _build_daily_device_round_storage_details(status_payload: dict[str, Any]) -> dict[str, str]:
    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    storage = overview.get("storage") if isinstance(overview.get("storage"), dict) else {}
    base_label = _display_value(storage.get("label"), default="확인 필요")
    return {
        "diskLabel": _display_value(storage.get("diskLabel"), default=base_label),
        "diskDetail": _display_value(storage.get("diskOverviewDetail"), default=""),
        "trashcanLabel": _display_value(storage.get("trashcanLabel"), default=base_label),
        "trashcanDetail": _display_value(
            storage.get("trashcanOverviewDetail"),
            default=_display_value(storage.get("overviewDetail"), default=""),
        ),
    }


def _build_daily_device_round_component_issue_text(
    device_result: dict[str, Any],
    *,
    component_key: str,
) -> str:
    status_payload = device_result.get("statusPayload") if isinstance(device_result.get("statusPayload"), dict) else {}
    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    component_payload = overview.get(component_key) if isinstance(overview.get(component_key), dict) else {}
    summary = _display_value(component_payload.get("summary"), default="")
    if component_key == "storage" and summary:
        detail_parts: list[str] = []
        directory_share_percent = component_payload.get("directorySharePercent")
        if directory_share_percent is not None:
            detail_parts.append(f"현재 `{_format_daily_device_round_percent(directory_share_percent)}`")
        directory_size_bytes = int(component_payload.get("directorySizeBytes") or 0)
        if directory_size_bytes > 0:
            detail_parts.append(f"폴더 `{_format_size(directory_size_bytes)}`")
        cleanup_age_days = max(
            1,
            int(component_payload.get("cleanupAgeDays") or cs.DAILY_DEVICE_ROUND_TRASHCAN_DELETE_AGE_DAYS),
        )
        expired_file_count = int(component_payload.get("expiredFileCount") or 0)
        if expired_file_count > 0:
            detail_parts.append(f"`{cleanup_age_days}일` 초과 `{expired_file_count:,}개`")
        if detail_parts:
            return f"{summary} | {' / '.join(detail_parts)}"
    if summary:
        return summary
    overview_detail = _display_value(component_payload.get("overviewDetail"), default="")
    if overview_detail:
        return overview_detail
    component_labels = device_result.get("componentLabels") if isinstance(device_result.get("componentLabels"), dict) else {}
    label = _display_value(component_labels.get(component_key), default="")
    if label and label != "정상":
        return f"{_DAILY_DEVICE_ROUND_COMPONENT_NAMES.get(component_key, component_key)} {label}"
    return ""


def _build_daily_device_round_issue_summary(device_result: dict[str, Any]) -> str:
    component_labels = device_result.get("componentLabels") if isinstance(device_result.get("componentLabels"), dict) else {}
    issues: list[str] = []
    for key in ("audio", "pm2", "storage", "captureboard", "led"):
        label = _display_value(component_labels.get(key), default="")
        if not label or label == "정상":
            continue
        issue_text = _build_daily_device_round_component_issue_text(
            device_result,
            component_key=key,
        )
        if issue_text:
            issues.append(issue_text)

    if issues:
        return " / ".join(issues)

    overall_label = _display_value(device_result.get("overallLabel"), default="확인 필요")
    if overall_label != "정상":
        return _display_value(device_result.get("priorityReason"), default="")
    return ""


def _build_daily_device_round_disk_detail(device_result: dict[str, Any]) -> str:
    status_payload = device_result.get("statusPayload") if isinstance(device_result.get("statusPayload"), dict) else {}
    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    storage_payload = overview.get("storage") if isinstance(overview.get("storage"), dict) else {}
    filesystem_used_percent = storage_payload.get("filesystemUsedPercent")
    filesystem_available_bytes = int(storage_payload.get("filesystemAvailableBytes") or 0)
    filesystem_size_bytes = int(storage_payload.get("filesystemSizeBytes") or 0)

    parts: list[str] = []
    if filesystem_used_percent is not None:
        parts.append(f"사용량 `{_format_daily_device_round_percent(filesystem_used_percent)}`")
    if filesystem_available_bytes > 0:
        parts.append(f"여유 `{_format_size(filesystem_available_bytes)}`")
    if filesystem_size_bytes > 0:
        parts.append(f"전체 `{_format_size(filesystem_size_bytes)}`")

    if parts:
        return " / ".join(parts)

    storage_details = device_result.get("storageDetails") if isinstance(device_result.get("storageDetails"), dict) else {}
    return _display_value(storage_details.get("diskDetail"), default="")


def _build_daily_device_round_trashcan_detail(device_result: dict[str, Any]) -> str:
    status_payload = device_result.get("statusPayload") if isinstance(device_result.get("statusPayload"), dict) else {}
    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    storage_payload = overview.get("storage") if isinstance(overview.get("storage"), dict) else {}
    directory_size_bytes = int(storage_payload.get("directorySizeBytes") or 0)
    directory_share_percent = storage_payload.get("directorySharePercent")
    file_count = int(storage_payload.get("fileCount") or 0)
    expired_file_count = int(storage_payload.get("expiredFileCount") or 0)
    age_days = max(1, int(storage_payload.get("cleanupAgeDays") or cs.DAILY_DEVICE_ROUND_TRASHCAN_DELETE_AGE_DAYS))

    parts: list[str] = []
    if directory_size_bytes > 0:
        folder_part = f"폴더 `{_format_size(directory_size_bytes)}`"
        if directory_share_percent is not None:
            folder_part = f"{folder_part} (`{_format_daily_device_round_percent(directory_share_percent)}`)"
        parts.append(folder_part)
    elif directory_share_percent is not None:
        parts.append(f"비중 `{_format_daily_device_round_percent(directory_share_percent)}`")
    if file_count > 0:
        parts.append(f"파일 `{file_count:,}개`")
    if expired_file_count > 0:
        parts.append(f"{age_days}일 초과 `{expired_file_count:,}개`")

    if parts:
        return " / ".join(parts)

    storage_details = device_result.get("storageDetails") if isinstance(device_result.get("storageDetails"), dict) else {}
    return _display_value(storage_details.get("trashcanDetail"), default="")


def _build_daily_device_round_device_line(device_result: dict[str, Any]) -> str:
    overall_label = _display_value(device_result.get("overallLabel"), default="확인 필요")
    agent_status_kind, agent_status_label, agent_detail = _describe_daily_device_round_route_summary(
        device_result,
        route_kind="agent",
    )
    box_status_kind, box_status_label, box_detail = _describe_daily_device_round_route_summary(
        device_result,
        route_kind="box",
    )
    power_status_kind, power_status_label, power_detail = _describe_daily_device_round_power_summary(device_result)
    cleanup_status, cleanup_detail = _describe_daily_device_round_trashcan_cleanup(device_result)
    room_name = _display_value(device_result.get("roomName"), default="")
    device_name = _display_value(device_result.get("deviceName"), default="미확인")
    header_parts: list[str] = []
    if room_name and room_name != "미확인":
        header_parts.append(f"*{room_name}*")
    header_parts.append(f"*{device_name}*")
    header_parts.append(_format_daily_device_round_status_badge(overall_label))
    header = f"• {'  |  '.join(header_parts)}"

    if _is_daily_device_round_ssh_unavailable(device_result):
        return "\n".join(
            [
                header,
                "  *안내*  ⚫ *장비 종료 또는 네트워크 연결 불가로 점검 불가*",
            ]
        )

    issue_summary = _build_daily_device_round_issue_summary(device_result)
    lines = [header]
    if issue_summary:
        lines.append(f"  *이슈*  {issue_summary}")
    elif overall_label != "정상":
        lines.append(
            f"  *이슈*  {_display_value(device_result.get('priorityReason'), default='상세 확인 필요')}"
        )
    if _is_daily_device_round_cleanup_actionable(device_result):
        lines.append(f"  *디스크 정리*  *{cleanup_status}* | {cleanup_detail}")
    if _is_daily_device_round_route_actionable(device_result, route_kind="agent"):
        lines.append(
            f"  *에이전트 업데이트*  {_format_daily_device_round_update_badge(agent_status_kind, agent_status_label)} | {agent_detail}"
        )
    if _is_daily_device_round_route_actionable(device_result, route_kind="box"):
        lines.append(
            f"  *박스 업데이트*  {_format_daily_device_round_update_badge(box_status_kind, box_status_label)} | {box_detail}"
        )
    if power_status_kind:
        lines.append(
            f"  *장비 종료*  {_format_daily_device_round_update_badge(power_status_kind, power_status_label)} | {power_detail}"
        )
    return "\n".join(lines)


def _run_daily_device_round_for_device(
    device_name: str,
    *,
    auto_update_agent: bool = False,
    auto_update_box: bool = False,
    auto_cleanup_trashcan: bool = False,
    auto_power_off: bool = False,
) -> dict[str, Any]:
    status_text, status_payload = _probe_device_status_overview(device_name)
    trashcan_cleanup = _run_device_trashcan_cleanup(
        status_payload,
        execute=auto_cleanup_trashcan,
        cleanup_threshold_percent=cs.DAILY_DEVICE_ROUND_TRASHCAN_USAGE_THRESHOLD_PERCENT,
        cleanup_age_days=cs.DAILY_DEVICE_ROUND_TRASHCAN_DELETE_AGE_DAYS,
    )
    if trashcan_cleanup.get("executed"):
        status_text, status_payload = _probe_device_status_overview(device_name)

    update_status_text, update_status_payload = _query_device_update_status(device_name)
    initial_update_status_text = update_status_text
    initial_plan = _build_daily_device_round_update_plan(update_status_payload)

    agent_action = None
    if auto_update_agent and initial_plan["agent"]["shouldUpdate"]:
        agent_text, agent_payload = _request_device_agent_update(
            f"{device_name} 에이전트 업데이트",
            device_name=device_name,
        )
        agent_action = _build_daily_device_round_action_result(
            result_text=agent_text,
            result_payload=agent_payload,
        )
        update_status_text, update_status_payload = _query_device_update_status(device_name)

    box_plan = _build_daily_device_round_update_plan(update_status_payload)
    box_action = None
    if auto_update_box and box_plan["box"]["shouldUpdate"]:
        box_text, box_payload = _request_device_box_update(
            f"{device_name} 장비 업데이트",
            device_name=device_name,
        )
        box_action = _build_daily_device_round_action_result(
            result_text=box_text,
            result_payload=box_payload,
        )
        update_status_text, update_status_payload = _query_device_update_status(device_name)

    power_action = None
    if auto_power_off:
        # 점검/업데이트 결과를 다 모은 뒤 마지막에만 종료를 걸어야 리포트 근거가 흔들리지 않아.
        power_text, power_payload = _request_device_power_off(
            f"{device_name} 장비 종료",
            device_name=device_name,
        )
        power_action = _build_daily_device_round_action_result(
            result_text=power_text,
            result_payload=power_payload,
        )

    final_plan = _build_daily_device_round_update_plan(update_status_payload)
    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    device_payload = update_status_payload.get("device") if isinstance(update_status_payload.get("device"), dict) else {}
    component_labels = {
        "audio": _display_value(((overview.get("audio") or {}) if isinstance(overview.get("audio"), dict) else {}).get("label"), default="확인 필요"),
        "pm2": _display_value(((overview.get("pm2") or {}) if isinstance(overview.get("pm2"), dict) else {}).get("label"), default="확인 필요"),
        "storage": _display_value(((overview.get("storage") or {}) if isinstance(overview.get("storage"), dict) else {}).get("label"), default="확인 필요"),
        "captureboard": _display_value(((overview.get("captureboard") or {}) if isinstance(overview.get("captureboard"), dict) else {}).get("label"), default="확인 필요"),
        "led": _display_value(((overview.get("led") or {}) if isinstance(overview.get("led"), dict) else {}).get("label"), default="확인 필요"),
    }
    storage_details = _build_daily_device_round_storage_details(status_payload)
    priority = _build_daily_device_round_priority(status_payload)

    return {
        "deviceName": _display_value(device_payload.get("deviceName"), default=device_name),
        "hospitalName": _display_value(device_payload.get("hospitalName"), default="미확인"),
        "roomName": _display_value(device_payload.get("roomName"), default="미확인"),
        "overallLabel": _daily_device_round_status_label(status_payload),
        "priorityEligible": bool(priority.get("eligible")),
        "priorityScore": int(priority.get("score") or 0),
        "priorityLabel": _display_value(priority.get("label"), default="판단 보류"),
        "priorityReason": _display_value(priority.get("reason"), default="네트워크 연결 불가로 이상 징후 판단 보류"),
        "componentLabels": component_labels,
        "storageDetails": storage_details,
        "statusText": status_text,
        "statusPayload": status_payload,
        "trashcanCleanup": trashcan_cleanup,
        "initialUpdateStatusText": initial_update_status_text,
        "initialPlan": initial_plan,
        "finalUpdateStatusText": update_status_text,
        "finalUpdateStatusPayload": update_status_payload,
        "finalPlan": final_plan,
        "agentAction": agent_action,
        "boxAction": box_action,
        "powerAction": power_action,
        "agentActionText": _describe_daily_device_round_action(
            agent_action,
            route_kind="agent",
            plan=final_plan["agent"],
        ),
        "boxActionText": _describe_daily_device_round_action(
            box_action,
            route_kind="box",
            plan=final_plan["box"],
        ),
        "powerActionText": _describe_daily_device_round_power_action(power_action),
    }


def _build_daily_device_round_error_result(
    device_context: dict[str, Any],
    exc: Exception,
) -> dict[str, Any]:
    priority = _build_daily_device_round_priority({})
    return {
        "deviceName": _display_value(device_context.get("deviceName"), default="미확인"),
        "hospitalName": _display_value(device_context.get("hospitalName"), default="미확인"),
        "roomName": _display_value(device_context.get("roomName"), default="미확인"),
        "overallLabel": "점검 불가",
        "priorityEligible": bool(priority.get("eligible")),
        "priorityScore": int(priority.get("score") or 0),
        "priorityLabel": _display_value(priority.get("label"), default="판단 보류"),
        "priorityReason": _display_value(priority.get("reason"), default="네트워크 연결 불가로 이상 징후 판단 보류"),
        "componentLabels": {
            "audio": "점검 불가",
            "pm2": "점검 불가",
            "storage": "점검 불가",
            "captureboard": "점검 불가",
            "led": "점검 불가",
        },
        "storageDetails": {
            "diskLabel": "점검 불가",
            "diskDetail": "",
            "trashcanLabel": "점검 불가",
            "trashcanDetail": "",
        },
        "trashcanCleanup": {
            "status": "unavailable",
            "label": "실행 불가",
            "detail": "점검 실패로 정리 판단을 못 했어",
        },
        "statusText": f"점검 실패: {type(exc).__name__}",
        "statusPayload": {},
        "initialUpdateStatusText": "",
        "initialPlan": {
            "agent": {"shouldUpdate": False, "isLatest": False, "reason": "점검 실패"},
            "box": {"shouldUpdate": False, "alreadyLatest": False, "reason": "점검 실패"},
        },
        "finalUpdateStatusText": "",
        "finalUpdateStatusPayload": {},
        "finalPlan": {
            "agent": {"shouldUpdate": False, "isLatest": False, "reason": "점검 실패"},
            "box": {"shouldUpdate": False, "alreadyLatest": False, "reason": "점검 실패"},
        },
        "agentAction": None,
        "boxAction": None,
        "powerAction": None,
        "agentActionText": "에이전트 점검 실패",
        "boxActionText": "박스 점검 실패",
        "powerActionText": "장비 종료 점검 실패",
        "error": f"{type(exc).__name__}: {exc}",
    }


def _build_daily_device_round_summary(
    *,
    now: datetime | None = None,
    state: dict[str, Any] | None = None,
    auto_update_agent: bool = False,
    auto_update_box: bool = False,
    auto_cleanup_trashcan: bool = False,
    auto_power_off: bool = False,
    progress_callback: _DailyDeviceRoundProgressCallback | None = None,
) -> dict[str, Any]:
    local_now = _coerce_daily_device_round_now(now)
    candidates = _load_daily_device_round_hospital_candidates()
    state_payload = state if isinstance(state, dict) else {}
    processed_hospital_seqs = _coerce_daily_device_round_hospital_seqs(state_payload.get("processedHospitalSeqs"))
    candidate_hospital_seqs = {
        hospital_seq
        for hospital_seq in (
            _coerce_int(item.get("hospitalSeq"))
            for item in candidates
        )
        if hospital_seq is not None
    }
    processed_candidate_count = sum(
        1
        for hospital_seq in processed_hospital_seqs
        if hospital_seq in candidate_hospital_seqs
    )
    hospital = _select_daily_device_round_hospital(candidates, state=state)
    if hospital is None:
        return {
            "runDate": local_now.date().isoformat(),
            "startedAt": local_now.isoformat(),
            "finishedAt": local_now.isoformat(),
            "hospitalSeq": None,
            "hospitalName": "미선정",
            "deviceCount": 0,
            "autoUpdateAgent": bool(auto_update_agent),
            "autoUpdateBox": bool(auto_update_box),
            "autoCleanupTrashCan": bool(auto_cleanup_trashcan),
            "autoPowerOff": bool(auto_power_off),
            "statusCounts": {
                "정상": 0,
                "확인 필요": 0,
                "이상": 0,
                "점검 불가": 0,
            },
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
            "deviceResults": [],
            "nextHospitalSeq": None,
            "candidateHospitalCount": len(candidate_hospital_seqs),
            "summaryLine": (
                "이번 야간 업데이트 창에서 처리할 병원을 모두 끝냈어"
                if candidate_hospital_seqs and processed_candidate_count >= len(candidate_hospital_seqs)
                else "점검 대상 병원이 없어"
            ),
        }

    devices = _load_daily_device_round_devices(int(hospital["hospitalSeq"]))
    if progress_callback is not None:
        # 장비별 원격 점검/업데이트 대기 전에 현재 병원을 먼저 알려서
        # 리포터가 제목을 선발송하고 진행 상태를 저장할 수 있게 해.
        progress_callback(
            "hospital_started",
            {
                "hospitalSeq": int(hospital["hospitalSeq"]),
                "hospitalName": _display_value(hospital.get("hospitalName"), default="미확인"),
                "deviceCount": len(devices),
                "startedAt": local_now.isoformat(),
            },
        )
    device_results: list[dict[str, Any]] = []
    for index, device in enumerate(devices, start=1):
        if progress_callback is not None:
            progress_callback(
                "device_started",
                {
                    "hospitalSeq": int(hospital["hospitalSeq"]),
                    "hospitalName": _display_value(hospital.get("hospitalName"), default="미확인"),
                    "deviceCount": len(devices),
                    "deviceIndex": index,
                    "deviceName": _display_value(device.get("deviceName"), default=""),
                    "updatedAt": _coerce_daily_device_round_now(now).isoformat(),
                },
            )
        try:
            device_results.append(
                _run_daily_device_round_for_device(
                    _display_value(device.get("deviceName"), default=""),
                    auto_update_agent=auto_update_agent,
                    auto_update_box=auto_update_box,
                    auto_cleanup_trashcan=auto_cleanup_trashcan,
                    auto_power_off=auto_power_off,
                )
            )
        except Exception as exc:
            device_results.append(_build_daily_device_round_error_result(device, exc))

    finished_at = _coerce_daily_device_round_now(now)
    status_counts = {
        "정상": 0,
        "확인 필요": 0,
        "이상": 0,
        "점검 불가": 0,
    }
    update_counts = {
        "agentCandidates": 0,
        "agentUpdated": 0,
        "agentUpdateFailed": 0,
        "boxCandidates": 0,
        "boxUpdated": 0,
        "boxUpdateFailed": 0,
    }
    cleanup_counts = {
        "candidates": 0,
        "executed": 0,
        "failed": 0,
    }
    power_counts = {
        "requested": 0,
        "poweredOff": 0,
        "alreadyOffline": 0,
        "powerOffFailed": 0,
    }
    for item in device_results:
        label = _display_value(item.get("overallLabel"), default="점검 불가")
        status_counts[label if label in status_counts else "점검 불가"] += 1

        initial_plan = item.get("initialPlan") if isinstance(item.get("initialPlan"), dict) else {}
        final_plan = item.get("finalPlan") if isinstance(item.get("finalPlan"), dict) else {}
        if bool(((initial_plan.get("agent") or {}) if isinstance(initial_plan.get("agent"), dict) else {}).get("shouldUpdate")):
            update_counts["agentCandidates"] += 1
        if bool(((initial_plan.get("box") or {}) if isinstance(initial_plan.get("box"), dict) else {}).get("shouldUpdate")) or bool(
            ((final_plan.get("box") or {}) if isinstance(final_plan.get("box"), dict) else {}).get("shouldUpdate")
        ):
            update_counts["boxCandidates"] += 1

        agent_action = item.get("agentAction") if isinstance(item.get("agentAction"), dict) else {}
        box_action = item.get("boxAction") if isinstance(item.get("boxAction"), dict) else {}
        if agent_action:
            if agent_action.get("ok"):
                update_counts["agentUpdated"] += 1
            else:
                update_counts["agentUpdateFailed"] += 1
        if box_action:
            if box_action.get("ok"):
                update_counts["boxUpdated"] += 1
            else:
                update_counts["boxUpdateFailed"] += 1

        cleanup = item.get("trashcanCleanup") if isinstance(item.get("trashcanCleanup"), dict) else {}
        if cleanup.get("required"):
            cleanup_counts["candidates"] += 1
        if cleanup.get("executed"):
            cleanup_counts["executed"] += 1
        elif _display_value(cleanup.get("status"), default="") == "failed":
            cleanup_counts["failed"] += 1

        power_action = item.get("powerAction") if isinstance(item.get("powerAction"), dict) else {}
        power_status = _display_value(power_action.get("status"), default="")
        if power_action:
            power_counts["requested"] += 1
            if power_status == "already_offline" and power_action.get("ok"):
                power_counts["alreadyOffline"] += 1
            elif power_action.get("ok"):
                power_counts["poweredOff"] += 1
            else:
                power_counts["powerOffFailed"] += 1

    next_hospital_seq = _resolve_next_daily_device_round_hospital_seq(
        candidates,
        _coerce_int(hospital.get("hospitalSeq")),
    )
    return {
        "runDate": local_now.date().isoformat(),
        "startedAt": local_now.isoformat(),
        "finishedAt": finished_at.isoformat(),
        "hospitalSeq": int(hospital["hospitalSeq"]),
        "hospitalName": _display_value(hospital.get("hospitalName"), default="미확인"),
        "deviceCount": len(device_results),
        "scheduledDeviceCount": int(hospital.get("deviceCount") or len(device_results)),
        "autoUpdateAgent": bool(auto_update_agent),
        "autoUpdateBox": bool(auto_update_box),
        "autoCleanupTrashCan": bool(auto_cleanup_trashcan),
        "autoPowerOff": bool(auto_power_off),
        "statusCounts": status_counts,
        "updateCounts": update_counts,
        "cleanupCounts": cleanup_counts,
        "powerCounts": power_counts,
        "deviceResults": device_results,
        "nextHospitalSeq": next_hospital_seq,
        "candidateHospitalCount": len(candidate_hospital_seqs),
        "summaryLine": (
            f"정상 {status_counts['정상']} / 확인 필요 {status_counts['확인 필요']} / "
            f"이상 {status_counts['이상']} / 점검 불가 {status_counts['점검 불가']}"
        ),
    }


def _format_daily_device_round_report(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
    include_title: bool = True,
) -> str:
    local_now = _coerce_daily_device_round_now(now)
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    device_results = report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []
    actionable_device_results = _collect_daily_device_round_actionable_device_results(device_results)

    lines: list[str] = []
    if include_title:
        lines.append(f"*{_build_daily_device_round_title_text(report_summary)}*")

    hospital_detail = _format_daily_device_round_hospital_label(
        report_summary.get("hospitalName"),
        hospital_seq,
    )
    lines.extend(
        [
            _build_daily_device_round_hospital_heading_text(
                report_summary.get("hospitalName"),
                hospital_seq,
            ),
            f"• 실행: `{local_now:%Y-%m-%d %H:%M:%S} KST`",
            (
                f"• 장비: `{int(report_summary.get('deviceCount') or 0)}대` "
                f"(후보 `{int(report_summary.get('scheduledDeviceCount') or 0)}대`)"
            ),
        ]
    )

    if hospital_seq is None:
        lines.append(f"• 결과: {_display_value(report_summary.get('summaryLine'), default='점검 대상 병원이 없어')}")
        return "\n".join(lines)

    lines.extend(_build_daily_device_round_summary_lines(report_summary))

    if not actionable_device_results:
        lines.append("• 결과: 문제 있거나 작업한 장비가 없어")
        return "\n".join(lines)

    lines.append("")
    lines.append("*문제/작업 장비*")
    for index, item in enumerate(actionable_device_results):
        if index > 0:
            lines.append("")
        lines.append(_build_daily_device_round_device_line(item))

    return "\n".join(lines)


def _build_daily_device_round_blocks(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
    include_header: bool = True,
) -> list[dict[str, Any]]:
    local_now = _coerce_daily_device_round_now(now)
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    hospital_name = _display_value(report_summary.get("hospitalName"), default="미선정")
    device_results = report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []
    actionable_device_results = _collect_daily_device_round_actionable_device_results(device_results)

    blocks: list[dict[str, Any]] = []
    if include_header:
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": _build_daily_device_round_title_text(report_summary),
                },
            }
        )

    hospital_label = _format_daily_device_round_hospital_label(hospital_name, hospital_seq)
    if hospital_seq is not None:
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": hospital_label,
                },
            }
        )
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"발송 `{local_now:%Y-%m-%d %H:%M:%S} KST` | 장비 `{int(report_summary.get('deviceCount') or 0)}대`",
                    }
                ],
            }
        )
    else:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": (
                            f"병원 *{hospital_label}* | 발송 `{local_now:%Y-%m-%d %H:%M:%S} KST` | "
                            f"장비 `{int(report_summary.get('deviceCount') or 0)}대`"
                        ),
                    }
                ],
            }
        )

    if hospital_seq is None:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*결과*\n{_display_value(report_summary.get('summaryLine'), default='점검 대상 병원이 없어')}",
                },
            }
        )
        return blocks

    blocks.append(_build_daily_device_round_summary_rich_text_block(report_summary))

    if not actionable_device_results:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*결과*\n문제 있거나 작업한 장비가 없어",
                },
            }
        )
        return blocks

    blocks.append({"type": "divider"})
    for item in actionable_device_results:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _build_daily_device_round_device_line(item),
                },
            }
        )
    return blocks
