from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.core.utils import _display_value
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company.routers.device_status_probe import _probe_device_status_overview
from boxer_company.routers.device_update import (
    _describe_agent_box_update_gate,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
    _resolve_agent_runtime_version,
)

_DAILY_DEVICE_ROUND_TIMEZONE = ZoneInfo("Asia/Seoul")
_DAILY_DEVICE_ROUND_TITLE = "일일 장비 순회 점검 & 업데이트"
_DAILY_DEVICE_ROUND_MAX_DEVICE_LINES = 20


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
    for key in ("audio", "pm2", "captureboard", "led"):
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
    agent_latest = bool(agent_repo.get("latest")) if agent_repo_available else False
    box_already_latest = bool(latest_box_version and current_box_version == latest_box_version)

    if not device_connected:
        agent_reason = "장비 agent 연결 끊김"
    elif not agent_repo_available:
        agent_reason = "에이전트 repo 상태 확인 필요"
    elif agent_latest:
        agent_reason = "에이전트 최신"
    else:
        agent_reason = f"에이전트 {current_agent_version or '미확인'} 업데이트 필요"

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
            "isLatest": agent_latest,
            "shouldUpdate": bool(device_connected and agent_repo_available and not agent_latest),
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
        if plan.get("isLatest"):
            return "에이전트 최신"
        if plan.get("shouldUpdate"):
            return "에이전트 업데이트 후보"
        return _display_value(plan.get("reason"), default="에이전트 확인 필요")

    if plan.get("alreadyLatest"):
        return "박스 최신"
    if plan.get("shouldUpdate"):
        return "박스 업데이트 후보"
    return _display_value(plan.get("reason"), default="박스 확인 필요")


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


def _describe_daily_device_round_route_summary(
    device_result: dict[str, Any],
    *,
    route_kind: str,
) -> tuple[str, str]:
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
                    return "성공", f"`{previous_version}` -> `{current_version}`"
                return "성공", f"버전 `{current_version or '미확인'}`"
            if status == "dispatch_failed":
                return "실패", _display_value(plan.get("reason"), default="업데이트 실패")
            return "확인 필요", _display_value(plan.get("reason"), default="업데이트 확인 필요")
        if plan.get("isLatest"):
            return "최신", f"이번 업데이트 불필요 | 버전 `{current_version or '미확인'}`"
        if plan.get("shouldUpdate"):
            return "대기", _display_value(plan.get("reason"), default="업데이트 후보")
        return "확인 필요", _display_value(plan.get("reason"), default="상태 확인 필요")

    if action:
        status = _display_value(action.get("status"), default="")
        if status in {"completed", "already_latest"} and action.get("ok"):
            final_version = current_version or latest_version
            if previous_version and final_version and previous_version != final_version:
                return "성공", f"`{previous_version}` -> `{final_version}`"
            return "성공", f"버전 `{final_version or '미확인'}`"
        if status == "dispatch_failed":
            return "실패", _display_value(plan.get("reason"), default="업데이트 실패")
        return "확인 필요", _display_value(plan.get("reason"), default="업데이트 확인 필요")
    if plan.get("alreadyLatest"):
        return "최신", f"이번 업데이트 불필요 | 버전 `{current_version or latest_version or '미확인'}`"
    if plan.get("shouldUpdate"):
        return "대기", _display_value(plan.get("reason"), default="업데이트 후보")
    return "확인 필요", _display_value(plan.get("reason"), default="상태 확인 필요")


def _build_daily_device_round_device_line(device_result: dict[str, Any]) -> str:
    component_labels = device_result.get("componentLabels") if isinstance(device_result.get("componentLabels"), dict) else {}
    component_text = " | ".join(
        [
            f"`오디오 {component_labels.get('audio', '확인 필요')}`",
            f"`pm2 {component_labels.get('pm2', '확인 필요')}`",
            f"`캡처보드 {component_labels.get('captureboard', '확인 필요')}`",
            f"`LED {component_labels.get('led', '확인 필요')}`",
        ]
    )
    agent_status, agent_detail = _describe_daily_device_round_route_summary(device_result, route_kind="agent")
    box_status, box_detail = _describe_daily_device_round_route_summary(device_result, route_kind="box")
    room_name = _display_value(device_result.get("roomName"), default="")
    header = f"• *{_display_value(device_result.get('deviceName'), default='미확인')}*"
    if room_name and room_name != "미확인":
        header = f"{header} `{room_name}`"
    return "\n".join(
        [
            header,
            f"  *상태*  *{_display_value(device_result.get('overallLabel'), default='확인 필요')}*",
            f"  *점검*  {component_text}",
            f"  *에이전트*  *{agent_status}* | {agent_detail}",
            f"  *박스*  *{box_status}* | {box_detail}",
        ]
    )


def _run_daily_device_round_for_device(
    device_name: str,
    *,
    auto_update_agent: bool = False,
    auto_update_box: bool = False,
) -> dict[str, Any]:
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

    final_plan = _build_daily_device_round_update_plan(update_status_payload)
    overview = status_payload.get("overview") if isinstance(status_payload.get("overview"), dict) else {}
    device_payload = update_status_payload.get("device") if isinstance(update_status_payload.get("device"), dict) else {}
    component_labels = {
        "audio": _display_value(((overview.get("audio") or {}) if isinstance(overview.get("audio"), dict) else {}).get("label"), default="확인 필요"),
        "pm2": _display_value(((overview.get("pm2") or {}) if isinstance(overview.get("pm2"), dict) else {}).get("label"), default="확인 필요"),
        "captureboard": _display_value(((overview.get("captureboard") or {}) if isinstance(overview.get("captureboard"), dict) else {}).get("label"), default="확인 필요"),
        "led": _display_value(((overview.get("led") or {}) if isinstance(overview.get("led"), dict) else {}).get("label"), default="확인 필요"),
    }

    return {
        "deviceName": _display_value(device_payload.get("deviceName"), default=device_name),
        "hospitalName": _display_value(device_payload.get("hospitalName"), default="미확인"),
        "roomName": _display_value(device_payload.get("roomName"), default="미확인"),
        "overallLabel": _daily_device_round_status_label(status_payload),
        "componentLabels": component_labels,
        "statusText": status_text,
        "statusPayload": status_payload,
        "initialUpdateStatusText": initial_update_status_text,
        "initialPlan": initial_plan,
        "finalUpdateStatusText": update_status_text,
        "finalUpdateStatusPayload": update_status_payload,
        "finalPlan": final_plan,
        "agentAction": agent_action,
        "boxAction": box_action,
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
    }


def _build_daily_device_round_error_result(
    device_context: dict[str, Any],
    exc: Exception,
) -> dict[str, Any]:
    return {
        "deviceName": _display_value(device_context.get("deviceName"), default="미확인"),
        "hospitalName": _display_value(device_context.get("hospitalName"), default="미확인"),
        "roomName": _display_value(device_context.get("roomName"), default="미확인"),
        "overallLabel": "점검 불가",
        "componentLabels": {
            "audio": "점검 불가",
            "pm2": "점검 불가",
            "captureboard": "점검 불가",
            "led": "점검 불가",
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
        "agentActionText": "에이전트 점검 실패",
        "boxActionText": "박스 점검 실패",
        "error": f"{type(exc).__name__}: {exc}",
    }


def _build_daily_device_round_summary(
    *,
    now: datetime | None = None,
    state: dict[str, Any] | None = None,
    auto_update_agent: bool = False,
    auto_update_box: bool = False,
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
    device_results: list[dict[str, Any]] = []
    for device in devices:
        try:
            device_results.append(
                _run_daily_device_round_for_device(
                    _display_value(device.get("deviceName"), default=""),
                    auto_update_agent=auto_update_agent,
                    auto_update_box=auto_update_box,
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
        "statusCounts": status_counts,
        "updateCounts": update_counts,
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
    status_counts = report_summary.get("statusCounts") if isinstance(report_summary.get("statusCounts"), dict) else {}
    update_counts = report_summary.get("updateCounts") if isinstance(report_summary.get("updateCounts"), dict) else {}
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    device_results = report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []

    lines: list[str] = []
    if include_title:
        lines.append(f"*{_build_daily_device_round_title_text(report_summary)}*")

    hospital_detail = _format_daily_device_round_hospital_label(
        report_summary.get("hospitalName"),
        hospital_seq,
    )
    lines.extend(
        [
            f"• 병원: {hospital_detail}",
            f"• 실행: `{local_now:%Y-%m-%d %H:%M:%S} KST`",
            (
                f"• 자동 업데이트: 에이전트 `{'켜짐' if report_summary.get('autoUpdateAgent') else '꺼짐'}` / "
                f"박스 `{'켜짐' if report_summary.get('autoUpdateBox') else '꺼짐'}`"
            ),
            (
                f"• 장비: `{int(report_summary.get('deviceCount') or 0)}대` "
                f"(후보 `{int(report_summary.get('scheduledDeviceCount') or 0)}대`)"
            ),
        ]
    )

    if hospital_seq is None:
        lines.append(f"• 결과: {_display_value(report_summary.get('summaryLine'), default='점검 대상 병원이 없어')}")
        return "\n".join(lines)

    lines.extend(
        [
            (
                f"• 상태: 정상 `{int(status_counts.get('정상') or 0)}` / "
                f"확인 필요 `{int(status_counts.get('확인 필요') or 0)}` / "
                f"이상 `{int(status_counts.get('이상') or 0)}` / "
                f"점검 불가 `{int(status_counts.get('점검 불가') or 0)}`"
            ),
            (
                f"• 업데이트: 에이전트 성공 `{int(update_counts.get('agentUpdated') or 0)}` "
                f"(대상 `{int(update_counts.get('agentCandidates') or 0)}`, 실패 `{int(update_counts.get('agentUpdateFailed') or 0)}`) / "
                f"박스 성공 `{int(update_counts.get('boxUpdated') or 0)}` "
                f"(대상 `{int(update_counts.get('boxCandidates') or 0)}`, 실패 `{int(update_counts.get('boxUpdateFailed') or 0)}`)"
            ),
        ]
    )

    if not device_results:
        lines.append("• 결과: 점검할 장비가 없어")
        return "\n".join(lines)

    lines.append("")
    lines.append("*장비별 결과*")
    for index, item in enumerate(device_results[:_DAILY_DEVICE_ROUND_MAX_DEVICE_LINES]):
        if index > 0:
            lines.append("")
        lines.append(_build_daily_device_round_device_line(item))
    if len(device_results) > _DAILY_DEVICE_ROUND_MAX_DEVICE_LINES:
        lines.append(f"• 참고: 장비 `{_DAILY_DEVICE_ROUND_MAX_DEVICE_LINES}대`까지만 표시했어")

    return "\n".join(lines)


def _build_daily_device_round_blocks(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
    include_header: bool = True,
) -> list[dict[str, Any]]:
    local_now = _coerce_daily_device_round_now(now)
    status_counts = report_summary.get("statusCounts") if isinstance(report_summary.get("statusCounts"), dict) else {}
    update_counts = report_summary.get("updateCounts") if isinstance(report_summary.get("updateCounts"), dict) else {}
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    hospital_name = _display_value(report_summary.get("hospitalName"), default="미선정")
    device_results = report_summary.get("deviceResults") if isinstance(report_summary.get("deviceResults"), list) else []

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
    blocks.append(
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"병원 *{hospital_label}* | 발송 `{local_now:%Y-%m-%d %H:%M:%S} KST` | "
                        f"에이전트 자동업데이트 `{'켜짐' if report_summary.get('autoUpdateAgent') else '꺼짐'}` | "
                        f"박스 자동업데이트 `{'켜짐' if report_summary.get('autoUpdateBox') else '꺼짐'}`"
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

    blocks.append(
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": (
                        "*상태*\n"
                        f"정상 `{int(status_counts.get('정상') or 0)}` / "
                        f"확인 필요 `{int(status_counts.get('확인 필요') or 0)}` / "
                        f"이상 `{int(status_counts.get('이상') or 0)}` / "
                        f"점검 불가 `{int(status_counts.get('점검 불가') or 0)}`"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "*업데이트*\n"
                        f"에이전트 성공 `{int(update_counts.get('agentUpdated') or 0)}` "
                        f"(대상 `{int(update_counts.get('agentCandidates') or 0)}`)\n"
                        f"박스 성공 `{int(update_counts.get('boxUpdated') or 0)}` "
                        f"(대상 `{int(update_counts.get('boxCandidates') or 0)}`)"
                    ),
                },
            ],
        }
    )

    if not device_results:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*결과*\n점검할 장비가 없어",
                },
            }
        )
        return blocks

    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*장비별 결과*",
            },
        }
    )
    for item in device_results[:_DAILY_DEVICE_ROUND_MAX_DEVICE_LINES]:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _build_daily_device_round_device_line(item),
                },
            }
        )
    if len(device_results) > _DAILY_DEVICE_ROUND_MAX_DEVICE_LINES:
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"장비 `{_DAILY_DEVICE_ROUND_MAX_DEVICE_LINES}대`까지만 표시",
                    }
                ],
            }
        )
    return blocks
