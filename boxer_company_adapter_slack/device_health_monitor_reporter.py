import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from boxer.core import settings as s
from boxer.core.utils import _display_value
from boxer_company import settings as cs
from boxer_company.daily_device_round import (
    _build_daily_device_round_priority,
    _build_daily_device_round_storage_details,
    _coerce_daily_device_round_hospital_seqs,
    _coerce_daily_device_round_now,
    _coerce_int,
    _daily_device_round_status_label,
    _load_daily_device_round_devices,
    _load_daily_device_round_hospital_candidates,
    _resolve_next_daily_device_round_hospital_seq,
    _select_daily_device_round_hospital,
)
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
from boxer_company_adapter_slack.daily_device_round_reporter import (
    _collect_daily_device_round_abnormal_alert_items,
    _post_daily_device_round_abnormal_alert,
)

_DEVICE_HEALTH_MONITOR_THREAD: threading.Thread | None = None
_DEVICE_HEALTH_MONITOR_THREAD_LOCK = threading.Lock()
_DEVICE_HEALTH_MONITOR_RUNTIME_STATE: dict[str, Any] = {}
_DEVICE_HEALTH_MONITOR_RUNTIME_STATE_LOCK = threading.Lock()


def _device_health_monitor_state_path() -> Path:
    return Path(cs.DEVICE_HEALTH_MONITOR_STATE_PATH).expanduser()


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
    normalized_state["sshTunnelRecords"] = _normalize_device_health_monitor_ssh_tunnel_records(
        state_payload.get("sshTunnelRecords")
    )
    return normalized_state


def _is_device_health_monitor_runtime_configured() -> bool:
    return bool(
        cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


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
    candidates = _load_daily_device_round_hospital_candidates()
    candidate_hospital_seqs = {
        hospital_seq
        for hospital_seq in (_coerce_int(item.get("hospitalSeq")) for item in candidates)
        if hospital_seq is not None
    }
    state_payload = state if isinstance(state, dict) else {}
    processed_hospital_seqs = _coerce_daily_device_round_hospital_seqs(
        state_payload.get("processedHospitalSeqs")
    )
    processed_candidate_count = sum(
        1 for hospital_seq in processed_hospital_seqs if hospital_seq in candidate_hospital_seqs
    )
    hospital = _select_daily_device_round_hospital(candidates, state=state_payload)
    if hospital is None:
        return {
            "runDate": local_now.date().isoformat(),
            "startedAt": local_now.isoformat(),
            "finishedAt": local_now.isoformat(),
            "hospitalSeq": None,
            "hospitalName": "미선정",
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
            "candidateHospitalCount": len(candidate_hospital_seqs),
            "sshTunnelRecords": _normalize_device_health_monitor_ssh_tunnel_records(
                state_payload.get("sshTunnelRecords")
            ),
            "summaryLine": (
                "이번 상태 모니터 순회에서 처리할 병원을 모두 끝냈어"
                if candidate_hospital_seqs and processed_candidate_count >= len(candidate_hospital_seqs)
                else "상태 모니터 대상 병원이 없어"
            ),
        }

    hospital_seq = int(hospital["hospitalSeq"])
    devices = _load_daily_device_round_devices(hospital_seq)
    device_results: list[dict[str, Any]] = []
    ssh_tunnel_records = _normalize_device_health_monitor_ssh_tunnel_records(
        state_payload.get("sshTunnelRecords")
    )
    for device_context in devices:
        try:
            device_results.append(
                _run_device_health_monitor_for_device(
                    device_context,
                    now=local_now,
                    ssh_tunnel_records=ssh_tunnel_records,
                )
            )
        except Exception as exc:
            device_results.append(_build_device_health_monitor_error_result(device_context, exc))

    finished_at = _coerce_daily_device_round_now(now)
    status_counts = _build_device_health_monitor_zero_counts()
    for item in device_results:
        label = _display_value(item.get("overallLabel"), default="점검 불가")
        status_counts[label if label in status_counts else "점검 불가"] += 1

    next_hospital_seq = _resolve_next_daily_device_round_hospital_seq(candidates, hospital_seq)
    return {
        "runDate": local_now.date().isoformat(),
        "startedAt": local_now.isoformat(),
        "finishedAt": finished_at.isoformat(),
        "hospitalSeq": hospital_seq,
        "hospitalName": _display_value(hospital.get("hospitalName"), default="미확인"),
        "deviceCount": len(device_results),
        "scheduledDeviceCount": int(hospital.get("deviceCount") or len(device_results)),
        "autoUpdateAgent": False,
        "autoUpdateBox": False,
        "autoCleanupTrashCan": False,
        "autoPowerOff": False,
        "statusCounts": status_counts,
        **_build_device_health_monitor_empty_action_counts(),
        "deviceResults": device_results,
        "nextHospitalSeq": next_hospital_seq,
        "candidateHospitalCount": len(candidate_hospital_seqs),
        "sshTunnelRecords": ssh_tunnel_records,
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
) -> tuple[set[str], dict[str, dict[str, Any]]]:
    alert_fingerprints = _normalize_device_health_monitor_alerts(state.get("alertFingerprints"))
    current_items = _collect_daily_device_round_abnormal_alert_items(report_summary)
    current_fingerprints = {
        _build_device_health_monitor_alert_fingerprint(item)
        for item in current_items
    }
    reminder_delta = _device_health_monitor_alert_reminder_delta()
    now_text = now.isoformat()
    alertable_fingerprints: set[str] = set()
    updated_alerts: dict[str, dict[str, Any]] = {}

    # 같은 장비의 같은 이상은 최초 발견 또는 reminder 주기 경과 때만 다시 알림을 보낸다.
    for fingerprint in current_fingerprints:
        previous = alert_fingerprints.get(fingerprint, {})
        last_alerted_at = _parse_device_health_monitor_datetime(previous.get("lastAlertedAt"))
        should_alert = last_alerted_at is None or now - last_alerted_at >= reminder_delta
        if should_alert:
            alertable_fingerprints.add(fingerprint)
        updated_alerts[fingerprint] = {
            "firstAlertedAt": str(previous.get("firstAlertedAt") or now_text),
            "lastAlertedAt": now_text if should_alert else str(previous.get("lastAlertedAt") or ""),
            "lastSeenAt": now_text,
            "count": max(0, int(previous.get("count") or 0)) + 1,
        }

    return alertable_fingerprints, updated_alerts


def _build_device_health_monitor_next_state(
    state: dict[str, Any],
    report_summary: dict[str, Any],
    *,
    now: datetime,
    alert_fingerprints: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    processed_hospital_seqs = _coerce_daily_device_round_hospital_seqs(state.get("processedHospitalSeqs"))
    if hospital_seq is not None and hospital_seq not in processed_hospital_seqs:
        processed_hospital_seqs.append(hospital_seq)

    candidate_hospital_count = max(0, int(report_summary.get("candidateHospitalCount") or 0))
    if candidate_hospital_count > 0 and len(processed_hospital_seqs) >= candidate_hospital_count:
        processed_hospital_seqs = []

    return {
        **state,
        "processedHospitalSeqs": processed_hospital_seqs,
        "lastRunAt": now.isoformat(),
        "lastHospitalSeq": report_summary.get("hospitalSeq"),
        "lastHospitalName": report_summary.get("hospitalName"),
        "nextHospitalSeq": report_summary.get("nextHospitalSeq"),
        "candidateHospitalCount": candidate_hospital_count,
        "statusCounts": report_summary.get("statusCounts"),
        "alertFingerprints": alert_fingerprints,
        "sshTunnelRecords": _normalize_device_health_monitor_ssh_tunnel_records(
            report_summary.get("sshTunnelRecords")
        ),
    }


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
    if not _is_device_health_monitor_runtime_configured():
        logger.warning("장비 상태 모니터를 켤 수 없어. MDA/SSH 설정이 부족해")
        return False

    channel_id = _device_health_monitor_channel_id()
    if not channel_id:
        logger.warning("장비 상태 모니터 채널 ID가 없어. DEVICE_HEALTH_MONITOR_CHANNEL_ID를 확인해줘")
        return False

    local_now = _coerce_daily_device_round_now(now)
    state = _normalize_device_health_monitor_state(_load_device_health_monitor_state(logger=logger))
    report_summary = _build_device_health_monitor_summary(
        now=local_now,
        state=state,
    )
    if _coerce_int(report_summary.get("hospitalSeq")) is None:
        next_state = {
            **state,
            "processedHospitalSeqs": [],
            "lastRunAt": local_now.isoformat(),
            "candidateHospitalCount": int(report_summary.get("candidateHospitalCount") or 0),
            "statusCounts": report_summary.get("statusCounts"),
        }
        _persist_device_health_monitor_state_best_effort(next_state, logger=logger)
        return False

    alertable_fingerprints, updated_alerts = _collect_device_health_monitor_alert_updates(
        report_summary,
        state,
        now=local_now,
    )
    next_state = _build_device_health_monitor_next_state(
        state,
        report_summary,
        now=local_now,
        alert_fingerprints=updated_alerts,
    )
    _persist_device_health_monitor_state_best_effort(next_state, logger=logger)

    if not alertable_fingerprints:
        logger.info(
            "Checked device health channel=%s hospitalSeq=%s abnormal=0 alertable=0",
            channel_id,
            report_summary.get("hospitalSeq"),
        )
        return False

    alert_summary = _filter_device_health_monitor_alert_summary(report_summary, alertable_fingerprints)
    _post_daily_device_round_abnormal_alert(
        client,
        alert_summary,
        channel_id=channel_id,
        message_ts="",
        logger=logger,
    )
    logger.info(
        "Posted device health alert channel=%s hospitalSeq=%s alertable=%s",
        channel_id,
        report_summary.get("hospitalSeq"),
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


def attach_device_health_monitor_reporter(app: Any, *, logger: logging.Logger | None = None) -> None:
    if not cs.DEVICE_HEALTH_MONITOR_ENABLED:
        return

    actual_logger = logger or logging.getLogger(__name__)
    if not s.DB_QUERY_ENABLED:
        actual_logger.warning(
            "장비 상태 모니터가 활성화됐는데 DB_QUERY_ENABLED가 꺼져 있어 시작하지 않을게"
        )
        return
    if not _is_device_health_monitor_runtime_configured():
        actual_logger.warning(
            "장비 상태 모니터가 활성화됐는데 MDA/SSH 설정이 부족해 시작하지 않을게"
        )
        return
    if not _device_health_monitor_channel_id():
        actual_logger.warning("장비 상태 모니터 채널 ID가 없어 시작하지 않을게")
        return

    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("장비 상태 모니터를 시작하지 못했어. Slack client가 없어")
        return

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
