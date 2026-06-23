import re
import threading
import time
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core.utils import _display_value, _truncate_text
from boxer_company import settings as cs
from boxer_company.routers.barcode_log import _extract_device_name_scope
from boxer_company.routers.device_file_probe import _connect_device_ssh_client
from boxer_company.routers.device_status_probe import (
    _display_device_status_probe_reason,
    _parse_pm2_processes,
    _run_remote_ssh_command,
)
from boxer_company.routers.mda_graphql import _wait_for_mda_device_agent_ssh

_LEADING_DEVICE_DIAGNOSTIC_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)
_DEVICE_DIAGNOSTIC_START_HINTS = (
    "진단 시작",
    "진단시작",
)
_DEVICE_DIAGNOSTIC_LOG_PATTERN = re.compile(
    r"(error|exception|fatal|fail|failed|restart|restarted|exit|exited|oom|out of memory|"
    r"killed process|heap|segfault|timeout|timed out|에러|오류|실패|재시작|재부팅)",
    re.IGNORECASE,
)
_DEVICE_DIAGNOSTIC_LIVE_FOLLOWUP_HINTS = (
    "왜",
    "원인",
    "로그",
    "log",
    "앱",
    "app",
    "pm2",
    "시스템",
    "system",
    "os",
    "journal",
    "꺼졌",
    "꺼짐",
    "종료",
    "전원",
    "재시작",
    "재부팅",
    "크래시",
    "에러",
    "오류",
    "실패",
    "메모리",
    "memory",
    "디스크",
    "disk",
)
_DEVICE_DIAGNOSTIC_APP_LOG_HINTS = (
    "왜",
    "원인",
    "로그",
    "log",
    "앱",
    "app",
    "pm2",
    "재시작",
    "크래시",
    "에러",
    "오류",
    "실패",
)
_DEVICE_DIAGNOSTIC_SYSTEM_LOG_HINTS = (
    "시스템",
    "system",
    "os",
    "journal",
    "꺼졌",
    "꺼짐",
    "종료",
    "전원",
    "재부팅",
    "크래시",
)
_DEVICE_DIAGNOSTIC_MEMORY_HINTS = ("메모리", "memory", "oom", "out of memory")
_DEVICE_DIAGNOSTIC_DISK_HINTS = ("디스크", "disk", "용량", "저장공간")
_LOGIN_SHELL_USER_PATH_EXPORT = 'export PATH="$HOME/.npm-global/bin:$HOME/bin:/usr/local/bin:$PATH"; '
_PM2_JLIST_COMMAND = (
    "bash -lc '"
    f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
    "if command -v pm2 >/dev/null 2>&1; then "
    "pm2 jlist 2>&1; "
    "else echo pm2_missing; fi'"
)
_DEVICE_DIAGNOSTIC_COMMANDS: dict[str, dict[str, Any]] = {
    "uptime": {
        "summary": "uptime/load 확인",
        "timeout_sec": 8,
        "command": "sh -lc 'uptime 2>&1'",
    },
    "memory": {
        "summary": "메모리 사용량 확인",
        "timeout_sec": 8,
        "command": "sh -lc 'free -m 2>&1 || vm_stat 2>&1'",
    },
    "disk": {
        "summary": "디스크 사용량 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'df -h / /home/mommytalk /home/mommytalk/AppData "
            "/home/mommytalk/AppData/TrashCan 2>&1 | head -80'"
        ),
    },
    "pm2_jlist": {
        "summary": "PM2 앱 상태 확인",
        "timeout_sec": 12,
        "command": _PM2_JLIST_COMMAND,
    },
    "pm2_describe_box": {
        "summary": "마미박스 PM2 상세 확인",
        "timeout_sec": 12,
        "command": (
            "bash -lc '"
            f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
            "if command -v pm2 >/dev/null 2>&1; then "
            "pm2 describe mommybox-v2 2>&1 || true; "
            "else echo pm2_missing; fi'"
        ),
    },
    "pm2_describe_agent": {
        "summary": "에이전트 PM2 상세 확인",
        "timeout_sec": 12,
        "command": (
            "bash -lc '"
            f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
            "if command -v pm2 >/dev/null 2>&1; then "
            "pm2 describe mommybox-v2-agent 2>&1 || pm2 describe mommybox-agent 2>&1 || true; "
            "else echo pm2_missing; fi'"
        ),
    },
    "pm2_logs_box": {
        "summary": "마미박스 최근 PM2 로그 확인",
        "timeout_sec": 20,
        "command": (
            "bash -lc '"
            f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
            "if command -v pm2 >/dev/null 2>&1; then "
            "pm2 logs mommybox-v2 --nostream --lines 120 2>&1 || true; "
            "else echo pm2_missing; fi'"
        ),
    },
    "pm2_logs_agent": {
        "summary": "에이전트 최근 PM2 로그 확인",
        "timeout_sec": 20,
        "command": (
            "bash -lc '"
            f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
            "if command -v pm2 >/dev/null 2>&1; then "
            "pm2 logs mommybox-v2-agent --nostream --lines 80 2>&1 "
            "|| pm2 logs mommybox-agent --nostream --lines 80 2>&1 || true; "
            "else echo pm2_missing; fi'"
        ),
    },
    "reboot_history": {
        "summary": "최근 재부팅 이력 확인",
        "timeout_sec": 10,
        "command": "sh -lc 'last -x reboot -n 5 2>&1 || true'",
    },
    "kernel_oom": {
        "summary": "커널 OOM/크래시 단서 확인",
        "timeout_sec": 12,
        "command": (
            "sh -lc '(journalctl -k --no-pager -n 300 2>/dev/null || dmesg 2>/dev/null || true) "
            "| grep -Ei \"oom|out of memory|killed process|segfault|thermal|i/o error|ext4-fs error\" "
            "| tail -80'"
        ),
    },
    "app_recent_logs": {
        "summary": "앱 로그 파일 최근 내용 확인",
        "timeout_sec": 18,
        "command": (
            "sh -lc '"
            "for dir in \"$HOME/.pm2/logs\" \"$HOME/AppData\" \"$HOME/AppData/logs\" \"$HOME/AppData/Logs\"; do "
            "[ -d \"$dir\" ] && find \"$dir\" -maxdepth 2 -type f "
            "\\( -iname \"*.log\" -o -iname \"*.txt\" \\) -printf \"%T@ %p\\n\" 2>/dev/null; "
            "done | sort -nr | head -8 | cut -d\" \" -f2- | "
            "while IFS= read -r file; do "
            "[ -f \"$file\" ] || continue; "
            "echo \"== $file ==\"; tail -80 \"$file\" 2>&1; "
            "done | tail -500'"
        ),
    },
    "system_journal_recent": {
        "summary": "최근 시스템 종료/재부팅 로그 확인",
        "timeout_sec": 15,
        "command": (
            "sh -lc '(journalctl --no-pager -n 500 -o short-iso 2>/dev/null || true) "
            "| grep -Ei \"power key|powering off|system is powering down|shutdown|reboot|"
            "stopping pm2|segfault|oom|out of memory|killed process|uncaught|unhandled\" "
            "| tail -160'"
        ),
    },
}
_DEVICE_DIAGNOSTIC_SNAPSHOTS: dict[str, dict[str, Any]] = {}
_DEVICE_DIAGNOSTIC_SNAPSHOTS_LOCK = threading.Lock()


def _normalize_device_diagnostic_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _has_device_diagnostic_start_hint(question: str) -> bool:
    normalized = _normalize_device_diagnostic_question(question)
    return any(hint in normalized for hint in _DEVICE_DIAGNOSTIC_START_HINTS)


def _extract_device_name_for_diagnostic_start(question: str) -> str | None:
    normalized = _normalize_device_diagnostic_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _has_device_diagnostic_start_hint(normalized):
        return extracted

    matched = _LEADING_DEVICE_DIAGNOSTIC_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _has_device_diagnostic_start_hint(remainder):
        return None
    return candidate


def _is_device_diagnostic_start_request(question: str, device_name: str | None = None) -> bool:
    resolved_device_name = str(device_name or _extract_device_name_for_diagnostic_start(question) or "").strip()
    return bool(resolved_device_name and _has_device_diagnostic_start_hint(question))


def _is_device_diagnostic_runtime_configured() -> bool:
    return bool(
        cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


def _extract_device_name_for_diagnostic_freeform(question: str) -> str | None:
    device_name = _extract_device_name_scope(_normalize_device_diagnostic_question(question))
    if not device_name:
        return None
    if not _select_device_diagnostic_followup_command_keys(question):
        return None
    return device_name


def _is_device_diagnostic_freeform_request(question: str, device_name: str | None = None) -> bool:
    resolved_device_name = str(device_name or _extract_device_name_for_diagnostic_freeform(question) or "").strip()
    return bool(resolved_device_name and _select_device_diagnostic_followup_command_keys(question))


def _build_device_diagnostic_config_message() -> str:
    return (
        "장비 진단 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD, DEVICE_SSH_PASSWORD가 필요해"
    )


def _build_device_diagnostic_device_required_message() -> str:
    return "진단 시작은 장비명이 필요해. 예: `MB2-C00419 진단 시작`"


def _device_diagnostic_now() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).isoformat(timespec="seconds")


def _device_diagnostic_snapshot_key(workspace_id: str, channel_id: str, thread_ts: str) -> str:
    return "|".join(
        (
            _display_value(workspace_id, default="unknown"),
            _display_value(channel_id, default="unknown"),
            _display_value(thread_ts, default="unknown"),
        )
    )


def _device_diagnostic_snapshot_ttl_sec() -> int:
    return max(60, int(getattr(cs, "DEVICE_DIAGNOSTIC_SNAPSHOT_TTL_SEC", 3600) or 3600))


def _cleanup_device_diagnostic_snapshots(now: float | None = None) -> None:
    current = time.time() if now is None else float(now)
    expired_keys = [
        key
        for key, snapshot in _DEVICE_DIAGNOSTIC_SNAPSHOTS.items()
        if float(snapshot.get("expiresAtEpoch") or 0) <= current
    ]
    for key in expired_keys:
        _DEVICE_DIAGNOSTIC_SNAPSHOTS.pop(key, None)


def _save_device_diagnostic_snapshot(
    *,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    snapshot: dict[str, Any],
) -> None:
    key = _device_diagnostic_snapshot_key(workspace_id, channel_id, thread_ts)
    ttl_sec = _device_diagnostic_snapshot_ttl_sec()
    now = time.time()
    normalized_snapshot = dict(snapshot)
    normalized_snapshot["storedAtEpoch"] = now
    normalized_snapshot["expiresAtEpoch"] = now + ttl_sec
    normalized_snapshot["ttlSec"] = ttl_sec

    with _DEVICE_DIAGNOSTIC_SNAPSHOTS_LOCK:
        _cleanup_device_diagnostic_snapshots(now)
        _DEVICE_DIAGNOSTIC_SNAPSHOTS[key] = normalized_snapshot


def _load_device_diagnostic_snapshot(
    *,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
) -> dict[str, Any] | None:
    key = _device_diagnostic_snapshot_key(workspace_id, channel_id, thread_ts)
    now = time.time()
    with _DEVICE_DIAGNOSTIC_SNAPSHOTS_LOCK:
        _cleanup_device_diagnostic_snapshots(now)
        snapshot = _DEVICE_DIAGNOSTIC_SNAPSHOTS.get(key)
        return dict(snapshot) if isinstance(snapshot, dict) else None


def _clear_device_diagnostic_snapshots() -> None:
    with _DEVICE_DIAGNOSTIC_SNAPSHOTS_LOCK:
        _DEVICE_DIAGNOSTIC_SNAPSHOTS.clear()


def _build_device_diagnostic_ssh_state(wait_result: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    device_info = wait_result.get("device") if isinstance(wait_result.get("device"), dict) else {}
    agent_ssh = device_info.get("agentSsh") if isinstance(device_info.get("agentSsh"), dict) else {}
    host = _display_value(agent_ssh.get("host"), default="")
    try:
        port = int(agent_ssh.get("port") or 0)
    except (TypeError, ValueError):
        port = 0

    ready = bool(wait_result.get("ready")) and bool(host) and port > 0
    ssh_opened = bool(wait_result.get("opened")) and not bool(wait_result.get("reusedExisting"))
    return device_info, {
        "ready": ready,
        "reason": "ready" if ready else "agent_ssh_not_ready",
        "host": host,
        "port": port,
        "pollCount": wait_result.get("pollCount"),
        "reusedExisting": bool(wait_result.get("reusedExisting")),
        "opened": ssh_opened,
        "readOnly": True,
    }


def _run_device_diagnostic_commands(
    client: Any,
    command_keys: list[str] | None = None,
) -> dict[str, dict[str, Any]]:
    results: dict[str, dict[str, Any]] = {}
    selected_keys = command_keys or list(_DEVICE_DIAGNOSTIC_COMMANDS.keys())
    for key in selected_keys:
        spec = _DEVICE_DIAGNOSTIC_COMMANDS.get(key)
        if not spec:
            continue
        # 진단 시작은 상태 확인 전용이라 command registry도 read-only 명령만 둔다.
        result = _run_remote_ssh_command(
            client,
            command=str(spec["command"]),
            summary=str(spec["summary"]),
            timeout_sec=max(1, int(spec["timeout_sec"])),
        )
        output = _display_value(result.get("output"), default="")
        result["output"] = _truncate_text(output, 8000)
        results[key] = result
    return results


def _has_any_device_diagnostic_hint(question: str, hints: tuple[str, ...]) -> bool:
    text = _normalize_device_diagnostic_question(question)
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in hints)


def _select_device_diagnostic_followup_command_keys(question: str) -> list[str]:
    if not _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_LIVE_FOLLOWUP_HINTS):
        return []

    selected: list[str] = ["pm2_jlist"]
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_APP_LOG_HINTS):
        selected.extend(["pm2_describe_box", "pm2_describe_agent", "pm2_logs_box", "pm2_logs_agent", "app_recent_logs"])
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_SYSTEM_LOG_HINTS):
        selected.extend(["reboot_history", "system_journal_recent", "kernel_oom"])
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_MEMORY_HINTS):
        selected.extend(["memory", "kernel_oom"])
    if _has_any_device_diagnostic_hint(question, _DEVICE_DIAGNOSTIC_DISK_HINTS):
        selected.append("disk")

    deduped: list[str] = []
    for key in selected:
        if key not in deduped:
            deduped.append(key)
    return deduped


def _extract_device_diagnostic_log_lines(checks: dict[str, dict[str, Any]]) -> list[dict[str, str]]:
    lines: list[dict[str, str]] = []
    for key in ("pm2_logs_box", "pm2_logs_agent", "app_recent_logs", "system_journal_recent", "kernel_oom"):
        result = checks.get(key) if isinstance(checks.get(key), dict) else {}
        output = _display_value((result or {}).get("output"), default="")
        for raw_line in output.splitlines():
            line = " ".join(raw_line.strip().split())
            if not line or not _DEVICE_DIAGNOSTIC_LOG_PATTERN.search(line):
                continue
            lines.append(
                {
                    "source": key,
                    "line": _truncate_text(line, 500),
                }
            )
            if len(lines) >= 24:
                return lines
    return lines


def _build_device_diagnostic_pm2_summary(checks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    pm2_result = checks.get("pm2_jlist") if isinstance(checks.get("pm2_jlist"), dict) else {}
    parsed = _parse_pm2_processes(_display_value((pm2_result or {}).get("output"), default=""))
    processes = [
        process
        for process in (parsed.get("processes") or [])
        if isinstance(process, dict)
    ]
    target_processes = [
        process
        for process in processes
        if _display_value(process.get("name"), default="")
        in {"mommybox-v2", "mommybox-v2-agent", "mommybox-agent"}
    ]
    return {
        "available": bool(parsed.get("available")),
        "reason": _display_value(parsed.get("reason"), default=""),
        "processes": target_processes,
        "processCount": len(processes),
    }


def _build_device_diagnostic_summary(snapshot: dict[str, Any]) -> dict[str, Any]:
    checks = snapshot.get("checks") if isinstance(snapshot.get("checks"), dict) else {}
    pm2 = _build_device_diagnostic_pm2_summary(checks)
    interesting_lines = _extract_device_diagnostic_log_lines(checks)
    ssh = snapshot.get("ssh") if isinstance(snapshot.get("ssh"), dict) else {}
    return {
        "sshReady": bool(ssh.get("ready")),
        "pm2": pm2,
        "interestingLogLines": interesting_lines,
        "interestingLogLineCount": len(interesting_lines),
    }


def _device_diagnostic_snapshot_device_name(snapshot: dict[str, Any]) -> str:
    request = snapshot.get("request") if isinstance(snapshot.get("request"), dict) else {}
    device = snapshot.get("device") if isinstance(snapshot.get("device"), dict) else {}
    return _display_value(
        request.get("deviceName") or device.get("deviceName"),
        default="",
    ).strip()


def _format_diagnostic_pm2_line(pm2: dict[str, Any]) -> str:
    processes = pm2.get("processes") if isinstance(pm2.get("processes"), list) else []
    if not pm2.get("available"):
        return f"확인 불가 (`{_display_value(pm2.get('reason'), default='unknown')}`)"
    if not processes:
        return "대상 앱 미감지"

    parts = []
    for process in processes:
        if not isinstance(process, dict):
            continue
        name = _display_value(process.get("name"), default="미확인")
        status = _display_value(process.get("status"), default="미확인")
        version = _display_value(process.get("version"), default="")
        restart_count = int(process.get("restartCount") or 0)
        version_part = f" v{version}" if version else ""
        parts.append(f"{name} {status}{version_part} / 재시작 {restart_count}회")
    return " / ".join(parts) if parts else "대상 앱 미감지"


def _build_device_diagnostic_start_reply(snapshot: dict[str, Any]) -> str:
    request = snapshot.get("request") if isinstance(snapshot.get("request"), dict) else {}
    device = snapshot.get("device") if isinstance(snapshot.get("device"), dict) else {}
    ssh = snapshot.get("ssh") if isinstance(snapshot.get("ssh"), dict) else {}
    summary = snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {}
    pm2 = summary.get("pm2") if isinstance(summary.get("pm2"), dict) else {}

    device_name = _display_value(device.get("deviceName"), default=_display_value(request.get("deviceName"), default="미확인"))
    lines = [
        "*장비 진단 스냅샷*",
        f"• 장비: `{device_name}`",
    ]
    hospital_name = _display_value(device.get("hospitalName"), default="")
    room_name = _display_value(device.get("roomName"), default="")
    if hospital_name or room_name:
        lines.append(f"• 위치: `{hospital_name or '미확인'}` / `{room_name or '미확인'}`")
    lines.extend(
        [
            f"• 수집 시각: `{_display_value(request.get('capturedAt'), default='미확인')}`",
            "• 모드: 조회 전용 (SSH open 허용, ping/업데이트/재시작 미실행)",
            f"• SSH: {'준비됨' if bool(ssh.get('ready')) else '미준비'}"
            + ("" if bool(ssh.get("ready")) else f" | {_display_device_status_probe_reason(_display_value(ssh.get('reason'), default=''))}"),
        ]
    )
    if bool(ssh.get("ready")):
        lines.append(f"• PM2: {_format_diagnostic_pm2_line(pm2)}")
        lines.append(f"• 로그 단서: `{int(summary.get('interestingLogLineCount') or 0)}건`")
    lines.append("• 다음: 이 thread에서 `왜 반복 재시작해?`처럼 물어보면 이 스냅샷 기준으로 답할게")
    return "\n".join(lines)


def _collect_device_diagnostic_snapshot(
    *,
    device_name: str,
    question: str,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    requested_by: str | None,
) -> dict[str, Any]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 진단 시작`")

    # 진단 시작은 원인 조사 접근을 위해 SSH open만 허용한다. 이후 원격 실행은 상태 조회와 로그 확인
    # 같은 read-only 명령으로 제한하고, 업데이트/재시작/종료 계열 조작 명령은 넣지 않는다.
    device_info, ssh_state = _build_device_diagnostic_ssh_state(
        _wait_for_mda_device_agent_ssh(normalized_device_name)
    )

    snapshot: dict[str, Any] = {
        "route": "device_diagnostic_snapshot",
        "source": "mda_graphql_ssh_open+ssh_read",
        "request": {
            "deviceName": normalized_device_name,
            "question": question,
            "capturedAt": _device_diagnostic_now(),
            "workspaceId": workspace_id,
            "channelId": channel_id,
            "threadTs": thread_ts,
            "requestedBy": requested_by,
        },
        "device": {
            "deviceName": _display_value(device_info.get("deviceName"), default=normalized_device_name),
            "version": _display_value(device_info.get("version"), default=""),
            "hospitalName": _display_value(device_info.get("hospitalName"), default=""),
            "roomName": _display_value(device_info.get("roomName"), default=""),
            "isConnected": bool(device_info.get("isConnected")),
            "deviceIsConnected": bool(device_info.get("deviceIsConnected")),
            "deviceStatus": _display_value(device_info.get("deviceStatus"), default=""),
            "agentVersion": _display_value(device_info.get("agentVersion"), default=""),
            "agentConnectedAt": _display_value(device_info.get("agentConnectedAt"), default=""),
            "agentUpdatedAt": _display_value(device_info.get("agentUpdatedAt"), default=""),
        },
        "mode": {
            "readOnly": True,
            "mdaPingSent": False,
            "sshOpenSent": bool(ssh_state.get("opened")),
            "mutatingCommandsSent": False,
        },
        "ssh": ssh_state,
        "checks": {},
    }
    if not ssh_state.get("ready"):
        snapshot["summary"] = _build_device_diagnostic_summary(snapshot)
        return snapshot

    connection = _connect_device_ssh_client(
        _display_value(ssh_state.get("host"), default=""),
        int(ssh_state.get("port") or 0),
    )
    if not connection.get("ok"):
        snapshot["ssh"] = {
            **ssh_state,
            "ready": False,
            "reason": _display_value(connection.get("reason"), default="ssh_connect_failed"),
        }
        snapshot["summary"] = _build_device_diagnostic_summary(snapshot)
        return snapshot

    client = connection["client"]
    try:
        snapshot["checks"] = _run_device_diagnostic_commands(client)
    finally:
        client.close()
    snapshot["summary"] = _build_device_diagnostic_summary(snapshot)
    return snapshot


def _start_device_diagnostic_snapshot(
    *,
    device_name: str,
    question: str,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    requested_by: str | None,
) -> tuple[str, dict[str, Any]]:
    snapshot = _collect_device_diagnostic_snapshot(
        device_name=device_name,
        question=question,
        workspace_id=workspace_id,
        channel_id=channel_id,
        thread_ts=thread_ts,
        requested_by=requested_by,
    )
    _save_device_diagnostic_snapshot(
        workspace_id=workspace_id,
        channel_id=channel_id,
        thread_ts=thread_ts,
        snapshot=snapshot,
    )
    return _build_device_diagnostic_start_reply(snapshot), snapshot


def _build_device_diagnostic_followup_evidence(
    question: str,
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    evidence = dict(snapshot)
    command_keys = _select_device_diagnostic_followup_command_keys(question)
    device_name = _device_diagnostic_snapshot_device_name(snapshot)
    live_check: dict[str, Any] = {
        "performed": False,
        "readOnly": True,
        "question": question,
        "capturedAt": None,
        "commandKeys": command_keys,
        "mutatingCommandsSent": False,
    }
    evidence["followupLiveCheck"] = live_check

    if not command_keys:
        live_check["reason"] = "no_live_hint"
        return evidence
    if not device_name:
        live_check["reason"] = "missing_device_name"
        return evidence

    live_check["performed"] = True
    live_check["capturedAt"] = _device_diagnostic_now()
    live_check["source"] = "mda_graphql_ssh_open+ssh_read"

    try:
        device_info, ssh_state = _build_device_diagnostic_ssh_state(
            _wait_for_mda_device_agent_ssh(device_name)
        )
        live_check["device"] = {
            "deviceName": _display_value(device_info.get("deviceName"), default=device_name),
            "version": _display_value(device_info.get("version"), default=""),
            "hospitalName": _display_value(device_info.get("hospitalName"), default=""),
            "roomName": _display_value(device_info.get("roomName"), default=""),
            "isConnected": bool(device_info.get("isConnected")),
            "deviceIsConnected": bool(device_info.get("deviceIsConnected")),
            "deviceStatus": _display_value(device_info.get("deviceStatus"), default=""),
            "agentVersion": _display_value(device_info.get("agentVersion"), default=""),
            "agentConnectedAt": _display_value(device_info.get("agentConnectedAt"), default=""),
            "agentUpdatedAt": _display_value(device_info.get("agentUpdatedAt"), default=""),
        }
        live_check["ssh"] = ssh_state
        live_check["sshOpenSent"] = bool(ssh_state.get("opened"))

        if not ssh_state.get("ready"):
            live_check["reason"] = "agent_ssh_not_ready"
            live_check["checks"] = {}
            live_check["summary"] = _build_device_diagnostic_summary(
                {
                    "ssh": ssh_state,
                    "checks": {},
                }
            )
            return evidence

        connection = _connect_device_ssh_client(
            _display_value(ssh_state.get("host"), default=""),
            int(ssh_state.get("port") or 0),
        )
        if not connection.get("ok"):
            ssh_state = {
                **ssh_state,
                "ready": False,
                "reason": _display_value(connection.get("reason"), default="ssh_connect_failed"),
            }
            live_check["ssh"] = ssh_state
            live_check["reason"] = ssh_state["reason"]
            live_check["checks"] = {}
            live_check["summary"] = _build_device_diagnostic_summary(
                {
                    "ssh": ssh_state,
                    "checks": {},
                }
            )
            return evidence

        client = connection["client"]
        try:
            checks = _run_device_diagnostic_commands(client, command_keys=command_keys)
        finally:
            client.close()
        live_check["checks"] = checks
        live_check["summary"] = _build_device_diagnostic_summary(
            {
                "ssh": ssh_state,
                "checks": checks,
            }
        )
        live_check["reason"] = "ok"
        return evidence
    except Exception as exc:
        live_check["reason"] = type(exc).__name__
        live_check["error"] = _truncate_text(str(exc), 300)
        live_check["checks"] = {}
        live_check["summary"] = _build_device_diagnostic_summary(
            {
                "ssh": {"ready": False, "reason": type(exc).__name__},
                "checks": {},
            }
        )
        return evidence


def _build_device_diagnostic_freeform_snapshot(
    *,
    question: str,
    device_name: str,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    requested_by: str | None,
) -> dict[str, Any]:
    normalized_device_name = str(device_name or "").strip()
    return {
        "route": "device_diagnostic_freeform",
        "source": "mda_graphql_ssh_open+ssh_read",
        "request": {
            "deviceName": normalized_device_name,
            "question": question,
            "capturedAt": _device_diagnostic_now(),
            "workspaceId": workspace_id,
            "channelId": channel_id,
            "threadTs": thread_ts,
            "requestedBy": requested_by,
            "autoStarted": True,
        },
        "device": {
            "deviceName": normalized_device_name,
        },
        "mode": {
            "readOnly": True,
            "mdaPingSent": False,
            "sshOpenSent": False,
            "mutatingCommandsSent": False,
            "autoStarted": True,
        },
        "ssh": {
            "ready": False,
            "reason": "not_checked_yet",
        },
        "checks": {},
        "summary": {},
    }


def _start_device_diagnostic_freeform_analysis(
    *,
    question: str,
    device_name: str,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    requested_by: str | None,
) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 찾지 못했어")

    snapshot = _build_device_diagnostic_freeform_snapshot(
        question=question,
        device_name=normalized_device_name,
        workspace_id=workspace_id,
        channel_id=channel_id,
        thread_ts=thread_ts,
        requested_by=requested_by,
    )
    evidence = _build_device_diagnostic_followup_evidence(question, snapshot)
    _save_device_diagnostic_snapshot(
        workspace_id=workspace_id,
        channel_id=channel_id,
        thread_ts=thread_ts,
        snapshot=evidence,
    )
    return _build_device_diagnostic_followup_fallback(question, evidence), evidence


def _build_device_diagnostic_followup_fallback(
    question: str,
    snapshot: dict[str, Any],
) -> str:
    request = snapshot.get("request") if isinstance(snapshot.get("request"), dict) else {}
    live_check = snapshot.get("followupLiveCheck") if isinstance(snapshot.get("followupLiveCheck"), dict) else {}
    live_summary = live_check.get("summary") if isinstance(live_check.get("summary"), dict) else {}
    summary = live_summary or (snapshot.get("summary") if isinstance(snapshot.get("summary"), dict) else {})
    live_ssh = live_check.get("ssh") if isinstance(live_check.get("ssh"), dict) else {}
    ssh = live_ssh or (snapshot.get("ssh") if isinstance(snapshot.get("ssh"), dict) else {})
    pm2 = summary.get("pm2") if isinstance(summary.get("pm2"), dict) else {}
    interesting_lines = (
        summary.get("interestingLogLines")
        if isinstance(summary.get("interestingLogLines"), list)
        else []
    )

    lines = [
        "*장비 진단 답변*",
        f"• 질문: `{_truncate_text((question or '').strip(), 120)}`",
        f"• 장비: `{_display_value(request.get('deviceName'), default='미확인')}`",
        f"• 기준 시각: `{_display_value(request.get('capturedAt'), default='미확인')}`",
    ]
    if live_check.get("performed"):
        lines.append(f"• 추가 조사: 장비 직접 접속으로 `{len(live_check.get('commandKeys') or [])}개` 조회 실행")
        captured_at = _display_value(live_check.get("capturedAt"), default="")
        if captured_at:
            lines.append(f"• 추가 조사 시각: `{captured_at}`")
    elif live_check:
        lines.append("• 추가 조사: 질문상 실시간 장비 로그 조회가 필요하진 않아 기존 스냅샷을 사용했어")

    if not bool(ssh.get("ready")):
        lines.append(f"• 결론: SSH 접속이 안 돼서 앱/시스템 로그까지는 못 봤어")
        lines.append(f"• 근거: {_display_device_status_probe_reason(_display_value(ssh.get('reason'), default=''))}")
        return "\n".join(lines)

    lines.append(f"• PM2: {_format_diagnostic_pm2_line(pm2)}")
    if interesting_lines:
        first_line = interesting_lines[0] if isinstance(interesting_lines[0], dict) else {}
        lines.append(f"• 로그 단서: `{len(interesting_lines)}건` | `{_display_value(first_line.get('line'), default='')}`")
    else:
        lines.append("• 로그 단서: 즉시 눈에 띄는 error/restart/OOM 라인은 없어")
    lines.append("• 안내: 더 정확한 판단은 위 스냅샷 시각 기준이야. 이후 상태가 바뀌었으면 `진단 시작`을 다시 실행해")
    return "\n".join(lines)
