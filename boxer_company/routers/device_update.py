import json
import re
import time
from collections.abc import Callable
from typing import Any

from boxer.core.utils import _display_value
from boxer_company import settings as cs
from boxer_company.routers.barcode_log import _extract_device_name_scope
from boxer_company.routers.device_file_probe import _connect_device_ssh_client
from boxer_company.routers.device_status_probe import _parse_pm2_processes
from boxer_company.routers.mda_graphql import (
    _get_mda_device_detail,
    _get_mda_latest_device_version,
    _update_mda_device_agent,
    _update_mda_device_box,
    _wait_for_mda_device_agent_ssh,
)

_LEADING_DEVICE_UPDATE_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)
_SEMVER_PATTERN = re.compile(r"(?<!\d)(\d+\.\d+\.\d+)(?!\d)")
_AGENT_GIT_KV_PATTERN = re.compile(r"^(HEAD|ORIGIN_MAIN|BRANCH|PKG_VERSION)=(.*)$")

_UPDATE_HINTS = (
    "업데이트",
    "update",
    "upgrade",
    "패치",
)
_UPDATE_STATUS_HINTS = (
    "상태",
    "현황",
    "확인",
    "체크",
)
_GENERIC_DEVICE_HINTS = (
    "장비",
    "device",
)
_BOX_UPDATE_HINTS = (
    "박스",
    "box",
    "mommybox-v2",
    "momybox-v2",
    "mommybox",
    "momybox",
)
_AGENT_UPDATE_HINTS = (
    "에이전트",
    "agent",
    "mommybox-v2-agent",
    "momybox-v2-agent",
    "mommybox-agent",
    "momybox-agent",
)
_LOGIN_SHELL_USER_PATH_EXPORT = 'export PATH="$HOME/.npm-global/bin:$HOME/bin:/usr/local/bin:$PATH"; '
_PM2_JLIST_COMMAND = (
    "bash -lc '"
    f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
    "if command -v pm2 >/dev/null 2>&1; then "
    "pm2 jlist 2>&1; "
    "else echo pm2_missing; fi'"
)
_AGENT_REPO_PATH = "/home/mommytalk/mommybox-v2-agent"
_AGENT_GIT_STATUS_COMMAND = (
    "bash -lc '"
    "if [ -d /home/mommytalk/mommybox-v2-agent/.git ]; then "
    "cd /home/mommytalk/mommybox-v2-agent && "
    "GIT_SSH_COMMAND=\"ssh -o StrictHostKeyChecking=no\" git fetch origin --tags -f >/dev/null 2>&1 || true; "
    "printf \"HEAD=%s\\n\" \"$(git rev-parse HEAD 2>/dev/null)\"; "
    "printf \"ORIGIN_MAIN=%s\\n\" \"$(git rev-parse origin/main 2>/dev/null)\"; "
    "printf \"BRANCH=%s\\n\" \"$(git rev-parse --abbrev-ref HEAD 2>/dev/null)\"; "
    "if [ -f package.json ]; then "
    "node -e \"console.log((JSON.parse(require(\\\"fs\\\").readFileSync(\\\"package.json\\\",\\\"utf8\\\")).version)||\\\"\\\")\" 2>/dev/null "
    "| sed \"s/^/PKG_VERSION=/\"; "
    "fi; "
    "else echo repo_missing; fi'"
)
_BOX_UPDATE_WAIT_TIMEOUT_SEC = 300
_AGENT_UPDATE_WAIT_TIMEOUT_SEC = 180
_UPDATE_POLL_INTERVAL_SEC = 5
_AGENT_MINIMUM_FOR_BOX_UPDATE = (2, 0, 0)
_BOX_PM2_NAMES = {"mommybox-v2"}
_AGENT_PM2_NAMES = {"mommybox-v2-agent", "mommybox-agent"}
_DeviceUpdateDispatchNoticeFn = Callable[[str], None]


def _normalize_device_update_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _contains_hint(text: str, hints: tuple[str, ...]) -> bool:
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    return any(hint in normalized or hint in lowered for hint in hints)


def _extract_device_name_for_update(question: str) -> str | None:
    normalized = _normalize_device_update_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _contains_hint(normalized, _UPDATE_HINTS + _UPDATE_STATUS_HINTS):
        return extracted

    matched = _LEADING_DEVICE_UPDATE_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _contains_hint(remainder, _UPDATE_HINTS + _UPDATE_STATUS_HINTS):
        return None
    return candidate


def _extract_requested_update_version(question: str) -> str | None:
    matched = _SEMVER_PATTERN.search(_normalize_device_update_question(question))
    if not matched:
        return None
    return str(matched.group(1) or "").strip() or None


def _resolve_update_device_name(question: str, device_name: str | None = None) -> str:
    resolved = str(device_name or _extract_device_name_for_update(question) or "").strip()
    if not resolved:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 박스 업데이트`")
    return resolved


def _is_device_update_status_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_update_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_update(normalized) or "").strip()
    if not resolved_device_name:
        return False
    return _contains_hint(normalized, _UPDATE_HINTS) and _contains_hint(normalized, _UPDATE_STATUS_HINTS)


def _is_device_agent_update_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_update_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_update(normalized) or "").strip()
    if not resolved_device_name or _is_device_update_status_request(normalized, resolved_device_name):
        return False
    return _contains_hint(normalized, _UPDATE_HINTS) and _contains_hint(normalized, _AGENT_UPDATE_HINTS)


def _is_device_box_update_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_update_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_update(normalized) or "").strip()
    if not resolved_device_name or _is_device_update_status_request(normalized, resolved_device_name):
        return False
    if _contains_hint(normalized, _AGENT_UPDATE_HINTS):
        return False
    return _contains_hint(normalized, _UPDATE_HINTS) and (
        _contains_hint(normalized, _BOX_UPDATE_HINTS)
        or _contains_hint(normalized, _GENERIC_DEVICE_HINTS)
    )


def _build_device_update_config_message() -> str:
    return (
        "장비 업데이트 기능 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD, DEVICE_SSH_PASSWORD가 필요해"
    )


def _build_device_header_lines(
    *,
    title: str,
    device_name: str,
    device_info: dict[str, Any],
) -> list[str]:
    lines = [
        title,
        f"• 장비: `{_display_value(device_info.get('deviceName'), default=device_name)}`",
    ]
    hospital_name = _display_value(device_info.get("hospitalName"), default="")
    room_name = _display_value(device_info.get("roomName"), default="")
    if hospital_name or room_name:
        lines.append(f"• 위치: `{hospital_name or '미확인'}` / `{room_name or '미확인'}`")
    return lines


def _build_device_snapshot(device_name: str, device_info: dict[str, Any] | None) -> dict[str, Any]:
    info = device_info if isinstance(device_info, dict) else {}
    return {
        "deviceName": _display_value(info.get("deviceName"), default=device_name),
        "version": _display_value(info.get("version"), default=""),
        "hospitalName": _display_value(info.get("hospitalName"), default=""),
        "roomName": _display_value(info.get("roomName"), default=""),
        "isConnected": bool(info.get("isConnected")),
    }


def _connection_label(is_connected: bool) -> str:
    return "연결됨" if is_connected else "끊김"


def _run_remote_ssh_command(
    client: Any,
    *,
    command: str,
    timeout_sec: int,
) -> dict[str, Any]:
    normalized_command = str(command or "").strip()
    actual_timeout = max(1, int(timeout_sec or cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10))
    try:
        _, stdout, stderr = client.exec_command(normalized_command, timeout=actual_timeout)
        exit_status = stdout.channel.recv_exit_status()
        stdout_text = (stdout.read() or b"").decode("utf-8", errors="replace").strip()
        stderr_text = (stderr.read() or b"").decode("utf-8", errors="replace").strip()
        combined = stdout_text
        if stderr_text:
            combined = combined or stderr_text
            if stdout_text and stderr_text not in stdout_text:
                combined = f"{stdout_text}\n{stderr_text}"
        return {
            "ok": exit_status == 0,
            "exitStatus": exit_status,
            "output": combined,
            "reason": "" if exit_status == 0 else f"ssh_exit_{exit_status}",
        }
    except Exception as exc:  # pragma: no cover - network/remote dependent
        return {
            "ok": False,
            "exitStatus": None,
            "output": "",
            "reason": type(exc).__name__.lower(),
        }


def _open_device_ssh_for_update(device_name: str) -> tuple[dict[str, Any], dict[str, Any], Any | None]:
    wait_result = _wait_for_mda_device_agent_ssh(device_name)
    device_info = wait_result.get("device") if isinstance(wait_result.get("device"), dict) else {}
    agent_ssh = device_info.get("agentSsh") if isinstance(device_info.get("agentSsh"), dict) else {}
    host = _display_value(agent_ssh.get("host"), default="")
    try:
        port = int(agent_ssh.get("port") or 0)
    except (TypeError, ValueError):
        port = 0

    ssh_state = {
        "ready": bool(wait_result.get("ready")) and bool(host) and port > 0,
        "reason": "ready" if bool(wait_result.get("ready")) and bool(host) and port > 0 else "agent_ssh_not_ready",
        "host": host,
        "port": port,
    }
    if not ssh_state["ready"]:
        return device_info, ssh_state, None

    connection = _connect_device_ssh_client(host, port)
    if not connection.get("ok"):
        ssh_state["ready"] = False
        ssh_state["reason"] = _display_value(connection.get("reason"), default="ssh_connect_failed")
        return device_info, ssh_state, None
    return device_info, ssh_state, connection["client"]


def _select_pm2_process(output: str, target_names: set[str]) -> dict[str, Any] | None:
    parsed = _parse_pm2_processes(output)
    if not parsed.get("available"):
        return None
    candidates = [
        item
        for item in (parsed.get("processes") or [])
        if isinstance(item, dict) and _display_value(item.get("name"), default="") in target_names
    ]
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda item: (
            0 if _display_value(item.get("status"), default="").strip().lower() == "online" else 1,
            int(item.get("restartCount") or 0),
        ),
    )[0]


def _parse_agent_repo_state(output: str) -> dict[str, Any]:
    normalized = str(output or "").strip()
    if normalized == "repo_missing":
        return {
            "available": False,
            "reason": "repo_missing",
            "head": "",
            "originMain": "",
            "branch": "",
            "packageVersion": "",
            "latest": False,
        }

    values: dict[str, str] = {}
    for line in normalized.splitlines():
        matched = _AGENT_GIT_KV_PATTERN.match(line.strip())
        if not matched:
            continue
        values[matched.group(1)] = matched.group(2).strip()

    head = values.get("HEAD", "")
    origin_main = values.get("ORIGIN_MAIN", "")
    branch = values.get("BRANCH", "")
    package_version = values.get("PKG_VERSION", "")
    return {
        "available": bool(head or origin_main or branch),
        "reason": "ok" if head or origin_main or branch else "parse_failed",
        "head": head,
        "originMain": origin_main,
        "branch": branch,
        "packageVersion": package_version,
        "latest": bool(head and origin_main and head == origin_main and branch == "main"),
    }


def _read_box_runtime_state(device_name: str) -> dict[str, Any]:
    device_info, ssh_state, client = _open_device_ssh_for_update(device_name)
    state: dict[str, Any] = {
        "device": _build_device_snapshot(device_name, device_info),
        "ssh": ssh_state,
        "process": None,
    }
    if client is None:
        return state

    try:
        pm2_result = _run_remote_ssh_command(
            client,
            command=_PM2_JLIST_COMMAND,
            timeout_sec=max(12, int(cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10)),
        )
    finally:
        client.close()

    state["pm2"] = pm2_result
    state["process"] = _select_pm2_process(_display_value(pm2_result.get("output"), default=""), _BOX_PM2_NAMES)
    return state


def _read_agent_runtime_state(device_name: str) -> dict[str, Any]:
    device_info, ssh_state, client = _open_device_ssh_for_update(device_name)
    state: dict[str, Any] = {
        "device": _build_device_snapshot(device_name, device_info),
        "ssh": ssh_state,
        "process": None,
        "repo": {
            "available": False,
            "reason": "ssh_not_ready" if not ssh_state.get("ready") else "unknown",
            "head": "",
            "originMain": "",
            "branch": "",
            "packageVersion": "",
            "latest": False,
        },
    }
    if client is None:
        return state

    try:
        pm2_result = _run_remote_ssh_command(
            client,
            command=_PM2_JLIST_COMMAND,
            timeout_sec=max(12, int(cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10)),
        )
        repo_result = _run_remote_ssh_command(
            client,
            command=_AGENT_GIT_STATUS_COMMAND,
            timeout_sec=max(20, int(cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10)),
        )
    finally:
        client.close()

    state["pm2"] = pm2_result
    state["repoCommand"] = repo_result
    state["process"] = _select_pm2_process(_display_value(pm2_result.get("output"), default=""), _AGENT_PM2_NAMES)
    if repo_result.get("ok"):
        state["repo"] = _parse_agent_repo_state(_display_value(repo_result.get("output"), default=""))
    else:
        state["repo"] = {
            "available": False,
            "reason": _display_value(repo_result.get("reason"), default="repo_check_failed"),
            "head": "",
            "originMain": "",
            "branch": "",
            "packageVersion": "",
            "latest": False,
        }
    return state


def _format_pm2_process_line(process: dict[str, Any] | None, fallback: str) -> str:
    if not isinstance(process, dict):
        return fallback
    name = _display_value(process.get("name"), default="미확인")
    status = _display_value(process.get("status"), default="미확인") or "미확인"
    version = _display_value(process.get("version"), default="")
    parts = [name, status]
    if version:
        parts.append(f"v{version}")
    return " / ".join(parts)


def _format_agent_repo_line(repo: dict[str, Any]) -> str:
    if not repo.get("available"):
        reason = _display_value(repo.get("reason"), default="확인 불가")
        return f"확인 불가 ({reason})"
    head = _display_value(repo.get("head"), default="")
    origin_main = _display_value(repo.get("originMain"), default="")
    branch = _display_value(repo.get("branch"), default="")
    package_version = _display_value(repo.get("packageVersion"), default="")
    parts = [f"branch `{branch or '미확인'}`"]
    if package_version:
        parts.append(f"pkg `{package_version}`")
    if head:
        parts.append(f"head `{head[:7]}`")
    if origin_main:
        parts.append(f"origin/main `{origin_main[:7]}`")
    parts.append("latest" if repo.get("latest") else "outdated")
    return " / ".join(parts)


def _resolve_agent_runtime_version(agent_runtime: dict[str, Any]) -> str:
    process = agent_runtime.get("process") if isinstance(agent_runtime.get("process"), dict) else {}
    repo = agent_runtime.get("repo") if isinstance(agent_runtime.get("repo"), dict) else {}
    for version in (
        _display_value(repo.get("packageVersion"), default=""),
        _display_value((process or {}).get("version"), default=""),
    ):
        if version:
            return version
    return ""


def _parse_semver_parts(version: str | None) -> tuple[int, int, int] | None:
    raw = str(version or "").strip()
    matched = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", raw)
    if not matched:
        return None
    return int(matched.group(1)), int(matched.group(2)), int(matched.group(3))


def _describe_agent_box_update_gate(agent_runtime: dict[str, Any]) -> dict[str, Any]:
    process = agent_runtime.get("process") if isinstance(agent_runtime.get("process"), dict) else {}
    repo = agent_runtime.get("repo") if isinstance(agent_runtime.get("repo"), dict) else {}
    candidates = (
        ("repo", _display_value(repo.get("packageVersion"), default="")),
        ("pm2", _display_value((process or {}).get("version"), default="")),
    )
    selected_source = ""
    selected_version = ""
    selected_parts: tuple[int, int, int] | None = None
    for source, version in candidates:
        parts = _parse_semver_parts(version)
        if parts is None:
            continue
        selected_source = source
        selected_version = version
        selected_parts = parts
        break

    minimum_text = ".".join(str(part) for part in _AGENT_MINIMUM_FOR_BOX_UPDATE)
    if selected_parts is None:
        return {
            "ok": False,
            "version": "",
            "source": "",
            "reason": f"에이전트 버전을 확인하지 못했어. 먼저 에이전트 2.0 이상으로 올려줘",
            "minimumVersion": minimum_text,
        }
    if selected_parts < _AGENT_MINIMUM_FOR_BOX_UPDATE:
        return {
            "ok": False,
            "version": selected_version,
            "source": selected_source,
            "reason": f"에이전트 {selected_version}라서 박스 업데이트를 막았어. 먼저 에이전트 2.0 이상으로 올려줘",
            "minimumVersion": minimum_text,
        }
    return {
        "ok": True,
        "version": selected_version,
        "source": selected_source,
        "reason": f"에이전트 {selected_version} 확인돼서 박스 업데이트 진행 가능해",
        "minimumVersion": minimum_text,
    }


def _wait_for_box_update_completion(device_name: str, target_version: str) -> dict[str, Any]:
    deadline = time.monotonic() + _BOX_UPDATE_WAIT_TIMEOUT_SEC
    attempts = 0
    last_state: dict[str, Any] = {}
    while True:
        attempts += 1
        last_state = _read_box_runtime_state(device_name)
        process = last_state.get("process") if isinstance(last_state.get("process"), dict) else {}
        status = _display_value((process or {}).get("status"), default="").strip().lower()
        version = _display_value((process or {}).get("version"), default="")
        if status == "online" and version == target_version:
            return {
                "ok": True,
                "status": "completed",
                "attempts": attempts,
                "observedVersion": version,
                "observedStatus": status,
                "runtime": last_state,
            }
        if time.monotonic() >= deadline:
            break
        time.sleep(_UPDATE_POLL_INTERVAL_SEC)

    process = last_state.get("process") if isinstance(last_state.get("process"), dict) else {}
    return {
        "ok": False,
        "status": "timed_out",
        "attempts": attempts,
        "observedVersion": _display_value((process or {}).get("version"), default=""),
        "observedStatus": _display_value((process or {}).get("status"), default=""),
        "runtime": last_state,
    }


def _wait_for_agent_update_completion(device_name: str) -> dict[str, Any]:
    deadline = time.monotonic() + _AGENT_UPDATE_WAIT_TIMEOUT_SEC
    attempts = 0
    last_state: dict[str, Any] = {}
    while True:
        attempts += 1
        last_state = _read_agent_runtime_state(device_name)
        process = last_state.get("process") if isinstance(last_state.get("process"), dict) else {}
        repo = last_state.get("repo") if isinstance(last_state.get("repo"), dict) else {}
        status = _display_value((process or {}).get("status"), default="").strip().lower()
        if bool(repo.get("latest")) and status == "online":
            return {
                "ok": True,
                "status": "completed",
                "attempts": attempts,
                "observedStatus": status,
                "observedVersion": _display_value((process or {}).get("version"), default=""),
                "runtime": last_state,
            }
        if time.monotonic() >= deadline:
            break
        time.sleep(_UPDATE_POLL_INTERVAL_SEC)

    process = last_state.get("process") if isinstance(last_state.get("process"), dict) else {}
    return {
        "ok": False,
        "status": "timed_out",
        "attempts": attempts,
        "observedStatus": _display_value((process or {}).get("status"), default=""),
        "observedVersion": _display_value((process or {}).get("version"), default=""),
        "runtime": last_state,
    }


def _render_device_update_status_result(
    *,
    device_name: str,
    device_info: dict[str, Any] | None,
    latest_box_version: str,
    box_runtime: dict[str, Any],
    agent_runtime: dict[str, Any],
) -> str:
    snapshot = _build_device_snapshot(device_name, device_info)
    lines = _build_device_header_lines(
        title="*장비 업데이트 상태*",
        device_name=device_name,
        device_info=snapshot,
    )
    if not device_info:
        lines.append("• 결과: *확인 불가*")
        lines.append("• 안내: MDA에서 장비를 찾지 못했어")
        return "\n".join(lines)

    lines.append(f"• MDA 연결: *{_connection_label(bool(snapshot.get('isConnected')))}*")
    lines.append(f"• MDA 박스 버전: `{_display_value(snapshot.get('version'), default='미확인')}`")
    lines.append(f"• 최신 박스 버전: `{latest_box_version or '미확인'}`")

    box_ssh = box_runtime.get("ssh") if isinstance(box_runtime.get("ssh"), dict) else {}
    if not box_ssh.get("ready"):
        lines.append(f"• 런타임 박스 상태: SSH 확인 불가 (`{_display_value(box_ssh.get('reason'), default='unknown')}`)")
    else:
        lines.append(
            f"• 런타임 박스 상태: `{_format_pm2_process_line(box_runtime.get('process'), 'mommybox-v2 미감지')}`"
        )

    agent_ssh = agent_runtime.get("ssh") if isinstance(agent_runtime.get("ssh"), dict) else {}
    if not agent_ssh.get("ready"):
        lines.append(f"• 런타임 에이전트 상태: SSH 확인 불가 (`{_display_value(agent_ssh.get('reason'), default='unknown')}`)")
    else:
        lines.append(
            f"• 런타임 에이전트 상태: `{_format_pm2_process_line(agent_runtime.get('process'), 'mommybox-agent 미감지')}`"
        )
        repo = agent_runtime.get("repo") if isinstance(agent_runtime.get("repo"), dict) else {}
        lines.append(f"• 에이전트 repo 상태: `{_format_agent_repo_line(repo)}`")
    agent_gate = _describe_agent_box_update_gate(agent_runtime)
    lines.append(
        f"• 박스 업데이트 선행조건: *{'충족' if agent_gate.get('ok') else '미충족'}* | "
        f"{_display_value(agent_gate.get('reason'), default='미확인')}"
    )

    return "\n".join(lines)


def _build_device_update_started_notice(result_payload: dict[str, Any]) -> str:
    route = _display_value(result_payload.get("route"), default="")
    request_payload = result_payload.get("request") if isinstance(result_payload.get("request"), dict) else {}
    device_payload = result_payload.get("device") if isinstance(result_payload.get("device"), dict) else {}
    precheck_payload = result_payload.get("precheck") if isinstance(result_payload.get("precheck"), dict) else {}
    device_name = _display_value(
        device_payload.get("deviceName"),
        default=_display_value(request_payload.get("deviceName"), default="미확인"),
    )

    if route == "device_agent_update":
        current_agent_version = _resolve_agent_runtime_version(precheck_payload)
        lines = [
            "*장비 에이전트 업데이트 진행 중*",
            f"• 장비: `{device_name}`",
            f"• 현재 에이전트 버전: `{current_agent_version or '미확인'}`",
            "• 대상: `latest`",
            "• 안내: 업데이트 중이야. 완료되면 다시 알려줄게",
        ]
        return "\n".join(lines)

    requested_version = _display_value(request_payload.get("requestedVersion"), default="")
    current_box_version = _display_value(device_payload.get("version"), default="")
    lines = [
        "*장비 박스 업데이트 진행 중*",
        f"• 장비: `{device_name}`",
        f"• 현재 박스 버전: `{current_box_version or '미확인'}`",
        f"• 대상 박스 버전: `{requested_version or '미확인'}`",
        "• 안내: 업데이트 중이야. 완료되면 다시 알려줄게",
    ]
    return "\n".join(lines)


def _request_device_box_update(
    question: str,
    device_name: str | None = None,
    on_dispatched: _DeviceUpdateDispatchNoticeFn | None = None,
) -> tuple[str, dict[str, Any]]:
    normalized_device_name = _resolve_update_device_name(question, device_name)
    if _extract_requested_update_version(question):
        raise ValueError("박스 업데이트는 최신만 지원해. 예: `MB2-C00419 박스 업데이트`")

    device_info = _get_mda_device_detail(normalized_device_name)
    latest_device_version = _get_mda_latest_device_version()
    latest_version = _display_value(latest_device_version.get("versionName"), default="")
    snapshot = _build_device_snapshot(normalized_device_name, device_info)
    precheck_runtime = _read_box_runtime_state(normalized_device_name)
    agent_runtime = _read_agent_runtime_state(normalized_device_name)
    agent_gate = _describe_agent_box_update_gate(agent_runtime)
    payload: dict[str, Any] = {
        "route": "device_box_update",
        "source": "mda_graphql+ssh",
        "request": {
            "deviceName": normalized_device_name,
            "requestedVersion": latest_version,
            "silent": False,
        },
        "device": snapshot,
        "latestVersion": latest_version,
        "precheck": precheck_runtime,
        "agentPrecheck": agent_runtime,
        "agentGate": agent_gate,
        "dispatch": None,
        "wait": None,
    }

    lines = _build_device_header_lines(
        title="*장비 박스 업데이트*",
        device_name=normalized_device_name,
        device_info=snapshot,
    )
    if not device_info:
        lines.append("• 결과: *요청 불가*")
        lines.append("• 안내: MDA에서 장비를 찾지 못했어")
        return "\n".join(lines), payload

    lines.append(f"• MDA 연결: *{_connection_label(bool(snapshot.get('isConnected')))}*")
    lines.append(f"• 현재 박스 버전: `{_display_value(snapshot.get('version'), default='미확인')}`")
    lines.append(f"• 최신 박스 버전: `{latest_version or '미확인'}`")
    lines.append(
        f"• SSH 사전 확인: `{_format_pm2_process_line(precheck_runtime.get('process'), 'mommybox-v2 미감지')}`"
        if (precheck_runtime.get("ssh") or {}).get("ready")
        else f"• SSH 사전 확인: 불가 (`{_display_value(((precheck_runtime.get('ssh') or {}) if isinstance(precheck_runtime.get('ssh'), dict) else {}).get('reason'), default='unknown')}`)"
    )
    lines.append(
        f"• 에이전트 선행 확인: `{_format_pm2_process_line(agent_runtime.get('process'), 'mommybox-agent 미감지')}`"
        if (agent_runtime.get("ssh") or {}).get("ready")
        else f"• 에이전트 선행 확인: 불가 (`{_display_value(((agent_runtime.get('ssh') or {}) if isinstance(agent_runtime.get('ssh'), dict) else {}).get('reason'), default='unknown')}`)"
    )
    lines.append(f"• 선행조건 판정: *{'충족' if agent_gate.get('ok') else '미충족'}*")

    pre_process = precheck_runtime.get("process") if isinstance(precheck_runtime.get("process"), dict) else {}
    if (
        _display_value((pre_process or {}).get("status"), default="").strip().lower() == "online"
        and _display_value((pre_process or {}).get("version"), default="") == latest_version
    ):
        payload["wait"] = {"status": "already_latest", "ok": True, "runtime": precheck_runtime}
        lines.append("• 결과: *생략*")
        lines.append("• 안내: 이미 최신 박스 버전으로 실행 중이야")
        return "\n".join(lines), payload

    if not snapshot.get("isConnected"):
        lines.append("• 결과: *요청 불가*")
        lines.append("• 안내: 장비 agent 연결이 끊겨 있어. 장비 온라인 상태를 먼저 확인해")
        return "\n".join(lines), payload

    if not agent_gate.get("ok"):
        lines.append("• 결과: *요청 불가*")
        lines.append(f"• 안내: {_display_value(agent_gate.get('reason'), default='에이전트 2.0 이상이 필요해')}")
        lines.append(f"• 조치: 먼저 `{normalized_device_name} 에이전트 업데이트`를 실행해")
        return "\n".join(lines), payload

    dispatch_result = _update_mda_device_box(normalized_device_name, version=latest_version, silent=False)
    payload["dispatch"] = dispatch_result
    if not dispatch_result.get("status"):
        lines.append("• 결과: *요청 실패*")
        lines.append(f"• MDA 응답: {_display_value(dispatch_result.get('message'), default='미확인')}")
        return "\n".join(lines), payload

    if on_dispatched is not None:
        on_dispatched(_build_device_update_started_notice(payload))

    wait_result = _wait_for_box_update_completion(normalized_device_name, latest_version)
    payload["wait"] = wait_result
    wait_runtime = wait_result.get("runtime") if isinstance(wait_result.get("runtime"), dict) else {}
    lines.append("• MDA 응답: 업데이트 요청 전송 완료")
    lines.append(
        f"• SSH 완료 확인: `{_format_pm2_process_line(wait_runtime.get('process'), 'mommybox-v2 미감지')}`"
    )
    if wait_result.get("ok"):
        lines.append("• 결과: *완료*")
        lines.append(f"• 안내: SSH 기준 `mommybox-v2`가 최신 버전 `{latest_version}`로 online 상태야")
    else:
        lines.append("• 결과: *확인 필요*")
        lines.append(
            f"• 안내: {_BOX_UPDATE_WAIT_TIMEOUT_SEC}초 안에 최신 버전 online 상태까지는 못 봤어. `"
            f"{normalized_device_name} 업데이트 상태`나 `{normalized_device_name} pm2 상태`로 다시 확인해"
        )
    return "\n".join(lines), payload


def _request_device_agent_update(
    question: str,
    device_name: str | None = None,
    on_dispatched: _DeviceUpdateDispatchNoticeFn | None = None,
) -> tuple[str, dict[str, Any]]:
    normalized_device_name = _resolve_update_device_name(question, device_name)
    if _extract_requested_update_version(question):
        raise ValueError("에이전트 업데이트는 최신만 지원해. 예: `MB2-C00419 에이전트 업데이트`")

    device_info = _get_mda_device_detail(normalized_device_name)
    snapshot = _build_device_snapshot(normalized_device_name, device_info)
    precheck_runtime = _read_agent_runtime_state(normalized_device_name)
    payload: dict[str, Any] = {
        "route": "device_agent_update",
        "source": "mda_graphql+ssh",
        "request": {
            "deviceName": normalized_device_name,
            "requestedVersion": "latest",
            "force": False,
        },
        "device": snapshot,
        "precheck": precheck_runtime,
        "dispatch": None,
        "wait": None,
    }

    lines = _build_device_header_lines(
        title="*장비 에이전트 업데이트*",
        device_name=normalized_device_name,
        device_info=snapshot,
    )
    if not device_info:
        lines.append("• 결과: *요청 불가*")
        lines.append("• 안내: MDA에서 장비를 찾지 못했어")
        return "\n".join(lines), payload

    lines.append(f"• MDA 연결: *{_connection_label(bool(snapshot.get('isConnected')))}*")
    lines.append(
        f"• SSH 사전 확인: `{_format_pm2_process_line(precheck_runtime.get('process'), 'mommybox-agent 미감지')}`"
        if (precheck_runtime.get("ssh") or {}).get("ready")
        else f"• SSH 사전 확인: 불가 (`{_display_value(((precheck_runtime.get('ssh') or {}) if isinstance(precheck_runtime.get('ssh'), dict) else {}).get('reason'), default='unknown')}`)"
    )
    pre_repo = precheck_runtime.get("repo") if isinstance(precheck_runtime.get("repo"), dict) else {}
    if pre_repo:
        lines.append(f"• 에이전트 repo 사전 확인: `{_format_agent_repo_line(pre_repo)}`")

    pre_process = precheck_runtime.get("process") if isinstance(precheck_runtime.get("process"), dict) else {}
    if bool(pre_repo.get("latest")) and _display_value((pre_process or {}).get("status"), default="").strip().lower() == "online":
        payload["wait"] = {"status": "already_latest", "ok": True, "runtime": precheck_runtime}
        lines.append("• 결과: *생략*")
        lines.append("• 안내: 이미 최신 `origin/main` 기준으로 실행 중이야")
        return "\n".join(lines), payload

    if not snapshot.get("isConnected"):
        lines.append("• 결과: *요청 불가*")
        lines.append("• 안내: 장비 agent 연결이 끊겨 있어. 장비 온라인 상태를 먼저 확인해")
        return "\n".join(lines), payload

    dispatch_result = _update_mda_device_agent(normalized_device_name, force=False)
    payload["dispatch"] = dispatch_result
    if not dispatch_result.get("status"):
        lines.append("• 결과: *요청 실패*")
        lines.append(f"• MDA 응답: {_display_value(dispatch_result.get('message'), default='미확인')}")
        return "\n".join(lines), payload

    if on_dispatched is not None:
        on_dispatched(_build_device_update_started_notice(payload))

    wait_result = _wait_for_agent_update_completion(normalized_device_name)
    payload["wait"] = wait_result
    wait_runtime = wait_result.get("runtime") if isinstance(wait_result.get("runtime"), dict) else {}
    wait_repo = wait_runtime.get("repo") if isinstance(wait_runtime.get("repo"), dict) else {}
    lines.append("• MDA 응답: 업데이트 요청 전송 완료")
    lines.append(
        f"• SSH 완료 확인: `{_format_pm2_process_line(wait_runtime.get('process'), 'mommybox-agent 미감지')}`"
    )
    lines.append(f"• 에이전트 repo 완료 확인: `{_format_agent_repo_line(wait_repo)}`")
    if wait_result.get("ok"):
        lines.append("• 결과: *완료*")
        lines.append("• 안내: SSH 기준 agent repo가 `origin/main`과 일치하고 PM2도 online 상태야")
    else:
        lines.append("• 결과: *확인 필요*")
        lines.append(
            f"• 안내: {_AGENT_UPDATE_WAIT_TIMEOUT_SEC}초 안에 `origin/main + online` 상태까지는 못 봤어. "
            f"`{normalized_device_name} 업데이트 상태`나 `{normalized_device_name} pm2 상태`로 다시 확인해"
        )
    return "\n".join(lines), payload


def _query_device_update_status(device_name: str) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 업데이트 상태`")

    device_info = _get_mda_device_detail(normalized_device_name)
    latest_device_version = _get_mda_latest_device_version()
    latest_version = _display_value(latest_device_version.get("versionName"), default="")
    box_runtime = _read_box_runtime_state(normalized_device_name)
    agent_runtime = _read_agent_runtime_state(normalized_device_name)
    agent_gate = _describe_agent_box_update_gate(agent_runtime)
    payload = {
        "route": "device_update_status",
        "source": "mda_graphql+ssh",
        "request": {
            "deviceName": normalized_device_name,
        },
        "device": _build_device_snapshot(normalized_device_name, device_info),
        "latestVersion": latest_version,
        "boxRuntime": box_runtime,
        "agentRuntime": agent_runtime,
        "agentGate": agent_gate,
    }
    return (
        _render_device_update_status_result(
            device_name=normalized_device_name,
            device_info=device_info,
            latest_box_version=latest_version,
            box_runtime=box_runtime,
            agent_runtime=agent_runtime,
        ),
        payload,
    )


def _build_device_update_activity_input(
    *,
    question: str,
    user_id: str,
    user_name: str | None,
    channel_id: str,
    thread_ts: str,
    result_payload: dict[str, Any],
) -> dict[str, Any]:
    route = _display_value(result_payload.get("route"), default="")
    request_payload = result_payload.get("request") if isinstance(result_payload.get("request"), dict) else {}
    device_payload = result_payload.get("device") if isinstance(result_payload.get("device"), dict) else {}
    dispatch_payload = result_payload.get("dispatch") if isinstance(result_payload.get("dispatch"), dict) else {}
    wait_payload = result_payload.get("wait") if isinstance(result_payload.get("wait"), dict) else {}
    device_name = _display_value(
        device_payload.get("deviceName"),
        default=_display_value(request_payload.get("deviceName"), default="미확인"),
    )
    requested_version = _display_value(request_payload.get("requestedVersion"), default="")
    requester_label = str(user_name or user_id or "").strip()
    is_agent_update = route == "device_agent_update"

    detail_log = {
        "source": "boxer_slack_device_update",
        "route": route,
        "question": question,
        "slackUserId": user_id,
        "slackUserName": user_name,
        "slackChannelId": channel_id,
        "slackThreadTs": thread_ts,
        "requestedBySlackUserId": user_id,
        "requestedBySlackUserName": user_name,
        "deviceName": device_name,
        "requestedVersion": requested_version,
        "currentBoxVersion": _display_value(device_payload.get("version"), default=""),
        "dispatchStatus": bool(dispatch_payload.get("status")),
        "dispatchMessage": _display_value(dispatch_payload.get("message"), default=""),
        "waitStatus": _display_value(wait_payload.get("status"), default=""),
        "waitOk": bool(wait_payload.get("ok")),
    }

    action_label = "에이전트" if is_agent_update else "박스"
    version_label = requested_version or "latest"
    return {
        "activityType": "agent.stateChange" if is_agent_update else "device.edit",
        "reason": f"Boxer Slack {action_label} 업데이트 요청 전송",
        "description": (
            f"Boxer Slack {action_label} 업데이트 요청: 장비명 [{device_name}], "
            f"대상 [{version_label}]"
            f"{f', 요청자 [{requester_label}]' if requester_label else ''}"
        ),
        "detailLog": json.dumps(detail_log, ensure_ascii=False, separators=(",", ":")),
    }
