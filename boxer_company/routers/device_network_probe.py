"""고정된 읽기 전용 수집과 근거 제한을 적용하는 장비 네트워크 진단."""

import json
import re
import shlex
from datetime import datetime
from typing import Any

# 원격 Python은 Ubuntu 장비의 Python 3.6에서도 실행되는 표준 라이브러리만 쓴다.
# stderr와 앱 원문은 반환하지 않고 허용한 상태, 시각, 이벤트 코드만 내보낸다.
_REMOTE_COMMON = r'''
import datetime
import glob
import ipaddress
import json
import os
import re
import shutil
import subprocess
import time

def emit(value):
    print(json.dumps(value, ensure_ascii=True, separators=(",", ":")))

def run(args, seconds=4):
    if not shutil.which(args[0]):
        return "tool_missing", ""
    try:
        result = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=seconds, universal_newlines=True)
    except subprocess.TimeoutExpired:
        return "timeout", ""
    except PermissionError:
        return "permission_denied", ""
    except Exception:
        return "query_failed", ""
    error = result.stderr.lower()
    if any(word in error for word in ("permission denied", "access denied", "not seeing messages", "insufficient permissions")):
        return "permission_denied", ""
    if result.returncode:
        return "query_failed", ""
    return "ok", result.stdout

def safe_iface(value):
    name = str(value or "")
    if not re.match(r"^[a-zA-Z][a-zA-Z0-9_.:-]{0,31}$", name):
        return ""
    if name == "lo" or name.startswith(("docker", "veth", "virbr", "br-", "tun", "tap", "tailscale", "zt")):
        return ""
    return name

def clock_stamp():
    return datetime.datetime.now().astimezone().isoformat(timespec="seconds")
'''

_REMOTE_STATE = r'''
def read_text(path):
    try:
        with open(path) as stream:
            return stream.read(2048).strip()
    except Exception:
        return ""

def main():
    status, text = run(["ip", "-j", "route", "show", "default"])
    if status != "ok":
        emit({"status": status})
        return
    try:
        routes = [row for row in json.loads(text) if safe_iface(row.get("dev"))]
        routes.sort(key=lambda row: int(row.get("metric", 0)))
    except Exception:
        emit({"status": "parse_failed"})
        return
    interface = safe_iface(routes[0].get("dev")) if routes else ""
    # 기본 경로가 사라졌을 때도 연결된 물리 인터페이스만 보조 후보로 확인한다.
    if not interface:
        candidates = []
        for path in sorted(glob.glob("/sys/class/net/*"))[:32]:
            name = safe_iface(os.path.basename(path))
            if name and "/virtual/" not in os.path.realpath(path) and read_text(path + "/carrier") == "1":
                candidates.append(name)
        interface = candidates[0] if candidates else ""
    base = "/sys/class/net/" + interface
    wireless = bool(interface and (os.path.isdir(base + "/wireless") or interface.startswith(("wl", "wifi"))))
    carrier = read_text(base + "/carrier") if interface else ""
    result = {"status": "ok", "capturedAt": clock_stamp(), "interface": interface,
              "connectionType": ("wireless" if wireless else "wired") if interface else "unknown",
              "operstate": read_text(base + "/operstate") if interface else "unknown",
              "carrier": {"1": True, "0": False}.get(carrier),
              "hasDefaultRoute": bool(routes), "addresses": [], "dnsServers": []}
    if interface:
        state, addresses = run(["ip", "-j", "address", "show", "dev", interface])
        if state == "ok":
            try:
                result["addresses"] = [str(ipaddress.ip_address(item["local"])) for row in json.loads(addresses)
                                       for item in row.get("addr_info", []) if item.get("scope") == "global"][:8]
            except Exception:
                result["status"] = "partial"
        else:
            result["status"] = "partial"
        gateway = routes[0].get("gateway") if routes else None
        if gateway:
            try:
                result["gateway"] = str(ipaddress.ip_address(gateway))
            except ValueError:
                pass
    # resolvectl은 uplink DNS만 추출하며 검색 도메인과 무선 SSID는 노출하지 않는다.
    if interface and shutil.which("resolvectl"):
        dns_status, dns_text = run(["resolvectl", "dns", interface])
    else:
        dns_status, dns_text = "tool_missing", ""
    if dns_status == "ok":
        for token in dns_text.split():
            try:
                result["dnsServers"].append(str(ipaddress.ip_address(token)))
            except ValueError:
                pass
    else:
        for path in ("/run/systemd/resolve/resolv.conf", "/etc/resolv.conf"):
            text = read_text(path)
            for line in text.splitlines():
                parts = line.split()
                if len(parts) == 2 and parts[0] == "nameserver":
                    try:
                        result["dnsServers"].append(str(ipaddress.ip_address(parts[1])))
                    except ValueError:
                        pass
            if result["dnsServers"]:
                break
    result["dnsServers"] = sorted(set(result["dnsServers"]))[:8]
    if not result["dnsServers"] or (interface and carrier not in ("0", "1")):
        result["status"] = "partial"
    emit(result)
'''

_REMOTE_LINK_HISTORY = r'''
def main():
    # -k는 현재 부팅만 선택하므로 사용하지 않는다. 커널 transport와 네트워크 unit을 OR로 조회한다.
    args = ["journalctl", "--since", "72 hours ago", "--no-pager", "-o", "json", "-n", "401",
            "--grep", "(?i)(Link is (Up|Down)|Gained carrier|Lost carrier|carrier: link|state change:|Stopping Network Configuration|Reached target Shutdown|Powering off|Shutting down)",
            "_TRANSPORT=kernel", "+", "_SYSTEMD_UNIT=systemd-networkd.service", "+",
            "_SYSTEMD_UNIT=NetworkManager.service", "+", "_PID=1"]
    status, text = run(args, 12)
    if status != "ok":
        emit({"status": status, "historyWindowHours": 72, "events": []})
        return
    records = [line for line in text.splitlines() if line.strip().startswith("{")]
    events = []
    invalid = False
    for line in records:
        try:
            row = json.loads(line)
            message = str(row.get("MESSAGE", ""))
            source = "kernel" if row.get("_TRANSPORT") == "kernel" else str(row.get("_SYSTEMD_UNIT", ""))
            if str(row.get("_PID", "")) == "1" and re.search(r"Stopping Network Configuration|Reached target Shutdown|Powering off|Shutting down", message, re.I):
                boot = str(row.get("_BOOT_ID", ""))
                micros = int(row.get("__REALTIME_TIMESTAMP", 0))
                if micros and re.match(r"^[0-9a-f]{32}$", boot):
                    stamp = datetime.datetime.fromtimestamp(micros / 1000000.0).astimezone().isoformat(timespec="seconds")
                    events.append({"timestamp": stamp, "source": "system_lifecycle", "interface": "", "state": "shutdown", "boot": boot, "transition": "shutdown"})
                continue
            if source not in ("kernel", "systemd-networkd.service", "NetworkManager.service"):
                continue
            match = re.search(r"(?:\bdevice \()?([a-zA-Z][a-zA-Z0-9_.:-]{0,31})\)?:\s*(Link is (?:Up|Down)|Gained carrier|Lost carrier|carrier: link (?:connected|disconnected))", message, re.I)
            manager_change = False
            if not match and source == "NetworkManager.service":
                match = re.search(r"device \(([a-zA-Z][a-zA-Z0-9_.:-]{0,31})\): state change: (?:[^ ]+) -> (activated|disconnected|unavailable)", message, re.I)
                manager_change = bool(match)
            if not match:
                continue
            interface = safe_iface(match.group(1))
            if not interface:
                continue
            transition = match.group(2).lower()
            state = ("up" if transition == "activated" else "disconnected") if manager_change else ("down" if any(word in transition for word in ("down", "lost", "disconnected")) else "up")
            micros = int(row.get("__REALTIME_TIMESTAMP", 0))
            if not micros:
                invalid = True
                continue
            stamp = datetime.datetime.fromtimestamp(micros / 1000000.0).astimezone().isoformat(timespec="seconds")
            boot = str(row.get("_BOOT_ID", ""))
            if not re.match(r"^[0-9a-f]{32}$", boot):
                invalid = True
                continue
            events.append({"timestamp": stamp, "source": source, "interface": interface,
                           "state": state, "boot": boot,
                           "bootElapsedSeconds": int(row.get("__MONOTONIC_TIMESTAMP", 0)) // 1000000,
                           "transition": "manager_state" if manager_change else ("lost_carrier" if "lost carrier" in transition else "link_" + state)})
        except Exception:
            invalid = True
    events.sort(key=lambda item: item["timestamp"])
    compact = []
    for event in events:
        previous = compact[-1] if compact else None
        # 커널과 networkd의 같은 전이를 합쳐 과거 단절이 출력 상한 밖으로 밀리지 않게 한다.
        duplicate = previous and previous["boot"] == event["boot"] and previous["interface"] == event["interface"] and previous["state"] == event["state"] and previous["source"] != event["source"] and abs((datetime.datetime.strptime(previous["timestamp"][:19], "%Y-%m-%dT%H:%M:%S") - datetime.datetime.strptime(event["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")).total_seconds()) <= 2
        if duplicate:
            if event.get("transition") == "lost_carrier" or (event["state"] == "up" and event["source"] == "kernel"):
                compact[-1] = event
        else:
            compact.append(event)
    events = compact
    limited = len(events) > 32 or len(records) >= 401
    emit({"status": "partial" if limited or invalid else ("ok" if records else "empty"),
          "historyWindowHours": 72, "eventsTruncated": limited, "eventCount": len(events), "events": events[-32:]})
'''

_REMOTE_APP_HISTORY = r'''
APP_PATTERNS = (
    ("dns_retry", re.compile(r"\bEAI_AGAIN\b", re.I)),
    ("route_unreachable", re.compile(r"\bENETUNREACH\b", re.I)),
    ("dns_not_found", re.compile(r"\bENOTFOUND\b", re.I)),
    ("socket_error", re.compile(r"websocket error|xhr poll error|ping timeout|socket.*(?:connect_error|reconnection error)|transport (?:error|close)", re.I)),
    ("app_offline", re.compile(r"\bNoInternet\b|No Internet Connection", re.I)),
    ("socket_disconnected", re.compile(r"socket.*disconnect|disconnect.*socket", re.I)),
    ("socket_connected", re.compile(r"socket.*connected|connected.*socket", re.I)),
    ("app_lifecycle", re.compile(r"SIGINT|SIGTERM|App Init|Mommybox Starting|ACPI shutdown|Shutdown requested|application.*(?:start|restart)", re.I)),
)

def main():
    paths = []
    seen_files = set()
    permission_error = False
    today = datetime.date.today()
    cutoff = time.time() - 72 * 3600
    # 72시간의 시작 날짜가 오늘로부터 3일 전일 수 있어 네 날짜를 확인한다.
    for days in range(4):
        date = (today - datetime.timedelta(days=days)).isoformat()
        for directory in ("logs", "Logs"):
            path = os.path.expanduser("~/AppData/" + directory + "/log-" + date + ".log")
            try:
                metadata = os.stat(path)
                identity = (metadata.st_dev, metadata.st_ino)
                if os.path.isfile(path) and identity not in seen_files:
                    paths.append(os.path.realpath(path))
                    seen_files.add(identity)
            except PermissionError:
                permission_error = True
            except OSError:
                pass
    if not paths:
        emit({"status": "permission_denied" if permission_error else "empty", "historyWindowHours": 72, "events": [], "filesRead": 0})
        return
    events = []
    counts = {}
    files_read = 0
    limited = False
    for path in paths[:8]:
        try:
            with open(path, "rb") as stream:
                size = os.fstat(stream.fileno()).st_size
                start = max(0, size - 2 * 1024 * 1024)
                stream.seek(start)
                if start:
                    stream.readline()
                    limited = True
                text = stream.read(2 * 1024 * 1024).decode("utf-8", errors="replace")
            files_read += 1
        except PermissionError:
            permission_error = True
            continue
        except OSError:
            limited = True
            continue
        # 앱 원문에서 시각과 고정 이벤트 종류만 뽑는다. URL, 환자, 바코드, 토큰은 반환하지 않는다.
        for line in text.splitlines():
            match = re.search(r"(20\d\d-\d\d-\d\d)[T_ ](\d\d:\d\d:\d\d)(?:\.\d+)?", line)
            if not match:
                continue
            try:
                stamp_value = datetime.datetime.strptime(match.group(1) + "T" + match.group(2), "%Y-%m-%dT%H:%M:%S").astimezone()
            except ValueError:
                continue
            if stamp_value.timestamp() < cutoff or stamp_value.timestamp() > time.time() + 60:
                continue
            for kind, pattern in APP_PATTERNS:
                if pattern.search(line):
                    stamp = stamp_value.isoformat(timespec="seconds")
                    counts[kind] = counts.get(kind, 0) + 1
                    events.append({"timestamp": stamp, "source": "app_daily_log", "kind": kind})
                    break
    events.sort(key=lambda item: item["timestamp"])
    # 같은 오류의 재시도 묶음은 첫 시각과 마지막 시각을 남겨 장애 시작을 보존한다.
    groups = []
    latest = {}
    for event in events:
        kind = event["kind"]
        previous = latest.get(kind)
        current_time = datetime.datetime.strptime(event["timestamp"][:19], "%Y-%m-%dT%H:%M:%S")
        previous_time = datetime.datetime.strptime(previous["last"]["timestamp"][:19], "%Y-%m-%dT%H:%M:%S") if previous else None
        if previous_time is not None and (current_time - previous_time).total_seconds() <= 300:
            previous["last"] = event
        else:
            group = {"first": event, "last": event}
            groups.append(group)
            latest[kind] = group
    # 정상 연결/시작 로그보다 최근 오류 묶음을 우선해 단절 직후 최초 오류를 보존한다.
    error_kinds = {"dns_retry", "route_unreachable", "dns_not_found", "socket_error", "app_offline"}
    error_groups = [group for group in groups if group["first"]["kind"] in error_kinds]
    other_groups = [group for group in groups if group["first"]["kind"] not in error_kinds]
    selected = error_groups[-12:]
    remaining = 12 - len(selected)
    if remaining:
        selected += other_groups[-remaining:]
    grouped = []
    for group in selected:
        grouped.append(group["first"])
        if group["last"]["timestamp"] != group["first"]["timestamp"]:
            grouped.append(group["last"])
    grouped.sort(key=lambda item: item["timestamp"])
    limited = limited or len(groups) > len(selected) or len(paths) > 8
    status = "permission_denied" if permission_error and not files_read else ("partial" if limited or permission_error else "ok")
    emit({"status": status, "historyWindowHours": 72, "filesRead": files_read,
          "eventsTruncated": limited, "counts": counts, "events": grouped})
'''


def _remote_command(script: str) -> str:
    body = _REMOTE_COMMON + script + "\ntry:\n    main()\nexcept Exception:\n    emit({\"status\": \"query_failed\"})\n"
    invocation = "python3 -c " + shlex.quote(body)
    missing = shlex.quote('{"status":"tool_missing","reason":"python3_missing"}')
    return "sh -lc " + shlex.quote(
        "if command -v python3 >/dev/null 2>&1; then " + invocation + "; else printf '%s\\n' " + missing + "; fi"
    )


NETWORK_COMMAND_KEYS = ("network_state", "network_link_history", "network_app_history")
NETWORK_COMMAND_SPECS: dict[str, dict[str, Any]] = {
    "network_state": {"summary": "현재 연결 인터페이스와 경로, DNS 확인", "timeout_sec": 22, "command": _remote_command(_REMOTE_STATE)},
    "network_link_history": {"summary": "최근 72시간 유선과 무선 링크 이력 확인", "timeout_sec": 18, "command": _remote_command(_REMOTE_LINK_HISTORY)},
    "network_app_history": {"summary": "최근 72시간 앱 통신 오류 이력 확인", "timeout_sec": 18, "command": _remote_command(_REMOTE_APP_HISTORY)},
}

_APP_LABELS = {
    "dns_retry": "DNS EAI_AGAIN", "route_unreachable": "네트워크 ENETUNREACH",
    "dns_not_found": "DNS ENOTFOUND", "socket_error": "소켓 통신 오류",
    "app_offline": "앱 NoInternet", "socket_disconnected": "소켓 연결 종료",
    "socket_connected": "소켓 연결 복구", "app_lifecycle": "앱 시작 또는 종료 신호",
}
_APP_ERRORS = set(_APP_LABELS) - {"socket_connected", "socket_disconnected", "app_lifecycle"}
_FAILURE_LABELS = {
    "permission_denied": "조회 권한 부족", "tool_missing": "필수 도구 없음", "empty": "조회 기록 없음",
    "timeout": "조회 시간 초과", "partial": "일부 기록만 조회", "missing": "미수집",
    "query_failed": "조회 실패", "parse_failed": "응답 해석 실패",
}
_CHECK_LABELS = {
    "network_state": "현재 연결", "network_link_history": "링크 이력", "network_app_history": "앱 통신 이력",
}


def _payload(checks: dict[str, Any], key: str) -> dict[str, Any]:
    check = checks.get(key)
    if not isinstance(check, dict):
        return {"status": "missing"}
    if not check.get("ok"):
        reason = str(check.get("reason") or "")
        return {"status": "timeout" if "timeout" in reason else "query_failed"}
    try:
        value = json.loads(str(check.get("output") or ""))
    except (ValueError, TypeError):
        return {"status": "parse_failed"}
    if not isinstance(value, dict) or value.get("status") not in {"ok", *_FAILURE_LABELS}:
        return {"status": "parse_failed"}
    return value


def _interface(value: Any) -> str:
    name = str(value or "")
    if not re.fullmatch(r"[a-zA-Z][a-zA-Z0-9_.:-]{0,31}", name):
        return ""
    if name == "lo" or name.startswith(("docker", "veth", "virbr", "br-", "tun", "tap", "tailscale", "zt")):
        return ""
    return name


def _timestamp(value: Any) -> str:
    stamp = str(value or "")
    if not re.fullmatch(r"20\d\d-\d\d-\d\dT\d\d:\d\d:\d\d(?:[+-]\d\d:\d\d|Z)?", stamp):
        return ""
    try:
        datetime.fromisoformat(stamp)
    except ValueError:
        return ""
    return stamp


def _link_failures(payload: dict[str, Any], interface: str) -> list[dict[str, str]]:
    failures: list[dict[str, str]] = []
    previous: dict[tuple[str, str], str] = {}
    events = payload.get("events") if isinstance(payload.get("events"), list) else []
    shutdowns = [row for row in events if isinstance(row, dict) and row.get("transition") == "shutdown" and _timestamp(row.get("timestamp"))]
    for event in sorted((row for row in events if isinstance(row, dict)), key=lambda row: str(row.get("timestamp", ""))):
        source = str(event.get("source") or "")
        stamp = _timestamp(event.get("timestamp"))
        boot = str(event.get("boot") or "")
        if not interface or _interface(event.get("interface")) != interface or not stamp:
            continue
        if source not in {"kernel", "systemd-networkd.service", "NetworkManager.service"} or not re.fullmatch(r"[0-9a-f]{32}", boot):
            continue
        key = (boot, source)
        state = str(event.get("state") or "")
        # 수일 전 Up이 조회창 밖인 경우도 있으므로 부팅 후 120초가 지난 Down은 보존한다.
        elapsed = event.get("bootElapsedSeconds")
        after_boot = isinstance(elapsed, (int, float)) and elapsed > 120
        down = state == "down" and (previous.get(key) == "up" or event.get("transition") == "lost_carrier" or after_boot)
        # 정상 OS 종료와 거의 동시에 생긴 Down은 원인으로 확정하지 않는다.
        if down and any(row.get("boot") == boot and _time_distance(stamp, row["timestamp"]) is not None and 0 <= _time_distance(stamp, row["timestamp"]) <= 60 for row in shutdowns):
            down = False
        if down and (not failures or _time_distance(stamp, failures[-1]["timestamp"]) is None or abs(_time_distance(stamp, failures[-1]["timestamp"])) > 2):
            failures.append({"timestamp": stamp, "source": source, "kind": "link_down", "line": stamp + " " + interface + ": 링크 단절"})
        if state in {"up", "down"}:
            previous[key] = state
    return failures


def _time_distance(left: str, right: str) -> float | None:
    try:
        return (datetime.fromisoformat(left) - datetime.fromisoformat(right)).total_seconds()
    except (ValueError, TypeError):
        return None


def build_network_summary(checks: dict[str, Any]) -> dict[str, Any]:
    """현재 연결과 과거 오류를 나누고, 미수집을 정상으로 바꾸지 않는다."""
    state, history, app = (_payload(checks, key) for key in NETWORK_COMMAND_KEYS)
    interface = _interface(state.get("interface"))
    connection = state.get("connectionType") if state.get("connectionType") in {"wired", "wireless"} and interface else "unknown"
    statuses = {key: str(value.get("status") or "parse_failed") for key, value in zip(NETWORK_COMMAND_KEYS, (state, history, app))}
    available = any(status in {"ok", "partial"} for status in statuses.values())
    evidence = _link_failures(history, interface)
    app_events = app.get("events") if isinstance(app.get("events"), list) else []
    errors = []
    for event in app_events:
        if not isinstance(event, dict):
            continue
        kind, stamp = str(event.get("kind") or ""), _timestamp(event.get("timestamp"))
        if kind in _APP_ERRORS and stamp:
            errors.append({"timestamp": stamp, "source": "app_daily_log", "line": stamp + " " + _APP_LABELS[kind]})
    # NetworkManager의 일반 상태 전이는 물리 단절 확정 근거와 분리한다.
    for event in history.get("events", []) if isinstance(history.get("events"), list) else []:
        if not isinstance(event, dict) or event.get("transition") != "manager_state" or event.get("state") != "disconnected":
            continue
        stamp = _timestamp(event.get("timestamp"))
        if stamp and interface and _interface(event.get("interface")) == interface:
            errors.append({"timestamp": stamp, "source": "NetworkManager.service", "line": stamp + " " + interface + ": 연결 종료 상태 전이"})
    current_down = state.get("status") in {"ok", "partial"} and bool(interface) and (state.get("carrier") is False or state.get("operstate") in {"down", "lowerlayerdown"})
    route_missing = state.get("status") in {"ok", "partial"} and state.get("hasDefaultRoute") is False
    current_up = bool(interface) and state.get("carrier") is True and state.get("hasDefaultRoute") is True
    incomplete = [_CHECK_LABELS[name] + ": " + _FAILURE_LABELS.get(status, "조회 상태 미확인") for name, status in statuses.items() if status != "ok"]
    result: dict[str, Any] = {
        "available": available, "status": "check_needed", "label": "네트워크 확인 필요",
        "connectionType": connection, "interface": interface, "classification": "check_unavailable",
        "summary": "네트워크 상태를 판단할 조회 근거가 부족해.",
        "action": "조회 권한과 도구, 로그 보관 상태를 확인한 뒤 다시 점검해.",
        "evidence": [], "historyWindowHours": 72, "collectionStatus": statuses,
        "correlation": {"available": False},
    }
    network_name = {"wired": "유선", "wireless": "무선", "unknown": "네트워크"}[connection]
    pair = None
    if evidence or current_down:
        result.update(classification="network_link_failure", status="fail" if current_down else "check_needed", label="링크 단절 확인")
        result["summary"] = ("최근 72시간 " + network_name + " 링크 단절 " + str(len(evidence)) + "건이 확인됐어. ") if evidence else ""
        result["summary"] += "현재 링크도 끊겨 있어." if current_down else ("현재 링크는 연결돼 있어." if current_up else "현재 연결 상태는 추가 확인이 필요해.")
        result["action"] = "케이블과 접속 단자, 스위치 포트를 확인하고 지속되면 장비 랜포트를 점검해." if connection == "wired" else "무선 연결과 AP, 신호 상태를 확인해."
        # 수집한 사건의 시각 차이만 설명하며 캡처 실패나 책임 소재로 확장하지 않는다.
        pairs = [(link, error, _time_distance(error["timestamp"], link["timestamp"])) for link in evidence for error in errors if error["source"] == "app_daily_log"]
        pair = next(((link, error, seconds) for link, error, seconds in reversed(pairs) if seconds is not None and abs(seconds) <= 90), None)
        if pair:
            result["summary"] += " 링크 단절과 90초 이내에 앱 통신 오류도 기록됐어."
            result["correlation"] = {"available": True, "linkTimestamp": pair[0]["timestamp"], "appTimestamp": pair[1]["timestamp"], "linkBeforeAppError": pair[2] >= 0}
    elif route_missing:
        result.update(classification="network_route_failure", status="fail", label="기본 경로 없음", summary="현재 외부 통신에 필요한 기본 경로가 없어.", action="장비 IP와 게이트웨이 설정, 연결된 네트워크를 확인해.")
    elif errors:
        result.update(classification="network_error_only", label="통신 이상 기록 확인", summary="앱 통신 오류 또는 네트워크 연결 종료 기록이 확인됐어. 이 기록만으로 물리적 링크 단절은 확정할 수 없어.", action="같은 시각의 링크 이력과 DNS, 게이트웨이를 함께 확인해.")
    elif available:
        result.update(classification="no_network_evidence", label="수집 범위 내 이상 근거 없음", summary="수집된 범위에서 네트워크 오류 근거는 확인되지 않았어. 외부 연결 성공 여부는 검사하지 않았어.", action="증상이 반복되면 발생 시각을 기준으로 다시 점검해.")
        if current_up and not incomplete:
            result["status"] = "pass"
    if incomplete:
        result["summary"] += " 일부 조회가 불완전해(" + ", ".join(incomplete) + ")."
    selected_links, selected_errors = evidence[-6:], errors[-6:]
    if pair:
        # 시각 대조에 사용한 사건이 화면과 LLM 근거에서도 누락되지 않게 한다.
        for selected, item in ((selected_links, pair[0]), (selected_errors, pair[1])):
            if item not in selected:
                selected[:] = selected[-5:] + [item]
    result["evidence"] = selected_links + selected_errors
    return result
