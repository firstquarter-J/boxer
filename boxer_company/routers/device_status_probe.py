import json
import re
from typing import Any

from boxer.core.utils import _display_value
from boxer_company import settings as cs
from boxer_company.routers.barcode_log import _extract_device_name_scope
from boxer_company.routers.device_audio_probe import (
    _parse_default_sink,
    _parse_mixer_control,
    _parse_playback_devices,
    _parse_tool_paths,
    _summarize_device_audio_probe,
)
from boxer_company.routers.device_file_probe import _connect_device_ssh_client
from boxer_company.routers.mda_graphql import _wait_for_mda_device_agent_ssh

_LEADING_DEVICE_PROBE_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)
_USB_LINE_PATTERN = re.compile(
    r"^Bus\s+\d+\s+Device\s+\d+:\s+ID\s+([0-9a-fA-F]{4}):([0-9a-fA-F]{4})\s*(.*)$",
    re.IGNORECASE,
)
_DEVICE_STATUS_HINTS = (
    "장비 상태",
    "상태 점검",
    "장비 점검",
    "전체 상태",
    "종합 상태",
    "health check",
    "healthcheck",
    "헬스 체크",
    "헬스체크",
)
_DEVICE_PM2_HINTS = ("pm2",)
_DEVICE_CAPTUREBOARD_HINTS = (
    "캡처보드",
    "캡쳐보드",
    "captureboard",
    "capture board",
)
_DEVICE_LED_HINTS = ("led", "엘이디")
_DEVICE_STATUS_ALL_HINTS = (
    *_DEVICE_STATUS_HINTS,
    *_DEVICE_PM2_HINTS,
    *_DEVICE_CAPTUREBOARD_HINTS,
    *_DEVICE_LED_HINTS,
)
_CAPTUREBOARD_USB_SIGNATURES = {
    (0x534D, 0x0021): "LS_EASYCAP",
    (0x1BCF, 0x2C99): "LS_HDMI",
    (0x1164, 0xF57A): "YUH01",
    (0x32ED, 0x3200): "GAMEDOCK_ULTRA",
    (0x32ED, 0x3201): "GAMEDOCK_ULTRA",
    (0x1164, 0x656A): "YUH01",
    (0x0B05, 0xE001): "ASUS",
}
_LED_USB_SIGNATURES = {
    (0x1A86, 0x7523): "MmtLEDv3",
}
_PM2_TARGET_APP_ALIASES = {
    "mommybox-v2": "mommybox-v2",
    "mommybox-v2-agent": "mommybox-agent",
    "mommybox-agent": "mommybox-agent",
}
_PM2_CANONICAL_APP_ORDER = ("mommybox-v2", "mommybox-agent")
_PM2_REQUIRED_APPS = ("mommybox-v2",)
_PM2_TRANSITION_STATUSES = {"launching", "waiting restart", "stopping"}
_LOGIN_SHELL_USER_PATH_EXPORT = 'export PATH="$HOME/.npm-global/bin:$HOME/bin:/usr/local/bin:$PATH"; '

_PROBE_COMMAND_SPECS: dict[str, dict[str, Any]] = {
    "tools": {
        "summary": "점검 도구 확인",
        "timeout_sec": 10,
        "command": (
            "bash -lc '"
            f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
            "for t in aplay amixer pactl pm2 lsusb v4l2-ctl; do "
            "printf \"%s=\" \"$t\"; command -v \"$t\" || true; echo; "
            "done'"
        ),
    },
    "playback_devices": {
        "summary": "재생 장치 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v aplay >/dev/null 2>&1; then "
            "aplay -l 2>&1; "
            "else echo aplay_missing; fi'"
        ),
    },
    "master_mixer": {
        "summary": "Master 볼륨 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v amixer >/dev/null 2>&1; then "
            "amixer sget Master 2>&1; "
            "else echo amixer_missing; fi'"
        ),
    },
    "pcm_mixer": {
        "summary": "PCM 볼륨 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v amixer >/dev/null 2>&1; then "
            "amixer sget PCM 2>&1; "
            "else echo amixer_missing; fi'"
        ),
    },
    "pactl_info": {
        "summary": "기본 sink 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v pactl >/dev/null 2>&1; then "
            "pactl info 2>&1; "
            "else echo pactl_missing; fi'"
        ),
    },
    "pm2_jlist": {
        "summary": "PM2 앱 상태 확인",
        "timeout_sec": 12,
        "command": (
            "bash -lc '"
            f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
            "if command -v pm2 >/dev/null 2>&1; then "
            "pm2 jlist 2>&1; "
            "else echo pm2_missing; fi'"
        ),
    },
    "lsusb": {
        "summary": "USB 장치 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v lsusb >/dev/null 2>&1; then "
            "lsusb 2>&1; "
            "else echo lsusb_missing; fi'"
        ),
    },
    "serial_devices": {
        "summary": "시리얼 장치 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'paths=$(ls /dev/ttyUSB* /dev/ttyACM* 2>/dev/null || true); "
            "if [ -n \"$paths\" ]; then printf \"%s\\n\" \"$paths\"; else echo no_serial_device; fi'"
        ),
    },
    "video_devices": {
        "summary": "비디오 장치 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'paths=$(ls /dev/video* 2>/dev/null || true); "
            "if [ -n \"$paths\" ]; then printf \"%s\\n\" \"$paths\"; else echo no_video_device; fi'"
        ),
    },
    "v4l2_devices": {
        "summary": "v4l2 장치 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v v4l2-ctl >/dev/null 2>&1; then "
            "v4l2-ctl --list-devices 2>&1; "
            "else echo v4l2_missing; fi'"
        ),
    },
}
_PROBE_COMPONENT_COMMAND_KEYS = {
    "all": (
        "tools",
        "playback_devices",
        "master_mixer",
        "pcm_mixer",
        "pactl_info",
        "pm2_jlist",
        "lsusb",
        "serial_devices",
        "video_devices",
        "v4l2_devices",
    ),
    "pm2": (
        "tools",
        "pm2_jlist",
    ),
    "captureboard": (
        "tools",
        "lsusb",
        "video_devices",
        "v4l2_devices",
    ),
    "led": (
        "tools",
        "lsusb",
        "serial_devices",
    ),
}


def _normalize_device_status_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _contains_hint(text: str, hints: tuple[str, ...]) -> bool:
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    return any(hint in normalized or hint in lowered for hint in hints)


def _extract_device_name_for_status_probe(question: str) -> str | None:
    normalized = _normalize_device_status_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _contains_hint(normalized, _DEVICE_STATUS_ALL_HINTS):
        return extracted

    matched = _LEADING_DEVICE_PROBE_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _contains_hint(remainder, _DEVICE_STATUS_ALL_HINTS):
        return None
    return candidate


def _is_device_pm2_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_PM2_HINTS))


def _is_device_captureboard_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_CAPTUREBOARD_HINTS))


def _is_device_led_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_LED_HINTS))


def _is_device_status_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    if not resolved_device_name:
        return False
    if (
        _is_device_pm2_probe_request(normalized, resolved_device_name)
        or _is_device_captureboard_probe_request(normalized, resolved_device_name)
        or _is_device_led_probe_request(normalized, resolved_device_name)
    ):
        return False
    return _contains_hint(normalized, _DEVICE_STATUS_HINTS)


def _build_device_status_probe_config_message() -> str:
    return (
        "장비 상태 점검 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD, DEVICE_SSH_PASSWORD가 필요해"
    )


def _display_device_status_probe_reason(reason: str | None) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized in {"agent_ssh_not_ready", "novalidconnectionserror", "timeout", "oerror"}:
        return "장비 SSH 연결 준비 실패. 온라인 상태, 네트워크, 원격 접속 상태 먼저 확인해"
    if normalized == "ssh_auth_failed":
        return "장비 SSH 인증 실패"
    if normalized == "missing_device_name":
        return "장비명이 없어 장비 상태 점검 불가"
    if normalized == "missing_password":
        return "DEVICE_SSH_PASSWORD 설정이 없어 장비 상태 점검 불가"
    if normalized == "paramiko_missing":
        return "paramiko 설치가 없어 장비 상태 점검 불가"
    if normalized.startswith("ssh_exit_"):
        return f"장비 상태 점검 명령 실패 ({normalized})"
    if not normalized:
        return "장비 상태 점검 실패"
    return normalized


def _run_status_probe_command(client: Any, key: str) -> dict[str, Any]:
    spec = _PROBE_COMMAND_SPECS[key]
    command = str(spec.get("command") or "").strip()
    timeout_sec = max(1, int(spec.get("timeout_sec") or cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10))
    try:
        _, stdout, stderr = client.exec_command(command, timeout=timeout_sec)
        exit_status = stdout.channel.recv_exit_status()
        stdout_text = (stdout.read() or b"").decode("utf-8", errors="replace").strip()
        stderr_text = (stderr.read() or b"").decode("utf-8", errors="replace").strip()
        combined = stdout_text
        if stderr_text:
            combined = combined or stderr_text
            if stdout_text and stderr_text not in stdout_text:
                combined = f"{stdout_text}\n{stderr_text}"
        return {
            "key": key,
            "summary": _display_value(spec.get("summary"), default=""),
            "ok": exit_status == 0,
            "exitStatus": exit_status,
            "output": combined,
            "reason": "" if exit_status == 0 else f"ssh_exit_{exit_status}",
        }
    except Exception as exc:  # pragma: no cover - network/remote dependent
        return {
            "key": key,
            "summary": _display_value(spec.get("summary"), default=""),
            "ok": False,
            "exitStatus": None,
            "output": "",
            "reason": type(exc).__name__.lower(),
        }


def _parse_usb_devices(text: str) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == "lsusb_missing":
        return {
            "available": False,
            "reason": "lsusb_missing",
            "devices": [],
        }

    devices: list[dict[str, Any]] = []
    for line in normalized.splitlines():
        matched = _USB_LINE_PATTERN.search(line.strip())
        if not matched:
            continue
        devices.append(
            {
                "vendorId": int(matched.group(1), 16),
                "productId": int(matched.group(2), 16),
                "label": str(matched.group(3) or "").strip(),
                "raw": line.strip(),
            }
        )
    return {
        "available": True,
        "reason": "ok",
        "devices": devices,
    }


def _parse_device_path_list(text: str, *, missing_token: str) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == missing_token:
        return {
            "available": False,
            "reason": missing_token,
            "count": 0,
            "paths": [],
        }

    paths = [line.strip() for line in normalized.splitlines() if line.strip()]
    return {
        "available": bool(paths),
        "reason": "ok" if paths else "not_found",
        "count": len(paths),
        "paths": paths,
    }


def _parse_pm2_processes(text: str) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == "pm2_missing":
        return {
            "available": False,
            "reason": "pm2_missing",
            "processes": [],
        }

    start = normalized.find("[")
    end = normalized.rfind("]")
    if start < 0 or end < start:
        return {
            "available": True,
            "reason": "json_parse_failed",
            "processes": [],
        }

    try:
        payload = json.loads(normalized[start : end + 1])
    except json.JSONDecodeError:
        return {
            "available": True,
            "reason": "json_parse_failed",
            "processes": [],
        }

    processes: list[dict[str, Any]] = []
    if isinstance(payload, list):
        for item in payload:
            if not isinstance(item, dict):
                continue
            env = item.get("pm2_env") if isinstance(item.get("pm2_env"), dict) else {}
            monit = item.get("monit") if isinstance(item.get("monit"), dict) else {}
            processes.append(
                {
                    "name": _display_value(item.get("name"), default=""),
                    "status": _display_value(env.get("status"), default=""),
                    "version": (
                        _display_value(env.get("version"), default="")
                        or _display_value(
                            (env.get("versioning") or {}).get("version")
                            if isinstance(env.get("versioning"), dict)
                            else "",
                            default="",
                        )
                        or _display_value(
                            (env.get("versioning") or {}).get("revision")
                            if isinstance(env.get("versioning"), dict)
                            else "",
                            default="",
                        )
                    ),
                    "restartCount": int(env.get("restart_time") or 0),
                    "cpu": monit.get("cpu"),
                    "memory": monit.get("memory"),
                }
            )

    return {
        "available": True,
        "reason": "ok",
        "processes": processes,
    }


def _find_usb_signature_matches(
    usb_devices: dict[str, Any],
    signatures: dict[tuple[int, int], str],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    for device in usb_devices.get("devices") or []:
        if not isinstance(device, dict):
            continue
        signature = (int(device.get("vendorId") or 0), int(device.get("productId") or 0))
        alias = signatures.get(signature)
        if alias:
            matches.append(
                {
                    **device,
                    "alias": alias,
                }
            )
    return matches


def _format_pm2_target_evidence(canonical_name: str, process: dict[str, Any]) -> str:
    actual_name = _display_value(process.get("name"), default="미확인")
    status = _display_value(process.get("status"), default="미확인")
    version = _display_value(process.get("version"), default="")
    restart_count = int(process.get("restartCount") or 0)
    details = [status]
    if version:
        details.append(f"v{version}")
    details.append(f"재시작 {restart_count}회")
    return f"{canonical_name}={actual_name}({' / '.join(details)})"


def _format_pm2_target_overview(canonical_name: str, process: dict[str, Any]) -> str:
    status = _display_value(process.get("status"), default="미확인")
    version = _display_value(process.get("version"), default="")
    parts = [canonical_name]
    if version:
        parts.append(f"v{version}")
    if status:
        parts.append(status)
    return " ".join(parts)


def _summarize_pm2_probe(pm2_processes: dict[str, Any]) -> dict[str, Any]:
    if not pm2_processes.get("available"):
        return {
            "status": "fail",
            "label": "이상",
            "summary": "PM2 명령을 찾지 못했어",
            "evidence": "pm2 미설치 또는 PATH 미확인",
            "action": "장비에서 PM2 설치 상태와 PATH를 확인해",
        }

    processes = [item for item in pm2_processes.get("processes") or [] if isinstance(item, dict)]
    grouped_targets: dict[str, list[dict[str, Any]]] = {}
    for item in processes:
        actual_name = _display_value(item.get("name"), default="")
        canonical_name = _PM2_TARGET_APP_ALIASES.get(actual_name)
        if not canonical_name:
            continue
        grouped_targets.setdefault(canonical_name, []).append(item)

    if not grouped_targets:
        return {
            "status": "fail",
            "label": "이상",
            "summary": "PM2에는 있지만 mommybox-v2 나 mommybox-agent 앱이 보이지 않아",
            "evidence": "대상 PM2 프로세스 미감지",
            "action": "pm2 등록 상태와 앱 실행 구성을 확인해",
        }

    selected_targets: dict[str, dict[str, Any]] = {}
    for canonical_name in _PM2_CANONICAL_APP_ORDER:
        candidates = grouped_targets.get(canonical_name) or []
        if not candidates:
            continue
        selected_targets[canonical_name] = sorted(
            candidates,
            key=lambda item: (
                0 if _display_value(item.get("status"), default="").strip().lower() == "online" else 1,
                int(item.get("restartCount") or 0),
            ),
        )[0]

    evidence_parts = [
        _format_pm2_target_evidence(canonical_name, selected_targets[canonical_name])
        for canonical_name in _PM2_CANONICAL_APP_ORDER
        if canonical_name in selected_targets
    ]
    overview_detail = " / ".join(
        _format_pm2_target_overview(canonical_name, selected_targets[canonical_name])
        for canonical_name in _PM2_CANONICAL_APP_ORDER
        if canonical_name in selected_targets
    )

    missing_required = [name for name in _PM2_REQUIRED_APPS if name not in selected_targets]
    if missing_required:
        return {
            "status": "fail",
            "label": "이상",
            "summary": "PM2에서 핵심 앱 mommybox-v2 가 보이지 않아",
            "evidence": " / ".join(evidence_parts) or "mommybox-v2 미감지",
            "overviewDetail": overview_detail or "mommybox-v2 미감지",
            "action": "pm2 등록 상태와 본 앱 실행 구성을 확인해",
        }

    if "mommybox-agent" not in selected_targets:
        return {
            "status": "warning",
            "label": "확인 필요",
            "summary": "mommybox-v2 는 보이지만 mommybox-agent 앱은 안 보여",
            "evidence": " / ".join(evidence_parts),
            "overviewDetail": overview_detail,
            "action": "장비가 agent 구성을 써야 하는 장비인지와 PM2 등록 상태를 확인해",
        }

    statuses = {
        canonical_name: _display_value(item.get("status"), default="").strip().lower()
        for canonical_name, item in selected_targets.items()
    }
    if all(status == "online" for status in statuses.values()):
        return {
            "status": "pass",
            "label": "정상",
            "summary": "PM2 기준 mommybox-v2 와 mommybox-agent 앱이 정상 실행 중이야",
            "evidence": " / ".join(evidence_parts),
            "overviewDetail": overview_detail,
            "action": "앱 프로세스와 실행 버전은 정상으로 보여",
        }
    if any(status in _PM2_TRANSITION_STATUSES for status in statuses.values()):
        return {
            "status": "warning",
            "label": "확인 필요",
            "summary": "PM2 앱이 전환 중이거나 재시작 중이야",
            "evidence": " / ".join(evidence_parts),
            "overviewDetail": overview_detail,
            "action": "잠시 후 다시 확인하고 반복되면 PM2 로그를 봐",
        }
    return {
        "status": "fail",
        "label": "이상",
        "summary": "PM2 앱 상태가 online이 아니야",
        "evidence": " / ".join(evidence_parts),
        "overviewDetail": overview_detail,
        "action": "PM2 상태와 앱 로그를 같이 확인해",
    }


def _summarize_captureboard_probe(
    *,
    device_info: dict[str, Any],
    usb_devices: dict[str, Any],
    video_devices: dict[str, Any],
    v4l2_devices: str,
) -> dict[str, Any]:
    matches = _find_usb_signature_matches(usb_devices, _CAPTUREBOARD_USB_SIGNATURES)
    aliases = sorted({str(item.get("alias") or "").strip() for item in matches if str(item.get("alias") or "").strip()})
    expected_alias = _display_value(device_info.get("captureBoardType"), default="")
    video_count = int(video_devices.get("count") or 0)
    v4l2_has_video = "/dev/video" in str(v4l2_devices or "")

    evidence_parts: list[str] = []
    if expected_alias:
        evidence_parts.append(f"MDA 타입 `{expected_alias}`")
    if aliases:
        evidence_parts.append(f"USB `{', '.join(aliases)}`")
    if video_count > 0:
        evidence_parts.append(f"/dev/video `{video_count}개`")
    elif v4l2_has_video:
        evidence_parts.append("v4l2 장치 확인")

    if aliases and (video_count > 0 or v4l2_has_video):
        if expected_alias and expected_alias not in aliases:
            return {
                "status": "warning",
                "label": "확인 필요",
                "summary": "캡처보드는 잡히지만 MDA 타입과 로컬 인식 타입이 달라 보여",
                "evidence": " / ".join(evidence_parts) or "캡처보드 타입 불일치",
                "overviewDetail": " / ".join(evidence_parts) or "캡처보드 타입 불일치",
                "action": "실제 연결된 캡처보드 모델과 장비 설정을 확인해",
            }
        return {
            "status": "pass",
            "label": "정상",
            "summary": "캡처보드 USB와 비디오 장치가 같이 보여",
            "evidence": " / ".join(evidence_parts) or "캡처보드 감지",
            "overviewDetail": " / ".join(evidence_parts) or "캡처보드 감지",
            "action": "하드웨어 연결 자체는 정상으로 보여",
        }
    if aliases and video_count <= 0 and not v4l2_has_video:
        return {
            "status": "warning",
            "label": "확인 필요",
            "summary": "캡처보드 USB는 보이지만 비디오 장치가 안 보여",
            "evidence": " / ".join(evidence_parts) or "USB만 감지",
            "overviewDetail": " / ".join(evidence_parts) or "USB만 감지",
            "action": "/dev/video 장치 생성 여부와 재인식 상태를 확인해",
        }
    if video_count > 0 or v4l2_has_video:
        return {
            "status": "warning",
            "label": "확인 필요",
            "summary": "비디오 장치는 보이지만 캡처보드 USB 식별은 확실하지 않아",
            "evidence": " / ".join(evidence_parts) or "비디오 장치만 감지",
            "overviewDetail": " / ".join(evidence_parts) or "비디오 장치만 감지",
            "action": "캡처보드 USB 연결과 모델 인식 상태를 같이 확인해",
        }
    return {
        "status": "fail",
        "label": "이상",
        "summary": "캡처보드 USB나 비디오 장치를 찾지 못했어",
        "evidence": " / ".join(evidence_parts) or "캡처보드 미감지",
        "overviewDetail": " / ".join(evidence_parts) or "캡처보드 미감지",
        "action": "캡처보드 전원, USB 연결, 장치 재인식을 먼저 확인해",
    }


def _summarize_led_probe(
    *,
    usb_devices: dict[str, Any],
    serial_devices: dict[str, Any],
) -> dict[str, Any]:
    matches = _find_usb_signature_matches(usb_devices, _LED_USB_SIGNATURES)
    serial_paths = [path for path in serial_devices.get("paths") or [] if isinstance(path, str) and path.strip()]

    evidence_parts: list[str] = []
    if matches:
        evidence_parts.append("LED USB 감지")
    if serial_paths:
        evidence_parts.append(f"시리얼 경로 `{len(serial_paths)}개`")

    if matches:
        return {
            "status": "pass",
            "label": "정상",
            "summary": "LED 장치 USB 연결은 정상으로 보여",
            "evidence": " / ".join(evidence_parts) or "LED USB 감지",
            "overviewDetail": " / ".join(evidence_parts) or "LED USB 감지",
            "action": "LED 물리 연결 자체는 정상으로 보여",
        }
    if serial_paths:
        return {
            "status": "warning",
            "label": "확인 필요",
            "summary": "시리얼 장치는 보이지만 LED 장치 ID는 확실하지 않아",
            "evidence": " / ".join(evidence_parts) or "시리얼 장치 감지",
            "overviewDetail": " / ".join(evidence_parts) or "시리얼 장치 감지",
            "action": "LED USB 장치와 시리얼 변환기 연결 상태를 확인해",
        }
    return {
        "status": "fail",
        "label": "이상",
        "summary": "LED USB 장치를 찾지 못했어",
        "evidence": " / ".join(evidence_parts) or "LED 미감지",
        "overviewDetail": " / ".join(evidence_parts) or "LED 미감지",
        "action": "LED 케이블과 USB 연결 상태를 먼저 확인해",
    }


def _summarize_audio_path_probe(checks: dict[str, dict[str, Any]]) -> dict[str, Any]:
    tool_paths = _parse_tool_paths(_display_value((checks.get("tools") or {}).get("output"), default=""))
    playback_devices = _parse_playback_devices(
        _display_value((checks.get("playback_devices") or {}).get("output"), default="")
    )
    master_mixer = _parse_mixer_control(
        _display_value((checks.get("master_mixer") or {}).get("output"), default=""),
        control_name="Master",
    )
    pcm_mixer = _parse_mixer_control(
        _display_value((checks.get("pcm_mixer") or {}).get("output"), default=""),
        control_name="PCM",
    )
    default_sink = _parse_default_sink(
        _display_value((checks.get("pactl_info") or {}).get("output"), default="")
    )
    summary = _summarize_device_audio_probe(
        tool_paths=tool_paths,
        playback_devices=playback_devices,
        master_mixer=master_mixer,
        pcm_mixer=pcm_mixer,
        default_sink=default_sink,
        playback_test={
            "available": False,
            "ok": False,
            "reason": "playback_test_skipped",
            "usedCommand": "none",
        },
    )
    effective_status = _display_value(summary.get("status"), default="check_needed")
    device_count = int(playback_devices.get("deviceCount") or 0)
    mixer_muted = bool(summary.get("mixerMuted"))
    if effective_status in {"check_needed", "warning"} and device_count > 0 and not mixer_muted:
        effective_status = "pass"

    label = "정상" if effective_status == "pass" else "이상" if effective_status == "fail" else "확인 필요"
    evidence_parts: list[str] = []
    device_labels = [
        _display_value(item.get("deviceName"), default="")
        for item in playback_devices.get("devices") or []
        if isinstance(item, dict) and _display_value(item.get("deviceName"), default="")
    ]
    unique_device_labels: list[str] = []
    for label_text in device_labels:
        if label_text not in unique_device_labels:
            unique_device_labels.append(label_text)
    if unique_device_labels:
        evidence_parts.append("오디오 장치 " + ", ".join(f"`{label}`" for label in unique_device_labels))
    else:
        evidence_parts.append(f"재생 장치 `{device_count}개`")
    mixer_summary = _display_value(summary.get("mixerSummary"), default="")
    if mixer_summary:
        evidence_parts.append(f"음량 `{mixer_summary}`")
    if default_sink.get("available"):
        evidence_parts.append(f"기본 sink `{_display_value(default_sink.get('defaultSink'), default='미확인')}`")
    if effective_status == "pass":
        summary_text = "미니PC 오디오 장치와 음량 설정은 정상으로 보여"
    else:
        summary_text = _display_value(summary.get("summary"), default="확인 필요")
    return {
        "status": effective_status,
        "label": label,
        "summary": summary_text,
        "evidence": " / ".join(evidence_parts),
        "overviewDetail": " / ".join(evidence_parts),
        "deviceLabelsText": ", ".join(f"`{label}`" for label in unique_device_labels),
        "volumeText": f"`{mixer_summary}`" if mixer_summary else "",
        "sinkText": f"`{_display_value(default_sink.get('defaultSink'), default='미확인')}`" if default_sink.get("available") else "",
        "action": "실제 소리 재생 확인은 `장비 소리 출력 점검`으로 따로 점검해",
    }


def _build_device_header_lines(
    *,
    title: str,
    device_name: str,
    device_info: dict[str, Any],
) -> list[str]:
    version = _display_value(device_info.get("version"), default="")
    hospital_name = _display_value(device_info.get("hospitalName"), default="")
    room_name = _display_value(device_info.get("roomName"), default="")
    lines = [title]
    device_line = f"• 장비: `{device_name}`"
    if version:
        device_line = f"{device_line} | 버전: `{version}`"
    lines.append(device_line)
    if hospital_name or room_name:
        location_parts: list[str] = []
        if hospital_name:
            location_parts.append(f"`{hospital_name}`")
        if room_name:
            location_parts.append(f"`{room_name}`")
        lines.append(f"• 위치: {' / '.join(location_parts)}")
    return lines


def _render_single_probe_result(
    *,
    title: str,
    device_name: str,
    device_info: dict[str, Any],
    ssh_ready: bool,
    ssh_reason: str,
    summary: dict[str, Any],
) -> str:
    lines = _build_device_header_lines(title=title, device_name=device_name, device_info=device_info)
    if not ssh_ready:
        lines.append("• 판정: *점검 불가*")
        lines.append(f"• 안내: {_display_device_status_probe_reason(ssh_reason)}")
        return "\n".join(lines)

    lines.append(f"• 판정: *{_display_value(summary.get('label'), default='확인 필요')}*")
    lines.append(f"• 근거: {_display_value(summary.get('evidence'), default='미확인')}")
    lines.append(f"• 안내: {_display_value(summary.get('summary'), default='확인 필요')}")
    action = _display_value(summary.get("action"), default="")
    if action:
        lines.append(f"• 조치: {action}")
    return "\n".join(lines)


def _render_device_status_overview_result(
    *,
    device_name: str,
    device_info: dict[str, Any],
    ssh_ready: bool,
    ssh_reason: str,
    audio_summary: dict[str, Any] | None,
    pm2_summary: dict[str, Any] | None,
    captureboard_summary: dict[str, Any] | None,
    led_summary: dict[str, Any] | None,
) -> str:
    lines = _build_device_header_lines(
        title="*장비 상태 점검*",
        device_name=device_name,
        device_info=device_info,
    )
    if not ssh_ready:
        lines.append("• SSH 연결: *점검 불가*")
        lines.append("• 소리 출력 경로: *점검 불가*")
        lines.append("• pm2 앱: *점검 불가*")
        lines.append("• 캡처보드: *점검 불가*")
        lines.append("• LED: *점검 불가*")
        lines.append(f"• 안내: {_display_device_status_probe_reason(ssh_reason)}")
        return "\n".join(lines)

    component_summaries = {
        "소리 출력": audio_summary or {},
        "pm2 앱": pm2_summary or {},
        "캡처보드": captureboard_summary or {},
        "LED": led_summary or {},
    }
    worst_rank = 0
    for label, summary in component_summaries.items():
        state = _display_value(summary.get("status"), default="check_needed")
        if state == "fail":
            worst_rank = max(worst_rank, 2)
        elif state != "pass":
            worst_rank = max(worst_rank, 1)

    if worst_rank >= 2:
        overall = "이상"
    elif worst_rank == 1:
        overall = "확인 필요"
    else:
        overall = "정상"

    audio_payload = audio_summary or {}
    audio_label = _display_value(audio_payload.get("label"), default="확인 필요")
    audio_device_labels = _display_value(audio_payload.get("deviceLabelsText"), default="")
    audio_volume_text = _display_value(audio_payload.get("volumeText"), default="")
    audio_parts: list[str] = []
    if audio_device_labels:
        audio_parts.append(f"장치 {audio_device_labels}")
    if audio_volume_text:
        audio_parts.append(f"음량 {audio_volume_text}")

    pm2_payload = pm2_summary or {}
    pm2_label = _display_value(pm2_payload.get("label"), default="확인 필요")
    pm2_detail = _display_value(pm2_payload.get("overviewDetail"), default="")

    capture_payload = captureboard_summary or {}
    capture_label = _display_value(capture_payload.get("label"), default="확인 필요")
    capture_detail = _display_value(capture_payload.get("overviewDetail"), default="")

    led_payload = led_summary or {}
    led_label = _display_value(led_payload.get("label"), default="확인 필요")
    led_detail = _display_value(led_payload.get("overviewDetail"), default="")

    lines.append("")
    lines.append("*오디오*")
    audio_line = f"• 소리 출력: *{audio_label}*"
    if audio_parts:
        audio_line = f"{audio_line} | {' / '.join(audio_parts)}"
    lines.append(audio_line)

    lines.append("")
    lines.append("*런타임*")
    pm2_line = f"• pm2 앱: *{pm2_label}*"
    if pm2_detail:
        pm2_line = f"{pm2_line} | {pm2_detail}"
    lines.append(pm2_line)

    lines.append("")
    lines.append("*하드웨어*")
    capture_line = f"• 캡처보드: *{capture_label}*"
    if capture_detail:
        capture_line = f"{capture_line} | {capture_detail}"
    lines.append(capture_line)
    led_line = f"• LED: *{led_label}*"
    if led_detail:
        led_line = f"{led_line} | {led_detail}"
    lines.append(led_line)

    lines.append("")
    lines.append("*종합*")
    lines.append(f"• 상태: *{overall}*")
    lines.append(f"• 안내: 실제 소리 출력 테스트는 `{device_name} 장비 소리 출력 점검`으로 다시 명령해")
    return "\n".join(lines)


def _build_runtime_probe_payload(
    *,
    device_name: str,
    component: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    wait_result = _wait_for_mda_device_agent_ssh(device_name)
    device_info = wait_result.get("device") if isinstance(wait_result.get("device"), dict) else {}
    agent_ssh = device_info.get("agentSsh") if isinstance(device_info.get("agentSsh"), dict) else {}
    host = _display_value(agent_ssh.get("host"), default="")
    port = agent_ssh.get("port")
    try:
        port = int(port)
    except (TypeError, ValueError):
        port = 0

    evidence_payload: dict[str, Any] = {
        "route": "device_status_probe",
        "source": "mda_graphql+ssh",
        "request": {
            "deviceName": device_name,
            "component": component,
        },
        "device": {
            "deviceName": _display_value(device_info.get("deviceName"), default=device_name),
            "version": _display_value(device_info.get("version"), default=""),
            "captureBoardType": _display_value(device_info.get("captureBoardType"), default=""),
            "hospitalName": _display_value(device_info.get("hospitalName"), default=""),
            "roomName": _display_value(device_info.get("roomName"), default=""),
            "isConnected": bool(device_info.get("isConnected")),
        },
        "ssh": {
            "ready": bool(wait_result.get("ready")) and bool(host) and port > 0,
            "reason": "ready" if bool(wait_result.get("ready")) and bool(host) and port > 0 else "agent_ssh_not_ready",
            "host": host,
            "port": port,
            "pollCount": wait_result.get("pollCount"),
            "reusedExisting": bool(wait_result.get("reusedExisting")),
        },
    }
    return evidence_payload, device_info


def _collect_runtime_checks(device_name: str, component: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, dict[str, Any]]]:
    evidence_payload, device_info = _build_runtime_probe_payload(device_name=device_name, component=component)
    if not evidence_payload["ssh"]["ready"]:
        return evidence_payload, device_info, {}

    agent_ssh = evidence_payload.get("ssh") if isinstance(evidence_payload.get("ssh"), dict) else {}
    host = _display_value(agent_ssh.get("host"), default="")
    try:
        port = int(agent_ssh.get("port") or 0)
    except (TypeError, ValueError):
        port = 0
    connection = _connect_device_ssh_client(host, port)
    if not connection.get("ok"):
        evidence_payload["ssh"] = {
            **agent_ssh,
            "ready": False,
            "reason": _display_value(connection.get("reason"), default="ssh_connect_failed"),
        }
        return evidence_payload, device_info, {}

    client = connection["client"]
    keys = _PROBE_COMPONENT_COMMAND_KEYS[component]
    try:
        results = {
            key: _run_status_probe_command(client, key)
            for key in keys
        }
    finally:
        client.close()
    return evidence_payload, device_info, results


def _probe_device_runtime_component(device_name: str, *, component: str) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 장비 상태`")

    evidence_payload, device_info, checks = _collect_runtime_checks(normalized_device_name, component)
    ssh = evidence_payload.get("ssh") if isinstance(evidence_payload.get("ssh"), dict) else {}
    ssh_ready = bool(ssh.get("ready"))
    ssh_reason = _display_value(ssh.get("reason"), default="")

    if component == "pm2":
        summary = _summarize_pm2_probe(
            _parse_pm2_processes(_display_value((checks.get("pm2_jlist") or {}).get("output"), default=""))
        )
        title = "*장비 PM2 상태 점검*"
    elif component == "captureboard":
        summary = _summarize_captureboard_probe(
            device_info=device_info,
            usb_devices=_parse_usb_devices(_display_value((checks.get("lsusb") or {}).get("output"), default="")),
            video_devices=_parse_device_path_list(
                _display_value((checks.get("video_devices") or {}).get("output"), default=""),
                missing_token="no_video_device",
            ),
            v4l2_devices=_display_value((checks.get("v4l2_devices") or {}).get("output"), default=""),
        )
        title = "*장비 캡처보드 점검*"
    elif component == "led":
        summary = _summarize_led_probe(
            usb_devices=_parse_usb_devices(_display_value((checks.get("lsusb") or {}).get("output"), default="")),
            serial_devices=_parse_device_path_list(
                _display_value((checks.get("serial_devices") or {}).get("output"), default=""),
                missing_token="no_serial_device",
            ),
        )
        title = "*장비 LED 점검*"
    else:
        raise ValueError(f"지원하지 않는 장비 점검 종류야: {component}")

    evidence_payload["componentSummary"] = summary
    result_text = _render_single_probe_result(
        title=title,
        device_name=normalized_device_name,
        device_info=device_info,
        ssh_ready=ssh_ready,
        ssh_reason=ssh_reason,
        summary=summary,
    )
    return result_text, evidence_payload


def _probe_device_status_overview(device_name: str) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 장비 상태`")

    evidence_payload, device_info, checks = _collect_runtime_checks(normalized_device_name, "all")
    ssh = evidence_payload.get("ssh") if isinstance(evidence_payload.get("ssh"), dict) else {}
    ssh_ready = bool(ssh.get("ready"))
    ssh_reason = _display_value(ssh.get("reason"), default="")

    audio_summary = None
    pm2_summary = None
    captureboard_summary = None
    led_summary = None
    if ssh_ready:
        audio_summary = _summarize_audio_path_probe(checks)
        pm2_summary = _summarize_pm2_probe(
            _parse_pm2_processes(_display_value((checks.get("pm2_jlist") or {}).get("output"), default=""))
        )
        captureboard_summary = _summarize_captureboard_probe(
            device_info=device_info,
            usb_devices=_parse_usb_devices(_display_value((checks.get("lsusb") or {}).get("output"), default="")),
            video_devices=_parse_device_path_list(
                _display_value((checks.get("video_devices") or {}).get("output"), default=""),
                missing_token="no_video_device",
            ),
            v4l2_devices=_display_value((checks.get("v4l2_devices") or {}).get("output"), default=""),
        )
        led_summary = _summarize_led_probe(
            usb_devices=_parse_usb_devices(_display_value((checks.get("lsusb") or {}).get("output"), default="")),
            serial_devices=_parse_device_path_list(
                _display_value((checks.get("serial_devices") or {}).get("output"), default=""),
                missing_token="no_serial_device",
            ),
        )

    evidence_payload["overview"] = {
        "audio": audio_summary,
        "pm2": pm2_summary,
        "captureboard": captureboard_summary,
        "led": led_summary,
    }
    result_text = _render_device_status_overview_result(
        device_name=normalized_device_name,
        device_info=device_info,
        ssh_ready=ssh_ready,
        ssh_reason=ssh_reason,
        audio_summary=audio_summary,
        pm2_summary=pm2_summary,
        captureboard_summary=captureboard_summary,
        led_summary=led_summary,
    )
    return result_text, evidence_payload
