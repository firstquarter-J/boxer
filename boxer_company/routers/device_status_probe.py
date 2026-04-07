import json
import re
from typing import Any

from boxer.core.utils import _display_value, _format_size, _truncate_text
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
_DEVICE_MEMORY_PATCH_HINTS = (
    "메모리 패치",
    "메모리패치",
    "memory patch",
)
_DEVICE_MEMORY_PATCH_BLOCKING_HINTS = (
    "방법",
    "어떻게",
    "확인 방법",
    "문제 확인",
    "가이드",
    "설명",
    "뭐야",
    "무엇",
    "왜",
)
_DEVICE_CAPTUREBOARD_HINTS = (
    "캡처보드",
    "캡쳐보드",
    "captureboard",
    "capture board",
)
_DEVICE_LED_HINTS = ("led", "엘이디")
_DEVICE_LED_PATTERN_EXPLAIN_HINTS = (
    "증상",
    "패턴",
    "의미",
    "뜻",
    "무슨 상태",
    "어떤 상태",
    "어떨 때",
    "언제",
    "왜",
    "원인",
    "나타나",
    "나와",
    "설명",
)
_DEVICE_LED_COLOR_HINTS = (
    "초록불",
    "녹색불",
    "빨간불",
    "적색불",
    "파란불",
    "청색불",
    "초록",
    "녹색",
    "빨강",
    "빨간",
    "적색",
    "파랑",
    "파란",
    "청색",
    "깜빡",
    "깜빡이",
    "blink",
)
_LED_STATE_SPECS: tuple[dict[str, str], ...] = (
    {
        "state": "ready",
        "command": "LC:ON:G:",
        "meaning": "정상 대기/준비 상태",
    },
    {
        "state": "motion",
        "command": "LC:ON:B:",
        "meaning": "모션 감지 대기 상태",
    },
    {
        "state": "recording",
        "command": "LC:BR:R:",
        "meaning": "녹화 중",
    },
    {
        "state": "paused",
        "command": "LC:BL:B:",
        "meaning": "일시정지",
    },
    {
        "state": "warning",
        "command": "LC:FBL:R:G:",
        "meaning": "현장 알림 피드백이 켜진 장비 기준 이미지 품질 이상, 녹화 정체, 비디오 길이 불일치",
    },
    {
        "state": "error",
        "command": "LC:FBL:R:B:",
        "meaning": "단색 화면, 입력 없음, 화면 어두움, 캡처 입력 이상 같은 에러 상태",
    },
    {
        "state": "busy",
        "command": "LC:3C:",
        "meaning": "종료/재시작 같은 busy 상태",
    },
)
_DEVICE_STATUS_ALL_HINTS = (
    *_DEVICE_STATUS_HINTS,
    *_DEVICE_PM2_HINTS,
    *_DEVICE_MEMORY_PATCH_HINTS,
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
_MEMORY_PATCH_EXPECTED_BYTES = 4 * 1024 * 1024 * 1024
_MEMORY_PATCH_VALUE_PATTERN = re.compile(r"\b(\d{6,})\b")
_MEMORY_PATCH_EXECUTION_COMMAND = (
    "bash -lc '"
    f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
    "cd mommybox-v2 && (pm2 delete mommybox-v2 || true) && pm2 start --env production && pm2 save'"
)
_MEMORY_PATCH_VERIFY_COMMAND = (
    "bash -lc '"
    f"{_LOGIN_SHELL_USER_PATH_EXPORT}"
    "if command -v pm2 >/dev/null 2>&1; then "
    "pm2 prettylist | grep max_memory_restart || true; "
    "else echo pm2_missing; fi'"
)

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


def _is_device_memory_patch_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(
        device_name or _extract_device_name_scope(normalized) or _extract_device_name_for_status_probe(normalized) or ""
    ).strip()
    if not resolved_device_name or not _contains_hint(normalized, _DEVICE_MEMORY_PATCH_HINTS):
        return False
    return not _contains_hint(normalized, _DEVICE_MEMORY_PATCH_BLOCKING_HINTS)


def _is_device_captureboard_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_CAPTUREBOARD_HINTS))


def _is_device_led_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_status_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_status_probe(normalized) or "").strip()
    return bool(resolved_device_name and _contains_hint(normalized, _DEVICE_LED_HINTS))


def _is_device_led_pattern_help_request(question: str) -> bool:
    normalized = _normalize_device_status_question(question)
    if not normalized:
        return False
    has_led_context = _contains_hint(normalized, _DEVICE_LED_HINTS) or _contains_hint(normalized, _DEVICE_LED_COLOR_HINTS)
    if not has_led_context:
        return False
    return _contains_hint(normalized, _DEVICE_LED_PATTERN_EXPLAIN_HINTS)


def _infer_led_pattern_help(question: str) -> dict[str, Any]:
    normalized = _normalize_device_status_question(question)
    has_red = any(token in normalized for token in ("빨간불", "적색불", "빨강", "빨간", "적색", "red"))
    has_green = any(token in normalized for token in ("초록불", "녹색불", "초록", "녹색", "green"))
    has_blue = any(token in normalized for token in ("파란불", "청색불", "파랑", "파란", "청색", "blue"))
    has_blink = any(token in normalized for token in ("깜빡", "깜빡이", "blink"))

    signals: list[str] = []
    if has_green:
        signals.append("green")
    if has_red:
        signals.append("red")
    if has_blue:
        signals.append("blue")
    if has_blink:
        signals.append("blink")

    if has_red and has_green and has_blink:
        return {
            "status": "warning",
            "confidence": "high",
            "conclusion": "설명한 패턴은 `warning` 상태로 보는 게 맞아",
            "reason": "초록/빨강 깜빡 표현이 있고 warning 매핑은 `LC:FBL:R:G:` 기준이야",
            "guide": "영상 품질 이상, 녹화 정체, 비디오 길이 불일치 쪽을 먼저 확인해",
            "signals": signals,
            "relatedStates": ("warning", "error", "ready"),
        }
    if has_red and has_blue and has_blink:
        return {
            "status": "mixed",
            "confidence": "medium",
            "conclusion": "빨강/파랑 점멸 설명만으로는 `error`와 `paused` 해석이 섞일 수 있어",
            "reason": "빨강 계열은 error, 파랑 점멸은 paused 쪽과 겹쳐서 현장 표현만으론 단정이 어려워",
            "guide": "입력 없음, 단색 화면, 화면 이상 같은 에러 징후가 같이 있었는지 먼저 확인해",
            "signals": signals,
            "relatedStates": ("error", "paused", "warning"),
        }
    if has_blue and has_blink:
        return {
            "status": "paused",
            "confidence": "medium",
            "conclusion": "파란 점멸이면 `paused` 상태로 먼저 보는 게 맞아",
            "reason": "paused 매핑은 `LC:BL:B:` 기준이야",
            "guide": "일시정지 직전 조작이나 재개 동작이 있었는지 확인해",
            "signals": signals,
            "relatedStates": ("paused", "motion"),
        }
    return {
        "status": "",
        "confidence": "low",
        "conclusion": "질문만으로 특정 LED 상태를 단정하긴 어려워",
        "reason": "색상, 점멸 방식, 반복 여부가 더 있어야 정확히 매핑할 수 있어",
        "guide": "초록/빨강/파랑, 점등/점멸, 반복 여부를 같이 받아서 매핑해",
        "signals": signals,
        "relatedStates": ("ready", "motion", "recording", "warning", "error", "busy"),
    }


def _build_led_pattern_help_evidence(question: str) -> dict[str, Any]:
    interpretation = _infer_led_pattern_help(question)
    return {
        "route": "device_led_pattern_guide",
        "source": "device_led_spec",
        "request": {
            "question": question,
        },
        "patternInterpretation": interpretation,
        "ledSpec": [dict(item) for item in _LED_STATE_SPECS],
        "notes": {
            "networkOfflineLedMapped": False,
            "networkOfflineHandling": "네트워크 오프라인은 현재 LED 변경이 아니라 internet 음성 안내로 처리돼",
        },
    }


def _build_led_pattern_help_reply(question: str) -> str:
    evidence = _build_led_pattern_help_evidence(question)
    interpretation = evidence.get("patternInterpretation") if isinstance(evidence, dict) else {}
    if not isinstance(interpretation, dict):
        interpretation = {}

    reference_states = []
    related_state_names = {
        str(value).strip()
        for value in (interpretation.get("relatedStates") or ())
        if str(value).strip()
    }
    for item in _LED_STATE_SPECS:
        state_name = str(item.get("state") or "").strip()
        if not state_name:
            continue
        if related_state_names and state_name not in related_state_names:
            continue
        reference_states.append(f"`{state_name}={item.get('command')}` {item.get('meaning')}")
        if len(reference_states) >= 3:
            break
    if not reference_states:
        reference_states = [
            f"`warning=LC:FBL:R:G:` {_LED_STATE_SPECS[4]['meaning']}",
            f"`error=LC:FBL:R:B:` {_LED_STATE_SPECS[5]['meaning']}",
            f"`busy=LC:3C:` {_LED_STATE_SPECS[6]['meaning']}",
        ]

    lines = [
        "*LED 증상 안내*",
        f"• 결론: {str(interpretation.get('conclusion') or '').strip()}",
        (
            "• 근거: "
            f"{str(interpretation.get('reason') or '').strip()} / "
            "네트워크 오프라인은 LED 매핑이 아니라 `internet` 음성 안내야"
        ),
        f"• 참고 상태: {' / '.join(reference_states)}",
        f"• 안내: {str(interpretation.get('guide') or '').strip()}",
    ]
    return "\n".join(lines)


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


def _build_device_memory_patch_config_message() -> str:
    return (
        "장비 메모리 패치 설정이 부족해. "
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


def _display_device_memory_patch_reason(reason: str | None) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized == "pm2_missing":
        return "장비에서 pm2 명령을 찾지 못했어"
    if normalized.startswith("ssh_exit_"):
        return f"메모리 패치 명령 실패 ({normalized})"
    return _display_device_status_probe_reason(reason)


def _run_remote_ssh_command(
    client: Any,
    *,
    command: str,
    summary: str,
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
            "summary": _display_value(summary, default=""),
            "command": normalized_command,
            "ok": exit_status == 0,
            "exitStatus": exit_status,
            "output": combined,
            "reason": "" if exit_status == 0 else f"ssh_exit_{exit_status}",
        }
    except Exception as exc:  # pragma: no cover - network/remote dependent
        return {
            "summary": _display_value(summary, default=""),
            "command": normalized_command,
            "ok": False,
            "exitStatus": None,
            "output": "",
            "reason": type(exc).__name__.lower(),
        }


def _run_status_probe_command(client: Any, key: str) -> dict[str, Any]:
    spec = _PROBE_COMMAND_SPECS[key]
    result = _run_remote_ssh_command(
        client,
        command=str(spec.get("command") or "").strip(),
        summary=_display_value(spec.get("summary"), default=""),
        timeout_sec=max(1, int(spec.get("timeout_sec") or cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10)),
    )
    return {
        "key": key,
        **result,
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


def _parse_pm2_memory_restart_values(text: str) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == "pm2_missing":
        return {
            "available": False,
            "reason": "pm2_missing",
            "values": [],
            "display": "",
            "hasExpectedLimit": False,
        }

    raw_values = [int(match) for match in _MEMORY_PATCH_VALUE_PATTERN.findall(normalized)]
    values: list[int] = []
    for value in raw_values:
        if value not in values:
            values.append(value)

    return {
        "available": True,
        "reason": "ok" if values else "value_missing",
        "values": values,
        "display": ", ".join(f"{value} ({_format_size(value)})" for value in values),
        "hasExpectedLimit": bool(values) and all(value >= _MEMORY_PATCH_EXPECTED_BYTES for value in values),
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


def _format_probe_ssh_status_display(ready: bool) -> str:
    return "🔵 *연결 가능*" if ready else "🔴 *연결 불가*"


def _format_probe_download_availability_display(ready: bool) -> str:
    return "🔵 *가능*" if ready else "🔴 *불가*"


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


def _compact_probe_output(text: str, *, max_chars: int = 280) -> str:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    return _truncate_text(" / ".join(lines), max_chars)


def _summarize_device_memory_patch(
    *,
    precheck: dict[str, Any],
    execution: dict[str, Any],
    verification: dict[str, Any],
) -> dict[str, Any]:
    if not precheck.get("ok"):
        return {
            "status": "fail",
            "label": "실패",
            "summary": _display_device_memory_patch_reason(precheck.get("reason")),
            "action": "장비 SSH 상태와 PM2 접근 상태를 확인하고 다시 시도해",
        }
    if precheck.get("reason") == "pm2_missing":
        return {
            "status": "fail",
            "label": "실패",
            "summary": _display_device_memory_patch_reason(precheck.get("reason")),
            "action": "장비에서 PM2 설치 상태와 PATH를 먼저 확인해",
        }
    if precheck.get("hasExpectedLimit"):
        return {
            "status": "pass",
            "label": "정상",
            "summary": "이미 4GB 메모리 설정이라 메모리 패치를 생략했어",
            "action": "추가 조치 필요 없어",
        }

    if not execution.get("ok"):
        return {
            "status": "fail",
            "label": "실패",
            "summary": _display_device_memory_patch_reason(execution.get("reason")),
            "action": "장비 경로, PM2 상태, 권한을 확인하고 다시 시도해",
        }
    if verification.get("hasExpectedLimit"):
        return {
            "status": "pass",
            "label": "완료",
            "summary": "CS에서 말하는 메모리 패치를 적용했고 4GB 설정으로 확인됐어",
            "action": "재부팅 이후에도 유지되도록 pm2 save까지 끝냈어",
        }
    if verification.get("available") and verification.get("values"):
        return {
            "status": "warning",
            "label": "확인 필요",
            "summary": "명령은 끝났지만 4GB 메모리 설정으로 보이지 않아",
            "action": "PM2 ecosystem 설정과 prettylist 결과를 다시 확인해",
        }
    return {
        "status": "warning",
        "label": "확인 필요",
        "summary": "명령은 끝났지만 max_memory_restart 값을 확인하지 못했어",
        "action": "장비에서 `pm2 prettylist | grep max_memory_restart` 결과를 다시 확인해",
    }


def _render_device_memory_patch_result(
    *,
    device_name: str,
    device_info: dict[str, Any],
    ssh_ready: bool,
    ssh_reason: str,
    precheck: dict[str, Any] | None,
    execution: dict[str, Any] | None,
    verification: dict[str, Any] | None,
) -> str:
    lines = _build_device_header_lines(
        title="*장비 메모리 패치*",
        device_name=device_name,
        device_info=device_info,
    )
    if not ssh_ready:
        lines.append("• 판정: *실행 불가*")
        lines.append(f"• 안내: {_display_device_memory_patch_reason(ssh_reason)}")
        return "\n".join(lines)

    precheck_payload = precheck or {}
    execution_payload = execution or {}
    verification_payload = verification or {}
    summary = _summarize_device_memory_patch(
        precheck=precheck_payload,
        execution=execution_payload,
        verification=verification_payload,
    )

    lines.append(f"• 판정: *{_display_value(summary.get('label'), default='확인 필요')}*")

    precheck_display = _display_value(precheck_payload.get("display"), default="")
    if precheck_display:
        lines.append(f"• 사전 확인: `max_memory_restart={precheck_display}`")
    elif precheck_payload.get("reason") == "pm2_missing":
        lines.append("• 사전 확인: `pm2` 명령을 찾지 못했어")
    elif not precheck_payload.get("ok"):
        lines.append(
            f"• 사전 확인: {_display_device_memory_patch_reason(precheck_payload.get('reason'))}"
        )
    else:
        lines.append("• 사전 확인: `max_memory_restart` 값을 읽지 못했어")

    if execution_payload:
        lines.append("• 실행: `mommybox-v2` PM2 재등록 후 `pm2 save`")
    else:
        lines.append("• 실행: 이미 정상이라 생략")

    if execution_payload:
        verification_display = _display_value(verification_payload.get("display"), default="")
        if verification_display:
            lines.append(f"• 실행 후 확인: `max_memory_restart={verification_display}`")
        elif verification_payload.get("reason") == "pm2_missing":
            lines.append("• 실행 후 확인: `pm2` 명령을 찾지 못해 `max_memory_restart`를 읽지 못했어")
        else:
            lines.append("• 실행 후 확인: `max_memory_restart` 값을 읽지 못했어")

    lines.append(f"• 안내: {_display_value(summary.get('summary'), default='확인 필요')}")
    action = _display_value(summary.get("action"), default="")
    if action:
        lines.append(f"• 조치: {action}")

    if not precheck_payload.get("ok"):
        precheck_output = _compact_probe_output(_display_value(precheck_payload.get("output"), default=""))
        if precheck_output:
            lines.append(f"• 로그: `{precheck_output}`")
    elif execution_payload and not execution_payload.get("ok"):
        failure_output = _compact_probe_output(_display_value(execution_payload.get("output"), default=""))
        if failure_output:
            lines.append(f"• 로그: `{failure_output}`")
    elif execution_payload and verification_payload.get("available") and not verification_payload.get("hasExpectedLimit"):
        verification_output = _compact_probe_output(_display_value(verification_payload.get("output"), default=""))
        if verification_output:
            lines.append(f"• 참고: `{verification_output}`")

    return _truncate_text("\n".join(lines), 38000)


def _build_memory_patch_check_payload(command_result: dict[str, Any]) -> dict[str, Any]:
    output = _display_value(command_result.get("output"), default="")
    if not command_result.get("ok"):
        return {
            "available": False,
            "reason": _display_value(command_result.get("reason"), default="check_failed"),
            "values": [],
            "display": "",
            "hasExpectedLimit": False,
            "output": output,
            "ok": False,
            "exitStatus": command_result.get("exitStatus"),
        }

    parsed = _parse_pm2_memory_restart_values(output)
    parsed["output"] = output
    parsed["ok"] = True
    parsed["exitStatus"] = command_result.get("exitStatus")
    return parsed


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
        lines.append(f"• SSH 연결 상태: {_format_probe_ssh_status_display(False)}")
        lines.append(f"• 초음파 영상 다운로드 가능 상태: {_format_probe_download_availability_display(False)}")
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
    lines.append(f"• SSH 연결 상태: {_format_probe_ssh_status_display(ssh_ready)}")
    lines.append(f"• 초음파 영상 다운로드 가능 상태: {_format_probe_download_availability_display(ssh_ready)}")
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


def _patch_device_pm2_memory(device_name: str) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 메모리 패치`")

    evidence_payload, device_info = _build_runtime_probe_payload(
        device_name=normalized_device_name,
        component="pm2_memory_patch",
    )
    ssh = evidence_payload.get("ssh") if isinstance(evidence_payload.get("ssh"), dict) else {}
    ssh_ready = bool(ssh.get("ready"))
    ssh_reason = _display_value(ssh.get("reason"), default="")

    if not ssh_ready:
        result_text = _render_device_memory_patch_result(
            device_name=normalized_device_name,
            device_info=device_info,
            ssh_ready=False,
            ssh_reason=ssh_reason,
            precheck=None,
            execution=None,
            verification=None,
        )
        return result_text, evidence_payload

    host = _display_value(ssh.get("host"), default="")
    try:
        port = int(ssh.get("port") or 0)
    except (TypeError, ValueError):
        port = 0

    connection = _connect_device_ssh_client(host, port)
    if not connection.get("ok"):
        ssh_reason = _display_value(connection.get("reason"), default="ssh_connect_failed")
        evidence_payload["ssh"] = {
            **ssh,
            "ready": False,
            "reason": ssh_reason,
        }
        result_text = _render_device_memory_patch_result(
            device_name=normalized_device_name,
            device_info=device_info,
            ssh_ready=False,
            ssh_reason=ssh_reason,
            precheck=None,
            execution=None,
            verification=None,
        )
        return result_text, evidence_payload

    client = connection["client"]
    try:
        precheck_command = _run_remote_ssh_command(
            client,
            command=_MEMORY_PATCH_VERIFY_COMMAND,
            summary="사전 max_memory_restart 확인",
            timeout_sec=max(10, int(cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10)),
        )
        precheck = _build_memory_patch_check_payload(precheck_command)

        execution: dict[str, Any] | None = None
        verification: dict[str, Any] | None = None
        if precheck.get("ok") and precheck.get("reason") != "pm2_missing" and not precheck.get("hasExpectedLimit"):
            execution = _run_remote_ssh_command(
                client,
                command=_MEMORY_PATCH_EXECUTION_COMMAND,
                summary="메모리 패치 실행",
                timeout_sec=max(30, int(cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10)),
            )
            if execution.get("ok"):
                verification_command = _run_remote_ssh_command(
                    client,
                    command=_MEMORY_PATCH_VERIFY_COMMAND,
                    summary="실행 후 max_memory_restart 확인",
                    timeout_sec=max(10, int(cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10)),
                )
                verification = _build_memory_patch_check_payload(verification_command)
    finally:
        client.close()

    evidence_payload["precheck"] = precheck
    evidence_payload["execution"] = execution
    evidence_payload["verification"] = verification

    result_text = _render_device_memory_patch_result(
        device_name=normalized_device_name,
        device_info=device_info,
        ssh_ready=True,
        ssh_reason="ready",
        precheck=precheck,
        execution=execution,
        verification=verification,
    )
    return result_text, evidence_payload
