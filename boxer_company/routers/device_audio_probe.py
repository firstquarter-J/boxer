import re
from typing import Any

from boxer.core.utils import _display_value, _truncate_text
from boxer_company import settings as cs
from boxer_company.routers.barcode_log import _extract_device_name_scope
from boxer_company.routers.device_file_probe import _connect_device_ssh_client
from boxer_company.routers.mda_graphql import _wait_for_mda_device_agent_ssh

_DEVICE_AUDIO_HINTS = (
    "소리",
    "오디오",
    "사운드",
    "스피커",
    "음량",
    "볼륨",
    "mute",
    "muted",
)
_DEVICE_AUDIO_PROBE_HINTS = (
    "점검",
    "체크",
    "확인",
    "테스트",
    "진단",
    "출력",
    "재생",
    "무음",
    "안나와",
    "안 나와",
    "안들려",
    "안 들려",
    "문제",
)
_LEADING_DEVICE_AUDIO_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)
_PLAYBACK_DEVICE_PATTERN = re.compile(
    r"card\s+(?P<card>\d+):\s*(?P<card_label>[^\[]+)\[(?P<card_name>[^\]]+)\],\s*"
    r"device\s+(?P<device>\d+):\s*(?P<device_label>[^\[]+)\[(?P<device_name>[^\]]+)\]",
    re.IGNORECASE,
)
_PERCENT_PATTERN = re.compile(r"\[(\d+)%\]")
_SWITCH_PATTERN = re.compile(r"\[(on|off)\]", re.IGNORECASE)
_DEFAULT_SINK_PATTERN = re.compile(r"^Default Sink:\s*(.+)$", re.IGNORECASE | re.MULTILINE)

_AUDIO_PROBE_COMMANDS = (
    {
        "key": "tools",
        "summary": "오디오 도구 확인",
        "timeout_sec": 10,
        "command": (
            "for t in aplay amixer pactl speaker-test; do "
            "printf '%s=' \"$t\"; command -v \"$t\" || true; echo; "
            "done"
        ),
    },
    {
        "key": "playback_devices",
        "summary": "재생 장치 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v aplay >/dev/null 2>&1; then "
            "aplay -l 2>&1; "
            "else echo aplay_missing; fi'"
        ),
    },
    {
        "key": "master_mixer",
        "summary": "Master 볼륨 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v amixer >/dev/null 2>&1; then "
            "amixer sget Master 2>&1; "
            "else echo amixer_missing; fi'"
        ),
    },
    {
        "key": "pcm_mixer",
        "summary": "PCM 볼륨 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v amixer >/dev/null 2>&1; then "
            "amixer sget PCM 2>&1; "
            "else echo amixer_missing; fi'"
        ),
    },
    {
        "key": "pactl_info",
        "summary": "기본 sink 확인",
        "timeout_sec": 10,
        "command": (
            "sh -lc 'if command -v pactl >/dev/null 2>&1; then "
            "pactl info 2>&1; "
            "else echo pactl_missing; fi'"
        ),
    },
    {
        "key": "playback_test",
        "summary": "짧은 소리 출력 테스트",
        "timeout_sec": 15,
        "command": (
            "sh -lc 'if command -v speaker-test >/dev/null 2>&1; then "
            "speaker-test -D default -c 2 -t sine -f 440 -l 1 2>&1; "
            "elif command -v aplay >/dev/null 2>&1 && [ -f /usr/share/sounds/alsa/Front_Center.wav ]; then "
            "aplay -D default /usr/share/sounds/alsa/Front_Center.wav 2>&1; "
            "else echo playback_test_unavailable; fi'"
        ),
    },
)


def _normalize_device_audio_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _has_device_audio_hint(text: str) -> bool:
    normalized = str(text or "").strip()
    lowered = normalized.lower()
    has_audio = any(token in normalized or token in lowered for token in _DEVICE_AUDIO_HINTS)
    has_probe = any(token in normalized or token in lowered for token in _DEVICE_AUDIO_PROBE_HINTS)
    return bool(has_audio and has_probe)


def _extract_device_name_for_audio_probe(question: str) -> str | None:
    normalized = _normalize_device_audio_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _has_device_audio_hint(normalized):
        return extracted

    matched = _LEADING_DEVICE_AUDIO_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _has_device_audio_hint(remainder):
        return None
    return candidate


def _is_device_audio_probe_request(question: str, device_name: str | None = None) -> bool:
    normalized = _normalize_device_audio_question(question)
    resolved_device_name = str(device_name or _extract_device_name_for_audio_probe(normalized) or "").strip()
    if not resolved_device_name:
        return False
    return _has_device_audio_hint(normalized)


def _build_device_audio_probe_config_message() -> str:
    return (
        "장비 소리 출력 점검 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD, DEVICE_SSH_PASSWORD가 필요해"
    )


def _display_device_audio_probe_reason(reason: str | None) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized in {"agent_ssh_not_ready", "novalidconnectionserror", "timeout", "oerror"}:
        return "장비 SSH 연결 준비 실패. 온라인 상태, 네트워크, 원격 접속 상태 먼저 확인해"
    if normalized == "ssh_auth_failed":
        return "장비 SSH 인증 실패"
    if normalized == "missing_device_name":
        return "장비명이 없어 장비 소리 점검 불가"
    if normalized == "missing_password":
        return "DEVICE_SSH_PASSWORD 설정이 없어 장비 소리 점검 불가"
    if normalized == "paramiko_missing":
        return "paramiko 설치가 없어 장비 소리 점검 불가"
    if normalized.startswith("ssh_exit_"):
        return f"장비 소리 점검 명령 실패 ({normalized})"
    if not normalized:
        return "장비 소리 점검 실패"
    return normalized


def _truncate_probe_output(text: str, *, max_lines: int = 12, max_chars: int = 1200) -> str:
    lines = [line.rstrip() for line in str(text or "").splitlines() if line.strip()]
    if max_lines > 0:
        lines = lines[:max_lines]
    return _truncate_text("\n".join(lines), max_chars)


def _run_audio_probe_command(client: Any, spec: dict[str, Any]) -> dict[str, Any]:
    command = str(spec.get("command") or "").strip()
    timeout_sec = max(
        1,
        int(spec.get("timeout_sec") or cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC or 10),
    )
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
            "key": _display_value(spec.get("key"), default="unknown"),
            "summary": _display_value(spec.get("summary"), default=""),
            "ok": exit_status == 0,
            "exitStatus": exit_status,
            "output": combined,
            "outputExcerpt": _truncate_probe_output(combined),
            "reason": "" if exit_status == 0 else f"ssh_exit_{exit_status}",
        }
    except Exception as exc:  # pragma: no cover - network/remote dependent
        return {
            "key": _display_value(spec.get("key"), default="unknown"),
            "summary": _display_value(spec.get("summary"), default=""),
            "ok": False,
            "exitStatus": None,
            "output": "",
            "outputExcerpt": "",
            "reason": type(exc).__name__.lower(),
        }


def _parse_tool_paths(text: str) -> dict[str, str]:
    tool_paths: dict[str, str] = {}
    for line in str(text or "").splitlines():
        if "=" not in line:
            continue
        name, _, raw_path = line.partition("=")
        normalized_name = name.strip()
        if not normalized_name:
            continue
        tool_paths[normalized_name] = raw_path.strip()
    return tool_paths


def _parse_playback_devices(text: str) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == "aplay_missing":
        return {
            "available": False,
            "reason": "aplay_missing",
            "deviceCount": 0,
            "devices": [],
        }

    devices: list[dict[str, Any]] = []
    for line in normalized.splitlines():
        matched = _PLAYBACK_DEVICE_PATTERN.search(line)
        if not matched:
            continue
        devices.append(
            {
                "card": int(matched.group("card")),
                "cardLabel": matched.group("card_label").strip(),
                "cardName": matched.group("card_name").strip(),
                "device": int(matched.group("device")),
                "deviceLabel": matched.group("device_label").strip(),
                "deviceName": matched.group("device_name").strip(),
            }
        )

    return {
        "available": True,
        "reason": "ok",
        "deviceCount": len(devices),
        "devices": devices,
    }


def _parse_mixer_control(text: str, *, control_name: str) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == "amixer_missing":
        return {
            "controlName": control_name,
            "available": False,
            "reason": "amixer_missing",
            "percent": None,
            "switch": "",
        }

    if "Unable to find simple control" in normalized:
        return {
            "controlName": control_name,
            "available": False,
            "reason": "control_missing",
            "percent": None,
            "switch": "",
        }

    percents = [int(value) for value in _PERCENT_PATTERN.findall(normalized)]
    switches = [value.lower() for value in _SWITCH_PATTERN.findall(normalized)]
    return {
        "controlName": control_name,
        "available": True,
        "reason": "ok",
        "percent": max(percents) if percents else None,
        "switch": switches[-1] if switches else "",
    }


def _parse_default_sink(text: str) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == "pactl_missing":
        return {
            "available": False,
            "reason": "pactl_missing",
            "defaultSink": "",
        }

    matched = _DEFAULT_SINK_PATTERN.search(normalized)
    return {
        "available": bool(matched),
        "reason": "ok" if matched else "not_reported",
        "defaultSink": str(matched.group(1) or "").strip() if matched else "",
    }


def _parse_playback_test(text: str, exit_status: int | None) -> dict[str, Any]:
    normalized = str(text or "").strip()
    if normalized == "playback_test_unavailable":
        return {
            "available": False,
            "ok": False,
            "reason": "playback_test_unavailable",
            "exitStatus": exit_status,
            "usedCommand": "none",
        }

    used_command = "speaker-test" if "speaker-test" in normalized else "aplay" if "Playing WAVE" in normalized else "unknown"
    ok = bool(
        exit_status == 0
        and (
            "Playback device is default" in normalized
            or "Time per period" in normalized
            or "Playing WAVE" in normalized
        )
    )
    return {
        "available": True,
        "ok": ok,
        "reason": "ok" if ok else "playback_test_failed",
        "exitStatus": exit_status,
        "usedCommand": used_command,
    }


def _build_mixer_summary(master: dict[str, Any], pcm: dict[str, Any]) -> dict[str, Any]:
    muted = False
    if master.get("available"):
        if master.get("switch") == "off":
            muted = True
        elif master.get("percent") == 0:
            muted = True
    elif pcm.get("available") and pcm.get("percent") == 0:
        muted = True

    parts: list[str] = []
    if master.get("available"):
        master_percent = master.get("percent")
        master_switch = master.get("switch") or "unknown"
        if master_percent is not None:
            parts.append(f"Master {master_percent}% {master_switch}")
        else:
            parts.append(f"Master {master_switch}")
    if pcm.get("available"):
        pcm_percent = pcm.get("percent")
        if pcm_percent is not None:
            parts.append(f"PCM {pcm_percent}%")
    return {
        "muted": muted,
        "summary": ", ".join(parts),
    }


def _summarize_device_audio_probe(
    *,
    tool_paths: dict[str, str],
    playback_devices: dict[str, Any],
    master_mixer: dict[str, Any],
    pcm_mixer: dict[str, Any],
    default_sink: dict[str, Any],
    playback_test: dict[str, Any],
) -> dict[str, Any]:
    available_tools = [
        name
        for name in ("aplay", "amixer", "speaker-test", "pactl")
        if _display_value(tool_paths.get(name), default="")
    ]
    mixer_summary = _build_mixer_summary(master_mixer, pcm_mixer)
    device_count = int(playback_devices.get("deviceCount") or 0)

    status = "check_needed"
    summary = "원격 점검 결과만으로는 확정이 어려워"
    recommended_action = "연결된 스피커 전원, 케이블, 입력 소스부터 확인해"
    if device_count <= 0:
        status = "fail"
        summary = "오디오 재생 장치를 찾지 못해서 OS 기준 출력 경로 이상을 의심해"
        recommended_action = "박스 오디오 장치 인식, 드라이버, 출력 경로를 먼저 점검해"
    elif mixer_summary.get("muted"):
        status = "warning"
        summary = "Master 또는 PCM 음소거/볼륨 설정 이상을 의심해"
        recommended_action = "장비 음량과 mute 설정부터 풀고 다시 재생 테스트해"
    elif playback_test.get("available") and playback_test.get("ok"):
        status = "pass"
        summary = "OS 기준 소리 출력 경로는 정상으로 보여"
        recommended_action = "장비는 정상으로 보여서 연결된 스피커 전원, 케이블, 입력 소스를 먼저 점검해"
    elif playback_test.get("available") and not playback_test.get("ok"):
        status = "fail"
        summary = "짧은 소리 출력 테스트가 실패해서 출력 경로 문제를 의심해"
        recommended_action = "장비 오디오 출력 경로와 재생 장치 설정을 먼저 점검해"
    elif device_count > 0 and available_tools:
        status = "warning"
        summary = "오디오 장치와 볼륨은 보이지만 실제 재생 테스트는 확정하지 못했어"
        recommended_action = "연결된 스피커 점검과 함께 장비에서 재생 테스트를 한 번 더 확인해"

    limitations = [
        "원격 명령만으로는 물리 스피커 고장이나 케이블 접촉 불량까지 확정 못 해",
    ]
    if not playback_test.get("available"):
        limitations.append("재생 테스트 도구가 없어 실제 음 출력 확인은 제한적이야")
    if not default_sink.get("available"):
        limitations.append("PulseAudio sink 정보는 확인되지 않았어")

    return {
        "status": status,
        "summary": summary,
        "recommendedAction": recommended_action,
        "availableTools": available_tools,
        "mixerMuted": bool(mixer_summary.get("muted")),
        "mixerSummary": _display_value(mixer_summary.get("summary"), default=""),
        "limitations": limitations,
    }


def _render_device_audio_probe_result(
    *,
    device_name: str,
    device_info: dict[str, Any] | None,
    ssh_ready: bool,
    ssh_reason: str,
    checks: list[dict[str, Any]],
    summary: dict[str, Any] | None,
    playback_devices: dict[str, Any] | None,
    master_mixer: dict[str, Any] | None,
    pcm_mixer: dict[str, Any] | None,
    default_sink: dict[str, Any] | None,
    playback_test: dict[str, Any] | None,
) -> str:
    info = device_info or {}
    hospital_name = _display_value(info.get("hospitalName"), default="")
    room_name = _display_value(info.get("roomName"), default="")
    version = _display_value(info.get("version"), default="")
    lines = [
        "*장비 소리 출력 점검*",
    ]
    device_line = f"• 장비: `{device_name}`"
    if version:
        device_line = f"{device_line} | 버전: `{version}`"
    lines.append(device_line)
    if hospital_name or room_name:
        location_parts = []
        if hospital_name:
            location_parts.append(f"`{hospital_name}`")
        if room_name:
            location_parts.append(f"`{room_name}`")
        lines.append(f"• 위치: {' / '.join(location_parts)}")

    if not ssh_ready:
        lines.append("• 판정: *점검 불가*")
        lines.append(f"• 안내: {_display_device_audio_probe_reason(ssh_reason)}")
        lines.append("• 조치: 장비 온라인 상태, 네트워크, 원격 접속 상태 먼저 확인해")
        return "\n".join(lines)

    summary_payload = summary or {}
    playback_payload = playback_devices or {}
    master_payload = master_mixer or {}
    pcm_payload = pcm_mixer or {}
    sink_payload = default_sink or {}
    test_payload = playback_test or {}

    device_items = playback_payload.get("devices") if isinstance(playback_payload, dict) else []
    status = _display_value(summary_payload.get("status"), default="")
    status_label = "확인 필요"
    if status == "pass":
        status_label = "정상"
    elif status == "fail":
        status_label = "이상"
    elif status == "warning":
        status_label = "확인 필요"
    lines.append(f"• 판정: *{status_label}*")

    evidence_parts: list[str] = []
    if device_items:
        evidence_parts.append(f"재생 장치 {len(device_items)}개")
    else:
        evidence_parts.append("재생 장치 0개")

    if master_payload.get("available"):
        percent = master_payload.get("percent")
        switch = _display_value(master_payload.get("switch"), default="")
        if percent is not None and switch:
            evidence_parts.append(f"Master {percent}% {switch}")
        elif percent is not None:
            evidence_parts.append(f"Master {percent}%")
    if pcm_payload.get("available"):
        pcm_percent = pcm_payload.get("percent")
        if pcm_percent is not None:
            evidence_parts.append(f"PCM {pcm_percent}%")

    if test_payload.get("available"):
        test_state = "성공" if test_payload.get("ok") else "실패"
        test_command = _display_value(test_payload.get("usedCommand"), default="unknown")
        evidence_parts.append(f"{test_command} {test_state}")
    elif sink_payload.get("reason") == "pactl_missing":
        evidence_parts.append("pactl 미설치")

    lines.append(f"• 근거: {' / '.join(evidence_parts)}")

    conclusion = _display_value(summary_payload.get("summary"), default="확인 필요")
    recommended_action = _display_value(summary_payload.get("recommendedAction"), default="")
    if sink_payload.get("reason") == "pactl_missing" and "pactl 미설치" not in conclusion:
        conclusion = f"{conclusion}. pactl 미설치는 실패 원인으로 보지 않아"
    if recommended_action:
        lines.append(f"• 안내: {conclusion}. {recommended_action}")
    else:
        lines.append(f"• 안내: {conclusion}")
    limitations = summary_payload.get("limitations") if isinstance(summary_payload, dict) else []
    if isinstance(limitations, list) and limitations:
        lines.append(f"• 참고: {_display_value(limitations[0], default='')}")

    failed_checks = [
        item
        for item in checks
        if isinstance(item, dict) and not item.get("ok") and item.get("reason")
    ]
    if failed_checks and not test_payload.get("ok"):
        failed = failed_checks[0]
        lines.append(
            f"• 실패 명령: `{_display_value(failed.get('summary'), default='미확인')}`"
        )

    return _truncate_text("\n".join(lines), 38000)


def _probe_device_audio_output(device_name: str) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 장비 소리 출력 점검`")

    wait_result = _wait_for_mda_device_agent_ssh(normalized_device_name)
    device_info = wait_result.get("device") if isinstance(wait_result.get("device"), dict) else {}
    agent_ssh = device_info.get("agentSsh") if isinstance(device_info.get("agentSsh"), dict) else {}
    host = _display_value(agent_ssh.get("host"), default="")
    port = agent_ssh.get("port")
    try:
        port = int(port)
    except (TypeError, ValueError):
        port = 0

    evidence_payload: dict[str, Any] = {
        "route": "device_audio_probe",
        "source": "mda_graphql+ssh",
        "request": {
            "deviceName": normalized_device_name,
        },
        "device": {
            "deviceName": _display_value(device_info.get("deviceName"), default=normalized_device_name),
            "version": _display_value(device_info.get("version"), default=""),
            "captureBoardType": _display_value(device_info.get("captureBoardType"), default=""),
            "hospitalName": _display_value(device_info.get("hospitalName"), default=""),
            "roomName": _display_value(device_info.get("roomName"), default=""),
            "isConnected": bool(device_info.get("isConnected")),
        },
        "ssh": {
            "ready": bool(wait_result.get("ready")) and bool(host) and port > 0,
            "reason": "ready" if bool(wait_result.get("ready")) and bool(host) and port > 0 else "agent_ssh_not_ready",
            "pollCount": wait_result.get("pollCount"),
            "reusedExisting": bool(wait_result.get("reusedExisting")),
        },
    }

    if not evidence_payload["ssh"]["ready"]:
        result_text = _render_device_audio_probe_result(
            device_name=normalized_device_name,
            device_info=device_info,
            ssh_ready=False,
            ssh_reason=str(evidence_payload["ssh"]["reason"]),
            checks=[],
            summary=None,
            playback_devices=None,
            master_mixer=None,
            pcm_mixer=None,
            default_sink=None,
            playback_test=None,
        )
        return result_text, evidence_payload

    connection = _connect_device_ssh_client(host, int(port))
    if not connection.get("ok"):
        ssh_reason = _display_value(connection.get("reason"), default="ssh_connect_failed")
        evidence_payload["ssh"] = {
            **evidence_payload["ssh"],
            "ready": False,
            "reason": ssh_reason,
        }
        result_text = _render_device_audio_probe_result(
            device_name=normalized_device_name,
            device_info=device_info,
            ssh_ready=False,
            ssh_reason=ssh_reason,
            checks=[],
            summary=None,
            playback_devices=None,
            master_mixer=None,
            pcm_mixer=None,
            default_sink=None,
            playback_test=None,
        )
        return result_text, evidence_payload

    client = connection["client"]
    try:
        checks = [_run_audio_probe_command(client, spec) for spec in _AUDIO_PROBE_COMMANDS]
    finally:
        client.close()

    by_key = {
        _display_value(item.get("key"), default=""): item
        for item in checks
        if isinstance(item, dict)
    }

    tool_paths = _parse_tool_paths(_display_value((by_key.get("tools") or {}).get("output"), default=""))
    playback_devices = _parse_playback_devices(
        _display_value((by_key.get("playback_devices") or {}).get("output"), default="")
    )
    master_mixer = _parse_mixer_control(
        _display_value((by_key.get("master_mixer") or {}).get("output"), default=""),
        control_name="Master",
    )
    pcm_mixer = _parse_mixer_control(
        _display_value((by_key.get("pcm_mixer") or {}).get("output"), default=""),
        control_name="PCM",
    )
    default_sink = _parse_default_sink(
        _display_value((by_key.get("pactl_info") or {}).get("output"), default="")
    )
    playback_test_result = by_key.get("playback_test") or {}
    playback_test = _parse_playback_test(
        _display_value(playback_test_result.get("output"), default=""),
        playback_test_result.get("exitStatus"),
    )
    summary = _summarize_device_audio_probe(
        tool_paths=tool_paths,
        playback_devices=playback_devices,
        master_mixer=master_mixer,
        pcm_mixer=pcm_mixer,
        default_sink=default_sink,
        playback_test=playback_test,
    )

    evidence_payload["audioProbe"] = {
        "toolPaths": tool_paths,
        "playbackDevices": playback_devices,
        "masterMixer": master_mixer,
        "pcmMixer": pcm_mixer,
        "defaultSink": default_sink,
        "playbackTest": playback_test,
        "summary": summary,
        "checks": [
            {
                "key": _display_value(item.get("key"), default=""),
                "summary": _display_value(item.get("summary"), default=""),
                "ok": bool(item.get("ok")),
                "exitStatus": item.get("exitStatus"),
                "reason": _display_value(item.get("reason"), default=""),
                "outputExcerpt": _display_value(item.get("outputExcerpt"), default=""),
            }
            for item in checks
        ],
    }

    result_text = _render_device_audio_probe_result(
        device_name=normalized_device_name,
        device_info=device_info,
        ssh_ready=True,
        ssh_reason="ready",
        checks=checks,
        summary=summary,
        playback_devices=playback_devices,
        master_mixer=master_mixer,
        pcm_mixer=pcm_mixer,
        default_sink=default_sink,
        playback_test=playback_test,
    )
    return result_text, evidence_payload
