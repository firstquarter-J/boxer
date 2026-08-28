from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from importlib import resources
import re
import secrets
import shlex
import threading
import time
from typing import Any, Callable

from boxer_company.utils import _display_value
from boxer_company import settings as cs
from boxer_company.routers.device_ssh_security import (
    _mark_company_api_mutation_attempted,
)
from boxer_company.routers.device_update import (
    _open_device_ssh_for_update,
    _run_remote_ssh_command,
)
from boxer_company.routers.mda_graphql import (
    _get_mda_device_detail,
    _send_mda_device_ping,
)
from boxer_company.routers.ssh_command import _close_ssh_streams
from boxer_company.transport_contracts import (
    DEVICE_SCANNER_ABI_PATCH_ROUTE,
    _is_device_scanner_abi_patch_intent,
)


_PATCH_ASSET_PACKAGE = "boxer_company.assets"
_PATCH_ASSET_NAME = "repair-node-hid-abi.sh"
_PATCH_ASSET_SHA256 = (
    "8c3e4e1faba16cc87b34a97f8b6453d1c64852f0a5844b08a8957f434cf1164c"
)
_DEVICE_NAME_PATTERN = re.compile(r"MB2-[A-Z0-9]+", re.IGNORECASE)
_CANONICAL_PATCH_PATTERN = re.compile(
    r"^(MB2-[A-Z0-9]+)\s+스캐너\s+패치[.!]?$",
    re.IGNORECASE,
)
_REMOTE_OUTPUT_LIMIT_BYTES = 65_536
_STATE_POLL_INTERVAL_SEC = 5.0
_GATE_STATE_POLL_INTERVAL_SEC = 1.0
_PING_REFRESH_TIMEOUT_SEC = 45.0
_ABORT_WAIT_TIMEOUT_SEC = 45.0
_DEVICE_PATCH_LOCK_STRIPES = tuple(threading.Lock() for _ in range(64))


class DeviceScannerAbiPatchError(RuntimeError):
    """사용자에게 노출 가능한 안전 문구와 감사용 실패 이유를 보존한다."""

    def __init__(self, user_message: str, fallback_reason: str) -> None:
        super().__init__(fallback_reason)
        self.user_message = str(user_message).strip()
        self.fallback_reason = str(fallback_reason).strip()


@dataclass(frozen=True, slots=True)
class _IdleAssessment:
    ok: bool
    reason: str
    status: str
    is_recording: bool | None
    is_uploading: bool | None
    updated_at: str
    updated_epoch: float | None


class _RemoteOutputTail:
    """출력은 계속 소비하되 결과 marker 확인용 마지막 64KiB만 보존한다."""

    def __init__(self, limit_bytes: int = _REMOTE_OUTPUT_LIMIT_BYTES) -> None:
        self._limit_bytes = max(1_024, int(limit_bytes))
        self._buffer = bytearray()
        self._lock = threading.Lock()

    def append(self, chunk: bytes) -> None:
        if not chunk:
            return
        with self._lock:
            self._buffer.extend(chunk)
            overflow = len(self._buffer) - self._limit_bytes
            if overflow > 0:
                del self._buffer[:overflow]

    def text(self) -> str:
        with self._lock:
            payload = bytes(self._buffer)
        return payload.decode("utf-8", errors="replace")


def _normalize_scanner_patch_intent_question(question: str) -> str:
    """실행 parser도 경량 intent와 같은 mention 제거 규칙을 쓴다."""

    text = re.sub(r"<@[^>]+>", " ", str(question or ""))
    return " ".join(text.split()).strip()


def _normalize_scanner_patch_exact_question(question: str) -> str:
    """실행 승인용으로 단 하나의 선행 bot mention만 허용한다.

    백틱·따옴표를 지우지 않아 예시나 인용문이 실제 장비
    mutation 명령으로 바뀌는 것을 막는다.
    """

    text = re.sub(
        r"^\s*<@[A-Z0-9]+>\s*",
        "",
        str(question or ""),
        count=1,
        flags=re.IGNORECASE,
    )
    return " ".join(text.split()).strip()


def _extract_device_name_for_scanner_abi_patch(
    question: str,
) -> str | None:
    if not _is_device_scanner_abi_patch_intent(question):
        return None
    matched = _DEVICE_NAME_PATTERN.search(
        _normalize_scanner_patch_intent_question(question)
    )
    return matched.group(0).upper() if matched else None


def _is_device_scanner_abi_patch_request(
    question: str,
    device_name: str | None = None,
) -> bool:
    matched = _CANONICAL_PATCH_PATTERN.fullmatch(
        _normalize_scanner_patch_exact_question(question)
    )
    if matched is None:
        return False
    target = matched.group(1).upper()
    normalized_device = str(device_name or target).strip().upper()
    return target == normalized_device


def _is_device_scanner_abi_patch_runtime_configured() -> bool:
    return bool(
        cs.DEVICE_SCANNER_ABI_PATCH_ENABLED
        and cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


def _build_device_scanner_abi_patch_config_message() -> str:
    if not cs.DEVICE_SCANNER_ABI_PATCH_ENABLED:
        return "스캐너 ABI 패치 기능이 아직 비활성화돼 있어"
    return (
        "스캐너 ABI 패치 실행 설정이 부족해. "
        "MDA와 장비 SSH 설정을 확인해줘"
    )


def _build_device_scanner_abi_patch_command_message() -> str:
    return (
        "정확한 단일 장비 적용 명령이 필요해. 예: "
        "`MB2-A00037 스캐너 패치`"
    )


def _parse_mda_datetime(value: Any) -> datetime | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(
            normalized[:-1] + "+00:00"
            if normalized.endswith("Z")
            else normalized
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _assess_device_idle(
    device_info: dict[str, Any] | None,
    *,
    expected_device: str,
    now: datetime | None = None,
    require_fresh: bool = True,
) -> _IdleAssessment:
    """누락·문자열 false·부분 장비 일치를 모두 유휴로 인정하지 않는다."""

    info = device_info if isinstance(device_info, dict) else {}
    status = _display_value(info.get("deviceStatus"), default="").upper()
    is_recording = info.get("isRecording")
    is_uploading = info.get("isUploading")
    updated_at = _display_value(info.get("deviceUpdatedAt"), default="")
    parsed_updated_at = _parse_mda_datetime(updated_at)
    reference_now = (now or datetime.now(timezone.utc)).astimezone(
        timezone.utc
    )
    updated_epoch = (
        parsed_updated_at.timestamp() if parsed_updated_at is not None else None
    )
    max_age_sec = max(
        1,
        int(cs.DEVICE_SCANNER_ABI_PATCH_STATE_MAX_AGE_SEC or 120),
    )

    reason = "ready"
    if _display_value(info.get("deviceName"), default="") != expected_device:
        reason = "device_identity_mismatch"
    elif info.get("isConnected") is not True:
        reason = "agent_disconnected"
    elif info.get("deviceIsConnected") is not True:
        reason = "device_disconnected"
    elif status != "NOSESS":
        reason = "device_not_standby"
    elif is_recording is not False:
        reason = "recording_state_unknown_or_active"
    elif is_uploading is not False:
        reason = "upload_state_unknown_or_active"
    elif require_fresh and parsed_updated_at is None:
        reason = "device_state_timestamp_invalid"
    elif require_fresh:
        age_sec = (reference_now - parsed_updated_at).total_seconds()
        if age_sec < -30 or age_sec > max_age_sec:
            reason = "device_state_stale"

    return _IdleAssessment(
        ok=reason == "ready",
        reason=reason,
        status=status,
        is_recording=(
            is_recording if isinstance(is_recording, bool) else None
        ),
        is_uploading=(
            is_uploading if isinstance(is_uploading, bool) else None
        ),
        updated_at=updated_at,
        updated_epoch=updated_epoch,
    )


def _build_idle_block_message(
    device_name: str,
    assessment: _IdleAssessment,
) -> str:
    status = assessment.status or "미확인"
    recording = (
        "아님"
        if assessment.is_recording is False
        else "진행 중"
        if assessment.is_recording is True
        else "미확인"
    )
    uploading = (
        "아님"
        if assessment.is_uploading is False
        else "진행 중"
        if assessment.is_uploading is True
        else "미확인"
    )
    return (
        "*스캐너 ABI 패치*\n"
        f"• 장비: `{device_name}`\n"
        "• 결과: 적용하지 않음\n"
        f"• 이유: 안전한 유휴 상태를 확인하지 못했어 "
        f"(상태 `{status}`, 녹화 `{recording}`, 업로드 `{uploading}`)"
    )


def _device_patch_lock(device_name: str) -> threading.Lock:
    digest = hashlib.sha256(
        str(device_name).strip().casefold().encode("utf-8")
    ).digest()
    index = int.from_bytes(digest[:4], "big") % len(
        _DEVICE_PATCH_LOCK_STRIPES
    )
    return _DEVICE_PATCH_LOCK_STRIPES[index]


def _load_patch_asset() -> bytes:
    payload = (
        resources.files(_PATCH_ASSET_PACKAGE)
        .joinpath(_PATCH_ASSET_NAME)
        .read_bytes()
    )
    actual_sha = hashlib.sha256(payload).hexdigest()
    if actual_sha != _PATCH_ASSET_SHA256:
        raise DeviceScannerAbiPatchError(
            "스캐너 ABI 패치 자산 검증에 실패해서 적용하지 않았어",
            "device_scanner_abi_patch_asset_invalid",
        )
    return payload


def _drain_remote_channel(
    channel: Any,
    output: _RemoteOutputTail,
    stop_event: threading.Event,
) -> None:
    """node-gyp 출력이 SSH window를 채우지 않게 실행 중 양쪽을 소비한다."""

    while not stop_event.is_set():
        consumed = False
        try:
            while channel.recv_ready():
                output.append(channel.recv(32_768))
                consumed = True
            while channel.recv_stderr_ready():
                output.append(channel.recv_stderr(32_768))
                consumed = True
            if (
                channel.exit_status_ready()
                and not channel.recv_ready()
                and not channel.recv_stderr_ready()
            ):
                return
        except Exception:
            return
        if not consumed:
            stop_event.wait(0.05)


def _write_idle_confirmation(
    client: Any,
    *,
    target_path: str,
    token: str,
) -> None:
    """권한을 확정한 임시 파일을 rename해 부분 토큰 노출을 막는다."""

    temp_path = f"{target_path}.tmp.{secrets.token_hex(8)}"
    sftp = client.open_sftp()
    remote_file = None
    try:
        _mark_company_api_mutation_attempted()
        # Paramiko에서 `x`는 EXCL만 설정하고 WRITE를 설정하지
        # 않는다. `wx`로 배타 생성과 쓰기를 둘 다 고정한다.
        remote_file = sftp.open(temp_path, "wx")
        remote_file.write((token + "\n").encode("ascii"))
        remote_file.flush()
        remote_file.close()
        remote_file = None
        sftp.chmod(temp_path, 0o600)
        sftp.rename(temp_path, target_path)
    finally:
        if remote_file is not None:
            remote_file.close()
        try:
            sftp.remove(temp_path)
        except OSError:
            pass
        sftp.close()


def _remove_remote_control_files(client: Any, paths: tuple[str, ...]) -> None:
    sftp = None
    try:
        sftp = client.open_sftp()
        for path in paths:
            try:
                sftp.remove(path)
            except OSError:
                continue
    except Exception:
        return
    finally:
        if sftp is not None:
            sftp.close()


def _terminate_remote_patch(client: Any, *, pid_file: str) -> bool:
    """무작위 pidfile과 실제 session leader를 모두 검증한 뒤에만 종료한다."""

    quoted_pid_file = shlex.quote(pid_file)
    command = (
        "bash -lc "
        + shlex.quote(
            "pid_file="
            + quoted_pid_file
            + "; "
            "[ -f \"$pid_file\" ] && [ ! -L \"$pid_file\" ] || exit 3; "
            "[ \"$(stat -c %u \"$pid_file\")\" = \"$(id -u)\" ] || exit 4; "
            "read -r pid < \"$pid_file\"; "
            "case \"$pid\" in ''|0|*[!0-9]*) exit 5;; esac; "
            "sid=$(ps -o sid= -p \"$pid\" | tr -d ' '); "
            "[ \"$sid\" = \"$pid\" ] || exit 6; "
            "kill -TERM -- \"-$pid\""
        )
    )
    result = _run_remote_ssh_command(
        client,
        command=command,
        timeout_sec=10,
        mutation=True,
    )
    return bool(result.get("ok"))


def _newer_device_state(
    assessment: _IdleAssessment,
    baseline_epoch: float | None,
) -> bool:
    return bool(
        assessment.updated_epoch is not None
        and baseline_epoch is not None
        and assessment.updated_epoch > baseline_epoch
    )


def _execute_monitored_patch(
    client: Any,
    *,
    device_name: str,
    script_payload: bytes,
    state_loader: Callable[[str], dict[str, Any] | None] = (
        _get_mda_device_detail
    ),
    ping_sender: Callable[[str], dict[str, Any]] = _send_mda_device_ping,
) -> dict[str, Any]:
    """빌드 중 상태를 감시하고 PM2 stop 직전 새 MDA 상태로 gate를 연다."""

    control_id = secrets.token_hex(16)
    idle_token = secrets.token_hex(32)
    pid_file = f"/home/mommytalk/.node-hid-abi-pid.{control_id}"
    idle_file = f"/home/mommytalk/.node-hid-abi-idle.{control_id}"
    session_command = (
        "pid_file=$1; shift; "
        "printf '%s\\n' \"$$\" > \"$pid_file\"; "
        "exec bash -s -- \"$@\""
    )
    inner_command = (
        "umask 077; "
        f"pid_file={shlex.quote(pid_file)}; "
        "[ ! -e \"$pid_file\" ] || exit 97; "
        # non-interactive Bash의 background command는 명시적 redirect가
        # 없으면 stdin을 /dev/null로 바꾸므로 `<&0`를 반드시 유지한다.
        # setsid 내부 session leader가 pidfile을 쓰게 해 중단 시
        # 무작위 파일과 실제 SID를 둘 다 검증할 수 있게 한다.
        "setsid -f -w bash -c "
        f"{shlex.quote(session_command)} node-hid-abi-session "
        f"{shlex.quote(pid_file)} "
        "--apply --confirm-idle "
        f"--expected-device {shlex.quote(device_name)} "
        f"--idle-confirm-file {shlex.quote(idle_file)} "
        f"--idle-confirm-token {shlex.quote(idle_token)} <&0 & "
        "child=$!; "
        "wait \"$child\"; rc=$?; rm -f -- \"$pid_file\"; exit \"$rc\""
    )
    command = "bash -lc " + shlex.quote(inner_command)
    timeout_sec = max(
        120,
        int(cs.DEVICE_SCANNER_ABI_PATCH_TIMEOUT_SEC or 780),
    )
    output = _RemoteOutputTail()
    stop_event = threading.Event()
    drain_thread: threading.Thread | None = None
    stdin = None
    stdout = None
    stderr = None
    channel = None
    abort_reason = ""
    terminate_ok: bool | None = None
    gate_seen = False
    gate_written = False
    gate_accepted = False
    pm2_stopping = False
    ping_sent = False
    ping_baseline_epoch: float | None = None
    ping_deadline = 0.0
    next_state_poll = 0.0
    started_at = time.monotonic()
    abort_deadline = 0.0

    try:
        _mark_company_api_mutation_attempted()
        stdin, stdout, stderr = client.exec_command(
            command,
            timeout=timeout_sec,
        )
        channel = stdout.channel
        drain_thread = threading.Thread(
            target=_drain_remote_channel,
            args=(channel, output, stop_event),
            name="boxer-node-hid-output-drain",
            daemon=True,
        )
        drain_thread.start()
        stdin.channel.sendall(script_payload)
        stdin.channel.shutdown_write()

        while not channel.exit_status_ready():
            now_monotonic = time.monotonic()
            current_output = output.text()
            if "AWAITING_IDLE_CONFIRMATION" in current_output:
                gate_seen = True
            if "IDLE_CONFIRMATION_ACCEPTED" in current_output:
                gate_accepted = True
            if "PM2_STOPPING" in current_output:
                pm2_stopping = True

            if not abort_reason and now_monotonic - started_at >= timeout_sec:
                abort_reason = "timeout"

            should_poll = bool(
                not abort_reason
                and not pm2_stopping
                and now_monotonic >= next_state_poll
            )
            if should_poll:
                try:
                    assessment = _assess_device_idle(
                        state_loader(device_name),
                        expected_device=device_name,
                        # 대기 중에는 exact 연결/상태/boolean 변화를 감시한다.
                        # timestamp 신선도는 아래 단일 ping 뒤에만 확정한다.
                        require_fresh=ping_sent,
                    )
                except Exception:
                    assessment = _IdleAssessment(
                        False,
                        "device_state_query_failed",
                        "",
                        None,
                        None,
                        "",
                        None,
                    )
                if not assessment.ok:
                    abort_reason = assessment.reason
                elif gate_seen and not ping_sent:
                    ping_baseline_epoch = assessment.updated_epoch
                    try:
                        ping_result = ping_sender(device_name)
                    except Exception:
                        ping_result = {}
                    if ping_result.get("status") is not True:
                        abort_reason = "device_ping_failed"
                    else:
                        ping_sent = True
                        ping_deadline = (
                            now_monotonic + _PING_REFRESH_TIMEOUT_SEC
                        )
                elif ping_sent and not gate_written:
                    if _newer_device_state(
                        assessment,
                        ping_baseline_epoch,
                    ):
                        try:
                            _write_idle_confirmation(
                                client,
                                target_path=idle_file,
                                token=idle_token,
                            )
                        except Exception:
                            abort_reason = "idle_confirmation_write_failed"
                        else:
                            gate_written = True
                    elif now_monotonic >= ping_deadline:
                        abort_reason = "device_ping_state_not_refreshed"
                next_state_poll = now_monotonic + (
                    _GATE_STATE_POLL_INTERVAL_SEC
                    if gate_seen
                    else _STATE_POLL_INTERVAL_SEC
                )

            if abort_reason and terminate_ok is None:
                terminate_ok = _terminate_remote_patch(
                    client,
                    pid_file=pid_file,
                )
                abort_deadline = now_monotonic + _ABORT_WAIT_TIMEOUT_SEC
            elif (
                abort_reason
                and abort_deadline
                and now_monotonic >= abort_deadline
            ):
                break
            time.sleep(0.05)

        completed = channel.exit_status_ready()
        # drain thread가 마지막 chunk를 recv한 뒤 append하기 전에
        # exit-status가 보일 수 있다. 결과 snapshot 전에 반드시 단일
        # consumer로 전환해 성공 marker 유실 race를 막는다.
        stop_event.set()
        if drain_thread is not None:
            drain_thread.join(timeout=2)
        drain_thread_alive = bool(
            drain_thread is not None and drain_thread.is_alive()
        )
        if drain_thread_alive and not abort_reason:
            abort_reason = "output_drain_incomplete"

        # exit-status가 먼저 보이는 짧은 no-op 경로에서도 마지막
        # stdout/stderr가 도착할 여유를 주고, 연속해 빈 상태가
        # 확인될 때까지 단일 consumer로 끝까지 비운다.
        empty_rounds = 0
        drain_deadline = time.monotonic() + 1.0
        while (
            not drain_thread_alive
            and empty_rounds < 5
            and time.monotonic() < drain_deadline
        ):
            consumed = False
            while channel.recv_ready():
                output.append(channel.recv(32_768))
                consumed = True
            while channel.recv_stderr_ready():
                output.append(channel.recv_stderr(32_768))
                consumed = True
            empty_rounds = 0 if consumed else empty_rounds + 1
            time.sleep(0.02)
        exit_status = channel.recv_exit_status() if completed else None
        final_output = output.text()
        gate_seen = gate_seen or "AWAITING_IDLE_CONFIRMATION" in final_output
        gate_accepted = (
            gate_accepted or "IDLE_CONFIRMATION_ACCEPTED" in final_output
        )
        pm2_stopping = pm2_stopping or "PM2_STOPPING" in final_output
        return {
            "ok": completed and exit_status == 0 and not abort_reason,
            "exitStatus": exit_status,
            "output": final_output,
            "reason": abort_reason or (
                "" if exit_status == 0 else f"ssh_exit_{exit_status}"
            ),
            "gateSeen": gate_seen,
            "gateWritten": gate_written,
            "gateAccepted": gate_accepted,
            "pm2Stopping": pm2_stopping,
            "terminateOk": terminate_ok,
            "completed": completed,
        }
    except Exception as exc:
        if channel is not None and not channel.exit_status_ready():
            terminate_ok = _terminate_remote_patch(
                client,
                pid_file=pid_file,
            )
        return {
            "ok": False,
            "exitStatus": None,
            "output": output.text(),
            "reason": type(exc).__name__.lower(),
            "gateSeen": gate_seen,
            "gateWritten": gate_written,
            "gateAccepted": gate_accepted,
            "pm2Stopping": pm2_stopping,
            "terminateOk": terminate_ok,
            "completed": False,
        }
    finally:
        stop_event.set()
        if drain_thread is not None:
            drain_thread.join(timeout=2)
        # 유휴 승인 파일은 항상 없애 예기치 않게 gate가 열리지 않게 한다.
        # 반면 종료를 확인하지 못한 detached session의 pidfile은 남겨야
        # 후속 운영자가 정확한 session group을 찾아 수동 복구할 수 있다.
        remote_completed = bool(
            channel is not None and channel.exit_status_ready()
        )
        _close_ssh_streams(stdin, stdout, stderr)
        cleanup_paths = (
            (idle_file, pid_file) if remote_completed else (idle_file,)
        )
        _remove_remote_control_files(client, cleanup_paths)


def _render_patch_success(device_name: str) -> str:
    return (
        "*스캐너 ABI 패치*\n"
        f"• 장비: `{device_name}`\n"
        "• 결과: 적용 완료\n"
        "• 확인: node-hid 로드, 등록 스캐너 HID 연결, "
        "mommybox-v2 online 상태를 확인했어\n"
        "• 안내: 실제 바코드 입력은 현장에서 한 번 확인해줘"
    )


def _render_no_action(device_name: str) -> str:
    return (
        "*스캐너 ABI 패치*\n"
        f"• 장비: `{device_name}`\n"
        "• 결과: 조치 불필요\n"
        "• 확인: 현재 node-hid가 이미 정상 로드돼서 앱을 재시작하지 않았어"
    )


def _raise_patch_execution_error(
    device_name: str,
    execution: dict[str, Any],
) -> None:
    output = str(execution.get("output") or "")
    if "CRITICAL: 자동 복구가 완전히 끝나지 않았습니다" in output:
        message = (
            "*스캐너 ABI 패치*\n"
            f"• 장비: `{device_name}`\n"
            "• 결과: 즉시 수동 확인 필요\n"
            "• 이유: 자동 복구가 완전히 끝났는지 확인되지 않았어"
        )
    elif "원본 복원 및 앱 재시작 완료" in output:
        message = (
            "*스캐너 ABI 패치*\n"
            f"• 장비: `{device_name}`\n"
            "• 결과: 적용 실패 · 원본 복원 완료\n"
            "• 안내: 같은 메시지를 재전송하지 말고 원인을 먼저 확인해줘"
        )
    elif "GLIBCXX ABI 장애가 아닌 node-hid 오류" in output:
        raise DeviceScannerAbiPatchError(
            "*스캐너 ABI 패치*\n"
            f"• 장비: `{device_name}`\n"
            "• 결과: 적용하지 않음\n"
            "• 이유: GLIBCXX ABI 장애가 아니라 다른 원인이야",
            "device_scanner_abi_patch_unsupported_cause",
        )
    elif execution.get("reason") in {
        "device_not_standby",
        "recording_state_unknown_or_active",
        "upload_state_unknown_or_active",
        "agent_disconnected",
        "device_disconnected",
        "device_state_stale",
        "device_state_query_failed",
        "device_ping_failed",
        "device_ping_state_not_refreshed",
    }:
        message = (
            "*스캐너 ABI 패치*\n"
            f"• 장비: `{device_name}`\n"
            "• 결과: 적용 중단\n"
            "• 이유: 실제 교체 직전까지 안전한 유휴 상태를 유지하지 못했어"
        )
    else:
        message = (
            "*스캐너 ABI 패치*\n"
            f"• 장비: `{device_name}`\n"
            "• 결과: 완료 여부 확인 필요\n"
            "• 안내: 자동 재실행하지 말고 장비 상태를 먼저 확인해줘"
        )
    raise DeviceScannerAbiPatchError(
        message,
        "operation_error",
    )


def _apply_device_scanner_abi_patch(
    question: str,
    *,
    device_name: str | None = None,
    resend_ssh_open: bool = True,
) -> tuple[str, dict[str, Any]]:
    """정확한 한 장비에서만 불변 스크립트를 한 번 실행한다."""

    matched = _CANONICAL_PATCH_PATTERN.fullmatch(
        _normalize_scanner_patch_exact_question(question)
    )
    if matched is None:
        raise DeviceScannerAbiPatchError(
            _build_device_scanner_abi_patch_command_message(),
            "device_scanner_abi_patch_command_required",
        )
    target_device = matched.group(1).upper()
    if str(device_name or target_device).strip().upper() != target_device:
        raise DeviceScannerAbiPatchError(
            _build_device_scanner_abi_patch_command_message(),
            "device_scanner_abi_patch_target_mismatch",
        )
    if not _is_device_scanner_abi_patch_runtime_configured():
        raise DeviceScannerAbiPatchError(
            _build_device_scanner_abi_patch_config_message(),
            "device_scanner_abi_patch_not_configured",
        )

    operation_lock = _device_patch_lock(target_device)
    if not operation_lock.acquire(blocking=False):
        raise DeviceScannerAbiPatchError(
            f"`{target_device}` 스캐너 ABI 패치 작업이 이미 진행 중이야",
            "device_scanner_abi_patch_busy",
        )

    client = None
    try:
        initial_assessment = _assess_device_idle(
            _get_mda_device_detail(target_device),
            expected_device=target_device,
            # 오래 유휴인 장비는 timestamp가 갱신되지 않을 수 있다. 실제
            # 교체 직전 gate의 단일 ping에서 새 timestamp를 별도 증명한다.
            require_fresh=False,
        )
        if not initial_assessment.ok:
            raise DeviceScannerAbiPatchError(
                _build_idle_block_message(
                    target_device,
                    initial_assessment,
                ),
                "device_scanner_abi_patch_not_idle",
            )

        _, ssh_state, client = _open_device_ssh_for_update(
            target_device,
            resend_ssh_open=resend_ssh_open,
        )
        if client is None or not ssh_state.get("ready"):
            raise DeviceScannerAbiPatchError(
                "*스캐너 ABI 패치*\n"
                f"• 장비: `{target_device}`\n"
                "• 결과: 적용하지 않음\n"
                "• 이유: MDA가 보고한 장비 SSH에 연결하지 못했어",
                "device_scanner_abi_patch_ssh_unavailable",
            )

        # SSH open 대기 중 상태가 바뀌었을 수 있어 원격 실행 전에 다시 읽는다.
        second_assessment = _assess_device_idle(
            _get_mda_device_detail(target_device),
            expected_device=target_device,
            require_fresh=False,
        )
        if not second_assessment.ok:
            raise DeviceScannerAbiPatchError(
                _build_idle_block_message(
                    target_device,
                    second_assessment,
                ),
                "device_scanner_abi_patch_not_idle",
            )

        preflight = _run_remote_ssh_command(
            client,
            command=(
                "bash -lc 'command -v setsid >/dev/null && "
                "setsid --help 2>&1 | grep -q -- '--fork' && "
                "setsid --help 2>&1 | grep -q -- '--wait' && "
                "command -v ps >/dev/null && command -v stat >/dev/null'"
            ),
            timeout_sec=10,
        )
        if not preflight.get("ok"):
            raise DeviceScannerAbiPatchError(
                "*스캐너 ABI 패치*\n"
                f"• 장비: `{target_device}`\n"
                "• 결과: 적용하지 않음\n"
                "• 이유: 안전 실행에 필요한 장비 명령을 찾지 못했어",
                "device_scanner_abi_patch_preflight_failed",
            )

        execution = _execute_monitored_patch(
            client,
            device_name=target_device,
            script_payload=_load_patch_asset(),
        )
        output = str(execution.get("output") or "")
        if execution.get("ok") and "REPAIR_SUCCESS:" in output:
            return _render_patch_success(target_device), {
                "route": DEVICE_SCANNER_ABI_PATCH_ROUTE,
                "device": target_device,
                "status": "repair_success",
                "scriptSha256": _PATCH_ASSET_SHA256,
            }
        if execution.get("ok") and "NO_ACTION_REQUIRED:" in output:
            return _render_no_action(target_device), {
                "route": DEVICE_SCANNER_ABI_PATCH_ROUTE,
                "device": target_device,
                "status": "no_action_required",
                "scriptSha256": _PATCH_ASSET_SHA256,
            }
        _raise_patch_execution_error(target_device, execution)
        raise AssertionError("unreachable")
    finally:
        if client is not None:
            client.close()
        operation_lock.release()


__all__ = [
    "DEVICE_SCANNER_ABI_PATCH_ROUTE",
    "DeviceScannerAbiPatchError",
    "_apply_device_scanner_abi_patch",
    "_assess_device_idle",
    "_build_device_scanner_abi_patch_command_message",
    "_build_device_scanner_abi_patch_config_message",
    "_extract_device_name_for_scanner_abi_patch",
    "_is_device_scanner_abi_patch_intent",
    "_is_device_scanner_abi_patch_request",
    "_is_device_scanner_abi_patch_runtime_configured",
    "_load_patch_asset",
]
