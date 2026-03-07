import shlex
import socket
import tempfile
from pathlib import PurePosixPath
from typing import Any

try:
    import paramiko
except ImportError:  # pragma: no cover - runtime guard
    paramiko = None

from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.core.utils import _display_value, _truncate_text
from boxer.routers.company.barcode_log import (
    _build_phase2_scope_request_message,
    _error_lines_in_session,
    _expand_device_contexts_to_recordings_hospital_scope,
    _extract_recording_sessions,
    _extract_scan_events_with_line_no,
    _find_error_lines,
    _find_first_ffmpeg_error_context,
    _find_recording_recovery_context,
)
from boxer.routers.company.box_db import (
    _lookup_device_contexts_by_barcode,
)
from boxer.routers.common.s3 import _build_s3_client
from boxer.routers.company.mda_graphql import (
    _open_mda_device_ssh,
    _get_mda_device_agent_ssh,
    _is_mda_graphql_configured,
    _wait_for_mda_device_agent_ssh,
)
from boxer.routers.company.s3_domain import _fetch_s3_device_log_lines

_DEVICE_FILE_ID_HINTS = (
    "fileid",
    "file id",
    "파일id",
    "파일 id",
    "파일 아이디",
    "파일아이디",
)

_DEVICE_FILE_DOWNLOAD_HINTS = (
    "다운로드",
    "받아줘",
    "받아 줘",
    "내려받아",
    "복구",
)

_DEVICE_FILE_REMOTE_HINTS = (
    "파일 있",
    "파일있",
    "파일 있어",
    "파일있어",
    "파일 존재",
    "존재 확인",
    "남은 영상",
    "남은 파일",
    "장비에 남은 영상",
    "장비에 남은 파일",
    "장비 영상",
    "로컬 영상",
    "장비 파일",
    "장비에 파일",
    "디바이스 파일",
    "로컬 파일",
    *_DEVICE_FILE_DOWNLOAD_HINTS,
)
_DEVICE_FILE_PROBE_HINTS = _DEVICE_FILE_ID_HINTS + _DEVICE_FILE_REMOTE_HINTS


def _is_barcode_device_file_probe_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_PROBE_HINTS)


def _should_probe_device_files(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_REMOTE_HINTS)


def _should_download_device_files(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_DOWNLOAD_HINTS)


def _is_device_file_probe_allowed(user_id: str | None) -> bool:
    allowed = cs.DEVICE_FILE_PROBE_ALLOWED_USER_IDS
    if not allowed:
        return True
    return bool(user_id and user_id in allowed)


def _build_device_file_probe_permission_message() -> str:
    return "장비 파일 존재 확인은 허용된 사용자만 가능해"


def _build_device_file_probe_config_message() -> str:
    return (
        "장비 파일 존재 확인 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_GRAPHQL_BEARER_TOKEN, DEVICE_SSH_PASSWORD가 필요해"
    )


def _build_device_file_download_config_message() -> str:
    return (
        "장비 파일 다운로드 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_GRAPHQL_BEARER_TOKEN, DEVICE_SSH_PASSWORD, "
        "S3_ULTRASOUND_BUCKET(또는 DEVICE_FILE_DOWNLOAD_BUCKET)이 필요해"
    )


def _display_device_probe_reason(reason: str | None) -> str:
    normalized = str(reason or "").strip().lower()
    if normalized in {"agent_ssh_not_ready", "novalidconnectionserror", "timeout", "oerror"}:
        return "장비 SSH 연결 준비 실패 (장비 오프라인 또는 네트워크 불안정 가능)"
    if normalized == "ssh_auth_failed":
        return "장비 SSH 인증 실패"
    if normalized == "file_id_missing":
        return "fileId가 없어 장비 파일 확인 불가"
    if normalized == "missing_device_name":
        return "장비명이 없어 장비 파일 확인 불가"
    if normalized == "missing_password":
        return "DEVICE_SSH_PASSWORD 설정이 없어 장비 파일 확인 불가"
    if normalized == "paramiko_missing":
        return "paramiko 설치가 없어 장비 파일 확인 불가"
    if normalized == "missing_download_bucket":
        return "다운로드 버킷 설정이 없어 장비 파일 다운로드 불가"
    if normalized == "s3_upload_failed":
        return "S3 업로드 실패"
    if normalized == "presigned_url_failed":
        return "presigned URL 생성 실패"
    if normalized.startswith("ssh_exit_"):
        return f"장비 파일 확인 명령 실패 ({normalized})"
    if not normalized:
        return "장비 파일 확인 실패"
    return normalized


def _connect_device_ssh_client(host: str, port: int) -> Any:
    if paramiko is None:
        return {
            "ok": False,
            "reason": "paramiko_missing",
        }
    if not cs.DEVICE_SSH_PASSWORD:
        return {
            "ok": False,
            "reason": "missing_password",
        }

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=host,
            port=int(port),
            username=cs.DEVICE_SSH_USER,
            password=cs.DEVICE_SSH_PASSWORD,
            timeout=max(1, cs.DEVICE_SSH_CONNECT_TIMEOUT_SEC),
            banner_timeout=max(1, cs.DEVICE_SSH_CONNECT_TIMEOUT_SEC),
            auth_timeout=max(1, cs.DEVICE_SSH_CONNECT_TIMEOUT_SEC),
            look_for_keys=False,
            allow_agent=False,
        )
    except paramiko.AuthenticationException:
        client.close()
        return {
            "ok": False,
            "reason": "ssh_auth_failed",
        }
    except (
        paramiko.SSHException,
        paramiko.ssh_exception.NoValidConnectionsError,
        socket.timeout,
        TimeoutError,
        OSError,
    ) as exc:
        client.close()
        return {
            "ok": False,
            "reason": type(exc).__name__.lower(),
        }

    return {
        "ok": True,
        "client": client,
    }


def _find_device_files_by_file_id(
    host: str,
    port: int,
    file_id: str,
) -> dict[str, Any]:
    if not host or not port or not file_id:
        return {
            "ok": False,
            "reason": "missing_input",
            "files": [],
        }

    pattern = f"*{file_id}*.mp4"
    search_paths = cs.DEVICE_FILE_SEARCH_PATHS or [
        "/home/mommytalk/AppData/Videos",
        "/home/mommytalk/AppData/TrashCan",
    ]
    path_args = " ".join(shlex.quote(path) for path in search_paths)
    remote_cmd = (
        f"find {path_args} -type f -name {shlex.quote(pattern)} 2>/dev/null | sort -u"
    )

    connection = _connect_device_ssh_client(host, int(port))
    if not connection.get("ok"):
        return {
            "ok": False,
            "reason": connection.get("reason"),
            "files": [],
        }

    client = connection["client"]
    try:
        _, stdout, stderr = client.exec_command(
            remote_cmd,
            timeout=max(1, cs.DEVICE_SSH_COMMAND_TIMEOUT_SEC),
        )
        exit_status = stdout.channel.recv_exit_status()
        stdout_text = (stdout.read() or b"").decode("utf-8", errors="replace").strip()
        stderr_text = (stderr.read() or b"").decode("utf-8", errors="replace").strip()
    except (
        paramiko.SSHException,
        paramiko.ssh_exception.NoValidConnectionsError,
        socket.timeout,
        TimeoutError,
        OSError,
    ) as exc:
        return {
            "ok": False,
            "reason": type(exc).__name__.lower(),
            "files": [],
        }
    finally:
        client.close()

    files = [line.strip() for line in stdout_text.splitlines() if line.strip()]
    if exit_status not in (0, 1):
        return {
            "ok": False,
            "reason": f"ssh_exit_{exit_status}",
            "stderr": stderr_text[:300],
            "files": files,
        }

    return {
        "ok": True,
        "reason": "ok",
        "files": files,
    }


def _build_device_download_s3_key(file_name: str) -> str:
    prefix = (s.DEVICE_FILE_DOWNLOAD_PREFIX or "").strip().strip("/")
    if prefix:
        return f"{prefix}/{file_name}"
    return file_name


def _download_device_files_to_s3(
    host: str,
    port: int,
    remote_files: list[str],
) -> dict[str, Any]:
    bucket = (s.DEVICE_FILE_DOWNLOAD_BUCKET or "").strip()
    if not bucket:
        return {
            "ok": False,
            "reason": "missing_download_bucket",
            "downloads": [],
        }

    connection = _connect_device_ssh_client(host, int(port))
    if not connection.get("ok"):
        return {
            "ok": False,
            "reason": connection.get("reason"),
            "downloads": [],
        }

    client = connection["client"]
    s3_client = _build_s3_client()
    downloads: list[dict[str, Any]] = []
    try:
        sftp = client.open_sftp()
        try:
            for remote_path in remote_files:
                file_name = PurePosixPath(_display_value(remote_path, default="")).name
                if not file_name:
                    continue
                key = _build_device_download_s3_key(file_name)
                temp_path = ""
                try:
                    with tempfile.NamedTemporaryFile(prefix="device-file-", suffix=f"-{file_name}", delete=False) as tmp_file:
                        temp_path = tmp_file.name
                    sftp.get(remote_path, temp_path)
                    s3_client.upload_file(temp_path, bucket, key)
                    presigned_url = s3_client.generate_presigned_url(
                        "get_object",
                        Params={"Bucket": bucket, "Key": key},
                        ExpiresIn=max(60, s.DEVICE_FILE_DOWNLOAD_PRESIGNED_EXPIRES_SEC),
                    )
                    downloads.append(
                        {
                            "ok": True,
                            "fileName": file_name,
                            "key": key,
                            "url": presigned_url,
                        }
                    )
                except Exception as exc:
                    reason = "s3_upload_failed"
                    if "presigned" in type(exc).__name__.lower():
                        reason = "presigned_url_failed"
                    downloads.append(
                        {
                            "ok": False,
                            "fileName": file_name,
                            "key": key,
                            "reason": reason,
                        }
                    )
                finally:
                    if temp_path:
                        try:
                            import os

                            os.remove(temp_path)
                        except OSError:
                            pass
        finally:
            sftp.close()
    finally:
        client.close()

    return {
        "ok": any(item.get("ok") for item in downloads),
        "reason": "ok" if any(item.get("ok") for item in downloads) else "s3_upload_failed",
        "downloads": downloads,
    }


def _probe_device_files_for_record(record: dict[str, Any]) -> dict[str, Any]:
    device_name = str(record.get("deviceName") or "").strip()
    if not device_name:
        return {
            "sshReady": False,
            "sshReason": "missing_device_name",
        }

    wait_result = _wait_for_mda_device_agent_ssh(device_name)
    device_info = wait_result.get("device") if isinstance(wait_result, dict) else {}
    agent_ssh = (device_info or {}).get("agentSsh") if isinstance(device_info, dict) else None
    if not wait_result.get("ready") or not isinstance(agent_ssh, dict):
        return {
            "sshReady": False,
            "sshReason": "agent_ssh_not_ready",
            "opened": wait_result.get("opened"),
            "pollCount": wait_result.get("pollCount"),
            "reusedExisting": bool(wait_result.get("reusedExisting")),
        }

    def build_results(current_agent_ssh: dict[str, Any]) -> list[dict[str, Any]]:
        host = str(current_agent_ssh.get("host") or "").strip()
        port = current_agent_ssh.get("port")
        built: list[dict[str, Any]] = []
        for session in record.get("sessions") or []:
            file_id = str(session.get("fileId") or "").strip()
            if not file_id:
                built.append(
                    {
                        "fileId": "",
                        "ok": False,
                        "reason": "file_id_missing",
                        "files": [],
                    }
                )
                continue
            built.append(
                {
                    "fileId": file_id,
                    **_find_device_files_by_file_id(host, int(port), file_id),
                }
            )
        return built

    results = build_results(agent_ssh)
    should_retry = any(
        item.get("reason") in {"novalidconnectionserror", "timeout", "oerror"}
        for item in results
        if isinstance(item, dict) and not item.get("ok")
    )

    if should_retry:
        _open_mda_device_ssh(device_name)
        wait_result = _wait_for_mda_device_agent_ssh(device_name)
        device_info = wait_result.get("device") if isinstance(wait_result, dict) else {}
        retried_agent_ssh = (device_info or {}).get("agentSsh") if isinstance(device_info, dict) else None
        if wait_result.get("ready") and isinstance(retried_agent_ssh, dict):
            agent_ssh = retried_agent_ssh
            results = build_results(agent_ssh)

    return {
        "sshReady": True,
        "sshReason": "ready",
        "agentSsh": {
            "host": _display_value(agent_ssh.get("host"), default=""),
            "port": int(agent_ssh.get("port") or 0),
            "status": _display_value(agent_ssh.get("status"), default=""),
        },
        "opened": wait_result.get("opened"),
        "pollCount": wait_result.get("pollCount"),
        "reusedExisting": bool(wait_result.get("reusedExisting")),
        "results": results,
    }


def _build_session_file_candidate_entry(
    source_lines: list[str],
    session: dict[str, Any],
    session_error_lines: list[tuple[int, str]],
) -> dict[str, Any]:
    first_ffmpeg_error = _find_first_ffmpeg_error_context(session_error_lines, [session])
    recovery_context = _find_recording_recovery_context(
        source_lines,
        session,
    )

    added_recording = (recovery_context or {}).get("addedRecording") or {}
    started_recording = (recovery_context or {}).get("startedRecording") or {}
    spawned_recording = (recovery_context or {}).get("spawnedRecordingFfmpeg") or {}
    spawned_motion = (recovery_context or {}).get("spawnedMotionFfmpeg") or {}

    return {
        "startTime": _display_value(session.get("start_time_label"), default="시간미상"),
        "stopTime": _display_value(session.get("stop_time_label"), default="미확인"),
        "stopToken": _display_value(session.get("stop_token"), default=""),
        "fileId": _display_value((recovery_context or {}).get("fileId"), default=""),
        "addedRecordingTime": _display_value(added_recording.get("timeLabel"), default=""),
        "startedRecordingTime": _display_value(started_recording.get("timeLabel"), default=""),
        "spawnedRecordingTime": _display_value(spawned_recording.get("timeLabel"), default=""),
        "spawnedMotionTime": _display_value(spawned_motion.get("timeLabel"), default=""),
        "firstFfmpegErrorTime": _display_value(
            (first_ffmpeg_error or {}).get("timeLabel"),
            default="",
        ),
        "probe": None,
        "download": None,
    }


def _render_file_candidate_result(
    *,
    barcode: str,
    log_date: str,
    all_device_contexts: list[dict[str, Any]],
    records: list[dict[str, Any]],
    used_expanded_scope: bool,
    logs_found_any: int,
) -> str:
    if logs_found_any == 0:
        return (
            "*파일 확인 대상 세션 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`"
        )

    if not records:
        lines = [
            "*파일 확인 대상 세션 조회 결과*",
            f"• 바코드: `{barcode}`",
            f"• 날짜: `{log_date}`",
            f"• 매핑 장비: `{len(all_device_contexts)}개`",
            f"• 확인한 로그 파일: `{logs_found_any}개`",
            "• 결과: 요청 바코드 세션을 찾지 못했어",
        ]
        if used_expanded_scope:
            lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비까지 확장 검색했어")
        return "\n".join(lines)

    lines = [
        "*파일 확인 대상 세션 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(all_device_contexts)}개`",
        f"• 세션이 확인된 장비: `{len(records)}개`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비까지 확장 검색했어")

    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{_display_value(record.get('deviceName'), default='미확인')}`")
        lines.append(f"• 병원: `{_display_value(record.get('hospitalName'), default='미확인')}`")
        lines.append(f"• 병실: `{_display_value(record.get('roomName'), default='미확인')}`")
        lines.append(f"• 파일: `{_display_value(record.get('logKey'), default='미확인')}`")
        lines.append(f"• 세션 수: `{len(record.get('sessions') or [])}건`")

        for index, session in enumerate(record.get("sessions") or [], start=1):
            lines.append("")
            lines.append(
                f"*세션 {index}* (`{_display_value(session.get('startTime'), default='시간미상')}`"
                f" ~ `{_display_value(session.get('stopTime'), default='미확인')}`)"
            )
            stop_token = _display_value(session.get("stopToken"), default="")
            if stop_token:
                lines.append(f"• 종료 토큰: `{stop_token}`")
            file_id = _display_value(session.get("fileId"), default="미추출")
            lines.append(f"• fileId: `{file_id}`")

            added_time = _display_value(session.get("addedRecordingTime"), default="")
            started_time = _display_value(session.get("startedRecordingTime"), default="")
            spawned_time = _display_value(session.get("spawnedRecordingTime"), default="")
            spawned_motion_time = _display_value(session.get("spawnedMotionTime"), default="")
            first_ffmpeg_error_time = _display_value(session.get("firstFfmpegErrorTime"), default="")
            start_logs: list[str] = []
            if added_time:
                start_logs.append(f"addRecording `{added_time}`")
            if started_time:
                start_logs.append(f"Started recording `{started_time}`")
            if spawned_time:
                start_logs.append(f"RECORDING ffmpeg 시작 `{spawned_time}`")
            if spawned_motion_time and not spawned_time:
                start_logs.append(f"MOTION ffmpeg 시작 `{spawned_motion_time}`")
            if start_logs:
                lines.append(f"• fileId 근거 로그: {', '.join(start_logs)}")
            if first_ffmpeg_error_time:
                lines.append(f"• 첫 ffmpeg 오류: `{first_ffmpeg_error_time}`")

            probe = session.get("probe") if isinstance(session.get("probe"), dict) else None
            if probe:
                if probe.get("ok"):
                    found_files = probe.get("files") or []
                    lines.append(f"• 장비 파일 확인: `{len(found_files)}개`")
                    for found_file in found_files:
                        file_name = PurePosixPath(_display_value(found_file, default="")).name
                        lines.append(f"  - `{file_name}`")
                else:
                    reason = _display_device_probe_reason(probe.get("reason"))
                    lines.append(f"• 장비 파일 확인: 실패 ({reason})")

            download = session.get("download") if isinstance(session.get("download"), dict) else None
            if download:
                if download.get("ok"):
                    download_items = [
                        item
                        for item in (download.get("downloads") or [])
                        if isinstance(item, dict) and item.get("ok") and item.get("url")
                    ]
                    lines.append(f"• 다운로드 링크: `{len(download_items)}개` (1시간)")
                    for item in download_items:
                        file_name = _display_value(item.get("fileName"), default="파일")
                        url = _display_value(item.get("url"), default="")
                        lines.append(f"  - <{url}|{file_name}>")
                else:
                    reason = _display_device_probe_reason(download.get("reason"))
                    lines.append(f"• 다운로드 준비: 실패 ({reason})")

        record_probe = record.get("deviceProbe") if isinstance(record.get("deviceProbe"), dict) else None
        if record_probe:
            if not record_probe.get("sshReady"):
                lines.append(
                    f"• 장비 파일 확인: 실패 ({_display_device_probe_reason(record_probe.get('sshReason'))})"
                )

    return _truncate_text("\n".join(lines), 38000)


def _locate_barcode_file_candidates(
    s3_client: Any,
    barcode: str,
    log_date: str,
    *,
    recordings_context: dict[str, Any] | None = None,
    device_contexts: list[dict[str, Any]] | None = None,
    probe_remote_files: bool = False,
    download_remote_files: bool = False,
) -> tuple[str, dict[str, Any]]:
    all_device_contexts = device_contexts
    if all_device_contexts is None:
        all_device_contexts = _lookup_device_contexts_by_barcode(
            barcode,
            recordings_context=recordings_context,
        )

    if not all_device_contexts:
        result_text = (
            "*파일 확인 대상 세션 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• devices에서 장비 매핑 정보를 찾지 못했어"
        )
        return result_text, {
            "route": "device_file_candidate_lookup",
            "request": {"barcode": barcode, "date": log_date},
            "records": [],
        }

    max_devices = max(1, min(20, cs.LOG_ANALYSIS_MAX_DEVICES))
    target_device_contexts = all_device_contexts[:max_devices]
    logs_found_any = 0
    records: list[dict[str, Any]] = []
    used_expanded_scope = False

    def _analyze_batch(device_context_batch: list[dict[str, Any]]) -> None:
        nonlocal logs_found_any
        for device_context in device_context_batch:
            device_name = str(device_context.get("deviceName") or "").strip()
            if not device_name:
                continue

            log_data = _fetch_s3_device_log_lines(
                s3_client,
                device_name,
                log_date,
                tail_only=False,
            )
            if not log_data["found"]:
                continue

            logs_found_any += 1
            source_lines = log_data["lines"]
            events = _extract_scan_events_with_line_no(source_lines)
            sessions = _extract_recording_sessions(
                source_lines,
                barcode,
                cs.LOG_SESSION_SAFETY_LINES,
                scan_events=events,
            )
            if not sessions:
                continue

            error_lines = _find_error_lines(source_lines)
            session_entries: list[dict[str, Any]] = []
            for session in sessions:
                session_entries.append(
                    _build_session_file_candidate_entry(
                        source_lines,
                        session,
                        _error_lines_in_session(error_lines, session),
                    )
                )

            records.append(
                {
                    "deviceName": device_name,
                    "hospitalName": _display_value(device_context.get("hospitalName"), default="미확인"),
                    "roomName": _display_value(device_context.get("roomName"), default="미확인"),
                    "logKey": _display_value(log_data.get("key"), default="미확인"),
                    "sessions": session_entries,
                    "deviceProbe": None,
                }
            )

    _analyze_batch(target_device_contexts)

    if not records:
        expanded_device_contexts = _expand_device_contexts_to_recordings_hospital_scope(
            recordings_context,
            target_device_contexts,
        )
        if expanded_device_contexts:
            used_expanded_scope = True
            _analyze_batch(expanded_device_contexts[: max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 4))])

    if probe_remote_files and records:
        for record in records:
            device_probe = _probe_device_files_for_record(record)
            record["deviceProbe"] = device_probe
            results_by_file_id = {
                str(item.get("fileId") or "").strip(): item
                for item in (device_probe.get("results") or [])
                if isinstance(item, dict)
            }
            for session in record.get("sessions") or []:
                file_id = str(session.get("fileId") or "").strip()
                session["probe"] = results_by_file_id.get(file_id)
                probe = session.get("probe") if isinstance(session.get("probe"), dict) else None
                if download_remote_files and probe and probe.get("ok"):
                    agent_ssh = device_probe.get("agentSsh") if isinstance(device_probe, dict) else None
                    if isinstance(agent_ssh, dict):
                        host = str(agent_ssh.get("host") or "").strip()
                        port = int(agent_ssh.get("port") or 0)
                        remote_files = [
                            _display_value(item, default="")
                            for item in (probe.get("files") or [])
                            if _display_value(item, default="")
                        ]
                        if host and port and remote_files:
                            session["download"] = _download_device_files_to_s3(
                                host,
                                port,
                                remote_files,
                            )

    result_text = _render_file_candidate_result(
        barcode=barcode,
        log_date=log_date,
        all_device_contexts=all_device_contexts,
        records=records,
        used_expanded_scope=used_expanded_scope,
        logs_found_any=logs_found_any,
    )
    payload = {
        "route": "device_file_candidate_lookup",
        "source": "box_db+s3",
        "request": {
            "barcode": barcode,
            "date": log_date,
            "usedExpandedScope": used_expanded_scope,
            "probeRemoteFiles": probe_remote_files,
            "downloadRemoteFiles": download_remote_files,
        },
        "summary": {
            "recordCount": len(records),
            "logsFound": logs_found_any,
            "deviceCount": len(all_device_contexts),
        },
        "records": records,
    }
    return result_text, payload


def _build_device_file_scope_request_message(barcode: str, reason: str) -> str:
    base = _build_phase2_scope_request_message(
        barcode,
        reason,
        "*파일 확인 대상 세션 조회 결과*",
    )
    return base.replace("로그 분석`", "파일 있나`")
