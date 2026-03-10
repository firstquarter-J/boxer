import base64
import json
import hashlib
import hmac
import re
import shlex
import socket
import tempfile
from datetime import datetime
from pathlib import PurePosixPath
from typing import Any
from zoneinfo import ZoneInfo

try:
    import paramiko
except ImportError:  # pragma: no cover - runtime guard
    paramiko = None

try:
    import requests
except ImportError:  # pragma: no cover - runtime guard
    requests = None

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

_DEVICE_FILE_LIST_HINTS = (
    "목록",
    "남은 영상",
    "남은 파일",
    "장비에 남은 영상",
    "장비에 남은 파일",
    "장비 영상",
    "로컬 영상",
    "장비 파일",
    "로컬 파일",
)

_DEVICE_FILE_DOWNLOAD_HINTS = (
    "다운로드",
    "영상 다운",
    "영상다운",
    "파일 다운",
    "파일다운",
    "영상 받아",
    "영상 받아줘",
    "영상받아",
    "영상받아줘",
    "받아줘",
    "받아 줘",
    "내려받아",
)

_DEVICE_FILE_RECOVERY_HINTS = (
    "영상 복구",
    "영상복구",
    "파일 복구",
    "파일복구",
    "복구",
    "복구해",
    "복구 해",
    "복구해줘",
    "복구 해줘",
)

_DEVICE_FILE_REMOTE_HINTS = (
    "파일 있",
    "파일있",
    "파일 있어",
    "파일있어",
    "파일 있는지",
    "파일있는지",
    "파일 존재",
    "영상 있",
    "영상있",
    "영상 있어",
    "영상있어",
    "영상 있는지",
    "영상있는지",
    "영상 존재",
    "있는지",
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
    *_DEVICE_FILE_RECOVERY_HINTS,
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


def _should_recover_device_files(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_RECOVERY_HINTS)


def _should_render_compact_device_file_list(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    if any(hint in text or hint in lowered for hint in _DEVICE_FILE_ID_HINTS):
        return False
    if any(hint in text or hint in lowered for hint in _DEVICE_FILE_DOWNLOAD_HINTS):
        return False
    if any(hint in text or hint in lowered for hint in _DEVICE_FILE_RECOVERY_HINTS):
        return False
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_LIST_HINTS)


def _should_render_compact_file_id_result(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    if any(hint in text or hint in lowered for hint in _DEVICE_FILE_DOWNLOAD_HINTS):
        return False
    if any(hint in text or hint in lowered for hint in _DEVICE_FILE_RECOVERY_HINTS):
        return False
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_ID_HINTS)


def _should_render_compact_device_download_result(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_DOWNLOAD_HINTS)


def _should_render_compact_device_recovery_result(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_RECOVERY_HINTS)


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
        "MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD, DEVICE_SSH_PASSWORD가 필요해"
    )


def _build_device_file_download_config_message() -> str:
    return (
        "장비 파일 다운로드 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD, DEVICE_SSH_PASSWORD, "
        "DEVICE_FILE_DOWNLOAD_BUCKET이 필요해"
    )


def _build_device_file_recovery_config_message() -> str:
    return (
        "장비 영상 복구 설정이 부족해. "
        "MDA_GRAPHQL_URL, MDA_ADMIN_USER_PASSWORD, DEVICE_SSH_PASSWORD, "
        "BOX_UPLOADER_BASE_URL이 필요해"
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
    if normalized == "requests_missing":
        return "requests 설치가 없어 업로더 전송 불가"
    if normalized == "recording_file_missing":
        return "본편 mp4 파일이 없어 업로더 전송 불가"
    if normalized == "missing_recorded_at":
        return "recordedAt 계산 실패로 업로더 전송 불가"
    if normalized == "uploader_request_failed":
        return "업로더 전송 실패"
    if normalized.startswith("uploader_http_"):
        return f"업로더 HTTP 오류 ({normalized.split('_')[-1]})"
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


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _build_uploader_bearer_token(device_name: str) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    payload = {"deviceName": device_name}
    signing_input = (
        f"{_b64url_encode(json.dumps(header, separators=(',', ':')).encode('utf-8'))}."
        f"{_b64url_encode(json.dumps(payload, separators=(',', ':')).encode('utf-8'))}"
    )
    signature = hmac.new(
        (cs.UPLOADER_JWT_SECRET or "").encode("utf-8"),
        signing_input.encode("ascii"),
        hashlib.sha256,
    ).digest()
    return f"{signing_input}.{_b64url_encode(signature)}"


def _build_uploader_recorded_at_ms(log_date: str, *time_labels: str) -> str:
    for time_label in time_labels:
        normalized = _display_value(time_label, default="").strip()
        if not normalized:
            continue
        try:
            dt = datetime.strptime(f"{log_date} {normalized}", "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=ZoneInfo("Asia/Seoul")
            )
        except ValueError:
            continue
        return str(int(dt.timestamp() * 1000))
    return ""


def _normalize_recovery_file_base(file_base: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "", _display_value(file_base, default=""))


def _build_uploader_targets(remote_files: list[str]) -> list[dict[str, str]]:
    targets: list[dict[str, str]] = []
    seen_file_ids: set[str] = set()

    for remote_path in remote_files:
        normalized_path = _display_value(remote_path, default="").strip()
        if not normalized_path:
            continue

        original_name = PurePosixPath(normalized_path).name
        lowered_name = original_name.lower()
        file_id = ""
        upload_name = original_name

        if lowered_name.endswith(".motion.mp4"):
            file_base = _normalize_recovery_file_base(original_name[: -len(".motion.mp4")])
            if not file_base:
                continue
            file_id = f"{file_base}motion"
            upload_name = f"{file_id}.mp4"
        else:
            segmented_match = re.match(r"^(?P<base>.+)\.(?P<segment>\d+)\.mp4$", original_name, re.IGNORECASE)
            if segmented_match:
                file_base = _normalize_recovery_file_base(segmented_match.group("base"))
                segment = segmented_match.group("segment")
                if not file_base:
                    continue
                file_id = f"{file_base}{segment}"
                upload_name = f"{file_id}.mp4"
            elif lowered_name.endswith(".mp4"):
                file_base = _normalize_recovery_file_base(original_name[:-4])
                if not file_base:
                    continue
                file_id = file_base
                upload_name = f"{file_id}.mp4"

        if not file_id or file_id in seen_file_ids:
            continue
        seen_file_ids.add(file_id)
        targets.append(
            {
                "remotePath": normalized_path,
                "fileId": file_id,
                "uploadName": upload_name,
                "sourceName": original_name,
            }
        )

    return targets


def _upload_device_files_to_uploader(
    host: str,
    port: int,
    remote_files: list[str],
    *,
    barcode: str,
    device_name: str,
    file_id: str,
    log_date: str,
    started_time: str,
    added_time: str,
    spawned_time: str,
    session_start_time: str,
) -> dict[str, Any]:
    if requests is None:
        return {
            "ok": False,
            "reason": "requests_missing",
            "uploads": [],
        }

    uploader_base_url = (cs.BOX_UPLOADER_BASE_URL or "").strip().rstrip("/")
    uploader_path = (cs.BOX_UPLOADER_RECORDING_PATH or "").strip()
    if not uploader_base_url or not uploader_path:
        return {
            "ok": False,
            "reason": "missing_uploader_config",
            "uploads": [],
        }

    targets = _build_uploader_targets(remote_files)
    if not targets:
        return {
            "ok": False,
            "reason": "recording_file_missing",
            "uploads": [],
        }

    recorded_at_ms = _build_uploader_recorded_at_ms(
        log_date,
        started_time,
        added_time,
        spawned_time,
        session_start_time,
    )
    if not recorded_at_ms:
        return {
            "ok": False,
            "reason": "missing_recorded_at",
            "uploads": [],
        }

    connection = _connect_device_ssh_client(host, int(port))
    if not connection.get("ok"):
        return {
            "ok": False,
            "reason": connection.get("reason"),
            "uploads": [],
        }

    client = connection["client"]
    uploads: list[dict[str, Any]] = []
    temp_paths: list[str] = []
    upload_url = f"{uploader_base_url}{uploader_path}"

    try:
        sftp = client.open_sftp()
        try:
            token = _build_uploader_bearer_token(device_name)
            for target in targets:
                upload_name = target["uploadName"]
                with tempfile.NamedTemporaryFile(
                    prefix="device-upload-",
                    suffix=f"-{upload_name}",
                    delete=False,
                ) as tmp_file:
                    local_temp_path = tmp_file.name
                temp_paths.append(local_temp_path)
                sftp.get(target["remotePath"], local_temp_path)

                with open(local_temp_path, "rb") as recording_fp:
                    response = requests.post(
                        upload_url,
                        headers={"Authorization": f"Bearer {token}"},
                        data={
                            "deviceName": device_name,
                            "barcode": barcode,
                            "fileId": target["fileId"],
                            "recordedAt": recorded_at_ms,
                        },
                        files={
                            "recording": (upload_name, recording_fp, "video/mp4"),
                        },
                        timeout=max(1, cs.BOX_UPLOADER_TIMEOUT_SEC),
                    )

                try:
                    payload = response.json()
                except ValueError:
                    payload = {}

                status = _display_value(payload.get("status"), default="")
                data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
                item_ok = response.status_code < 400 and status in {"success", "alreadyExists"}
                item_reason = "ok" if item_ok else (
                    f"uploader_http_{response.status_code}"
                    if response.status_code >= 400
                    else "uploader_request_failed"
                )
                uploads.append(
                    {
                        "ok": item_ok,
                        "reason": item_reason,
                        "status": status or "unknown",
                        "fileNames": [upload_name],
                        "sourceFileName": target["sourceName"],
                        "uploadedFileId": target["fileId"],
                        "ultrasoundSeq": _display_value(data.get("ultrasoundSeq"), default=""),
                        "mdaUrl": f"https://mda.kr.mmtalkbox.com/cs?search={barcode}",
                        "message": _display_value(payload.get("message"), default=""),
                    }
                )
        finally:
            sftp.close()
    except requests.RequestException:
        return {
            "ok": False,
            "reason": "uploader_request_failed",
            "uploads": [],
        }
    except Exception as exc:
        return {
            "ok": False,
            "reason": type(exc).__name__.lower(),
            "uploads": [],
        }
    finally:
        client.close()
        for temp_path in temp_paths:
            try:
                import os

                os.remove(temp_path)
            except OSError:
                pass
    any_success = any(item.get("ok") for item in uploads)
    return {
        "ok": any_success,
        "reason": "ok" if any_success else "uploader_request_failed",
        "uploads": uploads,
    }


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
    compact_file_list: bool = False,
    compact_file_id: bool = False,
    compact_download: bool = False,
    compact_recovery: bool = False,
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

    if compact_file_list:
        lines = [
            "*장비 파일 목록 조회 결과*",
            f"• 바코드: `{barcode}`",
            f"• 날짜: `{log_date}`",
            f"• 세션이 확인된 장비: `{len(records)}개`",
        ]
        if used_expanded_scope:
            lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비까지 확장 검색했어")

        for record in records:
            lines.append("")
            lines.append(f"• 장비: `{_display_value(record.get('deviceName'), default='미확인')}`")
            lines.append(f"• 병원: `{_display_value(record.get('hospitalName'), default='미확인')}`")
            lines.append(f"• 병실: `{_display_value(record.get('roomName'), default='미확인')}`")
            file_names: list[str] = []
            seen: set[str] = set()
            for session in record.get("sessions") or []:
                probe = session.get("probe") if isinstance(session.get("probe"), dict) else None
                if not probe or not probe.get("ok"):
                    continue
                for found_file in probe.get("files") or []:
                    file_name = PurePosixPath(_display_value(found_file, default="")).name
                    if file_name and file_name not in seen:
                        seen.add(file_name)
                        file_names.append(file_name)
            if file_names:
                lines.append(f"• 파일 목록: `{len(file_names)}개`")
                for file_name in file_names:
                    lines.append(f"  - `{file_name}`")
            else:
                record_probe = record.get("deviceProbe") if isinstance(record.get("deviceProbe"), dict) else None
                if record_probe and not record_probe.get("sshReady"):
                    lines.append(
                        f"• 장비 파일 확인: 실패 ({_display_device_probe_reason(record_probe.get('sshReason'))})"
                    )
                else:
                    lines.append("• 파일 목록: `0개`")
        return _truncate_text("\n".join(lines), 38000)

    if compact_file_id:
        file_ids: list[str] = []
        for record in records:
            for session in record.get("sessions") or []:
                file_id = _display_value(session.get("fileId"), default="").strip()
                if file_id and file_id not in file_ids:
                    file_ids.append(file_id)

        if not file_ids:
            return (
                "*fileId 조회 결과*\n"
                f"• 바코드: `{barcode}`\n"
                f"• 날짜: `{log_date}`\n"
                "• fileId: `미추출`"
            )

        if len(file_ids) == 1:
            return (
                "*fileId 조회 결과*\n"
                f"• fileId: `{file_ids[0]}`"
            )

        lines = [
            "*fileId 조회 결과*",
            f"• fileId: `{len(file_ids)}개`",
        ]
        for index, file_id in enumerate(file_ids, start=1):
            lines.append(f"- 세션 {index}: `{file_id}`")
        return "\n".join(lines)

    if compact_download:
        lines = [
            "*장비 영상 다운로드 결과*",
            f"• 바코드: `{barcode}`",
            f"• 날짜: `{log_date}`",
        ]
        if used_expanded_scope:
            lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비까지 확장 검색했어")

        for record in records:
            lines.append("")
            lines.append(f"• 장비: `{_display_value(record.get('deviceName'), default='미확인')}`")
            lines.append(f"• 병원: `{_display_value(record.get('hospitalName'), default='미확인')}`")
            lines.append(f"• 병실: `{_display_value(record.get('roomName'), default='미확인')}`")
            lines.append(f"• 날짜: `{log_date}`")

            file_names: list[str] = []
            seen_files: set[str] = set()
            download_items: list[dict[str, Any]] = []
            download_failures: list[str] = []

            for session in record.get("sessions") or []:
                probe = session.get("probe") if isinstance(session.get("probe"), dict) else None
                if probe and probe.get("ok"):
                    for found_file in probe.get("files") or []:
                        file_name = PurePosixPath(_display_value(found_file, default="")).name
                        if file_name and file_name not in seen_files:
                            seen_files.add(file_name)
                            file_names.append(file_name)

                download = session.get("download") if isinstance(session.get("download"), dict) else None
                if not download:
                    continue
                download_entries = [
                    item
                    for item in (download.get("downloads") or [])
                    if isinstance(item, dict)
                ]
                if download_entries:
                    download_items.extend(download_entries)
                else:
                    failure_reason = _display_device_probe_reason(download.get("reason"))
                    if failure_reason not in download_failures:
                        download_failures.append(failure_reason)

            if file_names:
                lines.append(f"• 장비 파일 목록: `{len(file_names)}개`")
                for file_name in file_names:
                    lines.append(f"  - `{file_name}`")
            else:
                record_probe = record.get("deviceProbe") if isinstance(record.get("deviceProbe"), dict) else None
                if record_probe and not record_probe.get("sshReady"):
                    lines.append(
                        f"• 장비 파일 확인: 실패 ({_display_device_probe_reason(record_probe.get('sshReason'))})"
                    )
                else:
                    lines.append("• 장비 파일 목록: `0개`")

            successful_items = [item for item in download_items if item.get("ok")]
            if successful_items:
                lines.append(f"• 다운로드 링크: `{len(successful_items)}개` (1시간)")
                for item in successful_items:
                    file_name = _display_value(item.get("fileName"), default="파일")
                    url = _display_value(item.get("url"), default="")
                    if url:
                        lines.append(f"  - 🎣 <{url}|{file_name}>")
                    else:
                        lines.append(f"  - 🎣 `{file_name}`")
            elif download_failures:
                lines.append(f"• 다운로드 준비: 실패 ({', '.join(download_failures)})")
            else:
                lines.append("• 다운로드 링크: `0개`")

        return _truncate_text("\n".join(lines), 38000)

    if compact_recovery:
        lines = [
            "*장비 영상 복구 결과*",
            f"• 바코드: `{barcode}`",
            f"• 날짜: `{log_date}`",
        ]
        if used_expanded_scope:
            lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비까지 확장 검색했어")

        for record in records:
            lines.append("")
            lines.append(f"• 장비: `{_display_value(record.get('deviceName'), default='미확인')}`")
            lines.append(f"• 병원: `{_display_value(record.get('hospitalName'), default='미확인')}`")
            lines.append(f"• 병실: `{_display_value(record.get('roomName'), default='미확인')}`")
            lines.append(f"• 날짜: `{log_date}`")

            file_names: list[str] = []
            seen_files: set[str] = set()
            upload_items: list[dict[str, Any]] = []
            seen_uploads: set[str] = set()
            upload_failures: list[str] = []

            for session in record.get("sessions") or []:
                probe = session.get("probe") if isinstance(session.get("probe"), dict) else None
                if probe and probe.get("ok"):
                    for found_file in probe.get("files") or []:
                        file_name = PurePosixPath(_display_value(found_file, default="")).name
                        if file_name and file_name not in seen_files:
                            seen_files.add(file_name)
                            file_names.append(file_name)

                upload = session.get("upload") if isinstance(session.get("upload"), dict) else None
                if not upload:
                    continue
                upload_entries = [
                    item
                    for item in (upload.get("uploads") or [])
                    if isinstance(item, dict)
                ]
                if upload_entries:
                    for item in upload_entries:
                        for file_name in item.get("fileNames") or []:
                            normalized_name = _display_value(file_name, default="")
                            if not normalized_name:
                                continue
                            dedupe_key = f"{normalized_name}|{_display_value(item.get('status'), default='')}"
                            if dedupe_key not in seen_uploads:
                                seen_uploads.add(dedupe_key)
                                upload_items.append(
                                    {
                                        "fileName": normalized_name,
                                        "status": _display_value(item.get("status"), default="unknown"),
                                        "mdaUrl": _display_value(item.get("mdaUrl"), default=""),
                                    }
                                )
                else:
                    failure_reason = _display_device_probe_reason(upload.get("reason"))
                    if failure_reason not in upload_failures:
                        upload_failures.append(failure_reason)

            if file_names:
                lines.append(f"• 장비 파일 목록: `{len(file_names)}개`")
                for file_name in file_names:
                    lines.append(f"  - `{file_name}`")
            else:
                record_probe = record.get("deviceProbe") if isinstance(record.get("deviceProbe"), dict) else None
                if record_probe and not record_probe.get("sshReady"):
                    lines.append(
                        f"• 장비 파일 확인: 실패 ({_display_device_probe_reason(record_probe.get('sshReason'))})"
                    )
                else:
                    lines.append("• 장비 파일 목록: `0개`")

            if upload_items:
                lines.append(f"• 복구 업로드 결과: `{len(upload_items)}개`")
                for item in upload_items:
                    status = _display_value(item.get("status"), default="unknown")
                    lines.append(f"  - `{item['fileName']}` | `{status}`")
                lines.append(f"• MDA URL: 🎣 <https://mda.kr.mmtalkbox.com/cs?search={barcode}|열기>")
            elif upload_failures:
                lines.append(f"• 복구 업로드: 실패 ({', '.join(upload_failures)})")
            else:
                lines.append("• 복구 업로드 결과: `0개`")

        return _truncate_text("\n".join(lines), 38000)

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

            upload = session.get("upload") if isinstance(session.get("upload"), dict) else None
            if upload:
                upload_items = [
                    item
                    for item in (upload.get("uploads") or [])
                    if isinstance(item, dict)
                ]
                if upload_items:
                    lines.append(f"• 복구 업로드 결과: `{len(upload_items)}개`")
                    for item in upload_items:
                        file_names = [name for name in (item.get("fileNames") or []) if _display_value(name, default="")]
                        joined_names = ", ".join(f"`{_display_value(name, default='')}`" for name in file_names) or "`파일`"
                        status = _display_value(item.get("status"), default="unknown")
                        lines.append(f"  - {joined_names} | `{status}`")
                    lines.append(f"• MDA URL: 🎣 <https://mda.kr.mmtalkbox.com/cs?search={barcode}|열기>")
                else:
                    reason = _display_device_probe_reason(upload.get("reason"))
                    lines.append(f"• 복구 업로드: 실패 ({reason})")

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
    compact_file_list: bool = False,
    compact_file_id: bool = False,
    compact_download: bool = False,
    recover_remote_files: bool = False,
    compact_recovery: bool = False,
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
                if recover_remote_files and probe and probe.get("ok"):
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
                            session["upload"] = _upload_device_files_to_uploader(
                                host,
                                port,
                                remote_files,
                                barcode=barcode,
                                device_name=_display_value(record.get("deviceName"), default=""),
                                file_id=file_id,
                                log_date=log_date,
                                started_time=_display_value(session.get("startedRecordingTime"), default=""),
                                added_time=_display_value(session.get("addedRecordingTime"), default=""),
                                spawned_time=_display_value(session.get("spawnedRecordingTime"), default=""),
                                session_start_time=_display_value(session.get("startTime"), default=""),
                            )

    result_text = _render_file_candidate_result(
        barcode=barcode,
        log_date=log_date,
        all_device_contexts=all_device_contexts,
        records=records,
        used_expanded_scope=used_expanded_scope,
        logs_found_any=logs_found_any,
        compact_file_list=compact_file_list,
        compact_file_id=compact_file_id,
        compact_download=compact_download,
        compact_recovery=compact_recovery,
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
            "recoverRemoteFiles": recover_remote_files,
            "compactFileList": compact_file_list,
            "compactFileId": compact_file_id,
            "compactDownload": compact_download,
            "compactRecovery": compact_recovery,
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
    return _build_phase2_scope_request_message(
        barcode,
        reason,
        "*파일 확인 대상 세션 조회 결과*",
        example_action="파일 있나",
    )
