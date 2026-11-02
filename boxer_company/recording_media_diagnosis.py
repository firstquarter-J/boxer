"""중앙 DB가 식별한 업로드 MP4의 영상 디코딩만 읽기 전용으로 재검사한다."""

from __future__ import annotations

import json
import math
import os
import re
import selectors
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as core_settings
from boxer.retrieval.connectors.db import _create_db_connection
from boxer.retrieval.connectors.s3 import _load_boto3_components
from boxer_company import settings
from boxer_company.recording_audio_summary import normalize_recording_audio_details
from boxer_company.recording_media_timeline import bounded_seconds
from boxer_company.recording_video_summary import normalize_recording_video_details
from boxer_company.routers.box_db import _build_recordings_barcode_match

_MAX_BYTES = 512 * 1024 * 1024
_MAX_DURATION_SEC = 2 * 60 * 60
_DOWNLOAD_SEC = 60
_PROBE_SEC = 15
_DECODE_SEC = 120
_MAX_OUTPUT_BYTES = 64 * 1024
_CHECK_LOCK = threading.Lock()
_FILE_ID = re.compile(r"[A-Za-z0-9_-]{1,80}")
_BARCODE = re.compile(r"[0-9]{11}")
_ISSUES = frozenset(
    {
        "video_file_missing",
        "video_metadata_unreadable",
        "video_stream_missing",
        "video_decode_failed",
        "video_no_decodable_frames",
        "video_frozen",
        "video_uniform_color",
        "audio_stream_missing",
        "audio_decode_failed",
        "audio_duration_mismatch",
    }
)


class _InspectionUnavailable(Exception):
    """원문 exception 대신 허용된 실패 사유만 응답 경계로 전달한다."""


def diagnose_recording_media(
    *,
    notification_id: int | None = None,
    barcode: str | None = None,
    device_name: str | None = None,
    log_date: str | None = None,
    file_id: str | None = None,
    recorded_at: str | None = None,
) -> dict[str, Any]:
    # 현재 단일 API worker 안에서 큰 파일 검사 한 건만 실행한다.
    # SSH, 복원, 업로드와 Slack 발송은 이 경로에서 실행하지 않는다.
    if not _CHECK_LOCK.acquire(blocking=False):
        return {"status": "unavailable", "reason": "inspection_busy"}
    evidence: dict[str, Any] = {}
    try:
        target = _load_media_target(
            notification_id=notification_id,
            barcode=barcode,
            device_name=device_name,
            log_date=log_date,
            file_id=file_id,
            recorded_at=recorded_at,
        )
        evidence = {
            key: value for key, value in target.items() if not key.startswith("_")
        }
        if target["status"] != "ready":
            return evidence
        ffmpeg, ffprobe = shutil.which("ffmpeg"), shutil.which("ffprobe")
        if not ffmpeg or not ffprobe:
            raise _InspectionUnavailable("media_tools_missing")
        # TemporaryDirectory는 0700으로 만들고 어떤 종료 경로에서도 파일을 제거한다.
        with tempfile.TemporaryDirectory(prefix="boxer-media-check-") as directory:
            media_path = Path(directory) / "recording.mp4"
            _download_media(target, media_path)
            duration = _probe_video(ffprobe, media_path)
            evidence["durationSec"] = duration
            strict = _decode_video(ffmpeg, media_path, duration, strict=True)
            evidence["strictDecode"] = strict
            findings: list[str] = []
            if strict["status"] == "completed":
                findings.append("video_decode_clean")
            elif strict["status"] == "decode_error":
                if strict["diagnosticCode"] in {
                    "h264_corrupt_frame",
                    "video_corrupt_frame",
                }:
                    findings.append("partial_frame_corruption")
                normal = _decode_video(ffmpeg, media_path, duration, strict=False)
                evidence["normalDecode"] = normal
                if normal["status"] == "completed":
                    findings.append("strict_decode_failed_normal_completed")
            else:
                findings.append("decode_inconclusive")
            return {
                **evidence,
                "status": "analyzed",
                "reason": None,
                "findings": findings,
            }
    except _InspectionUnavailable as exc:
        return {**evidence, "status": "unavailable", "reason": str(exc)}
    except Exception:  # noqa: BLE001 - provider 원문은 이 응답 경계에서 제거한다.
        # DB/SDK/프로세스 원문에는 경로나 credential이 있을 수 있어 반환하지 않는다.
        return {**evidence, "status": "unavailable", "reason": "inspection_failed"}
    finally:
        _CHECK_LOCK.release()


def _load_media_target(
    *,
    notification_id: int | None,
    barcode: str | None,
    device_name: str | None,
    log_date: str | None,
    file_id: str | None,
    recorded_at: str | None = None,
) -> dict[str, Any]:
    if (
        notification_id is not None
        and (type(notification_id) is not int or notification_id < 1)
        or barcode is not None
        and (not isinstance(barcode, str) or not _BARCODE.fullmatch(barcode))
        or file_id is not None
        and (not isinstance(file_id, str) or not _FILE_ID.fullmatch(file_id))
        or device_name is not None
        and (
            not isinstance(device_name, str)
            or not settings.S3_DEVICE_NAME_PATTERN.fullmatch(device_name)
        )
    ):
        return {"status": "needs_input", "reason": "invalid_target"}
    if notification_id is None and barcode is None:
        return {"status": "needs_input", "reason": "target_required"}
    date_bounds = None
    session_bounds = None
    if log_date is not None:
        try:
            start = datetime.strptime(log_date, "%Y-%m-%d").replace(
                tzinfo=ZoneInfo("Asia/Seoul")
            )
        except (ValueError, TypeError):
            return {"status": "needs_input", "reason": "invalid_target"}
        date_bounds = tuple(
            value.astimezone(UTC).replace(tzinfo=None)
            for value in (start, start + timedelta(days=1))
        )
    if recorded_at is not None:
        try:
            # 카드에 표시한 KST 세션 시작의 초 단위 범위만 사용한다.
            session_start = datetime.strptime(recorded_at, "%Y-%m-%d %H:%M:%S").replace(
                tzinfo=ZoneInfo("Asia/Seoul")
            )
        except (ValueError, TypeError):
            return {"status": "needs_input", "reason": "invalid_target"}
        session_bounds = tuple(
            value.astimezone(UTC).replace(tzinfo=None)
            for value in (session_start, session_start + timedelta(seconds=1))
        )
    if not core_settings.DB_QUERY_ENABLED:
        raise _InspectionUnavailable("db_disabled")
    evidence: dict[str, Any] = {"notificationIssues": [], "notificationDetails": {}}
    connection = _create_db_connection(core_settings.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            notification: dict[str, Any] = {}
            if notification_id is not None:
                # 알림 PK로 원본을 다시 읽는다. 메시지/클라이언트의 S3 주소를 신뢰하지 않는다.
                cursor.execute(
                    "SELECT id, deviceSeq, deviceName, barcode, fileId, details "
                    "FROM device_notification WHERE id = %s AND code = %s LIMIT 1",
                    (notification_id, "recording_media_quality_issue"),
                )
                notification = cursor.fetchone() or {}
                if not notification:
                    return {"status": "not_found", "reason": "notification_not_found"}
                for requested, actual in (
                    (barcode, notification.get("barcode")),
                    (file_id, notification.get("fileId")),
                    (device_name, notification.get("deviceName")),
                ):
                    if requested is not None and requested != str(actual or ""):
                        return {"status": "needs_input", "reason": "target_conflict"}
                barcode = str(notification.get("barcode") or "")
                file_id = str(notification.get("fileId") or "")
                if not _BARCODE.fullmatch(barcode) or not _FILE_ID.fullmatch(file_id):
                    return {
                        "status": "unavailable",
                        "reason": "invalid_notification_target",
                    }
                evidence.update(_notification_evidence(notification))
            # fileId 단독 전체 스캔을 피하고 기존 바코드 인덱스 계약을 재사용한다.
            barcode_match, barcode_params = _build_recordings_barcode_match(barcode)
            conditions = [barcode_match, "r.deleteFlag = 0"]
            params: list[Any] = list(barcode_params)
            for clause, value in (
                ("r.fileId = %s", file_id),
                ("d.deviceName = %s", device_name),
            ):
                if value is not None:
                    conditions.append(clause)
                    params.append(value)
            if date_bounds:
                conditions.extend(["r.recordedAt >= %s", "r.recordedAt < %s"])
                params.extend(date_bounds)
            if session_bounds:
                conditions.extend(["r.recordedAt >= %s", "r.recordedAt < %s"])
                params.extend(session_bounds)
            cursor.execute(
                "SELECT r.seq, r.deviceSeq, r.fileId, r.s3Bucket, r.s3FileKey, "
                "r.streamingStatus, d.deviceName FROM recordings r "
                "LEFT JOIN devices d ON d.seq = r.deviceSeq "
                f"WHERE {' AND '.join(conditions)} ORDER BY r.seq DESC LIMIT 2",
                tuple(params),
            )
            rows = list(cursor.fetchall() or [])
            if not notification and len(rows) == 1:
                # 알림 ID 없는 카드도 확정한 같은 장비/바코드/파일의 원본만 읽는다.
                # 부가 알림 조회 실패가 이미 찾은 업로드 파일 검사를 막지는 않는다.
                try:
                    cursor.execute(
                        "SELECT id, deviceSeq, deviceName, barcode, fileId, details "
                        "FROM device_notification WHERE deviceSeq = %s AND barcode = %s "
                        "AND fileId = %s AND code = %s ORDER BY id DESC LIMIT 1",
                        (
                            rows[0].get("deviceSeq"),
                            barcode,
                            rows[0].get("fileId"),
                            "recording_media_quality_issue",
                        ),
                    )
                    original = cursor.fetchone() or {}
                    if type(original.get("id")) is int:
                        evidence.update(_notification_evidence(original))
                except Exception:  # noqa: BLE001 - 부가 근거만 제외하고 파일 검사는 유지한다.
                    evidence["notificationLookupStatus"] = "unavailable"
    finally:
        connection.close()
    if not rows:
        return {**evidence, "status": "not_found", "reason": "recording_not_found"}
    if len(rows) != 1:
        return {**evidence, "status": "needs_input", "reason": "ambiguous_recording"}
    row = rows[0]
    if notification and notification.get("deviceSeq") != row.get("deviceSeq"):
        return {
            **evidence,
            "status": "unavailable",
            "reason": "recording_device_mismatch",
        }
    resolved_file_id = str(row.get("fileId") or "")
    resolved_device = str(row.get("deviceName") or "")
    if not _FILE_ID.fullmatch(
        resolved_file_id
    ) or not settings.S3_DEVICE_NAME_PATTERN.fullmatch(resolved_device):
        return {
            **evidence,
            "status": "unavailable",
            "reason": "invalid_recording_target",
        }
    evidence.update(
        {"fileId": resolved_file_id, "barcode": barcode, "deviceName": resolved_device}
    )
    if str(row.get("streamingStatus") or "").upper() != "AVAILABLE":
        return {**evidence, "status": "unavailable", "reason": "upload_not_ready"}
    bucket, key = str(row.get("s3Bucket") or ""), str(row.get("s3FileKey") or "")
    owner = settings.S3_ULTRASOUND_BUCKET_OWNER_ID
    if (
        not settings.S3_ULTRASOUND_BUCKET
        or bucket != settings.S3_ULTRASOUND_BUCKET
        or not _FILE_ID.fullmatch(resolved_file_id)
        or key != f"{barcode}/{resolved_file_id}.mp4"
        or not re.fullmatch(r"[0-9]{12}", owner)
    ):
        return {**evidence, "status": "unavailable", "reason": "invalid_s3_location"}
    return {
        **evidence,
        "status": "ready",
        "_bucket": bucket,
        "_key": key,
        "_owner": owner,
    }


def _notification_evidence(notification: dict[str, Any]) -> dict[str, Any]:
    details = notification.get("details")
    if isinstance(details, str):
        try:
            details = json.loads(details)
        except ValueError:
            details = {}
    details = details if isinstance(details, dict) else {}
    raw_issues = details.get("issues")
    issues = (
        [value for value in raw_issues if isinstance(value, str) and value in _ISSUES]
        if isinstance(raw_issues, list)
        else []
    )
    # 이미 알림이 사용하는 숫자/원인 allowlist를 재사용해 임의 원문을 버린다.
    safe_details = {
        **normalize_recording_video_details(details),
        **normalize_recording_audio_details(details),
        "issues": issues,
        "mediaType": "audio" if details.get("mediaType") == "audio" else "video",
    }
    return {
        "notificationId": notification["id"],
        "notificationIssues": issues,
        "notificationDetails": safe_details,
    }


def _download_media(target: dict[str, Any], path: Path) -> None:
    # socket timeout은 수신 사이의 대기만 제한한다. 별도 프로세스로 내려받아
    # 데이터가 조금씩 계속 도착해도 부모의 전체 시간 한도를 넘기지 않는다.
    payload = json.dumps(
        {
            "target": {key: target[key] for key in ("_bucket", "_key", "_owner")},
            "path": str(path),
        }
    ).encode("utf-8")
    process = subprocess.Popen(
        [sys.executable, "-m", "boxer_company.recording_media_diagnosis"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        shell=False,
    )
    try:
        try:
            output, _ = process.communicate(input=payload, timeout=_DOWNLOAD_SEC)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate(timeout=5)
            raise _InspectionUnavailable("download_timeout") from None
        if process.returncode != 0 or len(output) > 1024:
            raise _InspectionUnavailable("media_read_failed")
        response = json.loads(output)
        reason = response.get("reason")
        if reason in {
            "s3_disabled",
            "media_size_limit",
            "download_timeout",
            "download_incomplete",
            "media_read_failed",
        }:
            raise _InspectionUnavailable(reason)
        if response != {"status": "downloaded"}:
            raise _InspectionUnavailable("media_read_failed")
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)


def _download_media_in_process(target: dict[str, Any], path: Path) -> None:
    if not core_settings.S3_QUERY_ENABLED:
        raise _InspectionUnavailable("s3_disabled")
    # 일반 S3 조회와 다른 실행 예산이므로 공통 SDK loader를 사용하되 재시도와
    # socket 대기는 이 검사 전용 정책으로 제한한다. credential chain은 동일하다.
    boto3, config_type = _load_boto3_components()
    client = boto3.client(
        "s3",
        region_name=core_settings.AWS_REGION,
        config=config_type(
            region_name=core_settings.AWS_REGION,
            connect_timeout=5,
            read_timeout=5,
            retries={"total_max_attempts": 1, "mode": "standard"},
        ),
    )
    body = None
    deadline = time.monotonic() + _DOWNLOAD_SEC
    try:
        response = client.get_object(
            Bucket=target["_bucket"],
            Key=target["_key"],
            ExpectedBucketOwner=target["_owner"],
        )
        body = response["Body"]
        length = response.get("ContentLength")
        if type(length) is not int or not 0 < length <= _MAX_BYTES:
            raise _InspectionUnavailable("media_size_limit")
        total = 0
        with path.open("xb") as stream:
            os.chmod(path, 0o600)
            # Content-Length를 채우면 urllib3가 socket을 정리하므로 EOF 확인을
            # 위해 timeout을 다시 설정하지 않는다. 한 바이트 여유로 초과도 감지한다.
            while total < length:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _InspectionUnavailable("download_timeout")
                body.set_socket_timeout(min(5, remaining))
                chunk = body.read(min(1024 * 1024, length - total + 1))
                if not chunk:
                    break
                total += len(chunk)
                if total > _MAX_BYTES or total > length:
                    raise _InspectionUnavailable("media_size_limit")
                stream.write(chunk)
        if total != length:
            raise _InspectionUnavailable("download_incomplete")
    except _InspectionUnavailable:
        raise
    except Exception:  # noqa: BLE001 - SDK 원문과 주소를 응답에 내보내지 않는다.
        # 403, 보관 객체, 없는 객체, 네트워크 실패를 손상으로 분류하지 않는다.
        raise _InspectionUnavailable("media_read_failed") from None
    finally:
        if body is not None:
            body.close()
        client.close()


def _input_args(path: Path) -> list[str]:
    # MP4 외 포맷과 원격 protocol/외부 참조를 열지 않는다.
    return [
        "-protocol_whitelist",
        "file",
        "-f",
        "mov",
        "-enable_drefs",
        "0",
        "-use_absolute_path",
        "0",
        "-i",
        str(path),
    ]


def _probe_video(binary: str, path: Path) -> float:
    result = _run_process(
        [
            binary,
            "-hide_banner",
            "-v",
            "error",
            "-max_alloc",
            "67108864",
            "-threads",
            "1",
            *_input_args(path),
            "-select_streams",
            "v:0",
            "-show_entries",
            "stream=width,height,duration:format=duration",
            "-of",
            "json",
        ],
        _PROBE_SEC,
    )
    if result["reason"] or result["returncode"] != 0:
        raise _InspectionUnavailable(result["reason"] or "media_probe_failed")
    try:
        payload = json.loads(result["stdout"])
        stream = payload["streams"][0]
        width, height = int(stream["width"]), int(stream["height"])
        duration = float(stream.get("duration") or payload["format"]["duration"])
    except (ValueError, TypeError, KeyError, IndexError):
        raise _InspectionUnavailable("video_metadata_unavailable") from None
    if (
        not math.isfinite(duration)
        or not 0 < duration <= _MAX_DURATION_SEC
        or not 0 < width <= 4096
        or not 0 < height <= 2160
    ):
        raise _InspectionUnavailable("media_limits_exceeded")
    return duration


def _decode_video(
    binary: str, path: Path, duration: float, *, strict: bool
) -> dict[str, Any]:
    args = [
        binary,
        "-hide_banner",
        "-nostdin",
        "-nostats",
        "-loglevel",
        "warning",
        "-max_alloc",
        "67108864",
        "-threads",
        "1",
        "-filter_threads",
        "1",
        "-progress",
        "pipe:1",
    ]
    if strict:
        args.append("-xerror")
    args.extend(
        [*_input_args(path), "-map", "0:v:0", "-an", "-sn", "-dn", "-f", "null", "-"]
    )
    result = _run_process(args, _DECODE_SEC)
    progress = {}
    for line in result["stdout"].splitlines():
        key, _, value = line.partition("=")
        if key in {"frame", "out_time_us", "progress"}:
            progress[key] = value
    try:
        last = bounded_seconds(
            float(progress["out_time_us"]) / 1_000_000, maximum=_MAX_DURATION_SEC
        )
        frames = int(progress.get("frame", 0))
    except (KeyError, ValueError, TypeError):
        last, frames = None, 0
    status = "unknown"
    diagnostic = result["diagnosticCode"]
    if not result["reason"] and result["returncode"] == 0:
        if (
            (not strict or diagnostic is None)
            and diagnostic
            not in {
                "permission_denied",
                "unsupported_codec",
                "input_missing",
                "io_error",
            }
            and progress.get("progress") == "end"
            and frames > 0
            and last is not None
            and last >= duration - 0.05
        ):
            status = "completed"
    elif (
        not result["reason"]
        and result["returncode"] > 0
        and diagnostic
        in {"h264_corrupt_frame", "video_corrupt_frame", "invalid_media_data"}
    ):
        status = "decode_error"
    return {
        "status": status,
        "lastDecodedSec": last,
        "diagnosticCode": diagnostic,
        "reason": result["reason"],
        "decodedFrames": frames,
    }


def _diagnostic_code(text: str) -> str | None:
    # 원문 stderr는 저장하지 않고 환경 오류를 손상과 분리한 고정 코드만 남긴다.
    for words, code in (
        (r"permission denied", "permission_denied"),
        (r"no such file", "input_missing"),
        (r"unknown decoder|decoder .*not found|unsupported codec", "unsupported_codec"),
        (r"input/output error", "io_error"),
        (
            r"error while decoding MB|corrupt decoded frame|concealing .* errors in .* frame",
            "video_corrupt_frame",
        ),
        (
            r"invalid data found when processing input|invalid NAL unit",
            "invalid_media_data",
        ),
    ):
        if re.search(words, text, re.IGNORECASE):
            # codec를 명시한 decoder 로그가 있을 때만 H.264로 좁힌다.
            if code == "video_corrupt_frame" and re.search(
                r"\[h264(?:\s|@|\])", text, re.IGNORECASE
            ):
                return "h264_corrupt_frame"
            return code
    return None


def _run_process(args: list[str], timeout: float) -> dict[str, Any]:
    stdout = bytearray()
    diagnostic = None
    reason = None
    process = subprocess.Popen(
        args,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        shell=False,
    )
    deadline = time.monotonic() + timeout
    # 두 pipe를 동시에 비우고 stderr는 작은 rolling window만 검사한다.
    # 오류가 반복돼도 메모리나 임시 디스크를 로그 크기만큼 사용하지 않는다.
    stderr_tail = ""
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ, "stdout")
            selector.register(process.stderr, selectors.EVENT_READ, "stderr")
            while selector.get_map():
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    reason = "media_process_timeout"
                    break
                for key, _ in selector.select(min(remaining, 0.25)):
                    chunk = os.read(key.fileobj.fileno(), 8192)
                    if not chunk:
                        selector.unregister(key.fileobj)
                    elif key.data == "stdout":
                        stdout.extend(chunk)
                        if len(stdout) > _MAX_OUTPUT_BYTES:
                            reason = "media_output_limit"
                            break
                    else:
                        stderr_tail = (
                            stderr_tail + chunk.decode("utf-8", errors="replace")
                        )[-16384:]
                        code = _diagnostic_code(stderr_tail)
                        if code and (
                            diagnostic is None
                            or diagnostic == "invalid_media_data"
                            or diagnostic == "video_corrupt_frame"
                            and code == "h264_corrupt_frame"
                            or code
                            in {
                                "permission_denied",
                                "input_missing",
                                "unsupported_codec",
                                "io_error",
                            }
                        ):
                            diagnostic = code
                if reason:
                    break
        if reason:
            process.kill()
        try:
            returncode = process.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            process.kill()
            returncode = process.wait(timeout=5)
            reason = "media_process_timeout"
        return {
            "stdout": stdout[:_MAX_OUTPUT_BYTES].decode("utf-8", errors="replace"),
            "diagnosticCode": diagnostic,
            "returncode": returncode,
            "reason": reason,
        }
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        process.stdout.close()
        process.stderr.close()


def _download_worker() -> None:
    # stdin은 부모가 DB로 확정한 exact 객체와 임시 경로만 전달한다. worker는
    # 작은 상태 JSON 외에는 출력하지 않고 SDK/파일 원문 예외도 버린다.
    try:
        payload = json.loads(sys.stdin.buffer.read(16 * 1024))
        _download_media_in_process(payload["target"], Path(payload["path"]))
        result = {"status": "downloaded"}
    except _InspectionUnavailable as exc:
        result = {"status": "unavailable", "reason": str(exc)}
    except Exception:  # noqa: BLE001 - 부모에게 원문 exception을 전달하지 않는다.
        result = {"status": "unavailable", "reason": "media_read_failed"}
    sys.stdout.write(json.dumps(result))


if __name__ == "__main__":
    _download_worker()
