"""장비 health 이벤트 JSONL 기록과 일별 S3 보관의 채널 중립 정본."""

from __future__ import annotations

from datetime import datetime, timedelta
import gzip
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import stat
import tempfile
import threading
from typing import Any, Mapping

from boxer.retrieval.connectors.s3 import _build_s3_client
from boxer_company import settings as company_settings
from boxer_company.daily_device_round import _coerce_daily_device_round_now


_EVENT_LOG_PATTERN = re.compile(
    r"^device_health_monitor_events-(\d{4}-\d{2}-\d{2})\.jsonl$"
)
_ARCHIVE_ATTEMPT_LOCK = threading.Lock()
_ARCHIVE_ATTEMPT_DATE: str = ""
_ARCHIVE_THREAD: threading.Thread | None = None


def device_health_event_log_dir() -> Path:
    return Path(
        company_settings.DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR
    ).expanduser()


def device_health_event_log_path(now: datetime) -> Path:
    local_now = _coerce_daily_device_round_now(now)
    return device_health_event_log_dir() / (
        f"device_health_monitor_events-{local_now.date().isoformat()}.jsonl"
    )


def append_device_health_monitor_event(
    event_type: str,
    payload: Mapping[str, Any],
    *,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
    log_path: Path | None = None,
) -> bool:
    """legacy와 같은 일별 JSONL 한 줄을 best-effort로 추가한다."""

    local_now = _coerce_daily_device_round_now(now)
    event_payload = {
        "eventType": str(event_type or "unknown").strip() or "unknown",
        "createdAt": local_now.isoformat(),
        **dict(payload),
    }
    path = log_path or device_health_event_log_path(local_now)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as event_log:
            event_log.write(
                json.dumps(
                    event_payload,
                    ensure_ascii=True,
                    sort_keys=True,
                    default=str,
                )
                + "\n"
            )
        return True
    except Exception:
        if logger is not None:
            logger.warning(
                "장비 상태 모니터 이벤트 로그를 저장하지 못했어: %s",
                path,
                exc_info=True,
            )
        return False


def _event_archive_key(source_path: Path) -> str:
    match = _EVENT_LOG_PATTERN.fullmatch(source_path.name)
    if match is None:
        raise ValueError(
            f"장비 상태 이벤트 로그 파일명이 아니야: {source_path.name}"
        )
    source_date = datetime.strptime(match.group(1), "%Y-%m-%d").date()
    prefix = str(
        company_settings.DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX
        or ""
    ).strip().strip("/")
    relative_key = f"{source_date:%Y/%m}/{source_path.name}.gz"
    return f"{prefix}/{relative_key}" if prefix else relative_key


def _event_archive_metadata(
    source_path: Path,
    *,
    source_size: int,
    source_sha256: str,
    compressed_size: int,
    compressed_sha256: str,
) -> dict[str, str]:
    match = _EVENT_LOG_PATTERN.fullmatch(source_path.name)
    return {
        "source-date": match.group(1) if match is not None else "",
        "source-filename": source_path.name,
        "source-sha256": source_sha256,
        "source-size": str(source_size),
        "compressed-size": str(compressed_size),
        "compressed-sha256": compressed_sha256,
    }


def _archive_matches(
    head_response: Mapping[str, Any],
    *,
    metadata: Mapping[str, str],
    compressed_size: int,
) -> bool:
    remote_metadata = (
        head_response.get("Metadata")
        if isinstance(head_response.get("Metadata"), Mapping)
        else {}
    )
    return (
        max(0, int(head_response.get("ContentLength") or 0))
        == compressed_size
        and all(
            str(remote_metadata.get(key) or "") == value
            for key, value in metadata.items()
        )
    )


def _load_archive_head(
    s3_client: Any,
    *,
    bucket: str,
    key: str,
) -> dict[str, Any] | None:
    try:
        response = s3_client.head_object(Bucket=bucket, Key=key)
    except Exception as exc:
        error_payload = getattr(exc, "response", {})
        error = (
            error_payload.get("Error")
            if isinstance(error_payload, Mapping)
            else {}
        )
        error_code = (
            str(error.get("Code") or "")
            if isinstance(error, Mapping)
            else ""
        )
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise
    return dict(response) if isinstance(response, Mapping) else {}


def _put_archive_object_create_only(
    s3_client: Any,
    *,
    body: Any,
    put_parameters: Mapping[str, Any],
) -> Any:
    """구형 botocore에서도 S3 create-only 헤더를 서명해 전송한다."""

    client_meta = getattr(s3_client, "meta", None)
    client_events = getattr(client_meta, "events", None)
    if client_events is None:
        # 단순 test double은 botocore event bus가 없으므로 조건을
        # 명시적인 인자로 전달한다. 실제 boto3 client는 아래 raw-header
        # 경로만 쓴다.
        return s3_client.put_object(
            **dict(put_parameters),
            IfNoneMatch="*",
        )

    event_name = "before-call.s3.PutObject"

    def _add_create_only_header(
        *,
        params: dict[str, Any],
        **_: Any,
    ) -> None:
        # botocore 1.34의 PutObject model에는 IfNoneMatch가 없어 API
        # 인자로 주면 validation에서 막힌다. 직렬화가 끝난 해당 body의
        # 요청에만 raw header를 넣으면 SigV4 서명과 retry에 포함된다.
        if params.get("body") is not body:
            return
        headers = params.setdefault("headers", {})
        headers["If-None-Match"] = "*"

    client_events.register_first(event_name, _add_create_only_header)
    try:
        return s3_client.put_object(**dict(put_parameters))
    finally:
        # 공유 client의 다른 PutObject에는 create-only 조건이 새지 않게
        # 성공하거나 예외가 나도 이 요청 전용 handler를 제거한다.
        client_events.unregister(event_name, handler=_add_create_only_header)


def _event_source_identity(
    source_path: Path,
) -> tuple[int, int, int, int]:
    source_stat = source_path.lstat()
    if not stat.S_ISREG(source_stat.st_mode):
        raise ValueError(
            "일반 파일이 아닌 장비 상태 이벤트 로그는 보관하지 않아"
        )
    return (
        source_stat.st_dev,
        source_stat.st_ino,
        source_stat.st_size,
        source_stat.st_mtime_ns,
    )


def _file_identity(
    file_stat: os.stat_result,
) -> tuple[int, int, int, int]:
    return (
        file_stat.st_dev,
        file_stat.st_ino,
        file_stat.st_size,
        file_stat.st_mtime_ns,
    )


def _compress_event_log(
    source_path: Path,
) -> tuple[Any, str, int, str, tuple[int, int, int, int]]:
    source_identity = _event_source_identity(source_path)
    source_digest = hashlib.sha256()
    compressed_file = tempfile.SpooledTemporaryFile(
        mode="w+b",
        max_size=8 * 1024 * 1024,
    )
    try:
        with source_path.open("rb") as source_file:
            if _file_identity(os.fstat(source_file.fileno())) != source_identity:
                raise RuntimeError(
                    "압축 직전에 장비 상태 이벤트 로그가 변경됐어"
                )
            # mtime과 원본 경로를 gzip header에서 제거해 같은 원본은
            # 항상 같은 archive body가 되게 한다.
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=compressed_file,
                mtime=0,
            ) as gzip_file:
                while chunk := source_file.read(1024 * 1024):
                    source_digest.update(chunk)
                    gzip_file.write(chunk)
            if _file_identity(os.fstat(source_file.fileno())) != source_identity:
                raise RuntimeError(
                    "압축하는 동안 장비 상태 이벤트 로그가 변경됐어"
                )
        if _event_source_identity(source_path) != source_identity:
            raise RuntimeError(
                "압축한 뒤 장비 상태 이벤트 로그가 변경됐어"
            )
        compressed_size = compressed_file.tell()
        compressed_digest = hashlib.sha256()
        compressed_file.seek(0)
        while chunk := compressed_file.read(1024 * 1024):
            compressed_digest.update(chunk)
        compressed_file.seek(0)
    except Exception:
        compressed_file.close()
        raise
    return (
        compressed_file,
        source_digest.hexdigest(),
        compressed_size,
        compressed_digest.hexdigest(),
        source_identity,
    )


def archive_device_health_monitor_event_logs(
    *,
    now: datetime | None = None,
    log_dir: Path | None = None,
    s3_client: Any | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    """retention 밖 JSONL을 조건부 PUT과 metadata 검증으로 S3에 옮긴다."""

    local_now = _coerce_daily_device_round_now(now)
    actual_log_dir = log_dir or device_health_event_log_dir()
    retention_days = max(
        1,
        int(
            company_settings.DEVICE_HEALTH_MONITOR_EVENT_LOG_RETENTION_DAYS
        ),
    )
    first_kept_date = local_now.date() - timedelta(
        days=retention_days - 1
    )
    bucket = str(
        company_settings.DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET
        or ""
    ).strip()
    result: dict[str, Any] = {
        "enabled": bool(bucket),
        "bucket": bucket,
        "firstKeptDate": first_kept_date.isoformat(),
        "archivedCount": 0,
        "archivedSourceBytes": 0,
        "archivedCompressedBytes": 0,
        "keptCount": 0,
        "failedCount": 0,
        "failures": [],
    }
    if not bucket or not actual_log_dir.exists():
        return result

    archive_candidates: list[Path] = []
    for source_path in sorted(
        actual_log_dir.glob("device_health_monitor_events-*.jsonl")
    ):
        match = _EVENT_LOG_PATTERN.fullmatch(source_path.name)
        if match is None or source_path.is_symlink():
            continue
        try:
            source_date = datetime.strptime(
                match.group(1),
                "%Y-%m-%d",
            ).date()
        except ValueError:
            continue
        if source_date >= first_kept_date:
            result["keptCount"] += 1
            continue
        archive_candidates.append(source_path)

    if not archive_candidates:
        return result
    try:
        client = s3_client or _build_s3_client()
    except Exception as exc:
        result["failedCount"] = len(archive_candidates)
        result["failures"] = [
            {"file": path.name, "errorType": type(exc).__name__}
            for path in archive_candidates
        ]
        if logger is not None:
            logger.warning(
                "장비 상태 이벤트 로그 S3 client를 만들지 못했어",
                exc_info=True,
            )
        return result

    for source_path in archive_candidates:
        compressed_file: Any | None = None
        try:
            (
                compressed_file,
                source_sha256,
                compressed_size,
                compressed_sha256,
                source_identity,
            ) = _compress_event_log(source_path)
            source_size = source_identity[2]
            metadata = _event_archive_metadata(
                source_path,
                source_size=source_size,
                source_sha256=source_sha256,
                compressed_size=compressed_size,
                compressed_sha256=compressed_sha256,
            )
            key = _event_archive_key(source_path)
            try:
                # ListBucket 없는 최소 IAM에서는 missing HeadObject가 403이다.
                # create-only 조건부 PUT을 먼저 사용해 기존 key를 덮지 않고,
                # 성공·timeout·412 모두 아래 HEAD metadata로 최종 판정한다.
                compressed_file.seek(0)
                _put_archive_object_create_only(
                    client,
                    body=compressed_file,
                    put_parameters={
                        "Bucket": bucket,
                        "Key": key,
                        "Body": compressed_file,
                        "ContentLength": compressed_size,
                        "ContentType": "application/x-ndjson",
                        "ContentEncoding": "gzip",
                        "ServerSideEncryption": "AES256",
                        "Metadata": metadata,
                    },
                )
            except Exception as put_exc:
                existing_head = _load_archive_head(
                    client,
                    bucket=bucket,
                    key=key,
                )
                if existing_head is None:
                    raise put_exc
                if not _archive_matches(
                    existing_head,
                    metadata=metadata,
                    compressed_size=compressed_size,
                ):
                    raise RuntimeError(
                        "같은 S3 key에 다른 장비 상태 이벤트 로그가 이미 있어"
                    ) from put_exc

            uploaded_head = _load_archive_head(
                client,
                bucket=bucket,
                key=key,
            )
            if uploaded_head is None or not _archive_matches(
                uploaded_head,
                metadata=metadata,
                compressed_size=compressed_size,
            ):
                raise RuntimeError(
                    "S3에 보관한 장비 상태 이벤트 로그 검증에 실패했어"
                )

            if _event_source_identity(source_path) != source_identity:
                raise RuntimeError(
                    "S3 보관 후 장비 상태 이벤트 로그가 변경돼 원본을 유지해"
                )
            source_path.unlink()
            result["archivedCount"] += 1
            result["archivedSourceBytes"] += source_size
            result["archivedCompressedBytes"] += compressed_size
        except Exception as exc:
            result["failedCount"] += 1
            result["failures"].append(
                {
                    "file": source_path.name,
                    "errorType": type(exc).__name__,
                }
            )
            if logger is not None:
                logger.warning(
                    "장비 상태 이벤트 로그를 S3에 보관하지 못했어: %s",
                    source_path,
                    exc_info=True,
                )
        finally:
            if compressed_file is not None:
                compressed_file.close()
    return result




def start_device_health_monitor_event_archive_once(
    *,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> bool:
    """legacy loop와 같이 KST 날짜당 한 daemon archive worker만 띄운다."""

    global _ARCHIVE_ATTEMPT_DATE, _ARCHIVE_THREAD
    local_now = _coerce_daily_device_round_now(now)
    attempt_date = local_now.date().isoformat()
    with _ARCHIVE_ATTEMPT_LOCK:
        if _ARCHIVE_ATTEMPT_DATE == attempt_date:
            return False
        if _ARCHIVE_THREAD is not None and _ARCHIVE_THREAD.is_alive():
            return False
        _ARCHIVE_ATTEMPT_DATE = attempt_date

        def _archive() -> None:
            try:
                result = archive_device_health_monitor_event_logs(
                    now=local_now,
                    logger=logger,
                )
                if logger is not None:
                    logger.info(
                        "Device health event archive completed bucket=%s "
                        "kept=%s archived=%s failed=%s sourceBytes=%s "
                        "compressedBytes=%s",
                        result.get("bucket"),
                        result.get("keptCount"),
                        result.get("archivedCount"),
                        result.get("failedCount"),
                        result.get("archivedSourceBytes"),
                        result.get("archivedCompressedBytes"),
                    )
            except Exception:
                if logger is not None:
                    logger.exception(
                        "장비 상태 이벤트 로그 보관 중 오류가 발생했어"
                    )

        _ARCHIVE_THREAD = threading.Thread(
            target=_archive,
            name="boxer-device-health-event-archive",
            daemon=True,
        )
        _ARCHIVE_THREAD.start()
    return True


__all__ = [
    "append_device_health_monitor_event",
    "archive_device_health_monitor_event_logs",
    "device_health_event_log_dir",
    "device_health_event_log_path",
    "start_device_health_monitor_event_archive_once",
]
