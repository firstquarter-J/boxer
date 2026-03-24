from __future__ import annotations

import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.retrieval.connectors.s3 import _build_s3_client


def _resolve_sqlite_path(raw_path: str | Path) -> Path:
    path = Path(raw_path).expanduser()
    if not path.is_absolute():
        path = s.PROJECT_ROOT / path
    return path.resolve()


def _ensure_sqlite_parent_dir(db_path: str | Path) -> Path:
    resolved_path = _resolve_sqlite_path(db_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return resolved_path


def _connect_sqlite(
    db_path: str | Path,
    *,
    timeout_sec: int | None = None,
    row_factory: bool = True,
    wal_enabled: bool = True,
) -> sqlite3.Connection:
    actual_timeout = max(
        1,
        int(timeout_sec if timeout_sec is not None else s.REQUEST_LOG_SQLITE_TIMEOUT_SEC),
    )
    resolved_path = _ensure_sqlite_parent_dir(db_path)
    connection = sqlite3.connect(
        resolved_path,
        timeout=float(actual_timeout),
        isolation_level=None,
    )
    connection.execute(
        f"PRAGMA busy_timeout = {max(1000, s.REQUEST_LOG_SQLITE_BUSY_TIMEOUT_MS)}"
    )
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute("PRAGMA temp_store = MEMORY")
    if wal_enabled:
        connection.execute("PRAGMA journal_mode = WAL")
    connection.execute("PRAGMA synchronous = NORMAL")
    if row_factory:
        connection.row_factory = sqlite3.Row
    return connection


def _build_sqlite_snapshot_key(
    db_path: str | Path,
    *,
    key_prefix: str = "",
    object_key: str = "",
) -> str:
    normalized_object_key = str(object_key or "").strip().strip("/")
    if normalized_object_key:
        return normalized_object_key

    resolved_path = _resolve_sqlite_path(db_path)
    try:
        local_now = datetime.now(ZoneInfo(s.REQUEST_LOG_TIMEZONE))
    except Exception:
        local_now = datetime.now(timezone.utc)
    local_date = local_now.strftime("%Y-%m-%d")
    suffix = resolved_path.suffix or ".sqlite3"
    filename = f"{local_date}-{resolved_path.stem}{suffix}"
    normalized_prefix = str(key_prefix or "").strip().strip("/")
    if normalized_prefix:
        return f"{normalized_prefix}/{filename}"
    return filename


def _sqlite_file_exists(db_path: str | Path) -> bool:
    resolved_path = _resolve_sqlite_path(db_path)
    return resolved_path.exists() and resolved_path.stat().st_size > 0


def _create_sqlite_snapshot(
    db_path: str | Path,
    *,
    snapshot_path: str | Path | None = None,
) -> Path:
    source_path = _resolve_sqlite_path(db_path)
    if not source_path.exists():
        raise FileNotFoundError(f"SQLite 파일을 찾지 못했어: {source_path}")

    if snapshot_path is None:
        temp_file = tempfile.NamedTemporaryFile(
            prefix=f"{source_path.stem}-",
            suffix=source_path.suffix or ".sqlite3",
            delete=False,
        )
        temp_file.close()
        destination_path = Path(temp_file.name)
    else:
        destination_path = _ensure_sqlite_parent_dir(snapshot_path)

    source_connection = sqlite3.connect(source_path)
    destination_connection = sqlite3.connect(destination_path)
    try:
        source_connection.execute(
            f"PRAGMA busy_timeout = {max(1000, s.REQUEST_LOG_SQLITE_BUSY_TIMEOUT_MS)}"
        )
        source_connection.backup(destination_connection)
    finally:
        destination_connection.close()
        source_connection.close()

    return destination_path.resolve()


def _list_sqlite_backups_in_s3(
    *,
    bucket: str,
    key_prefix: str = "",
    s3_client: Any | None = None,
) -> list[dict[str, Any]]:
    actual_bucket = str(bucket or "").strip()
    if not actual_bucket:
        raise ValueError("S3 조회 버킷이 필요해")

    client = s3_client or _build_s3_client()
    normalized_prefix = str(key_prefix or "").strip().strip("/")
    prefix = f"{normalized_prefix}/" if normalized_prefix else ""
    continuation_token: str | None = None
    objects: list[dict[str, Any]] = []

    while True:
        params: dict[str, Any] = {
            "Bucket": actual_bucket,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token
        response = client.list_objects_v2(**params)
        contents = response.get("Contents") or []
        for item in contents:
            key = str(item.get("Key") or "").strip()
            if not key:
                continue
            objects.append(
                {
                    "bucket": actual_bucket,
                    "key": key,
                    "lastModified": item.get("LastModified"),
                    "size": int(item.get("Size") or 0),
                    "etag": item.get("ETag"),
                }
            )
        if not response.get("IsTruncated"):
            break
        continuation_token = str(response.get("NextContinuationToken") or "").strip() or None

    return objects


def _find_latest_sqlite_backup_in_s3(
    *,
    bucket: str,
    key_prefix: str = "",
    s3_client: Any | None = None,
) -> dict[str, Any] | None:
    objects = _list_sqlite_backups_in_s3(
        bucket=bucket,
        key_prefix=key_prefix,
        s3_client=s3_client,
    )
    if not objects:
        return None
    return max(
        objects,
        key=lambda item: (
            str(item.get("lastModified") or ""),
            str(item.get("key") or ""),
        ),
    )


def _backup_sqlite_to_s3(
    db_path: str | Path,
    *,
    bucket: str,
    key_prefix: str = "",
    object_key: str = "",
    s3_client: Any | None = None,
    storage_class: str | None = None,
    server_side_encryption: str | None = None,
) -> dict[str, Any]:
    actual_bucket = str(bucket or "").strip()
    if not actual_bucket:
        raise ValueError("S3 백업 버킷이 필요해")

    snapshot_path = _create_sqlite_snapshot(db_path)
    resolved_object_key = _build_sqlite_snapshot_key(
        db_path,
        key_prefix=key_prefix,
        object_key=object_key,
    )
    client = s3_client or _build_s3_client()
    extra_args: dict[str, str] = {}

    actual_storage_class = str(storage_class or "").strip()
    if actual_storage_class:
        extra_args["StorageClass"] = actual_storage_class

    actual_sse = str(server_side_encryption or "").strip()
    if actual_sse:
        extra_args["ServerSideEncryption"] = actual_sse

    try:
        if extra_args:
            client.upload_file(
                str(snapshot_path),
                actual_bucket,
                resolved_object_key,
                ExtraArgs=extra_args,
            )
        else:
            client.upload_file(str(snapshot_path), actual_bucket, resolved_object_key)
    finally:
        snapshot_path.unlink(missing_ok=True)

    return {
        "bucket": actual_bucket,
        "key": resolved_object_key,
        "dbPath": str(_resolve_sqlite_path(db_path)),
    }


def _restore_sqlite_from_s3(
    db_path: str | Path,
    *,
    bucket: str,
    key_prefix: str = "",
    object_key: str = "",
    s3_client: Any | None = None,
    only_if_missing: bool = True,
) -> dict[str, Any]:
    target_path = _ensure_sqlite_parent_dir(db_path)
    if only_if_missing and _sqlite_file_exists(target_path):
        return {
            "restored": False,
            "reason": "local_exists",
            "dbPath": str(target_path),
        }

    client = s3_client or _build_s3_client()
    resolved_object_key = str(object_key or "").strip().strip("/")
    backup_target: dict[str, Any] | None
    if resolved_object_key:
        try:
            metadata = client.head_object(Bucket=str(bucket), Key=resolved_object_key)
        except Exception:
            return {
                "restored": False,
                "reason": "remote_missing",
                "dbPath": str(target_path),
                "bucket": str(bucket or "").strip(),
                "key": resolved_object_key,
            }
        backup_target = {
            "bucket": str(bucket or "").strip(),
            "key": resolved_object_key,
            "lastModified": metadata.get("LastModified"),
            "size": int(metadata.get("ContentLength") or 0),
            "etag": metadata.get("ETag"),
        }
    else:
        backup_target = _find_latest_sqlite_backup_in_s3(
            bucket=bucket,
            key_prefix=key_prefix,
            s3_client=client,
        )
    if backup_target is None:
        return {
            "restored": False,
            "reason": "remote_missing",
            "dbPath": str(target_path),
            "bucket": str(bucket or "").strip(),
        }

    temp_file = tempfile.NamedTemporaryFile(
        prefix=f"{target_path.stem}-restore-",
        suffix=target_path.suffix or ".sqlite3",
        delete=False,
    )
    temp_file.close()
    temp_path = Path(temp_file.name)

    try:
        client.download_file(str(bucket), str(backup_target["key"]), str(temp_path))
        with sqlite3.connect(temp_path) as validation_connection:
            row = validation_connection.execute("PRAGMA integrity_check").fetchone()
            if not row or str(row[0]).strip().lower() != "ok":
                raise RuntimeError("다운로드한 SQLite snapshot 무결성 검증에 실패했어")
        Path(f"{target_path}-wal").unlink(missing_ok=True)
        Path(f"{target_path}-shm").unlink(missing_ok=True)
        target_path.unlink(missing_ok=True)
        temp_path.replace(target_path)
    finally:
        temp_path.unlink(missing_ok=True)

    return {
        "restored": True,
        "dbPath": str(target_path),
        "bucket": str(bucket or "").strip(),
        "key": str(backup_target["key"]),
        "size": backup_target.get("size"),
        "lastModified": backup_target.get("lastModified"),
    }
