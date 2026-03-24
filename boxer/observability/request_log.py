from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.observability.sqlite_store import (
    _backup_sqlite_to_s3,
    _connect_sqlite,
    _restore_sqlite_from_s3,
    _resolve_sqlite_path,
)

_REQUEST_LOG_TABLE_NAME = "request_log"
_LEGACY_REQUEST_AUDIT_TABLE_NAME = "request_audit_log"
_REQUEST_LOG_INDEX_PREFIX = _LEGACY_REQUEST_AUDIT_TABLE_NAME
_REQUEST_LOG_SCHEMA_STATEMENTS = (
    f"""
    CREATE TABLE IF NOT EXISTS {_REQUEST_LOG_TABLE_NAME} (
        seq INTEGER PRIMARY KEY AUTOINCREMENT,
        createdAtUtc TEXT NOT NULL,
        createdAtLocal TEXT NOT NULL,
        requestDateLocal TEXT NOT NULL,
        sourcePlatform TEXT NOT NULL,
        workspaceId TEXT NOT NULL DEFAULT '',
        eventType TEXT NOT NULL,
        routeName TEXT NOT NULL,
        routeMode TEXT,
        handlerType TEXT NOT NULL DEFAULT 'unknown',
        status TEXT NOT NULL,
        userId TEXT NOT NULL,
        userName TEXT,
        channelId TEXT NOT NULL DEFAULT '',
        threadId TEXT NOT NULL,
        messageId TEXT NOT NULL,
        isThreadRoot INTEGER NOT NULL DEFAULT 1,
        permalink TEXT,
        threadPermalink TEXT,
        requestText TEXT NOT NULL,
        normalizedQuestion TEXT,
        requestKey TEXT,
        subjectType TEXT,
        subjectKey TEXT,
        requestedDate TEXT,
        replyCount INTEGER NOT NULL DEFAULT 0,
        firstRepliedAtUtc TEXT,
        firstRepliedAtLocal TEXT,
        errorType TEXT,
        metadataJson TEXT,
        UNIQUE(sourcePlatform, channelId, messageId)
    )
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{_REQUEST_LOG_INDEX_PREFIX}_createdAtUtc
    ON {_REQUEST_LOG_TABLE_NAME}(createdAtUtc)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{_REQUEST_LOG_INDEX_PREFIX}_requestDateLocal
    ON {_REQUEST_LOG_TABLE_NAME}(requestDateLocal)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{_REQUEST_LOG_INDEX_PREFIX}_userId_createdAtUtc
    ON {_REQUEST_LOG_TABLE_NAME}(userId, createdAtUtc)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{_REQUEST_LOG_INDEX_PREFIX}_routeName_createdAtUtc
    ON {_REQUEST_LOG_TABLE_NAME}(routeName, createdAtUtc)
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{_REQUEST_LOG_INDEX_PREFIX}_threadId
    ON {_REQUEST_LOG_TABLE_NAME}(sourcePlatform, channelId, threadId)
    """,
)

_REQUEST_LOG_UPSERT_SQL = f"""
INSERT INTO {_REQUEST_LOG_TABLE_NAME} (
    createdAtUtc,
    createdAtLocal,
    requestDateLocal,
    sourcePlatform,
    workspaceId,
    eventType,
    routeName,
    routeMode,
    handlerType,
    status,
    userId,
    userName,
    channelId,
    threadId,
    messageId,
    isThreadRoot,
    permalink,
    threadPermalink,
    requestText,
    normalizedQuestion,
    requestKey,
    subjectType,
    subjectKey,
    requestedDate,
    replyCount,
    firstRepliedAtUtc,
    firstRepliedAtLocal,
    errorType,
    metadataJson
) VALUES (
    :createdAtUtc,
    :createdAtLocal,
    :requestDateLocal,
    :sourcePlatform,
    :workspaceId,
    :eventType,
    :routeName,
    :routeMode,
    :handlerType,
    :status,
    :userId,
    :userName,
    :channelId,
    :threadId,
    :messageId,
    :isThreadRoot,
    :permalink,
    :threadPermalink,
    :requestText,
    :normalizedQuestion,
    :requestKey,
    :subjectType,
    :subjectKey,
    :requestedDate,
    :replyCount,
    :firstRepliedAtUtc,
    :firstRepliedAtLocal,
    :errorType,
    :metadataJson
)
ON CONFLICT(sourcePlatform, channelId, messageId) DO UPDATE SET
    routeName = CASE
        WHEN excluded.routeName = 'unknown' THEN {_REQUEST_LOG_TABLE_NAME}.routeName
        ELSE excluded.routeName
    END,
    routeMode = COALESCE(excluded.routeMode, {_REQUEST_LOG_TABLE_NAME}.routeMode),
    handlerType = CASE
        WHEN excluded.handlerType = 'unknown' THEN {_REQUEST_LOG_TABLE_NAME}.handlerType
        ELSE excluded.handlerType
    END,
    status = excluded.status,
    userName = COALESCE(excluded.userName, {_REQUEST_LOG_TABLE_NAME}.userName),
    permalink = COALESCE(excluded.permalink, {_REQUEST_LOG_TABLE_NAME}.permalink),
    threadPermalink = COALESCE(excluded.threadPermalink, {_REQUEST_LOG_TABLE_NAME}.threadPermalink),
    normalizedQuestion = COALESCE(
        excluded.normalizedQuestion,
        {_REQUEST_LOG_TABLE_NAME}.normalizedQuestion
    ),
    requestKey = COALESCE(excluded.requestKey, {_REQUEST_LOG_TABLE_NAME}.requestKey),
    subjectType = COALESCE(excluded.subjectType, {_REQUEST_LOG_TABLE_NAME}.subjectType),
    subjectKey = COALESCE(excluded.subjectKey, {_REQUEST_LOG_TABLE_NAME}.subjectKey),
    requestedDate = COALESCE(excluded.requestedDate, {_REQUEST_LOG_TABLE_NAME}.requestedDate),
    replyCount = MAX({_REQUEST_LOG_TABLE_NAME}.replyCount, excluded.replyCount),
    firstRepliedAtUtc = COALESCE(
        {_REQUEST_LOG_TABLE_NAME}.firstRepliedAtUtc,
        excluded.firstRepliedAtUtc
    ),
    firstRepliedAtLocal = COALESCE(
        {_REQUEST_LOG_TABLE_NAME}.firstRepliedAtLocal,
        excluded.firstRepliedAtLocal
    ),
    errorType = COALESCE(excluded.errorType, {_REQUEST_LOG_TABLE_NAME}.errorType),
    metadataJson = COALESCE(excluded.metadataJson, {_REQUEST_LOG_TABLE_NAME}.metadataJson)
"""


class RequestLogRecord(TypedDict, total=False):
    createdAtUtc: str | datetime
    sourcePlatform: str
    workspaceId: str
    eventType: str
    routeName: str
    routeMode: str | None
    handlerType: str
    status: str
    userId: str
    userName: str | None
    channelId: str
    threadId: str
    messageId: str
    isThreadRoot: bool | int
    permalink: str | None
    threadPermalink: str | None
    requestText: str
    normalizedQuestion: str | None
    requestKey: str | None
    subjectType: str | None
    subjectKey: str | None
    requestedDate: str | None
    replyCount: int
    firstRepliedAtUtc: str | datetime | None
    errorType: str | None
    metadata: dict[str, Any] | list[Any] | str | None
    metadataJson: str | None


def _request_log_timezone() -> ZoneInfo:
    try:
        return ZoneInfo(s.REQUEST_LOG_TIMEZONE)
    except Exception as exc:
        raise RuntimeError(
            f"REQUEST_LOG_TIMEZONE 설정이 올바르지 않아: {s.REQUEST_LOG_TIMEZONE}"
        ) from exc


def _coerce_utc_datetime(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if not text:
            return None
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def _render_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.isoformat()


def _normalize_request_log_metadata(record: RequestLogRecord) -> str | None:
    metadata_json = record.get("metadataJson")
    if metadata_json is not None:
        text = str(metadata_json).strip()
        return text or None

    metadata = record.get("metadata")
    if metadata is None:
        return None
    if isinstance(metadata, str):
        text = metadata.strip()
        return text or None
    return json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))


def _normalize_request_log_record(record: RequestLogRecord) -> dict[str, Any]:
    created_at_utc = _coerce_utc_datetime(record.get("createdAtUtc"))
    if created_at_utc is None:
        created_at_utc = datetime.now(timezone.utc).replace(microsecond=0)
    local_timezone = _request_log_timezone()
    created_at_local = created_at_utc.astimezone(local_timezone)

    first_replied_at_utc = _coerce_utc_datetime(record.get("firstRepliedAtUtc"))
    first_replied_at_local = (
        first_replied_at_utc.astimezone(local_timezone)
        if first_replied_at_utc is not None
        else None
    )

    message_id = str(record.get("messageId") or "").strip()
    if not message_id:
        raise ValueError("request log 저장에는 messageId가 필요해")

    request_text = str(record.get("requestText") or "").strip()
    if not request_text:
        raise ValueError("request log 저장에는 requestText가 필요해")

    user_id = str(record.get("userId") or "").strip()
    if not user_id:
        raise ValueError("request log 저장에는 userId가 필요해")

    channel_id = str(record.get("channelId") or "").strip()
    thread_id = str(record.get("threadId") or "").strip() or message_id

    reply_count = max(0, int(record.get("replyCount") or 0))
    is_thread_root = int(
        bool(record.get("isThreadRoot"))
        if "isThreadRoot" in record
        else thread_id == message_id
    )

    requested_date = str(record.get("requestedDate") or "").strip() or None
    route_name = str(record.get("routeName") or "unknown").strip() or "unknown"
    status = str(record.get("status") or "handled").strip() or "handled"
    user_name = str(record.get("userName") or "").strip() or None

    return {
        "createdAtUtc": _render_iso(created_at_utc),
        "createdAtLocal": _render_iso(created_at_local),
        "requestDateLocal": created_at_local.date().isoformat(),
        "sourcePlatform": str(record.get("sourcePlatform") or "slack").strip() or "slack",
        "workspaceId": str(record.get("workspaceId") or "").strip(),
        "eventType": str(record.get("eventType") or "message").strip() or "message",
        "routeName": route_name,
        "routeMode": str(record.get("routeMode") or "").strip() or None,
        "handlerType": str(record.get("handlerType") or "unknown").strip() or "unknown",
        "status": status,
        "userId": user_id,
        "userName": user_name,
        "channelId": channel_id,
        "threadId": thread_id,
        "messageId": message_id,
        "isThreadRoot": is_thread_root,
        "permalink": str(record.get("permalink") or "").strip() or None,
        "threadPermalink": str(record.get("threadPermalink") or "").strip() or None,
        "requestText": request_text,
        "normalizedQuestion": str(record.get("normalizedQuestion") or "").strip() or None,
        "requestKey": str(record.get("requestKey") or "").strip() or None,
        "subjectType": str(record.get("subjectType") or "").strip() or None,
        "subjectKey": str(record.get("subjectKey") or "").strip() or None,
        "requestedDate": requested_date,
        "replyCount": reply_count,
        "firstRepliedAtUtc": _render_iso(first_replied_at_utc),
        "firstRepliedAtLocal": _render_iso(first_replied_at_local),
        "errorType": str(record.get("errorType") or "").strip() or None,
        "metadataJson": _normalize_request_log_metadata(record),
    }


def _request_log_db_path(db_path: str | Path | None = None) -> Path:
    return _resolve_sqlite_path(db_path or s.REQUEST_LOG_SQLITE_PATH)


def _migrate_legacy_request_audit_table(connection) -> bool:
    tables = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    if _REQUEST_LOG_TABLE_NAME in tables:
        return False
    if _LEGACY_REQUEST_AUDIT_TABLE_NAME not in tables:
        return False
    connection.execute(
        f"ALTER TABLE {_LEGACY_REQUEST_AUDIT_TABLE_NAME} RENAME TO {_REQUEST_LOG_TABLE_NAME}"
    )
    return True


def _ensure_request_log_columns(connection) -> None:
    columns = {
        str(row[1])
        for row in connection.execute(f"PRAGMA table_info({_REQUEST_LOG_TABLE_NAME})").fetchall()
    }
    if "userName" not in columns:
        connection.execute(
            f"ALTER TABLE {_REQUEST_LOG_TABLE_NAME} ADD COLUMN userName TEXT"
        )
    if "handlerType" not in columns:
        connection.execute(
            f"""
            ALTER TABLE {_REQUEST_LOG_TABLE_NAME}
            ADD COLUMN handlerType TEXT NOT NULL DEFAULT 'unknown'
            """
        )


def _ensure_request_log_schema(
    db_path: str | Path | None = None,
) -> Path:
    actual_path = _request_log_db_path(db_path)
    connection = _connect_sqlite(actual_path, row_factory=False)
    try:
        _migrate_legacy_request_audit_table(connection)
        for statement in _REQUEST_LOG_SCHEMA_STATEMENTS:
            connection.execute(statement)
        _ensure_request_log_columns(connection)
    finally:
        connection.close()
    return actual_path


def _save_request_log_record(
    record: RequestLogRecord,
    *,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    actual_path = _ensure_request_log_schema(db_path)
    normalized_record = _normalize_request_log_record(record)
    connection = _connect_sqlite(actual_path, row_factory=False)
    try:
        cursor = connection.execute(_REQUEST_LOG_UPSERT_SQL, normalized_record)
        row = connection.execute(
            f"""
            SELECT seq, createdAtUtc, routeName, handlerType, status, replyCount, userName
            FROM {_REQUEST_LOG_TABLE_NAME}
            WHERE sourcePlatform = :sourcePlatform
              AND channelId = :channelId
              AND messageId = :messageId
            """,
            normalized_record,
        ).fetchone()
    finally:
        connection.close()

    return {
        "dbPath": str(actual_path),
        "rowcount": cursor.rowcount,
        "seq": row[0] if row else None,
        "createdAtUtc": row[1] if row else None,
        "routeName": row[2] if row else normalized_record["routeName"],
        "handlerType": row[3] if row else normalized_record["handlerType"],
        "status": row[4] if row else normalized_record["status"],
        "replyCount": row[5] if row else normalized_record["replyCount"],
        "userName": row[6] if row else normalized_record["userName"],
}


def _build_request_log_filter_clause(
    *,
    target_date: str | None = None,
    user_query: str | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    parameters: list[Any] = []

    normalized_target_date = str(target_date or "").strip()
    if normalized_target_date:
        clauses.append("requestDateLocal = ?")
        parameters.append(normalized_target_date)

    normalized_user_query = str(user_query or "").strip()
    if normalized_user_query:
        lowered_user_query = normalized_user_query.lower()
        clauses.append(
            "("
            "userId = ? "
            "OR LOWER(COALESCE(userName, '')) = ? "
            "OR LOWER(COALESCE(userName, '')) LIKE ?"
            ")"
        )
        parameters.extend(
            [
                normalized_user_query,
                lowered_user_query,
                f"%{lowered_user_query}%",
            ]
        )

    if not clauses:
        return "", []
    return f"WHERE {' AND '.join(clauses)}", parameters


def _query_request_log_rows(
    sql: str,
    parameters: list[Any] | tuple[Any, ...] | None = None,
    *,
    db_path: str | Path | None = None,
) -> list[dict[str, Any]]:
    actual_path = _ensure_request_log_schema(db_path)
    connection = _connect_sqlite(
        actual_path,
        row_factory=True,
        wal_enabled=False,
    )
    try:
        connection.execute("PRAGMA query_only = ON")
        rows = connection.execute(sql, tuple(parameters or ())).fetchall()
    finally:
        connection.close()
    return [dict(row) for row in rows]


def _query_request_log_value(
    sql: str,
    parameters: list[Any] | tuple[Any, ...] | None = None,
    *,
    db_path: str | Path | None = None,
) -> Any:
    rows = _query_request_log_rows(sql, parameters, db_path=db_path)
    if not rows:
        return None
    first_row = rows[0]
    if not first_row:
        return None
    first_key = next(iter(first_row))
    return first_row.get(first_key)


def _normalize_request_log_query_limit(
    limit: int | None,
    *,
    default: int,
    max_limit: int,
) -> int:
    try:
        normalized = int(limit or default)
    except Exception:
        normalized = default
    normalized = max(1, normalized)
    return min(normalized, max_limit)


def _list_request_log_recent(
    *,
    target_date: str | None = None,
    user_query: str | None = None,
    limit: int | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    actual_limit = _normalize_request_log_query_limit(limit, default=10, max_limit=30)
    where_clause, parameters = _build_request_log_filter_clause(
        target_date=target_date,
        user_query=user_query,
    )
    total_count = int(
        _query_request_log_value(
            f"SELECT COUNT(*) AS value FROM {_REQUEST_LOG_TABLE_NAME} {where_clause}",
            parameters,
            db_path=db_path,
        )
        or 0
    )
    rows = _query_request_log_rows(
        f"""
        SELECT
            seq,
            createdAtUtc,
            createdAtLocal,
            requestDateLocal,
            userId,
            userName,
            routeName,
            routeMode,
            handlerType,
            status,
            requestText,
            normalizedQuestion,
            permalink,
            threadPermalink,
            replyCount
        FROM {_REQUEST_LOG_TABLE_NAME}
        {where_clause}
        ORDER BY seq DESC
        LIMIT ?
        """,
        [*parameters, actual_limit],
        db_path=db_path,
    )
    return {
        "dbPath": str(_request_log_db_path(db_path)),
        "targetDate": target_date,
        "userQuery": user_query,
        "limit": actual_limit,
        "totalCount": total_count,
        "rows": rows,
    }


def _summarize_request_log_by_user(
    *,
    target_date: str | None = None,
    limit: int | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    actual_limit = _normalize_request_log_query_limit(limit, default=10, max_limit=20)
    where_clause, parameters = _build_request_log_filter_clause(target_date=target_date)
    total_count = int(
        _query_request_log_value(
            f"SELECT COUNT(*) AS value FROM {_REQUEST_LOG_TABLE_NAME} {where_clause}",
            parameters,
            db_path=db_path,
        )
        or 0
    )
    unique_user_count = int(
        _query_request_log_value(
            f"""
            SELECT COUNT(DISTINCT userId) AS value
            FROM {_REQUEST_LOG_TABLE_NAME}
            {where_clause}
            """,
            parameters,
            db_path=db_path,
        )
        or 0
    )
    rows = _query_request_log_rows(
        f"""
        SELECT
            userId,
            COALESCE(NULLIF(TRIM(userName), ''), userId) AS userLabel,
            COUNT(*) AS requestCount,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errorCount,
            MAX(createdAtLocal) AS lastRequestedAtLocal
        FROM {_REQUEST_LOG_TABLE_NAME}
        {where_clause}
        GROUP BY userId, COALESCE(NULLIF(TRIM(userName), ''), userId)
        ORDER BY requestCount DESC, lastRequestedAtLocal DESC, userId ASC
        LIMIT ?
        """,
        [*parameters, actual_limit],
        db_path=db_path,
    )
    return {
        "dbPath": str(_request_log_db_path(db_path)),
        "targetDate": target_date,
        "limit": actual_limit,
        "totalCount": total_count,
        "uniqueUserCount": unique_user_count,
        "rows": rows,
    }


def _summarize_request_log_by_route(
    *,
    target_date: str | None = None,
    limit: int | None = None,
    db_path: str | Path | None = None,
) -> dict[str, Any]:
    actual_limit = _normalize_request_log_query_limit(limit, default=10, max_limit=20)
    where_clause, parameters = _build_request_log_filter_clause(target_date=target_date)
    total_count = int(
        _query_request_log_value(
            f"SELECT COUNT(*) AS value FROM {_REQUEST_LOG_TABLE_NAME} {where_clause}",
            parameters,
            db_path=db_path,
        )
        or 0
    )
    unique_route_count = int(
        _query_request_log_value(
            f"""
            SELECT COUNT(*) AS value
            FROM (
                SELECT routeName, COALESCE(NULLIF(TRIM(routeMode), ''), '') AS routeMode
                FROM {_REQUEST_LOG_TABLE_NAME}
                {where_clause}
                GROUP BY routeName, COALESCE(NULLIF(TRIM(routeMode), ''), '')
            )
            """,
            parameters,
            db_path=db_path,
        )
        or 0
    )
    rows = _query_request_log_rows(
        f"""
        SELECT
            routeName,
            COALESCE(NULLIF(TRIM(routeMode), ''), '') AS routeMode,
            COUNT(*) AS requestCount,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errorCount,
            MAX(createdAtLocal) AS lastRequestedAtLocal
        FROM {_REQUEST_LOG_TABLE_NAME}
        {where_clause}
        GROUP BY routeName, COALESCE(NULLIF(TRIM(routeMode), ''), '')
        ORDER BY requestCount DESC, lastRequestedAtLocal DESC, routeName ASC
        LIMIT ?
        """,
        [*parameters, actual_limit],
        db_path=db_path,
    )
    return {
        "dbPath": str(_request_log_db_path(db_path)),
        "targetDate": target_date,
        "limit": actual_limit,
        "totalCount": total_count,
        "uniqueRouteCount": unique_route_count,
        "rows": rows,
    }


def _summarize_request_log_overview(
    *,
    target_date: str | None = None,
    db_path: str | Path | None = None,
    top_limit: int | None = None,
) -> dict[str, Any]:
    actual_top_limit = _normalize_request_log_query_limit(top_limit, default=5, max_limit=10)
    where_clause, parameters = _build_request_log_filter_clause(target_date=target_date)
    summary_rows = _query_request_log_rows(
        f"""
        SELECT
            COUNT(*) AS totalCount,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS errorCount,
            COUNT(DISTINCT userId) AS uniqueUserCount,
            MIN(createdAtLocal) AS firstRequestedAtLocal,
            MAX(createdAtLocal) AS lastRequestedAtLocal
        FROM {_REQUEST_LOG_TABLE_NAME}
        {where_clause}
        """,
        parameters,
        db_path=db_path,
    )
    summary = summary_rows[0] if summary_rows else {}
    return {
        "dbPath": str(_request_log_db_path(db_path)),
        "targetDate": target_date,
        "topLimit": actual_top_limit,
        "totalCount": int(summary.get("totalCount") or 0),
        "errorCount": int(summary.get("errorCount") or 0),
        "uniqueUserCount": int(summary.get("uniqueUserCount") or 0),
        "firstRequestedAtLocal": summary.get("firstRequestedAtLocal"),
        "lastRequestedAtLocal": summary.get("lastRequestedAtLocal"),
        "topUsers": _summarize_request_log_by_user(
            target_date=target_date,
            limit=actual_top_limit,
            db_path=db_path,
        ).get("rows", []),
        "topRoutes": _summarize_request_log_by_route(
            target_date=target_date,
            limit=actual_top_limit,
            db_path=db_path,
        ).get("rows", []),
    }


def _backup_request_log_to_s3(
    *,
    db_path: str | Path | None = None,
    bucket: str | None = None,
    object_key: str | None = None,
    key_prefix: str | None = None,
    s3_client: Any | None = None,
) -> dict[str, Any]:
    actual_path = _ensure_request_log_schema(db_path)
    actual_bucket = str(bucket or s.REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET).strip()
    if not actual_bucket:
        raise RuntimeError("REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET이 비어 있어")

    return _backup_sqlite_to_s3(
        actual_path,
        bucket=actual_bucket,
        object_key=object_key if object_key is not None else s.REQUEST_LOG_SQLITE_S3_OBJECT_KEY,
        key_prefix=key_prefix if key_prefix is not None else s.REQUEST_LOG_SQLITE_S3_PREFIX,
        s3_client=s3_client,
        storage_class=s.REQUEST_LOG_SQLITE_S3_STORAGE_CLASS,
        server_side_encryption=s.REQUEST_LOG_SQLITE_S3_SERVER_SIDE_ENCRYPTION,
    )


def _backup_request_log_to_configured_s3(
    *,
    db_path: str | Path | None = None,
    s3_client: Any | None = None,
) -> dict[str, Any] | None:
    if not s.REQUEST_LOG_SQLITE_S3_BACKUP_ENABLED:
        return None
    return _backup_request_log_to_s3(
        db_path=db_path,
        s3_client=s3_client,
    )


def _restore_request_log_from_s3(
    *,
    db_path: str | Path | None = None,
    bucket: str | None = None,
    object_key: str | None = None,
    key_prefix: str | None = None,
    s3_client: Any | None = None,
    only_if_missing: bool = True,
) -> dict[str, Any]:
    actual_bucket = str(bucket or s.REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET).strip()
    if not actual_bucket:
        raise RuntimeError("REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET이 비어 있어")

    return _restore_sqlite_from_s3(
        _request_log_db_path(db_path),
        bucket=actual_bucket,
        object_key=object_key if object_key is not None else s.REQUEST_LOG_SQLITE_S3_OBJECT_KEY,
        key_prefix=key_prefix if key_prefix is not None else s.REQUEST_LOG_SQLITE_S3_PREFIX,
        s3_client=s3_client,
        only_if_missing=only_if_missing,
    )


def _restore_request_log_from_configured_s3(
    *,
    db_path: str | Path | None = None,
    s3_client: Any | None = None,
    only_if_missing: bool = True,
) -> dict[str, Any] | None:
    if not s.REQUEST_LOG_SQLITE_S3_RESTORE_ON_STARTUP:
        return None
    return _restore_request_log_from_s3(
        db_path=db_path,
        s3_client=s3_client,
        only_if_missing=only_if_missing,
    )


def _run_request_log_backup_job(
    *,
    db_path: str | Path | None = None,
    s3_client: Any | None = None,
) -> dict[str, Any] | None:
    if not s.REQUEST_LOG_SQLITE_S3_BACKUP_ENABLED:
        return None
    return _backup_request_log_to_s3(
        db_path=db_path,
        s3_client=s3_client,
    )


def _initialize_request_log_storage(
    *,
    db_path: str | Path | None = None,
    s3_client: Any | None = None,
) -> dict[str, Any] | None:
    if not s.REQUEST_LOG_SQLITE_ENABLED:
        return None

    restore_result: dict[str, Any] | None = None
    if s.REQUEST_LOG_SQLITE_S3_RESTORE_ON_STARTUP:
        restore_result = _restore_request_log_from_configured_s3(
            db_path=db_path,
            s3_client=s3_client,
            only_if_missing=True,
        )

    actual_path = _ensure_request_log_schema(db_path)
    return {
        "dbPath": str(actual_path),
        "restored": restore_result,
    }


RequestAuditRecord = RequestLogRecord
_request_audit_timezone = _request_log_timezone
_normalize_request_audit_metadata = _normalize_request_log_metadata
_normalize_request_audit_record = _normalize_request_log_record
_request_audit_db_path = _request_log_db_path
_ensure_request_audit_schema = _ensure_request_log_schema
_save_request_audit_record = _save_request_log_record
_backup_request_audit_to_s3 = _backup_request_log_to_s3
_backup_request_audit_to_configured_s3 = _backup_request_log_to_configured_s3
_restore_request_audit_from_s3 = _restore_request_log_from_s3
_restore_request_audit_from_configured_s3 = _restore_request_log_from_configured_s3
_run_request_audit_backup_job = _run_request_log_backup_job
_initialize_request_audit_storage = _initialize_request_log_storage


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backup request log SQLite snapshot to configured S3",
    )
    parser.parse_args()

    result = _run_request_log_backup_job()
    print(json.dumps(result, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
