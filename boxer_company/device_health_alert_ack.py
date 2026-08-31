from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import re
import sqlite3
from typing import Any, Mapping

from boxer.core import settings as core_settings
from boxer.observability.sqlite_store import _connect_sqlite
from boxer_company.device_health_fingerprint import (
    canonical_device_health_alert_fingerprint,
)


_DEVICE_HEALTH_ALERT_ACK_TABLE = "device_health_alert_ack"
_SLACK_SCOPED_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$")
_SLACK_MESSAGE_TS_PATTERN = re.compile(r"^[0-9]{1,20}\.[0-9]{1,12}$")
_DEVICE_HEALTH_ALERT_ACK_SCHEMA = (
    f"""
    CREATE TABLE IF NOT EXISTS {_DEVICE_HEALTH_ALERT_ACK_TABLE} (
        ackKey TEXT PRIMARY KEY,
        acknowledgedAtUtc TEXT NOT NULL,
        workspaceId TEXT NOT NULL,
        channelId TEXT NOT NULL,
        messageTs TEXT NOT NULL,
        targetFingerprint TEXT NOT NULL,
        actorUserId TEXT NOT NULL,
        UNIQUE(workspaceId, channelId, messageTs, targetFingerprint)
    )
    """,
    f"""
    CREATE INDEX IF NOT EXISTS idx_{_DEVICE_HEALTH_ALERT_ACK_TABLE}_message
    ON {_DEVICE_HEALTH_ALERT_ACK_TABLE}(workspaceId, channelId, messageTs)
    """,
)


@dataclass(frozen=True, slots=True)
class DeviceHealthAlertAcknowledgement:
    """최초 확인 담당자와 시간을 반환하는 영속 완료 claim이다."""

    created: bool
    actor_user_id: str
    acknowledged_at: datetime


def ensure_device_health_alert_ack_schema(
    db_path: str | Path | None = None,
) -> Path:
    """회사 전용 ack 테이블만 기존 중앙 request-log SQLite에 추가한다."""

    actual_path = Path(
        db_path or core_settings.REQUEST_LOG_SQLITE_PATH
    ).expanduser().resolve()
    connection = _connect_sqlite(actual_path, row_factory=False)
    try:
        for statement in _DEVICE_HEALTH_ALERT_ACK_SCHEMA:
            connection.execute(statement)
    finally:
        connection.close()
    return actual_path


def claim_device_health_alert_acknowledgement(
    *,
    workspace_id: str,
    channel_id: str,
    message_ts: str,
    target: Mapping[str, Any],
    actor_user_id: str,
    acknowledged_at: datetime,
    db_path: str | Path | None = None,
    schema_prepared: bool = False,
) -> DeviceHealthAlertAcknowledgement:
    """같은 Slack 카드 대상에는 최초 한 명의 완료 정보만 원자 저장한다."""

    if db_path is None and not core_settings.REQUEST_LOG_SQLITE_ENABLED:
        raise RuntimeError("device health alert ack storage is disabled")
    workspace = _validated_scoped_id(workspace_id, "workspace")
    channel = _validated_scoped_id(channel_id, "channel")
    actor = _validated_scoped_id(actor_user_id, "actor")
    normalized_message_ts = str(message_ts or "").strip()
    if not _SLACK_MESSAGE_TS_PATTERN.fullmatch(normalized_message_ts):
        raise ValueError("device health alert ack message ts is invalid")
    fingerprint = canonical_device_health_alert_fingerprint(target)
    if len(fingerprint.split("|", 3)) != 4 or any(
        not part.strip() for part in fingerprint.split("|", 3)
    ):
        raise ValueError("device health alert ack target is invalid")
    occurred_at = _normalized_utc_datetime(acknowledged_at)
    ack_key = hashlib.sha256(
        "\x1f".join(
            (workspace, channel, normalized_message_ts, fingerprint)
        ).encode("utf-8")
    ).hexdigest()
    actual_path = (
        _resolved_ack_db_path(db_path)
        if schema_prepared
        else ensure_device_health_alert_ack_schema(db_path)
    )
    # API startup이 이미 schema를 준비한 운영 경로는 기존 파일만 rw로
    # 연다. 실행 중 삭제된 감사 DB를 빈 ACK DB로 조용히 재생성하지 않는다.
    connection = (
        _connect_prepared_ack_sqlite(actual_path)
        if schema_prepared
        else _connect_sqlite(actual_path, row_factory=False)
    )
    try:
        # 단일 API worker 밖의 동시 요청도 unique row를 두 번 만들지 못하게
        # SQLite write transaction에서 first-writer-wins를 확정한다.
        connection.execute("BEGIN IMMEDIATE")
        cursor = connection.execute(
            f"""
            INSERT OR IGNORE INTO {_DEVICE_HEALTH_ALERT_ACK_TABLE} (
                ackKey,
                acknowledgedAtUtc,
                workspaceId,
                channelId,
                messageTs,
                targetFingerprint,
                actorUserId
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                ack_key,
                occurred_at.isoformat(),
                workspace,
                channel,
                normalized_message_ts,
                fingerprint,
                actor,
            ),
        )
        row = connection.execute(
            f"""
            SELECT actorUserId, acknowledgedAtUtc
            FROM {_DEVICE_HEALTH_ALERT_ACK_TABLE}
            WHERE workspaceId = ?
              AND channelId = ?
              AND messageTs = ?
              AND targetFingerprint = ?
            """,
            (workspace, channel, normalized_message_ts, fingerprint),
        ).fetchone()
        connection.execute("COMMIT")
    except Exception:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    finally:
        connection.close()
    if row is None:
        raise RuntimeError("device health alert ack row is missing")
    return DeviceHealthAlertAcknowledgement(
        created=cursor.rowcount == 1,
        actor_user_id=str(row[0]),
        acknowledged_at=_normalized_utc_datetime(
            datetime.fromisoformat(str(row[1]).replace("Z", "+00:00"))
        ),
    )


def _validated_scoped_id(value: str, label: str) -> str:
    normalized = str(value or "").strip()
    if not _SLACK_SCOPED_ID_PATTERN.fullmatch(normalized):
        raise ValueError(f"device health alert ack {label} is invalid")
    return normalized


def _resolved_ack_db_path(db_path: str | Path | None) -> Path:
    return Path(
        db_path or core_settings.REQUEST_LOG_SQLITE_PATH
    ).expanduser().resolve()


def _connect_prepared_ack_sqlite(db_path: Path) -> sqlite3.Connection:
    """준비된 중앙 SQLite를 생성 없이 열어 ACK 이력 초기화를 막는다."""

    timeout_sec = max(1, int(core_settings.REQUEST_LOG_SQLITE_TIMEOUT_SEC))
    connection = sqlite3.connect(
        f"{db_path.as_uri()}?mode=rw",
        timeout=float(timeout_sec),
        isolation_level=None,
        uri=True,
    )
    try:
        connection.execute(
            "PRAGMA busy_timeout = "
            f"{max(1000, core_settings.REQUEST_LOG_SQLITE_BUSY_TIMEOUT_MS)}"
        )
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA temp_store = MEMORY")
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
    except Exception:
        connection.close()
        raise
    return connection


def _normalized_utc_datetime(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("device health alert ack timestamp is invalid")
    normalized = (
        value.replace(tzinfo=timezone.utc)
        if value.tzinfo is None
        else value.astimezone(timezone.utc)
    )
    return normalized.replace(microsecond=0)


__all__ = [
    "DeviceHealthAlertAcknowledgement",
    "claim_device_health_alert_acknowledgement",
    "ensure_device_health_alert_ack_schema",
]
