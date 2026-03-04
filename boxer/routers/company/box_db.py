import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pymysql

from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.core.utils import _display_value, _format_datetime
from boxer.routers.common.db import _create_db_connection


def _local_zone() -> ZoneInfo:
    tz_name = os.getenv("TZ", "Asia/Seoul")
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return ZoneInfo("Asia/Seoul")


def _local_date_to_utc_range(target_date: str) -> tuple[datetime, datetime]:
    try:
        parsed = datetime.strptime(target_date, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("날짜 형식은 YYYY-MM-DD로 입력해줘") from exc

    local_tz = _local_zone()
    local_start = datetime(
        year=parsed.year,
        month=parsed.month,
        day=parsed.day,
        hour=0,
        minute=0,
        second=0,
        tzinfo=local_tz,
    )
    local_end = local_start + timedelta(days=1)

    utc_start = local_start.astimezone(timezone.utc).replace(tzinfo=None)
    utc_end = local_end.astimezone(timezone.utc).replace(tzinfo=None)
    return utc_start, utc_end


def _format_recorded_at_local(value: object) -> str:
    if isinstance(value, datetime):
        local_tz = _local_zone()
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        localized = value.astimezone(local_tz)
        return localized.strftime("%Y-%m-%d %H:%M:%S")
    return _format_datetime(value)


def _query_recordings_count_by_barcode(barcode: str) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS recordingCount FROM recordings WHERE fullBarcode = %s",
                (barcode,),
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()

    count = int(row.get("recordingCount") or 0)
    return (
        "*바코드 영상 개수 조회 결과*\n"
        f"• 바코드: `{barcode}`\n"
        f"• recordings row 수: *{count}개*"
    )


def _query_last_recorded_at_by_barcode(barcode: str) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS recordingCount, "
                "MAX(recordedAt) AS lastRecordedAt "
                "FROM recordings "
                "WHERE fullBarcode = %s "
                "AND recordedAt IS NOT NULL",
                (barcode,),
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()

    count = int(row.get("recordingCount") or 0)
    last_recorded_at = row.get("lastRecordedAt")
    if count <= 0 or not last_recorded_at:
        return (
            "*바코드 마지막 녹화 날짜 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            "• 결과: recordedAt 기준 녹화 기록이 없어"
        )

    return (
        "*바코드 마지막 녹화 날짜 조회 결과*\n"
        f"• 바코드: `{barcode}`\n"
        f"• recordings row 수: *{count}개*\n"
        f"• 마지막 recordedAt(KST): *{_format_recorded_at_local(last_recorded_at)}*"
    )


def _query_recordings_on_date_by_barcode(barcode: str, target_date: str) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    utc_start, utc_end = _local_date_to_utc_range(target_date)

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS recordingCount, "
                "MIN(recordedAt) AS firstRecordedAt, "
                "MAX(recordedAt) AS lastRecordedAt "
                "FROM recordings "
                "WHERE fullBarcode = %s "
                "AND recordedAt >= %s "
                "AND recordedAt < %s",
                (barcode, utc_start, utc_end),
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()

    count = int(row.get("recordingCount") or 0)
    first_recorded_at = row.get("firstRecordedAt")
    last_recorded_at = row.get("lastRecordedAt")
    if count <= 0:
        return (
            "*바코드 날짜별 녹화 여부 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{target_date}`\n"
            "• 결과: recordedAt 기준 녹화 기록이 없어"
        )

    return (
        "*바코드 날짜별 녹화 여부 조회 결과*\n"
        f"• 바코드: `{barcode}`\n"
        f"• 날짜(KST): `{target_date}`\n"
        f"• 조회 범위(UTC): `{utc_start:%Y-%m-%d %H:%M:%S}` ~ `{utc_end:%Y-%m-%d %H:%M:%S}`\n"
        f"• recordings row 수: *{count}개*\n"
        f"• 첫 recordedAt(KST): `{_format_recorded_at_local(first_recorded_at)}`\n"
        f"• 마지막 recordedAt(KST): `{_format_recorded_at_local(last_recorded_at)}`"
    )


def _lookup_device_names_by_barcode(barcode: str) -> list[str]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    sql_candidates = [
        (
            "SELECT DISTINCT d.deviceName AS deviceName "
            "FROM recordings r "
            "JOIN devices d ON d.seq = r.deviceSeq AND d.hospitalSeq = r.hospitalSeq "
            "WHERE r.fullBarcode = %s AND COALESCE(d.deviceName, '') <> '' "
            "LIMIT %s"
        ),
        (
            "SELECT DISTINCT d.deviceName AS deviceName "
            "FROM recordings r "
            "JOIN devices d ON d.seq = r.deviceSeq "
            "WHERE r.fullBarcode = %s AND COALESCE(d.deviceName, '') <> '' "
            "LIMIT %s"
        ),
    ]

    limit = max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 2))
    last_error: Exception | None = None
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            for sql in sql_candidates:
                try:
                    cursor.execute(sql, (barcode, limit))
                    rows = cursor.fetchall()
                except pymysql.MySQLError as exc:
                    last_error = exc
                    continue

                names: list[str] = []
                seen: set[str] = set()
                for row in rows:
                    name = _display_value(row.get("deviceName"), default="")
                    if not name:
                        continue
                    if name in seen:
                        continue
                    seen.add(name)
                    names.append(name)
                if names:
                    return names
    finally:
        connection.close()

    if last_error:
        raise last_error
    return []
