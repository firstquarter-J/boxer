import os
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pymysql

from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.core.utils import _display_value, _format_datetime, _truncate_text
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


def _to_local_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    local_tz = _local_zone()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(local_tz)


def _to_utc_datetime(value: object) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _context_limit() -> int:
    return max(1, min(200, cs.RECORDINGS_CONTEXT_LIMIT))


def _load_recordings_context_by_barcode(barcode: str) -> dict[str, Any]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    limit = _context_limit()
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS recordingCount, "
                "MIN(recordedAt) AS firstRecordedAt, "
                "MAX(recordedAt) AS lastRecordedAt "
                "FROM recordings "
                "WHERE fullBarcode = %s",
                (barcode,),
            )
            summary = cursor.fetchone() or {}

            cursor.execute(
                "SELECT "
                "r.seq, "
                "r.hospitalSeq, "
                "r.hospitalRoomSeq, "
                "r.deviceSeq, "
                "h.hospitalName AS hospitalName, "
                "hr.roomName AS roomName, "
                "r.recordedAt, "
                "r.createdAt "
                "FROM recordings r "
                "LEFT JOIN hospitals h ON r.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON r.hospitalRoomSeq = hr.seq "
                "WHERE r.fullBarcode = %s "
                "ORDER BY COALESCE(r.recordedAt, r.createdAt) DESC, r.seq DESC "
                "LIMIT %s",
                (barcode, limit),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    total_count = int(summary.get("recordingCount") or 0)
    return {
        "barcode": barcode,
        "limit": limit,
        "summary": {
            "recordingCount": total_count,
            "firstRecordedAt": summary.get("firstRecordedAt"),
            "lastRecordedAt": summary.get("lastRecordedAt"),
        },
        "rows": rows,
        "has_more": total_count > len(rows),
    }


def _query_recordings_count_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    summary = context.get("summary") or {}
    count = int(summary.get("recordingCount") or 0)
    return (
        "*바코드 영상 개수 조회 결과*\n"
        f"• 바코드: `{barcode}`\n"
        f"• recordings row 수: *{count}개*"
    )


def _query_last_recorded_at_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    summary = context.get("summary") or {}
    count = int(summary.get("recordingCount") or 0)
    last_recorded_at = summary.get("lastRecordedAt")
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


def _query_recordings_on_date_by_barcode(
    barcode: str,
    target_date: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    utc_start, utc_end = _local_date_to_utc_range(target_date)
    utc_start_aware = utc_start.replace(tzinfo=timezone.utc)
    utc_end_aware = utc_end.replace(tzinfo=timezone.utc)

    matched_rows: list[dict[str, Any]] = []
    for row in context.get("rows") or []:
        recorded_at_utc = _to_utc_datetime(row.get("recordedAt"))
        if not recorded_at_utc:
            continue
        if utc_start_aware <= recorded_at_utc < utc_end_aware:
            matched_rows.append(row)

    count = len(matched_rows)
    first_recorded_at = None
    last_recorded_at = None
    if matched_rows:
        recorded_values: list[datetime] = []
        for row in matched_rows:
            recorded_at_utc = _to_utc_datetime(row.get("recordedAt"))
            if recorded_at_utc:
                recorded_values.append(recorded_at_utc)
        if recorded_values:
            first_recorded_at = min(recorded_values)
            last_recorded_at = max(recorded_values)

    has_more = bool(context.get("has_more"))
    limit = int(context.get("limit") or _context_limit())
    if count <= 0:
        lines = [
            "*바코드 날짜별 녹화 여부 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{target_date}`\n"
            "• 결과: recordedAt 기준 녹화 기록이 없어"
        ]
        if has_more:
            lines.append(
                f"• 참고: 최근 `{limit}개` 컨텍스트 기준 결과야(전체는 더 있을 수 있어)"
            )
        return "\n".join(lines)

    lines = [
        "*바코드 날짜별 녹화 여부 조회 결과*\n"
        f"• 바코드: `{barcode}`\n"
        f"• 날짜(KST): `{target_date}`\n"
        f"• 조회 범위(UTC): `{utc_start:%Y-%m-%d %H:%M:%S}` ~ `{utc_end:%Y-%m-%d %H:%M:%S}`\n"
        f"• recordings row 수: *{count}개*\n"
        f"• 첫 recordedAt(KST): `{_format_recorded_at_local(first_recorded_at)}`\n"
        f"• 마지막 recordedAt(KST): `{_format_recorded_at_local(last_recorded_at)}`"
    ]
    if has_more:
        lines.append(f"• 참고: 최근 `{limit}개` 컨텍스트 기준 결과야(전체는 더 있을 수 있어)")
    return "\n".join(lines)


def _query_all_recorded_dates_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    summary = context.get("summary") or {}
    total_rows = int(summary.get("recordingCount") or 0)
    rows = context.get("rows") or []
    if total_rows <= 0:
        return (
            "*바코드 전체 녹화 날짜 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            "• 결과: recordedAt 기준 녹화 기록이 없어"
        )

    unique_dates: list[str] = []
    seen_dates: set[str] = set()
    for row in rows:
        local_dt = _to_local_datetime(row.get("recordedAt"))
        if local_dt is None:
            continue
        label = local_dt.strftime("%Y-%m-%d")
        if label in seen_dates:
            continue
        seen_dates.add(label)
        unique_dates.append(label)

    if not unique_dates:
        return (
            "*바코드 전체 녹화 날짜 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            "• 결과: recordedAt 기준 녹화 기록이 없어"
        )

    unique_dates.sort()
    max_display_dates = 200
    display_dates = unique_dates[:max_display_dates]
    omitted_count = max(0, len(unique_dates) - len(display_dates))
    has_more = bool(context.get("has_more"))
    limit = int(context.get("limit") or _context_limit())

    lines = [
        "*바코드 전체 녹화 날짜 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• recordings row 수: *{total_rows}개*",
        f"• recordedAt 날짜 수(KST): *{len(unique_dates)}일*",
        f"• 첫 날짜(KST): `{unique_dates[0]}`",
        f"• 마지막 날짜(KST): `{unique_dates[-1]}`",
        "• 날짜 목록(KST):",
    ]
    for label in display_dates:
        lines.append(f"- `{label}`")

    if omitted_count > 0:
        lines.append(f"• 참고: 날짜가 많아서 `{len(display_dates)}일`만 표시했고 `{omitted_count}일`은 생략했어")
    if has_more:
        lines.append(
            f"• 참고: 최근 `{limit}개` 컨텍스트 기준 집계라 이전 녹화 날짜가 더 있을 수 있어"
        )

    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _lookup_device_names_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> list[str]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    pairs: list[tuple[Any, Any]] = []
    seen_pairs: set[tuple[Any, Any]] = set()
    for row in context.get("rows") or []:
        pair = (row.get("deviceSeq"), row.get("hospitalSeq"))
        if pair in seen_pairs:
            continue
        seen_pairs.add(pair)
        pairs.append(pair)

    if not pairs:
        return []

    names: list[str] = []
    seen_names: set[str] = set()
    limit = max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 2))
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            for device_seq, hospital_seq in pairs:
                if len(names) >= limit:
                    break
                if device_seq is None:
                    continue

                selected_name = ""
                if hospital_seq is not None:
                    cursor.execute(
                        "SELECT deviceName AS deviceName "
                        "FROM devices "
                        "WHERE seq = %s "
                        "AND hospitalSeq = %s "
                        "AND COALESCE(deviceName, '') <> '' "
                        "LIMIT 1",
                        (device_seq, hospital_seq),
                    )
                    row = cursor.fetchone() or {}
                    selected_name = _display_value(row.get("deviceName"), default="")

                if not selected_name:
                    cursor.execute(
                        "SELECT deviceName AS deviceName "
                        "FROM devices "
                        "WHERE seq = %s "
                        "AND COALESCE(deviceName, '') <> '' "
                        "LIMIT 1",
                        (device_seq,),
                    )
                    row = cursor.fetchone() or {}
                    selected_name = _display_value(row.get("deviceName"), default="")

                if not selected_name or selected_name in seen_names:
                    continue
                seen_names.add(selected_name)
                names.append(selected_name)
    finally:
        connection.close()

    return names
