import os
import socket
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

import pymysql
try:
    import paramiko
except ImportError:  # pragma: no cover - runtime guard
    paramiko = None

from boxer_company import settings as cs
from boxer.core import settings as s
from boxer.core.utils import _display_value, _format_datetime, _truncate_text
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company.routers.mda_graphql import (
    _get_mda_devices_details,
    _is_mda_graphql_configured,
    _wait_for_mda_device_agent_ssh,
)


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


def _local_year_to_utc_range(target_year: int) -> tuple[datetime, datetime]:
    local_tz = _local_zone()
    local_start = datetime(
        year=int(target_year),
        month=1,
        day=1,
        hour=0,
        minute=0,
        second=0,
        tzinfo=local_tz,
    )
    local_end = datetime(
        year=int(target_year) + 1,
        month=1,
        day=1,
        hour=0,
        minute=0,
        second=0,
        tzinfo=local_tz,
    )
    return (
        local_start.astimezone(timezone.utc).replace(tzinfo=None),
        local_end.astimezone(timezone.utc).replace(tzinfo=None),
    )


def _format_recorded_at_local(value: object) -> str:
    if isinstance(value, datetime):
        local_tz = _local_zone()
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        localized = value.astimezone(local_tz)
        return localized.strftime("%Y-%m-%d %H:%M:%S")
    return _format_datetime(value)


def _format_active_flag_label(value: object) -> str:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return _display_value(value, default="미확인")
    return "활성" if normalized == 1 else "비활성" if normalized == 0 else str(normalized)


def _format_install_flag_label(value: object) -> str:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        return _display_value(value, default="미확인")
    return "설치" if normalized == 1 else "미설치" if normalized == 0 else str(normalized)


def _format_ssh_status_display(value: object) -> str:
    text = _display_value(value, default="미확인")
    if text == "연결 가능":
        return "🔵 *연결 가능*"
    if text == "연결 불가":
        return "🔴 *연결 불가*"
    if text == "미확인":
        return "⚪ *미확인*"
    return f"⚪ *{text}*"


def _format_device_download_availability_display(value: object) -> str:
    text = _display_value(value, default="미확인")
    if text == "가능":
        return "🔵 *가능*"
    if text == "불가":
        return "🔴 *불가*"
    if text == "미확인":
        return "⚪ *미확인*"
    return f"⚪ *{text}*"


def _derive_device_download_availability(ssh_status: object) -> str:
    normalized = _display_value(ssh_status, default="미확인")
    if normalized == "연결 가능":
        return "가능"
    if normalized == "연결 불가":
        return "불가"
    return "미확인"


def _lookup_device_ssh_status(device_name: str) -> str:
    normalized_name = str(device_name or "").strip()
    if not normalized_name or not _is_mda_graphql_configured() or not cs.DEVICE_SSH_PASSWORD or paramiko is None:
        return "미확인"

    try:
        wait_result = _wait_for_mda_device_agent_ssh(
            normalized_name,
            poll_timeout_sec=min(10, max(1, cs.MDA_SSH_POLL_TIMEOUT_SEC)),
        )
    except Exception:
        return "미확인"

    if not isinstance(wait_result, dict) or not wait_result.get("ready"):
        return "연결 불가"

    device_info = wait_result.get("device") if isinstance(wait_result.get("device"), dict) else {}
    agent_ssh = device_info.get("agentSsh") if isinstance(device_info, dict) else None
    host = str((agent_ssh or {}).get("host") or "").strip()
    port = (agent_ssh or {}).get("port")
    try:
        port = int(port)
    except (TypeError, ValueError):
        port = 0
    if not host or port <= 0:
        return "연결 불가"

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(
            hostname=host,
            port=port,
            username=cs.DEVICE_SSH_USER,
            password=cs.DEVICE_SSH_PASSWORD,
            timeout=max(1, cs.DEVICE_SSH_CONNECT_TIMEOUT_SEC),
            banner_timeout=max(1, cs.DEVICE_SSH_CONNECT_TIMEOUT_SEC),
            auth_timeout=max(1, cs.DEVICE_SSH_CONNECT_TIMEOUT_SEC),
            look_for_keys=False,
            allow_agent=False,
        )
    except (
        paramiko.AuthenticationException,
        paramiko.SSHException,
        paramiko.ssh_exception.NoValidConnectionsError,
        socket.timeout,
        TimeoutError,
        OSError,
    ):
        return "연결 불가"
    finally:
        client.close()
    return "연결 가능"


def _lookup_mda_device_details(device_names: list[object]) -> dict[str, dict[str, Any]]:
    normalized_names: list[str] = []
    seen_names: set[str] = set()
    for raw_name in device_names:
        normalized_name = str(raw_name or "").strip()
        if not normalized_name or normalized_name in seen_names:
            continue
        seen_names.add(normalized_name)
        normalized_names.append(normalized_name)

    if not normalized_names or not _is_mda_graphql_configured():
        return {}

    try:
        return _get_mda_devices_details(normalized_names)
    except Exception:
        return {}


def _build_device_detail_lines(
    row: dict[str, Any],
    *,
    line_prefix: str,
    ssh_status: str | None = None,
) -> list[str]:
    lines = [
        f"{line_prefix}장비 번호: `{_display_value(row.get('seq'), default='미확인')}`",
        f"{line_prefix}장비명: `{_display_value(row.get('deviceName'), default='미확인')}`",
        f"{line_prefix}버전: `{_display_value(row.get('version'), default='미확인')}`",
        f"{line_prefix}병원: `{_display_value(row.get('hospitalName'), default='미확인')}`",
        f"{line_prefix}병실: `{_display_value(row.get('roomName'), default='미확인')}`",
    ]
    if ssh_status is not None:
        lines.append(f"{line_prefix}SSH 연결 상태: {_format_ssh_status_display(ssh_status)}")
        lines.append(
            f"{line_prefix}초음파 영상 다운로드 가능 상태: "
            f"{_format_device_download_availability_display(_derive_device_download_availability(ssh_status))}"
        )
    lines.extend(
        [
            f"{line_prefix}캡처보드 종류: `{_display_value(row.get('captureBoardType'), default='미확인')}`",
            f"{line_prefix}status: `{_display_value(row.get('status'), default='미확인')}`",
            f"{line_prefix}활성 유무: `{_format_active_flag_label(row.get('activeFlag'))}`",
            f"{line_prefix}설치 유무: `{_format_install_flag_label(row.get('installFlag'))}`",
        ]
    )
    description = _display_value(row.get("description"), default="")
    if description:
        lines.append(f"{line_prefix}description: `{description}`")
    return lines


def _format_video_length(value: object) -> str:
    try:
        total_seconds = int(value)
    except (TypeError, ValueError):
        return "미확인"

    if total_seconds < 0:
        return "미확인"

    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if hours > 0:
        human = f"{hours}시간 {minutes}분 {seconds}초"
    elif minutes > 0:
        human = f"{minutes}분 {seconds}초"
    else:
        human = f"{seconds}초"
    return f"{total_seconds}초 ({human})"


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


def _lookup_hospital_seq_by_name(hospital_name: str) -> int | None:
    normalized_name = str(hospital_name or "").strip()
    if not normalized_name:
        return None
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT seq, activeFlag FROM hospitals "
                "WHERE hospitalName = %s "
                "ORDER BY activeFlag DESC, seq DESC LIMIT 5",
                (normalized_name,),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    if len(rows) == 1:
        try:
            return int((rows[0] or {}).get("seq"))
        except (AttributeError, TypeError, ValueError):
            return None

    active_rows = []
    for row in rows:
        try:
            if int((row or {}).get("activeFlag")) == 1:
                active_rows.append(row)
        except (AttributeError, TypeError, ValueError):
            continue

    if len(active_rows) != 1:
        return None
    try:
        return int((active_rows[0] or {}).get("seq"))
    except (AttributeError, TypeError, ValueError):
        return None


def _lookup_hospital_room_seq_by_name(
    room_name: str,
    *,
    hospital_seq: int | None = None,
) -> int | None:
    normalized_name = str(room_name or "").strip()
    if not normalized_name:
        return None
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    where_clauses = ["roomName = %s"]
    params: list[object] = [normalized_name]
    if hospital_seq is not None:
        where_clauses.append("hospitalSeq = %s")
        params.append(int(hospital_seq))

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT seq FROM hospital_rooms "
                f"WHERE {' AND '.join(where_clauses)} "
                "ORDER BY seq DESC LIMIT 2",
                tuple(params),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    if len(rows) != 1:
        return None
    try:
        return int((rows[0] or {}).get("seq"))
    except (AttributeError, TypeError, ValueError):
        return None


def _context_limit() -> int:
    return max(1, min(200, cs.RECORDINGS_CONTEXT_LIMIT))


def _baby_ai_context_limit() -> int:
    return max(1, min(5, int(s.DB_QUERY_MAX_ROWS or 20)))


def _build_baby_magic_cdn_url(s3_file_key: object) -> str:
    base_url = (cs.BABY_MAGIC_CDN_BASE_URL or "").strip().rstrip("/")
    file_key = str(s3_file_key or "").strip().lstrip("/")
    if not base_url or not file_key:
        return ""
    return f"{base_url}/{file_key}"


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
                "r.videoLength, "
                "r.streamingStatus, "
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


def _query_recordings_list_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    summary = context.get("summary") or {}
    total_rows = int(summary.get("recordingCount") or 0)
    rows = context.get("rows") or []
    has_more = bool(context.get("has_more"))
    limit = int(context.get("limit") or _context_limit())

    if total_rows <= 0 or not rows:
        return (
            "*바코드 영상 목록 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            "• 결과: 영상 기록이 없어"
        )

    lines = [
        "*바코드 영상 목록 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• recordings row 수: *{total_rows}개*",
        "• 영상 목록(최근순):",
    ]

    for index, row in enumerate(rows, start=1):
        recorded_at = row.get("recordedAt")
        created_at = row.get("createdAt")
        time_label = "미확인"
        if isinstance(recorded_at, datetime):
            time_label = _format_recorded_at_local(recorded_at)
        elif isinstance(created_at, datetime):
            time_label = _format_recorded_at_local(created_at)

        hospital_name = _display_value(row.get("hospitalName"), default="미확인")
        room_name = _display_value(row.get("roomName"), default="미확인")
        streaming_status = _display_value(row.get("streamingStatus"), default="미확인")
        lines.extend(
            [
                f"- {index}.",
                f"  날짜(KST): `{time_label}`",
                f"  병원: `{hospital_name}`",
                f"  병실: `{room_name}`",
                f"  streamingStatus: `{streaming_status}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()

    if has_more:
        lines.append(f"• 참고: 최근 `{limit}개`만 표시했고 이전 영상은 생략했어")

    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_baby_ai_list_by_barcode(barcode: str, target_date: str | None = None) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    limit = _baby_ai_context_limit()
    summary_conditions = [
        "barcode = %s",
        "deletedAt IS NULL",
        "COALESCE(babyMagicImageS3FileKey, '') <> ''",
    ]
    row_conditions = [
        "ba.barcode = %s",
        "ba.deletedAt IS NULL",
        "COALESCE(ba.babyMagicImageS3FileKey, '') <> ''",
    ]
    summary_params: list[Any] = [barcode]
    row_params: list[Any] = [barcode]

    if target_date:
        utc_start, utc_end = _local_date_to_utc_range(target_date)
        summary_conditions.extend(["createdAt >= %s", "createdAt < %s"])
        row_conditions.extend(["ba.createdAt >= %s", "ba.createdAt < %s"])
        summary_params.extend([utc_start, utc_end])
        row_params.extend([utc_start, utc_end])

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS babyAiCount, "
                "MIN(createdAt) AS firstCreatedAt, "
                "MAX(createdAt) AS lastCreatedAt "
                "FROM baby_ai "
                f"WHERE {' AND '.join(summary_conditions)}",
                tuple(summary_params),
            )
            summary = cursor.fetchone() or {}

            row_params_with_limit = row_params + [limit]
            cursor.execute(
                "SELECT "
                "ba.seq, "
                "ba.captureSeq, "
                "ba.recordingSeq, "
                "ba.fileId, "
                "ba.babyMagicImageS3FileKey, "
                "ba.visibleFlag, "
                "ba.billableVisibleFlag, "
                "ba.regenerationReason, "
                "ba.createdAt, "
                "ba.webhookSentAt, "
                "h.hospitalName AS hospitalName, "
                "hr.roomName AS roomName, "
                "d.deviceName AS deviceName "
                "FROM baby_ai ba "
                "LEFT JOIN hospitals h ON ba.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON ba.hospitalRoomSeq = hr.seq "
                "LEFT JOIN devices d ON ba.deviceSeq = d.seq "
                f"WHERE {' AND '.join(row_conditions)} "
                "ORDER BY ba.createdAt DESC, ba.seq DESC "
                "LIMIT %s",
                tuple(row_params_with_limit),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    total_rows = int(summary.get("babyAiCount") or 0)
    has_more = total_rows > len(rows)

    if total_rows <= 0 or not rows:
        result_line = "• 결과: 베이비매직 기록이 없어"
        if target_date:
            result_line = f"• 결과: `{target_date}` createdAt(KST) 기준 베이비매직 기록이 없어"
        return (
            "*바코드 베이비매직 목록 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            + result_line
        )

    lines = [
        "*바코드 베이비매직 목록 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• baby_ai row 수: *{total_rows}개*",
    ]
    if target_date:
        lines.append(f"• createdAt 날짜(KST): `{target_date}`")
    lines.append(
        "• 베이비매직 목록(최근순):",
    )

    for index, row in enumerate(rows, start=1):
        created_at_label = _format_recorded_at_local(row.get("createdAt"))
        hospital_name = _display_value(row.get("hospitalName"), default="미확인")
        room_name = _display_value(row.get("roomName"), default="미확인")
        device_name = _display_value(row.get("deviceName"), default="미확인")
        visible_flag = "공개" if int(row.get("visibleFlag") or 0) == 1 else "비공개"
        regeneration_reason = _display_value(row.get("regenerationReason"), default="없음")
        baby_magic_url = _build_baby_magic_cdn_url(row.get("babyMagicImageS3FileKey"))
        baby_magic_link = f"<{baby_magic_url}|열기>" if baby_magic_url else "없음"
        webhook_sent_at = row.get("webhookSentAt")
        webhook_status = "미전송"
        if webhook_sent_at:
            webhook_status = f"성공 (`{_format_recorded_at_local(webhook_sent_at)}`)"
        lines.extend(
            [
                f"- {index}. createdAt(KST): `{created_at_label}` | 병원: `{hospital_name}` | 병실: `{room_name}` | 장비: `{device_name}`",
                f"  공개 상태: `{visible_flag}` | 앱 발송: {webhook_status}",
                f"  결과 링크: {baby_magic_link}",
                f"  regenerationReason: `{regeneration_reason}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()

    if has_more:
        lines.append(f"• 참고: 최근 `{limit}개`만 표시했고 이전 베이비매직은 생략했어")

    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_recordings_detail_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    summary = context.get("summary") or {}
    total_rows = int(summary.get("recordingCount") or 0)
    rows = context.get("rows") or []
    has_more = bool(context.get("has_more"))
    limit = int(context.get("limit") or _context_limit())

    if total_rows <= 0 or not rows:
        return (
            "*바코드 영상별 정보 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            "• 결과: 영상 기록이 없어"
        )

    lines = [
        "*바코드 영상별 정보 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• recordings row 수: *{total_rows}개*",
        "• 영상별 정보(최근순):",
    ]

    for index, row in enumerate(rows, start=1):
        recorded_at = row.get("recordedAt")
        created_at = row.get("createdAt")
        recorded_at_label = _format_recorded_at_local(recorded_at) if isinstance(recorded_at, datetime) else "미확인"
        created_at_label = _format_recorded_at_local(created_at) if isinstance(created_at, datetime) else "미확인"
        hospital_name = _display_value(row.get("hospitalName"), default="미확인")
        room_name = _display_value(row.get("roomName"), default="미확인")
        length_label = _format_video_length(row.get("videoLength"))
        streaming_status = _display_value(row.get("streamingStatus"), default="미확인")
        lines.extend(
            [
                f"- {index}.",
                f"  recordedAt(KST): `{recorded_at_label}`",
                f"  createdAt(KST): `{created_at_label}`",
                f"  videoLength: `{length_label}`",
                f"  streamingStatus: `{streaming_status}`",
                f"  병원: `{hospital_name}`",
                f"  병실: `{room_name}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()

    if has_more:
        lines.append(f"• 참고: 최근 `{limit}개`만 표시했고 이전 영상은 생략했어")

    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_recordings_length_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> str:
    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    summary = context.get("summary") or {}
    total_rows = int(summary.get("recordingCount") or 0)
    rows = context.get("rows") or []
    has_more = bool(context.get("has_more"))
    limit = int(context.get("limit") or _context_limit())

    if total_rows <= 0 or not rows:
        return (
            "*바코드 영상 길이 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            "• 결과: 영상 기록이 없어"
        )

    lines = [
        "*바코드 영상 길이 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• recordings row 수: *{total_rows}개*",
        "• 영상 길이 목록(최근순):",
    ]

    for index, row in enumerate(rows, start=1):
        recorded_at = row.get("recordedAt")
        created_at = row.get("createdAt")
        time_label = "미확인"
        time_key = "recordedAt"
        if isinstance(recorded_at, datetime):
            time_label = _format_recorded_at_local(recorded_at)
            time_key = "recordedAt"
        elif isinstance(created_at, datetime):
            time_label = _format_recorded_at_local(created_at)
            time_key = "createdAt"

        hospital_name = _display_value(row.get("hospitalName"), default="미확인")
        room_name = _display_value(row.get("roomName"), default="미확인")
        length_label = _format_video_length(row.get("videoLength"))
        streaming_status = _display_value(row.get("streamingStatus"), default="미확인")
        lines.append(
            f"- {index}. {time_key}(KST): `{time_label}` | videoLength: `{length_label}` | streamingStatus: `{streaming_status}` | 병원: `{hospital_name}` | 병실: `{room_name}`"
        )

    if has_more:
        lines.append(f"• 참고: 최근 `{limit}개`만 표시했고 이전 영상은 생략했어")

    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


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
            "• 결과: 날짜 기준 recordings DB row가 없어",
            "• 참고: 실제 녹화 시도가 있었더라도 영상 손상 또는 업로드/DB 기록 생성 실패 가능성은 별도 로그 확인이 필요해",
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


def _load_recordings_rows_on_date_by_barcode(
    barcode: str,
    target_date: str,
    *,
    device_seq: int | None = None,
) -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    utc_start, utc_end = _local_date_to_utc_range(target_date)
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            sql = (
                "SELECT "
                "r.seq, "
                "r.deviceSeq, "
                "r.videoLength, "
                "r.streamingStatus, "
                "r.recordedAt, "
                "r.createdAt "
                "FROM recordings r "
                "WHERE r.fullBarcode = %s "
                "AND r.recordedAt >= %s "
                "AND r.recordedAt < %s "
            )
            params: list[Any] = [barcode, utc_start, utc_end]
            if device_seq is not None:
                sql += "AND r.deviceSeq = %s "
                params.append(device_seq)
            sql += "ORDER BY COALESCE(r.recordedAt, r.createdAt) DESC, r.seq DESC"
            cursor.execute(sql, tuple(params))
            return cursor.fetchall() or []
    finally:
        connection.close()


def _query_recordings_length_on_date_by_barcode(
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

    has_more = bool(context.get("has_more"))
    limit = int(context.get("limit") or _context_limit())
    if not matched_rows:
        lines = [
            "*바코드 날짜별 영상 길이 조회 결과*",
            f"• 바코드: `{barcode}`",
            f"• 날짜(KST): `{target_date}`",
            "• 결과: recordedAt 기준 녹화 기록이 없어",
        ]
        if has_more:
            lines.append(f"• 참고: 최근 `{limit}개` 컨텍스트 기준 결과야(전체는 더 있을 수 있어)")
        return "\n".join(lines)

    lines = [
        "*바코드 날짜별 영상 길이 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜(KST): `{target_date}`",
        f"• recordings row 수: *{len(matched_rows)}개*",
        "• 영상 길이 목록:",
    ]

    for index, row in enumerate(matched_rows, start=1):
        recorded_at = row.get("recordedAt")
        created_at = row.get("createdAt")
        time_label = "미확인"
        time_key = "recordedAt"
        if isinstance(recorded_at, datetime):
            time_label = _format_recorded_at_local(recorded_at)
            time_key = "recordedAt"
        elif isinstance(created_at, datetime):
            time_label = _format_recorded_at_local(created_at)
            time_key = "createdAt"

        hospital_name = _display_value(row.get("hospitalName"), default="미확인")
        room_name = _display_value(row.get("roomName"), default="미확인")
        length_label = _format_video_length(row.get("videoLength"))
        streaming_status = _display_value(row.get("streamingStatus"), default="미확인")
        lines.append(
            f"- {index}. {time_key}(KST): `{time_label}` | videoLength: `{length_label}` | streamingStatus: `{streaming_status}` | 병원: `{hospital_name}` | 병실: `{room_name}`"
        )

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


def _format_barcode_label(value: object) -> str:
    try:
        return f"{int(value):011d}"
    except (TypeError, ValueError):
        return _display_value(value, default="미확인")


def _query_ultrasound_captures(
    *,
    barcode: str | None = None,
    target_date: str | None = None,
    hospital_seq: int | None = None,
    hospital_room_seq: int | None = None,
    count_only: bool = False,
) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized_barcode = str(barcode or "").strip() or None
    if hospital_seq is not None:
        hospital_seq = int(hospital_seq)
    if hospital_room_seq is not None:
        hospital_room_seq = int(hospital_room_seq)

    if not any((normalized_barcode, target_date, hospital_seq is not None, hospital_room_seq is not None)):
        raise ValueError("캡처 조회는 barcode, 날짜, hospitalSeq, hospitalRoomSeq 중 최소 1개 조건이 필요해")

    where_clauses: list[str] = []
    params: list[object] = []
    if normalized_barcode:
        where_clauses.append("uc.barcode = %s")
        params.append(int(normalized_barcode))
    if target_date:
        utc_start, utc_end = _local_date_to_utc_range(target_date)
        where_clauses.append("uc.capturedAt >= %s")
        where_clauses.append("uc.capturedAt < %s")
        params.extend([utc_start, utc_end])
    if hospital_seq is not None:
        where_clauses.append("uc.hospitalSeq = %s")
        params.append(hospital_seq)
    if hospital_room_seq is not None:
        where_clauses.append("uc.hospitalRoomSeq = %s")
        params.append(hospital_room_seq)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1 = 1"
    limit = max(1, min(100, cs.RECORDINGS_CONTEXT_LIMIT))

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS captureCount "
                "FROM ultrasound_captures uc "
                f"WHERE {where_sql}",
                tuple(params),
            )
            summary = cursor.fetchone() or {}
            total_count = int(summary.get("captureCount") or 0)

            rows: list[dict[str, Any]] = []
            if not count_only and total_count > 0:
                cursor.execute(
                    "SELECT "
                    "uc.seq, "
                    "uc.barcode, "
                    "uc.fileId, "
                    "uc.hospitalSeq, "
                    "uc.hospitalRoomSeq, "
                    "uc.deviceSeq, "
                    "uc.s3Bucket, "
                    "uc.s3FileKey, "
                    "uc.studyInstanceUid, "
                    "uc.capturedAt, "
                    "uc.createdAt, "
                    "h.hospitalName AS hospitalName, "
                    "hr.roomName AS roomName "
                    "FROM ultrasound_captures uc "
                    "LEFT JOIN hospitals h ON uc.hospitalSeq = h.seq "
                    "LEFT JOIN hospital_rooms hr ON uc.hospitalRoomSeq = hr.seq "
                    f"WHERE {where_sql} "
                    "ORDER BY uc.capturedAt DESC, uc.seq DESC "
                    "LIMIT %s",
                    tuple([*params, limit]),
                )
                rows = cursor.fetchall() or []
    finally:
        connection.close()

    title = "*초음파 캡처 조회 결과*"
    lines = [title]
    if normalized_barcode:
        lines.append(f"• 바코드: `{normalized_barcode}`")
    if target_date:
        lines.append(f"• 날짜(KST): `{target_date}`")
    if hospital_seq is not None:
        lines.append(f"• hospitalSeq: `{hospital_seq}`")
    if hospital_room_seq is not None:
        lines.append(f"• hospitalRoomSeq: `{hospital_room_seq}`")
    lines.append(f"• ultrasound_captures row 수: *{total_count}개*")

    if count_only:
        return "\n".join(lines)

    if total_count <= 0:
        lines.append("• 결과: 조건에 맞는 캡처 기록이 없어")
        return "\n".join(lines)

    lines.append("• 캡처 목록(최근순):")
    for index, row in enumerate(rows, start=1):
        lines.extend(
            [
                f"- {index}.",
                f"  barcode: `{_format_barcode_label(row.get('barcode'))}`",
                f"  capturedAt(KST): `{_format_recorded_at_local(row.get('capturedAt'))}`",
                f"  fileId: `{_display_value(row.get('fileId'), default='미확인')}`",
                f"  hospitalSeq: `{_display_value(row.get('hospitalSeq'), default='미확인')}`",
                f"  hospitalRoomSeq: `{_display_value(row.get('hospitalRoomSeq'), default='미확인')}`",
                f"  병원: `{_display_value(row.get('hospitalName'), default='미확인')}`",
                f"  병실: `{_display_value(row.get('roomName'), default='미확인')}`",
                f"  s3FileKey: `{_display_value(row.get('s3FileKey'), default='미확인')}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()
    if total_count > len(rows):
        lines.append(f"• 참고: 최근 `{len(rows)}개`만 표시했고 이전 캡처는 생략했어")
    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_recordings_by_filters(
    *,
    barcode: str | None = None,
    target_date: str | None = None,
    target_year: int | None = None,
    hospital_name: str | None = None,
    room_name: str | None = None,
    hospital_seq: int | None = None,
    hospital_room_seq: int | None = None,
    count_only: bool = False,
) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized_barcode = str(barcode or "").strip() or None
    normalized_hospital_name = str(hospital_name or "").strip() or None
    normalized_room_name = str(room_name or "").strip() or None
    resolved_hospital_seq = hospital_seq
    resolved_hospital_room_seq = hospital_room_seq
    if hospital_seq is not None:
        hospital_seq = int(hospital_seq)
        resolved_hospital_seq = hospital_seq
    if hospital_room_seq is not None:
        hospital_room_seq = int(hospital_room_seq)
        resolved_hospital_room_seq = hospital_room_seq
    if resolved_hospital_seq is None and normalized_hospital_name:
        resolved_hospital_seq = _lookup_hospital_seq_by_name(normalized_hospital_name)
    if resolved_hospital_room_seq is None and normalized_room_name and resolved_hospital_seq is not None:
        resolved_hospital_room_seq = _lookup_hospital_room_seq_by_name(
            normalized_room_name,
            hospital_seq=resolved_hospital_seq,
        )

    if not any(
        (
            normalized_barcode,
            target_date,
            target_year is not None,
            normalized_hospital_name,
            normalized_room_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
        )
    ):
        raise ValueError("영상 조회는 barcode, 날짜, 연도, 병원명, 병실명, hospitalSeq, hospitalRoomSeq 중 최소 1개 조건이 필요해")

    where_clauses: list[str] = []
    params: list[object] = []
    if normalized_barcode:
        where_clauses.append("(CAST(r.fullBarcode AS CHAR) = %s OR CAST(r.barcode AS CHAR) = %s)")
        params.extend([normalized_barcode, normalized_barcode])
    if target_date:
        utc_start, utc_end = _local_date_to_utc_range(target_date)
        where_clauses.append("r.recordedAt >= %s")
        where_clauses.append("r.recordedAt < %s")
        params.extend([utc_start, utc_end])
    elif target_year is not None:
        utc_start, utc_end = _local_year_to_utc_range(target_year)
        where_clauses.append("r.recordedAt >= %s")
        where_clauses.append("r.recordedAt < %s")
        params.extend([utc_start, utc_end])
    if resolved_hospital_seq is None and normalized_hospital_name:
        where_clauses.append("h.hospitalName = %s")
        params.append(normalized_hospital_name)
    if normalized_room_name and resolved_hospital_room_seq is None:
        where_clauses.append("hr.roomName = %s")
        params.append(normalized_room_name)
    if resolved_hospital_seq is not None:
        where_clauses.append("r.hospitalSeq = %s")
        params.append(resolved_hospital_seq)
    if resolved_hospital_room_seq is not None:
        where_clauses.append("r.hospitalRoomSeq = %s")
        params.append(resolved_hospital_room_seq)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1 = 1"
    limit = max(1, min(100, cs.RECORDINGS_CONTEXT_LIMIT))
    count_join_clauses: list[str] = []
    if normalized_hospital_name and resolved_hospital_seq is None:
        count_join_clauses.append("LEFT JOIN hospitals h ON r.hospitalSeq = h.seq")
    if normalized_room_name and resolved_hospital_room_seq is None:
        count_join_clauses.append("LEFT JOIN hospital_rooms hr ON r.hospitalRoomSeq = hr.seq")
    count_join_sql = (" " + " ".join(count_join_clauses)) if count_join_clauses else ""

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS recordingCount "
                "FROM recordings r"
                f"{count_join_sql} "
                f"WHERE {where_sql}",
                tuple(params),
            )
            summary = cursor.fetchone() or {}
            total_count = int(summary.get("recordingCount") or 0)

            rows: list[dict[str, Any]] = []
            if not count_only and total_count > 0:
                cursor.execute(
                    "SELECT "
                    "r.seq, "
                    "r.fullBarcode, "
                    "r.barcode, "
                    "r.fileId, "
                    "r.hospitalSeq, "
                    "r.hospitalRoomSeq, "
                    "r.deviceSeq, "
                    "r.videoLength, "
                    "r.streamingStatus, "
                    "r.s3FileKey, "
                    "r.recordedAt, "
                    "r.createdAt, "
                    "h.hospitalName AS hospitalName, "
                    "hr.roomName AS roomName "
                    "FROM recordings r "
                    "LEFT JOIN hospitals h ON r.hospitalSeq = h.seq "
                    "LEFT JOIN hospital_rooms hr ON r.hospitalRoomSeq = hr.seq "
                    f"WHERE {where_sql} "
                    "ORDER BY COALESCE(r.recordedAt, r.createdAt) DESC, r.seq DESC "
                    "LIMIT %s",
                    tuple([*params, limit]),
                )
                rows = cursor.fetchall() or []
    finally:
        connection.close()

    lines = ["*영상 조회 결과*"]
    if normalized_barcode:
        lines.append(f"• 바코드: `{normalized_barcode}`")
    if target_date:
        lines.append(f"• 날짜(KST): `{target_date}`")
    elif target_year is not None:
        lines.append(f"• 연도(KST): `{target_year}`")
    if normalized_hospital_name:
        lines.append(f"• 병원: `{normalized_hospital_name}`")
    if normalized_room_name:
        lines.append(f"• 병실: `{normalized_room_name}`")
    if hospital_seq is not None:
        lines.append(f"• hospitalSeq: `{hospital_seq}`")
    if hospital_room_seq is not None:
        lines.append(f"• hospitalRoomSeq: `{hospital_room_seq}`")
    lines.append(f"• recordings row 수: *{total_count}개*")

    if count_only:
        return "\n".join(lines)

    if total_count <= 0:
        lines.append("• 결과: 조건에 맞는 영상 기록이 없어")
        return "\n".join(lines)

    lines.append("• 영상 목록(최근순):")
    for index, row in enumerate(rows, start=1):
        barcode_label = _display_value(row.get("fullBarcode"), default="") or _format_barcode_label(row.get("barcode"))
        lines.extend(
            [
                f"- {index}.",
                f"  바코드: `{barcode_label}`",
                f"  recordedAt(KST): `{_format_recorded_at_local(row.get('recordedAt'))}`",
                f"  fileId: `{_display_value(row.get('fileId'), default='미확인')}`",
                f"  streamingStatus: `{_display_value(row.get('streamingStatus'), default='미확인')}`",
                f"  병원: `{_display_value(row.get('hospitalName'), default='미확인')}`",
                f"  병실: `{_display_value(row.get('roomName'), default='미확인')}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()
    if total_count > len(rows):
        lines.append(f"• 참고: 최근 `{len(rows)}개`만 표시했고 이전 영상은 생략했어")
    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_ultrasound_captures_by_filters(
    *,
    barcode: str | None = None,
    target_date: str | None = None,
    target_year: int | None = None,
    hospital_name: str | None = None,
    room_name: str | None = None,
    hospital_seq: int | None = None,
    hospital_room_seq: int | None = None,
    count_only: bool = False,
) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized_barcode = str(barcode or "").strip() or None
    normalized_hospital_name = str(hospital_name or "").strip() or None
    normalized_room_name = str(room_name or "").strip() or None
    resolved_hospital_seq = hospital_seq
    resolved_hospital_room_seq = hospital_room_seq
    if hospital_seq is not None:
        hospital_seq = int(hospital_seq)
        resolved_hospital_seq = hospital_seq
    if hospital_room_seq is not None:
        hospital_room_seq = int(hospital_room_seq)
        resolved_hospital_room_seq = hospital_room_seq
    if resolved_hospital_seq is None and normalized_hospital_name:
        resolved_hospital_seq = _lookup_hospital_seq_by_name(normalized_hospital_name)
    if resolved_hospital_room_seq is None and normalized_room_name and resolved_hospital_seq is not None:
        resolved_hospital_room_seq = _lookup_hospital_room_seq_by_name(
            normalized_room_name,
            hospital_seq=resolved_hospital_seq,
        )

    if not any(
        (
            normalized_barcode,
            target_date,
            target_year is not None,
            normalized_hospital_name,
            normalized_room_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
        )
    ):
        raise ValueError("캡처 조회는 barcode, 날짜, 연도, 병원명, 병실명, hospitalSeq, hospitalRoomSeq 중 최소 1개 조건이 필요해")

    where_clauses: list[str] = []
    params: list[object] = []
    if normalized_barcode:
        where_clauses.append("CAST(uc.barcode AS CHAR) = %s")
        params.append(normalized_barcode)
    if target_date:
        utc_start, utc_end = _local_date_to_utc_range(target_date)
        where_clauses.append("uc.capturedAt >= %s")
        where_clauses.append("uc.capturedAt < %s")
        params.extend([utc_start, utc_end])
    elif target_year is not None:
        utc_start, utc_end = _local_year_to_utc_range(target_year)
        where_clauses.append("uc.capturedAt >= %s")
        where_clauses.append("uc.capturedAt < %s")
        params.extend([utc_start, utc_end])
    if resolved_hospital_seq is None and normalized_hospital_name:
        where_clauses.append("h.hospitalName = %s")
        params.append(normalized_hospital_name)
    if normalized_room_name and resolved_hospital_room_seq is None:
        where_clauses.append("hr.roomName = %s")
        params.append(normalized_room_name)
    if resolved_hospital_seq is not None:
        where_clauses.append("uc.hospitalSeq = %s")
        params.append(resolved_hospital_seq)
    if resolved_hospital_room_seq is not None:
        where_clauses.append("uc.hospitalRoomSeq = %s")
        params.append(resolved_hospital_room_seq)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1 = 1"
    limit = max(1, min(100, cs.RECORDINGS_CONTEXT_LIMIT))
    count_join_clauses: list[str] = []
    if normalized_hospital_name and resolved_hospital_seq is None:
        count_join_clauses.append("LEFT JOIN hospitals h ON uc.hospitalSeq = h.seq")
    if normalized_room_name and resolved_hospital_room_seq is None:
        count_join_clauses.append("LEFT JOIN hospital_rooms hr ON uc.hospitalRoomSeq = hr.seq")
    count_join_sql = (" " + " ".join(count_join_clauses)) if count_join_clauses else ""

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS captureCount "
                "FROM ultrasound_captures uc"
                f"{count_join_sql} "
                f"WHERE {where_sql}",
                tuple(params),
            )
            summary = cursor.fetchone() or {}
            total_count = int(summary.get("captureCount") or 0)

            rows: list[dict[str, Any]] = []
            if not count_only and total_count > 0:
                cursor.execute(
                    "SELECT "
                    "uc.seq, "
                    "uc.barcode, "
                    "uc.fileId, "
                    "uc.hospitalSeq, "
                    "uc.hospitalRoomSeq, "
                    "uc.deviceSeq, "
                    "uc.s3Bucket, "
                    "uc.s3FileKey, "
                    "uc.capturedAt, "
                    "h.hospitalName AS hospitalName, "
                    "hr.roomName AS roomName "
                    "FROM ultrasound_captures uc "
                    "LEFT JOIN hospitals h ON uc.hospitalSeq = h.seq "
                    "LEFT JOIN hospital_rooms hr ON uc.hospitalRoomSeq = hr.seq "
                    f"WHERE {where_sql} "
                    "ORDER BY uc.capturedAt DESC, uc.seq DESC "
                    "LIMIT %s",
                    tuple([*params, limit]),
                )
                rows = cursor.fetchall() or []
    finally:
        connection.close()

    lines = ["*초음파 캡처 조회 결과*"]
    if normalized_barcode:
        lines.append(f"• 바코드: `{normalized_barcode}`")
    if target_date:
        lines.append(f"• 날짜(KST): `{target_date}`")
    elif target_year is not None:
        lines.append(f"• 연도(KST): `{target_year}`")
    if normalized_hospital_name:
        lines.append(f"• 병원: `{normalized_hospital_name}`")
    if normalized_room_name:
        lines.append(f"• 병실: `{normalized_room_name}`")
    if hospital_seq is not None:
        lines.append(f"• hospitalSeq: `{hospital_seq}`")
    if hospital_room_seq is not None:
        lines.append(f"• hospitalRoomSeq: `{hospital_room_seq}`")
    lines.append(f"• ultrasound_captures row 수: *{total_count}개*")

    if count_only:
        return "\n".join(lines)

    if total_count <= 0:
        lines.append("• 결과: 조건에 맞는 캡처 기록이 없어")
        return "\n".join(lines)

    lines.append("• 캡처 목록(최근순):")
    for index, row in enumerate(rows, start=1):
        lines.extend(
            [
                f"- {index}.",
                f"  바코드: `{_format_barcode_label(row.get('barcode'))}`",
                f"  capturedAt(KST): `{_format_recorded_at_local(row.get('capturedAt'))}`",
                f"  fileId: `{_display_value(row.get('fileId'), default='미확인')}`",
                f"  병원: `{_display_value(row.get('hospitalName'), default='미확인')}`",
                f"  병실: `{_display_value(row.get('roomName'), default='미확인')}`",
                f"  s3FileKey: `{_display_value(row.get('s3FileKey'), default='미확인')}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()
    if total_count > len(rows):
        lines.append(f"• 참고: 최근 `{len(rows)}개`만 표시했고 이전 캡처는 생략했어")
    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_hospitals_by_filters(
    *,
    hospital_name: str | None = None,
    hospital_seq: int | None = None,
    target_date: str | None = None,
    target_year: int | None = None,
    count_only: bool = False,
) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized_hospital_name = str(hospital_name or "").strip() or None
    resolved_hospital_seq = hospital_seq
    if hospital_seq is not None:
        hospital_seq = int(hospital_seq)
        resolved_hospital_seq = hospital_seq
    if resolved_hospital_seq is None and normalized_hospital_name:
        resolved_hospital_seq = _lookup_hospital_seq_by_name(normalized_hospital_name)

    if not any((normalized_hospital_name, hospital_seq is not None, target_date, target_year is not None)):
        raise ValueError("병원 조회는 병원명, hospitalSeq, 날짜, 연도 중 최소 1개 조건이 필요해")

    where_clauses: list[str] = []
    params: list[object] = []
    if normalized_hospital_name and resolved_hospital_seq is None:
        where_clauses.append("h.hospitalName = %s")
        params.append(normalized_hospital_name)
    if resolved_hospital_seq is not None:
        where_clauses.append("h.seq = %s")
        params.append(resolved_hospital_seq)
    if target_date:
        utc_start, utc_end = _local_date_to_utc_range(target_date)
        where_clauses.append("h.createdAt >= %s")
        where_clauses.append("h.createdAt < %s")
        params.extend([utc_start, utc_end])
    elif target_year is not None:
        utc_start, utc_end = _local_year_to_utc_range(target_year)
        where_clauses.append("h.createdAt >= %s")
        where_clauses.append("h.createdAt < %s")
        params.extend([utc_start, utc_end])

    where_sql = " AND ".join(where_clauses) if where_clauses else "1 = 1"
    limit = max(1, min(100, cs.RECORDINGS_CONTEXT_LIMIT))

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS hospitalCount "
                "FROM hospitals h "
                f"WHERE {where_sql}",
                tuple(params),
            )
            summary = cursor.fetchone() or {}
            total_count = int(summary.get("hospitalCount") or 0)

            rows: list[dict[str, Any]] = []
            if not count_only and total_count > 0:
                cursor.execute(
                    "SELECT "
                    "h.seq, "
                    "h.hospitalName, "
                    "h.regionAlias, "
                    "h.province, "
                    "h.city, "
                    "h.telephone, "
                    "h.activeFlag, "
                    "h.isTest, "
                    "h.isMaternity, "
                    "h.externalVisible, "
                    "h.createdAt, "
                    "h.updatedAt, "
                    "h.deletedAt "
                    "FROM hospitals h "
                    f"WHERE {where_sql} "
                    "ORDER BY h.createdAt DESC, h.seq DESC "
                    "LIMIT %s",
                    tuple([*params, limit]),
                )
                rows = cursor.fetchall() or []
    finally:
        connection.close()

    lines = ["*병원 조회 결과*"]
    if normalized_hospital_name:
        lines.append(f"• 병원: `{normalized_hospital_name}`")
    if hospital_seq is not None:
        lines.append(f"• hospitalSeq: `{hospital_seq}`")
    if target_date:
        lines.append(f"• 병원 생성일(KST): `{target_date}`")
    elif target_year is not None:
        lines.append(f"• 병원 생성연도(KST): `{target_year}`")
    lines.append(f"• hospitals row 수: *{total_count}개*")

    if count_only:
        return "\n".join(lines)

    if total_count <= 0:
        lines.append("• 결과: 조건에 맞는 병원 기록이 없어")
        return "\n".join(lines)

    lines.append("• 병원 목록(최근 생성순):")
    for index, row in enumerate(rows, start=1):
        region_parts = [
            str(row.get("province") or "").strip(),
            str(row.get("city") or "").strip(),
        ]
        region = " ".join(part for part in region_parts if part).strip() or _display_value(
            row.get("regionAlias"),
            default="미확인",
        )
        deleted_at = _display_value(row.get("deletedAt"), default="")
        status_parts = [
            f"activeFlag={_display_value(row.get('activeFlag'), default='미확인')}",
            f"isTest={_display_value(row.get('isTest'), default='미확인')}",
            f"isMaternity={_display_value(row.get('isMaternity'), default='미확인')}",
            f"externalVisible={_display_value(row.get('externalVisible'), default='미확인')}",
        ]
        if deleted_at:
            status_parts.append(f"deletedAt={deleted_at}")
        lines.extend(
            [
                f"- {index}.",
                f"  hospitalSeq: `{_display_value(row.get('seq'), default='미확인')}`",
                f"  병원: `{_display_value(row.get('hospitalName'), default='미확인')}`",
                f"  지역: `{region}`",
                f"  전화번호: `{_display_value(row.get('telephone'), default='미확인')}`",
                f"  병원 생성일(KST): `{_format_recorded_at_local(row.get('createdAt'))}`",
                f"  updatedAt(KST): `{_format_recorded_at_local(row.get('updatedAt'))}`",
                f"  상태: `{' | '.join(status_parts)}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()
    if total_count > len(rows):
        lines.append(f"• 참고: 최근 `{len(rows)}개`만 표시했고 이전 병원은 생략했어")
    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_hospital_rooms_by_filters(
    *,
    hospital_name: str | None = None,
    room_name: str | None = None,
    hospital_seq: int | None = None,
    hospital_room_seq: int | None = None,
    count_only: bool = False,
) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized_hospital_name = str(hospital_name or "").strip() or None
    normalized_room_name = str(room_name or "").strip() or None
    resolved_hospital_seq = hospital_seq
    if hospital_seq is not None:
        hospital_seq = int(hospital_seq)
        resolved_hospital_seq = hospital_seq
    if hospital_room_seq is not None:
        hospital_room_seq = int(hospital_room_seq)
    if resolved_hospital_seq is None and normalized_hospital_name:
        resolved_hospital_seq = _lookup_hospital_seq_by_name(normalized_hospital_name)

    if not any((normalized_hospital_name, hospital_seq is not None, hospital_room_seq is not None)):
        raise ValueError("병실 조회는 병원명, hospitalSeq, hospitalRoomSeq 중 최소 1개 조건이 필요해")

    where_clauses: list[str] = []
    params: list[object] = []
    if normalized_hospital_name and resolved_hospital_seq is None:
        where_clauses.append("h.hospitalName = %s")
        params.append(normalized_hospital_name)
    if normalized_room_name:
        where_clauses.append("hr.roomName = %s")
        params.append(normalized_room_name)
    if resolved_hospital_seq is not None:
        where_clauses.append("hr.hospitalSeq = %s")
        params.append(resolved_hospital_seq)
    if hospital_room_seq is not None:
        where_clauses.append("hr.seq = %s")
        params.append(hospital_room_seq)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1 = 1"
    limit = max(1, min(100, cs.RECORDINGS_CONTEXT_LIMIT))
    count_join_sql = " LEFT JOIN hospitals h ON hr.hospitalSeq = h.seq" if normalized_hospital_name and resolved_hospital_seq is None else ""

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS roomCount "
                "FROM hospital_rooms hr "
                f"{count_join_sql} "
                f"WHERE {where_sql}",
                tuple(params),
            )
            summary = cursor.fetchone() or {}
            total_count = int(summary.get("roomCount") or 0)

            rows: list[dict[str, Any]] = []
            if not count_only and total_count > 0:
                cursor.execute(
                    "SELECT "
                    "hr.seq, "
                    "hr.hospitalSeq, "
                    "hr.roomName, "
                    "hr.doctorName, "
                    "hr.memo, "
                    "hr.activeFlag, "
                    "hr.createdAt, "
                    "hr.updatedAt, "
                    "h.hospitalName AS hospitalName "
                    "FROM hospital_rooms hr "
                    "LEFT JOIN hospitals h ON hr.hospitalSeq = h.seq "
                    f"WHERE {where_sql} "
                    "ORDER BY hr.createdAt DESC, hr.seq DESC "
                    "LIMIT %s",
                    tuple([*params, limit]),
                )
                rows = cursor.fetchall() or []
    finally:
        connection.close()

    lines = ["*병실 조회 결과*"]
    if normalized_hospital_name:
        lines.append(f"• 병원: `{normalized_hospital_name}`")
    if normalized_room_name:
        lines.append(f"• 병실: `{normalized_room_name}`")
    if hospital_seq is not None:
        lines.append(f"• hospitalSeq: `{hospital_seq}`")
    if hospital_room_seq is not None:
        lines.append(f"• hospitalRoomSeq: `{hospital_room_seq}`")
    lines.append(f"• hospital_rooms row 수: *{total_count}개*")

    if count_only:
        return "\n".join(lines)

    if total_count <= 0:
        lines.append("• 결과: 조건에 맞는 병실 기록이 없어")
        return "\n".join(lines)

    lines.append("• 병실 목록(최근 생성순):")
    for index, row in enumerate(rows, start=1):
        status = _display_value(row.get("activeFlag"), default="미확인")
        lines.extend(
            [
                f"- {index}.",
                f"  hospitalRoomSeq: `{_display_value(row.get('seq'), default='미확인')}`",
                f"  병원: `{_display_value(row.get('hospitalName'), default='미확인')}`",
                f"  병실: `{_display_value(row.get('roomName'), default='미확인')}`",
                f"  담당의: `{_display_value(row.get('doctorName'), default='미확인')}`",
                f"  병실 생성일(KST): `{_format_recorded_at_local(row.get('createdAt'))}`",
                f"  updatedAt(KST): `{_format_recorded_at_local(row.get('updatedAt'))}`",
                f"  상태: `activeFlag={status}`",
                "",
            ]
        )

    if lines and lines[-1] == "":
        lines.pop()
    if total_count > len(rows):
        lines.append(f"• 참고: 최근 `{len(rows)}개`만 표시했고 이전 병실은 생략했어")
    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _query_devices_by_filters(
    *,
    device_name: str | None = None,
    device_seq: int | None = None,
    hospital_name: str | None = None,
    room_name: str | None = None,
    hospital_seq: int | None = None,
    hospital_room_seq: int | None = None,
    status: str | None = None,
    active_flag: int | None = None,
    install_flag: int | None = None,
    count_only: bool = False,
) -> str:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized_device_name = str(device_name or "").strip() or None
    normalized_hospital_name = str(hospital_name or "").strip() or None
    normalized_room_name = str(room_name or "").strip() or None
    normalized_status = str(status or "").strip() or None
    resolved_hospital_seq = hospital_seq
    resolved_hospital_room_seq = hospital_room_seq
    if device_seq is not None:
        device_seq = int(device_seq)
    if hospital_seq is not None:
        hospital_seq = int(hospital_seq)
        resolved_hospital_seq = hospital_seq
    if hospital_room_seq is not None:
        hospital_room_seq = int(hospital_room_seq)
        resolved_hospital_room_seq = hospital_room_seq
    if active_flag is not None:
        active_flag = int(active_flag)
    if install_flag is not None:
        install_flag = int(install_flag)
    if resolved_hospital_seq is None and normalized_hospital_name:
        resolved_hospital_seq = _lookup_hospital_seq_by_name(normalized_hospital_name)
    if resolved_hospital_room_seq is None and normalized_room_name and resolved_hospital_seq is not None:
        resolved_hospital_room_seq = _lookup_hospital_room_seq_by_name(
            normalized_room_name,
            hospital_seq=resolved_hospital_seq,
        )

    if not any(
        (
            normalized_device_name,
            device_seq is not None,
            normalized_hospital_name,
            normalized_room_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
            normalized_status,
            active_flag is not None,
            install_flag is not None,
        )
    ):
        raise ValueError(
            "장비 조회는 장비명, deviceSeq, 병원명, 병실명, hospitalSeq, hospitalRoomSeq, status, activeFlag, installFlag 중 최소 1개 조건이 필요해"
        )

    where_clauses: list[str] = []
    params: list[object] = []
    if normalized_device_name:
        where_clauses.append("d.deviceName = %s")
        params.append(normalized_device_name)
    if device_seq is not None:
        where_clauses.append("d.seq = %s")
        params.append(device_seq)
    if normalized_hospital_name and resolved_hospital_seq is None:
        where_clauses.append("h.hospitalName = %s")
        params.append(normalized_hospital_name)
    if normalized_room_name and resolved_hospital_room_seq is None:
        where_clauses.append("hr.roomName = %s")
        params.append(normalized_room_name)
    if resolved_hospital_seq is not None:
        where_clauses.append("d.hospitalSeq = %s")
        params.append(resolved_hospital_seq)
    if resolved_hospital_room_seq is not None:
        where_clauses.append("d.hospitalRoomSeq = %s")
        params.append(resolved_hospital_room_seq)
    if normalized_status:
        where_clauses.append("UPPER(COALESCE(d.status, '')) = UPPER(%s)")
        params.append(normalized_status)
    if active_flag is not None:
        where_clauses.append("d.activeFlag = %s")
        params.append(active_flag)
    if install_flag is not None:
        where_clauses.append("d.installFlag = %s")
        params.append(install_flag)

    where_sql = " AND ".join(where_clauses) if where_clauses else "1 = 1"
    limit = max(1, min(100, cs.RECORDINGS_CONTEXT_LIMIT))
    count_join_clauses: list[str] = []
    if normalized_hospital_name and resolved_hospital_seq is None:
        count_join_clauses.append("LEFT JOIN hospitals h ON d.hospitalSeq = h.seq")
    if normalized_room_name and resolved_hospital_room_seq is None:
        count_join_clauses.append("LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq")
    count_join_sql = (" " + " ".join(count_join_clauses)) if count_join_clauses else ""

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS deviceCount "
                "FROM devices d "
                f"{count_join_sql} "
                f"WHERE {where_sql}",
                tuple(params),
            )
            summary = cursor.fetchone() or {}
            total_count = int(summary.get("deviceCount") or 0)

            rows: list[dict[str, Any]] = []
            if not count_only and total_count > 0:
                cursor.execute(
                    "SELECT "
                    "d.seq, "
                    "d.hospitalSeq, "
                    "d.hospitalRoomSeq, "
                    "d.deviceName, "
                    "d.description, "
                    "d.status, "
                    "d.activeFlag, "
                    "d.installFlag, "
                    "h.hospitalName AS hospitalName, "
                    "hr.roomName AS roomName "
                    "FROM devices d "
                    "LEFT JOIN hospitals h ON d.hospitalSeq = h.seq "
                    "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                    f"WHERE {where_sql} "
                    "ORDER BY d.seq DESC "
                    "LIMIT %s",
                    tuple([*params, limit]),
                )
                rows = cursor.fetchall() or []
    finally:
        connection.close()

    if rows:
        detail_by_name = _lookup_mda_device_details([row.get("deviceName") for row in rows])
        if detail_by_name:
            for row in rows:
                device_name = _display_value(row.get("deviceName"), default="")
                detail = detail_by_name.get(device_name)
                if not isinstance(detail, dict):
                    continue
                version = _display_value(detail.get("version"), default="")
                if version:
                    row["version"] = version
                capture_board_type = _display_value(detail.get("captureBoardType"), default="")
                if capture_board_type:
                    row["captureBoardType"] = capture_board_type

    lines = ["*장비 조회 결과*"]
    summary_lines: list[str] = []
    if normalized_device_name:
        summary_lines.append(f"• 장비명: `{normalized_device_name}`")
    if device_seq is not None:
        summary_lines.append(f"• 장비 번호: `{device_seq}`")
    if normalized_hospital_name:
        summary_lines.append(f"• 병원: `{normalized_hospital_name}`")
    if normalized_room_name:
        summary_lines.append(f"• 병실: `{normalized_room_name}`")
    if hospital_seq is not None:
        summary_lines.append(f"• hospitalSeq: `{hospital_seq}`")
    if hospital_room_seq is not None:
        summary_lines.append(f"• hospitalRoomSeq: `{hospital_room_seq}`")
    if normalized_status:
        summary_lines.append(f"• status: `{normalized_status}`")
    if active_flag is not None:
        summary_lines.append(f"• 활성 유무: `{_format_active_flag_label(active_flag)}`")
    if install_flag is not None:
        summary_lines.append(f"• 설치 유무: `{_format_install_flag_label(install_flag)}`")
    summary_lines.append(f"• devices row 수: *{total_count}개*")

    if count_only:
        return "\n".join([*lines, *summary_lines])

    if total_count <= 0:
        lines.extend(summary_lines)
        lines.append("• 결과: 조건에 맞는 장비 기록이 없어")
        return "\n".join(lines)

    if total_count == 1 and rows:
        ssh_status = _lookup_device_ssh_status(_display_value(rows[0].get("deviceName"), default=""))
        lines.extend(_build_device_detail_lines(rows[0], line_prefix="• ", ssh_status=ssh_status))
        return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))

    lines.extend(summary_lines)
    lines.append("• 장비 목록(최신 seq순):")
    for index, row in enumerate(rows, start=1):
        lines.append(f"- {index}.")
        lines.extend(_build_device_detail_lines(row, line_prefix="  "))
        lines.append("")

    if lines and lines[-1] == "":
        lines.pop()
    if total_count > len(rows):
        lines.append(f"• 참고: 최근 `{len(rows)}개`만 표시했고 이전 장비는 생략했어")
    return _truncate_text("\n".join(lines), max(1, s.DB_QUERY_MAX_RESULT_CHARS))


def _lookup_device_contexts_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    context = recordings_context or _load_recordings_context_by_barcode(barcode)
    pair_meta_map: dict[tuple[Any, Any], dict[str, Any]] = {}
    for row in context.get("rows") or []:
        device_seq = row.get("deviceSeq")
        hospital_seq = row.get("hospitalSeq")
        if device_seq is None:
            continue
        pair = (device_seq, hospital_seq)
        meta = pair_meta_map.get(pair)
        if meta is None:
            pair_meta_map[pair] = {
                "deviceSeq": device_seq,
                "hospitalSeq": hospital_seq,
                "hospitalRoomSeq": row.get("hospitalRoomSeq"),
                "hospitalName": row.get("hospitalName"),
                "roomName": row.get("roomName"),
            }
            continue

        if not meta.get("hospitalName") and row.get("hospitalName"):
            meta["hospitalName"] = row.get("hospitalName")
        if not meta.get("roomName") and row.get("roomName"):
            meta["roomName"] = row.get("roomName")
        if not meta.get("hospitalRoomSeq") and row.get("hospitalRoomSeq"):
            meta["hospitalRoomSeq"] = row.get("hospitalRoomSeq")

    if not pair_meta_map:
        return []

    items: list[dict[str, Any]] = []
    seen_device_keys: set[tuple[str, str, str]] = set()
    limit = max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 2))
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            for pair, meta in pair_meta_map.items():
                if len(items) >= limit:
                    break
                device_seq, hospital_seq = pair

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

                if not selected_name:
                    continue

                hospital_name = _display_value(meta.get("hospitalName"), default="")
                room_name = _display_value(meta.get("roomName"), default="")
                dedupe_key = (selected_name, hospital_name, room_name)
                if dedupe_key in seen_device_keys:
                    continue
                seen_device_keys.add(dedupe_key)

                items.append(
                    {
                        "deviceName": selected_name,
                        "deviceSeq": device_seq,
                        "hospitalSeq": hospital_seq,
                        "hospitalRoomSeq": meta.get("hospitalRoomSeq"),
                        "hospitalName": meta.get("hospitalName"),
                        "roomName": meta.get("roomName"),
                    }
                )
    finally:
        connection.close()

    return items


def _lookup_device_names_by_barcode(
    barcode: str,
    recordings_context: dict[str, Any] | None = None,
) -> list[str]:
    items = _lookup_device_contexts_by_barcode(
        barcode,
        recordings_context=recordings_context,
    )
    return [str(item.get("deviceName")) for item in items if item.get("deviceName")]


def _lookup_device_contexts_by_hospital_room(
    hospital_name: str,
    room_name: str,
) -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized_hospital_name = str(hospital_name or "").strip()
    normalized_room_name = str(room_name or "").strip()
    if not normalized_hospital_name or not normalized_room_name:
        return []

    hospital_seq = _lookup_hospital_seq_by_name(normalized_hospital_name)
    if hospital_seq is None:
        return []
    hospital_room_seq = _lookup_hospital_room_seq_by_name(
        normalized_room_name,
        hospital_seq=hospital_seq,
    )
    if hospital_room_seq is None:
        return []

    limit = max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 4))
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT "
                "d.seq AS deviceSeq, "
                "d.deviceName AS deviceName "
                "FROM devices d "
                "WHERE d.hospitalSeq = %s "
                "AND d.hospitalRoomSeq = %s "
                "AND COALESCE(d.deviceName, '') <> '' "
                "ORDER BY d.seq DESC "
                "LIMIT %s",
                (
                    hospital_seq,
                    hospital_room_seq,
                    limit,
                ),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    items: list[dict[str, Any]] = []
    seen_device_names: set[str] = set()
    for row in rows:
        device_name = _display_value(row.get("deviceName"), default="")
        if not device_name or device_name in seen_device_names:
            continue
        seen_device_names.add(device_name)
        items.append(
            {
                "deviceName": device_name,
                "deviceSeq": row.get("deviceSeq"),
                "hospitalSeq": hospital_seq,
                "hospitalRoomSeq": hospital_room_seq,
                "hospitalName": normalized_hospital_name,
                "roomName": normalized_room_name,
            }
        )
    return items


def _lookup_device_contexts_by_hospital_seqs(
    hospital_seqs: list[int] | tuple[int, ...] | set[int],
) -> list[dict[str, Any]]:
    if not s.DB_HOST or not s.DB_USERNAME or not s.DB_PASSWORD or not s.DB_DATABASE:
        raise RuntimeError("DB 접속 정보(DB_*)가 비어 있어")

    normalized = [int(seq) for seq in hospital_seqs if seq is not None]
    if not normalized:
        return []

    seen_hospital_seqs: set[int] = set()
    ordered_hospital_seqs: list[int] = []
    for seq in normalized:
        if seq in seen_hospital_seqs:
            continue
        seen_hospital_seqs.add(seq)
        ordered_hospital_seqs.append(seq)

    limit = max(1, min(100, cs.LOG_ANALYSIS_MAX_DEVICES * 8))
    placeholders = ", ".join(["%s"] * len(ordered_hospital_seqs))
    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT "
                "d.seq AS deviceSeq, "
                "d.deviceName AS deviceName, "
                "d.hospitalSeq AS hospitalSeq, "
                "d.hospitalRoomSeq AS hospitalRoomSeq, "
                "h.hospitalName AS hospitalName, "
                "hr.roomName AS roomName "
                "FROM devices d "
                "INNER JOIN hospitals h ON d.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON d.hospitalRoomSeq = hr.seq "
                f"WHERE d.hospitalSeq IN ({placeholders}) "
                "AND COALESCE(d.deviceName, '') <> '' "
                "ORDER BY d.hospitalSeq ASC, d.seq DESC "
                "LIMIT %s",
                tuple(ordered_hospital_seqs) + (limit,),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    items: list[dict[str, Any]] = []
    seen_device_names: set[str] = set()
    for row in rows:
        device_name = _display_value(row.get("deviceName"), default="")
        if not device_name or device_name in seen_device_names:
            continue
        seen_device_names.add(device_name)
        items.append(
            {
                "deviceName": device_name,
                "deviceSeq": row.get("deviceSeq"),
                "hospitalSeq": row.get("hospitalSeq"),
                "hospitalRoomSeq": row.get("hospitalRoomSeq"),
                "hospitalName": row.get("hospitalName"),
                "roomName": row.get("roomName"),
            }
        )
    return items
