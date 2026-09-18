from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.retrieval.connectors.db import _create_db_connection
from boxer_company.read_routing import (
    _resolve_weekly_recordings_report_question_target_date as _resolve_weekly_target_date_contract,
)
from boxer_company.recordings_report_options import (
    RecordingsReportOptions,
    RecordingsReportOptionsError,
)

_WEEKLY_RECORDINGS_REPORT_TIMEZONE = ZoneInfo("Asia/Seoul")
_WEEKLY_RECORDINGS_REPORT_TITLE = "주간 초음파 촬영 요약"
_WEEKLY_RECORDINGS_REPORT_TOP_HOSPITALS = 10
_WEEKLY_RECORDINGS_REPORT_MAX_CHANGE_ROWS = 10
_WEEKLY_RECORDINGS_REPORT_CHANGE_MIN_DELTA = 20
_WEEKLY_RECORDINGS_REPORT_SURGE_MIN_RATIO = 2.0
_WEEKLY_RECORDINGS_REPORT_DROP_MAX_RATIO = 0.5
_WEEKLY_RECORDINGS_ROOM_DROP_MIN_DELTA = 3


def _weekly_recordings_report_timezone() -> ZoneInfo:
    return _WEEKLY_RECORDINGS_REPORT_TIMEZONE


def _coerce_weekly_recordings_report_now(now: datetime | None = None) -> datetime:
    report_tz = _weekly_recordings_report_timezone()
    if now is None:
        return datetime.now(report_tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=report_tz)
    return now.astimezone(report_tz)


def _resolve_weekly_recordings_report_question_target_date(
    question: str,
    *,
    explicit_target_date: date | None,
    now: datetime | None = None,
) -> date | None:
    """순수 계약에 기존 주입 가능한 weekly KST clock을 연결한다."""

    return _resolve_weekly_target_date_contract(
        question,
        explicit_target_date=explicit_target_date,
        now=_coerce_weekly_recordings_report_now(now),
    )


def _weekly_recordings_report_week_start(target_date: date) -> date:
    return target_date - timedelta(days=target_date.weekday())


def _resolve_weekly_recordings_report_target_week(
    *,
    target_date: date | None = None,
    now: datetime | None = None,
) -> tuple[date, date]:
    if target_date is not None:
        week_start = _weekly_recordings_report_week_start(target_date)
        return week_start, week_start + timedelta(days=6)

    local_today = _coerce_weekly_recordings_report_now(now).date()
    current_week_start = _weekly_recordings_report_week_start(local_today)
    target_week_start = current_week_start - timedelta(days=7)
    return target_week_start, target_week_start + timedelta(days=6)


def _weekly_recordings_report_date_range_to_utc_range(
    start_date: date,
    end_date: date,
) -> tuple[datetime, datetime]:
    local_tz = _weekly_recordings_report_timezone()
    local_start = datetime.combine(start_date, time.min, tzinfo=local_tz)
    local_end = datetime.combine(end_date + timedelta(days=1), time.min, tzinfo=local_tz)
    return (
        local_start.astimezone(timezone.utc).replace(tzinfo=None),
        local_end.astimezone(timezone.utc).replace(tzinfo=None),
    )


def _load_weekly_recordings_report(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    target_date: date | None = None,
    now: datetime | None = None,
    new_barcodes_only: bool = False,
    hospital_seqs: tuple[int, ...] | None = None,
) -> dict[str, Any]:
    if hospital_seqs is not None and not hospital_seqs:
        raise RecordingsReportOptionsError("조회할 병원을 지정해줘")
    resolved_start_date = start_date
    resolved_end_date = end_date
    if resolved_start_date is None or resolved_end_date is None:
        resolved_start_date, resolved_end_date = _resolve_weekly_recordings_report_target_week(
            target_date=target_date,
            now=now,
        )

    utc_start, utc_end = _weekly_recordings_report_date_range_to_utc_range(
        resolved_start_date,
        resolved_end_date,
    )

    # 최초 촬영 확인은 전체 이력을 인덱스로 대조한다. 클라이언트와 서버 모두
    # 조회 시간을 제한하되 일반 단건 조회의 짧은 timeout과 구분한다.
    timeout = max(s.DB_QUERY_TIMEOUT_SEC, 30) if new_barcodes_only else s.DB_QUERY_TIMEOUT_SEC
    connection = _create_db_connection(timeout)
    select = "SELECT /*+ MAX_EXECUTION_TIME(25000) */ " if new_barcodes_only else "SELECT "
    first_recording_filter = ""
    hospital_filter = ""
    params = (utc_start, utc_end)
    if hospital_seqs is not None:
        # 병원 범위는 현재 집계에만 적용한다. 최초 촬영 여부는 여전히 전체 이력으로 판정한다.
        hospital_filter = "AND r.hospitalSeq IN (" + ", ".join("%s" for _ in hospital_seqs) + ") "
        params += hospital_seqs
    if new_barcodes_only:
        # 병원·진료실·장비가 바뀌어도 신규는 한 번뿐이다. 같은 촬영 시각의
        # 중복 row는 seq가 작은 한 건에 귀속하고 삭제 표시 이력도 비교한다.
        first_recording_filter = (
            "AND r.fullBarcode IS NOT NULL AND r.fullBarcode <> '' "
            "AND NOT EXISTS (SELECT 1 FROM recordings history "
            "WHERE history.fullBarcode = r.fullBarcode "
            "AND (history.recordedAt < r.recordedAt "
            "OR (history.recordedAt = r.recordedAt AND history.seq < r.seq))) "
        )
    try:
        with connection.cursor() as cursor:
            # 녹화 당시 진료실로 장비들을 합산하고 같은 결과에서 병원 합계도 만든다.
            cursor.execute(
                select +
                "r.hospitalSeq AS hospitalSeq, "
                "h.hospitalName AS hospitalName, "
                "r.hospitalRoomSeq AS hospitalRoomSeq, "
                "hr.roomName AS roomName, "
                "COUNT(*) AS rowCount "
                "FROM recordings r "
                "LEFT JOIN hospitals h ON r.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON r.hospitalRoomSeq = hr.seq "
                "AND r.hospitalSeq = hr.hospitalSeq "
                "WHERE r.recordedAt >= %s "
                "AND r.recordedAt < %s "
                + hospital_filter + first_recording_filter +
                "GROUP BY r.hospitalSeq, h.hospitalName, "
                "r.hospitalRoomSeq, hr.roomName "
                "ORDER BY rowCount DESC, r.hospitalSeq ASC",
                params,
            )
            raw_rows = cursor.fetchall() or []
    finally:
        connection.close()

    hospitals: dict[object, dict[str, Any]] = {}
    room_rows: list[dict[str, Any]] = []
    total_count = 0
    for raw_row in raw_rows:
        row_count = int(raw_row.get("rowCount") or 0)
        total_count += row_count
        raw_hospital_seq = raw_row.get("hospitalSeq")
        try:
            hospital_seq = int(raw_hospital_seq) if raw_hospital_seq is not None else None
        except (TypeError, ValueError):
            hospital_seq = raw_hospital_seq
        hospital_name = str(raw_row.get("hospitalName") or "").strip() or "미확인"
        hospital = hospitals.setdefault(
            hospital_seq,
            {"hospitalSeq": hospital_seq, "hospitalName": hospital_name, "rowCount": 0},
        )
        hospital["rowCount"] += row_count
        try:
            room_seq = int(raw_row.get("hospitalRoomSeq") or 0)
        except (TypeError, ValueError):
            room_seq = 0
        # 진료실 미지정 기록은 병원 총계에만 포함하고 장비의 현재 위치로 추정하지 않는다.
        if isinstance(hospital_seq, int) and hospital_seq > 0 and room_seq > 0:
            room_rows.append({
                "hospitalSeq": hospital_seq,
                "hospitalName": hospital_name,
                "hospitalRoomSeq": room_seq,
                "roomName": str(raw_row.get("roomName") or "").strip() or f"진료실 #{room_seq}",
                "rowCount": row_count,
            })

    rows = sorted(
        hospitals.values(),
        key=lambda row: (-row["rowCount"], row["hospitalSeq"] or 0),
    )

    return {
        "weekStartDate": resolved_start_date.isoformat(),
        "weekEndDate": resolved_end_date.isoformat(),
        "utcStart": utc_start,
        "utcEnd": utc_end,
        "hospitalCount": len(rows),
        "totalCount": total_count,
        "rows": rows,
        "roomRows": room_rows,
    }


def _weekly_recordings_report_row_key(row: dict[str, Any]) -> tuple[object, str]:
    hospital_name = str(row.get("hospitalName") or "").strip() or "미확인"
    return row.get("hospitalSeq"), hospital_name


def _weekly_recordings_report_change_rate(
    current_count: int,
    previous_count: int,
) -> float | None:
    if previous_count <= 0:
        return None
    return ((current_count - previous_count) / previous_count) * 100.0


def _matches_report_drop(
    previous: int, current: int, *, minimum_delta: int, drop_percent: float,
) -> bool:
    # 0으로 조건을 풀어도 실제 감소만 보고한다. 소수 퍼센트의 경계도 반올림 없이 비교한다.
    delta = previous - current
    return (
        previous > 0 and delta > 0 and delta >= minimum_delta
        and Decimal(delta * 100) >= Decimal(previous) * Decimal(str(drop_percent))
    )


def _build_weekly_recordings_report_change_rows(
    current_report: dict[str, Any],
    previous_report: dict[str, Any],
    *,
    direction: str,
    minimum_delta: int = _WEEKLY_RECORDINGS_REPORT_CHANGE_MIN_DELTA,
    drop_percent: float = 50,
) -> list[dict[str, Any]]:
    current_rows = current_report.get("rows") if isinstance(current_report.get("rows"), list) else []
    previous_rows = previous_report.get("rows") if isinstance(previous_report.get("rows"), list) else []
    current_by_key = {
        _weekly_recordings_report_row_key(row): row
        for row in current_rows
        if isinstance(row, dict)
    }
    previous_by_key = {
        _weekly_recordings_report_row_key(row): row
        for row in previous_rows
        if isinstance(row, dict)
    }

    result: list[dict[str, Any]] = []
    for key in set(current_by_key) | set(previous_by_key):
        current_row = current_by_key.get(key) or {}
        previous_row = previous_by_key.get(key) or {}
        current_count = int(current_row.get("rowCount") or 0)
        previous_count = int(previous_row.get("rowCount") or 0)
        delta = current_count - previous_count

        if direction == "surge":
            if delta < minimum_delta:
                continue
            if previous_count <= 0:
                if current_count < minimum_delta:
                    continue
            elif (current_count / previous_count) < _WEEKLY_RECORDINGS_REPORT_SURGE_MIN_RATIO:
                continue
        elif direction == "drop":
            if not _matches_report_drop(
                previous_count, current_count, minimum_delta=minimum_delta,
                drop_percent=drop_percent,
            ):
                continue
        else:
            raise ValueError(f"지원하지 않는 주간 리포트 변화 방향이야: {direction}")

        result.append(
            {
                "hospitalSeq": current_row.get("hospitalSeq", previous_row.get("hospitalSeq")),
                "hospitalName": str(
                    current_row.get("hospitalName")
                    or previous_row.get("hospitalName")
                    or ""
                ).strip()
                or "미확인",
                "currentCount": current_count,
                "previousCount": previous_count,
                "delta": delta,
                "changeRate": _weekly_recordings_report_change_rate(current_count, previous_count),
            }
        )

    if direction == "surge":
        result.sort(
            key=lambda row: (
                -int(row.get("delta") or 0),
                -int(row.get("currentCount") or 0),
                str(row.get("hospitalName") or ""),
            )
        )
    else:
        result.sort(
            key=lambda row: (
                int(row.get("delta") or 0),
                -int(row.get("previousCount") or 0),
                str(row.get("hospitalName") or ""),
            )
        )
    return result


def _build_weekly_recordings_room_drop_rows(
    current_report: dict[str, Any],
    previous_report: dict[str, Any],
    *,
    minimum_delta: int = _WEEKLY_RECORDINGS_ROOM_DROP_MIN_DELTA,
    drop_percent: float = 50,
) -> list[dict[str, Any]]:
    """지정한 감소율과 감소량을 모두 만족하는 진료실을 반환한다."""

    current_by_key = {
        (row["hospitalSeq"], row["hospitalRoomSeq"]): row
        for row in current_report.get("roomRows", [])
    }
    result: list[dict[str, Any]] = []
    # 전주 진료실을 기준으로 순회해야 이번 주 기록이 아예 없는 0건도 잡힌다.
    for previous in previous_report.get("roomRows", []):
        key = (previous["hospitalSeq"], previous["hospitalRoomSeq"])
        current = current_by_key.get(key)
        previous_count = int(previous["rowCount"])
        current_count = int(current["rowCount"]) if current else 0
        if not _matches_report_drop(
            previous_count, current_count, minimum_delta=minimum_delta,
            drop_percent=drop_percent,
        ):
            continue
        labels = current or previous
        result.append({
            "hospitalSeq": key[0],
            "hospitalRoomSeq": key[1],
            "hospitalName": labels["hospitalName"],
            "roomName": labels["roomName"],
            "previousCount": previous_count,
            "currentCount": current_count,
            "delta": current_count - previous_count,
            "changeRate": _weekly_recordings_report_change_rate(current_count, previous_count),
        })
    # 같은 감소량에서도 병원·진료실 ID로 정렬을 고정해 주간 출력이 흔들리지 않게 한다.
    return sorted(result, key=lambda row: (row["delta"], row["hospitalSeq"], row["hospitalRoomSeq"]))


def _resolve_report_hospitals(names: tuple[str, ...]) -> list[dict[str, Any]]:
    """정확한 이름을 우선하고, 부분 이름은 하나의 병원으로 확정될 때만 조회한다."""

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    resolved: dict[int, dict[str, Any]] = {}
    try:
        with connection.cursor() as cursor:
            for name in names:
                # 와일드카드는 병원 이름의 문자로 취급하고 입력은 전부 바인딩한다.
                escaped = name.replace("=", "==").replace("%", "=%").replace("_", "=_")
                cursor.execute(
                    "SELECT seq, hospitalName FROM hospitals "
                    "WHERE hospitalName LIKE %s ESCAPE '=' ORDER BY seq",
                    (f"%{escaped}%",),
                )
                matches = cursor.fetchall() or []
                exact = [row for row in matches if str(row["hospitalName"]).strip() == name]
                candidates = exact or matches
                if not candidates:
                    raise RecordingsReportOptionsError(
                        "지정한 병원을 찾지 못했어. '병원: 동탄제일병원'처럼 이름을 확인해줘"
                    )
                if len(candidates) != 1:
                    raise RecordingsReportOptionsError(
                        "지정한 이름에 해당하는 병원이 여러 곳이야. 지역을 포함한 정확한 병원명을 알려줘"
                    )
                row = candidates[0]
                resolved[int(row["seq"])] = row
    finally:
        connection.close()
    return list(resolved.values())


def _build_weekly_recordings_report_summary(
    *,
    target_date: date | None = None,
    now: datetime | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    include_new_barcodes: bool = False,
    options: RecordingsReportOptions | None = None,
) -> dict[str, Any]:
    if (start_date is None) != (end_date is None):
        raise ValueError("시작일과 종료일을 함께 알려줘")
    if start_date is not None and end_date is not None:
        if start_date > end_date:
            raise ValueError("시작일은 종료일보다 늦을 수 없어")
        week_start_date, week_end_date = start_date, end_date
    else:
        week_start_date, week_end_date = _resolve_weekly_recordings_report_target_week(
            target_date=target_date,
            now=now,
        )
    # 지정 기간은 양 끝 날짜를 포함하며 바로 앞의 같은 일수와 비교한다.
    period_days = (week_end_date - week_start_date).days + 1
    previous_week_start_date = week_start_date - timedelta(days=period_days)
    previous_week_end_date = week_start_date - timedelta(days=1)
    resolved_options = options or RecordingsReportOptions()
    hospitals = _resolve_report_hospitals(resolved_options.hospitals) if resolved_options.hospitals else []
    scope = {"hospital_seqs": tuple(int(row["seq"]) for row in hospitals)} if hospitals else {}
    drop_percent = 50 if resolved_options.drop_percent is None else resolved_options.drop_percent
    minimum_drop = 3 if resolved_options.minimum_drop is None else resolved_options.minimum_drop
    # 요청형 네 지표는 같은 기본값을 쓴다. 정기 발송의 기존 병원 녹화 계약은 보존한다.
    hospital_minimum = minimum_drop if include_new_barcodes or options is not None else 20
    current_report = _load_weekly_recordings_report(
        start_date=week_start_date,
        end_date=week_end_date,
        **scope,
    )
    previous_report = _load_weekly_recordings_report(
        start_date=previous_week_start_date,
        end_date=previous_week_end_date,
        **scope,
    )
    current_rows = current_report.get("rows") if isinstance(current_report.get("rows"), list) else []
    surge_rows = _build_weekly_recordings_report_change_rows(
        current_report,
        previous_report,
        direction="surge",
    )
    drop_rows = _build_weekly_recordings_report_change_rows(
        current_report,
        previous_report,
        direction="drop",
        minimum_delta=hospital_minimum,
        drop_percent=drop_percent,
    )
    room_drop_rows = _build_weekly_recordings_room_drop_rows(
        current_report, previous_report, minimum_delta=minimum_drop, drop_percent=drop_percent,
    )
    current_total = int(current_report.get("totalCount") or 0)
    previous_total = int(previous_report.get("totalCount") or 0)
    summary = {
        "weekStartDate": current_report.get("weekStartDate"),
        "weekEndDate": current_report.get("weekEndDate"),
        "previousWeekStartDate": previous_report.get("weekStartDate"),
        "previousWeekEndDate": previous_report.get("weekEndDate"),
        "hospitalCount": int(current_report.get("hospitalCount") or 0),
        "totalCount": current_total,
        "previousTotalCount": previous_total,
        "totalDelta": current_total - previous_total,
        "totalChangeRate": _weekly_recordings_report_change_rate(current_total, previous_total),
        "topRows": list(current_rows[:_WEEKLY_RECORDINGS_REPORT_TOP_HOSPITALS]),
        "topRowsLimit": _WEEKLY_RECORDINGS_REPORT_TOP_HOSPITALS,
        "surgeRows": surge_rows[:_WEEKLY_RECORDINGS_REPORT_MAX_CHANGE_ROWS],
        "surgeCount": len(surge_rows),
        "dropRows": drop_rows[:_WEEKLY_RECORDINGS_REPORT_MAX_CHANGE_ROWS],
        "dropCount": len(drop_rows),
        "changeRowsLimit": _WEEKLY_RECORDINGS_REPORT_MAX_CHANGE_ROWS,
        "roomDropRows": room_drop_rows,
        "roomDropCount": len(room_drop_rows),
    }
    if include_new_barcodes or options is not None:
        summary["queryOptions"] = {
            "hospitalNames": [row["hospitalName"] for row in hospitals],
            "dropPercent": drop_percent,
            "minimumDrop": minimum_drop,
            "hospitalRecordingMinimumDrop": hospital_minimum,
        }
    if include_new_barcodes:
        # 자동 주간 delivery의 기존 계약은 유지하고 요청형 리포트가 네 지표를 쓴다.
        current_new = _load_weekly_recordings_report(
            start_date=week_start_date, end_date=week_end_date, new_barcodes_only=True,
            **scope,
        )
        previous_new = _load_weekly_recordings_report(
            start_date=previous_week_start_date, end_date=previous_week_end_date,
            new_barcodes_only=True,
            **scope,
        )
        new_drops = _build_weekly_recordings_report_change_rows(
            current_new, previous_new, direction="drop", minimum_delta=minimum_drop,
            drop_percent=drop_percent,
        )
        new_room_drops = _build_weekly_recordings_room_drop_rows(
            current_new, previous_new, minimum_delta=minimum_drop, drop_percent=drop_percent,
        )
        current_count, previous_count = current_new["totalCount"], previous_new["totalCount"]
        summary["newBarcodes"] = {
            "hospitalCount": current_new["hospitalCount"],
            "totalCount": current_count,
            "previousTotalCount": previous_count,
            "totalDelta": current_count - previous_count,
            "totalChangeRate": _weekly_recordings_report_change_rate(current_count, previous_count),
            "topRows": current_new["rows"][:_WEEKLY_RECORDINGS_REPORT_TOP_HOSPITALS],
            "dropRows": new_drops,
            "dropCount": len(new_drops),
            "roomDropRows": new_room_drops,
            "roomDropCount": len(new_room_drops),
        }
    return summary


def _format_weekly_recordings_report_delta(value: int) -> str:
    if value > 0:
        return f"+{value}"
    return str(value)


def _format_weekly_recordings_report_change_rate_label(value: float | None) -> str:
    if value is None:
        return "신규/비교불가"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def _format_weekly_recordings_report_count(value: int, suffix: str = "개") -> str:
    return f"{int(value):,}{suffix}"


def _format_weekly_recordings_report_hospital_seq_label(value: object) -> str:
    text = str(value).strip() if value is not None else ""
    return f"#{text}" if text else "#미확인"


def _format_weekly_recordings_report_range_label(
    start_date: str | None,
    end_date: str | None,
) -> str:
    normalized_start_date = str(start_date or "").strip() or "미확인"
    normalized_end_date = str(end_date or "").strip() or "미확인"
    return f"{normalized_start_date} ~ {normalized_end_date}"


def _build_weekly_recordings_report_top_row_lines(rows: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for index, row in enumerate(rows, start=1):
        hospital_name = str(row.get("hospitalName") or "").strip() or "미확인"
        row_count = int(row.get("rowCount") or 0)
        lines.append(
            " ".join(
                [
                    f"{index}.",
                    f"*{hospital_name}*",
                    f"`{_format_weekly_recordings_report_hospital_seq_label(row.get('hospitalSeq'))}`",
                    f"`{_format_weekly_recordings_report_count(row_count)}`",
                ]
            )
        )
    return lines


def _build_weekly_recordings_report_change_lines(rows: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for index, row in enumerate(rows, start=1):
        hospital_name = str(row.get("hospitalName") or "").strip() or "미확인"
        previous_count = int(row.get("previousCount") or 0)
        current_count = int(row.get("currentCount") or 0)
        delta = int(row.get("delta") or 0)
        change_rate = _format_weekly_recordings_report_change_rate_label(row.get("changeRate"))
        lines.append(
            " ".join(
                [
                    f"{index}.",
                    f"*{hospital_name}*",
                    f"`{_format_weekly_recordings_report_hospital_seq_label(row.get('hospitalSeq'))}`",
                    f"`{previous_count:,} -> {current_count:,}`",
                    f"`{_format_weekly_recordings_report_delta(delta)}`",
                    f"(`{change_rate}`)",
                ]
            )
        )
    return lines


def _format_weekly_recordings_report(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
    include_title: bool = True,
) -> str:
    local_now = _coerce_weekly_recordings_report_now(now)
    current_week_label = _format_weekly_recordings_report_range_label(
        report_summary.get("weekStartDate"),
        report_summary.get("weekEndDate"),
    )
    previous_week_label = _format_weekly_recordings_report_range_label(
        report_summary.get("previousWeekStartDate"),
        report_summary.get("previousWeekEndDate"),
    )
    hospital_count = int(report_summary.get("hospitalCount") or 0)
    total_count = int(report_summary.get("totalCount") or 0)
    previous_total_count = int(report_summary.get("previousTotalCount") or 0)
    total_delta = int(report_summary.get("totalDelta") or 0)
    total_change_rate = report_summary.get("totalChangeRate")
    top_rows = report_summary.get("topRows") if isinstance(report_summary.get("topRows"), list) else []
    top_rows_limit = int(report_summary.get("topRowsLimit") or _WEEKLY_RECORDINGS_REPORT_TOP_HOSPITALS)
    surge_rows = report_summary.get("surgeRows") if isinstance(report_summary.get("surgeRows"), list) else []
    surge_count = int(report_summary.get("surgeCount") or len(surge_rows))
    drop_rows = report_summary.get("dropRows") if isinstance(report_summary.get("dropRows"), list) else []
    drop_count = int(report_summary.get("dropCount") or len(drop_rows))
    top_row_lines = _build_weekly_recordings_report_top_row_lines(top_rows)
    surge_lines = _build_weekly_recordings_report_change_lines(surge_rows)
    drop_lines = _build_weekly_recordings_report_change_lines(drop_rows)

    lines: list[str] = []
    if include_title:
        lines.append(f"*{_WEEKLY_RECORDINGS_REPORT_TITLE}*")

    lines.extend(
        [
        f"• 기준 주간: `{current_week_label}` | 비교 주간: `{previous_week_label}`",
        f"• 발송: `{local_now:%Y-%m-%d %H:%M:%S} KST`",
        f"• 전체 row: `{_format_weekly_recordings_report_count(total_count)}` | 병원: `{hospital_count:,}곳`",
        (
            "• 전주 대비: "
            f"`{previous_total_count:,} -> {total_count:,}` "
            f"(`{_format_weekly_recordings_report_delta(total_delta)}`, "
            f"`{_format_weekly_recordings_report_change_rate_label(total_change_rate)}`)"
        ),
        f"• 변화 병원: 급증 `{surge_count:,}곳` | 급감 `{drop_count:,}곳`",
        ]
    )

    if total_count <= 0:
        lines.append("• 결과: 해당 주간 recordings row가 없어")
    else:
        lines.append("")
        lines.append(f"*상위 병원 Top {top_rows_limit}*")
        lines.extend(top_row_lines)
        if hospital_count > len(top_rows):
            lines.append(f"• 참고: 상위 `{len(top_rows):,}곳`만 표시")

    if surge_rows:
        lines.append("")
        lines.append("*급증*")
        lines.extend(surge_lines)
        if surge_count > len(surge_rows):
            lines.append(f"• 참고: 급증은 상위 `{len(surge_rows):,}곳`만 표시")
    else:
        lines.append("")
        lines.append("*급증*")
        lines.append("• 없어")

    if drop_rows:
        lines.append("")
        lines.append("*급감*")
        lines.extend(drop_lines)
        if drop_count > len(drop_rows):
            lines.append(f"• 참고: 급감은 상위 `{len(drop_rows):,}곳`만 표시")
    else:
        lines.append("")
        lines.append("*급감*")
        lines.append("• 없어")

    # 전체 촬영이 0건이어도 진료실 급감 근거는 사용자 요청형 답변에 남긴다.
    if "roomDropRows" in report_summary:
        room_drop_rows = report_summary["roomDropRows"]
        lines.extend(["", f"*진료실별 녹화 급감* `{len(room_drop_rows):,}곳` (전주 대비 50% 이상·3건 이상 감소)"])
        for index, row in enumerate(room_drop_rows, 1):
            lines.append(
                f"{index}. *{row['hospitalName']} · {row['roomName']}* "
                f"`{row['previousCount']:,}건 → {row['currentCount']:,}건` · "
                f"`{row['delta']:+,}건` (`{row['changeRate']:.1f}%`)"
            )
        if not room_drop_rows:
            lines.append("• 없어")

    return "\n".join(lines)
