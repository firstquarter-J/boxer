from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.retrieval.connectors.db import _create_db_connection

_WEEKLY_RECORDINGS_REPORT_TIMEZONE = ZoneInfo("Asia/Seoul")
_WEEKLY_RECORDINGS_REPORT_TOP_HOSPITALS = 10
_WEEKLY_RECORDINGS_REPORT_MAX_CHANGE_ROWS = 10
_WEEKLY_RECORDINGS_REPORT_CHANGE_MIN_DELTA = 20
_WEEKLY_RECORDINGS_REPORT_SURGE_MIN_RATIO = 2.0
_WEEKLY_RECORDINGS_REPORT_DROP_MAX_RATIO = 0.5


def _weekly_recordings_report_timezone() -> ZoneInfo:
    return _WEEKLY_RECORDINGS_REPORT_TIMEZONE


def _coerce_weekly_recordings_report_now(now: datetime | None = None) -> datetime:
    report_tz = _weekly_recordings_report_timezone()
    if now is None:
        return datetime.now(report_tz)
    if now.tzinfo is None:
        return now.replace(tzinfo=report_tz)
    return now.astimezone(report_tz)


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
) -> dict[str, Any]:
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

    connection = _create_db_connection(s.DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT "
                "r.hospitalSeq AS hospitalSeq, "
                "h.hospitalName AS hospitalName, "
                "COUNT(*) AS rowCount "
                "FROM recordings r "
                "LEFT JOIN hospitals h ON r.hospitalSeq = h.seq "
                "WHERE r.recordedAt >= %s "
                "AND r.recordedAt < %s "
                "GROUP BY r.hospitalSeq, h.hospitalName "
                "ORDER BY rowCount DESC, r.hospitalSeq ASC",
                (utc_start, utc_end),
            )
            raw_rows = cursor.fetchall() or []
    finally:
        connection.close()

    rows: list[dict[str, Any]] = []
    total_count = 0
    for raw_row in raw_rows:
        row_count = int(raw_row.get("rowCount") or 0)
        total_count += row_count
        raw_hospital_seq = raw_row.get("hospitalSeq")
        try:
            hospital_seq = int(raw_hospital_seq) if raw_hospital_seq is not None else None
        except (TypeError, ValueError):
            hospital_seq = raw_hospital_seq
        rows.append(
            {
                "hospitalSeq": hospital_seq,
                "hospitalName": str(raw_row.get("hospitalName") or "").strip() or "미확인",
                "rowCount": row_count,
            }
        )

    return {
        "weekStartDate": resolved_start_date.isoformat(),
        "weekEndDate": resolved_end_date.isoformat(),
        "utcStart": utc_start,
        "utcEnd": utc_end,
        "hospitalCount": len(rows),
        "totalCount": total_count,
        "rows": rows,
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


def _build_weekly_recordings_report_change_rows(
    current_report: dict[str, Any],
    previous_report: dict[str, Any],
    *,
    direction: str,
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
            if delta < _WEEKLY_RECORDINGS_REPORT_CHANGE_MIN_DELTA:
                continue
            if previous_count <= 0:
                if current_count < _WEEKLY_RECORDINGS_REPORT_CHANGE_MIN_DELTA:
                    continue
            elif (current_count / previous_count) < _WEEKLY_RECORDINGS_REPORT_SURGE_MIN_RATIO:
                continue
        elif direction == "drop":
            if (-delta) < _WEEKLY_RECORDINGS_REPORT_CHANGE_MIN_DELTA:
                continue
            if previous_count <= 0:
                continue
            if (current_count / previous_count) > _WEEKLY_RECORDINGS_REPORT_DROP_MAX_RATIO:
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


def _build_weekly_recordings_report_summary(
    *,
    target_date: date | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    week_start_date, week_end_date = _resolve_weekly_recordings_report_target_week(
        target_date=target_date,
        now=now,
    )
    previous_week_start_date = week_start_date - timedelta(days=7)
    previous_week_end_date = week_end_date - timedelta(days=7)
    current_report = _load_weekly_recordings_report(
        start_date=week_start_date,
        end_date=week_end_date,
    )
    previous_report = _load_weekly_recordings_report(
        start_date=previous_week_start_date,
        end_date=previous_week_end_date,
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
    )
    current_total = int(current_report.get("totalCount") or 0)
    previous_total = int(previous_report.get("totalCount") or 0)
    return {
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
    }


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

    lines = [
        "*주간 Recordings 요약*",
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

    if total_count <= 0:
        lines.append("• 결과: 해당 주간 recordings row가 없어")
        return "\n".join(lines)

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

    return "\n".join(lines)


def _build_weekly_recordings_report_blocks(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
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

    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "주간 Recordings 요약",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": (
                        f"기준 주간 `{current_week_label}` | 비교 주간 `{previous_week_label}` | "
                        f"발송 `{local_now:%Y-%m-%d %H:%M:%S} KST`"
                    ),
                }
            ],
        },
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"*전체 row*\n`{_format_weekly_recordings_report_count(total_count)}`",
                },
                {
                    "type": "mrkdwn",
                    "text": f"*집계 병원*\n`{hospital_count:,}곳`",
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "*전주 대비*\n"
                        f"`{previous_total_count:,} -> {total_count:,}`\n"
                        f"`{_format_weekly_recordings_report_delta(total_delta)}` "
                        f"(`{_format_weekly_recordings_report_change_rate_label(total_change_rate)}`)"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": f"*변화 병원*\n급증 `{surge_count:,}곳` | 급감 `{drop_count:,}곳`",
                },
            ],
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "기준 주간은 `월요일 ~ 일요일`이고, 변화 기준은 증감 `20개 이상` + 급증 `2배 이상` / 급감 `50% 이하`야",
                }
            ],
        },
    ]

    if total_count <= 0:
        blocks.append({"type": "divider"})
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*결과*\n해당 주간 recordings row가 없어",
                },
            }
        )
        return blocks

    top_row_lines = _build_weekly_recordings_report_top_row_lines(top_rows)
    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*상위 병원 Top {top_rows_limit}*\n" + "\n".join(top_row_lines),
            },
        }
    )
    if hospital_count > len(top_rows):
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"상위 `{len(top_rows):,}곳`만 표시",
                    }
                ],
            }
        )

    surge_lines = _build_weekly_recordings_report_change_lines(surge_rows)
    drop_lines = _build_weekly_recordings_report_change_lines(drop_rows)
    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*급증*\n" + "\n".join(surge_lines)
                    if surge_lines
                    else "*급증*\n없어"
                ),
            },
        }
    )
    if surge_count > len(surge_rows):
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"급증은 상위 `{len(surge_rows):,}곳`만 표시",
                    }
                ],
            }
        )

    blocks.append({"type": "divider"})
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": (
                    "*급감*\n" + "\n".join(drop_lines)
                    if drop_lines
                    else "*급감*\n없어"
                ),
            },
        }
    )
    if drop_count > len(drop_rows):
        blocks.append(
            {
                "type": "context",
                "elements": [
                    {
                        "type": "mrkdwn",
                        "text": f"급감은 상위 `{len(drop_rows):,}곳`만 표시",
                    }
                ],
            }
        )

    return blocks
