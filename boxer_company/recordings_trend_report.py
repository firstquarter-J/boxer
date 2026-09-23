"""요청형 주별 추이와 연속 감소를 recordings의 단일 read-only 집계로 계산한다."""

from datetime import UTC, datetime
from itertools import pairwise
from typing import Any

from boxer_company import weekly_recordings_report as report
from boxer_company.recordings_report_options import RecordingsReportOptions
from boxer_company.recordings_trend_query import RecordingsTrendQuery, trend_periods


def build_recordings_trend_report(
    query: RecordingsTrendQuery, *, now: datetime, options: RecordingsReportOptions,
) -> dict[str, Any]:
    local_now = report._coerce_weekly_recordings_report_now(now)
    periods = trend_periods(query, today=local_now.date())
    hospitals = report._resolve_report_hospitals(options.hospitals) if options.hospitals else []
    utc_start, utc_end = report._weekly_recordings_report_date_range_to_utc_range(query.start, query.end)
    utc_end = min(utc_end, local_now.astimezone(UTC).replace(tzinfo=None))
    # 각 주의 UTC 끝 경계를 CASE에 바인딩한다. 기간이 늘어도 DB 집계는 한 번이다.
    cases = []
    params: list[Any] = []
    for index, (start, end, _) in enumerate(periods):
        cases.append(f"WHEN r.recordedAt < %s THEN {index}")
        params.append(report._weekly_recordings_report_date_range_to_utc_range(start, end)[1])
    params.extend((utc_start, utc_end))
    scope = ""
    if hospitals:
        scope = "AND r.hospitalSeq IN (" + ", ".join("%s" for _ in hospitals) + ") "
        params.extend(int(row["seq"]) for row in hospitals)
    connection = report._create_db_connection(max(report.s.DB_QUERY_TIMEOUT_SEC, 30))
    try:
        with connection.cursor() as cursor:
            # 신규 바코드는 병원/기간 밖 전체 이력과 대조하고 동일 시각은 seq로 귀속한다.
            cursor.execute(
                "SELECT /*+ MAX_EXECUTION_TIME(25000) */ CASE " + " ".join(cases) + " END AS periodIndex, "
                "r.hospitalSeq AS hospitalSeq, h.hospitalName AS hospitalName, "
                "r.hospitalRoomSeq AS hospitalRoomSeq, hr.roomName AS roomName, "
                "COUNT(*) AS rowCount, "
                "SUM(CASE WHEN r.fullBarcode IS NOT NULL AND r.fullBarcode <> '' "
                "AND NOT EXISTS (SELECT 1 FROM recordings history "
                "WHERE history.fullBarcode = r.fullBarcode "
                "AND (history.recordedAt < r.recordedAt "
                "OR (history.recordedAt = r.recordedAt AND history.seq < r.seq))) "
                "THEN 1 ELSE 0 END) AS newBarcodeCount "
                "FROM recordings r "
                "LEFT JOIN hospitals h ON r.hospitalSeq = h.seq "
                "LEFT JOIN hospital_rooms hr ON r.hospitalRoomSeq = hr.seq "
                "AND r.hospitalSeq = hr.hospitalSeq "
                "WHERE r.recordedAt >= %s AND r.recordedAt < %s " + scope +
                "GROUP BY periodIndex, r.hospitalSeq, h.hospitalName, r.hospitalRoomSeq, hr.roomName",
                tuple(params),
            )
            rows = cursor.fetchall() or []
    finally:
        connection.close()

    weeks = [
        {"startDate": start.isoformat(), "endDate": end.isoformat(), "complete": complete,
         "totalCount": 0, "newBarcodeCount": 0, "hospitals": {}, "rooms": {}}
        for start, end, complete in periods
    ]
    for row in rows:
        week = weeks[int(row["periodIndex"])]
        counts = {"totalCount": int(row["rowCount"]), "newBarcodeCount": int(row["newBarcodeCount"])}
        for metric, count in counts.items():
            week[metric] += count
        hospital_seq = row["hospitalSeq"]
        room_seq = row["hospitalRoomSeq"]
        label = str(row["hospitalName"] or "미확인").strip() or "미확인"
        hospital = week["hospitals"].setdefault(
            hospital_seq, {"name": label, "totalCount": 0, "newBarcodeCount": 0},
        )
        for metric, count in counts.items():
            hospital[metric] += count
        # 진료실 미지정은 병원 총계에만 포함하고 현재 장비 위치로 보충하지 않는다.
        if hospital_seq and room_seq:
            room = week["rooms"].setdefault(
                (hospital_seq, room_seq),
                {"name": label + " · " + (str(row["roomName"] or "").strip() or f"진료실 #{room_seq}"),
                 "totalCount": 0, "newBarcodeCount": 0},
            )
            for metric, count in counts.items():
                room[metric] += count
    minimum = 1 if options.minimum_drop is None else options.minimum_drop
    percent = 0 if options.drop_percent is None else options.drop_percent
    complete_weeks = [week for week in weeks if week["complete"]]
    # 감소 필터와 별개로 모든 병원·병실의 두 지표를 분석해 증가·유지도 보존한다.
    streaks = {
        metric: {
            group: _consecutive_declines(
                complete_weeks, group=group, metric=metric,
                required=1, minimum=minimum, percent=percent,
            )
            for group in ("hospitals", "rooms")
        }
        for metric in ("totalCount", "newBarcodeCount")
    }
    return {
        "startDate": query.start.isoformat(), "endDate": query.end.isoformat(),
        "totalCount": sum(week["totalCount"] for week in weeks), "weeks": weeks,
        "hospitalNames": [row["hospitalName"] for row in hospitals],
        "declineWeeks": query.decline_weeks, "minimumDrop": minimum, "dropPercent": percent,
        "declines": {
            metric: {
                group: [row for row in streaks[metric][group] if row["streak"] >= (query.decline_weeks or 1)]
                for group in ("hospitals", "rooms")
            }
            for metric in ("totalCount", "newBarcodeCount")
        },
        "entityTrends": {
            metric: {
                group: _entity_trends(weeks, group=group, metric=metric, streaks=streaks[metric][group])
                for group in ("hospitals", "rooms")
            }
            for metric in ("totalCount", "newBarcodeCount")
        },
    }


def _entity_trends(
    weeks: list[dict[str, Any]], *, group: str, metric: str, streaks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """불변 ID로 주별 수량을 연결하고 부분 주를 제외한 변화만 분석한다."""

    labels = {key: row["name"] for week in weeks for key, row in week[group].items()}
    streak_by_key = {row["key"]: row["streak"] for row in streaks}
    result = []
    for key, name in labels.items():
        counts = [week[group].get(key, {}).get(metric, 0) for week in weeks]
        complete = [count for week, count in zip(weeks, counts, strict=True) if week["complete"]]
        comparable = len(complete) >= 2
        changes = [right - left for left, right in pairwise(complete)]
        if not comparable:
            direction = "비교 불가"
        elif all(change > 0 for change in changes):
            direction = "계속 증가"
        elif all(change < 0 for change in changes):
            direction = "계속 감소"
        elif all(change == 0 for change in changes):
            direction = "유지"
        elif all(change >= 0 for change in changes):
            direction = "증가·보합"
        elif all(change <= 0 for change in changes):
            direction = "감소·보합"
        else:
            direction = "증감 혼재"
        result.append({
            "key": key, "name": name, "counts": counts, "direction": direction,
            "netDelta": complete[-1] - complete[0] if comparable else None,
            "netRate": report._weekly_recordings_report_change_rate(complete[-1], complete[0]) if comparable else None,
            "latestDelta": complete[-1] - complete[-2] if comparable else None,
            "latestRate": report._weekly_recordings_report_change_rate(complete[-1], complete[-2]) if comparable else None,
            "declineStreak": streak_by_key.get(key, 0),
        })
    # 증가·감소를 모두 포함해 첫 완료 주 대비 변화량이 큰 대상부터 보여준다.
    return sorted(result, key=lambda row: (-abs(row["netDelta"] or 0), -sum(row["counts"]), row["name"], str(row["key"])))


def _consecutive_declines(
    weeks: list[dict[str, Any]], *, group: str, metric: str, required: int,
    minimum: int, percent: float,
) -> list[dict[str, Any]]:
    """완료된 마지막 주까지 이어진 감소만 판정하며 없는 주는 0건으로 채운다."""

    labels = {key: row["name"] for week in weeks for key, row in week[group].items()}
    result = []
    for key, name in labels.items():
        counts = [week[group].get(key, {}).get(metric, 0) for week in weeks]
        streak = 0
        for index in range(len(counts) - 1, 0, -1):
            if not report._matches_report_drop(
                counts[index - 1], counts[index], minimum_delta=minimum, drop_percent=percent,
            ):
                break
            streak += 1
        if streak >= required:
            first = counts[-streak - 1]
            result.append({
                "key": key, "name": name, "streak": streak, "counts": counts[-streak - 1:],
                "startDate": weeks[-streak - 1]["startDate"], "endDate": weeks[-1]["endDate"],
                "delta": counts[-1] - first,
                "changeRate": report._weekly_recordings_report_change_rate(counts[-1], first),
            })
    return sorted(result, key=lambda row: (-row["streak"], row["delta"], row["name"]))
