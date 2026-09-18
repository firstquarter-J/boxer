import sqlite3
import unittest
from contextlib import contextmanager
from datetime import date, datetime, timezone
from unittest.mock import patch

from boxer_company import weekly_recordings_report as report


class _FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class _FakeConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.cursor_obj = _FakeCursor(rows)
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


class WeeklyRecordingsReportLoadTests(unittest.TestCase):
    def test_loads_previous_week_rows_grouped_by_hospital(self) -> None:
        connection = _FakeConnection(
            [
                {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 120},
                {"hospitalSeq": 185, "hospitalName": "", "rowCount": 30},
            ]
        )

        with (
            patch("boxer_company.weekly_recordings_report._create_db_connection", return_value=connection),
            patch.object(report.s, "DB_QUERY_TIMEOUT_SEC", 8),
        ):
            result = report._load_weekly_recordings_report(
                start_date=date(2026, 3, 23),
                end_date=date(2026, 3, 29),
            )

        self.assertTrue(connection.closed)
        executed_sql, executed_params = connection.cursor_obj.executed[0]
        self.assertIn("GROUP BY r.hospitalSeq, h.hospitalName", executed_sql)
        self.assertEqual(
            executed_params,
            (
                datetime(2026, 3, 22, 15, 0, 0),
                datetime(2026, 3, 29, 15, 0, 0),
            ),
        )
        self.assertEqual(result["weekStartDate"], "2026-03-23")
        self.assertEqual(result["weekEndDate"], "2026-03-29")
        self.assertEqual(result["hospitalCount"], 2)
        self.assertEqual(result["totalCount"], 150)
        self.assertEqual(result["rows"][1]["hospitalName"], "미확인")


class WeeklyRecordingsReportSummaryTests(unittest.TestCase):
    def test_room_drops_survive_device_changes_and_hospital_growth(self) -> None:
        # 실제 GROUP BY를 실행해 여러 장비 합산, 같은 이름의 다른 병원 진료실,
        # 이번 주 0건과 KST 주간 경계를 한 번에 확인한다.
        def connect(_timeout):
            db = sqlite3.connect(":memory:")
            db.row_factory = sqlite3.Row
            db.executescript("""
                CREATE TABLE hospitals (seq INTEGER, hospitalName TEXT);
                CREATE TABLE hospital_rooms (seq INTEGER, hospitalSeq INTEGER, roomName TEXT);
                CREATE TABLE recordings (hospitalSeq INTEGER, hospitalRoomSeq INTEGER,
                    deviceSeq INTEGER, recordedAt TEXT);
                INSERT INTO hospitals VALUES (1, 'A병원'), (2, 'B병원');
                INSERT INTO hospital_rooms VALUES
                    (11, 1, '1진료실'), (12, 1, '2진료실'),
                    (13, 1, '3진료실'), (21, 2, '1진료실');
            """)
            for hospital, room, device, previous, current in (
                (1, 11, 101, 6, 0),
                (1, 11, 102, 0, 2),
                (1, 11, 103, 0, 1),
                (1, 12, 104, 3, 0),
                (1, 13, 105, 10, 20),
                (2, 21, 106, 1, 1),
                (1, None, 107, 0, 1),
                (1, 0, 108, 1, 0),
            ):
                for count, recorded_at in (
                    (previous, "2026-03-16 03:00:00"),
                    (current, "2026-03-23 03:00:00"),
                ):
                    db.executemany("INSERT INTO recordings VALUES (?, ?, ?, ?)",
                                   [(hospital, room, device, recorded_at)] * count)
            db.execute("INSERT INTO recordings VALUES (1, 11, 102, '2026-03-29 15:00:00')")

            class Connection:
                @contextmanager
                def cursor(self):
                    class Cursor:
                        def execute(self, sql, params):
                            self.result = db.execute(sql.replace("%s", "?"), tuple(
                                value.isoformat(sep=" ") if isinstance(value, datetime) else value
                                for value in params
                            ))

                        def fetchall(self):
                            return [dict(row) for row in self.result.fetchall()]
                    yield Cursor()

                def close(self):
                    db.close()
            return Connection()

        with patch.object(report, "_create_db_connection", side_effect=connect):
            summary = report._build_weekly_recordings_report_summary(target_date=date(2026, 3, 23))

        self.assertEqual(summary["totalCount"], 25)
        self.assertEqual(summary["previousTotalCount"], 21)
        self.assertEqual(summary["hospitalCount"], 2)
        self.assertEqual(summary["topRows"][0]["rowCount"], 24)
        self.assertEqual(summary["dropCount"], 0)
        self.assertEqual(summary["roomDropCount"], 2)
        self.assertEqual(
            [(row["hospitalSeq"], row["hospitalRoomSeq"], row["previousCount"], row["currentCount"])
             for row in summary["roomDropRows"]],
            [(1, 11, 6, 3), (1, 12, 3, 0)],
        )

    def test_room_drop_requires_fifty_percent_and_at_least_three_fewer_recordings(self) -> None:
        # 감소 비율만 큰 1~2건 변동은 제외하고 두 조건의 경계값을 확인한다.
        for previous, current, included in (
            (1, 0, False), (2, 0, False), (2, 1, False),
            (3, 0, True), (3, 1, False), (3, 2, False), (4, 2, False),
            (5, 2, True), (6, 3, True), (7, 4, False),
            (20, 10, True), (20, 11, False), (0, 0, False), (0, 1, False),
        ):
            with self.subTest(previous=previous, current=current):
                room = {"hospitalSeq": 1, "hospitalRoomSeq": 11,
                        "hospitalName": "A병원", "roomName": "이전 이름"}
                rows = report._build_weekly_recordings_room_drop_rows(
                    {"roomRows": [{**room, "roomName": "바뀐 이름", "rowCount": current}]},
                    {"roomRows": [{**room, "rowCount": previous}]},
                )
                self.assertEqual(bool(rows), included)
                if included:
                    self.assertEqual(rows[0]["roomName"], "바뀐 이름")

    def test_builds_summary_with_top_rows_and_week_over_week_changes(self) -> None:
        with patch(
            "boxer_company.weekly_recordings_report._load_weekly_recordings_report",
            side_effect=[
                {
                    "weekStartDate": "2026-03-23",
                    "weekEndDate": "2026-03-29",
                    "hospitalCount": 3,
                    "totalCount": 750,
                    "rows": [
                        {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 400},
                        {"hospitalSeq": 185, "hospitalName": "애플산부인과의원(안양)", "rowCount": 250},
                        {"hospitalSeq": 777, "hospitalName": "미래여성병원", "rowCount": 100},
                    ],
                },
                {
                    "weekStartDate": "2026-03-16",
                    "weekEndDate": "2026-03-22",
                    "hospitalCount": 3,
                    "totalCount": 400,
                    "rows": [
                        {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 120},
                        {"hospitalSeq": 185, "hospitalName": "애플산부인과의원(안양)", "rowCount": 280},
                        {"hospitalSeq": 333, "hospitalName": "서울여성병원", "rowCount": 0},
                    ],
                },
            ],
        ):
            summary = report._build_weekly_recordings_report_summary(target_date=date(2026, 3, 23))

        self.assertEqual(summary["weekStartDate"], "2026-03-23")
        self.assertEqual(summary["weekEndDate"], "2026-03-29")
        self.assertEqual(summary["previousWeekStartDate"], "2026-03-16")
        self.assertEqual(summary["previousWeekEndDate"], "2026-03-22")
        self.assertEqual(summary["totalCount"], 750)
        self.assertEqual(summary["previousTotalCount"], 400)
        self.assertEqual(summary["totalDelta"], 350)
        self.assertEqual(summary["surgeCount"], 2)
        self.assertEqual(summary["dropCount"], 0)
        self.assertEqual(summary["surgeRows"][0]["hospitalSeq"], 297)

    def test_resolves_previous_complete_week_from_now(self) -> None:
        week_start_date, week_end_date = report._resolve_weekly_recordings_report_target_week(
            now=datetime(2026, 4, 3, 13, 0, 0),
        )

        self.assertEqual(week_start_date, date(2026, 3, 23))
        self.assertEqual(week_end_date, date(2026, 3, 29))

    def test_resolves_explicit_current_week_question_in_kst(self) -> None:
        now = datetime(2026, 4, 3, 13, 0, 0)

        current = (
            report._resolve_weekly_recordings_report_question_target_date(
                "이번 주 초음파 영상 현황",
                explicit_target_date=None,
                now=now,
            )
        )
        previous = (
            report._resolve_weekly_recordings_report_question_target_date(
                "지난주 초음파 영상 현황",
                explicit_target_date=None,
                now=now,
            )
        )
        explicit = (
            report._resolve_weekly_recordings_report_question_target_date(
                "이번 주 초음파 영상 현황",
                explicit_target_date=date(2026, 3, 23),
                now=now,
            )
        )

        self.assertEqual(current, date(2026, 4, 3))
        self.assertIsNone(previous)
        self.assertEqual(explicit, date(2026, 3, 23))


class WeeklyRecordingsReportFormatTests(unittest.TestCase):
    def test_formats_weekly_report_message(self) -> None:
        message = report._format_weekly_recordings_report(
            {
                "weekStartDate": "2026-03-23",
                "weekEndDate": "2026-03-29",
                "previousWeekStartDate": "2026-03-16",
                "previousWeekEndDate": "2026-03-22",
                "hospitalCount": 2,
                "totalCount": 150,
                "previousTotalCount": 70,
                "totalDelta": 80,
                "totalChangeRate": (80 / 70) * 100,
                "topRows": [
                    {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 120},
                    {"hospitalSeq": None, "hospitalName": "미확인", "rowCount": 30},
                ],
                "topRowsLimit": 10,
                "surgeRows": [
                    {
                        "hospitalSeq": 297,
                        "hospitalName": "다온미래산부인과의원(아산)",
                        "previousCount": 20,
                        "currentCount": 120,
                        "delta": 100,
                        "changeRate": 500.0,
                    }
                ],
                "surgeCount": 1,
                "dropRows": [],
                "dropCount": 0,
                "changeRowsLimit": 10,
            },
            now=datetime(2026, 4, 6, 0, 0, 1, tzinfo=timezone.utc),
        )

        self.assertIn("*주간 초음파 촬영 요약*", message)
        self.assertIn("• 기준 주간: `2026-03-23 ~ 2026-03-29` | 비교 주간: `2026-03-16 ~ 2026-03-22`", message)
        self.assertIn("• 발송: `2026-04-06 09:00:01 KST`", message)
        self.assertIn("• 전체 row: `150개` | 병원: `2곳`", message)
        self.assertIn("• 전주 대비: `70 -> 150` (`+80`, `+114.3%`)", message)
        self.assertIn("*상위 병원 Top 10*", message)
        self.assertIn("1. *다온미래산부인과의원(아산)* `#297` `120개`", message)
        self.assertIn(
            "1. *다온미래산부인과의원(아산)* `#297` `20 -> 120` `+100` (`+500.0%`)",
            message,
        )
        self.assertIn("*급감*\n• 없어", message)

    def test_formats_weekly_report_message_without_title(self) -> None:
        message = report._format_weekly_recordings_report(
            {
                "weekStartDate": "2026-03-23",
                "weekEndDate": "2026-03-29",
                "previousWeekStartDate": "2026-03-16",
                "previousWeekEndDate": "2026-03-22",
                "hospitalCount": 1,
                "totalCount": 150,
                "previousTotalCount": 70,
                "totalDelta": 80,
                "totalChangeRate": (80 / 70) * 100,
                "topRows": [],
                "surgeRows": [],
                "surgeCount": 0,
                "dropRows": [],
                "dropCount": 0,
            },
            now=datetime(2026, 4, 6, 0, 0, 1, tzinfo=timezone.utc),
            include_title=False,
        )

        self.assertFalse(message.startswith("*주간 초음파 촬영 요약*"))
        self.assertTrue(message.startswith("• 기준 주간: `2026-03-23 ~ 2026-03-29`"))

    def test_formats_empty_weekly_report_message(self) -> None:
        message = report._format_weekly_recordings_report(
            {
                "weekStartDate": "2026-03-23",
                "weekEndDate": "2026-03-29",
                "previousWeekStartDate": "2026-03-16",
                "previousWeekEndDate": "2026-03-22",
                "hospitalCount": 0,
                "totalCount": 0,
                "previousTotalCount": 40,
                "totalDelta": -40,
                "totalChangeRate": -100.0,
                "topRows": [],
                "surgeRows": [],
                "surgeCount": 0,
                "dropRows": [],
                "dropCount": 0,
                "roomDropRows": [{
                    "hospitalSeq": 1, "hospitalRoomSeq": 11,
                    "hospitalName": "A병원", "roomName": "1진료실",
                    "previousCount": 3, "currentCount": 0, "delta": -3, "changeRate": -100.0,
                }],
                "roomDropCount": 1,
            },
            now=datetime(2026, 4, 6, 9, 0, 0),
        )

        self.assertIn("• 결과: 해당 주간 recordings row가 없어", message)
        self.assertIn("*진료실별 녹화 급감* `1곳`", message)
        self.assertIn("전주 대비 50% 이상·3건 이상 감소", message)
        self.assertIn("A병원 · 1진료실", message)
        self.assertIn("`3건 → 0건` · `-3건` (`-100.0%`)", message)




if __name__ == "__main__":
    unittest.main()
