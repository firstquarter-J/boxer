import unittest
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

        self.assertIn("*주간 Recordings 요약*", message)
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
            },
            now=datetime(2026, 4, 6, 9, 0, 0),
        )

        self.assertIn("• 결과: 해당 주간 recordings row가 없어", message)

    def test_builds_weekly_report_blocks(self) -> None:
        blocks = report._build_weekly_recordings_report_blocks(
            {
                "weekStartDate": "2026-03-23",
                "weekEndDate": "2026-03-29",
                "previousWeekStartDate": "2026-03-16",
                "previousWeekEndDate": "2026-03-22",
                "hospitalCount": 2,
                "totalCount": 1500,
                "previousTotalCount": 1200,
                "totalDelta": 300,
                "totalChangeRate": 25.0,
                "topRows": [
                    {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 1200},
                    {"hospitalSeq": None, "hospitalName": "미확인", "rowCount": 300},
                ],
                "topRowsLimit": 10,
                "surgeRows": [
                    {
                        "hospitalSeq": 297,
                        "hospitalName": "다온미래산부인과의원(아산)",
                        "previousCount": 400,
                        "currentCount": 1200,
                        "delta": 800,
                        "changeRate": 200.0,
                    }
                ],
                "surgeCount": 1,
                "dropRows": [],
                "dropCount": 0,
            },
            now=datetime(2026, 4, 6, 0, 0, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(blocks[0]["type"], "header")
        self.assertIn("주간 Recordings 요약", blocks[0]["text"]["text"])
        self.assertIn("기준 주간 `2026-03-23 ~ 2026-03-29`", blocks[1]["elements"][0]["text"])
        self.assertIn("`1,500개`", blocks[2]["fields"][0]["text"])
        self.assertIn("*상위 병원 Top 10*", blocks[5]["text"]["text"])
        self.assertIn("*다온미래산부인과의원(아산)* `#297` `1,200개`", blocks[5]["text"]["text"])
        self.assertIn("*급증*", blocks[7]["text"]["text"])
        self.assertIn("`400 -> 1,200`", blocks[7]["text"]["text"])
        self.assertIn("*급감*\n없어", blocks[9]["text"]["text"])


if __name__ == "__main__":
    unittest.main()
