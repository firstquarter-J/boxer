import unittest
from datetime import date, datetime, timezone
from unittest.mock import patch

from boxer_company import daily_recordings_report as report


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


class DailyRecordingsReportLoadTests(unittest.TestCase):
    def test_loads_previous_day_rows_grouped_by_hospital(self) -> None:
        connection = _FakeConnection(
            [
                {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 12},
                {"hospitalSeq": 185, "hospitalName": "", "rowCount": 3},
            ]
        )

        with (
            patch("boxer_company.daily_recordings_report._create_db_connection", return_value=connection),
            patch.object(report.s, "DB_QUERY_TIMEOUT_SEC", 8),
        ):
            result = report._load_daily_recordings_report(target_date=date(2026, 4, 2))

        self.assertTrue(connection.closed)
        executed_sql, executed_params = connection.cursor_obj.executed[0]
        self.assertIn("GROUP BY r.hospitalSeq, h.hospitalName", executed_sql)
        self.assertEqual(
            executed_params,
            (
                datetime(2026, 4, 1, 15, 0, 0),
                datetime(2026, 4, 2, 15, 0, 0),
            ),
        )
        self.assertEqual(result["targetDate"], "2026-04-02")
        self.assertEqual(result["hospitalCount"], 2)
        self.assertEqual(result["totalCount"], 15)
        self.assertEqual(result["rows"][1]["hospitalName"], "미확인")


class DailyRecordingsReportSummaryTests(unittest.TestCase):
    def test_builds_summary_with_top_rows_and_day_over_day_changes(self) -> None:
        with patch(
            "boxer_company.daily_recordings_report._load_daily_recordings_report",
            side_effect=[
                {
                    "targetDate": "2026-04-02",
                    "hospitalCount": 3,
                    "totalCount": 75,
                    "rows": [
                        {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 40},
                        {"hospitalSeq": 185, "hospitalName": "애플산부인과의원(안양)", "rowCount": 25},
                        {"hospitalSeq": 777, "hospitalName": "미래여성병원", "rowCount": 10},
                    ],
                },
                {
                    "targetDate": "2026-04-01",
                    "hospitalCount": 3,
                    "totalCount": 40,
                    "rows": [
                        {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 12},
                        {"hospitalSeq": 185, "hospitalName": "애플산부인과의원(안양)", "rowCount": 28},
                        {"hospitalSeq": 333, "hospitalName": "서울여성병원", "rowCount": 0},
                    ],
                },
            ],
        ):
            summary = report._build_daily_recordings_report_summary(target_date=date(2026, 4, 2))

        self.assertEqual(summary["targetDate"], "2026-04-02")
        self.assertEqual(summary["previousDate"], "2026-04-01")
        self.assertEqual(summary["totalCount"], 75)
        self.assertEqual(summary["previousTotalCount"], 40)
        self.assertEqual(summary["totalDelta"], 35)
        self.assertEqual(summary["surgeCount"], 1)
        self.assertEqual(summary["dropCount"], 0)
        self.assertEqual(summary["surgeRows"][0]["hospitalSeq"], 297)


class DailyRecordingsReportFormatTests(unittest.TestCase):
    def test_formats_daily_report_message(self) -> None:
        message = report._format_daily_recordings_report(
            {
                "targetDate": "2026-04-02",
                "previousDate": "2026-04-01",
                "hospitalCount": 2,
                "totalCount": 15,
                "previousTotalCount": 7,
                "totalDelta": 8,
                "totalChangeRate": (8 / 7) * 100,
                "topRows": [
                    {"hospitalSeq": 297, "hospitalName": "다온미래산부인과의원(아산)", "rowCount": 12},
                    {"hospitalSeq": None, "hospitalName": "미확인", "rowCount": 3},
                ],
                "topRowsLimit": 10,
                "surgeRows": [
                    {
                        "hospitalSeq": 297,
                        "hospitalName": "다온미래산부인과의원(아산)",
                        "previousCount": 2,
                        "currentCount": 12,
                        "delta": 10,
                        "changeRate": 500.0,
                    }
                ],
                "surgeCount": 1,
                "dropRows": [],
                "dropCount": 0,
                "changeRowsLimit": 10,
            },
            now=datetime(2026, 4, 3, 0, 0, 1, tzinfo=timezone.utc),
        )

        self.assertIn("*전일 Recordings 요약*", message)
        self.assertIn("• 기준일: `2026-04-02` | 비교일: `2026-04-01`", message)
        self.assertIn("• 발송: `2026-04-03 09:00:01 KST`", message)
        self.assertIn("• 전체 row: `15개` | 병원: `2곳`", message)
        self.assertIn("• 전일 대비: `7 -> 15` (`+8`, `+114.3%`)", message)
        self.assertIn("*상위 병원 Top 10*", message)
        self.assertIn("1. *다온미래산부인과의원(아산)* `#297` `12개`", message)
        self.assertIn(
            "1. *다온미래산부인과의원(아산)* `#297` `2 -> 12` `+10` (`+500.0%`)",
            message,
        )
        self.assertIn("*급감*\n• 없어", message)

    def test_formats_empty_daily_report_message(self) -> None:
        message = report._format_daily_recordings_report(
            {
                "targetDate": "2026-04-02",
                "previousDate": "2026-04-01",
                "hospitalCount": 0,
                "totalCount": 0,
                "previousTotalCount": 4,
                "totalDelta": -4,
                "totalChangeRate": -100.0,
                "topRows": [],
                "surgeRows": [],
                "surgeCount": 0,
                "dropRows": [],
                "dropCount": 0,
            },
            now=datetime(2026, 4, 3, 9, 0, 0),
        )

        self.assertIn("• 결과: 전날 recordings row가 없어", message)

    def test_builds_daily_report_blocks(self) -> None:
        blocks = report._build_daily_recordings_report_blocks(
            {
                "targetDate": "2026-04-02",
                "previousDate": "2026-04-01",
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
            now=datetime(2026, 4, 3, 0, 0, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(blocks[0]["type"], "header")
        self.assertIn("전일 Recordings 요약", blocks[0]["text"]["text"])
        self.assertIn("기준일 `2026-04-02`", blocks[1]["elements"][0]["text"])
        self.assertIn("`1,500개`", blocks[2]["fields"][0]["text"])
        self.assertIn("*상위 병원 Top 10*", blocks[5]["text"]["text"])
        self.assertIn("*다온미래산부인과의원(아산)* `#297` `1,200개`", blocks[5]["text"]["text"])
        self.assertIn("*급증*", blocks[7]["text"]["text"])
        self.assertIn("`400 -> 1,200`", blocks[7]["text"]["text"])
        self.assertIn("*급감*\n없어", blocks[9]["text"]["text"])


if __name__ == "__main__":
    unittest.main()
