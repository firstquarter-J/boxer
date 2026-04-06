import unittest
from datetime import date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack.company import (
    _build_weekly_recordings_report_reply_payload,
    _is_weekly_recordings_report_request,
)


class WeeklyRecordingsReportMentionTests(unittest.TestCase):
    def test_detects_weekly_recordings_report_request(self) -> None:
        self.assertTrue(
            _is_weekly_recordings_report_request(
                "지난주 초음파 영상 현황",
                barcode=None,
                target_date=None,
            )
        )
        self.assertTrue(
            _is_weekly_recordings_report_request(
                "2026-03-23 주간 recordings 요약",
                barcode=None,
                target_date="2026-03-23",
            )
        )
        self.assertFalse(
            _is_weekly_recordings_report_request(
                "지난주 초음파 영상 목록",
                barcode=None,
                target_date=None,
            )
        )
        self.assertFalse(
            _is_weekly_recordings_report_request(
                "지난주 초음파 영상 현황",
                barcode="12345678910",
                target_date=None,
            )
        )
        self.assertFalse(
            _is_weekly_recordings_report_request(
                "초음파 영상 현황",
                barcode=None,
                target_date=None,
            )
        )

    def test_builds_weekly_recordings_report_reply_payload(self) -> None:
        local_now = datetime(2026, 4, 3, 13, 55, tzinfo=ZoneInfo("Asia/Seoul"))

        with (
            patch(
                "boxer_company_adapter_slack.weekly_reports._coerce_weekly_recordings_report_now",
                return_value=local_now,
            ),
            patch(
                "boxer_company_adapter_slack.weekly_reports._build_weekly_recordings_report_summary",
                return_value={
                    "weekStartDate": "2026-03-23",
                    "weekEndDate": "2026-03-29",
                    "totalCount": 10,
                },
            ) as summary_mock,
            patch(
                "boxer_company_adapter_slack.weekly_reports._format_weekly_recordings_report",
                return_value="report text",
            ) as format_mock,
            patch(
                "boxer_company_adapter_slack.weekly_reports._build_weekly_recordings_report_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
            ) as blocks_mock,
        ):
            text, blocks, week_start_text, week_end_text = _build_weekly_recordings_report_reply_payload(
                target_date="2026-03-23",
                now=local_now,
            )

        summary_mock.assert_called_once_with(
            target_date=date(2026, 3, 23),
            now=local_now,
        )
        format_mock.assert_called_once_with(
            {
                "weekStartDate": "2026-03-23",
                "weekEndDate": "2026-03-29",
                "totalCount": 10,
            },
            now=local_now,
        )
        blocks_mock.assert_called_once_with(
            {
                "weekStartDate": "2026-03-23",
                "weekEndDate": "2026-03-29",
                "totalCount": 10,
            },
            now=local_now,
        )
        self.assertEqual(text, "report text")
        self.assertEqual(
            blocks,
            [{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
        )
        self.assertEqual(week_start_text, "2026-03-23")
        self.assertEqual(week_end_text, "2026-03-29")


if __name__ == "__main__":
    unittest.main()
