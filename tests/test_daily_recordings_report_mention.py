import unittest
from datetime import date, datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack.company import (
    _build_daily_recordings_report_reply_payload,
    _is_daily_recordings_report_request,
)


class DailyRecordingsReportMentionTests(unittest.TestCase):
    def test_detects_daily_recordings_report_request(self) -> None:
        self.assertTrue(
            _is_daily_recordings_report_request(
                "전일 초음파 영상 현황",
                barcode=None,
                target_date="2026-04-02",
            )
        )
        self.assertTrue(
            _is_daily_recordings_report_request(
                "2026-04-02 recordings 요약",
                barcode=None,
                target_date="2026-04-02",
            )
        )
        self.assertFalse(
            _is_daily_recordings_report_request(
                "전일 초음파 영상 목록",
                barcode=None,
                target_date="2026-04-02",
            )
        )
        self.assertFalse(
            _is_daily_recordings_report_request(
                "전일 초음파 영상 현황",
                barcode="12345678910",
                target_date="2026-04-02",
            )
        )
        self.assertFalse(
            _is_daily_recordings_report_request(
                "초음파 영상 현황",
                barcode=None,
                target_date=None,
            )
        )

    def test_builds_daily_recordings_report_reply_payload(self) -> None:
        local_now = datetime(2026, 4, 3, 13, 55, tzinfo=ZoneInfo("Asia/Seoul"))

        with (
            patch(
                "boxer_company_adapter_slack.company._coerce_daily_recordings_report_now",
                return_value=local_now,
            ),
            patch(
                "boxer_company_adapter_slack.company._build_daily_recordings_report_summary",
                return_value={"targetDate": "2026-04-02", "totalCount": 10},
            ) as summary_mock,
            patch(
                "boxer_company_adapter_slack.company._format_daily_recordings_report",
                return_value="report text",
            ) as format_mock,
            patch(
                "boxer_company_adapter_slack.company._build_daily_recordings_report_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
            ) as blocks_mock,
        ):
            text, blocks, target_date_text = _build_daily_recordings_report_reply_payload(
                target_date="2026-04-02",
                now=local_now,
            )

        summary_mock.assert_called_once_with(
            target_date=date(2026, 4, 2),
            now=local_now,
        )
        format_mock.assert_called_once_with(
            {"targetDate": "2026-04-02", "totalCount": 10},
            now=local_now,
        )
        blocks_mock.assert_called_once_with(
            {"targetDate": "2026-04-02", "totalCount": 10},
            now=local_now,
        )
        self.assertEqual(text, "report text")
        self.assertEqual(
            blocks,
            [{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
        )
        self.assertEqual(target_date_text, "2026-04-02")


if __name__ == "__main__":
    unittest.main()
