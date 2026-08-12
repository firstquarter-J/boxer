import unittest
from datetime import date, datetime
import logging
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack.company import (
    _build_weekly_recordings_report_reply_payload,
    _is_weekly_recordings_report_request,
)
from boxer_company_adapter_slack.structured_routes import (
    StructuredRoutesContext,
    _handle_structured_routes,
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

    def test_local_mention_resolves_current_week_before_legacy_query(
        self,
    ) -> None:
        reply = Mock()
        payload = {
            "workspace_id": "TENANT-1",
            "channel_id": "CHANNEL-1",
            "current_ts": "1.0",
            "thread_ts": "1.0",
            "user_id": "ACTOR-1",
            "question": "이번 주 초음파 영상 현황",
        }

        with (
            patch(
                "boxer_company_adapter_slack.structured_routes."
                "_resolve_weekly_recordings_report_question_target_date",
                return_value=date(2026, 4, 3),
            ),
            patch(
                "boxer_company_adapter_slack.structured_routes."
                "_build_weekly_recordings_report_reply_payload",
                return_value=("report", [], "2026-03-30", "2026-04-05"),
            ) as report_builder,
        ):
            handled = _handle_structured_routes(
                StructuredRoutesContext(
                    question="이번 주 초음파 영상 현황",
                    barcode=None,
                    payload=payload,
                    thread_ts="1.0",
                    reply=reply,
                    logger=logging.getLogger(__name__),
                )
            )

        self.assertTrue(handled)
        report_builder.assert_called_once_with(target_date="2026-04-03")
        reply.assert_called_once_with("report", mention_user=False, blocks=[])


if __name__ == "__main__":
    unittest.main()
