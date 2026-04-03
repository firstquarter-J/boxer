import logging
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack import daily_recordings_reporter as reporter


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs) -> None:
        self.messages.append(kwargs)


class DailyRecordingsReporterDueTests(unittest.TestCase):
    def test_is_due_only_after_scheduled_time_and_once_per_day(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_HOUR_KST", 9),
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_MINUTE_KST", 0),
        ):
            self.assertFalse(
                reporter._is_daily_recordings_report_due(
                    datetime(2026, 4, 3, 8, 59, tzinfo=local_tz),
                    {},
                )
            )
            self.assertTrue(
                reporter._is_daily_recordings_report_due(
                    datetime(2026, 4, 3, 9, 0, tzinfo=local_tz),
                    {},
                )
            )
            self.assertFalse(
                reporter._is_daily_recordings_report_due(
                    datetime(2026, 4, 3, 9, 1, tzinfo=local_tz),
                    {"lastReportedLocalDate": "2026-04-03"},
                )
            )


class DailyRecordingsReporterRunTests(unittest.TestCase):
    def test_posts_report_and_saves_state_when_due(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_recordings_reporter")
        local_now = datetime(2026, 4, 3, 9, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with (
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_CHANNEL_ID", "C_REPORT"),
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_HOUR_KST", 9),
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_MINUTE_KST", 0),
            patch(
                "boxer_company_adapter_slack.daily_recordings_reporter._load_daily_recordings_report_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.daily_recordings_reporter._build_daily_recordings_report_summary",
                return_value={
                    "targetDate": "2026-04-02",
                    "hospitalCount": 1,
                    "totalCount": 4,
                },
            ),
            patch(
                "boxer_company_adapter_slack.daily_recordings_reporter._format_daily_recordings_report",
                return_value="report body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_recordings_reporter._build_daily_recordings_report_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_recordings_reporter._save_daily_recordings_report_state"
            ) as save_state_mock,
        ):
            sent = reporter._run_daily_recordings_report_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 1)
        self.assertEqual(client.messages[0]["channel"], "C_REPORT")
        self.assertEqual(client.messages[0]["text"], "report body")
        self.assertEqual(
            client.messages[0]["blocks"],
            [{"type": "section", "text": {"type": "mrkdwn", "text": "report block"}}],
        )
        save_state_mock.assert_called_once()

    def test_skips_when_already_reported_today(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_recordings_reporter")
        local_now = datetime(2026, 4, 3, 9, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with (
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_CHANNEL_ID", "C_REPORT"),
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_HOUR_KST", 9),
            patch.object(reporter.cs, "DAILY_RECORDINGS_REPORT_MINUTE_KST", 0),
            patch(
                "boxer_company_adapter_slack.daily_recordings_reporter._load_daily_recordings_report_state",
                return_value={"lastReportedLocalDate": "2026-04-03"},
            ),
            patch(
                "boxer_company_adapter_slack.daily_recordings_reporter._build_daily_recordings_report_summary"
            ) as load_report_mock,
        ):
            sent = reporter._run_daily_recordings_report_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertFalse(sent)
        self.assertEqual(client.messages, [])
        load_report_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
