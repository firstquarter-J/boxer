import logging
import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack import daily_device_round_reporter as reporter


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs) -> dict[str, str]:
        self.messages.append(kwargs)
        return {"ts": f"2000.{len(self.messages):03d}"}


class DailyDeviceRoundReporterDueTests(unittest.TestCase):
    def test_is_due_after_scheduled_time_once_per_day(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 9),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 30),
        ):
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 8, 9, 29, tzinfo=local_tz),
                    {},
                )
            )
            self.assertTrue(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 8, 9, 30, tzinfo=local_tz),
                    {},
                )
            )
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 8, 9, 31, tzinfo=local_tz),
                    {"lastRunDate": "2026-04-08"},
                )
            )


class DailyDeviceRoundReporterRunTests(unittest.TestCase):
    def test_posts_report_and_saves_state_when_due(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 9, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 2,
            "nextHospitalSeq": 10,
            "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
            "updateCounts": {
                "agentCandidates": 1,
                "agentUpdated": 1,
                "agentUpdateFailed": 0,
                "boxCandidates": 1,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 9),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 30),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._load_daily_device_round_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ) as build_summary_mock,
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ) as format_mock,
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ) as blocks_mock,
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ) as save_state_mock,
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        build_summary_mock.assert_called_once_with(
            now=local_now,
            state={},
            auto_update_agent=True,
            auto_update_box=False,
        )
        format_mock.assert_called_once_with(summary, now=local_now)
        blocks_mock.assert_called_once_with(
            summary,
            now=local_now,
            include_header=False,
        )
        self.assertEqual(len(client.messages), 2)
        self.assertEqual(client.messages[0]["channel"], "C_DAILY")
        self.assertEqual(client.messages[0]["text"], "일일 장비 순회 점검 | #20 B병원")
        self.assertEqual(client.messages[1]["channel"], "C_DAILY")
        self.assertEqual(client.messages[1]["text"], "daily round body")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        save_state_mock.assert_called_once_with(
            {
                "lastRunDate": "2026-04-08",
                "lastHospitalSeq": 20,
                "lastHospitalName": "B병원",
                "nextHospitalSeq": 10,
                "lastSentAt": local_now.isoformat(),
                "channelId": "C_DAILY",
                "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 1,
                    "agentUpdated": 1,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 1,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
            }
        )


if __name__ == "__main__":
    unittest.main()
