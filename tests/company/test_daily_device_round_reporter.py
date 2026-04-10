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
    def test_clears_legacy_fixed_target_self_loop_on_new_window(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
        ):
            normalized = reporter._normalize_daily_device_round_state(
                {
                    "lastHospitalSeq": 604,
                    "nextHospitalSeq": 604,
                    "lastRunDate": "2026-04-08",
                },
                now=datetime(2026, 4, 9, 22, 0, tzinfo=local_tz),
            )

        self.assertEqual(normalized["windowKey"], "2026-04-09")
        self.assertEqual(normalized["lastHospitalSeq"], 604)
        self.assertIsNone(normalized["nextHospitalSeq"])
        self.assertEqual(normalized["processedHospitalSeqs"], [])
        self.assertEqual(normalized["windowThreadTs"], "")
        self.assertEqual(normalized["windowThreadChannelId"], "")

    def test_is_due_only_inside_overnight_window_until_completed(self) -> None:
        local_tz = ZoneInfo("Asia/Seoul")

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
        ):
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 8, 21, 59, tzinfo=local_tz),
                    {},
                )
            )
            self.assertTrue(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 8, 22, 0, tzinfo=local_tz),
                    {},
                )
            )
            self.assertTrue(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 9, 4, 59, tzinfo=local_tz),
                    {"windowKey": "2026-04-08", "processedHospitalSeqs": [10]},
                )
            )
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 9, 5, 0, tzinfo=local_tz),
                    {"windowKey": "2026-04-08", "processedHospitalSeqs": [10]},
                )
            )
            self.assertFalse(
                reporter._is_daily_device_round_due(
                    datetime(2026, 4, 9, 1, 0, tzinfo=local_tz),
                    {"windowKey": "2026-04-08", "windowCompletedAt": "2026-04-09T00:30:00+09:00"},
                )
            )


class DailyDeviceRoundReporterRunTests(unittest.TestCase):
    def test_posts_report_and_saves_window_state_when_due(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 2,
            "nextHospitalSeq": 10,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
            "updateCounts": {
                "agentCandidates": 1,
                "agentUpdated": 1,
                "agentUpdateFailed": 0,
                "boxCandidates": 1,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "cleanupCounts": {
                "candidates": 1,
                "executed": 1,
                "failed": 0,
            },
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": True,
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", True),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._load_daily_device_round_state",
                return_value={
                    "windowKey": "2026-04-08",
                    "processedHospitalSeqs": [10],
                    "lastHospitalSeq": 10,
                    "nextHospitalSeq": 20,
                },
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
            state={
                "windowKey": "2026-04-08",
                "processedHospitalSeqs": [10],
                "lastHospitalSeq": 10,
                "nextHospitalSeq": 20,
                "windowThreadTs": "",
                "windowThreadChannelId": "",
            },
            auto_update_agent=True,
            auto_update_box=False,
            auto_cleanup_trashcan=True,
        )
        format_mock.assert_called_once_with(summary, now=local_now)
        blocks_mock.assert_called_once_with(
            summary,
            now=local_now,
            include_header=False,
        )
        self.assertEqual(len(client.messages), 2)
        self.assertEqual(client.messages[0]["channel"], "C_DAILY")
        self.assertEqual(client.messages[0]["text"], "일일 장비 순회 점검 & 업데이트 | 2026-04-08")
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
                "windowKey": "2026-04-08",
                "windowThreadTs": "2000.001",
                "windowThreadChannelId": "C_DAILY",
                "processedHospitalSeqs": [10, 20],
                "windowCompletedAt": "",
                "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 1,
                    "agentUpdated": 1,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 1,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 1,
                    "executed": 1,
                    "failed": 0,
                },
            }
        )

    def test_reuses_existing_window_thread_for_next_hospital(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 30,
            "hospitalName": "C병원",
            "deviceCount": 1,
            "nextHospitalSeq": 40,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
            "updateCounts": {
                "agentCandidates": 0,
                "agentUpdated": 0,
                "agentUpdateFailed": 0,
                "boxCandidates": 0,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "cleanupCounts": {
                "candidates": 0,
                "executed": 0,
                "failed": 0,
            },
            "deviceResults": [],
            "autoUpdateAgent": False,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 30),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._load_daily_device_round_state",
                return_value={
                    "windowKey": "2026-04-08",
                    "processedHospitalSeqs": [10, 20],
                    "lastHospitalSeq": 20,
                    "nextHospitalSeq": 30,
                    "windowThreadTs": "2000.777",
                    "windowThreadChannelId": "C_DAILY",
                },
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                return_value="daily round body",
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ),
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
        self.assertEqual(len(client.messages), 1)
        self.assertEqual(client.messages[0]["channel"], "C_DAILY")
        self.assertEqual(client.messages[0]["thread_ts"], "2000.777")
        save_state_mock.assert_called_once_with(
            {
                "windowKey": "2026-04-08",
                "processedHospitalSeqs": [10, 20, 30],
                "lastHospitalSeq": 30,
                "lastHospitalName": "C병원",
                "nextHospitalSeq": 40,
                "windowThreadTs": "2000.777",
                "windowThreadChannelId": "C_DAILY",
                "windowCompletedAt": local_now.isoformat(),
                "lastRunDate": "2026-04-08",
                "lastSentAt": local_now.isoformat(),
                "channelId": "C_DAILY",
                "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 0,
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 0,
                    "executed": 0,
                    "failed": 0,
                },
            }
        )

    def test_marks_window_completed_without_post_when_no_hospital_left(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 9, 4, 30, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-09",
            "hospitalSeq": None,
            "hospitalName": "미선정",
            "deviceCount": 0,
            "nextHospitalSeq": None,
            "candidateHospitalCount": 2,
            "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 0, "점검 불가": 0},
            "updateCounts": {
                "agentCandidates": 0,
                "agentUpdated": 0,
                "agentUpdateFailed": 0,
                "boxCandidates": 0,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "cleanupCounts": {
                "candidates": 0,
                "executed": 0,
                "failed": 0,
            },
            "deviceResults": [],
            "autoUpdateAgent": True,
            "autoUpdateBox": True,
            "autoCleanupTrashCan": False,
            "summaryLine": "이번 야간 업데이트 창에서 처리할 병원을 모두 끝냈어",
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._load_daily_device_round_state",
                return_value={
                    "windowKey": "2026-04-08",
                    "processedHospitalSeqs": [10, 20],
                    "lastHospitalSeq": 20,
                    "nextHospitalSeq": 30,
                },
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                return_value=summary,
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ) as save_state_mock,
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertFalse(sent)
        self.assertEqual(client.messages, [])
        save_state_mock.assert_called_once_with(
            {
                "windowKey": "2026-04-08",
                "processedHospitalSeqs": [10, 20],
                "windowCompletedAt": local_now.isoformat(),
                "lastRunDate": "2026-04-09",
                "lastHospitalSeq": None,
                "lastHospitalName": "미선정",
                "nextHospitalSeq": None,
                "lastSentAt": local_now.isoformat(),
                "channelId": "C_DAILY",
                "windowThreadTs": "",
                "windowThreadChannelId": "",
                "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 0, "점검 불가": 0},
                "updateCounts": {
                    "agentCandidates": 0,
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 0,
                    "executed": 0,
                    "failed": 0,
                },
            }
        )


if __name__ == "__main__":
    unittest.main()
