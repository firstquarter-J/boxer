import logging
import unittest
from datetime import datetime
from unittest.mock import ANY, patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack import daily_device_round_reporter as reporter


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.permalink_requests: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs) -> dict[str, str]:
        self.messages.append(kwargs)
        return {"ts": f"2000.{len(self.messages):03d}"}

    def chat_getPermalink(self, **kwargs) -> dict[str, str]:
        self.permalink_requests.append(kwargs)
        message_ts = str(kwargs.get("message_ts") or "").replace(".", "")
        return {"permalink": f"https://slack.example/{kwargs.get('channel')}/p{message_ts}"}


class DailyDeviceRoundReporterPreviewTests(unittest.TestCase):
    def setUp(self) -> None:
        with reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
            reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()

    def test_builds_minimal_preview_text_with_only_cleanup(self) -> None:
        text = reporter._build_daily_device_round_report_text(
            {
                "hospitalSeq": 24,
                "hospitalName": "푸른산부인과의원(전주)",
                "statusCounts": {"정상": 0, "확인 필요": 1, "이상": 0, "점검 불가": 1},
                "updateCounts": {
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "executed": 1,
                    "failed": 0,
                },
            }
        )

        self.assertEqual(
            text,
            "#24 푸른산부인과의원(전주) | 정리 실행 1",
        )

    def test_builds_minimal_preview_text_with_update_and_cleanup_failures(self) -> None:
        text = reporter._build_daily_device_round_report_text(
            {
                "hospitalSeq": 24,
                "hospitalName": "푸른산부인과의원(전주)",
                "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 1, "점검 불가": 0},
                "updateCounts": {
                    "agentUpdated": 1,
                    "agentUpdateFailed": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 1,
                },
                "cleanupCounts": {
                    "executed": 0,
                    "failed": 1,
                },
            }
        )

        self.assertEqual(
            text,
            "#24 푸른산부인과의원(전주) | 업데이트 에이전트 1 / 박스 0 실패 1 | 정리 실행 0 / 실패 1",
        )

    def test_splits_long_text_fallback_by_line_and_character_limit(self) -> None:
        with patch.object(reporter, "_DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE", 10):
            chunks = reporter._split_daily_device_round_text("12345678901\nabc")

        self.assertEqual(chunks, ["1234567890", "1\nabc"])


class DailyDeviceRoundReporterDueTests(unittest.TestCase):
    def setUp(self) -> None:
        with reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
            reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()

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
                    "activeHospitalSeq": 601,
                    "activeHospitalName": "A병원",
                    "activeDeviceIndex": 3,
                },
                now=datetime(2026, 4, 9, 22, 0, tzinfo=local_tz),
            )

        self.assertEqual(normalized["windowKey"], "2026-04-09")
        self.assertEqual(normalized["lastHospitalSeq"], 604)
        self.assertIsNone(normalized["nextHospitalSeq"])
        self.assertEqual(normalized["processedHospitalSeqs"], [])
        self.assertEqual(normalized["windowThreadTs"], "")
        self.assertEqual(normalized["windowThreadChannelId"], "")
        self.assertNotIn("activeHospitalSeq", normalized)
        self.assertNotIn("activeDeviceIndex", normalized)

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
    def setUp(self) -> None:
        with reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
            reporter._DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()

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
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
        }

        with (
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "MDA_GRAPHQL_ORIGIN", "https://mda.kr.mmtalkbox.com"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_HOUR_KST", 22),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False),
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
            auto_power_off=False,
            progress_callback=ANY,
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
        self.assertEqual(save_state_mock.call_count, 2)
        self.assertEqual(
            save_state_mock.call_args_list[0].args[0],
            {
                "windowKey": "2026-04-08",
                "processedHospitalSeqs": [10],
                "lastHospitalSeq": 10,
                "nextHospitalSeq": 20,
                "windowThreadTs": "2000.001",
                "windowThreadChannelId": "C_DAILY",
                "channelId": "C_DAILY",
            },
        )
        self.assertEqual(
            save_state_mock.call_args_list[1].args[0],
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
                "powerCounts": {
                    "requested": 0,
                    "poweredOff": 0,
                    "alreadyOffline": 0,
                    "powerOffFailed": 0,
                },
            }
        )

    def test_posts_only_report_when_abnormal_found(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 1, "점검 불가": 0},
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
            "deviceResults": [
                {
                    "roomName": "1진료실",
                    "deviceName": "MB2-C00043",
                    "overallLabel": "이상",
                    "priorityReason": "LED USB 장치를 찾지 못했어",
                }
            ],
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
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
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", False),
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
            ),
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 2)
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertEqual(client.permalink_requests, [])

    def test_skips_root_alert_when_health_monitor_owns_alerting(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 0, "확인 필요": 0, "이상": 1, "점검 불가": 0},
            "updateCounts": {},
            "cleanupCounts": {},
            "powerCounts": {},
            "deviceResults": [{"deviceName": "MB2-C00043", "overallLabel": "이상"}],
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
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", True),
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
            ),
        ):
            sent = reporter._run_daily_device_round_if_due(client, logger, now=local_now)

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 2)
        self.assertEqual(client.permalink_requests, [])

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
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
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
        self.assertEqual(save_state_mock.call_count, 1)
        self.assertEqual(
            save_state_mock.call_args_list[0].args[0],
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
                "powerCounts": {
                    "requested": 0,
                    "poweredOff": 0,
                    "alreadyOffline": 0,
                    "powerOffFailed": 0,
                },
            }
        )

    def test_falls_back_to_plain_text_when_block_post_fails(self) -> None:
        class _BlockFailingClient(_FakeSlackClient):
            def chat_postMessage(self, **kwargs) -> dict[str, str]:
                self.messages.append(kwargs)
                if kwargs.get("blocks"):
                    raise RuntimeError("invalid_blocks")
                return {"ts": f"2000.{len(self.messages):03d}"}

        client = _BlockFailingClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "scheduledDeviceCount": 1,
            "nextHospitalSeq": 30,
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
            "autoUpdateAgent": True,
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
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_HOUR_KST", 5),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_END_MINUTE_KST", 0),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
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
                return_value=[{"type": "rich_text", "elements": []}],
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
        self.assertEqual(len(client.messages), 3)
        self.assertEqual(client.messages[0]["text"], "일일 장비 순회 점검 & 업데이트 | 2026-04-08")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertIn("blocks", client.messages[1])
        self.assertEqual(client.messages[2]["thread_ts"], "2000.001")
        self.assertNotIn("blocks", client.messages[2])
        self.assertIn("B병원", client.messages[2]["text"])
        save_state_mock.assert_called()

    def test_splits_block_messages_when_chunk_limit_is_hit(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 2,
            "scheduledDeviceCount": 2,
            "nextHospitalSeq": 30,
            "candidateHospitalCount": 3,
            "statusCounts": {"정상": 1, "확인 필요": 1, "이상": 0, "점검 불가": 0},
            "updateCounts": {
                "agentCandidates": 1,
                "agentUpdated": 0,
                "agentUpdateFailed": 0,
                "boxCandidates": 1,
                "boxUpdated": 0,
                "boxUpdateFailed": 0,
            },
            "cleanupCounts": {
                "candidates": 1,
                "executed": 0,
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
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", True),
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
                return_value=[
                    {"type": "section", "text": {"type": "mrkdwn", "text": "block-1"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": "block-2"}},
                    {"type": "section", "text": {"type": "mrkdwn", "text": "block-3"}},
                ],
            ),
            patch.object(reporter, "_DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE", 1),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state"
            ),
        ):
            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 4)
        self.assertEqual(client.messages[0]["text"], "일일 장비 순회 점검 & 업데이트 | 2026-04-08")
        self.assertEqual(client.messages[1]["text"], "daily round body | 계속 1/3")
        self.assertEqual(client.messages[2]["text"], "daily round body | 계속 2/3")
        self.assertEqual(client.messages[3]["text"], "daily round body | 계속 3/3")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertEqual(client.messages[2]["thread_ts"], "2000.001")
        self.assertEqual(client.messages[3]["thread_ts"], "2000.001")

    def test_reuses_runtime_thread_state_when_final_state_save_fails(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        first_summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
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
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
        }
        second_summary = {
            **first_summary,
            "hospitalSeq": 30,
            "hospitalName": "C병원",
            "nextHospitalSeq": 40,
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
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                side_effect=[first_summary, second_summary],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_report_text",
                side_effect=["first body", "second body"],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_blocks",
                return_value=[{"type": "section", "text": {"type": "mrkdwn", "text": "daily round block"}}],
            ),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._save_daily_device_round_state",
                side_effect=[
                    RuntimeError("disk write failed"),
                    RuntimeError("disk write failed"),
                    None,
                    None,
                ],
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "disk write failed"):
                reporter._run_daily_device_round_if_due(
                    client,
                    logger,
                    now=local_now,
                )

            sent = reporter._run_daily_device_round_if_due(
                client,
                logger,
                now=local_now,
            )

        self.assertTrue(sent)
        self.assertEqual(len(client.messages), 3)
        self.assertEqual(client.messages[0]["text"], "일일 장비 순회 점검 & 업데이트 | 2026-04-08")
        self.assertEqual(client.messages[1]["thread_ts"], "2000.001")
        self.assertEqual(client.messages[2]["thread_ts"], "2000.001")

    def test_posts_title_and_saves_active_progress_before_hospital_finishes(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.daily_device_round_reporter")
        local_now = datetime(2026, 4, 8, 22, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        summary = {
            "runDate": "2026-04-08",
            "hospitalSeq": 20,
            "hospitalName": "B병원",
            "deviceCount": 1,
            "nextHospitalSeq": 30,
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
            "autoUpdateAgent": True,
            "autoUpdateBox": False,
            "autoCleanupTrashCan": False,
        }

        def _build_summary_side_effect(**kwargs):
            progress_callback = kwargs["progress_callback"]
            progress_callback(
                "hospital_started",
                {
                    "hospitalSeq": 20,
                    "hospitalName": "B병원",
                    "deviceCount": 1,
                    "startedAt": local_now.isoformat(),
                },
            )
            self.assertEqual(len(client.messages), 1)
            self.assertEqual(client.messages[0]["text"], "일일 장비 순회 점검 & 업데이트 | 2026-04-08")
            progress_callback(
                "device_started",
                {
                    "hospitalSeq": 20,
                    "hospitalName": "B병원",
                    "deviceCount": 1,
                    "deviceIndex": 1,
                    "deviceName": "MB2-C00001",
                    "updatedAt": local_now.isoformat(),
                },
            )
            return summary

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
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", False),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", False),
            patch(
                "boxer_company_adapter_slack.daily_device_round_reporter._build_daily_device_round_summary",
                side_effect=_build_summary_side_effect,
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
        self.assertGreaterEqual(save_state_mock.call_count, 4)
        self.assertEqual(
            save_state_mock.call_args_list[1].args[0]["activeHospitalSeq"],
            20,
        )
        self.assertEqual(
            save_state_mock.call_args_list[2].args[0]["activeDeviceIndex"],
            1,
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
            "autoPowerOff": False,
            "powerCounts": {
                "requested": 0,
                "poweredOff": 0,
                "alreadyOffline": 0,
                "powerOffFailed": 0,
            },
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
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", True),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", False),
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
                "powerCounts": {
                    "requested": 0,
                    "poweredOff": 0,
                    "alreadyOffline": 0,
                    "powerOffFailed": 0,
                },
            }
        )


if __name__ == "__main__":
    unittest.main()
