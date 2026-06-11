import json
import logging
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company.redis_device_state import DeviceStateRedisClient
from boxer_company_adapter_slack import daily_device_round_reporter
from boxer_company_adapter_slack import device_health_monitor_reporter as reporter


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []
        self.views: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs) -> dict[str, str]:
        self.messages.append(kwargs)
        return {"ts": f"3000.{len(self.messages):03d}"}

    def views_open(self, **kwargs) -> dict[str, bool]:
        self.views.append(kwargs)
        return {"ok": True}


class _FakeWebhookResponse:
    def __init__(self, status_code: int = 202, text: str = "") -> None:
        self.status_code = status_code
        self.text = text

    def json(self) -> dict:
        return json.loads(self.text or "{}")


class _FakeSshClient:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeRedisStateClient:
    def __init__(
        self,
        snapshot: dict[str, dict[str, object]] | None = None,
        *,
        ping_error: Exception | None = None,
    ) -> None:
        self.snapshot = snapshot or {}
        self.ping_error = ping_error
        self.loaded_names: list[str] = []
        self.ping_count = 0

    def ping(self) -> None:
        self.ping_count += 1
        if self.ping_error:
            raise self.ping_error

    def load_device_and_agent_states(self, device_names: list[str]) -> dict[str, dict[str, object]]:
        self.loaded_names = list(device_names)
        return self.snapshot


class _FakeRawRedis:
    def __init__(self, values: dict[str, str]) -> None:
        self.values = values
        self.mget_calls: list[list[str]] = []

    def mget(self, keys: list[str]) -> list[str | None]:
        self.mget_calls.append(list(keys))
        return [self.values.get(key) for key in keys]


def _abnormal_summary() -> dict:
    return {
        "runDate": "2026-05-03",
        "hospitalSeq": 69,
        "hospitalName": "수지미래산부인과의원(용인)",
        "deviceCount": 1,
        "nextHospitalSeq": 70,
        "candidateHospitalCount": 2,
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
        "powerCounts": {
            "requested": 0,
            "poweredOff": 0,
            "alreadyOffline": 0,
            "powerOffFailed": 0,
        },
        "deviceResults": [
            {
                "hospitalSeq": 69,
                "hospitalName": "수지미래산부인과의원(용인)",
                "hospitalTelephone": "031-123-4567",
                "hospitalDeviceAlertPhone": "",
                "roomName": "1진료실",
                "deviceName": "MB2-C00043",
                "overallLabel": "이상",
                "priorityReason": "LED USB 장치를 찾지 못했어",
            }
        ],
    }


def _mobile_abnormal_summary() -> dict:
    summary = _abnormal_summary()
    return {
        **summary,
        "deviceResults": [
            {
                **summary["deviceResults"][0],
                "hospitalDeviceAlertPhone": "010-1234-4567",
            }
        ],
    }


def _captureboard_abnormal_summary() -> dict:
    summary = _abnormal_summary()
    return {
        **summary,
        "deviceResults": [
            {
                **summary["deviceResults"][0],
                "priorityReason": "캡처보드 이상",
                "componentLabels": {
                    "audio": "정상",
                    "pm2": "정상",
                    "storage": "정상",
                    "captureboard": "이상",
                    "led": "정상",
                },
                "statusPayload": {
                    "overview": {
                        "captureboard": {
                            "status": "fail",
                            "label": "이상",
                            "summary": "캡처보드 USB나 비디오 장치를 찾지 못했어",
                        }
                    }
                },
            }
        ],
    }


def _captureboard_led_abnormal_summary() -> dict:
    summary = _captureboard_abnormal_summary()
    device_result = summary["deviceResults"][0]
    return {
        **summary,
        "deviceResults": [
            {
                **device_result,
                "priorityReason": "캡처보드/LED 이상",
                "componentLabels": {
                    **device_result["componentLabels"],
                    "led": "이상",
                },
                "statusPayload": {
                    "overview": {
                        **device_result["statusPayload"]["overview"],
                        "led": {
                            "status": "fail",
                            "label": "이상",
                            "summary": "LED USB 장치를 찾지 못했어",
                        },
                    }
                },
            }
        ],
    }


class DeviceHealthMonitorReporterTests(unittest.TestCase):
    def setUp(self) -> None:
        with reporter._DEVICE_HEALTH_MONITOR_RUNTIME_STATE_LOCK:
            reporter._DEVICE_HEALTH_MONITOR_RUNTIME_STATE.clear()
        # 로컬 .env의 실제 테스트 수신번호가 병원번호 기반 시나리오를 덮어쓰지 않게 격리해.
        self._sms_test_phone_patcher = patch.object(
            reporter.cs,
            "DEVICE_HEALTH_MONITOR_SMS_TEST_PHONE_NUMBER",
            "",
        )
        self._sms_test_phone_patcher.start()
        self.addCleanup(self._sms_test_phone_patcher.stop)

    def test_posts_abnormal_alert_without_running_maintenance_actions(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_CHANNEL_ID", "C_HEALTH"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "MDA_GRAPHQL_ORIGIN", "https://mda.kr.mmtalkbox.com"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", 6),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_summary",
                return_value=_abnormal_summary(),
            ) as build_summary_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._save_device_health_monitor_state"
            ) as save_state_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ) as append_event_mock,
        ):
            sent = reporter._run_device_health_monitor_once(client, logger, now=local_now)

        self.assertTrue(sent)
        build_summary_mock.assert_called_once_with(
            now=local_now,
            state={
                "lastHospitalSeq": None,
                "nextHospitalSeq": None,
                "processedHospitalSeqs": [],
                "alertFingerprints": {},
                "pendingAlertFingerprints": {},
                "sshTunnelRecords": {},
                "deviceCandidateCache": [],
                "deviceCandidateCachedAt": "",
            },
        )
        self.assertEqual(len(client.messages), 1)
        self.assertEqual(client.messages[0]["channel"], "C_HEALTH")
        self.assertEqual(
            client.messages[0]["text"],
            "\n".join(
                [
                    ":alert: *이상 발견 - 확인 요망*",
                    "*#69 수지미래산부인과의원(용인)*",
                    "> *전화*  031-123-4567",
                    "> *문자*  *저장된 번호 없음. 자동발송 불가*",
                    "> *병실*  1진료실",
                    "> *장비*  `MB2-C00043`",
                    "> *문제 장치*  `LED`",
                    "> *이슈*  LED USB 장치를 찾지 못했어",
                    "> <https://mda.kr.mmtalkbox.com/monitoring?focusDevice=MB2-C00043&hospitalSeq=69|MDA 에서 장비 확인 바로가기>",
                ]
            ),
        )
        blocks = client.messages[0]["blocks"]
        action_blocks = [block for block in blocks if block["type"] == "actions"]
        self.assertEqual(len(action_blocks), 1)
        self.assertEqual(
            [element["action_id"] for element in action_blocks[0]["elements"]],
            [
                reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                reporter._DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
            ],
        )
        self.assertEqual(action_blocks[0]["elements"][0]["text"]["text"], "병원 문자 보내기")
        self.assertEqual(action_blocks[0]["elements"][1]["text"]["text"], "장비 음성 안내(미구현)")
        self.assertIn("MB2-C00043", action_blocks[0]["elements"][0]["value"])
        self.assertIn('"hospitalSeq":"69"', action_blocks[0]["elements"][0]["value"])
        self.assertIn('"telephone":"031-123-4567"', action_blocks[0]["elements"][0]["value"])
        self.assertIn('"deviceAlertPhone":""', action_blocks[0]["elements"][0]["value"])
        saved_state = save_state_mock.call_args.args[0]
        self.assertEqual(saved_state["processedHospitalSeqs"], [])
        self.assertIsNone(saved_state["lastHospitalSeq"])
        self.assertIsNone(saved_state["nextHospitalSeq"])
        self.assertEqual(len(saved_state["alertFingerprints"]), 1)
        self.assertEqual(
            [call.args[0] for call in append_event_mock.call_args_list],
            ["run_summary", "slack_alert_sent"],
        )

    def test_abnormal_alert_highlights_problem_components(self) -> None:
        text = daily_device_round_reporter._build_daily_device_round_abnormal_alert_text(
            _captureboard_led_abnormal_summary(),
            permalink=None,
        )

        self.assertIn("> *문제 장치*  `캡처보드` `LED`", text)
        self.assertIn(
            "> *이슈*  캡처보드 USB나 비디오 장치를 찾지 못했어 / LED USB 장치를 찾지 못했어",
            text,
        )

    def test_auto_sends_sms_when_device_alert_phone_is_mobile(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_CHANNEL_ID", "C_HEALTH"),
            patch.object(reporter.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C_DAILY"),
            patch.object(reporter.cs, "MDA_GRAPHQL_ORIGIN", "https://mda.kr.mmtalkbox.com"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", 6),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_summary",
                return_value=_mobile_abnormal_summary(),
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._save_device_health_monitor_state"
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "ok",
                    "hospitalSeq": "69",
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "telephone": "031-123-4567",
                    "deviceAlertPhone": "010-1234-4567",
                    "phoneNumber": "01012344567",
                },
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._post_device_health_monitor_sms_payload",
                return_value={"status": "sent", "ok": True, "provider": "fake"},
            ) as post_sms_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ) as append_event_mock,
        ):
            sent = reporter._run_device_health_monitor_once(client, logger, now=local_now)

        self.assertTrue(sent)
        post_sms_mock.assert_called_once()
        sms_payload = post_sms_mock.call_args.args[0]
        self.assertEqual(sms_payload["sms"]["to"], "01012344567")
        self.assertEqual(sms_payload["sms"]["templateId"], "led_disconnected")
        self.assertTrue(sms_payload["sms"]["message"].startswith("안녕하세요 마미톡입니다. 🌷"))
        self.assertIn("LED USB 케이블을 분리했다가 다시", sms_payload["sms"]["message"])

        self.assertEqual(len(client.messages), 1)
        self.assertIn(
            "> *문자*  010-1234-4567",
            client.messages[0]["text"],
        )
        self.assertNotIn("> *문자*  문자 자동발송 완료", client.messages[0]["text"])
        blocks = client.messages[0]["blocks"]
        section_texts = [
            block["text"]["text"]
            for block in blocks
            if block.get("type") == "section" and isinstance(block.get("text"), dict)
        ]
        self.assertTrue(any("*문제 장치*  `LED`" in text for text in section_texts))
        self.assertTrue(
            any(
                "*문자*  010-1234-4567" in text
                for text in section_texts
            )
        )
        self.assertFalse(any("*문자*  문자 자동발송 완료" in text for text in section_texts))
        action_blocks = [block for block in blocks if block["type"] == "actions"]
        self.assertEqual(len(action_blocks), 1)
        self.assertEqual(
            [element["action_id"] for element in action_blocks[0]["elements"]],
            [
                reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                reporter._DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
            ],
        )
        self.assertEqual(action_blocks[0]["elements"][0]["text"]["text"], "문자 자동발송 완료")
        self.assertIn('"smsPhoneNumber":"01012344567"', action_blocks[0]["elements"][0]["value"])
        self.assertIn(
            f'"smsModalMode":"{reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT}"',
            action_blocks[0]["elements"][0]["value"],
        )
        self.assertIn("LED USB 케이블을 분리했다가 다시", action_blocks[0]["elements"][0]["value"])
        self.assertEqual(
            [call.args[0] for call in append_event_mock.call_args_list],
            ["run_summary", "alert_sms_auto_sent", "slack_alert_sent"],
        )

    def test_suppresses_duplicate_alert_until_reminder_window_passes(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        fingerprint = (
            "#69 수지미래산부인과의원(용인)|1진료실|"
            "MB2-C00043|LED USB 장치를 찾지 못했어"
        )
        previous_alert_at = (local_now - timedelta(hours=1)).isoformat()

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_CHANNEL_ID", "C_HEALTH"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", 6),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={
                    "alertFingerprints": {
                        fingerprint: {
                            "firstAlertedAt": previous_alert_at,
                            "lastAlertedAt": previous_alert_at,
                            "lastSeenAt": previous_alert_at,
                            "count": 1,
                        }
                    }
                },
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_summary",
                return_value=_abnormal_summary(),
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._save_device_health_monitor_state"
            ) as save_state_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ) as append_event_mock,
        ):
            sent = reporter._run_device_health_monitor_once(client, logger, now=local_now)

        self.assertFalse(sent)
        self.assertEqual(client.messages, [])
        saved_alert = save_state_mock.call_args.args[0]["alertFingerprints"][fingerprint]
        self.assertEqual(saved_alert["firstAlertedAt"], previous_alert_at)
        self.assertEqual(saved_alert["lastAlertedAt"], previous_alert_at)
        self.assertEqual(saved_alert["lastSeenAt"], local_now.isoformat())
        self.assertEqual(saved_alert["count"], 2)
        self.assertEqual(
            [call.args[0] for call in append_event_mock.call_args_list],
            ["run_summary"],
        )

    def test_contact_action_opens_sms_modal_with_editable_defaults(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "캡처보드 USB나 비디오 장치를 찾지 못했어",
            "mdaUrl": "https://mda.example/device",
        }
        body = {
            "trigger_id": "TRIGGER123",
            "user": {"id": "U123"},
            "channel": {"id": "C_HEALTH"},
            "message": {"ts": "3000.001"},
            "actions": [
                {
                    "action_id": reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                    "value": json.dumps(item, ensure_ascii=False),
                }
            ],
        }

        with (
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "ok",
                    "hospitalSeq": "69",
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "telephone": "031-123-4567",
                    "deviceAlertPhone": "010-1234-4567",
                    "phoneNumber": "01012344567",
                },
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ),
        ):
            result = reporter._handle_device_health_monitor_slack_action(body, client, logger)

        self.assertEqual(result["result"]["status"], "modal_opened")
        self.assertEqual(len(client.views), 1)
        self.assertEqual(client.views[0]["trigger_id"], "TRIGGER123")
        view = client.views[0]["view"]
        self.assertEqual(view["callback_id"], reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID)
        metadata = json.loads(view["private_metadata"])
        self.assertEqual(metadata["channelId"], "C_HEALTH")
        self.assertEqual(metadata["messageTs"], "3000.001")
        phone_block = next(
            block
            for block in view["blocks"]
            if block.get("block_id") == reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID
        )
        message_block = next(
            block
            for block in view["blocks"]
            if block.get("block_id") == reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID
        )
        self.assertEqual(phone_block["element"]["initial_value"], "01012344567")
        self.assertTrue(
            message_block["element"]["initial_value"].startswith("안녕하세요 마미톡입니다. 🌷")
        )
        self.assertIn("초음파 진단기와 캡처보드", message_block["element"]["initial_value"])
        self.assertIn("두 HDMI 케이블을 분리했다가 다시", message_block["element"]["initial_value"])
        self.assertIn("\n\n", message_block["element"]["initial_value"])

    def test_auto_sms_status_action_opens_confirmation_modal_without_resending(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "LED USB 장치를 찾지 못했어",
            "mdaUrl": "https://mda.example/device",
            "deviceAlertPhone": "010-1234-4567",
            "smsStatusText": "문자 자동발송 완료",
            "smsPhoneNumber": "01012344567",
            "smsMessage": "안녕하세요 마미톡입니다.\n\nLED USB 케이블을 분리했다가 다시 연결해 주세요.",
            "smsTemplateId": "led_disconnected",
        }
        body = {
            "trigger_id": "TRIGGER456",
            "user": {"id": "U123"},
            "channel": {"id": "C_HEALTH"},
            "message": {"ts": "3000.002"},
            "actions": [
                {
                    "action_id": reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                    "value": json.dumps(item, ensure_ascii=False),
                }
            ],
        }

        with patch(
            "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
        ):
            result = reporter._handle_device_health_monitor_slack_action(body, client, logger)

        self.assertEqual(result["result"]["status"], "modal_opened")
        self.assertEqual(len(client.views), 1)
        view = client.views[0]["view"]
        self.assertEqual(view["callback_id"], reporter._DEVICE_HEALTH_MONITOR_SMS_VIEW_MODAL_CALLBACK_ID)
        self.assertEqual(view["title"]["text"], "병원 문자 확인")
        self.assertNotIn("submit", view)
        metadata = json.loads(view["private_metadata"])
        self.assertEqual(metadata["actionId"], reporter._DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS)
        self.assertEqual(metadata["mode"], reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MODE_VIEW_AUTO_SENT)
        phone_block = view["blocks"][0]
        message_block = view["blocks"][1]
        self.assertEqual(phone_block["type"], "section")
        self.assertEqual(message_block["type"], "section")
        self.assertIn("*받는 번호*\n01012344567", phone_block["text"]["text"])
        self.assertIn("*문자 내용*", message_block["text"]["text"])
        self.assertIn(item["smsMessage"], message_block["text"]["text"])
        self.assertFalse(any(block.get("type") == "input" for block in view["blocks"]))
        self.assertFalse(
            any(
                block.get("block_id") == reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID
                for block in view["blocks"]
            )
        )
        self.assertFalse(
            any(
                block.get("block_id") == reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID
                for block in view["blocks"]
            )
        )

        submission_body = {
            "user": {"id": "U123"},
            "view": {
                "private_metadata": view["private_metadata"],
                "state": {"values": {}},
            },
        }
        self.assertEqual(
            reporter._validate_device_health_monitor_contact_modal_submission(submission_body),
            {},
        )
        with patch(
            "boxer_company_adapter_slack.device_health_monitor_reporter._post_device_health_monitor_sms_payload"
        ) as post_sms_mock:
            submission_result = reporter._handle_device_health_monitor_contact_modal_submission(
                submission_body,
                client,
                logger,
            )

        post_sms_mock.assert_not_called()
        self.assertEqual(submission_result["result"]["status"], "viewed")

    def test_contact_modal_leaves_phone_blank_for_landline_even_with_test_number(self) -> None:
        item = {
            "hospitalSeq": "404",
            "hospital": "#404 진주경상대학교병원(진주)",
            "room": "3진료실",
            "device": "MB2-C00650",
            "issue": "LED USB 장치를 찾지 못했어",
            "mdaUrl": "https://mda.example/device",
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_TEST_PHONE_NUMBER", "010-4813-0831"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "ok",
                    "hospitalSeq": "404",
                    "hospitalName": "진주경상대학교병원(진주)",
                    "telephone": "055-750-8000",
                    "deviceAlertPhone": "055-750-8000",
                    "phoneNumber": "0557508000",
                },
            ),
        ):
            view = reporter._build_device_health_monitor_sms_modal_view(
                item=item,
                actor_user_id="U123",
                channel_id="C_HEALTH",
                message_ts="3000.001",
                thread_ts="3000.001",
            )

        phone_block = next(
            block
            for block in view["blocks"]
            if block.get("block_id") == reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID
        )
        message_block = next(
            block
            for block in view["blocks"]
            if block.get("block_id") == reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID
        )
        self.assertNotIn("initial_value", phone_block["element"])
        self.assertEqual(phone_block["element"]["placeholder"]["text"], "휴대전화번호 입력 필요")
        self.assertTrue(
            message_block["element"]["initial_value"].startswith("안녕하세요 마미톡입니다. 🌷")
        )
        self.assertIn("LED USB 케이블을 분리했다가 다시", message_block["element"]["initial_value"])

    def test_contact_modal_submission_sends_custom_phone_and_message(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "LED USB 장치를 찾지 못했어",
            "mdaUrl": "https://mda.example/device",
        }
        body = {
            "user": {"id": "U999"},
            "view": {
                "private_metadata": json.dumps(
                    {
                        "actionId": reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                        "actorUserId": "U123",
                        "channelId": "C_HEALTH",
                        "messageTs": "3000.001",
                        "threadTs": "3000.001",
                        "item": item,
                    },
                    ensure_ascii=False,
                ),
                "state": {
                    "values": {
                        reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID: {
                            reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID: {
                                "value": "+82 10-9999-0000",
                            }
                        },
                        reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID: {
                            reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID: {
                                "value": "직접 작성한 안내 문자입니다.",
                            }
                        },
                    }
                },
            },
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "webhook"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL", "https://hook.example/sms"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC", 3),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "ok",
                    "hospitalSeq": "69",
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "telephone": "031-123-4567",
                    "deviceAlertPhone": "",
                    "phoneNumber": "0311234567",
                },
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter.requests.post",
                return_value=_FakeWebhookResponse(),
            ) as post_mock,
        ):
            result = reporter._handle_device_health_monitor_contact_modal_submission(
                body,
                client,
                logger,
                now=local_now,
            )

        self.assertEqual(result["result"]["status"], "sent")
        payload = post_mock.call_args.kwargs["json"]
        self.assertEqual(payload["actorUserId"], "U999")
        self.assertEqual(payload["hospital"]["phoneNumber"], "01099990000")
        self.assertEqual(payload["sms"]["to"], "01099990000")
        self.assertEqual(payload["sms"]["templateId"], "manual")
        self.assertEqual(payload["sms"]["message"], "직접 작성한 안내 문자입니다.")
        self.assertIn("병원 문자 발송 요청을 보냈어", client.messages[0]["text"])

    def test_contact_modal_submission_validates_phone_and_message(self) -> None:
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "LED USB 장치를 찾지 못했어",
            "mdaUrl": "",
        }
        body = {
            "view": {
                "private_metadata": json.dumps({"item": item}, ensure_ascii=False),
                "state": {
                    "values": {
                        reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID: {
                            reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID: {
                                "value": "031-123-4567",
                            }
                        },
                        reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID: {
                            reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID: {
                                "value": "",
                            }
                        },
                    }
                },
            }
        }

        errors = reporter._validate_device_health_monitor_contact_modal_submission(body)

        self.assertEqual(
            errors[reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID],
            "휴대전화번호만 입력할 수 있어",
        )
        self.assertEqual(
            errors[reporter._DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID],
            "문자 내용을 입력해줘",
        )

    def test_contact_action_posts_webhook_and_thread_reply(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "캡처보드 USB나 비디오 장치를 찾지 못했어",
            "mdaUrl": "https://mda.example/device",
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "webhook"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL", "https://hook.example/sms"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC", 3),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ) as append_event_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "ok",
                    "hospitalSeq": "69",
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "telephone": "031-123-4567",
                    "deviceAlertPhone": "010-1234-4567",
                    "phoneNumber": "01012344567",
                },
            ) as lookup_contact_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter.requests.post",
                return_value=_FakeWebhookResponse(),
            ) as post_mock,
        ):
            result = reporter._handle_device_health_monitor_alert_action(
                action_id=reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                raw_item=json.dumps(item, ensure_ascii=False),
                actor_user_id="U123",
                channel_id="C_HEALTH",
                message_ts="3000.001",
                thread_ts="3000.001",
                client=client,
                logger=logger,
                now=local_now,
            )

        self.assertEqual(result["result"]["status"], "sent")
        self.assertEqual(result["result"]["templateId"], "captureboard_disconnected")
        self.assertEqual(result["result"]["phoneLast4"], "4567")
        lookup_contact_mock.assert_called_once_with(69)
        post_mock.assert_called_once()
        self.assertEqual(post_mock.call_args.kwargs["timeout"], 3)
        payload = post_mock.call_args.kwargs["json"]
        self.assertEqual(payload["requestType"], "sms")
        self.assertEqual(payload["actorUserId"], "U123")
        self.assertEqual(payload["hospital"]["phoneNumber"], "01012344567")
        self.assertEqual(payload["device"]["name"], "MB2-C00043")
        self.assertEqual(payload["sms"]["to"], "01012344567")
        self.assertEqual(payload["sms"]["templateId"], "captureboard_disconnected")
        self.assertTrue(payload["sms"]["message"].startswith("안녕하세요 마미톡입니다. 🌷"))
        self.assertIn("초음파 진단기와 캡처보드", payload["sms"]["message"])
        self.assertIn("두 HDMI 케이블을 분리했다가 다시", payload["sms"]["message"])
        self.assertEqual(len(client.messages), 1)
        self.assertEqual(client.messages[0]["thread_ts"], "3000.001")
        self.assertIn("병원 문자 발송 요청을 보냈어", client.messages[0]["text"])
        self.assertEqual(append_event_mock.call_args.args[0], "alert_action_requested")

    def test_contact_action_skips_internal_issue_without_call(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "PM2 프로세스 상태 이상",
            "mdaUrl": "",
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "webhook"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL", "https://hook.example/sms"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact"
            ) as lookup_contact_mock,
            patch("boxer_company_adapter_slack.device_health_monitor_reporter.requests.post") as post_mock,
        ):
            result = reporter._handle_device_health_monitor_alert_action(
                action_id=reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                raw_item=item,
                actor_user_id="U123",
                channel_id="C_HEALTH",
                message_ts="3000.001",
                thread_ts="3000.001",
                client=client,
                logger=logger,
                now=local_now,
            )

        self.assertEqual(result["result"]["status"], "unsupported_issue")
        lookup_contact_mock.assert_not_called()
        post_mock.assert_not_called()
        self.assertIn("병원 문자 발송 대상이 아니야", client.messages[0]["text"])

    def test_contact_action_requires_hospital_phone(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "LED USB 장치를 찾지 못했어",
            "mdaUrl": "",
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "webhook"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL", "https://hook.example/sms"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "missing_telephone",
                    "hospitalSeq": "69",
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "telephone": "",
                    "deviceAlertPhone": "",
                    "phoneNumber": "",
                },
            ),
            patch("boxer_company_adapter_slack.device_health_monitor_reporter.requests.post") as post_mock,
        ):
            result = reporter._handle_device_health_monitor_alert_action(
                action_id=reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                raw_item=item,
                actor_user_id="U123",
                channel_id="C_HEALTH",
                message_ts="3000.001",
                thread_ts="3000.001",
                client=client,
                logger=logger,
                now=local_now,
            )

        self.assertEqual(result["result"]["status"], "missing_telephone")
        post_mock.assert_not_called()
        self.assertIn("마미박스 이상 알림 전용 연락 번호가 없어", client.messages[0]["text"])

    def test_contact_action_rejects_landline_phone(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "hospitalSeq": "69",
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "LED USB 장치를 찾지 못했어",
            "mdaUrl": "",
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "webhook"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL", "https://hook.example/sms"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "ok",
                    "hospitalSeq": "69",
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "telephone": "031-123-4567",
                    "deviceAlertPhone": "031-123-4567",
                    "phoneNumber": "0311234567",
                },
            ),
            patch("boxer_company_adapter_slack.device_health_monitor_reporter.requests.post") as post_mock,
        ):
            result = reporter._handle_device_health_monitor_alert_action(
                action_id=reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                raw_item=item,
                actor_user_id="U123",
                channel_id="C_HEALTH",
                message_ts="3000.001",
                thread_ts="3000.001",
                client=client,
                logger=logger,
                now=local_now,
            )

        self.assertEqual(result["result"]["status"], "non_mobile_telephone")
        post_mock.assert_not_called()
        self.assertIn("휴대전화번호가 아니라", client.messages[0]["text"])

    def test_contact_action_can_send_sms_via_solapi_provider(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "hospitalSeq": "",
            "hospital": "AI콜 테스트 병원",
            "room": "1진료실",
            "device": "MB2-TEST0831",
            "issue": "캡처보드 USB나 비디오 장치를 찾지 못했어",
            "mdaUrl": "",
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "solapi"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SMS_TEST_PHONE_NUMBER", "010-4813-0831"),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC", 3),
            patch.object(reporter.cs, "SOLAPI_API_KEY", "api-key"),
            patch.object(reporter.cs, "SOLAPI_API_SECRET", "api-secret"),
            patch.object(reporter.cs, "SOLAPI_FROM_NUMBER", "0212345678"),
            patch.object(reporter.cs, "SOLAPI_BASE_URL", "https://api.solapi.com"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._lookup_device_health_monitor_hospital_contact",
                return_value={
                    "status": "missing_hospital_seq",
                    "hospitalSeq": "",
                    "hospitalName": "",
                    "telephone": "",
                    "deviceAlertPhone": "",
                    "phoneNumber": "",
                },
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter.requests.post",
                return_value=_FakeWebhookResponse(status_code=200, text='{"groupId":"G123"}'),
            ) as post_mock,
        ):
            result = reporter._handle_device_health_monitor_alert_action(
                action_id=reporter._DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                raw_item=item,
                actor_user_id="U123",
                channel_id="C_HEALTH",
                message_ts="3000.001",
                thread_ts="3000.001",
                client=client,
                logger=logger,
                now=local_now,
            )

        self.assertEqual(result["result"]["status"], "sent")
        self.assertEqual(result["result"]["provider"], "solapi")
        self.assertTrue(result["result"]["testMode"])
        post_mock.assert_called_once()
        self.assertEqual(post_mock.call_args.args[0], "https://api.solapi.com/messages/v4/send-many/detail")
        self.assertEqual(post_mock.call_args.kwargs["timeout"], 3)
        self.assertIn("HMAC-SHA256 apiKey=api-key", post_mock.call_args.kwargs["headers"]["Authorization"])
        solapi_payload = post_mock.call_args.kwargs["json"]
        self.assertEqual(solapi_payload["messages"][0]["to"], "01048130831")
        self.assertEqual(solapi_payload["messages"][0]["from"], "0212345678")
        self.assertTrue(solapi_payload["messages"][0]["text"].startswith("안녕하세요 마미톡입니다. 🌷"))
        self.assertIn("초음파 진단기와 캡처보드", solapi_payload["messages"][0]["text"])
        self.assertIn("두 HDMI 케이블을 분리했다가 다시", solapi_payload["messages"][0]["text"])
        self.assertIn("병원 문자 발송 요청을 보냈어", client.messages[0]["text"])

    def test_voice_action_is_marked_not_implemented(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor.action")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "hospital": "#69 수지미래산부인과의원(용인)",
            "room": "1진료실",
            "device": "MB2-C00043",
            "issue": "LED USB 장치를 찾지 못했어",
            "mdaUrl": "",
        }

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_VOICE_GUIDE_WEBHOOK_URL", "https://hook.example/voice"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ) as append_event_mock,
            patch("boxer_company_adapter_slack.device_health_monitor_reporter.requests.post") as post_mock,
        ):
            result = reporter._handle_device_health_monitor_alert_action(
                action_id=reporter._DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
                raw_item=item,
                actor_user_id="U123",
                channel_id="C_HEALTH",
                message_ts="3000.001",
                thread_ts="3000.001",
                client=client,
                logger=logger,
                now=local_now,
            )

        self.assertEqual(result["result"]["status"], "not_implemented")
        post_mock.assert_not_called()
        self.assertEqual(len(client.messages), 1)
        self.assertIn("장비 코드 추가 후 연결해야 해", client.messages[0]["text"])
        self.assertEqual(append_event_mock.call_args.args[0], "alert_action_requested")

    def test_retains_recent_alert_when_issue_temporarily_disappears(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        fingerprint = (
            "#69 수지미래산부인과의원(용인)|1진료실|"
            "MB2-C00043|LED USB 장치를 찾지 못했어"
        )
        previous_alert_at = (local_now - timedelta(minutes=2)).isoformat()
        previous_state = {
            "alertFingerprints": {
                fingerprint: {
                    "firstAlertedAt": previous_alert_at,
                    "lastAlertedAt": previous_alert_at,
                    "lastSeenAt": previous_alert_at,
                    "count": 1,
                }
            }
        }
        normal_summary = {
            **_abnormal_summary(),
            "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
            "deviceResults": [],
        }

        with patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", 6):
            alertable, retained_alerts, retained_pending = reporter._collect_device_health_monitor_alert_updates(
                normal_summary,
                previous_state,
                now=local_now,
            )
            reappeared_alertable, reappeared_alerts, reappeared_pending = (
                reporter._collect_device_health_monitor_alert_updates(
                    _abnormal_summary(),
                    {
                        "alertFingerprints": retained_alerts,
                        "pendingAlertFingerprints": retained_pending,
                    },
                    now=local_now + timedelta(minutes=1),
                )
            )

        self.assertEqual(alertable, set())
        self.assertIn(fingerprint, retained_alerts)
        self.assertEqual(retained_pending, {})
        self.assertEqual(retained_alerts[fingerprint]["lastAlertedAt"], previous_alert_at)
        self.assertEqual(reappeared_alertable, set())
        self.assertEqual(reappeared_pending, {})
        self.assertEqual(reappeared_alerts[fingerprint]["lastAlertedAt"], previous_alert_at)
        self.assertEqual(reappeared_alerts[fingerprint]["count"], 2)

    def test_prunes_inactive_alert_after_reminder_window(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        fingerprint = (
            "#69 수지미래산부인과의원(용인)|1진료실|"
            "MB2-C00043|LED USB 장치를 찾지 못했어"
        )
        previous_alert_at = (local_now - timedelta(hours=7)).isoformat()
        normal_summary = {
            **_abnormal_summary(),
            "statusCounts": {"정상": 1, "확인 필요": 0, "이상": 0, "점검 불가": 0},
            "deviceResults": [],
        }

        with patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", 6):
            alertable, retained_alerts, retained_pending = reporter._collect_device_health_monitor_alert_updates(
                normal_summary,
                {
                    "alertFingerprints": {
                        fingerprint: {
                            "firstAlertedAt": previous_alert_at,
                            "lastAlertedAt": previous_alert_at,
                            "lastSeenAt": previous_alert_at,
                            "count": 1,
                        }
                    }
                },
                now=local_now,
            )

        self.assertEqual(alertable, set())
        self.assertNotIn(fingerprint, retained_alerts)
        self.assertEqual(retained_pending, {})

    def test_alerts_immediately_when_captureboard_failure_is_confirmed(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        fingerprint = (
            "#69 수지미래산부인과의원(용인)|1진료실|"
            "MB2-C00043|캡처보드 USB나 비디오 장치를 찾지 못했어"
        )

        with patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", 6):
            first_alertable, first_alerts, first_pending = (
                reporter._collect_device_health_monitor_alert_updates(
                    _captureboard_abnormal_summary(),
                    {},
                    now=local_now,
                )
            )

        self.assertEqual(first_alertable, {fingerprint})
        self.assertIn(fingerprint, first_alerts)
        self.assertEqual(first_alerts[fingerprint]["lastAlertedAt"], local_now.isoformat())
        self.assertEqual(first_pending, {})

    def test_captureboard_alert_still_uses_reminder_suppression(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        fingerprint = (
            "#69 수지미래산부인과의원(용인)|1진료실|"
            "MB2-C00043|캡처보드 USB나 비디오 장치를 찾지 못했어"
        )

        with patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", 6):
            first_alertable, first_alerts, first_pending = (
                reporter._collect_device_health_monitor_alert_updates(
                    _captureboard_abnormal_summary(),
                    {},
                    now=local_now,
                )
            )
            second_alertable, second_alerts, second_pending = (
                reporter._collect_device_health_monitor_alert_updates(
                    _captureboard_abnormal_summary(),
                    {
                        "alertFingerprints": first_alerts,
                        "pendingAlertFingerprints": first_pending,
                    },
                    now=local_now + timedelta(minutes=1),
                )
            )

        self.assertEqual(first_alertable, {fingerprint})
        self.assertEqual(second_alertable, set())
        self.assertEqual(second_alerts[fingerprint]["lastAlertedAt"], local_now.isoformat())
        self.assertEqual(second_pending, {})

    def test_appends_device_health_monitor_event_log_jsonl(self) -> None:
        logger = logging.getLogger("test.device_health_monitor")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))

        with tempfile.TemporaryDirectory() as temp_dir:
            with patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR", temp_dir):
                reporter._append_device_health_monitor_event(
                    "run_summary",
                    {"checkedDeviceCount": 1, "statusCounts": {"정상": 1}},
                    now=local_now,
                    logger=logger,
                )

            event_path = Path(temp_dir) / "device_health_monitor_events-2026-05-03.jsonl"
            lines = event_path.read_text(encoding="utf-8").splitlines()

        self.assertEqual(len(lines), 1)
        event = json.loads(lines[0])
        self.assertEqual(event["eventType"], "run_summary")
        self.assertEqual(event["createdAt"], local_now.isoformat())
        self.assertEqual(event["checkedDeviceCount"], 1)

    def test_builds_monitor_event_summaries_without_raw_redis_snapshot(self) -> None:
        summary = {
            "deviceResults": [
                {
                    "hospitalSeq": 69,
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "roomName": "1진료실",
                    "deviceName": "MB2-C00043",
                    "overallLabel": "점검 불가",
                    "priorityReason": "상태 미갱신",
                    "statusPayload": {
                        "source": "redis_device_state",
                        "redis": {
                            "availabilityReasons": ["상태 정보가 Redis에서 갱신되지 않고 있어"],
                            "deviceState": {
                                "updatedAt": "2026-05-03T11:50:00+09:00",
                                "isConnected": False,
                                "status": "CONNECTED",
                                "screenshot": "raw-image",
                            },
                            "agentState": {"updatedAt": "2026-05-03T11:50:00+09:00"},
                        },
                    },
                },
                {
                    "hospitalSeq": 70,
                    "hospitalName": "테스트병원",
                    "roomName": "2진료실",
                    "deviceName": "MB2-C00044",
                    "overallLabel": "확인 필요",
                    "priorityReason": "Redis USB 목록에서 LED 장치를 찾지 못했어",
                    "statusPayload": {
                        "source": "redis_device_state",
                        "request": {"component": "all"},
                        "redis": {
                            "deviceState": {
                                "updatedAt": "2026-05-03T12:00:00+09:00",
                                "isConnected": True,
                                "captureBoardStatus": "connected",
                                "screenshot": "raw-image",
                            },
                            "agentState": {"updatedAt": "2026-05-03T12:00:00+09:00"},
                        },
                    },
                },
                {
                    "hospitalSeq": 71,
                    "hospitalName": "검증병원",
                    "roomName": "3진료실",
                    "deviceName": "MB2-C00045",
                    "overallLabel": "이상",
                    "priorityReason": "LED USB 장치를 찾지 못했어",
                    "statusPayload": {
                        "source": "mda_graphql+ssh_linux_commands",
                        "request": {"component": "all"},
                        "ssh": {
                            "ready": True,
                            "reason": "ready",
                            "openedThisRun": True,
                            "close": {"status": "closed"},
                        },
                    },
                },
            ]
        }

        events = reporter._iter_device_health_monitor_device_events(summary)

        self.assertEqual([event_type for event_type, _payload in events], [
            "device_unavailable",
            "redis_candidate",
            "ssh_verified_abnormal",
        ])
        self.assertEqual(events[0][1]["count"], 1)
        self.assertEqual(events[0][1]["sampleDevices"][0]["redis"]["deviceIsConnected"], False)
        self.assertNotIn("deviceState", events[1][1]["redis"])
        self.assertEqual(events[2][1]["ssh"]["closeStatus"], "closed")

    def test_builds_device_result_from_direct_linux_checks(self) -> None:
        device_context = {
            "deviceSeq": 1001,
            "deviceName": "MB2-C00043",
            "hospitalSeq": 69,
            "hospitalName": "수지미래산부인과의원(용인)",
            "hospitalTelephone": "031-123-4567",
            "hospitalDeviceAlertPhone": "",
            "roomName": "1진료실",
        }
        evidence_payload = {
            "route": "device_status_probe",
            "source": "mda_graphql+ssh",
            "request": {"deviceName": "MB2-C00043", "component": "all"},
            "device": {
                "deviceName": "MB2-C00043",
                "captureBoardType": "LS_HDMI",
                "hospitalName": "MDA 병원명",
                "roomName": "MDA 병실명",
            },
            "ssh": {"ready": True, "reason": "ready", "host": "127.0.0.1", "port": 2222},
        }
        checks = {
            "pm2_jlist": {"output": "[]"},
            "lsusb": {"output": ""},
            "serial_devices": {"output": "no_serial_device"},
            "video_devices": {"output": "/dev/video0"},
            "v4l2_devices": {"output": "video-device"},
        }

        with (
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._collect_device_health_monitor_runtime_checks_once",
                return_value=(evidence_payload, evidence_payload["device"], checks),
            ) as collect_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._summarize_audio_path_probe",
                return_value={"status": "pass", "label": "정상", "summary": "오디오 정상"},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._summarize_pm2_probe",
                return_value={"status": "pass", "label": "정상", "summary": "PM2 정상"},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_trashcan_storage_summary_from_checks",
                return_value={"status": "pass", "label": "정상", "summary": "용량 정상"},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._summarize_captureboard_probe",
                return_value={"status": "pass", "label": "정상", "summary": "캡처보드 정상"},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._summarize_led_probe",
                return_value={
                    "status": "fail",
                    "label": "이상",
                    "summary": "LED USB 장치를 찾지 못했어",
                },
            ),
        ):
            result = reporter._run_device_health_monitor_for_device(device_context)

        collect_mock.assert_called_once_with(
            "MB2-C00043",
            "all",
            now=None,
            ssh_tunnel_records=None,
        )
        self.assertEqual(result["overallLabel"], "이상")
        self.assertEqual(result["hospitalSeq"], 69)
        self.assertEqual(result["hospitalName"], "수지미래산부인과의원(용인)")
        self.assertEqual(result["roomName"], "1진료실")
        self.assertEqual(result["componentLabels"]["led"], "이상")
        self.assertEqual(result["priorityReason"], "LED 이상")
        self.assertEqual(result["statusPayload"]["route"], "device_health_monitor")
        self.assertEqual(result["statusPayload"]["source"], "mda_graphql+ssh_linux_commands")
        self.assertEqual(result["statusPayload"]["checks"], checks)
        alert_items = reporter._collect_daily_device_round_abnormal_alert_items(
            {
                "hospitalSeq": 69,
                "hospitalName": "수지미래산부인과의원(용인)",
                "deviceResults": [result],
            }
        )
        self.assertEqual(alert_items[0]["issue"], "LED USB 장치를 찾지 못했어")

    def test_builds_summary_from_redis_batch_without_ssh_for_normal_devices(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        device_context = {
            "deviceSeq": 1001,
            "deviceName": "MB2-C00043",
            "hospitalSeq": 69,
            "hospitalName": "수지미래산부인과의원(용인)",
            "hospitalTelephone": "031-123-4567",
            "hospitalDeviceAlertPhone": "",
            "roomName": "1진료실",
        }
        redis_client = _FakeRedisStateClient(
            {
                "MB2-C00043": {
                    "deviceState": {
                        "deviceName": "MB2-C00043",
                        "isConnected": True,
                        "status": "CONNECTED",
                        "captureBoardStatus": "connected",
                        "captureBoardType": "LS_HDMI",
                        "updatedAt": local_now.isoformat(),
                        "acme": {
                            "usbList": [
                                {"name": "MMTLED"},
                                {"name": "LS_HDMI captureboard"},
                            ],
                            "systemInfo": {"hddUsage": "20%"},
                        },
                    },
                    "agentState": {
                        "isConnected": True,
                        "updatedAt": local_now.isoformat(),
                    },
                }
            }
        )

        with (
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_device_candidates",
                return_value=[device_context],
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_redis_client",
                return_value=redis_client,
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._run_device_health_monitor_for_device"
            ) as ssh_verify_mock,
        ):
            summary = reporter._build_device_health_monitor_summary(now=local_now, state={})

        ssh_verify_mock.assert_not_called()
        self.assertEqual(redis_client.loaded_names, ["MB2-C00043"])
        self.assertEqual(summary["checkedDeviceCount"], 1)
        self.assertEqual(summary["abnormalCandidateCount"], 0)
        self.assertEqual(summary["sshVerifiedCandidateCount"], 0)
        self.assertEqual(summary["statusCounts"]["정상"], 1)
        self.assertEqual(summary["deviceResults"][0]["overallLabel"], "정상")
        self.assertEqual(summary["deviceResults"][0]["statusPayload"]["source"], "redis_device_state")

    def test_reuses_fresh_device_candidate_cache_without_db_lookup(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        cached_at = (local_now - timedelta(hours=1)).isoformat()
        device_context = {
            "deviceSeq": 1001,
            "deviceName": "MB2-C00043",
            "hospitalSeq": 69,
            "hospitalName": "수지미래산부인과의원(용인)",
            "hospitalTelephone": "031-123-4567",
            "hospitalDeviceAlertPhone": "",
            "roomName": "1진료실",
        }
        redis_client = _FakeRedisStateClient(
            {
                "MB2-C00043": {
                    "deviceState": {
                        "deviceName": "MB2-C00043",
                        "isConnected": True,
                        "status": "CONNECTED",
                        "captureBoardStatus": "connected",
                        "captureBoardType": "LS_HDMI",
                        "updatedAt": local_now.isoformat(),
                        "acme": {
                            "usbList": [
                                {"name": "MMTLED"},
                                {"name": "LS_HDMI captureboard"},
                            ],
                            "systemInfo": {"hddUsage": "20%"},
                        },
                    },
                    "agentState": {
                        "isConnected": True,
                        "updatedAt": local_now.isoformat(),
                    },
                }
            }
        )

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_DEVICE_CACHE_TTL_SEC", 86400),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_device_candidates"
            ) as db_lookup_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_redis_client",
                return_value=redis_client,
            ),
        ):
            summary = reporter._build_device_health_monitor_summary(
                now=local_now,
                state={
                    "deviceCandidateCache": [device_context],
                    "deviceCandidateCachedAt": cached_at,
                },
            )

        db_lookup_mock.assert_not_called()
        self.assertEqual(redis_client.loaded_names, ["MB2-C00043"])
        self.assertEqual(summary["deviceCacheSource"], "state_cache")
        self.assertFalse(summary["deviceCacheRefreshed"])
        self.assertEqual(summary["deviceCandidateCachedAt"], cached_at)
        self.assertEqual(summary["statusCounts"]["정상"], 1)

    def test_excludes_number_prefixed_hospitals_from_candidate_cache(self) -> None:
        # 출고/작업 상태를 병원명 앞 숫자_로 표시하는 가상 병원은 24시간 이상 감시 대상에서 제외해.
        candidates = reporter._normalize_device_health_monitor_device_candidate_cache(
            [
                {
                    "deviceSeq": 1001,
                    "deviceName": "MB2-C00805",
                    "hospitalSeq": 3,
                    "hospitalName": "3_작업완료 병실 출고대기",
                    "hospitalTelephone": "031-111-2222",
                    "hospitalDeviceAlertPhone": "010-1111-2222",
                    "roomName": "출고대기",
                },
                {
                    "deviceSeq": 1002,
                    "deviceName": "MB2-C00043",
                    "hospitalSeq": 69,
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "hospitalTelephone": "031-123-4567",
                    "hospitalDeviceAlertPhone": "",
                    "roomName": "1진료실",
                },
            ]
        )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["deviceName"], "MB2-C00043")

    def test_uses_stale_device_candidate_cache_when_refresh_fails(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        cached_at = (local_now - timedelta(days=2)).isoformat()
        device_context = {
            "deviceSeq": 1001,
            "deviceName": "MB2-C00043",
            "hospitalSeq": 69,
            "hospitalName": "수지미래산부인과의원(용인)",
            "hospitalTelephone": "031-123-4567",
            "hospitalDeviceAlertPhone": "",
            "roomName": "1진료실",
        }
        redis_client = _FakeRedisStateClient(
            {
                "MB2-C00043": {
                    "deviceState": {
                        "deviceName": "MB2-C00043",
                        "isConnected": True,
                        "status": "CONNECTED",
                        "captureBoardStatus": "connected",
                        "captureBoardType": "LS_HDMI",
                        "updatedAt": local_now.isoformat(),
                        "acme": {
                            "usbList": [
                                {"name": "MMTLED"},
                                {"name": "LS_HDMI captureboard"},
                            ],
                            "systemInfo": {"hddUsage": "20%"},
                        },
                    },
                    "agentState": {
                        "isConnected": True,
                        "updatedAt": local_now.isoformat(),
                    },
                }
            }
        )

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_DEVICE_CACHE_TTL_SEC", 86400),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_device_candidates",
                side_effect=RuntimeError("db down"),
            ) as db_lookup_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_redis_client",
                return_value=redis_client,
            ),
        ):
            summary = reporter._build_device_health_monitor_summary(
                now=local_now,
                state={
                    "deviceCandidateCache": [device_context],
                    "deviceCandidateCachedAt": cached_at,
                },
            )

        db_lookup_mock.assert_called_once()
        self.assertEqual(summary["deviceCacheSource"], "stale_state_cache")
        self.assertIn("db down", summary["deviceCacheRefreshError"])
        self.assertEqual(summary["statusCounts"]["정상"], 1)

    def test_redis_offline_device_is_not_alert_candidate(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        device_context = {
            "deviceSeq": 1001,
            "deviceName": "MB2-C00043",
            "hospitalSeq": 69,
            "hospitalName": "수지미래산부인과의원(용인)",
            "roomName": "1진료실",
        }
        redis_client = _FakeRedisStateClient(
            {
                "MB2-C00043": {
                    "deviceState": {
                        "deviceName": "MB2-C00043",
                        "isConnected": False,
                        "status": "CONNECTED",
                        "updatedAt": local_now.isoformat(),
                    },
                    "agentState": {
                        "isConnected": True,
                        "updatedAt": local_now.isoformat(),
                    },
                }
            }
        )

        with (
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_device_candidates",
                return_value=[device_context],
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_redis_client",
                return_value=redis_client,
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._run_device_health_monitor_for_device"
            ) as ssh_verify_mock,
        ):
            summary = reporter._build_device_health_monitor_summary(now=local_now, state={})

        ssh_verify_mock.assert_not_called()
        self.assertEqual(summary["abnormalCandidateCount"], 0)
        self.assertEqual(summary["sshVerifiedCandidateCount"], 0)
        self.assertEqual(summary["statusCounts"]["점검 불가"], 1)
        result = summary["deviceResults"][0]
        self.assertEqual(result["overallLabel"], "점검 불가")
        self.assertEqual(result["componentLabels"]["pm2"], "점검 불가")
        self.assertEqual(
            result["statusPayload"]["redis"]["availabilityReasons"],
            ["장비 socket 연결이 끊겼어"],
        )
        alert_items = reporter._collect_daily_device_round_abnormal_alert_items(summary)
        self.assertEqual(alert_items, [])

    def test_redis_stale_device_is_not_alert_candidate(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        stale_at = (local_now - timedelta(minutes=10)).isoformat()
        device_context = {
            "deviceSeq": 1001,
            "deviceName": "MB2-C00043",
            "hospitalSeq": 69,
            "hospitalName": "수지미래산부인과의원(용인)",
            "roomName": "1진료실",
        }
        redis_client = _FakeRedisStateClient(
            {
                "MB2-C00043": {
                    "deviceState": {
                        "deviceName": "MB2-C00043",
                        "isConnected": True,
                        "status": "CONNECTED",
                        "updatedAt": stale_at,
                    },
                    "agentState": {
                        "isConnected": True,
                        "updatedAt": stale_at,
                    },
                }
            }
        )

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC", 180),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_device_candidates",
                return_value=[device_context],
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_redis_client",
                return_value=redis_client,
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._run_device_health_monitor_for_device"
            ) as ssh_verify_mock,
        ):
            summary = reporter._build_device_health_monitor_summary(now=local_now, state={})

        ssh_verify_mock.assert_not_called()
        self.assertEqual(summary["abnormalCandidateCount"], 0)
        self.assertEqual(summary["statusCounts"]["점검 불가"], 1)
        self.assertEqual(summary["deviceResults"][0]["overallLabel"], "점검 불가")
        self.assertEqual(reporter._collect_daily_device_round_abnormal_alert_items(summary), [])

    def test_redis_captureboard_status_none_is_ignored_when_usb_list_confirms_captureboard(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        issues = reporter._collect_device_health_monitor_redis_issues(
            device_context={"deviceName": "MB2-C00544"},
            device_state={
                "captureBoardStatus": "none",
                "captureBoardType": "YUH01",
                "updatedAt": local_now.isoformat(),
                "acme": {
                    "usbList": [
                        {"deviceId": "1a86:7523", "name": "마미톡 LED", "type": "LED"},
                        {
                            "alias": "YUH01",
                            "deviceId": "1164:f57a",
                            "name": "신캡 (유안 캡처보드)",
                            "type": "CAPTUREBOARD",
                        }
                    ]
                },
            },
            agent_state={"updatedAt": local_now.isoformat()},
            now=local_now,
        )

        self.assertEqual([issue["component"] for issue in issues], [])

    def test_redis_led_candidate_uses_ssh_verification_before_alerting(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        device_context = {
            "deviceSeq": 1001,
            "deviceName": "MB2-C00043",
            "hospitalSeq": 69,
            "hospitalName": "수지미래산부인과의원(용인)",
            "roomName": "1진료실",
        }
        verified_result = {
            **device_context,
            "overallLabel": "이상",
            "priorityReason": "LED USB 장치를 찾지 못했어",
            "componentLabels": {
                "audio": "정상",
                "pm2": "정상",
                "storage": "정상",
                "captureboard": "정상",
                "led": "이상",
            },
            "statusPayload": {
                "ssh": {"ready": True},
                "overview": {
                    "audio": {"status": "pass", "label": "정상"},
                    "pm2": {"status": "pass", "label": "정상"},
                    "storage": {"status": "pass", "label": "정상"},
                    "captureboard": {"status": "pass", "label": "정상"},
                    "led": {
                        "status": "fail",
                        "label": "이상",
                        "summary": "LED USB 장치를 찾지 못했어",
                    },
                },
            },
        }
        redis_client = _FakeRedisStateClient(
            {
                "MB2-C00043": {
                    "deviceState": {
                        "deviceName": "MB2-C00043",
                        "isConnected": True,
                        "status": "CONNECTED",
                        "captureBoardStatus": "connected",
                        "captureBoardType": "LS_HDMI",
                        "updatedAt": local_now.isoformat(),
                        "acme": {
                            "usbList": [{"name": "LS_HDMI captureboard"}],
                            "systemInfo": {"hddUsage": "20%"},
                        },
                    },
                    "agentState": {
                        "isConnected": True,
                        "updatedAt": local_now.isoformat(),
                    },
                }
            }
        )

        with (
            patch.object(reporter.cs, "MDA_GRAPHQL_URL", "https://example.com/graphql"),
            patch.object(reporter.cs, "MDA_ADMIN_USER_PASSWORD", "secret"),
            patch.object(reporter.cs, "DEVICE_SSH_PASSWORD", "ssh-secret"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_device_candidates",
                return_value=[device_context],
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_redis_client",
                return_value=redis_client,
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._run_device_health_monitor_for_device",
                return_value=verified_result,
            ) as ssh_verify_mock,
        ):
            summary = reporter._build_device_health_monitor_summary(now=local_now, state={})

        ssh_verify_mock.assert_called_once_with(
            device_context,
            now=local_now,
            ssh_tunnel_records={},
        )
        self.assertEqual(summary["abnormalCandidateCount"], 1)
        self.assertEqual(summary["sshVerifiedCandidateCount"], 1)
        self.assertEqual(summary["statusCounts"]["이상"], 1)
        alert_items = reporter._collect_daily_device_round_abnormal_alert_items(summary)
        self.assertEqual(alert_items[0]["issue"], "LED USB 장치를 찾지 못했어")

    def test_redis_unavailable_records_state_without_fallback_or_alert(self) -> None:
        client = _FakeSlackClient()
        logger = logging.getLogger("test.device_health_monitor")
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        redis_client = _FakeRedisStateClient(
            ping_error=reporter.DeviceStateRedisUnavailable("redis down")
        )

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_CHANNEL_ID", "C_HEALTH"),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_state",
                return_value={},
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._load_device_health_monitor_device_candidates",
                return_value=[],
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._build_device_health_monitor_redis_client",
                return_value=redis_client,
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._run_device_health_monitor_for_device"
            ) as ssh_verify_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._save_device_health_monitor_state"
            ) as save_state_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._append_device_health_monitor_event"
            ) as append_event_mock,
        ):
            sent = reporter._run_device_health_monitor_once(client, logger, now=local_now)

        self.assertFalse(sent)
        self.assertEqual(client.messages, [])
        ssh_verify_mock.assert_not_called()
        saved_state = save_state_mock.call_args.args[0]
        self.assertEqual(saved_state["monitorUnavailableReason"], "redis_unavailable")
        self.assertIn("redis down", saved_state["monitorUnavailableDetail"])
        self.assertEqual(
            [call.args[0] for call in append_event_mock.call_args_list],
            ["monitor_unavailable"],
        )

    def test_marks_unready_when_open_wait_timeout_expires(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        ssh_tunnel_records: dict[str, dict[str, object]] = {}
        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SSH_OPEN_WAIT_SEC", 0),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._get_mda_device_agent_ssh",
                return_value={"deviceName": "MB2-C00043", "agentSsh": None},
            ) as get_ssh_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._open_mda_device_ssh",
                return_value={"status": "ok", "message": "open requested"},
            ) as open_ssh_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._connect_device_ssh_client"
            ) as connect_mock,
        ):
            evidence_payload, _device_info, checks = (
                reporter._collect_device_health_monitor_runtime_checks_once(
                    "MB2-C00043",
                    "all",
                    now=local_now,
                    ssh_tunnel_records=ssh_tunnel_records,
                )
            )

        get_ssh_mock.assert_called_once_with("MB2-C00043")
        open_ssh_mock.assert_called_once_with("MB2-C00043", host=None)
        connect_mock.assert_not_called()
        self.assertEqual(checks, {})
        self.assertFalse(evidence_payload["ssh"]["ready"])
        self.assertEqual(evidence_payload["ssh"]["reason"], "agent_ssh_open_timeout")
        self.assertEqual(evidence_payload["ssh"]["pollCount"], 0)
        self.assertEqual(ssh_tunnel_records, {})

    def test_waits_after_open_and_runs_checks_when_ssh_becomes_ready(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        fake_client = _FakeSshClient()
        ssh_tunnel_records: dict[str, dict[str, object]] = {}

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SSH_OPEN_WAIT_SEC", 1),
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SSH_OPEN_POLL_INTERVAL_SEC", 1),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._get_mda_device_agent_ssh",
                side_effect=[
                    {"deviceName": "MB2-C00043", "agentSsh": None},
                    {"deviceName": "MB2-C00043", "agentSsh": None},
                    {"deviceName": "MB2-C00043", "agentSsh": {"host": "127.0.0.1", "port": 2222}},
                ],
            ) as get_ssh_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._open_mda_device_ssh",
                return_value={"status": "ok", "message": "open requested"},
            ) as open_ssh_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._connect_device_ssh_client",
                return_value={"ok": True, "client": fake_client},
            ) as connect_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._run_status_probe_command",
                side_effect=lambda _client, key: {"key": key, "ok": True, "output": ""},
            ) as command_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._get_active_device_ssh_client_count",
                return_value=0,
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._close_mda_device_ssh",
                return_value={"status": "ok", "message": "closed"},
            ) as close_ssh_mock,
            patch("boxer_company_adapter_slack.device_health_monitor_reporter.time.sleep"),
        ):
            evidence_payload, _device_info, checks = (
                reporter._collect_device_health_monitor_runtime_checks_once(
                    "MB2-C00043",
                    "led",
                    now=local_now,
                    ssh_tunnel_records=ssh_tunnel_records,
                )
            )

        self.assertEqual(get_ssh_mock.call_count, 3)
        open_ssh_mock.assert_called_once_with("MB2-C00043", host=None)
        connect_mock.assert_called_once_with("127.0.0.1", 2222)
        self.assertTrue(fake_client.closed)
        self.assertTrue(evidence_payload["ssh"]["ready"])
        self.assertEqual(evidence_payload["ssh"]["reason"], "ready")
        self.assertEqual(evidence_payload["ssh"]["pollCount"], 1)
        self.assertTrue(evidence_payload["ssh"]["openedThisRun"])
        self.assertFalse(evidence_payload["ssh"]["reusedExisting"])
        self.assertEqual(set(checks), {"tools", "lsusb", "serial_devices"})
        self.assertEqual(command_mock.call_count, 3)
        close_ssh_mock.assert_called_once_with("MB2-C00043", host="127.0.0.1")
        self.assertEqual(evidence_payload["ssh"]["close"]["status"], "closed")
        self.assertEqual(ssh_tunnel_records["MB2-C00043"]["closeStatus"], "closed")

    def test_closes_ssh_client_after_direct_linux_checks(self) -> None:
        fake_client = _FakeSshClient()

        with (
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._get_mda_device_agent_ssh",
                return_value={
                    "deviceName": "MB2-C00043",
                    "agentSsh": {"host": "127.0.0.1", "port": 2222},
                },
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._open_mda_device_ssh"
            ) as open_ssh_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._connect_device_ssh_client",
                return_value={"ok": True, "client": fake_client},
            ) as connect_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._run_status_probe_command",
                side_effect=lambda _client, key: {"key": key, "ok": True, "output": ""},
            ) as command_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._close_mda_device_ssh"
            ) as close_ssh_mock,
        ):
            evidence_payload, _device_info, checks = (
                reporter._collect_device_health_monitor_runtime_checks_once("MB2-C00043", "led")
            )

        open_ssh_mock.assert_not_called()
        close_ssh_mock.assert_not_called()
        connect_mock.assert_called_once_with("127.0.0.1", 2222)
        self.assertTrue(fake_client.closed)
        self.assertTrue(evidence_payload["ssh"]["ready"])
        self.assertEqual(set(checks), {"tools", "lsusb", "serial_devices"})
        self.assertEqual(command_mock.call_count, 3)

    def test_does_not_close_owned_tunnel_when_another_ssh_client_is_active(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        ssh_tunnel_records: dict[str, dict[str, object]] = {
            "MB2-C00043": {
                "openedAt": local_now.isoformat(),
                "host": "127.0.0.1",
                "port": 2222,
                "closeStatus": "open",
                "count": 1,
            }
        }

        with (
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._get_active_device_ssh_client_count",
                return_value=1,
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._close_mda_device_ssh"
            ) as close_ssh_mock,
        ):
            close_payload = reporter._close_device_health_monitor_owned_ssh_tunnel(
                "MB2-C00043",
                host="127.0.0.1",
                port=2222,
                ssh_tunnel_records=ssh_tunnel_records,
                now=local_now,
            )

        close_ssh_mock.assert_not_called()
        self.assertEqual(close_payload["status"], "skipped_active")
        self.assertEqual(close_payload["activeClientCount"], 1)
        self.assertEqual(ssh_tunnel_records["MB2-C00043"]["closeStatus"], "skipped_active")

    def test_does_not_send_duplicate_open_when_ssh_is_already_opening(self) -> None:
        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_MONITOR_SSH_OPEN_WAIT_SEC", 0),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._get_mda_device_agent_ssh",
                return_value={
                    "deviceName": "MB2-C00043",
                    "agentSsh": {"action": "open", "status": "pending", "host": "", "port": None},
                },
            ),
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._open_mda_device_ssh"
            ) as open_ssh_mock,
            patch(
                "boxer_company_adapter_slack.device_health_monitor_reporter._connect_device_ssh_client"
            ) as connect_mock,
        ):
            evidence_payload, _device_info, checks = (
                reporter._collect_device_health_monitor_runtime_checks_once("MB2-C00043", "all")
            )

        open_ssh_mock.assert_not_called()
        connect_mock.assert_not_called()
        self.assertEqual(checks, {})
        self.assertFalse(evidence_payload["ssh"]["ready"])
        self.assertEqual(evidence_payload["ssh"]["reason"], "agent_ssh_open_in_progress")
        self.assertEqual(evidence_payload["ssh"]["pollCount"], 0)


class DeviceStateRedisClientTests(unittest.TestCase):
    def test_loads_device_and_agent_states_in_batches(self) -> None:
        names = [f"MB2-C{i:05d}" for i in range(201)]
        raw_client = _FakeRawRedis(
            {
                "device:MB2-C00000": '{"isConnected": true}',
                "agent:MB2-C00000": '{"agentVersion": "1.0.0"}',
                "device:MB2-C00200": '{"isConnected": false}',
                "agent:MB2-C00200": '{"agentVersion": "2.0.0"}',
            }
        )

        snapshot = DeviceStateRedisClient(raw_client).load_device_and_agent_states(names)

        self.assertEqual(len(raw_client.mget_calls), 4)
        self.assertEqual(raw_client.mget_calls[0][0], "device:MB2-C00000")
        self.assertEqual(raw_client.mget_calls[1][0], "agent:MB2-C00000")
        self.assertEqual(raw_client.mget_calls[2], ["device:MB2-C00200"])
        self.assertEqual(raw_client.mget_calls[3], ["agent:MB2-C00200"])
        self.assertTrue(snapshot["MB2-C00000"]["deviceState"]["isConnected"])
        self.assertEqual(snapshot["MB2-C00000"]["agentState"]["agentVersion"], "1.0.0")
        self.assertFalse(snapshot["MB2-C00200"]["deviceState"]["isConnected"])
        self.assertEqual(snapshot["MB2-C00200"]["agentState"]["agentVersion"], "2.0.0")


if __name__ == "__main__":
    unittest.main()
