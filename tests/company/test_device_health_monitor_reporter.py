import json
import logging
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company.redis_device_state import DeviceStateRedisClient
from boxer_company_adapter_slack import device_health_monitor_reporter as reporter


class _FakeSlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs) -> dict[str, str]:
        self.messages.append(kwargs)
        return {"ts": f"3000.{len(self.messages):03d}"}


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
                "roomName": "1진료실",
                "deviceName": "MB2-C00043",
                "overallLabel": "이상",
                "priorityReason": "LED USB 장치를 찾지 못했어",
            }
        ],
    }


class DeviceHealthMonitorReporterTests(unittest.TestCase):
    def setUp(self) -> None:
        with reporter._DEVICE_HEALTH_MONITOR_RUNTIME_STATE_LOCK:
            reporter._DEVICE_HEALTH_MONITOR_RUNTIME_STATE.clear()

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
                    ":rotating_light: *이상 발견 - 확인 요망*",
                    "*#69 수지미래산부인과의원(용인)*",
                    "> *병실*  1진료실",
                    "> *장비*  `MB2-C00043`",
                    "> *이슈*  LED USB 장치를 찾지 못했어",
                    "> *MDA*  <https://mda.kr.mmtalkbox.com/monitoring?focusDevice=MB2-C00043&hospitalSeq=69|MDA Link>",
                ]
            ),
        )
        saved_state = save_state_mock.call_args.args[0]
        self.assertEqual(saved_state["processedHospitalSeqs"], [])
        self.assertIsNone(saved_state["lastHospitalSeq"])
        self.assertIsNone(saved_state["nextHospitalSeq"])
        self.assertEqual(len(saved_state["alertFingerprints"]), 1)
        self.assertEqual(
            [call.args[0] for call in append_event_mock.call_args_list],
            ["run_summary", "slack_alert_sent"],
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

    def test_uses_stale_device_candidate_cache_when_refresh_fails(self) -> None:
        local_now = datetime(2026, 5, 3, 12, 0, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        cached_at = (local_now - timedelta(days=2)).isoformat()
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
