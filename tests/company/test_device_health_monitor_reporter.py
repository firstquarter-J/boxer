import logging
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch
from zoneinfo import ZoneInfo

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
        self.assertEqual(saved_state["processedHospitalSeqs"], [69])
        self.assertEqual(saved_state["lastHospitalSeq"], 69)
        self.assertEqual(saved_state["nextHospitalSeq"], 70)
        self.assertEqual(len(saved_state["alertFingerprints"]), 1)

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
        ):
            sent = reporter._run_device_health_monitor_once(client, logger, now=local_now)

        self.assertFalse(sent)
        self.assertEqual(client.messages, [])
        saved_alert = save_state_mock.call_args.args[0]["alertFingerprints"][fingerprint]
        self.assertEqual(saved_alert["firstAlertedAt"], previous_alert_at)
        self.assertEqual(saved_alert["lastAlertedAt"], previous_alert_at)
        self.assertEqual(saved_alert["lastSeenAt"], local_now.isoformat())
        self.assertEqual(saved_alert["count"], 2)

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


if __name__ == "__main__":
    unittest.main()
