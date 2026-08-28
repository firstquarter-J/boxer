import unittest
from unittest.mock import patch

from boxer_company.operation_routing import (
    _extract_device_name_for_diagnostic_start,
    _is_device_diagnostic_freeform_request,
    _is_device_diagnostic_start_request,
    _select_device_diagnostic_followup_command_keys,
)
from boxer_company.routers import device_diagnostics as diagnostics_runtime
from boxer_company.routers.device_diagnostics import (
    _build_device_diagnostic_followup_evidence,
    _build_device_diagnostic_followup_fallback,
    _collect_device_diagnostic_snapshot,
    _load_device_diagnostic_snapshot,
    _save_device_diagnostic_snapshot,
)
def _diagnostic_snapshot() -> dict[str, object]:
    return {
        "route": "device_diagnostic_snapshot",
        "source": "mda_graphql_ssh_open+ssh_read",
        "request": {
            "deviceName": "MB2-C00419",
            "question": "MB2-C00419 진단 시작",
            "capturedAt": "2026-06-17T10:00:00+09:00",
            "requestedBy": "U123",
        },
        "device": {
            "deviceName": "MB2-C00419",
            "version": "2.11.300",
            "hospitalName": "테스트병원",
            "roomName": "1진료실",
            "isConnected": True,
        },
        "mode": {
            "readOnly": True,
            "mdaPingSent": False,
            "sshOpenSent": True,
            "mutatingCommandsSent": False,
        },
        "ssh": {
            "ready": True,
            "reason": "ready",
        },
        "summary": {
            "sshReady": True,
            "pm2": {
                "available": True,
                "reason": "ok",
                "processes": [
                    {
                        "name": "mommybox-v2",
                        "status": "online",
                        "version": "2.11.300",
                        "restartCount": 7,
                    }
                ],
            },
            "interestingLogLines": [
                {
                    "source": "pm2_logs_box",
                    "line": "Error: process exited after restart",
                }
            ],
            "interestingLogLineCount": 1,
        },
        "checks": {},
    }


class DeviceDiagnosticRoutingTests(unittest.TestCase):
    def setUp(self) -> None:
        # 프로덕션에 테스트 전용 reset API를 두지 않고 process-local state를
        # patcher가 각 테스트 전후로 격리·복원한다.
        snapshots = patch.dict(
            diagnostics_runtime._DEVICE_DIAGNOSTIC_SNAPSHOTS,
            clear=True,
        )
        snapshots.start()
        self.addCleanup(snapshots.stop)

    def test_extracts_device_name_for_diagnostic_start(self) -> None:
        self.assertEqual(
            _extract_device_name_for_diagnostic_start("MB2-C00419 진단 시작"),
            "MB2-C00419",
        )
        self.assertTrue(_is_device_diagnostic_start_request("MB2-C00419 진단 시작"))
        self.assertFalse(_is_device_diagnostic_start_request("MB2-C00419 앱 반복 재시작 원인"))

    def test_detects_device_diagnostic_freeform_after_specific_routes(self) -> None:
        self.assertTrue(_is_device_diagnostic_freeform_request("MB2-C00419 왜 녹화 중간에 꺼졌어?"))
        self.assertTrue(_is_device_diagnostic_freeform_request("MB2-C00419 앱 로그 보고 원인 찾아줘"))
        self.assertFalse(_is_device_diagnostic_freeform_request("MB2-C00419 장비 상태"))
        self.assertFalse(_is_device_diagnostic_freeform_request("장비가 왜 꺼졌어?"))

    def test_saves_and_loads_snapshot_by_thread(self) -> None:
        snapshot = _diagnostic_snapshot()

        _save_device_diagnostic_snapshot(
            workspace_id="W123",
            channel_id="C123",
            thread_ts="1.0",
            snapshot=snapshot,  # type: ignore[arg-type]
        )

        loaded = _load_device_diagnostic_snapshot(
            workspace_id="W123",
            channel_id="C123",
            thread_ts="1.0",
        )
        missing = _load_device_diagnostic_snapshot(
            workspace_id="W123",
            channel_id="C123",
            thread_ts="2.0",
        )

        self.assertIsNotNone(loaded)
        self.assertEqual(((loaded or {}).get("request") or {}).get("deviceName"), "MB2-C00419")
        self.assertIsNone(missing)

    def test_diagnostic_snapshot_opens_ssh_but_does_not_run_commands_when_not_ready(self) -> None:
        with (
            patch(
                "boxer_company.routers.device_diagnostics._wait_for_mda_device_agent_ssh",
                return_value={
                    "opened": {"status": "requested"},
                    "device": {
                        "deviceName": "MB2-C00419",
                        "hospitalName": "테스트병원",
                        "roomName": "1진료실",
                        "agentSsh": {},
                    },
                    "pollCount": 3,
                    "ready": False,
                    "reusedExisting": False,
                },
            ) as wait_ssh,
            patch("boxer_company.routers.device_diagnostics._connect_device_ssh_client") as connect_ssh,
        ):
            snapshot = _collect_device_diagnostic_snapshot(
                device_name="MB2-C00419",
                question="MB2-C00419 진단 시작",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                requested_by="U123",
            )

        wait_ssh.assert_called_once_with(
            "MB2-C00419",
            resend_enabled=True,
        )
        connect_ssh.assert_not_called()
        self.assertEqual(snapshot["source"], "mda_graphql_ssh_open+ssh_read")
        self.assertFalse(snapshot["ssh"]["ready"])  # type: ignore[index]
        self.assertEqual(snapshot["ssh"]["reason"], "agent_ssh_not_ready")  # type: ignore[index]
        self.assertTrue(snapshot["mode"]["readOnly"])  # type: ignore[index]
        self.assertFalse(snapshot["mode"]["mdaPingSent"])  # type: ignore[index]
        self.assertTrue(snapshot["mode"]["sshOpenSent"])  # type: ignore[index]
        self.assertFalse(snapshot["mode"]["mutatingCommandsSent"])  # type: ignore[index]

    def test_followup_live_evidence_opens_ssh_and_runs_read_only_commands(self) -> None:
        class FakeClient:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        commands: list[str] = []
        fake_client = FakeClient()

        def fake_run_remote_command(*args, **kwargs):  # type: ignore[no-untyped-def]
            commands.append(str(kwargs.get("command") or ""))
            return {
                "ok": True,
                "output": "Error: process restarted after power event",
            }

        with (
            patch(
                "boxer_company.routers.device_diagnostics._wait_for_mda_device_agent_ssh",
                return_value={
                    "opened": {"status": "requested"},
                    "device": {
                        "deviceName": "MB2-C00419",
                        "hospitalName": "테스트병원",
                        "roomName": "1진료실",
                        "agentSsh": {"host": "127.0.0.1", "port": 2222},
                    },
                    "pollCount": 1,
                    "ready": True,
                    "reusedExisting": False,
                },
            ) as wait_ssh,
            patch(
                "boxer_company.routers.device_diagnostics._connect_device_ssh_client",
                return_value={"ok": True, "client": fake_client},
            ) as connect_ssh,
            patch(
                "boxer_company.routers.device_diagnostics._run_remote_ssh_command",
                side_effect=fake_run_remote_command,
            ),
        ):
            evidence = _build_device_diagnostic_followup_evidence(
                "왜 녹화 중간에 장비가 꺼졌어?",
                _diagnostic_snapshot(),  # type: ignore[arg-type]
            )

        wait_ssh.assert_called_once_with(
            "MB2-C00419",
            resend_enabled=True,
        )
        connect_ssh.assert_called_once_with("127.0.0.1", 2222)
        self.assertTrue(fake_client.closed)
        live_check = evidence["followupLiveCheck"]
        self.assertTrue(live_check["performed"])  # type: ignore[index]
        self.assertTrue(live_check["sshOpenSent"])  # type: ignore[index]
        self.assertFalse(live_check["mutatingCommandsSent"])  # type: ignore[index]
        self.assertIn("system_journal_recent", live_check["commandKeys"])  # type: ignore[index]
        self.assertGreater(len(commands), 0)
        forbidden_fragments = ("pm2 restart", "pm2 stop", "pm2 delete", "shutdown -h", "poweroff", "rm -", "sudo ")
        self.assertFalse(
            any(fragment in command.lower() for command in commands for fragment in forbidden_fragments),
            commands,
        )

    def test_followup_command_selection_skips_live_for_plain_metadata_question(self) -> None:
        self.assertEqual(_select_device_diagnostic_followup_command_keys("이 장비 어느 병원이야?"), [])

    def test_followup_fallback_reports_ssh_not_ready(self) -> None:
        snapshot = _diagnostic_snapshot()
        snapshot["ssh"] = {"ready": False, "reason": "agent_ssh_not_ready"}
        snapshot["summary"] = {"sshReady": False}

        fallback = _build_device_diagnostic_followup_fallback("왜 안 돼?", snapshot)  # type: ignore[arg-type]

        self.assertIn("SSH 접속이 안 돼서", fallback)
        self.assertIn("장비 SSH 연결 준비 실패", fallback)


if __name__ == "__main__":
    unittest.main()
