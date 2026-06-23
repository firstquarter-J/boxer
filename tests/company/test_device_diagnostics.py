import logging
import unittest
from unittest.mock import patch

from boxer_company.routers.device_diagnostics import (
    _build_device_diagnostic_followup_evidence,
    _build_device_diagnostic_followup_fallback,
    _collect_device_diagnostic_snapshot,
    _clear_device_diagnostic_snapshots,
    _extract_device_name_for_diagnostic_start,
    _is_device_diagnostic_freeform_request,
    _is_device_diagnostic_start_request,
    _load_device_diagnostic_snapshot,
    _save_device_diagnostic_snapshot,
    _select_device_diagnostic_followup_command_keys,
)
from boxer_company_adapter_slack.device_routes import (
    DeviceRoutesContext,
    DeviceRoutesDeps,
    _handle_device_routes,
)
from boxer_company_adapter_slack.knowledge_routes import (
    KnowledgeRoutesContext,
    KnowledgeRoutesDeps,
    _handle_knowledge_routes,
)


def _payload() -> dict[str, object]:
    return {
        "text": "핑",
        "question": "핑",
        "user_id": "U123",
        "workspace_id": "W123",
        "channel_id": "C123",
        "current_ts": "1.1",
        "thread_ts": "1.0",
    }


def _diagnostic_snapshot() -> dict[str, object]:
    return {
        "route": "device_diagnostic_snapshot",
        "source": "mda_graphql_ssh_open+ssh_read",
        "request": {
            "deviceName": "MB2-C00419",
            "question": "MB2-C00419 진단 시작",
            "capturedAt": "2026-06-17T10:00:00+09:00",
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
        _clear_device_diagnostic_snapshots()

    def tearDown(self) -> None:
        _clear_device_diagnostic_snapshots()

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

    def test_device_route_starts_diagnostic_snapshot(self) -> None:
        replies: list[str] = []
        deps = DeviceRoutesDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {},
            has_recordings_device_mapping=lambda context: False,
            send_dm_message=lambda user_id, text: False,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        )

        with (
            patch(
                "boxer_company_adapter_slack.device_routes._is_device_runtime_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.device_routes._start_device_diagnostic_snapshot",
                return_value=("*장비 진단 스냅샷*\n• 장비: `MB2-C00419`", _diagnostic_snapshot()),
            ) as start_snapshot,
        ):
            handled = _handle_device_routes(
                DeviceRoutesContext(
                    question="MB2-C00419 진단 시작",
                    barcode=None,
                    phase2_hospital_name=None,
                    phase2_room_name=None,
                    payload=_payload(),  # type: ignore[arg-type]
                    user_id="U123",
                    workspace_id="W123",
                    channel_id="C123",
                    thread_ts="1.0",
                    reply=lambda text, **kwargs: replies.append(text),
                    client=None,
                    logger=logging.getLogger(__name__),
                ),
                deps,
            )

        self.assertTrue(handled)
        self.assertEqual(replies, ["*장비 진단 스냅샷*\n• 장비: `MB2-C00419`"])
        self.assertEqual(start_snapshot.call_args.kwargs["device_name"], "MB2-C00419")
        self.assertEqual(start_snapshot.call_args.kwargs["thread_ts"], "1.0")

    def test_device_route_requires_device_name_for_diagnostic_start(self) -> None:
        replies: list[str] = []
        deps = DeviceRoutesDeps(
            get_s3_client=lambda: None,
            get_recordings_context=lambda: {},
            has_recordings_device_mapping=lambda context: False,
            send_dm_message=lambda user_id, text: False,
            build_dependency_failure_reply=lambda action, exc: f"{action}: {type(exc).__name__}",
            reply_with_retrieval_synthesis=lambda *args, **kwargs: None,
        )

        handled = _handle_device_routes(
            DeviceRoutesContext(
                question="진단 시작",
                barcode=None,
                phase2_hospital_name=None,
                phase2_room_name=None,
                payload=_payload(),  # type: ignore[arg-type]
                user_id="U123",
                workspace_id="W123",
                channel_id="C123",
                thread_ts="1.0",
                reply=lambda text, **kwargs: replies.append(text),
                client=None,
                logger=logging.getLogger(__name__),
            ),
            deps,
        )

        self.assertTrue(handled)
        self.assertEqual(replies, ["진단 시작은 장비명이 필요해. 예: `MB2-C00419 진단 시작`"])

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

        wait_ssh.assert_called_once_with("MB2-C00419")
        connect_ssh.assert_not_called()
        self.assertEqual(snapshot["source"], "mda_graphql_ssh_open+ssh_read")
        self.assertFalse(snapshot["ssh"]["ready"])  # type: ignore[index]
        self.assertEqual(snapshot["ssh"]["reason"], "agent_ssh_not_ready")  # type: ignore[index]
        self.assertTrue(snapshot["mode"]["readOnly"])  # type: ignore[index]
        self.assertFalse(snapshot["mode"]["mdaPingSent"])  # type: ignore[index]
        self.assertTrue(snapshot["mode"]["sshOpenSent"])  # type: ignore[index]
        self.assertFalse(snapshot["mode"]["mutatingCommandsSent"])  # type: ignore[index]

    def test_knowledge_route_uses_thread_diagnostic_snapshot_before_freeform(self) -> None:
        synth_calls: list[tuple[str, dict[str, object], str, int | None]] = []
        _save_device_diagnostic_snapshot(
            workspace_id="W123",
            channel_id="C123",
            thread_ts="1.0",
            snapshot=_diagnostic_snapshot(),  # type: ignore[arg-type]
        )

        diagnostic_evidence = _diagnostic_snapshot()
        diagnostic_evidence["followupLiveCheck"] = {
            "performed": True,
            "commandKeys": ["pm2_jlist", "pm2_logs_box"],
            "capturedAt": "2026-06-17T10:01:00+09:00",
            "ssh": {"ready": True, "reason": "ready"},
            "summary": diagnostic_evidence["summary"],
        }

        with patch(
            "boxer_company_adapter_slack.knowledge_routes._build_device_diagnostic_followup_evidence",
            return_value=diagnostic_evidence,
        ) as build_evidence:
            handled = _handle_knowledge_routes(
                KnowledgeRoutesContext(
                    question="왜 반복 재시작해?",
                    barcode=None,
                    user_id="U123",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="1.0",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda *args, **kwargs: None,
                    logger=logging.getLogger(__name__),
                    client=None,
                    claude_client=None,
                ),
                KnowledgeRoutesDeps(
                    reply_with_retrieval_synthesis=lambda fallback_text, evidence_payload, route_name, **kwargs: synth_calls.append(
                        (fallback_text, evidence_payload, route_name, kwargs.get("max_tokens"))
                    ),
                    timeout_reply_text=lambda: "timeout",
                    llm_unavailable_reply_text=lambda summary=None: "down",
                    is_timeout_error=lambda exc: False,
                    is_claude_allowed_user=lambda user_id: True,
                    build_barcode_fallback_evidence=lambda: None,
                ),
            )

        self.assertTrue(handled)
        build_evidence.assert_called_once()
        self.assertEqual(len(synth_calls), 1)
        self.assertEqual(synth_calls[0][2], "device diagnostic followup")
        self.assertEqual(synth_calls[0][3], 500)
        self.assertIn("재시작 7회", synth_calls[0][0])
        self.assertEqual(synth_calls[0][1]["route"], "device_diagnostic_snapshot")
        self.assertTrue(synth_calls[0][1]["followupLiveCheck"]["performed"])  # type: ignore[index]

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

        wait_ssh.assert_called_once_with("MB2-C00419")
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

    def test_knowledge_route_auto_starts_device_diagnostic_for_freeform_device_question(self) -> None:
        synth_calls: list[tuple[str, dict[str, object], str, int | None]] = []
        evidence = _diagnostic_snapshot()
        evidence["route"] = "device_diagnostic_freeform"
        evidence["followupLiveCheck"] = {
            "performed": True,
            "commandKeys": ["pm2_jlist", "system_journal_recent"],
            "capturedAt": "2026-06-17T10:02:00+09:00",
            "ssh": {"ready": True, "reason": "ready"},
            "summary": evidence["summary"],
        }

        with (
            patch("boxer_company_adapter_slack.knowledge_routes._load_slack_thread_context", return_value=""),
            patch(
                "boxer_company_adapter_slack.knowledge_routes._is_device_diagnostic_runtime_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.knowledge_routes._start_device_diagnostic_freeform_analysis",
                return_value=("*장비 진단 답변*\n• 추가 조사: 장비 직접 접속", evidence),
            ) as start_freeform,
        ):
            handled = _handle_knowledge_routes(
                KnowledgeRoutesContext(
                    question="MB2-C00419 왜 녹화 중간에 꺼졌어?",
                    barcode=None,
                    user_id="U123",
                    payload=_payload(),  # type: ignore[arg-type]
                    thread_ts="1.0",
                    channel_id="C123",
                    current_ts="1.1",
                    reply=lambda *args, **kwargs: None,
                    logger=logging.getLogger(__name__),
                    client=None,
                    claude_client=None,
                ),
                KnowledgeRoutesDeps(
                    reply_with_retrieval_synthesis=lambda fallback_text, evidence_payload, route_name, **kwargs: synth_calls.append(
                        (fallback_text, evidence_payload, route_name, kwargs.get("max_tokens"))
                    ),
                    timeout_reply_text=lambda: "timeout",
                    llm_unavailable_reply_text=lambda summary=None: "down",
                    is_timeout_error=lambda exc: False,
                    is_claude_allowed_user=lambda user_id: True,
                    build_barcode_fallback_evidence=lambda: None,
                ),
            )

        self.assertTrue(handled)
        start_freeform.assert_called_once()
        self.assertEqual(start_freeform.call_args.kwargs["device_name"], "MB2-C00419")
        self.assertEqual(len(synth_calls), 1)
        self.assertEqual(synth_calls[0][2], "device diagnostic freeform")
        self.assertEqual(synth_calls[0][3], 500)
        self.assertEqual(synth_calls[0][1]["route"], "device_diagnostic_freeform")

    def test_followup_fallback_reports_ssh_not_ready(self) -> None:
        snapshot = _diagnostic_snapshot()
        snapshot["ssh"] = {"ready": False, "reason": "agent_ssh_not_ready"}
        snapshot["summary"] = {"sshReady": False}

        fallback = _build_device_diagnostic_followup_fallback("왜 안 돼?", snapshot)  # type: ignore[arg-type]

        self.assertIn("SSH 접속이 안 돼서", fallback)
        self.assertIn("장비 SSH 연결 준비 실패", fallback)


if __name__ == "__main__":
    unittest.main()
