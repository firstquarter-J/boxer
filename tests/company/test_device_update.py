import json
import shlex
import unittest
from unittest.mock import MagicMock, patch

from boxer_company.operation_routing import (
    _extract_device_name_for_update,
    _is_device_agent_update_request,
    _is_device_box_update_request,
    _is_device_power_off_request,
    _is_device_update_status_request,
)
from boxer_company.routers.device_update import (
    _AGENT_GIT_STATUS_COMMAND,
    _AGENT_INSTALL_SCRIPT_COMMAND,
    _AGENT_INSTALLER_ACTIVE_STATUS_COMMAND,
    _AGENT_INSTALLER_CLI,
    _AGENT_INSTALLER_SETUP_COMMAND,
    _build_device_update_activity_input,
    _DeviceSshOpenBudget,
    _dispatch_device_agent_install_script,
    _parse_agent_repo_state,
    _query_device_update_status,
    _read_agent_runtime_state,
    _render_device_update_status_result,
    _request_device_agent_update,
    _request_device_box_update,
    _request_device_power_off,
    _run_remote_ssh_command,
    _wait_for_agent_update_completion,
    _wait_for_device_update_ssh,
)


class DeviceUpdateRoutingTests(unittest.TestCase):
    def test_extracts_device_name_for_update(self) -> None:
        self.assertEqual(
            _extract_device_name_for_update("MB2-C00419 박스 업데이트"),
            "MB2-C00419",
        )
        self.assertEqual(
            _extract_device_name_for_update("장비명 MB2-C00419 에이전트 업데이트"),
            "MB2-C00419",
        )

    def test_routes_box_agent_and_status_requests(self) -> None:
        self.assertTrue(_is_device_box_update_request("MB2-C00419 박스 업데이트"))
        self.assertTrue(_is_device_box_update_request("MB2-C00419 장비 업데이트"))
        self.assertTrue(_is_device_agent_update_request("MB2-C00419 에이전트 업데이트"))
        self.assertTrue(_is_device_update_status_request("MB2-C00419 업데이트 상태"))
        self.assertTrue(_is_device_power_off_request("MB2-C00419 장비 종료"))
        self.assertTrue(_is_device_power_off_request("MB2-C00419 전원 꺼줘"))
        self.assertFalse(_is_device_box_update_request("MB2-C00419 에이전트 업데이트"))
        self.assertFalse(_is_device_agent_update_request("MB2-C00419 업데이트 상태"))
        self.assertFalse(_is_device_power_off_request("MB2-C00419 장비 업데이트"))


class DeviceUpdateExecutionTests(unittest.TestCase):
    def test_parses_agent_repo_state_with_package_version(self) -> None:
        parsed = _parse_agent_repo_state(
            "\n".join(
                [
                    "HEAD=253cea0c888fb788f3cb803af0d0b23cb08777c2",
                    "ORIGIN_MAIN=253cea0c888fb788f3cb803af0d0b23cb08777c2",
                    "BRANCH=main",
                    "PKG_VERSION=2.0.0",
                    "ORIGIN_PKG_VERSION=2.0.1",
                ]
            )
        )

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["packageVersion"], "2.0.0")
        self.assertEqual(parsed["originPackageVersion"], "2.0.1")
        self.assertTrue(parsed["latest"])

    def test_agent_git_status_command_avoids_node_command_substitution(self) -> None:
        self.assertNotIn("$(node ", _AGENT_GIT_STATUS_COMMAND)
        self.assertIn('sed "s/^/PKG_VERSION=/"', _AGENT_GIT_STATUS_COMMAND)
        self.assertIn('sed "s/^/ORIGIN_PKG_VERSION=/"', _AGENT_GIT_STATUS_COMMAND)

    @patch("boxer_company.routers.device_update._wait_for_box_update_completion")
    @patch("boxer_company.routers.device_update._update_mda_device_box")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._read_box_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_latest_device_version")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_dispatches_box_update_via_latest_version(
        self,
        mock_get_device_detail,
        mock_get_latest_device_version,
        mock_read_box_runtime_state,
        mock_read_agent_runtime_state,
        mock_update_box,
        mock_wait_for_box_update_completion,
    ) -> None:
        mock_get_device_detail.return_value = {
            "deviceName": "MB2-C00419",
            "version": "2.11.290",
            "hospitalName": "아이사랑산부인과의원(부산)",
            "roomName": "2진료실",
            "isConnected": True,
        }
        mock_get_latest_device_version.return_value = {"versionName": "2.11.300"}
        mock_read_box_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-v2",
                "status": "online",
                "version": "2.11.290",
            },
        }
        mock_read_agent_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-agent",
                "status": "online",
                "version": "2.0.0",
            },
            "repo": {
                "available": True,
                "reason": "ok",
                "head": "abcdef0",
                "originMain": "abcdef0",
                "branch": "main",
                "packageVersion": "2.0.0",
                "latest": True,
            },
        }
        mock_update_box.return_value = {
            "affected": 1,
            "status": True,
            "message": "Box update dispatched",
        }
        mock_wait_for_box_update_completion.return_value = {
            "ok": True,
            "status": "completed",
            "runtime": {
                "process": {
                    "name": "mommybox-v2",
                    "status": "online",
                    "version": "2.11.300",
                }
            },
        }

        result_text, payload = _request_device_box_update(
            "MB2-C00419 장비 업데이트",
            resend_ssh_open=False,
        )

        mock_update_box.assert_called_once_with("MB2-C00419", version="2.11.300", silent=False)
        box_budget = mock_read_box_runtime_state.call_args.kwargs["ssh_open_budget"]
        self.assertIs(
            mock_read_agent_runtime_state.call_args.kwargs["ssh_open_budget"],
            box_budget,
        )
        mock_wait_for_box_update_completion.assert_called_once_with(
            "MB2-C00419",
            "2.11.300",
            resend_ssh_open=False,
            ssh_open_budget=box_budget,
        )
        self.assertIn("*장비 박스 업데이트*", result_text)
        self.assertIn("최신 박스 버전", result_text)
        self.assertIn("완료", result_text)
        self.assertEqual(payload["route"], "device_box_update")
        self.assertEqual(payload["latestVersion"], "2.11.300")
        self.assertTrue(payload["dispatch"]["status"])

    @patch("boxer_company.routers.device_update._wait_for_device_power_off_completion")
    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._prepare_device_power_off_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_requests_device_power_off_via_ssh(
        self,
        mock_open_device_ssh_for_update,
        mock_prepare_device_power_off_command,
        mock_run_remote_ssh_command,
        mock_wait_for_device_power_off_completion,
    ) -> None:
        client = MagicMock()
        mock_open_device_ssh_for_update.return_value = (
            {
                "deviceName": "MB2-C00419",
                "version": "2.11.300",
                "hospitalName": "아이사랑산부인과의원(부산)",
                "roomName": "2진료실",
                "isConnected": True,
            },
            {
                "ready": True,
                "reason": "ready",
                "host": "10.0.0.10",
                "port": 22,
            },
            client,
        )
        mock_prepare_device_power_off_command.return_value = {
            "ok": True,
            "reason": "ok",
            "command": "sudo -n /sbin/shutdown -h now",
            "commandLabel": "shutdown",
        }
        mock_run_remote_ssh_command.return_value = {
            "ok": True,
            "exitStatus": 0,
            "output": "poweroff_scheduled",
            "reason": "",
        }
        mock_wait_for_device_power_off_completion.return_value = {
            "ok": True,
            "status": "completed",
        }

        result_text, payload = _request_device_power_off(
            "MB2-C00419 장비 종료",
            resend_ssh_open=False,
        )

        self.assertIn("*장비 전원 종료*", result_text)
        self.assertIn("완료", result_text)
        self.assertEqual(payload["route"], "device_power_off")
        self.assertTrue(payload["dispatch"]["status"])
        self.assertFalse(
            mock_open_device_ssh_for_update.call_args.kwargs["resend_ssh_open"]
        )
        self.assertIsInstance(
            mock_open_device_ssh_for_update.call_args.kwargs["ssh_open_budget"],
            _DeviceSshOpenBudget,
        )
        mock_wait_for_device_power_off_completion.assert_called_once_with("MB2-C00419")
        client.close.assert_called()

    @patch("boxer_company.routers.device_update._wait_for_box_update_completion")
    @patch("boxer_company.routers.device_update._update_mda_device_box")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._read_box_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_latest_device_version")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_sends_box_update_progress_notice_after_dispatch(
        self,
        mock_get_device_detail,
        mock_get_latest_device_version,
        mock_read_box_runtime_state,
        mock_read_agent_runtime_state,
        mock_update_box,
        mock_wait_for_box_update_completion,
    ) -> None:
        mock_get_device_detail.return_value = {
            "deviceName": "MB2-C00419",
            "version": "2.11.290",
            "hospitalName": "아이사랑산부인과의원(부산)",
            "roomName": "2진료실",
            "isConnected": True,
        }
        mock_get_latest_device_version.return_value = {"versionName": "2.11.300"}
        mock_read_box_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-v2",
                "status": "online",
                "version": "2.11.290",
            },
        }
        mock_read_agent_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-agent",
                "status": "online",
                "version": "2.0.0",
            },
            "repo": {
                "available": True,
                "reason": "ok",
                "head": "abcdef0",
                "originMain": "abcdef0",
                "branch": "main",
                "packageVersion": "2.0.0",
                "latest": True,
            },
        }
        mock_update_box.return_value = {
            "affected": 1,
            "status": True,
            "message": "Box update dispatched",
        }
        mock_wait_for_box_update_completion.return_value = {
            "ok": True,
            "status": "completed",
            "runtime": {
                "process": {
                    "name": "mommybox-v2",
                    "status": "online",
                    "version": "2.11.300",
                }
            },
        }
        notices: list[str] = []

        _request_device_box_update(
            "MB2-C00419 장비 업데이트",
            on_dispatched=notices.append,
        )

        self.assertEqual(len(notices), 1)
        self.assertIn("장비 박스 업데이트 진행 중", notices[0])
        self.assertIn("현재 박스 버전", notices[0])
        self.assertIn("2.11.290", notices[0])
        self.assertIn("2.11.300", notices[0])

    @patch("boxer_company.routers.device_update._update_mda_device_box")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._read_box_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_latest_device_version")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_blocks_box_update_until_agent_is_v2(
        self,
        mock_get_device_detail,
        mock_get_latest_device_version,
        mock_read_box_runtime_state,
        mock_read_agent_runtime_state,
        mock_update_box,
    ) -> None:
        mock_get_device_detail.return_value = {
            "deviceName": "MB2-C00419",
            "version": "2.11.290",
            "hospitalName": "아이사랑산부인과의원(부산)",
            "roomName": "2진료실",
            "isConnected": True,
        }
        mock_get_latest_device_version.return_value = {"versionName": "3.2.10"}
        mock_read_box_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-v2",
                "status": "online",
                "version": "2.11.290",
            },
        }
        mock_read_agent_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-agent",
                "status": "online",
                "version": "1.2.0",
            },
            "repo": {
                "available": True,
                "reason": "ok",
                "head": "1234567",
                "originMain": "1234567",
                "branch": "main",
                "packageVersion": "1.2.0",
                "latest": True,
            },
        }

        result_text, payload = _request_device_box_update("MB2-C00419 장비 업데이트")

        mock_update_box.assert_not_called()
        self.assertIn("요청 불가", result_text)
        self.assertIn("에이전트 1.2.0", result_text)
        self.assertFalse(payload["agentGate"]["ok"])
        self.assertIsNone(payload["dispatch"])

    def test_rejects_explicit_box_version_request(self) -> None:
        with self.assertRaisesRegex(ValueError, "최신만 지원"):
            _request_device_box_update("MB2-C00419 박스 업데이트 2.11.296")

    @patch("boxer_company.routers.device_update._dispatch_device_agent_install_script")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_delegates_latest_check_to_common_installer(
        self,
        mock_get_device_detail,
        mock_read_agent_runtime_state,
        mock_dispatch,
    ) -> None:
        mock_get_device_detail.return_value = {
            "deviceName": "MB2-C00419",
            "version": "2.11.300",
            "hospitalName": "아이사랑산부인과의원(부산)",
            "roomName": "2진료실",
            "isConnected": True,
        }
        mock_read_agent_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-agent",
                "status": "online",
                "version": "1.2.0",
            },
            "repo": {
                "available": True,
                "reason": "ok",
                "head": "abcdef0",
                "originMain": "abcdef0",
                "branch": "main",
                "packageVersion": "1.2.0",
                "latest": True,
            },
        }

        mock_dispatch.return_value = {
            "status": True, "phase": "already_latest",
            "receipt": {"phase": "already_latest", "version": "2.0.3", "revision": "b" * 40},
        }
        result_text, payload = _request_device_agent_update("MB2-C00419 에이전트 업데이트")

        self.assertIn("생략", result_text)
        self.assertIn("이미 최신", result_text)
        self.assertEqual(payload["wait"]["status"], "already_latest")
        mock_dispatch.assert_called_once_with("MB2-C00419")
        self.assertIsNotNone(payload["dispatch"])

    def test_rejects_explicit_agent_version_request(self) -> None:
        with self.assertRaisesRegex(ValueError, "최신만 지원"):
            _request_device_agent_update("MB2-C00419 에이전트 업데이트 1.2.3")

    @patch("boxer_company.routers.device_update._wait_for_agent_update_completion")
    @patch("boxer_company.routers.device_update._dispatch_device_agent_install_script")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_dispatches_agent_update_via_install_script(
        self,
        mock_get_device_detail,
        mock_read_agent_runtime_state,
        mock_dispatch_device_agent_install_script,
        mock_wait_for_agent_update_completion,
    ) -> None:
        mock_get_device_detail.return_value = {
            "deviceName": "MB2-C00819",
            "version": "",
            "hospitalName": "통일산부인과의원(서초)",
            "roomName": "2진료실",
            "isConnected": True,
        }
        mock_read_agent_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-v2-agent",
                "status": "online",
                "version": "1.5.0",
            },
            "repo": {
                "available": False,
                "reason": "repo_missing",
                "head": "",
                "originMain": "",
                "branch": "",
                "packageVersion": "",
                "latest": False,
            },
        }
        mock_dispatch_device_agent_install_script.return_value = {
            "affected": 1,
            "status": True,
            "message": "install-agent 스크립트 실행 완료",
            "method": "ssh_install_script",
            "phase": "queued",
            "requestId": "a" * 32,
        }
        mock_wait_for_agent_update_completion.return_value = {
            "ok": True,
            "status": "completed",
            "installer": {"request": "a" * 32, "phase": "committed", "version": "2.0.3", "revision": "b" * 40},
            "runtime": {
                "process": {
                    "name": "mommybox-v2-agent",
                    "status": "online",
                    "version": "2.0.0",
                },
                "repo": {
                    "available": True,
                    "reason": "ok",
                    "head": "abcdef0",
                    "originMain": "abcdef0",
                    "branch": "main",
                    "packageVersion": "2.0.0",
                    "latest": True,
                },
            },
        }

        result_text, payload = _request_device_agent_update(
            "MB2-C00819 에이전트 업데이트",
            resend_ssh_open=False,
        )

        agent_budget = mock_read_agent_runtime_state.call_args.kwargs[
            "ssh_open_budget"
        ]
        mock_dispatch_device_agent_install_script.assert_called_once_with(
            "MB2-C00819",
            resend_ssh_open=False,
            ssh_open_budget=agent_budget,
        )
        mock_wait_for_agent_update_completion.assert_called_once_with(
            "MB2-C00819",
            baseline_runtime=mock_read_agent_runtime_state.return_value,
            update_request_id="a" * 32,
            resend_ssh_open=False,
            ssh_open_budget=agent_budget,
        )
        self.assertIn("safe_agent.js update --version latest", result_text)
        self.assertIn("공통 설치기", result_text)
        self.assertEqual(payload["dispatch"]["method"], "ssh_install_script")
        self.assertEqual(payload["source"], "mda_graphql+ssh_install_script")

    @patch("boxer_company.routers.device_update._wait_for_agent_update_completion")
    @patch("boxer_company.routers.device_update._dispatch_device_agent_install_script")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_sends_agent_update_progress_notice_after_dispatch(
        self,
        mock_get_device_detail,
        mock_read_agent_runtime_state,
        mock_dispatch_device_agent_install_script,
        mock_wait_for_agent_update_completion,
    ) -> None:
        mock_get_device_detail.return_value = {
            "deviceName": "MB2-C00419",
            "version": "3.2.10",
            "hospitalName": "아이사랑산부인과의원(부산)",
            "roomName": "2진료실",
            "isConnected": True,
        }
        mock_read_agent_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-agent",
                "status": "online",
                "version": "1.9.0",
            },
            "repo": {
                "available": True,
                "reason": "ok",
                "head": "1234567",
                "originMain": "7654321",
                "branch": "main",
                "packageVersion": "1.9.0",
                "latest": False,
            },
        }
        mock_dispatch_device_agent_install_script.return_value = {
            "affected": 1,
            "status": True,
            "message": "install-agent 스크립트 실행 완료",
            "method": "ssh_install_script",
            "phase": "queued",
            "requestId": "a" * 32,
        }
        mock_wait_for_agent_update_completion.return_value = {
            "ok": True,
            "status": "completed",
            "runtime": {
                "process": {
                    "name": "mommybox-agent",
                    "status": "online",
                    "version": "2.0.0",
                }
            },
        }
        notices: list[str] = []

        _request_device_agent_update(
            "MB2-C00419 에이전트 업데이트",
            on_dispatched=notices.append,
        )

        self.assertEqual(len(notices), 1)
        self.assertIn("장비 에이전트 업데이트 진행 중", notices[0])
        self.assertIn("현재 에이전트 버전", notices[0])
        self.assertIn("1.9.0", notices[0])
        self.assertIn("latest", notices[0])

    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._read_box_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_latest_device_version")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_renders_update_status_from_mda_and_ssh(
        self,
        mock_get_device_detail,
        mock_get_latest_device_version,
        mock_read_box_runtime_state,
        mock_read_agent_runtime_state,
    ) -> None:
        mock_get_device_detail.return_value = {
            "deviceName": "MB2-C00419",
            "version": "3.2.10",
            "hospitalName": "아이사랑산부인과의원(부산)",
            "roomName": "2진료실",
            "isConnected": True,
        }
        mock_get_latest_device_version.return_value = {"versionName": "2.11.302"}
        mock_read_box_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-v2",
                "status": "online",
                "version": "2.11.300",
            },
        }
        mock_read_agent_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-agent",
                "status": "online",
                "version": "1.2.0",
            },
            "repo": {
                "available": True,
                "reason": "ok",
                "head": "abcdef0",
                "originMain": "abcdef0",
                "branch": "main",
                "packageVersion": "1.2.0",
                "latest": True,
            },
            "installer": {"request": "a" * 32, "phase": "rolled_back", "error": "ssh_verification_failed"},
            "installerStatus": {"available": True, "reason": "ready"},
        }

        result_text, payload = _query_device_update_status(
            "MB2-C00419",
            resend_ssh_open=False,
        )

        self.assertIn("*장비 업데이트 상태*", result_text)
        self.assertIn("최신 박스 버전", result_text)
        self.assertIn("에이전트 repo 상태", result_text)
        self.assertIn("박스 업데이트 선행조건", result_text)
        self.assertIn("자동 업데이트 요청: `" + "a" * 32 + "`", result_text)
        self.assertIn("자동 업데이트 상태: `rolled_back`", result_text)
        self.assertIn("설치기 오류: `ssh_verification_failed`", result_text)
        self.assertEqual(payload["route"], "device_update_status")
        self.assertEqual(payload["latestVersion"], "2.11.302")
        self.assertFalse(payload["agentGate"]["ok"])
        self.assertEqual(payload["agentGate"]["version"], "1.2.0")
        self.assertIn("에이전트 1.2.0", payload["agentGate"]["reason"])
        status_budget = mock_read_box_runtime_state.call_args.kwargs[
            "ssh_open_budget"
        ]
        self.assertIs(
            mock_read_agent_runtime_state.call_args.kwargs["ssh_open_budget"],
            status_budget,
        )

    @patch("boxer_company.routers.device_update._get_mda_device_agent_ssh")
    @patch("boxer_company.routers.device_update._wait_for_mda_device_agent_ssh")
    def test_api_update_operation_uses_one_initial_open_budget(
        self,
        mock_wait_for_mda_device_agent_ssh,
        mock_get_mda_device_agent_ssh,
    ) -> None:
        # 첫 wait가 open을 사용한 뒤 같은 operation의 후속 조회는 상태만 읽는다.
        mock_wait_for_mda_device_agent_ssh.return_value = {
            "opened": {"status": True},
            "device": {"deviceName": "MB2-C00419", "agentSsh": None},
            "pollCount": 3,
            "ready": False,
            "reusedExisting": False,
        }
        mock_get_mda_device_agent_ssh.return_value = {
            "deviceName": "MB2-C00419",
            "agentSsh": None,
        }
        budget = _DeviceSshOpenBudget()

        first = _wait_for_device_update_ssh(
            "MB2-C00419",
            resend_ssh_open=False,
            ssh_open_budget=budget,
        )
        second = _wait_for_device_update_ssh(
            "MB2-C00419",
            resend_ssh_open=False,
            ssh_open_budget=budget,
        )

        self.assertFalse(first["ready"])
        self.assertFalse(second["ready"])
        mock_wait_for_mda_device_agent_ssh.assert_called_once_with(
            "MB2-C00419",
            resend_enabled=False,
        )
        mock_get_mda_device_agent_ssh.assert_called_once_with("MB2-C00419")


class _FakeSshChannel:
    def __init__(self, exit_status: int) -> None:
        self._exit_status = exit_status
        self.closed = False

    def recv_exit_status(self) -> int:
        return self._exit_status

    def close(self) -> None:
        self.closed = True


class _FakeSshStream:
    def __init__(self, text: str, exit_status: int) -> None:
        self._text = text
        self.channel = _FakeSshChannel(exit_status)
        self.closed = False

    def read(self) -> bytes:
        return self._text.encode("utf-8")

    def close(self) -> None:
        self.closed = True


class _FakeSshClient:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self.streams: list[_FakeSshStream] = []

    def exec_command(self, command: str, timeout: int | None = None):
        del command, timeout
        response = self._responses.pop(0)
        exit_status = int(response.get("exit_status") or 0)
        stdout = _FakeSshStream(str(response.get("stdout") or ""), exit_status)
        stderr = _FakeSshStream(str(response.get("stderr") or ""), exit_status)
        self.streams.extend([stdout, stderr])
        return None, stdout, stderr


class _NeverReadySshChannel:
    def __init__(self) -> None:
        self.closed = False
        self.timeout: int | None = None

    def settimeout(self, timeout: int) -> None:
        self.timeout = timeout

    def exit_status_ready(self) -> bool:
        return False

    def recv_exit_status(self) -> int:
        raise AssertionError("stalled channel should not call recv_exit_status")

    def close(self) -> None:
        self.closed = True


class _NeverReadySshStream:
    def __init__(self) -> None:
        self.channel = _NeverReadySshChannel()
        self.closed = False

    def read(self) -> bytes:
        raise AssertionError("stalled channel should not read output")

    def close(self) -> None:
        self.closed = True


class _NeverReadySshClient:
    def __init__(self) -> None:
        self.streams: list[_NeverReadySshStream] = []

    def exec_command(self, command: str, timeout: int | None = None):
        del command, timeout
        stdout = _NeverReadySshStream()
        stderr = _NeverReadySshStream()
        self.streams.extend([stdout, stderr])
        return None, stdout, stderr


class AgentInstallerContractTests(unittest.TestCase):
    REQUEST = "a" * 32
    REVISION = "b" * 40

    def setUp(self) -> None:
        password = patch("boxer_company.routers.device_update.cs.DEVICE_SSH_PASSWORD", "synthetic-setup-password")
        password.start()
        self.addCleanup(password.stop)

    @staticmethod
    def ready() -> dict:
        return {"ok": True, "exitStatus": 0, "output": json.dumps({"phase": "idle", "setupReady": True})}

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_dispatch_accepts_only_valid_common_installer_receipts(self, mock_open, mock_run) -> None:
        # 프로세스 실행 성공과 업데이트 접수 성공은 다르다. 기존 텍스트 응답도 거부한다.
        cases = [
            ({"accepted": True, "request": self.REQUEST, "phase": "queued"}, True, 1),
            ({"accepted": True, "request": "not-a-request", "phase": "queued"}, False, 0),
            ({"accepted": False, "request": self.REQUEST, "phase": "queued"}, False, 0),
            ({"accepted": True, "request": self.REQUEST, "phase": "committed"}, False, 0),
            ({"accepted": False, "needsUpdate": False, "phase": "already_latest", "version": "2.0.3", "revision": self.REVISION}, True, 0),
            ({"accepted": False, "needsUpdate": False, "phase": "already_latest", "version": "2.0.3", "revision": "short"}, False, 0),
            ("install-agent 스크립트 실행 완료", False, 0),
            ([], False, 0),
        ]
        for receipt, expected, affected in cases:
            with self.subTest(receipt=receipt):
                client = MagicMock()
                mock_open.return_value = ({"deviceName": "MB2-U00044"}, {"ready": True}, client)
                mock_run.side_effect = [self.ready(), {"ok": True, "exitStatus": 0, "output": json.dumps(receipt)}]
                result = _dispatch_device_agent_install_script("MB2-U00044")
                self.assertEqual(result["status"], expected)
                self.assertEqual(result["affected"], affected)
                client.close.assert_called_once()
                self.assertTrue(mock_run.call_args.kwargs["mutation"])
                if affected:
                    self.assertEqual(result["requestId"], self.REQUEST)
                    self.assertIn("확정 대기", result["message"])
                    self.assertNotIn("완료", result["message"])
        mock_run.side_effect = [self.ready(), {"ok": False, "exitStatus": 1, "reason": "ssh_exit_1", "output": json.dumps(cases[0][0])}]
        self.assertFalse(_dispatch_device_agent_install_script("MB2-U00044")["status"])
        for malformed in ["{broken", json.dumps(cases[0][0]) + "\nwarning"]:
            mock_run.side_effect = [self.ready(), {"ok": True, "exitStatus": 0, "output": malformed}]
            self.assertFalse(_dispatch_device_agent_install_script("MB2-U00044")["status"])
        self.assertEqual(_AGENT_INSTALL_SCRIPT_COMMAND, "LC_ALL=C sudo -n -- /usr/bin/node " + _AGENT_INSTALLER_CLI + " update --version latest")
        self.assertNotIn("--force", _AGENT_INSTALL_SCRIPT_COMMAND)

    def test_setup_executes_only_root_owned_download_with_fixed_environment(self) -> None:
        shell = shlex.split(_AGENT_INSTALLER_SETUP_COMMAND)
        self.assertEqual(shell[:-1], ["LC_ALL=C", "sudo", "-S", "-p", "", "--", "/usr/bin/env", "-i", "PATH=/usr/sbin:/usr/bin:/sbin:/bin", "/bin/bash", "-c"])
        self.assertIn('umask 077; BOXER_AGENT_INSTALLER_DIR="$(mktemp -d /tmp/boxer-agent-installer.XXXXXXXX)"', shell[-1])
        self.assertIn('trap \'find "$BOXER_AGENT_INSTALLER_DIR" -depth -delete\' EXIT', shell[-1])
        self.assertIn('bash "$BOXER_AGENT_INSTALLER_DIR/install-agent.sh" --safe setup', shell[-1])
        self.assertIn("https://mommybox-release.s3.ap-northeast-2.amazonaws.com/agent/installer/install-agent.sh", shell[-1])
        self.assertNotIn("synthetic-setup-password", _AGENT_INSTALLER_SETUP_COMMAND)

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_initial_setup_then_update_once_on_same_connection(self, mock_open, mock_run) -> None:
        setup = {"ok": True, "exitStatus": 0, "output": json.dumps({"phase": "ready", "setupReady": True, "cli": _AGENT_INSTALLER_CLI})}
        accepted = {"ok": True, "exitStatus": 0, "output": json.dumps({"accepted": True, "phase": "queued", "request": self.REQUEST})}
        preflights = [
            {"ok": True, "exitStatus": 0, "output": '{"installerMissing":true}'},
            {"ok": True, "exitStatus": 0, "output": '{"phase":"idle"}'},
            {"ok": True, "exitStatus": 0, "output": '{"phase":"idle","setupReady":false}'},
            {"ok": False, "exitStatus": 1, "reason": "ssh_exit_1", "output": "sudo: a password is required"},
        ]
        for preflight in preflights:
            with self.subTest(preflight=preflight):
                mock_open.reset_mock()
                mock_run.reset_mock()
                client = MagicMock()
                mock_open.return_value = ({}, {"ready": True}, client)
                mock_run.side_effect = [preflight, setup, accepted]
                budget = _DeviceSshOpenBudget()
                result = _dispatch_device_agent_install_script("MB2-U00044", resend_ssh_open=False, ssh_open_budget=budget)
                self.assertTrue(result["status"])
                self.assertEqual(result["requestId"], self.REQUEST)
                mock_open.assert_called_once_with("MB2-U00044", resend_ssh_open=False, ssh_open_budget=budget)
                calls = mock_run.call_args_list
                self.assertEqual(len(calls), 3)
                self.assertTrue(all(call.args[0] is client for call in calls))
                self.assertEqual([call.kwargs["command"] for call in calls], [_AGENT_INSTALLER_ACTIVE_STATUS_COMMAND, _AGENT_INSTALLER_SETUP_COMMAND, _AGENT_INSTALL_SCRIPT_COMMAND])
                self.assertNotIn("mutation", calls[0].kwargs)
                self.assertEqual(calls[1].kwargs["stdin_text"], "synthetic-setup-password\n")
                self.assertTrue(calls[1].kwargs["mutation"])
                self.assertTrue(calls[2].kwargs["mutation"])
                self.assertNotIn("stdin_text", calls[2].kwargs)
                self.assertNotIn("synthetic-setup-password", str(result))
                client.close.assert_called_once()

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_ambiguous_preflight_never_mutates(self, mock_open, mock_run) -> None:
        cases = [
            {"ok": False, "exitStatus": None, "reason": "timeout", "output": ""},
            {"ok": False, "exitStatus": None, "output": "sudo: a password is required"},
            {"ok": False, "exitStatus": 1, "reason": "timeout", "output": "sudo: a password is required"},
            {"ok": True, "exitStatus": 0, "output": "{broken"},
            {"ok": True, "exitStatus": 0, "output": '{"setupReady":"true","phase":"idle"}'},
            {"ok": True, "exitStatus": 0, "output": '{"phase":[]}'},
            {"ok": False, "exitStatus": 1, "output": "unrelated command failure"},
        ]
        for preflight in cases:
            with self.subTest(preflight=preflight):
                mock_run.reset_mock()
                client = MagicMock()
                mock_open.return_value = ({}, {"ready": True}, client)
                mock_run.side_effect = [preflight]
                self.assertFalse(_dispatch_device_agent_install_script("MB2-U00044")["status"])
                self.assertEqual(mock_run.call_count, 1)
                self.assertNotIn("mutation", mock_run.call_args.kwargs)
                client.close.assert_called_once()

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_setup_failure_or_invalid_receipt_never_dispatches_update(self, mock_open, mock_run) -> None:
        correct = {"phase": "ready", "setupReady": True, "cli": _AGENT_INSTALLER_CLI}
        failures = [
            {"ok": False, "exitStatus": None, "reason": "timeout", "output": ""},
            {"ok": False, "exitStatus": 1, "output": json.dumps(correct)},
            {"ok": True, "exitStatus": None, "output": json.dumps(correct)},
            {"ok": True, "exitStatus": 0, "output": json.dumps({**correct, "cli": "/tmp/untrusted.js"})},
            {"ok": True, "exitStatus": 0, "output": json.dumps({**correct, "setupReady": 1})},
            {"ok": True, "exitStatus": 0, "output": json.dumps({**correct, "error": "failed"})},
            {"ok": True, "exitStatus": 0, "output": "{broken"},
        ]
        for failure in failures:
            with self.subTest(failure=failure):
                mock_run.reset_mock()
                client = MagicMock()
                mock_open.return_value = ({}, {"ready": True}, client)
                mock_run.side_effect = [{"ok": True, "exitStatus": 0, "output": '{"installerMissing":true}'}, failure]
                result = _dispatch_device_agent_install_script("MB2-U00044")
                self.assertFalse(result["status"])
                self.assertEqual(mock_run.call_count, 2)
                self.assertEqual(mock_run.call_args.kwargs["command"], _AGENT_INSTALLER_SETUP_COMMAND)
                client.close.assert_called_once()

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_update_failure_never_retries_setup_or_dispatch(self, mock_open, mock_run) -> None:
        failure = {"ok": False, "exitStatus": None, "reason": "timeout", "output": ""}
        for initial in [False, True]:
            with self.subTest(initial=initial):
                client = MagicMock()
                mock_open.return_value = ({}, {"ready": True}, client)
                responses = [self.ready()]
                if initial:
                    responses = [
                        {"ok": False, "exitStatus": 1, "output": "sudo: a password is required"},
                        {"ok": True, "exitStatus": 0, "output": json.dumps({"phase": "ready", "setupReady": True, "cli": _AGENT_INSTALLER_CLI})},
                    ]
                mock_run.reset_mock()
                mock_run.side_effect = responses + [failure]
                self.assertFalse(_dispatch_device_agent_install_script("MB2-U00044")["status"])
                self.assertEqual(mock_run.call_count, len(responses) + 1)
                self.assertEqual(mock_run.call_args.kwargs["command"], _AGENT_INSTALL_SCRIPT_COMMAND)
                client.close.assert_called_once()

    @patch("boxer_company.routers.device_update.cs.DEVICE_SSH_PASSWORD", "")
    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_setup_requires_password_but_ready_cli_does_not(self, mock_open, mock_run) -> None:
        client = MagicMock()
        mock_open.return_value = ({}, {"ready": True}, client)
        mock_run.side_effect = [{"ok": True, "exitStatus": 0, "output": '{"installerMissing":true}'}]
        self.assertFalse(_dispatch_device_agent_install_script("MB2-U00044")["status"])
        self.assertEqual(mock_run.call_count, 1)
        mock_run.reset_mock()
        mock_run.side_effect = [self.ready(), {"ok": True, "exitStatus": 0, "output": json.dumps({"accepted": True, "phase": "queued", "request": self.REQUEST})}]
        self.assertTrue(_dispatch_device_agent_install_script("MB2-U00044")["status"])
        self.assertEqual(mock_run.call_count, 2)
        self.assertTrue(all("stdin_text" not in call.kwargs for call in mock_run.call_args_list))

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_reads_request_status_on_existing_runtime_connection(self, mock_open, mock_run) -> None:
        client = MagicMock()
        mock_open.return_value = ({}, {"ready": True}, client)
        status = {"request": self.REQUEST, "phase": "trial"}
        mock_run.side_effect = [
            {"ok": True, "output": json.dumps(status)},
        ]
        budget = _DeviceSshOpenBudget()
        runtime = _read_agent_runtime_state("MB2-U00044", resend_ssh_open=False, ssh_open_budget=budget, update_request_id=self.REQUEST)
        mock_open.assert_called_once_with("MB2-U00044", resend_ssh_open=False, ssh_open_budget=budget)
        client.close.assert_called_once()
        self.assertEqual(mock_run.call_count, 1)
        self.assertEqual(runtime["installer"], status)
        self.assertEqual(mock_run.call_args.kwargs["command"], "LC_ALL=C sudo -n -- /usr/bin/node /opt/mommybox-agent-installer/current/safe_agent.js status --request " + self.REQUEST)
        self.assertNotIn("mutation", mock_run.call_args.kwargs)

        mock_run.reset_mock(side_effect=True)
        client.reset_mock()
        mock_run.side_effect = [
            {"ok": True, "output": "[]"},
            {"ok": True, "output": "repo_missing"},
            {"ok": True, "output": json.dumps(status)},
        ]
        runtime = _read_agent_runtime_state("MB2-U00044")
        self.assertEqual(mock_run.call_count, 3)
        self.assertEqual(runtime["installer"], status)
        self.assertEqual(runtime["installerStatus"], {"available": True, "reason": "ready"})
        self.assertIn("if [ -f /opt/mommybox-agent-installer/current/safe_agent.js ]", mock_run.call_args.kwargs["command"])
        self.assertIn("status --active", mock_run.call_args.kwargs["command"])
        self.assertNotIn("mutation", mock_run.call_args.kwargs)
        client.close.assert_called_once()


    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_receipt_poll_checks_runtime_only_after_valid_matching_terminal(self, mock_open, mock_run) -> None:
        for receipt, terminal in [
            ({"request": self.REQUEST, "phase": "preparing"}, False),
            ({"request": "b" * 32, "phase": "committed", "version": "2.0.4", "revision": self.REVISION}, False),
            ({"request": self.REQUEST, "phase": "committed", "version": "2.0.4"}, False),
            ({"request": self.REQUEST, "phase": []}, False),
            ({"request": self.REQUEST, "phase": "committed", "version": "2.0.4", "revision": self.REVISION}, True),
            ({"request": self.REQUEST, "phase": "rolled_back"}, True),
        ]:
            with self.subTest(receipt=receipt):
                client = MagicMock()
                mock_open.return_value = ({}, {"ready": True}, client)
                mock_run.reset_mock(side_effect=True)
                mock_run.side_effect = [{"ok": True, "output": json.dumps(receipt)},
                                        {"ok": False, "reason": "timeout"}, {"ok": False, "reason": "timeout"}]
                runtime = _read_agent_runtime_state("MB2-U00044", update_request_id=self.REQUEST)
                self.assertEqual(mock_run.call_count, 3 if terminal else 1)
                self.assertIn("status --request", mock_run.call_args_list[0].kwargs["command"])
                self.assertEqual(runtime["installer"], receipt)
                client.close.assert_called_once()

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    @patch("boxer_company.routers.device_update._connect_runtime_probe")
    def test_nightly_agent_uses_existing_recovery_and_preserves_initial_resends(self, mock_recover, mock_open, mock_run) -> None:
        client = MagicMock()
        mock_recover.return_value = ({"ssh": {"ready": True}}, {"deviceName": "MB2-U00044", "isConnected": True},
                                     {"ok": True, "client": client})
        mock_run.side_effect = [{"ok": True, "output": "[]"}, {"ok": True, "output": "repo_missing"},
                                {"ok": True, "output": '{"installerMissing":true}'}]
        _read_agent_runtime_state("MB2-U00044", recover_stale_ssh=True)
        mock_recover.assert_called_once_with("MB2-U00044", "agent", resend_ssh_open=True)
        mock_open.assert_not_called()
        client.close.assert_called_once()

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_valid_commit_is_not_reversed_when_final_runtime_checks_fail(self, mock_open, mock_run) -> None:
        client = MagicMock()
        mock_open.return_value = ({"deviceName": "MB2-U00044"}, {"ready": True}, client)
        receipt = {"request": self.REQUEST, "phase": "committed", "version": "2.0.4", "revision": self.REVISION}
        mock_run.side_effect = [{"ok": True, "output": json.dumps(receipt)},
                                {"ok": False, "reason": "timeout"}, {"ok": False, "reason": "timeout"}]
        result = _wait_for_agent_update_completion("MB2-U00044", update_request_id=self.REQUEST)
        self.assertTrue(result["ok"])
        self.assertEqual(result["installer"], receipt)
        self.assertFalse(result["runtime"]["repo"]["available"])
        self.assertIsNone(result["runtime"]["process"])

    @patch("boxer_company.routers.device_update._dispatch_device_agent_install_script")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_reuses_only_matching_precheck_and_never_dispatches_failed_ssh(self, mock_detail, mock_read, mock_dispatch) -> None:
        runtime = {"device": {"deviceName": "MB2-U00044", "isConnected": True}, "ssh": {"ready": False}}
        _, payload = _request_device_agent_update("MB2-U00044 에이전트 업데이트", prechecked_runtime=runtime)
        self.assertIsNone(payload["dispatch"])
        mock_detail.assert_not_called()
        mock_read.assert_not_called()
        mock_dispatch.assert_not_called()
        for device in ({"deviceName": "MB2-U00045", "isConnected": True}, {"deviceName": "MB2-U00044"}):
            with self.subTest(device=device), self.assertRaises(ValueError):
                _request_device_agent_update("MB2-U00044 에이전트 업데이트", prechecked_runtime={"device": device})

    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._read_box_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_latest_device_version")
    def test_nightly_offline_and_first_ssh_failure_skip_followup_reads(self, mock_latest, mock_box, mock_agent) -> None:
        device = {"deviceName": "MB2-U00044", "isConnected": False}
        _, payload = _query_device_update_status("MB2-U00044", agent_only=True, device_info=device)
        self.assertFalse(payload["agentRuntime"]["ssh"]["ready"])
        mock_latest.assert_not_called()
        mock_box.assert_not_called()
        mock_agent.assert_not_called()
        device["isConnected"] = True
        mock_box.return_value = {"ssh": {"ready": False, "reason": "timeout"}}
        _query_device_update_status("MB2-U00044", device_info=device)
        mock_agent.assert_not_called()
        mock_box.assert_called_once()
        with self.assertRaises(ValueError):
            _query_device_update_status("MB2-U00044", device_info={**device, "deviceName": "MB2-U00045"})

    @patch("boxer_company.routers.device_update._run_remote_ssh_command")
    @patch("boxer_company.routers.device_update._open_device_ssh_for_update")
    def test_preserves_runtime_when_active_installer_is_missing_or_unreadable(self, mock_open, mock_run) -> None:
        cases = [
            ({"ok": True, "output": '{"installerMissing":true}'}, False, "installer_missing"),
            ({"ok": False, "reason": "ssh_exit_1", "output": "failure"}, None, "status_unavailable"),
            ({"ok": True, "output": "{broken"}, None, "status_unavailable"),
            ({"ok": True, "output": '{"phase":"committed","request":"bad"}'}, None, "status_unavailable"),
        ]
        for result, available, reason in cases:
            with self.subTest(result=result):
                client = MagicMock()
                mock_open.return_value = ({}, {"ready": True}, client)
                mock_run.side_effect = [
                    {"ok": True, "output": json.dumps([{"name": "mommybox-v2-agent", "pid": 99, "pm2_env": {"status": "online", "version": "1.2.0"}}])},
                    {"ok": True, "output": "HEAD=abcdef0\nORIGIN_MAIN=abcdef0\nBRANCH=main\nPKG_VERSION=1.2.0"},
                    result,
                ]
                runtime = _read_agent_runtime_state("MB2-U00044")
                self.assertEqual(runtime["installerStatus"], {"available": available, "reason": reason})
                self.assertNotIn("installer", runtime)
                self.assertEqual(runtime["process"]["status"], "online")
                self.assertTrue(runtime["repo"]["latest"])
                client.close.assert_called_once()

    def test_renders_missing_and_unknown_automatic_update_status_separately(self) -> None:
        cases = [
            ({"installerStatus": {"available": False, "reason": "installer_missing"}}, "없음 (안전 설치기 미설치)"),
            ({"installer": {"phase": "idle"}, "installerStatus": {"available": True, "reason": "ready"}}, "없음 (요청 기록 없음)"),
            ({"installerStatus": {"available": None, "reason": "status_unavailable"}}, "미확인 (`status_unavailable`)"),
        ]
        for installer_state, expected in cases:
            with self.subTest(installer_state=installer_state):
                runtime = {"ssh": {"ready": True}, "process": {"name": "mommybox-v2-agent", "status": "online", "version": "1.2.0"}, **installer_state}
                rendered = _render_device_update_status_result(
                    device_name="MB2-U00044", device_info={"deviceName": "MB2-U00044"},
                    latest_box_version="2.11.315", box_runtime={}, agent_runtime=runtime,
                )
                self.assertIn(expected, rendered)
                self.assertIn("에이전트 repo 상태", rendered)
                self.assertIn("online", rendered)

    @patch("boxer_company.routers.device_update.time.sleep")
    @patch("boxer_company.routers.device_update.time.monotonic", side_effect=[0, 1, 2])
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    def test_waits_for_matching_commit_even_when_old_process_is_online(self, mock_read, mock_clock, mock_sleep) -> None:
        def state(phase: str) -> dict:
            return {"process": {"status": "online", "version": "2.0.0", "pid": 99},
                    "installer": {"request": self.REQUEST, "phase": phase, "version": "2.0.3", "revision": self.REVISION}}
        mock_read.side_effect = [state("queued"), state("trial"), state("committed")]
        budget = _DeviceSshOpenBudget()
        result = _wait_for_agent_update_completion("MB2-U00044", update_request_id=self.REQUEST, resend_ssh_open=False, ssh_open_budget=budget)
        self.assertTrue(result["ok"])
        self.assertEqual(result["attempts"], 3)
        self.assertEqual(result["observedVersion"], "2.0.3")
        for call in mock_read.call_args_list:
            self.assertEqual(call.kwargs["update_request_id"], self.REQUEST)
            self.assertIs(call.kwargs["ssh_open_budget"], budget)
            self.assertFalse(call.kwargs["resend_ssh_open"])

    @patch("boxer_company.routers.device_update.time.sleep")
    @patch("boxer_company.routers.device_update.time.monotonic", side_effect=[0, 10000])
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    def test_rejects_other_request_commit(self, mock_read, mock_clock, mock_sleep) -> None:
        mock_read.return_value = {"process": {"status": "online", "version": "2.0.3"},
                                  "installer": {"request": "c" * 32, "phase": "committed", "version": "2.0.3", "revision": self.REVISION}}
        result = _wait_for_agent_update_completion("MB2-U00044", update_request_id=self.REQUEST)
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "timed_out")
        mock_sleep.assert_not_called()

    @patch("boxer_company.routers.device_update.time.sleep")
    @patch("boxer_company.routers.device_update.time.monotonic")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    def test_does_not_complete_from_online_or_malformed_status(self, mock_read, mock_clock, mock_sleep) -> None:
        for installer in [None, {"request": self.REQUEST, "phase": []}, {"request": self.REQUEST, "phase": "committed", "version": "2.0.3"}]:
            with self.subTest(installer=installer):
                mock_clock.side_effect = [0, 10000]
                mock_read.return_value = {"process": {"status": "online", "version": "2.0.3"}, "installer": installer}
                result = _wait_for_agent_update_completion("MB2-U00044", update_request_id=self.REQUEST)
                self.assertFalse(result["ok"])
                self.assertEqual(result["status"], "timed_out")
        mock_sleep.assert_not_called()

    @patch("boxer_company.routers.device_update.time.sleep")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    def test_stops_wait_immediately_on_installer_failure(self, mock_read, mock_sleep) -> None:
        for phase in ["rolled_back", "failed"]:
            with self.subTest(phase=phase):
                mock_read.return_value = {"process": {"status": "online", "version": "2.0.3"},
                                          "installer": {"request": self.REQUEST, "phase": phase, "error": "probe_failed"}}
                result = _wait_for_agent_update_completion("MB2-U00044", update_request_id=self.REQUEST)
                self.assertFalse(result["ok"])
                self.assertEqual(result["status"], phase)
                self.assertEqual(result["attempts"], 1)
        mock_sleep.assert_not_called()

    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    def test_accepts_matching_already_latest_status_and_rejects_invalid_request(self, mock_read) -> None:
        mock_read.return_value = {"installer": {"request": self.REQUEST, "phase": "already_latest", "version": "2.0.3", "revision": self.REVISION}}
        result = _wait_for_agent_update_completion("MB2-U00044", update_request_id=self.REQUEST)
        self.assertTrue(result["ok"])
        mock_read.reset_mock()
        self.assertFalse(_wait_for_agent_update_completion("MB2-U00044", update_request_id="bad;command")["ok"])
        mock_read.assert_not_called()


class DeviceUpdateSshLifecycleTests(unittest.TestCase):
    @patch("boxer_company.routers.device_update._wait_for_ssh_exit_status")
    @patch("boxer_company.routers.device_update._mark_company_api_mutation_attempted")
    def test_sends_secret_only_to_stdin_before_wait_and_redacts_remote_echo(self, mock_mark, mock_wait) -> None:
        # 합성 비밀번호의 echo가 돌아와도 결과 payload에 남기지 않는다.
        secret = "synthetic-stdin-secret"
        events = []
        stdin = MagicMock()
        stdin.write.side_effect = lambda value: events.append(("write", value))
        stdin.flush.side_effect = lambda: events.append(("flush",))
        stdin.channel.shutdown_write.side_effect = lambda: events.append(("eof",))
        stdout = _FakeSshStream("ready " + secret, 0)
        stderr = _FakeSshStream("echo " + secret, 0)
        client = MagicMock()
        client.exec_command.side_effect = lambda *args, **kwargs: events.append(("exec",)) or (stdin, stdout, stderr)
        mock_mark.side_effect = lambda: events.append(("mutation",))
        mock_wait.side_effect = lambda *args, **kwargs: events.append(("wait",)) or 0

        result = _run_remote_ssh_command(client, command=_AGENT_INSTALLER_SETUP_COMMAND, timeout_sec=120, mutation=True, stdin_text=secret + "\n")

        self.assertTrue(result["ok"])
        self.assertEqual(events, [("mutation",), ("exec",), ("write", secret + "\n"), ("flush",), ("eof",), ("wait",)])
        self.assertNotIn(secret, str(result))
        self.assertNotIn(secret, str(client.exec_command.call_args))
        self.assertIn("[REDACTED]", result["output"])
        stdin.close.assert_called_once()
        self.assertTrue(stdout.closed and stderr.closed)

    @patch("boxer_company.routers.device_update._wait_for_ssh_exit_status")
    def test_secret_write_failure_is_ambiguous_and_never_echoes_exception_text(self, mock_wait) -> None:
        stdin = MagicMock()
        stdin.write.side_effect = OSError("synthetic-stdin-secret")
        client = MagicMock()
        client.exec_command.return_value = (stdin, _FakeSshStream("", 0), _FakeSshStream("", 0))
        result = _run_remote_ssh_command(client, command=_AGENT_INSTALLER_SETUP_COMMAND, timeout_sec=120, stdin_text="synthetic-stdin-secret\n")
        self.assertFalse(result["ok"])
        self.assertIsNone(result["exitStatus"])
        self.assertNotIn("synthetic-stdin-secret", str(result))
        mock_wait.assert_not_called()
        stdin.close.assert_called_once()

    def test_closes_ssh_streams_after_command(self) -> None:
        client = _FakeSshClient(
            [
                {
                    "exit_status": 0,
                    "stdout": "ok",
                    "stderr": "",
                }
            ]
        )

        result = _run_remote_ssh_command(
            client,
            command="echo ok",
            timeout_sec=5,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(len(client.streams), 2)
        self.assertTrue(all(stream.closed for stream in client.streams))
        self.assertTrue(all(stream.channel.closed for stream in client.streams))

    def test_times_out_and_closes_stalled_channel(self) -> None:
        client = _NeverReadySshClient()

        with patch("boxer_company.routers.ssh_command.time.monotonic", side_effect=[0, 2]):
            result = _run_remote_ssh_command(
                client,
                command="sleep forever",
                timeout_sec=1,
            )

        self.assertFalse(result["ok"])
        self.assertEqual(result["reason"], "timeout")
        self.assertIsNone(result["exitStatus"])
        self.assertEqual(len(client.streams), 2)
        self.assertTrue(all(stream.closed for stream in client.streams))
        self.assertTrue(all(stream.channel.closed for stream in client.streams))


class DeviceUpdateActivityLogTests(unittest.TestCase):
    def test_builds_box_update_activity_payload(self) -> None:
        payload = _build_device_update_activity_input(
            question="MB2-C00419 박스 업데이트",
            user_id="U123",
            user_name="Rosa",
            channel_id="C123",
            thread_ts="123.456",
            result_payload={
                "route": "device_box_update",
                "request": {
                    "deviceName": "MB2-C00419",
                    "requestedVersion": "2.11.300",
                },
                "device": {
                    "deviceName": "MB2-C00419",
                    "version": "2.11.299",
                },
                "dispatch": {
                    "status": True,
                    "message": "Box update dispatched",
                },
                "wait": {
                    "status": "completed",
                    "ok": True,
                },
            },
        )

        self.assertEqual(payload["activityType"], "device.edit")
        self.assertIn("MB2-C00419", payload["description"])
        self.assertIn("2.11.300", payload["description"])
        self.assertIn("Boxer Slack 박스 업데이트 요청", payload["reason"])

    def test_builds_power_off_activity_payload(self) -> None:
        payload = _build_device_update_activity_input(
            question="MB2-C00419 장비 종료",
            user_id="U123",
            user_name="Rosa",
            channel_id="C123",
            thread_ts="123.456",
            result_payload={
                "route": "device_power_off",
                "request": {
                    "deviceName": "MB2-C00419",
                    "requestedAction": "power_off",
                },
                "device": {
                    "deviceName": "MB2-C00419",
                    "version": "2.11.300",
                },
                "dispatch": {
                    "status": True,
                    "message": "poweroff_scheduled",
                },
                "wait": {
                    "status": "completed",
                    "ok": True,
                },
            },
        )

        self.assertEqual(payload["activityType"], "device.edit")
        self.assertIn("MB2-C00419", payload["description"])
        self.assertIn("장비 종료", payload["reason"])


if __name__ == "__main__":
    unittest.main()
