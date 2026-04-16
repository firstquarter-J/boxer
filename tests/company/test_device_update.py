import unittest
from unittest.mock import patch

from boxer_company.routers.device_update import (
    _AGENT_GIT_STATUS_COMMAND,
    _build_device_update_activity_input,
    _extract_device_name_for_update,
    _is_device_agent_update_request,
    _is_device_box_update_request,
    _is_device_update_status_request,
    _parse_agent_repo_state,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
    _run_remote_ssh_command,
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
        self.assertFalse(_is_device_box_update_request("MB2-C00419 에이전트 업데이트"))
        self.assertFalse(_is_device_agent_update_request("MB2-C00419 업데이트 상태"))


class DeviceUpdateExecutionTests(unittest.TestCase):
    def test_parses_agent_repo_state_with_package_version(self) -> None:
        parsed = _parse_agent_repo_state(
            "\n".join(
                [
                    "HEAD=253cea0c888fb788f3cb803af0d0b23cb08777c2",
                    "ORIGIN_MAIN=253cea0c888fb788f3cb803af0d0b23cb08777c2",
                    "BRANCH=main",
                    "PKG_VERSION=2.0.0",
                ]
            )
        )

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["packageVersion"], "2.0.0")
        self.assertTrue(parsed["latest"])

    def test_agent_git_status_command_avoids_node_command_substitution(self) -> None:
        self.assertNotIn("$(node ", _AGENT_GIT_STATUS_COMMAND)
        self.assertIn('sed "s/^/PKG_VERSION=/"', _AGENT_GIT_STATUS_COMMAND)

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

        result_text, payload = _request_device_box_update("MB2-C00419 장비 업데이트")

        mock_update_box.assert_called_once_with("MB2-C00419", version="2.11.300", silent=False)
        mock_wait_for_box_update_completion.assert_called_once_with("MB2-C00419", "2.11.300")
        self.assertIn("*장비 박스 업데이트*", result_text)
        self.assertIn("최신 박스 버전", result_text)
        self.assertIn("완료", result_text)
        self.assertEqual(payload["route"], "device_box_update")
        self.assertEqual(payload["latestVersion"], "2.11.300")
        self.assertTrue(payload["dispatch"]["status"])

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

    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_skips_agent_dispatch_when_already_latest(
        self,
        mock_get_device_detail,
        mock_read_agent_runtime_state,
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

        result_text, payload = _request_device_agent_update("MB2-C00419 에이전트 업데이트")

        self.assertIn("생략", result_text)
        self.assertIn("이미 최신", result_text)
        self.assertEqual(payload["wait"]["status"], "already_latest")
        self.assertIsNone(payload["dispatch"])

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
        }
        mock_wait_for_agent_update_completion.return_value = {
            "ok": True,
            "status": "completed",
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

        result_text, payload = _request_device_agent_update("MB2-C00819 에이전트 업데이트")

        mock_dispatch_device_agent_install_script.assert_called_once_with("MB2-C00819")
        mock_wait_for_agent_update_completion.assert_called_once_with(
            "MB2-C00819",
            baseline_runtime=mock_read_agent_runtime_state.return_value,
        )
        self.assertIn("install-agent.sh -f 1", result_text)
        self.assertIn("SSH 스크립트", result_text)
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
        }

        result_text, payload = _query_device_update_status("MB2-C00419")

        self.assertIn("*장비 업데이트 상태*", result_text)
        self.assertIn("최신 박스 버전", result_text)
        self.assertIn("에이전트 repo 상태", result_text)
        self.assertIn("박스 업데이트 선행조건", result_text)
        self.assertEqual(payload["route"], "device_update_status")
        self.assertEqual(payload["latestVersion"], "2.11.302")
        self.assertFalse(payload["agentGate"]["ok"])
        self.assertEqual(payload["agentGate"]["version"], "1.2.0")
        self.assertIn("에이전트 1.2.0", payload["agentGate"]["reason"])


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


class DeviceUpdateSshLifecycleTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
