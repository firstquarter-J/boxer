import unittest
from unittest.mock import patch

from boxer_company.routers.device_update import (
    _build_device_update_activity_input,
    _extract_device_name_for_update,
    _is_device_agent_update_request,
    _is_device_box_update_request,
    _is_device_update_status_request,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
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
                    "version": "3.2.10",
                }
            },
        }

        result_text, payload = _request_device_box_update("MB2-C00419 장비 업데이트")

        mock_update_box.assert_called_once_with("MB2-C00419", version="3.2.10", silent=False)
        mock_wait_for_box_update_completion.assert_called_once_with("MB2-C00419", "3.2.10")
        self.assertIn("*장비 박스 업데이트*", result_text)
        self.assertIn("최신 박스 버전", result_text)
        self.assertIn("완료", result_text)
        self.assertEqual(payload["route"], "device_box_update")
        self.assertEqual(payload["latestVersion"], "3.2.10")
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
                    "version": "3.2.10",
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
        self.assertIn("3.2.10", notices[0])

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
    @patch("boxer_company.routers.device_update._update_mda_device_agent")
    @patch("boxer_company.routers.device_update._read_agent_runtime_state")
    @patch("boxer_company.routers.device_update._get_mda_device_detail")
    def test_sends_agent_update_progress_notice_after_dispatch(
        self,
        mock_get_device_detail,
        mock_read_agent_runtime_state,
        mock_update_agent,
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
        mock_update_agent.return_value = {
            "affected": 1,
            "status": True,
            "message": "Agent update dispatched",
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
        mock_get_latest_device_version.return_value = {"versionName": "3.2.12"}
        mock_read_box_runtime_state.return_value = {
            "ssh": {"ready": True, "reason": "ready"},
            "process": {
                "name": "mommybox-v2",
                "status": "online",
                "version": "3.2.10",
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
        self.assertEqual(payload["latestVersion"], "3.2.12")


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
                    "requestedVersion": "3.2.10",
                },
                "device": {
                    "deviceName": "MB2-C00419",
                    "version": "3.2.9",
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
        self.assertIn("3.2.10", payload["description"])
        self.assertIn("Boxer Slack 박스 업데이트 요청", payload["reason"])


if __name__ == "__main__":
    unittest.main()
