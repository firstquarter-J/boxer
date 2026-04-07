import unittest
from unittest.mock import patch

from boxer_company.routers.device_status_probe import (
    _build_led_pattern_help_evidence,
    _build_led_pattern_help_reply,
    _patch_device_pm2_memory,
    _extract_device_name_for_status_probe,
    _is_device_captureboard_probe_request,
    _is_device_led_pattern_help_request,
    _is_device_led_probe_request,
    _is_device_memory_patch_request,
    _is_device_pm2_probe_request,
    _is_device_status_probe_request,
    _parse_device_path_list,
    _parse_pm2_memory_restart_values,
    _parse_pm2_processes,
    _parse_usb_devices,
    _render_device_status_overview_result,
    _summarize_captureboard_probe,
    _summarize_led_probe,
    _summarize_audio_path_probe,
    _summarize_pm2_probe,
)


_PM2_JLIST_OUTPUT = """[
  {
    "name": "mommybox-v2",
    "pm2_env": {
      "status": "online",
      "restart_time": 1,
      "version": "2.11.300"
    },
    "monit": {
      "cpu": 0,
      "memory": 12345678
    }
  },
  {
    "name": "mommybox-v2-agent",
    "pm2_env": {
      "status": "online",
      "restart_time": 0,
      "versioning": {
        "version": "1.2.0"
      }
    },
    "monit": {
      "cpu": 1,
      "memory": 2345678
    }
  }
]"""

_LSUSB_OUTPUT = """Bus 001 Device 002: ID 1a86:7523 QinHeng Electronics CH340 serial converter
Bus 001 Device 003: ID 1164:f57a YUH01 HDMI capture
"""


class DeviceStatusProbeRoutingTests(unittest.TestCase):
    def test_extracts_device_name_for_status_probe(self) -> None:
        self.assertEqual(
            _extract_device_name_for_status_probe("MB2-C00419 장비 상태 점검"),
            "MB2-C00419",
        )

    def test_routes_specific_pm2_captureboard_led_questions(self) -> None:
        self.assertTrue(_is_device_pm2_probe_request("MB2-C00419 pm2 상태"))
        self.assertTrue(_is_device_captureboard_probe_request("MB2-C00419 캡처보드 상태"))
        self.assertTrue(_is_device_led_probe_request("MB2-C00419 LED 상태"))

    def test_routes_led_pattern_help_questions_without_runtime_probe(self) -> None:
        self.assertTrue(_is_device_led_pattern_help_request("LED 증상은 어떨 때 나타나?"))
        self.assertTrue(_is_device_led_pattern_help_request("MB2-C00419 LED 패턴 의미가 뭐야?"))
        self.assertFalse(_is_device_led_pattern_help_request("MB2-C00419 LED 상태"))

    def test_routes_memory_patch_only_for_explicit_action_question(self) -> None:
        self.assertTrue(_is_device_memory_patch_request("MB2-C00419 메모리 패치"))
        self.assertTrue(_is_device_memory_patch_request("MB2-C00419 pm2 메모리 패치 해줘"))
        self.assertFalse(_is_device_memory_patch_request("MB2-C00419 메모리 패치 방법"))

    def test_routes_overview_status_but_not_specific_probe(self) -> None:
        self.assertTrue(_is_device_status_probe_request("MB2-C00419 장비 상태"))
        self.assertFalse(_is_device_status_probe_request("MB2-C00419 pm2 상태"))


class DeviceStatusProbeParsingTests(unittest.TestCase):
    def test_parses_pm2_processes(self) -> None:
        parsed = _parse_pm2_processes(_PM2_JLIST_OUTPUT)

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["processes"][0]["name"], "mommybox-v2")
        self.assertEqual(parsed["processes"][0]["status"], "online")
        self.assertEqual(parsed["processes"][0]["version"], "2.11.300")
        self.assertEqual(parsed["processes"][1]["name"], "mommybox-v2-agent")
        self.assertEqual(parsed["processes"][1]["version"], "1.2.0")

    def test_summarizes_pm2_as_pass_when_main_and_agent_apps_are_online(self) -> None:
        summary = _summarize_pm2_probe(_parse_pm2_processes(_PM2_JLIST_OUTPUT))

        self.assertEqual(summary["status"], "pass")
        self.assertIn("정상 실행", summary["summary"])
        self.assertIn("2.11.300", summary["evidence"])
        self.assertIn("1.2.0", summary["evidence"])

    def test_summarizes_pm2_as_warning_when_agent_app_is_missing(self) -> None:
        summary = _summarize_pm2_probe(
            {
                "available": True,
                "reason": "ok",
                "processes": [
                    {
                        "name": "mommybox-v2",
                        "status": "online",
                        "version": "2.11.300",
                        "restartCount": 0,
                    }
                ],
            }
        )

        self.assertEqual(summary["status"], "warning")
        self.assertIn("agent", summary["summary"])

    def test_parses_pm2_memory_restart_values(self) -> None:
        parsed = _parse_pm2_memory_restart_values("max_memory_restart: 4294967296")

        self.assertTrue(parsed["available"])
        self.assertEqual(parsed["values"], [4294967296])
        self.assertTrue(parsed["hasExpectedLimit"])
        self.assertIn("4.0 GB", parsed["display"])

    def test_summarizes_captureboard_as_pass_with_usb_and_video_device(self) -> None:
        summary = _summarize_captureboard_probe(
            device_info={"captureBoardType": "YUH01"},
            usb_devices=_parse_usb_devices(_LSUSB_OUTPUT),
            video_devices=_parse_device_path_list("/dev/video0\n/dev/video1", missing_token="no_video_device"),
            v4l2_devices="YUH01 HDMI capture\n\t/dev/video0\n\t/dev/video1",
        )

        self.assertEqual(summary["status"], "pass")
        self.assertIn("USB `YUH01`", summary["evidence"])

    def test_summarizes_led_as_pass_when_led_usb_is_found(self) -> None:
        summary = _summarize_led_probe(
            usb_devices=_parse_usb_devices(_LSUSB_OUTPUT),
            serial_devices=_parse_device_path_list("/dev/ttyUSB0", missing_token="no_serial_device"),
        )

        self.assertEqual(summary["status"], "pass")
        self.assertIn("LED USB", summary["evidence"])

    def test_builds_led_pattern_help_reply_with_warning_and_network_note(self) -> None:
        reply = _build_led_pattern_help_reply("LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 뭐야?")

        self.assertIn("• 결론:", reply)
        self.assertIn("warning", reply)
        self.assertIn("네트워크 오프라인", reply)

    def test_builds_led_pattern_help_evidence_with_spec_and_interpretation(self) -> None:
        evidence = _build_led_pattern_help_evidence("LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 뭐야?")

        self.assertEqual(evidence["route"], "device_led_pattern_guide")
        self.assertEqual(evidence["patternInterpretation"]["status"], "warning")
        self.assertFalse(evidence["notes"]["networkOfflineLedMapped"])
        self.assertGreaterEqual(len(evidence["ledSpec"]), 6)

    def test_summarizes_audio_path_as_passive_ok_when_devices_and_volume_exist(self) -> None:
        summary = _summarize_audio_path_probe(
            {
                "tools": {"output": "aplay=/usr/bin/aplay\namixer=/usr/bin/amixer\npactl=\nspeaker-test="},
                "playback_devices": {
                    "output": (
                        "**** List of PLAYBACK Hardware Devices ****\n"
                        "card 0: Intel [HDA Intel], device 0: Generic Analog [Generic Analog]\n"
                        "card 0: Intel [HDA Intel], device 3: Generic Digital [Generic Digital]\n"
                    )
                },
                "master_mixer": {
                    "output": (
                        "Simple mixer control 'Master',0\n"
                        "  Front Left: Playback 76 [87%] [-8.25dB] [on]\n"
                        "  Front Right: Playback 76 [87%] [-8.25dB] [on]\n"
                    )
                },
                "pcm_mixer": {
                    "output": (
                        "Simple mixer control 'PCM',0\n"
                        "  Front Left: Playback 255 [100%] [0.00dB]\n"
                        "  Front Right: Playback 255 [100%] [0.00dB]\n"
                    )
                },
                "pactl_info": {"output": "pactl_missing"},
            }
        )

        self.assertEqual(summary["status"], "pass")
        self.assertEqual(summary["label"], "정상")
        self.assertIn("Generic Analog", summary["evidence"])
        self.assertIn("87%", summary["evidence"])
        self.assertIn("오디오 장치", summary["evidence"])

    def test_overview_render_mentions_each_component(self) -> None:
        rendered = _render_device_status_overview_result(
            device_name="MB2-C00419",
            device_info={
                "version": "2.11.300",
                "hospitalName": "아이사랑산부인과의원(부산)",
                "roomName": "2진료실",
            },
            ssh_ready=True,
            ssh_reason="ready",
            audio_summary={
                "label": "정상",
                "status": "pass",
                "summary": "미니PC 오디오 장치와 음량 설정은 정상으로 보여",
                "overviewDetail": "오디오 장치 `Generic Analog`, `Generic Digital` / 음량 `Master 87% on, PCM 100%`",
                "deviceLabelsText": "`Generic Analog`, `Generic Digital`",
                "volumeText": "`Master 87% on, PCM 100%`",
            },
            pm2_summary={
                "label": "정상",
                "status": "pass",
                "summary": "PM2 기준 mommybox-v2 와 mommybox-agent 앱이 정상 실행 중이야",
                "overviewDetail": "mommybox-v2 v2.11.300 online / mommybox-agent v2.0.0 online",
            },
            captureboard_summary={
                "label": "정상",
                "status": "pass",
                "summary": "캡처보드 USB와 비디오 장치가 같이 보여",
                "overviewDetail": "MDA 타입 `YUH01` / USB `YUH01` / /dev/video `1개`",
            },
            led_summary={
                "label": "정상",
                "status": "pass",
                "summary": "LED 장치 USB 연결은 정상으로 보여",
                "overviewDetail": "LED USB 감지 / 시리얼 경로 `1개`",
            },
        )

        self.assertIn("*장비 상태 점검*", rendered)
        self.assertIn("*오디오*", rendered)
        self.assertIn("• 소리 출력: *정상* | 장치 `Generic Analog`, `Generic Digital` / 음량 `Master 87% on, PCM 100%`", rendered)
        self.assertIn("*런타임*", rendered)
        self.assertIn("• SSH 연결 상태: 🔵 *연결 가능*", rendered)
        self.assertIn("• 초음파 영상 다운로드 가능 상태: 🔵 *가능*", rendered)
        self.assertIn("• pm2 앱: *정상* | mommybox-v2 v2.11.300 online / mommybox-agent v2.0.0 online", rendered)
        self.assertIn("*하드웨어*", rendered)
        self.assertIn("• 캡처보드: *정상* | MDA 타입 `YUH01` / USB `YUH01` / /dev/video `1개`", rendered)
        self.assertIn("• LED: *정상* | LED USB 감지 / 시리얼 경로 `1개`", rendered)
        self.assertIn("*종합*", rendered)
        self.assertIn("• 상태: *정상*", rendered)
        self.assertIn("• 안내: 실제 소리 출력 테스트는 `MB2-C00419 장비 소리 출력 점검`으로 다시 명령해", rendered)

    def test_overview_render_marks_ssh_and_download_unavailable_when_ssh_not_ready(self) -> None:
        rendered = _render_device_status_overview_result(
            device_name="MB2-C00419",
            device_info={},
            ssh_ready=False,
            ssh_reason="agent_ssh_not_ready",
            audio_summary=None,
            pm2_summary=None,
            captureboard_summary=None,
            led_summary=None,
        )

        self.assertIn("• SSH 연결 상태: 🔴 *연결 불가*", rendered)
        self.assertIn("• 초음파 영상 다운로드 가능 상태: 🔴 *불가*", rendered)
        self.assertIn("• 안내: 장비 SSH 연결 준비 실패. 온라인 상태, 네트워크, 원격 접속 상태 먼저 확인해", rendered)


class DeviceMemoryPatchExecutionTests(unittest.TestCase):
    def test_patches_device_memory_only_when_precheck_is_not_4gb(self) -> None:
        client = _FakeSshClient(
            [
                {
                    "exit_status": 0,
                    "stdout": "max_memory_restart: 209715200",
                    "stderr": "",
                },
                {
                    "exit_status": 0,
                    "stdout": "deleted\nstarted\nsaved",
                    "stderr": "",
                },
                {
                    "exit_status": 0,
                    "stdout": "max_memory_restart: 4294967296",
                    "stderr": "",
                },
            ]
        )

        with (
            patch(
                "boxer_company.routers.device_status_probe._wait_for_mda_device_agent_ssh",
                return_value={
                    "ready": True,
                    "pollCount": 0,
                    "reusedExisting": True,
                    "device": {
                        "deviceName": "MB2-C00419",
                        "version": "2.11.300",
                        "hospitalName": "아이사랑산부인과의원(부산)",
                        "roomName": "2진료실",
                        "agentSsh": {
                            "host": "127.0.0.1",
                            "port": 22,
                        },
                    },
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._connect_device_ssh_client",
                return_value={
                    "ok": True,
                    "client": client,
                },
            ),
        ):
            result_text, evidence = _patch_device_pm2_memory("MB2-C00419")

        self.assertIn("*장비 메모리 패치*", result_text)
        self.assertIn("• 판정: *완료*", result_text)
        self.assertIn("• 사전 확인: `max_memory_restart=209715200 (200.0 MB)`", result_text)
        self.assertIn("• 실행 후 확인: `max_memory_restart=4294967296 (4.0 GB)`", result_text)
        self.assertEqual(evidence["precheck"]["display"], "209715200 (200.0 MB)")
        self.assertEqual(evidence["execution"]["summary"], "메모리 패치 실행")
        self.assertTrue(evidence["verification"]["hasExpectedLimit"])
        self.assertEqual(len(client.commands), 3)
        self.assertIn("pm2 prettylist | grep max_memory_restart", client.commands[0])
        self.assertTrue(client.closed)

    def test_skips_memory_patch_when_precheck_is_already_4gb(self) -> None:
        client = _FakeSshClient(
            [
                {
                    "exit_status": 0,
                    "stdout": "max_memory_restart: 4294967296",
                    "stderr": "",
                },
            ]
        )

        with (
            patch(
                "boxer_company.routers.device_status_probe._wait_for_mda_device_agent_ssh",
                return_value={
                    "ready": True,
                    "pollCount": 0,
                    "reusedExisting": True,
                    "device": {
                        "deviceName": "MB2-C00419",
                        "version": "2.11.300",
                        "hospitalName": "아이사랑산부인과의원(부산)",
                        "roomName": "2진료실",
                        "agentSsh": {
                            "host": "127.0.0.1",
                            "port": 22,
                        },
                    },
                },
            ),
            patch(
                "boxer_company.routers.device_status_probe._connect_device_ssh_client",
                return_value={
                    "ok": True,
                    "client": client,
                },
            ),
        ):
            result_text, evidence = _patch_device_pm2_memory("MB2-C00419")

        self.assertIn("• 판정: *정상*", result_text)
        self.assertIn("• 실행: 이미 정상이라 생략", result_text)
        self.assertIn("이미 4GB 메모리 설정이라 메모리 패치를 생략했어", result_text)
        self.assertTrue(evidence["precheck"]["hasExpectedLimit"])
        self.assertIsNone(evidence["execution"])
        self.assertIsNone(evidence["verification"])
        self.assertEqual(len(client.commands), 1)
        self.assertTrue(client.closed)


class _FakeSshChannel:
    def __init__(self, exit_status: int) -> None:
        self._exit_status = exit_status

    def recv_exit_status(self) -> int:
        return self._exit_status


class _FakeSshStream:
    def __init__(self, text: str, exit_status: int) -> None:
        self._text = text
        self.channel = _FakeSshChannel(exit_status)

    def read(self) -> bytes:
        return self._text.encode("utf-8")


class _FakeSshClient:
    def __init__(self, responses: list[dict[str, object]]) -> None:
        self._responses = list(responses)
        self.commands: list[str] = []
        self.closed = False

    def exec_command(self, command: str, timeout: int | None = None):
        del timeout
        self.commands.append(command)
        response = self._responses.pop(0)
        exit_status = int(response.get("exit_status") or 0)
        stdout = _FakeSshStream(str(response.get("stdout") or ""), exit_status)
        stderr = _FakeSshStream(str(response.get("stderr") or ""), exit_status)
        return None, stdout, stderr

    def close(self) -> None:
        self.closed = True


if __name__ == "__main__":
    unittest.main()
