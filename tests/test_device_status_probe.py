import unittest

from boxer_company.routers.device_status_probe import (
    _extract_device_name_for_status_probe,
    _is_device_captureboard_probe_request,
    _is_device_led_probe_request,
    _is_device_pm2_probe_request,
    _is_device_status_probe_request,
    _parse_device_path_list,
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


if __name__ == "__main__":
    unittest.main()
