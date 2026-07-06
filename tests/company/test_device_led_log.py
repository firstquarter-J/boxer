import unittest
from unittest.mock import patch

from boxer_company.routers.device_led_log import (
    _analyze_device_led_log,
    _is_device_led_log_analysis_request,
)


class DeviceLedLogRoutingTests(unittest.TestCase):
    def test_detects_device_led_log_question(self) -> None:
        self.assertTrue(
            _is_device_led_log_analysis_request(
                "MB2-C00570 7/4일 로그중 led관련 문제의 로그가 있을까?"
            )
        )
        self.assertTrue(_is_device_led_log_analysis_request("MB2-C00570 7/4 LED 이상 조사"))
        self.assertFalse(_is_device_led_log_analysis_request("MB2-C00570 LED 이상 조사"))
        self.assertFalse(_is_device_led_log_analysis_request("MB2-C00570 LED 상태"))
        self.assertFalse(_is_device_led_log_analysis_request("LED 증상 의미가 뭐야?"))


class DeviceLedLogAnalysisTests(unittest.TestCase):
    @patch("boxer_company.routers.device_led_log._fetch_s3_device_log_lines")
    def test_summarizes_busy_and_error_led_events_with_causes(self, mock_fetch) -> None:
        # 전원 버튼/SIGINT와 캡처 입력 오류가 LED 명령으로 이어지는 흐름을 요약해야 한다.
        mock_fetch.return_value = {
            "found": True,
            "key": "MB2-C00570/log-2026-07-04.log",
            "content_length": 2048,
            "lines": [
                "2026-07-04_11:05:14.557 [Acpi] warn: listen: button/power PBTN 00000080 00000000",
                "2026-07-04_11:05:14.979 [app] info: SIGINT received App Exiting. code: SIGINT",
                "2026-07-04_11:05:15.004 [MmtLED] info: Sending Command LC:3C:",
                "2026-07-04_11:05:15.223 [MmtLED] info: LED response ok: LC:OK",
                "2026-07-04_11:06:05.442 [MmtLED] info: Found device at /dev/ttyUSB0 (MmtLEDv3)",
                "2026-07-04_11:06:05.445 [MmtLED] info: LED device connected successfully",
                "2026-07-04_11:06:08.234 [FfmpegController] error: /dev/video0: No such file or directory",
                "2026-07-04_11:06:08.254 [app] warn: sendCurrentState() forcedState: Error",
                "2026-07-04_11:06:08.254 [MmtLED] info: Sending Command LC:FBL:R:B:",
                "2026-07-04_11:06:08.277 [MmtLED] info: LED response ok: LC:OK",
            ],
        }

        result_text, payload = _analyze_device_led_log(None, "MB2-C00570", "2026-07-04")

        self.assertEqual(payload["stateCounts"]["busy"], 1)
        self.assertEqual(payload["stateCounts"]["error"], 1)
        self.assertEqual(payload["ledResponseOkCount"], 2)
        self.assertIn("LC:3C:", result_text)
        self.assertIn("종료/재시작 계열", result_text)
        self.assertIn("button/power", result_text)
        self.assertIn("LC:FBL:R:B:", result_text)
        self.assertIn("/dev/video0", result_text)
        self.assertIn("실패 로그 `0건`", result_text)


if __name__ == "__main__":
    unittest.main()
