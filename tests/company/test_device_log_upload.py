import unittest
from unittest.mock import patch

from boxer_company.routers.device_log_upload import (
    _check_and_request_device_log_upload,
    _is_device_log_upload_check_request,
)


class DeviceLogUploadRoutingTests(unittest.TestCase):
    def test_detects_device_log_upload_check_request(self) -> None:
        self.assertTrue(
            _is_device_log_upload_check_request(
                "MB2-C00419 로그 업로드 확인해줘",
                device_name="MB2-C00419",
            )
        )
        self.assertTrue(
            _is_device_log_upload_check_request(
                "장비명 MB2-C00419 2026-03-06 로그 올려줘",
                device_name="MB2-C00419",
            )
        )
        self.assertFalse(
            _is_device_log_upload_check_request(
                "MB2-C00419 pm2 상태",
                device_name="MB2-C00419",
            )
        )
        self.assertFalse(
            _is_device_log_upload_check_request(
                "s3 로그 MB2-C00419 2026-03-06",
                device_name="MB2-C00419",
            )
        )


class DeviceLogUploadExecutionTests(unittest.TestCase):
    @patch("boxer_company.routers.device_log_upload._fetch_s3_device_log_lines")
    def test_skips_dispatch_when_log_already_exists(self, mock_fetch) -> None:
        mock_fetch.return_value = {
            "found": True,
            "key": "MB2-C00419/log-2026-04-13.log",
            "content_length": 123,
            "lines": [],
        }
        dispatched: list[tuple[str, str]] = []

        result_text, payload = _check_and_request_device_log_upload(
            None,
            "MB2-C00419",
            "2026-04-13",
            has_requested_date=False,
            today_date="2026-04-13",
            dispatch_device_command=lambda device_name, command: (
                dispatched.append((device_name, command)) or {"status": True, "message": "ok"}
            ),
        )

        self.assertEqual(dispatched, [])
        self.assertTrue(payload["logFound"])
        self.assertFalse(payload["uploadRequested"])
        self.assertIn("S3에 로그가 이미 있어", result_text)

    @patch("boxer_company.routers.device_log_upload._fetch_s3_device_log_lines")
    def test_requests_today_log_upload_with_fdl(self, mock_fetch) -> None:
        mock_fetch.return_value = {
            "found": False,
            "key": "MB2-C00419/log-2026-04-13.log",
            "content_length": 0,
            "lines": [],
        }
        dispatched: list[tuple[str, str]] = []

        result_text, payload = _check_and_request_device_log_upload(
            None,
            "MB2-C00419",
            "2026-04-13",
            has_requested_date=False,
            today_date="2026-04-13",
            dispatch_device_command=lambda device_name, command: (
                dispatched.append((device_name, command)) or {"status": True, "message": "queued"}
            ),
        )

        self.assertEqual(dispatched, [("MB2-C00419", "fdl")])
        self.assertTrue(payload["uploadRequested"])
        self.assertEqual(payload["command"], "fdl")
        self.assertIn("오늘 로그 업로드 요청 보냈어", result_text)
        self.assertIn("날짜 기준: 미지정이라 오늘로 봤어", result_text)

    @patch("boxer_company.routers.device_log_upload._fetch_s3_device_log_lines")
    def test_requests_historical_log_upload_with_fdla(self, mock_fetch) -> None:
        mock_fetch.return_value = {
            "found": False,
            "key": "MB2-C00419/log-2026-04-11.log",
            "content_length": 0,
            "lines": [],
        }
        dispatched: list[tuple[str, str]] = []

        result_text, payload = _check_and_request_device_log_upload(
            None,
            "MB2-C00419",
            "2026-04-11",
            has_requested_date=True,
            today_date="2026-04-13",
            dispatch_device_command=lambda device_name, command: (
                dispatched.append((device_name, command)) or {"status": True, "message": "queued"}
            ),
        )

        self.assertEqual(dispatched, [("MB2-C00419", "fdla")])
        self.assertTrue(payload["uploadRequested"])
        self.assertEqual(payload["command"], "fdla")
        self.assertIn("지정 날짜 단건 명령이 없어 전체 로그 업로드 요청 보냈어", result_text)


if __name__ == "__main__":
    unittest.main()
