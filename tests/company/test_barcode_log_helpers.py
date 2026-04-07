import unittest

from boxer_company_adapter_slack.barcode_logs import (
    _build_barcode_log_error_session_section,
    _needs_barcode_log_fallback,
    _split_barcode_log_reply,
)


class BarcodeLogHelperTests(unittest.TestCase):
    def test_split_barcode_log_reply_preserves_scanned_block_context(self) -> None:
        reply_text = "\n".join(
            [
                "*로그 분석 결과*",
                "",
                "• scanned 이벤트:",
                "```",
                "09:00:01 scanned foo",
                "09:00:02 scanned bar",
                "09:00:03 scanned baz",
                "```",
            ]
        )

        chunks = _split_barcode_log_reply(reply_text, max_chars=60)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(chunks[0].startswith("*로그 분석 결과*"))
        self.assertTrue(any("• scanned 이벤트 (계속)" in chunk for chunk in chunks[1:]))

    def test_needs_barcode_log_fallback_when_required_metadata_is_missing(self) -> None:
        fallback_text = "\n".join(
            [
                "*로그 분석 결과*",
                "• 바코드: `123`",
                "• 날짜: `2026-04-06`",
                "• 매핑 장비: `box-a`",
            ]
        )

        self.assertTrue(
            _needs_barcode_log_fallback(
                "요약만 있는 답변",
                fallback_text,
                "barcode log analysis",
            )
        )
        self.assertFalse(
            _needs_barcode_log_fallback(
                fallback_text,
                fallback_text,
                "barcode log analysis",
            )
        )

    def test_error_summary_marks_pre_recording_stop_as_canceled_failure(self) -> None:
        session_entry = {
            "barcode": "23318551080",
            "deviceName": "MB2-C01118",
            "hospitalName": "분당제일여성병원(성남)",
            "roomName": "3층 C동 초음파실3",
            "date": "2026-04-07",
            "recordingsOnDateCount": 1,
            "detail": {
                "index": 2,
                "startTime": "14:06:53",
                "stopTime": "14:21:30",
                "normalClosed": True,
                "restartDetected": False,
                "terminationStatus": "정상 종료 (`C_STOPSESS` 확인)",
                "videoStatus": "정상 녹화 실패로 판단 (모션 감지 단계에서 종료 스캔, 모션 미감지, 첫 ffmpeg 오류 `14:06:53`, 세션 시작 후 `0초`)",
                "recordingResult": "정상 녹화 실패로 판단 (모션 감지 단계에서 종료 스캔, 모션 미감지, 첫 ffmpeg 오류 `14:06:53`, 세션 시작 후 `0초`)",
                "errorLineCount": 1,
                "errorGroups": [
                    {
                        "component": "FfmpegController",
                        "signature": "MOTION FFmpeg error: ffmpeg exited with code 1: /dev/video0: Device or resource busy",
                        "count": 1,
                        "sampleTime": "14:06:53",
                        "sampleMessage": "/dev/video0: Device or resource busy",
                    }
                ],
                "firstFfmpegError": {
                    "timeLabel": "14:06:53",
                    "message": "MOTION FFmpeg error: ffmpeg exited with code 1: /dev/video0: Device or resource busy",
                    "raw": "/dev/video0: Device or resource busy",
                },
                "preRecordingStopDetected": True,
                "preRecordingStopLabel": "모션 감지 단계에서 종료 스캔",
                "sessionDiagnostic": {
                    "severity": "normal",
                },
            },
        }

        lines = _build_barcode_log_error_session_section(session_entry)
        text = "\n".join(lines)

        self.assertIn("모션 감지 단계에서 종료 스캔돼 녹화 취소로 끝났고", text)
        self.assertIn("본 녹화 시작 전이라 정상 녹화 실패", text)


if __name__ == "__main__":
    unittest.main()
