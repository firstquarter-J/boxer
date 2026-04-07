import unittest

from boxer_company.routers.barcode_log import (
    _append_session_card,
    _build_session_card_context,
    _build_session_recording_result_text,
    _extract_motion_events_with_line_no,
    _extract_scan_events_with_line_no,
    _is_normal_video_status,
)


def _build_session() -> dict[str, object]:
    return {
        "start_line_no": 1,
        "start_time_label": "09:37:58",
        "stop_line_no": 3,
        "stop_time_label": "09:38:02",
        "stop_token": "C_STOPSESS",
        "end_line_no": 3,
    }


class BarcodeLogVideoStatusTests(unittest.TestCase):
    def test_normal_stop_without_positive_evidence_is_not_normal(self) -> None:
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:38:00] session still open",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        session = _build_session()

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_motion_events=[],
        )

        self.assertEqual(result, "영상 생성 근거 없음 (날짜 기준 DB 영상 기록 없음)")
        self.assertFalse(_is_normal_video_status(result))

        context = _build_session_card_context(
            source_lines,
            session,
            [],
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
        )

        self.assertEqual(context["terminationStatus"], "정상 종료 (`C_STOPSESS` 확인)")
        self.assertEqual(context["videoStatus"], result)

    def test_motion_success_without_db_row_is_not_normal(self) -> None:
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:37:59] motion detection passed",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        session = _build_session()

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_motion_events=motion_events,
        )

        self.assertIn("모션 감지 성공까지 확인", result)
        self.assertFalse(_is_normal_video_status(result))

    def test_recording_start_log_without_db_row_is_not_normal(self) -> None:
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:37:59] started recording : abc123",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        session = _build_session()

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_motion_events=[],
        )

        self.assertIn("영상 생성 확인, 업로드 확인 안 됨", result)
        self.assertIn("실녹화 시작 로그 확인", result)
        self.assertFalse(_is_normal_video_status(result))

    def test_stop_scan_during_motion_stage_is_failure_even_with_db_row(self) -> None:
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:37:59] motion detection process initiated successfully",
            "[09:38:02] Scanned : C_STOPSESS",
            "[09:38:02] Stopping motion detection. Motion detected: false, Error: false",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        session = {
            "start_line_no": 1,
            "start_time_label": "09:37:58",
            "stop_line_no": 3,
            "stop_time_label": "09:38:02",
            "stop_token": "C_STOPSESS",
            "end_line_no": 4,
        }

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=1,
            session_motion_events=motion_events,
        )

        self.assertIn("정상 녹화 실패로 판단", result)
        self.assertIn("모션 감지 단계에서 종료 스캔", result)
        self.assertFalse(_is_normal_video_status(result))

        context = _build_session_card_context(
            source_lines,
            session,
            motion_events,
            [],
            [],
            scan_events,
            recordings_on_date_count=1,
        )

        self.assertTrue(context["preRecordingStopDetected"])
        self.assertEqual(context["preRecordingStopLabel"], "모션 감지 단계에서 종료 스캔")
        self.assertEqual(context["terminationStatus"], "녹화 취소 (모션 감지 단계에서 `C_STOPSESS` 확인)")
        self.assertIn("모션 감지 단계에서 종료 스캔", context["anomalyText"])

    def test_db_row_keeps_normal_video_status_and_card_shows_video_label(self) -> None:
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:38:00] session still open",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        session = _build_session()

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=1,
            session_motion_events=[],
        )

        self.assertTrue(_is_normal_video_status(result))

        lines: list[str] = []
        _append_session_card(
            lines,
            index=1,
            source_lines=source_lines,
            session=session,
            session_scan_events=scan_events,
            session_motion_events=[],
            session_restart_events=[],
            session_error_lines=[],
            diagnostic_scan_events=scan_events,
            recordings_on_date_count=1,
        )

        self.assertIn("• 종료 상태: 정상 종료 (`C_STOPSESS` 확인)", lines)
        self.assertTrue(any(line.startswith("• 영상 상태: ") for line in lines))


if __name__ == "__main__":
    unittest.main()
