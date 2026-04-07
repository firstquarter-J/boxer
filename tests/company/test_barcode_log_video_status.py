from datetime import datetime
import unittest

from boxer_company.routers.barcode_log import (
    _append_session_card,
    _build_log_analysis_record,
    _build_session_card_context,
    _build_session_recording_result_text,
    _extract_motion_events_with_line_no,
    _extract_scan_events_with_line_no,
    _is_normal_video_status,
    _match_recordings_rows_to_sessions,
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

        self.assertEqual(result, "영상 생성 근거 없음 (세션 기준 DB 영상 기록 없음)")
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

    def test_add_recording_only_before_motion_stop_is_canceled_failure(self) -> None:
        source_lines = [
            "[14:02:35] Scanned : 23318551080",
            "[14:02:37] motion detection process initiated successfully",
            "[14:02:38] addRecording(abc123)",
            "[14:06:51] Scanned : C_STOPSESS",
            "[14:06:52] Stopping motion detection. Motion detected: false, Error: false",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        session = {
            "start_line_no": 1,
            "start_time_label": "14:02:35",
            "stop_line_no": 4,
            "stop_time_label": "14:06:51",
            "stop_token": "C_STOPSESS",
            "end_line_no": 5,
        }

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_motion_events=motion_events,
        )

        self.assertIn("정상 녹화 실패로 판단", result)
        self.assertIn("모션 감지 단계에서 종료 스캔", result)
        self.assertNotIn("녹화 파일 생성 로그 확인", result)

        context = _build_session_card_context(
            source_lines,
            session,
            motion_events,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
        )

        self.assertEqual(context["terminationStatus"], "녹화 취소 (모션 감지 단계에서 `C_STOPSESS` 확인)")
        self.assertTrue(context["preRecordingStopDetected"])

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

        self.assertFalse(_is_normal_video_status(result))
        self.assertEqual(result, "영상 생성 근거 없음 (세션 기준 DB 영상 기록 없음)")

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
        self.assertIn("• DB 영상 기록(세션 기준): `0개`", lines)
        self.assertTrue(any(line.startswith("• 영상 상태: ") for line in lines))

    def test_matched_positive_session_db_row_keeps_normal_video_status(self) -> None:
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:38:00] session still open",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        session = _build_session()
        session_recordings_rows = [
            {
                "createdAt": datetime(2026, 4, 7, 0, 38, 0),
                "recordedAt": datetime(2026, 4, 7, 0, 38, 2),
                "videoLength": 622,
            }
        ]

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=1,
            session_motion_events=[],
            session_recordings_rows=session_recordings_rows,
        )

        self.assertTrue(_is_normal_video_status(result))
        self.assertEqual(result, "정상 녹화로 판단 (세션 기준 DB 영상 기록 `1개` 확인)")

    def test_zero_length_session_db_row_is_not_normal(self) -> None:
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:38:00] session still open",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        session = _build_session()
        session_recordings_rows = [
            {
                "createdAt": datetime(2026, 4, 7, 0, 38, 0),
                "recordedAt": datetime(2026, 4, 7, 0, 38, 2),
                "videoLength": 0,
            }
        ]

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=1,
            session_motion_events=[],
            session_recordings_rows=session_recordings_rows,
        )

        self.assertFalse(_is_normal_video_status(result))
        self.assertIn("추가 확인 필요", result)
        self.assertIn("영상 길이 `0초", result)

    def test_recordings_rows_are_matched_to_the_closest_session(self) -> None:
        source_lines = [
            "[14:02:35] Scanned : 23318551080",
            "[14:02:37] motion detection process initiated successfully",
            "[14:06:51] Scanned : C_STOPSESS",
            "[14:06:52] Stopping motion detection. Motion detected: false, Error: false",
            "[14:06:53] Scanned : 23318551080",
            "[14:06:54] motion detection process initiated successfully",
            "[14:21:30] Scanned : C_STOPSESS",
            "[14:21:30] Stopping motion detection. Motion detected: false, Error: false",
            "[14:22:12] Scanned : 23318551080",
            "[14:22:14] motion detection process initiated successfully",
            "[14:22:40] motion detection passed",
            "[14:33:03] Scanned : C_STOPSESS",
        ]
        sessions = [
            {
                "start_line_no": 1,
                "start_time_label": "14:02:35",
                "stop_line_no": 3,
                "stop_time_label": "14:06:51",
                "stop_token": "C_STOPSESS",
                "end_line_no": 4,
            },
            {
                "start_line_no": 5,
                "start_time_label": "14:06:53",
                "stop_line_no": 7,
                "stop_time_label": "14:21:30",
                "stop_token": "C_STOPSESS",
                "end_line_no": 8,
            },
            {
                "start_line_no": 9,
                "start_time_label": "14:22:12",
                "stop_line_no": 12,
                "stop_time_label": "14:33:03",
                "stop_token": "C_STOPSESS",
                "end_line_no": 12,
            },
        ]
        recordings_rows = [
            {
                "createdAt": datetime(2026, 4, 7, 5, 6, 53),
                "recordedAt": datetime(2026, 4, 7, 5, 21, 30),
                "videoLength": 0,
            },
            {
                "createdAt": datetime(2026, 4, 7, 5, 22, 40),
                "recordedAt": datetime(2026, 4, 7, 5, 33, 3),
                "videoLength": 622,
            },
        ]

        matched_rows = _match_recordings_rows_to_sessions(
            source_lines,
            sessions,
            recordings_rows,
        )

        self.assertEqual(len(matched_rows[1]), 0)
        self.assertEqual(len(matched_rows[2]), 1)
        self.assertEqual(len(matched_rows[3]), 1)

    def test_pre_recording_stop_is_not_counted_as_normal_close(self) -> None:
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

        record = _build_log_analysis_record(
            source_lines=source_lines,
            device_name="MB2-C01118",
            hospital_name="분당제일여성병원(성남)",
            room_name="3층 C동 초음파실3",
            log_key="MB2-C01118/log-2026-04-07.log",
            log_date="2026-04-07",
            line_count=len(source_lines),
            sessions=[session],
            session_scans=scan_events,
            all_scan_events=scan_events,
            session_motions=motion_events,
            session_restarts=[],
            session_error_lines=[],
            recordings_on_date_count=0,
            recordings_on_date_rows=[],
            recordings_on_date_statuses=[],
        )

        self.assertEqual(record["sessions"]["normalCount"], 0)
        self.assertEqual(record["sessions"]["abnormalCount"], 1)
        self.assertEqual(record["sessions"]["canceledCount"], 1)
        self.assertFalse(record["sessionDetails"][0]["normalClosed"])
        self.assertEqual(record["sessionDetails"][0]["terminationKind"], "pre_recording_stop")
        self.assertEqual(
            record["sessionDetails"][0]["terminationStatus"],
            "녹화 취소 (모션 감지 단계에서 `C_STOPSESS` 확인)",
        )


if __name__ == "__main__":
    unittest.main()
