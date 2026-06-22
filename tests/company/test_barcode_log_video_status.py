from datetime import datetime
import unittest

from boxer_company.routers.barcode_log import (
    _analyze_barcode_log_scan_events,
    _append_session_card,
    _build_log_analysis_record,
    _build_session_card_context,
    _build_session_recording_result_text,
    _extract_motion_events_with_line_no,
    _extract_restart_events_with_line_no,
    _extract_scan_events_with_line_no,
    _fetch_device_os_lifecycle_events_for_sessions,
    _is_normal_video_status,
    _match_recordings_rows_to_sessions,
)
from unittest.mock import patch


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

    def test_post_stop_app_restart_without_upload_marks_zero_byte_suspicion(self) -> None:
        # 세션 종료 스캔 뒤 파일 마무리 전에 앱이 내려가면 0바이트/미완성 파일 가능성을 드러낸다.
        source_lines = [
            "[12:20:29] Scanned : 33682817209",
            "[12:20:32] addRecording(en3xakpqa0ppx56v)",
            "[12:22:13] motion detection passed",
            "[12:22:16] Started recording : en3xakpqa0ppx56v | 33682817209 ||| segment : 0",
            "[12:22:16] Spawned RECORDING ffmpeg with command : ffmpeg /home/mommytalk/AppData/Videos/en3xakpqa0ppx56v.mp4",
            "[12:24:22] Scanned : C_STOPSESS",
            "[12:24:24] Stopping recording...",
            "[12:27:59] SIGINT received App Exiting. code : SIGINT : app cleanup, sending log",
            "[12:28:04] cleanup finished, sending SIGTERM...",
            "[12:30:40] Initializing Mommybox - MB2-C01040",
            "[12:30:45] Checking for available uploads after endpoint connection...",
            "[12:30:45] Check upload available recording",
            "[12:30:45] Uploadable Recording: none",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        session = {
            "start_line_no": 1,
            "start_time_label": "12:20:29",
            "stop_line_no": 6,
            "stop_time_label": "12:24:22",
            "stop_token": "C_STOPSESS",
            "end_line_no": len(source_lines),
        }

        result, _, post_stop_context = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_motion_events=motion_events,
            session_recordings_rows=[],
        )

        self.assertIn("녹화 & 업로드 실패로 판단", result)
        self.assertIn("세션 종료 스캔 후 녹화 파일 마무리·업로드 처리 전 앱 종료/재시작", result)
        self.assertIn("본 mp4 미완성·0바이트 가능성", result)
        self.assertIn("재시작 후 업로드 대상 없음", result)
        self.assertFalse(_is_normal_video_status(result))
        self.assertEqual(post_stop_context["severity"], "high")
        self.assertIn("recording_interrupted_before_upload", post_stop_context["tags"])
        self.assertIn("post_restart_upload_none", post_stop_context["tags"])

        context = _build_session_card_context(
            source_lines,
            session,
            motion_events,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_recordings_rows=[],
        )

        self.assertIn("세션 종료 스캔 후 녹화 파일 마무리·업로드 처리 전 앱 종료/재시작", context["anomalyText"])
        self.assertIn("본 mp4 미완성·0바이트 가능성", context["anomalyText"])
        self.assertIn("재시작 후 업로드 대상 없음", context["anomalyText"])

        card_lines: list[str] = []
        _append_session_card(
            card_lines,
            index=1,
            source_lines=source_lines,
            session=session,
            session_scan_events=scan_events,
            session_motion_events=motion_events,
            session_restart_events=[],
            session_error_lines=[],
            diagnostic_scan_events=scan_events,
            recordings_on_date_count=0,
            session_recordings_rows=[],
        )
        rendered_card = "\n".join(card_lines)

        self.assertIn("• 종료 방식: 앱 종료 신호(SIGINT, 원인은 OS 로그 필요)", rendered_card)
        self.assertIn("• scanned 이벤트: *2건* (앱/상태 이벤트 *4건* 함께 표시)", rendered_card)
        self.assertIn("12:27:59  앱 종료 신호(SIGINT)", rendered_card)
        self.assertIn("12:28:04  앱 종료 완료(SIGTERM)", rendered_card)
        self.assertIn("12:30:40  앱 재시작 감지", rendered_card)
        self.assertIn("12:30:45  업로드 대상 없음", rendered_card)

    def test_os_power_key_shutdown_method_is_displayed_when_journal_lines_exist(self) -> None:
        # OS 로그까지 확보된 분석에서는 앱 SIGINT의 상위 원인인 전원 버튼 종료를 함께 보여준다.
        source_lines = [
            "[12:20:29] Scanned : 33682817209",
            "[12:24:22] Scanned : C_STOPSESS",
            "2026-05-26T12:27:58+0900 mommytalk systemd-logind[678]: Power key pressed.",
            "2026-05-26T12:27:58+0900 mommytalk systemd-logind[678]: Powering Off...",
            "2026-05-26T12:27:58+0900 mommytalk systemd[1]: Stopping PM2 process manager...",
            "[12:27:59] SIGINT received App Exiting. code : SIGINT : app cleanup, sending log",
            "[12:28:04] cleanup finished, sending SIGTERM...",
            "2026-05-26T12:28:05+0900 mommytalk systemd[1]: Reached target Shutdown.",
            "-- Reboot --",
            "2026-05-26T12:30:35+0900 mommytalk kernel: Linux version 4.15.0-147-generic",
            "[12:30:40] Initializing Mommybox - MB2-C01040",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        session = {
            "start_line_no": 1,
            "start_time_label": "12:20:29",
            "stop_line_no": 2,
            "stop_time_label": "12:24:22",
            "stop_token": "C_STOPSESS",
            "end_line_no": len(source_lines),
        }
        card_lines: list[str] = []

        _append_session_card(
            card_lines,
            index=1,
            source_lines=source_lines,
            session=session,
            session_scan_events=scan_events,
            session_motion_events=[],
            session_restart_events=[],
            session_error_lines=[],
            diagnostic_scan_events=scan_events,
            recordings_on_date_count=0,
            session_recordings_rows=[],
        )
        rendered_card = "\n".join(card_lines)

        self.assertIn("• 종료 방식: 전원 버튼 종료 요청(OS 로그) → 앱 종료 신호(SIGINT)", rendered_card)
        self.assertIn("12:27:58  전원 버튼 종료 요청", rendered_card)
        self.assertIn("12:27:58  OS 전원 종료 진행", rendered_card)
        self.assertIn("12:27:58  PM2 앱 종료 진행", rendered_card)
        self.assertIn("12:27:59  앱 종료 신호(SIGINT)", rendered_card)
        self.assertIn("12:28:05  OS 종료 단계", rendered_card)
        self.assertIn("시간미상  장비 재부팅 구간", rendered_card)
        self.assertIn("12:30:35  장비 부팅 시작", rendered_card)

    def test_os_lifecycle_events_are_fetched_only_for_sigint_sessions(self) -> None:
        # 앱 SIGINT가 있는 세션만 장비 OS 로그 조회 대상으로 삼는다.
        source_lines = [
            "[12:20:29] Scanned : 33682817209",
            "[12:24:22] Scanned : C_STOPSESS",
            "[12:27:59] SIGINT received App Exiting. code : SIGINT : app cleanup, sending log",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        sessions = [
            {
                "start_line_no": 1,
                "start_time_label": "12:20:29",
                "stop_line_no": 2,
                "stop_time_label": "12:24:22",
                "stop_token": "C_STOPSESS",
                "end_line_no": len(source_lines),
            }
        ]
        journal_output = "\n".join(
            [
                "2026-05-26T12:27:58+0900 mommytalk systemd-logind[678]: Power key pressed.",
                "2026-05-26T12:27:58+0900 mommytalk systemd-logind[678]: System is powering down.",
                "2026-05-26T12:28:05+0900 mommytalk systemd[1]: Reached target Shutdown.",
                "-- Reboot --",
                "2026-05-26T12:30:35+0900 mommytalk kernel: Linux version 4.15.0-147-generic",
            ]
        )

        class FakeClient:
            def __init__(self) -> None:
                self.closed = False

            def close(self) -> None:
                self.closed = True

        fake_client = FakeClient()
        with (
            patch(
                "boxer_company.routers.barcode_log._connect_device_ssh_for_barcode_log",
                return_value={"ok": True, "client": fake_client},
            ) as connect,
            patch(
                "boxer_company.routers.barcode_log._run_device_ssh_command_for_barcode_log",
                return_value={"ok": True, "stdout": journal_output, "stderr": "", "exitStatus": 0},
            ) as run_command,
        ):
            events_by_session = _fetch_device_os_lifecycle_events_for_sessions(
                device_name="MB2-C01040",
                log_date="2026-05-26",
                source_lines=source_lines,
                sessions=sessions,
                scan_events=scan_events,
            )

        connect.assert_called_once_with("MB2-C01040")
        run_command.assert_called_once()
        command = run_command.call_args.args[1]
        self.assertIn("journalctl", command)
        self.assertIn("2026-05-26 12:24:59", command)
        self.assertIn("2026-05-26 12:34:59", command)
        self.assertTrue(fake_client.closed)
        self.assertEqual(
            [event["event_type"] for event in events_by_session[1]],
            ["power_key_shutdown", "os_powering_off", "os_shutdown", "device_reboot", "device_boot"],
        )

    def test_log_analysis_merges_fetched_os_lifecycle_events_into_session_card(self) -> None:
        # S3 앱 로그에 SIGINT만 있어도 SSH로 얻은 OS 로그가 있으면 종료 방식을 확정 표시한다.
        source_lines = [
            "[12:20:29] Scanned : 33682817209",
            "[12:22:16] Started recording : en3xakpqa0ppx56v | 33682817209 ||| segment : 0",
            "[12:24:22] Scanned : C_STOPSESS",
            "[12:27:59] SIGINT received App Exiting. code : SIGINT : app cleanup, sending log",
            "[12:28:04] cleanup finished, sending SIGTERM...",
        ]
        os_events = {
            1: [
                {
                    "line_no": 1_000_001,
                    "time_label": "12:27:58",
                    "event_type": "power_key_shutdown",
                    "label": "전원 버튼 종료 요청",
                    "raw_line": "mommytalk systemd-logind[678]: Power key pressed.",
                },
            ]
        }

        with (
            patch(
                "boxer_company.routers.barcode_log._fetch_s3_device_log_lines",
                return_value={
                    "found": True,
                    "lines": source_lines,
                    "key": "MB2-C01040/log-2026-05-26.log",
                    "content_length": 1234,
                },
            ),
            patch(
                "boxer_company.routers.barcode_log._fetch_device_os_lifecycle_events_for_sessions",
                return_value=os_events,
            ) as fetch_os_events,
        ):
            result_text, _ = _analyze_barcode_log_scan_events(
                None,
                "33682817209",
                "2026-05-26",
                recordings_context=None,
                device_contexts=[
                    {
                        "deviceName": "MB2-C01040",
                        "hospitalName": "미즈맘의원(당진)",
                        "roomName": "1진료실",
                    }
                ],
            )

        fetch_os_events.assert_called_once()
        self.assertIn("• 종료 방식: 전원 버튼 종료 요청(OS 로그) → 앱 종료 신호(SIGINT)", result_text)
        self.assertIn("12:27:58  전원 버튼 종료 요청", result_text)
        self.assertIn("12:27:59  앱 종료 신호(SIGINT)", result_text)

    def test_initializing_mommybox_is_treated_as_restart_event(self) -> None:
        restart_events = _extract_restart_events_with_line_no(
            [
                "[12:30:40] Initializing Mommybox - MB2-C01040",
                "[12:30:40] App Version : 2.11.293",
            ]
        )

        self.assertEqual(len(restart_events), 1)
        self.assertEqual(restart_events[0]["time_label"], "12:30:40")

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

    def test_motion_start_without_trigger_or_db_row_is_canceled(self) -> None:
        # 현장 로그에서 motion_start만 남고 motion_stop 로그가 누락된 케이스를 재현한다.
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:37:59] Starting motion detection <- cmd_processBarcodeScan. Current state: STANDBY",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        session = _build_session()

        self.assertEqual(motion_events[0]["event_type"], "motion_start")

        result, _, _ = _build_session_recording_result_text(
            source_lines,
            session,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_motion_events=motion_events,
            session_recordings_rows=[],
        )

        self.assertIn("정상 녹화 실패로 판단", result)
        self.assertIn("모션 감지 단계에서 종료 스캔", result)

        context = _build_session_card_context(
            source_lines,
            session,
            motion_events,
            [],
            [],
            scan_events,
            recordings_on_date_count=0,
            session_recordings_rows=[],
        )

        self.assertTrue(context["preRecordingStopDetected"])
        self.assertEqual(context["terminationStatus"], "녹화 취소 (모션 감지 단계에서 `C_STOPSESS` 확인)")

    def test_motion_start_without_trigger_but_positive_db_row_stays_normal_close(self) -> None:
        # 세션 DB 영상이 있으면 추정 취소로 덮어쓰지 않고 추가 확인 대상으로 남겨야 한다.
        source_lines = [
            "[09:37:58] Scanned : 87752940438",
            "[09:37:59] Starting motion detection <- cmd_processBarcodeScan. Current state: STANDBY",
            "[09:38:02] Scanned : C_STOPSESS",
        ]
        scan_events = _extract_scan_events_with_line_no(source_lines)
        motion_events = _extract_motion_events_with_line_no(source_lines)
        session = _build_session()
        session_recordings_rows = [
            {
                "createdAt": datetime(2026, 4, 7, 0, 38, 0),
                "recordedAt": datetime(2026, 4, 7, 0, 38, 2),
                "videoLength": 622,
            }
        ]

        context = _build_session_card_context(
            source_lines,
            session,
            motion_events,
            [],
            [],
            scan_events,
            recordings_on_date_count=1,
            session_recordings_rows=session_recordings_rows,
        )

        self.assertFalse(context["preRecordingStopDetected"])
        self.assertEqual(context["terminationStatus"], "정상 종료 (`C_STOPSESS` 확인)")

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
