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

    def test_free_barcode_blocked_scan_is_reported_as_scan_only(self) -> None:
        # 무료 바코드 차단은 Scanned 로그가 있어도 녹화 세션으로 만들면 안 된다.
        source_lines = [
            "2026-06-22_16:34:26.585 [app] info: Scanned : 43078716167",
            "2026-06-22_16:34:26.595 [BarcodeManager] info: Free barcode detected: 43078716167. Blocking recording.",
            "2026-06-22_16:34:26.596 [app] info: Barcode validation result: barcode=43078716167, result=FREE",
            "2026-06-22_16:34:26.596 [app] info: Free Barcode: 43078716167",
        ]

        with patch(
            "boxer_company.routers.barcode_log._fetch_s3_device_log_lines",
            return_value={
                "found": True,
                "lines": source_lines,
                "key": "MB2-C00375/log-2026-06-22.log",
                "content_length": 1234,
            },
        ):
            result_text, payload = _analyze_barcode_log_scan_events(
                None,
                "43078716167",
                "2026-06-22",
                recordings_context=None,
                device_contexts=[
                    {
                        "deviceName": "MB2-C00375",
                        "hospitalName": "에덴메디여성병원(수원)",
                        "roomName": "입체초음파",
                    }
                ],
            )

        self.assertIn("바코드 스캔은 확인됐지만 녹화 세션은 생성되지 않았어", result_text)
        self.assertIn("무료 바코드로 검증되어 장비가 녹화를 차단했어", result_text)
        self.assertIn("result=FREE", result_text)
        self.assertEqual(payload["summary"]["sessionCount"], 0)
        self.assertEqual(payload["summary"]["scanEventCount"], 1)
        self.assertTrue(payload["records"][0]["scanOnly"])

    def test_scan_only_fallback_reports_outside_mapped_scope(self) -> None:
        # 과거 recordings 매핑 병원 밖에서 스캔된 무료 바코드는 전체 일자 로그 fallback으로 보강한다.
        fallback_record = {
            "scanOnly": True,
            "deviceName": "MB2-C00375",
            "hospitalName": "에덴메디여성병원(수원)",
            "roomName": "입체초음파",
            "date": "2026-06-22",
            "logKey": "MB2-C00375/log-2026-06-22.log",
            "lineCount": 7000,
            "sessions": {"sessionCount": 0, "normalCount": 0, "abnormalCount": 0},
            "scanEventCount": 2,
            "displayedScanEventCount": 2,
            "scanEvents": [
                {
                    "lineNo": 1,
                    "timeLabel": "16:34:26",
                    "token": "43078716167",
                    "rawLine": "[app] info: Scanned : 43078716167",
                },
                {
                    "lineNo": 5,
                    "timeLabel": "16:34:28",
                    "token": "43078716167",
                    "rawLine": "[app] info: Scanned : 43078716167",
                },
            ],
            "scanOnlyReason": "무료 바코드로 검증되어 장비가 녹화를 차단했어 (`result=FREE`, `Blocking recording`)",
            "blockingContextCount": 1,
            "blockingContexts": [
                {
                    "result": "FREE",
                    "rawLines": [
                        {
                            "timeLabel": "16:34:26",
                            "rawLine": "[BarcodeManager] info: Free barcode detected: 43078716167. Blocking recording.",
                        }
                    ],
                }
            ],
            "restartEventCount": 0,
            "errorLineCount": 0,
            "errorLines": [],
            "errorGroups": [],
            "sessionDetails": [],
            "sessionDiagnostics": [],
        }

        with (
            patch(
                "boxer_company.routers.barcode_log._fetch_s3_device_log_lines",
                return_value={
                    "found": True,
                    "lines": ["[09:00:00] Scanned : 11111111111"],
                    "key": "MB2-C00830/log-2026-06-22.log",
                    "content_length": 1234,
                },
            ),
            patch(
                "boxer_company.routers.barcode_log._find_scan_only_records_by_date_log_search",
                return_value=([fallback_record], {"searchedLogCount": 384, "candidateLogCount": 1878}),
            ),
            patch("boxer_company.routers.barcode_log._lookup_blocking_special_barcode_context", return_value=None),
            patch("boxer_company.routers.barcode_log._SCAN_ONLY_GLOBAL_FALLBACK_ENABLED", True),
        ):
            result_text, payload = _analyze_barcode_log_scan_events(
                None,
                "43078716167",
                "2026-06-22",
                recordings_context=None,
                device_contexts=[
                    {
                        "deviceName": "MB2-C00830",
                        "hospitalName": "아이오라여성의원(수원)",
                        "roomName": "3진료실",
                    }
                ],
            )

        self.assertIn("전체 일자 로그 추가 검색", result_text)
        self.assertIn("MB2-C00375", result_text)
        self.assertIn("무료 바코드로 검증되어 장비가 녹화를 차단했어", result_text)
        self.assertEqual(payload["summary"]["scanEventCount"], 2)
        self.assertTrue(payload["records"][0]["scanOnly"])

    def test_blocking_special_barcode_context_short_circuits_global_fallback(self) -> None:
        # 전체 S3 검색 전에 MDA 제한 목록으로 FREE 차단 원인을 빠르게 설명한다.
        with (
            patch(
                "boxer_company.routers.barcode_log._fetch_s3_device_log_lines",
                return_value={
                    "found": True,
                    "lines": ["[09:00:00] Scanned : 11111111111"],
                    "key": "MB2-C00830/log-2026-06-22.log",
                    "content_length": 1234,
                },
            ),
            patch(
                "boxer_company.routers.barcode_log._lookup_blocking_special_barcode_context",
                return_value={
                    "barcode": "43078716167",
                    "type": "FREE",
                    "typeLabel": "무료 바코드",
                    "reason": "아이오라여성의원(수원)",
                },
            ),
            patch(
                "boxer_company.routers.barcode_log._find_scan_only_records_by_date_log_search",
            ) as fallback_search,
        ):
            result_text, payload = _analyze_barcode_log_scan_events(
                None,
                "43078716167",
                "2026-06-22",
                recordings_context=None,
                device_contexts=[
                    {
                        "deviceName": "MB2-C00830",
                        "hospitalName": "아이오라여성의원(수원)",
                        "roomName": "3진료실",
                    }
                ],
            )

        fallback_search.assert_not_called()
        self.assertIn("운영 제한 목록에서 `무료 바코드`(`FREE`)로 등록", result_text)
        self.assertIn("유효성 검사가 켜진 장비에서는 스캔 직후 녹화가 시작되지 않고 차단", result_text)
        self.assertTrue(payload["records"][0]["validationBlock"])

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

        self.assertIn("• 종료 방식: 앱 종료 신호(SIGINT, 앱 크래시 아님, 원인은 OS 로그 필요)", rendered_card)
        self.assertIn("• scanned 이벤트: *2건* (종료 원인 *1건* 함께 표시)", rendered_card)
        self.assertIn("12:27:59  종료 원인: 앱 종료 신호(SIGINT, 앱 크래시 아님)", rendered_card)
        self.assertIn("OS 종료 방식 확인 필요", rendered_card)
        self.assertNotIn("12:28:04  앱 종료 완료(SIGTERM)", rendered_card)
        self.assertNotIn("12:30:40  앱 재시작 감지", rendered_card)
        self.assertNotIn("12:30:45  업로드 대상 없음", rendered_card)

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

        self.assertIn("• 종료 방식: 전원 버튼 종료(물리적 재부팅, 앱 크래시 아님)", rendered_card)
        self.assertIn("• scanned 이벤트: *2건* (종료 원인 *1건* 함께 표시)", rendered_card)
        self.assertIn("12:27:58  종료 원인: 전원 버튼 종료(물리적 재부팅, 앱 크래시 아님)", rendered_card)
        self.assertNotIn("OS 전원 종료 진행", rendered_card)
        self.assertNotIn("PM2 앱 종료 진행", rendered_card)
        self.assertNotIn("앱 종료 신호(SIGINT) |", rendered_card)
        self.assertNotIn("OS 종료 단계", rendered_card)
        self.assertNotIn("장비 재부팅 구간", rendered_card)
        self.assertNotIn("장비 부팅 시작", rendered_card)

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
        self.assertIn("• 종료 방식: 전원 버튼 종료(물리적 재부팅, 앱 크래시 아님)", result_text)
        self.assertIn("12:27:58  종료 원인: 전원 버튼 종료(물리적 재부팅, 앱 크래시 아님)", result_text)
        self.assertNotIn("12:27:59  앱 종료 신호(SIGINT)", result_text)

    def test_app_crash_lifecycle_is_displayed_as_single_shutdown_cause(self) -> None:
        # 앱 비정상 종료 단서가 있으면 상세 로그 대신 크래시 여부만 한 줄로 남긴다.
        source_lines = [
            "[12:20:29] Scanned : 33682817209",
            "[12:24:22] Scanned : C_STOPSESS",
            "[12:27:59] UnhandledException: Cannot read property 'emit' of undefined",
            "[12:28:01] Initializing Mommybox - MB2-C01040",
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

        self.assertIn("• 종료 방식: 앱 크래시/비정상 종료", rendered_card)
        self.assertIn("12:27:59  종료 원인: 앱 크래시/비정상 종료", rendered_card)
        self.assertNotIn("12:28:01  앱 재시작 감지", rendered_card)

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
