import logging
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack import daily_device_round_reporter
from boxer_company_adapter_slack import device_notification_alert_reporter as reporter
from boxer_company_adapter_slack import (
    device_health_monitor_reporter as health_reporter,
)


class _FakeCursor:
    def __init__(self, *, latest_id: int, rows: list[dict]) -> None:
        self.latest_id = latest_id
        self.rows = rows
        self.execute_calls: list[tuple[str, tuple | None]] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None

    def execute(self, sql: str, params: tuple | None = None) -> None:
        self.execute_calls.append((sql, params))

    def fetchone(self) -> dict[str, int]:
        return {"latestId": self.latest_id}

    def fetchall(self) -> list[dict]:
        return self.rows


class _FakeConnection:
    def __init__(self, cursor: _FakeCursor) -> None:
        self._cursor = cursor
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self._cursor

    def close(self) -> None:
        self.closed = True


def _captureboard_event(notification_id: int = 12) -> dict:
    return {
        "notificationId": notification_id,
        "deviceSeq": 992,
        "deviceName": "MB2-C00992",
        "code": "captureboard_connection_error",
        "message": "녹화 중 캡쳐보드 연결에 문제가 발생하여 녹화가 중단되었습니다.",
        "occurredAt": "2026-07-09T03:34:31+00:00",
        "hospitalSeq": 69,
        "hospitalName": "뉴서울여성의원(인천)",
        "hospitalTelephone": "032-123-4567",
        "hospitalDeviceAlertPhone": "010-1234-5678",
        "hospitalRoomSeq": 1,
        "roomName": "1진료실",
    }


def _recording_stall_event(
    notification_id: int,
    *,
    duration_seconds: int,
    current_size: int,
    occurred_at: str,
    growth_rate: float = 0,
    current_status: str = "recording",
    file_type: str = "",
) -> dict:
    details = {
        "currentSize": current_size,
        "growthRate": growth_rate,
        "expectedMinGrowth": 145984,
        "consecutiveFailures": duration_seconds // 5,
        "durationSeconds": duration_seconds,
        "severity": "critical",
        "currentStatus": current_status,
    }
    if file_type:
        details["fileType"] = file_type
    return {
        **_captureboard_event(notification_id),
        "code": "recording_critically_stalled",
        "message": (
            "녹화 중 심각한 파일 증가 속도 이상 "
            f"({duration_seconds}초간 지속): {growth_rate / 1024:.2f} KB/sec"
        ),
        "details": details,
        "occurredAt": occurred_at,
    }


def _segmented_recordings_merge_error_event(notification_id: int = 15) -> dict:
    return {
        **_captureboard_event(notification_id),
        "code": "segmented_recordings_merge_error",
        "message": "분할된 녹화 파일 병합 중 오류가 발생했습니다",
        "fileId": "recording-20260709-123431",
        "details": {
            "error": "ffmpeg exited with code 1",
            "targetPath": "/home/pi/AppData/Videos/recording.mp4",
            "segmentCount": 3,
            "listPath": "/tmp/recording-merge-list.txt",
        },
    }


class DeviceNotificationAlertReporterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.logger = logging.getLogger("test.device_notification_alert_reporter")
        self.now = datetime(2026, 7, 13, 10, 0, tzinfo=ZoneInfo("Asia/Seoul"))
        # 알림 테스트가 로컬 자격증명으로 실제 운영 시트를 건드리지 않게 기록 경계를 기본 차단해.
        sheet_patcher = patch.object(
            reporter,
            "_append_device_health_sheet_alerts",
            return_value=None,
        )
        self.append_sheet_mock = sheet_patcher.start()
        self.addCleanup(sheet_patcher.stop)
        sms_outbox_patcher = patch.object(
            reporter,
            "remember_sms_delivery_sheet_record",
            return_value=False,
        )
        self.remember_sms_delivery_mock = sms_outbox_patcher.start()
        self.addCleanup(sms_outbox_patcher.stop)
        # 상태 동기화 테스트만 명시적으로 값을 바꾸고, 나머지는 빈 Sheet snapshot으로 격리해.
        load_sheet_incidents_patcher = patch.object(
            reporter,
            "_load_device_health_sheet_captureboard_incidents",
            return_value={},
        )
        self.load_sheet_incidents_mock = load_sheet_incidents_patcher.start()
        self.addCleanup(load_sheet_incidents_patcher.stop)

    def _settings_patches(self):
        return (
            patch.object(reporter.cs, "DEVICE_NOTIFICATION_ALERT_ENABLED", True),
            patch.object(reporter.s, "DB_QUERY_ENABLED", True),
            patch.object(
                reporter.cs,
                "DEVICE_NOTIFICATION_ALERT_CHANNEL_ID",
                "C094UC05PQW",
            ),
        )

    def test_first_run_initializes_latest_cursor_without_replaying_history(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            enabled_patch, db_patch, channel_patch = self._settings_patches()
            with (
                enabled_patch,
                db_patch,
                channel_patch,
                patch.object(
                    reporter,
                    "_load_latest_device_notification_id",
                    return_value=1200,
                ),
                patch.object(reporter, "_load_device_notification_batch") as batch_mock,
                patch.object(
                    reporter, "_post_daily_device_round_abnormal_alert"
                ) as post_mock,
            ):
                sent = reporter._run_device_notification_alert_once(
                    object(),
                    self.logger,
                    now=self.now,
                    state_path=state_path,
                )

            state = reporter._load_device_notification_alert_state(state_path)
            self.assertFalse(sent)
            self.assertTrue(state["initialized"])
            self.assertEqual(state["lastSeenId"], 1200)
            self.assertEqual(state["pendingEvents"], [])
            batch_mock.assert_not_called()
            post_mock.assert_not_called()

    def test_new_captureboard_event_is_queued_before_slack_and_then_marked_sent(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            reporter._save_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 10,
                    "pendingEvents": [],
                },
                state_path,
            )
            event = _captureboard_event()
            enabled_patch, db_patch, channel_patch = self._settings_patches()
            with (
                enabled_patch,
                db_patch,
                channel_patch,
                patch.object(
                    reporter,
                    "_load_device_notification_batch",
                    return_value=(12, [event]),
                ),
                patch.object(
                    reporter,
                    "_post_daily_device_round_abnormal_alert",
                    return_value={
                        "channelId": "C094UC05PQW",
                        "messageTs": "1000.001",
                        "permalink": "https://example.com/alert",
                    },
                ) as post_mock,
            ):
                sent = reporter._run_device_notification_alert_once(
                    object(),
                    self.logger,
                    now=self.now,
                    state_path=state_path,
                )

            state = reporter._load_device_notification_alert_state(state_path)
            self.assertTrue(sent)
            self.assertEqual(state["lastSeenId"], 12)
            self.assertEqual(state["pendingEvents"], [])
            self.assertEqual(state["lastSentNotificationId"], 12)
            self.assertEqual(
                state["recentCaptureboardAlerts"]["MB2-C00992"]["notificationId"],
                12,
            )
            alert_summary = post_mock.call_args.args[1]
            device_result = alert_summary["deviceResults"][0]
            self.assertEqual(device_result["deviceName"], "MB2-C00992")
            self.assertEqual(device_result["alertCategory"], "video_signal")
            self.assertEqual(device_result["componentLabels"]["captureboard"], "이상")
            self.assertIn("2026-07-09 12:34:31 KST", device_result["priorityReason"])
            self.assertNotIn("`2026-07-09", device_result["priorityReason"])
            self.assertEqual(post_mock.call_args.kwargs["channel_id"], "C094UC05PQW")
            self.assertTrue(post_mock.call_args.kwargs["include_blocks"])
            self.assertFalse(post_mock.call_args.kwargs["include_actions"])
            self.append_sheet_mock.assert_called_once()
            sheet_items = self.append_sheet_mock.call_args.args[0]
            self.assertEqual(sheet_items[0]["device"], "MB2-C00992")
            self.assertEqual(sheet_items[0]["problemComponents"], ["캡처보드"])
            self.assertEqual(
                self.append_sheet_mock.call_args.kwargs["detected_at"],
                datetime(2026, 7, 9, 3, 34, 31, tzinfo=timezone.utc),
            )
            self.assertEqual(
                self.append_sheet_mock.call_args.kwargs["slack_permalink"],
                "https://example.com/alert",
            )

    def test_slack_failure_keeps_pending_event_and_retries_before_new_query(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            reporter._save_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 10,
                    "pendingEvents": [],
                },
                state_path,
            )
            event = _captureboard_event()
            attempted_statuses: list[str] = []
            attempted_delivery_statuses: list[str] = []

            def _send_auto_sms(*_args, **_kwargs):
                claimed_state = reporter._load_device_notification_alert_state(
                    state_path
                )
                attempted_statuses.append(
                    claimed_state["pendingEvents"][0]["autoSms"]["status"]
                )
                attempted_delivery_statuses.append(
                    claimed_state["pendingEvents"][0]["autoSms"][
                        "smsDeliveryStatus"
                    ]
                )
                return {
                    "status": "sent",
                    "ok": True,
                    "smsStatusText": "문자 자동발송 완료",
                    "smsContactActionEnabled": False,
                    "smsPhoneNumber": "01012345678",
                    "smsMessage": "캡처보드 연결 확인 안내",
                    "smsTemplateId": "captureboard_disconnected",
                    "smsProvider": "solapi",
                    "smsGroupId": "G4V-TRACK",
                    "smsMessageId": "M4V-TRACK",
                    "smsDeliveryStatus": "accepted",
                    "smsAcceptedAt": self.now.isoformat(),
                }

            auto_sms_sender = Mock(side_effect=_send_auto_sms)
            enabled_patch, db_patch, channel_patch = self._settings_patches()
            with (
                enabled_patch,
                db_patch,
                channel_patch,
                patch.object(
                    reporter,
                    "_load_device_notification_batch",
                    return_value=(12, [event]),
                ),
                patch.object(
                    reporter,
                    "_post_daily_device_round_abnormal_alert",
                    return_value=None,
                ),
            ):
                first_sent = reporter._run_device_notification_alert_once(
                    object(),
                    self.logger,
                    now=self.now,
                    state_path=state_path,
                    auto_sms_sender=auto_sms_sender,
                )

            failed_state = reporter._load_device_notification_alert_state(state_path)
            self.assertFalse(first_sent)
            self.assertEqual(failed_state["lastSeenId"], 12)
            self.assertEqual(
                [item["notificationId"] for item in failed_state["pendingEvents"]],
                [12],
            )
            self.assertEqual(
                failed_state["pendingEvents"][0]["autoSms"]["status"],
                "sent",
            )
            self.assertEqual(
                failed_state["pendingEvents"][0]["autoSms"]["smsGroupId"],
                "G4V-TRACK",
            )
            self.assertEqual(
                failed_state["pendingEvents"][0]["autoSms"]["smsAcceptedAt"],
                self.now.isoformat(),
            )

            enabled_patch, db_patch, channel_patch = self._settings_patches()
            with (
                enabled_patch,
                db_patch,
                channel_patch,
                patch.object(
                    reporter,
                    "_load_device_notification_batch",
                    return_value=(12, []),
                ) as batch_mock,
                patch.object(
                    reporter,
                    "_post_daily_device_round_abnormal_alert",
                    return_value={
                        "channelId": "C094UC05PQW",
                        "messageTs": "1000.002",
                        "permalink": "https://example.com/retried-alert",
                    },
                ) as retried_post_mock,
            ):
                retried = reporter._run_device_notification_alert_once(
                    object(),
                    self.logger,
                    now=self.now,
                    state_path=state_path,
                    auto_sms_sender=auto_sms_sender,
                )

            retried_state = reporter._load_device_notification_alert_state(state_path)
            self.assertTrue(retried)
            self.assertEqual(retried_state["pendingEvents"], [])
            self.assertEqual(retried_state["lastSentNotificationId"], 12)
            auto_sms_sender.assert_called_once()
            self.assertEqual(attempted_statuses, ["attempting"])
            self.assertEqual(attempted_delivery_statuses, ["confirm_required"])
            retried_result = retried_post_mock.call_args.args[1]["deviceResults"][0]
            self.assertEqual(retried_result["smsStatusText"], "문자 자동발송 완료")
            self.assertEqual(
                retried_result["smsTemplateId"],
                "captureboard_disconnected",
            )
            self.assertEqual(retried_result["smsGroupId"], "G4V-TRACK")
            self.assertEqual(retried_result["smsDeliveryStatus"], "accepted")
            self.assertEqual(retried_result["smsAcceptedAt"], self.now.isoformat())
            batch_mock.assert_called_once_with(12)
            # Slack 실패 시점에는 시트를 쓰지 않고, 재시도 성공 후 딱 한 번만 기록해.
            self.append_sheet_mock.assert_called_once()
            self.assertEqual(
                self.append_sheet_mock.call_args.kwargs["slack_permalink"],
                "https://example.com/retried-alert",
            )
            retried_sheet_item = self.append_sheet_mock.call_args.args[0][0]
            self.assertEqual(retried_sheet_item["smsGroupId"], "G4V-TRACK")
            self.assertEqual(retried_sheet_item["smsDeliveryStatus"], "accepted")
            self.assertEqual(
                retried_sheet_item["smsAcceptedAt"],
                self.now.isoformat(),
            )

    def test_sheet_failure_does_not_undo_successful_slack_alert(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            event = _captureboard_event()
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": event["notificationId"],
                    "pendingEvents": [event],
                }
            )
            self.append_sheet_mock.side_effect = RuntimeError("Sheets down")

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.020",
                    "permalink": "https://example.com/sheet-failure",
                },
            ) as post_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 1)
            self.assertEqual(result_state["pendingEvents"], [])
            post_mock.assert_called_once()
            self.load_sheet_incidents_mock.assert_called_once_with()
            self.append_sheet_mock.assert_called_once()

    def test_incomplete_auto_sms_claim_is_not_retried_after_restart(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            event = {
                **_captureboard_event(20),
                "autoSms": {
                    "attempted": True,
                    "attemptedAt": self.now.isoformat(),
                    "status": "attempting",
                    "ok": False,
                    "smsStatusText": "문자 자동발송 실패 - 수동 발송 가능",
                    "smsContactActionEnabled": True,
                },
            }
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 20,
                    "pendingEvents": [event],
                }
            )
            auto_sms_sender = Mock()

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.020",
                    "permalink": "https://example.com/recovered-claim",
                },
            ) as post_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                        auto_sms_sender=auto_sms_sender,
                    )
                )

            self.assertEqual(sent_count, 1)
            self.assertEqual(result_state["pendingEvents"], [])
            auto_sms_sender.assert_not_called()
            posted_result = post_mock.call_args.args[1]["deviceResults"][0]
            self.assertEqual(
                posted_result["smsStatusText"],
                "문자 자동발송 실패 - 수동 발송 가능",
            )
            self.assertEqual(posted_result["smsContactActionEnabled"], "true")

    def test_captureboard_incident_state_normalizes_open_status_and_metadata(
        self,
    ) -> None:
        normalized = reporter._normalize_device_notification_alert_state(
            {
                "captureboardIncidentsLastSheetCheckedAt": " 2026-07-13T10:00:00+09:00 ",
                "captureboardIncidents": {
                    " MB2-C00992 ": {
                        "status": "처리 중",
                        "rowNumber": "17",
                        "slackPermalink": " https://example.com/open ",
                        "suppressedCount": "2",
                    },
                    "MB2-C00993": {
                        "deviceName": "MB2-C00993",
                        "status": "완료",
                    },
                    "": {"status": "대기"},
                },
            }
        )

        self.assertEqual(
            normalized["captureboardIncidentsLastSheetCheckedAt"],
            "2026-07-13T10:00:00+09:00",
        )
        self.assertEqual(set(normalized["captureboardIncidents"]), {"MB2-C00992"})
        incident = normalized["captureboardIncidents"]["MB2-C00992"]
        self.assertEqual(incident["status"], "처리중")
        self.assertEqual(incident["rowNumber"], 17)
        self.assertEqual(incident["slackPermalink"], "https://example.com/open")
        self.assertEqual(incident["suppressedCount"], 2)

    def test_open_sheet_incident_suppresses_both_captureboard_event_codes(
        self,
    ) -> None:
        for status in ("대기", "처리 중", "진행중"):
            with self.subTest(status=status), tempfile.TemporaryDirectory() as temp_dir:
                state_path = Path(temp_dir) / "state.json"
                events = [
                    _captureboard_event(51),
                    _recording_stall_event(
                        52,
                        duration_seconds=240,
                        current_size=1000,
                        occurred_at="2026-07-09T03:36:31+00:00",
                    ),
                ]
                state = reporter._normalize_device_notification_alert_state(
                    {
                        "initialized": True,
                        "lastSeenId": 52,
                        "pendingEvents": events,
                    }
                )
                self.load_sheet_incidents_mock.return_value = {
                    "MB2-C00992": {
                        "deviceName": "MB2-C00992",
                        "status": status,
                        "slackPermalink": "https://example.com/open-incident",
                        "rowNumber": 20,
                    }
                }
                auto_sms_sender = Mock()

                with patch.object(
                    reporter,
                    "_post_daily_device_round_abnormal_alert",
                ) as post_mock:
                    result_state, sent_count = (
                        reporter._deliver_pending_device_notification_alerts(
                            Mock(),
                            self.logger,
                            state,
                            channel_id="C094UC05PQW",
                            now=self.now,
                            state_path=state_path,
                            auto_sms_sender=auto_sms_sender,
                        )
                    )

                self.assertEqual(sent_count, 0)
                self.assertEqual(result_state["pendingEvents"], [])
                post_mock.assert_not_called()
                auto_sms_sender.assert_not_called()
                self.append_sheet_mock.assert_not_called()
                incident = result_state["captureboardIncidents"]["MB2-C00992"]
                self.assertEqual(incident["status"], "".join(status.split()))
                self.assertEqual(incident["suppressedCount"], 2)
                self.assertEqual(incident["lastSuppressedNotificationId"], 52)
                self.assertEqual(
                    incident["lastSuppressedCode"],
                    "recording_critically_stalled",
                )
                self.assertEqual(result_state["recordingStallIncidents"], {})
                self.assertEqual(
                    result_state["captureboardIncidentsLastSheetCheckedAt"],
                    self.now.isoformat(),
                )
                self.append_sheet_mock.reset_mock()

    def test_sheet_backed_first_alert_suppresses_later_code_in_same_batch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 62,
                    "pendingEvents": [
                        _captureboard_event(61),
                        _recording_stall_event(
                            62,
                            duration_seconds=240,
                            current_size=1000,
                            occurred_at="2026-07-09T03:36:31+00:00",
                        ),
                    ],
                }
            )
            self.append_sheet_mock.return_value = 1
            auto_sms_sender = Mock(
                return_value={
                    "status": "sent",
                    "ok": True,
                    "smsStatusText": "문자 자동발송 완료",
                    "smsContactActionEnabled": False,
                }
            )

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.061",
                    "permalink": "https://example.com/first-incident",
                },
            ) as post_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                        auto_sms_sender=auto_sms_sender,
                    )
                )

            self.assertEqual(sent_count, 1)
            self.assertEqual(result_state["pendingEvents"], [])
            post_mock.assert_called_once()
            auto_sms_sender.assert_called_once()
            self.append_sheet_mock.assert_called_once()
            incident = result_state["captureboardIncidents"]["MB2-C00992"]
            self.assertEqual(incident["openedNotificationId"], 61)
            self.assertEqual(incident["suppressedCount"], 1)
            self.assertEqual(incident["lastSuppressedNotificationId"], 62)
            self.assertEqual(result_state["recordingStallIncidents"], {})

    def test_closed_or_unknown_sheet_incident_allows_new_captureboard_alert(
        self,
    ) -> None:
        scenarios = {
            "완료": "완료",
            "이상없음": "이상없음",
            "unknown": "확인필요",
            "missing": None,
        }
        for scenario, status in scenarios.items():
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temp_dir:
                state_path = Path(temp_dir) / "state.json"
                state = reporter._normalize_device_notification_alert_state(
                    {
                        "initialized": True,
                        "lastSeenId": 71,
                        "pendingEvents": [_captureboard_event(71)],
                        "captureboardIncidents": {
                            "MB2-C00992": {
                                "deviceName": "MB2-C00992",
                                "status": "대기",
                                "openedNotificationId": 42,
                                "openedCode": "recording_critically_stalled",
                            }
                        },
                    }
                )
                self.load_sheet_incidents_mock.return_value = (
                    {
                        "MB2-C00992": {
                            "deviceName": "MB2-C00992",
                            "status": status,
                            "slackPermalink": "https://example.com/closed",
                            "rowNumber": 21,
                        }
                    }
                    if status is not None
                    else {}
                )
                self.append_sheet_mock.return_value = None

                with patch.object(
                    reporter,
                    "_post_daily_device_round_abnormal_alert",
                    return_value={
                        "channelId": "C094UC05PQW",
                        "messageTs": "1000.071",
                        "permalink": "https://example.com/new-alert",
                    },
                ) as post_mock:
                    result_state, sent_count = (
                        reporter._deliver_pending_device_notification_alerts(
                            Mock(),
                            self.logger,
                            state,
                            channel_id="C094UC05PQW",
                            now=self.now,
                            state_path=state_path,
                        )
                    )

                self.assertEqual(sent_count, 1)
                post_mock.assert_called_once()
                self.append_sheet_mock.assert_called_once()
                self.assertEqual(result_state["captureboardIncidents"], {})
                self.append_sheet_mock.reset_mock()

    def test_sheet_lookup_failure_fails_open_and_clears_old_recording_state(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            incident_key = "MB2-C00992|-|-|recording"
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 81,
                    "pendingEvents": [_captureboard_event(81)],
                    "captureboardIncidents": {
                        "MB2-C00992": {
                            "deviceName": "MB2-C00992",
                            "status": "대기",
                        }
                    },
                    "recordingStallIncidents": {
                        incident_key: {
                            "phase": "candidate",
                            "deviceName": "MB2-C00992",
                            "lastNotificationId": 80,
                            "lastOccurredAt": "2026-07-09T03:34:31+00:00",
                            "lastDurationSeconds": 120,
                        }
                    },
                }
            )
            self.load_sheet_incidents_mock.side_effect = RuntimeError("Sheets down")

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.081",
                    "permalink": "https://example.com/fail-open",
                },
            ) as post_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 1)
            post_mock.assert_called_once()
            self.assertEqual(result_state["captureboardIncidents"], {})
            self.assertEqual(result_state["recordingStallIncidents"], {})
            self.load_sheet_incidents_mock.side_effect = None

    def test_supported_device_notification_codes_are_accepted(self) -> None:
        stalled_event = _recording_stall_event(
            13,
            duration_seconds=120,
            current_size=1000,
            occurred_at="2026-07-09T03:34:31+00:00",
        )
        unsupported_event = {
            **_captureboard_event(14),
            "code": "recording_stalled",
        }

        self.assertIsNotNone(reporter._normalize_pending_event(stalled_event))
        self.assertIsNotNone(
            reporter._normalize_pending_event(
                _segmented_recordings_merge_error_event()
            )
        )
        self.assertIsNone(reporter._normalize_pending_event(unsupported_event))

    def test_all_root_event_types_use_common_auto_sms_path(self) -> None:
        scenarios = (
            (
                "captureboard",
                [_captureboard_event(101)],
                "video_signal",
                "captureboard_disconnected",
            ),
            (
                "recording_stall",
                [
                    _recording_stall_event(
                        102,
                        duration_seconds=120,
                        current_size=1000,
                        occurred_at="2026-07-09T03:34:31+00:00",
                    ),
                ],
                "recording",
                "recording_stalled",
            ),
            (
                "recording_merge",
                [_segmented_recordings_merge_error_event(104)],
                "recording_processing",
                "recording_merge_failed",
            ),
        )

        for scenario, events, expected_category, template_id in scenarios:
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temp_dir:
                state_path = Path(temp_dir) / "state.json"
                state = reporter._normalize_device_notification_alert_state(
                    {
                        "initialized": True,
                        "lastSeenId": events[-1]["notificationId"],
                        "pendingEvents": events,
                    }
                )
                auto_sms_sender = Mock(
                    return_value={
                        "status": "sent",
                        "ok": True,
                        "smsStatusText": "문자 자동발송 완료",
                        "smsContactActionEnabled": False,
                        "smsPhoneNumber": "01012345678",
                        "smsMessage": f"{scenario} 병원 안내",
                        "smsTemplateId": template_id,
                    }
                )
                self.append_sheet_mock.return_value = None

                with patch.object(
                    reporter,
                    "_post_daily_device_round_abnormal_alert",
                    return_value={
                        "channelId": "C094UC05PQW",
                        "messageTs": f"1000.{events[-1]['notificationId']}",
                        "permalink": f"https://example.com/{scenario}",
                    },
                ) as post_mock:
                    result_state, sent_count = (
                        reporter._deliver_pending_device_notification_alerts(
                            Mock(),
                            self.logger,
                            state,
                            channel_id="C094UC05PQW",
                            now=self.now,
                            state_path=state_path,
                            auto_sms_sender=auto_sms_sender,
                        )
                    )

                self.assertEqual(sent_count, 1)
                self.assertEqual(result_state["pendingEvents"], [])
                auto_sms_sender.assert_called_once()
                sms_item = auto_sms_sender.call_args.args[0]
                self.assertEqual(sms_item["alertCategory"], expected_category)
                post_mock.assert_called_once()
                self.assertTrue(post_mock.call_args.kwargs["include_actions"])
                self.assertFalse(
                    post_mock.call_args.kwargs["include_device_voice_action"]
                )
                posted_result = post_mock.call_args.args[1]["deviceResults"][0]
                self.assertEqual(posted_result["smsStatusText"], "문자 자동발송 완료")
                self.assertEqual(posted_result["smsTemplateId"], template_id)
                self.assertEqual(posted_result["smsContactActionEnabled"], "false")

                blocks = (
                    daily_device_round_reporter._build_daily_device_round_abnormal_alert_blocks(
                        post_mock.call_args.args[1],
                        permalink=None,
                        include_actions=True,
                        include_device_voice_action=False,
                    )
                )
                action_blocks = [block for block in blocks if block["type"] == "actions"]
                self.assertEqual(len(action_blocks), 1)
                self.assertEqual(len(action_blocks[0]["elements"]), 1)
                self.assertEqual(
                    action_blocks[0]["elements"][0]["text"]["text"],
                    "문자 자동발송 완료",
                )
                self.append_sheet_mock.reset_mock()

    def test_auto_sms_failure_does_not_block_slack_or_sheet(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            event = _captureboard_event(111)
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 111,
                    "pendingEvents": [event],
                }
            )
            auto_sms_sender = Mock(side_effect=RuntimeError("SMS provider down"))

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.111",
                    "permalink": "https://example.com/sms-failure",
                },
            ) as post_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                        auto_sms_sender=auto_sms_sender,
                    )
                )

            self.assertEqual(sent_count, 1)
            self.assertEqual(result_state["pendingEvents"], [])
            auto_sms_sender.assert_called_once()
            post_mock.assert_called_once()
            posted_result = post_mock.call_args.args[1]["deviceResults"][0]
            self.assertEqual(
                posted_result["smsStatusText"],
                "문자 자동발송 실패 - 수동 발송 가능",
            )
            self.assertEqual(posted_result["smsContactActionEnabled"], "true")
            self.append_sheet_mock.assert_called_once()

    def test_merge_event_without_open_incident_skips_sheet_status_snapshot(
        self,
    ) -> None:
        state = reporter._normalize_device_notification_alert_state(
            {"pendingEvents": [_segmented_recordings_merge_error_event()]}
        )

        refreshed = reporter._refresh_captureboard_incidents_from_sheet(
            state,
            now=self.now,
            logger=self.logger,
        )

        self.assertIs(refreshed, state)
        self.load_sheet_incidents_mock.assert_not_called()

    def test_segmented_recordings_merge_error_posts_immediate_root_alert(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            event = _segmented_recordings_merge_error_event()
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": event["notificationId"],
                    "pendingEvents": [event],
                    "captureboardIncidents": {
                        "MB2-C00992": {
                            "deviceName": "MB2-C00992",
                            "status": "대기",
                        }
                    },
                }
            )
            self.load_sheet_incidents_mock.return_value = {
                "MB2-C00992": {
                    "deviceName": "MB2-C00992",
                    "status": "대기",
                    "slackPermalink": "https://example.com/open-captureboard",
                    "rowNumber": 19,
                }
            }

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.010",
                    "permalink": "https://example.com/merge-error",
                },
            ) as post_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 1)
            self.assertEqual(result_state["pendingEvents"], [])
            self.assertEqual(result_state["recentCaptureboardAlerts"], {})
            alert_summary = post_mock.call_args.args[1]
            device_result = alert_summary["deviceResults"][0]
            self.assertEqual(device_result["deviceName"], "MB2-C00992")
            self.assertIn("분할 파일 3개", device_result["priorityReason"])
            self.assertIn(
                "ffmpeg exited with code 1",
                device_result["priorityReason"],
            )
            self.assertIn(
                "2026-07-09 12:34:31 KST",
                device_result["priorityReason"],
            )
            self.assertNotIn("`2026-07-09", device_result["priorityReason"])
            self.assertEqual(
                device_result["componentLabels"]["captureboard"],
                "정상",
            )
            self.assertEqual(device_result["alertCategory"], "recording_processing")
            merge_blocks = (
                daily_device_round_reporter._build_daily_device_round_abnormal_alert_blocks(
                    alert_summary,
                    permalink=None,
                    include_actions=False,
                )
            )
            self.assertEqual(
                merge_blocks[0]["text"]["text"],
                ":alert: 녹화 파일 처리 확인 필요",
            )
            self.assertEqual(post_mock.call_args.kwargs["channel_id"], "C094UC05PQW")
            self.assertTrue(post_mock.call_args.kwargs["include_blocks"])
            self.assertFalse(post_mock.call_args.kwargs["include_actions"])
            self.append_sheet_mock.assert_called_once()
            merge_sheet_items = self.append_sheet_mock.call_args.args[0]
            self.assertEqual(merge_sheet_items[0]["device"], "MB2-C00992")
            self.assertEqual(merge_sheet_items[0]["problemComponents"], [])
            self.assertEqual(
                self.append_sheet_mock.call_args.kwargs["slack_permalink"],
                "https://example.com/merge-error",
            )
            self.load_sheet_incidents_mock.assert_not_called()

    def test_two_minute_recording_stall_sheet_row_suppresses_same_batch_repeats(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            events = [
                _recording_stall_event(
                    21,
                    duration_seconds=120,
                    current_size=1000,
                    occurred_at="2026-07-09T03:34:31+00:00",
                ),
                _recording_stall_event(
                    22,
                    duration_seconds=240,
                    current_size=1000,
                    occurred_at="2026-07-09T03:36:31+00:00",
                ),
                _recording_stall_event(
                    23,
                    duration_seconds=360,
                    current_size=1000,
                    occurred_at="2026-07-09T03:38:31+00:00",
                ),
                _recording_stall_event(
                    24,
                    duration_seconds=480,
                    current_size=1000,
                    occurred_at="2026-07-09T03:40:31+00:00",
                ),
            ]
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 24,
                    "pendingEvents": events,
                }
            )
            client = Mock()
            self.append_sheet_mock.return_value = 1

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.001",
                    "permalink": "https://example.com/recording-stall",
                },
            ) as post_root_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        client,
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 1)
            self.assertEqual(result_state["pendingEvents"], [])
            post_root_mock.assert_called_once()
            self.assertTrue(post_root_mock.call_args.kwargs["include_blocks"])
            self.assertFalse(post_root_mock.call_args.kwargs["include_actions"])
            root_summary = post_root_mock.call_args.args[1]
            self.assertEqual(
                root_summary["deviceResults"][0]["alertCategory"],
                "recording",
            )
            root_issue = root_summary["deviceResults"][0]["priorityReason"]
            self.assertEqual(
                root_issue,
                "녹화 파일 증가 정지가 120초 (2분) 동안 지속됐어: "
                "0.00 KB/sec (발생 2026-07-09 12:34:31 KST)",
            )
            # 장비 이벤트는 공통 2열 카드를 쓰면서 모니터 조치 버튼만 제외해.
            root_blocks = (
                daily_device_round_reporter._build_daily_device_round_abnormal_alert_blocks(
                    root_summary,
                    permalink=None,
                    include_actions=False,
                )
            )
            self.assertEqual(
                [block["type"] for block in root_blocks],
                ["header", "section", "section", "section"],
            )
            self.assertEqual(
                root_blocks[0]["text"]["text"],
                ":alert: 녹화 상태 확인 필요",
            )
            self.assertTrue(
                root_blocks[1]["fields"][0]["text"].startswith("⚙️ *장비*\n")
            )
            self.assertEqual(
                root_blocks[2]["fields"],
                [{"type": "mrkdwn", "text": f"🔎 *감지 내용*\n`{root_issue}`"}],
            )
            # 120초 루트 행이 대기로 생성되면 240초 이후 이벤트는 같은 장애로 소비한다.
            self.append_sheet_mock.assert_called_once()
            recording_sheet_items = self.append_sheet_mock.call_args.args[0]
            self.assertEqual(recording_sheet_items[0]["device"], "MB2-C00992")
            self.assertEqual(recording_sheet_items[0]["problemComponents"], [])
            self.assertEqual(
                self.append_sheet_mock.call_args.kwargs["detected_at"],
                datetime(2026, 7, 9, 3, 34, 31, tzinfo=timezone.utc),
            )
            self.assertEqual(
                self.append_sheet_mock.call_args.kwargs["slack_permalink"],
                "https://example.com/recording-stall",
            )
            client.chat_postMessage.assert_not_called()
            self.assertEqual(result_state["recordingStallIncidents"], {})
            incident = result_state["captureboardIncidents"]["MB2-C00992"]
            self.assertEqual(incident["openedNotificationId"], 21)
            self.assertEqual(incident["openedCode"], "recording_critically_stalled")
            self.assertEqual(incident["suppressedCount"], 3)
            self.assertEqual(incident["lastSuppressedNotificationId"], 24)

    def test_sheet_append_failure_keeps_followup_in_thread_without_resending_sms(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 31,
                    "pendingEvents": [
                        _recording_stall_event(
                            31,
                            duration_seconds=120,
                            current_size=1000,
                            occurred_at="2026-07-09T03:34:31+00:00",
                        )
                    ],
                }
            )
            client = Mock()
            client.chat_postMessage.return_value = {"ts": "1000.032"}
            auto_sms_sender = Mock(
                return_value={
                    "status": "sent",
                    "ok": True,
                    "smsStatusText": "문자 자동발송 완료",
                    "smsContactActionEnabled": False,
                }
            )
            self.append_sheet_mock.side_effect = RuntimeError("Sheets down")

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.031",
                    "permalink": "https://example.com/two-minute-recording-stall",
                },
            ) as post_root_mock:
                first_state, first_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        client,
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                        auto_sms_sender=auto_sms_sender,
                    )
                )
                second_state, second_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        client,
                        self.logger,
                        {
                            **first_state,
                            "lastSeenId": 32,
                            "pendingEvents": [
                                _recording_stall_event(
                                    32,
                                    duration_seconds=240,
                                    current_size=1000,
                                    occurred_at="2026-07-09T03:36:31+00:00",
                                )
                            ],
                        },
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                        auto_sms_sender=auto_sms_sender,
                    )
                )

            self.assertEqual((first_count, second_count), (1, 1))
            post_root_mock.assert_called_once()
            auto_sms_sender.assert_called_once()
            self.append_sheet_mock.assert_called_once()
            client.chat_postMessage.assert_called_once()
            incident = next(iter(second_state["recordingStallIncidents"].values()))
            self.assertEqual(incident["phase"], "alerted")
            self.assertEqual(incident["lastNotificationId"], 32)
            self.assertEqual(incident["lastDurationSeconds"], 240)

    def test_recording_stall_alerts_immediately_at_two_minutes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            event = _recording_stall_event(
                31,
                duration_seconds=120,
                current_size=1000,
                occurred_at="2026-07-09T03:34:31+00:00",
            )
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 31,
                    "pendingEvents": [event],
                }
            )

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.031",
                    "permalink": "https://example.com/two-minute-recording-stall",
                },
            ) as post_root_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 1)
            post_root_mock.assert_called_once()
            incident = next(iter(result_state["recordingStallIncidents"].values()))
            self.assertEqual(incident["phase"], "alerted")
            self.assertEqual(incident["firstNotificationId"], 31)
            self.assertEqual(incident["lastDurationSeconds"], 120)

    def test_recording_stall_keeps_non_alertable_events_outside_two_minute_scope(
        self,
    ) -> None:
        missing_size_event = _recording_stall_event(
            35,
            duration_seconds=120,
            current_size=1000,
            occurred_at="2026-07-09T03:34:31+00:00",
        )
        missing_size_event["details"]["currentSize"] = None
        scenarios = (
            (
                "before_two_minutes",
                _recording_stall_event(
                    31,
                    duration_seconds=119,
                    current_size=1000,
                    occurred_at="2026-07-09T03:34:30+00:00",
                ),
            ),
            (
                "file_is_growing",
                _recording_stall_event(
                    32,
                    duration_seconds=120,
                    current_size=1001,
                    growth_rate=1,
                    occurred_at="2026-07-09T03:34:31+00:00",
                ),
            ),
            (
                "not_recording",
                _recording_stall_event(
                    33,
                    duration_seconds=120,
                    current_size=1000,
                    current_status="idle",
                    occurred_at="2026-07-09T03:34:31+00:00",
                ),
            ),
            (
                "motion_file",
                _recording_stall_event(
                    34,
                    duration_seconds=120,
                    current_size=1000,
                    file_type="motion",
                    occurred_at="2026-07-09T03:34:31+00:00",
                ),
            ),
            ("missing_current_size", missing_size_event),
        )

        for scenario, event in scenarios:
            with (
                self.subTest(scenario=scenario),
                tempfile.TemporaryDirectory() as temp_dir,
                patch.object(
                    reporter,
                    "_post_daily_device_round_abnormal_alert",
                ) as post_root_mock,
            ):
                state_path = Path(temp_dir) / "state.json"
                state = reporter._normalize_device_notification_alert_state(
                    {
                        "initialized": True,
                        "lastSeenId": event["notificationId"],
                        "pendingEvents": [event],
                    }
                )
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        Mock(),
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 0)
            self.assertEqual(result_state["pendingEvents"], [])
            self.assertEqual(result_state["recordingStallIncidents"], {})
            post_root_mock.assert_not_called()

    def test_completed_sheet_incident_restarts_recording_alert_immediately(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            incident_key = "MB2-C00992|-|-|recording"
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 43,
                    "pendingEvents": [
                        _recording_stall_event(
                            43,
                            duration_seconds=360,
                            current_size=1000,
                            occurred_at="2026-07-09T03:38:31+00:00",
                        ),
                    ],
                    "captureboardIncidents": {
                        "MB2-C00992": {
                            "deviceName": "MB2-C00992",
                            "status": "대기",
                            "openedNotificationId": 42,
                            "openedCode": "recording_critically_stalled",
                        }
                    },
                    "recordingStallIncidents": {
                        incident_key: {
                            "phase": "alerted",
                            "deviceName": "MB2-C00992",
                            "firstNotificationId": 41,
                            "firstOccurredAt": "2026-07-09T03:34:31+00:00",
                            "firstDurationSeconds": 120,
                            "lastNotificationId": 42,
                            "lastOccurredAt": "2026-07-09T03:36:31+00:00",
                            "lastDurationSeconds": 240,
                            "lastCurrentSize": 1000,
                            "slackMessageTs": "1000.001",
                        }
                    },
                }
            )
            self.load_sheet_incidents_mock.return_value = {
                "MB2-C00992": {
                    "deviceName": "MB2-C00992",
                    "status": "완료",
                    "slackPermalink": "https://example.com/completed",
                    "rowNumber": 30,
                }
            }
            self.append_sheet_mock.return_value = 1
            client = Mock()

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
                return_value={
                    "channelId": "C094UC05PQW",
                    "messageTs": "1000.043",
                    "permalink": "https://example.com/reopened-recording",
                },
            ) as post_root_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        client,
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 1)
            post_root_mock.assert_called_once()
            client.chat_postMessage.assert_not_called()
            self.append_sheet_mock.assert_called_once()
            self.assertEqual(result_state["recordingStallIncidents"], {})
            reopened = result_state["captureboardIncidents"]["MB2-C00992"]
            self.assertEqual(reopened["openedNotificationId"], 43)
            self.assertEqual(reopened["openedCode"], "recording_critically_stalled")

    def test_missing_sheet_row_preserves_non_sheet_recording_thread(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            incident_key = "MB2-C00992|-|-|recording"
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 43,
                    "pendingEvents": [
                        _recording_stall_event(
                            43,
                            duration_seconds=360,
                            current_size=1000,
                            occurred_at="2026-07-09T03:38:31+00:00",
                        )
                    ],
                    "recordingStallIncidents": {
                        incident_key: {
                            "phase": "alerted",
                            "deviceName": "MB2-C00992",
                            "firstNotificationId": 41,
                            "firstOccurredAt": "2026-07-09T03:34:31+00:00",
                            "firstDurationSeconds": 120,
                            "lastNotificationId": 42,
                            "lastOccurredAt": "2026-07-09T03:36:31+00:00",
                            "lastDurationSeconds": 240,
                            "lastCurrentSize": 1000,
                            "slackMessageTs": "1000.001",
                        }
                    },
                }
            )
            client = Mock()
            client.chat_postMessage.return_value = {"ts": "1000.043"}
            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
            ) as post_root_mock:
                result_state, sent_count = (
                    reporter._deliver_pending_device_notification_alerts(
                        client,
                        self.logger,
                        state,
                        channel_id="C094UC05PQW",
                        now=self.now,
                        state_path=state_path,
                    )
                )

            self.assertEqual(sent_count, 1)
            self.assertEqual(result_state["pendingEvents"], [])
            post_root_mock.assert_not_called()
            client.chat_postMessage.assert_called_once()
            incident = next(iter(result_state["recordingStallIncidents"].values()))
            self.assertEqual(incident["phase"], "alerted")
            self.assertEqual(incident["firstNotificationId"], 41)
            self.assertEqual(incident["lastDurationSeconds"], 360)

    def test_disabled_sheet_preserves_recording_thread_retry(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            incident_key = "MB2-C00992|-|-|recording"
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 43,
                    "pendingEvents": [
                        _recording_stall_event(
                            43,
                            duration_seconds=360,
                            current_size=1000,
                            occurred_at="2026-07-09T03:38:31+00:00",
                        )
                    ],
                    "recordingStallIncidents": {
                        incident_key: {
                            "phase": "alerted",
                            "deviceName": "MB2-C00992",
                            "firstNotificationId": 41,
                            "firstOccurredAt": "2026-07-09T03:34:31+00:00",
                            "firstDurationSeconds": 120,
                            "lastNotificationId": 42,
                            "lastOccurredAt": "2026-07-09T03:36:31+00:00",
                            "lastDurationSeconds": 240,
                            "lastCurrentSize": 1000,
                            "slackMessageTs": "1000.001",
                        }
                    },
                }
            )
            # Sheet 연동이 꺼진 환경에서는 기존 스레드 발송과 재시도 의미를 유지한다.
            self.load_sheet_incidents_mock.return_value = None
            failed_client = Mock()
            failed_client.chat_postMessage.side_effect = RuntimeError("Slack down")

            failed_state, failed_count = (
                reporter._deliver_pending_device_notification_alerts(
                    failed_client,
                    self.logger,
                    state,
                    channel_id="C094UC05PQW",
                    now=self.now,
                    state_path=state_path,
                )
            )

            self.assertEqual(failed_count, 0)
            self.assertEqual(
                [event["notificationId"] for event in failed_state["pendingEvents"]],
                [43],
            )
            self.assertEqual(
                failed_state["recordingStallIncidents"][incident_key][
                    "lastDurationSeconds"
                ],
                240,
            )

            retry_client = Mock()
            retry_client.chat_postMessage.return_value = {"ts": "1000.002"}
            retried_state, retried_count = (
                reporter._deliver_pending_device_notification_alerts(
                    retry_client,
                    self.logger,
                    failed_state,
                    channel_id="C094UC05PQW",
                    now=self.now,
                    state_path=state_path,
                )
            )

            self.assertEqual(retried_count, 1)
            self.assertEqual(retried_state["pendingEvents"], [])
            self.assertEqual(
                retried_state["recordingStallIncidents"][incident_key][
                    "lastDurationSeconds"
                ],
                360,
            )

    def test_recent_event_alert_suppresses_existing_captureboard_monitor_alert(
        self,
    ) -> None:
        event = _captureboard_event()
        summary = reporter._build_captureboard_notification_alert_summary(event)

        with patch.object(
            health_reporter,
            "_load_recent_captureboard_notification_alerts",
            return_value={"MB2-C00992": self.now},
        ):
            alertable, updated_alerts, pending_alerts = (
                health_reporter._collect_device_health_monitor_alert_updates(
                    summary,
                    {},
                    now=self.now,
                )
            )

        self.assertEqual(alertable, set())
        self.assertEqual(pending_alerts, {})
        self.assertEqual(len(updated_alerts), 1)
        self.assertEqual(
            next(iter(updated_alerts.values()))["lastAlertedAt"],
            self.now.isoformat(),
        )

    def test_remembers_accepted_sms_before_notification_sheet_append(self) -> None:
        event = _captureboard_event(1300)
        alert_summary = reporter._build_captureboard_notification_alert_summary(event)
        alert_summary["deviceResults"][0].update(
            {
                "smsDeliveryStatus": "accepted",
                "smsGroupId": "G1300",
                "smsMessageId": "M1300",
            }
        )
        operations: list[str] = []

        with (
            patch.object(reporter.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                reporter,
                "remember_sms_delivery_sheet_record",
                side_effect=lambda *args, **kwargs: operations.append("remember"),
            ) as remember_mock,
            patch.object(
                reporter,
                "_append_device_health_sheet_alerts",
                side_effect=lambda *args, **kwargs: operations.append("append") or 1,
            ),
        ):
            recorded = reporter._record_device_notification_sheet_alert_best_effort(
                alert_summary,
                event,
                fallback_detected_at=self.now,
                slack_permalink="https://example.com/notification-1300",
                logger=self.logger,
            )

        self.assertTrue(recorded)
        self.assertEqual(operations, ["remember", "append"])
        remembered_item = remember_mock.call_args.args[0]
        self.assertEqual(remembered_item["smsGroupId"], "G1300")
        self.assertEqual(
            remember_mock.call_args.kwargs["permalink"],
            "https://example.com/notification-1300",
        )

    def test_corrupted_state_stops_without_resetting_cursor(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            state_path.write_text("{broken", encoding="utf-8")

            with self.assertRaises(RuntimeError):
                reporter._load_device_notification_alert_state(
                    state_path,
                    logger=self.logger,
                )

            self.assertEqual(state_path.read_text(encoding="utf-8"), "{broken")

    def test_db_batch_uses_fixed_upper_bound_and_advances_to_latest_id(self) -> None:
        cursor = _FakeCursor(latest_id=30, rows=[_captureboard_event(21)])
        connection = _FakeConnection(cursor)

        with patch.object(reporter, "_create_db_connection", return_value=connection):
            next_cursor, events = reporter._load_device_notification_batch(20)

        self.assertTrue(connection.closed)
        self.assertEqual(next_cursor, 30)
        self.assertEqual([event["notificationId"] for event in events], [21])
        self.assertEqual(
            cursor.execute_calls[1][1],
            (
                20,
                30,
                "captureboard_connection_error",
                "recording_critically_stalled",
                "segmented_recordings_merge_error",
                200,
            ),
        )


if __name__ == "__main__":
    unittest.main()
