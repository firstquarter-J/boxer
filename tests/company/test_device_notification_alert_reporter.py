import logging
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

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
            self.assertEqual(device_result["componentLabels"]["captureboard"], "이상")
            self.assertIn("2026-07-09 12:34:31 KST", device_result["priorityReason"])
            self.assertEqual(post_mock.call_args.kwargs["channel_id"], "C094UC05PQW")
            self.assertFalse(post_mock.call_args.kwargs["include_actions"])

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
                )

            failed_state = reporter._load_device_notification_alert_state(state_path)
            self.assertFalse(first_sent)
            self.assertEqual(failed_state["lastSeenId"], 12)
            self.assertEqual(
                [item["notificationId"] for item in failed_state["pendingEvents"]],
                [12],
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
                ),
            ):
                retried = reporter._run_device_notification_alert_once(
                    object(),
                    self.logger,
                    now=self.now,
                    state_path=state_path,
                )

            retried_state = reporter._load_device_notification_alert_state(state_path)
            self.assertTrue(retried)
            self.assertEqual(retried_state["pendingEvents"], [])
            self.assertEqual(retried_state["lastSentNotificationId"], 12)
            batch_mock.assert_called_once_with(12)

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
                }
            )

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
            self.assertEqual(
                device_result["componentLabels"]["captureboard"],
                "정상",
            )
            self.assertEqual(post_mock.call_args.kwargs["channel_id"], "C094UC05PQW")
            self.assertFalse(post_mock.call_args.kwargs["include_actions"])

    def test_confirmed_recording_stall_posts_root_then_repeats_as_thread_replies(
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
            client.chat_postMessage.side_effect = [
                {"ts": "1000.002"},
                {"ts": "1000.003"},
            ]

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

            self.assertEqual(sent_count, 3)
            self.assertEqual(result_state["pendingEvents"], [])
            post_root_mock.assert_called_once()
            self.assertEqual(client.chat_postMessage.call_count, 2)
            for call in client.chat_postMessage.call_args_list:
                self.assertEqual(call.kwargs["thread_ts"], "1000.001")
            self.assertIn(
                "360초 (6분)", client.chat_postMessage.call_args_list[0].kwargs["text"]
            )
            self.assertIn(
                "480초 (8분)", client.chat_postMessage.call_args_list[1].kwargs["text"]
            )
            incident = next(iter(result_state["recordingStallIncidents"].values()))
            self.assertEqual(incident["phase"], "alerted")
            self.assertEqual(incident["firstNotificationId"], 21)
            self.assertEqual(incident["lastDurationSeconds"], 480)
            self.assertEqual(incident["lastCommentNotificationId"], 24)

    def test_recording_stall_requires_two_equal_file_sizes_before_root_alert(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "state.json"
            first_two_events = [
                _recording_stall_event(
                    31,
                    duration_seconds=120,
                    current_size=1000,
                    occurred_at="2026-07-09T03:34:31+00:00",
                ),
                _recording_stall_event(
                    32,
                    duration_seconds=240,
                    current_size=1001,
                    occurred_at="2026-07-09T03:36:31+00:00",
                ),
            ]
            state = reporter._normalize_device_notification_alert_state(
                {
                    "initialized": True,
                    "lastSeenId": 32,
                    "pendingEvents": first_two_events,
                }
            )

            with patch.object(
                reporter,
                "_post_daily_device_round_abnormal_alert",
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

            self.assertEqual(sent_count, 0)
            post_root_mock.assert_not_called()
            incident = next(iter(result_state["recordingStallIncidents"].values()))
            self.assertEqual(incident["firstNotificationId"], 32)
            self.assertEqual(incident["lastCurrentSize"], 1001)

    def test_failed_recording_stall_thread_reply_stays_pending_for_retry(self) -> None:
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
