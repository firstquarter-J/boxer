import copy
import gzip
import json
import logging
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack import device_health_monitor_reporter as reporter


_KST = ZoneInfo("Asia/Seoul")


class _MissingS3Object(Exception):
    def __init__(self) -> None:
        super().__init__("missing")
        self.response = {"Error": {"Code": "404"}}


class _FakeS3Client:
    def __init__(
        self,
        *,
        fail_put: bool = False,
        on_existing_head=None,
    ) -> None:
        self.objects: dict[str, dict[str, object]] = {}
        self.put_calls: list[dict[str, object]] = []
        self.head_calls: list[dict[str, str]] = []
        self.fail_put = fail_put
        self.on_existing_head = on_existing_head

    def head_object(self, *, Bucket: str, Key: str) -> dict[str, object]:
        self.head_calls.append({"Bucket": Bucket, "Key": Key})
        stored = self.objects.get(Key)
        if stored is None:
            raise _MissingS3Object()
        response = {
            "ContentLength": len(stored["body"]),
            "Metadata": dict(stored["metadata"]),
        }
        if self.on_existing_head is not None:
            callback = self.on_existing_head
            self.on_existing_head = None
            callback()
        return response

    def put_object(self, **kwargs) -> dict[str, str]:
        if self.fail_put:
            raise RuntimeError("put failed")
        body_source = kwargs["Body"]
        body = body_source.read() if hasattr(body_source, "read") else bytes(body_source)
        if len(body) != kwargs["ContentLength"]:
            raise AssertionError("ContentLength와 실제 업로드 크기가 달라")
        self.put_calls.append({**kwargs, "Body": body})
        self.objects[str(kwargs["Key"])] = {
            "body": body,
            "metadata": dict(kwargs["Metadata"]),
        }
        return {"ETag": '"test-etag"'}


def _unavailable_device(
    index: int,
    *,
    checked_at: str = "2026-07-23T09:00:00+09:00",
    availability_reason: str = "device_stale",
) -> dict[str, object]:
    return {
        "hospitalSeq": 100 + index,
        "hospitalName": f"테스트병원 {index}",
        "roomName": f"{index}번방",
        "deviceName": f"MB2-T{index:05d}",
        "overallLabel": "점검 불가",
        "priorityReason": "장비 상태 확인 불가",
        "statusText": "장비가 오프라인이야",
        "statusPayload": {
            "source": "redis_device_state",
            "redis": {
                "checkedAt": checked_at,
                "availabilityReasons": [availability_reason],
                "deviceState": {
                    "updatedAt": checked_at,
                    "isConnected": False,
                    "status": "OFFLINE",
                },
                "agentState": {
                    "updatedAt": checked_at,
                    "isConnected": False,
                },
            },
            "ssh": {
                "ready": False,
                "verified": False,
                "reason": "device_unavailable",
            },
        },
    }


def _unavailable_summary(count: int = 1) -> dict[str, object]:
    devices = [_unavailable_device(index) for index in range(1, count + 1)]
    return {
        "runDate": "2026-07-23",
        "startedAt": "2026-07-23T09:00:00+09:00",
        "finishedAt": "2026-07-23T09:00:10+09:00",
        "checkedDeviceCount": count,
        "scheduledDeviceCount": count,
        "deviceCount": count,
        "statusCounts": {
            "정상": 0,
            "확인 필요": 0,
            "이상": 0,
            "점검 불가": count,
        },
        "deviceResults": devices,
    }


class DeviceHealthEventStorageTests(unittest.TestCase):
    def _archive_settings(self, *, retention_days: int = 14) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(
            patch.object(
                reporter.cs,
                "DEVICE_HEALTH_MONITOR_EVENT_LOG_RETENTION_DAYS",
                retention_days,
            )
        )
        stack.enter_context(
            patch.object(
                reporter.cs,
                "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET",
                "boxer-kr",
            )
        )
        stack.enter_context(
            patch.object(
                reporter.cs,
                "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX",
                "device-health-monitor/events",
            )
        )
        return stack

    @staticmethod
    def _write_daily_log(log_dir: Path, day: datetime, content: bytes) -> Path:
        path = log_dir / f"device_health_monitor_events-{day.date().isoformat()}.jsonl"
        path.write_bytes(content)
        return path

    def test_archive_keeps_today_inclusive_fourteen_days_and_uploads_older_log(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        source_content = b'{"eventType":"run_summary"}\n'
        with tempfile.TemporaryDirectory() as temp_dir, self._archive_settings():
            log_dir = Path(temp_dir)
            first_kept = self._write_daily_log(
                log_dir,
                now - timedelta(days=13),
                b'{"kept":true}\n',
            )
            old_source = self._write_daily_log(
                log_dir,
                now - timedelta(days=14),
                source_content,
            )
            s3_client = _FakeS3Client()

            result = reporter._archive_device_health_monitor_event_logs(
                now=now,
                log_dir=log_dir,
                s3_client=s3_client,
            )

            self.assertEqual(result["firstKeptDate"], "2026-07-10")
            self.assertEqual(result["archivedCount"], 1)
            self.assertEqual(result["keptCount"], 1)
            self.assertEqual(result["failedCount"], 0)
            self.assertTrue(first_kept.exists())
            self.assertFalse(old_source.exists())
            self.assertEqual(len(s3_client.put_calls), 1)
            upload = s3_client.put_calls[0]
            self.assertEqual(
                upload["Key"],
                (
                    "device-health-monitor/events/2026/07/"
                    "device_health_monitor_events-2026-07-09.jsonl.gz"
                ),
            )
            self.assertEqual(gzip.decompress(upload["Body"]), source_content)
            self.assertEqual(upload["ContentEncoding"], "gzip")
            self.assertEqual(upload["ServerSideEncryption"], "AES256")
            # 업로드 전 부재 확인과 업로드 후 검증 HEAD가 모두 수행돼야 한다.
            self.assertEqual(len(s3_client.head_calls), 2)

    def test_archive_gzip_is_deterministic(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        source_content = (b'{"eventType":"device_unavailable"}\n' * 100)
        archived_bodies: list[bytes] = []

        with self._archive_settings():
            for _ in range(2):
                with tempfile.TemporaryDirectory() as temp_dir:
                    log_dir = Path(temp_dir)
                    self._write_daily_log(log_dir, now - timedelta(days=20), source_content)
                    s3_client = _FakeS3Client()
                    result = reporter._archive_device_health_monitor_event_logs(
                        now=now,
                        log_dir=log_dir,
                        s3_client=s3_client,
                    )
                    self.assertEqual(result["archivedCount"], 1)
                    archived_bodies.append(s3_client.put_calls[0]["Body"])

        self.assertEqual(archived_bodies[0], archived_bodies[1])
        self.assertEqual(gzip.decompress(archived_bodies[0]), source_content)

    def test_archive_upload_failure_preserves_original(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        source_content = b'{"mustRemain":true}\n'
        with tempfile.TemporaryDirectory() as temp_dir, self._archive_settings():
            log_dir = Path(temp_dir)
            source = self._write_daily_log(
                log_dir,
                now - timedelta(days=20),
                source_content,
            )

            result = reporter._archive_device_health_monitor_event_logs(
                now=now,
                log_dir=log_dir,
                s3_client=_FakeS3Client(fail_put=True),
            )

            self.assertEqual(result["archivedCount"], 0)
            self.assertEqual(result["failedCount"], 1)
            self.assertTrue(source.exists())
            self.assertEqual(source.read_bytes(), source_content)

    def test_archive_ignores_future_malformed_and_symlink_logs(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        with tempfile.TemporaryDirectory() as temp_dir, self._archive_settings():
            log_dir = Path(temp_dir)
            future_source = self._write_daily_log(
                log_dir,
                now + timedelta(days=1),
                b'{"future":true}\n',
            )
            malformed_source = (
                log_dir / "device_health_monitor_events-not-a-date.jsonl"
            )
            malformed_source.write_bytes(b'{"malformed":true}\n')
            symlink_target = log_dir / "external-old-log.jsonl"
            symlink_target.write_bytes(b'{"target":true}\n')
            symlink_source = (
                log_dir / "device_health_monitor_events-2026-06-01.jsonl"
            )
            symlink_source.symlink_to(symlink_target)
            s3_client = _FakeS3Client()

            result = reporter._archive_device_health_monitor_event_logs(
                now=now,
                log_dir=log_dir,
                s3_client=s3_client,
            )

            self.assertEqual(result["archivedCount"], 0)
            self.assertEqual(result["failedCount"], 0)
            self.assertEqual(s3_client.put_calls, [])
            self.assertTrue(future_source.exists())
            self.assertTrue(malformed_source.exists())
            self.assertTrue(symlink_source.is_symlink())
            self.assertEqual(symlink_target.read_bytes(), b'{"target":true}\n')

    def test_archive_existing_matching_object_skips_put_and_removes_original(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        source_content = b'{"sameArchive":true}\n'
        s3_client = _FakeS3Client()

        with self._archive_settings():
            with tempfile.TemporaryDirectory() as first_temp_dir:
                first_log_dir = Path(first_temp_dir)
                self._write_daily_log(
                    first_log_dir,
                    now - timedelta(days=20),
                    source_content,
                )
                first_result = reporter._archive_device_health_monitor_event_logs(
                    now=now,
                    log_dir=first_log_dir,
                    s3_client=s3_client,
                )
                self.assertEqual(first_result["archivedCount"], 1)

            put_count = len(s3_client.put_calls)
            with tempfile.TemporaryDirectory() as retry_temp_dir:
                retry_log_dir = Path(retry_temp_dir)
                retry_source = self._write_daily_log(
                    retry_log_dir,
                    now - timedelta(days=20),
                    source_content,
                )
                retry_result = reporter._archive_device_health_monitor_event_logs(
                    now=now,
                    log_dir=retry_log_dir,
                    s3_client=s3_client,
                )

                self.assertEqual(retry_result["archivedCount"], 1)
                self.assertEqual(retry_result["failedCount"], 0)
                self.assertEqual(len(s3_client.put_calls), put_count)
                self.assertFalse(retry_source.exists())

    def test_archive_existing_different_object_is_not_overwritten(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        source_content = b'{"local":"must survive"}\n'
        key = (
            "device-health-monitor/events/2026/07/"
            "device_health_monitor_events-2026-07-03.jsonl.gz"
        )
        remote_body = b"already-stored-different-object"
        s3_client = _FakeS3Client()
        s3_client.objects[key] = {
            "body": remote_body,
            "metadata": {
                "source-filename": "device_health_monitor_events-2026-07-03.jsonl",
                "source-sha256": "different",
            },
        }

        with tempfile.TemporaryDirectory() as temp_dir, self._archive_settings():
            log_dir = Path(temp_dir)
            source = self._write_daily_log(
                log_dir,
                now - timedelta(days=20),
                source_content,
            )
            result = reporter._archive_device_health_monitor_event_logs(
                now=now,
                log_dir=log_dir,
                s3_client=s3_client,
            )

            self.assertEqual(result["archivedCount"], 0)
            self.assertEqual(result["failedCount"], 1)
            self.assertEqual(s3_client.put_calls, [])
            self.assertTrue(source.exists())
            self.assertEqual(source.read_bytes(), source_content)
            self.assertEqual(s3_client.objects[key]["body"], remote_body)

    def test_archive_does_not_remove_source_replaced_during_upload(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        original_content = b'{"original":true}\n'
        replacement_content = b'{"replacement":"must survive"}\n'
        with tempfile.TemporaryDirectory() as temp_dir, self._archive_settings():
            log_dir = Path(temp_dir)
            source = self._write_daily_log(
                log_dir,
                now - timedelta(days=20),
                original_content,
            )

            def replace_source_before_delete() -> None:
                source.write_bytes(replacement_content)

            s3_client = _FakeS3Client(on_existing_head=replace_source_before_delete)
            result = reporter._archive_device_health_monitor_event_logs(
                now=now,
                log_dir=log_dir,
                s3_client=s3_client,
            )

            self.assertEqual(result["archivedCount"], 0)
            self.assertEqual(result["failedCount"], 1)
            self.assertTrue(source.exists())
            self.assertEqual(source.read_bytes(), replacement_content)
            stored = next(iter(s3_client.objects.values()))
            self.assertEqual(gzip.decompress(stored["body"]), original_content)

    def test_unavailable_signature_uses_full_set_and_ignores_volatile_timestamps(self) -> None:
        summary = _unavailable_summary(21)
        first_signature = reporter._device_health_monitor_unavailable_event_signature(summary)

        reordered = copy.deepcopy(summary)
        reordered["deviceResults"].reverse()
        for device in reordered["deviceResults"]:
            redis_payload = device["statusPayload"]["redis"]
            redis_payload["checkedAt"] = "2026-07-23T09:01:00+09:00"
            redis_payload["deviceState"]["updatedAt"] = "2026-07-23T09:01:00+09:00"
            redis_payload["agentState"]["updatedAt"] = "2026-07-23T09:01:00+09:00"
        self.assertEqual(
            reporter._device_health_monitor_unavailable_event_signature(reordered),
            first_signature,
        )

        changed_outside_sample = copy.deepcopy(summary)
        changed_outside_sample["deviceResults"][20]["statusPayload"]["redis"][
            "availabilityReasons"
        ] = ["agent_stale"]
        self.assertNotEqual(
            reporter._device_health_monitor_unavailable_event_signature(changed_outside_sample),
            first_signature,
        )

    def test_unavailable_event_is_initial_then_periodic_and_resets_on_recovery(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        payload = {
            "count": 2,
            "stateSignature": "signature-a",
            "sampleDevices": [],
        }
        with patch.object(
            reporter.cs,
            "DEVICE_HEALTH_MONITOR_UNAVAILABLE_EVENT_SUMMARY_HOURS",
            6,
        ):
            should_log, initial_state = (
                reporter._resolve_device_health_monitor_unavailable_event_log(
                    payload,
                    state={},
                    now=now,
                )
            )
            self.assertTrue(should_log)
            self.assertEqual(initial_state["lastLoggedAt"], now.isoformat())
            self.assertEqual(initial_state["unavailableCount"], 2)

            changed_payload = {**payload, "stateSignature": "signature-b"}
            should_log, changed_state = (
                reporter._resolve_device_health_monitor_unavailable_event_log(
                    changed_payload,
                    state={"deviceUnavailableEventState": initial_state},
                    now=now + timedelta(minutes=1),
                )
            )
            self.assertFalse(should_log)
            self.assertEqual(changed_state["lastLoggedAt"], initial_state["lastLoggedAt"])
            self.assertEqual(changed_state["signature"], "signature-b")

            should_log, periodic_state = (
                reporter._resolve_device_health_monitor_unavailable_event_log(
                    changed_payload,
                    state={"deviceUnavailableEventState": changed_state},
                    now=now + timedelta(hours=6),
                )
            )
            self.assertTrue(should_log)
            self.assertEqual(
                periodic_state["lastLoggedAt"],
                (now + timedelta(hours=6)).isoformat(),
            )

            should_log, reset_state = (
                reporter._resolve_device_health_monitor_unavailable_event_log(
                    None,
                    state={"deviceUnavailableEventState": periodic_state},
                    now=now + timedelta(hours=6, minutes=1),
                )
            )
            self.assertFalse(should_log)
            self.assertEqual(reset_state["unavailableCount"], 0)
            self.assertEqual(reset_state["signature"], "")

            should_log, recurrence_state = (
                reporter._resolve_device_health_monitor_unavailable_event_log(
                    changed_payload,
                    state={"deviceUnavailableEventState": reset_state},
                    now=now + timedelta(hours=6, minutes=2),
                )
            )
            self.assertTrue(should_log)
            self.assertEqual(recurrence_state["unavailableCount"], 2)

    def test_log_tracks_change_outside_sample_but_emits_at_periodic_interval(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        initial_summary = _unavailable_summary(21)
        logger = logging.getLogger("device-health-event-storage-test")

        with (
            patch.object(
                reporter.cs,
                "DEVICE_HEALTH_MONITOR_UNAVAILABLE_EVENT_SUMMARY_HOURS",
                6,
            ),
            patch.object(
                reporter,
                "_append_device_health_monitor_event",
                return_value=True,
            ) as append_mock,
        ):
            initial_state = reporter._log_device_health_monitor_run_events(
                initial_summary,
                now=now,
                logger=logger,
                state={},
            )
            self.assertEqual(
                [call.args[0] for call in append_mock.call_args_list],
                ["run_summary", "device_unavailable"],
            )
            initial_unavailable_payload = append_mock.call_args_list[1].args[1]
            self.assertEqual(
                initial_unavailable_payload["stateSignature"],
                initial_state["signature"],
            )
            self.assertEqual(
                initial_unavailable_payload["emissionReason"],
                "initial_snapshot",
            )

            changed_summary = copy.deepcopy(initial_summary)
            changed_summary["deviceResults"][20]["statusPayload"]["redis"][
                "availabilityReasons"
            ] = ["agent_stale"]
            append_mock.reset_mock()
            changed_state = reporter._log_device_health_monitor_run_events(
                changed_summary,
                now=now + timedelta(minutes=1),
                logger=logger,
                state={"deviceUnavailableEventState": initial_state},
            )
            self.assertEqual(
                [call.args[0] for call in append_mock.call_args_list],
                ["run_summary"],
            )
            self.assertNotEqual(changed_state["signature"], initial_state["signature"])

            append_mock.reset_mock()
            periodic_state = reporter._log_device_health_monitor_run_events(
                changed_summary,
                now=now + timedelta(hours=6),
                logger=logger,
                state={"deviceUnavailableEventState": changed_state},
            )
            self.assertEqual(
                [call.args[0] for call in append_mock.call_args_list],
                ["run_summary", "device_unavailable"],
            )
            periodic_payload = append_mock.call_args_list[1].args[1]
            self.assertEqual(periodic_payload["emissionReason"], "periodic_summary")
            self.assertEqual(periodic_payload["stateSignature"], periodic_state["signature"])

    def test_append_returns_success_and_failure(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        with tempfile.TemporaryDirectory() as temp_dir:
            event_path = Path(temp_dir) / "device_health_monitor_events-2026-07-23.jsonl"
            with patch.object(
                reporter,
                "_device_health_monitor_event_log_path",
                return_value=event_path,
            ):
                self.assertTrue(
                    reporter._append_device_health_monitor_event(
                        "run_summary",
                        {"value": 1},
                        now=now,
                    )
                )
            saved_event = json.loads(event_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_event["eventType"], "run_summary")
            self.assertEqual(saved_event["value"], 1)

            blocked_parent = Path(temp_dir) / "not-a-directory"
            blocked_parent.write_text("file", encoding="utf-8")
            blocked_path = blocked_parent / "events.jsonl"
            with patch.object(
                reporter,
                "_device_health_monitor_event_log_path",
                return_value=blocked_path,
            ):
                self.assertFalse(
                    reporter._append_device_health_monitor_event(
                        "run_summary",
                        {"value": 2},
                        now=now,
                    )
                )

    def test_failed_unavailable_append_does_not_advance_state(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        summary = _unavailable_summary(1)
        logger = logging.getLogger("device-health-event-storage-test")
        append_mock = Mock(side_effect=[True, False])

        with patch.object(
            reporter,
            "_append_device_health_monitor_event",
            append_mock,
        ):
            state = reporter._log_device_health_monitor_run_events(
                summary,
                now=now,
                logger=logger,
                state={},
            )

        self.assertEqual(
            [call.args[0] for call in append_mock.call_args_list],
            ["run_summary", "device_unavailable"],
        )
        self.assertEqual(state, {})


if __name__ == "__main__":
    unittest.main()
