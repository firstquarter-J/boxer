import gzip
import io
import json
import tempfile
import unittest
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

import boto3

from boxer_company import device_health_event_log
from boxer_company import settings as company_settings


_KST = ZoneInfo("Asia/Seoul")


class _MissingS3Object(Exception):
    def __init__(self) -> None:
        super().__init__("missing")
        self.response = {"Error": {"Code": "404"}}


class _PreconditionFailed(Exception):
    def __init__(self) -> None:
        super().__init__("precondition failed")
        self.response = {"Error": {"Code": "412"}}


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
        if kwargs.get("IfNoneMatch") == "*" and str(kwargs["Key"]) in self.objects:
            raise _PreconditionFailed()
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


class DeviceHealthEventStorageTests(unittest.TestCase):
    def _archive_settings(self, *, retention_days: int = 14) -> ExitStack:
        stack = ExitStack()
        stack.enter_context(
            patch.object(
                company_settings,
                "DEVICE_HEALTH_MONITOR_EVENT_LOG_RETENTION_DAYS",
                retention_days,
            )
        )
        stack.enter_context(
            patch.object(
                company_settings,
                "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET",
                "boxer-kr",
            )
        )
        stack.enter_context(
            patch.object(
                company_settings,
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

            result = device_health_event_log.archive_device_health_monitor_event_logs(
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
            # 신규 key는 조건부 PUT 뒤 검증 HEAD 한 번만 수행해야 한다.
            self.assertEqual(upload["IfNoneMatch"], "*")
            self.assertEqual(len(s3_client.head_calls), 1)

    def test_create_only_put_uses_raw_header_with_botocore_model(self) -> None:
        client = boto3.client(
            "s3",
            region_name="ap-northeast-2",
            aws_access_key_id="test-access-key",
            aws_secret_access_key="test-secret-key",
        )
        body = io.BytesIO(b"archive-body")
        request_headers: list[dict[str, object]] = []

        class _SuccessfulHttpResponse:
            status_code = 200

        def capture_request(operation_model, request_dict, request_context):
            del operation_model, request_context
            request_headers.append(dict(request_dict["headers"]))
            return _SuccessfulHttpResponse(), {"ETag": '"test-etag"'}

        # 설치된 botocore service model의 허용 인자와 무관하게 실제
        # client validation을 통과하고 서명 직전 조건부 헤더가 들어간다.
        with patch.object(client, "_make_request", side_effect=capture_request):
            device_health_event_log._put_archive_object_create_only(
                client,
                body=body,
                put_parameters={
                    "Bucket": "boxer-kr",
                    "Key": "device-health-monitor/events/test.jsonl.gz",
                    "Body": body,
                    "ContentLength": len(b"archive-body"),
                    "Metadata": {"source-sha256": "test-digest"},
                },
            )
            client.put_object(
                Bucket="boxer-kr",
                Key="unrelated-object",
                Body=b"other",
            )

        self.assertEqual(request_headers[0]["If-None-Match"], "*")
        self.assertNotIn("If-None-Match", request_headers[1])

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
                    result = device_health_event_log.archive_device_health_monitor_event_logs(
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

            result = device_health_event_log.archive_device_health_monitor_event_logs(
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

            result = device_health_event_log.archive_device_health_monitor_event_logs(
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
                first_result = device_health_event_log.archive_device_health_monitor_event_logs(
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
                retry_result = device_health_event_log.archive_device_health_monitor_event_logs(
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
            result = device_health_event_log.archive_device_health_monitor_event_logs(
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
            result = device_health_event_log.archive_device_health_monitor_event_logs(
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

    def test_append_returns_success_and_failure(self) -> None:
        now = datetime(2026, 7, 23, 9, 0, tzinfo=_KST)
        with tempfile.TemporaryDirectory() as temp_dir:
            event_path = Path(temp_dir) / "device_health_monitor_events-2026-07-23.jsonl"
            self.assertTrue(
                device_health_event_log.append_device_health_monitor_event(
                    "run_summary",
                    {"value": 1},
                    now=now,
                    log_path=event_path,
                )
            )
            saved_event = json.loads(event_path.read_text(encoding="utf-8"))
            self.assertEqual(saved_event["eventType"], "run_summary")
            self.assertEqual(saved_event["value"], 1)

            blocked_parent = Path(temp_dir) / "not-a-directory"
            blocked_parent.write_text("file", encoding="utf-8")
            blocked_path = blocked_parent / "events.jsonl"
            self.assertFalse(
                device_health_event_log.append_device_health_monitor_event(
                    "run_summary",
                    {"value": 2},
                    now=now,
                    log_path=blocked_path,
                )
            )


if __name__ == "__main__":
    unittest.main()
