import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from boxer_adapter_slack import set_request_log_route
from boxer.observability.request_log import (
    _backup_request_log_to_s3,
    _initialize_request_log_storage,
    _list_request_log_recent,
    _save_request_log_record,
)


class RequestLogRouteSetterTests(unittest.TestCase):
    def test_sets_handler_type_in_request_log_context(self) -> None:
        payload = {"request_log": {}}

        set_request_log_route(
            payload,
            "llm_freeform",
            route_mode="claude",
            handler_type="llm_freeform",
        )

        self.assertEqual(payload["request_log"]["route_name"], "llm_freeform")
        self.assertEqual(payload["request_log"]["route_mode"], "claude")
        self.assertEqual(payload["request_log"]["handler_type"], "llm_freeform")


class RequestLogHandlerTypePersistenceTests(unittest.TestCase):
    def test_persists_handler_type_column(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "request_log.db"

            _save_request_log_record(
                {
                    "sourcePlatform": "slack",
                    "workspaceId": "T123",
                    "eventType": "app_mention",
                    "routeName": "llm_freeform",
                    "routeMode": "claude",
                    "handlerType": "llm_freeform",
                    "status": "handled",
                    "userId": "U123",
                    "channelId": "C123",
                    "threadId": "1730000000.000100",
                    "messageId": "1730000000.000100",
                    "requestText": "@Boxer 자유대화 테스트",
                    "normalizedQuestion": "자유대화 테스트",
                },
                db_path=db_path,
            )

            result = _list_request_log_recent(db_path=db_path, limit=1)
            row = result["rows"][0]

            self.assertEqual(row["routeName"], "llm_freeform")
            self.assertEqual(row["routeMode"], "claude")
            self.assertEqual(row["handlerType"], "llm_freeform")


class RequestLogBackupSafetyTests(unittest.TestCase):
    def test_backup_does_not_create_a_missing_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "request_log.db"

            with self.assertRaises(FileNotFoundError):
                _backup_request_log_to_s3(
                    db_path=db_path,
                    bucket="request-log-backup",
                    s3_client=object(),
                )

            self.assertFalse(db_path.exists())

    def test_required_restore_fails_closed_when_remote_is_unavailable(
        self,
    ) -> None:
        class UnavailableS3Client:
            def head_object(self, **_kwargs: object) -> object:
                raise TimeoutError("unavailable")

        for precreate_empty in (False, True):
            with self.subTest(precreate_empty=precreate_empty):
                with tempfile.TemporaryDirectory() as tmpdir:
                    db_path = Path(tmpdir) / "request_log.db"
                    if precreate_empty:
                        db_path.touch()
                    settings_path = "boxer.observability.request_log.s"
                    with (
                        patch(
                            f"{settings_path}.REQUEST_LOG_SQLITE_ENABLED",
                            True,
                        ),
                        patch(
                            f"{settings_path}.REQUEST_LOG_SQLITE_S3_RESTORE_ON_STARTUP",
                            True,
                        ),
                        patch(
                            f"{settings_path}.REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET",
                            "request-log-backup",
                        ),
                        patch(
                            f"{settings_path}.REQUEST_LOG_SQLITE_S3_OBJECT_KEY",
                            "request-log.db",
                        ),
                    ):
                        with self.assertRaises(RuntimeError):
                            _initialize_request_log_storage(
                                db_path=db_path,
                                s3_client=UnavailableS3Client(),
                            )

                    self.assertEqual(
                        db_path.stat().st_size if db_path.exists() else 0,
                        0,
                    )


if __name__ == "__main__":
    unittest.main()
