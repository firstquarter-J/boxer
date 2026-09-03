import importlib
import os
import unittest
from unittest.mock import patch


class OpenCoreSettingsBoundaryTests(unittest.TestCase):
    def test_notion_personal_token_keeps_legacy_env_compatibility(self) -> None:
        settings = importlib.import_module("boxer.core.settings")
        original_personal = os.environ.get("NOTION_TOKEN_PERSONAL")
        original_legacy = os.environ.get("NOTION_TOKEN")

        # 기존 설치는 NOTION_TOKEN만 있어도 동작하고, 새 개인 토큰이 있으면 그 값을 우선한다.
        try:
            with patch.dict(os.environ, {"BOXER_SKIP_DOTENV": "true"}, clear=False):
                os.environ.pop("NOTION_TOKEN_PERSONAL", None)
                os.environ["NOTION_TOKEN"] = "legacy-token"
                reloaded = importlib.reload(settings)
                self.assertEqual(reloaded.NOTION_TOKEN_PERSONAL, "legacy-token")
                self.assertEqual(reloaded.NOTION_TOKEN, "legacy-token")

                os.environ["NOTION_TOKEN_PERSONAL"] = "personal-token"
                reloaded = importlib.reload(settings)
                self.assertEqual(reloaded.NOTION_TOKEN_PERSONAL, "personal-token")
                self.assertEqual(reloaded.NOTION_TOKEN, "personal-token")
        finally:
            if original_personal is None:
                os.environ.pop("NOTION_TOKEN_PERSONAL", None)
            else:
                os.environ["NOTION_TOKEN_PERSONAL"] = original_personal
            if original_legacy is None:
                os.environ.pop("NOTION_TOKEN", None)
            else:
                os.environ["NOTION_TOKEN"] = original_legacy
            importlib.reload(settings)

    def test_s3_region_has_no_implicit_default(self) -> None:
        settings = importlib.import_module("boxer.core.settings")
        original_region = os.environ.get("AWS_REGION")

        with patch.dict(os.environ, {"BOXER_SKIP_DOTENV": "true"}, clear=False):
            os.environ.pop("AWS_REGION", None)
            reloaded = importlib.reload(settings)
            self.assertEqual(reloaded.AWS_REGION, "")

        if original_region is None:
            os.environ.pop("AWS_REGION", None)
        else:
            os.environ["AWS_REGION"] = original_region
        importlib.reload(settings)

    def test_request_log_reads_legacy_env_without_exporting_audit_aliases(self) -> None:
        settings = importlib.import_module("boxer.core.settings")

        # 구 설치 env는 계속 읽되 새 코드가 request-audit module API에 다시
        # 의존하지 못하도록 출력 심볼은 REQUEST_LOG_* 하나만 유지한다.
        with patch.dict(
            os.environ,
            {
                "BOXER_SKIP_DOTENV": "true",
                "REQUEST_AUDIT_SQLITE_ENABLED": "true",
            },
            clear=False,
        ):
            os.environ.pop("REQUEST_LOG_SQLITE_ENABLED", None)
            reloaded = importlib.reload(settings)
            self.assertTrue(reloaded.REQUEST_LOG_SQLITE_ENABLED)
            for legacy_alias in (
                "REQUEST_AUDIT_SQLITE_ENABLED",
                "REQUEST_AUDIT_SQLITE_PATH",
                "REQUEST_AUDIT_SQLITE_TIMEOUT_SEC",
                "REQUEST_AUDIT_SQLITE_BUSY_TIMEOUT_MS",
                "REQUEST_AUDIT_SQLITE_INIT_ON_STARTUP",
                "REQUEST_AUDIT_TIMEZONE",
                "REQUEST_AUDIT_SQLITE_S3_BACKUP_ENABLED",
                "REQUEST_AUDIT_SQLITE_S3_BACKUP_BUCKET",
                "REQUEST_AUDIT_SQLITE_S3_OBJECT_KEY",
                "REQUEST_AUDIT_SQLITE_S3_BACKUP_PREFIX",
                "REQUEST_AUDIT_SQLITE_S3_STORAGE_CLASS",
                "REQUEST_AUDIT_SQLITE_S3_SERVER_SIDE_ENCRYPTION",
                "REQUEST_AUDIT_SQLITE_S3_RESTORE_ON_STARTUP",
            ):
                self.assertFalse(hasattr(reloaded, legacy_alias), legacy_alias)

        importlib.reload(settings)

    def test_open_core_s3_settings_are_domain_neutral(self) -> None:
        settings = importlib.import_module("boxer.core.settings")

        # 결과 길이·bucket·scan/tail 정책은 공개 connector가 아니라 설치자의 domain layer가 소유한다.
        for company_setting in (
            "DB_QUERY_MAX_RESULT_CHARS",
            "S3_ULTRASOUND_BUCKET",
            "S3_ULTRASOUND_BUCKET_OWNER_ID",
            "S3_LOG_BUCKET",
            "S3_QUERY_MAX_KEYS",
            "S3_QUERY_MAX_ITEMS",
            "S3_QUERY_MAX_RESULT_CHARS",
            "S3_LOG_TAIL_BYTES",
            "S3_LOG_TAIL_LINES",
            "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED",
            "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC",
            "DEVICE_NOTIFICATION_VIDEO_MIN_OBJECT_BYTES",
        ):
            self.assertFalse(hasattr(settings, company_setting), company_setting)


if __name__ == "__main__":
    unittest.main()
