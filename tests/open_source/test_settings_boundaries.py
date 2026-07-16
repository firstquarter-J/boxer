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


if __name__ == "__main__":
    unittest.main()
