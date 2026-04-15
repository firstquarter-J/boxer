import importlib
import os
import unittest
from unittest.mock import patch


class OpenCoreSettingsBoundaryTests(unittest.TestCase):
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
