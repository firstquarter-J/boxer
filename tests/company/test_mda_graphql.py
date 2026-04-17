import unittest
from unittest.mock import patch

from boxer_company.routers.mda_graphql import _get_mda_latest_device_version, _normalize_mda_device_detail


class MdaLatestDeviceVersionTests(unittest.TestCase):
    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql")
    def test_picks_highest_visible_semver_even_when_auto_update_is_false(
        self,
        mock_execute_mda_graphql,
    ) -> None:
        mock_execute_mda_graphql.return_value = {
            "deviceVersions": [
                {"versionName": "legacy", "autoUpdate": True, "visibleFlag": True},
                {"versionName": "2.11.299", "autoUpdate": False, "visibleFlag": True},
                {"versionName": "2.11.300", "autoUpdate": False, "visibleFlag": True},
                {"versionName": "3.0.0-beta", "autoUpdate": True, "visibleFlag": True},
                {"versionName": "3.2.10", "autoUpdate": True, "visibleFlag": False},
            ]
        }

        result = _get_mda_latest_device_version()

        self.assertEqual(result["versionName"], "2.11.300")
        self.assertTrue(result["visibleFlag"])

    @patch("boxer_company.routers.mda_graphql._execute_mda_graphql")
    def test_raises_when_no_semver_version_exists(
        self,
        mock_execute_mda_graphql,
    ) -> None:
        mock_execute_mda_graphql.return_value = {
            "deviceVersions": [
                {"versionName": "legacy", "autoUpdate": True, "visibleFlag": True},
                {"versionName": "", "autoUpdate": False, "visibleFlag": True},
            ]
        }

        with self.assertRaisesRegex(RuntimeError, "최신 박스 버전"):
            _get_mda_latest_device_version()


class MdaDeviceDetailNormalizationTests(unittest.TestCase):
    def test_normalizes_optional_device_config_booleans(self) -> None:
        result = _normalize_mda_device_detail(
            {
                "deviceName": "MB2-C00419",
                "version": "2.11.300",
                "cfg1_use_diary_capture": 1,
                "cfg1_check_invalid_barcode": 0,
                "deviceState": {},
                "hospital": {},
                "hospitalRoom": {},
                "agentState": {},
            },
            device_name="MB2-C00419",
        )

        self.assertTrue(result["useDiaryCapture"])
        self.assertFalse(result["checkInvalidBarcode"])


if __name__ == "__main__":
    unittest.main()
