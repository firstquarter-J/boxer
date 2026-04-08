import unittest
from unittest.mock import patch

from boxer_company.routers.mda_graphql import _get_mda_latest_device_version


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


if __name__ == "__main__":
    unittest.main()
