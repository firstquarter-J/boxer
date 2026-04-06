import json
import unittest

from boxer_company_adapter_slack.company import _build_device_download_activity_input


class DeviceDownloadActivityLogTests(unittest.TestCase):
    def test_includes_slack_requester_in_description_and_detail_log(self) -> None:
        payload = _build_device_download_activity_input(
            record={
                "deviceName": "MB2-D00061",
                "deviceSeq": 61,
                "hospitalSeq": 101,
                "hospitalRoomSeq": 202,
                "hospitalName": "애플산부인과의원(안양)",
                "roomName": "1진료실",
                "fileNames": ["vydaenudm5vm04kd.motion.mp4"],
                "downloadLinks": [
                    {
                        "fileName": "vydaenudm5vm04kd.motion.mp4",
                        "url": "https://example.invalid/download/vydaenudm5vm04kd.motion.mp4",
                    }
                ],
            },
            barcode="68616387368",
            log_date="2026-03-28",
            question="68616387368 2026-03-28 파일 다운로드",
            user_id="U_ROSA",
            user_name="Rosa",
            channel_id="C_DOWNLOAD",
            thread_ts="1900000000.000100",
        )

        self.assertIn("요청자 [Rosa]", payload["description"])

        detail_log = json.loads(payload["detailLog"])

        self.assertEqual(detail_log["slackUserId"], "U_ROSA")
        self.assertEqual(detail_log["slackUserName"], "Rosa")
        self.assertEqual(detail_log["requestedBySlackUserId"], "U_ROSA")
        self.assertEqual(detail_log["requestedBySlackUserName"], "Rosa")

    def test_falls_back_to_slack_user_id_when_name_is_missing(self) -> None:
        payload = _build_device_download_activity_input(
            record={
                "deviceName": "MB2-D00061",
                "deviceSeq": 61,
                "hospitalSeq": 101,
                "hospitalRoomSeq": 202,
                "hospitalName": "애플산부인과의원(안양)",
                "roomName": "1진료실",
                "fileNames": [],
                "downloadLinks": [
                    {
                        "fileName": "vydaenudm5vm04kd.motion.mp4",
                        "url": "https://example.invalid/download/vydaenudm5vm04kd.motion.mp4",
                    }
                ],
            },
            barcode="68616387368",
            log_date="2026-03-28",
            question="68616387368 2026-03-28 파일 다운로드",
            user_id="U_ROSA",
            user_name=None,
            channel_id="C_DOWNLOAD",
            thread_ts="1900000000.000100",
        )

        self.assertIn("요청자 [U_ROSA]", payload["description"])

        detail_log = json.loads(payload["detailLog"])

        self.assertEqual(detail_log["slackUserName"], "")
        self.assertEqual(detail_log["requestedBySlackUserName"], "")


if __name__ == "__main__":
    unittest.main()
