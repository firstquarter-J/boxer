import json
import unittest

from boxer_company_adapter_slack.company import _build_device_download_activity_input
from boxer_company_adapter_slack.device_activity import (
    _render_device_download_dm_link_texts,
    _render_device_download_dm_text,
)


class DeviceDownloadActivityLogTests(unittest.TestCase):
    def test_download_dm_summary_omits_long_urls_and_splits_link_messages(self) -> None:
        long_url_1 = "https://example.invalid/temp/a.motion.mp4?" + "X-Amz-Security-Token=" + "a" * 3500
        long_url_2 = "https://example.invalid/temp/b.motion.mp4?" + "X-Amz-Security-Token=" + "b" * 3500
        records = [
            {
                "deviceName": "MB2-D00061",
                "hospitalName": "애플산부인과의원(안양)",
                "roomName": "1진료실",
                "fileNames": ["a.motion.mp4", "b.motion.mp4"],
                "downloadLinks": [
                    {"fileName": "a.motion.mp4", "url": long_url_1},
                    {"fileName": "b.motion.mp4", "url": long_url_2},
                ],
            }
        ]

        # S3 presigned URL은 길어서 요약 DM에 직접 넣지 않고 파일별 메시지로 분리한다.
        summary = _render_device_download_dm_text("68616387368", "2026-03-28", records)
        link_messages = _render_device_download_dm_link_texts(records)

        self.assertIn("• 다운로드 링크: `2개` (1시간, 파일별 별도 DM)", summary)
        self.assertNotIn(long_url_1, summary)
        self.assertNotIn(long_url_2, summary)
        self.assertEqual(len(link_messages), 2)
        self.assertIn(f"🎣 <{long_url_1}|a.motion.mp4>", link_messages[0])
        self.assertIn(f"🎣 <{long_url_2}|b.motion.mp4>", link_messages[1])

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
