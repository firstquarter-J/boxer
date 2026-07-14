import unittest
from datetime import datetime
from unittest.mock import patch
from zoneinfo import ZoneInfo

from boxer_company import device_health_sheet


class _FakeResponse:
    def __init__(self, payload: dict | None = None) -> None:
        self.payload = payload or {}

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return self.payload


class _FakeAuthorizedSession:
    def __init__(self, *, get_payload: dict | None = None) -> None:
        self.calls: list[dict] = []
        self.get_payload = get_payload or {}

    def get(self, url: str, **kwargs) -> _FakeResponse:
        self.calls.append({"method": "GET", "url": url, **kwargs})
        return _FakeResponse(self.get_payload)

    def post(self, url: str, **kwargs) -> _FakeResponse:
        self.calls.append({"method": "POST", "url": url, **kwargs})
        return _FakeResponse()


class DeviceHealthSheetTests(unittest.TestCase):
    def test_builds_fourteen_column_row_with_pending_status(self) -> None:
        detected_at = datetime(2026, 7, 13, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))

        rows = device_health_sheet._build_device_health_sheet_rows(
            [
                {
                    "device": "MB2-C00043",
                    "hospitalName": "수지미래산부인과의원(용인)",
                    "room": "1진료실",
                    "problemComponents": ["캡처보드", "LED", "용량"],
                    "issue": "캡처보드와 LED를 찾지 못했어",
                }
            ],
            detected_at=detected_at,
            slack_permalink="https://lifexio.slack.com/archives/C_HEALTH/p3000001",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(len(rows[0]), 14)
        self.assertIsInstance(rows[0][0], float)
        self.assertEqual(rows[0][1], "MB2-C00043")
        self.assertEqual(rows[0][2], "수지미래산부인과의원(용인)")
        self.assertEqual(rows[0][3], "1진료실")
        self.assertEqual(rows[0][4], "캡처보드 LED")
        self.assertEqual(rows[0][5], "캡처보드와 LED를 찾지 못했어")
        self.assertEqual(rows[0][6], "")
        self.assertEqual(rows[0][7], "대기")
        self.assertEqual(rows[0][12], "https://lifexio.slack.com/archives/C_HEALTH/p3000001")

    def test_appends_rows_with_adc_authorized_session(self) -> None:
        session = _FakeAuthorizedSession()
        detected_at = datetime(2026, 7, 13, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "spreadsheet-id",
            ),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_TAB_NAME",
                "Boxer 장애 감지 처리 현황",
            ),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TIMEOUT_SEC", 7),
        ):
            row_count = device_health_sheet._append_device_health_sheet_alerts(
                [
                    {
                        "device": "MB2-C00043",
                        "hospitalName": "테스트 병원",
                        "room": "1진료실",
                        "problemComponents": ["LED"],
                        "issue": "LED 이상",
                    }
                ],
                detected_at=detected_at,
                slack_permalink="https://lifexio.slack.com/archives/C_HEALTH/p3000001",
                authorized_session=session,
            )

        self.assertEqual(row_count, 1)
        self.assertEqual(len(session.calls), 1)
        call = session.calls[0]
        self.assertIn("/spreadsheet-id/values/", call["url"])
        self.assertTrue(call["url"].endswith(":append"))
        self.assertEqual(
            call["params"],
            {"valueInputOption": "USER_ENTERED", "insertDataOption": "OVERWRITE"},
        )
        self.assertEqual(call["json"]["majorDimension"], "ROWS")
        self.assertEqual(call["json"]["values"][0][1], "MB2-C00043")
        self.assertEqual(call["timeout"], 7)

    def test_stamps_completion_and_calculates_duration(self) -> None:
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    [46217.5, "장비1", "", "", "", "", "", "완료"],
                    [46217.5, "장비2", "", "", "", "", "", "완료", 46217.6],
                    [46217.5, "장비3", "", "", "", "", "", "이상없음"],
                    [
                        46217.5,
                        "장비4",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "완료",
                        46217.6,
                        "2시간 24분",
                        "",
                        "",
                        "",
                        144,
                    ],
                ]
            }
        )

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_SPREADSHEET_ID", "spreadsheet-id"),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "처리 현황"),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TIMEOUT_SEC", 7),
        ):
            stamped_count = device_health_sheet._stamp_device_health_sheet_status_times(
                now=datetime(2026, 7, 14, 17, 0, tzinfo=ZoneInfo("Asia/Seoul")),
                authorized_session=session,
            )

        self.assertEqual(stamped_count, 2)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(session.calls[0]["method"], "GET")
        update_call = session.calls[1]
        self.assertEqual(update_call["method"], "POST")
        self.assertTrue(update_call["url"].endswith("/values:batchUpdate"))
        update_data = update_call["json"]["data"]
        self.assertEqual(
            [item["range"] for item in update_data],
            ["'처리 현황'!I2", "'처리 현황'!J2", "'처리 현황'!N2", "'처리 현황'!J3", "'처리 현황'!N3"],
        )
        self.assertEqual(update_data[1]["values"], [["5시간"]])
        self.assertEqual(update_data[2]["values"], [[300.0]])
        self.assertEqual(update_data[3]["values"], [["2시간 24분"]])
        self.assertEqual(update_data[4]["values"], [[144.0]])
        self.assertEqual(update_call["json"]["valueInputOption"], "USER_ENTERED")

    def test_formats_duration_with_seconds(self) -> None:
        self.assertEqual(device_health_sheet._format_device_health_sheet_duration(0.5), "30초")
        self.assertEqual(
            device_health_sheet._format_device_health_sheet_duration(61.5),
            "1시간 1분 30초",
        )

    def test_skips_sheet_when_feature_is_disabled(self) -> None:
        session = _FakeAuthorizedSession()

        with patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", False):
            result = device_health_sheet._append_device_health_sheet_alerts(
                [],
                detected_at=datetime(2026, 7, 13, 9, 30),
                slack_permalink="",
                authorized_session=session,
            )

        self.assertIsNone(result)
        self.assertEqual(session.calls, [])


if __name__ == "__main__":
    unittest.main()
