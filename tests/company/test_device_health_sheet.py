import unittest
from datetime import datetime
from unittest.mock import Mock, patch
from urllib.parse import unquote
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
    def test_builds_fifteen_column_row_with_pending_status(self) -> None:
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
        self.assertEqual(len(rows[0]), 15)
        self.assertIsInstance(rows[0][0], float)
        self.assertEqual(rows[0][1], "MB2-C00043")
        self.assertEqual(rows[0][2], "수지미래산부인과의원(용인)")
        self.assertEqual(rows[0][3], "1진료실")
        self.assertEqual(rows[0][4], "캡처보드 LED")
        self.assertEqual(rows[0][5], "캡처보드와 LED를 찾지 못했어")
        self.assertEqual(rows[0][6], "")
        self.assertEqual(rows[0][7], "")
        self.assertEqual(rows[0][8], "대기")
        self.assertEqual(rows[0][9], "")
        self.assertIn('INDIRECT("I"&ROW())="완료"', rows[0][10])
        self.assertIn('INDIRECT("K"&ROW())=0', rows[0][10])
        self.assertIn("LET(total", rows[0][11])
        self.assertIn('INDIRECT("K"&ROW())-INDIRECT("A"&ROW())', rows[0][11])
        self.assertEqual(rows[0][12], "")
        self.assertEqual(rows[0][13], "")
        self.assertEqual(rows[0][14], "https://lifexio.slack.com/archives/C_HEALTH/p3000001")

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
        self.assertTrue(unquote(call["url"]).endswith("'Boxer 장애 감지 처리 현황'!A:O:append"))
        self.assertTrue(call["json"]["values"][0][10].startswith("=IF("))
        self.assertTrue(call["json"]["values"][0][11].startswith("=IF("))
        self.assertEqual(call["timeout"], 7)

    def test_loads_bottommost_captureboard_incident_for_each_device(self) -> None:
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    [],
                    ["MB2-C00043", "병원", "진료실", "LED", "LED를 찾지 못했어"],
                    [
                        "MB2-C00172",
                        "삼성미래산부인과(부천)",
                        "최정금 원장",
                        "",
                        "녹화 파일 증가 정지가 240초 동안 지속됐어",
                        "Leon",
                        "MDA 모니터링",
                        "대기",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "https://slack.example/recording-stall",
                    ],
                    [
                        "MB2-C00172",
                        "삼성미래산부인과(부천)",
                        "최정금 원장",
                        "캡처보드",
                        "분할 녹화 파일 병합 실패",
                        "Leon",
                        "MDA 모니터링",
                        "대기",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "https://slack.example/merge-error",
                    ],
                    [
                        "MB2-C00999",
                        "테스트병원",
                        "진료실",
                        "캡처보드",
                        "녹화 파일 업로드 실패",
                        "Leon",
                        "MDA 모니터링",
                        "대기",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "https://slack.example/upload-error",
                    ],
                    [
                        "MB2-C01263",
                        "웰하이여성아동병원(부산)",
                        "정밀초음파실",
                        "",
                        "비디오 장치를 찾지 못했어",
                        "Leon",
                        "CS 인입",
                        "대기",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "https://slack.example/video-device",
                    ],
                    [
                        "MB2-C00172",
                        "삼성미래산부인과(부천)",
                        "최정금 원장",
                        "캡처보드",
                        "캡처보드 USB를 찾지 못했어",
                        "Leon",
                        "선제연락",
                        "완료",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "https://slack.example/captureboard-latest",
                    ],
                ]
            }
        )

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "spreadsheet/id",
            ),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "TA's 현황"),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TIMEOUT_SEC", 7),
        ):
            incidents = device_health_sheet._load_device_health_sheet_captureboard_incidents(
                authorized_session=session
            )

        self.assertEqual(
            incidents,
            {
                "MB2-C00172": {
                    "deviceName": "MB2-C00172",
                    "status": "완료",
                    "slackPermalink": "https://slack.example/captureboard-latest",
                    "rowNumber": 8,
                },
                "MB2-C01263": {
                    "deviceName": "MB2-C01263",
                    "status": "대기",
                    "slackPermalink": "https://slack.example/video-device",
                    "rowNumber": 7,
                },
            },
        )
        self.assertEqual(len(session.calls), 1)
        call = session.calls[0]
        self.assertEqual(call["method"], "GET")
        self.assertTrue(
            unquote(call["url"]).endswith("/spreadsheet/id/values/'TA''s 현황'!B2:O")
        )
        self.assertEqual(
            call["params"],
            {"majorDimension": "ROWS", "valueRenderOption": "FORMATTED_VALUE"},
        )
        self.assertEqual(call["timeout"], 7)

    def test_loads_short_captureboard_row_with_empty_status_and_permalink(self) -> None:
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    [""],
                    ["MB2-C00043", "병원", "진료실", "캡처보드"],
                ]
            }
        )

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "spreadsheet-id",
            ),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "현황"),
        ):
            incidents = device_health_sheet._load_device_health_sheet_captureboard_incidents(
                authorized_session=session
            )

        self.assertEqual(
            incidents,
            {
                "MB2-C00043": {
                    "deviceName": "MB2-C00043",
                    "status": "",
                    "slackPermalink": "",
                    "rowNumber": 3,
                }
            },
        )

    def test_captureboard_incident_loader_includes_stall_and_excludes_processing_errors(
        self,
    ) -> None:
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    [
                        "MB2-STALL",
                        "병원",
                        "진료실",
                        "",
                        "녹화 파일 증가 정지가 240초 동안 지속됐어",
                        "Leon",
                        "MDA 모니터링",
                        "대기",
                    ],
                    [
                        "MB2-MERGE",
                        "병원",
                        "진료실",
                        "캡처보드",
                        "분할 녹화 파일 병합 실패",
                        "Leon",
                        "MDA 모니터링",
                        "대기",
                    ],
                    [
                        "MB2-UPLOAD",
                        "병원",
                        "진료실",
                        "캡처보드",
                        "녹화 파일 upload 실패",
                        "Leon",
                        "MDA 모니터링",
                        "대기",
                    ],
                    [
                        "MB2-FFMPEG",
                        "병원",
                        "진료실",
                        "캡처보드",
                        "FFmpeg exited with code 1",
                        "Leon",
                        "MDA 모니터링",
                        "대기",
                    ],
                ]
            }
        )

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "spreadsheet-id",
            ),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "현황"),
        ):
            incidents = device_health_sheet._load_device_health_sheet_captureboard_incidents(
                authorized_session=session
            )

        self.assertEqual(set(incidents or {}), {"MB2-STALL"})

    def test_captureboard_incident_loader_skips_sheet_when_feature_is_disabled(self) -> None:
        session = _FakeAuthorizedSession()

        with patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", False):
            result = device_health_sheet._load_device_health_sheet_captureboard_incidents(
                authorized_session=session
            )

        self.assertIsNone(result)
        self.assertEqual(session.calls, [])

    def test_captureboard_incident_loader_requires_sheet_configuration(self) -> None:
        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_SPREADSHEET_ID", ""),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "현황"),
        ):
            with self.assertRaises(ValueError):
                device_health_sheet._load_device_health_sheet_captureboard_incidents(
                    authorized_session=_FakeAuthorizedSession()
                )

    def test_captureboard_incident_loader_propagates_http_error(self) -> None:
        response = Mock()
        response.raise_for_status.side_effect = RuntimeError("Sheets HTTP error")
        session = Mock()
        session.get.return_value = response

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "spreadsheet-id",
            ),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "현황"),
        ):
            with self.assertRaisesRegex(RuntimeError, "Sheets HTTP error"):
                device_health_sheet._load_device_health_sheet_captureboard_incidents(
                    authorized_session=session
                )

        response.json.assert_not_called()

    def test_captureboard_incident_loader_propagates_json_error(self) -> None:
        response = Mock()
        response.json.side_effect = ValueError("invalid JSON")
        session = Mock()
        session.get.return_value = response

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "spreadsheet-id",
            ),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "현황"),
        ):
            with self.assertRaisesRegex(ValueError, "invalid JSON"):
                device_health_sheet._load_device_health_sheet_captureboard_incidents(
                    authorized_session=session
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
