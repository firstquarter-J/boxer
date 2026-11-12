import json
import unittest
from datetime import datetime
from unittest.mock import patch
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
    def __init__(
        self,
        *,
        get_payload: dict | None = None,
        get_payloads: list[dict] | None = None,
    ) -> None:
        self.calls: list[dict] = []
        self.get_payload = get_payload or {}
        self.get_payloads = list(get_payloads or [])

    def get(self, url: str, **kwargs) -> _FakeResponse:
        self.calls.append({"method": "GET", "url": url, **kwargs})
        if self.get_payloads:
            return _FakeResponse(self.get_payloads.pop(0))
        return _FakeResponse(self.get_payload)

    def post(self, url: str, **kwargs) -> _FakeResponse:
        self.calls.append({"method": "POST", "url": url, **kwargs})
        return _FakeResponse()

    def put(self, url: str, **kwargs) -> _FakeResponse:
        self.calls.append({"method": "PUT", "url": url, **kwargs})
        return _FakeResponse()


def _sms_tracking_metadata(
    *,
    device_name: str,
    issue: str,
    permalink: str,
    group_id: str,
    accepted_at: str = "2026-07-13T00:30:00Z",
    message_id: str = "",
) -> str:
    metadata = {
        "v": 1,
        "g": group_id,
        "k": device_health_sheet._device_health_sheet_sms_tracking_key(
            device_name,
            issue,
            permalink,
        ),
        "t": accepted_at,
    }
    if message_id:
        metadata["m"] = message_id
    return json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))


def _sms_sheet_row(
    *,
    device_name: str = "",
    issue: str = "",
    permalink: str = "",
    sms_status: str = "",
    tracking_metadata: str = "",
) -> list[str]:
    # B:T 응답의 실제 열 위치를 유지해 T가 대상 행과 분리된 경우도 표현한다.
    row = [""] * 19
    row[0] = device_name
    row[4] = issue
    row[6] = sms_status
    row[17] = permalink
    row[18] = tracking_metadata
    return row



class DeviceHealthSheetTests(unittest.TestCase):
    def test_builds_twenty_column_row_without_ta_managed_values(self) -> None:
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
        self.assertEqual(len(rows[0]), 20)
        self.assertIsInstance(rows[0][0], float)
        self.assertEqual(rows[0][1], "MB2-C00043")
        self.assertEqual(rows[0][2], "수지미래산부인과의원(용인)")
        self.assertEqual(rows[0][3], "1진료실")
        self.assertEqual(rows[0][4], "캡처보드 LED")
        self.assertEqual(rows[0][5], "캡처보드와 LED를 찾지 못했어")
        self.assertEqual(rows[0][6], "")
        self.assertEqual(rows[0][7], "미발송")
        self.assertEqual(rows[0][8], "")
        self.assertEqual(rows[0][9], "")
        self.assertEqual(rows[0][10], "")
        self.assertEqual(rows[0][11], "")
        self.assertEqual(rows[0][12], "")
        self.assertEqual(rows[0][13], "")
        self.assertEqual(rows[0][14], "")
        self.assertEqual(rows[0][15], "")
        self.assertEqual(rows[0][16], "")
        self.assertEqual(rows[0][17], "")
        self.assertEqual(rows[0][18], "https://lifexio.slack.com/archives/C_HEALTH/p3000001")
        self.assertEqual(rows[0][19], "")

    def test_maps_actual_auto_sms_result_to_sheet_status(self) -> None:
        detected_at = datetime(2026, 7, 13, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))
        scenarios = (
            (
                {
                    "smsDeliveryStatus": "accepted",
                    "smsGroupId": "G123",
                    "smsMessageId": "M123",
                },
                "접수됨",
                "G123",
            ),
            ({"smsDeliveryStatus": "delivered"}, "수신 완료", ""),
            ({"smsDeliveryStatus": "delivery_failed"}, "수신 실패", ""),
            ({"smsDeliveryStatus": "request_failed"}, "발송 실패", ""),
            ({"smsDeliveryStatus": "not_sent"}, "미발송", ""),
            ({"smsDeliveryStatus": "confirm_required"}, "확인 필요", ""),
            (
                {"smsStatusText": "문자 발송 접수"},
                "확인 필요",
                "",
            ),
            (
                {"smsStatusText": "문자 자동발송 완료"},
                "확인 필요",
                "",
            ),
        )

        for sms_fields, expected_status, expected_group_id in scenarios:
            with self.subTest(sms_fields=sms_fields):
                rows = device_health_sheet._build_device_health_sheet_rows(
                    [
                        {
                            "device": "MB2-C00043",
                            "hospitalName": "테스트 병원",
                            "room": "1진료실",
                            "issue": "캡처보드 이상",
                            **sms_fields,
                        }
                    ],
                    detected_at=detected_at,
                    slack_permalink="https://lifexio.slack.com/archives/C_HEALTH/p3000001",
                )

                self.assertEqual(rows[0][7], expected_status)
                if expected_group_id:
                    tracking_metadata = json.loads(rows[0][19])
                    self.assertEqual(
                        tracking_metadata,
                        {
                            "v": 1,
                            "g": "G123",
                            "k": device_health_sheet._device_health_sheet_sms_tracking_key(
                                "MB2-C00043",
                                "캡처보드 이상",
                                "https://lifexio.slack.com/archives/C_HEALTH/p3000001",
                            ),
                            "t": "2026-07-13T00:30:00Z",
                            "m": "M123",
                        },
                    )
                    self.assertNotEqual(rows[0][19], expected_group_id)
                else:
                    self.assertEqual(rows[0][19], "")

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
        self.assertTrue(unquote(call["url"]).endswith("'Boxer 장애 감지 처리 현황'!A:T:append"))
        self.assertEqual(call["json"]["values"][0][11], "")
        self.assertEqual(call["json"]["values"][0][12], "")
        self.assertEqual(call["timeout"], 7)

    def test_append_retry_reads_delivery_hash_and_skips_committed_row(self) -> None:
        class _CommittedTimeoutSession(_FakeAuthorizedSession):
            def __init__(self) -> None:
                super().__init__()
                self.committed_rows: list[list[object]] = []
                self.post_count = 0

            def get(self, url: str, **kwargs) -> _FakeResponse:
                self.calls.append({"method": "GET", "url": url, **kwargs})
                # Sheets B:T 조회와 같은 열 offset으로 방금 반영된 A:T 행을 돌려준다.
                return _FakeResponse(
                    {"values": [row[1:20] for row in self.committed_rows]}
                )

            def post(self, url: str, **kwargs) -> _FakeResponse:
                self.calls.append({"method": "POST", "url": url, **kwargs})
                self.post_count += 1
                self.committed_rows.extend(kwargs["json"]["values"])
                raise TimeoutError("response lost after append commit")

        session = _CommittedTimeoutSession()
        detected_at = datetime(2026, 7, 13, 9, 30, tzinfo=ZoneInfo("Asia/Seoul"))
        item = {
            "device": "MB2-C00043",
            "hospitalName": "테스트 병원",
            "room": "1진료실",
            "problemComponents": ["LED"],
            "issue": "LED 이상",
            "sheetDeliveryId": "device_health_monitor:stable-delivery",
        }

        with (
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_ENABLED", True),
            patch.object(
                device_health_sheet.cs,
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "spreadsheet-id",
            ),
            patch.object(device_health_sheet.cs, "DEVICE_HEALTH_SHEET_TAB_NAME", "현황"),
        ):
            with self.assertRaisesRegex(TimeoutError, "response lost"):
                device_health_sheet._append_device_health_sheet_alerts(
                    [item],
                    detected_at=detected_at,
                    slack_permalink="https://slack.example/stable",
                    authorized_session=session,
                )
            reconciled_count = device_health_sheet._append_device_health_sheet_alerts(
                [item],
                detected_at=detected_at,
                slack_permalink="https://slack.example/stable",
                authorized_session=session,
            )

        self.assertEqual(reconciled_count, 0)
        self.assertEqual(session.post_count, 1)
        self.assertEqual(len(session.committed_rows), 1)
        metadata = json.loads(session.committed_rows[0][19])
        self.assertEqual(metadata["v"], 1)
        self.assertEqual(len(metadata["d"]), 64)
        self.assertNotIn(item["sheetDeliveryId"], session.committed_rows[0][19])

    def test_loads_only_pending_sms_deliveries_with_group_id(self) -> None:
        device_one = "MB2-C00043"
        issue_one = "캡처보드 이상"
        permalink_one = "https://slack.example/one"
        device_two = "MB2-C00044"
        issue_two = "LED 이상"
        permalink_two = "https://slack.example/two"
        completed_tracking = _sms_tracking_metadata(
            device_name="MB2-C00045",
            issue="오디오 이상",
            permalink="https://slack.example/completed",
            group_id="G124",
        )
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    _sms_sheet_row(
                        device_name=device_one,
                        issue=issue_one,
                        permalink=permalink_one,
                        sms_status="접수됨",
                    ),
                    _sms_sheet_row(
                        device_name="MB2-C00045",
                        issue="오디오 이상",
                        permalink="https://slack.example/completed",
                        sms_status="수신 완료",
                        tracking_metadata=completed_tracking,
                    ),
                    _sms_sheet_row(
                        tracking_metadata=_sms_tracking_metadata(
                            device_name=device_one,
                            issue=issue_one,
                            permalink=permalink_one,
                            group_id="G123",
                            message_id="M123",
                        )
                    ),
                    _sms_sheet_row(
                        device_name=device_two,
                        issue=issue_two,
                        permalink=permalink_two,
                        sms_status="접수됨",
                    ),
                    _sms_sheet_row(
                        tracking_metadata=_sms_tracking_metadata(
                            device_name=device_two,
                            issue=issue_two,
                            permalink=permalink_two,
                            group_id="G125",
                            accepted_at="2026-07-13T00:31:00Z",
                        )
                    ),
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
            pending = (
                device_health_sheet._load_device_health_sheet_pending_sms_deliveries(
                    authorized_session=session
                )
            )

        self.assertEqual(
            pending,
            [
                {
                    "rowNumber": 2,
                    "groupId": "G123",
                    "acceptedAt": "2026-07-13T00:30:00Z",
                    "messageId": "M123",
                },
                {
                    "rowNumber": 5,
                    "groupId": "G125",
                    "acceptedAt": "2026-07-13T00:31:00Z",
                },
            ],
        )
        self.assertTrue(unquote(session.calls[0]["url"]).endswith("'현황'!B2:T"))

    def test_loads_all_exact_sms_delivery_matches_for_outbox(self) -> None:
        accepted_target = {
            "device_name": "MB2-C00043",
            "issue": "캡처보드 이상",
            "permalink": "https://slack.example/accepted",
        }
        completed_target = {
            "device_name": "MB2-C00044",
            "issue": "LED 이상",
            "permalink": "https://slack.example/completed",
        }
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    _sms_sheet_row(
                        **accepted_target,
                        sms_status="접수됨",
                    ),
                    _sms_sheet_row(
                        **completed_target,
                        sms_status="수신 완료",
                    ),
                    _sms_sheet_row(
                        tracking_metadata=_sms_tracking_metadata(
                            **accepted_target,
                            group_id="G123",
                        )
                    ),
                    _sms_sheet_row(
                        tracking_metadata=_sms_tracking_metadata(
                            **completed_target,
                            group_id="G123",
                        )
                    ),
                    _sms_sheet_row(
                        device_name="MB2-LEGACY",
                        issue="과거 형식",
                        permalink="https://slack.example/legacy",
                        sms_status="접수됨",
                        tracking_metadata="RAW-GROUP-ID",
                    ),
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
            matches = (
                device_health_sheet._load_device_health_sheet_sms_delivery_matches(
                    authorized_session=session
                )
            )

        self.assertEqual(
            [
                (match["rowNumber"], match["groupId"], match["smsStatus"])
                for match in matches or []
            ],
            [
                (2, "G123", "접수됨"),
                (3, "G123", "수신 완료"),
            ],
        )

    def test_finds_tracking_group_id_without_resolved_identity(self) -> None:
        orphaned_tracking = _sms_tracking_metadata(
            device_name="MB2-ORPHAN",
            issue="대상 행과 분리된 이슈",
            permalink="https://slack.example/orphan",
            group_id="G-ORPHAN",
        )
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    _sms_sheet_row(tracking_metadata=orphaned_tracking),
                    _sms_sheet_row(tracking_metadata=orphaned_tracking),
                    _sms_sheet_row(tracking_metadata="G-LEGACY-RAW"),
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
            self.assertTrue(
                device_health_sheet._has_device_health_sheet_sms_tracking_group_id(
                    "G-ORPHAN",
                    authorized_session=session,
                )
            )
            self.assertFalse(
                device_health_sheet._has_device_health_sheet_sms_tracking_group_id(
                    "G-LEGACY-RAW",
                    authorized_session=session,
                )
            )

        self.assertEqual([call["method"] for call in session.calls], ["GET", "GET"])

    def test_updates_sms_status_only_when_group_id_still_matches(self) -> None:
        device_name = "MB2-C00043"
        issue = "캡처보드 이상"
        permalink = "https://slack.example/one"
        tracking_metadata = _sms_tracking_metadata(
            device_name=device_name,
            issue=issue,
            permalink=permalink,
            group_id="G123",
        )
        pending_rows = [
            _sms_sheet_row(
                device_name=device_name,
                issue=issue,
                permalink=permalink,
                sms_status="접수됨",
            ),
            _sms_sheet_row(tracking_metadata=tracking_metadata),
        ]
        completed_rows = [
            _sms_sheet_row(
                device_name=device_name,
                issue=issue,
                permalink=permalink,
                sms_status="수신 완료",
            ),
            _sms_sheet_row(tracking_metadata=tracking_metadata),
        ]
        session = _FakeAuthorizedSession(
            get_payloads=[
                {"values": pending_rows},
                {"values": pending_rows},
                {"values": completed_rows},
            ]
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
            updated = (
                device_health_sheet._update_device_health_sheet_sms_status_by_group_id(
                    row_number=2,
                    group_id="G123",
                    sms_status="수신 완료",
                    authorized_session=session,
                )
            )

        self.assertTrue(updated)
        self.assertEqual(
            [call["method"] for call in session.calls],
            ["GET", "GET", "PUT", "GET"],
        )
        self.assertTrue(unquote(session.calls[0]["url"]).endswith("'현황'!B2:T"))
        self.assertTrue(unquote(session.calls[1]["url"]).endswith("'현황'!B2:T"))
        self.assertTrue(unquote(session.calls[2]["url"]).endswith("'현황'!H2"))
        self.assertEqual(session.calls[2]["json"]["values"], [["수신 완료"]])
        self.assertTrue(unquote(session.calls[3]["url"]).endswith("'현황'!B2:T"))

    def test_finds_target_again_when_r_and_target_rows_move_after_scan(self) -> None:
        device_name = "MB2-C00043"
        issue = "캡처보드 이상"
        permalink = "https://slack.example/one"
        tracking_metadata = _sms_tracking_metadata(
            device_name=device_name,
            issue=issue,
            permalink=permalink,
            group_id="G123",
        )
        initial_rows = [
            _sms_sheet_row(
                device_name=device_name,
                issue=issue,
                permalink=permalink,
                sms_status="접수됨",
            ),
            _sms_sheet_row(tracking_metadata=tracking_metadata),
        ]
        moved_pending_rows = [
            _sms_sheet_row(tracking_metadata=tracking_metadata),
            [],
            _sms_sheet_row(
                device_name=device_name,
                issue=issue,
                permalink=permalink,
                sms_status="접수됨",
            ),
        ]
        moved_completed_rows = [
            _sms_sheet_row(tracking_metadata=tracking_metadata),
            [],
            _sms_sheet_row(
                device_name=device_name,
                issue=issue,
                permalink=permalink,
                sms_status="수신 완료",
            ),
        ]
        session = _FakeAuthorizedSession(
            get_payloads=[
                {"values": initial_rows},
                {"values": moved_pending_rows},
                {"values": moved_completed_rows},
            ]
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
            updated = (
                device_health_sheet._update_device_health_sheet_sms_status_by_group_id(
                    row_number=2,
                    group_id="G123",
                    sms_status="수신 완료",
                    authorized_session=session,
                )
            )

        self.assertTrue(updated)
        self.assertTrue(unquote(session.calls[2]["url"]).endswith("'현황'!H4"))

    def test_row_hint_selects_one_identity_when_group_id_has_multiple_targets(
        self,
    ) -> None:
        target_one = {
            "device_name": "MB2-C00043",
            "issue": "캡처보드 이상",
            "permalink": "https://slack.example/one",
        }
        target_two = {
            "device_name": "MB2-C00044",
            "issue": "LED 이상",
            "permalink": "https://slack.example/two",
        }
        tracking_one = _sms_tracking_metadata(
            **target_one,
            group_id="G-SHARED",
            message_id="M-ONE",
        )
        tracking_two = _sms_tracking_metadata(
            **target_two,
            group_id="G-SHARED",
            message_id="M-TWO",
        )
        initial_rows = [
            _sms_sheet_row(**target_one, sms_status="접수됨"),
            _sms_sheet_row(**target_two, sms_status="접수됨"),
            _sms_sheet_row(tracking_metadata=tracking_one),
            _sms_sheet_row(tracking_metadata=tracking_two),
        ]
        moved_pending_rows = [
            _sms_sheet_row(**target_two, sms_status="접수됨"),
            _sms_sheet_row(tracking_metadata=tracking_two),
            _sms_sheet_row(tracking_metadata=tracking_one),
            _sms_sheet_row(**target_one, sms_status="접수됨"),
        ]
        moved_completed_rows = [
            _sms_sheet_row(**target_two, sms_status="접수됨"),
            _sms_sheet_row(tracking_metadata=tracking_two),
            _sms_sheet_row(tracking_metadata=tracking_one),
            _sms_sheet_row(**target_one, sms_status="수신 완료"),
        ]
        session = _FakeAuthorizedSession(
            get_payloads=[
                {"values": initial_rows},
                {"values": moved_pending_rows},
                {"values": moved_completed_rows},
            ]
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
            updated = (
                device_health_sheet._update_device_health_sheet_sms_status_by_group_id(
                    row_number=2,
                    group_id="G-SHARED",
                    sms_status="수신 완료",
                    authorized_session=session,
                )
            )

        self.assertTrue(updated)
        self.assertTrue(unquote(session.calls[2]["url"]).endswith("'현황'!H5"))

    def test_skips_sms_status_update_when_target_identity_changes_before_write(
        self,
    ) -> None:
        device_name = "MB2-C00043"
        issue = "캡처보드 이상"
        permalink = "https://slack.example/one"
        tracking_metadata = _sms_tracking_metadata(
            device_name=device_name,
            issue=issue,
            permalink=permalink,
            group_id="G123",
        )
        initial_rows = [
            _sms_sheet_row(
                device_name=device_name,
                issue=issue,
                permalink=permalink,
                sms_status="접수됨",
                tracking_metadata=tracking_metadata,
            )
        ]
        changed_rows = [
            _sms_sheet_row(
                device_name=device_name,
                issue="사용자가 수정한 다른 이슈",
                permalink=permalink,
                sms_status="접수됨",
                tracking_metadata=tracking_metadata,
            )
        ]
        session = _FakeAuthorizedSession(
            get_payloads=[
                {"values": initial_rows},
                {"values": changed_rows},
            ]
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
            updated = (
                device_health_sheet._update_device_health_sheet_sms_status_by_group_id(
                    row_number=2,
                    group_id="G123",
                    sms_status="수신 완료",
                    authorized_session=session,
                )
            )

        self.assertFalse(updated)
        self.assertEqual([call["method"] for call in session.calls], ["GET", "GET"])

    def test_returns_false_when_post_write_verification_does_not_match(self) -> None:
        device_name = "MB2-C00043"
        issue = "캡처보드 이상"
        permalink = "https://slack.example/one"
        tracking_metadata = _sms_tracking_metadata(
            device_name=device_name,
            issue=issue,
            permalink=permalink,
            group_id="G123",
        )
        pending_rows = [
            _sms_sheet_row(
                device_name=device_name,
                issue=issue,
                permalink=permalink,
                sms_status="접수됨",
                tracking_metadata=tracking_metadata,
            )
        ]
        session = _FakeAuthorizedSession(
            get_payloads=[
                {"values": pending_rows},
                {"values": pending_rows},
                {"values": pending_rows},
            ]
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
            updated = (
                device_health_sheet._update_device_health_sheet_sms_status_by_group_id(
                    row_number=2,
                    group_id="G123",
                    sms_status="수신 완료",
                    authorized_session=session,
                )
            )

        self.assertFalse(updated)
        self.assertEqual(
            [call["method"] for call in session.calls],
            ["GET", "GET", "PUT", "GET"],
        )

    def test_skips_ambiguous_duplicate_b_f_q_targets(self) -> None:
        device_name = "MB2-C00043"
        issue = "캡처보드 이상"
        permalink = "https://slack.example/one"
        tracking_metadata = _sms_tracking_metadata(
            device_name=device_name,
            issue=issue,
            permalink=permalink,
            group_id="G123",
        )
        duplicate_target = _sms_sheet_row(
            device_name=device_name,
            issue=issue,
            permalink=permalink,
            sms_status="접수됨",
        )
        session = _FakeAuthorizedSession(
            get_payload={
                "values": [
                    duplicate_target,
                    list(duplicate_target),
                    _sms_sheet_row(tracking_metadata=tracking_metadata),
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
            pending = (
                device_health_sheet._load_device_health_sheet_pending_sms_deliveries(
                    authorized_session=session
                )
            )

        self.assertEqual(pending, [])
        self.assertEqual([call["method"] for call in session.calls], ["GET"])

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
