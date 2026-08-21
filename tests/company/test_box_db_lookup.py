import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from boxer_company.routers import box_db


class _FakeCursor:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self._rows = rows
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False

    def execute(self, sql: str, params: tuple[object, ...]) -> None:
        self.executed.append((sql, params))

    def fetchall(self) -> list[dict[str, object]]:
        return self._rows


class _FakeConnection:
    def __init__(self, rows: list[dict[str, object]]) -> None:
        self.cursor_obj = _FakeCursor(rows)
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return self.cursor_obj

    def close(self) -> None:
        self.closed = True


class LookupHospitalSeqByNameTests(unittest.TestCase):
    def _run_lookup(self, rows: list[dict[str, object]]) -> tuple[int | None, _FakeConnection]:
        connection = _FakeConnection(rows)
        with (
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch("boxer_company.routers.box_db._create_db_connection", return_value=connection),
        ):
            result = box_db._lookup_hospital_seq_by_name("다온미래산부인과의원(아산)")
        return result, connection

    def test_returns_seq_for_single_match(self) -> None:
        result, connection = self._run_lookup([{"seq": 297, "activeFlag": 1}])

        self.assertEqual(result, 297)
        self.assertTrue(connection.closed)

    def test_prefers_single_active_hospital_when_name_is_duplicated(self) -> None:
        result, connection = self._run_lookup(
            [
                {"seq": 297, "activeFlag": 1},
                {"seq": 185, "activeFlag": 0},
            ]
        )

        self.assertEqual(result, 297)
        executed_sql, executed_params = connection.cursor_obj.executed[0]
        self.assertIn("activeFlag", executed_sql)
        self.assertEqual(executed_params, ("다온미래산부인과의원(아산)",))

    def test_returns_none_when_multiple_active_hospitals_exist(self) -> None:
        result, _ = self._run_lookup(
            [
                {"seq": 297, "activeFlag": 1},
                {"seq": 185, "activeFlag": 1},
            ]
        )

        self.assertIsNone(result)


class DeviceDetailRenderingTests(unittest.TestCase):
    def test_single_device_detail_includes_download_availability_from_ssh_status(self) -> None:
        lines = box_db._build_device_detail_lines(
            {
                "seq": 1079,
                "deviceName": "MB2-B00045",
                "version": "2.11.300",
                "hospitalName": "한사랑병원(목포)",
                "roomName": "2진료실",
                "captureBoardType": "YUH01",
                "status": "NOSESS",
                "activeFlag": 1,
                "installFlag": 1,
                "description": "진료실2",
            },
            line_prefix="• ",
            ssh_status="연결 가능",
        )

        rendered = "\n".join(lines)

        self.assertIn("• SSH 연결 상태: 🔵 *연결 가능*", rendered)
        self.assertIn("• 초음파 영상 다운로드 가능 상태: 🔵 *가능*", rendered)


class DeviceReadOnlyQueryTests(unittest.TestCase):
    def test_db_only_device_detail_skips_mda_and_ssh_enrichment(self) -> None:
        # 공통 API용 조회는 장비 기본 row만 읽고 MDA mutation이나 SSH를 열지 않는다.
        cursor = MagicMock()
        cursor.fetchone.return_value = {"deviceCount": 1}
        cursor.fetchall.return_value = [
            {
                "seq": 1079,
                "deviceName": "MB2-C00419",
                "hospitalName": "다온미래산부인과의원(아산)",
                "roomName": "초음파실1",
                "status": "NOSESS",
                "activeFlag": 1,
                "installFlag": 1,
            }
        ]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        with (
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._create_db_connection",
                return_value=connection,
            ),
            patch(
                "boxer_company.routers.box_db._lookup_mda_device_details"
            ) as mda_lookup,
            patch(
                "boxer_company.routers.box_db._lookup_device_ssh_status"
            ) as ssh_lookup,
        ):
            rendered = box_db._query_devices_by_filters(
                device_name="MB2-C00419",
                include_live_enrichment=False,
            )

        self.assertIn("MB2-C00419", rendered)
        mda_lookup.assert_not_called()
        ssh_lookup.assert_not_called()

    def test_api_device_detail_keeps_slack_enrichment_and_disables_open_resend(
        self,
    ) -> None:
        # API 상세도 기존 Slack과 같은 version/captureBoard/SSH 필드를 만들되
        # 하나의 HTTP 요청 안에서는 sshOrder를 재전송하지 않는다.
        cursor = MagicMock()
        cursor.fetchone.return_value = {"deviceCount": 1}
        cursor.fetchall.return_value = [
            {
                "seq": 2410,
                "deviceName": "MB2-C00419",
                "hospitalName": "아이사랑산부인과의원(부산)",
                "roomName": "2진료실",
                "status": "NOSESS",
                "activeFlag": 1,
                "installFlag": 1,
            }
        ]
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        with (
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._create_db_connection",
                return_value=connection,
            ),
            patch(
                "boxer_company.routers.box_db._lookup_mda_device_details",
                return_value={
                    "MB2-C00419": {
                        "version": "2.11.307",
                        "captureBoardType": "YUH01",
                    }
                },
            ),
            patch(
                "boxer_company.routers.box_db._lookup_device_ssh_status",
                return_value="연결 가능",
            ) as ssh_lookup,
        ):
            rendered = box_db._query_devices_by_filters(
                device_name="MB2-C00419",
                include_live_enrichment=True,
                allow_ssh_open_resend=False,
            )

        self.assertIn("버전: `2.11.307`", rendered)
        self.assertIn("캡처보드 종류: `YUH01`", rendered)
        self.assertIn("SSH 연결 상태: 🔵 *연결 가능*", rendered)
        ssh_lookup.assert_called_once_with(
            "MB2-C00419",
            allow_ssh_open_resend=False,
        )


class RecordingTimelineAggregateTests(unittest.TestCase):
    def test_last_recorded_at_uses_exact_full_or_short_barcode_aggregate(self) -> None:
        # 최근 30건 context가 낡아도 마지막 녹화일은 DB 전체 aggregate가 기준이다.
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "recordingCount": 41,
            "firstRecordedAt": datetime(2024, 1, 1, 0, 0, 0),
            "lastRecordedAt": datetime(2026, 8, 4, 14, 30, 0),
        }
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        capped_context = {
            "limit": 30,
            "has_more": True,
            "summary": {
                "recordingCount": 30,
                "lastRecordedAt": datetime(2026, 7, 1, 0, 0, 0),
            },
            "rows": [],
        }

        with (
            patch.dict("os.environ", {"TZ": "Asia/Seoul"}),
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._create_db_connection",
                return_value=connection,
            ),
        ):
            rendered = box_db._query_last_recorded_at_by_barcode(
                "13194526492",
                recordings_context=capped_context,
            )

        sql, params = cursor.execute.call_args.args
        self.assertIn("COUNT(*) AS recordingCount", sql)
        self.assertIn("MAX(r.recordedAt) AS lastRecordedAt", sql)
        self.assertIn("r.fullBarcode = %s", sql)
        self.assertIn("%s IS NOT NULL AND r.barcode = %s", sql)
        self.assertNotIn("CAST(", sql)
        self.assertNotIn("LIMIT", sql)
        self.assertEqual(params, ("13194526492", None, None))
        self.assertIn("recordings row 수: *41개*", rendered)
        self.assertIn("2026-08-04 23:30:00", rendered)
        self.assertNotIn("2026-07-01", rendered)
        connection.close.assert_called_once_with()

    def test_recorded_on_old_date_uses_kst_range_aggregate_not_recent_rows(self) -> None:
        # 최근 context 밖의 오래된 날짜도 KST 하루를 UTC 범위로 바꿔 DB 전체에서 찾는다.
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "recordingCount": 2,
            "firstRecordedAt": datetime(2024, 1, 1, 15, 5, 0),
            "lastRecordedAt": datetime(2024, 1, 2, 14, 59, 0),
        }
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        capped_context = {
            "limit": 30,
            "has_more": True,
            "summary": {"recordingCount": 300},
            "rows": [
                {"recordedAt": datetime(2026, 8, 1, 0, 0, 0)}
                for _ in range(30)
            ],
        }

        with (
            patch.dict("os.environ", {"TZ": "Asia/Seoul"}),
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._create_db_connection",
                return_value=connection,
            ),
        ):
            rendered = box_db._query_recordings_on_date_by_barcode(
                "13194526492",
                "2024-01-02",
                recordings_context=capped_context,
            )

        sql, params = cursor.execute.call_args.args
        self.assertIn("MIN(r.recordedAt) AS firstRecordedAt", sql)
        self.assertIn("MAX(r.recordedAt) AS lastRecordedAt", sql)
        self.assertIn("r.fullBarcode = %s", sql)
        self.assertIn("%s IS NOT NULL AND r.barcode = %s", sql)
        self.assertNotIn("CAST(", sql)
        self.assertIn("r.recordedAt >= %s", sql)
        self.assertIn("r.recordedAt < %s", sql)
        self.assertNotIn("LIMIT", sql)
        self.assertEqual(
            params,
            (
                "13194526492",
                None,
                None,
                datetime(2024, 1, 1, 15, 0, 0),
                datetime(2024, 1, 2, 15, 0, 0),
            ),
        )
        self.assertIn("recordings row 수: *2개*", rendered)
        self.assertIn("첫 recordedAt(KST): `2024-01-02 00:05:00`", rendered)
        self.assertIn("마지막 recordedAt(KST): `2024-01-02 23:59:00`", rendered)
        self.assertNotIn("최근 `30개` 컨텍스트", rendered)

    def test_recorded_on_date_zero_result_does_not_trust_matching_context_row(self) -> None:
        # context에 같은 날짜 row가 있어도 정확한 aggregate가 0이면 없음으로 응답한다.
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "recordingCount": 0,
            "firstRecordedAt": None,
            "lastRecordedAt": None,
        }
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        stale_context = {
            "limit": 30,
            "has_more": False,
            "rows": [{"recordedAt": datetime(2024, 1, 1, 15, 5, 0)}],
        }

        with (
            patch.dict("os.environ", {"TZ": "Asia/Seoul"}),
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._create_db_connection",
                return_value=connection,
            ),
        ):
            rendered = box_db._query_recordings_on_date_by_barcode(
                "13194526492",
                "2024-01-02",
                recordings_context=stale_context,
            )

        self.assertIn("날짜 기준 recordings DB row가 없어", rendered)
        self.assertNotIn("최근 `30개` 컨텍스트", rendered)

    def test_timeline_aggregate_keeps_missing_db_error_contract(self) -> None:
        # context 전달 여부와 관계없이 정확한 DB 집계를 할 수 없으면 기존 RuntimeError를 유지한다.
        with patch.object(box_db.s, "DB_HOST", ""):
            with self.assertRaisesRegex(RuntimeError, r"DB 접속 정보\(DB_\*\)가 비어 있어"):
                box_db._query_last_recorded_at_by_barcode(
                    "13194526492",
                    recordings_context={"summary": {"recordingCount": 1}},
                )

    def test_barcode_match_keeps_exact_legacy_short_value_semantics(self) -> None:
        # int unsigned에 문자열 CAST로 실제 일치하던 canonical short 값만
        # native 보조 조건으로 보내고 11자리·선행 0 값은 fullBarcode만 본다.
        sql, params = box_db._build_recordings_barcode_match("1234567890")
        self.assertEqual(
            sql,
            "(r.fullBarcode = %s OR (%s IS NOT NULL AND r.barcode = %s))",
        )
        self.assertEqual(params, ("1234567890", 1234567890, 1234567890))

        for full_barcode in ("13194526492", "01234567890"):
            with self.subTest(full_barcode=full_barcode):
                _, full_params = box_db._build_recordings_barcode_match(
                    full_barcode
                )
                self.assertEqual(full_params, (full_barcode, None, None))

    def test_structured_recordings_filter_uses_same_native_barcode_match(
        self,
    ) -> None:
        # 이미 remote인 structured barcode 조회도 같은 1,100만-row CAST
        # full scan을 만들지 않도록 공통 indexed 조건을 사용한다.
        cursor = MagicMock()
        cursor.fetchone.return_value = {"recordingCount": 0}
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        with (
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._create_db_connection",
                return_value=connection,
            ),
        ):
            rendered = box_db._query_recordings_by_filters(
                barcode="13194526492",
                count_only=True,
            )

        sql, params = cursor.execute.call_args.args
        self.assertIn("r.fullBarcode = %s", sql)
        self.assertIn("%s IS NOT NULL AND r.barcode = %s", sql)
        self.assertNotIn("CAST(", sql)
        self.assertEqual(params, ("13194526492", None, None))
        self.assertIn("recordings row 수: *0개*", rendered)

    def test_rows_on_date_keeps_existing_composite_index_shape(self) -> None:
        # 로그 분석용 row 조회는 이미 fullBarcode + recordedAt 조건이라
        # timeline 변경과 별개로 CAST/OR 병목이 없음을 계약으로 고정한다.
        cursor = MagicMock()
        cursor.fetchall.return_value = []
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor

        with (
            patch.dict("os.environ", {"TZ": "Asia/Seoul"}),
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._create_db_connection",
                return_value=connection,
            ),
        ):
            rows = box_db._load_recordings_rows_on_date_by_barcode(
                "13194526492",
                "2024-01-02",
            )

        sql, params = cursor.execute.call_args.args
        self.assertEqual(rows, [])
        self.assertIn("WHERE r.fullBarcode = %s", sql)
        self.assertIn("r.recordedAt >= %s", sql)
        self.assertIn("r.recordedAt < %s", sql)
        self.assertNotIn("CAST(", sql)
        self.assertNotIn(" OR ", sql)
        self.assertEqual(
            params,
            (
                "13194526492",
                datetime(2024, 1, 1, 15, 0, 0),
                datetime(2024, 1, 2, 15, 0, 0),
            ),
        )


class LookupDeviceContextsByBarcodeOnDateTests(unittest.TestCase):
    def test_falls_back_to_hospital_room_scope_when_device_seq_is_missing(self) -> None:
        room_scope_contexts = [
            {
                "deviceName": "MB2-C00419",
                "deviceSeq": 1079,
                "hospitalSeq": 297,
                "hospitalRoomSeq": 412,
                "hospitalName": "다온미래산부인과의원(아산)",
                "roomName": "초음파실1",
            }
        ]

        with (
            patch.object(box_db.s, "DB_HOST", "db-host"),
            patch.object(box_db.s, "DB_USERNAME", "db-user"),
            patch.object(box_db.s, "DB_PASSWORD", "db-pass"),
            patch.object(box_db.s, "DB_DATABASE", "db-name"),
            patch(
                "boxer_company.routers.box_db._load_recordings_rows_on_date_by_barcode",
                return_value=[
                    {
                        "hospitalSeq": 297,
                        "hospitalRoomSeq": 412,
                        "deviceSeq": None,
                        "hospitalName": "다온미래산부인과의원(아산)",
                        "roomName": "초음파실1",
                    }
                ],
            ),
            patch(
                "boxer_company.routers.box_db._lookup_device_contexts_by_hospital_room_seqs",
                return_value=room_scope_contexts,
            ) as room_scope_lookup,
            patch(
                "boxer_company.routers.box_db._lookup_device_contexts_by_hospital_seqs",
                return_value=[],
            ) as hospital_scope_lookup,
        ):
            result = box_db._lookup_device_contexts_by_barcode_on_date("13194526492", "2026-04-18")

        self.assertEqual(result, room_scope_contexts)
        room_scope_lookup.assert_called_once()
        hospital_scope_lookup.assert_not_called()


if __name__ == "__main__":
    unittest.main()
