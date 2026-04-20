import unittest
from unittest.mock import patch

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
