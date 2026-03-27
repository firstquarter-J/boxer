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


if __name__ == "__main__":
    unittest.main()
