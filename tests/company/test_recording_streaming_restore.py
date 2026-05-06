import unittest
from unittest.mock import patch

from boxer_company.routers import recording_streaming_restore
from boxer_company.routers.recording_streaming_restore import (
    RecordingStreamingRestoreResult,
    _StreamingRestoreHospitalSummary,
)


class RecordingStreamingRestoreRoutingTests(unittest.TestCase):
    def test_detects_streaming_restore_request_and_year_month(self) -> None:
        question = "35033165423 2024년 4월 영상 블라인드를 해제해줘"

        self.assertTrue(
            recording_streaming_restore._is_recording_streaming_restore_request(
                question,
                "35033165423",
            )
        )
        self.assertEqual(
            recording_streaming_restore._extract_recording_streaming_restore_month(question),
            (2024, 4),
        )
        self.assertEqual(
            recording_streaming_restore._extract_recording_streaming_restore_month(
                "35033165423 2024년 4월 영상 복원"
            ),
            (2024, 4),
        )
        self.assertEqual(
            recording_streaming_restore._extract_recording_streaming_restore_month(
                "35033165423 2024-04 영상 복원"
            ),
            (2024, 4),
        )
        self.assertEqual(
            recording_streaming_restore._extract_recording_streaming_restore_month(
                "35033165423 202404 영상 복원"
            ),
            (2024, 4),
        )

    def test_rejects_month_without_year(self) -> None:
        with self.assertRaises(ValueError):
            recording_streaming_restore._extract_recording_streaming_restore_month(
                "35033165423 4월 영상 복원"
            )

    def test_does_not_parse_compact_year_month_inside_long_number(self) -> None:
        with self.assertRaises(ValueError):
            recording_streaming_restore._extract_recording_streaming_restore_month(
                "12024041234 영상 복원"
            )

    def test_restores_only_target_month_recording_seqs_from_db(self) -> None:
        target_rows = [
            {
                "seq": 101,
                "hospitalSeq": 53,
                "hospitalName": "미래산부인과(춘천)",
                "recordedAt": "2024-04-12T01:45:44.000Z",
            },
            {
                "seq": 102,
                "hospitalSeq": 53,
                "hospitalName": "미래산부인과(춘천)",
                "recordedAt": "2024-04-29T05:06:18.000Z",
            },
        ]
        candidates = [
            {
                "seq": 101,
                "recordedAt": "2024-04-12T01:45:44.000Z",
                "restorable": True,
                "fileId": "a",
            },
            {
                "seq": 102,
                "recordedAt": "2024-04-29T05:06:18.000Z",
                "restorable": False,
                "fileId": "b",
                "failureReason": "원본 영상이 S3에 없습니다.",
            },
            {
                "seq": 103,
                "recordedAt": "2024-05-01T00:00:00.000Z",
                "restorable": True,
                "fileId": "c",
            },
        ]

        with (
            patch(
                "boxer_company.routers.recording_streaming_restore._load_recording_streaming_restore_targets",
                return_value=(2024, target_rows),
            ),
            patch(
                "boxer_company.routers.recording_streaming_restore."
                "_get_mda_stopped_recording_restore_candidates",
                return_value=candidates,
            ),
            patch(
                "boxer_company.routers.recording_streaming_restore._restore_mda_stopped_recordings",
                return_value={
                    "requestedCount": 1,
                    "restoredCount": 1,
                    "failedCount": 0,
                    "message": "복원 1건, 실패 0건",
                    "failedItems": [],
                },
            ) as restore_mock,
        ):
            result = recording_streaming_restore._restore_streaming_stopped_recordings_by_barcode_month(
                "12345678901",
                requested_year=2024,
                requested_month=4,
                requester="U123",
                requester_name="Rosa",
            )

        self.assertEqual(result.target_year, 2024)
        self.assertEqual(result.target_month, 4)
        self.assertEqual(result.db_target_count, 2)
        self.assertEqual(result.mda_candidate_count, 2)
        self.assertEqual(result.restorable_count, 1)
        self.assertEqual(result.requested_count, 1)
        self.assertEqual(result.restored_count, 1)
        restore_mock.assert_called_once()
        restore_kwargs = restore_mock.call_args.kwargs
        self.assertEqual(restore_kwargs["barcode"], "12345678901")
        self.assertEqual(restore_kwargs["hospital_seq"], 53)
        self.assertEqual(restore_kwargs["recording_seqs"], [101])
        self.assertIn("requester=U123", restore_kwargs["reason"])
        self.assertIn("requesterName=Rosa", restore_kwargs["reason"])

    def test_rejects_when_target_recordings_have_no_hospital_seq(self) -> None:
        target_rows = [
            {
                "seq": 101,
                "hospitalSeq": None,
                "hospitalName": "",
                "recordedAt": "2024-04-12T01:45:44.000Z",
            }
        ]

        with patch(
            "boxer_company.routers.recording_streaming_restore._load_recording_streaming_restore_targets",
            return_value=(2024, target_rows),
        ):
            with self.assertRaises(ValueError):
                recording_streaming_restore._restore_streaming_stopped_recordings_by_barcode_month(
                    "12345678901",
                    requested_year=2024,
                    requested_month=4,
                    requester="U123",
                )

    def test_loads_recording_targets_by_required_year_month(self) -> None:
        rows = [
            {"seq": 101, "hospitalSeq": 53, "recordedAt": "2024-04-12T01:45:44.000Z"},
        ]

        with patch(
            "boxer_company.routers.recording_streaming_restore._query_recording_streaming_restore_rows",
            return_value=rows,
        ):
            target_year, target_rows = recording_streaming_restore._load_recording_streaming_restore_targets(
                "12345678901",
                requested_year=2024,
                requested_month=4,
            )

        self.assertEqual(target_year, 2024)
        self.assertEqual([row["seq"] for row in target_rows], [101])

    def test_formats_restore_result_without_internal_mda_metrics(self) -> None:
        result = RecordingStreamingRestoreResult(
            barcode="35033165423",
            target_year=2024,
            target_month=4,
            db_target_count=2,
            mda_candidate_count=2,
            restorable_count=2,
            requested_count=2,
            restored_count=2,
            failed_count=0,
            message="복원 2건, 실패 0건",
            failed_items=[],
            hospitals=[
                _StreamingRestoreHospitalSummary(
                    hospital_seq=53,
                    hospital_name="미래산부인과(춘천)",
                    db_target_count=2,
                    mda_candidate_count=2,
                    restorable_count=2,
                )
            ],
        )

        text = recording_streaming_restore._format_recording_streaming_restore_result(result)

        self.assertIn("• 결과: *복원 완료* (`2개`)", text)
        self.assertIn("• DB 대상 recordings: `2개`", text)
        self.assertIn("대상 `2개`, 복원 가능 `2개`", text)
        self.assertNotIn("MDA 후보", text)
        self.assertNotIn("MDA 실행", text)


if __name__ == "__main__":
    unittest.main()
