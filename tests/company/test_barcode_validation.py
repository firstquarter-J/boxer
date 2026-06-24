import unittest
from datetime import datetime
from unittest.mock import patch

from boxer_company.routers.barcode_validation import (
    _is_barcode_pink_classification_reason_request,
    _is_barcode_validation_status_request,
    _query_barcode_pink_classification_reason,
    _query_barcode_validation_status,
)


class BarcodeValidationRouteDetectionTests(unittest.TestCase):
    def test_matches_barcode_validation_status_question(self) -> None:
        self.assertTrue(
            _is_barcode_validation_status_request(
                "10255657857 이건 유효성 검사에 걸리는 바코드냐",
                "10255657857",
            )
        )

    def test_matches_pink_or_refund_status_question(self) -> None:
        self.assertTrue(
            _is_barcode_validation_status_request(
                "58291583958 핑크바코드거나 환불 바코드인지 확인해줘",
                "58291583958",
            )
        )

    def test_matches_pink_classification_reason_question(self) -> None:
        self.assertTrue(
            _is_barcode_pink_classification_reason_request(
                "58291583958 왜 핑크바코드로 분류되지 않았어?",
                "58291583958",
            )
        )

    def test_does_not_match_without_barcode(self) -> None:
        self.assertFalse(
            _is_barcode_validation_status_request(
                "이건 유효성 검사에 걸리는 바코드냐",
                None,
            )
        )


class BarcodeValidationQueryTests(unittest.TestCase):
    @patch("boxer_company.routers.barcode_validation._lookup_mda_special_barcodes_by_barcode")
    def test_reports_blocked_barcode_when_free_type_matches(self, mock_lookup: object) -> None:
        mock_lookup.return_value = [  # type: ignore[attr-defined]
            {
                "barcode": "10255657857",
                "type": "FREE",
                "reason": "테스트 병원",
            }
        ]

        text = _query_barcode_validation_status("10255657857")

        self.assertIn("• 결론: 이 바코드는 유효성 검사에 걸리는 바코드야", text)
        self.assertIn("무료 바코드", text)
        self.assertIn("테스트 병원", text)

    @patch("boxer_company.routers.barcode_validation._lookup_mda_special_barcodes_by_barcode")
    def test_reports_blocked_barcode_when_refund_type_matches(self, mock_lookup: object) -> None:
        mock_lookup.return_value = [  # type: ignore[attr-defined]
            {
                "barcode": "10255657857",
                "type": "REFUND",
                "reason": "환불 완료",
            }
        ]

        text = _query_barcode_validation_status("10255657857")

        self.assertIn("• 결론: 이 바코드는 유효성 검사에 걸리는 바코드야", text)
        self.assertIn("환불 처리 바코드", text)
        self.assertIn("환불 완료", text)

    @patch("boxer_company.routers.barcode_validation._lookup_mda_special_barcodes_by_barcode")
    def test_reports_not_found_as_not_confirmed_blocked(self, mock_lookup: object) -> None:
        mock_lookup.return_value = []  # type: ignore[attr-defined]

        text = _query_barcode_validation_status("10255657857")

        self.assertIn("운영 제한 목록 기준으로는 유효성 검사에 걸리는 바코드로 확인되지 않았어", text)
        self.assertIn("무료/핑크 바코드나 환불 처리 바코드 목록에는 없어", text)
        self.assertNotIn("• 조치:", text)


class BarcodePinkClassificationReasonQueryTests(unittest.TestCase):
    @patch("boxer_company.routers.barcode_validation._load_barcode_pink_classification_context")
    @patch("boxer_company.routers.barcode_validation._lookup_mda_special_barcodes_by_barcode")
    def test_explains_backfilled_pink_setting_after_first_recording(
        self,
        mock_lookup: object,
        mock_context: object,
    ) -> None:
        mock_lookup.return_value = []  # type: ignore[attr-defined]
        mock_context.return_value = {  # type: ignore[attr-defined]
            "firstRecording": {
                "seq": 1,
                "hospitalName": "삼성나음여성의원(양천)",
                "deviceName": "MB2-C01498",
                "recordedAt": datetime(2026, 5, 9, 3, 6, 45),
                "createdAt": datetime(2026, 5, 9, 3, 9, 2),
                "hospitalPinkBarcodeAt": datetime(2026, 4, 16, 15, 0, 0),
            },
            "historyRows": [
                {
                    "hospitalName": "삼성나음여성의원(양천)",
                    "createdAt": datetime(2026, 5, 9, 3, 9, 2),
                }
            ],
            "pinkActivityRows": [
                {
                    "description": "수정된 병원명: [삼성나음여성의원(양천)], 수정 내용: [핑크바코드 적용일: null -> 2026-04-17 00:00:00]",
                    "createdAt": datetime(2026, 6, 16, 0, 21, 44),
                }
            ],
        }

        text = _query_barcode_pink_classification_reason("58291583958")

        self.assertIn("*핑크바코드 분류 확인*", text)
        self.assertIn("현재는 핑크바코드(`FREE`)나 환불 바코드(`REFUND`)가 아니야", text)
        self.assertIn("*왜 아니냐면*", text)
        self.assertIn("병원의 핑크바코드 설정은", text)
        self.assertIn("첫 녹화 이후 변경됐어", text)
        self.assertIn("나중에 소급 입력된 값", text)
        self.assertIn("기존 바코드 backfill이 없어", text)
        self.assertIn("• 변경 내용: `핑크바코드 적용일: null -> 2026-04-17 00:00:00`", text)
        self.assertNotIn("수정된 병원명", text)

    @patch("boxer_company.routers.barcode_validation._load_barcode_pink_classification_context")
    @patch("boxer_company.routers.barcode_validation._lookup_mda_special_barcodes_by_barcode")
    def test_reports_existing_special_barcode_as_not_missing(
        self,
        mock_lookup: object,
        mock_context: object,
    ) -> None:
        mock_lookup.return_value = [  # type: ignore[attr-defined]
            {"barcode": "10255657857", "type": "FREE", "reason": "테스트 병원"}
        ]
        mock_context.return_value = {  # type: ignore[attr-defined]
            "firstRecording": {
                "hospitalName": "테스트 병원",
                "deviceName": "MB2-C00001",
                "recordedAt": datetime(2026, 6, 1, 0, 0, 0),
                "createdAt": datetime(2026, 6, 1, 0, 1, 0),
                "hospitalPinkBarcodeAt": datetime(2026, 5, 1, 0, 0, 0),
            },
            "historyRows": [],
            "pinkActivityRows": [],
        }

        text = _query_barcode_pink_classification_reason("10255657857")

        self.assertIn("이미 핑크/환불 제한 목록에 등록", text)
        self.assertIn("미분류 케이스는 아니야", text)


if __name__ == "__main__":
    unittest.main()
