import unittest
from unittest.mock import patch

from boxer_company.routers.barcode_validation import (
    _is_barcode_validation_status_request,
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
    def test_reports_not_found_as_not_confirmed_blocked(self, mock_lookup: object) -> None:
        mock_lookup.return_value = []  # type: ignore[attr-defined]

        text = _query_barcode_validation_status("10255657857")

        self.assertIn("운영 제한 목록 기준으로는 유효성 검사에 걸리는 바코드로 확인되지 않았어", text)
        self.assertIn("일반 바코드 만료 여부까지는 여기서 바로 단정 못 해", text)


if __name__ == "__main__":
    unittest.main()
