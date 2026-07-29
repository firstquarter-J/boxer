import unittest
from unittest.mock import patch

from boxer_company.routers.app_user import (
    _analyze_app_user_baby_selection_by_barcode,
    _lookup_app_user_by_barcode,
    _should_analyze_app_user_baby_selection,
)


_BARCODE = "16326662589"
_QUESTION = (
    "16326662589 바코드로 유저조회 람다 호출시 결과가 똑딱이만 나온대. "
    "쑥쑥이는 왜 안나오는지 원인분석해"
)
_DETAILED_EXPLANATION = (
    "Lambda 조회 결과 태아 상태 아이가 두 명이야.\n"
    "• 똑딱: 2027-05-04\n"
    "• 쑥쑥이: 2026-10-22\n"
    "\n"
    "두 아이가 다태아로 설정되지 않아, 출산예정일이 더 먼 "
    "똑딱이 선택되고 쑥쑥이는 제외된 거야."
)


def _baby(
    nickname: str,
    birth_date: str | None,
    *,
    twin_flag: int = 0,
    twin_key: str | None = None,
) -> dict[str, object]:
    return {
        "babySeq": 1,
        "babyNickname": nickname,
        "twinFlag": twin_flag,
        "twinKey": twin_key,
        "birthDate": birth_date,
    }


class AppUserBabySelectionAnalysisTests(unittest.TestCase):
    def test_detects_real_baby_selection_analysis_question(self) -> None:
        self.assertTrue(
            _should_analyze_app_user_baby_selection(
                _QUESTION,
                _BARCODE,
            )
        )
        self.assertFalse(
            _should_analyze_app_user_baby_selection(
                f"{_BARCODE} 유저조회",
                _BARCODE,
            )
        )
        self.assertFalse(
            _should_analyze_app_user_baby_selection(
                f"{_BARCODE} HPA 영상이 왜 안 나와?",
                _BARCODE,
            )
        )

    def test_explains_two_non_twin_embryos_with_names_and_dates(
        self,
    ) -> None:
        babies = [
            _baby("똑딱", "2027-05-04"),
            _baby("쑥쑥이", "2026-10-22"),
        ]

        # Lambda 배열 순서가 바뀌어도 동일한 HPA 선택 원인으로 판단해야 한다.
        for ordered_babies in (babies, list(reversed(babies))):
            with self.subTest(ordered_babies=ordered_babies):
                with patch(
                    "boxer_company.routers.app_user."
                    "_request_app_users_by_barcode",
                    return_value=[{"babies": ordered_babies}],
                ):
                    result = (
                        _analyze_app_user_baby_selection_by_barcode(
                            _BARCODE
                        )
                    )

                self.assertEqual(result, _DETAILED_EXPLANATION)

    def test_does_not_claim_single_selection_for_twins(self) -> None:
        with patch(
            "boxer_company.routers.app_user._request_app_users_by_barcode",
            return_value=[
                {
                    "babies": [
                        _baby(
                            "첫째",
                            "2027-05-04",
                            twin_flag=1,
                            twin_key="twins",
                        ),
                        _baby(
                            "둘째",
                            "2027-05-04",
                            twin_flag=1,
                            twin_key="twins",
                        ),
                    ]
                }
            ],
        ):
            result = _analyze_app_user_baby_selection_by_barcode(
                _BARCODE
            )

        self.assertIn("다태아로 식별", result)
        self.assertNotEqual(result, _DETAILED_EXPLANATION)

    def test_does_not_claim_cause_for_one_embryo(self) -> None:
        with patch(
            "boxer_company.routers.app_user._request_app_users_by_barcode",
            return_value=[
                {"babies": [_baby("똑딱", "2027-05-04")]}
            ],
        ):
            result = _analyze_app_user_baby_selection_by_barcode(
                _BARCODE
            )

        self.assertIn("태아 상태 아이가 한 명", result)
        self.assertNotEqual(result, _DETAILED_EXPLANATION)

    def test_does_not_claim_cause_when_birth_date_is_missing_or_tied(
        self,
    ) -> None:
        scenarios = (
            [
                _baby("똑딱", "2027-05-04"),
                _baby("쑥쑥이", None),
            ],
            [
                _baby("똑딱", "2027-05-04"),
                _baby("쑥쑥이", "2027-05-04"),
            ],
        )

        for babies in scenarios:
            with self.subTest(babies=babies):
                with patch(
                    "boxer_company.routers.app_user."
                    "_request_app_users_by_barcode",
                    return_value=[{"babies": babies}],
                ):
                    result = (
                        _analyze_app_user_baby_selection_by_barcode(
                            _BARCODE
                        )
                    )

                self.assertIn("확정할 수 없어", result)
                self.assertNotEqual(result, _DETAILED_EXPLANATION)

    def test_existing_app_user_lookup_format_is_preserved(self) -> None:
        with patch(
            "boxer_company.routers.app_user._request_app_users_by_barcode",
            return_value=[
                {
                    "userPhoneNumber": "01000000000",
                    "userSeq": 123,
                    "userRealName": "테스트",
                    "babies": [_baby("똑딱", "2027-05-04")],
                }
            ],
        ):
            result = _lookup_app_user_by_barcode(_BARCODE)

        self.assertIn("*바코드 조회 결과*", result)
        self.assertIn("`babyNickname`: `똑딱`", result)
        self.assertIn("`birthDate`: `2027-05-04`", result)


if __name__ == "__main__":
    unittest.main()
