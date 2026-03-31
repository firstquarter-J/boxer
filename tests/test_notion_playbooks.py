import unittest
from unittest.mock import patch

from boxer_company.notion_playbooks import _select_notion_references


class NotionPlaybooksTests(unittest.TestCase):
    @patch("boxer_company.notion_playbooks._is_notion_configured", return_value=False)
    def test_local_pink_barcode_overview_playbook_is_selected_for_overview_query(self, _: object) -> None:
        references = _select_notion_references(
            "핑크 바코드 전체 정리해줘",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "핑크 바코드 전체 정리해줘",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "핑크 바코드: 운영 개요")
        self.assertIn("동기화, 앱 표시, 검증 정책 3가지", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_notion_configured", return_value=False)
    def test_local_barcode_edge_case_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "분만 병원에서 바코드 구매 후 첫 촬영이 비분만 병원이면 앱에 핑크 바코드로 떠?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "분만 병원에서 바코드 구매 후 첫 촬영이 비분만 병원이면 앱에 핑크 바코드로 떠?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "바코드 표시: 구매 병원과 첫 촬영 병원이 다른 경우")
        self.assertIn("첫 녹화가 발생한 병원 기준", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_notion_configured", return_value=False)
    def test_local_pink_barcode_validation_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "핑크바코드는 분만 병원에서 녹화 차단 설정 가능해? 검증 풀면 녹화 진행돼?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "핑크바코드는 분만 병원에서 녹화 차단 설정 가능해? 검증 풀면 녹화 진행돼?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지")
        self.assertIn("핑크 바코드만 따로 녹화 허용/차단하는 설정은 없어", references[0]["previewLines"][0])


if __name__ == "__main__":
    unittest.main()
