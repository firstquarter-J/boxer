import unittest
from unittest.mock import patch

from boxer_company.notion_playbooks import _select_notion_references


class NotionPlaybooksTests(unittest.TestCase):
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


if __name__ == "__main__":
    unittest.main()
