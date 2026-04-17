import unittest
from unittest.mock import patch

from boxer_company.notion_playbooks import _select_notion_references


class NotionPlaybooksTests(unittest.TestCase):
    @patch("boxer_company.notion_playbooks._is_notion_configured", return_value=False)
    def test_local_mommybox_process_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "마미박스 녹화 프로세스 설명해줘",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "마미박스 녹화 프로세스 설명해줘",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 프로세스 순서")
        self.assertIn("준비 음성", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_notion_configured", return_value=False)
    def test_local_mommybox_cancel_voice_question_selects_process_playbook(self, _: object) -> None:
        references = _select_notion_references(
            "마미박스 녹화 취소 음성 왜 나와?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "마미박스 녹화 취소 음성 왜 나와?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 프로세스 순서")
        self.assertTrue(any("녹화 취소 안내 음성" in line for line in references[0]["previewLines"]))

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
        self.assertIn("핑크 바코드만 따로 허용하거나 막는 설정은 없어", references[0]["previewLines"][0])

    @patch("boxer_company.notion_playbooks._is_notion_configured", return_value=False)
    def test_local_validation_status_question_selects_validation_playbook_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "해당 박스에 유효성 검사가 정상적으로 작동 되고 있어?",
            evidence_payload={
                "route": "notion_playbook_qa",
                "request": {
                    "question": "해당 박스에 유효성 검사가 정상적으로 작동 되고 있어?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지")
        self.assertIn("촬영 전에 이 바코드로 진행해도 되는지", references[0]["previewLines"][1])

    @patch("boxer_company.notion_playbooks._is_notion_configured", return_value=False)
    def test_local_led_playbook_is_selected_without_notion(self, _: object) -> None:
        references = _select_notion_references(
            "LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 어떤 상태야?",
            evidence_payload={
                "route": "device_led_pattern_guide",
                "request": {
                    "question": "LED 초록불 길게 깜빡이다가 빨간불 잠시 들어옴 반복은 어떤 상태야?",
                },
            },
        )

        self.assertTrue(references)
        self.assertEqual(references[0]["title"], "마미박스 장비 LED 상태표시등")
        self.assertIn("warning", references[0]["previewLines"][1])


if __name__ == "__main__":
    unittest.main()
