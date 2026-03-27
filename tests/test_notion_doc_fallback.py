import unittest

from boxer_company_adapter_slack.company import (
    _build_notion_doc_fallback,
    _needs_notion_doc_fallback,
)


_BABYMAGIC_REFERENCES = [
    {
        "title": "베이비매직 장애 안내",
        "previewLines": [
            "바코드 등록 없이 베이비매직이 생성돼서 유저 정보 미매칭 → 앱 전송 안 됨",
            '스레드에도 "유저 번호 매칭이 안 되어 있다"고 명시됨',
            "MDA 베이비매직 관리에서 재전송 버튼 클릭하면 유저를 찾아서 앱으로 전송 가능",
        ],
    }
]
_BARCODE_FIRST_RECORDING_EDGE_CASE_REFERENCES = [
    {
        "title": "바코드 표시: 구매 병원과 첫 촬영 병원이 다른 경우",
        "previewLines": [
            "정책 첫 녹화가 발생한 병원 기준으로 앱의 바코드 표시가 정해질 수 있어",
        ],
    }
]


class NotionDocFallbackTests(unittest.TestCase):
    def test_babymagic_send_issue_action_checks_barcode_before_resend(self) -> None:
        text = _build_notion_doc_fallback("베이비매직 전송 안 된 이유", _BABYMAGIC_REFERENCES)

        self.assertIn(
            "• 조치: 유저가 앱에서 생성한 아이에 바코드를 등록했는지 먼저 확인하고, "
            "그다음 MDA 베이비매직 관리에서 재전송을 시도해",
            text,
        )

    def test_babymagic_retry_action_missing_triggers_fallback(self) -> None:
        fallback_text = _build_notion_doc_fallback("베이비매직 전송 안 된 이유", _BABYMAGIC_REFERENCES)
        synthesized_text = """*문서 기반 답변*
• 결론: 바코드 등록 없이 베이비매직이 생성돼서 앱 전송이 안 됐어
• 확인: 유저 번호 매칭이 안 되어 있는지 확인해
• 조치: MDA 베이비매직 관리에서 재전송 버튼을 눌러봐"""

        self.assertTrue(
            _needs_notion_doc_fallback(synthesized_text, "notion playbook qa", fallback_text)
        )

    def test_barcode_first_recording_edge_case_mentions_no_extra_purchase(self) -> None:
        text = _build_notion_doc_fallback(
            "분만 병원에서 구매 후 첫 촬영이 비분만 병원이면 핑크 바코드로 보여?",
            _BARCODE_FIRST_RECORDING_EDGE_CASE_REFERENCES,
        )

        self.assertIn(
            "• 결론: 첫 녹화가 비분만 병원에서 먼저 나가면 앱에는 핑크 바코드로 보일 수 있어",
            text,
        )
        self.assertIn(
            "• 조치: 이건 표시상 엣지케이스라 실제 녹화 차단이나 신규 바코드 추가 구매가 필요한 건 아니라고 안내해",
            text,
        )

    def test_barcode_first_recording_edge_case_missing_purchase_guidance_triggers_fallback(self) -> None:
        fallback_text = _build_notion_doc_fallback(
            "분만 병원에서 구매 후 첫 촬영이 비분만 병원이면 핑크 바코드로 보여?",
            _BARCODE_FIRST_RECORDING_EDGE_CASE_REFERENCES,
        )
        synthesized_text = """*문서 기반 답변*
• 결론: 첫 촬영 병원 영향으로 핑크 바코드처럼 보일 수 있어
• 확인: 첫 촬영 병원이 어디인지 확인해
• 조치: 고객에게 상태를 안내해"""

        self.assertTrue(
            _needs_notion_doc_fallback(synthesized_text, "notion playbook qa", fallback_text)
        )


if __name__ == "__main__":
    unittest.main()
