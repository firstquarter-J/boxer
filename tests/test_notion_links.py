import unittest

from boxer_company.notion_links import select_company_notion_doc_links


class NotionLinksTests(unittest.TestCase):
    def test_pink_barcode_overview_query_keeps_related_doc_links_available(self) -> None:
        docs = select_company_notion_doc_links(
            "핑크 바코드 정리해줘",
            notion_playbooks=[
                {
                    "title": "핑크 바코드: 운영 개요",
                    "matchedKeywords": ["핑크 바코드", "정리", "분만 병원"],
                }
            ],
        )

        titles = [doc["title"] for doc in docs]
        self.assertIn("핑크 바코드: 운영 개요", titles)
        self.assertIn("바코드 동기화: 분만 병원에서 핑크 바코드가 스캔되는 경우", titles)

    def test_pink_barcode_validation_playbook_returns_exact_matching_doc(self) -> None:
        docs = select_company_notion_doc_links(
            "핑크 바코드만 따로 허용할 수 있어?",
            notion_playbooks=[
                {
                    "title": "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지",
                    "matchedKeywords": ["핑크 바코드", "검증 해제", "녹화 차단"],
                }
            ],
        )

        titles = [doc["title"] for doc in docs]
        self.assertIn("바코드 검증: 핑크 바코드만 예외 허용할 수 있는지", titles)

    def test_firewall_reference_doc_is_excluded_from_company_links(self) -> None:
        docs = select_company_notion_doc_links(
            "장비 ssh 연결이 안 되면 뭘 해야 해?",
            notion_playbooks=[
                {
                    "title": "병원 방화벽으로 MDA/원격 접속이 안 될 때",
                    "matchedKeywords": ["ssh", "방화벽", "원격 접속"],
                },
                {
                    "title": "초음파 영상 업로드 이슈 분석 가이드",
                    "matchedKeywords": ["업로드", "이슈", "분석"],
                },
                {
                    "title": "초음파 영상 업로드 안됨(네트워크 이슈)",
                    "matchedKeywords": ["업로드", "네트워크"],
                },
            ],
        )

        titles = [doc["title"] for doc in docs]
        self.assertNotIn("병원 방화벽으로 MDA/원격 접속이 안 될 때", titles)
        self.assertIn("초음파 영상 업로드 이슈 분석 가이드", titles)


if __name__ == "__main__":
    unittest.main()
