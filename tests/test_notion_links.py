import unittest

from boxer_company.notion_links import select_company_notion_doc_links


class NotionLinksTests(unittest.TestCase):
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
