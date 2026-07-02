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

    def test_direct_playbook_link_prevents_lower_ranked_doc_link_leak(self) -> None:
        docs = select_company_notion_doc_links(
            "모션감지 사용안함 설정 상태에서 자동으로 녹화시작 되는 이유는?",
            notion_playbooks=[
                {
                    "title": "모션감지 사용안함 상태에서 바코드 스캔 후 1시간 뒤 자동 녹화 시작",
                    "matchedKeywords": ["모션감지 사용안함", "자동 녹화"],
                },
                {
                    "title": "모션감지 사용안함 설정 시 바코드 스캔 후 자동 녹화 시작",
                    "url": "https://app.notion.com/p/383cf826870c81d68f82e63f3835fa24",
                    "matchedKeywords": ["모션감지 사용안함", "자동 녹화"],
                },
                {
                    "title": "초음파 영상 확인",
                    "url": "https://www.notion.so/928b6cfcb7c7463d92c787c69d0ca7f1?pvs=21",
                    "matchedKeywords": ["녹화"],
                },
            ],
        )

        self.assertEqual(
            [doc["title"] for doc in docs],
            ["모션감지 사용안함 설정 시 바코드 스캔 후 자동 녹화 시작"],
        )

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
