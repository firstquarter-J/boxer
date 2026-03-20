import unittest

from boxer_company_adapter_slack.company import _looks_like_notion_doc_followup


_NOTION_THREAD_CONTEXT = """*문서 기반 답변*
• 결론: 나는 마미박스 운영 문서 기반으로 동작하는 슬랙 봇이야.

*함께 참고할 문서*
- 초음파 영상 업로드 반복 실패
"""


class NotionFollowupRoutingTests(unittest.TestCase):
    def test_small_talk_is_not_treated_as_notion_followup(self) -> None:
        self.assertFalse(_looks_like_notion_doc_followup("안녕?", _NOTION_THREAD_CONTEXT))
        self.assertFalse(_looks_like_notion_doc_followup("넌 누구?", _NOTION_THREAD_CONTEXT))
        self.assertFalse(_looks_like_notion_doc_followup("넌 나야?", _NOTION_THREAD_CONTEXT))

    def test_team_profile_questions_are_not_treated_as_notion_followup(self) -> None:
        self.assertFalse(_looks_like_notion_doc_followup("dd 는 어떤 사람이야?", _NOTION_THREAD_CONTEXT))
        self.assertFalse(_looks_like_notion_doc_followup("올리비아 어때?", _NOTION_THREAD_CONTEXT))
        self.assertFalse(_looks_like_notion_doc_followup("누가 더 세?", _NOTION_THREAD_CONTEXT))

    def test_operational_followup_still_routes_to_notion(self) -> None:
        self.assertTrue(_looks_like_notion_doc_followup("그럼 왜 그래?", _NOTION_THREAD_CONTEXT))
        self.assertTrue(_looks_like_notion_doc_followup("재부팅해야 돼?", _NOTION_THREAD_CONTEXT))


if __name__ == "__main__":
    unittest.main()
