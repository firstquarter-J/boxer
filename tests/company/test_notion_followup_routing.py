import unittest

from boxer_company_adapter_slack.company import (
    _build_notion_doc_query_text,
    _looks_like_notion_doc_followup,
    _looks_like_notion_doc_question,
    _resolve_notion_doc_thread_context,
    _sanitize_notion_doc_thread_context,
)


_NOTION_THREAD_CONTEXT = """*문서 기반 답변*
• 결론: 나는 마미박스 운영 문서 기반으로 동작하는 슬랙 봇이야.

*함께 참고할 문서*
- 초음파 영상 업로드 반복 실패
"""


class NotionFollowupRoutingTests(unittest.TestCase):
    def test_thread_answer_instruction_is_not_treated_as_notion_question(self) -> None:
        self.assertFalse(
            _looks_like_notion_doc_question("마미박스 2.11.300 버전 참고해서 Zion 의 직전 질문에 대답해봐")
        )

    def test_recording_cancel_voice_question_is_treated_as_notion_question(self) -> None:
        self.assertTrue(
            _looks_like_notion_doc_question("모션감지 종료 전 종료스캔하면 녹화 취소 음성 나와?")
        )

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

    def test_unrelated_thread_with_notion_keywords_is_not_treated_as_notion_context(self) -> None:
        unrelated_thread = "베이비매직 문의 있었음\n근데 지금은 영상 업로드 얘기 중"

        self.assertFalse(_looks_like_notion_doc_followup("그럼 왜 그래?", unrelated_thread))
        self.assertEqual(_build_notion_doc_query_text("베이비매직 전송 안 된 이유", unrelated_thread), "베이비매직 전송 안 된 이유")
        self.assertEqual(_sanitize_notion_doc_thread_context(unrelated_thread), "")
        self.assertEqual(_resolve_notion_doc_thread_context("베이비매직 전송 안 된 이유", unrelated_thread), "")

    def test_explicit_notion_question_in_notion_thread_stays_standalone(self) -> None:
        question = "베이비매직 전송 안 된 이유"

        self.assertFalse(_looks_like_notion_doc_followup(question, _NOTION_THREAD_CONTEXT))
        self.assertEqual(_build_notion_doc_query_text(question, _NOTION_THREAD_CONTEXT), question)
        self.assertEqual(_resolve_notion_doc_thread_context(question, _NOTION_THREAD_CONTEXT), "")

    def test_thread_answer_instruction_is_not_treated_as_notion_followup(self) -> None:
        self.assertFalse(
            _looks_like_notion_doc_followup(
                "마미박스 2.11.300 버전 참고해서 Zion 의 직전 질문에 대답해봐",
                _NOTION_THREAD_CONTEXT,
            )
        )


if __name__ == "__main__":
    unittest.main()
