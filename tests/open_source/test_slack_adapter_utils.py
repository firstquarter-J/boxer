import unittest

from boxer_adapter_slack.utils import (
    _extract_question,
    _format_reply_text,
    _safe_float,
)


class SlackAdapterUtilsTests(unittest.TestCase):
    def test_slack_adapter_utils_keep_payload_and_rendering_contracts(self) -> None:
        # 기본 helper 호출의 기존 mention 제거·timestamp 정렬·requester mention 계약을 보존한다.
        self.assertEqual(_extract_question("<@U123>  질문"), "질문")
        self.assertEqual(_safe_float("bad"), float("inf"))
        self.assertEqual(_format_reply_text("U123", " 답변 "), "<@U123> 답변")


    def test_bound_bot_mention_extraction_preserves_the_question_target(self) -> None:
        # 공개 CI의 unittest discovery에서도 각 멘션·명령 시나리오를 실제 실행한다.
        cases = (
            ("<@UBOT> 오늘 <@UTARGET> 가 한 말 요약해줘", "오늘 <@UTARGET> 가 한 말 요약해줘"),
            ("<@UTARGET> <@UBOT>의 의견은?", "<@UTARGET> 의 의견은?"),
            ("<@UBOT|helper> <@UTARGET|person> 발언", "<@UTARGET|person> 발언"),
            ("<@UBOT> <@UOTHER> <@UTARGET> 비교", "<@UOTHER> <@UTARGET> 비교"),
            ("<@UBOT> ping", "ping"), ("<@UBOT>", ""),
            ("<@UBOT> <@UTARGET> 권한 부여", "<@UTARGET> 권한 부여"),
            ("<@UBOT> 첨부 문서에서 <@UTARGET> 담당 부분 찾아줘", "첨부 문서에서 <@UTARGET> 담당 부분 찾아줘"),
            ("<@UBOT> 질문 <@UBOT>", "질문"),
        )
        for text, expected in cases:
            with self.subTest(text=text):
                self.assertEqual(_extract_question(text, bot_user_id="UBOT"), expected)


    def test_unknown_bot_identity_does_not_guess_the_first_mentioned_user(self) -> None:
        self.assertEqual(_extract_question("  <@UTARGET> 발언 요약  ", bot_user_id=""), "<@UTARGET> 발언 요약")


    def test_bot_id_is_matched_as_a_literal_complete_mention(self) -> None:
        self.assertEqual(_extract_question("<@UBOT> <@UBOT2> <@U.1>", bot_user_id="U.1"), "<@UBOT> <@UBOT2>")
