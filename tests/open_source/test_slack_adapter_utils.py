import pytest

from boxer_adapter_slack.utils import (
    _extract_question,
    _format_reply_text,
    _safe_float,
)


def test_slack_adapter_utils_keep_payload_and_rendering_contracts() -> None:
    # core에서 adapter로 옮겨도 mention 제거, timestamp 정렬과 requester
    # mention 계약은 그대로 유지한다.
    assert _extract_question("<@U123>  질문") == "질문"
    assert _safe_float("bad") == float("inf")
    assert _format_reply_text("U123", " 답변 ") == "<@U123> 답변"


@pytest.mark.parametrize("text,expected", [
    ("<@UBOT> 오늘 <@UTARGET> 가 한 말 요약해줘", "오늘 <@UTARGET> 가 한 말 요약해줘"),
    ("<@UTARGET> <@UBOT>의 의견은?", "<@UTARGET> 의 의견은?"),
    ("<@UBOT|helper> <@UTARGET|person> 발언", "<@UTARGET|person> 발언"),
    ("<@UBOT> <@UOTHER> <@UTARGET> 비교", "<@UOTHER> <@UTARGET> 비교"),
    ("<@UBOT> ping", "ping"), ("<@UBOT>", ""),
    ("<@UBOT> <@UTARGET> 권한 부여", "<@UTARGET> 권한 부여"),
    ("<@UBOT> 첨부 문서에서 <@UTARGET> 담당 부분 찾아줘", "첨부 문서에서 <@UTARGET> 담당 부분 찾아줘"),
    ("<@UBOT> 질문 <@UBOT>", "질문"),
])
def test_bound_bot_mention_extraction_preserves_the_question_target(text, expected) -> None:
    assert _extract_question(text, bot_user_id="UBOT") == expected


def test_unknown_bot_identity_does_not_guess_the_first_mentioned_user() -> None:
    assert _extract_question("  <@UTARGET> 발언 요약  ", bot_user_id="") == "<@UTARGET> 발언 요약"


def test_bot_id_is_matched_as_a_literal_complete_mention() -> None:
    assert _extract_question("<@UBOT> <@UBOT2> <@U.1>", bot_user_id="U.1") == "<@UBOT> <@UBOT2>"
