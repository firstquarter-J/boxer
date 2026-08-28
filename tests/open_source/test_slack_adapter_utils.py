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
