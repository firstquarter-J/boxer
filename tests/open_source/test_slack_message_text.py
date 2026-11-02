from boxer_adapter_slack.message_text import slack_block_visible_text


def test_visible_text_preserves_nested_rich_text_and_field_order() -> None:
    # 한 문장의 rich text 조각은 붙이고 별개 fields/context는 줄로 구분한다.
    block = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "제목"},
        "fields": [
            {"type": "mrkdwn", "text": "첫째"},
            {"type": "rich_text_section", "elements": [{"text": "둘"}, {"text": "째"}]},
        ],
    }
    assert slack_block_visible_text(block) == "제목\n첫째\n둘째"


def test_visible_text_does_not_render_non_text_or_hidden_action_metadata() -> None:
    block = {
        "type": "actions",
        "block_id": "hidden-block",
        "metadata": {"text": "hidden-metadata"},
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "버튼"},
                "value": "hidden-value",
                "url": "https://hidden.invalid",
                "action_id": "hidden-action",
            },
            {"text": 123},
            "bad-element",
        ],
        "fields": [None, {"text": []}],
    }
    # 비문자 자식이 만들던 빈 줄도 기존 daily fallback의 표시 계약대로 유지한다.
    assert slack_block_visible_text(block) == "버튼\n"


def test_visible_text_retains_display_mrkdwn_for_existing_callers() -> None:
    assert (
        slack_block_visible_text({"text": "<https://example.com/path|표시 이름>"})
        == "<https://example.com/path|표시 이름>"
    )
