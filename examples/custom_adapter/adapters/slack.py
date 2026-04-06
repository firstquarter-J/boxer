import logging
from typing import Any

from slack_bolt import App

from boxer_adapter_slack import (
    MentionPayload,
    SlackReplyFn,
    create_slack_app,
    set_request_log_route,
)
from examples.custom_adapter.routers.faq import (
    find_faq_answer,
    is_sensitive_question,
)


def create_app() -> App:
    def _handle_custom_mention(
        payload: MentionPayload,
        reply: SlackReplyFn,
        _client: Any,
        logger: logging.Logger,
    ) -> None:
        question = payload["question"].strip()
        normalized_question = question.lower()

        if normalized_question == "ping":
            set_request_log_route(payload, "example_ping")
            reply("pong")
            return

        if not question:
            set_request_log_route(payload, "example_empty_question", status="rejected")
            reply("질문 내용을 같이 보내줘")
            return

        if is_sensitive_question(question):
            set_request_log_route(
                payload,
                "example_policy_block",
                route_mode="sensitive",
                status="rejected",
            )
            reply("이 예제 adapter에서는 민감 정보 질문을 허용하지 않아")
            return

        faq_answer = find_faq_answer(question)
        if faq_answer:
            set_request_log_route(payload, "example_faq", route_mode="faq_match")
            reply(faq_answer)
            return

        set_request_log_route(payload, "example_default")
        reply(
            "custom adapter 예제가 동작 중이야. `ping`, `what is boxer`, "
            "`how does routing work` 중 하나를 물어봐"
        )
        logger.info("Handled example adapter fallback question=%s", question)

    return create_slack_app(_handle_custom_mention)
