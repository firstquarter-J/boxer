import logging
from dataclasses import dataclass
from typing import Any

from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _merge_request_log_metadata,
    _set_request_log_route,
)
from boxer_company.assistant import CompanyAssistantService
from boxer_company_adapter_slack.assistant_bridge import (
    build_company_assistant_request,
    render_company_assistant_result,
)


@dataclass(frozen=True)
class CompanyNotionRoutesContext:
    question: str
    user_id: str | None
    payload: MentionPayload
    thread_ts: str
    reply: SlackReplyFn
    logger: logging.Logger
    client: Any | None = None


@dataclass(frozen=True)
class CompanyNotionRoutesDeps:
    assistant_service: CompanyAssistantService


def _handle_company_notion_routes(
    context: CompanyNotionRoutesContext,
    deps: CompanyNotionRoutesDeps,
) -> bool:
    # Slack payload는 여기서 중립 DTO로 끊고 service 결과만 Slack 형식으로 렌더링한다.
    result = deps.assistant_service.answer(
        build_company_assistant_request(context.payload)
    )
    if result is None:
        return False

    _set_request_log_route(
        context.payload,
        result.route,
        handler_type="router",
    )
    _merge_request_log_metadata(
        context.payload,
        assistantOutcome=result.outcome,
        assistantFallbackReason=result.fallback_reason,
        assistantUsedLlm=result.used_llm,
    )
    sent_count = render_company_assistant_result(
        result,
        reply=context.reply,
        actor_id=context.user_id,
        client=context.client,
        logger=context.logger,
    )
    context.logger.info(
        "Responded with company assistant route=%s outcome=%s thread_ts=%s messages=%s",
        result.route,
        result.outcome,
        context.thread_ts,
        sent_count,
    )
    return True


__all__ = [
    "CompanyNotionRoutesContext",
    "CompanyNotionRoutesDeps",
    "_handle_company_notion_routes",
]
