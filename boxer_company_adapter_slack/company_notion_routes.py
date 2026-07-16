import logging
from dataclasses import dataclass
from typing import Any, Callable

from boxer_adapter_slack.common import MentionPayload, SlackReplyFn, _set_request_log_route
from boxer_company.notion_workspace_search import (
    _build_company_notion_search_reply,
    _extract_company_notion_search_query,
    _is_company_notion_search_allowed,
    _is_company_notion_search_configured,
    _looks_like_company_notion_search,
    _load_company_notion_references,
    _search_company_notion,
)


@dataclass(frozen=True)
class CompanyNotionRoutesContext:
    question: str
    user_id: str | None
    payload: MentionPayload
    thread_ts: str
    reply: SlackReplyFn
    logger: logging.Logger


@dataclass(frozen=True)
class CompanyNotionRoutesDeps:
    reply_with_retrieval_synthesis: Callable[..., None]


def _handle_company_notion_routes(
    context: CompanyNotionRoutesContext,
    deps: CompanyNotionRoutesDeps,
) -> bool:
    if not _looks_like_company_notion_search(context.question):
        return False

    # 전사 문서 검색은 마미박스 라우터보다 먼저 종료해
    # 두 도메인의 검색 정책과 근거를 섞지 않는다.
    _set_request_log_route(context.payload, "company_notion_search", handler_type="router")
    if not _is_company_notion_search_allowed(context.user_id):
        context.reply("회사 Notion 검색은 아직 허용된 사용자만 쓸 수 있어")
        context.logger.warning(
            "Blocked company Notion search in thread_ts=%s user_id=%s",
            context.thread_ts,
            context.user_id,
        )
        return True
    if not _is_company_notion_search_configured():
        context.reply("회사 Notion 검색 root 설정이 없어. Work Board 페이지 ID를 먼저 설정해줘")
        context.logger.warning("Company Notion search is not configured")
        return True

    notion_query = _extract_company_notion_search_query(context.question)
    if not notion_query:
        context.reply("`회사 노션에서 커머스 찾아줘`처럼 검색할 키워드를 같이 말해줘")
        return True

    try:
        notion_results = _search_company_notion(notion_query)
        if not notion_results:
            context.reply(_build_company_notion_search_reply(notion_query, notion_results))
            return True

        notion_references = _load_company_notion_references(notion_query, notion_results)
        has_content_evidence = any(
            isinstance(reference, dict) and bool(reference.get("excerpts"))
            for reference in notion_references
        )
        if not has_content_evidence:
            context.reply(_build_company_notion_search_reply(notion_query, notion_results))
            return True

        # URL과 원문 전체 대신 제한된 관련 문단만 LLM 근거로 넘기고, 출처 링크는 합성 뒤 별도로 붙인다.
        evidence_payload: dict[str, Any] = {
            "route": "company_notion_qa",
            "source": "notion.work_board",
            "request": {
                "question": context.question,
                "searchQuery": notion_query,
            },
            "companyNotionReferences": notion_references,
        }
        fallback_text = (
            "*회사 Notion 문서 답변*\n"
            "관련 문서는 찾았지만 지금은 답변을 만들지 못했어. 아래 원문을 확인해줘"
        )
        deps.reply_with_retrieval_synthesis(
            fallback_text,
            evidence_payload,
            route_name="company_notion_qa",
            max_tokens=600,
        )
        context.logger.info(
            "Responded with company Notion answer in thread_ts=%s refs=%s",
            context.thread_ts,
            len(notion_references),
        )
        return True
    except Exception:
        context.logger.exception("Company Notion search failed")
        context.reply("회사 Notion을 조회하는 중 오류가 발생했어. 잠시 후 다시 시도해줘")
        return True


__all__ = [
    "CompanyNotionRoutesContext",
    "CompanyNotionRoutesDeps",
    "_handle_company_notion_routes",
]
