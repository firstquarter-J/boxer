import logging
import unittest
from typing import Any
from unittest.mock import patch

from boxer_company.notion_workspace_search import CompanyNotionSearchResult
from boxer_company_adapter_slack.company_notion_routes import (
    CompanyNotionRoutesContext,
    CompanyNotionRoutesDeps,
    _handle_company_notion_routes,
)

_PAGE_ID = "44444444444444444444444444444444"


def _context(question: str, replies: list[str]) -> CompanyNotionRoutesContext:
    logger = logging.Logger(__name__)
    logger.disabled = True
    payload = {
        "text": question,
        "question": question,
        "user_id": "U-HYUN",
        "workspace_id": "W123",
        "channel_id": "C123",
        "current_ts": "1.1",
        "thread_ts": "1.0",
        "bot_name": "Boxer",
        "app_id": "A123",
        "request_log": {},
    }
    return CompanyNotionRoutesContext(
        question=question,
        user_id="U-HYUN",
        payload=payload,  # type: ignore[arg-type]
        thread_ts="1.0",
        reply=lambda text, **kwargs: replies.append(text),
        logger=logger,
    )


def _deps(synthesis_calls: list[dict[str, Any]]) -> CompanyNotionRoutesDeps:
    def reply_with_retrieval_synthesis(
        fallback_text: str,
        evidence_payload: dict[str, Any],
        **kwargs: Any,
    ) -> None:
        synthesis_calls.append(
            {
                "fallback_text": fallback_text,
                "evidence_payload": evidence_payload,
                "kwargs": kwargs,
            }
        )

    return CompanyNotionRoutesDeps(
        reply_with_retrieval_synthesis=reply_with_retrieval_synthesis,
    )


class CompanyNotionRoutesTests(unittest.TestCase):
    def test_unrelated_question_is_not_handled(self) -> None:
        replies: list[str] = []
        synthesis_calls: list[dict[str, Any]] = []

        handled = _handle_company_notion_routes(
            _context("커머스 조직이 뭐야?", replies),
            _deps(synthesis_calls),
        )

        self.assertFalse(handled)
        self.assertEqual(replies, [])
        self.assertEqual(synthesis_calls, [])

    def test_explicit_search_synthesizes_answer_from_page_excerpts(self) -> None:
        replies: list[str] = []
        synthesis_calls: list[dict[str, Any]] = []
        result = CompanyNotionSearchResult(
            object_id=_PAGE_ID,
            object_type="page",
            title="Commerce",
            url=f"https://app.notion.com/p/{_PAGE_ID}",
            last_edited_time="",
        )

        with (
            patch(
                "boxer_company_adapter_slack.company_notion_routes._is_company_notion_search_allowed",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes._is_company_notion_search_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes._search_company_notion",
                return_value=[result],
            ) as search,
            patch(
                "boxer_company_adapter_slack.company_notion_routes._load_company_notion_references",
                return_value=[
                    {
                        "title": "Commerce",
                        "url": result.url,
                        "objectType": "page",
                        "lastEditedTime": "2026-07-16T05:33:00.000Z",
                        "excerpts": ["Commerce는 커머스 사업을 담당해."],
                        "blockCount": 10,
                        "contentTruncated": False,
                    }
                ],
            ) as load_references,
        ):
            context = _context("회사 노션에서 Commerce 찾아줘", replies)
            handled = _handle_company_notion_routes(context, _deps(synthesis_calls))

        self.assertTrue(handled)
        search.assert_called_once_with("Commerce")
        load_references.assert_called_once_with("Commerce", [result])
        self.assertEqual(replies, [])
        self.assertEqual(len(synthesis_calls), 1)
        synthesis_call = synthesis_calls[0]
        self.assertIn("관련 문서는 찾았지만", synthesis_call["fallback_text"])
        self.assertEqual(synthesis_call["kwargs"], {"route_name": "company_notion_qa", "max_tokens": 600})
        self.assertNotIn("threadContext", synthesis_call["evidence_payload"])
        self.assertEqual(
            synthesis_call["evidence_payload"],
            {
                "route": "company_notion_qa",
                "source": "notion.work_board",
                "request": {
                    "question": "회사 노션에서 Commerce 찾아줘",
                    "searchQuery": "Commerce",
                },
                "companyNotionReferences": [
                    {
                        "title": "Commerce",
                        "url": result.url,
                        "objectType": "page",
                        "lastEditedTime": "2026-07-16T05:33:00.000Z",
                        "excerpts": ["Commerce는 커머스 사업을 담당해."],
                        "blockCount": 10,
                        "contentTruncated": False,
                    }
                ],
            },
        )
        self.assertEqual(context.payload["request_log"]["route_name"], "company_notion_search")

    def test_search_without_content_evidence_returns_title_links(self) -> None:
        replies: list[str] = []
        synthesis_calls: list[dict[str, Any]] = []
        result = CompanyNotionSearchResult(
            object_id=_PAGE_ID,
            object_type="page",
            title="Commerce",
            url=f"https://app.notion.com/p/{_PAGE_ID}",
            last_edited_time="",
        )

        with (
            patch(
                "boxer_company_adapter_slack.company_notion_routes._is_company_notion_search_allowed",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes._is_company_notion_search_configured",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes._search_company_notion",
                return_value=[result],
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes._load_company_notion_references",
                return_value=[
                    {
                        "title": "Commerce",
                        "url": result.url,
                        "excerpts": [],
                        "contentUnavailable": True,
                    }
                ],
            ),
        ):
            handled = _handle_company_notion_routes(
                _context("회사 노션에서 Commerce 찾아줘", replies),
                _deps(synthesis_calls),
            )

        self.assertTrue(handled)
        self.assertEqual(len(replies), 1)
        self.assertIn("Commerce", replies[0])
        self.assertIn(result.url, replies[0])
        self.assertEqual(synthesis_calls, [])

    def test_unauthorized_user_is_blocked_before_search(self) -> None:
        replies: list[str] = []
        synthesis_calls: list[dict[str, Any]] = []

        with (
            patch(
                "boxer_company_adapter_slack.company_notion_routes._is_company_notion_search_allowed",
                return_value=False,
            ),
            patch("boxer_company_adapter_slack.company_notion_routes._search_company_notion") as search,
        ):
            handled = _handle_company_notion_routes(
                _context("회사 노션에서 영업 찾아줘", replies),
                _deps(synthesis_calls),
            )

        self.assertTrue(handled)
        search.assert_not_called()
        self.assertEqual(replies, ["회사 Notion 검색은 아직 허용된 사용자만 쓸 수 있어"])
        self.assertEqual(synthesis_calls, [])

    def test_empty_query_returns_usage(self) -> None:
        replies: list[str] = []
        synthesis_calls: list[dict[str, Any]] = []

        with (
            patch(
                "boxer_company_adapter_slack.company_notion_routes._is_company_notion_search_allowed",
                return_value=True,
            ),
            patch(
                "boxer_company_adapter_slack.company_notion_routes._is_company_notion_search_configured",
                return_value=True,
            ),
        ):
            handled = _handle_company_notion_routes(
                _context("회사 노션 조회해줘", replies),
                _deps(synthesis_calls),
            )

        self.assertTrue(handled)
        self.assertIn("검색할 키워드", replies[0])
        self.assertEqual(synthesis_calls, [])


if __name__ == "__main__":
    unittest.main()
