import logging
from types import SimpleNamespace
import unittest

from boxer_company.assistant import (
    AssistantMessage,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company_adapter_slack.company_notion_routes import (
    CompanyNotionRoutesContext,
    CompanyNotionRoutesDeps,
    _handle_company_notion_routes,
)


class _FakeAssistantService:
    def __init__(self, result: CompanyAssistantResult | None) -> None:
        self.result = result
        self.requests = []

    def answer(self, request):
        self.requests.append(request)
        return self.result


def _context(question: str, replies: list[tuple[str, dict]]) -> CompanyNotionRoutesContext:
    logger = logging.Logger(__name__)
    logger.disabled = True
    payload = {
        "raw_text": question,
        "text": question.lower(),
        "question": question,
        "user_id": "U-HYUN",
        "workspace_id": "W123",
        "channel_id": "C123",
        "current_ts": "1.1",
        "thread_ts": "1.0",
        "request_log": {},
    }
    return CompanyNotionRoutesContext(
        question=question,
        user_id="U-HYUN",
        payload=payload,  # type: ignore[arg-type]
        thread_ts="1.0",
        reply=lambda text, **kwargs: replies.append((text, kwargs)),
        logger=logger,
        client=SimpleNamespace(),
    )


class CompanyNotionRoutesTests(unittest.TestCase):
    def test_unmatched_service_result_is_not_handled(self) -> None:
        replies: list[tuple[str, dict]] = []
        service = _FakeAssistantService(None)

        handled = _handle_company_notion_routes(
            _context("일반 질문", replies),
            CompanyNotionRoutesDeps(
                assistant_service=service,  # type: ignore[arg-type]
            ),
        )

        self.assertFalse(handled)
        self.assertEqual(replies, [])
        self.assertEqual(service.requests[0].question, "일반 질문")

    def test_service_result_is_logged_and_rendered_once(self) -> None:
        replies: list[tuple[str, dict]] = []
        result = CompanyAssistantResult(
            route="company_notion_qa",
            outcome="answered",
            messages=(AssistantMessage(body="**문서 답변**"),),
            sources=(
                SourceReference(
                    source_id="DOC-1",
                    title="Commerce",
                    uri="https://app.notion.com/p/commerce",
                ),
            ),
            used_llm=True,
        )
        service = _FakeAssistantService(result)
        context = _context("회사 노션에서 Commerce 찾아줘", replies)

        handled = _handle_company_notion_routes(
            context,
            CompanyNotionRoutesDeps(
                assistant_service=service,  # type: ignore[arg-type]
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(len(replies), 1)
        self.assertIn("*문서 답변*", replies[0][0])
        self.assertIn("https://app.notion.com/p/commerce", replies[0][0])
        self.assertEqual(
            context.payload["request_log"],
            {
                "route_name": "company_notion_qa",
                "handler_type": "router",
                "metadata": {
                    "assistantOutcome": "answered",
                    "assistantUsedLlm": True,
                },
            },
        )

    def test_denied_result_keeps_existing_public_message(self) -> None:
        replies: list[tuple[str, dict]] = []
        result = CompanyAssistantResult(
            route="company_notion_search",
            outcome="denied",
            messages=(
                AssistantMessage(
                    body="회사 Notion 검색은 아직 허용된 사용자만 쓸 수 있어"
                ),
            ),
            fallback_reason="actor_not_allowed",
        )

        handled = _handle_company_notion_routes(
            _context("회사 노션에서 영업 찾아줘", replies),
            CompanyNotionRoutesDeps(
                assistant_service=_FakeAssistantService(result),  # type: ignore[arg-type]
            ),
        )

        self.assertTrue(handled)
        self.assertEqual(
            replies,
            [("회사 Notion 검색은 아직 허용된 사용자만 쓸 수 있어", {})],
        )


if __name__ == "__main__":
    unittest.main()
