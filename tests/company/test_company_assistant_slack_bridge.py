import logging
from types import SimpleNamespace
import unittest

from boxer_company.assistant import (
    AssistantMessage,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company_adapter_slack.assistant_bridge import (
    build_company_assistant_request,
    render_company_assistant_result,
)


class CompanyAssistantSlackBridgeTests(unittest.TestCase):
    def test_payload_maps_to_exact_channel_neutral_request(self) -> None:
        payload = {
            "raw_text": "<@BOT> 회사 노션",
            "text": "회사 노션",
            "question": "회사 노션",
            "user_id": "U1",
            "workspace_id": "T1",
            "channel_id": "C1",
            "current_ts": "2.0",
            "thread_ts": "1.0",
            "request_log": {"request_key": "REQ-1"},
        }
        entries = (
            {
                "kind": "message",
                "source": "slack",
                "author_id": "U0",
                "text": "이전 질문",
            },
        )

        request = build_company_assistant_request(
            payload,  # type: ignore[arg-type]
            context_entries=entries,  # type: ignore[arg-type]
        )

        self.assertEqual(request.request_id, "REQ-1")
        self.assertEqual(request.tenant_id, "T1")
        self.assertEqual(request.actor_id, "U1")
        self.assertEqual(request.channel, "slack")
        self.assertEqual(request.conversation_id, "1.0")
        self.assertEqual(request.question, "회사 노션")
        self.assertEqual(request.locale, "ko")
        self.assertEqual(request.context_entries, entries)

    def test_result_renders_commonmark_sources_and_mentions_only_once(self) -> None:
        replies: list[tuple[str, dict]] = []
        result = CompanyAssistantResult(
            route="company_notion_qa",
            outcome="answered",
            messages=(
                AssistantMessage(body="**첫 답변**"),
                AssistantMessage(body="두 번째 답변"),
            ),
            sources=(
                SourceReference(
                    source_id="DOC-1",
                    title="문서 | 제목",
                    uri="https://app.notion.com/p/doc-1",
                ),
            ),
            used_llm=True,
        )

        sent = render_company_assistant_result(
            result,
            reply=lambda text, **kwargs: replies.append((text, kwargs)),
            actor_id="U1",
            client=None,
            logger=logging.getLogger(__name__),
        )

        self.assertEqual(sent, 2)
        self.assertEqual(replies[0][1], {})
        self.assertEqual(replies[1][1], {"mention_user": False})
        self.assertIn("*첫 답변*", replies[0][0])
        self.assertIn(
            "<https://app.notion.com/p/doc-1|문서 / 제목>",
            replies[0][0],
        )

    def test_requester_message_uses_dm_without_public_fallback(self) -> None:
        public_replies: list[str] = []
        dm_calls: list[dict] = []
        client = SimpleNamespace(
            conversations_open=lambda **kwargs: {"channel": {"id": "D1"}},
            chat_postMessage=lambda **kwargs: dm_calls.append(kwargs),
        )
        result = CompanyAssistantResult(
            route="private",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body="개인 결과",
                    delivery_scope="requester",
                ),
            ),
        )

        sent = render_company_assistant_result(
            result,
            reply=lambda text, **kwargs: public_replies.append(text),
            actor_id="U1",
            client=client,
            logger=logging.getLogger(__name__),
        )

        self.assertEqual(sent, 1)
        self.assertEqual(public_replies, [])
        self.assertEqual(dm_calls[0]["channel"], "D1")
        self.assertEqual(dm_calls[0]["text"], "개인 결과")

    def test_raw_mentions_and_malformed_links_are_not_rendered_as_slack_tokens(
        self,
    ) -> None:
        replies: list[str] = []
        result = CompanyAssistantResult(
            route="unsafe_output",
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=(
                        "본문 <!channel> <@U123> "
                        "[링크](https://ok.test/a|<!here>)"
                    )
                ),
            ),
            sources=(
                SourceReference(
                    source_id="BAD",
                    title="<!everyone>|제목",
                    uri="https://ok.test/a|<!everyone>",
                ),
            ),
        )

        render_company_assistant_result(
            result,
            reply=lambda text, **kwargs: replies.append(text),
            actor_id="U1",
            client=None,
            logger=logging.getLogger(__name__),
        )

        rendered = replies[0]
        self.assertNotIn("<!channel>", rendered)
        self.assertNotIn("<@U123>", rendered)
        self.assertNotIn("<!here>", rendered)
        self.assertNotIn("<!everyone>", rendered)
        self.assertIn("&lt;!channel&gt;", rendered)
        self.assertIn("&lt;@U123&gt;", rendered)

    def test_only_result_source_urls_become_clickable_links(self) -> None:
        replies: list[str] = []
        render_company_assistant_result(
            CompanyAssistantResult(
                route="source_allowlist",
                outcome="answered",
                messages=(
                    AssistantMessage(
                        body=(
                            "[근거 문서](https://docs.test/allowed) "
                            "[생성 링크](https://evil.test/hallucinated) "
                            "원문 https://evil.test/bare"
                        )
                    ),
                ),
                sources=(
                    SourceReference(
                        source_id="DOC-1",
                        title="근거 문서",
                        uri="https://docs.test/allowed",
                    ),
                ),
            ),
            reply=lambda text, **kwargs: replies.append(text),
            actor_id="U1",
            client=None,
            logger=logging.getLogger(__name__),
        )

        self.assertIn(
            "<https://docs.test/allowed|근거 문서>",
            replies[0],
        )
        self.assertNotIn("https://evil.test", replies[0])
        self.assertIn("생성 링크", replies[0])
        self.assertIn("[링크 생략]", replies[0])

    def test_plain_text_pipe_is_preserved(self) -> None:
        replies: list[str] = []
        render_company_assistant_result(
            CompanyAssistantResult(
                route="plain",
                outcome="answered",
                messages=(AssistantMessage(body="A | B"),),
            ),
            reply=lambda text, **kwargs: replies.append(text),
            actor_id="U1",
            client=None,
            logger=logging.getLogger(__name__),
        )

        self.assertEqual(replies, ["A | B"])

    def test_code_block_markdown_is_not_rewritten(self) -> None:
        replies: list[str] = []
        body = "```\n10:00 **fatal** [raw](https://internal/x)\n```"
        render_company_assistant_result(
            CompanyAssistantResult(
                route="raw_log",
                outcome="answered",
                messages=(AssistantMessage(body=body),),
            ),
            reply=lambda text, **kwargs: replies.append(text),
            actor_id="U1",
            client=None,
            logger=logging.getLogger(__name__),
        )

        self.assertEqual(replies, [body])


if __name__ == "__main__":
    unittest.main()
