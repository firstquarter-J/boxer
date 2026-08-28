from __future__ import annotations

import unittest

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.knowledge_write_route import (
    ThreadPlaybookLearningAssistantRoute,
)
from boxer_company.operation_routing import match_thread_playbook_learning_route
from boxer_company.thread_playbook_learning import ThreadPlaybookSaveResult


def _request(*, with_context: bool = True) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="req-learning",
        tenant_id="T123",
        actor_id="U123",
        channel="slack",
        conversation_id="1710000000.000001",
        question="이 스레드 학습",
        locale="ko-KR",
        context_entries=(
            (
                {
                    "kind": "message",
                    "source": "slack",
                    "author_id": "U123",
                    "text": "장비 장애 원인과 처리 방법",
                },
            )
            if with_context
            else ()
        ),
        metadata={
            "route_group": "operations",
            "channel_id": "C123",
            "thread_permalink": (
                "https://humanscape.slack.com/archives/C123/"
                "p1710000000000001"
            ),
        },
    )


class ThreadPlaybookLearningAssistantRouteTests(unittest.TestCase):
    def test_matches_only_explicit_slack_learning_operation(self) -> None:
        self.assertEqual(
            match_thread_playbook_learning_route(_request()),
            "thread_playbook_learning",
        )
        request = _request()
        self.assertEqual(
            match_thread_playbook_learning_route(
                CompanyAssistantRequest(
                    request_id=request.request_id,
                    tenant_id=request.tenant_id,
                    actor_id=request.actor_id,
                    channel=request.channel,
                    conversation_id=request.conversation_id,
                    question="스레드 학습 방법",
                    locale=request.locale,
                    context_entries=request.context_entries,
                    metadata=request.metadata,
                )
            ),
            "thread_playbook_learning",
        )
        self.assertIsNone(
            match_thread_playbook_learning_route(
                CompanyAssistantRequest(
                    request_id=request.request_id,
                    tenant_id=request.tenant_id,
                    actor_id=request.actor_id,
                    channel=request.channel,
                    conversation_id=request.conversation_id,
                    question="그냥 질문",
                    locale=request.locale,
                    context_entries=request.context_entries,
                    metadata=request.metadata,
                )
            )
        )

    def test_calls_learner_with_server_derived_thread_scope(self) -> None:
        captured: dict[str, object] = {}
        claude_client = object()

        def learner(context: str, **kwargs: object) -> ThreadPlaybookSaveResult:
            captured["context"] = context
            captured.update(kwargs)
            return ThreadPlaybookSaveResult(
                title="장비 장애 대응",
                page_id="page-1",
                url="https://www.notion.so/page-1",
                keywords=["장비", "장애"],
            )

        result = ThreadPlaybookLearningAssistantRoute(
            learner=learner,
            context_max_chars=5_000,
            claude_client=claude_client,
        ).handle(_request())

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "answered")
        self.assertEqual(captured["workspace_id"], "T123")
        self.assertEqual(captured["channel_id"], "C123")
        self.assertEqual(captured["thread_ts"], "1710000000.000001")
        self.assertEqual(captured["learned_by_user_id"], "U123")
        self.assertEqual(
            captured["thread_permalink"],
            "https://humanscape.slack.com/archives/C123/"
            "p1710000000000001",
        )
        self.assertIs(captured["claude_client"], claude_client)
        self.assertIn("장비 장애", str(captured["context"]))
        self.assertEqual(result.sources[0].uri, "https://www.notion.so/page-1")
        self.assertEqual(
            result.messages[0].body,
            "**스레드 학습 완료**\n"
            "• 제목: 장비 장애 대응\n"
            "• 키워드: 장비, 장애\n"
            "• Notion: https://www.notion.so/page-1",
        )

    def test_requires_thread_context_without_calling_learner(self) -> None:
        calls = 0

        def learner(*_args: object, **_kwargs: object) -> ThreadPlaybookSaveResult:
            nonlocal calls
            calls += 1
            raise AssertionError("must not run")

        result = ThreadPlaybookLearningAssistantRoute(
            learner=learner,
            context_max_chars=5_000,
        ).handle(_request(with_context=False))

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "needs_input")
        self.assertEqual(
            result.messages[0].body,
            "학습할 스레드 내용을 찾지 못했어. "
            "답변이 달린 thread 안에서 다시 멘션해줘",
        )
        self.assertEqual(calls, 0)

    def test_feature_off_denies_before_learner(self) -> None:
        calls = 0

        def learner(*_args: object, **_kwargs: object) -> ThreadPlaybookSaveResult:
            nonlocal calls
            calls += 1
            raise AssertionError("must not run")

        result = ThreadPlaybookLearningAssistantRoute(
            learner=learner,
            feature_enabled=lambda: False,
            context_max_chars=5_000,
        ).handle(_request())

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.outcome, "denied")
        self.assertEqual(result.fallback_reason, "feature_disabled")
        self.assertEqual(
            result.messages[0].body,
            "스레드 학습 기능이 꺼져 있어",
        )
        self.assertEqual(calls, 0)


if __name__ == "__main__":
    unittest.main()
