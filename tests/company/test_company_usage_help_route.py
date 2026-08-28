from __future__ import annotations

import unittest
from unittest.mock import patch

from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.factory import create_company_assistant_runtime
from boxer_company.assistant.usage_help_route import UsageHelpAssistantRoute
from boxer_company.read_routing import (
    is_usage_help_question,
    match_usage_help_rollout_route,
    match_usage_help_route,
)
from boxer_company.routers.usage_help import _build_usage_help_response


def _request(
    question: str,
    *,
    route_group: str | None = None,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-USAGE-HELP-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="test",
        conversation_id="CONVERSATION-1",
        question=question,
        locale="ko",
        metadata=(
            {"route_group": route_group}
            if route_group is not None
            else {}
        ),
    )


class CompanyUsageHelpRouteTests(unittest.TestCase):
    def test_matcher_preserves_legacy_command_forms(self) -> None:
        for question in (
            "사용법",
            " 사용   방법! ",
            "도움말 알려줘",
            "HELP?",
            "헬프 보여줘",
            "명령어 목록 안내해줘",
        ):
            with self.subTest(question=question):
                self.assertTrue(is_usage_help_question(question))

        for question in ("", "사용법을 변경해줘", "help me"):
            with self.subTest(question=question):
                self.assertFalse(is_usage_help_question(question))

    def test_route_requires_explicit_freeform_stage(self) -> None:
        self.assertEqual(
            match_usage_help_route(
                _request("사용법", route_group="freeform")
            ),
            "usage_help",
        )
        self.assertIsNone(match_usage_help_route(_request("사용법")))
        self.assertIsNone(
            match_usage_help_route(
                _request("사용법", route_group="fun")
            )
        )
        # Slack transport matcher는 실행 없이 freeform hint만 복사한다.
        self.assertEqual(
            match_usage_help_rollout_route(_request("사용법")),
            "usage_help",
        )

    def test_route_keeps_existing_help_body_and_no_mention(self) -> None:
        result = UsageHelpAssistantRoute().handle(
            _request("사용법", route_group="freeform")
        )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "usage_help")
        self.assertEqual(result.outcome, "answered")
        self.assertFalse(result.used_llm)
        self.assertEqual(result.sources, ())
        self.assertEqual(len(result.messages), 1)
        self.assertFalse(result.messages[0].mention_actor)
        self.assertEqual(
            result.messages[0].body,
            slack_mrkdwn_to_commonmark(_build_usage_help_response()),
        )

    def test_factory_answers_usage_help_before_provider_routes(self) -> None:
        with patch(
            "boxer_company.assistant.factory.core_settings.LLM_PROVIDER",
            "",
        ):
            result = create_company_assistant_runtime().answer_stage(
                _request("사용법", route_group="freeform"),
                "freeform",
            )

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.route, "usage_help")
        self.assertFalse(result.used_llm)


if __name__ == "__main__":
    unittest.main()
