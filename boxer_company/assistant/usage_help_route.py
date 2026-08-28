from __future__ import annotations

from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.read_routing import match_usage_help_route
from boxer_company.routers.usage_help import _build_usage_help_response


class UsageHelpAssistantRoute:
    """회사 사용법을 provider 호출 없이 채널 중립 결과로 반환한다."""

    name = "usage_help"

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_usage_help_route(request) != self.name:
            return None

        # 기존 Slack mrkdwn 문구를 공통 API 계약의 CommonMark로
        # 바꾸고, adapter가 최종 채널 표현으로 다시 렌더링하게 한다.
        return CompanyAssistantResult(
            route=self.name,
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=slack_mrkdwn_to_commonmark(
                        _build_usage_help_response()
                    ),
                    mention_actor=False,
                ),
            ),
        )


__all__ = ["UsageHelpAssistantRoute"]
