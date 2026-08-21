from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any

from boxer.context.windowing import _render_context_text
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company import settings as company_settings
from boxer_company.thread_playbook_learning import (
    ThreadPlaybookSaveResult,
    _is_thread_playbook_learning_request,
    _learn_slack_thread_playbook,
)


ThreadPlaybookLearner = Callable[..., ThreadPlaybookSaveResult]
FeatureEnabled = Callable[[], bool]


def _thread_playbook_learning_enabled() -> bool:
    return bool(company_settings.THREAD_PLAYBOOK_LEARNING_ENABLED)


def match_thread_playbook_learning_candidate_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """질문형까지 포함해 thread 학습 guard가 선점할 후보를 분류한다."""

    if str(request.metadata.get("route_group") or "").strip() != "operations":
        return None
    if request.channel != "slack" or not request.actor_id:
        return None
    if not _is_thread_playbook_learning_request(request.question):
        return None
    return "thread_playbook_learning"


def match_thread_playbook_learning_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """기존 Slack과 같은 thread 학습 matcher 결과를 그대로 반환한다."""

    return match_thread_playbook_learning_candidate_route(request)


class ThreadPlaybookLearningAssistantRoute:
    name = "thread_playbook_learning"

    def __init__(
        self,
        *,
        learner: ThreadPlaybookLearner = _learn_slack_thread_playbook,
        feature_enabled: FeatureEnabled = _thread_playbook_learning_enabled,
        context_max_chars: int,
        claude_client: Any = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._learner = learner
        self._feature_enabled = feature_enabled
        self._context_max_chars = max(1, context_max_chars)
        self._claude_client = claude_client
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_thread_playbook_learning_route(request) is None:
            return None
        if not self._feature_enabled():
            # Slack local과 같은 kill switch를 API mutation 전에도 재검증한다.
            return CompanyAssistantResult(
                route=self.name,
                outcome="denied",
                messages=(
                    AssistantMessage(
                        body="스레드 학습 기능이 꺼져 있어"
                    ),
                ),
                fallback_reason="feature_disabled",
            )

        thread_context = _render_context_text(
            list(request.context_entries),
            max_chars=self._context_max_chars,
        )
        if not thread_context:
            return CompanyAssistantResult(
                route=self.name,
                outcome="needs_input",
                messages=(
                    AssistantMessage(
                        body=(
                            "학습할 스레드 내용을 찾지 못했어. "
                            "답변이 달린 thread 안에서 다시 멘션해줘"
                        )
                    ),
                ),
                fallback_reason="thread_context_missing",
            )

        channel_id = str(request.metadata.get("channel_id") or "").strip()
        if not channel_id:
            return CompanyAssistantResult(
                route=self.name,
                outcome="denied",
                messages=(
                    AssistantMessage(body="학습할 Slack channel 범위를 확인할 수 없어"),
                ),
                fallback_reason="channel_scope_missing",
            )

        try:
            thread_permalink = str(
                request.metadata.get("thread_permalink") or ""
            ).strip()
            result = self._learner(
                thread_context,
                workspace_id=request.tenant_id,
                channel_id=channel_id,
                thread_ts=request.conversation_id,
                thread_permalink=thread_permalink or None,
                learned_by_user_id=request.actor_id,
                claude_client=self._claude_client,
            )
        except Exception as exc:
            # Notion/LLM 오류에 문서 원문이나 credential이 섞일 수 있어 타입만 기록한다.
            self._logger.warning(
                "Thread playbook learning failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return CompanyAssistantResult(
                route=self.name,
                outcome="failed",
                messages=(
                    AssistantMessage(
                        body=(
                            "스레드 학습 중 오류가 발생했어. "
                            "Notion/LLM 설정과 권한을 확인해줘"
                        )
                    ),
                ),
                fallback_reason="knowledge_write_failed",
            )

        # CommonMark 렌더링 뒤 기존 Slack local 성공 문구와 같은 본문이 된다.
        created_label = (
            "스레드 학습 완료"
            if result.created
            else "이미 학습된 스레드야"
        )
        body_lines = [
            f"**{created_label}**",
            f"• 제목: {result.title}",
        ]
        if result.keywords:
            body_lines.append(f"• 키워드: {', '.join(result.keywords[:8])}")
        if str(result.url or "").strip():
            body_lines.append(f"• Notion: {result.url}")

        sources: tuple[SourceReference, ...] = ()
        if str(result.url or "").strip():
            sources = (
                SourceReference(
                    source_id=f"notion:{result.page_id}",
                    title=result.title,
                    uri=result.url,
                ),
            )
        return CompanyAssistantResult(
            route=self.name,
            outcome="answered",
            messages=(
                AssistantMessage(
                    body="\n".join(body_lines),
                    mention_actor=False,
                ),
            ),
            sources=sources,
        )


__all__ = [
    "ThreadPlaybookLearningAssistantRoute",
    "ThreadPlaybookLearner",
    "match_thread_playbook_learning_candidate_route",
    "match_thread_playbook_learning_route",
]
