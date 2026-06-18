import logging
from dataclasses import dataclass
from typing import Any

from boxer.context.entries import ContextEntry
from boxer.context.windowing import _limit_context_entries, _render_context_text
from boxer_adapter_slack.common import (
    MentionPayload,
    SlackReplyFn,
    _load_slack_permalink,
    _set_request_log_route,
)
from boxer_adapter_slack.context import _normalize_slack_context_entries
from boxer_company import settings as cs
from boxer_company.thread_playbook_learning import (
    _is_thread_playbook_learning_request,
    _learn_slack_thread_playbook,
)


@dataclass(frozen=True)
class ThreadLearningRoutesContext:
    question: str
    payload: MentionPayload
    user_id: str | None
    channel_id: str
    current_ts: str
    thread_ts: str
    reply: SlackReplyFn
    logger: logging.Logger
    client: Any
    claude_client: Any


def _is_thread_playbook_learning_allowed(user_id: str | None) -> bool:
    allowed_user_ids = cs.THREAD_PLAYBOOK_LEARNING_ALLOWED_USER_IDS
    if not allowed_user_ids:
        return True
    return bool(user_id) and user_id in allowed_user_ids


def _fetch_thread_messages_for_learning(
    client: Any,
    logger: logging.Logger,
    *,
    channel_id: str,
    thread_ts: str,
) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    cursor = ""
    fetch_limit = max(1, cs.THREAD_PLAYBOOK_LEARNING_FETCH_LIMIT)

    while len(messages) < fetch_limit:
        page_size = min(200, fetch_limit - len(messages))
        try:
            response = client.conversations_replies(
                channel=channel_id,
                ts=thread_ts,
                limit=page_size,
                inclusive=True,
                cursor=cursor or None,
            )
        except TypeError:
            try:
                response = client.conversations_replies(
                    channel=channel_id,
                    ts=thread_ts,
                    limit=page_size,
                    inclusive=True,
                )
            except Exception:
                logger.exception("Failed to fetch Slack thread for playbook learning")
                return messages
        except Exception:
            logger.exception("Failed to fetch Slack thread for playbook learning")
            return messages

        page_messages = response.get("messages") or []
        if isinstance(page_messages, list):
            messages.extend(item for item in page_messages if isinstance(item, dict))

        metadata = response.get("response_metadata") if isinstance(response, dict) else {}
        next_cursor = str((metadata or {}).get("next_cursor") or "").strip()
        if not next_cursor or not response.get("has_more"):
            break
        cursor = next_cursor

    return messages[:fetch_limit]


def _render_thread_context_for_learning(
    messages: list[dict[str, Any]],
    *,
    current_ts: str,
) -> str:
    normalized_entries = _normalize_slack_context_entries(
        messages,
        current_ts=current_ts,
    )
    # 학습은 답변 생성보다 긴 컨텍스트가 필요하지만, LLM 입력 한도는 설정값으로 통제한다.
    trimmed_entries: list[ContextEntry] = _limit_context_entries(
        normalized_entries,
        max(1, cs.THREAD_PLAYBOOK_LEARNING_FETCH_LIMIT),
    )
    return _render_context_text(
        trimmed_entries,
        max_chars=max(1, cs.THREAD_PLAYBOOK_LEARNING_MAX_THREAD_CHARS),
    )


def _load_thread_context_for_learning(
    client: Any,
    logger: logging.Logger,
    *,
    channel_id: str,
    thread_ts: str,
    current_ts: str,
) -> str:
    if not channel_id or not thread_ts:
        return ""
    messages = _fetch_thread_messages_for_learning(
        client,
        logger,
        channel_id=channel_id,
        thread_ts=thread_ts,
    )
    return _render_thread_context_for_learning(messages, current_ts=current_ts)


def _format_thread_learning_success(
    *,
    title: str,
    url: str,
    keywords: list[str],
) -> str:
    lines = [
        "*스레드 학습 완료*",
        f"• 제목: {title}",
    ]
    if keywords:
        lines.append(f"• 키워드: {', '.join(keywords[:8])}")
    if url:
        lines.append(f"• Notion: {url}")
    return "\n".join(lines)


def _handle_thread_learning_routes(context: ThreadLearningRoutesContext) -> bool:
    if not _is_thread_playbook_learning_request(context.question):
        return False

    _set_request_log_route(
        context.payload,
        "thread_playbook_learning",
        route_mode="notion",
        handler_type="router",
    )

    if not cs.THREAD_PLAYBOOK_LEARNING_ENABLED:
        context.reply("스레드 학습 기능이 꺼져 있어")
        return True

    if not _is_thread_playbook_learning_allowed(context.user_id):
        context.reply("스레드 학습은 지정된 사용자만 할 수 있어")
        context.logger.info("Rejected thread playbook learning for user=%s", context.user_id)
        return True

    thread_context = _load_thread_context_for_learning(
        context.client,
        context.logger,
        channel_id=context.channel_id,
        thread_ts=context.thread_ts,
        current_ts=context.current_ts,
    )
    if not thread_context:
        context.reply("학습할 스레드 내용을 찾지 못했어. 답변이 달린 thread 안에서 다시 멘션해줘")
        return True

    thread_permalink = _load_slack_permalink(
        context.client,
        context.channel_id,
        context.thread_ts,
        context.logger,
    )
    try:
        result = _learn_slack_thread_playbook(
            thread_context,
            thread_permalink=thread_permalink,
            learned_by_user_id=context.user_id,
            claude_client=context.claude_client,
        )
    except Exception:
        context.logger.exception("Failed to learn Slack thread as Notion playbook")
        context.reply("스레드 학습 중 오류가 발생했어. Notion/LLM 설정과 권한을 확인해줘")
        return True

    context.reply(
        _format_thread_learning_success(
            title=result.title,
            url=result.url,
            keywords=result.keywords,
        ),
        mention_user=False,
    )
    context.logger.info(
        "Learned Slack thread into Notion playbook thread_ts=%s page_id=%s",
        context.thread_ts,
        result.page_id,
    )
    return True
