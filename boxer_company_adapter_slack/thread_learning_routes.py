from __future__ import annotations

import logging
from typing import Any

from boxer.context.entries import ContextEntry
from boxer.context.windowing import _limit_context_entries
from boxer_adapter_slack.context import _normalize_slack_context_entries
from boxer_company import settings as cs


def _fetch_thread_messages_for_learning(
    client: Any,
    logger: logging.Logger,
    *,
    channel_id: str,
    thread_ts: str,
) -> list[dict[str, Any]]:
    """operation API에 넘길 bounded Slack thread만 읽는다."""

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
            # 일부 mock/구 SDK는 cursor 인자를 받지 않는다. provider 실행이
            # 아니라 Slack context 호환만 유지하는 제한된 응답 형태다.
            try:
                response = client.conversations_replies(
                    channel=channel_id,
                    ts=thread_ts,
                    limit=page_size,
                    inclusive=True,
                )
            except Exception as exc:
                logger.warning(
                    "Thread context fetch failed error_type=%s",
                    type(exc).__name__,
                )
                return messages
        except Exception as exc:
            logger.warning(
                "Thread context fetch failed error_type=%s",
                type(exc).__name__,
            )
            return messages
        page_messages = response.get("messages") or []
        if isinstance(page_messages, list):
            messages.extend(
                item for item in page_messages if isinstance(item, dict)
            )
        metadata = response.get("response_metadata") if isinstance(response, dict) else {}
        next_cursor = str((metadata or {}).get("next_cursor") or "").strip()
        if not next_cursor or not response.get("has_more"):
            break
        cursor = next_cursor
    return messages[:fetch_limit]


def _load_thread_context_entries_for_learning(
    client: Any,
    logger: logging.Logger,
    *,
    channel_id: str,
    thread_ts: str,
    current_ts: str,
) -> tuple[ContextEntry, ...]:
    """thread 학습의 domain 판단 없이 API용 context entry만 만든다."""

    if not channel_id or not thread_ts:
        return ()
    messages = _fetch_thread_messages_for_learning(
        client,
        logger,
        channel_id=channel_id,
        thread_ts=thread_ts,
    )
    normalized = _normalize_slack_context_entries(
        messages,
        current_ts=current_ts,
    )
    return tuple(
        _limit_context_entries(
            normalized,
            max(1, cs.THREAD_PLAYBOOK_LEARNING_FETCH_LIMIT),
        )
    )


__all__ = ["_load_thread_context_entries_for_learning"]
