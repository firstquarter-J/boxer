import logging
from typing import Any

from boxer.context.entries import ContextEntry
from boxer.context.windowing import _limit_context_entries, _render_context_text
from boxer.core import settings as s
from boxer.core.utils import _safe_float


def _normalize_slack_context_entries(
    messages: list[dict[str, Any]],
    *,
    current_ts: str | None,
) -> list[ContextEntry]:
    filtered: list[ContextEntry] = []
    current_ts_float = _safe_float(current_ts or "")
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        msg_ts = _safe_float(msg.get("ts") or "")
        if current_ts and msg_ts >= current_ts_float:
            continue
        user = str(msg.get("user") or "unknown").strip() or "unknown"
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        filtered.append(
            {
                "kind": "message",
                "source": "slack",
                "author_id": user,
                "text": text,
                "created_at": str(msg.get("ts") or "").strip() or None,
            }
        )
    return filtered


def _load_slack_thread_context(
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str | None,
    current_ts: str | None,
) -> str:
    if not channel_id or not thread_ts:
        return ""

    try:
        replies = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=max(1, s.THREAD_CONTEXT_FETCH_LIMIT),
            inclusive=True,
        )
    except Exception:
        logger.exception("Failed to fetch Slack thread context")
        return ""

    messages = replies.get("messages") or []
    if not isinstance(messages, list):
        return ""

    normalized_entries = _normalize_slack_context_entries(
        messages,
        current_ts=current_ts,
    )
    trimmed_entries = _limit_context_entries(
        normalized_entries,
        max(1, s.THREAD_CONTEXT_MAX_MESSAGES),
    )
    return _render_context_text(
        trimmed_entries,
        max_chars=max(1, s.THREAD_CONTEXT_MAX_CHARS),
    )
