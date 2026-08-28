from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any

from boxer_adapter_slack.common import (
    MessagePayload,
    SlackMessageReplyFn,
    _set_request_log_skip_persist,
)
from boxer_adapter_slack.context import _load_slack_thread_context
from boxer_company import settings as cs


ALLOWED_FUN_CHANNEL_ID = "C0621TL2HSB"
RemoteFunReplyGenerator = Callable[
    [str, str, str],
    tuple[str, str, bool],
]
RemoteFortuneReplyGenerator = Callable[
    [str, str, str],
    str | None,
]


def _load_thread_root_text(
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str,
) -> str:
    """fortune API matcher에 필요한 Slack root 한 건만 읽는다."""

    if not channel_id or not thread_ts:
        return ""
    try:
        response = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=1,
            inclusive=True,
        )
    except Exception as exc:
        logger.warning(
            "Fun thread root fetch failed error_type=%s",
            type(exc).__name__,
        )
        return ""
    messages = response.get("messages") or []
    if not isinstance(messages, list) or not messages:
        return ""
    root = messages[0]
    return str(root.get("text") or "").strip() if isinstance(root, dict) else ""


def _is_dd_active(client: Any, logger: logging.Logger) -> bool:
    """API의 mention hint를 현재 Slack presence에 맞춰 전달한다."""

    if not cs.DD_USER_ID:
        return False
    try:
        response = client.users_getPresence(user=cs.DD_USER_ID)
    except Exception as exc:
        logger.warning(
            "DD presence lookup failed error_type=%s",
            type(exc).__name__,
        )
        return False
    return str(response.get("presence") or "").strip().lower() == "active"


def is_human_fun_trigger(payload: MessagePayload) -> bool:
    """사람 시작 fun 후보만 공통 membership gate 대상으로 표시한다."""

    return (
        payload.get("channel_id") == ALLOWED_FUN_CHANNEL_ID
        and payload.get("subtype") != "bot_message"
        and "모대" in str(payload.get("raw_text") or "")
    )


def handle_fun_message(
    payload: MessagePayload,
    reply: SlackMessageReplyFn,
    client: Any,
    logger: logging.Logger,
    *,
    remote_reply_generator: RemoteFunReplyGenerator | None = None,
    remote_fortune_reply_generator: RemoteFortuneReplyGenerator | None = None,
) -> None:
    """Slack context를 수집하고 API가 확정한 fun 답변만 전달한다."""

    if payload.get("channel_id") != ALLOWED_FUN_CHANNEL_ID:
        return
    raw_text = str(payload.get("raw_text") or "")
    is_threaded_bot_message = (
        payload.get("subtype") == "bot_message"
        and payload.get("thread_ts") != payload.get("current_ts")
    )
    if is_threaded_bot_message:
        if remote_fortune_reply_generator is None:
            return
        root_text = _load_thread_root_text(
            client,
            logger,
            str(payload.get("channel_id") or ""),
            str(payload.get("thread_ts") or ""),
        )
        _set_request_log_skip_persist(payload, True)
        actor = str(
            payload.get("user_id")
            or payload.get("bot_user_id")
            or payload.get("bot_id")
            or payload.get("app_id")
            or ""
        ).strip()
        try:
            fortune_reply = remote_fortune_reply_generator(
                raw_text,
                root_text,
                actor,
            )
        except Exception as exc:
            logger.warning(
                "Company API fortune reply failed error_type=%s",
                type(exc).__name__,
            )
            return
        if fortune_reply is not None:
            reply(fortune_reply, thread=True)
        return
    if payload.get("subtype") == "bot_message" or not is_human_fun_trigger(payload):
        return
    if remote_reply_generator is None:
        # remote callback 누락은 local LLM/template로 우회하지 않는다.
        logger.warning("Company API fun reply generator가 없어")
        return
    thread_context = _load_slack_thread_context(
        client,
        logger,
        str(payload.get("channel_id") or ""),
        str(payload.get("thread_ts") or payload.get("current_ts") or ""),
        str(payload.get("current_ts") or ""),
    )
    _set_request_log_skip_persist(payload, True)
    speaker_user_id = str(payload.get("user_id") or "").strip()
    try:
        reply_text, reply_mode, mention_dd = remote_reply_generator(
            raw_text,
            thread_context,
            speaker_user_id,
        )
    except Exception as exc:
        logger.warning(
            "Company API fun reply failed error_type=%s",
            type(exc).__name__,
        )
        reply_text, reply_mode, mention_dd = (
            "지금은 모대 답변을 만들 수 없어.",
            "company_api_error",
            False,
        )
    if mention_dd and cs.DD_USER_ID and _is_dd_active(client, logger):
        reply(f"<@{cs.DD_USER_ID}> {reply_text}", thread=True)
    elif mention_dd and cs.DD_USER_ID:
        reply("디디가 오프라인이라 대답하지 않습니다.", thread=True)
    else:
        reply(reply_text, thread=True)
    logger.info(
        "Delivered API fun reply channel=%s mode=%s",
        payload.get("channel_id"),
        reply_mode,
    )


__all__ = ["handle_fun_message", "is_human_fun_trigger"]
