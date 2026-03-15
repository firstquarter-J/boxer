import logging
from datetime import datetime, timezone
from typing import Any, Callable, Protocol, TypedDict

from slack_bolt import App

from boxer.core import settings as s
from boxer.core.utils import _extract_question, _format_reply_text, _validate_tokens
from boxer.routers.common.request_log import (
    _initialize_request_log_storage,
    _save_request_log_record,
)

_SLACK_USER_NAME_CACHE: dict[tuple[str, str], str | None] = {}


class SlackRequestLogContext(TypedDict, total=False):
    route_name: str
    route_mode: str | None
    status: str
    user_name: str | None
    request_key: str | None
    subject_type: str | None
    subject_key: str | None
    requested_date: str | None
    error_type: str | None
    metadata: dict[str, Any]
    reply_count: int
    first_replied_at_utc: datetime | None
    permalink: str | None
    thread_permalink: str | None
    skip_persist: bool


class MentionPayload(TypedDict):
    raw_text: str
    text: str
    question: str
    user_id: str | None
    workspace_id: str
    channel_id: str
    current_ts: str
    thread_ts: str
    request_log: SlackRequestLogContext


class MessagePayload(TypedDict):
    raw_text: str
    text: str
    user_id: str | None
    workspace_id: str
    channel_id: str
    current_ts: str
    thread_ts: str
    subtype: str
    bot_id: str
    bot_name: str
    app_id: str
    request_log: SlackRequestLogContext


class SlackReplyFn(Protocol):
    def __call__(self, text: str, *, mention_user: bool = True) -> None: ...


MentionHandler = Callable[[MentionPayload, SlackReplyFn, Any, logging.Logger], None]


class SlackMessageReplyFn(Protocol):
    def __call__(self, text: str, *, thread: bool = False) -> None: ...


MessageHandler = Callable[[MessagePayload, SlackMessageReplyFn, Any, logging.Logger], None]


def _ensure_request_log_context(
    payload: MentionPayload | MessagePayload,
) -> SlackRequestLogContext:
    context = payload.get("request_log")
    if isinstance(context, dict):
        return context
    legacy_context = payload.get("request_audit")
    if isinstance(legacy_context, dict):
        payload["request_log"] = legacy_context
        return legacy_context
    context = {}
    payload["request_log"] = context
    payload["request_audit"] = context
    return context


def _set_request_log_route(
    payload: MentionPayload | MessagePayload,
    route_name: str,
    *,
    route_mode: str | None = None,
    status: str | None = None,
    request_key: str | None = None,
    subject_type: str | None = None,
    subject_key: str | None = None,
    requested_date: str | None = None,
) -> None:
    context = _ensure_request_log_context(payload)
    normalized_route_name = str(route_name or "").strip()
    if normalized_route_name:
        context["route_name"] = normalized_route_name
    normalized_route_mode = str(route_mode or "").strip()
    if normalized_route_mode:
        context["route_mode"] = normalized_route_mode
    normalized_status = str(status or "").strip()
    if normalized_status:
        context["status"] = normalized_status
    normalized_request_key = str(request_key or "").strip()
    if normalized_request_key:
        context["request_key"] = normalized_request_key
    normalized_subject_type = str(subject_type or "").strip()
    if normalized_subject_type:
        context["subject_type"] = normalized_subject_type
    normalized_subject_key = str(subject_key or "").strip()
    if normalized_subject_key:
        context["subject_key"] = normalized_subject_key
    normalized_requested_date = str(requested_date or "").strip()
    if normalized_requested_date:
        context["requested_date"] = normalized_requested_date


def _set_request_log_status(
    payload: MentionPayload | MessagePayload,
    status: str,
    *,
    error_type: str | None = None,
) -> None:
    context = _ensure_request_log_context(payload)
    normalized_status = str(status or "").strip()
    if normalized_status:
        context["status"] = normalized_status
    normalized_error_type = str(error_type or "").strip()
    if normalized_error_type:
        context["error_type"] = normalized_error_type


def _set_request_log_skip_persist(
    payload: MentionPayload | MessagePayload,
    skip_persist: bool = True,
) -> None:
    context = _ensure_request_log_context(payload)
    context["skip_persist"] = bool(skip_persist)


def _merge_request_log_metadata(
    payload: MentionPayload | MessagePayload,
    **metadata: Any,
) -> None:
    filtered = {
        key: value
        for key, value in metadata.items()
        if value is not None and value != ""
    }
    if not filtered:
        return
    context = _ensure_request_log_context(payload)
    existing = context.get("metadata")
    if isinstance(existing, dict):
        existing.update(filtered)
        return
    context["metadata"] = filtered


def _mark_request_log_reply(
    payload: MentionPayload | MessagePayload,
) -> None:
    context = _ensure_request_log_context(payload)
    context["reply_count"] = int(context.get("reply_count") or 0) + 1
    if context.get("first_replied_at_utc") is None:
        context["first_replied_at_utc"] = datetime.now(timezone.utc).replace(microsecond=0)


def _load_slack_permalink(
    client: Any,
    channel_id: str,
    message_ts: str,
    logger: logging.Logger,
) -> str | None:
    normalized_channel_id = str(channel_id or "").strip()
    normalized_message_ts = str(message_ts or "").strip()
    if not normalized_channel_id or not normalized_message_ts:
        return None
    try:
        response = client.chat_getPermalink(
            channel=normalized_channel_id,
            message_ts=normalized_message_ts,
        )
        return str((response or {}).get("permalink") or "").strip() or None
    except Exception:
        logger.warning(
            "Failed to resolve Slack permalink channel=%s ts=%s",
            normalized_channel_id,
            normalized_message_ts,
            exc_info=True,
        )
        return None


def _extract_slack_user_name(user: dict[str, Any] | None) -> str | None:
    if not isinstance(user, dict):
        return None
    profile = user.get("profile")
    profile_dict = profile if isinstance(profile, dict) else {}
    candidates = (
        profile_dict.get("display_name_normalized"),
        profile_dict.get("display_name"),
        profile_dict.get("real_name_normalized"),
        profile_dict.get("real_name"),
        user.get("real_name"),
        user.get("name"),
    )
    for candidate in candidates:
        text = str(candidate or "").strip()
        if text:
            return text
    return None


def _load_slack_user_name(
    client: Any,
    workspace_id: str,
    user_id: str,
    logger: logging.Logger,
) -> str | None:
    normalized_workspace_id = str(workspace_id or "").strip()
    normalized_user_id = str(user_id or "").strip()
    if not normalized_user_id or normalized_user_id == "unknown":
        return None
    cache_key = (normalized_workspace_id, normalized_user_id)
    if cache_key in _SLACK_USER_NAME_CACHE:
        return _SLACK_USER_NAME_CACHE[cache_key]
    try:
        response = client.users_info(user=normalized_user_id)
        user_name = _extract_slack_user_name((response or {}).get("user"))
    except Exception:
        logger.warning(
            "Failed to resolve Slack user name workspace=%s user=%s",
            normalized_workspace_id,
            normalized_user_id,
            exc_info=True,
        )
        user_name = None
    _SLACK_USER_NAME_CACHE[cache_key] = user_name
    return user_name


def _persist_request_log(
    payload: MentionPayload | MessagePayload,
    *,
    event_type: str,
    client: Any,
    logger: logging.Logger,
) -> None:
    if not s.REQUEST_LOG_SQLITE_ENABLED:
        return

    current_ts = str(payload.get("current_ts") or "").strip()
    if not current_ts:
        return

    context = _ensure_request_log_context(payload)
    channel_id = str(payload.get("channel_id") or "").strip()
    thread_ts = str(payload.get("thread_ts") or "").strip() or current_ts
    workspace_id = str(payload.get("workspace_id") or "").strip()
    user_id = str(payload.get("user_id") or "unknown").strip() or "unknown"
    permalink = str(context.get("permalink") or "").strip() or None
    if permalink is None:
        permalink = _load_slack_permalink(client, channel_id, current_ts, logger)

    thread_permalink = str(context.get("thread_permalink") or "").strip() or None
    if thread_permalink is None and thread_ts != current_ts:
        thread_permalink = _load_slack_permalink(client, channel_id, thread_ts, logger)

    if event_type == "app_mention":
        normalized_question = str(payload.get("question") or "").strip() or None
    else:
        normalized_question = str(payload.get("raw_text") or "").strip() or None

    user_name = str(context.get("user_name") or "").strip() or None
    if user_name is None:
        user_name = _load_slack_user_name(client, workspace_id, user_id, logger)
        if user_name is not None:
            context["user_name"] = user_name

    try:
        _save_request_log_record(
            {
                "sourcePlatform": "slack",
                "workspaceId": workspace_id,
                "eventType": event_type,
                "routeName": str(context.get("route_name") or event_type).strip() or event_type,
                "routeMode": str(context.get("route_mode") or "").strip() or None,
                "status": str(context.get("status") or "handled").strip() or "handled",
                "userId": user_id,
                "userName": user_name,
                "channelId": channel_id,
                "threadId": thread_ts,
                "messageId": current_ts,
                "isThreadRoot": int(thread_ts == current_ts),
                "permalink": permalink,
                "threadPermalink": thread_permalink,
                "requestText": str(payload.get("raw_text") or "").strip(),
                "normalizedQuestion": normalized_question,
                "requestKey": str(context.get("request_key") or "").strip() or None,
                "subjectType": str(context.get("subject_type") or "").strip() or None,
                "subjectKey": str(context.get("subject_key") or "").strip() or None,
                "requestedDate": str(context.get("requested_date") or "").strip() or None,
                "replyCount": int(context.get("reply_count") or 0),
                "firstRepliedAtUtc": context.get("first_replied_at_utc"),
                "errorType": str(context.get("error_type") or "").strip() or None,
                "metadata": context.get("metadata"),
            }
        )
    except Exception:
        logger.warning(
            "Failed to persist request log event_type=%s channel=%s ts=%s",
            event_type,
            channel_id,
            current_ts,
            exc_info=True,
        )


def _should_persist_request_log_event(
    payload: MentionPayload | MessagePayload,
    *,
    event_type: str,
) -> bool:
    context = _ensure_request_log_context(payload)
    if bool(context.get("skip_persist")):
        return False
    if event_type != "message":
        return True
    reply_count = int(context.get("reply_count") or 0)
    if reply_count > 0:
        return True
    status = str(context.get("status") or "").strip().lower()
    return status == "error"


def create_slack_app(
    mention_handler: MentionHandler,
    message_handler: MessageHandler | None = None,
) -> App:
    _validate_tokens(include_llm=False, include_data_sources=False)
    logger = logging.getLogger(__name__)
    if s.REQUEST_LOG_SQLITE_ENABLED and s.REQUEST_LOG_SQLITE_INIT_ON_STARTUP:
        try:
            init_result = _initialize_request_log_storage()
            logger.info("Initialized request log storage: %s", init_result)
        except Exception:
            logger.warning("Failed to initialize request log storage", exc_info=True)
    app = App(token=s.SLACK_BOT_TOKEN, signing_secret=s.SLACK_SIGNING_SECRET)

    @app.event("app_mention")
    def handle_app_mention(event: dict[str, Any], say, client) -> None:
        raw_text = event.get("text") or ""
        text = raw_text.lower()
        user_id = event.get("user")
        thread_ts = event.get("thread_ts") or event.get("ts")
        workspace_id = str(event.get("team") or event.get("team_id") or "").strip()

        payload: MentionPayload = {
            "raw_text": raw_text,
            "text": text,
            "question": _extract_question(raw_text),
            "user_id": user_id,
            "workspace_id": workspace_id,
            "channel_id": event.get("channel") or "",
            "current_ts": event.get("ts") or "",
            "thread_ts": thread_ts or "",
            "request_log": {
                "route_name": "app_mention",
                "status": "handled",
            },
        }
        logger.info("Received app_mention: user=%s text=%s", user_id, text)

        def reply(reply_text: str, *, mention_user: bool = True) -> None:
            if mention_user:
                say(
                    text=_format_reply_text(user_id, reply_text),
                    thread_ts=thread_ts,
                    unfurl_links=False,
                    unfurl_media=False,
                )
                _mark_request_log_reply(payload)
                return

            clean_text = (reply_text or "").strip()
            if not clean_text:
                clean_text = "응답 내용이 비어 있어"
            say(
                text=clean_text,
                thread_ts=thread_ts,
                unfurl_links=False,
                unfurl_media=False,
            )
            _mark_request_log_reply(payload)

        try:
            mention_handler(payload, reply, client, logger)
        except Exception as exc:
            _set_request_log_status(payload, "error", error_type=type(exc).__name__)
            raise
        finally:
            _persist_request_log(
                payload,
                event_type="app_mention",
                client=client,
                logger=logger,
            )

    @app.event("message")
    def handle_message_events(event: dict[str, Any], say, client) -> None:
        subtype = str(event.get("subtype") or "").strip()
        if subtype and subtype != "bot_message":
            logger.debug("Ignored message event subtype=%s", subtype)
            return
        if message_handler is None:
            logger.debug("Ignored message event without message_handler")
            return

        raw_text = str(event.get("text") or "").strip()
        user_id = str(event.get("user") or "").strip() or None
        bot_id = str(event.get("bot_id") or "").strip()
        bot_profile = event.get("bot_profile")
        bot_profile_dict = bot_profile if isinstance(bot_profile, dict) else {}
        bot_name = str(
            event.get("username")
            or bot_profile_dict.get("name")
            or ""
        ).strip()
        app_id = str(
            event.get("app_id")
            or bot_profile_dict.get("app_id")
            or ""
        ).strip()
        if not raw_text or (subtype != "bot_message" and not user_id):
            logger.debug("Ignored message event without text/user")
            return

        thread_ts = event.get("thread_ts") or event.get("ts") or ""
        workspace_id = str(event.get("team") or event.get("team_id") or "").strip()
        payload: MessagePayload = {
            "raw_text": raw_text,
            "text": raw_text.lower(),
            "user_id": user_id,
            "workspace_id": workspace_id,
            "channel_id": event.get("channel") or "",
            "current_ts": event.get("ts") or "",
            "thread_ts": thread_ts,
            "subtype": subtype,
            "bot_id": bot_id,
            "bot_name": bot_name,
            "app_id": app_id,
            "request_log": {
                "route_name": "message",
                "status": "handled",
            },
        }
        logger.info(
            "Received message: user=%s bot=%s subtype=%s text=%s",
            user_id or "unknown",
            bot_id or "none",
            subtype or "none",
            payload["text"],
        )

        def reply(reply_text: str, *, thread: bool = False) -> None:
            clean_text = (reply_text or "").strip()
            if not clean_text:
                clean_text = "응답 내용이 비어 있어"
            if thread:
                say(
                    text=clean_text,
                    thread_ts=thread_ts,
                    unfurl_links=False,
                    unfurl_media=False,
                )
                _mark_request_log_reply(payload)
                return
            say(
                text=clean_text,
                unfurl_links=False,
                unfurl_media=False,
            )
            _mark_request_log_reply(payload)

        try:
            message_handler(payload, reply, client, logger)
        except Exception as exc:
            _set_request_log_status(payload, "error", error_type=type(exc).__name__)
            raise
        finally:
            if _should_persist_request_log_event(payload, event_type="message"):
                _persist_request_log(
                    payload,
                    event_type="message",
                    client=client,
                    logger=logger,
                )
            else:
                logger.debug(
                    "Skipped request log persistence for unhandled message event channel=%s ts=%s",
                    payload.get("channel_id"),
                    payload.get("current_ts"),
                )

    return app


SlackRequestAuditContext = SlackRequestLogContext
_ensure_request_audit_context = _ensure_request_log_context
_set_request_audit_route = _set_request_log_route
_set_request_audit_status = _set_request_log_status
_set_request_audit_skip_persist = _set_request_log_skip_persist
_merge_request_audit_metadata = _merge_request_log_metadata
_mark_request_audit_reply = _mark_request_log_reply
_persist_request_audit = _persist_request_log
