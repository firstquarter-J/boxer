from __future__ import annotations

import logging
import re
from typing import Any, Mapping, Sequence
from urllib.parse import urlsplit

from boxer.context.entries import ContextEntry
from boxer_adapter_slack.common import MentionPayload, SlackReplyFn
from boxer_company.assistant import (
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)
from boxer_company.assistant.commonmark import transform_outside_code

_COMMONMARK_LINK_PATTERN = re.compile(r"\[([^\]]+)\]\((https?://[^)\s]+)\)")
_COMMONMARK_BOLD_PATTERN = re.compile(r"\*\*([^*\n]+)\*\*")
_HTTP_URL_PATTERN = re.compile(r"https?://[^\s<>()\[\]|]+")
_HANGUL_PATTERN = re.compile(r"[가-힣ㄱ-ㅎㅏ-ㅣ]")
_LEGACY_SLACK_ROUTE_NAMES = {
    "device_led_log_analysis": "device led log analysis",
    "device_led_pattern_guide": "device led pattern guide",
    "barcode_log_analysis": "barcode log analysis",
    "recording_failure_analysis": "recording failure analysis",
    "device_diagnostic_followup": "device diagnostic followup",
    "notion_playbook_qa": "notion playbook qa",
    "barcode_evidence_freeform": "llm_freeform",
}


def build_company_assistant_request(
    payload: MentionPayload,
    *,
    context_entries: Sequence[ContextEntry] = (),
    metadata: Mapping[str, Any] | None = None,
) -> CompanyAssistantRequest:
    workspace_id = str(payload.get("workspace_id") or "").strip()
    channel_id = str(payload.get("channel_id") or "").strip()
    current_ts = str(payload.get("current_ts") or "").strip()
    thread_ts = str(payload.get("thread_ts") or current_ts).strip()
    request_log = payload.get("request_log")
    request_key = (
        str((request_log or {}).get("request_key") or "").strip()
        if isinstance(request_log, dict)
        else ""
    )
    question = str(payload.get("question") or "").strip()

    request_metadata = {
        "channel_id": channel_id,
        "message_id": current_ts,
    }
    request_metadata.update(metadata or {})

    return CompanyAssistantRequest(
        request_id=(
            request_key
            or f"slack:{workspace_id}:{channel_id}:{current_ts}"
        ),
        tenant_id=workspace_id,
        actor_id=str(payload.get("user_id") or "").strip() or None,
        channel="slack",
        conversation_id=thread_ts,
        question=question,
        locale="ko" if _HANGUL_PATTERN.search(question) else "en",
        context_entries=tuple(context_entries),
        metadata=request_metadata,
    )


def render_company_assistant_result(
    result: CompanyAssistantResult,
    *,
    reply: SlackReplyFn,
    actor_id: str | None,
    client: Any | None,
    logger: logging.Logger,
) -> int:
    sent_count = 0
    conversation_message_index = 0
    allowed_source_uris = frozenset(
        str(source.uri or "").strip()
        for source in result.sources
        if _is_safe_http_uri(str(source.uri or ""))
    )
    for index, message in enumerate(result.messages):
        text = _commonmark_to_slack(
            message.body,
            allowed_uris=allowed_source_uris,
        )
        if index == 0:
            text = _append_source_section(text, result.sources)

        if message.delivery_scope == "requester":
            if _send_requester_dm(
                client=client,
                actor_id=actor_id,
                text=text,
                logger=logger,
            ):
                sent_count += 1
            continue

        # 여러 공개 메시지를 내보내도 첫 메시지만 요청자를 mention한다.
        should_mention = (
            message.mention_actor and conversation_message_index == 0
        )
        if should_mention:
            reply(text)
        else:
            reply(text, mention_user=False)
        conversation_message_index += 1
        sent_count += 1
    return sent_count


def assistant_slack_route_name(route: str) -> str:
    """채널 중립 route를 기존 Slack request-log 계약 이름으로 바꾼다."""
    normalized = str(route or "").strip()
    return _LEGACY_SLACK_ROUTE_NAMES.get(normalized, normalized)


def _commonmark_to_slack(
    text: str,
    *,
    allowed_uris: frozenset[str] = frozenset(),
) -> str:
    normalized = (text or "").strip()
    return transform_outside_code(
        normalized,
        lambda segment: _commonmark_segment_to_slack(
            segment,
            allowed_uris=allowed_uris,
        ),
    )


def _commonmark_segment_to_slack(
    normalized: str,
    *,
    allowed_uris: frozenset[str],
) -> str:
    parts: list[str] = []
    cursor = 0
    for match in _COMMONMARK_LINK_PATTERN.finditer(normalized):
        parts.append(
            _render_plain_segment(
                normalized[cursor : match.start()],
                allowed_uris=allowed_uris,
            )
        )
        uri = match.group(2)
        if _is_safe_http_uri(uri) and uri in allowed_uris:
            parts.append(
                f"<{uri}|{_escape_slack_link_label(match.group(1))}>"
            )
        elif _is_safe_http_uri(uri):
            # LLM이 근거 없이 만든 URL은 클릭 가능한 Slack 링크로 승격하지 않는다.
            parts.append(_escape_slack_body_text(match.group(1)))
        else:
            # 유효하지 않은 링크는 Slack 토큰으로 만들지 않고 평문으로 남긴다.
            parts.append(
                _render_plain_segment(
                    match.group(0),
                    allowed_uris=allowed_uris,
                )
            )
        cursor = match.end()
    parts.append(
        _render_plain_segment(
            normalized[cursor:],
            allowed_uris=allowed_uris,
        )
    )
    rendered = "".join(parts)
    return _COMMONMARK_BOLD_PATTERN.sub(r"*\1*", rendered)


def _append_source_section(
    text: str,
    sources: tuple[SourceReference, ...],
) -> str:
    normalized = (text or "").strip()
    rendered_sources: list[str] = []
    seen_uris: set[str] = set()
    for source in sources:
        uri = str(source.uri or "").strip()
        title = str(source.title or "").strip()
        if (
            not _is_safe_http_uri(uri)
            or uri in seen_uris
            or uri in normalized
        ):
            continue
        seen_uris.add(uri)
        rendered_sources.append(
            f"- <{uri}|{_escape_slack_link_label(title or '출처')}>"
        )
    if not rendered_sources:
        return normalized
    section = "\n".join(["*함께 참고할 문서*", *rendered_sources])
    if not normalized:
        return section
    return f"{normalized}\n\n{section}"


def _send_requester_dm(
    *,
    client: Any | None,
    actor_id: str | None,
    text: str,
    logger: logging.Logger,
) -> bool:
    if client is None or not actor_id or not text:
        logger.warning("Skipped requester-only assistant message without Slack DM target")
        return False
    try:
        response = client.conversations_open(users=[actor_id])
        dm_channel = str(((response or {}).get("channel") or {}).get("id") or "").strip()
        if not dm_channel:
            return False
        client.chat_postMessage(
            channel=dm_channel,
            text=text,
            unfurl_links=False,
            unfurl_media=False,
        )
        return True
    except Exception as exc:
        logger.warning(
            "Failed to send requester-only assistant message user=%s error_type=%s",
            actor_id,
            type(exc).__name__,
        )
        return False


def _escape_slack_body_text(text: str) -> str:
    return (
        (text or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _render_plain_segment(
    text: str,
    *,
    allowed_uris: frozenset[str],
) -> str:
    """본문의 bare URL도 출처 allowlist 안에 있을 때만 Slack 링크로 만든다."""
    parts: list[str] = []
    cursor = 0
    for match in _HTTP_URL_PATTERN.finditer(text or ""):
        parts.append(_escape_slack_body_text((text or "")[cursor:match.start()]))
        matched_uri = match.group(0)
        uri = matched_uri.rstrip(".,!?;:")
        suffix = matched_uri[len(uri):]
        if _is_safe_http_uri(uri) and uri in allowed_uris:
            parts.append(
                f"<{uri}|{_escape_slack_link_label(uri)}>"
            )
        else:
            parts.append("[링크 생략]")
        parts.append(_escape_slack_body_text(suffix))
        cursor = match.end()
    parts.append(_escape_slack_body_text((text or "")[cursor:]))
    return "".join(parts)


def _escape_slack_link_label(text: str) -> str:
    return _escape_slack_body_text(text).replace("|", "/")


def _is_safe_http_uri(uri: str) -> bool:
    normalized = (uri or "").strip()
    if (
        not normalized
        or any(char.isspace() or ord(char) < 32 for char in normalized)
        or any(char in normalized for char in "<>|")
    ):
        return False
    try:
        parsed = urlsplit(normalized)
    except ValueError:
        return False
    return (
        parsed.scheme in {"http", "https"}
        and bool(parsed.netloc)
        and parsed.username is None
        and parsed.password is None
    )


__all__ = [
    "assistant_slack_route_name",
    "build_company_assistant_request",
    "render_company_assistant_result",
]
