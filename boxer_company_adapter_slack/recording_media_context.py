"""영상 진단에 필요한 알림 root의 화면 식별 정보만 API 문맥으로 보존한다."""

from __future__ import annotations

import logging
import re
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from slack_sdk.errors import SlackClientError

from boxer.context.entries import ContextEntry
from boxer_adapter_slack.context import (
    _normalize_slack_context_entries,
    load_slack_thread_context_entries,
)
from boxer_adapter_slack.message_text import slack_block_visible_text
from boxer_company.recording_media_scope import is_recording_media_alert_text

_TIMESTAMP = re.compile(r"\d{1,20}(?:\.\d{1,9})?")
_LINK = re.compile(r"<https?://[^|>\s]+(?:\|([^>\r\n]*))?>", re.IGNORECASE)
_URL = re.compile(r"https?://[^\s<>]+", re.IGNORECASE)
_IDENTITY_LABELS = frozenset(
    {"장비", "장비명", "바코드", "세션 시작", "녹화 시작", "세션 시각"}
)


def _without_link_targets(text: str) -> str:
    # Slack link의 화면 label은 남기고 URL과 query를 조회 대상으로 전달하지 않는다.
    return _URL.sub("", _LINK.sub(lambda match: match.group(1) or "", text))


def _field_label(text: str) -> str:
    first = text.split("\n", 1)[0]
    # Slack은 Unicode emoji를 :label: 같은 named emoji로 돌려줄 수 있다.
    return re.sub(r"^(?:(?::[^\s:]{1,64}:)|[^\w가-힣])+", "", first).strip("*_` :\t")


def recording_media_root_visible_text(message: dict[str, Any]) -> str | None:
    """짧은 fallback 대신 header와 식별 fields를 읽으며 담당자와 연락처는 제외한다."""
    blocks = message.get("blocks")
    if isinstance(blocks, list):
        headers: list[str] = []
        identities: list[str] = []
        for block in blocks[:50]:
            if not isinstance(block, dict):
                continue
            if block.get("type") == "header":
                header = slack_block_visible_text(block)
                if len(header) <= 256:
                    headers.append(_without_link_targets(header))
            elif block.get("type") == "section":
                # section 본문은 담당자나 설명일 수 있어 식별 라벨이 있는 fields만 읽는다.
                fields = block.get("fields")
                if not isinstance(fields, list):
                    continue
                for field in fields[:10]:
                    if not isinstance(field, dict):
                        continue
                    text = slack_block_visible_text(field)
                    if len(text) <= 512 and _field_label(text) in _IDENTITY_LABELS:
                        identities.append(_without_link_targets(text))
        text = "\n\n".join((*headers, *identities))
        if identities and len(text) <= 4096 and is_recording_media_alert_text(text):
            return text
    # 기존 상세 평문 카드는 같은 정규화 계약을 유지한다. 임의 block이나 action value로 보강하지 않는다.
    text = message.get("text")
    if isinstance(text, str) and text.strip() and is_recording_media_alert_text(text):
        return _without_link_targets(text.strip())
    return None


def load_recording_media_thread_context_entries(
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str | None,
    current_ts: str | None,
    *,
    fallback_loader: Callable[
        ..., list[ContextEntry]
    ] = load_slack_thread_context_entries,
) -> list[ContextEntry]:
    """회사 영상 질문에만 exact root를 보존하고 나머지는 기존 문맥 loader를 사용한다."""
    if (
        not channel_id
        or not isinstance(thread_ts, str)
        or not _TIMESTAMP.fullmatch(thread_ts)
        or not isinstance(current_ts, str)
        or not _TIMESTAMP.fullmatch(current_ts)
        or Decimal(thread_ts) >= Decimal(current_ts)
    ):
        return []
    try:
        # root 한 건만 읽으므로 replies pagination이나 최근 댓글 window가 식별 정보를 밀어내지 않는다.
        response = client.conversations_replies(
            channel=channel_id, ts=thread_ts, limit=1, inclusive=True
        )
        messages = response.get("messages") or []
        root = (
            next(
                (
                    message
                    for message in messages
                    if isinstance(message, dict)
                    and str(message.get("ts") or "") == thread_ts
                ),
                None,
            )
            if isinstance(messages, list)
            else None
        )
        text = recording_media_root_visible_text(root) if root is not None else None
        if text is not None:
            return _normalize_slack_context_entries(
                [{**root, "text": text}], current_ts=current_ts
            )
    except (
        AttributeError,
        KeyError,
        RecursionError,
        TypeError,
        ValueError,
        OSError,
        SlackClientError,
    ) as exc:
        logger.warning(
            "Recording media root context unavailable error_type=%s", type(exc).__name__
        )
    # 다른 종류의 스레드에서는 공통 loader의 기존 수집과 제한 규칙을 유지한다.
    return fallback_loader(client, logger, channel_id, thread_ts, current_ts)


__all__ = [
    "load_recording_media_thread_context_entries",
    "recording_media_root_visible_text",
]
