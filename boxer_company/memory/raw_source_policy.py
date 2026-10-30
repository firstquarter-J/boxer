"""승인된 원문 보관의 검증을 일반 기억 추출의 내용 필터와 분리한다."""

from __future__ import annotations

import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES

from .contracts import MemoryError, source_identity, source_revision
from .policy import _valid_permalink, normalize_source
from .raw_policy import is_raw_only_scope

_TIMESTAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")
_AUTHOR = re.compile(r"[UW][A-Z0-9]{1,127}")
_MAX_TEXT_CHARS = 40_000


def _timestamp(value: Any) -> bool:
    return bool(isinstance(value, str) and _TIMESTAMP.fullmatch(value)
                and 0 <= Decimal(value) < 2**63)


def normalize_raw_source(
    source: Mapping[str, Any], *, tenant_id: str, channel_id: str,
    thread_ts: str, current_message_ts: str,
) -> dict[str, Any] | None:
    """고정 범위의 팀원 원문만 내용으로 거르지 않으며 출처와 수정 검증은 유지한다."""
    if not isinstance(source, Mapping):
        return None
    author, message = source.get("authorId"), source.get("messageTs")
    profiles = {str(profile["user_id"]) for profile in TEAM_MEMBER_PROFILES if profile.get("user_id")}
    if (not is_raw_only_scope(tenant_id, channel_id, message)
            or not isinstance(author, str) or not _AUTHOR.fullmatch(author) or author not in profiles):
        # 다른 채널, 기간과 작성자의 기존 운영자 import 정책은 바꾸지 않는다.
        return normalize_source(source, tenant_id=tenant_id, channel_id=channel_id,
                                thread_ts=thread_ts, current_message_ts=current_message_ts)
    text, edited = source.get("text"), source.get("editedAt") or ""
    if (source.get("authorKind") != "human" or source.get("deliveryVerified", False) is not False
            or not isinstance(text, str) or not text.strip() or len(text) > _MAX_TEXT_CHARS):
        return None
    if not _timestamp(thread_ts) or not _timestamp(current_message_ts):
        raise MemoryError("source_timestamp_invalid")
    if source.get("createdAt", message) != message:
        raise MemoryError("source_created_timestamp_invalid")
    if (source.get("editedAt") is not None and not isinstance(source.get("editedAt"), str)
            or edited and not _timestamp(edited)):
        raise MemoryError("source_edited_timestamp_invalid")
    if Decimal(message) < Decimal(thread_ts):
        raise MemoryError("source_before_thread")
    if Decimal(message) > Decimal(current_message_ts):
        raise MemoryError("source_after_capture_cutoff")
    if edited and not Decimal(message) <= Decimal(edited) <= Decimal(current_message_ts):
        raise MemoryError("source_edited_timestamp_invalid")
    revision = source.get("revisionHash")
    if revision != source_revision(text, edited):
        raise MemoryError("source_revision_mismatch")
    permalink = source.get("permalink")
    if (not isinstance(permalink, str) or len(permalink) > 2048
            or not _valid_permalink(permalink, channel_id, message, thread_ts)):
        raise MemoryError("source_permalink_invalid")
    identity = source_identity(tenant_id, channel_id, message, revision)
    if source.get("sourceId") is not None and source["sourceId"] != identity:
        raise MemoryError("source_identity_mismatch")
    # 본문은 변환하지 않는다. 일반 추출, 모델 입력과 삭제 tombstone 정책에는 관여하지 않는다.
    return {"sourceId": identity, "messageTs": message, "revisionHash": revision, "authorId": author,
            "authorKind": "human", "text": text, "permalink": permalink,
            "createdAt": message, "editedAt": edited}


__all__ = ["normalize_raw_source"]
