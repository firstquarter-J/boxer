"""Slack/API/worker가 공유하는 외부 의존 없는 대화 기억 계약."""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal, TypedDict

EXTRACTOR_VERSION = "memory-v1"
MEMORY_SCHEMA_VERSION = 3
ELIGIBLE_ROUTES = frozenset({"freeform", "team_fun", "fun", "team_chat", "company_freeform", "company_team_fun"})
MAX_CHUNK_SOURCES = 100
MAX_CHUNK_CHARS = 16_000
MAX_CANDIDATES = 16
MAX_MEMORY_CHARS = 4_000
PERSONAL_STYLE_TOPICS = frozenset({"response_length", "response_language", "response_order"})


class PersonalStylePreference(TypedDict):
    """다른 채널에서도 현재 작성자에게만 적용하는 출처 없는 표준 표현 값."""

    kind: Literal["personal_style"]
    topicKey: str
    value: str


class MemoryError(ValueError):
    """원문 없는 오류 코드만 transport/관측에 넘기는 저장 계약 오류."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


class MemorySourceMessage(TypedDict, total=False):
    sourceId: str
    messageTs: str
    revisionHash: str
    authorId: str
    authorKind: str
    text: str
    permalink: str
    createdAt: str
    editedAt: str | None
    route: str
    deliveryVerified: bool


class MemoryCandidate(TypedDict, total=False):
    kind: str
    category: str
    subjectUserId: str | None
    topicKey: str
    text: str
    evidenceType: str
    sources: list[dict[str, Any]]
    subjects: list[dict[str, str]]
    action: str
    replacesId: str | None


def canonical_digest(value: Any) -> str:
    """전송 chunk와 불변 입력의 digest를 언어 간 일정한 JSON으로 만든다."""

    data = json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def source_revision(text: str, edited_at: str | None = None) -> str:
    return canonical_digest({"text": str(text).strip(), "editedAt": edited_at or ""})


def source_identity(tenant_id: str, channel_id: str, message_ts: str, revision_hash: str) -> str:
    return "src_" + canonical_digest([tenant_id, channel_id, message_ts, revision_hash])
