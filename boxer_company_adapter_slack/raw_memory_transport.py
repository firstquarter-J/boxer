"""승인된 원문 event의 본문 없는 영수증과 재조회 전송을 처리한다."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

from boxer_company.memory.contracts import canonical_digest, source_revision
from boxer_company.memory.policy import normalize_source
from boxer_company.memory.raw_policy import is_raw_only_scope
from boxer_company_adapter_slack.memory_event_outbox import _validated_event

_STAMP = re.compile(r"[0-9]{10,16}\.[0-9]{6}")
_PREFIX = "memory.raw_event:"
_STATUSES = frozenset({"stored", "duplicate", "stale", "deleted", "unavailable"})
_ENDPOINT = "/internal/v1/memory/sources/raw"


class RawMemoryTransportError(RuntimeError):
    """원문과 외부 오류 메시지를 넣지 않는 고정 전송 오류다."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _require(condition: Any, code: str) -> None:
    if not condition:
        raise RawMemoryTransportError(code)


def _data(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    value = getattr(value, "data", None)
    return value if isinstance(value, Mapping) else {}


def raw_event_scope(event: Mapping[str, Any], body: Mapping[str, Any]) -> bool:
    """편집과 삭제도 원본 발언 생성 시각을 기준으로 고정 범위를 판정한다."""
    message = _data(event.get("message"))
    stamp = str(event.get("deleted_ts") or message.get("ts") or event.get("ts") or "")
    tenant = str(body.get("team_id") or event.get("team") or "")
    return is_raw_only_scope(tenant, str(event.get("channel") or ""), stamp)


class RawMemoryTransport:
    def __init__(self, api: Any, *, is_allowed: Callable[[str, str], bool],
                 profile_ids: Callable[[], set[str]], client: Callable[[], Any]) -> None:
        self.api, self.is_allowed, self.profile_ids, self.client = api, is_allowed, profile_ids, client

    def _author_allowed(self, tenant: str, author: str) -> bool:
        return bool(author and author in self.profile_ids() and self.is_allowed(tenant, author))

    @staticmethod
    def _private(client: Any, tenant: str, channel: str) -> None:
        _require(client is not None, "memory_raw_client_missing")
        result = _data(client.conversations_info(channel=channel, include_num_members=False))
        info = _data(result.get("channel"))
        _require(result.get("ok") is True and info.get("id") == channel
                 and info.get("is_private") is True and info.get("is_member") is True
                 and not info.get("is_im") and not info.get("is_mpim")
                 and not info.get("is_ext_shared") and not info.get("is_org_shared")
                 and info.get("context_team_id", tenant) == tenant, "memory_raw_private_scope_unverified")

    def _source(self, message: Mapping[str, Any], *, tenant: str, channel: str,
                thread: str, stamp: str) -> dict[str, Any] | None:
        author, text = str(message.get("user") or ""), message.get("text")
        if not self._author_allowed(tenant, author):
            return None
        _require(str(message.get("thread_ts") or thread) == thread, "memory_raw_source_thread_changed")
        if (message.get("bot_id") or message.get("bot_profile") or message.get("app_id")
                or message.get("subtype") or message.get("files") or message.get("attachments")
                or not isinstance(text, str) or not text.strip() or len(text) > 40_000):
            return None
        edited = str(_data(message.get("edited")).get("ts") or "")
        _require(not edited or _STAMP.fullmatch(edited) and Decimal(edited) >= Decimal(stamp),
                 "memory_raw_edit_timestamp_invalid")
        # 같은 revision의 재전송에서 쿼리·표기 차이로 본문 binding이 바뀌지 않게 URL을 정규화한다.
        source = {"messageTs": stamp, "authorId": author, "authorKind": "human", "text": text,
            "revisionHash": source_revision(text, edited), "createdAt": stamp, "editedAt": edited,
            "permalink": "https://lifexio.slack.com/archives/" + channel + "/p" + stamp.replace(".", "")}
        normalized = normalize_source(source, tenant_id=tenant, channel_id=channel,
                                      thread_ts=thread, current_message_ts=stamp)
        return {**normalized, "text": text, "threadTs": thread} if normalized is not None else None

    def stage(self, event: Mapping[str, Any], body: Mapping[str, Any], client: Any) -> tuple[dict[str, str], dict[str, Any] | None] | None:
        if not raw_event_scope(event, body):
            return None
        subtype = str(event.get("subtype") or "")
        if subtype and subtype not in {"message_changed", "message_deleted"}:
            return None
        deleted = subtype == "message_deleted"
        message = _data(event.get("previous_message")) if deleted else (_data(event.get("message")) if subtype else event)
        tenant, channel = str(body.get("team_id") or event.get("team") or ""), str(event.get("channel") or "")
        stamp = str(event.get("deleted_ts") or message.get("ts") or event.get("ts") or "")
        thread = str(message.get("thread_ts") or stamp)
        author = str(message.get("user") or "")
        _require(_STAMP.fullmatch(stamp) and _STAMP.fullmatch(thread) and Decimal(thread) <= Decimal(stamp),
                 "memory_raw_timestamp_invalid")
        bot = bool(message.get("bot_id") or message.get("bot_profile") or message.get("app_id")
                   or message.get("subtype") == "bot_message")
        allowed_author = False if deleted else self._author_allowed(tenant, author)
        if not subtype and (not allowed_author or bot):
            return None
        # 채널 정보가 확인되기 전에는 durable 영수증도 생성하지 않는다.
        self._private(client, tenant, channel)
        edited = str(_data(message.get("edited")).get("ts") or "")
        source = (self._source(message, tenant=tenant, channel=channel, thread=thread, stamp=stamp)
                  if not deleted and allowed_author and not bot else None)
        raw_type = "deleted" if deleted else "changed" if subtype else "created"
        if source is None and not deleted:
            if raw_type == "created":
                return None
            raw_type = "unavailable"
        # 생성은 실제 발언 ts만 사용해 native/app_mention의 서로 다른 envelope를 합친다.
        event_ts = stamp if raw_type == "created" else str(event.get("event_ts") or edited or stamp)
        _require(_STAMP.fullmatch(event_ts) and Decimal(event_ts) >= Decimal(stamp)
                 and (not edited or Decimal(event_ts) >= Decimal(edited)), "memory_raw_event_timestamp_invalid")
        revision = source_revision(str(message.get("text") or ""), edited) if not deleted else ""
        identity = _PREFIX + canonical_digest([tenant, channel, stamp, raw_type, event_ts, revision])
        receipt = {"tenantId": tenant, "channelId": channel, "messageTs": stamp, "threadTs": thread,
            "eventId": identity, "eventType": "changed" if raw_type in {"created", "changed"} else raw_type,
            "rawEventType": raw_type, "eventTs": event_ts,
            # 무효화는 작성자 권한 철회 뒤에도 본문 없이 전송할 수 있어야 한다.
            **({"authorId": author} if raw_type in {"created", "changed"} else {}),
            **({"revisionHash": revision} if revision else {})}
        return _validated_event(receipt), source

    def _refresh(self, receipt: Mapping[str, str], client: Any) -> tuple[str, str, str, dict[str, Any] | None]:
        """재시작 뒤 저장된 식별자로만 exact 원본을 읽고 현재 revision을 반환한다."""
        result = _data(client.conversations_replies(channel=receipt["channelId"], ts=receipt["threadTs"],
            oldest=receipt["messageTs"], latest=receipt["messageTs"], inclusive=True, limit=100))
        # 권한·네트워크 오류와 부분 페이지를 삭제로 해석해 성공 ACK하지 않는다.
        _require(result.get("ok") is True and not result.get("has_more")
                 and not _data(result.get("response_metadata")).get("next_cursor"),
                 "memory_raw_source_read_unverified")
        messages = [message for message in result.get("messages") or []
                    if isinstance(message, Mapping) and message.get("ts") == receipt["messageTs"]]
        _require(len(messages) <= 1, "memory_raw_source_ambiguous")
        if not messages:
            return "unavailable", receipt["eventTs"], "unavailable", None
        message = messages[0]
        _require(message.get("user") == receipt.get("authorId"), "memory_raw_source_author_changed")
        edited = str(_data(message.get("edited")).get("ts") or "")
        revision = source_revision(str(message.get("text") or ""), edited)
        source = self._source(message, tenant=receipt["tenantId"], channel=receipt["channelId"],
                              thread=receipt["threadTs"], stamp=receipt["messageTs"])
        raw_type = ("changed" if edited else "created") if source is not None else "unavailable"
        # 편집 시각이 없는 원본은 receipt의 고정 관측 시각을 유지해 재시도 payload를 바꾸지 않는다.
        event_ts = edited or receipt["eventTs"]
        return raw_type, event_ts, revision, source

    def deliver(self, receipt: Mapping[str, str], *, source: Mapping[str, Any] | None = None,
                source_provided: bool = False) -> dict[str, Any]:
        receipt = _validated_event(receipt)
        _require("rawEventType" in receipt and is_raw_only_scope(receipt["tenantId"], receipt["channelId"], receipt["messageTs"]),
                 "memory_raw_scope_denied")
        client = self.client()
        self._private(client, receipt["tenantId"], receipt["channelId"])
        raw_type, event_ts, identity = receipt["rawEventType"], receipt["eventTs"], receipt["eventId"]
        actual_source = dict(source) if source is not None else None
        if raw_type in {"created", "changed"} and not self._author_allowed(receipt["tenantId"], receipt.get("authorId", "")):
            # 본문 요청의 응답만 유실됐어도 새 binding으로 무효화해 이미 저장된 원문을 남기지 않는다.
            identity = _PREFIX + "refresh_" + canonical_digest([receipt["eventId"], "author_unavailable"])
            raw_type, actual_source = "unavailable", None
        elif raw_type in {"deleted", "unavailable"}:
            # 이전 버전 receipt에 authorId가 있어도 삭제/무효화에는 작성자 권한을 요구하지 않는다.
            actual_source = None
        else:
            if not source_provided:
                fresh_type, fresh_ts, revision, actual_source = self._refresh(receipt, client)
                if revision != receipt.get("revisionHash") or fresh_type != raw_type:
                    # 첨부 상태만 바뀌어 text hash가 같아도 원래 event ID의 payload를 바꾸지 않는다.
                    binding = revision if revision != receipt.get("revisionHash") else canonical_digest({
                        "revisionHash": revision, "eventType": fresh_type})
                    identity = _PREFIX + "refresh_" + canonical_digest([receipt["eventId"], binding])
                    raw_type, event_ts = fresh_type, fresh_ts
            if actual_source is not None:
                _require(actual_source.get("authorId") == receipt.get("authorId")
                         and actual_source.get("messageTs") == receipt["messageTs"]
                         and actual_source.get("threadTs") == receipt["threadTs"], "memory_raw_source_binding_invalid")
                if source_provided:
                    _require(actual_source.get("revisionHash") == receipt.get("revisionHash"),
                             "memory_raw_source_revision_invalid")
            else:
                raw_type = "unavailable"
        payload = {"tenantId": receipt["tenantId"], "channelId": receipt["channelId"],
            "messageTs": receipt["messageTs"], "eventId": identity, "eventTs": event_ts,
            "eventType": raw_type,
            **({"actorId": receipt["authorId"]} if raw_type in {"created", "changed"} else {})}
        if actual_source is not None and raw_type != "deleted":
            payload["source"] = actual_source
        response = self.api.call(_ENDPOINT, payload)
        _require(response.get("eventId") == identity and response.get("messageTs") == receipt["messageTs"]
                 and response.get("status") in _STATUSES and type(response.get("stored")) is bool,
                 "memory_raw_ack_invalid")
        # 새 revision event의 성공도 원래 durable receipt의 ACK에만 연결한다.
        return {**response, "receiptEventId": receipt["eventId"]}
