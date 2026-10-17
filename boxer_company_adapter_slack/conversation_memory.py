"""회사 대화 기억의 Slack 읽기·출처 확인·전송 경계다."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal, InvalidOperation
import json
import logging
import os
import re
import threading
import time
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit
import uuid

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.memory.access import can_read_memory_source, is_private_memory_hub
from boxer_company.memory.contracts import MAX_CHUNK_CHARS, canonical_digest, source_revision
from boxer_company.memory.policy import is_allowed_conversation_source, is_safe_memory_text
from boxer_company.memory.commands import parse_memory_command
from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES
from boxer_company_adapter_slack.memory_api_client import CompanyMemoryApiClient
from boxer_company_adapter_slack.memory_event_outbox import MemoryEventOutboxError, MemorySourceEventOutbox
from boxer_company_adapter_slack.company_api_client import _serialize_context_entries


_TS = re.compile(r"^\d{1,20}(?:\.\d{1,9})?$")
_CAPTURE_ID = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_ALLOWED_ROUTES = {"company_freeform", "company_team_fun"}
_PREFIX = "/internal/v1/memory"


def memory_enabled() -> bool:
    return os.getenv("BOXER_COMPANY_SLACK_MEMORY_ENABLED", "false").strip().lower() in {
        "1", "true", "yes", "on"
    }


def build_slack_conversation_memory(
    api: CompanyMemoryApiClient, *, is_allowed: Callable[[str, str], bool],
    logger: logging.Logger, environ: Mapping[str, str] | None = None,
) -> SlackConversationMemory | None:
    source = os.environ if environ is None else environ
    if str(source.get("BOXER_COMPANY_SLACK_MEMORY_ENABLED", "false")).strip().lower() not in {"1", "true", "yes", "on"}:
        return None
    try:
        raw_path = str(source.get("BOXER_COMPANY_SLACK_MEMORY_EVENT_STATE_PATH") or "").strip()
        if not raw_path:
            raise MemoryEventOutboxError("memory_event_state_path_missing")
        outbox = MemorySourceEventOutbox(raw_path)
        memory = SlackConversationMemory(api, is_allowed=is_allowed, logger=logger, event_outbox=outbox)
        logger.info("Conversation memory state=enabled pending_source_events=%s", outbox.pending_count())
        return memory
    except Exception as exc:
        # 선택형 기억 저장소만 닫고 기존 Slack 봇·operations 기동은 보존한다.
        logger.error("Conversation memory state=disabled reason=event_outbox_unavailable error_type=%s error_code=%s", type(exc).__name__, _event_error_code(exc))
        return None


def delivery_client_id(capture_id: str, part_index: int) -> str:
    # 재시작 후에도 같은 전달을 식별하며 전송 자체를 다시 실행하지 않는다.
    return str(uuid.uuid5(uuid.NAMESPACE_URL, f"boxer-memory:{capture_id}:{part_index}"))


def _data(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    payload = getattr(value, "data", None)
    return payload if isinstance(payload, Mapping) else {}


def _before_cutoff(ts: str, cutoff: str) -> bool:
    try:
        return bool(_TS.fullmatch(ts) and _TS.fullmatch(cutoff) and Decimal(ts) <= Decimal(cutoff))
    except InvalidOperation:
        return False


def _safe_permalink(value: Any, channel_id: str, message_ts: str) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = urlsplit(value)
    except ValueError:
        return None
    expected = "/archives/" + channel_id + "/p" + message_ts.replace(".", "")
    if (
        parsed.scheme != "https" or not (parsed.hostname or "").endswith(".slack.com")
        or parsed.username or parsed.password or parsed.fragment or parsed.path != expected
    ):
        return None
    return value


class SlackConversationMemory:
    def __init__(
        self,
        api: CompanyMemoryApiClient,
        *,
        is_allowed: Callable[[str, str], bool],
        logger: logging.Logger | None = None,
        clock: Callable[[], float] = time.monotonic,
        event_outbox: MemorySourceEventOutbox | None = None,
    ) -> None:
        self.api = api
        self.is_allowed = is_allowed
        self.logger = logger or logging.getLogger(__name__)
        self.clock = clock
        self.client: Any = None
        self.tenant_id = ""
        self.bot_user_id = ""
        self.worker_id = "slack-memory-" + uuid.uuid4().hex
        self._wake = threading.Event()
        self._stop = threading.Event()
        self._worker: threading.Thread | None = None
        self.event_outbox = event_outbox
        self._event_delivery_lock = threading.Lock()
        self._event_status = (
            "pending" if event_outbox is not None and event_outbox.pending_count()
            else "ok" if event_outbox is not None else "disabled"
        )
        self._profile_ids = {
            str(profile.get("user_id") or "") for profile in TEAM_MEMBER_PROFILES
            if profile.get("user_id")
        }

    def context_for(self, payload: Mapping[str, Any], *, manage: bool = False) -> dict[str, Any] | None:
        actor = str(payload.get("user_id") or "")
        tenant = str(payload.get("workspace_id") or "")
        channel = str(payload.get("channel_id") or "")
        current = str(payload.get("current_ts") or "")
        thread = str(payload.get("thread_ts") or current)
        if (
            not tenant or not actor or not channel or not _TS.fullmatch(current)
            or not _TS.fullmatch(thread) or not self.is_allowed(tenant, actor)
            or (not manage and actor not in self._profile_ids)
        ):
            return None
        channel_kind = self._channel_kind(payload, channel)
        if channel_kind is None:
            # 최초 수집의 잘못된 종류가 thread에 영구 고정되지 않게 기억만 건너뛴다.
            return None
        context = {
            "workspaceId": tenant, "channelId": channel, "threadTs": thread,
            "currentMessageTs": current, "actorId": actor,
            "channelKind": channel_kind,
            "triggerKind": "app_mention",
        }
        if manage and self.client is not None:
            try:
                permalink = _safe_permalink(_data(self.client.chat_getPermalink(channel=channel, message_ts=current)).get("permalink"), channel, current)
                if permalink:
                    context["currentMessagePermalink"] = permalink
                    # 명시적 기억 정정은 멘션 제거 전의 실제 source revision을 사용한다.
                    raw_text = str(payload.get("raw_text") or "").strip()
                    if raw_text and len(raw_text) <= MAX_CHUNK_CHARS and is_safe_memory_text(raw_text):
                        context["currentMessageText"] = raw_text
                        context["currentMessageEditedAt"] = ""
            except Exception as exc:
                self._failure("management_source", exc)
        return context

    def _channel_kind(self, payload: Mapping[str, Any], channel_id: str) -> str | None:
        # adapter가 이미 확인한 종류를 우선하고, ID만으로 비공개 채널을 추측하지 않는다.
        kind = payload.get("channel_kind")
        if isinstance(kind, str) and kind in {"channel", "private_channel", "dm", "group_dm"}:
            return str(kind)
        channel_type = payload.get("channel_type")
        if isinstance(channel_type, str) and channel_type in {"im", "mpim", "group"}:
            return {"im": "dm", "mpim": "group_dm", "group": "private_channel"}[channel_type]
        if payload.get("is_im") is True or channel_id.startswith("D"):
            return "dm"
        if payload.get("is_mpim") is True:
            return "group_dm"
        if isinstance(payload.get("is_private"), bool):
            return "private_channel" if payload["is_private"] else "channel"
        try:
            # 짧은 별도 client로 분류하며 공유 client의 timeout·retry는 변경하지 않는다.
            client = self._bounded_client(.5)
            if client is not None:
                result = _data(client.conversations_info(channel=channel_id, include_num_members=False))
                info = _data(result.get("channel"))
                if result.get("ok") is not False and info and str(info.get("id") or channel_id) == channel_id:
                    if info.get("is_im") is True:
                        return "dm"
                    if info.get("is_mpim") is True:
                        return "group_dm"
                    if info.get("is_private") is True:
                        return "private_channel"
                    if info.get("is_private") is False:
                        return "channel"
        except Exception as exc:
            self._failure("channel_kind", exc)
        # 분류 실패를 공개 채널로 저장하지 않고 해당 turn의 기억 기능만 닫는다.
        return None

    def prepare(self, request: CompanyAssistantRequest) -> CompanyAssistantRequest:
        context = dict(request.memory_context or {})
        if not context or self.client is None:
            return request
        # 후보 확보는 딱 한 번만 하고 검증 실패는 기억 없는 기존 답변으로 닫는다.
        try:
            manage = parse_memory_command(request.question) is not None
            entries = _serialize_context_entries(request.context_entries, max_entries=100 if manage else 12, max_chars=12_000 if manage else 5_000, max_entry_chars=12_000 if manage else 5_000)
            result = self.api.call(_PREFIX + "/retrievals", {
                "tenantId": request.tenant_id, "actorId": request.actor_id,
                "channel": "slack", "conversationId": request.conversation_id,
                "question": request.question, "contextEntries": entries,
                "memoryContext": context, "requestId": request.request_id,
            })
            retrieval_id = result.get("retrievalId")
            candidates = result.get("sources")
            if not isinstance(retrieval_id, str) or not isinstance(candidates, list):
                return request
            context["retrievalId"] = retrieval_id
            context["verifications"] = self.verify_sources(
                candidates, channel_id=context["channelId"], tenant_id=request.tenant_id,
                actor_id=request.actor_id,
            )
            plan = result.get("liveQuery")
            if isinstance(plan, Mapping) and is_private_memory_hub(request.tenant_id, context["channelId"]):
                try:
                    from boxer_company_adapter_slack.private_live_retrieval import collect_private_live_sources

                    # 서버 발행 범위가 이 turn과 정확히 일치할 때만 고정 Slack 읽기 API를 호출한다.
                    expected = {"tenantId": request.tenant_id, "actorId": request.actor_id,
                                "channelId": context["channelId"], "requestId": request.request_id,
                                "question": request.question, "beforeTs": context["currentMessageTs"]}
                    authors = plan.get("authorIds")
                    if any(plan.get(key) != value for key, value in expected.items()) or not isinstance(authors, list):
                        raise ValueError("live_query_scope_invalid")
                    live = collect_private_live_sources(client_factory=self._bounded_client,
                        tenant_id=request.tenant_id, destination_channel_id=context["channelId"],
                        actor_id=str(request.actor_id or ""), question=request.question,
                        author_ids=authors, before_ts=context["currentMessageTs"],
                        is_allowed=self.is_allowed, clock=self.clock)
                    self.api.call(_PREFIX + "/retrievals/" + retrieval_id + "/live", {
                        "tenantId": request.tenant_id, "actorId": request.actor_id,
                        "channelId": context["channelId"], "requestId": request.request_id,
                        "sources": live["sources"], "coverage": live["coverage"],
                        "channelCount": live["channelCount"],
                    })
                except Exception as exc:
                    # 실시간 읽기 실패도 저장 기억 검증과 일반 답변을 버리지 않는다.
                    self._failure("live_retrieval", exc)
            return replace(request, memory_context=context)
        except Exception as exc:
            self._failure("prepare", exc)
            return request

    def verify_sources(self, sources: list[Any], *, channel_id: str, tenant_id: str,
                       actor_id: str | None = None) -> list[dict[str, str]]:
        # 기존 Slack client의 긴 timeout/자동 retry를 답변 대기 시간에 들이지 않는다.
        deadline = self.clock() + 2
        groups: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
        # API 후보 상한도 다시 적용해 선택형 기억 때문에 본 turn이 422가 되지 않는다.
        for source in sources[:64]:
            if not isinstance(source, Mapping):
                continue
            origin = str(source.get("channelId") or "")
            style_only = source.get("styleOnly") is True
            # 허브 예외는 API가 반환한 원본 종류와 고정 tenant·목적지 정책으로만 판단한다.
            # 개인 답변 선호는 허브에서도 기존 본인 원문 검증 경계를 그대로 유지한다.
            if origin != channel_id and style_only:
                allowed = bool(origin and actor_id)
            else:
                allowed = can_read_memory_source(
                    tenant_id, channel_id, origin, str(source.get("channelKind") or ""),
                )
            if not allowed:
                continue
            thread = str(source.get("threadTs") or "")
            group = (origin, thread)
            if not _TS.fullmatch(thread) or (group not in groups and len(groups) >= 3):
                continue
            groups.setdefault(group, []).append(source)
        verified: list[dict[str, str]] = []
        memberships: dict[str, bool] = {}
        for (origin, thread), candidates in groups.items():
            # 허브의 일반 비공개 기억은 원채널 소속 대신 명시된 공유 정책을 사용한다.
            # styleOnly는 목적지에 관계없이 원채널 소속과 자기 발언을 모두 확인한다.
            if origin != channel_id and any(source.get("styleOnly") is True for source in candidates):
                if origin not in memberships:
                    memberships[origin] = self._origin_member(origin, actor_id or "", deadline)
                if not memberships[origin]:
                    candidates = [source for source in candidates if source.get("styleOnly") is not True]
            expected = {str(s.get("messageTs")): s for s in candidates if _TS.fullmatch(str(s.get("messageTs") or ""))}
            if not expected or self.clock() >= deadline:
                continue
            remaining = max(.05, deadline - self.clock())
            # 요청별 bounded SDK clone은 공유 Slack client의 timeout을 바꾸지 않는다.
            client = self._bounded_client(remaining)
            if client is None:
                continue
            cursor = ""
            seen: dict[str, Mapping[str, Any]] = {}
            complete = False
            try:
                while self.clock() < deadline:
                    client = self._bounded_client(max(.05, deadline - self.clock()))
                    result = _data(client.conversations_replies(
                        channel=origin, ts=thread, limit=100,
                        oldest=min(expected, key=Decimal), latest=max(expected, key=Decimal),
                        inclusive=True, cursor=cursor or None,
                    ))
                    if result.get("ok") is False:
                        break
                    for message in result.get("messages") or []:
                        if isinstance(message, Mapping):
                            seen[str(message.get("ts") or "")] = message
                    cursor = str(_data(result.get("response_metadata")).get("next_cursor") or "")
                    complete = not cursor and not result.get("has_more")
                    if complete or all(ts in seen for ts in expected) or not cursor:
                        break
                for ts, candidate in expected.items():
                    message = seen.get(ts)
                    if message is None:
                        status, revision = ("deleted" if complete else "unavailable"), str(candidate.get("revisionHash") or "")
                    else:
                        edited = str(_data(message.get("edited")).get("ts") or "")
                        revision = source_revision(str(message.get("text") or ""), edited)
                        status = "verified" if revision == candidate.get("revisionHash") else "changed"
                        author = str(message.get("user") or "")
                        if not author or (author != self.bot_user_id and not self.is_allowed(tenant_id, author)):
                            status = "unavailable"
                        if candidate.get("styleOnly") is True and (author != actor_id or message.get("bot_id")):
                            status = "unavailable"
                    verified.append({"sourceId": str(candidate.get("sourceId") or ""), "revisionHash": revision, "status": status})
            except Exception as exc:
                self._failure("verify", exc)
        return verified

    def _origin_member(self, channel_id: str, actor_id: str, deadline: float) -> bool:
        # 사용자 token의 공개채널 읽기 권한을 사용자 본인의 채널 소속으로 오인하지 않는다.
        cursor = ""
        try:
            for _ in range(5):
                if self.clock() >= deadline:
                    return False
                client = self._bounded_client(max(.05, deadline - self.clock()))
                if client is None:
                    return False
                result = _data(client.conversations_members(channel=channel_id, limit=1000, cursor=cursor or None))
                if result.get("ok") is False:
                    return False
                if actor_id in (result.get("members") or []):
                    return True
                next_cursor = str(_data(result.get("response_metadata")).get("next_cursor") or "")
                if not next_cursor or next_cursor == cursor:
                    return False
                cursor = next_cursor
        except Exception as exc:
            self._failure("origin_membership", exc)
        return False

    def _bounded_client(self, timeout: float) -> Any:
        from slack_sdk import WebClient

        if isinstance(self.client, WebClient):
            return WebClient(token=self.client.token, base_url=self.client.base_url,
                             timeout=timeout, retry_handlers=[])
        # 테스트/주입 client는 호출자에게 timeout 제어 책임이 있다.
        return self.client

    def delivery_reply(self, reply: Callable[..., Any], capture: Mapping[str, Any], receipts: list[dict[str, Any]]) -> Callable[..., Any]:
        part_index = 0
        def send(text: str, **kwargs: Any) -> Any:
            nonlocal part_index
            index = part_index
            part_index += 1
            client_id = delivery_client_id(str(capture["captureId"]), index)
            response = reply(text, **{**kwargs, "client_msg_id": client_id})
            data = _data(response)
            ts = str(data.get("ts") or "")
            if _TS.fullmatch(ts):
                message = _data(data.get("message"))
                parts = capture.get("answerParts") or []
                delivery_id = parts[index].get("deliveryId", client_id) if index < len(parts) and isinstance(parts[index], Mapping) else client_id
                receipt = {"deliveryId": delivery_id, "partIndex": index, "messageTs": ts, "clientMsgId": client_id}
                if message.get("user") and message.get("text"):
                    receipt.update({"authorId": str(message["user"]), "textHash": canonical_digest(str(message["text"]).strip())})
                receipts.append(receipt)
            return response
        return send

    def record_delivery(self, capture: Mapping[str, Any], receipts: list[dict[str, Any]]) -> None:
        try:
            if capture.get("claimToken"):
                self.api.call(self._capture_path(capture, "checkpoint"), {
                    "tenantId": capture["tenantId"], "actorId": capture["actorId"],
                    "claimToken": capture["claimToken"], "deliveryReceipts": receipts,
                })
        except Exception as exc:
            self._failure("delivery", exc)
        self._wake.set()

    def source_event(self, event: Mapping[str, Any], body: Mapping[str, Any], client: Any) -> bool:
        del client
        subtype = event.get("subtype")
        if subtype not in {"message_changed", "message_deleted"}:
            return False
        message = _data(event.get("message"))
        ts = str(event.get("deleted_ts") or message.get("ts") or "")
        tenant = str(body.get("team_id") or event.get("team") or "")
        if not tenant or not _TS.fullmatch(ts):
            return False
        try:
            payload = {
                "tenantId": tenant, "channelId": str(event.get("channel") or ""),
                "messageTs": ts, "eventType": "deleted" if subtype == "message_deleted" else "changed",
                "eventId": str(body.get("event_id") or canonical_digest([tenant, event.get("channel"), ts, subtype, event.get("event_ts")])),
                **({"revisionHash": source_revision(str(message.get("text") or ""), str(_data(message.get("edited")).get("ts") or ""))} if subtype == "message_changed" else {}),
            }
            if self.event_outbox is None:
                raise MemoryEventOutboxError("memory_event_outbox_missing")
            # 원문 없는 식별자부터 fsync하므로 API 장애·프로세스 종료 뒤에도 재전송할 수 있다.
            self.event_outbox.enqueue(payload)
            self._event_status = "pending"
            self._wake.set()
        except Exception as exc:
            self._event_status = "degraded"
            self.logger.error("Conversation memory source_events state=degraded stage=enqueue error_type=%s error_code=%s", type(exc).__name__, _event_error_code(exc))
            return False
        self.drain_source_events(limit=1)
        return True

    def drain_source_events(self, *, limit: int = 25) -> int:
        if self.event_outbox is None or not self._event_delivery_lock.acquire(blocking=False):
            return 0
        acknowledged = 0
        try:
            for event in self.event_outbox.pending(limit=limit):
                response = self.api.call(_PREFIX + "/sources/events", event)
                # HTTP 성공만으로 지우지 않고 exact event의 적용 확인을 받아야 한다.
                if (response.get("eventId") != event["eventId"]
                        or type(response.get("invalidatedCount")) is not int
                        or response["invalidatedCount"] < 0):
                    raise MemoryEventOutboxError("memory_source_event_ack_invalid")
                self.event_outbox.acknowledge(event)
                acknowledged += 1
            pending = self.event_outbox.pending_count()
            self._event_status = "pending" if pending else "ok"
            if acknowledged:
                self.logger.info("Conversation memory source_events state=%s acknowledged=%s pending=%s", self._event_status, acknowledged, pending)
        except Exception as exc:
            # ACK 전 종료·실패는 같은 event ID로 다시 보내 API 멱등 처리를 사용한다.
            self._event_status = "degraded"
            self.logger.warning("Conversation memory source_events state=degraded stage=drain error_type=%s error_code=%s", type(exc).__name__, _event_error_code(exc))
        finally:
            self._event_delivery_lock.release()
        return acknowledged

    def status(self) -> dict[str, Any]:
        try:
            pending = self.event_outbox.pending_count() if self.event_outbox is not None else None
        except Exception:
            self._event_status, pending = "degraded", None
        return {"sourceEvents": self._event_status, "pendingSourceEvents": pending}

    def start(self, client: Any, tenant_id: str) -> None:
        self.client, self.tenant_id = client, tenant_id
        if self._worker is not None:
            return
        self._worker = threading.Thread(target=self._run, name="boxer-memory-transport", daemon=True)
        self._worker.start()

    def stop(self) -> None:
        self._stop.set()
        self._wake.set()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.poll_once()
            except Exception as exc:
                self._failure("poll", exc)
            self._wake.wait(30)
            self._wake.clear()

    def poll_once(self) -> None:
        # capture가 없거나 Slack 읽기가 실패해도 삭제·수정 receipt부터 복구한다.
        self.drain_source_events()
        if not self.bot_user_id:
            try:
                self.bot_user_id = str(_data(self.client.auth_test()).get("user_id") or "")
            except Exception as exc:
                # bot identity가 불명확하면 사람 원본만 수집하고 봇 근거는 제외한다.
                self._failure("bot_identity", exc)
        result = self.api.call(_PREFIX + "/captures/pull", {
            "tenantId": self.tenant_id, "workerId": self.worker_id, "limit": 3,
        })
        for capture in result.get("captures") or []:
            if not isinstance(capture, Mapping) or capture.get("tenantId") != self.tenant_id:
                continue
            try:
                self.collect_capture(capture)
            except Exception as exc:
                self._failure("collect", exc)

    def collect_capture(self, capture: Mapping[str, Any]) -> None:
        tenant, actor = str(capture["tenantId"]), str(capture["actorId"])
        if not self.is_allowed(tenant, actor) or actor not in self._profile_ids:
            self._checkpoint(capture, state="canceled")
            return
        if capture.get("route") not in _ALLOWED_ROUTES:
            return
        channel, thread, cutoff = str(capture["channelId"]), str(capture["threadTs"]), str(capture["currentMessageTs"])
        checkpoint = _data(capture.get("checkpoint"))
        state = json.loads(str(checkpoint.get("cursor") or '{}'))
        cursor, number = str(state.get("slackCursor") or ""), int(state.get("nextChunkNo") or 0)
        digests = {int(chunk["chunkNo"]): str(chunk["chunkDigest"]) for chunk in capture.get("receivedChunks") or []}
        coverage = str(state.get("coverage") or "complete")
        saw_current = bool(state.get("sawCurrent"))
        context_gap = bool(state.get("contextGap"))
        receipts = list(checkpoint.get("deliveryReceipts") or [])
        if len({receipt.get("partIndex") for receipt in receipts if isinstance(receipt, Mapping)}) < len(capture.get("answerParts") or []):
            coverage = "partial"
        checkpoint_cursor = checkpoint.get("cursor")
        last_heartbeat = self.clock()

        def heartbeat(*, force: bool = False) -> None:
            nonlocal last_heartbeat
            if force or self.clock() - last_heartbeat >= 20:
                # 페이지 중에는 직전 확정 cursor를 보존해야 재시작 시 같은 chunk를 재전송한다.
                self._checkpoint(capture, cursor=checkpoint_cursor, coverage=coverage)
                last_heartbeat = self.clock()

        # 한 poll의 Slack 부하를 제한하고 API checkpoint부터 다음 poll에 이어간다.
        for _ in range(5):
            heartbeat(force=True)
            response = _data(self._bounded_client(5).conversations_replies(
                channel=channel, ts=thread, latest=cutoff, inclusive=True,
                limit=100, cursor=cursor or None,
            ))
            if response.get("ok") is False:
                raise ValueError("slack_thread_unavailable")
            sources = []
            for message in response.get("messages") or []:
                heartbeat()
                if not isinstance(message, Mapping) or not _before_cutoff(str(message.get("ts") or ""), cutoff):
                    continue
                saw_current = saw_current or message.get("ts") == cutoff
                # 빠진 운영/봇 대화 뒤의 짧은 후속 발언은 의미를 추측하지 않는다.
                # 최종 freeform 판정이 끝난 현재 멘션만 새 수집 구간을 연다.
                if message.get("ts") == cutoff and message.get("user") == actor:
                    context_gap = False
                if context_gap:
                    coverage = "partial"
                    continue
                source = self._human_source(message, tenant=tenant, channel=channel)
                if source is None:
                    coverage = "partial"
                    context_gap = True
                else:
                    sources.append(source)
            for chunk in self._chunks(sources):
                number = self._put_chunk(capture, number, chunk, digests, coverage=coverage)
            cursor = str(_data(response.get("response_metadata")).get("next_cursor") or "")
            if not cursor and response.get("has_more"):
                self._checkpoint(capture, cursor=checkpoint_cursor, coverage="partial")
                return
            if cursor:
                checkpoint_cursor = json.dumps({"slackCursor": cursor, "nextChunkNo": number, "coverage": coverage, "sawCurrent": saw_current, "contextGap": context_gap})
                self._checkpoint(capture, cursor=checkpoint_cursor, coverage=coverage)
                continue
            if not saw_current:
                coverage = "partial"
            # 현재 응답은 cutoff 이후지만 exact 전달 ID로만 추가한다.
            bot_sources = self._delivered_sources(capture, receipts, heartbeat=heartbeat)
            if len(bot_sources) < len(receipts):
                coverage = "partial"
            for chunk in self._chunks(bot_sources):
                number = self._put_chunk(capture, number, chunk, digests, coverage=coverage)
            self._put_chunk(capture, number, [], digests, coverage=coverage, final=True)
            return

    def _human_source(self, message: Mapping[str, Any], *, tenant: str, channel: str) -> dict[str, Any] | None:
        actor, text = str(message.get("user") or ""), str(message.get("text") or "").strip()
        if (message.get("bot_id") or message.get("subtype") or message.get("files")
                or not actor or not self.is_allowed(tenant, actor)
                or not is_allowed_conversation_source(text) or len(text) > MAX_CHUNK_CHARS):
            return None
        return self._source(message, channel=channel, author_kind="human")

    def _source(self, message: Mapping[str, Any], *, channel: str, author_kind: str) -> dict[str, Any] | None:
        ts, text = str(message.get("ts") or ""), str(message.get("text") or "").strip()
        if not _TS.fullmatch(ts) or not text or len(text) > MAX_CHUNK_CHARS:
            return None
        permalink = _safe_permalink(_data(self._bounded_client(5).chat_getPermalink(channel=channel, message_ts=ts)).get("permalink"), channel, ts)
        if not permalink:
            return None
        edited = str(_data(message.get("edited")).get("ts") or "")
        return {"messageTs": ts, "revisionHash": source_revision(text, edited),
                "authorId": str(message.get("user") or ""), "authorKind": author_kind,
                "text": text, "permalink": permalink, "createdAt": ts, "editedAt": edited}

    def _delivered_sources(self, capture: Mapping[str, Any], receipts: list[Any], *, heartbeat: Callable[[], None] | None = None) -> list[dict[str, Any]]:
        if not self.bot_user_id:
            return []
        result = []
        channel, thread = str(capture["channelId"]), str(capture["threadTs"])
        for receipt in receipts:
            if heartbeat is not None:
                heartbeat()
            if not isinstance(receipt, Mapping) or not _TS.fullmatch(str(receipt.get("messageTs") or "")):
                continue
            ts = str(receipt["messageTs"])
            page = _data(self._bounded_client(5).conversations_replies(channel=channel, ts=thread, oldest=ts, latest=ts, inclusive=True, limit=10))
            for message in page.get("messages") or []:
                if (not isinstance(message, Mapping) or message.get("ts") != ts
                        or message.get("user") != self.bot_user_id
                        or not is_safe_memory_text(str(message.get("text") or ""))):
                    continue
                source = self._source(message, channel=channel, author_kind="boxer")
                if source is not None:
                    source.update({"route": capture["route"], "deliveryVerified": True})
                    result.append(source)
        return result

    @staticmethod
    def _chunks(sources: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
        chunks: list[list[dict[str, Any]]] = []
        current: list[dict[str, Any]] = []
        size = 0
        for source in sources:
            text_size = len(source["text"])
            if current and (len(current) >= 100 or size + text_size > MAX_CHUNK_CHARS):
                chunks.append(current)
                current, size = [], 0
            current.append(source)
            size += text_size
        if current:
            chunks.append(current)
        return chunks

    def _put_chunk(self, capture: Mapping[str, Any], number: int, sources: list[Any], digests: dict[int, str], *, coverage: str, final: bool = False) -> int:
        digest = canonical_digest(sources)
        if number in digests and digests[number] != digest:
            # 수정된 페이지를 동일 번호에 덮어쓰지 않고 현재 capture를 중단한다.
            self._checkpoint(capture, state="canceled")
            raise ValueError("memory_chunk_revision_conflict")
        digests[number] = digest
        self.api.call(self._capture_path(capture, "chunks"), {
            "tenantId": capture["tenantId"], "actorId": capture["actorId"],
            "claimToken": capture["claimToken"], "chunkNo": number,
            "sources": sources, "coverage": coverage, "isLast": final,
            **({"totalChunks": number + 1, "inputDigest": canonical_digest([digests[n] for n in range(number + 1)])} if final else {}),
        })
        return number + 1

    def _checkpoint(self, capture: Mapping[str, Any], **fields: Any) -> None:
        self.api.call(self._capture_path(capture, "checkpoint"), {
            "tenantId": capture["tenantId"], "actorId": capture["actorId"],
            "claimToken": capture["claimToken"], **fields,
        })

    @staticmethod
    def _capture_path(capture: Mapping[str, Any], suffix: str) -> str:
        capture_id = str(capture.get("captureId") or "")
        if not _CAPTURE_ID.fullmatch(capture_id):
            raise ValueError("memory_capture_id_invalid")
        return f"{_PREFIX}/captures/{capture_id}/{suffix}"

    def _failure(self, stage: str, exc: Exception) -> None:
        self.logger.warning("Conversation memory transport failed stage=%s error_type=%s", stage, type(exc).__name__)


def _event_error_code(exc: Exception) -> str:
    # 외부 exception 메시지는 원문을 포함할 수 있어 내부 고정 코드만 관측에 남긴다.
    return exc.code if isinstance(exc, MemoryEventOutboxError) else "transport_or_storage_error"


__all__ = ["SlackConversationMemory", "build_slack_conversation_memory", "delivery_client_id", "memory_enabled"]
