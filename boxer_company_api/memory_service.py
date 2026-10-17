"""답변 runtime과 durable 기억 저장소 사이의 scope·출처 경계."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import replace
import logging
import re
import threading
import time
from typing import Any, Callable, Sequence
from uuid import uuid4

from boxer_company.assistant.contracts import AssistantMessage, CompanyAssistantRequest, CompanyAssistantResult
from boxer_company.memory.commands import MEMORY_MANAGE_ROUTE, parse_memory_command
from boxer_company.memory.contracts import canonical_digest, source_revision
from boxer_company.memory.policy import is_safe_memory_text
from boxer_company_api.memory_schemas import MemoryCaptureOutput


_LOG = logging.getLogger(__name__)
_ELIGIBLE_ROUTES = frozenset({"company_freeform", "company_team_fun"})


class MemoryService:
    def __init__(self, store: Any, *, enabled: bool = False, read_enabled: bool = False,
                 profile_ids: frozenset[str] | None = None,
                 flush_deletions: Callable[[Any], Any] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.store = store
        self.enabled = enabled
        self.read_enabled = read_enabled
        self._profiles = profile_ids
        self._flush_deletions = flush_deletions
        self._clock = clock
        self._cache: OrderedDict[str, tuple[float, str, list[dict[str, Any]]]] = OrderedDict()
        self._lock = threading.Lock()

    def status(self) -> str:
        if not self.enabled:
            return "disabled"
        try:
            return "ok" if self.store is not None and self.store.health() else "degraded"
        except Exception:
            return "degraded"

    def _profiled(self, actor_id: str | None) -> bool:
        if self._profiles is None:
            from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES
            profiles = frozenset(str(profile.get("user_id") or "") for profile in TEAM_MEMBER_PROFILES)
        else:
            profiles = self._profiles
        return bool(actor_id and actor_id in profiles)

    def _eligible(self, request: CompanyAssistantRequest, *, management: bool = False) -> bool:
        memory = request.memory_context or {}
        return bool(self.enabled and self.store is not None and request.channel == "slack"
                    and bool(request.actor_id) and (management or self._profiled(request.actor_id))
                    and memory.get("triggerKind") == "app_mention"
                    and memory.get("workspaceId") == request.tenant_id
                    and memory.get("actorId") == request.actor_id
                    and memory.get("threadTs") == request.conversation_id
                    and memory.get("channelId") == request.metadata.get("channel_id"))

    @staticmethod
    def _binding(request: CompanyAssistantRequest) -> str:
        # 후보는 actor·질문·현재 문맥·원 요청 ID에 고정되어 다른 turn에 재사용되지 않는다.
        memory = request.memory_context or {}
        return canonical_digest({
            "requestId": request.request_id, "tenantId": request.tenant_id,
            "actorId": request.actor_id, "channelId": memory.get("channelId"),
            "threadTs": request.conversation_id, "currentMessageTs": memory.get("currentMessageTs"),
            "question": request.question, "context": [dict(entry) for entry in request.context_entries],
        })

    def prepare(self, request: CompanyAssistantRequest) -> dict[str, Any]:
        management = parse_memory_command(request.question) is not None
        if (not self.read_enabled and not management) or not self._eligible(request, management=management) or not is_safe_memory_text(request.question):
            return {"retrievalId": None, "sources": []}
        memory = request.memory_context or {}
        if parse_memory_command(request.question) is not None:
            candidates = self.store.list_own_memories(
                tenant_id=request.tenant_id, channel_id=memory["channelId"], actor_id=request.actor_id,
            )
        else:
            mentioned = set(re.findall(r"<@([A-Z0-9]+)>", request.question))
            from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES
            for profile in TEAM_MEMBER_PROFILES:
                aliases = (str(profile.get("name") or ""), *profile.get("aliases", ()))
                if any(alias and re.search(r"(?<!\w)" + re.escape(str(alias)) + r"(?!\w)", request.question, re.I) for alias in aliases):
                    mentioned.add(str(profile.get("user_id") or ""))
            candidates = self.store.retrieval_candidates(
                tenant_id=request.tenant_id, channel_id=memory["channelId"],
                actor_id=request.actor_id, thread_ts=request.conversation_id,
                question=request.question, limit=8, mentioned_user_ids=tuple(mentioned - {""}),
            )
        # 한 번 확인할 수 있는 원본 스레드만 골라 조회를 무한 확장하지 않는다.
        threads: set[tuple[str, str]] = set()
        selected, refs = [], {}
        for candidate in candidates:
            candidate_threads = {(source["channelId"], source["threadTs"]) for source in candidate["sources"]}
            candidate_ids = {source["sourceId"] for source in candidate["sources"]}
            if len(threads | candidate_threads) > 3 or len(set(refs) | candidate_ids) > 64:
                continue
            threads |= candidate_threads
            selected.append(candidate)
            for source in candidate["sources"]:
                refs[source["sourceId"]] = {key: source[key] for key in (
                    "sourceId", "channelId", "threadTs", "messageTs", "revisionHash")}
                if candidate.get("styleOnly"):
                    # 내부 adapter 검증에만 사용하며 모델에는 정규화된 선호만 전달한다.
                    refs[source["sourceId"]]["styleOnly"] = True
            if len(selected) >= 8:
                break
        retrieval_id = uuid4().hex
        now = self._clock()
        with self._lock:
            self._cache = OrderedDict((key, value) for key, value in self._cache.items() if value[0] > now)
            while len(self._cache) >= 1024:
                self._cache.popitem(last=False)
            self._cache[retrieval_id] = (now + 60, self._binding(request), selected)
        return {"retrievalId": retrieval_id, "sources": list(refs.values()), "expiresIn": 60}

    def bind_verified(self, request: CompanyAssistantRequest) -> CompanyAssistantRequest:
        management = parse_memory_command(request.question) is not None
        if (not self.read_enabled and not management) or not self._eligible(request, management=management):
            return request
        memory = request.memory_context or {}
        with self._lock:
            prepared = self._cache.pop(str(memory.get("retrievalId") or ""), None)
        if prepared is None or prepared[0] <= self._clock() or prepared[1] != self._binding(request):
            return request
        try:
            source_refs = {source["sourceId"]: source for item in prepared[2] for source in item["sources"]}
            for check in memory.get("verifications") or []:
                source = source_refs.get(check.get("sourceId"))
                if source and check.get("status") in {"changed", "deleted", "unavailable"}:
                    self.store.source_event(
                        tenant_id=request.tenant_id, channel_id=source["channelId"],
                        message_ts=source["messageTs"], event_type=check["status"],
                        event_id="verify:" + canonical_digest([request.request_id, source["sourceId"], check["status"]]),
                        revision_hash=check.get("revisionHash"),
                    )
            verified = self.store.load_verified_items(
                tenant_id=request.tenant_id, channel_id=memory["channelId"],
                candidates=prepared[2], verified_sources=list(memory.get("verifications") or []), limit=8,
                actor_id=request.actor_id,
            )
            return replace(request, memory_entries=tuple(verified))
        except Exception as exc:
            _LOG.warning("memory_read_failed error_type=%s", type(exc).__name__)
            return request

    def capture(self, request: CompanyAssistantRequest, result: CompanyAssistantResult | None,
                *, answer_parts: Sequence[str] | None = None) -> CompanyAssistantResult | None:
        if (result is None or result.outcome != "answered" or result.route not in _ELIGIBLE_ROUTES
                or not self._eligible(request) or not is_safe_memory_text(request.question)):
            return result
        memory = request.memory_context or {}
        try:
            parts = answer_parts if answer_parts is not None else tuple(
                message.body for message in result.messages if message.delivery_scope == "conversation")
            receipt = self.store.create_capture(
                tenant_id=request.tenant_id, actor_id=request.actor_id,
                channel_id=memory["channelId"], thread_ts=request.conversation_id,
                current_message_ts=memory["currentMessageTs"], channel_kind=memory["channelKind"],
                request_id=request.request_id, route=result.route,
                answer_parts=tuple({"partIndex": index, "textHash": canonical_digest(body.strip())}
                                   for index, body in enumerate(parts)),
            )
            # 선택형 확장 직렬화 오류도 일반 답변을 망가뜨리지 않게 여기서 검증한다.
            validated = MemoryCaptureOutput.model_validate(receipt).model_dump(exclude_none=True)
            return replace(result, memory_capture=validated)
        except Exception as exc:
            # 기억 장애가 이미 만들어진 정상 답변을 실패로 바꾸지 않는다.
            _LOG.warning("memory_capture_failed error_type=%s", type(exc).__name__)
            return result

    def manage(self, request: CompanyAssistantRequest) -> CompanyAssistantResult | None:
        if request.metadata.get("operation_action") is not None:
            return None
        command = parse_memory_command(request.question)
        if command is None:
            return None

        def reply(text: str, outcome: str = "answered") -> CompanyAssistantResult:
            return CompanyAssistantResult(route=MEMORY_MANAGE_ROUTE, outcome=outcome,
                                          messages=(AssistantMessage(body=text),))

        if not self._eligible(request, management=True) or self.status() != "ok":
            return reply("지금은 기억 기능을 사용할 수 없어. 저장하거나 삭제했다고 처리하지 않았어", "failed")
        memory = request.memory_context or {}
        scope = {"tenant_id": request.tenant_id, "channel_id": memory["channelId"], "actor_id": request.actor_id}
        try:
            if command.action in {"list", "choose_forget", "choose_suppress"}:
                own = [item for item in request.memory_entries if item.get("kind") == "person"
                       and item.get("subjectUserId") == request.actor_id]
                if not own:
                    return reply("이 대화에서 원본을 확인할 수 있는 네 기억이 없어")
                rows = [f"• `{item['memoryId']}`: {item['text']}" for item in own]
                body = "이 채널에서 기억하고 있는 내용이야.\n" + "\n".join(rows)
                if command.action == "choose_forget":
                    body += "\n지울 항목을 `기억 ID 잊어줘`로 지정해줘"
                elif command.action == "choose_suppress":
                    body += "\n앞으로도 기억하지 않을 항목을 `기억 ID 주제는 기억하지 마`로 지정해줘"
                return reply(body, "needs_input" if command.action != "list" else "answered")
            if command.action in {"forget", "suppress"}:
                if command.action == "forget":
                    result = self.store.forget_item(**scope, memory_id=command.memory_id, request_id=request.request_id)
                else:
                    own = self.store.list_own_memories(**scope)
                    target = next((item for item in own if item["memoryId"] == command.memory_id), None)
                    if target is None:
                        return reply("네 기억 항목 ID를 지정해줘", "needs_input")
                    result = self.store.suppress_topic(**scope, topic_key=target["topicKey"], request_id=request.request_id)
                with self._lock:
                    self._cache.clear()
                durable = result.get("durabilityState") == "durable"
                if self._flush_deletions is not None:
                    try:
                        self._flush_deletions(self.store)
                        durable = True
                    except Exception:
                        durable = False
                return reply(("해당 주제는 앞으로도 기억하지 않을게" if command.action == "suppress" else "해당 기억을 잊었어") if durable
                             else "현재 기억에서는 제외했어. 백업 복구에도 삭제가 유지되는지 확인 중이라 삭제 완료는 아직 아니야")
            if command.action == "choose_preference":
                return reply("답변 선호는 짧게·자세히·한국어·영어·결론 먼저 중에서 지정해줘", "needs_input")
            if command.action == "set_preference":
                permalink = memory.get("currentMessagePermalink")
                source_text = memory.get("currentMessageText")
                edited_at = memory.get("currentMessageEditedAt") or ""
                if not permalink or not source_text:
                    return reply("현재 발언의 원본 링크를 확인하지 못해서 선호를 저장하지 못했어", "failed")
                self.store.set_own_preference(
                    **scope, thread_ts=request.conversation_id, current_message_ts=memory["currentMessageTs"],
                    request_id=request.request_id, topic_key=command.topic_key, text=command.text,
                    source={"messageTs": memory["currentMessageTs"], "authorId": request.actor_id,
                            "authorKind": "human", "text": source_text, "permalink": permalink,
                            "createdAt": memory["currentMessageTs"], "editedAt": edited_at,
                            "revisionHash": source_revision(source_text, edited_at)},
                )
                return reply(f"앞으로 네 답변에 적용할 선호를 저장했어: {command.text}")
        except Exception as exc:
            _LOG.warning("memory_manage_failed error_type=%s", type(exc).__name__)
            return reply("기억 변경을 완료하지 못했어. 항목 ID와 현재 대화 범위를 확인해줘", "failed")
        return None
