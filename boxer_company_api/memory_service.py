"""답변 runtime과 durable 기억 저장소 사이의 scope·출처 경계."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, replace
import logging
import re
import threading
import time
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

from boxer_company.assistant.contracts import AssistantMessage, CompanyAssistantRequest, CompanyAssistantResult
from boxer_company.memory.commands import MEMORY_MANAGE_ROUTE, parse_memory_command
from boxer_company.memory.access import can_query_private_live_sources, is_private_memory_hub
from boxer_company.memory.contracts import MemoryError, canonical_digest, source_revision
from boxer_company.memory.live_query import is_within_live_query_period, resolve_live_query_period, wants_private_live_sources
from boxer_company.memory.policy import is_safe_memory_text
from boxer_company.team_member_aliases import matches_team_member_alias
from boxer_company_api.memory_schemas import MemoryCaptureOutput


_LOG = logging.getLogger(__name__)
_ELIGIBLE_ROUTES = frozenset({"company_freeform", "company_team_fun"})


@dataclass
class _PreparedRetrieval:
    expires_at: float
    binding: str
    candidates: list[dict[str, Any]]
    live_query: dict[str, Any] | None = None
    live_destination_channel_kind: str | None = None
    live_fingerprint: str | None = None
    live_entries: tuple[dict[str, Any], ...] = ()
    live_coverage: str = "unavailable"
    live_channel_count: int = 0
    live_received_at: float | None = None
    live_generation: int = 0


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
        self._cache: OrderedDict[str, _PreparedRetrieval] = OrderedDict()
        # 진행 중인 원문 조회에만 필요한 짧은 무효화 표식이며 원문은 보관하지 않는다.
        self._live_source_events: dict[tuple[str, str, str], tuple[float, str, str | None]] = {}
        self._live_generation = 0
        self._lock = threading.Lock()

    def _prune_live_sources(self, now: float) -> None:
        self._live_source_events = {key: value for key, value in self._live_source_events.items() if now - value[0] < 60}

    def status(self) -> str:
        if not self.enabled:
            return "disabled"
        try:
            # readiness 확인 때도 지난 lease의 원문 참조를 정리해 실패 turn의 캐시를 남기지 않는다.
            with self._lock:
                now = self._clock()
                self._prune_live_sources(now)
                self._cache = OrderedDict((key, value) for key, value in self._cache.items() if value.expires_at > now)
                for value in self._cache.values():
                    if value.live_received_at is not None and now - value.live_received_at > 10:
                        value.live_entries = ()
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
            "channelKind": memory.get("channelKind"),
            "threadTs": request.conversation_id, "currentMessageTs": memory.get("currentMessageTs"),
            "question": request.question, "context": [dict(entry) for entry in request.context_entries],
        })

    @staticmethod
    def _mentioned_user_ids(question: str) -> set[str]:
        from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES

        # Slack의 표시명 첨부 멘션도 ID를 정본으로 읽고 표시명 자체는 신뢰하지 않는다.
        mentioned = set(re.findall(r"<@([A-Z0-9]+)(?:\|[^<>]+)?>", question))
        alias_question = re.sub(r"<@[^<>]*>", " ", question)
        for profile in TEAM_MEMBER_PROFILES:
            aliases = (str(profile.get("name") or ""), *profile.get("aliases", ()))
            if matches_team_member_alias(alias_question, aliases):
                mentioned.add(str(profile.get("user_id") or ""))
        return mentioned - {""}

    def prepare(self, request: CompanyAssistantRequest) -> dict[str, Any]:
        management = parse_memory_command(request.question) is not None
        if (not self.read_enabled and not management) or not self._eligible(request, management=management) or not is_safe_memory_text(request.question):
            return {"retrievalId": None, "sources": []}
        memory = request.memory_context or {}
        mentioned: set[str] = set()
        if parse_memory_command(request.question) is not None:
            candidates = self.store.list_own_memories(
                tenant_id=request.tenant_id, channel_id=memory["channelId"], actor_id=request.actor_id,
            )
        else:
            # 프로필과 같은 조사 경계로 작성자 ID를 정하고 원문 조회 계획에도 재사용한다.
            mentioned = self._mentioned_user_ids(request.question)
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
                # 출처 DB의 채널 종류를 전달해 지정 허브의 private-channel 예외를 검증한다.
                if source.get("channelKind"):
                    refs[source["sourceId"]]["channelKind"] = source["channelKind"]
                if candidate.get("styleOnly"):
                    # 내부 adapter 검증에만 사용하며 모델에는 정규화된 선호만 전달한다.
                    refs[source["sourceId"]]["styleOnly"] = True
            if len(selected) >= 8:
                break
        retrieval_id = uuid4().hex
        now = self._clock()
        # 허브 외에는 바인딩된 비공개 원채널만 읽으며 관리 명령은 제외한다.
        live_query = None
        destination_kind = memory.get("channelKind")
        if (not management and can_query_private_live_sources(request.tenant_id,
                str(memory["channelId"]), destination_kind)
                and wants_private_live_sources(request.question, tuple(mentioned))):
            live_query = {"tenantId": request.tenant_id, "actorId": request.actor_id,
                          "channelId": memory["channelId"], "requestId": request.request_id,
                          "question": request.question, "authorIds": sorted(mentioned - {""}),
                          "beforeTs": memory["currentMessageTs"]}
        with self._lock:
            self._prune_live_sources(now)
            self._cache = OrderedDict((key, value) for key, value in self._cache.items() if value.expires_at > now)
            while len(self._cache) >= 1024:
                self._cache.popitem(last=False)
            self._cache[retrieval_id] = _PreparedRetrieval(now + 60, self._binding(request), selected,
                live_query, live_destination_channel_kind=destination_kind,
                live_generation=self._live_generation)
        result = {"retrievalId": retrieval_id, "sources": list(refs.values()), "expiresIn": 60}
        if live_query is not None:
            result["liveQuery"] = live_query
        return result

    def receive_live(self, *, retrieval_id: str, tenant_id: str, actor_id: str,
                     channel_id: str, request_id: str, sources: Sequence[dict[str, Any]],
                     coverage: str, channel_count: int,
                     context_messages: Sequence[dict[str, Any]] = ()) -> dict[str, Any]:
        from boxer_company.memory.live import normalize_live_sources

        # 원문을 DB나 작업 큐에 쓰지 않고 원 요청의 짧은 1회용 캐시에만 보관한다.
        with self._lock:
            prepared = self._cache.get(retrieval_id)
            plan = prepared.live_query if prepared else None
            if (prepared is None or prepared.expires_at <= self._clock() or plan is None
                    or not can_query_private_live_sources(tenant_id, channel_id,
                        prepared.live_destination_channel_kind)
                    or any(plan.get(key) != value for key, value in
                           (("tenantId", tenant_id), ("actorId", actor_id),
                            ("channelId", channel_id), ("requestId", request_id)))):
                raise MemoryError("memory_live_retrieval_not_found")
            if coverage not in {"complete", "partial", "unavailable"} or type(channel_count) is not int or not 0 <= channel_count <= 200:
                raise MemoryError("memory_live_result_invalid")
            # 원채널 조회가 다른 채널까지 확인한 결과처럼 표시되지 않게 범위 수치도 제한한다.
            if not is_private_memory_hub(tenant_id, channel_id) and channel_count > 1:
                raise MemoryError("memory_live_channel_count_invalid")
            if coverage == "unavailable" and (sources or context_messages):
                raise MemoryError("memory_live_result_invalid")
            fingerprint = canonical_digest([sources, list(context_messages), coverage, channel_count])
            if prepared.live_fingerprint is not None and prepared.live_fingerprint != fingerprint:
                raise MemoryError("memory_live_result_conflict")
            if prepared.live_fingerprint is None:
                entries = normalize_live_sources(sources, tenant_id=tenant_id,
                    destination_channel_id=channel_id, before_ts=plan["beforeTs"],
                    author_ids=plan["authorIds"], question=plan["question"],
                    destination_channel_kind=prepared.live_destination_channel_kind,
                    context_messages=context_messages)
                if channel_count < len({source["channelId"] for item in entries for source in item["sources"]}):
                    raise MemoryError("memory_live_channel_count_invalid")
                self._prune_live_sources(self._clock())
                entries = tuple(item for item in entries if all(
                    self._live_source_current(tenant_id, source) for source in item["sources"]))
                # 안전필터·수정/삭제 표식 때문에 누락된 결과를 완전한 원문 조사라고 표시하지 않는다.
                supplied_count = len({(source["channelId"], source["messageTs"], source["revisionHash"])
                    for source in [*sources, *context_messages]})
                accepted_count = len({(source["channelId"], source["messageTs"], source["revisionHash"])
                    for item in entries for source in item["sources"]})
                prepared.live_entries = entries
                prepared.live_fingerprint = fingerprint
                prepared.live_coverage = "partial" if coverage == "complete" and accepted_count < supplied_count else coverage
                prepared.live_channel_count = channel_count
                prepared.live_received_at = self._clock()
            return {"retrievalId": retrieval_id, "acceptedSourceCount": len(prepared.live_entries)}

    def _live_source_current(self, tenant_id: str, source: dict[str, Any]) -> bool:
        event = self._live_source_events.get((tenant_id, source["channelId"], source["messageTs"]))
        return event is None or event[1] == "changed" and event[2] == source["revisionHash"]

    def invalidate_live_source(self, *, tenant_id: str, channel_id: str, message_ts: str,
                               event_type: str, revision_hash: str | None = None) -> None:
        # 원본 알림이 수집 전후 어느 쪽에 도착해도 이미 무효화된 본문을 답변에 넣지 않는다.
        with self._lock:
            now = self._clock()
            self._prune_live_sources(now)
            key = (tenant_id, channel_id, message_ts)
            previous = self._live_source_events.get(key)
            # Slack 삭제는 같은 메시지 ID에서 복구되지 않는다. 늦게 온 옛 수정 알림이
            # 삭제 표식을 덮어써 이미 읽어둔 본문을 다시 허용하지 않게 삭제를 우선한다.
            if previous is not None and previous[1] == "deleted":
                event_type, revision_hash = "deleted", None
            elif previous is not None and (
                previous[1] == "unavailable"
                or previous[1] == event_type == "changed" and previous[2] != revision_hash
            ):
                # 이벤트에 순서 시각이 없으므로 서로 다른 수정본의 선후는 추정하지 않는다.
                # 충돌한 수정은 표식 유효 시간 동안 닫아 지연 이벤트의 옛 본문 재노출을 막는다.
                event_type, revision_hash = "unavailable", None
            self._live_source_events[key] = (now, event_type, revision_hash)
            if len(self._live_source_events) > 4096:
                # 무효화 표식 상한에서 오래된 요청을 계속 허용하지 않고 원문 lease 자체를 폐기한다.
                self._cache.clear()
                self._live_source_events.clear()
                self._live_generation += 1
                return
            for prepared in self._cache.values():
                if prepared.live_query is None or prepared.live_query["tenantId"] != tenant_id:
                    continue
                remaining = tuple(item for item in prepared.live_entries if all(
                    self._live_source_current(tenant_id, source) for source in item["sources"]))
                if len(remaining) != len(prepared.live_entries):
                    prepared.live_entries = remaining
                    prepared.live_coverage = "partial"

    def _unavailable_live_result(self, request: CompanyAssistantRequest) -> CompanyAssistantRequest:
        memory = request.memory_context or {}
        channel_id = str(memory.get("channelId") or "")
        if (can_query_private_live_sources(request.tenant_id, channel_id, memory.get("channelKind"))
                and wants_private_live_sources(request.question, tuple(self._mentioned_user_ids(request.question)))):
            # 캐시 유실·검증 장애도 접근 권한 부재로 오해하지 않도록 원문 없는 상태만 남긴다.
            return replace(request, memory_entries=({"kind": "retrieval_status", "coverage": "unavailable",
                "channelCount": 0, "sourceCount": 0, "scope": "accessible_private_channels"
                    if is_private_memory_hub(request.tenant_id, channel_id) else "current_private_channel"},))
        return request

    def bind_verified(self, request: CompanyAssistantRequest) -> CompanyAssistantRequest:
        management = parse_memory_command(request.question) is not None
        if (not self.read_enabled and not management) or not self._eligible(request, management=management):
            return request
        memory = request.memory_context or {}
        with self._lock:
            prepared = self._cache.pop(str(memory.get("retrievalId") or ""), None)
        if prepared is None or prepared.expires_at <= self._clock() or prepared.binding != self._binding(request):
            return self._unavailable_live_result(request) if not management else request
        try:
            source_refs = {source["sourceId"]: source for item in prepared.candidates for source in item["sources"]}
            for check in memory.get("verifications") or []:
                source = source_refs.get(check.get("sourceId"))
                if source and check.get("status") in {"changed", "deleted", "unavailable"}:
                    self.store.source_event(
                        tenant_id=request.tenant_id, channel_id=source["channelId"],
                        message_ts=source["messageTs"], event_type=check["status"],
                        event_id="verify:" + canonical_digest([request.request_id, source["sourceId"], check["status"]]),
                        revision_hash=check.get("revisionHash"),
                    )
                    self.invalidate_live_source(tenant_id=request.tenant_id, channel_id=source["channelId"],
                        message_ts=source["messageTs"], event_type=check["status"],
                        revision_hash=check.get("revisionHash"))
            verified = self.store.load_verified_items(
                tenant_id=request.tenant_id, channel_id=memory["channelId"],
                candidates=prepared.candidates, verified_sources=list(memory.get("verifications") or []), limit=8,
                actor_id=request.actor_id,
            )
            period = resolve_live_query_period(request.question, str(memory.get("currentMessageTs") or ""))
            if period is not None:
                # 날짜 질문에 과거 요약을 그날의 발언처럼 섞지 않는다. 본인 표준 선호는 보존한다.
                verified = [item for item in verified if item.get("kind") == "personal_style" or (
                    item.get("sources") and all(isinstance(source, Mapping)
                        and is_within_live_query_period(source.get("messageTs"), period) for source in item["sources"]))]
            if prepared.live_query is not None:
                # 최신 발언은 오래된 저장 요약보다 먼저 넣되 본인 표준 답변 선호는 보존한다.
                with self._lock:
                    # DB 원본 확인 중 도착한 이벤트도 마지막 모델 입력 경계에서 다시 반영한다.
                    now = self._clock()
                    fresh = (prepared.live_received_at is not None
                        and prepared.expires_at > now and now - prepared.live_received_at <= 10
                        and prepared.live_generation == self._live_generation)
                    live = tuple(item for item in prepared.live_entries if all(
                        self._live_source_current(request.tenant_id, source) for source in item["sources"])) if fresh else ()
                coverage = prepared.live_coverage if fresh else "unavailable"
                if fresh and len(live) < len(prepared.live_entries):
                    coverage = "partial"
                styles = [item for item in verified if item.get("kind") == "personal_style"]
                ordinary = [item for item in verified if item.get("kind") != "personal_style"]
                # 일반 질문은 저장 요약과 현재 원문을 함께, 최근 발언 질문은 시각순 원문부터 보여준다.
                recent_utterances = period is not None or bool(re.search(r"최근|마지막|방금|요즘|직전", request.question)
                    and re.search(r"말|발언|대화|썼|쓴|적은|보낸|메시지|글", request.question))
                if recent_utterances:
                    evidence = [*styles, *live, *ordinary][:8]
                else:
                    evidence = [*styles, *ordinary[:3], *live[:4]][:8]
                    seen = {str(item.get("memoryId") or "") for item in evidence}
                    evidence.extend(item for item in [*ordinary[3:], *live[4:]]
                                    if item.get("memoryId") not in seen)
                    evidence = evidence[:8]
                notice = {"kind": "retrieval_status", "coverage": coverage,
                          "channelCount": prepared.live_channel_count, "sourceCount": len(live),
                          "scope": "accessible_private_channels" if is_private_memory_hub(
                              request.tenant_id, memory["channelId"]) else "current_private_channel"}
                return replace(request, memory_entries=tuple([*evidence, notice]))
            return replace(request, memory_entries=tuple(verified))
        except Exception as exc:
            _LOG.warning("memory_read_failed error_type=%s", type(exc).__name__)
            return self._unavailable_live_result(request) if not management else request

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
