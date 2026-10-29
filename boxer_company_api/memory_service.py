"""답변 runtime과 durable 기억 저장소 사이의 scope·출처 경계."""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass, replace
from decimal import Decimal
from typing import Any, Callable, Mapping, Sequence
from uuid import uuid4

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.memory.access import (
    can_query_private_live_sources,
    is_private_memory_hub,
)
from boxer_company.memory.commands import MEMORY_MANAGE_ROUTE, parse_memory_command
from boxer_company.memory.contracts import (
    MemoryError,
    canonical_digest,
    source_revision,
)
from boxer_company.memory.conversation_count import (
    format_conversation_count_answer,
    parse_conversation_count_query,
)
from boxer_company.memory.conversation_history import format_conversation_history_answer
from boxer_company.memory.live_limits import (
    LIVE_CACHE_MAX_BYTES,
    LIVE_RETRIEVAL_LEASE_SECONDS,
)
from boxer_company.memory.live_query import (
    STORED_HISTORY_START_TS,
    is_supported_stored_period,
    is_within_live_query_period,
    live_query_period_error,
    resolve_live_query_period,
    wants_private_live_sources,
    wants_stored_history_sources,
)
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
    live_size: int = 0


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

    def profiles(self) -> frozenset[str]:
        """수집 transport도 일반 기억과 같은 서버 팀원 registry를 사용한다."""
        if self._profiles is not None:
            return self._profiles
        from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES

        return frozenset(str(profile.get("user_id") or "") for profile in TEAM_MEMBER_PROFILES
                         if profile.get("user_id"))

    def _count_query(self, request: CompanyAssistantRequest) -> dict[str, Any] | None:
        memory = request.memory_context or {}
        return parse_conversation_count_query(request.question, str(memory.get("currentMessageTs") or ""),
            sorted(self._mentioned_user_ids(request.question)))

    def conversation_count(self, request: CompanyAssistantRequest) -> CompanyAssistantResult | None:
        query = self._count_query(request)
        if query is None:
            return None
        # 기존 DB의 발췌만 즉시 읽는다. 원문 재수집과 별도 작업 상태를 만들지 않는다.
        memory = request.memory_context or {}
        notice, outcome = None, "needs_input"
        result: dict[str, Any] = {"status": "failure"}
        if not self._eligible(request) or not self.read_enabled:
            notice, outcome = "지금은 저장된 대화 기록을 조회할 수 없어", "failed"
        elif query.get("periodError") == "unbounded_past":
            result = {"status": "unsupported_period"}
        elif query.get("periodError"):
            notice = "조회할 날짜를 하나의 유효한 기간으로 지정해줘"
        elif not query.get("period"):
            notice = "집계할 기간을 지정해줘. 예: 디디가 올해 욕한 메시지는 몇 건이야?"
        elif not is_supported_stored_period(query["period"]):
            result = {"status": "unsupported_period"}
        elif len(query.get("authorIds", [])) != 1 or not self._profiled(query["authorIds"][0]):
            notice = "집계할 팀원 한 명을 이름이나 멘션으로 지정해줘"
        elif not can_query_private_live_sources(request.tenant_id, str(memory.get("channelId") or ""), memory.get("channelKind")):
            notice = "저장된 대화 집계는 비공개 채널에서 요청해줘"
        else:
            from boxer_company.memory.conversation_count_store import (
                count_stored_messages,
            )
            from boxer_company.team_chat_context import get_registered_team_member_name

            try:
                result = count_stored_messages(self.store, tenant_id=request.tenant_id,
                    channel_id=memory["channelId"], author_id=query["authorIds"][0], period=query["period"],
                    include_messages=query["includeDetails"])
                result["authorDisplayName"] = get_registered_team_member_name(query["authorIds"][0]) or ""
                outcome = "answered"
            except Exception as exc:  # noqa: BLE001 - DB 장애를 실제 횟수 0건이나 모델 추정으로 대체하지 않는다.
                _LOG.warning("stored_conversation_count_failed error_type=%s", type(exc).__name__)
                outcome = "failed"
        # 숫자 질문은 집계를 유지하고 발언 조회는 같은 근거의 시간순 목록과 저장된 주제로 표현한다.
        formatter = format_conversation_history_answer if query["includeDetails"] else format_conversation_count_answer
        return CompanyAssistantResult(route="company_conversation_count", outcome=outcome,
            messages=(AssistantMessage(body=notice or formatter(result)),), used_llm=False)

    def stored_history_response(self, request: CompanyAssistantRequest, *, route: str = "company_freeform") -> CompanyAssistantResult | None:
        if not self._eligible(request) or parse_memory_command(request.question) is not None:
            return None
        memory = request.memory_context or {}
        stamp = str(memory.get("currentMessageTs") or "")
        period = resolve_live_query_period(request.question, stamp)
        unbounded_past = live_query_period_error(request.question, stamp) == "unbounded_past"
        if (not unbounded_past and (period is None or is_supported_stored_period(period))) or not wants_stored_history_sources(
                request.question, tuple(self._mentioned_user_ids(request.question))):
            return None
        # 지원 이전 기간과 걸친 범위는 조회나 모델 호출 전에 같은 안내로 끝낸다.
        return CompanyAssistantResult(route=route, outcome="needs_input", used_llm=False,
            messages=(AssistantMessage(body="대화 기록 조회는 2026년 1월 1일부터 지원해. 그 이전 기간은 지원하지 않아. 조회할 기간을 다시 알려줘."),))

    def is_stored_history_request(self, request: CompanyAssistantRequest) -> bool:
        return bool(self._count_query(request) is not None or wants_stored_history_sources(
            request.question, tuple(self._mentioned_user_ids(request.question))))

    def stored_history_request(self, request: CompanyAssistantRequest) -> CompanyAssistantRequest:
        if parse_memory_command(request.question) is not None or not self.is_stored_history_request(request):
            return request
        # 요청 바인딩 확인 뒤 caller가 보낸 과거 스레드 원문을 버리고 DB 기억만 답변에 사용한다.
        metadata = dict(request.metadata)
        metadata.pop("team_fun_context", None)
        return replace(request, context_entries=(), metadata=metadata)

    @staticmethod
    def _stored_period(request: CompanyAssistantRequest) -> Mapping[str, str] | None:
        memory = request.memory_context or {}
        stamp = str(memory.get("currentMessageTs") or "")
        period = resolve_live_query_period(request.question, stamp)
        if period is not None:
            return period
        # 기간을 생략해도 지원 시작일보다 앞선 저장 기록을 검색 범위에 섞지 않는다.
        return {"startTs": STORED_HISTORY_START_TS, "endTs": str(Decimal(stamp) + 1), "cutoffTs": stamp}

    @staticmethod
    def _stored_result(request: CompanyAssistantRequest, entries: Sequence[Mapping[str, Any]]) -> CompanyAssistantRequest:
        ordinary = [item for item in entries if item.get("kind") != "personal_style"]
        if not ordinary and not wants_private_live_sources(request.question,
                tuple(MemoryService._mentioned_user_ids(request.question))):
            # 인사와 일반 지식 답변에는 불필요한 대화 기록 조회 안내를 붙이지 않는다.
            return replace(request, memory_entries=tuple(entries))
        notice = {"kind": "retrieval_status", "sourceMode": "stored", "supportedFrom": "2026-01-01",
                  "coverage": "partial" if ordinary else "unavailable", "sourceCount": len(ordinary),
                  "scope": "accessible_private_channels" if is_private_memory_hub(request.tenant_id,
                      str((request.memory_context or {}).get("channelId") or "")) else "current_private_channel"}
        return replace(request, memory_entries=(*entries, notice))

    def _prune_live_sources(self, now: float) -> None:
        # 60초 수집 중 도착한 수정과 삭제도 원 요청 lease가 끝날 때까지 기억한다.
        self._live_source_events = {key: value for key, value in self._live_source_events.items()
                                    if now - value[0] < LIVE_RETRIEVAL_LEASE_SECONDS}
        for prepared in self._cache.values():
            if prepared.live_received_at is not None and now - prepared.live_received_at > 10:
                prepared.live_entries = ()
                prepared.live_size = 0

    def status(self) -> str:
        if not self.enabled:
            return "disabled"
        try:
            # readiness 확인 때도 지난 lease의 원문 참조를 정리해 실패 turn의 캐시를 남기지 않는다.
            with self._lock:
                now = self._clock()
                self._prune_live_sources(now)
                self._cache = OrderedDict((key, value) for key, value in self._cache.items() if value.expires_at > now)
            return "ok" if self.store is not None and self.store.health() else "degraded"
        except Exception:
            return "degraded"

    def _profiled(self, actor_id: str | None) -> bool:
        return bool(actor_id and actor_id in self.profiles())

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
        count_query = None if management else self._count_query(request)
        if count_query is not None:
            return {"retrievalId": None, "sources": [], "storedOnly": True}
        stamp = str(memory.get("currentMessageTs") or "")
        period_error = None if management else live_query_period_error(request.question, stamp)
        period = None if management else self._stored_period(request)
        mentioned: set[str] = set()
        if parse_memory_command(request.question) is not None:
            candidates = self.store.list_own_memories(
                tenant_id=request.tenant_id, channel_id=memory["channelId"], actor_id=request.actor_id,
            )
        elif period_error is not None or (period is not None and not is_supported_stored_period(period)):
            # 잘못되거나 상충한 날짜를 무시한 채 최신 기억과 실시간 조회로 대체하지 않는다.
            candidates = []
        else:
            # 프로필과 같은 조사 경계로 작성자 ID를 정해 저장된 근거의 검색에만 사용한다.
            mentioned = self._mentioned_user_ids(request.question)
            candidates = self.store.retrieval_candidates(
                tenant_id=request.tenant_id, channel_id=memory["channelId"],
                actor_id=request.actor_id, thread_ts=request.conversation_id,
                question=request.question, limit=8, mentioned_user_ids=tuple(mentioned - {""}),
                period=period,
            )
        # 한 번 확인할 수 있는 원본 스레드만 골라 조회를 무한 확장하지 않는다.
        threads: set[tuple[str, str]] = set()
        selected, refs = [], {}
        for candidate in candidates:
            # 기간 밖 출처가 섞인 요약이 검증 예산을 먼저 차지하지 않게 후보 단계에서도 거른다.
            if (period is not None and candidate.get("kind") != "personal_style"
                    and candidate.get("styleOnly") is not True and not (
                    candidate.get("sources") and all(is_within_live_query_period(
                        source.get("messageTs"), period) for source in candidate["sources"]))):
                continue
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
        # 일반 질문은 저장된 후보만 발행하고 Slack 원문 조회 계획을 만들지 않는다.
        live_query = None
        destination_kind = memory.get("channelKind")
        with self._lock:
            self._prune_live_sources(now)
            self._cache = OrderedDict((key, value) for key, value in self._cache.items() if value.expires_at > now)
            while len(self._cache) >= 1024:
                self._cache.popitem(last=False)
            self._cache[retrieval_id] = _PreparedRetrieval(now + LIVE_RETRIEVAL_LEASE_SECONDS, self._binding(request), selected,
                live_query, live_destination_channel_kind=destination_kind,
                live_generation=self._live_generation)
        result = {"retrievalId": retrieval_id, "sources": list(refs.values()), "expiresIn": LIVE_RETRIEVAL_LEASE_SECONDS}
        if not management:
            result["storedOnly"] = True
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
            if coverage not in {"complete", "partial", "unavailable"} or type(channel_count) is not int or channel_count < 0:
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
                live_size = len(json.dumps(entries, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
                if live_size > LIVE_CACHE_MAX_BYTES:
                    raise MemoryError("memory_live_result_too_large")
                # 늘어난 원문이 동시 요청 수만큼 쌓이지 않게 오래된 원문 lease부터 제외한다.
                for cached_id, cached in list(self._cache.items()):
                    if sum(value.live_size for value in self._cache.values()) + live_size <= LIVE_CACHE_MAX_BYTES:
                        break
                    if cached_id != retrieval_id and cached.live_size:
                        del self._cache[cached_id]
                prepared.live_entries = entries
                prepared.live_size = live_size
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
                    # 삭제된 본문의 예산도 즉시 반환해 다른 정상 요청을 불필요하게 비우지 않는다.
                    prepared.live_size = len(json.dumps(remaining, ensure_ascii=False,
                        separators=(",", ":")).encode("utf-8")) if remaining else 0
                    prepared.live_coverage = "partial"

    def bind_verified(self, request: CompanyAssistantRequest) -> CompanyAssistantRequest:
        management = parse_memory_command(request.question) is not None
        if (not self.read_enabled and not management) or not self._eligible(request, management=management):
            return request
        memory = request.memory_context or {}
        with self._lock:
            prepared = self._cache.pop(str(memory.get("retrievalId") or ""), None)
        if prepared is None or prepared.expires_at <= self._clock() or prepared.binding != self._binding(request):
            return self._stored_result(request, ()) if not management else request
        if not management and (live_query_period_error(request.question, str(memory.get("currentMessageTs") or "")) is not None
                or not is_supported_stored_period(self._stored_period(request))):
            # prepare와 같은 원 질문으로 다시 판정해 잘못된 기간에 과거 기억이 끼어들지 않게 한다.
            return self._stored_result(replace(request, memory_entries=()), ())
        try:
            if not management:
                verified = self.store.load_stored_items(tenant_id=request.tenant_id,
                    channel_id=memory["channelId"], candidates=prepared.candidates, limit=8, actor_id=request.actor_id)
            else:
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
            period = None if management else self._stored_period(request)
            if period is not None:
                # 날짜 질문에 과거 요약을 그날의 발언처럼 섞지 않는다. 본인 표준 선호는 보존한다.
                verified = [item for item in verified if item.get("kind") == "personal_style" or (
                    item.get("sources") and all(isinstance(source, Mapping)
                        and is_within_live_query_period(source.get("messageTs"), period) for source in item["sources"]))]
            return self._stored_result(request, verified) if not management else replace(request, memory_entries=tuple(verified))
        except Exception as exc:
            _LOG.warning("memory_read_failed error_type=%s", type(exc).__name__)
            return self._stored_result(request, ()) if not management else request

    def capture(self, request: CompanyAssistantRequest, result: CompanyAssistantResult | None,
                *, answer_parts: Sequence[str] | None = None) -> CompanyAssistantResult | None:
        if (result is None or result.outcome != "answered" or result.route not in _ELIGIBLE_ROUTES
                or not self._eligible(request) or not is_safe_memory_text(request.question)):
            return result
        memory = request.memory_context or {}
        from boxer_company.memory.raw_policy import is_raw_only_channel

        # 원문 시범 기간이 끝나도 두 채널의 요약 추출을 다시 켜지 않는다.
        if is_raw_only_channel(request.tenant_id, str(memory.get("channelId") or "")):
            return result
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
                from boxer_company.memory.raw_policy import is_raw_only_channel

                if is_raw_only_channel(request.tenant_id, memory["channelId"]):
                    return reply("현재 이 채널은 원문 보존 모드라 선호나 요약 기억을 새로 추출하지 않아.", "needs_input")
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
