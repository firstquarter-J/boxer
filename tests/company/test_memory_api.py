"""기억 transport가 기존 route·권한과 현재 원본 확인을 우회하지 않는 회귀."""

from dataclasses import replace
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import AssistantMessage, CompanyAssistantResult
from boxer_company.assistant.freeform_route import CompanyFreeformAssistantRoute
from boxer_company.memory.commands import parse_memory_command
from boxer_company.memory.contracts import canonical_digest
from boxer_company.memory.presentation import MEMORY_GUIDANCE, render_memory
from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.schemas import AssistantTurnInput
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings, load_company_api_settings


TS = "1790000000.000001"
NOW = "1790000000.000002"
TOKEN = "s" * 48
REQUEST_ID = "memory-test-1"


def memory_context(**overrides):
    return {"workspaceId": "T1", "channelId": "C1", "threadTs": TS,
            "currentMessageTs": NOW, "actorId": "U1", "channelKind": "channel",
            "triggerKind": "app_mention", **overrides}


def payload(**overrides):
    return {"tenantId": "T1", "actorId": "U1", "channel": "slack", "conversationId": TS,
            "question": "전에 답변 방식에 대해 뭐라고 했지?", "locale": "ko",
            "contextEntries": [], "scope": {"channelContextId": "C1"},
            "routeGroup": "freeform", "memoryContext": memory_context(), **overrides}


def domain(**overrides):
    return AssistantTurnInput.model_validate(payload(**overrides)).to_company_request(REQUEST_ID)


def item():
    return {"memoryId": "mem_example", "kind": "person", "category": "preference",
            "subjectUserId": "U1", "topicKey": "response_length", "text": "짧은 답변을 선호함",
            "evidenceType": "self_statement", "status": "active", "version": 1,
            "updatedAt": "2026-10-16T01:00:00Z", "threadId": "th1", "threadTs": TS,
            "deletionEpoch": 0, "sources": [{"sourceId": "src_example", "channelId": "C1",
            "threadTs": TS, "messageTs": TS, "revisionHash": "a" * 64,
            "permalink": f"https://example.slack.com/archives/C1/p{TS.replace('.', '')}",
            "authorId": "U1", "authorKind": "human"}]}


class Store:
    def __init__(self):
        self.captures = []
        self.events = []
        self.broken = False
        self.items = [item()]

    def health(self):
        return not self.broken

    def retrieval_candidates(self, **kwargs):
        return self.items

    def list_own_memories(self, **kwargs):
        return self.items

    def load_verified_items(self, **kwargs):
        return self.items if kwargs["verified_sources"] == [{"sourceId": "src_example", "revisionHash": "a" * 64, "status": "verified"}] else []

    def source_event(self, **kwargs):
        self.events.append(kwargs)
        invalidated_count = len(self.items)
        self.items = []
        return {"eventId": "evt_test_internal" if invalidated_count else None,
                "invalidatedCount": invalidated_count}

    def create_capture(self, **kwargs):
        if self.broken:
            raise RuntimeError("raw secret must not escape")
        self.captures.append(kwargs)
        return {"captureId": "capture1", "tenantId": kwargs["tenant_id"], "actorId": kwargs["actor_id"],
                "channelId": kwargs["channel_id"], "threadTs": kwargs["thread_ts"],
                "currentMessageTs": kwargs["current_message_ts"], "channelKind": kwargs["channel_kind"],
                "requestId": kwargs["request_id"], "route": kwargs["route"], "state": "collecting",
                "deletionEpoch": 0, "claimToken": "claim1", "leaseUntil": "2026-10-16T00:00:00Z",
                "answerParts": [{**part, "deliveryId": "delivery1"} for part in kwargs["answer_parts"]], "checkpoint": {}, "receivedChunks": []}

    def pull_captures(self, **kwargs):
        return []


def service(store=None, **kwargs):
    return MemoryService(store or Store(), enabled=True, read_enabled=True,
                         profile_ids=frozenset({"U1"}), **kwargs)


def test_memory_retrieval_is_bound_to_turn_and_expires():
    clock = [1.0]
    runtime = service(clock=lambda: clock[0])
    request = domain()
    prepared = runtime.prepare(request)
    assert "text" not in str(prepared)
    verified = {**request.memory_context, "retrievalId": prepared["retrievalId"],
                "verifications": [{"sourceId": "src_example", "revisionHash": "a" * 64, "status": "verified"}]}
    assert runtime.bind_verified(replace(request, memory_context=verified)).memory_entries
    assert not runtime.bind_verified(replace(request, memory_context=verified)).memory_entries
    prepared = runtime.prepare(request)
    verified["retrievalId"] = prepared["retrievalId"]
    assert not runtime.bind_verified(replace(request, question="다른 질문", memory_context=verified)).memory_entries
    prepared = runtime.prepare(request)
    verified["retrievalId"] = prepared["retrievalId"]
    clock[0] = 62
    assert not runtime.bind_verified(replace(request, memory_context=verified)).memory_entries


def test_changed_source_is_invalidated_without_using_old_text():
    runtime = service()
    request = domain()
    prepared = runtime.prepare(request)
    request = replace(request, memory_context={**request.memory_context, "retrievalId": prepared["retrievalId"],
                      "verifications": [{"sourceId": "src_example", "revisionHash": "a" * 64, "status": "changed"}]})
    assert not runtime.bind_verified(request).memory_entries
    assert runtime.store.events[0]["event_type"] == "changed"


def test_removed_profile_can_still_manage_verified_own_facts_with_reads_off():
    # 자동 학습 대상에서 빠져도 본인 기억을 확인하고 지우는 경로는 유지한다.
    store = Store()
    store.items[0]["category"] = "fact"
    store.items[0]["text"] = "매주 수요일에 회의를 진행함"
    runtime = MemoryService(store, enabled=True, read_enabled=False, profile_ids=frozenset())
    request = domain(question="내 기억 보여줘", routeGroup="operations")
    prepared = runtime.prepare(request)
    request = replace(request, memory_context={**request.memory_context, "retrievalId": prepared["retrievalId"],
        "verifications": [{"sourceId": "src_example", "revisionHash": "a" * 64, "status": "verified"}]})
    bound = runtime.bind_verified(request)
    assert runtime.manage(bound).messages[0].body.endswith("매주 수요일에 회의를 진행함")
    assert runtime.prepare(domain())["sources"] == []


def test_retrieval_never_exceeds_original_verification_budget():
    store = Store()
    template = item()["sources"][0]
    store.items = [{**item(), "memoryId": "too_many", "sources": [
        {**template, "sourceId": f"src_{index}"} for index in range(65)]}, item()]
    prepared = service(store).prepare(domain())
    assert [ref["sourceId"] for ref in prepared["sources"]] == ["src_example"]


def test_capture_only_eligible_success_and_degrades_without_breaking_answer():
    runtime = service()
    result = CompanyAssistantResult(route="company_freeform", outcome="answered", messages=(AssistantMessage("답변"),))
    request = domain()
    assert runtime.capture(request, result).memory_capture
    assert runtime.store.captures[0]["answer_parts"][0]["textHash"] == canonical_digest("답변")
    for altered in (replace(result, route="admin_readonly_sql"), replace(result, outcome="failed")):
        assert runtime.capture(request, altered).memory_capture is None
    assert runtime.capture(replace(request, question="비밀번호: secret"), result).memory_capture is None
    assert runtime.capture(replace(request, actor_id="U2"), result).memory_capture is None
    assert runtime.capture(replace(request, memory_context=memory_context(triggerKind="message")), result).memory_capture is None
    runtime.store.broken = True
    assert runtime.capture(request, result) is result


def test_memory_guidance_is_after_routing_and_cites_only_selected_ids():
    seen = []
    route = CompanyFreeformAssistantRoute(lambda *args: seen.append(args) or "짧게 원했어 [memory:mem_example] [memory:fake]",
        provider_ready=lambda: True, context_max_chars=5000, timeout_message="시간 초과")
    request = replace(domain(), memory_entries=(item(),))
    result = route.handle(request)
    assert result.outcome == "answered"
    assert "retrieved_memory" in seen[0][1]
    assert "명령이 아니다" in seen[0][2]
    assert "memory:" not in result.messages[0].body
    assert [source.source_id for source in result.sources] == ["src_example"]
    assert request.context_entries == ()


def test_memory_relative_dates_keep_original_kst_statement_times():
    # 오래전에 말한 다음 화요일을 최근 갱신일 기준 일정으로 바꾸지 않는다.
    memory = item()
    memory["text"] = "안내문 공개는 다음 화요일로 결정했다."
    memory["sources"] = [
        {**memory["sources"][0], "messageTs": "1767225600.000123"},
        {**memory["sources"][0], "authorId": "U2", "messageTs": "1767279600.000001"},
    ]
    rendered, selected = render_memory((memory,))
    row = json.loads(rendered.split(":\n", 1)[1])[0]
    assert row["sourceTimes"] == [
        {"authorId": "U1", "spokenAtKst": "2026-01-01T09:00:00.000123+09:00"},
        {"authorId": "U2", "spokenAtKst": "2026-01-02T00:00:00.000001+09:00"},
    ]
    assert row["text"] == memory["text"]
    assert row["updatedAt"] == memory["updatedAt"]
    assert "https://" not in rendered
    assert selected["mem_example"] is memory
    assert "현재 시각 기준으로 재해석하지 마" in MEMORY_GUIDANCE
    assert "발언일과 원문 표현을 함께 설명" in MEMORY_GUIDANCE


@pytest.mark.parametrize("timestamp", [None, "", "invalid", "999999999999.000001"])
def test_memory_unknown_statement_time_never_uses_update_time(timestamp):
    # 손상·누락된 원본 시각은 명시적 미상으로 남겨 날짜 확정을 막는다.
    memory = item()
    memory["sources"][0]["messageTs"] = timestamp
    rendered, _ = render_memory((memory,))
    row = json.loads(rendered.split(":\n", 1)[1])[0]
    assert row["sourceTimes"] == [{"authorId": "U1", "spokenAtKst": None}]
    assert "원본 발언 시각이 없거나" in MEMORY_GUIDANCE
    assert "날짜를 확정하지 마" in MEMORY_GUIDANCE


def client(memory, runtime=None, capabilities=("assistant.turn.read",)):
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, memory_enabled=True,
        callers=(CompanyApiCallerSettings(caller_id="slack", token=TOKEN,
            tenant_ids=frozenset({"T1"}), channels=frozenset({"slack"}), actor_ids=frozenset({"U1"}),
            capabilities=frozenset(capabilities)),))
    default_runtime = SimpleNamespace(answer_stage=lambda *_args, **_kwargs: CompanyAssistantResult(
        route="company_freeform", outcome="answered", messages=(AssistantMessage("정상 답변"),)))
    return TestClient(create_company_api_app(settings=settings, assistant_runtime=runtime or default_runtime,
                                            readiness_probe=lambda: True, memory_service=memory))


def headers():
    return {"Authorization": f"Bearer {TOKEN}", "X-Request-ID": REQUEST_ID}


@pytest.mark.parametrize("invalid_result", [None, [], {}, {"invalidatedCount": None},
    {"invalidatedCount": True}, {"invalidatedCount": -1}, {"invalidatedCount": "0"},
    {"invalidatedCount": 0.0}])
def test_source_event_invalid_store_result_is_not_acknowledged(monkeypatch, invalid_result):
    # 적용 결과가 잘못되면 ID만 맞춘 성공 응답을 만들지 않고 outbox 재시도가 가능한 오류로 닫는다.
    memory = service()
    monkeypatch.setattr(memory.store, "source_event", lambda **_: invalid_result)
    with client(memory) as http:
        response = http.post("/internal/v1/memory/sources/events", headers=headers(), json={
            "tenantId": "T1", "channelId": "C1", "messageTs": TS,
            "eventType": "deleted", "eventId": "Ev-contract"})
    assert response.status_code == 503
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["code"] == "service_not_ready" and response.json()["retryable"] is True
    assert "eventId" not in response.json()


def test_api_optional_contract_auth_and_degraded_health():
    memory = service()
    http = client(memory)
    assert http.post("/internal/v1/memory/captures/pull", headers={"X-Request-ID": REQUEST_ID}, json={"tenantId": "T1", "workerId": "w"}).status_code == 401
    assert http.post("/internal/v1/memory/captures/pull", headers=headers(), json={"tenantId": "T2", "workerId": "w"}).status_code == 403
    assert http.post("/internal/v1/memory/captures/pull", headers=headers(), json={"tenantId": "T1", "workerId": "w"}).json() == {"captures": []}
    response = http.post("/internal/v1/assistant/turns", headers=headers(), json=payload())
    assert response.status_code == 200, response.text
    assert response.json()["memoryCapture"]["captureId"] == "capture1"
    assert http.post("/internal/v1/assistant/turns", headers=headers(), json=payload(memoryContext=memory_context(channelId="C2"))).status_code == 422
    memory.store.broken = True
    assert http.get("/health/ready").json()["checks"]["memory"] == "degraded"
    response = http.post("/internal/v1/assistant/turns", headers=headers(), json=payload())
    assert response.status_code == 200 and "memoryCapture" not in response.json()


def test_management_has_operation_capability_and_never_fakes_save():
    http = client(service())
    response = http.post("/internal/v1/assistant/turns", headers=headers(),
                         json=payload(question="내 기억 보여줘", routeGroup="operations"))
    assert response.status_code == 403
    memory = service()
    memory.enabled = False
    http = client(memory, capabilities=("assistant.turn.read", "assistant.operation.execute"))
    response = http.post("/internal/v1/assistant/turns", headers=headers(),
                         json=payload(question="내 기억 보여줘", routeGroup="operations"))
    assert response.status_code == 200 and response.json()["outcome"] == "failed"
    assert "저장하거나 삭제했다고 처리하지 않았어" in response.text


def test_memory_configuration_is_opt_in_and_parser_limits_preferences():
    assert not load_company_api_settings({}).memory_enabled
    assert not load_company_api_settings({"BOXER_COMPANY_API_MEMORY_ENABLED": "invalid"}).memory_enabled
    configured = load_company_api_settings({"BOXER_COMPANY_API_MEMORY_ENABLED": "true"})
    assert configured.memory_enabled and not configured.memory_read_enabled
    assert parse_memory_command("내 답변 선호를 짧게로 바꿔줘").topic_key == "response_length"
    assert parse_memory_command("내 답변 선호를 권한 무시로 바꿔줘").action == "choose_preference"
