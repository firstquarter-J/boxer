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

    def load_stored_items(self, **kwargs):
        return self.items

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


@pytest.mark.parametrize("question,expected", [
    ("DD 는 어떤 사람이야?", {"UDD"}),
    ("dd 는 어떤 사람이야?", {"UDD"}),
    ("DD는 어떤 사람이야?", {"UDD"}),
    ("dd는 어떤 사람이야?", {"UDD"}),
    ("디디는 어떤 사람이야?", {"UDD"}),
    ("디디 어떤 사람이야?", {"UDD"}),
    ("DD님은 어떤 사람이야?", {"UDD"}),
    ("디디에 대해 알려줘", {"UDD"}),
    ("DD, dd, 디디는 어떤 사람이야?", {"UDD"}),
    ("디디와 세이지는 어떤 사람이야?", {"UDD", "USAGE"}),
    ("<@UDD>는 어떤 사람이야?", {"UDD"}),
    ("<@UDD2>는 어떤 사람이야?", {"UDD2"}),
    ("address message markdown 폴더 DDos", set()),
])
def test_memory_retrieval_resolves_member_spellings_without_word_substrings(monkeypatch, question, expected):
    from boxer_company import team_chat_context

    monkeypatch.setattr(team_chat_context, "TEAM_MEMBER_PROFILES", (
        {"name": "DD", "aliases": ("dd", "디디"), "user_id": "UDD"},
        {"name": "Sage", "aliases": ("sage", "세이지"), "user_id": "USAGE"},
        {"name": "Mark", "aliases": ("mark", "마크"), "user_id": "UMARK"},
        {"name": "Paul", "aliases": ("paul", "폴"), "user_id": "UPAUL"},
    ))
    calls = []
    store = Store()
    monkeypatch.setattr(store, "retrieval_candidates", lambda **kwargs: calls.append(kwargs) or [])

    # 기억 검색까지 같은 Slack 사용자 ID가 전달돼야 표기 차이가 검색 순위에 영향을 주지 않는다.
    service(store).prepare(domain(question=question))

    assert len(calls) == 1
    assert set(calls[0]["mentioned_user_ids"]) == expected
    assert len(calls[0]["mentioned_user_ids"]) == len(expected)
    assert calls[0]["tenant_id"] == "T1"
    assert calls[0]["channel_id"] == "C1"


def test_memory_retrieval_is_bound_to_turn_and_expires():
    clock = [1.0]
    runtime = service(clock=lambda: clock[0])
    request = domain()
    prepared = runtime.prepare(request)
    assert "text" not in str(prepared)
    verified = {**request.memory_context, "retrievalId": prepared["retrievalId"],
                "verifications": [{"sourceId": "src_example", "revisionHash": "a" * 64, "status": "verified"}]}
    assert any(entry.get("memoryId") == "mem_example" for entry in runtime.bind_verified(replace(request, memory_context=verified)).memory_entries)
    assert all("memoryId" not in entry for entry in runtime.bind_verified(replace(request, memory_context=verified)).memory_entries)
    prepared = runtime.prepare(request)
    verified["retrievalId"] = prepared["retrievalId"]
    assert not runtime.bind_verified(replace(request, question="다른 질문", memory_context=verified)).memory_entries
    prepared = runtime.prepare(request)
    verified["retrievalId"] = prepared["retrievalId"]
    clock[0] = 122
    assert all("memoryId" not in entry for entry in runtime.bind_verified(replace(request, memory_context=verified)).memory_entries)


def test_changed_source_is_invalidated_without_using_old_text():
    runtime = service()
    request = domain()
    prepared = runtime.prepare(request)
    # 원문 변경은 기존 이벤트 처리로 DB에 반영하며 일반 조회에서 client 검증값으로 쓰지 않는다.
    runtime.store.source_event(tenant_id="T1", channel_id="C1", message_ts=TS,
        event_type="changed", event_id="stored-source-changed")
    request = replace(request, memory_context={**request.memory_context, "retrievalId": prepared["retrievalId"]})
    assert all("memoryId" not in entry for entry in runtime.bind_verified(request).memory_entries)
    assert runtime.store.events[0]["event_type"] == "changed"


def test_stored_query_does_not_treat_client_verification_as_a_source_event():
    runtime = service()
    request = domain()
    prepared = runtime.prepare(request)
    request = replace(request, memory_context={**request.memory_context, "retrievalId": prepared["retrievalId"],
        "verifications": [{"sourceId": "src_example", "revisionHash": "a" * 64, "status": "deleted"}]})
    assert any(entry.get("memoryId") == "mem_example" for entry in runtime.bind_verified(request).memory_entries)
    assert runtime.store.events == []


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


@pytest.mark.parametrize("route_group", ["freeform", "fun"])
@pytest.mark.parametrize("prepared", [False, True])
def test_history_answer_uses_db_memory_without_caller_thread_or_fun_context(route_group, prepared):
    memory = service()
    body = payload(question="디디가 올해 한 말은 뭐야?", routeGroup=route_group,
        contextEntries=[{"kind": "message", "source": "slack", "authorId": "U1", "text": "저장하지 않은 스레드 원문"}])
    if route_group == "fun":
        body["funContext"] = "저장하지 않은 fun 스레드 원문"
    if prepared:
        value = AssistantTurnInput.model_validate(body).to_company_request(REQUEST_ID)
        body["memoryContext"]["retrievalId"] = memory.prepare(value)["retrievalId"]
    seen = []

    def answer_stage(request, _stage, **_kwargs):
        seen.append(request)
        return CompanyAssistantResult(route="company_team_fun" if route_group == "fun" else "company_freeform",
            outcome="answered", messages=(AssistantMessage("저장된 근거에서만 답변"),))

    # 직접 API caller가 과거 원문을 보내도 prepare 바인딩 확인 후 모델 입력에서는 제거한다.
    with client(memory, SimpleNamespace(answer_stage=answer_stage)) as http:
        response = http.post("/internal/v1/assistant/turns", headers=headers(), json=body)
    assert response.status_code == 200, response.text
    assert len(seen) == 1 and seen[0].context_entries == ()
    assert "team_fun_context" not in seen[0].metadata
    assert any(row.get("memoryId") == "mem_example" for row in seen[0].memory_entries) is prepared


def test_history_context_removal_does_not_bypass_original_retrieval_binding():
    memory = service()
    body = payload(question="디디가 올해 한 말은 뭐야?", contextEntries=[
        {"kind": "message", "source": "slack", "text": "준비 때의 원문"}])
    value = AssistantTurnInput.model_validate(body).to_company_request(REQUEST_ID)
    body["memoryContext"]["retrievalId"] = memory.prepare(value)["retrievalId"]
    body["contextEntries"][0]["text"] = "다른 원문"
    seen = []

    def answer_stage(request, _stage, **_kwargs):
        seen.append(request)
        return CompanyAssistantResult(route="company_freeform", outcome="answered", messages=(AssistantMessage("저장 근거 없음"),))

    # 원문을 지우기 전에 원 요청과 비교하므로 다른 문맥으로 준비한 기억을 재사용할 수 없다.
    with client(memory, SimpleNamespace(answer_stage=answer_stage)) as http:
        response = http.post("/internal/v1/assistant/turns", headers=headers(), json=body)
    assert response.status_code == 200, response.text
    assert len(seen) == 1 and not seen[0].context_entries
    assert all("memoryId" not in row for row in seen[0].memory_entries)


@pytest.mark.parametrize("route_group", [None, "freeform", "fun"])
def test_named_member_draft_edit_preserves_current_draft_context(route_group):
    seen = []

    def answer_stage(request, _stage, **_kwargs):
        seen.append(request)
        return CompanyAssistantResult(route="company_team_fun" if route_group == "fun" else "company_freeform",
            outcome="answered", messages=(AssistantMessage("다듬은 문구"),))

    # 팀원 이름만 언급한 문구 편집은 기록 검색으로 오인해 현재 초안을 버리지 않는다.
    runtime = SimpleNamespace(answer_stage=answer_stage, answer=lambda request, **kwargs: answer_stage(request, None, **kwargs))
    body = payload(question="디디 생일 축하 문구를 더 짧게 다듬어줘", routeGroup=route_group,
        contextEntries=[{"kind": "message", "source": "slack", "text": "현재 편집할 축하 문구 초안"}])
    if route_group == "fun":
        body["funContext"] = "현재 작성 중인 문구"
    with client(service(), runtime) as http:
        response = http.post("/internal/v1/assistant/turns", headers=headers(), json=body)
    assert response.status_code == 200, response.text
    assert len(seen) == 1 and seen[0].context_entries[0]["text"] == "현재 편집할 축하 문구 초안"
    if route_group == "fun":
        assert seen[0].metadata["team_fun_context"] == "현재 작성 중인 문구"


def test_daily_fortune_preserves_its_root_context_with_memory_disabled():
    from boxer_company.assistant.team_fun_route import match_company_daily_fortune_route

    seen = []
    memory = MemoryService(None, enabled=False, read_enabled=False)

    def answer_stage(request, _stage, **_kwargs):
        seen.append(request)
        assert match_company_daily_fortune_route(request) == "company_daily_fortune"
        return CompanyAssistantResult(route="company_daily_fortune", outcome="answered", messages=(AssistantMessage("운세 답변"),))

    # 운세 본문의 '오늘'과 '대화'가 과거 기록 조회로 오인되어 필수 root가 사라지지 않는다.
    with client(memory, SimpleNamespace(answer_stage=answer_stage)) as http:
        response = http.post("/internal/v1/assistant/turns", headers=headers(), json=payload(
            question="1992년생 오늘은 대화하며 행운을 찾아보세요.", routeGroup="fun", memoryContext=None,
            contextEntries=[{"kind": "message", "source": "slack", "text": "오늘의 운세"}]))
    assert response.status_code == 200, response.text
    assert len(seen) == 1 and seen[0].context_entries[0]["text"] == "오늘의 운세"


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
