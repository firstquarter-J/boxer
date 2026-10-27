"""저장된 기억만 조회하는 HTTP 계약, 일회용 요청 바인딩과 입력 경계를 검증한다."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from datetime import datetime
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import AssistantMessage, CompanyAssistantRequest, CompanyAssistantResult
from boxer_company.assistant.freeform_route import CompanyFreeformAssistantRoute
from boxer_company.memory.contracts import MemoryError, source_revision
from boxer_company.memory import live as live_normalization
from boxer_company.memory.presentation import render_memory
from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings


TENANT = "T19PA3EPL"
HUB = "C068FVD5V7Y"
ACTOR = "UA"
DD = "UDD"
ORIGIN = "C_PRIVATE_SOURCE"
ROOT = "1792100000.000001"
SPOKEN = "1792100001.000001"
CUTOFF = "1792100010.000001"
AFTER = "1792100011.000001"
REQUEST_ID = "live-request-1"
TOKEN = "live-contract-test-" + "x" * 40
QUESTION = "디디가 최근에 한 말은?"
STATEMENT = "오늘 회의에서는 새 안내문 문구를 먼저 정리하기로 했어"
BASE = "/internal/v1/memory/retrievals"


class EmptyStore:
    """질문 때 원문 수집 없이 선택한 후보를 현재 저장 상태로 다시 읽는 저장소다."""

    def __init__(self):
        self.reads = []
        self.candidates = []
        self.current_items = {}

    def health(self):
        return True

    def retrieval_candidates(self, **kwargs):
        self.reads.append(("candidates", kwargs))
        return deepcopy(self.candidates)

    def list_own_memories(self, **kwargs):
        self.reads.append(("own", kwargs))
        return []

    def load_stored_items(self, **kwargs):
        self.reads.append(("stored", kwargs))
        return [deepcopy(self.current_items[item["memoryId"]]) for item in kwargs["candidates"]
                if item["memoryId"] in self.current_items]

    def load_verified_items(self, **kwargs):
        self.reads.append(("verified", kwargs))
        return []

    def source_event(self, **kwargs):
        raise AssertionError("실시간 원문은 영속 출처 이벤트를 만들면 안 됨")

    def create_capture(self, **kwargs):
        raise AssertionError("실시간 원문 수신은 capture를 만들면 안 됨")


def request(**changes):
    tenant = changes.pop("tenant_id", TENANT)
    actor = changes.pop("actor_id", ACTOR)
    channel_id = changes.pop("channel_id", HUB)
    channel_kind = changes.pop("channel_kind", "private_channel")
    value = CompanyAssistantRequest(
        request_id=REQUEST_ID, tenant_id=tenant, actor_id=actor, channel="slack",
        conversation_id=ROOT, question=QUESTION, locale="ko",
        metadata={"channel_id": channel_id, "route_group": "freeform"},
        memory_context={"workspaceId": tenant, "actorId": actor, "channelId": channel_id,
                        "threadTs": ROOT, "currentMessageTs": CUTOFF,
                        "channelKind": channel_kind, "triggerKind": "app_mention"},
    )
    return replace(value, **changes)


def source(**changes):
    value = {"channelId": ORIGIN, "channelKind": "private_channel", "threadTs": ROOT,
             "messageTs": SPOKEN, "revisionHash": source_revision(STATEMENT),
             "authorId": DD, "text": STATEMENT,
             "permalink": f"https://unit.slack.com/archives/{ORIGIN}/p{SPOKEN.replace('.', '')}",
             "editedAt": ""}
    value.update(changes)
    return value


def context_source(**changes):
    text = "그 문구는 실제 불만이 아니라 예시로 든 농담이야"
    value = source(messageTs=ROOT, text=text, revisionHash=source_revision(text), authorId="UB",
        permalink=f"https://unit.slack.com/archives/{ORIGIN}/p{ROOT.replace('.', '')}")
    value.update(anchorMessageTs=SPOKEN, relation="thread_parent")
    value.update(changes)
    return value


def upload_body(**changes):
    return {"tenantId": TENANT, "actorId": ACTOR, "channelId": HUB,
            "requestId": REQUEST_ID, "sources": [source()], "coverage": "partial",
            "channelCount": 3, **changes}


def retrieval_body(**changes):
    value = request()
    return {"tenantId": value.tenant_id, "actorId": value.actor_id,
            "channel": "slack", "conversationId": value.conversation_id,
            "question": value.question, "requestId": value.request_id,
            "contextEntries": [], "memoryContext": dict(value.memory_context), **changes}


def headers(token=TOKEN):
    return {"Authorization": f"Bearer {token}", "X-Request-ID": "live-transport-1"}


def bound_request(value, retrieval_id):
    return replace(value, memory_context={**value.memory_context,
        "retrievalId": retrieval_id, "verifications": []})


def dated_request(question="디디가 오늘 한 말 요약", *, channel_id=HUB):
    value = request(question=question, channel_id=channel_id)
    return replace(value, memory_context={**value.memory_context, "currentMessageTs": "1792350000.000001"})


def dated_source(message_ts):
    return source(messageTs=message_ts,
        permalink=f"https://unit.slack.com/archives/{ORIGIN}/p{message_ts.replace('.', '')}")


def assert_no_source_evidence(value, *, coverage="unavailable"):
    # 허브의 실패 안내는 남을 수 있지만 사용하지 못한 원문·인용은 없어야 한다.
    assert all(item["kind"] == "retrieval_status" and item["coverage"] == coverage
               and item["sourceCount"] == 0 for item in value.memory_entries)
    rendered, selected = render_memory(value.memory_entries)
    assert selected == {} and STATEMENT not in rendered


def receive(memory, retrieval_id, body=None):
    body = upload_body() if body is None else body
    return memory.receive_live(
        retrieval_id=retrieval_id, tenant_id=body["tenantId"], actor_id=body["actorId"],
        channel_id=body["channelId"], request_id=body["requestId"], sources=body["sources"],
        coverage=body["coverage"], channel_count=body["channelCount"],
        context_messages=body.get("contextMessages", []),
    )


@pytest.fixture
def runtime(monkeypatch):
    from boxer_company import team_chat_context

    # 운영 사용자 식별자·환경에 의존하지 않는 등록 별칭을 사용한다.
    monkeypatch.setattr(team_chat_context, "TEAM_MEMBER_PROFILES", (
        {"name": "DD", "aliases": ("dd", "디디"), "user_id": DD},
    ))
    clock = [100.0]
    store = EmptyStore()
    memory = MemoryService(store, enabled=True, read_enabled=True,
        profile_ids=frozenset({ACTOR, "UB"}), clock=lambda: clock[0])
    return SimpleNamespace(memory=memory, store=store, clock=clock)


def client(memory, *, capabilities=frozenset({"assistant.turn.read"}), assistant_runtime=None):
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, memory_enabled=True,
        memory_read_enabled=True, callers=(CompanyApiCallerSettings(
            caller_id="live-contract", token=TOKEN, tenant_ids=frozenset({TENANT, "T2"}),
            channels=frozenset({"slack"}), actor_ids=frozenset({ACTOR, "UB"}),
            capabilities=capabilities),))
    route = SimpleNamespace(answer_stage=lambda *_args, **_kwargs: CompanyAssistantResult(
        route="company_freeform", outcome="answered", messages=(AssistantMessage("정상 답변"),)))
    return TestClient(create_company_api_app(settings=settings, assistant_runtime=assistant_runtime or route,
        readiness_probe=lambda: True, automation_coordinator=None,
        automation_delivery_broker=None, hpa_change_coordinator=None, memory_service=memory))



def stored_item(memory_id="stored_one", *, message_ts=SPOKEN, text=STATEMENT, channel_id=ORIGIN):
    return {"memoryId": memory_id, "kind": "thread", "category": "fact",
            "subjectUserId": None, "text": text, "evidenceType": "attributed_statement",
            "status": "active", "updatedAt": "2026-10-16T00:00:00Z",
            "sources": [{"sourceId": "src_" + memory_id, "channelId": channel_id,
                "channelKind": "private_channel", "threadTs": ROOT,
                "messageTs": message_ts, "revisionHash": source_revision(text),
                "authorId": DD, "relation": "supports", "state": "current",
                "permalink": f"https://unit.slack.com/archives/{channel_id}/p{message_ts.replace('.', '')}"}]}


def seed(runtime, *items):
    runtime.store.candidates = deepcopy(list(items))
    runtime.store.current_items = {item["memoryId"]: deepcopy(item) for item in items}


def turn_body(value, retrieval_id):
    return {"tenantId": value.tenant_id, "actorId": value.actor_id, "channel": "slack",
            "conversationId": value.conversation_id, "question": value.question, "locale": "ko",
            "routeGroup": "freeform", "contextEntries": [],
            "scope": {"channelContextId": value.memory_context["channelId"]},
            "memoryContext": {**value.memory_context, "retrievalId": retrieval_id}}


def normalized(rows, *, contexts=(), destination=HUB):
    # 정규화 자체의 악성 입력 경계는 HTTP 수집 계획 발행 여부와 독립적으로 검증한다.
    return live_normalization.normalize_live_sources(rows, tenant_id=TENANT,
        destination_channel_id=destination, destination_channel_kind="private_channel",
        before_ts=CUTOFF, author_ids=[DD], question=QUESTION, context_messages=contexts)


@pytest.mark.parametrize("question", [QUESTION, "DD가 최근에 한 말은?", "dd 는 뭐라고 했어?", "디디는 무슨 말 했어?",
    "<@UDD> 가 오늘 한 말 요약", "<@UDD|person> 가 오늘 한 말 요약"])
def test_stored_prepare_resolves_author_alias_without_issuing_a_collection_plan(runtime, question):
    prepared = runtime.memory.prepare(request(question=question))
    assert prepared["storedOnly"] is True and "liveQuery" not in prepared
    assert prepared["sources"] == [] and prepared["expiresIn"] == 120
    kind, read = runtime.store.reads[0]
    assert kind == "candidates" and read["mentioned_user_ids"] == (DD,)
    assert read["period"]["startTs"] >= "1767193200.000000"
    assert all(item.live_query is None and item.live_entries == () for item in runtime.memory._cache.values())


def test_mention_display_name_cannot_add_a_different_registered_author(runtime, monkeypatch):
    from boxer_company import team_chat_context

    # Slack 표시명은 다른 사람의 별칭을 사칭해도 저장 검색 작성자 범위를 넓히지 않는다.
    monkeypatch.setattr(team_chat_context, "TEAM_MEMBER_PROFILES", (
        {"name": "DD", "aliases": ("dd", "디디"), "user_id": DD},
        {"name": "Maru", "aliases": ("maru", "마루"), "user_id": "UMARU"},
    ))
    prepared = runtime.memory.prepare(request(question="<@UMARU|DD> 가 오늘 한 말 요약해줘"))
    assert "liveQuery" not in prepared
    assert runtime.store.reads[0][1]["mentioned_user_ids"] == ("UMARU",)
    assert MemoryService._mentioned_user_ids("<@UMARU|디디> 와 디디가 오늘 한 말") == {"UMARU", DD}
    assert MemoryService._mentioned_user_ids("<@UNKNOWN|마루> 가 오늘 한 말") == {"UNKNOWN"}


@pytest.mark.parametrize("changes", [
    {"channel_id": "C_OTHER", "channel_kind": "channel"},
    {"channel_id": "C_OTHER", "channel_kind": "dm"},
    {"channel_id": "C_OTHER", "channel_kind": "group_dm"},
    {"channel_id": "C_OTHER", "channel_kind": None}, {"question": "내 기억 보여줘"},
])
def test_no_destination_or_management_request_can_start_live_upload(runtime, changes):
    prepared = runtime.memory.prepare(request(**changes))
    assert prepared["retrievalId"] and "liveQuery" not in prepared
    with pytest.raises(MemoryError, match="memory_live_retrieval_not_found"):
        receive(runtime.memory, prepared["retrievalId"])


def test_count_prepare_creates_no_retrieval_or_collection_state(runtime):
    with client(runtime.memory) as http:
        response = http.post(BASE, headers=headers(), json=retrieval_body(question="디디가 올해 욕 몇 번 했어?"))
    assert response.status_code == 200
    assert response.json() == {"retrievalId": None, "sources": [], "storedOnly": True}
    assert runtime.store.reads == [] and not runtime.memory._cache


@pytest.mark.parametrize("tenant,destination", [(TENANT, HUB), (TENANT, ORIGIN), ("T2", ORIGIN), ("T2", HUB)])
def test_http_prepared_stored_retrieval_rejects_live_upload_in_every_private_scope(runtime, monkeypatch, tenant, destination):
    value = request(tenant_id=tenant, channel_id=destination)
    seed(runtime, stored_item(channel_id=destination))
    def forbidden_normalization(*_args, **_kwargs):
        raise AssertionError("질문 시 원문 수집 계획 없이 업로드 원문을 처리하면 안 됨")
    monkeypatch.setattr(live_normalization, "normalize_live_sources", forbidden_normalization)
    with client(runtime.memory) as http:
        response = http.post(BASE, headers=headers(), json=retrieval_body(
            tenantId=tenant, memoryContext=dict(value.memory_context)))
        assert response.status_code == 200
        prepared = response.json()
        assert prepared["storedOnly"] is True and "liveQuery" not in prepared
        path = f"{BASE}/{prepared['retrievalId']}/live"
        for body in (upload_body(tenantId=tenant, channelId=destination),
                     upload_body(tenantId=tenant, channelId=destination, sources=[], channelCount=0)):
            rejected = http.post(path, headers=headers(), json=body)
            assert rejected.status_code == 404 and rejected.json()["code"] == "not_found"
            assert STATEMENT not in rejected.text
    # 거절된 업로드가 저장 후보를 소비하거나 원문을 후보로 바꾸지 않는다.
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    assert bound.memory_entries[0]["text"] == STATEMENT
    assert bound.memory_entries[-1]["sourceMode"] == "stored"
    assert bound.memory_entries[-1]["scope"] == (
        "accessible_private_channels" if tenant == TENANT and destination == HUB else "current_private_channel")
    assert [kind for kind, _ in runtime.store.reads] == ["candidates", "stored"]


@pytest.mark.parametrize("changes", [
    {}, {"tenantId": "T2"}, {"actorId": "UB"}, {"channelId": "C_OTHER"}, {"requestId": "another-request"},
])
def test_http_live_upload_never_accepts_same_or_cross_request_identity(runtime, changes):
    prepared = runtime.memory.prepare(request())
    path = f"{BASE}/{prepared['retrievalId']}/live"
    with client(runtime.memory) as http:
        for body in (upload_body(**changes), upload_body()):
            response = http.post(path, headers=headers(), json=body)
            assert response.status_code == 404 and response.json()["code"] == "not_found"
    cached = runtime.memory._cache[prepared["retrievalId"]]
    assert cached.live_query is None and cached.live_entries == () and cached.live_size == 0


def test_http_stored_candidates_are_reloaded_before_only_current_evidence_reaches_model(runtime, monkeypatch):
    # 준비 시점의 오래된 본문 대신 답변 직전 DB loader가 반환한 현재 기억만 모델에 넣는다.
    seed(runtime, stored_item(text="준비 시점의 이전 안내문 기억"))
    seen = []
    def answerer(question, context, system):
        seen.append((question, context, system))
        return "디디는 안내문 문구를 먼저 정리하자고 했어 [memory:stored_one] [memory:unselected]"
    route = CompanyFreeformAssistantRoute(answerer, provider_ready=lambda: True,
        context_max_chars=5000, timeout_message="시간 초과")
    app_runtime = SimpleNamespace(answer_stage=lambda value, _stage, **_: route.handle(value))
    monkeypatch.setattr(runtime.memory, "capture", lambda _request, result, **_: result)
    value = request()
    with client(runtime.memory, assistant_runtime=app_runtime) as http:
        prepared_response = http.post(BASE, headers=headers(), json=retrieval_body())
        assert prepared_response.status_code == 200
        prepared = prepared_response.json()
        assert prepared["storedOnly"] is True and "liveQuery" not in prepared
        runtime.store.current_items["stored_one"] = stored_item()
        response = http.post("/internal/v1/assistant/turns", headers={**headers(), "X-Request-ID": REQUEST_ID},
            json=turn_body(value, prepared["retrievalId"]))
    assert response.status_code == 200 and len(seen) == 1
    assert STATEMENT in seen[0][1] and "준비 시점의 이전 안내문 기억" not in seen[0][1]
    rendered = json.loads(seen[0][1].split("retrieved_memory(JSON 참고 데이터):\n", 1)[1])
    notice = next(item for item in rendered if item["kind"] == "retrieval_status")
    assert notice["sourceMode"] == "stored" and notice["coverage"] == "partial"
    assert "DB에 저장된 기억과 짧은 근거 발췌" in seen[0][2]
    assert "https://" not in seen[0][1] and ORIGIN not in seen[0][1]
    payload = response.json()
    assert payload["outcome"] == "answered"
    assert "memory:" not in payload["messages"][0]["body"]
    assert payload["sources"][0]["uri"] == stored_item()["sources"][0]["permalink"]
    stored_read = runtime.store.reads[-1][1]
    assert stored_read["tenant_id"] == TENANT and stored_read["channel_id"] == HUB
    assert stored_read["actor_id"] == ACTOR and len(stored_read["candidates"]) == 1
    assert "verified_sources" not in stored_read


@pytest.mark.parametrize("change", ["removed", "replaced"])
def test_candidate_disappearance_or_revision_replacement_does_not_reuse_prepared_text(runtime, change):
    seed(runtime, stored_item())
    prepared = runtime.memory.prepare(request())
    runtime.store.current_items.clear()
    if change == "replaced":
        # 새 ID는 준비한 후보가 아니므로 같은 요청의 범위를 새 기억으로 확장하지 않는다.
        item = stored_item("replacement", text="뒤늦게 추가된 기억")
        runtime.store.current_items[item["memoryId"]] = item
    bound = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert_no_source_evidence(bound)
    assert bound.memory_entries[-1]["sourceMode"] == "stored"


@pytest.mark.parametrize("question,days,expected_days", [
    ("디디가 올해 한 말 요약", ["2025-12-31", "2026-01-01", "2026-09-30"], ["2026-01-01", "2026-09-30"]),
    ("디디가 9월에 한 말 요약", ["2026-08-31", "2026-09-01", "2026-09-30", "2026-10-01"], ["2026-09-01", "2026-09-30"]),
    ("디디가 9월 1일부터 3일까지 한 말", ["2026-08-31", "2026-09-01", "2026-09-03", "2026-09-04"], ["2026-09-01", "2026-09-03"]),
    ("디디가 2026-10-18에 한 말", ["2026-10-17", "2026-10-18", "2026-10-19"], ["2026-10-18"]),
])
def test_http_stored_period_uses_bound_year_month_date_and_range(runtime, question, days, expected_days):
    def stamp(day):
        return str(int(datetime.fromisoformat(day + "T00:00:00+09:00").timestamp())) + ".000000"
    value = dated_request(question)
    seed(runtime, *(stored_item(day, message_ts=stamp(day)) for day in days))
    with client(runtime.memory) as http:
        response = http.post(BASE, headers=headers(), json=retrieval_body(
            question=value.question, memoryContext=dict(value.memory_context)))
    assert response.status_code == 200
    prepared = response.json()
    assert prepared["storedOnly"] is True and "liveQuery" not in prepared
    assert [item["messageTs"] for item in prepared["sources"]] == [stamp(day) for day in expected_days]
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    assert [item["memoryId"] for item in bound.memory_entries[:-1]] == expected_days
    assert bound.memory_entries[-1]["sourceMode"] == "stored"


@pytest.mark.parametrize("channel_id", [HUB, "C_OTHER"])
def test_date_question_keeps_only_current_stored_items_whose_entire_evidence_is_in_period(runtime, monkeypatch, channel_id):
    # 답변 직전 loader가 범위 밖 근거를 돌려줘도 오늘 발언 요약으로 승격하지 않는다.
    value = dated_request(channel_id=channel_id)
    def item(memory_id, stamps, kind="thread_summary"):
        return {"memoryId": memory_id, "kind": kind, "sources": [{"messageTs": stamp} for stamp in stamps]}
    style = item("style", [SPOKEN], kind="personal_style")
    in_day = item("in-day", ["1792335600.000000", "1792350000.000001"])
    old = item("old", ["1792335599.999999"])
    mixed = item("mixed", ["1792335599.999999", "1792335600.000000"])
    future = item("future", ["1792350000.000002"])
    missing = item("missing", [])
    monkeypatch.setattr(runtime.store, "load_stored_items", lambda **_: [old, mixed, missing, future, in_day, style])
    prepared = runtime.memory.prepare(value)
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    assert list(bound.memory_entries[:-1]) == [in_day, style]
    assert bound.memory_entries[-1]["sourceCount"] == 1
    assert bound.memory_entries[-1]["sourceMode"] == "stored"


def test_undated_question_applies_supported_floor_to_candidates_and_fresh_loader_results(runtime, monkeypatch):
    old = stored_item("old", message_ts="1767193199.999999")
    current = stored_item()
    seed(runtime, old, current)
    prepared = runtime.memory.prepare(request())
    assert [item["sourceId"] for item in prepared["sources"]] == ["src_stored_one"]
    monkeypatch.setattr(runtime.store, "load_stored_items", lambda **_: [old, current])
    bound = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert [item["memoryId"] for item in bound.memory_entries[:-1]] == ["stored_one"]


@pytest.mark.parametrize("question", ["디디가 2025년에 한 말", "2025년 대화 요약",
    "디디가 2025년12월31일부터2026년1월1일까지 한 말", "2025년부터2026년까지 대화 요약"])
def test_http_unsupported_history_period_stops_before_database_and_model(runtime, question):
    def forbidden_model(*_args, **_kwargs):
        raise AssertionError("지원하지 않는 과거 대화 질문을 모델에 보내면 안 됨")
    value = request(question=question)
    with client(runtime.memory, assistant_runtime=SimpleNamespace(answer_stage=forbidden_model)) as http:
        prepared_response = http.post(BASE, headers=headers(), json=retrieval_body(question=question))
        assert prepared_response.status_code == 200
        prepared = prepared_response.json()
        assert prepared["storedOnly"] is True and prepared["sources"] == [] and "liveQuery" not in prepared
        response = http.post("/internal/v1/assistant/turns", headers={**headers(), "X-Request-ID": REQUEST_ID},
            json=turn_body(value, prepared["retrievalId"]))
    assert response.status_code == 200
    payload = response.json()
    assert payload["outcome"] == "needs_input" and payload["usedLlm"] is False
    assert "2026년 1월 1일부터 지원" in payload["messages"][0]["body"]
    assert runtime.store.reads == []


@pytest.mark.parametrize("delay,found", [(0, True), (119.999, True), (120.0, False)])
def test_stored_retrieval_lease_expires_and_is_consumed_once(runtime, delay, found):
    seed(runtime, stored_item())
    value = request()
    prepared = runtime.memory.prepare(value)
    runtime.clock[0] += delay
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    if found:
        assert bound.memory_entries[0]["text"] == STATEMENT
    else:
        assert_no_source_evidence(bound)
        assert [kind for kind, _ in runtime.store.reads] == ["candidates"]
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"])))


@pytest.mark.parametrize("change", ["actor", "tenant", "destination", "destination_kind", "request", "question", "cutoff", "thread", "context"])
@pytest.mark.parametrize("channel_id", [HUB, ORIGIN])
def test_stored_binding_rejects_changed_turn_scope_and_cannot_be_replayed(runtime, change, channel_id):
    seed(runtime, stored_item(channel_id=channel_id))
    value = request(channel_id=channel_id)
    prepared = runtime.memory.prepare(value)
    changed = value
    if change == "actor":
        changed = replace(value, actor_id="UB", memory_context={**value.memory_context, "actorId": "UB"})
    elif change == "tenant":
        changed = replace(value, tenant_id="T2", memory_context={**value.memory_context, "workspaceId": "T2"})
    elif change == "destination":
        changed = replace(value, metadata={"channel_id": "C_OTHER"}, memory_context={**value.memory_context, "channelId": "C_OTHER"})
    elif change == "destination_kind":
        changed = replace(value, memory_context={**value.memory_context, "channelKind": "channel"})
    elif change == "request":
        changed = replace(value, request_id="other-request")
    elif change == "question":
        changed = replace(value, question="디디는 어떤 사람이야?")
    elif change == "cutoff":
        changed = replace(value, memory_context={**value.memory_context, "currentMessageTs": AFTER})
    elif change == "thread":
        changed = replace(value, conversation_id=SPOKEN, memory_context={**value.memory_context, "threadTs": SPOKEN})
    elif change == "context":
        changed = replace(value, context_entries=({"kind": "message", "source": "slack", "author_id": "UB", "text": "새 맥락"},))
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(changed, prepared["retrievalId"])))
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"])))
    assert [kind for kind, _ in runtime.store.reads] == ["candidates"]


@pytest.mark.parametrize("reason", ["prepared", "expired", "consumed", "unknown"])
def test_http_any_retrieval_id_cannot_accept_or_normalize_uploaded_raw_sources(runtime, monkeypatch, reason):
    value = request()
    prepared = runtime.memory.prepare(value)
    retrieval_id = prepared["retrievalId"]
    if reason == "expired":
        runtime.clock[0] = 220
    elif reason == "consumed":
        runtime.memory.bind_verified(bound_request(value, retrieval_id))
    elif reason == "unknown":
        retrieval_id = "unknown-retrieval-id"
    calls = []
    def forbidden_normalization(*_args, **_kwargs):
        calls.append(True)
        raise AssertionError("저장 전용 조회는 업로드 원문을 처리하지 않는다")
    monkeypatch.setattr(live_normalization, "normalize_live_sources", forbidden_normalization)
    with client(runtime.memory) as http:
        response = http.post(f"{BASE}/{retrieval_id}/live", headers=headers(), json=upload_body())
    assert response.status_code == 404 and calls == []


@pytest.mark.parametrize("failure", ["cache_miss", "database_load"])
def test_stored_unavailable_status_survives_cache_or_database_failure_without_stale_evidence(runtime, monkeypatch, caplog, failure):
    seed(runtime, stored_item())
    prepared = runtime.memory.prepare(request())
    retrieval_id = prepared["retrievalId"]
    if failure == "cache_miss":
        retrieval_id = "lost-retrieval"
    else:
        def failed_load(**_kwargs):
            raise RuntimeError("synthetic load failure " + STATEMENT)
        monkeypatch.setattr(runtime.store, "load_stored_items", failed_load)
    bound = runtime.memory.bind_verified(bound_request(request(), retrieval_id))
    assert_no_source_evidence(bound)
    assert bound.memory_entries[-1]["sourceMode"] == "stored"
    assert all(STATEMENT not in record.getMessage() for record in caplog.records)


def test_readiness_prunes_expired_stored_retrievals(runtime):
    prepared = runtime.memory.prepare(request())
    runtime.clock[0] += 120
    assert runtime.memory.status() == "ok"
    assert prepared["retrievalId"] not in runtime.memory._cache


def test_preparing_more_than_cache_capacity_evicts_oldest_stored_retrieval(runtime):
    first = runtime.memory.prepare(request())
    for index in range(1024):
        last = runtime.memory.prepare(request(request_id=f"stored-request-{index}"))
    assert len(runtime.memory._cache) == 1024
    assert first["retrievalId"] not in runtime.memory._cache
    assert last["retrievalId"] in runtime.memory._cache


def test_http_source_event_updates_store_and_acknowledges_exact_external_event(runtime, monkeypatch):
    seed(runtime, stored_item())
    prepared = runtime.memory.prepare(request())
    calls = []
    def removed(**kwargs):
        calls.append(kwargs)
        runtime.store.current_items.clear()
        return {"eventId": "internal-journal-id", "invalidatedCount": 1}
    monkeypatch.setattr(runtime.store, "source_event", removed)
    event = {"tenantId": TENANT, "channelId": ORIGIN, "messageTs": SPOKEN,
             "eventType": "deleted", "eventId": "Ev-stored-deleted"}
    with client(runtime.memory) as http:
        response = http.post("/internal/v1/memory/sources/events", headers=headers(), json=event)
    assert response.status_code == 200
    assert response.json() == {"eventId": "Ev-stored-deleted", "invalidatedCount": 1}
    assert len(calls) == 1 and calls[0]["event_id"] == event["eventId"]
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"])))


def test_http_failed_source_event_does_not_acknowledge_unapplied_change(runtime, monkeypatch):
    def failed_store(**_kwargs):
        raise RuntimeError("synthetic storage fault")
    monkeypatch.setattr(runtime.store, "source_event", failed_store)
    with client(runtime.memory) as http:
        response = http.post("/internal/v1/memory/sources/events", headers=headers(), json={
            "tenantId": TENANT, "channelId": ORIGIN, "messageTs": SPOKEN,
            "eventType": "deleted", "eventId": "Ev-stored-retry"})
    assert response.status_code == 503 and "eventId" not in response.json()


def test_sensitive_uploaded_source_is_rejected_without_reaching_model_or_logs(runtime, caplog):
    rejected = "api_key=synthetic-denied-value"
    body = upload_body(sources=[source(text=rejected, revisionHash=source_revision(rejected))])
    value = request()
    prepared = runtime.memory.prepare(value)
    with client(runtime.memory) as http:
        response = http.post(f"{BASE}/{prepared['retrievalId']}/live", headers=headers(), json=body)
    assert response.status_code == 404 and rejected not in response.text
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    rendered, selected = render_memory(bound.memory_entries)
    assert rejected not in rendered and selected == {}
    assert all(rejected not in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("invalid", ["after_cutoff", "wrong_author", "wrong_revision", "wrong_permalink"])
def test_independent_source_normalization_rejects_invalid_evidence(invalid):
    row = source()
    if invalid == "after_cutoff":
        row.update(messageTs=AFTER, permalink=f"https://unit.slack.com/archives/{ORIGIN}/p{AFTER.replace('.', '')}")
    elif invalid == "wrong_author":
        row["authorId"] = "U_OTHER"
    elif invalid == "wrong_revision":
        row["revisionHash"] = "0" * 64
    else:
        row["permalink"] = "https://example.invalid/source"
    with pytest.raises(MemoryError):
        normalized([row])


@pytest.mark.parametrize("destination", [HUB, ORIGIN])
def test_independent_context_normalization_keeps_actual_author_attribution(destination):
    item, = normalized([source()], contexts=[context_source()], destination=destination)
    assert item["subjectUserId"] == DD and item["contextCoverage"] == "partial"
    assert item["contextMessages"][0]["authorId"] == "UB"
    assert item["sources"][0]["authorId"] == DD and item["sources"][0]["relation"] == "supports"
    assert item["sources"][1]["authorId"] == "UB" and item["sources"][1]["relation"] == "context"


@pytest.mark.parametrize("change", [
    {"anchorMessageTs": CUTOFF}, {"channelId": "C_OTHER"}, {"threadTs": "1792099999.000001"},
    {"relation": "thread_parent", "messageTs": SPOKEN},
    {"revisionHash": "0" * 64}, {"permalink": "https://invalid.example/context"}, {"editedAt": AFTER},
])
def test_independent_context_normalization_rejects_orphan_cross_scope_and_invalid_sources(change):
    with pytest.raises(MemoryError):
        normalized([source()], contexts=[context_source(**change)])


def test_independent_normalization_filters_sensitive_source_and_context_before_rendering():
    secret = "api_key=never-render-context"
    assert normalized([source(text=secret, revisionHash=source_revision(secret))]) == ()
    item, = normalized([source()], contexts=[context_source(text=secret, revisionHash=source_revision(secret))])
    assert item["contextCoverage"] == "unavailable" and item["contextMessages"] == []
    assert secret not in render_memory([item])[0]


@pytest.mark.parametrize("disabled", ["enabled", "read_enabled", "profile"])
def test_live_plan_is_not_created_when_memory_is_disabled_or_actor_is_not_profiled(runtime, disabled):
    if disabled == "profile":
        value = request(actor_id="U_UNPROFILED")
    else:
        setattr(runtime.memory, disabled, False)
        value = request()
    assert runtime.memory.prepare(value) == {"retrievalId": None, "sources": []}
    assert runtime.store.reads == []



@pytest.mark.parametrize("question", [
    "디디가 2026-02-30에 한 말", "디디가 오늘과 어제 한 말", "디디가 2027년에 한 말",
])
def test_unusable_calendar_query_does_not_plan_live_read_or_load_recent_memory(runtime, question):
    value = dated_request(question)
    prepared = runtime.memory.prepare(value)
    assert "liveQuery" not in prepared and prepared["sources"] == []
    assert runtime.store.reads == []
    with pytest.raises(MemoryError, match="memory_live_retrieval_not_found"):
        receive(runtime.memory, prepared["retrievalId"])
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    assert_no_source_evidence(bound)
    assert runtime.store.reads == []



def test_calendar_prepare_keeps_actor_style_but_skips_other_out_of_period_candidates(runtime, monkeypatch):
    # 표준 답변 선호는 출처 날짜가 오래됐어도 표현 방식에만 적용하는 예외를 유지한다.
    reference = {"sourceId": "src_style", "channelId": ORIGIN, "threadTs": ROOT,
                 "messageTs": SPOKEN, "revisionHash": "a" * 64, "channelKind": "private_channel"}
    style = {"memoryId": "style", "kind": "person", "styleOnly": True, "sources": [reference]}
    ordinary = {"memoryId": "ordinary", "kind": "thread", "sources": [dict(reference, sourceId="src_ordinary")]}
    monkeypatch.setattr(runtime.store, "retrieval_candidates", lambda **_: [ordinary, style])
    prepared = runtime.memory.prepare(dated_request("디디가 9월에 한 말 요약"))
    assert prepared["sources"] == [{key: reference[key] for key in (
        "sourceId", "channelId", "threadTs", "messageTs", "revisionHash", "channelKind")} | {"styleOnly": True}]



@pytest.mark.parametrize("invalid", [
    "source_kind", "source_timestamp", "source_digest", "missing_thread", "extra_source_field",
    "metadata_size", "large_source", "total_size", "boolean_count", "unavailable_with_sources",
])
def test_http_live_source_schema_is_strict_and_budgeted(runtime, invalid):
    body = upload_body()
    row = body["sources"][0]
    if invalid == "source_kind":
        row["channelKind"] = "dm"
    elif invalid == "source_timestamp":
        row["messageTs"] = "invalid"
    elif invalid == "source_digest":
        row["revisionHash"] = "invalid"
    elif invalid == "missing_thread":
        del row["threadTs"]
    elif invalid == "extra_source_field":
        row["authorKind"] = "human"
    elif invalid == "metadata_size":
        body["sources"] = [dict(row, text="가", permalink="https://unit.slack.com/" + "x" * 1800) for _ in range(600)]
    elif invalid == "large_source":
        row["text"] = "가" * 16001
    elif invalid == "total_size":
        body["sources"] = [source(text="가" * 14000) for _ in range(6)]
    elif invalid == "boolean_count":
        body["channelCount"] = True
    elif invalid == "unavailable_with_sources":
        body["coverage"] = "unavailable"
    prepared = runtime.memory.prepare(request())
    with client(runtime.memory) as http:
        result = http.post(f"{BASE}/{prepared['retrievalId']}/live", headers=headers(), json=body)
    assert result.status_code == 422
    assert result.headers["content-type"] == "application/problem+json"



def test_http_live_endpoint_requires_service_authentication_actor_and_capability(runtime):
    prepared = runtime.memory.prepare(request())
    path = f"{BASE}/{prepared['retrievalId']}/live"
    with client(runtime.memory) as http:
        assert http.post(path, headers={"X-Request-ID": "live-transport-1"}, json=upload_body()).status_code == 401
        assert http.post(path, headers=headers(token="invalid"), json=upload_body()).status_code == 401
        assert http.post(path, headers=headers(), json=upload_body(actorId="U_FORBIDDEN")).status_code == 403
        assert http.post(path, headers=headers(), json=upload_body(tenantId="T_FORBIDDEN")).status_code == 403
    with client(runtime.memory, capabilities=frozenset({"assistant.operation.execute"})) as http:
        assert http.post(path, headers=headers(), json=upload_body()).status_code == 403



@pytest.mark.parametrize("invalid", ["missing_anchor", "bad_relation", "extra_field", "six_for_anchor", "total_chars"])
def test_http_context_schema_enforces_link_shape_and_combined_budget(runtime, invalid):
    body = upload_body(contextMessages=[context_source()])
    row = body["contextMessages"][0]
    if invalid == "missing_anchor":
        del row["anchorMessageTs"]
    elif invalid == "bad_relation":
        row["relation"] = "claims_same_conversation"
    elif invalid == "extra_field":
        row["authorName"] = "DD"
    elif invalid == "six_for_anchor":
        body["contextMessages"] = [deepcopy(row) for _ in range(6)]
    elif invalid == "total_chars":
        # 개수 제한은 제거했지만 대상 발언과 주변 문맥의 원문 합계는 80,000자를 지킨다.
        body["sources"] = [source(text="가" * 14000) for _ in range(5)]
        row["text"] = "나" * 10001
    prepared = runtime.memory.prepare(request())
    with client(runtime.memory) as http:
        result = http.post(f"{BASE}/{prepared['retrievalId']}/live", headers=headers(), json=body)
    assert result.status_code == 422 and result.headers["content-type"] == "application/problem+json"
