"""실시간 비공개 발언의 HTTP 계약·일회용 캐시·모델 입력 경계를 검증한다."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
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
    """실시간 원문을 영속 쓰기로 넘기지 않는지 함께 감시하는 저장소다."""

    def __init__(self):
        self.reads = []

    def health(self):
        return True

    def retrieval_candidates(self, **kwargs):
        self.reads.append(("candidates", kwargs))
        return []

    def list_own_memories(self, **kwargs):
        self.reads.append(("own", kwargs))
        return []

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
    value = CompanyAssistantRequest(
        request_id=REQUEST_ID, tenant_id=tenant, actor_id=actor, channel="slack",
        conversation_id=ROOT, question=QUESTION, locale="ko",
        metadata={"channel_id": channel_id, "route_group": "freeform"},
        memory_context={"workspaceId": tenant, "actorId": actor, "channelId": channel_id,
                        "threadTs": ROOT, "currentMessageTs": CUTOFF,
                        "channelKind": "private_channel", "triggerKind": "app_mention"},
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


def client(memory, *, capabilities=frozenset({"assistant.turn.read"})):
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, memory_enabled=True,
        memory_read_enabled=True, callers=(CompanyApiCallerSettings(
            caller_id="live-contract", token=TOKEN, tenant_ids=frozenset({TENANT, "T2"}),
            channels=frozenset({"slack"}), actor_ids=frozenset({ACTOR, "UB"}),
            capabilities=capabilities),))
    route = SimpleNamespace(answer_stage=lambda *_args, **_kwargs: CompanyAssistantResult(
        route="company_freeform", outcome="answered", messages=(AssistantMessage("정상 답변"),)))
    return TestClient(create_company_api_app(settings=settings, assistant_runtime=route,
        readiness_probe=lambda: True, automation_coordinator=None,
        automation_delivery_broker=None, hpa_change_coordinator=None, memory_service=memory))


@pytest.mark.parametrize("question", [QUESTION, "DD가 최근에 한 말은?", "dd 는 뭐라고 했어?", "디디는 무슨 말 했어?"])
def test_live_plan_resolves_alias_and_binds_author_scope(runtime, question):
    value = request(question=question)
    prepared = runtime.memory.prepare(value)
    assert prepared["liveQuery"] == {
        "tenantId": TENANT, "actorId": ACTOR, "channelId": HUB, "requestId": REQUEST_ID,
        "question": question, "authorIds": [DD], "beforeTs": CUTOFF,
    }
    assert prepared["sources"] == [] and prepared["expiresIn"] == 60
    assert runtime.store.reads[0][1]["mentioned_user_ids"] == (DD,)


@pytest.mark.parametrize("changes", [
    {"tenant_id": "T2"}, {"channel_id": "C_OTHER"}, {"question": "내 기억 보여줘"},
])
def test_live_plan_is_absent_outside_fixed_hub_or_for_management(runtime, changes):
    value = request(**changes)
    prepared = runtime.memory.prepare(value)
    assert prepared["retrievalId"] and "liveQuery" not in prepared
    with pytest.raises(MemoryError, match="memory_live_retrieval_not_found"):
        receive(runtime.memory, prepared["retrievalId"])


@pytest.mark.parametrize("disabled", ["enabled", "read_enabled", "profile"])
def test_live_plan_is_not_created_when_memory_is_disabled_or_actor_is_not_profiled(runtime, disabled):
    if disabled == "profile":
        value = request(actor_id="U_UNPROFILED")
    else:
        setattr(runtime.memory, disabled, False)
        value = request()
    assert runtime.memory.prepare(value) == {"retrievalId": None, "sources": []}
    assert runtime.store.reads == []


def test_http_live_sources_reach_model_with_verified_citations_but_without_origin_url(runtime):
    # 실제 schema·정규화·렌더러를 연결하고 LLM만 순수 함수로 대체한다.
    with client(runtime.memory) as http:
        prepared = http.post(BASE, headers=headers(), json=retrieval_body())
        assert prepared.status_code == 200
        retrieval_id = prepared.json()["retrievalId"]
        result = http.post(f"{BASE}/{retrieval_id}/live", headers=headers(), json=upload_body())
        assert result.status_code == 200
        assert result.json() == {"retrievalId": retrieval_id, "acceptedSourceCount": 1}
    value = runtime.memory.bind_verified(bound_request(request(), retrieval_id))
    utterance, notice = value.memory_entries
    assert utterance["kind"] == "utterance" and utterance["subjectUserId"] == DD
    assert utterance["text"] == STATEMENT
    assert notice == {"kind": "retrieval_status", "coverage": "partial", "channelCount": 3,
                      "sourceCount": 1, "scope": "accessible_private_channels"}
    rendered, selected = render_memory(value.memory_entries)
    rows = json.loads(rendered.split(":\n", 1)[1])
    assert rows[0] == notice
    assert rows[1]["authors"] == [DD]
    assert rows[1]["sourceTimes"] == [{"authorId": DD, "authorName": "DD",
                                     "spokenAtKst": "2026-10-16T06:33:21.000001+09:00"}]
    assert set(selected) == {utterance["memoryId"]}
    seen = []
    def answerer(question, context, system):
        seen.append((question, context, system))
        return f"디디는 안내문 문구를 먼저 정리하자고 했어 [memory:{utterance['memoryId']}] [memory:unselected]"
    route = CompanyFreeformAssistantRoute(answerer, provider_ready=lambda: True,
        context_max_chars=5000, timeout_message="시간 초과")
    answer = route.handle(value)
    assert answer.outcome == "answered"
    assert STATEMENT in seen[0][1] and "retrieval_status" in seen[0][1]
    assert "https://" not in seen[0][1] and ORIGIN not in seen[0][1]
    assert all(source()["permalink"] not in argument for argument in seen[0] if argument)
    assert "partial이면" in seen[0][2]
    assert "memory:" not in answer.messages[0].body
    assert len(answer.sources) == 1 and answer.sources[0].uri == source()["permalink"]
    assert answer.sources[0].source_id == utterance["sources"][0]["sourceId"]


@pytest.mark.parametrize("question,expected", [
    ("디디가 오늘 한 말 요약", ["1792350000.000001", "1792335600.000000"]),
    ("디디가 어제 한 말 요약", ["1792335599.999999", "1792249200.000000"]),
])
def test_http_live_upload_is_filtered_by_original_question_kst_day(runtime, question, expected):
    # adapter가 여러 날짜를 보냈더라도 HTTP lease의 원 질문으로 최종 기간을 결정한다.
    value = dated_request(question)
    stamps = ["1792249199.999999", "1792249200.000000", "1792335599.999999",
              "1792335600.000000", "1792350000.000001"]
    with client(runtime.memory) as http:
        prepared = http.post(BASE, headers=headers(), json=retrieval_body(
            question=value.question, memoryContext=dict(value.memory_context)))
        assert prepared.status_code == 200
        payload = prepared.json()
        assert payload["liveQuery"]["beforeTs"] == "1792350000.000001"
        uploaded = http.post(f"{BASE}/{payload['retrievalId']}/live", headers=headers(),
            json=upload_body(sources=[dated_source(stamp) for stamp in stamps], coverage="complete", channelCount=1))
        assert uploaded.status_code == 200 and uploaded.json()["acceptedSourceCount"] == 2
    bound = runtime.memory.bind_verified(bound_request(value, payload["retrievalId"]))
    assert [item["sources"][0]["messageTs"] for item in bound.memory_entries if item["kind"] == "utterance"] == expected
    assert bound.memory_entries[-1]["coverage"] == "partial"
    assert bound.memory_entries[-1]["sourceCount"] == 2


@pytest.mark.parametrize("channel_id", [HUB, "C_OTHER"])
def test_date_question_keeps_only_stored_items_whose_entire_evidence_is_in_period(runtime, monkeypatch, channel_id):
    # 한 요약의 출처가 어제와 오늘에 걸치면 오늘 발언으로 재사용하지 않는다.
    value = dated_request(channel_id=channel_id)
    def stored(memory_id, stamps, kind="thread_summary"):
        return {"memoryId": memory_id, "kind": kind,
                "sources": [{"messageTs": stamp} for stamp in stamps]}
    style = stored("style", [SPOKEN], kind="personal_style")
    in_day = stored("in-day", ["1792335600.000000", "1792350000.000001"])
    old = stored("old", ["1792335599.999999"])
    mixed = stored("mixed", ["1792335599.999999", "1792335600.000000"])
    future = stored("future", ["1792350000.000002"])
    missing = stored("missing", [])
    monkeypatch.setattr(runtime.store, "load_verified_items", lambda **_: [old, mixed, missing, future, in_day, style])
    prepared = runtime.memory.prepare(value)
    if channel_id == HUB:
        receive(runtime.memory, prepared["retrievalId"], upload_body(
            sources=[dated_source("1792340000.000001")], coverage="complete", channelCount=1))
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    ids = [item.get("memoryId") for item in bound.memory_entries if item["kind"] != "retrieval_status"]
    assert set(ids) == ({"style", "in-day", "live_" + bound.memory_entries[1]["sources"][0]["sourceId"]}
                        if channel_id == HUB else {"style", "in-day"})
    if channel_id == HUB:
        assert bound.memory_entries[0] == style
        assert bound.memory_entries[1]["kind"] == "utterance"
        assert bound.memory_entries[2] == in_day
    else:
        assert "liveQuery" not in prepared


@pytest.mark.parametrize("changes", [
    {"tenantId": "T2"}, {"actorId": "UB"}, {"channelId": "C_OTHER"}, {"requestId": "another-request"},
])
def test_http_live_upload_rejects_cross_request_identity_without_consuming_valid_lease(runtime, changes):
    prepared = runtime.memory.prepare(request())
    path = f"{BASE}/{prepared['retrievalId']}/live"
    with client(runtime.memory) as http:
        rejected = http.post(path, headers=headers(), json=upload_body(**changes))
        assert rejected.status_code == 404 and rejected.json()["code"] == "not_found"
        accepted = http.post(path, headers=headers(), json=upload_body())
        assert accepted.status_code == 200 and accepted.json()["acceptedSourceCount"] == 1


def test_live_upload_same_payload_is_idempotent_and_conflicting_payload_is_rejected(runtime):
    prepared = runtime.memory.prepare(request())
    path = f"{BASE}/{prepared['retrievalId']}/live"
    with client(runtime.memory) as http:
        first = http.post(path, headers=headers(), json=upload_body())
        repeat = http.post(path, headers=headers(), json=upload_body())
        changed = "새 안내문은 다음 회의에서 같이 보자"
        conflicting = upload_body(sources=[source(text=changed, revisionHash=source_revision(changed))])
        conflict = http.post(path, headers=headers(), json=conflicting)
        assert first.status_code == repeat.status_code == 200
        assert repeat.json() == first.json()
        assert conflict.status_code == 409 and conflict.json()["code"] == "request_id_conflict"
    value = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert value.memory_entries[0]["text"] == STATEMENT


@pytest.mark.parametrize("delay,coverage,count", [(10.0, "partial", 1), (10.001, "unavailable", 0)])
def test_live_entries_expire_ten_seconds_after_first_receipt(runtime, delay, coverage, count):
    value = request()
    prepared = runtime.memory.prepare(value)
    receive(runtime.memory, prepared["retrievalId"])
    runtime.clock[0] += delay
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    assert len([item for item in bound.memory_entries if item["kind"] == "utterance"]) == count
    assert bound.memory_entries[-1]["coverage"] == coverage
    assert bound.memory_entries[-1]["sourceCount"] == count
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"])))


def test_idempotent_retry_does_not_refresh_raw_source_freshness(runtime):
    value = request()
    prepared = runtime.memory.prepare(value)
    receive(runtime.memory, prepared["retrievalId"])
    runtime.clock[0] = 109
    receive(runtime.memory, prepared["retrievalId"])
    runtime.clock[0] = 111
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    assert [item["kind"] for item in bound.memory_entries] == ["retrieval_status"]
    assert bound.memory_entries[0]["coverage"] == "unavailable"


def test_retrieval_lease_expires_at_sixty_seconds_even_when_live_sources_are_fresh(runtime):
    value = request()
    prepared = runtime.memory.prepare(value)
    runtime.clock[0] = 159
    receive(runtime.memory, prepared["retrievalId"])
    runtime.clock[0] = 160
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"])))


@pytest.mark.parametrize("reason", ["expired", "consumed", "unknown"])
def test_http_expired_consumed_or_unknown_lease_cannot_accept_or_normalize_more_raw_sources(runtime, monkeypatch, reason):
    value = request()
    prepared = runtime.memory.prepare(value)
    retrieval_id = prepared["retrievalId"]
    if reason == "expired":
        runtime.clock[0] = 160
    elif reason == "consumed":
        runtime.memory.bind_verified(bound_request(value, retrieval_id))
    else:
        retrieval_id = "unknown-retrieval-id"
    calls = []
    def forbidden_normalization(*args, **kwargs):
        calls.append(True)
        raise AssertionError("유효 lease 없이 원문을 처리하면 안 됨")
    monkeypatch.setattr(live_normalization, "normalize_live_sources", forbidden_normalization)
    with client(runtime.memory) as http:
        result = http.post(f"{BASE}/{retrieval_id}/live", headers=headers(), json=upload_body())
    assert result.status_code == 404 and calls == []


@pytest.mark.parametrize("change", ["actor", "tenant", "destination", "request", "question", "cutoff", "thread", "context"])
def test_live_binding_rejects_changed_turn_scope_and_cannot_be_replayed(runtime, change):
    value = request()
    prepared = runtime.memory.prepare(value)
    receive(runtime.memory, prepared["retrievalId"])
    changed = value
    if change == "actor":
        changed = replace(value, actor_id="UB", memory_context={**value.memory_context, "actorId": "UB"})
    elif change == "tenant":
        changed = replace(value, tenant_id="T2", memory_context={**value.memory_context, "workspaceId": "T2"})
    elif change == "destination":
        changed = replace(value, metadata={"channel_id": "C_OTHER"}, memory_context={**value.memory_context, "channelId": "C_OTHER"})
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


def test_sensitive_raw_source_is_filtered_and_never_rendered_or_logged(runtime, caplog):
    # 테스트용 민감 패턴도 원문 그대로 모델·로그·영속 저장에 남지 않아야 한다.
    rejected = "api_key=synthetic-denied-value"
    body = upload_body(sources=[source(text=rejected, revisionHash=source_revision(rejected))])
    value = request()
    prepared = runtime.memory.prepare(value)
    with client(runtime.memory) as http:
        result = http.post(f"{BASE}/{prepared['retrievalId']}/live", headers=headers(), json=body)
    assert result.status_code == 200 and result.json()["acceptedSourceCount"] == 0
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    rendered, selected = render_memory(bound.memory_entries)
    assert rejected not in rendered and selected == {}
    assert all(rejected not in record.getMessage() for record in caplog.records)


@pytest.mark.parametrize("invalid", ["after_cutoff", "wrong_author", "wrong_revision", "wrong_permalink"])
def test_http_live_source_semantic_validation_rejects_invalid_evidence(runtime, invalid):
    row = source()
    if invalid == "after_cutoff":
        row.update(messageTs=AFTER, permalink=f"https://unit.slack.com/archives/{ORIGIN}/p{AFTER.replace('.', '')}")
    elif invalid == "wrong_author":
        row["authorId"] = "U_OTHER"
    elif invalid == "wrong_revision":
        row["revisionHash"] = "0" * 64
    elif invalid == "wrong_permalink":
        row["permalink"] = "https://example.invalid/source"
    prepared = runtime.memory.prepare(request())
    with client(runtime.memory) as http:
        result = http.post(f"{BASE}/{prepared['retrievalId']}/live", headers=headers(), json=upload_body(sources=[row]))
    assert result.status_code == 409
    assert result.headers["content-type"] == "application/problem+json"
    assert STATEMENT not in result.text


@pytest.mark.parametrize("invalid", [
    "source_kind", "source_timestamp", "source_digest", "missing_thread", "extra_source_field",
    "too_many_sources", "large_source", "total_size", "boolean_count", "unavailable_with_sources",
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
    elif invalid == "too_many_sources":
        body["sources"] = [deepcopy(row) for _ in range(9)]
    elif invalid == "large_source":
        row["text"] = "가" * 8001
    elif invalid == "total_size":
        body["sources"] = [source(text="가" * 4500), source(text="나" * 4500)]
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


@pytest.mark.parametrize("event_type", ["deleted", "unavailable", "changed"])
def test_live_source_event_after_receipt_removes_cached_raw_evidence(runtime, event_type):
    value = request()
    prepared = runtime.memory.prepare(value)
    receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
        message_ts=SPOKEN, event_type=event_type, revision_hash="0" * 64 if event_type == "changed" else None)
    bound = runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"]))
    assert_no_source_evidence(bound, coverage="partial")


@pytest.mark.parametrize("stage", ["before_prepare", "before_receive"])
def test_live_source_deleted_before_receipt_is_not_reintroduced_by_upload(runtime, stage):
    # 원문 조회보다 삭제 알림이 빨라도 예전 페이지의 발언이 다시 살아나면 안 된다.
    if stage == "before_prepare":
        runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
            message_ts=SPOKEN, event_type="deleted")
    value = request()
    prepared = runtime.memory.prepare(value)
    if stage == "before_receive":
        runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
            message_ts=SPOKEN, event_type="deleted")
    accepted = receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    assert accepted["acceptedSourceCount"] == 0
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(value, prepared["retrievalId"])), coverage="partial")


@pytest.mark.parametrize("stage", ["before_prepare", "before_receipt", "after_receipt"])
@pytest.mark.parametrize("late_event", ["changed", "unavailable"])
def test_deleted_live_source_cannot_be_restored_by_a_delayed_older_event(runtime, stage, late_event):
    # 삭제 뒤 옛 수정 알림이 역순 도착해도 같은 ts의 캐시 본문을 재노출하면 안 된다.
    def invalidate(event_type):
        runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
            message_ts=SPOKEN, event_type=event_type,
            revision_hash=source()["revisionHash"] if event_type == "changed" else None)

    if stage == "before_prepare":
        invalidate("deleted")
    prepared = runtime.memory.prepare(request())
    if stage == "before_receipt":
        invalidate("deleted")
    if stage == "after_receipt":
        receive(runtime.memory, prepared["retrievalId"])
        invalidate("deleted")
    runtime.clock[0] += 1
    invalidate(late_event)
    receive(runtime.memory, prepared["retrievalId"])
    result = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert_no_source_evidence(result, coverage="partial")


@pytest.mark.parametrize("between", [None, "unavailable"])
@pytest.mark.parametrize("last_revision", ["old", "new"])
def test_different_change_revisions_cannot_restore_raw_evidence_without_event_order(runtime, between, last_revision):
    # 이벤트 순서를 판정할 수 없으면 옛 수정본과 새 수정본 중 어느 것도 확정 근거로 쓰지 않는다.
    prepared = runtime.memory.prepare(request())
    old_hash = source()["revisionHash"]
    new_hash = source_revision("새 안내문 문구는 다음 회의에 다시 정리하자")
    def invalidate(event_type, revision_hash=None):
        runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
            message_ts=SPOKEN, event_type=event_type, revision_hash=revision_hash)
    invalidate("changed", new_hash)
    if between:
        invalidate(between)
    invalidate("changed", old_hash)
    invalidate("changed", old_hash if last_revision == "old" else new_hash)
    receive(runtime.memory, prepared["retrievalId"])
    assert_no_source_evidence(
        runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"])),
        coverage="partial",
    )


def test_changed_event_allows_only_the_same_new_revision(runtime):
    updated_text = "안내문 문구는 새 회의에서 다시 확인하기로 했어"
    row = source(text=updated_text, editedAt=SPOKEN,
        revisionHash=source_revision(updated_text, SPOKEN))
    prepared = runtime.memory.prepare(request())
    runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
        message_ts=SPOKEN, event_type="changed", revision_hash=row["revisionHash"])
    accepted = receive(runtime.memory, prepared["retrievalId"], upload_body(
        sources=[row], coverage="complete", channelCount=1))
    assert accepted["acceptedSourceCount"] == 1
    bound = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert bound.memory_entries[0]["text"] == updated_text
    assert bound.memory_entries[0]["sources"][0]["revisionHash"] == row["revisionHash"]
    assert bound.memory_entries[-1]["coverage"] == "complete"


@pytest.mark.parametrize("change", ["tenant", "channel", "message"])
def test_live_source_event_does_not_cross_tenant_channel_or_message_identity(runtime, change):
    prepared = runtime.memory.prepare(request())
    receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    runtime.memory.invalidate_live_source(tenant_id="T2" if change == "tenant" else TENANT,
        channel_id="C_OTHER" if change == "channel" else ORIGIN,
        message_ts=ROOT if change == "message" else SPOKEN, event_type="deleted")
    bound = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert bound.memory_entries[0]["text"] == STATEMENT
    assert bound.memory_entries[-1]["coverage"] == "complete"
    assert bound.memory_entries[-1]["sourceCount"] == 1


@pytest.mark.parametrize("age,count", [(59.999, 0), (60.0, 1)])
def test_live_source_event_watermark_has_a_sixty_second_lifetime(runtime, age, count):
    runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
        message_ts=SPOKEN, event_type="deleted")
    runtime.clock[0] += age
    prepared = runtime.memory.prepare(request())
    accepted = receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    assert accepted["acceptedSourceCount"] == count
    bound = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert bound.memory_entries[-1]["sourceCount"] == count


def test_http_source_event_invalidates_live_evidence_and_acknowledges_exact_external_event(runtime, monkeypatch):
    prepared = runtime.memory.prepare(request())
    receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    calls = []
    monkeypatch.setattr(runtime.store, "source_event", lambda **kwargs: calls.append(kwargs) or {
        "eventId": "internal-journal-id", "invalidatedCount": 0})
    event = {"tenantId": TENANT, "channelId": ORIGIN, "messageTs": SPOKEN,
             "eventType": "deleted", "eventId": "Ev-live-deleted"}
    with client(runtime.memory) as http:
        result = http.post("/internal/v1/memory/sources/events", headers=headers(), json=event)
    assert result.status_code == 200
    assert result.json() == {"eventId": "Ev-live-deleted", "invalidatedCount": 0}
    assert len(calls) == 1 and calls[0]["event_id"] == event["eventId"]
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"])), coverage="partial")


def test_http_failed_source_event_does_not_claim_live_invalidation(runtime, monkeypatch):
    prepared = runtime.memory.prepare(request())
    receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    def failed_store(**kwargs):
        raise RuntimeError("synthetic storage fault")
    monkeypatch.setattr(runtime.store, "source_event", failed_store)
    with client(runtime.memory) as http:
        result = http.post("/internal/v1/memory/sources/events", headers=headers(), json={
            "tenantId": TENANT, "channelId": ORIGIN, "messageTs": SPOKEN,
            "eventType": "deleted", "eventId": "Ev-live-retry"})
    assert result.status_code == 503 and "eventId" not in result.json()
    bound = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert bound.memory_entries[0]["text"] == STATEMENT


def test_live_event_during_database_verification_is_checked_again_before_model_input(runtime, monkeypatch):
    # bind가 캐시를 소비한 뒤에도 DB 조회 중 도착한 삭제 알림을 놓치면 안 된다.
    prepared = runtime.memory.prepare(request())
    receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    def load_after_delete(**kwargs):
        runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
            message_ts=SPOKEN, event_type="deleted")
        return []
    monkeypatch.setattr(runtime.store, "load_verified_items", load_after_delete)
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"])), coverage="partial")


def test_live_lease_expiry_during_database_verification_cannot_extend_the_sixty_second_lease(runtime, monkeypatch):
    prepared = runtime.memory.prepare(request())
    runtime.clock[0] = 159
    receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    def load_after_lease_expiry(**kwargs):
        runtime.clock[0] = 160
        return []
    monkeypatch.setattr(runtime.store, "load_verified_items", load_after_lease_expiry)
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"])))


def test_live_event_capacity_reset_during_database_verification_invalidates_consumed_generation(runtime, monkeypatch):
    # 표식 상한에서 reset돼도 이미 꺼낸 옛 lease가 사라진 표식을 근거로 살아나지 않는다.
    prepared = runtime.memory.prepare(request())
    receive(runtime.memory, prepared["retrievalId"], upload_body(coverage="complete", channelCount=1))
    def load_after_event_capacity_reset(**kwargs):
        for index in range(4097):
            runtime.memory.invalidate_live_source(tenant_id=TENANT, channel_id=ORIGIN,
                message_ts=f"1700000000.{index:06d}", event_type="deleted")
        return []
    monkeypatch.setattr(runtime.store, "load_verified_items", load_after_event_capacity_reset)
    assert_no_source_evidence(runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"])))
    with pytest.raises(MemoryError, match="memory_live_retrieval_not_found"):
        receive(runtime.memory, prepared["retrievalId"])


@pytest.mark.parametrize("failure", ["cache_miss", "database_load"])
def test_live_unavailable_status_survives_cache_or_database_failure_without_raw_evidence(runtime, monkeypatch, caplog, failure):
    prepared = runtime.memory.prepare(request())
    receive(runtime.memory, prepared["retrievalId"])
    retrieval_id = prepared["retrievalId"]
    if failure == "cache_miss":
        retrieval_id = "lost-retrieval"
    else:
        def failed_load(**kwargs):
            raise RuntimeError("synthetic load failure")
        monkeypatch.setattr(runtime.store, "load_verified_items", failed_load)
    bound = runtime.memory.bind_verified(bound_request(request(), retrieval_id))
    assert_no_source_evidence(bound)
    assert bound.memory_entries[-1]["coverage"] == "unavailable"
    assert all(STATEMENT not in record.getMessage() for record in caplog.records)


def test_complete_live_result_becomes_partial_when_sensitive_source_is_filtered(runtime):
    filtered = "api_key=synthetic-filtered-value"
    rejected = source(messageTs=ROOT, text=filtered, revisionHash=source_revision(filtered),
        permalink=f"https://unit.slack.com/archives/{ORIGIN}/p{ROOT.replace('.', '')}")
    prepared = runtime.memory.prepare(request())
    accepted = receive(runtime.memory, prepared["retrievalId"], upload_body(
        sources=[source(), rejected], coverage="complete", channelCount=1))
    assert accepted["acceptedSourceCount"] == 1
    bound = runtime.memory.bind_verified(bound_request(request(), prepared["retrievalId"]))
    assert bound.memory_entries[-1]["coverage"] == "partial"
    assert bound.memory_entries[-1]["sourceCount"] == 1
    rendered, selected = render_memory(bound.memory_entries)
    assert filtered not in rendered and len(selected) == 1


@pytest.mark.parametrize("origin_count,reported_count", [(1, 0), (2, 1)])
def test_http_live_channel_count_cannot_be_less_than_distinct_result_origins(runtime, origin_count, reported_count):
    sources = [source()]
    if origin_count == 2:
        sources.append(source(channelId="C_PRIVATE_SECOND",
            permalink=f"https://unit.slack.com/archives/C_PRIVATE_SECOND/p{SPOKEN.replace('.', '')}"))
    prepared = runtime.memory.prepare(request())
    with client(runtime.memory) as http:
        result = http.post(f"{BASE}/{prepared['retrievalId']}/live", headers=headers(), json=upload_body(
            sources=sources, coverage="complete", channelCount=reported_count))
    assert result.status_code == 409 and result.json()["code"] == "request_id_conflict"
