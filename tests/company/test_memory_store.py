from __future__ import annotations

import json
import sqlite3

import pytest

from boxer_company.memory.contracts import (
    MEMORY_SCHEMA_VERSION,
    MemoryError,
    canonical_digest,
    source_revision,
)
from boxer_company.memory.policy import is_allowed_conversation_source
from boxer_company.memory.presentation import extract_memory_citations, render_memory
from boxer_company.memory.store import MemoryStore, initialize_memory_store


@pytest.fixture
def state(tmp_path):
    # 두 store 인스턴스가 같은 파일을 쓰도록 실제 SQLite 경계를 검증한다.
    clock = [1_800_000_000.0]
    path = tmp_path / "private" / "memory.db"
    initialize_memory_store(path)
    return MemoryStore(path, clock=lambda: clock[0], debounce_seconds=0), clock


def source(text="앞으로 짧게 답해줘", *, author="U1", ts="1800000000.000001", channel="C1", edited=""):
    return {"messageTs": ts, "revisionHash": source_revision(text, edited), "authorId": author,
            "authorKind": "human", "text": text, "permalink": f"https://team.slack.com/archives/{channel}/p{ts.replace('.', '')}",
            "createdAt": ts, "editedAt": edited}


def capture(store, *, request="event1", channel="C1", thread="1800000000.000001", message="1800000000.000001", actor="U1"):
    return store.create_capture(tenant_id="T1", actor_id=actor, channel_id=channel, thread_ts=thread,
                                current_message_ts=message, channel_kind="channel", request_id=request,
                                route="company_freeform")


def upload(store, cap, sources):
    return store.put_chunk(tenant_id=cap["tenantId"], capture_id=cap["captureId"], actor_id=cap["actorId"],
                           claim_token=cap["claimToken"], chunk_no=0, sources=sources, coverage="complete", is_last=True,
                           total_chunks=1, input_digest=canonical_digest([canonical_digest(sources)]))


def candidate(item, **kwargs):
    return {"kind": "person", "category": "preference", "subjectUserId": item["authorId"],
            "topicKey": "response_length", "text": "짧은 답변을 선호함", "evidenceType": "self_statement",
            "sources": [{"sourceId": item["sourceId"], "quote": item["text"], "relation": "supports"}],
            "subjects": [], "action": "add", **kwargs}


def publish(store, cap=None, sources=None, make_candidate=candidate):
    cap = cap or capture(store)
    upload(store, cap, sources or [source()])
    job = store.claim_job("worker")
    identity = {"tenant_id": "T1", "job_id": job["jobId"], "claim_token": job["claimToken"]}
    inputs = store.inputs(**identity)
    for chunk in inputs:
        values = [make_candidate(item) for item in chunk["sources"]]
        store.stage_candidates(**identity, chunk_no=chunk["chunkNo"], candidates=values)
    store.complete(**identity)
    return job


def verified(items):
    return [{"sourceId": ref["sourceId"], "revisionHash": ref["revisionHash"], "status": "verified"}
            for item in items for ref in item["sources"]]


def test_initialization_is_explicit_private_and_rejects_missing_runtime_file(state):
    store, _ = state
    assert store.path.stat().st_mode & 0o777 == 0o600
    store.path.unlink()
    assert not store.health()
    with pytest.raises(MemoryError, match="memory_storage_missing"):
        capture(store)
    assert not store.path.exists()


def test_unsafe_directory_and_symlink_rejected(tmp_path):
    unsafe = tmp_path / "unsafe"
    unsafe.mkdir(mode=0o755)
    with pytest.raises(MemoryError, match="directory_unsafe"):
        initialize_memory_store(unsafe / "memory.db")
    safe = tmp_path / "safe"
    safe.mkdir(mode=0o700)
    link = safe / "link.db"
    target = safe / "target.db"
    target.touch(mode=0o600)
    link.symlink_to(target)
    with pytest.raises(MemoryError, match="path_invalid"):
        initialize_memory_store(link)


def test_capture_request_is_idempotent_but_scope_change_is_rejected(state):
    store, _ = state
    first = capture(store)
    assert capture(store)["captureId"] == first["captureId"]
    with pytest.raises(MemoryError, match="identity_conflict"):
        capture(store, channel="C2")


def test_two_actors_keep_independent_preferences_and_scope(state):
    store, _ = state
    cap = capture(store, message="1800000001.000001")
    publish(store, cap, [source(), source("앞으로 자세히 답해줘", author="U2", ts="1800000001.000001")],
            lambda item: candidate(item, text="짧은 답변 선호" if item["authorId"] == "U1" else "자세한 답변 선호"))
    own = store.list_own_memories("T1", "C1", "U1")
    other = store.list_own_memories("T1", "C1", "U2")
    assert len(own) == len(other) == 1
    assert "짧은" in own[0]["text"] and "자세한" in other[0]["text"]
    assert store.retrieval_candidates("T2", "C1", "U1", "x", "답변") == []
    shared = store.retrieval_candidates("T1", "C2", "U1", "x", "답변")
    assert len(shared) == 1 and shared[0]["styleOnly"] is True
    assert shared[0]["subjectUserId"] == "U1"
    assert store.retrieval_candidates("T1", "C1", "U3", "x", "그 사람", mentioned_user_ids=["U2"])[0]["subjectUserId"] == "U2"


@pytest.mark.parametrize(("topic", "original", "repeated"), [
    ("response_length", "짧은 답변을 선호함", "앞으로도 핵심만 간단히 짧게 답변해 주기를 선호함"),
    ("response_length", "짧은 답변을 선호함", "앞으로도 핵심만 간단히 답하는 것을 선호함"),
    ("response_length", "짧은 답변을 선호함", "앞으로도 핵심만 간단히 답변하는 것을 선호함"),
    ("response_length", "짧은 답변 선호", "간결한 답변 선호"),
    ("response_length", "답변은 짧게 받는 것을 선호함", "간단한 응답을 선호함"),
    ("response_length", "자세한 답변을 선호함", "상세한 답변 선호"),
    ("response_language", "한국어 답변을 선호함", "한글로 답변해 주기를 선호함"),
    ("response_language", "영어 답변 선호", "앞으로 영어로 답변해 주기를 선호함"),
    ("response_order", "결론을 먼저 설명하는 답변을 선호함", "결론부터 답변해 주기를 선호함"),
])
def test_standard_preference_paraphrase_adds_evidence_without_conflict(state, topic, original, repeated):
    # 실모델 same_preference 결과처럼 같은 선호를 다른 문장으로 요약해도 근거만 누적한다.
    store, _ = state
    publish(store, sources=[source(original)], make_candidate=lambda item: candidate(item, topicKey=topic, text=original))
    old_id = store.list_own_memories("T1", "C1", "U1")[0]["memoryId"]
    newer = "1800000001.000001"
    publish(store, capture(store, request="repeat", thread=newer, message=newer),
            [source(repeated, ts=newer)], lambda item: candidate(item, topicKey=topic, text=repeated))
    memories = store.list_own_memories("T1", "C1", "U1")
    assert len(memories) == 1
    assert memories[0]["memoryId"] == old_id
    assert memories[0]["status"] == "active"
    assert len(memories[0]["sources"]) == 2


@pytest.mark.parametrize(("topic", "original", "opposite"), [
    ("response_length", "짧은 답변 선호", "자세한 답변 선호"),
    ("response_language", "한국어 답변 선호", "영어 답변 선호"),
    ("response_order", "결론 먼저 답변 선호", "설명 먼저 답변 선호"),
])
@pytest.mark.parametrize("corrects", [False, True])
def test_standard_opposite_preference_requires_explicit_correction(state, topic, original, opposite, corrects):
    store, _ = state
    publish(store, sources=[source(original)], make_candidate=lambda item: candidate(item, topicKey=topic, text=original))
    old_id = store.list_own_memories("T1", "C1", "U1")[0]["memoryId"]
    newer = "1800000001.000001"

    def changed(item):
        result = candidate(item, topicKey=topic, text=opposite)
        if corrects:
            result.update(action="replace", replacesId=old_id)
            result["sources"][0]["relation"] = "corrects"
        return result

    message = ("기존 선호를 바꿀게. " if corrects else "") + opposite
    publish(store, capture(store, request="change", thread=newer, message=newer),
            [source(message, ts=newer)], changed)
    memories = store.list_own_memories("T1", "C1", "U1")
    if corrects:
        assert [(item["text"], item["status"]) for item in memories] == [(opposite, "active")]
    else:
        assert len(memories) == 2
        assert {item["status"] for item in memories} == {"conflicted"}


@pytest.mark.parametrize(("topic", "original", "different"), [
    ("response_length", "짧은 답변 선호", "짧은 답변을 선호하지 않음"),
    ("response_length", "짧은 답변 선호", "짧은 답변 대신 자세한 답변을 선호함"),
    ("response_length", "짧은 답변 선호", "바쁠 때만 짧은 답변을 선호함"),
    ("response_length", "짧은 답변 선호", "짧은 답변에 예시를 포함하는 것을 선호함"),
    ("response_length", "짧은 답변 선호", "짧은 답변과 상세한 설명을 선호함"),
    ("response_length", "짧은 답변 선호", "3줄 이내의 짧은 답변을 선호함"),
    ("response_language", "한국어 답변 선호", "한국어와 영어 답변을 선호함"),
    ("response_order", "결론 먼저 답변 선호", "결론을 먼저 말하지 않는 답변 선호"),
    ("custom_response_style", "짧은 답변 선호", "간결한 답변 선호"),
])
def test_preference_paraphrase_does_not_discard_qualifiers_or_negation(state, topic, original, different):
    # 값이 같아 보여도 추가 의미가 남거나 표준 주제가 아니면 기존 충돌 처리를 유지한다.
    store, _ = state
    publish(store, sources=[source(original)], make_candidate=lambda item: candidate(item, topicKey=topic, text=original))
    newer = "1800000001.000001"
    publish(store, capture(store, request="different", thread=newer, message=newer),
            [source(different, ts=newer)], lambda item: candidate(item, topicKey=topic, text=different))
    memories = store.list_own_memories("T1", "C1", "U1")
    assert len(memories) == 2
    assert {item["status"] for item in memories} == {"conflicted"}


@pytest.mark.parametrize(("topic", "text", "value"), [
    ("response_length", "짧은 답변을 선호함", "short"),
    ("response_language", "영어 답변을 선호함", "en"),
    ("response_order", "결론부터 답변을 선호함", "conclusion_first"),
])
def test_cross_channel_style_exposes_only_own_canonical_value(state, topic, text, value):
    store, _ = state
    private_text = "비공개 프로젝트 오로라 검토 중이야. " + text
    publish(store, sources=[source(private_text)], make_candidate=lambda item: candidate(item, topicKey=topic, text=text))
    entries = store.retrieval_candidates("T1", "PUBLIC", "U1", "other-thread", "오늘 얘기해줘")
    assert len(entries) == 1 and entries[0]["styleOnly"] is True
    assert store.load_verified_items(tenant_id="T1", channel_id="PUBLIC", candidates=entries, verified_sources=verified(entries)) == []
    assert store.load_verified_items(tenant_id="T1", channel_id="PUBLIC", actor_id="U2", candidates=entries, verified_sources=verified(entries)) == []
    loaded = store.load_verified_items(tenant_id="T1", channel_id="PUBLIC", actor_id="U1", candidates=entries, verified_sources=verified(entries))
    assert loaded == [{"kind": "personal_style", "topicKey": topic, "value": value}]
    for supplied in (loaded, entries):
        rendered, selected = render_memory(supplied)
        assert selected == {}
        assert all(token not in rendered for token in ("C1", "slack.com", "오로라", "sourceId", "memoryId", "U1", "sourceTimes"))
    assert extract_memory_citations("답변 [memory:" + entries[0]["memoryId"] + "]", {entries[0]["memoryId"]: entries[0]}) == ("답변", ())
    assert store.retrieval_candidates("T1", "PUBLIC", "U2", "other-thread", "U1 답변 선호", mentioned_user_ids=["U1"]) == []
    assert store.retrieval_candidates("OTHER_TENANT", "PUBLIC", "U1", "other-thread", "답변") == []
    assert store.list_own_memories("T1", "PUBLIC", "U1") == []


@pytest.mark.parametrize("mutation", ["changed", "deleted", "unavailable", "forget", "suppress"])
def test_cross_channel_style_rechecks_source_and_forget_revocation(state, mutation):
    store, _ = state
    publish(store)
    entries = store.retrieval_candidates("T1", "PUBLIC", "U1", "other-thread", "오늘 얘기해줘")
    if mutation == "forget":
        store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U1", memory_id=entries[0]["memoryId"], request_id="forget")
    elif mutation == "suppress":
        store.suppress_topic(tenant_id="T1", channel_id="C1", actor_id="U1", topic_key="response_length", request_id="suppress")
    else:
        store.source_event(tenant_id="T1", channel_id="C1", message_ts="1800000000.000001", event_type=mutation, event_id="event")
    assert store.load_verified_items(tenant_id="T1", channel_id="PUBLIC", actor_id="U1", candidates=entries, verified_sources=verified(entries)) == []
    assert store.retrieval_candidates("T1", "PUBLIC", "U1", "other-thread", "오늘 얘기해줘") == []


@pytest.mark.parametrize("overrides", [
    {"category": "fact", "text": "기획 담당자임", "topicKey": "job_role"},
    {"text": "짧은 답변에 오로라 프로젝트 예시를 포함하는 것을 선호함"},
    {"topicKey": "custom_style", "text": "짧은 답변 선호"},
])
def test_cross_channel_does_not_share_facts_or_qualified_preferences(state, overrides):
    store, _ = state
    publish(store, make_candidate=lambda item: candidate(item, **overrides))
    assert store.retrieval_candidates("T1", "PUBLIC", "U1", "other-thread", "오로라 기획 답변") == []
    assert store.list_own_memories("T1", "C1", "U1")


def test_cross_channel_opposite_values_wait_for_explicit_latest_correction(state):
    store, _ = state
    publish(store)
    first = store.retrieval_candidates("T1", "PUBLIC", "U1", "other-thread", "답변")
    second_ts = "1800000001.000001"
    publish(store, capture(store, request="opposite", channel="C2", thread=second_ts, message=second_ts),
            [source("앞으로 자세히 답해줘", ts=second_ts, channel="C2")], lambda item: candidate(item, text="자세한 답변 선호"))
    assert store.retrieval_candidates("T1", "PUBLIC", "U1", "other-thread", "답변") == []
    assert store.retrieval_candidates("T1", "C1", "U1", "other-thread", "답변") == []
    assert store.load_verified_items(tenant_id="T1", channel_id="PUBLIC", actor_id="U1", candidates=first, verified_sources=verified(first)) == []
    latest_ts = "1800000002.000001"
    store.set_own_preference(tenant_id="T1", channel_id="PUBLIC", actor_id="U1", thread_ts=latest_ts,
                            current_message_ts=latest_ts, request_id="explicit-change", topic_key="response_length",
                            text="자세한 답변 선호", source=source("앞으로 자세히 답해줘", ts=latest_ts, channel="PUBLIC"))
    latest = store.retrieval_candidates("T1", "C1", "U1", "other-thread", "답변")
    assert [item["styleValue"] for item in latest] == ["detailed"]
    assert store.list_own_memories("T1", "C1", "U1") == []
    assert store.list_own_memories("T1", "C2", "U1") == []
    # 최신 명시 선호를 지워도 대체된 다른 채널의 예전 선호가 부활하지 않는다.
    store.forget_item(tenant_id="T1", channel_id="PUBLIC", actor_id="U1", memory_id=latest[0]["memoryId"], request_id="forget-latest")
    assert store.retrieval_candidates("T1", "C1", "U1", "other-thread", "답변") == []


def test_cross_channel_correction_uses_canonical_previous_and_blocks_old_recollection(state):
    store, _ = state
    publish(store)
    old = store.list_own_memories("T1", "C1", "U1")[0]
    ts = "1800000002.000001"
    cap = capture(store, request="correct", channel="C2", thread=ts, message=ts)
    upload(store, cap, [source("기존 선호를 바꿀게. 앞으로 자세히 답해줘", ts=ts, channel="C2")])
    job = store.claim_job("worker")
    identity = {"tenant_id": "T1", "job_id": job["jobId"], "claim_token": job["claimToken"]}
    inputs = store.inputs(**identity)
    prior = inputs[0]["previousItems"][0]
    assert prior["memoryId"] == old["memoryId"]
    assert "C1" not in json.dumps(prior) and "slack.com" not in json.dumps(prior)
    correction = candidate(inputs[0]["sources"][0], text="자세한 답변 선호", action="replace", replacesId=old["memoryId"])
    correction["sources"][0]["relation"] = "corrects"
    store.stage_candidates(**identity, chunk_no=0, candidates=[correction])
    store.complete(**identity)
    older = "1800000001.000001"
    publish(store, capture(store, request="old-recollect", channel="C3", thread=older, message=older),
            [source("앞으로 짧게 답해줘", ts=older, channel="C3")])
    assert [item["styleValue"] for item in store.retrieval_candidates("T1", "PUBLIC", "U1", "x", "답변")] == ["detailed"]


def test_cross_channel_new_preference_invalidates_running_snapshot(state):
    store, _ = state
    upload(store, capture(store), [source()])
    job = store.claim_job("worker")
    identity = {"tenant_id": "T1", "job_id": job["jobId"], "claim_token": job["claimToken"]}
    item = store.inputs(**identity)[0]["sources"][0]
    store.stage_candidates(**identity, chunk_no=0, candidates=[candidate(item)])
    ts = "1800000001.000001"
    store.set_own_preference(tenant_id="T1", channel_id="C2", actor_id="U1", thread_ts=ts, current_message_ts=ts,
                            request_id="new-preference", topic_key="response_length", text="자세한 답변 선호",
                            source=source("앞으로 자세히 답해줘", ts=ts, channel="C2"))
    with pytest.raises(MemoryError, match="items_revision_stale"):
        store.complete(**identity)


def test_import_capture_has_separate_identity_manifest_and_resume_claim(state):
    store, clock = state
    sources = [source()]
    digest = canonical_digest([canonical_digest(sources)])
    arguments = dict(tenant_id="T1", operator_id="OPERATOR", channel_id="C1", thread_ts="1800000000.000001",
                     cutoff_ts="1800000000.000001", channel_kind="channel", import_id="approved-history", input_digest=digest)
    cap = store.create_import_capture(**arguments)
    assert cap["route"] == "history_import" and cap["answerParts"] == []
    with pytest.raises(MemoryError, match="capture_scope_invalid"):
        store.create_capture(tenant_id="T1", actor_id="U1", channel_id="C1", thread_ts="1800000000.000001",
                             current_message_ts="1800000000.000001", channel_kind="channel", request_id="fake-mention", route="history_import")
    clock[0] += 121
    assert store.pull_captures("T1", "slack-worker") == []
    resumed = store.create_import_capture(**arguments)
    assert resumed["captureId"] == cap["captureId"] and resumed["claimToken"] != cap["claimToken"]
    with pytest.raises(MemoryError, match="claim_stale"):
        upload(store, cap, sources)
    with pytest.raises(MemoryError, match="manifest_conflict"):
        store.create_import_capture(**{**arguments, "input_digest": "0" * 64})
    upload(store, resumed, sources)
    assert store.create_import_capture(**arguments)["state"] == "ready"
    with sqlite3.connect(store.path) as connection:
        metadata = json.loads(connection.execute("SELECT metadata FROM memory_jobs").fetchone()[0])
    assert (metadata["triggerKind"], metadata["importId"], metadata["inputDigest"]) == ("history_import", "approved-history", digest)


def test_import_manifest_mismatch_rolls_back_and_cancelled_import_cannot_revive(state):
    store, _ = state
    args = dict(tenant_id="T1", operator_id="OPERATOR", channel_id="C1", thread_ts="1800000000.000001",
                cutoff_ts="1800000000.000001", channel_kind="channel", import_id="history", input_digest="0" * 64)
    cap = store.create_import_capture(**args)
    with pytest.raises(MemoryError, match="manifest_conflict"):
        upload(store, cap, [source()])
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0
    store.source_event(tenant_id="T1", channel_id="C1", message_ts="1800000000.000001", event_type="deleted", event_id="deleted")
    with pytest.raises(MemoryError, match="import_terminal"):
        store.create_import_capture(**args)


def test_reconciled_stage_validates_all_job_sources_and_publishes_atomically(state):
    store, clock = state
    cap = capture(store, message="1800000001.000001")
    parts = [[source()], [source("앞으로 자세히 답해줘", ts="1800000001.000001")]]
    for index, sources in enumerate(parts):
        store.put_chunk(tenant_id="T1", capture_id=cap["captureId"], actor_id="U1", claim_token=cap["claimToken"],
                        chunk_no=index, sources=sources, coverage="complete", is_last=index == 1,
                        total_chunks=2 if index == 1 else None,
                        input_digest=canonical_digest([canonical_digest(part) for part in parts]) if index == 1 else None)
    job = store.claim_job("worker")
    identity = {"tenant_id": "T1", "job_id": job["jobId"], "claim_token": job["claimToken"]}
    inputs = store.inputs(**identity)
    for part in inputs:
        store.stage_candidates(**identity, chunk_no=part["chunkNo"], candidates=[candidate(part["sources"][0])])
    clock[0] += 100
    store.renew_claim(**identity)
    clock[0] += 100
    final = candidate(inputs[1]["sources"][0], text="자세한 답변 선호")
    # chunk 1 근거로 만든 최종 결과를 chunk 0에 원자적으로 보관해도 실제 원본 검증은 유지한다.
    store.stage_reconciled_candidates(**identity, candidates=[final], usage={"inputTokens": 9})
    store.stage_reconciled_candidates(**identity, candidates=[final], usage={"inputTokens": 9})
    staged = store.inputs(**identity)
    assert all(part["reconciled"] for part in staged)
    assert staged[1]["candidates"] == [] and len(staged[0]["candidates"]) == 1
    with pytest.raises(MemoryError, match="result_conflict"):
        store.stage_reconciled_candidates(**identity, candidates=[])
    forged = {**final, "sources": [{"sourceId": "outside", "quote": "위조", "relation": "supports"}]}
    with pytest.raises(MemoryError, match="evidence_invalid"):
        store.stage_reconciled_candidates(**identity, candidates=[forged])
    assert store.list_own_memories("T1", "C1", "U1") == []
    store.complete(**identity)
    assert [item["text"] for item in store.list_own_memories("T1", "C1", "U1")] == ["자세한 답변 선호"]
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT input_tokens FROM memory_jobs").fetchone()[0] == 9


@pytest.mark.parametrize(("author", "message", "status"), [
    ("U1", "내가 맡은 검토 다 해놨어.", "resolved"),
    ("U2", "U1이 맡은 검토 다 해놨어.", "active"),
    ("U1", "완료했다고 말한 건 취소할게. 아직 검토하지 못했어.", "active"),
])
def test_new_task_completion_requires_direct_assignee_source(state, author, message, status):
    store, _ = state
    publish(store, sources=[source(message, author=author)], make_candidate=lambda item: candidate(
        item, kind="thread", category="task", subjectUserId=None, topicKey="안내문 검토",
        text="안내문 검토", evidenceType="attributed_statement", subjects=[{"userId": "U1", "role": "assignee"}]))
    with sqlite3.connect(store.path) as connection:
        row = connection.execute("SELECT topic_key,status FROM memory_items").fetchone()
    assert row == ("안내문검토", status)


def test_resolved_task_recollection_with_same_sources_does_not_duplicate(state):
    store, _ = state
    message = source("내가 맡은 안내문 검토 다 해놨어.")

    def task(item, text):
        return candidate(item, kind="thread", category="task", subjectUserId=None, topicKey="안내문 검토",
                         text=text, evidenceType="attributed_statement", subjects=[{"userId": "U1", "role": "assignee"}])

    publish(store, sources=[message], make_candidate=lambda item: task(item, "안내문 검토 완료"))
    upload(store, capture(store, request="recollect", message="1800000001.000001"),
           [message, source("고마워", ts="1800000001.000001")])
    job = store.claim_job("worker")
    identity = {"tenant_id": "T1", "job_id": job["jobId"], "claim_token": job["claimToken"]}
    repeated = store.inputs(**identity)[0]["sources"][0]
    store.stage_candidates(**identity, chunk_no=0, candidates=[task(repeated, "안내문 검토를 끝냄")])
    store.complete(**identity)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_items WHERE status='resolved'").fetchone()[0] == 1


def _task_candidate(item, text="안내문 검토 담당", topic="안내문 검토"):
    return candidate(item, kind="thread", category="task", subjectUserId=None, topicKey=topic,
                     text=text, evidenceType="attributed_statement", subjects=[{"userId": "U1", "role": "assignee"}])


def _history_capture(store, message, *, request="past-import", thread=None):
    return store.create_import_capture(
        tenant_id="T1", operator_id="OPERATOR", channel_id="C1", thread_ts=thread or message["messageTs"],
        cutoff_ts=message["messageTs"], channel_kind="channel", import_id=request,
        input_digest=canonical_digest([canonical_digest([message])]))


@pytest.mark.parametrize("resolved", [False, True])
@pytest.mark.parametrize("historical", [False, True])
def test_past_history_task_preserves_newer_active_or_resolved_memory(state, resolved, historical):
    store, _ = state
    # 같은 주제의 더 오래된 다른 원문은 현재 상태를 강등하거나 완료 업무를 되살리지 않는다.
    current = source("내가 맡은 안내문 검토 다 해놨어." if resolved else "내가 안내문 검토를 맡을게.")
    publish(store, sources=[current], make_candidate=lambda item: _task_candidate(
        item, "안내문 검토 완료" if resolved else "안내문 검토 담당", "안내문_검토"))
    with sqlite3.connect(store.path) as connection:
        before = connection.execute("SELECT * FROM memory_items").fetchall()
    past = source("내가 안내문 검토를 진행하고 있어.", ts="1799999000.000001")
    cap = (_history_capture(store, past) if historical else capture(
        store, request="late-live-message", thread=past["messageTs"], message=past["messageTs"]))
    publish(store, cap, [past], lambda item: _task_candidate(item, "안내문 검토 진행 중"))
    with sqlite3.connect(store.path) as connection:
        after = connection.execute("SELECT * FROM memory_items").fetchall()
        statuses = dict(connection.execute("SELECT status,count(*) FROM memory_items GROUP BY status"))
    if historical:
        assert after == before
        assert statuses == {"resolved" if resolved else "active": 1}
    else:
        # 이번 수정의 범위 밖인 실시간 add의 기존 충돌/새 업무 처리는 바꾸지 않는다.
        assert statuses == ({"resolved": 1, "active": 1} if resolved else {"conflicted": 2})


def test_past_history_different_topic_is_added(state):
    store, _ = state
    publish(store, sources=[source("내가 안내문 검토를 맡을게.")], make_candidate=_task_candidate)
    past = source("내가 회의록 정리를 맡을게.", ts="1799999000.000001")
    publish(store, _history_capture(store, past), [past],
            lambda item: _task_candidate(item, "회의록 정리 담당", "회의록 정리"))
    with sqlite3.connect(store.path) as connection:
        assert set(connection.execute("SELECT topic_key,status FROM memory_items")) == {
            ("안내문검토", "active"), ("회의록정리", "active")}


@pytest.mark.parametrize("resolved", [False, True])
@pytest.mark.parametrize("current_subject,past_subject", [(None, "U1"), ("U1", None)])
def test_past_history_task_subject_variation_does_not_bypass_newer_memory(state, resolved, current_subject, past_subject):
    store, _ = state
    # 모델이 스레드 기억의 subject를 작성자나 null로 바꾸어도 같은 업무의 최신 상태를 보호한다.
    current = source("내가 맡은 안내문 검토 다 해놨어." if resolved else "내가 안내문 검토를 맡을게.")
    publish(store, sources=[current], make_candidate=lambda item: {
        **_task_candidate(item, "안내문 검토 완료" if resolved else "안내문 검토 담당"),
        "subjectUserId": current_subject})
    with sqlite3.connect(store.path) as connection:
        before = connection.execute("SELECT * FROM memory_items").fetchall()
    past = source("내가 안내문 검토를 진행하고 있어.", ts="1799999000.000001")
    publish(store, _history_capture(store, past), [past], lambda item: {
        **_task_candidate(item, "안내문 검토 진행 중"), "subjectUserId": past_subject})
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT * FROM memory_items").fetchall() == before
        assert dict(connection.execute("SELECT status,count(*) FROM memory_items GROUP BY status")) == {
            "resolved" if resolved else "active": 1}


@pytest.mark.parametrize("historical", [False, True])
def test_newer_task_can_start_after_resolved_memory(state, historical):
    store, _ = state
    publish(store, sources=[source("내가 맡은 안내문 검토 다 해놨어.")],
            make_candidate=lambda item: _task_candidate(item, "안내문 검토 완료"))
    newer = source("이번에 새로 받은 안내문 검토를 내가 맡을게.", ts="1800000001.000001")
    cap = (_history_capture(store, newer) if historical else capture(
        store, request="new-task", thread=newer["messageTs"], message=newer["messageTs"]))
    publish(store, cap, [newer], lambda item: _task_candidate(item, "새 안내문 검토 담당"))
    with sqlite3.connect(store.path) as connection:
        assert dict(connection.execute("SELECT status,count(*) FROM memory_items GROUP BY status")) == {
            "resolved": 1, "active": 1}


def test_past_history_preserves_sources_and_monotonic_thread_cutoff(state):
    store, _ = state
    past = source("내가 안내문 검토를 진행하고 있어.", ts="1799999000.000001")
    current = source("내가 안내문 검토를 맡을게.")
    publish(store, capture(store, thread=past["messageTs"]), [current], _task_candidate)
    with sqlite3.connect(store.path) as connection:
        original = connection.execute("SELECT * FROM memory_sources WHERE message_ts=?", (current["messageTs"],)).fetchone()
    publish(store, _history_capture(store, past, thread=past["messageTs"]), [past],
            lambda item: _task_candidate(item, "안내문 검토 진행 중"))
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT * FROM memory_sources WHERE message_ts=?", (current["messageTs"],)).fetchone() == original
        assert set(connection.execute("SELECT message_ts,state FROM memory_sources")) == {
            (past["messageTs"], "current"), (current["messageTs"], "current")}
        assert connection.execute("SELECT collected_through_ts,input_revision FROM memory_threads").fetchone() == (current["messageTs"], 2)
        assert connection.execute("SELECT count(*) FROM memory_job_inputs").fetchone()[0] == 0


def test_atomic_publish_and_staged_payload_cleanup(state):
    store, _ = state
    cap = capture(store)
    upload(store, cap, [source()])
    job = store.claim_job("worker")
    identity = dict(tenant_id="T1", job_id=job["jobId"], claim_token=job["claimToken"])
    item = store.inputs(**identity)[0]["sources"][0]
    store.stage_candidates(**identity, chunk_no=0, candidates=[candidate(item)])
    assert store.list_own_memories("T1", "C1", "U1") == []
    store.complete(**identity)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0
        assert "previousItems" not in json.loads(connection.execute("SELECT metadata FROM memory_jobs").fetchone()[0])
        assert connection.execute("SELECT COUNT(*) FROM memory_summaries").fetchone()[0] == 1


def test_retrieval_requires_all_current_source_versions(state):
    store, _ = state
    publish(store)
    items = store.retrieval_candidates("T1", "C1", "U1", "x", "답변")
    assert store.load_verified_items(tenant_id="T1", channel_id="C1", candidates=items, verified_sources=[]) == []
    assert len(store.load_verified_items(tenant_id="T1", channel_id="C1", actor_id="U1", candidates=items, verified_sources=verified(items))) == 1
    store.source_event(tenant_id="T1", channel_id="C1", message_ts="1800000000.000001", event_type="deleted", event_id="delete1")
    assert store.load_verified_items(tenant_id="T1", channel_id="C1", candidates=items, verified_sources=verified(items)) == []


def test_lease_takeover_blocks_old_worker_result(state):
    store, clock = state
    upload(store, capture(store), [source()])
    old = store.claim_job("old")
    clock[0] += 121
    second = MemoryStore(store.path, clock=lambda: clock[0], debounce_seconds=0)
    new = second.claim_job("new")
    assert new["claimToken"] != old["claimToken"]
    with pytest.raises(MemoryError, match="claim_stale"):
        store.stage_candidates(tenant_id="T1", job_id=old["jobId"], claim_token=old["claimToken"], chunk_no=0, candidates=[])


def test_delete_cancels_running_job_and_late_capture_chunk(state):
    store, _ = state
    cap = capture(store)
    upload(store, cap, [source()])
    job = store.claim_job("worker")
    store.source_event(tenant_id="T1", channel_id="C1", message_ts="1800000000.000001", event_type="deleted", event_id="delete1")
    with pytest.raises(MemoryError, match="claim_stale"):
        store.complete(tenant_id="T1", job_id=job["jobId"], claim_token=job["claimToken"])
    with pytest.raises(MemoryError):
        upload(store, cap, [source()])
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0


def test_chunk_digest_and_author_are_enforced(state):
    store, _ = state
    cap = capture(store)
    arguments = dict(tenant_id="T1", capture_id=cap["captureId"], actor_id="U1", claim_token=cap["claimToken"], chunk_no=0, sources=[source()])
    store.put_chunk(**arguments)
    with pytest.raises(MemoryError, match="chunk_conflict"):
        store.put_chunk(**{**arguments, "sources": [source("앞으로 상세히 설명해줘")]})
    with pytest.raises(MemoryError, match="actor_invalid"):
        store.put_chunk(**{**arguments, "actor_id": "U2"})


def test_sensitive_source_never_persisted_and_coverage_stays_partial(state):
    store, _ = state
    cap = capture(store)
    unsafe = [source("password=secret-never-persist")]
    safe = [source("앞으로 결론부터 알려줘", ts="1800000000.000001")]
    store.put_chunk(tenant_id="T1", capture_id=cap["captureId"], actor_id="U1", claim_token=cap["claimToken"], chunk_no=0, sources=unsafe)
    store.put_chunk(tenant_id="T1", capture_id=cap["captureId"], actor_id="U1", claim_token=cap["claimToken"], chunk_no=1, sources=safe, coverage="complete", is_last=True, total_chunks=2, input_digest=canonical_digest([canonical_digest(unsafe), canonical_digest(safe)]))
    with sqlite3.connect(store.path) as connection:
        dump = "\n".join(connection.iterdump())
        assert "secret-never-persist" not in dump
        assert connection.execute("SELECT coverage FROM memory_threads").fetchone()[0] == "partial"


def test_forged_author_and_source_id_candidate_rejected(state):
    store, _ = state
    upload(store, capture(store), [source()])
    job = store.claim_job("worker")
    identity = dict(tenant_id="T1", job_id=job["jobId"], claim_token=job["claimToken"])
    item = store.inputs(**identity)[0]["sources"][0]
    with pytest.raises(MemoryError, match="author_invalid"):
        store.stage_candidates(**identity, chunk_no=0, candidates=[candidate(item, subjectUserId="U2")])
    with pytest.raises(MemoryError, match="evidence_invalid"):
        store.stage_candidates(**identity, chunk_no=0, candidates=[candidate(item, sources=[{"sourceId": "invented", "quote": "fake"}])])


def test_forget_removes_payload_and_prevents_recollection(state):
    store, clock = state
    publish(store)
    memory = store.list_own_memories("T1", "C1", "U1")[0]
    result = store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U1", memory_id=memory["memoryId"], request_id="forget1")
    assert result["durabilityState"] == "pending"
    assert store.list_own_memories("T1", "C1", "U1") == []
    store.mark_deletion_durable(result["id"], 1)
    assert next(event for event in store.deletion_events() if event["id"] == result["id"])["durabilityState"] == "durable"
    clock[0] += 1
    cap = capture(store, request="event2", message="1800000001.000001")
    publish(store, cap, [source("앞으로 짧게 답해줘", ts="1800000001.000001")])
    assert store.list_own_memories("T1", "C1", "U1") == []


def test_other_actor_cannot_forget_memory(state):
    store, _ = state
    publish(store)
    memory = store.list_own_memories("T1", "C1", "U1")[0]
    with pytest.raises(MemoryError, match="own_item_required"):
        store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U2", memory_id=memory["memoryId"], request_id="attack")


def test_snapshot_restore_replays_deletion_and_purges_jobs(state, tmp_path):
    store, _ = state
    publish(store)
    old_snapshot = tmp_path / "snapshot.db"
    store.snapshot(old_snapshot)
    memory = store.list_own_memories("T1", "C1", "U1")[0]
    event = store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U1", memory_id=memory["memoryId"], request_id="forget1")
    restored = MemoryStore(old_snapshot)
    restored.apply_deletion_event(event)
    restored.apply_deletion_event(event)
    restored.cleanup(restored=True)
    assert restored.list_own_memories("T1", "C1", "U1") == []
    with pytest.raises(FileExistsError):
        store.snapshot(old_snapshot)


def test_expired_temporary_inputs_are_removed(state):
    store, clock = state
    cap = capture(store)
    store.put_chunk(tenant_id="T1", capture_id=cap["captureId"], actor_id="U1", claim_token=cap["claimToken"], chunk_no=0, sources=[source()])
    clock[0] += 8 * 86400
    assert store.cleanup()["payloadsRemoved"] == 1
    assert store.pull_captures("T1", "worker") == []


def test_explicit_preference_is_own_source_idempotent_and_replaces(state):
    store, _ = state
    arguments = dict(tenant_id="T1", channel_id="C1", actor_id="U1", thread_ts="1800000000.000001",
                     current_message_ts="1800000000.000001", request_id="preference1", topic_key="response_length",
                     text="짧게 답변", source=source())
    first = store.set_own_preference(**arguments)
    assert store.set_own_preference(**arguments)["memoryId"] == first["memoryId"]
    second = store.set_own_preference(**{**arguments, "request_id": "preference2", "current_message_ts": "1800000002.000001",
                                        "source": source("앞으로 자세히 답해줘", ts="1800000002.000001"), "text": "자세히 답변"})
    assert second["memoryId"] != first["memoryId"]
    assert [item["text"] for item in store.list_own_memories("T1", "C1", "U1")] == ["자세히 답변"]


def test_cross_thread_item_update_prevents_stale_job_publish(state):
    store, _ = state
    publish(store)
    cap = capture(store, request="event2", thread="1800000010.000001", message="1800000010.000001")
    upload(store, cap, [source("앞으로 순서대로 답해줘", ts="1800000010.000001")])
    job = store.claim_job("worker")
    identity = dict(tenant_id="T1", job_id=job["jobId"], claim_token=job["claimToken"])
    store.stage_candidates(**identity, chunk_no=0, candidates=[])
    store.set_own_preference(tenant_id="T1", channel_id="C1", actor_id="U1", thread_ts="1800000020.000001", current_message_ts="1800000020.000001", request_id="newpref", topic_key="response_length", text="자세히 답변", source=source("앞으로 자세히 답해줘", ts="1800000020.000001"))
    with pytest.raises(MemoryError, match="revision_stale"):
        store.complete(**identity)


def test_interest_requires_three_distinct_threads(state):
    store, _ = state
    interest = lambda item: candidate(item, category="interest", evidenceType="inference", topicKey="독서", text="독서에 관심이 있음")
    for index, ts in enumerate(["1800000000.000001", "1800000010.000001", "1800000020.000001"]):
        publish(store, capture(store, request=f"reading{index}", thread=ts, message=ts), [source("독서 모임 이야기하자", ts=ts)], interest)
        items = store.list_own_memories("T1", "C1", "U1")
        assert len(items) == (1 if index == 2 else 0)


def test_bot_text_requires_exact_delivery_receipt(state):
    store, clock = state
    cap = store.create_capture(tenant_id="T1", actor_id="U1", channel_id="C1", thread_ts="1800000000.000001", current_message_ts="1800000000.000001", channel_kind="channel", request_id="bot1", route="company_freeform", answer_parts=["답변이야"])
    receipt = {"messageTs": "1800000001.000001", "partIndex": 0, "deliveryId": cap["answerParts"][0]["deliveryId"], "authorId": "B1", "textHash": canonical_digest("<@U1> 답변이야")}
    store.checkpoint(tenant_id="T1", capture_id=cap["captureId"], claim_token=cap["claimToken"], delivery_receipts=[receipt])
    cap = store.pull_captures("T1", "collector")[0]
    bot = {**source("<@U1> 답변이야", author="B1", ts="1800000001.000001"), "authorKind": "boxer", "deliveryVerified": True, "route": "company_freeform"}
    upload(store, cap, [source(), bot])
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_sources WHERE author_kind='boxer'").fetchone()[0] == 1


@pytest.mark.parametrize("text", ["마미박스 무료병원 자동 업데이트 켜줘", "이 스레드 학습해줘", "<@B1> 이 스레드 학습해줘"])
def test_real_operation_router_excludes_old_sensitive_requests(text):
    assert not is_allowed_conversation_source(text)
    assert is_allowed_conversation_source("내 답변 선호를 짧게 바꿔줘")


@pytest.mark.parametrize("running", [False, True])
def test_new_partial_capture_carries_unprocessed_safe_sources(state, running):
    store, _ = state
    first = capture(store)
    upload(store, first, [source()])
    old_job = store.claim_job("old") if running else None
    second = capture(store, request="event2", message="1800000001.000001", actor="U2")
    new_sources = [source("앞으로 자세히 답해줘", author="U2", ts="1800000001.000001")]
    store.put_chunk(tenant_id="T1", capture_id=second["captureId"], actor_id="U2", claim_token=second["claimToken"],
                    chunk_no=0, sources=new_sources, coverage="partial", is_last=True, total_chunks=1,
                    input_digest=canonical_digest([canonical_digest(new_sources)]))
    if old_job:
        with pytest.raises(MemoryError, match="claim_stale"):
            store.complete(tenant_id="T1", job_id=old_job["jobId"], claim_token=old_job["claimToken"])
    current = store.claim_job("latest")
    identity = dict(tenant_id="T1", job_id=current["jobId"], claim_token=current["claimToken"])
    chunks = store.inputs(**identity)
    assert {item["authorId"] for chunk in chunks for item in chunk["sources"]} == {"U1", "U2"}
    for chunk in chunks:
        store.stage_candidates(**identity, chunk_no=chunk["chunkNo"], candidates=[candidate(item) for item in chunk["sources"]])
    store.complete(**identity)
    assert len(store.list_own_memories("T1", "C1", "U1")) == len(store.list_own_memories("T1", "C1", "U2")) == 1
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0


def test_collecting_source_delete_cancels_before_finalize(state):
    store, _ = state
    cap = capture(store)
    store.put_chunk(tenant_id="T1", capture_id=cap["captureId"], actor_id="U1", claim_token=cap["claimToken"], chunk_no=0, sources=[source()])
    store.source_event(tenant_id="T1", channel_id="C1", message_ts="1800000000.000001", event_type="deleted", event_id="early-delete")
    with pytest.raises(MemoryError, match="claim_stale"):
        upload(store, cap, [source()])
    assert store.deletion_events()[0]["deletionKind"] == "source"


def test_source_only_keeps_accepted_short_evidence(state):
    store, _ = state
    text = "앞으로 짧게 답해줘. " + "이번 대화의 일회성 설명이야. " * 30
    cap = capture(store)
    upload(store, cap, [source(text)])
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT excerpt FROM memory_sources").fetchone()[0] == ""
    job = store.claim_job("worker")
    identity = dict(tenant_id="T1", job_id=job["jobId"], claim_token=job["claimToken"])
    item = store.inputs(**identity)[0]["sources"][0]
    memory = candidate(item)
    memory["sources"][0]["quote"] = "앞으로 짧게 답해줘"
    store.stage_candidates(**identity, chunk_no=0, candidates=[memory])
    store.complete(**identity)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT excerpt FROM memory_sources").fetchone()[0] == "앞으로 짧게 답해줘"
        assert "일회성 설명" not in "\n".join(connection.iterdump())


def test_out_of_order_edit_event_does_not_invalidate_already_collected_revision(state):
    store, _ = state
    publish(store)
    edited = source("앞으로 자세히 답해줘", edited="1800000002.000001")
    publish(store, capture(store, request="edit-capture"), [edited], lambda item: candidate(item, text="자세한 답변 선호"))
    store.source_event(tenant_id="T1", channel_id="C1", message_ts=edited["messageTs"], event_type="changed", event_id="delayed-edit", revision_hash=edited["revisionHash"])
    current = store.retrieval_candidates("T1", "C1", "U1", "x", "답변")
    assert len(current) == 1 and current[0]["text"] == "자세한 답변 선호"


def test_topic_suppression_is_own_scope_and_idempotent(state):
    store, _ = state
    publish(store)
    arguments = dict(tenant_id="T1", channel_id="C1", actor_id="U1", topic_key="response_length", request_id="suppress1")
    first = store.suppress_topic(**arguments)
    assert store.suppress_topic(**arguments)["id"] == first["id"]
    assert store.list_own_memories("T1", "C1", "U1") == []
    with pytest.raises(MemoryError, match="request_conflict"):
        store.suppress_topic(**{**arguments, "actor_id": "U2"})


@pytest.mark.parametrize("event_type", ["changed", "deleted"])
def test_source_event_before_first_upload_invalidates_in_memory_capture(state, event_type):
    store, _ = state
    cap = capture(store)
    # adapter가 읽은 시점과 첫 전송 사이에 온 삭제도 이전 token을 막아야 한다.
    fetched = [source()]
    assert store.source_event(tenant_id="T1", channel_id="C1", message_ts=fetched[0]["messageTs"],
                              event_type=event_type, event_id="before-upload")["eventId"] is None
    with pytest.raises(MemoryError, match="claim_stale"):
        upload(store, cap, fetched)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0


def raw_import(store, *, tenant="T1", channel="C1", state="collecting", name="raw1",
               start="1799999999.000001", cutoff="1800000001.000001"):
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_raw_imports(id,tenant_id,channel_id,author_id,operator_id,start_ts,"
                           "cutoff_ts,manifest_digest,state,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
                           (name, tenant, channel, "U1", "UOP", start, cutoff, canonical_digest(name), state, store._clock()))


def attach_raw_text(store, source_id, *, text="원문 조회 보존용 문장"):
    raw_import(store)
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_source_texts(tenant_id,source_id,import_id,text,stored_at) VALUES(?,?,?,?,?)",
                           ("T1", source_id, "raw1", text, store._clock()))


def test_schema1_migration_preserves_all_existing_rows_and_is_explicit_idempotent(state):
    store, _ = state
    publish(store)
    with store._connection(write=True) as connection:
        connection.execute("DROP TABLE memory_raw_event_states")
        connection.execute("DROP TABLE memory_source_texts")
        connection.execute("DROP TABLE memory_raw_imports")
        connection.execute("PRAGMA user_version=1")
        tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        before = {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in tables}
    # runtime은 구버전 DB를 자동 migration하지 않고 닫힌다.
    with pytest.raises(MemoryError, match="storage_unhealthy"):
        MemoryStore(store.path)
    initialize_memory_store(store.path)
    initialize_memory_store(store.path)
    with store._connection() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == MEMORY_SCHEMA_VERSION == 3
        assert {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in tables} == before
        assert connection.execute("SELECT COUNT(*) FROM memory_source_texts").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_raw_imports").fetchone()[0] == 0
    assert MemoryStore(store.path).health()


def test_schema1_migration_rolls_back_partial_ddl_and_version_on_failure(state, monkeypatch):
    from boxer_company.memory import store as module

    store, _ = state
    publish(store)
    with store._connection(write=True) as connection:
        connection.execute("DROP TABLE memory_raw_event_states")
        connection.execute("DROP TABLE memory_source_texts")
        connection.execute("DROP TABLE memory_raw_imports")
        connection.execute("PRAGMA user_version=1")
        sources = connection.execute("SELECT * FROM memory_sources").fetchall()
    monkeypatch.setattr(module, "_RAW_SCHEMA", module._RAW_SCHEMA + "CREATE TABLE broken (;")
    with pytest.raises(sqlite3.OperationalError):
        initialize_memory_store(store.path)
    with store._connection() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert not connection.execute("SELECT name FROM sqlite_master WHERE name LIKE 'memory_raw_%' OR name='memory_source_texts'").fetchall()
        assert connection.execute("SELECT * FROM memory_sources").fetchall() == sources


def test_schema1_migration_rejects_an_unknown_database_even_with_claimed_version(tmp_path):
    directory = tmp_path / "private"
    directory.mkdir(mode=0o700)
    path = directory / "unknown.db"
    path.touch(mode=0o600)
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE unrelated (id TEXT)")
        connection.execute("PRAGMA user_version=1")
    with pytest.raises(MemoryError, match="schema_unsupported"):
        initialize_memory_store(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall() == [("unrelated",)]


def test_raw_source_text_tenant_foreign_keys_cannot_link_another_tenant_or_import(state):
    store, _ = state
    publish(store)
    source_id = store.list_own_memories("T1", "C1", "U1")[0]["sources"][0]["sourceId"]
    raw_import(store, tenant="T2")
    with pytest.raises(sqlite3.IntegrityError), store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_source_texts(tenant_id,source_id,import_id,text,stored_at) VALUES(?,?,?,?,?)",
                           ("T1", source_id, "raw1", "다른 tenant import는 사용할 수 없어", store._clock()))


@pytest.mark.parametrize("event_type", ["changed", "deleted", "unavailable"])
def test_source_invalidation_deletes_raw_text_in_the_same_transaction(state, event_type):
    store, _ = state
    publish(store)
    source_id = store.list_own_memories("T1", "C1", "U1")[0]["sources"][0]["sourceId"]
    attach_raw_text(store, source_id)
    store.source_event(tenant_id="T1", channel_id="C1", message_ts=source()["messageTs"],
                       event_type=event_type, event_id="raw-change")
    with store._connection() as connection:
        assert not connection.execute("SELECT * FROM memory_source_texts").fetchall()


def test_new_source_revision_and_forgetting_item_remove_raw_text(state):
    store, _ = state
    publish(store)
    item = store.list_own_memories("T1", "C1", "U1")[0]
    attach_raw_text(store, item["sources"][0]["sourceId"])
    edited = source("앞으로 자세히 답해줘", edited="1800000002.000001")
    publish(store, capture(store, request="edited-capture"), [edited], lambda row: candidate(row, text="자세한 답변 선호"))
    with store._connection(write=True) as connection:
        assert not connection.execute("SELECT * FROM memory_source_texts").fetchall()
        latest = connection.execute("SELECT id FROM memory_sources WHERE state='current'").fetchone()[0]
        connection.execute("INSERT INTO memory_source_texts VALUES(?,?,?,?,?)", ("T1", latest, "raw1", edited["text"], store._clock()))
    own = store.list_own_memories("T1", "C1", "U1")[0]
    store.forget_item(tenant_id="T1", channel_id="C1", actor_id="U1", memory_id=own["memoryId"], request_id="forget-raw")
    with store._connection() as connection:
        assert not connection.execute("SELECT * FROM memory_source_texts").fetchall()


@pytest.mark.parametrize("kind", ["source", "source_revision", "source_redaction", "item", "subject_topic"])
def test_deletion_replay_also_erases_raw_text(state, kind):
    store, _ = state
    publish(store)
    item = store.list_own_memories("T1", "C1", "U1")[0]
    source_id = item["sources"][0]["sourceId"]
    attach_raw_text(store, source_id)
    store.apply_deletion_event({"id": "raw-delete", "tenantId": "T1", "scopeChannelId": "C1",
        "deletionKind": kind, "sourceMessageId": source()["messageTs"], "sourceId": source_id,
        "memoryId": item["memoryId"], "subjectId": "U1", "topicKeyHash": canonical_digest("response_length")})
    with store._connection() as connection:
        assert not connection.execute("SELECT * FROM memory_source_texts").fetchall()


@pytest.mark.parametrize("event_type,kind", [("changed", "source_redaction"), ("deleted", "source"), ("unavailable", "source_redaction")])
def test_unknown_event_in_approved_raw_snapshot_blocks_later_import_idempotently(state, event_type, kind):
    store, _ = state
    raw_import(store)
    arguments = {"tenant_id": "T1", "channel_id": "C1", "message_ts": source()["messageTs"],
                 "event_type": event_type, "event_id": "raw-before-source"}
    result = store.source_event(**arguments)
    assert result["eventId"] and store.source_event(**arguments)["eventId"] == result["eventId"]
    with store._connection() as connection:
        event_row = connection.execute("SELECT * FROM memory_events WHERE id=?", (result["eventId"],)).fetchone()
        assert event_row["deletion_kind"] == kind and event_row["durability_state"] == "pending"
        assert not connection.execute("SELECT * FROM memory_sources").fetchall()
    upload(store, capture(store), [source()])
    with store._connection() as connection:
        assert not connection.execute("SELECT * FROM memory_sources").fetchall()
        assert all(json.loads(row[0]) == [] for row in connection.execute("SELECT payload FROM memory_job_inputs"))


@pytest.mark.parametrize("change", [
    {"tenant_id": "T2"}, {"channel_id": "C2"}, {"message_ts": "1799999999.000000999"},
    {"message_ts": "1800000001.000001001"},
])
def test_unknown_event_outside_approved_raw_scope_does_not_create_tombstones(state, change):
    store, _ = state
    raw_import(store)
    result = store.source_event(**{"tenant_id": "T1", "channel_id": "C1", "message_ts": source()["messageTs"],
                                   "event_type": "deleted", "event_id": "outside", **change})
    assert result["eventId"] is None
    with store._connection() as connection:
        assert not connection.execute("SELECT * FROM memory_events").fetchall()


@pytest.mark.parametrize("import_state", ["failed", "cancelled", "completed"])
def test_unknown_event_only_uses_collecting_raw_imports(state, import_state):
    store, _ = state
    raw_import(store, state=import_state)
    assert store.source_event(tenant_id="T1", channel_id="C1", message_ts=source()["messageTs"],
                              event_type="changed", event_id="inactive")["eventId"] is None


@pytest.mark.parametrize("message_ts", ["1799999999.000001", "1800000001.000001"])
def test_raw_snapshot_unknown_event_window_includes_exact_decimal_endpoints(state, message_ts):
    store, _ = state
    raw_import(store)
    assert store.source_event(tenant_id="T1", channel_id="C1", message_ts=message_ts,
                              event_type="deleted", event_id="endpoint")["eventId"]


def test_same_revision_changed_event_preserves_already_collected_raw_text(state):
    store, _ = state
    publish(store)
    item = store.list_own_memories("T1", "C1", "U1")[0]
    attach_raw_text(store, item["sources"][0]["sourceId"])
    assert store.source_event(tenant_id="T1", channel_id="C1", message_ts=source()["messageTs"], event_type="changed",
                              event_id="same-revision", revision_hash=source()["revisionHash"])["eventId"] is None
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_source_texts").fetchone()[0] == 1


def test_cleanup_keeps_valid_explicit_raw_text_beyond_excerpt_expiry_and_never_injects_it(state):
    store, clock = state
    publish(store)
    item = store.list_own_memories("T1", "C1", "U1")[0]
    hidden = "원문 전용 문장은 요약과 모델 입력으로 자동 주입하지 않아"
    attach_raw_text(store, item["sources"][0]["sourceId"], text=hidden)
    assert hidden not in json.dumps(store.retrieval_candidates("T1", "C1", "U1", "x", "답변"), ensure_ascii=False)
    clock[0] += 100 * 86400
    assert store.cleanup()["sourceTextsRemoved"] == 0
    with store._connection(write=True) as connection:
        assert connection.execute("SELECT text FROM memory_source_texts").fetchone()[0] == hidden
        assert connection.execute("SELECT excerpt FROM memory_sources").fetchone()[0] == ""
        connection.execute("UPDATE memory_sources SET state='unavailable'")
    assert store.cleanup()["sourceTextsRemoved"] == 1


@pytest.mark.parametrize("kind", ["source", "source_redaction", "source_revision"])
def test_cleanup_deletes_raw_text_when_only_the_revocation_journal_is_present(state, kind):
    store, _ = state
    publish(store)
    item = store.list_own_memories("T1", "C1", "U1")[0]
    source_id = item["sources"][0]["sourceId"]
    attach_raw_text(store, source_id)
    with store._connection(write=True) as connection:
        connection.execute("INSERT INTO memory_events(id,tenant_id,scope_channel_id,event_type,request_id,deletion_kind,"
                           "source_id,source_message_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                           ("journal-only", "T1", "C1", "source_deleted", "event1", kind, source_id,
                            source()["messageTs"], store._clock()))
    assert store.cleanup()["sourceTextsRemoved"] == 1


def test_schema2_migration_preserves_raw_rows_and_requires_explicit_upgrade(state):
    store, _ = state
    publish(store)
    source_id = store.list_own_memories("T1", "C1", "U1")[0]["sources"][0]["sourceId"]
    attach_raw_text(store, source_id)
    with store._connection(write=True) as connection:
        connection.execute("DROP TABLE memory_raw_event_states")
        connection.execute("PRAGMA user_version=2")
        before = {table: connection.execute(f"SELECT * FROM {table}").fetchall()
                  for table in ("memory_sources", "memory_source_texts", "memory_raw_imports", "memory_items", "memory_events")}
    with pytest.raises(MemoryError, match="storage_unhealthy"):
        MemoryStore(store.path)
    initialize_memory_store(store.path)
    with store._connection() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 3
        assert {table: connection.execute(f"SELECT * FROM {table}").fetchall() for table in before} == before
        assert connection.execute("SELECT COUNT(*) FROM memory_raw_event_states").fetchone()[0] == 0


def test_schema2_migration_rolls_back_watermark_table_and_version_on_failure(state, monkeypatch):
    from boxer_company.memory import store as module

    store, _ = state
    with store._connection(write=True) as connection:
        connection.execute("DROP TABLE memory_raw_event_states")
        connection.execute("PRAGMA user_version=2")
    monkeypatch.setattr(module, "_RAW_EVENT_SCHEMA", module._RAW_EVENT_SCHEMA + "CREATE TABLE broken (;")
    with pytest.raises(sqlite3.OperationalError):
        initialize_memory_store(store.path)
    with store._connection() as connection:
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 2
        assert not connection.execute("SELECT 1 FROM sqlite_master WHERE name='memory_raw_event_states'").fetchone()


def test_raw_watermark_schema_has_nonnull_columns_compound_identity_and_health_guard(state):
    store, _ = state
    with store._connection(write=True) as connection:
        columns = connection.execute("PRAGMA table_info(memory_raw_event_states)").fetchall()
        assert [row["name"] for row in columns] == ["tenant_id", "channel_id", "message_ts", "event_ts", "revision_hash", "state"]
        assert all(row["notnull"] for row in columns)
        assert [row["name"] for row in columns if row["pk"]] == ["tenant_id", "channel_id", "message_ts"]
        connection.execute("DROP TABLE memory_raw_event_states")
    assert not store.health()


def test_raw_only_scope_rejects_new_automatic_capture_and_explicit_preference_before_writing(state):
    from boxer_company.memory.raw_policy import RAW_ONLY_START_TS

    store, _ = state
    arguments = {"tenant_id": "T19PA3EPL", "channel_id": "C0621TL2HSB", "actor_id": "U1",
                 "thread_ts": RAW_ONLY_START_TS, "current_message_ts": RAW_ONLY_START_TS, "request_id": "raw-only"}
    with pytest.raises(MemoryError, match="memory_raw_only_scope"):
        store.create_capture(**arguments, channel_kind="private_channel", route="freeform")
    with pytest.raises(MemoryError, match="memory_raw_only_scope"):
        store.set_own_preference(**arguments, topic_key="response_length", text="짧은 답변 선호", source={})
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_jobs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 0


def test_raw_only_policy_cancels_existing_ready_job_before_any_model_input_is_read(state, monkeypatch):
    from boxer_company.memory import store as module

    store, _ = state
    cap = capture(store)
    upload(store, cap, [source()])
    monkeypatch.setattr(module, "_raw_only_channel", lambda tenant, channel: tenant == "T1" and channel == "C1")
    assert store.claim_job("must-not-call-model") is None
    with store._connection() as connection:
        assert connection.execute("SELECT state FROM memory_jobs").fetchone()[0] == "canceled"
        assert connection.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 1


@pytest.mark.parametrize("operation", ["inputs", "stage", "complete"])
def test_raw_only_policy_blocks_already_running_job_at_every_read_and_publication_stage(state, monkeypatch, operation):
    from boxer_company.memory import store as module

    store, _ = state
    cap = capture(store)
    upload(store, cap, [source()])
    job = store.claim_job("worker")
    identity = {"tenant_id": "T1", "job_id": job["jobId"], "claim_token": job["claimToken"]}
    monkeypatch.setattr(module, "_raw_only_channel", lambda tenant, channel: channel == "C1")
    with pytest.raises(MemoryError, match="memory_raw_only_scope"):
        if operation == "inputs":
            store.inputs(**identity)
        elif operation == "stage":
            store.stage_candidates(**identity, chunk_no=0, candidates=[])
        else:
            store.complete(**identity)
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_items").fetchone()[0] == 0


@pytest.mark.parametrize("event_type", ["changed", "unavailable", "deleted"])
def test_unknown_raw_only_events_block_only_deletions_and_never_a_later_normal_edit(state, event_type):
    from boxer_company.memory.policy import normalize_source
    from boxer_company.memory.raw_policy import RAW_ONLY_START_TS

    store, _ = state
    tenant, channel, stamp = "T19PA3EPL", "C0621TL2HSB", RAW_ONLY_START_TS
    event = store.source_event(tenant_id=tenant, channel_id=channel, message_ts=stamp,
                               event_type=event_type, event_id="unknown-raw-event")
    with store._connection(write=True) as connection:
        if event_type == "deleted":
            assert event["eventId"] and store._source_deleted(connection, tenant, channel, stamp)
        else:
            assert event["eventId"] is None
            assert not connection.execute("SELECT 1 FROM memory_events WHERE deletion_kind='source_redaction'").fetchone()
            thread = store._thread(connection, tenant, channel, stamp, "private_channel")
            normalized = normalize_source(source("새 정상 편집 본문", ts=stamp, channel=channel),
                tenant_id=tenant, channel_id=channel, thread_ts=stamp, current_message_ts=stamp)
            assert store._register_source(connection, tenant, thread, normalized)


@pytest.mark.parametrize("stamp", ["1788188400.000001", "1793458800.000000", "1893456000.000001"])
@pytest.mark.parametrize("channel", ["C0621TL2HSB", "C068FVD5V7Y"])
def test_raw_only_channel_never_restarts_derived_capture_or_preference_outside_october(state, stamp, channel):
    store, _ = state
    arguments = {"tenant_id": "T19PA3EPL", "channel_id": channel, "actor_id": "U1",
                 "thread_ts": stamp, "current_message_ts": stamp, "request_id": "all-period-raw-only"}
    with pytest.raises(MemoryError, match="memory_raw_only_scope"):
        store.create_capture(**arguments, channel_kind="private_channel", route="freeform")
    with pytest.raises(MemoryError, match="memory_raw_only_scope"):
        store.set_own_preference(**arguments, topic_key="response_length", text="짧은 답변 선호", source={})
    with store._connection() as connection:
        for table in ("memory_jobs", "memory_sources", "memory_items"):
            assert connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0


@pytest.mark.parametrize("stamp", ["1788188400.000001", "1793458800.000001"])
def test_old_snapshot_non_october_job_is_canceled_by_channel_policy_without_reading_model_inputs(state, monkeypatch, stamp):
    from boxer_company.memory import store as module

    store, _ = state
    channel, tenant = "C0621TL2HSB", "T19PA3EPL"
    channel_policy = module._raw_only_channel
    # 정책 전 snapshot에는 다른 달의 ready job도 있을 수 있지만 재기동 시 다시 모델에 넘기지 않는다.
    monkeypatch.setattr(module, "_raw_only_channel", lambda *_: False)
    cap = store.create_capture(tenant_id=tenant, channel_id=channel, actor_id="U1", thread_ts=stamp,
        current_message_ts=stamp, request_id="old-snapshot-job", channel_kind="private_channel", route="freeform")
    upload(store, cap, [source(channel=channel, ts=stamp)])
    snapshot = store.path.with_name("old-ready-snapshot.db")
    store.snapshot(snapshot)
    monkeypatch.setattr(module, "_raw_only_channel", channel_policy)
    restored = MemoryStore(snapshot, clock=store._clock)
    assert restored.claim_job("must-not-call-model") is None
    with restored._connection() as connection:
        assert connection.execute("SELECT state FROM memory_jobs").fetchone()[0] == "canceled"
        assert connection.execute("SELECT COUNT(*) FROM memory_job_inputs").fetchone()[0] == 0
        assert connection.execute("SELECT COUNT(*) FROM memory_sources").fetchone()[0] == 1


def test_raw_only_channel_final_publication_guard_also_rejects_non_october_sources(state):
    from boxer_company.memory.policy import normalize_source

    store, _ = state
    tenant, channel, stamp = "T19PA3EPL", "C068FVD5V7Y", "1793458800.000001"
    normalized = normalize_source(source(ts=stamp, channel=channel), tenant_id=tenant, channel_id=channel,
                                  thread_ts=stamp, current_message_ts=stamp)
    with store._connection(write=True) as connection:
        thread = store._thread(connection, tenant, channel, stamp, "private_channel")
        assert store._register_source(connection, tenant, thread, normalized)
        assert not store._publish_candidate(connection, tenant, thread, candidate(normalized), "late-publication")
        assert connection.execute("SELECT COUNT(*) FROM memory_items").fetchone()[0] == 0


def test_neighbor_channel_keeps_existing_capture_and_explicit_preference_behavior(state):
    store, _ = state
    tenant, channel, stamp = "T19PA3EPL", "CNEIGHBOR", "1793458800.000001"
    cap = store.create_capture(tenant_id=tenant, channel_id=channel, actor_id="U1", thread_ts=stamp,
        current_message_ts=stamp, request_id="neighbor-capture", channel_kind="private_channel", route="freeform")
    assert cap["state"] == "collecting"
    result = store.set_own_preference(tenant_id=tenant, channel_id=channel, actor_id="U1", thread_ts=stamp,
        current_message_ts=stamp, request_id="neighbor-preference", topic_key="response_length", text="짧은 답변 선호",
        source=source(channel=channel, ts=stamp))
    assert result["status"] == "active" and result["topicKey"] == "response_length"


@pytest.mark.parametrize("event_type", ["changed", "deleted", "unavailable"])
@pytest.mark.parametrize("stamp", ["1788188400.000001", "1793458800.000001"])
def test_channel_derivative_policy_does_not_expand_unknown_source_event_collection_window(state, event_type, stamp):
    store, _ = state
    result = store.source_event(tenant_id="T19PA3EPL", channel_id="C0621TL2HSB", message_ts=stamp,
        event_type=event_type, event_id="outside-october-source")
    assert result["eventId"] is None
    with store._connection() as connection:
        assert connection.execute("SELECT COUNT(*) FROM memory_events").fetchone()[0] == 0
