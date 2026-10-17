from __future__ import annotations

from copy import deepcopy
import sqlite3

import pytest

from boxer_company.memory.contracts import canonical_digest, source_revision
from boxer_company.memory.store import MemoryStore, initialize_memory_store


TENANT = "T19PA3EPL"
HUB = "C068FVD5V7Y"
STAMP = "1800000000.000001"


@pytest.fixture
def store(tmp_path):
    path = tmp_path / "private" / "memory.db"
    initialize_memory_store(path)
    return MemoryStore(path, clock=lambda: 1_800_000_000.0, debounce_seconds=0)


def publish(store, channel, kind="private_channel", *, tenant=TENANT, preference=False):
    # 실제 capture·추출 publish 경로로 원본 채널 종류와 tenant를 DB에 고정한다.
    text = "앞으로 짧게 답해줘" if preference else "내가 안내문 검토를 맡았어"
    cap = store.create_capture(
        tenant_id=tenant, actor_id="UDD", channel_id=channel, thread_ts=STAMP,
        current_message_ts=STAMP, channel_kind=kind, request_id=f"capture-{tenant}-{channel}",
        route="company_freeform",
    )
    sources = [{
        "messageTs": STAMP, "revisionHash": source_revision(text), "authorId": "UDD",
        "authorKind": "human", "text": text, "createdAt": STAMP, "editedAt": "",
        "permalink": f"https://team.slack.com/archives/{channel}/p{STAMP.replace('.', '')}",
    }]
    store.put_chunk(
        tenant_id=tenant, capture_id=cap["captureId"], actor_id="UDD", claim_token=cap["claimToken"],
        chunk_no=0, sources=sources, coverage="complete", is_last=True, total_chunks=1,
        input_digest=canonical_digest([canonical_digest(sources)]),
    )
    job = store.claim_job("worker")
    identity = {"tenant_id": tenant, "job_id": job["jobId"], "claim_token": job["claimToken"]}
    item = store.inputs(**identity)[0]["sources"][0]
    store.stage_candidates(**identity, chunk_no=0, candidates=[{
        "kind": "person", "category": "preference" if preference else "fact",
        "subjectUserId": "UDD", "topicKey": "response_length" if preference else "안내문 검토",
        "text": "짧은 답변 선호" if preference else "안내문 검토를 맡음",
        "evidenceType": "self_statement", "action": "add", "subjects": [],
        "sources": [{"sourceId": item["sourceId"], "quote": text, "relation": "supports"}],
    }])
    store.complete(**identity)
    return store.list_own_memories(tenant, channel, "UDD")[0]


def candidates(store, destination, *, tenant=TENANT, actor="UREADER"):
    return store.retrieval_candidates(
        tenant, destination, actor, "other-thread", "디디가 안내문 검토를 맡았어?", mentioned_user_ids=["UDD"],
    )


def verified(items):
    return [{"sourceId": source["sourceId"], "revisionHash": source["revisionHash"], "status": "verified"}
            for item in items for source in item["sources"]]


def load(store, destination, items, *, tenant=TENANT, actor="UREADER"):
    return store.load_verified_items(
        tenant_id=tenant, channel_id=destination, actor_id=actor,
        candidates=items, verified_sources=verified(items),
    )


def test_hub_reads_local_and_same_tenant_private_sources_only(store):
    local = publish(store, HUB)
    private = publish(store, "CPRIVATE")
    publish(store, "CPUBLIC", "channel")
    publish(store, "DPRIVATE", "dm")
    publish(store, "GPRIVATE", "group_dm")
    publish(store, "COTHER", tenant="OTHER_TENANT")

    selected = candidates(store, HUB)
    assert {item["memoryId"] for item in selected} == {local["memoryId"], private["memoryId"]}
    assert {item["memoryId"] for item in load(store, HUB, selected)} == {local["memoryId"], private["memoryId"]}
    assert all(source["channelKind"] == "private_channel" for item in selected for source in item["sources"])


@pytest.mark.parametrize("kind", ["private_channel", "channel", "dm", "group_dm"])
def test_normal_channel_keeps_local_source_access_without_cross_channel_expansion(store, kind):
    local = publish(store, "CLOCAL", kind)
    publish(store, "CPRIVATE")
    publish(store, HUB)
    selected = candidates(store, "CLOCAL")
    assert [item["memoryId"] for item in selected] == [local["memoryId"]]
    assert [item["memoryId"] for item in load(store, "CLOCAL", selected)] == [local["memoryId"]]
    assert candidates(store, "CEMPTY") == []


def test_other_tenant_cannot_reuse_hub_channel_id_or_verification(store):
    private = publish(store, "CPRIVATE")
    local = publish(store, HUB, tenant="OTHER_TENANT")
    publish(store, "COTHERPRIVATE", tenant="OTHER_TENANT")
    selected = candidates(store, HUB, tenant="OTHER_TENANT")
    assert [item["memoryId"] for item in selected] == [local["memoryId"]]
    assert load(store, HUB, [private], tenant="OTHER_TENANT") == []


@pytest.mark.parametrize("kind", ["channel", "dm", "group_dm"])
def test_forged_private_channel_flag_never_grants_hub_access(store, kind):
    original = publish(store, "CEXCLUDED", kind)
    forged = deepcopy(original)
    forged.update(privateMemoryHub=True, allowCrossChannel=True, channelKind="private_channel")
    for source in forged["sources"]:
        source["channelKind"] = "private_channel"
    # 원본 검증 결과를 위조해도 item의 실제 DB 범위가 먼저 적용된다.
    assert load(store, HUB, [forged]) == []
    assert candidates(store, HUB) == []


def test_forged_hub_flag_never_expands_normal_destination(store):
    original = publish(store, "CPRIVATE")
    forged = {**original, "privateMemoryHub": True, "allowCrossChannel": True, "destinationChannelId": HUB}
    assert load(store, "CORDINARY", [forged]) == []
    assert candidates(store, "CORDINARY") == []


def test_source_kind_changes_between_search_and_verification_are_rechecked(store):
    publish(store, "CPRIVATE")
    selected = candidates(store, HUB)
    assert len(selected) == 1
    with sqlite3.connect(store.path) as connection:
        connection.execute("UPDATE memory_threads SET channel_kind='channel' WHERE channel_id='CPRIVATE'")
    assert load(store, HUB, selected) == []
    assert candidates(store, HUB) == []


def test_private_item_cannot_pull_in_a_public_source_through_its_references(store):
    private = publish(store, "CPRIVATE")
    public = publish(store, "CPUBLIC", "channel")
    # item의 원채널뿐 아니라 연결된 출처 각각의 DB 채널 종류도 확인해야 한다.
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "INSERT INTO memory_item_sources(tenant_id,memory_id,source_id,relation,evidence_span) VALUES(?,?,?,?,?)",
            (TENANT, private["memoryId"], public["sources"][0]["sourceId"], "supports", "안내문 검토"),
        )
    linked = store.list_own_memories(TENANT, "CPRIVATE", "UDD")
    assert {source["channelKind"] for source in linked[0]["sources"]} == {"channel", "private_channel"}
    assert load(store, HUB, linked) == []
    assert candidates(store, HUB) == []


def test_deleted_source_is_not_loaded_with_previously_verified_hub_candidate(store):
    publish(store, "CPRIVATE")
    selected = candidates(store, HUB)
    assert len(selected) == 1
    store.source_event(tenant_id=TENANT, channel_id="CPRIVATE", message_ts=STAMP,
                       event_type="deleted", event_id="delete-after-hub-search")
    assert load(store, HUB, selected) == []
    assert candidates(store, HUB) == []


def test_own_standard_style_keeps_existing_cross_channel_redaction(store):
    publish(store, "DPRIVATE", "dm", preference=True)
    own = candidates(store, HUB, actor="UDD")
    assert len(own) == 1 and own[0]["styleOnly"] is True
    assert load(store, HUB, own, actor="UDD") == [
        {"kind": "personal_style", "topicKey": "response_length", "value": "short"},
    ]
    assert candidates(store, HUB, actor="UREADER") == []
