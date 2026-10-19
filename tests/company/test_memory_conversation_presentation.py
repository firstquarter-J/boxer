"""맥락 묶음의 작성자·대화 경계·인용·최종 노출 건수를 검증한다."""

from copy import deepcopy
import json

import pytest

from boxer_company import team_chat_context as team
from boxer_company.memory.presentation import extract_memory_citations, render_memory


@pytest.fixture(autouse=True)
def people(monkeypatch):
    monkeypatch.setattr(team, "TEAM_MEMBER_PROFILES", (
        {"name": "나래", "user_id": "UNARAE", "aliases": ()},
        {"name": "도윤", "user_id": "UDOYUN", "aliases": ()},
    ))


def reference(source_id, author, stamp, *, relation="supports"):
    return {"sourceId": source_id, "authorId": author, "messageTs": stamp,
            "relation": relation, "channelId": "CPRIVATE", "threadTs": "1792335000.000001",
            "permalink": f"https://unit.slack.com/archives/CPRIVATE/p{stamp.replace('.', '')}"}


def bundle(*, memory_id="live_anchor", thread="thread_one", context="발표 선물은 나래가 받으세요."):
    return {"memoryId": memory_id, "kind": "utterance", "category": "statement",
            "subjectUserId": "UNARAE", "text": "제가 왜요", "evidenceType": "direct_statement",
            "status": "active", "updatedAt": "2026-10-19T00:00:00Z", "liveSource": True,
            "excerptOnly": False, "threadId": thread, "contextCoverage": "partial",
            "sources": [reference("anchor", "UNARAE", "1792371600.000001"),
                        reference("parent", "UDOYUN", "1792335000.000001", relation="context")],
            "contextMessages": [{"sourceId": "parent", "authorId": "UDOYUN", "text": context,
                                 "relation": "thread_parent", "excerptOnly": False}]}


def rows(rendered):
    return json.loads(rendered.split(":\n", 1)[1])


def test_parent_keeps_own_author_previous_day_and_separate_citation():
    item = bundle()
    before = deepcopy(item)
    rendered, selected = render_memory((item,))
    row = rows(rendered)[0]
    assert item == before
    assert row["subjectName"] == "나래" and row["authors"] == ["UNARAE"]
    assert row["sourceTimes"] == [{"authorId": "UNARAE", "authorName": "나래",
                                   "spokenAtKst": "2026-10-19T10:00:00.000001+09:00"}]
    assert row["contextMessages"] == [{"memoryId": "live_parent", "text": before["contextMessages"][0]["text"],
        "authorId": "UDOYUN", "authorName": "도윤", "spokenAtKst": "2026-10-18T23:50:00.000001+09:00",
        "relation": "thread_parent", "excerptOnly": False}]
    assert row["contextCoverage"] == "partial" and row["conversationKey"] == "thread_one"
    assert all(value not in rendered for value in ("CPRIVATE", "https://", "permalink"))
    answer, citations = extract_memory_citations("선물에 대한 답변 [memory:live_parent] [memory:live_anchor]", selected)
    assert answer == "선물에 대한 답변"
    assert [source.source_id for source in citations] == ["parent", "anchor"]


def test_anchor_citation_does_not_automatically_claim_every_context_was_used():
    _, selected = render_memory((bundle(),))
    _, citations = extract_memory_citations("나래가 답했다 [memory:live_anchor]", selected)
    assert [source.source_id for source in citations] == ["anchor"]


def test_identical_utterance_in_different_threads_retains_distinct_context():
    first = bundle()
    second = bundle(memory_id="live_second", thread="thread_two", context="실패 책임자는 나래인가요?")
    second["sources"][0]["sourceId"] = "second"
    second["sources"][1]["sourceId"] = "second_parent"
    second["contextMessages"][0]["sourceId"] = "second_parent"
    rendered, _ = render_memory((first, second))
    values = rows(rendered)
    assert [row["text"] for row in values] == ["제가 왜요", "제가 왜요"]
    assert [row["conversationKey"] for row in values] == ["thread_one", "thread_two"]
    assert [row["contextMessages"][0]["text"] for row in values] == [
        "발표 선물은 나래가 받으세요.", "실패 책임자는 나래인가요?"]


def test_channel_neighbor_is_not_upgraded_into_a_thread_reply():
    item = bundle()
    item["contextMessages"][0]["relation"] = "channel_neighbor"
    rendered, _ = render_memory((item,))
    assert rows(rendered)[0]["contextMessages"][0]["relation"] == "channel_neighbor"


def test_budget_omits_whole_bundle_and_reports_model_visible_anchor_count():
    # 대화 묶음이 예산을 넘으면 원문만 남겨 잘못 해석시키지 않고 묶음 전체를 제외한다.
    huge = bundle(context="발표 선물 이야기입니다. " * 1600)
    notice = {"kind": "retrieval_status", "coverage": "complete", "channelCount": 1,
              "sourceCount": 1, "scope": "current_private_channel"}
    rendered, selected = render_memory((huge, notice))
    assert selected == {}
    assert rows(rendered) == [{**notice, "coverage": "partial", "renderedSourceCount": 0, "omittedSourceCount": 1}]
    assert "제가 왜요" not in rendered and "발표 선물" not in rendered


def test_context_does_not_inflate_count_and_missing_context_is_explicit():
    item = bundle()
    notice = {"kind": "retrieval_status", "coverage": "partial", "channelCount": 1,
              "sourceCount": 1, "scope": "current_private_channel"}
    rendered, _ = render_memory((item, notice))
    assert rows(rendered)[0]["renderedSourceCount"] == 1
    assert rows(rendered)[0]["omittedSourceCount"] == 0
    item["contextMessages"] = []
    rendered, _ = render_memory((item, notice))
    assert rows(rendered)[1]["contextCoverage"] == "unavailable"
    assert rows(rendered)[1]["contextMessages"] == []
