"""맥락 묶음의 작성자·대화 경계·인용·최종 노출 건수를 검증한다."""

from copy import deepcopy
import json

import pytest

from boxer_company import team_chat_context as team
from boxer_company.memory import presentation
from boxer_company.memory.live_limits import LIVE_RENDER_MAX_BYTES, LIVE_RENDER_MAX_CHARS, LIVE_SOURCE_MAX_CHARS, LIVE_TOTAL_MAX_CHARS
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


def plain_live(index, text):
    item = bundle(memory_id=f"live_target_{index}")
    item["text"] = text
    item["sources"] = [reference(f"target_{index}", "UNARAE", f"1792371600.{index + 1:06d}")]
    item["contextMessages"] = []
    return item


def notice(count):
    return {"kind": "retrieval_status", "coverage": "complete", "channelCount": 1,
            "sourceCount": count, "scope": "current_private_channel"}


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


def test_more_than_eight_live_targets_keep_their_complete_text_and_selected_citations():
    items = [plain_live(index, f"발언 {index}: " + "가" * 2990) for index in range(20)]
    rendered, selected = render_memory((*items, notice(len(items))))
    values = rows(rendered)
    assert values[0]["renderedSourceCount"] == 20 and values[0]["omittedSourceCount"] == 0
    assert values[0]["coverage"] == "complete"
    assert [row["text"] for row in values[1:]] == [item["text"] for item in items]
    assert set(selected) == {item["memoryId"] for item in items}
    # 발언 개수와 인용 개수는 다르다. 실제 선택된 20개 중에도 표시 링크는 기존 2개만 쓴다.
    answer = " ".join(f"[memory:{item['memoryId']}]" for item in items)
    assert len(extract_memory_citations(answer, selected)[1]) == 2


def test_exact_raw_limit_preserves_full_live_sources_and_canonical_actor_style():
    items = [plain_live(index, "가" * LIVE_SOURCE_MAX_CHARS) for index in range(5)]
    style = {"kind": "personal_style", "topicKey": "response_length", "value": "short"}
    rendered, selected = render_memory((*items, style, notice(len(items))))
    values = rows(rendered)
    assert values[1]["kind"] == "personal_style"
    assert sum(len(row["text"]) for row in values if row.get("kind") == "utterance") == LIVE_TOTAL_MAX_CHARS
    assert len(selected) == 5 and values[0]["renderedSourceCount"] == 5
    payload = rendered.split(":\n", 1)[1]
    assert len(payload) <= LIVE_RENDER_MAX_CHARS and len(payload.encode("utf-8")) <= LIVE_RENDER_MAX_BYTES


def test_live_targets_take_priority_over_the_separate_eight_stored_items():
    stored = [{**plain_live(index + 100, "저장 요약"), "liveSource": False, "kind": "person"} for index in range(12)]
    live = [plain_live(index, "조회된 발언") for index in range(20)]
    rendered, selected = render_memory((*stored, *live, notice(len(live))))
    values = rows(rendered)
    assert [row["memoryId"] for row in values[1:21]] == [item["memoryId"] for item in live]
    assert [row["memoryId"] for row in values[21:]] == [item["memoryId"] for item in stored[:8]]
    assert len(selected) == 28


def test_raw_limit_omits_whole_bundles_and_only_selected_targets_can_be_cited():
    items = [plain_live(index, "가" * LIVE_SOURCE_MAX_CHARS) for index in range(6)]
    rendered, selected = render_memory((*items, notice(len(items))))
    assert rows(rendered)[0] == {**notice(6), "coverage": "partial", "renderedSourceCount": 5, "omittedSourceCount": 1}
    assert items[-1]["memoryId"] not in selected
    assert extract_memory_citations(f"답변 [memory:{items[-1]['memoryId']}]", selected) == ("답변", ())


def test_json_escape_overhead_can_omit_an_atomic_full_context_bundle():
    # 원문은 80k여도 JSON escaping과 작성자 metadata까지 합치면 렌더 예산을 넘을 수 있다.
    item = plain_live(0, '"' * LIVE_SOURCE_MAX_CHARS)
    for index in range(4):
        source = reference(f"parent_{index}", "UDOYUN", f"1792335000.{index + 1:06d}", relation="context")
        item["sources"].append(source)
        item["contextMessages"].append({"sourceId": source["sourceId"], "text": '"' * LIVE_SOURCE_MAX_CHARS,
                                        "relation": "thread_neighbor", "excerptOnly": False})
    rendered, selected = render_memory((item, notice(1)))
    assert selected == {} and rows(rendered)[0]["omittedSourceCount"] == 1
    assert len(rows(rendered)) == 1 and rows(rendered)[0]["coverage"] == "partial"


def test_utf8_byte_budget_is_checked_independently_of_character_budget(monkeypatch):
    ascii_item = plain_live(0, "a" * 1200)
    baseline, _ = render_memory((ascii_item, notice(1)))
    byte_limit = len(baseline.split(":\n", 1)[1].encode("utf-8")) + 300
    monkeypatch.setattr(presentation, "LIVE_RENDER_MAX_BYTES", byte_limit)
    assert len(render_memory((ascii_item, notice(1)))[1]) == 1
    unicode_item = plain_live(0, "가" * 1200)
    rendered, selected = render_memory((unicode_item, notice(1)))
    assert selected == {} and rows(rendered)[0]["omittedSourceCount"] == 1
    assert len(rendered.split(":\n", 1)[1].encode("utf-8")) <= byte_limit
