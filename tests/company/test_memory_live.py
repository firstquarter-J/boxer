from __future__ import annotations

from copy import deepcopy
import json

import pytest

from boxer_company.memory.contracts import MemoryError, source_identity, source_revision
from boxer_company.memory.live import normalize_live_sources
from boxer_company.memory.presentation import render_memory


TENANT = "T19PA3EPL"
HUB = "C068FVD5V7Y"
CUTOFF = "1800000010.000001"


def source(text="오늘 안내문 검토를 맡았어", *, channel="CPRIVATE", author="UDD",
           thread="1800000000.000001", message="1800000000.000001", edited="", **overrides):
    return {
        "channelId": channel, "channelKind": "private_channel", "threadTs": thread,
        "messageTs": message, "revisionHash": source_revision(text, edited), "authorId": author,
        "text": text, "permalink": f"https://team.slack.com/archives/{channel}/p{message.replace('.', '')}",
        "editedAt": edited, **overrides,
    }


def normalize(sources, **overrides):
    return normalize_live_sources(sources, **{
        "tenant_id": TENANT, "destination_channel_id": HUB, "before_ts": CUTOFF,
        "author_ids": ["UDD"], **overrides,
    })


def test_live_source_becomes_citable_temporary_item_without_trusting_source_id():
    original = source(sourceId="forged", tenantId=TENANT)
    before = deepcopy(original)
    (item,) = normalize([original])
    expected_id = source_identity(TENANT, "CPRIVATE", original["messageTs"], original["revisionHash"])
    assert original == before
    assert item["memoryId"] == "live_" + expected_id
    assert item["kind"] == "utterance" and item["category"] == "statement"
    assert item["evidenceType"] == "direct_statement" and item["status"] == "active"
    assert item["subjectUserId"] == "UDD" and item["version"] == 1
    assert item["text"] == original["text"] and item["excerptOnly"] is False
    assert item["liveSource"] is True and item["deletionEpoch"] == 0
    assert item["sources"][0]["sourceId"] == expected_id
    assert item["sources"][0]["authorKind"] == "human"
    assert item["updatedAt"] == "2027-01-15T08:00:00.000001Z"
    rendered, selected = render_memory([item])
    assert original["text"] in rendered and selected[item["memoryId"]] == item
    assert "2027-01-15T17:00:00.000001+09:00" in rendered
    assert original["permalink"] not in rendered


def test_sources_sort_by_spoken_timestamp_and_exact_duplicates_collapse():
    first = source(message="1800000001.000001")
    recent = source("검토를 마쳤어", message="1800000009.000002")
    middle = source("오늘 회의에서 설명할게", message="1800000009.000001")
    items = normalize([first, recent, first, middle])
    assert [item["sources"][0]["messageTs"] for item in items] == [
        "1800000009.000002", "1800000009.000001", "1800000001.000001",
    ]


def test_same_message_with_two_revisions_fails_closed():
    first = source()
    second = source("안내문 검토를 마쳤어", edited="1800000001.000001")
    with pytest.raises(MemoryError, match="memory_live_source_revision_conflict"):
        normalize([first, second])


def test_same_source_with_conflicting_thread_metadata_fails_closed():
    original = source(thread="1799999999.000001")
    same_message = {**original, "threadTs": "1800000000.000001"}
    with pytest.raises(MemoryError, match="memory_live_source_conflict"):
        normalize([original, same_message])


@pytest.mark.parametrize("kind", ["channel", "public_channel", "dm", "group_dm", "", "private"])
def test_other_source_kinds_are_forbidden_even_if_source_is_destination(kind):
    with pytest.raises(MemoryError, match="memory_live_source"):
        normalize([source(channel=HUB, channelKind=kind)])


@pytest.mark.parametrize(("tenant", "destination"), [
    (TENANT, "CORDINARY"), ("OTHER_TENANT", HUB), ("", HUB), (TENANT, ""),
])
def test_only_exact_authorized_tenant_and_destination_can_receive_live_sources(tenant, destination):
    with pytest.raises(MemoryError, match="memory_live_destination_forbidden"):
        normalize([source()], tenant_id=tenant, destination_channel_id=destination)


@pytest.mark.parametrize("source_tenant", ["OTHER_TENANT", "", None])
def test_explicit_mismatched_source_tenant_fails_closed(source_tenant):
    with pytest.raises(MemoryError, match="memory_live_source_scope_invalid"):
        normalize([source(tenantId=source_tenant)])


def test_author_filter_is_enforced_and_empty_filter_allows_other_humans():
    with pytest.raises(MemoryError, match="memory_live_source_author_invalid"):
        normalize([source(author="UOTHER")])
    assert normalize([source(author="UOTHER")], author_ids=[])[0]["subjectUserId"] == "UOTHER"


@pytest.mark.parametrize("authors", ["UDD", [None], [""], ["U" * 129], None])
def test_malformed_author_filters_fail_with_memory_error(authors):
    with pytest.raises(MemoryError, match="memory_live_authors_invalid"):
        normalize([source()], author_ids=authors)


@pytest.mark.parametrize("field", [
    "channelId", "channelKind", "threadTs", "messageTs", "revisionHash", "authorId", "text", "permalink",
])
def test_missing_required_fields_fail_closed(field):
    original = source()
    del original[field]
    with pytest.raises(MemoryError, match="memory_live_source_invalid"):
        normalize([original])


@pytest.mark.parametrize(("field", "value"), [
    ("messageTs", 1800000000), ("text", ["안내문"]), ("editedAt", None),
    ("permalink", None), ("authorId", ""), ("channelId", ""),
])
def test_wrong_field_types_and_empty_identifiers_fail_closed(field, value):
    with pytest.raises(MemoryError, match="memory_live_source_invalid"):
        normalize([source(**{field: value})])


@pytest.mark.parametrize("field", ["threadTs", "messageTs", "editedAt"])
@pytest.mark.parametrize("timestamp", ["NaN", "1800000000", "1800000000.1234567", "999999999999.000001"])
def test_malformed_or_unrepresentable_source_timestamps_fail_closed(field, timestamp):
    with pytest.raises(MemoryError, match="memory_live_timestamp_invalid"):
        normalize([source(**{field: timestamp})])


@pytest.mark.parametrize("cutoff", ["NaN", "", "1800000000", "999999999999.000001"])
def test_malformed_cutoff_fails_closed(cutoff):
    with pytest.raises(MemoryError, match="memory_live_timestamp_invalid"):
        normalize([], before_ts=cutoff)


def test_source_after_question_cutoff_and_before_thread_are_rejected():
    with pytest.raises(MemoryError, match="source_after_capture_cutoff"):
        normalize([source(message="1800000010.000002")])
    with pytest.raises(MemoryError, match="source_before_thread"):
        normalize([source(thread="1800000001.000001")])


def test_edit_at_cutoff_is_accepted_but_later_edit_of_old_message_is_rejected():
    at_cutoff = source(edited=CUTOFF)
    assert normalize([at_cutoff])[0]["sources"][0]["revisionHash"] == at_cutoff["revisionHash"]
    with pytest.raises(MemoryError, match="source_edited_after_capture_cutoff"):
        normalize([source(edited="1800000010.000002")])


def test_revision_and_permalink_are_checked_before_a_source_is_accepted():
    with pytest.raises(MemoryError, match="source_revision_mismatch"):
        normalize([source(revisionHash="0" * 64)])
    with pytest.raises(MemoryError, match="source_permalink_invalid"):
        normalize([source(permalink="https://evil.example/archives/CPRIVATE/p1800000000000001")])
    with pytest.raises(MemoryError, match="source_permalink_invalid"):
        normalize([source(permalink="https://team.slack.com/archives/COTHER/p1800000000000001")])


def test_sensitive_and_operation_sources_are_skipped_without_returning_the_payload():
    sensitive = source("password=never-return-this-secret")
    operation = source("마미박스 무료병원 자동 업데이트 켜줘", message="1800000001.000001")
    safe = source("오늘 설명을 마쳤어", message="1800000002.000001")
    items = normalize([sensitive, operation, safe])
    assert len(items) == 1 and items[0]["text"] == safe["text"]
    assert "never-return" not in json.dumps(items)


def test_text_is_excerpted_only_after_full_revision_and_safety_validation():
    text = "안내문 검토 의견을 정리했어. " * 90
    (item,) = normalize([source(text)])
    assert len(item["text"]) == 1000 and item["excerptOnly"] is True
    assert item["sources"][0]["revisionHash"] == source_revision(text)
    # 비밀값이 표시 발췌 뒤에 있어도 전체 원문 안전 검사에서 제외한다.
    assert normalize([source(text + "password=secret-at-the-end")]) == ()


def test_source_count_and_total_character_budgets_are_enforced():
    eight = [source("가" * 1000, message=f"180000000{index}.000001") for index in range(8)]
    assert len(normalize(eight)) == 8
    with pytest.raises(MemoryError, match="memory_live_sources_invalid"):
        normalize([*eight, source(message="1800000008.000001")])
    with pytest.raises(MemoryError, match="memory_live_sources_too_large"):
        normalize([source("가" * 8001)])


@pytest.mark.parametrize("sources", [None, "원문", [None], [{"text": "원문"}]])
def test_malformed_source_collections_fail_with_memory_error(sources):
    with pytest.raises(MemoryError, match="memory_live_source"):
        normalize(sources)
