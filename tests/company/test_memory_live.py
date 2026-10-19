from __future__ import annotations

from copy import deepcopy
import json

import pytest

from boxer_company.memory.contracts import MemoryError, source_identity, source_revision
from boxer_company.memory.access import can_query_private_live_sources
from boxer_company.memory.live import normalize_live_sources
from boxer_company.memory.live_query import is_within_live_query_period, resolve_live_query_period
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


def context(anchor, text="그 문구는 예시로 든 농담이야", *, message=None, relation="thread_parent", **overrides):
    # 대상이 아닌 작성자의 발언을 원문의 실제 스레드와 anchor에 연결한다.
    return source(text, channel=anchor["channelId"], author="UOTHER", thread=anchor["threadTs"],
        message=message or anchor["threadTs"], anchorMessageTs=anchor["messageTs"], relation=relation, **overrides)


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


@pytest.mark.parametrize(("tenant", "destination", "kind", "allowed"), [
    (TENANT, HUB, None, True), (TENANT, "CORDINARY", "private_channel", True),
    ("OTHER_TENANT", "CORDINARY", "private_channel", True),
    (TENANT, "CORDINARY", None, False), (TENANT, "CORDINARY", "channel", False),
    (TENANT, "CORDINARY", "dm", False), (TENANT, "CORDINARY", "group_dm", False),
    ("", "CORDINARY", "private_channel", False), (TENANT, "", "private_channel", False),
])
def test_live_query_scope_needs_confirmed_private_kind_outside_fixed_hub(tenant, destination, kind, allowed):
    # 다른 tenant도 자기 원채널만 읽을 수 있고 고정 허브의 교차 예외는 물려받지 않는다.
    assert can_query_private_live_sources(tenant, destination, kind) is allowed


@pytest.mark.parametrize("tenant", [TENANT, "OTHER_TENANT"])
def test_confirmed_private_destination_accepts_only_its_own_channel_sources(tenant):
    kwargs = {"tenant_id": tenant, "destination_channel_id": "CORDINARY",
              "destination_channel_kind": "private_channel"}
    original = source(channel="CORDINARY", tenantId=tenant)
    (item,) = normalize([original], **kwargs)
    assert item["sources"][0]["channelId"] == "CORDINARY"
    with pytest.raises(MemoryError, match="memory_live_source_scope_invalid"):
        normalize([source(tenantId=tenant)], **kwargs)


@pytest.mark.parametrize("kind", [None, "channel", "dm", "group_dm", "private"])
def test_ordinary_destination_cannot_enable_live_read_with_source_flags(kind):
    # source의 private 표시만으로 목적지의 확인되지 않은 종류를 승격하지 않는다.
    with pytest.raises(MemoryError, match="memory_live_destination_forbidden"):
        normalize([source(channel="CORDINARY", allowCrossChannel=True)],
                  destination_channel_id="CORDINARY", destination_channel_kind=kind)


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


@pytest.mark.parametrize("question,kind,start,end", [
    ("디디가 오늘 한 말 요약", "today", "1792335600.000000", "1792422000.000000"),
    ("오늘은 무슨 대화 했어?", "today", "1792335600.000000", "1792422000.000000"),
    ("어제의 발언은?", "yesterday", "1792249200.000000", "1792335600.000000"),
    ("디디는 어제 뭐라고 했어?", "yesterday", "1792249200.000000", "1792335600.000000"),
])
def test_relative_day_uses_question_kst_date_instead_of_process_clock(question, kind, start, end):
    # UTC로는 18일인 질문도 KST 19일을 기준으로 해석한다.
    cutoff = "1792350000.123456789"
    period = resolve_live_query_period(question, cutoff)
    assert period == {
        "kind": kind, "referenceDateKst": "2026-10-19", "startTs": start, "endTs": end,
        "startAtKst": "2026-10-" + ("19" if kind == "today" else "18") + "T00:00:00+09:00",
        "endAtKst": "2026-10-" + ("20" if kind == "today" else "19") + "T00:00:00+09:00",
        "cutoffTs": cutoff,
    }


@pytest.mark.parametrize("question", ["최근 한 말은?", "지난주 발언", "2026-10-18 대화", "오늘날 사회", "어제오늘 근황", "오늘과 어제 비교", "오늘부터의 일정"])
def test_only_unambiguous_relative_day_tokens_create_a_period(question):
    assert resolve_live_query_period(question, "1792350000.000001") is None


@pytest.mark.parametrize("cutoff", ["", "NaN", "1792350000", "99999999999999999999.000000001", None])
def test_unusable_question_time_does_not_guess_a_date(cutoff):
    assert resolve_live_query_period("오늘 한 말은?", cutoff) is None


@pytest.mark.parametrize("timestamp,expected", [
    ("1792335599.999999999", False), ("1792335600.000000000", True),
    ("1792350000.123456789", True), ("1792350000.123456790", False),
    ("1792422000.000000000", False), ("NaN", False), (None, False),
])
def test_today_includes_midnight_and_question_time_with_exact_fraction(timestamp, expected):
    period = resolve_live_query_period("오늘 한 말은?", "1792350000.123456789")
    assert is_within_live_query_period(timestamp, period) is expected


def test_yesterday_excludes_the_following_midnight_even_before_question_cutoff():
    period = resolve_live_query_period("어제 한 말은?", "1792350000.000001")
    assert is_within_live_query_period("1792249200.000000", period)
    assert is_within_live_query_period("1792335599.999999999", period)
    assert not is_within_live_query_period("1792335600.000000", period)


@pytest.mark.parametrize("question,accepted", [
    ("디디가 오늘 한 말 요약", ["1792350000.000001", "1792335600.000000"]),
    ("디디가 어제 한 말 요약", ["1792335599.999999", "1792249200.000000"]),
])
def test_api_normalization_enforces_period_even_for_adapter_sources_from_other_days(question, accepted):
    # 부모가 더 오래돼도 기간 내 댓글은 허용하고 질문의 날짜 밖 원문은 최종 경계에서 뺀다.
    stamps = ["1792249199.999999", "1792249200.000000", "1792335599.999999",
              "1792335600.000000", "1792350000.000001"]
    items = normalize([source(message=stamp, thread="1792200000.000001") for stamp in stamps],
                      question=question, before_ts="1792350000.000001")
    assert [item["sources"][0]["messageTs"] for item in items] == accepted


def test_period_filter_does_not_bypass_existing_future_source_and_revision_checks():
    with pytest.raises(MemoryError, match="source_after_capture_cutoff"):
        normalize([source(message="1792350000.000002", thread="1792200000.000001")],
                  question="오늘 한 말은?", before_ts="1792350000.000001")
    with pytest.raises(MemoryError, match="source_revision_mismatch"):
        normalize([source(message="1792249200.000000", thread="1792200000.000001", revisionHash="0" * 64)],
                  question="오늘 한 말은?", before_ts="1792350000.000001")


def test_context_preserves_other_author_and_previous_day_parent_without_weakening_anchor_period():
    # 오늘 발언만 대상이지만 그 발언의 전날 부모는 다른 작성자의 문맥으로 별도 보존한다.
    anchor = source(message="1792350000.000001", thread="1792249200.000001")
    parent = context(anchor, sourceId="forged-context-source-id")
    (item,) = normalize([anchor], context_messages=[parent, parent],
        question="디디가 오늘 한 말 요약", before_ts="1792350000.000001")
    assert item["subjectUserId"] == "UDD" and item["sources"][0]["relation"] == "supports"
    assert item["sources"][0]["authorId"] == "UDD"
    assert item["contextCoverage"] == "partial" and len(item["contextMessages"]) == 1
    row = item["contextMessages"][0]
    assert row["authorId"] == "UOTHER" and row["messageTs"] == "1792249200.000001"
    assert row["relation"] == "thread_parent" and row["sourceId"] != "forged-context-source-id"
    assert item["sources"][1]["sourceId"] == row["sourceId"]
    assert item["sources"][1]["relation"] == "context" and item["sources"][1]["authorId"] == "UOTHER"


@pytest.mark.parametrize("relation,message,thread", [
    ("thread_neighbor", "1800000007.000001", "1800000000.000001"),
    ("channel_neighbor", "1800000905.000001", "1800000905.000001"),
    ("channel_neighbor", "1799999105.000001", "1799999105.000001"),
])
def test_valid_thread_or_nearby_channel_context_keeps_its_actual_thread(relation, message, thread):
    anchor = source(message="1800000005.000001")
    row = context(anchor, message=message, relation=relation, threadTs=thread)
    (item,) = normalize([anchor], context_messages=[row], before_ts="1800001000.000001")
    assert item["contextMessages"][0]["threadTs"] == thread
    assert item["contextMessages"][0]["relation"] == relation


@pytest.mark.parametrize("change", [
    {"anchorMessageTs": "1800000006.000001"}, {"channelId": "COTHER"},
    {"tenantId": "OTHER"}, {"channelKind": "channel"},
    {"relation": "thread_parent", "messageTs": "1800000001.000001"},
    {"relation": "thread_neighbor", "threadTs": "1799999999.000001"},
    {"relation": "channel_neighbor", "messageTs": "1800000905.000002", "threadTs": "1800000905.000002"},
    {"relation": "made_up"}, {"relation": []}, {"anchorMessageTs": None},
])
def test_orphan_cross_channel_and_invalid_context_relation_fail_closed(change):
    anchor = source(message="1800000005.000001")
    row = context(anchor)
    row.update(change)
    with pytest.raises(MemoryError):
        normalize([anchor], context_messages=[row], before_ts="1800001000.000001")


@pytest.mark.parametrize("changes,error", [
    ({"messageTs": "1800000010.000002", "threadTs": "1800000010.000002", "relation": "channel_neighbor"},
     "source_after_capture_cutoff"),
    ({"editedAt": "1800000010.000002"}, "source_edited_after_capture_cutoff"),
    ({"revisionHash": "0" * 64}, "source_revision_mismatch"),
    ({"permalink": "https://invalid.example/source"}, "source_permalink_invalid"),
])
def test_context_date_author_exception_keeps_cutoff_edit_revision_and_permalink_checks(changes, error):
    anchor = source(message="1800000005.000001")
    row = context(anchor)
    row.update(changes)
    # 새 timestamp의 링크는 맞춰 다른 검증보다 실제 cutoff 검사를 확인한다.
    if "messageTs" in changes:
        row["permalink"] = f"https://team.slack.com/archives/CPRIVATE/p{row['messageTs'].replace('.', '')}"
    with pytest.raises(MemoryError, match=error):
        normalize([anchor], context_messages=[row])


def test_sensitive_context_is_skipped_and_no_context_is_reported_as_unavailable():
    anchor = source(message="1800000005.000001")
    unsafe = context(anchor, "가" * 1200 + " password=never-render-context-secret")
    (item,) = normalize([anchor], context_messages=[unsafe])
    assert item["contextMessages"] == [] and item["contextCoverage"] == "unavailable"
    assert len(item["sources"]) == 1 and "never-render" not in json.dumps(item)


def test_context_conflicts_cannot_mix_two_revisions_or_authors_of_one_message():
    anchor = source(message="1800000005.000001")
    original = context(anchor)
    for changed in (context(anchor, "그건 실제 결정이야"), {**original, "authorId": "UANOTHER"},
                    context(anchor, "password=unsafe-newer-revision")):
        with pytest.raises(MemoryError, match="memory_live_source_(revision_conflict|conflict)"):
            normalize([anchor], context_messages=[original, changed])


def test_context_counts_and_raw_character_budget_include_all_linked_messages():
    anchor = source(message="1800000005.000001")
    rows = [context(anchor, message=f"180000000{n}.000001", relation="thread_neighbor") for n in range(5)]
    assert len(normalize([anchor], context_messages=rows)[0]["contextMessages"]) == 5
    with pytest.raises(MemoryError, match="memory_live_contexts_too_many"):
        normalize([anchor], context_messages=[*rows, rows[0]])
    anchors = [source(message=f"180000000{n}.000001") for n in range(2, 10)]
    with pytest.raises(MemoryError, match="memory_live_contexts_invalid"):
        normalize(anchors, context_messages=rows)
    with pytest.raises(MemoryError, match="memory_live_sources_too_large"):
        normalize([source("가" * 4000, message="1800000005.000001")],
                  context_messages=[context(anchor, "나" * 4001)])


@pytest.mark.parametrize("contexts", [None, "문맥", [None], [{}]])
def test_malformed_context_collections_fail_with_memory_error(contexts):
    with pytest.raises(MemoryError, match="memory_live_context"):
        normalize([source()], context_messages=contexts)
