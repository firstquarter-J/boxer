"""저장된 발췌만 시간순과 주제별로 표시하고 손상된 목록은 노출하지 않는다."""

from copy import deepcopy
from datetime import UTC, datetime

import pytest

from boxer_company.memory.conversation_count import (
    PROFANITY_CLASSIFIER_VERSION,
    PROFANITY_EXPRESSION_ALLOWLIST,
    classify_profanity_message,
    format_conversation_count_answer,
    parse_conversation_count_query,
)
from boxer_company.memory.conversation_history import (
    MAX_HISTORY_ANSWER_CHARS,
    format_conversation_history_answer,
)
from boxer_company.memory.raw_presentation import raw_display_text


def stamp(text):
    value = datetime.fromisoformat(text).astimezone(UTC)
    return f"{int(value.timestamp())}.000001"


NOW = stamp("2026-10-28T12:00:00+09:00")
PERIOD = parse_conversation_count_query("올해 욕 횟수", NOW)["period"]


def message(text="씨발 빌드가 또 실패했어", *, when="2026-10-01T10:00:00+09:00", topic="빌드 확인"):
    return {"messageTs": stamp(when), "excerpt": text,
            "matchedExpressions": classify_profanity_message(text)["matchedExpressions"], "topic": topic}


def result(messages=None, **overrides):
    rows = [message()] if messages is None else messages
    expressions = sorted({value for row in rows for value in row["matchedExpressions"]})
    value = {"status": "stored", "scope": "stored_excerpts", "classifierVersion": PROFANITY_CLASSIFIER_VERSION,
             "period": dict(PERIOD), "authorDisplayName": "DD", "matchedMessageCount": len(rows),
             "ambiguousMessageCount": 0, "scannedMessageCount": len(rows),
             "unavailableExcerptMessageCount": 0, "eligibleStoredMessageCount": len(rows),
             "matchedExpressions": expressions, "matchedMessages": rows, "messageLimit": 20,
             "messagesTruncated": False}
    value.update(overrides)
    return value


def test_raw_history_displays_quoted_operation_and_masks_values_without_rejecting_the_whole_message():
    text = raw_display_text("씨발 장비 로그 조회 010-0000-0000")
    payload = result([message(text, topic=None)], storedRawMessageCount=1)
    answer = format_conversation_history_answer(payload)
    assert "장비 로그 조회" in answer and "개인정보 가림" in answer
    assert "synthetic-only" not in answer and "010-0000-0000" not in answer
    assert "목록 근거의 확인이 필요" not in answer


@pytest.mark.parametrize("raw_count", [True, "1", -1, 2])
def test_raw_history_requires_valid_raw_count_before_using_raw_display_policy(raw_count):
    assert "목록 근거의 확인이 필요" in format_conversation_history_answer(result(storedRawMessageCount=raw_count))


def test_raw_history_refuses_unmasked_values_even_when_result_claims_raw_provenance():
    payload = result([message("씨발 전화해 010-0000-0000")], storedRawMessageCount=1)
    answer = format_conversation_history_answer(payload)
    assert "010-0000-0000" not in answer and "목록 근거의 확인이 필요" in answer


def test_chronology_uses_source_timestamp_in_kst_then_topics_reference_stable_numbers():
    rows = [message("존나 점검이 오래 걸려", when="2026-10-02T10:00:00+09:00", topic=None),
            message("씨발 빌드가 실패했어", when="2026-10-01T10:00:00+09:00"),
            message("지랄 같은 빌드야", when="2026-10-01T11:00:00+09:00")]
    payload = result(rows)
    answer = format_conversation_history_answer(payload)
    assert answer.startswith(format_conversation_count_answer(payload))
    assert answer.index("1. 2026-10-01 10:00:00 KST") < answer.index("2. 2026-10-01 11:00:00 KST")
    assert answer.index("2. 2026-10-01 11:00:00 KST") < answer.index("3. 2026-10-02 10:00:00 KST")
    assert "**날짜와 시간순 (KST)**" in answer and "**저장된 주제별**" in answer
    assert "- 빌드 확인: 1번, 2번" in answer and "- 주제 확인 어려움: 3번" in answer
    assert "저장된 짧은 발췌만" in answer and "원문 전문이나 전후 대화는 확인할 수 없어" in answer
    assert "빌드가 실패했어" in answer and "점검이 오래 걸려" in answer


def test_equal_precise_timestamps_preserve_store_order_without_dropping_cross_channel_messages():
    # HTTP용 내부 목록에는 채널 ID가 없으므로 같은 시각을 중복이라고 삭제하지 않는다.
    first, second = message("씨발 첫 채널 발췌"), message("존나 둘째 채널 발췌")
    first["messageTs"], second["messageTs"] = "1790816400.000000009", "1790816400.000000001"
    answer = format_conversation_history_answer(result([first, second]))
    assert answer.index("둘째 채널") < answer.index("첫 채널")
    second["messageTs"] = first["messageTs"]
    answer = format_conversation_history_answer(result([first, second]))
    assert answer.index("첫 채널") < answer.index("둘째 채널")


def test_no_matches_does_not_invent_examples_or_topics_and_missing_excerpts_stay_unknown():
    answer = format_conversation_history_answer(result([], scannedMessageCount=152,
        unavailableExcerptMessageCount=3216, eligibleStoredMessageCount=3368))
    assert "저장된 발췌 152건" in answer and "3216건" in answer
    assert "시간순으로 보여줄 욕 발언은 저장된 발췌에서 확인되지 않았어" in answer
    assert "전체 발언이 0회라는 뜻은 아니야" in answer and "발언 내용과 주제는 확인할 수 없어" in answer
    assert "**날짜와 시간순 (KST)**" not in answer and "**저장된 주제별**" not in answer
    assert "빌드" not in answer and "주제 확인 어려움" not in answer


def test_matched_but_unsafe_or_unavailable_display_excerpts_can_all_be_omitted():
    payload = result([], matchedMessageCount=2, matchedExpressions=["씨발"], scannedMessageCount=2,
                     eligibleStoredMessageCount=2, messagesTruncated=True)
    answer = format_conversation_history_answer(payload)
    assert "확인된 2건 중 표시 가능한 발췌 0건" in answer
    assert "표시 상한 또는 발췌 표시 조건" in answer and "시간순으로 보여줄 수 있는 발췌" in answer
    assert "**저장된 주제별**" not in answer


def test_partial_list_accepts_safe_subset_and_does_not_claim_only_limit_caused_omission():
    payload = result([message()], matchedMessageCount=3, matchedExpressions=["씨발", "존나"],
                     scannedMessageCount=3, eligibleStoredMessageCount=3, messagesTruncated=True)
    answer = format_conversation_history_answer(payload)
    assert "확인된 3건 중 표시 가능한 발췌 1건" in answer and "나머지 2건" in answer
    assert "표시 상한 또는 발췌 표시 조건" in answer


def test_twenty_long_excerpts_and_topics_stay_bounded_and_excerpt_truncation_is_explicit():
    rows = [message("씨발 " + "상황 설명 " * 140, topic=f"저장 주제 {number} " + "가" * 130)
            for number in range(20)]
    answer = format_conversation_history_answer(result(rows))
    assert len(answer) <= MAX_HISTORY_ANSWER_CHARS
    assert answer.count("[발췌 일부 생략]") == 20 and "20. 2026-10-01" in answer


def test_matching_expression_after_display_window_is_still_visible_in_its_own_entry():
    # 저장 발췌 뒤쪽의 일치를 앞부분 잘림 때문에 표시에서 잃지 않는다.
    answer = format_conversation_history_answer(result([message("상황 설명 " * 140 + "씨발")]))
    assert "   확인된 표현: `씨발`" in answer and "[발췌 일부 생략]" in answer


def test_maximum_safe_expression_sets_and_topic_lengths_still_fit_the_answer_budget():
    text = " ".join(sorted(PROFANITY_EXPRESSION_ALLOWLIST)) + " 상황 설명" * 100
    rows = [message(text, topic=f"{number:02d}" + "가" * 158) for number in range(20)]
    answer = format_conversation_history_answer(result(rows))
    assert len(answer) <= MAX_HISTORY_ANSWER_CHARS and "20. 2026-10-01" in answer
    assert answer.count("   확인된 표현:") == 20


def test_display_disarms_mentions_urls_and_markdown_without_adding_source_links():
    text = "씨발 **강조** <@UOTHER> <!channel> @here [보기](https://example.com/a) example.com `명령`"
    answer = format_conversation_history_answer(result([message(text, topic="**빌드** <@UOTHER>")]))
    assert "씨발" in answer and "멘션 생략" in answer and "링크 생략" in answer
    for active in ("<@", "<!", "@here", "https://", "example.com", "**강조**", "`명령`", "[보기]("):
        assert active not in answer
    assert "＊＊강조＊＊" in answer and "＠here" in answer and "example．com" in answer


@pytest.mark.parametrize("change", [
    {"matchedMessages": None}, {"matchedMessages": {}}, {"messageLimit": 0}, {"messageLimit": 21},
    {"messageLimit": True}, {"messagesTruncated": "false"}, {"messagesTruncated": True},
    {"matchedMessageCount": True}, {"matchedMessageCount": -1}, {"scannedMessageCount": 0},
    {"eligibleStoredMessageCount": 2}, {"matchedExpressions": []}, {"matchedExpressions": ["씨발", "씨발"]},
    {"matchedExpressions": ["없는표현"]}, {"classifierVersion": "other"}, {"scope": "all_slack_history"},
    {"matchedMessageCount": 10**100},
])
def test_invalid_summary_or_detail_declaration_never_leaks_an_excerpt(change):
    payload = result([message("씨발 노출되면안되는발췌")])
    payload.update(change)
    answer = format_conversation_history_answer(payload)
    assert "노출되면안되는발췌" not in answer and "발언 목록을 확인할 수 없어" in answer


@pytest.mark.parametrize("field,value", [
    ("messageTs", "invalid"), ("messageTs", "-1.000000"),
    ("messageTs", "1767193199.999999"), ("messageTs", "1798729200.000000"),
    ("messageTs", "99999999999999999999.000000"), ("excerpt", ""), ("excerpt", "씨발 " + "가" * 1000),
    ("excerpt", "씨발\x00본문"), ("excerpt", "씨발 password=secret"),
    ("excerpt", "씨발 환자이름은 비밀이야"), ("excerpt", "인용에 실제 표현이 없어"),
    ("excerpt", '"씨발"이라는 단어의 뜻'), ("matchedExpressions", []),
    ("matchedExpressions", ["없는표현"]), ("matchedExpressions", ["씨발", "존나"]),
    ("matchedExpressions", ["씨발", "씨발"]), ("topic", {}), ("topic", ""),
    ("topic", "가" * 161), ("topic", "password=secret"),
])
def test_malformed_or_unsafe_message_is_rejected_as_one_result(field, value):
    payload = result([message("씨발 노출되면안되는발췌")])
    payload["matchedMessages"][0][field] = value
    answer = format_conversation_history_answer(payload)
    assert "노출되면안되는발췌" not in answer and "발언 목록을 확인할 수 없어" in answer
    assert "secret" not in answer


def test_message_after_original_question_cutoff_is_not_displayed():
    payload = result([message(when="2026-10-28T12:00:01+09:00")])
    assert "발언 목록을 확인할 수 없어" in format_conversation_history_answer(payload)


def test_inconsistent_zero_matches_nonempty_rows_and_oversized_list_are_rejected():
    assert "발언 목록을 확인할 수 없어" in format_conversation_history_answer(result(
        matchedMessageCount=0, matchedExpressions=[]))
    assert "발언 목록을 확인할 수 없어" in format_conversation_history_answer(result([message()] * 21))


@pytest.mark.parametrize("payload", [
    {"status": "unsupported_period"}, {"status": "failed"},
    {"status": "stored", "periodError": "invalid"},
    result(period={}),
])
def test_existing_failure_and_period_guidance_is_preserved(payload):
    assert format_conversation_history_answer(payload) == format_conversation_count_answer(payload)


def test_formatter_does_not_mutate_source_result_or_require_any_provider():
    payload = result()
    before = deepcopy(payload)
    assert "날짜와 시간순 (KST)" in format_conversation_history_answer(payload)
    assert payload == before
