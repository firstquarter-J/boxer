"""집계의 단위와 불확실성을 고정하고 의미 추정 없이 사전 표현만 센다."""

import unicodedata
from datetime import UTC, datetime

import pytest

from boxer_company.memory.conversation_count import (
    CONVERSATION_COUNT_KIND,
    PROFANITY_CLASSIFIER_VERSION,
    PROFANITY_EXPRESSION_ALLOWLIST,
    classify_profanity_message,
    format_conversation_count_answer,
    parse_conversation_count_query,
)
from boxer_company.memory.live_query import (
    STORED_HISTORY_START_TS,
    is_supported_stored_period,
    is_within_live_query_period,
)

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def stamp(value: str) -> str:
    delta = datetime.fromisoformat(value).astimezone(UTC) - EPOCH
    return str(delta.days * 86400 + delta.seconds) + ".123456789"


NOW = stamp("2026-10-23T01:02:03+09:00")


@pytest.mark.parametrize("text,term,expression", [
    ("개병신세들", "byeongsin", "개병신"), ("병신들", "byeongsin", "병신"),
    ("ㅈㄴ맡기고", "jonna", "ㅈㄴ"), ("시바...", "ssibal", "시바"),
    ("씨발 왜 이래", "ssibal", "씨발"), ("시발 또 고장났네", "ssibal", "시발"),
    ("존나 오래 걸린다", "jonna", "존나"), ("개새끼야", "gaesaekki", "개새끼"),
    ("지랄하지 마", "jiral", "지랄"), ("좆같다", "jot", "좆"),
])
def test_explicit_lexicon_examples_are_message_matches(text, term, expression):
    assert classify_profanity_message(text) == {
        "classifierVersion": PROFANITY_CLASSIFIER_VERSION, "classification": "matched",
        "matchedTerms": [term], "matchedExpressions": [expression], "reasons": [],
    }


@pytest.mark.parametrize("expression", sorted(PROFANITY_EXPRESSION_ALLOWLIST))
def test_every_allowed_display_expression_is_a_real_match_after_normalization(expression):
    # 표시 allowlist가 사전을 넓히거나 정규화된 초성을 그대로 노출하지 않도록 계약을 고정한다.
    classified = classify_profanity_message(unicodedata.normalize("NFKC", expression))
    assert classified["classification"] == "matched"
    assert classified["matchedExpressions"] == [expression]


@pytest.mark.parametrize("text", [
    "개꿀딱", "개나소나", "마크만죽으면됨", "오늘 너무 화난다", "욕했어", "욕심이 많아",
    "시발점이 중요해", "시발역에서 탔어", "시바견이 귀엽다", "시바이누", "새끼 고양이",
    "mybyeongsin", "병원 신축", "", None,
])
def test_general_roots_nonprofane_words_and_no_dictionary_match_are_not_counted(text):
    classified = classify_profanity_message(text)
    assert classified["classification"] == "not_matched"
    assert classified["matchedExpressions"] == []


@pytest.mark.parametrize("text,reason", [
    ('그가 "병신"이라고 했어', "quoted_or_code"),
    ("‘시바’라고 들었어", "quoted_or_code"),
    ("「씨발」이라고 적힌 예시", "quoted_or_code"),
    ("『병신』이라는 표현", "quoted_or_code"),
    ("> 개병신세들\n이라는 발언이 있었어", "quoted_or_code"),
    ("&gt; 병신들", "quoted_or_code"),
    ("`ㅈㄴ맡기고` 예시", "quoted_or_code"),
    ("```python\nword = '씨발'\n```", "quoted_or_code"),
    ("```\n병신", "quoted_or_code"),
    ("~~~\n시바\n~~~", "quoted_or_code"),
    ("병신이라고 했대", "mention_or_explanation"),
    ("시발이라는 단어 뜻이 뭐야", "mention_or_explanation"),
    ("욕설 표현 시바는 사용하지 말아줘", "mention_or_explanation"),
    ("욕설 병신을 차단하는 정책을 만들어줘", "mention_or_explanation"),
    ("씨발을 쓰지 말아줘", "mention_or_explanation"),
    ("병신은 금지해줘", "mention_or_explanation"),
    ("시바라고 답해줘", "mention_or_explanation"),
    ("씨발 말해줘", "mention_or_explanation"),
    ("2016년은 병신년이야", "mention_or_explanation"),
    ("병신은 육십갑자의 간지야", "mention_or_explanation"),
    ("힌두교의 시바는 파괴의 신이야", "mention_or_explanation"),
])
def test_quotation_code_explanation_and_policy_are_separated_from_clear_matches(text, reason):
    result = classify_profanity_message(text)
    assert result["classification"] == "ambiguous"
    assert result["matchedTerms"] == []
    assert result["matchedExpressions"] == []
    assert reason in result["reasons"]


def test_repeated_terms_and_mixed_quotation_still_make_one_message_not_occurrence_count():
    result = classify_profanity_message('"병신"이라고 했고, 나는 존나 짜증나. 씨발 씨발')
    assert result["classification"] == "matched"
    assert result["matchedTerms"] == ["jonna", "ssibal"]
    assert result["matchedExpressions"] == ["씨발", "존나"]
    assert result["reasons"] == ["quoted_or_code"]
    assert "count" not in result  # 출현 횟수를 메시지 수와 혼동할 필드를 반환하지 않는다.


def test_spelling_variants_remain_distinct_while_repeated_expressions_are_deduplicated():
    classified = classify_profanity_message("씨발 시발 존나 ㅈㄴ 병신 개병신 씨발 ㅈㄴ")
    assert classified["matchedTerms"] == ["byeongsin", "jonna", "ssibal"]
    assert classified["matchedExpressions"] == sorted(["씨발", "시발", "존나", "ㅈㄴ", "병신", "개병신"])


def test_a_quoted_expression_is_only_listed_if_it_also_has_a_clear_match():
    classified = classify_profanity_message('"씨발"이라고 했대. 씨발 또 오류야. "병신"')
    assert classified["matchedExpressions"] == ["씨발"]
    assert classified["reasons"] == ["quoted_or_code"]


def test_explanation_is_scoped_to_its_sentence_and_text_is_not_returned():
    result = classify_profanity_message("욕설 단어 병신의 뜻을 알려줘. 씨발 또 오류야")
    assert result["classification"] == "matched"
    assert result["matchedTerms"] == ["ssibal"]
    assert result["matchedExpressions"] == ["씨발"]
    assert set(result) == {"classifierVersion", "classification", "matchedTerms", "matchedExpressions", "reasons"}
    assert "오류" not in str(result)


@pytest.mark.parametrize("text,term,expression,ambiguous", [
    ("존나 간지나네", "jonna", "존나", False),
    ("씨발 힌두 영화 재미없네", "ssibal", "씨발", False),
    ("개시바 힌두 영화 재미없네", "ssibal", "개시바", False),
    ("시바 간지가 좋다", "ssibal", "시바", False),
    ("병신 힌두 영화", "byeongsin", "병신", False),
    ("시바는 힌두교의 신인데 존나 멋있어", "jonna", "존나", True),
    ("병신은 육십갑자 간지인데 씨발 헷갈리네", "ssibal", "씨발", True),
])
def test_homonym_context_only_excludes_its_exact_expression(text, term, expression, ambiguous):
    # 동음이의어 자체는 애매하게 남겨도 무관한 직접 표현은 메시지 한 건으로 센다.
    result = classify_profanity_message(text)
    assert result["classification"] == "matched"
    assert result["matchedTerms"] == [term]
    assert result["matchedExpressions"] == [expression]
    assert result["reasons"] == (["mention_or_explanation"] if ambiguous else [])


@pytest.mark.parametrize("text,classification,terms,reasons", [
    ("병신 " * 10000, "matched", ["byeongsin"], []),
    ('"병신" ' * 8000, "ambiguous", [], ["quoted_or_code"]),
    (("욕설 단어 병신 뜻을 설명해 " * 3000)[:40000], "ambiguous", [], ["mention_or_explanation"]),
    (('"병신" ' * 7997) + "\n시바...", "matched", ["ssibal"], ["quoted_or_code"]),
])
def test_long_repeated_messages_preserve_the_same_classification_without_counting_occurrences(
    text, classification, terms, reasons,
):
    # 실행 시간 임계값 대신 긴 본문의 의미가 짧은 예시와 같고 메시지 단위를 유지하는지 확인한다.
    result = classify_profanity_message(text)
    assert result["classification"] == classification
    assert result["matchedTerms"] == terms
    assert result["reasons"] == reasons


@pytest.mark.parametrize("question", [
    "DD가 올해 욕 몇 번 했어?", "디디 올해 욕횟수 알려줘", "디디 올해 욕한 횟수",
    "오늘 욕설 메시지는 몇 건이야", "어제 욕을 얼마나 했어", "9월 욕 표현 개수",
    "2026-01-01~2026-09-30 욕설 메시지 수", "올해 욕설 수는?",
])
def test_only_explicit_profanity_message_count_intent_is_parsed(question):
    query = parse_conversation_count_query(question, NOW, ["UDD123", "UDD123", "UMARU123"])
    assert query["kind"] == CONVERSATION_COUNT_KIND
    assert query["unit"] == "messages"
    assert query["classifierVersion"] == PROFANITY_CLASSIFIER_VERSION
    assert query["authorIds"] == ["UDD123", "UMARU123"]
    assert query["period"] is not None
    assert query["periodError"] is None


@pytest.mark.parametrize("question", [
    "디디가 올해 한 욕 보여줘", "올해 디디가 했던 욕을 알려줘", "디디가 올해 욕한 메시지 찾아줘",
    "디디가 올해 쓴 욕 표현 목록", "올해 디디가 한 욕 정리해줘", "디디가 올해 한 욕은 뭐야?",
    "디디가 올해 무슨 욕 했어?", "디디가 올해 어떤 욕을 했는지 알려줘",
    "모대야 디디가 올해 한 욕 보여줘", "<@UDD123>가 올해 한 욕 보여줘",
])
def test_spoken_profanity_lookup_uses_the_same_stored_message_contract(question):
    # 숫자를 묻지 않아도 발언 조회는 검색 상위 몇 건과 모델 추정으로 빠지면 안 된다.
    query = parse_conversation_count_query(question, NOW, ["UDD123"])
    assert query is not None and query["kind"] == CONVERSATION_COUNT_KIND
    assert query["authorIds"] == ["UDD123"] and query["unit"] == "messages"
    assert query["period"]["startAtKst"] == "2026-01-01T00:00:00+09:00"
    assert query["periodError"] is None


@pytest.mark.parametrize("question", [
    "디디 올해 한 말 요약해줘", "올해 욕설이란 무엇이야", "욕심을 몇 번 부렸어",
    "목욕 몇 번 했어", "욕구가 몇 개야", "욕설을 몇 번 하면 차단해야 해?", "욕설 예시를 알려줘",
    "욕설 뜻 보여줘", "올해 쓸 욕 예시 보여줘", "디디에게 할 욕을 써줘",
    "디디가 올해 한 욕 보여줘라는 문구를 다듬어줘", "디디가 올해 무슨 욕 했어라는 문장을 번역해줘",
    "디디가 올해 한 욕 보여줘라는 문구를 다듬어서 보여줘",
    "디디가 올해 한 욕이라는 제목으로 농담을 만들어서 보여줘",
    '"디디가 올해 한 욕 보여줘"라는 문장을 보여줘',
    '"디디가 올해 한 욕 보여줘"라는 문장의 뜻을 알려줘',
    "`디디가 올해 한 욕 보여줘` 문자열을 정리해줘", "디디가 올해 한 욕 보여줘라는 문구의 뜻을 알려줘",
    "디디가 올해 한 욕을 소재로 운세를 정리해줘", "욕설을 한 번 보여줘",
    "디디에게 할 욕설을 한 번 보여줘", "올해 욕을 한다면 어떤 표현인지 알려줘",
    "욕설을 한 달 배운 뒤 예시를 보여줘",
    "`디디가 올해 한 욕 보여줘`", "올해 디디가 욕한 장비를 업데이트해줘",
])
def test_summary_general_words_and_policy_threshold_questions_are_not_count_requests(question):
    assert parse_conversation_count_query(question, NOW, ["UDD123"]) is None


def test_parser_does_not_resolve_nicknames_or_silently_create_a_missing_period():
    query = parse_conversation_count_query("디디 욕 몇 번 했어", NOW, ["디디", "bad<@id>"])
    assert query["authorIds"] == []
    assert query["period"] is None
    assert query["periodError"] is None


@pytest.mark.parametrize("question,error", [
    ("2026년2월29일 욕 몇 번 했어", "invalid"),
    ("오늘과 어제 욕 몇 번 했어", "ambiguous"),
    ("내년 대신 2027년 욕 횟수", "future"),
    ("1~8월 욕 횟수", "invalid"),
    ("2026년 이전 욕 몇 번 했어", "unbounded_past"),
    ("2026년1월1일 이전 욕 몇 번 했어", "unbounded_past"),
    ("2025년 이전 욕 몇 번 했어", "unbounded_past"),
])
def test_invalid_ambiguous_or_future_dates_remain_count_requests_without_recent_fallback(question, error):
    query = parse_conversation_count_query(question, NOW, ["UDD123"])
    assert query["period"] is None
    assert query["periodError"] == error


def test_year_period_and_question_nanosecond_cutoff_are_shared_with_live_parser():
    query = parse_conversation_count_query("디디 올해 욕 몇 번 했어", NOW)
    period = query["period"]
    assert period["startAtKst"] == "2026-01-01T00:00:00+09:00"
    assert period["endAtKst"] == "2027-01-01T00:00:00+09:00"
    assert period["cutoffTs"] == NOW
    assert is_within_live_query_period(NOW, period)
    assert not is_within_live_query_period(NOW[:-3] + "790", period)


def test_explicit_inclusive_range_and_kst_year_boundary_do_not_use_wall_clock():
    query = parse_conversation_count_query("2월28일부터29일까지 욕 횟수", stamp("2024-03-01T00:00:00+09:00"))
    assert query["period"]["endAtKst"] == "2024-03-01T00:00:00+09:00"
    query = parse_conversation_count_query("올해 욕 횟수", stamp("2026-01-01T00:00:00+09:00"))
    assert query["period"]["referenceDateKst"] == "2026-01-01"


@pytest.mark.parametrize("question,supported", [
    ("2026년1월1일 욕 횟수", True),
    ("올해 욕 횟수", True),
    ("2026년1월부터9월까지 욕 횟수", True),
    ("2025년 욕 횟수", False),
    ("2025년12월31일부터2026년1월1일까지 욕 횟수", False),
    ("2025년부터2026년까지 욕 횟수", False),
    ("2026년부터 욕 횟수", True),
    ("2026년1월부터지금까지 욕 횟수", True),
    ("2026년1월1일부터현재까지 욕 횟수", True),
    ("2025년부터지금까지 욕 횟수", False),
])
def test_stored_period_policy_keeps_general_date_parsing_but_rejects_any_older_part(question, supported):
    # 날짜 해석은 과거에도 유효하며 저장 정책은 별도로 판정한다. 지원 범위로 자동 절단하지 않는다.
    query = parse_conversation_count_query(question, NOW)
    assert query["period"] is not None
    assert query["periodError"] is None
    assert is_supported_stored_period(query["period"]) is supported


@pytest.mark.parametrize("period,supported", [
    ({"startTs": "1767193200.000000", "endTs": "1767193200.000000001"}, True),
    ({"startTs": "1767193199.999999999", "endTs": "1767193201.000000000"}, False),
    ({"startTs": "1767193200.000000001", "endTs": "1767193200.000000001"}, False),
    ({"startTs": "1767193200.000000002", "endTs": "1767193200.000000001"}, False),
    ({"startTs": "NaN", "endTs": "1767193201.000000"}, False),
    ({"startTs": "1767193200.000000", "endTs": "Infinity"}, False),
    ({"startTs": 1767193200, "endTs": "1767193201.000000"}, False),
    ({"startTs": "1767193200.000000", "endTs": None}, False),
    ({"startTs": "1767193200.000000"}, False),
    ({}, False), (None, False), ([], False),
])
def test_stored_period_floor_is_exact_and_malformed_ranges_fail_closed(period, supported):
    assert STORED_HISTORY_START_TS == "1767193200.000000"
    assert is_supported_stored_period(period) is supported


def result(**changes):
    value = {
        "status": "stored", "scope": "stored_excerpts", "classifierVersion": PROFANITY_CLASSIFIER_VERSION,
        "matchedMessageCount": 4, "ambiguousMessageCount": 2, "scannedMessageCount": 100,
        "matchedExpressions": ["씨발", "존나"],
        "unavailableExcerptMessageCount": 0, "eligibleStoredMessageCount": 100,
        "period": parse_conversation_count_query("올해 욕 횟수", NOW)["period"],
    }
    return value | changes


def test_stored_answer_states_excerpt_scope_message_unit_and_ambiguity():
    answer = format_conversation_count_answer(result())
    assert "저장된 발췌 100건 중" in answer
    assert "욕 표현이 확인된 메시지는 4건" in answer
    assert "설명 2건은 합계에서 제외" in answer
    assert "여러 표현이 있어도 1건" in answer
    assert "전체 대화의 욕설 횟수는 아니야" in answer
    assert "\n확인된 욕 표현: `씨발`, `존나`." in answer
    assert "2026-01-01부터 질문 시각(2026-10-23 01:02:03 KST)까지" in answer
    assert "총 4" not in answer
    assert "·" not in answer


@pytest.mark.parametrize("eligible", [0, 12])
def test_absent_sources_or_unavailable_excerpts_are_unknown_not_zero(eligible):
    # 원문 metadata만 있거나 자료가 전혀 없는 두 경우 모두 실제 발언 0회로 답하지 않는다.
    answer = format_conversation_count_answer(result(
        matchedMessageCount=0, ambiguousMessageCount=0, scannedMessageCount=0,
        matchedExpressions=[],
        unavailableExcerptMessageCount=eligible, eligibleStoredMessageCount=eligible,
    ))
    assert "판정할 저장 발췌가 없어" in answer
    assert "메시지 수를 확인할 수 없어" in answer
    assert "0회라는 뜻은 아니야" in answer
    assert "메시지는 0건" not in answer
    assert "확인된 욕 표현:" not in answer
    if eligible:
        assert "발췌가 없는 저장 메시지 12건은 제외" in answer


def test_zero_matches_with_available_excerpts_only_describes_the_stored_scope():
    answer = format_conversation_count_answer(result(matchedMessageCount=0, matchedExpressions=[]))
    assert "저장된 발췌 100건 중 욕 표현이 확인된 메시지는 0건" in answer
    assert "전체 대화의 욕설 횟수는 아니야" in answer
    assert "확인된 욕 표현:" not in answer


def test_answer_only_lists_unique_sorted_display_expressions_without_per_term_counts():
    answer = format_conversation_count_answer(result(matchedExpressions=["존나", "씨발", "ㅈㄴ", "씨발"]))
    assert "확인된 욕 표현: `ㅈㄴ`, `씨발`, `존나`." in answer
    assert answer.count("`씨발`") == 1
    assert "`씨발` 4" not in answer


@pytest.mark.parametrize("expressions", [
    None, "씨발", {"씨발": 1}, [None], [True], [[]], [{}],
    ["씨발", "token=secret"], ["`씨발`"], ["씨발\nsecret"], ["<@UOTHER>"],
    [unicodedata.normalize("NFKC", "ㅈㄴ")], [],
])
def test_invalid_or_missing_positive_expressions_fail_closed_without_echoing_raw_text(expressions):
    # 목록 안의 임의 원문, 타입 변조, 양성인데 표현이 없는 결과를 답변에 반영하지 않는다.
    answer = format_conversation_count_answer(result(matchedExpressions=expressions))
    assert answer == "저장된 발췌의 집계 결과를 확인할 수 없어. 전체 횟수는 확인할 수 없어."


def test_missing_expression_field_or_zero_match_with_expressions_is_an_inconsistent_result():
    missing = result()
    missing.pop("matchedExpressions")
    for payload in (missing, result(matchedMessageCount=0), result(
        matchedMessageCount=0, ambiguousMessageCount=0, scannedMessageCount=0, eligibleStoredMessageCount=0,
    )):
        assert format_conversation_count_answer(payload) == (
            "저장된 발췌의 집계 결과를 확인할 수 없어. 전체 횟수는 확인할 수 없어."
        )


def test_missing_excerpts_are_reported_without_inflating_the_classified_denominator():
    answer = format_conversation_count_answer(result(
        unavailableExcerptMessageCount=7, eligibleStoredMessageCount=107,
    ))
    assert "저장된 발췌 100건 중" in answer
    assert "발췌가 없는 저장 메시지 7건은 제외" in answer
    assert "발췌 107건" not in answer


def test_stored_past_month_label_includes_last_day_but_never_next_month():
    period = parse_conversation_count_query("9월 욕 횟수", NOW)["period"]
    answer = format_conversation_count_answer(result(period=period))
    assert "2026-09-01~2026-09-30(KST)" in answer


@pytest.mark.parametrize("changes", [
    {"status": "failure"}, {"status": "failed"}, {"status": "invented"}, {"classifierVersion": "future_version"},
    {"status": "pending"}, {"status": "partial"}, {"status": "complete"}, {"status": "invalidated"},
    {"scope": "accessible_private_channels"}, {"scope": None},
    {"matchedMessageCount": True}, {"matchedMessageCount": -1}, {"ambiguousMessageCount": 97},
    {"scannedMessageCount": "100"}, {"unavailableExcerptMessageCount": True},
    {"unavailableExcerptMessageCount": -1}, {"eligibleStoredMessageCount": 99},
    {"eligibleStoredMessageCount": 101}, {"unavailableExcerptMessageCount": None},
])
def test_failure_unknown_version_and_impossible_counters_never_produce_a_count(changes):
    answer = format_conversation_count_answer(result(**changes))
    assert answer == "저장된 발췌의 집계 결과를 확인할 수 없어. 전체 횟수는 확인할 수 없어."
    assert "집계 중" not in answer
    assert "다시 확인해야" not in answer


@pytest.mark.parametrize("error", ["invalid", "ambiguous", "future"])
def test_period_errors_explain_the_problem_without_emitting_a_count(error):
    answer = format_conversation_count_answer(result(periodError=error))
    assert "4건" not in answer


def test_untrusted_metadata_is_not_echoed_in_the_answer():
    answer = format_conversation_count_answer(result(coverageReason="token=secret", period={"startTs": "secret"}))
    assert "secret" not in answer
    assert "token=" not in answer


@pytest.mark.parametrize("period", [None, {"secret": "value"}])
def test_unsupported_period_has_a_fixed_notice_without_requiring_classifier_metadata(period):
    answer = format_conversation_count_answer({"status": "unsupported_period", "period": period})
    assert answer == "2026년 1월 1일부터 저장된 대화만 지원해. 그 이전이 포함되지 않도록 기간을 지정해줘."
    assert "secret" not in answer


def test_stored_result_cannot_bypass_the_supported_period_floor():
    period = parse_conversation_count_query("2025년부터2026년까지 욕 횟수", NOW)["period"]
    answer = format_conversation_count_answer(result(period=period))
    assert "2026년 1월 1일부터 저장된 대화만 지원" in answer
    assert "4건" not in answer


def test_server_display_name_is_bounded_and_channel_metadata_is_not_presented_as_revalidation():
    answer = format_conversation_count_answer(result(authorDisplayName="DD", channelCount=3))
    assert "DD의 저장된 발췌" in answer
    assert "3곳" not in answer
    answer = format_conversation_count_answer(result(authorDisplayName="<@UOTHER> secret", channelCount=True))
    assert "secret" not in answer
    assert "<@" not in answer
    assert "True곳" not in answer


def test_non_string_period_error_and_untrusted_reason_never_raise_or_echo():
    answer = format_conversation_count_answer(result(periodError={"secret": "value"}, coverageReason=["secret"]))
    assert "secret" not in answer


def test_legacy_collection_metadata_never_adds_queue_or_revalidation_guidance():
    answer = format_conversation_count_answer(result(excludedSourceCount=9876, coverageReason="source_changed"))
    assert "9876" not in answer
    assert "재검증" not in answer
    assert "계속 처리" not in answer
    assert "같은 질문을 다시" not in answer
