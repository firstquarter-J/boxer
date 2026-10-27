"""질문 시각의 KST 달력으로만 대화 조회 범위를 고정한다."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from boxer_company.memory.live_query import (
    is_within_live_query_period,
    live_query_period_error,
    resolve_live_query_period,
    strip_live_query_period,
    wants_private_live_sources,
    wants_stored_history_sources,
)

KST = ZoneInfo("Asia/Seoul")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def timestamp(value: str, fraction: str = "000000") -> str:
    delta = datetime.fromisoformat(value).astimezone(UTC) - EPOCH
    return str(delta.days * 86400 + delta.seconds) + "." + fraction


NOW = timestamp("2026-10-22T18:00:00+09:00", "123456789")


@pytest.mark.parametrize("question,start,end,kind", [
    ("그제 디디 발언", "2026-10-20", "2026-10-21", "date"),
    ("올해의 대화", "2026-01-01", "2027-01-01", "year"),
    ("작년 발언", "2025-01-01", "2026-01-01", "year"),
    ("이번달 대화", "2026-10-01", "2026-11-01", "month"),
    ("이번 달의 대화", "2026-10-01", "2026-11-01", "month"),
    ("지난달 대화", "2026-09-01", "2026-10-01", "month"),
    ("지난 달 대화", "2026-09-01", "2026-10-01", "month"),
    ("이번주 대화", "2026-10-19", "2026-10-26", "week"),
    ("지난 주 대화", "2026-10-12", "2026-10-19", "week"),
    ("2024년 대화", "2024-01-01", "2025-01-01", "year"),
    ("2024년 2월 대화", "2024-02-01", "2024-03-01", "month"),
    ("올해 9월 대화", "2026-09-01", "2026-10-01", "month"),
    ("작년 9월 3일 대화", "2025-09-03", "2025-09-04", "date"),
    ("2월 대화", "2026-02-01", "2026-03-01", "month"),
    ("2024년 2월 29일 발언", "2024-02-29", "2024-03-01", "date"),
    ("2월 28일 발언", "2026-02-28", "2026-03-01", "date"),
    ("2026-10-01 발언", "2026-10-01", "2026-10-02", "date"),
    ("2026.10.1 발언", "2026-10-01", "2026-10-02", "date"),
    ("2026/10/1 발언", "2026-10-01", "2026-10-02", "date"),
    ("２０２６년 １０월 １일 발언", "2026-10-01", "2026-10-02", "date"),
    ("10월1일부터15일까지 발언", "2026-10-01", "2026-10-16", "range"),
    ("2024년2월28일부터29일까지 발언", "2024-02-28", "2024-03-01", "range"),
    ("2026년9월30일부터10월2일까지", "2026-09-30", "2026-10-03", "range"),
    ("2026-09-30~2026-10-02 대화", "2026-09-30", "2026-10-03", "range"),
    ("2026-10-01~15일 발언", "2026-10-01", "2026-10-16", "range"),
    ("1월~8월 발언", "2026-01-01", "2026-09-01", "range"),
    ("10월 1일 ~ 10월 15일 발언", "2026-10-01", "2026-10-16", "range"),
    ("2025년12월부터2026년1월까지", "2025-12-01", "2026-02-01", "range"),
    ("어제부터오늘까지 대화", "2026-10-21", "2026-10-23", "range"),
    ("2026년부터 대화", "2026-01-01", "2026-10-23", "range"),
    ("2026년1월부터지금까지 대화", "2026-01-01", "2026-10-23", "range"),
    ("2026년1월1일부터현재까지 발언", "2026-01-01", "2026-10-23", "range"),
    ("2025년부터 지금까지 대화", "2025-01-01", "2026-10-23", "range"),
    ("10월1일부터 현재까지 발언", "2026-10-01", "2026-10-23", "range"),
    ("어제부터 대화", "2026-10-21", "2026-10-23", "range"),
])
def test_calendar_periods_use_kst_midnights_and_inclusive_range_end(question, start, end, kind):
    period = resolve_live_query_period(question, NOW)
    assert period == {
        "kind": kind, "referenceDateKst": "2026-10-22", "cutoffTs": NOW,
        "startAtKst": start + "T00:00:00+09:00", "endAtKst": end + "T00:00:00+09:00",
        "startTs": timestamp(start + "T00:00:00+09:00"), "endTs": timestamp(end + "T00:00:00+09:00"),
    }
    assert live_query_period_error(question, NOW) is None


@pytest.mark.parametrize("count", [1, 2, 7, 365, 3660])
@pytest.mark.parametrize("prefix", ["최근", "지난"])
def test_recent_days_include_today_and_use_bounded_calendar_days(count, prefix):
    period = resolve_live_query_period(f"{prefix} {count}일 대화", NOW)
    assert period["kind"] == "recent_days"
    start = datetime(2026, 10, 22, tzinfo=KST) - timedelta(days=count - 1)
    assert period["startAtKst"] == start.isoformat()
    assert period["endAtKst"] == "2026-10-23T00:00:00+09:00"
    assert is_within_live_query_period(NOW, period)


@pytest.mark.parametrize("question,error", [
    ("2026년2월29일 대화", "invalid"),
    ("1900년2월29일 대화", "invalid"),
    ("2026-02-30 대화", "invalid"),
    ("2026/10.01 대화", "invalid"),
    ("2026년13월 대화", "invalid"),
    ("0월 발언", "invalid"),
    ("10월0일 대화", "invalid"),
    ("0000년 발언", "invalid"),
    ("9999년 발언", "invalid"),
    ("최근 0일 발언", "invalid"),
    ("지난 3661일 발언", "invalid"),
    ("10월15일부터1일까지 발언", "invalid"),
    ("2월28일부터30일까지 대화", "invalid"),
    ("2026-10-15~2026-10-01 대화", "invalid"),
    ("2026-10-01~ 대화", "invalid"),
    ("오늘부터의 일정", "invalid"),
    ("1~8월 대화", "invalid"),
    ("10월 1~15일 대화", "invalid"),
    ("10월1~15일 대화", "invalid"),
    ("오늘과 어제 비교", "ambiguous"),
    ("오늘과어제 대화", "ambiguous"),
    ("2026년과 2025년 대화", "ambiguous"),
    ("지난달과 이번달 발언", "ambiguous"),
    ("오늘 10월1일 발언", "ambiguous"),
    ("2027년 대화", "future"),
    ("11월 발언", "future"),
    ("10월23일 발언", "future"),
    ("2026-11-01~2026-11-02 발언", "future"),
    ("2027년부터지금까지 대화", "future"),
    ("2026년 이전 대화", "unbounded_past"),
    ("2026년1월1일 이전 대화", "unbounded_past"),
    ("2025년 이전 대화", "unbounded_past"),
    ("2026년 10월 이전 대화", "unbounded_past"),
    ("2026년이전까지의 대화", "unbounded_past"),
    ("2026년 전 대화", "unbounded_past"),
    ("2027년 이전 대화", "unbounded_past"),
    ("2026년 이후 대화", "invalid"),
    ("2026년이전이후 대화", "invalid"),
    ("2025년부터2026년 이전 대화", "ambiguous"),
    ("2026년 이전과 이후 대화", "ambiguous"),
    ("2026년 이전 2026년 대화", "ambiguous"),
    ("2026년부터 지금까지 9월 대화", "ambiguous"),
    ("2026년부터 이전 대화", "ambiguous"),
])
def test_invalid_conflicting_and_future_dates_cannot_fall_back_to_recent(question, error):
    assert resolve_live_query_period(question, NOW) is None
    assert live_query_period_error(question, NOW) == error


@pytest.mark.parametrize("question", [
    "최근 디디 발언", "오늘날 사회", "오늘날씨", "어제오늘 근황", "today", "todayish",
    "버전 v12.22.1", "버전 v2026.10.01", "기기번호2026-10-01", "12345 기록",
    "배포 2026.10.01.1 버전", "장비 123월호", "발언 15일 분량",
])
def test_ordinary_words_versions_and_numbers_do_not_create_dates(question):
    assert resolve_live_query_period(question, NOW) is None
    assert live_query_period_error(question, NOW) is None


@pytest.mark.parametrize("before", [None, "", "NaN", "1", "99999999999999999999.1"])
def test_unusable_reference_does_not_guess_even_for_invalid_calendar_date(before):
    assert resolve_live_query_period("2026년2월30일", before) is None
    assert live_query_period_error("2026년2월30일", before) is None


@pytest.mark.parametrize("question,start,end", [
    ("지난달", "2025-12-01", "2026-01-01"),
    ("지난주", "2025-12-22", "2025-12-29"),
    ("그제", "2025-12-30", "2025-12-31"),
    ("1월 1일", "2026-01-01", "2026-01-02"),
])
def test_utc_previous_day_and_year_boundary_use_question_kst_year(question, start, end):
    # UTC로는 12월 31일이어도 질문의 KST 1월 1일을 기준으로 연도와 요일을 정한다.
    before = timestamp("2026-01-01T00:00:00+09:00", "000000001")
    period = resolve_live_query_period(question, before)
    assert period["referenceDateKst"] == "2026-01-01"
    assert period["startAtKst"] == start + "T00:00:00+09:00"
    assert period["endAtKst"] == end + "T00:00:00+09:00"


def test_yearless_february_uses_question_year_instead_of_most_recent_leap_year():
    assert live_query_period_error("2월29일", NOW) == "invalid"
    before = timestamp("2024-03-01T00:00:00+09:00")
    assert resolve_live_query_period("2월29일", before)["startAtKst"] == "2024-02-29T00:00:00+09:00"


def test_current_natural_periods_keep_future_end_but_never_include_future_messages():
    for question in ("올해", "이번달", "이번주", "최근7일", "2026년부터", "2026년1월부터지금까지"):
        period = resolve_live_query_period(question, NOW)
        assert is_within_live_query_period(NOW, period)
        assert is_within_live_query_period(NOW[:-1] + "8", period)
        assert not is_within_live_query_period(timestamp("2026-10-22T18:00:00+09:00", "123456790"), period)
        assert not is_within_live_query_period(timestamp("2026-10-22T18:00:01+09:00"), period)


def test_range_end_includes_every_fraction_before_next_midnight():
    period = resolve_live_query_period("10월1일부터15일까지", NOW)
    assert is_within_live_query_period(timestamp("2026-10-15T23:59:59+09:00", "999999999"), period)
    assert not is_within_live_query_period(timestamp("2026-10-16T00:00:00+09:00"), period)


def test_repeated_same_date_is_unambiguous_and_before_epoch_calendar_boundary_is_valid():
    assert resolve_live_query_period("오늘 오늘 발언", NOW)["kind"] == "today"
    period = resolve_live_query_period("1970년", NOW)
    assert period["startTs"] == "-32400.000000"
    assert is_within_live_query_period("0.000000000", period)


@pytest.mark.parametrize("question,remaining", [
    ("8월 대화 요약", "대화 요약"),
    ("2026년 8월에 박서 v12.22.1 배포 대화", "박서 v12.22.1 배포 대화"),
    ("10월1일부터15일까지 디디 제품123 발언", "디디 제품123 발언"),
    ("2026-09-01 ~ 2026-09-30 대화", "대화"),
    ("오늘의 디디 발언", "디디 발언"),
    ("최근 7일 대화", "대화"),
    ("2026년부터 대화", "대화"),
    ("2026년1월부터지금까지 디디 발언", "디디 발언"),
    ("2026년1월1일부터현재까지 발언", "발언"),
    ("2026년 이전 대화", "2026년 이전 대화"),
    ("Straße 9월 대화", "strasse 대화"),
    ("İ 9월 대화", "i\u0307 대화"),
    ("오늘날씨 v12.22.1", "오늘날씨 v12.22.1"),
    ("2026년2월30일 대화", "2026년2월30일 대화"),
    ("오늘과 어제 비교", "오늘과 어제 비교"),
])
def test_only_valid_date_spans_are_removed_and_topic_numbers_remain(question, remaining):
    assert strip_live_query_period(question, NOW) == remaining


def test_explicit_date_and_conversation_reference_select_live_lookup_without_query_verb():
    assert wants_private_live_sources("9월 대화 요약")
    assert wants_private_live_sources("작년 디디 발언")
    assert not wants_private_live_sources("9월 모델 버전")


@pytest.mark.parametrize("question,authors", [
    ("디디가최근뭐랬어?", ("UDD",)),
    ("디디가 최근 뭐랬어", ("UDD",)),
    ("<@UDD|디디>가 최근 뭐랬어?", ("UDD",)),
    ("모대야 디디가 최근 뭐랬어?", ("UDD",)),
    ("디디는어떤사람이야?", ("UDD",)),
    ("디디는 어떤 사람이야", ("UDD",)),
    ("디디에 대해 알려줘", ("UDD",)),
    ("디디 생일이 언제야?", ("UDD",)),
    ("디디가 어떤 업무를 담당하는지 알려줘", ("UDD",)),
    ("2026년이전대화요약해줘", ()),
    ("디디가 2025년에 한 말", ("UDD",)),
    ("디디가 2025년12월31일부터2026년1월1일까지 한 말", ("UDD",)),
    ("작년 디디 발언", ("UDD",)),
    ("오늘디디가한말요약", ()),
    ("저번대화찾아줘", ()),
    ("2025년에한말은뭐야", ()),
    ("2025년에 한 말은 뭐야?", ()),
    ("2025년 대화는 뭐였어", ()),
    ("2025년 대화에서 어떤 결정이 있었는지", ()),
    ("지난 대화에서 무슨 얘기 했어", ()),
    ("슬랙 발언을 보여줄래?", ()),
    ("어제 대화 정리", ()),
    ("２０２６년 대화 요약해줘", ()),
])
def test_stored_history_intent_requires_actual_record_or_person_fact_lookup(question, authors):
    assert wants_stored_history_sources(question, authors)


@pytest.mark.parametrize("question,authors", [
    ("디디생일축하문구를더짧게다듬어줘", ("UDD",)),
    ("모대 디디 생일축하 문구 다듬어줘", ("UDD",)),
    ("디디 생일축하 문구를 어떻게 다듬어줄래?", ("UDD",)),
    ("디디 생일축하 문구를 어떻게 고칠까?", ("UDD",)),
    ("디디에 대한 생일축하 문구 써줘", ("UDD",)),
    ("디디가 한 말 더 짧게 다듬어줘", ("UDD",)),
    ("디디 대화 문장을 어떻게 번역해줄래?", ("UDD",)),
    ("모대디디놀려줘", ("UDD",)),
    ("디디 안녕", ("UDD",)),
    ("디디 오늘도 힘내", ("UDD",)),
    ("1992년생오늘은대화하며행운을찾아보세요.", ()),
    ("1992년생 오늘은 대화하며 행운을 찾아보세요.", ("UDD",)),
    ("오늘 대화에서는 먼저 웃어보세요.", ()),
    ("대화하며 행운을 찾는 하루가 될 거예요.", ()),
    ("오늘 날씨 알려줘", ()),
    ("2026년 모델 버전 알려줘", ()),
    ("생일 축하 문구 다듬어줘", ()),
    ("안녕", ()), ("", ()), (None, ()),
])
def test_names_editing_fun_and_declarative_fortunes_do_not_request_stored_history(question, authors):
    assert not wants_stored_history_sources(question, authors)


def test_stored_history_intent_does_not_change_the_legacy_live_matcher():
    question = "디디 생일축하 문구 다듬어줘"
    assert wants_private_live_sources(question, ("UDD",))
    assert not wants_stored_history_sources(question, ("UDD",))
