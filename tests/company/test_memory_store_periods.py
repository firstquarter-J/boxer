"""기간에 맞는 저장 기억이 최신 후보와 개수 제한에 밀리지 않는지 확인한다."""

from decimal import Decimal

import pytest

from boxer_company.memory.contracts import MemoryError, source_identity, source_revision
from boxer_company.memory.store import MemoryStore, initialize_memory_store

TENANT, CHANNEL, HUB = "T19PA3EPL", "C0621TL2HSB", "C068FVD5V7Y"
PERIOD = {"startTs": "1788188400.000000", "endTs": "1790780400.000000", "cutoffTs": "1792663200.123456789"}


@pytest.fixture
def store(tmp_path):
    return MemoryStore(initialize_memory_store(tmp_path / "private" / "memory.db"), clock=lambda: 1_792_663_200)


def seed(store, entries):
    # 모델이나 원문 수집 없이 실제 FK를 가진 SQLite 후보만 한 transaction에 준비한다.
    with store._connection(write=True) as connection:
        for item in entries:
            name, stamps = item["id"], item["stamps"]
            tenant, channel = item.get("tenant", TENANT), item.get("channel", CHANNEL)
            thread_id = "thread_" + name
            thread_ts = min(stamps, key=Decimal) if stamps else PERIOD["startTs"]
            connection.execute("INSERT INTO memory_threads(id,tenant_id,channel_id,thread_ts,channel_kind) VALUES(?,?,?,?,?)",
                (thread_id, tenant, channel, thread_ts, item.get("channel_kind", "private_channel")))
            connection.execute("""INSERT INTO memory_items(id,tenant_id,scope_channel_id,origin_thread_id,kind,category,
                subject_user_id,topic_key,text,evidence_type,valid_from,observed_at,updated_at,extractor_version)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (name, tenant, channel, thread_id,
                item.get("kind", "person"), item.get("category", "fact"), item.get("subject", "UDD"),
                item.get("topic", "회의 기록"), item.get("text", "디디 회의 기록"),
                item.get("evidence", "attributed_statement"), 1, 1, item.get("updated", 1), "memory-v1"))
            for stamp in stamps:
                revision = source_revision(name)
                identity = source_identity(tenant, channel, stamp, revision)
                connection.execute("""INSERT INTO memory_sources(id,tenant_id,channel_id,thread_id,message_ts,revision_hash,
                    author_id,author_kind,created_at,edited_at,permalink,excerpt,last_verified_at) VALUES(?,?,?,?,?,?,?,'human',?,?,?,?,?)""",
                    (identity, tenant, channel, thread_id, stamp, revision, item.get("author", "UDD"), stamp,
                     item.get("edited", ""), f"https://team.slack.com/archives/{channel}/p{stamp.replace('.', '')}", "", 1_792_663_200))
                connection.execute("INSERT INTO memory_item_sources(tenant_id,memory_id,source_id,relation,evidence_span) VALUES(?,?,?,'supports','')",
                                   (tenant, name, identity))


def retrieve(store, *, period=PERIOD, channel=CHANNEL, actor="REQUESTER", tenant=TENANT):
    return store.retrieval_candidates(tenant, channel, actor, "unrelated-query", "디디 회의 기록",
                                      mentioned_user_ids=["UDD"], period=period)


@pytest.mark.parametrize("mixed", [False, True])
def test_period_filter_runs_before_the_latest_256_candidate_limit(store, mixed):
    newer = [{"id": f"newer-{number}", "updated": 1000 + number,
              "stamps": [f"1791000000.{number:06d}"] + ([f"1789000000.{number:06d}"] if mixed else [])}
             for number in range(256)]
    seed(store, [{"id": "old-but-in-period", "stamps": ["1789000001.000001"]}, *newer])
    assert "old-but-in-period" not in {item["memoryId"] for item in retrieve(store, period=None)}
    # 혼합 근거도 SQL 제한 전에 제외해야 온전한 과거 기억이 최신256개 뒤에 남지 않는다.
    assert [item["memoryId"] for item in retrieve(store)] == ["old-but-in-period"]


def test_source_time_not_memory_updated_time_defines_the_period(store):
    seed(store, [{"id": "late-summary", "stamps": ["1789000000.000001"], "updated": 1_900_000_000},
                 {"id": "old-summary-new-source", "stamps": ["1791000000.000001"], "updated": 1}])
    assert [item["memoryId"] for item in retrieve(store)] == ["late-summary"]


def test_authorless_date_summary_accepts_the_period_as_a_retrieval_condition(store):
    seed(store, [{"id": "dated-only", "stamps": ["1789000000.000001"], "topic": "배포 일정", "text": "배포일을 합의함"}])
    def query(period):
        return store.retrieval_candidates(TENANT, CHANNEL, "REQUESTER", "unrelated", "9월 대화 요약",
                                           mentioned_user_ids=[], period=period)
    assert query(None) == []
    assert [item["memoryId"] for item in query(PERIOD)] == ["dated-only"]


def test_a_date_does_not_broaden_an_explicit_author_to_unrelated_people(store):
    seed(store, [{"id": "target", "stamps": ["1789000000.000001"], "topic": "배포 일정", "text": "배포일을 합의함"},
        {"id": "unrelated-author", "stamps": ["1789000000.000002"], "topic": "문서 정리", "text": "자료를 정리함",
         "subject": "OTHER", "author": "OTHER"}])
    found = store.retrieval_candidates(TENANT, CHANNEL, "REQUESTER", "unrelated", "디디 9월 대화 요약",
                                        mentioned_user_ids=["UDD"], period=PERIOD)
    assert [item["memoryId"] for item in found] == ["target"]


@pytest.mark.parametrize("stamp,expected", [
    ("1788188399.999999999", False), ("1788188400.000000000", True),
    ("1790780399.999999999", True), ("1790780400.000000000", False),
])
def test_period_midnight_boundaries_do_not_round_through_float(store, stamp, expected):
    seed(store, [{"id": "boundary", "stamps": [stamp]}])
    assert bool(retrieve(store)) is expected


def test_question_cutoff_is_exact_before_sql_limit_and_ranking(store):
    period = {"startTs": "1792594800.000000", "endTs": "1792681200.000000", "cutoffTs": "1792663200.123456789"}
    # 같은 초의 더 늦은 256개 근거도 cutoff 직전의 유효 기억을 밀어내지 못한다.
    seed(store, [{"id": "at-cutoff", "stamps": [period["cutoffTs"]]}, *[
        {"id": f"after-{number}", "stamps": [f"1792663200.{123456790 + number:09d}"], "updated": 1000 + number}
        for number in range(256)]])
    assert [item["memoryId"] for item in retrieve(store, period=period)] == ["at-cutoff"]


@pytest.mark.parametrize("edited,expected", [("1791000000.000000", True),
    ("1792663200.123456789", True), ("1792663200.123456790", False)])
def test_edit_time_only_obeys_question_cutoff_and_not_the_requested_source_period(store, edited, expected):
    seed(store, [{"id": "edited-source", "stamps": ["1789000000.000001"], "edited": edited}])
    assert bool(retrieve(store)) is expected


@pytest.mark.parametrize("start", ["-32400.000000", "-0.100000000"])
def test_kst_year_1970_accepts_a_boundary_before_the_utc_epoch(store, start):
    # 1970년 KST의 시작은 UTC epoch보다9시간이르지만 실제 Slack 출처는 비음수다.
    period = {"startTs": start, "endTs": "31503600.000000", "cutoffTs": PERIOD["cutoffTs"]}
    seed(store, [{"id": "epoch", "stamps": ["0.000001"]}])
    assert [item["memoryId"] for item in retrieve(store, period=period)] == ["epoch"]


def test_empty_sources_and_any_outside_period_reference_are_excluded(store):
    seed(store, [{"id": "no-source", "stamps": []}, {"id": "mixed", "stamps": ["1789000000.000001", "1791000000.000001"]},
                 {"id": "inside", "stamps": ["1789000000.000002", "1789000001.000002"]}])
    assert [item["memoryId"] for item in retrieve(store)] == ["inside"]


def test_own_standard_style_keeps_the_existing_cross_channel_period_exception(store):
    seed(store, [{"id": "own-style", "stamps": ["1700000000.000001"], "channel": "C_STYLE",
        "category": "preference", "subject": "REQUESTER", "author": "REQUESTER", "topic": "response_length",
        "text": "짧은 답변을 선호함", "evidence": "self_statement"},
        {"id": "other-style", "stamps": ["1700000000.000002"], "category": "preference",
         "topic": "response_length", "text": "짧은 답변을 선호함", "evidence": "self_statement"},
        {"id": "inside", "stamps": ["1789000000.000001"]}])
    selected = retrieve(store)
    assert {item["memoryId"] for item in selected} == {"own-style", "inside"}
    assert selected[0]["styleOnly"] is True and selected[0]["subjectUserId"] == "REQUESTER"


def test_period_filter_preserves_normal_and_hub_visibility(store):
    seed(store, [{"id": "local", "stamps": ["1789000000.000001"]},
        {"id": "private-other", "stamps": ["1789000000.000002"], "channel": "C_OTHER"},
        {"id": "public-other", "stamps": ["1789000000.000003"], "channel": "C_PUBLIC", "channel_kind": "channel"},
        {"id": "dm-other", "stamps": ["1789000000.000004"], "channel": "D_OTHER", "channel_kind": "dm"},
        {"id": "another-tenant", "stamps": ["1789000000.000005"], "tenant": "OTHER"}])
    assert {item["memoryId"] for item in retrieve(store)} == {"local"}
    assert {item["memoryId"] for item in retrieve(store, channel=HUB)} == {"local", "private-other"}


@pytest.mark.parametrize("period", [{}, {**PERIOD, "startTs": "NaN"}, {**PERIOD, "endTs": PERIOD["startTs"]},
    {**PERIOD, "cutoffTs": "99999999999999999999.000000"}, {**PERIOD, "startTs": "-99999999999999999999.000000"},
    {**PERIOD, "cutoffTs": "-1.000000"}])
def test_malformed_period_cannot_fall_back_to_unbounded_retrieval(store, period):
    seed(store, [{"id": "inside", "stamps": ["1789000000.000001"]}])
    with pytest.raises(MemoryError, match="memory_query_period_invalid"):
        retrieve(store, period=period)
