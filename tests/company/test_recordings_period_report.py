import logging
import sqlite3
from contextlib import contextmanager
from datetime import date, datetime
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from boxer_company import weekly_recordings_report as report
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.factory import create_company_assistant_runtime
from boxer_company.assistant.operational_read_routes import (
    WeeklyRecordingsSummaryAssistantRoute,
)
from boxer_company.read_routing import (
    WEEKLY_RECORDINGS_SUMMARY_ROUTE,
    _extract_recordings_report_date_range,
    match_weekly_recordings_summary_route,
)
from boxer_company.recordings_report_options import (
    RecordingsReportOptions,
    RecordingsReportOptionsError,
    parse_recordings_report_options,
)
from boxer_company_adapter_slack.assistant_bridge import render_company_assistant_result
from boxer_company_adapter_slack.company_api_rollout import (
    CompanyWeeklySummaryApiRolloutService,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings


def request(question):
    return CompanyAssistantRequest(
        request_id="period-report-1", tenant_id="T1", actor_id="U1", channel="slack",
        conversation_id="1.0", question=question, locale="ko",
    )


@pytest.mark.parametrize("question", [
    "2026-09-01 ~ 2026-09-10",
    "<@U123> `2026-09-01 ~ 2026-09-10`",
    "2026-9-1부터 2026-9-10까지 녹화·신규 바코드 요약",
    "2026/09/01 ～ 2026/09/10 녹화 리포트",
    "2026-09-01 - 2026-09-10 신규바코드 병원별 요약",
    "2026-09-01 ~ 2026-09-10 A병원만",
    "2026-09-01 ~ 2026-09-10 A병원,B병원",
    "2026-09-01 ~ 2026-09-10 30% 이상 5개 이상",
])
def test_matches_and_preserves_inclusive_explicit_dates(question):
    assert match_weekly_recordings_summary_route(request(question)) == WEEKLY_RECORDINGS_SUMMARY_ROUTE
    assert _extract_recordings_report_date_range(question) == (date(2026, 9, 1), date(2026, 9, 10))


@pytest.mark.parametrize("question", [
    "2026-09-10 ~ 2026-09-01",
    "2026-09-01 ~ 2026-09-31",
    "2026-09-01 ~ 녹화 요약",
    "2026-09-01 ~",
    "2026-09-01 2026-09-10",
    "2026-09-01 2026-09-10 녹화 요약",
    "2026-09-01 ~ 2026-09-10 비교 2026-08-20 녹화 요약",
    "0001-01-01 ~ 0001-01-02",
    "9999-12-30 ~ 9999-12-31",
])
def test_invalid_period_never_queries_a_different_week(question):
    with patch.object(report, "_create_db_connection") as db:
        result = WeeklyRecordingsSummaryAssistantRoute().handle(request(question))
    assert result.outcome == "needs_input"
    assert result.fallback_reason == "invalid_date"
    db.assert_not_called()


@pytest.mark.parametrize("question", [
    "2026-09-01 ~ 2026-09-10 영상 목록",
    "2026-09-01 ~ 2026-09-10 신규 바코드 로그 요약",
    "2026-09-01 ~ 2026-09-10 신규 바코드 복구",
    "2026-09-01 ~ 2026-09-10 12345678910 영상 요약",
    "지난주 바코드 영상 현황",
    "지난주 A병원 장비 개수",
    "2026-09-01 ~ 2026-09-10 병원: A병원 장비 상태",
])
def test_period_report_does_not_take_over_other_requests(question):
    assert match_weekly_recordings_summary_route(request(question)) is None


@pytest.fixture
def recordings_db():
    # 실제 SQL을 실행해 첫 촬영의 전역 범위, 동시각 중복, 진료실 이동과 KST 경계를 검증한다.
    db = sqlite3.connect(":memory:", check_same_thread=False)
    db.row_factory = sqlite3.Row
    db.executescript("""
        CREATE TABLE hospitals (seq INTEGER, hospitalName TEXT);
        CREATE TABLE hospital_rooms (seq INTEGER, hospitalSeq INTEGER, roomName TEXT);
        CREATE TABLE recordings (seq INTEGER PRIMARY KEY, hospitalSeq INTEGER,
            hospitalRoomSeq INTEGER, deviceSeq INTEGER, fullBarcode TEXT,
            recordedAt TEXT, createdAt TEXT, deleteFlag INTEGER);
        INSERT INTO hospitals VALUES (1, 'A병원'), (2, 'B병원');
        INSERT INTO hospital_rooms VALUES (11, 1, '1진료실'), (12, 1, '2진료실'), (21, 2, '1진료실');
    """)

    def add(barcode, at, hospital=1, room=11, deleted=0, device=101):
        db.execute("INSERT INTO recordings VALUES (NULL, ?, ?, ?, ?, ?, '2026-09-17 00:00:00', ?)",
                   (hospital, room, device, barcode, at, deleted))

    add("10000000001", "2025-01-01 00:00:00", hospital=2, room=21, deleted=1)
    for i in range(3):
        add(f"3000000000{i}", "2026-08-21 15:00:00")
    for i in range(6):
        add(f"4000000000{i}", "2026-08-25 00:00:00", room=12)
    add("10000000001", "2026-09-01 00:00:00")
    add("30000000000", "2026-09-01 00:00:00", device=102)
    add("40000000000", "2026-09-01 00:00:00", hospital=2, room=21)
    add("20000000001", "2026-08-31 15:00:00")
    add("20000000001", "2026-08-31 15:00:00", hospital=2, room=21)
    add("20000000002", "2026-09-10 14:59:59")
    add("20000000003", "2026-09-10 15:00:00")
    add("20000000004", "2026-09-05 00:00:00", hospital=2, room=21)
    add("", "2026-09-05 00:00:00")
    add(None, "2026-09-05 00:00:00")
    add("20000000005", "2026-09-05 00:00:00", room=None)
    executed = []

    class Connection:
        @contextmanager
        def cursor(self):
            class Cursor:
                def execute(self, sql, params):
                    executed.append((sql, params))
                    values = tuple(v.isoformat(sep=" ") if isinstance(v, datetime) else v for v in params)
                    self.result = db.execute(sql.replace("%s", "?"), values)

                def fetchall(self):
                    return [dict(row) for row in self.result.fetchall()]
            yield Cursor()

        def close(self):
            pass

    with patch.object(report, "_create_db_connection", side_effect=lambda timeout: Connection()):
        yield executed
    db.close()


def test_counts_first_recording_once_and_compares_equal_periods(recordings_db):
    summary = report._build_weekly_recordings_report_summary(
        start_date=date(2026, 9, 1), end_date=date(2026, 9, 10), include_new_barcodes=True,
    )
    assert summary["weekStartDate"] == "2026-09-01"
    assert summary["weekEndDate"] == "2026-09-10"
    assert summary["previousWeekStartDate"] == "2026-08-22"
    assert summary["previousWeekEndDate"] == "2026-08-31"
    assert (summary["previousTotalCount"], summary["totalCount"]) == (9, 10)
    assert summary["roomDropRows"][0]["hospitalRoomSeq"] == 12
    new = summary["newBarcodes"]
    assert (new["previousTotalCount"], new["totalCount"]) == (9, 4)
    assert new["hospitalCount"] == 2
    assert [(r["hospitalSeq"], r["rowCount"]) for r in new["topRows"]] == [(1, 3), (2, 1)]
    assert (new["dropRows"][0]["previousCount"], new["dropRows"][0]["currentCount"]) == (9, 3)
    assert [(r["hospitalRoomSeq"], r["previousCount"], r["currentCount"])
            for r in new["roomDropRows"]] == [(12, 6, 0)]
    assert len(recordings_db) == 4
    assert tuple(v.isoformat() for v in recordings_db[2][1]) == ("2026-08-31T15:00:00", "2026-09-10T15:00:00")
    assert tuple(v.isoformat() for v in recordings_db[3][1]) == ("2026-08-21T15:00:00", "2026-08-31T15:00:00")


def test_empty_current_period_retains_drop_evidence_and_four_messages(recordings_db):
    # 9/11의 기록은 제외되는 9/12 이후를 요청해 신규와 녹화 모두 0건인 결과를 확인한다.
    result = WeeklyRecordingsSummaryAssistantRoute().handle(request("2026-09-12 ~ 2026-09-21"))
    assert result.outcome == "answered"
    assert len(result.messages) == 4
    assert all(m.format == "commonmark" and not m.mention_actor for m in result.messages)
    assert "**총 녹화** `0건`" in result.messages[0].body
    assert "**총 신규 바코드** `0개`" in result.messages[2].body
    assert "3건 이상" in result.messages[1].body
    assert "3개 이상" in result.messages[3].body


@pytest.mark.parametrize("options", ["", " 병원: A병원 감소율 30% 이상 감소량 5건 이상"])
def test_date_only_mention_goes_through_authenticated_api_and_slack_renderer(recordings_db, options):
    token = "t" * 48
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, callers=(CompanyApiCallerSettings(
        caller_id="period-test", token=token, tenant_ids=frozenset({"T1"}),
        channels=frozenset({"slack"}), actor_ids=frozenset({"U1"}),
        capabilities=frozenset({"assistant.turn.read"}),
    ),))
    with patch("boxer_company.assistant.factory.core_settings.LLM_PROVIDER", ""):
        runtime = create_company_assistant_runtime()
    app = create_company_api_app(settings=settings, assistant_runtime=runtime, readiness_probe=lambda: True)
    payload = {"tenantId": "T1", "actorId": "U1", "channel": "slack",
               "conversationId": "1.0", "question": "2026-09-01 ~ 2026-09-10" + options,
               "routeGroup": "structured", "locale": "ko"}
    with TestClient(app) as client:
        denied = client.post("/internal/v1/assistant/turns", json=payload,
                             headers={"X-Request-ID": "period-http-denied"})
        response = client.post("/internal/v1/assistant/turns", json=payload,
                               headers={"Authorization": f"Bearer {token}", "X-Request-ID": "period-http-1"})
    assert denied.status_code == 401
    assert response.status_code == 200, response.text
    data = response.json()
    assert data["route"] == WEEKLY_RECORDINGS_SUMMARY_ROUTE
    assert data["outcome"] == "answered" and not data["usedLlm"]
    assert len(data["messages"]) == 4
    assert all("2026-09-01 ~ 2026-09-10" in m["body"] for m in data["messages"])
    assert all("2026-08-22 ~ 2026-08-31" in m["body"] for m in data["messages"])
    if options:
        assert all("**대상 병원** A병원" in m["body"] for m in data["messages"])
        assert all("30% 이상 · 감소량 5" in m["body"] for m in data["messages"])
    # Slack wrapper가 같은 matcher로 remote에 보내고 결과를 댓글 4개로 렌더링한다.
    result = WeeklyRecordingsSummaryAssistantRoute().handle(request(payload["question"]))
    api = Mock()
    api.answer.return_value = result
    next_service = Mock()
    service = CompanyWeeklySummaryApiRolloutService(
        next_service=next_service, api_client=api, logger=logging.getLogger(__name__),
    )
    assert service.answer(request(payload["question"])) == result
    api.answer.assert_called_once()
    assert api.answer.call_args.kwargs["route_group"] == "structured"
    next_service.answer.assert_not_called()
    reply = Mock()
    assert render_company_assistant_result(result, reply=reply, actor_id="U1", client=None,
                                          logger=logging.getLogger(__name__)) == 4
    assert reply.call_count == 4
    assert "③ 병원별 신규 바코드 요약" in reply.call_args_list[2].args[0]


@pytest.mark.parametrize(("question", "expected"), [
    ("2026-09-01 ~ 2026-09-10", RecordingsReportOptions()),
    ("병원: A병원, B병원 감소율 30% 이상 감소량 5건 이상",
     RecordingsReportOptions(("A병원", "B병원"), 30, 5)),
    ("병원: A병원 2026-09-01 ~ 2026-09-10", RecordingsReportOptions(("A병원",))),
    ("병원: 전체 감소율 30퍼센트 이상", RecordingsReportOptions(drop_percent=30)),
    ("30 퍼센트 이상", RecordingsReportOptions(drop_percent=30)),
    ("최소 건수 0건 이상", RecordingsReportOptions(minimum_drop=0)),
    ("최소 5개", RecordingsReportOptions(minimum_drop=5)),
    ("A병원,B병원", RecordingsReportOptions(("A병원", "B병원"))),
    ("지난주 A병원만 30% 이상 5개 이상", RecordingsReportOptions(("A병원",), 30, 5)),
    ("병원: A병원, 30% 이상, 3건 이상", RecordingsReportOptions(("A병원",), 30, 3)),
    ("병원: A병원, A병원 최소 감소량 1,000", RecordingsReportOptions(("A병원",), None, 1000)),
    ("2026-09-01 ~ 2026-09-10 병원: A병원 <@U123>", RecordingsReportOptions(("A병원",))),
    ("감소율 0% 감소량 0", RecordingsReportOptions(drop_percent=0, minimum_drop=0)),
    ("감소율 33.3% 최소 수량 3개 이상", RecordingsReportOptions(drop_percent=33.3, minimum_drop=3)),
])
def test_parse_request_options(question, expected):
    assert parse_recordings_report_options(question) == expected


@pytest.mark.parametrize("options", [
    "감소율 -1%", "감소율 101%", "감소율 nan", "감소율 abc", "감소율 30.5.2%",
    "감소율 30% 감소율 40%", "감소량 -1", "감소량 1.5건", "감소량 없음",
    "감소량 3건 최소 건수 5건", "감소율 50% 미만", "3건 초과", "병원:",
    "병원: A병원, 전체", "병원: A병원 병원: B병원", "병원: A병원,, B병원",
])
def test_invalid_options_fail_before_reading_database(options):
    question = "2026-09-01 ~ 2026-09-10 " + options
    with patch.object(report, "_create_db_connection") as db:
        result = WeeklyRecordingsSummaryAssistantRoute().handle(request(question))
    assert result.outcome == "needs_input"
    assert result.fallback_reason == "invalid_report_options"
    db.assert_not_called()


@pytest.mark.parametrize(("previous", "current", "percent", "minimum", "expected"), [
    (10, 7, 30, 3, True), (10, 7, 30.1, 3, False), (10, 7, 30, 4, False),
    (1000, 667, 33.3, 333, True), (1000, 668, 33.3, 0, False),
    (3, 0, 100, 3, True), (3, 1, 50, 3, False), (2, 1, 50, 0, True),
    (100, 99, 0, 0, True), (0, 0, 0, 0, False), (3, 3, 0, 0, False), (3, 4, 0, 0, False),
])
def test_drop_threshold_boundaries(previous, current, percent, minimum, expected):
    # 병원과 진료실이 동일한 판정을 사용하고 0 조건에서도 유지·증가는 제외해야 한다.
    def rows(count):
        row = {"hospitalSeq": 1, "hospitalRoomSeq": 11, "hospitalName": "A병원",
               "roomName": "1진료실", "rowCount": count}
        return {"rows": [row], "roomRows": [row]}
    assert bool(report._build_weekly_recordings_report_change_rows(
        rows(current), rows(previous), direction="drop", minimum_delta=minimum, drop_percent=percent,
    )) is expected
    assert bool(report._build_weekly_recordings_room_drop_rows(
        rows(current), rows(previous), minimum_delta=minimum, drop_percent=percent,
    )) is expected


def test_selected_hospital_filters_all_queries_without_redefining_new_barcode(recordings_db):
    summary = report._build_weekly_recordings_report_summary(
        start_date=date(2026, 9, 1), end_date=date(2026, 9, 10), include_new_barcodes=True,
        options=RecordingsReportOptions(("B병원",)),
    )
    assert summary["queryOptions"]["hospitalNames"] == ["B병원"]
    assert summary["totalCount"] == 3
    # B에서 찍은 3개 중 A가 최초인 바코드 2개는 B의 신규로 바뀌지 않는다.
    assert summary["newBarcodes"]["totalCount"] == 1
    assert summary["previousTotalCount"] == 0
    for sql, params in recordings_db[1:]:
        assert "AND r.hospitalSeq IN (%s)" in sql and params[2:] == (2,)
        assert "history.hospitalSeq" not in sql
    assert len(recordings_db) == 5


def test_custom_conditions_apply_to_four_sections_and_do_not_leak(recordings_db):
    options = RecordingsReportOptions(("A병원",), drop_percent=20, minimum_drop=2)
    kwargs = {"start_date": date(2026, 9, 1), "end_date": date(2026, 9, 10), "include_new_barcodes": True}
    selected = report._build_weekly_recordings_report_summary(**kwargs, options=options)
    assert selected["dropCount"] == 1  # 9 -> 7: 22.2%, 감소량 2
    assert selected["roomDropCount"] == 1
    assert selected["newBarcodes"]["dropCount"] == 1
    assert selected["newBarcodes"]["roomDropCount"] == 1
    default = report._build_weekly_recordings_report_summary(**kwargs)
    assert default["queryOptions"] == {
        "hospitalNames": [], "dropPercent": 50, "minimumDrop": 3,
        "hospitalRecordingMinimumDrop": 3,
    }
    assert default["totalCount"] == 10
    assert default["dropCount"] == 0
    automatic = report._build_weekly_recordings_report_summary(
        start_date=date(2026, 9, 1), end_date=date(2026, 9, 10),
    )
    assert "queryOptions" not in automatic and "newBarcodes" not in automatic


@pytest.mark.parametrize("names", [("A", "B"), ("A병원", "B병원"), ("A", "A병원", "B")])
def test_multiple_hospitals_resolve_once_and_deduplicate_ids(recordings_db, names):
    summary = report._build_weekly_recordings_report_summary(
        start_date=date(2026, 9, 1), end_date=date(2026, 9, 10), include_new_barcodes=True,
        options=RecordingsReportOptions(names),
    )
    assert summary["totalCount"] == 10
    assert summary["queryOptions"]["hospitalNames"] == ["A병원", "B병원"]
    assert len(recordings_db) == len(names) + 4
    assert all(params[2:] == (1, 2) for _, params in recordings_db[len(names):])


@pytest.mark.parametrize("name", ["없는병원", "병원", "%", "_", "A' OR 1=1 --"])
def test_unknown_or_ambiguous_hospital_never_falls_back_to_all(recordings_db, name):
    result = WeeklyRecordingsSummaryAssistantRoute().handle(request(
        "2026-09-01 ~ 2026-09-10 병원: " + name,
    ))
    assert result.outcome == "needs_input"
    assert result.fallback_reason == "invalid_report_options"
    assert len(recordings_db) == 1
    sql, params = recordings_db[0]
    assert "FROM hospitals" in sql and "%s" in sql
    escaped = name.replace("=", "==").replace("%", "=%").replace("_", "=_")
    assert params == (f"%{escaped}%",)


def test_empty_resolved_hospital_scope_cannot_mean_all():
    with (patch.object(report, "_create_db_connection") as db,
          pytest.raises(RecordingsReportOptionsError)):
        report._load_weekly_recordings_report(hospital_seqs=())
    db.assert_not_called()
