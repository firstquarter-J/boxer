"""기간 해석부터 실제 집계 SQL, 인증 API와 Slack mock까지 추이 계약을 검증한다."""

import logging
from datetime import UTC, date, datetime
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from boxer_company import weekly_recordings_report as report
from boxer_company.assistant.factory import create_company_assistant_runtime
from boxer_company.assistant.operational_read_routes import (
    WeeklyRecordingsSummaryAssistantRoute,
)
from boxer_company.assistant.recordings_trend_format import format_recordings_trend
from boxer_company.read_routing import match_weekly_recordings_summary_route
from boxer_company.recordings_report_options import RecordingsReportOptions
from boxer_company.recordings_trend_query import (
    parse_recordings_trend_query,
    trend_periods,
)
from boxer_company.recordings_trend_report import (
    _consecutive_declines,
    build_recordings_trend_report,
)
from boxer_company_adapter_slack.assistant_bridge import render_company_assistant_result
from boxer_company_adapter_slack.company_api_rollout import (
    CompanyWeeklySummaryApiRolloutService,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings
from tests.company import test_recordings_period_report as period_tests

# 기간 요약과 동일한 DB 이력 fixture를 사용해 두 조회의 집계 기준을 비교한다.
recordings_db = period_tests.recordings_db
request = period_tests.request

NOW = datetime(2026, 9, 23, 12, tzinfo=ZoneInfo("Asia/Seoul"))


@pytest.mark.parametrize(("question", "start", "end", "decline"), [
    ("주간 녹화 리포트 최근 4주 추이 분석", "2026-08-24", "2026-09-20", None),
    ("8월부터 지금까지의 추이 조회", "2026-08-01", "2026-09-23", None),
    ("2026년 8월부터 현재까지 녹화 추이", "2026-08-01", "2026-09-23", None),
    ("올해 8월부터 현재까지 녹화 추이", "2026-08-01", "2026-09-23", None),
    ("작년 12월부터 현재까지 녹화 추이", "2025-12-01", "2026-09-23", None),
    ("8월 3일부터 9월 20일까지 녹화 추이", "2026-08-03", "2026-09-20", None),
    ("2026-08-03 ~ 2026-09-20 녹화 추이", "2026-08-03", "2026-09-20", None),
    ("2026-08-03부터 오늘까지 녹화 추이", "2026-08-03", "2026-09-23", None),
    ("2주 이상 감소 데이터 조회", "2026-08-24", "2026-09-20", 2),
    ("2주 이상 감소한 데이터 조회", "2026-08-24", "2026-09-20", 2),
    ("최근 8주 녹화 3주 연속 감소한 병원 목록", "2026-07-27", "2026-09-20", 3),
    ("지난달 녹화 추이", "2026-08-01", "2026-08-31", None),
    ("이번 달 녹화 추이", "2026-09-01", "2026-09-23", None),
    ("최근 30일 녹화 추이", "2026-08-25", "2026-09-23", None),
    ("최근 2개월 녹화 추이", "2026-07-23", "2026-09-23", None),
])
def test_period_and_decline_are_independent_and_route_to_remote(question, start, end, decline):
    assert match_weekly_recordings_summary_route(request(question)) == "weekly_recordings_summary"
    query = parse_recordings_trend_query(question, now=NOW)
    assert (query.start.isoformat(), query.end.isoformat(), query.decline_weeks) == (start, end, decline)


@pytest.mark.parametrize("question", [
    "매출 최근 4주 추이", "최근 4주 병원 매출 추이", "장비 2주 이상 감소 데이터 조회", "최근 4주 녹화 다운로드 추이",
    "최근 4주 12345678910 녹화 추이", "8월부터 지금까지 신규 바코드 로그 추이",
])
def test_trend_does_not_take_other_scopes(question):
    assert match_weekly_recordings_summary_route(request(question)) is None


@pytest.mark.parametrize("period", [
    "최근 0주", "최근 -2주", "최근 53주", "최근 100개월", "최근 4.5주", "최근 네 주",
    "13월부터 지금까지", "8월 32일부터 지금까지", "10월부터 지금까지",
    "2026-09-01 ~ 2026-09-31", "9월부터 8월까지", "2026-09-01 ~",
    "최근 4주 최근 8주", "8월부터 현재까지 최근 4주", "지난달 이번달", "최근 4주 2026-08-01",
    "최근 2주 2주 이상 감소", "0주 이상 감소", "52주 이상 감소",
    "8월부터 오늘까지 2026-07-01", "일별 최근 4주",
])
def test_invalid_or_ambiguous_period_never_queries_another_period(period):
    with patch.object(report, "_create_db_connection") as db, patch(
        "boxer_company.assistant.operational_read_routes._coerce_weekly_recordings_report_now", return_value=NOW,
    ):
        result = WeeklyRecordingsSummaryAssistantRoute().handle(request(period + " 녹화 추이"))
    assert result.outcome == "needs_input"
    db.assert_not_called()


def test_kst_year_boundary_and_leap_month():
    # UTC 일요일이어도 KST 월요일이면 막 끝난 주까지 네 주를 포함한다.
    now = datetime(2025, 12, 28, 15, tzinfo=UTC)
    query = parse_recordings_trend_query("최근 4주 녹화 추이", now=now)
    assert (query.start, query.end) == (date(2025, 12, 1), date(2025, 12, 28))
    leap = parse_recordings_trend_query("최근 1개월 녹화 추이", now=datetime(2024, 3, 31, tzinfo=UTC).replace(tzinfo=None))
    assert leap.start == date(2024, 2, 29)


def test_partial_and_ongoing_weeks_are_not_decline_evidence():
    query = parse_recordings_trend_query("8월부터 지금까지 추이", now=NOW)
    periods = trend_periods(query, today=NOW.date())
    assert periods[0] == (date(2026, 8, 1), date(2026, 8, 2), False)
    assert periods[-1] == (date(2026, 9, 21), date(2026, 9, 23), False)
    assert all(complete for _, _, complete in periods[1:-1])


@pytest.mark.parametrize(("counts", "required", "expected"), [
    ([10, 8, 5, 0], 2, 3), ([10, 8, 8, 5], 2, 0), ([10, 8, 0, 0], 2, 0),
    ([10, 8, 5, 6], 2, 0), ([0, 10, 8, 5], 2, 2), ([0, 0, 0], 1, 0),
])
def test_strict_decline_streak_ends_at_latest_completed_week(counts, required, expected):
    # 0건 누락·보합·반등은 과거 감소와 이어 붙이지 않는다.
    weeks = [
        {"startDate": f"week-{i}", "endDate": f"week-{i}",
         "hospitals": {1: {"name": "A병원", "totalCount": count}} if count else {}}
        for i, count in enumerate(counts)
    ]
    rows = _consecutive_declines(weeks, group="hospitals", metric="totalCount",
                                required=required, minimum=1, percent=0)
    assert (rows[0]["streak"] if rows else 0) == expected


def test_decline_thresholds_must_hold_for_every_transition():
    weeks = [
        {"startDate": f"week-{i}", "endDate": f"week-{i}",
         "hospitals": {1: {"name": "A병원", "totalCount": count}}}
        for i, count in enumerate((100, 70, 49))
    ]
    kwargs = {"group": "hospitals", "metric": "totalCount", "required": 2, "percent": 30}
    assert _consecutive_declines(weeks, minimum=20, **kwargs)[0]["streak"] == 2
    assert not _consecutive_declines(weeks, minimum=22, **kwargs)


def test_zero_recordings_are_reported_without_inventing_a_trend(recordings_db):
    with patch("boxer_company.assistant.operational_read_routes._coerce_weekly_recordings_report_now", return_value=NOW):
        result = WeeklyRecordingsSummaryAssistantRoute().handle(request("2026-07-01 ~ 2026-07-31 녹화 추이"))
    assert result.outcome == "no_evidence"
    assert "해당 조건의 대상 없음" in result.messages[1].body
    assert "비교 불가" in result.messages[0].body


def test_single_sql_preserves_kst_boundaries_and_global_first_barcode(recordings_db):
    query = parse_recordings_trend_query("최근 4주 녹화 추이", now=NOW)
    summary = build_recordings_trend_report(query, now=NOW, options=RecordingsReportOptions())
    assert len(recordings_db) == 1
    sql, params = recordings_db[0]
    assert "MAX_EXECUTION_TIME(25000)" in sql and "NOT EXISTS" in sql
    # DB 바인딩은 기존 집계와 동일하게 tzinfo 없는 UTC datetime을 사용한다.
    assert params[-2:] == tuple(datetime(2026, month, day, 15, tzinfo=UTC).replace(tzinfo=None)
                                for month, day in ((8, 23), (9, 20)))
    assert [w["totalCount"] for w in summary["weeks"]] == [6, 9, 2, 0]
    assert [w["newBarcodeCount"] for w in summary["weeks"]] == [6, 3, 2, 0]
    text = format_recordings_trend(summary, now=NOW)[0]
    assert "증감 혼재" in text and "-100.0%" in text
    assert "2026-09-14 ~ 2026-09-20" in text


def test_selected_hospital_and_thresholds_apply_to_all_weeks(recordings_db):
    query = parse_recordings_trend_query("8월부터 지금까지 녹화 추이", now=NOW)
    summary = build_recordings_trend_report(
        query, now=NOW, options=RecordingsReportOptions(("B병원",), 50, 3),
    )
    assert summary["hospitalNames"] == ["B병원"]
    assert summary["totalCount"] == 3
    assert sum(w["newBarcodeCount"] for w in summary["weeks"]) == 1
    sql, params = recordings_db[-1]
    assert "AND r.hospitalSeq IN (%s)" in sql and params[-1] == 2
    assert params[-2] == NOW.astimezone(UTC).replace(tzinfo=None)
    assert "history.hospitalSeq" not in sql
    assert len(recordings_db) == 2


def test_missing_or_ambiguous_hospital_is_not_all_hospitals(recordings_db):
    with patch("boxer_company.assistant.operational_read_routes._coerce_weekly_recordings_report_now", return_value=NOW):
        result = WeeklyRecordingsSummaryAssistantRoute().handle(request("병원: 없는병원 최근 4주 녹화 추이"))
    assert result.outcome == "needs_input"
    assert len(recordings_db) == 1


def test_authenticated_api_and_slack_use_same_trend_contract(recordings_db):
    token = "t" * 48
    settings = CompanyApiSettings(host="127.0.0.1", port=8010, callers=(CompanyApiCallerSettings(
        caller_id="trend-test", token=token, tenant_ids=frozenset({"T1"}),
        channels=frozenset({"slack"}), actor_ids=frozenset({"U1"}),
        capabilities=frozenset({"assistant.turn.read"}),
    ),))
    with patch("boxer_company.assistant.factory.core_settings.LLM_PROVIDER", ""):
        runtime = create_company_assistant_runtime()
    app = create_company_api_app(settings=settings, assistant_runtime=runtime, readiness_probe=lambda: True)
    question = "병원: A병원 최근 4주 녹화 2주 이상 감소 추이"
    payload = {"tenantId": "T1", "actorId": "U1", "channel": "slack", "conversationId": "1.0",
               "question": question, "routeGroup": "structured", "locale": "ko"}
    with TestClient(app) as client, patch(
        "boxer_company.assistant.operational_read_routes._coerce_weekly_recordings_report_now", return_value=NOW,
    ):
        denied = client.post("/internal/v1/assistant/turns", json=payload,
                             headers={"X-Request-ID": "trend-http-denied"})
        response = client.post("/internal/v1/assistant/turns", json=payload,
                               headers={"Authorization": f"Bearer {token}", "X-Request-ID": "trend-http"})
        result = WeeklyRecordingsSummaryAssistantRoute().handle(request(question))
    assert denied.status_code == 401
    assert response.status_code == 200, response.text
    assert response.json()["route"] == "weekly_recordings_summary"
    assert response.json()["outcome"] == "answered" and not response.json()["usedLlm"]
    assert "**대상 병원** A병원" in result.messages[0].body
    assert "연속 감소" in result.messages[1].body
    api, next_service = Mock(), Mock()
    api.answer.return_value = result
    service = CompanyWeeklySummaryApiRolloutService(
        next_service=next_service, api_client=api, logger=logging.getLogger(__name__),
    )
    assert service.answer(request(question)) == result
    assert api.answer.call_args.kwargs["route_group"] == "structured"
    next_service.answer.assert_not_called()
    reply = Mock()
    assert render_company_assistant_result(result, reply=reply, actor_id="U1", client=None,
                                          logger=logging.getLogger(__name__)) == 2
