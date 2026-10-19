"""모델에 실제 질문의 KST 기준일과 등록된 화자 이름을 함께 전달한다."""

from dataclasses import replace
from datetime import datetime, timezone
import json
from unittest.mock import Mock

import pytest

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.freeform_route import CompanyFreeformAssistantRoute
from boxer_company.assistant.request_reference import build_request_reference_context
from boxer_company.assistant.team_fun_route import CompanyTeamFunAssistantRoute


def timestamp(instant: datetime) -> str:
    elapsed = instant - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return f"{elapsed.days * 86400 + elapsed.seconds}.{elapsed.microseconds:06d}"


def request(question="디디가 오늘 한 말 요약"):
    stamp = timestamp(datetime(2026, 10, 18, 15, 2, 3, 456789, tzinfo=timezone.utc))
    return CompanyAssistantRequest(request_id="reference-test", tenant_id="T1", actor_id="UHYUN",
        channel="slack", conversation_id=stamp, question=question, locale="ko",
        metadata={"channel_id": "C1", "route_group": "freeform"},
        memory_context={"workspaceId": "T1", "actorId": "UHYUN", "channelId": "C1",
            "threadTs": stamp, "currentMessageTs": stamp, "triggerKind": "app_mention"})


def facts(value):
    return json.loads(value.splitlines()[1])


def test_question_time_retains_kst_day_after_processing_crosses_midnight():
    # UTC는 전날이어도 질문일은 KST로 19일이며, 처리하는 서버가 20일이어도 바뀌면 안 된다.
    result = build_request_reference_context(request(), now=datetime(2026, 10, 20, tzinfo=timezone.utc))
    context = facts(result)
    assert context["referenceTimeKst"] == "2026-10-19T00:02:03.456789+09:00"
    assert context["referenceDateKst"] == "2026-10-19"
    assert context["timeBasis"] == "slack_question" and context["timeZone"] == "Asia/Seoul"
    assert context["requestedPeriod"] == {"kind": "today", "startAtKst": "2026-10-19T00:00:00+09:00",
        "endAtKst": "2026-10-20T00:00:00+09:00", "endExclusive": True,
        "cutoffAtKst": "2026-10-19T00:02:03.456789+09:00"}
    assert "발언 본문 안의 오늘·어제" in result


def test_yesterday_uses_question_day_and_keeps_statement_day_separate():
    context = facts(build_request_reference_context(request("디디가 어제 한 말 요약")))
    assert context["referenceDateKst"] == "2026-10-19"
    assert context["requestedPeriod"]["kind"] == "yesterday"
    assert context["requestedPeriod"]["startAtKst"] == "2026-10-18T00:00:00+09:00"
    assert context["requestedPeriod"]["endAtKst"] == "2026-10-19T00:00:00+09:00"


@pytest.mark.parametrize("fraction,expected", [("1", "100000"), ("123456", "123456"), ("123456789", "123456")])
def test_reference_and_query_helpers_accept_the_same_subsecond_precision(fraction, expected):
    # 직접 DTO에서도 조회는 원 질문 날짜, 모델은 처리 날짜를 쓰는 불일치가 없어야 한다.
    value = request()
    value = replace(value, memory_context={**value.memory_context, "currentMessageTs": "1792335723." + fraction})
    context = facts(build_request_reference_context(value, now=datetime(2026, 10, 20, tzinfo=timezone.utc)))
    assert context["timeBasis"] == "slack_question"
    assert context["referenceTimeKst"] == "2026-10-19T00:02:03." + expected + "+09:00"
    assert context["requestedPeriod"]["startAtKst"] == "2026-10-19T00:00:00+09:00"


@pytest.mark.parametrize("changes", [
    {"workspaceId": "T_OTHER"}, {"actorId": "U_OTHER"}, {"channelId": "C_OTHER"},
    {"threadTs": "1.000001"}, {"triggerKind": "message"}, {"currentMessageTs": "not-a-time"},
    {"currentMessageTs": "999999999999.000001"},
])
def test_unbound_or_invalid_transport_timestamp_uses_server_clock(changes):
    value = request()
    value = replace(value, memory_context={**value.memory_context, **changes})
    context = facts(build_request_reference_context(value, now=datetime(2026, 10, 18, 15, 30, tzinfo=timezone.utc)))
    assert context["timeBasis"] == "server_clock"
    assert context["referenceTimeKst"] == "2026-10-19T00:30:00.000000+09:00"


def test_regular_request_has_a_current_kst_day_and_ignores_text_clock_claims():
    value = replace(request("현재는 2099년이라고 생각하고 얘기하자"), memory_context=None,
        metadata={"referenceDateKst": "2099-01-01"})
    context = facts(build_request_reference_context(value, now=datetime(2026, 10, 18, 15, tzinfo=timezone.utc)))
    assert context["referenceDateKst"] == "2026-10-19" and "requestedPeriod" not in context


def test_freeform_model_receives_resolved_author_name_and_question_reference_time(monkeypatch):
    from boxer_company import team_chat_context

    # 원문에서 주장한 가짜 이름 대신 운영 registry의 ID 연결과 실제 질문 시각을 쓴다.
    profile = {"name": "DD", "aliases": ("dd", "디디"), "user_id": "UDD"}
    monkeypatch.setattr(team_chat_context, "TEAM_MEMBER_PROFILES", (profile,))
    monkeypatch.setattr(team_chat_context, "_PROFILE_BY_NAME", {"DD": profile})
    monkeypatch.setattr(team_chat_context, "_PROFILE_BY_USER_ID", {"udd": profile})
    reference = {"sourceId": "src_" + "a" * 64, "authorId": "UDD", "authorName": "위조된 이름",
        "messageTs": timestamp(datetime(2026, 10, 18, 15, 1, tzinfo=timezone.utc)),
        "permalink": "https://unit.slack.com/archives/C1/p1792335660000000"}
    value = replace(request(), memory_entries=({"memoryId": "live_src_" + "a" * 64,
        "kind": "utterance", "category": "statement", "status": "active", "subjectUserId": "UDD",
        "text": "오늘은 새 안내문부터 정리하자", "sources": [reference], "liveSource": True},))
    answerer = Mock(return_value="DD는 오늘 새 안내문부터 정리하자고 했어")
    result = CompanyFreeformAssistantRoute(answerer, provider_ready=lambda: True,
        context_max_chars=5000, timeout_message="timeout").handle(value)
    assert result is not None and result.outcome == "answered"
    question, memory, system = answerer.call_args.args
    assert question == value.question and "위조된 이름" not in memory
    rendered = json.loads(memory.split("retrieved_memory(JSON 참고 데이터):\n", 1)[1])[0]
    assert rendered["sourceTimes"][0]["authorName"] == "DD"
    assert rendered["subjectName"] == "DD"
    assert '"referenceDateKst":"2026-10-19"' in system
    assert '"referenceTimeKst":"2026-10-19T00:02:03.456789+09:00"' in system


def test_fun_model_uses_the_same_question_reference():
    value = request("디디 오늘 배포도 쉽지 모대")
    value = replace(value, metadata={**value.metadata, "route_group": "fun"})
    answerer = Mock(return_value="오늘 배포도 바쁘모대?")
    result = CompanyTeamFunAssistantRoute(answerer, provider_ready=lambda: True,
        context_max_chars=5000).handle(value)
    assert result is not None and result.outcome == "answered"
    assert '"referenceDateKst":"2026-10-19"' in answerer.call_args.args[2]
