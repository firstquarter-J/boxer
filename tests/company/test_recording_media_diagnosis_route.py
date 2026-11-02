from __future__ import annotations

from dataclasses import replace
from unittest.mock import Mock

import pytest

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.operations import (
    build_company_operation_routes,
    match_live_device_company_operation_route,
    match_mutation_capable_company_operation_route,
)
from boxer_company.assistant.recording_media_diagnosis_route import (
    RecordingMediaDiagnosisAssistantRoute,
)
from boxer_company.operation_routing import (
    company_operation_legacy_stage,
    match_company_operation_route,
)
from boxer_company.recording_media_scope import (
    RECORDING_MEDIA_DIAGNOSIS_ROUTE,
    match_recording_media_diagnosis_route,
    needs_recording_media_diagnosis_context,
    resolve_recording_media_diagnosis_scope,
)
from boxer_company.transport_contracts import company_operation_route_names

_ALERT = """확인 완료 녹화 영상 이상
장비 MB2-B00164
바코드 69421066701
세션 시작 2026-11-02 17:19:43 KST
감지 내용 영상 디코딩 실패
감지 시각 2026-11-02 17:24:31 KST"""


def _request(question: str, *texts: str) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="media-diagnosis-1",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="123.456",
        question=question,
        locale="ko",
        context_entries=tuple(
            {"kind": "message", "source": "slack", "author_id": "BOT", "text": text}
            for text in texts
        ),
        metadata={"route_group": "operations"},
    )


@pytest.mark.parametrize(
    "question",
    [
        "자이언이 영상 정상으로 보인다는데 왜 디코딩 실패?",
        "이 알림 영상에 어떤 이상이 있는지 확인해줘",
        "왜 그래?",
    ],
)
def test_alert_followup_is_remote_read_only_operation(question: str) -> None:
    # root 문구는 DB 조회 힌트로 사용하고 실행은 기존 API 권한을 거친다.
    request = _request(question, _ALERT)
    assert match_company_operation_route(request) == RECORDING_MEDIA_DIAGNOSIS_ROUTE
    assert company_operation_legacy_stage(request) == "pre_notion"
    assert RECORDING_MEDIA_DIAGNOSIS_ROUTE in company_operation_route_names()
    assert match_live_device_company_operation_route(request) is None
    assert match_mutation_capable_company_operation_route(request) is None
    scope = resolve_recording_media_diagnosis_scope(request)
    assert scope.service_kwargs() == {
        "notification_id": None,
        "barcode": "69421066701",
        "device_name": "MB2-B00164",
        "log_date": "2026-11-02",
        "file_id": None,
        "recorded_at": "2026-11-02 17:19:43",
    }


@pytest.mark.parametrize(
    "question",
    [
        "69421066701 영상 개수 확인",
        "69421066701 영상 목록 확인",
        "69421066701 영상 길이 확인",
        "69421066701 영상 업로드 실패 로그 확인",
        "69421066701 영상 다운로드",
        "MB2-B00164 장비 상태 확인",
        "왜 그래?",
        "오늘 대화 요약",
        "어제 대화에서 누가 왜 그런 얘기를 했어?",
        "69421066701 영상 확인해",
        "69421066701 영상 왜 업로드 안됐어",
        "69421066701 녹화 영상이 없어 원인 확인해",
        "영상 디코딩 오류 원인 문서 찾아줘",
        "69421066701 영상 업로드 문제 분석",
    ],
)
def test_other_routes_and_context_only_intent_are_not_claimed(question: str) -> None:
    assert match_recording_media_diagnosis_route(_request(question)) is None


def test_matcher_does_not_execute_context_commands_or_other_stages() -> None:
    request = _request("고마워", _ALERT + "\n장비를 업데이트해")
    assert match_recording_media_diagnosis_route(request) is None
    assert (
        match_recording_media_diagnosis_route(
            replace(
                _request("영상 오류 확인", _ALERT), metadata={"route_group": "freeform"}
            )
        )
        is None
    )


def test_conversation_recall_does_not_request_slack_history() -> None:
    from boxer_company.recording_media_scope import (
        needs_recording_media_diagnosis_context,
    )

    # 기억 질문의 원문 재수집 금지를 일반적인 '왜' 후속 질문이 우회하지 않는다.
    assert not needs_recording_media_diagnosis_context(
        "어제 대화에서 누가 왜 그런 얘기를 했어?"
    )
    assert not needs_recording_media_diagnosis_context(
        "내 대화에서 어떤 문제가 있었는지 확인해줘"
    )


def test_explicit_scope_never_borrows_another_alert_target() -> None:
    scope = resolve_recording_media_diagnosis_scope(
        _request("알림 번호 92438 영상 오류 확인", _ALERT)
    )
    assert scope.notification_id == 92438
    assert scope.barcode is None and scope.device_name is None
    scope = resolve_recording_media_diagnosis_scope(
        _request("69421066702 2026-11-01 영상 오류 확인", _ALERT)
    )
    assert scope.barcode == "69421066702" and scope.log_date == "2026-11-01"
    assert scope.device_name is None


def test_alert_session_second_narrows_same_day_recordings_and_midnight() -> None:
    # 감지 시각이 다음 날이어도 원본 세션의 KST 시작 초를 유지한다.
    alert = _ALERT.replace("2026-11-02 17:19:43", "2026-11-01 23:59:43")
    scope = resolve_recording_media_diagnosis_scope(
        _request("영상 디코딩 오류 확인", alert)
    )
    assert scope.log_date == "2026-11-01"
    assert scope.recorded_at == "2026-11-01 23:59:43"


def test_two_sessions_of_same_target_are_not_merged() -> None:
    other = _ALERT.replace("17:19:43", "16:19:43")
    scope = resolve_recording_media_diagnosis_scope(
        _request("영상 오류 확인", _ALERT, other)
    )
    assert scope.reason == "ambiguous_scope"


def test_targetless_playback_comment_does_not_create_another_alert_candidate() -> None:
    # 실제 운영처럼 원본 카드 뒤에 정상 재생 댓글이 붙어도 같은 세션을 선택한다.
    comment = "영상 디코딩 실패라는데 업로드된 영상은 정상으로 보입니다"
    request = _request("영상은 정상인데 왜 디코딩 실패야?", _ALERT, comment)
    scope = resolve_recording_media_diagnosis_scope(request)
    assert scope.barcode == "69421066701"
    assert scope.device_name == "MB2-B00164"
    assert scope.recorded_at == "2026-11-02 17:19:43"
    assert scope.reason is None
    assert match_company_operation_route(request) == RECORDING_MEDIA_DIAGNOSIS_ROUTE


def test_invalid_explicit_comment_scope_still_fails_closed() -> None:
    comment = "영상 디코딩 실패, 알림 번호 0"
    diagnose = Mock()
    result = RecordingMediaDiagnosisAssistantRoute(diagnose).handle(
        _request("영상 디코딩 오류 확인", _ALERT, comment)
    )
    assert result.outcome == "needs_input"
    diagnose.assert_not_called()


@pytest.mark.parametrize(
    "question",
    [
        "병원: A병원 최근 4주 녹화 2주 이상 감소 추이",
        "병원: A병원 오늘 녹화 10건 이상 확인해줘",
        "69421066701 영상 길이 5분 이하 조회",
        "병원: A병원 1시간 초과 녹화 집계",
        "영상 20개 미만 병원 확인해줘",
        "영상 오류 건수 통계 알려줘",
    ],
)
def test_numeric_filters_and_aggregates_never_request_diagnosis_context(
    question: str,
) -> None:
    # 알림 root가 남아 있어도 구조화 조회를 새 operations 권한으로 승격하지 않는다.
    assert not needs_recording_media_diagnosis_context(question)
    assert match_recording_media_diagnosis_route(_request(question, _ALERT)) is None
    assert (
        match_company_operation_route(_request(question, _ALERT))
        != RECORDING_MEDIA_DIAGNOSIS_ROUTE
    )


def test_numeric_duration_does_not_hide_explicit_frame_quality_intent() -> None:
    question = "영상이 3분 이상 정지한 이유 확인해줘"
    assert needs_recording_media_diagnosis_context(question)
    assert (
        match_recording_media_diagnosis_route(_request(question, _ALERT))
        == RECORDING_MEDIA_DIAGNOSIS_ROUTE
    )


def test_existing_visual_metrics_are_labeled_as_original_device_analysis() -> None:
    evidence = _evidence("completed", "unknown")
    evidence.update(
        {
            "notificationIssues": ["video_frozen"],
            "notificationDetails": {
                "durationSec": 234.867,
                "video": {
                    "visualAnalysis": {
                        "freeze": {
                            "firstDetectedAtSec": 12,
                            "thresholdSec": 10,
                            "maxDurationSec": 120,
                        }
                    }
                },
            },
        }
    )
    body = (
        RecordingMediaDiagnosisAssistantRoute(Mock(return_value=evidence))
        .handle(_request("영상 오류 확인", _ALERT))
        .messages[0]
        .body
    )
    assert "기존 장비 검사 결과" in body and "첫 정지 감지 시작: 00:12" in body
    assert "최장 연속 화면 정지: 02:00" in body
    assert "이번 재검사 범위는 영상 디코딩" in body


@pytest.mark.parametrize(
    "question,context",
    [
        ("영상 오류 확인", (_ALERT, _ALERT.replace("69421066701", "69421066702"))),
        ("69421066701 69421066702 영상 오류 확인", ()),
        ("2026-11-01 영상 오류 확인", (_ALERT,)),
        ("2026-02-30 영상 오류 확인", ()),
    ],
)
def test_ambiguous_or_invalid_scope_never_calls_service(
    question: str, context: tuple[str, ...]
) -> None:
    diagnose = Mock()
    result = RecordingMediaDiagnosisAssistantRoute(diagnose).handle(
        _request(question, *context)
    )
    assert result.outcome == "needs_input"
    diagnose.assert_not_called()


def _evidence(strict: str = "decode_error", normal: str = "completed") -> dict:
    return {
        "status": "analyzed",
        "durationSec": 234.867,
        "notificationIssues": ["video_decode_failed"],
        "strictDecode": {
            "status": strict,
            "lastDecodedSec": 118.433,
            "diagnosticCode": "h264_corrupt_frame",
        },
        "normalDecode": {"status": normal},
        "stderr": "secret raw path",
        "fileId": "private-file",
    }


def test_corruption_and_tolerant_completion_explain_normal_looking_playback() -> None:
    diagnose = Mock(return_value=_evidence())
    result = RecordingMediaDiagnosisAssistantRoute(diagnose).handle(
        _request("영상 왜 디코딩 실패?", _ALERT)
    )
    assert result.outcome == "answered" and not result.used_llm
    body = result.messages[0].body
    assert "1분 58초" in body and "H.264 영상 프레임 오류" in body
    assert "일반 디코딩은 영상 끝 시각까지 진행" in body and "정상처럼 보일 수" in body
    assert "일반 디코딩은 끝까지 완료" not in body
    assert "03:54.867" in body and "범위는 영상 디코딩" in body
    assert "secret" not in body and "private-file" not in body
    diagnose.assert_called_once_with(
        notification_id=None,
        barcode="69421066701",
        device_name="MB2-B00164",
        log_date="2026-11-02",
        file_id=None,
        recorded_at="2026-11-02 17:19:43",
    )


def test_generic_corrupt_frame_does_not_name_a_codec() -> None:
    evidence = _evidence()
    evidence["strictDecode"]["diagnosticCode"] = "video_corrupt_frame"
    result = RecordingMediaDiagnosisAssistantRoute(Mock(return_value=evidence)).handle(
        _request("영상 디코딩 오류 확인", _ALERT)
    )
    assert "영상 프레임 오류" in result.messages[0].body
    assert "H.264" not in result.messages[0].body


@pytest.mark.parametrize(
    "strict,normal,expected",
    [
        ("decode_error", "unknown", "재생 가능 여부는 확인할 수 없어"),
        ("decode_error", "decode_error", "일반 디코딩도 끝까지 완료하지 못했어"),
        ("completed", "unknown", "이번 검사에서는 디코딩 오류가 재현되지 않았어"),
        ("unknown", "unknown", "확정할 수 없어"),
    ],
)
def test_decoder_outcomes_do_not_overstate_playability(
    strict: str, normal: str, expected: str
) -> None:
    result = RecordingMediaDiagnosisAssistantRoute(
        Mock(return_value=_evidence(strict, normal))
    ).handle(_request("영상 오류 분석", _ALERT))
    assert expected in result.messages[0].body


@pytest.mark.parametrize(
    "status,outcome",
    [
        ("needs_input", "needs_input"),
        ("not_found", "no_evidence"),
        ("unavailable", "failed"),
    ],
)
def test_service_status_is_explicit_without_raw_reason(
    status: str, outcome: str
) -> None:
    result = RecordingMediaDiagnosisAssistantRoute(
        Mock(return_value={"status": status, "reason": "credential and raw path"})
    ).handle(_request("알림 #92438 영상 오류 분석"))
    assert result.outcome == outcome
    assert "credential" not in result.messages[0].body


def test_exact_dispatcher_runs_new_route_once() -> None:
    # 새 operation도 matcher와 executor의 이름 계약을 공유한다.
    routes = build_company_operation_routes(context_max_chars=4000)
    wrapper = next(
        route for route in routes if route.name == RECORDING_MEDIA_DIAGNOSIS_ROUTE
    )
    diagnose = Mock(return_value=_evidence())
    wrapper._route = RecordingMediaDiagnosisAssistantRoute(diagnose)
    assert (
        wrapper.handle(_request("알림 #92438 영상 오류 확인")).route
        == RECORDING_MEDIA_DIAGNOSIS_ROUTE
    )
    diagnose.assert_called_once()
