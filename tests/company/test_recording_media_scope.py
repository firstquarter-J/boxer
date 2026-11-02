"""Slack 카드의 표시 field와 원본 thread identity만 진단 범위 힌트로 사용한다."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from boxer_company.recording_media_scope import (
    is_recording_media_alert_text,
    resolve_recording_media_diagnosis_scope,
)

_ROOT_TS = "1793614260.000001"
_CARD = """✅ 확인 완료, 녹화 영상 이상
담당자 <@USER1>, `2026-11-03 00:01:00 KST`
🏥 *127 합성병원*
⚙️ *장비*
*<https://mda.kr.mmtalkbox.com/monitoring?hospitalSeq=127&focusDevice=MB2-X00001|MB2-X00001>*
🚪 *병실*
`합성 진료실`
🏷️ *바코드*
`81000000000`
🕐 *세션 시작*
2026-11-02 19:11:00 KST
🔎 *감지 내용*
영상 디코딩 실패
📞 *전화*
01011112222
💬 *문자*
01033334444"""


def _request(*texts: str, question: str = "영상 어떤 이상 있어?", rooted: bool = False):
    entries = tuple(
        {"kind": "message", "source": "slack", "text": text,
         **({"created_at": _ROOT_TS if index == 0 else f"1793614261.{index:06d}"} if rooted else {})}
        for index, text in enumerate(texts)
    )
    request = SimpleNamespace(question=question, metadata={"route_group": "operations"},
                              context_entries=entries)
    if rooted:
        request.conversation_id = _ROOT_TS
    return request


def _assert_card_scope(scope) -> None:
    assert scope.reason is None
    assert scope.barcode == "81000000000"
    assert scope.device_name == "MB2-X00001"
    assert scope.log_date == "2026-11-02"
    assert scope.recorded_at == "2026-11-02 19:11:00"


@pytest.mark.parametrize("rooted", [False, True])
def test_card_label_excludes_two_phone_numbers_links_and_completion_date(rooted) -> None:
    # 연락처 두 개도 11자리지만 바코드 field와 다음 날의 완료 시각을 섞지 않는다.
    _assert_card_scope(resolve_recording_media_diagnosis_scope(_request(_CARD, rooted=rooted)))


@pytest.mark.parametrize("field", [
    "바코드: 81000000000", "*바코드*\n*81000000000*",
    "**바코드**\n**81000000000**", ":label: *바코드*\n`81000000000`",
])
def test_card_label_supports_existing_plain_and_slack_markdown_field_styles(field) -> None:
    card = _CARD.replace("🏷️ *바코드*\n`81000000000`", field)
    _assert_card_scope(resolve_recording_media_diagnosis_scope(_request(card)))


@pytest.mark.parametrize("value", [
    "", "미확인", "8100000000", "810000000000", "81000000000 81000000001",
    "*81000000000**", "８１０００００００００",
])
def test_invalid_card_barcode_cannot_fall_back_to_phone_or_comment(value) -> None:
    card = _CARD.replace("`81000000000`", value)
    result = resolve_recording_media_diagnosis_scope(_request(
        card, "영상 디코딩 실패, 바코드 81000000000", rooted=True,
    ))
    assert result.reason == "invalid_barcode"


@pytest.mark.parametrize("other", ["81000000000", "81000000001"])
def test_multiple_barcode_labels_are_ambiguous_even_when_values_are_identical(other) -> None:
    card = _CARD + f"\n🏷️ *바코드*\n`{other}`"
    assert resolve_recording_media_diagnosis_scope(_request(card, rooted=True)).reason == "ambiguous_scope"


@pytest.mark.parametrize("comment", [
    "MB2-X00001 영상 디코딩 실패라는데 정상으로 보여요",
    "81000000000 영상 디코딩 실패라는데 정상으로 보여요",
    "MB2-X00002 바코드 81000000001 영상 디코딩 실패 알림도 있었어요",
    "영상 디코딩 실패, 알림 번호 0",
])
def test_identified_thread_root_cannot_be_replaced_or_mixed_with_comments(comment) -> None:
    _assert_card_scope(resolve_recording_media_diagnosis_scope(_request(_CARD, comment, rooted=True)))


@pytest.mark.parametrize("comment", [
    "MB2-X00001 영상 디코딩 실패라는데 정상으로 보여요",
    "81000000000 영상 디코딩 실패라는데 정상으로 보여요",
])
def test_legacy_context_ignores_only_partial_comments_consistent_with_one_full_card(comment) -> None:
    # 예전 호출처럼 conversation_id와 created_at이 없어도 동일 대상만 합의한다.
    _assert_card_scope(resolve_recording_media_diagnosis_scope(_request(_CARD, comment)))


@pytest.mark.parametrize("comment", [
    "MB2-X00002 영상 디코딩 실패라는데 정상으로 보여요",
    "81000000001 영상 디코딩 실패라는데 정상으로 보여요",
    "영상 디코딩 실패, 알림 번호 0",
])
def test_legacy_context_keeps_conflicting_or_invalid_comment_scopes_closed(comment) -> None:
    assert resolve_recording_media_diagnosis_scope(_request(_CARD, comment)).reason is not None


@pytest.mark.parametrize("rooted", [False, True])
def test_two_genuine_cards_or_sessions_remain_ambiguous(rooted) -> None:
    second = _CARD.replace("19:11:00", "18:11:00")
    # root 안에 카드 두 개가 있으면 댓글 우선순위로 어느 한쪽을 고르지 않는다.
    assert resolve_recording_media_diagnosis_scope(_request(_CARD + "\n" + second, rooted=rooted)).reason == "ambiguous_scope"
    if not rooted:
        assert resolve_recording_media_diagnosis_scope(_request(_CARD, second)).reason == "ambiguous_scope"


def test_root_priority_preserves_explicit_current_question_and_date_conflict_rules() -> None:
    explicit = resolve_recording_media_diagnosis_scope(_request(
        _CARD, rooted=True, question="81000000001 영상 디코딩 오류 확인",
    ))
    assert explicit.reason is None and explicit.barcode == "81000000001"
    assert explicit.device_name is None and explicit.recorded_at is None
    invalid = resolve_recording_media_diagnosis_scope(_request(
        _CARD, rooted=True, question="알림 번호 0 영상 디코딩 오류 확인",
    ))
    assert invalid.reason == "invalid_notification_id"
    conflict = resolve_recording_media_diagnosis_scope(_request(
        _CARD, rooted=True, question="2026-11-01 영상 디코딩 오류 확인",
    ))
    assert conflict.reason == "scope_mismatch"


def test_duplicate_root_identity_is_not_silently_deduplicated() -> None:
    request = _request(_CARD, _CARD, rooted=True)
    request.context_entries[1]["created_at"] = _ROOT_TS
    assert resolve_recording_media_diagnosis_scope(request).reason == "ambiguous_scope"


def test_adapter_and_scope_share_provider_free_alert_title_detection() -> None:
    assert is_recording_media_alert_text(_CARD)
    assert is_recording_media_alert_text("영상 디코딩 실패")
    assert not is_recording_media_alert_text("평범한 장비 상태 댓글")
