from __future__ import annotations

import logging
from copy import deepcopy
from unittest.mock import Mock

import pytest
from slack_sdk.errors import SlackApiError, SlackRequestError

from boxer_adapter_slack.message_text import slack_block_visible_text
from boxer_company.recording_media_scope import resolve_recording_media_diagnosis_scope
from boxer_company_adapter_slack.assistant_bridge import build_company_assistant_request
from boxer_company_adapter_slack.company_api_client import _serialize_request
from boxer_company_adapter_slack.daily_device_round_reporter import (
    _daily_device_round_block_fallback_text,
)
from boxer_company_adapter_slack.recording_media_context import (
    load_recording_media_thread_context_entries,
    recording_media_root_visible_text,
)
from boxer_company_api.schemas import AssistantTurnInput

_ROOT_TS = "1793607878.637159"
_CURRENT_TS = "1793614278.637160"


def _root(*, completed: bool = True) -> dict:
    # 운영에서 확인한 짧은 text와 상세 block 구조를 합성 식별값으로 재현한다.
    title = "✅ 확인 완료, 녹화 영상 이상" if completed else "녹화 영상 이상 감지"
    return {
        "type": "message",
        "ts": _ROOT_TS,
        "user": "UBOXER",
        "text": "*녹화 영상 이상 알림, 장비 상태 확인 필요*",
        "metadata": {"event_payload": {"auth": "hidden-auth"}},
        "blocks": [
            {
                "type": "header",
                "block_id": "hidden-header-id",
                "text": {"type": "plain_text", "text": title},
            },
            {
                "type": "section",
                "block_id": "hidden-identity-id",
                "text": {"type": "mrkdwn", "text": "담당자 <@UOWNER> 처리 완료"},
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "⚙️ *장비*\n<https://device.invalid/monitor?auth=hidden-auth|MB2-TEST1>",
                    },
                    {"type": "mrkdwn", "text": "🚪 *병실*\n`숨길 병실`"},
                    {"type": "mrkdwn", "text": "🏷️ *바코드*\n`12345678901`"},
                    {
                        "type": "mrkdwn",
                        "text": "🕐 *세션 시작*\n2026-11-02 17:19:43 KST",
                    },
                ],
            },
            {
                "type": "section",
                "block_id": "hidden-detail-id",
                "text": {
                    "type": "mrkdwn",
                    "text": "🔎 *감지 내용*\n영상 디코딩 실패\n장비 상세 오류 없음",
                },
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "🕒 *감지 시각*\n2026-11-02 17:24:31 KST",
                    },
                ],
            },
            {
                "type": "section",
                "block_id": "hidden-contact-id",
                "fields": [
                    {"type": "mrkdwn", "text": "📞 *전화*\n01099999999"},
                    {"type": "mrkdwn", "text": "💬 *문자*\n01088888888"},
                ],
            },
            {
                "type": "actions",
                "block_id": "hidden-action-id",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "병원 문자 보내기"},
                        "value": '{"barcode":"99999999999","auth":"hidden-auth"}',
                        "url": "https://hidden.invalid",
                    },
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "장비 음성 안내"},
                        "value": "MB2-OTHER2",
                    },
                ],
            },
        ],
    }


def _round_trip(entries: list[dict]):
    payload = {
        "workspace_id": "T1",
        "user_id": "U1",
        "channel_id": "C1",
        "current_ts": _CURRENT_TS,
        "thread_ts": _ROOT_TS,
        "question": "이 영상에 어떤 이상 있어?",
        "request_log": {"request_key": "media-block-contract"},
    }
    request = build_company_assistant_request(
        payload, context_entries=entries, metadata={"route_group": "operations"}
    )
    serialized = _serialize_request(request, route_group="operations")
    restored = AssistantTurnInput.model_validate(serialized).to_company_request(
        "media-block-contract"
    )
    return serialized, restored, resolve_recording_media_diagnosis_scope(restored)


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("named_emoji", [False, True])
def test_short_fallback_blocks_survive_real_slack_to_api_contract(
    completed: bool,
    named_emoji: bool,
) -> None:
    root = _root(completed=completed)
    if named_emoji:
        # 실제 Slack 응답에서 보이는 alias 형식도 Unicode 카드와 동일하게 해석한다.
        root["blocks"][0]["text"]["text"] = (
            ":white_check_mark: " + root["blocks"][0]["text"]["text"]
        )
        for field in root["blocks"][1]["fields"]:
            field["text"] = (
                field["text"]
                .replace("⚙️", ":gear:")
                .replace("🚪", ":door:")
                .replace("🏷️", ":label:")
                .replace("🕐", ":clock1:")
            )
    assert "12345678901" not in root["text"] and "MB2-TEST1" not in root["text"]
    client, fallback = Mock(), Mock()
    client.conversations_replies.return_value = {
        "messages": [root],
        "has_more": True,
        "response_metadata": {"next_cursor": "ignored-cursor"},
    }
    entries = load_recording_media_thread_context_entries(
        client,
        logging.getLogger(__name__),
        "C1",
        _ROOT_TS,
        _CURRENT_TS,
        fallback_loader=fallback,
    )
    wire, restored, scope = _round_trip(entries)
    assert len(wire["contextEntries"]) == len(restored.context_entries) == 1
    assert restored.context_entries[0]["created_at"] == _ROOT_TS
    assert (
        scope.reason is None
        and scope.barcode == "12345678901"
        and scope.device_name == "MB2-TEST1"
    )
    assert scope.recorded_at == "2026-11-02 17:19:43"
    text = wire["contextEntries"][0]["text"]
    for hidden in (
        "UOWNER",
        "숨길 병실",
        "01099999999",
        "01088888888",
        "hidden-",
        "https://",
        "auth=",
        "MB2-OTHER2",
        "99999999999",
    ):
        assert hidden not in text
    assert "*바코드*\n`12345678901`" in text and "*세션 시작*\n2026-11-02" in text
    fallback.assert_not_called()
    client.conversations_replies.assert_called_once_with(
        channel="C1", ts=_ROOT_TS, limit=1, inclusive=True
    )
    client.conversations_history.assert_not_called()


def test_many_long_replies_cannot_push_the_media_root_out_of_api_window() -> None:
    # 과거 blind spot인 loader 12개와 API 12k 제한을 실제 직렬화까지 검증한다.
    comments = [
        {"ts": f"1793610000.{index:06d}", "user": "UCOMMENT", "text": "댓글" * 3000}
        for index in range(12)
    ]
    client = Mock()
    client.conversations_replies.return_value = {
        "messages": [_root(), *comments],
        "has_more": True,
        "response_metadata": {"next_cursor": "unused"},
    }
    entries = load_recording_media_thread_context_entries(
        client, logging.getLogger(__name__), "C1", _ROOT_TS, _CURRENT_TS
    )
    wire, _, scope = _round_trip(entries)
    assert len(wire["contextEntries"]) == 1 and scope.reason is None
    assert "댓글" not in wire["contextEntries"][0]["text"]
    client.conversations_replies.assert_called_once_with(
        channel="C1", ts=_ROOT_TS, limit=1, inclusive=True
    )


def test_plain_text_media_roots_keep_existing_context_contract() -> None:
    root = {
        "ts": _ROOT_TS,
        "user": "UBOXER",
        "text": "녹화 영상 이상\n장비 MB2-TEST1\n바코드 12345678901\n세션 시작 2026-11-02 17:19:43 KST",
    }
    client = Mock()
    client.conversations_replies.return_value = {"messages": [root]}
    entries = load_recording_media_thread_context_entries(
        client, logging.getLogger(__name__), "C1", _ROOT_TS, _CURRENT_TS
    )
    assert entries[0]["text"] == root["text"]
    assert _round_trip(entries)[2].reason is None


def test_plain_text_root_uses_visible_device_label_instead_of_hidden_link_target() -> (
    None
):
    root = {
        "ts": _ROOT_TS,
        "user": "UBOXER",
        "text": (
            "녹화 영상 이상\n장비 <https://device.invalid/?auth=hidden-auth&focusDevice=MB2-OTHER2|MB2-TEST1>\n"
            "바코드 12345678901\n세션 시작 2026-11-02 17:19:43 KST"
        ),
    }
    client = Mock()
    client.conversations_replies.return_value = {"messages": [root]}
    entries = load_recording_media_thread_context_entries(
        client, logging.getLogger(__name__), "C1", _ROOT_TS, _CURRENT_TS
    )
    wire, _, scope = _round_trip(entries)
    assert scope.reason is None and scope.device_name == "MB2-TEST1"
    assert "MB2-OTHER2" not in wire["contextEntries"][0]["text"]
    assert "hidden-auth" not in wire["contextEntries"][0]["text"]
    assert "https://" not in wire["contextEntries"][0]["text"]


@pytest.mark.parametrize("fallback_text", [None, "", 123, {"text": "hidden-original"}])
def test_empty_or_non_text_fallback_still_uses_visible_identity_blocks(
    fallback_text,
) -> None:
    root = _root()
    root["text"] = fallback_text
    client = Mock()
    client.conversations_replies.return_value = {"messages": [root]}
    entries = load_recording_media_thread_context_entries(
        client, logging.getLogger(__name__), "C1", _ROOT_TS, _CURRENT_TS
    )
    assert _round_trip(entries)[2].barcode == "12345678901"
    assert "hidden-original" not in entries[0]["text"]


def test_unrelated_roots_delegate_to_the_unchanged_context_loader() -> None:
    client, fallback = Mock(), Mock(return_value=[{"text": "기존 일반 문맥"}])
    client.conversations_replies.return_value = {
        "messages": [{"ts": _ROOT_TS, "text": "일반 대화"}]
    }
    assert load_recording_media_thread_context_entries(
        client,
        logging.getLogger(__name__),
        "C1",
        _ROOT_TS,
        _CURRENT_TS,
        fallback_loader=fallback,
    ) == [{"text": "기존 일반 문맥"}]
    fallback.assert_called_once_with(
        client, logging.getLogger(__name__), "C1", _ROOT_TS, _CURRENT_TS
    )


@pytest.mark.parametrize(
    "error",
    [
        SlackApiError("synthetic failure", {"ok": False, "error": "missing_scope"}),
        SlackRequestError("synthetic connection failure"),
    ],
)
def test_slack_read_error_uses_the_existing_context_fallback(error: Exception) -> None:
    client, fallback = Mock(), Mock(return_value=[])
    client.conversations_replies.side_effect = error
    assert (
        load_recording_media_thread_context_entries(
            client,
            logging.getLogger(__name__),
            "C1",
            _ROOT_TS,
            _CURRENT_TS,
            fallback_loader=fallback,
        )
        == []
    )
    fallback.assert_called_once()


@pytest.mark.parametrize(
    "blocks",
    [
        None,
        "invalid",
        [
            None,
            "invalid",
            {"type": "header", "text": 123},
            {"type": "section", "fields": [123, {"text": []}]},
        ],
    ],
)
def test_non_text_blocks_do_not_promote_hidden_values_to_scope(blocks) -> None:
    message = {"text": None, "blocks": blocks, "metadata": {"barcode": "99999999999"}}
    assert recording_media_root_visible_text(message) is None


def test_action_values_and_links_do_not_supply_missing_identity_fields() -> None:
    root = _root()
    root["blocks"][1]["fields"] = []
    text = recording_media_root_visible_text(root)
    assert text == root["text"]
    assert "99999999999" not in text and "MB2-OTHER2" not in text


@pytest.mark.parametrize(
    "thread,current",
    [
        ("bad", _CURRENT_TS),
        (_ROOT_TS, "bad"),
        (_CURRENT_TS, _ROOT_TS),
        (_ROOT_TS, _ROOT_TS),
    ],
)
def test_invalid_or_non_prior_roots_are_not_fetched(thread: str, current: str) -> None:
    client = Mock()
    assert (
        load_recording_media_thread_context_entries(
            client, logging.getLogger(__name__), "C1", thread, current
        )
        == []
    )
    client.conversations_replies.assert_not_called()


def test_old_daily_fallback_name_uses_the_shared_exact_display_renderer() -> None:
    # 기존 import 이름과 rich-text 문장/필드 줄 구분을 유지한다.
    assert _daily_device_round_block_fallback_text is slack_block_visible_text
    block = {
        "type": "section",
        "text": {"text": "제목"},
        "fields": [
            {"text": "첫째"},
            {"type": "rich_text_section", "elements": [{"text": "둘"}, {"text": "째"}]},
        ],
    }
    assert (
        _daily_device_round_block_fallback_text(deepcopy(block)) == "제목\n첫째\n둘째"
    )
