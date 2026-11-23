from __future__ import annotations

import logging
from unittest.mock import Mock

import pytest

from boxer_company_adapter_slack import device_file_scope as file_scope

_CHANNEL = "CALERT"
_ROOT_TS = "1795495400.100000"
_REPLY_TS = "1795495420.100000"
_EXPECTED = file_scope.AlertDownloadScope("12345678901", "2026-11-23", "MB2-TEST1")


@pytest.fixture(autouse=True)
def _configure_alert_channel(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(file_scope.cs, "DEVICE_NOTIFICATION_ALERT_CHANNEL_ID", _CHANNEL)
    monkeypatch.setattr(file_scope, "get_timezone_label", lambda: "KST")


def _root(**content: object) -> dict:
    return {
        "type": "message",
        "ts": _ROOT_TS,
        "user": "UBOXER",
        "bot_id": "BBOXER",
        **content,
    }


def _recording_card(title: str, *, session_label: str = "세션 시작") -> dict:
    return _root(
        text="접근성 요약",
        blocks=[
            {"type": "header", "text": {"type": "plain_text", "text": title}},
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": ":gear: *장비*\n*<https://example.invalid/?other=other-target|MB2-TEST1>*"},
                {"type": "mrkdwn", "text": ":label: *바코드*\n`12345678901`"},
                {"type": "mrkdwn", "text": f"🕐 *{session_label}*\n2026-11-23 01:20:30 KST"},
            ]},
            {"type": "section", "fields": [
                {"type": "mrkdwn", "text": "📞 *전화*\n01000000000"},
                {"type": "mrkdwn", "text": "🕒 *감지 시각*\n2026-11-24 01:00:00 KST"},
            ]},
            {"type": "actions", "elements": [{"value": "MB2-OTHER2 98765432101"}]},
        ],
    )


def _client(*messages: dict) -> Mock:
    client = Mock()
    client.conversations_replies.return_value = {"messages": list(messages)}
    client.auth_test.return_value = {"ok": True, "user_id": "UBOXER", "bot_id": "BBOXER"}
    return client


def _lookup(client: Mock, **overrides: str) -> file_scope.AlertDownloadScope | None:
    return file_scope.lookup_alert_download_scope(
        client=client,
        logger=logging.getLogger(__name__),
        **{"channel_id": _CHANNEL, "thread_ts": _ROOT_TS, "current_ts": _REPLY_TS, **overrides},
    )


@pytest.mark.parametrize(
    ("title", "session_label"),
    [
        (":alert: 업로드 실패 영상 감지", "세션 시작(추정)"),
        (":white_check_mark: 확인 완료 · 녹화 영상 이상", "세션 시작"),
        (":alert: 녹화 오디오 이상 감지", "세션 시작"),
    ],
)
def test_alert_scope_uses_only_the_same_card_identity(title: str, session_label: str) -> None:
    client = _client(_recording_card(title, session_label=session_label))

    assert _lookup(client) == _EXPECTED
    client.conversations_replies.assert_called_once_with(
        channel=_CHANNEL, ts=_ROOT_TS, limit=1, inclusive=True,
    )


@pytest.mark.parametrize("session_label", ["영상 이상 감지 시각", "세션 시작(추정)"])
def test_media_alert_needs_an_actual_session_start(session_label: str) -> None:
    client = _client(_recording_card(":alert: 녹화 영상 이상 감지", session_label=session_label))

    assert _lookup(client) is None


def test_multiple_or_incomplete_identity_fields_cannot_be_combined() -> None:
    root = _recording_card(":alert: 녹화 영상 이상 감지")
    identity = root["blocks"][1]["fields"]
    identity.append(identity[1])
    assert _lookup(_client(root)) is None

    identity.pop()
    session = identity.pop()
    root["blocks"][2]["fields"].append(session)
    assert _lookup(_client(root)) is None


def test_scope_requires_a_single_root_from_the_current_bot() -> None:
    root = _recording_card(":alert: 녹화 영상 이상 감지")
    assert _lookup(_client(root, root)) is None
    assert _lookup(_client({**root, "user": "UOTHER"})) is None
    assert _lookup(_client({**root, "thread_ts": "1795495390.100000"})) is None
    assert _lookup(_client({**root, "bot_profile": {"id": "BOTHER"}})) is None

    client = _client(root)
    assert _lookup(client, current_ts=_ROOT_TS) is None
    assert _lookup(client, channel_id="COTHER") is None
    client.conversations_replies.assert_not_called()


def test_mda_alert_works_with_existing_and_download_scope_contracts() -> None:
    client = _client(_root(text=(
        "*업로드 실패한 마미박스 초음파 영상을 모두 낚았습니다!*\n"
        "바코드: [12345678901]\n촬영일: [2026-11-23:01:20:30]\n"
        "병원명: [테스트 병원]\n병실명: [테스트 병실]\n장비명: [MB2-TEST1]\n파일명: [file.mp4]"
    )))

    assert _lookup(client) == _EXPECTED
    assert file_scope.lookup_device_file_scope_from_mda_recovery_thread(
        client=client,
        logger=logging.getLogger(__name__),
        channel_id=_CHANNEL,
        thread_ts=_ROOT_TS,
        requested_barcode=_EXPECTED.barcode,
        requested_date=_EXPECTED.log_date,
    ) == [{"deviceName": "MB2-TEST1", "hospitalName": "테스트 병원", "roomName": "테스트 병실"}]
