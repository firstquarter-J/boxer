from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import logging
import threading
from types import SimpleNamespace
from unittest.mock import Mock

from boxer_company.transport_contracts import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
)
from boxer_company_adapter_slack import device_alert_slack as alert


def _item() -> dict[str, object]:
    return {
        "hospitalSeq": 1,
        "hospitalName": "테스트병원",
        "hospital": "#1 테스트병원",
        "room": "1진료실",
        "device": "MB2-TEST1",
        "issue": "LED 이상",
        "alertCategory": "led",
        "problemComponents": ["LED"],
    }


def _second_item() -> dict[str, object]:
    return {
        **_item(),
        "room": "2진료실",
        "device": "MB2-TEST2",
        "issue": "녹화 정지",
        "alertCategory": "recording",
        "problemComponents": [],
    }


class _App:
    def __init__(self) -> None:
        self.actions: dict[str, object] = {}
        self.views: dict[str, object] = {}

    def action(self, action_id: str):  # type: ignore[no-untyped-def]
        def register(handler):  # type: ignore[no-untyped-def]
            self.actions[action_id] = handler
            return handler

        return register

    def view(self, callback_id: str):  # type: ignore[no-untyped-def]
        def register(handler):  # type: ignore[no-untyped-def]
            self.views[callback_id] = handler
            return handler

        return register


class _StatefulSlackClient:
    """Slack root를 보존해 read-merge-update 흐름을 실제 상태처럼 검증한다."""

    def __init__(
        self,
        message: dict[str, object],
        *,
        read_error: Exception | None = None,
        update_errors: tuple[Exception, ...] = (),
    ) -> None:
        self.root = {
            "ts": "1723000000.000001",
            "text": str(message.get("text") or ""),
            "blocks": deepcopy(message.get("blocks")),
        }
        self._read_error = read_error
        self._update_errors = list(update_errors)
        self.conversations_replies = Mock(side_effect=self._read_root)
        self.chat_update = Mock(side_effect=self._update_root)
        self.chat_postMessage = Mock(return_value={"ts": "1723000002.000001"})

    def _read_root(self, **_kwargs: object) -> dict[str, object]:
        if self._read_error is not None:
            raise self._read_error
        return {"messages": [deepcopy(self.root)]}

    def _update_root(self, **kwargs: object) -> dict[str, object]:
        if self._update_errors:
            raise self._update_errors.pop(0)
        self.root["text"] = str(kwargs.get("text") or "")
        self.root["blocks"] = deepcopy(kwargs.get("blocks"))
        return {"ok": True, "ts": self.root["ts"]}


def _action_body(action_id: str) -> dict[str, object]:
    return {
        "trigger_id": "TRIGGER-1",
        "team": {"id": "T1"},
        "user": {"id": "U1"},
        "channel": {"id": "C123456"},
        "message": {"ts": "1723000000.000001"},
        "actions": [
            {
                "action_id": action_id,
                "action_ts": "1723000001.000001",
                "value": json.dumps(_item(), ensure_ascii=False),
            }
        ],
    }


def _rendered_message(
    *items: dict[str, object],
    include_device_voice_action: bool = True,
) -> dict[str, object]:
    client = Mock()
    client.chat_postMessage.return_value = {"ts": "1723000000.000001"}
    client.chat_getPermalink.return_value = {"permalink": ""}
    alert.post_device_alert_summary(
        client,
        {"deviceResults": list(items)},
        channel_id="C123456",
        logger=logging.getLogger("test.alert.action-message"),
        include_actions=True,
        include_device_voice_action=include_device_voice_action,
        client_msg_id="11111111-1111-1111-1111-111111111111",
    )
    return deepcopy(client.chat_postMessage.call_args.kwargs)


def _rendered_action_body(
    message: dict[str, object],
    *,
    device: str,
    actor_id: str = "U1",
    action_ts: str = "1788134832.709819",
) -> dict[str, object]:
    blocks = message.get("blocks")
    assert isinstance(blocks, list)
    for raw_block in blocks:
        if not isinstance(raw_block, dict) or raw_block.get("type") != "actions":
            continue
        elements = raw_block.get("elements")
        if not isinstance(elements, list):
            continue
        for raw_element in elements:
            if not isinstance(raw_element, dict):
                continue
            if raw_element.get("action_id") != DEVICE_HEALTH_ALERT_MARK_DONE_ACTION:
                continue
            value = json.loads(str(raw_element.get("value") or "{}"))
            if value.get("device") != device:
                continue
            action = deepcopy(raw_element)
            action["block_id"] = raw_block["block_id"]
            action["action_ts"] = action_ts
            return {
                "trigger_id": "TRIGGER-1",
                "team": {"id": "T1"},
                "user": {"id": actor_id},
                "channel": {"id": "C123456"},
                # 서로 다른 클릭 body가 같은 원본 blocks를 갖는 Slack의 stale
                # payload 상황을 그대로 재현한다.
                "message": {
                    "ts": "1723000000.000001",
                    "text": message["text"],
                    "blocks": deepcopy(blocks),
                },
                "actions": [action],
            }
    raise AssertionError(f"{device} 확인 완료 버튼을 찾지 못했어")


def _mark_done_result(
    *,
    outcome: str = "answered",
    actor_id: str = "U1",
    acknowledged_at: str = "2026-08-31T00:07:12+00:00",
    created: bool = True,
    messages: tuple[str, ...] = ("확인 완료",),
) -> SimpleNamespace:
    return SimpleNamespace(
        outcome=outcome,
        messages=messages,
        operation_result=(
            {
                "kind": "device_health_alert_ack",
                "created": created,
                "actorUserId": actor_id,
                "acknowledgedAt": acknowledged_at,
            }
            if outcome == "answered"
            else None
        ),
    )


def _completion_rows(blocks: object) -> list[tuple[str, str]]:
    """완료 담당자와 시간을 기존 카드의 한 쌍 field로 검증한다."""

    assert isinstance(blocks, list)
    rows: list[tuple[str, str]] = []
    for block in blocks:
        if not isinstance(block, dict) or block.get("type") != "section":
            continue
        fields = block.get("fields")
        if not isinstance(fields, list):
            continue
        for index, field in enumerate(fields):
            if not isinstance(field, dict):
                continue
            status_text = str(field.get("text") or "")
            if not status_text.startswith("✅ *확인 완료*"):
                continue
            assert index + 1 < len(fields)
            time_field = fields[index + 1]
            assert isinstance(time_field, dict)
            time_text = str(time_field.get("text") or "")
            assert time_text.startswith("🕒 *처리 시간*")
            rows.append((status_text, time_text))
    return rows


def _card_action_ids(blocks: object, *, device: str) -> set[str]:
    assert isinstance(blocks, list)
    for block in blocks:
        if not isinstance(block, dict) or block.get("type") != "actions":
            continue
        elements = block.get("elements")
        if not isinstance(elements, list):
            continue
        values = [
            json.loads(str(element.get("value") or "{}"))
            for element in elements
            if isinstance(element, dict)
        ]
        if any(value.get("device") == device for value in values):
            return {
                str(element.get("action_id") or "")
                for element in elements
                if isinstance(element, dict)
            }
    return set()


def test_renderer_honors_api_voice_action_hint() -> None:
    client = Mock()
    client.chat_postMessage.return_value = {"ts": "1723000000.000001"}
    client.chat_getPermalink.return_value = {"permalink": ""}

    result = alert.post_device_alert_summary(
        client,
        {"deviceResults": [_item()]},
        channel_id="C123456",
        logger=logging.getLogger("test.alert.render"),
        include_actions=True,
        include_device_voice_action=False,
        client_msg_id="11111111-1111-1111-1111-111111111111",
    )

    assert result == {"messageTs": "1723000000.000001", "permalink": ""}
    action_ids = {
        element["action_id"]
        for block in client.chat_postMessage.call_args.kwargs["blocks"]
        if block["type"] == "actions"
        for element in block["elements"]
    }
    assert alert.DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE not in action_ids
    assert alert.DEVICE_HEALTH_ALERT_ACTION_MARK_DONE in action_ids


def test_renderer_restores_rich_card_layout_for_remote_led_alert() -> None:
    client = Mock()
    client.chat_postMessage.return_value = {"ts": "1723000000.000001"}
    client.chat_getPermalink.return_value = {"permalink": ""}
    item = {
        **_item(),
        "telephone": "031-123-4567",
        "deviceAlertPhone": "010-1234-5678",
        "mdaUrl": "https://mda.example.com/monitoring?device=MB2-TEST1",
    }

    alert.post_device_alert_summary(
        client,
        {"deviceResults": [item]},
        channel_id="C123456",
        logger=logging.getLogger("test.alert.rich-card"),
        include_actions=True,
        include_device_voice_action=False,
        client_msg_id="11111111-1111-1111-1111-111111111111",
    )

    # API 경계 뒤에도 기존 카드의 2열 정보 구조를 exact하게 고정한다.
    message = client.chat_postMessage.call_args.kwargs
    assert message["text"] == "\n".join(
        (
            ":alert: *LED 연결 확인 필요*",
            "*#1 테스트병원*",
            (
                "⚙️ *장비*  "
                "*<https://mda.example.com/monitoring?device=MB2-TEST1|MB2-TEST1>*"
                "  ·  🚪 *병실*  `1진료실`"
            ),
            "",
            ":rotating_light: *문제 장치*\n`LED`",
            "🔎 *감지 내용*\n`LED 이상`",
            "",
            "📞 *전화*\n031-123-4567",
            "💬 *문자*\n010-1234-5678",
        )
    )
    blocks = message["blocks"]
    assert [block["type"] for block in blocks] == [
        "header",
        "section",
        "section",
        "section",
        "actions",
    ]
    assert blocks[0]["text"] == {
        "type": "plain_text",
        "text": ":alert: LED 연결 확인 필요",
        "emoji": True,
    }
    assert [field["text"] for field in blocks[1]["fields"]] == [
        "⚙️ *장비*\n*<https://mda.example.com/monitoring?device=MB2-TEST1|MB2-TEST1>*",
        "🚪 *병실*\n`1진료실`",
    ]
    assert [field["text"] for field in blocks[2]["fields"]] == [
        ":rotating_light: *문제 장치*\n`LED`",
        "🔎 *감지 내용*\n`LED 이상`",
    ]
    assert [field["text"] for field in blocks[3]["fields"]] == [
        "📞 *전화*\n031-123-4567",
        "💬 *문자*\n010-1234-5678",
    ]


def test_renderer_keeps_oversized_action_value_as_valid_json() -> None:
    client = Mock()
    client.chat_postMessage.return_value = {"ts": "1723000000.000001"}
    client.chat_getPermalink.return_value = {"permalink": ""}
    item = {
        **_item(),
        "issue": "장비 오류 " * 1_000,
        "problemComponents": [f"문제 장치 {index} " * 20 for index in range(20)],
        "mdaUrl": "https://mda.example.com/" + ("path" * 1_000),
    }

    alert.post_device_alert_summary(
        client,
        {"deviceResults": [item]},
        channel_id="C123456",
        logger=logging.getLogger("test.alert.large-action"),
        include_actions=True,
        include_device_voice_action=False,
        client_msg_id="11111111-1111-1111-1111-111111111111",
    )

    # 길이 제한을 맞추더라도 버튼 value는 자르지 않은 JSON이어야 한다.
    action_block = client.chat_postMessage.call_args.kwargs["blocks"][-1]
    value = action_block["elements"][0]["value"]
    parsed = json.loads(value)
    assert len(value) <= 1900
    assert parsed["device"] == "MB2-TEST1"
    assert parsed["mdaUrl"] == ""


def test_action_is_membership_guarded_and_calls_remote_bridge_only() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.action"),
        lambda workspace_id, actor_id: (workspace_id, actor_id) == ("T1", "U1"),
        bridge,
    )
    ack = Mock()
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    body = _rendered_action_body(message, device="MB2-TEST1")

    handler = app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION]
    handler(ack, body, client)

    ack.assert_called_once_with()
    bridge.mark_done.assert_called_once()
    assert bridge.mark_done.call_args.kwargs["workspace_id"] == "T1"
    assert bridge.mark_done.call_args.kwargs["conversation_id"] == (
        "1723000000.000001"
    )
    client.conversations_replies.assert_called_once_with(
        channel="C123456",
        ts="1723000000.000001",
        limit=1,
    )
    client.chat_update.assert_called_once()
    updated_blocks = client.chat_update.call_args.kwargs["blocks"]
    assert len(updated_blocks) == len(message["blocks"])  # type: ignore[arg-type]
    assert _card_action_ids(updated_blocks, device="MB2-TEST1") == {
        alert.DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
        alert.DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
    }
    assert _completion_rows(updated_blocks) == [
        (
            "✅ *확인 완료*\n담당자 <@U1>",
            "🕒 *처리 시간*\n`2026-08-31 09:07:12 KST`",
        ),
    ]
    assert client.chat_update.call_args.kwargs["text"] == message["text"]
    assert "\n" in client.chat_update.call_args.kwargs["text"]
    client.chat_postMessage.assert_not_called()


def test_mark_done_keeps_actions_when_remote_result_is_not_answered() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result(
        outcome="failed",
        messages=("확인 완료 상태를 저장하지 못했어",),
    )
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-failed"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    body = _rendered_action_body(message, device="MB2-TEST1")

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](Mock(), body, client)

    bridge.mark_done.assert_called_once()
    client.chat_update.assert_not_called()
    assert body["message"]["blocks"] == message["blocks"]  # type: ignore[index]
    assert client.chat_postMessage.call_args.kwargs["text"] == (
        "확인 완료 상태를 저장하지 못했어"
    )


def test_mark_done_keeps_legacy_api_success_until_receipt_is_available() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = SimpleNamespace(
        outcome="answered",
        messages=("확인 완료",),
        operation_result=None,
    )
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-legacy-api"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    body = _rendered_action_body(message, device="MB2-TEST1")

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](Mock(), body, client)

    bridge.mark_done.assert_called_once()
    client.conversations_replies.assert_not_called()
    client.chat_update.assert_not_called()
    assert client.chat_postMessage.call_args.kwargs["text"] == "확인 완료"


def test_mark_done_keeps_actions_when_remote_result_is_uncertain() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.side_effect = RuntimeError("timeout")
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-uncertain"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    body = _rendered_action_body(message, device="MB2-TEST1")

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](Mock(), body, client)

    bridge.mark_done.assert_called_once()
    client.chat_update.assert_not_called()
    assert body["message"]["blocks"] == message["blocks"]  # type: ignore[index]
    assert client.chat_postMessage.call_args.kwargs["text"] == (
        "장비 이상 알림 작업 결과를 확인하지 못했어. "
        "중복 실행하지 말고 운영 로그를 확인해줘"
    )


def test_mark_done_suppresses_consecutive_clicks_for_the_same_card() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-consecutive"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    first_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134832.709819",
    )
    second_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134833.709819",
    )
    handler = app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION]
    first_ack = Mock()
    second_ack = Mock()

    handler(first_ack, first_body, client)
    handler(second_ack, second_body, client)

    first_ack.assert_called_once_with()
    second_ack.assert_called_once_with()
    bridge.mark_done.assert_called_once()
    client.conversations_replies.assert_called_once()
    client.chat_update.assert_called_once()
    client.chat_postMessage.assert_not_called()


def test_mark_done_suppresses_concurrent_clicks_for_the_same_card() -> None:
    app = _App()
    bridge = Mock()
    remote_started = threading.Event()
    release_remote = threading.Event()

    def blocking_mark_done(**_kwargs: object) -> SimpleNamespace:
        remote_started.set()
        assert release_remote.wait(timeout=3)
        return _mark_done_result()

    bridge.mark_done.side_effect = blocking_mark_done
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-concurrent"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    first_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134832.709819",
    )
    second_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134833.709819",
    )
    handler = app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION]

    with ThreadPoolExecutor(max_workers=2) as executor:
        first = executor.submit(handler, Mock(), first_body, client)
        assert remote_started.wait(timeout=2)
        second = executor.submit(handler, Mock(), second_body, client)
        try:
            # 두 번째 클릭은 첫 API 결과를 기다리지 않고 즉시 억제돼야 한다.
            second.result(timeout=1)
        finally:
            release_remote.set()
        first.result(timeout=2)

    bridge.mark_done.assert_called_once()
    client.conversations_replies.assert_called_once()
    client.chat_update.assert_called_once()
    client.chat_postMessage.assert_not_called()


def test_mark_done_preserves_prior_completion_from_stale_multi_card_body() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.side_effect = (
        _mark_done_result(),
        _mark_done_result(
            actor_id="U2",
            acknowledged_at="2026-08-31T00:08:13+00:00",
        ),
    )
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-stale-blocks"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item(), _second_item())
    client = _StatefulSlackClient(message)
    # 두 body 모두 Slack이 각 클릭 시 전달한 같은 원본 message blocks를 가진다.
    first_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        actor_id="U1",
    )
    second_body = _rendered_action_body(
        message,
        device="MB2-TEST2",
        actor_id="U2",
        action_ts="1788134893.000001",
    )
    handler = app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION]

    handler(Mock(), first_body, client)
    handler(Mock(), second_body, client)

    assert bridge.mark_done.call_count == 2
    assert client.conversations_replies.call_count == 2
    assert client.chat_update.call_count == 2
    first_blocks = client.chat_update.call_args_list[0].kwargs["blocks"]
    assert len(_completion_rows(first_blocks)) == 1
    assert sum(block["type"] == "actions" for block in first_blocks) == 2
    final_blocks = client.chat_update.call_args_list[1].kwargs["blocks"]
    assert sum(block["type"] == "actions" for block in final_blocks) == 2
    assert _card_action_ids(final_blocks, device="MB2-TEST1") == {
        alert.DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
        alert.DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
    }
    assert _card_action_ids(final_blocks, device="MB2-TEST2") == {
        alert.DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
        alert.DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
    }
    assert _completion_rows(final_blocks) == [
        (
            "✅ *확인 완료*\n담당자 <@U1>",
            "🕒 *처리 시간*\n`2026-08-31 09:07:12 KST`",
        ),
        (
            "✅ *확인 완료*\n담당자 <@U2>",
            "🕒 *처리 시간*\n`2026-08-31 09:08:13 KST`",
        ),
    ]
    assert client.root["blocks"] == final_blocks
    client.chat_postMessage.assert_not_called()


def test_mark_done_recovers_ui_after_slack_update_failure_without_new_reply() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.side_effect = (
        _mark_done_result(created=True),
        _mark_done_result(created=False),
    )
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-ui-recovery"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(
        message,
        update_errors=(RuntimeError("slack error"),),
    )
    first_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134832.709819",
    )
    second_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134833.709819",
    )
    handler = app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION]

    handler(Mock(), first_body, client)
    handler(Mock(), second_body, client)

    assert bridge.mark_done.call_count == 2
    assert client.conversations_replies.call_count == 2
    assert client.chat_update.call_count == 2
    assert _completion_rows(client.chat_update.call_args.kwargs["blocks"]) == [
        (
            "✅ *확인 완료*\n담당자 <@U1>",
            "🕒 *처리 시간*\n`2026-08-31 09:07:12 KST`",
        ),
    ]
    # API 완료 댓글은 만들지 않고 첫 Slack 갱신 실패 경고만 한 번 남긴다.
    assert [
        item.kwargs["text"] for item in client.chat_postMessage.call_args_list
    ] == [
        "확인 완료 기록은 남겼지만 카드 표시를 갱신하지 못했어. "
        "잠시 후 다시 눌러 표시만 복구해줘",
    ]


def test_mark_done_does_not_update_from_stale_body_when_root_read_fails() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-read-failed"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(
        message,
        read_error=RuntimeError("Slack read failed"),
    )
    body = _rendered_action_body(message, device="MB2-TEST1")

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](Mock(), body, client)

    bridge.mark_done.assert_called_once()
    client.conversations_replies.assert_called_once()
    client.chat_update.assert_not_called()
    assert client.root["blocks"] == message["blocks"]
    assert [
        item.kwargs["text"] for item in client.chat_postMessage.call_args_list
    ] == [
        "확인 완료 기록은 남겼지만 카드 표시를 갱신하지 못했어. "
        "잠시 후 다시 눌러 표시만 복구해줘"
    ]


def test_mark_done_uses_container_identity_when_optional_fields_are_missing() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-container"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    body = _rendered_action_body(message, device="MB2-TEST1")
    body.pop("channel")
    payload_message = body["message"]
    assert isinstance(payload_message, dict)
    payload_message.pop("ts")
    body["container"] = {
        "type": "message",
        "channel_id": "C123456",
        "message_ts": "1723000000.000001",
    }

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](Mock(), body, client)

    bridge.mark_done.assert_called_once()
    assert bridge.mark_done.call_args.kwargs["channel_id"] == "C123456"
    assert bridge.mark_done.call_args.kwargs["conversation_id"] == (
        "1723000000.000001"
    )
    client.conversations_replies.assert_called_once_with(
        channel="C123456",
        ts="1723000000.000001",
        limit=1,
    )
    client.chat_update.assert_called_once()
    client.chat_postMessage.assert_not_called()


def test_mark_done_replay_accepts_existing_status_without_slack_update() -> None:
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    stale_body = _rendered_action_body(message, device="MB2-TEST1")

    first_app = _App()
    first_bridge = Mock()
    first_bridge.mark_done.return_value = _mark_done_result(created=True)
    alert.attach_device_alert_actions(
        first_app,
        logging.getLogger("test.alert.mark-done-first"),
        lambda _workspace_id, _actor_id: True,
        first_bridge,
    )
    first_app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](
        Mock(),
        stale_body,
        client,
    )
    assert len(_completion_rows(client.root["blocks"])) == 1

    # 프로세스가 바뀐 뒤 오래된 클릭 payload가 와도 최신 root의 기존
    # status를 읽으면 같은 blocks를 다시 쓰지 않고 성공으로 끝낸다.
    client.conversations_replies.reset_mock()
    client.chat_update.reset_mock()
    client.chat_postMessage.reset_mock()
    replay_app = _App()
    replay_bridge = Mock()
    replay_bridge.mark_done.return_value = _mark_done_result(created=False)
    alert.attach_device_alert_actions(
        replay_app,
        logging.getLogger("test.alert.mark-done-replay"),
        lambda _workspace_id, _actor_id: True,
        replay_bridge,
    )

    replay_app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](
        Mock(),
        stale_body,
        client,
    )

    replay_bridge.mark_done.assert_called_once()
    client.conversations_replies.assert_called_once()
    client.chat_update.assert_not_called()
    client.chat_postMessage.assert_not_called()


def test_mark_done_updates_legacy_card_without_explicit_block_id() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-legacy-card"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    blocks = message["blocks"]
    assert isinstance(blocks, list)
    action_block = next(
        block
        for block in blocks
        if isinstance(block, dict) and block.get("type") == "actions"
    )
    action_block.pop("block_id")
    client = _StatefulSlackClient(message)

    # API 분리 전에 발송돼 명시 block_id가 없는 카드도 버튼 value가
    # 유일하면 같은 완료 UI로 안전하게 갱신한다.
    body = _action_body(DEVICE_HEALTH_ALERT_MARK_DONE_ACTION)
    elements = action_block["elements"]
    assert isinstance(elements, list)
    mark_done_button = next(
        element
        for element in elements
        if isinstance(element, dict)
        and element.get("action_id")
        == DEVICE_HEALTH_ALERT_MARK_DONE_ACTION
    )
    body["actions"][0]["value"] = mark_done_button["value"]  # type: ignore[index]
    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](Mock(), body, client)

    bridge.mark_done.assert_called_once()
    client.chat_update.assert_called_once()
    assert _completion_rows(client.root["blocks"]) == [
        (
            "✅ *확인 완료*\n담당자 <@U1>",
            "🕒 *처리 시간*\n`2026-08-31 09:07:12 KST`",
        ),
    ]
    assert len(_completion_rows(client.root["blocks"])) == 1


def test_action_fails_closed_when_membership_is_missing() -> None:
    app = _App()
    bridge = Mock()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.denied"),
        lambda _workspace_id, _actor_id: False,
        bridge,
    )
    client = Mock()

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](
        Mock(),
        _action_body(DEVICE_HEALTH_ALERT_MARK_DONE_ACTION),
        client,
    )

    bridge.assert_not_called()
    client.chat_postMessage.assert_not_called()


def test_sms_modal_uses_remote_preparation_without_local_provider() -> None:
    app = _App()
    bridge = Mock()
    bridge.prepare_sms.return_value = SimpleNamespace(
        operation_result={
            "phoneNumber": "01012345678",
            "message": "API가 준비한 안내 문자",
        }
    )
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.sms-modal"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    client = Mock()

    app.actions[alert.DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL](
        Mock(),
        _action_body(alert.DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL),
        client,
    )

    # Slack은 API가 준비한 값만 modal에 렌더링하고 DB/MDA/SMS provider를
    # 직접 호출할 수 있는 의존성을 갖지 않는다.
    bridge.prepare_sms.assert_called_once()
    bridge.record_modal_receipt.assert_called_once()
    client.views_open.assert_called_once()
    view = client.views_open.call_args.kwargs["view"]
    assert view["callback_id"] == alert.DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID
    assert "01012345678" in json.dumps(view, ensure_ascii=False)
