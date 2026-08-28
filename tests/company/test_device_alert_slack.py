from __future__ import annotations

import json
import logging
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


def test_action_is_membership_guarded_and_calls_remote_bridge_only() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = SimpleNamespace(messages=("확인 완료",))
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.action"),
        lambda workspace_id, actor_id: (workspace_id, actor_id) == ("T1", "U1"),
        bridge,
    )
    client = Mock()
    ack = Mock()

    handler = app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION]
    handler(ack, _action_body(DEVICE_HEALTH_ALERT_MARK_DONE_ACTION), client)

    ack.assert_called_once_with()
    bridge.mark_done.assert_called_once()
    assert bridge.mark_done.call_args.kwargs["workspace_id"] == "T1"
    client.chat_postMessage.assert_called_once()
    assert client.chat_postMessage.call_args.kwargs["text"] == "확인 완료"


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
