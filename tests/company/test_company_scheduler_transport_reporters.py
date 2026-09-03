from __future__ import annotations

from datetime import datetime
import hashlib
import logging
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

from boxer_company.automation_contracts import AutomationDelivery
from boxer_company_adapter_slack import automation_reporter
from boxer_company_adapter_slack import daily_device_round_reporter as daily
from boxer_company_adapter_slack import device_health_monitor_reporter as health
from boxer_company_adapter_slack import (
    device_notification_alert_reporter as notification,
)
from boxer_company_adapter_slack import weekly_recordings_reporter as weekly
from boxer_company_adapter_slack.automation_api_client import (
    AutomationRemoteDeliveryBatch,
)


_KST = ZoneInfo("Asia/Seoul")
_NOW = datetime(2026, 8, 10, 9, 0, tzinfo=_KST)


class _SlackClient:
    def __init__(self, *, fail_on: int | None = None) -> None:
        self.messages: list[dict[str, object]] = []
        self.fail_on = fail_on

    def chat_postMessage(self, **kwargs: object) -> dict[str, str]:
        self.messages.append(dict(kwargs))
        if self.fail_on == len(self.messages):
            raise RuntimeError("ambiguous Slack POST")
        return {"ts": f"1723000000.{len(self.messages):06d}"}

    def chat_getPermalink(self, **_kwargs: object) -> dict[str, str]:
        return {"permalink": ""}


def _batch(
    cycle: str,
    cycle_key: str,
    deliveries: tuple[AutomationDelivery, ...],
    *,
    scheduled_at: datetime = _NOW,
) -> AutomationRemoteDeliveryBatch:
    raw = "\0".join(
        ("T1", cycle, cycle_key, *sorted(item.delivery_id for item in deliveries))
    )
    return AutomationRemoteDeliveryBatch(
        batch_id="batch:" + hashlib.sha256(raw.encode()).hexdigest(),
        tenant_id="T1",
        cycle=cycle,
        cycle_key=cycle_key,
        scheduled_at=scheduled_at,
        channel_id="C123456",
        deliveries=deliveries,
    )


def _weekly_delivery() -> AutomationDelivery:
    return AutomationDelivery(
        delivery_id="weekly_recordings:2026-08-03",
        kind="weekly_recordings_report",
        payload={
            "weekStartDate": "2026-08-03",
            "weekEndDate": "2026-08-09",
            "previousWeekStartDate": "2026-07-27",
            "previousWeekEndDate": "2026-08-02",
            "hospitalCount": 1,
            "totalCount": 3,
            "previousTotalCount": 2,
            "totalDelta": 1,
            "totalChangeRate": 50.0,
            "topRows": [],
            "topRowsLimit": 10,
            "surgeRows": [],
            "surgeCount": 0,
            "dropRows": [],
            "dropCount": 0,
            "changeRowsLimit": 10,
        },
    )


def _daily_delivery(
    index: int = 1,
    *,
    run_date: str = "2026-08-10",
) -> AutomationDelivery:
    return AutomationDelivery(
        delivery_id=f"daily_device_round:{run_date}:{index}",
        kind="daily_device_round_report",
        payload={
            "runDate": run_date,
            "hospitalSeq": index,
            "hospitalName": f"테스트병원{index}",
            "deviceCount": 1,
            "scheduledDeviceCount": 1,
            "statusCounts": {"정상": 1},
            "updateCounts": {},
            "cleanupCounts": {},
            "powerCounts": {},
            "summaryLine": "정상 1",
            "messageBlocks": [],
            "fallbackText": "테스트병원 정상 1",
            "deviceResults": [],
        },
    )


def _health_delivery(index: int = 1) -> AutomationDelivery:
    return AutomationDelivery(
        delivery_id=f"device_health_monitor:led:{index}",
        kind="device_health_alert",
        payload={
            "alert": {
                "hospitalSeq": index,
                "hospitalName": f"테스트병원{index}",
                "room": "1진료실",
                "device": f"MB2-TEST{index}",
                "issue": "LED 이상",
                "telephone": "031-123-4567",
                "deviceAlertPhone": "010-1234-5678",
                "smsMessage": "렌더러로 전달하면 안 되는 자동문자 본문",
                "alertCategory": "led",
                "problemComponents": ["LED"],
            }
        },
    )


def _notification_delivery() -> AutomationDelivery:
    return AutomationDelivery(
        delivery_id="device_notification:42",
        kind="device_notification_alert",
        payload={
            "alertSummary": {
                "deviceResults": [
                    {
                        "hospitalSeq": 1,
                        "hospitalName": "테스트병원",
                        "hospitalTelephone": "031-123-4567",
                        "hospitalDeviceAlertPhone": "010-1234-5678",
                        "roomName": "1진료실",
                        "deviceName": "MB2-TEST1",
                        "priorityReason": "캡처보드 연결 이상",
                        "alertCategory": "video_signal",
                        "componentLabels": {"captureboard": "이상"},
                    }
                ]
            },
            "render": {
                "type": "device_health_abnormal_alert",
                "includeActions": True,
                "includeDeviceVoiceAction": False,
            },
        },
    )


def test_weekly_transport_ignores_removed_local_feature_gate() -> None:
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (_weekly_delivery(),))
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()

    with (
        patch.object(weekly, "flush_automation_deliveries") as flush,
        patch.object(weekly, "remember_automation_delivery") as remember,
    ):
        sent = weekly._run_weekly_recordings_report_if_due(
            client,
            logging.getLogger("test.weekly.remote"),
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    flush.assert_called_once()
    api.run.assert_not_called()
    assert len(client.messages) == 2
    remember.assert_called_once()
    assert remember.call_args.kwargs["batch"] is batch


def test_daily_transport_uses_api_presentation_without_domain_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        automation_reporter.cs,
        "AUTOMATION_DELIVERY_STATE_PATH",
        str(tmp_path / "delivery.json"),
    )
    batch = _batch("daily_device_round", "daily:2026-08-10", (_daily_delivery(),))
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()

    with (
        patch.object(daily, "flush_automation_deliveries") as flush,
        patch.object(daily, "remember_automation_delivery") as remember,
    ):
        sent = daily._run_daily_device_round_if_due(
            client,
            logging.getLogger("test.daily.remote"),
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    flush.assert_called_once()
    assert client.messages[1]["thread_ts"] == "1723000000.000001"
    remember.assert_called_once()
    assert remember.call_args.kwargs["batch"] is batch


def test_daily_transport_reuses_one_window_root_for_each_hospital(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        automation_reporter.cs,
        "AUTOMATION_DELIVERY_STATE_PATH",
        str(tmp_path / "delivery.json"),
    )
    next_now = datetime(2026, 8, 11, 9, 0, tzinfo=_KST)
    batches = (
        _batch(
            "daily_device_round",
            "daily:2026-08-10",
            (_daily_delivery(1),),
        ),
        _batch(
            "daily_device_round",
            "daily:2026-08-10",
            (_daily_delivery(2),),
        ),
        _batch(
            "daily_device_round",
            "daily:2026-08-11",
            (_daily_delivery(3, run_date="2026-08-11"),),
            scheduled_at=next_now,
        ),
    )
    api = Mock(pull_pending=Mock(side_effect=batches))
    client = _SlackClient()

    # 병원별 API batch가 이어져도 같은 window는 root 한 건만 만들고,
    # 다음 scheduler window에서만 새 root를 만든다.
    with (
        patch.object(daily, "flush_automation_deliveries"),
        patch.object(daily, "remember_automation_delivery") as remember,
    ):
        assert daily._run_daily_device_round_if_due(
            client,
            logging.getLogger("test.daily.first"),
            now=_NOW,
            automation_client=api,
        )
        assert daily._run_daily_device_round_if_due(
            client,
            logging.getLogger("test.daily.second"),
            now=_NOW,
            automation_client=api,
        )
        assert daily._run_daily_device_round_if_due(
            client,
            logging.getLogger("test.daily.next"),
            now=next_now,
            automation_client=api,
        )

    assert len(client.messages) == 5
    assert "thread_ts" not in client.messages[0]
    assert client.messages[1]["thread_ts"] == "1723000000.000001"
    assert client.messages[2]["thread_ts"] == "1723000000.000001"
    assert "thread_ts" not in client.messages[3]
    assert client.messages[4]["thread_ts"] == "1723000000.000004"
    assert remember.call_count == 3


def test_daily_root_replay_uses_window_client_id_after_ambiguous_post(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        automation_reporter.cs,
        "AUTOMATION_DELIVERY_STATE_PATH",
        str(tmp_path / "delivery.json"),
    )
    batch = _batch(
        "daily_device_round",
        "daily:2026-08-10",
        (_daily_delivery(),),
    )
    api = Mock(pull_pending=Mock(return_value=batch))
    calls: list[dict[str, object]] = []
    message_ts_by_client_id: dict[str, str] = {}

    def _post_with_ambiguous_first_response(
        **kwargs: object,
    ) -> dict[str, str]:
        calls.append(dict(kwargs))
        client_msg_id = str(kwargs["client_msg_id"])
        message_ts = message_ts_by_client_id.setdefault(
            client_msg_id,
            f"1723000000.{len(message_ts_by_client_id) + 1:06d}",
        )
        if len(calls) == 1:
            # Slack이 root를 수락한 뒤 transport 응답만 유실된 창을 재현한다.
            raise RuntimeError("ambiguous Slack POST")
        return {"ts": message_ts}

    client = Mock()
    client.chat_postMessage.side_effect = _post_with_ambiguous_first_response
    with (
        patch.object(daily, "flush_automation_deliveries", return_value=False),
        patch.object(daily, "remember_automation_delivery"),
    ):
        with pytest.raises(RuntimeError, match="ambiguous Slack POST"):
            daily._run_daily_device_round_if_due(
                client,
                logging.getLogger("test.daily.root-crash"),
                now=_NOW,
                automation_client=api,
            )
        assert daily._run_daily_device_round_if_due(
            client,
            logging.getLogger("test.daily.root-replay"),
            now=_NOW,
            automation_client=api,
        )

    assert len(calls) == 3
    assert calls[0]["client_msg_id"] == calls[1]["client_msg_id"]
    assert calls[2]["thread_ts"] == "1723000000.000001"


def test_health_transport_aggregates_api_deliveries_into_one_slack_message() -> None:
    deliveries = (_health_delivery(1), _health_delivery(2))
    batch = _batch("device_health_monitor", "continuous", deliveries)
    api = Mock(pull_pending=Mock(return_value=batch))
    posted = {"messageTs": "1723000000.000001", "permalink": ""}

    with (
        patch.object(health, "flush_automation_deliveries"),
        patch.object(
            health,
            "_post_daily_device_round_abnormal_alert",
            return_value=posted,
        ) as post,
        patch.object(health, "remember_automation_deliveries") as remember,
    ):
        sent = health._run_device_health_monitor_once(
            object(),
            logging.getLogger("test.health.remote"),
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    device_results = post.call_args.args[1]["deviceResults"]
    assert len(device_results) == 2
    # API delivery의 표시용 연락처가 Slack renderer 앞에서 유실되지 않는다.
    assert device_results[0]["telephone"] == "031-123-4567"
    assert device_results[0]["deviceAlertPhone"] == "010-1234-5678"
    assert "smsMessage" not in device_results[0]
    assert len(remember.call_args.kwargs["deliveries"]) == 2
    assert remember.call_args.kwargs["batch"] is batch


def test_notification_transport_uses_api_render_hint_and_exact_batch_receipt() -> None:
    batch = _batch(
        "device_notification_alert",
        "notification:42",
        (_notification_delivery(),),
    )
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()

    with (
        patch.object(notification, "flush_automation_deliveries"),
        patch.object(notification, "remember_automation_delivery") as remember,
    ):
        sent = notification._run_device_notification_alert_once(
            client,
            logging.getLogger("test.notification.remote"),
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    action_ids = {
        element["action_id"]
        for block in client.messages[0]["blocks"]
        if block["type"] == "actions"
        for element in block["elements"]
    }
    assert "device_health_alert_device_voice_guide" not in action_ids
    field_texts = [
        field["text"]
        for block in client.messages[0]["blocks"]
        for field in block.get("fields", [])
    ]
    assert ":rotating_light: *문제 장치*\n`캡처보드`" in field_texts
    assert "📞 *전화*\n031-123-4567" in field_texts
    assert "💬 *문자*\n010-1234-5678" in field_texts
    assert remember.call_args.kwargs["batch"] is batch


def test_video_mismatch_notification_renders_without_hospital_or_voice_actions(
) -> None:
    delivery = _notification_delivery()
    delivery.payload["alertSummary"]["deviceResults"][0].update(
        {
            "priorityReason": "영상 업로드가 확인되지 않았어",
            "alertCategory": "upload",
            "problemComponents": ["영상 업로드"],
            "barcode": "81000000000",
            "sessionAtLabel": "세션 시작(추정)",
            "sessionAt": "2026-08-14 08:50:00 KST",
        }
    )
    delivery.payload["render"].update(
        {
            "includeActions": False,
            "includeDeviceVoiceAction": False,
        }
    )
    batch = _batch(
        "device_notification_alert",
        "notification:video-mismatch",
        (delivery,),
    )
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()

    with (
        patch.object(notification, "flush_automation_deliveries"),
        patch.object(notification, "remember_automation_delivery"),
    ):
        sent = notification._run_device_notification_alert_once(
            client,
            logging.getLogger("test.notification.video-mismatch"),
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    assert all(
        block["type"] != "actions" for block in client.messages[0]["blocks"]
    )
    assert "업로드 실패 영상 감지" in client.messages[0]["text"]
    field_texts = [
        field["text"]
        for block in client.messages[0]["blocks"]
        for field in block.get("fields", [])
    ]
    assert "🏷️ *바코드*\n`81000000000`" in field_texts
    assert "🕐 *세션 시작(추정)*\n2026-08-14 08:50:00 KST" in field_texts


def test_notification_transport_rejects_non_boolean_render_hint_before_slack() -> None:
    delivery = _notification_delivery()
    delivery.payload["render"]["includeActions"] = "true"
    batch = _batch("device_notification_alert", "notification:42", (delivery,))
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()

    with patch.object(notification, "flush_automation_deliveries"):
        with pytest.raises(RuntimeError, match="render hint"):
            notification._run_device_notification_alert_once(
                client,
                logging.getLogger("test.notification.invalid"),
                now=_NOW,
                automation_client=api,
            )

    assert client.messages == []


@pytest.mark.parametrize(
    ("module", "runner", "cycle", "cycle_key", "delivery"),
    (
        (weekly, weekly._run_weekly_recordings_report_if_due, "weekly_recordings", "weekly:2026-08-03", _weekly_delivery()),
        (daily, daily._run_daily_device_round_if_due, "daily_device_round", "daily:2026-08-10", _daily_delivery()),
    ),
)
def test_thread_transport_replay_uses_same_deterministic_client_ids(
    module: object,
    runner: object,
    cycle: str,
    cycle_key: str,
    delivery: AutomationDelivery,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        automation_reporter.cs,
        "AUTOMATION_DELIVERY_STATE_PATH",
        str(tmp_path / "delivery.json"),
    )
    batch = _batch(cycle, cycle_key, (delivery,))
    api = Mock(pull_pending=Mock(return_value=batch))
    failed = _SlackClient(fail_on=2)
    replay = _SlackClient()

    with (
        patch.object(module, "flush_automation_deliveries", return_value=False),
        patch.object(module, "remember_automation_delivery"),
    ):
        with pytest.raises(RuntimeError, match="ambiguous Slack POST"):
            runner(failed, logging.getLogger("test.crash"), now=_NOW, automation_client=api)
        assert runner(replay, logging.getLogger("test.replay"), now=_NOW, automation_client=api)

    failed_ids = [item["client_msg_id"] for item in failed.messages]
    replay_ids = [item["client_msg_id"] for item in replay.messages]
    if cycle == "daily_device_round":
        # root receipt를 먼저 저장했으므로 replay는 같은 root의 실패한
        # chunk부터 재호출하며 결정적 ID도 그대로 유지한다.
        assert len(replay.messages) == 1
        assert replay.messages[0]["thread_ts"] == "1723000000.000001"
        assert failed_ids[-1] == replay_ids[0]
    else:
        assert failed_ids == replay_ids[: len(failed_ids)]
