from __future__ import annotations

from datetime import datetime
import hashlib
import logging
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

from boxer_company.automation_contracts import AutomationDelivery
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


def _daily_delivery() -> AutomationDelivery:
    return AutomationDelivery(
        delivery_id="daily_device_round:2026-08-10:1",
        kind="daily_device_round_report",
        payload={
            "runDate": "2026-08-10",
            "hospitalSeq": 1,
            "hospitalName": "테스트병원",
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
                        "room": "1진료실",
                        "device": "MB2-TEST1",
                        "issue": "캡처보드 연결 이상",
                        "alertCategory": "video_signal",
                        "problemComponents": ["캡처보드"],
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


def test_daily_transport_uses_api_presentation_without_domain_state() -> None:
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
    assert len(post.call_args.args[1]["deviceResults"]) == 2
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
    assert remember.call_args.kwargs["batch"] is batch


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
) -> None:
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

    assert [item["client_msg_id"] for item in failed.messages] == [
        item["client_msg_id"] for item in replay.messages[: len(failed.messages)]
    ]
