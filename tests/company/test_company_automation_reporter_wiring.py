from __future__ import annotations

from datetime import datetime
import logging
from types import SimpleNamespace
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

from boxer_company.automation import AutomationDelivery
from boxer_company_adapter_slack import daily_device_round_reporter as daily
from boxer_company_adapter_slack import device_health_monitor_reporter as health
from boxer_company_adapter_slack import device_notification_alert_reporter as notification
from boxer_company_adapter_slack import weekly_recordings_reporter as weekly


_NOW = datetime(2026, 8, 10, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))


class _SlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs: object) -> dict[str, str]:
        self.messages.append(dict(kwargs))
        return {"ts": f"1723000000.{len(self.messages):06d}"}


def _api_result(
    delivery: AutomationDelivery | tuple[AutomationDelivery, ...] | None,
) -> SimpleNamespace:
    deliveries = (
        delivery
        if isinstance(delivery, tuple)
        else (delivery,)
        if delivery is not None
        else ()
    )
    return SimpleNamespace(
        deliveries=deliveries,
        metrics={},
        outcome="completed" if deliveries else "no_change",
    )


def test_weekly_remote_uses_api_summary_and_never_queries_local_db() -> None:
    api = Mock()
    api.run.return_value = _api_result(
        AutomationDelivery(
            delivery_id="weekly_recordings:2026-08-03",
            kind="weekly_recordings_report",
            payload={
                "weekStartDate": "2026-08-03",
                "weekEndDate": "2026-08-09",
                "hospitalCount": 1,
                "totalCount": 3,
            },
        )
    )
    client = _SlackClient()
    logger = logging.getLogger("test.automation.weekly")

    with (
        patch.object(weekly.cs, "WEEKLY_RECORDINGS_REPORT_ENABLED", True),
        patch.object(weekly.cs, "WEEKLY_RECORDINGS_REPORT_CHANNEL_ID", "C1"),
        patch.object(weekly.cs, "WEEKLY_RECORDINGS_REPORT_HOUR_KST", 9),
        patch.object(weekly.cs, "WEEKLY_RECORDINGS_REPORT_MINUTE_KST", 0),
        patch.object(weekly.s, "DB_QUERY_ENABLED", False),
        patch.object(weekly, "_load_weekly_recordings_report_state", return_value={}),
        patch.object(weekly, "_save_weekly_recordings_report_state"),
        patch.object(weekly, "flush_automation_deliveries", return_value=False),
        patch.object(weekly, "remember_automation_delivery") as remember,
        patch.object(weekly, "_build_weekly_recordings_report_summary") as local_query,
        patch.object(weekly, "_format_weekly_recordings_report", return_value="body"),
        patch.object(weekly, "_build_weekly_recordings_report_blocks", return_value=[]),
    ):
        sent = weekly._run_weekly_recordings_report_if_due(
            client,
            logger,
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    local_query.assert_not_called()
    api.run.assert_called_once()
    remember.assert_called_once()


def test_daily_remote_renders_api_delivery_without_local_mda_or_db() -> None:
    api = Mock()
    api.run.return_value = _api_result(
        AutomationDelivery(
            delivery_id="daily_device_round:2026-08-10:1",
            kind="daily_device_round_report",
            payload={
                "runDate": "2026-08-10",
                "hospitalSeq": 1,
                "hospitalName": "테스트병원",
                "deviceCount": 1,
                "scheduledDeviceCount": 1,
                "statusCounts": {
                    "정상": 1,
                    "확인 필요": 0,
                    "이상": 0,
                    "점검 불가": 0,
                },
                "updateCounts": {
                    "agentCandidates": 0,
                    "agentUpdated": 0,
                    "agentUpdateFailed": 0,
                    "boxCandidates": 0,
                    "boxUpdated": 0,
                    "boxUpdateFailed": 0,
                },
                "cleanupCounts": {
                    "candidates": 0,
                    "executed": 0,
                    "failed": 0,
                },
                "powerCounts": {
                    "requested": 0,
                    "poweredOff": 0,
                    "alreadyOffline": 0,
                    "powerOffFailed": 0,
                },
                "summaryLine": "정상 1",
                "messageBlocks": [],
                "fallbackText": "테스트병원",
                "deviceResults": [],
            },
        )
    )
    client = _SlackClient()
    logger = logging.getLogger("test.automation.daily")

    with (
        patch.object(daily.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
        patch.object(daily.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", "C1"),
        patch.multiple(
            daily.cs,
            DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT=True,
            DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE=False,
            DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID=False,
            DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN=True,
            DAILY_DEVICE_ROUND_AUTO_POWER_OFF=False,
        ),
        patch.object(daily.s, "DB_QUERY_ENABLED", False),
        patch.object(
            daily,
            "_load_daily_device_round_state",
            return_value={
                "autoUpdateAgentOverride": False,
                "autoUpdateBoxFreeOverride": True,
                "autoUpdateBoxPaidOverride": True,
            },
        ),
        patch.object(daily, "_normalize_daily_device_round_state", side_effect=lambda state, **_: state),
        patch.object(daily, "_daily_device_round_window_key", return_value="2026-08-10"),
        patch.object(daily, "_is_daily_device_round_due", return_value=True),
        patch.object(daily, "flush_automation_deliveries", return_value=False),
        patch.object(daily, "remember_automation_delivery") as remember,
        patch.object(daily, "_save_daily_device_round_state"),
        patch.object(daily, "_remember_daily_device_round_runtime_state"),
        patch.object(daily, "_build_daily_device_round_window_title_text", return_value="title"),
        patch.object(daily, "_build_daily_device_round_report_text", return_value="body"),
        patch.object(daily, "_build_remote_daily_device_round_blocks", return_value=[]),
        patch.object(daily, "_split_daily_device_round_blocks", return_value=[[]]),
        patch.object(daily, "_build_daily_device_round_summary") as local_round,
    ):
        sent = daily._run_daily_device_round_if_due(
            client,
            logger,
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    local_round.assert_not_called()
    api.run.assert_called_once()
    assert api.run.call_args.kwargs["options"] == {
        # remote에서도 기존 Slack 제어 override가 env 기본값을 덮어쓴다.
        "autoUpdateAgent": False,
        "autoUpdateBoxFree": True,
        "autoUpdateBoxPaid": True,
        "autoCleanupTrashCan": True,
        "autoPowerOff": False,
    }
    remember.assert_called_once()


def test_health_remote_posts_semantic_alert_without_local_probe() -> None:
    api = Mock()
    api.run.return_value = _api_result(
        tuple(
            AutomationDelivery(
                delivery_id=f"device_health_monitor:{suffix}",
                kind="device_health_alert",
                payload={
                    "alert": {
                        "hospitalSeq": str(index),
                        "hospitalName": f"테스트병원{index}",
                        "telephone": f"02-0000-000{index}",
                        "deviceAlertPhone": f"010-0000-000{index}",
                        "room": f"{index}진료실",
                        "device": f"MB2-TEST{index}",
                        "issue": "캡처보드 확인 필요",
                        "problemComponents": ["캡처보드"],
                        "alertCategory": "video_signal",
                        "smsStatusText": "문자 발송 접수",
                        "smsPhoneNumber": "01012345678",
                        "smsMessage": "캡처보드 연결 상태를 확인해 주세요.",
                        "smsTemplateId": "captureboard_disconnected",
                    }
                },
            )
            for index, suffix in ((1, "abc"), (2, "def"))
        )
    )
    logger = logging.getLogger("test.automation.health")
    posted = {"messageTs": "1723000000.000001", "permalink": ""}

    with (
        patch.object(health.cs, "DEVICE_HEALTH_MONITOR_ENABLED", True),
        patch.object(health.s, "DB_QUERY_ENABLED", False),
        patch.object(health, "_load_device_health_monitor_state") as load_state,
        patch.object(health, "_normalize_device_health_monitor_state") as normalize_state,
        patch.object(health, "_resolve_device_health_monitor_alert_delivery_status") as resolve_status,
        patch.object(health, "_device_health_monitor_channel_id", return_value="C1"),
        patch.object(health, "flush_automation_deliveries", return_value=False) as flush,
        patch.object(health, "remember_automation_deliveries") as remember,
        patch.object(
            health,
            "_post_daily_device_round_abnormal_alert",
            return_value=posted,
        ) as post_alert,
        patch.object(health, "_build_device_health_monitor_summary") as local_probe,
    ):
        sent = health._run_device_health_monitor_once(
            object(),
            logger,
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    local_probe.assert_not_called()
    load_state.assert_not_called()
    normalize_state.assert_not_called()
    resolve_status.assert_not_called()
    api.run.assert_called_once()
    assert "options" not in api.run.call_args.kwargs
    assert "options" not in flush.call_args.kwargs
    remember.assert_called_once()
    remembered = remember.call_args.kwargs["deliveries"]
    assert len(remembered) == 2
    # local monitor처럼 한 poll의 장비 알림은 Slack 메시지 하나로 집계한다.
    assert len(
        post_alert.call_args.args[1]["deviceResults"]
    ) == 2
    first_result = post_alert.call_args.args[1]["deviceResults"][0]
    assert first_result["smsPhoneNumber"] == "01012345678"
    assert first_result["smsMessage"] == (
        "캡처보드 연결 상태를 확인해 주세요."
    )
    assert first_result["smsTemplateId"] == "captureboard_disconnected"


def test_notification_remote_posts_api_alert_without_local_db_or_sms() -> None:
    api = Mock()
    api.run.return_value = _api_result(
        AutomationDelivery(
            delivery_id="device_notification:10",
            kind="device_notification_alert",
            payload={
                "alertSummary": {"deviceResults": [{}]},
                "render": {
                    "includeActions": True,
                    "includeDeviceVoiceAction": True,
                },
            },
        )
    )
    logger = logging.getLogger("test.automation.notification")

    with (
        patch.object(notification.cs, "DEVICE_NOTIFICATION_ALERT_ENABLED", True),
        patch.object(notification.cs, "DEVICE_NOTIFICATION_ALERT_CHANNEL_ID", "C1"),
        patch.object(notification.s, "DB_QUERY_ENABLED", False),
        patch.object(notification, "flush_automation_deliveries", return_value=False),
        patch.object(notification, "remember_automation_delivery") as remember,
        patch.object(notification, "_post_daily_device_round_abnormal_alert", return_value={"messageTs": "1723000000.000001", "permalink": ""}),
        patch.object(notification, "_load_latest_device_notification_id") as local_db,
    ):
        sent = notification._run_device_notification_alert_once(
            object(),
            logger,
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    local_db.assert_not_called()
    api.run.assert_called_once()
    remember.assert_called_once()


def test_notification_remote_drains_legacy_batch_after_each_receipt() -> None:
    def _delivery(notification_id: int) -> AutomationDelivery:
        return AutomationDelivery(
            delivery_id=f"device_notification:{notification_id}",
            kind="device_notification_alert",
            payload={
                "alertSummary": {"deviceResults": [{}]},
                "render": {
                    "includeActions": True,
                    "includeDeviceVoiceAction": True,
                },
            },
        )

    api = Mock()
    api.run.side_effect = (
        _api_result(_delivery(10)),
        _api_result(_delivery(11)),
        _api_result(None),
    )
    logger = logging.getLogger("test.automation.notification.batch")

    with (
        patch.object(notification.cs, "DEVICE_NOTIFICATION_ALERT_ENABLED", True),
        patch.object(notification.cs, "DEVICE_NOTIFICATION_ALERT_CHANNEL_ID", "C1"),
        patch.object(notification.s, "DB_QUERY_ENABLED", False),
        patch.object(
            notification,
            "flush_automation_deliveries",
            side_effect=(False, True, True),
        ) as flush,
        patch.object(notification, "remember_automation_delivery") as remember,
        patch.object(
            notification,
            "_post_daily_device_round_abnormal_alert",
            side_effect=(
                {"messageTs": "1723000000.000010", "permalink": ""},
                {"messageTs": "1723000000.000011", "permalink": ""},
            ),
        ) as post_alert,
    ):
        sent = notification._run_device_notification_alert_once(
            object(),
            logger,
            now=_NOW,
            automation_client=api,
        )

    assert sent is True
    assert api.run.call_count == 3
    assert flush.call_count == 3
    assert post_alert.call_count == 2
    assert remember.call_count == 2
    assert len(
        {call.kwargs["request_id"] for call in api.run.call_args_list}
    ) == 3
