from __future__ import annotations

import logging
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from boxer_company_adapter_slack import daily_device_round_reporter as daily
from boxer_company_adapter_slack import device_health_monitor_reporter as health
from boxer_company_adapter_slack import (
    device_notification_alert_reporter as notification,
)
from boxer_company_adapter_slack import weekly_recordings_reporter as weekly


@pytest.mark.parametrize(
    ("module", "attach", "thread_name"),
    (
        (weekly, weekly.attach_weekly_recordings_reporter, "_WEEKLY_RECORDINGS_REPORT_THREAD"),
        (daily, daily.attach_daily_device_round_reporter, "_DAILY_DEVICE_ROUND_THREAD"),
        (notification, notification.attach_device_notification_alert_reporter, "_DEVICE_NOTIFICATION_ALERT_THREAD"),
    ),
)
def test_transport_reporter_only_needs_slack_and_api_client(
    module: object,
    attach: object,
    thread_name: str,
) -> None:
    app = SimpleNamespace(client=object())
    api = Mock()
    thread = Mock()

    with (
        patch.object(module, thread_name, None),
        patch.object(module.threading, "Thread", return_value=thread) as create,
    ):
        attach(
            app,
            logger=logging.getLogger("test.transport.attach"),
            automation_client=api,
        )

    assert create.call_args.kwargs["args"][-1] is api
    thread.start.assert_called_once()


def test_transport_reporters_do_not_start_without_api_client() -> None:
    app = SimpleNamespace(client=object())

    with (
        patch.object(weekly.threading, "Thread") as weekly_thread,
        patch.object(daily.threading, "Thread") as daily_thread,
        patch.object(notification.threading, "Thread") as notification_thread,
    ):
        weekly.attach_weekly_recordings_reporter(app)
        daily.attach_daily_device_round_reporter(app)
        notification.attach_device_notification_alert_reporter(app)

    weekly_thread.assert_not_called()
    daily_thread.assert_not_called()
    notification_thread.assert_not_called()


def test_health_attach_registers_api_actions_and_transport() -> None:
    app = SimpleNamespace(client=object())
    api = Mock()
    action_bridge = Mock()
    checker = Mock()
    thread = Mock()

    with (
        patch.object(health, "_DEVICE_HEALTH_MONITOR_THREAD", None),
        patch.object(health.threading, "Thread", return_value=thread) as create,
        patch.object(
            health,
            "_attach_device_health_monitor_alert_actions",
        ) as attach_actions,
    ):
        health.attach_device_health_monitor_reporter(
            app,
            base_access_checker=checker,
            action_api_bridge=action_bridge,
            automation_client=api,
        )

    attach_actions.assert_called_once()
    assert attach_actions.call_args.args[2] is checker
    assert attach_actions.call_args.args[3] is action_bridge
    assert create.call_args.kwargs["args"][-1] is api
    thread.start.assert_called_once()


def test_health_actions_can_be_registered_for_notification_transport_only() -> None:
    app = SimpleNamespace(client=object())

    with (
        patch.object(
            health,
            "_attach_device_health_monitor_alert_actions",
        ) as attach_actions,
        patch.object(health.threading, "Thread") as create,
    ):
        health.attach_device_health_monitor_reporter(
            app,
            notification_automation_client=Mock(),
            action_api_bridge=Mock(),
        )

    attach_actions.assert_called_once()
    create.assert_not_called()
