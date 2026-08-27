from __future__ import annotations

from datetime import datetime
import hashlib
import logging
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

from boxer_company.automation import AutomationDelivery
from boxer_company_adapter_slack import daily_device_round_reporter as daily
from boxer_company_adapter_slack import device_health_monitor_reporter as health
from boxer_company_adapter_slack import (
    device_notification_alert_reporter as notification,
)
from boxer_company_adapter_slack import weekly_recordings_reporter as weekly
from boxer_company_adapter_slack.automation_api_client import (
    AutomationRemoteDeliveryBatch,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
)


_KST = ZoneInfo("Asia/Seoul")
_POLL_NOW = datetime(2026, 8, 11, 12, 0, tzinfo=_KST)


class _SlackClient:
    def __init__(self) -> None:
        self.messages: list[dict[str, object]] = []

    def chat_postMessage(self, **kwargs: object) -> dict[str, str]:
        self.messages.append(dict(kwargs))
        return {"ts": f"1723000000.{len(self.messages):06d}"}


class _FailingSlackClient(_SlackClient):
    def __init__(self, *, fail_on_call: int) -> None:
        super().__init__()
        self._fail_on_call = fail_on_call

    def chat_postMessage(self, **kwargs: object) -> dict[str, str]:
        self.messages.append(dict(kwargs))
        if len(self.messages) == self._fail_on_call:
            raise RuntimeError("ambiguous Slack POST")
        return {"ts": f"1723000000.{len(self.messages):06d}"}


def _batch(
    *,
    cycle: str,
    cycle_key: str,
    scheduled_at: datetime,
    delivery: AutomationDelivery,
    channel_id: str = "C123456",
    conversation: dict[str, object] | None = None,
) -> AutomationRemoteDeliveryBatch:
    """실제 pull client와 같은 deterministic batch identity를 만든다."""

    raw = "\0".join(("T1", cycle, cycle_key, delivery.delivery_id))
    return AutomationRemoteDeliveryBatch(
        batch_id="batch:" + hashlib.sha256(raw.encode("utf-8")).hexdigest(),
        tenant_id="T1",
        cycle=cycle,
        cycle_key=cycle_key,
        scheduled_at=scheduled_at,
        channel_id=channel_id,
        conversation=conversation or {},
        deliveries=(delivery,),
    )


def _weekly_payload() -> dict[str, object]:
    return {
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
    }


def _daily_payload() -> dict[str, object]:
    return {
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
    }


def test_weekly_scheduler_mode_only_flushes_pulls_and_delivers_batch() -> None:
    scheduled_at = datetime(2026, 8, 10, 9, 0, tzinfo=_KST)
    batch = _batch(
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=scheduled_at,
        delivery=AutomationDelivery(
            delivery_id="weekly_recordings:2026-08-03",
            kind="weekly_recordings_report",
            payload=_weekly_payload(),
        ),
    )
    events: list[str] = []
    api = Mock()
    api.pull_pending.side_effect = lambda **_kwargs: (
        events.append("pull") or batch
    )
    client = _SlackClient()

    with (
        patch.object(weekly.cs, "WEEKLY_RECORDINGS_REPORT_ENABLED", True),
        patch.object(
            weekly.cs,
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
            True,
        ),
        patch.object(weekly.cs, "WEEKLY_RECORDINGS_REPORT_CHANNEL_ID", ""),
        patch.object(weekly.s, "DB_QUERY_ENABLED", False),
        patch.object(
            weekly,
            "flush_automation_deliveries",
            side_effect=lambda *_args, **_kwargs: (
                events.append("flush") or False
            ),
        ),
        patch.object(
            weekly,
            "_load_weekly_recordings_report_state",
            side_effect=AssertionError("local state must not be loaded"),
        ),
        patch.object(
            weekly,
            "_is_weekly_recordings_report_due",
            side_effect=AssertionError("local due planner must not run"),
        ),
        patch.object(
            weekly,
            "_build_weekly_recordings_report_summary",
            side_effect=AssertionError("local DB must not be queried"),
        ),
        patch.object(
            weekly,
            "_format_weekly_recordings_report",
            return_value="body",
        ) as format_report,
        patch.object(
            weekly,
            "_build_weekly_recordings_report_blocks",
            return_value=[],
        ),
        patch.object(weekly, "remember_automation_delivery") as remember,
    ):
        sent = weekly._run_weekly_recordings_report_if_due(
            client,
            logging.getLogger("test.transport.weekly"),
            now=_POLL_NOW,
            automation_client=api,
        )

    assert sent is True
    assert events == ["flush", "pull"]
    api.run.assert_not_called()
    api.pull_pending.assert_called_once()
    assert [item["channel"] for item in client.messages] == [
        "C123456",
        "C123456",
    ]
    assert format_report.call_args.kwargs["now"] == scheduled_at
    assert remember.call_args.kwargs["batch"] == batch


def test_weekly_report_crash_replays_same_title_and_report_client_ids() -> None:
    batch = _batch(
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=datetime(2026, 8, 10, 9, 0, tzinfo=_KST),
        delivery=AutomationDelivery(
            delivery_id="weekly_recordings:2026-08-03",
            kind="weekly_recordings_report",
            payload=_weekly_payload(),
        ),
    )
    api = Mock()
    api.pull_pending.return_value = batch
    failed_client = _FailingSlackClient(fail_on_call=2)
    replay_client = _SlackClient()

    with (
        patch.object(weekly, "flush_automation_deliveries", return_value=False),
        patch.object(
            weekly,
            "_format_weekly_recordings_report",
            return_value="body",
        ),
        patch.object(
            weekly,
            "_build_weekly_recordings_report_blocks",
            return_value=[],
        ),
        patch.object(weekly, "remember_automation_delivery") as remember,
    ):
        with pytest.raises(RuntimeError, match="ambiguous Slack POST"):
            weekly._run_weekly_recordings_report_transport(
                failed_client,
                logging.getLogger("test.transport.weekly.crash"),
                automation_client=api,
                poll_now=_POLL_NOW,
            )
        remember.assert_not_called()

        sent = weekly._run_weekly_recordings_report_transport(
            replay_client,
            logging.getLogger("test.transport.weekly.replay"),
            automation_client=api,
            poll_now=_POLL_NOW,
        )

    assert sent is True
    assert [item["client_msg_id"] for item in failed_client.messages] == [
        item["client_msg_id"] for item in replay_client.messages
    ]
    remember.assert_called_once()


def test_daily_scheduler_mode_uses_api_target_and_keeps_thread_transport() -> None:
    scheduled_at = datetime(2026, 8, 10, 22, 0, tzinfo=_KST)
    batch = _batch(
        cycle="daily_device_round",
        cycle_key="daily:2026-08-10",
        scheduled_at=scheduled_at,
        delivery=AutomationDelivery(
            delivery_id="daily_device_round:2026-08-10:1",
            kind="daily_device_round_report",
            payload=_daily_payload(),
        ),
    )
    events: list[str] = []
    api = Mock()
    api.pull_pending.side_effect = lambda **_kwargs: (
        events.append("pull") or batch
    )
    client = _SlackClient()

    with (
        patch.object(daily.cs, "DAILY_DEVICE_ROUND_ENABLED", True),
        patch.object(
            daily.cs,
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
            True,
        ),
        patch.object(daily.cs, "DAILY_DEVICE_ROUND_CHANNEL_ID", ""),
        patch.object(daily.s, "DB_QUERY_ENABLED", False),
        patch.object(
            daily,
            "flush_automation_deliveries",
            side_effect=lambda *_args, **_kwargs: (
                events.append("flush") or False
            ),
        ),
        patch.object(daily, "_load_daily_device_round_state", return_value={}),
        patch.object(
            daily,
            "_persist_daily_device_round_transport_state",
            side_effect=lambda state: state,
        ),
        patch.object(
            daily,
            "_daily_device_round_window_key",
            side_effect=AssertionError("local due planner must not run"),
        ),
        patch.object(
            daily,
            "_resolve_daily_device_round_auto_update_agent",
            side_effect=AssertionError("local options must not be read"),
        ),
        patch.object(
            daily,
            "_build_daily_device_round_window_title_text",
            return_value="title",
        ) as build_title,
        patch.object(
            daily,
            "_build_daily_device_round_report_text",
            return_value="body",
        ) as build_report,
        patch.object(
            daily,
            "_build_remote_daily_device_round_blocks",
            return_value=[],
        ),
        patch.object(
            daily,
            "_split_daily_device_round_blocks",
            return_value=[[]],
        ),
        patch.object(daily, "remember_automation_delivery") as remember,
    ):
        sent = daily._run_daily_device_round_if_due(
            client,
            logging.getLogger("test.transport.daily"),
            now=_POLL_NOW,
            automation_client=api,
        )

    assert sent is True
    assert events == ["flush", "pull"]
    api.run.assert_not_called()
    api.pull_pending.assert_called_once()
    assert [item["channel"] for item in client.messages] == [
        "C123456",
        "C123456",
    ]
    build_title.assert_called_once_with(scheduled_at)
    assert build_report.call_args.kwargs["now"] == scheduled_at
    assert client.messages[1]["thread_ts"] == "1723000000.000001"
    assert remember.call_args.kwargs["batch"] == batch


def test_daily_transport_accepts_after_midnight_in_same_overnight_window() -> None:
    scheduled_at = datetime(2026, 8, 11, 1, 0, tzinfo=_KST)
    payload = _daily_payload()
    payload["runDate"] = "2026-08-11"
    batch = _batch(
        cycle="daily_device_round",
        cycle_key="daily:2026-08-10",
        scheduled_at=scheduled_at,
        delivery=AutomationDelivery(
            delivery_id="daily_device_round:2026-08-10:1",
            kind="daily_device_round_report",
            payload=payload,
        ),
    )

    # cycle key는 window 시작일, presentation runDate는 실제 API 실행일이다.
    report, render_now, window_key = (
        daily._validate_daily_device_round_transport_batch(batch)
    )

    assert window_key == "2026-08-10"
    assert render_now == scheduled_at
    assert report["runDate"] == "2026-08-11"


def test_daily_transport_rejects_scheduled_date_outside_window_day_pair() -> None:
    payload = _daily_payload()
    payload["runDate"] = "2026-08-12"
    batch = _batch(
        cycle="daily_device_round",
        cycle_key="daily:2026-08-10",
        scheduled_at=datetime(2026, 8, 12, 1, 0, tzinfo=_KST),
        delivery=AutomationDelivery(
            delivery_id="daily_device_round:2026-08-10:1",
            kind="daily_device_round_report",
            payload=payload,
        ),
    )

    with pytest.raises(RuntimeError, match="transport batch"):
        daily._validate_daily_device_round_transport_batch(batch)


def test_daily_partial_post_retries_same_chunks_without_fallback_duplicate() -> None:
    scheduled_at = datetime(2026, 8, 10, 22, 0, tzinfo=_KST)
    batch = _batch(
        cycle="daily_device_round",
        cycle_key="daily:2026-08-10",
        scheduled_at=scheduled_at,
        delivery=AutomationDelivery(
            delivery_id="daily_device_round:2026-08-10:1",
            kind="daily_device_round_report",
            payload=_daily_payload(),
        ),
    )
    api = Mock()
    api.pull_pending.return_value = batch
    failed_client = _FailingSlackClient(fail_on_call=3)
    replay_client = _SlackClient()
    transport_state: dict[str, object] = {}

    def persist_state(state: dict[str, object]) -> dict[str, object]:
        transport_state.clear()
        transport_state.update(state)
        return dict(state)

    with (
        patch.object(daily, "flush_automation_deliveries", return_value=False),
        patch.object(
            daily,
            "_load_daily_device_round_state",
            side_effect=lambda **_kwargs: dict(transport_state),
        ),
        patch.object(
            daily,
            "_persist_daily_device_round_transport_state",
            side_effect=persist_state,
        ),
        patch.object(
            daily,
            "_build_daily_device_round_window_title_text",
            return_value="title",
        ),
        patch.object(
            daily,
            "_build_daily_device_round_report_text",
            return_value="body",
        ),
        patch.object(
            daily,
            "_build_remote_daily_device_round_blocks",
            return_value=[{"type": "section"}],
        ),
        patch.object(
            daily,
            "_split_daily_device_round_blocks",
            return_value=[
                [{"type": "section", "block_id": "first"}],
                [{"type": "section", "block_id": "second"}],
            ],
        ),
        patch.object(daily, "remember_automation_delivery") as remember,
    ):
        with pytest.raises(RuntimeError, match="ambiguous Slack POST"):
            daily._run_daily_device_round_transport(
                failed_client,
                logging.getLogger("test.transport.daily.partial"),
                automation_client=api,
                poll_now=_POLL_NOW,
            )
        remember.assert_not_called()

        sent = daily._run_daily_device_round_transport(
            replay_client,
            logging.getLogger("test.transport.daily.replay"),
            automation_client=api,
            poll_now=_POLL_NOW,
        )

    assert sent is True
    # 첫 시도의 title 뒤 두 chunk 시도와 재시도의 두 chunk가 같은 dedupe key다.
    assert len(failed_client.messages) == 3
    assert len(replay_client.messages) == 2
    assert [
        item["client_msg_id"] for item in failed_client.messages[1:]
    ] == [item["client_msg_id"] for item in replay_client.messages]
    remember.assert_called_once()


@pytest.mark.parametrize(
    ("module", "enabled_name", "attach_name", "thread_name"),
    (
        (
            weekly,
            "WEEKLY_RECORDINGS_REPORT_ENABLED",
            "attach_weekly_recordings_reporter",
            "_WEEKLY_RECORDINGS_REPORT_THREAD",
        ),
        (
            daily,
            "DAILY_DEVICE_ROUND_ENABLED",
            "attach_daily_device_round_reporter",
            "_DAILY_DEVICE_ROUND_THREAD",
        ),
    ),
)
def test_scheduler_transport_attach_does_not_require_local_domain_settings(
    module: object,
    enabled_name: str,
    attach_name: str,
    thread_name: str,
) -> None:
    """attach는 API client와 Slack client만 있으면 transport를 시작한다."""

    fake_thread = Mock()
    fake_thread.is_alive.return_value = False
    setattr(module, thread_name, None)
    try:
        with (
            patch.object(module.cs, enabled_name, True),
            patch.object(
                module.cs,
                "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
                True,
            ),
            patch.object(module.s, "DB_QUERY_ENABLED", False),
            patch.object(
                module.cs,
                (
                    "WEEKLY_RECORDINGS_REPORT_CHANNEL_ID"
                    if module is weekly
                    else "DAILY_DEVICE_ROUND_CHANNEL_ID"
                ),
                "",
            ),
            patch.object(module.threading, "Thread", return_value=fake_thread),
            patch.object(
                module,
                "_is_daily_device_round_runtime_configured",
                side_effect=AssertionError("local MDA config must not be read"),
                create=True,
            ),
        ):
            getattr(module, attach_name)(
                Mock(client=object()),
                automation_client=Mock(),
            )
    finally:
        setattr(module, thread_name, None)

    fake_thread.start.assert_called_once()


@pytest.mark.parametrize(
    ("module", "enabled_name", "runner_name"),
    (
        (
            weekly,
            "WEEKLY_RECORDINGS_REPORT_ENABLED",
            "_run_weekly_recordings_report_if_due",
        ),
        (
            daily,
            "DAILY_DEVICE_ROUND_ENABLED",
            "_run_daily_device_round_if_due",
        ),
        (
            health,
            "DEVICE_HEALTH_MONITOR_ENABLED",
            "_run_device_health_monitor_once",
        ),
        (
            notification,
            "DEVICE_NOTIFICATION_ALERT_ENABLED",
            "_run_device_notification_alert_once",
        ),
    ),
)
def test_scheduler_transport_ack_failure_never_pulls_same_batch(
    module: object,
    enabled_name: str,
    runner_name: str,
) -> None:
    api = Mock()

    with (
        patch.object(module.cs, enabled_name, True),
        patch.object(
            module.cs,
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
            True,
        ),
        patch.object(
            module,
            "flush_automation_deliveries",
            side_effect=CompanyApiContractError(
                "company_api_automation_delivery_ack_incomplete"
            ),
        ),
        pytest.raises(
            CompanyApiContractError,
            match="company_api_automation_delivery_ack_incomplete",
        ),
    ):
        getattr(module, runner_name)(
            object(),
            logging.getLogger("test.transport.ack_failure"),
            now=_POLL_NOW,
            automation_client=api,
        )

    api.pull_pending.assert_not_called()
    api.run.assert_not_called()


def test_weekly_transport_rejects_unowned_conversation_before_slack_post() -> None:
    batch = _batch(
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=datetime(2026, 8, 10, 9, 0, tzinfo=_KST),
        delivery=AutomationDelivery(
            delivery_id="weekly_recordings:2026-08-03",
            kind="weekly_recordings_report",
            payload=_weekly_payload(),
        ),
        conversation={"threadTs": "untrusted"},
    )
    api = Mock()
    api.pull_pending.return_value = batch
    client = _SlackClient()

    with (
        patch.object(weekly.cs, "WEEKLY_RECORDINGS_REPORT_ENABLED", True),
        patch.object(
            weekly.cs,
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
            True,
        ),
        patch.object(weekly, "flush_automation_deliveries", return_value=False),
        pytest.raises(RuntimeError, match="transport batch"),
    ):
        weekly._run_weekly_recordings_report_if_due(
            client,
            logging.getLogger("test.transport.weekly.strict"),
            now=_POLL_NOW,
            automation_client=api,
        )

    assert client.messages == []
    api.run.assert_not_called()
