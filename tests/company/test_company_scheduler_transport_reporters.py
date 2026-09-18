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


def test_weekly_transport_preserves_api_week_over_week_changes() -> None:
    # 실제 전송 경로에서 API의 급증·급감이 알림 텍스트와 화면 블록 모두에 남아야 한다.
    delivery = _weekly_delivery()
    delivery.payload.update(
        {
            "hospitalCount": 2,
            "totalCount": 150,
            "previousTotalCount": 250,
            "totalDelta": -100,
            "totalChangeRate": -40.0,
            "topRows": [{"hospitalName": "증가병원", "rowCount": 120}],
            "surgeRows": [
                {"hospitalName": "증가병원", "previousCount": 10,
                 "currentCount": 120, "delta": 110, "changeRate": 1100.0},
                {"hospitalName": "신규병원", "previousCount": 0,
                 "currentCount": 30, "delta": 30, "changeRate": None},
            ],
            "surgeCount": 2,
            "dropRows": [
                {"hospitalName": "감소병원", "previousCount": 240,
                 "currentCount": 0, "delta": -240, "changeRate": -100.0},
            ],
            "dropCount": 1,
        }
    )
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (delivery,))
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()
    with (
        patch.object(weekly, "flush_automation_deliveries"),
        patch.object(weekly, "remember_automation_delivery"),
    ):
        assert weekly._run_weekly_recordings_report_if_due(
            client, logging.getLogger("test.weekly.changes"),
            now=_NOW, automation_client=api,
        )

    message = client.messages[1]
    assert message["thread_ts"] == "1723000000.000001"
    block_text = "\n\n".join(block["text"]["text"] for block in message["blocks"])
    assert block_text == message["text"]
    assert "*비교 기간* `2026-07-27 ~ 2026-08-02`" in block_text
    assert "증감 `-100건` (`-40.0%`)" in block_text
    assert "*급증 병원* `2곳`" in block_text
    assert "증가병원 `10건 → 120건` · `+110건` (`+1100.0%`)" in block_text
    assert "신규병원 `0건 → 30건` · `+30건` (`신규/비교불가`)" in block_text
    assert "*급감 병원* `1곳`" in block_text
    assert "감소병원 `240건 → 0건` · `-240건` (`-100.0%`)" in block_text


def test_weekly_report_shows_empty_change_groups_and_zero_previous_count() -> None:
    summary = _weekly_delivery().payload
    summary.update(previousTotalCount=0, totalDelta=3, totalChangeRate=None)

    text = weekly._format_weekly_recordings_report(summary)

    assert "증감 `+3건` (`신규/비교불가`)" in text
    assert "*급증 병원* `0곳`\n• 없어" in text
    assert "*급감 병원* `0곳`\n• 없어" in text


def _room_drop_row(index: int = 1) -> dict[str, object]:
    return {
        "hospitalSeq": 1,
        "hospitalRoomSeq": index,
        "hospitalName": "테스트병원",
        "roomName": f"{index}진료실",
        "previousCount": 6,
        "currentCount": 3,
        "delta": -3,
        "changeRate": -50.0,
    }


def _weekly_activity_delivery() -> AutomationDelivery:
    delivery = _weekly_delivery()
    room = _room_drop_row()
    hospital = {key: value for key, value in room.items() if key not in {"hospitalRoomSeq", "roomName"}}
    delivery.payload.update(
        totalCount=3, previousTotalCount=6, totalDelta=-3, totalChangeRate=-50.0,
        dropRows=[hospital], dropCount=1, roomDropRows=[room], roomDropCount=1,
        queryOptions={"hospitalNames": [], "dropPercent": 50, "minimumDrop": 3,
                      "hospitalRecordingMinimumDrop": 3},
        newBarcodes={
            "hospitalCount": 1, "totalCount": 3, "previousTotalCount": 6,
            "totalDelta": -3, "totalChangeRate": -50.0,
            "topRows": [{"hospitalSeq": 1, "hospitalName": "테스트병원", "rowCount": 3}],
            "dropRows": [hospital], "dropCount": 1, "roomDropRows": [room], "roomDropCount": 1,
        },
    )
    return delivery


def test_weekly_activity_posts_title_and_four_sections_in_one_thread() -> None:
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (_weekly_activity_delivery(),))
    client = _SlackClient()
    with (
        patch.object(weekly, "flush_automation_deliveries"),
        patch.object(weekly, "remember_automation_delivery") as remember,
    ):
        weekly._run_weekly_recordings_report_if_due(
            client, logging.getLogger(__name__), now=_NOW,
            automation_client=Mock(pull_pending=Mock(return_value=batch)),
        )
    assert len(client.messages) == 5
    assert client.messages[0]["text"] == "주간 초음파 녹화 & 신규 바코드 요약"
    titles = ("① 병원별 녹화 요약", "② 진료실별 녹화 급감", "③ 병원별 신규 바코드 요약", "④ 진료실별 신규 바코드 급감")
    for title, message in zip(titles, client.messages[1:], strict=True):
        assert message["text"].startswith(f"*{title}*")
        assert message["thread_ts"] == "1723000000.000001"
        assert "50% 이상 · 감소량 3" in message["text"]
        assert "2026-08-03 ~ 2026-08-09" in message["text"]
        assert "2026-07-27 ~ 2026-08-02" in message["text"]
    assert len({m["client_msg_id"] for m in client.messages}) == 5
    remember.assert_called_once()
    assert remember.call_args.kwargs["delivery"].external_message_id == "1723000000.000005"


def test_weekly_activity_splits_long_new_barcode_list_without_losing_rows() -> None:
    delivery = _weekly_activity_delivery()
    new = delivery.payload["newBarcodes"]
    new.update(roomDropRows=[_room_drop_row(i) for i in range(1, 601)], roomDropCount=600)
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (delivery,))
    client = _SlackClient()
    with patch.object(weekly, "flush_automation_deliveries"), patch.object(weekly, "remember_automation_delivery"):
        weekly._run_weekly_recordings_report_if_due(
            client, logging.getLogger(__name__), now=_NOW,
            automation_client=Mock(pull_pending=Mock(return_value=batch)),
        )
    # 마지막 항목만 추가 댓글로 나누고 병원/진료실 600곳을 모두 보존한다.
    texts = []
    assert len(client.messages) > 5
    for message in client.messages[4:]:
        assert message["thread_ts"] == "1723000000.000001"
        assert len(message["blocks"]) <= 40 and len(message["text"]) <= 12000
        assert all(len(block["text"]["text"]) <= 3000 for block in message["blocks"])
        texts.append(message["text"])
    combined = "\n".join(texts)
    assert len([line for line in combined.splitlines() if "테스트병원 ·" in line]) == 600
    assert "600. 테스트병원 · 600진료실 `6개 → 3개`" in combined


def test_weekly_activity_partial_failure_preserves_ids_and_report_time() -> None:
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (_weekly_activity_delivery(),))
    api = Mock(pull_pending=Mock(return_value=batch))
    failed, replay = _SlackClient(fail_on=4), _SlackClient()
    with (
        patch.object(weekly, "flush_automation_deliveries"),
        patch.object(weekly, "remember_automation_delivery") as remember,
    ):
        with pytest.raises(RuntimeError, match="ambiguous Slack POST"):
            weekly._run_weekly_recordings_report_if_due(failed, logging.getLogger(__name__), now=_NOW, automation_client=api)
        remember.assert_not_called()
        weekly._run_weekly_recordings_report_if_due(
            replay, logging.getLogger(__name__), now=_NOW.replace(hour=10), automation_client=api,
        )
        remember.assert_called_once()
    assert failed.messages == replay.messages[:4]


@pytest.mark.parametrize("fields", [
    {"totalCount": True}, {"totalDelta": 123}, {"totalChangeRate": float("nan")},
    {"roomDropCount": 10}, {"dropCount": 10}, {"topRows": [{}]},
    {"roomDropRows": [{**_room_drop_row(), "roomName": None}]},
    {"roomDropRows": [{**_room_drop_row(), "currentCount": 5}]},
    {"unexpected": True},
])
def test_weekly_activity_rejects_invalid_new_barcode_data_before_title(fields) -> None:
    delivery = _weekly_activity_delivery()
    delivery.payload["newBarcodes"].update(fields)
    client = _SlackClient()
    with patch.object(weekly, "flush_automation_deliveries"):
        with pytest.raises(RuntimeError, match="계약"):
            weekly._run_weekly_recordings_report_if_due(
                client, logging.getLogger(__name__), now=_NOW,
                automation_client=Mock(pull_pending=Mock(return_value=_batch(
                    "weekly_recordings", "weekly:2026-08-03", (delivery,),
                ))),
            )
    assert client.messages == []


@pytest.mark.parametrize("missing", ["newBarcodes", "queryOptions", "roomDropRows", "roomDropCount"])
def test_weekly_activity_requires_complete_contract(missing) -> None:
    delivery = _weekly_activity_delivery()
    del delivery.payload[missing]
    with pytest.raises(RuntimeError, match="계약"):
        weekly._validate_weekly_transport_batch(_batch("weekly_recordings", "weekly:2026-08-03", (delivery,)))


def test_weekly_transport_renders_all_room_drops_across_messages() -> None:
    # 급감 기준을 충족한 많은 진료실도 동일 thread에서 빠짐없이 전달한다.
    delivery = _weekly_delivery()
    delivery.payload.update(roomDropRows=[_room_drop_row(i) for i in range(1, 601)], roomDropCount=600)
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (delivery,))
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()
    with (
        patch.object(weekly, "flush_automation_deliveries"),
        patch.object(weekly, "remember_automation_delivery") as remember,
    ):
        assert weekly._run_weekly_recordings_report_if_due(
            client, logging.getLogger("test.weekly.rooms"), now=_NOW, automation_client=api,
        )

    assert len(client.messages) > 2
    texts = []
    for message in client.messages[1:]:
        assert message["thread_ts"] == "1723000000.000001"
        assert len(message["blocks"]) <= 40
        assert len(message["text"]) <= 12000
        assert all(len(block["text"]["text"]) <= 3000 for block in message["blocks"])
        texts.append(message["text"])
    combined = "\n".join(texts)
    assert "*진료실별 녹화 급감* `600곳` (전주 대비 50% 이상·3건 이상 감소)" in combined
    assert len([line for line in combined.splitlines() if "테스트병원 ·" in line]) == 600
    assert "600. 테스트병원 · 600진료실 `6건 → 3건` · `-3건` (`-50.0%`)" in combined
    remember.assert_called_once()


def test_weekly_split_failure_is_not_acknowledged_and_reuses_message_ids() -> None:
    # 뒷부분 전송이 실패하면 delivery 완료를 남기지 않고 같은 part ID로 재개한다.
    delivery = _weekly_delivery()
    delivery.payload.update(roomDropRows=[_room_drop_row(i) for i in range(1, 601)], roomDropCount=600)
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (delivery,))
    api = Mock(pull_pending=Mock(return_value=batch))
    failed, replay = _SlackClient(fail_on=3), _SlackClient()
    with (
        patch.object(weekly, "flush_automation_deliveries"),
        patch.object(weekly, "remember_automation_delivery") as remember,
    ):
        with pytest.raises(RuntimeError, match="ambiguous Slack POST"):
            weekly._run_weekly_recordings_report_if_due(
                failed, logging.getLogger("test.weekly.partial"), now=_NOW, automation_client=api,
            )
        remember.assert_not_called()
        assert weekly._run_weekly_recordings_report_if_due(
            replay, logging.getLogger("test.weekly.replay"), now=_NOW, automation_client=api,
        )
        remember.assert_called_once()
    assert [item["client_msg_id"] for item in failed.messages] == [
        item["client_msg_id"] for item in replay.messages[:len(failed.messages)]
    ]


@pytest.mark.parametrize("fields", [
    {"roomDropRows": []},
    {"roomDropRows": [], "roomDropCount": 1},
    {"roomDropRows": [], "roomDropCount": False},
    {"roomDropRows": "invalid", "roomDropCount": 0},
    {"roomDropRows": [{**_room_drop_row(), "previousCount": "2"}], "roomDropCount": 1},
    {"roomDropRows": [{**_room_drop_row(), "roomName": None}], "roomDropCount": 1},
])
def test_weekly_rejects_malformed_room_drop_payload_before_posting(fields) -> None:
    delivery = _weekly_delivery()
    delivery.payload.update(fields)
    batch = _batch("weekly_recordings", "weekly:2026-08-03", (delivery,))
    client = _SlackClient()
    with patch.object(weekly, "flush_automation_deliveries"):
        with pytest.raises(RuntimeError, match="계약"):
            weekly._run_weekly_recordings_report_if_due(
                client, logging.getLogger("test.weekly.invalid"), now=_NOW,
                automation_client=Mock(pull_pending=Mock(return_value=batch)),
            )
    assert client.messages == []


def test_weekly_change_lists_fit_slack_sections_without_losing_hospitals() -> None:
    # 상위·급증·급감 각 10개 목록과 긴 병원명이 함께 와도 블록 제한을 넘지 않는다.
    summary = _weekly_delivery().payload
    for rows_key, count_key, previous_count, current_count, rate in (
        ("surgeRows", "surgeCount", 20, 40, 100.0),
        ("dropRows", "dropCount", 40, 20, -50.0),
    ):
        summary[rows_key] = [
            {"hospitalName": f"{rows_key}-{index}-" + "긴병원명" * 80,
             "previousCount": previous_count, "currentCount": current_count,
             "delta": current_count - previous_count, "changeRate": rate}
            for index in range(10)
        ]
        summary[count_key] = 12
    summary["topRows"] = [
        {"hospitalName": row["hospitalName"], "rowCount": 40}
        for row in summary["surgeRows"]
    ]
    blocks = weekly._build_weekly_recordings_report_blocks(summary, include_header=True)

    assert blocks[0]["type"] == "header"
    section_texts = [block["text"]["text"] for block in blocks[1:]]
    assert all(0 < len(text) <= 3000 for text in section_texts)
    # 분할 과정에서 줄이나 마지막 병원을 누락하지 않았는지 전체 표시 내용을 비교한다.
    expected = weekly._format_weekly_recordings_report(summary)
    assert "\n".join(section_texts).splitlines() == [line for line in expected.splitlines() if line]
    assert expected.count("• 상위 `10곳`만 표시") == 2


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
