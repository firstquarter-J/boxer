from __future__ import annotations

import hashlib
import json
import logging
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, call
from zoneinfo import ZoneInfo

import pytest
from slack_sdk.errors import SlackApiError
from slack_sdk.web.slack_response import SlackResponse

from boxer_company.automation_contracts import AutomationDelivery
from boxer_company_adapter_slack import automation_reporter
from boxer_company_adapter_slack import daily_device_round_reporter as daily
from boxer_company_adapter_slack.automation_api_client import (
    AutomationRemoteAckResult,
    AutomationRemoteDeliveryBatch,
)

_NOW = datetime(2026, 10, 2, 23, 30, tzinfo=ZoneInfo("Asia/Seoul"))
_CYCLE_KEY = "daily:2026-10-02"
_MAX_BLOCKS = 12
_MAX_BLOCK_BYTES = 8_000


def _section(text: str) -> dict[str, object]:
    return {"type": "section", "text": {"type": "mrkdwn", "text": text}}


def _wire_bytes(blocks: list[dict[str, object]]) -> int:
    # 실제 Slack SDK의 ASCII escape JSON을 측정해 한글·emoji가
    # 원문 문자 수 기준으로 작게 계산되던 회귀를 잡는다.
    return len(json.dumps(blocks, ensure_ascii=True).encode("utf-8"))


def _flatten(chunks: list[list[dict[str, object]]]) -> list[dict[str, object]]:
    return [block for chunk in chunks for block in chunk]


def _daily_batch(
    *, device_count: int = 31, extra_detail: str = ""
) -> AutomationRemoteDeliveryBatch:
    device_lines = [
        f"• *4층 {index:02d}진료실* | *MB2-TEST{index:03d}* | 🟢 *정상*\n"
        "  *에이전트 업데이트* 🟢 *업데이트 완료* | `2.0.2` -> `2.0.3`\n"
        "  *박스 업데이트* 🟢 *업데이트 완료* | `2.11.307` -> `2.11.310`"
        f"{extra_detail}"
        for index in range(1, device_count + 1)
    ]
    blocks = [
        {"type": "header", "text": {"type": "plain_text", "text": "#345 테스트병원"}},
        {
            "type": "context",
            "elements": [
                {"type": "mrkdwn", "text": f"장비 `{device_count}대` | 야간 순회"}
            ],
        },
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_list",
                    "style": "bullet",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [
                                {
                                    "type": "text",
                                    "text": f"🟢 업데이트 성공 {device_count}대",
                                }
                            ],
                        }
                    ],
                }
            ],
        },
        {"type": "divider"},
        *[_section(line) for line in device_lines],
    ]
    delivery = AutomationDelivery(
        delivery_id="daily_device_round:2026-10-02:345",
        kind="daily_device_round_report",
        payload={
            "runDate": "2026-10-02",
            "hospitalSeq": 345,
            "hospitalName": "테스트병원",
            "deviceCount": device_count,
            "scheduledDeviceCount": device_count,
            "statusCounts": {"정상": device_count},
            "updateCounts": {"agentUpdated": device_count, "boxUpdated": device_count},
            "cleanupCounts": {},
            "powerCounts": {},
            "summaryLine": f"업데이트 성공 {device_count}대",
            "messageBlocks": blocks,
            "fallbackText": "#345 테스트병원\n\n" + "\n\n".join(device_lines),
            "deviceResults": [
                {"deviceName": f"MB2-TEST{index:03d}"}
                for index in range(1, device_count + 1)
            ],
        },
    )
    raw_identity = f"T1\0daily_device_round\0{_CYCLE_KEY}\0{delivery.delivery_id}"
    return AutomationRemoteDeliveryBatch(
        batch_id="batch:" + hashlib.sha256(raw_identity.encode()).hexdigest(),
        tenant_id="T1",
        cycle="daily_device_round",
        cycle_key=_CYCLE_KEY,
        scheduled_at=_NOW,
        channel_id="C123456",
        deliveries=(delivery,),
    )


class _SlackClient:
    def __init__(self, *, fail_on_body: int | None = None) -> None:
        self.messages: list[dict[str, object]] = []
        self.accepted_ids: dict[str, str] = {}
        self.fail_on_body = fail_on_body
        self.body_count = 0

    def chat_postMessage(self, **kwargs: object) -> dict[str, str]:
        self.messages.append(deepcopy(kwargs))
        if "blocks" in kwargs:
            self.body_count += 1
            if self.body_count == self.fail_on_body:
                # 일부 댓글만 전송된 상태에서 실패해도 domain receipt를
                # 확정하지 않고 같은 part ID로 재시도하는 상황을 재현한다.
                response = SlackResponse(
                    client=Mock(),
                    http_verb="POST",
                    api_url="https://slack.com/api/chat.postMessage",
                    req_args={},
                    data={"ok": False, "error": "internal_error"},
                    headers={},
                    status_code=200,
                )
                raise SlackApiError("mock Slack refusal", response)
        message_id = str(kwargs["client_msg_id"])
        timestamp = self.accepted_ids.setdefault(
            message_id,
            f"1790970600.{len(self.accepted_ids) + 1:06d}",
        )
        return {"ts": timestamp}


def _use_journal(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    state_path = tmp_path / "delivery.json"
    monkeypatch.setattr(
        automation_reporter.cs, "AUTOMATION_DELIVERY_STATE_PATH", str(state_path)
    )
    # pacing 자체는 유지하되 mock 발송 검증에서 실제 1초 대기를 하지 않는다.
    monkeypatch.setattr(daily.threading, "Event", Mock())
    return state_path


def test_korean_emoji_report_preserves_all_31_devices_and_block_order() -> None:
    batch = _daily_batch()
    blocks = batch.deliveries[0].payload["messageBlocks"]
    original = deepcopy(blocks)
    chunks = daily._split_daily_device_round_blocks(blocks)

    assert len(chunks) >= 2
    assert _flatten(chunks) == original
    assert blocks == original
    assert all(len(chunk) <= _MAX_BLOCKS for chunk in chunks)
    assert all(_wire_bytes(chunk) <= _MAX_BLOCK_BYTES for chunk in chunks)
    device_sections = [
        block["text"]["text"]
        for block in _flatten(chunks)
        if block["type"] == "section"
    ]
    assert len(device_sections) == 31
    assert all(
        f"MB2-TEST{index:03d}" in device_sections[index - 1] for index in range(1, 32)
    )


@pytest.mark.parametrize(
    "character,count,expected_chunks",
    [("a", 1_000, 1), ("가", 1_000, 2), ("🟢", 350, 2)],
)
def test_chunk_budget_counts_ascii_escaped_korean_and_emoji(
    character: str,
    count: int,
    expected_chunks: int,
) -> None:
    blocks = [_section(character * count), _section(character * count)]
    chunks = daily._split_daily_device_round_blocks(blocks)
    assert len(chunks) == expected_chunks
    assert _flatten(chunks) == blocks
    assert all(_wire_bytes(chunk) <= _MAX_BLOCK_BYTES for chunk in chunks)


def test_block_count_splits_small_payload_without_dropping_dividers() -> None:
    blocks = [{"type": "divider"} for _ in range(21)]
    chunks = daily._split_daily_device_round_blocks(blocks)
    assert [len(chunk) for chunk in chunks] == [12, 9]
    assert _flatten(chunks) == blocks


@pytest.mark.parametrize("wire_size,expected_chunks", [(8_000, 1), (8_001, 2)])
def test_wire_budget_includes_array_brackets_and_item_separators(
    wire_size: int,
    expected_chunks: int,
) -> None:
    # 유효한 section 세 개로 경계를 고정해 개별 block 합산에서 빠지는
    # 배열의 괄호·쉼표·공백까지 바이트 예산에 포함하는지 검증한다.
    blocks = [_section("") for _ in range(3)]
    padding = wire_size - _wire_bytes(blocks)
    for index, block in enumerate(blocks):
        block["text"]["text"] = "a" * (padding // 3 + (index < padding % 3))
    assert _wire_bytes(blocks) == wire_size
    chunks = daily._split_daily_device_round_blocks(blocks)
    assert len(chunks) == expected_chunks
    assert _flatten(chunks) == blocks
    assert all(_wire_bytes(chunk) <= _MAX_BLOCK_BYTES for chunk in chunks)


def test_transport_uses_one_thread_and_distinct_ids_for_every_chunk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = _use_journal(tmp_path, monkeypatch)
    batch = _daily_batch()
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()
    assert daily._run_daily_device_round_if_due(
        client, logging.getLogger("test.daily.chunk"), now=_NOW, automation_client=api
    )

    title, *messages = client.messages
    assert len(messages) >= 2
    root_ts = client.accepted_ids[title["client_msg_id"]]
    assert all(message["thread_ts"] == root_ts for message in messages)
    assert len({message["client_msg_id"] for message in messages}) == len(messages)
    assert daily.threading.Event.return_value.wait.call_args_list == [call(1)] * len(
        messages
    )
    assert (
        _flatten([message["blocks"] for message in messages])
        == batch.deliveries[0].payload["messageBlocks"]
    )
    for index, message in enumerate(messages):
        assert len(message["blocks"]) <= _MAX_BLOCKS
        assert _wire_bytes(message["blocks"]) <= _MAX_BLOCK_BYTES
        assert message[
            "client_msg_id"
        ] == automation_reporter.build_automation_delivery_client_msg_id(
            cycle=batch.cycle,
            cycle_key=batch.cycle_key,
            delivery_id=batch.deliveries[0].delivery_id,
            part=f"chunk:v2:{index}",
        )
        # 현재 chunk의 모든 장비만 접근성 본문에 남기고 다른 장비를 반복하지 않는다.
        block_text = json.dumps(message["blocks"], ensure_ascii=False)
        for device_index in range(1, 32):
            device_name = f"MB2-TEST{device_index:03d}"
            assert (device_name in message["text"]) == (device_name in block_text)
        assert message["text"].endswith(f"계속 {index + 1}/{len(messages)}")
    receipts = json.loads(state_path.read_text())["cycles"]["daily_device_round"][
        "receipts"
    ]
    assert len(receipts) == 1
    assert receipts[0]["deliveryId"] == batch.deliveries[0].delivery_id
    api.acknowledge_batch.assert_not_called()


def test_single_chunk_keeps_existing_fallback_and_part_identity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _use_journal(tmp_path, monkeypatch)
    batch = _daily_batch(device_count=1)
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()
    assert daily._run_daily_device_round_if_due(
        client,
        logging.getLogger("test.daily.chunk.single"),
        now=_NOW,
        automation_client=api,
    )
    assert len(client.messages) == 2
    daily.threading.Event.return_value.wait.assert_called_once_with(1)
    message = client.messages[1]
    assert message["text"] == batch.deliveries[0].payload["fallbackText"]
    assert message["blocks"] == batch.deliveries[0].payload["messageBlocks"]
    assert message[
        "client_msg_id"
    ] == automation_reporter.build_automation_delivery_client_msg_id(
        cycle=batch.cycle,
        cycle_key=batch.cycle_key,
        delivery_id=batch.deliveries[0].delivery_id,
        part="chunk:0",
    )


def test_chunk_fallback_preserves_nested_elements_and_fields() -> None:
    # rich_text의 중첩 본문과 section.fields도 접근성 fallback에서 빠지면 안 된다.
    blocks = [
        {
            "type": "rich_text",
            "elements": [
                {
                    "type": "rich_text_list",
                    "style": "bullet",
                    "elements": [
                        {
                            "type": "rich_text_section",
                            "elements": [{"type": "text", "text": "🟢 중첩 장비 결과"}],
                        }
                    ],
                }
            ],
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": "장비 번호 MB2-TEST001"},
                {"type": "plain_text", "text": "박스 업데이트 완료"},
            ],
        },
    ]
    text = daily._build_daily_device_round_chunk_text(
        "다른 chunk를 포함한 원문 전체",
        blocks=blocks,
        chunk_index=0,
        chunk_count=2,
    )
    assert "🟢 중첩 장비 결과" in text
    assert "장비 번호 MB2-TEST001" in text
    assert "박스 업데이트 완료" in text
    assert "원문 전체" not in text
    assert text.endswith("계속 1/2")


def test_failed_middle_chunk_leaves_no_receipt_and_replays_same_ids(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = _use_journal(tmp_path, monkeypatch)
    batch = _daily_batch(
        extra_detail="\n  *확인* " + "장비 상태와 저장 공간 정상. " * 15
    )
    blocks = batch.deliveries[0].payload["messageBlocks"]
    chunks = daily._split_daily_device_round_blocks(blocks)
    assert len(chunks) >= 3
    api = Mock(
        pull_pending=Mock(side_effect=[batch, batch, None]),
        acknowledge_batch=Mock(
            return_value=AutomationRemoteAckResult(acknowledged=True)
        ),
    )
    client = _SlackClient(fail_on_body=2)
    logger = logging.getLogger("test.daily.chunk.replay")

    with pytest.raises(SlackApiError):
        daily._run_daily_device_round_if_due(
            client, logger, now=_NOW, automation_client=api
        )
    failed_calls = deepcopy(client.messages)
    state = json.loads(state_path.read_text())["cycles"]["daily_device_round"]
    assert state["receipts"] == []
    assert state["threadReceipt"]["rootMessageId"]
    assert daily.threading.Event.return_value.wait.call_args_list == [call(1), call(1)]
    api.acknowledge_batch.assert_not_called()

    assert daily._run_daily_device_round_if_due(
        client, logger, now=_NOW, automation_client=api
    )
    replay_calls = client.messages[len(failed_calls) :]
    assert len(replay_calls) == len(chunks)
    assert [message["client_msg_id"] for message in failed_calls[1:]] == [
        message["client_msg_id"] for message in replay_calls[:2]
    ]
    assert all(
        message["thread_ts"] == state["threadReceipt"]["rootMessageId"]
        for message in replay_calls
    )
    assert _flatten([message["blocks"] for message in replay_calls]) == blocks
    assert daily.threading.Event.return_value.wait.call_args_list == [call(1)] * (
        len(chunks) + 1
    )
    receipts = json.loads(state_path.read_text())["cycles"]["daily_device_round"][
        "receipts"
    ]
    assert len(receipts) == 1
    api.acknowledge_batch.assert_not_called()

    # 다음 poll은 모든 chunk의 확정 receipt를 한 번만 ACK하고,
    # pending이 없으면 Slack 메시지를 추가로 보내지 않는다.
    message_count = len(client.messages)
    assert (
        daily._run_daily_device_round_if_due(
            client, logger, now=_NOW, automation_client=api
        )
        is False
    )
    api.acknowledge_batch.assert_called_once()
    assert len(api.acknowledge_batch.call_args.kwargs["receipts"]) == 1
    assert len(client.messages) == message_count
    assert (
        json.loads(state_path.read_text())["cycles"]["daily_device_round"]["receipts"]
        == []
    )


def test_oversized_single_block_is_rejected_before_any_slack_post(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_path = _use_journal(tmp_path, monkeypatch)
    original = _daily_batch()
    payload = dict(original.deliveries[0].payload)
    payload["messageBlocks"] = [_section("한" * 2_000)]
    oversized = deepcopy(payload["messageBlocks"])
    batch = AutomationRemoteDeliveryBatch(
        batch_id=original.batch_id,
        tenant_id=original.tenant_id,
        cycle=original.cycle,
        cycle_key=original.cycle_key,
        scheduled_at=original.scheduled_at,
        channel_id=original.channel_id,
        deliveries=(
            AutomationDelivery(
                delivery_id=original.deliveries[0].delivery_id,
                kind="daily_device_round_report",
                payload=payload,
            ),
        ),
    )
    api = Mock(pull_pending=Mock(return_value=batch))
    client = _SlackClient()
    with pytest.raises(RuntimeError):
        daily._run_daily_device_round_if_due(
            client,
            logging.getLogger("test.daily.chunk.oversized"),
            now=_NOW,
            automation_client=api,
        )
    assert client.messages == []
    daily.threading.Event.assert_not_called()
    assert not state_path.exists()
    assert payload["messageBlocks"] == oversized
    api.acknowledge_batch.assert_not_called()
