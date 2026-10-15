from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import json
import logging
import threading
from types import SimpleNamespace
from unittest.mock import Mock
from urllib.parse import parse_qs, urlsplit

import pytest

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


def test_audio_alert_matches_approved_two_column_comparison() -> None:
    # 승인한 실측 예시의 두 길이를 먼저 나란히 표시하고 차이·주석은 아래에 둔다.
    item = {**_item(), "alertCategory": "recording_audio", "problemComponents": [], "issue": (
        "녹화 파일 오디오 이상: 영상 길이 대비 오디오 길이 부족 (발생 2026-10-15 15:42:25 KST)\n"
        "파일 길이 03:06.803 · 디코딩 오디오 00:34.219\n"
        "오디오 길이 차이 02:32.584 (81.7%)\n"
        "오디오 데이터 누락 구간 및 비율: 확인 필요 (완료된 구간 분석 없음)"
    )}
    message = _rendered_message(item)
    text = alert._alert_issue_mrkdwn(item)
    comparison_fields = [
        {"type": "mrkdwn", "text": "확인된 오디오\n*34.2초*"},
        {"type": "mrkdwn", "text": "영상 전체\n*3분 6.8초*"},
    ]
    footer = (
        "길이 차이 *2분 32.6초 · 81.7%*\n"
        "※ 길이 기준 비교이며 실제 누락 구간은 미확인\n\n"
        "🕒 *감지 시각*\n2026-10-15 15:42:25 KST"
    )
    assert message["blocks"][2] == {
        "type": "section", "text": {"type": "mrkdwn", "text": "🔎 *감지 내용*"},
        "fields": comparison_fields,
    }
    assert message["blocks"][3]["text"]["text"] == footer
    assert text == "\n\n".join(["🔎 *감지 내용*", *(field["text"] for field in comparison_fields), footer])
    assert text in message["text"]
    assert len(message["blocks"]) == 5
    # 화면에서만 반올림하며 API action의 길이·비율과 장애 식별은 바꾸지 않는다.
    target = json.loads(message["blocks"][-1]["elements"][0]["value"])
    assert target["issue"] == item["issue"]


@pytest.mark.parametrize(("length", "expected"), [
    ("00:59.999", "1분"), ("01:59.95", "2분"), ("00:34.25", "34.3초"),
    ("00:00", "0초"), ("확인 필요", "확인 필요"),
])
def test_audio_display_rounds_once_and_preserves_unknown_lengths(length: str, expected: str) -> None:
    # 분 경계와 반올림 경계를 검증하고 미확인 길이를 0으로 만들어 표시하지 않는다.
    text = alert._recording_media_issue_mrkdwn(
        f"녹화 파일 오디오 이상: 오디오 디코딩 실패\n영상 길이 05:00\n디코딩 오디오 {length}"
    )
    assert f"확인된 오디오\n*{expected}*" in text
    assert "영상 전체\n*5분*" in text


def test_audio_alert_keeps_other_failures_and_future_api_descriptions() -> None:
    # 디코딩 실패 등 복수 원인과 새 설명을 수치 요약 때문에 숨기지 않는다.
    text = alert._recording_media_issue_mrkdwn(
        "녹화 파일 오디오 이상: 오디오 디코딩 실패, 영상 길이 대비 오디오 길이 부족\n"
        "영상 길이 05:00\n오디오 데이터 누락 합계 00:10 (전체의 3.3%)\n"
        "오디오 데이터 누락 구간: 04:50~05:00\n추가 분석 정보"
    )
    assert text.startswith("🔎 *감지 내용*\n오디오 데이터 누락 *10초* (영상의 3.3%) / 영상 전체 *5분*")
    for value in ("오디오 디코딩 실패", "04:50~05:00", "추가 분석 정보"):
        assert value in text
    assert "확인 필요" not in text


@pytest.mark.parametrize(("reason", "length"), [
    ("녹화 파일 없음", "확인 필요"),
    ("영상 파일 정보 읽기 실패", "확인 필요"),
    ("영상 트랙 없음", "확인 필요"),
    ("영상 트랙 없음", "00:21.854"),
    ("영상 디코딩 실패", "확인 필요"),
    ("재생 가능한 영상 프레임 없음", "확인 필요"),
    ("화면 정지 구간 감지 (정지 시간 확인 필요)", "확인 필요"),
    ("단색 화면 구간 감지", "확인 필요"),
])
def test_video_inspection_failures_use_same_full_width_layout(reason: str, length: str) -> None:
    # 원인은 보존하되 영상 트랙이 없으면 길이의 확인 여부와 관계없이 표시하지 않는다.
    item = {**_item(), "alertCategory": "recording_video", "problemComponents": [], "issue": (
        f"녹화 파일 영상 이상: {reason} (발생 2026-10-14 15:17:03 KST)\n영상 길이 {length}"
    )}
    text = alert._alert_issue_mrkdwn(item)
    assert reason.split(" (")[0] in text
    if reason == "영상 트랙 없음":
        assert text.split("\n\n")[0] == "🔎 *감지 내용*\n*영상 트랙 없음*"
    else:
        assert "영상 전체 *확인 필요*" in text
    assert text.endswith("🕒 *감지 시각*\n2026-10-14 15:17:03 KST")
    assert "`" not in text
    blocks = alert._alert_item_blocks(item, card_index=0, include_actions=False, include_device_voice_action=False)
    assert blocks[1]["text"]["text"] == text.split("\n\n")[0]
    assert blocks[1]["fields"] == [{"type": "mrkdwn", "text": "🕒 *감지 시각*\n2026-10-14 15:17:03 KST"}]
    assert text in alert._alert_item_fallback_text(item)


def test_video_freeze_and_legacy_uniform_metrics_remain_separate() -> None:
    # 최장 구간의 비율만 계산하고 최초 시작으로 끝 시각·전체 합계를 만들지 않는다.
    text = alert._recording_media_issue_mrkdwn(
        "녹화 파일 영상 이상: 화면 정지 구간 감지 (최장 연속 정지 50분 31.8초), 단색 화면 구간 감지\n"
        "영상 길이 60:00\n첫 단색 감지 시작: 00:08.333 (영상 시작 기준)\n"
        "최장 연속 단색: 00:10.9\n전체 단색 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)"
    )
    assert "최장 연속 정지 *50분 31.8초(영상 전체의 84.2%)*" in text
    assert "최장 연속 단색 *10.9초(영상 전체의 0.3%)*" in text
    assert "첫 단색 시작 *00:08.333* (영상 시작 기준)" in text
    assert "단색 화면 합계" not in text
    assert "~" not in text
    assert "영상 전체 *60분*" in text


def test_complete_uniform_summary_preserves_ranges_and_additional_failures() -> None:
    # 복수 판정과 생략된 구간 수까지 유지하며 전체 합계만 강조한다.
    text = alert._recording_media_issue_mrkdwn(
        "녹화 파일 영상 이상: 영상 디코딩 실패, 단색 화면 구간 감지\n영상 길이 20:00\n"
        "단색 감지 합계 16:40 (전체의 83.3%)\n"
        "단색 감지 구간: 00:00~00:10, 00:12~00:22 외 98구간 (합계에 포함)\n"
        "감지 기준: 연속 10초 이상 · 구간은 영상 시작 기준\n추가 영상 정보"
    )
    assert text.startswith("🔎 *감지 내용*\n단색 화면 합계 *16분 40초* (영상의 83.3%)")
    for value in ("*영상 디코딩 실패*", "외 98구간 (합계에 포함)",
                  "단색 구간 00:00~00:10", "영상 전체 *20분*", "추가 영상 정보"):
        assert value in text
    assert "확인 필요" not in text
    assert "감지 기준" not in text


@pytest.mark.parametrize(("longest", "video_length", "ratio"), [
    ("5분", "10:00", "50.0"),
    ("1초", "00:03", "33.3"),
    ("1분 0.001초", "01:00.001", "100.0"),
    ("0초", "00:10", None),
    ("11초", "00:10", None),
    ("5초", "00:00", None),
    ("5초", None, None),
])
def test_longest_ratio_uses_valid_measured_lengths_only(
    longest: str, video_length: str | None, ratio: str | None,
) -> None:
    # 길이 미확인·0·모순된 구간에는 추정 백분율을 붙이지 않는다.
    length = f"영상 길이 {video_length}" if video_length is not None else "영상 길이 확인 필요"
    text = alert._recording_media_issue_mrkdwn(
        f"녹화 파일 영상 이상: 화면 정지 구간 감지 (최장 연속 정지 {longest})\n{length}"
    )
    assert "\n영상 전체 " in text
    if ratio is None:
        assert "%" not in text
    else:
        assert f"최장 연속 정지 *{longest}(영상 전체의 {ratio}%)*" in text


@pytest.mark.parametrize("guidance", ["", "다음 확인: 캡처 입력을 확인해."])
def test_captureboard_alert_keeps_same_emphasis_and_time_with_or_without_guidance(
    guidance: str,
) -> None:
    # 예전 한 줄 payload와 새 안내 payload가 같은 감지 영역으로 렌더링된다.
    item = {
        **_item(),
        "alertCategory": "video_signal",
        "problemComponents": ["캡처보드"],
        "issue": "캡처보드 연결 이상으로 녹화가 중단됐어. (발생 2026-10-15 10:11:31 KST)",
        "alertGuidance": guidance,
    }
    message = _rendered_message(item)
    detail = message["blocks"][2]["text"]["text"]
    assert detail.startswith(
        ":rotating_light: *문제 장치*\n`캡처보드`\n\n"
        "🔎 *감지 내용*\n*캡처보드 연결 이상으로 녹화가 중단됐어.*"
    )
    assert message["blocks"][2]["fields"] == [{
        "type": "mrkdwn", "text": "🕒 *감지 시각*\n2026-10-15 10:11:31 KST",
    }]
    assert "(발생 " not in detail
    assert "다음 확인" not in message["text"]
    assert detail in message["text"]
    # 표시 가공이 완료·문자 action의 원본 장애 식별을 바꾸지 않는다.
    target = json.loads(message["blocks"][-1]["elements"][0]["value"])
    assert target["issue"] == item["issue"]


def test_recording_alert_groups_measurements_without_repeating_guidance() -> None:
    # 일반 알림도 실측값과 새 정보를 같은 문단에 두고 일반적인 안내만 뺀다.
    item = {
        **_second_item(),
        "issue": (
            "녹화 파일 증가가 멈췄어\n현재 파일 크기: 0바이트 (빈 파일)\n"
            "기대 최소 증가량: 1,024바이트/초\n영향: 데이터가 추가되지 않았어.\n"
            "추가 분석 정보"
        ),
        "alertGuidance": "다음 확인: 현재 녹화 상태를 확인해.",
    }
    text = alert._alert_issue_mrkdwn(item)
    assert text == (
        "🔎 *감지 내용*\n*녹화 파일 증가가 멈췄어*\n"
        "현재 파일 크기 *0바이트 (빈 파일)*\n기대 최소 증가량 *1,024바이트/초*\n"
        "추가 분석 정보"
    )


def test_reported_freeze_alert_shows_ratio_and_hides_criterion_and_note() -> None:
    # 사용자 예시의 실제 시간으로 최장 비율·총길이 줄 구분과 action 원문 보존을 확인한다.
    issue = (
        "녹화 파일 영상 이상: 화면 정지 구간 감지 (최장 연속 정지 26분 9.434초) (발생 2026-10-15 16:58:44 KST)\n"
        "영상 길이 33:47.467\n첫 정지 감지 시작: 07:38.033 (영상 시작 기준)\n"
        "정지 감지 기준: 연속 10:00 이상\n"
        "전체 정지 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)\n"
        "확인된 내용: 화면 변화가 없는 연속 구간을 감지했어.\n"
        "영향: 해당 구간의 움직임이 기록되지 않았을 수 있어.\n"
        "다음 확인: 표시된 시점의 원본 화면과 캡처 입력 상태를 확인해."
    )
    message = _rendered_message({**_item(), "alertCategory": "recording_video", "problemComponents": [], "issue": issue})
    detail = message["blocks"][2]["text"]["text"]
    assert detail == (
        "🔎 *감지 내용*\n최장 연속 정지 *26분 9.434초(영상 전체의 77.4%)*\n영상 전체 *33분 47.467초*\n"
        "첫 정지 시작 *07:38.033* (영상 시작 기준)"
    )
    assert message["blocks"][2]["fields"] == [
        {"type": "mrkdwn", "text": "🕒 *감지 시각*\n2026-10-15 16:58:44 KST"},
    ]
    assert detail in message["text"]
    assert "측정 정보" not in message["text"]
    assert "감지 기준" not in message["text"]
    assert "서로 다른 구간" not in message["text"]
    target = json.loads(message["blocks"][-1]["elements"][0]["value"])
    assert target["issue"] == issue


def test_combined_video_issues_keep_unknown_values_as_short_notes() -> None:
    # 시간이 없는 판정은 주석으로 남기고 최장 시간을 합계·비율로 바꾸지 않는다.
    text = alert._recording_media_issue_mrkdwn(
        "녹화 파일 영상 이상: 화면 정지 구간 감지 (정지 시간 확인 필요), 단색 화면 구간 감지\n"
        "영상 길이 60:00\n최장 연속 단색: 00:10.9\n"
        "전체 단색 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)"
    )
    assert "⚠️ *확인 필요*" not in text
    assert "※ 정지 시간은 아직 확인되지 않았어." in text
    assert "최장 연속 단색 *10.9초(영상 전체의 0.3%)*" in text
    assert "🎞️ *분석 구간*" not in text
    assert "📏 *감지 기준*" not in text
    assert "🕒 *감지 시각*" not in text
    assert "~" not in text and "영상 전체 *60분*" in text


def test_reported_uniform_alert_matches_approved_compact_preview() -> None:
    # 승인된 예시 그대로 비교값을 붙이고 첫 시작으로 최장 구간 끝을 만들지 않는다.
    item = {**_item(), "alertCategory": "recording_video", "problemComponents": [], "issue": (
        "녹화 파일 영상 이상: 단색 화면 구간 감지\n영상 길이 00:39.934\n"
        "첫 단색 감지 시작: 00:18.3 (영상 시작 기준)\n최장 연속 단색: 00:21.633\n"
        "전체 단색 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)\n"
        "확인된 내용: 화면이 한 색으로 채워진 연속 구간을 감지했어.\n"
        "영향: 해당 구간에서 진료 화면이 보이지 않을 수 있어.\n"
        "다음 확인: 표시된 시점의 원본 화면과 캡처 입력 상태를 확인해."
    )}
    message = _rendered_message(item)
    expected = (
        "🔎 *감지 내용*\n최장 연속 단색 *21.633초(영상 전체의 54.2%)*\n영상 전체 *39.934초*\n"
        "첫 단색 시작 *00:18.3* (영상 시작 기준)"
    )
    assert message["blocks"][2] == {"type": "section", "text": {"type": "mrkdwn", "text": expected}}
    assert expected in message["text"]


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


def _completion_rows(blocks: object) -> list[str]:
    """카드 식별 정보보다 먼저 나오는 완료 담당자와 시간을 검증한다."""

    assert isinstance(blocks, list)
    rows: list[str] = []
    for block in blocks:
        if not isinstance(block, dict) or block.get("type") != "section":
            continue
        text = block.get("text", {}).get("text", "")
        if text.startswith(("✅ *확인 완료*", "담당자 <@")):
            rows.append(text.split("\n\n", 1)[0])
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

    # 식별·연락처는 2열, 감지 내용은 전체 폭으로 표시하고 본문도 같은 간격을 쓴다.
    message = client.chat_postMessage.call_args.kwargs
    assert message["text"] == ":alert: *LED 연결 확인 필요*\n" + "\n\n".join(
        (
            "🏥 *1 테스트병원*",
            (
                "⚙️ *장비*\n"
                "*<https://mda.example.com/monitoring?device=MB2-TEST1|MB2-TEST1>*"
            ),
            "🚪 *병실*\n`1진료실`",
            ":rotating_light: *문제 장치*\n`LED`",
            "🔎 *감지 내용*\n*LED 이상*",
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
    assert blocks[2] == {
        "type": "section",
        "text": {"type": "mrkdwn", "text": (
            ":rotating_light: *문제 장치*\n`LED`\n\n🔎 *감지 내용*\n*LED 이상*"
        )},
    }
    assert [field["text"] for field in blocks[3]["fields"]] == [
        "📞 *전화*\n031-123-4567",
        "💬 *문자*\n010-1234-5678",
    ]


@pytest.mark.parametrize("category", [
    "recording", "recording_processing", "video_signal", "led", "audio",
    "application", "storage", "device_connection", "upload", "recording_video", "recording_audio",
])
def test_all_alert_categories_link_device_to_mda_search_in_blocks_and_text(category: str) -> None:
    # MDA 검색창이 쓰는 두 파라미터를 본문·카드·action target에서 함께 유지한다.
    item = {
        **_item(), "hospitalSeq": 43, "device": "MB2-A00071", "alertCategory": category,
        "alertGuidance": "영향: 상태 확인이 필요해.\n다음 확인: 연결 상태를 확인해.",
    }
    message = _rendered_message(item)
    url = "https://mda.kr.mmtalkbox.com/monitoring?hospitalSeq=43&focusDevice=MB2-A00071"
    link = f"*<{url}|MB2-A00071>*"
    assert link in message["text"]
    assert message["blocks"][1]["fields"][0]["text"] == f"⚙️ *장비*\n{link}"
    # 유형이 달라도 감지 영역의 폭·라벨·본문 순서는 같고 action target도 보존한다.
    detail = message["blocks"][2]
    assert "fields" not in detail
    assert detail["text"]["text"].count("🔎 *감지 내용*\n") == 1
    assert "상태 확인이 필요해" not in message["text"]
    assert "다음 확인" not in message["text"]
    assert detail["text"]["text"] in message["text"]
    action = next(block for block in message["blocks"] if block["type"] == "actions")
    target = json.loads(action["elements"][0]["value"])
    assert target["device"] == "MB2-A00071"
    assert target["mdaUrl"] == url


def test_notification_device_name_alias_and_encoded_search_value_preserve_target() -> None:
    # notification은 deviceName을 쓰며 검색값 안의 공백·&를 쿼리로 분리하면 안 된다.
    item = {**_item(), "device": None, "deviceName": "BOX TEST&A", "mdaUrl": "http://invalid.example"}
    normalized = alert._normalize_alert_item(item)
    parsed = urlsplit(normalized["mdaUrl"])
    assert parsed.scheme == "https"
    assert parsed.netloc == "mda.kr.mmtalkbox.com"
    assert parsed.path == "/monitoring"
    assert parse_qs(parsed.query) == {"hospitalSeq": ["1"], "focusDevice": ["BOX TEST&A"]}


@pytest.mark.parametrize(("hospital_seq", "device"), [(None, "MB2-A00071"), (0, "MB2-A00071"), (43, "")])
def test_missing_device_identity_does_not_invent_monitoring_target(hospital_seq: object, device: str) -> None:
    item = alert._normalize_alert_item({"hospitalSeq": hospital_seq, "device": device})
    assert item["mdaUrl"] == ""
    assert "<https://" not in alert._device_name_text(item)


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
    # block identity는 축약 전 원본 item이 아니라 Slack에 실제 실린
    # compact value와 결합돼 클릭 payload를 그대로 검증할 수 있어야 한다.
    assert action_block["block_id"] == alert._build_alert_action_block_id(
        card_index=0,
        action_value=value,
    )


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
    # action value 내부 연속 공백도 digest 계산 전 정규화하지 않고
    # Slack renderer가 발급한 현재 카드 identity 그대로 통과시킨다.
    message = _rendered_message({**_item(), "issue": "LED  연결 이상"})
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
        "담당자 <@U1> · `2026-08-31 09:07:12 KST`",
    ]
    assert updated_blocks[0]["text"]["text"] == "✅ 확인 완료 · LED 연결"
    updated_text = client.chat_update.call_args.kwargs["text"]
    assert updated_text == (
        "*✅ 확인 완료 · LED 연결*\n" + message["text"].split("\n", 1)[1]
    )
    assert updated_blocks[1]["text"]["text"].endswith("\n\n🏥 *1 테스트병원*")
    assert updated_blocks[1]["fields"] == message["blocks"][1]["fields"]
    assert updated_blocks[2:4] == message["blocks"][2:4]
    assert all("style" not in element for element in updated_blocks[4]["elements"])
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


@pytest.mark.parametrize("include_voice", [True, False])
@pytest.mark.parametrize("media_type", ["video", "audio"])
def test_mark_done_highlights_media_alert_without_losing_session_details(
    include_voice: bool, media_type: str,
) -> None:
    app = _App()
    bridge = Mock()
    # 클릭자와 API의 최초 담당자가 다르면 최초 receipt가 표시돼야 한다.
    bridge.mark_done.return_value = _mark_done_result(created=False)
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.video-completion"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(
        {
            **_item(),
            "alertCategory": f"recording_{media_type}",
            "barcode": "12345678901",
            "sessionAtLabel": "세션 시작",
            "sessionAt": "2026-08-31 08:37:41 KST",
            "issue": (
                "화면 정지 구간 감지\n영상 길이 60:00\n"
                "정지 감지 기준: 연속 10:00 이상\n감지 시각: 2026-08-31 09:00:00 KST"
            ) if media_type == "video" else (
                "녹화 파일 오디오 이상: 영상 길이 대비 오디오 길이 부족\n"
                "파일 길이 60:00 · 디코딩 오디오 59:00\n오디오 길이 차이 01:00 (1.7%)\n"
                "감지 시각: 2026-08-31 09:00:00 KST"
            ),
        },
        include_device_voice_action=include_voice,
    )
    client = _StatefulSlackClient(message)

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](
        Mock(),
        _rendered_action_body(message, device="MB2-TEST1", actor_id="U2"),
        client,
    )

    blocks = client.root["blocks"]
    media_label = "영상" if media_type == "video" else "오디오"
    assert blocks[0]["text"]["text"] == f"✅ 확인 완료 · 녹화 {media_label} 이상"
    # 완료 처리 뒤에도 오디오의 두 열과 그 아래 차이·시각 본문이 그대로 남는다.
    assert "60분" in json.dumps(blocks, ensure_ascii=False)
    if media_type == "audio":
        assert "길이 차이 *1분 · 1.7%*" in blocks[3]["text"]["text"]
    assert blocks[1]["text"]["text"] == (
        "담당자 <@U1> · `2026-08-31 09:07:12 KST`\n\n"
        "🏥 *1 테스트병원*"
    )
    # 사용자가 보는 카드 전체에서 완료 문구는 제목에 딱 한 번만 나온다.
    assert json.dumps(blocks, ensure_ascii=False).count("확인 완료") == 1
    assert blocks[1]["fields"] == message["blocks"][1]["fields"]
    assert "12345678901" in json.dumps(blocks[1], ensure_ascii=False)
    assert blocks[2:4] == message["blocks"][2:4]
    assert len(blocks[2]["fields"]) == (2 if media_type == "audio" else 1)
    expected_actions = {alert.DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL}
    if include_voice:
        expected_actions.add(alert.DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE)
    assert _card_action_ids(blocks, device="MB2-TEST1") == expected_actions
    assert all("style" not in element for element in blocks[4]["elements"])
    client.chat_postMessage.assert_not_called()


@pytest.mark.parametrize("previous_layout", ["footer", "duplicate_heading"])
def test_mark_done_updates_existing_layout_and_keeps_original_receipt(
    previous_layout: str,
) -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result(
        actor_id="U2", acknowledged_at="2026-08-31T00:08:13+00:00",
    )
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.legacy-completion"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    stale_body = _rendered_action_body(message, device="MB2-TEST1", actor_id="U2")
    client = _StatefulSlackClient(message)
    # 하단 표시와 제목·본문 중복 표시를 모두 지원하면서 최초 기록을 보존한다.
    if previous_layout == "footer":
        client.root["blocks"][3]["fields"].extend([
            {"type": "mrkdwn", "text": "✅ *확인 완료*\n담당자 <@U1>"},
            {"type": "mrkdwn", "text": "🕒 *처리 시간*\n`2026-08-31 09:07:12 KST`"},
        ])
    else:
        client.root["blocks"][0]["text"]["text"] = "✅ 확인 완료 · LED 연결"
        client.root["blocks"][1]["text"]["text"] = (
            "✅ *확인 완료*\n담당자 <@U1> · `2026-08-31 09:07:12 KST`\n\n"
            "*#1 테스트병원*"
        )
        client.root["text"] = (
            "*✅ 확인 완료 · LED 연결*\n" + message["text"].split("\n", 1)[1]
        )
    client.root["blocks"][4]["elements"] = [
        element for element in client.root["blocks"][4]["elements"]
        if element["action_id"] != DEVICE_HEALTH_ALERT_MARK_DONE_ACTION
    ]

    app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION](Mock(), stale_body, client)

    blocks = client.root["blocks"]
    assert _completion_rows(blocks) == [
        "담당자 <@U1> · `2026-08-31 09:07:12 KST`",
    ]
    assert json.dumps(blocks, ensure_ascii=False).count("확인 완료") == 1
    assert blocks[0]["text"]["text"] == "✅ 확인 완료 · LED 연결"
    assert blocks[3]["fields"] == message["blocks"][3]["fields"]
    assert all("style" not in element for element in blocks[4]["elements"])
    client.chat_update.assert_called_once()
    client.chat_postMessage.assert_not_called()


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
    # 공통 renderer의 별도 점검 요약은 기존 제목을 유지한다.
    assert first_blocks[0] == message["blocks"][0]
    assert first_blocks[5:] == message["blocks"][5:]
    assert client.chat_update.call_args_list[0].kwargs["text"] == message["text"]
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
        "✅ *확인 완료*\n담당자 <@U1> · `2026-08-31 09:07:12 KST`",
        "✅ *확인 완료*\n담당자 <@U2> · `2026-08-31 09:08:13 KST`",
    ]
    assert final_blocks[0] == message["blocks"][0]
    assert client.root["text"] == message["text"]
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
        "담당자 <@U1> · `2026-08-31 09:07:12 KST`",
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


@pytest.mark.parametrize("escaped_emoji", [True, False])
def test_mark_done_replay_accepts_existing_status_without_slack_update(
    escaped_emoji: bool,
) -> None:
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
    if escaped_emoji:
        # 실제 Slack 조회처럼 이모지 표현이 바뀌어도 완료 기록은 동일하다.
        client.root = json.loads(
            json.dumps(client.root, ensure_ascii=False).replace(
                "✅", ":white_check_mark:",
            )
        )

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


def test_mark_done_ignores_card_without_current_block_id_before_api() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-missing-block-id"),
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

    # 현재 renderer가 발급한 block identity가 없는 과거 카드는 더 이상
    # 운영 대상이 아니므로 API ACK나 Slack 갱신을 만들지 않는다.
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

    bridge.mark_done.assert_not_called()
    client.conversations_replies.assert_not_called()
    client.chat_update.assert_not_called()
    client.chat_postMessage.assert_not_called()


def test_mark_done_rejects_mismatched_block_id_without_poisoning_guard() -> None:
    app = _App()
    bridge = Mock()
    bridge.mark_done.return_value = _mark_done_result()
    alert.attach_device_alert_actions(
        app,
        logging.getLogger("test.alert.mark-done-mismatched-block-id"),
        lambda _workspace_id, _actor_id: True,
        bridge,
    )
    message = _rendered_message(_item())
    client = _StatefulSlackClient(message)
    invalid_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134832.709819",
    )
    valid_body = _rendered_action_body(
        message,
        device="MB2-TEST1",
        action_ts="1788134833.709819",
    )
    invalid_action = invalid_body["actions"][0]
    assert isinstance(invalid_action, dict)
    # 현재 형식처럼 보이더라도 value digest가 다르면 coordinator 예약 전
    # 거부해 뒤따르는 정상 클릭의 중복 방지 상태를 오염시키지 않는다.
    invalid_action["block_id"] = "device_alert_actions_0_0000000000000000"

    handler = app.actions[DEVICE_HEALTH_ALERT_MARK_DONE_ACTION]
    handler(Mock(), invalid_body, client)

    bridge.mark_done.assert_not_called()
    client.conversations_replies.assert_not_called()
    client.chat_update.assert_not_called()

    handler(Mock(), valid_body, client)

    bridge.mark_done.assert_called_once()
    client.conversations_replies.assert_called_once()
    client.chat_update.assert_called_once()
    client.chat_postMessage.assert_not_called()


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
