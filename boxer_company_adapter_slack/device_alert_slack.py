from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
import hashlib
import json
import logging
import re
import threading
from typing import Any, Callable, Mapping
from urllib.parse import urlencode, urlsplit
from zoneinfo import ZoneInfo

from boxer_company.transport_contracts import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
    DEVICE_HEALTH_ALERT_SMS_ACTION,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION,
    DEVICE_HEALTH_ALERT_VOICE_ACTION,
)
from boxer_company_adapter_slack.assistant_bridge import _commonmark_to_slack
from boxer_company_adapter_slack.device_health_alert_api import (
    DeviceHealthAlertApiBridge,
    DeviceHealthAlertApiTarget,
    build_device_health_alert_api_target,
    build_device_health_alert_request_id,
)


_KST = ZoneInfo("Asia/Seoul")
DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL = (
    DEVICE_HEALTH_ALERT_SMS_ACTION
)
DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE = (
    DEVICE_HEALTH_ALERT_VOICE_ACTION
)
DEVICE_HEALTH_ALERT_ACTION_MARK_DONE = DEVICE_HEALTH_ALERT_MARK_DONE_ACTION
DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS = (
    "device_health_alert_view_auto_sms"
)
DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID = (
    "device_health_alert_contact_hospital_modal"
)
DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID = (
    "device_health_alert_sms_phone"
)
DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID = (
    "device_health_alert_sms_phone_value"
)
DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID = (
    "device_health_alert_sms_message"
)
DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID = (
    "device_health_alert_sms_message_value"
)
_ACTION_IDS = frozenset(
    {
        DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
        DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
        DEVICE_HEALTH_ALERT_ACTION_MARK_DONE,
        DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS,
    }
)
_MOBILE_PHONE_PATTERN = re.compile(r"^(?:\+?82|0)1[016789][0-9]{7,8}$")
_ALERT_ITEM_LIMIT = 10
_ALERT_ACTION_BLOCK_ID_PATTERN = re.compile(
    r"^device_alert_actions_"
    r"(?P<card_index>0|[1-9][0-9]{0,2})_"
    r"(?P<digest>[0-9a-f]{16})$"
)
_MARK_DONE_CARD_STATE_LIMIT = 4_096
_MARK_DONE_MESSAGE_LOCK_STRIPES = 64
_MARK_DONE_STATUS_PREFIX = "✅ *확인 완료*"
_MARK_DONE_RECEIPT_PREFIX = "담당자 <@"
_MARK_DONE_TIME_PREFIX = "🕒 *처리 시간*"
_MDA_MONITORING_URL = "https://mda.kr.mmtalkbox.com/monitoring"
_CATEGORY_TITLES = {
    "recording": "녹화 상태 확인 필요",
    "recording_processing": "녹화 파일 처리 확인 필요",
    "video_signal": "영상 신호 확인 필요",
    "led": "LED 연결 확인 필요",
    "audio": "음성 출력 확인 필요",
    "application": "장비 앱 실행 확인 필요",
    "storage": "장비 저장 공간 부족",
    "device_connection": "장비 연결 확인 필요",
    # 업로드 내부 상태보다 CX가 바로 이해할 수 있는 장애 결과를 제목으로 쓴다.
    "upload": "업로드 실패 영상 감지",
    # 완성 파일의 영상/오디오 이상을 장비 연결·스피커 장애와 구분한다.
    "recording_video": "녹화 영상 이상 감지",
    "recording_audio": "녹화 오디오 이상 감지",
}
_COMPONENT_TITLES = {
    "captureboard": "캡처보드",
    "led": "LED",
    "audio": "스피커",
    "pm2": "PM2",
    "storage": "저장 공간",
}
# 기존 API의 상세 라벨은 계속 읽되, 표시에서는 비교값과 사실만 감지 내용에 모은다.
_ALERT_DETAIL_ICONS = {
    "감지 내용": "🔎",
    "측정 정보": "📊",
    "분석 구간": "🎞️",
    "감지 기준": "📏",
    "확인된 내용": "📝",
    "영향": "💥",
    "확인 필요": "⚠️",
    "추가 정보": "ℹ️",
    "다음 확인": "🛠️",
    "감지 시각": "🕒",
}
# 판정 이름을 풀어쓴 설명만 제거한다. 원인 코드·새 진단 내용은 그대로 남긴다.
_REDUNDANT_ALERT_FACTS = (
    "화면 변화가 없는 연속 구간을 감지했어.",
    "화면이 한 색으로 채워진 연속 구간을 감지했어.",
    "디코딩된 오디오가 영상보다 짧아.",
    "검사 시점에 녹화 파일을 찾지 못했어.",
    "파일에 영상 트랙이 없어.",
    "파일에 오디오 트랙이 없어.",
    "장비가 캡처보드 연결 이상을 보고했어. 케이블·입력·보드 중 원인은 확인 필요해.",
)


class _DeviceAlertMarkDoneCoordinator:
    """같은 카드의 완료 실행과 한 메시지의 Block Kit 갱신을 직렬화한다."""

    def __init__(self) -> None:
        self._state_lock = threading.Lock()
        self._card_states: OrderedDict[str, str] = OrderedDict()
        # 네트워크 호출은 메시지별 stripe에서만 직렬화해 느린 Slack root
        # 하나가 다른 모든 알림 카드의 완료 표시를 막지 않게 한다.
        self._message_locks = tuple(
            threading.Lock()
            for _index in range(_MARK_DONE_MESSAGE_LOCK_STRIPES)
        )

    def reserve(self, card_key: str) -> bool:
        with self._state_lock:
            if card_key in self._card_states:
                return False
            self._card_states[card_key] = "in_flight"
            self._prune_card_states()
            return True

    def complete(self, card_key: str) -> None:
        with self._state_lock:
            if self._card_states.get(card_key) != "in_flight":
                return
            self._card_states[card_key] = "completed"
            self._card_states.move_to_end(card_key)
            self._prune_card_states()

    def release(self, card_key: str) -> None:
        with self._state_lock:
            if self._card_states.get(card_key) == "in_flight":
                self._card_states.pop(card_key, None)

    def update_message(
        self,
        client: Any,
        identity: Mapping[str, Any],
        *,
        actor_user_id: str,
        completed_at: datetime,
        logger: logging.Logger,
    ) -> bool:
        channel_id = _text(identity.get("channelId"), "")
        message_ts = _text(identity.get("messageTs"), "")

        # payload의 message snapshot은 이미 오래됐을 수 있다. 같은 메시지의
        # 완료 갱신을 직렬화하고 Slack root를 매번 한 번 읽은 뒤 그 최신
        # blocks에만 완료 상태를 병합한다.
        with self._message_lock(identity):
            try:
                response = client.conversations_replies(
                    channel=channel_id,
                    ts=message_ts,
                    limit=1,
                )
                response_get = getattr(response, "get", None)
                raw_messages = (
                    response_get("messages")
                    if callable(response_get)
                    else None
                )
                if not isinstance(raw_messages, list):
                    raise RuntimeError("Slack root messages are missing")
                root_message = next(
                    (
                        message
                        for message in raw_messages
                        if isinstance(message, Mapping)
                        and _text(message.get("ts"), "") == message_ts
                    ),
                    None,
                )
                if root_message is None:
                    raise RuntimeError("Slack root message is missing")
                raw_blocks = root_message.get("blocks")
                root_text = root_message.get("text")
                if not isinstance(raw_blocks, list) or not isinstance(
                    root_text,
                    str,
                ):
                    raise RuntimeError("Slack root presentation is invalid")
                source_blocks = deepcopy(raw_blocks)
            except Exception as exc:
                logger.warning(
                    "Device alert mark-done root read failed error_type=%s",
                    type(exc).__name__,
                )
                return False
            updated_blocks = _mark_done_blocks(
                source_blocks,
                clicked_block_id=_text(identity.get("blockId"), ""),
                clicked_value=_raw_action_value(identity.get("actionValue")),
                actor_user_id=actor_user_id,
                completed_at=completed_at,
            )
            if updated_blocks is None:
                logger.warning(
                    "Device alert mark-done block not found channel=%s message_ts=%s",
                    channel_id,
                    message_ts,
                )
                return False
            # 단일 이상 알림의 제목을 바꿀 때 미리보기도 함께 갱신한다.
            # 별도 점검 요약처럼 제목을 유지하는 메시지는 원문을 보존한다.
            updated_text = root_text
            if updated_blocks[0]["text"] != source_blocks[0]["text"]:
                _, separator, body = root_text.partition("\n")
                updated_text = (
                    f"*{updated_blocks[0]['text']['text']}*{separator}{body}"
                )
            if updated_blocks == source_blocks and (
                _normalize_status_emoji(updated_text)
                == _normalize_status_emoji(root_text)
            ):
                # 최신 root에 같은 완료 상태가 있으면 replay는 Slack
                # mutation 없이 성공으로 끝낸다.
                return True
            try:
                client.chat_update(
                    channel=channel_id,
                    ts=message_ts,
                    text=updated_text,
                    blocks=updated_blocks,
                )
            except Exception as exc:
                logger.warning(
                    "Device alert mark-done UI update failed error_type=%s",
                    type(exc).__name__,
                )
                return False
            return True

    def _message_lock(
        self,
        identity: Mapping[str, Any],
    ) -> threading.Lock:
        message_key = "\x1f".join(
            (
                _text(identity.get("workspaceId"), ""),
                _text(identity.get("channelId"), ""),
                _text(identity.get("messageTs"), ""),
            )
        )
        digest = hashlib.sha256(message_key.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % len(self._message_locks)
        return self._message_locks[index]

    def _prune_card_states(self) -> None:
        while len(self._card_states) > _MARK_DONE_CARD_STATE_LIMIT:
            completed_key = next(
                (
                    key
                    for key, state in self._card_states.items()
                    if state == "completed"
                ),
                None,
            )
            if completed_key is None:
                return
            self._card_states.pop(completed_key, None)


def post_device_alert_summary(
    client: Any,
    report_summary: Mapping[str, Any],
    *,
    channel_id: str,
    logger: logging.Logger,
    include_actions: bool,
    include_device_voice_action: bool = True,
    client_msg_id: str,
) -> dict[str, str] | None:
    """semantic alert summary를 Slack text/blocks로만 렌더링한다."""

    raw_devices = report_summary.get("deviceResults")
    if not isinstance(raw_devices, list) or not raw_devices:
        return None
    items = tuple(
        _normalize_alert_item(item)
        for item in raw_devices
        if isinstance(item, Mapping)
    )
    if not items:
        return None
    title = _alert_title(items)
    # fallback text에는 모든 항목을 남기고 Block Kit만 Slack block 한도에
    # 맞춰 상위 카드로 제한한다.
    fallback_text = "\n".join(
        (
            f":alert: *{title}*",
            "\n\n".join(_alert_item_fallback_text(item) for item in items),
        )
    )
    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f":alert: {title}",
                "emoji": True,
            },
        }
    ]
    for card_index, item in enumerate(items[:_ALERT_ITEM_LIMIT]):
        # 장비 식별 정보 → 장애 내용 → 연락처 순서의 현재 카드 구조를
        # 한 줄짜리 section으로 뭉개지지 않게 고정한다.
        blocks.extend(
            _alert_item_blocks(
                item,
                card_index=card_index,
                include_actions=include_actions,
                include_device_voice_action=include_device_voice_action,
            )
        )
    omitted_count = max(0, len(items) - _ALERT_ITEM_LIMIT)
    if omitted_count:
        omitted_text = (
            f"알림 카드는 상위 {_ALERT_ITEM_LIMIT}건만 표시했어. "
            f"나머지 {omitted_count}건은 본문에서 확인해."
        )
        blocks.append(
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": omitted_text}],
            }
        )
    response = client.chat_postMessage(
        channel=channel_id,
        text=fallback_text,
        blocks=blocks,
        unfurl_links=False,
        unfurl_media=False,
        client_msg_id=client_msg_id,
    )
    message_ts = _response_value(response, "ts")
    if not message_ts:
        raise RuntimeError("장비 이상 알림 Slack ts를 받지 못했어")
    permalink = ""
    try:
        permalink_response = client.chat_getPermalink(
            channel=channel_id,
            message_ts=message_ts,
        )
        candidate = _response_value(permalink_response, "permalink")
        if _is_safe_slack_permalink(candidate, channel_id):
            permalink = candidate
    except Exception as exc:
        logger.warning(
            "Device alert permalink lookup failed error_type=%s",
            type(exc).__name__,
        )
    return {"messageTs": message_ts, "permalink": permalink}


def _alert_title(items: tuple[dict[str, Any], ...]) -> str:
    categories = [item["alertCategory"] for item in items]
    # 미분류 항목이나 여러 유형이 섞이면 일부 장애만 대표하지 않는다.
    if not categories or any(
        category not in _CATEGORY_TITLES for category in categories
    ):
        return "장비 상태 확인 필요"
    unique_categories = set(categories)
    if len(unique_categories) != 1:
        return "장비 상태 확인 필요"
    return _CATEGORY_TITLES[next(iter(unique_categories))]


def _alert_item_fallback_text(item: Mapping[str, Any]) -> str:
    # 미리보기·접근성 본문도 카드와 같은 순서, 라벨, 문단 간격을 사용한다.
    sections: list[str] = []
    for block in _alert_item_blocks(
        item,
        card_index=0,
        include_actions=False,
        include_device_voice_action=False,
    ):
        if "text" in block:
            sections.append(block["text"]["text"])
        sections.extend(field["text"] for field in block.get("fields", []))
    return "\n\n".join(sections)


def _alert_issue_mrkdwn(item: Mapping[str, Any]) -> str:
    return _render_alert_detail_sections(_alert_issue_sections(item))


def _alert_issue_sections(item: Mapping[str, Any]) -> dict[str, list[str]]:
    issue = str(item["issue"])
    if item.get("alertGuidance"):
        issue += "\n" + str(item["alertGuidance"])
    if item.get("alertCategory") in {"recording_audio", "recording_video"}:
        return _recording_media_issue_sections(issue)
    # 일반 장애도 같은 입력 분류를 거쳐 감지값·진단·시각을 표시한다.
    sections = {label: [] for label in _ALERT_DETAIL_ICONS}
    lines = issue.splitlines()
    match = re.search(r" \(발생 ([^)]+)\)$", lines[0])
    if match:
        sections["감지 시각"].append(match.group(1))
        lines[0] = lines[0][:match.start()]
    sections["감지 내용"].append(f"*{lines[0]}*")
    for line in lines[1:]:
        _append_alert_detail_line(sections, line)
    return sections


def _append_alert_detail_line(sections: dict[str, list[str]], line: str) -> None:
    # 수치·진단과 일반 안내를 분류해 표시 단계에서 반복 안내만 생략한다.
    if not line.strip():
        return
    for label in _ALERT_DETAIL_ICONS:
        if line.startswith(f"{label}: "):
            sections[label].append(line.removeprefix(f"{label}: "))
            return
    if ": " in line:
        label, value = line.split(": ", 1)
        sections["측정 정보"].append(f"{label} *{value}*")
    else:
        sections["추가 정보"].append(line)


def _alert_detail_fields(sections: Mapping[str, list[str]]) -> list[dict[str, str]]:
    # 짧은 기준과 감지 시각은 실제 Slack fields 두 열로 나란히 배치한다.
    return [
        {
            "type": "mrkdwn",
            "text": f"{_ALERT_DETAIL_ICONS[label]} *{label}*\n" + "\n".join(sections[label]),
        }
        for label in ("감지 기준", "감지 시각") if sections.get(label)
    ]


def _render_alert_detail_sections(
    sections: Mapping[str, list[str]], *, include_metadata: bool = True,
) -> str:
    # 접근성 본문도 카드처럼 두 길이 → 차이·진단 → 시각 순서로 읽힌다.
    audio_fields = _audio_comparison_fields(sections)
    if audio_fields:
        paragraphs = ["🔎 *감지 내용*", *(field["text"] for field in audio_fields)]
        body = _render_alert_detail_body(sections, compare_lengths=False)
        if body:
            paragraphs.append(body)
    else:
        paragraphs = ["🔎 *감지 내용*\n" + _render_alert_detail_body(sections)]
    if include_metadata:
        paragraphs.extend(field["text"] for field in _alert_detail_fields(sections))
    return "\n\n".join(paragraphs)


def _audio_comparison_fields(sections: Mapping[str, list[str]]) -> list[dict[str, str]]:
    # 한쪽을 확인하지 못했어도 빈칸이나 0으로 대신하지 않고 원래 상태를 표시한다.
    fields = []
    for label in ("확인된 오디오", "영상 전체"):
        measurement = next((value for value in sections["측정 정보"] if value.startswith(f"{label} ")), "")
        if not measurement:
            return []
        fields.append({"type": "mrkdwn", "text": f"{label}\n{measurement.removeprefix(label + ' ')}"})
    return fields


def _render_alert_detail_body(
    sections: Mapping[str, list[str]], *, compare_lengths: bool = True,
) -> str:
    # 최장/합계는 영상 길이와, 오디오는 확인된 길이와 직접 비교한다.
    # 최초 시작을 최장 구간의 시작으로 해석하거나 새 비율·끝 시각을 계산하지 않는다.
    headlines = list(sections["감지 내용"])
    measurements = list(sections["측정 정보"])
    video = next((value for value in measurements if value.startswith("영상 전체 ")), "")
    audio = next((value for value in measurements if value.startswith("확인된 오디오 ")), "")
    if video:
        measurements.remove(video)
    if audio:
        measurements.remove(audio)
    if audio and compare_lengths:
        headlines.append(" / ".join(value for value in (audio, video) if value))
    elif video and compare_lengths:
        compared = False
        for index, headline in enumerate(headlines):
            if headline.startswith(("최장 연속 ", "단색 화면 합계 ", "오디오 데이터 누락 ")):
                headlines[index] = f"{headline} / {video}"
                compared = True
        if not compared:
            headlines.append(video)

    lines = headlines + measurements + sections["분석 구간"]
    for fact in sections["확인된 내용"]:
        for repeated in _REDUNDANT_ALERT_FACTS:
            fact = fact.replace(repeated, "")
        if fact.strip():
            lines.append(fact.strip())
    lines.extend(sections["추가 정보"])
    lines.extend(f"※ {note}" for note in dict.fromkeys(sections["확인 필요"]))
    return "\n".join(lines)


def _recording_media_issue_mrkdwn(issue: str) -> str:
    return _render_alert_detail_sections(_recording_media_issue_sections(issue))


def _recording_media_issue_sections(issue: str) -> dict[str, list[str]]:
    """API 판정과 실제 비교값을 분류하고 추론 없이 표시용 문구를 만든다."""

    def duration(value: str) -> str:
        # 오디오 비교값만 소수점 한 자리로 반올림한다. 먼저 총초를 반올림해
        # 59.99초가 60초로 넘어갈 때 분도 올리고, action의 원본은 바꾸지 않는다.
        match = re.fullmatch(r"(\d+):(\d{2}(?:\.\d+)?)", value)
        if not match:
            return value
        minutes, seconds = match.groups()
        if issue.startswith("녹화 파일 오디오 이상: "):
            total = (Decimal(minutes) * 60 + Decimal(seconds)).quantize(Decimal("0.1"), rounding=ROUND_HALF_UP)
            minutes, remainder = divmod(total, 60)
            minutes = str(int(minutes))
            seconds = format(remainder, "f").rstrip("0").rstrip(".")
        parts = [f"{int(minutes)}분"] if int(minutes) else []
        seconds = seconds.lstrip("0") or "0"
        if seconds.startswith("."):
            seconds = "0" + seconds
        if seconds != "0" or not parts:
            parts.append(f"{seconds}초")
        return " ".join(parts)

    sections = {label: [] for label in _ALERT_DETAIL_ICONS}
    headline, comparisons = sections["감지 내용"], sections["측정 정보"]
    reason = ""
    summarized_reasons = set()
    for index, line in enumerate(issue.splitlines()):
        if line.startswith(("녹화 파일 오디오 이상: ", "녹화 파일 영상 이상: ")):
            reason = line.split(": ", 1)[1]
            match = re.search(r" \(발생 ([^)]+)\)$", reason)
            if match:
                sections["감지 시각"].append(match.group(1))
                reason = reason[:match.start()]
        elif line.startswith("파일 길이 ") and " · 디코딩 오디오 " in line:
            video, audio = line.removeprefix("파일 길이 ").split(" · 디코딩 오디오 ", 1)
            comparisons.extend([f"영상 전체 *{duration(video)}*", f"확인된 오디오 *{duration(audio)}*"])
        elif line == "영상 길이 확인 필요":
            comparisons.append("영상 전체 *확인 필요*")
        elif line.startswith("영상 길이 "):
            comparisons.append(f"영상 전체 *{duration(line.removeprefix('영상 길이 '))}*")
        elif line.startswith("디코딩 오디오 "):
            comparisons.append(f"확인된 오디오 *{duration(line.removeprefix('디코딩 오디오 '))}*")
        elif (match := re.fullmatch(r"오디오 길이 차이 (\S+) \(([^)]+)\)", line)):
            headline.append(f"길이 차이 *{duration(match[1])} · {match[2]}*")
            summarized_reasons.add("영상 길이 대비 오디오 길이 부족")
        elif (match := re.fullmatch(r"오디오 데이터 누락 합계 (\S+) \(전체의 ([^)]+)\)", line)):
            headline.append(f"오디오 데이터 누락 *{duration(match[1])}* (영상의 {match[2]})")
            summarized_reasons.add("영상 길이 대비 오디오 길이 부족")
        elif line.startswith("오디오 데이터 누락 구간: "):
            sections["분석 구간"].append("오디오 누락 구간 " + line.removeprefix("오디오 데이터 누락 구간: ") + " (영상 시작 기준)")
        elif line == "오디오 데이터 누락 구간 및 비율: 확인 필요 (완료된 구간 분석 없음)":
            sections["확인 필요"].append(
                "길이 기준 비교이며 실제 누락 구간은 미확인"
                if "영상 길이 대비 오디오 길이 부족" in summarized_reasons
                else "실제 누락 구간·합계·비율은 미확인"
            )
        elif (match := re.fullmatch(r"단색 감지 합계 (\S+) \(전체의 ([^)]+)\)", line)):
            headline.append(f"단색 화면 합계 *{duration(match[1])}* (영상의 {match[2]})")
            summarized_reasons.add("단색 화면 구간 감지")
        elif line.startswith("단색 감지 구간: "):
            sections["분석 구간"].append("단색 구간 " + line.removeprefix("단색 감지 구간: ") + " (영상 시작 기준)")
        elif line.startswith(("정지 알림 기준: ", "단색 알림 기준: ")):
            # 새 비율 정책은 고정 초 기준과 구분하며, 이전 카드의 시간 문구도 계속 읽는다.
            kind, criterion = line.split(" 알림 기준: ", 1)
            sections["감지 기준"].append(f"{kind}: {criterion}")
        elif line.startswith("감지 기준: "):
            sections["감지 기준"].append("단색: " + line.removeprefix("감지 기준: ").removesuffix(" · 구간은 영상 시작 기준"))
        elif (match := re.fullmatch(r"첫 단색 감지 시작: (\S+) \(영상 시작 기준\)", line)):
            comparisons.append(f"첫 단색 시작 *{match[1]}* (영상 시작 기준)")
        elif line.startswith("최장 연속 단색: "):
            headline.append(f"최장 연속 단색 *{duration(line.removeprefix('최장 연속 단색: '))}*")
            summarized_reasons.add("단색 화면 구간 감지")
        elif line == "전체 단색 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)":
            # 최장 구간으로 표시하며 확인되지 않은 합계·비율을 추가하지 않는다.
            continue
        elif (match := re.fullmatch(r"첫 정지 감지 시작: (\S+) \(영상 시작 기준\)", line)):
            comparisons.append(f"첫 정지 시작 *{match[1]}* (영상 시작 기준)")
        elif (match := re.fullmatch(r"정지 감지 기준: 연속 (\S+) 이상", line)):
            sections["감지 기준"].append(f"정지: 연속 {duration(match[1])} 이상")
        elif line == "전체 정지 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)":
            sections["확인 필요"].append("첫 정지와 최장 정지는 서로 다른 구간일 수 있어.")
        elif index == 0 and line.strip():
            # 아직 분류하지 못한 API 판정도 감지 내용으로 보존한다.
            headline.append(f"*{line}*")
        else:
            _append_alert_detail_line(sections, line)

    # 정지는 최장 연속 시간만 보고되므로 단색 합계와 합산하거나 비율을 추정하지 않는다.
    reasons = []
    for item in reason.split(", ") if reason else []:
        match = re.fullmatch(r"화면 정지 구간 감지 \(최장 연속 정지 ([^)]+)\)", item)
        if match:
            reasons.append(f"최장 연속 정지 *{match[1]}*")
        elif item == "화면 정지 구간 감지 (정지 시간 확인 필요)":
            reasons.append("*화면 정지 구간 감지*")
            sections["확인 필요"].append("정지 시간은 아직 확인되지 않았어.")
        elif item not in summarized_reasons:
            reasons.append(f"*{item}*")
    headline.extend(reasons)
    return sections


def _alert_item_blocks(
    item: Mapping[str, Any],
    *,
    card_index: int,
    include_actions: bool,
    include_device_voice_action: bool,
) -> list[dict[str, Any]]:
    components = _problem_components_text(item.get("problemComponents"))
    issue_sections: list[str] = []
    if components:
        issue_sections.append(f":rotating_light: *문제 장치*\n{components}")
    detail_sections = _alert_issue_sections(item)
    audio_fields = _audio_comparison_fields(detail_sections)
    issue_sections.append(
        "🔎 *감지 내용*" if audio_fields
        else _render_alert_detail_sections(detail_sections, include_metadata=False)
    )
    detail_block: dict[str, Any] = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": "\n\n".join(issue_sections)},
    }
    detail_fields = audio_fields or _alert_detail_fields(detail_sections)
    if detail_fields:
        detail_block["fields"] = detail_fields
    identity_fields = [
        {
            "type": "mrkdwn",
            "text": f"⚙️ *장비*\n{_device_name_text(item)}",
        },
        {
            "type": "mrkdwn",
            "text": f"🚪 *병실*\n`{item['room']}`",
        },
    ]
    # 녹화 파일 알림은 바코드와 세션 시각까지 같은 식별 영역에 표시한다.
    if item.get("barcode") or item.get("sessionAt"):
        identity_fields.extend(
            (
                {
                    "type": "mrkdwn",
                    "text": (
                        "🏷️ *바코드*\n"
                        f"`{_text(item.get('barcode'), '미확인')}`"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        f"🕐 *{_text(item.get('sessionAtLabel'), '세션 시각')}*\n"
                        f"{_text(item.get('sessionAt'), '미확인')}"
                    ),
                },
            )
        )
    # 병원 번호와 action 원본은 유지하고 제목의 #만 병원 아이콘으로 바꾼다.
    hospital_title = item["hospital"].removeprefix("#")
    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"🏥 *{hospital_title}*"},
            "fields": identity_fields,
        },
        # 비교값은 전체 폭 한 문단, 기준·시각은 두 열로 묶는다.
        # section 수는 유지해 기존 완료 카드와 같은 위치 계약을 사용한다.
        detail_block,
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"📞 *전화*\n{_text(item.get('telephone'), '미확인')}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"💬 *문자*\n{_sms_contact_text(item)}",
                },
            ],
        },
    ]
    if audio_fields:
        # fields 뒤에는 같은 section의 text를 놓을 수 없으므로 다음 연락처
        # section 앞부분에 차이·주석·시각을 둔다. 세 section의 완료 위치 계약은 유지한다.
        footer = [_render_alert_detail_body(detail_sections, compare_lengths=False)]
        footer.extend(field["text"] for field in _alert_detail_fields(detail_sections))
        footer_text = "\n\n".join(part for part in footer if part)
        if footer_text:
            blocks[2]["text"] = {"type": "mrkdwn", "text": footer_text}
    if not include_actions:
        return blocks

    # 버튼에는 현재 API action이 요구하는 exact target만 넣어 Slack value
    # 제한과 자동문자 본문 노출을 피한다.
    value = _alert_action_value(item)
    action_elements = [
        {
            "type": "button",
            "action_id": DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
            "text": {"type": "plain_text", "text": "병원 문자 보내기"},
            "value": value,
            "style": "primary",
        }
    ]
    if include_device_voice_action:
        action_elements.append(
            {
                "type": "button",
                "action_id": DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE,
                "text": {"type": "plain_text", "text": "장비 음성 안내"},
                "value": value,
            }
        )
    action_elements.append(
        {
            "type": "button",
            "action_id": DEVICE_HEALTH_ALERT_ACTION_MARK_DONE,
            "text": {"type": "plain_text", "text": "확인 완료"},
            "value": value,
        }
    )
    blocks.append(
        {
            "type": "actions",
            "block_id": _build_alert_action_block_id(
                card_index=card_index,
                action_value=value,
            ),
            "elements": action_elements,
        }
    )
    return blocks


def _build_alert_action_block_id(
    *,
    card_index: int,
    action_value: str,
) -> str:
    """현재 카드 index와 실제 Slack value를 하나의 불변 ID로 묶는다."""

    if (
        type(card_index) is not int
        or not 0 <= card_index < _ALERT_ITEM_LIMIT
        or not isinstance(action_value, str)
        or not action_value
        or len(action_value) > 1900
    ):
        raise ValueError("장비 이상 알림 action block identity가 올바르지 않아")
    digest = hashlib.sha256(action_value.encode("utf-8")).hexdigest()[:16]
    return f"device_alert_actions_{card_index}_{digest}"


def _parse_current_alert_action_block_id(
    *,
    block_id: str,
    action_value: str,
) -> int | None:
    """현재 renderer가 발급한 exact block ID만 card index로 복원한다."""

    if (
        not isinstance(block_id, str)
        or not isinstance(action_value, str)
        or not action_value
        or len(action_value) > 1900
    ):
        return None
    matched = _ALERT_ACTION_BLOCK_ID_PATTERN.fullmatch(block_id)
    if matched is None:
        return None
    card_index = int(matched.group("card_index"))
    if not 0 <= card_index < _ALERT_ITEM_LIMIT:
        return None
    expected = _build_alert_action_block_id(
        card_index=card_index,
        action_value=action_value,
    )
    return card_index if block_id == expected else None


def _device_name_text(item: Mapping[str, Any]) -> str:
    device = _text(item.get("device"), "장비명 미확인")
    mda_url = _text(item.get("mdaUrl"), "")
    if _is_safe_https_url(mda_url):
        return f"*<{mda_url}|{device}>*"
    return f"`{device}`"


def _problem_components_text(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    labels = [_text(item, "") for item in value if _text(item, "")]
    return " ".join(f"`{label}`" for label in labels)


def _sms_contact_text(item: Mapping[str, Any]) -> str:
    return _text(
        item.get("smsPhoneNumber") or item.get("deviceAlertPhone"),
        "저장된 번호 없음 · 자동발송 불가",
    )


def _alert_action_value(item: Mapping[str, Any]) -> str:
    target = {
        key: item.get(key)
        for key in (
            "hospitalSeq",
            "hospitalName",
            "hospital",
            "room",
            "device",
            "issue",
            "alertCategory",
            "problemComponents",
            "mdaUrl",
        )
    }
    value = json.dumps(target, ensure_ascii=False, separators=(",", ":"))
    if len(value) <= 1900:
        return value

    # Slack button value 제한을 넘으면 API action에 필요한 식별 필드는
    # 유지하고 표시 문자열만 상한 안에서 줄여 항상 유효한 JSON을 보낸다.
    compact_target = {
        "hospitalSeq": target["hospitalSeq"],
        "hospitalName": _text(target.get("hospitalName"), "")[:120],
        "hospital": _text(target.get("hospital"), "")[:180],
        "room": _text(target.get("room"), "")[:100],
        "device": _text(target.get("device"), "")[:100],
        "issue": _text(target.get("issue"), "")[:300],
        "alertCategory": _text(target.get("alertCategory"), "")[:80],
        "problemComponents": [
            _text(component, "")[:60]
            for component in (
                target.get("problemComponents")
                if isinstance(target.get("problemComponents"), list)
                else []
            )[:8]
            if _text(component, "")
        ],
        # 긴 URL은 action 실행에 필수가 아니고 중간 절단하면 오히려
        # 잘못된 링크가 되므로 축약 payload에서는 제거한다.
        "mdaUrl": "",
    }
    compact_value = json.dumps(
        compact_target,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    if len(compact_value) > 1900:
        raise RuntimeError("장비 이상 알림 action payload가 너무 커")
    return compact_value


def attach_device_alert_actions(
    app: Any,
    logger: logging.Logger,
    base_access_checker: Callable[[str | None, str | None], bool] | None,
    action_api_bridge: DeviceHealthAlertApiBridge | None,
) -> None:
    """Slack action/modal을 membership 검사 뒤 API bridge에만 연결한다."""

    mark_done_coordinator = _DeviceAlertMarkDoneCoordinator()

    def action_handler(action_id: str) -> Callable[..., None]:
        def handle(ack: Any, body: dict[str, Any], client: Any) -> None:
            ack()
            payload = body if isinstance(body, dict) else {}
            if not _actor_allowed(payload, base_access_checker, logger):
                return
            if action_api_bridge is None:
                _post_thread_reply(
                    client,
                    payload,
                    "장비 이상 알림 API가 준비되지 않아 작업하지 않았어",
                    logger,
                )
                return
            if action_id in {
                DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL,
                DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS,
            }:
                _open_sms_modal(
                    payload,
                    client,
                    logger,
                    action_api_bridge,
                    action_id=action_id,
                )
                return
            _execute_remote_action(
                payload,
                client,
                logger,
                action_api_bridge,
                action_id=action_id,
                mark_done_coordinator=mark_done_coordinator,
            )

        return handle

    for action_id in sorted(_ACTION_IDS):
        app.action(action_id)(action_handler(action_id))

    def modal_submission(
        ack: Any,
        body: dict[str, Any],
        client: Any,
    ) -> None:
        payload = body if isinstance(body, dict) else {}
        if not _actor_allowed(payload, base_access_checker, logger):
            ack()
            return
        parsed = _parse_modal_submission(payload)
        errors = _modal_errors(parsed)
        if errors:
            ack(response_action="errors", errors=errors)
            return
        ack()
        if action_api_bridge is None:
            _post_thread_reply(
                client,
                parsed,
                "장비 이상 알림 API가 준비되지 않아 문자를 보내지 않았어",
                logger,
            )
            return
        _send_remote_sms(parsed, client, logger, action_api_bridge)

    app.view(DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID)(modal_submission)


def _normalize_alert_item(raw: Mapping[str, Any]) -> dict[str, Any]:
    hospital_seq = _positive_int(raw.get("hospitalSeq"))
    hospital_name = _text(
        raw.get("hospitalName") or raw.get("hospital"),
        "병원 미확인",
    )
    hospital_label = _text(raw.get("hospital"), "")
    if not hospital_label:
        hospital_label = (
            f"#{hospital_seq} {hospital_name}"
            if hospital_seq is not None
            else hospital_name
        )
    components = raw.get("problemComponents")
    if not isinstance(components, list):
        labels = raw.get("componentLabels")
        components = (
            [key for key, value in labels.items() if value == "이상"]
            if isinstance(labels, Mapping)
            else []
        )
    normalized_components = []
    for item in components:
        component = _text(item, "")
        if component:
            normalized_components.append(
                _COMPONENT_TITLES.get(component.casefold(), component)
            )
    issue = raw.get("issue") or raw.get("priorityReason")
    # API가 작성한 원인·영향·다음 확인의 줄 구분을 알림 종류와 무관하게 보존한다.
    issue_text = "\n".join(_text(line, "") for line in str(issue or "").splitlines() if line.strip())
    device = _text(raw.get("device") or raw.get("deviceName"), "")
    mda_url = _text(raw.get("mdaUrl"), "")
    if not _is_safe_https_url(mda_url) and hospital_seq and device:
        # MDA 장비 검색과 동일하게 병원 선택과 장비 focus를 함께 전달한다.
        # 공통 카드에서 만들면 알림 종류·구형 pending의 URL 누락과 무관하게 적용된다.
        mda_url = f"{_MDA_MONITORING_URL}?{urlencode({'hospitalSeq': hospital_seq, 'focusDevice': device})}"
    return {
        "hospitalSeq": hospital_seq or 0,
        "hospitalName": hospital_name,
        "hospital": hospital_label,
        "room": _text(raw.get("room") or raw.get("roomName"), "병실 미확인"),
        "device": device or "장비명 미확인",
        "issue": issue_text or "상세 확인 필요",
        "alertGuidance": "\n".join(_text(line, "") for line in str(raw.get("alertGuidance") or "").splitlines() if line.strip()),
        "alertCategory": _text(raw.get("alertCategory"), "device_connection"),
        "problemComponents": normalized_components,
        "telephone": _text(raw.get("telephone") or raw.get("hospitalTelephone"), ""),
        "deviceAlertPhone": _text(
            raw.get("deviceAlertPhone") or raw.get("hospitalDeviceAlertPhone"),
            "",
        ),
        "smsPhoneNumber": _text(raw.get("smsPhoneNumber"), ""),
        "mdaUrl": mda_url,
        "barcode": _text(raw.get("barcode"), ""),
        "sessionAtLabel": _text(raw.get("sessionAtLabel"), ""),
        "sessionAt": _text(raw.get("sessionAt"), ""),
    }


def _actor_allowed(
    body: Mapping[str, Any],
    checker: Callable[[str | None, str | None], bool] | None,
    logger: logging.Logger,
) -> bool:
    team = body.get("team") if isinstance(body.get("team"), Mapping) else {}
    user = body.get("user") if isinstance(body.get("user"), Mapping) else {}
    workspace_id = _text(team.get("id") or body.get("team_id"), "")
    actor_id = _text(user.get("id"), "")
    if checker is None or not workspace_id or not actor_id:
        return False
    try:
        return bool(checker(workspace_id, actor_id))
    except Exception as exc:
        logger.warning(
            "Device alert membership check failed error_type=%s",
            type(exc).__name__,
        )
        return False


def _action_identity(body: Mapping[str, Any]) -> dict[str, Any]:
    actions = body.get("actions") if isinstance(body.get("actions"), list) else []
    action = actions[0] if actions and isinstance(actions[0], Mapping) else {}
    team = body.get("team") if isinstance(body.get("team"), Mapping) else {}
    user = body.get("user") if isinstance(body.get("user"), Mapping) else {}
    channel = body.get("channel") if isinstance(body.get("channel"), Mapping) else {}
    container = (
        body.get("container")
        if isinstance(body.get("container"), Mapping)
        else {}
    )
    message = body.get("message") if isinstance(body.get("message"), Mapping) else {}
    message_ts = _text(
        message.get("ts") or container.get("message_ts"),
        "",
    )
    return {
        "workspaceId": _text(team.get("id") or body.get("team_id"), ""),
        "actorUserId": _text(user.get("id"), ""),
        "channelId": _text(
            channel.get("id") or container.get("channel_id"),
            "",
        ),
        "messageTs": message_ts,
        "threadTs": _text(
            message.get("thread_ts") or container.get("thread_ts"),
            message_ts,
        ),
        "interactionId": _text(action.get("action_ts"), ""),
        "triggerId": _text(body.get("trigger_id"), ""),
        "blockId": _text(action.get("block_id"), ""),
        # block ID digest는 Slack에 실린 value 원문을 기준으로 하므로
        # 공통 공백 정규화를 거치지 않고 interaction 전체에서 보존한다.
        "actionValue": _raw_action_value(action.get("value")),
        "item": _parse_json_mapping(action.get("value")),
    }


def _open_sms_modal(
    body: Mapping[str, Any],
    client: Any,
    logger: logging.Logger,
    bridge: DeviceHealthAlertApiBridge,
    *,
    action_id: str,
) -> None:
    identity = _action_identity(body)
    item = _normalize_alert_item(identity["item"])
    target = build_device_health_alert_api_target(item)
    status = "modal_open_failed"
    error_type = ""
    try:
        request_id = build_device_health_alert_request_id(
            workspace_id=identity["workspaceId"],
            actor_user_id=identity["actorUserId"],
            channel_id=identity["channelId"],
            message_ts=identity["messageTs"],
            interaction_id=identity["interactionId"],
            action_name=DEVICE_HEALTH_ALERT_SMS_ACTION,
            phase="prepare",
        )
        prepared = bridge.prepare_sms(
            request_id=request_id,
            workspace_id=identity["workspaceId"],
            actor_user_id=identity["actorUserId"],
            channel_id=identity["channelId"],
            conversation_id=identity["threadTs"],
            target=target,
        )
        receipt = dict(prepared.operation_result or {})
        phone = _text(receipt.get("phoneNumber"), "")
        message = _text(receipt.get("message"), "")
        metadata = {
            **identity,
            "item": item,
            "actionId": action_id,
        }
        client.views_open(
            trigger_id=identity["triggerId"],
            view=_sms_modal_view(metadata, phone=phone, message=message),
        )
        status = "modal_opened"
    except Exception as exc:
        error_type = type(exc).__name__
        logger.warning(
            "Device alert SMS modal failed error_type=%s",
            error_type,
        )
        _post_thread_reply(
            client,
            identity,
            "병원 문자 입력창을 열지 못했어. 수동으로 처리해줘",
            logger,
        )
    try:
        receipt_request_id = build_device_health_alert_request_id(
            workspace_id=identity["workspaceId"],
            actor_user_id=identity["actorUserId"],
            channel_id=identity["channelId"],
            message_ts=identity["messageTs"],
            interaction_id=identity["interactionId"],
            action_name=DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION,
            phase="receipt",
        )
        bridge.record_modal_receipt(
            request_id=receipt_request_id,
            workspace_id=identity["workspaceId"],
            actor_user_id=identity["actorUserId"],
            channel_id=identity["channelId"],
            conversation_id=identity["threadTs"],
            target=target,
            action_id=action_id,
            mode="send",
            message_ts=identity["messageTs"],
            thread_ts=identity["threadTs"],
            occurred_at=datetime.now(_KST).isoformat(),
            status=status,
            ok=status == "modal_opened",
            error_type=error_type,
        )
    except Exception as exc:
        logger.warning(
            "Device alert modal receipt failed error_type=%s",
            type(exc).__name__,
        )


def _execute_remote_action(
    body: Mapping[str, Any],
    client: Any,
    logger: logging.Logger,
    bridge: DeviceHealthAlertApiBridge,
    *,
    action_id: str,
    mark_done_coordinator: _DeviceAlertMarkDoneCoordinator,
) -> None:
    identity = _action_identity(body)
    is_mark_done = action_id == DEVICE_HEALTH_ALERT_ACTION_MARK_DONE
    if is_mark_done and _parse_current_alert_action_block_id(
        block_id=_text(identity.get("blockId"), ""),
        action_value=_raw_action_value(identity.get("actionValue")),
    ) is None:
        # 과거 카드의 버튼은 이미 운영 대상이 아니다. 현재 renderer가
        # 발급한 identity가 아니면 coordinator나 API mutation 전에 닫는다.
        logger.info(
            "Device alert mark-done ignored unsupported_card_format"
        )
        return
    mark_done_key = _mark_done_card_key(identity) if is_mark_done else ""
    if is_mark_done and (
        not mark_done_key or not mark_done_coordinator.reserve(mark_done_key)
    ):
        # Slack redelivery와 동시에 들어온 두 번째 클릭은 같은 카드에서
        # API 실행·감사 이벤트·스레드 답글을 다시 만들지 않는다.
        return
    try:
        target = build_device_health_alert_api_target(identity["item"])
        request_id = build_device_health_alert_request_id(
            workspace_id=identity["workspaceId"],
            actor_user_id=identity["actorUserId"],
            channel_id=identity["channelId"],
            message_ts=identity["messageTs"],
            interaction_id=identity["interactionId"],
            action_name=action_id,
            phase="execute",
        )
        method = (
            bridge.send_voice_guide
            if action_id == DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE
            else bridge.mark_done
        )
        result = method(
            request_id=request_id,
            workspace_id=identity["workspaceId"],
            actor_user_id=identity["actorUserId"],
            channel_id=identity["channelId"],
            conversation_id=(
                identity["messageTs"]
                if is_mark_done
                else identity["threadTs"]
            ),
            target=target,
        )
        messages = result.messages
        if (
            action_id == DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE
            and result.outcome == "answered"
        ):
            # API가 전송 성공을 확인한 경우에만 클릭한 사람과 대상을 카드로
            # 표시한다. 명령 전송 결과를 실제 음성 재생 완료로 표현하지 않는다.
            text, blocks = _voice_guide_success_reply(
                target, actor_user_id=identity["actorUserId"]
            )
            _post_thread_reply(client, identity, text, logger, blocks=blocks)
            return
        if is_mark_done:
            if str(result.outcome or "").strip() == "answered":
                if result.operation_result is None:
                    # Slack을 API보다 먼저 배포한 호환 창에는 구 API의 완료
                    # 댓글 동작을 유지하고 새 카드 상태는 아직 만들지 않는다.
                    mark_done_coordinator.release(mark_done_key)
                else:
                    acknowledgement = _mark_done_receipt(
                        result.operation_result
                    )
                    if acknowledgement is None:
                        raise RuntimeError(
                            "device health alert ack receipt is invalid"
                        )
                    actor_user_id, completed_at, _created = acknowledgement
                    ui_updated = mark_done_coordinator.update_message(
                        client,
                        identity,
                        actor_user_id=actor_user_id,
                        completed_at=completed_at,
                        logger=logger,
                    )
                    if ui_updated:
                        mark_done_coordinator.complete(mark_done_key)
                        # 성공 상태는 root 카드가 정본이다. 새 receipt와 replay
                        # 모두 완료 스레드 댓글을 추가하지 않는다.
                        messages = ()
                    else:
                        # API claim은 되돌리지 않는다. 가드는 풀어 다음 클릭이
                        # 최초 receipt로 UI만 복구할 수 있게 한다.
                        mark_done_coordinator.release(mark_done_key)
                        warning = (
                            "확인 완료 기록은 남겼지만 카드 표시를 갱신하지 "
                            "못했어. 잠시 후 다시 눌러 표시만 복구해줘"
                        )
                        messages = (warning,)
            else:
                mark_done_coordinator.release(mark_done_key)
    except Exception as exc:
        if is_mark_done and mark_done_key:
            mark_done_coordinator.release(mark_done_key)
        logger.warning(
            "Device alert remote action failed error_type=%s",
            type(exc).__name__,
        )
        messages = (
            "장비 이상 알림 작업 결과를 확인하지 못했어. "
            "중복 실행하지 말고 운영 로그를 확인해줘",
        )
    for message in messages:
        _post_thread_reply(client, identity, message, logger)


def _mark_done_card_key(identity: Mapping[str, Any]) -> str:
    parts = (
        _text(identity.get("workspaceId"), ""),
        _text(identity.get("channelId"), ""),
        _text(identity.get("messageTs"), ""),
    )
    item = identity.get("item")
    if any(not part for part in parts) or not isinstance(item, Mapping):
        return ""
    block_id = _text(identity.get("blockId"), "")
    action_value = _raw_action_value(identity.get("actionValue"))
    if _parse_current_alert_action_block_id(
        block_id=block_id,
        action_value=action_value,
    ) is None:
        return ""
    canonical_item = json.dumps(
        dict(item),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(
        "\x1f".join((*parts, block_id, canonical_item)).encode("utf-8")
    ).hexdigest()


def _mark_done_receipt(
    value: Any,
) -> tuple[str, datetime, bool] | None:
    if not isinstance(value, Mapping):
        return None
    actor_user_id = _text(value.get("actorUserId"), "")
    try:
        acknowledged_at = datetime.fromisoformat(
            _text(value.get("acknowledgedAt"), "").replace("Z", "+00:00")
        )
    except ValueError:
        return None
    created = value.get("created")
    if (
        value.get("kind") != "device_health_alert_ack"
        or not actor_user_id
        or acknowledged_at.tzinfo is None
        or not isinstance(created, bool)
    ):
        return None
    return actor_user_id, acknowledged_at.astimezone(_KST), created


def _mark_done_time_text(completed_at: datetime) -> str:
    return completed_at.astimezone(_KST).strftime("%Y-%m-%d %H:%M:%S KST")


def _normalize_status_emoji(text: str) -> str:
    # Slack이 이모지를 :name: 형식으로 돌려줘도 같은 상태로 판정해
    # 완료 문구가 중복되거나 replay가 불필요한 갱신을 만들지 않게 한다.
    return (
        text.replace(":white_check_mark:", "✅")
        .replace(":clock3:", "🕒")
    )


def _mark_done_blocks(
    blocks: list[dict[str, Any]],
    *,
    clicked_block_id: str,
    clicked_value: str,
    actor_user_id: str,
    completed_at: datetime,
) -> list[dict[str, Any]] | None:
    if _parse_current_alert_action_block_id(
        block_id=clicked_block_id,
        action_value=clicked_value,
    ) is None:
        return None
    matches: list[int] = []
    for index, raw_block in enumerate(blocks):
        block = raw_block if isinstance(raw_block, Mapping) else {}
        elements = block.get("elements")
        if block.get("type") != "actions" or not isinstance(elements, list):
            continue
        if _text(block.get("block_id"), "") != clicked_block_id:
            continue
        has_clicked_value = any(
            isinstance(element, Mapping)
            and _raw_action_value(element.get("value")) == clicked_value
            for element in elements
        )
        if has_clicked_value:
            matches.append(index)
    if len(matches) != 1:
        return None
    matched_index = matches[0]
    if matched_index < 3:
        return None
    raw_identity_block = blocks[matched_index - 3]
    raw_contact_block = blocks[matched_index - 1]
    if (
        not isinstance(raw_identity_block, Mapping)
        or raw_identity_block.get("type") != "section"
        or not isinstance(raw_identity_block.get("text"), Mapping)
        or not isinstance(raw_identity_block["text"].get("text"), str)
    ):
        return None
    if not isinstance(raw_contact_block, Mapping) or raw_contact_block.get(
        "type"
    ) != "section":
        return None
    raw_fields = raw_contact_block.get("fields")
    if not isinstance(raw_fields, list):
        return None

    updated_blocks = deepcopy(blocks)
    action_block = updated_blocks[matched_index]
    identity_block = updated_blocks[matched_index - 3]
    contact_block = updated_blocks[matched_index - 1]
    if not isinstance(action_block, dict) or not isinstance(contact_block, dict):
        return None
    elements = action_block.get("elements")
    fields = contact_block.get("fields")
    if not isinstance(elements, list) or not isinstance(fields, list):
        return None

    status_text = _mark_done_card_status(identity_block, contact_block)
    mark_done_indexes = [
        index
        for index, element in enumerate(elements)
        if isinstance(element, Mapping)
        and element.get("action_id") == DEVICE_HEALTH_ALERT_ACTION_MARK_DONE
        and _raw_action_value(element.get("value")) == clicked_value
    ]
    if len(mark_done_indexes) > 1 or (
        not mark_done_indexes and not status_text
    ):
        return None

    # 완료 기록은 API의 최초 receipt를 쓰고, 문자·음성은 보조 동작으로
    # 남긴다. 기존 section 안에서 담당자·시간을 상세 정보 위로 올린다.
    if mark_done_indexes:
        del elements[mark_done_indexes[0]]
    if not elements:
        return None
    if not status_text:
        status_text = (
            f"담당자 <@{actor_user_id}> · "
            f"`{_mark_done_time_text(completed_at)}`"
        )
    _promote_mark_done_status(
        identity_block,
        contact_block,
        elements,
        status_text,
        # 단일 알림은 제목에서 완료를 표시하므로 본문에는 담당자·시간만
        # 남긴다. 별도 점검 요약은 기존 카드별 완료 표시를 유지한다.
        include_status_label=(
            sum(block.get("type") == "actions" for block in updated_blocks) != 1
        ),
    )
    if not _refresh_mark_done_header(updated_blocks):
        return None
    return updated_blocks


def _mark_done_card_status(
    identity_block: Mapping[str, Any],
    contact_block: Mapping[str, Any],
) -> str:
    text = identity_block.get("text", {}).get("text", "")
    if _normalize_status_emoji(text).startswith(_MARK_DONE_STATUS_PREFIX):
        # 앞선 배포의 중복 제목은 버리고 최초 담당자·시간만 복원한다.
        return text.partition("\n")[2].split("\n\n", 1)[0]
    if text.startswith(_MARK_DONE_RECEIPT_PREFIX):
        return text.split("\n\n", 1)[0]
    # 같은 block identity로 이미 완료된 구형 카드도 최초 담당자·시간을
    # 읽어 새 상단 표시로 옮긴다. 재클릭한 사람으로 덮어쓰지 않는다.
    fields = contact_block.get("fields", [])
    status = next(
        (
            field["text"] for field in fields
            if isinstance(field, Mapping)
            and _normalize_status_emoji(str(field.get("text", ""))).startswith(
                _MARK_DONE_STATUS_PREFIX
            )
        ),
        "",
    )
    time = next(
        (
            field["text"] for field in fields
            if isinstance(field, Mapping)
            and _normalize_status_emoji(str(field.get("text", ""))).startswith(
                _MARK_DONE_TIME_PREFIX
            )
        ),
        "",
    )
    if status and time:
        actor = status.partition("\n")[2]
        completed_at = time.partition("\n")[2]
        return f"{actor} · {completed_at}"
    return ""


def _promote_mark_done_status(
    identity_block: dict[str, Any],
    contact_block: dict[str, Any],
    elements: list[dict[str, Any]],
    status_text: str,
    *,
    include_status_label: bool,
) -> None:
    # 병원·장비·감지 내용은 유지하고 완료 정보만 식별 영역 맨 위로 옮긴다.
    text = identity_block["text"]["text"]
    if _normalize_status_emoji(text).startswith(
        (_MARK_DONE_STATUS_PREFIX, _MARK_DONE_RECEIPT_PREFIX)
    ):
        text = text.partition("\n\n")[2]
    if include_status_label:
        status_text = f"{_MARK_DONE_STATUS_PREFIX}\n{status_text}"
    updated_text = f"{status_text}\n\n{text}"
    if _normalize_status_emoji(updated_text) != _normalize_status_emoji(
        identity_block["text"]["text"]
    ):
        identity_block["text"]["text"] = updated_text
    contact_block["fields"] = [
        field for field in contact_block["fields"]
        if not _normalize_status_emoji(str(field.get("text", ""))).startswith(
            (_MARK_DONE_STATUS_PREFIX, _MARK_DONE_TIME_PREFIX)
        )
    ]
    for element in elements:
        element.pop("style", None)


def _refresh_mark_done_header(blocks: list[dict[str, Any]]) -> bool:
    if not blocks or blocks[0].get("type") != "header":
        return False
    header = blocks[0].get("text")
    if not isinstance(header, dict) or not isinstance(header.get("text"), str):
        return False

    # notification 이상 알림은 한 메시지에 한 카드다. 같은 renderer를
    # 쓰는 별도 점검 요약에는 메시지 전체의 완료 상태를 새로 도입하지 않는다.
    if sum(block.get("type") == "actions" for block in blocks) != 1:
        return True

    current_title = _normalize_status_emoji(header["text"])
    title = (
        current_title.removeprefix(":alert: ")
        .removeprefix("✅ 확인 완료 · ")
        .removesuffix(" 확인 필요")
        .removesuffix(" 감지")
    )
    updated_title = f"✅ 확인 완료 · {title}"
    if updated_title != current_title:
        header["text"] = updated_title
    return True


def _send_remote_sms(
    payload: Mapping[str, Any],
    client: Any,
    logger: logging.Logger,
    bridge: DeviceHealthAlertApiBridge,
) -> None:
    try:
        item = _normalize_alert_item(payload["item"])
        target = build_device_health_alert_api_target(item)
        request_id = build_device_health_alert_request_id(
            workspace_id=payload["workspaceId"],
            actor_user_id=payload["actorUserId"],
            channel_id=payload["channelId"],
            message_ts=payload["messageTs"],
            interaction_id=payload["interactionId"],
            action_name=DEVICE_HEALTH_ALERT_SMS_ACTION,
            phase="execute",
        )
        result = bridge.send_sms(
            request_id=request_id,
            workspace_id=payload["workspaceId"],
            actor_user_id=payload["actorUserId"],
            channel_id=payload["channelId"],
            conversation_id=payload["threadTs"],
            target=target,
            phone_number=payload["phoneNumber"],
            message=payload["message"],
        )
        messages = result.messages
    except Exception as exc:
        logger.warning(
            "Device alert remote SMS failed error_type=%s",
            type(exc).__name__,
        )
        messages = (
            "병원 문자 발송 결과를 확인하지 못했어. 중복 발송하지 말고 운영 로그를 확인해줘",
        )
    for message in messages:
        _post_thread_reply(client, payload, message, logger)


def _sms_modal_view(
    metadata: Mapping[str, Any],
    *,
    phone: str,
    message: str,
) -> dict[str, Any]:
    return {
        "type": "modal",
        "callback_id": DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID,
        "private_metadata": json.dumps(
            dict(metadata),
            ensure_ascii=False,
            separators=(",", ":"),
        ),
        "title": {"type": "plain_text", "text": "병원 문자 보내기"},
        "submit": {"type": "plain_text", "text": "보내기"},
        "close": {"type": "plain_text", "text": "취소"},
        "blocks": [
            {
                "type": "input",
                "block_id": DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID,
                "label": {"type": "plain_text", "text": "받는 번호"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID,
                    "initial_value": phone,
                },
            },
            {
                "type": "input",
                "block_id": DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID,
                "label": {"type": "plain_text", "text": "문자 내용"},
                "element": {
                    "type": "plain_text_input",
                    "action_id": DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID,
                    "multiline": True,
                    "initial_value": message,
                },
            },
        ],
    }


def _parse_modal_submission(body: Mapping[str, Any]) -> dict[str, Any]:
    view = body.get("view") if isinstance(body.get("view"), Mapping) else {}
    team = body.get("team") if isinstance(body.get("team"), Mapping) else {}
    user = body.get("user") if isinstance(body.get("user"), Mapping) else {}
    metadata = _parse_json_mapping(view.get("private_metadata"))
    return {
        **metadata,
        "workspaceId": _text(team.get("id"), _text(metadata.get("workspaceId"), "")),
        "actorUserId": _text(user.get("id"), _text(metadata.get("actorUserId"), "")),
        "interactionId": ":".join(
            part
            for part in (_text(view.get("id"), ""), _text(view.get("hash"), ""))
            if part
        ),
        "phoneNumber": _view_input(
            view,
            DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID,
            DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_ACTION_ID,
        ),
        "message": _view_input(
            view,
            DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID,
            DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_ACTION_ID,
        ),
    }


def _modal_errors(payload: Mapping[str, Any]) -> dict[str, str]:
    errors: dict[str, str] = {}
    phone = re.sub(r"[^0-9+]", "", _text(payload.get("phoneNumber"), ""))
    if not _MOBILE_PHONE_PATTERN.fullmatch(phone):
        errors[DEVICE_HEALTH_MONITOR_SMS_MODAL_PHONE_BLOCK_ID] = (
            "휴대전화번호를 확인해줘"
        )
    if not _text(payload.get("message"), ""):
        errors[DEVICE_HEALTH_MONITOR_SMS_MODAL_MESSAGE_BLOCK_ID] = (
            "문자 내용을 입력해줘"
        )
    return errors


def _view_input(view: Mapping[str, Any], block_id: str, action_id: str) -> str:
    state = view.get("state") if isinstance(view.get("state"), Mapping) else {}
    values = state.get("values") if isinstance(state.get("values"), Mapping) else {}
    block = values.get(block_id) if isinstance(values.get(block_id), Mapping) else {}
    action = block.get(action_id) if isinstance(block.get(action_id), Mapping) else {}
    return _text(action.get("value"), "")


def _post_thread_reply(
    client: Any,
    identity: Mapping[str, Any],
    text: str,
    logger: logging.Logger,
    *,
    blocks: list[dict[str, Any]] | None = None,
) -> None:
    # API 본문은 줄바꿈을 유지해 Slack 문법으로 변환한다. 카드가 있으면
    # renderer가 만든 Slack 본문과 요청자 멘션을 그대로 사용한다.
    reply_text = str(text or "").strip() or "장비 이상 알림 작업 결과를 확인해줘"
    if blocks is None:
        reply_text = _commonmark_to_slack(reply_text)
    try:
        client.chat_postMessage(
            channel=_text(identity.get("channelId"), ""),
            thread_ts=_text(
                identity.get("threadTs"),
                _text(identity.get("messageTs"), ""),
            ),
            text=reply_text,
            unfurl_links=False,
            unfurl_media=False,
            **({"blocks": blocks} if blocks is not None else {}),
        )
    except Exception as exc:
        logger.warning(
            "Device alert action reply failed error_type=%s",
            type(exc).__name__,
        )


def _voice_guide_success_reply(
    target: DeviceHealthAlertApiTarget,
    *,
    actor_user_id: str,
) -> tuple[str, list[dict[str, Any]]]:
    title = "📣 장비 음성 안내 명령 전송 완료"
    # 대상 문자열은 일반 본문처럼 escape하고, 인증된 Slack 클릭 사용자만
    # 멘션으로 만든다. 알림 미리보기에도 카드와 같은 정보를 남긴다.
    fields = [
        _commonmark_to_slack(f"🏥 **병원**\n{target.hospital_name}"),
        _commonmark_to_slack(f"🚪 **진료실**\n{target.room_name}"),
        _commonmark_to_slack(f"⚙️ **장비**\n`{target.device_name}`"),
        f"👤 *요청자*\n<@{actor_user_id}>",
    ]
    return (
        "\n\n".join((f"*{title}*", *fields)),
        [
            {"type": "header", "text": {"type": "plain_text", "text": title}},
            {
                "type": "section",
                "fields": [{"type": "mrkdwn", "text": field} for field in fields],
            },
        ],
    )


def _response_value(response: Any, key: str) -> str:
    direct = str(
        getattr(response, "get", lambda *_args, **_kwargs: "")(key)
        or ""
    ).strip()
    if direct:
        return direct
    data = getattr(response, "data", None)
    return str(
        getattr(data, "get", lambda *_args, **_kwargs: "")(key)
        or ""
    ).strip()


def _is_safe_slack_permalink(value: str, channel_id: str) -> bool:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    return bool(
        parsed.scheme == "https"
        and str(parsed.hostname or "").casefold().endswith(".slack.com")
        and parsed.path.startswith(f"/archives/{channel_id}/p")
        and not parsed.username
        and not parsed.password
        and not parsed.fragment
    )


def _is_safe_https_url(value: str) -> bool:
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    return bool(
        parsed.scheme == "https"
        and parsed.hostname
        and not parsed.username
        and not parsed.password
        and not parsed.fragment
    )


def _parse_json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(str(value or ""))
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


def _positive_int(value: Any) -> int | None:
    try:
        result = int(value)
    except (TypeError, ValueError):
        return None
    return result if result > 0 else None


def _text(value: Any, default: str) -> str:
    normalized = " ".join(str(value or "").split())
    return normalized or default


def _raw_action_value(value: Any) -> str:
    """block identity용 Slack action value는 공백까지 원문 그대로 보존한다."""

    return value if isinstance(value, str) else ""


__all__ = [
    "DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL",
    "DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE",
    "DEVICE_HEALTH_ALERT_ACTION_MARK_DONE",
    "DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS",
    "DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID",
    "attach_device_alert_actions",
    "post_device_alert_summary",
]
