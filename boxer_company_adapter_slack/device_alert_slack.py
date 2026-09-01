from __future__ import annotations

from collections import OrderedDict
from copy import deepcopy
from datetime import datetime
import hashlib
import json
import logging
import re
import threading
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from boxer_company.transport_contracts import (
    DEVICE_HEALTH_ALERT_MARK_DONE_ACTION,
    DEVICE_HEALTH_ALERT_SMS_ACTION,
    DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION,
    DEVICE_HEALTH_ALERT_VOICE_ACTION,
)
from boxer_company_adapter_slack.device_health_alert_api import (
    DeviceHealthAlertApiBridge,
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
_CATEGORY_TITLES = {
    "recording": "녹화 상태 확인 필요",
    "recording_processing": "녹화 파일 처리 확인 필요",
    "video_signal": "영상 신호 확인 필요",
    "led": "LED 연결 확인 필요",
    "audio": "음성 출력 확인 필요",
    "application": "장비 앱 실행 확인 필요",
    "storage": "장비 저장 공간 부족",
    "device_connection": "장비 연결 확인 필요",
    "upload": "영상 업로드 확인 필요",
}
_COMPONENT_TITLES = {
    "captureboard": "캡처보드",
    "led": "LED",
    "audio": "스피커",
    "pm2": "PM2",
    "storage": "저장 공간",
}


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
            if updated_blocks == source_blocks:
                # 최신 root에 이미 같은 카드의 완료 field가 있으면 replay는
                # Slack mutation 없이 성공으로 끝낸다.
                return True
            try:
                client.chat_update(
                    channel=channel_id,
                    ts=message_ts,
                    text=root_text,
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
    components = _problem_components_text(item.get("problemComponents"))
    lines = [
        f"*{item['hospital']}*",
        (
            f"⚙️ *장비*  {_device_name_text(item)}  ·  "
            f"🚪 *병실*  `{item['room']}`"
        ),
        "",
    ]
    if components:
        lines.append(f":rotating_light: *문제 장치*\n{components}")
    lines.extend(
        (
            f"🔎 *감지 내용*\n`{item['issue']}`",
            "",
            f"📞 *전화*\n{_text(item.get('telephone'), '미확인')}",
            f"💬 *문자*\n{_sms_contact_text(item)}",
        )
    )
    return "\n".join(lines)


def _alert_item_blocks(
    item: Mapping[str, Any],
    *,
    card_index: int,
    include_actions: bool,
    include_device_voice_action: bool,
) -> list[dict[str, Any]]:
    components = _problem_components_text(item.get("problemComponents"))
    issue_fields: list[dict[str, str]] = []
    if components:
        issue_fields.append(
            {
                "type": "mrkdwn",
                "text": f":rotating_light: *문제 장치*\n{components}",
            }
        )
    issue_fields.append(
        {
            "type": "mrkdwn",
            "text": f"🔎 *감지 내용*\n`{item['issue']}`",
        }
    )
    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*{item['hospital']}*"},
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": f"⚙️ *장비*\n{_device_name_text(item)}",
                },
                {
                    "type": "mrkdwn",
                    "text": f"🚪 *병실*\n`{item['room']}`",
                },
            ],
        },
        {"type": "section", "fields": issue_fields},
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
    return {
        "hospitalSeq": hospital_seq or 0,
        "hospitalName": hospital_name,
        "hospital": hospital_label,
        "room": _text(raw.get("room") or raw.get("roomName"), "병실 미확인"),
        "device": _text(raw.get("device") or raw.get("deviceName"), "장비명 미확인"),
        "issue": _text(raw.get("issue") or raw.get("priorityReason"), "상세 확인 필요"),
        "alertCategory": _text(raw.get("alertCategory"), "device_connection"),
        "problemComponents": normalized_components,
        "telephone": _text(raw.get("telephone") or raw.get("hospitalTelephone"), ""),
        "deviceAlertPhone": _text(
            raw.get("deviceAlertPhone") or raw.get("hospitalDeviceAlertPhone"),
            "",
        ),
        "smsPhoneNumber": _text(raw.get("smsPhoneNumber"), ""),
        "mdaUrl": _text(raw.get("mdaUrl"), ""),
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
    if matched_index <= 0:
        return None
    raw_contact_block = blocks[matched_index - 1]
    if not isinstance(raw_contact_block, Mapping) or raw_contact_block.get(
        "type"
    ) != "section":
        return None
    raw_fields = raw_contact_block.get("fields")
    if not isinstance(raw_fields, list):
        return None

    updated_blocks = deepcopy(blocks)
    action_block = updated_blocks[matched_index]
    contact_block = updated_blocks[matched_index - 1]
    if not isinstance(action_block, dict) or not isinstance(contact_block, dict):
        return None
    elements = action_block.get("elements")
    fields = contact_block.get("fields")
    if not isinstance(elements, list) or not isinstance(fields, list):
        return None

    status_exists = any(
        isinstance(field, Mapping)
        and _text(field.get("text"), "").startswith(_MARK_DONE_STATUS_PREFIX)
        for field in fields
    )
    mark_done_indexes = [
        index
        for index, element in enumerate(elements)
        if isinstance(element, Mapping)
        and element.get("action_id") == DEVICE_HEALTH_ALERT_ACTION_MARK_DONE
        and _raw_action_value(element.get("value")) == clicked_value
    ]
    if len(mark_done_indexes) > 1 or (
        not mark_done_indexes and not status_exists
    ):
        return None

    # 같은 actions block의 문자·음성 버튼은 유지하고 완료 버튼 하나만
    # 제거한다. 전화·문자 아래에 담당자와 시간을 2열 field로 맞춰 기존
    # 카드 문법을 유지하면서 메시지 block 수도 늘리지 않는다.
    if mark_done_indexes:
        del elements[mark_done_indexes[0]]
    if not elements:
        return None
    if not status_exists:
        fields.extend(
            (
                {
                    "type": "mrkdwn",
                    "text": (
                        f"{_MARK_DONE_STATUS_PREFIX}\n"
                        f"담당자 <@{actor_user_id}>"
                    ),
                },
                {
                    "type": "mrkdwn",
                    "text": (
                        "🕒 *처리 시간*\n"
                        f"`{_mark_done_time_text(completed_at)}`"
                    ),
                },
            )
        )
    return updated_blocks


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
) -> None:
    try:
        client.chat_postMessage(
            channel=_text(identity.get("channelId"), ""),
            thread_ts=_text(
                identity.get("threadTs"),
                _text(identity.get("messageTs"), ""),
            ),
            text=_text(text, "장비 이상 알림 작업 결과를 확인해줘"),
            unfurl_links=False,
            unfurl_media=False,
        )
    except Exception as exc:
        logger.warning(
            "Device alert action reply failed error_type=%s",
            type(exc).__name__,
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
