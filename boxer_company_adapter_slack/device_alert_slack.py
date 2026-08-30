from __future__ import annotations

from datetime import datetime
import json
import logging
import re
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
    for item in items[:_ALERT_ITEM_LIMIT]:
        # API 분리 전 카드의 식별 정보 → 장애 내용 → 연락처 순서를 유지한다.
        # 한 줄짜리 section으로 뭉개지지 않게 한다.
        blocks.extend(
            _alert_item_blocks(
                item,
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
    blocks.append({"type": "actions", "elements": action_elements})
    return blocks


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
    message = body.get("message") if isinstance(body.get("message"), Mapping) else {}
    message_ts = _text(message.get("ts"), "")
    return {
        "workspaceId": _text(team.get("id") or body.get("team_id"), ""),
        "actorUserId": _text(user.get("id"), ""),
        "channelId": _text(channel.get("id"), ""),
        "messageTs": message_ts,
        "threadTs": _text(message.get("thread_ts"), message_ts),
        "interactionId": _text(action.get("action_ts"), ""),
        "triggerId": _text(body.get("trigger_id"), ""),
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
) -> None:
    identity = _action_identity(body)
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
            conversation_id=identity["threadTs"],
            target=target,
        )
        messages = result.messages
    except Exception as exc:
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


__all__ = [
    "DEVICE_HEALTH_ALERT_ACTION_CONTACT_HOSPITAL",
    "DEVICE_HEALTH_ALERT_ACTION_DEVICE_VOICE_GUIDE",
    "DEVICE_HEALTH_ALERT_ACTION_MARK_DONE",
    "DEVICE_HEALTH_ALERT_ACTION_VIEW_AUTO_SMS",
    "DEVICE_HEALTH_MONITOR_SMS_MODAL_CALLBACK_ID",
    "attach_device_alert_actions",
    "post_device_alert_summary",
]
