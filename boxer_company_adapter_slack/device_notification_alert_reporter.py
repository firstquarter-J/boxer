from __future__ import annotations

from datetime import datetime
import logging
import re
import threading
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack.automation_api_client import (
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.automation_reporter import (
    AutomationSlackDelivery,
    build_automation_delivery_client_msg_id,
    build_automation_request_id,
    flush_automation_deliveries,
    remember_automation_delivery,
)
from boxer_company_adapter_slack.device_alert_slack import (
    post_device_alert_summary,
)


_KST = ZoneInfo("Asia/Seoul")
_SLACK_CHANNEL_ID_PATTERN = re.compile(r"^[CGD][A-Z0-9]{5,31}$")
_DEVICE_NOTIFICATION_ALERT_THREAD: threading.Thread | None = None
_DEVICE_NOTIFICATION_ALERT_THREAD_LOCK = threading.Lock()


def _coerce_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(_KST)
    if now.tzinfo is None:
        return now.replace(tzinfo=_KST)
    return now.astimezone(_KST)


def _run_device_notification_alert_once(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> bool:
    """API pending을 Slack으로 전달하며 local provider로는 돌아가지 않는다."""

    if automation_client is None:
        logger.warning("장비 notification transport API client가 없어")
        return False
    local_now = _coerce_now(now)
    flush_automation_deliveries(
        automation_client,
        cycle="device_notification_alert",
        cycle_key="transport:notification",
        scheduled_at=local_now,
        logger=logger,
    )
    batch = automation_client.pull_pending(
        request_id=build_automation_request_id(
            cycle="device_notification_alert",
            cycle_key="transport:pull",
            scheduled_at=local_now,
        ),
        cycle="device_notification_alert",
    )
    if batch is None:
        return False
    _validate_notification_batch(batch)
    delivery = batch.deliveries[0]
    posted = _post_remote_device_notification_delivery(
        client,
        logger,
        delivery=delivery,
        channel_id=batch.channel_id,
        cycle_key=batch.cycle_key,
    )
    if posted is None:
        return False
    remember_automation_delivery(
        cycle=batch.cycle,
        cycle_key=batch.cycle_key,
        delivery=AutomationSlackDelivery(
            delivery_id=delivery.delivery_id,
            external_message_id=str(posted.get("messageTs") or "").strip(),
            permalink=str(posted.get("permalink") or "").strip(),
            delivered_at=local_now,
        ),
        batch=batch,
    )
    logger.info(
        "Delivered API-owned device notification channel=%s kind=%s",
        batch.channel_id,
        delivery.kind,
    )
    return True


def _validate_notification_batch(batch: Any) -> None:
    deliveries = getattr(batch, "deliveries", ())
    if (
        getattr(batch, "cycle", None) != "device_notification_alert"
        or not _SLACK_CHANNEL_ID_PATTERN.fullmatch(
            str(getattr(batch, "channel_id", "") or "")
        )
        or getattr(batch, "conversation", {}) != {}
        or not isinstance(deliveries, tuple)
        or len(deliveries) != 1
    ):
        # notification cursor는 한 ACK마다 하나씩 전진한다. 여러 delivery를
        # 부분 전송하면 cursor와 Slack receipt가 갈리므로 발송 전에 막는다.
        raise RuntimeError("장비 notification transport batch 계약이 올바르지 않아")
    delivery = deliveries[0]
    if delivery.kind not in {
        "device_notification_alert",
        "device_notification_thread_reply",
    } or not isinstance(delivery.payload, Mapping):
        raise RuntimeError("장비 notification delivery 계약이 올바르지 않아")


def _post_remote_device_notification_delivery(
    client: Any,
    logger: logging.Logger,
    *,
    delivery: Any,
    channel_id: str,
    cycle_key: str,
) -> dict[str, str] | None:
    """검증된 API presentation만 Slack 메시지로 바꾼다."""

    payload = delivery.payload
    if delivery.kind == "device_notification_thread_reply":
        return _post_remote_recording_stall_thread_reply(
            client,
            payload,
            channel_id=channel_id,
            logger=logger,
            client_msg_id=build_automation_delivery_client_msg_id(
                cycle="device_notification_alert",
                cycle_key=cycle_key,
                delivery_id=delivery.delivery_id,
                part="thread-reply",
            ),
        )
    alert_summary = payload.get("alertSummary")
    render = payload.get("render")
    if not isinstance(alert_summary, Mapping) or not isinstance(render, Mapping):
        raise RuntimeError("장비 notification alert presentation이 올바르지 않아")
    include_actions = render.get("includeActions")
    include_voice = render.get("includeDeviceVoiceAction")
    if (
        render.get("type") != "device_health_abnormal_alert"
        or not isinstance(include_actions, bool)
        or not isinstance(include_voice, bool)
    ):
        raise RuntimeError("장비 notification render hint가 올바르지 않아")
    return post_device_alert_summary(
        client,
        alert_summary,
        channel_id=channel_id,
        logger=logger,
        include_actions=include_actions,
        include_device_voice_action=include_voice,
        client_msg_id=build_automation_delivery_client_msg_id(
            cycle="device_notification_alert",
            cycle_key=cycle_key,
            delivery_id=delivery.delivery_id,
            part="alert",
        ),
    )


def _post_remote_recording_stall_thread_reply(
    client: Any,
    payload: Mapping[str, Any],
    *,
    channel_id: str,
    logger: logging.Logger,
    client_msg_id: str,
) -> dict[str, str] | None:
    """API가 지정한 기존 Slack root에 continuation만 전달한다."""

    thread_ts = str(payload.get("replyToExternalMessageId") or "").strip()
    values = {
        "duration": str(payload.get("durationText") or "").strip(),
        "growth": str(payload.get("growthRateText") or "").strip(),
        "occurred": str(payload.get("occurredAt") or "").strip(),
    }
    if not thread_ts or not all(values.values()):
        raise RuntimeError("장비 notification thread presentation이 올바르지 않아")
    text = "\n".join(
        (
            ":warning: *녹화 파일 증가 정지 지속*",
            f"> *지속 시간*  `{values['duration']}`",
            f"> *현재 증가율*  `{values['growth']}`",
            f"> *발생 시각*  `{values['occurred']}`",
        )
    )
    try:
        response = client.chat_postMessage(
            channel=channel_id,
            thread_ts=thread_ts,
            text=text,
            unfurl_links=False,
            unfurl_media=False,
            client_msg_id=client_msg_id,
        )
    except Exception as exc:
        logger.warning(
            "Device notification thread delivery failed error_type=%s",
            type(exc).__name__,
        )
        return None
    message_ts = _response_value(response, "ts")
    if not message_ts:
        raise RuntimeError("장비 notification Slack ts를 받지 못했어")
    return {"messageTs": message_ts, "permalink": ""}


def _response_value(response: Any, key: str) -> str:
    direct = str(
        getattr(response, "get", lambda *_args, **_kwargs: "")(key) or ""
    ).strip()
    if direct:
        return direct
    data = getattr(response, "data", None)
    return str(
        getattr(data, "get", lambda *_args, **_kwargs: "")(key) or ""
    ).strip()


def _device_notification_alert_loop(
    client: Any,
    logger: logging.Logger,
    automation_client: CompanyAutomationApiClient,
) -> None:
    while True:
        try:
            _run_device_notification_alert_once(
                client,
                logger,
                automation_client=automation_client,
            )
        except Exception as exc:
            logger.warning(
                "Device notification transport failed error_type=%s",
                type(exc).__name__,
            )
        threading.Event().wait(10)


def attach_device_notification_alert_reporter(
    app: Any,
    *,
    logger: logging.Logger | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> None:
    """API pending transport가 없으면 local reporter를 시작하지 않는다."""

    if automation_client is None:
        return
    actual_logger = logger or logging.getLogger(__name__)
    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("장비 notification transport Slack client가 없어")
        return
    global _DEVICE_NOTIFICATION_ALERT_THREAD
    with _DEVICE_NOTIFICATION_ALERT_THREAD_LOCK:
        if (
            _DEVICE_NOTIFICATION_ALERT_THREAD is not None
            and _DEVICE_NOTIFICATION_ALERT_THREAD.is_alive()
        ):
            return
        _DEVICE_NOTIFICATION_ALERT_THREAD = threading.Thread(
            target=_device_notification_alert_loop,
            args=(client, actual_logger, automation_client),
            name="device-notification-transport",
            daemon=True,
        )
        _DEVICE_NOTIFICATION_ALERT_THREAD.start()


__all__ = ["attach_device_notification_alert_reporter"]
