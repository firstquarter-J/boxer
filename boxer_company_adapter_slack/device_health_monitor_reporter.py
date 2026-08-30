from __future__ import annotations

from datetime import datetime
import hashlib
import logging
import re
import threading
from typing import Any, Callable, Mapping
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack.automation_api_client import (
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.automation_reporter import (
    AutomationSlackDelivery,
    build_automation_delivery_client_msg_id,
    build_automation_request_id,
    flush_automation_deliveries,
    remember_automation_deliveries,
)
from boxer_company_adapter_slack.device_alert_slack import (
    attach_device_alert_actions,
    post_device_alert_summary,
)
from boxer_company_adapter_slack.device_health_alert_api import (
    DeviceHealthAlertApiBridge,
)


_KST = ZoneInfo("Asia/Seoul")
_SLACK_CHANNEL_ID_PATTERN = re.compile(r"^[CGD][A-Z0-9]{5,31}$")
_DEVICE_HEALTH_MONITOR_THREAD: threading.Thread | None = None
_DEVICE_HEALTH_MONITOR_THREAD_LOCK = threading.Lock()


def _coerce_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(_KST)
    if now.tzinfo is None:
        return now.replace(tzinfo=_KST)
    return now.astimezone(_KST)


def _run_device_health_monitor_once(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> bool:
    """API LED health pending을 Slack으로 전달하고 receipt만 보존한다."""

    if automation_client is None:
        logger.warning("장비 health transport API client가 없어")
        return False
    local_now = _coerce_now(now)
    flush_automation_deliveries(
        automation_client,
        cycle="device_health_monitor",
        cycle_key="transport:health",
        scheduled_at=local_now,
        logger=logger,
    )
    batch = automation_client.pull_pending(
        request_id=build_automation_request_id(
            cycle="device_health_monitor",
            cycle_key="transport:pull",
            scheduled_at=local_now,
        ),
        cycle="device_health_monitor",
    )
    if batch is None:
        return False
    _validate_health_batch(batch)
    delivery_ids = tuple(
        sorted(delivery.delivery_id for delivery in batch.deliveries)
    )
    summary = _build_remote_device_health_alert_batch_summary(
        tuple(delivery.payload for delivery in batch.deliveries)
    )
    digest = hashlib.sha256(
        "\0".join(delivery_ids).encode("utf-8")
    ).hexdigest()[:32]
    slack_delivery = _post_daily_device_round_abnormal_alert(
        client,
        summary,
        channel_id=batch.channel_id,
        logger=logger,
        include_actions=True,
        client_msg_id=build_automation_delivery_client_msg_id(
            cycle=batch.cycle,
            cycle_key=batch.cycle_key,
            delivery_id=f"device_health_monitor:{digest}",
            part="alert",
        ),
    )
    if slack_delivery is None:
        return False
    remember_automation_deliveries(
        cycle=batch.cycle,
        cycle_key=batch.cycle_key,
        deliveries=tuple(
            AutomationSlackDelivery(
                delivery_id=delivery_id,
                external_message_id=str(
                    slack_delivery.get("messageTs") or ""
                ),
                permalink=str(slack_delivery.get("permalink") or ""),
                delivered_at=local_now,
            )
            for delivery_id in delivery_ids
        ),
        batch=batch,
    )
    logger.info(
        "Delivered API-owned device health batch channel=%s deliveries=%s",
        batch.channel_id,
        len(delivery_ids),
    )
    return True


def _validate_health_batch(batch: Any) -> None:
    deliveries = getattr(batch, "deliveries", ())
    if (
        getattr(batch, "cycle", None) != "device_health_monitor"
        or str(getattr(batch, "cycle_key", "") or "") != "continuous"
        or not _SLACK_CHANNEL_ID_PATTERN.fullmatch(
            str(getattr(batch, "channel_id", "") or "")
        )
        or getattr(batch, "conversation", {}) != {}
        or not isinstance(deliveries, tuple)
        or not deliveries
        or any(
            delivery.kind != "device_health_alert"
            or not isinstance(delivery.payload, Mapping)
            for delivery in deliveries
        )
    ):
        raise RuntimeError("장비 health transport batch 계약이 올바르지 않아")


def _build_remote_device_health_alert_batch_summary(
    payloads: tuple[Mapping[str, Any], ...],
) -> dict[str, Any]:
    device_results = [
        _build_remote_device_health_alert_device_result(payload)
        for payload in payloads
    ]
    return {
        "statusCounts": {"이상": len(device_results)},
        "deviceResults": device_results,
    }


def _build_remote_device_health_alert_device_result(
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    raw = payload.get("alert")
    if not isinstance(raw, Mapping):
        raise RuntimeError("장비 health API alert가 올바르지 않아")
    components = raw.get("problemComponents")
    if not isinstance(components, list):
        raise RuntimeError("장비 health API component가 올바르지 않아")
    try:
        hospital_seq = int(raw.get("hospitalSeq") or 0)
    except (TypeError, ValueError) as exc:
        raise RuntimeError("장비 health API 병원 식별자가 올바르지 않아") from exc
    hospital_name = str(
        raw.get("hospitalName") or raw.get("hospital") or ""
    ).strip()
    room = str(raw.get("room") or "").strip()
    device = str(raw.get("device") or "").strip()
    issue = str(raw.get("issue") or "").strip()
    if hospital_seq <= 0 or not hospital_name or not room or not device or not issue:
        raise RuntimeError("장비 health API alert가 올바르지 않아")
    return {
        "hospitalSeq": hospital_seq,
        "hospitalName": hospital_name,
        "hospital": f"#{hospital_seq} {hospital_name}",
        "room": room,
        "device": device,
        "issue": issue,
        # API가 이미 확정한 표시용 연락처만 Slack 카드까지 보존한다.
        # 렌더링하지 않는 자동문자 본문·장비 상태는 adapter로 복제하지 않는다.
        "telephone": str(raw.get("telephone") or "").strip(),
        "deviceAlertPhone": str(raw.get("deviceAlertPhone") or "").strip(),
        "smsPhoneNumber": str(raw.get("smsPhoneNumber") or "").strip(),
        "alertCategory": str(
            raw.get("alertCategory") or "device_connection"
        ).strip(),
        "problemComponents": [
            str(item).strip() for item in components if str(item).strip()
        ],
        "mdaUrl": str(raw.get("mdaUrl") or "").strip(),
    }


def _post_daily_device_round_abnormal_alert(
    client: Any,
    report_summary: Mapping[str, Any],
    *,
    channel_id: str,
    logger: logging.Logger,
    include_actions: bool = True,
    client_msg_id: str,
) -> dict[str, str] | None:
    """API presentation을 공통 장비 alert Slack renderer로 전달한다."""

    return post_device_alert_summary(
        client,
        report_summary,
        channel_id=channel_id,
        logger=logger,
        include_actions=include_actions,
        client_msg_id=client_msg_id,
    )


def _attach_device_health_monitor_alert_actions(
    app: Any,
    logger: logging.Logger,
    base_access_checker: Callable[[str | None, str | None], bool] | None,
    action_api_bridge: DeviceHealthAlertApiBridge | None = None,
) -> None:
    attach_device_alert_actions(
        app,
        logger,
        base_access_checker,
        action_api_bridge,
    )


def _device_health_monitor_loop(
    client: Any,
    logger: logging.Logger,
    automation_client: CompanyAutomationApiClient,
) -> None:
    while True:
        try:
            _run_device_health_monitor_once(
                client,
                logger,
                automation_client=automation_client,
            )
        except Exception as exc:
            logger.warning(
                "Device health transport failed error_type=%s",
                type(exc).__name__,
            )
        threading.Event().wait(30)


def attach_device_health_monitor_reporter(
    app: Any,
    *,
    logger: logging.Logger | None = None,
    base_access_checker: Callable[[str | None, str | None], bool] | None = None,
    action_api_bridge: DeviceHealthAlertApiBridge | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
    notification_automation_client: CompanyAutomationApiClient | None = None,
) -> None:
    """API transport와 Slack action bridge만 조립한다."""

    actual_logger = logger or logging.getLogger(__name__)
    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("장비 health transport Slack client가 없어")
        return
    if automation_client is not None or notification_automation_client is not None:
        _attach_device_health_monitor_alert_actions(
            app,
            actual_logger,
            base_access_checker,
            action_api_bridge,
        )
    if automation_client is None:
        return
    global _DEVICE_HEALTH_MONITOR_THREAD
    with _DEVICE_HEALTH_MONITOR_THREAD_LOCK:
        if (
            _DEVICE_HEALTH_MONITOR_THREAD is not None
            and _DEVICE_HEALTH_MONITOR_THREAD.is_alive()
        ):
            return
        _DEVICE_HEALTH_MONITOR_THREAD = threading.Thread(
            target=_device_health_monitor_loop,
            args=(client, actual_logger, automation_client),
            name="device-health-transport",
            daemon=True,
        )
        _DEVICE_HEALTH_MONITOR_THREAD.start()


__all__ = ["attach_device_health_monitor_reporter"]
