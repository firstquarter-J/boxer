from __future__ import annotations

from datetime import datetime, timedelta
import json
import logging
import re
import threading
from typing import Any
from zoneinfo import ZoneInfo

from boxer_company_adapter_slack.automation_api_client import (
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.automation_reporter import (
    AutomationSlackDelivery,
    build_automation_delivery_client_msg_id,
    build_automation_request_id,
    flush_automation_deliveries,
    load_automation_thread_receipt,
    remember_automation_delivery,
    remember_automation_thread_receipt,
)


_KST = ZoneInfo("Asia/Seoul")
_DAILY_DEVICE_ROUND_THREAD: threading.Thread | None = None
_DAILY_DEVICE_ROUND_THREAD_LOCK = threading.Lock()
_DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE = 40
_DAILY_DEVICE_ROUND_MAX_BLOCK_CHARS_PER_MESSAGE = 12_000
_DAILY_TRANSPORT_CYCLE_KEY_PATTERN = re.compile(
    r"^daily:(\d{4}-\d{2}-\d{2})$"
)
_SLACK_TRANSPORT_CHANNEL_ID_PATTERN = re.compile(
    r"^[CGD][A-Z0-9]{5,31}$"
)


def _coerce_daily_device_round_now(
    now: datetime | None = None,
) -> datetime:
    if now is None:
        return datetime.now(_KST)
    if now.tzinfo is None:
        return now.replace(tzinfo=_KST)
    return now.astimezone(_KST)


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _split_daily_device_round_blocks(
    blocks: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    """Slack block 수와 직렬화 크기를 동시에 제한한다."""

    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    current_size = 0
    for block in blocks:
        block_size = len(json.dumps(block, ensure_ascii=False))
        if current and (
            len(current) >= _DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE
            or current_size + block_size
            > _DAILY_DEVICE_ROUND_MAX_BLOCK_CHARS_PER_MESSAGE
        ):
            chunks.append(current)
            current = []
            current_size = 0
        current.append(block)
        current_size += block_size
    if current:
        chunks.append(current)
    return chunks or [[]]


def _build_daily_device_round_chunk_text(
    base_text: str,
    *,
    chunk_index: int,
    chunk_count: int,
) -> str:
    if chunk_count <= 1:
        return base_text
    return f"{base_text} | 계속 {chunk_index + 1}/{chunk_count}"


def _build_daily_device_round_window_title_text(
    now: datetime | None = None,
) -> str:
    local_now = _coerce_daily_device_round_now(now)
    return f"마미박스 일일 순회 업데이트 | {local_now:%Y-%m-%d}"


def _extract_daily_device_round_thread_ts(response: Any) -> str:
    direct = str(
        getattr(response, "get", lambda *_args, **_kwargs: "")("ts")
        or ""
    ).strip()
    if direct:
        return direct
    data = getattr(response, "data", None)
    return str(
        getattr(data, "get", lambda *_args, **_kwargs: "")("ts")
        or ""
    ).strip()


def _build_remote_daily_device_round_blocks(
    report_summary: dict[str, Any],
) -> list[dict[str, Any]]:
    blocks = report_summary.get("messageBlocks")
    if isinstance(blocks, list) and all(
        isinstance(block, dict) for block in blocks
    ):
        return [dict(block) for block in blocks]
    raise RuntimeError("일일 장비 순회 API blocks 계약이 올바르지 않아")


def _build_daily_device_round_report_text(
    report_summary: dict[str, Any],
) -> str:
    """API가 만든 presentation fallback text만 사용한다."""

    text = str(report_summary.get("fallbackText") or "").strip()
    if not text:
        raise RuntimeError("일일 장비 순회 API text 계약이 올바르지 않아")
    return text


def _run_daily_device_round_if_due(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> bool:
    """Slack은 due나 대상 병원을 계산하지 않고 pending만 poll한다."""

    if automation_client is None:
        logger.warning("일일 장비 순회 transport API client가 없어")
        return False
    return _run_daily_device_round_transport(
        client,
        logger,
        automation_client=automation_client,
        poll_now=_coerce_daily_device_round_now(now),
    )


def _run_daily_device_round_transport(
    client: Any,
    logger: logging.Logger,
    *,
    automation_client: CompanyAutomationApiClient,
    poll_now: datetime,
) -> bool:
    """API-owned 일일 pending 한 건을 deterministic Slack thread로 보낸다."""

    flush_automation_deliveries(
        automation_client,
        cycle="daily_device_round",
        cycle_key="transport:daily",
        scheduled_at=poll_now,
        logger=logger,
    )
    batch = automation_client.pull_pending(
        request_id=build_automation_request_id(
            cycle="daily_device_round",
            cycle_key="transport:pull",
            scheduled_at=poll_now,
        ),
        cycle="daily_device_round",
    )
    if batch is None:
        return False

    report_summary, render_now, window_key = (
        _validate_daily_device_round_transport_batch(batch)
    )
    delivery = batch.deliveries[0]
    thread_ts = load_automation_thread_receipt(
        cycle=batch.cycle,
        cycle_key=batch.cycle_key,
        channel_id=batch.channel_id,
    )
    if not thread_ts:
        # API는 병원별 batch를 순차 발행하므로 window root를 transport
        # receipt로 먼저 보존하고 이후 병원 결과를 같은 thread에 누적한다.
        title_response = client.chat_postMessage(
            channel=batch.channel_id,
            text=_build_daily_device_round_window_title_text(render_now),
            unfurl_links=False,
            unfurl_media=False,
            client_msg_id=build_automation_delivery_client_msg_id(
                cycle=batch.cycle,
                cycle_key=batch.cycle_key,
                delivery_id=(
                    f"daily_device_round:{window_key}:thread"
                ),
                part="title",
            ),
        )
        thread_ts = _extract_daily_device_round_thread_ts(title_response)
        if not thread_ts:
            raise RuntimeError("일일 장비 순회 제목 메시지 ts를 받지 못했어")
        remember_automation_thread_receipt(
            cycle=batch.cycle,
            cycle_key=batch.cycle_key,
            channel_id=batch.channel_id,
            root_message_id=thread_ts,
        )

    message_text = _build_daily_device_round_report_text(report_summary)
    block_chunks = _split_daily_device_round_blocks(
        _build_remote_daily_device_round_blocks(report_summary)
    )
    last_message_ts = ""
    for index, block_chunk in enumerate(block_chunks):
        response = client.chat_postMessage(
            channel=batch.channel_id,
            text=_build_daily_device_round_chunk_text(
                message_text,
                chunk_index=index,
                chunk_count=len(block_chunks),
            ),
            blocks=block_chunk,
            thread_ts=thread_ts,
            unfurl_links=False,
            unfurl_media=False,
            client_msg_id=build_automation_delivery_client_msg_id(
                cycle=batch.cycle,
                cycle_key=batch.cycle_key,
                delivery_id=delivery.delivery_id,
                part=f"chunk:{index}",
            ),
        )
        last_message_ts = (
            _extract_daily_device_round_thread_ts(response)
            or last_message_ts
        )
    remember_automation_delivery(
        cycle=batch.cycle,
        cycle_key=batch.cycle_key,
        delivery=AutomationSlackDelivery(
            delivery_id=delivery.delivery_id,
            external_message_id=last_message_ts or thread_ts,
            permalink="",
            delivered_at=poll_now,
        ),
        batch=batch,
    )
    logger.info(
        "Posted daily device round transport channel=%s cycle_key=%s "
        "window=%s",
        batch.channel_id,
        batch.cycle_key,
        window_key,
    )
    return True


def _validate_daily_device_round_transport_batch(
    batch: Any,
) -> tuple[dict[str, Any], datetime, str]:
    """일일 renderer가 이해하는 scheduler batch 한 건만 허용한다."""

    cycle_key_match = _DAILY_TRANSPORT_CYCLE_KEY_PATTERN.fullmatch(
        str(getattr(batch, "cycle_key", "") or "")
    )
    scheduled_at = getattr(batch, "scheduled_at", None)
    deliveries = getattr(batch, "deliveries", ())
    render_now = (
        _coerce_daily_device_round_now(scheduled_at)
        if isinstance(scheduled_at, datetime)
        and scheduled_at.tzinfo is not None
        else None
    )
    if (
        getattr(batch, "cycle", None) != "daily_device_round"
        or cycle_key_match is None
        or not _SLACK_TRANSPORT_CHANNEL_ID_PATTERN.fullmatch(
            str(getattr(batch, "channel_id", "") or "")
        )
        or getattr(batch, "conversation", {}) != {}
        or render_now is None
        or not isinstance(deliveries, tuple)
        or len(deliveries) != 1
        or deliveries[0].kind != "daily_device_round_report"
    ):
        raise RuntimeError("일일 장비 순회 transport batch 계약이 올바르지 않아")
    try:
        window_date = datetime.fromisoformat(
            cycle_key_match.group(1)
        ).date()
    except ValueError as exc:
        raise RuntimeError(
            "일일 장비 순회 transport batch 계약이 올바르지 않아"
        ) from exc
    report_summary = _validate_remote_daily_device_round_presentation(
        deliveries[0].payload
    )
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    scheduled_date = render_now.date()
    window_key = window_date.isoformat()
    if (
        hospital_seq is None
        or hospital_seq <= 0
        or scheduled_date
        not in {window_date, window_date + timedelta(days=1)}
        or report_summary.get("runDate") != scheduled_date.isoformat()
        or deliveries[0].delivery_id
        != f"daily_device_round:{window_key}:{hospital_seq}"
    ):
        raise RuntimeError("일일 장비 순회 transport batch 계약이 올바르지 않아")
    return report_summary, render_now, window_key


def _validate_remote_daily_device_round_presentation(
    payload: Any,
) -> dict[str, Any]:
    """API presentation DTO 외 실행용 필드를 Slack에 못 넣게 한다."""

    allowed_top_level = {
        "runDate",
        "hospitalSeq",
        "hospitalName",
        "deviceCount",
        "scheduledDeviceCount",
        "statusCounts",
        "updateCounts",
        "cleanupCounts",
        "powerCounts",
        "summaryLine",
        "messageBlocks",
        "fallbackText",
        "deviceResults",
    }
    if not isinstance(payload, dict) or set(payload) != allowed_top_level:
        raise RuntimeError("일일 장비 순회 API presentation 계약이 올바르지 않아")
    if (
        not isinstance(payload.get("deviceResults"), list)
        or not isinstance(payload.get("messageBlocks"), list)
        or any(
            not isinstance(block, dict)
            for block in payload["messageBlocks"]
        )
        or not str(payload.get("fallbackText") or "").strip()
        or any(
            not isinstance(payload.get(key), dict)
            for key in (
                "statusCounts",
                "updateCounts",
                "cleanupCounts",
                "powerCounts",
            )
        )
    ):
        raise RuntimeError("일일 장비 순회 API presentation 계약이 올바르지 않아")
    return dict(payload)


def _daily_device_round_loop(
    client: Any,
    logger: logging.Logger,
    automation_client: CompanyAutomationApiClient,
) -> None:
    while True:
        try:
            _run_daily_device_round_if_due(
                client,
                logger,
                automation_client=automation_client,
            )
        except Exception as exc:
            logger.warning(
                "Daily device round transport failed error_type=%s",
                type(exc).__name__,
            )
        threading.Event().wait(30)


def attach_daily_device_round_reporter(
    app: Any,
    *,
    logger: logging.Logger | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> None:
    """Slack client와 API pending client만으로 transport를 붙인다."""

    actual_logger = logger or logging.getLogger(__name__)
    client = getattr(app, "client", None)
    if automation_client is None or client is None:
        actual_logger.warning("일일 장비 순회 transport 의존성이 없어")
        return
    global _DAILY_DEVICE_ROUND_THREAD
    with _DAILY_DEVICE_ROUND_THREAD_LOCK:
        if (
            _DAILY_DEVICE_ROUND_THREAD is not None
            and _DAILY_DEVICE_ROUND_THREAD.is_alive()
        ):
            return
        _DAILY_DEVICE_ROUND_THREAD = threading.Thread(
            target=_daily_device_round_loop,
            args=(client, actual_logger, automation_client),
            name="daily-device-round-transport",
            daemon=True,
        )
        _DAILY_DEVICE_ROUND_THREAD.start()


__all__ = ["attach_daily_device_round_reporter"]
