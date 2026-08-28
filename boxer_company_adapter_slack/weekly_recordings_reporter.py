from __future__ import annotations

from datetime import date, datetime, timedelta
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
    remember_automation_delivery,
)


_KST = ZoneInfo("Asia/Seoul")
_WEEKLY_RECORDINGS_REPORT_TITLE = "주간 초음파 촬영 요약"
_WEEKLY_RECORDINGS_REPORT_THREAD: threading.Thread | None = None
_WEEKLY_RECORDINGS_REPORT_THREAD_LOCK = threading.Lock()
_WEEKLY_TRANSPORT_CYCLE_KEY_PATTERN = re.compile(
    r"^weekly:(\d{4}-\d{2}-\d{2})$"
)
_SLACK_TRANSPORT_CHANNEL_ID_PATTERN = re.compile(
    r"^[CGD][A-Z0-9]{5,31}$"
)


def _coerce_weekly_recordings_report_now(
    now: datetime | None = None,
) -> datetime:
    if now is None:
        return datetime.now(_KST)
    if now.tzinfo is None:
        return now.replace(tzinfo=_KST)
    return now.astimezone(_KST)


def _format_weekly_recordings_report(
    summary: dict[str, Any],
    *,
    include_title: bool = False,
) -> str:
    """API summary DTO를 provider-free Slack fallback text로 렌더링한다."""

    lines: list[str] = []
    if include_title:
        lines.append(f"*{_WEEKLY_RECORDINGS_REPORT_TITLE}*")
    lines.extend(
        (
            f"*기간* `{summary['weekStartDate']} ~ {summary['weekEndDate']}`",
            f"*총 촬영* `{summary['totalCount']:,}건` · "
            f"병원 `{summary['hospitalCount']:,}곳`",
            f"*전주 촬영* `{summary['previousTotalCount']:,}건` · "
            f"증감 `{summary['totalDelta']:+,}건`",
        )
    )
    top_rows = summary.get("topRows") or []
    if top_rows:
        lines.append("\n*촬영 상위 병원*")
        for index, row in enumerate(top_rows, 1):
            lines.append(
                f"{index}. {row.get('hospitalName') or '병원 미확인'} "
                f"`{int(row.get('rowCount') or 0):,}건`"
            )
    return "\n".join(lines)


def _build_weekly_recordings_report_blocks(
    summary: dict[str, Any],
    *,
    include_header: bool = False,
) -> list[dict[str, Any]]:
    """같은 fallback text를 Slack section block으로 안전하게 감싼다."""

    blocks: list[dict[str, Any]] = []
    if include_header:
        blocks.append(
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": _WEEKLY_RECORDINGS_REPORT_TITLE,
                },
            }
        )
    blocks.append(
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": _format_weekly_recordings_report(
                    summary,
                    include_title=False,
                ),
            },
        }
    )
    return blocks


def _run_weekly_recordings_report_if_due(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> bool:
    """Slack은 due를 계산하지 않고 API pending transport만 poll한다."""

    if automation_client is None:
        logger.warning("주간 recordings transport API client가 없어")
        return False
    return _run_weekly_recordings_report_transport(
        client,
        logger,
        automation_client=automation_client,
        poll_now=_coerce_weekly_recordings_report_now(now),
    )


def _run_weekly_recordings_report_transport(
    client: Any,
    logger: logging.Logger,
    *,
    automation_client: CompanyAutomationApiClient,
    poll_now: datetime,
) -> bool:
    """API-owned 주간 pending 한 건을 Slack thread로만 전달한다."""

    flush_automation_deliveries(
        automation_client,
        cycle="weekly_recordings",
        cycle_key="transport:weekly",
        scheduled_at=poll_now,
        logger=logger,
    )
    batch = automation_client.pull_pending(
        request_id=build_automation_request_id(
            cycle="weekly_recordings",
            cycle_key="transport:pull",
            scheduled_at=poll_now,
        ),
        cycle="weekly_recordings",
    )
    if batch is None:
        return False

    report_summary = _validate_weekly_transport_batch(batch)
    delivery = batch.deliveries[0]
    message_text = _format_weekly_recordings_report(
        report_summary,
        include_title=False,
    )
    message_blocks = _build_weekly_recordings_report_blocks(
        report_summary,
        include_header=False,
    )
    title_response = client.chat_postMessage(
        channel=batch.channel_id,
        text=_WEEKLY_RECORDINGS_REPORT_TITLE,
        unfurl_links=False,
        unfurl_media=False,
        client_msg_id=build_automation_delivery_client_msg_id(
            cycle=batch.cycle,
            cycle_key=batch.cycle_key,
            delivery_id=delivery.delivery_id,
            part="title",
        ),
    )
    thread_ts = _extract_weekly_recordings_message_ts(title_response)
    if not thread_ts:
        raise RuntimeError("주간 recordings 리포트 제목 메시지 ts를 받지 못했어")
    report_response = client.chat_postMessage(
        channel=batch.channel_id,
        text=message_text,
        blocks=message_blocks,
        thread_ts=thread_ts,
        unfurl_links=False,
        unfurl_media=False,
        client_msg_id=build_automation_delivery_client_msg_id(
            cycle=batch.cycle,
            cycle_key=batch.cycle_key,
            delivery_id=delivery.delivery_id,
            part="report",
        ),
    )
    remember_automation_delivery(
        cycle=batch.cycle,
        cycle_key=batch.cycle_key,
        delivery=AutomationSlackDelivery(
            delivery_id=delivery.delivery_id,
            external_message_id=(
                _extract_weekly_recordings_message_ts(report_response)
                or thread_ts
            ),
            permalink="",
            delivered_at=poll_now,
        ),
        batch=batch,
    )
    logger.info(
        "Posted weekly recordings transport channel=%s cycle_key=%s",
        batch.channel_id,
        batch.cycle_key,
    )
    return True


def _validate_weekly_transport_batch(
    batch: Any,
) -> dict[str, Any]:
    """주간 renderer가 이해하는 scheduler batch만 허용한다."""

    cycle_key_match = _WEEKLY_TRANSPORT_CYCLE_KEY_PATTERN.fullmatch(
        str(getattr(batch, "cycle_key", "") or "")
    )
    deliveries = getattr(batch, "deliveries", ())
    scheduled_at = getattr(batch, "scheduled_at", None)
    render_now = (
        _coerce_weekly_recordings_report_now(scheduled_at)
        if isinstance(scheduled_at, datetime)
        and scheduled_at.tzinfo is not None
        else None
    )
    if (
        getattr(batch, "cycle", None) != "weekly_recordings"
        or cycle_key_match is None
        or not _SLACK_TRANSPORT_CHANNEL_ID_PATTERN.fullmatch(
            str(getattr(batch, "channel_id", "") or "")
        )
        or getattr(batch, "conversation", {}) != {}
        or render_now is None
        or not isinstance(deliveries, tuple)
        or len(deliveries) != 1
        or deliveries[0].kind != "weekly_recordings_report"
    ):
        raise RuntimeError("주간 recordings transport batch 계약이 올바르지 않아")
    try:
        week_start = date.fromisoformat(cycle_key_match.group(1))
    except ValueError as exc:
        raise RuntimeError(
            "주간 recordings transport batch 계약이 올바르지 않아"
        ) from exc
    week_end = week_start + timedelta(days=6)
    delivery = deliveries[0]
    payload = delivery.payload
    expected_keys = {
        "weekStartDate",
        "weekEndDate",
        "previousWeekStartDate",
        "previousWeekEndDate",
        "hospitalCount",
        "totalCount",
        "previousTotalCount",
        "totalDelta",
        "totalChangeRate",
        "topRows",
        "topRowsLimit",
        "surgeRows",
        "surgeCount",
        "dropRows",
        "dropCount",
        "changeRowsLimit",
    }
    count_keys = {
        "hospitalCount",
        "totalCount",
        "previousTotalCount",
        "topRowsLimit",
        "surgeCount",
        "dropCount",
        "changeRowsLimit",
    }
    if (
        week_start.weekday() != 0
        or delivery.delivery_id
        != f"weekly_recordings:{week_start.isoformat()}"
        or not isinstance(payload, dict)
        or set(payload) != expected_keys
        or payload.get("weekStartDate") != week_start.isoformat()
        or payload.get("weekEndDate") != week_end.isoformat()
        or payload.get("previousWeekStartDate")
        != (week_start - timedelta(days=7)).isoformat()
        or payload.get("previousWeekEndDate")
        != (week_start - timedelta(days=1)).isoformat()
        or render_now.date() != week_start + timedelta(days=7)
        or any(
            type(payload.get(key)) is not int or payload[key] < 0
            for key in count_keys
        )
        or type(payload.get("totalDelta")) is not int
        or (
            payload.get("totalChangeRate") is not None
            and type(payload.get("totalChangeRate")) not in {int, float}
        )
        or any(
            not isinstance(payload.get(key), list)
            for key in ("topRows", "surgeRows", "dropRows")
        )
    ):
        raise RuntimeError("주간 recordings transport batch 계약이 올바르지 않아")
    return dict(payload)


def _extract_weekly_recordings_message_ts(response: Any) -> str:
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


def _weekly_recordings_report_loop(
    client: Any,
    logger: logging.Logger,
    automation_client: CompanyAutomationApiClient,
) -> None:
    while True:
        try:
            _run_weekly_recordings_report_if_due(
                client,
                logger,
                automation_client=automation_client,
            )
        except Exception as exc:
            logger.warning(
                "Weekly recordings transport failed error_type=%s",
                type(exc).__name__,
            )
        # API가 schedule을 소유하므로 Slack은 고정 간격으로 pending만 확인한다.
        threading.Event().wait(30)


def attach_weekly_recordings_reporter(
    app: Any,
    *,
    logger: logging.Logger | None = None,
    automation_client: CompanyAutomationApiClient | None = None,
) -> None:
    """Slack client와 API pending client만으로 transport를 붙인다."""

    actual_logger = logger or logging.getLogger(__name__)
    client = getattr(app, "client", None)
    if automation_client is None or client is None:
        actual_logger.warning("주간 recordings transport 의존성이 없어")
        return
    global _WEEKLY_RECORDINGS_REPORT_THREAD
    with _WEEKLY_RECORDINGS_REPORT_THREAD_LOCK:
        if (
            _WEEKLY_RECORDINGS_REPORT_THREAD is not None
            and _WEEKLY_RECORDINGS_REPORT_THREAD.is_alive()
        ):
            return
        _WEEKLY_RECORDINGS_REPORT_THREAD = threading.Thread(
            target=_weekly_recordings_report_loop,
            args=(client, actual_logger, automation_client),
            name="weekly-recordings-transport",
            daemon=True,
        )
        _WEEKLY_RECORDINGS_REPORT_THREAD.start()


__all__ = ["attach_weekly_recordings_reporter"]
