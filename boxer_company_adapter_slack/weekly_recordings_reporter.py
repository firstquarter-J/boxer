from __future__ import annotations

import logging
import math
import re
import threading
from datetime import date, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from boxer_company.assistant.recordings_report_format import (
    format_recordings_report_sections,
)
from boxer_company_adapter_slack.assistant_bridge import _commonmark_to_slack
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
_WEEKLY_ACTIVITY_REPORT_TITLE = "주간 초음파 녹화 & 신규 바코드 요약"
_SLACK_SECTION_TEXT_LIMIT = 3000
_SLACK_REPORT_BLOCK_LIMIT = 40
_SLACK_REPORT_TEXT_LIMIT = 12_000
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
            f"*비교 기간* `{summary['previousWeekStartDate']} ~ "
            f"{summary['previousWeekEndDate']}`",
            f"*총 촬영* `{summary['totalCount']:,}건` · "
            f"병원 `{summary['hospitalCount']:,}곳`",
            f"*전주 촬영* `{summary['previousTotalCount']:,}건` · "
            f"증감 `{summary['totalDelta']:+,}건` "
            f"(`{_format_weekly_change_rate(summary.get('totalChangeRate'))}`)",
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

    # API가 판정한 급증·급감 DTO를 그대로 표시해 집계 기준을 Slack에서 재계산하지 않는다.
    for title, rows_key, count_key in (
        ("급증", "surgeRows", "surgeCount"),
        ("급감", "dropRows", "dropCount"),
    ):
        rows = summary.get(rows_key) or []
        count = int(summary.get(count_key) or 0)
        lines.append(f"\n*{title} 병원* `{count:,}곳`")
        if not rows:
            lines.append("• 없어")
            continue
        for index, row in enumerate(rows, 1):
            lines.append(
                f"{index}. {row.get('hospitalName') or '병원 미확인'} "
                f"`{int(row.get('previousCount') or 0):,}건 → "
                f"{int(row.get('currentCount') or 0):,}건` · "
                f"`{int(row.get('delta') or 0):+,}건` "
                f"(`{_format_weekly_change_rate(row.get('changeRate'))}`)"
            )
        if count > len(rows):
            lines.append(f"• 상위 `{len(rows):,}곳`만 표시")

    # 진료실 급감은 API가 반환한 전체 목록을 표시하고 장비나 병원 합계로 다시 거르지 않는다.
    if "roomDropRows" in summary:
        room_rows = summary["roomDropRows"]
        lines.append(f"\n*진료실별 녹화 급감* `{summary['roomDropCount']:,}곳` (전주 대비 50% 이상·3건 이상 감소)")
        for index, row in enumerate(room_rows, 1):
            lines.append(
                f"{index}. {row['hospitalName']} · {row['roomName']} "
                f"`{row['previousCount']:,}건 → {row['currentCount']:,}건` · "
                f"`{row['delta']:+,}건` (`{_format_weekly_change_rate(row['changeRate'])}`)"
            )
        if not room_rows:
            lines.append("• 없어")
    return "\n".join(lines)


def _format_weekly_change_rate(value: float | None) -> str:
    """전주 촬영이 없어 비율을 계산할 수 없는 경우를 구분한다."""

    if value is None:
        return "신규/비교불가"
    sign = "+" if value > 0 else ""
    return f"{sign}{value:.1f}%"


def _build_weekly_recordings_report_blocks(
    summary: dict[str, Any],
    *,
    include_header: bool = False,
) -> list[dict[str, Any]]:
    """전체 요약을 항목별로 나눠 Slack section의 길이 제한 안에 담는다."""

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
    report_text = _format_weekly_recordings_report(summary, include_title=False)
    return blocks + _build_weekly_text_blocks(report_text)


def _build_weekly_text_blocks(report_text: str) -> list[dict[str, Any]]:
    """각 항목의 긴 목록도 줄 경계에서 나눠 Slack section 제한을 지킨다."""

    blocks: list[dict[str, Any]] = []
    for section in report_text.split("\n\n"):
        while section:
            split_at = len(section)
            if split_at > _SLACK_SECTION_TEXT_LIMIT:
                split_at = section.rfind("\n", 0, _SLACK_SECTION_TEXT_LIMIT + 1)
                if split_at <= 0:
                    split_at = _SLACK_SECTION_TEXT_LIMIT
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": section[:split_at]},
                }
            )
            section = section[split_at:].lstrip("\n")
    return blocks


def _split_weekly_report_blocks(
    blocks: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    """급감 진료실이 많아도 목록을 자르지 않고 같은 스레드의 여러 메시지로 나눈다."""

    chunks: list[list[dict[str, Any]]] = []
    current: list[dict[str, Any]] = []
    text_size = 0
    for block in blocks:
        block_size = len(block["text"]["text"]) + 2
        if current and (
            len(current) >= _SLACK_REPORT_BLOCK_LIMIT
            or text_size + block_size > _SLACK_REPORT_TEXT_LIMIT
        ):
            chunks.append(current)
            current = []
            text_size = 0
        current.append(block)
        text_size += block_size
    if current:
        chunks.append(current)
    return chunks


def _build_weekly_report_parts(
    summary: dict[str, Any], *, scheduled_at: datetime,
) -> list[tuple[str, list[dict[str, Any]]]]:
    # 새 보고는 네 항목의 댓글 경계를 보존한다. 재전송 때도 시각·part ID는 고정된다.
    if "newBarcodes" in summary:
        parts = []
        sections = format_recordings_report_sections(
            summary, now=_coerce_weekly_recordings_report_now(scheduled_at),
        )
        for section_index, section in enumerate(sections):
            chunks = _split_weekly_report_blocks(
                _build_weekly_text_blocks(_commonmark_to_slack(section))
            )
            parts.extend(
                (f"report:v2:{section_index}:{chunk_index}", blocks)
                for chunk_index, blocks in enumerate(chunks)
            )
        return parts
    # 배포 전에 생성된 pending은 새 지표를 추정하지 않고 기존 내용·메시지 ID로 마친다.
    return [
        ("report" if index == 0 else f"report:{index}", blocks)
        for index, blocks in enumerate(_split_weekly_report_blocks(
            _build_weekly_recordings_report_blocks(summary, include_header=False)
        ))
    ]


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
    parts = _build_weekly_report_parts(report_summary, scheduled_at=batch.scheduled_at)
    title_response = client.chat_postMessage(
        channel=batch.channel_id,
        text=(
            _WEEKLY_ACTIVITY_REPORT_TITLE if "newBarcodes" in report_summary
            else _WEEKLY_RECORDINGS_REPORT_TITLE
        ),
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
    last_message_ts = thread_ts
    for part_id, message_blocks in parts:
        message_text = "\n\n".join(block["text"]["text"] for block in message_blocks)
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
                part=part_id,
            ),
        )
        last_message_ts = _extract_weekly_recordings_message_ts(report_response) or last_message_ts
    # 모든 분할 메시지가 성공한 뒤에만 API delivery 전체를 완료 처리한다.
    remember_automation_delivery(
        cycle=batch.cycle,
        cycle_key=batch.cycle_key,
        delivery=AutomationSlackDelivery(
            delivery_id=delivery.delivery_id,
            external_message_id=last_message_ts,
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
        # 기존 pending 두 형식과 네 지표를 모두 가진 새 형식만 허용한다.
        or set(payload) not in (
            expected_keys,
            expected_keys | {"roomDropRows", "roomDropCount"},
            expected_keys | {"roomDropRows", "roomDropCount", "newBarcodes", "queryOptions"},
        )
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
    if "roomDropRows" in payload:
        rows = payload["roomDropRows"]
        row_keys = {
            "hospitalSeq", "hospitalRoomSeq", "hospitalName", "roomName",
            "previousCount", "currentCount", "delta", "changeRate",
        }
        if (
            not isinstance(rows, list)
            or type(payload["roomDropCount"]) is not int
            or payload["roomDropCount"] != len(rows)
            or any(
                not isinstance(row, dict)
                or set(row) != row_keys
                or any(type(row[key]) is not int or row[key] <= 0 for key in ("hospitalSeq", "hospitalRoomSeq", "previousCount"))
                or type(row["currentCount"]) is not int or row["currentCount"] < 0
                or type(row["delta"]) is not int
                or type(row["changeRate"]) not in {int, float}
                or any(not isinstance(row[key], str) or not row[key].strip() for key in ("hospitalName", "roomName"))
                for row in rows
            )
        ):
            raise RuntimeError("주간 recordings 진료실 급감 계약이 올바르지 않아")
    if "newBarcodes" in payload:
        _validate_weekly_new_barcodes(payload)
    return dict(payload)


def _validate_weekly_new_barcodes(payload: dict[str, Any]) -> None:
    """새 댓글을 만들기 전에 신규 바코드 지표와 자동 보고의 기본 조건을 검증한다."""

    def count(value: object) -> bool:
        return type(value) is int and value >= 0

    def rate(value: object, *, nullable: bool = False) -> bool:
        return (value is None and nullable) or (
            type(value) in (int, float) and math.isfinite(value)
        )

    def rows_valid(rows: object, *, room: bool = False, top: bool = False) -> bool:
        if not isinstance(rows, list):
            return False
        keys = {"hospitalSeq", "hospitalName"} | (
            {"rowCount"} if top else {"previousCount", "currentCount", "delta", "changeRate"}
        )
        if room:
            keys |= {"hospitalRoomSeq", "roomName"}
        for row in rows:
            if not isinstance(row, dict) or set(row) != keys:
                return False
            if row["hospitalSeq"] is not None and not count(row["hospitalSeq"]):
                return False
            if not isinstance(row["hospitalName"], str) or not row["hospitalName"].strip():
                return False
            if room and (
                not count(row["hospitalRoomSeq"]) or row["hospitalRoomSeq"] == 0
                or not isinstance(row["roomName"], str) or not row["roomName"].strip()
            ):
                return False
            if top:
                if not count(row["rowCount"]):
                    return False
            elif (
                not count(row["previousCount"]) or not count(row["currentCount"])
                or type(row["delta"]) is not int
                or row["delta"] != row["currentCount"] - row["previousCount"]
                or row["delta"] > -3 or row["currentCount"] * 2 > row["previousCount"]
                or not rate(row["changeRate"])
            ):
                return False
        return True

    new = payload["newBarcodes"]
    count_keys = {"hospitalCount", "totalCount", "previousTotalCount", "dropCount", "roomDropCount"}
    keys = count_keys | {"totalDelta", "totalChangeRate", "topRows", "dropRows", "roomDropRows"}
    if (
        payload["queryOptions"] != {
            "hospitalNames": [], "dropPercent": 50, "minimumDrop": 3,
            "hospitalRecordingMinimumDrop": 3,
        }
        or not isinstance(new, dict) or set(new) != keys
        or any(not count(new[key]) for key in count_keys)
        or type(new["totalDelta"]) is not int
        or new["totalDelta"] != new["totalCount"] - new["previousTotalCount"]
        or not rate(new["totalChangeRate"], nullable=True)
        or not rows_valid(new["topRows"], top=True)
        or not rows_valid(new["dropRows"])
        or not rows_valid(new["roomDropRows"], room=True)
        or new["dropCount"] != len(new["dropRows"])
        or new["roomDropCount"] != len(new["roomDropRows"])
    ):
        raise RuntimeError("주간 신규 바코드 transport 계약이 올바르지 않아")


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
