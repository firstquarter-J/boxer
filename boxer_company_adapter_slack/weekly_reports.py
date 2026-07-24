import re
from datetime import date, datetime
from typing import Any

from boxer_company.routers.barcode_log import (
    _build_phase2_scope_request_message,
    _extract_log_date_with_presence,
)
from boxer_company.weekly_recordings_report import (
    _build_weekly_recordings_report_blocks,
    _build_weekly_recordings_report_summary,
    _coerce_weekly_recordings_report_now,
    _format_weekly_recordings_report,
    _is_weekly_recordings_report_request,
)


def _rewrite_phase2_scope_request_message(
    result_text: str,
    title: str,
    example_action: str,
) -> str:
    barcode_match = re.search(r"• 바코드: `([^`]+)`", result_text or "")
    reason_match = re.search(r"• 사유: (.+)", result_text or "")
    barcode = barcode_match.group(1).strip() if barcode_match else ""
    reason = reason_match.group(1).strip() if reason_match else "2차 입력이 필요해"
    return _build_phase2_scope_request_message(
        barcode,
        reason,
        title,
        example_action=example_action,
    )


def _extract_optional_requested_date(question: str) -> tuple[str | None, bool]:
    parsed_date, has_requested_date = _extract_log_date_with_presence(question)
    return (parsed_date if has_requested_date else None, has_requested_date)


def _build_weekly_recordings_report_reply_payload(
    *,
    target_date: str | None = None,
    now: datetime | None = None,
) -> tuple[str, list[dict[str, Any]], str, str]:
    report_target_date = date.fromisoformat(target_date) if target_date else None
    local_now = _coerce_weekly_recordings_report_now(now)
    report_summary = _build_weekly_recordings_report_summary(
        target_date=report_target_date,
        now=local_now,
    )
    return (
        _format_weekly_recordings_report(report_summary, now=local_now),
        _build_weekly_recordings_report_blocks(report_summary, now=local_now),
        str(report_summary.get("weekStartDate") or "").strip()
        or (report_target_date.isoformat() if report_target_date is not None else ""),
        str(report_summary.get("weekEndDate") or "").strip(),
    )
