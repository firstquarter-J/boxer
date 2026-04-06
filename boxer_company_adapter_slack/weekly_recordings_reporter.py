import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.weekly_recordings_report import (
    _WEEKLY_RECORDINGS_REPORT_TITLE,
    _build_weekly_recordings_report_blocks,
    _build_weekly_recordings_report_summary,
    _coerce_weekly_recordings_report_now,
    _format_weekly_recordings_report,
    _resolve_weekly_recordings_report_target_week,
)

_WEEKLY_RECORDINGS_REPORT_THREAD: threading.Thread | None = None
_WEEKLY_RECORDINGS_REPORT_THREAD_LOCK = threading.Lock()


def _weekly_recordings_report_state_path() -> Path:
    return Path(cs.WEEKLY_RECORDINGS_REPORT_STATE_PATH).expanduser()


def _load_weekly_recordings_report_state(
    state_path: Path | None = None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    path = state_path or _weekly_recordings_report_state_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if logger is not None:
            logger.warning("주간 recordings 리포트 상태 파일을 읽지 못했어: %s", path, exc_info=True)
        return {}
    return data if isinstance(data, dict) else {}


def _save_weekly_recordings_report_state(
    state: dict[str, Any],
    state_path: Path | None = None,
) -> None:
    path = state_path or _weekly_recordings_report_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _is_weekly_recordings_report_due(
    now: datetime | None,
    state: dict[str, Any],
) -> bool:
    local_now = _coerce_weekly_recordings_report_now(now)
    if local_now.weekday() != 0:
        return False

    scheduled_hour = max(0, min(23, int(cs.WEEKLY_RECORDINGS_REPORT_HOUR_KST)))
    scheduled_minute = max(0, min(59, int(cs.WEEKLY_RECORDINGS_REPORT_MINUTE_KST)))
    if (local_now.hour, local_now.minute) < (scheduled_hour, scheduled_minute):
        return False

    target_week_start_date, _ = _resolve_weekly_recordings_report_target_week(now=local_now)
    return (
        str(state.get("lastReportedWeekStartDate") or "").strip()
        != target_week_start_date.isoformat()
    )


def _run_weekly_recordings_report_if_due(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> bool:
    if not cs.WEEKLY_RECORDINGS_REPORT_ENABLED:
        return False
    if not s.DB_QUERY_ENABLED:
        logger.warning("주간 recordings 리포트를 켤 수 없어. DB_QUERY_ENABLED가 비활성이야")
        return False

    channel_id = str(cs.WEEKLY_RECORDINGS_REPORT_CHANNEL_ID or "").strip()
    if not channel_id:
        logger.warning("주간 recordings 리포트 채널 ID가 없어. WEEKLY_RECORDINGS_REPORT_CHANNEL_ID를 확인해줘")
        return False

    local_now = _coerce_weekly_recordings_report_now(now)
    state = _load_weekly_recordings_report_state(logger=logger)
    if not _is_weekly_recordings_report_due(local_now, state):
        return False

    target_week_start_date, target_week_end_date = _resolve_weekly_recordings_report_target_week(
        now=local_now
    )
    report_summary = _build_weekly_recordings_report_summary(
        target_date=target_week_start_date,
        now=local_now,
    )
    message_text = _format_weekly_recordings_report(
        report_summary,
        now=local_now,
        include_title=False,
    )
    message_blocks = _build_weekly_recordings_report_blocks(
        report_summary,
        now=local_now,
        include_header=False,
    )
    title_response = client.chat_postMessage(
        channel=channel_id,
        text=_WEEKLY_RECORDINGS_REPORT_TITLE,
        unfurl_links=False,
        unfurl_media=False,
    )
    thread_ts = str(getattr(title_response, "get", lambda *_args, **_kwargs: "")("ts") or "").strip()
    if not thread_ts:
        response_data = getattr(title_response, "data", None)
        thread_ts = str(
            getattr(response_data, "get", lambda *_args, **_kwargs: "")("ts") or ""
        ).strip()
    if not thread_ts:
        raise RuntimeError("주간 recordings 리포트 제목 메시지 ts를 받지 못했어")
    client.chat_postMessage(
        channel=channel_id,
        text=message_text,
        blocks=message_blocks,
        thread_ts=thread_ts,
        unfurl_links=False,
        unfurl_media=False,
    )
    _save_weekly_recordings_report_state(
        {
            "lastReportedWeekStartDate": target_week_start_date.isoformat(),
            "lastReportedWeekEndDate": target_week_end_date.isoformat(),
            "lastSentAt": local_now.isoformat(),
            "channelId": channel_id,
        }
    )
    logger.info(
        "Posted weekly recordings report channel=%s week_start=%s week_end=%s total_count=%s",
        channel_id,
        report_summary.get("weekStartDate"),
        report_summary.get("weekEndDate"),
        report_summary.get("totalCount"),
    )
    return True


def _weekly_recordings_report_loop(client: Any, logger: logging.Logger) -> None:
    poll_interval_sec = max(5, int(cs.WEEKLY_RECORDINGS_REPORT_POLL_INTERVAL_SEC))
    while True:
        try:
            _run_weekly_recordings_report_if_due(client, logger)
        except Exception:
            logger.exception("주간 recordings 리포트 발송 중 오류가 발생했어")
        time.sleep(poll_interval_sec)


def attach_weekly_recordings_reporter(app: Any, *, logger: logging.Logger | None = None) -> None:
    if not cs.WEEKLY_RECORDINGS_REPORT_ENABLED:
        return

    actual_logger = logger or logging.getLogger(__name__)
    if not s.DB_QUERY_ENABLED:
        actual_logger.warning("주간 recordings 리포트가 활성화됐는데 DB_QUERY_ENABLED가 꺼져 있어 시작하지 않을게")
        return

    channel_id = str(cs.WEEKLY_RECORDINGS_REPORT_CHANNEL_ID or "").strip()
    if not channel_id:
        actual_logger.warning(
            "주간 recordings 리포트가 활성화됐는데 채널 ID가 없어. WEEKLY_RECORDINGS_REPORT_CHANNEL_ID를 확인해줘"
        )
        return

    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("주간 recordings 리포트를 시작하지 못했어. Slack client가 없어")
        return

    global _WEEKLY_RECORDINGS_REPORT_THREAD
    with _WEEKLY_RECORDINGS_REPORT_THREAD_LOCK:
        if _WEEKLY_RECORDINGS_REPORT_THREAD is not None and _WEEKLY_RECORDINGS_REPORT_THREAD.is_alive():
            return
        _WEEKLY_RECORDINGS_REPORT_THREAD = threading.Thread(
            target=_weekly_recordings_report_loop,
            args=(client, actual_logger),
            name="weekly-recordings-report",
            daemon=True,
        )
        _WEEKLY_RECORDINGS_REPORT_THREAD.start()

    actual_logger.info(
        "Started weekly recordings report scheduler channel=%s every Monday at %02d:%02d KST",
        channel_id,
        max(0, min(23, int(cs.WEEKLY_RECORDINGS_REPORT_HOUR_KST))),
        max(0, min(59, int(cs.WEEKLY_RECORDINGS_REPORT_MINUTE_KST))),
    )
