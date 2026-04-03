import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.daily_recordings_report import (
    _build_daily_recordings_report_summary,
    _build_daily_recordings_report_blocks,
    _coerce_daily_recordings_report_now,
    _format_daily_recordings_report,
)

_DAILY_RECORDINGS_REPORT_THREAD: threading.Thread | None = None
_DAILY_RECORDINGS_REPORT_THREAD_LOCK = threading.Lock()


def _daily_recordings_report_state_path() -> Path:
    return Path(cs.DAILY_RECORDINGS_REPORT_STATE_PATH).expanduser()


def _load_daily_recordings_report_state(
    state_path: Path | None = None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    path = state_path or _daily_recordings_report_state_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if logger is not None:
            logger.warning("일일 recordings 리포트 상태 파일을 읽지 못했어: %s", path, exc_info=True)
        return {}
    return data if isinstance(data, dict) else {}


def _save_daily_recordings_report_state(
    state: dict[str, Any],
    state_path: Path | None = None,
) -> None:
    path = state_path or _daily_recordings_report_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _is_daily_recordings_report_due(
    now: datetime | None,
    state: dict[str, Any],
) -> bool:
    local_now = _coerce_daily_recordings_report_now(now)
    scheduled_hour = max(0, min(23, int(cs.DAILY_RECORDINGS_REPORT_HOUR_KST)))
    scheduled_minute = max(0, min(59, int(cs.DAILY_RECORDINGS_REPORT_MINUTE_KST)))
    if (local_now.hour, local_now.minute) < (scheduled_hour, scheduled_minute):
        return False
    return str(state.get("lastReportedLocalDate") or "").strip() != local_now.date().isoformat()


def _run_daily_recordings_report_if_due(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> bool:
    if not cs.DAILY_RECORDINGS_REPORT_ENABLED:
        return False
    if not s.DB_QUERY_ENABLED:
        logger.warning("일일 recordings 리포트를 켤 수 없어. DB_QUERY_ENABLED가 비활성이야")
        return False

    channel_id = str(cs.DAILY_RECORDINGS_REPORT_CHANNEL_ID or "").strip()
    if not channel_id:
        logger.warning("일일 recordings 리포트 채널 ID가 없어. DAILY_RECORDINGS_REPORT_CHANNEL_ID를 확인해줘")
        return False

    local_now = _coerce_daily_recordings_report_now(now)
    state = _load_daily_recordings_report_state(logger=logger)
    if not _is_daily_recordings_report_due(local_now, state):
        return False

    report_summary = _build_daily_recordings_report_summary(now=local_now)
    message_text = _format_daily_recordings_report(report_summary, now=local_now)
    message_blocks = _build_daily_recordings_report_blocks(report_summary, now=local_now)
    client.chat_postMessage(
        channel=channel_id,
        text=message_text,
        blocks=message_blocks,
        unfurl_links=False,
        unfurl_media=False,
    )
    _save_daily_recordings_report_state(
        {
            "lastReportedLocalDate": local_now.date().isoformat(),
            "lastTargetDate": str(report_summary.get("targetDate") or "").strip() or None,
            "lastSentAt": local_now.isoformat(),
            "channelId": channel_id,
        }
    )
    logger.info(
        "Posted daily recordings report channel=%s target_date=%s total_count=%s",
        channel_id,
        report_summary.get("targetDate"),
        report_summary.get("totalCount"),
    )
    return True


def _daily_recordings_report_loop(client: Any, logger: logging.Logger) -> None:
    poll_interval_sec = max(5, int(cs.DAILY_RECORDINGS_REPORT_POLL_INTERVAL_SEC))
    while True:
        try:
            _run_daily_recordings_report_if_due(client, logger)
        except Exception:
            logger.exception("일일 recordings 리포트 발송 중 오류가 발생했어")
        time.sleep(poll_interval_sec)


def attach_daily_recordings_reporter(app: Any, *, logger: logging.Logger | None = None) -> None:
    if not cs.DAILY_RECORDINGS_REPORT_ENABLED:
        return

    actual_logger = logger or logging.getLogger(__name__)
    if not s.DB_QUERY_ENABLED:
        actual_logger.warning("일일 recordings 리포트가 활성화됐는데 DB_QUERY_ENABLED가 꺼져 있어 시작하지 않을게")
        return

    channel_id = str(cs.DAILY_RECORDINGS_REPORT_CHANNEL_ID or "").strip()
    if not channel_id:
        actual_logger.warning(
            "일일 recordings 리포트가 활성화됐는데 채널 ID가 없어. DAILY_RECORDINGS_REPORT_CHANNEL_ID를 확인해줘"
        )
        return

    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("일일 recordings 리포트를 시작하지 못했어. Slack client가 없어")
        return

    global _DAILY_RECORDINGS_REPORT_THREAD
    with _DAILY_RECORDINGS_REPORT_THREAD_LOCK:
        if _DAILY_RECORDINGS_REPORT_THREAD is not None and _DAILY_RECORDINGS_REPORT_THREAD.is_alive():
            return
        _DAILY_RECORDINGS_REPORT_THREAD = threading.Thread(
            target=_daily_recordings_report_loop,
            args=(client, actual_logger),
            name="daily-recordings-report",
            daemon=True,
        )
        _DAILY_RECORDINGS_REPORT_THREAD.start()

    actual_logger.info(
        "Started daily recordings report scheduler channel=%s at %02d:%02d KST",
        channel_id,
        max(0, min(23, int(cs.DAILY_RECORDINGS_REPORT_HOUR_KST))),
        max(0, min(59, int(cs.DAILY_RECORDINGS_REPORT_MINUTE_KST))),
    )
