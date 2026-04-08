import json
import logging
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from boxer.core import settings as s
from boxer_company import settings as cs
from boxer_company.daily_device_round import (
    _build_daily_device_round_blocks,
    _build_daily_device_round_summary,
    _build_daily_device_round_title_text,
    _coerce_daily_device_round_now,
    _daily_device_round_timezone,
)

_DAILY_DEVICE_ROUND_THREAD: threading.Thread | None = None
_DAILY_DEVICE_ROUND_THREAD_LOCK = threading.Lock()


def _daily_device_round_state_path() -> Path:
    return Path(cs.DAILY_DEVICE_ROUND_STATE_PATH).expanduser()


def _load_daily_device_round_state(
    state_path: Path | None = None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    path = state_path or _daily_device_round_state_path()
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if logger is not None:
            logger.warning("일일 장비 순회 상태 파일을 읽지 못했어: %s", path, exc_info=True)
        return {}
    return data if isinstance(data, dict) else {}


def _save_daily_device_round_state(
    state: dict[str, Any],
    state_path: Path | None = None,
) -> None:
    path = state_path or _daily_device_round_state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(state, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _is_daily_device_round_runtime_configured() -> bool:
    return bool(
        cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


def _is_daily_device_round_due(
    now: datetime | None,
    state: dict[str, Any],
) -> bool:
    local_now = _coerce_daily_device_round_now(now)
    scheduled_hour = max(0, min(23, int(cs.DAILY_DEVICE_ROUND_HOUR_KST)))
    scheduled_minute = max(0, min(59, int(cs.DAILY_DEVICE_ROUND_MINUTE_KST)))
    if (local_now.hour, local_now.minute) < (scheduled_hour, scheduled_minute):
        return False
    return str(state.get("lastRunDate") or "").strip() != local_now.date().isoformat()


def _run_daily_device_round_if_due(
    client: Any,
    logger: logging.Logger,
    *,
    now: datetime | None = None,
) -> bool:
    if not cs.DAILY_DEVICE_ROUND_ENABLED:
        return False
    if not s.DB_QUERY_ENABLED:
        logger.warning("일일 장비 순회를 켤 수 없어. DB_QUERY_ENABLED가 비활성이야")
        return False
    if not _is_daily_device_round_runtime_configured():
        logger.warning("일일 장비 순회를 켤 수 없어. MDA/SSH 설정이 부족해")
        return False

    channel_id = str(cs.DAILY_DEVICE_ROUND_CHANNEL_ID or "").strip()
    if not channel_id:
        logger.warning("일일 장비 순회 채널 ID가 없어. DAILY_DEVICE_ROUND_CHANNEL_ID를 확인해줘")
        return False

    local_now = _coerce_daily_device_round_now(now)
    state = _load_daily_device_round_state(logger=logger)
    if not _is_daily_device_round_due(local_now, state):
        return False

    report_summary = _build_daily_device_round_summary(
        now=local_now,
        state=state,
        auto_update_agent=bool(cs.DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT),
        auto_update_box=bool(cs.DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX),
    )
    message_text = _build_daily_device_round_report_text(report_summary, now=local_now)
    message_blocks = _build_daily_device_round_blocks(
        report_summary,
        now=local_now,
        include_header=False,
    )
    title_text = _build_daily_device_round_title_text(report_summary)
    title_response = client.chat_postMessage(
        channel=channel_id,
        text=title_text,
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
        raise RuntimeError("일일 장비 순회 제목 메시지 ts를 받지 못했어")

    client.chat_postMessage(
        channel=channel_id,
        text=message_text,
        blocks=message_blocks,
        thread_ts=thread_ts,
        unfurl_links=False,
        unfurl_media=False,
    )
    _save_daily_device_round_state(
        {
            "lastRunDate": _coerce_daily_device_round_now(local_now).date().isoformat(),
            "lastHospitalSeq": report_summary.get("hospitalSeq"),
            "lastHospitalName": report_summary.get("hospitalName"),
            "nextHospitalSeq": report_summary.get("nextHospitalSeq"),
            "lastSentAt": local_now.isoformat(),
            "channelId": channel_id,
            "statusCounts": report_summary.get("statusCounts"),
            "updateCounts": report_summary.get("updateCounts"),
        }
    )
    logger.info(
        "Posted daily device round channel=%s hospitalSeq=%s hospitalName=%s deviceCount=%s",
        channel_id,
        report_summary.get("hospitalSeq"),
        report_summary.get("hospitalName"),
        report_summary.get("deviceCount"),
    )
    return True


def _build_daily_device_round_report_text(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
) -> str:
    from boxer_company.daily_device_round import _format_daily_device_round_report

    return _format_daily_device_round_report(
        report_summary,
        now=now,
        include_title=False,
    )


def _daily_device_round_loop(client: Any, logger: logging.Logger) -> None:
    poll_interval_sec = max(5, int(cs.DAILY_DEVICE_ROUND_POLL_INTERVAL_SEC))
    while True:
        try:
            _run_daily_device_round_if_due(client, logger)
        except Exception:
            logger.exception("일일 장비 순회 중 오류가 발생했어")
        time.sleep(poll_interval_sec)


def attach_daily_device_round_reporter(app: Any, *, logger: logging.Logger | None = None) -> None:
    if not cs.DAILY_DEVICE_ROUND_ENABLED:
        return

    actual_logger = logger or logging.getLogger(__name__)
    if not s.DB_QUERY_ENABLED:
        actual_logger.warning("일일 장비 순회가 활성화됐는데 DB_QUERY_ENABLED가 꺼져 있어 시작하지 않을게")
        return
    if not _is_daily_device_round_runtime_configured():
        actual_logger.warning("일일 장비 순회가 활성화됐는데 MDA/SSH 설정이 부족해 시작하지 않을게")
        return

    channel_id = str(cs.DAILY_DEVICE_ROUND_CHANNEL_ID or "").strip()
    if not channel_id:
        actual_logger.warning(
            "일일 장비 순회가 활성화됐는데 채널 ID가 없어. DAILY_DEVICE_ROUND_CHANNEL_ID를 확인해줘"
        )
        return

    client = getattr(app, "client", None)
    if client is None:
        actual_logger.warning("일일 장비 순회를 시작하지 못했어. Slack client가 없어")
        return

    global _DAILY_DEVICE_ROUND_THREAD
    with _DAILY_DEVICE_ROUND_THREAD_LOCK:
        if _DAILY_DEVICE_ROUND_THREAD is not None and _DAILY_DEVICE_ROUND_THREAD.is_alive():
            return
        _DAILY_DEVICE_ROUND_THREAD = threading.Thread(
            target=_daily_device_round_loop,
            args=(client, actual_logger),
            name="daily-device-round",
            daemon=True,
        )
        _DAILY_DEVICE_ROUND_THREAD.start()

    local_tz = _daily_device_round_timezone()
    actual_logger.info(
        "Started daily device round scheduler channel=%s every day at %02d:%02d %s",
        channel_id,
        max(0, min(23, int(cs.DAILY_DEVICE_ROUND_HOUR_KST))),
        max(0, min(59, int(cs.DAILY_DEVICE_ROUND_MINUTE_KST))),
        local_tz.key,
    )
