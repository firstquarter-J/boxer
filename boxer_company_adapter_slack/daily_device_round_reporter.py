import json
import logging
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from boxer.core import settings as s
from boxer.core.utils import _display_value
from boxer_company import settings as cs
from boxer_company.daily_device_round import (
    _build_daily_device_round_blocks,
    _build_daily_device_round_summary,
    _coerce_daily_device_round_hospital_seqs,
    _coerce_daily_device_round_now,
    _coerce_int,
    _daily_device_round_timezone,
    _format_daily_device_round_report,
)

_DAILY_DEVICE_ROUND_THREAD: threading.Thread | None = None
_DAILY_DEVICE_ROUND_THREAD_LOCK = threading.Lock()
_DAILY_DEVICE_ROUND_RUNTIME_STATE: dict[str, Any] = {}
_DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK = threading.Lock()
_DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE = 40
_DAILY_DEVICE_ROUND_MAX_BLOCK_CHARS_PER_MESSAGE = 12000
_DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE = 3500


def _daily_device_round_state_path() -> Path:
    return Path(cs.DAILY_DEVICE_ROUND_STATE_PATH).expanduser()


def _load_daily_device_round_state(
    state_path: Path | None = None,
    *,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    path = state_path or _daily_device_round_state_path()
    runtime_state = _load_daily_device_round_runtime_state()
    if not path.exists():
        return runtime_state
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        if logger is not None:
            logger.warning("일일 장비 순회 상태 파일을 읽지 못했어: %s", path, exc_info=True)
        return runtime_state
    state = data if isinstance(data, dict) else {}
    if runtime_state:
        merged_state = dict(state)
        merged_state.update(runtime_state)
        return merged_state
    return state


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


def _load_daily_device_round_runtime_state() -> dict[str, Any]:
    with _DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
        return dict(_DAILY_DEVICE_ROUND_RUNTIME_STATE)


def _remember_daily_device_round_runtime_state(
    state: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    normalized_state = _normalize_daily_device_round_state(state, now=now)
    with _DAILY_DEVICE_ROUND_RUNTIME_STATE_LOCK:
        _DAILY_DEVICE_ROUND_RUNTIME_STATE.clear()
        _DAILY_DEVICE_ROUND_RUNTIME_STATE.update(normalized_state)
    return normalized_state


def _persist_daily_device_round_state_best_effort(
    state: dict[str, Any],
    *,
    now: datetime | None = None,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:
    normalized_state = _remember_daily_device_round_runtime_state(state, now=now)
    try:
        _save_daily_device_round_state(normalized_state)
    except Exception:
        if logger is not None:
            logger.warning("일일 장비 순회 상태를 즉시 저장하지 못했어", exc_info=True)
    return normalized_state


def _estimate_daily_device_round_block_size(block: dict[str, Any]) -> int:
    return len(json.dumps(block, ensure_ascii=False))


def _split_daily_device_round_blocks(
    blocks: list[dict[str, Any]],
) -> list[list[dict[str, Any]]]:
    chunks: list[list[dict[str, Any]]] = []
    current_chunk: list[dict[str, Any]] = []
    current_size = 0

    for block in blocks:
        block_size = _estimate_daily_device_round_block_size(block)
        should_rotate = (
            current_chunk
            and (
                len(current_chunk) >= _DAILY_DEVICE_ROUND_MAX_BLOCKS_PER_MESSAGE
                or current_size + block_size > _DAILY_DEVICE_ROUND_MAX_BLOCK_CHARS_PER_MESSAGE
            )
        )
        if should_rotate:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
        current_chunk.append(block)
        current_size += block_size

    if current_chunk:
        chunks.append(current_chunk)
    return chunks or [[]]


def _split_daily_device_round_text(
    text: str,
) -> list[str]:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return [""]
    if len(normalized_text) <= _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE:
        return [normalized_text]

    chunks: list[str] = []
    current_chunk = ""
    # Slack text fallback도 길이 제한을 넘지 않도록 줄 단위로 먼저 자르고,
    # 한 줄이 너무 길면 그 줄만 다시 잘라서 이어 보내.
    for line in normalized_text.split("\n"):
        line_parts = [line] or [""]
        if len(line) > _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE:
            line_parts = [
                line[index : index + _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE]
                for index in range(0, len(line), _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE)
            ]
        for line_part in line_parts:
            candidate = line_part if not current_chunk else f"{current_chunk}\n{line_part}"
            if current_chunk and len(candidate) > _DAILY_DEVICE_ROUND_MAX_TEXT_CHARS_PER_MESSAGE:
                chunks.append(current_chunk)
                current_chunk = line_part
                continue
            current_chunk = candidate

    if current_chunk:
        chunks.append(current_chunk)
    return chunks


def _build_daily_device_round_chunk_text(
    base_text: str,
    *,
    chunk_index: int,
    chunk_count: int,
) -> str:
    if chunk_count <= 1:
        return base_text
    return f"{base_text} | 계속 {chunk_index + 1}/{chunk_count}"


def _is_daily_device_round_runtime_configured() -> bool:
    return bool(
        cs.MDA_GRAPHQL_URL
        and cs.MDA_ADMIN_USER_PASSWORD
        and cs.DEVICE_SSH_PASSWORD
    )


def _daily_device_round_window_schedule() -> tuple[tuple[int, int], tuple[int, int]]:
    start_hour = max(0, min(23, int(cs.DAILY_DEVICE_ROUND_HOUR_KST)))
    start_minute = max(0, min(59, int(cs.DAILY_DEVICE_ROUND_MINUTE_KST)))
    end_hour = max(0, min(23, int(cs.DAILY_DEVICE_ROUND_END_HOUR_KST)))
    end_minute = max(0, min(59, int(cs.DAILY_DEVICE_ROUND_END_MINUTE_KST)))
    return (start_hour, start_minute), (end_hour, end_minute)


def _daily_device_round_window_key(now: datetime | None) -> str | None:
    local_now = _coerce_daily_device_round_now(now)
    (start_hour, start_minute), (end_hour, end_minute) = _daily_device_round_window_schedule()
    start_minutes = (start_hour * 60) + start_minute
    end_minutes = (end_hour * 60) + end_minute
    current_minutes = (local_now.hour * 60) + local_now.minute

    if start_minutes == end_minutes:
        return local_now.date().isoformat()

    if start_minutes < end_minutes:
        if start_minutes <= current_minutes < end_minutes:
            return local_now.date().isoformat()
        return None

    if current_minutes >= start_minutes:
        return local_now.date().isoformat()
    if current_minutes < end_minutes:
        return (local_now.date() - timedelta(days=1)).isoformat()
    return None


def _normalize_daily_device_round_state(
    state: dict[str, Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    state_payload = state if isinstance(state, dict) else {}
    normalized_state = dict(state_payload)
    current_window_key = _daily_device_round_window_key(now)
    normalized_state["lastHospitalSeq"] = _coerce_int(state_payload.get("lastHospitalSeq"))
    normalized_state["nextHospitalSeq"] = _coerce_int(state_payload.get("nextHospitalSeq"))
    normalized_state["windowThreadTs"] = str(state_payload.get("windowThreadTs") or "").strip()
    normalized_state["windowThreadChannelId"] = str(state_payload.get("windowThreadChannelId") or "").strip()
    normalized_state["processedHospitalSeqs"] = _coerce_daily_device_round_hospital_seqs(
        state_payload.get("processedHospitalSeqs")
    )
    if not current_window_key:
        normalized_state["windowKey"] = None
        normalized_state.pop("windowCompletedAt", None)
        normalized_state["windowThreadTs"] = ""
        normalized_state["windowThreadChannelId"] = ""
        return normalized_state

    previous_window_key = str(state_payload.get("windowKey") or "").strip()
    normalized_state["windowKey"] = current_window_key
    if previous_window_key != current_window_key:
        normalized_state["processedHospitalSeqs"] = []
        normalized_state.pop("windowCompletedAt", None)
        normalized_state["windowThreadTs"] = ""
        normalized_state["windowThreadChannelId"] = ""
        # Legacy fixed-target mode persisted the same hospital as both last/next.
        # Clear that self-loop on a new window so the first run can rotate forward.
        if normalized_state.get("nextHospitalSeq") == normalized_state.get("lastHospitalSeq"):
            normalized_state["nextHospitalSeq"] = None
    return normalized_state


def _build_daily_device_round_window_title_text(now: datetime | None = None) -> str:
    local_now = _coerce_daily_device_round_now(now)
    return f"일일 장비 순회 점검 & 업데이트 | {local_now:%Y-%m-%d}"


def _extract_daily_device_round_thread_ts(response: Any) -> str:
    thread_ts = str(getattr(response, "get", lambda *_args, **_kwargs: "")("ts") or "").strip()
    if thread_ts:
        return thread_ts
    response_data = getattr(response, "data", None)
    return str(
        getattr(response_data, "get", lambda *_args, **_kwargs: "")("ts") or ""
    ).strip()


def _is_daily_device_round_due(
    now: datetime | None,
    state: dict[str, Any],
) -> bool:
    if _daily_device_round_window_key(now) is None:
        return False
    normalized_state = _normalize_daily_device_round_state(state, now=now)
    return not str(normalized_state.get("windowCompletedAt") or "").strip()


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
    raw_state = _load_daily_device_round_state(logger=logger)
    state = _normalize_daily_device_round_state(raw_state, now=local_now)
    if not _is_daily_device_round_due(local_now, state):
        return False

    report_summary = _build_daily_device_round_summary(
        now=local_now,
        state=state,
        auto_update_agent=True,
        auto_update_box=bool(cs.DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX),
        auto_cleanup_trashcan=bool(cs.DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN),
    )
    hospital_seq = _coerce_int(report_summary.get("hospitalSeq"))
    processed_hospital_seqs = _coerce_daily_device_round_hospital_seqs(state.get("processedHospitalSeqs"))
    if hospital_seq is not None and hospital_seq not in processed_hospital_seqs:
        processed_hospital_seqs.append(hospital_seq)
    candidate_hospital_count = max(0, int(report_summary.get("candidateHospitalCount") or 0))
    next_state = {
        **state,
        "windowKey": _daily_device_round_window_key(local_now),
        "processedHospitalSeqs": processed_hospital_seqs,
        "windowThreadTs": str(state.get("windowThreadTs") or "").strip(),
        "windowThreadChannelId": str(state.get("windowThreadChannelId") or "").strip(),
        "windowCompletedAt": (
            local_now.isoformat()
            if hospital_seq is None or (
                candidate_hospital_count > 0
                and len(processed_hospital_seqs) >= candidate_hospital_count
            )
            else ""
        ),
        "lastRunDate": _coerce_daily_device_round_now(local_now).date().isoformat(),
        "lastHospitalSeq": report_summary.get("hospitalSeq"),
        "lastHospitalName": report_summary.get("hospitalName"),
        "nextHospitalSeq": report_summary.get("nextHospitalSeq"),
        "lastSentAt": local_now.isoformat(),
        "channelId": channel_id,
        "statusCounts": report_summary.get("statusCounts"),
        "updateCounts": report_summary.get("updateCounts"),
        "cleanupCounts": report_summary.get("cleanupCounts"),
    }
    if hospital_seq is None:
        _remember_daily_device_round_runtime_state(next_state, now=local_now)
        _save_daily_device_round_state(next_state)
        logger.info(
            "Daily device round window paused channel=%s windowKey=%s reason=%s",
            channel_id,
            next_state.get("windowKey"),
            report_summary.get("summaryLine"),
        )
        return False

    message_text = _build_daily_device_round_report_text(report_summary, now=local_now)
    message_blocks = _build_daily_device_round_blocks(
        report_summary,
        now=local_now,
        include_header=False,
    )
    message_block_chunks = _split_daily_device_round_blocks(message_blocks)
    thread_ts = str(state.get("windowThreadTs") or "").strip()
    thread_channel_id = str(state.get("windowThreadChannelId") or "").strip()
    if not thread_ts or thread_channel_id != channel_id:
        title_response = client.chat_postMessage(
            channel=channel_id,
            text=_build_daily_device_round_window_title_text(local_now),
            unfurl_links=False,
            unfurl_media=False,
        )
        thread_ts = _extract_daily_device_round_thread_ts(title_response)
    if not thread_ts:
        raise RuntimeError("일일 장비 순회 제목 메시지 ts를 받지 못했어")
    next_state["windowThreadTs"] = thread_ts
    next_state["windowThreadChannelId"] = channel_id
    # 본문 전송이 실패하더라도 다음 재시도에서 같은 제목 스레드를 재사용해.
    _persist_daily_device_round_state_best_effort(
        {
            **state,
            "windowKey": next_state.get("windowKey"),
            "windowThreadTs": thread_ts,
            "windowThreadChannelId": channel_id,
            "channelId": channel_id,
        },
        now=local_now,
        logger=logger,
    )

    try:
        for index, block_chunk in enumerate(message_block_chunks):
            client.chat_postMessage(
                channel=channel_id,
                text=_build_daily_device_round_chunk_text(
                    message_text,
                    chunk_index=index,
                    chunk_count=len(message_block_chunks),
                ),
                blocks=block_chunk,
                thread_ts=thread_ts,
                unfurl_links=False,
                unfurl_media=False,
            )
    except Exception:
        # rich_text/section 길이 같은 block payload 오류가 나면 plain text 전체 리포트로라도 남겨.
        logger.warning(
            "일일 장비 순회 block 전송이 실패해서 text fallback으로 다시 보낼게 channel=%s thread_ts=%s hospitalSeq=%s",
            channel_id,
            thread_ts,
            hospital_seq,
            exc_info=True,
        )
        fallback_text_chunks = _split_daily_device_round_text(
            _format_daily_device_round_report(
                report_summary,
                now=local_now,
                include_title=False,
            )
        )
        for index, text_chunk in enumerate(fallback_text_chunks):
            text_body = text_chunk
            if len(fallback_text_chunks) > 1:
                text_body = f"(계속 {index + 1}/{len(fallback_text_chunks)})\n{text_chunk}"
            client.chat_postMessage(
                channel=channel_id,
                text=text_body,
                thread_ts=thread_ts,
                unfurl_links=False,
                unfurl_media=False,
            )
    _remember_daily_device_round_runtime_state(next_state, now=local_now)
    _save_daily_device_round_state(next_state)
    logger.info(
        "Posted daily device round channel=%s hospitalSeq=%s hospitalName=%s deviceCount=%s windowKey=%s processed=%s/%s",
        channel_id,
        report_summary.get("hospitalSeq"),
        report_summary.get("hospitalName"),
        report_summary.get("deviceCount"),
        next_state.get("windowKey"),
        len(processed_hospital_seqs),
        candidate_hospital_count,
    )
    return True


def _build_daily_device_round_report_text(
    report_summary: dict[str, Any],
    *,
    now: datetime | None = None,
) -> str:
    from boxer_company.daily_device_round import _format_daily_device_round_hospital_label

    hospital_label = _format_daily_device_round_hospital_label(
        report_summary.get("hospitalName"),
        _coerce_int(report_summary.get("hospitalSeq")),
    )
    status_counts = report_summary.get("statusCounts") if isinstance(report_summary.get("statusCounts"), dict) else {}
    update_counts = report_summary.get("updateCounts") if isinstance(report_summary.get("updateCounts"), dict) else {}
    cleanup_counts = report_summary.get("cleanupCounts") if isinstance(report_summary.get("cleanupCounts"), dict) else {}
    summary_line = _display_value(report_summary.get("summaryLine"), default="점검 결과")

    if _coerce_int(report_summary.get("hospitalSeq")) is None:
        return summary_line

    executed_parts: list[str] = []

    agent_updated = int(update_counts.get("agentUpdated") or 0)
    agent_failed = int(update_counts.get("agentUpdateFailed") or 0)
    box_updated = int(update_counts.get("boxUpdated") or 0)
    box_failed = int(update_counts.get("boxUpdateFailed") or 0)
    update_parts: list[str] = []
    if agent_updated or agent_failed:
        item = f"에이전트 {agent_updated}"
        if agent_failed:
            item = f"{item} 실패 {agent_failed}"
        update_parts.append(item)
    if box_updated or box_failed:
        item = f"박스 {box_updated}"
        if box_failed:
            item = f"{item} 실패 {box_failed}"
        update_parts.append(item)
    if update_parts:
        executed_parts.append("업데이트 " + " / ".join(update_parts))

    cleanup_executed = int(cleanup_counts.get("executed") or 0)
    cleanup_failed = int(cleanup_counts.get("failed") or 0)
    if cleanup_executed or cleanup_failed:
        cleanup_text = f"정리 실행 {cleanup_executed}"
        if cleanup_failed:
            cleanup_text = f"{cleanup_text} / 실패 {cleanup_failed}"
        executed_parts.append(cleanup_text)

    if executed_parts:
        return f"{hospital_label} | {' | '.join(executed_parts)}"

    return hospital_label


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
    (start_hour, start_minute), (end_hour, end_minute) = _daily_device_round_window_schedule()
    actual_logger.info(
        "Started daily device round scheduler channel=%s every day from %02d:%02d to %02d:%02d %s",
        channel_id,
        start_hour,
        start_minute,
        end_hour,
        end_minute,
        local_tz.key,
    )
