from typing import Any

from boxer.company import settings as cs
from boxer.core.utils import _display_value, _truncate_text
from boxer.routers.company.barcode_log import (
    _build_phase2_scope_request_message,
    _error_lines_in_session,
    _expand_device_contexts_to_recordings_hospital_scope,
    _extract_recording_sessions,
    _extract_scan_events_with_line_no,
    _find_error_lines,
    _find_first_ffmpeg_error_context,
    _find_recording_recovery_context,
)
from boxer.routers.company.box_db import (
    _lookup_device_contexts_by_barcode,
)
from boxer.routers.company.s3_domain import _fetch_s3_device_log_lines

_DEVICE_FILE_PROBE_HINTS = (
    "fileid",
    "file id",
    "파일id",
    "파일 id",
    "파일 아이디",
    "파일아이디",
    "파일 있",
    "파일있",
    "파일 있어",
    "파일있어",
    "파일 존재",
    "존재 확인",
    "장비 파일",
    "장비에 파일",
    "디바이스 파일",
    "로컬 파일",
    "다운로드",
    "받아줘",
    "받아 줘",
    "내려받아",
    "복구",
)


def _is_barcode_device_file_probe_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_PROBE_HINTS)


def _build_session_file_candidate_entry(
    source_lines: list[str],
    session: dict[str, Any],
    session_error_lines: list[tuple[int, str]],
) -> dict[str, Any]:
    first_ffmpeg_error = _find_first_ffmpeg_error_context(session_error_lines, [session])
    recovery_context = _find_recording_recovery_context(
        source_lines,
        session,
    )

    added_recording = (recovery_context or {}).get("addedRecording") or {}
    started_recording = (recovery_context or {}).get("startedRecording") or {}
    spawned_recording = (recovery_context or {}).get("spawnedRecordingFfmpeg") or {}
    spawned_motion = (recovery_context or {}).get("spawnedMotionFfmpeg") or {}

    return {
        "startTime": _display_value(session.get("start_time_label"), default="시간미상"),
        "stopTime": _display_value(session.get("stop_time_label"), default="미확인"),
        "stopToken": _display_value(session.get("stop_token"), default=""),
        "fileId": _display_value((recovery_context or {}).get("fileId"), default=""),
        "addedRecordingTime": _display_value(added_recording.get("timeLabel"), default=""),
        "startedRecordingTime": _display_value(started_recording.get("timeLabel"), default=""),
        "spawnedRecordingTime": _display_value(spawned_recording.get("timeLabel"), default=""),
        "spawnedMotionTime": _display_value(spawned_motion.get("timeLabel"), default=""),
        "firstFfmpegErrorTime": _display_value(
            (first_ffmpeg_error or {}).get("timeLabel"),
            default="",
        ),
    }


def _render_file_candidate_result(
    *,
    barcode: str,
    log_date: str,
    all_device_contexts: list[dict[str, Any]],
    records: list[dict[str, Any]],
    used_expanded_scope: bool,
    logs_found_any: int,
) -> str:
    if logs_found_any == 0:
        return (
            "*파일 확인 대상 세션 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            f"• 매핑 장비: `{len(all_device_contexts)}개`\n"
            "• 확인한 로그 파일: `0개`"
        )

    if not records:
        lines = [
            "*파일 확인 대상 세션 조회 결과*",
            f"• 바코드: `{barcode}`",
            f"• 날짜: `{log_date}`",
            f"• 매핑 장비: `{len(all_device_contexts)}개`",
            f"• 확인한 로그 파일: `{logs_found_any}개`",
            "• 결과: 요청 바코드 세션을 찾지 못했어",
        ]
        if used_expanded_scope:
            lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비까지 확장 검색했어")
        return "\n".join(lines)

    lines = [
        "*파일 확인 대상 세션 조회 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(all_device_contexts)}개`",
        f"• 세션이 확인된 장비: `{len(records)}개`",
    ]
    if used_expanded_scope:
        lines.append("• 참고: 매핑 장비에서 세션을 못 찾아 동일 병원 장비까지 확장 검색했어")

    for record in records:
        lines.append("")
        lines.append(f"• 장비: `{_display_value(record.get('deviceName'), default='미확인')}`")
        lines.append(f"• 병원: `{_display_value(record.get('hospitalName'), default='미확인')}`")
        lines.append(f"• 병실: `{_display_value(record.get('roomName'), default='미확인')}`")
        lines.append(f"• 파일: `{_display_value(record.get('logKey'), default='미확인')}`")
        lines.append(f"• 세션 수: `{len(record.get('sessions') or [])}건`")

        for index, session in enumerate(record.get("sessions") or [], start=1):
            lines.append("")
            lines.append(
                f"*세션 {index}* (`{_display_value(session.get('startTime'), default='시간미상')}`"
                f" ~ `{_display_value(session.get('stopTime'), default='미확인')}`)"
            )
            stop_token = _display_value(session.get("stopToken"), default="")
            if stop_token:
                lines.append(f"• 종료 토큰: `{stop_token}`")
            file_id = _display_value(session.get("fileId"), default="미추출")
            lines.append(f"• fileId: `{file_id}`")

            added_time = _display_value(session.get("addedRecordingTime"), default="")
            started_time = _display_value(session.get("startedRecordingTime"), default="")
            spawned_time = _display_value(session.get("spawnedRecordingTime"), default="")
            spawned_motion_time = _display_value(session.get("spawnedMotionTime"), default="")
            first_ffmpeg_error_time = _display_value(session.get("firstFfmpegErrorTime"), default="")
            start_logs: list[str] = []
            if added_time:
                start_logs.append(f"addRecording `{added_time}`")
            if started_time:
                start_logs.append(f"Started recording `{started_time}`")
            if spawned_time:
                start_logs.append(f"RECORDING ffmpeg 시작 `{spawned_time}`")
            if spawned_motion_time and not spawned_time:
                start_logs.append(f"MOTION ffmpeg 시작 `{spawned_motion_time}`")
            if start_logs:
                lines.append(f"• fileId 근거 로그: {', '.join(start_logs)}")
            if first_ffmpeg_error_time:
                lines.append(f"• 첫 ffmpeg 오류: `{first_ffmpeg_error_time}`")

    return _truncate_text("\n".join(lines), 38000)


def _locate_barcode_file_candidates(
    s3_client: Any,
    barcode: str,
    log_date: str,
    *,
    recordings_context: dict[str, Any] | None = None,
    device_contexts: list[dict[str, Any]] | None = None,
) -> tuple[str, dict[str, Any]]:
    all_device_contexts = device_contexts
    if all_device_contexts is None:
        all_device_contexts = _lookup_device_contexts_by_barcode(
            barcode,
            recordings_context=recordings_context,
        )

    if not all_device_contexts:
        result_text = (
            "*파일 확인 대상 세션 조회 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• devices에서 장비 매핑 정보를 찾지 못했어"
        )
        return result_text, {
            "route": "device_file_candidate_lookup",
            "request": {"barcode": barcode, "date": log_date},
            "records": [],
        }

    max_devices = max(1, min(20, cs.LOG_ANALYSIS_MAX_DEVICES))
    target_device_contexts = all_device_contexts[:max_devices]
    logs_found_any = 0
    records: list[dict[str, Any]] = []
    used_expanded_scope = False

    def _analyze_batch(device_context_batch: list[dict[str, Any]]) -> None:
        nonlocal logs_found_any
        for device_context in device_context_batch:
            device_name = str(device_context.get("deviceName") or "").strip()
            if not device_name:
                continue

            log_data = _fetch_s3_device_log_lines(
                s3_client,
                device_name,
                log_date,
                tail_only=False,
            )
            if not log_data["found"]:
                continue

            logs_found_any += 1
            source_lines = log_data["lines"]
            events = _extract_scan_events_with_line_no(source_lines)
            sessions = _extract_recording_sessions(
                source_lines,
                barcode,
                cs.LOG_SESSION_SAFETY_LINES,
                scan_events=events,
            )
            if not sessions:
                continue

            error_lines = _find_error_lines(source_lines)
            session_entries: list[dict[str, Any]] = []
            for session in sessions:
                session_entries.append(
                    _build_session_file_candidate_entry(
                        source_lines,
                        session,
                        _error_lines_in_session(error_lines, session),
                    )
                )

            records.append(
                {
                    "deviceName": device_name,
                    "hospitalName": _display_value(device_context.get("hospitalName"), default="미확인"),
                    "roomName": _display_value(device_context.get("roomName"), default="미확인"),
                    "logKey": _display_value(log_data.get("key"), default="미확인"),
                    "sessions": session_entries,
                }
            )

    _analyze_batch(target_device_contexts)

    if not records:
        expanded_device_contexts = _expand_device_contexts_to_recordings_hospital_scope(
            recordings_context,
            target_device_contexts,
        )
        if expanded_device_contexts:
            used_expanded_scope = True
            _analyze_batch(expanded_device_contexts[: max(1, min(50, cs.LOG_ANALYSIS_MAX_DEVICES * 4))])

    result_text = _render_file_candidate_result(
        barcode=barcode,
        log_date=log_date,
        all_device_contexts=all_device_contexts,
        records=records,
        used_expanded_scope=used_expanded_scope,
        logs_found_any=logs_found_any,
    )
    payload = {
        "route": "device_file_candidate_lookup",
        "source": "box_db+s3",
        "request": {
            "barcode": barcode,
            "date": log_date,
            "usedExpandedScope": used_expanded_scope,
        },
        "summary": {
            "recordCount": len(records),
            "logsFound": logs_found_any,
            "deviceCount": len(all_device_contexts),
        },
        "records": records,
    }
    return result_text, payload


def _build_device_file_scope_request_message(barcode: str, reason: str) -> str:
    base = _build_phase2_scope_request_message(
        barcode,
        reason,
        "*파일 확인 대상 세션 조회 결과*",
    )
    return base.replace("로그 분석`", "파일 있나`")
