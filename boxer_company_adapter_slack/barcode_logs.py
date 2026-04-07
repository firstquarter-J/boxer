import logging
import re
from typing import Any, Callable

from boxer_adapter_slack.context import _load_slack_thread_context
from boxer.core import settings as s
from boxer.core.llm import _check_ollama_health
from boxer.retrieval.synthesis import _synthesize_retrieval_answer
from boxer_company import settings as cs
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.routers.barcode_log import _is_normal_video_status
from boxer_company.routers.recording_failure_analysis import (
    _build_cause_line,
    _classify_record,
    _get_top_error_group,
)
from boxer_company_adapter_slack.notion_freeform import _append_notion_playbook_section

BarcodeLogReplyFn = Callable[[str], None]
AttachNotionPlaybooksFn = Callable[[dict[str, Any] | None], list[dict[str, Any]]]
ClaudeAllowedFn = Callable[[str | None], bool]
TimeoutErrorFn = Callable[[BaseException], bool]


def _split_barcode_log_reply(reply_text: str, max_chars: int = 3000) -> list[str]:
    text = (reply_text or "").strip()
    if not text:
        return []
    if len(text) <= max_chars:
        return [text]

    def _extract_blocks(raw_text: str) -> list[str]:
        blocks: list[str] = []
        lines = raw_text.splitlines()
        index = 0
        while index < len(lines):
            while index < len(lines) and not lines[index].strip():
                index += 1
            if index >= len(lines):
                break

            if lines[index].strip() == "```":
                code_lines = [lines[index]]
                index += 1
                while index < len(lines):
                    code_lines.append(lines[index])
                    if lines[index].strip() == "```":
                        index += 1
                        break
                    index += 1
                blocks.append("\n".join(code_lines).strip())
                continue

            paragraph: list[str] = []
            while index < len(lines) and lines[index].strip() and lines[index].strip() != "```":
                paragraph.append(lines[index])
                index += 1
            blocks.append("\n".join(paragraph).strip())
        return [block for block in blocks if block]

    def _continuation_prefix(prefix: str) -> str:
        if "• error 라인:" in prefix:
            return "• error 라인 (계속)"
        if "• scanned 이벤트:" in prefix:
            return "• scanned 이벤트 (계속)"
        return ""

    def _split_lines_block(block: str, limit: int) -> list[str]:
        rows = block.splitlines()
        chunks: list[str] = []
        current_rows: list[str] = []
        for row in rows:
            candidate_rows = current_rows + [row]
            candidate = "\n".join(candidate_rows).strip()
            if current_rows and len(candidate) > limit:
                chunks.append("\n".join(current_rows).strip())
                current_rows = [row]
                continue
            current_rows = candidate_rows
        if current_rows:
            chunks.append("\n".join(current_rows).strip())
        return [chunk for chunk in chunks if chunk]

    def _render_fenced_chunk(prefix: str, code_lines: list[str]) -> str:
        fenced = "```\n" + "\n".join(code_lines) + "\n```"
        if prefix:
            return f"{prefix}\n\n{fenced}".strip()
        return fenced

    def _split_block(block: str, limit: int) -> list[str]:
        if len(block) <= limit:
            return [block]

        first_fence_index = block.find("```")
        last_fence_index = block.rfind("```")
        if first_fence_index != -1 and last_fence_index > first_fence_index:
            prefix = block[:first_fence_index].strip()
            code_body = block[first_fence_index + 3 : last_fence_index].strip("\n")
            code_lines = code_body.splitlines()
            if not code_lines:
                return [block]

            chunks: list[str] = []
            current_lines: list[str] = []
            current_prefix = prefix
            continuation = _continuation_prefix(prefix)

            for line in code_lines:
                candidate = _render_fenced_chunk(current_prefix, current_lines + [line])
                if current_lines and len(candidate) > limit:
                    chunks.append(_render_fenced_chunk(current_prefix, current_lines))
                    current_lines = [line]
                    current_prefix = continuation
                    continue
                current_lines.append(line)

            if current_lines:
                chunks.append(_render_fenced_chunk(current_prefix, current_lines))
            return chunks

        return _split_lines_block(block, limit)

    blocks = _extract_blocks(text)
    merged_blocks: list[str] = []
    index = 0
    while index < len(blocks):
        block = blocks[index]
        if index + 1 < len(blocks) and blocks[index + 1].startswith("```"):
            if "• scanned 이벤트:" in block or "• error 라인:" in block:
                merged_blocks.append(f"{block}\n\n{blocks[index + 1]}")
                index += 2
                continue
        merged_blocks.append(block)
        index += 1

    chunks: list[str] = []
    current = ""
    for block in merged_blocks:
        for piece in _split_block(block, max_chars):
            if not current:
                current = piece
                continue
            candidate = f"{current}\n\n{piece}"
            if len(candidate) <= max_chars:
                current = candidate
                continue
            chunks.append(current)
            current = piece

    if current:
        chunks.append(current)
    return chunks


def _contains_ymd(text_value: str) -> bool:
    return bool(re.search(r"\b\d{4}-\d{2}-\d{2}\b", text_value or ""))


def _needs_barcode_log_fallback(
    synthesized: str,
    fallback_text: str,
    route_name: str,
) -> bool:
    if route_name != "barcode log analysis":
        return False

    normalized_synth = synthesized or ""
    normalized_fallback = fallback_text or ""
    required_labels = ("매핑 장비", "병원", "병실")
    required_bullets = ("• 바코드:", "• 날짜:", "• 매핑 장비:")

    if (
        normalized_fallback.startswith("*바코드 로그")
        and not normalized_synth.startswith("*바코드 로그")
    ) or (
        normalized_fallback.startswith("*로그 분석 결과")
        and not normalized_synth.startswith("*로그 분석 결과")
    ):
        return True

    for bullet in required_bullets:
        if bullet in normalized_fallback and bullet not in normalized_synth:
            return True

    for label in required_labels:
        if label in normalized_fallback and label not in normalized_synth:
            return True

    if ("날짜" in normalized_fallback or _contains_ymd(normalized_fallback)) and (
        "날짜" not in normalized_synth and not _contains_ymd(normalized_synth)
    ):
        return True

    if "scanned 이벤트" in normalized_fallback:
        has_scan_lines = bool(re.search(r"\b\d{1,2}:\d{2}:\d{2}\b", normalized_synth))
        if "scanned 이벤트" not in normalized_synth and not has_scan_lines:
            return True

    return False


def _iter_barcode_log_error_summary_sessions(summary_payload: dict[str, Any]) -> list[dict[str, Any]]:
    request = summary_payload.get("request") if isinstance(summary_payload, dict) else {}
    records = summary_payload.get("records") if isinstance(summary_payload, dict) else []
    barcode = str((request or {}).get("barcode") or "미확인").strip() or "미확인"
    session_entries: list[dict[str, Any]] = []
    if not isinstance(records, list):
        return session_entries

    for record in records:
        if not isinstance(record, dict):
            continue
        session_details = record.get("sessionDetails")
        if not isinstance(session_details, list):
            continue
        for detail in session_details:
            if not isinstance(detail, dict):
                continue
            session_entries.append(
                {
                    "barcode": barcode,
                    "deviceName": str(record.get("deviceName") or "미확인").strip() or "미확인",
                    "hospitalName": str(record.get("hospitalName") or "미확인").strip() or "미확인",
                    "roomName": str(record.get("roomName") or "미확인").strip() or "미확인",
                    "date": str(record.get("date") or (request or {}).get("date") or "미확인").strip() or "미확인",
                    "recordingsOnDateCount": int(record.get("recordingsOnDateCount") or 0),
                    "deviceSessionCount": int((record.get("sessions") or {}).get("sessionCount") or 0),
                    "detail": detail,
                }
            )
    return session_entries


def _is_interesting_barcode_log_error_session(session_entry: dict[str, Any]) -> bool:
    detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
    if not isinstance(detail, dict):
        return False
    video_status = str(detail.get("videoStatus") or detail.get("recordingResult") or "").strip()
    return (
        bool(detail.get("restartDetected"))
        or not bool(detail.get("normalClosed"))
        or int(detail.get("errorLineCount") or 0) > 0
        or (video_status and not _is_normal_video_status(video_status))
    )


def _build_barcode_log_error_session_record(session_entry: dict[str, Any]) -> dict[str, Any]:
    detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
    if not isinstance(detail, dict):
        return {}

    session_recordings_count = int(
        detail.get("sessionRecordingsCount")
        or session_entry.get("sessionRecordingsCount")
        or session_entry.get("recordingsOnDateCount")
        or 0
    )
    normal_closed = bool(detail.get("normalClosed"))
    session_diagnostic = (
        detail.get("sessionDiagnostic") if isinstance(detail.get("sessionDiagnostic"), dict) else {}
    )
    record = {
        "deviceName": session_entry.get("deviceName"),
        "hospitalName": session_entry.get("hospitalName"),
        "roomName": session_entry.get("roomName"),
        "date": session_entry.get("date"),
        "recordingsOnDateCount": session_recordings_count,
        "sessions": {
            "sessionCount": 1,
            "normalCount": 1 if normal_closed else 0,
            "abnormalCount": 0 if normal_closed else 1,
        },
        "restartDetected": bool(detail.get("restartDetected")),
        "errorLineCount": int(detail.get("errorLineCount") or 0),
        "errorGroups": [
            group
            for group in (detail.get("errorGroups") or [])
            if isinstance(group, dict)
        ],
        "firstFfmpegError": (
            detail.get("firstFfmpegError") if isinstance(detail.get("firstFfmpegError"), dict) else {}
        ),
        "preRecordingStopDetected": bool(detail.get("preRecordingStopDetected")),
        "sessionDiagnostics": [session_diagnostic] if session_diagnostic else [],
    }
    record["classificationTags"] = _classify_record(record)
    return record


def _build_barcode_log_error_session_section(session_entry: dict[str, Any]) -> list[str]:
    detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
    if not isinstance(detail, dict):
        return []

    barcode = str(session_entry.get("barcode") or "미확인").strip() or "미확인"
    hospital_name = str(session_entry.get("hospitalName") or "미확인").strip() or "미확인"
    room_name = str(session_entry.get("roomName") or "미확인").strip() or "미확인"
    date_label = str(session_entry.get("date") or "미확인").strip() or "미확인"
    session_recordings_count = int(
        detail.get("sessionRecordingsCount")
        or session_entry.get("sessionRecordingsCount")
        or session_entry.get("recordingsOnDateCount")
        or 0
    )
    start_time = str(detail.get("startTime") or "시간미상").strip() or "시간미상"
    stop_time = str(detail.get("stopTime") or "미확인").strip() or "미확인"
    normal_closed = bool(detail.get("normalClosed"))
    restart_detected = bool(detail.get("restartDetected"))
    termination_status = str(
        detail.get("terminationStatus")
        or ("정상 종료" if bool(detail.get("normalClosed")) else "비정상 종료")
    ).strip() or "미확인"
    recording_result = str(detail.get("videoStatus") or detail.get("recordingResult") or "추가 확인 필요").strip() or "추가 확인 필요"
    session_record = _build_barcode_log_error_session_record(session_entry)
    tags = set(session_record.get("classificationTags") or [])
    error_line_count = int(session_record.get("errorLineCount") or 0)
    error_groups = session_record.get("errorGroups") if isinstance(session_record.get("errorGroups"), list) else []
    top_group = _get_top_error_group(session_record)
    top_component = str(top_group.get("component") or "미확인").strip() or "미확인"
    top_signature = str(top_group.get("signature") or "미확인").strip() or "미확인"
    top_count = int(top_group.get("count") or 0)
    first_ffmpeg_error = (
        session_record.get("firstFfmpegError")
        if isinstance(session_record.get("firstFfmpegError"), dict)
        else {}
    )
    ffmpeg_time = str(first_ffmpeg_error.get("timeLabel") or "").strip()
    session_diagnostic = detail.get("sessionDiagnostic") if isinstance(detail.get("sessionDiagnostic"), dict) else {}
    diagnostic_severity = str(session_diagnostic.get("severity") or "").strip()
    pre_recording_stop_detected = bool(detail.get("preRecordingStopDetected"))
    pre_recording_stop_label = (
        str(detail.get("preRecordingStopLabel") or "모션 감지 단계에서 종료 스캔").strip()
        or "모션 감지 단계에서 종료 스캔"
    )

    first_ffmpeg_text = " ".join(
        str(first_ffmpeg_error.get(key) or "").strip().lower()
        for key in ("message", "raw")
    )
    is_ffmpeg_error = "ffmpeg_error" in tags
    is_standby_ffmpeg_error = "standby error" in first_ffmpeg_text or any(
        "standby error" in str(group.get("signature") or "").strip().lower()
        for group in error_groups
        if isinstance(group, dict)
    )
    is_ffmpeg_timestamp_error = "ffmpeg_timestamp_error" in tags
    is_recording_stalled = "recording_stalled" in tags
    all_network_side_effect_errors = "status_network_error" in tags
    router_cause_hint = _build_cause_line(session_record)

    if restart_detected:
        cause_line = "• 핵심 원인: 세션 중 장비 재시작이 확인돼 정상 녹화 실패로 판단해"
        impact_line = "• 영향: 세션 중 장비 재시작으로 정상 녹화 실패가 발생한 것으로 봐야 해"
    elif pre_recording_stop_detected:
        if "device_busy" in tags:
            cause_line = (
                f"• 핵심 원인: {pre_recording_stop_label}돼 녹화 취소로 끝났고, "
                "직전 `/dev/video0` 점유 오류가 있어 녹화 전환이 막힌 정황이야"
            )
        elif is_ffmpeg_error:
            cause_line = (
                f"• 핵심 원인: {pre_recording_stop_label}돼 녹화 취소로 끝났고, "
                "세션 초반 ffmpeg 오류로 본 녹화 전환이 안 된 정황이야"
            )
        else:
            cause_line = f"• 핵심 원인: {pre_recording_stop_label}돼 녹화 취소로 끝났고 실녹화가 시작되지 않았어"
        impact_line = "• 영향: 종료 스캔은 있었지만 본 녹화 시작 전이라 정상 녹화 실패로 봐야 해"
    elif not normal_closed:
        cause_line = "• 핵심 원인: 종료 스캔이 없어 세션이 비정상 종료됐어"
        impact_line = "• 영향: 종료 처리가 끝나지 않아 정상 녹화 실패로 봐야 해"
    elif session_recordings_count <= 0 and (is_ffmpeg_error or is_recording_stalled or diagnostic_severity == "high"):
        if is_recording_stalled and is_ffmpeg_error:
            cause_line = "• 핵심 원인: 녹화 중 파일 증가율 저하(stall)와 ffmpeg 종료가 함께 확인됐고 세션 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 캡처보드 연결 불량을 우선 의심해"
        elif is_recording_stalled:
            cause_line = "• 핵심 원인: 녹화 중 파일 증가율 저하(stall)가 반복됐고 세션 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 캡처보드 연결 불량을 우선 의심해"
        else:
            cause_line = f"• 핵심 원인: {router_cause_hint}"
        impact_line = f"• 영향: 세션 기준 DB 영상 기록이 `{session_recordings_count}개`라 녹화 파일 저장/업로드가 실패한 상태야"
    elif all_network_side_effect_errors and normal_closed and diagnostic_severity != "high":
        if session_recordings_count > 0:
            cause_line = "• 핵심 원인: JWT 갱신/상태 전송/업로드 통신 오류가 있었지만 녹화 실패 원인이라기보다 네트워크/DNS 통신 이상으로 봐야 해"
            impact_line = f"• 영향: 세션 기준 DB 영상 기록 `{session_recordings_count}개`가 있어 녹화는 성공했고 통신 오류는 별도야"
        else:
            cause_line = "• 핵심 원인: 업로드/상태 전송 통신 오류가 반복됐고 세션 기준 DB 영상 기록이 없어 업로드 실패 가능성이 있어"
            impact_line = "• 영향: 녹화 흐름은 종료됐지만 업로드/상태 전송 단계 실패 가능성이 있어"
    elif diagnostic_severity == "high":
        cause_line = "• 핵심 원인: 종료 처리 지연과 종료 후 장치 오류가 이어져 실제 영상 손상 가능성이 높아"
        impact_line = f"• 영향: 종료는 됐지만 `{recording_result}` 상태로 봐야 해"
    elif is_standby_ffmpeg_error and normal_closed:
        cause_line = "• 핵심 원인: standby ffmpeg 오류가 확인돼 영상 손상 가능성을 의심해야 하고 캡처보드 이상을 우선 점검해야 해"
        impact_line = f"• 영향: 종료는 정상이어도 `{recording_result}` 상태로 봐야 해"
    elif is_ffmpeg_timestamp_error:
        cause_line = "• 핵심 원인: ffmpeg DTS/PTS 타임스탬프 이상이 확인돼 캡처보드 연결 불량 또는 캡처보드 고장을 우선 의심해"
        impact_line = f"• 영향: 종료는 됐지만 `{recording_result}` 상태로 봐야 해"
    elif top_signature != "미확인" and top_count >= 2:
        cause_line = f"• 핵심 원인: `{top_component}` 오류가 반복돼 원인 점검이 필요해"
        impact_line = f"• 영향: error 라인 `{error_line_count}줄`이 확인됐고 `{recording_result}` 상태야"
    elif top_signature != "미확인" and top_count == 1:
        cause_line = f"• 핵심 원인: `{top_component}` 오류가 1회 확인돼 영향 여부 점검이 필요해"
        impact_line = f"• 영향: 종료 상태는 `{termination_status}`인데 영상 상태는 `{recording_result}`야"
    else:
        cause_line = "• 핵심 원인: 운영 근거상 추가 확인이 필요해"
        impact_line = f"• 영향: 현재 판정은 `{recording_result}`이야"

    action_lines: list[str] = []
    if restart_detected:
        action_lines.append("전원 차단/전원 버튼 오입력 여부 확인")
    if pre_recording_stop_detected:
        action_lines.append("종료 스캔 시점과 녹화 취소 안내 여부 확인")
    if is_recording_stalled or is_ffmpeg_timestamp_error or is_standby_ffmpeg_error or is_ffmpeg_error:
        action_lines.append("캡처보드 연결 상태와 입력 신호 점검")
    if is_recording_stalled:
        action_lines.append("저장 경로 쓰기 상태와 파일 증가율 저하 원인 확인")
    if top_signature != "미확인":
        action_lines.append(f"{top_component} 관련 장치/프로세스 상태 확인")
    if not action_lines:
        action_lines.append("동일 시각 장비 상태와 관련 프로세스 로그 확인")

    time_label = f"{start_time} ~ {stop_time}" if stop_time != "미확인" else start_time
    if ffmpeg_time:
        time_label = f"{time_label} (첫 ffmpeg 오류 {ffmpeg_time})"
    lines = [
        f"• 바코드: `{barcode}` | 병원: `{hospital_name}` | 병실: `{room_name}` | 날짜: `{date_label}` | 시간: `{time_label}`",
        cause_line,
        impact_line,
    ]
    lines.append(f"• 조치: {' / '.join(action_lines[:3])}")
    return lines


def _build_barcode_log_error_summary_session_payload(
    summary_payload: dict[str, Any],
    session_entry: dict[str, Any],
) -> dict[str, Any]:
    request = summary_payload.get("request") if isinstance(summary_payload, dict) else {}
    detail = session_entry.get("detail") if isinstance(session_entry, dict) else {}
    if not isinstance(request, dict) or not isinstance(detail, dict):
        return {}

    session_record = _build_barcode_log_error_session_record(session_entry)
    error_groups = session_record.get("errorGroups") if isinstance(session_record.get("errorGroups"), list) else []
    session_diagnostic = (
        detail.get("sessionDiagnostic") if isinstance(detail.get("sessionDiagnostic"), dict) else {}
    )
    representative_error_group = _get_top_error_group(session_record)
    time_range = str(detail.get("startTime") or "시간미상").strip() or "시간미상"
    stop_time = str(detail.get("stopTime") or "미확인").strip() or "미확인"
    if stop_time != "미확인":
        time_range = f"{time_range} ~ {stop_time}"

    return {
        "route": "barcode_log_error_summary_session",
        "source": summary_payload.get("source"),
        "request": {
            "mode": request.get("mode"),
            "barcode": request.get("barcode"),
            "date": session_entry.get("date"),
        },
        "session": {
            "barcode": session_entry.get("barcode"),
            "deviceName": session_entry.get("deviceName"),
            "hospitalName": session_entry.get("hospitalName"),
            "roomName": session_entry.get("roomName"),
            "date": session_entry.get("date"),
            "time": time_range,
            "sessionIndex": detail.get("index"),
            "stopToken": detail.get("stopToken"),
            "normalClosed": detail.get("normalClosed"),
            "restartDetected": detail.get("restartDetected"),
            "terminationStatus": detail.get("terminationStatus"),
            "videoStatus": detail.get("videoStatus"),
            "recordingResult": detail.get("recordingResult"),
            "recordingsOnDateCount": session_entry.get("recordingsOnDateCount"),
            "errorLineCount": detail.get("errorLineCount"),
            "firstFfmpegError": detail.get("firstFfmpegError"),
            "preRecordingStopDetected": detail.get("preRecordingStopDetected"),
            "preRecordingStopLabel": detail.get("preRecordingStopLabel"),
            "classificationTags": session_record.get("classificationTags") or [],
            "routerCauseHint": _build_cause_line(session_record),
            "representativeErrorGroup": {
                "component": representative_error_group.get("component"),
                "signature": representative_error_group.get("signature"),
                "count": representative_error_group.get("count"),
                "sampleTime": representative_error_group.get("sampleTime"),
                "sampleMessage": representative_error_group.get("sampleMessage"),
            },
            "errorGroups": [
                {
                    "component": group.get("component"),
                    "signature": group.get("signature"),
                    "count": group.get("count"),
                    "sampleTime": group.get("sampleTime"),
                    "sampleMessage": group.get("sampleMessage"),
                }
                for group in error_groups[:6]
                if isinstance(group, dict)
            ],
            "sessionDiagnostic": {
                "severity": session_diagnostic.get("severity"),
                "finishDelay": session_diagnostic.get("finishDelay"),
                "postStopScanCount": session_diagnostic.get("postStopScanCount"),
                "postStopStopCount": session_diagnostic.get("postStopStopCount"),
                "postStopSnapCount": session_diagnostic.get("postStopSnapCount"),
                "postStopDeviceErrorCount": session_diagnostic.get("postStopDeviceErrorCount"),
                "displayText": session_diagnostic.get("displayText"),
            },
        },
    }


def _build_barcode_log_error_summary_fallback(summary_payload: dict[str, Any]) -> str:
    summary = summary_payload.get("summary") if isinstance(summary_payload, dict) else None
    if not isinstance(summary, dict):
        return ""

    session_entries = _iter_barcode_log_error_summary_sessions(summary_payload)
    interesting_entries = [entry for entry in session_entries if _is_interesting_barcode_log_error_session(entry)]
    if not interesting_entries:
        interesting_entries = session_entries
    if not interesting_entries:
        return ""

    lines = ["*세션별 에러 분석*"]
    for session_entry in interesting_entries:
        section_lines = _build_barcode_log_error_session_section(session_entry)
        if not section_lines:
            continue
        lines.append("")
        lines.extend(section_lines)
    return "\n".join(lines).strip()


def _is_bad_barcode_log_error_summary_session(text: str) -> bool:
    normalized = (text or "").strip()
    if not normalized:
        return True

    required_markers = ("• 바코드:", "• 핵심 원인:", "• 영향:", "• 조치:")
    if any(marker not in normalized for marker in required_markers):
        return True

    lowered = normalized.lower()
    bad_patterns = (
        "</think>",
        "<think>",
        "let me",
        "wait,",
        "wait ",
        "i should",
        "the error",
        "the user",
        "now,",
        "now ",
        "therefore",
        "looking at",
        "based on",
        "i need",
        "check if",
    )
    if any(pattern in lowered for pattern in bad_patterns):
        return True

    return False


def _needs_barcode_log_error_summary_session_fallback(
    synthesized: str,
    session_payload: dict[str, Any],
) -> bool:
    if _is_bad_barcode_log_error_summary_session(synthesized):
        return True

    session = session_payload.get("session") if isinstance(session_payload, dict) else {}
    if not isinstance(session, dict):
        return False

    tags = {
        str(tag).strip()
        for tag in (session.get("classificationTags") or [])
        if str(tag).strip()
    }
    recordings_on_date_count = int(session.get("recordingsOnDateCount") or 0)
    pre_recording_stop_detected = bool(session.get("preRecordingStopDetected"))
    normalized = (synthesized or "").strip()
    lowered = normalized.lower()

    if pre_recording_stop_detected and not any(token in normalized for token in ("녹화 취소", "실녹화", "본 녹화 시작 전", "모션 감지 단계")):
        return True

    if recordings_on_date_count <= 0 and tags.intersection({"ffmpeg_error", "ffmpeg_sigterm", "recording_stalled"}):
        if "녹화 & 업로드 실패" not in normalized:
            return True
        if not any(token in normalized for token in ("ffmpeg", "SIGTERM", "sigterm", "stall", "캡처보드", "영상 입력")):
            return True
        if "recording_stalled" in tags and "캡처보드" not in normalized:
            return True

    representative = session.get("representativeErrorGroup")
    if isinstance(representative, dict):
        representative_text = " ".join(
            str(representative.get(key) or "").strip().lower()
            for key in ("component", "signature")
        )
        if any(token in representative_text for token in ("ffmpeg", "sigterm", "recording may be stalled", "stalled")):
            if "app 오류" in normalized and not any(
                token in lowered for token in ("ffmpeg", "sigterm", "stall")
            ):
                return True

    return False


def _collect_interesting_barcode_log_error_sessions(summary_payload: dict[str, Any]) -> list[dict[str, Any]]:
    session_entries = _iter_barcode_log_error_summary_sessions(summary_payload)
    interesting_entries = [entry for entry in session_entries if _is_interesting_barcode_log_error_session(entry)]
    return interesting_entries or session_entries


def _render_barcode_log_error_summary_sections(
    summary_payload: dict[str, Any],
    session_entries: list[dict[str, Any]],
    *,
    attach_notion_playbooks_to_evidence: AttachNotionPlaybooksFn,
) -> list[str]:
    sections: list[str] = []
    for session_entry in session_entries:
        session_payload = _build_barcode_log_error_summary_session_payload(summary_payload, session_entry)
        if not session_payload:
            continue
        fallback_section = "\n".join(_build_barcode_log_error_session_section(session_entry)).strip()
        if not fallback_section:
            continue
        session_playbooks = attach_notion_playbooks_to_evidence(session_payload)
        sections.append(_append_notion_playbook_section(fallback_section, session_playbooks))
    return sections


def _compose_barcode_log_error_summary_text(fallback_text: str, rendered_sections: list[str]) -> str:
    if rendered_sections:
        return "*세션별 에러 분석*\n\n" + "\n\n".join(rendered_sections)
    return fallback_text.strip() or "*세션별 에러 분석*"


def _reply_with_barcode_log_error_summary(
    summary_payload: dict[str, Any] | None,
    *,
    question: str,
    reply: BarcodeLogReplyFn,
    logger: logging.Logger,
    thread_ts: str,
    user_id: str | None,
    claude_client: Any,
    client: Any,
    channel_id: str,
    current_ts: str,
    is_claude_allowed_user: ClaudeAllowedFn,
    is_timeout_error: TimeoutErrorFn,
    attach_notion_playbooks_to_evidence: AttachNotionPlaybooksFn,
) -> None:
    if not isinstance(summary_payload, dict):
        return

    summary = summary_payload.get("summary")
    if not isinstance(summary, dict):
        return

    error_line_count = int(summary.get("errorLineCount") or 0)
    abnormal_session_count = int(summary.get("abnormalSessionCount") or 0)
    restart_event_count = int(summary.get("restartEventCount") or 0)
    if error_line_count <= 0 and abnormal_session_count <= 0 and restart_event_count <= 0:
        return

    interesting_entries = _collect_interesting_barcode_log_error_sessions(summary_payload)
    if not interesting_entries:
        return

    fallback_text = _build_barcode_log_error_summary_fallback(summary_payload)

    def _build_rendered_fallback_sections() -> list[str]:
        return _render_barcode_log_error_summary_sections(
            summary_payload,
            interesting_entries,
            attach_notion_playbooks_to_evidence=attach_notion_playbooks_to_evidence,
        )

    provider = (s.LLM_PROVIDER or "").lower().strip()
    if not s.LLM_SYNTHESIS_ENABLED or not question:
        reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
        logger.info("Responded with barcode log error summary (direct)")
        return
    if provider not in {"claude", "ollama"}:
        reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
        logger.info(
            "Responded with barcode log error summary (direct, unsupported provider=%s)",
            provider,
        )
        return
    if provider == "ollama":
        health = _check_ollama_health()
        if not health["ok"]:
            reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
            logger.warning(
                "Responded with barcode log error summary (direct, ollama unavailable=%s)",
                health["summary"],
            )
            return
    if provider == "claude":
        if claude_client is None:
            reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
            logger.info("Responded with barcode log error summary (direct, claude client unavailable)")
            return
        if not is_claude_allowed_user(user_id):
            reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
            logger.info(
                "Responded with barcode log error summary (direct, claude synthesis not allowed for user=%s)",
                user_id,
            )
            return

    try:
        thread_context = ""
        if s.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT:
            thread_context = _load_slack_thread_context(
                client,
                logger,
                channel_id,
                thread_ts,
                current_ts,
            )
        rendered_sections: list[str] = []
        for session_entry in interesting_entries:
            session_payload = _build_barcode_log_error_summary_session_payload(summary_payload, session_entry)
            if not session_payload:
                continue
            fallback_section = "\n".join(_build_barcode_log_error_session_section(session_entry)).strip()
            if not fallback_section:
                continue
            session_playbooks = attach_notion_playbooks_to_evidence(session_payload)
            fallback_section = _append_notion_playbook_section(fallback_section, session_playbooks)
            synthesized_text = _synthesize_retrieval_answer(
                question=question,
                thread_context=thread_context,
                evidence_payload=session_payload,
                provider=provider,
                claude_client=claude_client,
                system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                extra_rules=_build_company_retrieval_rules(session_payload),
                evidence_transform=_transform_company_retrieval_payload,
                max_tokens=cs.BARCODE_LOG_ERROR_SUMMARY_MAX_TOKENS,
            )
            final_section = synthesized_text or fallback_section
            if _needs_barcode_log_error_summary_session_fallback(final_section, session_payload):
                final_section = fallback_section
            final_section = _append_notion_playbook_section(final_section, session_playbooks)
            rendered_sections.append(final_section)

        reply(_compose_barcode_log_error_summary_text(fallback_text, rendered_sections).strip())
        logger.info(
            "Responded with barcode log error summary (%s sections) in thread_ts=%s",
            len(rendered_sections),
            thread_ts,
        )
    except TimeoutError:
        logger.warning("Barcode log error summary timeout")
        reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
    except RuntimeError as exc:
        if is_timeout_error(exc):
            logger.warning("Barcode log error summary timeout")
            reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
            return
        logger.exception("Barcode log error summary synthesis failed")
        reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))
    except Exception:
        logger.exception("Barcode log error summary synthesis failed")
        reply(_compose_barcode_log_error_summary_text(fallback_text, _build_rendered_fallback_sections()))


__all__ = [
    "_needs_barcode_log_fallback",
    "_reply_with_barcode_log_error_summary",
    "_split_barcode_log_reply",
]
