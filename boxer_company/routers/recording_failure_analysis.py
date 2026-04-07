from __future__ import annotations

from copy import deepcopy
import re
from pathlib import Path
from typing import Any

from boxer_company import settings as cs
from boxer.core.utils import _display_value, _truncate_text

_FAILURE_ANALYSIS_HINTS = (
    "녹화 실패",
    "실패 원인",
    "원인 분석",
    "왜 실패",
    "왜 안 됐",
    "왜 깨졌",
    "영상 손상",
    "손상 원인",
    "업로드 실패",
    "정상 녹화 안",
    "정상 녹화 실패",
)

_CODE_HINTS = (
    "마미박스",
    "mommybox",
    "박스",
    "legacy",
    "2.11.300",
    "코드",
    "버전",
    "모듈",
    "recorder",
)

_NETWORK_ERROR_HINTS = (
    "couldn't renew jwt",
    "send status: failed",
    "sendcurrentframesnapbase64",
    "sendscreenshotbase64",
    "senddailylog",
    "getaddrinfo eai_again",
    "status.kr.mmtalkbox.com",
    "stream.kr.mmtalkbox.com",
    "couldn't be sent",
    "throttling:",
)

_SNAPSHOT_PATHS = {
    "v2.11.300": lambda: cs.MOMMYBOX_REF_V211300_PATH,
    "legacy": lambda: cs.MOMMYBOX_REF_LEGACY_PATH,
}

_LEGACY_DEVICE_HINTS = (
    "구버전 장비",
    "legacy",
)

_SESSION_LAST_HINTS = ("마지막 세션", "최근 세션", "최신 세션")
_SESSION_FIRST_HINTS = ("첫 세션", "첫번째 세션", "첫 번째 세션", "세션 1", "1번째 세션", "1번 세션")
_SESSION_INDEX_PATTERNS = (
    re.compile(r"세션\s*([1-9]\d*)"),
    re.compile(r"([1-9]\d*)\s*번째\s*세션"),
    re.compile(r"([1-9]\d*)\s*번\s*세션"),
)

_TAG_CODE_TARGETS: dict[str, list[dict[str, Any]]] = {
    "ffmpeg_error": [
        {
            "ref": "v2.11.300",
            "path": "lib/Recorder/index.js",
            "reason": "Recorder의 녹화 시작/종료와 ffmpeg 실행 흐름",
            "patterns": ["Started recording", "Spawned RECORDING ffmpeg", "finishRecording"],
        },
        {
            "ref": "v2.11.300",
            "path": "lib/Recorder/MediaProcessor.js",
            "reason": "녹화 모니터링과 RecordingMonitor 연동 흐름",
            "patterns": ["RecordingMonitor", "startRecordingMonitoring", "stopRecordingMonitoring"],
        },
        {
            "ref": "legacy",
            "path": "lib/Recorder/index.js",
            "reason": "legacy Recorder의 ffmpeg 실행/종료 흐름",
            "patterns": ["Started recording", "Spawned RECORDING ffmpeg", "finishRecording"],
        },
    ],
    "ffmpeg_timestamp_error": [
        {
            "ref": "v2.11.300",
            "path": "lib/Recorder/RecordingMonitor.js",
            "reason": "파일 증가율 저하와 녹화 이상 감지 로직",
            "patterns": ["stalled", "Low growth rate", "critical"],
        },
        {
            "ref": "legacy",
            "path": "lib/Recorder/index.js",
            "reason": "legacy 녹화 흐름에서 ffmpeg 실행과 파일 생성 연결부",
            "patterns": ["Spawned RECORDING ffmpeg", "Started recording"],
        },
    ],
    "restart_detected": [
        {
            "ref": "v2.11.300",
            "path": "app.js",
            "reason": "녹화 종료/마감 처리 흐름",
            "patterns": ["finishRecording", "recording_stopped", "stopRecording"],
        },
        {
            "ref": "legacy",
            "path": "app.js",
            "reason": "legacy finishRecording 처리 흐름",
            "patterns": ["finishRecording", "recording_stopped", "stopRecording"],
        },
    ],
    "db_row_missing": [
        {
            "ref": "v2.11.300",
            "path": "lib/SqliteStorage/index.js",
            "reason": "녹화 row 생성/마감 저장 흐름",
            "patterns": ["addRecording", "finishRecording"],
        },
        {
            "ref": "legacy",
            "path": "lib/LokiStorage/index.js",
            "reason": "legacy 녹화 row 생성/마감 저장 흐름",
            "patterns": ["addRecording", "finishRecording"],
        },
    ],
    "upload_network_error": [
        {
            "ref": "v2.11.300",
            "path": "lib/Uploader/index.js",
            "reason": "업로드 실패/재시도 처리 흐름",
            "patterns": ["throttling", "upload", "send"],
        },
        {
            "ref": "v2.11.300",
            "path": "lib/EndpointClient/index.js",
            "reason": "상태 전송과 스크린샷 전송 흐름",
            "patterns": ["sendScreenShotBase64", "sendDailyLog", "reverseShell"],
        },
        {
            "ref": "legacy",
            "path": "lib/Uploader/index.js",
            "reason": "legacy 업로드 처리 흐름",
            "patterns": ["throttling", "upload", "send"],
        },
        {
            "ref": "legacy",
            "path": "lib/EndpointClient/index.js",
            "reason": "legacy 상태 전송/스크린샷 전송 흐름",
            "patterns": ["sendScreenShotBase64", "reverseShell"],
        },
    ],
}


def _is_recording_failure_analysis_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    return _has_recording_failure_analysis_hints(question)


def _has_recording_failure_analysis_hints(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _FAILURE_ANALYSIS_HINTS)


def _has_code_hints(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _CODE_HINTS)


def _normalize_component(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_signature(value: Any) -> str:
    return str(value or "").strip().lower()


def _iter_error_groups(record: dict[str, Any]) -> list[dict[str, Any]]:
    groups = record.get("errorGroups") if isinstance(record.get("errorGroups"), list) else []
    return [group for group in groups if isinstance(group, dict)]


def _is_ffmpeg_related_group(group: dict[str, Any]) -> bool:
    component = _normalize_component(group.get("component"))
    signature = _normalize_signature(group.get("signature"))
    combined = " ".join(part for part in (component, signature) if part)
    if "ffmpeg" in combined:
        return True
    if component in {"recorder", "recordingmonitor", "mediaprocessor"}:
        return any(
            token in combined
            for token in (
                "startrecording",
                "start recording",
                "generatethumbnail",
                "recording ffmpeg",
                "spawned recording",
                "killed with signal",
                "sigterm",
            )
        )
    return False


def _is_ffmpeg_sigterm_group(group: dict[str, Any]) -> bool:
    component = _normalize_component(group.get("component"))
    signature = _normalize_signature(group.get("signature"))
    combined = " ".join(part for part in (component, signature) if part)
    if not _is_ffmpeg_related_group(group) and component not in {"recorder", "ffmpeg"}:
        return False
    return any(token in combined for token in ("signal sigterm", "killed with signal", "sigterm"))


def _is_ffmpeg_timestamp_group(group: dict[str, Any]) -> bool:
    signature = _normalize_signature(group.get("signature"))
    tokens = ("invalid dropping", "non-monotonous dts", "dts ", "pts ", "timestamp")
    return any(token in signature for token in tokens)


def _is_recording_stall_group(group: dict[str, Any]) -> bool:
    signature = _normalize_signature(group.get("signature"))
    tokens = ("recording may be stalled", "recording critically stalled", "stalled")
    return any(token in signature for token in tokens)


def _is_device_busy_group(group: dict[str, Any]) -> bool:
    signature = _normalize_signature(group.get("signature"))
    tokens = ("device or resource busy", "/dev/video0", "no such file or directory", "videodevice : error")
    return any(token in signature for token in tokens)


def _score_error_group_priority(group: dict[str, Any]) -> int:
    if _is_ffmpeg_sigterm_group(group):
        return 600
    if _is_recording_stall_group(group):
        return 560
    if _is_ffmpeg_timestamp_group(group):
        return 540
    if _is_device_busy_group(group):
        return 520
    if _is_ffmpeg_related_group(group):
        return 500
    if _is_network_side_effect_group(group):
        return 300
    return 100


def _get_top_error_group(record: dict[str, Any]) -> dict[str, Any]:
    groups = _iter_error_groups(record)
    if not groups:
        return {}
    return max(
        groups,
        key=lambda group: (
            _score_error_group_priority(group),
            int(group.get("count") or 0),
            -len(_normalize_component(group.get("component"))),
            -len(_normalize_signature(group.get("signature"))),
        ),
    )


def _is_network_side_effect_group(group: dict[str, Any]) -> bool:
    component = _normalize_component(group.get("component"))
    signature = _normalize_signature(group.get("signature"))
    if component not in {"endpoint", "endpointclient", "uploader"}:
        return False
    return any(token in signature for token in _NETWORK_ERROR_HINTS)


def _record_has_all_network_side_effect_errors(record: dict[str, Any]) -> bool:
    groups = _iter_error_groups(record)
    if not groups:
        return False
    return all(_is_network_side_effect_group(group) for group in groups)


def _record_has_uploader_network_error(record: dict[str, Any]) -> bool:
    for group in _iter_error_groups(record):
        if _normalize_component(group.get("component")) != "uploader":
            continue
        if any(token in _normalize_signature(group.get("signature")) for token in _NETWORK_ERROR_HINTS):
            return True
    return False


def _record_has_ffmpeg_sigterm_error(record: dict[str, Any]) -> bool:
    first_ffmpeg = record.get("firstFfmpegError")
    candidates: list[str] = []
    if isinstance(first_ffmpeg, dict):
        candidates.append(_normalize_signature(first_ffmpeg.get("message")))
        candidates.append(_normalize_signature(first_ffmpeg.get("raw")))
    if any(token in candidate for candidate in candidates for token in ("signal sigterm", "killed with signal", "sigterm")):
        return True
    return any(_is_ffmpeg_sigterm_group(group) for group in _iter_error_groups(record))


def _record_has_ffmpeg_error(record: dict[str, Any]) -> bool:
    first_ffmpeg = record.get("firstFfmpegError")
    if isinstance(first_ffmpeg, dict) and first_ffmpeg:
        return True
    for group in _iter_error_groups(record):
        if _is_ffmpeg_related_group(group):
            return True
    return False


def _record_has_ffmpeg_timestamp_error(record: dict[str, Any]) -> bool:
    candidates: list[str] = []
    first_ffmpeg = record.get("firstFfmpegError")
    if isinstance(first_ffmpeg, dict):
        candidates.append(_normalize_signature(first_ffmpeg.get("message")))
        candidates.append(_normalize_signature(first_ffmpeg.get("raw")))
    for group in _iter_error_groups(record):
        candidates.append(_normalize_signature(group.get("signature")))

    tokens = ("invalid dropping", "non-monotonous dts", "dts ", "pts ", "timestamp")
    return any(any(token in candidate for token in tokens) for candidate in candidates)


def _record_has_recording_stall_error(record: dict[str, Any]) -> bool:
    return any(_is_recording_stall_group(group) for group in _iter_error_groups(record))


def _record_has_device_busy_error(record: dict[str, Any]) -> bool:
    for group in _iter_error_groups(record):
        if _is_device_busy_group(group):
            return True
    return False


def _record_has_high_post_stop_anomaly(record: dict[str, Any]) -> bool:
    diagnostics = record.get("sessionDiagnostics") if isinstance(record.get("sessionDiagnostics"), list) else []
    return any(isinstance(item, dict) and str(item.get("severity") or "") == "high" for item in diagnostics)


def _record_has_suspect_post_stop_anomaly(record: dict[str, Any]) -> bool:
    diagnostics = record.get("sessionDiagnostics") if isinstance(record.get("sessionDiagnostics"), list) else []
    return any(isinstance(item, dict) and str(item.get("severity") or "") == "suspect" for item in diagnostics)


def _classify_record(record: dict[str, Any]) -> list[str]:
    tags: set[str] = set()
    sessions = record.get("sessions") if isinstance(record.get("sessions"), dict) else {}
    recordings_on_date_count = int(record.get("recordingsOnDateCount") or 0)
    canceled_count = int(sessions.get("canceledCount") or 0)
    stop_missing_count = int(sessions.get("stopMissingCount") or 0)

    if bool(record.get("restartDetected")):
        tags.add("restart_detected")
    if canceled_count > 0 or bool(record.get("preRecordingStopDetected")):
        tags.add("pre_recording_stop")
    if stop_missing_count > 0 or (
        int(sessions.get("abnormalCount") or 0) > 0
        and canceled_count <= 0
    ):
        tags.add("stop_missing")
    if recordings_on_date_count <= 0:
        tags.add("db_row_missing")
    else:
        tags.add("db_row_present")

    if _record_has_all_network_side_effect_errors(record):
        tags.add("status_network_error")
        if _record_has_uploader_network_error(record):
            tags.add("upload_network_error")

    if _record_has_ffmpeg_sigterm_error(record):
        tags.add("ffmpeg_sigterm")
    if _record_has_ffmpeg_error(record):
        tags.add("ffmpeg_error")
    if _record_has_ffmpeg_timestamp_error(record):
        tags.add("ffmpeg_timestamp_error")
    if _record_has_recording_stall_error(record):
        tags.add("recording_stalled")
    if _record_has_device_busy_error(record):
        tags.add("device_busy")
    if _record_has_high_post_stop_anomaly(record):
        tags.add("finish_anomaly")
    elif _record_has_suspect_post_stop_anomaly(record):
        tags.add("finish_delay")

    return sorted(tags)


def _extract_code_snippet(file_path: Path, patterns: list[str], *, context: int = 3, max_matches: int = 2) -> str | None:
    try:
        lines = file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return None

    lowered_patterns = [pattern.lower() for pattern in patterns if pattern]
    if not lowered_patterns:
        return None

    matched_indexes: list[int] = []
    for index, line in enumerate(lines):
        lowered = line.lower()
        if any(pattern in lowered for pattern in lowered_patterns):
            matched_indexes.append(index)
            if len(matched_indexes) >= max_matches:
                break

    if not matched_indexes:
        return None

    ranges: list[tuple[int, int]] = []
    for index in matched_indexes:
        start = max(0, index - context)
        end = min(len(lines), index + context + 1)
        if ranges and start <= ranges[-1][1]:
            ranges[-1] = (ranges[-1][0], max(ranges[-1][1], end))
        else:
            ranges.append((start, end))

    rendered: list[str] = []
    for start, end in ranges:
        for line_no in range(start, end):
            rendered.append(f"L{line_no + 1}: {lines[line_no]}")
        rendered.append("...")

    if rendered and rendered[-1] == "...":
        rendered.pop()
    return _truncate_text("\n".join(rendered), 1400)


def _resolve_snapshot_path(ref: str) -> Path | None:
    resolver = _SNAPSHOT_PATHS.get(ref)
    if resolver is None:
        return None
    raw_path = str(resolver() or "").strip()
    if not raw_path:
        return None
    path = Path(raw_path)
    if not path.exists():
        return None
    return path


def _select_code_reference_ref(question: str) -> str:
    text = (question or "").strip()
    lowered = text.lower()
    if any(hint in text or hint in lowered for hint in _LEGACY_DEVICE_HINTS):
        return "legacy"
    return "v2.11.300"


def _build_code_evidence(question: str, tags: list[str]) -> list[dict[str, Any]]:
    include_due_to_question = _has_code_hints(question)
    include_due_to_tags = any(
        tag in tags
        for tag in (
            "restart_detected",
            "ffmpeg_error",
            "ffmpeg_timestamp_error",
            "device_busy",
            "db_row_missing",
            "upload_network_error",
            "finish_anomaly",
            "finish_delay",
        )
    )
    if not include_due_to_question and not include_due_to_tags:
        return []

    selected_ref = _select_code_reference_ref(question)

    items: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for tag in tags:
        for target in _TAG_CODE_TARGETS.get(tag, []):
            ref = str(target.get("ref") or "").strip()
            rel_path = str(target.get("path") or "").strip()
            if not ref or not rel_path:
                continue
            if ref != selected_ref:
                continue
            key = (ref, rel_path)
            if key in seen:
                continue
            base_path = _resolve_snapshot_path(ref)
            if base_path is None:
                continue
            file_path = base_path / rel_path
            if not file_path.exists() or not file_path.is_file():
                continue
            snippet = _extract_code_snippet(file_path, list(target.get("patterns") or []))
            if not snippet:
                continue
            items.append(
                {
                    "ref": ref,
                    "path": rel_path,
                    "reason": str(target.get("reason") or "").strip(),
                    "snippet": snippet,
                }
            )
            seen.add(key)
            if len(items) >= 6:
                return items
    return items


def _build_compact_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "deviceName": _display_value(record.get("deviceName"), default="미확인"),
        "hospitalName": _display_value(record.get("hospitalName"), default="미확인"),
        "roomName": _display_value(record.get("roomName"), default="미확인"),
        "date": _display_value(record.get("date"), default="미확인"),
        "recordingsOnDateCount": int(record.get("recordingsOnDateCount") or 0),
        "recordingsOnDateStatuses": record.get("recordingsOnDateStatuses") or [],
        "sessions": record.get("sessions") or {},
        "restartDetected": bool(record.get("restartDetected")),
        "restartEvents": record.get("restartEvents") or [],
        "firstSessionStartTime": _display_value(record.get("firstSessionStartTime"), default="미확인"),
        "lastSessionStopTime": _display_value(record.get("lastSessionStopTime"), default="미확인"),
        "firstFfmpegError": record.get("firstFfmpegError") or {},
        "sessionDiagnostics": record.get("sessionDiagnostics") or [],
        "errorGroups": (record.get("errorGroups") or [])[:6],
        "scanEventCount": int(record.get("scanEventCount") or 0),
        "errorLineCount": int(record.get("errorLineCount") or 0),
        "sessionDetails": record.get("sessionDetails") or [],
        "classificationTags": _classify_record(record),
    }


def _extract_session_selector(text: str) -> tuple[str, int | None] | None:
    content = (text or "").strip()
    if not content:
        return None
    lowered = content.lower()
    if any(hint in content or hint in lowered for hint in _SESSION_LAST_HINTS):
        return ("last", None)
    if any(hint in content or hint in lowered for hint in _SESSION_FIRST_HINTS):
        return ("index", 1)
    for pattern in _SESSION_INDEX_PATTERNS:
        matched = pattern.search(content)
        if matched:
            return ("index", int(matched.group(1)))
    return None


def _build_session_candidates(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for record_index, record in enumerate(records):
        if not isinstance(record, dict):
            continue
        session_details = record.get("sessionDetails") if isinstance(record.get("sessionDetails"), list) else []
        for session_detail in session_details:
            if not isinstance(session_detail, dict):
                continue
            candidates.append(
                {
                    "recordIndex": record_index,
                    "sessionDetail": session_detail,
                }
            )
    return candidates


def _clone_record_for_session(record: dict[str, Any], session_detail: dict[str, Any]) -> dict[str, Any]:
    cloned = deepcopy(record)
    normal_closed = bool(session_detail.get("normalClosed"))
    diagnostic = session_detail.get("sessionDiagnostic") if isinstance(session_detail.get("sessionDiagnostic"), dict) else {}
    cloned["sessions"] = {
        "sessionCount": 1,
        "normalCount": 1 if normal_closed else 0,
        "abnormalCount": 0 if normal_closed else 1,
    }
    cloned["restartDetected"] = bool(session_detail.get("restartDetected"))
    cloned["restartEvents"] = []
    cloned["firstSessionStartTime"] = _display_value(session_detail.get("startTime"), default="미확인")
    cloned["lastSessionStopTime"] = _display_value(session_detail.get("stopTime"), default="미확인")
    cloned["scanEventCount"] = int(session_detail.get("scanEventCount") or 0)
    cloned["errorLineCount"] = int(session_detail.get("errorLineCount") or 0)
    cloned["errorGroups"] = list(session_detail.get("errorGroups") or [])
    cloned["firstFfmpegError"] = dict(session_detail.get("firstFfmpegError") or {})
    cloned["sessionDiagnostics"] = [diagnostic] if diagnostic else []
    cloned["sessionDetails"] = [deepcopy(session_detail)]
    cloned["classificationTags"] = _classify_record(cloned)
    return cloned


def _build_recording_failure_session_scope_request_message(
    barcode: str,
    request_date: str,
    records: list[dict[str, Any]],
) -> str:
    lines = [
        "*녹화 실패 원인 분석*",
        f"• 바코드: `{_display_value(barcode, default='미확인')}`",
    ]
    if request_date:
        lines.append(f"• 날짜: `{request_date}`")
    lines.append("• 세션이 여러 건이라 어떤 세션을 분석할지 지정해줘")
    lines.append("• 예: `마지막 세션 녹화 실패 원인`, `세션 1 녹화 실패 원인`, `세션 2 원인 분석`")

    candidate_index = 1
    for record in records:
        if not isinstance(record, dict):
            continue
        device_name = _display_value(record.get("deviceName"), default="미확인")
        hospital_name = _display_value(record.get("hospitalName"), default="미확인")
        room_name = _display_value(record.get("roomName"), default="미확인")
        for session_detail in (record.get("sessionDetails") or []):
            if not isinstance(session_detail, dict):
                continue
            start_time = _display_value(session_detail.get("startTime"), default="시간미상")
            stop_time = _display_value(session_detail.get("stopTime"), default="미확인")
            result_text = _display_value(
                session_detail.get("videoStatus") or session_detail.get("recordingResult"),
                default="미확인",
            )
            lines.append(
                f"- 세션 {candidate_index}: `{start_time}` ~ `{stop_time}` / `{device_name}` / `{hospital_name}` `{room_name}` / `{result_text}`"
            )
            candidate_index += 1
            if candidate_index > 4:
                return "\n".join(lines)
    return "\n".join(lines)


def _narrow_recording_failure_analysis_evidence(
    evidence_payload: dict[str, Any],
    selector_text: str,
) -> tuple[dict[str, Any] | None, str | None]:
    records = evidence_payload.get("records") if isinstance(evidence_payload, dict) else []
    if not isinstance(records, list) or not records:
        return evidence_payload, None

    candidates = _build_session_candidates(records)
    if len(candidates) <= 1:
        if len(candidates) == 1:
            selected = candidates[0]
            narrowed = deepcopy(evidence_payload)
            narrowed["records"] = [
                _clone_record_for_session(records[selected["recordIndex"]], selected["sessionDetail"])
            ]
            return narrowed, None
        return evidence_payload, None

    selector = _extract_session_selector(selector_text)
    if selector is None:
        request = evidence_payload.get("request") if isinstance(evidence_payload, dict) else {}
        return None, _build_recording_failure_session_scope_request_message(
            _display_value((request or {}).get("barcode"), default=""),
            _display_value((request or {}).get("date"), default=""),
            records,
        )

    mode, index = selector
    selected_candidate: dict[str, Any] | None = None
    if mode == "last":
        selected_candidate = candidates[-1]
    elif mode == "index" and index is not None and 1 <= index <= len(candidates):
        selected_candidate = candidates[index - 1]

    if selected_candidate is None:
        request = evidence_payload.get("request") if isinstance(evidence_payload, dict) else {}
        return None, _build_recording_failure_session_scope_request_message(
            _display_value((request or {}).get("barcode"), default=""),
            _display_value((request or {}).get("date"), default=""),
            records,
        )

    narrowed = deepcopy(evidence_payload)
    narrowed["records"] = [
        _clone_record_for_session(records[selected_candidate["recordIndex"]], selected_candidate["sessionDetail"])
    ]
    request_payload = narrowed.get("request") if isinstance(narrowed.get("request"), dict) else {}
    if request_payload is not None:
        request_payload["selectedSessionHint"] = selector_text
    return narrowed, None


def _build_recording_failure_analysis_evidence(
    *,
    question: str,
    summary_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = summary_payload if isinstance(summary_payload, dict) else {}
    request = payload.get("request") if isinstance(payload.get("request"), dict) else {}
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    raw_records = payload.get("records") if isinstance(payload.get("records"), list) else []

    compact_records: list[dict[str, Any]] = []
    tag_set: set[str] = set()
    for record in raw_records[:4]:
        if not isinstance(record, dict):
            continue
        compact_record = _build_compact_record(record)
        compact_records.append(compact_record)
        tag_set.update(compact_record.get("classificationTags") or [])

    classification_tags = sorted(tag_set)
    return {
        "route": "recording_failure_analysis",
        "source": "box_db+s3+mommybox_code",
        "request": {
            "question": question,
            "barcode": request.get("barcode"),
            "date": request.get("date"),
            "dateRange": request.get("dateRange"),
            "mode": request.get("mode"),
        },
        "summary": summary,
        "records": compact_records,
        "classificationTags": classification_tags,
        "codeEvidence": _build_code_evidence(question, classification_tags),
    }


def _describe_end_state(record: dict[str, Any]) -> str:
    sessions = record.get("sessions") if isinstance(record.get("sessions"), dict) else {}
    if bool(record.get("restartDetected")):
        return "마미박스 비정상 종료"
    canceled_count = int(sessions.get("canceledCount") or 0)
    if canceled_count > 0:
        return f"녹화 취소 세션 `{canceled_count}건`"
    abnormal_count = int(sessions.get("abnormalCount") or 0)
    if abnormal_count > 0:
        return f"비정상 종료 세션 `{abnormal_count}건`"
    return "정상 종료"


def _describe_recording_outcome(record: dict[str, Any]) -> str:
    tags = set(record.get("classificationTags") or [])
    recordings_on_date_count = int(record.get("recordingsOnDateCount") or 0)
    if "restart_detected" in tags:
        return "정상 녹화 실패로 판단"
    if "pre_recording_stop" in tags:
        return "정상 녹화 실패로 판단"
    if recordings_on_date_count <= 0 and (
        "ffmpeg_error" in tags or "recording_stalled" in tags or "finish_anomaly" in tags
    ):
        return "녹화 & 업로드 실패로 판단"
    if "finish_anomaly" in tags:
        return "영상 손상 가능성 높음"
    if "stop_missing" in tags:
        return "정상 녹화 실패 가능성 높음"
    if "ffmpeg_error" in tags:
        return "영상 손상 가능성 의심"
    if "upload_network_error" in tags and recordings_on_date_count <= 0:
        return "영상 업로드 실패 가능성 높음"
    if "status_network_error" in tags and recordings_on_date_count > 0:
        return "정상 녹화로 판단 (통신 오류 별도)"
    if recordings_on_date_count > 0:
        return "정상 녹화로 판단"
    return "추가 확인 필요"


def _build_cause_line(record: dict[str, Any]) -> str:
    tags = set(record.get("classificationTags") or [])
    top_group = _get_top_error_group(record)
    top_component = _display_value(top_group.get("component"), default="미확인")
    top_signature = _display_value(top_group.get("signature"), default="미확인")
    top_count = int(top_group.get("count") or 0)
    recordings_on_date_count = int(record.get("recordingsOnDateCount") or 0)
    has_stall = "recording_stalled" in tags
    has_ffmpeg_sigterm = "ffmpeg_sigterm" in tags

    if "restart_detected" in tags:
        return "세션 중 장비 재시작이 확인돼 정상 녹화 실패로 판단해"
    if "pre_recording_stop" in tags:
        return "모션 감지 종료 전에 종료 스캔돼 본 녹화 시작 전 녹화 취소로 끝났어"
    if recordings_on_date_count <= 0 and ("ffmpeg_error" in tags or has_stall or "finish_anomaly" in tags):
        if has_stall and "ffmpeg_error" in tags:
            return "녹화 중 파일 증가율 저하(stall)와 ffmpeg 종료가 함께 확인됐고 날짜 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 캡처보드 연결 불량을 우선 의심해"
        if has_stall:
            return "녹화 중 파일 증가율 저하(stall)가 반복됐고 날짜 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해. 캡처보드 이상 또는 캡처보드 연결 불량을 우선 의심해"
        if has_ffmpeg_sigterm:
            return "ffmpeg가 SIGTERM으로 종료됐고 날짜 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해"
        return "ffmpeg 오류가 확인됐고 날짜 기준 DB 영상 기록이 없어 녹화 & 업로드 실패로 판단해"
    if "finish_anomaly" in tags:
        return "초기 ffmpeg 오류보다 종료 처리 지연과 종료 후 장치 오류가 더 뚜렷해서 실제 영상 손상 가능성이 높아"
    if "status_network_error" in tags and "ffmpeg_error" not in tags:
        if recordings_on_date_count > 0:
            return "JWT/상태 전송/업로드 통신 오류가 있었지만 날짜 기준 DB 영상 기록이 확인돼 녹화 실패 원인이라기보다 네트워크/DNS 통신 이상으로 봐야 해"
        return "업로드/상태 전송 통신 오류가 반복됐고 날짜 기준 DB 영상 기록이 없어 업로드 실패 가능성을 의심해야 해"
    if "ffmpeg_timestamp_error" in tags:
        return "ffmpeg DTS/타임스탬프 이상이 확인돼 캡처보드 연결 불량 또는 캡처보드 고장을 우선 의심해"
    if has_ffmpeg_sigterm:
        return "ffmpeg가 SIGTERM으로 종료돼 녹화 흐름이 끊겼고 캡처보드/영상 입력 계열을 우선 점검해야 해"
    if "ffmpeg_error" in tags:
        return "ffmpeg 오류가 확인돼 영상 손상 가능성을 의심해야 하고 캡처보드 이상을 우선 점검해야 해"
    if "stop_missing" in tags:
        return "종료 스캔이 없는 세션이 있어 정상 녹화 실패 가능성이 높아"
    if top_signature != "미확인" and top_count >= 2:
        return f"`{top_component}`에서 `{top_signature}` 오류가 반복돼 녹화 실패 가능성이 높아"
    if top_signature != "미확인":
        return f"`{top_component}`에서 `{top_signature}` 오류가 확인돼 원인 점검이 필요해"
    return "운영 근거상 명확한 실패 원인을 추가 확인해야 해"


def _build_impact_line(record: dict[str, Any]) -> str:
    date_label = _display_value(record.get("date"), default="미확인")
    hospital_name = _display_value(record.get("hospitalName"), default="미확인")
    room_name = _display_value(record.get("roomName"), default="미확인")
    device_name = _display_value(record.get("deviceName"), default="미확인")
    error_line_count = int(record.get("errorLineCount") or 0)
    outcome = _describe_recording_outcome(record)
    return (
        f"`{date_label}` `{hospital_name}` `{room_name}` 장비 `{device_name}`에서 "
        f"종료 상태는 `{_describe_end_state(record)}`이고, 녹화 결과는 `{outcome}`야"
        f" (error 라인 `{error_line_count}줄`)")


def _build_operational_evidence_lines(record: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    if bool(record.get("restartDetected")):
        restart_events = record.get("restartEvents") if isinstance(record.get("restartEvents"), list) else []
        first_restart = restart_events[0] if restart_events and isinstance(restart_events[0], dict) else {}
        restart_time = _display_value(first_restart.get("timeLabel") or first_restart.get("time"), default="시간미상")
        lines.append(f"- `{restart_time}` 장비 재시작 감지 (`Mommybox Starting...`)")

    first_ffmpeg_error = record.get("firstFfmpegError") if isinstance(record.get("firstFfmpegError"), dict) else {}
    if first_ffmpeg_error:
        ffmpeg_time = _display_value(first_ffmpeg_error.get("timeLabel"), default="시간미상")
        session_start = _display_value(first_ffmpeg_error.get("sessionStartTime"), default="미확인")
        elapsed = _display_value(first_ffmpeg_error.get("elapsedFromSessionStart"), default="")
        parts = [f"첫 ffmpeg 오류 `{ffmpeg_time}`"]
        if session_start != "미확인":
            parts.append(f"세션 시작 `{session_start}`")
        if elapsed:
            parts.append(f"시작 후 `{elapsed}`")
        lines.append(f"- {', '.join(parts)}")

    diagnostics = record.get("sessionDiagnostics") if isinstance(record.get("sessionDiagnostics"), list) else []
    for item in diagnostics:
        if not isinstance(item, dict):
            continue
        if str(item.get("severity") or "") != "high":
            continue
        detail = _display_value(item.get("displayText"), default="")
        if detail:
            lines.append(f"- 종료 후 처리 이상: {detail}")
            break

    top_group = _get_top_error_group(record)
    top_signature = _display_value(top_group.get("signature"), default="미확인")
    top_component = _display_value(top_group.get("component"), default="미확인")
    top_count = int(top_group.get("count") or 0)
    if top_signature != "미확인" and top_count > 0:
        lines.append(f"- `{top_component}` `{top_signature}` `{top_count}회`")

    recordings_on_date_count = int(record.get("recordingsOnDateCount") or 0)
    lines.append(f"- 날짜 기준 DB 영상 기록 `{recordings_on_date_count}개`")
    return lines[:4]


def _build_action_lines(record: dict[str, Any]) -> list[str]:
    tags = set(record.get("classificationTags") or [])
    actions: list[str] = []
    if "recording_stalled" in tags or "ffmpeg_timestamp_error" in tags or "ffmpeg_error" in tags or "device_busy" in tags:
        actions.append("- 캡처보드 케이블 체결 상태와 입력 신호를 가장 먼저 점검")
    if "recording_stalled" in tags:
        actions.append("- 장비 저장 경로 쓰기 상태와 파일 증가율 저하 원인을 먼저 확인")
    if "restart_detected" in tags:
        actions.append("- 전원 차단/전원 버튼 오입력 여부 확인")
    if "upload_network_error" in tags or "status_network_error" in tags:
        actions.append("- 장비 네트워크 상태와 DNS 해석(getaddrinfo EAI_AGAIN) 여부 확인")
    top_group = _get_top_error_group(record)
    top_component = _display_value(top_group.get("component"), default="")
    if top_component:
        actions.append(f"- `{top_component}` 관련 장치/프로세스 상태 확인")
    if "ffmpeg_error" in tags:
        actions.append("- ffmpeg 프로세스 상태와 영상 입력 장치 점유 여부 확인")
    return actions[:3] or ["- 동일 시각 장비 상태와 관련 프로세스 로그 확인"]


def _build_confidence(record: dict[str, Any]) -> str:
    tags = set(record.get("classificationTags") or [])
    top_count = int(_get_top_error_group(record).get("count") or 0)
    if "restart_detected" in tags or "finish_anomaly" in tags:
        return "높음"
    if top_count >= 2 or "ffmpeg_timestamp_error" in tags:
        return "중간"
    return "중간"


def _render_recording_failure_analysis_fallback(evidence_payload: dict[str, Any]) -> str:
    request = evidence_payload.get("request") if isinstance(evidence_payload, dict) else {}
    records = evidence_payload.get("records") if isinstance(evidence_payload, dict) else []
    barcode = _display_value((request or {}).get("barcode"), default="미확인")
    request_date = _display_value((request or {}).get("date"), default="미확인")

    lines = [
        "*녹화 실패 원인 분석*",
        f"• 바코드: `{barcode}`",
    ]
    if request_date != "미확인":
        lines.append(f"• 날짜: `{request_date}`")

    if not isinstance(records, list) or not records:
        lines.append("• 핵심 원인: 해당 범위에서 실패 원인을 판단할 운영 근거를 찾지 못했어")
        lines.append("• 운영 근거: 세션 또는 에러 라인 확인 필요")
        lines.append("• 영향: 추가 확인 필요")
        lines.append("• 권장 조치:")
        lines.append("- 바코드/날짜/병원/병실 범위를 다시 확인해줘")
        lines.append("• 확실도: 낮음")
        return "\n".join(lines)

    primary = records[0] if isinstance(records[0], dict) else {}
    lines.extend(
        [
            f"• 장비: `{_display_value(primary.get('deviceName'), default='미확인')}`",
            f"• 병원: `{_display_value(primary.get('hospitalName'), default='미확인')}`",
            f"• 병실: `{_display_value(primary.get('roomName'), default='미확인')}`",
            f"• 종료 상태: `{_describe_end_state(primary)}`",
            f"• 녹화 결과: `{_describe_recording_outcome(primary)}`",
            f"• 핵심 원인: {_build_cause_line(primary)}",
            "• 운영 근거:",
        ]
    )
    lines.extend(_build_operational_evidence_lines(primary))
    lines.append(f"• 영향: {_build_impact_line(primary)}")
    lines.append("• 권장 조치:")
    lines.extend(_build_action_lines(primary))
    lines.append(f"• 확실도: `{_build_confidence(primary)}`")
    return _truncate_text("\n".join(lines), 5000)
