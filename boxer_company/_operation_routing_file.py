"""장비 파일·로그·streaming operation의 provider-free 분류 정본이다."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from boxer_company._operation_routing_common import (
    CompanyOperationRequestContract as CompanyAssistantRequest,
    _extract_hospital_room_scope,
    company_settings as cs,
)
from boxer_company.transport_contracts import (
    DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE,
    DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION,
    DEVICE_FILE_DOWNLOAD_ROUTE,
    DEVICE_FILE_LOOKUP_ROUTE,
    DEVICE_FILE_RECOVERY_ROUTE,
    DEVICE_LOG_UPLOAD_ROUTE,
)


_YEAR_MONTH_PATTERN = re.compile(
    r"(20\d{2})\s*(?:"
    r"년\s*(0?[1-9]|1[0-2])(?:\s*월(?!\s*(?:[0-3]?\d\s*일|\d))|(?!\s*(?:월|일|[0-3]?\d\s*일|\d)))"
    r"|[-./]\s*(0?[1-9]|1[0-2])(?!\s*(?:[-./]\s*\d{1,2}|\d))"
    r")"
)


_COMPACT_YEAR_MONTH_PATTERN = re.compile(r"(?<!\d)(20\d{2})(0[1-9]|1[0-2])(?!\d)")


_STREAMING_RESTORE_ACTION_PATTERN = re.compile(
    r"(스트리밍\s*종료.*(?:복원|복구|해제|원복)|복원|복구|원복|"
    r"블라인드(?:를|을)?\s*해제|숨김(?:을|를)?\s*해제|unblind|reveal|"
    r"공개\s*(?:처리|전환|해줘|해|시켜)|노출\s*(?:처리|전환|해줘|해|시켜|가능))",
    re.IGNORECASE,
)


_RECORDING_MEDIA_PATTERN = re.compile(
    r"(영상|동영상|녹화|recording|recordings|ultrasound)",
    re.IGNORECASE,
)


def _is_recording_streaming_restore_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    normalized = question or ""
    return bool(
        _RECORDING_MEDIA_PATTERN.search(normalized)
        and _STREAMING_RESTORE_ACTION_PATTERN.search(normalized)
    )


def _extract_recording_streaming_restore_month(question: str) -> tuple[int, int]:
    normalized = question or ""
    year_month_match = _YEAR_MONTH_PATTERN.search(normalized)
    if year_month_match:
        month_text = year_month_match.group(2) or year_month_match.group(3)
        return int(year_month_match.group(1)), int(month_text)

    compact_match = _COMPACT_YEAR_MONTH_PATTERN.search(normalized)
    if compact_match:
        return int(compact_match.group(1)), int(compact_match.group(2))

    raise ValueError("복원할 연도와 월을 같이 입력해줘. 예: `35033165423 2024년 4월 영상 복원`")


_DEVICE_FILE_ID_HINTS = (
    "fileid",
    "file id",
    "파일id",
    "파일 id",
    "파일 아이디",
    "파일아이디",
)


_DEVICE_FILE_DOWNLOAD_HINTS = (
    "다운로드",
    "영상 다운",
    "영상다운",
    "파일 다운",
    "파일다운",
    "영상 꺼내",
    "영상꺼내",
    "파일 꺼내",
    "파일꺼내",
    "영상 받아",
    "영상 받아줘",
    "영상받아",
    "영상받아줘",
    "받아줘",
    "받아 줘",
    "내려받아",
)


_DEVICE_FILE_RECOVERY_HINTS = (
    "영상 복구",
    "영상복구",
    "파일 복구",
    "파일복구",
    "복구",
    "복구해",
    "복구 해",
    "복구해줘",
    "복구 해줘",
)


_DEVICE_FILE_REMOTE_HINTS = (
    "파일 있",
    "파일있",
    "파일 있어",
    "파일있어",
    "파일 있는지",
    "파일있는지",
    "파일 존재",
    "영상 있",
    "영상있",
    "영상 있어",
    "영상있어",
    "영상 있는지",
    "영상있는지",
    "영상 존재",
    "있는지",
    "존재 확인",
    "남은 영상",
    "남은 파일",
    "장비에 남은 영상",
    "장비에 남은 파일",
    "장비 영상",
    "로컬 영상",
    "장비 파일",
    "장비에 파일",
    "디바이스 파일",
    "로컬 파일",
    *_DEVICE_FILE_DOWNLOAD_HINTS,
    *_DEVICE_FILE_RECOVERY_HINTS,
)


_DEVICE_FILE_PROBE_HINTS = _DEVICE_FILE_ID_HINTS + _DEVICE_FILE_REMOTE_HINTS


def _is_barcode_device_file_probe_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_PROBE_HINTS)


def _should_download_device_files(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_DOWNLOAD_HINTS)


def _should_recover_device_files(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _DEVICE_FILE_RECOVERY_HINTS)


_DEVICE_LOG_UPLOAD_CHECK_HINTS = (
    "확인",
    "확인해",
    "확인해줘",
    "있나",
    "있는지",
    "없나",
    "없는지",
    "해줘",
    "해주세요",
    "부탁",
)


_DEVICE_LOG_UPLOAD_ACTION_HINTS = (
    "업로드",
    "올려",
    "요청",
)


_HOSPITAL_HINT_TOKENS = ("병원", "의원", "클리닉", "센터")


_ROOM_HINT_TOKENS = ("진료실", "병실", "초음파실", "분만실", "수술실", "상담실")


def _normalize_device_log_upload_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _clean_log_upload_scope_value(value: str) -> str:
    normalized = " ".join(str(value or "").split()).strip().strip("`'\"")
    normalized = re.sub(r"(?<!\d)\d{11}(?!\d)", "", normalized)
    normalized = re.sub(
        r"\b(?:로그|업로드|확인해줘|확인해|확인|해줘|해주세요|부탁|요청|장비|마미박스|전원|운영)\b",
        " ",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"\b(?:today|check|upload|request|log|device)\b", " ", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"(?<!\d)(20\d{2}|19\d{2})[-./]\d{1,2}[-./]\d{1,2}(?!\d)", " ", normalized)
    normalized = re.sub(r"(?<!\d)\d{1,2}\s*월\s*\d{1,2}\s*일(?!\d)", " ", normalized)
    normalized = re.sub(r"[/:|]+$", "", normalized)
    return " ".join(normalized.split()).strip(" /")


def _looks_like_hospital_name(value: str) -> bool:
    normalized = _clean_log_upload_scope_value(value)
    return bool(normalized) and any(token in normalized for token in _HOSPITAL_HINT_TOKENS)


def _looks_like_room_name(value: str) -> bool:
    normalized = _clean_log_upload_scope_value(value)
    return bool(normalized) and any(token in normalized for token in _ROOM_HINT_TOKENS)


def _extract_hospital_room_scope_for_log_upload(question: str) -> tuple[str | None, str | None]:
    normalized = _normalize_device_log_upload_question(question)
    hospital_name, room_name = _extract_hospital_room_scope(normalized)
    hospital_name = _clean_log_upload_scope_value(hospital_name or "")
    room_name = _clean_log_upload_scope_value(room_name or "")
    if _looks_like_hospital_name(hospital_name) and _looks_like_room_name(room_name):
        return hospital_name, room_name

    slash_parts = [_clean_log_upload_scope_value(part) for part in re.split(r"\s*/\s*", normalized) if part.strip()]
    for index, part in enumerate(slash_parts):
        if not _looks_like_hospital_name(part):
            continue
        for candidate_room in slash_parts[index + 1 :]:
            if _looks_like_room_name(candidate_room):
                return part, candidate_room

    compact_text = _clean_log_upload_scope_value(normalized)
    if not compact_text:
        return None, None
    room_patterns = [
        r"(?P<hospital>.+?)\s+(?P<room>(?:초음파실|진료실|병실|분만실|수술실|상담실)\S*)$",
        r"(?P<hospital>.+?)\s+(?P<room>\S*(?:초음파실|진료실|병실|분만실|수술실|상담실)\S*)$",
    ]
    for pattern in room_patterns:
        match = re.search(pattern, compact_text)
        if not match:
            continue
        hospital_candidate = _clean_log_upload_scope_value(match.group("hospital"))
        room_candidate = _clean_log_upload_scope_value(match.group("room"))
        if _looks_like_hospital_name(hospital_candidate) and _looks_like_room_name(room_candidate):
            return hospital_candidate, room_candidate

    return None, None


def _is_device_log_upload_check_request(question: str) -> bool:
    text = _normalize_device_log_upload_question(question)
    lowered = text.lower()
    if not text:
        return False
    if "로그인" in text:
        return False
    if "s3 로그" in text or re.search(r"\bs3\s+log\b", lowered):
        return False
    if "로그 분석" in text or "로그 에러" in text:
        return False

    has_log_hint = "로그" in text or bool(re.search(r"\blog\b", lowered))
    if not has_log_hint:
        return False

    has_action_hint = any(token in text for token in _DEVICE_LOG_UPLOAD_ACTION_HINTS) or any(
        token in lowered for token in ("upload", "reupload", "request")
    )
    if not has_action_hint:
        return False

    has_check_hint = any(token in text for token in _DEVICE_LOG_UPLOAD_CHECK_HINTS) or "check" in lowered
    return has_check_hint or has_action_hint


_OPERATIONS_ROUTE_GROUP = "operations"


_BARCODE_PATTERN = re.compile(r"(?<!\d)(\d{11})(?!\d)")


_DEVICE_NAME_LABEL_PATTERN = re.compile(
    r"(?:장비명|devicename)\s*[:=]?\s*([A-Za-z0-9._-]+)",
    re.IGNORECASE,
)


_DEVICE_NAME_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"([A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*)"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)


def match_device_file_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 호출 없이 기존 Slack parser의 첫 작업 범위를 분류한다."""

    if (
        str(request.metadata.get("route_group") or "").strip()
        != _OPERATIONS_ROUTE_GROUP
    ):
        return None
    if is_device_file_download_delivery_receipt(request):
        # DM 성공 뒤 같은 request ID로 들어오는 typed receipt는 자연어를
        # 다시 실행하지 않고 URL 없는 delivery manifest만 확정한다.
        return DEVICE_FILE_DOWNLOAD_ROUTE
    question = request.question
    if (
        _single_request_barcode(request) is None
        and _is_missing_barcode_device_download_request(question)
    ):
        # 기존 Slack의 바코드 선행 안내를 장비 음성/상태 mutation보다 먼저
        # 확정해 복합 문장이 다른 작업으로 내려가지 않게 한다.
        return DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE
    explicit_devices = _explicit_device_names(question)
    if _is_device_log_upload_check_request(question):
        hospital_name, room_name = _request_log_hospital_room(request)
        if explicit_devices or (hospital_name and room_name):
            return DEVICE_LOG_UPLOAD_ROUTE
        return None

    barcode = _single_request_barcode(request)
    if barcode is None:
        return None
    if _is_recording_streaming_restore_request(question, barcode):
        try:
            _extract_recording_streaming_restore_month(question)
        except ValueError:
            # 일자가 지정된 장비 파일 복구는 아래 파일 route가 소유한다.
            pass
        else:
            # 월 단위 MDA recordings 복원은 기존 전용 operation이 소유한다.
            return None
    if not _is_barcode_device_file_probe_request(question, barcode):
        return None
    if _should_recover_device_files(question):
        return DEVICE_FILE_RECOVERY_ROUTE
    if _should_download_device_files(question):
        return DEVICE_FILE_DOWNLOAD_ROUTE
    return DEVICE_FILE_LOOKUP_ROUTE


def is_device_file_download_delivery_receipt(
    request: CompanyAssistantRequest,
) -> bool:
    """고정된 delivered action만 다운로드 전달 receipt로 인정한다."""

    if (
        str(request.metadata.get("route_group") or "").strip()
        != _OPERATIONS_ROUTE_GROUP
    ):
        return False
    action = request.metadata.get("operation_action")
    return bool(
        isinstance(action, Mapping)
        and frozenset(action) == {"name", "phase", "delivery"}
        and str(action.get("name") or "").strip()
        == DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION
        and str(action.get("phase") or "").strip() == "delivered"
    )


def _is_missing_barcode_device_download_request(question: str) -> bool:
    if _single_explicit_barcode(question) is not None:
        return False
    if not _should_download_device_files(question):
        return False
    normalized = str(question or "").strip()
    availability_hints = (
        "다운로드 가능 상태",
        "다운로드 가능 여부",
        "다운로드 가능한지",
        "다운 가능 상태",
        "다운 가능 여부",
        "다운 가능한지",
    )
    return not any(hint in normalized for hint in availability_hints)


def _single_explicit_barcode(question: str) -> str | None:
    matched = _BARCODE_PATTERN.search(str(question or ""))
    return matched.group(1) if matched is not None else None


def _single_request_barcode(
    request: CompanyAssistantRequest,
) -> str | None:
    explicit = _single_explicit_barcode(request.question)
    if explicit is not None:
        return explicit
    metadata_barcode = str(request.metadata.get("barcode") or "").strip()
    if _BARCODE_PATTERN.fullmatch(metadata_barcode):
        return metadata_barcode
    # 기존 turn scope처럼 같은 요청자의 최신 thread 메시지부터 보고,
    # 한 thread에 과거 바코드가 여러 개 있어도 가장 최근 대상을 쓴다.
    for entry in reversed(_request_context_entries(request)):
        matched = _BARCODE_PATTERN.search(str(entry.get("text") or ""))
        if matched is not None:
            return matched.group(1)
    return None


def _request_file_hospital_room(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    """현재 질문을 우선하고 adapter의 phase2 scope는 fallback으로 쓴다."""

    question_hospital, question_room = _extract_hospital_room_scope(
        request.question
    )
    metadata_hospital = _first_metadata_text(
        request,
        "hospital_name",
        "hospitalName",
        "phase2_hospital_name",
        "phase2HospitalName",
    )
    metadata_room = _first_metadata_text(
        request,
        "room_name",
        "roomName",
        "phase2_room_name",
        "phase2RoomName",
    )
    return (
        question_hospital or metadata_hospital,
        question_room or metadata_room,
    )


def _request_log_hospital_room(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    hospital_name, room_name = _extract_hospital_room_scope_for_log_upload(
        request.question
    )
    if hospital_name and room_name:
        return hospital_name, room_name
    metadata_hospital, metadata_room = _request_file_hospital_room(request)
    if metadata_hospital and metadata_room:
        return metadata_hospital, metadata_room
    for entry in reversed(_request_context_entries(request)):
        hospital_name, room_name = (
            _extract_hospital_room_scope_for_log_upload(
                str(entry.get("text") or "")
            )
        )
        if hospital_name and room_name:
            return hospital_name, room_name
    return None, None


def _first_metadata_text(
    request: CompanyAssistantRequest,
    *keys: str,
) -> str | None:
    for key in keys:
        value = request.metadata.get(key)
        if isinstance(value, str):
            normalized = " ".join(value.split()).strip()
            if normalized:
                return normalized
    return None


def _request_context_entries(
    request: CompanyAssistantRequest,
) -> tuple[dict[str, Any], ...]:
    """Slack thread 전체를 원래 순서대로 보존한다."""

    return tuple(
        entry
        for entry in request.context_entries
        if isinstance(entry, dict)
    )


def _explicit_device_names(question: str) -> dict[str, str]:
    normalized = re.sub(r"<@[^>]+>", " ", str(question or ""))
    candidates = [
        *(match.group(1) for match in _DEVICE_NAME_LABEL_PATTERN.finditer(normalized)),
        *(match.group(1) for match in _DEVICE_NAME_TOKEN_PATTERN.finditer(normalized)),
    ]
    exact: dict[str, str] = {}
    for raw_candidate in candidates:
        candidate = str(raw_candidate or "").strip().strip("`'\"")
        if candidate and cs.S3_DEVICE_NAME_PATTERN.fullmatch(candidate):
            exact.setdefault(candidate.casefold(), candidate)
    return exact
