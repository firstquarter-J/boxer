"""Slack gateway가 실행 provider 없이 읽는 회사 transport 계약이다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from enum import Enum
import re
from typing import Any, Mapping, Protocol
from zoneinfo import ZoneInfo

DEVICE_LOG_UPLOAD_ROUTE = "device_log_upload"
DEVICE_FILE_LOOKUP_ROUTE = "device_file_lookup"
DEVICE_FILE_DOWNLOAD_ROUTE = "device_file_download"
DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE = (
    "device_file_download_barcode_required"
)
DEVICE_FILE_RECOVERY_ROUTE = "device_file_recovery"
DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION = "device_file_download_delivery"
TRUSTED_MDA_RECOVERY_SCOPE_METADATA_KEY = "trusted_mda_recovery_scope"

DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION = (
    "device_diagnostic_followup_probe"
)
DEVICE_OPERATION_DELIVERY_ACTION = "device_operation_delivery"
DEVICE_SCANNER_ABI_PATCH_ROUTE = "device_scanner_abi_patch"

DEVICE_HEALTH_ALERT_SMS_ACTION = "device_health_alert_contact_hospital"
DEVICE_HEALTH_ALERT_VOICE_ACTION = "device_health_alert_device_voice_guide"
DEVICE_HEALTH_ALERT_MARK_DONE_ACTION = "device_health_alert_mark_done"
DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION = "device_health_alert_ui_receipt"
DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE = "device_health_alert_sms_prepare"
DEVICE_HEALTH_ALERT_SMS_ROUTE = "device_health_alert_sms"
DEVICE_HEALTH_ALERT_VOICE_ROUTE = "device_health_alert_voice_guide"
DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE = "device_health_alert_mark_done"
DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE = "device_health_alert_ui_receipt"

HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS = frozenset(
    {"C02C08K7YEN", "C068FVD5V7Y"}
)

_BARCODE_PATTERN = re.compile(r"(?<!\d)(\d{11})(?!\d)")
_DEVICE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,}$")
_EXPLICIT_DEVICE_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*)"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_SEARCH_INTENT_TOKENS = (
    "노션",
    "워크보드",
    "워크 보드",
    "work board",
    "회사 문서",
    "사내 문서",
)
_QUERY_NOISE_PATTERNS = (
    re.compile(r"work\s*board(?:에서|으로|로|의)?", re.IGNORECASE),
    re.compile(r"워크\s*보드(?:에서|으로|로|의)?"),
    re.compile(r"(?:회사|사내)\s*노션(?:에서|으로|로|의)?"),
    re.compile(r"노션(?:에서|으로|로|의)?"),
    re.compile(r"(?:회사|사내)\s*문서(?:에서|으로|로|의)?"),
)
_QUERY_REQUEST_WORDS = re.compile(
    r"(?:관련\s*)?(?:문서|페이지)(?:를|을)?|"
    r"찾아\s*줘|찾아줘|찾아|검색해\s*줘|검색해줘|검색|"
    r"조회해\s*줘|조회해줘|조회|보여\s*줘|보여줘|알려\s*줘|알려줘|"
    r"요약해\s*줘|요약해줘|정리해\s*줘|정리해줘|답변해\s*줘|답변해줘|"
    r"답해\s*줘|답해줘|설명해\s*줘|설명해줘|내용(?:을|은|이)?|"
    r"뭐야|무엇(?:인지|이야)?|어떻게(?:\s*해|\s*해야\s*해)?|왜|좀"
)
_FILE_HINTS = (
    "영상",
    "파일",
    "fileid",
    "file id",
    "다운로드",
    "복구",
)
_LOG_ACTION_HINTS = ("로그", "log")
_LOG_REQUEST_HINTS = ("업로드", "올려", "요청", "확인", "upload", "request")
_HOSPITAL_HINTS = ("병원", "의원", "클리닉", "센터")
_ROOM_HINTS = ("진료실", "병실", "초음파실", "분만실", "수술실", "상담실")


class CompanyAssistantRequestContract(Protocol):
    """Slack/API request가 matcher에 제공해야 하는 최소 provider-free shape다."""

    question: str
    metadata: Mapping[str, Any]
    context_entries: tuple[Any, ...]


class HpaChangePollState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    REVIEW_READY = "review_ready"
    NEEDS_CLARIFICATION = "needs_clarification"
    PR_OPENED = "pr_opened"
    NO_CHANGE_NEEDED = "no_change_needed"
    FAILED = "failed"


@dataclass(frozen=True, repr=False)
class HpaChangePollResult:
    """Slack renderer가 소비하는 provider 중립 poll snapshot이다."""

    task_id: str
    state: HpaChangePollState
    job: Any
    result: dict[str, Any]
    pr_urls: tuple[str, ...]


def _looks_like_company_notion_search(question: str) -> bool:
    normalized = re.sub(r"\s+", " ", str(question or "").strip().lower())
    return bool(normalized) and any(
        token in normalized for token in _SEARCH_INTENT_TOKENS
    )


def _extract_company_notion_search_query(question: str) -> str:
    query = str(question or "").strip()
    for pattern in _QUERY_NOISE_PATTERNS:
        query = pattern.sub(" ", query)
    query = _QUERY_REQUEST_WORDS.sub(" ", query)
    query = re.sub(r"[?？!！.,:;~]+", " ", query)
    return re.sub(r"\s+", " ", query).strip()[:120]


def _is_device_scanner_abi_patch_intent(question: str) -> bool:
    """실행 승인이 아니라 다른 mutation을 막는 넓은 intent 경계다."""

    normalized = re.sub(r"<@[^>]+>", " ", str(question or ""))
    lowered = " ".join(normalized.split()).casefold()
    has_scanner = any(
        hint in lowered for hint in ("스캐너", "scanner", "node-hid")
    )
    has_patch = any(
        hint in lowered
        for hint in (
            "abi",
            "패치",
            "patch",
            "복구",
            "호환",
            "적용",
            "조치",
            "수정",
            "repair",
            "fix",
        )
    )
    return has_scanner and has_patch


def build_trusted_mda_recovery_scope_metadata(
    *,
    barcode: str,
    log_date: str,
    device_context: Mapping[str, Any],
) -> dict[str, Any]:
    scope = _normalize_trusted_mda_recovery_scope(
        {
            "barcode": barcode,
            "logDate": log_date,
            "deviceName": device_context.get("deviceName"),
            "hospitalName": device_context.get("hospitalName"),
            "roomName": device_context.get("roomName"),
        }
    )
    if scope is None:
        raise ValueError("trusted MDA recovery scope is invalid")
    return {TRUSTED_MDA_RECOVERY_SCOPE_METADATA_KEY: scope}


def resolve_device_file_operation_scope(
    request: CompanyAssistantRequestContract,
) -> tuple[str | None, str | None]:
    """Slack root 검증에 필요한 explicit barcode/date만 해석한다."""

    barcode = _single_request_barcode(request)
    return barcode, _extract_explicit_log_date(request.question)


def needs_device_file_operation_context(question: str) -> bool:
    """현재 문장에 target이 없을 때만 bounded Slack thread를 요청한다."""

    text = str(question or "")
    if _looks_like_log_upload_request(text):
        return not _has_explicit_device(text) and not _has_hospital_room(text)
    if _BARCODE_PATTERN.search(text) is not None:
        return False
    lowered = text.lower()
    return any(hint in text or hint in lowered for hint in _FILE_HINTS)


def company_operation_route_names() -> frozenset[str]:
    """Slack 응답 검증과 API dispatcher가 공유하는 operation allowlist다."""

    return frozenset(
        {
            "security_review",
            DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE,
            DEVICE_HEALTH_ALERT_SMS_ROUTE,
            DEVICE_HEALTH_ALERT_VOICE_ROUTE,
            DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE,
            DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE,
            "thread_playbook_learning",
            "app_user_baby_selection_analysis",
            "app_user_lookup",
            "recording_streaming_restore",
            "barcode_pink_classification_reason",
            "barcode_validation_status",
            "admin_s3_ultrasound",
            "admin_s3_device_log",
            "admin_request_log",
            "admin_readonly_sql",
            DEVICE_LOG_UPLOAD_ROUTE,
            DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE,
            DEVICE_FILE_LOOKUP_ROUTE,
            DEVICE_FILE_DOWNLOAD_ROUTE,
            DEVICE_FILE_RECOVERY_ROUTE,
            "device_voice_catalog",
            "device_voice_change",
            "device_diagnostic_snapshot",
            "device_diagnostic_analysis",
            "device_diagnostic_followup",
            "device_update_status",
            "device_box_update",
            "device_agent_update",
            "device_power_off",
            DEVICE_SCANNER_ABI_PATCH_ROUTE,
            DEVICE_OPERATION_DELIVERY_ACTION,
            "device_audio_probe",
            "device_remote_access_probe",
            "device_memory_patch",
            "device_pm2_probe",
            "device_captureboard_probe",
            "device_led_probe",
            "device_status_probe",
        }
    )


def _normalize_trusted_mda_recovery_scope(
    raw_scope: Mapping[str, Any],
) -> dict[str, str] | None:
    barcode = str(raw_scope.get("barcode") or "").strip()
    log_date = str(raw_scope.get("logDate") or "").strip()
    device_name = str(raw_scope.get("deviceName") or "").strip()
    hospital_name = " ".join(
        str(raw_scope.get("hospitalName") or "").split()
    ).strip()
    room_name = " ".join(
        str(raw_scope.get("roomName") or "").split()
    ).strip()
    try:
        datetime.strptime(log_date, "%Y-%m-%d")
    except ValueError:
        return None
    if not _BARCODE_PATTERN.fullmatch(barcode):
        return None
    if len(device_name) > 64 or not _DEVICE_NAME_PATTERN.fullmatch(device_name):
        return None
    if not all(
        1 <= len(value) <= 200 and value.isprintable()
        for value in (hospital_name, room_name)
    ):
        return None
    return {
        "barcode": barcode,
        "logDate": log_date,
        "deviceName": device_name,
        "hospitalName": hospital_name,
        "roomName": room_name,
    }


def _single_request_barcode(
    request: CompanyAssistantRequestContract,
) -> str | None:
    explicit = _BARCODE_PATTERN.search(request.question)
    if explicit is not None:
        return explicit.group(1)
    metadata = str(request.metadata.get("barcode") or "").strip()
    if _BARCODE_PATTERN.fullmatch(metadata):
        return metadata
    for entry in reversed(request.context_entries):
        text = str(getattr(entry, "text", "") or "")
        if isinstance(entry, Mapping):
            text = str(entry.get("text") or "")
        matched = _BARCODE_PATTERN.search(text)
        if matched is not None:
            return matched.group(1)
    return None


def _extract_explicit_log_date(question: str) -> str | None:
    text = str(question or "")
    matched = re.search(r"(?<!\d)(20\d{2})[-./](\d{1,2})[-./](\d{1,2})(?!\d)", text)
    if matched is None:
        matched = re.search(r"(?<!\d)(\d{2})[-./](\d{1,2})[-./](\d{1,2})(?!\d)", text)
        if matched is not None:
            year = 2000 + int(matched.group(1))
            values = (year, int(matched.group(2)), int(matched.group(3)))
        else:
            korean = re.search(r"(?:(20\d{2})\s*년\s*)?(\d{1,2})\s*월\s*(\d{1,2})\s*일", text)
            if korean is None:
                relative = {"오늘": 0, "어제": -1}.get(text.strip())
                if relative is None:
                    return None
                today = datetime.now(ZoneInfo("Asia/Seoul")).date()
                return (today + timedelta(days=relative)).isoformat()
            year = int(korean.group(1) or datetime.now(ZoneInfo("Asia/Seoul")).year)
            values = (year, int(korean.group(2)), int(korean.group(3)))
    else:
        values = tuple(int(matched.group(index)) for index in (1, 2, 3))
    try:
        return date(*values).isoformat()
    except ValueError as exc:
        raise ValueError("날짜 형식을 확인해줘") from exc


def _looks_like_log_upload_request(question: str) -> bool:
    lowered = question.lower()
    return (
        any(hint in question or hint in lowered for hint in _LOG_ACTION_HINTS)
        and any(hint in question or hint in lowered for hint in _LOG_REQUEST_HINTS)
        and "로그인" not in question
        and "s3 로그" not in lowered
        and "로그 분석" not in question
    )


def _has_explicit_device(question: str) -> bool:
    return any(
        _DEVICE_NAME_PATTERN.fullmatch(match.group(1).strip())
        for match in _EXPLICIT_DEVICE_PATTERN.finditer(question)
    )


def _has_hospital_room(question: str) -> bool:
    return any(token in question for token in _HOSPITAL_HINTS) and any(
        token in question for token in _ROOM_HINTS
    )


__all__ = [
    "DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION",
    "DEVICE_HEALTH_ALERT_MARK_DONE_ACTION",
    "DEVICE_HEALTH_ALERT_MARK_DONE_ROUTE",
    "DEVICE_HEALTH_ALERT_SMS_ACTION",
    "DEVICE_HEALTH_ALERT_SMS_PREPARE_ROUTE",
    "DEVICE_HEALTH_ALERT_SMS_ROUTE",
    "DEVICE_HEALTH_ALERT_UI_RECEIPT_ACTION",
    "DEVICE_HEALTH_ALERT_UI_RECEIPT_ROUTE",
    "DEVICE_HEALTH_ALERT_VOICE_ACTION",
    "DEVICE_HEALTH_ALERT_VOICE_ROUTE",
    "DEVICE_FILE_DOWNLOAD_BARCODE_REQUIRED_ROUTE",
    "DEVICE_FILE_DOWNLOAD_DELIVERY_ACTION",
    "DEVICE_FILE_DOWNLOAD_ROUTE",
    "DEVICE_FILE_LOOKUP_ROUTE",
    "DEVICE_FILE_RECOVERY_ROUTE",
    "DEVICE_LOG_UPLOAD_ROUTE",
    "DEVICE_OPERATION_DELIVERY_ACTION",
    "DEVICE_SCANNER_ABI_PATCH_ROUTE",
    "HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS",
    "HpaChangePollResult",
    "HpaChangePollState",
    "TRUSTED_MDA_RECOVERY_SCOPE_METADATA_KEY",
    "_extract_company_notion_search_query",
    "_is_device_scanner_abi_patch_intent",
    "_looks_like_company_notion_search",
    "build_trusted_mda_recovery_scope_metadata",
    "company_operation_route_names",
    "needs_device_file_operation_context",
    "resolve_device_file_operation_scope",
]
