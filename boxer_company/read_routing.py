"""Slack과 API가 공유하는 provider-free 회사 read 분류 정본이다.

이 모듈은 질문 분류와 transport source 검증만 수행한다. DB, S3, MDA,
SSH, Redis, Google, LLM provider를 import하거나 실행하지 않는다.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime
import re
from typing import Any, Callable, Mapping
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

from boxer_company import settings as cs
from boxer_company._operation_routing_common import (
    AssistantRequestScopeMismatch,
    _COMPACT_MMDD_PATTERN,
    _COMPACT_YYMMDD_PATTERN,
    _COMPACT_YYYYMMDD_PATTERN,
    _KOREAN_MD_PATTERN,
    _KOREAN_YMD_PATTERN,
    _LEADING_HOSPITAL_KEYWORD_SCOPE_PATTERN,
    _NUMERIC_MD_DASH_PATTERN,
    _NUMERIC_MD_PATTERN,
    _NUMERIC_YMD_PATTERN,
    _YEAR_ONLY_PATTERN,
    _extract_barcode,
    _extract_device_name_scope,
    _extract_hospital_room_scope,
    _extract_log_date_with_presence,
    _extract_relative_day_offset,
    _is_barcode_log_analysis_request,
    _normalize_spaces,
    _normalize_year,
    _parse_explicit_date_expression,
    resolve_assistant_request_scope,
    window_assistant_context_entries,
)
from boxer_company._operation_routing_device import (
    _extract_device_name_for_diagnostic_freeform,
    _has_device_diagnostic_start_hint,
    _is_device_diagnostic_freeform_request,
    _select_device_diagnostic_followup_command_keys,
    match_device_read_route,
)
from boxer_company._operation_routing_file import (
    _is_recording_streaming_restore_request,
)
from boxer_company._operation_routing_knowledge import (
    _request_context_text,
    looks_like_notion_playbook_followup,
    looks_like_notion_playbook_question,
    match_notion_playbook_route,
)
from boxer_company._operation_routing_private import (
    _is_barcode_pink_classification_reason_request,
    _is_barcode_validation_status_request,
    _should_analyze_app_user_baby_selection,
    _should_lookup_barcode,
)
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.operation_routing import match_company_operation_route
from boxer_company.prompt_security import is_prompt_exfiltration_attempt
from boxer_company.transport_contracts import (
    _looks_like_company_notion_search,
)


COMMON_API_BARCODE_QUERY_ROUTES = frozenset(
    {
        "barcode_video_count",
        "baby_ai_list",
        "barcode_baby_ai_list",
        "barcode_video_info",
        "barcode_video_list",
        "barcode_video_length",
        "barcode_all_recorded_dates",
    }
)
BARCODE_TIMELINE_ROUTES = frozenset(
    {
        "barcode last recordedAt",
        "barcode recordedAt-on-date",
    }
)
WEEKLY_RECORDINGS_SUMMARY_ROUTE = "weekly_recordings_summary"

_USAGE_HELP_PATTERN = re.compile(
    r"^(?:사용법|사용 방법|도움말|help|헬프|명령어(?:\s*목록)?)\s*"
    r"(?:알려줘|보여줘|안내해줘)?$",
    re.IGNORECASE,
)

_CAPTURE_HINT_TOKENS = (
    "캡처",
    "capture",
    "captures",
    "capturedat",
    "스냅샷",
    "snapshot",
)
_BABY_AI_HINT_TOKENS = (
    "베이비매직",
    "babymagic",
    "baby magic",
    "baby_ai",
    "baby ai",
)
_HOSPITAL_QUERY_HINT_TOKENS = (
    "병원 조회",
    "병원 목록",
    "병원 개수",
    "병원 몇",
    "병원 수",
    "병원 있나",
    "병원 있는지",
    "병원 유무",
    "병원 생성일",
    "병원 생성연도",
    "생성된 병원",
    "hospitals",
)
_ROOMS_QUERY_HINT_TOKENS = (
    "병실 조회",
    "병실 목록",
    "병실 개수",
    "병실 몇",
    "병실 수",
    "병실 있나",
    "병실 있는지",
    "병실 유무",
    "진료실 조회",
    "진료실 목록",
    "진료실 개수",
    "진료실 몇",
    "진료실 수",
    "진료실 있나",
    "진료실 있는지",
    "hospital_rooms",
)
_DEVICE_QUERY_HINT_TOKENS = (
    "장비 조회",
    "장비 목록",
    "장비 개수",
    "장비 몇",
    "장비 수",
    "장비 있나",
    "장비 있는지",
    "장비 유무",
    "장비 상태",
    "장비상태",
    "장비 정보",
    "장비정보",
    "장비상세",
    "장비명",
    "devices",
    "devicename",
)
_HOSPITAL_SEQ_PATTERN = re.compile(
    r"(?:hospitalseq|병원seq)\s*[:=]?\s*(\d+)", re.IGNORECASE
)
_HOSPITAL_ROOM_SEQ_PATTERN = re.compile(
    r"(?:hospitalroomseq|hospital_room_seq|병실seq)\s*[:=]?\s*(\d+)",
    re.IGNORECASE,
)
_DEVICE_SEQ_PATTERN = re.compile(
    r"(?:deviceseq|device_seq|device\s*seq|장비seq)\s*[:=]?\s*(\d+)",
    re.IGNORECASE,
)
_DEVICE_STATUS_PATTERN = re.compile(
    r"(?:장비\s*상태|장비status|device\s*status|status)\s*[:=]?\s*([A-Za-z_]+)",
    re.IGNORECASE,
)
_ACTIVE_FLAG_PATTERN = re.compile(
    r"(?:activeflag|활성\s*flag)\s*[:=]?\s*([01])", re.IGNORECASE
)
_INSTALL_FLAG_PATTERN = re.compile(
    r"(?:installflag|설치\s*flag)\s*[:=]?\s*([01])", re.IGNORECASE
)
_BARE_YEAR_ONLY_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])(20\d{2}|19\d{2})(?![A-Za-z0-9])"
)
_LEADING_HOSPITAL_SCOPE_PATTERN = re.compile(
    r"^\s*(.+?)\s+(?:(?:초음파\s*)?영상|비디오|동영상|녹화|캡처|"
    r"스냅샷|병원|병실|진료실)(?:\s|$)",
    re.IGNORECASE,
)
_LEADING_HOSPITAL_SCOPE_QUESTION_TOKENS = (
    "모션감지",
    "모션 감지",
    "종료스캔",
    "종료 스캔",
    "녹화 취소",
    "취소 음성",
    "안내 음성",
    "왜",
    "원인",
    "이유",
    "어떻게",
    "나와",
)
_LEADING_HOSPITAL_SCOPE_QUESTION_ENDINGS = (
    "하면",
    "돼",
    "되나",
    "되나요",
    "맞아",
    "맞나요",
    "있어",
    "있나",
)


def _has_relative_date_token(text: str, lowered: str) -> bool:
    return _extract_relative_day_offset(text, lowered) is not None


def _extract_log_date(question: str) -> str:
    parsed_date, _ = _extract_log_date_with_presence(question)
    return parsed_date


def _extract_year_filter(question: str) -> int | None:
    text = (question or "").strip()
    for pattern in (
        _KOREAN_YMD_PATTERN,
        _NUMERIC_YMD_PATTERN,
        _KOREAN_MD_PATTERN,
        _NUMERIC_MD_PATTERN,
        _NUMERIC_MD_DASH_PATTERN,
        _COMPACT_YYYYMMDD_PATTERN,
        _COMPACT_YYMMDD_PATTERN,
    ):
        if pattern.search(text):
            return None

    matched = _YEAR_ONLY_PATTERN.search(text)
    if matched:
        return _normalize_year(int(matched.group(1)))

    bare_year_match = _BARE_YEAR_ONLY_PATTERN.search(text)
    return int(bare_year_match.group(1)) if bare_year_match else None


def _extract_capture_seq_filters(
    question: str,
) -> tuple[int | None, int | None]:
    text = str(question or "").strip()
    hospital_seq_match = _HOSPITAL_SEQ_PATTERN.search(text)
    room_seq_match = _HOSPITAL_ROOM_SEQ_PATTERN.search(text)
    return (
        int(hospital_seq_match.group(1)) if hospital_seq_match else None,
        int(room_seq_match.group(1)) if room_seq_match else None,
    )


def _extract_device_seq_filter(question: str) -> int | None:
    matched = _DEVICE_SEQ_PATTERN.search(str(question or "").strip())
    return int(matched.group(1)) if matched else None


def _extract_device_status_filter(question: str) -> str | None:
    matched = _DEVICE_STATUS_PATTERN.search(str(question or "").strip())
    if not matched:
        return None
    candidate = str(matched.group(1) or "").strip().strip("`'\"")
    return candidate or None


def _extract_device_flag_filters(
    question: str,
) -> tuple[int | None, int | None]:
    text = str(question or "").strip()
    lowered = text.lower()

    active_match = _ACTIVE_FLAG_PATTERN.search(text)
    active_flag = int(active_match.group(1)) if active_match else None
    if active_flag is None:
        if any(token in text for token in ("비활성 장비", "비활성된 장비")) or (
            "inactive device" in lowered
        ):
            active_flag = 0
        elif any(token in text for token in ("활성 장비", "활성된 장비")) or (
            "active device" in lowered
        ):
            active_flag = 1

    install_match = _INSTALL_FLAG_PATTERN.search(text)
    install_flag = int(install_match.group(1)) if install_match else None
    if install_flag is None:
        if any(
            token in text
            for token in (
                "미설치 장비",
                "미설치된 장비",
                "설치 안된 장비",
                "설치 안 된 장비",
            )
        ):
            install_flag = 0
        elif any(token in text for token in ("설치 장비", "설치된 장비")):
            install_flag = 1
    return active_flag, install_flag


def _extract_leading_hospital_scope(question: str) -> str | None:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    match = _LEADING_HOSPITAL_KEYWORD_SCOPE_PATTERN.search(text)
    if not match:
        match = _LEADING_HOSPITAL_SCOPE_PATTERN.search(text)
    if not match:
        return None

    candidate = " ".join(match.group(1).split()).strip().strip("`'\"")
    candidate = re.sub(r"(?<!\d)\d{11}(?!\d)", " ", candidate)
    for pattern in (
        _YEAR_ONLY_PATTERN,
        _KOREAN_YMD_PATTERN,
        _NUMERIC_YMD_PATTERN,
        _KOREAN_MD_PATTERN,
        _NUMERIC_MD_PATTERN,
        _NUMERIC_MD_DASH_PATTERN,
        _COMPACT_YYYYMMDD_PATTERN,
        _COMPACT_YYMMDD_PATTERN,
        _COMPACT_MMDD_PATTERN,
    ):
        candidate = pattern.sub(" ", candidate)
    candidate = re.sub(
        r"\b(?:개수|갯수|수|몇\s*개|있나|있는지|있어|유무|존재|조회|목록|"
        r"다운로드|다운|원인|분석|실패|로그)\b",
        " ",
        candidate,
        flags=re.IGNORECASE,
    )
    candidate = " ".join(candidate.split()).strip()
    lowered_candidate = candidate.lower()
    if not candidate:
        return None
    if any(
        token in candidate
        for token in (
            "fileid",
            "capturedat",
            "hospitalseq",
            "hospitalroomseq",
        )
    ):
        return None
    if any(
        token in candidate
        for token in _LEADING_HOSPITAL_SCOPE_QUESTION_TOKENS
    ):
        return None
    if any(
        lowered_candidate.endswith(ending)
        for ending in _LEADING_HOSPITAL_SCOPE_QUESTION_ENDINGS
    ):
        return None
    if "?" in candidate:
        return None
    return candidate


def _is_recordings_filter_query_request(
    question: str,
    *,
    target_date: str | None,
    target_year: int | None,
    hospital_name: str | None,
    room_name: str | None,
    hospital_seq: int | None,
    hospital_room_seq: int | None,
) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = (
        any(token in text for token in cs.VIDEO_HINT_TOKENS)
        or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
        or any(token in text for token in ("초음파", "촬영", "녹화"))
    )
    if not has_video_hint:
        return False
    return any(
        (
            target_year is not None,
            target_date is not None,
            hospital_name,
            room_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
        )
    )


def _is_ultrasound_capture_filter_query_request(
    question: str,
    *,
    barcode: str | None,
    target_date: str | None,
    target_year: int | None,
    hospital_name: str | None,
    room_name: str | None,
    hospital_seq: int | None,
    hospital_room_seq: int | None,
) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    has_capture_hint = any(
        token in text for token in _CAPTURE_HINT_TOKENS
    ) or any(token in lowered for token in _CAPTURE_HINT_TOKENS)
    if not has_capture_hint:
        return False
    return any(
        (
            barcode,
            target_date is not None,
            target_year is not None,
            hospital_name,
            room_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
        )
    )


def _is_hospitals_filter_query_request(
    question: str,
    *,
    target_date: str | None,
    target_year: int | None,
    hospital_name: str | None,
    hospital_seq: int | None,
) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    has_hospital_hint = any(
        token in text for token in _HOSPITAL_QUERY_HINT_TOKENS
    ) or "hospital" in lowered
    has_media_hint = any(
        token in text
        for token in (
            "영상",
            "비디오",
            "녹화",
            "recording",
            "캡처",
            "capture",
            "스냅샷",
            "로그",
            "fileid",
            "파일",
        )
    )
    has_scope = any(
        (
            target_date is not None,
            target_year is not None,
            hospital_name,
            hospital_seq is not None,
        )
    )
    return bool(has_scope and has_hospital_hint and not has_media_hint)


def _is_hospital_rooms_filter_query_request(
    question: str,
    *,
    hospital_name: str | None,
    room_name: str | None,
    hospital_seq: int | None,
    hospital_room_seq: int | None,
) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    has_room_hint = any(
        token in text for token in _ROOMS_QUERY_HINT_TOKENS
    ) or "room" in lowered
    has_media_hint = any(
        token in text
        for token in (
            "영상",
            "비디오",
            "녹화",
            "recording",
            "캡처",
            "capture",
            "스냅샷",
            "snapshot",
            "로그",
            "fileid",
            "파일",
        )
    )
    has_scope = any(
        (
            hospital_name,
            room_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
        )
    )
    has_hospital_context = any(
        (
            hospital_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
        )
    )
    return bool(
        has_scope
        and has_hospital_context
        and has_room_hint
        and not has_media_hint
    )


def _is_devices_filter_query_request(
    question: str,
    *,
    device_name: str | None,
    device_seq: int | None,
    hospital_name: str | None,
    room_name: str | None,
    hospital_seq: int | None,
    hospital_room_seq: int | None,
    status: str | None,
    active_flag: int | None,
    install_flag: int | None,
) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    has_device_hint = any(
        token in text for token in _DEVICE_QUERY_HINT_TOKENS
    ) or any(
        token in lowered
        for token in ("device", "devices", "devicename", "deviceseq")
    )
    if not has_device_hint and device_name:
        has_device_hint = any(
            token in lowered
            for token in ("정보", "상태", "상세", "세부", "온라인", "ssh")
        )
    has_media_hint = any(
        token in text
        for token in (
            "영상",
            "비디오",
            "녹화",
            "recording",
            "캡처",
            "capture",
            "스냅샷",
            "snapshot",
            "로그",
            "fileid",
            "파일",
            "다운로드",
            "복구",
        )
    ) or bool(re.search(r"\blog\b", lowered))
    has_scope = any(
        (
            device_name,
            device_seq is not None,
            hospital_name,
            room_name,
            hospital_seq is not None,
            hospital_room_seq is not None,
            status,
            active_flag is not None,
            install_flag is not None,
        )
    )
    return bool(has_scope and has_device_hint and not has_media_hint)


def _is_barcode_video_count_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = any(
        token in text for token in cs.VIDEO_HINT_TOKENS
    ) or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
    return bool(
        has_video_hint
        and (
            any(token in text for token in cs.VIDEO_COUNT_HINT_TOKENS)
            or "몇" in text
        )
    )


def _is_barcode_video_length_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = (
        any(token in text for token in cs.VIDEO_HINT_TOKENS)
        or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
        or any(token in text for token in ("녹화", "촬영"))
    )
    return has_video_hint and any(
        token in text
        for token in (
            "길이",
            "재생시간",
            "재생 시간",
            "duration",
            "videoLength",
        )
    )


def _is_barcode_last_recorded_at_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = (
        any(token in text for token in cs.VIDEO_HINT_TOKENS)
        or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
        or any(token in text for token in ("녹화", "촬영"))
    )
    has_last_hint = any(
        token in text for token in ("마지막", "최근", "최신")
    ) or any(token in lowered for token in ("last", "latest", "recent"))
    if not (has_video_hint and has_last_hint):
        return False
    # "최신 영상은?"도 기존 계약상 마지막 녹화 시점 조회다.
    return True


def _is_barcode_video_recorded_on_date_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    if _is_barcode_last_recorded_at_request(question, barcode):
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = (
        any(token in text for token in cs.VIDEO_HINT_TOKENS)
        or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
        or any(token in text for token in ("녹화", "촬영", "recordedAt"))
    )
    if not has_video_hint:
        return False
    has_explicit_date, _ = _parse_explicit_date_expression(text)
    return has_explicit_date or _has_relative_date_token(text, lowered)


def _is_barcode_all_recorded_dates_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    if _is_barcode_last_recorded_at_request(question, barcode):
        return False
    if _is_barcode_video_recorded_on_date_request(question, barcode):
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = (
        any(token in text for token in cs.VIDEO_HINT_TOKENS)
        or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
        or any(token in text for token in ("녹화", "촬영", "recordedAt"))
    )
    if not has_video_hint:
        return False
    has_all_hint = any(
        token in text for token in ("모든", "전체", "전부", "다")
    ) or any(token in lowered for token in ("all", "entire"))
    has_per_video_hint = any(
        token in text
        for token in (
            "영상별",
            "비디오별",
            "동영상별",
            "녹화별",
            "촬영별",
            "각 영상",
            "각 녹화",
            "각 촬영",
            "영상마다",
            "녹화마다",
        )
    )
    has_date_list_phrase = any(
        token in text
        for token in (
            "날짜 목록",
            "날짜 리스트",
            "일자 목록",
            "일자 리스트",
            "날짜별 목록",
            "일자별 목록",
            "영상 날짜",
            "영상 날짜 목록",
            "영상 날짜별 목록",
            "영상별 날짜",
            "영상별 날짜 목록",
            "비디오 날짜",
            "비디오 날짜 목록",
            "녹화 날짜",
            "녹화 날짜 목록",
            "촬영 날짜",
            "촬영 날짜 목록",
        )
    )
    has_date_hint = any(
        token in text for token in ("날짜", "일자", "목록", "리스트")
    ) or any(token in lowered for token in ("date", "dates", "list"))
    return bool(
        has_date_hint
        and (has_all_hint or has_per_video_hint or has_date_list_phrase)
    )


def _is_barcode_video_list_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False
    if _is_barcode_video_length_request(question, barcode):
        return False
    if _is_barcode_all_recorded_dates_request(question, barcode):
        return False
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = any(
        token in text for token in cs.VIDEO_HINT_TOKENS
    ) or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
    has_list_hint = any(token in text for token in ("목록", "리스트")) or any(
        token in lowered for token in ("list", "items")
    )
    has_all_date_hint = any(
        token in text for token in ("모든", "전체", "전부", "다")
    ) and any(token in text for token in ("날짜", "일자"))
    return bool(has_video_hint and has_list_hint and not has_all_date_hint)


def _is_barcode_video_info_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    if _is_barcode_video_count_request(question, barcode):
        return False
    if _is_barcode_video_length_request(question, barcode):
        return False
    if _is_barcode_all_recorded_dates_request(question, barcode):
        return False
    text = (question or "").strip()
    lowered = text.lower()
    if "로그" in text or re.search(r"\blog\b", lowered):
        return False
    has_video_hint = any(
        token in text for token in cs.VIDEO_HINT_TOKENS
    ) or any(token in lowered for token in cs.VIDEO_HINT_TOKENS)
    return has_video_hint and any(
        token in text for token in ("정보", "상세", "상세정보", "세부", "상태")
    )


def _is_barcode_baby_ai_list_request(
    question: str,
    barcode: str | None,
) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    has_baby_ai_hint = any(
        token in text for token in _BABY_AI_HINT_TOKENS
    ) or any(token in lowered for token in _BABY_AI_HINT_TOKENS)
    return has_baby_ai_hint and (
        any(token in text for token in ("목록", "리스트", "조회"))
        or any(token in lowered for token in ("list", "items"))
    )


def _is_baby_ai_list_request_without_barcode(
    question: str,
    barcode: str | None,
) -> bool:
    if barcode:
        return False
    return _is_barcode_baby_ai_list_request(question, "without-barcode")


_BARCODE_DATE_EXISTENCE_HINTS = (
    "있어",
    "있나",
    "있는지",
    "있었",
    "유무",
    "여부",
    "존재",
    "됐",
    "되었",
    "된 거",
    "된게",
    "된 게",
    "was recorded",
    "exists",
    "existence",
    "any recording",
    "any video",
)


def match_barcode_query_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """DB/MDA 호출 없이 mutation·PII 경로를 제외해 분류한다."""

    try:
        barcode = resolve_assistant_request_scope(request).barcode
    except AssistantRequestScopeMismatch:
        return None
    question = request.question
    if _is_barcode_pink_classification_reason_request(question, barcode):
        return "barcode_pink_classification_reason"
    if _is_barcode_validation_status_request(question, barcode):
        return "barcode_validation_status"
    if _is_recording_streaming_restore_request(question, barcode):
        return None
    if _is_barcode_video_count_request(question, barcode):
        return "barcode_video_count"
    if _is_baby_ai_list_request_without_barcode(question, barcode):
        return "baby_ai_list"
    if _is_barcode_baby_ai_list_request(question, barcode):
        return "barcode_baby_ai_list"
    if _is_barcode_video_info_request(question, barcode):
        return "barcode_video_info"
    if _is_barcode_video_list_request(question, barcode):
        return "barcode_video_list"
    if _is_barcode_video_length_request(question, barcode):
        return "barcode_video_length"
    if _is_barcode_all_recorded_dates_request(question, barcode):
        return "barcode_all_recorded_dates"
    if _is_barcode_last_recorded_at_request(question, barcode):
        return "barcode last recordedAt"
    if _is_barcode_video_recorded_on_date_request(question, barcode):
        return "barcode recordedAt-on-date"
    return None


def match_common_api_barcode_query_route(
    request: CompanyAssistantRequest,
) -> str | None:
    route = match_barcode_query_route(request)
    return route if route in COMMON_API_BARCODE_QUERY_ROUTES else None


def match_barcode_timeline_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """명시적인 녹화 시점·날짜별 존재 질문만 고른다."""

    route = match_barcode_query_route(request)
    if route == "barcode last recordedAt":
        return route
    if route != "barcode recordedAt-on-date":
        return None
    lowered = request.question.lower()
    if any(
        hint in request.question or hint in lowered
        for hint in _BARCODE_DATE_EXISTENCE_HINTS
    ):
        return route
    return None


_BABY_MAGIC_RESULT_LINK_PATTERN = re.compile(
    r"<(https?://[^>|\s]+)\|([^>]*)>"
)
_BABY_MAGIC_SOURCE_HOST = (
    urlsplit(cs.BABY_MAGIC_CDN_DEFAULT_BASE_URL).hostname or ""
).lower()


def is_safe_baby_magic_source_uri(value: object) -> bool:
    """고정 회사 CDN의 credential 없는 HTTPS 객체만 source로 허용한다."""

    normalized = str(value or "").strip()
    if not normalized or any(
        character.isspace()
        or ord(character) < 32
        or character in "<>|\\"
        for character in normalized
    ):
        return False
    try:
        parsed = urlsplit(normalized)
        port = parsed.port
    except ValueError:
        return False
    return bool(
        parsed.scheme.lower() == "https"
        and parsed.hostname is not None
        and parsed.hostname.lower() == _BABY_MAGIC_SOURCE_HOST
        and port is None
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
        and parsed.path not in {"", "/"}
    )


def _is_weekly_recordings_report_request(
    question: str,
    *,
    barcode: str | None,
) -> bool:
    """주간 리포트 의도를 외부 조회 없이 판정한다."""

    if barcode:
        return False
    text = (question or "").strip()
    if not text:
        return False
    lowered = text.lower()
    has_media_hint = any(
        token in text for token in ("초음파", "영상", "비디오", "동영상", "녹화")
    ) or any(token in lowered for token in ("recording", "recordings"))
    has_summary_hint = any(
        token in text
        for token in (
            "현황",
            "요약",
            "리포트",
            "보고",
            "집계",
            "통계",
            "정리",
            "병원별",
        )
    ) or any(
        token in lowered
        for token in ("summary", "report", "overview", "status")
    )
    has_week_hint = any(
        token in text
        for token in (
            "주간",
            "주별",
            "일주일",
            "한 주",
            "지난주",
            "지난 주",
            "저번주",
            "저번 주",
            "전주",
            "이번주",
            "이번 주",
        )
    ) or any(token in lowered for token in ("weekly", "week"))
    if not (has_media_hint and has_summary_hint and has_week_hint):
        return False
    has_excluded_hint = any(
        token in text
        for token in (
            "바코드",
            "목록",
            "리스트",
            "상세",
            "길이",
            "재생시간",
            "다운로드",
            "복구",
            "로그",
            "캡처",
            "스냅샷",
        )
    ) or any(
        token in lowered
        for token in (
            "list",
            "detail",
            "download",
            "recover",
            "log",
            "capture",
            "captures",
            "snapshot",
            "duration",
            "fileid",
        )
    )
    return not has_excluded_hint


def _resolve_weekly_recordings_report_question_target_date(
    question: str,
    *,
    explicit_target_date: date | None,
    now: datetime | None = None,
) -> date | None:
    if explicit_target_date is not None:
        return explicit_target_date
    normalized = " ".join(str(question or "").split()).lower()
    if any(
        hint in normalized
        for hint in (
            "이번주",
            "이번 주",
            "금주",
            "this week",
            "current week",
        )
    ):
        report_tz = ZoneInfo("Asia/Seoul")
        local_now = datetime.now(report_tz) if now is None else now
        if local_now.tzinfo is None:
            local_now = local_now.replace(tzinfo=report_tz)
        else:
            local_now = local_now.astimezone(report_tz)
        return local_now.date()
    return None


def _extract_weekly_target_date(question: str) -> date | None:
    parsed_date, has_requested_date = _extract_log_date_with_presence(question)
    explicit_target_date = (
        date.fromisoformat(parsed_date) if has_requested_date else None
    )
    return _resolve_weekly_recordings_report_question_target_date(
        question,
        explicit_target_date=explicit_target_date,
    )


def match_weekly_recordings_summary_route(
    request: CompanyAssistantRequest,
) -> str | None:
    try:
        barcode = resolve_assistant_request_scope(request).barcode
    except AssistantRequestScopeMismatch:
        return None
    if _is_weekly_recordings_report_request(
        request.question,
        barcode=barcode,
    ):
        return WEEKLY_RECORDINGS_SUMMARY_ROUTE
    return None


@dataclass(frozen=True, slots=True)
class _StructuredQueryMatch:
    """DB 호출 전에 확정한 구조화 조회 종류와 파싱 결과다."""

    route: str
    barcode: str | None
    target_date: str | None
    target_year: int | None
    hospital_name: str | None
    room_name: str | None
    hospital_seq: int | None
    hospital_room_seq: int | None
    device_name: str | None
    device_seq: int | None
    device_status: str | None
    active_flag: int | None
    install_flag: int | None
    count_only: bool
    date_error: ValueError | None


def _is_generic_count_or_existence_request(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return (
        any(token in text for token in cs.VIDEO_COUNT_HINT_TOKENS)
        or any(
            token in text
            for token in ("있나", "있어", "있는지", "유무", "존재", "몇")
        )
        or "count" in lowered
    )


def _build_structured_query_match(
    request: CompanyAssistantRequest,
    *,
    is_weekly_report_request: Callable[..., bool] = (
        _is_weekly_recordings_report_request
    ),
) -> _StructuredQueryMatch | None:
    """기존 route 우선순위를 보존하며 입력만 파싱한다."""

    question = request.question
    barcode = resolve_assistant_request_scope(request).barcode
    if _is_recording_streaming_restore_request(question, barcode):
        return None
    if _is_barcode_all_recorded_dates_request(question, barcode):
        return None
    if match_barcode_timeline_route(request) is not None:
        return None

    try:
        parsed_date, has_requested_date = _extract_log_date_with_presence(
            question
        )
        target_date = parsed_date if has_requested_date else None
    except ValueError as exc:
        target_date = None
        date_error: ValueError | None = exc
    else:
        date_error = None

    target_year = _extract_year_filter(question)
    if target_year is not None and target_date is None:
        date_error = None
    hospital_name, room_name = _extract_hospital_room_scope(question)
    if not hospital_name:
        hospital_name = _extract_leading_hospital_scope(question)
    hospital_seq, hospital_room_seq = _extract_capture_seq_filters(question)
    device_name = _extract_device_name_scope(question)
    device_seq = _extract_device_seq_filter(question)
    device_status = _extract_device_status_filter(question)
    active_flag, install_flag = _extract_device_flag_filters(question)
    count_only = _is_generic_count_or_existence_request(question)

    route: str | None = None
    if _is_hospitals_filter_query_request(
        question,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        hospital_seq=hospital_seq,
    ):
        route = "hospitals_filter"
    elif _is_hospital_rooms_filter_query_request(
        question,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
    ):
        route = "hospital_rooms_filter"
    elif _is_devices_filter_query_request(
        question,
        device_name=device_name,
        device_seq=device_seq,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
        status=device_status,
        active_flag=active_flag,
        install_flag=install_flag,
    ):
        route = "devices_filter"
    elif is_weekly_report_request(
        question,
        barcode=barcode,
    ):
        return None
    elif _is_ultrasound_capture_filter_query_request(
        question,
        barcode=barcode,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
    ):
        route = "ultrasound_captures_filter"
    elif _is_recordings_filter_query_request(
        question,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
    ):
        route = "recordings_filter"

    if route is None:
        return None
    return _StructuredQueryMatch(
        route=route,
        barcode=barcode,
        target_date=target_date,
        target_year=target_year,
        hospital_name=hospital_name,
        room_name=room_name,
        hospital_seq=hospital_seq,
        hospital_room_seq=hospital_room_seq,
        device_name=device_name,
        device_seq=device_seq,
        device_status=device_status,
        active_flag=active_flag,
        install_flag=install_flag,
        count_only=count_only,
        date_error=date_error,
    )


def match_structured_read_route(
    request: CompanyAssistantRequest,
) -> str | None:
    try:
        matched = _build_structured_query_match(request)
    except AssistantRequestScopeMismatch:
        return None
    return matched.route if matched is not None else None


def match_structured_device_count_route(
    request: CompanyAssistantRequest,
) -> str | None:
    try:
        matched = _build_structured_query_match(request)
    except AssistantRequestScopeMismatch:
        return None
    if (
        matched is not None
        and matched.route == "devices_filter"
        and matched.count_only
    ):
        return matched.route
    return None


_DEVICE_DETAIL_ROUTE_GROUP = "device_detail"
_LIVE_DEVICE_INTENT_TOKENS = (
    "온라인",
    "오프라인",
    "연결 상태",
    "연결상태",
    "연결 확인",
    "접속 상태",
    "접속상태",
    "접속 확인",
    "응답 확인",
    "마지막 보고",
    "최근 보고",
    "heartbeat",
    "last seen",
    "lastseen",
    "reachable",
    "connectivity",
    "uptime",
    "health check",
    "healthcheck",
    "status probe",
    "online",
    "offline",
    "connected",
    "disconnected",
    "connection check",
    "핑",
    "ping",
    "pong",
    "ssh",
    "mda",
    "엠디에이",
    "원격 접속",
    "원격접속",
    "버전",
    "version",
    "캡처보드",
    "캡쳐보드",
    "캡처 카드",
    "캡쳐 카드",
    "captureboard",
    "capture board",
    "capture card",
    "pm2",
    "프로세스 상태",
    "프로세스상태",
    "process status",
    "삭제",
    "수정",
    "변경",
    "업데이트",
    "바꿔",
    "재부팅",
    "재시작",
    "종료",
    "전원",
    "초기화",
    "등록",
    "해제",
)
_LIVE_STATUS_PROBE_PATTERNS = (
    re.compile(r"(?:현재|지금|실시간).{0,12}상태", re.IGNORECASE),
    re.compile(r"상태.{0,12}(?:현재|지금|실시간)", re.IGNORECASE),
    re.compile(
        r"(?:장비\s*)?상태\s*"
        r"(?:확인|체크|점검|봐|보여|알려|어때|어떤|정상|작동|동작)",
        re.IGNORECASE,
    ),
    re.compile(r"(?:장비\s*)?상태\s*[?!.,~]*$", re.IGNORECASE),
    re.compile(
        r"(?:device\s+)?status\s*(?:check|probe|show|tell|what|\?)",
        re.IGNORECASE,
    ),
    re.compile(r"(?:device\s+)?status\s*[?!.,~]*$", re.IGNORECASE),
    re.compile(
        r"장비.{0,12}정상\s*(?:이야|인가|맞아|해|한지|인지|\?)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:정상\s*)?(?:작동|동작)\s*(?:중|해|하나|하니|하는지)",
        re.IGNORECASE,
    ),
    re.compile(r"(?:살아\s*있|alive|responding)", re.IGNORECASE),
)
_DEVICE_NAME_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])([A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*)(?![A-Za-z0-9])",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class _DeviceDbDetailMatch:
    """구조화 parser가 확정한 장비 상세 조회 인자다."""

    device_name: str | None
    device_seq: int | None
    hospital_name: str | None
    room_name: str | None
    hospital_seq: int | None
    hospital_room_seq: int | None
    device_status: str | None
    active_flag: int | None
    install_flag: int | None


def _has_live_device_intent(question: str) -> bool:
    normalized = " ".join(str(question or "").split())
    lowered = normalized.lower()
    if any(token in lowered for token in _LIVE_DEVICE_INTENT_TOKENS):
        return True
    return any(pattern.search(normalized) for pattern in _LIVE_STATUS_PROBE_PATTERNS)


def _build_device_query_match(
    request: CompanyAssistantRequest,
) -> _DeviceDbDetailMatch | None:
    parsed = _build_structured_query_match(request)
    if (
        parsed is None
        or parsed.route != "devices_filter"
        or parsed.count_only
    ):
        return None
    return _DeviceDbDetailMatch(
        device_name=parsed.device_name,
        device_seq=parsed.device_seq,
        hospital_name=parsed.hospital_name,
        room_name=parsed.room_name,
        hospital_seq=parsed.hospital_seq,
        hospital_room_seq=parsed.hospital_room_seq,
        device_status=parsed.device_status,
        active_flag=parsed.active_flag,
        install_flag=parsed.install_flag,
    )


def _build_device_db_detail_match(
    request: CompanyAssistantRequest,
) -> _DeviceDbDetailMatch | None:
    if _has_live_device_intent(request.question):
        return None
    return _build_device_query_match(request)


def _build_device_detail_match(
    request: CompanyAssistantRequest,
) -> _DeviceDbDetailMatch | None:
    matched = _build_device_query_match(request)
    if matched is not None:
        return matched
    explicit_name_match = _DEVICE_NAME_TOKEN_PATTERN.search(request.question)
    if explicit_name_match is None or not _has_live_device_intent(
        request.question
    ):
        return None
    return _DeviceDbDetailMatch(
        device_name=explicit_name_match.group(1),
        device_seq=None,
        hospital_name=None,
        room_name=None,
        hospital_seq=None,
        hospital_room_seq=None,
        device_status=None,
        active_flag=None,
        install_flag=None,
    )


def _is_exact_device_detail_match(matched: _DeviceDbDetailMatch) -> bool:
    has_exact_identifier = bool(
        matched.device_name or matched.device_seq is not None
    )
    has_list_filter = any(
        (
            matched.hospital_name,
            matched.room_name,
            matched.hospital_seq is not None,
            matched.hospital_room_seq is not None,
            matched.device_status,
            matched.active_flag is not None,
            matched.install_flag is not None,
        )
    )
    return has_exact_identifier and not has_list_filter


def _is_exact_device_detail_request(
    matched: _DeviceDbDetailMatch,
) -> bool:
    return bool(matched.device_name and _is_exact_device_detail_match(matched))


def match_device_detail_route(
    request: CompanyAssistantRequest,
) -> str | None:
    try:
        matched = _build_device_detail_match(request)
    except AssistantRequestScopeMismatch:
        return None
    if matched is None:
        return None
    return (
        _DEVICE_DETAIL_ROUTE_GROUP
        if _is_exact_device_detail_request(matched)
        else "devices_filter"
    )


def _primitive_scope_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.split()).strip()
    return normalized or None


def _metadata_followup_kind(
    request: CompanyAssistantRequest,
) -> str | None:
    value = request.metadata.get("followup_kind")
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _resolve_barcode_log_barcode(
    request: CompanyAssistantRequest,
) -> str | None:
    metadata_barcode = _primitive_scope_text(request.metadata.get("barcode"))
    if metadata_barcode:
        return metadata_barcode
    direct_barcode = _extract_barcode(request.question)
    if direct_barcode:
        return direct_barcode
    for entry in reversed(window_assistant_context_entries(request)):
        author_id = str(entry.get("author_id") or "").strip()
        if not request.actor_id or author_id != request.actor_id:
            continue
        recovered = _extract_barcode(str(entry.get("text") or ""))
        if recovered:
            return recovered
    return None


def _resolve_barcode_log_manual_scope(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    parsed_hospital, parsed_room = _extract_hospital_room_scope(
        request.question
    )
    hospital_name = (
        _primitive_scope_text(request.metadata.get("hospital_name"))
        or _primitive_scope_text(request.metadata.get("phase2_hospital_name"))
        or parsed_hospital
    )
    room_name = (
        _primitive_scope_text(request.metadata.get("room_name"))
        or _primitive_scope_text(request.metadata.get("phase2_room_name"))
        or parsed_room
    )
    return hospital_name, room_name


def _context_has_log_request(request: CompanyAssistantRequest) -> bool:
    return any(
        (
            "로그" in str(entry.get("text") or "")
            or bool(
                re.search(
                    r"\blog\b",
                    str(entry.get("text") or "").lower(),
                )
            )
        )
        for entry in window_assistant_context_entries(request)
        if (
            request.actor_id
            and str(entry.get("author_id") or "").strip()
            == request.actor_id
        )
    )


def _looks_like_scope_followup(
    *,
    barcode: str | None,
    hospital_name: str | None,
    room_name: str | None,
    has_context_log_request: bool,
) -> bool:
    return bool(
        barcode and hospital_name and room_name and has_context_log_request
    )


def match_barcode_log_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """직접 로그 요청과 동일 actor의 2차 범위 입력만 고른다."""

    try:
        scope = resolve_assistant_request_scope(request)
    except AssistantRequestScopeMismatch:
        return None
    barcode = scope.barcode or _resolve_barcode_log_barcode(request)
    hospital_name = scope.hospital_name
    room_name = scope.room_name
    if not (hospital_name and room_name):
        hospital_name, room_name = _resolve_barcode_log_manual_scope(request)
    if _is_barcode_log_analysis_request(request.question, barcode):
        return "barcode_log_analysis"

    has_context_log_request = bool(
        _context_has_log_request(request)
        or _metadata_followup_kind(request) == "barcode_log"
    )
    try:
        _, has_scope_date = _extract_log_date_with_presence(request.question)
    except ValueError:
        has_scope_date = _looks_like_scope_followup(
            barcode=barcode,
            hospital_name=hospital_name,
            room_name=room_name,
            has_context_log_request=has_context_log_request,
        )
    if has_scope_date and _looks_like_scope_followup(
        barcode=barcode,
        hospital_name=hospital_name,
        room_name=room_name,
        has_context_log_request=has_context_log_request,
    ):
        return "barcode_log_analysis"
    return None


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


def _has_recording_failure_analysis_hints(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(
        hint in text or hint in lowered for hint in _FAILURE_ANALYSIS_HINTS
    )


def _is_recording_failure_analysis_request(
    question: str,
    barcode: str | None,
) -> bool:
    return bool(barcode and _has_recording_failure_analysis_hints(question))


def _metadata_text(
    request: CompanyAssistantRequest,
    *keys: str,
) -> str | None:
    for key in keys:
        value = request.metadata.get(key)
        if isinstance(value, str):
            normalized = value.strip()
            if normalized:
                return normalized
    return None


def _metadata_bool(
    request: CompanyAssistantRequest,
    *keys: str,
) -> bool:
    return any(request.metadata.get(key) is True for key in keys)


def _recording_failure_context_text(
    request: CompanyAssistantRequest,
) -> str:
    return "\n".join(
        str(entry.get("text") or "").strip()
        for entry in window_assistant_context_entries(request)
        if (
            isinstance(entry, Mapping)
            and request.actor_id
            and str(entry.get("author_id") or "").strip()
            == request.actor_id
            and str(entry.get("text") or "").strip()
        )
    )


def _resolve_recording_failure_barcode(
    request: CompanyAssistantRequest,
) -> str | None:
    explicit = _metadata_text(request, "barcode") or _extract_barcode(
        request.question
    )
    if explicit:
        return explicit
    for entry in reversed(window_assistant_context_entries(request)):
        if not isinstance(entry, Mapping):
            continue
        author_id = str(entry.get("author_id") or "").strip()
        if not request.actor_id or author_id != request.actor_id:
            continue
        barcode = _extract_barcode(str(entry.get("text") or ""))
        if barcode:
            return barcode
    return None


def _resolve_recording_failure_hospital_room(
    request: CompanyAssistantRequest,
) -> tuple[str | None, str | None]:
    question_hospital, question_room = _extract_hospital_room_scope(
        request.question
    )
    hospital_name = (
        _metadata_text(
            request,
            "hospital_name",
            "hospitalName",
            "phase2_hospital_name",
            "phase2HospitalName",
        )
        or question_hospital
    )
    room_name = (
        _metadata_text(
            request,
            "room_name",
            "roomName",
            "phase2_room_name",
            "phase2RoomName",
        )
        or question_room
    )
    return hospital_name, room_name


def _resolve_recording_failure_log_date(
    request: CompanyAssistantRequest,
) -> tuple[str, bool]:
    log_date, has_requested_date = _extract_log_date_with_presence(
        request.question
    )
    metadata_date = _metadata_text(request, "log_date", "logDate")
    if has_requested_date or not metadata_date:
        return log_date, has_requested_date
    try:
        return date.fromisoformat(metadata_date).isoformat(), True
    except ValueError as exc:
        raise ValueError("날짜는 YYYY-MM-DD 형식으로 입력해줘") from exc


def _is_failure_scope_followup(
    request: CompanyAssistantRequest,
    *,
    barcode: str | None,
    hospital_name: str | None,
    room_name: str | None,
    has_requested_date: bool,
    context_text: str,
) -> bool:
    explicit_followup = _metadata_bool(
        request,
        "is_failure_phase2_scope_followup",
        "isFailurePhase2ScopeFollowup",
    ) or _metadata_followup_kind(request) == "recording_failure"
    return bool(
        barcode
        and hospital_name
        and room_name
        and has_requested_date
        and (
            explicit_followup
            or _has_recording_failure_analysis_hints(context_text)
        )
    )


def match_recording_failure_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """녹화 실패 분석 의도와 신뢰 가능한 후속 문맥만 확인한다."""

    try:
        resolve_assistant_request_scope(request)
    except AssistantRequestScopeMismatch:
        return None
    barcode = _resolve_recording_failure_barcode(request)
    hospital_name, room_name = _resolve_recording_failure_hospital_room(
        request
    )
    context_text = _recording_failure_context_text(request)
    direct_match = _is_recording_failure_analysis_request(
        request.question,
        barcode,
    )
    explicit_followup = _metadata_bool(
        request,
        "is_failure_phase2_scope_followup",
        "isFailurePhase2ScopeFollowup",
    ) or _metadata_followup_kind(request) == "recording_failure"
    contextual_followup = bool(
        barcode
        and hospital_name
        and room_name
        and _has_recording_failure_analysis_hints(context_text)
    )
    if direct_match:
        return "recording_failure_analysis"
    if not (explicit_followup or contextual_followup):
        return None
    try:
        _, has_requested_date = _resolve_recording_failure_log_date(request)
    except ValueError:
        return "recording_failure_analysis"
    if _is_failure_scope_followup(
        request,
        barcode=barcode,
        hospital_name=hospital_name,
        room_name=room_name,
        has_requested_date=has_requested_date,
        context_text=context_text,
    ):
        return "recording_failure_analysis"
    return None


_BARCODE_EVIDENCE_FREEFORM_ROUTE = "barcode_evidence_freeform"
_BARCODE_EVIDENCE_SCOPE_HINTS = (
    "이 바코드",
    "해당 바코드",
    "그 바코드",
    "위 바코드",
    "방금 바코드",
)
_BARCODE_EVIDENCE_SUBJECT_HINTS = (
    "녹화",
    "영상",
    "촬영",
    "업로드",
    "recording",
    "recordings",
)
_BARCODE_EVIDENCE_BASIS_HINTS = (
    "근거",
    "기록",
    "이력",
    "데이터",
    "recordedat",
    "recorded at",
    " row",
    " rows",
)
_BARCODE_EVIDENCE_INTERPRETATION_HINTS = (
    "설명",
    "분석",
    "판단",
    "확인",
    "비교",
    "경향",
    "간격",
    "정상",
    "이상",
    "문제",
    "성공",
    "실패",
    "원인",
    "왜",
    "어때",
    "어떻게",
)
_BARCODE_EVIDENCE_PII_HINTS = (
    "유저",
    "사용자",
    "산모",
    "회원",
    "환자",
    "개인정보",
    "전화번호",
    "휴대폰",
    "이메일",
    "생년월일",
    "출산예정일",
    "태아",
    "app user",
    "app-user",
    "user email",
    "email",
    "phone number",
    "phone",
    "mobile",
    "patient",
    "personal data",
    "date of birth",
    "birth date",
    "due date",
    "lambda",
    "람다",
)
_BARCODE_EVIDENCE_MUTATION_HINTS = (
    "다운로드",
    "복구",
    "복원",
    "삭제",
    "수정",
    "변경",
    "업데이트",
    "재부팅",
    "재시작",
    "전원 꺼",
    "꺼줘",
    "켜줘",
    "명령 실행",
    "전송해",
    "전송 해",
    "실행해",
    "실행 해",
    "보내줘",
    "보내 줘",
    "발송해",
    "발송 해",
    "원격 접속",
    "download",
    "recover",
    "recovery",
    "restore",
    "delete",
    "remove",
    "modify",
    "edit",
    "update",
    "upgrade",
    "reboot",
    "restart",
    "shutdown",
    "power off",
    "turn off",
    "turn on",
    "run command",
    "execute",
    "remote access",
    "ssh",
)
_BARCODE_EVIDENCE_LIVE_HINTS = (
    "온라인",
    "오프라인",
    "연결 상태",
    "실시간",
    "현재 상태",
    "버전",
    "캡처 보드",
    "캡처보드",
    "캡쳐 카드",
    "캡쳐카드",
    "엠디에이",
    "online",
    "offline",
    "connection status",
    "live status",
    "current status",
    "real-time",
    "realtime",
    "version",
    "capture board",
    "capture card",
    "mda",
    "pm2",
)


def match_barcode_evidence_freeform_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """명시적인 recordings 근거 해석 요청만 마지막 read stage로 고른다."""

    try:
        barcode = resolve_assistant_request_scope(request).barcode
    except AssistantRequestScopeMismatch:
        return None
    question = (request.question or "").strip()
    lowered = question.lower()
    if not barcode or not question:
        return None
    if barcode not in question and not any(
        hint in question for hint in _BARCODE_EVIDENCE_SCOPE_HINTS
    ):
        return None

    context_text = _request_context_text(request)
    if is_prompt_exfiltration_attempt(question, context_text):
        return None
    if _looks_like_company_notion_search(question):
        return None
    if match_notion_playbook_route(request) is not None:
        return None
    if any(hint in lowered for hint in _BARCODE_EVIDENCE_PII_HINTS):
        return None
    if any(hint in lowered for hint in _BARCODE_EVIDENCE_MUTATION_HINTS):
        return None
    if any(hint in lowered for hint in _BARCODE_EVIDENCE_LIVE_HINTS):
        return None
    if _should_analyze_app_user_baby_selection(question, barcode):
        return None
    if _should_lookup_barcode(question, barcode):
        return None
    if _is_recording_streaming_restore_request(question, barcode):
        return None

    device_name = _extract_device_name_for_diagnostic_freeform(question)
    if (
        _has_device_diagnostic_start_hint(question)
        or _select_device_diagnostic_followup_command_keys(question)
        or _is_device_diagnostic_freeform_request(
            question,
            device_name=device_name,
        )
    ):
        return None
    try:
        earlier_route = next(
            (
                route
                for route in (
                    match_device_read_route(request),
                    match_recording_failure_route(request),
                    match_barcode_log_route(request),
                    match_structured_read_route(request),
                    match_barcode_query_route(request),
                )
                if route is not None
            ),
            None,
        )
    except Exception:
        # 사전 matcher가 예상 밖 입력을 받으면 원격 fallback으로
        # 넓히지 않는다.
        return None
    if earlier_route is not None:
        return None

    has_recording_subject = any(
        hint in lowered for hint in _BARCODE_EVIDENCE_SUBJECT_HINTS
    )
    has_evidence_basis = any(
        hint in lowered for hint in _BARCODE_EVIDENCE_BASIS_HINTS
    )
    has_interpretation_intent = any(
        hint in lowered
        for hint in _BARCODE_EVIDENCE_INTERPRETATION_HINTS
    )
    if not (
        has_recording_subject
        and has_evidence_basis
        and has_interpretation_intent
    ):
        return None
    return _BARCODE_EVIDENCE_FREEFORM_ROUTE


def match_company_freeform_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """adapter가 명시한 마지막 자유대화 stage만 처리한다."""

    route_group = str(request.metadata.get("route_group") or "").strip()
    if route_group != "freeform":
        return None
    if match_company_operation_route(request) is not None:
        return None
    return "company_freeform"


def _normalize_usage_help_question(question: str) -> str:
    """사용법 명령의 기존 공백·끝 문장부호 허용 규칙을 보존한다."""

    normalized = " ".join(str(question or "").strip().split())
    return normalized.rstrip("!?.")


def is_usage_help_question(question: str) -> bool:
    """provider 실행 없이 회사 사용법 명령인지 판정한다."""

    normalized = _normalize_usage_help_question(question)
    return bool(
        normalized and _USAGE_HELP_PATTERN.fullmatch(normalized)
    )


def match_usage_help_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """API가 명시적으로 연 freeform stage에서만 사용법을 고른다."""

    route_group = str(request.metadata.get("route_group") or "").strip()
    if route_group != "freeform":
        return None
    if not is_usage_help_question(request.question):
        return None
    return "usage_help"


def match_usage_help_rollout_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """Slack 원문을 실행 없이 사용법 API stage로 재분류한다."""

    metadata = dict(request.metadata)
    metadata["route_group"] = "freeform"
    return match_usage_help_route(
        replace(request, metadata=metadata)
    )


__all__ = [
    "AssistantRequestScopeMismatch",
    "BARCODE_TIMELINE_ROUTES",
    "COMMON_API_BARCODE_QUERY_ROUTES",
    "WEEKLY_RECORDINGS_SUMMARY_ROUTE",
    "_DeviceDbDetailMatch",
    "_StructuredQueryMatch",
    "_build_device_db_detail_match",
    "_build_device_detail_match",
    "_build_structured_query_match",
    "_extract_barcode",
    "_extract_capture_seq_filters",
    "_extract_device_flag_filters",
    "_extract_device_name_scope",
    "_extract_device_seq_filter",
    "_extract_device_status_filter",
    "_extract_hospital_room_scope",
    "_extract_leading_hospital_scope",
    "_extract_log_date",
    "_extract_log_date_with_presence",
    "_extract_year_filter",
    "_has_recording_failure_analysis_hints",
    "_is_baby_ai_list_request_without_barcode",
    "_is_barcode_all_recorded_dates_request",
    "_is_barcode_baby_ai_list_request",
    "_is_barcode_last_recorded_at_request",
    "_is_barcode_log_analysis_request",
    "_is_barcode_video_count_request",
    "_is_barcode_video_info_request",
    "_is_barcode_video_length_request",
    "_is_barcode_video_list_request",
    "_is_barcode_video_recorded_on_date_request",
    "_is_devices_filter_query_request",
    "_is_exact_device_detail_request",
    "_is_generic_count_or_existence_request",
    "_is_hospital_rooms_filter_query_request",
    "_is_hospitals_filter_query_request",
    "_is_recording_failure_analysis_request",
    "_is_recordings_filter_query_request",
    "_is_ultrasound_capture_filter_query_request",
    "_is_weekly_recordings_report_request",
    "_looks_like_company_notion_search",
    "_normalize_spaces",
    "_resolve_weekly_recordings_report_question_target_date",
    "is_safe_baby_magic_source_uri",
    "is_usage_help_question",
    "looks_like_notion_playbook_followup",
    "looks_like_notion_playbook_question",
    "match_barcode_evidence_freeform_route",
    "match_barcode_log_route",
    "match_barcode_query_route",
    "match_barcode_timeline_route",
    "match_common_api_barcode_query_route",
    "match_company_freeform_route",
    "match_device_detail_route",
    "match_device_read_route",
    "match_notion_playbook_route",
    "match_recording_failure_route",
    "match_structured_device_count_route",
    "match_structured_read_route",
    "match_usage_help_rollout_route",
    "match_usage_help_route",
    "match_weekly_recordings_summary_route",
    "resolve_assistant_request_scope",
    "window_assistant_context_entries",
]
