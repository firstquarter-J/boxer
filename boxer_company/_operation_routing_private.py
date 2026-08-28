"""Admin·app-user·barcode operation의 provider-free 분류 정본이다."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from boxer_company._operation_routing_common import (
    AssistantRequestScopeMismatch,
    CompanyOperationRequestContract as CompanyAssistantRequest,
    _extract_barcode,
    _normalize_spaces,
    company_settings as cs,
    core_settings as s,
    resolve_assistant_request_scope,
)
from boxer_company._operation_routing_file import (
    _is_recording_streaming_restore_request,
)


def _extract_db_query(question: str) -> str | None:
    normalized = (question or "").strip()
    lowered = normalized.lower()
    if lowered.startswith("db 조회"):
        return normalized[5:].strip()
    if lowered.startswith("db조회"):
        return normalized[4:].strip()
    return None


_BABY_SELECTION_CONTEXT_KEYWORDS = (
    "유저 조회",
    "유저조회",
    "산모 조회",
    "산모조회",
    "람다",
    "lambda",
)


_BABY_SELECTION_ISSUE_KEYWORDS = (
    "안 나",
    "안나",
    "누락",
    "한 명만",
    "한명만",
    "하나만",
    "선택",
)


_BABY_SELECTION_ANALYSIS_KEYWORDS = (
    "원인",
    "왜",
    "분석",
)


def _contains_any(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _should_analyze_app_user_baby_selection(
    question: str,
    barcode: str,
) -> bool:
    normalized = (question or "").strip().lower()
    if not normalized or barcode not in normalized:
        return False
    return _contains_any(
        normalized,
        _BABY_SELECTION_CONTEXT_KEYWORDS,
    ) and _contains_any(
        normalized,
        _BABY_SELECTION_ISSUE_KEYWORDS,
    ) and _contains_any(
        normalized,
        _BABY_SELECTION_ANALYSIS_KEYWORDS,
    )


def _should_lookup_barcode(question: str, barcode: str) -> bool:
    normalized = (question or "").strip()
    lookup_keywords = ("유저 조회", "유저조회", "산모 조회", "산모조회")

    non_profile_hints_ko = ("영상", "녹화", "촬영", "로그", "개수", "갯수", "최신", "마지막")
    has_non_profile_hint = _contains_any(normalized, non_profile_hints_ko)
    has_lookup_keyword = _contains_any(normalized, lookup_keywords)
    if has_non_profile_hint and not has_lookup_keyword:
        return False

    if normalized.startswith(barcode):
        suffix = normalized[len(barcode) :].strip()
        return suffix in lookup_keywords

    return has_lookup_keyword


_BARCODE_VALIDATION_CONTEXT_HINTS = (
    "유효성 검사",
    "유효성 검증",
    "바코드 검증",
    "걸리는 바코드",
    "막히는 바코드",
    "차단 대상",
    "제한 대상",
    "special_barcodes",
    "special barcode",
)


_BARCODE_VALIDATION_STATUS_HINTS = (
    "걸리",
    "막히",
    "차단",
    "제한",
    "통과",
    "허용",
    "무료 바코드",
    "무료바코드",
    "핑크 바코드",
    "핑크바코드",
    "pink barcode",
    "FREE",
    "환불 바코드",
    "환불바코드",
    "환불",
    "REFUND",
    "refund",
)


_BARCODE_VALIDATION_POLICY_HINTS = (
    "기준",
    "정책",
    "규칙",
    "방법",
)


_PINK_CLASSIFICATION_REASON_HINTS = (
    "왜",
    "이유",
    "분류되지",
    "분류 안",
    "기록되지",
    "기록 안",
    "등록되지",
    "등록 안",
    "안됐",
    "안 됐",
    "누락",
    "대조",
    "첫 녹화",
    "첫녹화",
)


_PINK_CLASSIFICATION_SUBJECT_HINTS = (
    "핑크",
    "무료",
    "FREE",
    "special_barcodes",
    "special barcode",
)


def _contains_any_hint(question: str, hints: tuple[str, ...]) -> bool:
    normalized = str(question or "").strip()
    lowered = normalized.lower()
    return any(hint in normalized or hint.lower() in lowered for hint in hints)


def _is_barcode_validation_status_request(question: str, barcode: str | None) -> bool:
    if not str(barcode or "").strip():
        return False
    normalized = str(question or "").strip()
    if not normalized:
        return False
    has_status_hint = _contains_any_hint(
        normalized,
        _BARCODE_VALIDATION_STATUS_HINTS,
    )
    if (
        _contains_any_hint(normalized, _BARCODE_VALIDATION_POLICY_HINTS)
        and not has_status_hint
    ):
        # thread에 바코드가 있어도 "검증 기준" 같은 플레이북 질문은
        # 현재 바코드의 MDA 상태 조회로 선점하지 않는다.
        return False
    if _contains_any_hint(normalized, _BARCODE_VALIDATION_CONTEXT_HINTS):
        return True
    return has_status_hint


def _is_barcode_pink_classification_reason_request(question: str, barcode: str | None) -> bool:
    if not str(barcode or "").strip():
        return False
    normalized = str(question or "").strip()
    if not normalized:
        return False
    return _contains_any_hint(
        normalized,
        _PINK_CLASSIFICATION_SUBJECT_HINTS,
    ) and _contains_any_hint(normalized, _PINK_CLASSIFICATION_REASON_HINTS)


_REQUEST_LOG_PREFIXES = (
    "요청 로그",
    "요청로그",
    "request log",
    "requestlog",
)


_REQUEST_LOG_OVERVIEW_PREFIXES = (
    "요청 통계",
    "요청통계",
)


_REQUEST_LOG_DATE_PATTERN = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")


_REQUEST_LOG_LIMIT_PATTERN = re.compile(r"(?<![-\d])([1-9]\d?)(?![-\d])")


@dataclass(frozen=True)
class RequestLogQuerySpec:
    mode: str
    target_date: str | None
    scope_label: str
    limit: int
    user_query: str | None = None


def _request_log_timezone() -> ZoneInfo:
    return ZoneInfo(s.REQUEST_LOG_TIMEZONE)


def _request_log_today() -> str:
    return datetime.now(_request_log_timezone()).date().isoformat()


def _request_log_yesterday() -> str:
    return (datetime.now(_request_log_timezone()).date() - timedelta(days=1)).isoformat()


def _extract_request_log_query(question: str) -> RequestLogQuerySpec | None:
    normalized = str(question or "").strip()
    if not normalized:
        return None

    remainder = ""
    matched_prefix = ""
    for prefix in (*_REQUEST_LOG_PREFIXES, *_REQUEST_LOG_OVERVIEW_PREFIXES):
        if normalized.lower().startswith(prefix.lower()):
            matched_prefix = prefix
            remainder = normalized[len(prefix):].strip()
            break
    else:
        return None

    lowered_remainder = remainder.lower()
    if remainder.startswith("최근") or lowered_remainder.startswith("recent"):
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="recent",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=100, max_limit=100),
        )

    if (
        remainder.startswith("사용자")
        or remainder.startswith("유저")
        or lowered_remainder.startswith("user")
    ):
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="users",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=10, max_limit=20),
        )

    if (
        remainder.startswith("라우트")
        or remainder.startswith("경로")
        or lowered_remainder.startswith("route")
    ):
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="routes",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=10, max_limit=20),
        )

    user_query = _extract_request_log_user_query(remainder)
    if matched_prefix in _REQUEST_LOG_OVERVIEW_PREFIXES:
        target_date, scope_label = _extract_request_log_scope(
            remainder,
            default_scope="today",
        )
        return RequestLogQuerySpec(
            mode="overview",
            target_date=target_date,
            scope_label=scope_label,
            limit=_extract_request_log_limit(remainder, default=5, max_limit=10),
        )

    target_date, scope_label = _extract_request_log_scope(
        remainder,
        default_scope="today",
    )
    return RequestLogQuerySpec(
        mode="recent",
        target_date=target_date,
        scope_label=scope_label,
        limit=_extract_request_log_limit(remainder, default=100, max_limit=100),
        user_query=user_query,
    )


def _extract_request_log_user_query(text: str) -> str | None:
    normalized = str(text or "").strip()
    if not normalized:
        return None

    working = _REQUEST_LOG_DATE_PATTERN.sub(" ", normalized)
    working = re.sub(
        r"\b(today|yesterday|all|recent|user|users|route|routes|summary|overview)\b",
        " ",
        working,
        flags=re.IGNORECASE,
    )
    working = re.sub(r"(?<![-\d])[1-9]\d?(?![-\d])", " ", working)
    for token in (
        "오늘",
        "어제",
        "전체",
        "누적",
        "최근",
        "사용자",
        "유저",
        "라우트",
        "경로",
        "요약",
        "통계",
    ):
        working = working.replace(token, " ")
    compact = " ".join(working.split()).strip(" ,")
    return compact or None


def _extract_request_log_scope(
    text: str,
    *,
    default_scope: str,
) -> tuple[str | None, str]:
    normalized = str(text or "").strip()
    lowered = normalized.lower()

    date_match = _REQUEST_LOG_DATE_PATTERN.search(normalized)
    if date_match:
        target_date = date_match.group(1)
        return target_date, f"`{target_date}`"

    if "어제" in normalized or "yesterday" in lowered:
        target_date = _request_log_yesterday()
        return target_date, f"어제 (`{target_date}`)"

    if "오늘" in normalized or "today" in lowered:
        target_date = _request_log_today()
        return target_date, f"오늘 (`{target_date}`)"

    if "전체" in normalized or "누적" in normalized or "all" in lowered:
        return None, "전체 누적"

    if default_scope == "today":
        target_date = _request_log_today()
        return target_date, f"오늘 (`{target_date}`)"
    return None, "전체 누적"


def _extract_request_log_limit(
    text: str,
    *,
    default: int,
    max_limit: int,
) -> int:
    match = _REQUEST_LOG_LIMIT_PATTERN.search(text or "")
    if not match:
        return default
    return min(max(1, int(match.group(1))), max_limit)


def _extract_s3_log_request(normalized_question: str) -> dict[str, str]:
    path_match = cs.S3_LOG_PATH_PATTERN.search(normalized_question)
    if path_match:
        device_name = path_match.group(1)
        log_date = path_match.group(2)
        return {"kind": "log", "device_name": device_name, "log_date": log_date}

    tokens = [token.strip().strip("`'\",.()[]{}") for token in normalized_question.split()]
    date_token = ""
    for token in tokens:
        if cs.S3_LOG_DATE_TOKEN_PATTERN.match(token):
            date_token = token
            break
        file_match = cs.S3_LOG_FILE_TOKEN_PATTERN.match(token)
        if file_match:
            date_token = file_match.group(1)
            break

    if not date_token:
        raise ValueError("로그 조회는 날짜가 필요해. 예: s3 로그 <device-name> 2026-03-04")
    try:
        datetime.strptime(date_token, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("날짜 형식은 YYYY-MM-DD로 입력해줘") from exc

    device_name = ""
    for token in tokens:
        if not token:
            continue
        if token == date_token:
            continue
        lowered = token.lower()
        if lowered in cs.S3_LOG_RESERVED_TOKENS:
            continue
        if cs.S3_LOG_FILE_TOKEN_PATTERN.match(token):
            continue
        if "/" in token:
            prefix, suffix = token.split("/", 1)
            if cs.S3_LOG_FILE_TOKEN_PATTERN.match(suffix) and cs.S3_DEVICE_NAME_PATTERN.match(prefix):
                device_name = prefix
                break
        if cs.S3_DEVICE_NAME_PATTERN.match(token):
            device_name = token
            break

    if not device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: s3 로그 <device-name> 2026-03-04")

    return {"kind": "log", "device_name": device_name, "log_date": date_token}


def _extract_s3_request(question: str) -> dict[str, str] | None:
    normalized = _normalize_spaces(question)
    if not normalized:
        return None

    lowered = normalized.lower()
    if not re.match(r"^s3(\s|$)", lowered):
        return None

    if "로그" in normalized or "log" in lowered:
        return _extract_s3_log_request(normalized)

    if any(keyword in normalized for keyword in ("초음파", "영상")) or "ultrasound" in lowered:
        barcode = _extract_barcode(normalized)
        if not barcode:
            raise ValueError("영상 조회는 바코드(11자리 숫자)가 필요해. 예: s3 영상 12345678910")
        return {"kind": "ultrasound", "barcode": barcode}

    raise ValueError("지원 형식: s3 영상 <바코드> 또는 s3 로그 <장비명> <YYYY-MM-DD>")


OPERATIONS_ROUTE_GROUP = "operations"


APP_USER_PROFILE_ROUTE = "app_user_lookup"


APP_USER_BABY_ANALYSIS_ROUTE = "app_user_baby_selection_analysis"


BARCODE_VALIDATION_STATUS_ROUTE = "barcode_validation_status"


BARCODE_PINK_CLASSIFICATION_ROUTE = "barcode_pink_classification_reason"


ADMIN_S3_ULTRASOUND_ROUTE = "admin_s3_ultrasound"


ADMIN_S3_DEVICE_LOG_ROUTE = "admin_s3_device_log"


ADMIN_READONLY_SQL_ROUTE = "admin_readonly_sql"


ADMIN_REQUEST_LOG_ROUTE = "admin_request_log"


RECORDING_STREAMING_RESTORE_ROUTE = "recording_streaming_restore"


def match_private_operations_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 조회나 mutation 없이 operations 요청의 정확한 route만 고른다."""

    if not _is_operations_request(request):
        return None
    try:
        return _match_private_operations_route_strict(request)
    except AssistantRequestScopeMismatch:
        # 실제 실행 route가 같은 불일치를 값 노출 없는 guard 응답으로 닫는다.
        return None


def _is_operations_request(request: CompanyAssistantRequest) -> bool:
    return (
        str(request.metadata.get("route_group") or "").strip()
        == OPERATIONS_ROUTE_GROUP
    )


def _match_private_operations_route_strict(
    request: CompanyAssistantRequest,
) -> str | None:
    question = request.question
    barcode = resolve_assistant_request_scope(request).barcode

    # 기존 Slack은 admin handler를 Notion·장비·바코드 handler보다 먼저
    # 실행했다. 한 문장에 바코드 작업 표현이 함께 있어도 명시적인 S3/DB/
    # request-log 요청이 조회 route를 선점하도록 이 순서를 그대로 둔다.
    s3_route = _match_s3_operation(question)
    if s3_route is not None:
        return s3_route
    if _extract_request_log_query(question) is not None:
        return ADMIN_REQUEST_LOG_ROUTE
    if _extract_db_query(question) is not None:
        return ADMIN_READONLY_SQL_ROUTE

    # 더 구체적인 PII 원인 분석을 일반 profile 조회보다 먼저 고정한다.
    if barcode and _should_analyze_app_user_baby_selection(question, barcode):
        return APP_USER_BABY_ANALYSIS_ROUTE
    if barcode and _should_lookup_barcode(question, barcode):
        return APP_USER_PROFILE_ROUTE

    if barcode and _is_barcode_pink_classification_reason_request(
        question,
        barcode,
    ):
        return BARCODE_PINK_CLASSIFICATION_ROUTE
    if barcode and _is_barcode_validation_status_request(question, barcode):
        return BARCODE_VALIDATION_STATUS_ROUTE
    # 기존 barcode handler는 핑크/유효성 read를 먼저 끝낸 뒤에만 월 단위
    # streaming 복원을 실행했다. 혼합 문장에서 mutation이 앞서지 않게 한다.
    if barcode and _is_recording_streaming_restore_request(question, barcode):
        return RECORDING_STREAMING_RESTORE_ROUTE

    return None


def _match_s3_operation(question: str) -> str | None:
    """S3 parser만 실행하며 잘못된 형식도 전용 route의 안전한 안내로 보낸다."""

    try:
        request = _extract_s3_request(question)
    except ValueError:
        normalized = " ".join(str(question or "").split())
        lowered = normalized.lower()
        if not lowered.startswith("s3 ") and lowered != "s3":
            return None
        if "로그" in normalized or "log" in lowered:
            return ADMIN_S3_DEVICE_LOG_ROUTE
        if any(token in normalized for token in ("영상", "초음파")) or (
            "ultrasound" in lowered
        ):
            return ADMIN_S3_ULTRASOUND_ROUTE
        # 기존 admin handler는 `s3`로 시작한 미지원 형식도 parser 오류를
        # 바로 답했다. 첫 S3 route를 입력 오류 sink로 써 remote에서도
        # local fallback 없이 같은 안내를 반환한다.
        return ADMIN_S3_ULTRASOUND_ROUTE

    if request is None:
        return None
    if request.get("kind") == "ultrasound":
        return ADMIN_S3_ULTRASOUND_ROUTE
    if request.get("kind") == "log":
        return ADMIN_S3_DEVICE_LOG_ROUTE
    return None
