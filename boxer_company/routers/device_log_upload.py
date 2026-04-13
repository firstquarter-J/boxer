import re
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from boxer_company.routers.barcode_log import _extract_device_name_scope, _extract_hospital_room_scope
from boxer_company.routers.s3_domain import _fetch_s3_device_log_lines

DeviceCommandDispatcher = Callable[[str, str], dict[str, Any]]

_LEADING_DEVICE_LOG_UPLOAD_SCOPE_PATTERN = re.compile(
    r"^\s*([A-Za-z0-9]+-[A-Za-z0-9-]+)\s+(.+)$",
    re.IGNORECASE,
)
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


def _current_kst_date() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")


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


def _extract_latest_hospital_room_scope_from_thread_context(
    thread_context: str,
) -> tuple[str | None, str | None]:
    lines = [line.strip() for line in (thread_context or "").splitlines() if line.strip()]
    for line in reversed(lines):
        message = line.split(":", 1)[1].strip() if ":" in line else line
        hospital_name, room_name = _extract_hospital_room_scope_for_log_upload(message)
        if hospital_name and room_name:
            return hospital_name, room_name
    return None, None


def _extract_device_name_for_log_upload(question: str) -> str | None:
    normalized = _normalize_device_log_upload_question(question)
    extracted = _extract_device_name_scope(normalized)
    if extracted and _is_device_log_upload_check_request(normalized, device_name=extracted):
        return extracted

    matched = _LEADING_DEVICE_LOG_UPLOAD_SCOPE_PATTERN.search(normalized)
    if not matched:
        return None

    candidate = " ".join(str(matched.group(1) or "").split()).strip()
    remainder = " ".join(str(matched.group(2) or "").split()).strip()
    if not candidate or not _is_device_log_upload_check_request(remainder, device_name=candidate):
        return None
    return candidate


def _is_device_log_upload_check_request(question: str, device_name: str | None = None) -> bool:
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


def _check_and_request_device_log_upload(
    s3_client: Any,
    device_name: str,
    log_date: str,
    *,
    has_requested_date: bool,
    dispatch_device_command: DeviceCommandDispatcher | None = None,
    today_date: str | None = None,
) -> tuple[str, dict[str, Any]]:
    normalized_device_name = str(device_name or "").strip()
    normalized_log_date = str(log_date or "").strip()
    if not normalized_device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: `MB2-C00419 로그 업로드 확인`")
    if not normalized_log_date:
        raise ValueError("날짜를 확인해줘")

    resolved_today_date = str(today_date or "").strip() or _current_kst_date()
    log_data = _fetch_s3_device_log_lines(
        s3_client,
        normalized_device_name,
        normalized_log_date,
        tail_only=True,
    )

    payload: dict[str, Any] = {
        "route": "device_log_upload_check",
        "deviceName": normalized_device_name,
        "logDate": normalized_log_date,
        "todayDate": resolved_today_date,
        "requestedDateExplicitly": bool(has_requested_date),
        "logFound": bool(log_data.get("found")),
        "logKey": str(log_data.get("key") or "").strip(),
        "uploadRequested": False,
        "command": None,
    }

    lines = [
        "*장비 로그 업로드 확인*",
        f"• 장비: `{normalized_device_name}`",
        f"• 날짜: `{normalized_log_date}`",
    ]
    if not has_requested_date:
        lines.append("• 날짜 기준: 미지정이라 오늘로 봤어")

    if normalized_log_date > resolved_today_date:
        lines.append("• 결과: 미래 날짜라 아직 로그가 생길 수 없어")
        return "\n".join(lines), payload

    if log_data.get("found"):
        lines.append("• 결과: S3에 로그가 이미 있어")
        lines.append(f"• 파일: `{payload['logKey']}`")
        return "\n".join(lines), payload

    lines.append("• 결과: S3에 로그가 아직 없어")
    lines.append(f"• 파일: `{payload['logKey']}`")

    command = "fdl" if normalized_log_date == resolved_today_date else "fdla"
    payload["command"] = command
    payload["requestMode"] = "today_only" if command == "fdl" else "all_logs_for_historical_date"

    if dispatch_device_command is None:
        payload["dispatch"] = {"status": False, "message": "device_command_not_configured"}
        lines.append("• 조치: 업로드 요청은 아직 못 보냈어")
        lines.append("• 이유: 장비 명령 전송 설정이 없어")
        return "\n".join(lines), payload

    dispatch = dispatch_device_command(normalized_device_name, command)
    payload["dispatch"] = dispatch
    dispatch_ok = bool(dispatch.get("status"))
    payload["uploadRequested"] = dispatch_ok

    if dispatch_ok:
        if command == "fdl":
            lines.append("• 조치: 오늘 로그 업로드 요청 보냈어")
        else:
            lines.append("• 조치: 지정 날짜 단건 명령이 없어 전체 로그 업로드 요청 보냈어")
            lines.append("• 참고: 장비에 남아 있는 로그를 다시 올리게 돼")
        lines.append(f"• 명령: `{command}`")
        lines.append("• 안내: 잠깐 뒤 다시 확인해줘")
        return "\n".join(lines), payload

    lines.append("• 조치: 로그 업로드 요청 전송에 실패했어")
    lines.append(f"• 시도 명령: `{command}`")
    dispatch_message = str(dispatch.get("message") or "").strip()
    if dispatch_message:
        lines.append(f"• 이유: `{dispatch_message}`")
    return "\n".join(lines), payload


def _build_device_log_upload_scope_not_found_reply(hospital_name: str, room_name: str) -> str:
    return "\n".join(
        [
            "*장비 로그 업로드 확인*",
            f"• 병원: `{hospital_name}`",
            f"• 병실: `{room_name}`",
            "• 결과: 해당 병원/병실로 장비를 찾지 못했어",
            "• 안내: MDA에 표시된 병원명/병실명과 같게 다시 보내줘",
        ]
    )


def _build_device_log_upload_scope_ambiguous_reply(
    hospital_name: str,
    room_name: str,
    device_contexts: list[dict[str, Any]],
) -> str:
    lines = [
        "*장비 로그 업로드 확인*",
        f"• 병원: `{hospital_name}`",
        f"• 병실: `{room_name}`",
        f"• 결과: 장비가 `{len(device_contexts)}개`라 하나로 못 정했어",
        "• 장비명으로 다시 보내줘",
    ]
    for index, item in enumerate(device_contexts[:10], start=1):
        device_name = str(item.get("deviceName") or "").strip()
        if device_name:
            lines.append(f"{index}. `{device_name}`")
    return "\n".join(lines)
