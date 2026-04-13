import re
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from boxer_company.routers.barcode_log import _extract_device_name_scope
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


def _normalize_device_log_upload_question(question: str) -> str:
    text = re.sub(r"<@[^>]+>", " ", str(question or "")).strip()
    return re.sub(r"[`'\"“”‘’]+", "", text)


def _current_kst_date() -> str:
    return datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d")


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
    resolved_device_name = str(device_name or "").strip()
    if not resolved_device_name:
        return False

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
