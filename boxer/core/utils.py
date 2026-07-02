import re
from datetime import datetime
from typing import Any

from boxer.core import settings as s


def _validate_llm_tokens(missing: list[str]) -> None:
    if s.LLM_PROVIDER == "claude":
        has_api_key = bool(s.ANTHROPIC_API_KEY and "REPLACE_ME" not in s.ANTHROPIC_API_KEY)
        has_oauth_token = bool(s.ANTHROPIC_AUTH_TOKEN and "REPLACE_ME" not in s.ANTHROPIC_AUTH_TOKEN)
        has_oauth_command = bool(
            s.ANTHROPIC_AUTH_TOKEN_COMMAND
            and "REPLACE_ME" not in s.ANTHROPIC_AUTH_TOKEN_COMMAND
        )
        if not (has_api_key or has_oauth_token or has_oauth_command):
            missing.append("ANTHROPIC_API_KEY 또는 ANTHROPIC_AUTH_TOKEN")
        if not s.ANTHROPIC_MODEL or "REPLACE_ME" in s.ANTHROPIC_MODEL:
            missing.append("ANTHROPIC_MODEL")

    if s.LLM_PROVIDER == "ollama":
        if not s.OLLAMA_BASE_URL or "REPLACE_ME" in s.OLLAMA_BASE_URL:
            missing.append("OLLAMA_BASE_URL")
        if not s.OLLAMA_MODEL or "REPLACE_ME" in s.OLLAMA_MODEL:
            missing.append("OLLAMA_MODEL")


def _validate_data_source_tokens(missing: list[str]) -> None:
    if s.DB_QUERY_ENABLED:
        if not s.DB_HOST or "REPLACE_ME" in s.DB_HOST:
            missing.append("DB_HOST")
        if s.DB_PORT <= 0:
            missing.append("DB_PORT")
        if not s.DB_USERNAME or "REPLACE_ME" in s.DB_USERNAME:
            missing.append("DB_USERNAME")
        if not s.DB_PASSWORD or "REPLACE_ME" in s.DB_PASSWORD:
            missing.append("DB_PASSWORD")
        if not s.DB_DATABASE or "REPLACE_ME" in s.DB_DATABASE:
            missing.append("DB_DATABASE")

    if s.S3_QUERY_ENABLED:
        if not s.AWS_REGION or "REPLACE_ME" in s.AWS_REGION:
            missing.append("AWS_REGION")
        if not s.S3_ULTRASOUND_BUCKET or "REPLACE_ME" in s.S3_ULTRASOUND_BUCKET:
            missing.append("S3_ULTRASOUND_BUCKET")
        if not s.S3_LOG_BUCKET or "REPLACE_ME" in s.S3_LOG_BUCKET:
            missing.append("S3_LOG_BUCKET")


def _validate_tokens(*, include_llm: bool = True, include_data_sources: bool = True) -> None:
    missing: list[str] = []
    if include_llm:
        _validate_llm_tokens(missing)
    if include_data_sources:
        _validate_data_source_tokens(missing)

    if missing:
        raise RuntimeError(
            "필수 환경변수가 설정되지 않았습니다(.env 확인): "
            + ", ".join(missing)
            + ". .env 값을 실제 값으로 교체하세요."
        )


def _extract_question(text: str) -> str:
    return re.sub(r"<@[^>]+>", "", text).strip()


def _display_value(value: Any, default: str = "없음") -> str:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    return text


def _safe_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def _normalize_spaces(text: str) -> str:
    return " ".join((text or "").strip().split())


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + "...(truncated)"


def _format_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        return value
    return "unknown"


def _format_size(size: int | None) -> str:
    if size is None:
        return "unknown"
    value = float(max(0, int(size)))
    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    if index == 0:
        return f"{int(value)} {units[index]}"
    return f"{value:.1f} {units[index]}"


def _format_reply_text(user_id: str | None, text: str) -> str:
    clean_text = (text or "").strip()
    if not clean_text:
        clean_text = "응답 내용이 비어 있어"
    if user_id:
        return f"<@{user_id}> {clean_text}"
    return clean_text
