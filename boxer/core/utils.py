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
        # 공개 S3 connector는 region/client 조립만 책임지고 bucket·조회 정책은 domain adapter가 검증한다.
        if not s.AWS_REGION or "REPLACE_ME" in s.AWS_REGION:
            missing.append("AWS_REGION")


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


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + "...(truncated)"
