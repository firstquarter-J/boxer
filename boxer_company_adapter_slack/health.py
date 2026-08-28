def _format_ping_llm_status(ok: bool | None) -> str:
    """API health의 삼상태를 Slack 표시 문구로만 변환한다."""

    if ok is None:
        return "미설정"
    return "가능" if ok else "불가"
