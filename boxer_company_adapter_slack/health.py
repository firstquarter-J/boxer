from boxer_company.assistant.dependency_errors import (
    build_dependency_failure_message as _build_dependency_failure_reply,
)


def _format_ping_llm_status(ok: bool | None) -> str:
    if ok is None:
        return "미설정"
    return "가능" if ok else "불가"
