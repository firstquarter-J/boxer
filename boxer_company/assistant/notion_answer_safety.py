from __future__ import annotations


_NOTION_DOCUMENT_LEAK_MARKERS = (
    "system prompt",
    "developer prompt",
    "internal prompt",
    "thread context",
    "evidence(json)",
    "page_id=",
    "authorization:",
    "bearer ",
    "notion_token",
    "<think>",
    "</think>",
)
_NOTION_DOCUMENT_ROUTES = {
    "notion playbook qa",
    "company_notion_qa",
}
_NOTION_DOCUMENT_SECURITY_REFUSAL = (
    "보안 위반 시도로 판단해 요청을 즉시 차단해. 문서 원문, 시스템 정보, "
    "내부 지시문은 공개하지 않아. 같은 시도가 반복되면 관리자 검토 및 "
    "접근 제한 대상으로 처리해."
)


def build_notion_document_security_refusal() -> str:
    return _NOTION_DOCUMENT_SECURITY_REFUSAL


def needs_notion_document_security_refusal(
    text: str,
    route_name: str,
) -> bool:
    """문서 기반 LLM 출력에서 내부 지시문이나 과도한 원문 노출을 차단한다."""
    if route_name not in _NOTION_DOCUMENT_ROUTES:
        return False

    raw_text = text or ""
    normalized = raw_text.strip().lower()
    if any(marker in normalized for marker in _NOTION_DOCUMENT_LEAK_MARKERS):
        return True
    meaningful_lines = [line for line in raw_text.splitlines() if line.strip()]
    return (
        "```" in raw_text
        or len(meaningful_lines) > 16
        or len(raw_text) > 1400
    )


__all__ = [
    "build_notion_document_security_refusal",
    "needs_notion_document_security_refusal",
]
