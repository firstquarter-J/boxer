"""Slack adapter 안에서만 쓰는 payload·렌더링 정규화 helper다."""

from __future__ import annotations

import re


def _extract_question(text: str) -> str:
    """Slack mention 토큰을 제거해 adapter 입력 질문을 만든다."""

    return re.sub(r"<@[^>]+>", "", text).strip()


def _safe_float(value: str) -> float:
    """정렬 가능한 Slack timestamp 값으로 안전하게 변환한다."""

    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def _format_reply_text(user_id: str | None, text: str) -> str:
    """Slack 응답 본문을 정규화하고 요청자 mention을 붙인다."""

    clean_text = (text or "").strip() or "응답 내용이 비어 있어"
    if user_id:
        return f"<@{user_id}> {clean_text}"
    return clean_text
