"""Slack adapter 안에서만 쓰는 payload·렌더링 정규화 helper다."""

from __future__ import annotations

import re


def _extract_question(text: str, *, bot_user_id: str | None = None) -> str:
    """호출봇의 멘션만 제거하고 질문 대상인 다른 사용자의 식별자는 보존한다."""

    # 기존 helper 호출은 유지하되 실제 이벤트 진입점은 확인된 봇 ID를 명시한다.
    if bot_user_id is None:
        return re.sub(r"<@[^>]+>", "", text).strip()
    # 인증 context가 비었을 때 첫 멘션을 봇으로 추측하면 질문 대상이 사라질 수 있다.
    if not bot_user_id:
        return text.strip()
    return re.sub(r"<@" + re.escape(bot_user_id) + r"(?:\|[^>]+)?>", "", text).strip()


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
