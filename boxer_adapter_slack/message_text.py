"""Slack block의 화면 텍스트를 순서대로 읽는 공통 presentation helper."""

from __future__ import annotations

from typing import Any


def slack_block_visible_text(block: dict[str, Any]) -> str:
    """text와 elements, fields만 읽고 action value나 metadata는 해석하지 않는다."""
    parts: list[str] = []
    text = block.get("text")
    if isinstance(text, str):
        parts.append(text)
    elif isinstance(text, dict):
        parts.append(slack_block_visible_text(text))
    for key in ("elements", "fields"):
        children = block.get(key)
        if isinstance(children, list):
            # rich text의 한 문장과 list/context 항목의 기존 줄 구분을 보존한다.
            separator = "" if block.get("type") == "rich_text_section" else "\n"
            parts.append(
                separator.join(
                    slack_block_visible_text(child)
                    for child in children
                    if isinstance(child, dict)
                )
            )
    return "\n".join(part for part in parts if part)


__all__ = ["slack_block_visible_text"]
