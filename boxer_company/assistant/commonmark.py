from __future__ import annotations

import re
from collections.abc import Callable


_SLACK_LINK_PATTERN = re.compile(r"<(https?://[^|>\s]+)\|([^>\n]+)>")
_SLACK_BOLD_PATTERN = re.compile(r"(?<!\*)\*([^*\n]+)\*(?!\*)")
_CODE_SPAN_PATTERN = re.compile(
    r"```[\s\S]*?(?:```|\Z)|`[^`\n]*(?:`|\Z)"
)


def slack_mrkdwn_to_commonmark(text: str) -> str:
    """회사 formatter가 남긴 Slack 강조·링크를 채널 중립 CommonMark로 바꾼다."""
    normalized = (text or "").strip()
    return transform_outside_code(
        normalized,
        _slack_segment_to_commonmark,
    )


def transform_outside_code(
    text: str,
    transform: Callable[[str], str],
) -> str:
    """inline/fenced code 원문은 보존하고 나머지 구간에만 변환을 적용한다."""
    parts: list[str] = []
    cursor = 0
    for match in _CODE_SPAN_PATTERN.finditer(text or ""):
        parts.append(transform((text or "")[cursor : match.start()]))
        parts.append(match.group(0))
        cursor = match.end()
    parts.append(transform((text or "")[cursor:]))
    return "".join(parts)


def _slack_segment_to_commonmark(text: str) -> str:
    normalized = _SLACK_LINK_PATTERN.sub(
        lambda match: (
            f"[{_escape_link_label(match.group(2))}]({match.group(1)})"
        ),
        text,
    )
    return _SLACK_BOLD_PATTERN.sub(r"**\1**", normalized)


def _escape_link_label(label: str) -> str:
    return (label or "").replace("\\", "\\\\").replace("[", "\\[").replace(
        "]",
        "\\]",
    )


__all__ = [
    "slack_mrkdwn_to_commonmark",
    "transform_outside_code",
]
