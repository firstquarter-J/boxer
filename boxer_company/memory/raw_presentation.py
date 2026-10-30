"""원문 보관과 분리해 답변에 표시하는 개인정보와 인증정보만 가린다."""

from __future__ import annotations

import re

_PRIVATE_KEY = re.compile(
    r"-----BEGIN[^\n]*PRIVATE KEY-----.*?(?:-----END[^\n]*PRIVATE KEY-----|\Z)",
    re.IGNORECASE | re.DOTALL,
)
_SECRET_LINE = re.compile(
    r"^.*(?:xox[baprs]-|sk-ant-|\bsk-[A-Za-z0-9_-]{12,}|github_pat_|gh[pousr]_|\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|"
    r"\bBearer\s+\S+|[\"']?(?:password|passwd|secret|token|api[_-]?key|access[_-]?token|"
    r"authorization|private[_-]?key|aws[_-]?secret[_-]?access[_-]?key|비밀번호)[\"']?\s*[:=]|"
    r"[?&](?:X-Amz-|token=|signature=|credential=)|"
    r"[A-Za-z][A-Za-z0-9+.-]{0,30}://[^\s/@]+@|"
    r"(?:curl|wget)\b[^\n]*(?:\s(?:-u|--user|--password|--proxy-user)(?:[=\s])|\s-u\S+)|"
    r"\bsshpass\s+-p\s+).*$",
    re.IGNORECASE | re.MULTILINE,
)
_PERSONAL_VALUES = re.compile(
    r"\b01[016789][ -]?\d{3,4}[ -]?\d{4}\b|\b\d{6}-?[1-4]\d{6}\b|"
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b"
)
_PERSONAL_LINE = re.compile(r"^.*(?:환자\s*(?:명|이름|정보)|고객\s*(?:명|이름|정보)).*$", re.MULTILINE)


def raw_display_text(text: str) -> str:
    """DB의 원문과 hash는 보존하고 표시 직전 사본에서만 값을 가린다."""
    # 인증정보나 환자정보는 여러 줄의 구조화 값일 수 있다. 표시 사본 전체를
    # 가려 다음 줄에 값이 남지 않게 하며, 원문 보관과 원문 기준 집계는 유지한다.
    if _PRIVATE_KEY.search(text) or _SECRET_LINE.search(text):
        return "[인증정보 가림]"
    if _PERSONAL_LINE.search(text):
        return "[개인정보 가림]"
    return _PERSONAL_VALUES.sub("[개인정보 가림]", text)


def is_safe_raw_display_text(text: str) -> bool:
    """이미 가린 표시 사본만 허용하며 제어 문자와 아직 남은 값을 거절한다."""
    return bool(isinstance(text, str) and text.strip()
                and not any((ord(char) < 32 and char not in "\n\r\t") or ord(char) == 127 for char in text)
                and raw_display_text(text) == text)
