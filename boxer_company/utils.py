from datetime import datetime
from typing import Any

# 회사 도메인의 DB/S3/장비 evidence 표시에만 쓰는 정규화 규칙이다.
# 공개 core가 회사 응답 포맷을 소유하지 않도록 회사 패키지에 둔다.
def _display_value(value: Any, default: str = "없음") -> str:
    if value is None:
        return default
    text = str(value).strip()
    return text or default


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
    units = ("B", "KB", "MB", "GB", "TB")
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    if index == 0:
        return f"{int(value)} {units[index]}"
    return f"{value:.1f} {units[index]}"
