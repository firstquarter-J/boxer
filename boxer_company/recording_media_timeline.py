"""녹화 오디오·영상 알림이 공유하는 숫자 구간 검증과 재생 시간 표시."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

_MAX_SECONDS = 7 * 24 * 60 * 60
_STORED_INTERVALS = 20
_DISPLAY_INTERVALS = 4


def bounded_seconds(value: Any, *, minimum: float = 0, maximum: float = _MAX_SECONDS) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        number = float(value)
    except OverflowError:
        return None
    return number if math.isfinite(number) and minimum <= number <= maximum else None


def normalize_ranges(value: Any, duration: float) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    total = bounded_seconds(value.get("durationSec"), maximum=duration)
    count = value.get("intervalCount")
    ranges = value.get("intervals")
    if (total is None or type(count) is not int or not 0 <= count <= 8192
            or not isinstance(ranges, list) or len(ranges) != min(count, _STORED_INTERVALS)):
        return None
    safe_ranges = []
    previous_end = 0.0
    for interval in ranges:
        if not isinstance(interval, Mapping):
            return None
        start = bounded_seconds(interval.get("startSec"), maximum=duration)
        end = bounded_seconds(interval.get("endSec"), maximum=duration)
        if start is None or end is None or start < previous_end or end < start:
            return None
        safe_ranges.append({"startSec": start, "endSec": end})
        previous_end = end
    shown_total = sum(item["endSec"] - item["startSec"] for item in safe_ranges)
    # 구간별 밀리초 반올림 오차만 허용하고 부분 목록을 전체 합계로 오인하지 않는다.
    tolerance = 0.001 * (len(safe_ranges) + 1)
    if shown_total > total + tolerance or (count <= _STORED_INTERVALS and abs(shown_total - total) > tolerance):
        return None
    return {"intervals": safe_ranges, "intervalCount": count, "durationSec": total}


def format_time(seconds: float) -> str:
    # 초를 먼저 반올림해 59.9999초가 00:60.000으로 표시되지 않게 한다.
    minutes, milliseconds = divmod(round(seconds * 1000), 60_000)
    return f"{minutes:02d}:{milliseconds / 1000:06.3f}".rstrip("0").rstrip(".")


def format_ranges(summary: Mapping[str, Any]) -> str:
    intervals = summary["intervals"][:_DISPLAY_INTERVALS]
    text = ", ".join(f"{format_time(item['startSec'])}~{format_time(item['endSec'])}" for item in intervals)
    remaining = summary["intervalCount"] - len(intervals)
    if remaining:
        text += f" 외 {remaining}구간 (합계에 포함)"
    return text or "없음"
