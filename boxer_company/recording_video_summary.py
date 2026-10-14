"""단색 화면의 연속 구간 실측값을 검증해 알림에 표시한다."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from boxer_company.recording_media_timeline import (
    bounded_seconds,
    format_ranges,
    format_time,
    normalize_ranges,
)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _timeline(value: Any, threshold: float | None) -> dict[str, Any] | None:
    value = _mapping(value)
    duration = bounded_seconds(value.get("videoDurationSec"))
    if value.get("status") != "complete" or not duration or not threshold:
        return None
    summary = normalize_ranges(value, duration)
    if summary is None:
        return None
    # 기준 미만 구간이나 일부 목록을 전체 합계로 잘못 받은 경우에는 표시하지 않는다.
    if summary["durationSec"] + 0.002 < summary["intervalCount"] * threshold:
        return None
    if any(item["endSec"] - item["startSec"] + 0.002 < threshold for item in summary["intervals"]):
        return None
    return {"status": "complete", "videoDurationSec": duration, **summary}


def normalize_recording_video_details(details: Mapping[str, Any]) -> dict[str, Any]:
    """원본과 재시작한 cursor 모두 숫자 지표만 보존한다."""

    visual = _mapping(_mapping(details.get("video")).get("visualAnalysis"))
    uniform = _mapping(visual.get("uniformColor"))
    threshold = bounded_seconds(uniform.get("thresholdSec"))
    return {"durationSec": bounded_seconds(details.get("durationSec")),
            "video": {"visualAnalysis": {"uniformColor": {
        "thresholdSec": threshold,
        "firstDetectedAtSec": bounded_seconds(uniform.get("firstDetectedAtSec")),
        "maxDurationSec": bounded_seconds(uniform.get("maxDurationSec")),
        "timeline": _timeline(uniform.get("timeline"), threshold),
    }}}}


def format_recording_video_length(details: Mapping[str, Any]) -> str:
    # 예상 녹화 시간이나 알림 발생 시각 대신 파일 검사기의 실측 길이만 쓴다.
    duration = bounded_seconds(details.get("durationSec"))
    return f"영상 길이 {format_time(duration)}" if duration else "영상 길이 확인 필요"


def format_recording_video_summary(details: Mapping[str, Any]) -> str:
    uniform = normalize_recording_video_details(details)["video"]["visualAnalysis"]["uniformColor"]
    timeline = uniform["timeline"]
    if timeline is not None:
        duration = timeline["videoDurationSec"]
        total = timeline["durationSec"]
        return "\n".join([
            f"영상 길이 {format_time(duration)}",
            f"단색 감지 합계 {format_time(total)} (전체의 {100 * total / duration:.1f}%)",
            f"단색 감지 구간: {format_ranges(timeline)}",
            f"감지 기준: 연속 {uniform['thresholdSec']:g}초 이상 · 구간은 영상 시작 기준",
        ])

    # 최초 시작 시각과 최장 시간은 서로 다른 구간일 수 있어 더해서 끝을 만들지 않는다.
    lines = [format_recording_video_length(details)]
    first = uniform["firstDetectedAtSec"]
    longest = uniform["maxDurationSec"]
    if first is not None:
        lines.append(f"첫 단색 감지 시작: {format_time(first)} (영상 시작 기준)")
    if longest is not None and longest > 0:
        lines.append(f"최장 연속 단색: {format_time(longest)}")
    lines.append("전체 단색 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)")
    return "\n".join(lines)
