"""자연스러운 무음과 구분해 오디오 데이터 누락의 실측값만 표시한다."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from boxer_company.recording_media_timeline import (
    bounded_seconds,
    format_ranges,
    format_time,
    normalize_ranges,
)


def _timeline(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, Mapping) or value.get("status") != "complete":
        return None
    duration = bounded_seconds(value.get("videoDurationSec"))
    tolerance = bounded_seconds(value.get("gapToleranceSec"), maximum=1)
    if not duration or tolerance is None:
        return None
    missing = normalize_ranges(value.get("missing"), duration)
    if missing is None:
        return None
    # 음량으로는 조용한 진료와 입력 장애를 구분할 수 없어 무음 지표를 보존하지 않는다.
    return {
        "status": "complete", "videoDurationSec": duration,
        "gapToleranceSec": tolerance, "missing": missing,
    }


def normalize_recording_audio_details(details: Mapping[str, Any]) -> dict[str, Any]:
    """원본 알림과 재시작한 cursor에 같은 allowlist를 적용한다."""

    audio = details.get("audio")
    audio = audio if isinstance(audio, Mapping) else {}
    return {
        "durationSec": bounded_seconds(details.get("durationSec")),
        "audio": {
            "audioDurationSec": bounded_seconds(audio.get("audioDurationSec")),
            "timeline": _timeline(audio.get("timeline")),
        },
    }


def format_recording_audio_summary(details: Mapping[str, Any]) -> str:
    normalized = normalize_recording_audio_details(details)
    audio = normalized["audio"]
    timeline = audio["timeline"]
    if timeline is not None:
        duration = timeline["videoDurationSec"]
        missing = timeline["missing"]
        total = missing["durationSec"]
        return "\n".join([
            f"영상 길이 {format_time(duration)}",
            f"오디오 데이터 누락 합계 {format_time(total)} (전체의 {100 * total / duration:.1f}%)",
            f"오디오 데이터 누락 구간: {format_ranges(missing)}",
        ])

    # 구형 길이 차이는 재생 타임라인의 위치나 실제 무음 비율을 증명하지 않는다.
    # silenceRatio는 오디오 길이를 분모로 쓰고 PTS 빈 구간까지 포함할 수 있어 사용하지 않는다.
    duration = normalized["durationSec"]
    audio_duration = audio["audioDurationSec"]
    lines = []
    if duration and audio_duration is not None:
        lines.append(f"파일 길이 {format_time(duration)} · 디코딩 오디오 {format_time(audio_duration)}")
        if audio_duration < duration:
            shortfall = duration - audio_duration
            lines.append(f"오디오 길이 차이 {format_time(shortfall)} ({100 * shortfall / duration:.1f}%)")
    lines.append("오디오 데이터 누락 구간 및 비율: 확인 필요 (완료된 구간 분석 없음)")
    return "\n".join(lines)
