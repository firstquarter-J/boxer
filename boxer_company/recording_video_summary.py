"""정지, 단색, 깜빡임의 실측 지표와 파일 검사 원인을 검증해 표시한다."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from boxer_company.recording_issue_explanation import normalize_media_diagnostic
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


_FLICKER_THRESHOLDS = {
    "windowSec": 10, "minFlashes": 6, "minBlackRatio": 0.2,
    "minFlashDurationSec": 0.05, "maxFlashDurationSec": 0.8,
    "blackPixelThreshold": 32, "blackFrameThresholdPct": 95,
}
_FLICKER_V2_THRESHOLDS = {
    **_FLICKER_THRESHOLDS,
    "policyVersion": 2, "minFlashes": 10, "minFlashDurationSec": 0.15,
    "nearBlackFrameThresholdPct": 99, "recoveryFrameThresholdPct": 90,
    "minRecoveryDurationSec": 0.1,
}
_FLICKER_V2_ONLY_FIELDS = _FLICKER_V2_THRESHOLDS.keys() - _FLICKER_THRESHOLDS.keys()


def _flicker(value: Any, duration: float | None) -> dict[str, Any] | None:
    # 검사가 완료되고 그 버전의 기준을 충족한 첫 감지 창만 진단에 표시한다.
    value = _mapping(value)
    if not duration or value.get("status") != "complete" or value.get("detected") is not True:
        return None
    # 버전이 없는 기존 기록과 복귀 조건이 추가된 기록은 각자의 정확한 기준을 쓴다.
    if "policyVersion" in value:
        version = value["policyVersion"]
        if type(version) is not int or version != _FLICKER_V2_THRESHOLDS["policyVersion"]:
            return None
        thresholds = _FLICKER_V2_THRESHOLDS
    else:
        if any(key in value for key in _FLICKER_V2_ONLY_FIELDS):
            return None
        thresholds = _FLICKER_THRESHOLDS
    if any(type(value.get(key)) not in (int, float) or value[key] != expected
           for key, expected in thresholds.items()):
        return None
    # 수치 검증과 안내 문구가 같은 판정 기준을 참조하게 한다.
    window_sec = thresholds["windowSec"]
    min_flash_sec = thresholds["minFlashDurationSec"]
    max_flash_sec = thresholds["maxFlashDurationSec"]
    min_black_ratio = thresholds["minBlackRatio"]
    window = _mapping(value.get("window"))
    start = bounded_seconds(window.get("startSec"), maximum=duration)
    end = bounded_seconds(window.get("endSec"), maximum=duration)
    first = bounded_seconds(value.get("firstDetectedAtSec"), maximum=duration)
    black = bounded_seconds(window.get("blackDurationSec"), maximum=window_sec)
    ratio = bounded_seconds(window.get("blackRatio"), maximum=1)
    count = window.get("flashCount")
    if (start is None or end is None or first is None or black is None or ratio is None
            or abs(end - start - window_sec) > 0.002 or not start <= first < end
            or type(count) is not int
            or not thresholds["minFlashes"] <= count <= window_sec / min_flash_sec
            or black < window_sec * min_black_ratio - 0.001 or ratio < min_black_ratio - 0.0001
            or abs(ratio - black / window_sec) > 0.001
            or black < count * min_flash_sec - 0.002 or black > count * max_flash_sec + 0.002):
        return None
    # 마지막 암전 뒤 복귀는 창 밖에서 확인할 수 있어 암전 사이 복귀만 합산한다.
    recovery = thresholds.get("minRecoveryDurationSec")
    if recovery and black + (count - 1) * recovery > end - first + 0.002:
        return None
    return {
        "status": "complete", "detected": True, **thresholds,
        "firstDetectedAtSec": first,
        "window": {"startSec": start, "endSec": end, "flashCount": count,
                   "blackDurationSec": black, "blackRatio": ratio},
    }


def normalize_recording_video_details(details: Mapping[str, Any]) -> dict[str, Any]:
    """원본과 재시작한 cursor 모두 실측 지표와 허용된 원인 코드만 보존한다."""

    video = _mapping(details.get("video"))
    visual = _mapping(video.get("visualAnalysis"))
    freeze = _mapping(visual.get("freeze"))
    uniform = _mapping(visual.get("uniformColor"))
    threshold = bounded_seconds(uniform.get("thresholdSec"))
    duration = bounded_seconds(details.get("durationSec"))
    return {
        "durationSec": duration,
        "video": {
            **normalize_media_diagnostic(video),
            "visualAnalysis": {
                "freeze": {
                    key: bounded_seconds(freeze.get(key)) for key in (
                        "thresholdSec", "firstDetectedAtSec", "maxDurationSec",
                    )
                },
                "uniformColor": {
                    "thresholdSec": threshold,
                    "firstDetectedAtSec": bounded_seconds(uniform.get("firstDetectedAtSec")),
                    "maxDurationSec": bounded_seconds(uniform.get("maxDurationSec")),
                    "timeline": _timeline(uniform.get("timeline"), threshold),
                },
                "flicker": _flicker(visual.get("flicker"), duration),
            },
        },
    }


def format_recording_flicker_summary(details: Mapping[str, Any]) -> str:
    flicker = normalize_recording_video_details(details)["video"]["visualAnalysis"]["flicker"]
    if flicker is None:
        return "깜빡임 감지 구간: 확인 필요 (완료된 구간 분석 없음)"
    window = flicker["window"]
    criterion = (
        f"깜빡임 감지 기준: {flicker['windowSec']:g}초 안에 "
        f"{flicker['minFlashDurationSec']:g}~{flicker['maxFlashDurationSec']:g}초 검은 화면 "
        f"{flicker['minFlashes']:g}회 이상, 창의 {flicker['minBlackRatio'] * 100:g}% 이상"
    )
    if "policyVersion" in flicker:
        criterion += (
            f", 검은 픽셀 {flicker['blackFrameThresholdPct']:g}% 이상인 구간에서 "
            f"{flicker['nearBlackFrameThresholdPct']:g}% 이상이 "
            f"{flicker['minFlashDurationSec']:g}초 이상 연속, "
            f"{flicker['recoveryFrameThresholdPct']:g}% 이하로 "
            f"{flicker['minRecoveryDurationSec']:g}초 이상 연속 복귀"
        )
    # 10초 창의 비율을 전체 영상 비율이나 모든 깜빡임의 합계로 표현하지 않는다.
    return "\n".join([
        f"깜빡임 감지 창: {format_time(window['startSec'])}~{format_time(window['endSec'])} (영상 시작 기준)",
        f"감지 창 내 깜빡임 {window['flashCount']}회",
        f"감지 창 내 검은 화면 {format_time(window['blackDurationSec'])} (창의 {window['blackRatio'] * 100:.1f}%)",
        criterion,
    ])


def format_recording_freeze_summary(details: Mapping[str, Any]) -> str:
    normalized = normalize_recording_video_details(details)
    freeze = normalized["video"]["visualAnalysis"]["freeze"]
    first, threshold = freeze["firstDetectedAtSec"], freeze["thresholdSec"]
    duration = normalized["durationSec"]
    lines = []
    # 최초 구간과 최장 구간은 다를 수 있다. 끝 시각·전체 합계·비율을 추정하지 않는다.
    if first is not None and (not duration or first < duration):
        lines.append(f"첫 정지 감지 시작: {format_time(first)} (영상 시작 기준)")
    # 장비의 시간 기준은 유지되고 Boxer가 비율 조건을 추가 적용한다.
    if threshold:
        lines.append(f"정지 감지 기준: 연속 {format_time(threshold)} 이상")
    lines.append("정지 알림 기준: 최장 연속 구간이 영상 전체의 50% 이상")
    lines.append("전체 정지 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)")
    return "\n".join(lines)


def format_recording_video_length(details: Mapping[str, Any]) -> str:
    # 예상 녹화 시간이나 알림 발생 시각 대신 파일 검사기의 실측 길이만 쓴다.
    duration = bounded_seconds(details.get("durationSec"))
    return f"영상 길이 {format_time(duration)}" if duration else "영상 길이 확인 필요"


def format_recording_video_summary(details: Mapping[str, Any]) -> str:
    uniform = normalize_recording_video_details(details)["video"]["visualAnalysis"]["uniformColor"]
    timeline = uniform["timeline"]
    criterion = "단색 알림 기준: 최장 연속 구간이 영상 전체의 50% 이상"
    if timeline is not None:
        duration = timeline["videoDurationSec"]
        total = timeline["durationSec"]
        return "\n".join([
            f"영상 길이 {format_time(duration)}",
            f"단색 감지 합계 {format_time(total)} (전체의 {100 * total / duration:.1f}%)",
            f"단색 감지 구간: {format_ranges(timeline)}",
            *([f"최장 연속 단색: {format_time(uniform['maxDurationSec'])}"] if uniform["maxDurationSec"] else []),
            f"감지 기준: 연속 {uniform['thresholdSec']:g}초 이상 · 구간은 영상 시작 기준",
            criterion,
        ])

    # 최초 시작 시각과 최장 시간은 서로 다른 구간일 수 있어 더해서 끝을 만들지 않는다.
    lines = [format_recording_video_length(details)]
    first = uniform["firstDetectedAtSec"]
    longest = uniform["maxDurationSec"]
    if first is not None:
        lines.append(f"첫 단색 감지 시작: {format_time(first)} (영상 시작 기준)")
    if longest is not None and longest > 0:
        lines.append(f"최장 연속 단색: {format_time(longest)}")
    if uniform["thresholdSec"]:
        lines.append(f"감지 기준: 연속 {uniform['thresholdSec']:g}초 이상 · 구간은 영상 시작 기준")
    lines.append(criterion)
    lines.append("전체 단색 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)")
    return "\n".join(lines)
