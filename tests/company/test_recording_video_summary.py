import json

import pytest

from boxer_company.recording_video_summary import (
    format_recording_video_summary,
    normalize_recording_video_details,
)


def video_details() -> dict:
    # 최초 구간과 최장 구간이 달라 최초 시작+최장 길이로 끝을 만들면 안 된다.
    return {"durationSec": 50, "video": {
        "path": "/private/recording.mp4", "stderr": "raw-log",
        "visualAnalysis": {"uniformColor": {
            "thresholdSec": 10, "firstDetectedAtSec": 2, "maxDurationSec": 15,
            "timeline": {"status": "complete", "videoDurationSec": 40,
                         "intervals": [{"startSec": 2, "endSec": 12}, {"startSec": 25, "endSec": 40}],
                         "intervalCount": 2, "durationSec": 25},
        }},
    }}


def test_uniform_intervals_survive_reload_and_use_video_length_for_ratio() -> None:
    normalized = normalize_recording_video_details(video_details())
    restored = json.loads(json.dumps(normalized, allow_nan=False))
    assert normalize_recording_video_details(restored) == normalized
    text = format_recording_video_summary(restored)
    assert "영상 길이 00:40" in text
    assert "단색 감지 합계 00:25 (전체의 62.5%)" in text
    assert "단색 감지 구간: 00:02~00:12, 00:25~00:40" in text
    assert "연속 10초 이상 · 구간은 영상 시작 기준" in text
    assert "private" not in json.dumps(restored)
    assert "raw-log" not in json.dumps(restored)


def test_legacy_actual_metrics_never_invent_end_or_total_from_first_and_longest() -> None:
    # 실제 81988 알림에는 최초 시작과 최장 길이만 있고 전체 구간 목록이 없다.
    details = {"durationSec": 567.734, "video": {"visualAnalysis": {"uniformColor": {
        "thresholdSec": 10, "firstDetectedAtSec": 8.33333, "maxDurationSec": 10.9,
    }}}}
    text = format_recording_video_summary(details)
    assert "첫 단색 감지 시작: 00:08.333" in text
    assert "최장 연속 단색: 00:10.9" in text
    assert "확인 필요" in text
    assert "~" not in text
    assert "%" not in text


@pytest.mark.parametrize(("key", "value"), [
    ("status", "partial"), ("videoDurationSec", 0), ("videoDurationSec", True),
    ("durationSec", 24), ("durationSec", float("nan")), ("durationSec", 41),
    ("intervalCount", 3), ("intervalCount", True), ("intervals", []),
    ("intervals", [{"startSec": -1, "endSec": 12}, {"startSec": 25, "endSec": 40}]),
    ("intervals", [{"startSec": 2, "endSec": 12}, {"startSec": 5, "endSec": 20}]),
    ("intervals", [{"startSec": 2, "endSec": 12}, {"startSec": 35, "endSec": 50}]),
])
def test_invalid_or_partial_timeline_does_not_claim_total(key: str, value: object) -> None:
    details = video_details()
    details["video"]["visualAnalysis"]["uniformColor"]["timeline"][key] = value
    text = format_recording_video_summary(details)
    assert "확인 필요" in text
    assert "~" not in text
    assert "%" not in text
    json.dumps(normalize_recording_video_details(details), allow_nan=False)


@pytest.mark.parametrize("value", [None, {}, [], True, "10", -1, float("inf"), 10 ** 400])
def test_invalid_measurements_are_not_displayed(value: object) -> None:
    details = {"video": {"visualAnalysis": {"uniformColor": {
        "thresholdSec": value, "firstDetectedAtSec": value, "maxDurationSec": value, "timeline": value,
    }}}}
    text = format_recording_video_summary(details)
    assert text == "전체 단색 구간·합계·비율: 확인 필요 (완료된 구간 분석 없음)"
    json.dumps(normalize_recording_video_details(details), allow_nan=False)


def test_threshold_and_long_list_keep_full_aggregate_without_claiming_short_segments() -> None:
    details = video_details()
    uniform = details["video"]["visualAnalysis"]["uniformColor"]
    uniform["thresholdSec"] = 11
    assert "확인 필요" in format_recording_video_summary(details)
    uniform["thresholdSec"] = 10
    uniform["timeline"] = {
        "status": "complete", "videoDurationSec": 1200,
        "intervals": [{"startSec": i * 12, "endSec": i * 12 + 10} for i in range(20)],
        "intervalCount": 100, "durationSec": 1000,
    }
    text = format_recording_video_summary(details)
    assert "16:40 (전체의 83.3%)" in text
    assert "외 96구간 (합계에 포함)" in text
    assert len(text) < 700
