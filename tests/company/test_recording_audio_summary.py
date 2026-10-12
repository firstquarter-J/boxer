import json

import pytest

from boxer_company.recording_audio_summary import (
    format_recording_audio_summary,
    normalize_recording_audio_details,
)


def audio_details() -> dict:
    # 이전 payload에 무음 지표가 섞여 있어도 실제 샘플 누락만 보존해야 한다.
    def summary(start: float, end: float) -> dict:
        return {"intervals": [{"startSec": start, "endSec": end}],
                "intervalCount": 1, "durationSec": end - start}

    return {
        "durationSec": 300,
        "audio": {
            "audioDurationSec": 290, "silenceRatio": 1,
            "path": "/private/file.mp4", "stderr": "private-raw-log",
            "timeline": {
                "status": "complete", "videoDurationSec": 300,
                "silenceThresholdDb": -50, "silenceMinDurationSec": 1,
                "gapToleranceSec": 0.005,
                "missing": summary(290, 300), "silence": summary(174, 179),
                "noSound": {"intervals": [{"startSec": 174, "endSec": 179},
                                          {"startSec": 290, "endSec": 300}],
                            "intervalCount": 2, "durationSec": 15},
            },
        },
    }


def test_complete_timeline_formats_playback_ranges_and_video_percentage() -> None:
    details = audio_details()
    # 컨테이너 길이가 달라도 분석이 측정한 영상 길이를 분모로 사용한다.
    details["durationSec"] = 400
    normalized = normalize_recording_audio_details(details)
    restored = json.loads(json.dumps(normalized, allow_nan=False))
    assert normalize_recording_audio_details(restored) == normalized
    text = format_recording_audio_summary(restored)
    assert "영상 길이 05:00" in text
    assert "오디오 데이터 누락 합계 00:10 (전체의 3.3%)" in text
    assert "오디오 데이터 누락 구간: 04:50~05:00" in text
    assert "02:54~02:59" not in text
    assert "무음" not in text
    saved = json.dumps(restored)
    assert "private" not in saved
    assert "silenceRatio" not in saved
    assert "silence" not in saved
    assert "noSound" not in saved


def test_legacy_duration_difference_never_claims_silence_position_or_percentage() -> None:
    # 실제 알림 형태: 무음 비율 1과 유한 peak가 공존하므로 이전 비율은 표시하지 않는다.
    text = format_recording_audio_summary({
        "durationSec": 418.867,
        "audio": {"audioDurationSec": 270.9973333333, "silenceRatio": 1, "peakLevelDb": -12.3},
    })
    assert "파일 길이 06:58.867 · 디코딩 오디오 04:30.997" in text
    assert "길이 차이 02:27.87 (35.3%)" in text
    assert "확인 필요" in text
    assert "100.0%" not in text
    assert "~" not in text


@pytest.mark.parametrize("value", [None, {}, [], True, "1", float("nan"), float("inf"), -1, 10 ** 400])
def test_bad_audio_metrics_remain_serializable_and_do_not_guess(value: object) -> None:
    details = {"durationSec": value, "audio": {"audioDurationSec": value, "timeline": value}}
    normalized = normalize_recording_audio_details(details)
    json.dumps(normalized, allow_nan=False)
    assert format_recording_audio_summary(normalized) == (
        "오디오 데이터 누락 구간 및 비율: 확인 필요 (완료된 구간 분석 없음)"
    )


@pytest.mark.parametrize(("key", "value"), [
    ("status", "partial"), ("videoDurationSec", 0), ("videoDurationSec", True),
    ("gapToleranceSec", 10), ("missing", []),
    ("missing", {"intervals": [{"startSec": -1, "endSec": 5}], "intervalCount": 1, "durationSec": 6}),
    ("missing", {"intervals": [{"startSec": 179, "endSec": 174}], "intervalCount": 1, "durationSec": 5}),
    ("missing", {"intervals": [{"startSec": 174, "endSec": 400}], "intervalCount": 1, "durationSec": 5}),
    ("missing", {"intervals": [], "intervalCount": 1, "durationSec": 5}),
    ("missing", {"intervals": [], "intervalCount": False, "durationSec": 0}),
    ("missing", {"intervals": [], "intervalCount": 0, "durationSec": 5}),
])
def test_incomplete_or_inconsistent_timeline_falls_back_to_length_only(key: str, value: object) -> None:
    details = audio_details()
    details["audio"]["timeline"][key] = value
    text = format_recording_audio_summary(details)
    assert "확인 필요" in text
    assert "~" not in text
    assert "오디오 데이터 누락 합계" not in text


def test_long_interval_list_keeps_full_total_and_explicit_omission_within_alert_limit() -> None:
    details = audio_details()
    timeline = details["audio"]["timeline"]
    timeline["missing"] = {
        "intervals": [{"startSec": index * 2, "endSec": index * 2 + 1} for index in range(20)],
        "intervalCount": 100, "durationSec": 100,
    }
    text = format_recording_audio_summary(details)
    assert "01:40 (전체의 33.3%)" in text
    assert "외 96구간 (합계에 포함)" in text
    assert len(text) < 700


def test_silence_with_complete_audio_data_does_not_count_as_missing() -> None:
    details = audio_details()
    details["audio"]["timeline"]["missing"] = {"intervals": [], "intervalCount": 0, "durationSec": 0}
    text = format_recording_audio_summary(details)
    assert "오디오 데이터 누락 구간: 없음" in text
    assert "무음" not in text
    assert "전체의 0.0%" in text
