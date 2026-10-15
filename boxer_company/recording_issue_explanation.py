"""검사기의 제한된 원인 코드로 확인된 사실과 다음 확인을 설명한다."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

# 장비의 stderr·파일 경로를 전달하지 않고 알려진 검사 결과만 보존한다.
_DIAGNOSTICS = {
    "mp4_metadata_missing": "MP4 재생 정보(moov)를 찾지 못했어.",
    "permission_denied": "검사기가 파일을 읽을 권한이 없어.",
    "input_missing": "검사 시점에 입력 파일을 찾지 못했어.",
    "io_error": "파일을 읽는 중 입출력 오류가 발생했어.",
    "invalid_media_data": "검사기가 처리할 수 없는 영상·오디오 데이터를 만났어.",
    "unsupported_codec": "검사기에 필요한 디코더가 없어.",
    "no_decodable_audio_samples": "오디오 트랙은 있지만 디코딩된 샘플이 0개야. 정상 녹음된 무음과는 달라.",
    "no_decodable_video_frames": "검사를 끝냈지만 디코딩된 영상 프레임이 0개야.",
}


def normalize_media_diagnostic(media: Any) -> dict[str, str | None]:
    if not isinstance(media, Mapping):
        return {"diagnosticCode": None}
    code = media.get("diagnosticCode")
    if not isinstance(code, str) or code not in _DIAGNOSTICS:
        # 구형 검사기가 이미 보내던 0샘플·0프레임 사유도 같은 계약으로 보존한다.
        reason = media.get("reason")
        code = reason if isinstance(reason, str) and reason in {
            "no_decodable_audio_samples", "no_decodable_video_frames",
        } else None
    return {"diagnosticCode": code}


# 감지 사실만으로 장비 고장·영구 손상·실제 누락 시간을 단정하지 않는다.
_EXPLANATIONS = {
    "video_file_missing": (
        "검사 시점에 녹화 파일을 찾지 못했어.",
        "이 파일의 재생 가능 여부를 확인하지 못했어.",
        "원본 파일의 이동·삭제 여부와 업로드 상태를 확인해.",
    ),
    "video_metadata_unreadable": (
        "재생 정보를 읽지 못했어. 장비가 상세 오류를 보내지 않아 세부 원인은 확인 필요해.",
        "총길이와 재생 가능 여부를 확인하지 못했어.",
        "녹화 종료·파일 마무리 로그와 원본 파일을 확인해.",
    ),
    "video_stream_missing": (
        "파일에 영상 트랙이 없어.", "녹화 화면을 재생할 수 없어.",
        "캡처 입력과 녹화 시작 로그를 확인해.",
    ),
    "video_decode_failed": (
        "영상 디코딩에 실패했어. 장비가 상세 오류를 보내지 않아 세부 원인은 확인 필요해.",
        "전체 영상의 정상 재생을 보장할 수 없어.", "원본 영상 재생과 검사 오류 로그를 확인해.",
    ),
    "video_no_decodable_frames": (
        _DIAGNOSTICS["no_decodable_video_frames"], "검사에서 재생 가능한 화면을 확인하지 못했어.",
        "원본 영상 재생과 녹화 종료 로그를 확인해.",
    ),
    "video_frozen": (
        "화면 변화가 없는 연속 구간을 감지했어.", "해당 구간의 움직임이 기록되지 않았을 수 있어.",
        "표시된 시점의 원본 화면과 캡처 입력 상태를 확인해.",
    ),
    "video_uniform_color": (
        "화면이 한 색으로 채워진 연속 구간을 감지했어.", "해당 구간에서 진료 화면이 보이지 않을 수 있어.",
        "표시된 시점의 원본 화면과 캡처 입력 상태를 확인해.",
    ),
    "audio_stream_missing": (
        "파일에 오디오 트랙이 없어.", "이 파일에서 녹음된 소리를 재생할 수 없어.",
        "오디오 입력 연결과 녹음 설정을 확인해.",
    ),
    "audio_decode_failed": (
        "오디오 디코딩에 실패했어. 장비가 상세 오류를 보내지 않아 세부 원인은 확인 필요해.",
        "전체 오디오의 정상 재생을 보장할 수 없어.", "원본 오디오 재생과 검사 오류 로그를 확인해.",
    ),
    "audio_duration_mismatch": (
        "디코딩된 오디오가 영상보다 짧아.", "영상 일부에서 소리가 재생되지 않을 수 있어.",
        "원본 영상의 소리와 오디오 입력·녹음 종료 로그를 확인해.",
    ),
}


def format_recording_issue_explanation(details: Mapping[str, Any]) -> str:
    media_type = details.get("mediaType")
    code = normalize_media_diagnostic(details.get(media_type) if isinstance(media_type, str) else None)["diagnosticCode"]
    facts, impacts, actions = [], [], []
    for issue in details.get("issues", []):
        if issue not in _EXPLANATIONS:
            continue
        fact, impact, action = _EXPLANATIONS[issue]
        if code and issue in {"video_metadata_unreadable", "video_decode_failed", "audio_decode_failed"}:
            fact = _DIAGNOSTICS[code]
            # 검사 환경 문제라면 파일 손상을 전제한 확인을 요청하지 않는다.
            if code in {"permission_denied", "unsupported_codec"}:
                action = "검사기의 파일 접근 권한·디코더 지원과 원본 재생을 확인해."
        facts.append(fact)
        impacts.append(impact)
        actions.append(action)
    return "\n".join(
        f"{label}: {' '.join(dict.fromkeys(values))}"
        for label, values in (("확인된 내용", facts), ("영향", impacts), ("다음 확인", actions)) if values
    )
