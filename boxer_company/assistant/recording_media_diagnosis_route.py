"""확인된 파일 검사 근거를 해석해 영상 알림의 후속 질문에 답한다."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import Any

from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.recording_audio_summary import format_recording_audio_summary
from boxer_company.recording_media_scope import (
    RECORDING_MEDIA_DIAGNOSIS_ROUTE,
    match_recording_media_diagnosis_route,
    resolve_recording_media_diagnosis_scope,
)
from boxer_company.recording_media_timeline import bounded_seconds, format_time
from boxer_company.recording_video_summary import (
    format_recording_freeze_summary,
    format_recording_video_summary,
    normalize_recording_video_details,
)


def _diagnose(**kwargs: Any) -> Mapping[str, Any]:
    # provider 실행 코드는 API route가 호출될 때만 가져온다.
    from boxer_company.recording_media_diagnosis import diagnose_recording_media

    return diagnose_recording_media(**kwargs)


_DIAGNOSTICS = {
    "h264_corrupt_frame": "H.264 영상 프레임 오류",
    "video_corrupt_frame": "영상 프레임 오류",
    "invalid_media_data": "검사기가 처리할 수 없는 영상 데이터",
    "unsupported_codec": "검사기의 디코더 지원 부족",
    "input_missing": "검사 시점의 입력 파일 부재",
    "permission_denied": "검사기의 파일 읽기 권한 오류",
    "io_error": "파일을 읽는 중 발생한 입출력 오류",
    "decode_process_failed": "영상 디코딩 과정의 오류",
}
_ISSUES = {
    "video_frozen": "연속 화면 정지",
    "video_uniform_color": "연속 단색 화면",
    "video_decode_failed": "영상 디코딩 실패",
    "video_no_decodable_frames": "디코딩된 영상 프레임 없음",
    "video_metadata_unreadable": "영상 재생 정보 읽기 실패",
    "video_stream_missing": "영상 트랙 없음",
    "audio_stream_missing": "오디오 트랙 없음",
    "audio_decode_failed": "오디오 디코딩 실패",
    "audio_duration_mismatch": "오디오 데이터 길이 부족",
}


def _time(value: float) -> str:
    # 디코더의 마지막 출력은 정확한 손상 프레임 PTS가 아니므로 부근으로 안내한다.
    minutes, seconds = divmod(int(value), 60)
    return f"{minutes}분 {seconds}초" if minutes else f"{seconds}초"


def _format_evidence(evidence: Mapping[str, Any]) -> str:
    strict = evidence.get("strictDecode")
    normal = evidence.get("normalDecode")
    strict = strict if isinstance(strict, Mapping) else {}
    normal = normal if isinstance(normal, Mapping) else {}
    code = strict.get("diagnosticCode")
    diagnostic = (
        _DIAGNOSTICS.get(code, "영상 디코딩 과정의 오류")
        if isinstance(code, str)
        else "영상 디코딩 과정의 오류"
    )
    last = bounded_seconds(strict.get("lastDecodedSec"))
    duration = bounded_seconds(evidence.get("durationSec"))
    location = (
        f"영상 약 {_time(last)} 부근에서 " if last is not None else "영상 검사에서 "
    )
    lines: list[str] = []
    if strict.get("status") == "decode_error":
        lines.append(f"**{location}{diagnostic}를 확인했어.**")
        if normal.get("status") == "completed":
            lines.append(
                "오류 발생 시 중단하는 엄격한 검사는 실패했지만, 일반 디코딩은 영상 끝 시각까지 진행됐어. 오류 프레임을 건너뛰거나 보정하면 정상처럼 보일 수 있어."
            )
            lines.append(
                "그래서 정상처럼 재생되는 것과 디코딩 실패 알림은 함께 나타날 수 있어. 영상 전체가 재생 불가하다는 뜻은 아니야."
            )
        elif normal.get("status") == "decode_error":
            lines.append(
                "일반 디코딩도 끝까지 완료하지 못했어. 전체 영상의 정상 재생은 확인할 수 없어."
            )
        else:
            lines.append(
                "일반 디코딩 검사가 완료되지 않아 전체 영상의 재생 가능 여부는 확인할 수 없어."
            )
    elif strict.get("status") == "completed":
        lines.append(
            "**업로드된 영상은 끝까지 디코딩됐고, 이번 검사에서는 디코딩 오류가 재현되지 않았어.**"
        )
        lines.append("장비의 당시 검사 환경이나 파일 상태 차이까지 확인한 것은 아니야.")
    else:
        lines.append(
            "영상 재검사를 완료하지 못해서 디코딩 오류 여부와 전체 재생 가능 여부를 확정할 수 없어."
        )
    if duration is not None:
        lines.append(f"확인한 영상 전체 길이는 **{format_time(duration)}**야.")
    issues = evidence.get("notificationIssues")
    labels = (
        [_ISSUES[item] for item in issues if isinstance(item, str) and item in _ISSUES]
        if isinstance(issues, list)
        else []
    )
    if labels:
        lines.append("기존 장비 알림: " + ", ".join(dict.fromkeys(labels)) + ".")
    details = evidence.get("notificationDetails")
    if isinstance(details, Mapping) and isinstance(issues, list):
        # 기존 알림의 구간 formatter를 재사용하고 현재 디코딩 검사 결과와 섞지 않는다.
        summaries = []
        if "video_frozen" in issues:
            summaries.append(format_recording_freeze_summary(details))
            longest = normalize_recording_video_details(details)["video"][
                "visualAnalysis"
            ]["freeze"]["maxDurationSec"]
            if longest and (duration is None or longest <= duration):
                summaries.append(f"최장 연속 화면 정지: {format_time(longest)}")
        if "video_uniform_color" in issues:
            summaries.append(format_recording_video_summary(details))
        if any(
            item in issues
            for item in ("audio_decode_failed", "audio_duration_mismatch")
        ):
            summaries.append(format_recording_audio_summary(details))
        if summaries:
            lines.append(
                "기존 장비 검사 결과:\n" + "\n".join(summaries).replace("·", ",")
            )
    lines.append("이번 재검사 범위는 영상 디코딩이야.")
    return "\n\n".join(lines)


class RecordingMediaDiagnosisAssistantRoute:
    name = RECORDING_MEDIA_DIAGNOSIS_ROUTE

    def __init__(
        self,
        diagnose: Callable[..., Mapping[str, Any]] | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._diagnose = diagnose or _diagnose
        self._logger = logger or logging.getLogger(__name__)

    def handle(self, request: CompanyAssistantRequest) -> CompanyAssistantResult | None:
        if match_recording_media_diagnosis_route(request) is None:
            return None
        scope = resolve_recording_media_diagnosis_scope(request)
        if scope.reason:
            return self._result(
                "needs_input",
                "분석할 영상을 하나로 특정할 수 없어. 알림 스레드에서 요청하거나 알림 번호 또는 바코드와 날짜, 장비를 함께 알려줘",
                "recording_media_scope_required",
            )
        try:
            evidence = self._diagnose(**scope.service_kwargs())
        except (OSError, RuntimeError, TypeError, ValueError) as exc:
            # 원문 stderr, 저장 경로와 credential이 예외에 섞여도 밖으로 내보내지 않는다.
            self._logger.warning(
                "Recording media diagnosis failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return self._result(
                "failed",
                "영상 검사 근거를 확인하지 못했어. 잠시 후 다시 확인해줘",
                "recording_media_diagnosis_failed",
            )
        status = evidence.get("status") if isinstance(evidence, Mapping) else None
        if status == "needs_input":
            return self._result(
                "needs_input",
                "조건에 맞는 영상을 하나로 특정할 수 없어. 정확한 알림 번호 또는 바코드와 날짜, 장비를 알려줘. fileId가 있으면 함께 보내줘",
                "recording_media_ambiguous",
            )
        if status == "not_found":
            return self._result(
                "no_evidence",
                "조건에 맞는 영상 알림이나 업로드된 영상을 찾지 못했어. 알림 번호와 대상, 날짜를 확인해줘",
                "recording_media_not_found",
            )
        if status != "analyzed":
            return self._result(
                "failed",
                "영상 재검사를 진행할 수 없어. 파일 접근 상태와 검사 환경을 확인해야 해",
                "recording_media_unavailable",
            )
        return self._result("answered", _format_evidence(evidence))

    def _result(
        self, outcome: AssistantOutcome, body: str, reason: str | None = None
    ) -> CompanyAssistantResult:
        return CompanyAssistantResult(
            route=self.name,
            outcome=outcome,
            messages=(AssistantMessage(body=body),),
            fallback_reason=reason,
        )
