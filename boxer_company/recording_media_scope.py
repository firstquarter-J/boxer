"""영상 알림 후속 질문의 의도와 DB 대조용 범위를 provider 없이 해석한다."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Protocol

RECORDING_MEDIA_DIAGNOSIS_ROUTE = "recording_media_diagnosis"
_BARCODE = re.compile(r"(?<!\d)(\d{11})(?!\d)")
_DEVICE = re.compile(
    r"(?<![A-Za-z0-9_-])(MB\d-[A-Za-z0-9-]+)(?![A-Za-z0-9_-])", re.IGNORECASE
)
_NOTIFICATION = re.compile(
    r"(?:notification\s*(?:id)?|알림\s*(?:번호|id|#))\s*[:=#]?\s*(\d{1,12})(?!\d)",
    re.IGNORECASE,
)
_FILE = re.compile(
    r"""\bfile\s*id\s*[:=]?\s*[`'"]?([A-Za-z0-9_-]{6,128})(?![A-Za-z0-9_-])""",
    re.IGNORECASE,
)
_DATE = re.compile(r"(?<!\d)(20\d{2})[-./](\d{1,2})[-./](\d{1,2})(?!\d)")
_SESSION_DATE = re.compile(
    r"(?:세션\s*시작|녹화\s*시작)[^\d]{0,40}(20\d{2}[-./]\d{1,2}[-./]\d{1,2})"
)
_SESSION_TIME = re.compile(
    r"(?:세션\s*시작|녹화\s*시작)[^\d]{0,40}(20\d{2}[-./]\d{1,2}[-./]\d{1,2})\s+(\d{2}:\d{2}:\d{2})(?!\d)"
)
_ALERT = re.compile(
    r"녹화\s*(?:파일\s*)?영상\s*이상|영상\s*디코딩\s*실패|recording_media_quality_issue",
    re.IGNORECASE,
)
_MEDIA = re.compile(
    r"영상|녹화|동영상|비디오|디코딩|\b(?:video|decode|decoding)\b", re.IGNORECASE
)
_QUESTION = re.compile(
    r"왜|원인|이상|문제|오류|실패|정상|분석|검사|확인|점검|어떤|무슨|뭐가|어디|\b(?:inspect|analy[sz]e|error|fail|corrupt)\b",
    re.IGNORECASE,
)
_FOLLOWUP = re.compile(
    r"(?:(?:이거|이것|이\s*알림|그럼|그래서)\s*)?"
    r"(?:왜(?:\s*(?:그래|이래|그런\s*거야))?|"
    r"(?:어떤|무슨|뭐가)\s*(?:문제|이상)(?:인지|야|이야|가\s*있어)?(?:\s*(?:확인|분석)(?:해줘|해))?|"
    r"원인(?:이\s*뭐야|\s*확인해줘|\s*분석해줘)?)"
    r"[?？!！.\s]*$"
)
_OTHER_OPERATION = re.compile(
    r"다운로드|복구|복원|삭제|업데이트|재부팅|전원|장비\s*상태|캡처보드|음성.*(?:변경|바꿔)|문자.*(?:보내|발송)|로그(?!인)|\b(?:download|delete|restore|update|reboot|log|led|pm2)\b",
    re.IGNORECASE,
)
_LIST_REQUEST = re.compile(
    r"개수|목록|리스트|몇\s*(?:개|건)|길이|녹화\s*날짜|촬영\s*날짜|언제"
)
_ISSUE_INTENT = re.compile(
    r"디코딩|이상|문제|오류|실패|손상|깨짐|끊김|정지|멈춤|단색|누락"
)
_QUALITY_INTENT = re.compile(
    r"디코딩|프레임|손상|깨짐|끊김|정지|멈춤|단색|이상|문제|오류"
)
_UPLOAD_INTENT = re.compile(r"업로드|업로더|녹화.*(?:없|안\s*(?:됐|돼|되)|실패)")
_DECODE_INTENT = re.compile(r"디코딩|프레임|손상|깨짐|끊김|정지|멈춤|단색")
_DOCUMENT_INTENT = re.compile(
    r"문서|매뉴얼|플레이북|사용법|구현|소스\s*코드|\b(?:playbook|manual)\b",
    re.IGNORECASE,
)
_AGGREGATE_INTENT = re.compile(
    r"추이|추세|집계|통계|평균|증감|증가율|감소율|건수|횟수|몇\s*(?:건|개)|개수|목록|리스트"
)
_NUMERIC_COMPARISON = re.compile(
    r"\d[\d,]*(?:\.\d+)?\s*(?:%|건|개|회|초|분|시간|일|주|개월|달|년)?\s*(?:이상|이하|초과|미만)"
)


class _Request(Protocol):
    question: str
    metadata: Mapping[str, Any]
    context_entries: tuple[Any, ...]


@dataclass(frozen=True, slots=True)
class RecordingMediaDiagnosisScope:
    notification_id: int | None = None
    barcode: str | None = None
    device_name: str | None = None
    log_date: str | None = None
    file_id: str | None = None
    recorded_at: str | None = None
    reason: str | None = None

    @property
    def has_target(self) -> bool:
        return any((self.notification_id, self.file_id, self.barcode, self.device_name))

    def service_kwargs(self) -> dict[str, Any]:
        return {
            "notification_id": self.notification_id,
            "barcode": self.barcode,
            "device_name": self.device_name,
            "log_date": self.log_date,
            "file_id": self.file_id,
            "recorded_at": self.recorded_at,
        }


def needs_recording_media_diagnosis_context(question: str) -> bool:
    """현재 질문이 검사 의도를 가질 때만 bounded 알림 스레드를 요청한다."""
    text = str(question or "")
    # 수치 비교의 '이상'은 품질 이상이 아니다. 집계와 추이는 기존 조회가 소유한다.
    quality_text = _NUMERIC_COMPARISON.sub(" ", text)
    if _AGGREGATE_INTENT.search(text):
        return False
    if _NUMERIC_COMPARISON.search(text) and not _QUALITY_INTENT.search(quality_text):
        return False
    if _DOCUMENT_INTENT.search(text):
        return False
    if _UPLOAD_INTENT.search(text) and not _DECODE_INTENT.search(text):
        return False
    if _LIST_REQUEST.search(text) and not _ISSUE_INTENT.search(quality_text):
        return False
    return not _OTHER_OPERATION.search(text) and bool(
        (_MEDIA.search(text) and _QUESTION.search(quality_text))
        or _FOLLOWUP.fullmatch(text.strip())
    )


def _alert_texts(request: _Request) -> tuple[str, ...]:
    # 스레드 문구는 실행 지시나 신뢰된 ID가 아니라 DB가 다시 검증할 조회 힌트다.
    return tuple(
        str(entry.get("text") or "")[:20_000]
        for entry in request.context_entries[-50:]
        if isinstance(entry, Mapping) and _ALERT.search(str(entry.get("text") or ""))
    )


def match_recording_media_diagnosis_route(request: _Request) -> str | None:
    if request.metadata.get("route_group") != "operations":
        return None
    question = str(request.question or "")
    if not needs_recording_media_diagnosis_context(question):
        return None
    # 일반 영상 조회나 업로드 실패는 기존 barcode/failure route에 남긴다.
    explicit_quality = bool(
        _MEDIA.search(question)
        and _QUALITY_INTENT.search(_NUMERIC_COMPARISON.sub(" ", question))
    )
    if not explicit_quality and not _alert_texts(request):
        return None
    return RECORDING_MEDIA_DIAGNOSIS_ROUTE


def _parse_text_scope(
    text: str, *, alert: bool = False
) -> RecordingMediaDiagnosisScope:
    values = {
        "notification_id": {int(value) for value in _NOTIFICATION.findall(text)},
        "barcode": set(_BARCODE.findall(text)),
        "device_name": {value.upper() for value in _DEVICE.findall(text)},
        "file_id": set(_FILE.findall(text)),
    }
    # 알림에는 담당자의 확인일과 감지일도 있으므로 세션 시작일을 우선한다.
    session = _SESSION_DATE.search(text) if alert else None
    date_text = session.group(1) if session else text
    try:
        days = {
            date(int(year), int(month), int(day)).isoformat()
            for year, month, day in _DATE.findall(date_text)
        }
    except ValueError:
        return RecordingMediaDiagnosisScope(reason="invalid_date")
    if any(len(items) > 1 for items in values.values()) or len(days) > 1:
        return RecordingMediaDiagnosisScope(reason="ambiguous_scope")
    parsed = {name: next(iter(items), None) for name, items in values.items()}
    if parsed["notification_id"] == 0:
        return RecordingMediaDiagnosisScope(reason="invalid_notification_id")
    recorded_at = None
    session_time = _SESSION_TIME.search(text)
    if session_time:
        try:
            day = next(iter(days))
            recorded_at = datetime.strptime(
                f"{day} {session_time.group(2)}", "%Y-%m-%d %H:%M:%S"
            ).strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, StopIteration):
            return RecordingMediaDiagnosisScope(reason="invalid_date")
    return RecordingMediaDiagnosisScope(
        **parsed, log_date=next(iter(days), None), recorded_at=recorded_at
    )


def resolve_recording_media_diagnosis_scope(
    request: _Request,
) -> RecordingMediaDiagnosisScope:
    explicit = _parse_text_scope(str(request.question or ""))
    if explicit.reason:
        return explicit
    if explicit.has_target:
        # 현재 질문의 대상을 이전 알림의 다른 장비나 바코드와 섞지 않는다.
        return explicit
    candidates = set()
    for text in _alert_texts(request):
        scope = _parse_text_scope(text, alert=True)
        # 일반 댓글은 사건 후보를 만들지 않는다. 명시된 잘못된 범위는 그대로 차단한다.
        if scope.reason or scope.has_target:
            candidates.add(scope)
    if len(candidates) > 1:
        return RecordingMediaDiagnosisScope(reason="ambiguous_scope")
    if candidates:
        candidate = next(iter(candidates))
        if candidate.reason:
            return candidate
        if (
            explicit.log_date
            and candidate.log_date
            and explicit.log_date != candidate.log_date
        ):
            return RecordingMediaDiagnosisScope(reason="scope_mismatch")
        if candidate.has_target:
            return candidate
    return RecordingMediaDiagnosisScope(reason="scope_required")


__all__ = [
    "RECORDING_MEDIA_DIAGNOSIS_ROUTE",
    "RecordingMediaDiagnosisScope",
    "match_recording_media_diagnosis_route",
    "needs_recording_media_diagnosis_context",
    "resolve_recording_media_diagnosis_scope",
]
