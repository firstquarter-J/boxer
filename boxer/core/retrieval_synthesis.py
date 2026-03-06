import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from anthropic import Anthropic

from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat
from boxer.core.utils import _truncate_text

_PHONE_PATTERN = re.compile(r"\b01[016789]-?\d{3,4}-?\d{4}\b")
_EMAIL_PATTERN = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
_NAME_KEYWORDS = (
    "realname",
    "fullname",
    "username",
    "userrealname",
    "mothername",
    "babyname",
    "babynickname",
)
_PHONE_KEYWORDS = ("phone", "phonenumber", "mobile", "tel")
_EMAIL_KEYWORDS = ("email",)


def _json_default(value: Any) -> Any:
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _mask_phone(text: str) -> str:
    digits = "".join(char for char in text if char.isdigit())
    if len(digits) < 7:
        return "***"
    return f"{digits[:3]}****{digits[-4:]}"


def _mask_name(text: str) -> str:
    clean = (text or "").strip()
    if not clean:
        return ""
    if len(clean) <= 1:
        return "*"
    if len(clean) == 2:
        return clean[0] + "*"
    return clean[0] + "*" * (len(clean) - 2) + clean[-1]


def _mask_text(text: str) -> str:
    masked = _PHONE_PATTERN.sub(lambda m: _mask_phone(m.group(0)), text)
    masked = _EMAIL_PATTERN.sub("***@***", masked)
    return masked


def _mask_by_key(key: str, value: Any) -> Any:
    lowered = key.lower()
    if isinstance(value, str):
        if any(token in lowered for token in _PHONE_KEYWORDS):
            return _mask_phone(value)
        if any(token in lowered for token in _EMAIL_KEYWORDS):
            return "***@***"
        if any(token in lowered for token in _NAME_KEYWORDS):
            return _mask_name(value)
        return _mask_text(value)
    if isinstance(value, dict):
        return {
            nested_key: _mask_by_key(str(nested_key), nested_value)
            for nested_key, nested_value in value.items()
        }
    if isinstance(value, list):
        return [_mask_by_key(key, item) for item in value]
    return value


def _mask_evidence_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return {
            key: _mask_by_key(str(key), value)
            for key, value in payload.items()
        }
    if isinstance(payload, list):
        return [_mask_evidence_payload(item) for item in payload]
    if isinstance(payload, str):
        return _mask_text(payload)
    return payload


def _serialize_evidence_payload(payload: Any) -> str:
    raw = json.dumps(payload, ensure_ascii=False, default=_json_default, separators=(",", ":"))
    return _truncate_text(raw, max(500, s.LLM_SYNTHESIS_MAX_EVIDENCE_CHARS))


def _compact_barcode_log_error_summary_payload(payload: Any) -> Any:
    if not isinstance(payload, dict):
        return payload

    route = str(payload.get("route") or "").strip().lower()
    if route != "barcode_log_error_summary":
        return payload

    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    request = payload.get("request") if isinstance(payload.get("request"), dict) else {}
    records = payload.get("records") if isinstance(payload.get("records"), list) else []
    error_groups = payload.get("errorGroups") if isinstance(payload.get("errorGroups"), list) else []

    compact_records: list[dict[str, Any]] = []
    for record in records[:3]:
        if not isinstance(record, dict):
            continue

        compact_restart_events = []
        for event in (record.get("restartEvents") or [])[:3]:
            if not isinstance(event, dict):
                continue
            compact_restart_events.append(
                {
                    "time": event.get("time"),
                    "label": event.get("label"),
                    "rawLine": event.get("rawLine"),
                }
            )

        compact_error_groups = []
        for group in (record.get("errorGroups") or [])[:6]:
            if not isinstance(group, dict):
                continue
            compact_error_groups.append(
                {
                    "component": group.get("component"),
                    "signature": group.get("signature"),
                    "count": group.get("count"),
                    "sampleTime": group.get("sampleTime"),
                    "sampleMessage": group.get("sampleMessage"),
                }
            )

        compact_error_lines = []
        for line in (record.get("errorLines") or [])[:6]:
            if not isinstance(line, dict):
                continue
            compact_error_lines.append(
                {
                    "time": line.get("time"),
                    "component": line.get("component"),
                    "message": line.get("message"),
                }
            )

        compact_session_diagnostics = []
        for item in (record.get("sessionDiagnostics") or [])[:6]:
            if not isinstance(item, dict):
                continue
            compact_session_diagnostics.append(
                {
                    "index": item.get("index"),
                    "startTime": item.get("startTime"),
                    "stopTime": item.get("stopTime"),
                    "severity": item.get("severity"),
                    "finishDelay": item.get("finishDelay"),
                    "postStopScanCount": item.get("postStopScanCount"),
                    "postStopStopCount": item.get("postStopStopCount"),
                    "postStopSnapCount": item.get("postStopSnapCount"),
                    "postStopDeviceErrorCount": item.get("postStopDeviceErrorCount"),
                    "displayText": item.get("displayText"),
                }
            )

        compact_records.append(
            {
                "deviceName": record.get("deviceName"),
                "hospitalName": record.get("hospitalName"),
                "roomName": record.get("roomName"),
                "date": record.get("date"),
                "recordingsOnDateCount": record.get("recordingsOnDateCount"),
                "recordingsOnDateStatuses": record.get("recordingsOnDateStatuses"),
                "sessions": record.get("sessions"),
                "restartDetected": record.get("restartDetected"),
                "restartEvents": compact_restart_events,
                "scanEventCount": record.get("scanEventCount"),
                "errorLineCount": record.get("errorLineCount"),
                "errorGroups": compact_error_groups,
                "errorLines": compact_error_lines,
                "sessionDiagnostics": compact_session_diagnostics,
            }
        )

    compact_top_groups = []
    for group in error_groups[:8]:
        if not isinstance(group, dict):
            continue
        compact_top_groups.append(
            {
                "component": group.get("component"),
                "signature": group.get("signature"),
                "count": group.get("count"),
                "sampleTime": group.get("sampleTime"),
                "sampleMessage": group.get("sampleMessage"),
            }
        )

    return {
        "route": payload.get("route"),
        "source": payload.get("source"),
        "request": request,
        "summary": summary,
        "records": compact_records,
        "errorGroups": compact_top_groups,
    }


def _build_route_specific_rules(evidence_payload: Any) -> str:
    if not isinstance(evidence_payload, dict):
        return ""

    route = str(evidence_payload.get("route") or "").strip().lower()
    if route == "barcode_log_error_summary":
        return (
            "\n"
            "7) 이 작업은 바코드 로그 에러 해석이다. 원문 로그를 길게 다시 쓰지 마.\n"
            "8) 아래 형식 그대로만 답해:\n"
            "   *에러 분석*\n"
            "   • 핵심 원인:\n"
            "   • 영향:\n"
            "   • 근거 로그:\n"
            "   • 권장 조치:\n"
            "   • 확실도:\n"
            "9) 반드시 한국어만 사용해. 영어 설명, 자기 사고 과정, 중간 추론, 검토 문장은 절대 쓰지 마.\n"
            "10) 제공된 evidence만 사용해. 추정이면 반드시 '추정:'으로 시작해.\n"
            "11) 6줄 안팎으로 짧게 끝내. 장황한 설명 금지.\n"
            "12) '근거 로그'는 시간/컴포넌트/핵심 메시지만 짧게 적어.\n"
            "13) restartEvents가 있으면 세션 중 재시작을 1차 원인으로 명확히 적고, `정상 녹화 실패로 판단`이라고 확정형으로 써. 가능성/추정 표현을 쓰지 마.\n"
            "14) ffmpeg 관련 오류가 보이면 '권장 조치'의 1순위는 캡처보드 연결 상태와 입력 신호 점검으로 적어.\n"
            "15) ffmpeg 로그에 DTS/invalid dropping/non-monotonous dts/timestamp 이상이 보이면 캡처보드 연결 불량 또는 캡처보드 고장을 우선 의심한다고 명확히 적어.\n"
            "16) 세션 시작 시각과 첫 ffmpeg 오류 시각이 evidence에 있으면 근거 로그에 반드시 같이 적어.\n"
            "17) `C_STOPSESS`가 확인돼 종료는 정상이어도 ffmpeg 오류가 있으면 종료 상태와 녹화 결과를 분리해서 설명해.\n"
            "18) `Standby error`만 있어도 영상 손상 가능성을 의심해야 한다. 이후 녹화 시작 흔적이 있어도 손상 가능성 판단을 제거하지 말고, 실제 영상 확인이 필요하다고 적어.\n"
            "19) 다만 sessionDiagnostics에 종료 처리 지연, 종료 후 추가 스캔, 종료 후 장치 오류가 있으면 이 신호를 초기 standby error보다 더 강한 이상 징후로 우선 해석해.\n"
            "20) `Couldn't renew JWT`, `Send Status: Failed`, `sendScreenShotBase64`, `sendCurrentFrameSnapBase64`, `sendDailyLog`, `Uploader ... couldn't be sent`, `getaddrinfo EAI_AGAIN` 같은 Endpoint/Uploader 통신 오류는 그것만으로 녹화 실패 원인이라고 판단하지 마.\n"
            "21) 위 통신 오류만 있고 종료 스캔/녹화 흐름이 정상이라면, 녹화 실패가 아니라 상태 전송/스크린샷/업로드 통신 오류로 설명해.\n"
            "22) evidence에 날짜 기준 DB 영상 기록(recordingsOnDateCount)이 있으면 반드시 같이 해석해. DB 영상 기록이 있으면 업로드 최종 성공 근거로 보고, 없으면 업로드 실패 가능성을 언급해.\n"
        )

    if route != "barcode_log_analysis":
        return ""

    request_payload = evidence_payload.get("request") if isinstance(evidence_payload, dict) else None
    mode = ""
    if isinstance(request_payload, dict):
        mode = str(request_payload.get("mode") or "").strip().lower()
    is_error_mode = "error" in mode

    common_rules = (
        "\n"
        "7) For barcode log analysis, keep this field order and labels explicitly:\n"
        "   - 매핑 장비:\n"
        "   - 병원:\n"
        "   - 병실:\n"
        "   - 날짜:\n"
        "8) If scanned/motion events exist in evidence, render them together under 'scanned 이벤트' as one compact code-block timeline in chronological order.\n"
        "9) The scanned count must count only real scanned tokens (exclude motion entries from the count).\n"
        "10) Do not collapse scanned events into only summary counts.\n"
        "11) If error lines exist in evidence, render them under 'error 라인' as one compact code-block timeline with time labels in chronological order. Do not summarize away individual lines.\n"
        "12) Never omit the date in barcode log analysis answers.\n"
        "13) If evidence contains notionPlaybook/notion references, include a '참고 플레이북' section and cite only those references."
    )
    if not is_error_mode:
        return common_rules

    return (
        common_rules
        + "\n"
        "14) For error-focused analysis, add these sections in order:\n"
        "    - 에러 요약\n"
        "    - 관찰된 에러 패턴(시간/컴포넌트/핵심 메시지)\n"
        "    - 가능 원인(근거 라인 기반, 확실/추정 구분)\n"
        "    - 즉시 확인할 항목(로그/메트릭/설정)\n"
        "    - 우선 조치(1~3순위)\n"
        "15) For causes, never guess without evidence. If inferred, prefix with '추정:'."
    )


def _build_retrieval_synthesis_input(
    question: str,
    thread_context: str,
    evidence_payload: Any,
) -> str:
    evidence_text = _serialize_evidence_payload(evidence_payload)
    normalized_question = (question or "").strip()
    route_rules = _build_route_specific_rules(evidence_payload)

    if thread_context:
        return (
            "Thread context (older -> newer):\n"
            f"{thread_context}\n\n"
            "User question:\n"
            f"{normalized_question}\n\n"
            "Evidence(JSON):\n"
            f"{evidence_text}\n\n"
            "Output rules:\n"
            "1) Answer in Korean.\n"
            "2) Use only evidence.\n"
            "3) If evidence is insufficient, say what is missing.\n"
            "4) Do not claim actions or results not in evidence.\n"
            "5) Do not suggest using another barcode/service unless evidence explicitly says so.\n"
            "6) For factual checks, start with direct yes/no and one-sentence reason."
            f"{route_rules}"
        )

    return (
        "User question:\n"
        f"{normalized_question}\n\n"
        "Evidence(JSON):\n"
        f"{evidence_text}\n\n"
        "Output rules:\n"
        "1) Answer in Korean.\n"
        "2) Use only evidence.\n"
        "3) If evidence is insufficient, say what is missing.\n"
        "4) Do not claim actions or results not in evidence.\n"
        "5) Do not suggest using another barcode/service unless evidence explicitly says so.\n"
        "6) For factual checks, start with direct yes/no and one-sentence reason."
        f"{route_rules}"
    )


def _synthesize_retrieval_answer(
    question: str,
    thread_context: str,
    evidence_payload: Any,
    *,
    provider: str,
    claude_client: Anthropic | None,
    system_prompt: str | None = None,
    max_tokens: int | None = None,
    ollama_timeout_sec: int | None = None,
) -> str:
    normalized_provider = (provider or "").lower().strip()
    if not normalized_provider:
        return ""

    payload = evidence_payload
    if s.LLM_SYNTHESIS_MASKING_ENABLED:
        payload = _mask_evidence_payload(evidence_payload)
    payload = _compact_barcode_log_error_summary_payload(payload)

    user_input = _build_retrieval_synthesis_input(
        question=question,
        thread_context=thread_context,
        evidence_payload=payload,
    )
    prompt = (system_prompt or s.RETRIEVAL_SYNTHESIS_SYSTEM_PROMPT).strip()

    if normalized_provider == "claude":
        if claude_client is None:
            return ""
        return _ask_claude(
            claude_client,
            user_input,
            system_prompt=prompt,
            max_tokens=max_tokens,
        )

    if normalized_provider == "ollama":
        return _ask_ollama_chat(
            user_input,
            system_prompt=prompt,
            max_tokens=max_tokens,
            timeout_sec=ollama_timeout_sec,
            think=False,
        )

    return ""
