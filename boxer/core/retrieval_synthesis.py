import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from anthropic import Anthropic

from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama
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


def _build_route_specific_rules(evidence_payload: Any) -> str:
    if not isinstance(evidence_payload, dict):
        return ""

    route = str(evidence_payload.get("route") or "").strip().lower()
    if route == "barcode_log_error_summary":
        return (
            "\n"
            "7) This is a barcode log error interpretation task. Do not restate the full raw logs.\n"
            "8) Answer in this exact section order:\n"
            "   - *에러 분석*\n"
            "   - • 핵심 원인:\n"
            "   - • 영향:\n"
            "   - • 근거 로그:\n"
            "   - • 권장 조치:\n"
            "   - • 확실도:\n"
            "9) Use only the provided evidence. If something is inferred, explicitly write '추정:'.\n"
            "10) Keep it concise and operational. Prefer 4~8 bullet lines total.\n"
            "11) In '근거 로그', cite compactly with time/component/message, not full long blocks.\n"
            "12) In '권장 조치', suggest practical checks that an operator can do immediately."
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
        return _ask_ollama(
            user_input,
            system_prompt=prompt,
            max_tokens=max_tokens,
            timeout_sec=ollama_timeout_sec,
        )

    return ""
