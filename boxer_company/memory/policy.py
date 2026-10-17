"""회사 기억에 들어가는 원문과 구조화 후보를 보수적으로 제한한다."""

from __future__ import annotations

import re
import unicodedata
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping
from urllib.parse import parse_qsl, urlsplit

from .contracts import ELIGIBLE_ROUTES, MemoryError, source_identity, source_revision

_SENSITIVE = re.compile(
    r"(?:xox[baprs]-|sk-ant-|github_pat_|gh[pousr]_|\b(?:AKIA|ASIA)[A-Z0-9]{16}\b|"
    r"-----BEGIN[^\n]*PRIVATE KEY|\bBearer\s+\S+|"
    r"(?:password|passwd|secret|api[_-]?key|access[_-]?token|authorization)\s*[:=]|"
    r"(?:비밀번호|주민등록|환자(?:명|이름|정보)|고객(?:명|이름|정보))|"
    r"\b01[016789][ -]?\d{3,4}[ -]?\d{4}\b|\b\d{6}-?[1-4]\d{6}\b|"
    r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b|"
    r"[?&](?:X-Amz-|token=|signature=|credential=)|"
    r"(?:시스템|개발자|system|developer)\s*(?:프롬프트|지시|prompt)|"
    r"(?:권한|정책|규칙|이전\s*지시).{0,12}(?:무시|우회)|"
    r"ignore.{0,20}(?:instructions|rules)|bypass.{0,15}(?:auth|policy)|"
    r"(?:ssh|sudo|curl|wget)\s+|\b(?:SELECT|INSERT|UPDATE|DELETE)\s+.{0,80}\b(?:FROM|INTO|SET)\b)",
    re.IGNORECASE,
)
_OPERATION = re.compile(
    r"(?:장비|바코드|녹화|환자|앱\s*유저|app.?user).{0,30}(?:조회|복구|다운로드|재시작|업데이트|정보|로그)|"
    r"(?:SQL|S3|PM2|SSH)\s*(?:조회|로그|실행|명령|접속)|\b\d{11}\b",
    re.IGNORECASE,
)
_TS = re.compile(r"^\d{1,20}\.\d{1,9}$")
_COMPLETION = re.compile(
    r"(?:(?:완료|해결)(?:했|됐|되었)|끝냈|끝났|마쳤|끝내뒀|끝내놓았|다\s*(?:했|해놨|해놓았|해뒀|해두었))"
    r"(?:습니다|어요|어|다|음)?(?=$|[\s.!。！])|"
    r"(?:완료|해결|완료함|해결함|마침)\s*(?=$|[.!。！])|\b(?:finished|completed|done)\b",
    re.IGNORECASE,
)
_UNCONFIRMED_COMPLETION = re.compile(
    r"[?？]|취소|철회|정정|아직|미완료|예정|해야|해줘|해\s*주세요|"
    r"(?:하지|되지|하진|되진)\s*(?:않|못)|(?:안|못)\s*(?:했|끝|마쳤|완료|해결)|"
    r"(?:완료|해결|끝냈|마쳤|다\s*(?:했|해놨|해뒀)).{0,12}(?:다고|다면|으면|는지|을까|나\b)|"
    r"(?:완료|해결|끝|마침).{0,12}(?:아니|않|못)|^\s*>|\b(?:said|says|not|never|if)\b|\b(?:haven't|hasn't|didn't)\b",
    re.IGNORECASE | re.MULTILINE,
)


def has_explicit_completion(text: str) -> bool:
    """추출·저장에 같은 완료 표현을 쓰며 부정·취소·질문은 완료로 바꾸지 않는다."""

    return bool(_COMPLETION.search(text) and not _UNCONFIRMED_COMPLETION.search(text))


def normalize_topic_key(value: str) -> str:
    """표기 차이만 정규화하며 서로 다른 의미의 주제를 추측해 합치지 않는다."""

    compact = re.sub(r"[\s_-]+", "", unicodedata.normalize("NFKC", value).strip().casefold())
    return {"responselength": "response_length", "responselanguage": "response_language", "responseorder": "response_order"}.get(compact, compact)


def is_safe_memory_text(text: str) -> bool:
    """위험한 부분을 요약 전에 제거하며 원문 hash가 달라지는 치환은 하지 않는다."""

    return bool(isinstance(text, str) and text.strip() and not _SENSITIVE.search(text) and not _OPERATION.search(text))


def standard_preference_value(topic_key: str, text: str) -> str | None:
    """표준 선호의 단순 동의어만 합치고 부정·조건·추가 요구는 거부한다."""

    patterns = {
        "response_length": {
            "short": r"짧(?:은|게|고)|간결(?:한|하게|하고)|간단(?:한|히|하게|하고)|핵심만",
            "detailed": r"자세(?:한|히|하게|하고)|상세(?:한|히|하게|하고)|길(?:게|고)|긴",
        },
        "response_language": {"ko": r"한국어|한글", "en": r"영어"},
        "response_order": {
            "conclusion_first": r"결론(?:을)?(?:먼저|우선)|결론부터",
            "explanation_first": r"설명(?:을)?먼저|설명부터",
        },
    }
    choices = patterns.get(topic_key)
    if choices is None:
        return None
    remainder = re.sub(r"\s+", "", text)
    values = set()
    for value, pattern in choices.items():
        remainder, count = re.subn(pattern, "#", remainder)
        if count:
            values.add(value)
    if len(values) != 1:
        return None
    # 문법 이외의 숫자 제한·예외·예시 요청을 제거해 의미를 바꾸지 않는다.
    grammar = r"앞으로도|앞으로|항상|계속|평소|답변하는것을|답하는것을|답변해|답변|응답|설명하는|설명해|설명|말하는|말해|받는것을|받기를|해주기를|주기를|선호함|선호한다|선호해|선호|원함|원한다|방식의|방식|형태의|형태|스타일의|스타일|으로|로|을|를|은|는|이|가"
    remainder = re.sub(grammar, "", remainder)
    return next(iter(values)) if re.fullmatch(r"[#!.,·]*", remainder) else None


def personal_style_text(topic_key: str, value: str) -> str | None:
    """모델에는 저장 원문 대신 고정된 선호 문구만 전달한다."""

    return {
        ("response_length", "short"): "짧은 답변을 선호함",
        ("response_length", "detailed"): "자세한 답변을 선호함",
        ("response_language", "ko"): "한국어 답변을 선호함",
        ("response_language", "en"): "영어 답변을 선호함",
        ("response_order", "conclusion_first"): "결론을 먼저 설명하는 답변을 선호함",
        ("response_order", "explanation_first"): "설명을 먼저 하는 답변을 선호함",
    }.get((topic_key, value))


def is_allowed_conversation_source(text: str) -> bool:
    """실제 회사 operation matcher로 과거 민감 요청이 기억 경계를 우회하지 못하게 한다."""

    if not is_safe_memory_text(text):
        return False
    # 명령 parser가 policy를 함께 import해도 모듈 초기화 순환을 만들지 않는다.
    from boxer_company.assistant.contracts import CompanyAssistantRequest
    from boxer_company.memory.commands import MEMORY_MANAGE_ROUTE, parse_memory_command
    from boxer_company.operation_routing import match_company_operation_route

    question = re.sub(r"<@[A-Za-z0-9]+>", "", text).strip()
    request = CompanyAssistantRequest(
        request_id="memory-source-classification", tenant_id="memory", actor_id="memory",
        channel="slack", conversation_id="memory", question=question, locale="ko-KR",
    )
    route = match_company_operation_route(request)
    if route == MEMORY_MANAGE_ROUTE:
        command = parse_memory_command(question)
        return command is not None and command.action == "set_preference"
    return route is None


def _valid_permalink(value: str, channel_id: str, message_ts: str, thread_ts: str) -> bool:
    try:
        parsed = urlsplit(value)
        query = parse_qsl(parsed.query, keep_blank_values=True)
        params = dict(query)
        return bool(
            parsed.scheme == "https" and str(parsed.hostname or "").endswith(".slack.com")
            and parsed.port in {None, 443} and not parsed.username and not parsed.password
            and not parsed.fragment and parsed.path.rstrip("/") == f"/archives/{channel_id}/p{message_ts.replace('.', '')}"
            and len(params) == len(query) and set(params) <= {"thread_ts", "cid"}
            and params.get("thread_ts", thread_ts) == thread_ts
            and params.get("cid", channel_id) == channel_id
        )
    except ValueError:
        return False


def normalize_source(
    source: Mapping[str, Any], *, tenant_id: str, channel_id: str,
    thread_ts: str, current_message_ts: str,
) -> dict[str, Any] | None:
    """scope는 capture로 고정하고 본문에 적힌 tenant/channel은 채택하지 않는다."""

    # scope・작성자는 adapter의 실제 Slack 원본 계약만 받고 모델 출력은 받지 않는다.
    text = str(source.get("text") or "").strip()
    message_ts = str(source.get("messageTs") or "")
    author = str(source.get("authorId") or "")
    kind = source.get("authorKind")
    edited = source.get("editedAt") or ""
    revision = str(source.get("revisionHash") or "")
    permalink = str(source.get("permalink") or "")
    if (not _TS.fullmatch(message_ts) or not author or len(author) > 128
            or kind not in {"human", "boxer"} or not is_allowed_conversation_source(text)):
        return None
    if edited and not _TS.fullmatch(str(edited)):
        raise MemoryError("source_edited_timestamp_invalid")
    if revision != source_revision(text, str(edited)):
        raise MemoryError("source_revision_mismatch")
    if not _valid_permalink(permalink, channel_id, message_ts, thread_ts):
        raise MemoryError("source_permalink_invalid")
    try:
        if Decimal(message_ts) < Decimal(thread_ts):
            raise MemoryError("source_before_thread")
        if kind == "human" and Decimal(message_ts) > Decimal(current_message_ts):
            raise MemoryError("source_after_capture_cutoff")
    except InvalidOperation as exc:
        raise MemoryError("source_timestamp_invalid") from exc
    if kind == "boxer" and (source.get("deliveryVerified") is not True or source.get("route") not in ELIGIBLE_ROUTES):
        return None
    return {
        "sourceId": source_identity(tenant_id, channel_id, message_ts, revision),
        "messageTs": message_ts, "revisionHash": revision, "authorId": author,
        "authorKind": kind, "text": text, "permalink": permalink,
        "createdAt": str(source.get("createdAt") or message_ts), "editedAt": str(edited),
    }


def validate_candidate(candidate: Mapping[str, Any], sources: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    """LLM 후보의 인용·주체·유형을 저장 직전에도 확인한다."""

    kind, category = candidate.get("kind"), candidate.get("category")
    evidence = candidate.get("evidenceType")
    text = str(candidate.get("text") or "").strip()
    raw_topic = str(candidate.get("topicKey") or "").strip()
    topic = normalize_topic_key(raw_topic)
    subject = candidate.get("subjectUserId") or None
    refs = candidate.get("sources")
    if (kind not in {"person", "thread", "team"}
            or category not in {"preference", "fact", "interest", "decision", "task", "open_question"}
            or evidence not in {"self_statement", "attributed_statement", "inference", "explicit_agreement"}
            or not is_safe_memory_text(text) or len(text) > 1000
            or not topic or len(topic) > 160 or not is_safe_memory_text(raw_topic) or not is_safe_memory_text(topic)
            or not isinstance(refs, list) or not refs or len(refs) > 12
            or (kind == "person" and not subject)):
        raise MemoryError("memory_candidate_invalid")
    normalized_refs = []
    humans: set[str] = set()
    for ref in refs:
        source = sources.get(str(ref.get("sourceId") or "")) if isinstance(ref, Mapping) else None
        quote = str(ref.get("quote") or "").strip() if isinstance(ref, Mapping) else ""
        relation = ref.get("relation", "supports") if isinstance(ref, Mapping) else ""
        if not source or not quote or len(quote) > 500 or quote not in str(source.get("text") or "") or relation not in {"supports", "corrects", "disputes"}:
            raise MemoryError("memory_evidence_invalid")
        if source.get("authorKind") == "human":
            humans.add(str(source["authorId"]))
        normalized_refs.append({"sourceId": source["sourceId"], "quote": quote, "relation": relation})
    # 봇의 재진술만으로 사람의 선호·사실이나 팀 결정을 만들지 않는다.
    if not humans or (evidence == "self_statement" and subject is not None and subject not in humans):
        raise MemoryError("memory_author_invalid")
    if category == "preference" and (kind != "person" or evidence != "self_statement" or humans != {subject}):
        raise MemoryError("memory_preference_invalid")
    if evidence == "explicit_agreement" and len(humans) < 2:
        raise MemoryError("memory_agreement_invalid")
    subjects = candidate.get("subjects") or []
    if not isinstance(subjects, list) or len(subjects) > 20:
        raise MemoryError("memory_subject_invalid")
    normalized_subjects = []
    for entry in subjects:
        if not isinstance(entry, Mapping) or entry.get("role") not in {"subject", "assignee", "requester", "agreeing_actor"} or not entry.get("userId"):
            raise MemoryError("memory_subject_invalid")
        if entry["role"] == "agreeing_actor" and entry["userId"] not in humans:
            raise MemoryError("memory_agreement_invalid")
        normalized_subjects.append({"userId": str(entry["userId"]), "role": str(entry["role"])})
    action = candidate.get("action", "add")
    if action not in {"add", "replace", "resolve"}:
        raise MemoryError("memory_action_invalid")
    return {
        "kind": kind, "category": category, "subjectUserId": subject, "topicKey": topic,
        "text": text, "evidenceType": evidence, "sources": normalized_refs,
        "subjects": normalized_subjects, "action": action, "replacesId": candidate.get("replacesId"),
    }
