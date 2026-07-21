import hashlib
import json
import logging
import os
import re
import threading
import time
import urllib.parse
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat
from boxer.retrieval.connectors.notion import (
    _extract_block_text,
    _fetch_all_notion_blocks,
    _load_notion_page_content_cached,
    _normalize_notion_id,
    _notion_request,
)
from boxer_company import settings as cs
from boxer_company.notion_playbooks import (
    _RAG_INDEX_HEADING,
    _company_notion_token,
    _invalidate_notion_playbook_cache,
    _is_company_notion_configured,
    _parse_notion_rag_index_line,
    _resolve_notion_root_page_id,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ThreadPlaybookDraft:
    title: str
    symptom: str
    cause: str
    answer_template: str
    checks: list[str]
    keywords: list[str]
    source_notes: list[str]


@dataclass(frozen=True)
class ThreadPlaybookSaveResult:
    title: str
    page_id: str
    url: str
    keywords: list[str]
    rag_index_updated: bool
    created: bool = True


@dataclass(frozen=True)
class ThreadSourcePendingReservation:
    block_id: str
    owner: str
    updated_at: int
    created_time: str


@dataclass(frozen=True)
class ThreadSourceIndexState:
    page_id: str | None
    pending_block_id: str | None
    pending_reservations: tuple[ThreadSourcePendingReservation, ...]
    pending_owner: str
    pending_updated_at: int
    insert_after_block_id: str | None
    found_index_heading: bool
    migration_complete: bool

    @property
    def pending_block_ids(self) -> tuple[str, ...]:
        return tuple(reservation.block_id for reservation in self.pending_reservations)


_CODE_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)
_THREAD_SOURCE_INDEX_HEADING = "Slack 스레드 소스 인덱스"
_THREAD_SOURCE_KEY_PATTERN = re.compile(r"^slack-(?:thread|permalink):v1:[0-9a-f]{64}$")
_THREAD_SOURCE_INDEX_LINE_PATTERN = re.compile(
    r"^source_key=(?P<source_key>slack-(?:thread|permalink):v1:[0-9a-f]{64})\s*\|\s*"
    r"page_id=(?P<page_id>[0-9a-fA-F-]{32,36})$"
)
_THREAD_SOURCE_PENDING_LINE_PATTERN = re.compile(
    r"^source_key=(?P<source_key>slack-thread:v1:[0-9a-f]{64})\s*\|\s*"
    r"status=pending\s*\|\s*owner=(?P<owner>[0-9a-f]{12})\s*\|\s*"
    r"updated_at=(?P<updated_at>\d+)$"
)
_THREAD_SOURCE_MIGRATION_MARKER = "migration=slack-permalink:v1 | status=complete"
_THREAD_SOURCE_PENDING_TTL_SEC = 300
_THREAD_SOURCE_RETRY_GRACE_SEC = 15
_THREAD_SOURCE_OWNER_SEED = uuid.uuid4().hex
# 고정 stripe lock은 source별 중복 생성을 막으면서 완료된 thread 수만큼 lock이 누적되지 않게 한다.
_THREAD_SOURCE_LOCKS = tuple(threading.Lock() for _ in range(64))
# 서로 다른 source가 같은 Notion root index를 동시에 수정하는 경쟁은 root 단위로 직렬화한다.
_THREAD_ROOT_INDEX_LOCKS = tuple(threading.RLock() for _ in range(16))


def _build_slack_thread_source_key(workspace_id: str, channel_id: str, thread_ts: str) -> str:
    components = [str(value or "").strip() for value in (workspace_id, channel_id, thread_ts)]
    if not all(components):
        raise ValueError("Slack workspace/channel/thread 식별자가 필요해")

    # 길이 prefix를 넣어 구분 문자가 값에 포함돼도 서로 다른 thread가 같은 키가 되지 않게 한다.
    identity = "".join(f"{len(value)}:{value}" for value in components)
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return f"slack-thread:v1:{digest}"


def _canonicalize_slack_thread_permalink(thread_permalink: str | None) -> str:
    value = str(thread_permalink or "").strip()
    if value.startswith("<") and value.endswith(">"):
        value = value[1:-1].split("|", 1)[0].strip()
    if not value:
        return ""

    parsed = urllib.parse.urlsplit(value)
    if not parsed.scheme or not parsed.netloc or not parsed.path:
        return ""
    return urllib.parse.urlunsplit(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            parsed.path.rstrip("/"),
            "",
            "",
        )
    )


def _build_slack_permalink_source_key(thread_permalink: str | None) -> str:
    canonical_permalink = _canonicalize_slack_thread_permalink(thread_permalink)
    if not canonical_permalink:
        return ""
    digest = hashlib.sha256(canonical_permalink.encode("utf-8")).hexdigest()
    return f"slack-permalink:v1:{digest}"


def _thread_source_lock(source_key: str) -> threading.Lock:
    lock_index = (
        int(hashlib.sha256(source_key.encode("utf-8")).hexdigest(), 16)
        % len(_THREAD_SOURCE_LOCKS)
    )
    return _THREAD_SOURCE_LOCKS[lock_index]


def _thread_root_index_lock(root_page_id: str) -> threading.RLock:
    lock_index = (
        int(hashlib.sha256(root_page_id.encode("utf-8")).hexdigest(), 16)
        % len(_THREAD_ROOT_INDEX_LOCKS)
    )
    return _THREAD_ROOT_INDEX_LOCKS[lock_index]


def _thread_source_owner() -> str:
    # pre-fork 서버에서도 worker별 owner가 달라지도록 process id를 seed에 포함한다.
    owner_identity = f"{_THREAD_SOURCE_OWNER_SEED}:{os.getpid()}"
    return hashlib.sha256(owner_identity.encode("utf-8")).hexdigest()[:12]


def _require_company_notion_token() -> str:
    notion_token = _company_notion_token()
    if not notion_token:
        raise RuntimeError("NOTION_TOKEN_COMPANY가 필요해")
    return notion_token


def _is_thread_playbook_learning_request(question: str) -> bool:
    normalized = re.sub(r"\s+", "", (question or "").strip().lower())
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "이스레드학습",
            "스레드학습",
            "스레드학습해",
            "스레드학습저장",
            "스레드학습시켜",
            "thread학습",
            "thread저장",
            "쓰레드학습",
        )
    )


def _clean_text(value: Any, *, max_chars: int = 1000) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if len(text) <= max_chars:
        return text
    return text[: max(1, max_chars - 1)].rstrip() + "..."


def _clean_list(value: Any, *, max_items: int = 8, max_chars: int = 180) -> list[str]:
    if isinstance(value, str):
        raw_items = re.split(r"[\n,]+", value)
    elif isinstance(value, list):
        raw_items = value
    else:
        raw_items = []

    cleaned: list[str] = []
    seen: set[str] = set()
    for raw_item in raw_items:
        item = _clean_text(raw_item, max_chars=max_chars)
        if not item or item in seen:
            continue
        seen.add(item)
        cleaned.append(item)
        if len(cleaned) >= max(1, max_items):
            break
    return cleaned


def _extract_json_object(text: str) -> dict[str, Any]:
    cleaned = _CODE_FENCE_RE.sub("", (text or "").strip()).strip()
    if not cleaned:
        raise ValueError("LLM 응답이 비어있어")

    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start < 0 or end <= start:
        raise ValueError("LLM 응답에서 JSON 객체를 찾지 못했어")
    return json.loads(cleaned[start : end + 1])


def _build_learning_prompt(thread_context: str, *, thread_permalink: str | None) -> str:
    source_line = f"Slack thread permalink: {thread_permalink}" if thread_permalink else ""
    return (
        "다음 Slack 스레드는 운영 질문-답변 사례야. "
        "앞으로 같은 질문에 답할 수 있도록 Notion 운영 플레이북 항목으로 요약해.\n"
        "규칙:\n"
        "- 마지막 확정 답변과 명시된 근거를 우선해.\n"
        "- 추정은 원인으로 쓰지 말고 확인 포인트로만 남겨.\n"
        "- 회사 내부 사람 이름보다 증상, 원인, 확인 방법 중심으로 써.\n"
        "- JSON 객체만 반환해. Markdown 코드블록은 쓰지 마.\n"
        "- 모든 값은 한국어로 써.\n\n"
        "JSON schema:\n"
        "{\n"
        '  "title": "짧은 플레이북 제목",\n'
        '  "symptom": "사용자가 관찰한 증상",\n'
        '  "cause": "확인된 원인 또는 동작 기준",\n'
        '  "answerTemplate": "Slack에 바로 답할 수 있는 짧은 답변",\n'
        '  "checks": ["확인 방법 1", "확인 방법 2"],\n'
        '  "keywords": ["검색 키워드 1", "검색 키워드 2"],\n'
        '  "sourceNotes": ["근거 요약 1", "근거 요약 2"]\n'
        "}\n\n"
        f"{source_line}\n"
        "Slack thread:\n"
        f"{thread_context}"
    )


def _parse_thread_playbook_draft(raw_answer: str) -> ThreadPlaybookDraft:
    payload = _extract_json_object(raw_answer)
    title = _clean_text(payload.get("title"), max_chars=90)
    symptom = _clean_text(payload.get("symptom"), max_chars=1000)
    cause = _clean_text(payload.get("cause"), max_chars=1000)
    answer_template = _clean_text(payload.get("answerTemplate"), max_chars=1000)
    checks = _clean_list(payload.get("checks"), max_items=8, max_chars=220)
    keywords = _clean_list(payload.get("keywords"), max_items=16, max_chars=40)
    source_notes = _clean_list(payload.get("sourceNotes"), max_items=8, max_chars=220)

    if not title:
        title = "Slack 스레드 학습 사례"
    if not symptom:
        symptom = "스레드에서 확인 필요"
    if not cause:
        cause = "스레드에서 확인 필요"
    if not answer_template:
        answer_template = cause

    return ThreadPlaybookDraft(
        title=title,
        symptom=symptom,
        cause=cause,
        answer_template=answer_template,
        checks=checks,
        keywords=keywords,
        source_notes=source_notes,
    )


def _generate_thread_playbook_draft(
    thread_context: str,
    *,
    thread_permalink: str | None = None,
    claude_client: Any = None,
) -> ThreadPlaybookDraft:
    trimmed_context = (thread_context or "").strip()[: max(1, cs.THREAD_PLAYBOOK_LEARNING_MAX_THREAD_CHARS)]
    if not trimmed_context:
        raise ValueError("학습할 Slack 스레드 내용이 없어")

    prompt = _build_learning_prompt(trimmed_context, thread_permalink=thread_permalink)
    provider = (s.LLM_PROVIDER or "").strip().lower()
    if provider == "claude":
        if claude_client is None:
            raise RuntimeError("Claude client가 없어 스레드를 분석할 수 없어")
        raw_answer = _ask_claude(
            claude_client,
            prompt,
            system_prompt=(
                "You convert operational Slack Q&A threads into concise Korean playbook entries. "
                "Return strict JSON only."
            ),
            max_tokens=max(1, cs.THREAD_PLAYBOOK_LEARNING_MAX_TOKENS),
        )
    elif provider == "ollama":
        raw_answer = _ask_ollama_chat(
            prompt,
            system_prompt=(
                "You convert operational Slack Q&A threads into concise Korean playbook entries. "
                "Return strict JSON only."
            ),
            max_tokens=max(1, cs.THREAD_PLAYBOOK_LEARNING_MAX_TOKENS),
            temperature=0.0,
            think=False,
        )
    else:
        raise RuntimeError("스레드 분석에 사용할 LLM_PROVIDER가 설정되지 않았어")

    return _parse_thread_playbook_draft(raw_answer)


def _build_rich_text(content: str) -> list[dict[str, Any]]:
    text = str(content or "").strip() or "없음"
    chunks = [text[index : index + 2000] for index in range(0, len(text), 2000)]
    return [{"type": "text", "text": {"content": chunk}} for chunk in chunks[:100]]


def _paragraph(content: str) -> dict[str, Any]:
    return {"object": "block", "type": "paragraph", "paragraph": {"rich_text": _build_rich_text(content)}}


def _heading_2(content: str) -> dict[str, Any]:
    return {"object": "block", "type": "heading_2", "heading_2": {"rich_text": _build_rich_text(content)}}


def _bulleted_item(content: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "bulleted_list_item",
        "bulleted_list_item": {"rich_text": _build_rich_text(content)},
    }


def _quote(content: str) -> dict[str, Any]:
    return {"object": "block", "type": "quote", "quote": {"rich_text": _build_rich_text(content)}}


def _unique_keywords(draft: ThreadPlaybookDraft) -> list[str]:
    candidates = [
        *draft.keywords,
        *re.findall(r"[0-9A-Za-z가-힣._+-]{2,}", draft.title),
        *re.findall(r"v?\d+\.\d+(?:\.\d+)?", f"{draft.symptom} {draft.cause}"),
    ]
    keywords: list[str] = []
    seen: set[str] = set()
    for raw_candidate in candidates:
        candidate = _clean_text(raw_candidate, max_chars=40)
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        keywords.append(candidate)
        if len(keywords) >= 16:
            break
    return keywords


def _build_playbook_page_children(
    draft: ThreadPlaybookDraft,
    *,
    source_key: str | None,
    thread_permalink: str | None,
    learned_by_user_id: str | None,
) -> list[dict[str, Any]]:
    now_kst = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S KST")
    children: list[dict[str, Any]] = [
        _paragraph(f"Slack 스레드에서 학습한 운영 플레이북이야. 생성 시각: {now_kst}"),
        _heading_2("증상"),
        _paragraph(draft.symptom),
        _heading_2("원인"),
        _paragraph(draft.cause),
        _heading_2("확인 방법"),
    ]
    for check in draft.checks or ["스레드 출처와 운영 로그를 함께 확인"]:
        children.append(_bulleted_item(check))

    children.extend(
        [
            _heading_2("답변 템플릿"),
            _quote(draft.answer_template),
            _heading_2("관련 키워드"),
            _paragraph(", ".join(_unique_keywords(draft)) or "없음"),
            _heading_2("근거 요약"),
        ]
    )
    for note in draft.source_notes or [draft.cause]:
        children.append(_bulleted_item(note))

    children.append(_heading_2("출처"))
    if source_key:
        children.append(_bulleted_item(f"Slack source key: {source_key}"))
    if thread_permalink:
        children.append(_bulleted_item(f"Slack thread: {thread_permalink}"))
    if learned_by_user_id:
        children.append(_bulleted_item(f"학습 요청자: {learned_by_user_id}"))
    return children[:100]


def _create_notion_playbook_page(
    draft: ThreadPlaybookDraft,
    *,
    root_page_id: str,
    source_key: str | None,
    thread_permalink: str | None,
    learned_by_user_id: str | None,
) -> dict[str, Any]:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    notion_token = _require_company_notion_token()
    payload = {
        "parent": {"page_id": normalized_root_page_id},
        "properties": {
            "title": [
                {
                    "type": "text",
                    "text": {"content": draft.title[:2000]},
                }
            ]
        },
        "children": _build_playbook_page_children(
            draft,
            source_key=source_key,
            thread_permalink=thread_permalink,
            learned_by_user_id=learned_by_user_id,
        ),
    }
    return _notion_request("/pages", method="POST", payload=payload, token=notion_token)


def _build_rag_index_line(draft: ThreadPlaybookDraft, *, page_id: str) -> str:
    # line protocol 구분자가 LLM 제목이나 env 값에 들어와도 parser 구조가 깨지지 않게 정리한다.
    def clean_field(value: str, fallback: str) -> str:
        cleaned = re.sub(r"[|\r\n]+", " ", str(value or "")).strip()
        return re.sub(r"\s+", " ", cleaned) or fallback

    section = clean_field(cs.THREAD_PLAYBOOK_NOTION_SECTION, "마미박스 장애 대응")
    kind = clean_field(cs.THREAD_PLAYBOOK_NOTION_KIND, "runbook")
    priority = clean_field(cs.THREAD_PLAYBOOK_NOTION_PRIORITY, "high")
    title = clean_field(draft.title, "Slack 스레드 학습 사례")
    keywords = ", ".join(
        clean_field(keyword, "")
        for keyword in _unique_keywords(draft)
        if clean_field(keyword, "")
    )
    return (
        f"page_id={_normalize_notion_id(page_id)} | "
        f"section={section} | "
        f"kind={kind} | "
        f"priority={priority} | "
        f"title={title} | "
        f"keywords={keywords}"
    )


def _extract_keywords_from_existing_playbook(page_content: dict[str, Any]) -> list[str]:
    raw_lines = page_content.get("lines") or []
    if not isinstance(raw_lines, list):
        return []

    lines = [str(line or "").lstrip("- ").strip() for line in raw_lines]
    for index, line in enumerate(lines):
        if line != "관련 키워드":
            continue
        for candidate in lines[index + 1 : index + 4]:
            if not candidate or candidate in {"근거 요약", "출처"}:
                continue
            return _clean_list(candidate, max_items=16, max_chars=40)
    return []


def _parse_thread_source_index_line(text: str) -> tuple[str, str] | None:
    matched = _THREAD_SOURCE_INDEX_LINE_PATTERN.fullmatch(str(text or "").strip())
    if not matched:
        return None
    return matched.group("source_key"), _normalize_notion_id(matched.group("page_id"))


def _build_thread_source_index_line(source_key: str, *, page_id: str) -> str:
    if not _THREAD_SOURCE_KEY_PATTERN.fullmatch(str(source_key or "").strip()):
        raise ValueError("Slack thread source key 형식이 올바르지 않아")
    return f"source_key={source_key} | page_id={_normalize_notion_id(page_id)}"


def _build_thread_source_pending_line(
    source_key: str,
    *,
    owner: str | None = None,
    updated_at: int | None = None,
) -> str:
    if not source_key.startswith("slack-thread:v1:") or not _THREAD_SOURCE_KEY_PATTERN.fullmatch(source_key):
        raise ValueError("Slack thread source key 형식이 올바르지 않아")
    resolved_owner = str(owner or _thread_source_owner()).strip()
    if not re.fullmatch(r"[0-9a-f]{12}", resolved_owner):
        raise ValueError("Slack thread source owner 형식이 올바르지 않아")
    timestamp = int(time.time()) if updated_at is None else int(updated_at)
    return f"source_key={source_key} | status=pending | owner={resolved_owner} | updated_at={timestamp}"


def _inspect_thread_source_index_state(
    blocks: list[dict[str, Any]],
    *,
    source_key: str,
) -> ThreadSourceIndexState:
    in_index = False
    found_index_heading = False
    insert_after_block_id: str | None = None
    matched_page_ids: set[str] = set()
    pending_records: list[ThreadSourcePendingReservation] = []
    migration_complete = False

    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_type = str(block.get("type") or "")
        text = _extract_block_text(block)
        if block_type in {"heading_1", "heading_2", "heading_3"}:
            if block_type == "heading_2" and text == _THREAD_SOURCE_INDEX_HEADING:
                in_index = True
                found_index_heading = True
                insert_after_block_id = str(block.get("id") or "").strip() or insert_after_block_id
                continue
            if in_index:
                break
        if not in_index or block_type != "bulleted_list_item":
            continue

        insert_after_block_id = str(block.get("id") or "").strip() or insert_after_block_id
        if text == _THREAD_SOURCE_MIGRATION_MARKER:
            migration_complete = True
            continue
        parsed = _parse_thread_source_index_line(text)
        if parsed is not None and parsed[0] == source_key:
            matched_page_ids.add(parsed[1])
            continue
        pending_match = _THREAD_SOURCE_PENDING_LINE_PATTERN.fullmatch(str(text or "").strip())
        if pending_match is not None and pending_match.group("source_key") == source_key:
            pending_records.append(
                ThreadSourcePendingReservation(
                    block_id=_normalize_notion_id(str(block.get("id") or "")),
                    owner=pending_match.group("owner"),
                    updated_at=int(pending_match.group("updated_at")),
                    created_time=str(block.get("created_time") or ""),
                )
            )

    if len(matched_page_ids) > 1:
        raise RuntimeError("같은 Slack thread source key가 여러 Notion 페이지를 가리켜")
    # 교차 process append가 겹쳐도 모든 worker가 같은 block을 승자로 고르게 한다.
    # Notion created_time이 없는 테스트/레거시 payload는 block id로 안정적으로 정렬한다.
    pending_records.sort(
        key=lambda record: (
            not bool(record.created_time),
            record.created_time,
            record.block_id,
        )
    )
    page_id = next(iter(matched_page_ids), None)
    winner = pending_records[0] if pending_records else None
    return ThreadSourceIndexState(
        page_id=page_id,
        pending_block_id=winner.block_id if winner else None,
        pending_reservations=tuple(pending_records),
        pending_owner=winner.owner if winner else "",
        pending_updated_at=winner.updated_at if winner else 0,
        insert_after_block_id=insert_after_block_id,
        found_index_heading=found_index_heading,
        migration_complete=migration_complete,
    )


def _inspect_thread_source_index(
    blocks: list[dict[str, Any]],
    *,
    source_key: str,
) -> tuple[str | None, str | None, bool]:
    state = _inspect_thread_source_index_state(blocks, source_key=source_key)
    return state.page_id, state.insert_after_block_id, state.found_index_heading


def _load_company_root_blocks(root_page_id: str) -> list[dict[str, Any]]:
    # source/RAG 인덱스는 200개를 넘을 수 있어서 회사 root만 제한 없이 순회한다.
    return _fetch_all_notion_blocks(
        _normalize_notion_id(root_page_id),
        token=_require_company_notion_token(),
        max_blocks=0,
    )


def _collect_ready_thread_source_mappings(blocks: list[dict[str, Any]]) -> dict[str, str]:
    in_index = False
    mappings: dict[str, str] = {}
    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_type = str(block.get("type") or "")
        text = _extract_block_text(block)
        if block_type in {"heading_1", "heading_2", "heading_3"}:
            if block_type == "heading_2" and text == _THREAD_SOURCE_INDEX_HEADING:
                in_index = True
                continue
            if in_index:
                break
        if not in_index or block_type != "bulleted_list_item":
            continue
        parsed = _parse_thread_source_index_line(text)
        if parsed is None:
            continue
        source_key, page_id = parsed
        existing_page_id = mappings.get(source_key)
        if existing_page_id and existing_page_id != page_id:
            raise RuntimeError("같은 Slack source key가 여러 Notion 페이지를 가리켜")
        mappings[source_key] = page_id
    return mappings


def _extract_page_source_keys(page_content: dict[str, Any]) -> set[str]:
    source_keys: set[str] = set()
    raw_lines = page_content.get("lines") or []
    if not isinstance(raw_lines, list):
        return source_keys

    # LLM이 만든 근거 문구를 source marker로 오인하지 않도록 마지막 출처 섹션의 bullet만 읽는다.
    source_heading_indexes = [
        index
        for index, raw_line in enumerate(raw_lines)
        if str(raw_line or "").strip() == "출처"
    ]
    if not source_heading_indexes:
        return source_keys

    for raw_line in raw_lines[source_heading_indexes[-1] + 1 :]:
        line = str(raw_line or "").strip()
        if not line.startswith("- "):
            break
        line = line[2:].strip()
        if line.startswith("Slack source key: "):
            source_key = line.removeprefix("Slack source key: ").strip()
            if _THREAD_SOURCE_KEY_PATTERN.fullmatch(source_key):
                source_keys.add(source_key)
            continue
        if line.startswith("Slack thread: "):
            permalink_key = _build_slack_permalink_source_key(
                line.removeprefix("Slack thread: ").strip()
            )
            if permalink_key:
                source_keys.add(permalink_key)
    return source_keys


def _append_thread_source_index_lines(
    *,
    root_page_id: str,
    lines: list[str],
    initial_blocks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    notion_token = _require_company_notion_token()
    remaining = list(lines)
    blocks = initial_blocks
    sentinel_key = f"slack-thread:v1:{'0' * 64}"

    while remaining:
        state = _inspect_thread_source_index_state(blocks, source_key=sentinel_key)
        batch_size = 99 if not state.found_index_heading else 100
        batch = remaining[:batch_size]
        remaining = remaining[batch_size:]
        children = [_bulleted_item(line) for line in batch]
        payload: dict[str, Any] = {"children": children}
        if state.insert_after_block_id:
            payload["after"] = _normalize_notion_id(state.insert_after_block_id)
        if not state.found_index_heading:
            payload.pop("after", None)
            payload["children"] = [_heading_2(_THREAD_SOURCE_INDEX_HEADING), *children]

        _notion_request(
            f"/blocks/{normalized_root_page_id}/children",
            method="PATCH",
            payload=payload,
            token=notion_token,
        )
        _invalidate_notion_playbook_cache(normalized_root_page_id, token=notion_token)
        if remaining:
            blocks = _load_company_root_blocks(normalized_root_page_id)

    return _load_company_root_blocks(normalized_root_page_id)


def _ensure_legacy_thread_sources_migrated(root_page_id: str) -> list[dict[str, Any]]:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    with _thread_root_index_lock(normalized_root_page_id):
        root_blocks = _load_company_root_blocks(normalized_root_page_id)
        sentinel_key = f"slack-thread:v1:{'0' * 64}"
        state = _inspect_thread_source_index_state(root_blocks, source_key=sentinel_key)
        if state.migration_complete:
            return root_blocks

        existing_mappings = _collect_ready_thread_source_mappings(root_blocks)
        discovered_mappings: dict[str, str] = {}
        notion_token = _require_company_notion_token()
        # 기존 학습 페이지의 정확한 출처 줄을 한 번만 읽어 permalink/source key index를 만든다.
        for block in root_blocks:
            if not isinstance(block, dict) or block.get("type") != "child_page":
                continue
            page_id = _normalize_notion_id(str(block.get("id") or ""))
            page_content = _load_notion_page_content_cached(page_id, token=notion_token)
            for source_key in _extract_page_source_keys(page_content):
                existing_page_id = discovered_mappings.get(source_key) or existing_mappings.get(source_key)
                if existing_page_id and existing_page_id != page_id:
                    raise RuntimeError("기존 Slack source가 여러 Notion 페이지에 중복 저장돼 있어")
                discovered_mappings[source_key] = page_id

        migration_lines = [
            _build_thread_source_index_line(source_key, page_id=page_id)
            for source_key, page_id in sorted(discovered_mappings.items())
            if source_key not in existing_mappings
        ]
        # marker를 마지막에 써서 중간 실패 시 다음 요청이 빠진 mapping만 다시 보완하게 한다.
        migration_lines.append(_THREAD_SOURCE_MIGRATION_MARKER)
        return _append_thread_source_index_lines(
            root_page_id=normalized_root_page_id,
            lines=migration_lines,
            initial_blocks=root_blocks,
        )


def _find_thread_source_page_id(*, root_page_id: str, source_key: str) -> str | None:
    blocks = _load_company_root_blocks(root_page_id)
    page_id, _, _ = _inspect_thread_source_index(blocks, source_key=source_key)
    return page_id


def _append_thread_source_index_entry(
    *,
    root_page_id: str,
    source_key: str,
    page_id: str,
) -> bool:
    notion_token = _require_company_notion_token()
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    normalized_page_id = _normalize_notion_id(page_id)
    with _thread_root_index_lock(normalized_root_page_id):
        root_blocks = _load_company_root_blocks(normalized_root_page_id)
        state = _inspect_thread_source_index_state(root_blocks, source_key=source_key)
        if state.page_id:
            if state.page_id != normalized_page_id:
                raise RuntimeError("같은 Slack thread source key가 다른 Notion 페이지에 이미 연결돼 있어")
            return False

        children = [_bulleted_item(_build_thread_source_index_line(source_key, page_id=normalized_page_id))]
        payload: dict[str, Any] = {"children": children}
        if state.insert_after_block_id:
            payload["after"] = _normalize_notion_id(state.insert_after_block_id)
        if not state.found_index_heading:
            payload.pop("after", None)
            payload["children"] = [_heading_2(_THREAD_SOURCE_INDEX_HEADING), *children]

        _notion_request(
            f"/blocks/{normalized_root_page_id}/children",
            method="PATCH",
            payload=payload,
            token=notion_token,
        )
        _invalidate_notion_playbook_cache(normalized_root_page_id, token=notion_token)
        return True


def _update_thread_source_reservation(
    *,
    root_page_id: str,
    source_key: str,
    reservation_block_id: str,
    expected_owner: str,
    expected_updated_at: int,
    line: str,
) -> None:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    normalized_block_id = _normalize_notion_id(reservation_block_id)
    notion_token = _require_company_notion_token()
    with _thread_root_index_lock(normalized_root_page_id):
        # 오래 걸린 LLM 호출 사이 다른 worker가 takeover했으면 그 예약을 덮어쓰지 않는다.
        current_block = _notion_request(f"/blocks/{normalized_block_id}", token=notion_token)
        pending_match = _THREAD_SOURCE_PENDING_LINE_PATTERN.fullmatch(
            _extract_block_text(current_block).strip()
        )
        if pending_match is None or pending_match.group("source_key") != source_key:
            raise RuntimeError("Slack thread source 예약이 더 이상 유효하지 않아")
        if pending_match.group("owner") != expected_owner:
            raise RuntimeError("다른 프로세스가 Slack thread source 예약을 인계받았어")
        if int(pending_match.group("updated_at")) != expected_updated_at:
            raise RuntimeError("Slack thread source 예약이 이미 갱신됐어")
        _notion_request(
            f"/blocks/{normalized_block_id}",
            method="PATCH",
            payload={"bulleted_list_item": {"rich_text": _build_rich_text(line)}},
            token=notion_token,
        )
        _invalidate_notion_playbook_cache(normalized_root_page_id, token=notion_token)


def _reserve_thread_source_index_entry(*, root_page_id: str, source_key: str) -> str:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    notion_token = _require_company_notion_token()
    current_owner = _thread_source_owner()
    pending_line = _build_thread_source_pending_line(source_key, owner=current_owner)
    with _thread_root_index_lock(normalized_root_page_id):
        root_blocks = _load_company_root_blocks(normalized_root_page_id)
        state = _inspect_thread_source_index_state(root_blocks, source_key=source_key)
        if state.page_id:
            _delete_thread_source_pending_blocks_best_effort(
                root_page_id=normalized_root_page_id,
                source_key=source_key,
                reservations=state.pending_reservations,
            )
            raise RuntimeError("Slack thread source가 이미 확정돼 있어")
        if state.pending_block_id:
            now = int(time.time())
            stale_losers = tuple(
                reservation
                for reservation in state.pending_reservations[1:]
                if max(0, now - reservation.updated_at) >= _THREAD_SOURCE_PENDING_TTL_SEC
            )
            _delete_thread_source_pending_blocks_best_effort(
                root_page_id=normalized_root_page_id,
                source_key=source_key,
                reservations=stale_losers,
            )
            reservation_age = max(0, now - state.pending_updated_at)
            if state.pending_owner != current_owner and reservation_age < _THREAD_SOURCE_PENDING_TTL_SEC:
                raise RuntimeError("다른 프로세스에서 이 Slack thread를 학습 중이야")
            _update_thread_source_reservation(
                root_page_id=normalized_root_page_id,
                source_key=source_key,
                reservation_block_id=state.pending_block_id,
                expected_owner=state.pending_owner,
                expected_updated_at=state.pending_updated_at,
                line=pending_line,
            )
            return state.pending_block_id

        children = [_bulleted_item(pending_line)]
        payload: dict[str, Any] = {"children": children}
        if state.insert_after_block_id:
            payload["after"] = _normalize_notion_id(state.insert_after_block_id)
        if not state.found_index_heading:
            payload.pop("after", None)
            payload["children"] = [_heading_2(_THREAD_SOURCE_INDEX_HEADING), *children]

        response = _notion_request(
            f"/blocks/{normalized_root_page_id}/children",
            method="PATCH",
            payload=payload,
            token=notion_token,
        )
        _invalidate_notion_playbook_cache(normalized_root_page_id, token=notion_token)
        created_block_id = ""
        for block in response.get("results") or []:
            if not isinstance(block, dict) or block.get("type") != "bulleted_list_item":
                continue
            if _extract_block_text(block) == pending_line:
                created_block_id = _normalize_notion_id(str(block.get("id") or ""))
                break

        # 다른 프로세스의 동시 reservation도 가능한 한 페이지 생성 전에 감지하도록 다시 읽는다.
        refreshed_state = _inspect_thread_source_index_state(
            _load_company_root_blocks(normalized_root_page_id),
            source_key=source_key,
        )
        if refreshed_state.page_id:
            _delete_thread_source_pending_blocks_best_effort(
                root_page_id=normalized_root_page_id,
                source_key=source_key,
                reservations=refreshed_state.pending_reservations,
            )
            raise RuntimeError("다른 프로세스에서 Slack thread source를 먼저 확정했어")
        if (
            refreshed_state.pending_block_id
            and refreshed_state.pending_owner == current_owner
            and (not created_block_id or refreshed_state.pending_block_id == created_block_id)
        ):
            now = int(time.time())
            stale_losers = tuple(
                reservation
                for reservation in refreshed_state.pending_reservations[1:]
                if max(0, now - reservation.updated_at) >= _THREAD_SOURCE_PENDING_TTL_SEC
            )
            _delete_thread_source_pending_blocks_best_effort(
                root_page_id=normalized_root_page_id,
                source_key=source_key,
                reservations=stale_losers,
            )
            return refreshed_state.pending_block_id
        created_reservation = next(
            (
                reservation
                for reservation in refreshed_state.pending_reservations
                if reservation.block_id == created_block_id
            ),
            None,
        )
        if created_reservation is not None:
            _delete_thread_source_pending_blocks_best_effort(
                root_page_id=normalized_root_page_id,
                source_key=source_key,
                reservations=(created_reservation,),
            )
        raise RuntimeError("Slack thread source 예약 block을 확인하지 못했어")


def _finalize_thread_source_reservation(
    *,
    root_page_id: str,
    source_key: str,
    page_id: str,
    reservation_block_id: str,
    expected_owner: str,
) -> None:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    normalized_block_id = _normalize_notion_id(reservation_block_id)
    with _thread_root_index_lock(normalized_root_page_id):
        state = _inspect_thread_source_index_state(
            _load_company_root_blocks(normalized_root_page_id),
            source_key=source_key,
        )
        if state.page_id:
            if state.page_id != _normalize_notion_id(page_id):
                raise RuntimeError("Slack thread source가 다른 Notion 페이지에 이미 연결돼 있어")
            return
        if state.pending_block_id != normalized_block_id:
            raise RuntimeError("Slack thread source 예약의 우선권을 잃었어")
        if state.pending_owner != expected_owner:
            raise RuntimeError("다른 프로세스가 Slack thread source 예약을 인계받았어")
        _update_thread_source_reservation(
            root_page_id=normalized_root_page_id,
            source_key=source_key,
            reservation_block_id=normalized_block_id,
            expected_owner=expected_owner,
            expected_updated_at=state.pending_updated_at,
            line=_build_thread_source_index_line(source_key, page_id=page_id),
        )


def _refresh_thread_source_reservation(
    *,
    root_page_id: str,
    source_key: str,
    reservation_block_id: str,
    expected_owner: str,
) -> None:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    normalized_block_id = _normalize_notion_id(reservation_block_id)
    with _thread_root_index_lock(normalized_root_page_id):
        state = _inspect_thread_source_index_state(
            _load_company_root_blocks(normalized_root_page_id),
            source_key=source_key,
        )
        if state.page_id:
            raise RuntimeError("Slack thread source가 이미 확정돼 있어")
        if state.pending_block_id != normalized_block_id:
            raise RuntimeError("Slack thread source 예약의 우선권을 잃었어")
        if state.pending_owner != expected_owner:
            raise RuntimeError("다른 프로세스가 Slack thread source 예약을 인계받았어")
        _update_thread_source_reservation(
            root_page_id=normalized_root_page_id,
            source_key=source_key,
            reservation_block_id=normalized_block_id,
            expected_owner=expected_owner,
            expected_updated_at=state.pending_updated_at,
            line=_build_thread_source_pending_line(source_key, owner=_thread_source_owner()),
        )


def _delete_thread_source_reservation_best_effort(
    *,
    root_page_id: str,
    source_key: str,
    reservation_block_id: str,
    expected_owner: str,
    expected_updated_at: int | None = None,
) -> bool:
    notion_token = _require_company_notion_token()
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    normalized_block_id = _normalize_notion_id(reservation_block_id)
    deleted = False
    try:
        with _thread_root_index_lock(normalized_root_page_id):
            current_block = _notion_request(f"/blocks/{normalized_block_id}", token=notion_token)
            pending_match = _THREAD_SOURCE_PENDING_LINE_PATTERN.fullmatch(
                _extract_block_text(current_block).strip()
            )
            if pending_match is None or pending_match.group("source_key") != source_key:
                return False
            if pending_match.group("owner") != expected_owner:
                return False
            if (
                expected_updated_at is not None
                and int(pending_match.group("updated_at")) != expected_updated_at
            ):
                return False
            _notion_request(
                f"/blocks/{normalized_block_id}",
                method="DELETE",
                token=notion_token,
            )
            deleted = True
    except Exception:
        logger.warning("Failed to delete Slack thread source reservation", exc_info=True)
    finally:
        if deleted:
            _invalidate_notion_playbook_cache(normalized_root_page_id, token=notion_token)
    return deleted


def _delete_thread_source_pending_blocks_best_effort(
    *,
    root_page_id: str,
    source_key: str,
    reservations: tuple[ThreadSourcePendingReservation, ...],
) -> None:
    # 동시 append로 생긴 loser와 ready mapping 뒤에 남은 pending만 정확한 source 확인 후 정리한다.
    for reservation in reservations:
        _delete_thread_source_reservation_best_effort(
            root_page_id=root_page_id,
            source_key=source_key,
            reservation_block_id=reservation.block_id,
            expected_owner=reservation.owner,
            expected_updated_at=reservation.updated_at,
        )


def _find_page_with_source_key(
    *,
    root_blocks: list[dict[str, Any]],
    source_key: str,
) -> str | None:
    notion_token = _require_company_notion_token()
    matched_page_ids: set[str] = set()
    for block in root_blocks:
        if not isinstance(block, dict) or block.get("type") != "child_page":
            continue
        page_id = _normalize_notion_id(str(block.get("id") or ""))
        page_content = _load_notion_page_content_cached(page_id, token=notion_token)
        if source_key in _extract_page_source_keys(page_content):
            matched_page_ids.add(page_id)

    if len(matched_page_ids) > 1:
        raise RuntimeError("같은 Slack source key를 가진 Notion 페이지가 여러 개 존재해")
    return next(iter(matched_page_ids), None)


def _inspect_rag_index(
    blocks: list[dict[str, Any]],
    *,
    page_id: str,
) -> tuple[str | None, bool, bool]:
    normalized_page_id = _normalize_notion_id(page_id)
    in_index = False
    found_index_heading = False
    insert_after_block_id: str | None = None
    contains_page = False

    for block in blocks:
        if not isinstance(block, dict):
            continue
        block_type = str(block.get("type") or "")
        text = _extract_block_text(block)
        if block_type == "heading_2" and text == _RAG_INDEX_HEADING:
            in_index = True
            found_index_heading = True
            insert_after_block_id = str(block.get("id") or "").strip() or insert_after_block_id
            continue
        if not in_index:
            continue
        if block_type in {"heading_1", "heading_2", "heading_3"}:
            break
        if block_type == "bulleted_list_item":
            insert_after_block_id = str(block.get("id") or "").strip() or insert_after_block_id
            parsed = _parse_notion_rag_index_line(text)
            if parsed is not None and parsed.get("pageId") == normalized_page_id:
                contains_page = True

    return insert_after_block_id, found_index_heading, contains_page


def _append_notion_rag_index_entry(
    draft: ThreadPlaybookDraft,
    *,
    root_page_id: str,
    page_id: str,
) -> bool:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    notion_token = _require_company_notion_token()
    index_line = _build_rag_index_line(draft, page_id=page_id)
    with _thread_root_index_lock(normalized_root_page_id):
        insert_after_block_id, found_index_heading, contains_page = _inspect_rag_index(
            _load_company_root_blocks(normalized_root_page_id),
            page_id=page_id,
        )
        if contains_page:
            return False

        children = [_bulleted_item(index_line)]
        payload: dict[str, Any] = {"children": children}
        if insert_after_block_id:
            payload["after"] = _normalize_notion_id(insert_after_block_id)

        if not found_index_heading:
            payload.pop("after", None)
            payload["children"] = [
                _heading_2(_RAG_INDEX_HEADING),
                *children,
            ]

        _notion_request(
            f"/blocks/{normalized_root_page_id}/children",
            method="PATCH",
            payload=payload,
            token=notion_token,
        )
        _invalidate_notion_playbook_cache(normalized_root_page_id, token=notion_token)
        return True


def _build_existing_thread_playbook_result(
    *,
    root_page_id: str,
    page_id: str,
) -> ThreadPlaybookSaveResult:
    notion_token = _require_company_notion_token()
    normalized_page_id = _normalize_notion_id(page_id)
    page_content = _load_notion_page_content_cached(normalized_page_id, token=notion_token)
    title = str(page_content.get("title") or "").strip() or "Slack 스레드 학습 사례"
    keywords = _extract_keywords_from_existing_playbook(page_content)
    # 페이지 생성 뒤 RAG index만 실패한 경우 재시도에서 기존 페이지를 재사용하고 index만 복구한다.
    recovery_draft = ThreadPlaybookDraft(
        title=title,
        symptom="",
        cause="",
        answer_template="",
        checks=[],
        keywords=keywords,
        source_notes=[],
    )
    rag_index_updated = _append_notion_rag_index_entry(
        recovery_draft,
        root_page_id=root_page_id,
        page_id=normalized_page_id,
    )
    return ThreadPlaybookSaveResult(
        title=title,
        page_id=normalized_page_id,
        url=str(page_content.get("url") or "").strip(),
        keywords=keywords,
        rag_index_updated=rag_index_updated,
        created=False,
    )


def _save_thread_playbook_to_notion(
    draft: ThreadPlaybookDraft,
    *,
    root_page_id: str,
    source_key: str | None = None,
    source_reservation_block_id: str | None = None,
    source_reservation_owner: str | None = None,
    thread_permalink: str | None,
    learned_by_user_id: str | None,
) -> ThreadPlaybookSaveResult:
    if not _is_company_notion_configured(root_page_id):
        raise RuntimeError("Notion 설정이 없어 저장할 수 없어")
    if not root_page_id:
        raise RuntimeError("THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID가 필요해")
    if source_reservation_block_id and (not source_key or not source_reservation_owner):
        # 예약 metadata 오류는 page를 만들기 전에 차단해 orphan page를 남기지 않는다.
        raise RuntimeError("Slack thread source 예약 key와 owner가 필요해")

    page_payload = _create_notion_playbook_page(
        draft,
        root_page_id=root_page_id,
        source_key=source_key,
        thread_permalink=thread_permalink,
        learned_by_user_id=learned_by_user_id,
    )
    page_id = _normalize_notion_id(str(page_payload.get("id") or ""))
    notion_token = _require_company_notion_token()
    # child page 생성 직후 root snapshot을 지워 이후 index 조회가 새 페이지를 보게 한다.
    _invalidate_notion_playbook_cache(root_page_id, token=notion_token)
    if source_key:
        if source_reservation_block_id:
            _finalize_thread_source_reservation(
                root_page_id=root_page_id,
                source_key=source_key,
                page_id=page_id,
                reservation_block_id=source_reservation_block_id,
                expected_owner=source_reservation_owner,
            )
        else:
            _append_thread_source_index_entry(
                root_page_id=root_page_id,
                source_key=source_key,
                page_id=page_id,
            )
    rag_index_updated = _append_notion_rag_index_entry(
        draft,
        root_page_id=root_page_id,
        page_id=page_id,
    )
    return ThreadPlaybookSaveResult(
        title=draft.title,
        page_id=page_id,
        url=str(page_payload.get("url") or "").strip(),
        keywords=_unique_keywords(draft),
        rag_index_updated=rag_index_updated,
    )


def _learn_slack_thread_playbook(
    thread_context: str,
    *,
    workspace_id: str,
    channel_id: str,
    thread_ts: str,
    thread_permalink: str | None = None,
    learned_by_user_id: str | None = None,
    claude_client: Any = None,
    root_page_id: str | None = None,
) -> ThreadPlaybookSaveResult:
    target_root_page_id = _resolve_notion_root_page_id(root_page_id)
    if not _is_company_notion_configured(target_root_page_id):
        raise RuntimeError("Notion 설정이 없어 저장할 수 없어")
    if not target_root_page_id:
        raise RuntimeError("THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID가 필요해")

    source_key = _build_slack_thread_source_key(workspace_id, channel_id, thread_ts)
    current_owner = _thread_source_owner()
    with _thread_source_lock(source_key):
        root_blocks = _ensure_legacy_thread_sources_migrated(target_root_page_id)
        source_state = _inspect_thread_source_index_state(root_blocks, source_key=source_key)
        if source_state.page_id:
            _delete_thread_source_pending_blocks_best_effort(
                root_page_id=target_root_page_id,
                source_key=source_key,
                reservations=source_state.pending_reservations,
            )
            return _build_existing_thread_playbook_result(
                root_page_id=target_root_page_id,
                page_id=source_state.page_id,
            )

        # 기존 permalink-only 페이지는 migration alias로 찾은 뒤 안정적인 event key도 함께 backfill한다.
        permalink_source_key = _build_slack_permalink_source_key(thread_permalink)
        if permalink_source_key:
            permalink_state = _inspect_thread_source_index_state(
                root_blocks,
                source_key=permalink_source_key,
            )
            if permalink_state.page_id:
                if source_state.pending_block_id:
                    _finalize_thread_source_reservation(
                        root_page_id=target_root_page_id,
                        source_key=source_key,
                        page_id=permalink_state.page_id,
                        reservation_block_id=source_state.pending_block_id,
                        expected_owner=source_state.pending_owner,
                    )
                    _delete_thread_source_pending_blocks_best_effort(
                        root_page_id=target_root_page_id,
                        source_key=source_key,
                        reservations=source_state.pending_reservations,
                    )
                else:
                    _append_thread_source_index_entry(
                        root_page_id=target_root_page_id,
                        source_key=source_key,
                        page_id=permalink_state.page_id,
                    )
                return _build_existing_thread_playbook_result(
                    root_page_id=target_root_page_id,
                    page_id=permalink_state.page_id,
                )

        reservation_block_id = source_state.pending_block_id
        reservation_owner = source_state.pending_owner
        recovering_existing_reservation = bool(reservation_block_id)
        if reservation_block_id:
            now = int(time.time())
            stale_losers = tuple(
                reservation
                for reservation in source_state.pending_reservations[1:]
                if max(0, now - reservation.updated_at) >= _THREAD_SOURCE_PENDING_TTL_SEC
            )
            _delete_thread_source_pending_blocks_best_effort(
                root_page_id=target_root_page_id,
                source_key=source_key,
                reservations=stale_losers,
            )
            recovered_page_id = _find_page_with_source_key(
                root_blocks=root_blocks,
                source_key=source_key,
            )
            if recovered_page_id:
                _finalize_thread_source_reservation(
                    root_page_id=target_root_page_id,
                    source_key=source_key,
                    page_id=recovered_page_id,
                    reservation_block_id=reservation_block_id,
                    expected_owner=source_state.pending_owner,
                )
                _delete_thread_source_pending_blocks_best_effort(
                    root_page_id=target_root_page_id,
                    source_key=source_key,
                    reservations=source_state.pending_reservations,
                )
                return _build_existing_thread_playbook_result(
                    root_page_id=target_root_page_id,
                    page_id=recovered_page_id,
                )

            reservation_age = max(0, now - source_state.pending_updated_at)
            if reservation_age < _THREAD_SOURCE_RETRY_GRACE_SEC:
                raise RuntimeError("이 Slack thread의 이전 학습 결과를 확인 중이야. 잠시 후 다시 시도해줘")
            if (
                source_state.pending_owner != current_owner
                and reservation_age < _THREAD_SOURCE_PENDING_TTL_SEC
            ):
                raise RuntimeError("다른 프로세스에서 이 Slack thread를 학습 중이야")
            _refresh_thread_source_reservation(
                root_page_id=target_root_page_id,
                source_key=source_key,
                reservation_block_id=reservation_block_id,
                expected_owner=source_state.pending_owner,
            )
            reservation_owner = current_owner
        else:
            reservation_block_id = _reserve_thread_source_index_entry(
                root_page_id=target_root_page_id,
                source_key=source_key,
            )
            reservation_owner = current_owner

        try:
            draft = _generate_thread_playbook_draft(
                thread_context,
                thread_permalink=thread_permalink,
                claude_client=claude_client,
            )

            # page POST 직전에 ready/orphan을 다시 확인하고 현재 예약이 canonical winner인지 갱신한다.
            latest_root_blocks = _load_company_root_blocks(target_root_page_id)
            latest_state = _inspect_thread_source_index_state(
                latest_root_blocks,
                source_key=source_key,
            )
            if latest_state.page_id:
                _delete_thread_source_pending_blocks_best_effort(
                    root_page_id=target_root_page_id,
                    source_key=source_key,
                    reservations=latest_state.pending_reservations,
                )
                return _build_existing_thread_playbook_result(
                    root_page_id=target_root_page_id,
                    page_id=latest_state.page_id,
                )
            # 전체 child page scan은 비싸므로 기존 예약 복구나 교차 process 충돌 때만 수행한다.
            should_scan_for_orphan = (
                recovering_existing_reservation
                or len(latest_state.pending_reservations) > 1
            )
            recovered_page_id = (
                _find_page_with_source_key(
                    root_blocks=latest_root_blocks,
                    source_key=source_key,
                )
                if should_scan_for_orphan
                else None
            )
            if recovered_page_id and latest_state.pending_block_id:
                _finalize_thread_source_reservation(
                    root_page_id=target_root_page_id,
                    source_key=source_key,
                    page_id=recovered_page_id,
                    reservation_block_id=latest_state.pending_block_id,
                    expected_owner=latest_state.pending_owner,
                )
                _delete_thread_source_pending_blocks_best_effort(
                    root_page_id=target_root_page_id,
                    source_key=source_key,
                    reservations=latest_state.pending_reservations,
                )
                return _build_existing_thread_playbook_result(
                    root_page_id=target_root_page_id,
                    page_id=recovered_page_id,
                )
            _refresh_thread_source_reservation(
                root_page_id=target_root_page_id,
                source_key=source_key,
                reservation_block_id=reservation_block_id,
                expected_owner=reservation_owner,
            )
        except Exception:
            # 페이지 생성 전 실패는 reservation을 지워 즉시 재시도할 수 있게 한다.
            _delete_thread_source_reservation_best_effort(
                root_page_id=target_root_page_id,
                source_key=source_key,
                reservation_block_id=reservation_block_id,
                expected_owner=reservation_owner,
            )
            raise

        return _save_thread_playbook_to_notion(
            draft,
            root_page_id=target_root_page_id,
            source_key=source_key,
            source_reservation_block_id=reservation_block_id,
            source_reservation_owner=reservation_owner,
            thread_permalink=thread_permalink,
            learned_by_user_id=learned_by_user_id,
        )
