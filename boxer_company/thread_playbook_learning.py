import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat
from boxer.retrieval.connectors.notion import (
    _extract_block_text,
    _fetch_all_notion_blocks,
    _is_notion_configured,
    _normalize_notion_id,
    _notion_request,
)
from boxer_company import settings as cs
from boxer_company.notion_playbooks import _RAG_INDEX_HEADING, _invalidate_notion_playbook_cache


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


_CODE_FENCE_RE = re.compile(r"^```(?:json)?\s*|\s*```$", re.IGNORECASE | re.MULTILINE)


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
    if thread_permalink:
        children.append(_bulleted_item(f"Slack thread: {thread_permalink}"))
    if learned_by_user_id:
        children.append(_bulleted_item(f"학습 요청자: {learned_by_user_id}"))
    return children[:100]


def _create_notion_playbook_page(
    draft: ThreadPlaybookDraft,
    *,
    root_page_id: str,
    thread_permalink: str | None,
    learned_by_user_id: str | None,
) -> dict[str, Any]:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
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
            thread_permalink=thread_permalink,
            learned_by_user_id=learned_by_user_id,
        ),
    }
    return _notion_request("/pages", method="POST", payload=payload)


def _build_rag_index_line(draft: ThreadPlaybookDraft, *, page_id: str) -> str:
    section = cs.THREAD_PLAYBOOK_NOTION_SECTION or "마미박스 장애 대응"
    kind = cs.THREAD_PLAYBOOK_NOTION_KIND or "runbook"
    priority = cs.THREAD_PLAYBOOK_NOTION_PRIORITY or "high"
    keywords = ", ".join(_unique_keywords(draft))
    return (
        f"page_id={_normalize_notion_id(page_id)} | "
        f"section={section} | "
        f"kind={kind} | "
        f"priority={priority} | "
        f"title={draft.title} | "
        f"keywords={keywords}"
    )


def _find_rag_index_insert_after_block_id(root_page_id: str) -> tuple[str | None, bool]:
    blocks = _fetch_all_notion_blocks(root_page_id)
    in_index = False
    found_index_heading = False
    insert_after_block_id: str | None = None

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
        if block_type == "heading_1":
            break
        if block_type == "bulleted_list_item":
            insert_after_block_id = str(block.get("id") or "").strip() or insert_after_block_id

    return insert_after_block_id, found_index_heading


def _append_notion_rag_index_entry(
    draft: ThreadPlaybookDraft,
    *,
    root_page_id: str,
    page_id: str,
) -> bool:
    normalized_root_page_id = _normalize_notion_id(root_page_id)
    index_line = _build_rag_index_line(draft, page_id=page_id)
    insert_after_block_id, found_index_heading = _find_rag_index_insert_after_block_id(normalized_root_page_id)
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
    )
    _invalidate_notion_playbook_cache(normalized_root_page_id)
    return True


def _save_thread_playbook_to_notion(
    draft: ThreadPlaybookDraft,
    *,
    root_page_id: str,
    thread_permalink: str | None,
    learned_by_user_id: str | None,
) -> ThreadPlaybookSaveResult:
    if not _is_notion_configured():
        raise RuntimeError("Notion 설정이 없어 저장할 수 없어")
    if not root_page_id:
        raise RuntimeError("THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID 또는 NOTION_TEST_PAGE_ID가 필요해")

    page_payload = _create_notion_playbook_page(
        draft,
        root_page_id=root_page_id,
        thread_permalink=thread_permalink,
        learned_by_user_id=learned_by_user_id,
    )
    page_id = _normalize_notion_id(str(page_payload.get("id") or ""))
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
    thread_permalink: str | None = None,
    learned_by_user_id: str | None = None,
    claude_client: Any = None,
    root_page_id: str | None = None,
) -> ThreadPlaybookSaveResult:
    target_root_page_id = (root_page_id or cs.THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID or s.NOTION_TEST_PAGE_ID).strip()
    draft = _generate_thread_playbook_draft(
        thread_context,
        thread_permalink=thread_permalink,
        claude_client=claude_client,
    )
    return _save_thread_playbook_to_notion(
        draft,
        root_page_id=target_root_page_id,
        thread_permalink=thread_permalink,
        learned_by_user_id=learned_by_user_id,
    )
