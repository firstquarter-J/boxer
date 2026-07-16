from __future__ import annotations

import hashlib
import re
import time
from dataclasses import dataclass
from typing import Any

from boxer.retrieval.connectors.notion import (
    _extract_block_text,
    _fetch_notion_block_children,
    _normalize_notion_id,
    _notion_request,
)
from boxer_company import settings as cs

_PARENT_CACHE_TTL_SEC = 300
_PARENT_CACHE: dict[str, dict[str, Any]] = {}
_CONTENT_CACHE_TTL_SEC = 300
_CONTENT_CACHE: dict[str, dict[str, Any]] = {}
_SEARCH_INTENT_TOKENS = (
    "노션",
    "워크보드",
    "워크 보드",
    "work board",
    "회사 문서",
    "사내 문서",
)
_QUERY_NOISE_PATTERNS = (
    re.compile(r"work\s*board(?:에서|으로|로|의)?", re.IGNORECASE),
    re.compile(r"워크\s*보드(?:에서|으로|로|의)?"),
    re.compile(r"(?:회사|사내)\s*노션(?:에서|으로|로|의)?"),
    re.compile(r"노션(?:에서|으로|로|의)?"),
    re.compile(r"(?:회사|사내)\s*문서(?:에서|으로|로|의)?"),
)
_QUERY_REQUEST_WORDS = re.compile(
    r"(?:관련\s*)?(?:문서|페이지)(?:를|을)?|"
    r"찾아\s*줘|찾아줘|찾아|검색해\s*줘|검색해줘|검색|"
    r"조회해\s*줘|조회해줘|조회|보여\s*줘|보여줘|알려\s*줘|알려줘|"
    r"요약해\s*줘|요약해줘|정리해\s*줘|정리해줘|답변해\s*줘|답변해줘|"
    r"답해\s*줘|답해줘|설명해\s*줘|설명해줘|내용(?:을|은|이)?|"
    r"뭐야|무엇(?:인지|이야)?|어떻게(?:\s*해|\s*해야\s*해)?|왜|좀"
)
_LOOKUP_TOKEN_PATTERN = re.compile(r"[0-9A-Za-z가-힣_+-]+")
_COMPANY_NOTION_QUERY_ALIASES = (
    (("커머스",), "Commerce"),
    (("영업",), "Sales"),
    (("코어 엔지니어링", "코어엔지니어링"), "Core Engineering"),
)
_PARENT_ENDPOINTS = {
    "page_id": "/pages/{object_id}",
    "block_id": "/blocks/{object_id}",
    "database_id": "/databases/{object_id}",
    "data_source_id": "/data_sources/{object_id}",
}
_CONTENT_BOUNDARY_BLOCK_TYPES = {"child_page", "child_database", "link_to_page"}
_CONTENT_LOW_SIGNAL_TERMS = {
    "관련",
    "내용",
    "노션",
    "문서",
    "알려줘",
    "요약",
    "정리",
    "조회",
    "찾아줘",
    "회사",
}


@dataclass(frozen=True, slots=True)
class CompanyNotionSearchResult:
    object_id: str
    object_type: str
    title: str
    url: str
    last_edited_time: str


def _looks_like_company_notion_search(question: str) -> bool:
    normalized = re.sub(r"\s+", " ", (question or "").strip().lower())
    return bool(normalized) and any(token in normalized for token in _SEARCH_INTENT_TOKENS)


def _extract_company_notion_search_query(question: str) -> str:
    query = (question or "").strip()
    for pattern in _QUERY_NOISE_PATTERNS:
        query = pattern.sub(" ", query)
    query = _QUERY_REQUEST_WORDS.sub(" ", query)
    query = re.sub(r"[?？!！.,:;~]+", " ", query)
    return re.sub(r"\s+", " ", query).strip()[:120]


def _is_company_notion_search_configured() -> bool:
    return bool(cs.NOTION_TOKEN_COMPANY and cs.COMPANY_NOTION_SEARCH_ROOT_PAGE_ID)


def _is_company_notion_search_allowed(user_id: str | None) -> bool:
    normalized_user_id = str(user_id or "").strip()
    return bool(normalized_user_id and normalized_user_id in cs.COMPANY_NOTION_SEARCH_ALLOWED_USER_IDS)


def _extract_notion_object_title(payload: dict[str, Any]) -> str:
    if payload.get("object") == "page":
        properties = payload.get("properties")
        if isinstance(properties, dict):
            for property_payload in properties.values():
                if not isinstance(property_payload, dict) or property_payload.get("type") != "title":
                    continue
                title_parts = property_payload.get("title") or []
                return "".join(
                    str(part.get("plain_text") or "")
                    for part in title_parts
                    if isinstance(part, dict)
                ).strip()

    title_parts = payload.get("title") or []
    if isinstance(title_parts, list):
        return "".join(
            str(part.get("plain_text") or "")
            for part in title_parts
            if isinstance(part, dict)
        ).strip()
    return ""


def _extract_parent_reference(parent: object) -> tuple[str, str]:
    if not isinstance(parent, dict):
        return "", ""
    parent_type = str(parent.get("type") or "").strip()
    if parent_type == "workspace":
        return parent_type, ""
    parent_id = str(parent.get(parent_type) or "").strip()
    if parent_type and parent_id:
        return parent_type, parent_id
    for fallback_type in _PARENT_ENDPOINTS:
        fallback_id = str(parent.get(fallback_type) or "").strip()
        if fallback_id:
            return fallback_type, fallback_id
    return "", ""


def _parent_cache_key(parent_type: str, object_id: str, token: str) -> str:
    # 같은 ID라도 integration마다 보이는 상위 트리가 다를 수 있어서
    # 토큰 scope까지 캐시 키에 포함한다.
    token_scope = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
    return f"{token_scope}:{parent_type}:{_normalize_notion_id(object_id)}"


def _load_parent_reference(parent_type: str, object_id: str, *, token: str) -> dict[str, Any]:
    endpoint_template = _PARENT_ENDPOINTS.get(parent_type)
    if not endpoint_template:
        return {}

    cache_key = _parent_cache_key(parent_type, object_id, token)
    now = time.time()
    cached = _PARENT_CACHE.get(cache_key)
    if isinstance(cached, dict) and float(cached.get("expires_at") or 0) > now:
        parent = cached.get("parent")
        return parent if isinstance(parent, dict) else {}

    payload = _notion_request(
        endpoint_template.format(object_id=_normalize_notion_id(object_id)),
        token=token,
    )
    parent = payload.get("parent") if isinstance(payload, dict) else None
    normalized_parent = parent if isinstance(parent, dict) else {}
    _PARENT_CACHE[cache_key] = {
        "expires_at": now + _PARENT_CACHE_TTL_SEC,
        "parent": normalized_parent,
    }
    return normalized_parent


def _is_notion_object_within_root(
    payload: dict[str, Any],
    *,
    root_page_id: str,
    token: str,
    max_depth: int,
) -> bool:
    root_id = _normalize_notion_id(root_page_id)
    object_id = str(payload.get("id") or "").strip()
    if object_id and _normalize_notion_id(object_id) == root_id:
        return True

    # 검색 API는 root 필터를 제공하지 않으므로 후보의 parent chain을 따라가며
    # Work Board 하위인지 확인한다.
    parent = payload.get("parent")
    seen: set[tuple[str, str]] = set()
    for _ in range(max(1, max_depth)):
        parent_type, parent_id = _extract_parent_reference(parent)
        if parent_type == "workspace" or not parent_type or not parent_id:
            return False
        normalized_parent_id = _normalize_notion_id(parent_id)
        if normalized_parent_id == root_id:
            return True
        marker = (parent_type, normalized_parent_id)
        if marker in seen:
            return False
        seen.add(marker)
        parent = _load_parent_reference(parent_type, normalized_parent_id, token=token)
    return False


def _score_company_notion_title(title: str, query: str) -> int:
    normalized_title = re.sub(r"\s+", " ", title.strip().lower())
    normalized_query = re.sub(r"\s+", " ", query.strip().lower())
    if not normalized_title or not normalized_query:
        return 0

    query_tokens = {token.lower() for token in _LOOKUP_TOKEN_PATTERN.findall(normalized_query)}
    title_tokens = {token.lower() for token in _LOOKUP_TOKEN_PATTERN.findall(normalized_title)}
    score = len(query_tokens & title_tokens) * 10
    if normalized_query == normalized_title:
        score += 100
    elif normalized_query in normalized_title:
        score += 40
    return score


def _build_company_notion_api_queries(query: str) -> list[str]:
    normalized_query = re.sub(r"\s+", " ", (query or "").strip())
    lowered_query = normalized_query.lower()
    queries = [normalized_query]
    for tokens, alias in _COMPANY_NOTION_QUERY_ALIASES:
        if not any(token in lowered_query for token in tokens):
            continue
        if alias.lower() not in {item.lower() for item in queries}:
            queries.append(alias)
    return queries


def _search_company_notion(
    query: str,
    *,
    max_results: int | None = None,
    max_candidates: int | None = None,
    root_page_id: str | None = None,
    token: str | None = None,
) -> list[CompanyNotionSearchResult]:
    normalized_query = re.sub(r"\s+", " ", (query or "").strip())
    if not normalized_query:
        return []

    notion_token = str(token or cs.NOTION_TOKEN_COMPANY or "").strip()
    target_root_page_id = str(root_page_id or cs.COMPANY_NOTION_SEARCH_ROOT_PAGE_ID or "").strip()
    if not notion_token or not target_root_page_id:
        raise RuntimeError("회사 Notion 검색 설정이 없어")

    result_limit = max(1, min(10, max_results or cs.COMPANY_NOTION_SEARCH_MAX_RESULTS))
    candidate_limit = max(
        result_limit,
        min(100, max_candidates or cs.COMPANY_NOTION_SEARCH_MAX_CANDIDATES),
    )
    api_queries = _build_company_notion_api_queries(normalized_query)
    candidates: list[dict[str, Any]] = []
    for api_query in api_queries:
        response = _notion_request(
            "/search",
            method="POST",
            payload={"query": api_query, "page_size": candidate_limit},
            token=notion_token,
        )
        candidates.extend(
            candidate
            for candidate in (response.get("results") or [])
            if isinstance(candidate, dict)
        )

    ranked: list[tuple[int, int, CompanyNotionSearchResult]] = []
    seen_ids: set[str] = set()
    ranking_query = " ".join(api_queries)
    for index, candidate in enumerate(candidates):
        object_type = str(candidate.get("object") or "").strip()
        if object_type not in {"page", "database", "data_source"}:
            continue
        if bool(candidate.get("archived")) or bool(candidate.get("in_trash")):
            continue
        object_id = str(candidate.get("id") or "").strip()
        title = _extract_notion_object_title(candidate)
        url = str(candidate.get("url") or "").strip()
        if not object_id or not title or not url:
            continue
        normalized_object_id = _normalize_notion_id(object_id)
        if normalized_object_id in seen_ids:
            continue
        title_score = _score_company_notion_title(title, ranking_query)
        if title_score <= 0:
            continue

        try:
            is_in_scope = _is_notion_object_within_root(
                candidate,
                root_page_id=target_root_page_id,
                token=notion_token,
                max_depth=cs.COMPANY_NOTION_SEARCH_PARENT_MAX_DEPTH,
            )
        except (RuntimeError, ValueError):
            # 상위 경로를 증명하지 못한 결과는 넓게 노출하지 않고 안전하게 제외한다.
            continue
        if not is_in_scope:
            continue

        seen_ids.add(normalized_object_id)
        result = CompanyNotionSearchResult(
            object_id=normalized_object_id,
            object_type=object_type,
            title=title,
            url=url,
            last_edited_time=str(candidate.get("last_edited_time") or "").strip(),
        )
        ranked.append((title_score, -index, result))

    ranked.sort(key=lambda item: (item[0], item[1], item[2].title.lower()), reverse=True)
    return [item[2] for item in ranked[:result_limit]]


def _extract_rich_text_parts(parts: object) -> str:
    if not isinstance(parts, list):
        return ""
    return "".join(
        str(part.get("plain_text") or "")
        for part in parts
        if isinstance(part, dict)
    ).strip()


def _extract_company_notion_block_text(block: dict[str, Any]) -> str:
    text = _extract_block_text(block)
    if text:
        return text[:600]

    block_type = str(block.get("type") or "").strip()
    payload = block.get(block_type)
    if not isinstance(payload, dict):
        return ""
    if block_type == "child_database":
        return str(payload.get("title") or "").strip()[:600]
    if block_type == "table_row":
        cells = payload.get("cells") or []
        return " | ".join(
            cell_text
            for cell_text in (_extract_rich_text_parts(cell) for cell in cells)
            if cell_text
        )[:600]
    if block_type == "equation":
        return str(payload.get("expression") or "").strip()[:600]
    return ""


def _content_cache_key(page_id: str, token: str, max_depth: int, max_blocks: int) -> str:
    token_scope = hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]
    return (
        f"{token_scope}:content:{_normalize_notion_id(page_id)}:"
        f"depth={max_depth}:blocks={max_blocks}"
    )


def _load_company_notion_page_lines(
    page_id: str,
    *,
    token: str,
    max_depth: int,
    max_blocks: int,
) -> dict[str, Any]:
    depth_limit = max(0, min(8, max_depth))
    block_limit = max(1, min(500, max_blocks))
    cache_key = _content_cache_key(page_id, token, depth_limit, block_limit)
    now = time.time()
    cached = _CONTENT_CACHE.get(cache_key)
    if isinstance(cached, dict) and float(cached.get("expires_at") or 0) > now:
        payload = cached.get("payload")
        if isinstance(payload, dict):
            return payload

    lines: list[str] = []
    block_count = 0
    truncated = False

    def collect(parent_id: str, depth: int) -> None:
        nonlocal block_count, truncated
        cursor: str | None = None
        while block_count < block_limit:
            remaining = block_limit - block_count
            response = _fetch_notion_block_children(
                parent_id,
                start_cursor=cursor,
                page_size=min(100, remaining),
                token=token,
            )
            blocks = response.get("results") or []
            for block in blocks:
                if block_count >= block_limit:
                    truncated = True
                    return
                if not isinstance(block, dict):
                    continue
                block_count += 1
                text = _extract_company_notion_block_text(block)
                if text:
                    lines.append(text)

                block_type = str(block.get("type") or "").strip()
                if not bool(block.get("has_children")):
                    continue
                if block_type in _CONTENT_BOUNDARY_BLOCK_TYPES:
                    continue
                if depth >= depth_limit:
                    truncated = True
                    continue
                child_id = str(block.get("id") or "").strip()
                if child_id:
                    collect(child_id, depth + 1)
                if block_count >= block_limit:
                    truncated = True
                    return

            if not response.get("has_more") or not response.get("next_cursor"):
                return
            cursor = str(response.get("next_cursor") or "").strip() or None
        truncated = True

    collect(_normalize_notion_id(page_id), 0)
    payload = {
        "lines": lines,
        "blockCount": block_count,
        "truncated": truncated,
    }
    _CONTENT_CACHE[cache_key] = {
        "expires_at": now + _CONTENT_CACHE_TTL_SEC,
        "payload": payload,
    }
    return payload


def _select_company_notion_excerpts(
    lines: list[str],
    query: str,
    *,
    max_chars: int,
) -> list[str]:
    normalized_query = re.sub(r"\s+", " ", (query or "").strip().lower())
    expanded_queries = _build_company_notion_api_queries(normalized_query)
    query_tokens = {
        token.lower()
        for expanded_query in expanded_queries
        for token in _LOOKUP_TOKEN_PATTERN.findall(expanded_query)
        if len(token.strip()) >= 2 and token.lower() not in _CONTENT_LOW_SIGNAL_TERMS
    }
    unique_lines: list[str] = []
    seen: set[str] = set()
    for raw_line in lines:
        line = re.sub(r"\s+", " ", str(raw_line or "").strip())
        normalized_line = line.lower()
        if len(line) < 2 or normalized_line in seen:
            continue
        seen.add(normalized_line)
        unique_lines.append(line)

    scored: list[tuple[int, int]] = []
    for index, line in enumerate(unique_lines):
        normalized_line = line.lower()
        score = sum(10 for token in query_tokens if token in normalized_line)
        if normalized_query and normalized_query in normalized_line:
            score += 30
        if ":" in line or "：" in line:
            score += 1
        scored.append((score, index))

    matched_seeds = [
        index
        for score, index in sorted(scored, key=lambda item: (item[0], -item[1]), reverse=True)
        if score > 0
    ][:3]
    seeds = matched_seeds or [index for _, index in scored[:3]]
    selected_indices: set[int] = set()
    for seed in seeds:
        for index in range(max(0, seed - 1), min(len(unique_lines), seed + 2)):
            selected_indices.add(index)

    excerpts: list[str] = []
    used_chars = 0
    char_limit = max(300, max_chars)
    for index in sorted(selected_indices):
        excerpt = unique_lines[index][:320]
        if not excerpt:
            continue
        next_size = len(excerpt) + (1 if excerpts else 0)
        if excerpts and used_chars + next_size > char_limit:
            break
        excerpts.append(excerpt)
        used_chars += next_size
        if len(excerpts) >= 9:
            break
    return excerpts


def _load_company_notion_references(
    query: str,
    results: list[CompanyNotionSearchResult],
    *,
    token: str | None = None,
    max_pages: int | None = None,
    max_total_chars: int | None = None,
) -> list[dict[str, Any]]:
    notion_token = str(token or cs.NOTION_TOKEN_COMPANY or "").strip()
    if not notion_token:
        raise RuntimeError("회사 Notion 토큰 설정이 없어")

    page_limit = max(1, min(5, max_pages or cs.COMPANY_NOTION_ANSWER_MAX_PAGES))
    total_char_limit = max(
        800,
        min(8000, max_total_chars or cs.COMPANY_NOTION_EVIDENCE_MAX_CHARS),
    )
    per_page_char_limit = max(500, min(1800, total_char_limit // page_limit))
    remaining_chars = total_char_limit
    loaded_pages = 0
    references: list[dict[str, Any]] = []

    for result in results[:5]:
        reference: dict[str, Any] = {
            "title": result.title,
            "url": result.url,
            "objectType": result.object_type,
            "lastEditedTime": result.last_edited_time,
            "excerpts": [],
        }
        if result.object_type == "page" and loaded_pages < page_limit and remaining_chars > 0:
            try:
                content = _load_company_notion_page_lines(
                    result.object_id,
                    token=notion_token,
                    max_depth=cs.COMPANY_NOTION_CONTENT_MAX_DEPTH,
                    max_blocks=cs.COMPANY_NOTION_CONTENT_MAX_BLOCKS,
                )
                excerpt_limit = min(per_page_char_limit, remaining_chars)
                excerpts = _select_company_notion_excerpts(
                    content.get("lines") or [],
                    query,
                    max_chars=excerpt_limit,
                )
                reference.update(
                    {
                        "excerpts": excerpts,
                        "blockCount": int(content.get("blockCount") or 0),
                        "contentTruncated": bool(content.get("truncated")),
                    }
                )
                remaining_chars -= sum(len(excerpt) for excerpt in excerpts)
                loaded_pages += 1
            except (RuntimeError, ValueError):
                # 한 문서 본문을 못 읽어도 다른 검색 결과와 원문 링크는 계속 제공한다.
                reference["contentUnavailable"] = True
        references.append(reference)
    return references


def _build_company_notion_source_docs(
    references: list[dict[str, Any]],
) -> list[dict[str, str]]:
    docs: list[dict[str, str]] = []
    for reference in references[:5]:
        title = str(reference.get("title") or "").strip()
        url = str(reference.get("url") or "").strip()
        if not title or not url.startswith(("https://www.notion.so/", "https://app.notion.com/")):
            continue
        docs.append({"title": _escape_slack_text(title), "url": url})
    return docs


def _escape_slack_text(text: str) -> str:
    return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace("|", "/")


def _build_company_notion_search_reply(
    query: str,
    results: list[CompanyNotionSearchResult],
) -> str:
    safe_query = _escape_slack_text(query).replace("`", "'")
    if not results:
        return (
            f"회사 Work Board에서 `{safe_query}` 제목의 문서를 찾지 못했어. "
            "지금은 제목 기준 검색이라 다른 핵심 키워드로 다시 찾아줘"
        )

    lines = ["*회사 Notion 검색*", f"• 키워드: `{safe_query}`"]
    for result in results:
        safe_title = _escape_slack_text(result.title)
        if result.url.startswith(("https://www.notion.so/", "https://app.notion.com/")):
            lines.append(f"• <{result.url}|{safe_title}>")
        else:
            lines.append(f"• {safe_title}")
    lines.append("_현재는 Work Board 범위의 제목 검색이야._")
    return "\n".join(lines)


__all__ = [
    "CompanyNotionSearchResult",
    "_build_company_notion_search_reply",
    "_build_company_notion_source_docs",
    "_extract_company_notion_search_query",
    "_is_company_notion_search_allowed",
    "_is_company_notion_search_configured",
    "_looks_like_company_notion_search",
    "_load_company_notion_references",
    "_search_company_notion",
]
