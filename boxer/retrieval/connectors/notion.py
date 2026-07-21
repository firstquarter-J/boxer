import hashlib
import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from boxer.core import settings as s

_NOTION_CACHE_TTL_SEC = 300
_NOTION_PAGE_CACHE: dict[str, dict[str, Any]] = {}


def _resolve_notion_token(token: str | None = None) -> str:
    return (token or s.NOTION_TOKEN_PERSONAL or "").strip()


def _notion_token_scope(token: str | None = None) -> str:
    # 원문 토큰을 캐시 키에 노출하지 않으면서 integration별 캐시를 격리한다.
    resolved_token = _resolve_notion_token(token)
    return hashlib.sha256(resolved_token.encode("utf-8")).hexdigest()[:12] if resolved_token else "default"


def _notion_cache_key(page_id: str, token: str | None = None) -> str:
    # 같은 page_id라도 integration이 다르면 접근 가능한 workspace가 달라질 수 있어서 캐시를 토큰별로 분리한다.
    return f"{_notion_token_scope(token)}:{page_id}"


def _invalidate_notion_page_cache(
    page_id: str | None = None,
    *,
    token: str | None = None,
    all_tokens: bool = False,
) -> None:
    # 특정 integration의 page만 지워 다른 workspace의 동일 page id 캐시는 보존한다.
    if page_id:
        normalized_page_id = _normalize_notion_id(page_id)
        if all_tokens:
            page_suffix = f":{normalized_page_id}"
            for cache_key in tuple(_NOTION_PAGE_CACHE):
                if cache_key.endswith(page_suffix):
                    _NOTION_PAGE_CACHE.pop(cache_key, None)
            return
        _NOTION_PAGE_CACHE.pop(_notion_cache_key(normalized_page_id, token), None)
        return

    if token is None:
        _NOTION_PAGE_CACHE.clear()
        return

    token_prefix = f"{_notion_token_scope(token)}:"
    for cache_key in tuple(_NOTION_PAGE_CACHE):
        if cache_key.startswith(token_prefix):
            _NOTION_PAGE_CACHE.pop(cache_key, None)


def _is_notion_configured(token: str | None = None) -> bool:
    return bool(_resolve_notion_token(token) and s.NOTION_API_BASE_URL and s.NOTION_API_VERSION)


def _normalize_notion_id(raw_value: str) -> str:
    value = (raw_value or "").strip()
    if not value:
        raise ValueError("Notion id가 비어있어")
    if "/" in value:
        value = value.rstrip("/").split("/")[-1]
    value = value.split("?")[0]
    value = value.replace("-", "")
    if len(value) > 32:
        value = value[-32:]
    if len(value) != 32:
        raise ValueError("Notion id 형식이 올바르지 않아")
    return value


def _build_notion_headers(token: str | None = None) -> dict[str, str]:
    resolved_token = _resolve_notion_token(token)
    if not _is_notion_configured(resolved_token):
        raise RuntimeError("Notion 설정이 없어")
    return {
        "Authorization": f"Bearer {resolved_token}",
        "Notion-Version": s.NOTION_API_VERSION,
        "Content-Type": "application/json",
    }


def _notion_request(
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    token: str | None = None,
) -> dict[str, Any]:
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{s.NOTION_API_BASE_URL}{path}",
        data=body,
        headers=_build_notion_headers(token),
        method=method,
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=max(1, s.NOTION_API_TIMEOUT_SEC)) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            response_headers = exc.headers
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            finally:
                exc.close()
            if exc.code == 429 and attempt < 2:
                # Notion의 Retry-After를 따라 일회성 rate limit 때문에 전체 조회가 실패하지 않게 한다.
                raw_retry_after = str((response_headers or {}).get("Retry-After") or "1").strip()
                try:
                    retry_after = float(raw_retry_after)
                except ValueError:
                    retry_after = 1.0
                time.sleep(max(0.1, min(10.0, retry_after)))
                continue
            raise RuntimeError(f"Notion API 오류: {exc.code} {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"Notion API 연결 실패: {exc.reason}") from exc
        except (OSError, http.client.HTTPException) as exc:
            # socket reset이나 불완전 응답도 상위 retrieval fallback이 처리할 수 있는 오류로 통일한다.
            raise RuntimeError(f"Notion API 연결 실패: {exc}") from exc

    raise RuntimeError("Notion API 재시도 횟수를 초과했어")


def _fetch_notion_page(page_id: str, *, token: str | None = None) -> dict[str, Any]:
    return _notion_request(f"/pages/{_normalize_notion_id(page_id)}", token=token)


def _fetch_notion_block_children(
    block_id: str,
    *,
    start_cursor: str | None = None,
    page_size: int = 100,
    token: str | None = None,
) -> dict[str, Any]:
    query: dict[str, Any] = {"page_size": max(1, min(100, page_size))}
    if start_cursor:
        query["start_cursor"] = start_cursor
    return _notion_request(
        f"/blocks/{_normalize_notion_id(block_id)}/children?{urllib.parse.urlencode(query)}",
        token=token,
    )


def _rich_text_to_plain_text(rich_text: list[dict[str, Any]] | None) -> str:
    if not rich_text:
        return ""
    return "".join(part.get("plain_text", "") for part in rich_text if isinstance(part, dict)).strip()


def _extract_notion_page_title(page_payload: dict[str, Any]) -> str:
    properties = page_payload.get("properties", {})
    if not isinstance(properties, dict):
        return ""
    for property_payload in properties.values():
        if not isinstance(property_payload, dict):
            continue
        if property_payload.get("type") == "title":
            return _rich_text_to_plain_text(property_payload.get("title"))
    return ""


def _extract_block_text(block: dict[str, Any]) -> str:
    block_type = block.get("type", "")
    payload = block.get(block_type, {})
    if not isinstance(payload, dict):
        return ""
    if block_type == "child_page":
        return payload.get("title", "").strip()
    if block_type == "to_do":
        prefix = "[x] " if payload.get("checked") else "[ ] "
        return f"{prefix}{_rich_text_to_plain_text(payload.get('rich_text'))}".strip()
    if "rich_text" in payload:
        return _rich_text_to_plain_text(payload.get("rich_text"))
    return ""


def _flatten_notion_blocks(blocks: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    for block in blocks:
        block_type = block.get("type", "")
        text = _extract_block_text(block)
        if not text:
            continue
        if block_type == "bulleted_list_item":
            lines.append(f"- {text}")
        elif block_type == "numbered_list_item":
            lines.append(f"1. {text}")
        elif block_type in {"heading_1", "heading_2", "heading_3"}:
            lines.append(text)
        elif block_type == "quote":
            lines.append(f"> {text}")
        elif block_type == "code":
            lines.append(f"`{text}`")
        else:
            lines.append(text)
    return lines


def _fetch_all_notion_blocks(
    page_id: str,
    *,
    token: str | None = None,
    max_blocks: int | None = None,
) -> list[dict[str, Any]]:
    # 생략 시 기존 설정 의미(최소 1개)를 유지하고, 명시적인 0만 무제한 조회로 쓴다.
    if max_blocks is not None and max_blocks < 0:
        raise ValueError("Notion block limit은 0 이상이어야 해")
    block_limit = max(1, s.NOTION_MAX_BLOCKS) if max_blocks is None else max_blocks
    blocks: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    while True:
        response = _fetch_notion_block_children(
            page_id,
            start_cursor=cursor,
            page_size=(
                100
                if block_limit <= 0
                else min(100, max(1, block_limit - len(blocks)))
            ),
            token=token,
        )
        results = response.get("results", [])
        for result in results:
            if isinstance(result, dict):
                blocks.append(result)
                if block_limit > 0 and len(blocks) >= max(1, block_limit):
                    return blocks
        next_cursor = str(response.get("next_cursor") or "").strip()
        if not response.get("has_more"):
            return blocks
        if not next_cursor:
            raise RuntimeError("Notion block pagination cursor가 비어있어")
        if next_cursor in seen_cursors:
            raise RuntimeError("Notion block pagination cursor가 반복됐어")
        seen_cursors.add(next_cursor)
        cursor = next_cursor


def _load_notion_page_content(page_id: str, *, token: str | None = None) -> dict[str, Any]:
    normalized_page_id = _normalize_notion_id(page_id)
    page_payload = _fetch_notion_page(normalized_page_id, token=token)
    blocks = _fetch_all_notion_blocks(normalized_page_id, token=token)
    lines = _flatten_notion_blocks(blocks)
    return {
        "pageId": normalized_page_id,
        "title": _extract_notion_page_title(page_payload),
        "url": page_payload.get("url", ""),
        "blockCount": len(blocks),
        "lines": lines,
        "plainText": "\n".join(lines).strip(),
    }


def _load_notion_page_content_cached(page_id: str, *, token: str | None = None) -> dict[str, Any]:
    normalized_page_id = _normalize_notion_id(page_id)
    cache_key = _notion_cache_key(normalized_page_id, token)
    now = time.time()
    cached = _NOTION_PAGE_CACHE.get(cache_key)
    if isinstance(cached, dict) and float(cached.get("expires_at") or 0) > now:
        payload = cached.get("payload")
        if isinstance(payload, dict):
            return payload

    payload = _load_notion_page_content(normalized_page_id, token=token)
    _NOTION_PAGE_CACHE[cache_key] = {
        "expires_at": now + _NOTION_CACHE_TTL_SEC,
        "payload": payload,
    }
    return payload
