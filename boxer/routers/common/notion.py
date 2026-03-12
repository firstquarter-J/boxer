import json
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from boxer.core import settings as s

_NOTION_CACHE_TTL_SEC = 300
_NOTION_PAGE_CACHE: dict[str, dict[str, Any]] = {}


def _is_notion_configured() -> bool:
    return bool(s.NOTION_TOKEN and s.NOTION_API_BASE_URL and s.NOTION_API_VERSION)


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


def _build_notion_headers() -> dict[str, str]:
    if not _is_notion_configured():
        raise RuntimeError("Notion 설정이 없어")
    return {
        "Authorization": f"Bearer {s.NOTION_TOKEN}",
        "Notion-Version": s.NOTION_API_VERSION,
        "Content-Type": "application/json",
    }


def _notion_request(
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        f"{s.NOTION_API_BASE_URL}{path}",
        data=body,
        headers=_build_notion_headers(),
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=max(1, s.NOTION_API_TIMEOUT_SEC)) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Notion API 오류: {exc.code} {detail}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Notion API 연결 실패: {exc.reason}") from exc


def _fetch_notion_page(page_id: str) -> dict[str, Any]:
    return _notion_request(f"/pages/{_normalize_notion_id(page_id)}")


def _fetch_notion_block_children(
    block_id: str,
    *,
    start_cursor: str | None = None,
    page_size: int = 100,
) -> dict[str, Any]:
    query: dict[str, Any] = {"page_size": max(1, min(100, page_size))}
    if start_cursor:
        query["start_cursor"] = start_cursor
    return _notion_request(
        f"/blocks/{_normalize_notion_id(block_id)}/children?{urllib.parse.urlencode(query)}"
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


def _fetch_all_notion_blocks(page_id: str) -> list[dict[str, Any]]:
    blocks: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        response = _fetch_notion_block_children(
            page_id,
            start_cursor=cursor,
            page_size=min(100, max(1, s.NOTION_MAX_BLOCKS)),
        )
        results = response.get("results", [])
        for result in results:
            if isinstance(result, dict):
                blocks.append(result)
                if len(blocks) >= max(1, s.NOTION_MAX_BLOCKS):
                    return blocks
        if not response.get("has_more") or not response.get("next_cursor"):
            return blocks
        cursor = response.get("next_cursor")


def _load_notion_page_content(page_id: str) -> dict[str, Any]:
    normalized_page_id = _normalize_notion_id(page_id)
    page_payload = _fetch_notion_page(normalized_page_id)
    blocks = _fetch_all_notion_blocks(normalized_page_id)
    lines = _flatten_notion_blocks(blocks)
    return {
        "pageId": normalized_page_id,
        "title": _extract_notion_page_title(page_payload),
        "url": page_payload.get("url", ""),
        "blockCount": len(blocks),
        "lines": lines,
        "plainText": "\n".join(lines).strip(),
    }


def _load_notion_page_content_cached(page_id: str) -> dict[str, Any]:
    normalized_page_id = _normalize_notion_id(page_id)
    now = time.time()
    cached = _NOTION_PAGE_CACHE.get(normalized_page_id)
    if isinstance(cached, dict) and float(cached.get("expires_at") or 0) > now:
        payload = cached.get("payload")
        if isinstance(payload, dict):
            return payload

    payload = _load_notion_page_content(normalized_page_id)
    _NOTION_PAGE_CACHE[normalized_page_id] = {
        "expires_at": now + _NOTION_CACHE_TTL_SEC,
        "payload": payload,
    }
    return payload
