from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from boxer.retrieval.connectors.notion import _load_notion_page_content_cached

_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+|[가-힣]+")


@dataclass(slots=True)
class KnowledgeDocument:
    id: str
    title: str
    content: str
    source_type: str
    source_uri: str
    metadata: dict[str, str]


@dataclass(slots=True)
class KnowledgeSearchResult:
    document: KnowledgeDocument
    score: float


class KnowledgeSource(Protocol):
    # 동기화와 검색을 모두 열어 두면 adapter가 로컬 인덱스/원격 소스를 모두 같은 인터페이스로 다룰 수 있다.
    def load_documents(self) -> list[KnowledgeDocument]: ...

    def search(self, query: str, *, limit: int = 5) -> list[KnowledgeSearchResult]: ...


class MarkdownKnowledgeSource:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).expanduser()

    def load_documents(self) -> list[KnowledgeDocument]:
        documents: list[KnowledgeDocument] = []
        if not self._root.exists():
            return documents

        for path in sorted(self._root.rglob("*.md")):
            text = path.read_text(encoding="utf-8").strip()
            if not text:
                continue
            title, content = _parse_markdown_document(path, text)
            relative_path = path.relative_to(self._root).as_posix()
            documents.append(
                KnowledgeDocument(
                    id=f"markdown:{relative_path}",
                    title=title,
                    content=content,
                    source_type="markdown",
                    source_uri=relative_path,
                    metadata={"path": relative_path},
                )
            )
        return documents

    def search(self, query: str, *, limit: int = 5) -> list[KnowledgeSearchResult]:
        return _search_documents(self.load_documents(), query, limit=limit)


class NotionKnowledgeSource:
    def __init__(self, page_ids: list[str]) -> None:
        self._page_ids = [page_id.strip() for page_id in page_ids if page_id.strip()]

    def load_documents(self) -> list[KnowledgeDocument]:
        documents: list[KnowledgeDocument] = []
        for raw_page_id in self._page_ids:
            payload = _load_notion_page_content_cached(raw_page_id)
            title = str(payload.get("title") or "").strip() or f"Notion {payload.get('pageId')}"
            content = str(payload.get("plainText") or "").strip()
            page_id = str(payload.get("pageId") or raw_page_id).strip()
            documents.append(
                KnowledgeDocument(
                    id=f"notion:{page_id}",
                    title=title,
                    content=content,
                    source_type="notion",
                    source_uri=str(payload.get("url") or page_id),
                    metadata={"pageId": page_id},
                )
            )
        return documents

    def search(self, query: str, *, limit: int = 5) -> list[KnowledgeSearchResult]:
        return _search_documents(self.load_documents(), query, limit=limit)


def _parse_markdown_document(path: Path, text: str) -> tuple[str, str]:
    title = ""
    body_lines: list[str] = []

    # 첫 heading을 제목으로 보고 나머지는 검색/미리보기 본문으로 사용한다.
    for line in text.splitlines():
        stripped = line.strip()
        if not title and stripped.startswith("# "):
            title = stripped[2:].strip()
            continue
        body_lines.append(line)

    normalized_title = title or path.stem.replace("-", " ").replace("_", " ").strip() or path.stem
    normalized_body = "\n".join(body_lines).strip() or text
    return normalized_title, normalized_body


def _search_documents(
    documents: list[KnowledgeDocument],
    query: str,
    *,
    limit: int = 5,
) -> list[KnowledgeSearchResult]:
    query_tokens = tokenize_knowledge_text(query)
    if not query_tokens:
        return []

    results: list[KnowledgeSearchResult] = []
    for document in documents:
        score = score_knowledge_document(document, query=query, tokens=query_tokens)
        if score <= 0:
            continue
        results.append(KnowledgeSearchResult(document=document, score=score))

    results.sort(key=lambda item: (-item.score, item.document.title.lower(), item.document.id))
    return results[: max(1, limit)]


def tokenize_knowledge_text(text: str) -> list[str]:
    """Markdown/Notion과 adapter 저장소가 함께 쓰는 결정적 검색 토큰을 만든다."""
    seen: set[str] = set()
    tokens: list[str] = []
    for token in _TOKEN_PATTERN.findall(normalize_knowledge_text(text)):
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def score_knowledge_document(
    document: KnowledgeDocument,
    *,
    query: str,
    tokens: list[str] | None = None,
) -> float:
    """source 종류와 무관하게 같은 overlap/phrase/title 점수로 문서를 정렬한다."""
    query_tokens = set(tokens if tokens is not None else tokenize_knowledge_text(query))
    if not query_tokens:
        return 0.0

    normalized_query = normalize_knowledge_text(query)
    normalized_title = normalize_knowledge_text(document.title)
    normalized_haystack = normalize_knowledge_text(f"{document.title}\n{document.content}")
    haystack_tokens = set(tokenize_knowledge_text(normalized_haystack))
    matched_tokens = query_tokens & haystack_tokens
    matched_substrings = {token for token in query_tokens if token in normalized_haystack}
    if not matched_tokens and not matched_substrings:
        return 0.0

    # 단순한 결정적 점수는 원격 source와 SQLite 후보가 같은 순서를 재현하게 한다.
    matched = matched_tokens | matched_substrings
    coverage_score = len(matched) / max(1, len(query_tokens))
    density_score = len(matched) / max(1.0, math.sqrt(len(haystack_tokens)))
    phrase_bonus = 0.25 if normalized_query and normalized_query in normalized_haystack else 0.0
    title_bonus = 0.15 if any(token in normalized_title for token in query_tokens) else 0.0
    return round(coverage_score + density_score + phrase_bonus + title_bonus, 4)


def normalize_knowledge_text(text: str) -> str:
    """모든 knowledge source와 adapter가 공유하는 검색 문자열 정규화 계약이다."""

    return str(text or "").strip().lower()


__all__ = [
    "KnowledgeDocument",
    "KnowledgeSearchResult",
    "KnowledgeSource",
    "MarkdownKnowledgeSource",
    "NotionKnowledgeSource",
    "normalize_knowledge_text",
    "score_knowledge_document",
    "tokenize_knowledge_text",
]
