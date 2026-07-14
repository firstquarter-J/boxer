from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from boxer.retrieval import MarkdownKnowledgeSource, NotionKnowledgeSource

from boxer_adapter_web.schemas import KnowledgeDocumentDetailDto, KnowledgeDocumentSummaryDto, KnowledgeStatusDto, KnowledgeSyncRunDto
from boxer_adapter_web.settings import WebSettings
from boxer_adapter_web.storage import WebChatStore


class KnowledgeManager:
    def __init__(self, store: WebChatStore, settings: WebSettings) -> None:
        self._store = store
        self._settings = settings

    def ensure_initial_sync(self) -> None:
        if self._store.knowledge_document_count() > 0:
            return
        self.sync_documents()

    def sync_documents(self) -> KnowledgeSyncRunDto:
        source_type = self._settings.knowledge_source
        started_at = _utc_now()

        try:
            source = self._build_source()
            documents = source.load_documents()
            record = self._store.replace_knowledge_documents(
                source_type=source_type,
                documents=documents,
                started_at=started_at,
                finished_at=_utc_now(),
            )
        except Exception as exc:
            record = self._store.record_failed_sync(
                source_type=source_type,
                error_message=str(exc),
                started_at=started_at,
                finished_at=_utc_now(),
            )

        return _to_sync_run_dto(record)

    def get_status(self) -> KnowledgeStatusDto:
        latest = self._store.get_latest_sync_run()
        return KnowledgeStatusDto(
            activeSource=self._settings.knowledge_source,
            documentCount=self._store.knowledge_document_count(),
            lastSync=_to_sync_run_dto(latest) if latest else None,
        )

    def list_documents(self) -> list[KnowledgeDocumentSummaryDto]:
        documents = []
        for row in self._store.list_knowledge_documents():
            excerpt = str(row.get("content") or "").strip().replace("\n", " ")[:160]
            documents.append(
                KnowledgeDocumentSummaryDto(
                    id=row["id"],
                    title=row["title"],
                    sourceType=row["source_type"],
                    sourceUri=row["source_uri"],
                    excerpt=excerpt,
                    syncedAt=row["synced_at"],
                )
            )
        return documents

    def get_document(self, document_id: str) -> KnowledgeDocumentDetailDto | None:
        row = self._store.get_knowledge_document(document_id)
        if not row:
            return None
        excerpt = str(row.get("content") or "").strip().replace("\n", " ")[:160]
        metadata = row.get("metadata_json")
        return KnowledgeDocumentDetailDto(
            id=row["id"],
            title=row["title"],
            sourceType=row["source_type"],
            sourceUri=row["source_uri"],
            excerpt=excerpt,
            syncedAt=row["synced_at"],
            content=row["content"],
            metadata={} if metadata is None else json.loads(str(metadata)),
        )

    def _build_source(self):
        if self._settings.knowledge_source == "markdown":
            return MarkdownKnowledgeSource(self._settings.markdown_root)
        if self._settings.knowledge_source == "notion":
            if not self._settings.notion_page_ids:
                raise RuntimeError("NOTION_TEST_PAGE_ID 설정이 없어")
            return NotionKnowledgeSource(self._settings.notion_page_ids)
        raise RuntimeError(f"지원하지 않는 knowledge source야: {self._settings.knowledge_source}")


def _to_sync_run_dto(record: dict[str, Any]) -> KnowledgeSyncRunDto:
    return KnowledgeSyncRunDto(
        id=int(record["id"]),
        sourceType=str(record["source_type"]),
        status=str(record["status"]),
        documentCount=int(record["document_count"]),
        errorMessage=str(record["error_message"]) if record.get("error_message") is not None else None,
        startedAt=str(record["started_at"]),
        finishedAt=str(record["finished_at"]) if record.get("finished_at") is not None else None,
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
