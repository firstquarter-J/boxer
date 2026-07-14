from __future__ import annotations

import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from boxer.retrieval import KnowledgeDocument, KnowledgeSearchResult

_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_]+|[가-힣]+")


class WebChatStore:
    def __init__(self, db_path: str | Path) -> None:
        self._db_path = Path(db_path)

    def initialize(self) -> None:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._managed_connection() as connection:
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS admin_users (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    name TEXT NOT NULL,
                    password_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS conversations (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL UNIQUE,
                    customer_id TEXT,
                    customer_name TEXT,
                    customer_email TEXT,
                    status TEXT NOT NULL DEFAULT 'starter',
                    workflow_key TEXT,
                    workflow_state_json TEXT NOT NULL DEFAULT '{}',
                    assigned_admin_user_id TEXT,
                    handoff_requested_at TEXT,
                    handoff_started_at TEXT,
                    closed_at TEXT,
                    context_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    conversation_id TEXT NOT NULL,
                    sender_type TEXT NOT NULL,
                    sender_name TEXT,
                    admin_user_id TEXT,
                    body TEXT NOT NULL,
                    source_refs_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS knowledge_documents (
                    id TEXT PRIMARY KEY,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    source_uri TEXT NOT NULL,
                    metadata_json TEXT NOT NULL,
                    synced_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS knowledge_sync_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    document_count INTEGER NOT NULL,
                    error_message TEXT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT
                );

                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    applied_at TEXT NOT NULL
                );

                CREATE VIRTUAL TABLE IF NOT EXISTS knowledge_documents_fts USING fts5(
                    document_id UNINDEXED,
                    title,
                    content
                );
                """
            )
            self._migrate_schema(connection)

    def upsert_admin_user(self, email: str, name: str, password_hash: str) -> dict[str, Any]:
        now = _utc_now()
        existing_admin_user = self.get_admin_user_by_email(email)
        admin_user_id = str(existing_admin_user["id"]) if existing_admin_user else str(uuid4())
        with self._managed_connection() as connection:
            connection.execute(
                """
                INSERT INTO admin_users (id, email, name, password_hash, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(email) DO UPDATE SET
                    name = excluded.name,
                    password_hash = excluded.password_hash,
                    updated_at = excluded.updated_at
                """,
                (admin_user_id, email.strip().lower(), name.strip(), password_hash, now, now),
            )
        return self.get_admin_user_by_email(email) or {}

    def get_admin_user_by_email(self, email: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM admin_users WHERE email = ?",
            (email.strip().lower(),),
        )

    def get_admin_user_by_id(self, admin_user_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM admin_users WHERE id = ?",
            (admin_user_id,),
        )

    def get_or_create_conversation(
        self,
        *,
        session_id: str | None,
        identity: dict[str, Any] | None,
        context: dict[str, Any] | None,
    ) -> dict[str, Any]:
        existing = None
        if session_id:
            existing = self._fetch_one(
                "SELECT * FROM conversations WHERE session_id = ?",
                (session_id,),
            )

        normalized_identity = identity or {}
        normalized_context = context or {}
        now = _utc_now()

        if existing:
            merged_context = _merge_context(existing.get("context_json"), normalized_context)
            with self._managed_connection() as connection:
                connection.execute(
                    """
                    UPDATE conversations
                    SET customer_id = COALESCE(?, customer_id),
                        customer_name = COALESCE(?, customer_name),
                        customer_email = COALESCE(?, customer_email),
                        status = CASE WHEN status = 'closed' THEN 'starter' ELSE status END,
                        workflow_key = CASE WHEN status = 'closed' THEN NULL ELSE workflow_key END,
                        workflow_state_json = CASE WHEN status = 'closed' THEN '{}' ELSE workflow_state_json END,
                        assigned_admin_user_id = CASE WHEN status = 'closed' THEN NULL ELSE assigned_admin_user_id END,
                        handoff_requested_at = CASE WHEN status = 'closed' THEN NULL ELSE handoff_requested_at END,
                        handoff_started_at = CASE WHEN status = 'closed' THEN NULL ELSE handoff_started_at END,
                        closed_at = CASE WHEN status = 'closed' THEN NULL ELSE closed_at END,
                        context_json = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        _nullable_strip(normalized_identity.get("id")),
                        _nullable_strip(normalized_identity.get("name")),
                        _nullable_strip(normalized_identity.get("email")),
                        json.dumps(merged_context, ensure_ascii=False),
                        now,
                        existing["id"],
                    ),
                )
            return self.get_conversation_by_session_id(existing["session_id"]) or existing

        created_session_id = session_id or str(uuid4())
        conversation_id = str(uuid4())
        with self._managed_connection() as connection:
            connection.execute(
                """
                INSERT INTO conversations (
                    id, session_id, customer_id, customer_name, customer_email, context_json, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    created_session_id,
                    _nullable_strip(normalized_identity.get("id")),
                    _nullable_strip(normalized_identity.get("name")),
                    _nullable_strip(normalized_identity.get("email")),
                    json.dumps(normalized_context, ensure_ascii=False),
                    now,
                    now,
                ),
            )
        return self.get_conversation_by_session_id(created_session_id) or {}

    def get_conversation_by_session_id(self, session_id: str) -> dict[str, Any] | None:
        conversation = self._fetch_one(
            "SELECT * FROM conversations WHERE session_id = ?",
            (session_id,),
        )
        if not conversation:
            return None
        return self.get_conversation_by_id(conversation["id"])

    def get_conversation_by_id(self, conversation_id: str) -> dict[str, Any] | None:
        conversation = self._fetch_one(
            """
            SELECT c.*, au.name AS assigned_admin_user_name
            FROM conversations c
            LEFT JOIN admin_users au ON au.id = c.assigned_admin_user_id
            WHERE c.id = ?
            """,
            (conversation_id,),
        )
        if not conversation:
            return None
        return {
            **conversation,
            "messages": self.list_messages(conversation_id),
        }

    def list_conversations(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        query: str | None = None,
        status: str | None = None,
        assigned_admin_user_id: str | None = None,
        include_messages: bool = False,
    ) -> list[dict[str, Any]]:
        where_sql, params = _build_conversation_filters(
            query=query,
            status=status,
            assigned_admin_user_id=assigned_admin_user_id,
        )
        rows = self._fetch_all(
            f"""
            SELECT
                c.*,
                au.name AS assigned_admin_user_name,
                (
                    SELECT body
                    FROM messages
                    WHERE conversation_id = c.id
                    ORDER BY created_at DESC
                    LIMIT 1
                ) AS last_message_preview
            FROM conversations c
            LEFT JOIN admin_users au ON au.id = c.assigned_admin_user_id
            {where_sql}
            ORDER BY c.updated_at DESC
            LIMIT ? OFFSET ?
            """,
            (*params, max(1, min(int(limit), 100)), max(0, int(offset))),
        )
        conversations: list[dict[str, Any]] = []
        for row in rows:
            conversations.append(
                {
                    **row,
                    "messages": self.list_messages(row["id"]) if include_messages else [],
                }
            )
        return conversations

    def count_conversations(
        self,
        *,
        query: str | None = None,
        status: str | None = None,
        assigned_admin_user_id: str | None = None,
    ) -> int:
        where_sql, params = _build_conversation_filters(
            query=query,
            status=status,
            assigned_admin_user_id=assigned_admin_user_id,
        )
        row = self._fetch_one(
            f"""
            SELECT COUNT(*) AS count
            FROM conversations c
            LEFT JOIN admin_users au ON au.id = c.assigned_admin_user_id
            {where_sql}
            """,
            params,
        )
        return int((row or {}).get("count") or 0)

    def list_messages(self, conversation_id: str) -> list[dict[str, Any]]:
        rows = self._fetch_all(
            """
            SELECT *
            FROM messages
            WHERE conversation_id = ?
            ORDER BY created_at ASC
            """,
            (conversation_id,),
        )
        messages: list[dict[str, Any]] = []
        for row in rows:
            messages.append(
                {
                    **row,
                    "source_refs": _decode_json_object(row.get("source_refs_json"), default=[]),
                }
            )
        return messages

    def create_message(
        self,
        conversation_id: str,
        *,
        sender_type: str,
        body: str,
        source_refs: list[dict[str, Any]] | None = None,
        sender_name: str | None = None,
        admin_user_id: str | None = None,
    ) -> dict[str, Any]:
        message_id = str(uuid4())
        created_at = _utc_now()
        normalized_body = body.strip()
        normalized_source_refs = source_refs or []
        with self._managed_connection() as connection:
            connection.execute(
                """
                INSERT INTO messages (
                    id, conversation_id, sender_type, sender_name, admin_user_id, body, source_refs_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id,
                    conversation_id,
                    sender_type,
                    _nullable_strip(sender_name),
                    _nullable_strip(admin_user_id),
                    normalized_body,
                    json.dumps(normalized_source_refs, ensure_ascii=False),
                    created_at,
                ),
            )
            connection.execute(
                """
                UPDATE conversations
                SET updated_at = ?
                WHERE id = ?
                """,
                (created_at, conversation_id),
            )
        return {
            "id": message_id,
            "conversation_id": conversation_id,
            "sender_type": sender_type,
            "sender_name": _nullable_strip(sender_name),
            "admin_user_id": _nullable_strip(admin_user_id),
            "body": normalized_body,
            "source_refs": normalized_source_refs,
            "created_at": created_at,
        }

    def update_conversation_state(self, conversation_id: str, **fields: Any) -> dict[str, Any] | None:
        allowed_fields = {
            "status",
            "workflow_key",
            "workflow_state_json",
            "assigned_admin_user_id",
            "handoff_requested_at",
            "handoff_started_at",
            "closed_at",
        }
        updates = {
            key: value
            for key, value in fields.items()
            if key in allowed_fields
        }
        if not updates:
            return self.get_conversation_by_id(conversation_id)

        updates["updated_at"] = _utc_now()
        assignments = ", ".join(f"{key} = ?" for key in updates)
        params = tuple(updates.values()) + (conversation_id,)
        with self._managed_connection() as connection:
            connection.execute(
                f"UPDATE conversations SET {assignments} WHERE id = ?",
                params,
            )
        return self.get_conversation_by_id(conversation_id)

    def request_handoff(self, conversation_id: str) -> dict[str, Any] | None:
        now = _utc_now()
        with self._managed_connection() as connection:
            connection.execute(
                """
                UPDATE conversations
                SET status = CASE
                        WHEN status = 'handoff_live' THEN 'handoff_live'
                        ELSE 'handoff_pending'
                    END,
                    assigned_admin_user_id = CASE
                        WHEN status = 'handoff_live' THEN assigned_admin_user_id
                        ELSE NULL
                    END,
                    handoff_requested_at = COALESCE(handoff_requested_at, ?),
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, conversation_id),
            )
        return self.get_conversation_by_id(conversation_id)

    def claim_conversation(self, conversation_id: str, admin_user_id: str) -> dict[str, Any] | None:
        now = _utc_now()
        with self._managed_connection() as connection:
            cursor = connection.execute(
                """
                UPDATE conversations
                SET status = 'handoff_live',
                    assigned_admin_user_id = ?,
                    handoff_started_at = COALESCE(handoff_started_at, ?),
                    updated_at = ?
                WHERE id = ?
                  AND status = 'handoff_pending'
                  AND assigned_admin_user_id IS NULL
                """,
                (admin_user_id, now, now, conversation_id),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_conversation_by_id(conversation_id)

    def release_conversation(self, conversation_id: str, admin_user_id: str) -> dict[str, Any] | None:
        now = _utc_now()
        with self._managed_connection() as connection:
            cursor = connection.execute(
                """
                UPDATE conversations
                SET status = 'handoff_pending',
                    assigned_admin_user_id = NULL,
                    updated_at = ?
                WHERE id = ?
                  AND status = 'handoff_live'
                  AND assigned_admin_user_id = ?
                """,
                (now, conversation_id, admin_user_id),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_conversation_by_id(conversation_id)

    def close_conversation(self, conversation_id: str, admin_user_id: str) -> dict[str, Any] | None:
        now = _utc_now()
        with self._managed_connection() as connection:
            cursor = connection.execute(
                """
                UPDATE conversations
                SET status = 'closed',
                    closed_at = ?,
                    updated_at = ?
                WHERE id = ?
                  AND status = 'handoff_live'
                  AND assigned_admin_user_id = ?
                """,
                (now, now, conversation_id, admin_user_id),
            )
            if cursor.rowcount != 1:
                return None
        return self.get_conversation_by_id(conversation_id)

    def end_widget_session(self, conversation_id: str) -> dict[str, Any] | None:
        now = _utc_now()
        with self._managed_connection() as connection:
            connection.execute(
                """
                UPDATE conversations
                SET status = 'closed',
                    closed_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, now, conversation_id),
            )
        return self.get_conversation_by_id(conversation_id)

    def replace_knowledge_documents(
        self,
        *,
        source_type: str,
        documents: list[KnowledgeDocument],
        started_at: str,
        finished_at: str,
    ) -> dict[str, Any]:
        with self._managed_connection() as connection:
            # active source는 하나만 유지하므로 sync 성공 시점에 문서 테이블과 FTS 인덱스를 통째로 교체한다.
            connection.execute("DELETE FROM knowledge_documents")
            connection.execute("DELETE FROM knowledge_documents_fts")

            synced_at = finished_at
            for document in documents:
                connection.execute(
                    """
                    INSERT INTO knowledge_documents (
                        id, title, content, source_type, source_uri, metadata_json, synced_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        document.id,
                        document.title,
                        document.content,
                        document.source_type,
                        document.source_uri,
                        json.dumps(document.metadata, ensure_ascii=False),
                        synced_at,
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO knowledge_documents_fts (document_id, title, content)
                    VALUES (?, ?, ?)
                    """,
                    (
                        document.id,
                        document.title,
                        document.content,
                    ),
                )

            cursor = connection.execute(
                """
                INSERT INTO knowledge_sync_runs (
                    source_type, status, document_count, error_message, started_at, finished_at
                )
                VALUES (?, 'success', ?, NULL, ?, ?)
                """,
                (source_type, len(documents), started_at, finished_at),
            )

        return {
            "id": int(cursor.lastrowid),
            "source_type": source_type,
            "status": "success",
            "document_count": len(documents),
            "error_message": None,
            "started_at": started_at,
            "finished_at": finished_at,
        }

    def record_failed_sync(
        self,
        *,
        source_type: str,
        error_message: str,
        started_at: str,
        finished_at: str,
    ) -> dict[str, Any]:
        with self._managed_connection() as connection:
            cursor = connection.execute(
                """
                INSERT INTO knowledge_sync_runs (
                    source_type, status, document_count, error_message, started_at, finished_at
                )
                VALUES (?, 'failed', 0, ?, ?, ?)
                """,
                (source_type, error_message, started_at, finished_at),
            )
        return {
            "id": int(cursor.lastrowid),
            "source_type": source_type,
            "status": "failed",
            "document_count": 0,
            "error_message": error_message,
            "started_at": started_at,
            "finished_at": finished_at,
        }

    def get_latest_sync_run(self) -> dict[str, Any] | None:
        return self._fetch_one(
            """
            SELECT *
            FROM knowledge_sync_runs
            ORDER BY id DESC
            LIMIT 1
            """
        )

    def list_knowledge_documents(self) -> list[dict[str, Any]]:
        return self._fetch_all(
            """
            SELECT *
            FROM knowledge_documents
            ORDER BY title ASC, id ASC
            """
        )

    def get_knowledge_document(self, document_id: str) -> dict[str, Any] | None:
        return self._fetch_one(
            "SELECT * FROM knowledge_documents WHERE id = ?",
            (document_id,),
        )

    def knowledge_document_count(self) -> int:
        row = self._fetch_one("SELECT COUNT(*) AS count FROM knowledge_documents")
        return int((row or {}).get("count") or 0)

    def search_knowledge_documents(self, query: str, *, limit: int = 5) -> list[KnowledgeSearchResult]:
        tokens = _tokenize_search_text(query)
        if not tokens:
            return []

        rows = self._search_knowledge_candidate_rows(query=query, tokens=tokens, limit=limit)
        if not rows:
            return []

        results: list[KnowledgeSearchResult] = []
        for row in rows:
            document = KnowledgeDocument(
                id=row["id"],
                title=row["title"],
                content=row["content"],
                source_type=row["source_type"],
                source_uri=row["source_uri"],
                metadata=_decode_json_object(row.get("metadata_json"), default={}),
            )
            score = _score_knowledge_document(document, query=query, tokens=tokens)
            if score <= 0:
                continue
            results.append(
                KnowledgeSearchResult(
                    document=document,
                    score=score,
                )
            )
        results.sort(key=lambda item: (-item.score, item.document.title.lower(), item.document.id))
        return results[: max(1, limit)]

    def _search_knowledge_candidate_rows(
        self,
        *,
        query: str,
        tokens: list[str],
        limit: int,
    ) -> list[dict[str, Any]]:
        rows_by_id: dict[str, dict[str, Any]] = {}
        normalized_query = _normalize_search_text(query)
        ascii_tokens = [token for token in tokens if re.search(r"[a-z0-9_]", token)]

        if ascii_tokens:
            match_query = " OR ".join(ascii_tokens)
            try:
                # 영문 질의는 FTS로 먼저 좁히고, 한글 질의는 아래 LIKE 후보를 합쳐서 점수화한다.
                fts_rows = self._fetch_all(
                    """
                    SELECT kd.*
                    FROM knowledge_documents_fts fts
                    JOIN knowledge_documents kd ON kd.id = fts.document_id
                    WHERE knowledge_documents_fts MATCH ?
                    LIMIT ?
                    """,
                    (match_query, max(5, limit * 5)),
                )
                for row in fts_rows:
                    rows_by_id[str(row["id"])] = row
            except sqlite3.OperationalError:
                pass

        like_clauses: list[str] = []
        like_params: list[Any] = []
        if normalized_query:
            like_clauses.extend(["lower(title) LIKE ?", "lower(content) LIKE ?"])
            like_params.extend([f"%{normalized_query}%", f"%{normalized_query}%"])
        for token in tokens:
            like_clauses.extend(["lower(title) LIKE ?", "lower(content) LIKE ?"])
            like_params.extend([f"%{token}%", f"%{token}%"])

        if like_clauses:
            # 한국어는 SQLite FTS tokenizer만 믿기 어렵기 때문에 토큰 LIKE 후보를 같이 모은다.
            like_rows = self._fetch_all(
                f"""
                SELECT *
                FROM knowledge_documents
                WHERE {" OR ".join(like_clauses)}
                LIMIT ?
                """,
                (*like_params, max(20, limit * 10)),
            )
            for row in like_rows:
                rows_by_id[str(row["id"])] = row

        return list(rows_by_id.values())

    def _migrate_schema(self, connection: sqlite3.Connection) -> None:
        # 공개 alpha 배포 후에도 schema 변경 이력을 추적할 수 있게 간단한 version table을 둔다.
        self._ensure_columns(
            connection,
            "conversations",
            {
                "status": "TEXT NOT NULL DEFAULT 'starter'",
                "workflow_key": "TEXT",
                "workflow_state_json": "TEXT NOT NULL DEFAULT '{}'",
                "assigned_admin_user_id": "TEXT",
                "handoff_requested_at": "TEXT",
                "handoff_started_at": "TEXT",
                "closed_at": "TEXT",
            },
        )
        self._ensure_columns(
            connection,
            "messages",
            {
                "sender_name": "TEXT",
                "admin_user_id": "TEXT",
            },
        )
        connection.executescript(
            """
            CREATE INDEX IF NOT EXISTS idx_conversations_updated_at
                ON conversations(updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_conversations_status_updated_at
                ON conversations(status, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_messages_conversation_created_at
                ON messages(conversation_id, created_at ASC);
            """
        )
        self._record_migration(connection, version=1, name="baseline_web_chat_schema")

    def _record_migration(self, connection: sqlite3.Connection, *, version: int, name: str) -> None:
        connection.execute(
            """
            INSERT OR IGNORE INTO schema_migrations (version, name, applied_at)
            VALUES (?, ?, ?)
            """,
            (version, name, _utc_now()),
        )

    def _ensure_columns(
        self,
        connection: sqlite3.Connection,
        table_name: str,
        columns: dict[str, str],
    ) -> None:
        existing_columns = {
            str(row["name"])
            for row in connection.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        for column_name, column_definition in columns.items():
            if column_name in existing_columns:
                continue
            connection.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_definition}"
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._db_path, timeout=5, check_same_thread=False)
        connection.row_factory = sqlite3.Row
        return connection

    @contextmanager
    def _managed_connection(self):
        # 읽기/쓰기 공통 경로를 하나로 묶어서 commit/close 누락으로 인한 잠금 문제를 막는다.
        connection = self._connect()
        try:
            yield connection
            connection.commit()
        finally:
            connection.close()

    def _fetch_one(self, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any] | None:
        with self._managed_connection() as connection:
            row = connection.execute(sql, params).fetchone()
        if row is None:
            return None
        return dict(row)

    def _fetch_all(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        with self._managed_connection() as connection:
            rows = connection.execute(sql, params).fetchall()
        return [dict(row) for row in rows]


def _decode_json_object(raw_value: Any, *, default: Any) -> Any:
    if not raw_value:
        return default
    try:
        return json.loads(str(raw_value))
    except json.JSONDecodeError:
        return default


def _build_conversation_filters(
    *,
    query: str | None,
    status: str | None,
    assigned_admin_user_id: str | None = None,
) -> tuple[str, tuple[Any, ...]]:
    clauses: list[str] = []
    params: list[Any] = []

    normalized_status = str(status or "").strip()
    if normalized_status:
        if normalized_status == "handoff":
            clauses.append("c.status IN ('handoff_pending', 'handoff_live')")
        elif normalized_status == "open":
            clauses.append("c.status != 'closed'")
        else:
            clauses.append("c.status = ?")
            params.append(normalized_status)

    normalized_assignee = str(assigned_admin_user_id or "").strip()
    if normalized_assignee:
        clauses.append("c.assigned_admin_user_id = ?")
        params.append(normalized_assignee)

    normalized_query = str(query or "").strip().lower()
    if normalized_query:
        like_query = f"%{normalized_query}%"
        # 목록 검색은 고객 식별자와 최근 본문을 함께 보되, 실제 메시지 payload는 상세 조회에서만 싣는다.
        clauses.append(
            """
            (
                lower(c.session_id) LIKE ?
                OR lower(COALESCE(c.customer_id, '')) LIKE ?
                OR lower(COALESCE(c.customer_name, '')) LIKE ?
                OR lower(COALESCE(c.customer_email, '')) LIKE ?
                OR lower(COALESCE(au.name, '')) LIKE ?
                OR EXISTS (
                    SELECT 1
                    FROM messages m
                    WHERE m.conversation_id = c.id
                      AND lower(m.body) LIKE ?
                )
            )
            """
        )
        params.extend([like_query, like_query, like_query, like_query, like_query, like_query])

    if not clauses:
        return "", tuple(params)
    return "WHERE " + " AND ".join(clauses), tuple(params)


def _merge_context(raw_existing_context: Any, incoming_context: dict[str, Any]) -> dict[str, Any]:
    existing_context = _decode_json_object(raw_existing_context, default={})
    merged = dict(existing_context)
    merged.update(incoming_context)
    return merged


def _nullable_strip(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _tokenize_search_text(text: str) -> list[str]:
    seen: set[str] = set()
    tokens: list[str] = []
    for token in _TOKEN_PATTERN.findall(_normalize_search_text(text)):
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def _normalize_search_text(text: str) -> str:
    return str(text or "").strip().lower()


def _score_knowledge_document(document: KnowledgeDocument, *, query: str, tokens: list[str]) -> float:
    query_tokens = set(tokens)
    if not query_tokens:
        return 0.0

    normalized_query = _normalize_search_text(query)
    normalized_title = _normalize_search_text(document.title)
    normalized_haystack = _normalize_search_text(f"{document.title}\n{document.content}")
    haystack_tokens = set(_tokenize_search_text(normalized_haystack))
    matched_tokens = query_tokens & haystack_tokens
    matched_substrings = {token for token in query_tokens if token in normalized_haystack}
    if not matched_tokens and not matched_substrings:
        return 0.0

    # alpha 단계에서는 BM25보다 재현 가능한 overlap 점수가 문서 후보를 안정적으로 고르기 쉽다.
    matched = matched_tokens | matched_substrings
    coverage_score = len(matched) / max(1, len(query_tokens))
    density_score = len(matched) / max(1.0, len(haystack_tokens) ** 0.5)
    phrase_bonus = 0.25 if normalized_query and normalized_query in normalized_haystack else 0.0
    title_bonus = 0.15 if any(token in normalized_title for token in query_tokens) else 0.0
    return round(coverage_score + density_score + phrase_bonus + title_bonus, 4)
