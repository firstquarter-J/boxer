from __future__ import annotations

import base64
import hashlib
import io
import json
import re
import secrets
import sqlite3
import threading
import zipfile
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


_TASK_ID_PATTERN = re.compile(r"^hpa-[0-9]{14}-[a-f0-9]{8}-[a-f0-9]{8}$")
_SLACK_EVENT_TS_PATTERN = re.compile(r"^[0-9]{1,20}\.[0-9]{1,20}$")
_GITHUB_SLUG_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
_GITHUB_WORKFLOW_PATTERN = re.compile(r"^(?:[0-9]+|[A-Za-z0-9_.-]+\.ya?ml)$")
_GITHUB_EVENT_TYPE_PATTERN = re.compile(r"^[A-Za-z0-9_.-]{1,100}$")
_GITHUB_TOKEN_PATTERN = re.compile(
    r"\b(?:github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,})\b",
    re.IGNORECASE,
)
_SLACK_TOKEN_PATTERN = re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{10,}\b", re.IGNORECASE)
_ANTHROPIC_TOKEN_PATTERN = re.compile(r"\bsk-ant-[A-Za-z0-9_-]{10,}\b", re.IGNORECASE)
_AWS_ACCESS_KEY_PATTERN = re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b")
_BEARER_TOKEN_PATTERN = re.compile(
    r"(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]{8,}",
)
_PRIVATE_KEY_PATTERN = re.compile(
    r"-----BEGIN(?: RSA)? PRIVATE KEY-----.*?-----END(?: RSA)? PRIVATE KEY-----",
    re.IGNORECASE | re.DOTALL,
)
_SENSITIVE_ASSIGNMENT_PATTERN = re.compile(
    r"(?im)(\b(?:authorization|api[_-]?key|access[_-]?token|auth[_-]?token|"
    r"github[_-]?token|slack[_-]?(?:bot[_-]?)?token|password|passwd|secret|"
    r"private[_-]?key|aws[_-]?(?:secret[_-]?access[_-]?key|session[_-]?token))\b"
    r"\s*(?:=|:)\s*)([^\s,;]+)",
)
_SENSITIVE_KEY_PATTERN = re.compile(
    r"(?:authorization|api[_-]?key|access[_-]?token|auth[_-]?token|github[_-]?token|"
    r"slack[_-]?(?:bot[_-]?)?token|password|passwd|secret|private[_-]?key|"
    r"aws[_-]?(?:secret[_-]?access[_-]?key|session[_-]?token))",
    re.IGNORECASE,
)
_RESULT_PR_URL_PATTERN = re.compile(r"^https://github\.com/[^/]+/[^/]+/pull/[0-9]+/?$")
_UNSET = object()


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _coerce_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _datetime_to_text(value: datetime) -> str:
    return _coerce_utc(value).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _datetime_from_text(value: str) -> datetime:
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _redact_known_secret_literals(value: str) -> str:
    text = str(value or "")
    text = _PRIVATE_KEY_PATTERN.sub("[REDACTED PRIVATE KEY]", text)
    text = _GITHUB_TOKEN_PATTERN.sub("[REDACTED GITHUB TOKEN]", text)
    text = _SLACK_TOKEN_PATTERN.sub("[REDACTED SLACK TOKEN]", text)
    text = _ANTHROPIC_TOKEN_PATTERN.sub("[REDACTED ANTHROPIC TOKEN]", text)
    text = _AWS_ACCESS_KEY_PATTERN.sub("[REDACTED AWS ACCESS KEY]", text)
    return _BEARER_TOKEN_PATTERN.sub(r"\1 [REDACTED]", text)


def redact_sensitive_text(value: str) -> str:
    """저장·로그·Slack 응답 전에 흔한 자격증명 형태를 값만 가린다."""

    text = _redact_known_secret_literals(value)
    return _SENSITIVE_ASSIGNMENT_PATTERN.sub(r"\1[REDACTED]", text)


def redact_sensitive_data(value: Any) -> Any:
    """중첩 JSON에서도 민감 키의 값과 문자열 안 토큰을 함께 제거한다."""

    if isinstance(value, Mapping):
        redacted: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            redacted[key] = (
                "[REDACTED]"
                if _SENSITIVE_KEY_PATTERN.search(key)
                else redact_sensitive_data(item)
            )
        return redacted
    if isinstance(value, (list, tuple, set)):
        return [redact_sensitive_data(item) for item in value]
    if isinstance(value, str):
        return redact_sensitive_text(value)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return redact_sensitive_text(str(value))


def generate_hpa_change_task_id(
    event_ts: str,
    *,
    now: datetime | None = None,
    entropy: str | None = None,
) -> str:
    """Slack event와 임의 entropy를 섞어 추적 가능하지만 추측하기 어려운 task id를 만든다."""

    normalized_event_ts = str(event_ts or "").strip()
    if not _SLACK_EVENT_TS_PATTERN.fullmatch(normalized_event_ts):
        raise ValueError("Slack event_ts 형식이 올바르지 않아")
    actual_now = _coerce_utc(now or _utc_now())
    event_digest = hashlib.sha256(normalized_event_ts.encode("utf-8")).hexdigest()[:8]
    random_part = str(entropy or secrets.token_hex(4)).strip().lower()
    if not re.fullmatch(r"[a-f0-9]{8}", random_part):
        raise ValueError("task id entropy는 8자리 16진수여야 해")
    return f"hpa-{actual_now.strftime('%Y%m%d%H%M%S')}-{event_digest}-{random_part}"


class HpaChangeWorkflowError(RuntimeError):
    pass


class InvalidHpaChangeTransition(HpaChangeWorkflowError):
    pass


class GitHubApiError(HpaChangeWorkflowError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        self.status_code = status_code
        super().__init__(redact_sensitive_text(message))


class GitHubArtifactError(HpaChangeWorkflowError):
    pass


class GitHubArtifactNotReady(GitHubArtifactError):
    pass


class HpaChangeStatus(str, Enum):
    RECEIVED = "received"
    DISPATCHING = "dispatching"
    DISPATCHED = "dispatched"
    RUNNING = "running"
    WORKFLOW_SUCCEEDED = "workflow_succeeded"
    RESULT_READY = "result_ready"
    REVIEW_READY = "review_ready"
    REVIEW_POSTED = "review_posted"
    NEEDS_CLARIFICATION = "needs_clarification"
    PR_CREATED = "pr_created"
    FAILED = "failed"
    CANCELED = "canceled"


_ALLOWED_STATUS_TRANSITIONS: dict[HpaChangeStatus, frozenset[HpaChangeStatus]] = {
    HpaChangeStatus.RECEIVED: frozenset(
        {HpaChangeStatus.DISPATCHING, HpaChangeStatus.FAILED, HpaChangeStatus.CANCELED}
    ),
    HpaChangeStatus.DISPATCHING: frozenset(
        {
            HpaChangeStatus.DISPATCHED,
            HpaChangeStatus.RUNNING,
            HpaChangeStatus.WORKFLOW_SUCCEEDED,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }
    ),
    HpaChangeStatus.DISPATCHED: frozenset(
        {
            HpaChangeStatus.RUNNING,
            HpaChangeStatus.WORKFLOW_SUCCEEDED,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }
    ),
    HpaChangeStatus.RUNNING: frozenset(
        {
            HpaChangeStatus.WORKFLOW_SUCCEEDED,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }
    ),
    HpaChangeStatus.WORKFLOW_SUCCEEDED: frozenset(
        {
            HpaChangeStatus.RESULT_READY,
            HpaChangeStatus.REVIEW_READY,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }
    ),
    HpaChangeStatus.RESULT_READY: frozenset(
        {
            HpaChangeStatus.REVIEW_READY,
            HpaChangeStatus.NEEDS_CLARIFICATION,
            HpaChangeStatus.PR_CREATED,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }
    ),
    HpaChangeStatus.REVIEW_READY: frozenset(
        {
            HpaChangeStatus.REVIEW_POSTED,
            HpaChangeStatus.NEEDS_CLARIFICATION,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }
    ),
    HpaChangeStatus.REVIEW_POSTED: frozenset(
        {
            HpaChangeStatus.DISPATCHING,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }
    ),
    HpaChangeStatus.NEEDS_CLARIFICATION: frozenset(
        {HpaChangeStatus.DISPATCHING, HpaChangeStatus.FAILED, HpaChangeStatus.CANCELED}
    ),
    HpaChangeStatus.PR_CREATED: frozenset(),
    HpaChangeStatus.FAILED: frozenset(
        {HpaChangeStatus.DISPATCHING, HpaChangeStatus.CANCELED}
    ),
    HpaChangeStatus.CANCELED: frozenset(),
}


@dataclass(frozen=True, repr=False)
class HpaChangeJob:
    task_id: str
    workspace_id: str
    event_ts: str
    channel_id: str
    thread_ts: str
    requested_by: str
    request_text: str
    thread_url: str
    attachments: tuple[HpaChangeAttachment, ...]
    status: HpaChangeStatus
    metadata: dict[str, Any]
    workflow_phase: str
    phase_started_at: datetime
    workflow_run_id: int | None
    workflow_run_url: str
    artifact_id: int | None
    artifact_name: str
    result: dict[str, Any]
    pr_urls: tuple[str, ...]
    status_message: str
    error_message: str
    notified_status: str
    dispatch_count: int
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class HpaChangeJobRegistration:
    job: HpaChangeJob
    created: bool


@dataclass(frozen=True, repr=False)
class HpaChangeAttachment:
    name: str
    content: str
    sha256: str


@dataclass(frozen=True, repr=False)
class HpaChangeRequest:
    workspace_id: str
    event_ts: str
    channel_id: str
    thread_ts: str
    requested_by: str
    request_text: str
    thread_url: str = ""
    attachments: tuple[HpaChangeAttachment, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


class HpaChangePollState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    REVIEW_READY = "review_ready"
    NEEDS_CLARIFICATION = "needs_clarification"
    PR_OPENED = "pr_opened"
    FAILED = "failed"


@dataclass(frozen=True, repr=False)
class HpaChangePollResult:
    task_id: str
    state: HpaChangePollState
    job: HpaChangeJob
    run_url: str
    result: dict[str, Any]
    message: str
    pr_urls: tuple[str, ...]


TaskIdFactory = Callable[[str, datetime], str]


class HpaChangeJobStore:
    """Slack 이벤트 하나를 하나의 영속 작업으로 보존하는 SQLite 저장소다."""

    def __init__(
        self,
        db_path: str | Path,
        *,
        timeout_sec: int = 5,
        clock: Callable[[], datetime] = _utc_now,
        task_id_factory: TaskIdFactory | None = None,
    ) -> None:
        raw_db_path = str(db_path)
        self._db_path = (
            raw_db_path
            if raw_db_path == ":memory:"
            else str(Path(raw_db_path).expanduser().resolve())
        )
        self._clock = clock
        self._task_id_factory = task_id_factory or (
            lambda event_ts, now: generate_hpa_change_task_id(event_ts, now=now)
        )
        self._lock = threading.RLock()
        if self._db_path != ":memory:":
            Path(self._db_path).parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(
            self._db_path,
            timeout=max(1, int(timeout_sec)),
            isolation_level=None,
            check_same_thread=False,
        )
        self._connection.row_factory = sqlite3.Row
        self._connection.execute(f"PRAGMA busy_timeout = {max(1000, int(timeout_sec) * 1000)}")
        self._connection.execute("PRAGMA foreign_keys = ON")
        if self._db_path != ":memory:":
            self._connection.execute("PRAGMA journal_mode = WAL")
        self._connection.execute("PRAGMA synchronous = NORMAL")
        self._initialize_schema()

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None  # type: ignore[assignment]

    def __enter__(self) -> HpaChangeJobStore:
        return self

    def __exit__(self, _exc_type, _exc, _traceback) -> None:
        self.close()

    def _initialize_schema(self) -> None:
        # workspace/event 복합 UNIQUE가 여러 Slack workspace의 같은 timestamp는 허용하면서 재전송만 막는다.
        schema = """
        CREATE TABLE IF NOT EXISTS hpa_change_jobs (
            task_id TEXT PRIMARY KEY,
            workspace_id TEXT NOT NULL,
            event_ts TEXT NOT NULL,
            channel_id TEXT NOT NULL,
            thread_ts TEXT NOT NULL,
            requested_by TEXT NOT NULL,
            request_text TEXT NOT NULL,
            thread_url TEXT NOT NULL,
            attachments_json TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            workflow_phase TEXT NOT NULL DEFAULT '',
            phase_started_at TEXT NOT NULL DEFAULT '',
            workflow_run_id INTEGER,
            workflow_run_url TEXT NOT NULL DEFAULT '',
            artifact_id INTEGER,
            artifact_name TEXT NOT NULL DEFAULT '',
            result_json TEXT NOT NULL DEFAULT '{}',
            pr_urls_json TEXT NOT NULL DEFAULT '[]',
            status_message TEXT NOT NULL DEFAULT '',
            error_message TEXT NOT NULL DEFAULT '',
            notified_status TEXT NOT NULL DEFAULT '',
            dispatch_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            UNIQUE(workspace_id, event_ts)
        );
        CREATE INDEX IF NOT EXISTS idx_hpa_change_jobs_status_updated_at
        ON hpa_change_jobs(status, updated_at);
        """
        with self._lock:
            self._connection.executescript(schema)
            # 기존 운영 DB도 재시작만으로 phase 추적 필드를 추가한다.
            columns = {
                str(row["name"])
                for row in self._connection.execute(
                    "PRAGMA table_info(hpa_change_jobs)"
                ).fetchall()
            }
            if "workflow_phase" not in columns:
                self._connection.execute(
                    "ALTER TABLE hpa_change_jobs ADD COLUMN workflow_phase TEXT NOT NULL DEFAULT ''"
                )
            if "phase_started_at" not in columns:
                self._connection.execute(
                    "ALTER TABLE hpa_change_jobs ADD COLUMN phase_started_at TEXT NOT NULL DEFAULT ''"
                )

    def register_job(
        self,
        *,
        workspace_id: str,
        event_ts: str,
        channel_id: str,
        thread_ts: str,
        requested_by: str,
        request_text: str,
        thread_url: str,
        attachments: Sequence[HpaChangeAttachment | Mapping[str, Any]] = (),
        metadata: Mapping[str, Any] | None = None,
    ) -> HpaChangeJobRegistration:
        normalized_event_ts = str(event_ts or "").strip()
        if not _SLACK_EVENT_TS_PATTERN.fullmatch(normalized_event_ts):
            raise ValueError("Slack event_ts 형식이 올바르지 않아")
        required_values = {
            "workspace_id": str(workspace_id or "").strip(),
            "channel_id": str(channel_id or "").strip(),
            "thread_ts": str(thread_ts or "").strip(),
            "requested_by": str(requested_by or "").strip(),
        }
        missing = [name for name, value in required_values.items() if not value]
        if missing:
            raise ValueError(f"HPA 변경 작업 필수값이 없어: {', '.join(missing)}")

        # 요청 코드의 `apiKey = process.env...` 같은 정상 구문은 보존하고 실제 token literal만 제거한다.
        safe_request_text = _redact_known_secret_literals(request_text)
        safe_thread_url = redact_sensitive_text(str(thread_url or "").strip())
        safe_attachments = tuple(self._normalize_attachment(item) for item in attachments)
        safe_metadata = redact_sensitive_data(dict(metadata or {}))
        now = _coerce_utc(self._clock())

        with self._lock:
            existing_row = self._connection.execute(
                "SELECT * FROM hpa_change_jobs WHERE workspace_id = ? AND event_ts = ?",
                (required_values["workspace_id"], normalized_event_ts),
            ).fetchone()
            if existing_row is not None:
                return HpaChangeJobRegistration(job=self._row_to_job(existing_row), created=False)

            for _attempt in range(5):
                task_id = str(self._task_id_factory(normalized_event_ts, now)).strip()
                if not _TASK_ID_PATTERN.fullmatch(task_id):
                    raise ValueError("task_id_factory가 올바르지 않은 task id를 반환했어")
                try:
                    self._connection.execute(
                        """
                        INSERT INTO hpa_change_jobs (
                            task_id, workspace_id, event_ts, channel_id, thread_ts, requested_by,
                            request_text, thread_url, attachments_json, status, metadata_json,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            task_id,
                            required_values["workspace_id"],
                            normalized_event_ts,
                            required_values["channel_id"],
                            required_values["thread_ts"],
                            required_values["requested_by"],
                            safe_request_text,
                            safe_thread_url,
                            json.dumps(
                                [
                                    {
                                        "name": item.name,
                                        "content": item.content,
                                        "sha256": item.sha256,
                                    }
                                    for item in safe_attachments
                                ],
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ),
                            HpaChangeStatus.RECEIVED.value,
                            json.dumps(safe_metadata, ensure_ascii=False, separators=(",", ":")),
                            _datetime_to_text(now),
                            _datetime_to_text(now),
                        ),
                    )
                except sqlite3.IntegrityError:
                    # 다른 프로세스가 같은 event를 먼저 넣은 경우 기존 job을 그대로 돌려준다.
                    existing_row = self._connection.execute(
                        "SELECT * FROM hpa_change_jobs WHERE workspace_id = ? AND event_ts = ?",
                        (required_values["workspace_id"], normalized_event_ts),
                    ).fetchone()
                    if existing_row is not None:
                        return HpaChangeJobRegistration(
                            job=self._row_to_job(existing_row),
                            created=False,
                        )
                    continue
                return HpaChangeJobRegistration(job=self.get_job(task_id), created=True)

        raise HpaChangeWorkflowError("고유한 HPA 변경 task id를 만들지 못했어")

    def get_job(self, task_id: str) -> HpaChangeJob:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM hpa_change_jobs WHERE task_id = ?",
                (str(task_id or "").strip(),),
            ).fetchone()
        if row is None:
            raise KeyError(f"HPA 변경 작업을 찾지 못했어: {task_id}")
        return self._row_to_job(row)

    def get_job_by_event_ts(self, workspace_id: str, event_ts: str) -> HpaChangeJob | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM hpa_change_jobs WHERE workspace_id = ? AND event_ts = ?",
                (str(workspace_id or "").strip(), str(event_ts or "").strip()),
            ).fetchone()
        return self._row_to_job(row) if row is not None else None

    def list_jobs(
        self,
        *,
        statuses: Sequence[HpaChangeStatus] | None = None,
        limit: int = 100,
    ) -> list[HpaChangeJob]:
        actual_limit = max(1, min(500, int(limit)))
        params: list[Any] = []
        where = ""
        if statuses:
            normalized_statuses = [HpaChangeStatus(item).value for item in statuses]
            placeholders = ", ".join("?" for _ in normalized_statuses)
            where = f"WHERE status IN ({placeholders})"
            params.extend(normalized_statuses)
        params.append(actual_limit)
        with self._lock:
            rows = self._connection.execute(
                f"SELECT * FROM hpa_change_jobs {where} ORDER BY updated_at ASC LIMIT ?",
                params,
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def list_reportable_jobs(self, *, limit: int = 500) -> list[HpaChangeJob]:
        """진행 중 작업과 아직 terminal 알림을 보내지 않은 작업만 오래된 순서로 돌려준다."""

        actual_limit = max(1, min(2000, int(limit)))
        active_statuses = (
            HpaChangeStatus.RECEIVED.value,
            HpaChangeStatus.DISPATCHING.value,
            HpaChangeStatus.DISPATCHED.value,
            HpaChangeStatus.RUNNING.value,
            HpaChangeStatus.WORKFLOW_SUCCEEDED.value,
            HpaChangeStatus.RESULT_READY.value,
            HpaChangeStatus.REVIEW_READY.value,
            HpaChangeStatus.REVIEW_POSTED.value,
        )
        placeholders = ", ".join("?" for _ in active_statuses)
        params: list[Any] = [
            *active_statuses,
            HpaChangeStatus.NEEDS_CLARIFICATION.value,
            HpaChangePollState.NEEDS_CLARIFICATION.value,
            HpaChangeStatus.PR_CREATED.value,
            HpaChangePollState.PR_OPENED.value,
            HpaChangeStatus.FAILED.value,
            HpaChangeStatus.CANCELED.value,
            HpaChangePollState.FAILED.value,
            actual_limit,
        ]
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT *
                FROM hpa_change_jobs
                WHERE status IN ({placeholders})
                   OR (status = ? AND notified_status != ?)
                   OR (status = ? AND notified_status != ?)
                   OR (status IN (?, ?) AND notified_status != ?)
                ORDER BY updated_at ASC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def transition(
        self,
        task_id: str,
        new_status: HpaChangeStatus,
        *,
        workflow_run_id: int | None | object = _UNSET,
        workflow_run_url: str | object = _UNSET,
        artifact_id: int | None | object = _UNSET,
        artifact_name: str | object = _UNSET,
        result: Mapping[str, Any] | object = _UNSET,
        pr_urls: Sequence[str] | object = _UNSET,
        status_message: str | object = _UNSET,
        error_message: str | object = _UNSET,
        workflow_phase: str | object = _UNSET,
        reset_phase_started_at: bool = False,
        increment_dispatch_count: bool = False,
        clear_execution: bool = False,
    ) -> HpaChangeJob:
        normalized_task_id = str(task_id or "").strip()
        target_status = HpaChangeStatus(new_status)
        now_text = _datetime_to_text(_coerce_utc(self._clock()))

        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT * FROM hpa_change_jobs WHERE task_id = ?",
                    (normalized_task_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"HPA 변경 작업을 찾지 못했어: {normalized_task_id}")
                current_status = HpaChangeStatus(str(row["status"]))
                if (
                    target_status != current_status
                    and target_status not in _ALLOWED_STATUS_TRANSITIONS[current_status]
                ):
                    raise InvalidHpaChangeTransition(
                        f"허용되지 않은 HPA 변경 상태 전이야: {current_status.value} -> {target_status.value}"
                    )

                updates: dict[str, Any] = {
                    "status": target_status.value,
                    "updated_at": now_text,
                }
                if clear_execution:
                    updates.update(
                        {
                            "workflow_run_id": None,
                            "workflow_run_url": "",
                            "artifact_id": None,
                            "artifact_name": "",
                            "result_json": "{}",
                            "pr_urls_json": "[]",
                            "error_message": "",
                        }
                    )
                if workflow_run_id is not _UNSET:
                    updates["workflow_run_id"] = workflow_run_id
                if workflow_run_url is not _UNSET:
                    updates["workflow_run_url"] = redact_sensitive_text(str(workflow_run_url))
                if artifact_id is not _UNSET:
                    updates["artifact_id"] = artifact_id
                if artifact_name is not _UNSET:
                    updates["artifact_name"] = redact_sensitive_text(str(artifact_name))
                if result is not _UNSET:
                    updates["result_json"] = json.dumps(
                        redact_sensitive_data(dict(result)),
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                if pr_urls is not _UNSET:
                    safe_urls = [redact_sensitive_text(str(item)) for item in pr_urls]
                    updates["pr_urls_json"] = json.dumps(safe_urls, separators=(",", ":"))
                if status_message is not _UNSET:
                    updates["status_message"] = redact_sensitive_text(str(status_message))
                if error_message is not _UNSET:
                    updates["error_message"] = redact_sensitive_text(str(error_message))
                if workflow_phase is not _UNSET:
                    normalized_phase = str(workflow_phase or "").strip().lower()
                    if normalized_phase not in {"", "review", "implementation"}:
                        raise ValueError("HPA workflow phase가 올바르지 않아")
                    updates["workflow_phase"] = normalized_phase
                if reset_phase_started_at:
                    updates["phase_started_at"] = now_text
                if increment_dispatch_count:
                    updates["dispatch_count"] = int(row["dispatch_count"] or 0) + 1

                assignments = ", ".join(f"{name} = ?" for name in updates)
                self._connection.execute(
                    f"UPDATE hpa_change_jobs SET {assignments} WHERE task_id = ?",
                    [*updates.values(), normalized_task_id],
                )
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return self.get_job(normalized_task_id)

    def begin_dispatch(self, task_id: str) -> HpaChangeJob:
        claimed = self.claim_review_dispatch(task_id)
        if claimed is None:
            return self.get_job(task_id)
        return claimed

    def claim_review_dispatch(
        self,
        task_id: str,
        *,
        allow_recovery: bool = False,
        retry_after_sec: int = 60,
    ) -> HpaChangeJob | None:
        """review dispatch도 원자적으로 소유하고 중단된 전송만 재시도한다."""

        normalized_task_id = str(task_id or "").strip()
        now = _coerce_utc(self._clock())
        now_text = _datetime_to_text(now)
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT * FROM hpa_change_jobs WHERE task_id = ?",
                    (normalized_task_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"HPA 변경 작업을 찾지 못했어: {normalized_task_id}")
                current = HpaChangeStatus(str(row["status"]))
                first_claim = current in {
                    HpaChangeStatus.RECEIVED,
                    HpaChangeStatus.NEEDS_CLARIFICATION,
                    HpaChangeStatus.FAILED,
                }
                recoverable = (
                    allow_recovery
                    and str(row["workflow_phase"] or "") == "review"
                    and current in {HpaChangeStatus.DISPATCHING, HpaChangeStatus.DISPATCHED}
                )
                if not first_claim and not recoverable:
                    if allow_recovery:
                        self._connection.execute("COMMIT")
                        return None
                    raise InvalidHpaChangeTransition(
                        f"review dispatch를 시작할 수 없는 상태야: {current.value}"
                    )
                if recoverable:
                    last_attempt = _datetime_from_text(str(row["updated_at"]))
                    if (now - last_attempt).total_seconds() < max(1, int(retry_after_sec)):
                        self._connection.execute("COMMIT")
                        return None

                updates = {
                    "status": HpaChangeStatus.DISPATCHING.value,
                    "workflow_phase": "review",
                    "updated_at": now_text,
                    "status_message": "GitHub coordinator review dispatch 준비 중",
                    "dispatch_count": int(row["dispatch_count"] or 0) + 1,
                }
                if first_claim:
                    updates.update(
                        {
                            "phase_started_at": now_text,
                            "workflow_run_id": None,
                            "workflow_run_url": "",
                            "artifact_id": None,
                            "artifact_name": "",
                            "result_json": "{}",
                            "pr_urls_json": "[]",
                            "error_message": "",
                        }
                    )
                assignments = ", ".join(f"{name} = ?" for name in updates)
                self._connection.execute(
                    f"UPDATE hpa_change_jobs SET {assignments} WHERE task_id = ?",
                    [*updates.values(), normalized_task_id],
                )
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return self.get_job(normalized_task_id)

    def mark_dispatched(self, task_id: str) -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.DISPATCHED,
            status_message="GitHub coordinator에 작업 전달 완료",
        )

    def mark_running(self, task_id: str, run: GitHubWorkflowRun) -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.RUNNING,
            workflow_run_id=run.run_id,
            workflow_run_url=run.html_url,
            status_message=f"GitHub workflow {run.status}",
        )

    def mark_workflow_succeeded(self, task_id: str, run: GitHubWorkflowRun) -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.WORKFLOW_SUCCEEDED,
            workflow_run_id=run.run_id,
            workflow_run_url=run.html_url,
            status_message="GitHub workflow 성공, 결과 artifact 확인 대기",
        )

    def mark_result_ready(
        self,
        task_id: str,
        archive: GitHubArtifactArchive,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.RESULT_READY,
            artifact_id=archive.artifact_id,
            artifact_name=archive.name,
            result=result or {},
            status_message="GitHub workflow 결과 artifact 수집 완료",
        )

    def mark_review_ready(
        self,
        task_id: str,
        archive: GitHubArtifactArchive,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> HpaChangeJob:
        """검토 artifact를 수집했지만 구현은 아직 시작하지 않은 상태로 남긴다."""

        return self.transition(
            task_id,
            HpaChangeStatus.REVIEW_READY,
            artifact_id=archive.artifact_id,
            artifact_name=archive.name,
            result=result or {},
            status_message="검토 결과 게시 전 대기",
        )

    def mark_review_posted(self, task_id: str) -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.REVIEW_POSTED,
            status_message="Slack 검토 결과 게시 완료, 구현 dispatch 준비 중",
        )

    def claim_implementation_dispatch(
        self,
        task_id: str,
        *,
        allow_recovery: bool = False,
        retry_after_sec: int = 60,
    ) -> HpaChangeJob | None:
        """한 poller만 구현 dispatch를 소유하고, 오래된 시도만 재소유하게 한다."""

        normalized_task_id = str(task_id or "").strip()
        now = _coerce_utc(self._clock())
        now_text = _datetime_to_text(now)
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT * FROM hpa_change_jobs WHERE task_id = ?",
                    (normalized_task_id,),
                ).fetchone()
                if row is None:
                    raise KeyError(f"HPA 변경 작업을 찾지 못했어: {normalized_task_id}")
                current = HpaChangeStatus(str(row["status"]))
                first_claim = current is HpaChangeStatus.REVIEW_POSTED
                recoverable = (
                    allow_recovery
                    and str(row["workflow_phase"] or "") == "implementation"
                    and current in {HpaChangeStatus.DISPATCHING, HpaChangeStatus.DISPATCHED}
                )
                if not first_claim and not recoverable:
                    if allow_recovery:
                        self._connection.execute("COMMIT")
                        return None
                    raise InvalidHpaChangeTransition(
                        f"검토 게시 전에는 구현을 시작할 수 없어: {current.value}"
                    )
                if recoverable:
                    last_attempt = _datetime_from_text(str(row["updated_at"]))
                    if (now - last_attempt).total_seconds() < max(1, int(retry_after_sec)):
                        self._connection.execute("COMMIT")
                        return None

                updates = {
                    "status": HpaChangeStatus.DISPATCHING.value,
                    "workflow_phase": "implementation",
                    "updated_at": now_text,
                    "status_message": "검토 게시 후 HPA 구현 dispatch 준비 중",
                    "dispatch_count": int(row["dispatch_count"] or 0) + 1,
                }
                if first_claim:
                    updates["phase_started_at"] = now_text
                assignments = ", ".join(f"{name} = ?" for name in updates)
                self._connection.execute(
                    f"UPDATE hpa_change_jobs SET {assignments} WHERE task_id = ?",
                    [*updates.values(), normalized_task_id],
                )
                self._connection.execute("COMMIT")
            except Exception:
                self._connection.execute("ROLLBACK")
                raise
        return self.get_job(normalized_task_id)

    def mark_needs_clarification(
        self,
        task_id: str,
        message: str,
        *,
        result: Mapping[str, Any] | None = None,
    ) -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.NEEDS_CLARIFICATION,
            result=result or {},
            status_message=message,
        )

    def mark_pr_created(
        self,
        task_id: str,
        pr_urls: Sequence[str],
        *,
        message: str = "HPA 변경 PR 생성 완료",
        result: Mapping[str, Any] | None = None,
    ) -> HpaChangeJob:
        if not pr_urls:
            raise ValueError("PR 생성 완료 상태에는 PR URL이 하나 이상 필요해")
        return self.transition(
            task_id,
            HpaChangeStatus.PR_CREATED,
            pr_urls=pr_urls,
            result=result or {},
            status_message=message,
        )

    def mark_failed(self, task_id: str, error_message: str) -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.FAILED,
            status_message="HPA 변경 자동화 실패",
            error_message=error_message,
        )

    def mark_canceled(self, task_id: str, message: str = "요청자 또는 운영자 취소") -> HpaChangeJob:
        return self.transition(
            task_id,
            HpaChangeStatus.CANCELED,
            status_message=message,
        )

    def mark_notified(self, task_id: str, status: str | HpaChangePollState) -> HpaChangeJob:
        normalized_status = str(
            status.value if isinstance(status, HpaChangePollState) else status
        ).strip()
        if normalized_status not in {item.value for item in HpaChangePollState}:
            raise ValueError("알림 완료 상태가 올바르지 않아")
        now_text = _datetime_to_text(_coerce_utc(self._clock()))
        with self._lock:
            cursor = self._connection.execute(
                """
                UPDATE hpa_change_jobs
                SET notified_status = ?, updated_at = ?
                WHERE task_id = ?
                """,
                (normalized_status, now_text, str(task_id or "").strip()),
            )
            if int(cursor.rowcount or 0) != 1:
                raise KeyError(f"HPA 변경 작업을 찾지 못했어: {task_id}")
        return self.get_job(task_id)

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> HpaChangeJob:
        metadata = json.loads(str(row["metadata_json"] or "{}"))
        raw_attachments = json.loads(str(row["attachments_json"] or "[]"))
        result = json.loads(str(row["result_json"] or "{}"))
        pr_urls = json.loads(str(row["pr_urls_json"] or "[]"))
        return HpaChangeJob(
            task_id=str(row["task_id"]),
            workspace_id=str(row["workspace_id"]),
            event_ts=str(row["event_ts"]),
            channel_id=str(row["channel_id"]),
            thread_ts=str(row["thread_ts"]),
            requested_by=str(row["requested_by"]),
            request_text=str(row["request_text"]),
            thread_url=str(row["thread_url"]),
            attachments=tuple(
                HpaChangeAttachment(
                    name=str(item.get("name") or ""),
                    content=str(item.get("content") or ""),
                    sha256=str(item.get("sha256") or ""),
                )
                for item in raw_attachments
                if isinstance(item, dict)
            ),
            status=HpaChangeStatus(str(row["status"])),
            metadata=metadata if isinstance(metadata, dict) else {},
            workflow_phase=str(row["workflow_phase"] or ""),
            phase_started_at=(
                _datetime_from_text(str(row["phase_started_at"]))
                if str(row["phase_started_at"] or "").strip()
                else _datetime_from_text(str(row["created_at"]))
            ),
            workflow_run_id=(
                int(row["workflow_run_id"]) if row["workflow_run_id"] is not None else None
            ),
            workflow_run_url=str(row["workflow_run_url"] or ""),
            artifact_id=int(row["artifact_id"]) if row["artifact_id"] is not None else None,
            artifact_name=str(row["artifact_name"] or ""),
            result=result if isinstance(result, dict) else {},
            pr_urls=tuple(str(item) for item in pr_urls if str(item).strip()),
            status_message=str(row["status_message"] or ""),
            error_message=str(row["error_message"] or ""),
            notified_status=str(row["notified_status"] or ""),
            dispatch_count=max(0, int(row["dispatch_count"] or 0)),
            created_at=_datetime_from_text(str(row["created_at"])),
            updated_at=_datetime_from_text(str(row["updated_at"])),
        )

    @staticmethod
    def _normalize_attachment(
        raw: HpaChangeAttachment | Mapping[str, Any],
    ) -> HpaChangeAttachment:
        if isinstance(raw, HpaChangeAttachment):
            name = raw.name
            content = raw.content
            declared_sha256 = raw.sha256
        elif isinstance(raw, Mapping):
            name = str(raw.get("name") or "")
            content = str(raw.get("content") or "")
            declared_sha256 = str(raw.get("sha256") or "")
        else:
            raise ValueError("HPA 변경 첨부 형식이 올바르지 않아")
        normalized_name = str(name or "").strip()
        if (
            not normalized_name
            or normalized_name in {".", ".."}
            or Path(normalized_name).name != normalized_name
            or "\\" in normalized_name
        ):
            raise ValueError("HPA 변경 첨부 파일명은 경로 없는 이름이어야 해")
        safe_content = _redact_known_secret_literals(content)
        actual_sha256 = hashlib.sha256(safe_content.encode("utf-8")).hexdigest()
        normalized_declared = str(declared_sha256 or "").strip().lower()
        original_sha256 = hashlib.sha256(str(content).encode("utf-8")).hexdigest()
        if normalized_declared and normalized_declared != original_sha256:
            raise ValueError("HPA 변경 첨부 sha256이 내용과 달라")
        return HpaChangeAttachment(
            name=normalized_name,
            content=safe_content,
            sha256=actual_sha256,
        )


class GitHubTokenProvider(Protocol):
    def get_token(self) -> str: ...


@dataclass(frozen=True, repr=False)
class StaticGitHubTokenProvider:
    token: str = field(repr=False)

    def get_token(self) -> str:
        token = str(self.token or "").strip()
        if not token:
            raise GitHubApiError("GitHub static token이 설정되지 않았어")
        return token


@dataclass(frozen=True)
class GitHubAppPermissions:
    repositories: tuple[str, ...] = ()
    permissions: dict[str, str] = field(default_factory=dict)


class GitHubAppTokenProvider:
    """외부 JWT 패키지나 shell 없이 RS256 App JWT와 installation token을 만든다."""

    def __init__(
        self,
        *,
        app_id: int | str,
        installation_id: int | str,
        private_key_pem: str,
        session: Any | None = None,
        api_base_url: str = "https://api.github.com",
        api_version: str = "2022-11-28",
        timeout_sec: int = 10,
        clock: Callable[[], datetime] = _utc_now,
        restrictions: GitHubAppPermissions | None = None,
    ) -> None:
        self._app_id = str(app_id or "").strip()
        self._installation_id = str(installation_id or "").strip()
        self._private_key_pem = str(private_key_pem or "")
        self._session = session or requests.Session()
        self._api_base_url = str(api_base_url or "").strip().rstrip("/")
        self._api_version = str(api_version or "").strip()
        self._timeout_sec = max(1, int(timeout_sec))
        self._clock = clock
        self._restrictions = restrictions or GitHubAppPermissions()
        self._cached_token = ""
        self._cached_expires_at: datetime | None = None
        self._lock = threading.Lock()

        if not self._app_id.isdigit() or int(self._app_id) <= 0:
            raise ValueError("GitHub App ID가 올바르지 않아")
        if not self._installation_id.isdigit() or int(self._installation_id) <= 0:
            raise ValueError("GitHub App installation ID가 올바르지 않아")
        if not self._private_key_pem.strip():
            raise ValueError("GitHub App private key가 없어")

    def __repr__(self) -> str:
        return (
            "GitHubAppTokenProvider("
            f"app_id={self._app_id!r}, installation_id={self._installation_id!r}, "
            "private_key_pem='[REDACTED]')"
        )

    def get_token(self) -> str:
        now = _coerce_utc(self._clock())
        with self._lock:
            if (
                self._cached_token
                and self._cached_expires_at is not None
                and now < self._cached_expires_at - timedelta(seconds=60)
            ):
                return self._cached_token

            app_jwt = self._build_app_jwt(now)
            url = (
                f"{self._api_base_url}/app/installations/"
                f"{quote(self._installation_id, safe='')}/access_tokens"
            )
            payload: dict[str, Any] = {}
            if self._restrictions.repositories:
                payload["repositories"] = list(self._restrictions.repositories)
            if self._restrictions.permissions:
                payload["permissions"] = dict(self._restrictions.permissions)
            try:
                response = self._session.request(
                    "POST",
                    url,
                    headers={
                        "Accept": "application/vnd.github+json",
                        "Authorization": f"Bearer {app_jwt}",
                        "X-GitHub-Api-Version": self._api_version,
                        "User-Agent": "boxer-hpa-change-workflow",
                    },
                    json=payload or None,
                    timeout=self._timeout_sec,
                )
            except Exception as exc:
                raise GitHubApiError(
                    f"GitHub App installation token 요청 실패: {redact_sensitive_text(str(exc))}"
                ) from exc
            if int(getattr(response, "status_code", 0)) != 201:
                detail = redact_sensitive_text(str(getattr(response, "text", "")))[:500]
                raise GitHubApiError(
                    f"GitHub App installation token 요청 실패 ({getattr(response, 'status_code', 0)}): {detail}",
                    status_code=int(getattr(response, "status_code", 0) or 0),
                )
            try:
                data = response.json()
                token = str(data.get("token") or "").strip()
                expires_at = _datetime_from_text(str(data.get("expires_at") or ""))
            except Exception as exc:
                raise GitHubApiError("GitHub App installation token 응답 형식이 올바르지 않아") from exc
            if not token:
                raise GitHubApiError("GitHub App installation token 응답이 비어 있어")
            self._cached_token = token
            self._cached_expires_at = expires_at
            return token

    def _build_app_jwt(self, now: datetime) -> str:
        header = {"alg": "RS256", "typ": "JWT"}
        issued_at = int(now.timestamp()) - 60
        payload = {
            "iat": issued_at,
            "exp": issued_at + 9 * 60,
            "iss": self._app_id,
        }
        header_part = _base64url(json.dumps(header, separators=(",", ":")).encode("utf-8"))
        payload_part = _base64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        signing_input = f"{header_part}.{payload_part}".encode("ascii")
        try:
            private_key = serialization.load_pem_private_key(
                self._private_key_pem.encode("utf-8"),
                password=None,
            )
        except Exception as exc:
            raise GitHubApiError("GitHub App private key를 읽지 못했어") from exc
        if not isinstance(private_key, rsa.RSAPrivateKey):
            raise GitHubApiError("GitHub App private key는 RSA key여야 해")
        signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
        return f"{header_part}.{payload_part}.{_base64url(signature)}"


@dataclass(frozen=True)
class GitHubCoordinatorConfig:
    owner: str
    repository: str
    workflow_id: str
    event_type: str = "boxer-hpa-change"
    implementation_event_type: str = "boxer-hpa-implement"
    workflow_run_name_prefix: str = "HPA Change Review"
    implementation_workflow_run_name_prefix: str = "HPA Change Implementation"
    result_artifact_name_prefix: str = "boxer-hpa-result"
    result_member_name: str = "result.json"
    api_base_url: str = "https://api.github.com"
    api_version: str = "2022-11-28"
    timeout_sec: int = 15
    max_dispatch_payload_bytes: int = 60_000
    max_artifact_bytes: int = 10 * 1024 * 1024
    max_result_json_bytes: int = 1024 * 1024

    def __post_init__(self) -> None:
        if self.owner in {".", ".."} or not _GITHUB_SLUG_PATTERN.fullmatch(self.owner):
            raise ValueError("GitHub coordinator owner 형식이 올바르지 않아")
        if self.repository in {".", ".."} or not _GITHUB_SLUG_PATTERN.fullmatch(self.repository):
            raise ValueError("GitHub coordinator repository 형식이 올바르지 않아")
        if not _GITHUB_WORKFLOW_PATTERN.fullmatch(self.workflow_id):
            raise ValueError("GitHub coordinator workflow_id 형식이 올바르지 않아")
        if not _GITHUB_EVENT_TYPE_PATTERN.fullmatch(self.event_type):
            raise ValueError("GitHub repository_dispatch event_type 형식이 올바르지 않아")
        if not _GITHUB_EVENT_TYPE_PATTERN.fullmatch(self.implementation_event_type):
            raise ValueError("GitHub implementation event_type 형식이 올바르지 않아")
        if not self.workflow_run_name_prefix.strip():
            raise ValueError("GitHub workflow run-name prefix가 없어")
        if not self.implementation_workflow_run_name_prefix.strip():
            raise ValueError("GitHub implementation run-name prefix가 없어")
        if not _GITHUB_SLUG_PATTERN.fullmatch(self.result_artifact_name_prefix):
            raise ValueError("GitHub result artifact prefix 형식이 올바르지 않아")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+\.json", self.result_member_name):
            raise ValueError("GitHub result artifact JSON member 이름이 올바르지 않아")

    @property
    def repository_path(self) -> str:
        return f"repos/{self.owner}/{self.repository}"

    def expected_run_title(self, task_id: str, *, phase: str = "review") -> str:
        prefix = (
            self.implementation_workflow_run_name_prefix
            if str(phase).strip().lower() == "implementation"
            else self.workflow_run_name_prefix
        )
        return f"{prefix.strip()} - {task_id}"

    def result_artifact_name(self, task_id: str) -> str:
        if not _TASK_ID_PATTERN.fullmatch(str(task_id or "").strip()):
            raise ValueError("result artifact task id 형식이 올바르지 않아")
        return f"{self.result_artifact_name_prefix}-{task_id}"


@dataclass(frozen=True)
class GitHubDispatchReceipt:
    task_id: str
    event_type: str
    dispatched_at: datetime


@dataclass(frozen=True)
class GitHubWorkflowRun:
    run_id: int
    status: str
    conclusion: str
    html_url: str
    display_title: str
    run_attempt: int
    created_at: datetime | None
    updated_at: datetime | None


@dataclass(frozen=True)
class GitHubArtifactArchive:
    artifact_id: int
    workflow_run_id: int
    name: str
    size_in_bytes: int
    sha256: str
    content: bytes = field(repr=False)


class GitHubCoordinatorClient:
    """고정된 coordinator repo/workflow에만 dispatch·조회·artifact 다운로드를 허용한다."""

    def __init__(
        self,
        config: GitHubCoordinatorConfig,
        token_provider: GitHubTokenProvider,
        *,
        session: Any | None = None,
        clock: Callable[[], datetime] = _utc_now,
    ) -> None:
        self.config = config
        self._token_provider = token_provider
        self._session = session or requests.Session()
        self._clock = clock

    def dispatch_job(
        self,
        job: HpaChangeJob,
    ) -> GitHubDispatchReceipt:
        if not _TASK_ID_PATTERN.fullmatch(job.task_id):
            raise ValueError("dispatch할 HPA task id 형식이 올바르지 않아")

        # worker에는 구현 입력만 전달하고 Slack 내부 routing 식별자는 보내지 않는다.
        client_payload: dict[str, Any] = {
            "task_id": job.task_id,
            "request": {
                "text": _redact_known_secret_literals(job.request_text),
                "requester_slack_user_id": job.requested_by,
                "thread_url": job.thread_url,
                "attachments": [
                    {
                        "name": item.name,
                        "content": item.content,
                        "sha256": item.sha256,
                    }
                    for item in job.attachments
                ],
            },
        }
        body = {
            "event_type": self.config.event_type,
            "client_payload": client_payload,
        }
        encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > max(1, self.config.max_dispatch_payload_bytes):
            raise ValueError("GitHub repository_dispatch payload가 허용 크기를 넘었어")

        self._request(
            "POST",
            f"/{self.config.repository_path}/dispatches",
            expected_statuses={204},
            json_body=body,
        )
        return GitHubDispatchReceipt(
            task_id=job.task_id,
            event_type=self.config.event_type,
            dispatched_at=_coerce_utc(self._clock()),
        )

    def dispatch_implementation(
        self,
        job: HpaChangeJob,
        *,
        review_run_id: int,
    ) -> GitHubDispatchReceipt:
        """Slack에 검토 결과를 게시한 뒤에만 구현 workflow를 시작한다."""

        if not _TASK_ID_PATTERN.fullmatch(job.task_id):
            raise ValueError("구현 dispatch할 HPA task id 형식이 올바르지 않아")
        if int(review_run_id) <= 0:
            raise ValueError("구현 dispatch에 필요한 review run id가 없어")

        # 원 요청과 검토 결과는 review run의 검증된 artifact에서만 가져온다.
        # 두 번째 dispatch에는 Slack 원문이나 첨부를 다시 싣지 않는다.
        client_payload: dict[str, Any] = {
            "task_id": job.task_id,
            "review_run_id": int(review_run_id),
        }
        body = {
            "event_type": self.config.implementation_event_type,
            "client_payload": client_payload,
        }
        encoded = json.dumps(body, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > max(1, self.config.max_dispatch_payload_bytes):
            raise ValueError("GitHub implementation dispatch payload가 허용 크기를 넘었어")

        self._request(
            "POST",
            f"/{self.config.repository_path}/dispatches",
            expected_statuses={204},
            json_body=body,
        )
        return GitHubDispatchReceipt(
            task_id=job.task_id,
            event_type=self.config.implementation_event_type,
            dispatched_at=_coerce_utc(self._clock()),
        )

    def find_workflow_run(
        self,
        task_id: str,
        *,
        phase: str = "review",
        created_after: datetime | None = None,
        max_pages: int = 3,
    ) -> GitHubWorkflowRun | None:
        normalized_phase = str(phase or "").strip().lower()
        if normalized_phase not in {"review", "implementation"}:
            raise ValueError("조회할 HPA workflow phase가 올바르지 않아")
        expected_title = self.config.expected_run_title(task_id, phase=normalized_phase)
        candidates: list[GitHubWorkflowRun] = []
        for page in range(1, max(1, min(10, int(max_pages))) + 1):
            data = self._request_json(
                "GET",
                (
                    f"/{self.config.repository_path}/actions/workflows/"
                    f"{quote(self.config.workflow_id, safe='')}/runs"
                ),
                expected_statuses={200},
                params={"event": "repository_dispatch", "per_page": 100, "page": page},
            )
            raw_runs = data.get("workflow_runs") if isinstance(data, dict) else None
            if not isinstance(raw_runs, list):
                raise GitHubApiError("GitHub workflow runs 응답 형식이 올바르지 않아")
            for raw_run in raw_runs:
                if not isinstance(raw_run, dict):
                    continue
                if str(raw_run.get("display_title") or "").strip() != expected_title:
                    continue
                run = self._parse_workflow_run(raw_run)
                if (
                    created_after is not None
                    and run.created_at is not None
                    and run.created_at < _coerce_utc(created_after)
                ):
                    continue
                candidates.append(run)
            if len(raw_runs) < 100:
                break
        if not candidates:
            return None
        key = lambda item: (
            item.created_at or datetime.min.replace(tzinfo=timezone.utc),
            item.run_id,
        )
        # dispatch 응답 유실로 같은 phase가 재전송돼도 최초 run만 정식 실행으로 추적한다.
        return min(candidates, key=key)

    def get_workflow_run(self, run_id: int) -> GitHubWorkflowRun:
        data = self._request_json(
            "GET",
            f"/{self.config.repository_path}/actions/runs/{int(run_id)}",
            expected_statuses={200},
        )
        if not isinstance(data, dict):
            raise GitHubApiError("GitHub workflow run 응답 형식이 올바르지 않아")
        path = str(data.get("path") or "").split("@", 1)[0].strip()
        expected_path = f".github/workflows/{self.config.workflow_id}"
        if path and path != expected_path:
            raise GitHubApiError("조회한 workflow run이 고정 coordinator workflow와 달라")
        return self._parse_workflow_run(data)

    def download_result_artifact_zip(
        self,
        run_id: int,
        task_id: str,
    ) -> GitHubArtifactArchive:
        run = self.get_workflow_run(run_id)
        if run.status != "completed" or run.conclusion != "success":
            raise GitHubArtifactError("성공 완료된 coordinator workflow의 artifact만 받을 수 있어")

        data = self._request_json(
            "GET",
            f"/{self.config.repository_path}/actions/runs/{int(run_id)}/artifacts",
            expected_statuses={200},
            params={"per_page": 100},
        )
        raw_artifacts = data.get("artifacts") if isinstance(data, dict) else None
        if not isinstance(raw_artifacts, list):
            raise GitHubArtifactError("GitHub workflow artifacts 응답 형식이 올바르지 않아")
        expected_artifact_name = self.config.result_artifact_name(task_id)
        matches = [
            item
            for item in raw_artifacts
            if isinstance(item, dict)
            and str(item.get("name") or "") == expected_artifact_name
            and not bool(item.get("expired"))
        ]
        if not matches:
            raise GitHubArtifactNotReady("HPA 변경 결과 artifact를 아직 찾지 못했어")
        selected = max(matches, key=lambda item: int(item.get("id") or 0))
        artifact_id = int(selected.get("id") or 0)
        declared_size = max(0, int(selected.get("size_in_bytes") or 0))
        if artifact_id <= 0:
            raise GitHubArtifactError("GitHub result artifact id가 올바르지 않아")
        if declared_size > self.config.max_artifact_bytes:
            raise GitHubArtifactError("GitHub result artifact가 허용 크기를 넘었어")

        # API가 내려준 임의 archive URL을 쓰지 않고, 고정 repo와 artifact id로 URL을 다시 만든다.
        response = self._request(
            "GET",
            f"/{self.config.repository_path}/actions/artifacts/{artifact_id}/zip",
            expected_statuses={200},
            stream=True,
            allow_redirects=True,
        )
        content_length = int(str(getattr(response, "headers", {}).get("Content-Length") or "0"))
        if content_length > self.config.max_artifact_bytes:
            raise GitHubArtifactError("GitHub result artifact가 허용 크기를 넘었어")
        chunks: list[bytes] = []
        actual_size = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            actual_size += len(chunk)
            if actual_size > self.config.max_artifact_bytes:
                raise GitHubArtifactError("GitHub result artifact가 허용 크기를 넘었어")
            chunks.append(bytes(chunk))
        content = b"".join(chunks)
        if not content or not zipfile.is_zipfile(io.BytesIO(content)):
            raise GitHubArtifactError("GitHub result artifact가 올바른 ZIP 파일이 아니야")
        return GitHubArtifactArchive(
            artifact_id=artifact_id,
            workflow_run_id=int(run_id),
            name=expected_artifact_name,
            size_in_bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            content=content,
        )

    def read_result_artifact_json(self, archive: GitHubArtifactArchive) -> dict[str, Any]:
        try:
            with zipfile.ZipFile(io.BytesIO(archive.content)) as bundle:
                matches = [
                    item
                    for item in bundle.infolist()
                    if item.filename == self.config.result_member_name
                ]
                if len(matches) != 1:
                    raise GitHubArtifactError("result artifact에 result JSON이 정확히 하나 있어야 해")
                member = matches[0]
                if member.flag_bits & 0x1:
                    raise GitHubArtifactError("암호화된 result artifact는 지원하지 않아")
                if member.file_size > self.config.max_result_json_bytes:
                    raise GitHubArtifactError("result JSON이 허용 크기를 넘었어")
                raw = bundle.read(member)
        except GitHubArtifactError:
            raise
        except (OSError, zipfile.BadZipFile, RuntimeError) as exc:
            raise GitHubArtifactError("result artifact ZIP을 읽지 못했어") from exc
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GitHubArtifactError("result artifact JSON 형식이 올바르지 않아") from exc
        if not isinstance(data, dict):
            raise GitHubArtifactError("result artifact JSON 최상위 값은 object여야 해")
        return redact_sensitive_data(data)

    def _request_json(
        self,
        method: str,
        path: str,
        *,
        expected_statuses: set[int],
        params: Mapping[str, Any] | None = None,
    ) -> Any:
        response = self._request(
            method,
            path,
            expected_statuses=expected_statuses,
            params=params,
        )
        try:
            return response.json()
        except Exception as exc:
            raise GitHubApiError("GitHub API JSON 응답 형식이 올바르지 않아") from exc

    def _request(
        self,
        method: str,
        path: str,
        *,
        expected_statuses: set[int],
        params: Mapping[str, Any] | None = None,
        json_body: Mapping[str, Any] | None = None,
        stream: bool = False,
        allow_redirects: bool = True,
    ) -> Any:
        if not path.startswith(f"/{self.config.repository_path}/"):
            raise GitHubApiError("고정 coordinator repository 밖의 GitHub API 호출은 허용하지 않아")
        url = f"{self.config.api_base_url.rstrip('/')}{path}"
        token = self._token_provider.get_token()
        try:
            response = self._session.request(
                method.upper(),
                url,
                headers={
                    "Accept": "application/vnd.github+json",
                    "Authorization": f"Bearer {token}",
                    "X-GitHub-Api-Version": self.config.api_version,
                    "User-Agent": "boxer-hpa-change-workflow",
                },
                params=dict(params or {}),
                json=dict(json_body) if json_body is not None else None,
                timeout=max(1, self.config.timeout_sec),
                stream=stream,
                allow_redirects=allow_redirects,
            )
        except Exception as exc:
            raise GitHubApiError(
                f"GitHub API 호출 실패: {redact_sensitive_text(str(exc))}"
            ) from exc
        status_code = int(getattr(response, "status_code", 0) or 0)
        if status_code not in expected_statuses:
            detail = redact_sensitive_text(str(getattr(response, "text", "")))[:500]
            raise GitHubApiError(
                f"GitHub API 응답 오류 ({status_code}): {detail}",
                status_code=status_code,
            )
        return response

    @staticmethod
    def _parse_workflow_run(data: Mapping[str, Any]) -> GitHubWorkflowRun:
        run_id = int(data.get("id") or 0)
        if run_id <= 0:
            raise GitHubApiError("GitHub workflow run id가 올바르지 않아")

        def parse_optional_datetime(raw: Any) -> datetime | None:
            text = str(raw or "").strip()
            if not text:
                return None
            try:
                return _datetime_from_text(text)
            except (TypeError, ValueError) as exc:
                raise GitHubApiError("GitHub workflow run 시간 형식이 올바르지 않아") from exc

        return GitHubWorkflowRun(
            run_id=run_id,
            status=str(data.get("status") or "").strip().lower(),
            conclusion=str(data.get("conclusion") or "").strip().lower(),
            html_url=redact_sensitive_text(str(data.get("html_url") or "").strip()),
            display_title=str(data.get("display_title") or "").strip(),
            run_attempt=max(1, int(data.get("run_attempt") or 1)),
            created_at=parse_optional_datetime(data.get("created_at")),
            updated_at=parse_optional_datetime(data.get("updated_at")),
        )


class HpaChangeWorkflowService:
    """Slack route와 reporter가 쓰는 등록·dispatch·poll·결과 반영 facade다."""

    def __init__(
        self,
        store: HpaChangeJobStore,
        github: GitHubCoordinatorClient,
    ) -> None:
        self.store = store
        self.github = github

    def register_request(self, **kwargs: Any) -> HpaChangeJobRegistration:
        return self.store.register_job(**kwargs)

    def submit(self, request: HpaChangeRequest) -> tuple[HpaChangeJob, bool]:
        """Slack 재전송은 기존 job을 돌려주고, 최초 요청만 coordinator에 전달한다."""

        registration = self.store.register_job(
            workspace_id=request.workspace_id,
            event_ts=request.event_ts,
            channel_id=request.channel_id,
            thread_ts=request.thread_ts,
            requested_by=request.requested_by,
            request_text=request.request_text,
            thread_url=request.thread_url,
            attachments=request.attachments,
            metadata=request.metadata,
        )
        if not registration.created:
            return registration.job, False
        return (
            self.dispatch(registration.job.task_id),
            True,
        )

    def dispatch(
        self,
        task_id: str,
        *,
        allow_recovery: bool = False,
    ) -> HpaChangeJob:
        job = self.store.claim_review_dispatch(
            task_id,
            allow_recovery=allow_recovery,
        )
        if job is None:
            return self.store.get_job(task_id)
        try:
            self.github.dispatch_job(job)
        except GitHubApiError as exc:
            # POST가 수락된 뒤 응답만 유실됐을 수 있으므로 transport 오류는
            # 실패로 확정하지 않고 run 조회·stale 재전송으로 reconciliation한다.
            if exc.status_code is None:
                return self.store.get_job(task_id)
            self.store.mark_failed(task_id, str(exc))
            raise
        except Exception as exc:
            self.store.mark_failed(task_id, str(exc))
            raise
        return self.store.mark_dispatched(task_id)

    def dispatch_implementation(
        self,
        task_id: str,
        *,
        allow_recovery: bool = False,
    ) -> HpaChangeJob:
        """Slack 검토 게시가 영속화된 작업만 구현 phase로 넘긴다."""

        current = self.store.get_job(task_id)
        if current.workflow_run_id is None:
            raise HpaChangeWorkflowError("구현에 사용할 review workflow run id가 없어")
        review_run_id = current.workflow_run_id
        job = self.store.claim_implementation_dispatch(
            task_id,
            allow_recovery=allow_recovery,
        )
        if job is None:
            return self.store.get_job(task_id)
        try:
            self.github.dispatch_implementation(job, review_run_id=review_run_id)
        except GitHubApiError as exc:
            if exc.status_code is None:
                return self.store.get_job(task_id)
            self.store.mark_failed(task_id, str(exc))
            raise
        except Exception as exc:
            self.store.mark_failed(task_id, str(exc))
            raise
        return self.store.mark_dispatched(task_id)

    def refresh(self, task_id: str) -> HpaChangeJob:
        job = self.store.get_job(task_id)
        if job.status in {
            HpaChangeStatus.PR_CREATED,
            HpaChangeStatus.NEEDS_CLARIFICATION,
            HpaChangeStatus.REVIEW_READY,
            HpaChangeStatus.REVIEW_POSTED,
            HpaChangeStatus.RESULT_READY,
            HpaChangeStatus.WORKFLOW_SUCCEEDED,
            HpaChangeStatus.FAILED,
            HpaChangeStatus.CANCELED,
        }:
            return job
        run = self.github.find_workflow_run(
            task_id,
            phase=job.workflow_phase or "review",
            created_after=job.created_at - timedelta(minutes=1),
        )
        if run is None:
            return job
        if run.status != "completed":
            return self.store.mark_running(task_id, run)
        if run.conclusion == "success":
            return self.store.mark_workflow_succeeded(task_id, run)
        if run.conclusion == "cancelled":
            return self.store.mark_canceled(task_id, "GitHub coordinator workflow 취소")
        return self.store.mark_failed(
            task_id,
            f"GitHub coordinator workflow 실패: {run.conclusion or 'unknown'}",
        )

    def poll_job(self, job_or_task_id: HpaChangeJob | str) -> HpaChangePollResult:
        """reporter가 한 번 호출할 때 GitHub 상태와 result artifact까지 가능한 만큼 전진시킨다."""

        task_id = (
            job_or_task_id.task_id
            if isinstance(job_or_task_id, HpaChangeJob)
            else str(job_or_task_id or "").strip()
        )
        job = self.store.get_job(task_id)
        if job.status == HpaChangeStatus.RECEIVED:
            # register 직후 프로세스가 종료된 작업도 재시작한 poller가 coordinator에 전달한다.
            job = self.dispatch(task_id)
        if job.status == HpaChangeStatus.REVIEW_POSTED:
            # Slack 게시 직후 프로세스가 재시작돼도 구현 dispatch를 이어간다.
            job = self.dispatch_implementation(task_id)
        if job.status in {
            HpaChangeStatus.DISPATCHING,
            HpaChangeStatus.DISPATCHED,
            HpaChangeStatus.RUNNING,
        }:
            job = self.refresh(task_id)
        if (
            job.workflow_phase == "review"
            and job.status in {HpaChangeStatus.DISPATCHING, HpaChangeStatus.DISPATCHED}
        ):
            job = self.dispatch(task_id, allow_recovery=True)
        if (
            job.workflow_phase == "implementation"
            and job.status in {HpaChangeStatus.DISPATCHING, HpaChangeStatus.DISPATCHED}
        ):
            # API 수락 전후에 프로세스가 종료된 경우 일정 시간 뒤 안전하게 재전송한다.
            # workflow 쪽 최초-run gate가 중복 repository_dispatch의 실제 구현을 막는다.
            job = self.dispatch_implementation(task_id, allow_recovery=True)
        if job.status == HpaChangeStatus.WORKFLOW_SUCCEEDED:
            try:
                job = self.consume_result_artifact(task_id)
            except GitHubArtifactNotReady:
                # workflow 완료 직후 artifact 목록 반영이 늦을 수 있어서 다음 poll에서 재시도한다.
                job = self.store.get_job(task_id)
        return self._build_poll_result(job)

    def consume_result_artifact(self, task_id: str) -> HpaChangeJob:
        job = self.store.get_job(task_id)
        if job.workflow_run_id is None:
            raise HpaChangeWorkflowError("결과 artifact를 받을 GitHub workflow run id가 없어")
        try:
            archive = self.github.download_result_artifact_zip(job.workflow_run_id, task_id)
            result = self.github.read_result_artifact_json(archive)
            result_task_id = str(result.get("task_id") or "").strip()
            if result_task_id and result_task_id != task_id:
                raise GitHubArtifactError("result artifact task id가 요청 작업과 달라")
            raw_status = str(result.get("status") or "").strip().lower().replace("-", "_")
            if raw_status == "review_ready":
                return self.store.mark_review_ready(task_id, archive, result=result)
            self.store.mark_result_ready(task_id, archive, result=result)
            return self.apply_result_payload(task_id, result)
        except GitHubArtifactNotReady:
            raise
        except Exception as exc:
            self.store.mark_failed(task_id, str(exc))
            raise

    def apply_result_payload(
        self,
        task_id: str,
        result: Mapping[str, Any],
    ) -> HpaChangeJob:
        safe_result = redact_sensitive_data(dict(result))
        raw_status = str(safe_result.get("status") or "").strip().lower().replace("-", "_")
        summary = str(
            safe_result.get("summary")
            or safe_result.get("message")
            or ""
        ).strip()

        if raw_status == "review_ready":
            # consume_result_artifact가 archive 정보와 함께 처리해야 하는 상태다.
            raise GitHubArtifactError("review_ready 결과는 artifact 수집 경로로 처리해야 해")

        if raw_status in {"needs_clarification", "clarification_required", "blocked"}:
            questions = safe_result.get("questions")
            if isinstance(questions, list):
                rendered_questions = [str(item).strip() for item in questions if str(item).strip()]
                if rendered_questions:
                    summary = summary or "\n".join(f"• {item}" for item in rendered_questions)
            return self.store.mark_needs_clarification(
                task_id,
                summary or "구현 전 추가 확인이 필요해",
                result=safe_result,
            )

        if raw_status in {"pr_opened", "pr_created", "completed", "success"}:
            pr_urls = self._extract_pr_urls(safe_result)
            if pr_urls:
                return self.store.mark_pr_created(
                    task_id,
                    pr_urls,
                    message=summary or "HPA 변경 PR 생성 완료",
                    result=safe_result,
                )
            if raw_status in {"pr_opened", "pr_created"}:
                raise GitHubArtifactError("PR 생성 완료 결과에 유효한 GitHub PR URL이 없어")
            return self.store.transition(
                task_id,
                HpaChangeStatus.RESULT_READY,
                result=safe_result,
                status_message=summary or "workflow 결과 확인 완료",
            )

        if raw_status in {"failed", "error"}:
            error_message = str(
                safe_result.get("error")
                or safe_result.get("error_message")
                or summary
                or "coordinator가 실패 결과를 반환했어"
            )
            return self.store.mark_failed(task_id, error_message)
        if raw_status in {"canceled", "cancelled"}:
            return self.store.mark_canceled(task_id, summary or "coordinator 작업 취소")
        return self.store.transition(
            task_id,
            HpaChangeStatus.RESULT_READY,
            result=safe_result,
            status_message=summary or "workflow 결과 확인 필요",
        )

    @staticmethod
    def _extract_pr_urls(result: Mapping[str, Any]) -> tuple[str, ...]:
        candidates: list[Any] = []
        raw_urls = result.get("pr_urls")
        if isinstance(raw_urls, list):
            candidates.extend(raw_urls)
        raw_prs = result.get("pull_requests")
        if isinstance(raw_prs, list):
            for item in raw_prs:
                if isinstance(item, Mapping):
                    candidates.append(item.get("url") or item.get("html_url"))
                else:
                    candidates.append(item)
        contract_prs = result.get("prs")
        if isinstance(contract_prs, list):
            for item in contract_prs:
                if isinstance(item, Mapping):
                    candidates.append(item.get("url"))
        unique_urls: list[str] = []
        for candidate in candidates:
            url = str(candidate or "").strip()
            if not _RESULT_PR_URL_PATTERN.fullmatch(url) or url in unique_urls:
                continue
            unique_urls.append(url)
        return tuple(unique_urls)

    @staticmethod
    def _build_poll_result(job: HpaChangeJob) -> HpaChangePollResult:
        if job.status == HpaChangeStatus.REVIEW_READY:
            state = HpaChangePollState.REVIEW_READY
        elif job.status == HpaChangeStatus.NEEDS_CLARIFICATION:
            state = HpaChangePollState.NEEDS_CLARIFICATION
        elif job.status == HpaChangeStatus.PR_CREATED:
            state = HpaChangePollState.PR_OPENED
        elif job.status in {HpaChangeStatus.FAILED, HpaChangeStatus.CANCELED}:
            state = HpaChangePollState.FAILED
        elif job.status in {
            HpaChangeStatus.RUNNING,
            HpaChangeStatus.WORKFLOW_SUCCEEDED,
            HpaChangeStatus.RESULT_READY,
            HpaChangeStatus.REVIEW_POSTED,
        }:
            state = HpaChangePollState.RUNNING
        else:
            state = HpaChangePollState.QUEUED
        return HpaChangePollResult(
            task_id=job.task_id,
            state=state,
            job=job,
            run_url=job.workflow_run_url,
            result=job.result,
            message=job.error_message or job.status_message,
            pr_urls=job.pr_urls,
        )


__all__ = [
    "GitHubApiError",
    "GitHubAppPermissions",
    "GitHubAppTokenProvider",
    "GitHubArtifactArchive",
    "GitHubArtifactError",
    "GitHubArtifactNotReady",
    "GitHubCoordinatorClient",
    "GitHubCoordinatorConfig",
    "GitHubDispatchReceipt",
    "GitHubTokenProvider",
    "GitHubWorkflowRun",
    "HpaChangeJob",
    "HpaChangeAttachment",
    "HpaChangeJobRegistration",
    "HpaChangeJobStore",
    "HpaChangePollResult",
    "HpaChangePollState",
    "HpaChangeRequest",
    "HpaChangeStatus",
    "HpaChangeWorkflowError",
    "HpaChangeWorkflowService",
    "InvalidHpaChangeTransition",
    "StaticGitHubTokenProvider",
    "generate_hpa_change_task_id",
    "redact_sensitive_data",
    "redact_sensitive_text",
]
