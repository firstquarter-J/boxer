import os
import re
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _env_flag(name: str) -> bool:
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}


def _resolve_dotenv_path(raw_path: str) -> Path:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = PROJECT_ROOT / candidate
    return candidate


if not _env_flag("BOXER_SKIP_DOTENV"):
    dotenv_path_raw = os.getenv("BOXER_DOTENV_PATH", "").strip()
    dotenv_path = (
        _resolve_dotenv_path(dotenv_path_raw)
        if dotenv_path_raw
        else PROJECT_ROOT / ".env"
    )
    load_dotenv(dotenv_path=dotenv_path, override=False)


def _getenv_any(*names: str, default: str = "") -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None:
            return value
    return default

# Phase 1 로컬 실행은 .env 기준
# 운영 환경에서는 Secrets Manager 연동 예정
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "").lower()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
ANTHROPIC_TIMEOUT_SEC = int(os.getenv("ANTHROPIC_TIMEOUT_SEC", "60"))
ANTHROPIC_MAX_TOKENS = int(os.getenv("ANTHROPIC_MAX_TOKENS", "700"))
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
OLLAMA_TIMEOUT_SEC = int(os.getenv("OLLAMA_TIMEOUT_SEC", "300"))
OLLAMA_HEALTH_TIMEOUT_SEC = int(os.getenv("OLLAMA_HEALTH_TIMEOUT_SEC", "2"))
OLLAMA_TEMPERATURE = float(os.getenv("OLLAMA_TEMPERATURE", "0.0"))
LLM_SYNTHESIS_ENABLED = os.getenv("LLM_SYNTHESIS_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
LLM_SYNTHESIS_MAX_EVIDENCE_CHARS = int(os.getenv("LLM_SYNTHESIS_MAX_EVIDENCE_CHARS", "7000"))
LLM_SYNTHESIS_MASKING_ENABLED = os.getenv("LLM_SYNTHESIS_MASKING_ENABLED", "true").lower() in {
    "1",
    "true",
    "yes",
    "on",
}
LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT = os.getenv(
    "LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT",
    "false",
).lower() in {"1", "true", "yes", "on"}
RETRIEVAL_SYNTHESIS_SYSTEM_PROMPT = os.getenv(
    "RETRIEVAL_SYNTHESIS_SYSTEM_PROMPT",
    "You are a retrieval-grounded assistant. Answer briefly using only provided evidence. "
    "Match the user's language when it is clear from the question; otherwise answer in English. "
    "Prioritize Evidence(JSON) over thread context. Do not add recommendations unless evidence explicitly supports them. "
    "If evidence is insufficient, clearly say what is missing.",
)

THREAD_CONTEXT_FETCH_LIMIT = int(os.getenv("THREAD_CONTEXT_FETCH_LIMIT", "100"))
THREAD_CONTEXT_MAX_MESSAGES = int(os.getenv("THREAD_CONTEXT_MAX_MESSAGES", "12"))
THREAD_CONTEXT_MAX_CHARS = int(os.getenv("THREAD_CONTEXT_MAX_CHARS", "5000"))

NOTION_API_BASE_URL = os.getenv("NOTION_API_BASE_URL", "https://api.notion.com/v1").rstrip("/")
NOTION_API_VERSION = os.getenv("NOTION_API_VERSION", "2022-06-28").strip()
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
NOTION_API_TIMEOUT_SEC = int(os.getenv("NOTION_API_TIMEOUT_SEC", "10"))
NOTION_TEST_PAGE_ID = os.getenv("NOTION_TEST_PAGE_ID", "").strip()
NOTION_MAX_BLOCKS = int(os.getenv("NOTION_MAX_BLOCKS", "200"))

DB_QUERY_ENABLED = os.getenv("DB_QUERY_ENABLED", "").lower() in {"1", "true", "yes", "on"}

DB_HOST = os.getenv("DB_HOST", "")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USERNAME = os.getenv("DB_USERNAME", "")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_DATABASE = os.getenv("DB_DATABASE", "")

DB_QUERY_TIMEOUT_SEC = int(os.getenv("DB_QUERY_TIMEOUT_SEC", "8"))
DB_QUERY_MAX_ROWS = int(os.getenv("DB_QUERY_MAX_ROWS", "20"))
DB_QUERY_MAX_SQL_CHARS = int(os.getenv("DB_QUERY_MAX_SQL_CHARS", "600"))
DB_QUERY_MAX_RESULT_CHARS = int(os.getenv("DB_QUERY_MAX_RESULT_CHARS", "2500"))

S3_QUERY_ENABLED = os.getenv("S3_QUERY_ENABLED", "").lower() in {"1", "true", "yes", "on"}
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
S3_ULTRASOUND_BUCKET = os.getenv("S3_ULTRASOUND_BUCKET", "")
S3_LOG_BUCKET = os.getenv("S3_LOG_BUCKET", "")
S3_QUERY_TIMEOUT_SEC = int(os.getenv("S3_QUERY_TIMEOUT_SEC", "8"))
S3_QUERY_MAX_KEYS = int(os.getenv("S3_QUERY_MAX_KEYS", "20000"))
S3_QUERY_MAX_ITEMS = int(os.getenv("S3_QUERY_MAX_ITEMS", "20"))
S3_QUERY_MAX_RESULT_CHARS = int(os.getenv("S3_QUERY_MAX_RESULT_CHARS", "3500"))
S3_LOG_TAIL_BYTES = int(os.getenv("S3_LOG_TAIL_BYTES", "50000"))
S3_LOG_TAIL_LINES = int(os.getenv("S3_LOG_TAIL_LINES", "80"))

REQUEST_LOG_SQLITE_ENABLED = _getenv_any(
    "REQUEST_LOG_SQLITE_ENABLED",
    "REQUEST_AUDIT_SQLITE_ENABLED",
    default="false",
).lower() in {"1", "true", "yes", "on"}
REQUEST_LOG_SQLITE_PATH = _getenv_any(
    "REQUEST_LOG_SQLITE_PATH",
    "REQUEST_AUDIT_SQLITE_PATH",
    default=str(PROJECT_ROOT / "data" / "request_log.db"),
).strip()
REQUEST_LOG_SQLITE_TIMEOUT_SEC = int(
    _getenv_any(
        "REQUEST_LOG_SQLITE_TIMEOUT_SEC",
        "REQUEST_AUDIT_SQLITE_TIMEOUT_SEC",
        default="5",
    )
)
REQUEST_LOG_SQLITE_BUSY_TIMEOUT_MS = int(
    _getenv_any(
        "REQUEST_LOG_SQLITE_BUSY_TIMEOUT_MS",
        "REQUEST_AUDIT_SQLITE_BUSY_TIMEOUT_MS",
        default="5000",
    )
)
REQUEST_LOG_SQLITE_INIT_ON_STARTUP = _getenv_any(
    "REQUEST_LOG_SQLITE_INIT_ON_STARTUP",
    "REQUEST_AUDIT_SQLITE_INIT_ON_STARTUP",
    default="true",
).lower() in {"1", "true", "yes", "on"}
REQUEST_LOG_TIMEZONE = (
    _getenv_any(
        "REQUEST_LOG_TIMEZONE",
        "REQUEST_AUDIT_TIMEZONE",
        default="UTC",
    ).strip()
    or "UTC"
)
REQUEST_LOG_SQLITE_S3_BACKUP_ENABLED = _getenv_any(
    "REQUEST_LOG_SQLITE_S3_BACKUP_ENABLED",
    "REQUEST_AUDIT_SQLITE_S3_BACKUP_ENABLED",
    default="false",
).lower() in {"1", "true", "yes", "on"}
REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET = _getenv_any(
    "REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET",
    "REQUEST_AUDIT_SQLITE_S3_BACKUP_BUCKET",
    default="",
).strip()
REQUEST_LOG_SQLITE_S3_OBJECT_KEY = _getenv_any(
    "REQUEST_LOG_SQLITE_S3_OBJECT_KEY",
    "REQUEST_AUDIT_SQLITE_S3_OBJECT_KEY",
    default="boxer-request-log.db",
).strip().strip("/")
REQUEST_LOG_SQLITE_S3_PREFIX = _getenv_any(
    "REQUEST_LOG_SQLITE_S3_PREFIX",
    "REQUEST_AUDIT_SQLITE_S3_BACKUP_PREFIX",
    default="",
).strip().strip("/")
REQUEST_LOG_SQLITE_S3_STORAGE_CLASS = _getenv_any(
    "REQUEST_LOG_SQLITE_S3_STORAGE_CLASS",
    "REQUEST_AUDIT_SQLITE_S3_STORAGE_CLASS",
    default="",
).strip()
REQUEST_LOG_SQLITE_S3_SERVER_SIDE_ENCRYPTION = _getenv_any(
    "REQUEST_LOG_SQLITE_S3_SERVER_SIDE_ENCRYPTION",
    "REQUEST_AUDIT_SQLITE_S3_SERVER_SIDE_ENCRYPTION",
    default="",
).strip()
REQUEST_LOG_SQLITE_S3_RESTORE_ON_STARTUP = _getenv_any(
    "REQUEST_LOG_SQLITE_S3_RESTORE_ON_STARTUP",
    "REQUEST_AUDIT_SQLITE_S3_RESTORE_ON_STARTUP",
    default="false",
).lower() in {"1", "true", "yes", "on"}

# Backward-compatible aliases for earlier request audit naming.
REQUEST_AUDIT_SQLITE_ENABLED = REQUEST_LOG_SQLITE_ENABLED
REQUEST_AUDIT_SQLITE_PATH = REQUEST_LOG_SQLITE_PATH
REQUEST_AUDIT_SQLITE_TIMEOUT_SEC = REQUEST_LOG_SQLITE_TIMEOUT_SEC
REQUEST_AUDIT_SQLITE_BUSY_TIMEOUT_MS = REQUEST_LOG_SQLITE_BUSY_TIMEOUT_MS
REQUEST_AUDIT_SQLITE_INIT_ON_STARTUP = REQUEST_LOG_SQLITE_INIT_ON_STARTUP
REQUEST_AUDIT_TIMEZONE = REQUEST_LOG_TIMEZONE
REQUEST_AUDIT_SQLITE_S3_BACKUP_ENABLED = REQUEST_LOG_SQLITE_S3_BACKUP_ENABLED
REQUEST_AUDIT_SQLITE_S3_BACKUP_BUCKET = REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET
REQUEST_AUDIT_SQLITE_S3_OBJECT_KEY = REQUEST_LOG_SQLITE_S3_OBJECT_KEY
REQUEST_AUDIT_SQLITE_S3_BACKUP_PREFIX = REQUEST_LOG_SQLITE_S3_PREFIX
REQUEST_AUDIT_SQLITE_S3_STORAGE_CLASS = REQUEST_LOG_SQLITE_S3_STORAGE_CLASS
REQUEST_AUDIT_SQLITE_S3_SERVER_SIDE_ENCRYPTION = REQUEST_LOG_SQLITE_S3_SERVER_SIDE_ENCRYPTION
REQUEST_AUDIT_SQLITE_S3_RESTORE_ON_STARTUP = REQUEST_LOG_SQLITE_S3_RESTORE_ON_STARTUP

DEFAULT_DB_QUERY = "SELECT NOW() AS now_time, DATABASE() AS db_name"

DB_READONLY_SQL_HEAD_PATTERN = re.compile(
    r"^(select|show|describe|desc|explain|with)\b",
    re.IGNORECASE,
)
DB_FORBIDDEN_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|replace|rename|merge|upsert|call|do|handler|load|lock|unlock|analyze|optimize|repair)\b",
    re.IGNORECASE,
)
DB_FORBIDDEN_SQL_FRAGMENT_PATTERN = re.compile(
    r"\binto\s+(outfile|dumpfile)\b|\bload\s+data\b",
    re.IGNORECASE,
)
DB_LOCKING_READ_PATTERN = re.compile(
    r"\bfor\s+update\b|\block\s+in\s+share\s+mode\b",
    re.IGNORECASE,
)

DEFAULT_SYSTEM_PROMPT = (
    "You are a helpful assistant. Reply briefly, do not guess, and ask one clarifying question when needed."
)

ADAPTER_ENTRYPOINT = os.getenv("ADAPTER_ENTRYPOINT", "boxer.adapters.sample.slack:create_app")
