import os
import re

from dotenv import load_dotenv

load_dotenv()

# Phase 1 로컬 실행은 .env 기준
# 운영 환경에서는 Secrets Manager 연동 예정
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "")
SLACK_SIGNING_SECRET = os.getenv("SLACK_SIGNING_SECRET", "")

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "").lower()
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")
ANTHROPIC_MAX_TOKENS = int(os.getenv("ANTHROPIC_MAX_TOKENS", "700"))
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://127.0.0.1:11434").rstrip("/")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:1.5b")
OLLAMA_TIMEOUT_SEC = int(os.getenv("OLLAMA_TIMEOUT_SEC", "90"))
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
    "You are a retrieval-grounded assistant. Answer briefly in Korean using only provided evidence. "
    "Prioritize Evidence(JSON) over thread context. Do not add recommendations unless evidence explicitly supports them. "
    "If evidence is insufficient, clearly say what is missing.",
)

THREAD_CONTEXT_FETCH_LIMIT = int(os.getenv("THREAD_CONTEXT_FETCH_LIMIT", "100"))
THREAD_CONTEXT_MAX_MESSAGES = int(os.getenv("THREAD_CONTEXT_MAX_MESSAGES", "12"))
THREAD_CONTEXT_MAX_CHARS = int(os.getenv("THREAD_CONTEXT_MAX_CHARS", "5000"))

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
