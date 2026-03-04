import json
import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any
from urllib import error, parse, request
from zoneinfo import ZoneInfo

import boto3
import pymysql
from anthropic import Anthropic
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

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
THREAD_CONTEXT_FETCH_LIMIT = int(os.getenv("THREAD_CONTEXT_FETCH_LIMIT", "100"))
THREAD_CONTEXT_MAX_MESSAGES = int(os.getenv("THREAD_CONTEXT_MAX_MESSAGES", "12"))
THREAD_CONTEXT_MAX_CHARS = int(os.getenv("THREAD_CONTEXT_MAX_CHARS", "5000"))
MODEL_OWNER_USER_ID = "U0629HDSJHG"
MARK_USER_ID = "U02LBHACKEU"
DD_USER_ID = "U0A079J3L9M"
APP_USER_LOOKUP_ALLOWED_USER_IDS = {MODEL_OWNER_USER_ID, MARK_USER_ID}
DB_QUERY_ENABLED = os.getenv("DB_QUERY_ENABLED", "").lower() in {"1", "true", "yes", "on"}
BOX_DB_HOST = os.getenv("BOX_DB_HOST", "")
BOX_DB_PORT = int(os.getenv("BOX_DB_PORT", "3306"))
BOX_DB_USERNAME = os.getenv("BOX_DB_USERNAME", "")
BOX_DB_PASSWORD = os.getenv("BOX_DB_PASSWORD", "")
BOX_DB_DATABASE = os.getenv("BOX_DB_DATABASE", "")
APP_USER_API_URL = os.getenv(
    "APP_USER_API_URL",
    "https://bh63r1dl09.execute-api.ap-northeast-2.amazonaws.com/prod/app-user",
)
APP_USER_API_TIMEOUT_SEC = int(os.getenv("APP_USER_API_TIMEOUT_SEC", "8"))
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
LOG_ANALYSIS_MAX_DEVICES = int(os.getenv("LOG_ANALYSIS_MAX_DEVICES", "8"))
LOG_ANALYSIS_MAX_SAMPLES = int(os.getenv("LOG_ANALYSIS_MAX_SAMPLES", "5"))
LOG_SCAN_MAX_EVENTS = int(os.getenv("LOG_SCAN_MAX_EVENTS", "50"))
DEFAULT_DB_QUERY = "SELECT NOW() AS now_time, DATABASE() AS db_name"
BARCODE_PATTERN = re.compile(r"(?<!\d)(\d{11})(?!\d)")
DB_WRITE_SQL_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|replace)\b",
    re.IGNORECASE,
)
S3_LOG_DATE_TOKEN_PATTERN = re.compile(r"^20\d{2}-\d{2}-\d{2}$")
S3_LOG_PATH_PATTERN = re.compile(
    r"([A-Za-z0-9][A-Za-z0-9_-]*)/log-(20\d{2}-\d{2}-\d{2})\.log",
    re.IGNORECASE,
)
S3_LOG_FILE_TOKEN_PATTERN = re.compile(r"^log-(20\d{2}-\d{2}-\d{2})\.log$", re.IGNORECASE)
S3_DEVICE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,}$")
LOG_DATE_PATTERN = re.compile(r"(20\d{2}-\d{2}-\d{2})")
S3_LOG_RESERVED_TOKENS = {
    "s3",
    "조회",
    "확인",
    "읽어줘",
    "읽어",
    "읽기",
    "보여줘",
    "로그",
    "log",
}
YESTERDAY_HINTS = ("어제", "전일", "yesterday")
LOG_ERROR_KEYWORDS = (
    "error",
    "err",
    "exception",
    "fatal",
    "fail",
    "timeout",
    "timed out",
    "traceback",
    "panic",
    "오류",
    "에러",
    "실패",
    "타임아웃",
    "예외",
)
SCAN_FOCUSED_HINTS = (
    "단순",
    "스캔",
    "명령",
    "커맨드",
    "command",
    "scan",
    "타임라인",
)
SCANNED_TOKEN_PATTERN = re.compile(r"Scanned:\s*([^\s]+)", re.IGNORECASE)
LOG_LINE_TIME_PATTERN = re.compile(r"\b(\d{2}:\d{2}:\d{2})\b")
SCAN_CODE_LABELS: dict[str, str] = {
    "C_STOPSESS": "녹화 중지",
    "C_PAUSE": "일시정지",
    "C_RESUME": "재개",
    "C_CCLREC": "녹화 취소",
    "SPECIAL_TAKE_SNAP": "캡처/스냅샷",
}
VIDEO_HINT_TOKENS = ("영상", "비디오", "동영상", "recording")
VIDEO_COUNT_HINT_TOKENS = ("몇 개", "몇개", "개수", "갯수", "수", "count")
COMMON_SYSTEM_PROMPT = (
    "You are Boxer, the internal assistant for Box and Humanscape. "
    "Language policy: reply in Korean by default; if the user asks in English, reply in English. "
    "Tone policy: always use informal/casual tone; do not use Korean honorific endings such as 요/습니다. "
    "Answer policy: keep replies concise (normally 3-6 sentences) and start with the key point. "
    "Format policy: provide a one-line summary first, then details when useful. "
    "Do not add unnecessary apologies, meta commentary, or long preambles. "
    "If evidence is insufficient, explicitly say you do not know; do not guess. "
    "Do not assert uncertain facts (versions, specs, prices, policies). "
    "If a question is ambiguous, ask exactly one clarifying question. "
    "For list requests based on thread messages, return all items with original order and count, with no omissions. "
    "Prioritize Box/Humanscape context in your answers."
)


def _validate_tokens() -> None:
    missing = []
    if not SLACK_BOT_TOKEN or "REPLACE_ME" in SLACK_BOT_TOKEN:
        missing.append("SLACK_BOT_TOKEN")
    if not SLACK_APP_TOKEN or "REPLACE_ME" in SLACK_APP_TOKEN:
        missing.append("SLACK_APP_TOKEN")
    if not SLACK_SIGNING_SECRET or "REPLACE_ME" in SLACK_SIGNING_SECRET:
        missing.append("SLACK_SIGNING_SECRET")
    if LLM_PROVIDER == "claude":
        if not ANTHROPIC_API_KEY or "REPLACE_ME" in ANTHROPIC_API_KEY:
            missing.append("ANTHROPIC_API_KEY")
        if not ANTHROPIC_MODEL or "REPLACE_ME" in ANTHROPIC_MODEL:
            missing.append("ANTHROPIC_MODEL")
    if LLM_PROVIDER == "ollama":
        if not OLLAMA_BASE_URL or "REPLACE_ME" in OLLAMA_BASE_URL:
            missing.append("OLLAMA_BASE_URL")
        if not OLLAMA_MODEL or "REPLACE_ME" in OLLAMA_MODEL:
            missing.append("OLLAMA_MODEL")
    if DB_QUERY_ENABLED:
        if not BOX_DB_HOST or "REPLACE_ME" in BOX_DB_HOST:
            missing.append("BOX_DB_HOST")
        if BOX_DB_PORT <= 0:
            missing.append("BOX_DB_PORT")
        if not BOX_DB_USERNAME or "REPLACE_ME" in BOX_DB_USERNAME:
            missing.append("BOX_DB_USERNAME")
        if not BOX_DB_PASSWORD or "REPLACE_ME" in BOX_DB_PASSWORD:
            missing.append("BOX_DB_PASSWORD")
        if not BOX_DB_DATABASE or "REPLACE_ME" in BOX_DB_DATABASE:
            missing.append("BOX_DB_DATABASE")
    if S3_QUERY_ENABLED:
        if not AWS_REGION or "REPLACE_ME" in AWS_REGION:
            missing.append("AWS_REGION")
        if not S3_ULTRASOUND_BUCKET or "REPLACE_ME" in S3_ULTRASOUND_BUCKET:
            missing.append("S3_ULTRASOUND_BUCKET")
        if not S3_LOG_BUCKET or "REPLACE_ME" in S3_LOG_BUCKET:
            missing.append("S3_LOG_BUCKET")

    if missing:
        raise RuntimeError(
            "필수 환경변수가 설정되지 않았습니다(.env 확인): "
            + ", ".join(missing)
            + ". .env 값을 실제 값으로 교체하세요."
        )


def _extract_question(text: str) -> str:
    return re.sub(r"<@[^>]+>", "", text).strip()


def _extract_barcode(text: str) -> str | None:
    match = BARCODE_PATTERN.search(text)
    if not match:
        return None
    return match.group(1)


def _should_lookup_barcode(question: str, barcode: str) -> bool:
    normalized = (question or "").strip()
    if normalized == barcode:
        return True
    if normalized.startswith(barcode):
        suffix = normalized[len(barcode) :].strip()
        if suffix in {"", "조회", "조회해줘", "확인", "확인해줘"}:
            return True
    lowered = normalized.lower()
    return "바코드" in normalized or "barcode" in lowered


def _display_value(value: Any, default: str = "없음") -> str:
    if value is None:
        return default
    text = str(value).strip()
    if not text:
        return default
    return text


def _safe_float(value: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float("inf")


def _trim_context_lines(lines: list[str], max_chars: int) -> str:
    if max_chars <= 0:
        return ""
    kept: list[str] = []
    total_chars = 0
    for line in reversed(lines):
        next_len = len(line) + (1 if kept else 0)
        if total_chars + next_len > max_chars:
            break
        kept.append(line)
        total_chars += next_len
    kept.reverse()
    return "\n".join(kept)


def _load_thread_context(
    slack_client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str,
    current_ts: str,
) -> str:
    if not channel_id or not thread_ts:
        return ""
    try:
        response = slack_client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=max(1, min(200, THREAD_CONTEXT_FETCH_LIMIT)),
        )
    except Exception:
        logger.exception(
            "Failed to load thread context: channel=%s thread_ts=%s",
            channel_id,
            thread_ts,
        )
        return ""

    current_ts_num = _safe_float(current_ts)
    lines: list[str] = []
    for message in response.get("messages", []):
        message_ts = str(message.get("ts", ""))
        if message_ts == current_ts:
            continue
        if _safe_float(message_ts) > current_ts_num:
            continue

        text = (message.get("text") or "").strip()
        if not text:
            continue

        if message.get("bot_id"):
            speaker = "bot"
        else:
            speaker = message.get("user", "unknown")
        lines.append(f"{speaker}: {text}")

    if not lines:
        return ""

    # keep recent messages, not oldest messages
    if len(lines) > THREAD_CONTEXT_MAX_MESSAGES:
        lines = lines[-THREAD_CONTEXT_MAX_MESSAGES:]
    return _trim_context_lines(lines, THREAD_CONTEXT_MAX_CHARS)


def _build_model_input(question: str, thread_context: str) -> str:
    if not thread_context:
        return question
    return (
        "아래는 현재 스레드의 최근 대화다. 문맥을 반영해서 답변해라.\n\n"
        "[스레드 최근 대화]\n"
        f"{thread_context}\n\n"
        "[현재 질문]\n"
        f"{question}"
    )


def _format_reply_text(user_id: str | None, text: str) -> str:
    clean_text = (text or "").strip()
    if not clean_text:
        return clean_text
    if not user_id:
        return clean_text
    return f"<@{user_id}> {clean_text}"


def _normalize_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _truncate_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n...(truncated)"


def _format_datetime(value: Any) -> str:
    if hasattr(value, "strftime"):
        return value.strftime("%Y-%m-%d %H:%M:%S %Z")
    if value is None:
        return "unknown"
    return str(value)


def _format_size(size: int | None) -> str:
    if size is None:
        return "unknown"
    value = float(max(0, int(size)))
    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    while value >= 1024 and index < len(units) - 1:
        value /= 1024
        index += 1
    if index == 0:
        return f"{int(value)} {units[index]}"
    return f"{value:.1f} {units[index]}"


def _build_s3_client() -> Any:
    timeout_sec = max(1, S3_QUERY_TIMEOUT_SEC)
    config = BotoConfig(
        region_name=AWS_REGION,
        connect_timeout=timeout_sec,
        read_timeout=timeout_sec,
        retries={"max_attempts": 2, "mode": "standard"},
    )
    return boto3.client("s3", region_name=AWS_REGION, config=config)


def _current_local_date() -> datetime.date:
    tz_name = os.getenv("TZ", "Asia/Seoul")
    try:
        return datetime.now(ZoneInfo(tz_name)).date()
    except Exception:
        try:
            return datetime.now(ZoneInfo("Asia/Seoul")).date()
        except Exception:
            return datetime.utcnow().date()


def _extract_log_date(question: str) -> str:
    text = (question or "").strip()
    lowered = text.lower()

    matched = LOG_DATE_PATTERN.search(text)
    if matched:
        raw_date = matched.group(1)
        try:
            parsed = datetime.strptime(raw_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("날짜 형식은 YYYY-MM-DD로 입력해줘") from exc
        return parsed.strftime("%Y-%m-%d")

    base_date = _current_local_date()
    if any(token in lowered for token in YESTERDAY_HINTS):
        base_date = base_date - timedelta(days=1)
    # 기본값은 오늘(질문에 날짜가 없을 때)
    return base_date.strftime("%Y-%m-%d")


def _is_barcode_log_analysis_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()
    has_log_hint = ("로그" in text and "로그인" not in text) or bool(
        re.search(r"\blog\b", lowered)
    )
    if not has_log_hint:
        return False
    return True


def _is_barcode_video_count_request(question: str, barcode: str | None) -> bool:
    if not barcode:
        return False
    text = (question or "").strip()
    lowered = text.lower()

    if "로그" in text or re.search(r"\blog\b", lowered):
        return False

    has_video_hint = any(token in text for token in VIDEO_HINT_TOKENS) or any(
        token in lowered for token in VIDEO_HINT_TOKENS
    )
    if not has_video_hint:
        return False

    has_count_hint = any(token in text for token in VIDEO_COUNT_HINT_TOKENS) or (
        "몇" in text
    )
    return has_count_hint


def _create_db_connection(timeout_sec: int | None = None) -> Any:
    actual_timeout = max(1, timeout_sec if timeout_sec is not None else DB_QUERY_TIMEOUT_SEC)
    return pymysql.connect(
        host=BOX_DB_HOST,
        port=BOX_DB_PORT,
        user=BOX_DB_USERNAME,
        password=BOX_DB_PASSWORD,
        database=BOX_DB_DATABASE,
        connect_timeout=actual_timeout,
        read_timeout=actual_timeout,
        write_timeout=actual_timeout,
        charset="utf8mb4",
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _query_recordings_count_by_barcode(barcode: str) -> str:
    if not BOX_DB_HOST or not BOX_DB_USERNAME or not BOX_DB_PASSWORD or not BOX_DB_DATABASE:
        raise RuntimeError("BOX DB 접속 정보가 비어 있어")

    connection = _create_db_connection(DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) AS recordingCount FROM recordings WHERE fullBarcode = %s",
                (barcode,),
            )
            row = cursor.fetchone() or {}
    finally:
        connection.close()

    count = int(row.get("recordingCount") or 0)
    return (
        "*바코드 영상 개수 조회 결과*\n"
        f"• 바코드: `{barcode}`\n"
        f"• recordings row 수: *{count}개*"
    )


def _lookup_device_names_by_barcode(barcode: str) -> list[str]:
    if not BOX_DB_HOST or not BOX_DB_USERNAME or not BOX_DB_PASSWORD or not BOX_DB_DATABASE:
        raise RuntimeError("BOX DB 접속 정보가 비어 있어")

    sql_candidates = [
        (
            "SELECT DISTINCT d.deviceName AS deviceName "
            "FROM recordings r "
            "JOIN devices d ON d.deviceSeq = r.deviceSeq AND d.hospitalSeq = r.hospitalSeq "
            "WHERE r.fullBarcode = %s AND COALESCE(d.deviceName, '') <> '' "
            "LIMIT %s"
        ),
        (
            "SELECT DISTINCT d.deviceName AS deviceName "
            "FROM recordings r "
            "JOIN devices d ON d.deviceSeq = r.deviceSeq "
            "WHERE r.fullBarcode = %s AND COALESCE(d.deviceName, '') <> '' "
            "LIMIT %s"
        ),
    ]

    limit = max(1, min(50, LOG_ANALYSIS_MAX_DEVICES * 2))
    last_error: Exception | None = None
    connection = _create_db_connection(DB_QUERY_TIMEOUT_SEC)
    try:
        with connection.cursor() as cursor:
            for sql in sql_candidates:
                try:
                    cursor.execute(sql, (barcode, limit))
                    rows = cursor.fetchall()
                except pymysql.MySQLError as exc:
                    last_error = exc
                    continue

                names: list[str] = []
                seen: set[str] = set()
                for row in rows:
                    name = _display_value(row.get("deviceName"), default="")
                    if not name:
                        continue
                    if name in seen:
                        continue
                    seen.add(name)
                    names.append(name)
                if names:
                    return names
    finally:
        connection.close()

    if last_error:
        raise last_error
    return []


def _fetch_s3_device_log_lines(
    s3_client: Any,
    device_name: str,
    log_date: str,
) -> dict[str, Any]:
    key = f"{device_name}/log-{log_date}.log"
    try:
        head_response = s3_client.head_object(Bucket=S3_LOG_BUCKET, Key=key)
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NotFound", "NoSuchKey"}:
            return {
                "found": False,
                "device_name": device_name,
                "key": key,
                "content_length": 0,
                "lines": [],
            }
        raise

    content_length = int(head_response.get("ContentLength") or 0)
    tail_bytes = max(1024, S3_LOG_TAIL_BYTES)
    use_range = content_length > tail_bytes
    get_params: dict[str, Any] = {
        "Bucket": S3_LOG_BUCKET,
        "Key": key,
    }
    if use_range:
        range_start = max(0, content_length - tail_bytes)
        get_params["Range"] = f"bytes={range_start}-{content_length - 1}"

    get_response = s3_client.get_object(**get_params)
    body = get_response["Body"].read()
    text = body.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if use_range and lines:
        # Range 조회 시 첫 줄이 잘릴 수 있어 제거
        lines = lines[1:]

    return {
        "found": True,
        "device_name": device_name,
        "key": key,
        "content_length": content_length,
        "lines": lines,
    }


def _find_error_lines(lines: list[str]) -> list[tuple[int, str]]:
    matches: list[tuple[int, str]] = []
    for line_no, line in enumerate(lines, start=1):
        lowered = line.lower()
        if any(keyword in lowered for keyword in LOG_ERROR_KEYWORDS):
            matches.append((line_no, line))
    return matches


def _is_error_focused_request(question: str) -> bool:
    lowered = (question or "").lower()
    return any(keyword in lowered for keyword in LOG_ERROR_KEYWORDS)


def _is_scan_focused_request(question: str) -> bool:
    lowered = (question or "").lower()
    return any(keyword in lowered for keyword in SCAN_FOCUSED_HINTS)


def _extract_time_label_from_line(line: str) -> str:
    matched = LOG_LINE_TIME_PATTERN.search(line)
    if matched:
        return matched.group(1)
    return "시간미상"


def _parse_scanned_event(line: str) -> tuple[str, str] | None:
    matched = SCANNED_TOKEN_PATTERN.search(line)
    if not matched:
        return None
    token = matched.group(1).strip().strip("`'\",;:()[]{}")
    upper_token = token.upper()
    if upper_token in SCAN_CODE_LABELS:
        return token, SCAN_CODE_LABELS[upper_token]
    if re.fullmatch(r"\d{11}", token):
        return token, "녹화 시작 바코드 스캔"
    return token, f"기타 스캔 ({token})"


def _extract_scan_events(lines: list[str]) -> list[tuple[str, str, str]]:
    events: list[tuple[str, str, str]] = []
    for line in lines:
        parsed = _parse_scanned_event(line)
        if not parsed:
            continue
        token, label = parsed
        time_label = _extract_time_label_from_line(line)
        events.append((time_label, label, token))
    return events


def _analyze_barcode_log_scan_events(s3_client: Any, barcode: str, log_date: str) -> str:
    device_names = _lookup_device_names_by_barcode(barcode)
    if not device_names:
        return (
            "*바코드 로그 스캔 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• recordings/devices에서 매핑된 장비명을 찾지 못했어"
        )

    max_devices = max(1, min(20, LOG_ANALYSIS_MAX_DEVICES))
    target_devices = device_names[:max_devices]
    omitted_device_count = max(0, len(device_names) - len(target_devices))
    total_events = 0

    lines = [
        "*바코드 로그 스캔 분석 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(device_names)}개`",
    ]
    if omitted_device_count > 0:
        lines.append(
            f"• 참고: 장비가 많아서 상위 `{len(target_devices)}개`만 분석했어"
        )

    event_limit = max(1, min(200, LOG_SCAN_MAX_EVENTS))
    for device_name in target_devices:
        log_data = _fetch_s3_device_log_lines(s3_client, device_name, log_date)
        lines.append("")
        lines.append(f"*장비 `{device_name}`*")

        if not log_data["found"]:
            lines.append(f"• 로그 파일 없음: `{log_data['key']}`")
            continue

        source_lines = log_data["lines"]
        max_lines = max(1, min(500, S3_LOG_TAIL_LINES))
        if len(source_lines) > max_lines:
            source_lines = source_lines[-max_lines:]

        events = _extract_scan_events(source_lines)
        total_events += len(events)

        lines.append(f"• 파일: `{log_data['key']}`")
        lines.append(f"• 분석 범위: 최근 `{len(source_lines)}줄`")
        lines.append(f"• 스캔 이벤트: *{len(events)}건*")

        if not events:
            lines.append("• 타임라인: 없음")
            continue

        display_events = events[-event_limit:]
        if len(events) > len(display_events):
            lines.append(
                f"• 참고: 이벤트가 많아서 최근 `{len(display_events)}건`만 표시해"
            )
        for time_label, label, token in display_events:
            lines.append(f"- {time_label}: {label} (`{token}`)")

    lines.append("")
    if total_events > 0:
        lines.append(f"*요약*: 분석 범위에서 스캔 이벤트 `{total_events}건`을 찾았어")
    else:
        lines.append("*요약*: 분석 범위에서 스캔 이벤트를 찾지 못했어")
    lines.append("※ 현재는 로그 tail(최근 구간) 기준 분석이야")

    return _truncate_text("\n".join(lines), S3_QUERY_MAX_RESULT_CHARS)


def _analyze_barcode_log_errors(s3_client: Any, barcode: str, log_date: str) -> str:
    device_names = _lookup_device_names_by_barcode(barcode)
    if not device_names:
        return (
            "*바코드 로그 에러 분석 결과*\n"
            f"• 바코드: `{barcode}`\n"
            f"• 날짜: `{log_date}`\n"
            "• recordings/devices에서 매핑된 장비명을 찾지 못했어"
        )

    max_devices = max(1, min(20, LOG_ANALYSIS_MAX_DEVICES))
    target_devices = device_names[:max_devices]
    omitted_device_count = max(0, len(device_names) - len(target_devices))

    total_error_lines = 0
    lines = [
        "*바코드 로그 에러 분석 결과*",
        f"• 바코드: `{barcode}`",
        f"• 날짜: `{log_date}`",
        f"• 매핑 장비: `{len(device_names)}개`",
    ]
    if omitted_device_count > 0:
        lines.append(
            f"• 참고: 장비가 많아서 상위 `{len(target_devices)}개`만 분석했어"
        )

    for device_name in target_devices:
        log_data = _fetch_s3_device_log_lines(s3_client, device_name, log_date)
        lines.append("")
        lines.append(f"*장비 `{device_name}`*")

        if not log_data["found"]:
            lines.append(f"• 로그 파일 없음: `{log_data['key']}`")
            continue

        source_lines = log_data["lines"]
        max_lines = max(1, min(500, S3_LOG_TAIL_LINES))
        if len(source_lines) > max_lines:
            source_lines = source_lines[-max_lines:]

        error_lines = _find_error_lines(source_lines)
        total_error_lines += len(error_lines)

        lines.append(f"• 파일: `{log_data['key']}`")
        lines.append(f"• 파일 크기: `{_format_size(log_data['content_length'])}`")
        lines.append(f"• 분석 범위: 최근 `{len(source_lines)}줄`")
        lines.append(f"• 에러 패턴 라인 수: *{len(error_lines)}줄*")

        if not error_lines:
            lines.append("• 샘플: 없음")
            continue

        sample_count = max(1, min(10, LOG_ANALYSIS_MAX_SAMPLES))
        for index, (line_no, content) in enumerate(error_lines[-sample_count:], start=1):
            sample = content.strip()
            if len(sample) > 220:
                sample = sample[:220] + "...(truncated)"
            lines.append(f"{index}. [{line_no}] {sample}")

    lines.append("")
    if total_error_lines > 0:
        lines.append(f"*요약*: 분석 범위에서 에러 패턴 라인 `{total_error_lines}줄`을 찾았어")
    else:
        lines.append("*요약*: 분석 범위에서 에러 패턴 라인을 찾지 못했어")
    lines.append("※ 현재는 로그 tail(최근 구간) 기준 분석이야")

    return _truncate_text("\n".join(lines), S3_QUERY_MAX_RESULT_CHARS)


def _extract_s3_log_request(normalized_question: str) -> dict[str, str]:
    path_match = S3_LOG_PATH_PATTERN.search(normalized_question)
    if path_match:
        device_name = path_match.group(1)
        log_date = path_match.group(2)
        return {"kind": "log", "device_name": device_name, "log_date": log_date}

    tokens = [token.strip().strip("`'\",.()[]{}") for token in normalized_question.split()]
    date_token = ""
    for token in tokens:
        if S3_LOG_DATE_TOKEN_PATTERN.match(token):
            date_token = token
            break
        file_match = S3_LOG_FILE_TOKEN_PATTERN.match(token)
        if file_match:
            date_token = file_match.group(1)
            break

    if not date_token:
        raise ValueError("로그 조회는 날짜가 필요해. 예: s3 로그 MB2-X00001 2026-03-04")
    try:
        datetime.strptime(date_token, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("날짜 형식은 YYYY-MM-DD로 입력해줘") from exc

    device_name = ""
    for token in tokens:
        if not token:
            continue
        if token == date_token:
            continue
        lowered = token.lower()
        if lowered in S3_LOG_RESERVED_TOKENS:
            continue
        if S3_LOG_FILE_TOKEN_PATTERN.match(token):
            continue
        if "/" in token:
            prefix, suffix = token.split("/", 1)
            if S3_LOG_FILE_TOKEN_PATTERN.match(suffix) and S3_DEVICE_NAME_PATTERN.match(prefix):
                device_name = prefix
                break
        if S3_DEVICE_NAME_PATTERN.match(token):
            device_name = token
            break

    if not device_name:
        raise ValueError("장비명을 같이 입력해줘. 예: s3 로그 MB2-X00001 2026-03-04")

    return {"kind": "log", "device_name": device_name, "log_date": date_token}


def _extract_s3_request(question: str) -> dict[str, str] | None:
    normalized = _normalize_spaces(question)
    if not normalized:
        return None

    lowered = normalized.lower()
    if not re.match(r"^s3(\s|$)", lowered):
        return None

    if "로그" in normalized or "log" in lowered:
        return _extract_s3_log_request(normalized)

    if any(keyword in normalized for keyword in ("초음파", "영상")) or "ultrasound" in lowered:
        barcode = _extract_barcode(normalized)
        if not barcode:
            raise ValueError("영상 조회는 바코드(11자리 숫자)가 필요해. 예: s3 영상 43032748143")
        return {"kind": "ultrasound", "barcode": barcode}

    raise ValueError(
        "지원 형식: s3 영상 <바코드> 또는 s3 로그 <장비명> <YYYY-MM-DD>"
    )


def _query_s3_ultrasound_by_barcode(s3_client: Any, barcode: str) -> str:
    prefix = f"{barcode}/"
    continuation_token = None
    scanned_objects = 0
    reached_scan_limit = False
    video_objects: list[dict[str, Any]] = []
    image_objects: list[dict[str, Any]] = []

    while True:
        params: dict[str, Any] = {
            "Bucket": S3_ULTRASOUND_BUCKET,
            "Prefix": prefix,
            "MaxKeys": 1000,
        }
        if continuation_token:
            params["ContinuationToken"] = continuation_token

        response = s3_client.list_objects_v2(**params)
        contents = response.get("Contents") or []
        for obj in contents:
            scanned_objects += 1
            key = str(obj.get("Key") or "")
            lowered = key.lower()
            if lowered.endswith(".mp4"):
                video_objects.append(obj)
            elif lowered.endswith((".jpg", ".jpeg")):
                image_objects.append(obj)

            if scanned_objects >= max(1, S3_QUERY_MAX_KEYS):
                reached_scan_limit = True
                break

        if reached_scan_limit:
            break
        if not response.get("IsTruncated"):
            break

        continuation_token = response.get("NextContinuationToken")
        if not continuation_token:
            break

    if scanned_objects == 0:
        return (
            f"S3 조회 결과가 없어. "
            f"버킷 `{S3_ULTRASOUND_BUCKET}`에서 prefix `{prefix}`로 찾지 못했어"
        )

    def _last_modified_to_ts(obj: dict[str, Any]) -> float:
        value = obj.get("LastModified")
        if hasattr(value, "timestamp"):
            return float(value.timestamp())
        return 0.0

    video_objects.sort(key=_last_modified_to_ts, reverse=True)
    image_objects.sort(key=_last_modified_to_ts, reverse=True)

    display_limit = max(1, min(50, S3_QUERY_MAX_ITEMS))
    lines = [
        "*S3 초음파 객체 조회 결과*",
        f"• 버킷: `{S3_ULTRASOUND_BUCKET}`",
        f"• 바코드: `{barcode}`",
        f"• 영상(mp4): *{len(video_objects)}개*",
        f"• 이미지(jpg/jpeg): *{len(image_objects)}개*",
        f"• 스캔한 전체 객체 수: `{scanned_objects}`",
    ]
    if reached_scan_limit:
        lines.append(
            f"• 주의: 스캔 상한({max(1, S3_QUERY_MAX_KEYS)}개)에 도달해서 집계가 일부 누락될 수 있어"
        )

    lines.append("")
    lines.append(f"*최근 영상 상위 {min(len(video_objects), display_limit)}개*")
    if not video_objects:
        lines.append("- 없음")
    else:
        for index, obj in enumerate(video_objects[:display_limit], start=1):
            key = _display_value(obj.get("Key"), default="unknown")
            size = _format_size(obj.get("Size"))
            modified = _format_datetime(obj.get("LastModified"))
            lines.append(f"{index}. `{key}` | `{size}` | `{modified}`")

    lines.append("")
    lines.append(f"*최근 이미지 상위 {min(len(image_objects), display_limit)}개*")
    if not image_objects:
        lines.append("- 없음")
    else:
        for index, obj in enumerate(image_objects[:display_limit], start=1):
            key = _display_value(obj.get("Key"), default="unknown")
            size = _format_size(obj.get("Size"))
            modified = _format_datetime(obj.get("LastModified"))
            lines.append(f"{index}. `{key}` | `{size}` | `{modified}`")

    return _truncate_text("\n".join(lines), S3_QUERY_MAX_RESULT_CHARS)


def _query_s3_device_log(s3_client: Any, device_name: str, log_date: str) -> str:
    log_data = _fetch_s3_device_log_lines(s3_client, device_name, log_date)
    if not log_data["found"]:
        return f"S3 로그 파일을 찾지 못했어: `{log_data['key']}`"

    lines = log_data["lines"]
    if not lines:
        return (
            "*S3 로그 조회 결과*\n"
            f"• 버킷: `{S3_LOG_BUCKET}`\n"
            f"• 파일: `{log_data['key']}`\n"
            "• 로그 내용이 비어 있어"
        )

    max_lines = max(1, min(500, S3_LOG_TAIL_LINES))
    if len(lines) > max_lines:
        lines = lines[-max_lines:]
    excerpt = _truncate_text("\n".join(lines), S3_QUERY_MAX_RESULT_CHARS)

    response_lines = [
        "*S3 로그 조회 결과*",
        f"• 버킷: `{S3_LOG_BUCKET}`",
        f"• 장비: `{device_name}`",
        f"• 파일: `{log_data['key']}`",
        f"• 파일 크기: `{_format_size(log_data['content_length'])}`",
        f"• 표시 범위: 최근 `{len(lines)}줄`",
        "```text",
        excerpt,
        "```",
    ]
    return "\n".join(response_lines)


def _extract_db_query(question: str) -> str | None:
    normalized = question.strip()
    lowered = normalized.lower()
    if lowered.startswith("db 조회"):
        return normalized[5:].strip()
    if lowered.startswith("db조회"):
        return normalized[4:].strip()
    return None


def _validate_readonly_sql(raw_sql: str) -> str:
    sql = (raw_sql or "").strip()
    if not sql:
        return DEFAULT_DB_QUERY

    if len(sql) > max(1, DB_QUERY_MAX_SQL_CHARS):
        raise ValueError(f"SQL 길이는 최대 {DB_QUERY_MAX_SQL_CHARS}자까지 허용해")

    if sql.endswith(";"):
        sql = sql[:-1].strip()
    if ";" in sql:
        raise ValueError("한 번에 한 쿼리만 실행할 수 있어")

    lowered = sql.lower()
    if not lowered.startswith(("select", "show", "describe", "desc", "explain", "with")):
        raise ValueError("읽기 전용 쿼리(SELECT/SHOW/DESCRIBE/EXPLAIN/WITH)만 허용해")
    if DB_WRITE_SQL_PATTERN.search(sql):
        raise ValueError("쓰기/변경 쿼리는 허용하지 않아")

    return sql


def _query_db(sql: str) -> str:
    rows_limit = max(1, min(200, DB_QUERY_MAX_ROWS))
    timeout_sec = max(1, DB_QUERY_TIMEOUT_SEC)
    connection = _create_db_connection(timeout_sec)

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchmany(rows_limit)
            rowcount = cursor.rowcount
    finally:
        connection.close()

    if not rows:
        return "DB 조회 결과가 없어"

    payload = json.dumps(rows, ensure_ascii=False, default=str)
    if len(payload) > DB_QUERY_MAX_RESULT_CHARS:
        payload = payload[:DB_QUERY_MAX_RESULT_CHARS] + "...(truncated)"

    if isinstance(rowcount, int) and rowcount > len(rows):
        summary = f"DB 조회 결과 {rowcount}건 중 {len(rows)}건만 보여줄게"
    else:
        summary = f"DB 조회 결과 {len(rows)}건"
    return f"{summary}\n```json\n{payload}\n```"


def _lookup_app_user_by_barcode(barcode: str) -> str:
    if not APP_USER_API_URL:
        raise RuntimeError("APP_USER_API_URL is empty")

    timeout_sec = max(1, APP_USER_API_TIMEOUT_SEC)
    query = parse.urlencode({"barcode": barcode})
    delimiter = "&" if "?" in APP_USER_API_URL else "?"
    endpoint = f"{APP_USER_API_URL}{delimiter}{query}"
    req = request.Request(url=endpoint, method="GET")
    try:
        with request.urlopen(req, timeout=timeout_sec) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"app-user API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"app-user API connection failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("app-user API returned invalid JSON") from exc

    users = payload.get("data")
    if not isinstance(users, list) or not users:
        return f"바코드 {barcode}로 조회된 유저가 없어"

    lines = [
        f"*바코드 조회 결과* :barcode: `{barcode}`",
        f"• 조회 건수: *{len(users)}건*",
    ]
    for user_index, user in enumerate(users, start=1):
        user_phone = _display_value(user.get("userPhoneNumber"), default="null")
        user_seq = _display_value(user.get("userSeq"), default="null")
        user_real_name = _display_value(user.get("userRealName"), default="null")
        lines.append("")
        lines.append(f"*user {user_index}*")
        lines.append(f"• `userPhoneNumber`: `{user_phone}`")
        lines.append(f"• `userSeq`: `{user_seq}`")
        lines.append(f"• `userRealName`: `{user_real_name}`")

        babies = user.get("babies")
        if not isinstance(babies, list) or not babies:
            lines.append("• `babies`: `[]`")
            continue

        for baby_index, baby in enumerate(babies, start=1):
            baby_seq = _display_value(baby.get("babySeq"), default="null")
            twin_key = _display_value(baby.get("twinKey"), default="null")
            twin_flag = _display_value(baby.get("twinFlag"), default="null")
            birth_date = _display_value(baby.get("birthDate"), default="null")
            baby_nickname = _display_value(baby.get("babyNickname"), default="null")
            lines.append(f"• `babies[{baby_index - 1}]`")
            lines.append(f"  - `babySeq`: `{baby_seq}`")
            lines.append(f"  - `twinKey`: `{twin_key}`")
            lines.append(f"  - `twinFlag`: `{twin_flag}`")
            lines.append(f"  - `birthDate`: `{birth_date}`")
            lines.append(f"  - `babyNickname`: `{baby_nickname}`")

    return "\n".join(lines)


def _ask_claude(client: Anthropic, question: str) -> str:
    result = client.messages.create(
        model=ANTHROPIC_MODEL,
        max_tokens=ANTHROPIC_MAX_TOKENS,
        system=COMMON_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": question}],
    )
    text_blocks = [
        block.text
        for block in result.content
        if getattr(block, "type", "") == "text"
    ]
    return "".join(text_blocks).strip()


def _ask_ollama(question: str) -> str:
    payload = {
        "model": OLLAMA_MODEL,
        "system": COMMON_SYSTEM_PROMPT,
        "prompt": question,
        "stream": False,
        "options": {
            "temperature": OLLAMA_TEMPERATURE,
        },
    }
    req = request.Request(
        url=f"{OLLAMA_BASE_URL}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=OLLAMA_TIMEOUT_SEC) as response:
            body = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Ollama API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Ollama API connection failed: {exc.reason}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Ollama API returned invalid JSON") from exc

    return str(data.get("response", "")).strip()


def create_app() -> App:
    _validate_tokens()
    app = App(token=SLACK_BOT_TOKEN, signing_secret=SLACK_SIGNING_SECRET)
    logger = logging.getLogger(__name__)
    claude_client = Anthropic(api_key=ANTHROPIC_API_KEY) if LLM_PROVIDER == "claude" else None
    s3_client: Any | None = None

    def _get_s3_client() -> Any:
        nonlocal s3_client
        if s3_client is None:
            s3_client = _build_s3_client()
        return s3_client

    @app.event("app_mention")
    def handle_app_mention(event: dict[str, Any], say, client) -> None:
        raw_text = event.get("text") or ""
        text = raw_text.lower()
        question = _extract_question(raw_text)
        user_id = event.get("user")
        channel_id = event.get("channel") or ""
        current_ts = event.get("ts") or ""
        thread_ts = event.get("thread_ts") or event.get("ts")
        logger.info("Received app_mention: user=%s text=%s", user_id, text)

        if "ping" in text:
            say(text=_format_reply_text(user_id, "pong-ec2"), thread_ts=thread_ts)
            logger.info("Responded with pong-ec2 in thread_ts=%s", thread_ts)
            return

        try:
            s3_request = _extract_s3_request(question)
        except ValueError as exc:
            say(
                text=_format_reply_text(user_id, f"S3 조회 요청 형식 오류: {exc}"),
                thread_ts=thread_ts,
            )
            return

        if s3_request is not None:
            if not S3_QUERY_ENABLED:
                say(
                    text=_format_reply_text(
                        user_id,
                        "S3 조회 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘",
                    ),
                    thread_ts=thread_ts,
                )
                return

            try:
                client_s3 = _get_s3_client()
                if s3_request["kind"] == "ultrasound":
                    result_text = _query_s3_ultrasound_by_barcode(
                        client_s3,
                        s3_request["barcode"],
                    )
                    logger.info(
                        "Responded with s3 ultrasound result in thread_ts=%s barcode=%s",
                        thread_ts,
                        s3_request["barcode"],
                    )
                else:
                    result_text = _query_s3_device_log(
                        client_s3,
                        s3_request["device_name"],
                        s3_request["log_date"],
                    )
                    logger.info(
                        "Responded with s3 log result in thread_ts=%s device=%s date=%s",
                        thread_ts,
                        s3_request["device_name"],
                        s3_request["log_date"],
                    )
                say(text=_format_reply_text(user_id, result_text), thread_ts=thread_ts)
            except (BotoCoreError, ClientError):
                logger.exception("S3 query failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "S3 조회 중 오류가 발생했어. 버킷 권한/리전/키 경로를 확인해줘",
                    ),
                    thread_ts=thread_ts,
                )
            except Exception:
                logger.exception("S3 query failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "S3 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        db_query = _extract_db_query(question)
        barcode = _extract_barcode(question)
        if _is_barcode_log_analysis_request(question, barcode):
            if not S3_QUERY_ENABLED:
                say(
                    text=_format_reply_text(
                        user_id,
                        "로그 분석 기능이 꺼져 있어. .env에서 S3_QUERY_ENABLED=true로 설정해줘",
                    ),
                    thread_ts=thread_ts,
                )
                return
            if not BOX_DB_HOST or not BOX_DB_USERNAME or not BOX_DB_PASSWORD or not BOX_DB_DATABASE:
                say(
                    text=_format_reply_text(
                        user_id,
                        "바코드 로그 분석을 위해 BOX DB 접속 정보(BOX_DB_*)가 필요해",
                    ),
                    thread_ts=thread_ts,
                )
                return

            try:
                log_date = _extract_log_date(question)
                analysis_mode = (
                    "error"
                    if _is_error_focused_request(question) and not _is_scan_focused_request(question)
                    else "scan"
                )
                if analysis_mode == "error":
                    result_text = _analyze_barcode_log_errors(
                        _get_s3_client(),
                        barcode or "",
                        log_date,
                    )
                else:
                    result_text = _analyze_barcode_log_scan_events(
                        _get_s3_client(),
                        barcode or "",
                        log_date,
                    )
                say(text=_format_reply_text(user_id, result_text), thread_ts=thread_ts)
                logger.info(
                    "Responded with barcode log analysis in thread_ts=%s barcode=%s date=%s mode=%s",
                    thread_ts,
                    barcode,
                    log_date,
                    analysis_mode,
                )
            except ValueError as exc:
                say(
                    text=_format_reply_text(
                        user_id,
                        f"로그 분석 요청 형식 오류: {exc}",
                    ),
                    thread_ts=thread_ts,
                )
            except (BotoCoreError, ClientError, pymysql.MySQLError):
                logger.exception("Barcode log analysis failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "바코드 로그 분석 중 오류가 발생했어. DB 연결/S3 권한/로그 경로를 확인해줘",
                    ),
                    thread_ts=thread_ts,
                )
            except Exception:
                logger.exception("Barcode log analysis failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "바코드 로그 분석 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        if _is_barcode_video_count_request(question, barcode):
            try:
                count_result = _query_recordings_count_by_barcode(barcode or "")
                say(text=_format_reply_text(user_id, count_result), thread_ts=thread_ts)
                logger.info(
                    "Responded with barcode video count in thread_ts=%s barcode=%s",
                    thread_ts,
                    barcode,
                )
            except (pymysql.MySQLError, RuntimeError):
                logger.exception("Barcode video count query failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "영상 개수 조회 중 오류가 발생했어. DB 연결 정보와 네트워크 상태를 확인해줘",
                    ),
                    thread_ts=thread_ts,
                )
            except Exception:
                logger.exception("Barcode video count query failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "영상 개수 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        if barcode and _should_lookup_barcode(question, barcode):
            if user_id in APP_USER_LOOKUP_ALLOWED_USER_IDS:
                try:
                    lookup_result = _lookup_app_user_by_barcode(barcode)
                    say(text=_format_reply_text(user_id, lookup_result), thread_ts=thread_ts)
                    logger.info(
                        "Responded with barcode lookup in thread_ts=%s barcode=%s",
                        thread_ts,
                        barcode,
                    )
                except Exception:
                    logger.exception("Barcode lookup failed")
                    say(
                        text=_format_reply_text(
                            user_id,
                            "바코드 조회 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                        ),
                        thread_ts=thread_ts,
                    )
                return
            if db_query is None:
                say(
                    text=f"보안 책임자 <@{DD_USER_ID}> 의 승인이 필요합니다.",
                    thread_ts=thread_ts,
                )
                logger.info(
                    "Rejected app-user barcode lookup for unauthorized user=%s barcode=%s",
                    user_id,
                    barcode,
                )
                return
            logger.info(
                "Skipped app-user barcode lookup for unauthorized user=%s barcode=%s",
                user_id,
                barcode,
            )

        if db_query is not None:
            if not DB_QUERY_ENABLED:
                say(
                    text=_format_reply_text(
                        user_id,
                        "DB 조회 기능이 꺼져 있어. .env에서 DB_QUERY_ENABLED=true로 설정해줘",
                    ),
                    thread_ts=thread_ts,
                )
                return

            try:
                safe_sql = _validate_readonly_sql(db_query)
                db_result = _query_db(safe_sql)
                say(text=_format_reply_text(user_id, db_result), thread_ts=thread_ts)
                logger.info("Responded with db query result in thread_ts=%s", thread_ts)
            except ValueError as exc:
                say(
                    text=_format_reply_text(user_id, f"DB 조회 요청 형식 오류: {exc}"),
                    thread_ts=thread_ts,
                )
            except pymysql.MySQLError:
                logger.exception("DB query failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "DB 조회 중 오류가 발생했어. 연결 정보와 네트워크 상태를 확인해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        if LLM_PROVIDER == "claude" and claude_client:
            if not question:
                say(
                    text=_format_reply_text(user_id, "질문 내용을 같이 보내줘"),
                    thread_ts=thread_ts,
                )
                return
            if user_id != MODEL_OWNER_USER_ID:
                say(
                    text=_format_reply_text(
                        user_id,
                        "Claude 질문은 현재 지정된 사용자만 사용할 수 있어",
                    ),
                    thread_ts=thread_ts,
                )
                logger.info("Rejected claude call for user=%s", user_id)
                return
            try:
                thread_context = _load_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                model_input = _build_model_input(question, thread_context)
                answer = _ask_claude(claude_client, model_input)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                say(text=_format_reply_text(user_id, answer), thread_ts=thread_ts)
                logger.info("Responded with claude answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Claude API call failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "AI 응답 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        if LLM_PROVIDER == "ollama":
            if not question:
                say(
                    text=_format_reply_text(user_id, "질문 내용을 같이 보내줘"),
                    thread_ts=thread_ts,
                )
                return
            try:
                thread_context = _load_thread_context(
                    client,
                    logger,
                    channel_id,
                    thread_ts,
                    current_ts,
                )
                model_input = _build_model_input(question, thread_context)
                answer = _ask_ollama(model_input)
                if not answer:
                    answer = "답변을 생성하지 못했어. 다시 질문해줘"
                say(text=_format_reply_text(user_id, answer), thread_ts=thread_ts)
                logger.info("Responded with ollama answer in thread_ts=%s", thread_ts)
            except Exception:
                logger.exception("Ollama API call failed")
                say(
                    text=_format_reply_text(
                        user_id,
                        "Ollama 응답 중 오류가 발생했어. 서버 연결 상태를 확인해줘",
                    ),
                    thread_ts=thread_ts,
                )
            return

        say(
            text=_format_reply_text(
                user_id,
                "현재는 ping, s3 조회, db 조회 또는 LLM 질문에 응답해",
            ),
            thread_ts=thread_ts,
        )

    @app.event("message")
    def handle_message_events(event: dict[str, Any]) -> None:
        logger.debug("Ignored message event subtype=%s", event.get("subtype"))

    return app


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    bolt_app = create_app()
    SocketModeHandler(bolt_app, SLACK_APP_TOKEN).start()
