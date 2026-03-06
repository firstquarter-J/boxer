import os
import re

HYUN_USER_ID = os.getenv("HYUN_USER_ID", "").strip()
MARK_USER_ID = os.getenv("MARK_USER_ID", "").strip()
DD_USER_ID = os.getenv("DD_USER_ID", "").strip()

_raw_lookup_ids = os.getenv("APP_USER_LOOKUP_ALLOWED_USER_IDS", "")
if _raw_lookup_ids.strip():
    APP_USER_LOOKUP_ALLOWED_USER_IDS = {
        item.strip()
        for item in _raw_lookup_ids.split(",")
        if item.strip()
    }
else:
    APP_USER_LOOKUP_ALLOWED_USER_IDS = {
        user_id
        for user_id in (HYUN_USER_ID, MARK_USER_ID)
        if user_id
    }

APP_USER_API_URL = os.getenv("APP_USER_API_URL", "").strip()
APP_USER_API_TIMEOUT_SEC = int(os.getenv("APP_USER_API_TIMEOUT_SEC", "8"))


def apply_legacy_db_compat(core_settings: object) -> None:
    legacy_host = os.getenv("BOX_DB_HOST", "").strip()
    legacy_port = os.getenv("BOX_DB_PORT", "").strip()
    legacy_username = os.getenv("BOX_DB_USERNAME", "").strip()
    legacy_password = os.getenv("BOX_DB_PASSWORD", "").strip()
    legacy_database = os.getenv("BOX_DB_DATABASE", "").strip()

    if not getattr(core_settings, "DB_HOST", "") and legacy_host:
        setattr(core_settings, "DB_HOST", legacy_host)
    if not getattr(core_settings, "DB_USERNAME", "") and legacy_username:
        setattr(core_settings, "DB_USERNAME", legacy_username)
    if not getattr(core_settings, "DB_PASSWORD", "") and legacy_password:
        setattr(core_settings, "DB_PASSWORD", legacy_password)
    if not getattr(core_settings, "DB_DATABASE", "") and legacy_database:
        setattr(core_settings, "DB_DATABASE", legacy_database)

    db_port_env = os.getenv("DB_PORT", "").strip()
    if not db_port_env and legacy_port:
        try:
            parsed = int(legacy_port)
            if parsed > 0:
                setattr(core_settings, "DB_PORT", parsed)
        except ValueError:
            return

BARCODE_PATTERN = re.compile(r"(?<!\d)(\d{11})(?!\d)")
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

SCANNED_TOKEN_PATTERN = re.compile(r"Scanned\s*:\s*([^\s]+)", re.IGNORECASE)
LOG_LINE_TIME_PATTERN = re.compile(
    r"(?<!\d)(\d{1,2}:\d{2}:\d{2})(?:[.,]\d{1,6})?(?!\d)"
)
SCAN_CODE_LABELS: dict[str, str] = {
    "C_STOPSESS": "녹화 중지",
    "SPECIAL_RECORD_START_STOP": "녹화 시작/종료",
    "C_PAUSE": "일시정지",
    "C_RESUME": "재개",
    "C_CCLREC": "녹화 취소",
    "SPECIAL_TAKE_SNAP": "캡처/스냅샷",
}
SESSION_STOP_TOKENS = {"C_STOPSESS", "SPECIAL_RECORD_START_STOP"}

VIDEO_HINT_TOKENS = ("영상", "비디오", "동영상", "recording")
VIDEO_COUNT_HINT_TOKENS = ("몇 개", "몇개", "개수", "갯수", "수", "count")

LOG_ANALYSIS_MAX_DEVICES = int(os.getenv("LOG_ANALYSIS_MAX_DEVICES", "8"))
LOG_ANALYSIS_MAX_SAMPLES = int(os.getenv("LOG_ANALYSIS_MAX_SAMPLES", "5"))
LOG_SCAN_MAX_EVENTS = int(os.getenv("LOG_SCAN_MAX_EVENTS", "50"))
LOG_SESSION_SAFETY_LINES = int(os.getenv("LOG_SESSION_SAFETY_LINES", "20"))
LOG_POST_STOP_MAX_LINES = int(os.getenv("LOG_POST_STOP_MAX_LINES", "50"))
LOG_PHASE1_MAX_DAYS = int(os.getenv("LOG_PHASE1_MAX_DAYS", "30"))
RECORDINGS_CONTEXT_LIMIT = int(os.getenv("RECORDINGS_CONTEXT_LIMIT", "30"))

SYSTEM_PROMPT = os.getenv("COMPANY_SYSTEM_PROMPT", "").strip()
