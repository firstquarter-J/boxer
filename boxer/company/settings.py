import os
import re
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

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

MDA_GRAPHQL_URL = os.getenv("MDA_GRAPHQL_URL", "").strip()
MDA_ADMIN_USER_PASSWORD = os.getenv("MDA_ADMIN_USER_PASSWORD", "").strip()
MDA_SSH_OPEN_HOST = os.getenv("MDA_SSH_OPEN_HOST", "remotes.mmtalkbox.com").strip()
MDA_GRAPHQL_ORIGIN = os.getenv("MDA_GRAPHQL_ORIGIN", "https://mda.kr.mmtalkbox.com").strip()
MDA_GRAPHQL_REFERER = os.getenv("MDA_GRAPHQL_REFERER", "https://mda.kr.mmtalkbox.com/").strip()
MDA_GRAPHQL_USER_AGENT = os.getenv(
    "MDA_GRAPHQL_USER_AGENT",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
).strip()
MDA_API_TIMEOUT_SEC = int(os.getenv("MDA_API_TIMEOUT_SEC", "10"))
MDA_SSH_POLL_INTERVAL_SEC = int(os.getenv("MDA_SSH_POLL_INTERVAL_SEC", "2"))
MDA_SSH_POLL_TIMEOUT_SEC = int(os.getenv("MDA_SSH_POLL_TIMEOUT_SEC", "60"))
MDA_SSH_POLL_RESEND_EVERY = int(os.getenv("MDA_SSH_POLL_RESEND_EVERY", "5"))

DEVICE_SSH_USER = os.getenv("DEVICE_SSH_USER", "mommytalk").strip()
DEVICE_SSH_PASSWORD = os.getenv("DEVICE_SSH_PASSWORD", "").strip()
DEVICE_SSH_CONNECT_TIMEOUT_SEC = int(os.getenv("DEVICE_SSH_CONNECT_TIMEOUT_SEC", "8"))
DEVICE_SSH_COMMAND_TIMEOUT_SEC = int(os.getenv("DEVICE_SSH_COMMAND_TIMEOUT_SEC", "20"))
DEVICE_FILE_SEARCH_PATHS = [
    item.strip()
    for item in os.getenv(
    "DEVICE_FILE_SEARCH_PATHS",
    "/home/mommytalk/AppData/Videos,/home/mommytalk/AppData/TrashCan",
    ).split(",")
    if item.strip()
]
BOX_UPLOADER_BASE_URL = os.getenv(
    "BOX_UPLOADER_BASE_URL",
    "https://stream.kr.mmtalkbox.com",
).strip().rstrip("/")
BOX_UPLOADER_RECORDING_PATH = os.getenv(
    "BOX_UPLOADER_RECORDING_PATH",
    "/recording/upload-v4",
).strip()
BOX_UPLOADER_TIMEOUT_SEC = int(os.getenv("BOX_UPLOADER_TIMEOUT_SEC", "120"))
UPLOADER_JWT_SECRET = os.getenv("UPLOADER_JWT_SECRET", "").strip()

_raw_device_file_probe_ids = os.getenv("DEVICE_FILE_PROBE_ALLOWED_USER_IDS", "")
if _raw_device_file_probe_ids.strip():
    DEVICE_FILE_PROBE_ALLOWED_USER_IDS = {
        item.strip()
        for item in _raw_device_file_probe_ids.split(",")
        if item.strip()
    }
else:
    DEVICE_FILE_PROBE_ALLOWED_USER_IDS = {
        user_id
        for user_id in (HYUN_USER_ID, MARK_USER_ID)
        if user_id
    }

MOMMYBOX_REFERENCE_ROOT = os.getenv(
    "MOMMYBOX_REFERENCE_ROOT",
    "/home/ec2-user/reference-repos/mmb-mommybox-v2",
).strip()
MOMMYBOX_REF_V211300_PATH = os.getenv(
    "MOMMYBOX_REF_V211300_PATH",
    str(Path(MOMMYBOX_REFERENCE_ROOT) / "v2.11.300"),
).strip()
MOMMYBOX_REF_LEGACY_PATH = os.getenv(
    "MOMMYBOX_REF_LEGACY_PATH",
    str(Path(MOMMYBOX_REFERENCE_ROOT) / "legacy"),
).strip()

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
