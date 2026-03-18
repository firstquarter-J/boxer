import os
import re
from pathlib import Path

from boxer.core import settings as core_settings

HYUN_USER_ID = os.getenv("HYUN_USER_ID", "").strip()
MARK_USER_ID = os.getenv("MARK_USER_ID", "").strip()
DD_USER_ID = os.getenv("DD_USER_ID", "").strip()
JUNE_USER_ID = os.getenv("JUNE_USER_ID", "").strip()
JUNO_USER_ID = os.getenv("JUNO_USER_ID", "").strip()
ROY_USER_ID = os.getenv("ROY_USER_ID", "").strip()
MARU_USER_ID = os.getenv("MARU_USER_ID", "").strip()
PAUL_USER_ID = os.getenv("PAUL_USER_ID", "").strip()
DANNY_USER_ID = os.getenv("DANNY_USER_ID", "").strip()
LUKA_USER_ID = os.getenv("LUKA_USER_ID", "").strip()
OLIVIA_USER_ID = os.getenv("OLIVIA_USER_ID", "").strip()
SAGE_USER_ID = os.getenv("SAGE_USER_ID", "").strip()
_raw_claude_allowed_ids = os.getenv("CLAUDE_ALLOWED_USER_IDS", "")
CLAUDE_ALLOWED_USER_IDS = {
    item.strip()
    for item in _raw_claude_allowed_ids.split(",")
    if item.strip()
}

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

_raw_request_log_query_ids = os.getenv("REQUEST_LOG_QUERY_ALLOWED_USER_IDS", "")
if _raw_request_log_query_ids.strip():
    REQUEST_LOG_QUERY_ALLOWED_USER_IDS = {
        item.strip()
        for item in _raw_request_log_query_ids.split(",")
        if item.strip()
    }
else:
    REQUEST_LOG_QUERY_ALLOWED_USER_IDS = set(APP_USER_LOOKUP_ALLOWED_USER_IDS)

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
DEVICE_FILE_TEMP_DIR = os.getenv("DEVICE_FILE_TEMP_DIR", "/tmp/boxer-device-files").strip()
DEVICE_FILE_TEMP_RETENTION_SEC = int(os.getenv("DEVICE_FILE_TEMP_RETENTION_SEC", "86400"))
DEVICE_FILE_SEARCH_PATHS = [
    item.strip()
    for item in os.getenv(
    "DEVICE_FILE_SEARCH_PATHS",
    "/home/mommytalk/AppData/Videos,/home/mommytalk/AppData/TrashCan",
    ).split(",")
    if item.strip()
]
DEVICE_FILE_RECOVERY_ENABLED = (
    os.getenv("DEVICE_FILE_RECOVERY_ENABLED", "false").strip().lower() == "true"
)
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

DEVICE_FILE_DOWNLOAD_BUCKET = (
    os.getenv("DEVICE_FILE_DOWNLOAD_BUCKET", "").strip()
    or core_settings.S3_ULTRASOUND_BUCKET
)
DEVICE_FILE_DOWNLOAD_PREFIX = os.getenv("DEVICE_FILE_DOWNLOAD_PREFIX", "temp").strip().strip("/")
DEVICE_FILE_DOWNLOAD_PRESIGNED_EXPIRES_SEC = int(
    os.getenv("DEVICE_FILE_DOWNLOAD_PRESIGNED_EXPIRES_SEC", "3600")
)
BABY_MAGIC_CDN_BASE_URL = os.getenv(
    "BABY_MAGIC_CDN_BASE_URL",
    "https://cdn-kr.mmtalkbox.com/",
).strip().rstrip("/")

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
BARCODE_LOG_ERROR_SUMMARY_MAX_TOKENS = int(
    os.getenv("BARCODE_LOG_ERROR_SUMMARY_MAX_TOKENS", "1200")
)
RECORDING_FAILURE_ANALYSIS_MAX_TOKENS = int(
    os.getenv("RECORDING_FAILURE_ANALYSIS_MAX_TOKENS", "1200")
)

_LEGACY_SYSTEM_PROMPT = os.getenv("COMPANY_SYSTEM_PROMPT", "").strip()
RETRIEVAL_SYSTEM_PROMPT = (
    os.getenv("COMPANY_RETRIEVAL_SYSTEM_PROMPT", "").strip()
    or _LEGACY_SYSTEM_PROMPT
)
_DEFAULT_FREEFORM_SYSTEM_PROMPT = """
너는 Hyun처럼 답하는 AI Assistant다.

항상 한국어 반말로만 답해.
존댓말, 영어 위주 답변, 과한 인사말, 과한 공감, 비서체 표현은 금지한다.
"좋은 질문이야", "도와줄게", "확인해보겠습니다" 같은 말은 쓰지 마.

너의 역할은 감정 공감형 챗봇이 아니라 냉정하고 이성적인 판단 보조자다.
구조 > 감정
논리 > 위로
명확성 > 친절함
실행 가능성 > 그럴듯함

답변 원칙:
- 기술/업무/판단 질문은 기본적으로 "결론 -> 방법 -> 이유" 순서로 답해.
- 선택이 필요한 질문은 "옵션 -> 장단점 -> 추천 기준"으로 정리해.
- 문제 해결 질문은 "결론 -> 원인 가설 -> 확인 포인트 -> 다음 액션" 순서로 답해.
- 복잡한 문제는 구조화하고, 단순한 질문은 짧게 끝내.
- trade-off와 리스크는 숨기지 말고 같이 말해.
- 모르면 아는 척하지 말고, 필요한 정보만 짧게 요청해.
- 듣기 좋은 말보다 맞는 말을 우선해.
- 상담가처럼 말하지 말고, 냉정한 엔지니어처럼 답해.
""".strip()
FREEFORM_SYSTEM_PROMPT = (
    os.getenv("COMPANY_FREEFORM_SYSTEM_PROMPT", "").strip()
    or _DEFAULT_FREEFORM_SYSTEM_PROMPT
)
