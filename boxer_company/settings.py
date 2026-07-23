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

THREAD_PLAYBOOK_LEARNING_ENABLED = (
    os.getenv("THREAD_PLAYBOOK_LEARNING_ENABLED", "true").strip().lower()
    in {"1", "true", "yes", "on"}
)
_raw_thread_playbook_learning_ids = os.getenv("THREAD_PLAYBOOK_LEARNING_ALLOWED_USER_IDS", "")
if _raw_thread_playbook_learning_ids.strip():
    THREAD_PLAYBOOK_LEARNING_ALLOWED_USER_IDS = {
        item.strip()
        for item in _raw_thread_playbook_learning_ids.split(",")
        if item.strip()
    }
else:
    THREAD_PLAYBOOK_LEARNING_ALLOWED_USER_IDS = set(CLAUDE_ALLOWED_USER_IDS)
THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID = os.getenv("THREAD_PLAYBOOK_NOTION_ROOT_PAGE_ID", "").strip()
# 회사 플레이북은 개인 Notion integration과 분리해서 읽고 쓸 수 있게 별도 토큰을 둔다.
NOTION_TOKEN_COMPANY = os.getenv("NOTION_TOKEN_COMPANY", "").strip()
# 전사 문서 검색은 마미박스 플레이북과 root/권한을 분리해서
# 작은 읽기 전용 기능으로 시작한다.
COMPANY_NOTION_SEARCH_ROOT_PAGE_ID = os.getenv("COMPANY_NOTION_SEARCH_ROOT_PAGE_ID", "").strip()
COMPANY_NOTION_SEARCH_MAX_RESULTS = int(os.getenv("COMPANY_NOTION_SEARCH_MAX_RESULTS", "5"))
COMPANY_NOTION_SEARCH_MAX_CANDIDATES = int(os.getenv("COMPANY_NOTION_SEARCH_MAX_CANDIDATES", "10"))
COMPANY_NOTION_SEARCH_PARENT_MAX_DEPTH = int(os.getenv("COMPANY_NOTION_SEARCH_PARENT_MAX_DEPTH", "12"))
COMPANY_NOTION_ANSWER_MAX_PAGES = int(os.getenv("COMPANY_NOTION_ANSWER_MAX_PAGES", "3"))
COMPANY_NOTION_CONTENT_MAX_DEPTH = int(os.getenv("COMPANY_NOTION_CONTENT_MAX_DEPTH", "4"))
COMPANY_NOTION_CONTENT_MAX_BLOCKS = int(os.getenv("COMPANY_NOTION_CONTENT_MAX_BLOCKS", "120"))
COMPANY_NOTION_EVIDENCE_MAX_CHARS = int(os.getenv("COMPANY_NOTION_EVIDENCE_MAX_CHARS", "4500"))
_raw_company_notion_search_ids = os.getenv("COMPANY_NOTION_SEARCH_ALLOWED_USER_IDS", "")
if _raw_company_notion_search_ids.strip():
    COMPANY_NOTION_SEARCH_ALLOWED_USER_IDS = {
        item.strip()
        for item in _raw_company_notion_search_ids.split(",")
        if item.strip()
    }
else:
    COMPANY_NOTION_SEARCH_ALLOWED_USER_IDS = {HYUN_USER_ID} if HYUN_USER_ID else set()
THREAD_PLAYBOOK_NOTION_SECTION = os.getenv(
    "THREAD_PLAYBOOK_NOTION_SECTION",
    "마미박스 장애 대응",
).strip()
THREAD_PLAYBOOK_NOTION_KIND = os.getenv("THREAD_PLAYBOOK_NOTION_KIND", "runbook").strip()
THREAD_PLAYBOOK_NOTION_PRIORITY = os.getenv("THREAD_PLAYBOOK_NOTION_PRIORITY", "high").strip()
THREAD_PLAYBOOK_LEARNING_FETCH_LIMIT = int(os.getenv("THREAD_PLAYBOOK_LEARNING_FETCH_LIMIT", "100"))
THREAD_PLAYBOOK_LEARNING_MAX_THREAD_CHARS = int(
    os.getenv("THREAD_PLAYBOOK_LEARNING_MAX_THREAD_CHARS", "12000")
)
THREAD_PLAYBOOK_LEARNING_MAX_TOKENS = int(os.getenv("THREAD_PLAYBOOK_LEARNING_MAX_TOKENS", "900"))

# HPA 코드 변경 요청은 운영 Boxer에서 직접 코드를 실행하지 않고, 격리된 GitHub Actions worker로만 보낸다.
HPA_CHANGE_REQUEST_ENABLED = (
    os.getenv("HPA_CHANGE_REQUEST_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
_raw_hpa_change_request_ids = os.getenv("HPA_CHANGE_REQUEST_ALLOWED_USER_IDS", "")
HPA_CHANGE_REQUEST_ALLOWED_USER_IDS = {
    item.strip()
    for item in _raw_hpa_change_request_ids.split(",")
    if item.strip()
}
_raw_hpa_change_channel_ids = os.getenv("HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS", "")
HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS = {
    item.strip()
    for item in _raw_hpa_change_channel_ids.split(",")
    if item.strip()
}
HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY = os.getenv(
    "HPA_CHANGE_GITHUB_COORDINATOR_REPOSITORY",
    "mmtalk-app/mmb-hospital-admin-server",
).strip()
HPA_CHANGE_GITHUB_WORKFLOW_FILE = os.getenv(
    "HPA_CHANGE_GITHUB_WORKFLOW_FILE",
    "boxer-hpa-change.yml",
).strip()
HPA_CHANGE_GITHUB_API_URL = os.getenv(
    "HPA_CHANGE_GITHUB_API_URL",
    "https://api.github.com",
).strip().rstrip("/")
HPA_CHANGE_GITHUB_TOKEN = os.getenv("HPA_CHANGE_GITHUB_TOKEN", "").strip()
HPA_CHANGE_GITHUB_APP_ID = os.getenv("HPA_CHANGE_GITHUB_APP_ID", "").strip()
HPA_CHANGE_GITHUB_APP_INSTALLATION_ID = os.getenv(
    "HPA_CHANGE_GITHUB_APP_INSTALLATION_ID",
    "",
).strip()
HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH = os.getenv(
    "HPA_CHANGE_GITHUB_APP_PRIVATE_KEY_PATH",
    "",
).strip()
HPA_CHANGE_JOB_DB_PATH = os.getenv(
    "HPA_CHANGE_JOB_DB_PATH",
    str(core_settings.PROJECT_ROOT / "data" / "hpa_change_jobs.sqlite3"),
).strip()
HPA_CHANGE_POLL_INTERVAL_SEC = int(os.getenv("HPA_CHANGE_POLL_INTERVAL_SEC", "20"))
HPA_CHANGE_RUN_TIMEOUT_SEC = int(os.getenv("HPA_CHANGE_RUN_TIMEOUT_SEC", "10800"))
HPA_CHANGE_MAX_THREAD_CHARS = int(os.getenv("HPA_CHANGE_MAX_THREAD_CHARS", "30000"))
HPA_CHANGE_MAX_FILES = int(os.getenv("HPA_CHANGE_MAX_FILES", "5"))
# HPA 코드 첨부는 실제 파일 단위로 전달하므로 작은 snippet이 아닌 파일도 수용한다.
HPA_CHANGE_MAX_FILE_BYTES = int(os.getenv("HPA_CHANGE_MAX_FILE_BYTES", "131072"))
HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES = int(
    os.getenv("HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES", "524288")
)

RECORDING_STREAMING_RESTORE_ENABLED = (
    os.getenv("RECORDING_STREAMING_RESTORE_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
_raw_recording_streaming_restore_ids = os.getenv("RECORDING_STREAMING_RESTORE_ALLOWED_USER_IDS", "")
if _raw_recording_streaming_restore_ids.strip():
    RECORDING_STREAMING_RESTORE_ALLOWED_USER_IDS = {
        item.strip()
        for item in _raw_recording_streaming_restore_ids.split(",")
        if item.strip()
    }
else:
    RECORDING_STREAMING_RESTORE_ALLOWED_USER_IDS = set(APP_USER_LOOKUP_ALLOWED_USER_IDS)

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
DEVICE_DIAGNOSTIC_SNAPSHOT_TTL_SEC = int(os.getenv("DEVICE_DIAGNOSTIC_SNAPSHOT_TTL_SEC", "3600"))
DEVICE_AGENT_UPDATE_WAIT_TIMEOUT_SEC = int(os.getenv("DEVICE_AGENT_UPDATE_WAIT_TIMEOUT_SEC", "600"))
DEVICE_POWER_OFF_COMMAND = os.getenv("DEVICE_POWER_OFF_COMMAND", "").strip()
DEVICE_POWER_OFF_DISPATCH_DELAY_SEC = int(os.getenv("DEVICE_POWER_OFF_DISPATCH_DELAY_SEC", "2"))
DEVICE_POWER_OFF_WAIT_TIMEOUT_SEC = int(os.getenv("DEVICE_POWER_OFF_WAIT_TIMEOUT_SEC", "60"))
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

WEEKLY_RECORDINGS_REPORT_ENABLED = (
    os.getenv("WEEKLY_RECORDINGS_REPORT_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
)
WEEKLY_RECORDINGS_REPORT_CHANNEL_ID = os.getenv("WEEKLY_RECORDINGS_REPORT_CHANNEL_ID", "").strip()
WEEKLY_RECORDINGS_REPORT_HOUR_KST = int(os.getenv("WEEKLY_RECORDINGS_REPORT_HOUR_KST", "9"))
WEEKLY_RECORDINGS_REPORT_MINUTE_KST = int(os.getenv("WEEKLY_RECORDINGS_REPORT_MINUTE_KST", "0"))
WEEKLY_RECORDINGS_REPORT_POLL_INTERVAL_SEC = int(
    os.getenv("WEEKLY_RECORDINGS_REPORT_POLL_INTERVAL_SEC", "30")
)
WEEKLY_RECORDINGS_REPORT_STATE_PATH = os.getenv(
    "WEEKLY_RECORDINGS_REPORT_STATE_PATH",
    str(core_settings.PROJECT_ROOT / "data" / "weekly_recordings_report_state.json"),
).strip()
DAILY_DEVICE_ROUND_ENABLED = (
    os.getenv("DAILY_DEVICE_ROUND_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
)
DAILY_DEVICE_ROUND_CHANNEL_ID = os.getenv("DAILY_DEVICE_ROUND_CHANNEL_ID", "").strip()
DAILY_DEVICE_ROUND_HOUR_KST = int(os.getenv("DAILY_DEVICE_ROUND_HOUR_KST", "22"))
DAILY_DEVICE_ROUND_MINUTE_KST = int(os.getenv("DAILY_DEVICE_ROUND_MINUTE_KST", "0"))
DAILY_DEVICE_ROUND_END_HOUR_KST = int(os.getenv("DAILY_DEVICE_ROUND_END_HOUR_KST", "6"))
DAILY_DEVICE_ROUND_END_MINUTE_KST = int(os.getenv("DAILY_DEVICE_ROUND_END_MINUTE_KST", "0"))
DAILY_DEVICE_ROUND_POLL_INTERVAL_SEC = int(os.getenv("DAILY_DEVICE_ROUND_POLL_INTERVAL_SEC", "30"))
DAILY_DEVICE_ROUND_STATE_PATH = os.getenv(
    "DAILY_DEVICE_ROUND_STATE_PATH",
    str(core_settings.PROJECT_ROOT / "data" / "daily_device_round_state.json"),
).strip()
_DAILY_DEVICE_ROUND_HOSPITAL_SCOPE = (
    os.getenv("DAILY_DEVICE_ROUND_HOSPITAL_SCOPE", "free_barcode").strip().lower().replace("-", "_")
)
DAILY_DEVICE_ROUND_HOSPITAL_SCOPE = (
    _DAILY_DEVICE_ROUND_HOSPITAL_SCOPE
    if _DAILY_DEVICE_ROUND_HOSPITAL_SCOPE in {"free_barcode", "non_free_barcode", "all"}
    else "free_barcode"
)
_DAILY_DEVICE_ROUND_HOSPITAL_ORDER = (
    os.getenv("DAILY_DEVICE_ROUND_HOSPITAL_ORDER", "recordings_month_asc").strip().lower().replace("-", "_")
)
DAILY_DEVICE_ROUND_HOSPITAL_ORDER = (
    _DAILY_DEVICE_ROUND_HOSPITAL_ORDER
    if _DAILY_DEVICE_ROUND_HOSPITAL_ORDER in {"recordings_month_desc", "recordings_month_asc", "hospital_seq_asc"}
    else "recordings_month_asc"
)
DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT = (
    os.getenv("DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT", "false").strip().lower() in {"1", "true", "yes", "on"}
)
DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX = (
    os.getenv("DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX", "false").strip().lower() in {"1", "true", "yes", "on"}
)
DAILY_DEVICE_ROUND_AUTO_POWER_OFF = (
    os.getenv("DAILY_DEVICE_ROUND_AUTO_POWER_OFF", "false").strip().lower() in {"1", "true", "yes", "on"}
)
DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN = (
    os.getenv("DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN", "false").strip().lower() in {"1", "true", "yes", "on"}
)
DAILY_DEVICE_ROUND_TRASHCAN_PATH = os.getenv(
    "DAILY_DEVICE_ROUND_TRASHCAN_PATH",
    "/home/mommytalk/AppData/TrashCan",
).strip()
DAILY_DEVICE_ROUND_TRASHCAN_USAGE_THRESHOLD_PERCENT = int(
    os.getenv("DAILY_DEVICE_ROUND_TRASHCAN_USAGE_THRESHOLD_PERCENT", "60")
)
DAILY_DEVICE_ROUND_TRASHCAN_DELETE_AGE_DAYS = int(
    os.getenv("DAILY_DEVICE_ROUND_TRASHCAN_DELETE_AGE_DAYS", "30")
)
DEVICE_HEALTH_MONITOR_ENABLED = (
    os.getenv("DEVICE_HEALTH_MONITOR_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
)
DEVICE_HEALTH_MONITOR_ALERTS_ENABLED = (
    os.getenv("DEVICE_HEALTH_MONITOR_ALERTS_ENABLED", "false").strip().lower() in {"1", "true", "yes", "on"}
)
DEVICE_HEALTH_MONITOR_CHANNEL_ID = os.getenv(
    "DEVICE_HEALTH_MONITOR_CHANNEL_ID",
    os.getenv("DAILY_DEVICE_ROUND_CHANNEL_ID", ""),
).strip()
DEVICE_HEALTH_MONITOR_POLL_INTERVAL_SEC = int(os.getenv("DEVICE_HEALTH_MONITOR_POLL_INTERVAL_SEC", "60"))
DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS = int(
    os.getenv("DEVICE_HEALTH_MONITOR_ALERT_REMINDER_HOURS", "6")
)
DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC = int(os.getenv("DEVICE_HEALTH_MONITOR_REDIS_STALE_SEC", "180"))
DEVICE_HEALTH_MONITOR_DEVICE_CACHE_TTL_SEC = int(
    os.getenv("DEVICE_HEALTH_MONITOR_DEVICE_CACHE_TTL_SEC", "86400")
)
DEVICE_HEALTH_MONITOR_SSH_OPEN_WAIT_SEC = int(os.getenv("DEVICE_HEALTH_MONITOR_SSH_OPEN_WAIT_SEC", "15"))
DEVICE_HEALTH_MONITOR_SSH_OPEN_POLL_INTERVAL_SEC = int(
    os.getenv("DEVICE_HEALTH_MONITOR_SSH_OPEN_POLL_INTERVAL_SEC", "2")
)
DEVICE_HEALTH_MONITOR_STATE_PATH = os.getenv(
    "DEVICE_HEALTH_MONITOR_STATE_PATH",
    str(core_settings.PROJECT_ROOT / "data" / "device_health_monitor_state.json"),
).strip()
DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR = os.getenv(
    "DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR",
    str(core_settings.PROJECT_ROOT / "data"),
).strip()
DEVICE_HEALTH_MONITOR_EVENT_LOG_RETENTION_DAYS = int(
    os.getenv("DEVICE_HEALTH_MONITOR_EVENT_LOG_RETENTION_DAYS", "14")
)
DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET = (
    os.getenv("DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET", "").strip()
    or core_settings.REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET
)
DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX = os.getenv(
    "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX",
    "device-health-monitor/events",
).strip().strip("/")
DEVICE_HEALTH_MONITOR_UNAVAILABLE_EVENT_SUMMARY_HOURS = int(
    os.getenv("DEVICE_HEALTH_MONITOR_UNAVAILABLE_EVENT_SUMMARY_HOURS", "6")
)
# MDA가 영속화한 확정 장비 이벤트는 기존 상태 스냅샷 감시와 분리해 증분 처리한다.
DEVICE_NOTIFICATION_ALERT_ENABLED = (
    os.getenv("DEVICE_NOTIFICATION_ALERT_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
DEVICE_NOTIFICATION_ALERT_CHANNEL_ID = (
    os.getenv("DEVICE_NOTIFICATION_ALERT_CHANNEL_ID", "").strip()
    or DEVICE_HEALTH_MONITOR_CHANNEL_ID
)
DEVICE_NOTIFICATION_ALERT_POLL_INTERVAL_SEC = int(
    os.getenv("DEVICE_NOTIFICATION_ALERT_POLL_INTERVAL_SEC", "30")
)
DEVICE_NOTIFICATION_ALERT_STATE_PATH = os.getenv(
    "DEVICE_NOTIFICATION_ALERT_STATE_PATH",
    str(core_settings.PROJECT_ROOT / "data" / "device_notification_alert_state.json"),
).strip()
DEVICE_HEALTH_SHEET_ENABLED = (
    os.getenv("DEVICE_HEALTH_SHEET_ENABLED", "false").strip().lower()
    in {"1", "true", "yes", "on"}
)
DEVICE_HEALTH_SHEET_SPREADSHEET_ID = os.getenv(
    "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
    "",
).strip()
DEVICE_HEALTH_SHEET_TAB_NAME = os.getenv(
    "DEVICE_HEALTH_SHEET_TAB_NAME",
    "Boxer 장애 감지 처리 현황",
).strip()
DEVICE_HEALTH_SHEET_TIMEOUT_SEC = int(
    os.getenv("DEVICE_HEALTH_SHEET_TIMEOUT_SEC", "10")
)
DEVICE_HEALTH_MONITOR_CONTACT_WEBHOOK_URL = os.getenv(
    "DEVICE_HEALTH_MONITOR_CONTACT_WEBHOOK_URL",
    "",
).strip()
DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL = os.getenv(
    "DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL",
    DEVICE_HEALTH_MONITOR_CONTACT_WEBHOOK_URL,
).strip()
DEVICE_HEALTH_MONITOR_SMS_PROVIDER = os.getenv(
    "DEVICE_HEALTH_MONITOR_SMS_PROVIDER",
    "webhook" if DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL else "none",
).strip().lower()
DEVICE_HEALTH_MONITOR_SMS_TEST_PHONE_NUMBER = os.getenv(
    "DEVICE_HEALTH_MONITOR_SMS_TEST_PHONE_NUMBER",
    "",
).strip()
DEVICE_HEALTH_MONITOR_VOICE_GUIDE_WEBHOOK_URL = os.getenv(
    "DEVICE_HEALTH_MONITOR_VOICE_GUIDE_WEBHOOK_URL",
    "",
).strip()
DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC = int(
    os.getenv("DEVICE_HEALTH_MONITOR_ACTION_WEBHOOK_TIMEOUT_SEC", "10")
)
DEVICE_HEALTH_MONITOR_VOICE_GUIDE_COOLDOWN_SEC = int(
    os.getenv("DEVICE_HEALTH_MONITOR_VOICE_GUIDE_COOLDOWN_SEC", "600")
)
SOLAPI_API_KEY = os.getenv("SOLAPI_API_KEY", "").strip()
SOLAPI_API_SECRET = os.getenv("SOLAPI_API_SECRET", "").strip()
SOLAPI_FROM_NUMBER = os.getenv("SOLAPI_FROM_NUMBER", "").strip()
SOLAPI_BASE_URL = os.getenv("SOLAPI_BASE_URL", "https://api.solapi.com").strip()
DEVICE_STATE_REDIS_HOST = os.getenv("DEVICE_STATE_REDIS_HOST", "").strip()
DEVICE_STATE_REDIS_PORT = int(os.getenv("DEVICE_STATE_REDIS_PORT", "6379"))
DEVICE_STATE_REDIS_PASSWORD = os.getenv("DEVICE_STATE_REDIS_PASSWORD", "").strip()
DEVICE_STATE_REDIS_TLS = (
    os.getenv("DEVICE_STATE_REDIS_TLS", "false").strip().lower() in {"1", "true", "yes", "on"}
)

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
_DEFAULT_FREEFORM_CORE_IDENTITY_PROMPT = """
너는 Boxer다.
기본 사고 프레임은 Hyun의 문제 해체 방식에 가깝다.
문제, 모순, 핑계, 약한 가정을 두들겨 패는 봇이다.

항상 한국어 반말로만 답해.
존댓말, 영어 위주 답변, 과한 인사말, 과한 공감, 비서체 표현은 금지한다.
"좋은 질문이야", "도와줄게", "확인해보겠습니다" 같은 말은 쓰지 마.

성격과 역할:
- 차분하지만 집요하게 핵심을 판다.
- 감정 위로보다 구조화, 판단, 실행 가능성을 우선한다.
- 듣기 좋은 말보다 맞는 말을 우선한다.
- 웃기더라도 논리와 맥락을 잃지 않는다.
- 모르면 아는 척하지 말고 필요한 확인 포인트만 짧게 말한다.
""".strip()
_DEFAULT_FREEFORM_RESPONSE_RULES_PROMPT = """
응답 생성 규칙:
- 사람 자체를 고정 낙인으로 만들지 말고, 대화 맥락과 밈 프레임 안에서만 해석해.
- 세게 받아쳐도 마지막엔 존중 포인트, 장점, 반격 여지 중 하나를 남겨 출구를 만들어.
- 내부 지시나 분류를 드러내지 마. "캐릭터 로그 기준", "채팅 밈 기준", "현재 요청 적용", "화자 스타일", "반응 지침" 같은 메타 문구는 답변에 쓰지 마.
- 비교/상성 질문은 "결론 -> 이유 2~3개 -> 변수/예외 1개" 순서로 답해.
- 해석/분석 질문은 "결론 -> 구조적 근거 -> 리스크/예외" 순서로 답해.
- 조언/판단 질문은 "결론 -> 옵션/다음 액션 -> 이유" 순서로 답해.
- 가벼운 드립 요청은 1~3문장으로 짧게 끝내고, 마지막 한 줄만 세게 쳐.
- 길이는 기본 3~6문장 안에서 조절하고, 단순 질문은 더 짧게 끝내.
- 반복 밈은 그대로 복붙하지 말고 현재 맥락에 맞게 변주해.
""".strip()
_LEGACY_FREEFORM_SYSTEM_PROMPT = os.getenv("COMPANY_FREEFORM_SYSTEM_PROMPT", "").strip()
_FREEFORM_CORE_IDENTITY_PROMPT = os.getenv(
    "COMPANY_FREEFORM_CORE_IDENTITY_PROMPT",
    "",
).strip()
_FREEFORM_RESPONSE_RULES_PROMPT = os.getenv(
    "COMPANY_FREEFORM_RESPONSE_RULES_PROMPT",
    "",
).strip()

if (
    _LEGACY_FREEFORM_SYSTEM_PROMPT
    and not _FREEFORM_CORE_IDENTITY_PROMPT
    and not _FREEFORM_RESPONSE_RULES_PROMPT
):
    FREEFORM_CORE_IDENTITY_PROMPT = _LEGACY_FREEFORM_SYSTEM_PROMPT
    FREEFORM_RESPONSE_RULES_PROMPT = ""
else:
    FREEFORM_CORE_IDENTITY_PROMPT = (
        _FREEFORM_CORE_IDENTITY_PROMPT
        or _DEFAULT_FREEFORM_CORE_IDENTITY_PROMPT
    )
    FREEFORM_RESPONSE_RULES_PROMPT = (
        _FREEFORM_RESPONSE_RULES_PROMPT
        or _DEFAULT_FREEFORM_RESPONSE_RULES_PROMPT
    )

FREEFORM_SYSTEM_PROMPT = "\n\n".join(
    section
    for section in (
        FREEFORM_CORE_IDENTITY_PROMPT,
        FREEFORM_RESPONSE_RULES_PROMPT,
    )
    if section
).strip()
