from __future__ import annotations

from dataclasses import dataclass, field
import json
import os
from pathlib import Path
import re
import stat
import tempfile
from typing import Any, Mapping
from urllib.parse import urlsplit


_DEFAULT_HOST = "127.0.0.1"
_DEFAULT_PORT = 8010
_DEFAULT_AUTOMATION_STATE_PATH = (
    "/var/lib/boxer-company-api/automation_state.json"
)
_DEFAULT_SMS_DELIVERY_OUTBOX_PATH = (
    "/var/lib/boxer-company-api/sms_delivery_outbox.json"
)
_DEFAULT_REQUEST_LOG_SQLITE_PATH = (
    "/var/lib/boxer-company-api/request_log.db"
)
_DEFAULT_DEVICE_HEALTH_EVENT_LOG_DIR = (
    "/var/lib/boxer-company-api/device-health-events"
)
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})
_AUTOMATION_FEATURE_FLAGS = (
    "WEEKLY_RECORDINGS_REPORT_ENABLED",
    "DAILY_DEVICE_ROUND_ENABLED",
    "DEVICE_HEALTH_MONITOR_ENABLED",
    "DEVICE_NOTIFICATION_ALERT_ENABLED",
    "SMS_DELIVERY_REPORTER_ENABLED",
)
_ALL_AUTOMATION_CYCLES = frozenset(
    {
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    }
)
_MAX_ROOT_OWNED_CONFIG_BYTES = 4 * 1024 * 1024
_CALLER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._~+/=-]{32,512}$")
_SCOPED_ID_PATTERN = re.compile(
    r"^(?:\*|[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255})$"
)
_CAPABILITY_PATTERN = re.compile(
    r"^[a-z][a-z0-9._:-]{0,127}$"
)
_ALLOWED_CHANNELS = frozenset({"slack", "web", "api"})
_REQUIRED_TURN_CAPABILITY = "assistant.turn.read"
_AUTOMATION_SLACK_CAPABILITIES = frozenset(
    {
        "assistant.turn.read",
        "assistant.device.probe",
        "assistant.device.ssh.open",
        "assistant.operation.execute",
        "assistant.device.alert.execute",
        "assistant.automation.execute",
    }
)
_LIVE_DEVICE_SLACK_CAPABILITIES = frozenset(
    {
        "assistant.turn.read",
        "assistant.device.probe",
        "assistant.device.ssh.open",
    }
)
_OPERATIONS_SLACK_CAPABILITIES = frozenset(
    {
        "assistant.turn.read",
        "assistant.operation.execute",
        "assistant.device.alert.execute",
    }
)


@dataclass(frozen=True, slots=True)
class CompanyApiCallerSettings:
    """서버 설정에서만 만들어지는 내부 API 호출자 권한이다."""

    caller_id: str
    token: str = field(repr=False)
    tenant_ids: frozenset[str]
    channels: frozenset[str]
    actor_ids: frozenset[str]
    allow_anonymous_actor: bool
    capabilities: frozenset[str]


@dataclass(frozen=True, slots=True)
class CompanyApiSettings:
    host: str
    port: int
    callers: tuple[CompanyApiCallerSettings, ...]
    configuration_error: str | None = None
    automation_state_path: str = _DEFAULT_AUTOMATION_STATE_PATH
    sms_delivery_outbox_path: str = _DEFAULT_SMS_DELIVERY_OUTBOX_PATH
    # 직접 조립하는 단위 테스트는 주입한 runtime만 검증한다. 운영 env를
    # 읽은 설정만 로컬 durable storage와 root-owned 파일을 readiness에 묶는다.
    enforce_local_readiness: bool = False
    automation_storage_required: bool = False
    sms_delivery_storage_required: bool = False
    device_health_sheet_enabled: bool = False
    google_application_credentials_path: str = ""
    # 직접 주입하는 계약 테스트는 기존 live route를 유지한다. 운영 env loader는
    # 명시적으로 true를 받기 전까지 이 경계를 false로 덮어쓴다.
    live_device_enabled: bool = True
    operations_enabled: bool = True
    request_log_enabled: bool = False
    request_log_path: str = _DEFAULT_REQUEST_LOG_SQLITE_PATH
    # 직접 주입하는 테스트 app은 기존 endpoint 계약을 유지하고, env loader로
    # 만든 운영 설정만 실제 reporter flag에 맞춘 cycle 집합을 전달한다.
    automation_enabled_cycles: frozenset[str] = _ALL_AUTOMATION_CYCLES

    @property
    def authentication_configured(self) -> bool:
        return bool(self.callers) and self.configuration_error is None


def load_company_api_settings(
    env: Mapping[str, str] | None = None,
) -> CompanyApiSettings:
    """환경변수의 caller registry를 검증하고 오류 상세 없이 fail closed한다."""

    source = env if env is not None else os.environ
    live_device_enabled, live_device_error = _load_live_device_enabled(source)
    operations_enabled, operations_error = _load_operations_enabled(source)
    (
        request_log_enabled,
        request_log_path,
        request_log_error,
    ) = _load_request_log_settings(
        source,
        required=live_device_enabled or operations_enabled,
    )
    operations_dependency_error = _load_operations_dependency_settings(
        source,
        enabled=operations_enabled,
        live_device_enabled=live_device_enabled,
    )
    automation_enabled_cycles = _automation_enabled_cycles(source)
    host, port, server_error = _load_server_settings(source)
    automation_storage_required = bool(automation_enabled_cycles)
    sms_delivery_storage_required = _sms_delivery_storage_required(source)
    automation_state_path, automation_error = (
        _load_automation_state_path(source)
        if automation_storage_required
        else (_DEFAULT_AUTOMATION_STATE_PATH, None)
    )
    (
        sms_delivery_outbox_path,
        device_health_sheet_enabled,
        google_application_credentials_path,
        automation_dependency_error,
    ) = _load_automation_dependency_settings(
        source,
        live_device_enabled=live_device_enabled,
    )
    enforce_local_readiness = bool(
        request_log_enabled
        or automation_storage_required
        or sms_delivery_storage_required
        or device_health_sheet_enabled
    )
    raw_registry = str(
        source.get("BOXER_COMPANY_API_CALLERS_JSON", "")
    ).strip()
    if not raw_registry:
        return CompanyApiSettings(
            host=host,
            port=port,
            callers=(),
            configuration_error=(
                server_error
                or live_device_error
                or operations_error
                or request_log_error
                or operations_dependency_error
                or automation_error
                or automation_dependency_error
                or "caller_registry_missing"
            ),
            automation_state_path=automation_state_path,
            sms_delivery_outbox_path=sms_delivery_outbox_path,
            enforce_local_readiness=enforce_local_readiness,
            automation_storage_required=automation_storage_required,
            sms_delivery_storage_required=sms_delivery_storage_required,
            device_health_sheet_enabled=device_health_sheet_enabled,
            google_application_credentials_path=(
                google_application_credentials_path
            ),
            live_device_enabled=live_device_enabled,
            operations_enabled=operations_enabled,
            request_log_enabled=request_log_enabled,
            request_log_path=request_log_path,
            automation_enabled_cycles=automation_enabled_cycles,
        )

    try:
        parsed = json.loads(raw_registry)
        callers = _parse_caller_registry(parsed)
    except (TypeError, ValueError, json.JSONDecodeError):
        return CompanyApiSettings(
            host=host,
            port=port,
            callers=(),
            configuration_error=(
                server_error
                or live_device_error
                or operations_error
                or request_log_error
                or operations_dependency_error
                or automation_error
                or automation_dependency_error
                or "caller_registry_invalid"
            ),
            automation_state_path=automation_state_path,
            sms_delivery_outbox_path=sms_delivery_outbox_path,
            enforce_local_readiness=enforce_local_readiness,
            automation_storage_required=automation_storage_required,
            sms_delivery_storage_required=sms_delivery_storage_required,
            device_health_sheet_enabled=device_health_sheet_enabled,
            google_application_credentials_path=(
                google_application_credentials_path
            ),
            live_device_enabled=live_device_enabled,
            operations_enabled=operations_enabled,
            request_log_enabled=request_log_enabled,
            request_log_path=request_log_path,
            automation_enabled_cycles=automation_enabled_cycles,
        )

    automation_caller_error = (
        _validate_automation_slack_caller(callers)
        if automation_storage_required
        else None
    )
    live_device_caller_error = (
        _validate_live_device_slack_caller(callers)
        if live_device_enabled
        else None
    )
    operations_caller_error = (
        _validate_operations_slack_caller(callers)
        if operations_enabled
        else None
    )

    if (
        server_error is not None
        or live_device_error is not None
        or operations_error is not None
        or request_log_error is not None
        or operations_dependency_error is not None
        or automation_error is not None
        or automation_dependency_error is not None
        or automation_caller_error is not None
        or live_device_caller_error is not None
        or operations_caller_error is not None
    ):
        return CompanyApiSettings(
            host=host,
            port=port,
            callers=(),
            configuration_error=(
                server_error
                or live_device_error
                or operations_error
                or request_log_error
                or operations_dependency_error
                or automation_error
                or automation_dependency_error
                or automation_caller_error
                or live_device_caller_error
                or operations_caller_error
            ),
            automation_state_path=automation_state_path,
            sms_delivery_outbox_path=sms_delivery_outbox_path,
            enforce_local_readiness=enforce_local_readiness,
            automation_storage_required=automation_storage_required,
            sms_delivery_storage_required=sms_delivery_storage_required,
            device_health_sheet_enabled=device_health_sheet_enabled,
            google_application_credentials_path=(
                google_application_credentials_path
            ),
            live_device_enabled=live_device_enabled,
            operations_enabled=operations_enabled,
            request_log_enabled=request_log_enabled,
            request_log_path=request_log_path,
            automation_enabled_cycles=automation_enabled_cycles,
        )
    return CompanyApiSettings(
        host=host,
        port=port,
        callers=callers,
        automation_state_path=automation_state_path,
        sms_delivery_outbox_path=sms_delivery_outbox_path,
        enforce_local_readiness=enforce_local_readiness,
        automation_storage_required=automation_storage_required,
        sms_delivery_storage_required=sms_delivery_storage_required,
        device_health_sheet_enabled=device_health_sheet_enabled,
        google_application_credentials_path=(
            google_application_credentials_path
        ),
        live_device_enabled=live_device_enabled,
        operations_enabled=operations_enabled,
        request_log_enabled=request_log_enabled,
        request_log_path=request_log_path,
        automation_enabled_cycles=automation_enabled_cycles,
    )


def _load_server_settings(
    env: Mapping[str, str],
) -> tuple[str, int, str | None]:
    host = str(
        env.get("BOXER_COMPANY_API_HOST", _DEFAULT_HOST)
    ).strip()
    raw_port = str(
        env.get("BOXER_COMPANY_API_PORT", _DEFAULT_PORT)
    ).strip()
    try:
        port = int(raw_port)
    except (TypeError, ValueError):
        return _DEFAULT_HOST, _DEFAULT_PORT, "server_configuration_invalid"
    if not host or len(host) > 255 or not 1 <= port <= 65535:
        return _DEFAULT_HOST, _DEFAULT_PORT, "server_configuration_invalid"
    return host, port, None


def _load_automation_state_path(
    env: Mapping[str, str],
) -> tuple[str, str | None]:
    raw_value = str(
        env.get(
            "BOXER_COMPANY_API_AUTOMATION_STATE_PATH",
            _DEFAULT_AUTOMATION_STATE_PATH,
        )
    ).strip()
    path = Path(raw_value).expanduser()
    if (
        not raw_value
        or not path.is_absolute()
        or path == Path("/")
        or raw_value.endswith("/")
        or str(path) != _DEFAULT_AUTOMATION_STATE_PATH
    ):
        return _DEFAULT_AUTOMATION_STATE_PATH, (
            "automation_state_path_invalid"
        )
    return str(path), None


def _load_live_device_enabled(
    env: Mapping[str, str],
) -> tuple[bool, str | None]:
    """API live 장비 경계는 명시적인 true에서만 활성화한다."""

    raw_value = str(
        env.get("BOXER_COMPANY_API_LIVE_DEVICE_ENABLED", "false")
    ).strip().lower()
    if raw_value in _TRUE_VALUES:
        return True, None
    if raw_value in _FALSE_VALUES:
        return False, None
    return False, "live_device_configuration_invalid"


def _load_operations_enabled(
    env: Mapping[str, str],
) -> tuple[bool, str | None]:
    """민감 조회와 mutation이 섞인 operations stage도 명시적으로 연다."""

    raw_value = str(
        env.get("BOXER_COMPANY_API_OPERATIONS_ENABLED", "false")
    ).strip().lower()
    if raw_value in _TRUE_VALUES:
        return True, None
    if raw_value in _FALSE_VALUES:
        return False, None
    return False, "operations_configuration_invalid"


def _load_request_log_settings(
    env: Mapping[str, str],
    *,
    required: bool,
) -> tuple[bool, str, str | None]:
    """API 중앙 감사 SQLite는 systemd StateDirectory 경로만 허용한다."""

    enabled, error = _strict_optional_bool(env, "REQUEST_LOG_SQLITE_ENABLED")
    if error is not None:
        return False, _DEFAULT_REQUEST_LOG_SQLITE_PATH, (
            "request_log_configuration_invalid"
        )
    if not enabled and not required:
        return False, _DEFAULT_REQUEST_LOG_SQLITE_PATH, None
    if not enabled:
        return False, _DEFAULT_REQUEST_LOG_SQLITE_PATH, (
            "request_log_configuration_invalid"
        )
    raw_path = str(env.get("REQUEST_LOG_SQLITE_PATH", "")).strip()
    if raw_path != _DEFAULT_REQUEST_LOG_SQLITE_PATH:
        return False, _DEFAULT_REQUEST_LOG_SQLITE_PATH, (
            "request_log_configuration_invalid"
        )
    return True, raw_path, None


def _load_operations_dependency_settings(
    env: Mapping[str, str],
    *,
    enabled: bool,
    live_device_enabled: bool,
) -> str | None:
    """operations의 공통 기동 경계와 route별 kill switch를 분리한다."""

    del env, live_device_enabled
    if not enabled:
        return None
    # 기존 Slack은 DB/S3/app-user/MDA/복구 설정 하나가 비어 있거나 기능이
    # 꺼져 있어도 프로세스를 계속 띄우고 해당 route에서만 전용 안내를
    # 반환했다. 공통 API도 request-log/caller 검증은 전역에서 유지하되,
    # 기능별 dependency는 각 domain route가 같은 문구로 판정하게 둔다.
    return None


def _load_automation_dependency_settings(
    env: Mapping[str, str],
    *,
    live_device_enabled: bool,
) -> tuple[str, bool, str, str | None]:
    """활성 cycle과 live 장비의 로컬 의존성만 값 노출 없이 선검증한다."""

    feature_flags: dict[str, bool] = {}
    for key in (
        *_AUTOMATION_FEATURE_FLAGS,
        "DEVICE_HEALTH_MONITOR_ALERTS_ENABLED",
        "DEVICE_HEALTH_SHEET_ENABLED",
    ):
        enabled, error = _strict_optional_bool(env, key)
        if error is not None:
            return (
                _DEFAULT_SMS_DELIVERY_OUTBOX_PATH,
                False,
                "",
                error,
            )
        feature_flags[key] = enabled

    weekly_enabled = feature_flags["WEEKLY_RECORDINGS_REPORT_ENABLED"]
    daily_enabled = feature_flags["DAILY_DEVICE_ROUND_ENABLED"]
    health_enabled = feature_flags["DEVICE_HEALTH_MONITOR_ENABLED"]
    notification_enabled = feature_flags[
        "DEVICE_NOTIFICATION_ALERT_ENABLED"
    ]
    sms_delivery_enabled = feature_flags[
        "SMS_DELIVERY_REPORTER_ENABLED"
    ]
    alerts_enabled = feature_flags[
        "DEVICE_HEALTH_MONITOR_ALERTS_ENABLED"
    ]
    sheet_enabled = feature_flags["DEVICE_HEALTH_SHEET_ENABLED"]
    automation_enabled = any(
        (
            weekly_enabled,
            daily_enabled,
            health_enabled,
            notification_enabled,
            sms_delivery_enabled,
        )
    )

    # Assistant-only API는 과거 자동화 env가 남아 있어도 자동화 의존성 때문에
    # 기동이 막히지 않는다. 실제 cycle·alert·Sheet 기능이 켜질 때만 아래의
    # durable storage와 외부 provider 설정을 fail-closed로 검증한다.
    if (
        not automation_enabled
        and not alerts_enabled
        and not sheet_enabled
        and not live_device_enabled
    ):
        return (
            _DEFAULT_SMS_DELIVERY_OUTBOX_PATH,
            False,
            "",
            None,
        )

    data_cycle_enabled = any(
        (weekly_enabled, daily_enabled, health_enabled, notification_enabled)
    )
    if data_cycle_enabled and not _required_env_values(
        env,
        {
            "DB_HOST",
            "DB_USERNAME",
            "DB_PASSWORD",
            "DB_DATABASE",
        },
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if data_cycle_enabled:
        db_enabled, db_flag_error = _strict_optional_bool(
            env,
            "DB_QUERY_ENABLED",
        )
        if db_flag_error is not None or not db_enabled:
            return _automation_dependency_error(
                sheet_enabled=sheet_enabled,
            )

    ssh_runtime_enabled = (
        live_device_enabled
        or daily_enabled
        or health_enabled
    )
    if ssh_runtime_enabled and not _required_env_values(
        env,
        {
            "MDA_GRAPHQL_URL",
            "MDA_ADMIN_USER_PASSWORD",
            "MDA_SSH_OPEN_HOST",
            "DEVICE_SSH_USER",
            "DEVICE_SSH_PASSWORD",
        },
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if ssh_runtime_enabled and not _safe_https_url(
        env.get("MDA_GRAPHQL_URL", "")
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )

    if health_enabled:
        if str(
            env.get("DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR", "")
        ).strip() != _DEFAULT_DEVICE_HEALTH_EVENT_LOG_DIR:
            return _automation_dependency_error(
                sheet_enabled=sheet_enabled,
            )
        if not _required_env_values(env, {"DEVICE_STATE_REDIS_HOST"}):
            return _automation_dependency_error(
                sheet_enabled=sheet_enabled,
            )
        if not _valid_port(env.get("DEVICE_STATE_REDIS_PORT", "6379")):
            return _automation_dependency_error(
                sheet_enabled=sheet_enabled,
            )
        _redis_tls, redis_tls_error = _strict_optional_bool(
            env,
            "DEVICE_STATE_REDIS_TLS",
        )
        if redis_tls_error is not None:
            return _automation_dependency_error(
                sheet_enabled=sheet_enabled,
            )

    sms_domain_enabled = any(
        (
            health_enabled,
            notification_enabled,
            sms_delivery_enabled,
            alerts_enabled,
        )
    )
    provider = str(
        env.get("DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "none")
    ).strip().lower()
    if sms_domain_enabled and provider not in {"none", "webhook", "solapi"}:
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if sms_domain_enabled and alerts_enabled and provider == "none":
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if sms_domain_enabled and provider == "webhook" and not _required_env_values(
        env,
        {"DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL"},
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if sms_domain_enabled and provider == "webhook" and not _safe_https_url(
        env.get("DEVICE_HEALTH_MONITOR_SMS_WEBHOOK_URL", "")
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if sms_domain_enabled and provider == "solapi" and not _required_env_values(
        env,
        {
            "SOLAPI_API_KEY",
            "SOLAPI_API_SECRET",
            "SOLAPI_FROM_NUMBER",
            "SOLAPI_BASE_URL",
        },
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if sms_delivery_enabled and (
        provider != "solapi" or not sheet_enabled
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if (
        (health_enabled or notification_enabled or alerts_enabled)
        and provider == "solapi"
        and not sms_delivery_enabled
    ):
        # 신규 provider 접수 가능성이 있는 cycle과 outbox drain을 따로 켜서
        # receipt를 고립시키지 않는다.
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    if sms_domain_enabled and provider == "solapi" and not _safe_https_url(
        env.get("SOLAPI_BASE_URL", "")
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )

    # health/notification은 provider가 none 또는 webhook이어도 공통 claim
    # sidecar를 쓴다. 반대로 weekly/daily-only는 SMS 파일과 무관하다.
    sms_storage_required = _sms_delivery_storage_required(env)
    raw_outbox_path = str(env.get("SMS_DELIVERY_OUTBOX_PATH", "")).strip()
    outbox_path = Path(
        raw_outbox_path or _DEFAULT_SMS_DELIVERY_OUTBOX_PATH
    ).expanduser()
    if sms_storage_required and (
        not raw_outbox_path
        or str(outbox_path) != _DEFAULT_SMS_DELIVERY_OUTBOX_PATH
    ):
        return (
            _DEFAULT_SMS_DELIVERY_OUTBOX_PATH,
            sheet_enabled,
            "",
            "automation_storage_configuration_invalid",
        )
    if not sms_storage_required:
        # 무관한 이전 env 값은 weekly/daily-only rollout을 막거나 다른 경로를
        # readiness에 묶지 않도록 canonical 미사용 값으로 정규화한다.
        outbox_path = Path(_DEFAULT_SMS_DELIVERY_OUTBOX_PATH)

    credentials_path = str(
        env.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    ).strip()
    if sheet_enabled and (
        not credentials_path
        or not Path(credentials_path).is_absolute()
        or not _required_env_values(
            env,
            {
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID",
                "DEVICE_HEALTH_SHEET_TAB_NAME",
            },
        )
    ):
        return _automation_dependency_error(
            sheet_enabled=True,
        )

    archive_bucket = str(
        env.get("DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET", "")
    ).strip()
    if health_enabled and archive_bucket and not _required_env_values(
        env,
        {
            "AWS_REGION",
            "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX",
        },
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )
    archive_prefix = str(
        env.get("DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_PREFIX", "")
    ).strip().strip("/")
    if health_enabled and archive_bucket and (
        not _valid_s3_bucket_name(archive_bucket)
        or not archive_prefix
        or ".." in archive_prefix.split("/")
    ):
        return _automation_dependency_error(
            sheet_enabled=sheet_enabled,
        )

    return (
        str(outbox_path),
        sheet_enabled,
        credentials_path,
        None,
    )


def _automation_dependency_error(
    *,
    sheet_enabled: bool,
) -> tuple[str, bool, str, str]:
    return (
        _DEFAULT_SMS_DELIVERY_OUTBOX_PATH,
        sheet_enabled,
        "",
        "automation_dependency_configuration_invalid",
    )


def _strict_optional_bool(
    env: Mapping[str, str],
    key: str,
) -> tuple[bool, str | None]:
    raw_value = str(env.get(key, "")).strip().lower()
    if not raw_value:
        return False, None
    if raw_value in _TRUE_VALUES:
        return True, None
    if raw_value in _FALSE_VALUES:
        return False, None
    return False, "automation_dependency_configuration_invalid"


def _automation_enabled_cycles(env: Mapping[str, str]) -> frozenset[str]:
    """운영 feature flag가 실제로 열어 둔 cycle만 반환한다."""

    enabled: set[str] = set()
    flag_cycles = {
        "WEEKLY_RECORDINGS_REPORT_ENABLED": "weekly_recordings",
        "DAILY_DEVICE_ROUND_ENABLED": "daily_device_round",
        "DEVICE_HEALTH_MONITOR_ENABLED": "device_health_monitor",
        "DEVICE_NOTIFICATION_ALERT_ENABLED": "device_notification_alert",
        "SMS_DELIVERY_REPORTER_ENABLED": "sms_delivery",
    }
    for flag, cycle in flag_cycles.items():
        if str(env.get(flag, "")).strip().lower() in _TRUE_VALUES:
            enabled.add(cycle)
    # 접수 중단과 최종 결과 drain은 서로 다른 운영 단계다. source cycle과
    # 독립된 명시 flag만 SMS reconciliation endpoint를 열게 한다.
    return frozenset(enabled)


def _sms_delivery_storage_required(env: Mapping[str, str]) -> bool:
    """SMS claim 또는 receipt를 쓰는 기능에만 outbox를 요구한다."""

    def _enabled(key: str) -> bool:
        return str(env.get(key, "")).strip().lower() in _TRUE_VALUES

    provider = str(
        env.get("DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "none")
    ).strip().lower()
    return bool(
        _enabled("SMS_DELIVERY_REPORTER_ENABLED")
        or _enabled("DEVICE_HEALTH_MONITOR_ENABLED")
        or _enabled("DEVICE_NOTIFICATION_ALERT_ENABLED")
        or (
            _enabled("DEVICE_HEALTH_MONITOR_ALERTS_ENABLED")
            and provider == "solapi"
        )
    )


def _required_env_values(
    env: Mapping[str, str],
    keys: set[str],
) -> bool:
    return all(str(env.get(key, "")).strip() for key in keys)


def _valid_port(value: Any) -> bool:
    try:
        port = int(str(value).strip())
    except (TypeError, ValueError):
        return False
    return 1 <= port <= 65_535


def _safe_https_url(value: Any) -> bool:
    try:
        parsed = urlsplit(str(value or "").strip())
    except ValueError:
        return False
    return bool(
        parsed.scheme == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
        and not parsed.query
        and not parsed.fragment
    )


def _valid_s3_bucket_name(value: str) -> bool:
    return bool(
        3 <= len(value) <= 63
        and re.fullmatch(r"[a-z0-9][a-z0-9.-]*[a-z0-9]", value)
        and ".." not in value
        and ".-" not in value
        and "-." not in value
    )


def company_api_local_readiness(settings: CompanyApiSettings) -> bool:
    """외부 probe 없이 durable state와 root-owned 파일만 확인한다."""

    if not settings.enforce_local_readiness:
        return True
    runtime_paths: list[Path] = []
    if settings.request_log_enabled:
        runtime_paths.append(Path(settings.request_log_path))
    if settings.automation_storage_required:
        runtime_paths.append(Path(settings.automation_state_path))
    if settings.sms_delivery_storage_required:
        runtime_paths.append(Path(settings.sms_delivery_outbox_path))
    if runtime_paths:
        # 활성 기능이 실제로 쓰는 파일만 검사한다. 둘 다 활성일 때는 같은
        # systemd StateDirectory 안에 있어야 lock과 권한 경계가 갈라지지 않는다.
        runtime_parents = {path.parent for path in runtime_paths}
        if len(runtime_parents) != 1:
            return False
        state_directory = next(iter(runtime_parents))
        if not _private_writable_directory(state_directory):
            return False
        request_log_path = Path(settings.request_log_path)
        if settings.request_log_enabled and not (
            request_log_path.exists() or request_log_path.is_symlink()
        ):
            # request-log는 startup initializer가 항상 생성·복원한다. 이후
            # leaf가 사라지면 빈 SQLite를 조용히 재생성하지 않고 readiness를 닫는다.
            return False
        if any(
            not _private_runtime_file(path)
            for path in runtime_paths
            if path.exists() or path.is_symlink()
        ):
            return False
    if settings.device_health_sheet_enabled and not _secure_json_object_file(
        Path(settings.google_application_credentials_path)
    ):
        return False
    return True


def _private_writable_directory(path: Path) -> bool:
    try:
        path_stat = path.lstat()
        if (
            not stat.S_ISDIR(path_stat.st_mode)
            or path.is_symlink()
            or path_stat.st_uid != os.geteuid()
            or path_stat.st_mode & 0o077
            or not os.access(path, os.W_OK | os.X_OK)
        ):
            return False
        descriptor, probe_path = tempfile.mkstemp(
            prefix=".readiness-",
            dir=path,
        )
        try:
            os.fchmod(descriptor, 0o600)
        finally:
            os.close(descriptor)
            Path(probe_path).unlink(missing_ok=True)
        return True
    except OSError:
        return False


def _private_runtime_file(path: Path) -> bool:
    try:
        path_stat = path.lstat()
    except OSError:
        return False
    return bool(
        stat.S_ISREG(path_stat.st_mode)
        and not path.is_symlink()
        and path_stat.st_uid == os.geteuid()
        # runtime store/strict SMS recovery와 같은 exact owner-only mode를
        # readiness에서도 요구해 ready 뒤 첫 cycle 실패를 막는다.
        and stat.S_IMODE(path_stat.st_mode) == 0o600
        and os.access(path, os.R_OK | os.W_OK)
    )


def _secure_root_owned_file(path: Path) -> bool:
    if not path.is_absolute() or path == Path("/"):
        return False
    try:
        leaf_stat = path.lstat()
        if (
            not stat.S_ISREG(leaf_stat.st_mode)
            or path.is_symlink()
            or leaf_stat.st_uid != 0
            or leaf_stat.st_mode & 0o022
            or leaf_stat.st_size <= 0
            or leaf_stat.st_size > _MAX_ROOT_OWNED_CONFIG_BYTES
            or not os.access(path, os.R_OK)
        ):
            return False
        for parent in path.parents:
            parent_stat = parent.lstat()
            if (
                not stat.S_ISDIR(parent_stat.st_mode)
                or parent.is_symlink()
                or parent_stat.st_uid != 0
                or parent_stat.st_mode & 0o022
            ):
                return False
    except OSError:
        return False
    return True


def _secure_json_object_file(path: Path) -> bool:
    if not _secure_root_owned_file(path):
        return False
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return False
    return isinstance(payload, dict) and bool(payload)


def _parse_caller_registry(
    payload: Any,
) -> tuple[CompanyApiCallerSettings, ...]:
    if not isinstance(payload, list) or not payload:
        raise ValueError("caller registry must be a non-empty list")

    callers: list[CompanyApiCallerSettings] = []
    caller_ids: set[str] = set()
    tokens: set[str] = set()
    for item in payload:
        caller = _parse_caller(item)
        if caller.caller_id in caller_ids or caller.token in tokens:
            raise ValueError("caller registry contains a duplicate")
        caller_ids.add(caller.caller_id)
        tokens.add(caller.token)
        callers.append(caller)
    return tuple(callers)


def _validate_automation_slack_caller(
    callers: tuple[CompanyApiCallerSettings, ...],
) -> str | None:
    slack_callers = [caller for caller in callers if "slack" in caller.channels]
    if (
        len(slack_callers) != 1
        or slack_callers[0].capabilities != _AUTOMATION_SLACK_CAPABILITIES
    ):
        return "automation_caller_configuration_invalid"
    return None


def _validate_live_device_slack_caller(
    callers: tuple[CompanyApiCallerSettings, ...],
) -> str | None:
    """같은 Slack caller가 live 장비 turn의 전체 권한을 함께 가져야 한다."""

    slack_callers = [caller for caller in callers if "slack" in caller.channels]
    if (
        len(slack_callers) != 1
        or not _LIVE_DEVICE_SLACK_CAPABILITIES.issubset(
            slack_callers[0].capabilities
        )
    ):
        return "live_device_caller_configuration_invalid"
    return None


def _validate_operations_slack_caller(
    callers: tuple[CompanyApiCallerSettings, ...],
) -> str | None:
    """operations와 alert action 권한이 같은 Slack caller에 묶였는지 본다."""

    slack_callers = [caller for caller in callers if "slack" in caller.channels]
    if (
        len(slack_callers) != 1
        or not _OPERATIONS_SLACK_CAPABILITIES.issubset(
            slack_callers[0].capabilities
        )
    ):
        return "operations_caller_configuration_invalid"
    return None


def _parse_caller(payload: Any) -> CompanyApiCallerSettings:
    if not isinstance(payload, dict):
        raise ValueError("caller must be an object")
    allowed_keys = {
        "callerId",
        "token",
        "tenantIds",
        "channels",
        "actorIds",
        "allowAnonymousActor",
        "capabilities",
    }
    if set(payload) - allowed_keys:
        raise ValueError("caller contains unsupported fields")

    caller_id = _required_text(payload.get("callerId"))
    token = _required_text(payload.get("token"))
    allow_anonymous_actor = payload.get("allowAnonymousActor", False)
    if (
        not _CALLER_ID_PATTERN.fullmatch(caller_id)
        or not _TOKEN_PATTERN.fullmatch(token)
        or not isinstance(allow_anonymous_actor, bool)
        or allow_anonymous_actor
    ):
        raise ValueError("caller identity is invalid")

    tenant_ids = _parse_string_set(
        payload.get("tenantIds"),
        pattern=_SCOPED_ID_PATTERN,
    )
    channels = _parse_string_set(
        payload.get("channels"),
        allowed_values=_ALLOWED_CHANNELS,
    )
    actor_ids = _parse_string_set(
        payload.get("actorIds"),
        pattern=_SCOPED_ID_PATTERN,
    )
    capabilities = _parse_string_set(
        payload.get("capabilities"),
        pattern=_CAPABILITY_PATTERN,
    )
    if _REQUIRED_TURN_CAPABILITY not in capabilities:
        raise ValueError("caller lacks the required turn capability")
    return CompanyApiCallerSettings(
        caller_id=caller_id,
        token=token,
        tenant_ids=tenant_ids,
        channels=channels,
        actor_ids=actor_ids,
        allow_anonymous_actor=allow_anonymous_actor,
        capabilities=capabilities,
    )


def _parse_string_set(
    value: Any,
    *,
    pattern: re.Pattern[str] | None = None,
    allowed_values: frozenset[str] | None = None,
) -> frozenset[str]:
    if not isinstance(value, list) or not value:
        raise ValueError("caller scope must be a non-empty list")
    normalized: list[str] = []
    for item in value:
        if not isinstance(item, str):
            raise ValueError("caller scope value must be text")
        text = item.strip()
        if (
            not text
            or (pattern is not None and not pattern.fullmatch(text))
            or (
                allowed_values is not None
                and text not in allowed_values
            )
        ):
            raise ValueError("caller scope value is invalid")
        normalized.append(text)
    if len(normalized) != len(set(normalized)):
        raise ValueError("caller scope contains a duplicate")
    return frozenset(normalized)


def _required_text(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("required text is missing")
    text = value.strip()
    if not text:
        raise ValueError("required text is empty")
    return text


__all__ = [
    "CompanyApiCallerSettings",
    "CompanyApiSettings",
    "company_api_local_readiness",
    "load_company_api_settings",
]
