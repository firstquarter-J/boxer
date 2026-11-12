"""회사 배포의 국가와 시간대, 외부 서비스 경계를 한곳에서 정한다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import os
import re
from typing import Mapping
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})
_COUNTRY_DEFAULTS = {
    "kr": ("ap-northeast-2", "Asia/Seoul"),
    "us": ("us-east-1", "America/Los_Angeles"),
}


@dataclass(frozen=True, slots=True)
class DeploymentProfile:
    country: str
    aws_region: str
    timezone_name: str
    timezone: ZoneInfo
    timezone_label: str
    mda_ui_base_url: str
    cdn_base_url: str
    uploader_base_url: str

    @property
    def uploader_host(self) -> str:
        return urlsplit(self.uploader_base_url).hostname or ""

    @property
    def network_service_hosts(self) -> tuple[str, str]:
        # 두 로그 분석기가 같은 국가별 상태/업로드 서비스 판정을 공유한다.
        return f"status.{self.country}.mmtalkbox.com", self.uploader_host


def load_deployment_profile(
    env: Mapping[str, str] | None = None,
) -> DeploymentProfile:
    """옛 한국 설치는 유지하고 잘못된 국가나 시간대는 즉시 거부한다."""

    source = env if env is not None else os.environ
    country = str(source.get("BOXER_COMPANY_COUNTRY", "kr")).strip().lower()
    if country not in _COUNTRY_DEFAULTS:
        raise ValueError("company_deployment_configuration_invalid")
    default_region, default_timezone = _COUNTRY_DEFAULTS[country]
    region = str(source.get("AWS_REGION", "")).strip() or default_region
    timezone_name = (
        str(source.get("BOXER_COMPANY_TIMEZONE", "")).strip()
        or str(source.get("TZ", "")).strip()
        or default_timezone
    )
    try:
        zone = ZoneInfo(timezone_name)
    except (ValueError, ZoneInfoNotFoundError):
        raise ValueError("company_deployment_configuration_invalid") from None
    if region != default_region:
        raise ValueError("company_deployment_configuration_invalid")
    if country == "us" and not (
        timezone_name.startswith("America/")
        or timezone_name in {"Pacific/Honolulu", "Pacific/Guam", "Pacific/Pago_Pago"}
    ):
        # 한국 env의 TZ가 복사돼도 미국 조회가 조용히 KST로 실행되지 않는다.
        raise ValueError("company_deployment_configuration_invalid")
    # 주소 기본값은 요청의 country나 임의 URL이 아니라 서버 배포 국가에 고정한다.
    return DeploymentProfile(
        country=country,
        aws_region=region,
        timezone_name=timezone_name,
        timezone=zone,
        timezone_label="KST" if timezone_name == "Asia/Seoul" else timezone_name,
        mda_ui_base_url=f"https://mda.{country}.mmtalkbox.com",
        cdn_base_url=f"https://cdn-{country}.mmtalkbox.com",
        uploader_base_url=f"https://stream.{country}.mmtalkbox.com",
    )


def get_deployment_profile() -> DeploymentProfile:
    # 실행 host의 env를 읽어 API와 Slack이 같은 provider-free 계약을 사용한다.
    return load_deployment_profile()


def get_local_zone() -> ZoneInfo:
    return get_deployment_profile().timezone


def get_timezone_label() -> str:
    return get_deployment_profile().timezone_label


def localize_wall_datetime(value: datetime) -> datetime:
    """오프셋 없는 장비 시각이 DST로 사라지거나 중복되면 임의 선택하지 않는다."""

    zone = get_local_zone()
    if value.tzinfo is not None:
        return value.astimezone(zone)
    candidates = {}
    for fold in (0, 1):
        candidate = value.replace(tzinfo=zone, fold=fold)
        instant = candidate.astimezone(timezone.utc)
        if instant.astimezone(zone).replace(tzinfo=None) == value:
            candidates[instant] = candidate
    if len(candidates) != 1:
        raise ValueError("local datetime is ambiguous or invalid")
    return next(iter(candidates.values()))


def _parse_boolean(raw: str) -> bool:
    if raw in _TRUE_VALUES:
        return True
    if raw in _FALSE_VALUES:
        return False
    raise ValueError("company_deployment_configuration_invalid")


def _enabled(env: Mapping[str, str], key: str) -> bool:
    return _parse_boolean(str(env.get(key, "false")).strip().lower() or "false")


def _sms_sending_enabled(env: Mapping[str, str], profile: DeploymentProfile) -> bool:
    # 문자 기본값과 시작 검증은 같은 국가 정책을 사용하며 명시된 빈 값도 거부한다.
    default = "true" if profile.country == "kr" else "false"
    return _parse_boolean(str(env.get("BOXER_COMPANY_SMS_ENABLED", default)).strip().lower())


def sms_sending_enabled(env: Mapping[str, str] | None = None) -> bool:
    """신규 문자 발송은 한국 기본 켜짐, 미국 기본 꺼짐으로 판정한다."""

    source = env if env is not None else os.environ
    return _sms_sending_enabled(source, load_deployment_profile(source))


def _safe_host(value: str) -> str:
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError:
        raise ValueError("company_deployment_configuration_invalid") from None
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or port is not None
        or parsed.query
        or parsed.fragment
        or any(character.isspace() or ord(character) < 32 for character in value)
    ):
        raise ValueError("company_deployment_configuration_invalid")
    return parsed.hostname.lower()


def validate_deployment_settings(
    env: Mapping[str, str] | None = None,
    *,
    role: str,
) -> DeploymentProfile:
    """설치 host가 다른 국가의 데이터나 비활성 기능을 열지 못하게 검사한다."""

    source = env if env is not None else os.environ
    profile = load_deployment_profile(source)
    if role not in {"api", "slack"}:
        raise ValueError("company_deployment_configuration_invalid")
    # 신규 flag는 한국에서도 오타를 거부하되 기존 설치의 기본값을 유지한다.
    sms_enabled = _sms_sending_enabled(source, profile)
    if profile.country == "kr":
        return profile

    # 미국은 승인된 초기 운영 범위만 연다. 질문이나 client flag로 확대하지 않는다.
    forbidden_flags = (
        "BOXER_COMPANY_API_MEMORY_ENABLED",
        "BOXER_COMPANY_API_MEMORY_READ_ENABLED",
        "BOXER_COMPANY_SLACK_MEMORY_ENABLED",
        "HPA_CHANGE_REQUEST_ENABLED",
        "DAILY_DEVICE_ROUND_ENABLED",
        "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT",
        "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE",
        "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID",
        "DAILY_DEVICE_ROUND_AUTO_POWER_OFF",
        "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN",
        "SMS_DELIVERY_REPORTER_ENABLED",
    )
    if sms_enabled or any(_enabled(source, key) for key in forbidden_flags):
        raise ValueError("company_deployment_configuration_invalid")
    if str(source.get("DEVICE_HEALTH_MONITOR_SMS_PROVIDER", "none")).strip().lower() != "none":
        raise ValueError("company_deployment_configuration_invalid")
    if str(source.get("AWS_REGION", "")).strip() != profile.aws_region:
        raise ValueError("company_deployment_configuration_invalid")
    if not str(source.get("BOXER_COMPANY_TIMEZONE", "")).strip():
        # 미국 운영은 장비/서비스에서 검증한 IANA zone을 설치자가 명시해야 한다.
        raise ValueError("company_deployment_configuration_invalid")
    if role == "slack":
        return profile

    # 활성 provider만 요구하므로 조회 전용 미국 API는 SSH credential 없이 검증할 수 있다.
    data_enabled = _enabled(source, "DB_QUERY_ENABLED") or any(
        _enabled(source, key)
        for key in (
            "WEEKLY_RECORDINGS_REPORT_ENABLED",
            "DEVICE_HEALTH_MONITOR_ENABLED",
            "DEVICE_NOTIFICATION_ALERT_ENABLED",
            "BOXER_COMPANY_API_OPERATIONS_ENABLED",
            "BOXER_COMPANY_API_LIVE_DEVICE_ENABLED",
        )
    )
    db_host = str(source.get("DB_HOST", "")).strip().lower()
    if data_enabled or db_host:
        if not db_host.endswith(f".{profile.aws_region}.rds.amazonaws.com"):
            raise ValueError("company_deployment_configuration_invalid")
    for key, expected_bucket in (
        ("S3_LOG_BUCKET", "mmb-log-prod-us"),
        ("S3_ULTRASOUND_BUCKET", "ultrasound-prod-us"),
    ):
        bucket = str(source.get(key, "")).strip()
        # 파일 operations도 query flag와 독립적으로 버킷을 사용하므로 잔여값도 검사한다.
        if (_enabled(source, "S3_QUERY_ENABLED") or bucket) and bucket != expected_bucket:
            raise ValueError("company_deployment_configuration_invalid")

    allowed_service_hosts = {
        "MDA_GRAPHQL_URL": "mdaapi.us.mmtalkbox.com",
        "MDA_GRAPHQL_ORIGIN": urlsplit(profile.mda_ui_base_url).hostname,
        "MDA_GRAPHQL_REFERER": urlsplit(profile.mda_ui_base_url).hostname,
        "MDA_UI_BASE_URL": urlsplit(profile.mda_ui_base_url).hostname,
        "BOX_UPLOADER_BASE_URL": urlsplit(profile.uploader_base_url).hostname,
        "BABY_MAGIC_CDN_BASE_URL": urlsplit(profile.cdn_base_url).hostname,
    }
    for key, expected in allowed_service_hosts.items():
        value = str(source.get(key, "")).strip()
        if value and _safe_host(value) != expected:
            raise ValueError("company_deployment_configuration_invalid")
    # 기존 app-user Lambda는 한국 DB 전용이다. 미국 정본이 마련되기 전에는
    # 일반 미국 API 주소를 같은 계약으로 추측하거나 한국 endpoint를 재사용하지 않는다.
    if str(source.get("APP_USER_API_URL", "")).strip():
        raise ValueError("company_deployment_configuration_invalid")
    redis_host = str(source.get("DEVICE_STATE_REDIS_HOST", "")).strip().lower()
    if redis_host and not re.fullmatch(r"[a-z0-9.-]+\.(?:use1|us-east-1)\.cache\.amazonaws\.com", redis_host):
        raise ValueError("company_deployment_configuration_invalid")
    # 남은 선택형 저장 경로에서도 다른 국가의 버킷이 재사용되지 않게 한다.
    for key in (
        "DEVICE_FILE_DOWNLOAD_BUCKET",
        "DEVICE_HEALTH_MONITOR_EVENT_LOG_ARCHIVE_S3_BUCKET",
        "REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET",
    ):
        bucket = str(source.get(key, "")).strip().lower()
        if bucket and re.search(r"(?:^|[-.])kr(?:$|[-.])", bucket):
            raise ValueError("company_deployment_configuration_invalid")
    return profile
