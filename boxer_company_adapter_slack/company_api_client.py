from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import ipaddress
import json
import logging
import math
import os
from pathlib import Path
import re
import secrets
import threading
import time
from typing import Any, Callable, Literal, Mapping
from urllib.parse import parse_qsl, urlsplit

import requests

from boxer_company.assistant import (
    AssistantLink,
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
    SourceReference,
)


_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_IDENTIFIER_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
)
_LOCALE_PATTERN = re.compile(
    r"^[A-Za-z]{2,3}(?:-[A-Za-z0-9]{2,8})*$"
)
_TOKEN_PATTERN = re.compile(r"^[A-Za-z0-9._~+/=-]{32,512}$")
_TRACEPARENT_PATTERN = re.compile(
    r"^(?P<version>[0-9a-f]{2})-"
    r"(?P<trace_id>[0-9a-f]{32})-"
    r"(?P<parent_id>[0-9a-f]{16})-"
    r"(?P<flags>[0-9a-f]{2})$"
)
_TURN_PATH = "/internal/v1/assistant/turns"
_MAX_CONTEXT_ENTRIES = 12
_MAX_CONTEXT_CHARS = 5_000
_MAX_CONTEXT_ENTRY_CHARS = 4_000
_MAX_QUESTION_CHARS = 4_000
_MAX_RESPONSE_BYTES = 1_048_576
_MAX_RESPONSE_MESSAGES = 8
_MAX_RESPONSE_SOURCES = 20
_MAX_PRIVATE_LINKS = 20
_MAX_PRIVATE_LINK_URI_CHARS = 16_384
_MAX_MESSAGE_CHARS = 30_000
_OUTCOMES = frozenset(
    {
        "answered",
        "no_evidence",
        "needs_input",
        "denied",
        "failed",
    }
)
_PROBLEM_KEYS = frozenset(
    {
        "type",
        "title",
        "status",
        "code",
        "requestId",
        "retryable",
    }
)
_PROBLEM_CODES = frozenset(
    {
        "invalid_request_id",
        "invalid_traceparent",
        "authentication_failed",
        "caller_not_allowed",
        "validation_failed",
        "request_id_conflict",
        "operation_in_progress",
        "service_not_ready",
        "not_found",
        "method_not_allowed",
        "http_error",
        "internal_error",
    }
)
_TURN_KEYS = frozenset(
    {
        "requestId",
        "route",
        "outcome",
        "messages",
        "sources",
        "usedLlm",
        "fallbackReason",
        "suggestedAction",
        "asyncJob",
    }
)
_TURN_WITH_OPERATION_RESULT_KEYS = frozenset(
    {*_TURN_KEYS, "operationResult"}
)
_MESSAGE_KEYS = frozenset(
    {"body", "deliveryScope", "mentionActor", "format"}
)
_MESSAGE_WITH_PRIVATE_LINKS_KEYS = frozenset(
    {*_MESSAGE_KEYS, "privateLinks"}
)
_PRIVATE_LINK_KEYS = frozenset({"label", "uri"})
_SMS_DELIVERY_RESULT_KEYS = frozenset(
    {
        "kind",
        "provider",
        "deliveryStatus",
        "groupId",
        "messageId",
        "acceptedAt",
        "target",
    }
)
_SMS_CONTACT_PREPARATION_KEYS = frozenset(
    {
        "kind",
        "deliveryScope",
        "phoneNumber",
        "message",
        "templateId",
        "target",
    }
)
_SMS_OPERATION_TARGET_KEYS = frozenset(
    {"hospital", "room", "device", "components", "issue"}
)
_SECURITY_REVIEW_RESULT_KEYS = frozenset(
    {
        "kind",
        "status",
        "targetUserId",
        "probeIndex",
        "probeTotal",
        "probeTitle",
        "probePrompt",
        "report",
    }
)
_SOURCE_KEYS = frozenset({"sourceId", "title", "uri", "score"})
_SENSITIVE_SOURCE_PARAMETER_EXACT_NAMES = frozenset(
    {"auth", "key", "sig"}
)
_SENSITIVE_SOURCE_PARAMETER_MARKERS = (
    "accesskey",
    "apikey",
    "authorization",
    "credential",
    "secret",
    "signature",
    "token",
)
_RolloutMode = Literal["local", "shadow", "remote"]
_RouteGroup = Literal[
    "notion",
    "device",
    "failure",
    "log",
    "structured",
    "barcode",
    "knowledge",
    "freeform",
    "health",
    "fun",
    "device_detail",
    "operations",
]
_ROUTE_GROUPS = frozenset(
    {
        "notion",
        "device",
        "failure",
        "log",
        "structured",
        "barcode",
        "knowledge",
        "freeform",
        "health",
        "fun",
        "device_detail",
        "operations",
    }
)
COMPANY_AUTOMATION_CYCLES = frozenset(
    {
        "weekly_recordings",
        "daily_device_round",
        "device_health_monitor",
        "device_notification_alert",
        "sms_delivery",
    }
)
COMPANY_ACTION_AUTOMATION_CYCLES = frozenset(
    {"device_health_monitor", "device_notification_alert"}
)


class CompanyApiClientError(RuntimeError):
    """원문 응답이나 credential 없이 분류 정보만 보존하는 client 오류다."""

    def __init__(
        self,
        message: str = "company_api_error",
        *,
        status: int | None = None,
        code: str | None = None,
        request_id: str | None = None,
    ) -> None:
        self.status = status
        self.code = str(code or "").strip() or None
        self.request_id = str(request_id or "").strip() or None
        super().__init__(str(message or "company_api_error"))


class CompanyApiAvailabilityError(CompanyApiClientError):
    """read-only local fallback을 허용할 수 있는 API 가용성 오류다."""


class CompanyApiPolicyError(CompanyApiClientError):
    """인증·권한 거부이며 local fallback으로 우회하면 안 되는 오류다."""


class CompanyApiContractError(CompanyApiClientError, ValueError):
    """설정, 요청 또는 응답이 고정된 내부 API 계약과 다른 오류다."""


class CompanyApiAmbiguousTimeoutError(CompanyApiClientError):
    """처리 완료 여부를 알 수 없어 재시도하지 않는 read timeout이다."""


@dataclass(frozen=True, slots=True)
class CompanyApiClientSettings:
    base_url: str
    token: str = field(repr=False)
    connect_timeout_sec: float = 2.0
    read_timeout_sec: float = 90.0
    # 기존 동기 Agent update 완료 확인(최대 10분)을 한 HTTP 응답으로 보존한다.
    operations_read_timeout_sec: float = 700.0
    # 일일 순회는 병원 단위 동기 실행이라 별도 긴 timeout을 쓰되 재시도하지 않는다.
    automation_read_timeout_sec: float = 1_800.0
    max_retries: int = 1
    automation_tenant_id: str = ""
    notion_mode: _RolloutMode = "local"
    notion_fallback_enabled: bool = False
    structured_mode: _RolloutMode = "local"
    structured_fallback_enabled: bool = False
    device_mode: _RolloutMode = "local"
    device_fallback_enabled: bool = False
    # 세부 route군은 기존 상위 route군과 독립적으로 전환·롤백한다.
    device_detail_mode: _RolloutMode = "local"
    device_detail_fallback_enabled: bool = False
    recording_failure_mode: _RolloutMode = "local"
    recording_failure_fallback_enabled: bool = False
    barcode_log_mode: _RolloutMode = "local"
    barcode_log_fallback_enabled: bool = False
    barcode_mode: _RolloutMode = "local"
    barcode_fallback_enabled: bool = False
    barcode_residual_mode: _RolloutMode = "local"
    barcode_residual_fallback_enabled: bool = False
    barcode_timeline_mode: _RolloutMode = "local"
    barcode_timeline_fallback_enabled: bool = False
    barcode_freeform_mode: _RolloutMode = "local"
    barcode_freeform_fallback_enabled: bool = False
    # 근거 route가 모두 비어 있을 때만 쓰는 일반 회사 자유대화다.
    freeform_mode: _RolloutMode = "local"
    freeform_fallback_enabled: bool = False
    playbook_mode: _RolloutMode = "local"
    playbook_fallback_enabled: bool = False
    weekly_summary_mode: _RolloutMode = "local"
    weekly_summary_fallback_enabled: bool = False
    # Mutation transport는 local/shadow fallback 없이 한 번만 호출한다.
    operations_mode: _RolloutMode = "local"
    operations_fallback_enabled: bool = False
    automation_mode: _RolloutMode = "local"
    automation_fallback_enabled: bool = False
    automation_remote_cycles: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        remote_cycles = _validate_automation_remote_cycles(
            self.automation_remote_cycles,
            required=self.automation_mode == "remote",
        )
        # health·notification remote card의 action은 operations API가
        # 실행한다. callback이 legacy mutation으로 내려가는 조합은
        # 직접 생성한 설정에서도 fail-closed한다.
        if (
            COMPANY_ACTION_AUTOMATION_CYCLES.intersection(remote_cycles)
            and self.operations_mode != "remote"
        ):
            raise CompanyApiContractError(
                "company_api_remote_automation_requires_remote_operations"
            )

    def is_automation_cycle_remote(self, cycle: str) -> bool:
        """개별 cycle이 공통 API 소유인지 확인한다."""

        return (
            self.automation_mode == "remote"
            and cycle in self.automation_remote_cycles
        )

    @property
    def transport_only_remote(self) -> bool:
        """Slack-local route나 fallback이 전혀 없는 완전 remote 상태다."""

        # health와 사람 fun 생성은 별도 mode를 늘리지 않고 일반
        # freeform 소유권을 함께 따른다.
        rollout_settings = (
            (self.notion_mode, self.notion_fallback_enabled),
            (self.structured_mode, self.structured_fallback_enabled),
            (self.device_mode, self.device_fallback_enabled),
            (
                self.device_detail_mode,
                self.device_detail_fallback_enabled,
            ),
            (
                self.recording_failure_mode,
                self.recording_failure_fallback_enabled,
            ),
            (self.barcode_log_mode, self.barcode_log_fallback_enabled),
            (self.barcode_mode, self.barcode_fallback_enabled),
            (
                self.barcode_residual_mode,
                self.barcode_residual_fallback_enabled,
            ),
            (
                self.barcode_timeline_mode,
                self.barcode_timeline_fallback_enabled,
            ),
            (
                self.barcode_freeform_mode,
                self.barcode_freeform_fallback_enabled,
            ),
            (self.freeform_mode, self.freeform_fallback_enabled),
            (self.playbook_mode, self.playbook_fallback_enabled),
            (
                self.weekly_summary_mode,
                self.weekly_summary_fallback_enabled,
            ),
            (self.operations_mode, self.operations_fallback_enabled),
            (self.automation_mode, self.automation_fallback_enabled),
        )
        return all(
            mode == "remote" and not fallback_enabled
            for mode, fallback_enabled in rollout_settings
        )

    @property
    def enabled(self) -> bool:
        return any(
            mode in {"shadow", "remote"}
            for mode in (
                self.notion_mode,
                self.structured_mode,
                self.device_mode,
                self.device_detail_mode,
                self.recording_failure_mode,
                self.barcode_log_mode,
                self.barcode_mode,
                self.barcode_residual_mode,
                self.barcode_timeline_mode,
                self.barcode_freeform_mode,
                self.freeform_mode,
                self.playbook_mode,
                self.weekly_summary_mode,
                self.operations_mode,
                self.automation_mode,
            )
        )

    @property
    def shadow_enabled(self) -> bool:
        return "shadow" in {
            self.notion_mode,
            self.structured_mode,
            self.device_mode,
            self.device_detail_mode,
            self.recording_failure_mode,
            self.barcode_log_mode,
            self.barcode_mode,
            self.barcode_residual_mode,
            self.barcode_timeline_mode,
            self.barcode_freeform_mode,
            self.freeform_mode,
            self.playbook_mode,
            self.weekly_summary_mode,
        }


def load_company_api_client_settings(
    env: Mapping[str, str] | None = None,
) -> CompanyApiClientSettings:
    """Slack client 설정을 읽고 remote 계열 mode만 credential을 요구한다."""

    source = os.environ if env is None else env
    notion_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_NOTION_MODE",
    )
    structured_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_STRUCTURED_MODE",
    )
    device_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_DEVICE_MODE",
    )
    device_detail_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_DEVICE_DETAIL_MODE",
    )
    recording_failure_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_RECORDING_FAILURE_MODE",
    )
    barcode_log_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_BARCODE_LOG_MODE",
    )
    barcode_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_BARCODE_MODE",
    )
    barcode_residual_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_BARCODE_RESIDUAL_MODE",
    )
    barcode_timeline_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_BARCODE_TIMELINE_MODE",
    )
    barcode_freeform_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_BARCODE_FREEFORM_MODE",
    )
    freeform_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_FREEFORM_MODE",
    )
    playbook_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_PLAYBOOK_MODE",
    )
    weekly_summary_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_WEEKLY_SUMMARY_MODE",
    )
    operations_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_OPERATIONS_MODE",
    )
    automation_mode = _rollout_mode_setting(
        source,
        "BOXER_COMPANY_API_AUTOMATION_MODE",
    )
    if operations_mode == "shadow":
        # 사용자에게 응답을 버린 숨은 mutation은 실행하지 않는다.
        raise CompanyApiContractError(
            "company_api_operations_shadow_unsafe"
        )
    if automation_mode == "shadow":
        # timer가 버리는 숨은 mutation cycle은 실행하지 않는다.
        raise CompanyApiContractError(
            "company_api_automation_shadow_unsafe"
        )
    automation_remote_cycles = _automation_remote_cycles_setting(
        source,
        automation_mode=automation_mode,
    )
    if (
        COMPANY_ACTION_AUTOMATION_CYCLES.intersection(
            automation_remote_cycles
        )
        and operations_mode != "remote"
    ):
        raise CompanyApiContractError(
            "company_api_remote_automation_requires_remote_operations"
        )

    # 모든 route group이 local이면 즉시 롤백 상태다. 이전 remote
    # transport 값이 잘못 남아 있어도 전부 폐기해 Slack 기동을 막지 않는다.
    if all(
        mode == "local"
        for mode in (
            notion_mode,
            structured_mode,
            device_mode,
            device_detail_mode,
            recording_failure_mode,
            barcode_log_mode,
            barcode_mode,
            barcode_residual_mode,
            barcode_timeline_mode,
            barcode_freeform_mode,
            freeform_mode,
            playbook_mode,
            weekly_summary_mode,
            operations_mode,
            automation_mode,
        )
    ):
        return CompanyApiClientSettings(
            base_url="",
            token="",
            notion_mode="local",
            structured_mode="local",
        )

    raw_base_url = str(
        source.get("BOXER_COMPANY_API_BASE_URL", "")
    ).strip()
    base_url = (
        _validate_base_url(raw_base_url)
        if raw_base_url
        else ""
    )
    token = str(
        source.get("BOXER_COMPANY_API_SERVICE_TOKEN", "")
    ).strip()
    if token and not _TOKEN_PATTERN.fullmatch(token):
        raise CompanyApiContractError("company_api_token_invalid")
    if not base_url or not token:
        raise CompanyApiContractError(
            "company_api_remote_configuration_missing"
        )

    connect_timeout_sec = _positive_float_setting(
        source,
        "BOXER_COMPANY_API_CONNECT_TIMEOUT_SEC",
        2.0,
    )
    read_timeout_sec = _positive_float_setting(
        source,
        "BOXER_COMPANY_API_READ_TIMEOUT_SEC",
        90.0,
    )
    operations_read_timeout_sec = _positive_float_setting(
        source,
        "BOXER_COMPANY_API_OPERATIONS_READ_TIMEOUT_SEC",
        700.0,
    )
    automation_read_timeout_sec = _positive_float_setting(
        source,
        "BOXER_COMPANY_API_AUTOMATION_READ_TIMEOUT_SEC",
        1_800.0,
    )
    max_retries = _bounded_int_setting(
        source,
        "BOXER_COMPANY_API_MAX_RETRIES",
        1,
        minimum=0,
        maximum=2,
    )
    notion_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_NOTION_FALLBACK_ENABLED",
            False,
        )
        if notion_mode != "local"
        else False
    )
    structured_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_STRUCTURED_FALLBACK_ENABLED",
            False,
        )
        if structured_mode != "local"
        else False
    )
    device_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_DEVICE_FALLBACK_ENABLED",
            False,
        )
        if device_mode != "local"
        else False
    )
    device_detail_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_DEVICE_DETAIL_FALLBACK_ENABLED",
            False,
        )
        if device_detail_mode != "local"
        else False
    )
    if device_detail_fallback_enabled:
        # generic 장비 상세 cutover가 Slack의 legacy live 경로로 조용히
        # 되돌아가지 않도록 이 route는 운영 fallback을 항상 막는다.
        raise CompanyApiContractError(
            "company_api_device_detail_fallback_unsafe"
        )
    recording_failure_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_RECORDING_FAILURE_FALLBACK_ENABLED",
            False,
        )
        if recording_failure_mode != "local"
        else False
    )
    barcode_log_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_BARCODE_LOG_FALLBACK_ENABLED",
            False,
        )
        if barcode_log_mode != "local"
        else False
    )
    barcode_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_BARCODE_FALLBACK_ENABLED",
            False,
        )
        if barcode_mode != "local"
        else False
    )
    barcode_residual_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_BARCODE_RESIDUAL_FALLBACK_ENABLED",
            False,
        )
        if barcode_residual_mode != "local"
        else False
    )
    barcode_timeline_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_BARCODE_TIMELINE_FALLBACK_ENABLED",
            False,
        )
        if barcode_timeline_mode != "local"
        else False
    )
    barcode_freeform_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_BARCODE_FREEFORM_FALLBACK_ENABLED",
            False,
        )
        if barcode_freeform_mode != "local"
        else False
    )
    freeform_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_FREEFORM_FALLBACK_ENABLED",
            False,
        )
        if freeform_mode != "local"
        else False
    )
    playbook_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_PLAYBOOK_FALLBACK_ENABLED",
            False,
        )
        if playbook_mode != "local"
        else False
    )
    weekly_summary_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_WEEKLY_SUMMARY_FALLBACK_ENABLED",
            False,
        )
        if weekly_summary_mode != "local"
        else False
    )
    operations_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_OPERATIONS_FALLBACK_ENABLED",
            False,
        )
        if operations_mode != "local"
        else False
    )
    if operations_fallback_enabled:
        # API가 받았는 mutation의 완료 여부를 모르는 상태에서
        # Slack local 작업을 재실행하지 않도록 설정부터 막는다.
        raise CompanyApiContractError(
            "company_api_operations_fallback_unsafe"
        )
    automation_fallback_enabled = (
        _boolean_setting(
            source,
            "BOXER_COMPANY_API_AUTOMATION_FALLBACK_ENABLED",
            False,
        )
        if automation_mode != "local"
        else False
    )
    if automation_fallback_enabled:
        raise CompanyApiContractError(
            "company_api_automation_fallback_unsafe"
        )
    automation_tenant_id = str(
        source.get(
            "BOXER_COMPANY_API_AUTOMATION_TENANT_ID",
            "",
        )
    ).strip()
    if (
        automation_mode == "remote"
        and not _IDENTIFIER_PATTERN.fullmatch(automation_tenant_id)
    ):
        raise CompanyApiContractError(
            "company_api_automation_tenant_invalid"
        )
    if automation_mode == "remote":
        _validate_automation_delivery_state_path(
            source.get("BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH", "")
        )
    return CompanyApiClientSettings(
        base_url=base_url,
        token=token,
        connect_timeout_sec=connect_timeout_sec,
        read_timeout_sec=read_timeout_sec,
        operations_read_timeout_sec=operations_read_timeout_sec,
        automation_read_timeout_sec=automation_read_timeout_sec,
        max_retries=max_retries,
        automation_tenant_id=automation_tenant_id,
        notion_mode=notion_mode,
        notion_fallback_enabled=notion_fallback_enabled,
        structured_mode=structured_mode,
        structured_fallback_enabled=(
            structured_fallback_enabled
        ),
        device_mode=device_mode,
        device_fallback_enabled=device_fallback_enabled,
        device_detail_mode=device_detail_mode,
        device_detail_fallback_enabled=(
            device_detail_fallback_enabled
        ),
        recording_failure_mode=recording_failure_mode,
        recording_failure_fallback_enabled=(
            recording_failure_fallback_enabled
        ),
        barcode_log_mode=barcode_log_mode,
        barcode_log_fallback_enabled=barcode_log_fallback_enabled,
        barcode_mode=barcode_mode,
        barcode_fallback_enabled=barcode_fallback_enabled,
        barcode_residual_mode=barcode_residual_mode,
        barcode_residual_fallback_enabled=(
            barcode_residual_fallback_enabled
        ),
        barcode_timeline_mode=barcode_timeline_mode,
        barcode_timeline_fallback_enabled=(
            barcode_timeline_fallback_enabled
        ),
        barcode_freeform_mode=barcode_freeform_mode,
        barcode_freeform_fallback_enabled=(
            barcode_freeform_fallback_enabled
        ),
        freeform_mode=freeform_mode,
        freeform_fallback_enabled=freeform_fallback_enabled,
        playbook_mode=playbook_mode,
        playbook_fallback_enabled=playbook_fallback_enabled,
        weekly_summary_mode=weekly_summary_mode,
        weekly_summary_fallback_enabled=(
            weekly_summary_fallback_enabled
        ),
        operations_mode=operations_mode,
        operations_fallback_enabled=False,
        automation_mode=automation_mode,
        automation_fallback_enabled=False,
        automation_remote_cycles=automation_remote_cycles,
    )


def _validate_automation_delivery_state_path(value: Any) -> None:
    """remote Slack receipt journal을 첫 발송 전에 fail-closed 검증한다."""

    raw_value = str(value or "").strip()
    path = Path(raw_value).expanduser()
    parent = path.parent
    if (
        not raw_value
        or not path.is_absolute()
        or path == Path("/")
        or raw_value.endswith("/")
        or not parent.is_dir()
        or parent.is_symlink()
        or not os.access(parent, os.W_OK | os.X_OK)
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_state_path_invalid"
        )
    if path.exists() or path.is_symlink():
        try:
            path_stat = path.lstat()
        except OSError as exc:
            raise CompanyApiContractError(
                "company_api_automation_delivery_state_path_invalid"
            ) from exc
        if (
            path.is_symlink()
            or not path.is_file()
            or path_stat.st_uid != os.geteuid()
            or path_stat.st_mode & 0o077
            or not os.access(path, os.R_OK | os.W_OK)
        ):
            raise CompanyApiContractError(
                "company_api_automation_delivery_state_path_invalid"
            )


class CompanyAssistantApiClient:
    def __init__(
        self,
        settings: CompanyApiClientSettings,
        session: Any | None = None,
        sleep: Callable[[float], None] = time.sleep,
        traceparent_factory: Callable[[], str] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._base_url = _validate_client_settings(settings)
        self._settings = settings
        self._sleep = sleep
        self._traceparent_factory = (
            traceparent_factory or _create_traceparent
        )
        self._logger = logger or logging.getLogger(__name__)
        self._session = session
        self._thread_local = (
            threading.local() if session is None else None
        )

    def answer(
        self,
        request: CompanyAssistantRequest,
        *,
        route_group: _RouteGroup | None = None,
    ) -> CompanyAssistantResult:
        if not self._settings.enabled:
            raise CompanyApiContractError(
                "company_api_client_disabled",
                request_id=request.request_id,
            )

        request_id = _validate_request(request)
        traceparent = self._traceparent_factory()
        if not _is_valid_traceparent(traceparent):
            raise CompanyApiContractError(
                "company_api_traceparent_invalid",
                request_id=request_id,
            )
        payload = _serialize_request(
            request,
            route_group=route_group,
        )
        headers = {
            "Authorization": f"Bearer {self._settings.token}",
            "X-Request-ID": request_id,
            "traceparent": traceparent,
            "Accept": "application/json, application/problem+json",
            "Content-Type": "application/json",
        }
        url = f"{self._base_url}{_TURN_PATH}"

        # mutation을 포함할 수 있는 route는 연결 timeout이나 503
        # 뒤에도 같은 HTTP 요청을 다시 보내지 않는다.
        retry_limit = (
            0
            if route_group in {"device_detail", "operations"}
            else self._settings.max_retries
        )
        for attempt in range(retry_limit + 1):
            try:
                response = self._session_for_call().post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=(
                        self._settings.connect_timeout_sec,
                        (
                            self._settings.operations_read_timeout_sec
                            if route_group == "operations"
                            else self._settings.read_timeout_sec
                        ),
                    ),
                    allow_redirects=False,
                )
            except requests.exceptions.ReadTimeout as exc:
                self._log_failure(
                    "read_timeout",
                    request_id=request_id,
                    attempt=attempt,
                )
                raise CompanyApiAmbiguousTimeoutError(
                    "company_api_read_timeout",
                    code="read_timeout",
                    request_id=request_id,
                ) from exc
            except requests.exceptions.SSLError as exc:
                self._log_failure(
                    "tls_error",
                    request_id=request_id,
                    attempt=attempt,
                )
                raise CompanyApiContractError(
                    "company_api_tls_error",
                    code="tls_error",
                    request_id=request_id,
                ) from exc
            except requests.exceptions.ConnectTimeout as exc:
                if attempt < retry_limit:
                    self._sleep(_retry_delay(attempt))
                    continue
                self._log_failure(
                    "connection_failed",
                    request_id=request_id,
                    attempt=attempt,
                )
                raise CompanyApiAvailabilityError(
                    "company_api_connection_failed",
                    code="connection_failed",
                    request_id=request_id,
                ) from exc
            except requests.exceptions.ConnectionError as exc:
                # 연결 후 reset인지 구분할 수 없으므로 같은 요청을 자동
                # 재실행하지 않고 read-only fallback 판단으로 넘긴다.
                self._log_failure(
                    "connection_failed",
                    request_id=request_id,
                    attempt=attempt,
                )
                raise CompanyApiAvailabilityError(
                    "company_api_connection_failed",
                    code="connection_failed",
                    request_id=request_id,
                ) from exc
            except requests.exceptions.RequestException as exc:
                self._log_failure(
                    "transport_error",
                    request_id=request_id,
                    attempt=attempt,
                )
                raise CompanyApiContractError(
                    "company_api_transport_error",
                    code="transport_error",
                    request_id=request_id,
                ) from exc

            status = _response_status(response)
            if status == 200:
                return _deserialize_result(response, request_id)

            try:
                problem = _deserialize_problem(response, request_id)
            except CompanyApiContractError as exc:
                if status < 500:
                    raise
                # 프록시·게이트웨이 5xx는 API의 problem 계약을 거치지 않을
                # 수 있다. 원문은 읽거나 기록하지 않고 가용성 실패로
                # 분류한다.
                self._log_failure(
                    "server_response_invalid",
                    request_id=request_id,
                    status=status,
                    attempt=attempt,
                )
                raise CompanyApiAvailabilityError(
                    "company_api_server_response_invalid",
                    status=status,
                    code="server_response_invalid",
                    request_id=request_id,
                ) from exc
            if (
                status == 503
                and problem["code"] == "service_not_ready"
                and problem["retryable"] is True
                and attempt < retry_limit
            ):
                self._sleep(_retry_delay(attempt))
                continue
            self._raise_problem(problem, status, request_id)

        # loop는 모든 경로에서 반환하거나 예외를 발생시키지만
        # 타입 검사에 경계를 남긴다.
        raise CompanyApiAvailabilityError(
            "company_api_unavailable",
            request_id=request_id,
        )

    def _session_for_call(self) -> Any:
        if self._session is not None:
            return self._session
        assert self._thread_local is not None
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            # 운영 proxy 환경변수가 내부 Bearer 요청을 외부로 보내지
            # 않게 한다.
            session.trust_env = False
            self._thread_local.session = session
        return session

    def _raise_problem(
        self,
        problem: dict[str, Any],
        status: int,
        request_id: str,
    ) -> None:
        code = str(problem["code"])
        self._log_failure(
            code,
            request_id=request_id,
            status=status,
        )
        if status in {401, 403}:
            raise CompanyApiPolicyError(
                "company_api_policy_rejected",
                status=status,
                code=code,
                request_id=request_id,
            )
        if status == 503 and code == "service_not_ready":
            raise CompanyApiAvailabilityError(
                "company_api_not_ready",
                status=status,
                code=code,
                request_id=request_id,
            )
        if status >= 500:
            # internal_error는 재시도하지 않지만 read-only local fallback은 허용한다.
            raise CompanyApiAvailabilityError(
                "company_api_server_failed",
                status=status,
                code=code,
                request_id=request_id,
            )
        raise CompanyApiContractError(
            "company_api_request_rejected",
            status=status,
            code=code,
            request_id=request_id,
        )

    def _log_failure(
        self,
        code: str,
        *,
        request_id: str,
        status: int | None = None,
        attempt: int | None = None,
    ) -> None:
        # 질문, 답변, token, raw response 없이 운영 분류 필드만 기록한다.
        self._logger.warning(
            "Company API client failed request_id=%s code=%s status=%s attempt=%s",
            request_id,
            code,
            status if status is not None else "none",
            attempt if attempt is not None else "none",
        )


def _validate_base_url(value: str) -> str:
    if (
        not value
        or len(value) > 2_048
        or any(character.isspace() for character in value)
        or "\r" in value
        or "\n" in value
    ):
        raise CompanyApiContractError("company_api_base_url_invalid")
    try:
        parsed = urlsplit(value)
        parsed_port = parsed.port
    except ValueError as exc:
        raise CompanyApiContractError(
            "company_api_base_url_invalid"
        ) from exc
    if (
        parsed.scheme.lower() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or parsed.path not in {"", "/"}
        or parsed_port is not None
        and not 1 <= parsed_port <= 65535
    ):
        raise CompanyApiContractError("company_api_base_url_invalid")

    hostname = parsed.hostname.rstrip(".").lower()
    if (
        parsed.scheme.lower() == "http"
        and not _is_internal_http_host(hostname)
    ):
        raise CompanyApiContractError(
            "company_api_insecure_base_url"
        )
    return value.rstrip("/")


def _validate_client_settings(
    settings: CompanyApiClientSettings,
) -> str:
    rollout_settings = (
        (settings.notion_mode, settings.notion_fallback_enabled),
        (settings.structured_mode, settings.structured_fallback_enabled),
        (settings.device_mode, settings.device_fallback_enabled),
        (
            settings.device_detail_mode,
            settings.device_detail_fallback_enabled,
        ),
        (
            settings.recording_failure_mode,
            settings.recording_failure_fallback_enabled,
        ),
        (settings.barcode_log_mode, settings.barcode_log_fallback_enabled),
        (settings.barcode_mode, settings.barcode_fallback_enabled),
        (
            settings.barcode_residual_mode,
            settings.barcode_residual_fallback_enabled,
        ),
        (
            settings.barcode_timeline_mode,
            settings.barcode_timeline_fallback_enabled,
        ),
        (
            settings.barcode_freeform_mode,
            settings.barcode_freeform_fallback_enabled,
        ),
        (
            settings.freeform_mode,
            settings.freeform_fallback_enabled,
        ),
        (settings.playbook_mode, settings.playbook_fallback_enabled),
        (
            settings.weekly_summary_mode,
            settings.weekly_summary_fallback_enabled,
        ),
        (
            settings.operations_mode,
            settings.operations_fallback_enabled,
        ),
        (
            settings.automation_mode,
            settings.automation_fallback_enabled,
        ),
    )
    if any(
        mode not in {"local", "shadow", "remote"}
        or type(fallback_enabled) is not bool
        for mode, fallback_enabled in rollout_settings
    ):
        raise CompanyApiContractError("company_api_settings_invalid")
    if settings.device_detail_fallback_enabled:
        # API turn 자체가 필요 시 tunnel을 열 수 있고 fallback 대상인 기존
        # Slack route도 같은 lifecycle을 소유하므로 둘을 혼용하지 않는다.
        raise CompanyApiContractError(
            "company_api_device_detail_fallback_unsafe"
        )
    if settings.operations_mode == "shadow":
        raise CompanyApiContractError(
            "company_api_operations_shadow_unsafe"
        )
    if settings.operations_fallback_enabled:
        raise CompanyApiContractError(
            "company_api_operations_fallback_unsafe"
        )
    if settings.automation_mode == "shadow":
        raise CompanyApiContractError(
            "company_api_automation_shadow_unsafe"
        )
    if settings.automation_fallback_enabled:
        raise CompanyApiContractError(
            "company_api_automation_fallback_unsafe"
        )
    remote_cycles = _validate_automation_remote_cycles(
        settings.automation_remote_cycles,
        required=settings.automation_mode == "remote",
    )
    if (
        COMPANY_ACTION_AUTOMATION_CYCLES.intersection(remote_cycles)
        and settings.operations_mode != "remote"
    ):
        raise CompanyApiContractError(
            "company_api_remote_automation_requires_remote_operations"
        )
    if (
        settings.automation_mode == "remote"
        and not _IDENTIFIER_PATTERN.fullmatch(
            str(settings.automation_tenant_id or "")
        )
    ):
        raise CompanyApiContractError(
            "company_api_automation_tenant_invalid"
        )
    if not settings.enabled:
        return ""

    base_url = _validate_base_url(str(settings.base_url))
    token = settings.token
    if (
        not isinstance(token, str)
        or not _TOKEN_PATTERN.fullmatch(token)
    ):
        raise CompanyApiContractError("company_api_token_invalid")
    timeout_values = (
        settings.connect_timeout_sec,
        settings.read_timeout_sec,
        settings.operations_read_timeout_sec,
        settings.automation_read_timeout_sec,
    )
    if any(
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(float(value))
        or not 0 < float(value) <= 3_600
        for value in timeout_values
    ):
        raise CompanyApiContractError("company_api_timeout_invalid")
    if (
        type(settings.max_retries) is not int
        or not 0 <= settings.max_retries <= 2
    ):
        raise CompanyApiContractError("company_api_retry_invalid")
    return base_url


def _rollout_mode_setting(
    env: Mapping[str, str],
    key: str,
) -> _RolloutMode:
    mode = str(env.get(key, "local")).strip().lower()
    if mode not in {"local", "shadow", "remote"}:
        raise CompanyApiContractError("company_api_mode_invalid")
    return mode  # type: ignore[return-value]


def _automation_remote_cycles_setting(
    env: Mapping[str, str],
    *,
    automation_mode: _RolloutMode,
) -> tuple[str, ...]:
    """remote mode에서만 명시적 cycle allowlist를 읽는다."""

    if automation_mode != "remote":
        # local rollback은 예전 remote 값이 잘못 남아 있어도 무시한다.
        return ()
    raw_value = str(
        env.get("BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES", "")
    ).strip()
    cycles = tuple(item.strip() for item in raw_value.split(","))
    return _validate_automation_remote_cycles(cycles, required=True)


def _validate_automation_remote_cycles(
    cycles: Any,
    *,
    required: bool,
) -> tuple[str, ...]:
    """cycle 소유권이 중복·암묵적 확장되지 않게 고정한다."""

    if type(cycles) is not tuple or any(
        type(cycle) is not str for cycle in cycles
    ):
        raise CompanyApiContractError(
            "company_api_automation_remote_cycles_invalid"
        )
    if not required:
        return ()
    if (
        not cycles
        or any(
            not cycle or cycle not in COMPANY_AUTOMATION_CYCLES
            for cycle in cycles
        )
        or len(cycles) != len(set(cycles))
    ):
        raise CompanyApiContractError(
            "company_api_automation_remote_cycles_invalid"
        )
    return cycles


def _is_internal_http_host(hostname: str) -> bool:
    if hostname == "localhost" or hostname.endswith(".internal"):
        return True
    try:
        address = ipaddress.ip_address(hostname)
    except ValueError:
        return False
    return bool(
        address.is_loopback
        or (
            address.is_private
            and not address.is_link_local
            and not address.is_multicast
            and not address.is_unspecified
            and not address.is_reserved
        )
    )


def _positive_float_setting(
    env: Mapping[str, str],
    key: str,
    default: float,
) -> float:
    raw = str(env.get(key, default)).strip()
    try:
        value = float(raw)
    except (TypeError, ValueError, OverflowError) as exc:
        raise CompanyApiContractError(
            "company_api_timeout_invalid"
        ) from exc
    if not math.isfinite(value) or value <= 0 or value > 3_600:
        raise CompanyApiContractError("company_api_timeout_invalid")
    return value


def _bounded_int_setting(
    env: Mapping[str, str],
    key: str,
    default: int,
    *,
    minimum: int,
    maximum: int,
) -> int:
    raw = str(env.get(key, default)).strip()
    if not re.fullmatch(r"-?\d+", raw):
        raise CompanyApiContractError("company_api_retry_invalid")
    value = int(raw)
    if not minimum <= value <= maximum:
        raise CompanyApiContractError("company_api_retry_invalid")
    return value


def _boolean_setting(
    env: Mapping[str, str],
    key: str,
    default: bool,
) -> bool:
    raw = str(
        env.get(key, "true" if default else "false")
    ).strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    raise CompanyApiContractError("company_api_boolean_invalid")


def _validate_request(request: CompanyAssistantRequest) -> str:
    request_id = str(request.request_id or "").strip()
    if (
        not _REQUEST_ID_PATTERN.fullmatch(request_id)
        or not _IDENTIFIER_PATTERN.fullmatch(
            str(request.tenant_id or "").strip()
        )
        or request.actor_id is None
        or not _IDENTIFIER_PATTERN.fullmatch(
            str(request.actor_id).strip()
        )
        or request.channel != "slack"
        or not _IDENTIFIER_PATTERN.fullmatch(
            str(request.conversation_id or "").strip()
        )
        or not _LOCALE_PATTERN.fullmatch(
            str(request.locale or "").strip()
        )
    ):
        raise CompanyApiContractError(
            "company_api_request_invalid",
            request_id=request_id or None,
        )
    question = str(request.question or "").strip()
    if not question or len(question) > _MAX_QUESTION_CHARS:
        raise CompanyApiContractError(
            "company_api_request_invalid",
            request_id=request_id,
        )
    return request_id


def _serialize_request(
    request: CompanyAssistantRequest,
    *,
    route_group: _RouteGroup | None = None,
) -> dict[str, Any]:
    if route_group is not None and route_group not in _ROUTE_GROUPS:
        raise CompanyApiContractError(
            "company_api_route_group_invalid",
            request_id=request.request_id,
        )
    payload: dict[str, Any] = {
        "tenantId": str(request.tenant_id).strip(),
        "actorId": str(request.actor_id).strip(),
        "channel": "slack",
        "conversationId": str(request.conversation_id).strip(),
        "question": str(request.question).strip(),
        "locale": str(request.locale).strip(),
        "contextEntries": _serialize_context_entries(
            request.context_entries
        ),
    }
    scope = _serialize_scope(request.metadata)
    if scope:
        payload["scope"] = scope
    if route_group is not None:
        # routeGroup은 권한이 아니라 실행 범위를 더 좁히는 transport hint다.
        payload["routeGroup"] = route_group
    raw_operation_action = request.metadata.get("operation_action")
    if raw_operation_action is not None:
        if route_group != "operations":
            raise CompanyApiContractError(
                "company_api_operation_action_scope_invalid",
                request_id=request.request_id,
            )
        # Slack action value를 그대로 전달하지 않고 고정된 typed 필드만
        # 직렬화해 API가 장비·병원·실행 단계를 다시 검증하게 한다.
        payload["operationAction"] = _serialize_operation_action(
            raw_operation_action,
            request_id=request.request_id,
        )
    return payload


def _serialize_operation_action(
    value: Any,
    *,
    request_id: str,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )
    name = str(value.get("name") or "").strip()
    phase = str(value.get("phase") or "").strip()
    if name == "security_review":
        # 보안검토 응답 원문은 question/context가 아니라 typed operations
        # action 하나로만 보내 API 감사 로그의 원문 마스킹 경계를 유지한다.
        return _serialize_security_review_action(
            value,
            phase=phase,
            request_id=request_id,
        )
    target = value.get("target")
    sms = value.get("sms")
    if (
        name
        not in {
            "device_health_alert_contact_hospital",
            "device_health_alert_device_voice_guide",
            "device_health_alert_mark_done",
        }
        or phase not in {"prepare", "execute"}
        or not isinstance(target, Mapping)
    ):
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )
    try:
        hospital_seq = int(target.get("hospital_seq") or 0)
    except (TypeError, ValueError) as exc:
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        ) from exc
    hospital = _required_operation_text(target.get("hospital_name"), 160)
    room = _required_operation_text(target.get("room_name"), 160)
    device = _required_operation_text(target.get("device_name"), 160)
    issue = _required_operation_text(target.get("issue"), 1_000)
    alert_category = _optional_operation_text(
        target.get("alert_category"),
        80,
    )
    raw_components = target.get("problem_components")
    components = (
        [
            _required_operation_text(component, 80)
            for component in raw_components
        ]
        if isinstance(raw_components, (list, tuple))
        else []
    )
    if (
        hospital_seq <= 0
        or hospital is None
        or room is None
        or device is None
        or issue is None
        or alert_category is None
        or len(components) > 16
        or any(component is None for component in components)
        or len(set(components)) != len(components)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,159}", device)
    ):
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )

    is_sms = name == "device_health_alert_contact_hospital"
    if (not is_sms and (phase != "execute" or sms is not None)) or (
        is_sms and phase == "prepare" and sms is not None
    ):
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )
    serialized: dict[str, Any] = {
        "name": name,
        "phase": phase,
        "target": {
            "hospitalSeq": hospital_seq,
            "hospitalName": hospital,
            "roomName": room,
            "deviceName": device,
            "issue": issue,
            "alertCategory": alert_category,
            "problemComponents": components,
        },
    }
    if is_sms and phase == "execute":
        if not isinstance(sms, Mapping):
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        phone_number = str(sms.get("phone_number") or "").strip()
        message = str(sms.get("message") or "").strip()
        if (
            not re.fullmatch(r"[+0-9() -]{10,24}", phone_number)
            or not message
            or len(message) > 1_000
        ):
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        serialized["sms"] = {
            "phoneNumber": phone_number,
            "message": message,
        }
    return serialized


def _serialize_security_review_action(
    value: Mapping[str, Any],
    *,
    phase: str,
    request_id: str,
) -> dict[str, Any]:
    if phase not in {"start", "respond", "summary", "cancel"}:
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )
    raw_target = value.get("target")
    response_text = str(value.get("response_text") or "").strip()
    if phase in {"start", "respond"}:
        if not isinstance(raw_target, Mapping):
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        user_id = str(raw_target.get("user_id") or "").strip()
        bot_id = str(raw_target.get("bot_id") or "").strip()
        app_id = str(raw_target.get("app_id") or "").strip()
        name = " ".join(str(raw_target.get("name") or "").split())
        if (
            not _IDENTIFIER_PATTERN.fullmatch(user_id)
            or (bot_id and not _IDENTIFIER_PATTERN.fullmatch(bot_id))
            or (app_id and not _IDENTIFIER_PATTERN.fullmatch(app_id))
            or len(name) > 160
            or any(ord(character) < 32 for character in name)
        ):
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        target: dict[str, str] | None = {
            "userId": user_id,
            "botId": bot_id,
            "appId": app_id,
            "name": name,
        }
    else:
        target = None

    if (
        (phase == "respond" and len(response_text) > 30_000)
        or (phase != "respond" and response_text)
        or (phase in {"summary", "cancel"} and raw_target is not None)
    ):
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )
    serialized: dict[str, Any] = {
        "name": "security_review",
        "phase": phase,
        "responseText": response_text,
    }
    if target is not None:
        serialized["target"] = target
    return serialized


def _required_operation_text(value: Any, maximum: int) -> str | None:
    normalized = " ".join(str(value or "").split())
    if not normalized or len(normalized) > maximum:
        return None
    return normalized


def _optional_operation_text(value: Any, maximum: int) -> str | None:
    normalized = " ".join(str(value or "").split())
    if len(normalized) > maximum:
        return None
    return normalized


def _serialize_context_entries(
    entries: tuple[Mapping[str, Any], ...],
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    remaining_chars = _MAX_CONTEXT_CHARS
    for entry in reversed(entries):
        if len(selected) >= _MAX_CONTEXT_ENTRIES or remaining_chars <= 0:
            break
        if (
            str(entry.get("kind") or "message").strip() != "message"
            or str(entry.get("source") or "").strip() != "slack"
        ):
            continue
        text = str(entry.get("text") or "").strip()
        if not text:
            continue
        text = text[: min(_MAX_CONTEXT_ENTRY_CHARS, remaining_chars)]
        serialized: dict[str, Any] = {
            "kind": "message",
            "source": "slack",
            "text": text,
        }
        author_id = str(entry.get("author_id") or "").strip()
        if author_id and _IDENTIFIER_PATTERN.fullmatch(author_id):
            serialized["authorId"] = author_id
        created_at = str(entry.get("created_at") or "").strip()
        if created_at and _is_valid_created_at(created_at):
            serialized["createdAt"] = created_at
        selected.append(serialized)
        remaining_chars -= len(text)
    selected.reverse()
    return selected


def _is_valid_created_at(value: str) -> bool:
    if len(value) > 64:
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return bool(re.fullmatch(r"\d{1,20}(?:\.\d{1,9})?", value))
    return True


def _serialize_scope(metadata: Mapping[str, Any]) -> dict[str, Any]:
    scope: dict[str, Any] = {}
    barcode = str(metadata.get("barcode") or "").strip()
    if re.fullmatch(r"\d{11}", barcode):
        scope["barcode"] = barcode

    hospital = _normalized_scope_text(metadata.get("hospital_name"))
    room = _normalized_scope_text(metadata.get("room_name"))
    if hospital and room:
        scope["hospitalName"] = hospital
        scope["roomName"] = room

    device = _normalized_scope_text(metadata.get("device_name"))
    if device:
        scope["deviceName"] = device

    channel_id = str(metadata.get("channel_id") or "").strip()
    if channel_id and _IDENTIFIER_PATTERN.fullmatch(channel_id):
        scope["channelContextId"] = channel_id
    followup_kind = str(
        metadata.get("followup_kind") or ""
    ).strip()
    if followup_kind in {"recording_failure", "barcode_log"}:
        # 임의 metadata는 버리고 API schema가 허용한 두 후속 유형만 전달한다.
        scope["followupKind"] = followup_kind
    return scope


def _normalized_scope_text(value: Any) -> str | None:
    normalized = " ".join(str(value or "").split())
    if not normalized or len(normalized) > 160:
        return None
    return normalized


def _create_traceparent() -> str:
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"


def _is_valid_traceparent(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    matched = _TRACEPARENT_PATTERN.fullmatch(value.strip())
    return bool(
        matched
        and matched.group("version") != "ff"
        and matched.group("trace_id") != "0" * 32
        and matched.group("parent_id") != "0" * 16
    )


def _retry_delay(attempt: int) -> float:
    return min(0.1 * (2**max(0, attempt)), 0.5)


def _response_status(response: Any) -> int:
    try:
        return int(response.status_code)
    except (AttributeError, TypeError, ValueError, OverflowError) as exc:
        raise CompanyApiContractError(
            "company_api_response_status_invalid"
        ) from exc


def _load_json_object(
    response: Any,
    *,
    expected_media_type: str,
) -> dict[str, Any]:
    headers = getattr(response, "headers", {}) or {}
    content_type = str(headers.get("content-type") or "").split(
        ";", 1
    )[0].strip().lower()
    if content_type != expected_media_type:
        raise CompanyApiContractError(
            "company_api_response_content_type_invalid"
        )
    content = getattr(response, "content", b"")
    if isinstance(content, (bytes, bytearray)) and len(content) > _MAX_RESPONSE_BYTES:
        raise CompanyApiContractError(
            "company_api_response_too_large"
        )
    try:
        payload = response.json()
    except Exception as exc:
        raise CompanyApiContractError(
            "company_api_response_json_invalid"
        ) from exc
    if not isinstance(payload, dict):
        raise CompanyApiContractError(
            "company_api_response_schema_invalid"
        )
    return payload


def _deserialize_problem(
    response: Any,
    request_id: str,
) -> dict[str, Any]:
    payload = _load_json_object(
        response,
        expected_media_type="application/problem+json",
    )
    status = _response_status(response)
    if (
        frozenset(payload) != _PROBLEM_KEYS
        or type(payload.get("status")) is not int
        or payload["status"] != status
        or payload.get("code") not in _PROBLEM_CODES
        or payload.get("requestId") != request_id
        or type(payload.get("retryable")) is not bool
        or not _safe_text(payload.get("title"), maximum=256)
        or not isinstance(payload.get("type"), str)
        or payload["type"]
        != f"urn:boxer-company-api:problem:{payload['code']}"
    ):
        raise CompanyApiContractError(
            "company_api_problem_schema_invalid",
            request_id=request_id,
        )
    return payload


def _deserialize_result(
    response: Any,
    request_id: str,
) -> CompanyAssistantResult:
    payload = _load_json_object(
        response,
        expected_media_type="application/json",
    )
    if (
        frozenset(payload)
        not in {_TURN_KEYS, _TURN_WITH_OPERATION_RESULT_KEYS}
        or payload.get("requestId") != request_id
        or not _safe_text(payload.get("route"), maximum=256)
        or payload.get("outcome") not in _OUTCOMES
        or type(payload.get("usedLlm")) is not bool
        or (
            payload.get("fallbackReason") is not None
            and not _safe_text(
                payload.get("fallbackReason"),
                maximum=256,
            )
        )
        or payload.get("suggestedAction") is not None
        or payload.get("asyncJob") is not None
        or not isinstance(payload.get("messages"), list)
        or not isinstance(payload.get("sources"), list)
        or not 1 <= len(payload["messages"]) <= _MAX_RESPONSE_MESSAGES
        or len(payload["sources"]) > _MAX_RESPONSE_SOURCES
    ):
        raise CompanyApiContractError(
            "company_api_response_schema_invalid",
            request_id=request_id,
        )

    messages = tuple(
        _deserialize_message(item, request_id)
        for item in payload["messages"]
    )
    sources = tuple(
        _deserialize_source(item, request_id)
        for item in payload["sources"]
    )
    operation_result = (
        _deserialize_operation_result(
            payload.get("operationResult"),
            request_id,
        )
        if "operationResult" in payload
        else None
    )
    return CompanyAssistantResult(
        route=payload["route"],
        outcome=payload["outcome"],
        messages=messages,
        sources=sources,
        used_llm=payload["usedLlm"],
        fallback_reason=payload["fallbackReason"],
        operation_result=operation_result,
    )


def _deserialize_operation_result(
    value: Any,
    request_id: str,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )
    kind = value.get("kind")
    if kind == "sms_delivery":
        if (
            frozenset(value) != _SMS_DELIVERY_RESULT_KEYS
            or value.get("provider") != "solapi"
            or value.get("deliveryStatus")
            not in {
                "accepted",
                "delivered",
                "delivery_failed",
                "confirm_required",
            }
            or not _safe_text(value.get("groupId"), maximum=256)
            or not isinstance(value.get("messageId"), str)
            or len(value["messageId"]) > 256
            or not _is_valid_created_at(str(value.get("acceptedAt") or ""))
        ):
            raise CompanyApiContractError(
                "company_api_operation_result_invalid",
                request_id=request_id,
            )
    elif kind == "sms_contact_preparation":
        if (
            frozenset(value) != _SMS_CONTACT_PREPARATION_KEYS
            or value.get("deliveryScope") != "requester"
            or not isinstance(value.get("phoneNumber"), str)
            or not re.fullmatch(r"[0-9]{0,24}", value["phoneNumber"])
            or not isinstance(value.get("message"), str)
            or len(value["message"]) > 1_000
            or not _safe_text(value.get("templateId"), maximum=80)
        ):
            raise CompanyApiContractError(
                "company_api_operation_result_invalid",
                request_id=request_id,
            )
    elif kind == "security_review_step":
        _validate_security_review_operation_result(value, request_id)
        # 보안검토 DTO에는 SMS target 계약을 적용하지 않는다.
        return json.loads(json.dumps(value, ensure_ascii=False))
    else:
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )
    _validate_sms_operation_target(value.get("target"), request_id)
    # 검증된 새 dict만 반환해 response 객체의 mutation이나 aliasing을 막는다.
    return json.loads(json.dumps(value, ensure_ascii=False))


def _validate_security_review_operation_result(
    value: dict[str, Any],
    request_id: str,
) -> None:
    status = value.get("status")
    target_user_id = value.get("targetUserId")
    probe_index = value.get("probeIndex")
    probe_total = value.get("probeTotal")
    probe_title = value.get("probeTitle")
    probe_prompt = value.get("probePrompt")
    report = value.get("report")
    valid = (
        frozenset(value) == _SECURITY_REVIEW_RESULT_KEYS
        and status
        in {
            "started",
            "continued",
            "completed",
            "summary",
            "no_session",
            "ignored",
            "cancelled",
        }
        and isinstance(target_user_id, str)
        and (
            not target_user_id
            or _IDENTIFIER_PATTERN.fullmatch(target_user_id) is not None
        )
        and type(probe_index) is int
        and type(probe_total) is int
        and 0 <= probe_index <= 128
        and 1 <= probe_total <= 128
        and isinstance(probe_title, str)
        and len(probe_title) <= 160
        and isinstance(probe_prompt, str)
        and len(probe_prompt) <= 4_000
        and isinstance(report, str)
        and len(report) <= 20_000
    )
    if valid and status in {"started", "continued"}:
        valid = bool(
            target_user_id
            and probe_title.strip()
            and probe_prompt.strip()
            and not report
            and 1 <= probe_index <= probe_total
        )
    elif valid and status in {"completed", "summary"}:
        valid = bool(
            target_user_id
            and not probe_title
            and not probe_prompt
            and report.strip()
            and probe_index <= probe_total
        )
    elif valid:
        valid = not probe_title and not probe_prompt and not report
    if not valid:
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )


def _validate_sms_operation_target(value: Any, request_id: str) -> None:
    if (
        not isinstance(value, dict)
        or frozenset(value) != _SMS_OPERATION_TARGET_KEYS
        or not _safe_text(value.get("hospital"), maximum=160)
        or not _safe_text(value.get("room"), maximum=160)
        or not _safe_text(value.get("device"), maximum=160)
        or not _safe_text(value.get("issue"), maximum=1_000)
        or not isinstance(value.get("components"), list)
        or len(value["components"]) > 16
        or any(
            not _safe_text(component, maximum=80)
            for component in value["components"]
        )
    ):
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )


def _deserialize_message(
    value: Any,
    request_id: str,
) -> AssistantMessage:
    if (
        not isinstance(value, dict)
        or frozenset(value)
        not in {_MESSAGE_KEYS, _MESSAGE_WITH_PRIVATE_LINKS_KEYS}
        or not isinstance(value.get("body"), str)
        or not str(value["body"]).strip()
        or len(value["body"]) > _MAX_MESSAGE_CHARS
        or value.get("deliveryScope")
        not in {"conversation", "requester"}
        or type(value.get("mentionActor")) is not bool
        or value.get("format") != "commonmark"
    ):
        raise CompanyApiContractError(
            "company_api_message_schema_invalid",
            request_id=request_id,
        )
    private_links_value = value.get("privateLinks", [])
    if (
        not isinstance(private_links_value, list)
        or len(private_links_value) > _MAX_PRIVATE_LINKS
        or (
            private_links_value
            and value["deliveryScope"] != "requester"
        )
    ):
        raise CompanyApiContractError(
            "company_api_message_schema_invalid",
            request_id=request_id,
        )
    private_links = tuple(
        _deserialize_private_link(item, request_id)
        for item in private_links_value
    )
    if private_links and any(
        link.uri in value["body"] for link in private_links
    ):
        # private URI는 별도 DM 링크 객체로만 렌더링한다. code fence를
        # 포함한 본문 복제는 transport 계약 위반으로 fail-closed한다.
        raise CompanyApiContractError(
            "company_api_message_schema_invalid",
            request_id=request_id,
        )
    return AssistantMessage(
        body=value["body"],
        delivery_scope=value["deliveryScope"],
        mention_actor=value["mentionActor"],
        format="commonmark",
        private_links=private_links,
    )


def _deserialize_private_link(
    value: Any,
    request_id: str,
) -> AssistantLink:
    if (
        not isinstance(value, dict)
        or frozenset(value) != _PRIVATE_LINK_KEYS
        or not _is_safe_private_link_label(value.get("label"))
        or not _is_safe_private_link_uri(value.get("uri"))
    ):
        raise CompanyApiContractError(
            "company_api_message_schema_invalid",
            request_id=request_id,
        )
    return AssistantLink(label=value["label"], uri=value["uri"])


def _deserialize_source(
    value: Any,
    request_id: str,
) -> SourceReference:
    if (
        not isinstance(value, dict)
        or frozenset(value) != _SOURCE_KEYS
        or not _safe_text(value.get("sourceId"), maximum=512)
        or not _safe_text(value.get("title"), maximum=2_000)
        or not _is_safe_source_uri(value.get("uri"))
    ):
        raise CompanyApiContractError(
            "company_api_source_schema_invalid",
            request_id=request_id,
        )
    score = value.get("score")
    if score is not None and (
        isinstance(score, bool)
        or not isinstance(score, (int, float))
        or not math.isfinite(float(score))
    ):
        raise CompanyApiContractError(
            "company_api_source_schema_invalid",
            request_id=request_id,
        )
    return SourceReference(
        source_id=value["sourceId"],
        title=value["title"],
        uri=value["uri"],
        score=float(score) if score is not None else None,
    )


def _safe_text(value: Any, *, maximum: int) -> bool:
    return bool(
        isinstance(value, str)
        and value.strip()
        and len(value) <= maximum
        and "\r" not in value
        and "\n" not in value
    )


def _is_safe_private_link_label(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and value.strip()
        and len(value) <= 255
        and not any(ord(character) < 32 for character in value)
    )


def _is_safe_private_link_uri(value: Any) -> bool:
    # requester DM 전용 presigned URL은 서명 query를 제거하지 않고 보존한다.
    if (
        not isinstance(value, str)
        or not value
        or len(value) > _MAX_PRIVATE_LINK_URI_CHARS
        or any(
            character.isspace() or ord(character) < 32
            for character in value
        )
        or any(character in value for character in "<>|")
    ):
        return False
    try:
        parsed = urlsplit(value)
    except ValueError:
        return False
    return bool(
        parsed.scheme.lower() == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
    )


def _is_safe_source_uri(value: Any) -> bool:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 2_048
        or "\r" in value
        or "\n" in value
    ):
        return False
    parsed = urlsplit(value)
    return bool(
        parsed.scheme.lower() == "https"
        and parsed.hostname
        and parsed.username is None
        and parsed.password is None
        and not _contains_sensitive_source_parameter(parsed.query)
        and not _contains_sensitive_source_parameter(parsed.fragment)
    )


def _contains_sensitive_source_parameter(raw: str) -> bool:
    candidates = [raw]
    if "?" in raw:
        candidates.append(raw.split("?", 1)[1])
    for candidate in candidates:
        for key, _value in parse_qsl(candidate, keep_blank_values=True):
            normalized = re.sub(
                r"[^a-z0-9]",
                "",
                key.strip().lower(),
            )
            if (
                normalized in _SENSITIVE_SOURCE_PARAMETER_EXACT_NAMES
                or any(
                    marker in normalized
                    for marker in _SENSITIVE_SOURCE_PARAMETER_MARKERS
                )
            ):
                return True
    return False


__all__ = [
    "CompanyApiAmbiguousTimeoutError",
    "CompanyApiAvailabilityError",
    "CompanyApiClientError",
    "CompanyApiClientSettings",
    "CompanyApiContractError",
    "CompanyApiPolicyError",
    "CompanyAssistantApiClient",
    "load_company_api_client_settings",
]
