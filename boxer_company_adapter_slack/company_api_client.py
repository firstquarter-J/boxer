from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
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

from boxer_company.assistant.contracts import (
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
_MAX_OPERATION_CONTEXT_ENTRIES = 100
_MAX_OPERATION_CONTEXT_CHARS = 12_000
_MAX_QUESTION_CHARS = 40_000
_MAX_RESPONSE_BYTES = 1_048_576
_MAX_RESPONSE_MESSAGES = 8
_MAX_RESPONSE_SOURCES = 20
_MAX_PRIVATE_LINK_URI_CHARS = 16_384
_MAX_MESSAGE_CHARS = 30_000
_NDJSON_MEDIA_TYPE = "application/x-ndjson"
_SLACK_CHANNEL_ID_PATTERN = re.compile(r"^[CDG][A-Z0-9]{1,20}$")
_SLACK_MESSAGE_TS_PATTERN = re.compile(
    r"^\d{1,20}(?:\.\d{1,9})?$"
)
_SAFE_ERROR_TYPE_PATTERN = re.compile(
    r"^[A-Za-z][A-Za-z0-9_.]{0,159}$"
)
_STREAM_RESULT_KEYS = ("type", "result")
_STREAM_HEARTBEAT_KEYS = ("type", "requestId")
_STREAM_ERROR_KEYS = ("type", "problem")
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
_DEVICE_FILE_DOWNLOAD_DELIVERY_RESULT_KEYS = frozenset(
    {
        "kind",
        "status",
        "failureNotice",
        "linkCount",
        "links",
        "delivery",
    }
)
_DEVICE_FILE_DOWNLOAD_LINK_CONTEXT_KEYS = frozenset(
    {"deviceName", "fileName"}
)
_DEVICE_FILE_DOWNLOAD_DELIVERY_KEYS = frozenset(
    {"barcode", "logDate", "usedExpandedScope", "records"}
)
_DEVICE_FILE_DOWNLOAD_DELIVERY_RECORD_KEYS = frozenset(
    {
        "deviceName",
        "deviceSeq",
        "hospitalSeq",
        "hospitalRoomSeq",
        "hospitalName",
        "roomName",
        "fileNames",
        "downloadFileNames",
    }
)
_DEVICE_OPERATION_DELIVERY_RESULT_KEYS = frozenset(
    {"kind", "status", "delivery"}
)
_DEVICE_OPERATION_DELIVERY_KEYS = frozenset(
    {
        "route",
        "deviceName",
        "requestedVersion",
        "currentBoxVersion",
        "dispatchMessage",
        "waitStatus",
        "waitOk",
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
    """API가 응답할 수 없는 transport 가용성 오류다."""


class CompanyApiPolicyError(CompanyApiClientError):
    """인증·권한 거부이며 다른 실행 경계로 우회하면 안 되는 오류다."""


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
    # 기존 동기 Agent install과 완료 poll이 각각 최대 10분 이어질 수 있어
    # 두 구간과 응답 여유를 한 HTTP 요청 안에서 그대로 보존한다.
    operations_read_timeout_sec: float = 1_300.0
    max_retries: int = 1
    automation_tenant_id: str = ""


def load_company_api_client_settings(
    env: Mapping[str, str] | None = None,
) -> CompanyApiClientSettings:
    """Slack이 공통 API와 receipt journal에 필요한 설정만 읽는다."""

    source = os.environ if env is None else env
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
        1_300.0,
    )
    max_retries = _bounded_int_setting(
        source,
        "BOXER_COMPANY_API_MAX_RETRIES",
        1,
        minimum=0,
        maximum=2,
    )
    automation_tenant_id = str(
        source.get(
            "BOXER_COMPANY_API_AUTOMATION_TENANT_ID",
            "",
        )
    ).strip()
    if not _IDENTIFIER_PATTERN.fullmatch(automation_tenant_id):
        raise CompanyApiContractError(
            "company_api_automation_tenant_invalid"
        )
    _validate_automation_delivery_state_path(
        source.get("BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH", "")
    )
    return CompanyApiClientSettings(
        base_url=base_url,
        token=token,
        connect_timeout_sec=connect_timeout_sec,
        read_timeout_sec=read_timeout_sec,
        operations_read_timeout_sec=operations_read_timeout_sec,
        max_retries=max_retries,
        automation_tenant_id=automation_tenant_id,
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
        request_id = _validate_request(
            request,
            route_group=route_group,
        )
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
                # 재실행하지 않고 remote 실패 경계로 그대로 올린다.
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

    def answer_with_progress(
        self,
        request: CompanyAssistantRequest,
        *,
        route_group: _RouteGroup | None = None,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult:
        """부분 결과를 즉시 소비하고 terminal final 하나만 반환한다."""

        if not callable(on_partial_result):
            raise CompanyApiContractError(
                "company_api_progress_callback_invalid",
                request_id=request.request_id,
            )

        request_id = _validate_request(
            request,
            route_group=route_group,
        )
        traceparent = self._traceparent_factory()
        if not _is_valid_traceparent(traceparent):
            raise CompanyApiContractError(
                "company_api_traceparent_invalid",
                request_id=request_id,
            )
        headers = {
            "Authorization": f"Bearer {self._settings.token}",
            "X-Request-ID": request_id,
            "traceparent": traceparent,
            "Accept": (
                f"{_NDJSON_MEDIA_TYPE}, application/json, "
                "application/problem+json"
            ),
            "Content-Type": "application/json",
        }
        response: Any | None = None
        try:
            # 부분 응답을 받은 뒤에는 처리 완료 여부를 알 수 없다. 따라서
            # route 종류와 무관하게 이 호출은 항상 단 한 번만 전송한다.
            response = self._session_for_call().post(
                f"{self._base_url}{_TURN_PATH}",
                headers=headers,
                json=_serialize_request(
                    request,
                    route_group=route_group,
                ),
                timeout=(
                    self._settings.connect_timeout_sec,
                    (
                        self._settings.operations_read_timeout_sec
                        if route_group == "operations"
                        else self._settings.read_timeout_sec
                    ),
                ),
                allow_redirects=False,
                stream=True,
            )
        except requests.exceptions.ReadTimeout as exc:
            raise CompanyApiAmbiguousTimeoutError(
                "company_api_progress_read_timeout",
                code="read_timeout",
                request_id=request_id,
            ) from exc
        except requests.exceptions.SSLError as exc:
            raise CompanyApiContractError(
                "company_api_tls_error",
                code="tls_error",
                request_id=request_id,
            ) from exc
        except requests.exceptions.ConnectTimeout as exc:
            raise CompanyApiAvailabilityError(
                "company_api_connection_failed",
                code="connection_failed",
                request_id=request_id,
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            # 연결이 성립한 뒤 끊겼는지 판별할 수 없으므로 호출자는 이
            # progress 경로에서 local fallback이나 재전송을 하면 안 된다.
            raise CompanyApiAmbiguousTimeoutError(
                "company_api_progress_connection_lost",
                code="connection_lost",
                request_id=request_id,
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise CompanyApiContractError(
                "company_api_transport_error",
                code="transport_error",
                request_id=request_id,
            ) from exc

        try:
            status = _response_status(response)
            if status != 200:
                problem = _deserialize_problem(response, request_id)
                self._raise_problem(problem, status, request_id)

            media_type = _response_media_type(response)
            if media_type == "application/json":
                # 완료된 mutation guard replay는 NDJSON 요청에도 기존 JSON
                # 200을 반환한다. 이미 완료된 최종 결과로 안전하게 수용한다.
                return _deserialize_result(response, request_id)
            if media_type != _NDJSON_MEDIA_TYPE:
                raise _progress_ambiguous_error(
                    request_id,
                    code="stream_content_type_invalid",
                )
            return _deserialize_progress_stream(
                response,
                request_id=request_id,
                on_partial_result=on_partial_result,
            )
        finally:
            close = getattr(response, "close", None)
            if callable(close):
                close()

    def acknowledge_device_file_download(
        self,
        request: CompanyAssistantRequest,
        delivery_result: Mapping[str, Any],
    ) -> CompanyAssistantResult:
        """같은 request ID로 DM 전달 성공 receipt를 한 번만 보낸다."""

        try:
            normalized_result = json.loads(
                json.dumps(dict(delivery_result), ensure_ascii=False)
            )
            _validate_device_file_download_delivery_result(
                normalized_result,
                request.request_id,
            )
        except (TypeError, ValueError, CompanyApiContractError) as exc:
            raise CompanyApiContractError(
                "company_api_operation_result_invalid",
                request_id=request.request_id,
            ) from exc
        delivery = normalized_result["delivery"]
        metadata = dict(request.metadata)
        metadata["operation_action"] = {
            "name": "device_file_download_delivery",
            "phase": "delivered",
            # URL은 되돌려 보내지 않고 API가 만든 strict activity manifest만
            # 같은 request ID의 receipt에 실어 재시작 사이에도 보존한다.
            "delivery": json.loads(
                json.dumps(delivery, ensure_ascii=False)
            ),
        }
        # operations transport는 answer()의 retry_limit=0을 그대로 적용해
        # activity 처리 여부가 불명일 때 HTTP를 자동 재전송하지 않는다.
        return self.answer(
            replace(request, metadata=metadata),
            route_group="operations",
        )

    def acknowledge_device_operation_delivery(
        self,
        request: CompanyAssistantRequest,
        delivery_result: Mapping[str, Any],
    ) -> CompanyAssistantResult:
        """최종 Slack 응답 성공 뒤 같은 request ID로 activity receipt를 보낸다."""

        try:
            normalized_result = json.loads(
                json.dumps(dict(delivery_result), ensure_ascii=False)
            )
            _validate_device_operation_delivery_result(
                normalized_result,
                request.request_id,
            )
        except (TypeError, ValueError, CompanyApiContractError) as exc:
            raise CompanyApiContractError(
                "company_api_operation_result_invalid",
                request_id=request.request_id,
            ) from exc
        metadata = dict(request.metadata)
        metadata["operation_action"] = {
            "name": "device_operation_delivery",
            "phase": "delivered",
            "delivery": json.loads(
                json.dumps(
                    normalized_result["delivery"],
                    ensure_ascii=False,
                )
            ),
        }
        # receipt 자체도 외부 activity write를 포함하므로 자동 재시도 없는
        # operations JSON transport를 그대로 사용한다.
        return self.answer(
            replace(request, metadata=metadata),
            route_group="operations",
        )

    def acknowledge_request_log_delivery(
        self,
        request: CompanyAssistantRequest,
        *,
        delivered: bool,
        reply_count: int,
        first_replied_at_utc: datetime | str | None,
        error_type: str | None = None,
    ) -> CompanyAssistantResult:
        """같은 request ID로 Slack 최종 전달 상태를 0-retry 회신한다."""

        if type(delivered) is not bool or type(reply_count) is not int:
            raise CompanyApiContractError(
                "company_api_request_log_delivery_invalid",
                request_id=request.request_id,
            )
        normalized_first_reply = _normalize_first_replied_at_utc(
            first_replied_at_utc,
            request_id=request.request_id,
        )
        if error_type is not None and (
            not isinstance(error_type, str) or not error_type.strip()
        ):
            raise CompanyApiContractError(
                "company_api_request_log_delivery_invalid",
                request_id=request.request_id,
            )
        normalized_error_type = (
            error_type.strip() if error_type is not None else None
        )
        if (
            not 0 <= reply_count <= 10_000
            or (reply_count > 0) != (normalized_first_reply is not None)
            or delivered == (normalized_error_type is not None)
            or (
                normalized_error_type is not None
                and _SAFE_ERROR_TYPE_PATTERN.fullmatch(
                    normalized_error_type
                )
                is None
            )
        ):
            raise CompanyApiContractError(
                "company_api_request_log_delivery_invalid",
                request_id=request.request_id,
            )
        metadata = dict(request.metadata)
        if not isinstance(metadata.get("audit_context"), Mapping):
            raise CompanyApiContractError(
                "company_api_audit_context_missing",
                request_id=request.request_id,
            )
        metadata["operation_action"] = {
            "name": "request_log_delivery",
            "phase": "receipt",
            "delivered": delivered,
            "reply_count": reply_count,
            "first_replied_at_utc": normalized_first_reply,
            "error_type": normalized_error_type,
        }
        # operations answer()는 retry_limit=0이라 전달 상태가 불명일 때도
        # 같은 receipt를 transport가 자동 재전송하지 않는다.
        return self.answer(
            replace(request, metadata=metadata),
            route_group="operations",
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
            # internal_error는 재시도하거나 Slack-local 실행으로 우회하지 않는다.
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
    # client 객체에는 rollout 선택지가 없다. remote endpoint와 transport
    # 안전값만 검증해 모든 호출을 같은 프로세스 경계에 고정한다.
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


def _validate_request(
    request: CompanyAssistantRequest,
    *,
    route_group: _RouteGroup | None,
) -> str:
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
    if (
        len(question) > _MAX_QUESTION_CHARS
        or (not question and route_group != "freeform")
    ):
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
            request.context_entries,
            max_entries=(
                _MAX_OPERATION_CONTEXT_ENTRIES
                if route_group == "operations"
                else _MAX_CONTEXT_ENTRIES
            ),
            max_chars=(
                _MAX_OPERATION_CONTEXT_CHARS
                if route_group == "operations"
                else _MAX_CONTEXT_CHARS
            ),
            max_entry_chars=(
                _MAX_OPERATION_CONTEXT_CHARS
                if route_group == "operations"
                # Slack loader가 이미 최신 전체 5k window를 만들었다. 단일
                # entry를 다시 4k로 잘라 tail 문맥을 잃지 않는다.
                else _MAX_CONTEXT_CHARS
            ),
        ),
    }
    scope = _serialize_scope(request.metadata)
    if scope:
        payload["scope"] = scope
    if route_group is not None:
        # routeGroup은 권한이 아니라 실행 범위를 더 좁히는 transport hint다.
        payload["routeGroup"] = route_group
    raw_audit_context = request.metadata.get("audit_context")
    if raw_audit_context is not None:
        if route_group != "operations":
            raise CompanyApiContractError(
                "company_api_audit_context_scope_invalid",
                request_id=request.request_id,
            )
        # request-log identity는 Slack event adapter가 확정한 고정 필드만
        # 전달하고 질문 원문이나 임의 metadata는 auditContext에 넣지 않는다.
        payload["auditContext"] = _serialize_audit_context(
            raw_audit_context,
            request=request,
        )
    raw_fun_context = request.metadata.get("team_fun_context")
    if raw_fun_context is not None:
        if (
            route_group != "fun"
            or not isinstance(raw_fun_context, str)
            or len(raw_fun_context) > _MAX_CONTEXT_CHARS
        ):
            raise CompanyApiContractError(
                "company_api_fun_context_invalid",
                request_id=request.request_id,
            )
        # Slack fun loader가 만든 최신 5k 문자열을 앞/뒤 절단 없이 보낸다.
        payload["funContext"] = raw_fun_context
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
    if name == "device_diagnostic_followup_probe":
        # snapshot probe는 질문/문맥의 힌트를 권한처럼 쓰지 않고 고정된
        # action name 하나만 API로 보낸다.
        if frozenset(value) != {"name"}:
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        return {"name": name}
    if name == "request_log_delivery":
        expected_keys = {
            "name",
            "phase",
            "delivered",
            "reply_count",
            "first_replied_at_utc",
            "error_type",
        }
        delivered = value.get("delivered")
        reply_count = value.get("reply_count")
        first_replied_at_utc = value.get("first_replied_at_utc")
        error_type = value.get("error_type")
        try:
            normalized_first_reply = _normalize_first_replied_at_utc(
                first_replied_at_utc,
                request_id=request_id,
            )
        except CompanyApiContractError as exc:
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            ) from exc
        if (
            frozenset(value) != expected_keys
            or phase != "receipt"
            or type(delivered) is not bool
            or type(reply_count) is not int
            or not 0 <= reply_count <= 10_000
            or (reply_count > 0) != (normalized_first_reply is not None)
            or delivered == (error_type is not None)
            or (
                error_type is not None
                and (
                    not isinstance(error_type, str)
                    or _SAFE_ERROR_TYPE_PATTERN.fullmatch(error_type)
                    is None
                )
            )
        ):
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        return {
            "name": name,
            "phase": "receipt",
            "delivered": delivered,
            "replyCount": reply_count,
            "firstRepliedAtUtc": normalized_first_reply,
            "errorType": error_type,
        }
    if name == "device_file_download_delivery":
        if (
            frozenset(value) != {"name", "phase", "delivery"}
            or phase != "delivered"
            or not isinstance(value.get("delivery"), Mapping)
        ):
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        return {
            "name": name,
            "phase": phase,
            "delivery": _serialize_download_delivery_manifest(
                value["delivery"],
                request_id=request_id,
            ),
        }
    if name == "device_operation_delivery":
        if (
            frozenset(value) != {"name", "phase", "delivery"}
            or phase != "delivered"
            or not isinstance(value.get("delivery"), Mapping)
        ):
            raise CompanyApiContractError(
                "company_api_operation_action_invalid",
                request_id=request_id,
            )
        delivery = dict(value["delivery"])
        _validate_device_operation_delivery_manifest(
            delivery,
            request_id,
        )
        return {
            "name": name,
            "phase": phase,
            "delivery": json.loads(
                json.dumps(delivery, ensure_ascii=False)
            ),
        }
    if name == "security_review":
        # 보안검토 응답 원문은 question/context가 아니라 typed operations
        # action 하나로만 보내 API 감사 로그의 원문 마스킹 경계를 유지한다.
        return _serialize_security_review_action(
            value,
            phase=phase,
            request_id=request_id,
        )
    if name == "device_health_alert_ui_receipt":
        return _serialize_device_health_alert_ui_receipt(
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
    hospital_label = _optional_operation_text(
        target.get("hospital_label"),
        320,
    )
    room = _required_operation_text(target.get("room_name"), 160)
    device = _required_operation_text(target.get("device_name"), 160)
    issue = _required_operation_text(target.get("issue"), 1_000)
    alert_category = _optional_operation_text(
        target.get("alert_category"),
        80,
    )
    mda_url = str(target.get("mda_url") or "").strip()
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
        or hospital_label is None
        or room is None
        or device is None
        or issue is None
        or alert_category is None
        or len(mda_url) > 2_048
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
    serialized_target: dict[str, Any] = {
        "hospitalSeq": hospital_seq,
        "hospitalName": hospital,
        "roomName": room,
        "deviceName": device,
        "issue": issue,
        "alertCategory": alert_category,
        "problemComponents": components,
    }
    # 새 중앙 event writer에 필요한 표시 필드만 실제 값이 있을 때 더해
    # 기존 typed action wire payload와의 호환성을 유지한다.
    if hospital_label:
        serialized_target["hospitalLabel"] = hospital_label
    if mda_url:
        serialized_target["mdaUrl"] = mda_url
    serialized: dict[str, Any] = {
        "name": name,
        "phase": phase,
        "target": serialized_target,
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


def _serialize_device_health_alert_ui_receipt(
    value: Mapping[str, Any],
    *,
    phase: str,
    request_id: str,
) -> dict[str, Any]:
    """Slack modal 결과를 고정 event/상태만 가진 typed receipt로 직렬화한다."""

    raw_target = value.get("target")
    action_id = str(value.get("action_id") or "").strip()
    mode = str(value.get("mode") or "").strip()
    event_type = str(value.get("event_type") or "").strip()
    message_ts = str(value.get("message_ts") or "").strip()
    thread_ts = str(value.get("thread_ts") or "").strip()
    occurred_at = str(value.get("occurred_at") or "").strip()
    status = str(value.get("status") or "").strip()
    ok = value.get("ok")
    error_type = str(value.get("error_type") or "").strip()
    try:
        parsed_occurred_at = datetime.fromisoformat(
            occurred_at.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        ) from exc
    if (
        phase != "receipt"
        or event_type != "alert_contact_sms_modal_requested"
        or action_id
        not in {
            "device_health_alert_contact_hospital",
            "device_health_alert_view_auto_sms",
        }
        or mode not in {"send", "view_auto_sent"}
        or status
        not in {
            "missing_trigger_id",
            "modal_opened",
            "modal_open_failed",
        }
        or not isinstance(ok, bool)
        or ok != (status == "modal_opened")
        or (error_type and status != "modal_open_failed")
        or len(error_type) > 160
        or not re.fullmatch(r"\d{1,20}(?:\.\d{1,9})?", message_ts)
        or not re.fullmatch(r"\d{1,20}(?:\.\d{1,9})?", thread_ts)
        or parsed_occurred_at.tzinfo is None
        or not isinstance(raw_target, Mapping)
    ):
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )
    try:
        hospital_seq = int(raw_target.get("hospital_seq") or 0)
    except (TypeError, ValueError) as exc:
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        ) from exc
    hospital_name = _required_operation_text(
        raw_target.get("hospital_name"),
        160,
    )
    hospital_label = _optional_operation_text(
        raw_target.get("hospital_label"),
        320,
    )
    room = _required_operation_text(raw_target.get("room_name"), 160)
    device = _required_operation_text(raw_target.get("device_name"), 160)
    issue = _required_operation_text(raw_target.get("issue"), 1_000)
    alert_category = _optional_operation_text(
        raw_target.get("alert_category"),
        80,
    )
    mda_url = str(raw_target.get("mda_url") or "").strip()
    raw_components = raw_target.get("problem_components")
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
        or hospital_name is None
        or hospital_label is None
        or room is None
        or device is None
        or issue is None
        or alert_category is None
        or len(mda_url) > 2_048
        or len(components) > 16
        or any(component is None for component in components)
        or len(set(components)) != len(components)
        or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,159}",
            device,
        )
    ):
        raise CompanyApiContractError(
            "company_api_operation_action_invalid",
            request_id=request_id,
        )
    return {
        "name": "device_health_alert_ui_receipt",
        "phase": "receipt",
        "eventType": event_type,
        "actionId": action_id,
        "mode": mode,
        "target": {
            "hospitalSeq": hospital_seq,
            "hospitalName": hospital_name,
            "hospitalLabel": hospital_label,
            "roomName": room,
            "deviceName": device,
            "issue": issue,
            "alertCategory": alert_category,
            "mdaUrl": mda_url,
            "problemComponents": components,
        },
        "messageTs": message_ts,
        "threadTs": thread_ts,
        "occurredAt": occurred_at,
        "status": status,
        "ok": ok,
        "errorType": error_type,
    }


def _serialize_download_delivery_manifest(
    value: Mapping[str, Any],
    *,
    request_id: str,
) -> dict[str, Any]:
    _validate_download_delivery_manifest(value, request_id)
    return json.loads(json.dumps(value, ensure_ascii=False))


def _validate_download_delivery_manifest(
    value: Any,
    request_id: str,
) -> None:
    raw_records = value.get("records") if isinstance(value, Mapping) else None
    valid = bool(
        isinstance(value, Mapping)
        and frozenset(value) == _DEVICE_FILE_DOWNLOAD_DELIVERY_KEYS
        and isinstance(value.get("barcode"), str)
        and re.fullmatch(r"\d{11}", value["barcode"])
        and isinstance(value.get("logDate"), str)
        and _is_valid_log_date(value["logDate"])
        and type(value.get("usedExpandedScope")) is bool
        and isinstance(raw_records, list)
        and bool(raw_records)
    )
    if valid:
        valid = all(
            _valid_download_delivery_record(record)
            for record in raw_records
        )
    if not valid:
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )


def _valid_download_delivery_record(value: Any) -> bool:
    if (
        not isinstance(value, Mapping)
        or frozenset(value)
        != _DEVICE_FILE_DOWNLOAD_DELIVERY_RECORD_KEYS
        or not _safe_text(value.get("deviceName"), maximum=160)
        or not _safe_text(value.get("hospitalName"), maximum=200)
        or not _safe_text(value.get("roomName"), maximum=200)
    ):
        return False
    for key in ("deviceSeq", "hospitalSeq", "hospitalRoomSeq"):
        item = value.get(key)
        if item is not None and (type(item) is not int or item < 1):
            return False
    file_names = value.get("fileNames")
    download_file_names = value.get("downloadFileNames")
    return bool(
        isinstance(file_names, list)
        and isinstance(download_file_names, list)
        and download_file_names
        and all(_safe_download_file_name(item) for item in file_names)
        and all(
            _safe_download_file_name(item)
            for item in download_file_names
        )
    )


def _safe_download_file_name(value: Any) -> bool:
    return bool(
        isinstance(value, str)
        and _safe_text(value, maximum=255)
        and "/" not in value
        and "\\" not in value
    )


def _is_valid_log_date(value: str) -> bool:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        return False
    return True


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
    *,
    max_entries: int,
    max_chars: int,
    max_entry_chars: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    remaining_chars = max_chars
    for entry in reversed(entries):
        if len(selected) >= max_entries or remaining_chars <= 0:
            break
        if (
            str(entry.get("kind") or "message").strip() != "message"
            or str(entry.get("source") or "").strip() != "slack"
        ):
            continue
        text = str(entry.get("text") or "").strip()
        if not text:
            continue
        # operations는 개별 메시지를 4k에서 잘라 학습 원문을
        # 훼손하지 않고, 최신 문맥 전체 12k budget만 적용한다.
        text = text[: min(max_entry_chars, remaining_chars)]
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


def _normalize_first_replied_at_utc(
    value: datetime | str | None,
    *,
    request_id: str,
) -> str | None:
    if value is None:
        return None
    try:
        parsed = (
            value
            if isinstance(value, datetime)
            else datetime.fromisoformat(
                str(value).strip().replace("Z", "+00:00")
            )
        )
    except (TypeError, ValueError) as exc:
        raise CompanyApiContractError(
            "company_api_request_log_delivery_invalid",
            request_id=request_id,
        ) from exc
    if parsed.tzinfo is None:
        raise CompanyApiContractError(
            "company_api_request_log_delivery_invalid",
            request_id=request_id,
        )
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat()


def _validated_slack_audit_permalink(
    value: Any,
    *,
    channel_id: str,
    message_ts: str,
    thread_ts: str,
    request_id: str,
) -> str:
    normalized = str(value or "").strip()
    try:
        parsed = urlsplit(normalized)
        port = parsed.port
    except ValueError as exc:
        raise CompanyApiContractError(
            "company_api_audit_context_invalid",
            request_id=request_id,
        ) from exc
    hostname = str(parsed.hostname or "").casefold()
    matched_path = re.fullmatch(
        rf"/archives/({re.escape(channel_id)})/p(\d+)/?",
        parsed.path,
    )
    query_pairs = parse_qsl(parsed.query, keep_blank_values=True)
    query_keys = [key for key, _item in query_pairs]
    query = dict(query_pairs)
    if (
        not normalized
        or len(normalized) > 2_048
        or parsed.scheme != "https"
        or not hostname.endswith(".slack.com")
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or matched_path is None
        or matched_path.group(2) != message_ts.replace(".", "")
        or parsed.fragment
        or len(query_keys) != len(set(query_keys))
        or any(key not in {"thread_ts", "cid"} for key in query_keys)
        or query.get("cid", channel_id) != channel_id
        or query.get("thread_ts", thread_ts) != thread_ts
    ):
        raise CompanyApiContractError(
            "company_api_audit_context_invalid",
            request_id=request_id,
        )
    return normalized


def _serialize_audit_context(
    value: Any,
    *,
    request: CompanyAssistantRequest,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise CompanyApiContractError(
            "company_api_audit_context_invalid",
            request_id=request.request_id,
        )
    required_keys = {
        "event_type",
        "channel_id",
        "message_id",
        "thread_id",
        "is_thread_root",
    }
    allowed_keys = {
        *required_keys,
        "user_name",
        "permalink",
        "thread_permalink",
    }
    raw_channel_id = value.get("channel_id")
    raw_message_id = value.get("message_id")
    raw_thread_id = value.get("thread_id")
    channel_id = (
        raw_channel_id.strip()
        if isinstance(raw_channel_id, str)
        else ""
    )
    message_id = (
        raw_message_id.strip()
        if isinstance(raw_message_id, str)
        else ""
    )
    thread_id = (
        raw_thread_id.strip()
        if isinstance(raw_thread_id, str)
        else ""
    )
    is_thread_root = value.get("is_thread_root")
    request_channel_id = str(
        request.metadata.get("channel_id") or ""
    ).strip()
    request_message_id = str(
        request.metadata.get("message_id") or ""
    ).strip()
    if (
        not required_keys.issubset(value)
        or any(key not in allowed_keys for key in value)
        or value.get("event_type") != "app_mention"
        or _SLACK_CHANNEL_ID_PATTERN.fullmatch(channel_id) is None
        or _SLACK_MESSAGE_TS_PATTERN.fullmatch(message_id) is None
        or _SLACK_MESSAGE_TS_PATTERN.fullmatch(thread_id) is None
        or type(is_thread_root) is not bool
        or is_thread_root != (message_id == thread_id)
        or request_channel_id != channel_id
        or request_message_id != message_id
        or str(request.conversation_id or "").strip() != thread_id
    ):
        raise CompanyApiContractError(
            "company_api_audit_context_invalid",
            request_id=request.request_id,
        )

    user_name_value = value.get("user_name")
    user_name = (
        user_name_value.strip()
        if isinstance(user_name_value, str)
        else None
    )
    if (
        (user_name_value is not None and not isinstance(user_name_value, str))
        or (
            user_name is not None
            and (
                not user_name
                or len(user_name) > 160
                or not user_name.isprintable()
            )
        )
    ):
        raise CompanyApiContractError(
            "company_api_audit_context_invalid",
            request_id=request.request_id,
        )

    payload: dict[str, Any] = {
        "eventType": "app_mention",
        "channelId": channel_id,
        "messageId": message_id,
        "threadId": thread_id,
        "isThreadRoot": is_thread_root,
    }
    if user_name is not None:
        payload["userName"] = user_name
    permalink = value.get("permalink")
    if permalink is not None:
        if not isinstance(permalink, str):
            raise CompanyApiContractError(
                "company_api_audit_context_invalid",
                request_id=request.request_id,
            )
        payload["permalink"] = _validated_slack_audit_permalink(
            permalink,
            channel_id=channel_id,
            message_ts=message_id,
            thread_ts=thread_id,
            request_id=request.request_id,
        )
    thread_permalink = value.get("thread_permalink")
    if thread_permalink is not None:
        if not isinstance(thread_permalink, str):
            raise CompanyApiContractError(
                "company_api_audit_context_invalid",
                request_id=request.request_id,
            )
        payload["threadPermalink"] = _validated_slack_audit_permalink(
            thread_permalink,
            channel_id=channel_id,
            message_ts=thread_id,
            thread_ts=thread_id,
            request_id=request.request_id,
        )
    return payload


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

    actor_name = _normalized_scope_text(metadata.get("actor_name"))
    if actor_name:
        # operation 이력과 학습 페이지의 표시 이름만 전달하며 actor 권한은
        # 계속 top-level actorId와 service caller 검증으로 결정한다.
        scope["actorName"] = actor_name

    thread_permalink = str(
        metadata.get("thread_permalink") or ""
    ).strip()
    if thread_permalink:
        parsed_permalink = urlsplit(thread_permalink)
        hostname = str(parsed_permalink.hostname or "").casefold()
        if (
            len(thread_permalink) <= 2_048
            and parsed_permalink.scheme == "https"
            and hostname.endswith(".slack.com")
            and parsed_permalink.username is None
            and parsed_permalink.password is None
            and parsed_permalink.path.startswith("/archives/")
            and not parsed_permalink.fragment
        ):
            scope["threadPermalink"] = thread_permalink

    trusted_scope = metadata.get("trusted_mda_recovery_scope")
    if isinstance(trusted_scope, Mapping):
        barcode_value = str(trusted_scope.get("barcode") or "").strip()
        log_date = str(trusted_scope.get("logDate") or "").strip()
        device_name = str(trusted_scope.get("deviceName") or "").strip()
        hospital_name = _normalized_scope_text(
            trusted_scope.get("hospitalName"),
            maximum=200,
        )
        room_name = _normalized_scope_text(
            trusted_scope.get("roomName"),
            maximum=200,
        )
        valid_log_date = False
        try:
            datetime.strptime(log_date, "%Y-%m-%d")
            valid_log_date = True
        except ValueError:
            pass
        if (
            re.fullmatch(r"\d{11}", barcode_value)
            and valid_log_date
            and re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_-]{2,63}",
                device_name,
            )
            and hospital_name
            and room_name
        ):
            scope["trustedMdaRecoveryScope"] = {
                "barcode": barcode_value,
                "logDate": log_date,
                "deviceName": device_name,
                "hospitalName": hospital_name,
                "roomName": room_name,
            }
    return scope


def _normalized_scope_text(
    value: Any,
    *,
    maximum: int = 160,
) -> str | None:
    normalized = " ".join(str(value or "").split())
    if not normalized or len(normalized) > maximum or not normalized.isprintable():
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


def _response_media_type(response: Any) -> str:
    headers = getattr(response, "headers", {}) or {}
    return str(headers.get("content-type") or "").split(
        ";", 1
    )[0].strip().lower()


def _progress_ambiguous_error(
    request_id: str,
    *,
    code: str,
) -> CompanyApiAmbiguousTimeoutError:
    return CompanyApiAmbiguousTimeoutError(
        "company_api_progress_incomplete",
        code=code,
        request_id=request_id,
    )


def _strict_json_object(raw_line: bytes) -> dict[str, Any]:
    try:
        decoded = raw_line.decode("utf-8", errors="strict")

        def build_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
            keys = [key for key, _value in pairs]
            if len(keys) != len(set(keys)):
                raise ValueError("duplicate JSON key")
            return dict(pairs)

        payload = json.loads(decoded, object_pairs_hook=build_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise CompanyApiContractError(
            "company_api_progress_frame_invalid"
        ) from exc
    if not isinstance(payload, dict):
        raise CompanyApiContractError(
            "company_api_progress_frame_invalid"
        )
    return payload


def _deserialize_progress_stream(
    response: Any,
    *,
    request_id: str,
    on_partial_result: Callable[[CompanyAssistantResult], None],
) -> CompanyAssistantResult:
    """엄격한 NDJSON 순서를 읽고 final 직후 transport를 닫게 반환한다."""

    try:
        iterator = iter(
            response.iter_lines(
                # update dispatch·barcode 근거 frame은 대부분 작다.
                # 읽기 buffer가 차거나 final이 올 때까지 밀리지
                # 않게 newline을 받는 즉시 frame을 완성한다.
                chunk_size=1,
                decode_unicode=False,
            )
        )
    except Exception as exc:
        raise _progress_ambiguous_error(
            request_id,
            code="stream_unreadable",
        ) from exc

    total_bytes = 0
    stream_route: str | None = None
    while True:
        try:
            raw_line = next(iterator)
        except StopIteration as exc:
            # terminal final 없이 정상 EOF가 와도 작업 완료 여부는 불명이다.
            raise _progress_ambiguous_error(
                request_id,
                code="stream_incomplete",
            ) from exc
        except (
            requests.exceptions.ReadTimeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.ChunkedEncodingError,
            requests.exceptions.RequestException,
        ) as exc:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_interrupted",
            ) from exc
        except Exception as exc:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_unreadable",
            ) from exc

        if not isinstance(raw_line, (bytes, bytearray)) or not raw_line:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_frame_invalid",
            )
        total_bytes += len(raw_line) + 1
        if total_bytes > _MAX_RESPONSE_BYTES:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_too_large",
            )
        try:
            frame = _strict_json_object(bytes(raw_line))
        except CompanyApiContractError as exc:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_frame_invalid",
            ) from exc

        frame_type = frame.get("type")
        if frame_type == "heartbeat":
            if (
                tuple(frame) != _STREAM_HEARTBEAT_KEYS
                or frame.get("requestId") != request_id
            ):
                raise _progress_ambiguous_error(
                    request_id,
                    code="stream_frame_invalid",
                )
            continue
        if frame_type == "error":
            if (
                tuple(frame) != _STREAM_ERROR_KEYS
                or not isinstance(frame.get("problem"), dict)
                or not _valid_stream_problem(
                    frame["problem"],
                    request_id=request_id,
                )
            ):
                raise _progress_ambiguous_error(
                    request_id,
                    code="stream_frame_invalid",
                )
            raise _progress_ambiguous_error(
                request_id,
                code=str(frame["problem"]["code"]),
            )
        if frame_type not in {"partial", "final"}:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_frame_invalid",
            )
        if (
            tuple(frame) != _STREAM_RESULT_KEYS
            or not isinstance(frame.get("result"), dict)
        ):
            raise _progress_ambiguous_error(
                request_id,
                code="stream_frame_invalid",
            )
        try:
            result = _deserialize_result_payload(
                frame["result"],
                request_id,
            )
        except CompanyApiContractError as exc:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_result_invalid",
            ) from exc
        route = result.route
        if stream_route is not None and stream_route != route:
            raise _progress_ambiguous_error(
                request_id,
                code="stream_route_mismatch",
            )
        stream_route = route
        if frame_type == "final":
            return result
        # parsing/transport 예외와 Slack renderer 예외를 구분하기 위해
        # callback은 위의 frame 검증 블록 밖에서 그대로 호출한다.
        on_partial_result(result)


def _valid_stream_problem(
    value: dict[str, Any],
    *,
    request_id: str,
) -> bool:
    return bool(
        tuple(value)
        == ("type", "title", "status", "code", "requestId", "retryable")
        and type(value.get("status")) is int
        and 500 <= value["status"] <= 599
        and value.get("code") in _PROBLEM_CODES
        and value.get("requestId") == request_id
        and type(value.get("retryable")) is bool
        and _safe_text(value.get("title"), maximum=256)
        and value.get("type")
        == f"urn:boxer-company-api:problem:{value.get('code')}"
    )


def _load_json_object(
    response: Any,
    *,
    expected_media_type: str,
) -> dict[str, Any]:
    content_type = _response_media_type(response)
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
    return _deserialize_result_payload(payload, request_id)


def _deserialize_result_payload(
    payload: dict[str, Any],
    request_id: str,
) -> CompanyAssistantResult:
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
    if kind == "device_file_download_delivery":
        _validate_device_file_download_delivery_result(
            value,
            request_id,
        )
        return json.loads(json.dumps(value, ensure_ascii=False))
    if kind == "device_operation_delivery":
        _validate_device_operation_delivery_result(value, request_id)
        return json.loads(json.dumps(value, ensure_ascii=False))
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


def _validate_device_file_download_delivery_result(
    value: dict[str, Any],
    request_id: str,
) -> None:
    raw_links = value.get("links")
    failure_notice = value.get("failureNotice")
    valid = bool(
        frozenset(value) == _DEVICE_FILE_DOWNLOAD_DELIVERY_RESULT_KEYS
        and value.get("kind") == "device_file_download_delivery"
        and value.get("status") == "pending"
        # legacy 실패 안내는 여러 줄이므로 일반 source text의
        # 개행 금지 검증을 적용하지 않는다.
        and isinstance(failure_notice, str)
        and bool(failure_notice.strip())
        and len(failure_notice) <= _MAX_MESSAGE_CHARS
        and type(value.get("linkCount")) is int
        and value["linkCount"] >= 1
        and isinstance(raw_links, list)
        and len(raw_links) == value["linkCount"]
    )
    if valid:
        valid = all(
            isinstance(item, dict)
            and frozenset(item)
            == _DEVICE_FILE_DOWNLOAD_LINK_CONTEXT_KEYS
            and _safe_text(item.get("deviceName"), maximum=160)
            and _safe_text(item.get("fileName"), maximum=255)
            for item in raw_links
        )
    if valid:
        try:
            _validate_download_delivery_manifest(
                value.get("delivery"),
                request_id,
            )
        except CompanyApiContractError:
            valid = False
    if not valid:
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )


def _validate_device_operation_delivery_result(
    value: dict[str, Any],
    request_id: str,
) -> None:
    if (
        frozenset(value) != _DEVICE_OPERATION_DELIVERY_RESULT_KEYS
        or value.get("kind") != "device_operation_delivery"
        or value.get("status") != "pending"
        or not isinstance(value.get("delivery"), Mapping)
    ):
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )
    _validate_device_operation_delivery_manifest(
        value["delivery"],
        request_id,
    )


def _validate_device_operation_delivery_manifest(
    value: Mapping[str, Any],
    request_id: str,
) -> None:
    route = value.get("route")
    device_name = value.get("deviceName")
    requested_version = value.get("requestedVersion")
    current_box_version = value.get("currentBoxVersion")
    dispatch_message = value.get("dispatchMessage")
    wait_status = value.get("waitStatus")
    wait_ok = value.get("waitOk")
    version_pattern = r"[A-Za-z0-9][A-Za-z0-9._+-]{0,79}"
    valid_requested_version = bool(
        isinstance(requested_version, str)
        and (
            (
                route == "device_box_update"
                and re.fullmatch(version_pattern, requested_version)
            )
            or (
                route == "device_agent_update"
                and requested_version == "latest"
            )
            or (
                route == "device_power_off"
                and requested_version == ""
            )
        )
    )
    valid = bool(
        frozenset(value) == _DEVICE_OPERATION_DELIVERY_KEYS
        and route
        in {
            "device_box_update",
            "device_agent_update",
            "device_power_off",
        }
        and isinstance(device_name, str)
        and re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9._-]{0,159}",
            device_name,
        )
        and valid_requested_version
        and isinstance(current_box_version, str)
        and (
            not current_box_version
            or re.fullmatch(version_pattern, current_box_version)
        )
        and isinstance(dispatch_message, str)
        and len(dispatch_message) <= 300
        and (
            not dispatch_message
            or (
                dispatch_message == dispatch_message.strip()
                and dispatch_message.isprintable()
            )
        )
        and wait_status in {"completed", "timed_out"}
        and type(wait_ok) is bool
        and ((wait_status == "completed") is wait_ok)
    )
    if not valid:
        raise CompanyApiContractError(
            "company_api_operation_result_invalid",
            request_id=request_id,
        )


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
