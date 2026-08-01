from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import ipaddress
import logging
import math
import os
import re
import secrets
import threading
import time
from typing import Any, Callable, Literal, Mapping
from urllib.parse import parse_qsl, urlsplit

import requests

from boxer_company.assistant import (
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
_MESSAGE_KEYS = frozenset(
    {"body", "deliveryScope", "mentionActor", "format"}
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
    max_retries: int = 1
    notion_mode: _RolloutMode = "local"
    notion_fallback_enabled: bool = False
    structured_mode: _RolloutMode = "local"
    structured_fallback_enabled: bool = False

    @property
    def enabled(self) -> bool:
        return any(
            mode in {"shadow", "remote"}
            for mode in (
                self.notion_mode,
                self.structured_mode,
            )
        )

    @property
    def shadow_enabled(self) -> bool:
        return "shadow" in {
            self.notion_mode,
            self.structured_mode,
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

    # 모든 route group이 local이면 즉시 롤백 상태다. 이전 remote
    # transport 값이 잘못 남아 있어도 전부 폐기해 Slack 기동을 막지 않는다.
    if notion_mode == "local" and structured_mode == "local":
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
    return CompanyApiClientSettings(
        base_url=base_url,
        token=token,
        connect_timeout_sec=connect_timeout_sec,
        read_timeout_sec=read_timeout_sec,
        max_retries=max_retries,
        notion_mode=notion_mode,
        notion_fallback_enabled=notion_fallback_enabled,
        structured_mode=structured_mode,
        structured_fallback_enabled=(
            structured_fallback_enabled
        ),
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
        payload = _serialize_request(request)
        headers = {
            "Authorization": f"Bearer {self._settings.token}",
            "X-Request-ID": request_id,
            "traceparent": traceparent,
            "Accept": "application/json, application/problem+json",
            "Content-Type": "application/json",
        }
        url = f"{self._base_url}{_TURN_PATH}"

        for attempt in range(self._settings.max_retries + 1):
            try:
                response = self._session_for_call().post(
                    url,
                    headers=headers,
                    json=payload,
                    timeout=(
                        self._settings.connect_timeout_sec,
                        self._settings.read_timeout_sec,
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
                if attempt < self._settings.max_retries:
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
                and attempt < self._settings.max_retries
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
    if (
        settings.notion_mode not in {"local", "shadow", "remote"}
        or type(settings.notion_fallback_enabled) is not bool
        or settings.structured_mode
        not in {"local", "shadow", "remote"}
        or type(settings.structured_fallback_enabled) is not bool
    ):
        raise CompanyApiContractError("company_api_settings_invalid")
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
) -> dict[str, Any]:
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
    return payload


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
        frozenset(payload) != _TURN_KEYS
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
    return CompanyAssistantResult(
        route=payload["route"],
        outcome=payload["outcome"],
        messages=messages,
        sources=sources,
        used_llm=payload["usedLlm"],
        fallback_reason=payload["fallbackReason"],
    )


def _deserialize_message(
    value: Any,
    request_id: str,
) -> AssistantMessage:
    if (
        not isinstance(value, dict)
        or frozenset(value) != _MESSAGE_KEYS
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
    return AssistantMessage(
        body=value["body"],
        delivery_scope=value["deliveryScope"],
        mention_actor=value["mentionActor"],
        format="commonmark",
    )


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
