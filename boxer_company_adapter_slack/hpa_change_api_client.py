from __future__ import annotations

import hashlib
import logging
import re
import secrets
import threading
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Mapping

import requests

from boxer_company import settings as company_settings
from boxer_company.transport_contracts import (
    HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS,
    HpaChangePollResult,
    HpaChangePollState,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiAvailabilityError,
    CompanyApiClientSettings,
    CompanyApiContractError,
    CompanyApiPolicyError,
    _validate_base_url,
)
from boxer_company_adapter_slack.hpa_change_routes import (
    HpaChangeRequest,
    HpaChangeRoutesConfig,
    HpaChangeSubmissionResult,
    HpaChangeSubmissionStatus,
    HpaChangeThreadLookupResult,
    HpaChangeThreadLookupState,
)
_REQUEST_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$")
_TASK_ID_RE = re.compile(r"^hpa-[A-Za-z0-9-]{8,100}$")
_DELIVERY_ID_RE = re.compile(r"^hpa-delivery:[0-9a-f]{64}$")
_SLACK_CHANNEL_RE = re.compile(r"^[CDG][A-Z0-9]{5,30}$")
_SLACK_TS_RE = re.compile(r"^[0-9]{1,20}(?:\.[0-9]{1,9})?$")
HPA_CHANGE_SUBMIT_PATH = "/internal/v1/hpa-change/requests"
HPA_CHANGE_LOOKUP_PATH = "/internal/v1/hpa-change/threads/lookup"
HPA_CHANGE_DELIVERY_PULL_PATH = "/internal/v1/hpa-change/deliveries/pull"
HPA_CHANGE_DELIVERY_ACK_PATH = "/internal/v1/hpa-change/deliveries/ack"
_SUBMISSION_KEYS = frozenset(
    {"requestId", "status", "hpaRequestId", "message", "autoRetryAllowed"}
)
_LOOKUP_KEYS = frozenset(
    {
        "requestId",
        "state",
        "hpaRequestId",
        "jobStatus",
        "eventTs",
        "currentEvent",
    }
)
_PULL_KEYS = frozenset({"requestId", "deliveries", "autoRetryAllowed"})
_ACK_KEYS = frozenset(
    {
        "requestId",
        "deliveryId",
        "acknowledged",
        "hpaRequestId",
        "jobStatus",
        "implementationDispatchStarted",
        "autoRetryAllowed",
    }
)
_DELIVERY_KEYS = frozenset(
    {
        "deliveryId",
        "hpaRequestId",
        "workspaceId",
        "channelId",
        "threadTs",
        "state",
        "workflowPhase",
        "result",
        "prUrls",
        "requestSource",
    }
)


@dataclass(frozen=True, slots=True, repr=False)
class HpaChangeRemoteDelivery:
    delivery_id: str
    task_id: str
    workspace_id: str
    channel_id: str
    thread_ts: str
    state: HpaChangePollState
    workflow_phase: str
    result: Mapping[str, Any] = field(default_factory=dict)
    pr_urls: tuple[str, ...] = ()
    request_text: str = ""
    attachment_names: tuple[str, ...] = ()

    def to_poll_result(self) -> HpaChangePollResult:
        """기존 안전 renderer에 최소 presentation view만 제공한다."""

        attachment_views = tuple(
            SimpleNamespace(name=name) for name in self.attachment_names
        )
        job_view = SimpleNamespace(
            task_id=self.task_id,
            request_text=self.request_text,
            attachments=attachment_views,
        )
        return HpaChangePollResult(
            task_id=self.task_id,
            state=self.state,
            job=job_view,  # type: ignore[arg-type]
            result=dict(self.result),
            pr_urls=self.pr_urls,
        )


class HpaChangeApiClient:
    """Slack gateway가 HPA API transport를 자동 재시도 없이 호출한다."""

    def __init__(
        self,
        settings: CompanyApiClientSettings,
        *,
        workspace_id: str,
        session: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._base_url = _validate_base_url(settings.base_url).rstrip("/")
        self._token = settings.token
        self._workspace_id = str(workspace_id or "").strip()
        self._connect_timeout_sec = float(settings.connect_timeout_sec)
        self._read_timeout_sec = float(settings.operations_read_timeout_sec)
        if (
            not self._token
            or len(self._token) < 32
            or not self._workspace_id
        ):
            raise CompanyApiContractError("company_api_hpa_configuration_missing")
        self._session = session
        self._thread_local = threading.local() if session is None else None
        self._logger = logger or logging.getLogger(__name__)

    def submit_request(self, request: HpaChangeRequest) -> HpaChangeSubmissionResult:
        if str(request.workspace_id or "").strip() != self._workspace_id:
            # Slack event의 workspace가 고정 service tenant와 다르면 HTTP 전
            # 차단해 caller scope 오류를 transport 실패처럼 다루지 않는다.
            raise CompanyApiContractError(
                "company_api_hpa_workspace_mismatch"
            )
        request_id = _event_request_id("submit", request.workspace_id, request.event_ts)
        payload = {
            "workspaceId": request.workspace_id,
            "requestKey": request.request_key,
            "channelId": request.channel_id,
            "threadTs": request.thread_ts,
            "threadUrl": request.thread_url,
            "eventTs": request.event_ts,
            "requesterUserId": request.requester_user_id,
            "initiatorUserId": request.initiator_user_id,
            "threadText": request.thread_text,
            "attachments": [
                {
                    "name": item.name,
                    "content": item.content,
                    "sha256": hashlib.sha256(item.content.encode("utf-8")).hexdigest(),
                }
                for item in request.attachments
            ],
            "sourceChannelId": request.source_channel_id,
            "sourceMessageTs": request.source_message_ts,
            "selectionMode": request.selection_mode,
            "responseThreadUrl": request.response_thread_url,
            "continuationOfRequestId": request.continuation_of_request_id,
        }
        body = self._post(HPA_CHANGE_SUBMIT_PATH, request_id, payload)
        if (
            frozenset(body) != _SUBMISSION_KEYS
            or body.get("requestId") != request_id
            or body.get("status") not in {item.value for item in HpaChangeSubmissionStatus}
            or type(body.get("autoRetryAllowed")) is not bool
            or body.get("autoRetryAllowed") is not False
        ):
            raise CompanyApiContractError(
                "company_api_hpa_submission_schema_invalid",
                request_id=request_id,
            )
        task_id = str(body.get("hpaRequestId") or "")
        if task_id and not _TASK_ID_RE.fullmatch(task_id):
            raise CompanyApiContractError(
                "company_api_hpa_task_id_invalid",
                request_id=request_id,
            )
        return HpaChangeSubmissionResult(
            status=HpaChangeSubmissionStatus(str(body["status"])),
            request_id=task_id,
            user_message=_safe_text(body.get("message"), 1_000),
        )

    def lookup_thread_job(
        self,
        workspace_id: str,
        channel_id: str,
        thread_ts: str,
        event_ts: str,
    ) -> HpaChangeThreadLookupResult:
        if str(workspace_id or "").strip() != self._workspace_id:
            raise CompanyApiContractError(
                "company_api_hpa_workspace_mismatch"
            )
        request_id = _event_request_id("lookup", workspace_id, event_ts)
        body = self._post(
            HPA_CHANGE_LOOKUP_PATH,
            request_id,
            {
                "workspaceId": workspace_id,
                "channelId": channel_id,
                "threadTs": thread_ts,
                "eventTs": event_ts,
            },
        )
        if (
            frozenset(body) != _LOOKUP_KEYS
            or body.get("requestId") != request_id
            or body.get("state") not in {item.value for item in HpaChangeThreadLookupState}
            or type(body.get("currentEvent")) is not bool
        ):
            raise CompanyApiContractError(
                "company_api_hpa_lookup_schema_invalid",
                request_id=request_id,
            )
        task_id = str(body.get("hpaRequestId") or "")
        if task_id and not _TASK_ID_RE.fullmatch(task_id):
            raise CompanyApiContractError(
                "company_api_hpa_task_id_invalid",
                request_id=request_id,
            )
        return HpaChangeThreadLookupResult(
            state=HpaChangeThreadLookupState(str(body["state"])),
            request_id=task_id,
            job_status=_safe_text(body.get("jobStatus"), 64),
            event_ts=_safe_text(body.get("eventTs"), 64),
            current_event=bool(body["currentEvent"]),
        )

    def pull_pending(self, *, limit: int = 20) -> tuple[HpaChangeRemoteDelivery, ...]:
        request_id = f"hpa:pull:{secrets.token_hex(16)}"
        body = self._post(
            HPA_CHANGE_DELIVERY_PULL_PATH,
            request_id,
            {"workspaceId": self._workspace_id, "limit": max(1, min(100, int(limit)))},
        )
        if (
            frozenset(body) != _PULL_KEYS
            or body.get("requestId") != request_id
            or not isinstance(body.get("deliveries"), list)
            or body.get("autoRetryAllowed") is not False
        ):
            raise CompanyApiContractError(
                "company_api_hpa_delivery_pull_schema_invalid",
                request_id=request_id,
            )
        deliveries = tuple(_deserialize_delivery(item, request_id) for item in body["deliveries"])
        if len({item.delivery_id for item in deliveries}) != len(deliveries):
            raise CompanyApiContractError(
                "company_api_hpa_delivery_duplicate",
                request_id=request_id,
            )
        if any(item.workspace_id != self._workspace_id for item in deliveries):
            raise CompanyApiContractError(
                "company_api_hpa_delivery_workspace_mismatch",
                request_id=request_id,
            )
        return deliveries

    def acknowledge_delivery(self, delivery: HpaChangeRemoteDelivery) -> None:
        request_id = f"hpa:ack:{hashlib.sha256(delivery.delivery_id.encode()).hexdigest()[:32]}"
        body = self._post(
            HPA_CHANGE_DELIVERY_ACK_PATH,
            request_id,
            {
                "workspaceId": self._workspace_id,
                "taskId": delivery.task_id,
                "deliveryId": delivery.delivery_id,
                "state": delivery.state.value,
            },
        )
        if (
            frozenset(body) != _ACK_KEYS
            or body.get("requestId") != request_id
            or body.get("deliveryId") != delivery.delivery_id
            or body.get("hpaRequestId") != delivery.task_id
            or body.get("acknowledged") is not True
            or body.get("autoRetryAllowed") is not False
            or type(body.get("implementationDispatchStarted")) is not bool
        ):
            raise CompanyApiContractError(
                "company_api_hpa_delivery_ack_schema_invalid",
                request_id=request_id,
            )

    def _post(
        self,
        path: str,
        request_id: str,
        payload: Mapping[str, Any],
    ) -> dict[str, Any]:
        if not _REQUEST_ID_RE.fullmatch(request_id):
            raise CompanyApiContractError("company_api_hpa_request_id_invalid")
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-Request-ID": request_id,
            "traceparent": f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01",
            "Accept": "application/json, application/problem+json",
            "Content-Type": "application/json",
        }
        try:
            response = self._session_for_call().post(
                f"{self._base_url}{path}",
                headers=headers,
                json=dict(payload),
                timeout=(self._connect_timeout_sec, self._read_timeout_sec),
                allow_redirects=False,
            )
        except requests.exceptions.ReadTimeout as exc:
            raise CompanyApiAmbiguousTimeoutError(
                "company_api_hpa_read_timeout",
                code="read_timeout",
                request_id=request_id,
            ) from exc
        except (
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            raise CompanyApiAvailabilityError(
                "company_api_hpa_connection_failed",
                code="connection_failed",
                request_id=request_id,
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise CompanyApiContractError(
                "company_api_hpa_transport_failed",
                code="transport_error",
                request_id=request_id,
            ) from exc
        status = int(getattr(response, "status_code", 0) or 0)
        try:
            body = response.json()
        except Exception as exc:
            raise CompanyApiContractError(
                "company_api_hpa_response_json_invalid",
                request_id=request_id,
            ) from exc
        if status == 200 and isinstance(body, dict):
            return body
        code = str(body.get("code") or "http_error") if isinstance(body, dict) else "http_error"
        self._logger.warning(
            "HPA API request failed request_id=%s status=%s code=%s",
            request_id,
            status,
            code,
        )
        if status in {401, 403}:
            raise CompanyApiPolicyError(
                "company_api_hpa_policy_rejected",
                status=status,
                code=code,
                request_id=request_id,
            )
        if status >= 500:
            raise CompanyApiAvailabilityError(
                "company_api_hpa_server_failed",
                status=status,
                code=code,
                request_id=request_id,
            )
        raise CompanyApiContractError(
            "company_api_hpa_request_rejected",
            status=status,
            code=code,
            request_id=request_id,
        )

    def _session_for_call(self) -> Any:
        if self._session is not None:
            return self._session
        assert self._thread_local is not None
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_local.session = session
        return session


def build_hpa_change_remote_routes_config(
    settings: Any = company_settings,
) -> HpaChangeRoutesConfig:
    """Slack intake에 필요한 presentation 설정만 strict하게 조립한다."""

    enabled = bool(getattr(settings, "HPA_CHANGE_REQUEST_ENABLED", False))
    raw_channels = getattr(
        settings,
        "HPA_CHANGE_REQUEST_ALLOWED_CHANNEL_IDS",
        (),
    )
    channels = frozenset(
        str(item or "").strip()
        for item in (
            raw_channels.split(",")
            if isinstance(raw_channels, str)
            else raw_channels or ()
        )
        if str(item or "").strip()
    )
    limits = {
        "max_thread_chars": int(
            getattr(settings, "HPA_CHANGE_MAX_THREAD_CHARS", 30_000)
        ),
        "max_attachment_count": int(
            getattr(settings, "HPA_CHANGE_MAX_FILES", 5)
        ),
        "max_attachment_bytes": int(
            getattr(settings, "HPA_CHANGE_MAX_FILE_BYTES", 131_072)
        ),
        "max_total_attachment_bytes": int(
            getattr(
                settings,
                "HPA_CHANGE_MAX_TOTAL_ATTACHMENT_BYTES",
                524_288,
            )
        ),
    }
    if enabled and channels != HPA_CHANGE_POLICY_ALLOWED_CHANNEL_IDS:
        raise CompanyApiContractError(
            "company_api_hpa_channel_policy_invalid"
        )
    if any(value <= 0 for value in limits.values()) or (
        limits["max_total_attachment_bytes"]
        < limits["max_attachment_bytes"]
    ):
        raise CompanyApiContractError(
            "company_api_hpa_intake_limits_invalid"
        )
    return HpaChangeRoutesConfig(
        enabled=enabled,
        allowed_channel_ids=channels,
        **limits,
    )


def _event_request_id(kind: str, workspace_id: str, event_ts: str) -> str:
    digest = hashlib.sha256(f"{workspace_id}:{event_ts}".encode()).hexdigest()[:32]
    return f"hpa:{kind}:{digest}"


def _safe_text(value: Any, maximum: int) -> str:
    text = str(value or "").strip()
    if len(text) > maximum or not text.isprintable():
        raise CompanyApiContractError("company_api_hpa_text_invalid")
    return text


def _deserialize_delivery(value: Any, request_id: str) -> HpaChangeRemoteDelivery:
    if not isinstance(value, dict) or frozenset(value) != _DELIVERY_KEYS:
        raise CompanyApiContractError(
            "company_api_hpa_delivery_schema_invalid",
            request_id=request_id,
        )
    delivery_id = str(value.get("deliveryId") or "")
    task_id = str(value.get("hpaRequestId") or "")
    workspace_id = str(value.get("workspaceId") or "")
    channel_id = str(value.get("channelId") or "")
    thread_ts = str(value.get("threadTs") or "")
    state_value = str(value.get("state") or "")
    source = value.get("requestSource")
    result = value.get("result")
    pr_urls = value.get("prUrls")
    if (
        not _DELIVERY_ID_RE.fullmatch(delivery_id)
        or not _TASK_ID_RE.fullmatch(task_id)
        or not workspace_id
        or not _SLACK_CHANNEL_RE.fullmatch(channel_id)
        or not _SLACK_TS_RE.fullmatch(thread_ts)
        or state_value not in {item.value for item in HpaChangePollState}
        or state_value == HpaChangePollState.QUEUED.value
        or not isinstance(result, dict)
        or not isinstance(pr_urls, list)
        or not isinstance(source, dict)
        or frozenset(source) != {"text", "attachmentNames"}
        or not isinstance(source.get("attachmentNames"), list)
    ):
        raise CompanyApiContractError(
            "company_api_hpa_delivery_schema_invalid",
            request_id=request_id,
        )
    return HpaChangeRemoteDelivery(
        delivery_id=delivery_id,
        task_id=task_id,
        workspace_id=workspace_id,
        channel_id=channel_id,
        thread_ts=thread_ts,
        state=HpaChangePollState(state_value),
        workflow_phase=_safe_text(value.get("workflowPhase"), 32),
        result=result,
        pr_urls=tuple(_safe_text(item, 2_048) for item in pr_urls),
        request_text=_safe_text(source.get("text"), 60_000),
        attachment_names=tuple(
            _safe_text(item, 255) for item in source["attachmentNames"]
        ),
    )


__all__ = [
    "HpaChangeApiClient",
    "HpaChangeRemoteDelivery",
    "build_hpa_change_remote_routes_config",
]
