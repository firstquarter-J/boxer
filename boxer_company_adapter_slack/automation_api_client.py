from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import logging
import re
import secrets
import threading
from typing import Any, Literal, Mapping

import requests

from boxer_company.automation import AutomationDelivery
from boxer_company_adapter_slack.company_api_client import (
    COMPANY_AUTOMATION_CYCLES,
    CompanyApiAmbiguousTimeoutError,
    CompanyApiAvailabilityError,
    CompanyApiClientSettings,
    CompanyApiContractError,
    CompanyApiPolicyError,
    _validate_client_settings,
)


_AUTOMATION_PATH = "/internal/v1/automation/cycles"
_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_CYCLE_KEY_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,191}$"
)
_EXTERNAL_MESSAGE_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$"
)
@dataclass(frozen=True, slots=True)
class AutomationRemoteReceipt:
    delivery_id: str
    status: Literal["sent", "failed"]
    external_message_id: str = ""
    permalink: str = field(default="", repr=False)
    delivered_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class AutomationRemoteResult:
    request_id: str
    cycle: str
    outcome: Literal["completed", "no_change"]
    deliveries: tuple[AutomationDelivery, ...] = field(
        default_factory=tuple,
        repr=False,
    )
    metrics: Mapping[str, Any] = field(default_factory=dict, repr=False)


class CompanyAutomationApiClient:
    """mutation 가능 cycle을 자동 재시도 없이 한 번만 호출하는 client다."""

    def __init__(
        self,
        settings: CompanyApiClientSettings,
        *,
        session: Any | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if settings.automation_mode != "remote":
            raise CompanyApiContractError(
                "company_api_automation_client_disabled"
            )
        if settings.automation_fallback_enabled:
            raise CompanyApiContractError(
                "company_api_automation_fallback_unsafe"
            )
        # 직접 구성된 settings도 assistant client와 같은 internal URL,
        # token, timeout 검증을 통과해야 transport를 만들 수 있다.
        self._base_url = _validate_client_settings(settings).rstrip("/")
        self._token = settings.token
        self._tenant_id = str(
            settings.automation_tenant_id or ""
        ).strip()
        self._connect_timeout_sec = float(settings.connect_timeout_sec)
        self._read_timeout_sec = float(
            settings.automation_read_timeout_sec
        )
        self._remote_cycles = frozenset(
            settings.automation_remote_cycles
        )
        if not self._base_url or not self._token or not self._tenant_id:
            raise CompanyApiContractError(
                "company_api_automation_configuration_missing"
            )
        self._session = session
        self._thread_local = (
            threading.local() if session is None else None
        )
        self._logger = logger or logging.getLogger(__name__)

    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    def run(
        self,
        *,
        request_id: str,
        cycle: str,
        cycle_key: str,
        scheduled_at: datetime,
        options: Mapping[str, Any] | None = None,
        receipts: tuple[AutomationRemoteReceipt, ...] = (),
        ack_only: bool = False,
    ) -> AutomationRemoteResult:
        # 조립 실수가 있어도 allowlist 밖 cycle은 HTTP 요청 전에
        # 차단해 한 cycle의 rollout이 다른 상태를 가져가지 못하게 한다.
        if cycle not in self._remote_cycles:
            raise CompanyApiContractError(
                "company_api_automation_cycle_not_remote"
            )
        payload = _build_cycle_payload(
            tenant_id=self._tenant_id,
            request_id=request_id,
            cycle=cycle,
            cycle_key=cycle_key,
            scheduled_at=scheduled_at,
            options=options or {},
            receipts=receipts,
            ack_only=ack_only,
        )
        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-Request-ID": request_id,
            "traceparent": _create_traceparent(),
            "Accept": "application/json, application/problem+json",
            "Content-Type": "application/json",
        }
        try:
            # cycle에는 조회 뒤 mutation이 섞일 수 있어 한 번만 전송한다.
            response = self._session_for_call().post(
                f"{self._base_url}{_AUTOMATION_PATH}",
                headers=headers,
                json=payload,
                timeout=(
                    self._connect_timeout_sec,
                    self._read_timeout_sec,
                ),
                allow_redirects=False,
            )
        except requests.exceptions.ReadTimeout as exc:
            self._log_failure(request_id, "read_timeout")
            raise CompanyApiAmbiguousTimeoutError(
                "company_api_automation_read_timeout",
                code="read_timeout",
                request_id=request_id,
            ) from exc
        except requests.exceptions.SSLError as exc:
            self._log_failure(request_id, "tls_error")
            raise CompanyApiContractError(
                "company_api_automation_tls_error",
                code="tls_error",
                request_id=request_id,
            ) from exc
        except (
            requests.exceptions.ConnectTimeout,
            requests.exceptions.ConnectionError,
        ) as exc:
            self._log_failure(request_id, "connection_failed")
            raise CompanyApiAvailabilityError(
                "company_api_automation_connection_failed",
                code="connection_failed",
                request_id=request_id,
            ) from exc
        except requests.exceptions.RequestException as exc:
            self._log_failure(request_id, "transport_error")
            raise CompanyApiContractError(
                "company_api_automation_transport_error",
                code="transport_error",
                request_id=request_id,
            ) from exc

        status = int(getattr(response, "status_code", 0) or 0)
        if status == 200:
            return _deserialize_cycle_result(response, request_id, cycle)
        problem = _deserialize_problem(response, request_id)
        code = str(problem["code"])
        self._log_failure(request_id, code, status=status)
        if status in {401, 403}:
            raise CompanyApiPolicyError(
                "company_api_automation_policy_rejected",
                status=status,
                code=code,
                request_id=request_id,
            )
        if status >= 500:
            raise CompanyApiAvailabilityError(
                "company_api_automation_server_failed",
                status=status,
                code=code,
                request_id=request_id,
            )
        raise CompanyApiContractError(
            "company_api_automation_request_rejected",
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
            session.trust_env = False
            self._thread_local.session = session
        return session

    def _log_failure(
        self,
        request_id: str,
        code: str,
        *,
        status: int | None = None,
    ) -> None:
        # options, receipt, provider payload는 남기지 않고 correlation만 기록한다.
        self._logger.warning(
            "Company automation API failed request_id=%s code=%s status=%s",
            request_id,
            code,
            status if status is not None else "none",
        )


def _build_cycle_payload(
    *,
    tenant_id: str,
    request_id: str,
    cycle: str,
    cycle_key: str,
    scheduled_at: datetime,
    options: Mapping[str, Any],
    receipts: tuple[AutomationRemoteReceipt, ...],
    ack_only: bool,
) -> dict[str, Any]:
    if not _REQUEST_ID_PATTERN.fullmatch(request_id):
        raise CompanyApiContractError("company_api_request_id_invalid")
    if cycle not in COMPANY_AUTOMATION_CYCLES:
        raise CompanyApiContractError("company_api_automation_cycle_invalid")
    if not _CYCLE_KEY_PATTERN.fullmatch(cycle_key):
        raise CompanyApiContractError(
            "company_api_automation_cycle_key_invalid"
        )
    if scheduled_at.tzinfo is None:
        raise CompanyApiContractError(
            "company_api_automation_scheduled_at_invalid"
        )
    receipt_ids = [receipt.delivery_id for receipt in receipts]
    if len(receipt_ids) != len(set(receipt_ids)):
        raise CompanyApiContractError(
            "company_api_automation_receipt_invalid"
        )
    return {
        "tenantId": tenant_id,
        "cycle": cycle,
        "cycleKey": cycle_key,
        "scheduledAt": scheduled_at.isoformat(),
        "options": dict(options),
        "deliveryReceipts": [
            _serialize_receipt(receipt) for receipt in receipts
        ],
        "ackOnly": bool(ack_only),
    }


def _serialize_receipt(
    receipt: AutomationRemoteReceipt,
) -> dict[str, Any]:
    if (
        not _REQUEST_ID_PATTERN.fullmatch(receipt.delivery_id)
        or receipt.status not in {"sent", "failed"}
        or (
            receipt.external_message_id
            and not _EXTERNAL_MESSAGE_ID_PATTERN.fullmatch(
                receipt.external_message_id
            )
        )
    ):
        raise CompanyApiContractError(
            "company_api_automation_receipt_invalid"
        )
    payload: dict[str, Any] = {
        "deliveryId": receipt.delivery_id,
        "status": receipt.status,
        "externalMessageId": receipt.external_message_id,
        "permalink": receipt.permalink,
    }
    if receipt.delivered_at is not None:
        if receipt.delivered_at.tzinfo is None:
            raise CompanyApiContractError(
                "company_api_automation_receipt_invalid"
            )
        payload["deliveredAt"] = receipt.delivered_at.isoformat()
    return payload


def _deserialize_cycle_result(
    response: Any,
    request_id: str,
    expected_cycle: str,
) -> AutomationRemoteResult:
    payload = _safe_response_json(response)
    if set(payload) != {
        "requestId",
        "cycle",
        "outcome",
        "deliveries",
        "metrics",
        "autoRetryAllowed",
    }:
        raise CompanyApiContractError(
            "company_api_automation_response_invalid",
            request_id=request_id,
        )
    if (
        payload.get("requestId") != request_id
        or payload.get("cycle") != expected_cycle
        or payload.get("outcome") not in {"completed", "no_change"}
        or payload.get("autoRetryAllowed") is not False
        or not isinstance(payload.get("deliveries"), list)
        or not isinstance(payload.get("metrics"), dict)
    ):
        raise CompanyApiContractError(
            "company_api_automation_response_invalid",
            request_id=request_id,
        )
    deliveries: list[AutomationDelivery] = []
    for item in payload["deliveries"]:
        if not isinstance(item, dict) or set(item) != {
            "deliveryId",
            "kind",
            "payload",
        }:
            raise CompanyApiContractError(
                "company_api_automation_response_invalid",
                request_id=request_id,
            )
        try:
            deliveries.append(
                AutomationDelivery(
                    delivery_id=str(item["deliveryId"]),
                    kind=str(item["kind"]),
                    payload=item["payload"],
                )
            )
        except Exception as exc:
            raise CompanyApiContractError(
                "company_api_automation_response_invalid",
                request_id=request_id,
            ) from exc
    return AutomationRemoteResult(
        request_id=request_id,
        cycle=expected_cycle,
        outcome=payload["outcome"],
        deliveries=tuple(deliveries),
        metrics=dict(payload["metrics"]),
    )


def _deserialize_problem(
    response: Any,
    request_id: str,
) -> dict[str, Any]:
    payload = _safe_response_json(response)
    expected_keys = {
        "type",
        "title",
        "status",
        "code",
        "requestId",
        "retryable",
    }
    if (
        set(payload) != expected_keys
        or payload.get("requestId") != request_id
        or not isinstance(payload.get("status"), int)
        or payload.get("status")
        != int(getattr(response, "status_code", 0) or 0)
        or not isinstance(payload.get("code"), str)
        or not isinstance(payload.get("retryable"), bool)
    ):
        raise CompanyApiContractError(
            "company_api_problem_invalid",
            request_id=request_id,
        )
    return payload


def _safe_response_json(response: Any) -> dict[str, Any]:
    headers = getattr(response, "headers", {}) or {}
    content_type = str(headers.get("content-type") or "").lower()
    if "application/json" not in content_type and (
        "application/problem+json" not in content_type
    ):
        raise CompanyApiContractError(
            "company_api_response_content_type_invalid"
        )
    try:
        payload = response.json()
    except Exception as exc:
        raise CompanyApiContractError(
            "company_api_response_json_invalid"
        ) from exc
    if not isinstance(payload, dict):
        raise CompanyApiContractError(
            "company_api_response_json_invalid"
        )
    return payload


def _create_traceparent() -> str:
    return f"00-{secrets.token_hex(16)}-{secrets.token_hex(8)}-01"


__all__ = [
    "AutomationRemoteReceipt",
    "AutomationRemoteResult",
    "CompanyAutomationApiClient",
]
