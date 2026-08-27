from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
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
_AUTOMATION_DELIVERY_PULL_PATH = (
    "/internal/v1/automation/deliveries/pull"
)
_AUTOMATION_DELIVERY_ACK_PATH = (
    "/internal/v1/automation/deliveries/ack"
)
_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_CYCLE_KEY_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,191}$"
)
_EXTERNAL_MESSAGE_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}$"
)
_DELIVERY_BATCH_ID_PATTERN = re.compile(r"^batch:[0-9a-f]{64}$")
_DELIVERY_CHANNEL_ID_PATTERN = re.compile(r"^[CGD][A-Z0-9]{5,31}$")
_DELIVERY_CYCLE_KEY_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:@/-]{0,255}$"
)
_SLACK_DELIVERY_CYCLES = frozenset(
    COMPANY_AUTOMATION_CYCLES - {"sms_delivery"}
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


@dataclass(frozen=True, slots=True)
class AutomationRemoteDeliveryBatch:
    """API scheduler가 고정한 Slack 전달 대상과 pending 묶음이다."""

    batch_id: str
    tenant_id: str
    cycle: str
    cycle_key: str
    scheduled_at: datetime
    channel_id: str
    conversation: Mapping[str, Any] = field(default_factory=dict, repr=False)
    deliveries: tuple[AutomationDelivery, ...] = field(
        default_factory=tuple,
        repr=False,
    )

    @property
    def delivery_ids(self) -> tuple[str, ...]:
        return tuple(delivery.delivery_id for delivery in self.deliveries)

    def to_reference(self) -> "AutomationRemoteDeliveryBatchRef":
        """domain payload 없이 crash journal에 보존할 ACK 참조를 만든다."""

        return AutomationRemoteDeliveryBatchRef(
            batch_id=self.batch_id,
            tenant_id=self.tenant_id,
            cycle=self.cycle,
            cycle_key=self.cycle_key,
            delivery_ids=self.delivery_ids,
        )


@dataclass(frozen=True, slots=True)
class AutomationRemoteDeliveryBatchRef:
    """Slack crash journal이 보존하는 provider-free batch 식별자다."""

    batch_id: str
    tenant_id: str
    cycle: str
    cycle_key: str
    delivery_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class AutomationRemoteAckResult:
    """API가 exact batch receipt를 반영한 결과다."""

    request_id: str
    batch_id: str
    acknowledged: bool
    pending_delivery_count: int


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

    def pull_pending(
        self,
        *,
        request_id: str,
        cycle: str | None = None,
    ) -> AutomationRemoteDeliveryBatch | None:
        """API-owned pending을 실행 없이 한 번 조회한다."""

        _validate_delivery_request_id(request_id)
        if cycle is not None and (
            cycle not in _SLACK_DELIVERY_CYCLES
            or cycle not in self._remote_cycles
        ):
            raise CompanyApiContractError(
                "company_api_automation_delivery_cycle_invalid",
                request_id=request_id,
            )
        response = self._post_delivery_once(
            path=_AUTOMATION_DELIVERY_PULL_PATH,
            request_id=request_id,
            payload={
                "tenantId": self._tenant_id,
                "cycle": cycle,
            },
            operation="delivery_pull",
            ambiguous_read_timeout=False,
        )
        return _deserialize_delivery_pull_result(
            response,
            request_id=request_id,
            expected_tenant_id=self._tenant_id,
            expected_cycle=cycle,
            remote_cycles=self._remote_cycles,
        )

    def acknowledge_batch(
        self,
        *,
        request_id: str,
        batch: (
            AutomationRemoteDeliveryBatch
            | AutomationRemoteDeliveryBatchRef
        ),
        receipts: tuple[AutomationRemoteReceipt, ...],
    ) -> AutomationRemoteAckResult:
        """pulled batch 전체의 Slack receipt를 API에 한 번만 반영한다."""

        _validate_delivery_request_id(request_id)
        if not _is_valid_delivery_batch_reference(
            batch,
            expected_tenant_id=self._tenant_id,
            remote_cycles=self._remote_cycles,
        ):
            raise CompanyApiContractError(
                "company_api_automation_delivery_batch_invalid",
                request_id=request_id,
            )
        expected_ids = set(batch.delivery_ids)
        receipt_ids = [receipt.delivery_id for receipt in receipts]
        if (
            not receipts
            or len(receipt_ids) != len(set(receipt_ids))
            or set(receipt_ids) != expected_ids
        ):
            # 부분 ACK는 API cursor와 Slack journal의 소유권을 갈라 놓으므로
            # 네트워크 호출 전에 exact all-or-none으로 차단한다.
            raise CompanyApiContractError(
                "company_api_automation_delivery_receipts_invalid",
                request_id=request_id,
            )
        response = self._post_delivery_once(
            path=_AUTOMATION_DELIVERY_ACK_PATH,
            request_id=request_id,
            payload={
                "tenantId": self._tenant_id,
                "batchId": batch.batch_id,
                "deliveryReceipts": [
                    _serialize_receipt(receipt) for receipt in receipts
                ],
            },
            operation="delivery_ack",
            ambiguous_read_timeout=True,
        )
        return _deserialize_delivery_ack_result(
            response,
            request_id=request_id,
            batch=batch,
        )

    def _post_delivery_once(
        self,
        *,
        path: str,
        request_id: str,
        payload: Mapping[str, Any],
        operation: str,
        ambiguous_read_timeout: bool,
    ) -> Any:
        """transport 요청을 재시도 없이 보내고 공통 problem을 분류한다."""

        headers = {
            "Authorization": f"Bearer {self._token}",
            "X-Request-ID": request_id,
            "traceparent": _create_traceparent(),
            "Accept": "application/json, application/problem+json",
            "Content-Type": "application/json",
        }
        try:
            response = self._session_for_call().post(
                f"{self._base_url}{path}",
                headers=headers,
                json=dict(payload),
                timeout=(
                    self._connect_timeout_sec,
                    self._read_timeout_sec,
                ),
                allow_redirects=False,
            )
        except requests.exceptions.ReadTimeout as exc:
            self._log_failure(request_id, f"{operation}_read_timeout")
            if ambiguous_read_timeout:
                # ACK는 서버가 반영한 뒤 응답만 유실됐을 수 있어 자동으로
                # 다시 보내지 않고 journal을 보존할 불명 오류로 구분한다.
                raise CompanyApiAmbiguousTimeoutError(
                    "company_api_automation_delivery_ack_read_timeout",
                    code="read_timeout",
                    request_id=request_id,
                ) from exc
            raise CompanyApiAvailabilityError(
                "company_api_automation_delivery_pull_read_timeout",
                code="read_timeout",
                request_id=request_id,
            ) from exc
        except requests.exceptions.SSLError as exc:
            self._log_failure(request_id, f"{operation}_tls_error")
            raise CompanyApiContractError(
                "company_api_automation_delivery_tls_error",
                code="tls_error",
                request_id=request_id,
            ) from exc
        except requests.exceptions.ConnectTimeout as exc:
            self._log_failure(request_id, f"{operation}_connection_failed")
            raise CompanyApiAvailabilityError(
                "company_api_automation_delivery_connection_failed",
                code="connection_failed",
                request_id=request_id,
            ) from exc
        except requests.exceptions.ConnectionError as exc:
            self._log_failure(request_id, f"{operation}_connection_lost")
            if ambiguous_read_timeout:
                # 연결 성립 뒤 ACK가 처리됐는지는 판별할 수 없으므로 read
                # timeout과 같은 불명 상태로 두고 journal을 지우지 않는다.
                raise CompanyApiAmbiguousTimeoutError(
                    "company_api_automation_delivery_ack_connection_lost",
                    code="connection_lost",
                    request_id=request_id,
                ) from exc
            raise CompanyApiAvailabilityError(
                "company_api_automation_delivery_connection_failed",
                code="connection_failed",
                request_id=request_id,
            ) from exc
        except requests.exceptions.RequestException as exc:
            self._log_failure(request_id, f"{operation}_transport_error")
            raise CompanyApiContractError(
                "company_api_automation_delivery_transport_error",
                code="transport_error",
                request_id=request_id,
            ) from exc

        status = int(getattr(response, "status_code", 0) or 0)
        if status == 200:
            return response
        problem = _deserialize_problem(response, request_id)
        code = str(problem["code"])
        self._log_failure(request_id, code, status=status)
        if status in {401, 403}:
            raise CompanyApiPolicyError(
                "company_api_automation_delivery_policy_rejected",
                status=status,
                code=code,
                request_id=request_id,
            )
        if status >= 500:
            raise CompanyApiAvailabilityError(
                "company_api_automation_delivery_server_failed",
                status=status,
                code=code,
                request_id=request_id,
            )
        raise CompanyApiContractError(
            "company_api_automation_delivery_request_rejected",
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


def _validate_delivery_request_id(request_id: str) -> None:
    if not _REQUEST_ID_PATTERN.fullmatch(str(request_id or "")):
        raise CompanyApiContractError("company_api_request_id_invalid")


def _deserialize_delivery_pull_result(
    response: Any,
    *,
    request_id: str,
    expected_tenant_id: str,
    expected_cycle: str | None,
    remote_cycles: frozenset[str],
) -> AutomationRemoteDeliveryBatch | None:
    payload = _safe_response_json(response)
    if (
        set(payload) != {"requestId", "batch", "autoRetryAllowed"}
        or payload.get("requestId") != request_id
        or payload.get("autoRetryAllowed") is not False
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    raw_batch = payload.get("batch")
    if raw_batch is None:
        return None
    expected_batch_keys = {
        "batchId",
        "tenantId",
        "cycle",
        "cycleKey",
        "scheduledAt",
        "channelId",
        "conversation",
        "deliveries",
    }
    if not isinstance(raw_batch, dict) or set(raw_batch) != expected_batch_keys:
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )

    tenant_id = raw_batch.get("tenantId")
    cycle = raw_batch.get("cycle")
    cycle_key = raw_batch.get("cycleKey")
    batch_id = raw_batch.get("batchId")
    channel_id = raw_batch.get("channelId")
    conversation = raw_batch.get("conversation")
    raw_deliveries = raw_batch.get("deliveries")
    if (
        tenant_id != expected_tenant_id
        or not isinstance(cycle, str)
        or cycle not in _SLACK_DELIVERY_CYCLES
        or cycle not in remote_cycles
        or (expected_cycle is not None and cycle != expected_cycle)
        or not isinstance(cycle_key, str)
        or not _DELIVERY_CYCLE_KEY_PATTERN.fullmatch(cycle_key)
        or not isinstance(batch_id, str)
        or not _DELIVERY_BATCH_ID_PATTERN.fullmatch(batch_id)
        or not isinstance(channel_id, str)
        or not _DELIVERY_CHANNEL_ID_PATTERN.fullmatch(channel_id)
        or not isinstance(conversation, dict)
        or not isinstance(raw_deliveries, list)
        or not 1 <= len(raw_deliveries) <= 100
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    scheduled_at = _parse_delivery_scheduled_at(
        raw_batch.get("scheduledAt"),
        request_id=request_id,
    )
    deliveries = _deserialize_delivery_items(
        raw_deliveries,
        request_id=request_id,
    )
    result = AutomationRemoteDeliveryBatch(
        batch_id=batch_id,
        tenant_id=tenant_id,
        cycle=cycle,
        cycle_key=cycle_key,
        scheduled_at=scheduled_at,
        channel_id=channel_id,
        conversation=dict(conversation),
        deliveries=deliveries,
    )
    if not _is_valid_delivery_batch_reference(
        result,
        expected_tenant_id=expected_tenant_id,
        remote_cycles=remote_cycles,
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    return result


def _parse_delivery_scheduled_at(
    value: Any,
    *,
    request_id: str,
) -> datetime:
    if not isinstance(value, str):
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    try:
        scheduled_at = datetime.fromisoformat(value)
    except ValueError as exc:
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        ) from exc
    if scheduled_at.tzinfo is None:
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    return scheduled_at


def _deserialize_delivery_items(
    values: list[Any],
    *,
    request_id: str,
) -> tuple[AutomationDelivery, ...]:
    deliveries: list[AutomationDelivery] = []
    for value in values:
        if (
            not isinstance(value, dict)
            or set(value) != {"deliveryId", "kind", "payload"}
            or not isinstance(value.get("deliveryId"), str)
            or not isinstance(value.get("kind"), str)
            or not isinstance(value.get("payload"), dict)
        ):
            raise CompanyApiContractError(
                "company_api_automation_delivery_response_invalid",
                request_id=request_id,
            )
        try:
            deliveries.append(
                AutomationDelivery(
                    delivery_id=value["deliveryId"],
                    kind=value["kind"],
                    payload=value["payload"],
                )
            )
        except Exception as exc:
            raise CompanyApiContractError(
                "company_api_automation_delivery_response_invalid",
                request_id=request_id,
            ) from exc
    delivery_ids = [delivery.delivery_id for delivery in deliveries]
    if len(delivery_ids) != len(set(delivery_ids)):
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    return tuple(deliveries)


def _is_valid_delivery_batch_reference(
    batch: AutomationRemoteDeliveryBatch | AutomationRemoteDeliveryBatchRef,
    *,
    expected_tenant_id: str,
    remote_cycles: frozenset[str],
) -> bool:
    delivery_ids = batch.delivery_ids
    if (
        not isinstance(batch.batch_id, str)
        or not isinstance(batch.tenant_id, str)
        or not isinstance(batch.cycle, str)
        or not isinstance(batch.cycle_key, str)
        or not isinstance(delivery_ids, tuple)
        or batch.tenant_id != expected_tenant_id
        or batch.cycle not in remote_cycles
        or batch.cycle not in _SLACK_DELIVERY_CYCLES
        or not _DELIVERY_CYCLE_KEY_PATTERN.fullmatch(batch.cycle_key)
        or not delivery_ids
        or len(delivery_ids) > 100
        or len(delivery_ids) != len(set(delivery_ids))
        or any(
            not isinstance(delivery_id, str)
            or not _REQUEST_ID_PATTERN.fullmatch(delivery_id)
            for delivery_id in delivery_ids
        )
    ):
        return False
    raw_identity = "\0".join(
        (
            batch.tenant_id,
            batch.cycle,
            batch.cycle_key,
            *sorted(delivery_ids),
        )
    )
    expected_batch_id = (
        "batch:"
        + hashlib.sha256(raw_identity.encode("utf-8")).hexdigest()
    )
    return (
        bool(_DELIVERY_BATCH_ID_PATTERN.fullmatch(batch.batch_id))
        and secrets.compare_digest(batch.batch_id, expected_batch_id)
    )


def _deserialize_delivery_ack_result(
    response: Any,
    *,
    request_id: str,
    batch: AutomationRemoteDeliveryBatch | AutomationRemoteDeliveryBatchRef,
) -> AutomationRemoteAckResult:
    payload = _safe_response_json(response)
    if set(payload) != {
        "requestId",
        "batchId",
        "acknowledged",
        "pendingDeliveryCount",
        "autoRetryAllowed",
    }:
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    acknowledged = payload.get("acknowledged")
    pending_count = payload.get("pendingDeliveryCount")
    if (
        payload.get("requestId") != request_id
        or payload.get("batchId") != batch.batch_id
        or type(acknowledged) is not bool
        or type(pending_count) is not int
        or not 0 <= pending_count <= len(batch.delivery_ids)
        or (acknowledged and pending_count != 0)
        or (not acknowledged and pending_count == 0)
        or payload.get("autoRetryAllowed") is not False
    ):
        raise CompanyApiContractError(
            "company_api_automation_delivery_response_invalid",
            request_id=request_id,
        )
    return AutomationRemoteAckResult(
        request_id=request_id,
        batch_id=batch.batch_id,
        acknowledged=acknowledged,
        pending_delivery_count=pending_count,
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
    "AutomationRemoteAckResult",
    "AutomationRemoteDeliveryBatch",
    "AutomationRemoteDeliveryBatchRef",
    "AutomationRemoteReceipt",
    "AutomationRemoteResult",
    "CompanyAutomationApiClient",
]
