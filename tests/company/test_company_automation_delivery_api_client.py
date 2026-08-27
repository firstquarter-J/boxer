from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime
import hashlib
from typing import Any
from zoneinfo import ZoneInfo

import pytest
import requests

from boxer_company_adapter_slack.automation_api_client import (
    AutomationRemoteDeliveryBatchRef,
    AutomationRemoteReceipt,
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiAvailabilityError,
    CompanyApiClientSettings,
    CompanyApiContractError,
)


_TOKEN = "automation-transport-token-" + ("x" * 40)
_NOW = datetime(2026, 8, 27, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))
_DELIVERY_ID = "weekly_recordings:2026-08-24"


@dataclass
class _FakeResponse:
    status_code: int
    payload: Any
    content_type: str = "application/json"

    @property
    def headers(self) -> dict[str, str]:
        return {"content-type": self.content_type}

    def json(self) -> Any:
        return self.payload


class _FakeSession:
    def __init__(self, *results: Any) -> None:
        self.results = list(results)
        self.calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.calls.append({"url": url, **kwargs})
        if not self.results:
            raise AssertionError("unexpected HTTP retry")
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def _settings() -> CompanyApiClientSettings:
    return CompanyApiClientSettings(
        base_url="http://127.0.0.1:8010",
        token=_TOKEN,
        automation_mode="remote",
        automation_tenant_id="T1",
        automation_remote_cycles=(
            "weekly_recordings",
            "daily_device_round",
        ),
        automation_read_timeout_sec=33.0,
        max_retries=2,
    )


def _batch_id(
    *,
    tenant_id: str = "T1",
    cycle: str = "weekly_recordings",
    cycle_key: str = "weekly:2026-08-24",
    delivery_ids: tuple[str, ...] = (_DELIVERY_ID,),
) -> str:
    raw = "\0".join(
        (tenant_id, cycle, cycle_key, *sorted(delivery_ids))
    )
    return f"batch:{hashlib.sha256(raw.encode('utf-8')).hexdigest()}"


def _pull_payload() -> dict[str, Any]:
    return {
        "requestId": "automation:transport:pull:1",
        "batch": {
            "batchId": _batch_id(),
            "tenantId": "T1",
            "cycle": "weekly_recordings",
            "cycleKey": "weekly:2026-08-24",
            "scheduledAt": _NOW.isoformat(),
            "channelId": "C123456",
            "conversation": {},
            "deliveries": [
                {
                    "deliveryId": _DELIVERY_ID,
                    "kind": "weekly_recordings_report",
                    "payload": {"totalCount": 12},
                }
            ],
        },
        "autoRetryAllowed": False,
    }


def _batch_reference() -> AutomationRemoteDeliveryBatchRef:
    return AutomationRemoteDeliveryBatchRef(
        batch_id=_batch_id(),
        tenant_id="T1",
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-24",
        delivery_ids=(_DELIVERY_ID,),
    )


def _receipt() -> AutomationRemoteReceipt:
    return AutomationRemoteReceipt(
        delivery_id=_DELIVERY_ID,
        status="sent",
        external_message_id="172.001",
        permalink="https://example.slack.com/archives/C123456/p1",
        delivered_at=_NOW,
    )


def test_pull_pending_validates_and_returns_api_owned_batch() -> None:
    session = _FakeSession(_FakeResponse(200, _pull_payload()))
    client = CompanyAutomationApiClient(_settings(), session=session)

    batch = client.pull_pending(
        request_id="automation:transport:pull:1",
        cycle="weekly_recordings",
    )

    assert batch is not None
    assert batch.tenant_id == "T1"
    assert batch.channel_id == "C123456"
    assert batch.delivery_ids == (_DELIVERY_ID,)
    assert batch.to_reference() == _batch_reference()
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"].endswith("/internal/v1/automation/deliveries/pull")
    assert call["json"] == {
        "tenantId": "T1",
        "cycle": "weekly_recordings",
    }
    assert call["headers"]["Authorization"] == f"Bearer {_TOKEN}"
    assert all("capability" not in key.lower() for key in call["headers"])
    assert call["timeout"] == (2.0, 33.0)
    assert call["allow_redirects"] is False


def test_pull_pending_returns_none_for_exact_empty_response() -> None:
    session = _FakeSession(
        _FakeResponse(
            200,
            {
                "requestId": "automation:transport:pull:none",
                "batch": None,
                "autoRetryAllowed": False,
            },
        )
    )
    client = CompanyAutomationApiClient(_settings(), session=session)

    assert (
        client.pull_pending(
            request_id="automation:transport:pull:none",
        )
        is None
    )


@pytest.mark.parametrize(
    "case",
    (
        "outer_extra_key",
        "extra_key",
        "tenant_mismatch",
        "cycle_mismatch",
        "channel_invalid",
        "batch_id_invalid",
        "duplicate_delivery",
        "sensitive_delivery",
    ),
)
def test_pull_pending_rejects_mixed_or_unsafe_batch(case: str) -> None:
    payload = deepcopy(_pull_payload())
    batch = payload["batch"]
    if case == "outer_extra_key":
        payload["unexpected"] = True
    elif case == "extra_key":
        batch["unexpected"] = True
    elif case == "tenant_mismatch":
        batch["tenantId"] = "T2"
    elif case == "cycle_mismatch":
        batch["cycle"] = "daily_device_round"
    elif case == "channel_invalid":
        batch["channelId"] = "not-a-slack-channel"
    elif case == "batch_id_invalid":
        batch["batchId"] = "batch:" + ("0" * 64)
    elif case == "duplicate_delivery":
        batch["deliveries"].append(deepcopy(batch["deliveries"][0]))
    elif case == "sensitive_delivery":
        batch["deliveries"][0]["payload"] = {"apiToken": "private"}
    session = _FakeSession(_FakeResponse(200, payload))
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(
        CompanyApiContractError,
        match="company_api_automation_delivery_response_invalid",
    ):
        client.pull_pending(
            request_id="automation:transport:pull:1",
            cycle="weekly_recordings",
        )


def test_acknowledge_batch_accepts_crash_journal_reference() -> None:
    ack_payload = {
        "requestId": "automation:transport:ack:1",
        "batchId": _batch_id(),
        "acknowledged": True,
        "pendingDeliveryCount": 0,
        "autoRetryAllowed": False,
    }
    session = _FakeSession(_FakeResponse(200, ack_payload))
    client = CompanyAutomationApiClient(_settings(), session=session)

    result = client.acknowledge_batch(
        request_id="automation:transport:ack:1",
        batch=_batch_reference(),
        receipts=(_receipt(),),
    )

    assert result.acknowledged is True
    assert result.pending_delivery_count == 0
    assert len(session.calls) == 1
    call = session.calls[0]
    assert call["url"].endswith("/internal/v1/automation/deliveries/ack")
    assert set(call["json"]) == {
        "tenantId",
        "batchId",
        "deliveryReceipts",
    }
    assert call["json"]["tenantId"] == "T1"
    assert call["json"]["batchId"] == _batch_id()
    assert call["json"]["deliveryReceipts"][0]["deliveryId"] == (
        _DELIVERY_ID
    )


def test_acknowledge_batch_rejects_partial_receipt_before_http() -> None:
    second_id = "weekly_recordings:2026-08-24:details"
    reference = AutomationRemoteDeliveryBatchRef(
        batch_id=_batch_id(delivery_ids=(_DELIVERY_ID, second_id)),
        tenant_id="T1",
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-24",
        delivery_ids=(_DELIVERY_ID, second_id),
    )
    session = _FakeSession()
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(
        CompanyApiContractError,
        match="company_api_automation_delivery_receipts_invalid",
    ):
        client.acknowledge_batch(
            request_id="automation:transport:ack:partial",
            batch=reference,
            receipts=(_receipt(),),
        )

    assert session.calls == []


def test_acknowledge_batch_rejects_tampered_journal_reference() -> None:
    reference = AutomationRemoteDeliveryBatchRef(
        batch_id="batch:" + ("0" * 64),
        tenant_id="T1",
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-24",
        delivery_ids=(_DELIVERY_ID,),
    )
    session = _FakeSession()
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(
        CompanyApiContractError,
        match="company_api_automation_delivery_batch_invalid",
    ):
        client.acknowledge_batch(
            request_id="automation:transport:ack:tampered",
            batch=reference,
            receipts=(_receipt(),),
        )

    assert session.calls == []


def test_acknowledge_batch_rejects_inconsistent_ack_response() -> None:
    session = _FakeSession(
        _FakeResponse(
            200,
            {
                "requestId": "automation:transport:ack:invalid",
                "batchId": _batch_id(),
                "acknowledged": True,
                "pendingDeliveryCount": 1,
                "autoRetryAllowed": False,
            },
        )
    )
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(
        CompanyApiContractError,
        match="company_api_automation_delivery_response_invalid",
    ):
        client.acknowledge_batch(
            request_id="automation:transport:ack:invalid",
            batch=_batch_reference(),
            receipts=(_receipt(),),
        )


def test_pull_read_timeout_is_available_error_and_never_retried() -> None:
    session = _FakeSession(requests.exceptions.ReadTimeout("private"))
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(CompanyApiAvailabilityError):
        client.pull_pending(
            request_id="automation:transport:pull:timeout",
            cycle="weekly_recordings",
        )

    assert len(session.calls) == 1


def test_ack_read_timeout_is_ambiguous_and_never_retried() -> None:
    session = _FakeSession(requests.exceptions.ReadTimeout("private"))
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(CompanyApiAmbiguousTimeoutError):
        client.acknowledge_batch(
            request_id="automation:transport:ack:timeout",
            batch=_batch_reference(),
            receipts=(_receipt(),),
        )

    assert len(session.calls) == 1


def test_ack_connection_loss_is_ambiguous_and_never_retried() -> None:
    session = _FakeSession(requests.exceptions.ConnectionError("private"))
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(CompanyApiAmbiguousTimeoutError) as error:
        client.acknowledge_batch(
            request_id="automation:transport:ack:lost",
            batch=_batch_reference(),
            receipts=(_receipt(),),
        )

    assert error.value.code == "connection_lost"
    assert len(session.calls) == 1
