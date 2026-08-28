from __future__ import annotations

from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi.testclient import TestClient

from boxer_company.assistant.contracts import CompanyAssistantResult
from boxer_company.automation import (
    AutomationCycleResult,
    AutomationDelivery,
)
from boxer_company_api.app import create_company_api_app
from boxer_company_api.automation_delivery import AutomationDeliveryBatch
from boxer_company_api.settings import (
    CompanyApiCallerSettings,
    CompanyApiSettings,
)


_TOKEN = "t" * 48
_NOW = datetime(2026, 8, 27, 14, 0, tzinfo=ZoneInfo("Asia/Seoul"))
_BATCH_ID = "batch:" + "a" * 64


class _Runtime:
    def answer(self, _request: Any, **_kwargs: Any) -> CompanyAssistantResult:
        return CompanyAssistantResult(route="none", outcome="no_evidence")

    def answer_stage(
        self,
        _request: Any,
        _stage: str,
        **_kwargs: Any,
    ) -> CompanyAssistantResult:
        return CompanyAssistantResult(route="none", outcome="no_evidence")


class _Broker:
    def __init__(self) -> None:
        self.pull_calls: list[tuple[str, str | None]] = []
        self.ack_calls: list[dict[str, Any]] = []

    def pull(
        self,
        *,
        tenant_id: str,
        cycle: str | None = None,
    ) -> AutomationDeliveryBatch:
        self.pull_calls.append((tenant_id, cycle))
        return AutomationDeliveryBatch(
            batch_id=_BATCH_ID,
            tenant_id="T1",
            cycle="device_notification_alert",
            cycle_key="continuous",
            scheduled_at=_NOW,
            channel_id="C123456",
            deliveries=(
                AutomationDelivery(
                    delivery_id="device_notification_alert:event:1",
                    kind="device_notification_alert",
                    payload={"alert": {"device": "MB2-TEST"}},
                ),
            ),
        )

    def acknowledge(self, **kwargs: Any) -> AutomationCycleResult:
        self.ack_calls.append(dict(kwargs))
        return AutomationCycleResult(
            cycle="device_notification_alert",
            outcome="no_change",
            cursor={},
            metrics={"deliveryCount": 0},
        )


class _EmptyBroker(_Broker):
    def pull(
        self,
        *,
        tenant_id: str,
        cycle: str | None = None,
    ) -> None:
        self.pull_calls.append((tenant_id, cycle))
        return None


def _settings(
    *,
    capabilities: frozenset[str] = frozenset(
        {"assistant.automation.transport"}
    ),
    scheduler_enabled: bool = True,
    enabled_cycles: frozenset[str] = frozenset(
        {"device_notification_alert"}
    ),
) -> CompanyApiSettings:
    return CompanyApiSettings(
        host="127.0.0.1",
        port=8010,
        callers=(
            CompanyApiCallerSettings(
                caller_id="slack-prod",
                token=_TOKEN,
                tenant_ids=frozenset({"T1"}),
                channels=frozenset({"slack"}),
                actor_ids=frozenset({"*"}),
                capabilities=capabilities,
            ),
        ),
        automation_enabled_cycles=enabled_cycles,
        automation_scheduler_enabled=scheduler_enabled,
    )


def _headers(request_id: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_TOKEN}",
        "X-Request-ID": request_id,
    }


def test_pull_returns_server_owned_target_and_pending_batch() -> None:
    broker = _Broker()
    app = create_company_api_app(
        settings=_settings(),
        assistant_runtime=_Runtime(),
        readiness_probe=lambda: True,
        automation_coordinator=None,
        automation_delivery_broker=broker,
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/deliveries/pull",
            headers=_headers("transport:pull:1"),
            json={"tenantId": "T1"},
        )

    assert response.status_code == 200
    assert response.json() == {
        "requestId": "transport:pull:1",
        "batch": {
            "batchId": _BATCH_ID,
            "tenantId": "T1",
            "cycle": "device_notification_alert",
            "cycleKey": "continuous",
            "scheduledAt": _NOW.isoformat(),
            "channelId": "C123456",
            "conversation": {},
            "deliveries": [
                {
                    "deliveryId": "device_notification_alert:event:1",
                    "kind": "device_notification_alert",
                    "payload": {"alert": {"device": "MB2-TEST"}},
                }
            ],
        },
        "autoRetryAllowed": False,
    }
    assert broker.pull_calls == [("T1", None)]


def test_ack_passes_exact_receipts_and_reports_closed_batch() -> None:
    broker = _Broker()
    app = create_company_api_app(
        settings=_settings(),
        assistant_runtime=_Runtime(),
        readiness_probe=lambda: True,
        automation_coordinator=None,
        automation_delivery_broker=broker,
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/deliveries/ack",
            headers=_headers("transport:ack:1"),
            json={
                "tenantId": "T1",
                "batchId": _BATCH_ID,
                "deliveryReceipts": [
                    {
                        "deliveryId": (
                            "device_notification_alert:event:1"
                        ),
                        "status": "sent",
                        "externalMessageId": "1723600000.001",
                        "deliveredAt": _NOW.isoformat(),
                    }
                ],
            },
        )

    assert response.status_code == 200
    assert response.json() == {
        "requestId": "transport:ack:1",
        "batchId": _BATCH_ID,
        "acknowledged": True,
        "pendingDeliveryCount": 0,
        "autoRetryAllowed": False,
    }
    assert broker.ack_calls[0]["tenant_id"] == "T1"
    assert broker.ack_calls[0]["batch_id"] == _BATCH_ID
    assert broker.ack_calls[0]["receipts"][0].status == "sent"


def test_transport_requires_capability_and_scheduler_feature() -> None:
    denied_apps = (
        create_company_api_app(
            settings=_settings(capabilities=frozenset()),
            assistant_runtime=_Runtime(),
            readiness_probe=lambda: True,
            automation_coordinator=None,
            automation_delivery_broker=_Broker(),
        ),
        create_company_api_app(
            settings=_settings(scheduler_enabled=False),
            assistant_runtime=_Runtime(),
            readiness_probe=lambda: True,
            automation_coordinator=None,
            automation_delivery_broker=_Broker(),
        ),
    )

    statuses: list[int] = []
    for index, app in enumerate(denied_apps):
        with TestClient(app) as client:
            statuses.append(
                client.post(
                    "/internal/v1/automation/deliveries/pull",
                    headers=_headers(f"transport:denied:{index}"),
                    json={"tenantId": "T1"},
                ).status_code
            )

    assert statuses == [403, 503]


def test_feature_off_cycle_without_pending_returns_empty_batch() -> None:
    broker = _EmptyBroker()
    app = create_company_api_app(
        settings=_settings(enabled_cycles=frozenset()),
        assistant_runtime=_Runtime(),
        readiness_probe=lambda: True,
        automation_coordinator=None,
        automation_delivery_broker=broker,
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/deliveries/pull",
            headers=_headers("transport:off-empty:1"),
            json={
                "tenantId": "T1",
                "cycle": "device_notification_alert",
            },
        )

    assert response.status_code == 200
    assert response.json()["batch"] is None
    assert broker.pull_calls == [("T1", "device_notification_alert")]


def test_removed_cycle_execute_route_is_not_exposed() -> None:
    app = create_company_api_app(
        settings=_settings(
            capabilities=frozenset(
                {
                    "assistant.automation.transport",
                }
            )
        ),
        assistant_runtime=_Runtime(),
        readiness_probe=lambda: True,
        automation_coordinator=_Broker(),
        automation_delivery_broker=_Broker(),
    )

    with TestClient(app) as client:
        response = client.post(
            "/internal/v1/automation/cycles",
            headers=_headers("transport:legacy:1"),
            json={
                "tenantId": "T1",
                "cycle": "device_notification_alert",
                "cycleKey": "continuous",
                "scheduledAt": _NOW.isoformat(),
            },
        )

    assert response.status_code == 404
