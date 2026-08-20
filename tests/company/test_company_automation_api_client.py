from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import logging
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest
import requests

from boxer_company_adapter_slack.automation_api_client import (
    AutomationRemoteReceipt,
    CompanyAutomationApiClient,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAmbiguousTimeoutError,
    CompanyApiClientSettings,
    CompanyApiContractError,
    load_company_api_client_settings,
)


_TOKEN = "automation-token-" + ("x" * 40)
_NOW = datetime(2026, 8, 10, 9, 0, tzinfo=ZoneInfo("Asia/Seoul"))


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


def _settings(**overrides: Any) -> CompanyApiClientSettings:
    values: dict[str, Any] = {
        "base_url": "http://127.0.0.1:8010",
        "token": _TOKEN,
        "automation_mode": "remote",
        "automation_tenant_id": "T1",
        "automation_remote_cycles": ("weekly_recordings",),
        "automation_read_timeout_sec": 1_800.0,
        "max_retries": 2,
    }
    values.update(overrides)
    return CompanyApiClientSettings(**values)


def _success_payload() -> dict[str, Any]:
    return {
        "requestId": "automation:weekly:1",
        "cycle": "weekly_recordings",
        "outcome": "completed",
        "deliveries": [
            {
                "deliveryId": "weekly_recordings:2026-08-03",
                "kind": "weekly_recordings_report",
                "payload": {"totalCount": 12},
            }
        ],
        "metrics": {"deliveryCount": 1},
        "autoRetryAllowed": False,
    }


def test_client_posts_cycle_once_with_no_human_actor_or_cursor() -> None:
    session = _FakeSession(_FakeResponse(200, _success_payload()))
    client = CompanyAutomationApiClient(_settings(), session=session)

    result = client.run(
        request_id="automation:weekly:1",
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=_NOW,
    )

    assert result.deliveries[0].payload["totalCount"] == 12
    assert len(session.calls) == 1
    request_payload = session.calls[0]["json"]
    assert request_payload["tenantId"] == "T1"
    assert "actorId" not in request_payload
    assert "cursor" not in request_payload
    assert session.calls[0]["timeout"] == (2.0, 1_800.0)


def test_client_serializes_delivery_ack_without_provider_payload() -> None:
    response_payload = {
        **_success_payload(),
        "requestId": "automation:weekly:ack",
        "outcome": "no_change",
        "deliveries": [],
        "metrics": {"deliveryCount": 0},
    }
    session = _FakeSession(_FakeResponse(200, response_payload))
    client = CompanyAutomationApiClient(_settings(), session=session)

    client.run(
        request_id="automation:weekly:ack",
        cycle="weekly_recordings",
        cycle_key="weekly:2026-08-03",
        scheduled_at=_NOW,
        receipts=(
            AutomationRemoteReceipt(
                delivery_id="weekly_recordings:2026-08-03",
                status="sent",
                external_message_id="171.001",
                permalink="https://example.slack.com/archives/C1/p1",
                delivered_at=_NOW,
            ),
        ),
        ack_only=True,
    )

    receipt = session.calls[0]["json"]["deliveryReceipts"][0]
    assert set(receipt) == {
        "deliveryId",
        "status",
        "externalMessageId",
        "permalink",
        "deliveredAt",
    }
    assert "token" not in str(receipt).lower()


def test_client_never_retries_ambiguous_timeout_even_when_global_retry_is_two(
) -> None:
    session = _FakeSession(requests.exceptions.ReadTimeout("private payload"))
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(CompanyApiAmbiguousTimeoutError):
        client.run(
            request_id="automation:weekly:1",
            cycle="weekly_recordings",
            cycle_key="weekly:2026-08-03",
            scheduled_at=_NOW,
        )

    assert len(session.calls) == 1


def test_client_rejects_secret_field_in_remote_delivery() -> None:
    payload = _success_payload()
    payload["deliveries"][0]["payload"] = {
        "apiToken": "must-not-cross-wire"
    }
    session = _FakeSession(_FakeResponse(200, payload))
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(CompanyApiContractError):
        client.run(
            request_id="automation:weekly:1",
            cycle="weekly_recordings",
            cycle_key="weekly:2026-08-03",
            scheduled_at=_NOW,
        )


def test_client_rejects_cycle_outside_remote_allowlist_before_http() -> None:
    session = _FakeSession()
    client = CompanyAutomationApiClient(_settings(), session=session)

    with pytest.raises(
        CompanyApiContractError,
        match="company_api_automation_cycle_not_remote",
    ):
        client.run(
            request_id="automation:daily:1",
            cycle="daily_device_round",
            cycle_key="daily:2026-08-10",
            scheduled_at=_NOW,
        )

    assert session.calls == []


def test_remote_automation_settings_require_tenant_and_forbid_fallback(
    tmp_path: Path,
) -> None:
    common = {
        "BOXER_COMPANY_API_AUTOMATION_MODE": "remote",
        "BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES": "weekly_recordings",
        "BOXER_COMPANY_API_BASE_URL": "http://127.0.0.1:8010",
        "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
        "BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH": str(
            tmp_path / "automation-deliveries.json"
        ),
    }
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(common)
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(
            {
                **common,
                "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
                "BOXER_COMPANY_API_AUTOMATION_FALLBACK_ENABLED": "true",
            }
        )

    settings = load_company_api_client_settings(
        {
            **common,
            "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
        }
    )
    assert settings.automation_mode == "remote"
    assert settings.automation_remote_cycles == ("weekly_recordings",)
    assert settings.automation_tenant_id == "T1"
    assert settings.automation_read_timeout_sec == 1_800.0


@pytest.mark.parametrize(
    "value",
    (
        "",
        "unknown_cycle",
        "weekly_recordings,weekly_recordings",
        "weekly_recordings,",
    ),
)
def test_remote_automation_rejects_invalid_explicit_cycle_allowlist(
    value: str,
) -> None:
    with pytest.raises(
        CompanyApiContractError,
        match="company_api_automation_remote_cycles_invalid",
    ):
        load_company_api_client_settings(
            {
                "BOXER_COMPANY_API_AUTOMATION_MODE": "remote",
                "BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES": value,
            }
        )


def test_local_automation_ignores_stale_invalid_cycle_allowlist() -> None:
    settings = load_company_api_client_settings(
        {
            "BOXER_COMPANY_API_AUTOMATION_MODE": "local",
            "BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES": (
                "unknown_cycle,unknown_cycle"
            ),
        }
    )

    assert settings.automation_mode == "local"
    assert settings.automation_remote_cycles == ()


@pytest.mark.parametrize(
    "cycle",
    ("device_health_monitor", "device_notification_alert"),
)
def test_remote_action_cycle_requires_remote_operations(
    tmp_path: Path,
    cycle: str,
) -> None:
    common = {
        "BOXER_COMPANY_API_AUTOMATION_MODE": "remote",
        "BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES": cycle,
        "BOXER_COMPANY_API_BASE_URL": "http://127.0.0.1:8010",
        "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
        "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
        "BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH": str(
            tmp_path / "automation-deliveries.json"
        ),
    }
    with pytest.raises(
        CompanyApiContractError,
        match="company_api_remote_automation_requires_remote_operations",
    ):
        load_company_api_client_settings(common)

    settings = load_company_api_client_settings(
        {
            **common,
            "BOXER_COMPANY_API_OPERATIONS_MODE": "remote",
        }
    )
    assert settings.is_automation_cycle_remote(cycle)
    assert not settings.is_automation_cycle_remote("weekly_recordings")


def test_remote_automation_rejects_relative_delivery_state_path(
    tmp_path: Path,
) -> None:
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(
            {
                "BOXER_COMPANY_API_AUTOMATION_MODE": "remote",
                "BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES": "weekly_recordings",
                "BOXER_COMPANY_API_BASE_URL": "http://127.0.0.1:8010",
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
                "BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH": (
                    "data/automation-deliveries.json"
                ),
            }
        )

    unsafe = tmp_path / "unsafe.json"
    unsafe.write_text("{}", encoding="utf-8")
    unsafe.chmod(0o644)
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(
            {
                "BOXER_COMPANY_API_AUTOMATION_MODE": "remote",
                "BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES": "weekly_recordings",
                "BOXER_COMPANY_API_BASE_URL": "http://127.0.0.1:8010",
                "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
                "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
                "BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH": str(unsafe),
            }
        )
