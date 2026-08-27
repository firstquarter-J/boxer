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
_REMOTE_ROUTE_MODE_ENV_KEYS = (
    "BOXER_COMPANY_API_NOTION_MODE",
    "BOXER_COMPANY_API_STRUCTURED_MODE",
    "BOXER_COMPANY_API_DEVICE_MODE",
    "BOXER_COMPANY_API_DEVICE_DETAIL_MODE",
    "BOXER_COMPANY_API_RECORDING_FAILURE_MODE",
    "BOXER_COMPANY_API_BARCODE_LOG_MODE",
    "BOXER_COMPANY_API_BARCODE_MODE",
    "BOXER_COMPANY_API_BARCODE_RESIDUAL_MODE",
    "BOXER_COMPANY_API_BARCODE_TIMELINE_MODE",
    "BOXER_COMPANY_API_BARCODE_FREEFORM_MODE",
    "BOXER_COMPANY_API_FREEFORM_MODE",
    "BOXER_COMPANY_API_PLAYBOOK_MODE",
    "BOXER_COMPANY_API_WEEKLY_SUMMARY_MODE",
    "BOXER_COMPANY_API_OPERATIONS_MODE",
    "BOXER_COMPANY_API_AUTOMATION_MODE",
)
_ALL_AUTOMATION_CYCLES = (
    "weekly_recordings",
    "daily_device_round",
    "device_health_monitor",
    "device_notification_alert",
    "sms_delivery",
)


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


def _remote_loader_env(
    tmp_path: Path,
    **overrides: str,
) -> dict[str, str]:
    """production loader의 완전 remote 소유권을 갖춘 env fixture다."""

    env = {
        "BOXER_COMPANY_API_BASE_URL": "http://127.0.0.1:8010",
        "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
        "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
        "BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES": ",".join(
            _ALL_AUTOMATION_CYCLES
        ),
        "BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH": str(
            tmp_path / "automation-deliveries.json"
        ),
        **{key: "remote" for key in _REMOTE_ROUTE_MODE_ENV_KEYS},
    }
    env.update(overrides)
    return env


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
    common = _remote_loader_env(tmp_path)
    missing_tenant = dict(common)
    missing_tenant.pop("BOXER_COMPANY_API_AUTOMATION_TENANT_ID")
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(missing_tenant)
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(
            {
                **common,
                "BOXER_COMPANY_API_AUTOMATION_FALLBACK_ENABLED": "true",
            }
        )

    settings = load_company_api_client_settings(common)
    assert settings.automation_mode == "remote"
    assert settings.automation_remote_cycles == _ALL_AUTOMATION_CYCLES
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
    tmp_path: Path,
    value: str,
) -> None:
    with pytest.raises(
        CompanyApiContractError,
        match="company_api_automation_remote_cycles_invalid",
    ):
        load_company_api_client_settings(
            _remote_loader_env(
                tmp_path,
                BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES=value,
            )
        )


def test_local_automation_cannot_ignore_stale_cycle_allowlist(
    tmp_path: Path,
) -> None:
    # local mode가 잘못된 cycle 설정을 무시하며 rollback 경로를 열지 못한다.
    with pytest.raises(
        CompanyApiContractError,
        match="company_api_transport_only_remote_required",
    ):
        load_company_api_client_settings(
            _remote_loader_env(
                tmp_path,
                BOXER_COMPANY_API_AUTOMATION_MODE="local",
                BOXER_COMPANY_API_AUTOMATION_REMOTE_CYCLES=(
                    "unknown_cycle,unknown_cycle"
                ),
            )
        )


@pytest.mark.parametrize(
    "cycle",
    ("device_health_monitor", "device_notification_alert"),
)
def test_full_remote_automation_couples_action_cycles_to_remote_operations(
    tmp_path: Path,
    cycle: str,
) -> None:
    settings = load_company_api_client_settings(_remote_loader_env(tmp_path))

    assert settings.operations_mode == "remote"
    assert settings.is_automation_cycle_remote(cycle)
    assert settings.is_automation_cycle_remote("weekly_recordings")


def test_remote_automation_rejects_relative_delivery_state_path(
    tmp_path: Path,
) -> None:
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(
            _remote_loader_env(
                tmp_path,
                BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH=(
                    "data/automation-deliveries.json"
                ),
            )
        )

    unsafe = tmp_path / "unsafe.json"
    unsafe.write_text("{}", encoding="utf-8")
    unsafe.chmod(0o644)
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(
            _remote_loader_env(
                tmp_path,
                BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH=str(unsafe),
            )
        )
