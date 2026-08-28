from __future__ import annotations

from pathlib import Path

import pytest

from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
    load_company_api_client_settings,
)


_TOKEN = "automation-token-" + ("x" * 40)
def _remote_loader_env(
    tmp_path: Path,
    **overrides: str,
) -> dict[str, str]:
    """production loader의 API transport env fixture다."""

    env = {
        "BOXER_COMPANY_API_BASE_URL": "http://127.0.0.1:8010",
        "BOXER_COMPANY_API_SERVICE_TOKEN": _TOKEN,
        "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
        "BOXER_COMPANY_AUTOMATION_DELIVERY_STATE_PATH": str(
            tmp_path / "automation-deliveries.json"
        ),
    }
    env.update(overrides)
    return env


def test_remote_automation_settings_require_tenant(
    tmp_path: Path,
) -> None:
    common = _remote_loader_env(tmp_path)
    missing_tenant = dict(common)
    missing_tenant.pop("BOXER_COMPANY_API_AUTOMATION_TENANT_ID")
    with pytest.raises(CompanyApiContractError):
        load_company_api_client_settings(missing_tenant)
    settings = load_company_api_client_settings(common)
    assert settings.automation_tenant_id == "T1"


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
