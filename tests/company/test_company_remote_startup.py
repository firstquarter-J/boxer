from __future__ import annotations

from contextlib import ExitStack
import os
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from boxer_company_adapter_slack import company
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiClientSettings,
    CompanyApiContractError,
)


_TOKEN = "service-token-" + ("x" * 40)
def _all_remote_settings() -> CompanyApiClientSettings:
    """runtime 선택지 없는 API transport 설정을 만든다."""

    return CompanyApiClientSettings(
        base_url="http://127.0.0.1:8010",
        token=_TOKEN,
        automation_tenant_id="lifex",
    )


class CompanyRemoteStartupTests(unittest.TestCase):
    def test_transport_only_startup_requires_api_owned_scheduler(self) -> None:
        with patch.dict(
            os.environ,
            {"BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "false"},
            clear=False,
        ):
            with self.assertRaisesRegex(
                CompanyApiContractError,
                "company_api_transport_only_remote_required",
            ):
                company._require_transport_only_remote_settings()

    def _create_app(self) -> SimpleNamespace:
        fake_app = SimpleNamespace(client=object())
        settings = _all_remote_settings()
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(company, "_validate_ec2_runtime_aws_env")
            )
            stack.enter_context(
                patch.object(
                    company,
                    "load_company_api_client_settings",
                    return_value=settings,
                )
            )
            preflight = stack.enter_context(
                patch.object(
                    company,
                    "validate_automation_delivery_journal_preflight",
                )
            )
            assistant_client = Mock(name="assistant_client")
            stack.enter_context(
                patch.object(
                    company,
                    "CompanyAssistantApiClient",
                    return_value=assistant_client,
                )
            )
            automation_client = Mock(name="automation_client")
            stack.enter_context(
                patch.object(
                    company,
                    "CompanyAutomationApiClient",
                    return_value=automation_client,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "_build_remote_read_service",
                    return_value=Mock(name="remote_read_service"),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "wrap_company_operations_service",
                    return_value=Mock(name="operation_service"),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "DeviceHealthAlertApiBridge",
                    return_value=Mock(name="action_bridge"),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "build_slack_base_access_runtime",
                    return_value=SimpleNamespace(
                        is_allowed=Mock(return_value=True)
                    ),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "build_hpa_change_remote_routes_config",
                    return_value=SimpleNamespace(enabled=False),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "create_slack_app",
                    return_value=fake_app,
                )
            )
            reporters = {
                name: stack.enter_context(patch.object(company, name))
                for name in (
                    "attach_weekly_recordings_reporter",
                    "attach_device_health_monitor_reporter",
                    "attach_device_notification_alert_reporter",
                    "attach_daily_device_round_reporter",
                )
            }
            stack.enter_context(
                patch.dict(
                    os.environ,
                    {"BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true"},
                    clear=False,
                )
            )
            stack.enter_context(
                patch.object(
                    company.cs,
                    "AUTOMATION_DELIVERY_STATE_PATH",
                    "/tmp/company-automation-delivery.json",
                )
            )

            app = company.create_app()

        return SimpleNamespace(
            app=app,
            fake_app=fake_app,
            automation_client=automation_client,
            preflight=preflight,
            reporters=reporters,
        )

    def test_create_app_preflights_journal_before_transport_wiring(self) -> None:
        result = self._create_app()

        self.assertIs(result.app, result.fake_app)
        result.preflight.assert_called_once_with(
            state_path="/tmp/company-automation-delivery.json"
        )

    def test_all_reporters_share_one_remote_transport_client(self) -> None:
        result = self._create_app()

        weekly = result.reporters[
            "attach_weekly_recordings_reporter"
        ].call_args.kwargs
        health = result.reporters[
            "attach_device_health_monitor_reporter"
        ].call_args.kwargs
        notification = result.reporters[
            "attach_device_notification_alert_reporter"
        ].call_args.kwargs
        daily = result.reporters[
            "attach_daily_device_round_reporter"
        ].call_args.kwargs
        self.assertIs(weekly["automation_client"], result.automation_client)
        self.assertIs(health["automation_client"], result.automation_client)
        self.assertIs(
            health["notification_automation_client"],
            result.automation_client,
        )
        self.assertIs(
            notification["automation_client"],
            result.automation_client,
        )
        self.assertIs(daily["automation_client"], result.automation_client)

    def test_legacy_no_batch_journal_stops_before_api_clients(self) -> None:
        settings = _all_remote_settings()
        with (
            patch.object(company, "_validate_ec2_runtime_aws_env"),
            patch.object(
                company,
                "load_company_api_client_settings",
                return_value=settings,
            ),
            patch.dict(
                os.environ,
                {"BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true"},
                clear=False,
            ),
            patch.object(
                company,
                "validate_automation_delivery_journal_preflight",
                side_effect=CompanyApiContractError(
                    "company_api_automation_delivery_batch_missing"
                ),
            ),
            patch.object(company, "CompanyAssistantApiClient") as client,
        ):
            with self.assertRaisesRegex(
                CompanyApiContractError,
                "company_api_automation_delivery_batch_missing",
            ):
                company.create_app()

        client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
