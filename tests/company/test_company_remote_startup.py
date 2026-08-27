from __future__ import annotations

from contextlib import ExitStack
from dataclasses import fields, replace
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from boxer_company_adapter_slack import company
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiContractError,
    CompanyApiClientSettings,
)


_TOKEN = "service-token-" + ("x" * 40)
_REPORTER_FLAGS = (
    "WEEKLY_RECORDINGS_REPORT_ENABLED",
    "DEVICE_HEALTH_MONITOR_ENABLED",
    "DEVICE_NOTIFICATION_ALERT_ENABLED",
    "SMS_DELIVERY_REPORTER_ENABLED",
    "DAILY_DEVICE_ROUND_ENABLED",
)


def _all_remote_settings() -> CompanyApiClientSettings:
    base = CompanyApiClientSettings(
        base_url="http://127.0.0.1:8010",
        token=_TOKEN,
        automation_tenant_id="lifex",
    )
    return replace(
        base,
        **{
            item.name: "remote"
            for item in fields(base)
            if item.name.endswith("_mode")
        },
        **{
            item.name: False
            for item in fields(base)
            if item.name.endswith("_fallback_enabled")
        },
        automation_remote_cycles=(
            "weekly_recordings",
            "daily_device_round",
            "device_health_monitor",
            "device_notification_alert",
            "sms_delivery",
        ),
    )


class CompanyRemoteStartupTests(unittest.TestCase):
    def test_transport_only_startup_requires_api_owned_scheduler(self) -> None:
        with patch.object(
            company.cs,
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
            False,
        ):
            with self.assertRaisesRegex(
                CompanyApiContractError,
                "company_api_transport_only_remote_required",
            ):
                company._require_transport_only_remote_settings(
                    _all_remote_settings()
                )

    def _create_app(
        self,
        settings: CompanyApiClientSettings,
        *,
        enabled_reporter: str | None = None,
        enabled_reporters: tuple[str, ...] = (),
        sms_provider: str = "none",
    ) -> SimpleNamespace:
        fake_app = SimpleNamespace(client=object())
        enabled_reporter_names = set(enabled_reporters)
        if enabled_reporter is not None:
            enabled_reporter_names.add(enabled_reporter)
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
            validate_tokens = stack.enter_context(
                patch.object(company, "_validate_tokens")
            )
            stack.enter_context(
                patch.object(company.s, "LLM_PROVIDER", "claude")
            )
            build_claude = stack.enter_context(
                patch.object(
                    company,
                    "_build_claude_client",
                    return_value=object(),
                )
            )
            answer_engine = stack.enter_context(
                patch.object(
                    company,
                    "AnswerEngine",
                    return_value=Mock(),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "CompanyAssistantApiClient",
                    return_value=Mock(),
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
            action_bridge = Mock(name="action_bridge")
            stack.enter_context(
                patch.object(
                    company,
                    "DeviceHealthAlertApiBridge",
                    return_value=action_bridge,
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "BoundedShadowRunner",
                    return_value=Mock(),
                )
            )
            stack.enter_context(
                patch.object(
                    company,
                    "build_slack_base_access_runtime",
                    return_value=Mock(),
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
            reporters: dict[str, Mock] = {}
            for reporter_name in (
                "attach_hpa_change_remote_reporter",
                "attach_weekly_recordings_reporter",
                "attach_device_health_monitor_reporter",
                "attach_device_notification_alert_reporter",
                "attach_daily_device_round_reporter",
            ):
                reporters[reporter_name] = stack.enter_context(
                    patch.object(company, reporter_name)
                )
            stack.enter_context(
                patch.object(
                    company.cs,
                    "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
                    True,
                )
            )
            # reporter 설정 조합만 바꾸고 실제 thread나 외부 접근은 모두 막는다.
            for flag_name in _REPORTER_FLAGS:
                stack.enter_context(
                    patch.object(
                        company.cs,
                        flag_name,
                        flag_name in enabled_reporter_names,
                    )
                )
            stack.enter_context(
                patch.object(
                    company.cs,
                    "DEVICE_HEALTH_MONITOR_SMS_PROVIDER",
                    sms_provider,
                )
            )

            app = company.create_app()

        return SimpleNamespace(
            app=app,
            validate_tokens=validate_tokens,
            build_claude=build_claude,
            answer_engine=answer_engine,
            automation_client=automation_client,
            action_bridge=action_bridge,
            reporters=reporters,
        )

    def test_all_remote_without_reporters_skips_local_dependencies(
        self,
    ) -> None:
        result = self._create_app(_all_remote_settings())

        result.validate_tokens.assert_called_once_with(
            include_llm=False,
            include_data_sources=False,
        )
        result.build_claude.assert_not_called()
        self.assertEqual(
            result.answer_engine.call_args.kwargs["provider"],
            "",
        )

    def test_all_remote_reporters_skip_local_data_validation(self) -> None:
        for reporter_flag in _REPORTER_FLAGS:
            with self.subTest(reporter_flag=reporter_flag):
                result = self._create_app(
                    _all_remote_settings(),
                    enabled_reporter=reporter_flag,
                )

                result.validate_tokens.assert_called_once_with(
                    include_llm=False,
                    include_data_sources=False,
                )
                result.build_claude.assert_not_called()
                self.assertEqual(
                    result.answer_engine.call_args.kwargs["provider"],
                    "",
                )

    def test_partial_automation_rollout_keeps_unlisted_reporter_local(
        self,
    ) -> None:
        settings = replace(
            _all_remote_settings(),
            automation_remote_cycles=("weekly_recordings",),
        )

        result = self._create_app(
            settings,
            enabled_reporter="DAILY_DEVICE_ROUND_ENABLED",
        )

        result.validate_tokens.assert_called_once_with(
            include_llm=False,
            include_data_sources=True,
        )

    def test_reporters_receive_only_their_explicit_cycle_client(self) -> None:
        settings = replace(
            _all_remote_settings(),
            automation_remote_cycles=(
                "weekly_recordings",
                "sms_delivery",
            ),
        )

        result = self._create_app(settings)

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
        self.assertIs(
            weekly["automation_client"],
            result.automation_client,
        )
        self.assertIsNone(health["automation_client"])
        self.assertIs(
            health["sms_delivery_automation_client"],
            result.automation_client,
        )
        self.assertIs(
            health["action_api_bridge"],
            result.action_bridge,
        )
        self.assertIsNone(notification["automation_client"])
        self.assertIsNotNone(notification["auto_sms_sender"])
        self.assertIsNone(daily["automation_client"])

    def test_remote_health_alone_receives_required_action_bridge(self) -> None:
        settings = replace(
            _all_remote_settings(),
            automation_remote_cycles=("device_health_monitor",),
        )

        result = self._create_app(settings)

        health = result.reporters[
            "attach_device_health_monitor_reporter"
        ].call_args.kwargs
        self.assertIs(
            health["automation_client"],
            result.automation_client,
        )
        self.assertIsNone(health["sms_delivery_automation_client"])
        self.assertIs(
            health["action_api_bridge"],
            result.action_bridge,
        )

    def test_daily_and_notification_cycles_are_wired_independently(self) -> None:
        settings = replace(
            _all_remote_settings(),
            automation_remote_cycles=(
                "daily_device_round",
                "device_notification_alert",
            ),
        )

        result = self._create_app(settings)

        health = result.reporters[
            "attach_device_health_monitor_reporter"
        ].call_args.kwargs
        notification = result.reporters[
            "attach_device_notification_alert_reporter"
        ].call_args.kwargs
        daily = result.reporters[
            "attach_daily_device_round_reporter"
        ].call_args.kwargs
        self.assertIsNone(health["automation_client"])
        self.assertIs(
            health["notification_automation_client"],
            result.automation_client,
        )
        self.assertIs(
            health["action_api_bridge"],
            result.action_bridge,
        )
        self.assertIs(
            notification["automation_client"],
            result.automation_client,
        )
        self.assertIs(
            daily["automation_client"],
            result.automation_client,
        )

    def test_remote_sms_rejects_any_enabled_local_producer(self) -> None:
        settings = replace(
            _all_remote_settings(),
            automation_remote_cycles=("sms_delivery",),
        )

        for producer_flag in (
            "DEVICE_HEALTH_MONITOR_ENABLED",
            "DEVICE_NOTIFICATION_ALERT_ENABLED",
        ):
            with self.subTest(producer_flag=producer_flag):
                with self.assertRaisesRegex(
                    CompanyApiContractError,
                    "company_api_remote_sms_with_local_producer_unsafe",
                ):
                    self._create_app(
                        settings,
                        enabled_reporter=producer_flag,
                    )

    def test_enabled_remote_producer_requires_remote_sms_consumer(
        self,
    ) -> None:
        cases = (
            (
                "device_health_monitor",
                "DEVICE_HEALTH_MONITOR_ENABLED",
            ),
            (
                "device_notification_alert",
                "DEVICE_NOTIFICATION_ALERT_ENABLED",
            ),
        )
        for cycle, producer_flag in cases:
            with self.subTest(cycle=cycle):
                settings = replace(
                    _all_remote_settings(),
                    automation_remote_cycles=(cycle,),
                )
                with self.assertRaisesRegex(
                    CompanyApiContractError,
                    "company_api_remote_sms_producer_without_consumer_unsafe",
                ):
                    self._create_app(
                        settings,
                        enabled_reporters=(
                            producer_flag,
                            "SMS_DELIVERY_REPORTER_ENABLED",
                        ),
                    )

    def test_matching_local_and_remote_sms_ownership_are_allowed(self) -> None:
        local_result = self._create_app(
            replace(
                _all_remote_settings(),
                automation_remote_cycles=("weekly_recordings",),
            ),
            enabled_reporters=(
                "DEVICE_HEALTH_MONITOR_ENABLED",
                "SMS_DELIVERY_REPORTER_ENABLED",
            ),
        )
        remote_result = self._create_app(
            replace(
                _all_remote_settings(),
                automation_remote_cycles=(
                    "device_health_monitor",
                    "sms_delivery",
                ),
            ),
            enabled_reporters=(
                "DEVICE_HEALTH_MONITOR_ENABLED",
                "SMS_DELIVERY_REPORTER_ENABLED",
            ),
        )

        local_health = local_result.reporters[
            "attach_device_health_monitor_reporter"
        ].call_args.kwargs
        remote_health = remote_result.reporters[
            "attach_device_health_monitor_reporter"
        ].call_args.kwargs
        self.assertIsNone(local_health["automation_client"])
        self.assertIsNone(local_health["sms_delivery_automation_client"])
        self.assertIs(
            remote_health["automation_client"],
            remote_result.automation_client,
        )
        self.assertIs(
            remote_health["sms_delivery_automation_client"],
            remote_result.automation_client,
        )

    def test_remote_manual_solapi_action_requires_all_sources_and_consumer_remote(
        self,
    ) -> None:
        unsafe_cases = (
            (
                ("weekly_recordings",),
                ("DEVICE_HEALTH_MONITOR_ENABLED",),
            ),
            (
                ("sms_delivery",),
                (
                    "DEVICE_NOTIFICATION_ALERT_ENABLED",
                    "SMS_DELIVERY_REPORTER_ENABLED",
                ),
            ),
            (
                (
                    "device_health_monitor",
                    "sms_delivery",
                ),
                ("DEVICE_HEALTH_MONITOR_ENABLED",),
            ),
        )
        for remote_cycles, enabled_reporters in unsafe_cases:
            with self.subTest(remote_cycles=remote_cycles):
                settings = replace(
                    _all_remote_settings(),
                    automation_remote_cycles=remote_cycles,
                )
                with self.assertRaises(CompanyApiContractError):
                    self._create_app(
                        settings,
                        enabled_reporters=enabled_reporters,
                        sms_provider="solapi",
                    )

        safe_settings = replace(
            _all_remote_settings(),
            automation_remote_cycles=(
                "device_health_monitor",
                "device_notification_alert",
                "sms_delivery",
            ),
        )
        safe_result = self._create_app(
            safe_settings,
            enabled_reporters=(
                "DEVICE_HEALTH_MONITOR_ENABLED",
                "DEVICE_NOTIFICATION_ALERT_ENABLED",
                "SMS_DELIVERY_REPORTER_ENABLED",
            ),
            sms_provider="solapi",
        )
        health = safe_result.reporters[
            "attach_device_health_monitor_reporter"
        ].call_args.kwargs
        self.assertIs(
            health["sms_delivery_automation_client"],
            safe_result.automation_client,
        )

    def test_remote_manual_non_solapi_action_does_not_require_sms_cycle(
        self,
    ) -> None:
        settings = replace(
            _all_remote_settings(),
            automation_remote_cycles=("weekly_recordings",),
        )

        for provider in ("none", "webhook"):
            with self.subTest(provider=provider):
                self._create_app(
                    settings,
                    enabled_reporter="DEVICE_HEALTH_MONITOR_ENABLED",
                    sms_provider=provider,
                )

    def test_any_local_shadow_or_fallback_blocks_production_entry(self) -> None:
        remote = _all_remote_settings()
        cases: tuple[tuple[str, CompanyApiClientSettings], ...] = (
            ("local", replace(remote, notion_mode="local")),
            ("shadow", replace(remote, notion_mode="shadow")),
            (
                "fallback",
                replace(remote, notion_fallback_enabled=True),
            ),
        )

        for name, settings in cases:
            with self.subTest(path=name):
                # production create_app은 local provider 구성까지 진행하지
                # 않고 mode/fallback 경계에서 즉시 fail-closed한다.
                with self.assertRaisesRegex(
                    CompanyApiContractError,
                    "company_api_transport_only_remote_required",
                ):
                    self._create_app(settings)


if __name__ == "__main__":
    unittest.main()
