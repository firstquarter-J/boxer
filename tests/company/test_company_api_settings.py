from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from boxer_company import settings as company_settings
from boxer_company_api.settings import (
    CompanyApiSettings,
    company_api_local_readiness,
    load_company_api_settings,
)


class CompanyApiSettingsTests(unittest.TestCase):
    _AUTOMATION_CAPABILITIES = [
        "assistant.turn.read",
        "assistant.device.probe",
        "assistant.device.ssh.open",
        "assistant.operation.execute",
        "assistant.device.alert.execute",
        "assistant.automation.transport",
        "assistant.hpa.change.execute",
    ]

    @staticmethod
    def _registry(
        *,
        capabilities: list[str] | None = None,
    ) -> str:
        return json.dumps(
            [
                {
                    "callerId": "slack-prod",
                    "token": "z" * 48,
                    "tenantIds": ["TENANT-1"],
                    "channels": ["slack"],
                    "actorIds": ["*"],
                    "capabilities": capabilities
                    or ["assistant.turn.read"],
                }
            ]
        )

    def test_company_s3_routes_require_both_company_buckets(self) -> None:
        # 공개 data-source validator와 분리된 회사 bucket 경계가 누락을 막는다.
        invalid_pairs = (
            ("", "logs"),
            ("videos", "   "),
            ("REPLACE_ME", "logs"),
            ("videos", "logs-REPLACE_ME"),
        )
        for ultrasound_bucket, log_bucket in invalid_pairs:
            with self.subTest(
                ultrasound_bucket=ultrasound_bucket,
                log_bucket=log_bucket,
            ):
                with (
                    patch.object(
                        company_settings.core_settings,
                        "S3_QUERY_ENABLED",
                        True,
                    ),
                    patch.object(
                        company_settings,
                        "S3_ULTRASOUND_BUCKET",
                        ultrasound_bucket,
                    ),
                    patch.object(
                        company_settings,
                        "S3_LOG_BUCKET",
                        log_bucket,
                    ),
                ):
                    with self.assertRaisesRegex(
                        ValueError,
                        "bucket configuration",
                    ):
                        company_settings.validate_company_data_source_settings()

        with (
            patch.object(
                company_settings.core_settings,
                "S3_QUERY_ENABLED",
                True,
            ),
            patch.object(company_settings, "S3_ULTRASOUND_BUCKET", "videos"),
            patch.object(company_settings, "S3_LOG_BUCKET", "logs"),
        ):
            company_settings.validate_company_data_source_settings()

    def test_api_owned_automation_values_are_not_cached_in_domain_settings(self) -> None:
        # due/feature/options 값은 API scheduler가 env에서 한 번만 읽는다.
        # company domain module에 같은 파싱 결과가 생기면 소유권이 다시 갈라진다.
        for api_owned_setting in (
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED",
            "WEEKLY_RECORDINGS_REPORT_ENABLED",
            "WEEKLY_RECORDINGS_REPORT_CHANNEL_ID",
            "WEEKLY_RECORDINGS_REPORT_HOUR_KST",
            "WEEKLY_RECORDINGS_REPORT_MINUTE_KST",
            "SMS_DELIVERY_REPORTER_ENABLED",
            "DAILY_DEVICE_ROUND_ENABLED",
            "DAILY_DEVICE_ROUND_HOUR_KST",
            "DAILY_DEVICE_ROUND_MINUTE_KST",
            "DAILY_DEVICE_ROUND_END_HOUR_KST",
            "DAILY_DEVICE_ROUND_END_MINUTE_KST",
            "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT",
            "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX",
            "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE",
            "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID",
            "DAILY_DEVICE_ROUND_AUTO_POWER_OFF",
            "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN",
            "DEVICE_HEALTH_MONITOR_ENABLED",
            "DEVICE_HEALTH_MONITOR_ALERTS_ENABLED",
            "DEVICE_HEALTH_MONITOR_POLL_INTERVAL_SEC",
            "DEVICE_NOTIFICATION_ALERT_ENABLED",
            "DEVICE_NOTIFICATION_ALERT_POLL_INTERVAL_SEC",
        ):
            self.assertFalse(
                hasattr(company_settings, api_owned_setting),
                api_owned_setting,
            )

    def test_loads_server_side_caller_registry(self) -> None:
        token = "t" * 48
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_HOST": "127.0.0.1",
                "BOXER_COMPANY_API_PORT": "8010",
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                    [
                        {
                            "callerId": "slack-prod",
                            "token": token,
                            "tenantIds": ["TENANT-1"],
                            "channels": ["slack"],
                            "actorIds": ["*"],
                            "capabilities": ["assistant.turn.read"],
                        }
                    ]
                ),
            }
        )

        self.assertIsNone(settings.configuration_error)
        self.assertEqual(settings.host, "127.0.0.1")
        self.assertEqual(settings.port, 8010)
        self.assertEqual(settings.callers[0].caller_id, "slack-prod")
        self.assertEqual(settings.callers[0].token, token)
        self.assertEqual(settings.callers[0].channels, {"slack"})
        self.assertNotIn(token, repr(settings))
        # 자동 cycle을 켜지 않은 assistant-only API는 /var/lib state가
        # 아직 없어도 기존 readiness를 불필요하게 막지 않는다.
        self.assertFalse(settings.automation_storage_required)
        self.assertEqual(settings.automation_enabled_cycles, frozenset())
        self.assertFalse(settings.live_device_enabled)
        self.assertFalse(settings.enforce_local_readiness)
        self.assertTrue(company_api_local_readiness(settings))

    def test_live_device_requires_mda_ssh_dependencies_and_caller_scope(
        self,
    ) -> None:
        base_env = {
            "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                capabilities=self._AUTOMATION_CAPABILITIES
            ),
            "BOXER_COMPANY_API_LIVE_DEVICE_ENABLED": "true",
            "REQUEST_LOG_SQLITE_ENABLED": "true",
            "REQUEST_LOG_SQLITE_PATH": (
                "/var/lib/boxer-company-api/request_log.db"
            ),
        }
        device_dependencies = {
            "MDA_GRAPHQL_URL": "https://mda.example.invalid/graphql",
            "MDA_ADMIN_USER_PASSWORD": "not-logged",
            "MDA_SSH_OPEN_HOST": "remotes.example.invalid",
            "DEVICE_SSH_USER": "device-user",
            "DEVICE_SSH_PASSWORD": "not-logged",
        }
        missing_dependencies = load_company_api_settings(base_env)
        configured = load_company_api_settings(
            {**base_env, **device_dependencies}
        )
        stale_strict_ssh_keys = load_company_api_settings(
            {
                **base_env,
                **device_dependencies,
                # 이전 secret에 남은 strict SSH 키는 더 이상 runtime 설정이나
                # readiness를 바꾸지 않고 무시한다.
                "BOXER_COMPANY_API_DEVICE_SSH_ALLOWED_HOSTS": "not a host",
                "BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST": "8.8.8.8",
                "BOXER_COMPANY_API_DEVICE_SSH_KNOWN_HOSTS_PATH": "relative",
            }
        )
        missing_capabilities = load_company_api_settings(
            {
                **base_env,
                **device_dependencies,
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
            }
        )

        self.assertEqual(
            missing_dependencies.configuration_error,
            "automation_dependency_configuration_invalid",
        )
        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.live_device_enabled)
        self.assertTrue(configured.enforce_local_readiness)
        self.assertIsNone(stale_strict_ssh_keys.configuration_error)
        self.assertEqual(
            missing_capabilities.configuration_error,
            "live_device_caller_configuration_invalid",
        )

    def test_invalid_live_device_flag_fails_closed(self) -> None:
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
                "BOXER_COMPANY_API_LIVE_DEVICE_ENABLED": "sometimes",
            }
        )

        self.assertEqual(settings.callers, ())
        self.assertEqual(
            settings.configuration_error,
            "live_device_configuration_invalid",
        )

    def test_request_log_requires_fixed_systemd_state_path(self) -> None:
        missing_path = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
                "REQUEST_LOG_SQLITE_ENABLED": "true",
            }
        )
        configured = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
                "REQUEST_LOG_SQLITE_ENABLED": "true",
                "REQUEST_LOG_SQLITE_PATH": (
                    "/var/lib/boxer-company-api/request_log.db"
                ),
            }
        )

        self.assertEqual(
            missing_path.configuration_error,
            "request_log_configuration_invalid",
        )
        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.request_log_enabled)

    def test_operations_requires_audit_and_caller_but_keeps_route_kill_switches(
        self,
    ) -> None:
        base_env = {
            "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                capabilities=self._AUTOMATION_CAPABILITIES
            ),
            "BOXER_COMPANY_API_OPERATIONS_ENABLED": "true",
            "REQUEST_LOG_SQLITE_ENABLED": "true",
            "REQUEST_LOG_SQLITE_PATH": (
                "/var/lib/boxer-company-api/request_log.db"
            ),
            "DB_QUERY_ENABLED": "true",
            "DB_HOST": "db.internal",
            "DB_USERNAME": "readonly",
            "DB_PASSWORD": "not-logged",
            "DB_DATABASE": "box",
            "S3_QUERY_ENABLED": "true",
            "AWS_REGION": "ap-northeast-2",
            "S3_ULTRASOUND_BUCKET": "ultrasound-bucket",
            "S3_LOG_BUCKET": "device-log-bucket",
            "APP_USER_API_URL": "https://app-user.example.invalid/query",
            "APP_USER_API_TIMEOUT_SEC": "8",
            "MDA_GRAPHQL_URL": "https://mda.example.invalid/graphql",
            "MDA_ADMIN_USER_PASSWORD": "not-logged",
            "THREAD_PLAYBOOK_LEARNING_ENABLED": "false",
        }
        configured = load_company_api_settings(base_env)
        missing_audit = load_company_api_settings(
            {
                key: value
                for key, value in base_env.items()
                if not key.startswith("REQUEST_LOG_SQLITE_")
            }
        )
        missing_scope = load_company_api_settings(
            {
                **base_env,
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
            }
        )
        disabled_s3 = load_company_api_settings(
            {
                **base_env,
                "S3_QUERY_ENABLED": "false",
            }
        )
        disabled_db = load_company_api_settings(
            {
                **base_env,
                "DB_QUERY_ENABLED": "false",
            }
        )

        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.operations_enabled)
        self.assertEqual(
            missing_audit.configuration_error,
            "request_log_configuration_invalid",
        )
        self.assertEqual(
            missing_scope.configuration_error,
            "operations_caller_configuration_invalid",
        )
        # 기존 Slack처럼 data-source kill switch는 해당 route만 막고
        # operations와 나머지 assistant route의 readiness는 유지한다.
        self.assertIsNone(disabled_s3.configuration_error)
        self.assertTrue(disabled_s3.operations_enabled)
        self.assertIsNone(disabled_db.configuration_error)
        self.assertTrue(disabled_db.operations_enabled)

    def test_automation_state_path_is_fixed_to_systemd_state_directory(
        self,
    ) -> None:
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                    capabilities=self._AUTOMATION_CAPABILITIES
                ),
                "WEEKLY_RECORDINGS_REPORT_ENABLED": "true",
                "BOXER_COMPANY_API_AUTOMATION_STATE_PATH": (
                    "/tmp/automation.json"
                ),
                "SMS_DELIVERY_OUTBOX_PATH": (
                    "/var/lib/boxer-company-api/"
                    "sms_delivery_outbox.json"
                ),
                "DB_QUERY_ENABLED": "true",
                "DB_HOST": "db.internal",
                "DB_USERNAME": "readonly",
                "DB_PASSWORD": "not-logged",
                "DB_DATABASE": "box",
            }
        )

        self.assertEqual(settings.callers, ())
        self.assertEqual(
            settings.configuration_error,
            "automation_state_path_invalid",
        )

    def test_inactive_automation_ignores_stale_dependency_env(self) -> None:
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
                "BOXER_COMPANY_API_AUTOMATION_STATE_PATH": (
                    "/tmp/old-automation.json"
                ),
                "SMS_DELIVERY_OUTBOX_PATH": "/tmp/old-sms-outbox.json",
                "DEVICE_HEALTH_MONITOR_SMS_PROVIDER": "bogus",
            }
        )

        self.assertIsNone(settings.configuration_error)
        self.assertEqual(settings.automation_enabled_cycles, frozenset())
        self.assertEqual(
            settings.automation_state_path,
            "/var/lib/boxer-company-api/automation_state.json",
        )
        self.assertEqual(
            settings.sms_delivery_outbox_path,
            "/var/lib/boxer-company-api/sms_delivery_outbox.json",
        )

    def test_weekly_only_requires_db_but_not_sms_storage_or_provider(
        self,
    ) -> None:
        base_env = {
            "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                capabilities=self._AUTOMATION_CAPABILITIES
            ),
            "WEEKLY_RECORDINGS_REPORT_ENABLED": "true",
        }
        missing_db = load_company_api_settings(base_env)
        configured = load_company_api_settings(
            {
                **base_env,
                # 이전 SMS env가 남아 있어도 주간 집계의 DB/readiness 경계와
                # 무관하므로 검증하거나 실제 경로로 채택하지 않는다.
                "SMS_DELIVERY_OUTBOX_PATH": "/tmp/old-outbox.json",
                "DEVICE_HEALTH_MONITOR_SMS_PROVIDER": "bogus",
                "DB_QUERY_ENABLED": "true",
                "DB_HOST": "db.internal",
                "DB_USERNAME": "readonly",
                "DB_PASSWORD": "not-logged",
                "DB_DATABASE": "box",
            }
        )

        self.assertEqual(
            missing_db.configuration_error,
            "automation_dependency_configuration_invalid",
        )
        self.assertIsNone(configured.configuration_error)
        self.assertEqual(
            configured.automation_enabled_cycles,
            frozenset({"weekly_recordings"}),
        )
        self.assertFalse(configured.sms_delivery_storage_required)
        self.assertEqual(
            configured.sms_delivery_outbox_path,
            "/var/lib/boxer-company-api/sms_delivery_outbox.json",
        )
        self.assertNotIn("not-logged", repr(configured))

    def test_video_mismatch_alert_requires_parent_cycle_and_fixed_s3_scope(
        self,
    ) -> None:
        base_env = {
            "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                capabilities=self._AUTOMATION_CAPABILITIES
            ),
            "DEVICE_NOTIFICATION_ALERT_ENABLED": "true",
            "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_ENABLED": "true",
            "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC": "1800",
            "DEVICE_NOTIFICATION_VIDEO_MIN_OBJECT_BYTES": "128000",
            "SMS_DELIVERY_OUTBOX_PATH": (
                "/var/lib/boxer-company-api/sms_delivery_outbox.json"
            ),
            "DB_QUERY_ENABLED": "true",
            "DB_HOST": "db.internal",
            "DB_USERNAME": "readonly",
            "DB_PASSWORD": "not-logged",
            "DB_DATABASE": "box",
            "AWS_REGION": "ap-northeast-2",
            "S3_ULTRASOUND_BUCKET": "ultrasound-prod-kr",
            "S3_ULTRASOUND_BUCKET_OWNER_ID": "123456789012",
        }

        configured = load_company_api_settings(base_env)
        invalid_variants = (
            {
                **base_env,
                "DEVICE_NOTIFICATION_ALERT_ENABLED": "false",
            },
            {**base_env, "S3_ULTRASOUND_BUCKET": ""},
            {**base_env, "S3_ULTRASOUND_BUCKET_OWNER_ID": "not-an-owner"},
            {
                **base_env,
                "DEVICE_NOTIFICATION_VIDEO_DURATION_MISMATCH_GRACE_SEC": "59",
            },
            {
                **base_env,
                "DEVICE_NOTIFICATION_VIDEO_MIN_OBJECT_BYTES": "0",
            },
        )

        self.assertIsNone(configured.configuration_error)
        self.assertEqual(
            configured.automation_enabled_cycles,
            frozenset({"device_notification_alert"}),
        )
        for invalid in invalid_variants:
            with self.subTest(invalid=invalid):
                settings = load_company_api_settings(invalid)
                self.assertEqual(
                    settings.configuration_error,
                    "automation_dependency_configuration_invalid",
                )

    def test_health_automation_requires_redis_and_mda_ssh_config(
        self,
    ) -> None:
        env = {
            "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                capabilities=self._AUTOMATION_CAPABILITIES
            ),
            "SMS_DELIVERY_OUTBOX_PATH": (
                "/var/lib/boxer-company-api/sms_delivery_outbox.json"
            ),
            "DB_QUERY_ENABLED": "true",
            "DB_HOST": "db.internal",
            "DB_USERNAME": "readonly",
            "DB_PASSWORD": "not-logged",
            "DB_DATABASE": "box",
            "DEVICE_HEALTH_MONITOR_ENABLED": "true",
            "DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR": (
                "/var/lib/boxer-company-api/device-health-events"
            ),
            "MDA_GRAPHQL_URL": "https://mda.example.invalid/graphql",
            "MDA_ADMIN_USER_PASSWORD": "not-logged",
            "MDA_SSH_OPEN_HOST": "remotes.example.invalid",
            "DEVICE_SSH_USER": "device-user",
            "DEVICE_SSH_PASSWORD": "not-logged",
            "DEVICE_STATE_REDIS_PORT": "6379",
            "DEVICE_STATE_REDIS_TLS": "true",
        }
        missing_outbox = load_company_api_settings(
            {
                **{
                    key: value
                    for key, value in env.items()
                    if key != "SMS_DELIVERY_OUTBOX_PATH"
                },
                "DEVICE_STATE_REDIS_HOST": "redis.internal",
            }
        )
        missing_redis = load_company_api_settings(env)
        unsafe_event_log = load_company_api_settings(
            {
                **env,
                "DEVICE_STATE_REDIS_HOST": "redis.internal",
                "DEVICE_HEALTH_MONITOR_EVENT_LOG_DIR": (
                    "/opt/boxer-company-api/app/data"
                ),
            }
        )
        configured = load_company_api_settings(
            {**env, "DEVICE_STATE_REDIS_HOST": "redis.internal"}
        )

        self.assertEqual(
            missing_outbox.configuration_error,
            "automation_storage_configuration_invalid",
        )
        self.assertEqual(
            missing_redis.configuration_error,
            "automation_dependency_configuration_invalid",
        )
        self.assertEqual(
            unsafe_event_log.configuration_error,
            "automation_dependency_configuration_invalid",
        )
        self.assertIsNone(configured.configuration_error)

        stale_strict_ssh_keys = load_company_api_settings(
            {
                **env,
                "DEVICE_STATE_REDIS_HOST": "redis.internal",
                "BOXER_COMPANY_API_DEVICE_SSH_ALLOWED_HOSTS": "not a host",
                "BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST": "8.8.8.8",
                "BOXER_COMPANY_API_DEVICE_SSH_KNOWN_HOSTS_PATH": "relative",
            }
        )
        self.assertIsNone(stale_strict_ssh_keys.configuration_error)

    def test_enabled_automation_requires_exact_slack_caller_capabilities(
        self,
    ) -> None:
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                    capabilities=[
                        *self._AUTOMATION_CAPABILITIES,
                        "assistant.unexpected.execute",
                    ]
                ),
                "WEEKLY_RECORDINGS_REPORT_ENABLED": "true",
                "SMS_DELIVERY_OUTBOX_PATH": (
                    "/var/lib/boxer-company-api/"
                    "sms_delivery_outbox.json"
                ),
                "DB_QUERY_ENABLED": "true",
                "DB_HOST": "db.internal",
                "DB_USERNAME": "readonly",
                "DB_PASSWORD": "not-logged",
                "DB_DATABASE": "box",
            }
        )

        self.assertEqual(settings.callers, ())
        self.assertEqual(
            settings.configuration_error,
            "automation_caller_configuration_invalid",
        )

    def test_automation_requires_transport_and_hpa_capabilities(
        self,
    ) -> None:
        base_env = {
            "WEEKLY_RECORDINGS_REPORT_ENABLED": "true",
            "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true",
            "SMS_DELIVERY_OUTBOX_PATH": (
                "/var/lib/boxer-company-api/sms_delivery_outbox.json"
            ),
            "DB_QUERY_ENABLED": "true",
            "DB_HOST": "db.internal",
            "DB_USERNAME": "readonly",
            "DB_PASSWORD": "not-logged",
            "DB_DATABASE": "box",
        }
        configured = load_company_api_settings(
            {
                **base_env,
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                    capabilities=self._AUTOMATION_CAPABILITIES
                ),
            }
        )
        missing_transport = load_company_api_settings(
            {
                **base_env,
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                    capabilities=[
                        capability
                        for capability in self._AUTOMATION_CAPABILITIES
                        if capability != "assistant.automation.transport"
                    ]
                ),
            }
        )
        missing_hpa_capability = load_company_api_settings(
            {
                **base_env,
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                    capabilities=[
                        capability
                        for capability in self._AUTOMATION_CAPABILITIES
                        if capability != "assistant.hpa.change.execute"
                    ]
                ),
            }
        )

        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.automation_scheduler_enabled)
        self.assertEqual(
            missing_transport.configuration_error,
            "automation_caller_configuration_invalid",
        )
        self.assertEqual(
            missing_hpa_capability.configuration_error,
            "automation_caller_configuration_invalid",
        )

    def test_scheduler_keeps_transport_storage_when_all_cycles_are_off(
        self,
    ) -> None:
        configured = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                    capabilities=self._AUTOMATION_CAPABILITIES
                ),
                "BOXER_COMPANY_API_AUTOMATION_SCHEDULER_ENABLED": "true",
            }
        )

        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.automation_scheduler_enabled)
        self.assertTrue(configured.automation_storage_required)
        self.assertTrue(configured.enforce_local_readiness)
        self.assertEqual(configured.automation_enabled_cycles, frozenset())

    def test_enabled_sheet_requires_absolute_google_adc_path(self) -> None:
        base_env = {
            "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
            "DEVICE_HEALTH_SHEET_ENABLED": "true",
            "DEVICE_HEALTH_SHEET_SPREADSHEET_ID": "sheet-id",
            "DEVICE_HEALTH_SHEET_TAB_NAME": "alerts",
        }
        invalid = load_company_api_settings(
            {
                **base_env,
                "GOOGLE_APPLICATION_CREDENTIALS": "credentials.json",
            }
        )
        configured = load_company_api_settings(
            {
                **base_env,
                "GOOGLE_APPLICATION_CREDENTIALS": (
                    "/etc/boxer-company-api/google-credentials.json"
                ),
            }
        )

        self.assertEqual(
            invalid.configuration_error,
            "automation_dependency_configuration_invalid",
        )
        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.device_health_sheet_enabled)

    def test_sms_delivery_drain_requires_solapi_sheet_and_explicit_flag(
        self,
    ) -> None:
        base_env = {
            "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                capabilities=self._AUTOMATION_CAPABILITIES
            ),
            "SMS_DELIVERY_REPORTER_ENABLED": "true",
            "SMS_DELIVERY_OUTBOX_PATH": (
                "/var/lib/boxer-company-api/sms_delivery_outbox.json"
            ),
        }
        missing_dependencies = load_company_api_settings(base_env)
        configured = load_company_api_settings(
            {
                **base_env,
                "DEVICE_HEALTH_MONITOR_SMS_PROVIDER": "solapi",
                "SOLAPI_API_KEY": "key",
                "SOLAPI_API_SECRET": "secret",
                "SOLAPI_FROM_NUMBER": "0212345678",
                "SOLAPI_BASE_URL": "https://api.solapi.com",
                "DEVICE_HEALTH_SHEET_ENABLED": "true",
                "DEVICE_HEALTH_SHEET_SPREADSHEET_ID": "sheet-id",
                "DEVICE_HEALTH_SHEET_TAB_NAME": "alerts",
                "GOOGLE_APPLICATION_CREDENTIALS": (
                    "/etc/boxer-company-api/google-credentials.json"
                ),
            }
        )

        self.assertEqual(
            missing_dependencies.configuration_error,
            "automation_dependency_configuration_invalid",
        )
        self.assertIsNone(configured.configuration_error)
        self.assertEqual(
            configured.automation_enabled_cycles,
            frozenset({"sms_delivery"}),
        )

    def test_local_readiness_requires_private_writable_state_directory(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            # macOS의 /var symlink 자체는 protected-parent 검증 대상이므로
            # 실제 inode 경로를 사용해 Linux StateDirectory 조건을 재현한다.
            state_directory = Path(temporary_directory).resolve()
            os.chmod(state_directory, 0o700)
            settings = CompanyApiSettings(
                host="127.0.0.1",
                port=8010,
                callers=(),
                automation_state_path=str(
                    state_directory / "automation_state.json"
                ),
                sms_delivery_outbox_path=str(
                    state_directory / "sms_delivery_outbox.json"
                ),
                enforce_local_readiness=True,
                automation_storage_required=True,
                sms_delivery_storage_required=True,
            )

            self.assertTrue(company_api_local_readiness(settings))
            state_file = Path(settings.automation_state_path)
            sms_file = Path(settings.sms_delivery_outbox_path)
            state_file.write_text(
                '{"version":1,"cycles":{}}',
                encoding="utf-8",
            )
            os.chmod(state_file, 0o400)
            self.assertFalse(company_api_local_readiness(settings))
            os.chmod(state_file, 0o600)
            sms_file.write_text("{}", encoding="utf-8")
            os.chmod(sms_file, 0o600)
            self.assertTrue(company_api_local_readiness(settings))
            # owner만 접근할 수 있어도 executable bit가 붙은 0700은 runtime
            # JSON 계약과 달라 automation/SMS 어느 파일에서도 거부한다.
            os.chmod(state_file, 0o700)
            self.assertFalse(company_api_local_readiness(settings))
            os.chmod(state_file, 0o600)
            os.chmod(sms_file, 0o700)
            self.assertFalse(company_api_local_readiness(settings))
            os.chmod(sms_file, 0o600)
            os.chmod(state_directory, 0o755)
            self.assertFalse(company_api_local_readiness(settings))

    def test_readiness_rejects_legacy_pending_without_rewriting_state(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_directory = Path(temporary_directory).resolve()
            os.chmod(state_directory, 0o700)
            state_path = state_directory / "automation_state.json"
            settings = CompanyApiSettings(
                host="127.0.0.1",
                port=8010,
                callers=(),
                automation_state_path=str(state_path),
                enforce_local_readiness=True,
                automation_storage_required=True,
            )
            tenant_id = "TENANT-1"
            cycle = "device_notification_alert"
            cycle_key = "continuous"
            delivery_id = "device_notification_alert:event:1"
            state_key = hashlib.sha256(
                "\0".join((tenant_id, cycle, cycle_key)).encode("utf-8")
            ).hexdigest()
            batch_id = "batch:" + hashlib.sha256(
                "\0".join(
                    (tenant_id, cycle, cycle_key, delivery_id)
                ).encode("utf-8")
            ).hexdigest()
            pending = [
                {
                    "deliveryId": delivery_id,
                    "kind": cycle,
                    "payload": {"alert": {"device": "MB2-TEST"}},
                }
            ]
            current_state = {
                "identity": {
                    "tenantId": tenant_id,
                    "cycle": cycle,
                    "cycleKey": cycle_key,
                },
                "deliveryTarget": {
                    "channelId": "C123456",
                    "conversation": {"mode": "root"},
                },
                "pendingDeliveries": pending,
                "pendingScheduledAt": "2026-08-27T14:00:00+09:00",
                "pendingBatchId": batch_id,
            }

            def _write_state(state: dict[str, object]) -> bytes:
                raw = json.dumps(
                    {"version": 1, "cycles": {state_key: state}},
                    sort_keys=True,
                ).encode("utf-8")
                state_path.write_bytes(raw)
                os.chmod(state_path, 0o600)
                return raw

            _write_state(current_state)
            self.assertTrue(company_api_local_readiness(settings))

            missing_batch = dict(current_state)
            missing_batch.pop("pendingBatchId")
            missing_batch_raw = _write_state(missing_batch)
            self.assertFalse(company_api_local_readiness(settings))
            self.assertEqual(state_path.read_bytes(), missing_batch_raw)

            # 구 /cycles pending은 identity/target/batch 정본이 없어도 비어
            # 있다고 간주하지 않고, 운영자가 exact 복원할 때까지 닫는다.
            legacy_raw = _write_state(
                {
                    "pendingDeliveries": pending,
                    "lastCompletedAt": "2026-08-27T14:00:00+09:00",
                }
            )
            self.assertFalse(company_api_local_readiness(settings))
            self.assertEqual(state_path.read_bytes(), legacy_raw)

    def test_weekly_readiness_does_not_touch_unused_sms_outbox(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_directory = Path(temporary_directory)
            os.chmod(state_directory, 0o700)
            settings = CompanyApiSettings(
                host="127.0.0.1",
                port=8010,
                callers=(),
                automation_state_path=str(
                    state_directory / "automation_state.json"
                ),
                sms_delivery_outbox_path="/unavailable/sms-outbox.json",
                enforce_local_readiness=True,
                automation_storage_required=True,
                sms_delivery_storage_required=False,
            )

            self.assertTrue(company_api_local_readiness(settings))

    def test_request_log_readiness_rejects_deleted_runtime_database(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            state_directory = Path(temporary_directory)
            os.chmod(state_directory, 0o700)
            request_log_path = state_directory / "request_log.db"
            request_log_path.write_bytes(b"sqlite-state")
            os.chmod(request_log_path, 0o600)
            settings = CompanyApiSettings(
                host="127.0.0.1",
                port=8010,
                callers=(),
                request_log_enabled=True,
                request_log_path=str(request_log_path),
                enforce_local_readiness=True,
            )

            self.assertTrue(company_api_local_readiness(settings))
            request_log_path.unlink()
            # startup이 만든 감사 DB가 사라지면 다음 write가 빈 정본을
            # 재생성하기 전에 readiness부터 닫혀야 한다.
            self.assertFalse(company_api_local_readiness(settings))

    def test_local_readiness_rejects_insecure_google_adc_when_sheet_enabled(
        self,
    ) -> None:
        settings = CompanyApiSettings(
            host="127.0.0.1",
            port=8010,
            callers=(),
            enforce_local_readiness=True,
            device_health_sheet_enabled=True,
            google_application_credentials_path=(
                "/etc/boxer-company-api/google-credentials.json"
            ),
        )

        # 실제 credential을 읽지 않고 root-owned 파일 정책의 결과만 검증한다.
        with patch(
            "boxer_company_api.settings._secure_json_object_file",
            return_value=False,
        ):
            self.assertFalse(company_api_local_readiness(settings))
        with patch(
            "boxer_company_api.settings._secure_json_object_file",
            return_value=True,
        ):
            self.assertTrue(company_api_local_readiness(settings))

    def test_missing_or_invalid_registry_fails_closed_without_secret_detail(self) -> None:
        cases = (
            ({}, "caller_registry_missing"),
            (
                {
                    "BOXER_COMPANY_API_CALLERS_JSON": (
                        '[{"callerId":"slack","token":"secret-value"}]'
                    )
                },
                "caller_registry_invalid",
            ),
        )
        for env, expected_code in cases:
            with self.subTest(expected_code=expected_code):
                settings = load_company_api_settings(env)

                self.assertEqual(settings.callers, ())
                self.assertEqual(settings.configuration_error, expected_code)
                self.assertNotIn("secret-value", str(settings))

    def test_duplicate_tokens_fail_closed(self) -> None:
        token = "d" * 48
        caller = {
            "token": token,
            "tenantIds": ["TENANT-1"],
            "channels": ["slack"],
            "actorIds": ["*"],
            "capabilities": ["assistant.turn.read"],
        }
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                    [
                        {"callerId": "one", **caller},
                        {"callerId": "two", **caller},
                    ]
                )
            }
        )

        self.assertEqual(settings.callers, ())
        self.assertEqual(
            settings.configuration_error,
            "caller_registry_invalid",
        )

    def test_wildcard_or_missing_turn_capability_fails_closed(self) -> None:
        base_caller = {
            "callerId": "slack-prod",
            "token": "c" * 48,
            "tenantIds": ["TENANT-1"],
            "channels": ["slack"],
            "actorIds": ["*"],
        }
        for capabilities in (
            ["*"],
            ["unrelated.read"],
            ["assistant.device.probe"],
        ):
            with self.subTest(capabilities=capabilities):
                settings = load_company_api_settings(
                    {
                        "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                            [
                                {
                                    **base_caller,
                                    "capabilities": capabilities,
                                }
                            ]
                        )
                    }
                )

                self.assertEqual(settings.callers, ())
                self.assertEqual(
                    settings.configuration_error,
                    "caller_registry_invalid",
                )

    def test_accepts_device_probe_with_required_turn_capability(self) -> None:
        # route별 추가 capability는 기본 turn 권한을 대체하지 않고 함께 보존한다.
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                    [
                        {
                            "callerId": "slack-prod",
                            "token": "p" * 48,
                            "tenantIds": ["TENANT-1"],
                            "channels": ["slack"],
                            "actorIds": ["*"],
                            "capabilities": [
                                "assistant.turn.read",
                                "assistant.device.probe",
                            ],
                        }
                    ]
                )
            }
        )

        self.assertIsNone(settings.configuration_error)
        self.assertEqual(
            settings.callers[0].capabilities,
            {
                "assistant.turn.read",
                "assistant.device.probe",
            },
        )

    def test_loads_device_detail_caller_capabilities_without_store_settings(
        self,
    ) -> None:
        # 단순 이전은 기존 caller registry 권한만 사용하고 별도 action
        # feature flag나 외부 상태 저장소 설정을 요구하지 않는다.
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(
                    capabilities=[
                        "assistant.turn.read",
                        "assistant.device.probe",
                        "assistant.device.ssh.open",
                    ]
                ),
            }
        )

        self.assertIsNone(settings.configuration_error)
        self.assertEqual(
            settings.callers[0].capabilities,
            {
                "assistant.turn.read",
                "assistant.device.probe",
                "assistant.device.ssh.open",
            },
        )

    def test_registry_keeps_capabilities_independent_from_route_policy(
        self,
    ) -> None:
        # registry는 선언 형식만 검증하고 device_detail에 필요한 권한 조합은
        # authorize_turn이 실제 route 요청마다 판정한다.
        caller = json.loads(
            self._registry(
                capabilities=[
                    "assistant.turn.read",
                    "assistant.device.ssh.open",
                ]
            )
        )
        caller[0]["tenantIds"] = ["*"]

        settings = load_company_api_settings(
            {"BOXER_COMPANY_API_CALLERS_JSON": json.dumps(caller)}
        )

        self.assertIsNone(settings.configuration_error)
        self.assertEqual(settings.callers[0].tenant_ids, {"*"})
        self.assertEqual(
            settings.callers[0].capabilities,
            {
                "assistant.turn.read",
                "assistant.device.ssh.open",
            },
        )

    def test_legacy_false_anonymous_actor_field_is_input_compatible(
        self,
    ) -> None:
        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(
                    [
                        {
                            "callerId": "web-anonymous",
                            "token": "a" * 48,
                            "tenantIds": ["TENANT-1"],
                            "channels": ["web"],
                            "actorIds": ["*"],
                            "allowAnonymousActor": False,
                            "capabilities": [
                                "assistant.turn.read"
                            ],
                        }
                    ]
                )
            }
        )

        self.assertIsNone(settings.configuration_error)
        self.assertEqual(len(settings.callers), 1)
        self.assertFalse(
            hasattr(settings.callers[0], "allow_anonymous_actor")
        )

    def test_legacy_true_anonymous_actor_field_is_rejected(
        self,
    ) -> None:
        registry = json.loads(self._registry())
        registry[0]["allowAnonymousActor"] = True

        settings = load_company_api_settings(
            {
                "BOXER_COMPANY_API_CALLERS_JSON": json.dumps(registry),
            }
        )

        self.assertEqual(settings.callers, ())
        self.assertEqual(settings.configuration_error, "caller_registry_invalid")


if __name__ == "__main__":
    unittest.main()
