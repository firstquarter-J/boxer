from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

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
        "assistant.automation.execute",
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
                            "allowAnonymousActor": False,
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

    def test_live_device_requires_strict_dependencies_and_caller_scope(
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
        missing_dependencies = load_company_api_settings(base_env)
        configured = load_company_api_settings(
            {
                **base_env,
                "MDA_GRAPHQL_URL": "https://mda.example.invalid/graphql",
                "MDA_ADMIN_USER_PASSWORD": "not-logged",
                "MDA_SSH_OPEN_HOST": "remotes.example.invalid",
                "DEVICE_SSH_USER": "device-user",
                "DEVICE_SSH_PASSWORD": "not-logged",
                "BOXER_COMPANY_API_DEVICE_SSH_ALLOWED_HOSTS": (
                    "remotes.example.invalid"
                ),
                "BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST": "10.0.0.10",
                "BOXER_COMPANY_API_DEVICE_SSH_KNOWN_HOSTS_PATH": (
                    "/etc/boxer-company-api/device_known_hosts"
                ),
            }
        )
        missing_capabilities = load_company_api_settings(
            {
                **{
                    key: value
                    for key, value in {
                        **base_env,
                        "MDA_GRAPHQL_URL": (
                            "https://mda.example.invalid/graphql"
                        ),
                        "MDA_ADMIN_USER_PASSWORD": "not-logged",
                        "MDA_SSH_OPEN_HOST": "remotes.example.invalid",
                        "DEVICE_SSH_USER": "device-user",
                        "DEVICE_SSH_PASSWORD": "not-logged",
                        "BOXER_COMPANY_API_DEVICE_SSH_ALLOWED_HOSTS": (
                            "remotes.example.invalid"
                        ),
                        "BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST": (
                            "10.0.0.10"
                        ),
                        "BOXER_COMPANY_API_DEVICE_SSH_KNOWN_HOSTS_PATH": (
                            "/etc/boxer-company-api/device_known_hosts"
                        ),
                    }.items()
                    if key != "BOXER_COMPANY_API_CALLERS_JSON"
                },
                "BOXER_COMPANY_API_CALLERS_JSON": self._registry(),
            }
        )

        self.assertEqual(
            missing_dependencies.configuration_error,
            "automation_dependency_configuration_invalid",
        )
        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.live_device_enabled)
        self.assertTrue(configured.strict_ssh_required)
        self.assertTrue(configured.enforce_local_readiness)
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

    def test_operations_requires_audit_caller_and_base_dependencies(
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
        missing_data = load_company_api_settings(
            {
                key: value
                for key, value in base_env.items()
                if key != "S3_QUERY_ENABLED"
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
        self.assertEqual(
            missing_data.configuration_error,
            "operations_dependency_configuration_invalid",
        )

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

    def test_health_automation_requires_redis_mda_and_strict_ssh_config(
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
            "MDA_GRAPHQL_URL": "https://mda.example.invalid/graphql",
            "MDA_ADMIN_USER_PASSWORD": "not-logged",
            "MDA_SSH_OPEN_HOST": "remotes.example.invalid",
            "DEVICE_SSH_USER": "device-user",
            "DEVICE_SSH_PASSWORD": "not-logged",
            "BOXER_COMPANY_API_DEVICE_SSH_ALLOWED_HOSTS": (
                "remotes.example.invalid"
            ),
            "BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST": "10.0.0.10",
            "BOXER_COMPANY_API_DEVICE_SSH_KNOWN_HOSTS_PATH": (
                "/etc/boxer-company-api/device_known_hosts"
            ),
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
        self.assertIsNone(configured.configuration_error)
        self.assertTrue(configured.strict_ssh_required)

        unsafe_connect_host = load_company_api_settings(
            {
                **env,
                "DEVICE_STATE_REDIS_HOST": "redis.internal",
                "BOXER_COMPANY_API_DEVICE_SSH_CONNECT_HOST": "8.8.8.8",
            }
        )
        self.assertEqual(
            unsafe_connect_host.configuration_error,
            "automation_dependency_configuration_invalid",
        )

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
            state_directory = Path(temporary_directory)
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
            state_file.write_text("{}", encoding="utf-8")
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

    def test_anonymous_company_runtime_caller_is_not_configurable_yet(
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
                            "allowAnonymousActor": True,
                            "capabilities": [
                                "assistant.turn.read"
                            ],
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


if __name__ == "__main__":
    unittest.main()
