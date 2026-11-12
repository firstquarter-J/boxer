from __future__ import annotations

import json
import os
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from fastapi.testclient import TestClient

from boxer_company.deployment import (
    get_local_zone,
    get_timezone_label,
    load_deployment_profile,
    validate_deployment_settings,
)
from boxer_company_api.app import (
    _RUNTIME_UNSET,
    _deployment_matches_process,
    _resolve_runtime,
    create_company_api_app,
)
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company_api.schemas import serialize_result
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings, load_company_api_settings
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiClientSettings,
    CompanyApiContractError,
    CompanyAssistantApiClient,
    _validate_response_country,
    validate_company_api_deployment,
)


class DeploymentProfileTests(unittest.TestCase):
    def test_legacy_korean_defaults_and_us_defaults_are_separate(self) -> None:
        # 기존 설치는 설정 추가 없이 KST를 유지하고 US는 한국 URL을 물려받지 않는다.
        kr = load_deployment_profile({})
        us = load_deployment_profile({"BOXER_COMPANY_COUNTRY": "us"})
        self.assertEqual((kr.country, kr.aws_region, kr.timezone_label),
                         ("kr", "ap-northeast-2", "KST"))
        self.assertEqual(us.timezone_name, "America/Los_Angeles")
        self.assertEqual(us.mda_ui_base_url, "https://mda.us.mmtalkbox.com")
        self.assertEqual(us.cdn_base_url, "https://cdn-us.mmtalkbox.com")
        self.assertEqual(us.uploader_base_url, "https://stream.us.mmtalkbox.com")

    def test_timezone_override_and_legacy_alias_share_helpers(self) -> None:
        with patch.dict(os.environ, {"BOXER_COMPANY_COUNTRY": "us",
                                     "TZ": "America/Los_Angeles"}, clear=True):
            self.assertEqual(get_local_zone().key, "America/Los_Angeles")
            self.assertEqual(get_timezone_label(), "America/Los_Angeles")
            with patch.dict(os.environ, {"BOXER_COMPANY_TIMEZONE": "America/New_York"}):
                self.assertEqual(get_local_zone().key, "America/New_York")

    def test_invalid_country_region_or_timezone_does_not_fall_back_to_korea(self) -> None:
        for env in (
            {"BOXER_COMPANY_COUNTRY": "unknown"},
            {"BOXER_COMPANY_COUNTRY": "us", "AWS_REGION": "ap-northeast-2"},
            {"BOXER_COMPANY_COUNTRY": "us", "BOXER_COMPANY_TIMEZONE": "typo/zone"},
            {"BOXER_COMPANY_COUNTRY": "us", "TZ": "Asia/Seoul"},
            {"BOXER_COMPANY_COUNTRY": "kr", "TZ": "typo/zone"},
        ):
            with self.subTest(env=env), self.assertRaisesRegex(ValueError, "configuration_invalid"):
                load_deployment_profile(env)

    @staticmethod
    def _us_env(**overrides: str) -> dict[str, str]:
        env = {
            "BOXER_COMPANY_COUNTRY": "us",
            "AWS_REGION": "us-east-1",
            "BOXER_COMPANY_TIMEZONE": "America/New_York",
            "BOXER_COMPANY_SMS_ENABLED": "false",
            "DEVICE_HEALTH_MONITOR_SMS_PROVIDER": "none",
        }
        env.update(overrides)
        return env

    def test_us_startup_checks_active_data_country_and_known_urls(self) -> None:
        env = self._us_env(
            DB_QUERY_ENABLED="true",
            DB_HOST="cluster.cluster-ro-example.us-east-1.rds.amazonaws.com",
            S3_QUERY_ENABLED="true",
            S3_LOG_BUCKET="mmb-log-prod-us",
            S3_ULTRASOUND_BUCKET="ultrasound-prod-us",
            MDA_GRAPHQL_URL="https://mdaapi.us.mmtalkbox.com/graphql",
            DEVICE_STATE_REDIS_HOST="us-box-endpoint.example.use1.cache.amazonaws.com",
        )
        self.assertEqual(validate_deployment_settings(env, role="api").country, "us")
        for key, value in (
            ("DB_HOST", "cluster.ap-northeast-2.rds.amazonaws.com"),
            ("DB_HOST", ""),
            ("S3_LOG_BUCKET", "mmb-log-prod-kr"),
            ("S3_ULTRASOUND_BUCKET", "ultrasound-prod-kr"),
            ("MDA_GRAPHQL_URL", "https://mdaapi.kr.mmtalkbox.com/graphql"),
            ("MDA_GRAPHQL_URL", "https://mdaapi.us.mmtalkbox.com.evil.test/graphql"),
            ("BABY_MAGIC_CDN_BASE_URL", "https://cdn-kr.mmtalkbox.com"),
            ("DEVICE_STATE_REDIS_HOST", "kr.example.apn2.cache.amazonaws.com"),
            ("REQUEST_LOG_SQLITE_S3_BACKUP_BUCKET", "boxer-request-log-prod-kr"),
            ("APP_USER_API_URL", "https://api.us.mmtalkbox.com/app-user"),
        ):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "configuration_invalid"):
                validate_deployment_settings({**env, key: value}, role="api")

    def test_us_residual_korean_sources_are_rejected_when_query_flags_are_off(self) -> None:
        # 파일 operations가 query flag와 독립적으로 source 설정을 사용해도 국가를 넘지 않는다.
        for key, value in (("DB_HOST", "cluster.ap-northeast-2.rds.amazonaws.com"),
                           ("S3_LOG_BUCKET", "mmb-log-prod-kr"),
                           ("S3_ULTRASOUND_BUCKET", "ultrasound-prod-kr")):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "configuration_invalid"):
                validate_deployment_settings(self._us_env(DB_QUERY_ENABLED="false", S3_QUERY_ENABLED="false", **{key: value}), role="api")

    def test_us_startup_requires_explicit_verified_timezone(self) -> None:
        env = self._us_env()
        env.pop("BOXER_COMPANY_TIMEZONE")
        env["TZ"] = "America/Los_Angeles"
        with self.assertRaisesRegex(ValueError, "configuration_invalid"):
            validate_deployment_settings(env, role="slack")

    def test_cdn_source_policy_uses_only_current_country_canonical_host(self) -> None:
        from boxer_company.read_routing import is_safe_baby_magic_source_uri

        with patch.dict(os.environ, self._us_env(), clear=True):
            self.assertTrue(is_safe_baby_magic_source_uri("https://cdn-us.mmtalkbox.com/result.jpg"))
            for uri in ("https://cdn-kr.mmtalkbox.com/result.jpg",
                        "https://cdn-us.mmtalkbox.com.evil.test/result.jpg",
                        "https://user:password@cdn-us.mmtalkbox.com/result.jpg",
                        "https://cdn-us.mmtalkbox.com/result.jpg?token=secret",
                        "https://cdn-us.mmtalkbox.com/result.jpg#private"):
                with self.subTest(uri=uri):
                    self.assertFalse(is_safe_baby_magic_source_uri(uri))

    def test_us_disabled_features_cannot_be_opened_by_mistaken_env(self) -> None:
        for key in (
            "BOXER_COMPANY_SMS_ENABLED", "SMS_DELIVERY_REPORTER_ENABLED",
            "BOXER_COMPANY_API_MEMORY_ENABLED", "BOXER_COMPANY_API_MEMORY_READ_ENABLED",
            "BOXER_COMPANY_SLACK_MEMORY_ENABLED", "HPA_CHANGE_REQUEST_ENABLED",
            "DAILY_DEVICE_ROUND_ENABLED", "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT",
            "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE", "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID",
            "DAILY_DEVICE_ROUND_AUTO_POWER_OFF", "DAILY_DEVICE_ROUND_AUTO_CLEANUP_TRASHCAN",
        ):
            with self.subTest(key=key), self.assertRaisesRegex(ValueError, "configuration_invalid"):
                validate_deployment_settings(self._us_env(**{key: "true"}), role="api")
        # provider만 남은 설정도 발송 가능성이 있으므로 미국 startup을 닫는다.
        with self.assertRaisesRegex(ValueError, "configuration_invalid"):
            validate_deployment_settings(self._us_env(DEVICE_HEALTH_MONITOR_SMS_PROVIDER="solapi"), role="api")

    def test_api_settings_report_safe_deployment_error(self) -> None:
        # caller token과 잘못된 endpoint는 응답/오류 문자열에 포함되지 않는다.
        settings = load_company_api_settings(self._us_env(
            DB_QUERY_ENABLED="true", DB_HOST="private.kr.invalid",
            BOXER_COMPANY_API_CALLERS_JSON=json.dumps([{
                "callerId": "slack-us", "token": "x" * 48,
                "tenantIds": ["TENANT-1"], "channels": ["slack"],
                "actorIds": ["*"], "capabilities": ["assistant.turn.read"],
            }]),
        ))
        self.assertFalse(settings.authentication_configured)
        self.assertEqual(settings.configuration_error, "company_deployment_configuration_invalid")


class DeploymentCountryAdmissionTests(unittest.TestCase):
    @staticmethod
    def _app(country: str):
        # runtime과 coordinator는 주입해 국가 admission 자체만 외부 실행 없이 검사한다.
        with patch.dict(os.environ, {"BOXER_COMPANY_COUNTRY": country}, clear=True):
            return create_company_api_app(
                settings=CompanyApiSettings(
                    host="127.0.0.1", port=8010, callers=(),
                    deployment_country=country,
                    deployment_aws_region="us-east-1" if country == "us" else "ap-northeast-2",
                    deployment_timezone_name="America/New_York" if country == "us" else "Asia/Seoul",
                ),
                assistant_runtime=None,
                readiness_probe=lambda: False,
                automation_coordinator=None,
                automation_delivery_broker=None,
                hpa_change_coordinator=None,
            )

    def test_country_mismatch_is_rejected_before_auth_or_payload_execution(self) -> None:
        for country, supplied in (("kr", "us"), ("us", "kr"), ("us", None)):
            with self.subTest(country=country, supplied=supplied):
                client = TestClient(self._app(country))
                headers = {} if supplied is None else {"X-Boxer-Country": supplied}
                for path in (
                    "/internal/v1/assistant/turns",
                    "/internal/v1/automation/deliveries/pull",
                    "/internal/v1/automation/deliveries/ack",
                    "/internal/v1/memory/sources",
                    "/internal/v1/hpa-change/requests",
                ):
                    response = client.post(path, headers=headers, content=b"invalid-json")
                    self.assertEqual(response.status_code, 403)
                    self.assertEqual(response.json()["code"], "deployment_country_mismatch")
                    self.assertEqual(response.headers["X-Boxer-Country"], country)

    def test_korean_rollout_accepts_legacy_headerless_client(self) -> None:
        response = TestClient(self._app("kr")).post("/internal/v1/assistant/turns", json={})
        self.assertNotEqual(response.status_code, 403)
        self.assertEqual(response.headers["X-Boxer-Country"], "kr")

    def test_health_exposes_fixed_country_and_timezone_without_changing_body(self) -> None:
        response = TestClient(self._app("us")).get("/health/live")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["X-Boxer-Country"], "us")
        self.assertEqual(response.headers["X-Boxer-Timezone"], "America/New_York")


class DeploymentClientPreflightTests(unittest.TestCase):
    @staticmethod
    def _settings(country: str = "us") -> CompanyApiClientSettings:
        return CompanyApiClientSettings(
            base_url="http://10.45.1.10:8010", token="x" * 48,
            deployment_country=country,
            deployment_timezone_name="America/Los_Angeles" if country == "us" else "Asia/Seoul",
        )

    def test_korean_startup_does_not_add_network_dependency(self) -> None:
        session = Mock()
        validate_company_api_deployment(self._settings("kr"), session=session)
        session.get.assert_not_called()

    def test_us_startup_checks_country_and_timezone_before_events(self) -> None:
        session = Mock()
        session.get.return_value = SimpleNamespace(status_code=200, headers={
            "X-Boxer-Country": "us", "X-Boxer-Timezone": "America/Los_Angeles"})
        validate_company_api_deployment(self._settings(), session=session)
        call = session.get.call_args
        self.assertEqual(call.args[0], "http://10.45.1.10:8010/health/ready")
        self.assertEqual(call.kwargs["headers"]["X-Boxer-Country"], "us")
        self.assertNotIn("Authorization", call.kwargs["headers"])
        self.assertFalse(call.kwargs["allow_redirects"])

    def test_us_startup_rejects_old_korean_or_unready_api(self) -> None:
        for status, headers in (
            (200, {}), (200, {"X-Boxer-Country": "kr"}),
            (200, {"X-Boxer-Country": "us", "X-Boxer-Timezone": "Asia/Seoul"}),
            (503, {"X-Boxer-Country": "us", "X-Boxer-Timezone": "America/Los_Angeles"}),
        ):
            with self.subTest(status=status, headers=headers):
                session = Mock()
                session.get.return_value = SimpleNamespace(status_code=status, headers=headers)
                with self.assertRaises(CompanyApiContractError):
                    validate_company_api_deployment(self._settings(), session=session)
                self.assertEqual(session.get.call_count, 1)

    def test_client_checks_snapshot_country_in_response(self) -> None:
        # os.environ의 국가와 독립적으로 이미 만들어진 client snapshot을 검증한다.
        with patch.dict(os.environ, {}, clear=True):
            _validate_response_country(SimpleNamespace(headers={"X-Boxer-Country": "us"}), expected_country="us")
            with self.assertRaises(CompanyApiContractError):
                _validate_response_country(SimpleNamespace(headers={"X-Boxer-Country": "kr"}), expected_country="us")

    def test_us_request_uses_client_snapshot_even_when_process_env_is_korean(self) -> None:
        request = CompanyAssistantRequest(
            request_id="country-contract-001", tenant_id="TENANT-1", actor_id="ACTOR-1",
            channel="slack", conversation_id="THREAD-1", question="도움말", locale="ko")
        payload = serialize_result(None, request.request_id)
        response = SimpleNamespace(
            status_code=200,
            headers={"content-type": "application/json", "X-Boxer-Country": "us"},
            content=json.dumps(payload).encode(), json=lambda: payload)
        session = Mock()
        session.post.return_value = response
        with patch.dict(os.environ, {}, clear=True):
            result = CompanyAssistantApiClient(self._settings(), session=session).answer(request)
        self.assertEqual(result.outcome, "no_evidence")
        self.assertEqual(session.post.call_args.kwargs["headers"]["X-Boxer-Country"], "us")
        self.assertEqual(session.post.call_count, 1)


class DeploymentProviderSnapshotTests(unittest.TestCase):
    @staticmethod
    def _settings() -> CompanyApiSettings:
        return CompanyApiSettings(
            host="127.0.0.1", port=8010,
            callers=(CompanyApiCallerSettings(
                caller_id="slack-us", token="x" * 48,
                tenant_ids=frozenset({"TENANT-1"}), channels=frozenset({"slack"}),
                actor_ids=frozenset({"ACTOR-1"}), capabilities=frozenset({"assistant.turn.read"}),
            ),),
            deployment_country="us", deployment_aws_region="us-east-1",
            deployment_timezone_name="America/Los_Angeles",
            automation_storage_required=True, automation_scheduler_enabled=True,
            memory_enabled=True,
        )

    def test_default_providers_do_not_use_korean_env_for_us_snapshot(self) -> None:
        # 명시 설정만 미국으로 바꿔도 한국 provider factory나 상태 파일을 열지 않는다.
        with (
            patch.dict(os.environ, {}, clear=True),
            patch("boxer_company.assistant.factory.create_company_assistant_runtime") as runtime_factory,
            patch("boxer_company_api.app.create_hpa_change_coordinator") as hpa_factory,
            patch("boxer_company_api.app.build_default_automation_cycle_service") as automation_factory,
            patch("boxer_company_api.app._create_memory_service") as memory_factory,
        ):
            app = create_company_api_app(settings=self._settings())
            self.assertEqual(TestClient(app).get("/health/ready").status_code, 503)
            runtime_factory.assert_not_called()
            hpa_factory.assert_not_called()
            automation_factory.assert_not_called()
            memory_factory.assert_not_called()

    def test_runtime_injection_still_uses_supplied_test_boundary(self) -> None:
        # 계약 테스트의 명시 주입 runtime은 process-global provider를 조립하지 않는다.
        supplied = object()
        runtime, ready = _resolve_runtime(self._settings(), supplied)
        self.assertIs(runtime, supplied)
        self.assertTrue(ready)
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_resolve_runtime(self._settings(), _RUNTIME_UNSET), (None, False))

    def test_us_process_and_imported_provider_snapshot_must_both_match(self) -> None:
        from boxer.core import settings as core_settings
        from boxer_company import settings as company_settings

        env = {"BOXER_COMPANY_COUNTRY": "us", "AWS_REGION": "us-east-1",
               "BOXER_COMPANY_TIMEZONE": "America/Los_Angeles",
               "DB_HOST": "cluster.us-east-1.rds.amazonaws.com",
               "MDA_GRAPHQL_URL": "https://mdaapi.us.mmtalkbox.com/graphql"}
        profile = load_deployment_profile(env)
        with (
            patch.dict(os.environ, env, clear=True),
            patch.object(company_settings, "_DEPLOYMENT_PROFILE", profile),
            patch.object(company_settings, "MDA_GRAPHQL_URL", env["MDA_GRAPHQL_URL"]),
            patch.object(core_settings, "AWS_REGION", "us-east-1"),
            patch.object(core_settings, "DB_HOST", env["DB_HOST"]),
        ):
            self.assertTrue(_deployment_matches_process(self._settings()))
            with patch.object(core_settings, "DB_HOST", "cluster.ap-northeast-2.rds.amazonaws.com"):
                self.assertFalse(_deployment_matches_process(self._settings()))
            with patch.object(company_settings, "_DEPLOYMENT_PROFILE", load_deployment_profile({})):
                self.assertFalse(_deployment_matches_process(self._settings()))


if __name__ == "__main__":
    unittest.main()
