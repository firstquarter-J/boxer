from __future__ import annotations

import importlib
import os
from unittest.mock import Mock, patch

import pytest
from fastapi.testclient import TestClient

from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings

_COMPANIONS = (
    ("automation_scheduler", "DAILY_DEVICE_ROUND_ENABLED", "build_default_automation_cycle_service"),
    ("hpa_change_coordinator_runner", "HPA_CHANGE_REQUEST_ENABLED", "create_hpa_change_coordinator"),
    ("memory_companion", "BOXER_COMPANY_API_MEMORY_ENABLED", "MemoryStore"),
)


@pytest.mark.parametrize("module_name,forbidden_flag,factory_name", _COMPANIONS)
def test_us_companion_rejects_forbidden_feature_before_provider_creation(
    module_name: str, forbidden_flag: str, factory_name: str,
) -> None:
    # API가 503이어도 별도 systemd companion이 금지된 기능을 실행할 수 없어야 한다.
    module = importlib.import_module("boxer_company_api." + module_name)
    environment = {
        "BOXER_SKIP_DOTENV": "true", "BOXER_COMPANY_COUNTRY": "us",
        "AWS_REGION": "us-east-1", "BOXER_COMPANY_TIMEZONE": "America/Los_Angeles",
        forbidden_flag: "true",
    }
    with (
        patch.dict(os.environ, environment, clear=True),
        patch.object(module, "validate_company_api_runtime_security"),
        patch.object(module, factory_name) as factory,
        pytest.raises(ValueError, match="company_deployment_configuration_invalid"),
    ):
        module.main()
    factory.assert_not_called()


def test_valid_korean_scheduler_still_starts_without_new_deployment_keys() -> None:
    from boxer_company_api import automation_scheduler as module

    # 기존 한국 env는 country와 timezone 추가 없이 같은 CLI 조립 경로를 유지한다.
    configured = Mock(enabled_cycles=(), state_path="/synthetic/automation.json")
    with (
        patch.dict(os.environ, {"BOXER_SKIP_DOTENV": "true"}, clear=True),
        patch.object(module, "validate_company_api_runtime_security"),
        patch.object(module, "load_automation_scheduler_settings", return_value=configured),
        patch.object(module, "JsonAutomationCycleStateStore"),
        patch.object(module, "build_default_automation_cycle_service") as factory,
        patch.object(module, "AutomationScheduler") as scheduler,
    ):
        module.main()
    scheduler.return_value.run_forever.assert_called_once_with()
    factory.assert_not_called()


def test_valid_korean_hpa_companion_keeps_existing_provider_boundary() -> None:
    from boxer_company_api import hpa_change_coordinator_runner as module

    coordinator = Mock(enabled=True)
    with (
        patch.dict(os.environ, {"BOXER_SKIP_DOTENV": "true", "HPA_CHANGE_REQUEST_ENABLED": "true"}, clear=True),
        patch.object(module, "validate_company_api_runtime_security"),
        patch.object(module, "create_hpa_change_coordinator", return_value=coordinator) as factory,
        patch.object(module, "HpaChangeCoordinatorRunner") as runner,
    ):
        module.main()
    factory.assert_called_once()
    runner.return_value.run_forever.assert_called_once_with()
    coordinator.close.assert_called_once_with()


def test_valid_korean_memory_companion_keeps_existing_provider_boundary() -> None:
    from boxer_company_api import memory_companion as module

    with (
        patch.dict(os.environ, {"BOXER_SKIP_DOTENV": "true", "BOXER_COMPANY_API_MEMORY_ENABLED": "true"}, clear=True),
        patch.object(module, "validate_company_api_runtime_security"),
        patch.object(module, "MemoryStore") as store,
        patch.object(module, "build_configured_extractor"),
        patch.object(module, "configured_memory_backup"),
        patch.object(module, "MemoryWorker"),
        patch.object(module, "MemoryCompanion") as companion,
        patch.object(module.signal, "signal"),
    ):
        module.main()
    store.assert_called_once_with("/var/lib/boxer-company-api/conversation_memory.db")
    companion.return_value.run_forever.assert_called_once()


def _settings(**options: object) -> CompanyApiSettings:
    caller = CompanyApiCallerSettings(
        caller_id="synthetic", token="s" * 48,
        tenant_ids=frozenset({"TENANT-1"}), channels=frozenset({"slack"}),
        actor_ids=frozenset({"ACTOR-1"}), capabilities=frozenset({"assistant.turn.read"}),
    )
    return CompanyApiSettings(host="127.0.0.1", port=8010, callers=(caller,), **options)


def test_invalid_settings_do_not_construct_any_default_provider_or_storage() -> None:
    settings = _settings(
        configuration_error="company_deployment_configuration_invalid",
        memory_enabled=True, automation_storage_required=True,
        automation_scheduler_enabled=True, request_log_enabled=True,
    )
    # 같은 배포 profile이어도 설정이 거부됐으면 HPA/기억/자동화와 감사 DB를 열지 않는다.
    with (
        patch("boxer_company_api.app._deployment_matches_process", return_value=True),
        patch("boxer_company_api.app._create_memory_service") as memory,
        patch("boxer_company_api.app.create_hpa_change_coordinator") as hpa,
        patch("boxer_company_api.app.build_default_automation_cycle_service") as automation,
        patch("boxer_company_api.app.JsonAutomationCycleStateStore") as store,
        patch("boxer_company_api.app._initialize_request_log_readiness") as request_log,
    ):
        app = create_company_api_app(settings=settings, assistant_runtime=None)
        assert TestClient(app).get("/health/ready").status_code == 503
    for factory in (memory, hpa, automation, store, request_log):
        factory.assert_not_called()


def test_valid_korean_configuration_still_constructs_default_auxiliary_providers() -> None:
    with (
        patch("boxer_company_api.app._deployment_matches_process", return_value=True),
        patch("boxer_company_api.app._create_memory_service", return_value=MemoryService(None)) as memory,
        patch("boxer_company_api.app.create_hpa_change_coordinator", return_value=None) as hpa,
    ):
        app = create_company_api_app(settings=_settings(), assistant_runtime=object(), readiness_probe=lambda: True)
        assert TestClient(app).get("/health/ready").status_code == 200
    memory.assert_called_once()
    hpa.assert_called_once()


def test_explicit_providers_keep_injected_boundary_when_process_profile_differs() -> None:
    settings = _settings(
        deployment_country="us", deployment_aws_region="us-east-1",
        deployment_timezone_name="America/Los_Angeles", automation_storage_required=True,
        automation_scheduler_enabled=True,
    )
    # 계약 테스트가 명시 주입한 provider는 process env의 기본 provider와 분리한다.
    with (
        patch.dict(os.environ, {"BOXER_SKIP_DOTENV": "true"}, clear=True),
        patch("boxer_company_api.app._create_memory_service") as memory,
        patch("boxer_company_api.app.create_hpa_change_coordinator") as hpa,
        patch("boxer_company_api.app.build_default_automation_cycle_service") as automation,
        patch("boxer_company_api.app.JsonAutomationCycleStateStore") as store,
    ):
        app = create_company_api_app(
            settings=settings, assistant_runtime=object(), readiness_probe=lambda: True,
            memory_service=MemoryService(None), hpa_change_coordinator=Mock(enabled=False),
            automation_coordinator=Mock(), automation_delivery_broker=Mock(),
        )
        assert TestClient(app).get("/health/ready").status_code == 200
    for factory in (memory, hpa, automation, store):
        factory.assert_not_called()
