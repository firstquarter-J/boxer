from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[2]


def test_transport_contracts_import_without_execution_providers() -> None:
    """Slack 계약 import가 회사 실행 provider와 router를 끌어오지 않는다."""

    script = """
import json
import sys
import boxer_company.transport_contracts

forbidden = (
    "boto3", "botocore", "pymysql", "paramiko", "redis",
    "google.auth", "cryptography", "anthropic", "boxer_company.routers",
)
loaded = sorted(
    name for name in sys.modules
    if any(name == prefix or name.startswith(prefix + ".") for prefix in forbidden)
)
print(json.dumps(loaded))
"""
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(completed.stdout) == []


def test_execution_modules_reexport_transport_contracts() -> None:
    """기존 import 계약은 유지하되 값의 정본만 경량 모듈로 둔다."""

    from boxer_company import transport_contracts as contracts
    from boxer_company.assistant import device_file_operations_route
    from boxer_company.assistant import device_health_alert_action_route
    from boxer_company.assistant import device_operations_route
    from boxer_company.assistant import operations
    from boxer_company import notion_workspace_search
    from boxer_company.routers import device_scanner_abi_patch

    assert (
        device_file_operations_route.resolve_device_file_operation_scope
        is contracts.resolve_device_file_operation_scope
    )
    assert (
        device_operations_route.DEVICE_OPERATION_DELIVERY_ACTION
        == contracts.DEVICE_OPERATION_DELIVERY_ACTION
    )
    assert (
        device_health_alert_action_route.DEVICE_HEALTH_ALERT_SMS_ACTION
        == contracts.DEVICE_HEALTH_ALERT_SMS_ACTION
    )
    assert (
        device_scanner_abi_patch._is_device_scanner_abi_patch_intent
        is contracts._is_device_scanner_abi_patch_intent
    )
    assert (
        notion_workspace_search._looks_like_company_notion_search
        is contracts._looks_like_company_notion_search
    )
    assert (
        operations.company_operation_route_names
        is contracts.company_operation_route_names
    )
