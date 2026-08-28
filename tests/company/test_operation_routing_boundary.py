from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant import operations as execution_operations
from boxer_company.operation_routing import (
    company_operation_legacy_stage,
    match_company_operation_route,
)


def _request(
    question: str,
    *,
    metadata: dict[str, object] | None = None,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="routing-boundary",
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="C1",
        question=question,
        locale="ko",
        metadata=metadata or {},
    )


def test_heavy_operations_module_does_not_reexport_legacy_routing() -> None:
    """실행 모듈은 legacy request shaping 계약을 다시 노출하지 않는다."""

    for name in (
        "CompanyOperationLegacyStage",
        "as_operations_request",
        "company_operation_legacy_stage",
    ):
        assert name not in execution_operations.__dict__


def test_pure_routing_preserves_ambiguous_legacy_priority() -> None:
    """mutation이 섞인 문장에서도 이전 handler 순서를 그대로 고정한다."""

    cases = (
        (
            "이 스레드 학습해. 48194663047 2026-03-06 장비 파일 복구해줘",
            "thread_playbook_learning",
            "pre_notion",
        ),
        (
            "s3 로그 MMB001 2026-03-06 48194663047 장비 파일 복구해줘",
            "admin_s3_device_log",
            "pre_notion",
        ),
        (
            "MB2-C00419 진단 시작하고 48194663047 2026-03-06 장비 파일 복구해줘",
            "device_diagnostic_snapshot",
            "device",
        ),
        (
            "MB2-C00419 음성을 지니로 바꾸고 장비 파일 다운로드해줘",
            "device_file_download_barcode_required",
            "device",
        ),
        (
            "48194663047 유효성 검사하고 2024년 4월 영상 복원해줘",
            "barcode_validation_status",
            "barcode",
        ),
    )
    for question, route, stage in cases:
        request = _request(question)
        assert match_company_operation_route(request) == route
        assert company_operation_legacy_stage(request) == stage


def test_actual_pure_matcher_calls_do_not_import_execution_providers() -> None:
    """모든 matcher 축을 실제 호출해도 provider/router import가 없어야 한다."""

    project_root = Path(__file__).resolve().parents[2]
    script = r'''
from dataclasses import dataclass, field
import json
import sys
from typing import Any

from boxer_company.operation_routing import (
    company_operation_legacy_stage,
    match_company_operation_route,
)

@dataclass(frozen=True)
class Request:
    question: str
    metadata: dict[str, Any] = field(default_factory=dict)
    context_entries: tuple[dict[str, str], ...] = ()
    actor_id: str | None = "U1"
    channel: str = "slack"

cases = (
    Request("MB2-C00419 PM2 상태 확인"),
    Request("12345678910 유저 조회"),
    Request("48194663047 2026-03-06 영상 복구"),
    Request("이 스레드 학습해줘"),
    Request("s3 로그 MMB001 2026-03-06"),
    Request("요청 로그 오늘 최근 5"),
    Request("db 조회 select seq from recordings limit 1"),
    Request(
        "질문 본문은 분류에 쓰지 않아",
        {"operation_action": {"name": "security_review"}},
    ),
    Request(
        "질문 본문은 분류에 쓰지 않아",
        {
            "operation_action": {
                "name": "device_health_alert_contact_hospital",
                "phase": "prepare",
            }
        },
    ),
)
routes = [match_company_operation_route(item) for item in cases]
stages = [company_operation_legacy_stage(item) for item in cases]
forbidden_exact = {
    "anthropic",
    "boto3",
    "botocore",
    "pymysql",
    "paramiko",
    "redis",
    "google.auth",
    "cryptography",
}
forbidden_assistant = {
    "boxer_company.assistant.operations",
    "boxer_company.assistant.device_file_operations_route",
    "boxer_company.assistant.device_health_alert_action_route",
    "boxer_company.assistant.device_led_routes",
    "boxer_company.assistant.device_operations_route",
    "boxer_company.assistant.knowledge_routes",
    "boxer_company.assistant.knowledge_write_route",
    "boxer_company.assistant.private_admin_routes",
    "boxer_company.assistant.security_review_route",
}
loaded_forbidden = sorted(
    name
    for name in sys.modules
    if any(
        name == package or name.startswith(f"{package}.")
        for package in forbidden_exact
    )
    or name in forbidden_assistant
    or name.startswith("boxer_company.routers.")
)
print(json.dumps({"routes": routes, "stages": stages, "forbidden": loaded_forbidden}))
'''
    completed = subprocess.run(
        [sys.executable, "-c", script],
        cwd=project_root,
        check=True,
        capture_output=True,
        text=True,
        env={"BOXER_SKIP_DOTENV": "true"},
    )
    payload = json.loads(completed.stdout)
    assert payload["routes"] == [
        "device_pm2_probe",
        "app_user_lookup",
        "device_file_recovery",
        "thread_playbook_learning",
        "admin_s3_device_log",
        "admin_request_log",
        "admin_readonly_sql",
        "security_review",
        "device_health_alert_sms_prepare",
    ]
    assert payload["stages"] == [
        "device",
        "barcode",
        "device",
        "pre_notion",
        "pre_notion",
        "pre_notion",
        "pre_notion",
        "pre_notion",
        "pre_notion",
    ]
    assert payload["forbidden"] == []
