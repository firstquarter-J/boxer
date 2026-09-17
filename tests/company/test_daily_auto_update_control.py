from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest
from fastapi.testclient import TestClient

from boxer_company._operation_routing_automation import (
    DAILY_AUTO_UPDATE_ROUTE,
    parse_daily_auto_update_command,
)
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.daily_auto_update_route import (
    DailyAutoUpdateAssistantRoute,
)
from boxer_company.assistant.factory import create_company_assistant_runtime
from boxer_company.automation_schedule import AutomationScheduleConfig
from boxer_company.operation_routing import (
    company_operation_legacy_stage,
    match_company_operation_route,
)
from boxer_company.protected_json import create_protected_json_file
from boxer_company_api.app import (
    _RUNTIME_UNSET,
    _resolve_runtime,
    create_company_api_app,
)
from boxer_company_api.automation import (
    AutomationCycleContractError,
    JsonAutomationCycleStateStore,
)
from boxer_company_api.automation_delivery import validate_automation_delivery_state
from boxer_company_api.automation_scheduler import (
    AutomationDeliveryTarget,
    AutomationScheduler,
    AutomationSchedulerSettings,
)
from boxer_company_api.daily_auto_update import (
    DailyAutoUpdateController,
    _control_key,
    resolve_daily_auto_update_options,
)
from boxer_company_api.settings import CompanyApiCallerSettings, CompanyApiSettings

_DEFAULTS = {"agent": True, "box_free": True, "box_paid": False}
_TOKEN = "t" * 48


def _request(question: str, request_id: str = "command-1") -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id=request_id,
        tenant_id="T1",
        actor_id="U1",
        channel="slack",
        conversation_id="C1",
        question=question,
        locale="ko",
        metadata={"route_group": "operations"},
    )


def _controller(path: Path) -> DailyAutoUpdateController:
    return DailyAutoUpdateController(
        JsonAutomationCycleStateStore(path),
        tenant_id="T1",
        defaults=_DEFAULTS,
        daily_enabled=True,
    )


@pytest.fixture
def state_path(tmp_path: Path) -> Path:
    path = tmp_path / "automation.json"
    create_protected_json_file(path, {"version": 1, "cycles": {}}, label="automation")
    return path


@pytest.mark.parametrize(
    "question,target,action",
    [
        ("마미박스 무료병원 자동 업데이트 꺼", "box_free", "disable"),
        ("마미박스 유료병원 자동 업데이트 켜", "box_paid", "enable"),
        ("에이전트 자동업데이트 비활성화해줘", "agent", "disable"),
        ("<@U123> 마미박스 무료병원 자동 업데이트 꺼줘!", "box_free", "disable"),
        ("마미박스 자동 업데이트 꺼", "box_qualifier_required", "disable"),
        ("데일리 자동 업데이트 상태", None, "status"),
        ("마미박스 자동 업데이트 꺼져 있어?", None, "status"),
    ],
)
def test_commands_are_routed_before_notion(
    question: str, target: str | None, action: str
) -> None:
    command = parse_daily_auto_update_command(question)
    assert (command.target, command.action) == (target, action)
    request = _request(question)
    assert match_company_operation_route(request) == DAILY_AUTO_UPDATE_ROUTE
    assert company_operation_legacy_stage(request) == "pre_notion"


@pytest.mark.parametrize(
    "question",
    [
        "마미박스 무료병원 자동 업데이트 끄지 마",
        "마미박스 무료병원 자동 업데이트 켜지 마",
        "마미박스 무료병원 자동 업데이트 안 꺼",
        "문제가 있으면 마미박스 무료병원 자동 업데이트 꺼",
        "마미박스 무료병원 자동 업데이트 꺼 그리고 켜",
        "마미박스 무료병원 자동 업데이트 꺼?",
        "마미박스 무료병원 자동 업데이트 꺼 명령어 왜 동작안함?",
        "마미박스 무료 유료병원 자동 업데이트 켜",
        "에이전트 마미박스 무료병원 자동 업데이트 꺼",
        "MB2-C00419 마미박스 자동 업데이트 꺼",
    ],
)
def test_non_exact_or_negated_commands_never_write(question: str) -> None:
    control = Mock()
    result = DailyAutoUpdateAssistantRoute(control).handle(_request(question))
    assert result.outcome == "needs_input"
    control.read.assert_not_called()
    control.set_enabled.assert_not_called()


def test_missing_qualifier_gives_actionable_reply_without_storage_access() -> None:
    control = Mock()
    result = DailyAutoUpdateAssistantRoute(control).handle(
        _request("마미박스 자동 업데이트 꺼")
    )
    assert result.outcome == "needs_input"
    assert (
        "무료병원" in result.messages[0].body and "유료병원" in result.messages[0].body
    )
    control.set_enabled.assert_not_called()


def test_state_survives_restart_and_preserves_other_cycles(state_path: Path) -> None:
    store = JsonAutomationCycleStateStore(state_path)
    other = {"inFlight": {"requestId": "in-progress"}, "cursor": {"hospitalSeq": 7}}
    store.mutate_cycle("a" * 64, lambda *_: (other, None))
    request = _request("마미박스 무료병원 자동 업데이트 꺼")
    result = DailyAutoUpdateAssistantRoute(_controller(state_path)).handle(request)
    assert result.outcome == "answered"
    assert "다음 병원 순회" in result.messages[0].body
    assert _controller(state_path).read(request).enabled == {
        **_DEFAULTS,
        "box_free": False,
    }
    assert store.load("a" * 64) == other
    assert state_path.stat().st_mode & 0o777 == 0o600
    row = store.load(_control_key("T1"))["overrides"]["box_free"]
    assert row["updatedBy"] == "U1" and row["requestId"] == request.request_id
    # 같은 요청 replay는 timestamp와 저장 revision도 바꾸지 않는다.
    before = state_path.read_bytes()
    _controller(state_path).set_enabled(request, "box_free", False)
    assert state_path.read_bytes() == before
    validate_automation_delivery_state(store)


def test_concurrent_settings_updates_preserve_each_target(state_path: Path) -> None:
    def change(target: str, enabled: bool) -> None:
        _controller(state_path).set_enabled(_request("", target), target, enabled)

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [
            executor.submit(change, target, value)
            for target, value in (
                ("agent", False),
                ("box_free", False),
                ("box_paid", True),
            )
        ]
        for future in futures:
            future.result()
    assert _controller(state_path).read(_request("")).enabled == {
        "agent": False,
        "box_free": False,
        "box_paid": True,
    }


def test_status_question_never_changes_settings(state_path: Path) -> None:
    controller = _controller(state_path)
    controller.set_enabled(_request(""), "box_free", False)
    before = state_path.read_bytes()
    result = DailyAutoUpdateAssistantRoute(controller).handle(
        _request("마미박스 자동 업데이트 꺼져 있어?", "status-1")
    )
    assert result.outcome == "answered"
    assert "마미박스 무료병원: **꺼짐**" in result.messages[0].body
    assert state_path.read_bytes() == before


def test_api_default_factory_wires_shared_settings_store(state_path: Path) -> None:
    # HTTP용 운영 factory도 주입한 path와 scheduler env를 같은 기준으로 쓴다.
    caller = CompanyApiCallerSettings(
        caller_id="slack-test",
        token=_TOKEN,
        tenant_ids=frozenset({"T1"}),
        channels=frozenset({"slack"}),
        actor_ids=frozenset({"U1"}),
        capabilities=frozenset({"assistant.turn.read", "assistant.operation.execute"}),
    )
    settings = CompanyApiSettings(
        host="127.0.0.1",
        port=8010,
        callers=(caller,),
        automation_state_path=str(state_path),
        automation_storage_required=True,
        automation_scheduler_enabled=True,
        automation_enabled_cycles=frozenset({"daily_device_round"}),
    )
    _controller(state_path).set_enabled(_request(""), "box_free", False)
    with (
        patch.dict(
            os.environ,
            {
                "BOXER_COMPANY_API_AUTOMATION_TENANT_ID": "T1",
                "DAILY_DEVICE_ROUND_AUTO_UPDATE_AGENT": "true",
                "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_FREE": "true",
                "DAILY_DEVICE_ROUND_AUTO_UPDATE_BOX_PAID": "false",
            },
        ),
        patch("boxer_company_api.app.validate_company_api_runtime_security"),
        patch("boxer.core.utils._validate_tokens"),
        patch("boxer_company.settings.validate_company_data_source_settings"),
        patch(
            "boxer_company.assistant.factory.create_company_assistant_runtime"
        ) as factory,
    ):
        _, ready = _resolve_runtime(settings, _RUNTIME_UNSET)
    assert ready
    control = factory.call_args.kwargs["daily_auto_update_control"]
    assert control.read(_request("")).enabled == {**_DEFAULTS, "box_free": False}


def test_scheduler_reads_latest_settings_without_restart(state_path: Path) -> None:
    runs = []
    options = {
        "autoUpdateAgent": True,
        "autoUpdateBoxFree": True,
        "autoUpdateBoxPaid": False,
        "autoCleanupTrashCan": True,
        "autoPowerOff": True,
    }
    scheduler = AutomationScheduler(
        AutomationSchedulerSettings(
            tenant_id="T1",
            state_path=str(state_path),
            enabled_cycles=("daily_device_round",),
            schedule=AutomationScheduleConfig(),
            delivery_targets={
                "daily_device_round": AutomationDeliveryTarget("C123456")
            },
            daily_options=options,
        ),
        JsonAutomationCycleStateStore(state_path),
        runs.append,
    )
    now = datetime(2026, 9, 17, 22, 0, tzinfo=ZoneInfo("Asia/Seoul"))
    scheduler.run_once(now=now)
    _controller(state_path).set_enabled(_request(""), "box_free", False)
    scheduler.run_once(now=now)
    assert dict(runs[0].options) == options
    assert dict(runs[1].options) == {**options, "autoUpdateBoxFree": False}
    assert (
        resolve_daily_auto_update_options(
            JsonAutomationCycleStateStore(state_path), "T2", options
        )
        == options
    )


@pytest.mark.parametrize(
    "damage", ["missing", "invalid_json", "invalid_override", "symlink", "permissions"]
)
def test_invalid_state_fails_closed_without_overwrite(
    state_path: Path, damage: str
) -> None:
    if damage == "missing":
        state_path.unlink()
    elif damage == "invalid_json":
        state_path.write_text("{")
    elif damage == "permissions":
        state_path.chmod(0o644)
    elif damage == "symlink":
        target = state_path.with_name("target.json")
        state_path.rename(target)
        state_path.symlink_to(target)
    else:
        store = JsonAutomationCycleStateStore(state_path)
        store.mutate_cycle(
            _control_key("T1"),
            lambda *_: (
                {
                    "kind": "daily_auto_update_control",
                    "tenantId": "T1",
                    "overrides": {"box_free": True},
                },
                None,
            ),
        )
    before = state_path.read_bytes() if state_path.exists() else None
    controller = _controller(state_path)
    for operation in (
        lambda: controller.read(_request("")),
        lambda: controller.set_enabled(_request(""), "box_free", False),
    ):
        with pytest.raises(AutomationCycleContractError):
            operation()
    assert (state_path.read_bytes() if state_path.exists() else None) == before
    with pytest.raises(AutomationCycleContractError):
        resolve_daily_auto_update_options(
            JsonAutomationCycleStateStore(state_path), "T1", {}
        )
    if damage == "invalid_override":
        with pytest.raises(AutomationCycleContractError):
            validate_automation_delivery_state(
                JsonAutomationCycleStateStore(state_path)
            )


def test_write_failure_returns_failure_without_notion_fallback(
    state_path: Path,
) -> None:
    with patch.object(
        JsonAutomationCycleStateStore,
        "_write_document_unlocked",
        side_effect=OSError("private path"),
    ):
        result = DailyAutoUpdateAssistantRoute(_controller(state_path)).handle(
            _request("에이전트 자동 업데이트 꺼"),
        )
    assert result.outcome == "failed"
    assert "private path" not in result.messages[0].body
    assert _controller(state_path).read(_request("")).enabled == _DEFAULTS


@pytest.mark.parametrize(
    "changes", [{"tenant_id": "T2"}, {"channel": "web"}, {"actor_id": None}]
)
def test_control_rejects_non_scheduler_scope(state_path: Path, changes: dict) -> None:
    with pytest.raises(ValueError):
        _controller(state_path).set_enabled(
            replace(_request(""), **changes), "agent", False
        )


def test_http_turn_permissions_dispatch_replay_and_slack_contract(
    state_path: Path,
) -> None:
    # 실제 factory·HTTP 경계를 통과하되 장비/Slack/LLM provider는 호출하지 않는다.
    with patch("boxer_company.assistant.factory.core_settings.LLM_PROVIDER", ""):
        runtime = create_company_assistant_runtime(
            daily_auto_update_control=_controller(state_path)
        )
    caller = CompanyApiCallerSettings(
        caller_id="slack-test",
        token=_TOKEN,
        tenant_ids=frozenset({"T1"}),
        channels=frozenset({"slack"}),
        actor_ids=frozenset({"U1"}),
        capabilities=frozenset({"assistant.turn.read", "assistant.operation.execute"}),
    )
    settings = CompanyApiSettings(
        host="127.0.0.1",
        port=8010,
        callers=(caller,),
        live_device_enabled=False,
        request_log_enabled=True,
        request_log_path=str(state_path.with_name("requests.db")),
    )
    payload = {
        "tenantId": "T1",
        "actorId": "U1",
        "channel": "slack",
        "conversationId": "C1",
        "question": "마미박스 무료병원 자동 업데이트 꺼",
        "locale": "ko",
        "contextEntries": [],
    }
    headers = {"Authorization": f"Bearer {_TOKEN}", "X-Request-ID": "http-control-1"}
    for allowed, status in ((False, 403), (True, 200)):
        current = (
            settings
            if allowed
            else replace(
                settings,
                callers=(
                    replace(caller, capabilities=frozenset({"assistant.turn.read"})),
                ),
            )
        )
        app = create_company_api_app(
            settings=current, assistant_runtime=runtime, readiness_probe=lambda: True
        )
        with TestClient(app) as client:
            response = client.post(
                "/internal/v1/assistant/turns", headers=headers, json=payload
            )
            assert response.status_code == status
            if allowed:
                assert response.json()["route"] == DAILY_AUTO_UPDATE_ROUTE
                assert response.json()["outcome"] == "answered"
                before = state_path.read_bytes()
                replay = client.post(
                    "/internal/v1/assistant/turns", headers=headers, json=payload
                )
                assert replay.status_code == 200
                assert state_path.read_bytes() == before
                from boxer_company_adapter_slack.company_api_client import (
                    _deserialize_result,
                )

                assert (
                    _deserialize_result(response, "http-control-1").route
                    == DAILY_AUTO_UPDATE_ROUTE
                )
            else:
                assert _controller(state_path).read(_request("")).enabled == _DEFAULTS
