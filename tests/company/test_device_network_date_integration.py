"""네트워크 조회 날짜가 수집, 원인 판단과 사용자 답변까지 유지되는지 검증한다."""

import json
from datetime import datetime
from unittest.mock import Mock, patch
from zoneinfo import ZoneInfo

import pytest

from boxer_company.assistant import device_operations_route as operations
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.retrieval_rules import _transform_company_retrieval_payload
from boxer_company.routers import device_diagnostics as diagnostics
from boxer_company.routers import device_network_probe as network
from boxer_company.routers import device_status_probe as status_probe

_KST = ZoneInfo("Asia/Seoul")
_NOW = datetime(2026, 10, 22, 12, 30, tzinfo=_KST)
_QUESTION = (
    "MB2-C00330 네트워크 오류 원인 확인해줘. 마미박스 앱 문제인지 유선 네트워크 연결 문제인지, "
    "시스템 로그와 앱 로그를 함께 보고 근거랑 점검할 부분까지 알려줘."
)
_BOOT = "a" * 32


def _check(payload):
    return {"ok": True, "output": json.dumps(payload)}


def _checks(*, carrier=True, link_events=(), app_events=()):
    return {
        "network_state": _check({
            "status": "ok", "interface": "enp1s0", "connectionType": "wired",
            "carrier": carrier, "operstate": "up" if carrier else "down",
            "hasDefaultRoute": carrier, "capturedAt": _NOW.isoformat(timespec="seconds"),
        }),
        "network_link_history": _check({"status": "ok", "events": list(link_events)}),
        "network_app_history": _check({"status": "ok", "events": list(app_events)}),
    }


def _link(stamp):
    return {
        "timestamp": stamp, "source": "kernel", "interface": "enp1s0", "state": "down",
        "boot": _BOOT, "bootElapsedSeconds": 1000, "transition": "link_down",
    }


def test_past_day_diagnosis_does_not_promote_current_link_failure_to_past_cause():
    # 오늘의 실제 단절은 어제 조회에서 어제 장애의 증거가 되면 안 된다.
    window = network.resolve_network_window("2026-10-21 " + _QUESTION, now=_NOW)
    summary = network.build_network_summary(_checks(carrier=False), window=window)

    assert summary["classification"] not in {"network_link_failure", "network_route_failure"}
    assert summary["status"] != "pass"
    lines = "\n".join(diagnostics._diagnostic_network_lines({"network": summary}))
    assert "2026-10-21" in lines
    assert "72시간" not in lines


def test_selected_day_filters_link_and_app_evidence_together_in_answer():
    # 양쪽 로그에 다른 날짜가 섞여도 선택한 하루의 단절과 앱 오류만 원인 근거로 쓴다.
    window = network.resolve_network_window("2026-10-21 " + _QUESTION, now=_NOW)
    checks = _checks(
        link_events=(
            _link("2026-10-20T23:59:59+09:00"),
            _link("2026-10-21T09:15:42+09:00"),
            _link("2026-10-22T00:00:00+09:00"),
        ),
        app_events=(
            {"timestamp": "2026-10-20T23:59:59+09:00", "kind": "dns_retry"},
            {"timestamp": "2026-10-21T09:16:11+09:00", "kind": "socket_error"},
            {"timestamp": "2026-10-22T00:00:00+09:00", "kind": "route_unreachable"},
        ),
    )
    summary = network.build_network_summary(checks, window=window)

    assert summary["classification"] == "network_link_failure"
    assert summary["correlation"]["linkTimestamp"] == "2026-10-21T09:15:42+09:00"
    assert summary["correlation"]["appTimestamp"] == "2026-10-21T09:16:11+09:00"
    lines = "\n".join(diagnostics._diagnostic_network_lines({"network": summary}))
    assert "2026-10-21" in lines
    assert "09:15:42" in lines
    assert "09:16:11" in lines
    assert {item["timestamp"] for item in summary["evidence"]} == {
        "2026-10-21T09:15:42+09:00", "2026-10-21T09:16:11+09:00",
    }
    assert "72시간" not in lines


def test_default_day_scope_is_visible_without_old_snapshot_history_label():
    window = network.resolve_network_window(_QUESTION, now=_NOW)
    summary = network.build_network_summary(_checks(), window=window)
    lines = "\n".join(diagnostics._diagnostic_network_lines({"network": summary}))

    assert "2026-10-22" in lines
    assert "00:00" in lines
    assert "12:30" in lines
    assert "72시간" not in lines


@pytest.mark.parametrize(("prefix", "body", "network_only"), (
    ("", _QUESTION, True),
    ("어제 ", _QUESTION, True),
    ("2026-10-20 ", _QUESTION, True),
    ("", "MB2-C00330 진단 시작", False),
    ("", "MB2-C00330 최근 72시간 전원 종료 원인 확인해줘", False),
))
def test_snapshot_uses_same_window_and_preserves_non_network_log_collection(prefix, body, network_only):
    question = prefix + body
    window_question = question if network_only else ""
    window = network.resolve_network_window(window_question, now=_NOW)
    expected_specs = network.build_network_command_specs(window)
    expected_commands = {spec["command"]: key for key, spec in expected_specs.items()}
    checks = _checks()
    client = Mock()

    def run_remote(_client, *, command, summary, timeout_sec):
        # SSH 자체를 실행하지 않고 동적 네트워크 명령과 현재 상태 명령만 수집한다.
        key = expected_commands.get(command)
        return {
            **checks.get(key, {"ok": True, "output": ""}),
            "command": command, "summary": summary, "exitStatus": 0,
        }

    with (
        patch.object(diagnostics, "resolve_network_window", return_value=window) as resolve,
        patch.object(diagnostics, "_wait_for_mda_device_agent_ssh", return_value={
            "ready": True, "device": {
                "deviceName": "MB2-C00330", "agentSsh": {"host": "example.invalid", "port": 2222},
            },
        }),
        patch.object(diagnostics, "_connect_device_ssh_client", return_value={"ok": True, "client": client}),
        patch.object(diagnostics, "_run_remote_ssh_command", side_effect=run_remote) as run,
    ):
        snapshot = diagnostics._collect_device_diagnostic_snapshot(
            device_name="MB2-C00330", question=question, workspace_id="test-tenant",
            channel_id="test-channel", thread_ts="test-thread", requested_by="test-actor",
        )

    assert snapshot["networkWindow"] == window
    resolve.assert_called_once_with(window_question)
    assert snapshot["summary"]["network"]["networkWindow"] == window
    for key, spec in expected_specs.items():
        assert snapshot["checks"][key]["command"] == spec["command"]
    # 선택일 밖 원문이 PM2 tail이나 최근 journal을 통해 답변 근거로 우회하지 못한다.
    unbounded_logs = {
        "pm2_logs_box", "pm2_logs_agent", "app_recent_logs", "system_journal_recent", "kernel_oom",
    }
    if network_only:
        assert not unbounded_logs & snapshot["checks"].keys()
    else:
        # 네트워크 날짜 추가가 기존 일반 진단의 앱, OOM과 재부팅 근거를 제거하면 안 된다.
        assert unbounded_logs <= snapshot["checks"].keys()
    assert run.call_count == len(snapshot["checks"])
    client.close.assert_called_once()
    reply = diagnostics._build_device_diagnostic_start_reply(snapshot)
    assert window["label"] in reply
    assert "72시간" not in reply


@pytest.mark.parametrize("date", ("2026-02-30", "2026-13-01", "2026-10-21~2026-10-22"))
def test_invalid_date_never_opens_ssh_or_falls_back_to_today(date):
    # 명시 날짜가 잘못됐으면 네트워크/SSH 상태 조회보다 먼저 종료해야 한다.
    with (
        patch.object(diagnostics, "_wait_for_mda_device_agent_ssh") as wait,
        patch.object(diagnostics, "_connect_device_ssh_client") as connect,
        pytest.raises(ValueError),
    ):
        diagnostics._collect_device_diagnostic_snapshot(
            device_name="MB2-C00330", question=date + " " + _QUESTION, workspace_id="test-tenant",
            channel_id="test-channel", thread_ts="test-thread", requested_by="test-actor",
        )
    wait.assert_not_called()
    connect.assert_not_called()


def test_followup_replaces_old_day_evidence_before_llm_transform():
    question = "어제 " + _QUESTION
    window = network.resolve_network_window(question, now=_NOW)
    old_window = network.resolve_network_window("2026-10-20 " + _QUESTION, now=_NOW)
    previous = {
        "route": "device_diagnostic_snapshot",
        "request": {"deviceName": "MB2-C00330", "capturedAt": "2026-10-20T12:30:00+09:00"},
        "networkWindow": old_window,
        "ssh": {"ready": True},
        "checks": {"app_recent_logs": {"output": "old-day-log-sentinel"}},
        "summary": {"interestingLogLines": [{"line": "old-day-log-sentinel"}]},
    }
    client = Mock()
    with (
        patch.object(diagnostics, "resolve_network_window", return_value=window),
        patch.object(diagnostics, "_wait_for_mda_device_agent_ssh", return_value={
            "ready": True, "device": {
                "deviceName": "MB2-C00330", "agentSsh": {"host": "example.invalid", "port": 2222},
            },
        }),
        patch.object(diagnostics, "_connect_device_ssh_client", return_value={"ok": True, "client": client}),
        patch.object(diagnostics, "_run_device_diagnostic_commands", return_value=_checks()) as run,
    ):
        evidence = diagnostics._build_device_diagnostic_followup_evidence(question, previous)

    assert run.call_args.kwargs["network_window"] == window
    assert run.call_args.kwargs["network_only_logs"] is True
    assert evidence["networkWindow"] == window
    assert evidence["followupLiveCheck"]["networkWindow"] == window
    assert evidence["followupLiveCheck"]["summary"]["network"]["networkWindow"] == window
    transformed = _transform_company_retrieval_payload(evidence)
    assert "old-day-log-sentinel" not in json.dumps(transformed)
    assert previous["checks"]["app_recent_logs"]["output"] == "old-day-log-sentinel"
    assert window["label"] in diagnostics._build_device_diagnostic_followup_fallback(question, evidence)


def test_generic_followup_keeps_app_evidence_when_ssh_fails_and_drops_old_network_day():
    # 일반 종료 조사는 새 SSH 조회가 실패해도 기존 앱/OOM/PM2 근거를 유지해야 한다.
    question = "최근 72시간 종료 원인 확인해줘"
    window = network.resolve_network_window("", now=_NOW)
    old_window = network.resolve_network_window("2026-10-20", now=_NOW)
    app_checks = {
        "app_recent_logs": {"ok": True, "output": "retained-app-crash-evidence"},
        "kernel_oom": {"ok": True, "output": "retained-oom-evidence"},
        "pm2_jlist": {"ok": True, "output": "retained-pm2-evidence"},
    }
    old_network_checks = {
        key: {"ok": True, "output": "old-network-day-evidence"}
        for key in network.NETWORK_COMMAND_KEYS
    }
    previous = {
        "route": "device_diagnostic_snapshot",
        "request": {"deviceName": "MB2-C00330", "capturedAt": "2026-10-20T12:30:00+09:00"},
        "networkWindow": old_window,
        "checks": {**app_checks, **old_network_checks},
        "summary": {
            "interestingLogLines": [{"line": "retained-app-crash-evidence"}],
            "pm2": {"box": {"status": "online"}},
            "network": {"summary": "old-network-day-evidence", "networkWindow": old_window},
        },
    }
    with (
        patch.object(diagnostics, "resolve_network_window", return_value=window) as resolve,
        patch.object(diagnostics, "_wait_for_mda_device_agent_ssh", return_value={
            "ready": False, "reason": "ssh_unavailable", "device": {},
        }),
        patch.object(diagnostics, "_connect_device_ssh_client") as connect,
        patch.object(diagnostics, "_run_device_diagnostic_commands") as run,
    ):
        evidence = diagnostics._build_device_diagnostic_followup_evidence(question, previous)

    resolve.assert_called_once_with("")
    connect.assert_not_called()
    run.assert_not_called()
    assert evidence["networkWindow"] == window
    assert evidence["followupLiveCheck"]["reason"] == "agent_ssh_not_ready"
    assert evidence["checks"] == app_checks
    assert evidence["summary"] == {
        "interestingLogLines": [{"line": "retained-app-crash-evidence"}],
        "pm2": {"box": {"status": "online"}},
    }
    transformed = json.dumps(_transform_company_retrieval_payload(evidence))
    assert "retained-app-crash-evidence" in transformed
    assert "old-network-day-evidence" not in transformed
    assert previous["networkWindow"] == old_window
    assert set(network.NETWORK_COMMAND_KEYS) <= previous["checks"].keys()
    assert previous["summary"]["network"]["summary"] == "old-network-day-evidence"


def test_status_query_carries_explicit_day_to_collection_and_answer():
    question = "2026-10-21 MB2-C00330 장비 상태 확인해줘"
    window = network.resolve_network_window(question, now=_NOW)
    with (
        patch.object(status_probe, "resolve_network_window", return_value=window),
        patch.object(status_probe, "_send_mda_device_ping", return_value={"status": True}),
        patch.object(status_probe, "_collect_runtime_checks", return_value=(
            {"ssh": {"ready": True}}, {}, _checks(),
        )) as collect,
    ):
        reply, evidence = status_probe._probe_device_status_overview("MB2-C00330", question=question)

    assert collect.call_args.kwargs["network_window"] == window
    assert evidence["networkWindow"] == window
    assert evidence["overview"]["network"]["networkWindow"] == window
    assert window["label"] in reply
    assert "72시간" not in reply


def test_status_invalid_day_does_not_ping_or_open_connection():
    with (
        patch.object(status_probe, "_send_mda_device_ping") as ping,
        patch.object(status_probe, "_collect_runtime_checks") as collect,
        pytest.raises(ValueError),
    ):
        status_probe._probe_device_status_overview(
            "MB2-C00330", question="2026-02-30 MB2-C00330 장비 상태 확인해줘",
        )
    ping.assert_not_called()
    collect.assert_not_called()


def _status_request(question):
    return CompanyAssistantRequest(
        request_id="local-network-date-contract", tenant_id="test-tenant",
        actor_id="test-actor", channel="slack", conversation_id="test-thread",
        question=question, locale="ko", metadata={"route_group": "operations"},
    )


@pytest.mark.parametrize("prefix", ("", "어제 ", "2026-10-21 "))
def test_status_operation_forwards_explicit_date_and_preserves_default_call_contract(prefix):
    question = prefix + "MB2-C00330 장비 상태 확인해줘"
    window = network.resolve_network_window(question, now=_NOW)
    probe = Mock(return_value=("장비 상태 확인 결과", {}))
    route = operations.DeviceOperationsAssistantRoute(deps=operations.DeviceOperationsRouteDeps(
        probe_status=probe, device_runtime_configured=lambda: True,
    ))

    with patch.object(operations, "resolve_network_window", return_value=window) as resolve:
        result = route.handle(_status_request(question))

    assert result is not None
    assert result.route == "device_status_probe"
    assert result.outcome == "answered"
    resolve.assert_called_once_with(question)
    expected = {"question": question} if prefix else {}
    probe.assert_called_once_with(
        "MB2-C00330", resend_ssh_open=False, allow_force_reopen=False, **expected,
    )


@pytest.mark.parametrize("date", ("2026-02-30", "2026-13-01", "2026-10-21~2026-10-22"))
def test_status_operation_returns_date_guidance_before_status_provider_is_called(date):
    probe = Mock(side_effect=AssertionError("잘못된 날짜로 장비를 조회하면 안 돼"))
    route = operations.DeviceOperationsAssistantRoute(deps=operations.DeviceOperationsRouteDeps(
        probe_status=probe, device_runtime_configured=lambda: True,
    ))

    result = route.handle(_status_request(date + " MB2-C00330 장비 상태 확인해줘"))

    assert result is not None
    assert result.route == "device_status_probe"
    assert result.outcome == "needs_input"
    assert result.fallback_reason == "invalid_date"
    assert result.messages[0].body
    probe.assert_not_called()
