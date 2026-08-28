from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import subprocess
import threading
from unittest.mock import Mock, patch

import pytest

from boxer_company import settings as cs
from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.assistant.device_operations_route import (
    DeviceOperationsAssistantRoute,
    DeviceOperationsRouteDeps,
)
from boxer_company.assistant.operations import (
    as_operations_request,
    company_operation_legacy_stage,
    company_operation_route_names,
    is_mutation_capable_company_operation,
    match_company_operation_route,
    match_live_device_company_operation_route,
)
from boxer_company.routers.device_scanner_abi_patch import (
    DEVICE_SCANNER_ABI_PATCH_ROUTE,
    DeviceScannerAbiPatchError,
    _RemoteOutputTail,
    _apply_device_scanner_abi_patch,
    _assess_device_idle,
    _execute_monitored_patch,
    _extract_device_name_for_scanner_abi_patch,
    _is_device_scanner_abi_patch_intent,
    _is_device_scanner_abi_patch_request,
    _load_patch_asset,
)
from boxer_company.routers.mda_graphql import (
    _get_mda_device_detail,
    _normalize_mda_device_detail,
)
from boxer_company_api.app import _safe_operation_request_log_metadata
from boxer_company_api.schemas import serialize_result


def _request(question: str) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id="REQ-SCANNER-PATCH-1",
        tenant_id="TENANT-1",
        actor_id="ACTOR-1",
        channel="slack",
        conversation_id="THREAD-1",
        question=question,
        locale="ko",
        metadata={"channel_id": "CHANNEL-1"},
    )


def _fresh_device_info(
    *,
    device_name: str = "MB2-A00037",
    updated_at: datetime | None = None,
    status: str = "NOSESS",
    is_recording: object = False,
    is_uploading: object = False,
    agent_connected: object = True,
    device_connected: object = True,
) -> dict[str, object]:
    timestamp = updated_at or datetime.now(timezone.utc)
    return {
        "deviceName": device_name,
        "isConnected": agent_connected,
        "deviceIsConnected": device_connected,
        "deviceStatus": status,
        "isRecording": is_recording,
        "isUploading": is_uploading,
        "deviceUpdatedAt": timestamp.isoformat(),
    }


def _patch_runtime_enabled():
    return patch.multiple(
        cs,
        DEVICE_SCANNER_ABI_PATCH_ENABLED=True,
        MDA_GRAPHQL_URL="https://mda.example/graphql",
        MDA_ADMIN_USER_PASSWORD="configured",
        DEVICE_SSH_PASSWORD="configured",
        DEVICE_SCANNER_ABI_PATCH_STATE_MAX_AGE_SEC=120,
        DEVICE_SCANNER_ABI_PATCH_TIMEOUT_SEC=780,
    )


@pytest.mark.parametrize(
    "question",
    (
        "MB2-A00037 스캐너 패치",
        "<@U123> MB2-A00037 스캐너 패치",
        "mb2-a00037 스캐너 패치.",
    ),
)
def test_exact_scanner_patch_command_is_the_only_apply_form(
    question: str,
) -> None:
    assert _is_device_scanner_abi_patch_intent(question)
    assert _is_device_scanner_abi_patch_request(question)
    assert (
        _extract_device_name_for_scanner_abi_patch(question)
        == "MB2-A00037"
    )


@pytest.mark.parametrize(
    "question",
    (
        "MB2-A00037 스캐너 패치 가능해?",
        "MB2-A00037 스캐너 ABI 패치 적용",
        "MB2-A00037 스캐너 ABI 패치 확인",
        "MB2-A00037 스캐너 ABI 패치 적용하지마",
        "MB2-A00037 MB2-A00051 스캐너 패치",
        "전체 장비 스캐너 패치",
        "MB2-A00037 장비 스캐너 패치",
        "MB2-A00037 node-hid ABI 적용하고 장비 종료",
        "MB2-A00037 스캐너 ABI 적용하고 장비 종료",
        "MB2-A00037 스캐너 호환성 조치하고 박스 업데이트",
        "이 스레드 학습하고 MB2-A00037 스캐너 패치",
        "MB2-A00037 음성을 지니로 바꾸고 스캐너 패치",
        "MB2-A00037 스캐너 패치하고 장비 종료",
        "`MB2-A00037 스캐너 패치`",
        '"MB2-A00037 스캐너 패치"',
    ),
)
def test_scanner_patch_intent_is_isolated_but_not_executed(
    question: str,
) -> None:
    assert _is_device_scanner_abi_patch_intent(question)
    assert not _is_device_scanner_abi_patch_request(question)
    assert (
        match_company_operation_route(_request(question))
        == DEVICE_SCANNER_ABI_PATCH_ROUTE
    )


def test_scanner_patch_route_uses_operations_mutation_and_live_device_gates(
) -> None:
    request = _request("MB2-A00037 스캐너 패치")

    assert match_company_operation_route(request) == DEVICE_SCANNER_ABI_PATCH_ROUTE
    assert company_operation_legacy_stage(request) == "device"
    assert is_mutation_capable_company_operation(request)
    assert (
        match_live_device_company_operation_route(request)
        == DEVICE_SCANNER_ABI_PATCH_ROUTE
    )
    assert DEVICE_SCANNER_ABI_PATCH_ROUTE in company_operation_route_names()


def test_assistant_calls_scanner_patch_dependency_once_without_local_retry(
) -> None:
    operation = Mock(
        return_value=(
            "*적용 완료*",
            {
                "route": DEVICE_SCANNER_ABI_PATCH_ROUTE,
                "device": "MB2-A00037",
                "status": "repair_success",
                "scriptSha256": "a" * 64,
            },
        )
    )
    route = DeviceOperationsAssistantRoute(
        replace(
            DeviceOperationsRouteDeps(),
            apply_scanner_abi_patch=operation,
        )
    )

    with patch(
        "boxer_company.assistant.device_operations_route."
        "_is_device_scanner_abi_patch_runtime_configured",
        return_value=True,
    ):
        result = route.handle(
            as_operations_request(
                _request("MB2-A00037 스캐너 패치")
            )
        )

    assert result is not None
    assert result.route == DEVICE_SCANNER_ABI_PATCH_ROUTE
    assert result.outcome == "answered"
    assert result.messages[0].mention_actor is False
    assert result.operation_result == {
        "kind": "device_scanner_abi_patch",
        "deviceName": "MB2-A00037",
        "status": "repair_success",
        "scriptSha256": "a" * 64,
    }
    operation.assert_called_once_with(
        "MB2-A00037 스캐너 패치",
        device_name="MB2-A00037",
        resend_ssh_open=False,
    )


def test_scanner_patch_receipt_is_audit_only_and_strictly_reduced() -> None:
    receipt = {
        "kind": "device_scanner_abi_patch",
        "deviceName": "MB2-A00037",
        "status": "repair_success",
        "scriptSha256": "a" * 64,
    }
    result = DeviceOperationsAssistantRoute(
        replace(
            DeviceOperationsRouteDeps(),
            apply_scanner_abi_patch=Mock(
                return_value=(
                    "*적용 완료*",
                    {
                        "route": DEVICE_SCANNER_ABI_PATCH_ROUTE,
                        "device": "MB2-A00037",
                        "status": "repair_success",
                        "scriptSha256": "a" * 64,
                    },
                )
            ),
        )
    )
    with patch(
        "boxer_company.assistant.device_operations_route."
        "_is_device_scanner_abi_patch_runtime_configured",
        return_value=True,
    ):
        handled = result.handle(
            as_operations_request(
                _request("MB2-A00037 스캐너 패치")
            )
        )

    assert handled is not None
    assert handled.operation_result == receipt
    assert _safe_operation_request_log_metadata(receipt) == {
        "deviceName": "MB2-A00037",
        "operationStatus": "repair_success",
        "scriptSha256": "a" * 64,
    }
    # 새 receipt는 Slack HTTP 계약으로 내보내지 않고 API 감사에서만 쓴다.
    assert serialize_result(
        handled,
        "REQ-SCANNER-PATCH-1",
    ).get("operationResult") is None
    assert _safe_operation_request_log_metadata(
        {**receipt, "unexpected": "raw-output"}
    ) == {}


@pytest.mark.parametrize(
    "question",
    (
        "MB2-A00037 스캐너 ABI 패치 적용",
        "MB2-A00037 MB2-A00051 스캐너 패치",
        "MB2-A00037 node-hid ABI 적용하고 장비 종료",
        "MB2-A00037 스캐너 ABI 적용하고 장비 종료",
        "`MB2-A00037 스캐너 패치`",
        '"MB2-A00037 스캐너 패치"',
    ),
)
def test_assistant_rejects_noncanonical_command_before_dependency(
    question: str,
) -> None:
    operation = Mock()
    power_off = Mock()
    box_update = Mock()
    agent_update = Mock()
    voice_change = Mock()
    route = DeviceOperationsAssistantRoute(
        replace(
            DeviceOperationsRouteDeps(),
            apply_scanner_abi_patch=operation,
            request_power_off=power_off,
            request_box_update=box_update,
            request_agent_update=agent_update,
            change_voice=voice_change,
        )
    )

    result = route.handle(
        as_operations_request(
            _request(question)
        )
    )

    assert result is not None
    assert result.outcome == "failed"
    assert result.fallback_reason == "device_scanner_abi_patch_command_required"
    assert "정확한 단일 장비" in result.messages[0].body
    operation.assert_not_called()
    power_off.assert_not_called()
    box_update.assert_not_called()
    agent_update.assert_not_called()
    voice_change.assert_not_called()


def test_assistant_feature_flag_off_never_calls_patch_dependency() -> None:
    operation = Mock()
    route = DeviceOperationsAssistantRoute(
        replace(
            DeviceOperationsRouteDeps(),
            apply_scanner_abi_patch=operation,
        )
    )

    with patch.object(cs, "DEVICE_SCANNER_ABI_PATCH_ENABLED", False):
        result = route.handle(
            as_operations_request(
                _request("MB2-A00037 스캐너 패치")
            )
        )

    assert result is not None
    assert result.outcome == "failed"
    assert result.fallback_reason == "device_runtime_not_configured"
    assert "비활성화" in result.messages[0].body
    operation.assert_not_called()


@pytest.mark.parametrize(
    ("changes", "reason"),
    (
        ({"device_name": "MB2-A000370"}, "device_identity_mismatch"),
        ({"agent_connected": False}, "agent_disconnected"),
        ({"device_connected": False}, "device_disconnected"),
        ({"status": "RECORDING"}, "device_not_standby"),
        ({"is_recording": True}, "recording_state_unknown_or_active"),
        ({"is_recording": "false"}, "recording_state_unknown_or_active"),
        ({"is_uploading": True}, "upload_state_unknown_or_active"),
        ({"is_uploading": "false"}, "upload_state_unknown_or_active"),
    ),
)
def test_idle_assessment_fails_closed_for_each_unsafe_field(
    changes: dict[str, object],
    reason: str,
) -> None:
    now = datetime.now(timezone.utc)
    with patch.object(
        cs,
        "DEVICE_SCANNER_ABI_PATCH_STATE_MAX_AGE_SEC",
        120,
    ):
        assessment = _assess_device_idle(
            _fresh_device_info(updated_at=now, **changes),
            expected_device="MB2-A00037",
            now=now,
        )

    assert not assessment.ok
    assert assessment.reason == reason


def test_idle_assessment_requires_fresh_timestamp() -> None:
    now = datetime.now(timezone.utc)
    with patch.object(
        cs,
        "DEVICE_SCANNER_ABI_PATCH_STATE_MAX_AGE_SEC",
        120,
    ):
        assessment = _assess_device_idle(
            _fresh_device_info(updated_at=now - timedelta(seconds=121)),
            expected_device="MB2-A00037",
            now=now,
        )

    assert not assessment.ok
    assert assessment.reason == "device_state_stale"


def test_mda_normalizer_keeps_only_real_activity_booleans() -> None:
    base = {
        "deviceName": "MB2-A00037",
        "deviceState": {
            "isConnected": True,
            "status": "NOSESS",
            "isRecording": False,
            "isUploading": True,
        },
        "hospital": {},
        "hospitalRoom": {},
        "agentState": {},
    }
    normalized = _normalize_mda_device_detail(
        base,
        device_name="MB2-A00037",
    )
    assert normalized["isRecording"] is False
    assert normalized["isUploading"] is True

    base["deviceState"]["isRecording"] = "false"
    base["deviceState"]["isUploading"] = 0
    untrusted = _normalize_mda_device_detail(
        base,
        device_name="MB2-A00037",
    )
    assert untrusted["isRecording"] is None
    assert untrusted["isUploading"] is None


def test_mda_device_query_requests_recording_and_uploading_fields() -> None:
    with patch(
        "boxer_company.routers.mda_graphql._execute_mda_graphql",
        return_value={"paginatedDevices": {"nodes": []}},
    ) as execute:
        assert _get_mda_device_detail("MB2-A00037") is None

    query = execute.call_args.args[0]
    assert "isRecording" in query
    assert "isUploading" in query


def test_packaged_patch_asset_has_valid_shell_and_final_idle_gate() -> None:
    payload = _load_patch_asset()
    checked = subprocess.run(
        ["bash", "-n"],
        input=payload,
        capture_output=True,
        check=False,
    )

    assert checked.returncode == 0, checked.stderr.decode()
    assert b"AWAITING_IDLE_CONFIRMATION" in payload
    assert b"IDLE_CONFIRMATION_ACCEPTED" in payload
    assert b"PM2_STOPPING" in payload
    assert b"--idle-confirm-file" in payload
    assert b"--idle-confirm-token" in payload


class _ClosableClient:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


@pytest.mark.parametrize(
    ("marker", "expected_status"),
    (
        ("REPAIR_SUCCESS: pid=123 scanner_fds=1 abi=GLIBCXX_3.4.21", "repair_success"),
        ("NO_ACTION_REQUIRED: 현재 node-hid가 정상 로드됩니다.", "no_action_required"),
    ),
)
def test_domain_operation_classifies_only_fixed_success_markers(
    marker: str,
    expected_status: str,
) -> None:
    client = _ClosableClient()
    execution = {
        "ok": True,
        "exitStatus": 0,
        "output": f"[node-hid-abi-repair] {marker}",
        "reason": "",
    }
    with (
        _patch_runtime_enabled(),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_get_mda_device_detail",
            side_effect=[_fresh_device_info(), _fresh_device_info()],
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_open_device_ssh_for_update",
            return_value=({}, {"ready": True}, client),
        ) as open_ssh,
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_run_remote_ssh_command",
            return_value={"ok": True},
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_load_patch_asset",
            return_value=b"script",
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_execute_monitored_patch",
            return_value=execution,
        ) as execute,
    ):
        text, payload = _apply_device_scanner_abi_patch(
            "MB2-A00037 스캐너 패치",
            device_name="MB2-A00037",
            resend_ssh_open=False,
        )

    assert payload["status"] == expected_status
    assert "MB2-A00037" in text
    assert client.closed
    open_ssh.assert_called_once_with(
        "MB2-A00037",
        resend_ssh_open=False,
    )
    execute.assert_called_once_with(
        client,
        device_name="MB2-A00037",
        script_payload=b"script",
    )


def test_domain_operation_never_opens_ssh_when_initial_state_is_active() -> None:
    with (
        _patch_runtime_enabled(),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_get_mda_device_detail",
            return_value=_fresh_device_info(status="RECORDING"),
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_open_device_ssh_for_update",
        ) as open_ssh,
        pytest.raises(DeviceScannerAbiPatchError) as exc_info,
    ):
        _apply_device_scanner_abi_patch(
            "MB2-A00037 스캐너 패치",
            device_name="MB2-A00037",
            resend_ssh_open=False,
        )

    assert exc_info.value.fallback_reason == "device_scanner_abi_patch_not_idle"
    open_ssh.assert_not_called()


def test_domain_operation_fails_fast_when_same_device_is_busy() -> None:
    busy_lock = Mock()
    busy_lock.acquire.return_value = False
    with (
        _patch_runtime_enabled(),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_device_patch_lock",
            return_value=busy_lock,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_get_mda_device_detail",
        ) as get_state,
        pytest.raises(DeviceScannerAbiPatchError) as exc_info,
    ):
        _apply_device_scanner_abi_patch(
            "MB2-A00037 스캐너 패치",
            device_name="MB2-A00037",
            resend_ssh_open=False,
        )

    assert exc_info.value.fallback_reason == "device_scanner_abi_patch_busy"
    busy_lock.acquire.assert_called_once_with(blocking=False)
    busy_lock.release.assert_not_called()
    get_state.assert_not_called()


class _FakeRemoteFile:
    def __init__(self) -> None:
        self.payload = bytearray()

    def write(self, value: bytes) -> None:
        self.payload.extend(value)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FakeSftp:
    def __init__(
        self,
        channel: "_FakeChannel",
        opened_modes: list[str],
        removed_paths: list[str],
    ) -> None:
        self._channel = channel
        self._opened_modes = opened_modes
        self._removed_paths = removed_paths

    def open(self, path: str, mode: str) -> _FakeRemoteFile:
        del path
        self._opened_modes.append(mode)
        return _FakeRemoteFile()

    def chmod(self, path: str, mode: int) -> None:
        del path, mode

    def rename(self, source: str, target: str) -> None:
        del source, target
        self._channel.confirm_gate()

    def remove(self, path: str) -> None:
        self._removed_paths.append(path)

    def close(self) -> None:
        return None


class _FakeChannel:
    def __init__(
        self,
        *,
        complete_on_gate: bool = True,
        wait_for_final_recv: bool = False,
    ) -> None:
        self._stdout: list[bytes] = []
        self._stderr: list[bytes] = []
        self._exit_status: int | None = None
        self.received_script = b""
        self.write_shutdown = False
        self.closed = False
        self.gate_confirmed = False
        self.complete_on_gate = complete_on_gate
        self.wait_for_final_recv = wait_for_final_recv
        self.final_chunk_received = threading.Event()
        self._lock = threading.Lock()

    def sendall(self, payload: bytes) -> None:
        with self._lock:
            self.received_script += payload
            self._stdout.append(b"AWAITING_IDLE_CONFIRMATION\n")

    def shutdown_write(self) -> None:
        self.write_shutdown = True

    def confirm_gate(self) -> None:
        with self._lock:
            self.gate_confirmed = True
            if self.complete_on_gate:
                self._stdout.append(
                    b"IDLE_CONFIRMATION_ACCEPTED\nPM2_STOPPING\n"
                    b"REPAIR_SUCCESS: pid=42\n"
                )
                self._exit_status = 0
            else:
                self._stdout.append(b"IDLE_CONFIRMATION_ACCEPTED\n")
        if self.complete_on_gate and self.wait_for_final_recv:
            assert self.final_chunk_received.wait(timeout=2)

    def recv_ready(self) -> bool:
        with self._lock:
            return bool(self._stdout)

    def recv(self, size: int) -> bytes:
        del size
        with self._lock:
            payload = self._stdout.pop(0)
        if b"REPAIR_SUCCESS" in payload:
            self.final_chunk_received.set()
        return payload

    def recv_stderr_ready(self) -> bool:
        with self._lock:
            return bool(self._stderr)

    def recv_stderr(self, size: int) -> bytes:
        del size
        with self._lock:
            return self._stderr.pop(0)

    def exit_status_ready(self) -> bool:
        with self._lock:
            return self._exit_status is not None

    def recv_exit_status(self) -> int:
        with self._lock:
            assert self._exit_status is not None
            return self._exit_status

    def close(self) -> None:
        self.closed = True


class _FakeStream:
    def __init__(self, channel: _FakeChannel) -> None:
        self.channel = channel
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeMonitoredClient:
    def __init__(
        self,
        *,
        complete_on_gate: bool = True,
        wait_for_final_recv: bool = False,
    ) -> None:
        self.channel = _FakeChannel(
            complete_on_gate=complete_on_gate,
            wait_for_final_recv=wait_for_final_recv,
        )
        self.streams: list[_FakeStream] = []
        self.opened_modes: list[str] = []
        self.removed_paths: list[str] = []

    def exec_command(self, command: str, timeout: int):
        assert "setsid -f -w bash -c" in command
        assert "<&0" in command
        assert timeout >= 120
        streams = tuple(_FakeStream(self.channel) for _ in range(3))
        self.streams.extend(streams)
        return streams

    def open_sftp(self) -> _FakeSftp:
        return _FakeSftp(
            self.channel,
            self.opened_modes,
            self.removed_paths,
        )


def test_monitored_executor_refreshes_mda_once_before_final_gate() -> None:
    client = _FakeMonitoredClient()
    baseline = datetime.now(timezone.utc) - timedelta(seconds=1)
    refreshed = datetime.now(timezone.utc)
    ping = Mock(return_value={"status": True})

    def load_state(device_name: str) -> dict[str, object]:
        assert device_name == "MB2-A00037"
        return _fresh_device_info(
            updated_at=refreshed if ping.called else baseline
        )

    states = Mock(side_effect=load_state)

    with (
        _patch_runtime_enabled(),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_STATE_POLL_INTERVAL_SEC",
            0.0,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_GATE_STATE_POLL_INTERVAL_SEC",
            0.0,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_mark_company_api_mutation_attempted",
        ),
    ):
        result = _execute_monitored_patch(
            client,
            device_name="MB2-A00037",
            script_payload=b"#!/usr/bin/env bash\n",
            state_loader=states,
            ping_sender=ping,
        )

    assert result["ok"] is True
    assert result["gateSeen"] is True
    assert result["gateWritten"] is True
    assert result["gateAccepted"] is True
    assert result["pm2Stopping"] is True
    assert "REPAIR_SUCCESS" in result["output"]
    assert client.channel.received_script.startswith(b"#!/usr/bin/env bash")
    assert client.channel.write_shutdown
    assert client.opened_modes == ["wx"]
    ping.assert_called_once_with("MB2-A00037")


def test_monitored_executor_keeps_polling_after_gate_until_pm2_stop() -> None:
    client = _FakeMonitoredClient(complete_on_gate=False)
    baseline = datetime.now(timezone.utc) - timedelta(seconds=1)
    refreshed = datetime.now(timezone.utc)
    ping = Mock(return_value={"status": True})

    def load_state(device_name: str) -> dict[str, object]:
        assert device_name == "MB2-A00037"
        if client.channel.gate_confirmed:
            return _fresh_device_info(
                updated_at=refreshed,
                is_recording=True,
            )
        return _fresh_device_info(
            updated_at=refreshed if ping.called else baseline
        )

    with (
        _patch_runtime_enabled(),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_STATE_POLL_INTERVAL_SEC",
            0.0,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_GATE_STATE_POLL_INTERVAL_SEC",
            0.0,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_ABORT_WAIT_TIMEOUT_SEC",
            0.0,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_mark_company_api_mutation_attempted",
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_terminate_remote_patch",
            return_value=True,
        ) as terminate,
    ):
        result = _execute_monitored_patch(
            client,
            device_name="MB2-A00037",
            script_payload=b"#!/usr/bin/env bash\n",
            state_loader=load_state,
            ping_sender=ping,
        )

    assert result["ok"] is False
    assert result["reason"] == "recording_state_unknown_or_active"
    assert result["gateAccepted"] is True
    assert result["pm2Stopping"] is False
    terminate.assert_called_once()
    assert not any(
        ".node-hid-abi-pid." in path
        for path in client.removed_paths
    )


def test_monitored_executor_joins_drain_before_final_marker_snapshot() -> None:
    client = _FakeMonitoredClient(wait_for_final_recv=True)
    baseline = datetime.now(timezone.utc) - timedelta(seconds=1)
    refreshed = datetime.now(timezone.utc)
    ping = Mock(return_value={"status": True})
    marker_append_started = threading.Event()
    release_marker_append = threading.Event()
    result_holder: dict[str, dict[str, object]] = {}
    original_append = _RemoteOutputTail.append

    def delayed_append(output_tail, chunk: bytes) -> None:
        if b"REPAIR_SUCCESS" in chunk:
            marker_append_started.set()
            assert release_marker_append.wait(timeout=2)
        original_append(output_tail, chunk)

    def load_state(device_name: str) -> dict[str, object]:
        assert device_name == "MB2-A00037"
        return _fresh_device_info(
            updated_at=refreshed if ping.called else baseline
        )

    def execute() -> None:
        result_holder["result"] = _execute_monitored_patch(
            client,
            device_name="MB2-A00037",
            script_payload=b"#!/usr/bin/env bash\n",
            state_loader=load_state,
            ping_sender=ping,
        )

    with (
        _patch_runtime_enabled(),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_STATE_POLL_INTERVAL_SEC",
            0.0,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_GATE_STATE_POLL_INTERVAL_SEC",
            0.0,
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_mark_company_api_mutation_attempted",
        ),
        patch(
            "boxer_company.routers.device_scanner_abi_patch."
            "_RemoteOutputTail.append",
            new=delayed_append,
        ),
    ):
        worker = threading.Thread(target=execute)
        worker.start()
        assert marker_append_started.wait(timeout=2)
        release_marker_append.set()
        worker.join(timeout=3)

    assert not worker.is_alive()
    result = result_holder["result"]
    assert result["ok"] is True
    assert "REPAIR_SUCCESS" in str(result["output"])
