from __future__ import annotations

# 장비 실행은 provider-free 정본이 확정한 route와 target parser만 소비한다.
from boxer_company._operation_routing_device import (
    _OPERATIONS_ROUTE_GROUP,
)
from boxer_company.operation_routing import (
    _extract_device_name_for_audio_probe,
    _extract_device_name_for_diagnostic_freeform,
    _extract_device_name_for_diagnostic_start,
    _extract_device_name_for_remote_access_probe,
    _extract_device_name_for_status_probe,
    _extract_device_name_for_update,
    match_device_operation_route,
)
from boxer_company.read_routing import _extract_device_name_scope

from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass, replace
import hashlib
import json
import logging
import re
import threading
from typing import Any, Callable

from boxer.core import settings as core_settings
from boxer_company import settings as cs
from boxer_company.assistant.answer_composer import (
    CompanyEvidenceAnswerComposer,
    CompanyEvidenceAnswerPolicy,
)
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.routers.device_audio_probe import (
    _build_device_audio_probe_config_message,
    _probe_device_audio_output,
)
from boxer_company.routers.device_diagnostics import (
    _build_device_diagnostic_config_message,
    _build_device_diagnostic_followup_evidence,
    _build_device_diagnostic_followup_fallback,
    _build_device_diagnostic_device_required_message,
    _is_device_diagnostic_runtime_configured,
    _load_device_diagnostic_snapshot,
    _start_device_diagnostic_freeform_analysis,
    _start_device_diagnostic_snapshot,
)
from boxer_company.routers.device_status_probe import (
    _build_device_memory_patch_config_message,
    _build_device_remote_access_probe_config_message,
    _build_device_status_probe_config_message,
    _patch_device_pm2_memory,
    _probe_device_remote_access,
    _probe_device_runtime_component,
    _probe_device_status_overview,
)
from boxer_company.routers.device_scanner_abi_patch import (
    DeviceScannerAbiPatchError,
    _apply_device_scanner_abi_patch,
    _build_device_scanner_abi_patch_command_message,
    _build_device_scanner_abi_patch_config_message,
    _extract_device_name_for_scanner_abi_patch,
    _is_device_scanner_abi_patch_request,
    _is_device_scanner_abi_patch_runtime_configured,
)
from boxer_company.transport_contracts import (
    DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION,
    DEVICE_OPERATION_DELIVERY_ACTION,
    DEVICE_SCANNER_ABI_PATCH_ROUTE,
)
from boxer_company.routers.device_update import (
    _build_device_power_control_config_message,
    _build_device_update_config_message,
    _build_device_update_activity_input,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
    _request_device_power_off,
)
from boxer_company.routers.device_voice_control import (
    _build_device_voice_catalog_message,
    _build_device_voice_choices_message,
    _build_device_voice_config_message,
    _build_device_voice_device_required_message,
    _change_device_voice,
    _extract_device_voice_label,
)
from boxer_company.retrieval_rules import (
    _build_company_retrieval_rules,
    _transform_company_retrieval_payload,
)
from boxer_company.routers.mda_graphql import (
    _create_mda_activity_log,
    _send_mda_device_command,
)


OperationResult = tuple[str, dict[str, Any]]
OperationFn = Callable[..., OperationResult]
ActivityLogFn = Callable[[dict[str, Any]], dict[str, Any]]

_DELIVERED_DEVICE_OPERATION_ROUTES = frozenset(
    {
        "device_box_update",
        "device_agent_update",
        "device_power_off",
    }
)
_DEVICE_NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,159}")
_VERSION_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._+-]{0,79}")
_DELIVERY_WAIT_STATUS = frozenset({"completed", "timed_out"})
_MAX_DEVICE_OPERATION_DELIVERY_STATES = 1_024
_SYNTHESIZED_DEVICE_OPERATION_ROUTES = frozenset(
    {
        "device_audio_probe",
        "device_diagnostic_analysis",
        "device_diagnostic_followup",
    }
)


def _mda_configured() -> bool:
    return bool(cs.MDA_GRAPHQL_URL and cs.MDA_ADMIN_USER_PASSWORD)


@dataclass(frozen=True, slots=True)
class _DeviceOperationExecution:
    """domain fallback과 선택적인 합성 evidence를 함께 보존한다."""

    final_text: str
    evidence: dict[str, Any] | None = None
    operation_result: Mapping[str, Any] | None = None


class _DiagnosticSnapshotMissing(RuntimeError):
    """API process에 실제 진단 snapshot이 없음을 matcher 결과와 구분한다."""


@dataclass(frozen=True, slots=True)
class _DeviceOperationDelivery:
    """장비 명령이나 접속 주소 없이 activity에 필요한 값만 보존한다."""

    route: str
    device_name: str
    requested_version: str
    current_box_version: str
    dispatch_message: str
    wait_status: str
    wait_ok: bool


@dataclass(frozen=True, slots=True)
class DeviceOperationsRouteDeps:
    """기존 장비 도메인 함수를 한 번씩 호출하는 operations 경계다."""

    build_voice_catalog: Callable[[], str] = _build_device_voice_catalog_message
    build_voice_choices: Callable[[], str] = _build_device_voice_choices_message
    change_voice: OperationFn = _change_device_voice
    send_mda_command: Callable[..., dict[str, Any]] = _send_mda_device_command
    start_diagnostic: OperationFn = _start_device_diagnostic_snapshot
    start_diagnostic_analysis: OperationFn = (
        _start_device_diagnostic_freeform_analysis
    )
    query_update_status: OperationFn = _query_device_update_status
    request_box_update: OperationFn = _request_device_box_update
    request_agent_update: OperationFn = _request_device_agent_update
    request_power_off: OperationFn = _request_device_power_off
    apply_scanner_abi_patch: OperationFn = _apply_device_scanner_abi_patch
    probe_audio: OperationFn = _probe_device_audio_output
    probe_remote_access: OperationFn = _probe_device_remote_access
    probe_runtime_component: OperationFn = _probe_device_runtime_component
    probe_status: OperationFn = _probe_device_status_overview
    patch_pm2_memory: OperationFn = _patch_device_pm2_memory
    load_diagnostic_snapshot: Callable[..., dict[str, Any] | None] = (
        _load_device_diagnostic_snapshot
    )
    build_diagnostic_followup_evidence: Callable[
        [str, dict[str, Any]],
        dict[str, Any],
    ] = _build_device_diagnostic_followup_evidence
    build_diagnostic_followup_fallback: Callable[
        [str, dict[str, Any]],
        str,
    ] = _build_device_diagnostic_followup_fallback
    build_update_activity_input: Callable[..., dict[str, Any]] = (
        _build_device_update_activity_input
    )
    create_activity_log: ActivityLogFn = _create_mda_activity_log
    device_runtime_configured: Callable[[], bool] = (
        _is_device_diagnostic_runtime_configured
    )
    mda_configured: Callable[[], bool] = _mda_configured


def is_device_operation_delivery_receipt(
    request: CompanyAssistantRequest,
) -> bool:
    """strict delivered action인지 실행 없이 판정한다."""

    if (
        str(request.metadata.get("route_group") or "").strip()
        != _OPERATIONS_ROUTE_GROUP
    ):
        return False
    action = request.metadata.get("operation_action")
    return bool(
        isinstance(action, Mapping)
        and frozenset(action) == {"name", "phase", "delivery"}
        and action.get("name") == DEVICE_OPERATION_DELIVERY_ACTION
        and action.get("phase") == "delivered"
        and isinstance(action.get("delivery"), Mapping)
    )


class DeviceOperationsAssistantRoute:
    """장비 operation과 기존 진행 알림을 채널 중립 결과로 실행한다."""

    name = "device_operations"

    def __init__(
        self,
        deps: DeviceOperationsRouteDeps | None = None,
        *,
        answer_composer: CompanyEvidenceAnswerComposer | None = None,
        timeout_message: str = (
            "AI 답변 생성 시간이 초과됐어. 잠시 후 다시 시도해줘"
        ),
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps or DeviceOperationsRouteDeps()
        self._answer_composer = answer_composer
        self._timeout_message = timeout_message
        self._logger = logger or logging.getLogger(__name__)
        # 최초 명령 request와 같은 ID로 돌아오는 전달 receipt만 별도 추적한다.
        # fingerprint까지 묶어 같은 ack는 재사용하고 바뀐 ack는 거부한다.
        self._delivery_lock = threading.Lock()
        self._completed_deliveries: OrderedDict[
            str,
            tuple[str, CompanyAssistantResult],
        ] = OrderedDict()
        self._delivery_in_flight: dict[str, str] = {}

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        return self._handle(request)

    def handle_with_progress(
        self,
        request: CompanyAssistantRequest,
        on_partial_result: Callable[[CompanyAssistantResult], None],
    ) -> CompanyAssistantResult | None:
        """dispatch 직후 알림을 최종 장비 poll과 분리해 즉시 전달한다."""

        return self._handle(
            request,
            on_partial_result=on_partial_result,
        )

    def _handle(
        self,
        request: CompanyAssistantRequest,
        *,
        on_partial_result: Callable[[CompanyAssistantResult], None]
        | None = None,
    ) -> CompanyAssistantResult | None:
        route = match_device_operation_route(request)
        if route is None:
            return None
        if route == DEVICE_OPERATION_DELIVERY_ACTION:
            return self._handle_operation_delivery_receipt(request)

        device_name = _extract_device_name_for_route(
            route,
            request.question,
        )
        config_result = self._configuration_result(
            route,
            request.question,
            device_name=device_name,
        )
        if config_result is not None:
            return config_result
        progress_notices: list[str] = []
        try:
            # 각 분기는 기존 도메인 함수를 정확히 한 번만 호출한다. 이 경계는
            # mutation 실패 시 재호출하거나 다른 로컬 구현으로 fallback하지 않는다.
            execution = self._execute_once(
                route,
                request,
                device_name=device_name,
                progress_notices=progress_notices,
                on_partial_result=on_partial_result,
            )
        except _DiagnosticSnapshotMissing:
            # Slack local은 snapshot이 없는 thread를 진단 후속으로 소비하지
            # 않고 Notion/freeform 등 뒤 route로 넘겼다. adapter가 이 typed
            # no-match를 보고 같은 순서를 계속 탈 수 있게 원문 없이 알린다.
            return _result(
                route=route,
                outcome="no_evidence",
                body="저장된 장비 진단 상태가 없어 다른 답변 경로를 확인할게",
                fallback_reason="diagnostic_snapshot_missing",
            )
        except DeviceScannerAbiPatchError as exc:
            return _result(
                route=route,
                outcome="failed",
                body=slack_mrkdwn_to_commonmark(exc.user_message),
                fallback_reason=exc.fallback_reason,
                prefix_bodies=tuple(progress_notices),
            )
        except ValueError as exc:
            self._logger.warning(
                "Device operation input rejected request_id=%s route=%s "
                "error_type=%s",
                request.request_id,
                route,
                type(exc).__name__,
            )
            return _result(
                route=route,
                outcome="needs_input",
                body="장비 요청 형식이 올바르지 않아. 장비명과 명령을 다시 확인해줘",
                fallback_reason="invalid_request",
                prefix_bodies=tuple(progress_notices),
            )
        except Exception as exc:
            # dependency 원문이나 credential이 응답·로그에 섞이지 않게 타입만 남긴다.
            self._logger.warning(
                "Device operation failed request_id=%s route=%s error_type=%s",
                request.request_id,
                route,
                type(exc).__name__,
            )
            return _result(
                route=route,
                outcome="failed",
                body="장비 요청 처리 중 오류가 발생했어. 잠시 후 다시 시도해줘",
                fallback_reason="operation_error",
                prefix_bodies=tuple(progress_notices),
            )

        if (
            self._answer_composer is not None
            and route in _SYNTHESIZED_DEVICE_OPERATION_ROUTES
            and execution.evidence is not None
        ):
            return self._compose_evidence_answer(
                request,
                route=route,
                execution=execution,
            )

        return _result(
            route=route,
            outcome="answered",
            body=slack_mrkdwn_to_commonmark(execution.final_text),
            mention_actor=_mention_actor_for_operation_result(
                route,
                request.question,
                device_name=device_name,
            ),
            prefix_bodies=tuple(progress_notices),
            operation_result=execution.operation_result,
        )

    def _configuration_result(
        self,
        route: str,
        question: str,
        *,
        device_name: str | None,
    ) -> CompanyAssistantResult | None:
        body: str | None = None
        if route == "device_voice_change":
            # 기존 Slack은 target과 음성 선택 안내를 설정 검사보다 먼저 냈다.
            if (
                device_name
                and _extract_device_voice_label(question)
                and not self._deps.mda_configured()
            ):
                body = _build_device_voice_config_message()
        elif route in {
            "device_diagnostic_snapshot",
            "device_diagnostic_analysis",
            "device_update_status",
            "device_box_update",
            "device_agent_update",
            "device_power_off",
            "device_audio_probe",
            "device_memory_patch",
            "device_pm2_probe",
            "device_captureboard_probe",
            "device_led_probe",
            "device_status_probe",
        } and device_name and not self._deps.device_runtime_configured():
            if route.startswith("device_diagnostic"):
                body = _build_device_diagnostic_config_message()
            elif route in {
                "device_update_status",
                "device_box_update",
                "device_agent_update",
            }:
                body = _build_device_update_config_message()
            elif route == "device_power_off":
                body = _build_device_power_control_config_message()
            elif route == "device_audio_probe":
                body = _build_device_audio_probe_config_message()
            elif route == "device_memory_patch":
                body = _build_device_memory_patch_config_message()
            else:
                body = _build_device_status_probe_config_message()
        elif (
            route == DEVICE_SCANNER_ABI_PATCH_ROUTE
            and device_name
            and _is_device_scanner_abi_patch_request(
                question,
                device_name=device_name,
            )
            and not _is_device_scanner_abi_patch_runtime_configured()
        ):
            body = _build_device_scanner_abi_patch_config_message()
        elif (
            route == "device_remote_access_probe"
            and not self._deps.mda_configured()
        ):
            body = _build_device_remote_access_probe_config_message()
        if body is None:
            return None
        return _result(
            route=route,
            outcome="failed",
            body=slack_mrkdwn_to_commonmark(body),
            fallback_reason="device_runtime_not_configured",
        )

    def _execute_once(
        self,
        route: str,
        request: CompanyAssistantRequest,
        *,
        device_name: str | None,
        progress_notices: list[str],
        on_partial_result: Callable[[CompanyAssistantResult], None]
        | None,
    ) -> _DeviceOperationExecution:
        if route == "device_voice_catalog":
            return _DeviceOperationExecution(
                self._deps.build_voice_catalog()
            )
        if route == "device_diagnostic_followup":
            return self._execute_diagnostic_followup(request)
        if route == "device_voice_change":
            voice_label = _extract_device_voice_label(request.question)
            if voice_label is None:
                return _DeviceOperationExecution(
                    self._deps.build_voice_choices()
                )
            if device_name is None:
                return _DeviceOperationExecution(
                    _build_device_voice_device_required_message(
                        voice_label
                    )
                )
            result_text, _ = self._deps.change_voice(
                device_name,
                voice_label,
                command_dispatcher=self._deps.send_mda_command,
            )
            return _DeviceOperationExecution(result_text)
        if route == "device_diagnostic_snapshot" and device_name is None:
            return _DeviceOperationExecution(
                _build_device_diagnostic_device_required_message()
            )
        if route == DEVICE_SCANNER_ABI_PATCH_ROUTE:
            if not _is_device_scanner_abi_patch_request(
                request.question,
                device_name=device_name,
            ):
                raise DeviceScannerAbiPatchError(
                    _build_device_scanner_abi_patch_command_message(),
                    "device_scanner_abi_patch_command_required",
                )
            result_text, patch_result = self._deps.apply_scanner_abi_patch(
                request.question,
                device_name=device_name,
                resend_ssh_open=False,
            )
            operation_result: Mapping[str, Any] | None = None
            if (
                isinstance(patch_result, Mapping)
                and patch_result.get("route")
                == DEVICE_SCANNER_ABI_PATCH_ROUTE
                and patch_result.get("device") == device_name
                and patch_result.get("status")
                in {"repair_success", "no_action_required"}
                and re.fullmatch(
                    r"[a-f0-9]{64}",
                    str(patch_result.get("scriptSha256") or ""),
                )
            ):
                # HTTP 응답에는 노출하지 않고 API 중앙 감사 저장소만 읽는
                # 최소 receipt다. SSH endpoint나 원격 출력은 포함하지 않는다.
                operation_result = {
                    "kind": "device_scanner_abi_patch",
                    "deviceName": device_name,
                    "status": patch_result["status"],
                    "scriptSha256": patch_result["scriptSha256"],
                }
            return _DeviceOperationExecution(
                result_text,
                operation_result=operation_result,
            )
        if device_name is None:
            # 각 legacy matcher가 장비명을 찾은 route만 domain helper를 호출한다.
            raise ValueError("device is required")
        if route == "device_diagnostic_snapshot":
            result_text, _ = self._deps.start_diagnostic(
                device_name=device_name,
                question=request.question,
                workspace_id=request.tenant_id,
                channel_id=_request_channel_id(request),
                thread_ts=request.conversation_id,
                requested_by=request.actor_id,
                resend_ssh_open=False,
            )
            return _DeviceOperationExecution(result_text)
        if route == "device_diagnostic_analysis":
            result_text, evidence = self._deps.start_diagnostic_analysis(
                question=request.question,
                device_name=device_name,
                workspace_id=request.tenant_id,
                channel_id=_request_channel_id(request),
                thread_ts=request.conversation_id,
                requested_by=request.actor_id,
                resend_ssh_open=False,
            )
            return _DeviceOperationExecution(result_text, evidence)
        if route == "device_update_status":
            result_text, _ = self._deps.query_update_status(
                device_name,
                resend_ssh_open=False,
            )
            return _DeviceOperationExecution(result_text)
        if route == "device_box_update":
            return self._execute_update_operation(
                route,
                request,
                self._deps.request_box_update,
                device_name=device_name,
                progress_notices=progress_notices,
                on_partial_result=on_partial_result,
            )
        if route == "device_agent_update":
            return self._execute_update_operation(
                route,
                request,
                self._deps.request_agent_update,
                device_name=device_name,
                progress_notices=progress_notices,
                on_partial_result=on_partial_result,
            )
        if route == "device_power_off":
            return self._execute_update_operation(
                route,
                request,
                self._deps.request_power_off,
                device_name=device_name,
                progress_notices=progress_notices,
                on_partial_result=on_partial_result,
            )
        if route == "device_audio_probe":
            result_text, evidence = self._deps.probe_audio(
                device_name,
                resend_ssh_open=False,
            )
            return _DeviceOperationExecution(result_text, evidence)
        if route == "device_remote_access_probe":
            result_text, _ = self._deps.probe_remote_access(device_name)
            return _DeviceOperationExecution(result_text)
        if route == "device_memory_patch":
            result_text, _ = self._deps.patch_pm2_memory(
                device_name,
                resend_ssh_open=False,
            )
            return _DeviceOperationExecution(result_text)
        if route == "device_pm2_probe":
            result_text, _ = self._deps.probe_runtime_component(
                device_name,
                component="pm2",
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return _DeviceOperationExecution(result_text)
        if route == "device_captureboard_probe":
            result_text, _ = self._deps.probe_runtime_component(
                device_name,
                component="captureboard",
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return _DeviceOperationExecution(result_text)
        if route == "device_led_probe":
            result_text, _ = self._deps.probe_runtime_component(
                device_name,
                component="led",
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return _DeviceOperationExecution(result_text)
        if route == "device_status_probe":
            result_text, _ = self._deps.probe_status(
                device_name,
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return _DeviceOperationExecution(result_text)
        raise ValueError("unsupported device operation")

    def _execute_update_operation(
        self,
        route: str,
        request: CompanyAssistantRequest,
        operation: OperationFn,
        *,
        device_name: str,
        progress_notices: list[str],
        on_partial_result: Callable[[CompanyAssistantResult], None]
        | None,
    ) -> _DeviceOperationExecution:
        partial_sent = False

        def collect_dispatch_notice(notice_text: str) -> None:
            nonlocal partial_sent
            normalized = str(notice_text or "").strip()
            if not normalized:
                return
            if on_partial_result is None:
                # non-streaming/local 호출은 기존처럼 최종 결과 앞의 ordered
                # message로 보존한다.
                progress_notices.append(normalized)
                return
            if partial_sent:
                # legacy helper는 한 번만 호출하지만 dependency가 중복 callback을
                # 내더라도 remote Slack 진행 알림은 정확히 한 건만 보낸다.
                return
            partial_sent = True
            # API streaming 호출은 장비 완료 poll을 기다리지 않고 실제
            # dispatch callback 시점에 같은 비멘션 메시지를 내보낸다.
            on_partial_result(
                _result(
                    route=route,
                    outcome="answered",
                    body=slack_mrkdwn_to_commonmark(normalized),
                    mention_actor=False,
                )
            )

        result_text, result_payload = operation(
            request.question,
            device_name=device_name,
            on_dispatched=collect_dispatch_notice,
            # precheck·dispatch·completion poll이 같은 단일 open 예산을 쓴다.
            resend_ssh_open=False,
        )
        if on_partial_result is None:
            # non-progress/local 경로는 기존과 동일하게 명령 완료 직후 activity를
            # 기록하고 진행 문구를 최종 ordered message 앞에 붙인다.
            self._log_update_activity(request, result_payload)
            return _DeviceOperationExecution(result_text)

        operation_result = _build_pending_device_operation_result(
            route=route,
            device_name=device_name,
            result_payload=result_payload,
        )
        # progressive 경로는 Slack 최종 응답 성공 receipt가 오기 전까지 MDA
        # activity를 만들지 않는다. dispatch 실패면 pending receipt도 없다.
        return _DeviceOperationExecution(
            result_text,
            operation_result=operation_result,
        )

    def _handle_operation_delivery_receipt(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult:
        delivery = _device_operation_delivery_from_receipt(request)
        if delivery is None:
            return _delivery_ack_result(
                outcome="denied",
                fallback_reason="device_operation_delivery_receipt_invalid",
            )

        fingerprint = _device_operation_delivery_fingerprint(
            request,
            delivery,
        )
        request_id = str(request.request_id or "").strip()
        with self._delivery_lock:
            completed = self._completed_deliveries.get(request_id)
            if completed is not None:
                completed_fingerprint, completed_result = completed
                if completed_fingerprint != fingerprint:
                    return _delivery_ack_result(
                        outcome="denied",
                        fallback_reason=(
                            "device_operation_delivery_receipt_conflict"
                        ),
                    )
                self._completed_deliveries.move_to_end(request_id)
                return completed_result
            in_flight_fingerprint = self._delivery_in_flight.get(request_id)
            if in_flight_fingerprint is not None:
                if in_flight_fingerprint != fingerprint:
                    return _delivery_ack_result(
                        outcome="denied",
                        fallback_reason=(
                            "device_operation_delivery_receipt_conflict"
                        ),
                    )
                return _delivery_ack_result(
                    outcome="failed",
                    fallback_reason=(
                        "device_operation_delivery_receipt_in_progress"
                    ),
                )
            self._delivery_in_flight[request_id] = fingerprint

        try:
            # 원 장비 명령은 절대 다시 호출하지 않고, URL/host/command가 없는
            # 전달 manifest로 기존 activity payload만 한 번 복원한다.
            self._log_update_activity(
                request,
                _delivery_result_payload(delivery),
            )
            result = _delivery_ack_result(outcome="answered")
        except Exception:
            # 새 dependency가 추가돼 예외가 새더라도 reservation은 해제해
            # 운영자가 같은 exact receipt를 다시 판단할 수 있게 한다.
            with self._delivery_lock:
                self._delivery_in_flight.pop(request_id, None)
            raise

        with self._delivery_lock:
            self._completed_deliveries[request_id] = (
                fingerprint,
                result,
            )
            self._completed_deliveries.move_to_end(request_id)
            self._delivery_in_flight.pop(request_id, None)
            while (
                len(self._completed_deliveries)
                > _MAX_DEVICE_OPERATION_DELIVERY_STATES
            ):
                self._completed_deliveries.popitem(last=False)
        return result

    def _log_update_activity(
        self,
        request: CompanyAssistantRequest,
        result_payload: dict[str, Any],
    ) -> None:
        dispatch = (
            result_payload.get("dispatch")
            if isinstance(result_payload, dict)
            and isinstance(result_payload.get("dispatch"), dict)
            else {}
        )
        if not dispatch.get("status"):
            return
        try:
            # Slack callback 대신 adapter가 전달한 요청자/대화 식별자로 기존
            # MDA activity payload를 API route 안에서 그대로 만든다.
            activity_input = self._deps.build_update_activity_input(
                question=request.question,
                user_id=str(request.actor_id or "").strip(),
                user_name=_request_actor_name(request),
                channel_id=_request_channel_id(request),
                thread_ts=request.conversation_id,
                result_payload=result_payload,
            )
            self._deps.create_activity_log(activity_input)
        except Exception as exc:
            # activity 기록 실패는 이미 실행된 장비 명령 결과를 뒤집지 않는다.
            self._logger.warning(
                "Device update activity log failed request_id=%s route=%s "
                "error_type=%s",
                request.request_id,
                result_payload.get("route")
                if isinstance(result_payload, dict)
                else "",
                type(exc).__name__,
            )

    def _compose_evidence_answer(
        self,
        request: CompanyAssistantRequest,
        *,
        route: str,
        execution: _DeviceOperationExecution,
    ) -> CompanyAssistantResult:
        evidence = execution.evidence or {}
        fallback = slack_mrkdwn_to_commonmark(execution.final_text)
        validator = (
            _build_device_audio_answer_validator(fallback)
            if route == "device_audio_probe"
            else None
        )
        # 기존 Slack retrieval synthesis와 같은 prompt/rules/transform 및
        # route별 token 한도를 사용하고 provider 실패 시 domain fallback을 쓴다.
        return self._answer_composer.compose(
            request,
            evidence=evidence,
            policy=CompanyEvidenceAnswerPolicy(
                route=route,
                fallback_message=fallback,
                include_context=bool(
                    core_settings.LLM_SYNTHESIS_INCLUDE_THREAD_CONTEXT
                ),
                timeout_message=self._timeout_message,
                system_prompt=cs.RETRIEVAL_SYSTEM_PROMPT or None,
                extra_rules=_build_company_retrieval_rules(evidence),
                evidence_transform=_transform_company_retrieval_payload,
                max_tokens=(280 if route == "device_audio_probe" else 500),
                answer_validator=validator,
            ),
        )

    def _execute_diagnostic_followup(
        self,
        request: CompanyAssistantRequest,
    ) -> _DeviceOperationExecution:
        # 진단 시작과 같은 API process 메모리 key를 사용하고, 현재 질문이
        # 요구한 read-only live evidence만 기존 helper로 한 번 수집한다.
        snapshot = self._deps.load_diagnostic_snapshot(
            workspace_id=request.tenant_id,
            channel_id=_request_channel_id(request),
            thread_ts=request.conversation_id,
        )
        if snapshot is None:
            raise _DiagnosticSnapshotMissing
        # 기존 Slack thread 후속 질의는 저장된 snapshot 자체를 정본으로 썼고
        # 현재 질문의 actor/장비명으로 별도 scope 검증을 하지 않았다.
        evidence = self._deps.build_diagnostic_followup_evidence(
            request.question,
            snapshot,
            resend_ssh_open=False,
        )
        return _DeviceOperationExecution(
            self._deps.build_diagnostic_followup_fallback(
                request.question,
                evidence,
            ),
            evidence,
        )


def _build_pending_device_operation_result(
    *,
    route: str,
    device_name: str,
    result_payload: dict[str, Any],
) -> dict[str, Any] | None:
    """dispatch 성공 뒤 Slack 전달 전까지 보존할 최소 manifest를 만든다."""

    if route not in _DELIVERED_DEVICE_OPERATION_ROUTES:
        return None
    request_payload = (
        result_payload.get("request")
        if isinstance(result_payload.get("request"), dict)
        else {}
    )
    device_payload = (
        result_payload.get("device")
        if isinstance(result_payload.get("device"), dict)
        else {}
    )
    dispatch_payload = (
        result_payload.get("dispatch")
        if isinstance(result_payload.get("dispatch"), dict)
        else {}
    )
    wait_payload = (
        result_payload.get("wait")
        if isinstance(result_payload.get("wait"), dict)
        else {}
    )
    if dispatch_payload.get("status") is not True:
        return None

    delivery = _normalize_device_operation_delivery(
        route=route,
        device_name=device_name,
        requested_version=str(
            request_payload.get("requestedVersion") or ""
        ).strip(),
        current_box_version=str(
            device_payload.get("version") or ""
        ).strip(),
        dispatch_message=str(
            dispatch_payload.get("message") or ""
        ).strip(),
        wait_status=str(wait_payload.get("status") or "").strip(),
        wait_ok=wait_payload.get("ok"),
    )
    if delivery is None:
        return None
    return {
        "kind": DEVICE_OPERATION_DELIVERY_ACTION,
        "status": "pending",
        "delivery": {
            "route": delivery.route,
            "deviceName": delivery.device_name,
            "requestedVersion": delivery.requested_version,
            "currentBoxVersion": delivery.current_box_version,
            "dispatchMessage": delivery.dispatch_message,
            "waitStatus": delivery.wait_status,
            "waitOk": delivery.wait_ok,
        },
    }


def _device_operation_delivery_from_receipt(
    request: CompanyAssistantRequest,
) -> _DeviceOperationDelivery | None:
    """typed receipt를 strict manifest로 검증하고 원 질문 route와 결합한다."""

    if not is_device_operation_delivery_receipt(request):
        return None
    action = request.metadata.get("operation_action")
    assert isinstance(action, Mapping)
    raw_delivery = action.get("delivery")
    assert isinstance(raw_delivery, Mapping)
    if frozenset(raw_delivery) != {
        "route",
        "device_name",
        "requested_version",
        "current_box_version",
        "dispatch_message",
        "wait_status",
        "wait_ok",
    }:
        return None
    delivery = _normalize_device_operation_delivery(
        route=raw_delivery.get("route"),
        device_name=raw_delivery.get("device_name"),
        requested_version=raw_delivery.get("requested_version"),
        current_box_version=raw_delivery.get("current_box_version"),
        dispatch_message=raw_delivery.get("dispatch_message"),
        wait_status=raw_delivery.get("wait_status"),
        wait_ok=raw_delivery.get("wait_ok"),
    )
    if delivery is None:
        return None

    natural_metadata = dict(request.metadata)
    natural_metadata.pop("operation_action", None)
    natural_request = replace(request, metadata=natural_metadata)
    # 모듈 import cycle을 피하려고 receipt 실행 시점에만 전체 legacy
    # matcher를 가져온다. 장비 parser뿐 아니라 학습/admin/file 우선순위까지
    # 제거된 원 질문에서 다시 확인해야 위조 manifest가 activity를 못 만든다.
    # delivery manifest 재검증도 provider 실행 모듈 대신 같은 순수 정본을 쓴다.
    from boxer_company.operation_routing import (
        match_company_operation_route,
    )

    natural_route = match_company_operation_route(natural_request)
    natural_device_name = _extract_device_name_for_route(
        delivery.route,
        natural_request.question,
    )
    if (
        natural_route != delivery.route
        or natural_device_name != delivery.device_name
    ):
        return None
    return delivery


def _normalize_device_operation_delivery(
    *,
    route: Any,
    device_name: Any,
    requested_version: Any,
    current_box_version: Any,
    dispatch_message: Any,
    wait_status: Any,
    wait_ok: Any,
) -> _DeviceOperationDelivery | None:
    """activity 입력에 허용할 고정 필드와 route별 값만 정규화한다."""

    if route not in _DELIVERED_DEVICE_OPERATION_ROUTES:
        return None
    if type(device_name) is not str or _DEVICE_NAME_PATTERN.fullmatch(
        device_name
    ) is None:
        return None
    if type(requested_version) is not str:
        return None
    if route == "device_box_update":
        if _VERSION_PATTERN.fullmatch(requested_version) is None:
            return None
    elif route == "device_agent_update":
        if requested_version != "latest":
            return None
    elif requested_version:
        return None
    if (
        type(current_box_version) is not str
        or (
            current_box_version
            and _VERSION_PATTERN.fullmatch(current_box_version) is None
        )
    ):
        return None
    if not _is_safe_delivery_message(dispatch_message):
        return None
    if wait_status not in _DELIVERY_WAIT_STATUS or type(wait_ok) is not bool:
        return None
    if (wait_status == "completed") is not wait_ok:
        return None
    return _DeviceOperationDelivery(
        route=route,
        device_name=device_name,
        requested_version=requested_version,
        current_box_version=current_box_version,
        dispatch_message=dispatch_message,
        wait_status=wait_status,
        wait_ok=wait_ok,
    )


def _is_safe_delivery_message(value: Any) -> bool:
    if type(value) is not str or len(value) > 300:
        return False
    return not value or (
        value == value.strip()
        and value.isprintable()
        and "\r" not in value
        and "\n" not in value
    )


def _device_operation_delivery_fingerprint(
    request: CompanyAssistantRequest,
    delivery: _DeviceOperationDelivery,
) -> str:
    """activity에 영향을 주는 request scope와 manifest를 canonicalize한다."""

    canonical = {
        "requestId": str(request.request_id or "").strip(),
        "tenantId": request.tenant_id,
        "actorId": str(request.actor_id or "").strip(),
        "actorName": _request_actor_name(request),
        "channelId": _request_channel_id(request),
        "conversationId": request.conversation_id,
        "question": request.question,
        "delivery": {
            "route": delivery.route,
            "deviceName": delivery.device_name,
            "requestedVersion": delivery.requested_version,
            "currentBoxVersion": delivery.current_box_version,
            "dispatchMessage": delivery.dispatch_message,
            "waitStatus": delivery.wait_status,
            "waitOk": delivery.wait_ok,
        },
    }
    encoded = json.dumps(
        canonical,
        ensure_ascii=False,
        allow_nan=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _delivery_result_payload(
    delivery: _DeviceOperationDelivery,
) -> dict[str, Any]:
    """기존 activity builder가 읽는 최소 legacy payload를 복원한다."""

    return {
        "route": delivery.route,
        "request": {
            "deviceName": delivery.device_name,
            "requestedVersion": delivery.requested_version,
        },
        "device": {
            "deviceName": delivery.device_name,
            "version": delivery.current_box_version,
        },
        "dispatch": {
            "status": True,
            "message": delivery.dispatch_message,
        },
        "wait": {
            "status": delivery.wait_status,
            "ok": delivery.wait_ok,
        },
    }


def _delivery_ack_result(
    *,
    outcome: AssistantOutcome,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    """Slack adapter가 렌더하지 않는 receipt용 무해한 단일 ack다."""

    return _result(
        route=DEVICE_OPERATION_DELIVERY_ACTION,
        outcome=outcome,
        body="장비 작업 전달 결과를 확인했어",
        fallback_reason=fallback_reason,
        mention_actor=False,
    )


def _extract_device_name_for_route(
    route: str,
    question: str,
) -> str | None:
    """Slack 로컬과 같은 route별 parser가 찾은 첫 장비명을 쓴다."""

    structured_device_name = _extract_device_name_scope(question)
    if route == "device_voice_change":
        return structured_device_name
    if route == "device_diagnostic_snapshot":
        return (
            _extract_device_name_for_diagnostic_start(question)
            or structured_device_name
        )
    if route == "device_diagnostic_analysis":
        return (
            _extract_device_name_for_diagnostic_freeform(question)
            or structured_device_name
        )
    if route in {
        "device_update_status",
        "device_box_update",
        "device_agent_update",
        "device_power_off",
    }:
        return _extract_device_name_for_update(question) or structured_device_name
    if route == DEVICE_SCANNER_ABI_PATCH_ROUTE:
        return (
            _extract_device_name_for_scanner_abi_patch(question)
            or structured_device_name
        )
    if route == "device_audio_probe":
        return (
            _extract_device_name_for_audio_probe(question)
            or structured_device_name
        )
    if route == "device_remote_access_probe":
        return (
            _extract_device_name_for_remote_access_probe(question)
            or structured_device_name
        )
    if route in {
        "device_memory_patch",
        "device_pm2_probe",
        "device_captureboard_probe",
        "device_led_probe",
        "device_status_probe",
    }:
        return (
            _extract_device_name_for_status_probe(question)
            or structured_device_name
        )
    return None


def _request_channel_id(request: CompanyAssistantRequest) -> str:
    return str(
        request.metadata.get("channel_id") or request.channel
    ).strip()


def _request_actor_name(request: CompanyAssistantRequest) -> str | None:
    """adapter가 이미 알고 있는 표시 이름만 선택적으로 activity에 남긴다."""

    for key in ("actor_name", "actorName", "user_name", "userName"):
        value = request.metadata.get(key)
        if not isinstance(value, str):
            continue
        normalized = " ".join(value.split()).strip()
        if normalized:
            return normalized[:200]
    return None


def _build_device_audio_answer_validator(
    fallback_text: str,
) -> Callable[[str], bool]:
    """기존 Slack audio 합성의 필수 제목·근거 bullet 보존 규칙이다."""

    normalized_fallback = str(fallback_text or "").strip()
    required_bullets = tuple(
        bullet
        for bullet in ("• 장비:", "• 판정:", "• 근거:", "• 안내:")
        if bullet in normalized_fallback
    )
    requires_heading = normalized_fallback.startswith(
        "**장비 소리 출력 점검**"
    )

    def is_valid(answer_text: str) -> bool:
        normalized_answer = str(answer_text or "").strip()
        if requires_heading and not normalized_answer.startswith(
            "**장비 소리 출력 점검**"
        ):
            return False
        return all(
            bullet in normalized_answer for bullet in required_bullets
        )

    return is_valid


def _mention_actor_for_operation_result(
    route: str,
    question: str,
    *,
    device_name: str | None,
) -> bool:
    """기존 Slack route의 mention_user=False 분기만 그대로 보존한다."""

    if route == "device_voice_catalog":
        return False
    if route == "device_voice_change":
        # 선택지·장비명 보강 안내는 원래 mention했고 실제 변경 결과만
        # mention 없이 보냈다.
        return not bool(
            device_name and _extract_device_voice_label(question)
        )
    return route not in {
        "device_box_update",
        "device_agent_update",
        "device_power_off",
        DEVICE_SCANNER_ABI_PATCH_ROUTE,
    }


def _result(
    *,
    route: str,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
    mention_actor: bool = True,
    prefix_bodies: tuple[str, ...] = (),
    operation_result: Mapping[str, Any] | None = None,
) -> CompanyAssistantResult:
    prefix_messages = tuple(
        AssistantMessage(
            body=slack_mrkdwn_to_commonmark(prefix_body),
            mention_actor=False,
        )
        for prefix_body in prefix_bodies
        if str(prefix_body or "").strip()
    )
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,
        messages=(
            *prefix_messages,
            AssistantMessage(body=body, mention_actor=mention_actor),
        ),
        fallback_reason=fallback_reason,
        operation_result=operation_result,
    )


__all__ = [
    "DEVICE_DIAGNOSTIC_FOLLOWUP_PROBE_ACTION",
    "DEVICE_OPERATION_DELIVERY_ACTION",
    "DeviceOperationsAssistantRoute",
    "DeviceOperationsRouteDeps",
    "is_device_operation_delivery_receipt",
]
