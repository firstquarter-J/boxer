from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any, Callable

from boxer_company import settings as cs
from boxer_company.assistant.commonmark import slack_mrkdwn_to_commonmark
from boxer_company.assistant.contracts import (
    AssistantMessage,
    AssistantOutcome,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.operation_intent import (
    is_explicit_operation_execution,
    is_side_effect_permission_question,
)
from boxer_company.routers.device_audio_probe import (
    _is_device_audio_probe_request,
    _probe_device_audio_output,
)
from boxer_company.routers.device_diagnostics import (
    _build_device_diagnostic_followup_evidence,
    _build_device_diagnostic_followup_fallback,
    _has_device_diagnostic_start_hint,
    _is_device_diagnostic_freeform_request,
    _is_device_diagnostic_start_request,
    _load_device_diagnostic_snapshot,
    _select_device_diagnostic_followup_command_keys,
    _start_device_diagnostic_freeform_analysis,
    _start_device_diagnostic_snapshot,
)
from boxer_company.routers.device_led_log import (
    _is_device_led_log_analysis_request,
)
from boxer_company.routers.device_status_probe import (
    _is_device_captureboard_probe_request,
    _is_device_led_probe_request,
    _is_device_led_pattern_help_request,
    _is_device_memory_patch_request,
    _is_device_pm2_probe_request,
    _is_device_remote_access_probe_request,
    _is_device_status_probe_request,
    _patch_device_pm2_memory,
    _probe_device_remote_access,
    _probe_device_runtime_component,
    _probe_device_status_overview,
)
from boxer_company.routers.device_update import (
    _is_device_agent_update_request,
    _is_device_box_update_request,
    _is_device_power_off_request,
    _is_device_update_status_request,
    _query_device_update_status,
    _request_device_agent_update,
    _request_device_box_update,
    _request_device_power_off,
)
from boxer_company.routers.device_voice_control import (
    _build_device_voice_catalog_message,
    _build_device_voice_choices_message,
    _change_device_voice,
    _extract_device_voice_label,
    _is_device_voice_catalog_request,
    _is_device_voice_change_request,
)
from boxer_company.routers.mda_graphql import _send_mda_device_command


OperationResult = tuple[str, dict[str, Any]]
OperationFn = Callable[..., OperationResult]

_OPERATIONS_ROUTE_GROUP = "operations"
_DEVICE_NAME_LABEL_PATTERN = re.compile(
    r"(?:장비명|devicename)\s*[:=]?\s*([A-Za-z0-9._-]+)",
    re.IGNORECASE,
)
_DEVICE_NAME_TOKEN_PATTERN = re.compile(
    r"(?<![A-Za-z0-9])"
    r"([A-Za-z][A-Za-z0-9]*-[A-Za-z0-9-]*\d[A-Za-z0-9-]*)"
    r"(?![A-Za-z0-9])",
    re.IGNORECASE,
)
_POWER_OFF_EXECUTION_PATTERN = re.compile(
    r"(?:장비\s*)?(?:전원(?:을|를)?\s*)?"
    r"(?:꺼|꺼줘|꺼\s*줘|꺼주세요|꺼\s*주세요|끄기|"
    r"종료|종료해|종료해줘|종료\s*해줘|종료해주세요|"
    r"power\s*off|shut\s*down|shutdown)\s*[.!]*$",
    re.IGNORECASE,
)
_POWER_OFF_QUESTION_HINTS = (
    "왜",
    "이유",
    "원인",
    "분석",
    "꺼진",
    "꺼졌",
    "꺼짐",
    "종료된",
    "종료됐",
    "상태",
    "확인",
    "방법",
    "어떻게",
    "해도",
    "가능",
    "될까",
    "되나",
)
_MUTATING_DEVICE_OPERATION_ROUTES = frozenset(
    {
        "device_voice_change",
        "device_diagnostic_snapshot",
        "device_box_update",
        "device_agent_update",
        "device_power_off",
        "device_memory_patch",
    }
)


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


def match_device_operation_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """operations stage의 exact 단일 장비 요청만 외부 호출 없이 분류한다."""

    route = match_device_operation_candidate_route(request)
    if route == "device_diagnostic_analysis" and (
        is_side_effect_permission_question(request.question)
    ):
        return None
    if (
        route in _MUTATING_DEVICE_OPERATION_ROUTES
        and not is_explicit_operation_execution(request.question)
    ):
        return None
    if route == "device_power_off" and not _is_explicit_power_off_execution(
        request.question
    ):
        return None
    return route


def match_device_operation_candidate_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """안내 질문도 포함해 guard가 선점할 operation 후보만 분류한다."""

    route_group = str(
        request.metadata.get("route_group") or ""
    ).strip()
    if route_group != _OPERATIONS_ROUTE_GROUP:
        return None

    question = request.question
    is_voice_change = _is_device_voice_change_request(question)
    if (
        _is_device_voice_catalog_request(question)
        and not is_voice_change
    ):
        # catalog는 장비에 명령을 보내지 않는 전역 목록이라 target이 없어도 된다.
        return "device_voice_catalog"
    if _is_device_diagnostic_followup_request(request):
        return "device_diagnostic_followup"

    device_name = _extract_exact_device_name(request)
    if device_name is None:
        # live probe와 mutation은 한 장비로 확정되지 않으면 절대 실행하지 않는다.
        return None

    # 기존 read-only LED 로그/패턴 안내를 live component probe가 선점하면
    # S3 근거와 가이드 대신 장비 SSH를 실행하므로 명시적으로 넘긴다.
    if _is_device_led_log_analysis_request(
        question,
        device_name=device_name,
    ) or _is_device_led_pattern_help_request(question):
        return None

    # 구체적인 mutation과 component probe를 generic 상태보다 먼저 판정한다.
    if is_voice_change:
        return "device_voice_change"
    if _is_device_diagnostic_start_request(
        question,
        device_name=device_name,
    ):
        return "device_diagnostic_snapshot"
    if _is_device_update_status_request(
        question,
        device_name=device_name,
    ):
        return "device_update_status"
    if _is_device_box_update_request(
        question,
        device_name=device_name,
    ):
        return "device_box_update"
    if _is_device_agent_update_request(
        question,
        device_name=device_name,
    ):
        return "device_agent_update"
    if _is_device_audio_probe_request(
        question,
        device_name=device_name,
    ):
        return "device_audio_probe"
    if _is_device_remote_access_probe_request(
        question,
        device_name=device_name,
    ):
        return "device_remote_access_probe"
    if _is_device_memory_patch_request(
        question,
        device_name=device_name,
    ):
        return "device_memory_patch"
    if _is_device_pm2_probe_request(
        question,
        device_name=device_name,
    ):
        return "device_pm2_probe"
    if _is_device_captureboard_probe_request(
        question,
        device_name=device_name,
    ):
        return "device_captureboard_probe"
    if _is_device_led_probe_request(
        question,
        device_name=device_name,
    ):
        return "device_led_probe"
    if _is_device_status_probe_request(
        question,
        device_name=device_name,
    ):
        return "device_status_probe"
    if (
        _is_device_diagnostic_freeform_request(
            question,
            device_name=device_name,
        )
        and not is_explicit_operation_execution(question)
    ):
        return "device_diagnostic_analysis"
    if _is_device_power_off_request(
        question,
        device_name=device_name,
    ) and _is_explicit_power_off_execution(question):
        return "device_power_off"
    return None


def match_device_mutation_guard_candidate_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """실행 matcher가 넘긴 과거 power 질문도 local 진입 전에 잡는다."""

    route = match_device_operation_candidate_route(request)
    if route in _MUTATING_DEVICE_OPERATION_ROUTES:
        return route
    if route == "device_diagnostic_analysis":
        return (
            route
            if is_side_effect_permission_question(request.question)
            else None
        )
    device_name = _extract_exact_device_name(request)
    if (
        device_name
        and _is_device_power_off_request(
            request.question,
            device_name=device_name,
        )
    ):
        return "device_power_off"
    return None


def has_ambiguous_device_mutation_target(
    request: CompanyAssistantRequest,
) -> bool:
    """복수 장비 mutation/live probe가 exact matcher를 우회하지 못하게 한다."""

    if str(request.metadata.get("route_group") or "").strip() != (
        _OPERATIONS_ROUTE_GROUP
    ):
        return False
    devices = _explicit_device_names(request.question)
    if len(devices) <= 1:
        return False
    question = request.question
    first_device = next(iter(devices.values()))
    if _is_device_led_log_analysis_request(
        question,
        device_name=first_device,
    ) or _is_device_led_pattern_help_request(question):
        # 날짜 LED 로그와 패턴 가이드는 live 장비 operation이 아니다.
        return False
    return bool(
        _is_device_voice_change_request(question)
        or _is_device_diagnostic_start_request(
            question,
            device_name=first_device,
        )
        or _is_device_box_update_request(
            question,
            device_name=first_device,
        )
        or _is_device_agent_update_request(
            question,
            device_name=first_device,
        )
        or _is_device_memory_patch_request(
            question,
            device_name=first_device,
        )
        or _is_device_power_off_request(
            question,
            device_name=first_device,
        )
        or _is_device_update_status_request(
            question,
            device_name=first_device,
        )
        or _is_device_audio_probe_request(
            question,
            device_name=first_device,
        )
        or _is_device_remote_access_probe_request(
            question,
            device_name=first_device,
        )
        or _is_device_pm2_probe_request(
            question,
            device_name=first_device,
        )
        or _is_device_captureboard_probe_request(
            question,
            device_name=first_device,
        )
        or _is_device_led_probe_request(
            question,
            device_name=first_device,
        )
        or _is_device_status_probe_request(
            question,
            device_name=first_device,
        )
        or _is_device_diagnostic_freeform_request(
            question,
            device_name=first_device,
        )
    )


def _is_explicit_power_off_execution(question: str) -> bool:
    """과거 상태 질문을 종료 mutation으로 오인하지 않는 명령형 guard다."""

    normalized = " ".join(str(question or "").split()).strip()
    if not normalized or any(
        hint in normalized.lower()
        for hint in _POWER_OFF_QUESTION_HINTS
    ):
        return False
    return bool(_POWER_OFF_EXECUTION_PATTERN.search(normalized))


def has_device_diagnostic_followup_query(question: str) -> bool:
    """thread context를 읽을 가치가 있는 진단 후속 질문인지 판정한다."""

    return bool(_select_device_diagnostic_followup_command_keys(question))


def _is_device_diagnostic_followup_request(
    request: CompanyAssistantRequest,
) -> bool:
    if not has_device_diagnostic_followup_query(request.question):
        return False
    actor_id = str(request.actor_id or "").strip()
    if not actor_id:
        return False
    # 현재 질문만으로 일반 운영 질문을 흡수하지 않고, bounded thread
    # context에 같은 요청자의 진단 시작이 있었을 때만 API snapshot을 연다.
    return any(
        str(entry.get("author_id") or "").strip() == actor_id
        and _has_device_diagnostic_start_hint(str(entry.get("text") or ""))
        for entry in request.context_entries
        if isinstance(entry, dict)
    )


class DeviceOperationsAssistantRoute:
    """장비 operation을 채널 중립 단일 최종 결과로 실행한다."""

    name = "device_operations"

    def __init__(
        self,
        deps: DeviceOperationsRouteDeps | None = None,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._deps = deps or DeviceOperationsRouteDeps()
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        route = match_device_operation_route(request)
        if route is None:
            return None

        device_name = _extract_exact_device_name(request)
        try:
            # 각 분기는 기존 도메인 함수를 정확히 한 번만 호출한다. 이 경계는
            # mutation 실패 시 재호출하거나 다른 로컬 구현으로 fallback하지 않는다.
            result_text = self._execute_once(
                route,
                request,
                device_name=device_name,
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
            )

        return _result(
            route=route,
            outcome="answered",
            body=slack_mrkdwn_to_commonmark(result_text),
        )

    def _execute_once(
        self,
        route: str,
        request: CompanyAssistantRequest,
        *,
        device_name: str | None,
    ) -> str:
        if route == "device_voice_catalog":
            return self._deps.build_voice_catalog()
        if route == "device_diagnostic_followup":
            return self._execute_diagnostic_followup(request)
        if device_name is None:
            # matcher와 실행 사이의 불변식을 실행 경계에서도 한 번 더 닫는다.
            raise ValueError("single device is required")
        if route == "device_voice_change":
            voice_label = _extract_device_voice_label(request.question)
            if voice_label is None:
                return self._deps.build_voice_choices()
            result_text, _ = self._deps.change_voice(
                device_name,
                voice_label,
                command_dispatcher=self._deps.send_mda_command,
            )
            return result_text
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
            return result_text
        if route == "device_diagnostic_analysis":
            result_text, _ = self._deps.start_diagnostic_analysis(
                question=request.question,
                device_name=device_name,
                workspace_id=request.tenant_id,
                channel_id=_request_channel_id(request),
                thread_ts=request.conversation_id,
                requested_by=request.actor_id,
                resend_ssh_open=False,
            )
            return result_text
        if route == "device_update_status":
            result_text, _ = self._deps.query_update_status(
                device_name,
                resend_ssh_open=False,
            )
            return result_text
        if route == "device_box_update":
            result_text, _ = self._deps.request_box_update(
                request.question,
                device_name=device_name,
                resend_ssh_open=False,
            )
            return result_text
        if route == "device_agent_update":
            result_text, _ = self._deps.request_agent_update(
                request.question,
                device_name=device_name,
                resend_ssh_open=False,
            )
            return result_text
        if route == "device_power_off":
            result_text, _ = self._deps.request_power_off(
                request.question,
                device_name=device_name,
                resend_ssh_open=False,
            )
            return result_text
        if route == "device_audio_probe":
            result_text, _ = self._deps.probe_audio(
                device_name,
                resend_ssh_open=False,
            )
            return result_text
        if route == "device_remote_access_probe":
            result_text, _ = self._deps.probe_remote_access(device_name)
            return result_text
        if route == "device_memory_patch":
            result_text, _ = self._deps.patch_pm2_memory(
                device_name,
                resend_ssh_open=False,
            )
            return result_text
        if route == "device_pm2_probe":
            result_text, _ = self._deps.probe_runtime_component(
                device_name,
                component="pm2",
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return result_text
        if route == "device_captureboard_probe":
            result_text, _ = self._deps.probe_runtime_component(
                device_name,
                component="captureboard",
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return result_text
        if route == "device_led_probe":
            result_text, _ = self._deps.probe_runtime_component(
                device_name,
                component="led",
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return result_text
        if route == "device_status_probe":
            result_text, _ = self._deps.probe_status(
                device_name,
                resend_ssh_open=False,
                allow_force_reopen=False,
            )
            return result_text
        raise ValueError("unsupported device operation")

    def _execute_diagnostic_followup(
        self,
        request: CompanyAssistantRequest,
    ) -> str:
        # 진단 시작과 같은 API process 메모리 key를 사용하고, 현재 질문이
        # 요구한 read-only live evidence만 기존 helper로 한 번 수집한다.
        snapshot = self._deps.load_diagnostic_snapshot(
            workspace_id=request.tenant_id,
            channel_id=_request_channel_id(request),
            thread_ts=request.conversation_id,
        )
        if snapshot is None:
            raise ValueError("diagnostic snapshot is missing")
        explicit_devices = _explicit_device_names(request.question)
        snapshot_request = (
            snapshot.get("request")
            if isinstance(snapshot.get("request"), dict)
            else {}
        )
        snapshot_device = str(
            snapshot_request.get("deviceName") or ""
        ).strip()
        snapshot_actor = str(
            snapshot_request.get("requestedBy") or ""
        ).strip()
        current_actor = str(request.actor_id or "").strip()
        if (
            not current_actor
            or not snapshot_actor
            or snapshot_actor != current_actor
        ):
            # thread key가 actor를 포함하지 않으므로 snapshot 내부 요청자까지
            # 일치해야 다른 참여자의 live 진단을 재사용하지 않는다.
            raise ValueError("diagnostic actor scope mismatch")
        if explicit_devices:
            # 현재 질문이 장비를 다시 적었다면 저장된 snapshot과 정확히
            # 같아야 한다. 복수·불일치는 live SSH 수집 전에 fail-closed한다.
            if len(explicit_devices) != 1 or not snapshot_device:
                raise ValueError("diagnostic device scope mismatch")
            explicit_device = next(iter(explicit_devices.values()))
            if explicit_device.casefold() != snapshot_device.casefold():
                raise ValueError("diagnostic device scope mismatch")
        evidence = self._deps.build_diagnostic_followup_evidence(
            request.question,
            snapshot,
            resend_ssh_open=False,
        )
        return self._deps.build_diagnostic_followup_fallback(
            request.question,
            evidence,
        )


def _extract_exact_device_name(
    request: CompanyAssistantRequest,
) -> str | None:
    exact_by_key = _explicit_device_names(request.question)
    if len(exact_by_key) != 1:
        # mutation 의도는 질문 자체에 정확한 장비명이 한 개 있어야 한다.
        # transport metadata만으로 사용자의 실행 의도를 보충하지 않는다.
        return None
    question_key, question_name = next(iter(exact_by_key.items()))

    metadata_candidates: list[str] = []
    for metadata_key in ("device_name", "deviceName"):
        metadata_value = str(
            request.metadata.get(metadata_key) or ""
        ).strip()
        if metadata_value:
            metadata_candidates.append(metadata_value)
    metadata_by_key = _normalize_device_candidates(metadata_candidates)
    if metadata_by_key and (
        len(metadata_by_key) != 1 or question_key not in metadata_by_key
    ):
        # caller가 보낸 scope는 질문의 exact target을 좁히는 검증값일 뿐이다.
        return None
    return question_name


def _explicit_device_names(question: str) -> dict[str, str]:
    """질문 본문에 사용자가 직접 적은 유효 장비명을 모두 보존한다."""

    normalized = re.sub(r"<@[^>]+>", " ", str(question or ""))
    return _normalize_device_candidates(
        [
            *(
                match.group(1)
                for match in _DEVICE_NAME_LABEL_PATTERN.finditer(normalized)
            ),
            *(
                match.group(1)
                for match in _DEVICE_NAME_TOKEN_PATTERN.finditer(normalized)
            ),
        ]
    )


def _normalize_device_candidates(
    candidates: list[str],
) -> dict[str, str]:
    exact_by_key: dict[str, str] = {}
    for raw_candidate in candidates:
        candidate = str(raw_candidate or "").strip().strip("`'\"")
        if not candidate or not cs.S3_DEVICE_NAME_PATTERN.fullmatch(candidate):
            continue
        exact_by_key.setdefault(candidate.casefold(), candidate)
    return exact_by_key


def _request_channel_id(request: CompanyAssistantRequest) -> str:
    return str(
        request.metadata.get("channel_id") or request.channel
    ).strip()


def _result(
    *,
    route: str,
    outcome: AssistantOutcome,
    body: str,
    fallback_reason: str | None = None,
) -> CompanyAssistantResult:
    return CompanyAssistantResult(
        route=route,
        outcome=outcome,
        messages=(AssistantMessage(body=body),),
        fallback_reason=fallback_reason,
    )


__all__ = [
    "DeviceOperationsAssistantRoute",
    "DeviceOperationsRouteDeps",
    "has_ambiguous_device_mutation_target",
    "has_device_diagnostic_followup_query",
    "match_device_mutation_guard_candidate_route",
    "match_device_operation_candidate_route",
    "match_device_operation_route",
]
