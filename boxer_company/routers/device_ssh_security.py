from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Iterator

_API_DEVICE_SSH_CONTEXT_ACTIVE: ContextVar[bool] = ContextVar(
    "boxer_company_api_device_ssh_context_active",
    default=False,
)
class DeviceSshSecurityError(RuntimeError):
    """SSH credential이나 중복 side effect 전 fail-closed한 오류다."""


@dataclass(slots=True)
class CompanyApiDeviceSshRequestState:
    """한 API request 안에서 실제 외부 side effect 전송만 기록한다."""

    mutation_attempted: bool = False
    open_attempted: bool = False
    per_device_open_budget: bool = False
    opened_device_names: set[str] = field(default_factory=set)


_API_DEVICE_SSH_REQUEST_STATE: ContextVar[
    CompanyApiDeviceSshRequestState | None
] = ContextVar(
    "boxer_company_api_device_ssh_request_state",
    default=None,
)


@contextmanager
def company_api_device_ssh_context(
    *,
    per_device_open_budget: bool = False,
) -> Iterator[CompanyApiDeviceSshRequestState]:
    """API request의 mutation/open 예산 경계를 활성화한다."""

    context_token = _API_DEVICE_SSH_CONTEXT_ACTIVE.set(True)
    state = CompanyApiDeviceSshRequestState(
        per_device_open_budget=per_device_open_budget,
    )
    state_token = _API_DEVICE_SSH_REQUEST_STATE.set(state)
    try:
        yield state
    finally:
        _API_DEVICE_SSH_REQUEST_STATE.reset(state_token)
        _API_DEVICE_SSH_CONTEXT_ACTIVE.reset(context_token)


def _is_company_api_device_ssh_context() -> bool:
    """공통 API의 at-most-once mutation 경계를 노출한다."""

    return _API_DEVICE_SSH_CONTEXT_ACTIVE.get()


def _company_api_device_ssh_open_attempted(
    device_name: str | None = None,
) -> bool:
    """현재 turn 또는 automation의 해당 장비 open 사용 여부를 확인한다."""

    state = _API_DEVICE_SSH_REQUEST_STATE.get()
    if state is None:
        return False
    if not state.per_device_open_budget:
        return state.open_attempted
    normalized_device = str(device_name or "").strip().casefold()
    return bool(
        normalized_device
        and normalized_device in state.opened_device_names
    )


def _mark_company_api_mutation_attempted() -> None:
    """실제 외부 mutation 전송 직전에 현재 API request state를 표시한다."""

    state = _API_DEVICE_SSH_REQUEST_STATE.get()
    if state is not None:
        state.mutation_attempted = True


def _mark_company_api_device_ssh_open_attempted(
    device_name: str | None = None,
) -> None:
    """API turn의 허용된 첫 sshOrder를 외부 전송 직전에 표시한다."""

    state = _API_DEVICE_SSH_REQUEST_STATE.get()
    if state is not None:
        if state.per_device_open_budget:
            normalized_device = str(device_name or "").strip().casefold()
            if not normalized_device:
                raise DeviceSshSecurityError(
                    "device_ssh_open_identity_missing"
                )
            if normalized_device in state.opened_device_names:
                raise DeviceSshSecurityError(
                    "device_ssh_open_budget_exhausted"
                )
            state.opened_device_names.add(normalized_device)
        elif state.open_attempted:
            raise DeviceSshSecurityError("device_ssh_open_budget_exhausted")
        state.mutation_attempted = True
        state.open_attempted = True


def _prepare_device_ssh_client(
    client: Any,
    *,
    reported_host: str,
    port: int,
    paramiko_module: Any,
) -> str:
    """MDA가 보고한 endpoint를 기존 Slack과 같은 정책으로 준비한다."""

    # API도 MDA를 SSH endpoint의 신뢰원으로 사용한다. request context는
    # mutation 횟수만 제한하며 host 선택과 host-key 정책은 바꾸지 않는다.
    normalized_host = str(reported_host or "").strip()
    try:
        normalized_port = int(port)
    except (TypeError, ValueError) as exc:
        raise DeviceSshSecurityError("device_ssh_endpoint_invalid") from exc
    if not normalized_host or not 1 <= normalized_port <= 65535:
        raise DeviceSshSecurityError("device_ssh_endpoint_invalid")
    client.set_missing_host_key_policy(paramiko_module.AutoAddPolicy())
    return normalized_host


__all__ = [
    "CompanyApiDeviceSshRequestState",
    "DeviceSshSecurityError",
    "_company_api_device_ssh_open_attempted",
    "_is_company_api_device_ssh_context",
    "_mark_company_api_device_ssh_open_attempted",
    "_mark_company_api_mutation_attempted",
    "_prepare_device_ssh_client",
    "company_api_device_ssh_context",
]
