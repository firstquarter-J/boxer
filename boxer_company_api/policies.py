from __future__ import annotations

import re

from boxer_company_api.auth import CallerPrincipal
from boxer_company_api.problems import CompanyApiProblem
from boxer_company_api.schemas import AssistantTurnInput


_REQUEST_ID_PATTERN = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}$"
)
_TRACEPARENT_PATTERN = re.compile(
    r"^(?P<version>[0-9a-f]{2})-"
    r"(?P<trace_id>[0-9a-f]{32})-"
    r"(?P<parent_id>[0-9a-f]{16})-"
    r"(?P<flags>[0-9a-f]{2})$"
)
_CHANNEL_CONTEXT_SOURCES = {
    "slack": frozenset({"slack"}),
    "web": frozenset({"widget"}),
    # API caller는 원천 채널을 가장하지 않고 문맥이 없을 때만 직접 호출한다.
    "api": frozenset(),
}
_TURN_CAPABILITY = "assistant.turn.read"
_DEVICE_PROBE_CAPABILITY = "assistant.device.probe"
_DEVICE_SSH_OPEN_CAPABILITY = "assistant.device.ssh.open"
_OPERATION_EXECUTE_CAPABILITY = "assistant.operation.execute"
_DEVICE_HEALTH_ALERT_EXECUTE_CAPABILITY = (
    "assistant.device.alert.execute"
)
_AUTOMATION_TRANSPORT_CAPABILITY = "assistant.automation.transport"


def validate_request_id(value: str | None) -> str:
    """유효한 correlation ID를 반환하고, 없거나 위험하면 안전한 400을 발생시킨다."""

    normalized = str(value or "").strip()
    if not _REQUEST_ID_PATTERN.fullmatch(normalized):
        raise CompanyApiProblem(
            status=400,
            code="invalid_request_id",
        )
    return normalized


def validate_traceparent(
    value: str | None,
    request_id: str | None = None,
) -> str | None:
    """W3C traceparent의 초기 고정 형식만 받아 로그 주입과 잘못된 전파를 막는다."""

    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip()
    matched = _TRACEPARENT_PATTERN.fullmatch(normalized)
    if (
        matched is None
        or matched.group("version") == "ff"
        or matched.group("trace_id") == "0" * 32
        or matched.group("parent_id") == "0" * 16
    ):
        raise CompanyApiProblem(
            status=400,
            code="invalid_traceparent",
            request_id=request_id,
        )
    return normalized


def authorize_turn(
    principal: CallerPrincipal,
    turn: AssistantTurnInput,
    request_id: str | None = None,
    *,
    effective_route_group: str | None,
) -> CallerPrincipal:
    """서버가 인증한 caller scope와 turn의 actor/channel/context를 교차 검증한다."""

    # 민감 capability는 caller의 route hint가 아니라 서버 matcher가 확정한
    # route만 검사한다. 호출자가 이 값을 생략하는 호환 경로는 두지 않는다.
    route_group = effective_route_group
    if not _matches_scope(
        _TURN_CAPABILITY,
        principal.capabilities,
    ):
        _raise_not_allowed(request_id)
    if route_group == "device_detail" and any(
        not _matches_scope(capability, principal.capabilities)
        for capability in (
            _DEVICE_PROBE_CAPABILITY,
            _DEVICE_SSH_OPEN_CAPABILITY,
        )
    ):
        # 이 assistant turn 하나가 MDA/SSH probe와 필요 시 tunnel open까지
        # 수행하므로 두 장비 권한을 모두 가진 caller만 진입시킨다.
        _raise_not_allowed(request_id)
    if route_group == "operations" and not _matches_scope(
        _OPERATION_EXECUTE_CAPABILITY,
        principal.capabilities,
    ):
        # 작업 stage는 조회 capability로 우회할 수 없고,
        # mutation 실행을 명시적으로 허용한 caller만 받는다.
        _raise_not_allowed(request_id)
    operation_action_name = str(
        getattr(turn.operationAction, "name", "") or ""
    ).strip()
    if (
        operation_action_name.startswith("device_health_alert_")
        and not _matches_scope(
            _DEVICE_HEALTH_ALERT_EXECUTE_CAPABILITY,
            principal.capabilities,
        )
    ):
        # 일반 operations 권한만으로 기존 Slack 알림 카드의 MDA/SMS
        # mutation을 만들 수 없게 별도 capability를 한 번 더 요구한다.
        _raise_not_allowed(request_id)
    if not _matches_scope(turn.tenantId, principal.tenant_ids):
        _raise_not_allowed(request_id)
    if not _matches_scope(turn.channel, principal.channels):
        _raise_not_allowed(request_id)

    if turn.actorId is None:
        # 공개 CS 전용 capability와 route subset이 생기기 전에는
        # 익명 Web actor가 회사 조회 runtime에 진입하지 못하게 닫아 둔다.
        _raise_not_allowed(request_id)
    if not _matches_scope(turn.actorId, principal.actor_ids):
        _raise_not_allowed(request_id)

    allowed_sources = _CHANNEL_CONTEXT_SOURCES.get(
        turn.channel,
        frozenset(),
    )
    if any(
        entry.kind != "message"
        or entry.source not in allowed_sources
        for entry in turn.contextEntries
    ):
        _raise_not_allowed(request_id)
    return principal


def authorize_automation_transport(
    principal: CallerPrincipal,
    tenant_id: str,
    request_id: str | None = None,
) -> CallerPrincipal:
    """Slack gateway가 domain 실행 없이 pull/ACK만 하도록 별도 검증한다."""

    if not _matches_scope(
        _AUTOMATION_TRANSPORT_CAPABILITY,
        principal.capabilities,
    ):
        _raise_not_allowed(request_id)
    if not _matches_scope(tenant_id, principal.tenant_ids):
        _raise_not_allowed(request_id)
    if not _matches_scope("slack", principal.channels):
        _raise_not_allowed(request_id)
    return principal


def _matches_scope(value: str, allowed: frozenset[str]) -> bool:
    return "*" in allowed or value in allowed


def _raise_not_allowed(request_id: str | None) -> None:
    raise CompanyApiProblem(
        status=403,
        code="caller_not_allowed",
        request_id=request_id,
    )


__all__ = [
    "authorize_automation_transport",
    "authorize_turn",
    "validate_request_id",
    "validate_traceparent",
]
