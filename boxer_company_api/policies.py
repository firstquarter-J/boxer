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
) -> CallerPrincipal:
    """서버가 인증한 caller scope와 turn의 actor/channel/context를 교차 검증한다."""

    if not _matches_scope(
        _TURN_CAPABILITY,
        principal.capabilities,
    ):
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


def _matches_scope(value: str, allowed: frozenset[str]) -> bool:
    return "*" in allowed or value in allowed


def _raise_not_allowed(request_id: str | None) -> None:
    raise CompanyApiProblem(
        status=403,
        code="caller_not_allowed",
        request_id=request_id,
    )


__all__ = [
    "authorize_turn",
    "validate_request_id",
    "validate_traceparent",
]
