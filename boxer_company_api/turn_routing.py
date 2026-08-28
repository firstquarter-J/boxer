from __future__ import annotations

from dataclasses import dataclass

from boxer_company.assistant.contracts import CompanyAssistantRequest
from boxer_company.operation_routing import match_company_operation_route
from boxer_company.read_routing import (
    match_device_detail_route,
)


@dataclass(frozen=True, slots=True)
class EffectiveTurnRoute:
    """API가 외부 조회 없이 확정한 turn 보안·실행 범위다."""

    route_group: str | None
    client_hint_mismatch: bool = False


def resolve_effective_turn_route(
    request: CompanyAssistantRequest,
    *,
    client_route_group: str | None,
) -> EffectiveTurnRoute:
    """민감 route는 client hint보다 서버의 순수 matcher 판정을 우선한다."""

    normalized_hint = str(client_route_group or "").strip() or None

    # operation matcher는 typed action과 자연어 작업의 기존 우선순위를 모두
    # 공유한다. device mutation도 상세 조회 matcher와 겹칠 수 있어 먼저 본다.
    operation_route = match_company_operation_route(request)
    if operation_route is not None:
        return _sensitive_route(
            "operations",
            client_route_group=normalized_hint,
        )

    # 비-count 장비 상세·상태·목록은 MDA/SSH와 필요 시 sshOrder(open)을
    # 포함하므로 routeGroup 누락이나 structured/freeform 힌트로 낮출 수 없다.
    device_detail_route = match_device_detail_route(request)
    if device_detail_route is not None:
        return _sensitive_route(
            "device_detail",
            client_route_group=normalized_hint,
        )

    # 나머지 read-only rollout group은 현재 client hint로 실행 범위를 좁히되,
    # capability나 mutation 경계에는 쓰지 않는다.
    return EffectiveTurnRoute(route_group=normalized_hint)


def _sensitive_route(
    route_group: str,
    *,
    client_route_group: str | None,
) -> EffectiveTurnRoute:
    """민감 분류와 명시적 client hint가 다르면 실행 전 불일치로 표시한다."""

    return EffectiveTurnRoute(
        route_group=route_group,
        client_hint_mismatch=(
            client_route_group is not None
            and client_route_group != route_group
        ),
    )


__all__ = [
    "EffectiveTurnRoute",
    "resolve_effective_turn_route",
]
