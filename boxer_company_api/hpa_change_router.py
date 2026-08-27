from __future__ import annotations

from collections.abc import Callable
from typing import Annotated

from fastapi import APIRouter, Header

from boxer_company_api.auth import CallerPrincipal, CallerRegistry
from boxer_company_api.hpa_change_transport import (
    HPA_CHANGE_DELIVERY_ACK_PATH,
    HPA_CHANGE_DELIVERY_PULL_PATH,
    HPA_CHANGE_LOOKUP_PATH,
    HPA_CHANGE_SUBMIT_PATH,
    HpaChangeDeliveryAckInput,
    HpaChangeDeliveryPullInput,
    HpaChangeSubmitInput,
    HpaChangeThreadLookupInput,
    HpaChangeTransportService,
)
from boxer_company_api.policies import validate_request_id, validate_traceparent
from boxer_company_api.problems import CompanyApiProblem


HPA_CHANGE_EXECUTE_CAPABILITY = "assistant.hpa.change.execute"
ReadinessCheck = Callable[[], bool]


def create_hpa_change_router(
    *,
    service: HpaChangeTransportService,
    caller_registry: CallerRegistry,
    is_ready: ReadinessCheck,
) -> APIRouter:
    """기존 app에 한 번 include할 수 있는 HPA 전용 인증 transport router다."""

    router = APIRouter()

    def authenticate(
        authorization: str | None,
        request_id_header: str | None,
        traceparent_header: str | None,
        workspace_id: str,
        actor_id: str | None = None,
    ) -> tuple[str, CallerPrincipal]:
        request_id = validate_request_id(request_id_header)
        validate_traceparent(traceparent_header, request_id)
        principal = caller_registry.authenticate(authorization, request_id)
        if (
            HPA_CHANGE_EXECUTE_CAPABILITY not in principal.capabilities
            or not _matches(workspace_id, principal.tenant_ids)
            or not _matches("slack", principal.channels)
            or (
                actor_id is not None
                and not _matches(actor_id, principal.actor_ids)
            )
        ):
            raise CompanyApiProblem(
                status=403,
                code="caller_not_allowed",
                request_id=request_id,
            )
        if not is_ready() or not service.coordinator.enabled:
            raise CompanyApiProblem(
                status=503,
                code="service_not_ready",
                request_id=request_id,
                retryable=True,
            )
        return request_id, principal

    @router.post(HPA_CHANGE_SUBMIT_PATH, response_model=None)
    def submit_hpa_change(
        body: HpaChangeSubmitInput,
        authorization: Annotated[str | None, Header()] = None,
        x_request_id: Annotated[str | None, Header(alias="X-Request-ID")] = None,
        traceparent: Annotated[str | None, Header()] = None,
    ):
        request_id, _principal = authenticate(
            authorization,
            x_request_id,
            traceparent,
            body.workspaceId,
            body.requesterUserId,
        )
        return service.submit(request_id, body)

    @router.post(HPA_CHANGE_LOOKUP_PATH, response_model=None)
    def lookup_hpa_change(
        body: HpaChangeThreadLookupInput,
        authorization: Annotated[str | None, Header()] = None,
        x_request_id: Annotated[str | None, Header(alias="X-Request-ID")] = None,
        traceparent: Annotated[str | None, Header()] = None,
    ):
        request_id, _principal = authenticate(
            authorization,
            x_request_id,
            traceparent,
            body.workspaceId,
        )
        return service.lookup(request_id, body)

    @router.post(HPA_CHANGE_DELIVERY_PULL_PATH, response_model=None)
    def pull_hpa_change_delivery(
        body: HpaChangeDeliveryPullInput,
        authorization: Annotated[str | None, Header()] = None,
        x_request_id: Annotated[str | None, Header(alias="X-Request-ID")] = None,
        traceparent: Annotated[str | None, Header()] = None,
    ):
        request_id, _principal = authenticate(
            authorization,
            x_request_id,
            traceparent,
            body.workspaceId,
        )
        return service.pull(request_id, body)

    @router.post(HPA_CHANGE_DELIVERY_ACK_PATH, response_model=None)
    def acknowledge_hpa_change_delivery(
        body: HpaChangeDeliveryAckInput,
        authorization: Annotated[str | None, Header()] = None,
        x_request_id: Annotated[str | None, Header(alias="X-Request-ID")] = None,
        traceparent: Annotated[str | None, Header()] = None,
    ):
        request_id, _principal = authenticate(
            authorization,
            x_request_id,
            traceparent,
            body.workspaceId,
        )
        return service.acknowledge(request_id, body)

    return router


def _matches(value: str, allowed: frozenset[str]) -> bool:
    return "*" in allowed or value in allowed


__all__ = ["HPA_CHANGE_EXECUTE_CAPABILITY", "create_hpa_change_router"]
