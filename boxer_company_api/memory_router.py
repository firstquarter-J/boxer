"""인증된 Slack transport만 기억 수집을 이어가고 기존 출처를 무효화한다."""

from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Request
from pydantic import Field

from boxer_company.memory.commands import parse_memory_command
from boxer_company.memory.contracts import MemoryError
from boxer_company_api.auth import CallerRegistry
from boxer_company_api.memory_schemas import (
    Identifier,
    MemoryCheckpointInput,
    MemoryChunkInput,
    MemoryContextInput,
    MemoryInput,
    MemoryLiveResultInput,
    MemoryPullInput,
    MemoryRawEventInput,
    MemorySourceEventInput,
)
from boxer_company_api.memory_service import MemoryService
from boxer_company_api.policies import validate_request_id, validate_traceparent
from boxer_company_api.problems import CompanyApiProblem
from boxer_company_api.schemas import AssistantTurnInput, ContextEntryInput


class MemoryRetrievalInput(MemoryInput):
    tenantId: Identifier
    actorId: Identifier
    channel: Literal["slack"] = "slack"
    conversationId: Identifier
    question: str = Field(max_length=40000)
    locale: str = "ko"
    requestId: Identifier
    contextEntries: list[ContextEntryInput] = Field(default_factory=list, max_length=100)
    memoryContext: MemoryContextInput

    def to_request(self):
        # turn과 같은 문맥 정규화·교차 검증을 써서 hash가 transport마다 달라지지 않는다.
        command_route = "operations" if parse_memory_command(self.question) else "freeform"
        return AssistantTurnInput.model_validate({
            "tenantId": self.tenantId, "actorId": self.actorId, "channel": self.channel,
            "conversationId": self.conversationId, "question": self.question, "locale": self.locale,
            "contextEntries": [entry.model_dump() for entry in self.contextEntries],
            "memoryContext": self.memoryContext.model_dump(), "routeGroup": command_route,
            "scope": {"channelContextId": self.memoryContext.channelId},
        }).to_company_request(self.requestId)


def create_memory_router(*, service: MemoryService, caller_registry: CallerRegistry) -> APIRouter:
    router = APIRouter(prefix="/internal/v1/memory")

    def authorize(request: Request, tenant_id: str, actor_id: str | None = None) -> str:
        request_id = validate_request_id(request.headers.get("X-Request-ID"))
        request.state.request_id = request_id
        validate_traceparent(request.headers.get("traceparent"), request_id)
        principal = caller_registry.authenticate(request.headers.get("Authorization"), request_id)
        matches = lambda value, scope: "*" in scope or value in scope
        if (not matches("assistant.turn.read", principal.capabilities)
                or tenant_id == "*" or not matches(tenant_id, principal.tenant_ids)
                or not matches("slack", principal.channels)
                or actor_id is not None and not matches(actor_id, principal.actor_ids)):
            raise CompanyApiProblem(status=403, code="caller_not_allowed", request_id=request_id)
        if service.status() != "ok":
            raise CompanyApiProblem(status=503, code="service_not_ready", request_id=request_id, retryable=True)
        return request_id

    def invoke(transport_request_id: str, callback, **kwargs: Any):
        try:
            return callback(**kwargs)
        except MemoryError as exc:
            status = 404 if "not_found" in exc.code else 409
            raise CompanyApiProblem(status=status, code="not_found" if status == 404 else "request_id_conflict",
                                    request_id=transport_request_id) from None
        except ValueError:
            raise CompanyApiProblem(status=422, code="validation_failed", request_id=transport_request_id) from None
        except Exception:
            raise CompanyApiProblem(status=503, code="service_not_ready", request_id=transport_request_id, retryable=True) from None

    @router.post("/retrievals")
    def prepare(request: Request, body: MemoryRetrievalInput):
        request_id = authorize(request, body.tenantId, body.actorId)
        return invoke(request_id, lambda: service.prepare(body.to_request()))

    @router.post("/retrievals/{retrieval_id}/live")
    def receive_live(request: Request, retrieval_id: str, body: MemoryLiveResultInput):
        request_id = authorize(request, body.tenantId, body.actorId)
        # Slack transport가 현재 turn을 위해 조회한 원문만 서버 발행 lease에 연결한다.
        return invoke(request_id, service.receive_live, retrieval_id=retrieval_id,
                      tenant_id=body.tenantId, actor_id=body.actorId, channel_id=body.channelId,
                      request_id=body.requestId, sources=[source.model_dump() for source in body.sources],
                      context_messages=[source.model_dump() for source in body.contextMessages],
                      coverage=body.coverage, channel_count=body.channelCount)

    @router.post("/captures/pull")
    def pull(request: Request, body: MemoryPullInput):
        request_id = authorize(request, body.tenantId)
        captures = invoke(request_id, service.store.pull_captures, tenant_id=body.tenantId,
                          worker_id=body.workerId, limit=body.limit)
        return {"captures": captures}

    @router.post("/captures/{capture_id}/chunks")
    def put_chunk(request: Request, capture_id: str, body: MemoryChunkInput):
        request_id = authorize(request, body.tenantId, body.actorId)
        return invoke(request_id, service.store.put_chunk,
                      tenant_id=body.tenantId, capture_id=capture_id, actor_id=body.actorId,
                      claim_token=body.claimToken, chunk_no=body.chunkNo,
                      sources=[source.model_dump(exclude_unset=True) for source in body.sources],
                      coverage=body.coverage, is_last=body.isLast, total_chunks=body.totalChunks,
                      input_digest=body.inputDigest)

    @router.post("/captures/{capture_id}/checkpoint")
    def checkpoint(request: Request, capture_id: str, body: MemoryCheckpointInput):
        request_id = authorize(request, body.tenantId, body.actorId)
        return invoke(request_id, service.store.checkpoint,
                      tenant_id=body.tenantId, capture_id=capture_id, actor_id=body.actorId,
                      claim_token=body.claimToken, cursor=body.cursor, coverage=body.coverage,
                      state=body.state,
                      delivery_receipts=[receipt.model_dump(exclude_none=True) for receipt in body.deliveryReceipts])

    @router.post("/sources/events")
    def event(request: Request, body: MemorySourceEventInput):
        # 작성자 없는 삭제·권한 철회도 기존 source만 닫는다. 새 수집 권한은 만들지 않는다.
        request_id = authorize(request, body.tenantId)
        result = invoke(request_id, service.store.source_event,
                        tenant_id=body.tenantId, channel_id=body.channelId, message_ts=body.messageTs,
                        event_type=body.eventType, event_id=body.eventId, revision_hash=body.revisionHash)
        if (not isinstance(result, dict) or type(result.get("invalidatedCount")) is not int
                or result["invalidatedCount"] < 0):
            raise CompanyApiProblem(status=503, code="service_not_ready", request_id=request_id, retryable=True)
        invoke(request_id, service.invalidate_live_source, tenant_id=body.tenantId,
               channel_id=body.channelId, message_ts=body.messageTs,
               event_type=body.eventType, revision_hash=body.revisionHash)
        # 저장소 eventId는 삭제 journal의 내부 ID이며 noop이면 None이다. 전송 ACK는
        # 적용된 원요청 ID를 돌려줘야 Slack이 해당 outbox 항목만 지울 수 있다.
        return {"eventId": body.eventId, "invalidatedCount": result["invalidatedCount"]}

    @router.post("/sources/raw")
    def raw_source(request: Request, body: MemoryRawEventInput):
        request_id = authorize(request, body.tenantId, body.actorId)
        # 원문은 API 소유 DB에만 저장하며 추출 queue나 답변 runtime에 넣지 않는다.
        from boxer_company.memory.raw_live import ingest_raw_event

        if body.source is not None and body.actorId not in service.profiles():
            raise CompanyApiProblem(status=403, code="caller_not_allowed", request_id=request_id)
        source = body.source.model_dump(exclude_none=True) if body.source is not None else None
        if source is not None:
            source.pop("segmentStart", None)
            source.pop("segmentEnd", None)
        result = invoke(request_id, ingest_raw_event, store=service.store,
                        tenant_id=body.tenantId, channel_id=body.channelId,
                        actor_id=body.actorId, message_ts=body.messageTs,
                        event_id=body.eventId, event_ts=body.eventTs,
                        event_type=body.eventType, source=source)
        if body.eventType != "created" and result["status"] != "stale":
            # DB뿐 아니라 이미 준비된 답변의 원본 lease도 닫아 철회된 본문 재사용을 막는다.
            invoke(request_id, service.invalidate_live_source, tenant_id=body.tenantId,
                   channel_id=body.channelId, message_ts=body.messageTs, event_type=body.eventType,
                   revision_hash=body.source.revisionHash if body.source is not None else None)
        return result

    return router
