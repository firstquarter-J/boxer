from __future__ import annotations

from contextlib import asynccontextmanager
import logging
from pathlib import Path
from typing import Any

from fastapi import Cookie, Depends, FastAPI, Header, HTTPException, Query, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from boxer_adapter_web.auth import AdminCsrfManager, AdminSessionManager, verify_password
from boxer_adapter_web.chat import ChatService
from boxer_adapter_web.knowledge import KnowledgeManager
from boxer_adapter_web.realtime import AdminConnectionRegistry, WidgetConnectionRegistry
from boxer_adapter_web.schemas import (
    AdminLoginInput,
    AdminReplyInput,
    AdminAuthDto,
    AdminConversationUpdatedEventDto,
    AdminMessageCreatedEventDto,
    AdminMessageCreatedPayloadDto,
    AdminReadyEventDto,
    AdminReadyPayloadDto,
    AdminUserDto,
    ConversationListDto,
    ConversationSnapshotDto,
    ErrorPayloadDto,
    HandoffRequestPayload,
    MessageDto,
    MessageSendPayload,
    PaginationDto,
    SocketErrorEventDto,
    WidgetConversationUpdatedEventDto,
    WidgetMessageCreatedEventDto,
    WidgetSessionEndedEventDto,
    WidgetSessionReadyEventDto,
    SessionEndPayload,
    SessionInitPayload,
    WebSocketEvent,
    WorkflowStartPayload,
)
from boxer_adapter_web.security import SlidingWindowRateLimiter, is_origin_allowed, is_same_host_origin
from boxer_adapter_web.settings import WebSettings, get_web_settings
from boxer_adapter_web.storage import WebChatStore


logger = logging.getLogger(__name__)


def create_web_app() -> FastAPI:
    settings = get_web_settings()
    store = WebChatStore(settings.data_path)
    store.initialize()
    session_manager = AdminSessionManager(settings.secret_key)
    csrf_manager = AdminCsrfManager()
    knowledge_manager = KnowledgeManager(store, settings)
    chat_service = ChatService(store, settings.workflow_catalog, settings.handoff_policy)
    widget_connections = WidgetConnectionRegistry()
    admin_connections = AdminConnectionRegistry()
    widget_rate_limiter = SlidingWindowRateLimiter(limit=settings.ws_rate_limit_per_minute)

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        # 첫 실행에서도 widget이 바로 질문할 수 있게 문서가 비어 있으면 자동 동기화를 시도한다.
        try:
            knowledge_manager.ensure_initial_sync()
        except Exception:
            logger.exception("Initial web knowledge sync failed")
        yield

    app = FastAPI(title="Boxer Web Adapter", version="0.1.0", lifespan=_lifespan)
    app.state.web_settings = settings
    app.state.web_store = store
    app.state.session_manager = session_manager
    app.state.csrf_manager = csrf_manager
    app.state.knowledge_manager = knowledge_manager
    app.state.chat_service = chat_service
    app.state.widget_connections = widget_connections
    app.state.admin_connections = admin_connections

    admin_assets_dir = settings.admin_dist_path / "assets"
    if admin_assets_dir.exists():
        app.mount("/admin/assets", StaticFiles(directory=admin_assets_dir), name="admin-assets")

    @app.middleware("http")
    async def widget_api_cors(request: Request, call_next):
        if request.url.path != "/api/widget/config":
            return await call_next(request)

        origin = request.headers.get("origin")
        if origin and not is_origin_allowed(origin, settings.widget_allowed_origins):
            return JSONResponse(status_code=403, content={"detail": "Widget origin is not allowed"})
        if request.method == "OPTIONS":
            response = Response(status_code=204)
        else:
            response = await call_next(request)
        if origin:
            # widget config만 서비스 origin에서 읽도록 허용하고 admin API에는 CORS를 열지 않는다.
            response.headers["Access-Control-Allow-Origin"] = origin
            response.headers["Access-Control-Allow-Methods"] = "GET, OPTIONS"
            response.headers["Access-Control-Allow-Headers"] = "Content-Type"
            response.headers["Vary"] = "Origin"
        return response

    @app.get("/")
    def root() -> RedirectResponse:
        return RedirectResponse(url="/admin")

    @app.get("/api/health")
    def health() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "boxer-web-adapter",
            "knowledgeSource": settings.knowledge_source,
        }

    @app.get("/api/widget/config")
    def widget_config() -> dict[str, Any]:
        return {
            "welcomeTitle": settings.welcome_title,
            "welcomeMessage": settings.welcome_message,
            "starterOptions": settings.workflow_catalog.starter_options(),
            "starterEntries": settings.workflow_catalog.to_config_payload(),
            "workflowOptions": settings.workflow_catalog.to_widget_option_payload(),
            "workflowConfigVersion": "1",
            "welcomeTimeZones": {
                "ko": settings.welcome_timezone_ko,
                "en": settings.welcome_timezone_en,
            },
        }

    @app.post("/api/admin/auth/login")
    def admin_login(payload: AdminLoginInput, response: Response) -> dict[str, Any]:
        admin_user = store.get_admin_user_by_email(payload.email)
        if not admin_user or not verify_password(payload.password, str(admin_user["password_hash"])):
            raise HTTPException(status_code=401, detail="Invalid email or password")

        token = session_manager.dump(str(admin_user["id"]))
        csrf_token = csrf_manager.issue_token()
        response.set_cookie(
            key=settings.admin_cookie_name,
            value=token,
            httponly=True,
            samesite="lax",
            secure=settings.admin_cookie_secure,
            max_age=settings.admin_session_max_age_sec,
            path="/",
        )
        _set_admin_csrf_cookie(response, settings=settings, csrf_token=csrf_token)
        return AdminAuthDto(
            adminUser=_to_admin_user_dto(admin_user),
            csrfToken=csrf_token,
        ).model_dump()

    @app.post("/api/admin/auth/logout")
    def admin_logout(
        response: Response,
        admin_user: dict[str, Any] = Depends(_build_admin_write_dependency(store, session_manager, csrf_manager, settings)),
    ) -> dict[str, bool]:
        _ = admin_user
        response.delete_cookie(settings.admin_cookie_name, path="/")
        response.delete_cookie(settings.admin_csrf_cookie_name, path="/")
        return {"ok": True}

    @app.get("/api/admin/auth/me")
    def admin_me(
        response: Response,
        admin_user: dict[str, Any] = Depends(_build_admin_dependency(store, session_manager, settings)),
        csrf_token: str | None = Cookie(default=None, alias=settings.admin_csrf_cookie_name),
    ) -> dict[str, Any]:
        normalized_csrf_token = str(csrf_token or "").strip() or csrf_manager.issue_token()
        _set_admin_csrf_cookie(response, settings=settings, csrf_token=normalized_csrf_token)
        return AdminAuthDto(
            adminUser=_to_admin_user_dto(admin_user),
            csrfToken=normalized_csrf_token,
        ).model_dump()

    @app.get("/api/admin/knowledge/status")
    def knowledge_status(admin_user: dict[str, Any] = Depends(_build_admin_dependency(store, session_manager, settings))) -> dict[str, Any]:
        _ = admin_user
        return knowledge_manager.get_status().model_dump()

    @app.post("/api/admin/knowledge/sync")
    def knowledge_sync(
        admin_user: dict[str, Any] = Depends(_build_admin_write_dependency(store, session_manager, csrf_manager, settings))
    ) -> dict[str, Any]:
        _ = admin_user
        return knowledge_manager.sync_documents().model_dump()

    @app.get("/api/admin/knowledge/documents")
    def knowledge_documents(admin_user: dict[str, Any] = Depends(_build_admin_dependency(store, session_manager, settings))) -> dict[str, Any]:
        _ = admin_user
        return {
            "documents": [document.model_dump() for document in knowledge_manager.list_documents()]
        }

    @app.get("/api/admin/knowledge/documents/{document_id}")
    def knowledge_document(document_id: str, admin_user: dict[str, Any] = Depends(_build_admin_dependency(store, session_manager, settings))) -> dict[str, Any]:
        _ = admin_user
        document = knowledge_manager.get_document(document_id)
        if document is None:
            raise HTTPException(status_code=404, detail="Knowledge document not found")
        return {"document": document.model_dump()}

    @app.get("/api/admin/conversations")
    def admin_conversations(
        limit: int = Query(default=50, ge=1, le=100),
        offset: int = Query(default=0, ge=0),
        q: str = Query(default=""),
        status: str = Query(default=""),
        assigned: str = Query(default=""),
        admin_user: dict[str, Any] = Depends(_build_admin_dependency(store, session_manager, settings)),
    ) -> dict[str, Any]:
        query = q.strip() or None
        status_filter = status.strip() or None
        assigned_admin_user_id = str(admin_user["id"]) if assigned.strip() == "me" else None
        conversations = [
            chat_service._to_snapshot(conversation)
            for conversation in store.list_conversations(
                limit=limit,
                offset=offset,
                query=query,
                status=status_filter,
                assigned_admin_user_id=assigned_admin_user_id,
                include_messages=False,
            )
        ]
        return ConversationListDto(
            conversations=conversations,
            pagination=PaginationDto(
                limit=limit,
                offset=offset,
                total=store.count_conversations(
                    query=query,
                    status=status_filter,
                    assigned_admin_user_id=assigned_admin_user_id,
                ),
            ),
        ).model_dump()

    @app.get("/api/admin/conversations/{conversation_id}")
    def admin_conversation(conversation_id: str, admin_user: dict[str, Any] = Depends(_build_admin_dependency(store, session_manager, settings))) -> dict[str, Any]:
        _ = admin_user
        conversation = store.get_conversation_by_id(conversation_id)
        if conversation is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
        return {"conversation": chat_service._to_snapshot(conversation).model_dump()}

    @app.post("/api/admin/conversations/{conversation_id}/claim")
    async def admin_claim_conversation(
        conversation_id: str,
        admin_user: dict[str, Any] = Depends(_build_admin_write_dependency(store, session_manager, csrf_manager, settings)),
    ) -> dict[str, Any]:
        conversation = store.claim_conversation(conversation_id, str(admin_user["id"]))
        if conversation is None:
            raise HTTPException(status_code=409, detail="Conversation is not claimable")
        snapshot = chat_service._to_snapshot(conversation)
        await _broadcast_conversation_update(
            snapshot,
            widget_connections=widget_connections,
            admin_connections=admin_connections,
        )
        return {"conversation": snapshot.model_dump()}

    @app.post("/api/admin/conversations/{conversation_id}/release")
    async def admin_release_conversation(
        conversation_id: str,
        admin_user: dict[str, Any] = Depends(_build_admin_write_dependency(store, session_manager, csrf_manager, settings)),
    ) -> dict[str, Any]:
        conversation = store.release_conversation(conversation_id, str(admin_user["id"]))
        if conversation is None:
            raise HTTPException(status_code=409, detail="Conversation is not assigned to this admin")
        snapshot = chat_service._to_snapshot(conversation)
        await _broadcast_conversation_update(
            snapshot,
            widget_connections=widget_connections,
            admin_connections=admin_connections,
        )
        return {"conversation": snapshot.model_dump()}

    @app.post("/api/admin/conversations/{conversation_id}/close")
    async def admin_close_conversation(
        conversation_id: str,
        admin_user: dict[str, Any] = Depends(_build_admin_write_dependency(store, session_manager, csrf_manager, settings)),
    ) -> dict[str, Any]:
        conversation = store.close_conversation(conversation_id, str(admin_user["id"]))
        if conversation is None:
            raise HTTPException(status_code=409, detail="Conversation is not assigned to this admin")
        snapshot = chat_service._to_snapshot(conversation)
        await _broadcast_conversation_update(
            snapshot,
            widget_connections=widget_connections,
            admin_connections=admin_connections,
        )
        return {"conversation": snapshot.model_dump()}

    @app.post("/api/admin/conversations/{conversation_id}/reply")
    async def admin_reply_conversation(
        conversation_id: str,
        payload: AdminReplyInput,
        admin_user: dict[str, Any] = Depends(_build_admin_write_dependency(store, session_manager, csrf_manager, settings)),
    ) -> dict[str, Any]:
        conversation = store.get_conversation_by_id(conversation_id)
        if conversation is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
        if conversation.get("status") != "handoff_live" or conversation.get("assigned_admin_user_id") != admin_user["id"]:
            raise HTTPException(status_code=409, detail="Conversation is not assigned to this admin")

        message = store.create_message(
            conversation_id,
            sender_type="admin",
            sender_name=str(admin_user["name"]),
            admin_user_id=str(admin_user["id"]),
            body=payload.text,
            source_refs=[],
        )
        updated = store.get_conversation_by_id(conversation_id)
        if updated is None:
            raise HTTPException(status_code=404, detail="Conversation not found")
        snapshot = chat_service._to_snapshot(updated)
        message_dto = chat_service._to_message(message)
        await widget_connections.broadcast(
            snapshot.sessionId,
            WidgetMessageCreatedEventDto(payload=message_dto).model_dump(),
        )
        await widget_connections.broadcast(
            snapshot.sessionId,
            WidgetConversationUpdatedEventDto(payload=snapshot).model_dump(),
        )
        await _broadcast_admin_events(
            admin_connections,
            snapshot=snapshot,
            created_messages=[message_dto],
        )
        return {
            "message": message_dto.model_dump(),
            "conversation": snapshot.model_dump(),
        }

    @app.websocket("/ws/widget")
    async def widget_socket(websocket: WebSocket) -> None:
        if not is_origin_allowed(websocket.headers.get("origin"), settings.widget_allowed_origins):
            await websocket.close(code=1008)
            return

        await websocket.accept()
        active_session_id: str | None = None
        rate_limit_key = _widget_rate_limit_key(websocket)
        try:
            while True:
                raw_event = await websocket.receive_json()
                if not widget_rate_limiter.allow(rate_limit_key):
                    await websocket.send_json(
                        SocketErrorEventDto(
                            payload=ErrorPayloadDto(
                                code="rate_limited",
                                message="Too many widget events. Please slow down.",
                            )
                        ).model_dump()
                    )
                    continue

                event = WebSocketEvent.model_validate(raw_event)
                if event.type == "session.init":
                    payload = SessionInitPayload.model_validate(event.payload)
                    snapshot = chat_service.initialize_session(
                        session_id=payload.sessionId,
                        identity=payload.identity.model_dump() if payload.identity else None,
                        context=payload.context.model_dump() if payload.context else None,
                    )
                    if active_session_id != snapshot.sessionId:
                        widget_connections.unregister(active_session_id, websocket)
                        active_session_id = snapshot.sessionId
                        widget_connections.register(active_session_id, websocket)
                    await websocket.send_json(WidgetSessionReadyEventDto(payload=snapshot).model_dump())
                    continue

                if event.type == "workflow.start":
                    payload = WorkflowStartPayload.model_validate(event.payload)
                    snapshot, created_messages = chat_service.start_workflow(
                        session_id=payload.sessionId,
                        workflow_key=payload.workflowKey,
                    )
                    await _send_widget_result(
                        websocket,
                        snapshot=snapshot,
                        created_messages=created_messages,
                        admin_connections=admin_connections,
                    )
                    continue

                if event.type == "handoff.request":
                    payload = HandoffRequestPayload.model_validate(event.payload)
                    snapshot, created_messages = chat_service.request_handoff(
                        session_id=payload.sessionId,
                        reason=payload.reason,
                    )
                    await _send_widget_result(
                        websocket,
                        snapshot=snapshot,
                        created_messages=created_messages,
                        admin_connections=admin_connections,
                    )
                    continue

                if event.type == "session.end":
                    payload = SessionEndPayload.model_validate(event.payload)
                    snapshot, created_messages = chat_service.end_session(
                        session_id=payload.sessionId,
                    )
                    await _send_widget_result(
                        websocket,
                        snapshot=snapshot,
                        created_messages=created_messages,
                        admin_connections=admin_connections,
                        session_ended=True,
                    )
                    widget_connections.unregister(active_session_id, websocket)
                    active_session_id = None
                    continue

                if event.type == "message.send":
                    payload = MessageSendPayload.model_validate(event.payload)
                    snapshot, created_messages = chat_service.handle_user_message(
                        session_id=payload.sessionId,
                        text=payload.text,
                    )
                    await _send_widget_result(
                        websocket,
                        snapshot=snapshot,
                        created_messages=created_messages,
                        admin_connections=admin_connections,
                    )
                    continue

                await websocket.send_json(
                    SocketErrorEventDto(
                        payload=ErrorPayloadDto(
                            code="unknown_event",
                            message=f"Unsupported event type: {event.type}",
                        )
                    ).model_dump()
                )
        except WebSocketDisconnect:
            widget_connections.unregister(active_session_id, websocket)
            return
        except Exception as exc:
            widget_connections.unregister(active_session_id, websocket)
            await websocket.send_json(
                SocketErrorEventDto(
                    payload=ErrorPayloadDto(
                        code="server_error",
                        message=str(exc),
                    )
                ).model_dump()
            )
            await websocket.close()

    @app.websocket("/ws/admin")
    async def admin_socket(websocket: WebSocket) -> None:
        admin_origin = websocket.headers.get("origin")
        admin_host = websocket.headers.get("x-forwarded-host") or websocket.headers.get("host")
        if settings.admin_allowed_origins:
            admin_origin_allowed = is_origin_allowed(admin_origin, settings.admin_allowed_origins)
        else:
            admin_origin_allowed = is_same_host_origin(admin_origin, admin_host)
        if not admin_origin_allowed:
            await websocket.close(code=1008)
            return

        admin_user = _resolve_admin_user_from_token(
            websocket.cookies.get(settings.admin_cookie_name),
            store=store,
            session_manager=session_manager,
            settings=settings,
        )
        if admin_user is None:
            await websocket.close(code=1008)
            return

        await websocket.accept()
        admin_connections.register(websocket)
        try:
            await websocket.send_json(
                AdminReadyEventDto(
                    payload=AdminReadyPayloadDto(
                        adminUser=_to_admin_user_dto(admin_user),
                    )
                ).model_dump()
            )
            while True:
                # 브라우저가 ping을 보내는 경우만 소비한다. 실제 업데이트는 서버 push로 내려간다.
                await websocket.receive_text()
        except WebSocketDisconnect:
            admin_connections.unregister(websocket)
            return
        except Exception:
            admin_connections.unregister(websocket)
            await websocket.close()

    @app.get("/admin")
    def admin_redirect() -> RedirectResponse:
        return RedirectResponse(url="/admin/")

    @app.get("/admin/")
    @app.get("/admin/{path:path}")
    def admin_index(path: str = "") -> Response:
        _ = path
        return _serve_admin_index(settings.admin_dist_path)

    return app


def _build_admin_dependency(store: WebChatStore, session_manager: AdminSessionManager, settings: WebSettings):
    def _get_admin_user(session_token: str | None = Cookie(default=None, alias=settings.admin_cookie_name)) -> dict[str, Any]:
        admin_user = _resolve_admin_user_from_token(
            session_token,
            store=store,
            session_manager=session_manager,
            settings=settings,
        )
        if admin_user is None:
            raise HTTPException(status_code=401, detail="Admin login required")
        return admin_user

    return _get_admin_user


def _build_admin_write_dependency(
    store: WebChatStore,
    session_manager: AdminSessionManager,
    csrf_manager: AdminCsrfManager,
    settings: WebSettings,
):
    def _get_admin_user_for_write(
        session_token: str | None = Cookie(default=None, alias=settings.admin_cookie_name),
        csrf_cookie: str | None = Cookie(default=None, alias=settings.admin_csrf_cookie_name),
        csrf_header: str | None = Header(default=None, alias=settings.admin_csrf_header_name),
    ) -> dict[str, Any]:
        admin_user = _resolve_admin_user_from_token(
            session_token,
            store=store,
            session_manager=session_manager,
            settings=settings,
        )
        if admin_user is None:
            raise HTTPException(status_code=401, detail="Admin login required")
        if not csrf_manager.is_valid(csrf_cookie, csrf_header):
            raise HTTPException(status_code=403, detail="Invalid admin CSRF token")
        return admin_user

    return _get_admin_user_for_write


def _resolve_admin_user_from_token(
    session_token: str | None,
    *,
    store: WebChatStore,
    session_manager: AdminSessionManager,
    settings: WebSettings,
) -> dict[str, Any] | None:
    if not session_token:
        return None
    admin_user_id = session_manager.load(
        session_token,
        max_age=settings.admin_session_max_age_sec,
    )
    if not admin_user_id:
        return None
    return store.get_admin_user_by_id(admin_user_id)


def _set_admin_csrf_cookie(response: Response, *, settings: WebSettings, csrf_token: str) -> None:
    response.set_cookie(
        key=settings.admin_csrf_cookie_name,
        value=csrf_token,
        httponly=False,
        samesite="lax",
        secure=settings.admin_cookie_secure,
        max_age=settings.admin_session_max_age_sec,
        path="/",
    )


async def _send_widget_result(
    websocket: WebSocket,
    *,
    snapshot: ConversationSnapshotDto,
    created_messages: list[MessageDto],
    admin_connections: AdminConnectionRegistry,
    session_ended: bool = False,
) -> None:
    for message in created_messages:
        await websocket.send_json(WidgetMessageCreatedEventDto(payload=message).model_dump())
    await websocket.send_json(WidgetConversationUpdatedEventDto(payload=snapshot).model_dump())
    if session_ended:
        await websocket.send_json(WidgetSessionEndedEventDto(payload=snapshot).model_dump())
    await _broadcast_admin_events(
        admin_connections,
        snapshot=snapshot,
        created_messages=created_messages,
    )


async def _broadcast_conversation_update(
    snapshot: ConversationSnapshotDto,
    *,
    widget_connections: WidgetConnectionRegistry,
    admin_connections: AdminConnectionRegistry,
) -> None:
    await widget_connections.broadcast(
        snapshot.sessionId,
        WidgetConversationUpdatedEventDto(payload=snapshot).model_dump(),
    )
    await _broadcast_admin_events(admin_connections, snapshot=snapshot, created_messages=[])


async def _broadcast_admin_events(
    admin_connections: AdminConnectionRegistry,
    *,
    snapshot: ConversationSnapshotDto,
    created_messages: list[MessageDto],
) -> None:
    for message in created_messages:
        await admin_connections.broadcast(
            AdminMessageCreatedEventDto(
                payload=AdminMessageCreatedPayloadDto(
                    conversationId=snapshot.id,
                    message=message,
                )
            ).model_dump()
        )
    await admin_connections.broadcast(
        AdminConversationUpdatedEventDto(payload=snapshot).model_dump()
    )


def _widget_rate_limit_key(websocket: WebSocket) -> str:
    forwarded_for = websocket.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",", maxsplit=1)[0].strip()
    if websocket.client:
        return websocket.client.host
    return "unknown"


def _serve_admin_index(admin_dist_path: Path) -> Response:
    index_path = admin_dist_path / "index.html"
    if not index_path.exists():
        return AdminBuildNotFoundResponse()
    return FileResponse(index_path)


def _to_admin_user_dto(admin_user: dict[str, Any]) -> AdminUserDto:
    return AdminUserDto(
        id=str(admin_user["id"]),
        email=str(admin_user["email"]),
        name=str(admin_user["name"]),
    )


class AdminBuildNotFoundResponse(JSONResponse):
    def __init__(self) -> None:
        super().__init__(
            status_code=503,
            content={
                "ok": False,
                "message": "admin build output is missing. run `pnpm --prefix widget build:admin` first.",
            },
        )
