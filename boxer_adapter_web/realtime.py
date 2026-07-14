from __future__ import annotations

from typing import Any

from fastapi import WebSocket


class WidgetConnectionRegistry:
    def __init__(self) -> None:
        self._connections: dict[str, set[WebSocket]] = {}

    def register(self, session_id: str, websocket: WebSocket) -> None:
        self._connections.setdefault(session_id, set()).add(websocket)

    def unregister(self, session_id: str | None, websocket: WebSocket) -> None:
        if not session_id:
            return
        sockets = self._connections.get(session_id)
        if not sockets:
            return
        sockets.discard(websocket)
        if not sockets:
            self._connections.pop(session_id, None)

    async def broadcast(self, session_id: str, event: dict[str, Any]) -> None:
        sockets = list(self._connections.get(session_id) or [])
        for socket in sockets:
            try:
                await socket.send_json(event)
            except Exception:
                self.unregister(session_id, socket)


class AdminConnectionRegistry:
    def __init__(self) -> None:
        self._connections: set[WebSocket] = set()

    def register(self, websocket: WebSocket) -> None:
        self._connections.add(websocket)

    def unregister(self, websocket: WebSocket) -> None:
        self._connections.discard(websocket)

    async def broadcast(self, event: dict[str, Any]) -> None:
        sockets = list(self._connections)
        for socket in sockets:
            try:
                await socket.send_json(event)
            except Exception:
                self.unregister(socket)
