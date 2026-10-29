"""실시간 원문 업로드는 JSON 파싱 전에 실제 전송 바이트를 제한한다."""

from __future__ import annotations

import re

from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

from boxer_company.memory.live_limits import LIVE_REQUEST_MAX_BYTES
from boxer_company_api.problems import CompanyApiProblem, problem_response

_LIVE_UPLOAD_PATH = re.compile(r"/internal/v1/memory/(?:retrievals/[^/]+/live|sources/raw)/?")


class MemoryLiveRequestBodyLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_bytes: int = LIVE_REQUEST_MAX_BYTES) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if (scope["type"] != "http" or scope["method"] != "POST"
                or not _LIVE_UPLOAD_PATH.fullmatch(scope["path"])):
            await self.app(scope, receive, send)
            return

        # Content-Length만 믿지 않고 chunked 전송과 축소한 선언 길이도 검사한다.
        # 바이트별 chunk가 많아도 Python list의 부가 메모리가 늘지 않게 모은다.
        max_length = str(self.max_bytes).encode("ascii")
        for key, value in scope.get("headers", ()):
            length = value.strip().lstrip(b"0")
            if (key.lower() == b"content-length" and length.isdigit()
                    and (len(length) > len(max_length) or int(length) > self.max_bytes)):
                await self._reject(scope, receive, send)
                return
        body = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(chunk) > self.max_bytes - len(body):
                await self._reject(scope, receive, send)
                return
            body.extend(chunk)
            if not message.get("more_body", False):
                break

        # 제한 안의 본문만 router에 넘겨 Pydantic이 무제한 JSON을 만들지 못하게 한다.
        buffered_body = bytes(body)
        body.clear()
        replayed = False

        async def bounded_receive():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": buffered_body, "more_body": False}
            return await receive()

        await self.app(scope, bounded_receive, send)

    async def _reject(self, scope: Scope, receive: Receive, send: Send) -> None:
        # 본문이나 내부 예외를 응답과 로그에 반사하지 않는 기존 problem 계약을 쓴다.
        request_id = scope.get("state", {}).get("request_id") or Headers(scope=scope).get("X-Request-ID")
        response = problem_response(CompanyApiProblem(status=413, code="validation_failed"), request_id)
        await response(scope, receive, send)
