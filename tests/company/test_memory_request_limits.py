"""실시간 업로드의 실제 바이트 제한이 JSON 파싱보다 먼저 적용되는지 확인한다."""

from __future__ import annotations

import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from boxer_company.memory.live_limits import LIVE_REQUEST_MAX_BYTES
from boxer_company_api.app import create_company_api_app
from boxer_company_api.memory_request_limits import MemoryLiveRequestBodyLimitMiddleware
from boxer_company_api.settings import CompanyApiSettings

PATH = "/internal/v1/memory/retrievals/request-1/live"


def exchange(chunks, *, headers=(), path=PATH, method="POST", limit=16, scope_type="http"):
    seen = {"reads": 0, "calls": 0, "body": bytearray(), "sent": []}
    messages = iter(chunks)

    async def receive():
        seen["reads"] += 1
        return next(messages)

    async def send(message):
        seen["sent"].append(message)

    async def app(scope, receive, send):
        seen["calls"] += 1
        if scope["type"] != "http":
            return
        while True:
            message = await receive()
            seen["body"].extend(message.get("body", b""))
            if not message.get("more_body", False):
                break
        await send({"type": "http.response.start", "status": 200, "headers": []})

    scope = {"type": scope_type, "method": method, "path": path, "headers": list(headers)}
    asyncio.run(MemoryLiveRequestBodyLimitMiddleware(app, max_bytes=limit)(scope, receive, send))
    return seen


def chunk(body, more=False):
    return {"type": "http.request", "body": body, "more_body": more}


def assert_rejected(result):
    assert result["calls"] == 0
    assert result["sent"][0]["status"] == 413
    headers = dict(result["sent"][0]["headers"])
    assert headers[b"content-type"] == b"application/problem+json"
    assert headers[b"cache-control"] == b"no-store"
    problem = json.loads(result["sent"][1]["body"])
    assert problem["status"] == 413 and problem["retryable"] is False
    assert problem["code"] == "validation_failed"
    return problem


@pytest.mark.parametrize("size", [0, 15, 16])
def test_under_and_exact_limit_preserve_the_original_bytes(size):
    body = b"x" * size
    result = exchange([chunk(body[:7], True), chunk(body[7:])])
    assert result["calls"] == 1 and result["body"] == body
    assert result["sent"][0]["status"] == 200


@pytest.mark.parametrize("declared", [b"17", b"00017", b"9" * 5000])
def test_oversized_content_length_rejects_before_reading_or_parsing(declared):
    result = exchange([], headers=[(b"content-length", declared), (b"x-request-id", b"size-check")])
    assert assert_rejected(result)["requestId"] == "size-check"
    assert result["reads"] == 0


@pytest.mark.parametrize("headers", [[], [(b"content-length", b"1")], [(b"content-length", b"invalid")]])
def test_actual_stream_limit_cannot_be_bypassed_by_missing_or_false_length(headers):
    # 한도를 넘은 chunk 뒤는 읽지 않고 본문도 router에 넘기지 않는다.
    result = exchange([chunk(b"x" * 8, True), chunk(b"y" * 9, True)], headers=headers)
    assert_rejected(result)
    assert result["reads"] == 2


def test_utf8_limit_counts_bytes_and_does_not_echo_or_log_the_body(caplog):
    secret = "민감한본문"
    result = exchange([chunk(secret.encode("utf-8"))], limit=10)
    problem = assert_rejected(result)
    assert secret not in json.dumps(problem, ensure_ascii=False)
    assert secret not in caplog.text


def test_disconnect_during_upload_never_calls_the_router():
    result = exchange([chunk(b"small", True), {"type": "http.disconnect"}])
    assert result["calls"] == 0 and result["sent"] == []


@pytest.mark.parametrize("path,method,scope_type", [
    ("/internal/v1/memory/retrievals/request-1/live/", "POST", "http"),
    (PATH, "POST", "http"),
])
def test_both_live_route_spellings_are_capped(path, method, scope_type):
    assert_rejected(exchange([chunk(b"x" * 17)], path=path, method=method, scope_type=scope_type))


@pytest.mark.parametrize("path,method,scope_type", [
    ("/internal/v1/assistant/turns", "POST", "http"),
    ("/internal/v1/memory/retrievals", "POST", "http"),
    ("/internal/v1/memory/retrievals/request-1/live/extra", "POST", "http"),
    (PATH, "GET", "http"),
    (PATH, "POST", "websocket"),
])
def test_unrelated_routes_and_non_http_scopes_keep_their_original_behavior(path, method, scope_type):
    result = exchange([chunk(b"x" * 17)], path=path, method=method, scope_type=scope_type)
    assert result["calls"] == 1
    if scope_type == "http":
        assert result["body"] == b"x" * 17


def test_application_registration_rejects_large_json_before_authentication_and_schema():
    app = create_company_api_app(
        settings=CompanyApiSettings(host="127.0.0.1", port=8010, callers=()),
        assistant_runtime=None, readiness_probe=lambda: False,
        automation_coordinator=None, automation_delivery_broker=None, hpa_change_coordinator=None,
    )
    client = TestClient(app)
    result = client.post(PATH, content=b"x" * (LIVE_REQUEST_MAX_BYTES + 1),
                         headers={"Content-Type": "application/json", "X-Request-ID": "large-live-upload"})
    assert result.status_code == 413 and result.headers["content-type"] == "application/problem+json"
    assert result.json()["requestId"] == "large-live-upload"
    # 다른 POST 경로는 이 전용 제한 대신 기존 인증과 JSON 검증 계약을 유지한다.
    other = client.post("/internal/v1/memory/retrievals", content=b"x" * (LIVE_REQUEST_MAX_BYTES + 1),
                        headers={"Content-Type": "application/json"})
    assert other.status_code != 413
