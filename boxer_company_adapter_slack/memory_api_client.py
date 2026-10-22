"""기억 전송만 담당하며 일반 turn이나 Slack 발송을 재시도하지 않는다."""

from __future__ import annotations

import secrets
import threading
from typing import Any, Mapping

import requests

from boxer_company.memory.live_limits import LIVE_BUNDLE_MAX_BYTES, live_bundle_size
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiClientSettings,
    CompanyApiContractError,
    _create_traceparent,
    _validate_client_settings,
)


class CompanyMemoryApiClient:
    def __init__(self, settings: CompanyApiClientSettings, *, session: Any = None):
        self._base_url = _validate_client_settings(settings)
        self._settings = settings
        self._session = session
        self._local = threading.local()

    def call(self, path: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        # 경로는 호출 코드의 고정 endpoint와 API가 발행한 ID로만 조립한다.
        if not path.startswith("/internal/v1/memory/") or ".." in path:
            raise CompanyApiContractError("company_memory_path_invalid")
        # 대량 발언도 고정 개수 대신 같은 원문 bundle의 실제 JSON 크기로 전송을 제한한다.
        if path.endswith("/live") and live_bundle_size(
                payload.get("sources") or [], payload.get("contextMessages") or []) > LIVE_BUNDLE_MAX_BYTES:
            raise CompanyApiContractError("company_memory_payload_too_large")
        session = self._session or getattr(self._local, "session", None)
        if session is None:
            session = self._local.session = requests.Session()
        response = session.post(
            self._base_url + path,
            json=dict(payload),
            headers={
                "Authorization": f"Bearer {self._settings.token}",
                "X-Request-ID": "memory:" + secrets.token_hex(16),
                "traceparent": _create_traceparent(),
                "Accept": "application/json",
            },
            timeout=(min(self._settings.connect_timeout_sec, 2), 5),
            allow_redirects=False,
        )
        if (
            response.status_code != 200
            or len(response.content) > 1_048_576
            or response.headers.get("content-type", "").split(";", 1)[0]
            != "application/json"
        ):
            raise CompanyApiContractError("company_memory_response_invalid")
        value = response.json()
        if not isinstance(value, dict):
            raise CompanyApiContractError("company_memory_response_invalid")
        return value


__all__ = ["CompanyMemoryApiClient"]
