from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import hashlib
import logging
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time
from typing import Iterator
import unittest
from urllib.error import URLError
from urllib.request import urlopen

from boxer_company.assistant.contracts import (
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company_adapter_slack.company_api_client import (
    CompanyApiAvailabilityError,
    CompanyApiClientSettings,
    CompanyApiPolicyError,
    CompanyAssistantApiClient,
)


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SERVER_FACTORY = (
    "tests.company.fixtures.company_api_server:create_app"
)
_COMMERCE_QUESTION = "회사 노션에서 커머스 운영 기준 찾아줘"
_SALES_QUESTION = "회사 노션에서 영업 운영 기준 찾아줘"
_COMMERCE_BODY = (
    "**회사 Notion 문서 답변**\n"
    "- 커머스 운영 기준을 확인했어"
)
_SALES_BODY = (
    "**회사 Notion 문서 답변**\n"
    "- 영업 운영 기준을 확인했어"
)


@dataclass
class _CompanyApiProcess:
    process: subprocess.Popen[bytes]
    port: int

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    def stop(self) -> None:
        if self.process.poll() is not None:
            return
        self.process.terminate()
        try:
            self.process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait(timeout=5)


class _CollectingLogHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def _unused_loopback_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _child_environment(*, token: str, port: int) -> dict[str, str]:
    # 실제 로컬 .env와 회사 credential을 자식 프로세스에 넘기지 않고
    # fixture 실행에 필요한 최소 환경만 구성한다.
    child_env: dict[str, str] = {
        "BOXER_SKIP_DOTENV": "true",
        "BOXER_TEST_COMPANY_API_TOKEN": token,
        "BOXER_TEST_COMPANY_API_PORT": str(port),
        "PYTHONPATH": str(_PROJECT_ROOT),
        "PYTHONUNBUFFERED": "1",
    }
    for key in ("PATH", "LANG", "LC_ALL", "TMPDIR"):
        value = str(os.environ.get(key) or "").strip()
        if value:
            child_env[key] = value
    return child_env


def _wait_until_ready(server: _CompanyApiProcess) -> None:
    deadline = time.monotonic() + 10
    readiness_url = f"{server.base_url}/health/ready"
    while time.monotonic() < deadline:
        if server.process.poll() is not None:
            raise RuntimeError(
                "company_api_fixture_exited_before_readiness"
            )
        try:
            with urlopen(readiness_url, timeout=0.25) as response:
                if response.status == 200:
                    return
        except (OSError, URLError):
            time.sleep(0.05)
    raise RuntimeError("company_api_fixture_readiness_timeout")


@contextmanager
def _running_company_api(
    *,
    token: str,
) -> Iterator[_CompanyApiProcess]:
    port = _unused_loopback_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            _SERVER_FACTORY,
            "--factory",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
            "--log-level",
            "critical",
            "--no-access-log",
        ],
        cwd=_PROJECT_ROOT,
        env=_child_environment(token=token, port=port),
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    server = _CompanyApiProcess(process=process, port=port)
    try:
        _wait_until_ready(server)
        yield server
    finally:
        server.stop()


def _settings(
    *,
    base_url: str,
    token: str,
) -> CompanyApiClientSettings:
    return CompanyApiClientSettings(
        base_url=base_url,
        token=token,
        connect_timeout_sec=0.5,
        read_timeout_sec=2.0,
        max_retries=0,
        notion_mode="remote",
        notion_fallback_enabled=True,
    )


def _request(
    *,
    question: str,
    request_id: str,
) -> CompanyAssistantRequest:
    return CompanyAssistantRequest(
        request_id=request_id,
        tenant_id="T-PROCESS",
        actor_id="U-PROCESS",
        channel="slack",
        conversation_id="THREAD-PROCESS",
        question=question,
        locale="ko",
        context_entries=(
            {
                "kind": "message",
                "source": "slack",
                "author_id": "U-PROCESS",
                "text": "직전 회사 문서 질문",
                "created_at": "1785312000.000001",
            },
        ),
        metadata={"channel_id": "C-PROCESS"},
    )


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


class CompanyApiProcessIntegrationTests(unittest.TestCase):
    def test_client_crosses_real_uvicorn_process_boundary(
        self,
    ) -> None:
        token = secrets.token_urlsafe(48)
        logger = logging.getLogger(
            f"{__name__}.round_trip.{secrets.token_hex(4)}"
        )
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        captured_logs = _CollectingLogHandler()
        logger.addHandler(captured_logs)

        try:
            with _running_company_api(token=token) as server:
                client = CompanyAssistantApiClient(
                    _settings(
                        base_url=server.base_url,
                        token=token,
                    ),
                    logger=logger,
                )
                commerce_request_id = "REQ-PROCESS-COMMERCE"
                commerce = client.answer(
                    _request(
                        question=_COMMERCE_QUESTION,
                        request_id=commerce_request_id,
                    )
                )
                sales = client.answer(
                    _request(
                        question=_SALES_QUESTION,
                        request_id="REQ-PROCESS-SALES",
                    )
                )

                # 다른 service token은 같은 요청 본문이어도 runtime에
                # 진입할 수 없고, 거부 로그도 테스트 내부에서만 수집한다.
                unauthorized_client = CompanyAssistantApiClient(
                    _settings(
                        base_url=server.base_url,
                        token=secrets.token_urlsafe(48),
                    ),
                    logger=logger,
                )
                with self.assertRaises(CompanyApiPolicyError):
                    unauthorized_client.answer(
                        _request(
                            question=_COMMERCE_QUESTION,
                            request_id="REQ-PROCESS-UNAUTHORIZED",
                        )
                    )

            self.assertIs(type(commerce), CompanyAssistantResult)
            self.assertIs(type(sales), CompanyAssistantResult)
            self.assertEqual(commerce.route, "company_notion_qa")
            self.assertEqual(commerce.outcome, "answered")
            self.assertEqual(len(commerce.messages), 1)
            self.assertTrue(commerce.messages[0].body.startswith("**"))
            # 원문을 assertion failure에 출력하지 않고 exact CommonMark 왕복을
            # 검증한다.
            self.assertEqual(
                _digest(commerce.messages[0].body),
                _digest(_COMMERCE_BODY),
            )
            self.assertEqual(
                _digest(sales.messages[0].body),
                _digest(_SALES_BODY),
            )
            self.assertNotEqual(
                _digest(commerce.messages[0].body),
                _digest(sales.messages[0].body),
            )
            self.assertEqual(len(commerce.sources), 1)
            # source ID에 넣은 domain request ID로 X-Request-ID 전파도
            # 확인한다.
            self.assertEqual(
                commerce.sources[0].source_id,
                commerce_request_id,
            )

            rendered_logs = "\n".join(captured_logs.messages)
            leaked_sensitive_text = any(
                secret_value in rendered_logs
                for secret_value in (
                    token,
                    _COMMERCE_QUESTION,
                    _SALES_QUESTION,
                    _COMMERCE_BODY,
                    _SALES_BODY,
                )
            )
            self.assertFalse(leaked_sensitive_text)
        finally:
            logger.removeHandler(captured_logs)

    def test_stopped_process_becomes_sanitized_availability_error(
        self,
    ) -> None:
        token = secrets.token_urlsafe(48)
        logger = logging.getLogger(
            f"{__name__}.availability.{secrets.token_hex(4)}"
        )
        logger.setLevel(logging.DEBUG)
        logger.propagate = False
        captured_logs = _CollectingLogHandler()
        logger.addHandler(captured_logs)
        question = _COMMERCE_QUESTION
        expected_answer = _COMMERCE_BODY

        try:
            with _running_company_api(token=token) as server:
                client = CompanyAssistantApiClient(
                    _settings(
                        base_url=server.base_url,
                        token=token,
                    ),
                    logger=logger,
                )
                healthy = client.answer(
                    _request(
                        question=question,
                        request_id="REQ-PROCESS-BEFORE-STOP",
                    )
                )
                self.assertEqual(healthy.outcome, "answered")
                server.stop()

                with self.assertRaises(
                    CompanyApiAvailabilityError
                ) as captured_error:
                    client.answer(
                        _request(
                            question=question,
                            request_id="REQ-PROCESS-AFTER-STOP",
                        )
                    )

            rendered_diagnostics = "\n".join(
                [
                    str(captured_error.exception),
                    *captured_logs.messages,
                ]
            )
            # 실패 시에도 token·질문·답변 원문 포함 여부만 boolean으로
            # 비교해 unittest 자체가 민감 원문을 다시 출력하지 않게 한다.
            leaked_sensitive_text = any(
                secret_value in rendered_diagnostics
                for secret_value in (
                    token,
                    question,
                    expected_answer,
                )
            )
            self.assertFalse(leaked_sensitive_text)
        finally:
            logger.removeHandler(captured_logs)


if __name__ == "__main__":
    unittest.main()
