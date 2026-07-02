import json
import re
import shlex
import subprocess
import time
from typing import Any
from urllib import error, request

import anthropic
from anthropic import Anthropic

from boxer.core import settings as s

_THINK_BLOCK_RE = re.compile(r"<think>.*?</think>", re.IGNORECASE | re.DOTALL)
_REASONING_PREFIX_RE = re.compile(
    r"^\s*(okay\b|ok\b|let'?s\b|let me\b|first\b|the user\b|looking at\b|wait\b|now\b|based on\b|i need\b|we need\b|so,?\b|hmm\b|i should\b|check if\b|the evidence\b|each entry\b|therefore\b)",
    re.IGNORECASE,
)
_FINAL_SECTION_MARKERS = (
    "*에러 분석*",
    "## 에러 분석",
    "에러 분석",
    "• 핵심 원인:",
    "핵심 원인:",
)


def _flatten_error_text(value: Any) -> str:
    if isinstance(value, dict):
        return " ".join(_flatten_error_text(item) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten_error_text(item) for item in value)
    return str(value or "")


def _is_anthropic_credit_unavailable_error(exc: Exception) -> bool:
    body_text = _flatten_error_text(getattr(exc, "body", None))
    combined = f"{body_text} {getattr(exc, 'message', '')} {str(exc)}".lower()
    return any(
        token in combined
        for token in (
            "credit balance",
            "credits",
            "insufficient_quota",
            "insufficient quota",
            "quota_exceeded",
            "billing",
            "payment",
            "prepaid",
        )
    )


def _resolve_anthropic_auth_token() -> str:
    static_token = (s.ANTHROPIC_AUTH_TOKEN or "").strip()
    if static_token:
        return static_token

    command = (s.ANTHROPIC_AUTH_TOKEN_COMMAND or "").strip()
    if not command:
        return ""

    # OAuth 토큰은 만료될 수 있어서, 필요한 경우 외부 helper가 최신 토큰을 stdout으로 돌려준다.
    try:
        result = subprocess.run(
            shlex.split(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=max(1, s.ANTHROPIC_AUTH_TOKEN_COMMAND_TIMEOUT_SEC),
        )
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("Anthropic OAuth token command timed out") from exc
    except OSError as exc:
        raise RuntimeError("Failed to execute Anthropic OAuth token command") from exc
    if result.returncode != 0:
        raise RuntimeError(
            f"Anthropic OAuth token command failed with exit code {result.returncode}"
        )
    token_lines = (result.stdout or "").strip().splitlines()
    token = token_lines[0].strip() if token_lines else ""
    if not token:
        raise RuntimeError("Anthropic OAuth token command returned an empty token")
    return token


def _build_claude_client(*, timeout_sec: int | None = None) -> Anthropic:
    actual_timeout = timeout_sec if timeout_sec is not None else s.ANTHROPIC_TIMEOUT_SEC
    auth_token = _resolve_anthropic_auth_token()
    if auth_token:
        client = Anthropic(auth_token=auth_token, timeout=actual_timeout)
        # SDK는 env의 ANTHROPIC_API_KEY도 자동으로 읽으므로 OAuth 사용 시 API key 헤더를 명시적으로 끈다.
        client.api_key = None
        return client
    return Anthropic(api_key=s.ANTHROPIC_API_KEY, timeout=actual_timeout)


def _refresh_claude_oauth_token(client: Anthropic) -> None:
    if not (s.ANTHROPIC_AUTH_TOKEN or s.ANTHROPIC_AUTH_TOKEN_COMMAND):
        return
    auth_token = _resolve_anthropic_auth_token()
    if not auth_token:
        return
    # 장기 실행 서버에서 helper가 갱신한 OAuth 토큰을 다음 호출에 반영한다.
    client.auth_token = auth_token
    client.api_key = None


def _sanitize_ollama_output(text: str) -> str:
    cleaned = _THINK_BLOCK_RE.sub("", text or "").strip()
    if not cleaned:
        return ""

    if "</think>" in cleaned.lower():
        cleaned = cleaned.rsplit("</think>", 1)[-1].strip()
        if not cleaned:
            return ""

    for marker in _FINAL_SECTION_MARKERS:
        index = cleaned.find(marker)
        if index > 0:
            cleaned = cleaned[index:].strip()
            break

    if not cleaned:
        return ""

    lines = cleaned.splitlines()
    filtered: list[str] = []
    skipping_prefix = True
    for line in lines:
        stripped = line.strip()
        if not stripped:
            if filtered and filtered[-1] != "":
                filtered.append("")
            continue
        if skipping_prefix and _REASONING_PREFIX_RE.match(stripped):
            continue
        skipping_prefix = False
        filtered.append(stripped)

    return "\n".join(filtered).strip()


def _ask_claude(
    client: Anthropic,
    question: str,
    system_prompt: str | None = None,
    *,
    max_tokens: int | None = None,
) -> str:
    return _ask_claude_with_meta(
        client,
        question,
        system_prompt=system_prompt,
        max_tokens=max_tokens,
    )["text"]


def _ask_claude_with_meta(
    client: Anthropic,
    question: str,
    system_prompt: str | None = None,
    *,
    max_tokens: int | None = None,
) -> dict[str, str]:
    _refresh_claude_oauth_token(client)
    prompt = (system_prompt or s.DEFAULT_SYSTEM_PROMPT).strip()
    result = client.messages.create(
        model=s.ANTHROPIC_MODEL,
        max_tokens=max_tokens or s.ANTHROPIC_MAX_TOKENS,
        system=prompt,
        messages=[{"role": "user", "content": question}],
    )
    text_blocks = [
        block.text
        for block in result.content
        if getattr(block, "type", "") == "text"
    ]
    return {
        "text": "".join(text_blocks).strip(),
        "stop_reason": str(getattr(result, "stop_reason", "") or "").strip(),
    }


def _check_claude_health(
    client: Anthropic | None = None,
    *,
    timeout_sec: int | None = None,
    model: str | None = None,
) -> dict[str, str | bool]:
    actual_timeout = max(
        1,
        timeout_sec if timeout_sec is not None else min(5, s.ANTHROPIC_TIMEOUT_SEC),
    )
    started_at = time.monotonic()
    configured_model = (model or s.ANTHROPIC_MODEL or "").strip()

    try:
        health_client = client or _build_claude_client(timeout_sec=actual_timeout)
        if client is not None:
            _refresh_claude_oauth_token(health_client)
        health_client.messages.create(
            model=configured_model,
            max_tokens=1,
            messages=[{"role": "user", "content": "ping"}],
        )
    except anthropic.APITimeoutError:
        return {
            "ok": False,
            "summary": f"응답 없음 ({actual_timeout}초 초과)",
        }
    except anthropic.AuthenticationError:
        return {
            "ok": False,
            "summary": "인증 실패",
        }
    except anthropic.PermissionDeniedError as exc:
        if _is_anthropic_credit_unavailable_error(exc):
            return {
                "ok": False,
                "summary": "크레딧 부족",
            }
        return {
            "ok": False,
            "summary": "권한 없음",
        }
    except anthropic.RateLimitError as exc:
        if _is_anthropic_credit_unavailable_error(exc):
            return {
                "ok": False,
                "summary": "크레딧 부족",
            }
        return {
            "ok": False,
            "summary": "호출 제한",
        }
    except anthropic.APIConnectionError as exc:
        return {
            "ok": False,
            "summary": f"연결 실패 ({str(exc) or 'connection failed'})",
        }
    except anthropic.BadRequestError as exc:
        if _is_anthropic_credit_unavailable_error(exc):
            return {
                "ok": False,
                "summary": "크레딧 부족",
            }
        return {
            "ok": False,
            "summary": f"요청 실패 ({exc.status_code})",
        }
    except anthropic.APIStatusError as exc:
        return {
            "ok": False,
            "summary": f"HTTP {exc.status_code}",
        }
    except anthropic.AnthropicError as exc:
        return {
            "ok": False,
            "summary": f"응답 오류 ({type(exc).__name__})",
        }
    except Exception as exc:
        return {
            "ok": False,
            "summary": f"응답 오류 ({type(exc).__name__})",
        }

    latency_ms = int((time.monotonic() - started_at) * 1000)
    return {
        "ok": True,
        "summary": f"정상 ({latency_ms}ms)",
    }


def _ask_ollama(
    question: str,
    system_prompt: str | None = None,
    *,
    model: str | None = None,
    timeout_sec: int | None = None,
    max_tokens: int | None = None,
    temperature: float | None = None,
) -> str:
    prompt = (system_prompt or s.DEFAULT_SYSTEM_PROMPT).strip()
    actual_timeout = max(1, timeout_sec if timeout_sec is not None else s.OLLAMA_TIMEOUT_SEC)
    actual_temperature = s.OLLAMA_TEMPERATURE if temperature is None else temperature
    options: dict[str, int | float] = {
        "temperature": actual_temperature,
    }
    if max_tokens is not None and max_tokens > 0:
        options["num_predict"] = max_tokens
    payload = {
        "model": (model or s.OLLAMA_MODEL).strip(),
        "system": prompt,
        "prompt": question,
        "stream": False,
        "options": options,
    }
    req = request.Request(
        url=f"{s.OLLAMA_BASE_URL}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=actual_timeout) as response:
            body = response.read().decode("utf-8")
    except TimeoutError as exc:
        raise TimeoutError(f"Ollama API timed out after {actual_timeout}s") from exc
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Ollama API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        if "timed out" in str(exc.reason).lower():
            raise TimeoutError(f"Ollama API timed out after {actual_timeout}s") from exc
        raise RuntimeError(f"Ollama API connection failed: {exc.reason}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Ollama API returned invalid JSON") from exc

    return _sanitize_ollama_output(str(data.get("response", "")).strip())


def _ask_ollama_chat(
    question: str,
    system_prompt: str | None = None,
    *,
    model: str | None = None,
    timeout_sec: int | None = None,
    max_tokens: int | None = None,
    temperature: float | None = None,
    think: bool | None = None,
) -> str:
    prompt = (system_prompt or s.DEFAULT_SYSTEM_PROMPT).strip()
    actual_timeout = max(1, timeout_sec if timeout_sec is not None else s.OLLAMA_TIMEOUT_SEC)
    actual_temperature = s.OLLAMA_TEMPERATURE if temperature is None else temperature
    options: dict[str, int | float] = {
        "temperature": actual_temperature,
    }
    if max_tokens is not None and max_tokens > 0:
        options["num_predict"] = max_tokens
    payload: dict[str, object] = {
        "model": (model or s.OLLAMA_MODEL).strip(),
        "messages": [
            {"role": "system", "content": prompt},
            {"role": "user", "content": question},
        ],
        "stream": False,
        "options": options,
    }
    if think is not None:
        payload["think"] = think

    req = request.Request(
        url=f"{s.OLLAMA_BASE_URL}/api/chat",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=actual_timeout) as response:
            body = response.read().decode("utf-8")
    except TimeoutError as exc:
        raise TimeoutError(f"Ollama API timed out after {actual_timeout}s") from exc
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Ollama API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        if "timed out" in str(exc.reason).lower():
            raise TimeoutError(f"Ollama API timed out after {actual_timeout}s") from exc
        raise RuntimeError(f"Ollama API connection failed: {exc.reason}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Ollama API returned invalid JSON") from exc

    message = data.get("message")
    if not isinstance(message, dict):
        return ""
    return _sanitize_ollama_output(str(message.get("content", "")).strip())


def _check_ollama_health(
    timeout_sec: int | None = None,
    *,
    model: str | None = None,
) -> dict[str, str | bool]:
    actual_timeout = max(
        1,
        timeout_sec if timeout_sec is not None else s.OLLAMA_HEALTH_TIMEOUT_SEC,
    )
    started_at = time.monotonic()
    req = request.Request(
        url=f"{s.OLLAMA_BASE_URL}/api/tags",
        headers={"Content-Type": "application/json"},
        method="GET",
    )

    try:
        with request.urlopen(req, timeout=actual_timeout) as response:
            body = response.read().decode("utf-8")
    except TimeoutError:
        return {
            "ok": False,
            "summary": f"응답 없음 ({actual_timeout}초 초과)",
        }
    except error.HTTPError as exc:
        return {
            "ok": False,
            "summary": f"HTTP {exc.code}",
        }
    except error.URLError as exc:
        reason = str(exc.reason).strip() or "connection failed"
        if "timeout" in reason.lower() or "timed out" in reason.lower():
            return {
                "ok": False,
                "summary": f"응답 없음 ({actual_timeout}초 초과)",
            }
        return {
            "ok": False,
            "summary": f"연결 실패 ({reason})",
        }

    latency_ms = int((time.monotonic() - started_at) * 1000)
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        return {
            "ok": False,
            "summary": f"응답 오류 (invalid json, {latency_ms}ms)",
        }

    models = data.get("models")
    installed_names: list[str] = []
    if isinstance(models, list):
        installed_names = [
            str(item.get("name") or "").strip()
            for item in models
            if isinstance(item, dict) and str(item.get("name") or "").strip()
        ]

    configured_model = (model or s.OLLAMA_MODEL or "").strip()
    if configured_model and configured_model not in installed_names:
        return {
            "ok": False,
            "summary": f"서버 연결됨, 실행 모델 확인 필요 ({latency_ms}ms)",
        }

    return {
        "ok": True,
        "summary": f"정상 ({latency_ms}ms)",
    }
