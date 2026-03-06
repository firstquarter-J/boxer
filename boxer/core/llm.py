import json
import time
from urllib import error, request

from anthropic import Anthropic

from boxer.core import settings as s


def _ask_claude(
    client: Anthropic,
    question: str,
    system_prompt: str | None = None,
    *,
    max_tokens: int | None = None,
) -> str:
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
    return "".join(text_blocks).strip()


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

    return str(data.get("response", "")).strip()


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
    return str(message.get("content", "")).strip()


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
