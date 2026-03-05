import json
from urllib import error, request

from anthropic import Anthropic

from boxer.core import settings as s


def _ask_claude(client: Anthropic, question: str, system_prompt: str | None = None) -> str:
    prompt = (system_prompt or s.DEFAULT_SYSTEM_PROMPT).strip()
    result = client.messages.create(
        model=s.ANTHROPIC_MODEL,
        max_tokens=s.ANTHROPIC_MAX_TOKENS,
        system=prompt,
        messages=[{"role": "user", "content": question}],
    )
    text_blocks = [
        block.text
        for block in result.content
        if getattr(block, "type", "") == "text"
    ]
    return "".join(text_blocks).strip()


def _ask_ollama(question: str, system_prompt: str | None = None) -> str:
    prompt = (system_prompt or s.DEFAULT_SYSTEM_PROMPT).strip()
    payload = {
        "model": s.OLLAMA_MODEL,
        "system": prompt,
        "prompt": question,
        "stream": False,
        "options": {
            "temperature": s.OLLAMA_TEMPERATURE,
        },
    }
    req = request.Request(
        url=f"{s.OLLAMA_BASE_URL}/api/generate",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=s.OLLAMA_TIMEOUT_SEC) as response:
            body = response.read().decode("utf-8")
    except TimeoutError as exc:
        raise TimeoutError(f"Ollama API timed out after {s.OLLAMA_TIMEOUT_SEC}s") from exc
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="ignore")
        raise RuntimeError(f"Ollama API HTTP {exc.code}: {detail[:200]}") from exc
    except error.URLError as exc:
        if "timed out" in str(exc.reason).lower():
            raise TimeoutError(f"Ollama API timed out after {s.OLLAMA_TIMEOUT_SEC}s") from exc
        raise RuntimeError(f"Ollama API connection failed: {exc.reason}") from exc

    try:
        data = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Ollama API returned invalid JSON") from exc

    return str(data.get("response", "")).strip()
