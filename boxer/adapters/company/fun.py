import logging
import re
from typing import Any

from anthropic import Anthropic

from boxer.adapters.common.slack import MessagePayload, SlackMessageReplyFn
from boxer.company import settings as cs
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat, _check_ollama_health

ALLOWED_FUN_CHANNEL_ID = "C0621TL2HSB"
FUN_OLLAMA_MODEL = "qwen2.5:1.5b"
FUN_LLM_MAX_TOKENS = 48
FUN_LLM_TIMEOUT_SEC = 60
CLAUSE_SPLIT_RE = re.compile(r"[\n\r,.!?~]+")
MENTION_RE = re.compile(r"<@[^>]+>")
URL_RE = re.compile(r"https?://\S+")
EDGE_FILLER_RE = re.compile(r"^(또|진짜|완전|아니|근데|그럼|와|헐)\s+|\s+(또|진짜|완전|아니|근데|그럼|와|헐)$")
TRAILING_ENDING_RE = re.compile(r"(이네|이야|인가요|인가|인데|네요|네요|이냐|이군)$")
TRAILING_PARTICLE_RE = re.compile(r"(은|는|이|가|을|를|도|만|이나|나|랑|과|와|임|야|냐|네|군|지)$")
FUN_REPLY_SANITIZE_RE = re.compile(r"\s+")
FUN_SYSTEM_PROMPT = (
    "너는 슬랙 채널에서 DD를 유쾌하게 놀리는 한 줄 멘트 생성기야. "
    "사용자 문장에 들어 있는 '모대'의 맥락을 읽고, DD에게 짧고 장난스럽게 받아쳐. "
    "반말 한국어 한 문장만 출력해. "
    "욕설, 비하, 성적 표현, 개인정보, 과한 조롱은 금지. "
    "설명, 해설, 따옴표, 이모지, 줄바꿈, 멘션 표기는 금지. "
    "가능하면 '모대'를 살린 말장난으로 답해."
)


def _normalize_fun_text(text: str) -> str:
    normalized = MENTION_RE.sub(" ", text)
    normalized = URL_RE.sub(" ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _clean_fun_fragment(text: str) -> str:
    cleaned = text.strip(" \"'[]()")
    cleaned = EDGE_FILLER_RE.sub("", cleaned).strip()
    cleaned = TRAILING_ENDING_RE.sub("", cleaned).strip()
    cleaned = TRAILING_PARTICLE_RE.sub("", cleaned).strip()
    cleaned = EDGE_FILLER_RE.sub("", cleaned).strip()
    return cleaned


def _extract_fun_topic(text: str) -> str | None:
    normalized = _normalize_fun_text(text)
    if "모대" not in normalized:
        return None

    clauses = [segment.strip() for segment in CLAUSE_SPLIT_RE.split(normalized) if segment.strip()]
    clause = next((segment for segment in clauses if "모대" in segment), normalized)
    before, _, after = clause.partition("모대")
    before = _clean_fun_fragment(before)
    after = _clean_fun_fragment(after)

    topic = before or after
    if not topic:
        topic = _clean_fun_fragment(clause.replace("모대", " "))

    if not topic:
        return None

    words = topic.split()
    if len(words) > 4:
        topic = " ".join(words[-4:])
    if len(topic) > 24:
        topic = topic[-24:].strip()
    return topic or None
def _build_fun_llm_unavailable_reply(summary: str | None = None) -> str:
    base = "LLM 서버가 응답하지 않아 지금은 AI 답변을 생성할 수 없어"
    detail = (summary or "").strip()
    if not detail:
        return base
    return f"{base}\n• 상태: {detail}"


def _sanitize_fun_reply(text: str) -> str:
    cleaned = MENTION_RE.sub(" ", text or "")
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = FUN_REPLY_SANITIZE_RE.sub(" ", cleaned).strip(" \"'[]()")
    if len(cleaned) > 48:
        cleaned = cleaned[:48].rstrip()
    return cleaned


def _build_fun_llm_prompt(text: str) -> str:
    topic = _extract_fun_topic(text) or "없음"
    return (
        f"원문: {text.strip()}\n"
        f"추출 토픽: {topic}\n"
        "출력 규칙:\n"
        "- DD를 살짝 놀리는 톤\n"
        "- 한 문장만\n"
        "- 30자 안팎\n"
        "- '모대' 뉘앙스를 살릴 것"
    )


def _generate_fun_reply(
    text: str,
    logger: logging.Logger,
    *,
    claude_client: Anthropic | None,
) -> tuple[str, str, bool]:
    provider = (s.LLM_PROVIDER or "").lower().strip()
    prompt = _build_fun_llm_prompt(text)

    try:
        if provider == "ollama":
            health = _check_ollama_health(
                timeout_sec=min(s.OLLAMA_HEALTH_TIMEOUT_SEC, 2),
                model=FUN_OLLAMA_MODEL,
            )
            if not health["ok"]:
                logger.info("Fun reply unavailable: ollama (%s)", health["summary"])
                return _build_fun_llm_unavailable_reply(str(health["summary"])), "unavailable_ollama", False
            llm_text = _ask_ollama_chat(
                prompt,
                system_prompt=FUN_SYSTEM_PROMPT,
                model=FUN_OLLAMA_MODEL,
                timeout_sec=FUN_LLM_TIMEOUT_SEC,
                max_tokens=FUN_LLM_MAX_TOKENS,
                temperature=0.8,
                think=False,
            )
            sanitized = _sanitize_fun_reply(llm_text)
            if sanitized:
                return sanitized, f"ollama:{FUN_OLLAMA_MODEL}", True
            return _build_fun_llm_unavailable_reply("빈 응답"), "unavailable_empty", False

        if provider == "claude" and claude_client is not None:
            llm_text = _ask_claude(
                claude_client,
                prompt,
                system_prompt=FUN_SYSTEM_PROMPT,
                max_tokens=FUN_LLM_MAX_TOKENS,
            )
            sanitized = _sanitize_fun_reply(llm_text)
            if sanitized:
                return sanitized, "claude", True
            return _build_fun_llm_unavailable_reply("빈 응답"), "unavailable_empty", False
    except TimeoutError:
        logger.warning("Fun reply LLM timeout")
        return _build_fun_llm_unavailable_reply(f"응답 없음 ({FUN_LLM_TIMEOUT_SEC}초 초과)"), "unavailable_timeout", False
    except Exception:
        logger.exception("Fun reply LLM failed")
        return _build_fun_llm_unavailable_reply("호출 실패"), "unavailable_error", False

    if provider == "claude" and claude_client is None:
        return _build_fun_llm_unavailable_reply("claude 클라이언트 미설정"), "unavailable_claude", False
    return _build_fun_llm_unavailable_reply("LLM provider 미설정"), "unavailable_provider", False


def handle_fun_message(
    payload: MessagePayload,
    reply: SlackMessageReplyFn,
    _client: Any,
    logger: logging.Logger,
    *,
    claude_client: Anthropic | None = None,
) -> None:
    if payload["channel_id"] != ALLOWED_FUN_CHANNEL_ID:
        return

    raw_text = payload["raw_text"]
    if "모대" not in raw_text:
        return

    reply_text, reply_mode, mention_dd = _generate_fun_reply(
        raw_text,
        logger,
        claude_client=claude_client,
    )
    if mention_dd and cs.DD_USER_ID:
        reply(f"<@{cs.DD_USER_ID}> {reply_text}", thread=True)
    else:
        reply(reply_text, thread=True)

    logger.info(
        "Responded with fun trigger in channel=%s user=%s mode=%s",
        payload["channel_id"],
        payload["user_id"],
        reply_mode,
    )
