import logging
import re
from collections.abc import Callable
from typing import Any

from anthropic import Anthropic

from boxer_adapter_slack.common import (
    MessagePayload,
    SlackMessageReplyFn,
    _set_request_log_skip_persist,
)
from boxer_adapter_slack.context import _load_slack_thread_context
from boxer_company import settings as cs
from boxer_company.assistant.team_fun_route import (
    build_daily_fortune_reply as _build_daily_fortune_reply,
    is_daily_fortune_content,
)
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.team_chat_context import build_team_chat_context
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat, _check_ollama_health

ALLOWED_FUN_CHANNEL_ID = "C0621TL2HSB"
FUN_OLLAMA_MODEL = "qwen2.5:1.5b"
FUN_LLM_MAX_TOKENS = 48
FUN_LLM_TIMEOUT_SEC = 60
RemoteFunReplyGenerator = Callable[
    [str, str, str],
    tuple[str, str, bool],
]
RemoteFortuneReplyGenerator = Callable[
    [str, str, str],
    str | None,
]
CLAUSE_SPLIT_RE = re.compile(r"[\n\r,.!?~]+")
MENTION_RE = re.compile(r"<@[^>]+>")
URL_RE = re.compile(r"https?://\S+")
EDGE_FILLER_RE = re.compile(r"^(또|진짜|완전|아니|근데|그럼|와|헐)\s+|\s+(또|진짜|완전|아니|근데|그럼|와|헐)$")
TRAILING_ENDING_RE = re.compile(r"(이네|이야|인가요|인가|인데|네요|네요|이냐|이군)$")
TRAILING_PARTICLE_RE = re.compile(r"(은|는|이|가|을|를|도|만|이나|나|랑|과|와|임|야|냐|네|군|지)$")
FUN_REPLY_SANITIZE_RE = re.compile(r"\s+")
FUN_BAD_REPLY_RE = re.compile(
    r"(okay|let'?s|the user|i think|저는|나는|제가|설명|해설|안녕하세요|반갑|도와|죄송|미안|예시|출력 규칙)",
    re.IGNORECASE,
)
FUN_TEMPLATE_RULES: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (
        ("욕", "화내", "짜증", "분노"),
        (
            "말 좀 곱게 하지 모대?",
            "입이 너무 매운 거 모대?",
            "말로 좀 풀면 안 되모대?",
        ),
    ),
    (
        ("다이어트", "살빼", "식단", "헬스", "운동", "체중"),
        (
            "다이어트도 쉽지 모대?",
            "식단도 작심삼일 모대?",
            "살 빼는 게 말뿐 모대?",
        ),
    ),
    (
        ("연애", "썸", "소개팅", "고백", "플러팅", "뽀뽀"),
        (
            "연애도 쉽지 모대?",
            "썸도 뜻대로 안 되모대?",
            "마음대로 되는 게 모대?",
        ),
    ),
    (
        ("로그인", "로그아웃", "비번", "비밀번호", "아이디", "인증", "otp", "패스워드"),
        (
            "로그인도 버벅이지 모대?",
            "비번도 매번 헷갈리모대?",
            "인증도 한 번에 안 되모대?",
        ),
    ),
    (
        ("밥", "먹", "점심", "저녁", "야식", "치킨", "피자", "햄버거"),
        (
            "밥도 잘 먹지 모대?",
            "먹는 건 또 진심이모대?",
            "야식도 못 참지 모대?",
        ),
    ),
    (
        ("잠", "졸", "수면", "밤샘", "기절"),
        (
            "잠도 참기 힘들지 모대?",
            "눈꺼풀도 파업 모대?",
            "잠 앞에서는 장사 없모대?",
        ),
    ),
    (
        ("커피", "카페인", "아아", "라떼"),
        (
            "커피 없인 안 되모대?",
            "카페인도 생명수 모대?",
            "아아로 연명하모대?",
        ),
    ),
    (
        ("출근", "퇴근", "야근", "월급", "회의", "업무", "일", "보고"),
        (
            "일도 사람 뜻대로 안 되모대?",
            "회의도 끝이 없지 모대?",
            "출근부터 쉽지 않모대?",
        ),
    ),
    (
        ("배포", "버그", "에러", "장애", "코드", "리뷰", "리팩터링", "테스트", "커밋", "푸시"),
        (
            "배포도 한 번에 안 되모대?",
            "버그도 눈치 없이 뜨모대?",
            "코드도 말 안 듣지 모대?",
        ),
    ),
)
FUN_GENERIC_TEMPLATES: tuple[str, ...] = (
    "{topic_with_do} 쉽지 모대?",
    "{topic_with_do} 생각보다 빡세지 모대?",
    "{topic_with_do} 또 말처럼 되나 모대?",
    "{topic_with_do} 그냥 되는 줄 알았모대?",
)
FUN_SYSTEM_PROMPT = (
    "너는 슬랙에서 DD를 향한 가벼운 한방 드립을 짧게 다듬는 한국어 답글 보정기야. "
    "기본 템플릿을 더 자연스럽고 유쾌하게 다듬되, 최근 맥락과 인물 성향을 참고해. "
    "출력 규칙: "
    "1) 반드시 한국어 한 문장만 출력. "
    "2) 마지막은 반드시 '모대?'로 끝낼 것. "
    "3) 길이 8~22자 정도. "
    "4) 영어, 설명, 해설, 자기소개, 따옴표, 이모지, 멘션 금지. "
    "5) 욕설, 비하, 성적 표현, 외모 조롱, 따돌림, 집요한 모욕 금지. "
    "6) 가볍게 치고 빠지는 수준으로만 놀릴 것. "
    "7) 기본 템플릿보다 이상하면 기본 템플릿 그대로 출력."
)


def _normalize_fun_text(text: str) -> str:
    normalized = MENTION_RE.sub(" ", text)
    normalized = URL_RE.sub(" ", normalized)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _load_thread_root_text(
    client: Any,
    logger: logging.Logger,
    channel_id: str,
    thread_ts: str,
) -> str:
    if not channel_id or not thread_ts:
        return ""
    try:
        response = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=1,
            inclusive=True,
        )
    except Exception:
        logger.exception("Failed to fetch fortune thread root")
        return ""

    messages = response.get("messages") or []
    if not isinstance(messages, list) or not messages:
        return ""
    first_message = messages[0]
    if not isinstance(first_message, dict):
        return ""
    return str(first_message.get("text") or "").strip()


def _is_daily_fortune_message(
    payload: MessagePayload,
    thread_root_text: str,
) -> bool:
    if payload["channel_id"] != ALLOWED_FUN_CHANNEL_ID:
        return False
    if payload.get("subtype") != "bot_message":
        return False

    if payload.get("thread_ts") == payload.get("current_ts"):
        return False

    # local rollback에서도 의미 판정은 company domain helper 하나만 쓴다.
    return is_daily_fortune_content(payload["raw_text"], thread_root_text)


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


def _ensure_topic_suffix(topic: str, suffix: str = "도") -> str:
    cleaned = topic.strip()
    if not cleaned:
        return ""
    if cleaned.endswith(suffix):
        return cleaned
    return f"{cleaned}{suffix}"


def _pick_fun_template(seed_text: str, templates: tuple[str, ...]) -> str:
    if not templates:
        return ""
    index = sum(ord(char) for char in seed_text) % len(templates)
    return templates[index]


def _build_fun_template(text: str) -> str:
    topic = _extract_fun_topic(text) or "그거"
    compact_topic = topic.replace(" ", "")
    for keywords, templates in FUN_TEMPLATE_RULES:
        if any(keyword in compact_topic for keyword in keywords):
            return _pick_fun_template(compact_topic, templates)

    topic_with_do = _ensure_topic_suffix(topic, "도")
    if not topic_with_do:
        return "그거도 쉽지 모대?"
    template = _pick_fun_template(compact_topic or topic_with_do, FUN_GENERIC_TEMPLATES)
    return template.format(topic_with_do=topic_with_do)


def _sanitize_fun_reply(text: str) -> str:
    cleaned = MENTION_RE.sub(" ", text or "")
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = FUN_REPLY_SANITIZE_RE.sub(" ", cleaned).strip(" \"'[]()")
    if len(cleaned) > 48:
        cleaned = cleaned[:48].rstrip()
    return cleaned


def _finalize_fun_reply(source_text: str, generated_text: str, fallback_text: str) -> str:
    cleaned = _sanitize_fun_reply(generated_text)
    if not cleaned:
        return fallback_text
    if FUN_BAD_REPLY_RE.search(cleaned):
        return fallback_text

    cleaned = re.sub(r"^.*(?:->|=>|:)\s*", "", cleaned).strip()
    cleaned = re.split(r"(?:,|\.|!|;|:| 그런데 | 근데 | 하지만 | 그래서 )", cleaned, maxsplit=1)[0].strip()
    cleaned = cleaned.replace("모대", " ").replace("?", " ").strip()
    cleaned = cleaned.rstrip("!~. ")

    topic = _extract_fun_topic(source_text) or ""
    compact_topic = topic.replace(" ", "")
    compact_cleaned = cleaned.replace(" ", "")
    fallback_compact = fallback_text.replace(" ", "")
    should_prefix_topic = bool(topic) and compact_topic in fallback_compact
    if should_prefix_topic and compact_topic not in compact_cleaned:
        cleaned = f"{topic} {cleaned}".strip()

    cleaned = FUN_REPLY_SANITIZE_RE.sub(" ", cleaned).strip()
    if len(cleaned) < 2 or len(cleaned) > 18:
        return fallback_text
    return f"{cleaned} 모대?"


def _build_fun_llm_prompt(
    text: str,
    thread_context: str = "",
    *,
    speaker_user_id: str = "",
) -> str:
    topic = _extract_fun_topic(text) or "없음"
    template = _build_fun_template(text)
    team_context = build_team_chat_context(
        text,
        thread_context,
        speaker_user_id=speaker_user_id,
        required_names=("DD",),
    )
    context_block = ""
    if thread_context:
        context_block = f"최근 대화 맥락:\n{thread_context}\n\n"
    return (
        f"{context_block}"
        f"{team_context}\n\n"
        f"원문: {text.strip()}\n"
        f"추출 토픽: {topic}\n"
        f"기본 템플릿: {template}\n"
        "출력 규칙:\n"
        "- DD를 살짝 놀리는 톤\n"
        "- 최근 맥락이 있으면 그걸 재료로 짧게 받아칠 것\n"
        "- 기본 템플릿 의미 유지\n"
        "- 끝은 반드시 모대?\n"
        "- 영어/설명/자기소개 금지\n"
        "출력:"
    )


def _generate_fun_reply(
    text: str,
    logger: logging.Logger,
    *,
    claude_client: Anthropic | None,
    thread_context: str = "",
    speaker_user_id: str = "",
) -> tuple[str, str, bool]:
    provider = (s.LLM_PROVIDER or "").lower().strip()
    fallback_text = _build_fun_template(text)
    prompt = _build_fun_llm_prompt(
        text,
        thread_context,
        speaker_user_id=speaker_user_id,
    )

    try:
        if provider == "ollama":
            health = _check_ollama_health(
                timeout_sec=min(s.OLLAMA_HEALTH_TIMEOUT_SEC, 2),
                model=FUN_OLLAMA_MODEL,
            )
            if not health["ok"]:
                logger.info("Fun reply fallback template: ollama unavailable (%s)", health["summary"])
                return fallback_text, "template_unavailable_ollama", True
            llm_text = _ask_ollama_chat(
                prompt,
                system_prompt=FUN_SYSTEM_PROMPT,
                model=FUN_OLLAMA_MODEL,
                timeout_sec=FUN_LLM_TIMEOUT_SEC,
                max_tokens=FUN_LLM_MAX_TOKENS,
                temperature=0.5,
                think=False,
            )
            finalized = _finalize_fun_reply(text, llm_text, fallback_text)
            if finalized == fallback_text:
                return finalized, f"template_after_ollama:{FUN_OLLAMA_MODEL}", True
            return finalized, f"ollama:{FUN_OLLAMA_MODEL}", True

        if provider == "claude" and claude_client is not None:
            llm_text = _ask_claude(
                claude_client,
                prompt,
                system_prompt=FUN_SYSTEM_PROMPT,
                max_tokens=FUN_LLM_MAX_TOKENS,
            )
            finalized = _finalize_fun_reply(text, llm_text, fallback_text)
            if finalized == fallback_text:
                return finalized, "template_after_claude", True
            return finalized, "claude", True
    except TimeoutError:
        logger.warning("Fun reply LLM timeout")
        return fallback_text, "template_timeout", True
    except Exception:
        logger.exception("Fun reply LLM failed")
        return fallback_text, "template_error", True

    if provider == "claude" and claude_client is None:
        return fallback_text, "template_unavailable_claude", True
    return fallback_text, "template_no_provider", True


def _is_dd_active(client: Any, logger: logging.Logger) -> bool:
    if not cs.DD_USER_ID:
        return False
    try:
        response = client.users_getPresence(user=cs.DD_USER_ID)
    except Exception:
        logger.exception("Failed to fetch DD presence")
        return False

    presence = str(response.get("presence") or "").strip().lower()
    logger.info("DD presence=%s", presence or "unknown")
    return presence == "active"


def is_human_fun_trigger(payload: MessagePayload) -> bool:
    """일반 사람 메시지 중 실제 fun 응답 후보만 기본 사용 권한 검사 대상으로 삼는다."""

    return (
        payload.get("channel_id") == ALLOWED_FUN_CHANNEL_ID
        and payload.get("subtype") != "bot_message"
        and "모대" in str(payload.get("raw_text") or "")
    )


def handle_fun_message(
    payload: MessagePayload,
    reply: SlackMessageReplyFn,
    client: Any,
    logger: logging.Logger,
    *,
    claude_client: Anthropic | None = None,
    remote_reply_generator: RemoteFunReplyGenerator | None = None,
    remote_fortune_reply_generator: RemoteFortuneReplyGenerator | None = None,
) -> None:
    if payload["channel_id"] != ALLOWED_FUN_CHANNEL_ID:
        return

    raw_text = payload["raw_text"]
    is_threaded_bot_message = (
        payload.get("subtype") == "bot_message"
        and payload.get("thread_ts") != payload.get("current_ts")
    )
    thread_root_text = ""
    if is_threaded_bot_message:
        thread_root_text = _load_thread_root_text(
            client,
            logger,
            payload["channel_id"],
            payload.get("thread_ts") or "",
        )

    if is_threaded_bot_message and remote_fortune_reply_generator is not None:
        _set_request_log_skip_persist(payload, True)
        try:
            # Slack은 bot event와 thread root만 전달하고 의미 판정·답변
            # 조립은 API 한 번으로 끝낸다. 실패 시 local parser로 되돌아가지 않는다.
            fortune_reply = remote_fortune_reply_generator(
                raw_text,
                thread_root_text,
                str(
                    payload.get("user_id")
                    or payload.get("bot_user_id")
                    or payload.get("bot_id")
                    or payload.get("app_id")
                    or ""
                ).strip(),
            )
        except Exception as exc:
            logger.warning(
                "Company API fortune reply failed channel=%s bot=%s "
                "error_type=%s",
                payload["channel_id"],
                payload.get("bot_name") or payload.get("bot_id") or "unknown",
                type(exc).__name__,
            )
            return
        if fortune_reply is None:
            # API가 운세가 아니라고 판정한 bot reply에는 transport가 응답하지 않는다.
            return
        reply(fortune_reply, thread=True)
        logger.info(
            "Responded with API daily fortune analysis in channel=%s bot=%s",
            payload["channel_id"],
            payload.get("bot_name") or payload.get("bot_id") or "unknown",
        )
        return

    if _is_daily_fortune_message(payload, thread_root_text):
        _set_request_log_skip_persist(payload, True)
        reply(_build_daily_fortune_reply(raw_text, thread_root_text), thread=True)
        logger.info(
            "Responded with daily fortune analysis in channel=%s bot=%s",
            payload["channel_id"],
            payload.get("bot_name") or payload.get("bot_id") or "unknown",
        )
        return

    if payload.get("subtype") == "bot_message":
        return

    if not is_human_fun_trigger(payload):
        return

    thread_context = _load_slack_thread_context(
        client,
        logger,
        payload["channel_id"],
        payload.get("thread_ts") or payload.get("current_ts"),
        payload.get("current_ts"),
    )

    if is_prompt_exfiltration_attempt(raw_text, thread_context):
        _set_request_log_skip_persist(payload, True)
        reply(build_prompt_security_refusal(), thread=True)
        logger.warning(
            "Blocked fun prompt exfiltration attempt in channel=%s user=%s",
            payload["channel_id"],
            payload.get("user_id") or "unknown",
        )
        return

    _set_request_log_skip_persist(payload, True)
    speaker_user_id = str(payload.get("user_id") or "").strip()
    if remote_reply_generator is not None:
        try:
            # remote cutover에서는 provider 호출을 callback 한 번으로 고정하고,
            # 실패해도 Slack-local Claude/Ollama로 되돌아가지 않는다.
            reply_text, reply_mode, mention_dd = remote_reply_generator(
                raw_text,
                thread_context,
                speaker_user_id,
            )
        except Exception as exc:
            logger.warning(
                "Company API fun reply failed channel=%s user=%s error_type=%s",
                payload["channel_id"],
                speaker_user_id or "unknown",
                type(exc).__name__,
            )
            # remote 장애는 콘텐츠를 Slack에서 다시 생성하지 않고 고정된
            # transport 안내만 렌더링해 provider 소유권을 API에 남긴다.
            reply_text = "지금은 모대 답변을 만들 수 없어."
            reply_mode = "company_api_error"
            mention_dd = False
    else:
        reply_text, reply_mode, mention_dd = _generate_fun_reply(
            raw_text,
            logger,
            claude_client=claude_client,
            thread_context=thread_context,
            speaker_user_id=speaker_user_id,
        )
    if mention_dd and cs.DD_USER_ID and _is_dd_active(client, logger):
        reply(f"<@{cs.DD_USER_ID}> {reply_text}", thread=True)
    elif mention_dd and cs.DD_USER_ID:
        reply("디디가 오프라인이라 대답하지 않습니다.", thread=True)
    else:
        reply(reply_text, thread=True)

    logger.info(
        "Responded with fun trigger in channel=%s user=%s mode=%s",
        payload["channel_id"],
        payload["user_id"],
        reply_mode,
    )
