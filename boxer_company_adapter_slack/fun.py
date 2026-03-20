import logging
import re
from typing import Any

from anthropic import Anthropic

from boxer_adapter_slack.common import (
    MessagePayload,
    SlackMessageReplyFn,
    _set_request_log_skip_persist,
)
from boxer.company import settings as cs
from boxer.company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer.company.team_chat_context import build_team_chat_context
from boxer.core import settings as s
from boxer.core.llm import _ask_claude, _ask_ollama_chat, _check_ollama_health
from boxer.core.thread_context import _load_thread_context

ALLOWED_FUN_CHANNEL_ID = "C0621TL2HSB"
FUN_OLLAMA_MODEL = "qwen2.5:1.5b"
FUN_LLM_MAX_TOKENS = 48
FUN_LLM_TIMEOUT_SEC = 60
CLAUSE_SPLIT_RE = re.compile(r"[\n\r,.!?~]+")
MENTION_RE = re.compile(r"<@[^>]+>")
URL_RE = re.compile(r"https?://\S+")
FORTUNE_DATE_RE = re.compile(r"(?P<year>20\d{2})년\s*(?P<month>\d{1,2})월\s*(?P<day>\d{1,2})일")
FORTUNE_BIRTH_YEAR_RE = re.compile(r"(?<!\d)((?:19|20)?\d{2})년생(?!\d)")
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
FORTUNE_BOT_NAME_HINTS = ("ddalggak", "ddal ggak", "딸깍")
FORTUNE_REQUIRED_MARKERS = ("오늘의 운세",)
FORTUNE_THEME_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "행운",
        ("행운", "lucky", "luck", "clover", "four_leaf_clover", "반짝", "빛나", "sparkles", "순조", "좋은 소식", "해결", "기회"),
    ),
    (
        "응원",
        ("화이팅", "파이팅", "힘내", "응원", "오늘 하루", "fighting"),
    ),
    (
        "사랑",
        ("사랑", "연애", "썸", "love_letter", "heart", "인연", "대인관계", "고백", "데이트"),
    ),
    (
        "일",
        ("업무", "회의", "프로젝트", "출근", "퇴근", "일복", "직장", "성과", "계획", "집중", "공부"),
    ),
    (
        "돈",
        ("재물", "금전", "보너스", "수익", "용돈", "지출", "과소비", "소비", "투자", "수입"),
    ),
    (
        "건강",
        ("건강", "컨디션", "휴식", "수면", "회복", "쉬어", "피로", "몸관리", "면역", "무리"),
    ),
    (
        "주의",
        ("주의", "조심", "신중", "천천히", "무리", "참아", "말실수", "실수", "충동", "서두르"),
    ),
    (
        "행동",
        ("도전", "시작", "실행", "움직", "추진", "연락", "정리", "결단", "시도", "먼저"),
    ),
)
FORTUNE_THEME_PRIORITY = {
    label: index for index, (label, _) in enumerate(FORTUNE_THEME_RULES)
}
FORTUNE_EVIDENCE_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("반짝반짝", ("반짝반짝",)),
    ("빛나는 날", ("빛나는 날",)),
    ("행운", ("행운", "lucky", "luck")),
    ("클로버", ("네잎클로버", "클로버", "four_leaf_clover", "clover")),
    ("화이팅", ("화이팅", "파이팅", "fighting")),
    ("사랑", ("사랑", "love_letter", "heart")),
    ("업무", ("업무", "회의", "프로젝트", "출근", "퇴근", "일복")),
    ("돈", ("재물", "금전", "보너스", "수익", "용돈")),
    ("건강", ("건강", "컨디션", "휴식", "수면", "회복", "쉬어")),
    ("조심", ("주의", "조심", "신중", "천천히", "무리", "참아")),
    ("도전", ("도전", "시작", "실행", "움직", "추진")),
    ("연락", ("연락", "대화", "메시지")),
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


def _normalize_fortune_text(text: str) -> str:
    return _normalize_fun_text(text).lower()


def _count_marker_hits(text: str, markers: tuple[str, ...]) -> int:
    return sum(1 for marker in markers if marker.lower() in text)


def _extract_fortune_date(text: str) -> str | None:
    match = FORTUNE_DATE_RE.search(text)
    if not match:
        return None
    year = int(match.group("year"))
    month = int(match.group("month"))
    day = int(match.group("day"))
    return f"{year}년 {month}월 {day}일"


def _extract_fortune_birth_years(text: str) -> list[str]:
    seen: set[str] = set()
    years: list[str] = []
    for matched_year in FORTUNE_BIRTH_YEAR_RE.findall(text or ""):
        year = str(matched_year).strip()
        if not year:
            continue
        label = f"{year}년생"
        if label in seen:
            continue
        seen.add(label)
        years.append(label)
    return years


def _score_fortune_themes(normalized_text: str) -> dict[str, int]:
    scores: dict[str, int] = {}
    for label, markers in FORTUNE_THEME_RULES:
        hit_count = _count_marker_hits(normalized_text, markers)
        if hit_count > 0:
            scores[label] = hit_count
    return scores


def _pick_top_fortune_themes(scores: dict[str, int], *, limit: int = 2) -> list[str]:
    ranked = sorted(
        scores.items(),
        key=lambda item: (
            -item[1],
            FORTUNE_THEME_PRIORITY.get(item[0], len(FORTUNE_THEME_PRIORITY)),
        ),
    )
    return [label for label, _ in ranked[:limit]]


def _classify_fortune_tone(scores: dict[str, int]) -> str:
    luck_score = scores.get("행운", 0)
    cheer_score = scores.get("응원", 0)
    caution_score = scores.get("주의", 0)
    action_score = scores.get("행동", 0)
    health_score = scores.get("건강", 0)
    love_score = scores.get("사랑", 0)

    if caution_score and (luck_score or cheer_score or action_score):
        return "낙관+주의 혼합형"
    if caution_score:
        return "주의형"
    if action_score and (luck_score or cheer_score):
        return "행동 촉구형"
    if luck_score + cheer_score >= 2:
        return "초긍정 응원형"
    if health_score:
        return "회복형"
    if love_score:
        return "감성형"
    return "잔잔한 일반형"


def _extract_fortune_evidence(normalized_text: str, *, limit: int = 4) -> list[str]:
    evidence: list[str] = []
    for label, markers in FORTUNE_EVIDENCE_RULES:
        if any(marker.lower() in normalized_text for marker in markers):
            evidence.append(label)
        if len(evidence) >= limit:
            break
    return evidence


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


def _looks_like_fortune_detail_text(text: str) -> bool:
    normalized = _normalize_fortune_text(text)
    if _extract_fortune_birth_years(text):
        return True

    detail_markers = (
        "행운",
        "재물",
        "금전",
        "연애",
        "대인관계",
        "직장",
        "건강",
        "주의",
        "조심",
        "기회",
        "연락",
        "지출",
        "계획",
        "컨디션",
    )
    return _count_marker_hits(normalized, detail_markers) >= 2


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

    bot_name = str(payload.get("bot_name") or "").strip().lower()
    known_bot = any(hint in bot_name for hint in FORTUNE_BOT_NAME_HINTS)
    if not known_bot and not _looks_like_fortune_detail_text(payload["raw_text"]):
        return False

    normalized_thread = _normalize_fortune_text(thread_root_text)
    if not all(marker.lower() in normalized_thread for marker in FORTUNE_REQUIRED_MARKERS):
        return False
    return _looks_like_fortune_detail_text(payload["raw_text"])


def _build_fortune_target_text(years: list[str]) -> str:
    if not years:
        return "이 댓글 기준으론"
    if len(years) == 1:
        return f"{years[0]} 기준으론"
    if len(years) == 2:
        return f"{years[0]}, {years[1]} 기준으론"
    return f"{years[0]} 외 {len(years) - 1}개 년생 기준으론"


def _build_daily_fortune_reply(text: str, thread_root_text: str = "") -> str:
    normalized = _normalize_fortune_text(text)
    date_text = _extract_fortune_date(text) or _extract_fortune_date(thread_root_text) or "오늘"
    years = _extract_fortune_birth_years(text)
    theme_scores = _score_fortune_themes(normalized)
    tone = _classify_fortune_tone(theme_scores)
    top_themes = _pick_top_fortune_themes(theme_scores)
    evidence = _extract_fortune_evidence(normalized)
    target_text = _build_fortune_target_text(years)

    intro = f"운세 분석: {date_text} {target_text} {tone}이야."
    if top_themes:
        theme_text = ", ".join(top_themes)
        if "주의" in top_themes and len(top_themes) > 1:
            middle = f"핵심은 {theme_text} 쪽이고 낙관이랑 경계를 같이 주네."
        else:
            middle = f"핵심은 {theme_text} 쪽이네."
    else:
        middle = "구체 키워드는 적지만 방향성은 보이네."

    if evidence:
        evidence_text = ", ".join(f"`{item}`" for item in evidence)
        ending = f"근거는 {evidence_text}."
    else:
        ending = "근거는 응원성 표현이 반복되는 점이야."

    return f"{intro} {middle} {ending}"


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


def handle_fun_message(
    payload: MessagePayload,
    reply: SlackMessageReplyFn,
    client: Any,
    logger: logging.Logger,
    *,
    claude_client: Anthropic | None = None,
) -> None:
    if payload["channel_id"] != ALLOWED_FUN_CHANNEL_ID:
        return

    raw_text = payload["raw_text"]
    thread_root_text = ""
    if payload.get("subtype") == "bot_message" and payload.get("thread_ts") != payload.get("current_ts"):
        thread_root_text = _load_thread_root_text(
            client,
            logger,
            payload["channel_id"],
            payload.get("thread_ts") or "",
        )

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

    if "모대" not in raw_text:
        return

    thread_context = _load_thread_context(
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
    reply_text, reply_mode, mention_dd = _generate_fun_reply(
        raw_text,
        logger,
        claude_client=claude_client,
        thread_context=thread_context,
        speaker_user_id=str(payload.get("user_id") or "").strip(),
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
