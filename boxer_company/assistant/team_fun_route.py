from __future__ import annotations

from collections.abc import Callable
import logging
import re

from boxer.context.windowing import _render_context_text
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.assistant.freeform_route import FreeformAnswerer
from boxer_company.prompt_security import (
    build_prompt_security_refusal,
    is_prompt_exfiltration_attempt,
)
from boxer_company.team_chat_context import build_team_chat_context


LlmHealthProbe = Callable[[], bool | None]
TEAM_FUN_OLLAMA_MODEL = "qwen2.5:1.5b"
TEAM_FUN_LLM_MAX_TOKENS = 48
TEAM_FUN_LLM_TIMEOUT_SEC = 60

_MENTION_RE = re.compile(r"<@[^>]+>")
_URL_RE = re.compile(r"https?://\S+")
_WHITESPACE_RE = re.compile(r"\s+")
_CLAUSE_SPLIT_RE = re.compile(r"[\n\r,.!?~]+")
_EDGE_FILLER_RE = re.compile(
    r"^(또|진짜|완전|아니|근데|그럼|와|헐)\s+|"
    r"\s+(또|진짜|완전|아니|근데|그럼|와|헐)$"
)
_TRAILING_ENDING_RE = re.compile(
    r"(이네|이야|인가요|인가|인데|네요|네요|이냐|이군)$"
)
_TRAILING_PARTICLE_RE = re.compile(
    r"(은|는|이|가|을|를|도|만|이나|나|랑|과|와|임|야|냐|네|군|지)$"
)
_BAD_REPLY_RE = re.compile(
    r"(okay|let'?s|the user|i think|저는|나는|제가|설명|해설|"
    r"안녕하세요|반갑|도와|죄송|미안|예시|출력 규칙)",
    re.IGNORECASE,
)
_FUN_TEMPLATE_RULES: tuple[
    tuple[tuple[str, ...], tuple[str, ...]], ...
] = (
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
        (
            "로그인",
            "로그아웃",
            "비번",
            "비밀번호",
            "아이디",
            "인증",
            "otp",
            "패스워드",
        ),
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
        (
            "배포",
            "버그",
            "에러",
            "장애",
            "코드",
            "리뷰",
            "리팩터링",
            "테스트",
            "커밋",
            "푸시",
        ),
        (
            "배포도 한 번에 안 되모대?",
            "버그도 눈치 없이 뜨모대?",
            "코드도 말 안 듣지 모대?",
        ),
    ),
)
_FUN_GENERIC_TEMPLATES: tuple[str, ...] = (
    "{topic_with_do} 쉽지 모대?",
    "{topic_with_do} 생각보다 빡세지 모대?",
    "{topic_with_do} 또 말처럼 되나 모대?",
    "{topic_with_do} 그냥 되는 줄 알았모대?",
)
_TEAM_FUN_SYSTEM_PROMPT = (
    "너는 슬랙에서 DD를 향한 가벼운 한방 드립을 짧게 다듬는 "
    "한국어 답글 보정기야. 기본 템플릿을 더 자연스럽고 유쾌하게 "
    "다듬되, 최근 맥락과 인물 성향을 참고해. 출력 규칙: "
    "1) 반드시 한국어 한 문장만 출력. "
    "2) 마지막은 반드시 '모대?'로 끝낼 것. "
    "3) 길이 8~22자 정도. "
    "4) 영어, 설명, 해설, 자기소개, 따옴표, 이모지, 멘션 금지. "
    "5) 욕설, 비하, 성적 표현, 외모 조롱, 따돌림, 집요한 모욕 금지. "
    "6) 가볍게 치고 빠지는 수준으로만 놀릴 것. "
    "7) 기본 템플릿보다 이상하면 기본 템플릿 그대로 출력."
)
_FORTUNE_DATE_RE = re.compile(
    r"(?P<year>20\d{2})년\s*(?P<month>\d{1,2})월\s*(?P<day>\d{1,2})일"
)
_FORTUNE_BIRTH_YEAR_RE = re.compile(
    r"(?<!\d)((?:19|20)?\d{2})년생(?!\d)"
)
_FORTUNE_REQUIRED_MARKERS = ("오늘의 운세",)
_FORTUNE_DETAIL_MARKERS = (
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
_FORTUNE_THEME_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "행운",
        (
            "행운",
            "lucky",
            "luck",
            "clover",
            "four_leaf_clover",
            "반짝",
            "빛나",
            "sparkles",
            "순조",
            "좋은 소식",
            "해결",
            "기회",
        ),
    ),
    (
        "응원",
        ("화이팅", "파이팅", "힘내", "응원", "오늘 하루", "fighting"),
    ),
    (
        "사랑",
        (
            "사랑",
            "연애",
            "썸",
            "love_letter",
            "heart",
            "인연",
            "대인관계",
            "고백",
            "데이트",
        ),
    ),
    (
        "일",
        (
            "업무",
            "회의",
            "프로젝트",
            "출근",
            "퇴근",
            "일복",
            "직장",
            "성과",
            "계획",
            "집중",
            "공부",
        ),
    ),
    (
        "돈",
        (
            "재물",
            "금전",
            "보너스",
            "수익",
            "용돈",
            "지출",
            "과소비",
            "소비",
            "투자",
            "수입",
        ),
    ),
    (
        "건강",
        (
            "건강",
            "컨디션",
            "휴식",
            "수면",
            "회복",
            "쉬어",
            "피로",
            "몸관리",
            "면역",
            "무리",
        ),
    ),
    (
        "주의",
        (
            "주의",
            "조심",
            "신중",
            "천천히",
            "무리",
            "참아",
            "말실수",
            "실수",
            "충동",
            "서두르",
        ),
    ),
    (
        "행동",
        (
            "도전",
            "시작",
            "실행",
            "움직",
            "추진",
            "연락",
            "정리",
            "결단",
            "시도",
            "먼저",
        ),
    ),
)
_FORTUNE_THEME_PRIORITY = {
    label: index for index, (label, _) in enumerate(_FORTUNE_THEME_RULES)
}
_FORTUNE_EVIDENCE_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
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


def match_company_llm_health_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """명시적인 health stage의 ping만 provider probe로 보낸다."""

    if str(request.metadata.get("route_group") or "").strip() != "health":
        return None
    if "ping" not in str(request.question or "").strip().lower():
        return None
    return "company_llm_health"


def match_company_team_fun_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """adapter가 고른 fun stage에서도 기존 사람 trigger 표식을 재검사한다."""

    if str(request.metadata.get("route_group") or "").strip() != "fun":
        return None
    if "모대" not in str(request.question or ""):
        return None
    return "company_team_fun"


def match_company_daily_fortune_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """thread root와 bot 본문을 함께 재검사해 운세 분석만 선택한다."""

    if str(request.metadata.get("route_group") or "").strip() != "fun":
        return None
    thread_root_text = _daily_fortune_thread_root(request)
    if not is_daily_fortune_content(request.question, thread_root_text):
        return None
    return "company_daily_fortune"


class CompanyLlmHealthAssistantRoute:
    """실제 provider health probe를 API 프로세스 안에서만 실행한다."""

    name = "company_llm_health"

    def __init__(
        self,
        probe: LlmHealthProbe,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._probe = probe
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_company_llm_health_route(request) is None:
            return None
        try:
            health = self._probe()
        except Exception as exc:
            # provider 오류에는 credential이 섞일 수 있어 타입만 기록한다.
            self._logger.warning(
                "Company LLM health probe failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            health = False
        body = (
            "available"
            if health is True
            else "unavailable"
            if health is False
            else "unconfigured"
        )
        return CompanyAssistantResult(
            route=self.name,
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=body,
                    mention_actor=False,
                ),
            ),
        )


class CompanyDailyFortuneAssistantRoute:
    """Slack과 무관한 운세 분류·문장 조립을 API domain route가 소유한다."""

    name = "company_daily_fortune"

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_company_daily_fortune_route(request) is None:
            return None
        return CompanyAssistantResult(
            route=self.name,
            outcome="answered",
            messages=(
                AssistantMessage(
                    body=build_daily_fortune_reply(
                        request.question,
                        _daily_fortune_thread_root(request),
                    ),
                    mention_actor=False,
                ),
            ),
        )


class CompanyTeamFunAssistantRoute:
    """Slack에 종속되지 않은 bounded context로 팀 fun 한 문장을 생성한다."""

    name = "company_team_fun"

    def __init__(
        self,
        answerer: FreeformAnswerer,
        *,
        provider_ready: Callable[[], bool],
        context_max_chars: int,
        logger: logging.Logger | None = None,
    ) -> None:
        self._answerer = answerer
        self._provider_ready = provider_ready
        self._context_max_chars = max(1, context_max_chars)
        self._logger = logger or logging.getLogger(__name__)

    def handle(
        self,
        request: CompanyAssistantRequest,
    ) -> CompanyAssistantResult | None:
        if match_company_team_fun_route(request) is None:
            return None

        typed_fun_context = request.metadata.get("team_fun_context")
        context_text = (
            typed_fun_context[-self._context_max_chars :]
            if isinstance(typed_fun_context, str)
            else _render_context_text(
                list(request.context_entries),
                max_chars=self._context_max_chars,
            )
        )
        if is_prompt_exfiltration_attempt(request.question, context_text):
            return CompanyAssistantResult(
                route=self.name,
                outcome="denied",
                messages=(AssistantMessage(body=build_prompt_security_refusal()),),
                fallback_reason="prompt_security",
            )
        fallback = build_team_fun_template(request.question)
        try:
            provider_ready = self._provider_ready()
        except Exception as exc:
            self._logger.warning(
                "Company team fun provider check failed request_id=%s "
                "error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            provider_ready = False
        if not provider_ready:
            return CompanyAssistantResult(
                route=self.name,
                outcome="answered",
                messages=(AssistantMessage(body=fallback),),
                fallback_reason="provider_unavailable",
            )

        team_context = build_team_chat_context(
            request.question,
            context_text,
            speaker_user_id=str(request.actor_id or ""),
            required_names=("DD",),
        )
        prompt = build_team_fun_prompt(
            request.question,
            context_text,
            team_context,
        )
        try:
            generated = self._answerer(
                prompt,
                "",
                _TEAM_FUN_SYSTEM_PROMPT,
            )
        except TimeoutError:
            return CompanyAssistantResult(
                route=self.name,
                outcome="answered",
                messages=(AssistantMessage(body=fallback),),
                fallback_reason="timeout",
            )
        except Exception as exc:
            self._logger.warning(
                "Company team fun answer failed request_id=%s error_type=%s",
                request.request_id,
                type(exc).__name__,
            )
            return CompanyAssistantResult(
                route=self.name,
                outcome="answered",
                messages=(AssistantMessage(body=fallback),),
                fallback_reason="provider_error",
            )

        answer = finalize_team_fun_reply(
            request.question,
            generated,
            fallback,
        )
        return CompanyAssistantResult(
            route=self.name,
            outcome="answered",
            messages=(AssistantMessage(body=answer),),
            used_llm=True,
        )


def build_team_fun_prompt(
    question: str,
    context_text: str,
    team_context: str,
) -> str:
    topic = extract_team_fun_topic(question) or "없음"
    fallback = build_team_fun_template(question)
    context_block = (
        f"최근 대화 맥락:\n{context_text}\n\n"
        if context_text
        else ""
    )
    return (
        f"{context_block}{team_context}\n\n"
        f"원문: {str(question or '').strip()}\n"
        f"추출 토픽: {topic}\n"
        f"기본 템플릿: {fallback}\n"
        "출력 규칙:\n"
        "- DD를 살짝 놀리는 톤\n"
        "- 최근 맥락이 있으면 그걸 재료로 짧게 받아칠 것\n"
        "- 기본 템플릿 의미 유지\n"
        "- 끝은 반드시 모대?\n"
        "- 영어/설명/자기소개 금지\n"
        "출력:"
    )


def _normalize_team_fun_text(text: str) -> str:
    normalized = _MENTION_RE.sub(" ", str(text or ""))
    normalized = _URL_RE.sub(" ", normalized)
    return _WHITESPACE_RE.sub(" ", normalized).strip()


def _clean_team_fun_fragment(text: str) -> str:
    cleaned = str(text or "").strip(" \"'[]()")
    cleaned = _EDGE_FILLER_RE.sub("", cleaned).strip()
    cleaned = _TRAILING_ENDING_RE.sub("", cleaned).strip()
    cleaned = _TRAILING_PARTICLE_RE.sub("", cleaned).strip()
    return _EDGE_FILLER_RE.sub("", cleaned).strip()


def extract_team_fun_topic(text: str) -> str | None:
    """기존 Slack fun parser와 같은 규칙으로 결정적인 토픽을 고른다."""

    normalized = _normalize_team_fun_text(text)
    if "모대" not in normalized:
        return None
    clauses = [
        segment.strip()
        for segment in _CLAUSE_SPLIT_RE.split(normalized)
        if segment.strip()
    ]
    clause = next(
        (segment for segment in clauses if "모대" in segment),
        normalized,
    )
    before, _, after = clause.partition("모대")
    before = _clean_team_fun_fragment(before)
    after = _clean_team_fun_fragment(after)
    topic = before or after
    if not topic:
        topic = _clean_team_fun_fragment(clause.replace("모대", " "))
    if not topic:
        return None
    words = topic.split()
    if len(words) > 4:
        topic = " ".join(words[-4:])
    if len(topic) > 24:
        topic = topic[-24:].strip()
    return topic or None


def _pick_team_fun_template(
    seed_text: str,
    templates: tuple[str, ...],
) -> str:
    if not templates:
        return ""
    return templates[sum(ord(char) for char in seed_text) % len(templates)]


def build_team_fun_template(text: str) -> str:
    """provider 장애 때도 기존과 같은 안전한 결정적 답변을 만든다."""

    topic = extract_team_fun_topic(text) or "그거"
    compact_topic = topic.replace(" ", "")
    for keywords, templates in _FUN_TEMPLATE_RULES:
        if any(keyword in compact_topic for keyword in keywords):
            return _pick_team_fun_template(compact_topic, templates)
    topic_with_do = topic if topic.endswith("도") else f"{topic}도"
    template = _pick_team_fun_template(
        compact_topic or topic_with_do,
        _FUN_GENERIC_TEMPLATES,
    )
    return template.format(topic_with_do=topic_with_do)


def _sanitize_team_fun_answer(text: str) -> str:
    cleaned = _MENTION_RE.sub(" ", str(text or ""))
    cleaned = cleaned.replace("\n", " ").replace("\r", " ")
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip(" \"'[]()")
    if len(cleaned) > 48:
        cleaned = cleaned[:48].rstrip()
    return cleaned


def finalize_team_fun_reply(
    source_text: str,
    generated_text: str,
    fallback_text: str | None = None,
) -> str:
    """LLM 출력이 기존 계약을 벗어나면 local과 같은 template로 되돌린다."""

    fallback = fallback_text or build_team_fun_template(source_text)
    cleaned = _sanitize_team_fun_answer(generated_text)
    if not cleaned or _BAD_REPLY_RE.search(cleaned):
        return fallback
    cleaned = re.sub(r"^.*(?:->|=>|:)\s*", "", cleaned).strip()
    cleaned = re.split(
        r"(?:,|\.|!|;|:| 그런데 | 근데 | 하지만 | 그래서 )",
        cleaned,
        maxsplit=1,
    )[0].strip()
    cleaned = cleaned.replace("모대", " ").replace("?", " ").strip()
    cleaned = cleaned.rstrip("!~. ")

    topic = extract_team_fun_topic(source_text) or ""
    compact_topic = topic.replace(" ", "")
    if (
        topic
        and compact_topic in fallback.replace(" ", "")
        and compact_topic not in cleaned.replace(" ", "")
    ):
        cleaned = f"{topic} {cleaned}".strip()
    cleaned = _WHITESPACE_RE.sub(" ", cleaned).strip()
    if len(cleaned) < 2 or len(cleaned) > 18:
        return fallback
    return f"{cleaned} 모대?"


def is_daily_fortune_content(text: str, thread_root_text: str) -> bool:
    """transport 정보 없이 두 본문의 운세 근거만으로 후보를 재검사한다."""

    normalized_thread = _normalize_fortune_text(thread_root_text)
    if not all(
        marker.lower() in normalized_thread
        for marker in _FORTUNE_REQUIRED_MARKERS
    ):
        return False
    return _looks_like_fortune_detail_text(text)


def build_daily_fortune_reply(
    text: str,
    thread_root_text: str = "",
) -> str:
    """검증된 운세 키워드만 반영해 외부 provider 없이 답변을 조립한다."""

    normalized = _normalize_fortune_text(text)
    date_text = (
        _extract_fortune_date(text)
        or _extract_fortune_date(thread_root_text)
        or "오늘"
    )
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
            middle = (
                f"핵심은 {theme_text} 쪽이고 낙관이랑 경계를 같이 주네."
            )
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


def _daily_fortune_thread_root(request: CompanyAssistantRequest) -> str:
    # Slack adapter는 thread root 하나만 context entry로 전달하고 API는
    # 임의 metadata 대신 allowlisted context 본문을 다시 검사한다.
    for entry in request.context_entries:
        if entry.get("source") != "slack":
            continue
        text = str(entry.get("text") or "").strip()
        if text:
            return text
    return ""


def _normalize_fortune_text(text: str) -> str:
    normalized = _MENTION_RE.sub(" ", str(text or ""))
    normalized = re.sub(r"https?://\S+", " ", normalized)
    return _WHITESPACE_RE.sub(" ", normalized).strip().lower()


def _count_marker_hits(text: str, markers: tuple[str, ...]) -> int:
    return sum(1 for marker in markers if marker.lower() in text)


def _extract_fortune_date(text: str) -> str | None:
    match = _FORTUNE_DATE_RE.search(str(text or ""))
    if not match:
        return None
    year = int(match.group("year"))
    month = int(match.group("month"))
    day = int(match.group("day"))
    return f"{year}년 {month}월 {day}일"


def _extract_fortune_birth_years(text: str) -> list[str]:
    seen: set[str] = set()
    years: list[str] = []
    for matched_year in _FORTUNE_BIRTH_YEAR_RE.findall(str(text or "")):
        year = str(matched_year).strip()
        label = f"{year}년생"
        if not year or label in seen:
            continue
        seen.add(label)
        years.append(label)
    return years


def _looks_like_fortune_detail_text(text: str) -> bool:
    if _extract_fortune_birth_years(text):
        return True
    normalized = _normalize_fortune_text(text)
    return _count_marker_hits(normalized, _FORTUNE_DETAIL_MARKERS) >= 2


def _score_fortune_themes(normalized_text: str) -> dict[str, int]:
    scores: dict[str, int] = {}
    for label, markers in _FORTUNE_THEME_RULES:
        hit_count = _count_marker_hits(normalized_text, markers)
        if hit_count > 0:
            scores[label] = hit_count
    return scores


def _pick_top_fortune_themes(
    scores: dict[str, int],
    *,
    limit: int = 2,
) -> list[str]:
    ranked = sorted(
        scores.items(),
        key=lambda item: (
            -item[1],
            _FORTUNE_THEME_PRIORITY.get(
                item[0],
                len(_FORTUNE_THEME_PRIORITY),
            ),
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


def _extract_fortune_evidence(
    normalized_text: str,
    *,
    limit: int = 4,
) -> list[str]:
    evidence: list[str] = []
    for label, markers in _FORTUNE_EVIDENCE_RULES:
        if any(marker.lower() in normalized_text for marker in markers):
            evidence.append(label)
        if len(evidence) >= limit:
            break
    return evidence


def _build_fortune_target_text(years: list[str]) -> str:
    if not years:
        return "이 댓글 기준으론"
    if len(years) == 1:
        return f"{years[0]} 기준으론"
    if len(years) == 2:
        return f"{years[0]}, {years[1]} 기준으론"
    return f"{years[0]} 외 {len(years) - 1}개 년생 기준으론"


__all__ = [
    "CompanyDailyFortuneAssistantRoute",
    "CompanyLlmHealthAssistantRoute",
    "CompanyTeamFunAssistantRoute",
    "LlmHealthProbe",
    "TEAM_FUN_LLM_MAX_TOKENS",
    "TEAM_FUN_LLM_TIMEOUT_SEC",
    "TEAM_FUN_OLLAMA_MODEL",
    "build_daily_fortune_reply",
    "build_team_fun_prompt",
    "build_team_fun_template",
    "extract_team_fun_topic",
    "finalize_team_fun_reply",
    "is_daily_fortune_content",
    "match_company_daily_fortune_route",
    "match_company_llm_health_route",
    "match_company_team_fun_route",
]
