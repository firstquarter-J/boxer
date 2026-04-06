import re
from typing import Any

from boxer_company import settings as cs
from boxer_company.team_chat_context import TEAM_MEMBER_PROFILES, build_team_freeform_context

_NOTION_DOC_QUERY_TOKENS = (
    "마미박스",
    "mommybox",
    "박스",
    "동기화",
    "베이비매직",
    "babymagic",
    "바이오스",
    "bios",
    "초기화",
    "데스크탑 모드",
    "데스크탑",
    "네트워크 환경",
    "네트워크 설정",
    "설정 스크립트",
    "음량",
    "볼륨",
    "dvi",
    "qr 코드북",
    "qr코드",
    "커스텀 크롭",
    "크롭",
    "진단기",
    "원격 음성",
    "299버전",
    "299",
    "캡처보드",
    "바코드 스캐너",
    "바코드 동기화",
    "핑크 바코드",
    "하얀색 바코드",
    "무료 바코드",
    "유료 바코드",
    "분만 병원",
    "비분만 병원",
    "첫 촬영",
    "첫 녹화",
    "신규 바코드 구매",
    "추가 구매",
    "온라인 상태",
    "cfg1_barcode_sync_date",
    "프로비저닝",
    "오디오",
    "사운드케이블",
    "스피커",
    "노이즈",
    "잡음",
    "아티팩트",
    "지지직",
    "그라운드 루프",
    "메모리",
    "패치",
    "방화벽",
    "firewall",
    "mda",
    "모니터링",
    "종합모니터링",
    "원격 접속",
    "원격 연결",
    "ssh",
    "status none",
    "에이전트",
)
_NOTION_DOC_THREAD_MARKERS = (
    "문서 기반 답변",
    "함께 참고할 문서",
)
_NOTION_DOC_FOLLOWUP_TOKENS = (
    "다른 방법",
    "방법 있어",
    "방법 없어",
    "대안",
    "우회",
    "그럼",
    "그러면",
    "그래서",
    "이 경우",
    "이때",
    "그 뒤",
    "그 후",
    "이건",
    "이거",
    "그건",
    "그거",
    "말고",
    "추가로",
)
_NOTION_DOC_OPERATION_FOLLOWUP_TOKENS = (
    "왜",
    "원인",
    "이유",
    "어떻게",
    "어떻게 해",
    "어떻게 해야",
    "확인",
    "재부팅",
    "재시작",
    "동기화",
    "설정",
    "조치",
    "해결",
    "방법",
    "맞아",
    "맞아?",
    "맞나요",
    "어디",
)
_FREEFORM_THREAD_REFERENCE_TOKENS = (
    "직전 질문",
    "이전 질문",
    "방금 질문",
    "위 질문",
    "이전 대화",
    "직전 대화",
    "위 대화",
    "방금 대화",
    "앞 질문",
)
_FREEFORM_ANSWER_INSTRUCTION_TOKENS = (
    "답해봐",
    "대답해봐",
    "답해 줘",
    "답해줘",
    "대답해 줘",
    "대답해줘",
    "말해봐",
    "정리해봐",
    "정리해 줘",
    "정리해줘",
)
_FREEFORM_REFERENCE_INSTRUCTION_TOKENS = (
    "참고해서",
    "참고해",
    "기준으로",
    "기준 삼아",
    "기반으로",
)
_FREEFORM_SMALL_TALK_TOKENS = (
    "안녕",
    "반가",
    "하이",
    "hello",
    "hi",
    "hey",
    "굿모닝",
    "굿나잇",
    "잘자",
    "잘 자",
)
_FREEFORM_IDENTITY_TOKENS = (
    "넌누구",
    "너누구",
    "너는누구",
    "누구야",
    "정체",
    "자기소개",
    "넌나야",
    "너는나야",
    "너도나야",
)
_TEAM_MEMBER_ALIAS_TOKENS = tuple(
    sorted(
        {
            str(alias or "").strip().lower()
            for profile in TEAM_MEMBER_PROFILES
            for alias in (
                *(profile.get("aliases") or ()),
                profile.get("name"),
            )
            if str(alias or "").strip()
        }
    )
)
_FREEFORM_PROFILE_HINTS = (
    "어떤 사람",
    "어떤사람",
    "누구야",
    "누구 같",
    "성격",
    "스타일",
    "캐릭터",
    "타입",
    "mbti",
    "엠비티아이",
    "전투력",
    "상성",
    "서열",
    "누가 더 세",
    "누가 더 쎄",
    "누가 이겨",
    "누가이겨",
    "어때",
    "어때?",
)
_NOTION_DOC_EXFILTRATION_PATTERNS = (
    re.compile(
        r"(시스템\s*(정보|프롬프트|지시문)|system\s*prompt|developer\s*prompt|internal\s*prompt|hidden\s*prompt|instruction\s*prompt)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(문서\s*(원문|전문|본문)|원문|전문|본문|raw\s*text|full\s*text|complete\s*text|entire\s*text|whole\s*text|verbatim|dump|텍스트\s*전체|전체\s*텍스트)",
        re.IGNORECASE,
    ),
    re.compile(
        r"(하나하나\s*(오픈|열)|하나씩\s*(오픈|열)|전부\s*보여|모두\s*보여|통째로\s*보여|텍스트로\s*보여|그대로\s*보여|show\s+me|open\s+each|open\s+every|one\s+by\s+one|full\s+text|all\s+text)",
        re.IGNORECASE,
    ),
    re.compile(
        r"((i\s*am|i'?m|im)\s+(super\s+admin|admin|owner|developer|maintainer)|super\s+admin|admin\s+mode|override|ignore\s+(previous|all)\s+(rules|instructions)|bypass)",
        re.IGNORECASE,
    ),
)
_NOTION_DOC_LEAK_MARKERS = (
    "system prompt",
    "developer prompt",
    "internal prompt",
    "thread context",
    "evidence(json)",
    "page_id=",
    "authorization:",
    "bearer ",
    "notion_token",
    "<think>",
    "</think>",
)
_FREEFORM_COMPARISON_HINTS = (
    " vs ",
    "누가",
    "전투력",
    "상성",
    "서열",
    "더 세",
    "더 쎄",
    "누가 이겨",
    "우위",
)
_FREEFORM_PLAYFUL_HINTS = (
    "놀려",
    "드립",
    "농담",
    "웃기",
    "한마디",
    "밈",
    "모대",
)
_FREEFORM_ADVICE_HINTS = (
    "어떻게",
    "추천",
    "골라",
    "선택",
    "판단",
    "하는 게 낫",
    "말까",
    "갈까",
)
_FREEFORM_META_LINE_PATTERNS = (
    re.compile(r"(?mi)^\s*현재 요청 적용\s*:\s*.+$"),
    re.compile(r"(?mi)^\s*(?:팀원별 컨텍스트|현재 화자 스타일|언급된 대상 반응 가이드)\s*:\s*$"),
)
_FREEFORM_META_PREFIX_REWRITES: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"^\s*(?:캐릭터|대화|채팅)\s*로그\s*기준(?:으로)?\s*(?:해석하면|보면)\s*[,:\-]?\s*",
            re.IGNORECASE,
        ),
        "",
    ),
    (
        re.compile(
            r"^\s*(?:채팅\s*밈|오늘\s*로그|캐릭터상(?:으로)?)\s*기준(?:으로)?\s*(?:해석하면|보면)\s*[,:\-]?\s*",
            re.IGNORECASE,
        ),
        "",
    ),
    (
        re.compile(
            r"\bfictional framing\b",
            re.IGNORECASE,
        ),
        "밈 프레임",
    ),
)
_BABYMAGIC_DOC_TITLES = {"베이비매직 장애 안내", "베이비매직 CS 자동화"}
_BABYMAGIC_SEND_HINT_TOKENS = (
    "유저 번호",
    "미매칭",
    "재전송",
    "앱으로 전송",
    "앱 전송",
)
_BABYMAGIC_RETRY_ACTION = (
    "유저가 앱에서 생성한 아이에 바코드를 등록했는지 먼저 확인하고, "
    "그다음 MDA 베이비매직 관리에서 재전송을 시도해"
)
_MOMMYBOX_RECORDING_PROCESS_TITLE = "마미박스 프로세스 순서"
_PINK_BARCODE_OVERVIEW_TITLE = "핑크 바코드: 운영 개요"
_BARCODE_FIRST_RECORDING_EDGE_CASE_TITLE = "바코드 표시: 구매 병원과 첫 촬영 병원이 다른 경우"
_PINK_BARCODE_VALIDATION_POLICY_TITLE = "바코드 검증: 핑크 바코드만 예외 허용할 수 있는지"


def _is_generic_count_or_existence_request(question: str) -> bool:
    text = (question or "").strip()
    lowered = text.lower()
    return any(token in text for token in cs.VIDEO_COUNT_HINT_TOKENS) or any(
        token in text for token in ("있나", "있어", "있는지", "유무", "존재", "몇")
    ) or any(token in lowered for token in ("count",))


def _render_notion_playbook_section(playbooks: list[dict[str, Any]] | None) -> str:
    items = [item for item in (playbooks or []) if isinstance(item, dict)]
    if not items:
        return ""

    lines = ["*참고 플레이북*"]
    for item in items[:3]:
        title = str(item.get("title") or "").strip() or "제목 미상"
        matched_keywords = [
            str(keyword).strip()
            for keyword in (item.get("matchedKeywords") or [])
            if str(keyword).strip()
        ]
        line = f"- {title}"
        if matched_keywords:
            line += f" (`{', '.join(matched_keywords[:3])}`)"
        lines.append(line)
    return "\n".join(lines)


def _append_notion_playbook_section(
    text: str,
    playbooks: list[dict[str, Any]] | None,
) -> str:
    section = _render_notion_playbook_section(playbooks)
    normalized_text = (text or "").strip()
    if not section:
        return normalized_text
    if "참고 플레이북" in normalized_text:
        return normalized_text
    if not normalized_text:
        return section
    return f"{normalized_text}\n\n{section}"


def _render_company_notion_doc_section(docs: list[dict[str, Any]] | None) -> str:
    items = [item for item in (docs or []) if isinstance(item, dict)]
    if not items:
        return ""

    lines = ["*함께 참고할 문서*"]
    for item in items[:3]:
        title = str(item.get("title") or "").strip()
        url = str(item.get("url") or "").strip()
        if not title or not url:
            continue
        lines.append(f"- <{url}|{title}>")
    return "\n".join(lines)


def _append_company_notion_doc_section(
    text: str,
    docs: list[dict[str, Any]] | None,
) -> str:
    section = _render_company_notion_doc_section(docs)
    normalized_text = (text or "").strip()
    if not section:
        return normalized_text
    if "함께 참고할 문서" in normalized_text:
        return normalized_text
    if not normalized_text:
        return section
    return f"{normalized_text}\n\n{section}"


def _looks_like_notion_doc_question(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False
    if _looks_like_thread_answer_instruction(text):
        return False
    return any(token in text for token in _NOTION_DOC_QUERY_TOKENS)


def _thread_has_notion_doc_context(thread_context: str) -> bool:
    text = (thread_context or "").strip()
    if not text:
        return False
    return any(marker in text for marker in _NOTION_DOC_THREAD_MARKERS)


def _sanitize_notion_doc_thread_context(thread_context: str) -> str:
    text = (thread_context or "").strip()
    if not _thread_has_notion_doc_context(text):
        return ""
    return text


def _looks_like_small_talk_question(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False

    lowered = text.lower()
    collapsed = re.sub(r"[\s?!.,~]+", "", lowered)
    if any(token in text for token in _FREEFORM_SMALL_TALK_TOKENS):
        return True
    return any(token in collapsed for token in _FREEFORM_IDENTITY_TOKENS)


def _looks_like_team_freeform_question(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False

    lowered = text.lower()
    collapsed = re.sub(r"[\s?!.,~]+", "", lowered)
    has_member_alias = any(alias in lowered for alias in _TEAM_MEMBER_ALIAS_TOKENS)
    has_profile_hint = any(token in lowered for token in _FREEFORM_PROFILE_HINTS) or any(
        token in collapsed for token in _FREEFORM_PROFILE_HINTS
    )

    if has_member_alias and has_profile_hint:
        return True
    if any(token in lowered for token in _FREEFORM_COMPARISON_HINTS):
        return True
    return False


def _looks_like_thread_answer_instruction(question: str) -> bool:
    text = (question or "").strip()
    if not text:
        return False

    has_thread_reference = any(token in text for token in _FREEFORM_THREAD_REFERENCE_TOKENS)
    if not has_thread_reference:
        return False

    has_answer_instruction = any(token in text for token in _FREEFORM_ANSWER_INSTRUCTION_TOKENS)
    has_reference_instruction = any(token in text for token in _FREEFORM_REFERENCE_INSTRUCTION_TOKENS)
    return has_answer_instruction or has_reference_instruction


def _looks_like_notion_doc_followup(question: str, thread_context: str) -> bool:
    text = (question or "").strip()
    if not text or not _thread_has_notion_doc_context(thread_context):
        return False
    if _looks_like_small_talk_question(text):
        return False
    if _looks_like_team_freeform_question(text):
        return False
    if _looks_like_thread_answer_instruction(text):
        return False
    if _looks_like_notion_doc_question(text):
        return False

    lowered = text.lower()
    if any(token in text for token in _NOTION_DOC_FOLLOWUP_TOKENS):
        return True
    if any(token in text for token in _NOTION_DOC_OPERATION_FOLLOWUP_TOKENS):
        return True
    if any(token in lowered for token in ("alternative", "workaround", "other way", "else")):
        return True
    return False


def _build_notion_doc_query_text(question: str, thread_context: str) -> str:
    normalized_question = (question or "").strip()
    normalized_thread = _resolve_notion_doc_thread_context(question, thread_context)
    if not normalized_thread:
        return normalized_question

    thread_lines = [line.strip() for line in normalized_thread.splitlines() if line.strip()]
    relevant_thread = "\n".join(thread_lines[-6:])
    if not relevant_thread:
        return normalized_question
    return f"{relevant_thread}\n{normalized_question}".strip()


def _resolve_notion_doc_thread_context(question: str, thread_context: str) -> str:
    normalized_thread = _sanitize_notion_doc_thread_context(thread_context)
    if not normalized_thread:
        return ""
    if not _looks_like_notion_doc_followup(question, normalized_thread):
        return ""
    return normalized_thread


def _is_notion_doc_exfiltration_attempt(question: str, thread_context: str = "") -> bool:
    text = (question or "").strip()
    if not text:
        return False
    if not (
        _looks_like_notion_doc_question(text)
        or _thread_has_notion_doc_context(thread_context)
    ):
        return False
    return any(pattern.search(text) for pattern in _NOTION_DOC_EXFILTRATION_PATTERNS)


def _build_notion_doc_security_refusal() -> str:
    return "보안 위반 시도로 판단해 요청을 즉시 차단해. 문서 원문, 시스템 정보, 내부 지시문은 공개하지 않아. 같은 시도가 반복되면 관리자 검토 및 접근 제한 대상으로 처리해."


def _classify_freeform_response_mode(question: str, thread_context: str = "") -> str:
    normalized = f"{question or ''}\n{thread_context or ''}".lower()
    if any(token in normalized for token in _FREEFORM_COMPARISON_HINTS):
        return "comparison"
    if any(token in normalized for token in _FREEFORM_PLAYFUL_HINTS):
        return "playful"
    if any(token in normalized for token in _FREEFORM_ADVICE_HINTS):
        return "advice"
    return "analysis"


def _build_freeform_response_rules(question: str, thread_context: str = "") -> str | None:
    base_rules = (cs.FREEFORM_RESPONSE_RULES_PROMPT or "").strip()
    mode = _classify_freeform_response_mode(question, thread_context)
    mode_line = {
        "comparison": '- 비교/상성 질문이면 "결론 -> 이유 2~3개 -> 변수/예외 1개" 순서로 바로 답해.',
        "playful": "- 가벼운 드립 질문이면 1~3문장 안에서 임팩트 있게 답해. 마지막 한 줄만 세게 쳐.",
        "advice": '- 조언/판단 질문이면 "결론 -> 옵션/다음 액션 -> 이유" 순서로 답해.',
        "analysis": '- 해석/분석 질문이면 "결론 -> 구조적 근거 -> 리스크/예외" 순서로 답해.',
    }[mode]
    if base_rules:
        return f"{base_rules}\n{mode_line}"
    return mode_line


def _sanitize_freeform_reply(text: str) -> str:
    normalized = (text or "").strip()
    if not normalized:
        return ""

    cleaned = normalized
    for pattern in _FREEFORM_META_LINE_PATTERNS:
        cleaned = pattern.sub("", cleaned)
    for pattern, replacement in _FREEFORM_META_PREFIX_REWRITES:
        cleaned = pattern.sub(replacement, cleaned)

    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
    return cleaned or normalized


def _get_freeform_system_prompt(
    question: str = "",
    thread_context: str = "",
) -> str | None:
    sections = [
        (cs.FREEFORM_CORE_IDENTITY_PROMPT or "").strip(),
        _build_freeform_response_rules(question, thread_context) or "",
    ]
    prompt = "\n\n".join(section for section in sections if section).strip()
    return prompt or None


def _build_freeform_chat_system_prompt(
    question: str,
    thread_context: str,
    *,
    speaker_user_id: str = "",
) -> str | None:
    base_prompt = _get_freeform_system_prompt(question, thread_context) or ""
    team_context = build_team_freeform_context(
        question,
        thread_context,
        speaker_user_id=speaker_user_id,
    )
    if base_prompt and team_context:
        return f"{base_prompt}\n\n{team_context}"
    if base_prompt:
        return base_prompt
    return team_context or None


def _sanitize_notion_references_for_llm(references: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    items = [item for item in (references or []) if isinstance(item, dict)]
    sanitized: list[dict[str, Any]] = []
    for item in items[:3]:
        preview_lines = [
            str(line).strip()[:160]
            for line in (item.get("previewLines") or [])
            if str(line).strip()
        ][:5]
        sanitized.append(
            {
                "title": str(item.get("title") or "").strip(),
                "section": str(item.get("section") or "").strip(),
                "kind": str(item.get("kind") or "").strip(),
                "priority": str(item.get("priority") or "").strip(),
                "matchedKeywords": [
                    str(keyword).strip()
                    for keyword in (item.get("matchedKeywords") or [])
                    if str(keyword).strip()
                ][:4],
                "previewLines": preview_lines,
                "summary": " / ".join(preview_lines[:3]),
            }
        )
    return sanitized


def _needs_notion_doc_security_refusal(text: str, route_name: str) -> bool:
    if route_name != "notion playbook qa":
        return False
    normalized = (text or "").strip().lower()
    if any(marker in normalized for marker in _NOTION_DOC_LEAK_MARKERS):
        return True
    meaningful_lines = [line for line in (text or "").splitlines() if line.strip()]
    if "```" in (text or ""):
        return True
    if len(meaningful_lines) > 16:
        return True
    if len(text or "") > 1400:
        return True
    return False


def _build_notion_doc_fallback(question: str, references: list[dict[str, Any]] | None) -> str:
    def _clean_preview_line(text: str) -> str:
        line = re.sub(r"^#+\s*", "", str(text or "").strip())
        line = re.sub(r"^[-*•]\s*", "", line)
        line = re.sub(r"\s+", " ", line).strip()
        if ":" in line:
            prefix, rest = line.split(":", 1)
            normalized_prefix = prefix.strip()
            if normalized_prefix and (
                len(normalized_prefix) <= 24
                or normalized_prefix.endswith(("돼요", "되나", "포인트", "기준"))
                or normalized_prefix in {"정책", "전제", "운영 기준", "실제 사례"}
            ):
                line = rest.strip()
        replacements = (
            (
                "비분만 병원에서 무료 발급한 바코드(핑크 바코드)는 바코드를 유료로 판매하는 분만 병원 장비에서 스캔되지 않아야 함",
                "무료 바코드는 분만 병원에서 스캔되면 안 돼",
            ),
            (
                "이 정책은 분만 병원 마미박스가 온라인 상태에서 바코드 동기화를 받아야 반영됨",
                "온라인 바코드 동기화가 돼야 반영돼",
            ),
            (
                "장비의 마지막 바코드 동기화 일자가 오래되면 최신 제한 대상 바코드를 아직 내려받지 못해 녹화 준비로 넘어갈 수 있음",
                "동기화가 밀리면 차단 바코드가 아직 반영되지 않을 수 있어",
            ),
            (
                "오프라인이거나 동기화가 밀린 장비는 무료 바코드 차단 정책이 늦게 반영될 수 있음",
                "오프라인 장비는 차단 반영이 늦을 수 있어",
            ),
            (
                "마지막 바코드 동기화 일자와 `cfg1_barcode_sync_date` 갱신 여부를 먼저 확인",
                "마지막 동기화 일자와 `cfg1_barcode_sync_date`를 확인해",
            ),
            (
                "마지막 바코드 동기화 일자와 cfg1_barcode_sync_date 갱신 여부를 먼저 확인",
                "마지막 동기화 일자와 `cfg1_barcode_sync_date`를 확인해",
            ),
            (
                "아니고 평소에도 동기화는 계속 진행된다. 다만 재시작 직후에는 동기화가 실제로 도는지 확인하기 쉽다",
                "재부팅이 필수는 아니고 평소에도 동기화는 계속 돌아가",
            ),
            (
                "아니야. 평소에도 동기화는 계속 돌아가고, 재시작은 동기화가 실제로 진행됐는지 확인하기 쉬운 시점이야",
                "재부팅이 필수는 아니고 평소에도 동기화는 계속 돌아가",
            ),
            (
                "일반 사용 중에는 재부팅을 하지 않아도 매일 동기화가 진행된다고 보면 됨",
                "평소에도 매일 동기화가 진행돼",
            ),
        )
        for source, target in replacements:
            line = line.replace(source, target)
        return line[:90]

    def _pick_preview_line(
        lines: list[str],
        *,
        include_tokens: tuple[str, ...] = (),
        exclude_texts: set[str] | None = None,
    ) -> str:
        excluded = exclude_texts or set()
        for line in lines:
            if not line or line in excluded:
                continue
            if include_tokens and not any(token in line for token in include_tokens):
                continue
            return line
        return ""

    items = [item for item in (references or []) if isinstance(item, dict)]
    lines = ["*문서 기반 답변*"]
    if not items:
        lines.append("• 결론: 관련 문서를 못 찾았어")
        lines.append("• 확인: 증상이나 키워드를 더 구체적으로 말해줘")
        lines.append("• 조치: 문서 제목이나 장애 증상을 같이 보내줘")
        return "\n".join(lines)

    primary_title = str(items[0].get("title") or "").strip() or "제목 미상"
    preview_fragments: list[str] = []
    for item in items[:3]:
        for raw_line in item.get("previewLines") or []:
            line = _clean_preview_line(raw_line)
            if not line:
                continue
            if line == str(item.get("title") or "").strip():
                continue
            if line.startswith("- page_id="):
                continue
            if line in preview_fragments:
                continue
            preview_fragments.append(line)
            if len(preview_fragments) >= 8:
                break
        if len(preview_fragments) >= 8:
            break

    is_pink_barcode_overview_doc = primary_title == _PINK_BARCODE_OVERVIEW_TITLE
    is_mommybox_recording_process_doc = primary_title == _MOMMYBOX_RECORDING_PROCESS_TITLE
    is_barcode_sync_doc = primary_title == "바코드 동기화: 분만 병원에서 핑크 바코드가 스캔되는 경우"
    is_barcode_first_recording_edge_case_doc = primary_title == _BARCODE_FIRST_RECORDING_EDGE_CASE_TITLE
    is_pink_barcode_validation_policy_doc = primary_title == _PINK_BARCODE_VALIDATION_POLICY_TITLE
    is_firewall_doc = primary_title == "병원 방화벽으로 MDA/원격 접속이 안 될 때"
    normalized_question = (question or "").strip()
    is_babymagic_send_issue = primary_title in _BABYMAGIC_DOC_TITLES and (
        any(token in normalized_question for token in ("전송", "재전송", "이유", "원인", "왜", "안 된", "안된"))
        or any(
            any(token in fragment for token in _BABYMAGIC_SEND_HINT_TOKENS)
            for fragment in preview_fragments
        )
    )
    is_reason_question = any(token in normalized_question for token in ("왜", "원인", "이유"))
    is_restart_question = any(token in normalized_question for token in ("재부팅", "재시작", "껐다", "켜야"))
    is_meaning_question = "cfg1_barcode_sync_date" in normalized_question or any(
        token in normalized_question for token in ("뭐야", "무엇", "뜻", "의미")
    )

    conclusion = ""
    if is_restart_question:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("재부팅", "재시작"),
        )
    elif is_reason_question:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("반영되지", "스캔될 수 있어", "왜 스캔되나", "원인"),
        )
    elif is_meaning_question:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("cfg1_barcode_sync_date",),
        )
    if not conclusion:
        conclusion = _pick_preview_line(
            preview_fragments,
            include_tokens=("안 돼", "동기화", "원인"),
        ) or (preview_fragments[0] if preview_fragments else f"`{primary_title}` 기준 확인 필요")

    used_lines = {conclusion}
    confirm = _pick_preview_line(
        preview_fragments,
        include_tokens=("확인 포인트", "cfg1_barcode_sync_date", "마지막 동기화"),
        exclude_texts=used_lines,
    )
    if not confirm:
        confirm = _pick_preview_line(
            preview_fragments,
            include_tokens=("전제:", "온라인", "동기화"),
            exclude_texts=used_lines,
        )
    if not confirm:
        confirm = _pick_preview_line(preview_fragments, exclude_texts=used_lines) or f"`{primary_title}` 문서를 먼저 봐"
    used_lines.add(confirm)

    action = _pick_preview_line(
        preview_fragments,
        include_tokens=("확인 포인트", "cfg1_barcode_sync_date", "마지막 동기화"),
        exclude_texts=used_lines,
    )
    if not action:
        action = _pick_preview_line(
            preview_fragments,
            include_tokens=("온라인", "오프라인", "동기화"),
            exclude_texts=used_lines,
        )
    if not action:
        action = "문서 기준 확인 필요"
    if is_babymagic_send_issue:
        action = _BABYMAGIC_RETRY_ACTION

    if is_pink_barcode_overview_doc:
        lines.append("• 결론: 핑크 바코드 이슈는 동기화, 앱 표시, 검증 정책 3가지로 나눠 봐야 해")
        lines.append("• 확인: 지금 질문이 분만 병원에서 스캔된 건지, 앱에 핑크로 보이는 건지, 검증 해제 정책인지 먼저 구분해")
        lines.append("• 조치: 스캔 이슈면 동기화 문서, 앱 표시 이슈면 첫 촬영 병원 문서, 허용/차단 정책이면 검증 정책 문서 기준으로 이어서 보면 돼")
        return "\n".join(lines)

    if is_mommybox_recording_process_doc:
        lines.append("• 결론: 바코드 스캔 후 준비 음성이 나오고 세션이 생성된 뒤 모션 감지가 시작돼")
        lines.append("• 확인: 모션 감지 단계의 상태는 RECORDING이 아니라 SESSION이고, 모션 감지 성공 또는 타임아웃이면 그때 녹화 시작 음성 후 본 녹화가 시작돼. 모션 감지 단계에서 종료 스캔하면 본 녹화 종료와 같은 의미가 아니야")
        lines.append("• 조치: 녹화 중 종료 스캔하면 종료 음성이 나오고 파일을 마무리한 뒤 업로드를 시도한다고 안내해")
        return "\n".join(lines)

    if is_barcode_sync_doc and not is_meaning_question:
        if is_restart_question:
            barcode_sync_conclusion = "재부팅이 필수는 아니고, 마미박스는 매일 핑크 바코드 동기화를 시도해"
        else:
            barcode_sync_conclusion = "지금은 장비가 최신 핑크 바코드까지 동기화하지 못해서 분만 병원에서도 스캔된 거로 봐"
        lines.append(f"• 결론: {barcode_sync_conclusion}")
        lines.append("• 확인: 핑크 바코드 동기화가 가능한 버전인지 먼저 확인해")
        lines.append("• 조치: 마미박스를 핑크 바코드 동기화가 가능한 버전으로 업데이트해야 해. 1회당 약 10일치 바코드를 가져오고, 매일 동기화를 시도해. 현재 기본 DB에는 1월 1일부터의 핑크 바코드 목록이 있어")
        return "\n".join(lines)

    if is_barcode_first_recording_edge_case_doc:
        lines.append("• 결론: 첫 녹화가 비분만 병원에서 먼저 나가면 앱에는 핑크 바코드로 보일 수 있어")
        lines.append("• 확인: 분만 병원에서 실제 첫 촬영이 없었는지랑 첫 recording hospital이 비분만 병원인지 먼저 확인해")
        lines.append("• 조치: 이건 표시상 엣지케이스라 실제 녹화 차단이나 신규 바코드 추가 구매가 필요한 건 아니라고 안내해")
        return "\n".join(lines)

    if is_pink_barcode_validation_policy_doc:
        is_validation_disable_question = any(
            token in normalized_question
            for token in (
                "검증을 풀",
                "검증 풀",
                "검증 해제",
                "유효성 검증 해제",
                "검증없이",
                "검증 없이",
            )
        )
        if is_validation_disable_question:
            lines.append("• 결론: 맞아. 바코드 유효성 검증을 해제하면 검증 없이 녹화가 진행돼")
            lines.append("• 확인: 이건 핑크 바코드만 예외 허용하는 게 아니라 전체 검증을 푸는 설정이야")
            lines.append("• 조치: 특정 핑크 바코드만 따로 허용하는 건 현재 안 돼. 허용이 필요하면 검증 유지/전체 해제 중 운영 판단이 필요해")
            return "\n".join(lines)

        lines.append("• 결론: 핑크 바코드만 따로 녹화 허용/차단하는 설정은 없어")
        lines.append("• 확인: 분만 병원에서 차단이 걸리려면 바코드 유효성 검증이 켜져 있는지 먼저 확인해")
        lines.append("• 조치: 핑크 바코드도 녹화되게 하려면 바코드 유효성 검증 자체를 해제해야 하고, 그러면 검증 없이 녹화가 진행돼")
        return "\n".join(lines)

    if is_firewall_doc:
        lines.append("• 결론: 영상 업로드는 정상이어도 원격 접속은 별도 경로라 불가할 수 있어. 현재는 장비 원격 접근이 제한된 상태야")
        lines.append("• 확인: 병원 네트워크 또는 방화벽 설정 여부, 방화벽 정책, 장비 원격 접근 여부(SSH 연결) 확인이 필요해")
        lines.append("• 조치: 병원과 네트워크 또는 방화벽 설정을 소통 및 협의해야 해. 접속이 열리면 그 뒤에 원격 진단을 다시 진행할 수 있어")
        return "\n".join(lines)

    lines.append(f"• 결론: {conclusion}")
    lines.append(f"• 확인: {confirm}")
    lines.append(f"• 조치: {action}")
    return "\n".join(lines)


def _needs_notion_doc_fallback(text: str, route_name: str, fallback_text: str = "") -> bool:
    if route_name != "notion playbook qa":
        return False

    normalized = (text or "").strip()
    if not normalized:
        return True
    if normalized == _build_notion_doc_security_refusal():
        return False
    if not normalized.startswith("*문서 기반 답변*"):
        return True

    fallback_normalized = (fallback_text or "").strip()
    lowered = normalized.lower()
    fallback_lowered = fallback_normalized.lower()
    if "핑크 바코드 동기화가 가능한 버전" in fallback_normalized and "핑크 바코드 동기화가 가능한 버전" not in normalized:
        return True
    if "cfg1_barcode_sync_date" in lowered and "cfg1_barcode_sync_date" not in fallback_lowered:
        return True
    if _BABYMAGIC_RETRY_ACTION in fallback_normalized and _BABYMAGIC_RETRY_ACTION not in normalized:
        return True
    if "표시상 엣지케이스" in fallback_normalized and "엣지케이스" not in normalized:
        return True
    if "추가 구매" in fallback_normalized and "추가 구매" not in normalized:
        return True

    required_bullets = (
        "• 결론:",
        "• 확인:",
        "• 조치:",
    )
    return any(bullet not in normalized for bullet in required_bullets)


def _normalize_notion_doc_answer_style(text: str, route_name: str) -> str:
    if route_name != "notion playbook qa":
        return (text or "").strip()

    normalized = (text or "").strip()
    if not normalized:
        return normalized

    replacements = (
        ("소통·협의", "소통 및 협의"),
        ("원격으로 원인 확인이나 조치가 어렵다고 안내해", "원격으로 원인 확인이나 조치가 어려워"),
        ("협의가 필요하다고 안내해", "협의가 필요해"),
        ("확인이 필요하다고 안내해", "확인이 필요해"),
        ("다시 진행한다고 안내해", "다시 진행할 수 있어"),
        ("다시 진행한다고 답해", "다시 진행할 수 있어"),
        ("안내해.", ""),
        ("안내해", ""),
    )
    for source, target in replacements:
        normalized = normalized.replace(source, target)

    normalized = re.sub(r"\s+\.", ".", normalized)
    normalized = re.sub(r"\n{3,}", "\n\n", normalized)
    return normalized.strip()
