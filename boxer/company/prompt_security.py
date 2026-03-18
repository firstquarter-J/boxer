import re

_DIRECT_PROMPT_MARKER_RE = re.compile(
    r"(시스템\s*(프롬프트|지시문|규칙)|developer\s*prompt|system\s*prompt|internal\s*prompt|hidden\s*prompt|내부\s*(프롬프트|지시문|규칙|설정))",
    re.IGNORECASE,
)
_PROMPT_NOUN_RE = re.compile(
    r"(프롬프트|prompt|컨텍스트|context|지시문|설정|규칙)",
    re.IGNORECASE,
)
_SELF_REFERENCE_RE = re.compile(
    r"(너의|너한테|너를|너는|네가|니가|네\s*(프롬프트|설정|규칙|컨텍스트)|니\s*(프롬프트|설정|규칙|컨텍스트)|박서|boxer|이\s*봇|현재\s*(프롬프트|설정|컨텍스트)?|지금\s*(프롬프트|설정|컨텍스트)?)",
    re.IGNORECASE,
)
_REVEAL_INTENT_RE = re.compile(
    r"(보여|읽어|알려|말해|적혀|써있|써\s*있|되어|되있|들어|포함|원문|전문|그대로|복붙|복사|dump|list|리스트|찾아보|있어|없어|뭐야|뭐냐|어떻게)",
    re.IGNORECASE,
)
_PERSON_CONTEXT_RE = re.compile(
    r"(mark|hyun|dd|june|juno|roy|maru|paul|danny|luka|olivia|oliva|사람|팀원|인물|성향|전투력)",
    re.IGNORECASE,
)
_THREAD_MARKERS = (
    "프롬프트에",
    "system prompt",
    "developer prompt",
    "internal prompt",
    "현재 말하는 사람:",
    "관련 인물 성향:",
)
_FOLLOWUP_HINT_RE = re.compile(
    r"(그럼|또|더|나머지|다른|전부|전체|찾아보|포함|추가|는\?|도\s|도$|있어\?|있나\?|도 찾아)",
    re.IGNORECASE,
)


def _normalize_prompt_security_text(text: str) -> str:
    return re.sub(r"\s+", " ", str(text or "").strip()).lower()


def looks_like_prompt_exfiltration_question(question: str) -> bool:
    text = _normalize_prompt_security_text(question)
    if not text:
        return False
    if _DIRECT_PROMPT_MARKER_RE.search(text):
        return True
    if not (_PROMPT_NOUN_RE.search(text) and _REVEAL_INTENT_RE.search(text)):
        return False
    return bool(_SELF_REFERENCE_RE.search(text) or _PERSON_CONTEXT_RE.search(text))


def thread_has_prompt_exfiltration_context(thread_context: str) -> bool:
    text = _normalize_prompt_security_text(thread_context)
    if not text:
        return False
    if any(marker in text for marker in _THREAD_MARKERS):
        return True
    return looks_like_prompt_exfiltration_question(text)


def _looks_like_prompt_exfiltration_followup(question: str) -> bool:
    text = _normalize_prompt_security_text(question)
    if not text:
        return False
    if _REVEAL_INTENT_RE.search(text):
        return True
    if len(text) > 48:
        return False
    return bool(_FOLLOWUP_HINT_RE.search(text) or "," in text or "/" in text)


def is_prompt_exfiltration_attempt(question: str, thread_context: str = "") -> bool:
    if looks_like_prompt_exfiltration_question(question):
        return True
    if thread_has_prompt_exfiltration_context(thread_context):
        return _looks_like_prompt_exfiltration_followup(question)
    return False


def build_prompt_security_refusal() -> str:
    return "시스템 프롬프트나 내부 컨텍스트는 공개하지 않아. 거기에 들어간 사람 성향, 규칙, 설정도 그대로 말해주지 않아."
