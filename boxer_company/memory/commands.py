"""도구·권한 문구를 저장 지시로 해석하지 않는 좁은 개인 기억 명령."""

from __future__ import annotations

from dataclasses import dataclass
import re


MEMORY_MANAGE_ROUTE = "conversation_memory_manage"


@dataclass(frozen=True, slots=True)
class MemoryCommand:
    action: str
    memory_id: str | None = None
    topic_key: str | None = None
    text: str | None = None


def parse_memory_command(question: str) -> MemoryCommand | None:
    value = str(question or "").strip().rstrip(".!?")
    if re.fullmatch(r"내\s*(?:기억|답변\s*선호)(?:를|는)?\s*(?:보여줘|알려줘|목록)", value):
        return MemoryCommand("list")
    if re.fullmatch(r"(?:이\s*기억|내\s*기억)(?:을|은)?\s*(?:잊어줘|삭제해줘)", value):
        return MemoryCommand("choose_forget")
    if re.fullmatch(r"이\s*주제(?:는|를)?\s*기억하지\s*마", value):
        return MemoryCommand("choose_suppress")
    match = re.fullmatch(r"기억\s+([A-Za-z0-9_-]{8,80})\s*주제(?:는|를)?\s*기억하지\s*마", value)
    if match:
        return MemoryCommand("suppress", memory_id=match.group(1))
    match = re.fullmatch(r"기억\s+([A-Za-z0-9_-]{8,80})(?:을)?\s*(?:잊어줘|삭제해줘)", value)
    if match:
        return MemoryCommand("forget", memory_id=match.group(1))
    # 지속할 수 있는 표현 선호만 정해진 값으로 저장하고 원문은 명령으로 실행하지 않는다.
    match = re.fullmatch(r"내\s*답변\s*선호(?:를)?\s+(.{1,30}?)(?:로|으로)?\s*(?:바꿔줘|저장해줘|기억해줘)", value)
    if not match:
        return None
    preference = match.group(1).strip()
    options = {
        "짧게": ("response_length", "답변은 짧게 받는 것을 선호함"),
        "간결하게": ("response_length", "답변은 짧게 받는 것을 선호함"),
        "자세하게": ("response_length", "답변은 자세하게 받는 것을 선호함"),
        "자세히": ("response_length", "답변은 자세하게 받는 것을 선호함"),
        "한국어": ("response_language", "한국어 답변을 선호함"),
        "영어": ("response_language", "영어 답변을 선호함"),
        "결론 먼저": ("response_order", "결론을 먼저 설명하는 답변을 선호함"),
    }
    selected = options.get(preference)
    return MemoryCommand("set_preference", topic_key=selected[0], text=selected[1]) if selected else MemoryCommand("choose_preference")
