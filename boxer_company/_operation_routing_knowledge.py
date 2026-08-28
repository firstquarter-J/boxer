"""Notion·thread 학습 operation의 provider-free 분류 정본이다."""

from __future__ import annotations

import re
from typing import Callable

from boxer_company._operation_routing_common import (
    CompanyOperationRequestContract as CompanyAssistantRequest,
    window_assistant_context_entries,
)


_NOTION_QUESTION_TOKENS = (
    "마미박스",
    "mommybox",
    "박스",
    "유효성 검사",
    "유효성 검증",
    "바코드 검증",
    "녹화 취소",
    "취소 음성",
    "녹화 취소 음성",
    "모션감지",
    "모션 감지",
    "종료스캔",
    "종료 스캔",
    "C_STOPSESS",
    "c_stopsess",
    "자동 녹화",
    "자동 녹화 시작",
    "녹화 자동 시작",
    "자동으로 녹화",
    "녹화가 시작",
    "녹화 시작 음성",
    "녹화시작 안내음성",
    "녹화준비완료",
    "녹화 준비 완료",
    "재녹화",
    "파란 LED",
    "파란 led",
    "resource busy",
    "Device or resource busy",
    "/dev/video0",
    "ffmpeg",
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
    "led",
    "엘이디",
    "상태표시등",
    "초록불",
    "빨간불",
    "파란불",
    "깜빡",
    "깜박",
    "패턴",
    "증상",
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
    "invalid barcode",
    "invalid_barcode",
    "ln_invalid_barcode",
)


_NOTION_THREAD_MARKERS = (
    "문서 기반 답변",
    "함께 참고할 문서",
)


_NOTION_FOLLOWUP_TOKENS = (
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


_THREAD_REFERENCE_TOKENS = (
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


_ANSWER_INSTRUCTION_TOKENS = (
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


_REFERENCE_INSTRUCTION_TOKENS = (
    "참고해서",
    "참고해",
    "기준으로",
    "기준 삼아",
    "기반으로",
)


_SMALL_TALK_TOKENS = (
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


_IDENTITY_TOKENS = (
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


_PROFILE_HINTS = (
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


_COMPARISON_HINTS = (
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


# 실행 프로필과 provider-free matcher가 공유하는 사람 식별 alias 정본이다.
TEAM_MEMBER_ALIASES_BY_NAME: dict[str, tuple[str, ...]] = {
    "Mark": ("mark", "마크"),
    "Hyun": ("hyun",),
    "DD": ("dd", "디디"),
    "June": ("june",),
    "Juno": ("juno", "주노"),
    "Roy": ("roy", "로이"),
    "Maru": ("maru", "마루"),
    "Paul": ("paul", "폴"),
    "Danny": ("danny", "대니"),
    "Luka": ("luka", "루카"),
    "Sage": ("sage", "세이지"),
    "Olivia": ("olivia", "올리비아"),
}


_TEAM_MEMBER_ALIAS_TOKENS = tuple(
    sorted(
        alias
        for aliases in TEAM_MEMBER_ALIASES_BY_NAME.values()
        for alias in aliases
    )
)


_NOTION_PLAYBOOK_ROUTE = "notion_playbook_qa"


def _request_context_text(request: CompanyAssistantRequest) -> str:
    # matcher와 합성은 adapter 원본이 아니라 공통 창 제한을 거친 text만 사용한다.
    return "\n".join(
        str(entry.get("text") or "").strip()
        for entry in window_assistant_context_entries(request)
        if str(entry.get("text") or "").strip()
    )


def _looks_like_thread_answer_instruction(question: str) -> bool:
    text = (question or "").strip()
    if not text or not any(token in text for token in _THREAD_REFERENCE_TOKENS):
        return False
    return any(token in text for token in _ANSWER_INSTRUCTION_TOKENS) or any(
        token in text for token in _REFERENCE_INSTRUCTION_TOKENS
    )


def looks_like_notion_playbook_question(question: str) -> bool:
    text = (question or "").strip()
    if not text or _looks_like_thread_answer_instruction(text):
        return False
    return any(token in text for token in _NOTION_QUESTION_TOKENS)


def _has_notion_playbook_context(context_text: str) -> bool:
    normalized = (context_text or "").strip()
    return bool(normalized) and any(
        marker in normalized for marker in _NOTION_THREAD_MARKERS
    )


def looks_like_notion_playbook_followup(
    question: str,
    context_text: str,
) -> bool:
    text = (question or "").strip()
    if not text or not _has_notion_playbook_context(context_text):
        return False
    lowered = text.lower()
    collapsed = re.sub(r"[\s?!.,~]+", "", lowered)
    if any(token in text for token in _SMALL_TALK_TOKENS):
        return False
    if any(token in collapsed for token in _IDENTITY_TOKENS):
        return False
    if (
        any(alias in lowered for alias in _TEAM_MEMBER_ALIAS_TOKENS)
        and (
            any(token in lowered for token in _PROFILE_HINTS)
            or any(token in collapsed for token in _PROFILE_HINTS)
        )
    ):
        return False
    if any(token in lowered for token in _COMPARISON_HINTS):
        return False
    if _looks_like_thread_answer_instruction(text):
        return False
    if looks_like_notion_playbook_question(text):
        return False
    return any(token in text for token in _NOTION_FOLLOWUP_TOKENS) or any(
        token in lowered
        for token in ("alternative", "workaround", "other way", "else")
    )


def _match_notion_playbook_route(
    request: CompanyAssistantRequest,
    *,
    looks_like_question: Callable[[str], bool],
    looks_like_followup: Callable[[str, str], bool],
    context_text: str | None = None,
) -> str | None:
    """조회 없이 직접·후속 플레이북 질문의 공통 route만 확정한다."""

    # HTTP rollout matcher와 실제 route가 같은 정규화 문맥·판정 순서를
    # 공유해 adapter별 분류 차이로 다른 route를 호출하지 않게 한다.
    normalized_context = (
        _request_context_text(request)
        if context_text is None
        else context_text
    )
    if looks_like_question(request.question) or looks_like_followup(
        request.question,
        normalized_context,
    ):
        return _NOTION_PLAYBOOK_ROUTE
    return None


def match_notion_playbook_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """외부 조회·LLM 호출 없이 기본 플레이북 route를 분류한다."""

    return _match_notion_playbook_route(
        request,
        looks_like_question=looks_like_notion_playbook_question,
        looks_like_followup=looks_like_notion_playbook_followup,
    )


def _is_thread_playbook_learning_request(question: str) -> bool:
    normalized = re.sub(r"\s+", "", (question or "").strip().lower())
    if not normalized:
        return False
    return any(
        token in normalized
        for token in (
            "이스레드학습",
            "스레드학습",
            "스레드학습해",
            "스레드학습저장",
            "스레드학습시켜",
            "thread학습",
            "thread저장",
            "쓰레드학습",
        )
    )


def match_thread_playbook_learning_candidate_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """질문형까지 포함해 thread 학습 guard가 선점할 후보를 분류한다."""

    if str(request.metadata.get("route_group") or "").strip() != "operations":
        return None
    if request.channel != "slack" or not request.actor_id:
        return None
    if not _is_thread_playbook_learning_request(request.question):
        return None
    return "thread_playbook_learning"


def match_thread_playbook_learning_route(
    request: CompanyAssistantRequest,
) -> str | None:
    """기존 Slack과 같은 thread 학습 matcher 결과를 그대로 반환한다."""

    return match_thread_playbook_learning_candidate_route(request)
