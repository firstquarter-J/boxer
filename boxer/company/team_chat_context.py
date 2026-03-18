from boxer.company import settings as cs

TEAM_CHAT_GENERAL_GUIDE = (
    "팀 자유대화는 가벼운 드립과 메타 농담이 자주 오가지만, 한 번 치고 회수하는 톤이 맞아. "
    "기술 얘기도 농담으로 이어질 수 있지만 업무 관계를 해치지 않는 선을 지켜. "
    "특정 인물을 집요하게 모욕하거나 따돌리는 식으로 확대하지 마."
)

TEAM_MEMBER_PROFILES: tuple[dict[str, object], ...] = (
    {
        "name": "Mark",
        "aliases": ("mark", "마크"),
        "user_id": cs.MARK_USER_ID,
        "summary": "분위기를 가볍게 만들고 계속 굴리는 타입. 기술 얘기도 드립으로 연결하고 남의 발언을 재가공해 판을 키움.",
    },
    {
        "name": "Hyun",
        "aliases": ("hyun",),
        "user_id": cs.HYUN_USER_ID,
        "summary": "겉으로는 차분하고 논리적이지만 실제론 끝까지 밀어붙이는 구조 집착형. 도발도 논리적인 문장으로 함.",
    },
    {
        "name": "DD",
        "aliases": ("dd", "디디"),
        "user_id": cs.DD_USER_ID,
        "summary": "감정이 빠르게 드러나는 반응형. 순간적으로 세게 치고 바로 수습하는 인간적인 리액션이 강함.",
    },
    {
        "name": "June",
        "aliases": ("june",),
        "summary": "규칙보다 재미를 우선하고 흐름을 비틀어 새 판을 만드는 무정부주의자형.",
    },
    {
        "name": "Juno",
        "aliases": ("juno", "주노"),
        "summary": "평소 조용하지만 타이밍 보고 한 방으로 방향을 바꾸는 관찰자형.",
    },
    {
        "name": "Roy",
        "aliases": ("roy", "로이"),
        "summary": "현실적인 해결책과 장비·환경 관점 제안을 던지는 인프라형 사고.",
    },
    {
        "name": "Maru",
        "aliases": ("maru", "마루"),
        "summary": "배려 중심이고 갈등을 낮추는 고신뢰 완충재 역할.",
    },
    {
        "name": "Paul",
        "aliases": ("paul", "폴"),
        "summary": "생활 기반 현실 피드백을 주는 솔직한 타입.",
    },
    {
        "name": "Danny",
        "aliases": ("danny", "대니"),
        "summary": "짧은 리액션으로 흐름을 이어주는 분위기 유지자형.",
    },
    {
        "name": "Luka",
        "aliases": ("luka", "루카"),
        "summary": "원칙과 현실 체크를 들고 와서 선 넘는 흐름을 제동하는 브레이크 역할.",
    },
)

_PROFILE_BY_NAME = {
    str(profile["name"]): profile
    for profile in TEAM_MEMBER_PROFILES
}


def _normalize_context_text(*texts: str) -> str:
    joined = " ".join(str(text or "") for text in texts)
    return joined.lower().strip()


def _iter_profile_aliases(profile: dict[str, object]) -> tuple[str, ...]:
    aliases = [str(alias).strip().lower() for alias in (profile.get("aliases") or ()) if str(alias).strip()]
    user_id = str(profile.get("user_id") or "").strip()
    if user_id:
        aliases.append(f"<@{user_id.lower()}>")
    aliases.append(str(profile.get("name") or "").strip().lower())
    seen: set[str] = set()
    normalized_aliases: list[str] = []
    for alias in aliases:
        if not alias or alias in seen:
            continue
        seen.add(alias)
        normalized_aliases.append(alias)
    return tuple(normalized_aliases)


def build_team_chat_context(
    *texts: str,
    required_names: tuple[str, ...] = (),
    limit: int = 4,
) -> str:
    normalized = _normalize_context_text(*texts)
    matched_names: list[str] = []
    seen_names: set[str] = set()

    for required_name in required_names:
        canonical_name = str(required_name or "").strip()
        if canonical_name and canonical_name in _PROFILE_BY_NAME and canonical_name not in seen_names:
            seen_names.add(canonical_name)
            matched_names.append(canonical_name)

    for profile in TEAM_MEMBER_PROFILES:
        profile_name = str(profile.get("name") or "").strip()
        if not profile_name or profile_name in seen_names:
            continue
        if any(alias in normalized for alias in _iter_profile_aliases(profile)):
            seen_names.add(profile_name)
            matched_names.append(profile_name)
        if len(matched_names) >= max(0, limit):
            break

    lines = [
        "팀 대화 참고:",
        f"- {TEAM_CHAT_GENERAL_GUIDE}",
    ]
    if matched_names:
        lines.append("관련 인물 성향:")
        for name in matched_names[: max(0, limit)]:
            profile = _PROFILE_BY_NAME.get(name) or {}
            summary = str(profile.get("summary") or "").strip()
            if summary:
                lines.append(f"- {name}: {summary}")
    return "\n".join(lines)
