from boxer.company import settings as cs

TEAM_CHAT_GENERAL_GUIDE = (
    "팀 자유대화는 가벼운 드립과 메타 농담이 자주 오가지만, 한 번 치고 회수하는 톤이 맞아. "
    "기술 얘기도 농담으로 이어질 수 있지만 업무 관계를 해치지 않는 선을 지켜. "
    "특정 인물을 집요하게 모욕하거나 따돌리는 식으로 확대하지 마. "
    "아래 인물 평가는 대화 기반 캐릭터 프레임으로만 참고해."
)

TEAM_MEMBER_PROFILES: tuple[dict[str, object], ...] = (
    {
        "name": "Mark",
        "aliases": ("mark", "마크"),
        "user_id": cs.MARK_USER_ID,
        "title": "판 설계형 / 공격적 낙관주의자",
        "summary": "상황을 가볍게 만들면서도 계속 키우는 타입. 기술 얘기도 드립으로 연결하고 남의 발언을 재가공해 판을 키움.",
        "battle_power": 96,
        "battle_role": "메인 딜러 / 전장 장악형",
    },
    {
        "name": "Hyun",
        "aliases": ("hyun",),
        "user_id": cs.HYUN_USER_ID,
        "title": "차분한 광기 / 구조 집착형",
        "summary": "겉으로는 차분하고 논리적이지만 실제론 끝까지 밀어붙이는 타입. 도발도 논리적인 문장으로 하고 가장 아픈 지점을 집요하게 판다.",
        "battle_power": 93,
        "battle_role": "광기형 정밀 추격자 / 구조 분석형",
    },
    {
        "name": "DD",
        "aliases": ("dd", "디디"),
        "user_id": cs.DD_USER_ID,
        "title": "감정 직결형 / 반응형 인간",
        "summary": "감정이 빠르게 드러나는 반응형. 순간적으로 세게 치고 바로 수습하고, 맞아도 캐릭터를 유지하는 생존력이 강함.",
        "battle_power": 91,
        "battle_role": "메인 탱커 / 생존형 카운터",
    },
    {
        "name": "June",
        "aliases": ("june",),
        "user_id": cs.JUNE_USER_ID,
        "title": "무정부주의자 / 흐름 파괴형",
        "summary": "규칙보다 재미를 우선하고 흐름을 비틀어 새 판을 만드는 타입. 논리보다 임팩트로 판을 흔든다.",
        "battle_power": 84,
        "battle_role": "광역 교란형 / 흐름 파괴자",
    },
    {
        "name": "Juno",
        "aliases": ("juno", "주노"),
        "user_id": cs.JUNO_USER_ID,
        "title": "관찰자형 / 한방 결정형",
        "summary": "평소 조용하지만 타이밍을 보고, 말할 때는 메타 시점에서 판의 방향을 바꾸는 타입.",
        "battle_power": 82,
        "battle_role": "저빈도 고폭발형 / 순간 판 장악자",
    },
    {
        "name": "Roy",
        "aliases": ("roy", "로이"),
        "user_id": cs.ROY_USER_ID,
        "title": "현실주의자 / 인프라형 사고",
        "summary": "현실적인 해결책과 장비·환경 관점 제안을 던지는 타입. 직접 딜보다 실행 가능한 판 세팅에 강하다.",
        "battle_power": 77,
        "battle_role": "서포터 / 판 증폭기",
    },
    {
        "name": "Maru",
        "aliases": ("maru", "마루"),
        "user_id": cs.MARU_USER_ID,
        "title": "고신뢰형 / 비공격적 리더",
        "summary": "배려 중심이고 갈등을 낮추는 타입. 공격보다 감정 완충과 안정화에 강하다.",
        "battle_power": 65,
        "battle_role": "비공격형 안정화 유닛 / 분위기 완충",
    },
    {
        "name": "Paul",
        "aliases": ("paul", "폴"),
        "user_id": cs.PAUL_USER_ID,
        "title": "생활형 / 현실 피드백 제공자",
        "summary": "일상 기반 현실 피드백을 주는 솔직한 타입. 등장 빈도는 낮아도 현실감 있는 한 마디가 소재가 된다.",
        "battle_power": 72,
        "battle_role": "저빈도 단발형 / 생활형",
    },
    {
        "name": "Danny",
        "aliases": ("danny", "대니"),
        "user_id": cs.DANNY_USER_ID,
        "title": "리액션형 / 분위기 유지자",
        "summary": "짧은 리액션과 맞장구로 흐름을 끊지 않게 이어주는 타입. 주도성보다 유지력 쪽이다.",
        "battle_power": 63,
        "battle_role": "반응형 보조딜",
    },
    {
        "name": "Luka",
        "aliases": ("luka", "루카"),
        "user_id": cs.LUKA_USER_ID,
        "title": "규칙 기반 / 제동 장치",
        "summary": "원칙과 현실 체크를 들고 와서 선 넘는 흐름을 제동하는 타입. 드립보다 브레이크 역할에 가깝다.",
        "battle_power": 61,
        "battle_role": "규정/현실 체크형",
    },
)

_PROFILE_BY_NAME = {
    str(profile["name"]): profile
    for profile in TEAM_MEMBER_PROFILES
}
_PROFILE_BY_USER_ID = {
    str(profile.get("user_id") or "").strip().lower(): profile
    for profile in TEAM_MEMBER_PROFILES
    if str(profile.get("user_id") or "").strip()
}


def _normalize_context_text(*texts: str) -> str:
    joined = " ".join(str(text or "") for text in texts)
    return joined.lower().strip()


def _iter_profile_aliases(profile: dict[str, object]) -> tuple[str, ...]:
    aliases = [str(alias).strip().lower() for alias in (profile.get("aliases") or ()) if str(alias).strip()]
    user_id = str(profile.get("user_id") or "").strip()
    if user_id:
        aliases.append(user_id.lower())
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


def _append_profile_name(
    names: list[str],
    seen_names: set[str],
    profile_name: str,
    *,
    limit: int,
) -> None:
    if not profile_name or profile_name in seen_names or len(names) >= max(0, limit):
        return
    seen_names.add(profile_name)
    names.append(profile_name)


def _format_profile_line(profile: dict[str, object]) -> str:
    name = str(profile.get("name") or "").strip()
    title = str(profile.get("title") or "").strip()
    summary = str(profile.get("summary") or "").strip()
    battle_power = profile.get("battle_power")
    battle_role = str(profile.get("battle_role") or "").strip()

    segments = [segment for segment in (title, summary) if segment]
    if battle_power:
        power_text = f"전투력 {battle_power}"
        if battle_role:
            power_text = f"{power_text}, {battle_role}"
        segments.append(power_text)
    elif battle_role:
        segments.append(battle_role)
    return f"- {name}: {' '.join(segments)}".strip()


def build_team_chat_context(
    *texts: str,
    speaker_user_id: str = "",
    required_names: tuple[str, ...] = (),
    limit: int = 4,
) -> str:
    normalized = _normalize_context_text(*texts)
    matched_names: list[str] = []
    seen_names: set[str] = set()
    normalized_limit = max(0, limit)

    speaker_profile = _PROFILE_BY_USER_ID.get(str(speaker_user_id or "").strip().lower())
    speaker_name = str((speaker_profile or {}).get("name") or "").strip()
    _append_profile_name(
        matched_names,
        seen_names,
        speaker_name,
        limit=normalized_limit,
    )

    for required_name in required_names:
        canonical_name = str(required_name or "").strip()
        if canonical_name and canonical_name in _PROFILE_BY_NAME:
            _append_profile_name(
                matched_names,
                seen_names,
                canonical_name,
                limit=normalized_limit,
            )

    for profile in TEAM_MEMBER_PROFILES:
        profile_name = str(profile.get("name") or "").strip()
        if not profile_name or profile_name in seen_names:
            continue
        if any(alias in normalized for alias in _iter_profile_aliases(profile)):
            _append_profile_name(
                matched_names,
                seen_names,
                profile_name,
                limit=normalized_limit,
            )
        if len(matched_names) >= normalized_limit:
            break

    lines = [
        "팀 대화 참고:",
        f"- {TEAM_CHAT_GENERAL_GUIDE}",
    ]
    if speaker_name and speaker_name in matched_names:
        lines.append("현재 말하는 사람:")
        lines.append(_format_profile_line(_PROFILE_BY_NAME.get(speaker_name) or {}))
    related_names = [
        name
        for name in matched_names[:normalized_limit]
        if name and name != speaker_name
    ]
    if related_names:
        lines.append("관련 인물 성향:")
        for name in related_names:
            lines.append(_format_profile_line(_PROFILE_BY_NAME.get(name) or {}))
    return "\n".join(lines)
