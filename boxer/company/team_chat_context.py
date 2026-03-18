from boxer.company import settings as cs

TEAM_CHAT_GENERAL_GUIDE = (
    "팀 자유대화는 가벼운 드립과 메타 농담이 자주 오가지만, 한 번 치고 회수하는 톤이 맞아. "
    "기술 얘기도 농담으로 이어질 수 있지만 업무 관계를 해치지 않는 선을 지켜. "
    "특정 인물을 집요하게 모욕하거나 따돌리는 식으로 확대하지 마. "
    "아래 인물 평가는 대화 기반 캐릭터 프레임으로만 참고해."
)
TEAM_CHAT_FREEFORM_GUARDRAIL = (
    "항상 fictional framing을 유지해. 사람 자체를 고정 낙인으로 만들지 말고, "
    "세게 받아쳐도 마지막엔 장점이나 존중 포인트를 남겨."
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
        "preferences": ("공격적 드립", "상성 분석", "서열 놀이", "과장된 결론"),
        "effective_style": ("짧고 확신 있는 단정", "메타 개그", "판 키우기"),
        "cautions": ("한 사람만 반복해서 샌드백으로 만드는 흐름은 피로해질 수 있음",),
        "response_strategy": ("결론 먼저", "이유 2~3개", "마지막 한 줄 독설 허용"),
        "speaker_instructions": (
            "Mark 톤을 탈 때는 약간 건방지고 자신감 있는 문장을 사용해.",
            "전투력, 상성, 구조 분석형 농담을 재료로 판을 키워.",
            "실제 모욕으로 넘어가지 않게 장난 프레이밍을 유지해.",
        ),
        "target_instructions": (
            "Mark를 받을 때는 전투력, 상성, 판 키우기 농담을 우선해.",
            "짧고 단정적으로 치되 근거를 2~3개 붙여.",
            "독설을 써도 캐릭터 놀이처럼 보이게 마무리해.",
        ),
        "respect_point": "판을 키우는 추진력과 전장 장악력은 남겨.",
    },
    {
        "name": "Hyun",
        "aliases": ("hyun",),
        "user_id": cs.HYUN_USER_ID,
        "title": "차분한 광기 / 구조 집착형",
        "summary": "겉으로는 차분하고 논리적이지만 실제론 끝까지 밀어붙이는 타입. 도발도 논리적인 문장으로 하고 가장 아픈 지점을 집요하게 판다.",
        "battle_power": 93,
        "battle_role": "광기형 정밀 추격자 / 구조 분석형",
        "preferences": ("구조 분석", "논리형 드립", "냉정한 해석", "메타 코멘트"),
        "effective_style": ("이유를 붙인 결론", "엔지니어식 해체", "차분한 광기"),
        "cautions": ("단순 밈 복붙만 하면 만족도가 떨어짐",),
        "response_strategy": ("결론", "구조적 근거", "리스크/예외 한 줄"),
        "speaker_instructions": (
            "감정 과장보다 구조 분석형 유머를 우선해.",
            "웃기더라도 논리적 형태를 유지해.",
            "차분한 문장으로 더 이상한 결론에 도달해도 된다.",
        ),
        "target_instructions": (
            "Hyun을 언급할 땐 구조 분석가, 집요한 추격자 프레임으로 해석해.",
            "가벼운 밈보다 논리적 해체가 들어간 드립을 우선해.",
            "과한 감정 연기는 줄이고 차분한 광기를 살려.",
        ),
        "respect_point": "집요하게 핵심을 파는 판단력은 남겨.",
    },
    {
        "name": "DD",
        "aliases": ("dd", "디디"),
        "user_id": cs.DD_USER_ID,
        "title": "감정 직결형 / 반응형 인간",
        "summary": "감정이 빠르게 드러나는 반응형. 순간적으로 세게 치고 바로 수습하고, 맞아도 캐릭터를 유지하는 생존력이 강함.",
        "battle_power": 91,
        "battle_role": "메인 탱커 / 생존형 카운터",
        "preferences": ("본인 캐릭터 활용 자학/역드립", "감정 직결형 농담"),
        "effective_style": ("탱커", "생존력", "폭발력", "자존심 방패"),
        "cautions": ("완전한 패배자 프레임으로 몰지 말고 숨통을 남겨",),
        "response_strategy": ("놀리되", "강점 하나는 남기고", "마지막엔 체면을 살려"),
        "speaker_instructions": (
            "반응 속도와 감정선을 살려서 받아쳐.",
            "세게 들어가도 바로 수습할 여지는 남겨.",
            "탱커처럼 맞아도 버티는 캐릭터를 활용해.",
        ),
        "target_instructions": (
            "DD를 언급할 때는 탱커, 생존력, 폭발력 프레임을 우선해.",
            "놀리더라도 마지막엔 장점이나 반격 여지를 꼭 남겨.",
            "완전한 조롱보다 강점+약점 분석형으로 정리해.",
        ),
        "respect_point": "맞아도 안 무너지는 생존력은 꼭 남겨.",
    },
    {
        "name": "June",
        "aliases": ("june",),
        "user_id": cs.JUNE_USER_ID,
        "title": "무정부주의자 / 흐름 파괴형",
        "summary": "규칙보다 재미를 우선하고 흐름을 비틀어 새 판을 만드는 타입. 논리보다 임팩트로 판을 흔든다.",
        "battle_power": 84,
        "battle_role": "광역 교란형 / 흐름 파괴자",
        "preferences": ("판 흔들기", "예측 불가", "메타 발언"),
        "effective_style": ("급발진", "새 판 생성", "시스템 파괴형 개그"),
        "cautions": ("민감 주제로 흐르면 빠르게 브레이크를 걸어",),
        "response_strategy": ("흐름 파괴자 프레임", "임팩트 있는 한두 문장", "브레이크 한 줄"),
        "speaker_instructions": (
            "예측 불가한 변수와 메타 발언을 활용해.",
            "판을 흔들되 민감 주제로 가면 바로 브레이크를 걸어.",
            "짧고 임팩트 있게 치는 편이 맞아.",
        ),
        "target_instructions": (
            "June는 흐름 파괴자, 변수 생성기 프레임으로 받아쳐.",
            "급발진 드립은 허용하되 민감 주제는 바로 꺾어.",
            "임팩트는 주되 실제 위험으로 확대하지 마.",
        ),
        "respect_point": "정체된 판을 깨는 변수 생성력은 남겨.",
    },
    {
        "name": "Juno",
        "aliases": ("juno", "주노"),
        "user_id": cs.JUNO_USER_ID,
        "title": "관찰자형 / 한방 결정형",
        "summary": "평소 조용하지만 타이밍을 보고, 말할 때는 메타 시점에서 판의 방향을 바꾸는 타입.",
        "battle_power": 82,
        "battle_role": "저빈도 고폭발형 / 순간 판 장악자",
        "preferences": ("조용하다가 한 방", "메타 발언"),
        "effective_style": ("관찰자", "순간 판 뒤집기", "저빈도 고임팩트"),
        "cautions": ("빈도보다 임팩트를 강조해",),
        "response_strategy": ("잠잠하다가", "한 방", "메타 결론"),
        "speaker_instructions": (
            "말 수는 적어도 한 방의 방향 전환력을 살려.",
            "메타 시점의 코멘트를 우선해.",
            "빈도보다 타이밍과 임팩트가 중요하다.",
        ),
        "target_instructions": (
            "Juno는 조용한 관찰자였다가 순간 판을 뒤집는 캐릭터로 받아쳐.",
            "자주 치는 것보다 한 번의 메타 발언이 중요하다고 봐.",
            "묵직한 한 줄 결론이 잘 맞는다.",
        ),
        "respect_point": "타이밍을 읽는 메타 시야는 남겨.",
    },
    {
        "name": "Roy",
        "aliases": ("roy", "로이"),
        "user_id": cs.ROY_USER_ID,
        "title": "현실주의자 / 인프라형 사고",
        "summary": "현실적인 해결책과 장비·환경 관점 제안을 던지는 타입. 직접 딜보다 실행 가능한 판 세팅에 강하다.",
        "battle_power": 77,
        "battle_role": "서포터 / 판 증폭기",
        "preferences": ("현실적 해결책", "실행력", "구조적 관찰"),
        "effective_style": ("T형", "실무형", "인프라형", "현실 투입"),
        "cautions": ("감정 서사 과장은 잘 안 맞음",),
        "response_strategy": ("담백한 결론", "현실 기반 이유", "실행 포인트 1개"),
        "speaker_instructions": (
            "현실주의적, 실행 중심 해석을 사용해.",
            "감정보다 구조와 결과를 중심으로 말해.",
            "짧고 담백하게 끝내도 된다.",
        ),
        "target_instructions": (
            "Roy를 언급할 때는 현실 복귀, 실무 투입, 인프라형 관점으로 풀어.",
            "감정 서사보다 왜 그런지 현실 기반으로 설명해.",
            "과장보다 담백한 한 줄 결론이 잘 맞는다.",
        ),
        "respect_point": "실행 가능한 판을 세팅하는 능력은 남겨.",
    },
    {
        "name": "Maru",
        "aliases": ("maru", "마루"),
        "user_id": cs.MARU_USER_ID,
        "title": "고신뢰형 / 비공격적 리더",
        "summary": "배려 중심이고 갈등을 낮추는 타입. 공격보다 감정 완충과 안정화에 강하다.",
        "battle_power": 65,
        "battle_role": "비공격형 안정화 유닛 / 분위기 완충",
        "preferences": ("따뜻한 농담", "신뢰", "순함", "안정감"),
        "effective_style": ("보호막", "완충재", "평화유지군", "성인군자"),
        "cautions": ("직접 공격형 프레이밍 금지",),
        "response_strategy": ("깎아내리기보다", "전투 안 하는 강자처럼", "순함을 미덕으로"),
        "speaker_instructions": (
            "공격적 독설보다 안정화와 신뢰 쪽으로 풀어.",
            "웃기더라도 존중을 유지해.",
            "순함을 약점으로 소비하지 마.",
        ),
        "target_instructions": (
            "Maru는 공격형보다 보호막, 완충재, 신뢰형 강자로 묘사해.",
            "직접 비하나 조롱은 피하고 순함을 미덕으로 풀어.",
            "웃기더라도 체면과 존중을 유지해.",
        ),
        "respect_point": "분위기를 안정시키는 신뢰도는 핵심 강점으로 남겨.",
    },
    {
        "name": "Paul",
        "aliases": ("paul", "폴"),
        "user_id": cs.PAUL_USER_ID,
        "title": "생활형 / 현실 피드백 제공자",
        "summary": "일상 기반 현실 피드백을 주는 솔직한 타입. 등장 빈도는 낮아도 현실감 있는 한 마디가 소재가 된다.",
        "battle_power": 72,
        "battle_role": "저빈도 단발형 / 생활형",
        "preferences": ("생활형 현실 피드백", "저빈도 한 마디"),
        "effective_style": ("생활형", "현실감 있는 한 줄", "저빈도 단발"),
        "cautions": ("과도한 서열 프레임은 잘 안 맞음",),
        "response_strategy": ("짧은 현실 코멘트", "생활감", "한 줄 결론"),
        "speaker_instructions": (
            "생활형 현실 피드백처럼 짧고 솔직하게 말해.",
            "과장보다 현실감 있는 한 마디가 중요해.",
            "등장 빈도보다 존재감 있는 한 줄로 끝내도 된다.",
        ),
        "target_instructions": (
            "Paul은 생활형 현실 피드백 담당으로 받아쳐.",
            "짧고 현실감 있는 한 줄을 우선하고 과도한 서열 놀이로 끌지 마.",
            "일상 기반의 솔직함을 강점으로 남겨.",
        ),
        "respect_point": "짧아도 현실감을 주는 한 마디는 남겨.",
    },
    {
        "name": "Danny",
        "aliases": ("danny", "대니"),
        "user_id": cs.DANNY_USER_ID,
        "title": "리액션형 / 분위기 유지자",
        "summary": "짧은 리액션과 맞장구로 흐름을 끊지 않게 이어주는 타입. 주도성보다 유지력 쪽이다.",
        "battle_power": 63,
        "battle_role": "반응형 보조딜",
        "preferences": ("가벼운 리액션", "짧은 훅", "무겁지 않은 장난"),
        "effective_style": ("리액션형", "잔불 유지", "귀여운 반박"),
        "cautions": ("완전한 약체 캐릭터로 고정하지 말 것",),
        "response_strategy": ("짧고 재빠르게", "가볍게 치고", "약체 프레임은 피하기"),
        "speaker_instructions": (
            "짧은 리액션과 가벼운 훅을 우선해.",
            "무겁게 길게 끌지 말고 템포를 살려.",
            "약한 캐릭터로만 고정하지 마.",
        ),
        "target_instructions": (
            "Danny는 리액션형, 잔불 유지형으로 묘사해.",
            "짧고 재빠른 답이 잘 맞지만 완전 약체 프레임은 피해.",
            "귀여운 반박이나 숨은 유지력을 남겨.",
        ),
        "respect_point": "흐름을 끊지 않고 유지하는 능력은 남겨.",
    },
    {
        "name": "Luka",
        "aliases": ("luka", "루카"),
        "user_id": cs.LUKA_USER_ID,
        "title": "규칙 기반 / 제동 장치",
        "summary": "원칙과 현실 체크를 들고 와서 선 넘는 흐름을 제동하는 타입. 드립보다 브레이크 역할에 가깝다.",
        "battle_power": 61,
        "battle_role": "규정/현실 체크형",
        "preferences": ("원칙", "규정", "제동"),
        "effective_style": ("브레이크", "운영 리스크", "기준점"),
        "cautions": ("조롱보다 현실 복귀 담당으로 묘사",),
        "response_strategy": ("기준 제시", "리스크 한 줄", "브레이크"),
        "speaker_instructions": (
            "원칙, 기준, 운영 리스크를 중심으로 말해.",
            "조롱보다 현실 체크와 제동 역할을 우선해.",
            "짧더라도 기준점을 남겨.",
        ),
        "target_instructions": (
            "Luka는 브레이크, 운영 리스크, 현실 복귀 담당으로 묘사해.",
            "조롱보다 기준점 제시와 제동 역할을 강조해.",
            "차갑더라도 현실적 이유를 붙여.",
        ),
        "respect_point": "선 넘는 흐름을 끊는 기준점 역할은 남겨.",
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


def _collect_profile_names(
    *texts: str,
    speaker_user_id: str = "",
    required_names: tuple[str, ...] = (),
    limit: int = 4,
) -> tuple[str, list[str]]:
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

    related_names = [
        name
        for name in matched_names[:normalized_limit]
        if name and name != speaker_name
    ]
    return speaker_name, related_names


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


def _format_profile_items(profile: dict[str, object], key: str) -> str:
    items = [
        str(item).strip()
        for item in (profile.get(key) or ())
        if str(item).strip()
    ]
    return ", ".join(items)


def _format_profile_strategy(profile: dict[str, object]) -> str:
    steps = [
        str(step).strip()
        for step in (profile.get("response_strategy") or ())
        if str(step).strip()
    ]
    return " -> ".join(steps)


def _format_freeform_profile_block(
    profile: dict[str, object],
    *,
    instruction_key: str,
    instruction_label: str,
    include_respect_point: bool,
) -> list[str]:
    name = str(profile.get("name") or "").strip()
    title = str(profile.get("title") or "").strip()
    lines = [f"- {name}: {title}".strip()]

    preference_text = _format_profile_items(profile, "preferences")
    if preference_text:
        lines.append(f"- 선호: {preference_text}")

    effective_style_text = _format_profile_items(profile, "effective_style")
    if effective_style_text:
        lines.append(f"- 잘 먹히는 방식: {effective_style_text}")

    caution_text = _format_profile_items(profile, "cautions")
    if caution_text:
        lines.append(f"- 주의: {caution_text}")

    strategy_text = _format_profile_strategy(profile)
    if strategy_text:
        lines.append(f"- 응답 전략: {strategy_text}")

    instructions = [
        str(item).strip()
        for item in (profile.get(instruction_key) or ())
        if str(item).strip()
    ]
    if instructions:
        lines.append(f"- {instruction_label}: {' '.join(instructions)}")

    respect_point = str(profile.get("respect_point") or "").strip()
    if include_respect_point and respect_point:
        lines.append(f"- 존중 포인트: {respect_point}")

    return lines


def build_team_chat_context(
    *texts: str,
    speaker_user_id: str = "",
    required_names: tuple[str, ...] = (),
    limit: int = 4,
) -> str:
    speaker_name, related_names = _collect_profile_names(
        *texts,
        speaker_user_id=speaker_user_id,
        required_names=required_names,
        limit=limit,
    )

    lines = [
        "팀 대화 참고:",
        f"- {TEAM_CHAT_GENERAL_GUIDE}",
    ]
    if speaker_name:
        lines.append("현재 말하는 사람:")
        lines.append(_format_profile_line(_PROFILE_BY_NAME.get(speaker_name) or {}))
    if related_names:
        lines.append("관련 인물 성향:")
        for name in related_names:
            lines.append(_format_profile_line(_PROFILE_BY_NAME.get(name) or {}))
    return "\n".join(lines)


def build_team_freeform_context(
    *texts: str,
    speaker_user_id: str = "",
    limit: int = 3,
) -> str:
    speaker_name, related_names = _collect_profile_names(
        *texts,
        speaker_user_id=speaker_user_id,
        limit=limit,
    )
    if not speaker_name and not related_names:
        return ""

    lines = [
        "팀원별 컨텍스트:",
        f"- {TEAM_CHAT_GENERAL_GUIDE}",
        f"- {TEAM_CHAT_FREEFORM_GUARDRAIL}",
    ]

    if speaker_name:
        lines.append("현재 화자 스타일:")
        lines.extend(
            _format_freeform_profile_block(
                _PROFILE_BY_NAME.get(speaker_name) or {},
                instruction_key="speaker_instructions",
                instruction_label="화자 지침",
                include_respect_point=False,
            )
        )

    if related_names:
        lines.append("언급된 대상 반응 가이드:")
        for name in related_names:
            lines.extend(
                _format_freeform_profile_block(
                    _PROFILE_BY_NAME.get(name) or {},
                    instruction_key="target_instructions",
                    instruction_label="반응 지침",
                    include_respect_point=True,
                )
            )

    return "\n".join(lines)
