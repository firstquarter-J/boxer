"""팀원 별칭의 대소문자·조사와 단어 경계를 함께 처리한다."""

from __future__ import annotations

from functools import lru_cache
import re
from typing import Iterable
import unicodedata


# 임의의 한글 접미사를 허용하지 않고 실제 대화에서 쓰는 조사만 인식한다.
# 조사 뒤에도 경계를 검사해 DDos·디디다른이름 등을 사람으로 고르지 않는다.
_PARTICLES = (
    "은", "는", "이", "가", "을", "를", "의", "와", "과", "랑", "이랑", "하고",
    "도", "만", "에", "에는", "에도", "에만", "에게", "에게는", "에게도", "에게만",
    "에게서", "에게서는", "에게서도", "한테", "한테는", "한테도", "한테만",
    "한테서", "한테서는", "한테서도", "께", "께서", "로", "으로", "로는", "으로는",
    "로부터", "으로부터", "에서", "에서는", "에서도", "하고는", "하고도",
    "보다", "보다는", "처럼", "부터", "부터는", "까지", "까지는", "조차", "마저",
    "와는", "과는", "랑은", "이랑은", "라고", "이라고", "라는", "이라는", "라면", "이라면",
    "야", "아", "이야", "이다",
)
_PARTICLE_PATTERN = "|".join(re.escape(value) for value in sorted(_PARTICLES, key=len, reverse=True))


def _normalize(value: str) -> str:
    return unicodedata.normalize("NFKC", value).casefold()


@lru_cache(maxsize=256)
def _alias_pattern(alias: str) -> re.Pattern[str]:
    # 이름 내부의 공백은 합치지 않는다. 등록하지 않은 별칭을 추정하지 않는 경계다.
    return re.compile(
        r"(?<!\w)" + re.escape(alias)
        + r"(?:님|씨|형|누나|오빠|언니)?(?:" + _PARTICLE_PATTERN + r")?(?!\w)"
    )


def matches_team_member_alias(text: str, aliases: Iterable[str]) -> bool:
    """등록된 별칭 하나가 독립 토큰 또는 이름+호칭/조사로 등장하는지 확인한다."""
    normalized = _normalize(text)
    return any(
        _alias_pattern(_normalize(alias.strip())).search(normalized) is not None
        for alias in aliases if isinstance(alias, str) and alias.strip()
    )
