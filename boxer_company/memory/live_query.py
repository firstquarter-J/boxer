"""API와 Slack이 같은 질문에만 비공개 원문 조회를 실행한다."""

from __future__ import annotations

import re
from typing import Sequence
import unicodedata


def wants_private_live_sources(question: str, author_ids: Sequence[str] = ()) -> bool:
    if any(author_ids):
        return True
    # 인사·일반 지식은 조회 실패 안내를 붙이지 않고 기존 답변 흐름을 유지한다.
    text = unicodedata.normalize("NFKC", question).casefold()
    reference = re.search(
        r"슬랙|slack|비공개|채널|대화|발언|이야기|얘기|소식|"
        r"(?:^|\s)(?:한\s*)?말(?:은|이|을|들)?(?=\s|[?!？]|$)", text,
    )
    query = re.search(r"최근|요즘|요새|방금|오늘|어제|지난|마지막|조회|검색|찾|보여|알려|뭐|무슨|어떤|있", text)
    return bool(reference and query)
