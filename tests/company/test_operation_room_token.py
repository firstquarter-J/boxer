"""병실 토큰의 전체 범위를 유지하면서 긴 무관 본문도 같은 정책으로 판정한다."""

import pytest

from boxer_company._operation_routing_common import _ROOM_TOKEN_PATTERN
from boxer_company.memory.policy import is_allowed_conversation_source


@pytest.mark.parametrize("text,token", [
    ("테스트병원 2층1-1진료실 로그 조회", "2층1-1진료실"),
    ("앞말`가나다초음파실뒷말'뒤", "가나다초음파실뒷말"),
    ('병원,"상담분만실1"', "상담분만실1"),
    ("병원 12 호 로그", "12 호"),
    ("기록 A123호입니다", None),
])
def test_room_token_keeps_full_word_and_numeric_room_boundaries(text, token):
    match = _ROOM_TOKEN_PATTERN.search(text)
    assert (match.group(1) if match else None) == token


def test_long_unrelated_hangul_is_not_a_room_or_an_operation_request():
    # 실행 시간 임계값 대신 실제 전체 정책 경로가 무관 본문을 그대로 허용하는지 검사한다.
    text = "가" * 40000
    assert _ROOM_TOKEN_PATTERN.search(text) is None
    assert is_allowed_conversation_source(text)
