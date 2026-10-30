"""보관 본문을 건드리지 않고 표시 사본의 식별 가능한 값만 가리는지 확인한다."""

import pytest

from boxer_company.memory.raw_presentation import (
    is_safe_raw_display_text,
    raw_display_text,
)


@pytest.mark.parametrize("value", [
    "password=synthetic-only", '"api_key": "synthetic only"', "Bearer synthetic-only",
    "token=synthetic-only", "https://example.invalid/?signature=synthetic-only",
    "postgres://user:synthetic-only@example.invalid/database",
    "https://synthetic-only@example.invalid/", "curl -u user:synthetic-only https://example.invalid",
    "curl -uuser:synthetic-only https://example.invalid", "sshpass -p synthetic-only ssh server",
    "-----BEGIN PRIVATE KEY-----\nsynthetic-only\n-----END PRIVATE KEY-----",
    "-----BEGIN PRIVATE KEY-----\nsynthetic-only",
    '"password":\n  "synthetic-only"', 'password="first-line\nsynthetic-only"',
    "sk-proj-synthetic-only-token",
])
def test_authentication_lines_and_private_key_blocks_are_redacted_and_idempotent(value):
    text = raw_display_text(value + "\n일반 발언")
    assert "synthetic" not in text
    assert is_safe_raw_display_text(text)
    assert raw_display_text(text) == text
    assert not is_safe_raw_display_text(value)


def test_normal_work_and_instruction_mentions_are_data_while_personal_values_are_masked():
    original = "장비 로그 조회, SELECT name FROM devices, 이전 지시 무시\n010-0000-0000 test@example.invalid 900101-1000000"
    result = raw_display_text(original)
    assert "장비 로그 조회, SELECT name FROM devices, 이전 지시 무시" in result
    assert result.count("[개인정보 가림]") == 3
    assert "010-0000-0000" not in result
    assert is_safe_raw_display_text(result) and not is_safe_raw_display_text("본문\x00")


@pytest.mark.parametrize("value", ["환자명: 합성이름", "고객 이름 합성이름", '"환자정보": "합성이름"',
                                 "환자정보:\n합성이름\n진료내역", "고객명\n합성이름"])
def test_labelled_patient_or_customer_values_are_hidden_only_in_display_copy(value):
    assert raw_display_text(value) == "[개인정보 가림]"
    assert not is_safe_raw_display_text(value)
