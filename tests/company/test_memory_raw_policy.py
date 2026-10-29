"""고정 승인 범위는 KST 발언 생성 시각만으로 판단한다."""

from datetime import datetime

import pytest

from boxer_company.memory.raw_policy import (
    RAW_ONLY_CHANNEL_IDS,
    RAW_ONLY_END_TS,
    RAW_ONLY_START_TS,
    RAW_ONLY_TENANT_ID,
    is_raw_only_channel,
    is_raw_only_scope,
)


@pytest.mark.parametrize("channel", sorted(RAW_ONLY_CHANNEL_IDS))
@pytest.mark.parametrize("stamp,expected", [
    ("1790780399.999999999", False), (RAW_ONLY_START_TS, True),
    ("1793458799.999999999", True), (RAW_ONLY_END_TS, False),
    ("NaN", False), ("Infinity", False), (None, False), (1790780400, False),
])
def test_raw_scope_uses_precise_inclusive_october_start_and_exclusive_november_end(channel, stamp, expected):
    assert is_raw_only_scope(RAW_ONLY_TENANT_ID, channel, stamp) is expected


@pytest.mark.parametrize("tenant,channel", [
    ("OTHER", "C0621TL2HSB"), (RAW_ONLY_TENANT_ID, "COTHER"),
    (RAW_ONLY_TENANT_ID, "DTEST"), (RAW_ONLY_TENANT_ID, None),
    (RAW_ONLY_TENANT_ID, {}),
])
def test_question_or_channel_shape_cannot_expand_raw_scope(tenant, channel):
    assert is_raw_only_scope(tenant, channel, RAW_ONLY_START_TS) is False
    assert is_raw_only_channel(tenant, channel) is False


@pytest.mark.parametrize("channel", sorted(RAW_ONLY_CHANNEL_IDS))
@pytest.mark.parametrize("stamp", ["1000.000001", "1790780399.999999", RAW_ONLY_END_TS, "1893456000.000001"])
def test_channel_derivative_prohibition_is_permanent_without_expanding_october_raw_capture(channel, stamp):
    # 9월, 11월과 미래 시각에도 파생 기억은 금지하지만 원문을 새로 수집할 범위는 늘리지 않는다.
    assert is_raw_only_channel(RAW_ONLY_TENANT_ID, channel) is True
    assert is_raw_only_scope(RAW_ONLY_TENANT_ID, channel, stamp) is False


def test_fixed_timestamp_constants_represent_kst_october_boundaries():
    assert int(datetime.fromisoformat("2026-10-01T00:00:00+09:00").timestamp()) == int(RAW_ONLY_START_TS.split(".")[0])
    assert int(datetime.fromisoformat("2026-11-01T00:00:00+09:00").timestamp()) == int(RAW_ONLY_END_TS.split(".")[0])
