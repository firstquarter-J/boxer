"""지속 원문은 시작일 이후를 허용하고 재승인은 기존 10월 범위를 유지한다."""

from datetime import datetime

import pytest

from boxer_company.memory.raw_policy import (
    RAW_ONLY_CHANNEL_IDS,
    RAW_ONLY_END_TS,
    RAW_ONLY_START_TS,
    RAW_ONLY_TENANT_ID,
    is_legacy_raw_revalidation_scope,
    is_raw_only_channel,
    is_raw_only_scope,
)


@pytest.mark.parametrize("channel", sorted(RAW_ONLY_CHANNEL_IDS))
@pytest.mark.parametrize("stamp,expected", [
    ("1790780399.999999999", False), (RAW_ONLY_START_TS, True),
    ("1793458799.999999999", True), (RAW_ONLY_END_TS, True),
    ("1798729199.999999999", True), ("1798729200.000000000", True),
    ("9223372036854775807.999999999", True), ("9223372036854775808.000000000", False),
    ("NaN", False), ("Infinity", False), (None, False), (1790780400, False),
])
def test_raw_scope_has_precise_inclusive_start_and_no_month_or_year_end(channel, stamp, expected):
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
@pytest.mark.parametrize("stamp,raw_allowed", [("1000.000001", False), ("1790780399.999999", False),
    (RAW_ONLY_END_TS, True), ("1893456000.000001", True)])
def test_channel_derivative_prohibition_is_permanent_with_continuous_raw_capture(channel, stamp, raw_allowed):
    # 파생 기억 금지와 시작일 이후의 지속 원문 허용을 서로 독립적으로 확인한다.
    assert is_raw_only_channel(RAW_ONLY_TENANT_ID, channel) is True
    assert is_raw_only_scope(RAW_ONLY_TENANT_ID, channel, stamp) is raw_allowed


@pytest.mark.parametrize("channel", sorted(RAW_ONLY_CHANNEL_IDS))
@pytest.mark.parametrize("stamp,expected", [("1790780399.999999999", False), (RAW_ONLY_START_TS, True),
    ("1793458799.999999999", True), (RAW_ONLY_END_TS, False), ("1798729200.000000000", False)])
def test_revalidation_stays_in_legacy_october_even_when_raw_capture_continues(channel, stamp, expected):
    assert is_legacy_raw_revalidation_scope(RAW_ONLY_TENANT_ID, channel, stamp) is expected


def test_fixed_timestamp_constants_represent_kst_october_boundaries():
    assert int(datetime.fromisoformat("2026-10-01T00:00:00+09:00").timestamp()) == int(RAW_ONLY_START_TS.split(".")[0])
    assert int(datetime.fromisoformat("2026-11-01T00:00:00+09:00").timestamp()) == int(RAW_ONLY_END_TS.split(".")[0])
