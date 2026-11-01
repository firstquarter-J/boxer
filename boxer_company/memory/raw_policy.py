"""승인된 두 채널의 지속 원문 수집과 기존 10월 재승인 범위를 분리한다."""

from __future__ import annotations

import re
from decimal import Decimal

RAW_ONLY_TENANT_ID = "T19PA3EPL"
RAW_ONLY_CHANNEL_IDS = frozenset({"C0621TL2HSB", "C068FVD5V7Y"})
RAW_ONLY_START_TS = "1790780400.000000"
# 기존 10월 snapshot과 재승인 journal의 경계다. 지속 수집의 만료일로 쓰지 않는다.
RAW_ONLY_END_TS = "1793458800.000000"
RAW_ONLY_CUTOFF_TS = "1793458799.999999"
_TIMESTAMP = re.compile(r"[0-9]{1,20}\.[0-9]{1,9}")


def is_raw_only_channel(tenant_id: str, channel_id: str) -> bool:
    """두 채널에서는 기간과 무관하게 요약, 사실과 선호의 파생 기억을 생성하지 않는다."""
    return bool(tenant_id == RAW_ONLY_TENANT_ID and isinstance(channel_id, str) and channel_id in RAW_ONLY_CHANNEL_IDS)


def is_raw_only_scope(tenant_id: str, channel_id: str, message_ts: str) -> bool:
    """승인된 두 채널의 2026-10-01 KST 이후 발언을 종료일 없이 수집한다."""
    return bool(is_raw_only_channel(tenant_id, channel_id)
                and isinstance(message_ts, str) and _TIMESTAMP.fullmatch(message_ts)
                and Decimal(RAW_ONLY_START_TS) <= Decimal(message_ts) < 2**63)


def is_legacy_raw_revalidation_scope(tenant_id: str, channel_id: str, message_ts: str) -> bool:
    """기존 10월 원문의 명시 재승인만 허용해 옛 journal의 복원 의미를 유지한다."""
    return bool(is_raw_only_scope(tenant_id, channel_id, message_ts)
                and Decimal(message_ts) < Decimal(RAW_ONLY_END_TS))
