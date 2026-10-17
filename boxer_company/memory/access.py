"""운영자가 지정한 대화 채널의 비공개 기억 조회 범위를 정한다."""

from __future__ import annotations


# 사용자가 이 회사·채널에 다른 비공개 채널 대화의 조회·인용을 명시 허용했다.
# 질문 본문이나 API payload로 예외 대상과 tenant를 추가할 수 없게 고정한다.
_PRIVATE_MEMORY_HUBS = frozenset({("T19PA3EPL", "C068FVD5V7Y")})


def is_private_memory_hub(tenant_id: str, channel_id: str) -> bool:
    return (tenant_id, channel_id) in _PRIVATE_MEMORY_HUBS


def can_read_memory_source(
    tenant_id: str,
    destination_channel_id: str,
    source_channel_id: str,
    source_channel_kind: str,
) -> bool:
    """일반 요청은 원채널, 지정 허브는 같은 회사의 비공개 채널까지 허용한다."""
    return bool(
        source_channel_id
        and (
            source_channel_id == destination_channel_id
            or (
                is_private_memory_hub(tenant_id, destination_channel_id)
                and source_channel_kind == "private_channel"
            )
        )
    )
