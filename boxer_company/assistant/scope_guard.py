from __future__ import annotations

from boxer_company._operation_routing_common import (
    AssistantRequestScopeMismatch,
)

from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantResult,
)


def build_scope_mismatch_result(
    mismatch: AssistantRequestScopeMismatch,
) -> CompanyAssistantResult:
    labels = {
        "barcode": "바코드",
        "device": "장비",
        "hospital_room": "병원/병실",
    }
    label = labels.get(mismatch.dimension, "조회 범위")
    return CompanyAssistantResult(
        route=f"{mismatch.dimension}_scope_guard",
        outcome="denied",
        messages=(
            AssistantMessage(
                body=(
                    f"요청 {label}와 조회 컨텍스트가 일치하지 않아. "
                    "새 요청으로 다시 시도해줘"
                )
            ),
        ),
        fallback_reason=f"{mismatch.dimension}_scope_mismatch",
    )


__all__ = [
    "build_scope_mismatch_result",
]
