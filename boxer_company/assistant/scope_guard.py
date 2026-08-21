from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from boxer.context.entries import ContextEntry
from boxer.context.windowing import window_context_entries
from boxer.core import settings as s
from boxer_company.assistant.contracts import (
    AssistantMessage,
    CompanyAssistantRequest,
    CompanyAssistantResult,
)
from boxer_company.routers.barcode_log import (
    _extract_device_name_scope,
    _extract_hospital_room_scope,
)
from boxer_company.utils import _extract_barcode


class AssistantRequestScopeMismatch(ValueError):
    def __init__(self, dimension: str) -> None:
        super().__init__(f"{dimension} scope mismatch")
        self.dimension = dimension


@dataclass(frozen=True, slots=True)
class AssistantRequestScope:
    barcode: str | None
    hospital_name: str | None
    room_name: str | None
    device_name: str | None


def resolve_assistant_request_scope(
    request: CompanyAssistantRequest,
) -> AssistantRequestScope:
    """질문과 adapter scope가 함께 있으면 일치할 때만 조회 범위로 확정한다."""
    question_barcode = _extract_barcode(request.question)
    question_hospital, question_room = _extract_hospital_room_scope(
        request.question
    )
    question_device = _extract_device_name_scope(request.question)

    metadata_barcode = _metadata_text(request, "barcode")
    metadata_hospital = _metadata_text(
        request,
        "hospital_name",
        "hospitalName",
        "phase2_hospital_name",
        "phase2HospitalName",
    )
    metadata_room = _metadata_text(
        request,
        "room_name",
        "roomName",
        "phase2_room_name",
        "phase2RoomName",
    )
    metadata_device = _metadata_text(
        request,
        "device_name",
        "deviceName",
    )

    _raise_if_mismatch("barcode", metadata_barcode, question_barcode)
    _raise_if_mismatch(
        "hospital_room",
        metadata_hospital,
        question_hospital,
    )
    _raise_if_mismatch("hospital_room", metadata_room, question_room)
    _raise_if_mismatch("device", metadata_device, question_device)
    return AssistantRequestScope(
        barcode=metadata_barcode or question_barcode,
        hospital_name=metadata_hospital or question_hospital,
        room_name=metadata_room or question_room,
        device_name=metadata_device or question_device,
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


def window_assistant_context_entries(
    request: CompanyAssistantRequest,
) -> tuple[ContextEntry, ...]:
    return tuple(
        window_context_entries(
            list(request.context_entries),
            max_chars=max(1, s.THREAD_CONTEXT_MAX_CHARS),
        )
    )


def _metadata_text(
    request: CompanyAssistantRequest,
    *keys: str,
) -> str | None:
    for key in keys:
        value: Any = request.metadata.get(key)
        if isinstance(value, str):
            normalized = " ".join(value.split()).strip()
            if normalized:
                return normalized
    return None


def _raise_if_mismatch(
    dimension: str,
    metadata_value: str | None,
    question_value: str | None,
) -> None:
    if (
        metadata_value
        and question_value
        and metadata_value != question_value
    ):
        # 실제 scope 값은 exception이나 응답에 넣지 않아 교차 조회 정보를 숨긴다.
        raise AssistantRequestScopeMismatch(dimension)


__all__ = [
    "AssistantRequestScope",
    "AssistantRequestScopeMismatch",
    "build_scope_mismatch_result",
    "resolve_assistant_request_scope",
    "window_assistant_context_entries",
]
