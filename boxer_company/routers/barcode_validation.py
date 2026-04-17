from typing import Any

from boxer.core.utils import _display_value
from boxer_company.routers.mda_graphql import _lookup_mda_special_barcodes_by_barcode

_BARCODE_VALIDATION_CONTEXT_HINTS = (
    "유효성 검사",
    "유효성 검증",
    "바코드 검증",
    "걸리는 바코드",
    "막히는 바코드",
    "차단 대상",
    "제한 대상",
)
_BARCODE_VALIDATION_STATUS_HINTS = (
    "걸리",
    "막히",
    "차단",
    "제한",
    "통과",
    "허용",
    "무료 바코드",
    "환불 바코드",
)
_BLOCKING_SPECIAL_TYPE_LABELS = {
    "FREE": "무료 바코드",
    "REFUND": "환불 처리 바코드",
}


def _contains_any_hint(question: str, hints: tuple[str, ...]) -> bool:
    normalized = str(question or "").strip()
    lowered = normalized.lower()
    return any(hint in normalized or hint in lowered for hint in hints)


def _is_barcode_validation_status_request(question: str, barcode: str | None) -> bool:
    if not str(barcode or "").strip():
        return False
    normalized = str(question or "").strip()
    if not normalized:
        return False
    if _contains_any_hint(normalized, _BARCODE_VALIDATION_CONTEXT_HINTS):
        return True
    return _contains_any_hint(normalized, _BARCODE_VALIDATION_STATUS_HINTS)


def _normalize_special_barcode_type(value: Any) -> str:
    return _display_value(value, default="").strip().upper()


def _query_barcode_validation_status(barcode: str) -> str:
    normalized_barcode = str(barcode or "").strip()
    if not normalized_barcode:
        raise ValueError("바코드가 필요해")

    matches = _lookup_mda_special_barcodes_by_barcode(normalized_barcode)
    if not matches:
        return "\n".join(
            (
                "*바코드 유효성 검사 확인*",
                f"• 바코드: `{normalized_barcode}`",
                "• 결론: 현재 운영 제한 목록 기준으로는 유효성 검사에 걸리는 바코드로 확인되지 않았어",
                "• 확인: 무료 바코드나 환불 처리 바코드 목록에는 없어",
                "• 조치: 다만 일반 바코드 만료 여부까지는 여기서 바로 단정 못 해서, 꼭 확정이 필요하면 실제 장비에서 확인해",
            )
        )

    target = matches[0]
    special_type = _normalize_special_barcode_type(target.get("type"))
    reason = _display_value(target.get("reason"), default="")
    type_label = _BLOCKING_SPECIAL_TYPE_LABELS.get(special_type, special_type or "미확인")

    if special_type in _BLOCKING_SPECIAL_TYPE_LABELS:
        confirm = f"운영 제한 목록에서 `{type_label}`로 등록돼 있어"
        if reason:
            confirm = f"{confirm} / 사유: `{reason}`"
        return "\n".join(
            (
                "*바코드 유효성 검사 확인*",
                f"• 바코드: `{normalized_barcode}`",
                "• 결론: 이 바코드는 유효성 검사에 걸리는 바코드야",
                f"• 확인: {confirm}",
                "• 조치: 유효성 검사가 켜진 장비에서는 촬영 전에 막혀야 해",
            )
        )

    confirm = f"운영 제한 목록에서 `{type_label}` 유형으로 보이지만 현재 기준으로는 차단형 타입인지 바로 단정하긴 어려워"
    if reason:
        confirm = f"{confirm} / 사유: `{reason}`"
    return "\n".join(
        (
            "*바코드 유효성 검사 확인*",
            f"• 바코드: `{normalized_barcode}`",
            "• 결론: 이 바코드는 운영 제한 목록에 등록돼 있어",
            f"• 확인: {confirm}",
            "• 조치: 실제 차단 여부가 꼭 필요하면 유효성 검사가 켜진 장비에서 다시 확인해",
        )
    )
